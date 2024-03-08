mod generator;
mod workload;

#[macro_use]
extern crate log;

use crate::generator::ConstGen;
use clap::{Parser, ValueEnum};
use reqwest::{Client, StatusCode};
use std::{
    fmt,
    fs::File,
    io::Write,
    path::PathBuf,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::{runtime::Builder, sync::mpsc};

const RATE_INC_PER_SEC: u64 = 1000;
const REQ_ISSUE_SLACK_MS: u64 = 100;

type Result<T> = core::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Clone, Copy, ValueEnum)]
enum RequestType {
    Matmul,
    Compute,
    Io,
}

impl fmt::Display for RequestType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_possible_value().unwrap().get_name())
    }
}

#[derive(Parser)]
struct Args {
    /// IP address to issue requests to
    #[arg(long, default_value_t = String::from("localhost"))]
    ip: String,

    /// Number of seconds to run measurement for
    #[arg(short, long, default_value_t = 1)]
    duration: u64,

    /// Number of requests per second to issue, 0 means as fast as possible
    #[arg(short, long, default_value_t = 1)]
    rate: u64,

    /// The amount of users (maximum number of concurrent requests)
    #[arg(long, default_value_t = 10000)]
    num_users: usize,

    /// Request timeout in milliseconds
    #[arg(long, default_value_t = 10000)]
    timeout: u64,

    // Disable warmup phase before measurement
    #[arg(long)]
    no_warmup: bool,

    /// Path to output results
    #[arg(long)]
    output_file: Option<PathBuf>,

    /// What kind of requests to send
    #[arg(long, default_value_t = RequestType::Matmul)]
    request_type: RequestType,

    /// Size (N) of the NxN matrix to multiply / number of iterations to compute
    #[arg(long, default_value_t = 128)]
    input_size: u64,

    /// The IP of the HTTP storage server (used only for the composition experiment)
    #[arg(long, default_value_t = String::from("localhost"))]
    storage_ip: String,

    /// The average percentage of hot requests to issue
    #[arg(long, default_value_t = 1.0)]
    hot_percent: f64,
}

struct HotGenerator {
    hot_percent: f64,
    request_counter: f64,
}

impl HotGenerator {
    fn new(hot_percent: f64) -> Self {
        assert!(hot_percent >= 0.0);
        assert!(hot_percent <= 1.0);
        Self {
            hot_percent,
            request_counter: rand::random(),
        }
    }

    fn next(&mut self) -> bool {
        self.request_counter += self.hot_percent;
        if self.request_counter >= 1.0 {
            self.request_counter -= 1.0;
            true
        } else {
            false
        }
    }
}

#[derive(Debug)]
struct Record {
    start: SystemTime,
    end: SystemTime,
    url: String,
    timeout: bool,
    error: bool,
    status: Option<StatusCode>,
}

impl Record {
    fn start_time(&self) -> Duration {
        self.start.duration_since(UNIX_EPOCH).unwrap()
    }

    fn duration(&self) -> Duration {
        self.end.duration_since(self.start).unwrap()
    }
}

struct BenchLog {
    records: Vec<Record>,
    timeouts: usize,
    errors: usize,
}

impl BenchLog {
    fn new(num_records: usize) -> Self {
        Self {
            records: Vec::with_capacity(num_records),
            timeouts: 0,
            errors: 0,
        }
    }

    fn add_record(&mut self, record: Record) {
        if record.timeout {
            self.timeouts += 1;
        }
        if record.error {
            self.errors += 1;
        }
        self.records.push(record);
    }

    fn total(&self) -> usize {
        self.records.len()
    }

    fn errors(&self) -> usize {
        self.timeouts + self.errors
    }

    fn latencies(&self, percentages: &[f64]) -> Vec<Duration> {
        let mut latency: Vec<_> = self.records.iter().map(|t| t.duration()).collect();
        latency.sort();
        percentages
            .iter()
            .map(|p| {
                latency
                    .get(((latency.len() as f64 * p - 1.0) / 100.0) as usize)
                    .cloned()
                    .unwrap_or_default()
            })
            .collect()
    }
}

fn main() -> Result<()> {
    env_logger::init();
    let args = Args::parse();
    let rt = Builder::new_multi_thread().enable_all().build()?;
    rt.block_on(tokio_main(args))
}

// #[tokio::main]
async fn tokio_main(args: Args) -> Result<()> {
    let mut hot_gen = HotGenerator::new(args.hot_percent);
    let expected_checksum = args.request_type.checksum(args.input_size);

    let client = Client::builder()
        .timeout(Duration::from_millis(args.timeout))
        .build()
        .unwrap();

    let mut rate_per_sec = if args.no_warmup {
        vec![]
    } else {
        (RATE_INC_PER_SEC..args.rate)
            .step_by(RATE_INC_PER_SEC as usize)
            .collect()
    };
    let num_warmup = rate_per_sec.iter().sum::<u64>() as usize;
    rate_per_sec.extend(std::iter::repeat(args.rate).take(args.duration as usize));
    let starts = ConstGen::new(rate_per_sec);
    let mut bench_log = BenchLog::new(starts.expected_len() + 1);

    let (step_tx, mut step_rx) = mpsc::channel(100);
    let (user_tx, mut user_rx) = mpsc::channel(args.num_users);
    let (record_tx, mut record_rx) = mpsc::channel(100);

    let task_timer = tokio::task::spawn_blocking(move || {
        let base = Instant::now();
        if args.rate == 0 {
            while base.elapsed().as_secs() < args.duration {
                step_tx.blocking_send(()).unwrap();
            }
        } else {
            for start in starts {
                let next = base + start;
                if next.elapsed() > Duration::from_millis(REQ_ISSUE_SLACK_MS) {
                    warn!("Could not keep up with needed rate, canceling experiment");
                    let msg: Box<dyn std::error::Error + Send + Sync> = "Could not keep up".into();
                    return Err(msg);
                }
                // higher precision than tokio::time::sleep
                std::thread::sleep(next - Instant::now());
                step_tx.blocking_send(()).unwrap();
            }
        }
        info!("Started all requests in {:?}", base.elapsed());
        Ok(())
    });

    for user_id in 0..args.num_users {
        user_tx.send(user_id).await.unwrap();
    }

    tokio::spawn(async move {
        while let Some(_) = step_rx.recv().await {
            let is_hot = hot_gen.next();
            let url = args.request_type.url(&args.ip, is_hot);
            let body = args.request_type.body(args.input_size, &args.storage_ip);
            let request = client.post(&url).body(body);

            let user_id = user_rx.recv().await.unwrap();
            let user_tx = user_tx.clone();
            let record_tx = record_tx.clone();
            tokio::spawn(async move {
                let start = SystemTime::now();
                let result = request.send().await;
                let end = SystemTime::now();

                let mut record = Record {
                    start,
                    end,
                    url,
                    timeout: false,
                    error: false,
                    status: None,
                };
                match result.and_then(|r| r.error_for_status()) {
                    Ok(r) => {
                        record.status = Some(r.status());
                        let body = r.bytes().await.unwrap();
                        assert_eq!(body.len(), 8);
                        let mut buf = [0u8; 8];
                        buf.copy_from_slice(&body[..8]);
                        let checksum = u64::from_be_bytes(buf);
                        assert_eq!(checksum, expected_checksum);
                    }
                    Err(e) => {
                        if e.is_timeout() {
                            record.timeout = true;
                            debug!("Request timed out");
                        } else {
                            record.error = true;
                            record.status = e.status();
                            warn!("Request error: {}", e);
                        }
                    }
                }
                // user_rx could be dropped first (when timer_tx is closed), so we don't check the result here.
                let _ = user_tx.send(user_id).await;
                record_tx.send(record).await.unwrap();
            });
        }
    });

    let mut num_received = 0;
    while let Some(record) = record_rx.recv().await {
        num_received += 1;
        if num_received > num_warmup {
            bench_log.add_record(record);
        }
    }

    if let Some(results_path) = args.output_file {
        let mut file = File::create(results_path)?;
        writeln!(
            file,
            "instance,startTime,responseTime,connectionTimeout,functionTimeout,statusCode",
        )?;
        for record in &bench_log.records {
            writeln!(
                file,
                "{},{},{},{},{},{}",
                record.url,
                record.start_time().as_micros(),
                record.duration().as_micros(),
                record.timeout,
                record.error,
                record.status.map_or(0, |s| s.as_u16()),
            )?;
        }
    }

    let num_total = bench_log.total();
    let num_errors = bench_log.errors();
    println!("Total: {}, Errors: {}", num_total, num_errors);
    let percentages = [50.0, 90.0, 95.0, 99.0, 99.9, 100.0];
    let latencies = bench_log.latencies(&percentages);
    println!("Latency percentiles (in us):");
    for (p, l) in percentages.into_iter().zip(latencies) {
        println!("{:5}% -- {}\t", p, l.as_micros());
    }
    // required by the experiment script
    task_timer.await??;
    println!(
        "error%=\"{}\" goodput=\"{}\"",
        num_errors as f64 / num_total as f64,
        (num_total - num_errors) as f64 / args.duration as f64,
    );

    Ok(())
}
