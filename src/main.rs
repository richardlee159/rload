mod generator;
mod workload;

#[macro_use]
extern crate log;

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

use crate::workload::{matmul, matmul_checksum};

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

    /// Number of requests per second to ussue
    #[arg(short, long, default_value_t = 1)]
    rate: u64,

    /// Request timeout in milliseconds
    #[arg(long, default_value_t = 10000)]
    timeout: u64,

    /// Path to output results
    #[arg(long)]
    output_file: Option<PathBuf>,

    /// What kind of requests to send
    #[arg(long, default_value_t = RequestType::Matmul)]
    request_type: RequestType,

    /// Size (N) of the NxN matrix to multiply / number of iterations to compute
    #[arg(long, default_value_t = 128)]
    input_size: u64,

    /// The average percentage of hot requests to issue
    #[arg(long, default_value_t = 1.0)]
    hot_percent: f64,
}

struct UrlGenerator {
    hot_url: String,
    cold_url: String,
    hot_percent: f64,
    request_counter: f64,
}

impl UrlGenerator {
    fn new(ip: &str, hot_percent: f64) -> Self {
        assert!(hot_percent >= 0.0);
        assert!(hot_percent <= 1.0);
        let base_url = format!("http://{}:{}", ip, 8080);
        Self {
            hot_url: format!("{}/hot", base_url),
            cold_url: format!("{}/cold", base_url),
            hot_percent,
            request_counter: rand::random(),
        }
    }

    fn next(&mut self, request_type: RequestType) -> String {
        self.request_counter += self.hot_percent;
        let url = if self.request_counter >= 1.0 {
            self.request_counter -= 1.0;
            &self.hot_url
        } else {
            &self.cold_url
        };
        format!(
            "{}/{}",
            url,
            match request_type {
                RequestType::Matmul => "matmul",
                RequestType::Compute => "compute",
                RequestType::Io => "io",
            }
        )
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
    let mut url_gen = UrlGenerator::new(&args.ip, args.hot_percent);
    let expected_checksum = match args.request_type {
        RequestType::Matmul => matmul_checksum(args.input_size),
        RequestType::Compute => 0,
        RequestType::Io => 0,
    };

    let client = Client::builder()
        .timeout(Duration::from_millis(args.timeout))
        .build()
        .unwrap();

    let starts = generator::new_const(Duration::from_secs(args.duration), args.rate);
    let num_expected = starts.len();
    let mut bench_log = BenchLog::new(starts.len());
    let (tx, mut rx) = mpsc::channel(100);
    let (timer_tx, mut timer_rx) = mpsc::channel(100);

    tokio::task::spawn_blocking(move || {
        let base = Instant::now();
        for start in starts {
            let next = base + start;
            if next.elapsed() > Duration::from_millis(100) {
                warn!("Could not keep up with needed rate, canceling experiment");
                break;
            }
            // higher precision than tokio::time::sleep
            std::thread::sleep(next - Instant::now());
            timer_tx.blocking_send(()).unwrap();
        }
        info!("Started all requests in {:?}", base.elapsed());
    });

    tokio::spawn(async move {
        while let Some(_) = timer_rx.recv().await {
            let url = url_gen.next(args.request_type);
            let request = client.post(&url).body(match args.request_type {
                RequestType::Matmul => matmul(args.input_size),
                RequestType::Compute => vec![],
                RequestType::Io => vec![],
            });

            let tx = tx.clone();
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
                tx.send(record).await.unwrap();
            });
        }
    });

    while let Some(record) = rx.recv().await {
        bench_log.add_record(record);
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
    if num_total < num_expected {
        return Err("Could not keep up".into());
    }
    println!(
        "error%=\"{}\" goodput=\"{}\"",
        num_errors as f64 / num_total as f64,
        (num_total - num_errors) as f64 / args.duration as f64,
    );

    Ok(())
}
