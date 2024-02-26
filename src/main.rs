mod generator;
mod workload;

#[macro_use]
extern crate log;

use clap::{ArgGroup, Parser, ValueEnum};
use reqwest::{Client, StatusCode};
use std::{
    fs::File,
    io::Write,
    path::PathBuf,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    runtime::Builder,
    sync::mpsc,
    time::{self, Instant},
};

use crate::workload::matmul;

type Result<T> = core::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Clone, Copy, ValueEnum)]
enum RequestFormat {
    Matmul,
    Compute,
    Io,
}

#[derive(Parser)]
#[clap(version)]
#[clap(group(ArgGroup::new("generator").required(true).args(&["duration", "tracefile"])))]
struct Args {
    #[clap(long)]
    ip: String,
    #[clap(short = 'f', long)]
    tracefile: Option<PathBuf>,
    #[clap(short, long, requires = "rate", help = "Duration of test (s)")]
    duration: Option<u64>,
    #[clap(short, long, help = "Number of requests per second")]
    rate: Option<u64>,
    #[clap(long, default_value_t = 10000, help = "Request timeout (ms)")]
    timeout: u64,
    #[clap(long)]
    results_path: Option<PathBuf>,
    #[clap(long)]
    request_format: RequestFormat,
    #[clap(long)]
    input_size: u64,
    #[clap(long)]
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

    fn next(&mut self, request_format: RequestFormat) -> String {
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
            match request_format {
                RequestFormat::Matmul => "matmul",
                RequestFormat::Compute => "compute",
                RequestFormat::Io => "io",
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
    fn new() -> Self {
        Self {
            records: Vec::new(),
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

    let client = Client::builder()
        .timeout(Duration::from_millis(args.timeout))
        .build()
        .unwrap();

    let starts = if let Some(path) = args.tracefile {
        generator::new_tracefile(path)
    } else {
        let duration = Duration::from_secs(args.duration.unwrap());
        let rate = args.rate.unwrap();
        generator::new_const(duration, rate)
    };

    let mut bench_log = BenchLog::new();
    let (tx, mut rx) = mpsc::channel(100);

    let base = Instant::now();
    tokio::spawn(async move {
        for start in starts {
            let url = url_gen.next(args.request_format);
            let request = client.post(&url).body(match args.request_format {
                RequestFormat::Matmul => matmul(args.input_size),
                RequestFormat::Compute => vec![],
                RequestFormat::Io => vec![],
            });

            let tx = tx.clone();
            time::sleep_until(base + start).await;
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

    if let Some(results_path) = args.results_path {
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

    println!(
        "Total: {}, Errors: {}",
        bench_log.total(),
        bench_log.errors()
    );
    let percentages = [50.0, 90.0, 95.0, 99.0, 99.9, 100.0];
    let latencies = bench_log.latencies(&percentages);
    for p in percentages {
        print!("{}%\t", p);
    }
    println!();
    for l in latencies {
        print!("{}\t", l.as_micros());
    }
    println!();

    Ok(())
}
