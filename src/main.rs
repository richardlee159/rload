mod generator;
mod workload;

#[macro_use]
extern crate log;

use clap::{ArgGroup, Parser};
use hdrhistogram::Histogram;
use reqwest::{Client, Url};
use std::{path::PathBuf, time::Duration};
use tokio::{
    runtime::Builder,
    sync::mpsc,
    time::{self, Instant},
};

use crate::workload::matmul;

type Result<T> = core::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Parser)]
#[clap(version)]
#[clap(group(ArgGroup::new("generator").required(true).args(&["duration", "tracefile"])))]
struct Args {
    #[clap(short = 'f', long, parse(from_os_str))]
    tracefile: Option<PathBuf>,
    #[clap(short, long, requires = "rate", help = "Duration of test (s)")]
    duration: Option<u64>,
    #[clap(short, long, help = "Number of requests per second")]
    rate: Option<u64>,
    #[clap(long, default_value_t = 10000, help = "Request timeout (ms)")]
    timeout: u64,
    #[clap(long, parse(from_os_str))]
    summary: Option<PathBuf>,
    url: Url,
}

#[derive(Debug)]
struct Trace {
    start: Instant,
    end: Instant,
}

impl Trace {
    fn duration(&self) -> Duration {
        self.end - self.start
    }
}

struct BenchLog {
    traces: Vec<Trace>,
    timeouts: usize,
    status_errors: usize,
    connect_errors: usize,
    other_errors: usize,
}

impl BenchLog {
    fn new() -> Self {
        Self {
            traces: Vec::new(),
            timeouts: 0,
            status_errors: 0,
            connect_errors: 0,
            other_errors: 0,
        }
    }

    fn update_trace(&mut self, trace: Trace) {
        self.traces.push(trace);
    }

    fn update_err(&mut self, err: reqwest::Error) {
        match err {
            e if e.is_timeout() => {
                self.timeouts += 1;
            }
            e if e.is_status() => {
                self.status_errors += 1;
            }
            e if e.is_connect() => {
                self.connect_errors += 1;
            }
            _ => {
                self.other_errors += 1;
            }
        }
    }

    fn clear(&mut self) {
        self.traces.clear();
        self.timeouts = 0;
        self.status_errors = 0;
        self.connect_errors = 0;
        self.other_errors = 0;
    }

    fn successes(&self) -> usize {
        self.traces.len()
    }

    fn errors(&self) -> usize {
        self.timeouts + self.status_errors + self.connect_errors + self.other_errors
    }

    fn latencies(&self, percentages: &[f64]) -> Vec<Duration> {
        let mut latency: Vec<_> = self.traces.iter().map(|t| t.duration()).collect();
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
    let client = Client::builder()
        .timeout(Duration::from_millis(args.timeout))
        .build()
        .unwrap();

    let starts = if let Some(path) = args.tracefile {
        generator::new_tracefile(path)
    } else {
        let duration = Duration::from_secs(args.duration.unwrap());
        let rate = args.rate.unwrap();
        generator::new_exp(duration, rate)
    };

    let mut latency_us_hist = Histogram::<u64>::new(3)?;
    let mut bench_log = BenchLog::new();
    let (tx, mut rx) = mpsc::channel(100);

    let base = Instant::now();
    tokio::spawn(async move {
        for start in starts {
            let url = args.url.clone();
            let request = client.post(url).body(matmul(1));
            let tx = tx.clone();
            time::sleep_until(base + start).await;
            tokio::spawn(async move {
                let start = Instant::now();
                let result = request.send().await;
                let end = Instant::now();

                tx.send(match result {
                    Ok(r) => r.error_for_status().map(|_| Trace { start, end }),
                    Err(e) => Err(e),
                })
                .await
                .unwrap();
            });
        }
    });

    while let Some(result) = rx.recv().await {
        match result {
            Ok(trace) => {
                latency_us_hist.record(trace.duration().as_micros() as u64)?;
                bench_log.update_trace(trace);
            }
            Err(e) => {
                warn!("{}", e);
                bench_log.update_err(e);
            }
        }
    }

    println!(
        "Successes: {}, Errors: {}",
        bench_log.successes(),
        bench_log.errors()
    );
    let percentages = [50.0, 90.0, 95.0, 99.0, 99.9, 100.0];
    for p in percentages {
        print!("{}%: {}, ", p, latency_us_hist.value_at_percentile(p));
    }

    Ok(())
}
