mod generator;
mod luascript;
mod workload;

#[macro_use]
extern crate log;

use clap::{ArgGroup, Parser};
use hdrhistogram::Histogram;
use reqwest::{Client, Url};
use std::{
    collections::HashMap,
    fs::File,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    runtime::Builder,
    sync::mpsc,
    time::{self, Instant},
};

use crate::{
    luascript::{build_request, new_state},
    workload::compose_post,
};

type Result<T> = core::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Parser)]
#[clap(version)]
#[clap(group(ArgGroup::new("generator").required(true).args(&["duration", "tracefile"])))]
struct Args {
    #[clap(short, long, default_value_t = 1, help = "Number of threads to use")]
    threads: usize,
    #[clap(short = 'f', long, parse(from_os_str))]
    tracefile: Option<PathBuf>,
    #[clap(short, long, requires = "rate", help = "Duration of test (s)")]
    duration: Option<u64>,
    #[clap(short, long, help = "Number of requests per second")]
    rate: Option<u64>,
    #[clap(short, long, parse(from_os_str))]
    script: Option<PathBuf>,
    #[clap(long, default_value_t = 10000, help = "Request timeout (ms)")]
    timeout: u64,
    #[clap(long, default_value_t = 1, help = "Number of times to replay tracefile")]
    replay: u32,
    #[clap(long, default_value_t = 60, help = "Interval to report statistics (s)")]
    stats_report_interval: u64,
    #[clap(long, parse(from_os_str))]
    summary: Option<PathBuf>,
    #[clap(long, name = "CSV_PREFIX")]
    csv: Option<String>,
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
    let rt = Builder::new_multi_thread()
        .worker_threads(args.threads)
        .enable_all()
        .build()?;
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

    let lua = if let Some(path) = args.script {
        Some(new_state(&path)?)
    } else {
        None
    };

    let mut latency_us_hist = Histogram::<u64>::new(3)?;
    let bench_log = Arc::new(Mutex::new(BenchLog::new()));
    let (tx, mut rx) = mpsc::channel(100);

    let base = Instant::now();
    let duration = starts[starts.len() - 1];
    tokio::spawn(async move {
        for i in 0..args.replay {
            for start in starts.iter().map(|&t| t + duration * i) {
                let url = args.url.clone();
                let request = if let Some(lua) = &lua {
                    build_request(&client, url, lua).unwrap()
                } else {
                    client.post(url).body(compose_post())
                };
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
        }
    });

    let percentages = [50.0, 75.0, 90.0, 95.0, 98.0, 99.0, 99.9, 100.0];

    if let Some(csv_prefix) = args.csv {
        let bench_log = bench_log.clone();
        tokio::spawn(async move {
            let path = format!("{}_stats_history.csv", csv_prefix);
            let mut wtr = csv::Writer::from_path(path).unwrap();

            let headers = [
                "Timestamp",
                "Requests",
                "Failures",
                "Total Requests",
                "Total Failures",
            ]
            .into_iter()
            .map(|h| h.to_string())
            .chain(percentages.map(|p| format!("{}%", p)));
            wtr.write_record(headers).unwrap();
            wtr.flush().unwrap();

            let mut total_requests = 0;
            let mut total_failures = 0;

            let mut interval =
                time::interval_at(base, Duration::from_secs(args.stats_report_interval));
            loop {
                interval.tick().await;

                let mut bench_log_guard = bench_log.lock().unwrap();
                let timestamp = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs() as usize;
                let successes = bench_log_guard.successes();
                let errors = bench_log_guard.errors();
                let latencies = bench_log_guard.latencies(&percentages);
                bench_log_guard.clear();

                let requests = successes + errors;
                let failures = errors;
                total_requests += requests;
                total_failures += failures;

                let bodies = [
                    timestamp,
                    requests,
                    failures,
                    total_requests,
                    total_failures,
                ]
                .into_iter()
                .map(|n| n.to_string())
                .chain(latencies.into_iter().map(|t| t.as_millis().to_string()));
                wtr.write_record(bodies).unwrap();
                wtr.flush().unwrap();
            }
        });
    }

    while let Some(result) = rx.recv().await {
        match result {
            Ok(trace) => {
                latency_us_hist.record(trace.duration().as_micros() as u64)?;
                bench_log.lock().unwrap().update_trace(trace);
            }
            Err(e) => {
                warn!("{}", e);
                bench_log.lock().unwrap().update_err(e);
            }
        }
    }

    let tail_latency_ms: HashMap<_, _> = percentages
        .into_iter()
        .map(|p| {
            (
                format!("{}%", p),
                latency_us_hist.value_at_percentile(p) as f64 / 1000.0,
            )
        })
        .collect();
    if let Some(path) = args.summary {
        let file = File::create(path)?;
        serde_json::to_writer(file, &tail_latency_ms)?;
    }

    Ok(())
}
