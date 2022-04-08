mod luascript;
mod workload;

#[macro_use]
extern crate log;

use clap::Parser;
use reqwest::{Client, Url};
use serde_json::json;
use std::{
    fs::File,
    io::{BufRead, BufReader},
    path::PathBuf,
    time::Duration,
};
use tokio::{
    runtime::Builder,
    sync::mpsc,
    time::{sleep_until, Instant},
};

use crate::{
    luascript::{build_request, new_state},
    workload::compose_post,
};

type Result<T> = core::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Parser)]
#[clap(version)]
struct Args {
    #[clap(short = 'f', parse(from_os_str))]
    trace_file: PathBuf,
    #[clap(short, parse(from_os_str))]
    script: Option<PathBuf>,
    #[clap(long, default_value_t = 10000)]
    timeout: u64,
    #[clap(long, default_value_t = 1)]
    replay: u32,
    #[clap(long, default_value_t = 60)]
    stats_report_interval: u64,
    #[clap(long, default_value_t = 95)]
    stats_latency_percentage: usize,
    url: Url,
}

#[derive(Debug)]
struct Trace {
    start: Instant,
    end: Instant,
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

    fn latency_ms(&self, percentage: usize) -> f64 {
        let mut latency: Vec<_> = self.traces.iter().map(|t| t.end - t.start).collect();
        latency.sort();
        latency
            .get((latency.len() * percentage - 1) / 100)
            .cloned()
            .unwrap_or_default()
            .as_micros() as f64
            / 1000.0
    }
}

fn main() -> Result<()> {
    env_logger::init();
    let rt = Builder::new_current_thread().enable_all().build()?;
    rt.block_on(tokio_main())
}

// #[tokio::main]
async fn tokio_main() -> Result<()> {
    let args = Args::parse();
    let client = Client::builder()
        .timeout(Duration::from_millis(args.timeout))
        .build()
        .unwrap();

    let file = File::open(args.trace_file)?;
    let starts: Vec<_> = BufReader::new(file)
        .lines()
        .map(|l| Duration::from_micros(l.unwrap().parse::<u64>().unwrap()))
        .collect();

    let lua = if let Some(path) = args.script {
        Some(new_state(&path)?)
    } else {
        None
    };

    let mut bench_log = BenchLog::new();
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
                sleep_until(base + start).await;
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

    let mut last_report_time = Instant::now();
    while let Some(result) = rx.recv().await {
        match result {
            Ok(trace) => bench_log.update_trace(trace),
            Err(e) => {
                warn!("{}", e);
                bench_log.update_err(e);
            }
        }
        if last_report_time.elapsed() > Duration::from_secs(args.stats_report_interval) {
            last_report_time = Instant::now();
            let stats = json!({
                "successes": bench_log.successes(),
                "errors": bench_log.errors(),
                "latency": bench_log.latency_ms(args.stats_latency_percentage),
            });
            println!("{}", stats);
            bench_log.clear();
        }
    }

    Ok(())
}
