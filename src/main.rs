mod luascript;
mod workload;

#[macro_use]
extern crate log;

use clap::Parser;
use reqwest::{Client, Url};
use std::{
    fs::File,
    io::{BufRead, BufReader},
    path::PathBuf,
    process::exit,
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
    url: Url,
}

#[derive(Debug)]
struct Trace {
    start: Instant,
    end: Instant,
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

    let mut traces = Vec::new();
    let mut status_errors = 0usize;
    let mut timeouts = 0usize;
    let mut other_errors = 0usize;
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

    while let Some(result) = rx.recv().await {
        match result {
            Ok(trace) => {
                traces.push(trace);
            }
            Err(e) if e.is_status() => {
                warn!("{}", e);
                status_errors += 1;
            }
            Err(e) if e.is_timeout() => {
                warn!("{}", e);
                timeouts += 1;
            }
            Err(e) if e.is_connect() => {
                error!("{}", e);
                exit(-1);
            }
            Err(e) => {
                warn!("{}", e);
                other_errors += 1;
            }
        }
    }

    eprintln!("successful responses: {}", traces.len());
    eprintln!("4xx or 5xx responses: {}", status_errors);
    eprintln!("timeouts: {}", timeouts);
    eprintln!("other errors: {}", other_errors);
    eprintln!("latency distribution:");
    let mut latency: Vec<_> = traces.iter().map(|t| t.end - t.start).collect();
    latency.sort();
    for percentage in [50, 90, 95, 99] {
        eprintln!(
            "  {}% {:7.2}ms",
            percentage,
            latency[(latency.len() * percentage - 1) / 100].as_micros() as f64 / 1000.0
        );
    }

    println!("successful responses: {}", traces.len());
    println!("4xx or 5xx responses: {}", status_errors);
    println!("timeouts: {}", timeouts);
    println!("traces (start_us, end_us):");
    for trace in traces.iter() {
        println!(
            "{}, {}",
            (trace.start - base).as_micros(),
            (trace.end - base).as_micros()
        );
    }

    Ok(())
}
