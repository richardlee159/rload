mod workload;

use clap::Parser;
use reqwest::Client;
use std::{
    fs::File,
    io::{BufRead, BufReader},
    process::exit,
    time::Duration,
};
use tokio::{
    runtime::Builder,
    sync::mpsc,
    time::{sleep_until, Instant},
};
use workload::compose_post;

type Result<T> = core::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Parser)]
struct Args {
    #[clap(short, long, parse(from_os_str))]
    trace_file: std::path::PathBuf,
}

#[derive(Debug)]
struct Trace {
    start: Instant,
    end: Instant,
}

fn main() -> Result<()> {
    let rt = Builder::new_current_thread().enable_all().build()?;
    rt.block_on(tokio_main())
}

// #[tokio::main]
async fn tokio_main() -> Result<()> {
    let args = Args::parse();
    let client = Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .unwrap();

    let file = File::open(args.trace_file)?;
    let starts: Vec<_> = BufReader::new(file)
        .lines()
        .map(|l| Duration::from_micros(l.unwrap().parse::<u64>().unwrap()))
        .collect();

    let mut traces = Vec::new();
    let mut status_errors = 0usize;
    let mut timeouts = 0usize;
    let (tx, mut rx) = mpsc::channel(100);

    let base = Instant::now();
    tokio::spawn(async move {
        for start in starts {
            sleep_until(base + start).await;
            let client = client.clone();
            let tx = tx.clone();
            tokio::spawn(async move {
                let request = client
                    .post("http://localhost:30001/wrk2-api/post/compose")
                    .body(compose_post());
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
                traces.push(trace);
            }
            Err(e) if e.is_status() => {
                eprintln!("response status error: {}", e.status().unwrap());
                status_errors += 1;
            }
            Err(e) if e.is_timeout() => {
                eprintln!("request timed out");
                timeouts += 1;
            }
            Err(e) => {
                eprintln!("{}", e);
                exit(-1);
            }
        }
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
