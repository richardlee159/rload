mod workload;

use clap::Parser;
use reqwest::{Client, StatusCode};
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
    status: StatusCode,
}

fn main() -> Result<()> {
    let rt = Builder::new_current_thread().enable_all().build()?;
    rt.block_on(tokio_main())
}

// #[tokio::main]
async fn tokio_main() -> Result<()> {
    let args = Args::parse();
    let client = Client::new();

    let file = File::open(args.trace_file)?;
    let starts: Vec<_> = BufReader::new(file)
        .lines()
        .map(|l| Duration::from_millis(l.unwrap().parse::<u64>().unwrap()))
        .collect();

    let mut traces = Vec::new();
    let mut errors = 0usize;
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
                match request.send().await {
                    Ok(response) => {
                        let end = Instant::now();
                        let status = response.status();
                        let trace = Trace { start, end, status };
                        tx.send(trace).await.unwrap();
                    }
                    Err(e) => {
                        eprintln!("{}", e);
                        exit(-1);
                    }
                };
            });
        }
    });

    while let Some(trace) = rx.recv().await {
        if trace.status.is_success() | trace.status.is_redirection() {
            traces.push((trace.start, trace.end));
        } else {
            eprintln!("response error: {}", trace.status);
            errors += 1;
        }
    }

    println!("successful responses: {}", traces.len());
    println!("error (non-2xx or 3xx) responses: {}", errors);
    println!("traces (start_ms, end_ms):");
    for &(start, end) in traces.iter() {
        println!(
            "{}, {}",
            (start - base).as_millis(),
            (end - base).as_millis()
        );
    }

    Ok(())
}
