mod workload;

use clap::Parser;
use reqwest::Client;
use std::{
    fs::File,
    io::{BufRead, BufReader},
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    runtime::Builder,
    time::{sleep_until, Instant},
};
use workload::compose_post;

type Result<T> = core::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Parser)]
struct Args {
    #[clap(short, long, parse(from_os_str))]
    trace_file: std::path::PathBuf,
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

    let traces = Arc::new(Mutex::new(Vec::new()));
    let errors = Arc::new(Mutex::new(0usize));

    let base = Instant::now();
    for start in starts {
        sleep_until(base + start).await;
        let client = client.clone();
        let traces = traces.clone();
        let errors = errors.clone();
        tokio::spawn(async move {
            let request = client
                .post("http://localhost:30001/wrk2-api/post/compose")
                .body(compose_post());

            let start = Instant::now();
            let response = request.send().await.unwrap();
            let end = Instant::now();

            let status = response.status();
            if status.is_success() | status.is_redirection() {
                traces.lock().unwrap().push((start, end));
            } else {
                eprintln!("response error: {}", status);
                *errors.lock().unwrap() += 1;
            }
        });
    }

    let traces = traces.lock().unwrap();
    let errors = errors.lock().unwrap();
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
