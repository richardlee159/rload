mod workload;

use clap::Parser;
use reqwest::Client;
use std::{
    fs::File,
    io::{BufRead, BufReader},
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

    let base = Instant::now();
    for start in starts {
        sleep_until(base + start).await;
        let client = client.clone();
        tokio::spawn(async move {
            let _res = client
                .post("http://localhost:30001/wrk2-api/post/compose")
                .body(compose_post())
                .send()
                .await
                .unwrap();
        });
    }

    Ok(())
}
