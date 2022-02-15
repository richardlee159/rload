mod workload;

use clap::Parser;
use reqwest::Client;
use std::time::Duration;
use tokio::{
    runtime::Builder,
    time::{self, Instant},
};
use workload::compose_post;

type Result<T> = core::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Parser)]
struct Args {
    #[clap(short, long)]
    duration: u64,
    #[clap(short, long)]
    rate: u64,
}

fn main() -> Result<()> {
    let rt = Builder::new_current_thread().enable_all().build()?;
    rt.block_on(tokio_main())
}

// #[tokio::main]
async fn tokio_main() -> Result<()> {
    let args = Args::parse();

    let deadline = Instant::now() + Duration::from_secs(args.duration);
    let client = Client::new();
    let mut interval = time::interval(Duration::from_millis(1000 / args.rate));

    loop {
        if Instant::now() > deadline {
            break;
        }
        let client = client.clone();
        tokio::spawn(async move {
            let _res = client
                .post("http://localhost:30001/wrk2-api/post/compose")
                .body(compose_post())
                // .body(r#"username=username_685&user_id=685&text=vRG8Rb9atdlBN8v3Nxo130RdTlIVHuue1bXNQDjyfA0W3fPFfzCIthQG0xOZFxdGMPOq1BXBpLKkm1QvfKNOgJJhac9Y4GYRXE0bZQR7zy6CwbL4dsRiJEIcdlt8Y1ZZxU1vx3MwYOp8eauoMAOPkZxbn7DFPm4mCZypSmk4iNDnOLG4EDif0JopzVggOmWB9iyuNb3ovXRAgKRVi8nIDvqln09BBFkesIGQU6RCvHLaiz86p3n0qXXXi1wuNryB @username_394 http://zrOREwRoIf2w4W0Un4k1RXDYueUjTNxgm33lhUheIIsyJoPlEOOyQbX00xK4FYFx http://mIof2m1mWWiUzmmX6pJ1prtM0tGHLaSaaYQjwqfZGpXSNGpyx9ifSoXUMYCDfl4N http://RlZZneFW4Xydw7gTgyB65PfvFHzT9p5ERh5q3UcYEeXU6vQbTXeJ2xURS94DmSTO http://lkh7f4cSsJIVUU91ZldfgN2mDS7YUHdJeUTKt5SxVRFv6WEUwFXzg2VhmhkJp3xW&media_ids=["956985607964169601","916647042156843968"]&media_types=["png","png"]&post_type=0"#)
                .send()
                .await
                .unwrap();
        });
        interval.tick().await;
        // time::sleep(Duration::from_millis(10)).await;
    }

    Ok(())
}
