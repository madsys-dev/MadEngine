// this is a test for basic create and remove
// initialization as well

use async_spdk::{event::app_stop, *};
use log::*;
use mad_engine::FileEngine;
use tokio::time::Duration;

const PATH: &str = "data";

#[tokio::main]
async fn main() {
    env_logger::init();
    let (mut handle, mut opts) = FileEngine::new(
        PATH,
        std::env::args().nth(1).expect("expect config file"),
        "0x11",
        "Nvme0n1",
        "Nvme1n1",
        1,
        "test1",
        4096,
        1,
    )
    .await
    .unwrap();
    info!("get handle success");
    handle.create("file1".to_string()).unwrap();
    info!("create file pass...");
    handle.remove("file1".to_string()).unwrap();
    info!("remove file pass...");
    handle.unload_bs().await.unwrap();
    info!("unload blobstore pass...");
    // handle.close_engine().unwrap();
    drop(handle);
    tokio::time::sleep(Duration::from_secs(1)).await;
    // info!("close engine pass...");
    opts.finish();
    info!("close engine pass...");
}
