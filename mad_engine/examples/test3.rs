// this is a test for cross-boundary read and write

use async_spdk::{event::app_stop, *};
use log::*;
use mad_engine::*;
use tokio::time::Duration;

// read write data length
const DATA_LEN: usize = 5120;
const PATH: &str = "data";

#[tokio::main]
async fn main() {
    env_logger::init();
    let (mut handle, mut opts) = FileEngine::new(
        PATH,
        std::env::args().nth(1).expect("expect config file"),
        "0x3",
        "Nvme0n1",
        "Nvme1n1",
        1,
        "test3",
        4096,
        32,
        false,
    )
    .await
    .unwrap();
    info!("get handle success");
    handle.create("file3".into()).unwrap();
    let mut buf: Vec<u8> = vec![0u8; DATA_LEN];
    for i in 0..DATA_LEN {
        buf[i] = i as u8;
    }
    info!("init buf success");
    handle
        .write("file3".to_string(), 0, buf.as_ref())
        .await
        .unwrap();
    info!("write file3 success");
    let mut buf2 = vec![0u8; 200];
    let offset = 4000;
    handle
        .read("file3".to_string(), offset, buf2.as_mut())
        .await
        .unwrap();
    info!("read file3 success");
    for i in offset..offset + 200 {
        if buf[i as usize] != buf2[i as usize - offset as usize] {
            error!("data mismatch on {}!", i);
        }
    }
    info!("data match!");
    handle.remove("file3".into()).unwrap();
    info!("remove file3 success");
    handle.unload_bs().await.unwrap();
    info!("unload blobstore success");
    drop(handle);
    tokio::time::sleep(Duration::from_secs(1)).await;
    opts.finish();
    info!("close engine success");
}
