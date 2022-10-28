// this is a test for basic read and write
// write a file no more than 1 page then read


use log::*;
use mad_engine::*;
use tokio::time::Duration;

// read write data length
const DATA_LEN: usize = 512;

const PATH: &str = "data";

/*
    |------------------| <512B in total>
    |<------write----->|
    |<------read------>|
*/

#[tokio::main]
async fn main() {
    env_logger::init();
    let (handle, mut opts) = FileEngine::new(
        PATH,
        std::env::args().nth(1).expect("expect config file"),
        "0x3",
        "Nvme0n1",
        "Nvme1n1",
        1,
        "test2",
        4096,
        1,
        false,
    )
    .await
    .unwrap();
    info!("get handle success");
    handle.create("file2".to_string()).unwrap();
    info!("create file2 success");
    let mut buf: Vec<u8> = vec![0u8; DATA_LEN];
    for i in 0..DATA_LEN {
        buf[i] = i as u8;
    }
    info!("init buf success");
    handle
        .write("file2".to_string(), 0, buf.as_ref())
        .await
        .unwrap();
    info!("write file2 success");
    let mut buf2 = vec![0u8; DATA_LEN];
    handle
        .read("file2".to_string(), 0, buf2.as_mut())
        .await
        .unwrap();
    info!("read file2 success");
    for i in 0..DATA_LEN {
        if buf[i] != buf2[i] {
            error!("data mismatch on {}!", i);
        }
    }
    info!("data match!");
    handle.remove("file2".into()).unwrap();
    info!("remove file2 success");
    handle.unload_bs().await.unwrap();
    info!("unload blobstore success");
    drop(handle);
    tokio::time::sleep(Duration::from_secs(1)).await;
    opts.finish();
    info!("close engine success");
}
