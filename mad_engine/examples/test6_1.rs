// this is a test for data recovery if mad_engine is shutdown quietly

use log::*;
use mad_engine::*;
use tokio::time::Duration;

const DATA_LEN: usize = 6144;
const PATH: &str = "data";

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
        "test6",
        4096,
        1,
        false,
    )
    .await
    .unwrap();
    info!("first init handle success");
    handle.create("file6".to_string()).unwrap();
    info!("create file6 success");
    let mut buf1 = vec![0u8; DATA_LEN];
    for i in 0..DATA_LEN {
        buf1[i] = (i % 256) as u8;
    }
    info!("init buffer with 0,1,2..255 success");
    handle
        .write("file6".to_owned(), 0, buf1.as_ref())
        .await
        .unwrap();
    info!("first write success");
    let mut buf2 = vec![0u8; 200];
    handle
        .read("file6".to_owned(), 4000, buf2.as_mut())
        .await
        .unwrap();
    info!("first read success");
    for i in 4000..4200 {
        if buf1[i] != buf2[i - 4000] {
            error!("data mismatch on position: {}", i);
        }
    }
    info!("first data match");
    handle.unload_bs().await.unwrap();
    info!("unload blobstore success");
    drop(handle);
    tokio::time::sleep(Duration::from_secs(1)).await;
    opts.finish();
    info!("close engine success");
}
