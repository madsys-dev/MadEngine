// this is a test for data recovery if mad_engine is shutdown quietly

use log::*;
use mad_engine::*;
use tokio::time::Duration;

const DATA_LEN: usize = 6144;
const PATH: &str = "data";

#[tokio::main]
async fn main() {
    env_logger::init();
    let mut buf1 = vec![0u8; DATA_LEN];
    for i in 0..DATA_LEN {
        buf1[i] = (i % 256) as u8;
    }
    let mut buf2 = vec![0u8; 200];

    info!("=============restore=============");
    tokio::time::sleep(Duration::from_secs(2)).await;

    let (mut handle, mut opts) = FileEngine::new(
        PATH,
        std::env::args().nth(1).expect("expect config file"),
        "0x3",
        "Nvme0n1",
        "Nvme1n1",
        1,
        "test6",
        4096,
        1,
        true,
    )
    .await
    .unwrap();
    info!("second get handle success");

    let buf3 = vec![13u8; 300];
    handle
        .write("file6".to_owned(), 3800, buf3.as_ref())
        .await
        .unwrap();
    info!("second write success");
    handle
        .read("file6".to_owned(), 4000, buf2.as_mut())
        .await
        .unwrap();
    info!("second read success");
    for i in 4000..4100 {
        if buf2[i - 4000] != 13 {
            error!("data mismatch on position: {}", i);
        }
    }
    for i in 4100..4200 {
        if buf2[i - 4000] != buf1[i] {
            error!("data mismatch on position: {}", i);
        }
    }
    info!("second data match");
    handle.remove("file6".to_owned()).unwrap();

    handle.unload_bs().await.unwrap();
    info!("unload blobstore success");
    drop(handle);
    tokio::time::sleep(Duration::from_secs(1)).await;
    opts.finish();
    info!("test6 pass...");
}
