// this is a test for multiple read and write

use async_spdk::{event::app_stop, *};
use log::*;
use mad_engine::*;

const DATA_LEN: usize = 6144;
const PATH: &str = "data";
use tokio::time::Duration;

/*
    |0-----------3800-----4000----4099----4199----------6143| <6144 in total>
    |<-------------------------write----------------------->|
                           |<-----read----->|
                  |<-----write----->|
                           |<-----read----->|
*/

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
        "test4",
        4096,
        32,
        false,
    )
    .await
    .unwrap();
    info!("get handle success");
    handle.create("file4".to_string()).unwrap();
    info!("create file4 pass...");
    let mut buf1 = vec![0u8; DATA_LEN];
    for i in 0..DATA_LEN {
        buf1[i] = (i % 256) as u8;
    }
    info!("init buffer with 0,1,2..255 pass...");
    handle
        .write("file4".to_owned(), 0, buf1.as_ref())
        .await
        .unwrap();
    info!("first write pass...");
    let mut buf2 = vec![0u8; 200];
    handle
        .read("file4".to_owned(), 4000, buf2.as_mut())
        .await
        .unwrap();
    info!("first read pass...");
    for i in 4000..4200 {
        // if i >= 4150 && i <= 4170{
        //     info!("position {} = {}", i, buf2[i-4000]);
        // }
        if buf1[i] != buf2[i - 4000] {
            error!("data mismatch on position: {}", i);
        }
    }
    info!("first data match...");
    let buf3 = vec![13u8; 300];
    handle
        .write("file4".to_owned(), 3800, buf3.as_ref())
        .await
        .unwrap();
    info!("second write pass...");
    handle
        .read("file4".to_owned(), 4000, buf2.as_mut())
        .await
        .unwrap();
    info!("second read pass...");
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
    info!("second data match...");
    handle.remove("file4".to_owned()).unwrap();
    handle.unload_bs().await.unwrap();
    info!("unload blobstore success");
    drop(handle);
    tokio::time::sleep(Duration::from_secs(1)).await;
    opts.finish();
    info!("close engine success");
}
