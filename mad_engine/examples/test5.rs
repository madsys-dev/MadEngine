// this is an integration test for mad_engine
// basically an aggregation of test1-4

use async_spdk::*;
use log::*;
use mad_engine::*;
use tokio::time::Duration;

const DATA_LEN2: usize = 512;
const DATA_LEN3: usize = 5120;
const DATA_LEN4: usize = 6144;

const PATH: &str = "data";

#[tokio::main]
async fn main(){
    env_logger::init();
    let (mut handle, mut opts) = FileEngine::new(
        PATH,
        std::env::args().nth(1).expect("expect config file"),
        "0x11",
        "Nvme0n1",
        "Nvme1n1",
        1,
        "test5",
        4096,
        1,
    )
    .await
    .unwrap();
    info!("get handle success");

    handle.create("file1".to_string()).unwrap();
    handle.remove("file1".to_string()).unwrap();
    info!("====== test1 pass...");

    handle.create("file2".to_string()).unwrap();
    let mut buf: Vec<u8> = vec![0u8; DATA_LEN2];
    for i in 0..DATA_LEN2 {
        buf[i] = i as u8;
    }
    handle
        .write("file2".to_string(), 0, buf.as_ref())
        .await
        .unwrap();
    let mut buf2 = vec![0u8; DATA_LEN2];
    handle
        .read("file2".to_string(), 0, buf2.as_mut())
        .await
        .unwrap();
    for i in 0..DATA_LEN2 {
        if buf[i] != buf2[i] {
            error!("data mismatch on {}!", i);
        }
    }
    handle.remove("file2".into()).unwrap();
    info!("====== test2 pass...");

    handle.create("file3".into()).unwrap();
    let mut buf: Vec<u8> = vec![0u8; DATA_LEN3];
    for i in 0..DATA_LEN3 {
        buf[i] = i as u8;
    }
    handle
        .write("file3".to_string(), 0, buf.as_ref())
        .await
        .unwrap();
    let mut buf2 = vec![0u8; 200];
    let offset = 4000;
    handle
        .read("file3".to_string(), offset, buf2.as_mut())
        .await
        .unwrap();
    for i in offset..offset + 200 {
        if buf[i as usize] != buf2[i as usize - offset as usize] {
            error!("data mismatch on {}!", i);
        }
    }
    handle.remove("file3".into()).unwrap();
    info!("====== test3 pass...");

    handle.create("file4".to_string()).unwrap();
    let mut buf1 = vec![0u8; DATA_LEN4];
    for i in 0..DATA_LEN4 {
        buf1[i] = (i % 256) as u8;
    }
    handle
        .write("file4".to_owned(), 0, buf1.as_ref())
        .await
        .unwrap();
    let mut buf2 = vec![0u8; 200];
    handle
        .read("file4".to_owned(), 4000, buf2.as_mut())
        .await
        .unwrap();
    for i in 4000..4200 {
        if buf1[i] != buf2[i - 4000] {
            error!("data mismatch on position: {}", i);
        }
    }
    let buf3 = vec![13u8; 300];
    handle
        .write("file4".to_owned(), 3800, buf3.as_ref())
        .await
        .unwrap();
    handle
        .read("file4".to_owned(), 4000, buf2.as_mut())
        .await
        .unwrap();
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
    handle.remove("file4".to_owned()).unwrap();
    info!("====== test4 pass...");

    handle.unload_bs().await.unwrap();
    drop(handle);
    tokio::time::sleep(Duration::from_secs(1)).await;
    opts.finish();

    info!("====== env close");

}



