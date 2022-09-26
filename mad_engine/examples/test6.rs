// this is a test for data recovery if mad_engine is shutdown quietly

use std::time::Duration;

use async_spdk::{*, event::app_stop};
use log::*;
use mad_engine::*;

const DATA_LEN: usize = 6144;

fn main() {
    env_logger::init();
    event::AppOpts::new()
        .name("test6")
        .config_file(&std::env::args().nth(1).expect("no such config file"))
        .block_on(test6_helper("Nvme0n1"))
        .unwrap();
}

async fn test6_helper(name: &str) -> std::result::Result<(), EngineError> {
    let mut handle = MadEngineHandle::new("data", name).await.unwrap();
    handle.create("file6".to_string()).unwrap();
    info!("create file6 pass...");
    let mut buf1 = vec![0u8; DATA_LEN];
    for i in 0..DATA_LEN {
        buf1[i] = (i % 256) as u8;
    }
    info!("init buffer with 0,1,2..255 pass...");
    handle
        .write("file6".to_owned(), 0, buf1.as_ref())
        .await
        .unwrap();
    info!("first write pass...");
    let mut buf2 = vec![0u8; 200];
    handle
        .read("file6".to_owned(), 4000, buf2.as_mut())
        .await
        .unwrap();
    info!("first read pass...");
    for i in 4000..4200 {
        if buf1[i] != buf2[i - 4000] {
            error!("data mismatch on position: {}", i);
        }
    }
    info!("first data match...");
    handle.unload().await.unwrap();
    drop(handle);
    std::thread::sleep(Duration::from_secs(2));

    let mut handle = MadEngineHandle::new("data", name).await.unwrap();
    let buf3 = vec![13u8; 300];
    handle
        .write("file6".to_owned(), 3800, buf3.as_ref())
        .await
        .unwrap();
    info!("second write pass...");
    handle
        .read("file6".to_owned(), 4000, buf2.as_mut())
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
    handle.remove("file6".to_owned()).unwrap();
    handle.unload().await.unwrap();
    info!("test6 pass...");

    app_stop();
    info!("app stop");
    Ok(())
}
