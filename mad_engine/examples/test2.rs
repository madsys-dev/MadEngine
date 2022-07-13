// this is a test for basic read and write
// write a file no more than 1 page then read

use async_spdk::*;
use log::*;
use mad_engine::*;

// read write data length
const DATA_LEN: usize = 512;

fn main() {
    env_logger::init();
    event::AppOpts::new()
        .name("test1")
        .config_file(&std::env::args().nth(1).expect("expect config file"))
        .block_on(test1_helper("mad_engine"))
        .unwrap();
}

async fn test1_helper(name: &str) -> std::result::Result<(), EngineError> {
    let handle = MadEngineHandle::new("data", name).await.unwrap();
    handle.create("file2".to_string()).unwrap();
    info!("create file2 succeed...");
    let mut buf: Vec<u8> = vec![0u8; DATA_LEN];
    for i in 0..DATA_LEN {
        buf[i] = i as u8;
    }
    info!("init buf succeed...");
    handle
        .write("file2".to_string(), 0, buf.as_ref())
        .await
        .unwrap();
    info!("write file2 succeed...");
    let mut buf2 = vec![0u8; DATA_LEN];
    handle
        .read("file2".to_string(), 0, buf2.as_mut())
        .await
        .unwrap();
    info!("read file2 succeed...");
    for i in 0..DATA_LEN{
        if buf[i] != buf2[i]{
            error!("data mismatch on {}!", i);
        }
    }
    info!("data match...");
    handle.remove("file2".to_string()).unwrap();
    info!("remove file2 succeed...");
    handle.unload().await.unwrap();
    info!("basic read write succeed...");
    Ok(())
}
