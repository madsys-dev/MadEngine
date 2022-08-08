// this is a test for cross-boundary read and write

use async_spdk::*;
use log::*;
use mad_engine::*;

// read write data length
const DATA_LEN: usize = 5120;

fn main() {
    env_logger::init();
    event::AppOpts::new()
        .name("test3")
        .config_file(&std::env::args().nth(1).expect("expect config file"))
        .block_on(test3_helper("Nvme0n1"))
        .unwrap();
}

async fn test3_helper(name: &str) -> std::result::Result<(), EngineError> {
    let mut handle = MadEngineHandle::new("data", name).await.unwrap();
    handle.create("file3".to_string()).unwrap();
    info!("create file3 succeed...");
    let mut buf: Vec<u8> = vec![0u8; DATA_LEN];
    for i in 0..DATA_LEN {
        buf[i] = i as u8;
    }
    info!("init buf succeed...");
    handle
        .write("file3".to_string(), 0, buf.as_ref())
        .await
        .unwrap();
    info!("write file3 succeed...");
    let mut buf2 = vec![0u8; 200];
    let offset = 4000;
    handle
        .read("file3".to_string(), offset, buf2.as_mut())
        .await
        .unwrap();
    info!("read file3 succeed...");
    for i in offset..offset + 200 {
        if buf[i as usize] != buf2[i as usize - offset as usize] {
            error!("data mismatch on {}!", i);
        }
    }
    info!("data match...");
    handle.remove("file3".to_string()).unwrap();
    info!("remove file3 succeed...");
    handle.unload().await.unwrap();
    info!("cross-boundary read write succeed...");
    Ok(())
}
