// this is a test for multiple read and write

use async_spdk::*;
use log::*;
use mad_engine::*;

const DATA_LEN: usize = 6144;

fn main() {
    env_logger::init();
    event::AppOpts::new()
        .name("test4")
        .config_file(&std::env::args().nth(1).expect("no such config file"))
        .block_on(test4_helper("mad_engine"))
        .unwrap();
}

async fn test4_helper(name: &str) -> std::result::Result<(), EngineError> {
    let handle = MadEngineHandle::new("data", name).await.unwrap();
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
    handle.unload().await.unwrap();
    info!("test4 pass...");
    Ok(())
}
