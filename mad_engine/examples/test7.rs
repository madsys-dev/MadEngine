// this is a test for spdk environment
// now this cannot work due to lack of united SPDK environment

use async_spdk::*;
use log::*;
use mad_engine::*;

fn main() {
    env_logger::init();
    event::AppOpts::new()
        .name("test7")
        .config_file(&std::env::args().nth(1).expect("expect config file"))
        .block_on(test1_helper("Nvme0n1", &std::env::args().nth(1).expect("expect config file")))
        .unwrap();
}

async fn test1_helper(name: &str, conf_file: &str) -> std::result::Result<(), EngineError> {
    info!("trying to start spdk environment");
    let handle = MadEngineHandle::new_spdk("data", conf_file, name).await.unwrap();
    handle.create("file1".to_string()).unwrap();
    info!("create pass...");
    handle.remove("file1".to_string()).unwrap();
    info!("remove pass...");
    handle.unload().await.unwrap();
    info!("unload succeed...");
    Ok(())
}