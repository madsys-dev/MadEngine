// this is a test for basic create and remove
// initialization as well

use async_spdk::*;
use log::*;
use mad_engine::*;

fn main() {
    env_logger::init();
    event::AppOpts::new()
        .name("test1")
        .config_file(&std::env::args().nth(1).expect("expect config file"))
        .block_on(test1_helper("mad_engine"))
        .unwrap();
}

async fn test1_helper(name: &str) -> std::result::Result<(), EngineError> {
    Ok(())
}
