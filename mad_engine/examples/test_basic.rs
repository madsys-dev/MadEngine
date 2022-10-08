
use std::time::Duration;

use log::*;
use mad_engine::{EngineOpts, BsBindOpts};


#[tokio::main]
async fn main(){
    env_logger::init();
    let config = std::env::args().nth(1).expect("no config file");
    let mut opts = EngineOpts::default();
    opts.set_reactor_mask("0x3");
    opts.set_blobfs("Malloc0");
    opts.set_blobstore(vec![BsBindOpts{
        bdev_name: "Malloc1".to_string(), core: 1
    }]);
    info!("Set blobstores");
    opts.set_config_file(config);
    opts.set_name("Test Basic");
    opts.start_spdk();
    opts.ready();
    info!("got ready");
    
    let be = opts.create_be();
    info!("create_be success");

    tokio::time::sleep(Duration::from_secs(2));

    be.close().await;
    info!("close success");
    opts.finish();
    info!("App wait fot close");

}
