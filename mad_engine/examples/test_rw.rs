use std::time::Duration;

use log::*;
use mad_engine::{BsBindOpts, EngineOpts};

#[tokio::main]
async fn main() {
    env_logger::init();
    let config = std::env::args().nth(1).expect("no config file");
    let mut opts = EngineOpts::default();
    opts.set_reactor_mask("0x3");
    opts.set_blobfs("Malloc0");
    opts.set_blobstore(vec![BsBindOpts {
        bdev_name: "Malloc1".to_string(),
        core: 1,
    }]);
    info!("Set blobstores");
    opts.set_config_file(config);
    opts.set_name("Test Basic");
    opts.start_spdk();
    opts.ready();
    info!("got ready");

    let be = opts.create_be();
    info!("create_be success");

    let blob_ = be.create_blob(1).await.unwrap();
    info!("Blob Create Success");

    tokio::time::sleep(Duration::from_secs(5)).await;

    be.delete_blob(blob.get_id().unwrap()).await;
    info!("Blob Delete Success");

    be.close().await;
    info!("close success");
    opts.finish();
    info!("App wait fot close");
}
