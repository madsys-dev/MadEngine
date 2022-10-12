use std::time::Duration;

use futures::TryFutureExt;
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

    let bid = be.create_blob(1).await.unwrap();
    info!("Blob Create Success");

    let blob = be.open_blob(bid).await.unwrap();
    info!("Blob Open Success");

    be.resize_blob(blob, 1).await.unwrap();
    info!("Resize Success");

    be.sync_blob(blob).await.unwrap();
    info!("Sync Success");

    be.close_blob(blob).await.unwrap();
    info!("Close Success");

    be.delete_blob(bid).await;
    info!("Blob Delete Success");

    be.unload().await;
    info!("unload success");
    opts.finish();
    info!("App wait fot close");
}
