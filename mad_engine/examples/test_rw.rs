use tokio::time::Duration;
use async_spdk::env;

use log::*;
use mad_engine::{BsBindOpts, EngineOpts};

#[tokio::main]
async fn main() {
    env_logger::init();
    let config = std::env::args().nth(1).expect("no config file");
    let mut opts = EngineOpts::default();
    opts.set_reactor_mask("0x3");
    opts.set_blobfs("Malloc0");
    // opts.set_blobfs("Nvme0n1");
    opts.set_blobstore(vec![BsBindOpts {
        bdev_name: "Malloc1".to_string(),
        // bdev_name: "Nvme1n1".to_string(),
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

    let bid = be.create_blob().await.unwrap();
    info!("Blob Create Success");

    let blob = be.open_blob(bid).await.unwrap();
    info!("Blob Open Success");

    be.resize_blob(blob, 1).await.unwrap();
    info!("Resize Success");

    be.sync_blob(blob).await.unwrap();
    info!("Sync Success");

    let io_unit_size = 512;

    let mut write_buf = env::DmaBuf::alloc(io_unit_size as usize, 0x1000);
    write_buf.as_mut().fill(0x5a);
    info!("Write buff success");
    
    be.write(0, blob, write_buf.as_ref()).await.unwrap();
    info!("Write Success");

    let mut read_buf = env::DmaBuf::alloc(io_unit_size as usize, 0x1000);

    be.read(0, blob, read_buf.as_mut()).await.unwrap();
    info!("Read Success");

    if write_buf.as_ref() != read_buf.as_ref() {
        error!("Error in data compare");
    } else {
        info!("read SUCCESS and data matches!");
    }

    be.close_blob(blob).await.unwrap();
    info!("Close Success");

    be.delete_blob(bid).await.unwrap();
    info!("Blob Delete Success");

    be.unload().await;
    info!("unload success");
    opts.finish();
    info!("App wait fot close");
}
