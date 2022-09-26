// this is a test for spdk environment
// now this cannot work due to lack of united SPDK environment

use async_spdk::{event::AppOpts, *};
use log::*;
use mad_engine::*;
use rocksdb::*;
use std::ffi::c_void;
use std::sync::Arc;

fn main() {
    env_logger::init();
    // let db = Env::rocksdb_create_spdk_env(
    //     "data",
    //     &std::env::args().nth(1).expect("expect config file"),
    //     "Nvme1n1",
    //     4096);
    // let mut db_opts = Options::default();
    // db_opts.create_if_missing(true);
    // db_opts.set_env(&env);
    // let path = "spdk_integration_test_dir";
    // let db = Arc::new(DB::open(&db_opts, path).expect("fail to open db"));

    let app_opts = event::AppOpts::new()
        .name("test7")
        .config_file(&std::env::args().nth(1).expect("expect config file"));
    // let opts_copy = app_opts.clone();
    app_opts
        .block_on(test8_helper_copy(
            // opts_copy,
            "Nvme1n1",
            "Nvme0n1",
            &std::env::args().nth(1).expect("expect config file"),
        ))
        .unwrap();
}

async fn test8_helper_copy(
    // db_opts: AppOpts,
    db_name: &str,
    data_name: &str,
    conf_file: &str,
) -> std::result::Result<(), EngineError> {
    info!("only start rocksdb");
    let path = "data";
    // let mut box_opts = Box::new(db_opts.get_opts());
    // let opts_raw = Box::into_raw(box_opts);

    let mut bs_dev_db = blob_bdev::BlobStoreBDev::create(db_name)?;
    let mut bs_dev_data = blob_bdev::BlobStoreBDev::create(data_name)?;
    info!("register bs_dev succeed");
    //let bs_db = blob::Blobstore::init(&mut bs_dev_db).await?;
    let bs_data = blob::Blobstore::init(&mut bs_dev_data).await?;
    info!("create two blobstore");

    bs_db.unload().await?;
    bs_data.unload().await?;
    info!("unload succeed");

    // box_opts = unsafe {Box::from_raw(opts_raw)} ;

    Ok(())
}

async fn test8_helper(
    opts: AppOpts,
    db_name: &str,
    data_name: &str,
    conf_file: &str,
) -> std::result::Result<(), EngineError> {
    info!("trying to start spdk environment");
    let handle = MadEngineHandle::new_spdk(opts, "data", conf_file, db_name, data_name)
        .await
        .unwrap();
    handle.create("file1".to_string()).unwrap();
    info!("create pass...");
    handle.remove("file1".to_string()).unwrap();
    info!("remove pass...");
    handle.unload().await.unwrap();
    info!("unload succeed...");
    Ok(())
}
