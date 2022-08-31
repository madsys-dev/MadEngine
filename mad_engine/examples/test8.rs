// this is a test for spdk environment
// now this cannot work due to lack of united SPDK environment

use async_spdk::{event::AppOpts, *};
use log::*;
use mad_engine::*;
use rocksdb::*;
use std::sync::Arc;
use std::ffi::c_void;

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
    let opts_copy = app_opts.clone();
    app_opts
        .block_on(test8_helper_copy(
            opts_copy,
            "Nvme1n1",
            "Nvme0n1",
            &std::env::args().nth(1).expect("expect config file"),
        ))
        .unwrap();
}

async fn test8_helper_copy(
    db_opts: AppOpts,
    db_name: &str,
    data_name: &str,
    conf_file: &str,
) -> std::result::Result<(), EngineError>{
    info!("only start rocksdb");
    let path = "data";
    let box_opts = Box::new(db_opts.get_opts());
    let opts_raw = Box::into_raw(box_opts) as *mut c_void;

    

    
    // let env = Env::rocksdb_use_spdk_env(
    //     opts_raw,
    //     path,
    //     conf_file,
    //     db_name,
    //     4096,
    // )
    // .expect("fail to initilize spdk environment");
    // info!("env create success");
    // // let env = Env::rocksdb_create_spdk_env(cpath, config_file, device_name, 4096)
    // //     .expect("fail to initilize spdk environment");
    // let mut opts = Options::default();
    // opts.create_if_missing(true);
    // opts.set_env(&env);
    // let db = Arc::new(DB::open(&opts, path).expect("fail to open db"));
    // info!("open rocksdb use exist spdk environment");


    let box_opts = unsafe {Box::from_raw(opts_raw)} ;

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
