// this is a test for spdk environment
// now this cannot work due to lack of united SPDK environment

use async_spdk::{event::AppOpts, *};
use log::*;
use mad_engine::*;
use rocksdb::*;
use std::sync::Arc;

fn main() {
    env_logger::init();

    let env = Env::rocksdb_create_spdk_env(
        "data",
        &std::env::args().nth(1).expect("expect config file"),
        "Nvme1n1",
        4096,
    )
    .expect("fail to initialize spdk env");
    info!("env create succeed");
    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.set_env(&env);
    let path = "data";
    let db = Arc::new(DB::open(&opts, path).expect("fail to open db"));
    info!("db create succeed");

    db.put(b"foo", b"bar").expect("fail to put");
    info!("put succeed!");
    match db.get(b"foo") {
        Ok(Some(res)) => {
            info!("got value {:?} succeed!", String::from_utf8(res).unwrap());
        }
        Ok(None) => {
            error!("got none value");
        }
        Err(e) => {
            error!("err: {:?}", e);
        }
    };
    info!("Test SPDK Integration Succeed!");

    // let app_opts = event::AppOpts::new()
    //     .name("test7")
    //     .config_file(&std::env::args().nth(1).expect("expect config file"))
    //     .block_on(test7_helper(
    //         db,
    //         "Nvme0n1",
    //         &std::env::args().nth(1).expect("expect config file"),
    //     ))
    //     .unwrap();
    // let opts_copy = event::AppOpts::new()
    //     .name("test7")
    //     .config_file(&std::env::args().nth(1).expect("expect config file"));
    // info!("init opts succeed");

    // app_opts.block_on(test7_helper(
    //     db,
    //     "Nvme0n1",
    //     &std::env::args().nth(1).expect("expect config file"),
    // ))
    // .unwrap();
}

// async fn test7_helper(
//     db: Arc<DBWithThreadMode<SingleThreaded>>,
//     name: &str,
//     conf_file: &str,
// ) -> std::result::Result<(), EngineError> {
//     info!("trying to start spdk environment");
//     let handle = MadEngineHandle::new_spdk(db, "data", conf_file, name)
//         .await
//         .unwrap();
//     handle.create("file1".to_string()).unwrap();
//     info!("create pass...");
//     handle.remove("file1".to_string()).unwrap();
//     info!("remove pass...");
//     handle.unload().await.unwrap();
//     info!("unload succeed...");
//     Ok(())
// }
