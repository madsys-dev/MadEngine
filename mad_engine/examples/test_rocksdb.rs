//! This is a test for rocksdb-spdk integration
//! Test includes:
//! 1. Start spdk environment on a specific thread, reveal fs pointer
//! 2. Use revealed pointer to start rocksdb
//! 3. Test basic `put`, `get`

use std::{
    ffi::c_void,
    sync::{Arc, Mutex},
};

use async_spdk::event::app_stop;
use async_spdk::{blobfs::*, thread::Poller, *};

use log::*;

fn main() {
    env_logger::init();
    let fsflag = Arc::new(Mutex::new(false));
    let fs = Arc::new(Mutex::new(SpdkFilesystem::default()));
    let shutdown = Arc::new(Mutex::new(false));
    let shutdown_poller = Arc::new(Mutex::new(Poller::default()));

    let ff2 = fsflag.clone();
    let fs2 = fs.clone();
    let shutdown2 = shutdown.clone();

    let fs_handle = std::thread::spawn(|| {
        event::AppOpts::new()
            .name("test_rocksdb")
            .config_file(&std::env::args().nth(1).expect("no config file"))
            .reactor_mask("0x1")
            .block_on(async_main(ff2, fs2, shutdown2, shutdown_poller))
            .unwrap();
    });

    let ff3 = fsflag.clone();
    let fs3 = fs.clone();
    let shutdown3 = shutdown.clone();

    user_app(ff3, fs3, shutdown3).unwrap();

    fs_handle.join().unwrap();
    info!("rocksdb test pass...");
}

fn user_app(
    fflag: Arc<Mutex<bool>>,
    fs: Arc<Mutex<SpdkFilesystem>>,
    shutdown: Arc<Mutex<bool>>,
) -> Result<()> {
    loop {
        if *fflag.lock().unwrap() == true {
            break;
        }
    }

    let fs = fs.lock().unwrap();
    info!("Get fs handle");

    if fs.is_null() {
        info!("fs pointer is null");
    }

    let env = rocksdb::Env::rocksdb_use_spdk_env(
        fs.ptr as *mut c_void,
        0,
        "rocksdb_test_dir",
        &std::env::args().nth(1).expect("no config file"),
        "Malloc0",
        4096,
    )
    .expect("fail to initialize spdk env");
    info!("RocksDB env success");

    let mut opts = rocksdb::Options::default();
    opts.create_if_missing(true);
    opts.set_env(&env);
    info!("Opts set success");

    let path = "rocksdb_test_dir";
    let db = rocksdb::DB::open(&opts, path).expect("fail to open db");
    info!("db open success");

    db.put(b"foo", b"bar").expect("fail to put");
    info!("db put success");

    match db.get(b"foo") {
        Ok(Some(res)) => {
            info!("got value {:?} succeed!", String::from_utf8(res).unwrap());
        }
        Ok(None) => {
            info!("got none value");
        }
        Err(e) => {
            error!("err: {:?}", e);
        }
    };
    info!("db get success");

    drop(db);
    drop(fs);

    *shutdown.lock().unwrap() = true;
    info!("Set shutdown to true");

    Ok(())
}

async fn async_main(
    fflag: Arc<Mutex<bool>>,
    fs: Arc<Mutex<SpdkFilesystem>>,
    shutdown: Arc<Mutex<bool>>,
    shutdown_poller: Arc<Mutex<Poller>>,
) -> Result<()> {
    info!("start main: hello_blobfs");

    let mut bdev = blob_bdev::BlobStoreBDev::create("Malloc0")?;
    info!("BlobStoreBdev created");

    let mut blobfs_opt = SpdkBlobfsOpts::init().await?;
    info!("BlobFs opts init");

    let blobfs = SpdkFilesystem::init(&mut bdev, &mut blobfs_opt).await?;
    info!("BlobFs init");

    let shutdown_fs = fs.clone();
    let shutdown_copy = shutdown.clone();
    let shutdown_poller_copy = shutdown_poller.clone();

    *shutdown_poller.lock().unwrap() = Poller::register(move || {
        if *shutdown_copy.lock().unwrap() == true {
            info!("shutdonw poller receive shutdown signal");
            shutdown_fs.lock().unwrap().unload_sync().unwrap();
            info!("unload fs success");
            shutdown_poller_copy.lock().unwrap().unregister();
            info!("unregister poller success");
            app_stop();
        }
        true
    })?;

    *fs.lock().unwrap() = blobfs;
    info!("Pass fs to global");

    *fflag.lock().unwrap() = true;
    info!("Set flag to true");

    Ok(())
}
