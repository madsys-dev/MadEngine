//! This is a plugin for establishing SPDK environment

use crate::blob_engine::BlobEngine;
use crate::error::Result;
use async_spdk::blob::{self, Blobstore};
use async_spdk::blobfs::SpdkBlobfsOpts;
use async_spdk::event::SpdkEvent;
use async_spdk::thread::Poller;
use async_spdk::{
    blob_bdev,
    blobfs::SpdkFilesystem,
    event::{self, app_stop},
};
use log::*;
use std::ffi::{c_void, CString};
use std::{
    sync::{Arc, Mutex},
    thread::JoinHandle,
};
use tokio::sync::Notify;

pub struct EngineOpts {
    // start reactor on which core
    reactor_mask: String,
    // SPDK configuration file
    config_file: String,
    // start blobfs or not
    start_blobfs: bool,
    // start blobfs on which bdev
    blobfs_bdev: Option<String>,
    // start each blobstore on specific bdev, there could be multiple blobstore
    blobstore_bdev_list: Option<Vec<BsBindOpts>>,
    // Blobstore list
    blobstores: Arc<Mutex<Vec<Arc<Mutex<Blobstore>>>>>,
    // SPDK start thread handle
    thread_handle: Option<JoinHandle<()>>,
    // App name
    app_name: String,
    // Flag to indicate blobfs establish
    fsflag: Arc<Mutex<bool>>,
    // Flag to indicate blobstore establish
    bsflag: Arc<Mutex<bool>>,
    // Blobfs pointer
    pub fs: Arc<Mutex<SpdkFilesystem>>,
    // Shutdown signal
    shutdown: Arc<Mutex<bool>>,
    // Shutdown Poller
    shutdown_poller: Arc<Mutex<Poller>>,
}

/// This defines the mapping between bs to core
#[derive(Debug, Clone)]
pub struct BsBindOpts {
    // bdev to start blobstore
    pub bdev_name: String,
    // blobstore binding core
    pub core: u32,
}

impl Default for EngineOpts {
    fn default() -> Self {
        Self {
            reactor_mask: "0x1".to_string(),
            config_file: String::new(),
            start_blobfs: false,
            blobfs_bdev: None,
            blobstore_bdev_list: None,
            blobstores: Arc::new(Mutex::new(vec![])),
            thread_handle: None,
            app_name: String::new(),
            fsflag: Arc::new(Mutex::new(false)),
            bsflag: Arc::new(Mutex::new(false)),
            fs: Arc::new(Mutex::new(SpdkFilesystem::default())),
            shutdown: Arc::new(Mutex::new(false)),
            shutdown_poller: Arc::new(Mutex::new(Poller::default())),
        }
    }
}

impl Drop for EngineOpts {
    fn drop(&mut self) {
        if self.thread_handle.is_some() {
            self.thread_handle.take().unwrap().join().unwrap();
        } else {
            error!("thread handle should not be None");
        }
    }
}

impl EngineOpts {
    /// set reactor mask to instruct which cores to start SPDK reactor
    pub fn set_reactor_mask(&mut self, mask: &str) {
        self.reactor_mask = mask.to_string();
    }

    /// whether build blobfs or not
    pub fn set_blobfs(&mut self, blobfs_bdev: &str) {
        self.start_blobfs = true;
        self.blobfs_bdev = Some(blobfs_bdev.to_string());
    }

    /// set which bdev and core to run blobstore
    pub fn set_blobstore(&mut self, blobstore_bdev_list: Vec<BsBindOpts>) {
        self.blobstore_bdev_list = Some(blobstore_bdev_list);
        info!("Set bs success, bs: {:?}", self.blobstore_bdev_list);
    }

    /// set app name (optional)
    pub fn set_name(&mut self, app_name: &str) {
        self.app_name = app_name.to_string();
    }

    /// set configuration file
    pub fn set_config_file(&mut self, config: String) {
        self.config_file = config;
    }

    // start blobfs and blobstore by given configuration
    pub fn start_spdk(&mut self, is_reload: bool) {
        let app_name = if self.app_name.is_empty() {
            "None-name app".to_string()
        } else {
            self.app_name.clone()
        };
        let config_file = self.config_file.clone();
        let reactor_mask = self.reactor_mask.clone();
        let start_blobfs = self.start_blobfs;

        let blobfs_bdev = self.blobfs_bdev.clone();
        let blobstore_bdev_list = self.blobstore_bdev_list.clone();
        let blobstores = self.blobstores.clone();

        let fs = self.fs.clone();
        let fsflag = self.fsflag.clone();
        let bsflag = self.bsflag.clone();
        let shutdown = self.shutdown.clone();
        let shutdown_poller = self.shutdown_poller.clone();
        let fs_handle = std::thread::spawn(move || {
            event::AppOpts::new()
                .name(app_name.as_str())
                .config_file(config_file.as_str())
                .reactor_mask(reactor_mask.as_str())
                .block_on(Self::start_spdk_helper(
                    is_reload,
                    fs,
                    fsflag,
                    bsflag,
                    shutdown,
                    shutdown_poller,
                    blobfs_bdev.as_ref(),
                    start_blobfs,
                    blobstore_bdev_list.unwrap(),
                    blobstores,
                ))
                .unwrap();
        });
        self.thread_handle = Some(fs_handle);
    }

    async fn start_spdk_helper(
        is_reload: bool,
        fs: Arc<Mutex<SpdkFilesystem>>,
        fsflag: Arc<Mutex<bool>>,
        bsflag: Arc<Mutex<bool>>,
        shutdown: Arc<Mutex<bool>>,
        shutdown_poller: Arc<Mutex<Poller>>,
        blobfs_bdev: Option<&String>,
        start_blobfs: bool,
        blobstore_bdev_list: Vec<BsBindOpts>,
        blobstores: Arc<Mutex<Vec<Arc<Mutex<Blobstore>>>>>,
    ) -> Result<()> {
        let shutdown_fs = fs.clone();
        let shutdown_sig = shutdown.clone();
        let shutdown_poller_copy = shutdown_poller.clone();

        // register a shutdown poller
        // this is not a proper way to let user send shutdown signal
        *shutdown_poller.lock().unwrap() = Poller::register(move || {
            if *shutdown_sig.lock().unwrap() {
                info!("shutdown spdk environment");
                shutdown_fs.lock().unwrap().unload_sync().unwrap();
                shutdown_poller_copy.lock().unwrap().unregister();
                app_stop();
            }
            true
        })?;

        // initialize blobfs
        if start_blobfs && !is_reload {
            let mut bdev = blob_bdev::BlobStoreBDev::create(blobfs_bdev.unwrap().as_str())?;
            let mut blobfs_opts = SpdkBlobfsOpts::init().await?;
            let blobfs = SpdkFilesystem::init(&mut bdev, &mut blobfs_opts).await?;

            *fs.lock().unwrap() = blobfs;
            *fsflag.lock().unwrap() = true;
            info!("fs init success");
        } else if start_blobfs && is_reload {
            info!("before reload.....");
            let mut bdev = blob_bdev::BlobStoreBDev::create(blobfs_bdev.unwrap().as_str())?;
            let blobfs = SpdkFilesystem::load(&mut bdev).await?;

            *fs.lock().unwrap() = blobfs;
            *fsflag.lock().unwrap() = true;
            info!("fs reload success");
        }

        // initialize blobstore on specific core
        blobstore_bdev_list.into_iter().for_each(|opt| {
            let bs_tmp = Arc::new(Mutex::new(Blobstore::default()));
            let n = Arc::new(Notify::new());
            let e = SpdkEvent::alloc(
                opt.core,
                build_blobstore as *const () as *mut c_void,
                Box::into_raw(Box::new((
                    CString::new(opt.bdev_name).expect("fail to parse bdev name"),
                    bs_tmp.clone(),
                    n.clone(),
                    is_reload,
                ))) as *mut c_void,
            )
            .unwrap();
            e.call().unwrap();
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                n.notified().await;
            });
            {
                blobstores.clone().lock().unwrap().push(bs_tmp.clone());
                *bsflag.lock().unwrap() = true;
                if bs_tmp.lock().unwrap().ptr.is_null() {
                    error!("push a none pointer");
                } else {
                    info!("create blobstore finish");
                }
            };
        });

        Ok(())
    }

    /// call ready after start spdk to wait for blobfs if needed
    pub fn ready(&self) {
        loop {
            if *self.fsflag.lock().unwrap() && *self.bsflag.lock().unwrap() {
                break;
            }
        }
    }

    /// Send shutdown signal to SPDK env
    pub fn finish(&mut self) {
        *self.shutdown.lock().unwrap() = true;
    }

    /// create a blob engine, for test
    pub fn create_be(&self) -> BlobEngine {
        let bs_lock = self.blobstores.lock().unwrap();
        let bs_list = self.blobstore_bdev_list.clone().unwrap();
        BlobEngine::new(
            &bs_list[0].bdev_name.clone(),
            bs_list[0].core,
            512,
            bs_lock[0].clone(),
        )
    }
}

fn build_blobstore(arg: *mut c_void) {
    info!(">>>> build_blobstore is called");
    let (bdev, bs, n, is_reload) =
        unsafe { *Box::from_raw(arg as *mut (CString, Arc<Mutex<Blobstore>>, Arc<Notify>, bool)) };
    let mut bs_dev =
        blob_bdev::BlobStoreBDev::create(bdev.into_string().unwrap().as_str()).unwrap();
    if !is_reload {
        blob::Blobstore::init_sync(&mut bs_dev, Box::into_raw(Box::new((bs, n))) as *mut c_void)
            .unwrap();
    } else if is_reload {
        blob::Blobstore::load_sync(&mut bs_dev, Box::into_raw(Box::new((bs, n))) as *mut c_void)
            .unwrap();
    }
    info!("blob store initilize success");
}
