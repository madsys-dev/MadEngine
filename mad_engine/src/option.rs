//! This is a plugin for establishing SPDK environment

use crate::error::Result;
use async_spdk::blob;
use async_spdk::blobfs::SpdkBlobfsOpts;
use async_spdk::event::SpdkEvent;
use async_spdk::thread::Poller;
use async_spdk::{
    blob_bdev,
    blobfs::SpdkFilesystem,
    event::{self, app_stop},
};
use log::*;
use std::ffi::{CString, c_void};
use std::{
    sync::{Arc, Mutex},
    thread::JoinHandle,
};
use futures::executor::block_on;

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
    // SPDK start thread handle
    thread_handle: Option<JoinHandle<()>>,
    // App name
    app_name: String,
    // Flag to indicate blobfs establish
    fsflag: Arc<Mutex<bool>>,
    // Blobfs pointer
    fs: Arc<Mutex<SpdkFilesystem>>,
    // Shutdown signal
    shutdown: Arc<Mutex<bool>>,
    // Shutdown Poller
    shutdown_poller: Arc<Mutex<Poller>>,
}

/// This defines the mapping between bs to core
#[derive(Debug, Clone)]
pub struct BsBindOpts {
    // bdev to start blobstore
    bdev_name: String,
    // blobstore binding core
    core: u32,
}

impl Default for EngineOpts {
    fn default() -> Self {
        Self {
            reactor_mask: "0x1".to_string(),
            config_file: String::new(),
            start_blobfs: false,
            blobfs_bdev: None,
            blobstore_bdev_list: None,
            thread_handle: None,
            app_name: String::new(),
            fsflag: Arc::new(Mutex::new(false)),
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
    pub fn set_reactor_mask(&mut self, mask: &str) {
        self.reactor_mask = mask.to_string();
    }

    pub fn set_blobfs(&mut self, blobfs_bdev: &str) {
        self.start_blobfs = true;
        self.blobfs_bdev = Some(blobfs_bdev.to_string());
    }

    pub fn set_blobstore(&mut self, blobstore_bdev_list: Vec<BsBindOpts>) {
        self.blobstore_bdev_list = Some(blobstore_bdev_list);
    }

    pub fn set_name(&mut self, app_name: &str) {
        self.app_name = app_name.to_string();
    }

    // start blobfs and blobstore by given configuration
    pub fn start_spdk(
        &mut self,
    ) {
        let app_name = if self.app_name.len() == 0 {
            "None-name app".to_string()
        } else {
            self.app_name.clone()
        };
        let config_file = self.config_file.clone();
        let reactor_mask = self.reactor_mask.clone();
        let start_blobfs = self.start_blobfs;
        let blobfs_bdev = self.blobfs_bdev.clone();
        let blobstore_bdev_list = self.blobstore_bdev_list.clone();

        let fs = self.fs.clone();
        let fsflag = self.fsflag.clone();
        let shutdown = self.shutdown.clone();
        let shutdown_poller = self.shutdown_poller.clone();
        let fs_handle = std::thread::spawn(move || {
            event::AppOpts::new()
                .name(app_name.as_str())
                .config_file(config_file.as_str())
                .reactor_mask(reactor_mask.as_str())
                .block_on(Self::start_spdk_helper(
                    fs,
                    fsflag,
                    shutdown,
                    shutdown_poller,
                    blobfs_bdev.as_ref(),
                    start_blobfs,
                    blobstore_bdev_list.unwrap(),
                ))
                .unwrap();
        });
        self.thread_handle = Some(fs_handle);
    }

    async fn start_spdk_helper(
        fs: Arc<Mutex<SpdkFilesystem>>,
        fsflag: Arc<Mutex<bool>>,
        shutdown: Arc<Mutex<bool>>,
        shutdown_poller: Arc<Mutex<Poller>>,
        blobfs_bdev: Option<&String>,
        start_blobfs: bool,
        blobstore_bdev_list: Vec<BsBindOpts>,
    ) -> Result<()> {
        let shutdown_fs = fs.clone();
        let shutdown_sig = shutdown.clone();
        let shutdown_poller_copy = shutdown_poller.clone();

        // register a shutdown poller
        // this is not a proper way to let user send shutdown signal
        *shutdown_poller.lock().unwrap() = Poller::register(move || {
            if *shutdown_sig.lock().unwrap() == true {
                info!("shutdown spdk environment");
                shutdown_fs.lock().unwrap().unload_sync().unwrap();
                shutdown_poller_copy.lock().unwrap().unregister();
                app_stop();
            }
            true
        })?;

        // initialize blobfs
        if start_blobfs {
            let mut bdev = blob_bdev::BlobStoreBDev::create(blobfs_bdev.unwrap().as_str())?;
            let mut blobfs_opts = SpdkBlobfsOpts::init().await?;
            let blobfs = SpdkFilesystem::init(&mut bdev, &mut blobfs_opts).await?;

            *fs.lock().unwrap() = blobfs;
            *fsflag.lock().unwrap() = true;
        }

        // initialize blobstore on specific core
        blobstore_bdev_list.into_iter().for_each(|opt|{
            let e = SpdkEvent::alloc(
                opt.core, 
                build_blobstore as *const() as *mut c_void, 
                CString::new(opt.bdev_name).expect("fail to parse bdev name").into_raw() as *mut c_void).unwrap();
            e.call().unwrap();
        });

        Ok(())
    }

    // call ready after start spdk to wait for blobfs if needed
    pub fn ready(&self){
        loop{
            if *self.fsflag.lock().unwrap() == true{
                break;
            }
        }
    }

}

fn build_blobstore(bdev: *mut c_void){
    let bdev = unsafe {
        CString::from_raw(bdev as *mut _)
    };
    let mut bs_dev = blob_bdev::BlobStoreBDev::create(bdev.into_string().unwrap().as_str()).unwrap();
    let bs = block_on(blob::Blobstore::init(&mut bs_dev)).unwrap();
    info!("blob store initilize success");
    
}
