//! This layer provide basic I/O operations API to upper layer
//!
//! Basically like a message passing module

use std::ffi::c_void;
use std::sync::{Arc, Mutex};
use log::*;

use async_spdk::blob::{IoChannel, Blobstore};
use async_spdk::event::SpdkEvent;
use tokio::sync::Notify;

use crate::error::Result;
use crate::{EngineOpts, Msg, Op};

pub struct BlobEngine {
    // Intuitively each blobstore need its own BlobEngine
    // name equals to bdev name
    pub name: String,
    // Which core to play I/O, note that each core binds one BlobStore
    pub core: u32,
    pub io_size: u64,
    // io_channel to perform I/O
    pub channel: Option<IoChannel>,
    // Blobstore
    pub bs: Arc<Mutex<Blobstore>>,
}

impl std::fmt::Display for BlobEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "BlobEngine INFO:\n\tname: {:?}\n\tcore: {}\n\tio_size: {}\n",
            self.name.clone(),
            self.core,
            self.io_size
        )
    }
}

impl BlobEngine {
    pub fn get_name(&self) -> Result<String> {
        Ok(self.name.clone())
    }

    pub fn get_core_id(&self) -> Result<u32> {
        Ok(self.core)
    }

    pub fn get_io_size(&self) -> Result<u64> {
        Ok(self.io_size)
    }
}

impl BlobEngine {
    pub fn new(name: &str, core: u32, io_size: u64, bs: Arc<Mutex<Blobstore>>) -> Self{
        let ret = BlobEngine{
            name: name.to_string(),
            core,
            io_size,
            channel: None,
            bs: bs.clone(),
        };
        ret
    }

    fn close_helper(arg: *mut c_void){
        info!("close helper line1...");
        let m = unsafe { *Box::from_raw(arg as *mut Msg)};
        info!("close helper m transfer...");
        {
            info!("before unload");
            m.bs.unwrap().lock().unwrap().unload_sync();
            info!("unload success");
            m.notify.unwrap().notify_one();
            info!("close success");
        }
    }

    pub async fn close(&self) {
        let n = Arc::new(Notify::new());
        let m = Msg{
            op: Op::Close,
            channel: None,
            notify: Some(n.clone()),
            bs: Some(self.bs.clone()),
        };
        let e = SpdkEvent::alloc(
            self.core,
            Self::close_helper as *const() as *mut c_void,
            Box::into_raw(Box::new(
                m
            )) as *mut c_void
        ).unwrap();
        e.call().unwrap();
        info!("Wait for close notify");
        n.notified().await;
        info!("Close success");
    }
}
