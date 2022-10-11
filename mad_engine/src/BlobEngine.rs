//! This layer provide basic I/O operations API to upper layer
//!
//! Basically like a message passing module

use log::*;
use std::ffi::c_void;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_spdk::blob::{Blob, BlobId, Blobstore, IoChannel};
use async_spdk::event::SpdkEvent;
use tokio::sync::Notify;

use crate::error::Result;
use crate::{EngineBlob, EngineOpts, Msg, Op};

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
    pub fn new(name: &str, core: u32, io_size: u64, bs: Arc<Mutex<Blobstore>>) -> Self {
        let ret = BlobEngine {
            name: name.to_string(),
            core,
            io_size,
            channel: None,
            bs: bs.clone(),
        };
        ret
    }

    fn create_helper(arg: *mut c_void) {
        let (mut m, mut bid) = unsafe { *Box::from_raw(arg as *mut (Msg, Arc<Mutex<BlobId>>)) };
        m.bs.as_ref()
            .unwrap()
            .lock()
            .unwrap()
            .create_blob_sync(Arc::into_raw(bid.clone()) as *mut c_void)
            .unwrap();
        m.notify();
        info!("Create Blob");
    }

    fn op_helper(arg: *mut c_void) {
        let mut m = unsafe { *Box::from_raw(arg as *mut Msg) };
        match m.op {
            Op::IoSize => unimplemented!(),
            Op::Channel => unimplemented!(),
            Op::Write => {
                let bid = m.blob_id.as_ref().unwrap();
                let blob = Arc::new(Mutex::new(Blob::default()));
                {
                    m.bs.as_ref()
                        .unwrap()
                        .lock()
                        .unwrap()
                        .open_blob_sync(bid, Arc::into_raw(blob.clone()) as *mut c_void)
                        .unwrap();
                }
                while blob.lock().unwrap().ptr.is_null() {
                    info!("Wait for SPDK reactor execution");
                    std::thread::sleep(Duration::from_micros(10));
                }
                let channel = {
                    m.bs.as_ref()
                        .unwrap()
                        .lock()
                        .unwrap()
                        .alloc_io_channel()
                        .unwrap()
                };
                blob.lock()
                    .unwrap()
                    .write_sync(&channel, m.offset.unwrap(), m.write_buf.unwrap())
                    .unwrap();
                m.notify();
                info!("Write Blob");
            }
            Op::Read => {
                let bid = m.blob_id.as_ref().unwrap();
                let blob = Arc::new(Mutex::new(Blob::default()));
                {
                    m.bs.as_ref()
                        .unwrap()
                        .lock()
                        .unwrap()
                        .open_blob_sync(bid, Arc::into_raw(blob.clone()) as *mut c_void)
                        .unwrap();
                }
                while blob.lock().unwrap().ptr.is_null() {
                    info!("Wait for SPDK reactor execution");
                    std::thread::sleep(Duration::from_micros(10));
                }
                let channel = {
                    m.bs.as_ref()
                        .unwrap()
                        .lock()
                        .unwrap()
                        .alloc_io_channel()
                        .unwrap()
                };
                blob.lock()
                    .unwrap()
                    .read_sync(&channel, m.offset.unwrap(), m.read_buf.as_mut().unwrap())
                    .unwrap();
                m.notify();
                info!("Read Blob");
            }
            Op::Create => {
                todo!();
            }
            Op::Delete => {
                let bid = m.blob_id.as_ref().unwrap();
                m.bs.as_ref().unwrap().lock().unwrap().delete_blob_sync(bid);
                m.notify();
                info!("Delete Blob");
            }
            Op::ClusterCount => unimplemented!(),
            Op::Close => {
                m.bs.as_ref().unwrap().lock().unwrap().unload_sync();
                m.notify();
                info!("Close BlobStore");
            }
        }
    }

    pub async fn close(&self) {
        let n = Arc::new(Notify::new());
        let m = Msg::gen_close(n.clone(), self.bs.clone());
        let e = SpdkEvent::alloc(
            self.core,
            Self::op_helper as *const () as *mut c_void,
            Box::into_raw(Box::new(m)) as *mut c_void,
        )
        .unwrap();
        e.call().unwrap();
        info!("Wait for close notify");
        n.notified().await;
    }

    pub async fn write(&self, offset: u64, blob_id: BlobId, buf: &[u8]) -> Result<()> {
        let n = Arc::new(Notify::new());
        let m = Msg::gen_write(n, self.bs.clone(), offset, blob_id, buf);
        let e = SpdkEvent::alloc(
            self.core,
            Self::op_helper as *const () as *mut c_void,
            Box::into_raw(Box::new(m)) as *mut c_void,
        )
        .unwrap();
        e.call().unwrap();
        info!("Wait for write notify");
        n.notified().await;
        Ok(())
    }

    pub async fn read(&self, offset: u64, blob_id: BlobId, buf: &mut [u8]) -> Result<()> {
        let n = Arc::new(Notify::new());
        let m = Msg::gen_read(n, self.bs.clone(), offset, blob_id, buf);
        let e = SpdkEvent::alloc(
            self.core,
            Self::op_helper as *const () as *mut c_void,
            Box::into_raw(Box::new(m)) as *mut c_void,
        )
        .unwrap();
        e.call().unwrap();
        info!("Wait for read notify");
        n.notified().await;
        Ok(())
    }

    pub async fn delete_blob(&self, blob_id: BlobId) -> Result<()> {
        let n = Arc::new(Notify::new());
        let m = Msg::gen_delete(n, self.bs.clone(), blob_id);
        let e = SpdkEvent::alloc(
            self.core,
            Self::op_helper as *const () as *mut c_void,
            Box::into_raw(Box::new(m)) as *mut c_void,
        )
        .unwrap();
        e.call().unwrap();
        info!("Wait for delete notify");
        n.notified().await;
        Ok(())
    }

    pub async fn create_blob(&self, size: u64) -> Result<EngineBlob> {
        let n = Arc::new(Notify::new());
        let m = Msg::gen_create(n, self.bs.clone(), size);
        let mut bid = Arc::new(Mutex::new(BlobId::default()));
        let e = SpdkEvent::alloc(
            self.core,
            Self::create_helper as *const () as *mut c_void,
            Box::into_raw(Box::new((m, bid.clone())) as *mut c_void),
        )
        .unwrap();
        e.call().unwrap();
        info!("Wait for create notify");
        n.notified().await;
        Ok(())
    }
}
