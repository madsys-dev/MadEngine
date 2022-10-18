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
use crate::{EngineBlob, Msg, Op};

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
    /// Get corresponding bdev name
    pub fn get_name(&self) -> Result<String> {
        Ok(self.name.clone())
    }

    /// Get binding core number
    pub fn get_core_id(&self) -> Result<u32> {
        Ok(self.core)
    }

    /// Get io_size, for now it is hard-coded
    pub fn get_io_size(&self) -> Result<u64> {
        Ok(self.io_size)
    }
}

impl BlobEngine {
    /// New a blob engine
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

    /// Unload BlobStore
    ///
    /// All blobs must be closed
    pub async fn unload(&self) {
        let n = Arc::new(Notify::new());
        let m = Msg::gen_unload(n.clone(), self.bs.clone());
        let e = SpdkEvent::alloc(
            self.core,
            Self::op_helper as *const () as *mut c_void,
            Box::into_raw(Box::new((m, n.clone()))) as *mut c_void,
        )
        .unwrap();
        e.call().unwrap();
        // info!("Wait for unload notify");
        n.notified().await;
    }

    /// Write data to given blob
    pub async fn write(&self, offset: u64, bid: BlobId, buf: &[u8]) -> Result<()> {
        let blob = Self::open_blob(&self, bid).await?;
        let n = Arc::new(Notify::new());
        let m = Msg::gen_write(n.clone(), self.bs.clone(), offset, blob, buf);
        let e = SpdkEvent::alloc(
            self.core,
            Self::op_helper as *const () as *mut c_void,
            Box::into_raw(Box::new((m, n.clone()))) as *mut c_void,
        )
        .unwrap();
        e.call().unwrap();
        // info!("Wait for write notify");
        n.notified().await;
        Self::close_blob(&self, blob).await?;
        Ok(())
    }

    /// Read data from a given blob
    ///
    /// TODO: this should return read size
    pub async fn read(&self, offset: u64, bid: BlobId, buf: &mut [u8]) -> Result<()> {
        let blob = Self::open_blob(&self, bid).await?;
        let n = Arc::new(Notify::new());
        let m = Msg::gen_read(n.clone(), self.bs.clone(), offset, blob, buf);
        let e = SpdkEvent::alloc(
            self.core,
            Self::op_helper as *const () as *mut c_void,
            Box::into_raw(Box::new((m, n.clone()))) as *mut c_void,
        )
        .unwrap();
        e.call().unwrap();
        // info!("Wait for read notify");
        n.notified().await;
        Self::close_blob(&self, blob).await?;
        Ok(())
    }

    /// Delete a blob
    pub async fn delete_blob(&self, blob_id: BlobId) -> Result<()> {
        let n = Arc::new(Notify::new());
        let m = Msg::gen_delete(n.clone(), self.bs.clone(), blob_id);
        let e = SpdkEvent::alloc(
            self.core,
            Self::op_helper as *const () as *mut c_void,
            Box::into_raw(Box::new((m, n.clone()))) as *mut c_void,
        )
        .unwrap();
        e.call().unwrap();
        // info!("Wait for delete notify");
        n.notified().await;
        Ok(())
    }

    /// Create empty blob
    pub async fn create_blob(&self) -> Result<BlobId> {
        let n = Arc::new(Notify::new());
        let m = Msg::gen_create(n.clone(), self.bs.clone());
        let bid = Arc::new(Mutex::new(BlobId::default()));
        let e = SpdkEvent::alloc(
            self.core,
            Self::create_helper as *const () as *mut c_void,
            Box::into_raw(Box::new((m, bid.clone(), n.clone()))) as *mut c_void,
        )
        .unwrap();
        e.call().unwrap();
        // info!("Wait for create notify");
        n.notified().await;
        let b = *bid.lock().unwrap();
        Ok(b)
    }

    /// Open a blob, get blob handle
    pub async fn open_blob(&self, bid: BlobId) -> Result<Blob> {
        let n = Arc::new(Notify::new());
        let m = Msg::gen_open(n.clone(), self.bs.clone(), bid);
        let blob = Arc::new(Mutex::new(Blob::default()));
        let e = SpdkEvent::alloc(
            self.core,
            Self::open_helper as *const () as *mut c_void,
            Box::into_raw(Box::new((m, blob.clone(), n.clone()))) as *mut c_void,
        )
        .unwrap();
        e.call().unwrap();
        // info!("Wait for open notify");
        n.notified().await;
        let b = *blob.lock().unwrap();
        Ok(b)
    }

    /// Resize a blob
    ///
    /// Blob creation only creates null blob
    pub async fn resize_blob(&self, blob: Blob, size: u64) -> Result<()> {
        let n = Arc::new(Notify::new());
        let m = Msg::gen_resize(n.clone(), self.bs.clone(), blob, size);
        let e = SpdkEvent::alloc(
            self.core,
            Self::op_helper as *const () as *mut c_void,
            Box::into_raw(Box::new((m, n.clone()))) as *mut c_void,
        )
        .unwrap();
        e.call().unwrap();
        // info!("Wait for Resize notify");
        n.notified().await;
        Ok(())
    }

    /// Blob metadata sync
    pub async fn sync_blob(&self, blob: Blob) -> Result<()> {
        let n = Arc::new(Notify::new());
        let m = Msg::gen_sync(n.clone(), self.bs.clone(), blob);
        let e = SpdkEvent::alloc(
            self.core,
            Self::op_helper as *const () as *mut c_void,
            Box::into_raw(Box::new((m, n.clone()))) as *mut c_void,
        )
        .unwrap();
        e.call().unwrap();
        // info!("Wait for sync notify");
        n.notified().await;
        Ok(())
    }

    /// Close a blob
    ///
    /// All blobs must be closed before unload blobstore
    pub async fn close_blob(&self, blob: Blob) -> Result<()> {
        let n = Arc::new(Notify::new());
        let m = Msg::gen_close(n.clone(), self.bs.clone(), blob);
        let e = SpdkEvent::alloc(
            self.core,
            Self::op_helper as *const () as *mut c_void,
            Box::into_raw(Box::new((m, n.clone()))) as *mut c_void,
        )
        .unwrap();
        e.call().unwrap();
        // info!("Wait for close notify");
        n.notified().await;
        Ok(())
    }

    fn create_helper(arg: *mut c_void) {
        let (m, bid, n) =
            unsafe { *Box::from_raw(arg as *mut (Msg, Arc<Mutex<BlobId>>, Arc<Notify>)) };
        m.bs.as_ref()
            .unwrap()
            .lock()
            .unwrap()
            .create_blob_sync(Box::into_raw(Box::new((bid.clone(), n.clone()))) as *mut c_void)
            .unwrap();
        info!("Create Blob");
    }

    fn open_helper(arg: *mut c_void) {
        let (m, blob, n) =
            unsafe { *Box::from_raw(arg as *mut (Msg, Arc<Mutex<EngineBlob>>, Arc<Notify>)) };
        let bid = m.blob_id.as_ref().unwrap();
        m.bs.as_ref()
            .unwrap()
            .lock()
            .unwrap()
            .open_blob_sync(
                bid,
                Box::into_raw(Box::new((blob.clone(), n.clone()))) as *mut c_void,
            )
            .unwrap();
    }

    fn op_helper(arg: *mut c_void) {
        let (mut m, n) = unsafe { *Box::from_raw(arg as *mut (Msg, Arc<Notify>)) };
        match m.op {
            Op::IoSize => unimplemented!(),
            Op::Channel => unimplemented!(),
            Op::Write => {
                let channel = {
                    m.bs.as_ref()
                        .unwrap()
                        .lock()
                        .unwrap()
                        .alloc_io_channel()
                        .unwrap()
                };
                m.blob
                    .as_ref()
                    .unwrap()
                    .write_sync(
                        &channel,
                        m.offset.unwrap(),
                        m.write_buf.clone().unwrap(),
                        Box::into_raw(Box::new(n.clone())) as *mut c_void,
                    )
                    .unwrap();
                info!("Write Blob");
            }
            Op::Read => {
                let channel = {
                    m.bs.as_ref()
                        .unwrap()
                        .lock()
                        .unwrap()
                        .alloc_io_channel()
                        .unwrap()
                };
                m.blob
                    .as_ref()
                    .unwrap()
                    .read_sync(
                        &channel,
                        m.offset.unwrap(),
                        m.read_buf.as_mut().unwrap(),
                        Box::into_raw(Box::new(n.clone())) as *mut c_void,
                    )
                    .unwrap();
                info!("Read Blob");
            }
            Op::Create => {
                todo!();
            }
            Op::Delete => {
                let bid = m.blob_id.as_ref().unwrap();
                m.bs.as_ref()
                    .unwrap()
                    .lock()
                    .unwrap()
                    .delete_blob_sync(bid, Box::into_raw(Box::new(n.clone())) as *mut c_void)
                    .unwrap();
                info!("Delete Blob");
            }
            Op::ClusterCount => unimplemented!(),
            Op::Unload => {
                m.bs.as_ref()
                    .unwrap()
                    .lock()
                    .unwrap()
                    .unload_sync(Box::into_raw(Box::new(n.clone())) as *mut c_void)
                    .unwrap();
                info!("Unload BlobStore");
            }
            Op::Resize => {
                m.blob
                    .as_ref()
                    .unwrap()
                    .resize_sync(
                        m.blob_size.unwrap(),
                        Box::into_raw(Box::new(n.clone())) as *mut c_void,
                    )
                    .unwrap();
                info!("Resize Blob");
            }
            Op::Sync => {
                m.blob
                    .as_ref()
                    .unwrap()
                    .sync_metadata_sync(Box::into_raw(Box::new(n.clone())) as *mut c_void)
                    .unwrap();
                info!("Sync Metadata");
            }
            Op::Close => {
                m.blob
                    .as_ref()
                    .unwrap()
                    .close_sync(Box::into_raw(Box::new(n.clone())) as *mut c_void)
                    .unwrap();
                info!("Close Blob");
            }
            _ => unimplemented!(),
        }
    }
}
