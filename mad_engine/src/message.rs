//! This includes I/O operation enumeration

use async_spdk::blob::{Blob, BlobId, Blobstore, IoChannel};
use std::sync::{Arc, Mutex};
use tokio::sync::Notify;

pub enum Op {
    IoSize,
    Channel,
    Write,
    Read,
    // Create Blob
    Create,
    Delete,
    ClusterCount,
    Open,
    Unload,
    Sync,
    Resize,
    Close,
}

pub struct Msg<'a> {
    pub op: Op,
    channel: Option<IoChannel>,
    notify: Option<Arc<Notify>>,
    pub bs: Option<Arc<Mutex<Blobstore>>>,
    pub offset: Option<u64>,
    pub blob_id: Option<BlobId>,
    pub blob: Option<Blob>,
    pub read_buf: Option<&'a mut [u8]>,
    pub write_buf: Option<&'a [u8]>,
    pub blob_size: Option<u64>,
}

impl<'a> Msg<'a> {
    pub fn notify(&self) {
        self.notify.as_ref().unwrap().notify_one();
    }

    pub fn gen_close(notify: Arc<Notify>, bs: Arc<Mutex<Blobstore>>, blob: Blob) -> Self{
        Self{
            op: Op::Close,
            channel: None,
            notify: Some(notify),
            bs: Some(bs),
            offset: None,
            blob_id: None,
            blob: Some(blob),
            read_buf: None,
            write_buf: None,
            blob_size: None,
        }
    }

    pub fn gen_unload(notify: Arc<Notify>, bs: Arc<Mutex<Blobstore>>) -> Self {
        Self {
            op: Op::Unload,
            channel: None,
            notify: Some(notify),
            bs: Some(bs),
            offset: None,
            blob_id: None,
            blob: None,
            read_buf: None,
            write_buf: None,
            blob_size: None,
        }
    }

    pub fn gen_write(
        notify: Arc<Notify>,
        bs: Arc<Mutex<Blobstore>>,
        offset: u64,
        blob: Blob,
        buf: &'a [u8],
    ) -> Self {
        Self {
            op: Op::Write,
            channel: None,
            notify: Some(notify),
            bs: Some(bs),
            offset: Some(offset),
            blob_id: None,
            blob: Some(blob),
            read_buf: None,
            write_buf: Some(buf),
            blob_size: None,
        }
    }

    pub fn gen_read(
        notify: Arc<Notify>,
        bs: Arc<Mutex<Blobstore>>,
        offset: u64,
        blob: Blob,
        buf: &'a mut [u8],
    ) -> Self {
        Self {
            op: Op::Read,
            channel: None,
            notify: Some(notify),
            bs: Some(bs),
            offset: Some(offset),
            blob_id: None,
            blob: Some(blob),
            read_buf: Some(buf),
            write_buf: None,
            blob_size: None,
        }
    }

    pub fn gen_create(notify: Arc<Notify>, bs: Arc<Mutex<Blobstore>>, blob_size: u64) -> Self {
        Self {
            op: Op::Create,
            channel: None,
            notify: Some(notify),
            bs: Some(bs),
            offset: None,
            blob_id: None,
            blob: None,
            read_buf: None,
            write_buf: None,
            blob_size: Some(blob_size),
        }
    }

    pub fn gen_delete(notify: Arc<Notify>, bs: Arc<Mutex<Blobstore>>, blob_id: BlobId) -> Self {
        Self {
            op: Op::Delete,
            channel: None,
            notify: Some(notify),
            bs: Some(bs),
            offset: None,
            blob_id: Some(blob_id),
            blob: None,
            read_buf: None,
            write_buf: None,
            blob_size: None,
        }
    }

    pub fn gen_open(notify: Arc<Notify>, bs: Arc<Mutex<Blobstore>>, bid: BlobId) -> Self {
        Self {
            op: Op::Open,
            channel: None,
            notify: Some(notify),
            bs: Some(bs),
            offset: None,
            blob_id: Some(bid),
            blob: None,
            read_buf: None,
            write_buf: None,
            blob_size: None,
        }
    }

    pub fn gen_sync(notify: Arc<Notify>, bs: Arc<Mutex<Blobstore>>, blob: Blob) -> Self {
        Self {
            op: Op::Sync,
            channel: None,
            notify: Some(notify),
            bs: Some(bs),
            offset: None,
            blob_id: None,
            blob: Some(blob),
            read_buf: None,
            write_buf: None,
            blob_size: None,
        }
    }

    pub fn gen_resize(
        notify: Arc<Notify>,
        bs: Arc<Mutex<Blobstore>>,
        blob: Blob,
        size: u64,
    ) -> Self {
        Self {
            op: Op::Sync,
            channel: None,
            notify: Some(notify),
            bs: Some(bs),
            offset: None,
            blob_id: None,
            blob: Some(blob),
            read_buf: None,
            write_buf: None,
            blob_size: Some(size),
        }
    }
}
