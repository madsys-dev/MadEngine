//! This includes I/O operation enumeration

use async_spdk::blob::{BlobId, Blobstore, IoChannel};
use std::sync::{Arc, Mutex};
use tokio::sync::Notify;

pub enum Op {
    IoSize,
    Channel,
    Write,
    Read,
    Create,
    Delete,
    ClusterCount,
    Close,
}

pub struct Msg<'a> {
    op: Op,
    channel: Option<IoChannel>,
    notify: Option<Arc<Notify>>,
    pub bs: Option<Arc<Mutex<Blobstore>>>,
    offset: Option<u64>,
    blob_id: Option<BlobId>,
    buf: Option<&'a [u8]>,
}

impl<'a> Msg<'a> {
    pub fn notify(&self) {
        self.notify.as_ref().unwrap().notify_one();
    }

    pub fn gen_close(notify: Arc<Notify>, bs: Arc<Mutex<Blobstore>>) -> Self {
        Self {
            op: Op::Close,
            channel: None,
            notify: Some(notify),
            bs: Some(bs),
            offset: None,
            blob_id: None,
            buf: None,
        }
    }

    pub fn gen_write(
        notify: Arc<Notify>,
        bs: Arc<Mutex<Blobstore>>,
        offset: u64,
        blob_id: BlobId,
        buf: &'a [u8],
    ) -> Self {
        Self {
            op: Op::Write,
            channel: None,
            notify: Some(notify),
            bs: Some(bs),
            offset: Some(offset),
            blob_id: Some(blob_id),
            buf: Some(buf),
        }
    }

    pub fn gen_read(
        notify: Arc<Notify>,
        bs: Arc<Mutex<Blobstore>>,
        offset: u64,
        blob_id: BlobId,
        buf: &'a mut [u8],
    ) -> Self {
        Self {
            op: Op::Read,
            channel: None,
            notify: Some(notify),
            bs: Some(bs),
            offset: Some(offset),
            blob_id: Some(blob_id),
            buf: Some(buf),
        }
    }
}
