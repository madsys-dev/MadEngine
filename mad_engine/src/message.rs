//! This includes I/O operation enumeration

use async_spdk::blob::{IoChannel, Blobstore};
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

pub struct Msg {
    pub op: Op,
    pub channel: Option<IoChannel>,
    pub notify: Option<Arc<Notify>>,
    pub bs: Option<Arc<Mutex<Blobstore>>>,
}
