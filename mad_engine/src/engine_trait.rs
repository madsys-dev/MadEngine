use crate::error::Result;
use async_spdk::blob::{Blob, BlobId, Blobstore};
use std::sync::Arc;
use std::sync::Mutex;

// pub trait BlobEngineOp {
//     /// New a BlobEngine
//     pub fn new(name: &str, core: u32, io_size: u64, bs: Arc<Mutex<Blobstore>>) -> Self;
//     /// Unload BlobEngine, includign unload blobstore
//     pub async fn unload(&self);
//     /// Write data to a blob
//     pub async fn write(&self, offset: u64, blob: Blob, buf: &[u8]) -> Result<()>;
//     /// Read data from a blob
//     pub async fn read(&self, offset: u64, blob: Blob, buf: &mut [u8]) -> Result<()>;
//     /// Delete a blob
//     pub async fn delete_blob(&self, blob_id: BlobId) -> Result<()>;
//     /// Create a blob with zero capacity
//     pub async fn create_blob(&self) -> Result<BlobId>;
//     /// Open a blob
//     pub async fn open_blob(&self, bid: BlobId) -> Result<Blob>;
//     /// Resize a blob
//     pub async fn resize_blob(&self, blob: Blob, size: u64) -> Result<()>;
//     /// Sync blob's metadata to disk
//     pub async fn sync_blob(&self, blob: Blob) -> Result<()>;
//     /// Close a blob, all blobs must be closed before unload blobstore
//     pub async fn close_blob(&self, blob: Blob) -> Result<()>;
// }
