use crate::error::Result;
use async_spdk::blob::{self, BlobId, Blobstore};
use async_spdk::blob_bdev::{self};

#[derive(Debug)]
pub struct DeviceEngine {
    pub bs: Blobstore,
    pub name: String,
    pub io_size: u64,
}

impl DeviceEngine {
    pub fn get_name(&self) -> Result<String> {
        Ok(self.name.clone())
    }

    pub fn get_io_size(&self) -> Result<u64> {
        Ok(self.io_size)
    }
}

impl DeviceEngine {
    pub async fn new(name: &str) -> Result<Self> {
        let bs = DeviceEngine::open_bs(name).await?;
        let size = bs.io_unit_size();
        let ret = DeviceEngine {
            bs,
            name: String::from(name),
            io_size: size,
        };
        Ok(ret)
    }

    async fn open_bs(name: &str) -> Result<Blobstore> {
        let mut bs_dev = blob_bdev::BlobStoreBDev::create(name).unwrap();
        let bs = blob::Blobstore::init(&mut bs_dev).await.unwrap();
        Ok(bs)
    }

    pub async fn write(&self, offset: u64, blob_id: BlobId, buf: &[u8]) -> Result<()> {
        let blob = self.bs.open_blob(blob_id).await.unwrap();
        let channel = self.bs.alloc_io_channel().unwrap();
        blob.write(&channel, offset, buf).await.unwrap();
        blob.close().await.unwrap();
        drop(channel);
        Ok(())
    }

    pub async fn read(&self, offset: u64, blob_id: BlobId, buf: &mut [u8]) -> Result<()> {
        let blob = self.bs.open_blob(blob_id).await.unwrap();
        let channel = self.bs.alloc_io_channel().unwrap();
        blob.read(channel, offset, buf).await.unwrap();
        blob.close().await.unwrap();
        // drop(channel);
        Ok(())
    }

    /// create a blob with #size clusters
    ///
    /// note: size is number of clusters, usually 1MB per cluster
    pub async fn create_blob(&self, size: u64) -> Result<EngineBlob> {
        let blob_id = self.bs.create_blob().await.unwrap();
        let blob = self.bs.open_blob(blob_id).await.unwrap();
        blob.resize(size).await.unwrap();
        blob.sync_metadata().await.unwrap();
        blob.close().await.unwrap();
        Ok(EngineBlob { bl: blob_id })
    }

    pub async fn delete_blob(&self, bid: BlobId) -> Result<()> {
        self.bs.delete_blob(bid).await.unwrap();
        Ok(())
    }

    /// close blobstore handle
    ///
    /// todo: check io_channel closed or not
    pub async fn close_bs(&self) -> Result<()> {
        self.bs.unload().await.unwrap();
        Ok(())
    }

    pub fn total_data_cluster_count(&self) -> Result<u64> {
        Ok(self.bs.total_data_cluster_count())
    }
}

#[derive(Debug)]
pub struct EngineBlob {
    pub bl: BlobId,
}

impl EngineBlob {
    pub fn get_id(&self) -> Result<BlobId> {
        Ok(self.bl)
    }
}
