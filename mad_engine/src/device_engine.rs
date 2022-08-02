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
    pub async fn new(name: &str, is_reload: bool) -> Result<Self> {
        let bs = DeviceEngine::open_bs(name, is_reload).await?;
        let size = bs.io_unit_size();
        let ret = DeviceEngine {
            bs,
            name: String::from(name),
            io_size: size,
        };
        Ok(ret)
    }

    async fn open_bs(name: &str, is_reload: bool) -> Result<Blobstore> {
        let mut bs_dev = blob_bdev::BlobStoreBDev::create(name)?;
        let bs = if !is_reload{
            blob::Blobstore::init(&mut bs_dev).await?
        }else{
            blob::Blobstore::load(&mut bs_dev).await?
        };
        Ok(bs)
    }

    pub async fn write(&self, offset: u64, blob_id: BlobId, buf: &[u8]) -> Result<()> {
        let blob = self.bs.open_blob(blob_id).await?;
        let channel = self.bs.alloc_io_channel()?;
        blob.write(&channel, offset, buf).await?;
        blob.close().await?;
        drop(channel);
        Ok(())
    }

    pub async fn read(&self, offset: u64, blob_id: BlobId, buf: &mut [u8]) -> Result<()> {
        let blob = self.bs.open_blob(blob_id).await?;
        let channel = self.bs.alloc_io_channel()?;
        blob.read(channel, offset, buf).await?;
        blob.close().await?;
        // drop(channel);
        Ok(())
    }

    /// create a blob with #size clusters
    ///
    /// note: size is number of clusters, usually 1MB per cluster
    pub async fn create_blob(&self, size: u64) -> Result<EngineBlob> {
        let blob_id = self.bs.create_blob().await?;
        let blob = self.bs.open_blob(blob_id).await?;
        blob.resize(size).await?;
        blob.sync_metadata().await?;
        blob.close().await?;
        Ok(EngineBlob { bl: blob_id })
    }

    pub async fn delete_blob(&self, bid: BlobId) -> Result<()> {
        self.bs.delete_blob(bid).await?;
        Ok(())
    }

    /// close blobstore handle
    ///
    /// todo: check io_channel closed or not
    pub async fn close_bs(&self) -> Result<()> {
        self.bs.unload().await?;
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
