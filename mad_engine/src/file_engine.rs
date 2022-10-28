//! This module implemente basic read/write API

use crate::common::*;
// use crate::transactiondb_engine::*;
use crate::db_engine::*;
use crate::error::{EngineError, Result};
use crate::utils::*;
use crate::BlobEngine;
use crate::BsBindOpts;
use crate::EngineOpts;
use async_spdk::env::DmaBuf;
use rusty_pool::ThreadPool;
use std::time::Duration;
use std::{
    collections::HashMap,
    path::Path,
    sync::{Arc, Mutex},
};

// TODO:
const IO_SIZE: u64 = 512;

pub struct FileEngine {
    // db: Arc<RocksdbEngine>,
    db: Arc<DbEngine>,
    blob_engine: Arc<BlobEngine>,
    mad_engine: Arc<Mutex<MadEngine>>,
    pool: ThreadPool,
}

impl Drop for FileEngine {
    fn drop(&mut self) {}
}

impl FileEngine {
    /// get a file engine handle
    pub async fn new(
        path: impl AsRef<Path>,
        config_file: String,
        reactor_mask: &str,
        blobfs_dev: &str,
        // now only provide one single blobstore
        bs_dev: &str,
        bs_core: u32,
        app_name: &str,
        cache_size_in_mb: u64,
        init_blob_size: u64,
        is_reload: bool,
    ) -> Result<(Self, EngineOpts)> {
        // Set SPDK opts
        let mut opts = EngineOpts::default();
        opts.set_blobfs(blobfs_dev);
        opts.set_reactor_mask(reactor_mask);
        opts.set_blobstore(vec![BsBindOpts {
            bdev_name: bs_dev.to_string(),
            core: bs_core,
        }]);
        opts.set_config_file(config_file.clone());
        opts.set_name(app_name);

        // Start SPDK environment
        opts.start_spdk(is_reload);

        // Wait for blobfs and blobstore establishing
        opts.ready();

        // Build TransactionDB
        // let db = Arc::new(RocksdbEngine::new(
        //     opts.fs.clone(),
        //     0,
        //     path,
        //     &config_file,
        //     blobfs_dev,
        //     cache_size_in_mb,
        // )?);
        let db = Arc::new(DbEngine::new(
            opts.fs.clone(),
            0,
            path,
            &config_file,
            blobfs_dev,
            cache_size_in_mb,
        )?);

        let be = Arc::new(opts.create_be());

        let pool = ThreadPool::new(NUM_THREAD, NUM_THREAD, Duration::from_secs(1));
        let num_init = NUM_THREAD;
        if !is_reload {
            // TODO: finish cluster count API
            let mad_engine = Arc::new(Mutex::new(MadEngine::new(256)));
            for _ in 0..num_init {
                let handle = be.clone();
                let blob_id = handle.create_blob().await?;
                let blob = handle.open_blob(blob_id).await?;
                handle.resize_blob(blob, init_blob_size).await?;
                handle.sync_blob(blob).await?;
                handle.close_blob(blob).await?;

                let db = db.clone();
                let me = mad_engine.clone();

                // do the initialization work for each thread
                pool.spawn(async move {
                    TLS.with(move |f| {
                        let mut br = f.borrow_mut();
                        br.tblobs = vec![blob_id.clone()];
                        br.tfree_list = HashMap::new();
                        let bitmap = BitMap::new(BLOB_SIZE * CLUSTER_SIZE);
                        br.tfree_list.insert(blob_id.clone(), bitmap.clone());
                        {
                            let mut l = me.lock().unwrap();
                            l.blobs.push(blob_id.clone());
                            l.free_list
                                .insert(blob_id.clone().to_string(), bitmap.clone());
                        }
                        br.db = Some(db);
                    });
                });
            }
            pool.join();
            let global = {
                let magic = mad_engine.lock().unwrap();
                magic.clone()
            };

            db.put(
                Hasher::new().checksum(MAGIC.as_bytes()).to_string(),
                serde_json::to_string(&global).unwrap().as_bytes(),
            )?;

            Ok((
                Self {
                    db,
                    blob_engine: be,
                    mad_engine,
                    pool,
                },
                opts,
            ))
        } else {
            let global = db
                .get(Hasher::new().checksum(MAGIC.as_bytes()).to_string())
                .unwrap();
            if global.is_none() {
                return Err(EngineError::RestoreFail);
            }
            let global_meta: MadEngine =
                serde_json::from_slice(&String::from_utf8(global.unwrap()).unwrap().as_bytes())
                    .unwrap();
            let mad_engine = Arc::new(Mutex::new(global_meta));
            let num_blobs = {
                let l = mad_engine.lock().unwrap();
                l.blobs.len()
            };
            for i in 0..num_init {
                let db = db.clone();
                let me = mad_engine.clone();
                let mut thread_blobids = vec![];
                let mut blob2map = HashMap::new();
                {
                    // let l = me.lock().unwrap();
                    let mut id = i;
                    while id < num_blobs {
                        thread_blobids.push(me.lock().unwrap().blobs[id]);
                        blob2map.insert(
                            me.lock().unwrap().blobs[id],
                            me.lock()
                                .unwrap()
                                .free_list
                                .get(&me.lock().unwrap().blobs[id].to_string())
                                .unwrap()
                                .clone(),
                        );
                        id += num_init;
                    }
                }
                pool.spawn(async move {
                    TLS.with(move |f| {
                        let mut br = f.borrow_mut();
                        br.tblobs = thread_blobids;
                        br.tfree_list = blob2map;
                        br.db = Some(db);
                    });
                });
            }
            pool.join();
            Ok((
                Self {
                    db,
                    blob_engine: be,
                    mad_engine,
                    pool,
                },
                opts,
            ))
        }
    }

    /// remove file
    ///
    /// TODO: there should be a backend thread to recycle blob
    pub fn remove(&self, name: String) -> Result<()> {
        self.db.delete(name)?;
        Ok(())
    }

    /// create file
    pub fn create(&self, name: String) -> Result<()> {
        let chunk_meta = ChunkMeta::default();
        self.db
            .put(name, serde_json::to_string(&chunk_meta).unwrap().as_bytes())?;
        Ok(())
    }

    /// get a file state
    pub fn stat(&self, name: String) -> Result<StatMeta> {
        let chunk_meta = self.db.get(name)?;
        if chunk_meta.is_none() {
            return Err(EngineError::MetaNotExist);
        }
        let chunk_meta: ChunkMeta =
            serde_json::from_slice(&String::from_utf8(chunk_meta.unwrap()).unwrap().as_bytes())?;
        let ret = StatMeta {
            size: chunk_meta.size,
            csum_type: chunk_meta.csum_type.clone(),
        };
        Ok(ret)
    }

    /// get all new positions to write new data
    /// recycle old positions
    fn allocate_and_recycle_poses(
        &self,
        poses_copy: Vec<PagePos>,
        mut global_meta: MadEngine,
        total_page_num: u64,
    ) -> (Vec<PagePos>, HashMap<String, BitMap>) {
        let db_copy = self.db.clone();
        let (new_poses, new_blob2map) = self
            .pool
            .complete(async move {
                let mut ret = vec![];
                let mut new_blob2map = global_meta.free_list.clone();
                TLS.with(|f| {
                    let mut br = f.borrow_mut();
                    let mut cnt = total_page_num;
                    while cnt > 0 {
                        let tblobs = br.tblobs.clone();
                        for bid in tblobs.iter() {
                            let bm = br.tfree_list.get_mut(&bid);
                            if bm.is_none() {
                                continue;
                            }
                            let bm = bm.unwrap();
                            let idx = bm.find().unwrap();
                            // TODO: here should check full or not
                            ret.push(PagePos {
                                bid: bid.clone(),
                                offset: idx,
                            });
                            bm.set(idx);
                            global_meta
                                .free_list
                                .insert(bid.clone().to_string(), bm.clone());
                            cnt -= 1;
                            if cnt == 0 {
                                break;
                            }
                        }
                    }
                    // recycle old pages
                    // TODO: implement a merger
                    let mut new_blobs = vec![];
                    for pos in poses_copy {
                        let e = new_blob2map.get_mut(&pos.bid.to_string()).unwrap();
                        e.clear(pos.offset);
                        let mut flag = false;
                        let tblobs = br.tblobs.clone();
                        for bid in tblobs.iter() {
                            if *bid == pos.bid {
                                let bm = br.tfree_list.get_mut(&bid);
                                bm.unwrap().clear(pos.offset);
                                flag = true;
                                break;
                            }
                        }
                        // not in the old tblobs, but maybe in the newblobs
                        if flag == false {
                            for bid in new_blobs.iter() {
                                if *bid == pos.bid {
                                    flag = true;
                                    break;
                                }
                            }
                        }
                        if flag == false {
                            new_blobs.push(pos.bid);
                            let mut bm = BitMap::new_set_ones(BLOB_SIZE * CLUSTER_SIZE);
                            bm.clear(pos.offset);
                            br.tfree_list.insert(pos.bid, bm);
                        } else {
                            let bm = br.tfree_list.get_mut(&pos.bid).unwrap();
                            bm.clear(pos.offset);
                        }
                    }
                    for new_blob in new_blobs {
                        br.tblobs.push(new_blob);
                    }
                    global_meta.free_list = new_blob2map.clone();
                    db_copy
                        .put(
                            Hasher::new().checksum(MAGIC.as_bytes()).to_string(),
                            serde_json::to_string(&global_meta.clone())
                                .unwrap()
                                .as_bytes(),
                        )
                        .unwrap();
                });
                (ret, new_blob2map)
            })
            .await_complete();
        (new_poses, new_blob2map)
    }

    /// partial write a page
    async fn partial_write(
        &self,
        i: usize,
        pos: &PagePos,
        buf: &mut DmaBuf,
        offset: u64,
        len: u64,
        start_page: u64,
        end_page: u64,
        io_size: u64,
        data_anchor: &mut usize,
        data: &[u8],
        checksum_vec: &mut Vec<u32>,
        idx_anchor: &mut usize,
        new_poses: &Vec<PagePos>,
    ) -> Result<()> {
        self.blob_engine
            .read(pos.offset, pos.bid, buf.as_mut())
            .await
            .unwrap();
        let buffer = buf.as_mut();
        buffer[0..(offset + len - end_page * io_size) as usize]
            .copy_from_slice(&data[*data_anchor..]);
        checksum_vec[i + start_page as usize] = Hasher::new().checksum(buffer.as_ref());
        self.blob_engine
            .write(
                new_poses[*idx_anchor].offset,
                new_poses[*idx_anchor].bid,
                buffer,
            )
            .await
            .unwrap();
        *idx_anchor = *idx_anchor + 1;
        *data_anchor = data.len();
        Ok(())
    }

    /// write file
    pub async fn write(&self, name: String, offset: u64, data: &[u8]) -> Result<()> {
        let io_size = IO_SIZE;
        let len = data.len() as u64;

        // get and check metadata
        let chunk_meta = self.db.get(&name).unwrap();
        if chunk_meta.is_none() {
            return Err(EngineError::MetaNotExist);
        }
        let mut chunk_meta: ChunkMeta =
            serde_json::from_slice(&String::from_utf8(chunk_meta.unwrap()).unwrap().as_bytes())
                .unwrap();
        let size = chunk_meta.get_size();
        if offset > size {
            return Err(EngineError::HoleNotAllowed);
        }

        let mut checksum_vec = chunk_meta.csum_data.clone();
        let start_page = offset / io_size;
        // overwritten pages
        let cover_end_page = size.min(offset + len - 1) / io_size;
        // actual end page
        let end_page = (offset + len - 1) / io_size;
        // last page before this write, -1 if this is first write
        let mut last_page = size as i64 / io_size as i64;

        // first write
        if size == 0 {
            last_page = -1;
        }
        let total_page_num = end_page - start_page + 1;

        // global metadata
        let global = self
            .db
            .get(Hasher::new().checksum(MAGIC.as_bytes()).to_string())?;
        if global.is_none() {
            return Err(EngineError::GlobalGetFail);
        }
        let global_meta: MadEngine =
            serde_json::from_slice(&String::from_utf8(global.unwrap()).unwrap().as_bytes())
                .unwrap();
        let poses = if size == 0 {
            vec![]
        } else {
            (start_page..=cover_end_page)
                .map(|p| {
                    chunk_meta
                        .location
                        .clone()
                        .unwrap()
                        .get(&p)
                        .unwrap()
                        .clone()
                })
                .collect::<Vec<_>>()
        };

        // get all new positions to write new data
        // recycle old positions
        let poses_copy = poses.clone();
        let (new_poses, new_blob2map) =
            Self::allocate_and_recycle_poses(&self, poses_copy, global_meta, total_page_num);

        // *self.mad_engine.lock().unwrap().free_list = new_blob2map;
        {
            let mut l = self.mad_engine.lock().unwrap();
            l.free_list = new_blob2map;
        }
        let mut idx_anchor = 0;
        let mut data_anchor = 0;
        for (i, pos) in poses.iter().enumerate() {
            let mut buf = DmaBuf::alloc((io_size) as usize, 0x1000);
            // read first page
            if i == 0 {
                self.blob_engine
                    .clone()
                    .read(pos.offset, pos.bid, buf.as_mut())
                    .await
                    .unwrap();
                let buffer = buf.as_mut();
                buffer[(offset - start_page * io_size) as usize..].copy_from_slice(
                    &data[data_anchor..(io_size - (offset - start_page * io_size)) as usize],
                );
                checksum_vec[i + start_page as usize] = Hasher::new().checksum(buffer.as_ref());
                self.blob_engine
                    .clone()
                    .write(
                        new_poses[idx_anchor].offset,
                        new_poses[idx_anchor].bid,
                        buffer,
                    )
                    .await
                    .unwrap();
                idx_anchor += 1;
                data_anchor += (io_size - (offset - start_page * io_size)) as usize;
            } else if i == poses.len() - 1 {
                // all over write
                if end_page as i64 == last_page {
                    /*
                        origin last page: |------00000|
                        write:            |xxxxxxxxx00|
                    */
                    if size <= offset + len {
                        let buffer = buf.as_mut();
                        buffer.fill(0);
                        buffer[0..(data.len() - data_anchor)].copy_from_slice(&data[data_anchor..]);
                        checksum_vec[i + start_page as usize] =
                            Hasher::new().checksum(buffer.as_ref());
                        self.blob_engine
                            .clone()
                            .write(
                                new_poses[idx_anchor].offset,
                                new_poses[idx_anchor].bid,
                                buffer,
                            )
                            .await
                            .unwrap();
                        idx_anchor += 1;
                        data_anchor = data.len();
                    }
                    /*
                        origin last page: |------00000|
                        write:            |xxxx0000000|
                    */
                    else {
                        Self::partial_write(
                            &self,
                            i,
                            &pos,
                            &mut buf,
                            offset,
                            len,
                            start_page,
                            end_page,
                            io_size,
                            &mut data_anchor,
                            data,
                            &mut checksum_vec,
                            &mut idx_anchor,
                            &new_poses,
                        )
                        .await
                        .unwrap();
                    }
                }
                // partial overwrite, not exceed last page
                /*
                    origin: |---|---|---|---|---|
                    write:     |xxxxxxxxxx|
                */
                else if last_page > end_page as i64 {
                    Self::partial_write(
                        &self,
                        i,
                        &pos,
                        &mut buf,
                        offset,
                        len,
                        start_page,
                        end_page,
                        io_size,
                        &mut data_anchor,
                        data,
                        &mut checksum_vec,
                        &mut idx_anchor,
                        &new_poses,
                    )
                    .await
                    .unwrap();
                }
                /*
                    origin: |---|---|---|---|---|
                    write:     |xxxxxxxxxxxxxxxxxxx|
                */
                else {
                    self.blob_engine
                        .write(
                            new_poses[idx_anchor].offset,
                            new_poses[idx_anchor].bid,
                            &data[data_anchor..(data_anchor + io_size as usize)],
                        )
                        .await
                        .unwrap();
                    checksum_vec[i + start_page as usize] = Hasher::new()
                        .checksum(&data[data_anchor..(data_anchor + io_size as usize)]);
                    idx_anchor += 1;
                    data_anchor += io_size as usize;
                }
            } else {
                self.blob_engine
                    .write(
                        new_poses[idx_anchor].offset,
                        new_poses[idx_anchor].bid,
                        &data[data_anchor..(data_anchor + io_size as usize)],
                    )
                    .await
                    .unwrap();
                checksum_vec[i + start_page as usize] =
                    Hasher::new().checksum(&data[data_anchor..(data_anchor + io_size as usize)]);
                idx_anchor += 1;
                data_anchor += io_size as usize;
            }
        }
        if end_page as i64 > last_page {
            for page in last_page + 1..=end_page as i64 {
                let mut buf = DmaBuf::alloc((io_size) as usize, 0x1000);
                buf.as_mut().fill(0);
                if page as u64 == end_page {
                    let buffer = buf.as_mut();
                    buffer[0..(data.len() - data_anchor)].copy_from_slice(&data[data_anchor..]);
                    checksum_vec.push(Hasher::new().checksum(buffer.as_ref()));
                    self.blob_engine
                        .write(
                            new_poses[idx_anchor].offset,
                            new_poses[idx_anchor].bid,
                            buffer,
                        )
                        .await
                        .unwrap();
                    idx_anchor += 1;
                    data_anchor = data.len();
                } else {
                    buf.as_mut()
                        .copy_from_slice(&data[data_anchor..(data_anchor + io_size as usize)]);
                    self.blob_engine
                        .write(
                            new_poses[idx_anchor].offset,
                            new_poses[idx_anchor].bid,
                            buf.as_ref(),
                        )
                        .await
                        .unwrap();
                    checksum_vec.push(
                        Hasher::new()
                            .checksum(&data[data_anchor..(data_anchor + io_size as usize)]),
                    );
                    idx_anchor += 1;
                    data_anchor += io_size as usize;
                }
            }
        }
        chunk_meta.size = size.max(offset + data.len() as u64);
        let mut locations = match chunk_meta.location {
            Some(l) => l.clone(),
            None => HashMap::new(),
        };
        let mut idx = 0;
        for page in start_page..=end_page {
            let pos = PagePos {
                bid: new_poses[idx].bid,
                offset: new_poses[idx].offset,
            };
            locations.insert(page, pos);
            idx += 1;
        }
        chunk_meta.location = Some(locations);
        chunk_meta.csum_data = checksum_vec;
        chunk_meta.csum_type = "CRC32".into();
        self.db
            .put(name, serde_json::to_string(&chunk_meta).unwrap().as_bytes())?;
        Ok(())
    }

    /// read a chunk, this should return the read length
    ///
    /// TODO: check read range, return read length
    pub async fn read(&self, name: String, offset: u64, data: &mut [u8]) -> Result<()> {
        let len = data.len() as u64;
        let io_size = IO_SIZE;
        let start_page = offset / io_size;
        let end_page = (offset + len - 1) / io_size;

        let chunk_meta = self.db.get(name)?;
        if chunk_meta.is_none() {
            return Err(EngineError::MetaNotExist);
        }
        let chunk_meta: ChunkMeta =
            serde_json::from_slice(&String::from_utf8(chunk_meta.unwrap()).unwrap().as_bytes())?;
        let size = chunk_meta.size;
        if offset >= size || offset + len > size {
            return Err(EngineError::ReadOutRange);
        }

        let poses = (start_page..=end_page)
            .map(|p| {
                chunk_meta
                    .location
                    .clone()
                    .unwrap()
                    .get(&p)
                    .unwrap()
                    .clone()
            })
            .collect::<Vec<_>>();
        let checksum_vec = chunk_meta.csum_data.clone();
        let mut buf = DmaBuf::alloc((io_size) as usize, 0x1000);
        let mut anchor = 0;
        for (i, pos) in poses.iter().enumerate() {
            self.blob_engine
                .clone()
                .read(pos.offset, pos.bid, buf.as_mut())
                .await?;
            if Hasher::new().checksum(buf.as_ref()) != checksum_vec[i + start_page as usize] {
                return Err(EngineError::CheckSumErr);
            }
            // last page
            if i == poses.len() - 1 {
                let end = offset + len - 1 - end_page * io_size;
                let buffer = buf.as_ref();
                data[anchor..].copy_from_slice(&buffer[0..=end as usize]);
            }
            // first page
            else if i == 0 {
                let buffer = buf.as_ref();
                data[anchor..((start_page + 1) * io_size - offset) as usize]
                    .copy_from_slice(&buffer[((offset - start_page * io_size) as usize)..]);
                anchor += ((start_page + 1) * io_size - offset) as usize;
            } else {
                data[anchor..(anchor + io_size as usize)].copy_from_slice(&buf.as_ref());
                anchor += io_size as usize;
            }
        }
        Ok(())
    }

    /// unload blobstore
    pub async fn unload_bs(&self) -> Result<()> {
        self.blob_engine.unload().await;
        Ok(())
    }

    /// close thread pool
    pub fn close_engine(&mut self) -> Result<()> {
        self.pool.to_owned().shutdown();
        Ok(())
    }
}
