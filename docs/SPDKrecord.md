This is record file for RocksDB + SPDK integration.

- configure spdk with `./configure --with-fuse`, todo: change async-spdk build file
- to make RocksDB db_bench, remember to use `export USE_RTTI=1`, in order to avoid some undefined problems
- to test rocksdb db_bench, use:
    - ./db_bench -spdk /usr/local/etc/spdk/rocksdb.json -spdk_bdev Malloc0 -spdk_cache_size=1024 --benchmarks="readrandomwriterandom,stats" --num=10000 --db=./db_bench_test --wal_dir=./db_bench_test
- whenever some dependency changes when doing rocksdb db_bench, use `make clean`, and make again

- use memory as storage device to finish following tests
- mkfs on device called `Malloc0`
    - `./test/blobfs/mkfs/mkfs /usr/local/etc/spdk/rocksdb.json Malloc0` This is SPDK test for mkfs, second variable is the specification json file of the device, last variable is the device name, configured in the json file
    - if succeed, you'll see: `Initializing filesystem on bdev Malloc0...done.`
    - on real NVMe SSD, you shall use SPDK scripts to do the configuration as follows
    - `scripts/gen_nvme.sh --json-with-subsystems > /usr/local/etc/spdk/rocksdb.json` This will detect the correct NVMe SSD and generate configuration file automatically.
    - `HUGEMEM=5120 scripts/setup.sh` This will reserve 5GB hugepage for DPDK, and unbind device from OS
    - `./test/blobfs/mkfs/mkfs /usr/local/etc/spdk/rocksdb.json Malloc0` do the same job
    - `test/blobfs/fuse/fuse /usr/local/etc/spdk/rocksdb.json Nvme0n1 /mnt/fuse` This will use fuse to mount BlobFS. You will see "Mounting filesystem on bdev Nvme0n1 to path /mnt/fuse... done" if succeed

- use SPDK rpc and fuse to mount blobfs to test the correctness of blobfs
    - on one terminal: `./build/bin/spdk_tgt`, this will call the spdk rpc framework to reveive rpc request
    - on another terminal: `./scripts/rpc.py bdev_malloc_create 512 4096`, this command create a bdev device based on memory("malloc" indicates that), with size 4096MiB (I guess), this means more than 4096 hugemem should be reseverd. This will echo the name of the created bdev, generally "Malloc0", if succeed
    - `./scripts/rpc.py blobfs_create Malloc0`, create a BlobFS on bdev call "Malloc0" (mentioned above). This will echo "True" if succeed. 
    - `./scripts/rpc.py blobfs_mount Malloc0 /mnt/fuse`, use fuse to mount this FS. This will echo "True" if succeed.
    - `$ROCKSDB_DIR/db_bench --benchmarks="readrandomwriterandom,stats" --num=10000000 --db=/mnt/fuse --wal_dir=/mnt/fuse`, this is self-designed RocksDB db_bench test. This will print the performance result if succeed.

- a blog [从SPDK Blobstore到 Blob FS](https://blog.csdn.net/weixin_37097605/article/details/124977125?spm=1001.2101.3001.6650.1&utm_medium=distribute.pc_relevant.none-task-blog-2%7Edefault%7ECTRLIST%7Edefault-1-124977125-blog-119154739.pc_relevant_default&depth_1-utm_source=distribute.pc_relevant.none-task-blog-2%7Edefault%7ECTRLIST%7Edefault-1-124977125-blog-119154739.pc_relevant_default&utm_relevant_index=2) introduce an RPC style to create and mount blobfs

## SSD environment
- Close `secure boot` in your machine. This might cause problem like `pcieXXXX`
- Use DmaBuf(DPDK tool?). This might cause problems like `io error`
- A detailed [introduction](https://its201.com/article/cyq6239075/106732499) to SPDK