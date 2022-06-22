This is record file for RocksDB + SPDK integration.

- configure spdk with `./configure --with-fuse`, todo: change async-spdk build file
- for RocksDB db_bench, remember to use `export USE_RTTI=1`, in order to avoid some undefined problems
- a Malloc0 configuration file is now in `/usr/local/etc/spdk/rocksdb.json
- to integrate BlobFS to RocksDB:
    - first: `./test/blobfs/mkfs/mkfs /usr/local/etc/spdk/rocksdb.json Malloc0`, to create blobfs on bdev called Malloc0
    - second: `./test/blobfs/fuse/fuse /usr/local/etc/spdk/rocksdb.json Malloc0 /mnt/fuse`, use fuse plug-in to mount blobfs on bdev Malloc0, *now, stuck here*
- to test rocksdb db_bench, use:
    - ./db_bench -spdk /usr/local/etc/spdk/rocksdb.json -spdk_bdev Malloc0 -spdk_cache_size=1024 --benchmarks="readrandomwriterandom,stats" --num=10000 --db=./db_bench_test --wal_dir=./db_bench_test

