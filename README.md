This is a local storage engine implementation for MadFS.

# TEST instruction
- reserver enough hugepage:
    - `echo "1024" > /sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages`
    - This command reserve 1024MB page from memory (su)
- test5: single thread test, including cross-boundary read/write, metadata restore
    - `cargo run --example test5 ./examples/config_file.json` (sudo)
    - Since this configuration file use memory to simulate SSD, and use local filesystem to support RocksDB, the metadata can be restored correctly(already been tested in test5), but not for the data. So an extra removal for the RocksDB directory should be played before test5.