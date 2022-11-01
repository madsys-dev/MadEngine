use log::*;
use mad_engine::*;

const PATH: &str = "data";
use tokio::time::Duration;

#[tokio::main]
async fn main() {
    env_logger::init();
    let (handle, mut opts) = FileEngine::new(
        PATH,
        std::env::args().nth(1).expect("expect config file"),
        "0x3",
        "Nvme0n1",
        "Nvme1n1",
        1,
        "test_resize",
        4096,
        1,
        false,
    )
    .await
    .unwrap();
    info!("get handle success");
    handle.create("file1".to_string()).unwrap();
    info!("create file success");

    let size1 = handle.stat("file1".to_string()).unwrap().get_size();
    info!("size1 = {}", size1);

    handle.resize("file1".to_string(), 6000).await.unwrap();
    info!("first resize success");

    let size2 = handle.stat("file1".to_string()).unwrap().get_size();
    info!("size2 = {}", size2);

    handle.resize("file1".to_string(), 3000).await.unwrap();
    info!("second resize success");

    let size3 = handle.stat("file1".to_string()).unwrap().get_size();
    info!("size3 = {}", size3);

    handle.remove("file1".to_string()).unwrap();
    info!("remove file success");
    handle.unload_bs().await.unwrap();
    info!("unload blobstore success");
    drop(handle);
    tokio::time::sleep(Duration::from_secs(1)).await;
    opts.finish();
    info!("close engine success");
}
