use std::error::Error;

use bitcoincore_rpc::{Client, RpcApi};
use klickhouse::Client as Klient;
use log::debug;

pub(crate) async fn init(
    klient: Klient,
    client: Client,
    from: u64,
    batch: u64,
) -> Result<(), Box<dyn Error>> {
    debug!("start initializing schema");
    klient
        .execute(
            "
        CREATE DATABASE IF NOT EXISTS bitcoin;
        ",
        )
        .await?;

    klient
        .execute(
            "
        CREATE DATABASE IF NOT EXISTS bitcoin;
        ",
        )
        .await
        .unwrap();

    let latest_height = client.get_block_count()? - 1;
    let to = latest_height / 1000 * 1000;
    for num in from..=to {
        let hash = client.get_block_hash(num)?;
        // let cli = client.get_jsonrpc_client();
        let block = client.get_block(&hash)?;
    }

    Ok(())
}
