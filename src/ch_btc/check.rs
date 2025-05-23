use std::error::Error;

use bitcoin::{hashes::Hash};
use bitcoincore_rpc::RpcApi;
use klickhouse::{Client, Row};
use log::{debug, info, warn};
use url::Url;

use crate::{
    ch_btc::sync::insert_block, ProviderType
};

pub(crate) async fn check(
    db: String,
    provider_uri: String,
    _trace_provider_uri: Option<String>,
    provider_type: ProviderType,
    from: u64,
) -> Result<(), Box<dyn Error>> {
    let clickhouse_url = Url::parse(&db).unwrap();
    // warn!("db: {} path: {}", format!("{}:{}", clickhouse_url.host().unwrap(), clickhouse_url.port().unwrap()), clickhouse_url.path());

    let options = if clickhouse_url.path() != "/default" || !clickhouse_url.username().is_empty() {
        warn!("auth enabled for clickhouse");
        klickhouse::ClientOptions {
            username: clickhouse_url.username().to_string(),
            password: clickhouse_url.password().unwrap_or("").to_string(),
            default_database: clickhouse_url.path().to_string().strip_prefix('/').unwrap().to_string(),
        }
    } else {
        klickhouse::ClientOptions::default()
    };

    let klient = klickhouse::Client::connect(
        format!(
            "{}:{}",
            clickhouse_url.host().unwrap(),
            clickhouse_url.port().unwrap()
        ),
        options.clone(),
    )
    .await?;

    let bitcoin_rpc_url = Url::parse(&provider_uri).unwrap();

    let provider = bitcoincore_rpc::Client::new(
        format!(
            "{}://{}:{}",
            bitcoin_rpc_url.scheme(),
            bitcoin_rpc_url.host_str().unwrap_or("localhost"),
            bitcoin_rpc_url.port_or_known_default().unwrap_or(8332),
        )
        .as_str(),
        bitcoincore_rpc::Auth::UserPass(
            bitcoin_rpc_url.username().to_string(),
            bitcoin_rpc_url.password().unwrap_or("").to_string(),
        ),
    )
    .unwrap();

    #[derive(Row, Clone, Debug)]
    struct MaxNumberRow {
        max: u64,
    }

    debug!("start interval update");
    let local_height = klient
        .query_one::<MaxNumberRow>("SELECT max(height) as max FROM blocks")
        .await?;
    info!("local height {}", local_height.max);
    let latest = provider.get_block_count()? - 1;
    info!("checking from {} to height {}", from, latest);

    for num in from..=latest {
        health_check(klient.clone(), &provider, &None, provider_type, num).await;
    }

    Ok(())
}


#[derive(Row, Clone, Debug)]
struct BlockHashRow {
    hash: String,
}

pub async fn health_check(
    client: Client,
    provider: &bitcoincore_rpc::Client,
    trace_provider: &Option<&bitcoincore_rpc::Client>,
    provider_type: ProviderType,
    num: u64,
) {
    let block = client
        .query_one::<BlockHashRow>(format!(
            "SELECT hash FROM blocks WHERE height = {}",
            num
        ))
        .await;
    if block.is_err() {
        warn!("add missing block: {}, {:?}", num, block);
        insert_block(&client, provider, trace_provider, provider_type, num)
            .await
            .unwrap();
    } else {
        let block_hash_on_store = block.unwrap().hash;
        let block_hash_on_chain = hex::encode(provider.get_block_hash(num).unwrap().as_byte_array());

        if block_hash_on_store != block_hash_on_chain {
            warn!(
                "fix err block {}: {:?} != {:?}",
                num, block_hash_on_store, block_hash_on_chain
            );
            tokio::try_join!(
                client.execute(format!(
                    "DELETE FROM blocks WHERE height = {} ",
                    num
                )),
                client.execute(format!(
                    "DELETE FROM inputs WHERE blockHeight = {}') ",
                    num
                )),
                client.execute(format!(
                    "DELETE FROM outputs WHERE blockHeight = {}') ",
                    num
                )),
            )
            .ok();

            insert_block(&client, provider, trace_provider, provider_type, num)
                .await
                .unwrap();
        }
        // no need to check trace
    }
}
