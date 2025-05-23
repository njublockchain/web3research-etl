use bitcoin::hashes::Hash;
use bitcoincore_rpc::RpcApi;
use futures::StreamExt;
use klickhouse::{Client, Row};
use log::{debug, info, warn};
use std::error::Error;
use url::Url;

use crate::{ch_btc::sync::insert_block, ProviderType};

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
            default_database: clickhouse_url
                .path()
                .to_string()
                .strip_prefix('/')
                .unwrap()
                .to_string(),
        }
    } else {
        klickhouse::ClientOptions::default()
    };

    let client = klickhouse::Client::connect(
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
    struct NumberRow {
        number: u64,
    }

    debug!("start checking for missing blocks");
    let latest = provider.get_block_count()? - 1;
    info!("current chain height {}", latest);

    // Find missing blocks
    let query = r#"
        WITH (SELECT max(height) FROM blocks) AS max_number
        SELECT
            height as number
        FROM
            (SELECT arrayJoin(range(1, max_number + 1)) AS height)
        WHERE
            height NOT IN (SELECT height FROM blocks)
        ORDER BY height;
    "#;

    let mut missing_blocks_stream = client.query::<NumberRow>(query).await?;
    let mut missing_blocks = Vec::new();
    while let Some(row) = missing_blocks_stream.next().await {
        let row = row?;
        missing_blocks.push(row);
    }

    let missing_count = missing_blocks.len();
    info!("found {} missing blocks", missing_count);

    // Also check for blocks that might be missing after the current max block
    let max_query = "SELECT max(height) as number FROM blocks";
    let max_block = client.query_one::<NumberRow>(max_query).await?;

    info!("local max height {}", max_block.number);

    // Process missing blocks found by the query
    for row in missing_blocks {
        info!("processing missing block {}", row.number);
        health_check(client.clone(), &provider, &None, provider_type, row.number).await;
    }

    // Process blocks from the max_block to the latest block if needed
    if max_block.number < latest {
        info!(
            "processing blocks from {} to {}",
            max_block.number + 1,
            latest
        );
        for num in (max_block.number + 1)..=latest {
            health_check(client.clone(), &provider, &None, provider_type, num).await;
        }
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
        .query_one::<BlockHashRow>(format!("SELECT hash FROM blocks WHERE height = {}", num))
        .await;
    if block.is_err() {
        warn!("add missing block: {}, {:?}", num, block);
        insert_block(&client, provider, trace_provider, provider_type, num)
            .await
            .unwrap();
    } else {
        let block_hash_on_store = block.unwrap().hash;
        let block_hash_on_chain =
            hex::encode(provider.get_block_hash(num).unwrap().as_byte_array());

        if block_hash_on_store != block_hash_on_chain {
            warn!(
                "fix err block {}: {:?} != {:?}",
                num, block_hash_on_store, block_hash_on_chain
            );
            tokio::try_join!(
                client.execute(format!("DELETE FROM blocks WHERE height = {} ", num)),
                client.execute(format!("DELETE FROM inputs WHERE blockHeight = {}') ", num)),
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
