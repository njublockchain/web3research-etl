use std::error::Error;

use bitcoincore_rpc::RpcApi;
use klickhouse::Client;
use log::{info, warn};
use url::Url;

use crate::{
    ch_btc::schema::{BlockRow, InputRow, OutputRow},
    ProviderType,
};

pub async fn insert_block(
    client: &Client,
    provider: &bitcoincore_rpc::Client,
    _trace_provider: &Option<&bitcoincore_rpc::Client>,
    _provider_type: ProviderType,
    block_number: u64,
) -> Result<(), Box<dyn Error>> {
    let mut block_row_list = Vec::with_capacity((1_u64) as usize);
    let mut input_row_list = Vec::new();
    let mut output_row_list = Vec::new();

    let height = block_number;

    let hash = provider.get_block_hash(height)?;
    // let cli = client.get_jsonrpc_client();
    let block = provider.get_block(&hash)?;

    let block_row = BlockRow::from_bitcoin_rpc(height, &block);

    block_row_list.push(block_row);

    for tx in &block.txdata {
        for (index, vin) in tx.input.iter().enumerate() {
            let input_row =
                InputRow::from_bitcoin_rpc(height, &block, &tx, index.try_into().unwrap(), vin);
            input_row_list.push(input_row);
        }

        for (index, vout) in tx.output.iter().enumerate() {
            let output_row =
                OutputRow::from_bitcoin_rpc(height, &block, &tx, index.try_into().unwrap(), vout);
            output_row_list.push(output_row);
        }
    }

    tokio::try_join!(
        client.insert_native_block("INSERT INTO inputs FORMAT native", input_row_list.to_vec()),
        client.insert_native_block(
            "INSERT INTO outputs FORMAT native",
            output_row_list.to_vec()
        ),
    )
    .unwrap();

    client
        .insert_native_block("INSERT INTO blocks FORMAT native", block_row_list.to_vec())
        .await
        .unwrap();

    block_row_list.clear();
    input_row_list.clear();
    output_row_list.clear();

    Ok(())
}

// subscribe to the latest block, and insert it to the database
// TODO: use ZMQ to subscribe to the latest block
pub async fn sync(
    db: String,
    provider: String,
    _trace_provider: Option<String>,
    _provider_type: ProviderType,
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

    let bitcoin_rpc_url = Url::parse(&provider).unwrap();

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

    let mut prev_height = 0;
    let mut prev_hash = String::new();

    loop {
        let latest_block = provider.get_block_count()?;
        if latest_block > prev_height {
            for block_height in prev_height..=latest_block {
                let block_hash = provider.get_block_hash(block_height)?;
                if block_hash.to_string() != prev_hash {
                    info!("add missing block: {}, {:?}", block_height, block_hash);
                    insert_block(&client, &provider, &None, _provider_type, block_height)
                        .await
                        .unwrap();
                    prev_hash = block_hash.to_string();
                }
            }

            info!("latest block: {}, {:?}", latest_block, prev_hash);
            prev_height = latest_block;
            prev_hash = provider.get_block_hash(latest_block)?.to_string();
        } else {
            info!(
                "no new block, latest block: {}, {:?}",
                latest_block, prev_hash
            );
        }

        std::thread::sleep(std::time::Duration::from_secs(10));
    }
}
