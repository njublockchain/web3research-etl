use std::{
    collections::{HashMap, HashSet},
    error::Error,
};

use bitcoin::{hashes::Hash, Address};
use bitcoincore_rpc::RpcApi;
use futures::StreamExt;
use klickhouse::{Client, Row};
use log::{info, warn};
use url::Url;

use crate::{
    ch_btc::schema::{get_address, BlockRow, InputRow, OutputRow},
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

    let mut prev_vout_addresses: HashMap<(String, u32), Option<String>> = HashMap::new();

    let mut missing_prev_vouts: HashSet<(String, u32)> = HashSet::new();

    // batch fetch vout address from clickhosue
    for tx in block.txdata.iter() {
        for vin in tx.input.iter() {
            if vin.previous_output.txid != bitcoin::Txid::all_zeros()
                && !prev_vout_addresses.contains_key(&(
                    vin.previous_output.txid.to_string(),
                    vin.previous_output.vout,
                ))
            {
                missing_prev_vouts.insert((
                    vin.previous_output.txid.to_string(),
                    vin.previous_output.vout,
                ));
            }
        }
    }

    #[derive(Row, Clone, Debug, Default)]
    #[klickhouse(rename_all = "camelCase")]
    struct OutputAddressResult {
        txid: String,
        index: u32,
        address: Option<String>,
    }

    // Split the query into chunks to avoid max_query_size limit
    if !missing_prev_vouts.is_empty() {
        const CHUNK_SIZE: usize = 1000; // FIXME

        for chunk in missing_prev_vouts
            .iter()
            .cloned()
            .collect::<Vec<_>>()
            .chunks(CHUNK_SIZE)
        {
            if chunk.is_empty() {
                continue;
            }

            let conditions = chunk
                .iter()
                .map(|k| format!("('{}', {})", k.0, k.1))
                .collect::<Vec<_>>()
                .join(", ");
            let query = format!(
                "SELECT txid, index, address FROM outputs WHERE (txid, index) IN ({})",
                conditions
            );

            let mut result = client
                .query::<OutputAddressResult>(query)
                .await
                .expect(&format!(
                    "Failed to query some of the previous outputs: {:?}",
                    chunk
                ));
            while let Some(row) = result.next().await {
                let row = row?;
                prev_vout_addresses.insert((row.txid, row.index), row.address);
            }
        }
    }

    for (tx_index, tx) in block.txdata.iter().enumerate() {
        for (index, vin) in tx.input.iter().enumerate() {
            let address = if vin.previous_output.txid == bitcoin::Txid::all_zeros() {
                // Handle coinbase transaction
                Some("Coinbase".to_string())
            } else {
                Address::from_script(
                    &provider
                        .get_raw_transaction(&vin.previous_output.txid, None)
                        .unwrap()
                        .tx_out(vin.previous_output.vout.try_into().unwrap())
                        .unwrap()
                        .script_pubkey,
                    bitcoin::Network::Bitcoin,
                )
                .ok()
                .map(|s| s.to_string())
            };

            let input_row = InputRow::from_bitcoin_rpc(
                height,
                &block,
                &tx,
                tx_index.try_into().unwrap(),
                index.try_into().unwrap(),
                vin,
                address,
            );
            input_row_list.push(input_row);
        }

        for (index, vout) in tx.output.iter().enumerate() {
            let address = get_address(&vout.script_pubkey);
            if address.is_none() {
                warn!(
                    "Cannot decode script pubkey: {} on tx {} index {}",
                    vout.script_pubkey.to_asm_string(),
                    tx.compute_txid(),
                    index
                );
            }
            prev_vout_addresses.insert(
                (tx.compute_txid().to_string(), index.try_into().unwrap()),
                address.clone(),
            );

            let output_row = OutputRow::from_bitcoin_rpc(
                height,
                &block,
                &tx,
                tx_index.try_into().unwrap(),
                index.try_into().unwrap(),
                vout,
                address,
            );
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

    #[derive(Row, Clone, Debug)]
    struct MaxBlock {
        height: u64,
        hash: String,
    }

    let local_latest_block = client
        .query_one::<MaxBlock>(
            "
WITH (SELECT max(height) FROM blocks) AS max_block_height
SELECT height, hash FROM blocks WHERE height = max_block_height",
        )
        .await?;
    info!(
        "db latest block: {}@{}",
        local_latest_block.hash, local_latest_block.height
    );

    let mut local_latest_height = local_latest_block.height;
    let mut local_latest_hash = local_latest_block.hash.clone();

    loop {
        let latest_block_hash = provider.get_best_block_hash()?;
        let latest_block = provider.get_block(&latest_block_hash)?;

        let remote_latest_height = latest_block
            .bip34_block_height()
            .unwrap_or(provider.get_block_count()?);

        info!(
            "remote latest block: {}@{}, local latest block: {}@{}",
            latest_block_hash.to_string(),
            remote_latest_height,
            local_latest_hash,
            local_latest_height
        );

        if remote_latest_height > local_latest_height {
            // Check if we need to handle reorg by verifying the hash at local_latest_height
            let remote_block_at_local_height = provider.get_block_hash(local_latest_height)?;
            let remote_block_at_local_height_hex = remote_block_at_local_height.to_string();

            if remote_block_at_local_height_hex != local_latest_hash {
                warn!(
                    "Potential reorg detected! Local hash: {}, Remote hash: {} at height {}",
                    local_latest_hash, remote_block_at_local_height_hex, local_latest_height
                );

                // Find the common ancestor by going back until hashes match
                let mut check_height = local_latest_height;
                while check_height > 0 {
                    check_height -= 1;

                    let remote_hash = provider.get_block_hash(check_height)?;
                    let remote_hash_hex = remote_hash.to_string();

                    let local_block = client
                        .query_one::<MaxBlock>(&format!(
                            "SELECT height, hash FROM blocks WHERE height = {}",
                            check_height
                        ))
                        .await;

                    match local_block {
                        Ok(local_block) if local_block.hash == remote_hash_hex => {
                            info!("Found common ancestor at height {}", check_height);

                            // Delete blocks after the common ancestor
                            let delete_query = format!(
                                "ALTER TABLE blocks DELETE WHERE height > {}",
                                check_height
                            );
                            client.execute(&delete_query).await?;
                            let delete_inputs_query = format!(
                                "ALTER TABLE inputs DELETE WHERE block_number > {}",
                                check_height
                            );
                            client.execute(&delete_inputs_query).await?;
                            let delete_outputs_query = format!(
                                "ALTER TABLE outputs DELETE WHERE block_number > {}",
                                check_height
                            );
                            client.execute(&delete_outputs_query).await?;

                            info!("Deleted blocks after height {} due to reorg", check_height);

                            // Update local tracking variables
                            local_latest_height = check_height;
                            local_latest_hash = remote_hash_hex;
                            break;
                        }
                        _ => continue,
                    }
                }
            }

            // Sync new blocks from local_latest_height + 1 to remote_latest_height
            for height in (local_latest_height + 1)..=remote_latest_height {
                info!("Syncing block at height {}", height);

                match insert_block(&client, &provider, &None, _provider_type, height).await {
                    Ok(_) => {
                        info!("Successfully synced block at height {}", height);

                        // Update local tracking variables
                        let block_hash = provider.get_block_hash(height)?;
                        local_latest_height = height;
                        local_latest_hash = block_hash.to_string();
                    }
                    Err(e) => {
                        warn!("Failed to sync block at height {}: {}", height, e);
                        // Continue with next iteration to retry
                        break;
                    }
                }
            }
        } else if remote_latest_height == local_latest_height {
            // Check if the hash matches at the same height
            let remote_hash_hex = latest_block_hash.to_string();
            if remote_hash_hex != local_latest_hash {
                warn!(
                    "Hash mismatch at same height {}! Local: {}, Remote: {}",
                    local_latest_height, local_latest_hash, remote_hash_hex
                );

                // Handle the discrepancy - this might be a reorg at the tip
                // Delete the current tip block and re-sync it
                let delete_query = format!(
                    "ALTER TABLE blocks DELETE WHERE height = {}",
                    local_latest_height
                );
                client.execute(&delete_query).await?;
                let delete_inputs_query = format!(
                    "ALTER TABLE inputs DELETE WHERE block_number = {}",
                    local_latest_height
                );
                client.execute(&delete_inputs_query).await?;
                let delete_outputs_query = format!(
                    "ALTER TABLE outputs DELETE WHERE block_number = {}",
                    local_latest_height
                );
                client.execute(&delete_outputs_query).await?;

                // Re-sync the current height
                match insert_block(
                    &client,
                    &provider,
                    &None,
                    _provider_type,
                    local_latest_height,
                )
                .await
                {
                    Ok(_) => {
                        info!(
                            "Successfully re-synced block at height {}",
                            local_latest_height
                        );
                        local_latest_hash = remote_hash_hex;
                    }
                    Err(e) => {
                        warn!(
                            "Failed to re-sync block at height {}: {}",
                            local_latest_height, e
                        );
                    }
                }
            } else {
                info!(
                    "Local and remote are in sync at height {}",
                    local_latest_height
                );
            }
        } else {
            // remote_latest_height < local_latest_height - this shouldn't normally happen
            warn!(
                "Remote height {} is behind local height {}. This is unusual.",
                remote_latest_height, local_latest_height
            );
        }

        std::thread::sleep(std::time::Duration::from_secs(10));
    }
}
