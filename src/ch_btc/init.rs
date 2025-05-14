use bitcoincore_rpc::RpcApi;
use documented::Documented;
use log::{debug, info, warn};
use std::error::Error;
use url::Url;

use crate::{
    ch_btc::schema::{BlockRow, InputRow, OutputRow},
    ProviderType,
};

// Helper function to insert batch data into ClickHouse
async fn insert_batch_data(
    client: &clickhouse::Client,
    block_rows: &[BlockRow],
    input_rows: &[InputRow],
    output_rows: &[OutputRow],
) -> Result<(), Box<dyn Error>> {
    if !input_rows.is_empty() {
        let mut inserter = client.insert("inputs")?;
        for row in input_rows {
            inserter.write(row).await?;
        }
        inserter.end().await?;
    }

    if !output_rows.is_empty() {
        let mut inserter = client.insert("outputs")?;
        for row in output_rows {
            inserter.write(row).await?;
        }
        inserter.end().await?;
    }

    if !block_rows.is_empty() {
        let mut inserter = client.insert("blocks")?;
        for row in block_rows {
            inserter.write(row).await?;
        }
        inserter.end().await?;
    }

    Ok(())
}

pub(crate) async fn init(
    db_url_str: String,
    provider_url_str: String,
    _provider_type: ProviderType,
    from: u64,
    batch_size: u64,
) -> Result<(), Box<dyn Error>> {
    // Create connection to ClickHouse
    let parsed_db_url = Url::parse(&db_url_str)?;
    let database_name = parsed_db_url.path().strip_prefix('/').unwrap_or("default"); // Assuming 'default' is an acceptable fallback

    let ch_client = clickhouse::Client::default()
        .with_url(&db_url_str)
        .with_database(database_name);

    // Create connection to Bitcoin RPC provider
    let bitcoin_rpc_url = Url::parse(&provider_url_str)?;
    let rpc_client = bitcoincore_rpc::Client::new(
        &format!(
            "{}://{}:{}",
            bitcoin_rpc_url.scheme(),
            bitcoin_rpc_url
                .host_str()
                .ok_or("Bitcoin RPC host not found in URL")?,
            bitcoin_rpc_url.port_or_known_default().unwrap_or(8332), // Default Bitcoin RPC port
        ),
        bitcoincore_rpc::Auth::UserPass(
            bitcoin_rpc_url.username().to_string(),
            bitcoin_rpc_url.password().unwrap_or("").to_string(),
        ),
    )?;

    debug!("Start initializing schema");
    ch_client
        .query(&format!("CREATE DATABASE IF NOT EXISTS {}", database_name))
        .execute()
        .await?;

    // Create tables
    ch_client.query(BlockRow::DOCS).execute().await?;
    ch_client.query(InputRow::DOCS).execute().await?;
    ch_client.query(OutputRow::DOCS).execute().await?;

    let latest_height = rpc_client.get_block_count()? - 1;
    let to = latest_height / 1000 * 1000; // Process up to the largest multiple of 1000 <= latest_height
    warn!("Target block height: {}", to);

    if from > to {
        info!(
            "'from' height ({}) is greater than target height ({}). No blocks to process.",
            from, to
        );
        return Ok(());
    }
    if batch_size == 0 {
        return Err("Batch size must be greater than 0".into());
    }

    let mut block_row_list = Vec::with_capacity(batch_size as usize);
    let mut input_row_list = Vec::new(); // Capacity will grow as needed
    let mut output_row_list = Vec::new(); // Capacity will grow as needed

    for num in from..=to {
        let height = num;

        let block_hash_rpc = rpc_client.get_block_hash(height)?;
        let block = rpc_client.get_block(&block_hash_rpc)?;

        block_row_list.push(BlockRow::from_bitcoin_rpc(height, &block));

        for tx in &block.txdata {
            for (index, vin) in tx.input.iter().enumerate() {
                input_row_list.push(InputRow::from_bitcoin_rpc(
                    height,
                    &block,
                    &tx,
                    index as u32,
                    vin,
                ));
            }

            for (index, vout) in tx.output.iter().enumerate() {
                output_row_list.push(OutputRow::from_bitcoin_rpc(
                    height,
                    &block,
                    &tx,
                    index as u32,
                    vout,
                ));
            }
        }

        // Insert data if batch is full or if it's the last block in the range
        if block_row_list.len() >= batch_size as usize || num == to {
            if !block_row_list.is_empty() {
                // Ensure there's data to insert
                insert_batch_data(
                    &ch_client,
                    &block_row_list,
                    &input_row_list,
                    &output_row_list,
                )
                .await?;

                info!(
                    "Inserted batch of {} blocks (up to height {})",
                    block_row_list.len(),
                    num
                );

                block_row_list.clear();
                input_row_list.clear();
                output_row_list.clear();
            }
        }
    }

    info!("BTC initialization complete!");
    Ok(())
}
