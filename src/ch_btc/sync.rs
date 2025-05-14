use bitcoin::hashes::Hash;
use bitcoincore_rpc::RpcApi;
use clickhouse::{Client, Row};
use log::warn;
use std::error::Error;

use crate::{
    ch_btc::schema::{BlockRow, InputRow, OutputRow},
    ProviderType,
};

use super::utils;

async fn insert_block(
    client: &clickhouse::Client,
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

    block_row_list.push(BlockRow::from_bitcoin_rpc(height, &block));

    for tx in &block.txdata {
        for (index, vin) in tx.input.iter().enumerate() {
            input_row_list.push(InputRow::from_bitcoin_rpc(
                height,
                &block,
                tx,
                index as u32,
                vin,
            ));
        }

        for (index, vout) in tx.output.iter().enumerate() {
            output_row_list.push(OutputRow::from_bitcoin_rpc(
                height,
                &block,
                tx,
                index as u32,
                vout,
            ));
        }
    }

    block_row_list.clear();
    input_row_list.clear();
    output_row_list.clear();

    Ok(())
}

#[derive(Row, Clone, Debug, serde::Deserialize)]
struct BlockHashRow {
    hash: String,
}

pub async fn health_check(
    client: &Client,
    provider: &bitcoincore_rpc::Client,
    trace_provider: &Option<&bitcoincore_rpc::Client>,
    provider_type: ProviderType,
    num: u64,
) -> Result<(), Box<dyn Error>> {
    let block_result = client
        .query(&format!(
            "SELECT hex(hash) FROM blocks WHERE height = {}",
            num
        ))
        .fetch_one::<BlockHashRow>()
        .await;
    if block_result.is_err() {
        warn!("add missing block: {}, {:?}", num, block_result);
        insert_block(&client, provider, trace_provider, provider_type, num)
            .await
            .unwrap();
    } else {
        let block = block_result.unwrap();
        let block_hash_on_store = block.hash;
        let block_hash_on_chain = utils::bytes_to_btc_hex(provider.get_block_hash(num).unwrap().as_byte_array());

        if block_hash_on_store != block_hash_on_chain {
            warn!(
                "fix err block {}: {:?} != {:?}",
                num,
                block_hash_on_store,
                block_hash_on_chain,
            );
            tokio::try_join!(
                client
                    .query(format!("DELETE FROM blocks WHERE height = {}", num).as_str())
                    .execute(),
                client
                    .query(format!("DELETE FROM inputs WHERE blockHeight = {}", num).as_str())
                    .execute(),
                client
                    .query(format!("DELETE FROM outputs WHERE blockHeight = {}", num).as_str())
                    .execute(),
            )
            .ok();

            insert_block(&client, provider, trace_provider, provider_type, num)
                .await
                .unwrap();
        }
        // no need to check trace
    }
    Ok(())
}
