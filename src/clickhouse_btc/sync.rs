use std::error::Error;

use bitcoin::{hashes::Hash, Address};
use bitcoincore_rpc::RpcApi;
use klickhouse::{Client, Row};
use log::{info, warn};

use crate::{
    clickhouse_scheme::bitcoin::{BlockRow, InputRow, OutputRow},
    ProviderType,
};

async fn insert_block(
    client: &Client,
    provider: &bitcoincore_rpc::Client,
    trace_provider: &Option<&bitcoincore_rpc::Client>,
    provider_type: ProviderType,
    block_number: u64,
) -> Result<(), Box<dyn Error>> {
    let mut block_row_list = Vec::with_capacity((1_u64) as usize);
    let mut input_row_list = Vec::new();
    let mut output_row_list = Vec::new();

    let hash = provider.get_block_hash(block_number)?;
    // let cli = client.get_jsonrpc_client();
    let block = provider.get_block(&hash)?;
    let block_hash = block.block_hash();

    let block_row = BlockRow {
        height: block_number,
        hash: block_hash.as_byte_array().to_vec().into(),
        size: block.size() as u32,
        stripped_size: block.strippedsize() as u32,
        weight: block.weight().to_wu(),
        prev_block_hash: block.header.prev_blockhash.as_byte_array().to_vec().into(),
        version: block.header.version.to_consensus(),
        merkle_root: block.header.merkle_root.as_byte_array().to_vec().into(),
        time: block.header.time,
        bits: block.header.bits.to_consensus(),
        nonce: block.header.nonce,
        difficulty: block.header.difficulty(),
    };

    block_row_list.push(block_row);

    for tx in block.txdata {
        for (index, vin) in tx.input.iter().enumerate() {
            let input_row = InputRow {
                txid: tx.txid().as_byte_array().to_vec().into(),
                size: tx.size() as u32,
                vsize: tx.vsize() as u32,
                weight: tx.weight().to_wu(),
                version: tx.version,
                lock_time: tx.lock_time.to_consensus_u32(),
                block_hash: block_hash.as_byte_array().to_vec().into(),
                block_height: block_number,
                block_time: block.header.time,
                index: index as u32,
                prev_output_txid: vin.previous_output.txid.as_byte_array().to_vec().into(),
                prev_output_vout: vin.previous_output.vout,
                script_sig: vin.script_sig.as_bytes().to_vec().into(),
                sequence: vin.sequence.0,
                witness: vin
                    .witness
                    .to_vec()
                    .iter()
                    .map(|w| w.clone().into())
                    .collect(),
            };

            input_row_list.push(input_row);
        }

        for (index, vout) in tx.output.iter().enumerate() {
            let address = Address::from_script(&vout.script_pubkey, bitcoin::Network::Bitcoin)
                .ok()
                .map(|s| s.to_string());
            let output_row = OutputRow {
                txid: tx.txid().as_byte_array().to_vec().into(),
                size: tx.size() as u32,
                vsize: tx.vsize() as u32,
                weight: tx.weight().to_wu(),
                version: tx.version,
                lock_time: tx.lock_time.to_consensus_u32(),
                block_hash: block_hash.as_byte_array().to_vec().into(),
                block_height: block_number,
                block_time: block.header.time,
                index: index as u32,
                value: vout.value,
                script_pubkey: vout.script_pubkey.as_bytes().to_vec().into(),
                address,
            };

            output_row_list.push(output_row);
        }
    }

    tokio::try_join!(
        client.insert_native_block(
            "INSERT INTO blocks FORMAT native",
            block_row_list.to_vec()
        ),
        client.insert_native_block(
            "INSERT INTO inputs FORMAT native",
            input_row_list.to_vec()
        ),
        client.insert_native_block(
            "INSERT INTO outputs FORMAT native",
            output_row_list.to_vec()
        ),
    )
    .unwrap();

    block_row_list.clear();
    input_row_list.clear();
    output_row_list.clear();

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
            "SELECT hex(hash) FROM blocks WHERE height = {}",
            num
        ))
        .await;
    if block.is_err() {
        warn!("add missing block: {}, {:?}", num, block);
        insert_block(&client, provider, trace_provider, provider_type, num)
            .await
            .unwrap();
    } else {
        let block_hash_on_store = hex::decode(block.unwrap().hash).unwrap().as_slice()[0..32]
            .try_into()
            .unwrap();
        let block_hash_on_chain = provider.get_block_hash(num).unwrap().to_byte_array();

        if block_hash_on_store != block_hash_on_chain {
            warn!(
                "fix err block {}: {:?} != {:?}",
                num,
                format!(
                    "{:#032x}",
                    bitcoin::BlockHash::from_byte_array(block_hash_on_store)
                ),
                format!(
                    "{:#032x}",
                    bitcoin::BlockHash::from_byte_array(block_hash_on_chain)
                )
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
