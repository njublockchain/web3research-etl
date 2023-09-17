use std::error::Error;

use bitcoin::{hashes::Hash, Address};
use bitcoincore_rpc::{Client, RpcApi};
use log::{debug, info, warn};
use url::Url;

use crate::{clickhouse_scheme::bitcoin::{BlockRow, InputRow, OutputRow}, ProviderType};

pub(crate) async fn init(
    db: String,
    provider: String,
    provider_type: ProviderType,
    from: u64,
    batch: u64,
) -> Result<(), Box<dyn Error>> {
    let clickhouse_url = Url::parse(&db).unwrap();
    // warn!("db: {} path: {}", format!("{}:{}", clickhouse_url.host().unwrap(), clickhouse_url.port().unwrap()), clickhouse_url.path());

    let options = if clickhouse_url.path() != "/default"
        || !clickhouse_url.username().is_empty()
    {
        warn!("auth enabled for clickhouse");
        klickhouse::ClientOptions {
            username: clickhouse_url.username().to_string(),
            password: clickhouse_url.password().unwrap_or("").to_string(),
            default_database: clickhouse_url.path().to_string(),
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

    let bitcoin_rpc_url = Url::parse(&provider).unwrap();

    let client = bitcoincore_rpc::Client::new(
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
            -- bitcoin.blocks definition

            CREATE TABLE IF NOT EXISTS bitcoin.blocks
            (
            
                `height` UInt64,
            
                `hash` FixedString(32),
            
                `size` UInt32,
            
                `strippedSize` UInt32,
            
                `weight` UInt64,
            
                `prevBlockHash` FixedString(32),
            
                `version` Int32,
            
                `merkleRoot` FixedString(32),
            
                `time` UInt32,
            
                `bits` UInt32,
            
                `nonce` UInt32,
            
                `difficulty` UInt128
            )
            ENGINE = ReplacingMergeTree
            ORDER BY height
            SETTINGS index_granularity = 8192;
        ",
        )
        .await
        .unwrap();

    klient
        .execute(
            "
            -- bitcoin.inputs definition

            CREATE TABLE IF NOT EXISTS bitcoin.inputs
            (
            
                `txid` FixedString(32),
            
                `size` UInt32,
            
                `vsize` UInt32,
            
                `weight` UInt64,
            
                `version` Int32,
            
                `lockTime` UInt32,
            
                `blockHash` FixedString(32),
            
                `blockHeight` UInt64,
            
                `blockTime` UInt32,
            
                `index` UInt32,
            
                `prevOutputTxid` FixedString(32),
            
                `prevOutputVout` UInt32,
            
                `scriptSig` String,

                `sequence` UInt32,
            
                `witness` Array(String)
            )
            ENGINE = ReplacingMergeTree
            ORDER BY (txid,
             index)
            SETTINGS index_granularity = 8192;
        ",
        )
        .await
        .unwrap();

    klient
        .execute(
            "
            -- bitcoin.outputs definition

            CREATE TABLE IF NOT EXISTS bitcoin.outputs
            (
            
                `txid` FixedString(32),
            
                `size` UInt32,
            
                `vsize` UInt32,
            
                `weight` UInt64,
            
                `version` Int32,
            
                `lockTime` UInt32,
            
                `blockHash` FixedString(32),
            
                `blockHeight` UInt64,
            
                `blockTime` UInt32,
            
                `index` UInt32,
            
                `value` UInt64,
            
                `scriptPubkey` String,
            
                `address` Nullable(String)
            )
            ENGINE = ReplacingMergeTree
            ORDER BY (txid,
             index)
            SETTINGS index_granularity = 8192;
        ",
        )
        .await
        .unwrap();

    let latest_height = client.get_block_count()? - 1;
    let to = latest_height / 1000 * 1000;
    warn!("target: {}", to);

    let mut block_row_list = Vec::with_capacity((batch + 1_u64) as usize);
    let mut input_row_list = Vec::new();
    let mut output_row_list = Vec::new();

    for num in from..=to {
        let hash = client.get_block_hash(num)?;
        // let cli = client.get_jsonrpc_client();
        let block = client.get_block(&hash)?;
        let block_hash = block.block_hash();

        let block_row = BlockRow {
            height: num,
            hash: block_hash.as_byte_array().to_vec().into(),
            size: block.size() as u32,
            stripped_size: block.strippedsize() as u32,
            weight: block.weight().to_wu(),
            prev_block_hash: block.header.prev_blockhash.as_byte_array().to_vec().into(),
            version: block.header.version.to_consensus(),
            merkle_root: block.header.merkle_root.to_byte_array().to_vec().into(),
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
                    block_height: num,
                    block_time: block.header.time,
                    index: index as u32,
                    prev_output_txid: vin.previous_output.txid.as_byte_array().to_vec().into(),
                    prev_output_vout: vin.previous_output.vout,
                    script_sig: vin.script_sig.to_bytes().to_vec().into(),
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
                    .ok().map(|s| s.to_string());
                let output_row = OutputRow {
                    txid: tx.txid().as_byte_array().to_vec().into(),
                    size: tx.size() as u32,
                    vsize: tx.vsize() as u32,
                    weight: tx.weight().to_wu(),
                    version: tx.version,
                    lock_time: tx.lock_time.to_consensus_u32(),
                    block_hash: block_hash.as_byte_array().to_vec().into(),
                    block_height: num,
                    block_time: block.header.time,
                    index: index as u32,
                    value: vout.value,
                    script_pubkey: vout.script_pubkey.to_bytes().to_vec().into(),
                    address,
                };

                output_row_list.push(output_row);
            }
        }

        if (num - from + 1) % batch == 0 {
            tokio::try_join!(
                klient.insert_native_block(
                    "INSERT INTO bitcoin.blocks FORMAT native",
                    block_row_list.to_vec()
                ),
                klient.insert_native_block(
                    "INSERT INTO bitcoin.inputs FORMAT native",
                    input_row_list.to_vec()
                ),
                klient.insert_native_block(
                    "INSERT INTO bitcoin.outputs FORMAT native",
                    output_row_list.to_vec()
                ),
            )
            .unwrap();

            block_row_list.clear();
            input_row_list.clear();
            output_row_list.clear();

            info!("{} done blocks & txs", num)
        }
    }

    tokio::try_join!(
        klient.insert_native_block(
            "INSERT INTO bitcoin.blocks FORMAT native",
            block_row_list.to_vec()
        ),
        klient.insert_native_block(
            "INSERT INTO bitcoin.inputs FORMAT native",
            input_row_list.to_vec()
        ),
        klient.insert_native_block(
            "INSERT INTO bitcoin.outputs FORMAT native",
            output_row_list.to_vec()
        ),
    )
    .unwrap();

    Ok(())
}
