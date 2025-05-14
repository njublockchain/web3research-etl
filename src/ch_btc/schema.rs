use bitcoin::hashes::Hash;
use bitcoin::Address;
use clickhouse::Row;
use documented::Documented;
use serde::{Deserialize, Serialize};

use super::utils;

/**
CREATE TABLE IF NOT EXISTS blocks (
    `height` UInt64,
    `hash` FixedString(64),
    `size` UInt32,
    `strippedSize` UInt32,
    `weight` UInt64,
    `prevBlockHash` FixedString(64),
    `version` Int32,
    `merkleRoot` FixedString(64),
    `time` UInt32,
    `bits` UInt32,
    `nonce` UInt32,
    `difficulty` UInt128
)
ENGINE = ReplacingMergeTree
ORDER BY height
SETTINGS index_granularity = 8192;
*/
#[derive(Clone, Debug, Default, Documented, Row, Serialize, Deserialize)]
pub struct BlockRow {
    #[serde(rename = "height")]
    pub height: u64,
    pub hash: String,
    // pub base_size: u32,
    pub size: u32,
    #[serde(rename = "strippedSize")]
    pub stripped_size: u32,
    pub weight: u64,
    #[serde(rename = "prevBlockHash")]
    pub prev_block_hash: String,

    /// Block version, now repurposed for soft fork signalling.
    pub version: i32,
    /// Reference to the previous block in the chain.
    /// The root hash of the merkle tree of transactions in the block.
    #[serde(rename = "merkleRoot")]
    pub merkle_root: String,
    /// The timestamp of the block, as claimed by the miner.
    pub time: u32,
    /// The target value below which the blockhash must lie.
    pub bits: u32,
    /// The nonce, selected to obtain a low enough blockhash.
    pub nonce: u32,

    /// Computes the popular "difficulty" measure for mining.
    pub difficulty: u128,
}

impl BlockRow {
    pub fn from_bitcoin_rpc(height: u64, block: &bitcoin::blockdata::block::Block) -> Self {
        Self {
            height: height,
            hash: utils::bytes_to_btc_hex(block.block_hash().as_byte_array()),
            size: block.size() as u32,
            stripped_size: block.strippedsize() as u32,
            weight: block.weight().to_wu(),
            prev_block_hash: utils::bytes_to_btc_hex(block.header.prev_blockhash.as_byte_array()),
            version: block.header.version.to_consensus(),
            merkle_root: utils::bytes_to_btc_hex(block.header.merkle_root.as_byte_array()),
            time: block.header.time,
            bits: block.header.bits.to_consensus(),
            nonce: block.header.nonce,
            difficulty: block.header.difficulty(),
        }
    }
}

/**
CREATE TABLE IF NOT EXISTS inputs (
    `txid` FixedString(64),
    `size` UInt32,
    `vsize` UInt32,
    `weight` UInt64,
    `version` Int32,
    `lockTime` UInt32,
    `blockHash` FixedString(64),
    `blockHeight` UInt64,
    `blockTime` UInt32,
    `index` UInt32,
    `prevOutputTxid` FixedString(64),
    `prevOutputVout` UInt32,
    `scriptSig` String,
    `sequence` UInt32,
    `witness` Array(String)
)
ENGINE = ReplacingMergeTree
ORDER BY (txid, index)
SETTINGS index_granularity = 8192;
*/
#[derive(Clone, Debug, Default, Documented, Row, Serialize, Deserialize)]
pub struct InputRow {
    pub txid: String,
    pub size: u32,
    pub vsize: u32,
    pub weight: u64,
    pub version: i32,
    #[serde(rename = "lockTime")]
    pub lock_time: u32,
    #[serde(rename = "blockHash")]
    pub block_hash: String,
    #[serde(rename = "blockHeight")]
    pub block_height: u64,
    #[serde(rename = "blockTime")]
    pub block_time: u32,

    pub index: u32,

    /// The reference to the previous output that is being used an an input.
    /// The referenced transaction's txid.
    #[serde(rename = "prevOutputTxid")]
    pub prev_output_txid: String,
    /// The index of the referenced output in its transaction's vout.
    #[serde(rename = "prevOutputVout")]
    pub prev_output_vout: u32,

    /// The script which pushes values on the stack which will cause
    /// the referenced output's script to be accepted.
    #[serde(rename = "scriptSig")]
    pub script_sig: String,

    /// The sequence number, which suggests to miners which of two
    /// conflicting transactions should be preferred, or 0xFFFFFFFF
    /// to ignore this feature. This is generally never used since
    /// the miner behaviour cannot be enforced.
    pub sequence: u32,

    pub witness: Vec<String>,
}

impl InputRow {
    pub fn from_bitcoin_rpc(
        height: u64,
        block: &bitcoin::blockdata::block::Block,
        tx: &bitcoin::blockdata::transaction::Transaction,
        index: u32,
        vin: &bitcoin::blockdata::transaction::TxIn,
    ) -> Self {
        Self {
            txid: utils::bytes_to_btc_hex(tx.txid().as_byte_array()),
            size: tx.size() as u32,
            vsize: tx.vsize() as u32,
            weight: tx.weight().to_wu(),
            version: tx.version,
            lock_time: tx.lock_time.to_consensus_u32(),
            block_hash: utils::bytes_to_btc_hex(block.block_hash().as_byte_array()),
            block_height: height,
            block_time: block.header.time,
            index: index as u32,
            prev_output_txid: utils::bytes_to_btc_hex(vin.previous_output.txid.as_byte_array()),
            prev_output_vout: vin.previous_output.vout,
            script_sig: utils::bytes_to_btc_hex(&vin.script_sig.to_bytes()),
            sequence: vin.sequence.0,
            witness: vin
                .witness
                .to_vec()
                .iter()
                .map(|w| utils::bytes_to_btc_hex(w))
                .collect(),
        }
    }
}

/**
CREATE TABLE IF NOT EXISTS outputs (
    `txid` FixedString(64),
    `size` UInt32,
    `vsize` UInt32,
    `weight` UInt64,
    `version` Int32,
    `lockTime` UInt32,
    `blockHash` String,
    `blockHeight` UInt64,
    `blockTime` UInt32,
    `index` UInt32,
    `value` UInt64,
    `scriptPubkey` String,
    `address` Nullable(String)
)
ENGINE = ReplacingMergeTree
ORDER BY (txid, index)
SETTINGS index_granularity = 8192;
*/
#[derive(Clone, Debug, Default, Documented, Row, Serialize, Deserialize)]
pub struct OutputRow {
    pub txid: String,
    pub size: u32,
    pub vsize: u32,
    pub weight: u64,
    pub version: i32,
    #[serde(rename = "lockTime")]
    pub lock_time: u32,
    #[serde(rename = "blockHash")]
    pub block_hash: String,
    #[serde(rename = "blockHeight")]
    pub block_height: u64,
    #[serde(rename = "blockTime")]
    pub block_time: u32,

    pub index: u32,

    /// The value of the output, in satoshis.
    pub value: u64,
    /// The script which must be satisfied for the output to be spent.
    #[serde(rename = "scriptPubkey")]
    pub script_pubkey: String,
    pub address: Option<String>,
}

impl OutputRow {
    pub fn from_bitcoin_rpc(
        height: u64,
        block: &bitcoin::blockdata::block::Block,
        tx: &bitcoin::blockdata::transaction::Transaction,
        index: u32,
        vout: &bitcoin::blockdata::transaction::TxOut,
    ) -> Self {
        Self {
            txid: utils::bytes_to_btc_hex(tx.txid().as_byte_array()),
            size: tx.size() as u32,
            vsize: tx.vsize() as u32,
            weight: tx.weight().to_wu(),
            version: tx.version,
            lock_time: tx.lock_time.to_consensus_u32(),
            block_hash: utils::bytes_to_btc_hex(block.block_hash().as_byte_array()),
            block_height: height,
            block_time: block.header.time,
            index: index as u32,
            value: vout.value,
            script_pubkey: utils::bytes_to_btc_hex(&vout.script_pubkey.to_bytes()),
            address: Address::from_script(&vout.script_pubkey, bitcoin::Network::Bitcoin)
                .ok()
                .map(|s| s.to_string()),
        }
    }
}
