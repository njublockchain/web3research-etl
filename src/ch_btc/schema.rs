use bitcoin::Address;
use documented::Documented;
use klickhouse::Row;

/**
CREATE TABLE IF NOT EXISTS blocks (
    `height` UInt64,
    `hash` FixedString(64),
    `totalSize` UInt32,
    `weight` UInt64,
    `prevBlockHash` FixedString(64),
    `version` Int32,
    `time` UInt32,
    `bits` UInt32,
    `nonce` UInt32,
    `difficulty` Float64 CODEC(Gorilla)
)
ENGINE = ReplacingMergeTree
ORDER BY height
SETTINGS index_granularity = 8192;
*/
#[derive(Row, Clone, Debug, Default, Documented)]
#[klickhouse(rename_all = "camelCase")]
pub struct BlockRow {
    pub height: u64,
    pub hash: String,
    pub total_size: u32,
    pub weight: u64,
    pub prev_block_hash: String,

    /// Block version, now repurposed for soft fork signalling.
    pub version: i32,
    // /// Reference to the previous block in the chain.
    // /// The root hash of the merkle tree of transactions in the block.
    // pub merkle_root: String,
    /// The timestamp of the block, as claimed by the miner.
    pub time: u32,
    /// The target value below which the blockhash must lie.
    pub bits: u32,
    /// The nonce, selected to obtain a low enough blockhash.
    pub nonce: u32,

    /// Computes the popular "difficulty" measure for mining.
    pub difficulty: f64,
}

impl BlockRow {
    pub fn from_bitcoin_rpc(height: u64, block: &bitcoin::blockdata::block::Block) -> Self {
        Self {
            height: height,
            hash: block.block_hash().to_string(),
            total_size: block.total_size().try_into().unwrap(),
            weight: block.weight().to_wu(),
            prev_block_hash: block.header.prev_blockhash.to_string(),
            version: block.header.version.to_consensus(),
            // merkle_root: block.header.merkle_root.to_string(),
            time: block.header.time,
            bits: block.header.bits.to_consensus(),
            nonce: block.header.nonce,
            difficulty: block.header.difficulty_float(),
        }
    }
}

/**
CREATE TABLE IF NOT EXISTS inputs (
    `txid` FixedString(64),
    `txIndex` UInt32,
    `totalSize` UInt32,
    `baseSize` UInt32,
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
    `scriptSig` String CODEC(ZSTD(6)),
    `address` Nullable(String) CODEC(ZSTD(6)),
    `sequence` UInt32,
    `witness` Array(String) CODEC(ZSTD(6))
)
ENGINE = ReplacingMergeTree
ORDER BY (txid, index)
SETTINGS index_granularity = 8192;
*/
#[derive(Row, Clone, Debug, Default, Documented)]
#[klickhouse(rename_all = "camelCase")]
pub struct InputRow {
    pub txid: String,
    pub tx_index: u32,
    pub total_size: u32,
    pub base_size: u32,
    pub vsize: u32,
    pub weight: u64,
    pub version: i32,
    pub lock_time: u32,
    pub block_hash: String,
    pub block_height: u64,
    pub block_time: u32,

    pub index: u32,

    /// The reference to the previous output that is being used an an input.
    /// The referenced transaction's txid.
    pub prev_output_txid: String,
    /// The index of the referenced output in its transaction's vout.
    pub prev_output_vout: u32,

    /// The script which pushes values on the stack which will cause
    /// the referenced output's script to be accepted.
    pub script_sig: String, // can be used to infer the public key
    pub address: Option<String>,

    /// The sequence number, which suggests to miners which of two
    /// conflicting transactions should be preferred, or 0xFFFFFFFF
    /// to ignore this feature. This is generally never used since
    /// the miner behaviour cannot be enforced.
    pub sequence: u32,

    /// Witness data: an array of byte-arrays.
    /// Note that this field is not (de)serialized with the rest of
    /// the TxIn in Encodable/Decodable, as it is (de)serialized at
    /// the end of the full Transaction. It is (de)serialized with
    /// the rest of the TxIn in other (de)serialization routines.
    pub witness: Vec<String>,
}

impl InputRow {
    pub fn from_bitcoin_rpc(
        height: u64,
        block: &bitcoin::blockdata::block::Block,
        tx: &bitcoin::blockdata::transaction::Transaction,
        tx_index: u32,
        index: u32,
        vin: &bitcoin::blockdata::transaction::TxIn,
        address: Option<String>,
    ) -> Self {
        Self {
            txid: tx.compute_txid().to_string(),
            tx_index: tx_index,
            total_size: tx.total_size() as u32,
            base_size: tx.base_size() as u32,
            vsize: tx.vsize() as u32,
            weight: tx.weight().to_wu(),
            version: tx.version.0,
            lock_time: tx.lock_time.to_consensus_u32(),
            block_hash: block.block_hash().to_string(),
            block_height: height,
            block_time: block.header.time,
            index: index as u32,
            prev_output_txid: vin.previous_output.txid.to_string(),
            prev_output_vout: vin.previous_output.vout,
            script_sig: vin.script_sig.to_hex_string(),
            address,
            sequence: vin.sequence.0,
            witness: vin.witness.iter().map(|w| hex::encode(w)).collect(),
        }
    }
}

/**
CREATE TABLE IF NOT EXISTS outputs (
    `txid` FixedString(64),
    `txIndex` UInt32,
    `totalSize` UInt32,
    `baseSize` UInt32,
    `vsize` UInt32,
    `weight` UInt64,
    `version` Int32,
    `lockTime` UInt32,
    `blockHash` String,
    `blockHeight` UInt64,
    `blockTime` UInt32,
    `index` UInt32,
    `value` UInt64,
    `scriptPubkey` String CODEC(ZSTD(6)),
    `address` Nullable(String) CODEC(ZSTD(6))
)
ENGINE = ReplacingMergeTree
ORDER BY (txid, index)
SETTINGS index_granularity = 8192;
*/
#[derive(Row, Clone, Debug, Default, Documented)]
#[klickhouse(rename_all = "camelCase")]
pub struct OutputRow {
    pub txid: String,
    pub tx_index: u32,
    pub total_size: u32,
    pub base_size: u32,
    pub vsize: u32,
    pub weight: u64,
    pub version: i32,
    pub lock_time: u32,
    pub block_hash: String,
    pub block_height: u64,
    pub block_time: u32,

    pub index: u32,

    /// The value of the output, in satoshis.
    pub value: u64,
    /// The script which must be satisfied for the output to be spent.
    pub script_pubkey: String,
    pub address: Option<String>,
}

impl OutputRow {
    pub fn from_bitcoin_rpc(
        height: u64,
        block: &bitcoin::blockdata::block::Block,
        tx: &bitcoin::blockdata::transaction::Transaction,
        tx_index: u32,
        index: u32,
        vout: &bitcoin::blockdata::transaction::TxOut,
    ) -> Self {
        Self {
            txid: tx.compute_txid().to_string(),
            tx_index: tx_index,
            total_size: tx.total_size() as u32,
            base_size: tx.base_size() as u32,
            vsize: tx.vsize() as u32,
            weight: tx.weight().to_wu(),
            version: tx.version.0,
            lock_time: tx.lock_time.to_consensus_u32(),
            block_hash: block.block_hash().to_string(),
            block_height: height,
            block_time: block.header.time,
            index: index as u32,
            value: vout.value.to_sat(),
            script_pubkey: vout.script_pubkey.to_hex_string(),
            // Attempt to derive an address from the script_pubkey.
            address: Address::from_script(&vout.script_pubkey, bitcoin::Network::Bitcoin)
                .ok()
                .map(|s| s.to_string()),
        }
    }
}
