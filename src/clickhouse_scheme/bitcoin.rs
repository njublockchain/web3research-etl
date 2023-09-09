// use bitcoincore_rpc::json::Address;
use klickhouse::{Bytes, Row};

#[derive(Row, Clone, Debug, Default)]
#[klickhouse(rename_all = "camelCase")]
pub struct BlockRow {
    pub height: u64,
    pub hash: Bytes,
    // pub base_size: u32,
    pub size: u32,
    pub stripped_size: u32,
    pub weight: u64,
    pub prev_block_hash: Bytes,

    /// Block version, now repurposed for soft fork signalling.
    pub version: i32,
    /// Reference to the previous block in the chain.
    /// The root hash of the merkle tree of transactions in the block.
    pub merkle_root: Bytes,
    /// The timestamp of the block, as claimed by the miner.
    pub time: u32,
    /// The target value below which the blockhash must lie.
    pub bits: u32,
    /// The nonce, selected to obtain a low enough blockhash.
    pub nonce: u32,

    /// Computes the popular "difficulty" measure for mining.
    pub difficulty: u128,
}

// pub struct TxBase {
//     pub txid: Bytes,
//     pub height: u64,
//     pub hash: Bytes,
//     pub size: u32,
//     pub vsize: u32,
//     pub weight: u32,
//     pub version: u32,
//     pub locktime: u32,
//     pub block_hash: Bytes,
//     pub block_height: u64,
//     pub block_time: u64,
//     pub time: u64,
// }

#[derive(Row, Clone, Debug, Default)]
#[klickhouse(rename_all = "camelCase")]
pub struct InputRow {
    pub txid: Bytes,
    pub size: u32,
    pub vsize: u32,
    pub weight: u64,
    pub version: i32,
    pub lock_time: u32,
    pub block_hash: Bytes,
    pub block_height: u64,
    pub block_time: u32,

    pub index: u32,

    /// The reference to the previous output that is being used an an input.
    /// The referenced transaction's txid.
    pub prev_output_txid: Bytes,
    /// The index of the referenced output in its transaction's vout.
    pub prev_output_vout: u32,

    /// The script which pushes values on the stack which will cause
    /// the referenced output's script to be accepted.
    pub script_sig: Bytes,

    /// The sequence number, which suggests to miners which of two
    /// conflicting transactions should be preferred, or 0xFFFFFFFF
    /// to ignore this feature. This is generally never used since
    /// the miner behaviour cannot be enforced.
    pub sequence: u32,

    pub witness: Vec<Bytes>
}

#[derive(Row, Clone, Debug, Default)]
#[klickhouse(rename_all = "camelCase")]
pub struct OutputRow {
    pub txid: Bytes,
    pub size: u32,
    pub vsize: u32,
    pub weight: u64,
    pub version: i32,
    pub lock_time: u32,
    pub block_hash: Bytes,
    pub block_height: u64,
    pub block_time: u32,

    pub index: u32,

    /// The value of the output, in satoshis.
    pub value: u64,
    /// The script which must be satisfied for the output to be spent.
    pub script_pubkey: Bytes,
    pub address: Option<String>,
}
