use async_graphql::*;
use klickhouse::{u256, Bytes};

use crate::clickhouse_scheme::ethereum::BlockRow;

// #[derive(Clone, Debug)]
// pub struct CustomBytes(Bytes);

// #[Scalar]
// impl ScalarType for CustomBytes {
//     fn parse(value: Value) -> InputValueResult<Self> {
//         // Implement parsing logic here
//     }

//     fn to_value(&self) -> Value {
//         // Implement serialization logic here
//     }
// }

// #[derive(SimpleObject)]
// pub struct Block {
//     pub hash: Bytes,
//     pub number: u64,
//     pub parent_hash: Bytes,
//     pub uncles: Vec<Bytes>,
//     pub sha3_uncles: Bytes,
//     pub total_difficulty: u256,
//     pub difficulty: u256,
//     pub miner: Bytes,
//     pub nonce: Bytes,
//     pub mix_hash: Bytes,
//     pub base_fee_per_gas: Option<u256>,
//     pub gas_limit: u256,
//     pub gas_used: u256,
//     pub state_root: Bytes,
//     pub transactions_root: Bytes,
//     pub receipts_root: Bytes,
//     pub logs_bloom: Bytes,
//     pub withdrawls_root: Option<Bytes>,
//     pub extra_data: Bytes,
//     pub timestamp: u256,
//     pub size: u256,
// }

// impl From<BlockRow> for Block {
//     fn from(block_row: BlockRow) -> Self {
//         Block {
//             hash: block_row.hash,
//             number: block_row.number,
//             parent_hash: block_row.parentHash,
//             uncles: block_row.uncles,
//             sha3_uncles: block_row.sha3Uncles,
//             total_difficulty: block_row.totalDifficulty,
//             difficulty: block_row.difficulty,
//             miner: block_row.miner,
//             nonce: block_row.nonce,
//             mix_hash: block_row.mixHash,
//             base_fee_per_gas: block_row.baseFeePerGas,
//             gas_limit: block_row.gasLimit,
//             gas_used: block_row.gasUsed,
//             state_root: block_row.stateRoot,
//             transactions_root: block_row.transactionsRoot,
//             receipts_root: block_row.receiptsRoot,
//             logs_bloom: block_row.logsBloom,
//             withdrawls_root: block_row.withdrawlsRoot,
//             extra_data: block_row.extraData,
//             timestamp: block_row.timestamp,
//             size: block_row.size,
//         }
//     }
// }

// // impl From<BlockRow> for GraphBlock {
// //     fn from(block: BlockRow) -> Self {
// //         Self {
// //             hash: block.hash.map(|h| h.to_string()),
// //             parent_hash: block.parent_hash.to_string(),
// //             uncles_hash: block.uncles_hash.to_string(),
// //             // Other fields here...
// //         }
// //     }
// // }
