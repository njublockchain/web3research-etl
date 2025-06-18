use bitcoin::{params::MAINNET, Address, ScriptBuf};
use documented::Documented;
use klickhouse::Row;
use log::{info, warn};

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
            value: vout.value.to_sat(),
            script_pubkey: vout.script_pubkey.to_hex_string(),
            // Attempt to derive an address from the script_pubkey.
            address: address.clone(),
        }
    }
}

pub fn get_address(script_pubkey: &ScriptBuf) -> Option<String> {
    let address = if script_pubkey.is_empty() {
        Some("Blackhole".to_string())
    } else if script_pubkey.is_multisig() {
        let instructions = script_pubkey.instructions();
        let mut multisig_required_signatures = 0;
        let mut multisig_total_signatures = 0;
        let mut pubkey_count = 0;

        for instruction in instructions {
            if let Ok(instruction) = instruction {
                match instruction {
                    bitcoin::blockdata::script::Instruction::Op(op) => {
                        if op.to_u8() >= bitcoin::opcodes::all::OP_PUSHNUM_1.to_u8()
                            && op.to_u8() <= bitcoin::opcodes::all::OP_PUSHNUM_16.to_u8()
                        {
                            let num = op.to_u8() - bitcoin::opcodes::all::OP_PUSHNUM_1.to_u8() + 1;
                            if multisig_required_signatures == 0 {
                                multisig_required_signatures = num;
                            } else {
                                multisig_total_signatures = num;
                            }
                        } else if op == bitcoin::opcodes::all::OP_CHECKMULTISIG {
                            if multisig_total_signatures == 0 {
                                multisig_total_signatures = pubkey_count;
                            }
                            break;
                        }
                    }
                    bitcoin::blockdata::script::Instruction::PushBytes(bytes) => {
                        if bytes.len() == 33 || bytes.len() == 65 {
                            pubkey_count += 1;
                        }
                    }
                }
            }
        }

        Some(format!(
            "MultiSig:({}/{})",
            multisig_required_signatures, multisig_total_signatures
        ))
    } else if script_pubkey.is_op_return() {
        Some("OpReturn".to_string())
    } else if script_pubkey.is_p2pk() {
        let pubkey = script_pubkey
            .p2pk_public_key()
            .map(|pk| pk.to_string())
            .unwrap_or_else(|| "InvalidPublicKey".to_string());

        Some(pubkey)
    } else if script_pubkey.is_p2pkh()
        || script_pubkey.is_p2wpkh()
        || script_pubkey.is_p2sh()
        || script_pubkey.is_p2wpkh()
        || script_pubkey.is_p2tr()
    {
        Some(
            Address::from_script(&script_pubkey, &MAINNET)
                .unwrap()
                .to_string(),
        )
    } else if script_pubkey.is_push_only() {
        Some("PushOnly".to_string())
    } else if script_pubkey.is_witness_program() {
        warn!(
            "Found a non-address witness program: {}",
            script_pubkey.to_asm_string()
        );
        Some("WitnessProgram".to_string())
    } else {
        let instructions = script_pubkey.instructions();

        let mut has_checkmultisig = false;
        let mut has_checksig = false;
        let mut has_hash_ops = false;
        let mut has_equalverify = false;
        let mut has_checklocktime = false;

        for instruction in instructions {
            if let Ok(instruction) = instruction {
                match instruction {
                    bitcoin::script::Instruction::Op(op) => {
                        if op == bitcoin::opcodes::all::OP_CHECKSIG {
                            has_checksig = true;
                            break;
                        } else if op == bitcoin::opcodes::all::OP_CHECKMULTISIG {
                            has_checkmultisig = true;
                            break;
                        } else if op == bitcoin::opcodes::all::OP_HASH160
                            || op == bitcoin::opcodes::all::OP_HASH256
                            || op == bitcoin::opcodes::all::OP_RIPEMD160
                            || op == bitcoin::opcodes::all::OP_SHA1
                            || op == bitcoin::opcodes::all::OP_SHA256
                        {
                            has_hash_ops = true;
                        } else if op == bitcoin::opcodes::all::OP_EQUALVERIFY
                            || op == bitcoin::opcodes::all::OP_EQUAL
                        {
                            has_equalverify = true;
                        } else if op == bitcoin::opcodes::all::OP_CLTV {
                            has_checklocktime = true;
                        }
                    }
                    bitcoin::script::Instruction::PushBytes(_push_bytes) => {}
                }
            }
        }

        if has_checkmultisig {
            Some("NonstandardMultiSig".to_string())
        } else if has_checksig {
            Some("NonstandardSig".to_string())
        } else if has_hash_ops && has_equalverify {
            Some("HashLock".to_string())
        } else if has_checklocktime {
            Some("TimeLock".to_string())
        } else {
            info!(
                "Cannot decode script pubkey: {}",
                script_pubkey.to_asm_string()
            );
            None
        }
    };
    address
}
