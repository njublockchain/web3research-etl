use std::error::Error;

use log::{debug, info, warn};
use serde::Deserialize;
use solana_client::{rpc_client::RpcClient, rpc_config::RpcBlockConfig};
use solana_sdk::{
    commitment_config::CommitmentConfig, message::Message, system_instruction::SystemInstruction,
};
use solana_transaction_status::{
    option_serializer::OptionSerializer, EncodedTransaction, TransactionDetails, UiInstruction, UiMessage, UiParsedInstruction, UiTransactionEncoding
};
use url::Url;

use crate::ProviderType;

pub(crate) async fn init(
    db: String,
    provider: String,
    provider_type: ProviderType,
    from: u64,
    batch: u64,
) -> Result<(), Box<dyn Error>> {
    let client: RpcClient = RpcClient::new_with_commitment(provider, CommitmentConfig::confirmed());
    client.get_health().unwrap(); // require health
    let cluster_nodes = client.get_cluster_nodes().unwrap();
    for node in cluster_nodes {
        if node.rpc.is_some() {
            info!("Cluster Node: {:?}", node);
        }
    }

    let current_slot = client.get_slot().unwrap();
    info!("Current Slot: {:?}", current_slot);

    for slot in 0..=current_slot {
        match client.get_block_with_config(
            slot,
            RpcBlockConfig {
                encoding: Some(UiTransactionEncoding::JsonParsed),
                transaction_details: Some(TransactionDetails::Full),
                rewards: Some(true),
                max_supported_transaction_version: None,
                commitment: Some(CommitmentConfig::confirmed()),
            },
        ) {
            Ok(block) => {
                println!("Block for slot {}: {:?}", slot, block);
                match block.transactions {
                    Some(transactions) => {
                        for transaction_with_meta in transactions {
                            match transaction_with_meta.transaction {
                                EncodedTransaction::Json(transaction) => {
                                    println!("Transaction: {:#?}", transaction);

                                    // extract and print meta
                                    if let Some(meta) = transaction_with_meta.meta.clone() {
                                        println!("Transaction meta: {:#?}", meta);

                                        // extract and print log messages
                                        if let OptionSerializer::Some(log_messages) =
                                            meta.log_messages
                                        {
                                            for log in log_messages {
                                                println!("Log: {}", log);
                                            }
                                        }
                                    }

                                    if let Some(meta) = transaction_with_meta.meta.clone() {
                                        match meta.log_messages {
                                            OptionSerializer::Some(log_messages) => {
                                                for log_message in log_messages {
                                                    println!("Log Message: {}", log_message);
                                                }
                                            }
                                            OptionSerializer::None => todo!(),
                                            OptionSerializer::Skip => todo!(),
                                        }
                                    }

                                    match transaction.message {
                                        UiMessage::Parsed(parsed_message) => {
                                            for instruction in parsed_message.instructions {
                                                match instruction {
                                                    UiInstruction::Parsed(parsed_instruction) => {
                                                        match parsed_instruction {
                                                            UiParsedInstruction::Parsed(parsed_parsed_instruction) => todo!(),
                                                            UiParsedInstruction::PartiallyDecoded(partially_decoded_parsed_instruction) => todo!(),
                                                        }
                                                    },
                                                    UiInstruction::Compiled(compiled_instr) => {
                                                        // handle_compiled_instruction(compiled_instr);
                                                    },
                                                }
                                            }
                                        }
                                        UiMessage::Raw(_) => todo!(),
                                    }
                                }
                                _ => {}
                            }
                        }
                    }
                    None => {}
                }
            }
            Err(err) => {
                println!("Error getting block for slot {}: {:?}", slot, err);
            }
        }
    }

    Ok(())
}
