use std::error::Error;

use ethers::{
    providers::{Http, Middleware, Provider, ProviderError},
    types::{Action, Block, Res, Trace, Transaction, TransactionReceipt},
};
use klickhouse::{u256, Client};
use log::{debug, error, info, warn};
use tokio::{task::JoinSet, try_join};
use tokio_retry::{
    strategy::{jitter, ExponentialBackoff},
    Retry,
};

use crate::clickhouse_eth::main::{BlockRow, EventRow, TraceRow, TransactionRow, WithdrawalRow};

pub(crate) async fn init_http(
    client: Client,
    provider: Provider<Http>,
    from: u64,
    init_trace: bool,
    batch: u64,
) -> Result<(), Box<dyn Error>> {
    debug!("start initializing schema");
    client
        .execute(
            "
        CREATE DATABASE IF NOT EXISTS ethereum;
        ",
        )
        .await
        .unwrap();
    client
        .execute(
            "
        CREATE TABLE IF NOT EXISTS ethereum.blocks (
            hash             FixedString(32),
            number           UInt64,
            parentHash       FixedString(32),
            uncles           Array(String),
            sha3Uncles       FixedString(32),           
            totalDifficulty  UInt256,
            miner            FixedString(20),
            difficulty       UInt256,
            nonce            FixedString(8),
            mixHash          FixedString(32),
            baseFeePerGas    Nullable(UInt256),
            gasLimit         UInt256,
            gasUsed          UInt256,
            stateRoot        FixedString(32),
            transactionsRoot FixedString(32),
            receiptsRoot     FixedString(32),
            logsBloom        String,
            withdrawlsRoot  Nullable(FixedString(32)),
            extraData        String,
            timestamp        UInt256,
            size             UInt256,
        ) ENGINE=ReplacingMergeTree 
        ORDER BY (hash, number);
        ",
        )
        .await
        .unwrap();
    client.execute("
        CREATE TABLE IF NOT EXISTS ethereum.transactions (
            hash             FixedString(32),
            blockHash        FixedString(32),
            blockNumber      UInt64,
            blockTimestamp   UInt256,
            transactionIndex UInt64,
            chainId Nullable(UInt256),
            type    Nullable(UInt64),
            from             FixedString(20),
            to               Nullable(FixedString(20)),
            value            UInt256,
            nonce            UInt256,
            input            String,
            gas                  UInt256,
            gasPrice             Nullable(UInt256),
            maxFeePerGas         Nullable(UInt256),
            maxPriorityFeePerGas Nullable(UInt256),
            r UInt256,
            s UInt256,
            v UInt64,
            accessList Nullable(String),
            contractAddress Nullable(FixedString(20)),
            cumulativeGasUsed UInt256,
            effectiveGasPrice Nullable(UInt256),
            gasUsed           UInt256,
            logsBloom         String,
            root              Nullable(FixedString(32)) COMMENT 'Only present before activation of [EIP-658]',
            status            Nullable(UInt64) COMMENT 'Only present after activation of [EIP-658]'
        ) ENGINE=ReplacingMergeTree
        ORDER BY hash;
        ").await.unwrap();
    client
        .execute(
            "
        CREATE TABLE IF NOT EXISTS ethereum.events (
            address FixedString(20),
            blockHash FixedString(32),
            blockNumber UInt64,
            blockTimestamp UInt256,
            transactionHash FixedString(32),
            transactionIndex UInt64,
            logIndex UInt256,
            removed Boolean,
            topics Array(FixedString(32)),
            data String,
        ) ENGINE=ReplacingMergeTree
        ORDER BY (transactionHash, logIndex);
        ",
        )
        .await
        .unwrap();
    client
        .execute(
            "
        CREATE TABLE IF NOT EXISTS ethereum.withdraws (
            blockHash String,
            blockNumber UInt64,
            blockTimestamp UInt256,
            `index` UInt64,
            validatorIndex UInt64,
            address FixedString(20),
            amount UInt256
        ) ENGINE=ReplacingMergeTree
        ORDER BY (blockHash, index);
        ",
        )
        .await
        .unwrap();
    client
        .execute(
            "
            CREATE TABLE IF NOT EXISTS ethereum.traces
            (
                `blockPos`    UInt64,
                `blockNumber` UInt64,
                `blockTimestamp` UInt256,
                `blockHash` FixedString(32),
                `transactionHash` Nullable(FixedString(32)),
            
                `traceAddress` Array(UInt64),
            
                `subtraces` UInt64,
            
                `transactionPosition` Nullable(UInt64),
            
                `error` Nullable(String),
            
                `actionType` LowCardinality(String),
            
                `actionCallFrom` Nullable(FixedString(20)),
            
                `actionCallTo` Nullable(FixedString(20)),
            
                `actionCallValue` Nullable(UInt256),
            
                `actionCallInput` Nullable(String),
            
                `actionCallGas` Nullable(UInt256),
            
                `actionCallType` LowCardinality(String),
            
                `actionCreateFrom` Nullable(FixedString(20)),
            
                `actionCreateValue` Nullable(UInt256),
            
                `actionCreateInit` Nullable(String),
            
                `actionCreateGas` Nullable(UInt256),
            
                `actionSuicideAddress` Nullable(FixedString(20)),
            
                `actionSuicideRefundAddress` Nullable(FixedString(20)),
            
                `actionSuicideBalance` Nullable(UInt256),
            
                `actionRewardAuthor` Nullable(FixedString(20)),
            
                `actionRewardValue` Nullable(UInt256),
            
                `actionRewardType` LowCardinality(String),
            
                `resultType` LowCardinality(String),
            
                `resultCallGasUsed` Nullable(UInt256),
            
                `resultCallOutput` Nullable(String),
            
                `resultCreateGasUsed` Nullable(UInt256),
            
                `resultCreateCode` Nullable(String),
            
                `resultCreateAddress` Nullable(FixedString(20))
            )
            ENGINE = ReplacingMergeTree
            ORDER BY (blockNumber, blockPos);
        ",
        )
        .await
        .unwrap();
    debug!("schema initialized");

    let latest: u64 = provider.get_block_number().await?.as_u64();
    let to = latest / 1_000 * 1_000;

    warn!("target: {}", to);

    let retry_strategy = ExponentialBackoff::from_millis(100)
        .map(jitter) // add jitter to delays
        .take(3); // limit to 3 retries

    if !init_trace {
        let mut block_row_list = Vec::with_capacity((batch + 1_u64) as usize);
        let mut transaction_row_list = Vec::new();
        let mut event_row_list = Vec::new();
        let mut withdraw_row_list = Vec::new();

        async fn get_block_and_tx(
            provider: &Provider<Http>,
            num: u64,
        ) -> Result<(Option<Block<Transaction>>, Vec<TransactionReceipt>), ProviderError> {
            let result = tokio::try_join!(
                provider.get_block_with_txs(num),
                provider.get_block_receipts(num),
            );
            if result.is_err() {
                error!("{:?}", num);
                error!("{:?}", result);
            }

            result
        }

        for num in from..=to {
            let (block, receipts) =
                Retry::spawn(retry_strategy.clone(), || get_block_and_tx(&provider, num)).await?;

            let block = block.as_ref().unwrap();

            let block_row = BlockRow {
                hash: block.hash.unwrap().0.to_vec().into(), //block.hash.unwrap()),
                number: block.number.unwrap().as_u64(),
                parentHash: block.parent_hash.0.to_vec().into(),
                uncles: block
                    .uncles
                    .iter()
                    .map(|uncle| uncle.0.to_vec().into())
                    .collect(),
                sha3Uncles: block.uncles_hash.0.to_vec().into(),
                totalDifficulty: u256(block.total_difficulty.unwrap().into()),
                difficulty: u256(block.difficulty.into()),
                miner: block.author.unwrap().0.to_vec().into(),
                nonce: block.nonce.unwrap().0.to_vec().into(),
                mixHash: block.mix_hash.unwrap().0.to_vec().into(),
                baseFeePerGas: block
                    .base_fee_per_gas
                    .and_then(|fee| Some(u256(fee.into()))),
                gasLimit: u256(block.gas_limit.into()),
                gasUsed: u256(block.gas_used.into()),
                stateRoot: block.state_root.0.to_vec().into(),
                transactionsRoot: block.transactions_root.0.to_vec().into(),
                receiptsRoot: block.receipts_root.0.to_vec().into(),
                logsBloom: block.logs_bloom.unwrap().0.to_vec().into(),
                withdrawlsRoot: block
                    .withdrawals_root
                    .and_then(|root| Some(root.0.to_vec().into())),
                extraData: block.extra_data.to_vec().into(),
                timestamp: u256(block.timestamp.into()),
                size: u256(block.size.unwrap().into()),
            };
            block_row_list.push(block_row);

            for (transaction_index, transaction) in block.transactions.iter().enumerate() {
                let receipt = &receipts[transaction_index];

                let transaction_row = TransactionRow {
                    hash: transaction.hash.0.to_vec().into(),
                    blockHash: transaction.block_hash.unwrap().0.to_vec().into(),
                    blockNumber: transaction.block_number.unwrap().as_u64(),
                    blockTimestamp: u256(block.timestamp.into()),
                    transactionIndex: transaction.transaction_index.unwrap().as_u64(),
                    chainId: transaction.chain_id.and_then(|id| Some(u256(id.into()))),
                    r#type: transaction.transaction_type.and_then(|t| Some(t.as_u64())),
                    from: transaction.from.0.to_vec().into(),
                    to: transaction.to.and_then(|to| Some(to.0.to_vec().into())),
                    value: u256(transaction.value.into()),
                    nonce: u256(transaction.nonce.into()),
                    input: transaction.input.to_vec().into(),
                    gas: u256(transaction.gas.into()),
                    gasPrice: transaction
                        .gas_price
                        .and_then(|price| Some(u256(price.into()))),
                    maxFeePerGas: transaction
                        .max_fee_per_gas
                        .and_then(|fee| Some(u256(fee.into()))),
                    maxPriorityFeePerGas: transaction
                        .max_priority_fee_per_gas
                        .and_then(|fee| Some(u256(fee.into()))),
                    r: u256(transaction.r.into()),
                    s: u256(transaction.s.into()),
                    v: transaction.v.as_u64(),
                    accessList: transaction.access_list.as_ref().and_then(|al| {
                        Some(serde_json::to_string(&al.clone().to_owned()).unwrap())
                    }),
                    contractAddress: receipt
                        .contract_address
                        .and_then(|contract| Some(contract.0.to_vec().into())),
                    cumulativeGasUsed: u256(receipt.cumulative_gas_used.into()),
                    effectiveGasPrice: receipt
                        .effective_gas_price
                        .and_then(|price| Some(u256(price.into()))),
                    gasUsed: u256(receipt.gas_used.unwrap().into()),
                    logsBloom: receipt.logs_bloom.0.to_vec().into(),
                    root: receipt.root.and_then(|root| Some(root.0.to_vec().into())), // Only present before activation of [EIP-658]
                    status: receipt.status.and_then(|status| Some(status.as_u64())), // Only present after activation of [EIP-658]
                };
                transaction_row_list.push(transaction_row);

                for log in &receipt.logs {
                    let mut event_row = EventRow {
                        blockHash: log.block_hash.unwrap().0.to_vec().into(),
                        blockNumber: log.block_number.unwrap().as_u64(),
                        blockTimestamp: u256(block.timestamp.into()),
                        transactionHash: transaction.hash.0.to_vec().into(),
                        transactionIndex: transaction.transaction_index.unwrap().as_u64(),
                        logIndex: u256(log.log_index.unwrap().into()),
                        removed: log.removed.unwrap(),
                        topics: log
                            .topics
                            .iter()
                            .map(|topic| topic.0.to_vec().into())
                            .collect(),
                        data: log.data.to_vec().into(),
                        address: log.address.0.to_vec().into(),
                    };
                    event_row_list.push(event_row);
                }
            }

            if let Some(withdraws) = &block.withdrawals {
                for withdraw in withdraws {
                    let withdraw_row = WithdrawalRow {
                        blockHash: block.hash.unwrap().0.to_vec().into(),
                        blockNumber: block.number.unwrap().as_u64(),
                        blockTimestamp: u256(block.timestamp.into()),
                        index: withdraw.index.as_u64(),
                        validatorIndex: withdraw.validator_index.as_u64(),
                        address: withdraw.address.0.to_vec().into(),
                        amount: u256(withdraw.amount.into()),
                    };
                    withdraw_row_list.push(withdraw_row);
                }
            }

            if (num - from + 1) % batch == 0 {
                tokio::try_join!(
                    client.insert_native_block(
                        "INSERT INTO ethereum.blocks FORMAT native",
                        block_row_list.to_vec()
                    ),
                    client.insert_native_block(
                        "INSERT INTO ethereum.transactions FORMAT native",
                        transaction_row_list.to_vec()
                    ),
                    client.insert_native_block(
                        "INSERT INTO ethereum.events FORMAT native",
                        event_row_list.to_vec()
                    ),
                    client.insert_native_block(
                        "INSERT INTO ethereum.withdraws FORMAT native",
                        withdraw_row_list.to_vec()
                    )
                )
                .unwrap();

                block_row_list.clear();
                transaction_row_list.clear();
                event_row_list.clear();
                withdraw_row_list.clear();

                info!("{} done blocks & txs", num)
            }
        }

        try_join!(
            client.insert_native_block("INSERT INTO ethereum.blocks FORMAT native", block_row_list),
            client.insert_native_block(
                "INSERT INTO ethereum.transactions FORMAT native",
                transaction_row_list
            ),
            client.insert_native_block("INSERT INTO ethereum.events FORMAT native", event_row_list),
            client.insert_native_block(
                "INSERT INTO ethereum.withdraws FORMAT native",
                withdraw_row_list
            )
        )
        .unwrap();
    } else {
        let mut trace_row_list = Vec::new();

        async fn get_block_traces(
            provider: &Provider<Http>,
            nums: Vec<u64>,
        ) -> Result<Vec<(Block<ethers::types::H256>, Vec<Trace>)>, Box<dyn Error>> {
            let mut set = JoinSet::new();

            for num in nums {
                let provider = provider.clone();
                set.spawn(async move {
                    try_join!(provider.get_block(num), provider.trace_block(num.into()))
                });
            }

            let mut results = Vec::new();
            while let Some(res) = set.join_next().await {
                let join_result = res?;
                let (block, traces) = join_result?;
                let block = block.unwrap();
                results.push((block, traces))
            }

            Ok(results)
        }

        for num in (from..=to).into_iter().step_by(batch as usize) {
            let end = if num + batch > to + 1 {
                to
            } else {
                num + batch
            };
            let nums: Vec<u64> = (num..end).collect();

            let results = Retry::spawn(retry_strategy.clone(), || {
                get_block_traces(&provider, nums.clone())
            })
            .await?;

            for (block, traces) in results {
                for (index, trace) in traces.iter().enumerate() {
                    let mut trace_row = TraceRow {
                        blockPos: index as u64,
                        actionType: serde_json::to_string(&trace.action_type)?,
                        actionCallFrom: None,
                        actionCallTo: None,
                        actionCallValue: None,
                        actionCallInput: None,
                        actionCallGas: None,
                        actionCallType: "".to_owned(),
                        actionCreateFrom: None,
                        actionCreateValue: None,
                        actionCreateInit: None,
                        actionCreateGas: None,
                        actionSuicideAddress: None,
                        actionSuicideRefundAddress: None,
                        actionSuicideBalance: None,
                        actionRewardAuthor: None,
                        actionRewardValue: None,
                        actionRewardType: "".to_owned(),
                        resultType: "".to_owned(),
                        resultCallGasUsed: None,
                        resultCallOutput: None,
                        resultCreateGasUsed: None,
                        resultCreateCode: None,
                        resultCreateAddress: None,
                        traceAddress: trace.trace_address.iter().map(|t| *t as u64).collect(),
                        subtraces: trace.subtraces as u64,
                        transactionPosition: trace
                            .transaction_position
                            .and_then(|pos| Some(pos as u64)),
                        transactionHash: trace
                            .transaction_hash
                            .and_then(|h| Some(h.0.to_vec().into())),
                        blockNumber: trace.block_number,
                        blockTimestamp: u256(block.timestamp.into()),
                        blockHash: trace.block_hash.0.to_vec().into(),
                        error: trace.error.clone(),
                    };

                    // fill action
                    match &trace.action {
                        Action::Call(call) => {
                            trace_row.actionCallFrom = Some(call.from.0.to_vec().into());
                            trace_row.actionCallTo = Some(call.to.0.to_vec().into());
                            trace_row.actionCallType = serde_json::to_string(&call.call_type)?;
                            trace_row.actionCallGas = Some(u256(call.gas.into()));
                            trace_row.actionCallInput = Some(call.input.0.to_vec().into());
                        }
                        Action::Create(create) => {
                            trace_row.actionCreateFrom = Some(create.from.0.to_vec().into());
                            trace_row.actionCreateInit = Some(create.init.0.to_vec().into());
                            trace_row.actionCreateValue = Some(u256(create.value.into()));
                            trace_row.actionCreateGas = Some(u256(create.gas.into()));
                        }
                        Action::Suicide(suicide) => {
                            trace_row.actionSuicideAddress =
                                Some(suicide.address.0.to_vec().into());
                            trace_row.actionSuicideBalance = Some(u256(suicide.balance.into()));
                            trace_row.actionSuicideRefundAddress =
                                Some(suicide.refund_address.0.to_vec().into());
                        }
                        Action::Reward(reward) => {
                            trace_row.actionRewardAuthor = Some(reward.author.0.to_vec().into());
                            trace_row.actionRewardType =
                                serde_json::to_string(&reward.reward_type)?;
                            trace_row.actionRewardValue = Some(u256(reward.value.into()));
                        }
                    }

                    match &trace.result {
                        Some(result) => match result {
                            Res::Call(call) => {
                                trace_row.resultType = "call".to_owned();
                                trace_row.resultCallGasUsed = Some(u256(call.gas_used.into()));
                                trace_row.resultCallOutput = Some(call.output.0.to_vec().into());
                            }
                            Res::Create(create) => {
                                trace_row.resultType = "create".to_owned();
                                trace_row.resultCreateAddress =
                                    Some(create.address.0.to_vec().into());
                                trace_row.resultCreateCode = Some(create.code.0.to_vec().into());
                                trace_row.resultCreateGasUsed = Some(u256(create.gas_used.into()))
                            }
                            Res::None => {
                                // trace_row.resultType =
                            }
                        },
                        None => {} //trace_row.resultType = "none".to_owned(),
                    }

                    trace_row_list.push(trace_row);
                }
            }

            client
                .insert_native_block(
                    "INSERT INTO ethereum.traces FORMAT native",
                    trace_row_list.to_vec(),
                )
                .await?;

            trace_row_list.clear();

            info!("{:?} done block traces", nums)
        }
    }

    Ok(())
}
