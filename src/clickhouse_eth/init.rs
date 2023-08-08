use std::error::Error;

use ethers::{
    providers::{Http, Middleware, Provider, ProviderError, Ws},
    types::{Block, Transaction, TransactionReceipt},
};
use klickhouse::Client;
use log::{debug, error, info, warn};
use tokio_retry::{
    strategy::{jitter, ExponentialBackoff},
    Retry,
};

use crate::clickhouse_scheme::ethereum::{
    BlockRow, EventRow, TraceRow, TransactionRow, WithdrawalRow,
};

pub async fn get_block_details(
    provider: &Provider<Ws>,
    trace_provider: &Provider<Http>,
    num: u64,
) -> Result<
    (
        Option<Block<Transaction>>,
        Vec<TransactionReceipt>,
        Vec<ethers::types::Trace>,
    ),
    ProviderError,
> {
    let result = tokio::try_join!(
        provider.get_block_with_txs(num),
        provider.get_block_receipts(num),
        trace_provider.trace_block(num.into())
    );
    if result.is_err() {
        error!("{:?}: {:?}", num, result);
    }

    result
}


pub(crate) async fn init(
    client: Client,
    provider_ws: Provider<Ws>,
    from: u64,
    trace_provider: Provider<Http>,
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

    let latest: u64 = provider_ws.get_block_number().await?.as_u64();
    let to = latest / 1_000 * 1_000;

    warn!("target: {}", to);

    let retry_strategy = ExponentialBackoff::from_millis(100)
        .map(jitter) // add jitter to delays
        .take(3); // limit to 3 retries

    let mut block_row_list = Vec::with_capacity((batch + 1_u64) as usize);
    let mut transaction_row_list = Vec::new();
    let mut event_row_list = Vec::new();
    let mut withdraw_row_list = Vec::new();

    let mut trace_row_list = Vec::new();


    for num in from..=to {
        let (block, receipts, traces) = Retry::spawn(retry_strategy.clone(), || {
            get_block_details(&provider_ws, &trace_provider, num)
        })
        .await?;

        let block = block.as_ref().unwrap();

        let block_row = BlockRow::from_ethers(block);
        block_row_list.push(block_row);

        for (transaction_index, transaction) in block.transactions.iter().enumerate() {
            let receipt = &receipts[transaction_index];

            let transaction_row = TransactionRow::from_ethers(block, transaction, receipt);
            transaction_row_list.push(transaction_row);

            for log in &receipt.logs {
                let event_row = EventRow::from_ethers(block, transaction, log);
                event_row_list.push(event_row);
            }
        }

        if let Some(withdraws) = &block.withdrawals {
            for withdraw in withdraws {
                let withdraw_row = WithdrawalRow::from_ethers(block, withdraw);
                withdraw_row_list.push(withdraw_row);
            }
        }

        for (index, trace) in traces.into_iter().enumerate() {
            let trace_row = TraceRow::from_ethers(block, &trace, index);
            trace_row_list.push(trace_row);
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
                ),
                client.insert_native_block(
                    "INSERT INTO ethereum.traces FORMAT native",
                    trace_row_list.to_vec()
                )
            )
            .unwrap();

            block_row_list.clear();
            transaction_row_list.clear();
            event_row_list.clear();
            withdraw_row_list.clear();

            info!("{} done blocks & txs", num)
        }

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
            ),
            client.insert_native_block(
                "INSERT INTO ethereum.traces FORMAT native",
                trace_row_list.to_vec()
            )
        )
        .unwrap();
    }

    Ok(())
}
