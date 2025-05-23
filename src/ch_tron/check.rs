use futures::StreamExt;
use klickhouse::{Client, ClientOptions, Row};
use log::{debug, info};
use std::error::Error;
use tron_grpc::EmptyMessage;
use url::Url;

use crate::{ch_tron::sync::health_check, ProviderType};

pub(crate) async fn check(
    db: String,
    provider_uri: String,
    _trace_provider_uri: Option<String>,
    _provider_type: ProviderType,
    from: u64,
) -> Result<(), Box<dyn Error>> {
    let clickhouse_url = Url::parse(&db).unwrap();

    let clickhouse_options =
        if clickhouse_url.path() != "/default" || !clickhouse_url.username().is_empty() {
            ClientOptions {
                username: clickhouse_url.username().to_string(),
                password: clickhouse_url.password().unwrap_or("").to_string(),
                default_database: clickhouse_url
                    .path()
                    .to_string()
                    .strip_prefix('/')
                    .unwrap()
                    .to_string(),
            }
        } else {
            ClientOptions::default()
        };

    debug!("start checking for missing blocks");

    // Create Tron gRPC client
    let mut tron_client = tron_grpc::wallet_client::WalletClient::connect(provider_uri.to_string())
        .await
        .map_err(|e| format!("Failed to connect to Tron gRPC provider: {}", e))?;

    // Get latest block number
    let latest_block_number = tron_client
        .get_now_block2(EmptyMessage {})
        .await?
        .into_inner()
        .block_header
        .unwrap()
        .raw_data
        .unwrap()
        .number;

    info!("current chain height {}", latest_block_number);

    let client = Client::connect(
        format!(
            "{}:{}",
            clickhouse_url.host().unwrap(),
            clickhouse_url.port().unwrap()
        ),
        clickhouse_options.clone(),
    )
    .await?;

    #[derive(Row, Clone, Debug)]
    struct NumberRow {
        number: i64,
    }

    // Find missing blocks
    let query = r#"
        WITH (SELECT max(number) FROM blocks) AS max_number
        SELECT
            number
        FROM
            (SELECT arrayJoin(range(1, max_number + 1)) AS number)
        WHERE
            number NOT IN (SELECT number FROM blocks)
        ORDER BY number;
    "#;

    let mut missing_blocks_stream = client.query::<NumberRow>(query).await?;

    let mut missing_blocks = Vec::new();
    while let Some(row) = missing_blocks_stream.next().await {
        let row = row?;
        missing_blocks.push(row);
    }

    let missing_count = missing_blocks.len();
    info!("found {} missing blocks", missing_count);

    // Also check for blocks that might be missing after the current max block
    let max_query = "SELECT max(number) as number FROM blocks";
    let max_block = client.query_one::<NumberRow>(max_query).await?;

    info!("local max height {}", max_block.number);

    // Process missing blocks found by the query
    for row in missing_blocks {
        info!("processing missing block {}", row.number);
        health_check(client.clone(), &provider_uri, row.number).await?;
    }

    Ok(())
}
