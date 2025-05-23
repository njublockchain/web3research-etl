use futures::StreamExt;
use klickhouse::{Client, ClientOptions, Row};
use log::{debug, info};
use std::error::Error;
use url::Url;

use crate::{
    ch_eth::{sync::health_check, utils::create_provider},
    ProviderType,
};

pub(crate) async fn check(
    db: String,
    provider_uri: String,
    trace_provider_uri: Option<String>,
    provider_type: ProviderType,
    from: u64,
) -> Result<(), Box<dyn Error>> {
    let clickhouse_url = Url::parse(&db).unwrap();
    // warn!("db: {} path: {}", format!("{}:{}", clickhouse_url.host().unwrap(), clickhouse_url.port().unwrap()), clickhouse_url.path());

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

    debug!("start listening");

    let provider = create_provider(&provider_uri).await?;
    info!(
        "Created check provider of type: {}",
        provider.provider_type()
    );

    let trace_provider = match trace_provider_uri {
        Some(trace_uri) => {
            let provider = create_provider(&trace_uri).await?;
            info!(
                "Created check trace provider of type: {}",
                provider.provider_type()
            );
            Some(provider)
        }
        None => None,
    };

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
        number: u64,
    }

    debug!("start checking for missing blocks");
    let latest: u64 = provider.get_block_number().await?.as_u64();
    info!("current chain height {}", latest);

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
    
    let mut missing_blocks_stream = client
        .query::<NumberRow>(query)
        .await?;
    let mut missing_blocks = Vec::new();
    while let Some(row) = missing_blocks_stream.next().await {
        let row = row?;
        missing_blocks.push(row);
    }
    
    let missing_count = missing_blocks.len();
    info!("found {} missing blocks", missing_count);
    
    // Also check for blocks that might be missing after the current max block
    let max_query = "SELECT max(number) as number FROM blocks";
    let max_block = client
        .query_one::<NumberRow>(max_query)
        .await?;
    
    info!("local max height {}", max_block.number);
    
    // Process missing blocks found by the query
    for row in missing_blocks {
        info!("processing missing block {}", row.number);
        health_check(
            client.clone(),
            &provider,
            &trace_provider,
            provider_type,
            row.number,
        )
        .await;
    }
    
    // Process blocks from the max_block to the latest block if needed
    if max_block.number < latest {
        info!("processing blocks from {} to {}", max_block.number + 1, latest);
        for num in (max_block.number + 1)..=latest {
            health_check(
                client.clone(),
                &provider,
                &trace_provider,
                provider_type,
                num,
            )
            .await;
        }
    }

    Ok(())
}
