use std::error::Error;

use clickhouse::Row;

use crate::ProviderType;

#[derive(Row, Clone, Debug, serde::Deserialize)]
struct BlockNumberRow {
    number: u64,
}

/// check if the block data has been inserted into ClickHouse
/// if not, fetch the block data from the provider and insert it
/// into ClickHouse
pub async fn health_check(
    client: &clickhouse::Client,
    provider: String,
    _trace_provider: Option<String>,
    _provider_type: ProviderType,
    block_number: u64,
) -> Result<(), Box<dyn Error>> {
    let block_result = client
        .query("SELECT number FROM blocks WHERE number = ?")
        .bind(block_number)
        .fetch_one::<BlockNumberRow>()
        .await;
    panic!("health_check not implemented");

    Ok(())
}

pub(crate) async fn sync(
    db: String,
    provider: String,
    from: u64,
    batch: u64,
) -> Result<(), Box<dyn Error>> {
    panic!("sync not implemented");

    Ok(())
}
