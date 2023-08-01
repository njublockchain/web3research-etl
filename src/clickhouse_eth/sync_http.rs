use std::error::Error;

use ethers::providers::{Provider, Http, Middleware, StreamExt};

async fn sync_http(provider: Provider<Http>) -> Result<(), Box<dyn Error>> {
    // if in db, update it
    // https://clickhouse.com/docs/en/guides/developer/deduplication
    let watcher = provider.watch_blocks().await?;
    let mut stream = watcher.stream();
    while let Some(block_hash) = stream.next().await {
        // handle block hash
    }

    Ok(())
}
