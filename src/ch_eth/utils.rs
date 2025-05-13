use std::error::Error;
use std::fmt;
use std::time::Duration;
use std::pin::Pin;

use ethers::{
    providers::{Http, Middleware, Provider, ProviderError, StreamExt, Ws},
    types::{Block, H256, Transaction, TransactionReceipt},
};
use futures::stream::Stream;
use tokio::time::interval;
use tokio_stream::wrappers::IntervalStream;

/// Helper function to determine if a URL is a WebSocket URL
pub fn is_ws_url(url: &str) -> bool {
    url.starts_with("ws://") || url.starts_with("wss://")
}

/// A provider that can be either WebSocket or HTTP
pub enum EthProvider {
    Ws(Provider<Ws>),
    Http(Provider<Http>),
}

impl fmt::Debug for EthProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Ws(_) => write!(f, "EthProvider::Ws"),
            Self::Http(_) => write!(f, "EthProvider::Http"),
        }
    }
}

impl EthProvider {
    pub async fn get_block_with_txs(&self, block_number: u64) -> Result<Option<Block<Transaction>>, ProviderError> {
        match self {
            EthProvider::Ws(provider) => provider.get_block_with_txs(block_number).await,
            EthProvider::Http(provider) => provider.get_block_with_txs(block_number).await,
        }
    }

    pub async fn get_block_receipts(&self, block_number: ethers::types::BlockNumber) -> Result<Vec<TransactionReceipt>, ProviderError> {
        match self {
            EthProvider::Ws(provider) => provider.get_block_receipts(block_number).await,
            EthProvider::Http(provider) => provider.get_block_receipts(block_number).await,
        }
    }

    pub async fn trace_block(&self, block_number: ethers::types::BlockNumber) -> Result<Vec<ethers::types::Trace>, ProviderError> {
        match self {
            EthProvider::Ws(provider) => provider.trace_block(block_number).await,
            EthProvider::Http(provider) => provider.trace_block(block_number).await,
        }
    }

    pub async fn get_transaction_receipt(&self, tx_hash: ethers::types::H256) -> Result<Option<TransactionReceipt>, ProviderError> {
        match self {
            EthProvider::Ws(provider) => provider.get_transaction_receipt(tx_hash).await,
            EthProvider::Http(provider) => provider.get_transaction_receipt(tx_hash).await,
        }
    }

    pub async fn get_block_number(&self) -> Result<ethers::types::U64, ProviderError> {
        match self {
            EthProvider::Ws(provider) => provider.get_block_number().await,
            EthProvider::Http(provider) => provider.get_block_number().await,
        }
    }
    
    pub async fn get_block(&self, block_number: u64) -> Result<Option<Block<H256>>, ProviderError> {
        match self {
            EthProvider::Ws(provider) => provider.get_block(block_number).await,
            EthProvider::Http(provider) => provider.get_block(block_number).await,
        }
    }
    
    pub async fn subscribe_blocks(&self) -> Result<Pin<Box<dyn Stream<Item = Block<H256>> + Send + '_>>, ProviderError> {
        match self {
            EthProvider::Ws(provider) => {
                let stream = provider.subscribe_blocks().await?;
                Ok(Box::pin(stream))
            },
            EthProvider::Http(provider) => {
                // For HTTP providers, we implement polling with a default 1-second interval
                let poll_interval = Duration::from_secs(1);
                
                // Create a stream that polls for new blocks at specified interval
                let mut interval = IntervalStream::new(interval(poll_interval));
                
                // Clone the provider to move into the async block
                let provider = provider.clone();
                
                // Keep track of the last block number we've seen
                let mut last_block_number = provider.get_block_number().await?;
                
                // Create a stream that polls for new blocks
                let stream = async_stream::stream! {
                    while interval.next().await.is_some() {
                        // Get the latest block number
                        match provider.get_block_number().await {
                            Ok(block_number) => {
                                // If we have a new block
                                if block_number > last_block_number {
                                    // For each new block, fetch and yield it
                                    for num in (last_block_number.as_u64() + 1)..=block_number.as_u64() {
                                        if let Ok(Some(block)) = provider.get_block(num).await {
                                            yield block;
                                        }
                                    }
                                    // Update last block number
                                    last_block_number = block_number;
                                }
                            },
                            Err(e) => {
                                log::error!("Error polling for new blocks: {:?}", e);
                                // Optionally add a delay to avoid spamming in error cases
                                tokio::time::sleep(Duration::from_secs(1)).await;
                            }
                        }
                    }
                };
                
                Ok(Box::pin(stream))
            },
        }
    }

    pub fn provider_type(&self) -> &'static str {
        match self {
            EthProvider::Ws(_) => "WebSocket",
            EthProvider::Http(_) => "HTTP",
        }
    }
}

/// Create a provider based on the URL
pub async fn create_provider(provider_url: &str) -> Result<EthProvider, Box<dyn Error>> {
    if is_ws_url(provider_url) {
        log::debug!("Creating WebSocket provider for URL: {}", provider_url);
        let provider = Provider::<Ws>::connect(provider_url).await?;
        log::debug!("WebSocket connection established");
        Ok(EthProvider::Ws(provider))
    } else {
        log::debug!("Creating HTTP provider for URL: {}", provider_url);
        let provider = Provider::<Http>::try_from(provider_url)?;
        log::debug!("HTTP provider created");
        Ok(EthProvider::Http(provider))
    }
}

/// Create a WS Provider (kept for backward compatibility)
pub async fn create_ws_provider(provider_url: &str) -> Result<Provider<Ws>, Box<dyn Error>> {
    Ok(Provider::<Ws>::connect(provider_url).await?)
}

/// Create an HTTP provider (kept for backward compatibility)
pub fn create_http_provider(provider_url: &str) -> Result<Provider<Http>, Box<dyn Error>> {
    Ok(Provider::<Http>::try_from(provider_url)?)
}
