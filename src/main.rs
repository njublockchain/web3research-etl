mod clickhouse_arb_nova;
mod clickhouse_arb_one;
mod clickhouse_btc;
mod clickhouse_eth;
mod clickhouse_polygon;
mod clickhouse_tron;

mod clickhouse_scheme;

use clap::Parser;
use std::error::Error;

extern crate pretty_env_logger;

/// Web3Research-ETL is a multichain-supported data ETL kit used on https://web3resear.ch
/// If you find any bugs or have any suggestions, please feel free to open an issue on http://github.com/njublockchain/web3research-etl/issues
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    /// action type
    #[command(subcommand)]
    action_type: ClapActionType,
}

#[derive(clap::Subcommand, PartialEq, Eq, Debug)]
pub enum ClapActionType {
    Init {
        /// The chain to initialize
        #[arg(short, long, value_enum, default_value_t = SupportedChain::Ethereum)]
        chain: SupportedChain,

        /// The ClickHouse database DSN, [chain] will be replaced by the chain name
        #[arg(long, default_value = "clickhouse://localhost:9000/[chain]")]
        db: String,

        /// The provider URI of the chain's RPC node
        #[arg(short, long, default_value = "ws://localhost:8545")]
        provider: String,

        /// The provider URI of the chain's RPC node for init trace, can be null when trace data is not needed
        #[arg(long = "trace", default_value = None)]
        trace_provider: Option<String>,

        /// The provider type, default is the official provider, e.g. geth and bitcoind
        #[arg(long, value_enum, default_value_t = ProviderType::Default )]
        provider_type: ProviderType,

        /// The block number to start from
        #[arg(long, default_value_t = 0)]
        from: u64,

        /// The batch size while initializing
        #[arg(long, default_value_t = 1u64)]
        batch: u64,
    },
    Sync {
        /// The chain to synchronize
        #[arg(short, long, value_enum, default_value_t = SupportedChain::Ethereum)]
        chain: SupportedChain,

        /// The ClickHouse database DSN, [chain] will be replaced by the chain name
        #[arg(long, default_value = "clickhouse://localhost:9000/[chain]")]
        db: String,

        /// The provider URI of the chain's RPC node
        #[arg(short, long, default_value = "ws://localhost:8545")]
        provider: String,

        /// You can provide a trace provider to get the trace data, can be null when trace data is not needed
        #[arg(long = "trace", default_value = None)]
        trace_provider: Option<String>,

        /// The provider type, default is the official provider, e.g. geth and bitcoind
        #[arg(long, value_enum, default_value_t = ProviderType::Default )]
        provider_type: ProviderType,
    },
    Check {
        /// The chain to check the errors or missing data
        #[arg(short, long, value_enum, default_value_t = SupportedChain::Ethereum)]
        chain: SupportedChain,

        /// The ClickHouse database DSN, [chain] will be replaced by the chain name
        #[arg(long, default_value = "clickhouse://default@localhost:9000/[chain]")]
        db: String,

        /// The provider URI of the chain's RPC node
        #[arg(short, long, default_value = "ws://localhost:8545")]
        provider: String,

        /// The provider URI of the chain's RPC node for init trace, can be null when trace data is not needed
        #[arg(long = "trace", default_value = None)]
        trace_provider: Option<String>,

        /// The provider type, default is the official non-archive-optimised provider, e.g. geth and bitcoind
        #[arg(long, value_enum, default_value_t = ProviderType::Default )]
        provider_type: ProviderType,

        /// The block number to start from
        #[arg(long, default_value_t = 0)]
        from: u64,
    },
}

#[derive(clap::ValueEnum, Clone, PartialEq, Eq, Debug)]
pub enum SupportedChain {
    Ethereum,
    Bitcoin,
    Tron,
    ArbitrumOne,
    ArbitrumNova,
    Polygon,
}

#[derive(clap::ValueEnum, Copy, Clone, PartialEq, Eq, Debug)]
pub enum ProviderType {
    Default,
    Erigon,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    pretty_env_logger::init_timed();
    let args = Args::parse();

    match args.action_type {
        ClapActionType::Init {
            chain,
            db,
            provider,
            from,
            trace_provider,
            batch,
            provider_type,
        } => match chain {
            SupportedChain::Bitcoin => {
                let chain_name = "bitcoin";
                let provider = provider.replace("[chain]", chain_name);
                let trace_provider = trace_provider.map(|x| x.replace("[chain]", chain_name));

                clickhouse_btc::init::init(db, provider, provider_type, from, batch).await?
            }
            SupportedChain::Ethereum => {
                let chain_name = "ethereum";
                let provider = provider.replace("[chain]", chain_name);
                let trace_provider = trace_provider.map(|x| x.replace("[chain]", chain_name));

                clickhouse_eth::init::init(db, provider, trace_provider, provider_type, from, batch)
                    .await?
            }
            SupportedChain::Tron => clickhouse_tron::init::init(db, provider, from, batch).await?,
            SupportedChain::ArbitrumOne => {
                let chain_name = "arbitrum-one";
                let provider = provider.replace("[chain]", chain_name);
                let trace_provider = trace_provider.map(|x| x.replace("[chain]", chain_name));

                clickhouse_arb_one::init::init(
                    db,
                    provider,
                    trace_provider,
                    provider_type,
                    from,
                    batch,
                )
                .await?
            }
            SupportedChain::ArbitrumNova => {
                let chain_name = "arbitrum-nova";
                let provider = provider.replace("[chain]", chain_name);
                let trace_provider = trace_provider.map(|x| x.replace("[chain]", chain_name));

                clickhouse_arb_nova::init::init(
                    db,
                    provider,
                    trace_provider,
                    provider_type,
                    from,
                    batch,
                )
                .await?
            }
            SupportedChain::Polygon => {
                let chain_name = "polygon";
                let provider = provider.replace("[chain]", chain_name);
                let trace_provider = trace_provider.map(|x| x.replace("[chain]", chain_name));

                clickhouse_polygon::init::init(
                    db,
                    provider,
                    trace_provider,
                    provider_type,
                    from,
                    batch,
                )
                .await?
            }
        },
        ClapActionType::Sync {
            chain,
            db,
            provider,
            trace_provider,
            provider_type,
        } => match chain {
            SupportedChain::Bitcoin => todo!(),
            SupportedChain::Ethereum => {
                let chain_name = "ethereum";
                let provider = provider.replace("[chain]", chain_name);
                let trace_provider = trace_provider.map(|x| x.replace("[chain]", chain_name));

                clickhouse_eth::sync::sync(db, provider, trace_provider, provider_type).await?
            }
            SupportedChain::Tron => todo!(),
            SupportedChain::ArbitrumOne => todo!(),
            SupportedChain::ArbitrumNova => todo!(),
            SupportedChain::Polygon => {
                let chain_name = "polygon";
                let provider = provider.replace("[chain]", chain_name);
                let trace_provider = trace_provider.map(|x| x.replace("[chain]", chain_name));

                clickhouse_polygon::sync::sync(db, provider, trace_provider, provider_type).await?
            }
        },
        ClapActionType::Check {
            from,
            chain,
            db,
            provider,
            trace_provider,
            provider_type,
        } => match chain {
            SupportedChain::Bitcoin => {
                let chain_name = "bitcoin";
                let provider = provider.replace("[chain]", chain_name);
                let trace_provider = trace_provider.map(|x| x.replace("[chain]", chain_name));

                clickhouse_btc::check::check(db, provider, trace_provider, provider_type, from)
                    .await?;
            }
            SupportedChain::Ethereum => {
                let chain_name = "ethereum";
                let provider = provider.replace("[chain]", chain_name);
                let trace_provider = trace_provider.map(|x| x.replace("[chain]", chain_name));

                clickhouse_eth::check::check(db, provider, trace_provider, provider_type, from)
                    .await?;
            }
            SupportedChain::Tron => {}
            SupportedChain::ArbitrumOne => {}
            SupportedChain::ArbitrumNova => {}
            SupportedChain::Polygon => {
                let chain_name = "polygon";
                let provider = provider.replace("[chain]", chain_name);
                let trace_provider = trace_provider.map(|x| x.replace("[chain]", chain_name));

                clickhouse_polygon::check::check(db, provider, trace_provider, provider_type, from)
                    .await?;
            }
        },
    }
    // if args.db.starts_with("clickhouse") {
    //     clickhouse_eth::main(&args.db, &args.provider, &args.action_type).await?;
    // }

    Ok(())
}
