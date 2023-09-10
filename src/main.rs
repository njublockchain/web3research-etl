mod clickhouse_arb_nova;
mod clickhouse_arb_one;
mod clickhouse_btc;
mod clickhouse_eth;
mod clickhouse_scheme;
mod clickhouse_tron;

use clap::Parser;
use std::error::Error;

extern crate pretty_env_logger;

/// Simple program to greet a person
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
        /// Name of the person to greet
        #[arg(short, long, value_enum, default_value_t = SupportedChain::Ethereum)]
        chain: SupportedChain,

        #[arg(long, default_value = "clickhouse://localhost:9000")]
        db: String,

        #[arg(short, long, default_value = "ws://localhost:8545")]
        provider: String,

        /// provider uri for init trace, can be null when blockchain not requiring trace data
        #[arg(long = "trace", default_value = None)]
        trace_provider: Option<String>,

        #[arg(long, value_enum, default_value_t = ProviderType::Default )]
        provider_type: ProviderType,

        /// from block
        #[arg(long, default_value_t = 0)]
        from: u64,

        /// init batch size
        #[arg(long, default_value_t = 1u64)]
        batch: u64,
    },
    Sync {
        /// Name of the person to greet
        #[arg(short, long, value_enum, default_value_t = SupportedChain::Ethereum)]
        chain: SupportedChain,

        #[arg(long, default_value = "clickhouse://localhost:9000")]
        db: String,

        #[arg(short, long, default_value = "ws://localhost:8545")]
        provider: String,

        /// provider uri for sync trace
        #[arg(long = "trace", default_value = None)]
        trace_provider: Option<String>,

        #[arg(long, value_enum, default_value_t = ProviderType::Default )]
        provider_type: ProviderType,
    },
    Check {
        /// from block
        #[arg(long, default_value_t = 0)]
        from: u64,

        /// Name of the person to greet
        #[arg(short, long, value_enum, default_value_t = SupportedChain::Ethereum)]
        chain: SupportedChain,

        #[arg(long, default_value = "clickhouse://default@localhost:9000")]
        db: String,

        #[arg(short, long, default_value = "ws://localhost:8545")]
        provider: String,

        /// provider uri for sync trace
        #[arg(long = "trace", default_value = None)]
        trace_provider: Option<String>,

        #[arg(long, value_enum, default_value_t = ProviderType::Default )]
        provider_type: ProviderType,
    },
}

#[derive(clap::ValueEnum, Clone, PartialEq, Eq, Debug)]
pub enum SupportedChain {
    Ethereum,
    Bitcoin,
    Tron,
    ArbitrumOne,
    ArbitrumNova,
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
                clickhouse_btc::init::init(db, provider, provider_type, from, batch).await?
            }
            SupportedChain::Ethereum => {
                clickhouse_eth::init::init(db, provider, trace_provider, provider_type, from, batch)
                    .await?
            }
            SupportedChain::Tron => {}
            SupportedChain::ArbitrumOne => {
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
        },
        ClapActionType::Sync {
            chain,
            db,
            provider,
            trace_provider,
            provider_type,
        } => match chain {
            SupportedChain::Bitcoin => {}
            SupportedChain::Ethereum => {
                clickhouse_eth::sync::sync(db, provider, trace_provider, provider_type).await?
            }
            SupportedChain::Tron => {}
            SupportedChain::ArbitrumOne => todo!(),
            SupportedChain::ArbitrumNova => todo!(),
        },
        ClapActionType::Check {
            from,
            chain,
            db,
            provider,
            trace_provider,
            provider_type,
        } => match chain {
            SupportedChain::Bitcoin => {}
            SupportedChain::Ethereum => {
                clickhouse_eth::check::check(db, provider, trace_provider, provider_type, from).await?;
            }
            SupportedChain::Tron => {}
            SupportedChain::ArbitrumOne => {}
            SupportedChain::ArbitrumNova => {}
        },
    }
    // if args.db.starts_with("clickhouse") {
    //     clickhouse_eth::main(&args.db, &args.provider, &args.action_type).await?;
    // }

    Ok(())
}
