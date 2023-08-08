mod clickhouse_eth;
mod helpers;
mod clickhouse_scheme;
mod graph_scheme;
mod graph;

use std::error::Error;

use clap::Parser;

extern crate pretty_env_logger;

/// Simple program to greet a person
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    /// Name of the person to greet
    #[arg(short, long, value_enum, default_value_t = SupportedChain::Ethereum)]
    chain: SupportedChain,

    #[arg(long, default_value = "mongodb://localhost:8545")]
    db: String,

    #[arg(short, long)]
    provider: String,

    /// action type
    #[command(subcommand)]
    action_type: ClapActionType,
}

#[derive(clap::Subcommand, PartialEq, Eq, Debug)]
pub enum ClapActionType {
    Init {
        /// from block
        #[arg(long, default_value_t = 0)]
        from: u64,

        /// init trace (not block or tx)
        #[arg(long="trace",  default_value_t = false)]
        init_trace: bool,

        /// init trace (not block or tx)
        #[arg(long,  default_value_t = 1u64)]
        batch: u64,
    },
    Sync {
        /// sync trace (not block or tx)
        #[arg(long="trace",  default_value_t = false)]
        sync_trace: bool

    },
    GraphQL {},
}

#[derive(clap::ValueEnum, Clone, PartialEq, Eq, Debug)]
pub enum SupportedChain {
    Ethereum,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    pretty_env_logger::init_timed();
    let args = Args::parse();

    if args.db.starts_with("clickhouse") {
        clickhouse_eth::main(&args.db, &args.provider, &args.action_type).await?;
    }

    Ok(())
}
