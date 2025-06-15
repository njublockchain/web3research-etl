# Web3Research-ETL

`web3research-etl` is a multichain-supported data ETL kit used on [Web3Research Platform](https://web3resear.ch)

## Supported Chains

✅: working perfect
❎: looking good but not work, temporarily
🚧: working in progress, coming soon

| Chain | `init` | `sync` | `check` |
| --- | --- | --- | --- |
| Ethereum(& FORKs/L2s) | ✅ | ✅ | ✅ |
| Bitcoin | ✅ | ✅ | ✅ |
| Tron | ✅ | ✅ | ✅ |
| Solana | 🚧 | 🚧 | 🚧 |
| Near | 🚧 | 🚧 | 🚧 |
| The Open Network(TON) | 🚧 | 🚧 | 🚧 |

## Install

1. Install the latest [Rustlang](https://www.rust-lang.org/tools/install)
2. Clone the repo `git clone https://github.com/njublockchain/web3research-etl && cd web3research-etl`
3. Build the executable `cargo build --release`
4. Run the executable `./target/release/web3research-etl -h`

## Examples

see commands in [`Makefile`](./Makefile)
