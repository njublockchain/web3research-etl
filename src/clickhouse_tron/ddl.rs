pub const ACCOUNT_CREATE_CONTRACT_DDL: &str = "
CREATE TABLE IF NOT EXISTS tron.accountCreateContracts
(
    `blockNum` Int64,
    `transactionHash` FixedString(32),
    `transactionIndex` Int64,
    `contractIndex` Int64,

    `ownerAddress` FixedString(34),
    `accountAddress` FixedString(34),
    `type` Int32,
)
ENGINE = ReplacingMergeTree
ORDER BY (transactionHash, contractIndex)
SETTINGS index_granularity = 8192;
";
pub const TRANSFER_CONTRACT_DDL: &str = "
CREATE TABLE IF NOT EXISTS tron.transferContracts
(
    `blockNum` Int64,
    `transactionHash` FixedString(32),
    `transactionIndex` Int64,
    `contractIndex` Int64,

    `ownerAddress` FixedString(34),
    `toAddress` FixedString(34),
    `amount` Int64,
)
ENGINE = ReplacingMergeTree
ORDER BY (transactionHash, contractIndex)
SETTINGS index_granularity = 8192;
";
pub const TRANSFER_ASSET_CONTRACT_DDL: &str = "
CREATE TABLE IF NOT EXISTS tron.transferAssetContracts
(
    `blockNum` Int64,
    `transactionHash` FixedString(32),
    `transactionIndex` Int64,
    `contractIndex` Int64,

    `assetName` String,
    `ownerAddress` FixedString(34),
    `toAddress` FixedString(34),
    `amount` Int64,
)
ENGINE = ReplacingMergeTree
ORDER BY (transactionHash, contractIndex)
SETTINGS index_granularity = 8192;
";
pub const FREEZE_BALANCE_CONTRACT_DDL: &str = "
CREATE TABLE IF NOT EXISTS tron.freezeBalanceContracts 
(
    `blockNum` Int64,
    `transactionHash` FixedString(32),
    `transactionIndex` Int64,
    `contractIndex` Int64,

    `ownerAddress` FixedString(34),
    `frozenBalance` Int64,
    `frozenDuration` Int64,
    `resource` Int32,
    `receiverAddress` FixedString(34),
)
ENGINE = ReplacingMergeTree
ORDER BY (transactionHash, contractIndex)
SETTINGS index_granularity = 8192;
";
pub const UNFREEZE_BALANCE_CONTRACT_DDL: &str = "
CREATE TABLE IF NOT EXISTS tron.unfreezeBalanceContracts
(
    `blockNum` Int64,
    `transactionHash` FixedString(32),
    `transactionIndex` Int64,
    `contractIndex` Int64,

    `ownerAddress` FixedString(34),
    `resource` Int32,
    `receiverAddress` FixedString(34),
)
ENGINE = ReplacingMergeTree
ORDER BY (transactionHash, contractIndex)
SETTINGS index_granularity = 8192;
";
pub const TRIGGER_SMART_CONTRACT_DDL: &str = "
CREATE TABLE IF NOT EXISTS tron.triggerSmartContracts
(
    `blockNum` Int64,
    `transactionHash` FixedString(32),
    `transactionIndex` Int64,
    `contractIndex` Int64,

    `ownerAddress` FixedString(34),
    `contractAddress` FixedString(34),
    `callValue` Int64,
    `data` String,
    `callTokenValue` Int64,
    `tokenId` Int64,
)
ENGINE = ReplacingMergeTree
ORDER BY (transactionHash, contractIndex)
SETTINGS index_granularity = 8192;
";
pub const FREEZE_BALANCE_V2_CONTRACT_DDL: &str = "
CREATE TABLE IF NOT EXISTS tron.freezeBalanceV2Contracts
(
    `blockNum` Int64,
    `transactionHash` FixedString(32),
    `transactionIndex` Int64,
    `contractIndex` Int64,

    `ownerAddress` FixedString(34),
    `frozenBalance` Int64,
    `resource` Int32,
)
ENGINE = ReplacingMergeTree
ORDER BY (transactionHash, contractIndex)
SETTINGS index_granularity = 8192;
";
pub const UNFREEZE_BALANCE_V2_CONTRACT_DDL: &str = "
CREATE TABLE IF NOT EXISTS tron.unfreezeBalanceV2Contracts
(
    `blockNum` Int64,
    `transactionHash` FixedString(32),
    `transactionIndex` Int64,
    `contractIndex` Int64,

    `ownerAddress` FixedString(34),
    `unfreezeBalance` Int64,
    `resource` Int32,
)
ENGINE = ReplacingMergeTree
ORDER BY (transactionHash, contractIndex)
SETTINGS index_granularity = 8192;
";
