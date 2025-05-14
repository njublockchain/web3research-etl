use clickhouse::Row;
use serde_variant::to_variant_name;

#[derive(Row, Clone, Debug, Default)]
pub struct BlockRow {
}

#[derive(Row, Clone, Debug, Default)]
pub struct TransactionRow {
}
