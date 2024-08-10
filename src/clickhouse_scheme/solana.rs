use klickhouse::{u256, Bytes, Row};
use serde_variant::to_variant_name;

#[derive(Row, Clone, Debug, Default)]
#[klickhouse(rename_all = "camelCase")]
pub struct BlockRow {
}

#[derive(Row, Clone, Debug, Default)]
#[klickhouse(rename_all = "camelCase")]
pub struct TransactionRow {
}
