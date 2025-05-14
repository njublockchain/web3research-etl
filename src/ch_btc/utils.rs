pub fn bytes_to_btc_hex(bytes: &[u8]) -> String {
    // always use lowercase
    let hex_str = hex::encode(bytes);
    hex_str // btc style has no 0x prefix
}

