use std::fmt::Write;


/// Converts bytes to appropriate format for Solana
pub fn bytes_to_solana_format(bytes: &[u8]) -> String {
    // Solana typically uses base58 for public keys and hex for other data
    // Here we're defaulting to base58, but this may need adjustment
    bs58::encode(bytes).into_string()
}

/// Convert u256 to string (hex representation)
pub fn u256_to_string(value: &[u8]) -> String {
    assert!(value.len() == 32);
    let mut result = String::with_capacity(64);
    for &byte in value {
        write!(&mut result, "{:02x}", byte).expect("Writing to string shouldn't fail");
    }
    result
}