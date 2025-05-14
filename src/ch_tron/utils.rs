use log::warn;

/// Converts bytes to either base58 or hex according to context (for Tron)
pub fn bytes_to_tron_format(bytes: &[u8], is_address: bool) -> String {
    if is_address {
        // For addresses, prepend 0x41 and then convert to base58check
        if bytes.len() == 20 {
            // For Tron addresses that are just the 20 bytes (without prefix), add the prefix and encode
            let mut prefixed = vec![0x41];
            prefixed.extend_from_slice(bytes);
            return bs58::encode(&prefixed).with_check().into_string();
        }
        if bytes.len() == 21 {
            // For Tron addresses that are already prefixed, just encode
            assert!(
                bytes[0] == 0x41,
                "21-length TRON address should start with 41: {:X?}",
                bytes
            );
            return bs58::encode(bytes).with_check().into_string();
        }

        panic!(
            "Invalid address length: {}. Expected 20 or 21 bytes.",
            bytes.len()
        );
    } else {
        // For non-addresses, use hex
        hex::encode(bytes)
    }
}

/// always use T-prefix base58check encoding for TRON addresses
pub fn format_tron_address(any_addr: Vec<u8>) -> String {
    // consider a EVM address
    if any_addr.len() == 32 {
        assert!(
            any_addr.starts_with(&[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]),
            "32-length EVM address should start with 12*00: {:X?}",
            any_addr
        ); // 12 bytes prefix + 20 bytes addr
        // Use base58 encoding with 0x41 prefix for TRON addresses
        let mut prefixed = vec![0x41];
        prefixed.extend_from_slice(&any_addr[12..]);
        return bs58::encode(&prefixed).with_check().into_string();
    }

    // consider a TRON address
    if any_addr.len() == 21 {
        assert!(
            any_addr.starts_with(&[0x41]),
            "21-length TRON address should start with 41: {:X?}",
            any_addr
        );
        // Use base58 encoding with 0x41 prefix for TRON addresses
        return bs58::encode(&any_addr).with_check().into_string();
    }

    // consider a ETH address
    if any_addr.len() == 20 {
        // For Tron addresses that are just the 20 bytes (without prefix), add the prefix and encode
        let mut prefixed = vec![0x41];
        prefixed.extend_from_slice(&any_addr);
        return bs58::encode(&prefixed).with_check().into_string();
    }

    if any_addr.starts_with(&[0x41]) && any_addr.len() == 21 {
        return bs58::encode(&any_addr).with_check().into_string();
    }

    // if null
    if any_addr.is_empty() {
        return "".to_string();
    }

    // fallback
    warn!(
        "Address length {} is not considered yet! {:X?}",
        any_addr.len(),
        any_addr
    );
    return hex::encode(any_addr);
}
