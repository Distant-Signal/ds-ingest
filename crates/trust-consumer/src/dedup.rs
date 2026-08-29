use sha2::{Digest, Sha256};

/// Stable across Kafka redelivery of the exact same TRUST message (at-least-once
/// delivery means this WILL happen). Built from the fields that together
/// identify one real-world event -- not the whole message body, which may
/// carry a redelivery-specific envelope field this pass doesn't model.
/// Mirrors `crates/enricher/src/hash.rs`'s `text_hash` in shape and in the
/// null-byte separator rationale (prevents field-boundary collisions).
pub fn dedup_key(
    train_id: &str,
    msg_type: &str,
    event_type: Option<&str>,
    loc_stanox: Option<&str>,
    planned_timestamp: Option<&str>,
) -> String {
    let mut hasher = Sha256::new();
    for field in [train_id, msg_type, event_type.unwrap_or(""), loc_stanox.unwrap_or(""), planned_timestamp.unwrap_or("")] {
        hasher.update(field.as_bytes());
        hasher.update(b"\0");
    }
    let digest = hasher.finalize();
    digest.iter().map(|b| format!("{:02x}", b)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identical_inputs_hash_identically() {
        assert_eq!(
            dedup_key("221832406", "0003", Some("DEPARTURE"), Some("87701"), Some("1756400000000")),
            dedup_key("221832406", "0003", Some("DEPARTURE"), Some("87701"), Some("1756400000000")),
        );
    }

    #[test]
    fn a_different_event_type_at_the_same_location_hashes_differently() {
        let a = dedup_key("221832406", "0003", Some("ARRIVAL"), Some("87701"), Some("1756400000000"));
        let b = dedup_key("221832406", "0003", Some("DEPARTURE"), Some("87701"), Some("1756400000000"));
        assert_ne!(a, b);
    }

    #[test]
    fn a_different_location_hashes_differently() {
        let a = dedup_key("221832406", "0003", Some("PASS"), Some("87701"), None);
        let b = dedup_key("221832406", "0003", Some("PASS"), Some("11223"), None);
        assert_ne!(a, b);
    }

    #[test]
    fn the_separator_prevents_boundary_collisions() {
        assert_ne!(
            dedup_key("AB", "0003", None, None, None),
            dedup_key("A", "B0003", None, None, None),
        );
    }
}
