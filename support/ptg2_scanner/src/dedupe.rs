use crate::config::env_bool;
use crate::hashing::{
    hash_text_key, price_set_entry_key, provider_entry_component_key, provider_group_member_key,
    provider_set_component_key, provider_set_entry_key, shard_for_u128, shard_for_u64,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::io;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Mutex,
};

struct ShardedDedupe64 {
    shards: Vec<Mutex<HashSet<u64>>>,
}

impl ShardedDedupe64 {
    fn new(shard_count: usize) -> Self {
        let count = shard_count.max(1);
        Self {
            shards: (0..count).map(|_| Mutex::new(HashSet::new())).collect(),
        }
    }

    fn insert(&self, key: u64) -> bool {
        let shard_index = shard_for_u64(key, self.shards.len());
        let mut shard = self.shards[shard_index].lock().unwrap();
        shard.insert(key)
    }

    fn insert_hash_text(&self, key: &str) -> bool {
        self.insert(hash_text_key(key))
    }
}

struct ShardedDedupe128 {
    shards: Vec<Mutex<HashSet<u128>>>,
}

impl ShardedDedupe128 {
    fn new(shard_count: usize) -> Self {
        let count = shard_count.max(1);
        Self {
            shards: (0..count).map(|_| Mutex::new(HashSet::new())).collect(),
        }
    }

    fn insert(&self, key: u128) -> bool {
        let shard_index = shard_for_u128(key, self.shards.len());
        let mut shard = self.shards[shard_index].lock().unwrap();
        shard.insert(key)
    }
}

struct DedupeCounter {
    attempted: AtomicU64,
    unique: AtomicU64,
}

const PROVIDER_IDENTIFIER_QUARANTINE_CONTRACT: &str = "ptg2_provider_identifier_quarantine_v1";
const PROVIDER_IDENTIFIER_QUARANTINE_HASH_DOMAIN: &[u8] =
    b"PTG2_PROVIDER_IDENTIFIER_QUARANTINE_V1\0";
const MAX_QUARANTINED_PROVIDER_IDENTIFIERS: usize = 1024;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ProviderIdentifierQuarantine {
    occurrences_by_value: BTreeMap<i64, u64>,
}

impl ProviderIdentifierQuarantine {
    fn digest_hex(digest: &[u8]) -> String {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let mut encoded = String::with_capacity(digest.len() * 2);
        for byte in digest {
            encoded.push(HEX[(byte >> 4) as usize] as char);
            encoded.push(HEX[(byte & 0x0f) as usize] as char);
        }
        encoded
    }

    pub fn record(&mut self, values: &[i64]) -> io::Result<()> {
        for value in values {
            if *value == 0 || (1_000_000_000..=9_999_999_999).contains(value) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "provider identifier quarantine contains a non-malformed value",
                ));
            }
            if !self.occurrences_by_value.contains_key(value)
                && self.occurrences_by_value.len() >= MAX_QUARANTINED_PROVIDER_IDENTIFIERS
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "provider identifier quarantine exceeds 1024 distinct values",
                ));
            }
            let count = self.occurrences_by_value.entry(*value).or_default();
            *count = count.checked_add(1).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "provider identifier quarantine occurrence count overflow",
                )
            })?;
        }
        Ok(())
    }

    pub fn merge(&mut self, other: &Self) -> io::Result<()> {
        for (value, count) in &other.occurrences_by_value {
            if !self.occurrences_by_value.contains_key(value)
                && self.occurrences_by_value.len() >= MAX_QUARANTINED_PROVIDER_IDENTIFIERS
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "provider identifier quarantine exceeds 1024 distinct values",
                ));
            }
            let current = self.occurrences_by_value.entry(*value).or_default();
            *current = current.checked_add(*count).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "provider identifier quarantine occurrence count overflow",
                )
            })?;
        }
        Ok(())
    }

    pub fn payload(&self) -> io::Result<Value> {
        let mut digest = Sha256::new();
        digest.update(PROVIDER_IDENTIFIER_QUARANTINE_HASH_DOMAIN);
        let mut occurrence_count = 0u64;
        let entries = self
            .occurrences_by_value
            .iter()
            .map(|(value, count)| -> io::Result<Value> {
                occurrence_count = occurrence_count.checked_add(*count).ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        "provider identifier quarantine occurrence count overflow",
                    )
                })?;
                digest.update(value.to_string().as_bytes());
                digest.update([0]);
                digest.update(count.to_be_bytes());
                Ok(json!({
                    "value": value.to_string(),
                    "occurrence_count": count,
                }))
            })
            .collect::<io::Result<Vec<_>>>()?;
        Ok(json!({
            "contract": PROVIDER_IDENTIFIER_QUARANTINE_CONTRACT,
            "occurrence_count": occurrence_count,
            "distinct_value_count": entries.len(),
            "entries": entries,
            "sha256": Self::digest_hex(&digest.finalize()),
        }))
    }
}

impl DedupeCounter {
    fn new() -> Self {
        Self {
            attempted: AtomicU64::new(0),
            unique: AtomicU64::new(0),
        }
    }

    fn record(&self, inserted: bool) {
        self.attempted.fetch_add(1, Ordering::Relaxed);
        if inserted {
            self.unique.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn snapshot(&self) -> (u64, u64, u64) {
        let attempted = self.attempted.load(Ordering::Relaxed);
        let unique = self.unique.load(Ordering::Relaxed);
        let duplicate = attempted.saturating_sub(unique);
        (attempted, unique, duplicate)
    }
}

pub struct SharedDedupe {
    serving_rate: ShardedDedupe64,
    procedure: ShardedDedupe64,
    price_code_set: ShardedDedupe64,
    price_atom: ShardedDedupe64,
    price_set: ShardedDedupe64,
    price_set_entry: Option<ShardedDedupe128>,
    provider_set: ShardedDedupe64,
    provider_set_component: ShardedDedupe128,
    provider_set_entry: ShardedDedupe128,
    provider_entry_component: Option<ShardedDedupe128>,
    provider_group: ShardedDedupe64,
    provider_group_member: ShardedDedupe128,
    dedupe_high_cardinality_entries: bool,
    serving_rate_counter: DedupeCounter,
    procedure_counter: DedupeCounter,
    price_atom_counter: DedupeCounter,
    price_set_counter: DedupeCounter,
    price_set_entry_counter: DedupeCounter,
    provider_set_counter: DedupeCounter,
    provider_set_component_counter: DedupeCounter,
    provider_set_entry_counter: DedupeCounter,
    provider_entry_component_counter: DedupeCounter,
    provider_group_counter: DedupeCounter,
    provider_group_member_counter: DedupeCounter,
    provider_identifier_quarantine: Mutex<ProviderIdentifierQuarantine>,
}

impl SharedDedupe {
    pub fn new(worker_count: usize) -> Self {
        let shard_count = (worker_count.max(1) * 4).max(16);
        let dedupe_high_cardinality_entries =
            env_bool("HLTHPRT_PTG2_RUST_DEDUPE_HIGH_CARDINALITY_ENTRIES", false);
        Self {
            serving_rate: ShardedDedupe64::new(shard_count),
            procedure: ShardedDedupe64::new(shard_count),
            price_code_set: ShardedDedupe64::new(shard_count),
            price_atom: ShardedDedupe64::new(shard_count),
            price_set: ShardedDedupe64::new(shard_count),
            price_set_entry: dedupe_high_cardinality_entries
                .then(|| ShardedDedupe128::new(shard_count)),
            provider_set: ShardedDedupe64::new(shard_count),
            provider_set_component: ShardedDedupe128::new(shard_count),
            provider_set_entry: ShardedDedupe128::new(shard_count),
            provider_entry_component: dedupe_high_cardinality_entries
                .then(|| ShardedDedupe128::new(shard_count)),
            provider_group: ShardedDedupe64::new(shard_count),
            provider_group_member: ShardedDedupe128::new(shard_count),
            dedupe_high_cardinality_entries,
            serving_rate_counter: DedupeCounter::new(),
            procedure_counter: DedupeCounter::new(),
            price_atom_counter: DedupeCounter::new(),
            price_set_counter: DedupeCounter::new(),
            price_set_entry_counter: DedupeCounter::new(),
            provider_set_counter: DedupeCounter::new(),
            provider_set_component_counter: DedupeCounter::new(),
            provider_set_entry_counter: DedupeCounter::new(),
            provider_entry_component_counter: DedupeCounter::new(),
            provider_group_counter: DedupeCounter::new(),
            provider_group_member_counter: DedupeCounter::new(),
            provider_identifier_quarantine: Mutex::new(ProviderIdentifierQuarantine::default()),
        }
    }

    pub fn insert_serving_rate(&self, key: &str) -> bool {
        let inserted = self.serving_rate.insert_hash_text(key);
        self.serving_rate_counter.record(inserted);
        inserted
    }

    pub fn insert_procedure(&self, key: &str) -> bool {
        let inserted = self.procedure.insert_hash_text(key);
        self.procedure_counter.record(inserted);
        inserted
    }

    pub fn insert_price_set(&self, key: &str) -> bool {
        let inserted = self.price_set.insert_hash_text(key);
        self.price_set_counter.record(inserted);
        inserted
    }

    pub fn insert_price_atom(&self, key: &str) -> bool {
        let inserted = self.price_atom.insert_hash_text(key);
        self.price_atom_counter.record(inserted);
        inserted
    }

    pub fn insert_price_code_set(&self, key: &str) -> bool {
        self.price_code_set.insert_hash_text(key)
    }

    pub fn insert_price_set_entry(&self, price_set_hash: &str, price_atom_hash: &str) -> bool {
        let inserted = match &self.price_set_entry {
            Some(dedupe) => dedupe.insert(price_set_entry_key(price_set_hash, price_atom_hash)),
            None => true,
        };
        self.price_set_entry_counter.record(inserted);
        inserted
    }

    pub fn insert_provider_set(&self, key: &str) -> bool {
        let inserted = self.provider_set.insert_hash_text(key);
        self.provider_set_counter.record(inserted);
        inserted
    }

    pub fn insert_provider_set_component(
        &self,
        provider_set_hash: &str,
        provider_group_hash: i64,
    ) -> bool {
        let inserted = self
            .provider_set_component
            .insert(provider_set_component_key(
                provider_set_hash,
                provider_group_hash,
            ));
        self.provider_set_component_counter.record(inserted);
        inserted
    }

    pub fn insert_provider_set_entry(
        &self,
        provider_set_hash: &str,
        provider_entry_hash: i64,
    ) -> bool {
        let inserted = self.provider_set_entry.insert(provider_set_entry_key(
            provider_set_hash,
            provider_entry_hash,
        ));
        self.provider_set_entry_counter.record(inserted);
        inserted
    }

    pub fn insert_provider_entry_component(
        &self,
        provider_entry_hash: i64,
        provider_group_hash: i64,
    ) -> bool {
        let inserted = match &self.provider_entry_component {
            Some(dedupe) => dedupe.insert(provider_entry_component_key(
                provider_entry_hash,
                provider_group_hash,
            )),
            None => true,
        };
        self.provider_entry_component_counter.record(inserted);
        inserted
    }

    pub fn insert_provider_group(&self, group_hash: i64) -> bool {
        let inserted = self.provider_group.insert(group_hash as u64);
        self.provider_group_counter.record(inserted);
        inserted
    }

    pub fn insert_provider_group_member(&self, group_hash: i64, npi: i64) -> bool {
        let key = provider_group_member_key(group_hash, npi);
        let inserted = self.provider_group_member.insert(key);
        self.provider_group_member_counter.record(inserted);
        inserted
    }

    pub fn record_quarantined_provider_identifiers(&self, values: &[i64]) -> io::Result<()> {
        self.provider_identifier_quarantine
            .lock()
            .map_err(|_| io::Error::other("provider identifier quarantine lock poisoned"))?
            .record(values)
    }

    pub fn provider_identifier_quarantine(&self) -> io::Result<ProviderIdentifierQuarantine> {
        self.provider_identifier_quarantine
            .lock()
            .map_err(|_| io::Error::other("provider identifier quarantine lock poisoned"))
            .map(|quarantine| quarantine.clone())
    }
}

fn dedupe_reduction_pct(attempted: u64, duplicate: u64) -> f64 {
    if attempted == 0 {
        0.0
    } else {
        (duplicate as f64 / attempted as f64) * 100.0
    }
}

pub fn dedupe_summary_payload(
    dedupe: &SharedDedupe,
    object_counts: &HashMap<String, u64>,
) -> Value {
    let negotiated_rates = object_counts.get("negotiated_rates").copied().unwrap_or(0);
    let (serving_attempted, serving_unique, serving_duplicate) =
        dedupe.serving_rate_counter.snapshot();
    let (procedure_attempted, procedure_unique, procedure_duplicate) =
        dedupe.procedure_counter.snapshot();
    let (price_atom_attempted, price_atom_unique, price_atom_duplicate) =
        dedupe.price_atom_counter.snapshot();
    let (price_attempted, price_unique, price_duplicate) = dedupe.price_set_counter.snapshot();
    let (price_set_entry_attempted, price_set_entry_unique, price_set_entry_duplicate) =
        dedupe.price_set_entry_counter.snapshot();
    let (provider_attempted, provider_unique, provider_duplicate) =
        dedupe.provider_set_counter.snapshot();
    let (pse_attempted, pse_unique, pse_duplicate) = dedupe.provider_set_entry_counter.snapshot();
    let (pec_attempted, pec_unique, pec_duplicate) =
        dedupe.provider_entry_component_counter.snapshot();
    let (pg_attempted, pg_unique, pg_duplicate) = dedupe.provider_group_counter.snapshot();
    let (pgm_attempted, pgm_unique, pgm_duplicate) =
        dedupe.provider_group_member_counter.snapshot();
    let mut payload = json!({
        "negotiated_rates": negotiated_rates,
        "serving_rate_attempted": serving_attempted,
        "serving_rate_unique": serving_unique,
        "serving_rate_duplicate": serving_duplicate,
        "serving_rate_reduction_pct": dedupe_reduction_pct(serving_attempted, serving_duplicate),
        "procedure_attempted": procedure_attempted,
        "procedure_unique": procedure_unique,
        "procedure_duplicate": procedure_duplicate,
        "procedure_reduction_pct": dedupe_reduction_pct(procedure_attempted, procedure_duplicate),
        "price_atom_attempted": price_atom_attempted,
        "price_atom_unique": price_atom_unique,
        "price_atom_duplicate": price_atom_duplicate,
        "price_atom_reduction_pct": dedupe_reduction_pct(price_atom_attempted, price_atom_duplicate),
        "price_set_attempted": price_attempted,
        "price_set_unique": price_unique,
        "price_set_duplicate": price_duplicate,
        "price_set_reduction_pct": dedupe_reduction_pct(price_attempted, price_duplicate),
        "price_set_entry_attempted": price_set_entry_attempted,
        "price_set_entry_unique": price_set_entry_unique,
        "price_set_entry_duplicate": price_set_entry_duplicate,
        "price_set_entry_reduction_pct": dedupe_reduction_pct(price_set_entry_attempted, price_set_entry_duplicate),
        "price_set_entry_dedupe_enabled": dedupe.dedupe_high_cardinality_entries,
        "provider_set_attempted": provider_attempted,
        "provider_set_unique": provider_unique,
        "provider_set_duplicate": provider_duplicate,
        "provider_set_reduction_pct": dedupe_reduction_pct(provider_attempted, provider_duplicate),
        "provider_set_entry_attempted": pse_attempted,
        "provider_set_entry_unique": pse_unique,
        "provider_set_entry_duplicate": pse_duplicate,
        "provider_set_entry_reduction_pct": dedupe_reduction_pct(pse_attempted, pse_duplicate),
        "provider_entry_component_attempted": pec_attempted,
        "provider_entry_component_unique": pec_unique,
        "provider_entry_component_duplicate": pec_duplicate,
        "provider_entry_component_reduction_pct": dedupe_reduction_pct(pec_attempted, pec_duplicate),
        "provider_entry_component_dedupe_enabled": dedupe.dedupe_high_cardinality_entries,
        "provider_group_member_attempted": pgm_attempted,
        "provider_group_member_unique": pgm_unique,
        "provider_group_member_duplicate": pgm_duplicate,
        "provider_group_member_reduction_pct": dedupe_reduction_pct(pgm_attempted, pgm_duplicate),
    });
    if let Some(payload_map) = payload.as_object_mut() {
        payload_map.insert("provider_group_attempted".to_string(), json!(pg_attempted));
        payload_map.insert("provider_group_unique".to_string(), json!(pg_unique));
        payload_map.insert("provider_group_duplicate".to_string(), json!(pg_duplicate));
        payload_map.insert(
            "provider_group_reduction_pct".to_string(),
            json!(dedupe_reduction_pct(pg_attempted, pg_duplicate)),
        );
    }
    payload
}

pub fn emit_dedupe_summary(dedupe: &SharedDedupe, object_counts: &HashMap<String, u64>) {
    let payload = dedupe_summary_payload(dedupe, object_counts);
    eprintln!(
        "PTG2_DEDUPE_SUMMARY\tnegotiated_rates={}\tserving_rate_attempted={}\tserving_rate_unique={}\tserving_rate_duplicate={}\tserving_rate_reduction_pct={:.2}\tprocedure_attempted={}\tprocedure_unique={}\tprocedure_duplicate={}\tprocedure_reduction_pct={:.2}\tprice_atom_attempted={}\tprice_atom_unique={}\tprice_atom_duplicate={}\tprice_atom_reduction_pct={:.2}\tprice_set_attempted={}\tprice_set_unique={}\tprice_set_duplicate={}\tprice_set_reduction_pct={:.2}\tprice_set_entry_attempted={}\tprice_set_entry_unique={}\tprice_set_entry_duplicate={}\tprice_set_entry_reduction_pct={:.2}\tprovider_set_attempted={}\tprovider_set_unique={}\tprovider_set_duplicate={}\tprovider_set_reduction_pct={:.2}\tprovider_set_entry_attempted={}\tprovider_set_entry_unique={}\tprovider_set_entry_duplicate={}\tprovider_set_entry_reduction_pct={:.2}\tprovider_entry_component_attempted={}\tprovider_entry_component_unique={}\tprovider_entry_component_duplicate={}\tprovider_entry_component_reduction_pct={:.2}\tprovider_group_member_attempted={}\tprovider_group_member_unique={}\tprovider_group_member_duplicate={}\tprovider_group_member_reduction_pct={:.2}",
        payload.get("negotiated_rates").and_then(Value::as_u64).unwrap_or(0),
        payload.get("serving_rate_attempted").and_then(Value::as_u64).unwrap_or(0),
        payload.get("serving_rate_unique").and_then(Value::as_u64).unwrap_or(0),
        payload.get("serving_rate_duplicate").and_then(Value::as_u64).unwrap_or(0),
        payload.get("serving_rate_reduction_pct").and_then(Value::as_f64).unwrap_or(0.0),
        payload.get("procedure_attempted").and_then(Value::as_u64).unwrap_or(0),
        payload.get("procedure_unique").and_then(Value::as_u64).unwrap_or(0),
        payload.get("procedure_duplicate").and_then(Value::as_u64).unwrap_or(0),
        payload.get("procedure_reduction_pct").and_then(Value::as_f64).unwrap_or(0.0),
        payload.get("price_atom_attempted").and_then(Value::as_u64).unwrap_or(0),
        payload.get("price_atom_unique").and_then(Value::as_u64).unwrap_or(0),
        payload.get("price_atom_duplicate").and_then(Value::as_u64).unwrap_or(0),
        payload.get("price_atom_reduction_pct").and_then(Value::as_f64).unwrap_or(0.0),
        payload.get("price_set_attempted").and_then(Value::as_u64).unwrap_or(0),
        payload.get("price_set_unique").and_then(Value::as_u64).unwrap_or(0),
        payload.get("price_set_duplicate").and_then(Value::as_u64).unwrap_or(0),
        payload.get("price_set_reduction_pct").and_then(Value::as_f64).unwrap_or(0.0),
        payload.get("price_set_entry_attempted").and_then(Value::as_u64).unwrap_or(0),
        payload.get("price_set_entry_unique").and_then(Value::as_u64).unwrap_or(0),
        payload.get("price_set_entry_duplicate").and_then(Value::as_u64).unwrap_or(0),
        payload.get("price_set_entry_reduction_pct").and_then(Value::as_f64).unwrap_or(0.0),
        payload.get("provider_set_attempted").and_then(Value::as_u64).unwrap_or(0),
        payload.get("provider_set_unique").and_then(Value::as_u64).unwrap_or(0),
        payload.get("provider_set_duplicate").and_then(Value::as_u64).unwrap_or(0),
        payload.get("provider_set_reduction_pct").and_then(Value::as_f64).unwrap_or(0.0),
        payload.get("provider_set_entry_attempted").and_then(Value::as_u64).unwrap_or(0),
        payload.get("provider_set_entry_unique").and_then(Value::as_u64).unwrap_or(0),
        payload.get("provider_set_entry_duplicate").and_then(Value::as_u64).unwrap_or(0),
        payload.get("provider_set_entry_reduction_pct").and_then(Value::as_f64).unwrap_or(0.0),
        payload.get("provider_entry_component_attempted").and_then(Value::as_u64).unwrap_or(0),
        payload.get("provider_entry_component_unique").and_then(Value::as_u64).unwrap_or(0),
        payload.get("provider_entry_component_duplicate").and_then(Value::as_u64).unwrap_or(0),
        payload.get("provider_entry_component_reduction_pct").and_then(Value::as_f64).unwrap_or(0.0),
        payload.get("provider_group_member_attempted").and_then(Value::as_u64).unwrap_or(0),
        payload.get("provider_group_member_unique").and_then(Value::as_u64).unwrap_or(0),
        payload.get("provider_group_member_duplicate").and_then(Value::as_u64).unwrap_or(0),
        payload.get("provider_group_member_reduction_pct").and_then(Value::as_f64).unwrap_or(0.0),
    );
}

#[cfg(test)]
mod tests {
    use super::{dedupe_summary_payload, SharedDedupe};
    use std::collections::HashMap;

    #[test]
    fn shared_dedupe_counts_serving_rate_duplicates() {
        let dedupe = SharedDedupe::new(1);
        let mut object_counts = HashMap::new();
        object_counts.insert("negotiated_rates".to_string(), 7);

        assert!(dedupe.insert_serving_rate("rate-1"));
        assert!(!dedupe.insert_serving_rate("rate-1"));

        let payload = dedupe_summary_payload(&dedupe, &object_counts);
        assert_eq!(payload["negotiated_rates"], 7);
        assert_eq!(payload["serving_rate_attempted"], 2);
        assert_eq!(payload["serving_rate_unique"], 1);
        assert_eq!(payload["serving_rate_duplicate"], 1);
        assert_eq!(payload["serving_rate_reduction_pct"], 50.0);
    }

    #[test]
    fn shared_dedupe_counts_provider_group_member_duplicates() {
        let dedupe = SharedDedupe::new(2);
        let object_counts = HashMap::new();

        assert!(dedupe.insert_provider_group(100));
        assert!(!dedupe.insert_provider_group(100));
        assert!(dedupe.insert_provider_group_member(100, 1234567890));
        assert!(!dedupe.insert_provider_group_member(100, 1234567890));
        assert!(dedupe.insert_provider_group_member(100, 9876543210));

        let payload = dedupe_summary_payload(&dedupe, &object_counts);
        assert_eq!(payload["provider_group_attempted"], 2);
        assert_eq!(payload["provider_group_unique"], 1);
        assert_eq!(payload["provider_group_duplicate"], 1);
        assert_eq!(payload["provider_group_member_attempted"], 3);
        assert_eq!(payload["provider_group_member_unique"], 2);
        assert_eq!(payload["provider_group_member_duplicate"], 1);
    }
}
