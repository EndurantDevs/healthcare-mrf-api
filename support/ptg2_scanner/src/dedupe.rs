use crate::config::env_bool;
use crate::hashing::{
    hash_text_key, provider_entry_component_key, provider_group_member_key,
    provider_set_component_key, provider_set_entry_key, shard_for_u128, shard_for_u64,
};
use crate::manifest::GlobalId128;
use crate::tax_identity::{
    TaxIdentityObservation, TaxIdentityObservationV2, TaxIdentityState, TaxIdentityStateV2,
    TinTokenPolicy,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::io;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Mutex,
};
use xxhash_rust::xxh3::Xxh3;

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

struct ShardedProviderGroupTaxIdentity {
    policy: TinTokenPolicy,
    shards: Vec<Mutex<HashMap<u64, TaxIdentityObservation>>>,
}

struct ShardedProviderGroupTaxIdentityPairs {
    policy: TinTokenPolicy,
    shards: Vec<Mutex<HashMap<u64, PairedTaxIdentityObservation>>>,
}

enum ProviderGroupTaxIdentityDedupe {
    V1(ShardedProviderGroupTaxIdentity),
    Paired(ShardedProviderGroupTaxIdentityPairs),
}

const PROVIDER_GROUP_TAX_IDENTITY_SHARDS: usize = 256;
const INVALID_PAIRED_TAX_IDENTITY: &str =
    "provider group has invalid paired tax identity observation";

#[derive(Clone, Copy, Eq, PartialEq)]
struct PairedTaxIdentityObservation {
    v1: TaxIdentityObservation,
    v2: TaxIdentityObservationV2,
}

impl PairedTaxIdentityObservation {
    fn observe(policy: &TinTokenPolicy, tin: Option<&Value>) -> io::Result<Self> {
        let v1 = policy.observe(tin);
        let v2 = if v1.state == TaxIdentityState::MatchedEin {
            TaxIdentityObservationV2 {
                state: TaxIdentityStateV2::MatchedEin,
                tin_hmac_sha256: v1.tin_hmac_sha256,
            }
        } else {
            policy.observe_v2(tin)
        };
        let observation = Self { v1, v2 };
        observation.validate()?;
        Ok(observation)
    }

    fn merge(self, other: Self) -> io::Result<Self> {
        self.validate()?;
        other.validate()?;
        let next_v1 = self.v1.merge(other.v1)?;
        let next_v2 = self.v2.merge(other.v2)?;
        let merged = Self {
            v1: next_v1,
            v2: next_v2,
        };
        merged.validate()?;
        Ok(merged)
    }

    fn validate(self) -> io::Result<()> {
        let v1_shape_is_valid = match (self.v1.state, self.v1.tin_hmac_sha256) {
            (TaxIdentityState::MatchedEin, Some(hmac)) => hmac != [0; 32],
            (
                TaxIdentityState::Missing
                | TaxIdentityState::Malformed
                | TaxIdentityState::UnsupportedType,
                None,
            ) => true,
            _ => false,
        };
        let v2_shape_is_valid = match (self.v2.state, self.v2.tin_hmac_sha256) {
            (TaxIdentityStateV2::MatchedEin | TaxIdentityStateV2::MatchedNpi, Some(hmac)) => {
                hmac != [0; 32]
            }
            (
                TaxIdentityStateV2::Missing
                | TaxIdentityStateV2::Malformed
                | TaxIdentityStateV2::UnsupportedType,
                None,
            ) => true,
            _ => false,
        };
        let versions_are_compatible = match (self.v1.state, self.v2.state) {
            (TaxIdentityState::MatchedEin, TaxIdentityStateV2::MatchedEin) => {
                self.v1.tin_hmac_sha256 == self.v2.tin_hmac_sha256
            }
            (
                TaxIdentityState::UnsupportedType,
                TaxIdentityStateV2::MatchedNpi
                | TaxIdentityStateV2::Malformed
                | TaxIdentityStateV2::UnsupportedType,
            )
            | (TaxIdentityState::Missing, TaxIdentityStateV2::Missing)
            | (TaxIdentityState::Malformed, TaxIdentityStateV2::Malformed) => true,
            _ => false,
        };
        if v1_shape_is_valid && v2_shape_is_valid && versions_are_compatible {
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                INVALID_PAIRED_TAX_IDENTITY,
            ))
        }
    }
}

impl ShardedProviderGroupTaxIdentity {
    fn new(policy: TinTokenPolicy) -> Self {
        Self {
            policy,
            shards: (0..PROVIDER_GROUP_TAX_IDENTITY_SHARDS)
                .map(|_| Mutex::new(HashMap::new()))
                .collect(),
        }
    }

    fn record(&self, group_hash: i64, tin: Option<&Value>) -> io::Result<()> {
        let key = group_hash as u64;
        let group_id = provider_group_global_id_from_hash(group_hash);
        let shard_index = usize::from(group_id.0[0]);
        let mut shard = self.shards[shard_index]
            .lock()
            .map_err(|_| io::Error::other("provider tax identity lock poisoned"))?;
        let observation = self.policy.observe(tin);
        match shard.get_mut(&key) {
            Some(current) => *current = current.merge(observation)?,
            None => {
                shard.insert(key, observation);
            }
        }
        Ok(())
    }

    fn visit_sorted(
        &self,
        mut visitor: impl FnMut(i64, TaxIdentityObservation) -> io::Result<()>,
    ) -> io::Result<()> {
        for shard in &self.shards {
            let mut rows = shard
                .lock()
                .map_err(|_| io::Error::other("provider tax identity lock poisoned"))?
                .iter()
                .map(|(group_hash, observation)| {
                    let signed_hash = *group_hash as i64;
                    (
                        provider_group_global_id_from_hash(signed_hash),
                        signed_hash,
                        *observation,
                    )
                })
                .collect::<Vec<_>>();
            rows.sort_unstable_by_key(|(group_id, _group_hash, _observation)| *group_id);
            for (_group_id, group_hash, observation) in rows {
                visitor(group_hash, observation)?;
            }
        }
        Ok(())
    }
}

impl ShardedProviderGroupTaxIdentityPairs {
    fn new(policy: TinTokenPolicy) -> Self {
        Self {
            policy,
            shards: (0..PROVIDER_GROUP_TAX_IDENTITY_SHARDS)
                .map(|_| Mutex::new(HashMap::new()))
                .collect(),
        }
    }

    fn record(&self, group_hash: i64, tin: Option<&Value>) -> io::Result<()> {
        let key = group_hash as u64;
        let group_id = provider_group_global_id_from_hash(group_hash);
        let shard_index = usize::from(group_id.0[0]);
        let observation = PairedTaxIdentityObservation::observe(&self.policy, tin)?;
        let mut shard = self.shards[shard_index]
            .lock()
            .map_err(|_| io::Error::other("provider tax identity lock poisoned"))?;
        match shard.get_mut(&key) {
            Some(current) => {
                let merged = current.merge(observation)?;
                *current = merged;
            }
            None => {
                shard.insert(key, observation);
            }
        }
        Ok(())
    }

    fn visit_sorted_pairs(
        &self,
        mut visitor: impl FnMut(i64, TaxIdentityObservation, TaxIdentityObservationV2) -> io::Result<()>,
    ) -> io::Result<()> {
        // Shards follow the first global-ID byte, so ascending shard visits plus
        // each shard's local sort produce one global lexicographic ordering.
        for shard in &self.shards {
            let mut rows = shard
                .lock()
                .map_err(|_| io::Error::other("provider tax identity lock poisoned"))?
                .iter()
                .map(|(group_hash, observation)| {
                    let signed_hash = *group_hash as i64;
                    (
                        provider_group_global_id_from_hash(signed_hash),
                        signed_hash,
                        *observation,
                    )
                })
                .collect::<Vec<_>>();
            rows.sort_unstable_by_key(|(group_id, _group_hash, _observation)| *group_id);
            for (_group_id, group_hash, observation) in rows {
                observation.validate()?;
                visitor(group_hash, observation.v1, observation.v2)?;
            }
        }
        Ok(())
    }
}

impl ProviderGroupTaxIdentityDedupe {
    fn record(&self, group_hash: i64, tin: Option<&Value>) -> io::Result<()> {
        match self {
            Self::V1(identity) => identity.record(group_hash, tin),
            Self::Paired(identity) => identity.record(group_hash, tin),
        }
    }

    fn policy_id(&self) -> &str {
        match self {
            Self::V1(identity) => identity.policy.policy_id(),
            Self::Paired(identity) => identity.policy.policy_id(),
        }
    }

    fn visit_sorted(
        &self,
        visitor: impl FnMut(i64, TaxIdentityObservation) -> io::Result<()>,
    ) -> io::Result<()> {
        match self {
            Self::V1(identity) => identity.visit_sorted(visitor),
            Self::Paired(identity) => {
                let mut visitor = visitor;
                identity.visit_sorted_pairs(|group_hash, v1, _v2| visitor(group_hash, v1))
            }
        }
    }

    fn visit_sorted_pairs(
        &self,
        visitor: impl FnMut(i64, TaxIdentityObservation, TaxIdentityObservationV2) -> io::Result<()>,
    ) -> io::Result<()> {
        match self {
            Self::V1(_) => Err(io::Error::other(
                "paired provider tax identity output is not configured",
            )),
            Self::Paired(identity) => identity.visit_sorted_pairs(visitor),
        }
    }
}

pub fn provider_group_global_id_from_hash(provider_group_hash: i64) -> GlobalId128 {
    let hash_text = provider_group_hash.to_string();
    GlobalId128::from_parts("provider_group_manifest", &[&hash_text])
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

    fn record_attempted(&self, count: u64) {
        self.attempted.fetch_add(count, Ordering::Relaxed);
    }

    fn snapshot(&self) -> (u64, u64, u64) {
        let attempted = self.attempted.load(Ordering::Relaxed);
        let unique = self.unique.load(Ordering::Relaxed);
        let duplicate = attempted.saturating_sub(unique);
        (attempted, unique, duplicate)
    }
}

pub struct SharedDedupe {
    serving_rate: Option<ShardedDedupe64>,
    procedure: ShardedDedupe64,
    price_code_set: ShardedDedupe64,
    price_atom: ShardedDedupe128,
    price_set: ShardedDedupe128,
    price_set_entry: Option<ShardedDedupe128>,
    provider_set: ShardedDedupe128,
    provider_set_component: ShardedDedupe128,
    provider_set_entry: ShardedDedupe128,
    provider_entry_component: Option<ShardedDedupe128>,
    provider_group: ShardedDedupe64,
    provider_group_tax_identity: Option<ProviderGroupTaxIdentityDedupe>,
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
    empty_npi_tin_only_normalization_count: AtomicU64,
}

impl SharedDedupe {
    pub fn new(worker_count: usize) -> Self {
        Self::new_with_serving_rate_dedupe(worker_count, true)
    }

    pub fn new_with_serving_rate_dedupe(
        worker_count: usize,
        serving_rate_dedupe_enabled: bool,
    ) -> Self {
        Self::new_with_optional_tax_identity(worker_count, serving_rate_dedupe_enabled, None)
    }

    pub fn new_with_v4_tax_identity(
        worker_count: usize,
        serving_rate_dedupe_enabled: bool,
        policy: TinTokenPolicy,
    ) -> Self {
        Self::new_with_optional_tax_identity(
            worker_count,
            serving_rate_dedupe_enabled,
            Some(ProviderGroupTaxIdentityDedupe::V1(
                ShardedProviderGroupTaxIdentity::new(policy),
            )),
        )
    }

    /// Enables the opt-in atomic v1/v2 collector without activating a writer.
    pub fn new_with_v4_paired_tax_identity(
        worker_count: usize,
        serving_rate_dedupe_enabled: bool,
        policy: TinTokenPolicy,
    ) -> Self {
        Self::new_with_optional_tax_identity(
            worker_count,
            serving_rate_dedupe_enabled,
            Some(ProviderGroupTaxIdentityDedupe::Paired(
                ShardedProviderGroupTaxIdentityPairs::new(policy),
            )),
        )
    }

    fn new_with_optional_tax_identity(
        worker_count: usize,
        serving_rate_dedupe_enabled: bool,
        provider_group_tax_identity: Option<ProviderGroupTaxIdentityDedupe>,
    ) -> Self {
        let shard_count = (worker_count.max(1) * 4).max(16);
        let dedupe_high_cardinality_entries =
            env_bool("HLTHPRT_PTG2_RUST_DEDUPE_HIGH_CARDINALITY_ENTRIES", false);
        Self {
            serving_rate: serving_rate_dedupe_enabled.then(|| ShardedDedupe64::new(shard_count)),
            procedure: ShardedDedupe64::new(shard_count),
            price_code_set: ShardedDedupe64::new(shard_count),
            price_atom: ShardedDedupe128::new(shard_count),
            price_set: ShardedDedupe128::new(shard_count),
            price_set_entry: dedupe_high_cardinality_entries
                .then(|| ShardedDedupe128::new(shard_count)),
            provider_set: ShardedDedupe128::new(shard_count),
            provider_set_component: ShardedDedupe128::new(shard_count),
            provider_set_entry: ShardedDedupe128::new(shard_count),
            provider_entry_component: dedupe_high_cardinality_entries
                .then(|| ShardedDedupe128::new(shard_count)),
            provider_group: ShardedDedupe64::new(shard_count),
            provider_group_tax_identity,
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
            empty_npi_tin_only_normalization_count: AtomicU64::new(0),
        }
    }

    pub fn insert_serving_rate(&self, key: &str) -> Option<bool> {
        let inserted = self.serving_rate.as_ref()?.insert_hash_text(key);
        self.serving_rate_counter.record(inserted);
        Some(inserted)
    }

    pub fn record_unmeasured_serving_rates(&self, attempted: u64) {
        debug_assert!(self.serving_rate.is_none());
        self.serving_rate_counter.record_attempted(attempted);
    }

    pub fn insert_procedure(&self, key: &str) -> bool {
        let inserted = self.procedure.insert_hash_text(key);
        self.procedure_counter.record(inserted);
        inserted
    }

    pub fn insert_price_set(&self, key: GlobalId128) -> bool {
        let inserted = self.price_set.insert(u128::from_le_bytes(key.0));
        self.price_set_counter.record(inserted);
        inserted
    }

    pub fn record_local_price_set_duplicates(&self, count: u64) {
        self.price_set_counter.record_attempted(count);
    }

    pub fn record_local_price_atom_duplicates(&self, count: u64) {
        self.price_atom_counter.record_attempted(count);
    }

    pub fn insert_price_atom(&self, key: GlobalId128) -> bool {
        let inserted = self.price_atom.insert(u128::from_le_bytes(key.0));
        self.price_atom_counter.record(inserted);
        inserted
    }

    pub fn insert_price_code_set(&self, key: &str) -> bool {
        self.price_code_set.insert_hash_text(key)
    }

    pub fn insert_price_set_entry(
        &self,
        price_set_id: GlobalId128,
        price_atom_id: GlobalId128,
    ) -> bool {
        let inserted = match &self.price_set_entry {
            Some(dedupe) => {
                let mut hasher = Xxh3::new();
                hasher.update(b"price_set_entry_manifest_v3");
                hasher.update(&price_set_id.0);
                hasher.update(&price_atom_id.0);
                dedupe.insert(hasher.digest128())
            }
            None => true,
        };
        self.price_set_entry_counter.record(inserted);
        inserted
    }

    pub fn insert_provider_set(&self, key: GlobalId128) -> bool {
        let inserted = self.provider_set.insert(u128::from_le_bytes(key.0));
        self.provider_set_counter.record(inserted);
        inserted
    }

    pub fn record_local_provider_set_duplicates(&self, count: u64) {
        self.provider_set_counter.record_attempted(count);
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

    pub fn insert_provider_group_with_tax_identity(
        &self,
        group_hash: i64,
        tin: Option<&Value>,
    ) -> io::Result<bool> {
        let inserted = self.provider_group.insert(group_hash as u64);
        self.provider_group_counter.record(inserted);
        self.provider_group_tax_identity
            .as_ref()
            .ok_or_else(|| io::Error::other("provider tax identity output is not configured"))?
            .record(group_hash, tin)?;
        Ok(inserted)
    }

    pub fn provider_group_tax_identity_policy_id(&self) -> Option<&str> {
        self.provider_group_tax_identity
            .as_ref()
            .map(ProviderGroupTaxIdentityDedupe::policy_id)
    }

    /// Visits globally sorted v1 observations after ingestion has quiesced.
    /// This is not a snapshot-isolated view during concurrent inserts.
    pub fn visit_provider_group_tax_identities(
        &self,
        visitor: impl FnMut(i64, TaxIdentityObservation) -> io::Result<()>,
    ) -> io::Result<()> {
        self.provider_group_tax_identity
            .as_ref()
            .ok_or_else(|| io::Error::other("provider tax identity output is not configured"))?
            .visit_sorted(visitor)
    }

    /// Visits globally sorted atomic v1/v2 pairs after ingestion has quiesced.
    /// This is not a snapshot-isolated view during concurrent inserts.
    /// Existing group-hash collision proof remains a bundle-validation concern.
    pub fn visit_provider_group_tax_identity_pairs(
        &self,
        visitor: impl FnMut(i64, TaxIdentityObservation, TaxIdentityObservationV2) -> io::Result<()>,
    ) -> io::Result<()> {
        self.provider_group_tax_identity
            .as_ref()
            .ok_or_else(|| {
                io::Error::other("paired provider tax identity output is not configured")
            })?
            .visit_sorted_pairs(visitor)
    }

    pub fn unique_provider_group_count(&self) -> u64 {
        self.provider_group_counter.snapshot().1
    }

    pub fn record_cached_provider_group_attempts(&self, count: u64) {
        self.provider_group_counter.record_attempted(count);
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

    pub fn record_empty_npi_tin_only_normalizations(&self, count: u64) {
        self.empty_npi_tin_only_normalization_count
            .fetch_add(count, Ordering::Relaxed);
    }

    pub fn empty_npi_tin_only_normalization_count(&self) -> u64 {
        self.empty_npi_tin_only_normalization_count
            .load(Ordering::Relaxed)
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
    let serving_rate_dedupe_enabled = dedupe.serving_rate.is_some();
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
        "serving_rate_unique": serving_rate_dedupe_enabled.then_some(serving_unique),
        "serving_rate_duplicate": serving_rate_dedupe_enabled.then_some(serving_duplicate),
        "serving_rate_reduction_pct": serving_rate_dedupe_enabled.then(|| dedupe_reduction_pct(serving_attempted, serving_duplicate)),
        "serving_rate_dedupe_enabled": serving_rate_dedupe_enabled,
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
    let serving_unique = payload
        .get("serving_rate_unique")
        .and_then(Value::as_u64)
        .map_or_else(|| "not_measured".to_string(), |value| value.to_string());
    let serving_duplicate = payload
        .get("serving_rate_duplicate")
        .and_then(Value::as_u64)
        .map_or_else(|| "not_measured".to_string(), |value| value.to_string());
    let serving_reduction_pct = payload
        .get("serving_rate_reduction_pct")
        .and_then(Value::as_f64)
        .map_or_else(|| "not_measured".to_string(), |value| format!("{value:.2}"));
    eprintln!(
        "PTG2_DEDUPE_SUMMARY\tnegotiated_rates={}\tserving_rate_attempted={}\tserving_rate_unique={}\tserving_rate_duplicate={}\tserving_rate_reduction_pct={}\tserving_rate_dedupe_enabled={}\tprocedure_attempted={}\tprocedure_unique={}\tprocedure_duplicate={}\tprocedure_reduction_pct={:.2}\tprice_atom_attempted={}\tprice_atom_unique={}\tprice_atom_duplicate={}\tprice_atom_reduction_pct={:.2}\tprice_set_attempted={}\tprice_set_unique={}\tprice_set_duplicate={}\tprice_set_reduction_pct={:.2}\tprice_set_entry_attempted={}\tprice_set_entry_unique={}\tprice_set_entry_duplicate={}\tprice_set_entry_reduction_pct={:.2}\tprovider_set_attempted={}\tprovider_set_unique={}\tprovider_set_duplicate={}\tprovider_set_reduction_pct={:.2}\tprovider_set_entry_attempted={}\tprovider_set_entry_unique={}\tprovider_set_entry_duplicate={}\tprovider_set_entry_reduction_pct={:.2}\tprovider_entry_component_attempted={}\tprovider_entry_component_unique={}\tprovider_entry_component_duplicate={}\tprovider_entry_component_reduction_pct={:.2}\tprovider_group_member_attempted={}\tprovider_group_member_unique={}\tprovider_group_member_duplicate={}\tprovider_group_member_reduction_pct={:.2}",
        payload.get("negotiated_rates").and_then(Value::as_u64).unwrap_or(0),
        payload.get("serving_rate_attempted").and_then(Value::as_u64).unwrap_or(0),
        serving_unique,
        serving_duplicate,
        serving_reduction_pct,
        payload
            .get("serving_rate_dedupe_enabled")
            .and_then(Value::as_bool)
            .unwrap_or(false),
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
    use super::{
        dedupe_summary_payload, emit_dedupe_summary, provider_group_global_id_from_hash,
        PairedTaxIdentityObservation, ProviderGroupTaxIdentityDedupe, ProviderIdentifierQuarantine,
        SharedDedupe, MAX_QUARANTINED_PROVIDER_IDENTIFIERS,
    };
    use crate::manifest::GlobalId128;
    use crate::tax_identity::{
        TaxIdentityObservation, TaxIdentityObservationV2, TaxIdentityState, TaxIdentityStateV2,
        TinTokenPolicy,
    };
    use serde_json::{json, Value};
    use std::collections::HashMap;
    use std::io;
    use std::sync::{Arc, Barrier};

    fn tax_identity_policy() -> TinTokenPolicy {
        TinTokenPolicy::from_secret("ptg-tin-hmac-sha256-v1:paired-dedupe".to_string(), [7; 32])
            .unwrap()
    }

    fn frozen_tax_identity_policy() -> TinTokenPolicy {
        let mut secret = [0u8; 32];
        for (index, byte) in secret.iter_mut().enumerate() {
            *byte = index as u8;
        }
        TinTokenPolicy::from_secret("ptg-tin-hmac-sha256-v1:release-1".to_string(), secret).unwrap()
    }

    fn paired_rows(
        dedupe: &SharedDedupe,
    ) -> Vec<(i64, TaxIdentityObservation, TaxIdentityObservationV2)> {
        let mut rows = Vec::new();
        dedupe
            .visit_provider_group_tax_identity_pairs(|group_hash, v1, v2| {
                rows.push((group_hash, v1, v2));
                Ok(())
            })
            .unwrap();
        rows
    }

    fn accept_paired_row(
        _group_hash: i64,
        _v1: TaxIdentityObservation,
        _v2: TaxIdentityObservationV2,
    ) -> io::Result<()> {
        Ok(())
    }

    fn merged_pair(raw_tins: &[Option<Value>]) -> PairedTaxIdentityObservation {
        let dedupe = SharedDedupe::new_with_v4_paired_tax_identity(1, false, tax_identity_policy());
        for tin in raw_tins {
            dedupe
                .insert_provider_group_with_tax_identity(42, tin.as_ref())
                .unwrap();
        }
        let (_, v1, v2) = paired_rows(&dedupe).into_iter().next().unwrap();
        PairedTaxIdentityObservation { v1, v2 }
    }

    #[test]
    fn shared_dedupe_counts_serving_rate_duplicates() {
        let dedupe = SharedDedupe::new(1);
        let mut object_counts = HashMap::new();
        object_counts.insert("negotiated_rates".to_string(), 7);

        assert_eq!(dedupe.insert_serving_rate("rate-1"), Some(true));
        assert_eq!(dedupe.insert_serving_rate("rate-1"), Some(false));

        let payload = dedupe_summary_payload(&dedupe, &object_counts);
        assert_eq!(payload["negotiated_rates"], 7);
        assert_eq!(payload["serving_rate_attempted"], 2);
        assert_eq!(payload["serving_rate_unique"], 1);
        assert_eq!(payload["serving_rate_duplicate"], 1);
        assert_eq!(payload["serving_rate_reduction_pct"], 50.0);
        assert_eq!(payload["serving_rate_dedupe_enabled"], true);
    }

    #[test]
    fn disabled_serving_rate_dedupe_reports_attempts_without_claiming_uniqueness() {
        let dedupe = SharedDedupe::new_with_serving_rate_dedupe(2, false);
        dedupe.record_unmeasured_serving_rates(17);

        assert_eq!(dedupe.insert_serving_rate("unused"), None);
        let payload = dedupe_summary_payload(&dedupe, &HashMap::new());
        assert_eq!(payload["serving_rate_attempted"], 17);
        assert_eq!(payload["serving_rate_unique"], Value::Null);
        assert_eq!(payload["serving_rate_duplicate"], Value::Null);
        assert_eq!(payload["serving_rate_reduction_pct"], Value::Null);
        assert_eq!(payload["serving_rate_dedupe_enabled"], false);
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
        assert!(dedupe
            .insert_provider_group_with_tax_identity(101, None)
            .unwrap_err()
            .to_string()
            .contains("not configured"));
        assert!(dedupe
            .visit_provider_group_tax_identity_pairs(accept_paired_row)
            .unwrap_err()
            .to_string()
            .contains("not configured"));
    }

    #[test]
    fn shared_dedupe_uses_complete_v3_price_ids() {
        let dedupe = SharedDedupe::new(2);
        let low = GlobalId128([0; 16]);
        let mut high_bytes = [0; 16];
        high_bytes[15] = 1;
        let high = GlobalId128(high_bytes);

        assert!(dedupe.insert_price_atom(low));
        assert!(dedupe.insert_price_atom(high));
        assert!(!dedupe.insert_price_atom(low));
        assert!(dedupe.insert_price_set(low));
        assert!(dedupe.insert_price_set(high));
        assert!(!dedupe.insert_price_set(high));

        let payload = dedupe_summary_payload(&dedupe, &HashMap::new());
        assert_eq!(payload["price_atom_attempted"], 3);
        assert_eq!(payload["price_atom_unique"], 2);
        assert_eq!(payload["price_set_attempted"], 3);
        assert_eq!(payload["price_set_unique"], 2);
    }

    #[test]
    fn v4_tax_identity_dedupe_preserves_every_group_and_state() {
        let policy =
            TinTokenPolicy::from_secret("ptg-tin-hmac-sha256-v1:coverage".to_string(), [7; 32])
                .unwrap();
        let dedupe = SharedDedupe::new_with_v4_tax_identity(2, false, policy);
        assert_eq!(
            dedupe.provider_group_tax_identity_policy_id(),
            Some("ptg-tin-hmac-sha256-v1:coverage")
        );
        let matched = json!({"type": "ein", "value": "12-3456789"});
        let malformed = json!({"type": "ein", "value": "12 3456789"});
        assert!(dedupe
            .insert_provider_group_with_tax_identity(20, Some(&matched))
            .unwrap());
        assert!(!dedupe
            .insert_provider_group_with_tax_identity(20, Some(&matched))
            .unwrap());
        assert!(dedupe
            .insert_provider_group_with_tax_identity(-1, Some(&malformed))
            .unwrap());
        assert!(dedupe
            .insert_provider_group_with_tax_identity(5, None)
            .unwrap());

        let mut rows = Vec::new();
        dedupe
            .visit_provider_group_tax_identities(|group_hash, observation| {
                rows.push((group_hash, observation));
                Ok(())
            })
            .unwrap();
        assert_eq!(rows.len(), 3);
        assert!(rows.iter().any(|(group_hash, observation)| {
            *group_hash == 20
                && observation.state == TaxIdentityState::MatchedEin
                && observation.tin_hmac_sha256.is_some()
        }));
        assert!(rows.iter().any(|(group_hash, observation)| {
            *group_hash == -1 && observation.state == TaxIdentityState::Malformed
        }));
        assert_eq!(dedupe.unique_provider_group_count(), 3);
        dedupe.record_cached_provider_group_attempts(4);
        dedupe.record_empty_npi_tin_only_normalizations(2);
        assert_eq!(dedupe.empty_npi_tin_only_normalization_count(), 2);
        assert!(dedupe
            .visit_provider_group_tax_identity_pairs(accept_paired_row)
            .unwrap_err()
            .to_string()
            .contains("not configured"));
    }

    #[test]
    fn paired_tax_identity_dedupe_maps_raw_states_without_using_business_name() {
        let dedupe = SharedDedupe::new_with_v4_paired_tax_identity(2, false, tax_identity_policy());
        assert_eq!(
            dedupe.provider_group_tax_identity_policy_id(),
            Some("ptg-tin-hmac-sha256-v1:paired-dedupe")
        );
        let raw_tins = [
            (
                10,
                Some(json!({
                    "type": "ein",
                    "value": "12-3456789",
                    "business_name": "Synthetic Practice One"
                })),
            ),
            (11, Some(json!({"type": "npi", "value": "1000000491"}))),
            (12, Some(json!({"type": "npi", "value": "1000000492"}))),
            (13, None),
            (14, Some(json!({"type": "ein", "value": "12 3456789"}))),
            (15, Some(json!({"type": "other", "value": "opaque"}))),
        ];
        for (group_hash, tin) in &raw_tins {
            assert!(dedupe
                .insert_provider_group_with_tax_identity(*group_hash, tin.as_ref())
                .unwrap());
        }
        dedupe
            .visit_provider_group_tax_identity_pairs(accept_paired_row)
            .unwrap();
        let same_ein_different_name = json!({
            "type": "ein",
            "value": "12-3456789",
            "business_name": "Synthetic Practice Two"
        });
        assert!(!dedupe
            .insert_provider_group_with_tax_identity(10, Some(&same_ein_different_name))
            .unwrap());

        let rows = paired_rows(&dedupe)
            .into_iter()
            .map(|(group_hash, v1, v2)| (group_hash, (v1, v2)))
            .collect::<HashMap<_, _>>();
        let (ein_v1, ein_v2) = rows[&10];
        assert_eq!(ein_v1.state, TaxIdentityState::MatchedEin);
        assert_eq!(ein_v2.state, TaxIdentityStateV2::MatchedEin);
        assert_eq!(ein_v1.tin_hmac_sha256, ein_v2.tin_hmac_sha256);
        assert!(ein_v1.tin_hmac_sha256.is_some());
        assert_ne!(ein_v1.tin_hmac_sha256, Some([0; 32]));

        let (valid_npi_v1, valid_npi_v2) = rows[&11];
        assert_eq!(valid_npi_v1.state, TaxIdentityState::UnsupportedType);
        assert_eq!(valid_npi_v1.tin_hmac_sha256, None);
        assert_eq!(valid_npi_v2.state, TaxIdentityStateV2::MatchedNpi);
        assert!(valid_npi_v2.tin_hmac_sha256.is_some());
        assert_ne!(valid_npi_v2.tin_hmac_sha256, Some([0; 32]));

        assert_eq!(rows[&12].0.state, TaxIdentityState::UnsupportedType);
        assert_eq!(rows[&12].1.state, TaxIdentityStateV2::Malformed);
        assert_eq!(rows[&12].0.tin_hmac_sha256, None);
        assert_eq!(rows[&12].1.tin_hmac_sha256, None);
        assert_eq!(rows[&13].0.state, TaxIdentityState::Missing);
        assert_eq!(rows[&13].1.state, TaxIdentityStateV2::Missing);
        assert_eq!(rows[&14].0.state, TaxIdentityState::Malformed);
        assert_eq!(rows[&14].1.state, TaxIdentityStateV2::Malformed);
        assert_eq!(rows[&15].0.state, TaxIdentityState::UnsupportedType);
        assert_eq!(rows[&15].1.state, TaxIdentityStateV2::UnsupportedType);
    }

    #[test]
    fn paired_tax_identity_merges_are_order_independent_and_fail_closed() {
        let missing_to_npi = [
            None,
            Some(json!({"type": "ein", "value": "malformed"})),
            Some(json!({"type": "npi", "value": "100-000-0491"})),
            Some(json!({"type": "npi", "value": "1000000491"})),
        ];
        let mut npi_to_missing = missing_to_npi.clone();
        npi_to_missing.reverse();
        let expected_npi = merged_pair(&missing_to_npi);
        let reversed_npi = merged_pair(&npi_to_missing);
        assert_eq!(expected_npi.v1, reversed_npi.v1);
        assert_eq!(expected_npi.v2, reversed_npi.v2);
        assert_eq!(expected_npi.v1.state, TaxIdentityState::UnsupportedType);
        assert_eq!(expected_npi.v2.state, TaxIdentityStateV2::MatchedNpi);

        let unsupported_then_invalid = [
            Some(json!({"type": "other", "value": "opaque"})),
            Some(json!({"type": "npi", "value": "1000000492"})),
        ];
        let mut invalid_then_unsupported = unsupported_then_invalid.clone();
        invalid_then_unsupported.reverse();
        let expected_unsupported = merged_pair(&unsupported_then_invalid);
        let reversed_unsupported = merged_pair(&invalid_then_unsupported);
        assert_eq!(expected_unsupported.v1, reversed_unsupported.v1);
        assert_eq!(expected_unsupported.v2, reversed_unsupported.v2);
        assert_eq!(
            (expected_unsupported.v1.state, expected_unsupported.v2.state),
            (
                TaxIdentityState::UnsupportedType,
                TaxIdentityStateV2::UnsupportedType
            )
        );

        let dedupe = SharedDedupe::new_with_v4_paired_tax_identity(1, false, tax_identity_policy());
        let first_npi = json!({"type": "npi", "value": "1000000491"});
        dedupe
            .insert_provider_group_with_tax_identity(7, Some(&first_npi))
            .unwrap();
        let before = paired_rows(&dedupe);
        for conflicting in [
            json!({"type": "npi", "value": "2999999990"}),
            json!({"type": "ein", "value": "98-7654321"}),
        ] {
            let error = dedupe
                .insert_provider_group_with_tax_identity(7, Some(&conflicting))
                .unwrap_err();
            let error_message = error.to_string();
            assert!(!error_message.contains("1000000491"));
            assert!(!error_message.contains(conflicting["value"].as_str().unwrap()));
            assert_eq!(paired_rows(&dedupe), before);
        }
        let conflict_metrics = dedupe_summary_payload(&dedupe, &HashMap::new());
        assert_eq!(dedupe.unique_provider_group_count(), 1);
        assert_eq!(conflict_metrics["provider_group_attempted"], 3);
        assert_eq!(conflict_metrics["provider_group_unique"], 1);
        assert_eq!(conflict_metrics["provider_group_duplicate"], 2);

        let first_ein = json!({"type": "ein", "value": "12-3456789"});
        let different_ein = json!({"type": "ein", "value": "98-7654321"});
        dedupe
            .insert_provider_group_with_tax_identity(8, Some(&first_ein))
            .unwrap();
        let before_ein_conflict = paired_rows(&dedupe);
        let error = dedupe
            .insert_provider_group_with_tax_identity(8, Some(&different_ein))
            .unwrap_err();
        assert!(!error.to_string().contains("12-3456789"));
        assert!(!error.to_string().contains("98-7654321"));
        assert_eq!(paired_rows(&dedupe), before_ein_conflict);
        assert_eq!(dedupe.unique_provider_group_count(), 2);
    }

    #[test]
    fn paired_and_legacy_visitors_are_globally_sorted_and_reusable() {
        let dedupe = SharedDedupe::new_with_v4_paired_tax_identity(4, false, tax_identity_policy());
        let ein = json!({"type": "ein", "value": "12-3456789"});
        let group_hashes = (-256..=256).step_by(7).collect::<Vec<_>>();
        for group_hash in group_hashes.iter().rev() {
            dedupe
                .insert_provider_group_with_tax_identity(*group_hash, Some(&ein))
                .unwrap();
        }

        let callback_error = dedupe
            .visit_provider_group_tax_identity_pairs(|_, _, _| {
                Err(io::Error::other("synthetic visitor stop"))
            })
            .unwrap_err();
        assert_eq!(callback_error.to_string(), "synthetic visitor stop");

        let paired = paired_rows(&dedupe);
        let mut expected_hashes = group_hashes;
        expected_hashes.sort_unstable_by_key(|hash| provider_group_global_id_from_hash(*hash));
        assert_eq!(
            paired.iter().map(|(hash, _, _)| *hash).collect::<Vec<_>>(),
            expected_hashes
        );

        let mut legacy = Vec::new();
        dedupe
            .visit_provider_group_tax_identities(|group_hash, observation| {
                legacy.push((group_hash, observation));
                Ok(())
            })
            .unwrap();
        assert_eq!(
            legacy,
            paired
                .iter()
                .map(|(group_hash, v1, _v2)| (*group_hash, *v1))
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn concurrent_paired_tax_identity_merges_are_deterministic() {
        let dedupe = Arc::new(SharedDedupe::new_with_v4_paired_tax_identity(
            8,
            false,
            tax_identity_policy(),
        ));
        let raw_tins = [
            None,
            Some(json!({"type": "ein", "value": "malformed"})),
            Some(json!({"type": "npi", "value": "100-000-0491"})),
            Some(json!({"type": "npi", "value": "1000000491"})),
        ];
        let workers = raw_tins
            .into_iter()
            .cycle()
            .take(32)
            .map(|tin| {
                let dedupe = Arc::clone(&dedupe);
                std::thread::spawn(move || {
                    dedupe
                        .insert_provider_group_with_tax_identity(99, tin.as_ref())
                        .unwrap();
                })
            })
            .collect::<Vec<_>>();
        for worker in workers {
            worker.join().unwrap();
        }

        let rows = paired_rows(&dedupe);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].1.state, TaxIdentityState::UnsupportedType);
        assert_eq!(rows[0].2.state, TaxIdentityStateV2::MatchedNpi);
    }

    #[test]
    fn concurrent_supported_identity_conflicts_never_store_hybrid_pairs() {
        let cases = [
            (
                json!({"type": "npi", "value": "1000000491"}),
                json!({"type": "npi", "value": "2999999990"}),
            ),
            (
                json!({"type": "ein", "value": "12-3456789"}),
                json!({"type": "ein", "value": "98-7654321"}),
            ),
            (
                json!({"type": "ein", "value": "12-3456789"}),
                json!({"type": "npi", "value": "1000000491"}),
            ),
        ];
        for (first, second) in cases {
            let expected_policy = tax_identity_policy();
            let expected_pairs = [
                (
                    expected_policy.observe(Some(&first)),
                    expected_policy.observe_v2(Some(&first)),
                ),
                (
                    expected_policy.observe(Some(&second)),
                    expected_policy.observe_v2(Some(&second)),
                ),
            ];
            for _round in 0..32 {
                let dedupe = Arc::new(SharedDedupe::new_with_v4_paired_tax_identity(
                    2,
                    false,
                    tax_identity_policy(),
                ));
                let barrier = Arc::new(Barrier::new(3));
                let workers = [first.clone(), second.clone()]
                    .into_iter()
                    .map(|identity| {
                        let dedupe = Arc::clone(&dedupe);
                        let barrier = Arc::clone(&barrier);
                        std::thread::spawn(move || {
                            barrier.wait();
                            dedupe.insert_provider_group_with_tax_identity(404, Some(&identity))
                        })
                    })
                    .collect::<Vec<_>>();
                barrier.wait();
                let outcomes = workers
                    .into_iter()
                    .map(|worker| worker.join().unwrap())
                    .collect::<Vec<_>>();

                assert_eq!(outcomes.iter().filter(|outcome| outcome.is_ok()).count(), 1);
                let error = outcomes
                    .iter()
                    .find_map(|outcome| outcome.as_ref().err())
                    .unwrap()
                    .to_string();
                assert!(!error.contains(first["value"].as_str().unwrap()));
                assert!(!error.contains(second["value"].as_str().unwrap()));
                let rows = paired_rows(&dedupe);
                assert_eq!(rows.len(), 1);
                let stored_pair = (rows[0].1, rows[0].2);
                assert!(expected_pairs.contains(&stored_pair));
            }
        }
    }

    #[test]
    fn paired_v1_projection_is_exactly_equal_to_legacy_collection() {
        let legacy = SharedDedupe::new_with_v4_tax_identity(4, false, tax_identity_policy());
        let paired = SharedDedupe::new_with_v4_paired_tax_identity(4, false, tax_identity_policy());
        let inputs = [
            (29, Some(json!({"type": "ein", "value": "12-3456789"}))),
            (7, Some(json!({"type": "npi", "value": "1000000491"}))),
            (13, Some(json!({"type": "ein", "value": "12 3456789"}))),
            (3, None),
            (7, Some(json!({"type": "npi", "value": "1000000491"}))),
            (41, Some(json!({"type": "other", "value": "opaque"}))),
        ];
        for (group_hash, identity) in &inputs {
            legacy
                .insert_provider_group_with_tax_identity(*group_hash, identity.as_ref())
                .unwrap();
            paired
                .insert_provider_group_with_tax_identity(*group_hash, identity.as_ref())
                .unwrap();
        }

        let mut legacy_rows = Vec::new();
        legacy
            .visit_provider_group_tax_identities(|group_hash, observation| {
                legacy_rows.push((group_hash, observation));
                Ok(())
            })
            .unwrap();
        let paired_v1_rows = paired_rows(&paired)
            .into_iter()
            .map(|(group_hash, v1, _v2)| (group_hash, v1))
            .collect::<Vec<_>>();
        assert_eq!(paired_v1_rows, legacy_rows);
    }

    #[test]
    fn paired_tokens_match_frozen_v1_and_v2_vectors() {
        let dedupe =
            SharedDedupe::new_with_v4_paired_tax_identity(1, false, frozen_tax_identity_policy());
        let ein = json!({"type": "ein", "value": "12-3456789"});
        let npi = json!({"type": "npi", "value": "1000000491"});
        dedupe
            .insert_provider_group_with_tax_identity(1, Some(&ein))
            .unwrap();
        dedupe
            .insert_provider_group_with_tax_identity(2, Some(&npi))
            .unwrap();
        let rows = paired_rows(&dedupe)
            .into_iter()
            .map(|(group_hash, v1, v2)| (group_hash, (v1, v2)))
            .collect::<HashMap<_, _>>();
        let expected_ein = [
            0x2b, 0x5a, 0x27, 0x99, 0x04, 0x84, 0x8d, 0x15, 0xed, 0x9f, 0x42, 0xd5, 0xaf, 0xd7,
            0x33, 0x41, 0xf7, 0x7e, 0xe6, 0x3e, 0x13, 0x76, 0x52, 0x1f, 0x3f, 0x78, 0xc9, 0x4d,
            0x72, 0x29, 0x93, 0xc0,
        ];
        let expected_npi = [
            0x83, 0x70, 0xf2, 0x24, 0x6a, 0x6b, 0x7b, 0x08, 0xab, 0xb5, 0x5f, 0x6f, 0xc1, 0x1f,
            0xd7, 0x50, 0x15, 0x46, 0x7c, 0x42, 0x70, 0xdd, 0xee, 0xf3, 0xf8, 0x73, 0x96, 0xed,
            0x73, 0x4e, 0x1f, 0x73,
        ];

        assert_eq!(rows[&1].0.tin_hmac_sha256, Some(expected_ein));
        assert_eq!(rows[&1].1.tin_hmac_sha256, Some(expected_ein));
        assert_eq!(rows[&2].0.tin_hmac_sha256, None);
        assert_eq!(rows[&2].1.tin_hmac_sha256, Some(expected_npi));
    }

    #[test]
    fn paired_tax_identity_validation_rejects_invalid_shapes_and_cross_type_pairs() {
        let invalid_zero_token = PairedTaxIdentityObservation {
            v1: TaxIdentityObservation {
                state: TaxIdentityState::MatchedEin,
                tin_hmac_sha256: Some([0; 32]),
            },
            v2: TaxIdentityObservationV2 {
                state: TaxIdentityStateV2::MatchedEin,
                tin_hmac_sha256: Some([0; 32]),
            },
        };
        assert!(invalid_zero_token.validate().is_err());

        let cross_type = PairedTaxIdentityObservation {
            v1: TaxIdentityObservation {
                state: TaxIdentityState::MatchedEin,
                tin_hmac_sha256: Some([1; 32]),
            },
            v2: TaxIdentityObservationV2 {
                state: TaxIdentityStateV2::MatchedNpi,
                tin_hmac_sha256: Some([1; 32]),
            },
        };
        assert!(cross_type.validate().is_err());
        assert!(cross_type.merge(cross_type).is_err());
    }

    #[test]
    fn paired_tax_identity_poisoned_shard_fails_closed_without_raw_echo() {
        let dedupe = SharedDedupe::new_with_v4_paired_tax_identity(1, false, tax_identity_policy());
        let group_hash = 314;
        let shard_index = usize::from(provider_group_global_id_from_hash(group_hash).0[0]);
        let identity = match dedupe.provider_group_tax_identity.as_ref().unwrap() {
            ProviderGroupTaxIdentityDedupe::Paired(identity) => identity,
            ProviderGroupTaxIdentityDedupe::V1(_) => panic!("expected paired identity mode"),
        };
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = identity.shards[shard_index].lock().unwrap();
            panic!("poison paired tax identity shard for fail-closed coverage");
        }));

        let raw_tin = json!({"type": "ein", "value": "12-3456789"});
        let record_error = dedupe
            .insert_provider_group_with_tax_identity(group_hash, Some(&raw_tin))
            .unwrap_err();
        assert!(record_error.to_string().contains("lock poisoned"));
        assert!(!record_error.to_string().contains("12-3456789"));

        let visit_error = dedupe
            .visit_provider_group_tax_identity_pairs(accept_paired_row)
            .unwrap_err();
        assert!(visit_error.to_string().contains("lock poisoned"));
        assert!(!visit_error.to_string().contains("12-3456789"));
    }

    #[test]
    fn v4_projection_dedupe_and_quarantine_metrics_are_exact() {
        let dedupe = SharedDedupe::new(1);
        let first = GlobalId128([1; 16]);
        let second = GlobalId128([2; 16]);
        assert!(dedupe.insert_procedure("CPT:70553"));
        assert!(!dedupe.insert_procedure("CPT:70553"));
        assert!(dedupe.insert_price_code_set("11,12"));
        assert!(!dedupe.insert_price_code_set("11,12"));
        assert!(dedupe.insert_price_set_entry(first, second));
        assert!(dedupe.insert_provider_set(first));
        assert!(!dedupe.insert_provider_set(first));
        assert!(dedupe.insert_provider_set_component("set-1", 7));
        assert!(!dedupe.insert_provider_set_component("set-1", 7));
        assert!(dedupe.insert_provider_set_entry("set-1", 8));
        assert!(!dedupe.insert_provider_set_entry("set-1", 8));
        assert!(dedupe.insert_provider_entry_component(8, 7));
        dedupe.record_local_price_set_duplicates(2);
        dedupe.record_local_price_atom_duplicates(3);
        dedupe.record_local_provider_set_duplicates(4);
        dedupe
            .record_quarantined_provider_identifiers(&[123, 123])
            .unwrap();
        let quarantine = dedupe.provider_identifier_quarantine().unwrap();
        assert_eq!(quarantine.payload().unwrap()["occurrence_count"], 2);

        let payload = dedupe_summary_payload(&dedupe, &HashMap::new());
        assert_eq!(payload["procedure_duplicate"], 1);
        assert_eq!(payload["provider_set_component_duplicate"], Value::Null);
        emit_dedupe_summary(&dedupe, &HashMap::new());

        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = dedupe.provider_identifier_quarantine.lock().unwrap();
            panic!("poison quarantine lock for fail-closed coverage");
        }));
        assert!(dedupe
            .record_quarantined_provider_identifiers(&[-1])
            .is_err());
        assert!(dedupe.provider_identifier_quarantine().is_err());
    }

    #[test]
    fn provider_identifier_quarantine_rejects_valid_and_unbounded_values() {
        let mut quarantine = ProviderIdentifierQuarantine::default();
        assert!(quarantine.record(&[0]).is_err());
        assert!(quarantine.record(&[1_000_000_000]).is_err());
        for value in 1..=MAX_QUARANTINED_PROVIDER_IDENTIFIERS {
            quarantine.occurrences_by_value.insert(-(value as i64), 1);
        }
        assert!(quarantine.record(&[-2_000]).is_err());

        let mut incoming = ProviderIdentifierQuarantine::default();
        incoming.record(&[-2_000]).unwrap();
        assert!(quarantine.merge(&incoming).is_err());
    }

    #[test]
    fn provider_identifier_quarantine_count_overflow_fails_closed() {
        let mut record_overflow = ProviderIdentifierQuarantine::default();
        record_overflow.occurrences_by_value.insert(-1, u64::MAX);
        assert!(record_overflow.record(&[-1]).is_err());

        let mut merge_overflow = record_overflow.clone();
        let mut incoming = ProviderIdentifierQuarantine::default();
        incoming.occurrences_by_value.insert(-1, 1);
        assert!(merge_overflow.merge(&incoming).is_err());

        let mut payload_overflow = ProviderIdentifierQuarantine::default();
        payload_overflow.occurrences_by_value.insert(-2, u64::MAX);
        payload_overflow.occurrences_by_value.insert(-1, 1);
        assert!(payload_overflow.payload().is_err());
    }

    #[test]
    fn high_cardinality_identity_deduplication_is_exact_when_configured() {
        let mut dedupe = SharedDedupe::new(1);
        dedupe.price_set_entry = Some(super::ShardedDedupe128::new(16));
        dedupe.provider_entry_component = Some(super::ShardedDedupe128::new(16));
        let price_set = GlobalId128([1; 16]);
        let price_atom = GlobalId128([2; 16]);

        assert!(dedupe.insert_price_set_entry(price_set, price_atom));
        assert!(!dedupe.insert_price_set_entry(price_set, price_atom));
        assert!(dedupe.insert_provider_entry_component(8, 7));
        assert!(!dedupe.insert_provider_entry_component(8, 7));
    }
}
