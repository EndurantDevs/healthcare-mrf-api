//! PTG2 manifest identity and sidecar primitives.
//!
//! The active v2 scanner writes text hash keys directly into PostgreSQL COPY
//! rows. Manifest keeps stable global content identities, then maps them to
//! deterministic snapshot-local dense ids for the hot serving tables.

use crate::hashing::{canonical_json, update_hash_optional_str, update_hash_string_list};
use serde_json::Value;
use std::collections::BTreeMap;
use std::io::{self, Write};
use xxhash_rust::xxh3::Xxh3;

pub const GLOBAL_ID_BYTES: usize = 16;
const LOWER_HEX: &[u8; 16] = b"0123456789abcdef";

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct GlobalId128(pub [u8; GLOBAL_ID_BYTES]);

impl GlobalId128 {
    pub fn from_domain_payload(domain: &str, payload: &Value) -> Self {
        let mut hasher = Xxh3::new();
        hasher.update(domain.as_bytes());
        hasher.update(b"\x1f");
        hasher.update(canonical_json(payload).as_bytes());
        Self(hasher.digest128().to_le_bytes())
    }

    pub fn from_parts(domain: &str, parts: &[&str]) -> Self {
        let mut hasher = Xxh3::new();
        hasher.update(domain.as_bytes());
        for part in parts {
            let mut length_buffer = itoa::Buffer::new();
            hasher.update(b"\x1f");
            hasher.update(length_buffer.format(part.len()).as_bytes());
            hasher.update(b":");
            hasher.update(part.as_bytes());
        }
        Self(hasher.digest128().to_le_bytes())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn from_price_atom_parts(
        negotiated_type: Option<&str>,
        negotiated_rate: Option<&str>,
        expiration_date: Option<&str>,
        service_code: &[String],
        billing_class: Option<&str>,
        setting: Option<&str>,
        billing_code_modifier: &[String],
        additional_information: Option<&str>,
    ) -> Self {
        let mut hasher = Xxh3::new();
        hasher.update(b"price_atom_manifest");
        update_hash_optional_str(&mut hasher, negotiated_type);
        update_hash_optional_str(&mut hasher, negotiated_rate);
        update_hash_optional_str(&mut hasher, expiration_date);
        update_hash_string_list(&mut hasher, service_code);
        update_hash_optional_str(&mut hasher, billing_class);
        update_hash_optional_str(&mut hasher, setting);
        update_hash_string_list(&mut hasher, billing_code_modifier);
        update_hash_optional_str(&mut hasher, additional_information);
        Self(hasher.digest128().to_le_bytes())
    }

    pub fn serving_content(
        plan_id: &str,
        procedure_global_id: GlobalId128,
        provider_set_global_id: GlobalId128,
        price_set_global_id: GlobalId128,
    ) -> Self {
        let mut hasher = Xxh3::new();
        hasher.update(b"serving_content_manifest");
        hasher.update(b"\x1fplan:");
        hasher.update(&(plan_id.len() as u64).to_le_bytes());
        hasher.update(plan_id.as_bytes());
        hasher.update(b"\x1fprocedure:");
        hasher.update(&procedure_global_id.0);
        hasher.update(b"\x1fprovider_set:");
        hasher.update(&provider_set_global_id.0);
        hasher.update(b"\x1fprice_set:");
        hasher.update(&price_set_global_id.0);
        Self(hasher.digest128().to_le_bytes())
    }

    pub fn to_hex(self) -> String {
        let mut out = String::with_capacity(GLOBAL_ID_BYTES * 2);
        for byte in self.0 {
            out.push(LOWER_HEX[(byte >> 4) as usize] as char);
            out.push(LOWER_HEX[(byte & 0x0f) as usize] as char);
        }
        out
    }
}

pub fn procedure_global_id(procedure_payload: &Value) -> GlobalId128 {
    GlobalId128::from_domain_payload("procedure_manifest", procedure_payload)
}

pub fn provider_set_global_id_from_group_hashes(provider_group_hashes: &[i64]) -> GlobalId128 {
    let mut sorted_group_hashes = provider_group_hashes.to_vec();
    sorted_group_hashes.sort_unstable();
    sorted_group_hashes.dedup();

    let mut hasher = Xxh3::new();
    hasher.update(b"provider_set_manifest");
    for provider_group_hash in sorted_group_hashes {
        hasher.update(b"\x1f");
        hasher.update(&provider_group_hash.to_le_bytes());
    }
    GlobalId128(hasher.digest128().to_le_bytes())
}

pub fn price_set_global_id_from_atom_ids(price_atom_ids: &[GlobalId128]) -> GlobalId128 {
    let mut sorted_atom_ids = price_atom_ids.to_vec();
    sorted_atom_ids.sort_unstable();
    sorted_atom_ids.dedup();

    let mut hasher = Xxh3::new();
    hasher.update(b"price_set_manifest");
    for price_atom_id in sorted_atom_ids {
        hasher.update(b"\x1f");
        hasher.update(&price_atom_id.0);
    }
    GlobalId128(hasher.digest128().to_le_bytes())
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DenseIdMap {
    ids: Vec<GlobalId128>,
}

impl DenseIdMap {
    pub fn from_global_ids<I>(ids: I) -> Self
    where
        I: IntoIterator<Item = GlobalId128>,
    {
        let mut ids: Vec<GlobalId128> = ids.into_iter().collect();
        ids.sort_unstable();
        ids.dedup();
        Self { ids }
    }

    pub fn len(&self) -> usize {
        self.ids.len()
    }

    pub fn is_empty(&self) -> bool {
        self.ids.is_empty()
    }

    pub fn dense_id(&self, id: GlobalId128) -> Option<u32> {
        self.ids
            .binary_search(&id)
            .ok()
            .and_then(|index| u32::try_from(index).ok())
    }

    pub fn global_id(&self, dense_id: u32) -> Option<GlobalId128> {
        self.ids.get(dense_id as usize).copied()
    }

    pub fn iter(&self) -> impl Iterator<Item = (u32, GlobalId128)> + '_ {
        self.ids
            .iter()
            .copied()
            .enumerate()
            .map(|(index, id)| (index as u32, id))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SidecarEntry {
    pub owner: GlobalId128,
    pub members: Vec<GlobalId128>,
}

pub fn normalized_sidecar_entries<I>(entries: I) -> Vec<SidecarEntry>
where
    I: IntoIterator<Item = (GlobalId128, Vec<GlobalId128>)>,
{
    let mut grouped: BTreeMap<GlobalId128, Vec<GlobalId128>> = BTreeMap::new();
    for (owner, mut members) in entries {
        grouped.entry(owner).or_default().append(&mut members);
    }
    grouped
        .into_iter()
        .map(|(owner, mut members)| {
            members.sort_unstable();
            members.dedup();
            SidecarEntry { owner, members }
        })
        .collect()
}

pub fn write_global_sidecar<W: Write>(writer: &mut W, entries: &[SidecarEntry]) -> io::Result<()> {
    writer.write_all(b"PTG2MNSC")?;
    writer.write_all(&1u32.to_le_bytes())?;
    writer.write_all(&(entries.len() as u64).to_le_bytes())?;
    let mut offset: u64 = 0;
    for entry in entries {
        writer.write_all(&entry.owner.0)?;
        writer.write_all(&offset.to_le_bytes())?;
        writer.write_all(&(entry.members.len() as u32).to_le_bytes())?;
        offset = offset.saturating_add(entry.members.len() as u64);
    }
    for entry in entries {
        for member in &entry.members {
            writer.write_all(&member.0)?;
        }
    }
    Ok(())
}

pub fn write_dense_member_sidecar<W: Write>(
    writer: &mut W,
    entries: &[SidecarEntry],
) -> io::Result<()> {
    let mut member_ids: Vec<GlobalId128> = entries
        .iter()
        .flat_map(|entry| entry.members.iter().copied())
        .collect();
    member_ids.sort_unstable();
    member_ids.dedup();

    let local_ids: BTreeMap<GlobalId128, u32> = member_ids
        .iter()
        .copied()
        .enumerate()
        .map(|(index, member_id)| (member_id, index as u32))
        .collect();

    writer.write_all(b"PTG2MNDS")?;
    writer.write_all(&1u32.to_le_bytes())?;
    writer.write_all(&(entries.len() as u64).to_le_bytes())?;
    writer.write_all(&(member_ids.len() as u64).to_le_bytes())?;
    let mut offset: u64 = 0;
    for entry in entries {
        writer.write_all(&entry.owner.0)?;
        writer.write_all(&offset.to_le_bytes())?;
        writer.write_all(&(entry.members.len() as u32).to_le_bytes())?;
        offset = offset.saturating_add(entry.members.len() as u64);
    }
    for member_id in &member_ids {
        writer.write_all(&member_id.0)?;
    }
    for entry in entries {
        for member in &entry.members {
            let Some(local_id) = local_ids.get(member) else {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "dense sidecar member is missing from local dictionary",
                ));
            };
            writer.write_all(&local_id.to_le_bytes())?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn global_id_is_canonical_json_order_stable() {
        let first = GlobalId128::from_domain_payload("procedure", &json!({"b": 2, "a": 1}));
        let second = GlobalId128::from_domain_payload("procedure", &json!({"a": 1, "b": 2}));

        assert_eq!(first, second);
        assert_eq!(first.to_hex().len(), 32);
    }

    #[test]
    fn global_id_hex_uses_lowercase_fixed_width_encoding() {
        let id = GlobalId128([
            0x00, 0x01, 0x0f, 0x10, 0x2a, 0x3b, 0x4c, 0x5d, 0x6e, 0x7f, 0x80, 0x91, 0xa2, 0xb3,
            0xc4, 0xff,
        ]);

        assert_eq!(id.to_hex(), "00010f102a3b4c5d6e7f8091a2b3c4ff");
    }

    #[test]
    fn serving_content_hash_can_exclude_snapshot_specific_parts() {
        let proc = GlobalId128([1; GLOBAL_ID_BYTES]);
        let provider = GlobalId128([2; GLOBAL_ID_BYTES]);
        let price = GlobalId128([3; GLOBAL_ID_BYTES]);
        let first = GlobalId128::serving_content("plan", proc, provider, price);
        let second = GlobalId128::serving_content("plan", proc, provider, price);
        let with_snapshot = GlobalId128::from_parts(
            "serving_content",
            &["snapshot", "plan", "proc", "provider", "price"],
        );
        let with_other_plan = GlobalId128::serving_content("other-plan", proc, provider, price);

        assert_eq!(first, second);
        assert_ne!(first, with_snapshot);
        assert_ne!(first, with_other_plan);
    }

    #[test]
    fn provider_set_global_id_sorts_and_dedupes_members() {
        let first = provider_set_global_id_from_group_hashes(&[1, 2, 3]);
        let second = provider_set_global_id_from_group_hashes(&[3, 2, 1, 3]);
        let different_members = provider_set_global_id_from_group_hashes(&[1, 2, 4]);

        assert_eq!(first, second);
        assert_eq!(first.to_hex().len(), 32);
        assert_ne!(first, different_members);
    }

    #[test]
    fn price_set_global_id_sorts_and_dedupes_atom_ids() {
        let high = GlobalId128([9; GLOBAL_ID_BYTES]);
        let low = GlobalId128([1; GLOBAL_ID_BYTES]);

        let first = price_set_global_id_from_atom_ids(&[high, low, high]);
        let second = price_set_global_id_from_atom_ids(&[low, high]);
        let different = price_set_global_id_from_atom_ids(&[low]);

        assert_eq!(first, second);
        assert_ne!(first, different);
    }

    #[test]
    fn dense_ids_are_deterministic_and_sorted_by_global_id() {
        let high = GlobalId128([9; GLOBAL_ID_BYTES]);
        let low = GlobalId128([1; GLOBAL_ID_BYTES]);
        let duplicate = GlobalId128([9; GLOBAL_ID_BYTES]);

        let map = DenseIdMap::from_global_ids([high, low, duplicate]);

        assert_eq!(map.len(), 2);
        assert_eq!(map.dense_id(low), Some(0));
        assert_eq!(map.dense_id(high), Some(1));
        assert_eq!(map.global_id(0), Some(low));
    }

    #[test]
    fn sidecar_entries_merge_sort_and_dedupe_members() {
        let owner = GlobalId128([7; GLOBAL_ID_BYTES]);
        let high = GlobalId128([9; GLOBAL_ID_BYTES]);
        let low = GlobalId128([1; GLOBAL_ID_BYTES]);

        let entries = normalized_sidecar_entries([(owner, vec![high, low]), (owner, vec![high])]);

        assert_eq!(
            entries,
            vec![SidecarEntry {
                owner,
                members: vec![low, high],
            }]
        );
    }

    #[test]
    fn global_sidecar_is_fixed_width_and_deterministic() {
        let owner = GlobalId128([7; GLOBAL_ID_BYTES]);
        let low = GlobalId128([1; GLOBAL_ID_BYTES]);
        let high = GlobalId128([9; GLOBAL_ID_BYTES]);
        let entries = normalized_sidecar_entries([(owner, vec![high, low])]);
        let mut out = Vec::new();

        write_global_sidecar(&mut out, &entries).unwrap();

        assert_eq!(&out[0..8], b"PTG2MNSC");
        assert_eq!(u32::from_le_bytes(out[8..12].try_into().unwrap()), 1);
        assert_eq!(u64::from_le_bytes(out[12..20].try_into().unwrap()), 1);
        assert_eq!(&out[20..36], &[7; GLOBAL_ID_BYTES]);
        assert_eq!(u64::from_le_bytes(out[36..44].try_into().unwrap()), 0);
        assert_eq!(u32::from_le_bytes(out[44..48].try_into().unwrap()), 2);
        assert_eq!(&out[48..64], &[1; GLOBAL_ID_BYTES]);
        assert_eq!(&out[64..80], &[9; GLOBAL_ID_BYTES]);
        assert_eq!(out.len(), 8 + 4 + 8 + 28 + 32);
    }

    #[test]
    fn dense_member_sidecar_uses_sorted_local_member_dictionary() {
        let first_owner = GlobalId128([7; GLOBAL_ID_BYTES]);
        let second_owner = GlobalId128([8; GLOBAL_ID_BYTES]);
        let low = GlobalId128([1; GLOBAL_ID_BYTES]);
        let high = GlobalId128([9; GLOBAL_ID_BYTES]);
        let entries = normalized_sidecar_entries([
            (first_owner, vec![high, low]),
            (second_owner, vec![high]),
        ]);
        let mut out = Vec::new();

        write_dense_member_sidecar(&mut out, &entries).unwrap();

        assert_eq!(&out[0..8], b"PTG2MNDS");
        assert_eq!(u32::from_le_bytes(out[8..12].try_into().unwrap()), 1);
        assert_eq!(u64::from_le_bytes(out[12..20].try_into().unwrap()), 2);
        assert_eq!(u64::from_le_bytes(out[20..28].try_into().unwrap()), 2);
        assert_eq!(&out[28..44], &[7; GLOBAL_ID_BYTES]);
        assert_eq!(u64::from_le_bytes(out[44..52].try_into().unwrap()), 0);
        assert_eq!(u32::from_le_bytes(out[52..56].try_into().unwrap()), 2);
        assert_eq!(&out[84..100], &[1; GLOBAL_ID_BYTES]);
        assert_eq!(&out[100..116], &[9; GLOBAL_ID_BYTES]);
        assert_eq!(u32::from_le_bytes(out[116..120].try_into().unwrap()), 0);
        assert_eq!(u32::from_le_bytes(out[120..124].try_into().unwrap()), 1);
        assert_eq!(u32::from_le_bytes(out[124..128].try_into().unwrap()), 1);
        assert_eq!(out.len(), 8 + 4 + 8 + 8 + 2 * 28 + 2 * 16 + 3 * 4);
    }
}
