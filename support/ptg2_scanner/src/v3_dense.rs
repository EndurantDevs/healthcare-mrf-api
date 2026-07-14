//! Compact immutable lookups for strict-V3 global identities.

use std::io;
use xxhash_rust::xxh3::xxh3_64;

pub const IDENTITY_BYTES: usize = 16;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct DenseIdentityValue {
    pub key: u32,
    pub auxiliary: u32,
}

#[derive(Clone, Copy, Default)]
struct Slot {
    identity: [u8; IDENTITY_BYTES],
    value: DenseIdentityValue,
    occupied: bool,
}

/// A compact open-addressed table built once and shared read-only by partition workers.
#[derive(Clone)]
pub struct DenseIdentityMap {
    slots: Vec<Slot>,
    len: usize,
}

impl DenseIdentityMap {
    pub fn estimated_memory_bytes(expected_len: usize) -> io::Result<usize> {
        let minimum_slots = expected_len
            .checked_mul(10)
            .and_then(|value| value.checked_div(7))
            .and_then(|value| value.checked_add(1))
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "identity map is too large")
            })?;
        let slot_count = minimum_slots
            .max(2)
            .checked_next_power_of_two()
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "identity map is too large")
            })?;
        slot_count
            .checked_mul(std::mem::size_of::<Slot>())
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "identity map is too large"))
    }

    pub fn with_capacity(expected_len: usize) -> io::Result<Self> {
        let memory_bytes = Self::estimated_memory_bytes(expected_len)?;
        let slot_count = memory_bytes / std::mem::size_of::<Slot>();
        Ok(Self {
            slots: vec![Slot::default(); slot_count],
            len: 0,
        })
    }

    pub fn insert(
        &mut self,
        identity: [u8; IDENTITY_BYTES],
        value: DenseIdentityValue,
    ) -> io::Result<()> {
        let mask = self.slots.len() - 1;
        let maximum_len = self.slots.len().saturating_mul(7) / 10;
        let mut index = xxh3_64(&identity) as usize & mask;
        loop {
            let slot = &mut self.slots[index];
            if !slot.occupied {
                if self.len >= maximum_len {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "identity map exceeded its bounded load factor",
                    ));
                }
                *slot = Slot {
                    identity,
                    value,
                    occupied: true,
                };
                self.len += 1;
                return Ok(());
            }
            if slot.identity == identity {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "duplicate identity in immutable dense map",
                ));
            }
            index = (index + 1) & mask;
        }
    }

    #[inline]
    pub fn get(&self, identity: &[u8; IDENTITY_BYTES]) -> Option<DenseIdentityValue> {
        let mask = self.slots.len() - 1;
        let mut index = xxh3_64(identity) as usize & mask;
        loop {
            let slot = &self.slots[index];
            if !slot.occupied {
                return None;
            }
            if &slot.identity == identity {
                return Some(slot.value);
            }
            index = (index + 1) & mask;
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn memory_bytes(&self) -> usize {
        self.slots.len().saturating_mul(std::mem::size_of::<Slot>())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn identity(value: u64) -> [u8; IDENTITY_BYTES] {
        let mut identity = [0u8; IDENTITY_BYTES];
        identity[8..].copy_from_slice(&value.to_be_bytes());
        identity
    }

    #[test]
    fn immutable_dense_map_handles_collisions_and_missing_keys() {
        let mut map = DenseIdentityMap::with_capacity(10_000).unwrap();
        for key in 0..10_000u32 {
            map.insert(
                identity(u64::from(key)),
                DenseIdentityValue {
                    key,
                    auxiliary: key ^ 0x55aa,
                },
            )
            .unwrap();
        }
        assert_eq!(map.len(), 10_000);
        for key in (0..10_000u32).rev() {
            assert_eq!(
                map.get(&identity(u64::from(key))),
                Some(DenseIdentityValue {
                    key,
                    auxiliary: key ^ 0x55aa,
                })
            );
        }
        assert_eq!(map.get(&identity(10_001)), None);
        assert!(map.memory_bytes() >= map.len() * std::mem::size_of::<Slot>());
    }

    #[test]
    fn immutable_dense_map_rejects_duplicates() {
        let mut map = DenseIdentityMap::with_capacity(1).unwrap();
        map.insert(identity(7), DenseIdentityValue::default())
            .unwrap();
        assert!(map
            .insert(identity(7), DenseIdentityValue::default())
            .unwrap_err()
            .to_string()
            .contains("duplicate identity"));
    }
}
