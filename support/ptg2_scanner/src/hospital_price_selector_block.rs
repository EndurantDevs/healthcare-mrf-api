//! Authenticated lookup pages for normalized hospital-price blocks.

use flate2::{read::ZlibDecoder, write::ZlibEncoder, Compression};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::io::{Read, Write};

pub const HOSPITAL_PRICE_SELECTOR_BLOCK_MAGIC: &[u8; 8] = b"HPTSEL\0\0";
pub const HOSPITAL_PRICE_SELECTOR_BLOCK_VERSION: u32 = 1;
pub const HOSPITAL_PRICE_SELECTOR_BLOCK_HEADER_BYTES: usize = 72;
pub const HOSPITAL_PRICE_SELECTOR_BLOCK_MAX_ROWS: usize = 4_096;
pub const HOSPITAL_PRICE_SELECTOR_BLOCK_MAX_KEY_BYTES: usize = 1024 * 1024;
pub const HOSPITAL_PRICE_SELECTOR_BLOCK_MAX_RAW_BYTES: usize = 4 * 1024 * 1024;
pub const HOSPITAL_PRICE_SELECTOR_BLOCK_MAX_COMPRESSED_BYTES: usize =
    HOSPITAL_PRICE_SELECTOR_BLOCK_MAX_RAW_BYTES + 64 * 1024;

pub type HospitalPriceSelectorBlockResult<T> = Result<T, String>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u32)]
pub enum HospitalPriceSelectorKind {
    CodeToCharge = 1,
    PayerPlanToFact = 2,
}

impl HospitalPriceSelectorKind {
    fn from_u32(value: u32) -> HospitalPriceSelectorBlockResult<Self> {
        match value {
            1 => Ok(Self::CodeToCharge),
            2 => Ok(Self::PayerPlanToFact),
            _ => Err(invalid("kind is unsupported")),
        }
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum HospitalPriceSelectorKey {
    Code {
        code_type: String,
        code: String,
    },
    PayerPlan {
        payer_name: String,
        plan_name: String,
    },
}

impl HospitalPriceSelectorKey {
    pub fn kind(&self) -> HospitalPriceSelectorKind {
        match self {
            Self::Code { .. } => HospitalPriceSelectorKind::CodeToCharge,
            Self::PayerPlan { .. } => HospitalPriceSelectorKind::PayerPlanToFact,
        }
    }
}

pub fn selector_key_sha256(key: &HospitalPriceSelectorKey) -> [u8; 32] {
    let mut digest = Sha256::new();
    match key {
        HospitalPriceSelectorKey::Code { code_type, code } => {
            digest.update(b"code\0");
            digest.update((code_type.len() as u64).to_le_bytes());
            digest.update(code_type.as_bytes());
            digest.update((code.len() as u64).to_le_bytes());
            digest.update(code.as_bytes());
        }
        HospitalPriceSelectorKey::PayerPlan {
            payer_name,
            plan_name,
        } => {
            digest.update(b"payer-plan\0");
            digest.update((payer_name.len() as u64).to_le_bytes());
            digest.update(payer_name.as_bytes());
            digest.update((plan_name.len() as u64).to_le_bytes());
            digest.update(plan_name.as_bytes());
        }
    }
    digest.finalize().into()
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HospitalPriceSelectorEntry {
    pub key: HospitalPriceSelectorKey,
    pub refs: Vec<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HospitalPriceSelectorPage {
    pub kind: HospitalPriceSelectorKind,
    pub page_index: u32,
    pub page_count: u32,
    pub entries: Vec<HospitalPriceSelectorEntry>,
}

impl HospitalPriceSelectorPage {
    pub fn row_count(&self) -> usize {
        self.entries.len()
    }

    pub fn ref_count(&self) -> usize {
        self.entries.iter().map(|entry| entry.refs.len()).sum()
    }

    pub fn exact_refs(&self, key: &HospitalPriceSelectorKey) -> Option<&[u64]> {
        if key.kind() != self.kind {
            return None;
        }
        self.entries
            .binary_search_by(|entry| entry.key.cmp(key))
            .ok()
            .map(|index| self.entries[index].refs.as_slice())
    }
}

fn invalid(message: impl AsRef<str>) -> String {
    format!("hospital price selector block {}", message.as_ref())
}

fn put_u32(output: &mut Vec<u8>, value: u32) {
    output.extend_from_slice(&value.to_le_bytes());
}

fn put_u64(output: &mut Vec<u8>, value: u64) {
    output.extend_from_slice(&value.to_le_bytes());
}

fn checked_add(total: usize, value: usize) -> HospitalPriceSelectorBlockResult<usize> {
    let Some(total) = total.checked_add(value) else {
        return Err(invalid("raw length overflows"));
    };
    Ok(total)
}

fn checked_text_len(value: &str) -> HospitalPriceSelectorBlockResult<usize> {
    if value.len() > HOSPITAL_PRICE_SELECTOR_BLOCK_MAX_KEY_BYTES {
        return Err(invalid("key component exceeds 1 MiB"));
    }
    checked_add(4, value.len())
}

fn put_text(output: &mut Vec<u8>, value: &str) {
    put_u32(output, value.len() as u32);
    output.extend_from_slice(value.as_bytes());
}

pub(crate) fn entry_raw_len(
    entry: &HospitalPriceSelectorEntry,
) -> HospitalPriceSelectorBlockResult<usize> {
    if entry.refs.is_empty() {
        return Err(invalid("selector row has no references"));
    }
    let key_bytes = match &entry.key {
        HospitalPriceSelectorKey::Code { code_type, code } => {
            checked_text_len(code_type)? + checked_text_len(code)?
        }
        HospitalPriceSelectorKey::PayerPlan {
            payer_name,
            plan_name,
        } => checked_text_len(payer_name)? + checked_text_len(plan_name)?,
    };
    let refs_bytes = entry.refs.len() * 8;
    Ok(key_bytes + 4 + refs_bytes)
}

fn canonical_entries(
    kind: HospitalPriceSelectorKind,
    entries: &[HospitalPriceSelectorEntry],
) -> HospitalPriceSelectorBlockResult<Vec<HospitalPriceSelectorEntry>> {
    if entries.is_empty() || entries.len() > HOSPITAL_PRICE_SELECTOR_BLOCK_MAX_ROWS {
        return Err(invalid("row count must be between 1 and 4096"));
    }
    let mut merged = BTreeMap::<HospitalPriceSelectorKey, Vec<u64>>::new();
    let mut input_raw_len = 0usize;
    for entry in entries {
        if entry.key.kind() != kind {
            return Err(invalid("key does not match the selector kind"));
        }
        input_raw_len += entry_raw_len(entry)?;
        if input_raw_len > HOSPITAL_PRICE_SELECTOR_BLOCK_MAX_RAW_BYTES {
            return Err(invalid("input rows exceed the 4 MiB page limit"));
        }
        merged
            .entry(entry.key.clone())
            .or_default()
            .extend_from_slice(&entry.refs);
    }
    let canonical = merged
        .into_iter()
        .map(|(key, mut refs)| {
            refs.sort_unstable();
            refs.dedup();
            HospitalPriceSelectorEntry { key, refs }
        })
        .collect::<Vec<_>>();
    Ok(canonical)
}

fn encode_raw(entries: &[HospitalPriceSelectorEntry]) -> (Vec<u8>, usize) {
    let mut raw_len = 0usize;
    let mut ref_count = 0usize;
    for entry in entries {
        raw_len += entry_raw_len(entry).expect("canonical input rows were prevalidated");
        ref_count += entry.refs.len();
    }
    let mut raw = Vec::with_capacity(raw_len);
    for entry in entries {
        match &entry.key {
            HospitalPriceSelectorKey::Code { code_type, code } => {
                put_text(&mut raw, code_type);
                put_text(&mut raw, code);
            }
            HospitalPriceSelectorKey::PayerPlan {
                payer_name,
                plan_name,
            } => {
                put_text(&mut raw, payer_name);
                put_text(&mut raw, plan_name);
            }
        }
        put_u32(&mut raw, entry.refs.len() as u32);
        for reference in &entry.refs {
            put_u64(&mut raw, *reference);
        }
    }
    (raw, ref_count)
}

fn frame_raw(
    kind: HospitalPriceSelectorKind,
    page_index: u32,
    page_count: u32,
    row_count: usize,
    ref_count: usize,
    raw: &[u8],
) -> HospitalPriceSelectorBlockResult<Vec<u8>> {
    if page_count == 0 || page_index >= page_count {
        return Err(invalid("page index or count is invalid"));
    }
    if row_count == 0 || row_count > HOSPITAL_PRICE_SELECTOR_BLOCK_MAX_ROWS {
        return Err(invalid("row count must be between 1 and 4096"));
    }
    if raw.len() > HOSPITAL_PRICE_SELECTOR_BLOCK_MAX_RAW_BYTES {
        return Err(invalid("raw payload exceeds 4 MiB"));
    }
    let mut encoder = ZlibEncoder::new(Vec::new(), Compression::new(6));
    encoder
        .write_all(raw)
        .expect("writing compressed bytes to Vec cannot fail");
    let compressed = encoder
        .finish()
        .expect("finishing compressed bytes in Vec cannot fail");
    let mut block =
        Vec::with_capacity(HOSPITAL_PRICE_SELECTOR_BLOCK_HEADER_BYTES + compressed.len());
    block.extend_from_slice(HOSPITAL_PRICE_SELECTOR_BLOCK_MAGIC);
    put_u32(&mut block, HOSPITAL_PRICE_SELECTOR_BLOCK_VERSION);
    put_u32(&mut block, kind as u32);
    put_u32(&mut block, row_count as u32);
    put_u32(&mut block, page_index);
    put_u32(&mut block, page_count);
    put_u32(&mut block, ref_count as u32);
    put_u32(&mut block, raw.len() as u32);
    put_u32(&mut block, compressed.len() as u32);
    block.extend_from_slice(&Sha256::digest(raw));
    block.extend_from_slice(&compressed);
    Ok(block)
}

pub fn encode_selector_page(
    kind: HospitalPriceSelectorKind,
    page_index: u32,
    page_count: u32,
    entries: &[HospitalPriceSelectorEntry],
) -> HospitalPriceSelectorBlockResult<Vec<u8>> {
    let entries = canonical_entries(kind, entries)?;
    let (raw, ref_count) = encode_raw(&entries);
    frame_raw(kind, page_index, page_count, entries.len(), ref_count, &raw)
}

fn header_u32(block: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(
        block[offset..offset + 4]
            .try_into()
            .expect("fixed header field"),
    )
}

#[derive(Clone, Copy)]
struct FrameMetadata {
    kind: HospitalPriceSelectorKind,
    row_count: usize,
    page_index: u32,
    page_count: u32,
    ref_count: usize,
}

fn decode_frame(block: &[u8]) -> HospitalPriceSelectorBlockResult<(FrameMetadata, Vec<u8>)> {
    if block.len() < HOSPITAL_PRICE_SELECTOR_BLOCK_HEADER_BYTES {
        return Err(invalid("header is truncated"));
    }
    if &block[..8] != HOSPITAL_PRICE_SELECTOR_BLOCK_MAGIC {
        return Err(invalid("magic is invalid"));
    }
    if header_u32(block, 8) != HOSPITAL_PRICE_SELECTOR_BLOCK_VERSION {
        return Err(invalid("version is unsupported"));
    }
    let kind = HospitalPriceSelectorKind::from_u32(header_u32(block, 12))?;
    let row_count = header_u32(block, 16) as usize;
    if row_count == 0 || row_count > HOSPITAL_PRICE_SELECTOR_BLOCK_MAX_ROWS {
        return Err(invalid("row count is invalid"));
    }
    let page_index = header_u32(block, 20);
    let page_count = header_u32(block, 24);
    if page_count == 0 || page_index >= page_count {
        return Err(invalid("page index or count is invalid"));
    }
    let ref_count = header_u32(block, 28) as usize;
    if ref_count == 0 || ref_count > HOSPITAL_PRICE_SELECTOR_BLOCK_MAX_RAW_BYTES / 8 {
        return Err(invalid("reference count is invalid"));
    }
    let raw_len = header_u32(block, 32) as usize;
    if raw_len > HOSPITAL_PRICE_SELECTOR_BLOCK_MAX_RAW_BYTES {
        return Err(invalid("raw length exceeds 4 MiB"));
    }
    let compressed_len = header_u32(block, 36) as usize;
    if compressed_len > HOSPITAL_PRICE_SELECTOR_BLOCK_MAX_COMPRESSED_BYTES {
        return Err(invalid("compressed length exceeds the byte limit"));
    }
    let expected_len = HOSPITAL_PRICE_SELECTOR_BLOCK_HEADER_BYTES + compressed_len;
    if block.len() < expected_len {
        return Err(invalid("compressed payload is truncated"));
    }
    if block.len() > expected_len {
        return Err(invalid("block has trailing bytes"));
    }
    let compressed = &block[HOSPITAL_PRICE_SELECTOR_BLOCK_HEADER_BYTES..];
    let mut decoder = ZlibDecoder::new(compressed);
    let mut raw = Vec::with_capacity(raw_len);
    {
        let mut bounded = (&mut decoder).take(raw_len as u64 + 1);
        bounded
            .read_to_end(&mut raw)
            .map_err(|error| invalid(format!("decompression failed: {error}")))?;
    }
    if raw.len() != raw_len {
        return Err(invalid("decompressed length does not match the header"));
    }
    if decoder.total_in() != compressed_len as u64 {
        return Err(invalid("zlib stream has trailing bytes"));
    }
    if Sha256::digest(&raw).as_slice() != &block[40..72] {
        return Err(invalid("SHA-256 digest does not match"));
    }
    Ok((
        FrameMetadata {
            kind,
            row_count,
            page_index,
            page_count,
            ref_count,
        },
        raw,
    ))
}

struct SliceCursor<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> SliceCursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    fn take(&mut self, length: usize) -> HospitalPriceSelectorBlockResult<&'a [u8]> {
        let end = self.position.saturating_add(length);
        let value = self
            .bytes
            .get(self.position..end)
            .ok_or_else(|| invalid("raw payload is truncated"))?;
        self.position = end;
        Ok(value)
    }

    fn u32(&mut self) -> HospitalPriceSelectorBlockResult<u32> {
        let bytes = self.take(4)?;
        Ok(u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    fn u64(&mut self) -> HospitalPriceSelectorBlockResult<u64> {
        let bytes = self.take(8)?;
        Ok(u64::from_le_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }

    fn text(&mut self) -> HospitalPriceSelectorBlockResult<&'a str> {
        let length = self.u32()? as usize;
        if length > HOSPITAL_PRICE_SELECTOR_BLOCK_MAX_KEY_BYTES {
            return Err(invalid("key component exceeds 1 MiB"));
        }
        std::str::from_utf8(self.take(length)?).map_err(|_| invalid("key contains invalid UTF-8"))
    }

    fn finish(&self) -> HospitalPriceSelectorBlockResult<()> {
        if self.position == self.bytes.len() {
            Ok(())
        } else {
            Err(invalid("raw payload has trailing bytes"))
        }
    }
}

fn decode_key(
    kind: HospitalPriceSelectorKind,
    cursor: &mut SliceCursor<'_>,
) -> HospitalPriceSelectorBlockResult<HospitalPriceSelectorKey> {
    Ok(match kind {
        HospitalPriceSelectorKind::CodeToCharge => HospitalPriceSelectorKey::Code {
            code_type: cursor.text()?.to_owned(),
            code: cursor.text()?.to_owned(),
        },
        HospitalPriceSelectorKind::PayerPlanToFact => HospitalPriceSelectorKey::PayerPlan {
            payer_name: cursor.text()?.to_owned(),
            plan_name: cursor.text()?.to_owned(),
        },
    })
}

pub fn decode_selector_page(
    block: &[u8],
) -> HospitalPriceSelectorBlockResult<HospitalPriceSelectorPage> {
    let (metadata, raw) = decode_frame(block)?;
    let mut cursor = SliceCursor::new(&raw);
    let mut entries = Vec::with_capacity(metadata.row_count);
    let mut previous_key: Option<HospitalPriceSelectorKey> = None;
    let mut decoded_ref_count = 0usize;
    for _ in 0..metadata.row_count {
        let key = decode_key(metadata.kind, &mut cursor)?;
        if previous_key
            .as_ref()
            .is_some_and(|previous| previous >= &key)
        {
            return Err(invalid("keys are not strictly sorted and unique"));
        }
        let ref_count = cursor.u32()? as usize;
        if ref_count == 0 || ref_count > HOSPITAL_PRICE_SELECTOR_BLOCK_MAX_RAW_BYTES / 8 {
            return Err(invalid("selector row reference count is invalid"));
        }
        decoded_ref_count += ref_count;
        if decoded_ref_count > metadata.ref_count {
            return Err(invalid("reference count exceeds the header"));
        }
        let mut refs = Vec::with_capacity(ref_count);
        let mut previous_ref = None;
        for _ in 0..ref_count {
            let reference = cursor.u64()?;
            if previous_ref.is_some_and(|previous| previous >= reference) {
                return Err(invalid("references are not strictly sorted and unique"));
            }
            refs.push(reference);
            previous_ref = Some(reference);
        }
        previous_key = Some(key.clone());
        entries.push(HospitalPriceSelectorEntry { key, refs });
    }
    cursor.finish()?;
    if decoded_ref_count != metadata.ref_count {
        return Err(invalid("reference count does not match the header"));
    }
    Ok(HospitalPriceSelectorPage {
        kind: metadata.kind,
        page_index: metadata.page_index,
        page_count: metadata.page_count,
        entries,
    })
}

include!("hospital_price_selector_block/tests.rs");
