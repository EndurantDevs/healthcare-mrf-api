//! Columnar direct-stage encoding and PostgreSQL binary COPY emission.
//!
//! Shares canonical scalar normalization, key generation, and payload hashing
//! with ``custom_import_source_columns`` while emitting MessagePack positional
//! v2 slot buffers and per-bucket PostgreSQL binary COPY bodies directly in
//! Rust so Python never constructs per-row dicts or COPY tuples.

use crate::custom_import_source::{source_documents, SourceValue};
use crate::custom_import_source_columns::{
    evaluate_column_row, validate_column_buffers, Column, ColumnLayout, EvaluatedColumnRow,
};
use rayon::{prelude::*, ThreadPool};
use sha2::{Digest, Sha256};

pub const MAX_DIRECT_STAGE_ROWS: usize = 65_536;
pub const MAX_DIRECT_STAGE_INPUT_BYTES: usize = 64 * 1024 * 1024;
pub const MAX_DIRECT_STAGE_OUTPUT_BYTES: usize = 256 * 1024 * 1024;
pub const MAX_DIRECT_STAGE_BUCKETS: usize = 1_024;

const PG_BINARY_COPY_HEADER: &[u8] = b"PGCOPY\n\xff\r\n\0\0\0\0\0\0\0\0\0";
const PG_BINARY_COPY_TRAILER: &[u8] = &[0xff, 0xff];
const DIRECT_BUCKET_COLUMN_COUNT: i16 = 17;
// Offset from start of each 17-column PG binary COPY row to pack_id's 8-byte value:
// 2B field count + 3 * (4B len + 2B int2) + 4B int8 len = 24 bytes.
const PACK_ID_VALUE_OFFSET: usize = 24;
const PARALLEL_ROW_THRESHOLD: usize = 256;
const PACK_HASH_PREFIX: &[u8] = b"custom-import/v1\0candidate-runner/1\0pack\0";

pub struct DirectStageBatchConfig {
    pub record_kind_code: i16,
    pub stream_slot: i16,
    pub collection_slot: i16,
    pub first_source_ordinal: i64,
    pub bucket_count: usize,
    pub page_byte_limit: usize,
    pub maximum_record_bytes: usize,
    pub max_rows: usize,
    pub max_bytes: usize,
    pub has_prior_rows: bool,
}

pub struct DirectStageBucketChunk {
    pub bucket_idx: usize,
    pub row_count: usize,
    pub copy_body: Vec<u8>,
    pub pack_id_offsets: Vec<u8>,
}

pub enum DirectStageBatchResult {
    Prepared {
        taken_rows: usize,
        taken_bytes: usize,
        stopped_by_budget: bool,
        valid_payload_hashes: Vec<u8>,
        buckets: Vec<DirectStageBucketChunk>,
    },
    Fallback,
    Error {
        category: &'static str,
        code: &'static str,
        row_offset: usize,
    },
}

enum EvaluatedDirectRow {
    Accepted {
        bucket_idx: usize,
        byte_count: usize,
        raw_hash: [u8; 32],
        raw_canonical: String,
        typed_hash: [u8; 32],
        typed_canonical: String,
        child_hash: Option<[u8; 32]>,
        child_canonical: Option<String>,
        packed_slots: Vec<u8>,
        payload_hash: [u8; 32],
    },
    Rejected {
        bucket_idx: usize,
        byte_count: usize,
        raw_hash: Option<[u8; 32]>,
        raw_canonical: Option<String>,
        typed_hash: Option<[u8; 32]>,
        typed_canonical: Option<String>,
        initial_code: &'static str,
        rejection_evidence: String,
    },
    Error {
        category: &'static str,
        code: &'static str,
    },
}

fn hex_lower(bytes: &[u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(64);
    for &byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

fn write_msgpack_str(out: &mut Vec<u8>, text: &str) {
    let bytes = text.as_bytes();
    let len = bytes.len();
    if len <= 31 {
        out.push(0xa0 | (len as u8));
    } else if len <= u8::MAX as usize {
        out.push(0xd9);
        out.push(len as u8);
    } else if len <= u16::MAX as usize {
        out.push(0xda);
        out.extend_from_slice(&(len as u16).to_be_bytes());
    } else {
        out.push(0xdb);
        out.extend_from_slice(&(len as u32).to_be_bytes());
    }
    out.extend_from_slice(bytes);
}

fn write_msgpack_i64(out: &mut Vec<u8>, value: i64) {
    if value >= 0 {
        if value <= 127 {
            out.push(value as u8);
        } else if value <= u8::MAX as i64 {
            out.push(0xcc);
            out.push(value as u8);
        } else if value <= u16::MAX as i64 {
            out.push(0xcd);
            out.extend_from_slice(&(value as u16).to_be_bytes());
        } else if value <= u32::MAX as i64 {
            out.push(0xce);
            out.extend_from_slice(&(value as u32).to_be_bytes());
        } else {
            out.push(0xcf);
            out.extend_from_slice(&(value as u64).to_be_bytes());
        }
    } else if value >= -32 {
        out.push((value as i8) as u8);
    } else if value >= i8::MIN as i64 {
        out.push(0xd0);
        out.push((value as i8) as u8);
    } else if value >= i16::MIN as i64 {
        out.push(0xd1);
        out.extend_from_slice(&(value as i16).to_be_bytes());
    } else if value >= i32::MIN as i64 {
        out.push(0xd2);
        out.extend_from_slice(&(value as i32).to_be_bytes());
    } else {
        out.push(0xd3);
        out.extend_from_slice(&value.to_be_bytes());
    }
}

/// Encode positional v2 slot values identically to ``msgpack.packb(slots, use_bin_type=True)``.
pub fn encode_msgpack_slots(cells: &[SourceValue]) -> Vec<u8> {
    let mut out = Vec::with_capacity(cells.len() * 12 + 4);
    let len = cells.len();
    if len <= 15 {
        out.push(0x90 | (len as u8));
    } else if len <= u16::MAX as usize {
        out.push(0xdc);
        out.extend_from_slice(&(len as u16).to_be_bytes());
    } else {
        out.push(0xdd);
        out.extend_from_slice(&(len as u32).to_be_bytes());
    }
    for cell in cells {
        match cell {
            SourceValue::Missing => out.push(0x90), // empty list []
            SourceValue::Null => out.push(0xc0),
            SourceValue::Boolean(false) => out.push(0xc2),
            SourceValue::Boolean(true) => out.push(0xc3),
            SourceValue::Integer(value) => write_msgpack_i64(&mut out, *value),
            SourceValue::Text(text) => write_msgpack_str(&mut out, text),
        }
    }
    out
}

fn bucket_for_partition_key(key: Option<&[u8; 32]>, bucket_count: usize) -> usize {
    match key {
        Some(hash) => (u16::from_be_bytes([hash[0], hash[1]]) as usize) % bucket_count,
        None => 0,
    }
}

fn evaluate_direct_row(
    layout: &ColumnLayout,
    columns: &[Column],
    row: usize,
    cfg: &DirectStageBatchConfig,
) -> Result<EvaluatedDirectRow, &'static str> {
    match evaluate_column_row(layout, columns, row, cfg.page_byte_limit)? {
        EvaluatedColumnRow::Error {
            category,
            code,
            transport,
        } => {
            if transport > cfg.maximum_record_bytes {
                return Ok(EvaluatedDirectRow::Error {
                    category: "capture-error",
                    code: "record_byte_limit",
                });
            }
            Ok(EvaluatedDirectRow::Error { category, code })
        }
        EvaluatedColumnRow::Rejected {
            code,
            raw,
            typed,
            transport,
        } => {
            if transport > cfg.maximum_record_bytes {
                return Ok(EvaluatedDirectRow::Error {
                    category: "capture-error",
                    code: "record_byte_limit",
                });
            }
            let (raw_canonical, raw_hash) = match raw {
                Some((text, hash)) => (Some(text), Some(hash)),
                None => (None, None),
            };
            let (typed_canonical, typed_hash) = match typed {
                Some((text, hash)) => (Some(text), Some(hash)),
                None => (None, None),
            };
            let rejection_evidence = match &typed_hash {
                Some(hash) => format!(
                    r#"{{"code":"{code}","contract":"custom-import-rejection/v1","root_key_sha256":"{}"}}"#,
                    hex_lower(hash)
                ),
                None => format!(
                    r#"{{"code":"{code}","contract":"custom-import-rejection/v1","root_key_sha256":null}}"#
                ),
            };
            // Match family_block_direct_stage.py byte accounting:
            // raw_canonical + typed_canonical + rejection_key (= typed_canonical) + rejection_evidence.
            let typed_len = typed_canonical.as_ref().map_or(0, String::len);
            let byte_count = raw_canonical.as_ref().map_or(0, String::len)
                + typed_len
                + typed_len
                + rejection_evidence.len();
            if byte_count > cfg.page_byte_limit {
                return Ok(EvaluatedDirectRow::Error {
                    category: "preparation-error",
                    code: "occurrence_byte_limit",
                });
            }
            let bucket_idx = bucket_for_partition_key(
                typed_hash.as_ref().or(raw_hash.as_ref()),
                cfg.bucket_count,
            );
            Ok(EvaluatedDirectRow::Rejected {
                bucket_idx,
                byte_count,
                raw_hash,
                raw_canonical,
                typed_hash,
                typed_canonical,
                initial_code: code,
                rejection_evidence,
            })
        }
        EvaluatedColumnRow::Accepted {
            raw,
            cells,
            transport,
        } => {
            if transport > cfg.maximum_record_bytes {
                return Ok(EvaluatedDirectRow::Error {
                    category: "capture-error",
                    code: "record_byte_limit",
                });
            }
            let documents = source_documents(&layout.source, &cells)?;
            let packed_slots = encode_msgpack_slots(&cells);
            let byte_count = raw.0.len()
                + documents.root_key.len()
                + documents.payload.len()
                + documents.child_key.as_ref().map_or(0, String::len)
                + if layout.source.child {
                    documents.root_key.len()
                } else {
                    0
                };
            if byte_count > cfg.page_byte_limit {
                return Ok(EvaluatedDirectRow::Error {
                    category: "preparation-error",
                    code: "occurrence_byte_limit",
                });
            }
            let bucket_idx = bucket_for_partition_key(Some(&documents.root_hash), cfg.bucket_count);
            Ok(EvaluatedDirectRow::Accepted {
                bucket_idx,
                byte_count,
                raw_hash: raw.1,
                raw_canonical: raw.0,
                typed_hash: documents.root_hash,
                typed_canonical: documents.root_key,
                child_hash: documents.child_hash,
                child_canonical: documents.child_key,
                packed_slots,
                payload_hash: documents.payload_hash,
            })
        }
    }
}

#[inline]
fn write_pg_i16(out: &mut Vec<u8>, value: i16) {
    out.extend_from_slice(&2_i32.to_be_bytes());
    out.extend_from_slice(&value.to_be_bytes());
}

#[inline]
fn write_pg_i64(out: &mut Vec<u8>, value: i64) {
    out.extend_from_slice(&8_i32.to_be_bytes());
    out.extend_from_slice(&value.to_be_bytes());
}

#[inline]
fn write_pg_opt_bytes(out: &mut Vec<u8>, value: Option<&[u8]>) {
    match value {
        None => out.extend_from_slice(&(-1_i32).to_be_bytes()),
        Some(bytes) => {
            out.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
            out.extend_from_slice(bytes);
        }
    }
}

#[inline]
fn write_pg_opt_str(out: &mut Vec<u8>, value: Option<&str>) {
    write_pg_opt_bytes(out, value.map(str::as_bytes));
}

fn append_pg_direct_row(
    copy_body: &mut Vec<u8>,
    pack_id_offsets: &mut Vec<u8>,
    cfg: &DirectStageBatchConfig,
    source_ordinal: i64,
    row: &EvaluatedDirectRow,
) -> Result<(), &'static str> {
    let row_start = copy_body.len();
    let pack_id_offset = row_start
        .checked_add(PACK_ID_VALUE_OFFSET)
        .and_then(|v| u32::try_from(v).ok())
        .ok_or("direct stage binary COPY buffer exceeded u32 bounds")?;
    pack_id_offsets.extend_from_slice(&pack_id_offset.to_le_bytes());

    // 17-column header matching DIRECT_BUCKET_COLUMNS in exact order.
    copy_body.extend_from_slice(&DIRECT_BUCKET_COLUMN_COUNT.to_be_bytes());
    write_pg_i16(copy_body, cfg.record_kind_code);
    write_pg_i16(copy_body, cfg.stream_slot);
    write_pg_i16(copy_body, cfg.collection_slot);
    write_pg_i64(copy_body, 0); // pack_id placeholder stamped at commit time
    write_pg_i64(copy_body, source_ordinal);

    match row {
        EvaluatedDirectRow::Accepted {
            raw_hash,
            raw_canonical,
            typed_hash,
            typed_canonical,
            child_hash,
            child_canonical,
            packed_slots,
            payload_hash,
            ..
        } => {
            write_pg_opt_bytes(copy_body, Some(raw_hash));
            write_pg_opt_str(copy_body, Some(raw_canonical));
            write_pg_opt_bytes(copy_body, Some(typed_hash));
            write_pg_opt_str(copy_body, Some(typed_canonical));
            write_pg_opt_bytes(copy_body, child_hash.as_ref().map(|h| h.as_slice()));
            write_pg_opt_str(copy_body, child_canonical.as_deref());
            write_pg_opt_str(copy_body, None); // initial_code
            write_pg_opt_str(copy_body, None); // rejection_key
            write_pg_opt_bytes(copy_body, None); // rejection_hash
            write_pg_opt_str(copy_body, None); // rejection_evidence
            write_pg_opt_bytes(copy_body, Some(packed_slots));
            write_pg_opt_bytes(copy_body, Some(payload_hash));
        }
        EvaluatedDirectRow::Rejected {
            raw_hash,
            raw_canonical,
            typed_hash,
            typed_canonical,
            initial_code,
            rejection_evidence,
            ..
        } => {
            write_pg_opt_bytes(copy_body, raw_hash.as_ref().map(|h| h.as_slice()));
            write_pg_opt_str(copy_body, raw_canonical.as_deref());
            write_pg_opt_bytes(copy_body, typed_hash.as_ref().map(|h| h.as_slice()));
            write_pg_opt_str(copy_body, typed_canonical.as_deref());
            write_pg_opt_bytes(copy_body, None); // child_key_sha256
            write_pg_opt_str(copy_body, None); // child_key_canonical
            write_pg_opt_str(copy_body, Some(initial_code));
            write_pg_opt_str(copy_body, typed_canonical.as_deref()); // rejection_key
            write_pg_opt_bytes(copy_body, typed_hash.as_ref().map(|h| h.as_slice())); // rejection_hash
            write_pg_opt_str(copy_body, Some(rejection_evidence));
            write_pg_opt_bytes(copy_body, None); // packed_slots
            write_pg_opt_bytes(copy_body, None); // payload_sha256
        }
        EvaluatedDirectRow::Error { .. } => unreachable!("error rows are never encoded"),
    }
    Ok(())
}

/// Evaluate an entire Arrow column batch and encode directly into per-bucket PG binary COPY chunks.
pub fn prepare_direct_stage_batch(
    layout: &ColumnLayout,
    columns: &[Column],
    cfg: &DirectStageBatchConfig,
    pool: Option<&ThreadPool>,
) -> Result<DirectStageBatchResult, &'static str> {
    if columns.is_empty() || cfg.bucket_count == 0 || cfg.bucket_count > MAX_DIRECT_STAGE_BUCKETS {
        return Err("direct stage batch configuration is invalid");
    }
    if !validate_column_buffers(columns)? {
        return Ok(DirectStageBatchResult::Fallback);
    }
    let row_count = columns[0].rows;
    if row_count == 0 {
        return Ok(DirectStageBatchResult::Prepared {
            taken_rows: 0,
            taken_bytes: 0,
            stopped_by_budget: false,
            valid_payload_hashes: Vec::new(),
            buckets: Vec::new(),
        });
    }
    // Bound evaluation to at most max_rows (+1 only when max_rows == 0 and !has_prior_rows).
    let eval_limit = if cfg.has_prior_rows {
        row_count.min(cfg.max_rows)
    } else {
        row_count.min(cfg.max_rows.max(1))
    };
    if eval_limit == 0 {
        return Ok(DirectStageBatchResult::Prepared {
            taken_rows: 0,
            taken_bytes: 0,
            stopped_by_budget: true,
            valid_payload_hashes: Vec::new(),
            buckets: Vec::new(),
        });
    }

    let evaluated: Vec<EvaluatedDirectRow> = if eval_limit >= PARALLEL_ROW_THRESHOLD {
        match pool {
            Some(pool) => pool.install(|| {
                (0..eval_limit)
                    .into_par_iter()
                    .map(|row| evaluate_direct_row(layout, columns, row, cfg))
                    .collect::<Vec<_>>()
            }),
            None => (0..eval_limit)
                .into_par_iter()
                .map(|row| evaluate_direct_row(layout, columns, row, cfg))
                .collect::<Vec<_>>(),
        }
    } else {
        (0..eval_limit)
            .map(|row| evaluate_direct_row(layout, columns, row, cfg))
            .collect::<Vec<_>>()
    }
    .into_iter()
    .collect::<Result<Vec<_>, _>>()?;

    let mut bucket_bodies: Vec<Vec<u8>> = (0..cfg.bucket_count).map(|_| Vec::new()).collect();
    let mut bucket_offsets: Vec<Vec<u8>> = (0..cfg.bucket_count).map(|_| Vec::new()).collect();
    let mut bucket_counts: Vec<usize> = vec![0; cfg.bucket_count];
    let mut valid_payload_hashes = Vec::with_capacity(eval_limit * 32);

    let mut taken_rows = 0usize;
    let mut taken_bytes = 0usize;
    let mut total_copy_bytes = 0usize;
    let mut stopped_by_budget = false;

    for (idx, item) in evaluated.into_iter().enumerate() {
        let has_any = cfg.has_prior_rows || taken_rows > 0;
        if has_any && (taken_rows >= cfg.max_rows || taken_bytes >= cfg.max_bytes) {
            stopped_by_budget = true;
            break;
        }
        let (bucket_idx, row_bytes, payload_hash) = match &item {
            EvaluatedDirectRow::Error { category, code } => {
                return Ok(DirectStageBatchResult::Error {
                    category,
                    code,
                    row_offset: idx,
                });
            }
            EvaluatedDirectRow::Accepted {
                bucket_idx,
                byte_count,
                payload_hash,
                ..
            } => (*bucket_idx, *byte_count, Some(*payload_hash)),
            EvaluatedDirectRow::Rejected {
                bucket_idx,
                byte_count,
                ..
            } => (*bucket_idx, *byte_count, None),
        };
        if has_any && taken_bytes.saturating_add(row_bytes) > cfg.max_bytes {
            stopped_by_budget = true;
            break;
        }
        let source_ordinal = cfg
            .first_source_ordinal
            .checked_add(taken_rows as i64)
            .ok_or("direct stage source ordinal overflowed i64")?;
        let before_len = bucket_bodies[bucket_idx].len();
        append_pg_direct_row(
            &mut bucket_bodies[bucket_idx],
            &mut bucket_offsets[bucket_idx],
            cfg,
            source_ordinal,
            &item,
        )?;
        total_copy_bytes += bucket_bodies[bucket_idx].len() - before_len;
        if total_copy_bytes > MAX_DIRECT_STAGE_OUTPUT_BYTES {
            return Err("direct stage binary COPY output byte bound exceeded");
        }
        bucket_counts[bucket_idx] += 1;
        if let Some(hash) = payload_hash {
            valid_payload_hashes.extend_from_slice(&hash);
        }
        taken_rows += 1;
        taken_bytes += row_bytes;
    }

    if !stopped_by_budget && eval_limit < row_count {
        stopped_by_budget = true;
    }

    let mut buckets = Vec::new();
    for bucket_idx in 0..cfg.bucket_count {
        if bucket_counts[bucket_idx] > 0 {
            buckets.push(DirectStageBucketChunk {
                bucket_idx,
                row_count: bucket_counts[bucket_idx],
                copy_body: std::mem::take(&mut bucket_bodies[bucket_idx]),
                pack_id_offsets: std::mem::take(&mut bucket_offsets[bucket_idx]),
            });
        }
    }

    Ok(DirectStageBatchResult::Prepared {
        taken_rows,
        taken_bytes,
        stopped_by_budget,
        valid_payload_hashes,
        buckets,
    })
}

/// Concatenate per-bucket PG binary COPY row chunks, stamp ``pack_id`` in-place,
/// and wrap with the PostgreSQL binary COPY header and trailer.
pub fn finalize_direct_stage_copy(
    chunks: &[(&[u8], &[u8])],
    pack_id: i64,
) -> Result<Vec<u8>, &'static str> {
    let mut body_len = 0usize;
    for (body, offsets) in chunks {
        if !offsets.len().is_multiple_of(4) {
            return Err("direct stage pack_id offset table is misaligned");
        }
        body_len = body_len
            .checked_add(body.len())
            .ok_or("direct stage COPY body length overflowed")?;
        if body_len > MAX_DIRECT_STAGE_OUTPUT_BYTES {
            return Err("direct stage COPY body exceeds output byte bound");
        }
    }
    let total_len = PG_BINARY_COPY_HEADER.len() + body_len + PG_BINARY_COPY_TRAILER.len();
    let mut out = Vec::with_capacity(total_len);
    out.extend_from_slice(PG_BINARY_COPY_HEADER);
    let pack_id_be = pack_id.to_be_bytes();
    for (body, offsets) in chunks {
        let base = out.len();
        out.extend_from_slice(body);
        for &chunk in offsets.as_chunks::<4>().0 {
            let rel = u32::from_le_bytes(chunk) as usize;
            let start = base
                .checked_add(rel)
                .ok_or("direct stage pack_id offset overflowed")?;
            let end = start
                .checked_add(8)
                .ok_or("direct stage pack_id offset overflowed")?;
            if end > out.len() {
                return Err("direct stage pack_id offset out of bounds");
            }
            out[start..end].copy_from_slice(&pack_id_be);
        }
    }
    out.extend_from_slice(PG_BINARY_COPY_TRAILER);
    Ok(out)
}

/// Compute the deterministic ``pack_hash`` over concatenated 32-byte payload hashes.
pub fn compute_pack_hash(label: &str, hash_chunks: &[&[u8]]) -> Result<[u8; 32], &'static str> {
    if !label.is_ascii() {
        return Err("pack hash label must be ASCII");
    }
    let mut total_bytes = 0usize;
    for chunk in hash_chunks {
        if !chunk.len().is_multiple_of(32) {
            return Err("pack hash chunk length must be a multiple of 32");
        }
        total_bytes = total_bytes
            .checked_add(chunk.len())
            .ok_or("pack hash chunk total overflowed")?;
    }
    let mut hashes: Vec<[u8; 32]> = Vec::with_capacity(total_bytes / 32);
    for chunk in hash_chunks {
        hashes.extend_from_slice(chunk.as_chunks::<32>().0);
    }
    hashes.sort_unstable();
    let mut digest = Sha256::new();
    digest.update(PACK_HASH_PREFIX);
    digest.update(label.as_bytes());
    digest.update(b"\0");
    for entry in &hashes {
        digest.update(entry);
    }
    Ok(digest.finalize().into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::custom_import_source::SourceLayout;

    fn utf8_column(values: &[Option<&str>]) -> Column {
        let mut offsets = Vec::with_capacity((values.len() + 1) * 4);
        let mut data = Vec::new();
        let mut validity = vec![0u8; values.len().div_ceil(8)];
        let mut has_null = false;
        offsets.extend_from_slice(&0_i32.to_le_bytes());
        for (idx, val) in values.iter().enumerate() {
            if let Some(text) = val {
                validity[idx / 8] |= 1 << (idx % 8);
                data.extend_from_slice(text.as_bytes());
            } else {
                has_null = true;
            }
            offsets.extend_from_slice(&(data.len() as i32).to_le_bytes());
        }
        Column {
            kind: "utf8".into(),
            bit_offset: 0,
            rows: values.len(),
            precision: 0,
            scale: 0,
            validity: if has_null { Some(validity) } else { None },
            offsets: Some(offsets),
            data,
        }
    }

    fn int64_column(values: &[Option<i64>]) -> Column {
        let mut data = Vec::with_capacity(values.len() * 8);
        let mut validity = vec![0u8; values.len().div_ceil(8)];
        let mut has_null = false;
        for (idx, val) in values.iter().enumerate() {
            if let Some(num) = val {
                validity[idx / 8] |= 1 << (idx % 8);
                data.extend_from_slice(&num.to_le_bytes());
            } else {
                has_null = true;
                data.extend_from_slice(&0_i64.to_le_bytes());
            }
        }
        Column {
            kind: "int64".into(),
            bit_offset: 0,
            rows: values.len(),
            precision: 0,
            scale: 0,
            validity: if has_null { Some(validity) } else { None },
            offsets: None,
            data,
        }
    }

    #[test]
    fn msgpack_slots_match_canonical_encodings() {
        let cells = vec![
            SourceValue::Null,
            SourceValue::Missing,
            SourceValue::Boolean(false),
            SourceValue::Boolean(true),
            SourceValue::Integer(0),
            SourceValue::Integer(127),
            SourceValue::Integer(128),
            SourceValue::Integer(256),
            SourceValue::Integer(65_536),
            SourceValue::Integer(5_000_000_000),
            SourceValue::Integer(-1),
            SourceValue::Integer(-32),
            SourceValue::Integer(-33),
            SourceValue::Integer(-129),
            SourceValue::Integer(-32_769),
            SourceValue::Integer(-5_000_000_000),
            SourceValue::Text("abc".into()),
        ];
        let encoded = encode_msgpack_slots(&cells);
        // 17 elements => array16 (0xdc, 0x00, 0x11)
        assert_eq!(&encoded[0..3], &[0xdc, 0x00, 0x11]);
        assert_eq!(encoded[3], 0xc0); // null
        assert_eq!(encoded[4], 0x90); // missing []
        assert_eq!(encoded[5], 0xc2); // false
        assert_eq!(encoded[6], 0xc3); // true
        assert_eq!(encoded[7], 0x00); // 0
        assert_eq!(encoded[8], 0x7f); // 127
        assert_eq!(&encoded[9..11], &[0xcc, 0x80]); // 128
        assert_eq!(&encoded[11..14], &[0xcd, 0x01, 0x00]); // 256
        assert_eq!(&encoded[14..19], &[0xce, 0x00, 0x01, 0x00, 0x00]); // 65536
        assert_eq!(encoded[19], 0xcf); // u64
        assert_eq!(encoded[28], 0xff); // -1
        assert_eq!(encoded[29], 0xe0); // -32
        assert_eq!(&encoded[30..32], &[0xd0, 0xdf]); // -33
        assert_eq!(&encoded[32..35], &[0xd1, 0xff, 0x7f]); // -129
        assert_eq!(&encoded[35..40], &[0xd2, 0xff, 0xff, 0x7f, 0xff]); // -32769
        assert_eq!(encoded[40], 0xd3); // i64
        assert_eq!(&encoded[49..53], &[0xa3, b'a', b'b', b'c']); // "abc"
    }

    #[test]
    fn direct_stage_batch_and_copy_stamping_roundtrip() {
        let layout = ColumnLayout {
            source: SourceLayout {
                child: false,
                fields: vec![
                    ("npi".into(), "string".into()),
                    ("score".into(), "integer".into()),
                ],
                root_key: vec![("npi".into(), 0)],
                child_key: vec![],
            },
            nullable: vec![false, true],
            projected: vec![true, true],
            entity: Some(0),
        };
        // Row 0 valid NPI, Row 1 invalid NPI (rejected), Row 2 null root key (rejected)
        let columns = vec![
            utf8_column(&[Some("1992999874"), Some("1992999876"), None]),
            int64_column(&[Some(42), Some(7), Some(1)]),
        ];
        let cfg = DirectStageBatchConfig {
            record_kind_code: 0,
            stream_slot: 1,
            collection_slot: 0,
            first_source_ordinal: 10,
            bucket_count: 4,
            page_byte_limit: 1_048_576,
            maximum_record_bytes: 1_048_576,
            max_rows: 100,
            max_bytes: 1_048_576,
            has_prior_rows: false,
        };
        let result = prepare_direct_stage_batch(&layout, &columns, &cfg, None).unwrap();
        let DirectStageBatchResult::Prepared {
            taken_rows,
            stopped_by_budget,
            valid_payload_hashes,
            buckets,
            ..
        } = result
        else {
            panic!("expected Prepared result");
        };
        assert_eq!(taken_rows, 3);
        assert!(!stopped_by_budget);
        assert_eq!(valid_payload_hashes.len(), 32); // only row 0 accepted
        let total_bucket_rows: usize = buckets.iter().map(|b| b.row_count).sum();
        assert_eq!(total_bucket_rows, 3);

        for chunk in &buckets {
            let stamped = finalize_direct_stage_copy(
                &[(&chunk.copy_body, &chunk.pack_id_offsets)],
                987_654_321_i64,
            )
            .unwrap();
            assert!(stamped.starts_with(PG_BINARY_COPY_HEADER));
            assert!(stamped.ends_with(PG_BINARY_COPY_TRAILER));
            // Verify every stamped pack_id value equals 987_654_321
            for &offset_bytes in chunk.pack_id_offsets.as_chunks::<4>().0 {
                let rel = u32::from_le_bytes(offset_bytes) as usize;
                let pos = PG_BINARY_COPY_HEADER.len() + rel;
                let val = i64::from_be_bytes(stamped[pos..pos + 8].try_into().unwrap());
                assert_eq!(val, 987_654_321_i64);
            }
        }
    }

    #[test]
    fn compute_pack_hash_sorts_deterministically() {
        let a = [0x01u8; 32];
        let b = [0x02u8; 32];
        let h1 = compute_pack_hash("root", &[&b, &a]).unwrap();
        let h2 = compute_pack_hash("root", &[&[a, b].concat()]).unwrap();
        assert_eq!(h1, h2);
    }
}
