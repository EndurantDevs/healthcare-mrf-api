use flate2::read::MultiGzDecoder;
use rayon::prelude::*;
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::env;
use std::fmt::Display;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::Path;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use struson::reader::{JsonReader, JsonStreamReader};
use xxhash_rust::xxh3::Xxh3;

const READ_BUF_SIZE: usize = 8 * 1024 * 1024;
const DEFAULT_PROGRESS_BYTES: u64 = 256 * 1024 * 1024;
const DEFAULT_PROGRESS_OBJECTS: u64 = 100_000;
const DEFAULT_SPLIT_NEGOTIATED_RATES: usize = 8;
const DEFAULT_COMPACT_COPY_ROTATE_BYTES: u64 = 512 * 1024 * 1024;

fn to_io_error(error: impl Display) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error.to_string())
}

struct CountingReader<R: Read> {
    inner: R,
    bytes_read: Arc<AtomicU64>,
}

impl<R: Read> CountingReader<R> {
    fn new(inner: R, bytes_read: Arc<AtomicU64>) -> Self {
        Self { inner, bytes_read }
    }
}

impl<R: Read> Read for CountingReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let read = self.inner.read(buf)?;
        if read > 0 {
            self.bytes_read.fetch_add(read as u64, Ordering::Relaxed);
        }
        Ok(read)
    }
}

fn is_gzip(path: &Path) -> io::Result<bool> {
    if path
        .extension()
        .and_then(|value| value.to_str())
        .map(|value| value.eq_ignore_ascii_case("gz"))
        .unwrap_or(false)
    {
        return Ok(true);
    }
    let mut fp = File::open(path)?;
    let mut header = [0u8; 2];
    let read = fp.read(&mut header)?;
    Ok(read == 2 && header == [0x1f, 0x8b])
}

fn open_reader(path: &Path, compressed_bytes_read: Arc<AtomicU64>) -> io::Result<Box<dyn Read>> {
    let fp = File::open(path)?;
    if is_gzip(path)? {
        let compressed_reader = CountingReader::new(BufReader::new(fp), compressed_bytes_read);
        Ok(Box::new(MultiGzDecoder::new(compressed_reader)))
    } else {
        Ok(Box::new(CountingReader::new(
            BufReader::new(fp),
            compressed_bytes_read,
        )))
    }
}

fn emit_object<W: Write>(writer: &mut W, name: &str, payload: &[u8]) -> io::Result<()> {
    writer.write_all(name.as_bytes())?;
    writer.write_all(b"\t")?;
    writer.write_all(payload.len().to_string().as_bytes())?;
    writer.write_all(b"\n")?;
    writer.write_all(payload)?;
    writer.write_all(b"\n")?;
    Ok(())
}

fn split_interval(name: &str, default_value: usize) -> usize {
    env::var(name)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default_value)
}

fn json_string_end(payload: &[u8], start: usize) -> Option<usize> {
    let mut escaped = false;
    let mut index = start + 1;
    while index < payload.len() {
        let byte = payload[index];
        if escaped {
            escaped = false;
        } else if byte == b'\\' {
            escaped = true;
        } else if byte == b'"' {
            return Some(index);
        }
        index += 1;
    }
    None
}

fn skip_json_ws(payload: &[u8], mut index: usize) -> usize {
    while index < payload.len() && payload[index].is_ascii_whitespace() {
        index += 1;
    }
    index
}

fn find_json_array_close(payload: &[u8], array_open: usize) -> Option<usize> {
    let mut depth = 0usize;
    let mut index = array_open;
    while index < payload.len() {
        match payload[index] {
            b'"' => {
                index = json_string_end(payload, index)?;
            }
            b'[' | b'{' => {
                depth += 1;
            }
            b']' | b'}' => {
                depth = depth.checked_sub(1)?;
                if depth == 0 && payload[index] == b']' {
                    return Some(index);
                }
            }
            _ => {}
        }
        index += 1;
    }
    None
}

fn find_negotiated_rates_array(payload: &[u8]) -> Option<(usize, usize)> {
    let mut depth = 0usize;
    let mut index = 0usize;
    while index < payload.len() {
        match payload[index] {
            b'"' => {
                let end = json_string_end(payload, index)?;
                if depth == 1 && &payload[index + 1..end] == b"negotiated_rates" {
                    let mut cursor = skip_json_ws(payload, end + 1);
                    if payload.get(cursor) != Some(&b':') {
                        index = end + 1;
                        continue;
                    }
                    cursor = skip_json_ws(payload, cursor + 1);
                    if payload.get(cursor) != Some(&b'[') {
                        index = end + 1;
                        continue;
                    }
                    let close = find_json_array_close(payload, cursor)?;
                    return Some((cursor + 1, close));
                }
                index = end + 1;
                continue;
            }
            b'{' | b'[' => {
                depth += 1;
            }
            b'}' | b']' => {
                depth = depth.checked_sub(1)?;
            }
            _ => {}
        }
        index += 1;
    }
    None
}

fn top_level_item_ranges(payload: &[u8], start: usize, end: usize) -> Vec<(usize, usize)> {
    let mut ranges = Vec::new();
    let mut depth = 0usize;
    let mut index = start;
    let mut item_start: Option<usize> = None;
    while index < end {
        let byte = payload[index];
        if item_start.is_none() {
            if byte.is_ascii_whitespace() || byte == b',' {
                index += 1;
                continue;
            }
            item_start = Some(index);
        }
        match byte {
            b'"' => {
                if let Some(string_end) = json_string_end(payload, index) {
                    index = string_end;
                } else {
                    break;
                }
            }
            b'{' | b'[' => {
                depth += 1;
            }
            b'}' | b']' => {
                if depth == 0 {
                    break;
                }
                depth -= 1;
            }
            b',' if depth == 0 => {
                if let Some(first) = item_start.take() {
                    let mut last = index;
                    while last > first && payload[last - 1].is_ascii_whitespace() {
                        last -= 1;
                    }
                    if first < last {
                        ranges.push((first, last));
                    }
                }
            }
            _ => {}
        }
        index += 1;
    }
    if let Some(first) = item_start {
        let mut last = end;
        while last > first && payload[last - 1].is_ascii_whitespace() {
            last -= 1;
        }
        if first < last {
            ranges.push((first, last));
        }
    }
    ranges
}

fn emit_in_network_byte_splits<W: Write>(
    writer: &mut W,
    name: &str,
    payload: &[u8],
    negotiated_rate_chunk_size: usize,
) -> io::Result<Option<usize>> {
    let Some((items_start, array_close)) = find_negotiated_rates_array(payload) else {
        return Ok(None);
    };
    let ranges = top_level_item_ranges(payload, items_start, array_close);
    if ranges.len() <= negotiated_rate_chunk_size {
        return Ok(None);
    }

    let mut emitted = 0usize;
    for chunk in ranges.chunks(negotiated_rate_chunk_size) {
        let first = chunk.first().expect("non-empty chunk").0;
        let last = chunk.last().expect("non-empty chunk").1;
        let chunk_len = items_start + (last - first) + (payload.len() - array_close);
        writer.write_all(name.as_bytes())?;
        writer.write_all(b"\t")?;
        writer.write_all(chunk_len.to_string().as_bytes())?;
        writer.write_all(b"\n")?;
        writer.write_all(&payload[..items_start])?;
        writer.write_all(&payload[first..last])?;
        writer.write_all(&payload[array_close..])?;
        writer.write_all(b"\n")?;
        emitted += 1;
    }
    Ok(Some(emitted))
}

fn emit_object_maybe_split<W: Write>(
    writer: &mut W,
    name: &str,
    payload: &[u8],
    split_in_network: bool,
    negotiated_rate_chunk_size: usize,
) -> io::Result<usize> {
    if !split_in_network || name != "in_network" {
        emit_object(writer, name, payload)?;
        return Ok(1);
    }
    if let Some(emitted) =
        emit_in_network_byte_splits(writer, name, payload, negotiated_rate_chunk_size)?
    {
        return Ok(emitted);
    }
    let value: Value = match serde_json::from_slice(payload) {
        Ok(value) => value,
        Err(_) => {
            emit_object(writer, name, payload)?;
            return Ok(1);
        }
    };
    let object = match value.as_object() {
        Some(object) => object,
        None => {
            emit_object(writer, name, payload)?;
            return Ok(1);
        }
    };
    let rates = match object.get("negotiated_rates").and_then(Value::as_array) {
        Some(rates) if rates.len() > negotiated_rate_chunk_size => rates,
        _ => {
            emit_object(writer, name, payload)?;
            return Ok(1);
        }
    };
    let mut emitted = 0usize;
    for chunk in rates.chunks(negotiated_rate_chunk_size) {
        let mut chunked = object.clone();
        chunked.insert("negotiated_rates".to_string(), Value::Array(chunk.to_vec()));
        let bytes = serde_json::to_vec(&Value::Object(chunked))?;
        emit_object(writer, name, &bytes)?;
        emitted += 1;
    }
    Ok(emitted)
}

fn progress_interval(name: &str, default_value: u64) -> u64 {
    env::var(name)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(default_value)
}

fn emit_progress(
    path: &Path,
    total_bytes: u64,
    compressed_bytes_read: &Arc<AtomicU64>,
    object_counts: &HashMap<String, u64>,
    done: bool,
) {
    let bytes_read = compressed_bytes_read
        .load(Ordering::Relaxed)
        .min(total_bytes);
    let percent = if total_bytes > 0 {
        (bytes_read as f64 / total_bytes as f64) * 100.0
    } else {
        0.0
    };
    let total_objects: u64 = object_counts.values().sum();
    let counts = object_counts
        .iter()
        .map(|(name, count)| format!("{}={}", name, count))
        .collect::<Vec<_>>()
        .join("\t");
    eprintln!(
        "PTG2_SCANNER_PROGRESS\tpath={}\tcompressed_bytes={}\ttotal_bytes={}\tpercent={:.2}\tobjects={}\t{}\tdone={}",
        path.display(),
        bytes_read,
        total_bytes,
        percent,
        total_objects,
        counts,
        if done { "true" } else { "false" },
    );
}

fn scan(path: &Path, requested: &[String], split_in_network: bool) -> io::Result<()> {
    let mut targets: HashMap<Vec<u8>, String> = HashMap::new();
    for name in requested {
        targets.insert(name.as_bytes().to_vec(), name.clone());
    }

    let total_bytes = path.metadata().map(|metadata| metadata.len()).unwrap_or(0);
    let compressed_bytes_read = Arc::new(AtomicU64::new(0));
    let mut reader = open_reader(path, Arc::clone(&compressed_bytes_read))?;
    let stdout = io::stdout();
    let mut writer = BufWriter::new(stdout.lock());
    let mut buffer = vec![0u8; READ_BUF_SIZE];
    let progress_bytes_interval = progress_interval(
        "HLTHPRT_PTG2_SCANNER_PROGRESS_BYTES",
        DEFAULT_PROGRESS_BYTES,
    );
    let progress_objects_interval = progress_interval(
        "HLTHPRT_PTG2_SCANNER_PROGRESS_OBJECTS",
        DEFAULT_PROGRESS_OBJECTS,
    );
    let mut next_progress_bytes = progress_bytes_interval;
    let mut next_progress_objects = progress_objects_interval;
    let mut object_counts: HashMap<String, u64> = HashMap::new();
    let negotiated_rate_chunk_size = split_interval(
        "HLTHPRT_PTG2_RUST_SPLIT_NEGOTIATED_RATES",
        DEFAULT_SPLIT_NEGOTIATED_RATES,
    );

    let mut depth: usize = 0;
    let mut active_name: Option<String> = None;
    let mut active_array_depth: usize = 0;
    let mut capture: Vec<u8> = Vec::new();
    let mut capture_depth: usize = 0;
    let mut in_string = false;
    let mut escape = false;
    let mut string_buffer: Option<Vec<u8>> = None;
    let mut candidate_key: Option<Vec<u8>> = None;
    let mut pending_key: Option<Vec<u8>> = None;

    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        for &byte in &buffer[..read] {
            if capture_depth > 0 {
                capture.push(byte);
            }

            if in_string {
                if let Some(ref mut string_buf) = string_buffer {
                    string_buf.push(byte);
                }
                if escape {
                    escape = false;
                } else if byte == b'\\' {
                    escape = true;
                } else if byte == b'"' {
                    in_string = false;
                    if let Some(mut string_buf) = string_buffer.take() {
                        if string_buf.last() == Some(&b'"') {
                            string_buf.pop();
                        }
                        candidate_key = Some(string_buf);
                    }
                }
                continue;
            }

            if byte == b'"' {
                in_string = true;
                escape = false;
                if depth == 1 && active_name.is_none() && capture_depth == 0 {
                    string_buffer = Some(Vec::new());
                } else {
                    string_buffer = None;
                }
                continue;
            }

            if let Some(candidate) = candidate_key.take() {
                if byte.is_ascii_whitespace() {
                    candidate_key = Some(candidate);
                    continue;
                }
                if byte == b':' {
                    pending_key = Some(candidate);
                    continue;
                }
            }

            if let Some(pending) = pending_key.take() {
                if byte.is_ascii_whitespace() {
                    pending_key = Some(pending);
                    continue;
                }
                if byte == b'[' && targets.contains_key(&pending) && depth == 1 {
                    depth += 1;
                    active_name = targets.get(&pending).cloned();
                    active_array_depth = depth;
                    continue;
                }
            }

            if active_name.is_some()
                && capture_depth == 0
                && byte == b'{'
                && depth == active_array_depth
            {
                capture.clear();
                capture.push(b'{');
                capture_depth = 1;
                depth += 1;
                continue;
            }

            match byte {
                b'{' | b'[' => {
                    if capture_depth > 0 {
                        capture_depth += 1;
                    }
                    depth += 1;
                }
                b'}' | b']' => {
                    if capture_depth > 0 {
                        capture_depth -= 1;
                        if capture_depth == 0 {
                            if let Some(ref name) = active_name {
                                let emitted = emit_object_maybe_split(
                                    &mut writer,
                                    name,
                                    &capture,
                                    split_in_network,
                                    negotiated_rate_chunk_size,
                                )?;
                                let count = object_counts.entry(name.clone()).or_insert(0);
                                *count += emitted as u64;
                                let objects: u64 = object_counts.values().sum();
                                let bytes = compressed_bytes_read.load(Ordering::Relaxed);
                                if progress_bytes_interval > 0 && bytes >= next_progress_bytes {
                                    emit_progress(
                                        path,
                                        total_bytes,
                                        &compressed_bytes_read,
                                        &object_counts,
                                        false,
                                    );
                                    while bytes >= next_progress_bytes {
                                        next_progress_bytes += progress_bytes_interval;
                                    }
                                } else if progress_objects_interval > 0
                                    && objects >= next_progress_objects
                                {
                                    emit_progress(
                                        path,
                                        total_bytes,
                                        &compressed_bytes_read,
                                        &object_counts,
                                        false,
                                    );
                                    while objects >= next_progress_objects {
                                        next_progress_objects += progress_objects_interval;
                                    }
                                }
                            }
                            capture.clear();
                            depth = depth.saturating_sub(1);
                            continue;
                        }
                    }
                    if active_name.is_some() && byte == b']' && depth == active_array_depth {
                        active_name = None;
                        active_array_depth = 0;
                    }
                    depth = depth.saturating_sub(1);
                }
                _ => {}
            }
        }
    }
    writer.flush()?;
    emit_progress(
        path,
        total_bytes,
        &compressed_bytes_read,
        &object_counts,
        true,
    );
    Ok(())
}

#[derive(Clone, Debug)]
struct ProviderEntry {
    entry_hash: i64,
    provider_count: i64,
    provider_group_hashes: Vec<i64>,
}

#[derive(Clone, Debug)]
struct RateLite {
    provider_refs: Vec<String>,
    prices: Vec<Value>,
}

fn canonical_value(value: &Value) -> Value {
    match value {
        Value::Array(items) => Value::Array(items.iter().map(canonical_value).collect()),
        Value::Object(map) => {
            let mut sorted = Map::new();
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            for key in keys {
                sorted.insert(key.clone(), canonical_value(&map[key]));
            }
            Value::Object(sorted)
        }
        _ => value.clone(),
    }
}

fn canonical_json(value: &Value) -> String {
    serde_json::to_string(&canonical_value(value)).unwrap_or_else(|_| "null".to_string())
}

fn sha63_hex_from_json(value: &Value) -> String {
    let value = xxh3_63(canonical_json(value).as_bytes());
    format!("{:016x}", value)
}

fn make_checksum(values: Vec<Value>) -> i64 {
    xxh3_63(canonical_json(&Value::Array(values)).as_bytes()) as i64
}

fn semantic_hash(domain: &str, payload: Value) -> String {
    sha63_hex_from_json(&json!({"domain": domain, "payload": payload}))
}

fn hash_text(domain: &str, parts: &[String]) -> String {
    let mut hasher = Xxh3::new();
    hasher.update(domain.as_bytes());
    for part in parts {
        let bytes = part.as_bytes();
        hasher.update(b"\x1f");
        hasher.update(bytes.len().to_string().as_bytes());
        hasher.update(b":");
        hasher.update(bytes);
    }
    format!("{:016x}", hasher.digest() & ((1u64 << 63) - 1))
}

fn xxh3_63(bytes: &[u8]) -> u64 {
    let mut hasher = Xxh3::new();
    hasher.update(bytes);
    hasher.digest() & ((1u64 << 63) - 1)
}

fn normalize_string(value: Option<&Value>) -> Option<String> {
    match value {
        Some(Value::String(text)) => {
            let trimmed = text.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_string())
            }
        }
        Some(Value::Number(number)) => Some(number.to_string()),
        Some(Value::Bool(value)) => Some(value.to_string()),
        _ => None,
    }
}

fn normalize_code(value: Option<&Value>) -> Option<String> {
    normalize_string(value).map(|value| value.to_uppercase())
}

fn normalize_tin_type(value: Option<&Value>) -> String {
    normalize_string(value)
        .unwrap_or_default()
        .trim()
        .to_lowercase()
}

fn normalize_tin_value(value: Option<&Value>) -> String {
    normalize_string(value)
        .unwrap_or_default()
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric())
        .collect::<String>()
        .to_uppercase()
}

fn int_list(value: Option<&Value>) -> Vec<i64> {
    let mut out = Vec::new();
    match value {
        Some(Value::Array(items)) => {
            for item in items {
                if let Some(text) = normalize_string(Some(item)) {
                    if let Ok(number) = text.trim().parse::<i64>() {
                        out.push(number);
                    }
                }
            }
        }
        Some(item) => {
            if let Some(text) = normalize_string(Some(item)) {
                if let Ok(number) = text.trim().parse::<i64>() {
                    out.push(number);
                }
            }
        }
        None => {}
    }
    out.sort_unstable();
    out.dedup();
    out
}

fn normalize_money_value(value: Option<&Value>) -> Option<String> {
    let mut text = normalize_string(value)?;
    if text.contains('.') {
        while text.ends_with('0') {
            text.pop();
        }
        if text.ends_with('.') {
            text.pop();
        }
    }
    if text.is_empty() {
        None
    } else {
        Some(text)
    }
}

fn provider_group_hash(tin: &Value, npi: &[i64]) -> i64 {
    make_checksum(vec![
        json!("provider_group"),
        json!(normalize_tin_type(tin.get("type"))),
        json!(normalize_tin_value(tin.get("value"))),
        json!(npi),
    ])
}

fn build_provider_entry(provider_ref: &Value) -> Option<ProviderEntry> {
    let groups = provider_ref.get("provider_groups")?.as_array()?;
    let mut group_payloads: Vec<Value> = Vec::new();
    let mut group_hashes: Vec<i64> = Vec::new();
    let mut provider_count = 0i64;
    for group in groups {
        let tin = group.get("tin").unwrap_or(&Value::Null);
        let npi = int_list(group.get("npi"));
        let group_hash = provider_group_hash(tin, &npi);
        provider_count += npi.len() as i64;
        group_hashes.push(group_hash);
        group_payloads.push(json!({
            "provider_group_hash": group_hash,
            "tin_type": normalize_tin_type(tin.get("type")),
            "tin_value": normalize_tin_value(tin.get("value")),
            "npi": npi,
        }));
    }
    if group_payloads.is_empty() {
        return None;
    }
    group_payloads.sort_by_key(canonical_json);
    group_hashes.sort_unstable();
    group_hashes.dedup();
    let entry_hash = if group_payloads.len() == 1 {
        group_payloads[0]
            .get("provider_group_hash")
            .and_then(Value::as_i64)
            .unwrap_or(0)
    } else {
        make_checksum(vec![json!("provider_set"), Value::Array(group_payloads)])
    };
    Some(ProviderEntry {
        entry_hash,
        provider_count,
        provider_group_hashes: group_hashes,
    })
}

fn provider_ref_key(value: &Value) -> Option<String> {
    normalize_string(Some(value))
}

fn provider_set_from_refs(
    provider_map: &HashMap<String, ProviderEntry>,
    refs: &[Value],
) -> Option<ProviderEntry> {
    let keys: Vec<String> = refs.iter().filter_map(provider_ref_key).collect();
    provider_set_from_ref_keys(provider_map, &keys)
}

fn provider_set_from_ref_keys(
    provider_map: &HashMap<String, ProviderEntry>,
    refs: &[String],
) -> Option<ProviderEntry> {
    let mut entry_hashes: HashSet<i64> = HashSet::new();
    let mut group_hashes: HashSet<i64> = HashSet::new();
    let mut provider_count = 0i64;
    for key in refs {
        let entry = provider_map.get(key)?;
        if entry_hashes.insert(entry.entry_hash) {
            provider_count += entry.provider_count;
            for group_hash in &entry.provider_group_hashes {
                group_hashes.insert(*group_hash);
            }
        }
    }
    if entry_hashes.is_empty() {
        return None;
    }
    let mut sorted_entry_hashes: Vec<i64> = entry_hashes.into_iter().collect();
    sorted_entry_hashes.sort_unstable();
    let mut sorted_group_hashes: Vec<i64> = group_hashes.into_iter().collect();
    sorted_group_hashes.sort_unstable();
    let entry_hash = if sorted_entry_hashes.len() == 1 {
        sorted_entry_hashes[0]
    } else {
        make_checksum(vec![
            json!("provider_rate_provider_set"),
            json!(sorted_entry_hashes),
        ])
    };
    Some(ProviderEntry {
        entry_hash,
        provider_count,
        provider_group_hashes: sorted_group_hashes,
    })
}

fn price_key_and_payload(prices: &[Value]) -> Option<(String, Value)> {
    let mut key_parts: Vec<Value> = Vec::new();
    let mut payload: Vec<Value> = Vec::new();
    for price in prices {
        let rate = normalize_money_value(price.get("negotiated_rate"))?;
        let rate_value = rate
            .parse::<i64>()
            .map(Value::from)
            .or_else(|_| {
                rate.parse::<f64>()
                    .ok()
                    .filter(|value| value.is_finite())
                    .and_then(serde_json::Number::from_f64)
                    .map(Value::Number)
                    .ok_or(())
            })
            .unwrap_or_else(|_| Value::String(rate.clone()));
        let service_code = price
            .get("service_code")
            .cloned()
            .unwrap_or_else(|| json!([]));
        let modifiers = price
            .get("billing_code_modifier")
            .cloned()
            .unwrap_or_else(|| json!([]));
        let normalized = json!({
            "negotiated_type": price.get("negotiated_type").cloned().unwrap_or(Value::Null),
            "negotiated_rate": rate_value,
            "expiration_date": normalize_string(price.get("expiration_date")),
            "service_code": service_code,
            "billing_class": price.get("billing_class").cloned().unwrap_or(Value::Null),
            "setting": price.get("setting").cloned().unwrap_or(Value::Null),
            "billing_code_modifier": modifiers,
            "additional_information": price.get("additional_information").cloned().unwrap_or(Value::Null),
        });
        payload.push(normalized);
        key_parts.push(json!([
            price.get("negotiated_type").cloned().unwrap_or(Value::Null),
            rate,
            normalize_string(price.get("expiration_date")),
            price
                .get("service_code")
                .cloned()
                .unwrap_or_else(|| json!([])),
            price.get("billing_class").cloned().unwrap_or(Value::Null),
            price.get("setting").cloned().unwrap_or(Value::Null),
            price
                .get("billing_code_modifier")
                .cloned()
                .unwrap_or_else(|| json!([])),
            price
                .get("additional_information")
                .cloned()
                .unwrap_or(Value::Null),
        ]));
    }
    if payload.is_empty() {
        return None;
    }
    key_parts.sort_by_key(canonical_json);
    let price_set_hash = hash_text(
        "serving_price_set",
        &key_parts.iter().map(canonical_json).collect::<Vec<_>>(),
    );
    Some((price_set_hash, Value::Array(payload)))
}

fn emit_json_record<W: Write>(writer: &mut W, kind: &str, row: &Value) -> io::Result<()> {
    let payload = serde_json::to_vec(row)?;
    emit_raw_record(writer, kind, &payload)
}

fn emit_raw_record<W: Write>(writer: &mut W, kind: &str, payload: &[u8]) -> io::Result<()> {
    writer.write_all(kind.as_bytes())?;
    writer.write_all(b"\t")?;
    writer.write_all(payload.len().to_string().as_bytes())?;
    writer.write_all(b"\n")?;
    writer.write_all(&payload)?;
    writer.write_all(b"\n")?;
    Ok(())
}

struct CompactCopySink {
    base_path: Option<String>,
    record_kind: String,
    writer: Option<BufWriter<File>>,
    buffer: Vec<u8>,
    chunk_index: u64,
    rotate_bytes: u64,
    stdout_mode: bool,
}

impl CompactCopySink {
    fn new_file(base_path: String, rotate_bytes: u64) -> io::Result<Self> {
        Ok(Self {
            writer: Some(Self::open_writer(&base_path)?),
            base_path: Some(base_path),
            record_kind: "compact_copy_file".to_string(),
            buffer: Vec::new(),
            chunk_index: 0,
            rotate_bytes,
            stdout_mode: false,
        })
    }

    fn new_named_file(base_path: String, rotate_bytes: u64, record_kind: &str) -> io::Result<Self> {
        Ok(Self {
            writer: Some(Self::open_writer(&base_path)?),
            base_path: Some(base_path),
            record_kind: record_kind.to_string(),
            buffer: Vec::new(),
            chunk_index: 0,
            rotate_bytes,
            stdout_mode: false,
        })
    }

    fn new_stdout(rotate_bytes: u64) -> io::Result<Self> {
        Ok(Self {
            writer: None,
            base_path: None,
            record_kind: "compact_copy_data".to_string(),
            buffer: Vec::with_capacity(rotate_bytes.min(32 * 1024 * 1024) as usize),
            chunk_index: 0,
            rotate_bytes,
            stdout_mode: true,
        })
    }

    fn open_writer(path: &str) -> io::Result<BufWriter<File>> {
        Ok(BufWriter::new(
            OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(path)?,
        ))
    }

    fn write_row(
        &mut self,
        serving_rate_id: &str,
        snapshot_id: &str,
        plan_id: &str,
        plan_month_id: &str,
        procedure_hash: &str,
        procedure_code: Option<i64>,
        reported_code_system: Option<&str>,
        reported_code: Option<&str>,
        billing_code: &str,
        billing_code_type: &str,
        rate_pack_hash: &str,
        provider_set_hash: &str,
        provider_count: i64,
        price_set_hash: &str,
        source_trace_set_hash: &str,
        confidence_code: &str,
    ) -> io::Result<()> {
        if self.stdout_mode {
            return emit_compact_copy_row(
                &mut self.buffer,
                serving_rate_id,
                snapshot_id,
                plan_id,
                plan_month_id,
                procedure_hash,
                procedure_code,
                reported_code_system,
                reported_code,
                billing_code,
                billing_code_type,
                rate_pack_hash,
                provider_set_hash,
                provider_count,
                price_set_hash,
                source_trace_set_hash,
                confidence_code,
            );
        }
        let writer = self.writer.as_mut().ok_or_else(|| {
            io::Error::new(io::ErrorKind::BrokenPipe, "compact copy writer is closed")
        })?;
        emit_compact_copy_row(
            writer,
            serving_rate_id,
            snapshot_id,
            plan_id,
            plan_month_id,
            procedure_hash,
            procedure_code,
            reported_code_system,
            reported_code,
            billing_code,
            billing_code_type,
            rate_pack_hash,
            provider_set_hash,
            provider_count,
            price_set_hash,
            source_trace_set_hash,
            confidence_code,
        )
    }

    fn maybe_rotate<W: Write>(&mut self, stdout: &mut W) -> io::Result<()> {
        if self.rotate_bytes == 0 {
            return Ok(());
        }
        if self.stdout_mode {
            if self.buffer.len() < self.rotate_bytes as usize {
                return Ok(());
            }
            emit_raw_record(stdout, "compact_copy_data", &self.buffer)?;
            self.buffer.clear();
            self.chunk_index += 1;
            stdout.flush()?;
            return Ok(());
        }
        let writer = self.writer.as_mut().ok_or_else(|| {
            io::Error::new(io::ErrorKind::BrokenPipe, "compact copy writer is closed")
        })?;
        writer.flush()?;
        let bytes = writer.get_ref().metadata()?.len();
        if bytes < self.rotate_bytes {
            return Ok(());
        }
        let base_path = self.base_path.as_ref().ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "compact copy path is not configured")
        })?;
        let ready_path = format!("{}.part{:06}.ready", base_path, self.chunk_index);
        self.chunk_index += 1;
        let old_writer = self.writer.take();
        drop(old_writer);
        std::fs::rename(base_path, &ready_path)?;
        self.writer = Some(Self::open_writer(base_path)?);
        emit_json_record(
            stdout,
            &self.record_kind,
            &json!({
                "path": ready_path,
                "bytes": bytes,
                "final": false,
            }),
        )?;
        stdout.flush()?;
        Ok(())
    }

    fn finish<W: Write>(mut self, stdout: &mut W) -> io::Result<()> {
        if self.stdout_mode {
            if !self.buffer.is_empty() {
                emit_raw_record(stdout, "compact_copy_data", &self.buffer)?;
                self.buffer.clear();
                stdout.flush()?;
            }
            return Ok(());
        }
        let mut writer = self.writer.take().ok_or_else(|| {
            io::Error::new(io::ErrorKind::BrokenPipe, "compact copy writer is closed")
        })?;
        writer.flush()?;
        let bytes = writer.get_ref().metadata()?.len();
        drop(writer);
        if bytes > 0 {
            emit_json_record(
                stdout,
                &self.record_kind,
                &json!({
                    "path": self.base_path.unwrap_or_default(),
                    "bytes": bytes,
                    "final": true,
                }),
            )?;
            stdout.flush()?;
        }
        Ok(())
    }
}

struct DictionaryCopySinks {
    procedure: Option<CompactCopySink>,
    price_set: Option<CompactCopySink>,
    provider_set: Option<CompactCopySink>,
    provider_group_member: Option<CompactCopySink>,
}

impl DictionaryCopySinks {
    fn from_env(rotate_bytes: u64) -> io::Result<Self> {
        Ok(Self {
            procedure: Self::sink_from_env(
                "HLTHPRT_PTG2_PROCEDURE_COPY_PATH",
                rotate_bytes,
                "procedure_copy_file",
            )?,
            price_set: Self::sink_from_env(
                "HLTHPRT_PTG2_PRICE_SET_COPY_PATH",
                rotate_bytes,
                "price_set_copy_file",
            )?,
            provider_set: Self::sink_from_env(
                "HLTHPRT_PTG2_PROVIDER_SET_COPY_PATH",
                rotate_bytes,
                "provider_set_copy_file",
            )?,
            provider_group_member: Self::sink_from_env(
                "HLTHPRT_PTG2_PROVIDER_GROUP_MEMBER_COPY_PATH",
                rotate_bytes,
                "provider_group_member_copy_file",
            )?,
        })
    }

    fn sink_from_env(name: &str, rotate_bytes: u64, record_kind: &str) -> io::Result<Option<CompactCopySink>> {
        match env::var(name) {
            Ok(copy_path) if !copy_path.trim().is_empty() => {
                Ok(Some(CompactCopySink::new_named_file(copy_path, rotate_bytes, record_kind)?))
            }
            _ => Ok(None),
        }
    }

    fn maybe_rotate<W: Write>(&mut self, writer: &mut W) -> io::Result<()> {
        if let Some(sink) = self.procedure.as_mut() {
            sink.maybe_rotate(writer)?;
        }
        if let Some(sink) = self.price_set.as_mut() {
            sink.maybe_rotate(writer)?;
        }
        if let Some(sink) = self.provider_set.as_mut() {
            sink.maybe_rotate(writer)?;
        }
        if let Some(sink) = self.provider_group_member.as_mut() {
            sink.maybe_rotate(writer)?;
        }
        Ok(())
    }

    fn finish<W: Write>(mut self, writer: &mut W) -> io::Result<()> {
        if let Some(sink) = self.procedure.take() {
            sink.finish(writer)?;
        }
        if let Some(sink) = self.price_set.take() {
            sink.finish(writer)?;
        }
        if let Some(sink) = self.provider_set.take() {
            sink.finish(writer)?;
        }
        if let Some(sink) = self.provider_group_member.take() {
            sink.finish(writer)?;
        }
        Ok(())
    }

    fn write_procedure(
        &mut self,
        procedure_hash: &str,
        procedure_value: &Value,
        procedure_payload: &Value,
    ) -> io::Result<bool> {
        let Some(sink) = self.procedure.as_mut() else {
            return Ok(false);
        };
        let fields = [
            pg_text_copy_field(Some(procedure_hash)),
            pg_text_copy_field(Some(&procedure_hash[..procedure_hash.len().min(16)])),
            pg_text_copy_field(normalize_string(procedure_value.get("billing_code_type")).as_deref()),
            pg_text_copy_field(normalize_string(procedure_value.get("billing_code_type_version")).as_deref()),
            pg_text_copy_field(normalize_string(procedure_value.get("billing_code")).as_deref()),
            pg_text_copy_field(normalize_string(procedure_value.get("name")).as_deref()),
            pg_text_copy_field(normalize_string(procedure_value.get("description")).as_deref()),
            pg_json_copy_field(procedure_payload),
        ];
        let writer = sink.writer.as_mut().ok_or_else(|| {
            io::Error::new(io::ErrorKind::BrokenPipe, "procedure copy writer is closed")
        })?;
        write_copy_fields(writer, &fields)?;
        Ok(true)
    }

    fn write_price_set(
        &mut self,
        price_set_hash: &str,
        price_payload: &Value,
    ) -> io::Result<bool> {
        let Some(sink) = self.price_set.as_mut() else {
            return Ok(false);
        };
        let single_price = price_payload.as_array().and_then(|items| {
            if items.len() == 1 {
                items[0].as_object()
            } else {
                None
            }
        });
        let negotiated_rate = single_price
            .and_then(|price| price.get("negotiated_rate"))
            .and_then(json_scalar_to_string);
        let canonical_payload = if single_price.is_some() {
            pg_text_copy_field(None)
        } else {
            pg_json_copy_field(price_payload)
        };
        let fields = [
            pg_text_copy_field(Some(price_set_hash)),
            pg_text_copy_field(Some(&price_set_hash[..price_set_hash.len().min(16)])),
            pg_empty_text_array_field(),
            pg_text_copy_field(single_price.and_then(|price| normalize_string(price.get("negotiated_type"))).as_deref()),
            pg_text_copy_field(negotiated_rate.as_deref()),
            pg_text_copy_field(single_price.and_then(|price| normalize_string(price.get("expiration_date"))).as_deref()),
            pg_text_array_value_field(single_price.and_then(|price| price.get("service_code"))),
            pg_text_copy_field(single_price.and_then(|price| normalize_string(price.get("billing_class"))).as_deref()),
            pg_text_copy_field(single_price.and_then(|price| normalize_string(price.get("setting"))).as_deref()),
            pg_text_array_value_field(single_price.and_then(|price| price.get("billing_code_modifier"))),
            pg_text_copy_field(single_price.and_then(|price| normalize_string(price.get("additional_information"))).as_deref()),
            canonical_payload,
        ];
        let writer = sink.writer.as_mut().ok_or_else(|| {
            io::Error::new(io::ErrorKind::BrokenPipe, "price set copy writer is closed")
        })?;
        write_copy_fields(writer, &fields)?;
        Ok(true)
    }

    fn write_provider_set(
        &mut self,
        provider_set_hash: &str,
        provider_count: i64,
        sorted_provider_hashes: &[i64],
    ) -> io::Result<bool> {
        let Some(sink) = self.provider_set.as_mut() else {
            return Ok(false);
        };
        let provider_count_text = provider_count.to_string();
        let fields = [
            pg_text_copy_field(Some(provider_set_hash)),
            pg_text_copy_field(Some(&provider_set_hash[..provider_set_hash.len().min(16)])),
            pg_text_copy_field(Some(&provider_count_text)),
            pg_text_copy_field(None),
            pg_i64_array_field(sorted_provider_hashes),
            pg_text_copy_field(Some("set")),
            pg_text_copy_field(None),
            pg_text_copy_field(None),
        ];
        let writer = sink.writer.as_mut().ok_or_else(|| {
            io::Error::new(io::ErrorKind::BrokenPipe, "provider set copy writer is closed")
        })?;
        write_copy_fields(writer, &fields)?;
        Ok(true)
    }

    fn write_provider_group_members(
        &mut self,
        provider_ref: &Value,
        emitted_members: &mut HashSet<(i64, i64)>,
    ) -> io::Result<()> {
        let Some(sink) = self.provider_group_member.as_mut() else {
            return Ok(());
        };
        let Some(groups) = provider_ref.get("provider_groups").and_then(Value::as_array) else {
            return Ok(());
        };
        let writer = sink.writer.as_mut().ok_or_else(|| {
            io::Error::new(io::ErrorKind::BrokenPipe, "provider group member copy writer is closed")
        })?;
        for group in groups {
            let tin = group.get("tin").unwrap_or(&Value::Null);
            let npi = int_list(group.get("npi"));
            let group_hash = provider_group_hash(tin, &npi);
            for (ordinal, npi_value) in npi.iter().enumerate() {
                if !emitted_members.insert((group_hash, *npi_value)) {
                    continue;
                }
                let group_hash_text = group_hash.to_string();
                let npi_text = npi_value.to_string();
                let ordinal_text = (ordinal + 1).to_string();
                let fields = [
                    pg_text_copy_field(Some(&group_hash_text)),
                    pg_text_copy_field(Some(&npi_text)),
                    pg_text_copy_field(Some(&ordinal_text)),
                ];
                write_copy_fields(writer, &fields)?;
            }
        }
        Ok(())
    }
}

fn pg_text_copy_field(value: Option<&str>) -> String {
    match value {
        None => "\\N".to_string(),
        Some(text) => {
            let mut out = String::with_capacity(text.len());
            for ch in text.chars() {
                match ch {
                    '\\' => out.push_str("\\\\"),
                    '\t' => out.push_str("\\t"),
                    '\n' => out.push_str("\\n"),
                    '\r' => out.push_str("\\r"),
                    _ => out.push(ch),
                }
            }
            out
        }
    }
}

fn pg_json_copy_field(value: &Value) -> String {
    let text = serde_json::to_string(value).unwrap_or_else(|_| "null".to_string());
    pg_text_copy_field(Some(&text))
}

fn pg_empty_text_array_field() -> String {
    "{}".to_string()
}

fn json_scalar_to_string(value: &Value) -> Option<String> {
    match value {
        Value::Null => None,
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        Value::Bool(flag) => Some(flag.to_string()),
        _ => None,
    }
}

fn pg_text_array_value_field(value: Option<&Value>) -> String {
    let Some(Value::Array(items)) = value else {
        return "{}".to_string();
    };
    let values: Vec<String> = items.iter().filter_map(json_scalar_to_string).collect();
    if values.is_empty() {
        return "{}".to_string();
    }
    let body = values
        .iter()
        .map(|value| {
            let escaped = value.replace('\\', "\\\\").replace('"', "\\\"");
            format!("\"{}\"", escaped)
        })
        .collect::<Vec<_>>()
        .join(",");
    format!("{{{}}}", body)
}

fn pg_i64_array_field(values: &[i64]) -> String {
    if values.is_empty() {
        return "{}".to_string();
    }
    let body = values
        .iter()
        .map(|value| value.to_string())
        .collect::<Vec<_>>()
        .join(",");
    format!("{{{}}}", body)
}

fn write_copy_fields<W: Write>(writer: &mut W, fields: &[String]) -> io::Result<()> {
    writer.write_all(fields.join("\t").as_bytes())?;
    writer.write_all(b"\n")?;
    Ok(())
}

fn emit_compact_copy_row<W: Write>(
    writer: &mut W,
    serving_rate_id: &str,
    snapshot_id: &str,
    plan_id: &str,
    plan_month_id: &str,
    procedure_hash: &str,
    procedure_code: Option<i64>,
    reported_code_system: Option<&str>,
    reported_code: Option<&str>,
    billing_code: &str,
    billing_code_type: &str,
    rate_pack_hash: &str,
    provider_set_hash: &str,
    provider_count: i64,
    price_set_hash: &str,
    source_trace_set_hash: &str,
    confidence_code: &str,
) -> io::Result<()> {
    let procedure_code_text = procedure_code.map(|value| value.to_string());
    let provider_count_text = provider_count.to_string();
    let fields = [
        pg_text_copy_field(Some(serving_rate_id)),
        pg_text_copy_field(Some(snapshot_id)),
        pg_text_copy_field(Some(plan_id)),
        pg_text_copy_field(Some(plan_month_id)),
        pg_text_copy_field(Some(procedure_hash)),
        pg_text_copy_field(procedure_code_text.as_deref()),
        pg_text_copy_field(reported_code_system),
        pg_text_copy_field(reported_code),
        pg_text_copy_field(Some(billing_code)),
        pg_text_copy_field(Some(billing_code_type)),
        pg_text_copy_field(Some(rate_pack_hash)),
        pg_text_copy_field(Some(provider_set_hash)),
        pg_text_copy_field(Some(&provider_count_text)),
        pg_text_copy_field(Some(price_set_hash)),
        pg_text_copy_field(Some(source_trace_set_hash)),
        pg_text_copy_field(Some(confidence_code)),
    ];
    write_copy_fields(writer, &fields)
}

fn process_compact_object<W: Write>(
    writer: &mut W,
    provider_map: &mut HashMap<String, ProviderEntry>,
    emitted_price_sets: &mut HashSet<String>,
    emitted_provider_sets: &mut HashSet<String>,
    emitted_procedures: &mut HashSet<String>,
    name: &str,
    payload: &[u8],
    snapshot_id: &str,
    plan_id: &str,
    plan_month_id: &str,
    source_trace_set_hash: &str,
    confidence_code: &str,
) -> io::Result<()> {
    let value: Value = match serde_json::from_slice(payload) {
        Ok(value) => value,
        Err(_) => return Ok(()),
    };
    if name == "provider_references" {
        if let Some(key_value) = value.get("provider_group_id") {
            if let (Some(key), Some(entry)) =
                (provider_ref_key(key_value), build_provider_entry(&value))
            {
                provider_map.insert(key, entry);
            }
        }
        return Ok(());
    }
    if name != "in_network" {
        return Ok(());
    }
    let billing_code = normalize_string(value.get("billing_code")).unwrap_or_default();
    let billing_code_type = normalize_string(value.get("billing_code_type")).unwrap_or_default();
    let reported_code = normalize_code(value.get("billing_code"));
    let reported_code_system = normalize_code(value.get("billing_code_type"));
    let procedure_payload = json!({
        "billing_code_type": value.get("billing_code_type").cloned().unwrap_or(Value::Null),
        "billing_code_type_version": value.get("billing_code_type_version").cloned().unwrap_or(Value::Null),
        "billing_code": value.get("billing_code").cloned().unwrap_or(Value::Null),
        "name": value.get("name").cloned().unwrap_or(Value::Null),
        "description": value.get("description").cloned().unwrap_or(Value::Null),
    });
    let procedure_hash = semantic_hash("procedure", procedure_payload.clone());
    if emitted_procedures.insert(procedure_hash.clone()) {
        emit_json_record(
            writer,
            "procedure",
            &json!({
                "procedure_hash": procedure_hash,
                "hash_prefix": &procedure_hash[..procedure_hash.len().min(16)],
                "billing_code_type": value.get("billing_code_type").cloned().unwrap_or(Value::Null),
                "billing_code_type_version": value.get("billing_code_type_version").cloned().unwrap_or(Value::Null),
                "billing_code": value.get("billing_code").cloned().unwrap_or(Value::Null),
                "name": value.get("name").cloned().unwrap_or(Value::Null),
                "description": value.get("description").cloned().unwrap_or(Value::Null),
                "canonical_payload": procedure_payload,
            }),
        )?;
    }

    let mut grouped: BTreeMap<String, (String, Value, HashSet<i64>, i64)> = BTreeMap::new();
    if let Some(rates) = value.get("negotiated_rates").and_then(Value::as_array) {
        for rate in rates {
            if rate.get("provider_groups").is_some() {
                continue;
            }
            let refs = match rate.get("provider_references").and_then(Value::as_array) {
                Some(refs) => refs,
                None => continue,
            };
            let provider_entry = match provider_set_from_refs(provider_map, refs) {
                Some(entry) => entry,
                None => continue,
            };
            let prices = match rate.get("negotiated_prices").and_then(Value::as_array) {
                Some(prices) => prices,
                None => continue,
            };
            let (price_set_hash, price_payload) = match price_key_and_payload(prices) {
                Some(value) => value,
                None => continue,
            };
            let group = grouped
                .entry(price_set_hash.clone())
                .or_insert_with(|| (price_set_hash, price_payload, HashSet::new(), 0));
            if group.2.insert(provider_entry.entry_hash) {
                group.3 += provider_entry.provider_count;
            }
        }
    }

    for (_key, (price_set_hash, price_payload, provider_hashes, provider_count)) in grouped {
        let mut sorted_provider_hashes: Vec<i64> = provider_hashes.into_iter().collect();
        sorted_provider_hashes.sort_unstable();
        let provider_set_hash = semantic_hash(
            "provider_set",
            json!({
                "provider_group_count": sorted_provider_hashes.len(),
                "provider_group_hashes": sorted_provider_hashes,
                "tin_type": "set",
                "tin_value": Value::Null,
            }),
        );
        let rate_pack_hash = hash_text(
            "serving_rate_pack",
            &[
                snapshot_id.to_string(),
                procedure_hash.clone(),
                provider_set_hash.clone(),
                price_set_hash.clone(),
            ],
        );
        let serving_rate_id = hash_text(
            "serving_rate_id",
            &[
                snapshot_id.to_string(),
                plan_id.to_string(),
                billing_code.clone(),
                rate_pack_hash.clone(),
            ],
        );
        if emitted_price_sets.insert(price_set_hash.clone()) {
            emit_json_record(
                writer,
                "price_set",
                &json!({
                    "price_set_hash": price_set_hash,
                    "hash_prefix": &price_set_hash[..price_set_hash.len().min(16)],
                    "price_atom_hashes": [],
                    "canonical_payload": price_payload,
                }),
            )?;
        }
        if emitted_provider_sets.insert(provider_set_hash.clone()) {
            emit_json_record(
                writer,
                "provider_set",
                &json!({
                    "provider_set_hash": provider_set_hash,
                    "hash_prefix": &provider_set_hash[..provider_set_hash.len().min(16)],
                    "provider_count": provider_count,
                    "npi": Value::Null,
                    "tin_type": "set",
                    "tin_value": Value::Null,
                    "canonical_payload": {
                        "provider_group_hashes": sorted_provider_hashes,
                        "provider_group_count": sorted_provider_hashes.len(),
                        "provider_count": provider_count,
                        "provider_count_mode": "summed_provider_groups",
                        "npi_inline": false,
                        "tin_type": "set",
                        "tin_value": Value::Null,
                    },
                }),
            )?;
        }
        emit_json_record(
            writer,
            "serving_rate_compact",
            &json!({
                "serving_rate_id": serving_rate_id,
                "snapshot_id": snapshot_id,
                "plan_id": plan_id,
                "plan_month_id": plan_month_id,
                "procedure_hash": procedure_hash,
                "procedure_code": Value::Null,
                "reported_code_system": reported_code_system,
                "reported_code": reported_code,
                "billing_code": billing_code,
                "billing_code_type": billing_code_type,
                "rate_pack_hash": rate_pack_hash,
                "provider_set_hash": provider_set_hash,
                "provider_count": provider_count,
                "price_set_hash": price_set_hash,
                "source_trace_set_hash": source_trace_set_hash,
                "confidence_code": confidence_code,
            }),
        )?;
    }
    Ok(())
}

fn process_compact_in_network_streamed<W: Write>(
    writer: &mut W,
    compact_copy_writer: &mut Option<CompactCopySink>,
    provider_map: &HashMap<String, ProviderEntry>,
    emitted_price_sets: &mut HashSet<String>,
    emitted_provider_sets: &mut HashSet<String>,
    emitted_procedures: &mut HashSet<String>,
    payload: &[u8],
    items_start: usize,
    array_close: usize,
    _ranges: &[(usize, usize)],
    snapshot_id: &str,
    plan_id: &str,
    plan_month_id: &str,
    source_trace_set_hash: &str,
    confidence_code: &str,
) -> io::Result<()> {
    let mut metadata_payload = Vec::with_capacity(items_start + (payload.len() - array_close));
    metadata_payload.extend_from_slice(&payload[..items_start]);
    metadata_payload.extend_from_slice(&payload[array_close..]);
    let value: Value = match serde_json::from_slice(&metadata_payload) {
        Ok(value) => value,
        Err(_) => return Ok(()),
    };
    let billing_code = normalize_string(value.get("billing_code")).unwrap_or_default();
    let billing_code_type = normalize_string(value.get("billing_code_type")).unwrap_or_default();
    let reported_code = normalize_code(value.get("billing_code"));
    let reported_code_system = normalize_code(value.get("billing_code_type"));
    let procedure_payload = json!({
        "billing_code_type": value.get("billing_code_type").cloned().unwrap_or(Value::Null),
        "billing_code_type_version": value.get("billing_code_type_version").cloned().unwrap_or(Value::Null),
        "billing_code": value.get("billing_code").cloned().unwrap_or(Value::Null),
        "name": value.get("name").cloned().unwrap_or(Value::Null),
        "description": value.get("description").cloned().unwrap_or(Value::Null),
    });
    let procedure_hash = semantic_hash("procedure", procedure_payload.clone());
    if emitted_procedures.insert(procedure_hash.clone()) {
        emit_json_record(
            writer,
            "procedure",
            &json!({
                "procedure_hash": procedure_hash,
                "hash_prefix": &procedure_hash[..procedure_hash.len().min(16)],
                "billing_code_type": value.get("billing_code_type").cloned().unwrap_or(Value::Null),
                "billing_code_type_version": value.get("billing_code_type_version").cloned().unwrap_or(Value::Null),
                "billing_code": value.get("billing_code").cloned().unwrap_or(Value::Null),
                "name": value.get("name").cloned().unwrap_or(Value::Null),
                "description": value.get("description").cloned().unwrap_or(Value::Null),
                "canonical_payload": procedure_payload,
            }),
        )?;
    }

    let chunk_size = env::var("HLTHPRT_PTG2_RUST_SPLIT_NEGOTIATED_RATES")
        .ok()
        .and_then(|raw| raw.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_SPLIT_NEGOTIATED_RATES);

    let mut process_range_chunk = |range_chunk: &[(usize, usize)]| -> io::Result<()> {
        let parsed_rates: Vec<(String, Value, i64, i64)> = range_chunk
            .par_iter()
            .filter_map(|(first, last)| {
                let rate: Value = serde_json::from_slice(&payload[*first..*last]).ok()?;
                if rate.get("provider_groups").is_some() {
                    return None;
                }
                let refs = rate.get("provider_references").and_then(Value::as_array)?;
                let provider_entry = provider_set_from_refs(provider_map, refs)?;
                let prices = rate.get("negotiated_prices").and_then(Value::as_array)?;
                let (price_set_hash, price_payload) = price_key_and_payload(prices)?;
                Some((
                    price_set_hash,
                    price_payload,
                    provider_entry.entry_hash,
                    provider_entry.provider_count,
                ))
            })
            .collect();

        let mut grouped: BTreeMap<String, (String, Value, HashSet<i64>, i64)> = BTreeMap::new();
        for (price_set_hash, price_payload, provider_entry_hash, provider_count) in parsed_rates {
            let group = grouped
                .entry(price_set_hash.clone())
                .or_insert_with(|| (price_set_hash, price_payload, HashSet::new(), 0));
            if group.2.insert(provider_entry_hash) {
                group.3 += provider_count;
            }
        }

        for (_key, (price_set_hash, price_payload, provider_hashes, provider_count)) in grouped {
            let mut sorted_provider_hashes: Vec<i64> = provider_hashes.into_iter().collect();
            sorted_provider_hashes.sort_unstable();
            let provider_set_hash = semantic_hash(
                "provider_set",
                json!({
                    "provider_group_count": sorted_provider_hashes.len(),
                    "provider_group_hashes": sorted_provider_hashes,
                    "tin_type": "set",
                    "tin_value": Value::Null,
                }),
            );
            let rate_pack_hash = hash_text(
                "serving_rate_pack",
                &[
                    snapshot_id.to_string(),
                    procedure_hash.clone(),
                    provider_set_hash.clone(),
                    price_set_hash.clone(),
                ],
            );
            let serving_rate_id = hash_text(
                "serving_rate_id",
                &[
                    snapshot_id.to_string(),
                    plan_id.to_string(),
                    billing_code.clone(),
                    rate_pack_hash.clone(),
                ],
            );
            if emitted_price_sets.insert(price_set_hash.clone()) {
                emit_json_record(
                    writer,
                    "price_set",
                    &json!({
                        "price_set_hash": price_set_hash,
                        "hash_prefix": &price_set_hash[..price_set_hash.len().min(16)],
                        "price_atom_hashes": [],
                        "canonical_payload": price_payload,
                    }),
                )?;
            }
            if emitted_provider_sets.insert(provider_set_hash.clone()) {
                emit_json_record(
                    writer,
                    "provider_set",
                    &json!({
                        "provider_set_hash": provider_set_hash,
                        "hash_prefix": &provider_set_hash[..provider_set_hash.len().min(16)],
                        "provider_count": provider_count,
                        "npi": Value::Null,
                        "tin_type": "set",
                        "tin_value": Value::Null,
                        "canonical_payload": {
                            "provider_group_hashes": sorted_provider_hashes,
                            "provider_group_count": sorted_provider_hashes.len(),
                            "provider_count": provider_count,
                            "provider_count_mode": "summed_provider_groups",
                            "npi_inline": false,
                            "tin_type": "set",
                            "tin_value": Value::Null,
                        },
                    }),
                )?;
            }
            if let Some(copy_writer) = compact_copy_writer.as_mut() {
                copy_writer.write_row(
                    &serving_rate_id,
                    snapshot_id,
                    plan_id,
                    plan_month_id,
                    &procedure_hash,
                    None,
                    reported_code_system.as_deref(),
                    reported_code.as_deref(),
                    &billing_code,
                    &billing_code_type,
                    &rate_pack_hash,
                    &provider_set_hash,
                    provider_count,
                    &price_set_hash,
                    source_trace_set_hash,
                    confidence_code,
                )?;
            } else {
                emit_json_record(
                    writer,
                    "serving_rate_compact",
                    &json!({
                        "serving_rate_id": serving_rate_id,
                        "snapshot_id": snapshot_id,
                        "plan_id": plan_id,
                        "plan_month_id": plan_month_id,
                        "procedure_hash": procedure_hash,
                        "procedure_code": Value::Null,
                        "reported_code_system": reported_code_system,
                        "reported_code": reported_code,
                        "billing_code": billing_code,
                        "billing_code_type": billing_code_type,
                        "rate_pack_hash": rate_pack_hash,
                        "provider_set_hash": provider_set_hash,
                        "provider_count": provider_count,
                        "price_set_hash": price_set_hash,
                        "source_trace_set_hash": source_trace_set_hash,
                        "confidence_code": confidence_code,
                    }),
                )?;
            }
        }
        if let Some(copy_writer) = compact_copy_writer.as_mut() {
            copy_writer.maybe_rotate(writer)?;
        }
        Ok(())
    };

    let mut depth = 0usize;
    let mut index = items_start;
    let mut item_start: Option<usize> = None;
    let mut chunk: Vec<(usize, usize)> = Vec::with_capacity(chunk_size);
    while index < array_close {
        let byte = payload[index];
        if item_start.is_none() {
            if byte.is_ascii_whitespace() || byte == b',' {
                index += 1;
                continue;
            }
            item_start = Some(index);
        }
        match byte {
            b'"' => {
                if let Some(string_end) = json_string_end(payload, index) {
                    index = string_end;
                } else {
                    break;
                }
            }
            b'{' | b'[' => depth += 1,
            b'}' | b']' => {
                if depth == 0 {
                    break;
                }
                depth -= 1;
            }
            b',' if depth == 0 => {
                if let Some(first) = item_start.take() {
                    let mut last = index;
                    while last > first && payload[last - 1].is_ascii_whitespace() {
                        last -= 1;
                    }
                    if first < last {
                        chunk.push((first, last));
                    }
                    if chunk.len() >= chunk_size {
                        process_range_chunk(&chunk)?;
                        chunk.clear();
                    }
                }
            }
            _ => {}
        }
        index += 1;
    }
    if let Some(first) = item_start {
        let mut last = array_close;
        while last > first && payload[last - 1].is_ascii_whitespace() {
            last -= 1;
        }
        if first < last {
            chunk.push((first, last));
        }
    }
    if !chunk.is_empty() {
        process_range_chunk(&chunk)?;
    }
    Ok(())
}

fn process_compact_object_maybe_split<W: Write>(
    writer: &mut W,
    compact_copy_writer: &mut Option<CompactCopySink>,
    provider_map: &mut HashMap<String, ProviderEntry>,
    emitted_price_sets: &mut HashSet<String>,
    emitted_provider_sets: &mut HashSet<String>,
    emitted_procedures: &mut HashSet<String>,
    name: &str,
    payload: &[u8],
    snapshot_id: &str,
    plan_id: &str,
    plan_month_id: &str,
    source_trace_set_hash: &str,
    confidence_code: &str,
    split_in_network: bool,
    _negotiated_rate_chunk_size: usize,
) -> io::Result<usize> {
    if split_in_network && name == "in_network" {
        if let Some((items_start, array_close)) = find_negotiated_rates_array(payload) {
            process_compact_in_network_streamed(
                writer,
                compact_copy_writer,
                provider_map,
                emitted_price_sets,
                emitted_provider_sets,
                emitted_procedures,
                payload,
                items_start,
                array_close,
                &[],
                snapshot_id,
                plan_id,
                plan_month_id,
                source_trace_set_hash,
                confidence_code,
            )?;
            return Ok(1);
        }
    }
    process_compact_object(
        writer,
        provider_map,
        emitted_price_sets,
        emitted_provider_sets,
        emitted_procedures,
        name,
        payload,
        snapshot_id,
        plan_id,
        plan_month_id,
        source_trace_set_hash,
        confidence_code,
    )?;
    Ok(1)
}

#[allow(dead_code)]
fn process_compact_rate_values<W: Write>(
    writer: &mut W,
    compact_copy_writer: &mut Option<CompactCopySink>,
    dictionary_copy_sinks: &mut DictionaryCopySinks,
    provider_map: &HashMap<String, ProviderEntry>,
    emitted_price_sets: &mut HashSet<String>,
    emitted_provider_sets: &mut HashSet<String>,
    emitted_procedures: &mut HashSet<String>,
    rates: &[Value],
    procedure_value: &Value,
    snapshot_id: &str,
    plan_id: &str,
    plan_month_id: &str,
    source_trace_set_hash: &str,
    confidence_code: &str,
) -> io::Result<()> {
    if rates.is_empty() {
        return Ok(());
    }
    let billing_code = normalize_string(procedure_value.get("billing_code")).unwrap_or_default();
    let billing_code_type =
        normalize_string(procedure_value.get("billing_code_type")).unwrap_or_default();
    let reported_code = normalize_code(procedure_value.get("billing_code"));
    let reported_code_system = normalize_code(procedure_value.get("billing_code_type"));
    let procedure_payload = json!({
        "billing_code_type": procedure_value.get("billing_code_type").cloned().unwrap_or(Value::Null),
        "billing_code_type_version": procedure_value.get("billing_code_type_version").cloned().unwrap_or(Value::Null),
        "billing_code": procedure_value.get("billing_code").cloned().unwrap_or(Value::Null),
        "name": procedure_value.get("name").cloned().unwrap_or(Value::Null),
        "description": procedure_value.get("description").cloned().unwrap_or(Value::Null),
    });
    let procedure_hash = semantic_hash("procedure", procedure_payload.clone());
    if emitted_procedures.insert(procedure_hash.clone()) {
        if !dictionary_copy_sinks.write_procedure(&procedure_hash, procedure_value, &procedure_payload)? {
            emit_json_record(
                writer,
                "procedure",
                &json!({
                    "procedure_hash": procedure_hash,
                    "hash_prefix": &procedure_hash[..procedure_hash.len().min(16)],
                    "billing_code_type": procedure_value.get("billing_code_type").cloned().unwrap_or(Value::Null),
                    "billing_code_type_version": procedure_value.get("billing_code_type_version").cloned().unwrap_or(Value::Null),
                    "billing_code": procedure_value.get("billing_code").cloned().unwrap_or(Value::Null),
                    "name": procedure_value.get("name").cloned().unwrap_or(Value::Null),
                    "description": procedure_value.get("description").cloned().unwrap_or(Value::Null),
                    "canonical_payload": procedure_payload,
                }),
            )?;
        }
    }

    let parsed_rates: Vec<(String, Value, i64, i64)> = rates
        .par_iter()
        .filter_map(|rate| {
            if rate.get("provider_groups").is_some() {
                return None;
            }
            let refs = rate.get("provider_references").and_then(Value::as_array)?;
            let provider_entry = provider_set_from_refs(provider_map, refs)?;
            let prices = rate.get("negotiated_prices").and_then(Value::as_array)?;
            let (price_set_hash, price_payload) = price_key_and_payload(prices)?;
            Some((
                price_set_hash,
                price_payload,
                provider_entry.entry_hash,
                provider_entry.provider_count,
            ))
        })
        .collect();

    let mut grouped: BTreeMap<String, (String, Value, HashSet<i64>, i64)> = BTreeMap::new();
    for (price_set_hash, price_payload, provider_entry_hash, provider_count) in parsed_rates {
        let group = grouped
            .entry(price_set_hash.clone())
            .or_insert_with(|| (price_set_hash, price_payload, HashSet::new(), 0));
        if group.2.insert(provider_entry_hash) {
            group.3 += provider_count;
        }
    }

    for (_key, (price_set_hash, price_payload, provider_hashes, provider_count)) in grouped {
        let mut sorted_provider_hashes: Vec<i64> = provider_hashes.into_iter().collect();
        sorted_provider_hashes.sort_unstable();
        let provider_set_hash = semantic_hash(
            "provider_set",
            json!({
                "provider_group_count": sorted_provider_hashes.len(),
                "provider_group_hashes": sorted_provider_hashes.clone(),
                "tin_type": "set",
                "tin_value": Value::Null,
            }),
        );
        let rate_pack_hash = hash_text(
            "serving_rate_pack",
            &[
                snapshot_id.to_string(),
                procedure_hash.clone(),
                provider_set_hash.clone(),
                price_set_hash.clone(),
            ],
        );
        let serving_rate_id = hash_text(
            "serving_rate_id",
            &[
                snapshot_id.to_string(),
                plan_id.to_string(),
                billing_code.clone(),
                rate_pack_hash.clone(),
            ],
        );
        if emitted_price_sets.insert(price_set_hash.clone()) {
            if !dictionary_copy_sinks.write_price_set(&price_set_hash, &price_payload)? {
                emit_json_record(
                    writer,
                    "price_set",
                    &json!({
                        "price_set_hash": price_set_hash,
                        "hash_prefix": &price_set_hash[..price_set_hash.len().min(16)],
                        "price_atom_hashes": [],
                        "canonical_payload": price_payload,
                    }),
                )?;
            }
        }
        if emitted_provider_sets.insert(provider_set_hash.clone()) {
            if !dictionary_copy_sinks.write_provider_set(
                &provider_set_hash,
                provider_count,
                &sorted_provider_hashes,
            )? {
                emit_json_record(
                    writer,
                    "provider_set",
                    &json!({
                        "provider_set_hash": provider_set_hash,
                        "hash_prefix": &provider_set_hash[..provider_set_hash.len().min(16)],
                        "provider_count": provider_count,
                        "npi": Value::Null,
                        "tin_type": "set",
                        "tin_value": Value::Null,
                        "canonical_payload": {
                            "provider_group_hashes": sorted_provider_hashes,
                            "provider_group_count": sorted_provider_hashes.len(),
                            "provider_count": provider_count,
                            "provider_count_mode": "summed_provider_groups",
                            "npi_inline": false,
                            "tin_type": "set",
                            "tin_value": Value::Null,
                        },
                    }),
                )?;
            }
        }
        if let Some(copy_writer) = compact_copy_writer.as_mut() {
            copy_writer.write_row(
                &serving_rate_id,
                snapshot_id,
                plan_id,
                plan_month_id,
                &procedure_hash,
                None,
                reported_code_system.as_deref(),
                reported_code.as_deref(),
                &billing_code,
                &billing_code_type,
                &rate_pack_hash,
                &provider_set_hash,
                provider_count,
                &price_set_hash,
                source_trace_set_hash,
                confidence_code,
            )?;
        } else {
            emit_json_record(
                writer,
                "serving_rate_compact",
                &json!({
                    "serving_rate_id": serving_rate_id,
                    "snapshot_id": snapshot_id,
                    "plan_id": plan_id,
                    "plan_month_id": plan_month_id,
                    "procedure_hash": procedure_hash,
                    "procedure_code": Value::Null,
                    "reported_code_system": reported_code_system,
                    "reported_code": reported_code,
                    "billing_code": billing_code,
                    "billing_code_type": billing_code_type,
                    "rate_pack_hash": rate_pack_hash,
                    "provider_set_hash": provider_set_hash,
                    "provider_count": provider_count,
                    "price_set_hash": price_set_hash,
                    "source_trace_set_hash": source_trace_set_hash,
                    "confidence_code": confidence_code,
                }),
            )?;
        }
    }
    if let Some(copy_writer) = compact_copy_writer.as_mut() {
        copy_writer.maybe_rotate(writer)?;
    }
    dictionary_copy_sinks.maybe_rotate(writer)?;
    Ok(())
}

fn process_compact_rate_lites<W: Write>(
    writer: &mut W,
    compact_copy_writer: &mut Option<CompactCopySink>,
    dictionary_copy_sinks: &mut DictionaryCopySinks,
    provider_map: &HashMap<String, ProviderEntry>,
    emitted_price_sets: &mut HashSet<String>,
    emitted_provider_sets: &mut HashSet<String>,
    emitted_procedures: &mut HashSet<String>,
    rates: &[RateLite],
    procedure_value: &Value,
    snapshot_id: &str,
    plan_id: &str,
    plan_month_id: &str,
    source_trace_set_hash: &str,
    confidence_code: &str,
) -> io::Result<()> {
    if rates.is_empty() {
        return Ok(());
    }
    let billing_code = normalize_string(procedure_value.get("billing_code")).unwrap_or_default();
    let billing_code_type =
        normalize_string(procedure_value.get("billing_code_type")).unwrap_or_default();
    let reported_code = normalize_code(procedure_value.get("billing_code"));
    let reported_code_system = normalize_code(procedure_value.get("billing_code_type"));
    let procedure_payload = json!({
        "billing_code_type": procedure_value.get("billing_code_type").cloned().unwrap_or(Value::Null),
        "billing_code_type_version": procedure_value.get("billing_code_type_version").cloned().unwrap_or(Value::Null),
        "billing_code": procedure_value.get("billing_code").cloned().unwrap_or(Value::Null),
        "name": procedure_value.get("name").cloned().unwrap_or(Value::Null),
        "description": procedure_value.get("description").cloned().unwrap_or(Value::Null),
    });
    let procedure_hash = semantic_hash("procedure", procedure_payload.clone());
    if emitted_procedures.insert(procedure_hash.clone()) {
        if !dictionary_copy_sinks.write_procedure(&procedure_hash, procedure_value, &procedure_payload)? {
            emit_json_record(
                writer,
                "procedure",
                &json!({
                    "procedure_hash": procedure_hash,
                    "hash_prefix": &procedure_hash[..procedure_hash.len().min(16)],
                    "billing_code_type": procedure_value.get("billing_code_type").cloned().unwrap_or(Value::Null),
                    "billing_code_type_version": procedure_value.get("billing_code_type_version").cloned().unwrap_or(Value::Null),
                    "billing_code": procedure_value.get("billing_code").cloned().unwrap_or(Value::Null),
                    "name": procedure_value.get("name").cloned().unwrap_or(Value::Null),
                    "description": procedure_value.get("description").cloned().unwrap_or(Value::Null),
                    "canonical_payload": procedure_payload,
                }),
            )?;
        }
    }

    let parsed_rates: Vec<(String, Value, i64, i64)> = rates
        .par_iter()
        .filter_map(|rate| {
            let provider_entry = provider_set_from_ref_keys(provider_map, &rate.provider_refs)?;
            let (price_set_hash, price_payload) = price_key_and_payload(&rate.prices)?;
            Some((
                price_set_hash,
                price_payload,
                provider_entry.entry_hash,
                provider_entry.provider_count,
            ))
        })
        .collect();

    let mut grouped: BTreeMap<String, (String, Value, HashSet<i64>, i64)> = BTreeMap::new();
    for (price_set_hash, price_payload, provider_entry_hash, provider_count) in parsed_rates {
        let group = grouped
            .entry(price_set_hash.clone())
            .or_insert_with(|| (price_set_hash, price_payload, HashSet::new(), 0));
        if group.2.insert(provider_entry_hash) {
            group.3 += provider_count;
        }
    }

    for (_key, (price_set_hash, price_payload, provider_hashes, provider_count)) in grouped {
        let mut sorted_provider_hashes: Vec<i64> = provider_hashes.into_iter().collect();
        sorted_provider_hashes.sort_unstable();
        let provider_set_hash = semantic_hash(
            "provider_set",
            json!({
                "provider_group_count": sorted_provider_hashes.len(),
                "provider_group_hashes": sorted_provider_hashes.clone(),
                "tin_type": "set",
                "tin_value": Value::Null,
            }),
        );
        let rate_pack_hash = hash_text(
            "serving_rate_pack",
            &[
                snapshot_id.to_string(),
                procedure_hash.clone(),
                provider_set_hash.clone(),
                price_set_hash.clone(),
            ],
        );
        let serving_rate_id = hash_text(
            "serving_rate_id",
            &[
                snapshot_id.to_string(),
                plan_id.to_string(),
                billing_code.clone(),
                rate_pack_hash.clone(),
            ],
        );
        if emitted_price_sets.insert(price_set_hash.clone()) {
            if !dictionary_copy_sinks.write_price_set(&price_set_hash, &price_payload)? {
                emit_json_record(
                    writer,
                    "price_set",
                    &json!({
                        "price_set_hash": price_set_hash,
                        "hash_prefix": &price_set_hash[..price_set_hash.len().min(16)],
                        "price_atom_hashes": [],
                        "canonical_payload": price_payload,
                    }),
                )?;
            }
        }
        if emitted_provider_sets.insert(provider_set_hash.clone()) {
            if !dictionary_copy_sinks.write_provider_set(
                &provider_set_hash,
                provider_count,
                &sorted_provider_hashes,
            )? {
                emit_json_record(
                    writer,
                    "provider_set",
                    &json!({
                        "provider_set_hash": provider_set_hash,
                        "hash_prefix": &provider_set_hash[..provider_set_hash.len().min(16)],
                        "provider_count": provider_count,
                        "npi": Value::Null,
                        "tin_type": "set",
                        "tin_value": Value::Null,
                        "canonical_payload": {
                            "provider_group_hashes": sorted_provider_hashes,
                            "provider_group_count": sorted_provider_hashes.len(),
                            "provider_count": provider_count,
                            "provider_count_mode": "summed_provider_groups",
                            "npi_inline": false,
                            "tin_type": "set",
                            "tin_value": Value::Null,
                        },
                    }),
                )?;
            }
        }
        if let Some(copy_writer) = compact_copy_writer.as_mut() {
            copy_writer.write_row(
                &serving_rate_id,
                snapshot_id,
                plan_id,
                plan_month_id,
                &procedure_hash,
                None,
                reported_code_system.as_deref(),
                reported_code.as_deref(),
                &billing_code,
                &billing_code_type,
                &rate_pack_hash,
                &provider_set_hash,
                provider_count,
                &price_set_hash,
                source_trace_set_hash,
                confidence_code,
            )?;
        } else {
            emit_json_record(
                writer,
                "serving_rate_compact",
                &json!({
                    "serving_rate_id": serving_rate_id,
                    "snapshot_id": snapshot_id,
                    "plan_id": plan_id,
                    "plan_month_id": plan_month_id,
                    "procedure_hash": procedure_hash,
                    "procedure_code": Value::Null,
                    "reported_code_system": reported_code_system,
                    "reported_code": reported_code,
                    "billing_code": billing_code,
                    "billing_code_type": billing_code_type,
                    "rate_pack_hash": rate_pack_hash,
                    "provider_set_hash": provider_set_hash,
                    "provider_count": provider_count,
                    "price_set_hash": price_set_hash,
                    "source_trace_set_hash": source_trace_set_hash,
                    "confidence_code": confidence_code,
                }),
            )?;
        }
    }
    if let Some(copy_writer) = compact_copy_writer.as_mut() {
        copy_writer.maybe_rotate(writer)?;
    }
    dictionary_copy_sinks.maybe_rotate(writer)?;
    Ok(())
}

fn read_rate_lite_struson<R: Read>(json_reader: &mut JsonStreamReader<R>) -> io::Result<Option<RateLite>> {
    let mut provider_refs: Vec<String> = Vec::new();
    let mut prices: Vec<Value> = Vec::new();
    let mut has_inline_provider_groups = false;
    json_reader.begin_object().map_err(to_io_error)?;
    while json_reader.has_next().map_err(to_io_error)? {
        let name = json_reader.next_name_owned().map_err(to_io_error)?;
        match name.as_str() {
            "provider_groups" => {
                has_inline_provider_groups = true;
                json_reader.skip_value().map_err(to_io_error)?;
            }
            "provider_references" => {
                json_reader.begin_array().map_err(to_io_error)?;
                while json_reader.has_next().map_err(to_io_error)? {
                    let value: Value = json_reader.deserialize_next().map_err(to_io_error)?;
                    if let Some(key) = provider_ref_key(&value) {
                        provider_refs.push(key);
                    }
                }
                json_reader.end_array().map_err(to_io_error)?;
            }
            "negotiated_prices" => {
                json_reader.begin_array().map_err(to_io_error)?;
                while json_reader.has_next().map_err(to_io_error)? {
                    prices.push(json_reader.deserialize_next().map_err(to_io_error)?);
                }
                json_reader.end_array().map_err(to_io_error)?;
            }
            _ => {
                json_reader.skip_value().map_err(to_io_error)?;
            }
        }
    }
    json_reader.end_object().map_err(to_io_error)?;
    if has_inline_provider_groups || provider_refs.is_empty() || prices.is_empty() {
        return Ok(None);
    }
    Ok(Some(RateLite {
        provider_refs,
        prices,
    }))
}

fn process_in_network_struson<R: Read, W: Write>(
    json_reader: &mut JsonStreamReader<R>,
    writer: &mut W,
    compact_copy_writer: &mut Option<CompactCopySink>,
    dictionary_copy_sinks: &mut DictionaryCopySinks,
    provider_map: &HashMap<String, ProviderEntry>,
    emitted_price_sets: &mut HashSet<String>,
    emitted_provider_sets: &mut HashSet<String>,
    emitted_procedures: &mut HashSet<String>,
    snapshot_id: &str,
    plan_id: &str,
    plan_month_id: &str,
    source_trace_set_hash: &str,
    confidence_code: &str,
    chunk_size: usize,
) -> io::Result<u64> {
    let mut procedure = Map::new();
    let mut rate_chunk: Vec<RateLite> = Vec::with_capacity(chunk_size);
    let mut rate_count = 0u64;
    json_reader.begin_object().map_err(to_io_error)?;
    while json_reader.has_next().map_err(to_io_error)? {
        let name = json_reader.next_name_owned().map_err(to_io_error)?;
        match name.as_str() {
            "billing_code_type"
            | "billing_code_type_version"
            | "billing_code"
            | "name"
            | "description" => {
                let value: Value = json_reader.deserialize_next().map_err(to_io_error)?;
                procedure.insert(name, value);
            }
            "negotiated_rates" => {
                json_reader.begin_array().map_err(to_io_error)?;
                while json_reader.has_next().map_err(to_io_error)? {
                    rate_count += 1;
                    if let Some(rate) = read_rate_lite_struson(json_reader)? {
                        rate_chunk.push(rate);
                        if rate_chunk.len() >= chunk_size {
                            let procedure_value = Value::Object(procedure.clone());
                            process_compact_rate_lites(
                                writer,
                                compact_copy_writer,
                                dictionary_copy_sinks,
                                provider_map,
                                emitted_price_sets,
                                emitted_provider_sets,
                                emitted_procedures,
                                &rate_chunk,
                                &procedure_value,
                                snapshot_id,
                                plan_id,
                                plan_month_id,
                                source_trace_set_hash,
                                confidence_code,
                            )?;
                            rate_chunk.clear();
                        }
                    }
                }
                json_reader.end_array().map_err(to_io_error)?;
            }
            _ => {
                json_reader.skip_value().map_err(to_io_error)?;
            }
        }
    }
    json_reader.end_object().map_err(to_io_error)?;
    if !rate_chunk.is_empty() {
        let procedure_value = Value::Object(procedure);
        process_compact_rate_lites(
            writer,
            compact_copy_writer,
            dictionary_copy_sinks,
            provider_map,
            emitted_price_sets,
            emitted_provider_sets,
            emitted_procedures,
            &rate_chunk,
            &procedure_value,
            snapshot_id,
            plan_id,
            plan_month_id,
            source_trace_set_hash,
            confidence_code,
        )?;
    }
    Ok(rate_count)
}

fn scan_compact_struson(path: &Path) -> io::Result<()> {
    let snapshot_id = env::var("HLTHPRT_PTG2_COMPACT_SNAPSHOT_ID").unwrap_or_default();
    let plan_id = env::var("HLTHPRT_PTG2_COMPACT_PLAN_ID").unwrap_or_default();
    let plan_month_id = env::var("HLTHPRT_PTG2_COMPACT_PLAN_MONTH_ID").unwrap_or_default();
    let source_trace_set_hash =
        env::var("HLTHPRT_PTG2_COMPACT_SOURCE_TRACE_SET_HASH").unwrap_or_default();
    let confidence_code = env::var("HLTHPRT_PTG2_COMPACT_CONFIDENCE_CODE")
        .unwrap_or_else(|_| "tic_rate_npi_tin".to_string());
    let total_bytes = path.metadata().map(|metadata| metadata.len()).unwrap_or(0);
    let compressed_bytes_read = Arc::new(AtomicU64::new(0));
    let reader = open_reader(path, Arc::clone(&compressed_bytes_read))?;
    let mut json_reader = JsonStreamReader::new(reader);
    let stdout = io::stdout();
    let mut writer = BufWriter::new(stdout.lock());
    let compact_copy_rotate_bytes = progress_interval(
        "HLTHPRT_PTG2_COMPACT_SERVING_COPY_ROTATE_BYTES",
        DEFAULT_COMPACT_COPY_ROTATE_BYTES,
    );
    let compact_copy_stdout = env::var("HLTHPRT_PTG2_COMPACT_SERVING_COPY_STDOUT")
        .map(|value| {
            let normalized = value.trim().to_ascii_lowercase();
            matches!(normalized.as_str(), "1" | "true" | "yes" | "on")
        })
        .unwrap_or(false);
    let mut compact_copy_writer: Option<CompactCopySink> = if compact_copy_stdout {
        Some(CompactCopySink::new_stdout(compact_copy_rotate_bytes)?)
    } else {
        match env::var("HLTHPRT_PTG2_COMPACT_SERVING_COPY_PATH") {
            Ok(copy_path) if !copy_path.trim().is_empty() => Some(CompactCopySink::new_file(
                copy_path,
                compact_copy_rotate_bytes,
            )?),
            _ => None,
        }
    };
    let mut dictionary_copy_sinks = DictionaryCopySinks::from_env(compact_copy_rotate_bytes)?;
    let progress_bytes_interval = progress_interval(
        "HLTHPRT_PTG2_SCANNER_PROGRESS_BYTES",
        DEFAULT_PROGRESS_BYTES,
    );
    let mut next_progress_bytes = progress_bytes_interval;
    let progress_objects_interval = progress_interval(
        "HLTHPRT_PTG2_SCANNER_PROGRESS_OBJECTS",
        DEFAULT_PROGRESS_OBJECTS,
    );
    let mut next_progress_objects = progress_objects_interval;
    let mut object_counts: HashMap<String, u64> = HashMap::new();
    let mut provider_map: HashMap<String, ProviderEntry> = HashMap::new();
    let mut emitted_price_sets: HashSet<String> = HashSet::new();
    let mut emitted_provider_sets: HashSet<String> = HashSet::new();
    let mut emitted_procedures: HashSet<String> = HashSet::new();
    let mut emitted_provider_group_members: HashSet<(i64, i64)> = HashSet::new();
    let negotiated_rate_chunk_size = split_interval(
        "HLTHPRT_PTG2_RUST_SPLIT_NEGOTIATED_RATES",
        DEFAULT_SPLIT_NEGOTIATED_RATES,
    );

    json_reader.begin_object().map_err(to_io_error)?;
    while json_reader.has_next().map_err(to_io_error)? {
        let name = json_reader.next_name_owned().map_err(to_io_error)?;
        match name.as_str() {
            "provider_references" => {
                json_reader.begin_array().map_err(to_io_error)?;
                while json_reader.has_next().map_err(to_io_error)? {
                    let value: Value = json_reader.deserialize_next().map_err(to_io_error)?;
                    if let Some(key_value) = value.get("provider_group_id") {
                        if let (Some(key), Some(entry)) =
                            (provider_ref_key(key_value), build_provider_entry(&value))
                        {
                            dictionary_copy_sinks.write_provider_group_members(
                                &value,
                                &mut emitted_provider_group_members,
                            )?;
                            provider_map.insert(key, entry);
                        }
                    }
                    *object_counts
                        .entry("provider_references".to_string())
                        .or_insert(0) += 1;
                }
                json_reader.end_array().map_err(to_io_error)?;
            }
            "in_network" => {
                json_reader.begin_array().map_err(to_io_error)?;
                while json_reader.has_next().map_err(to_io_error)? {
                    let rate_count = process_in_network_struson(
                        &mut json_reader,
                        &mut writer,
                        &mut compact_copy_writer,
                        &mut dictionary_copy_sinks,
                        &provider_map,
                        &mut emitted_price_sets,
                        &mut emitted_provider_sets,
                        &mut emitted_procedures,
                        &snapshot_id,
                        &plan_id,
                        &plan_month_id,
                        &source_trace_set_hash,
                        &confidence_code,
                        negotiated_rate_chunk_size,
                    )?;
                    *object_counts.entry("in_network".to_string()).or_insert(0) += 1;
                    *object_counts
                        .entry("negotiated_rates".to_string())
                        .or_insert(0) += rate_count;
                    let bytes = compressed_bytes_read.load(Ordering::Relaxed);
                    let objects: u64 = object_counts.values().sum();
                    if progress_bytes_interval > 0 && bytes >= next_progress_bytes {
                        emit_progress(
                            path,
                            total_bytes,
                            &compressed_bytes_read,
                            &object_counts,
                            false,
                        );
                        while bytes >= next_progress_bytes {
                            next_progress_bytes += progress_bytes_interval;
                        }
                    } else if progress_objects_interval > 0 && objects >= next_progress_objects {
                        emit_progress(
                            path,
                            total_bytes,
                            &compressed_bytes_read,
                            &object_counts,
                            false,
                        );
                        while objects >= next_progress_objects {
                            next_progress_objects += progress_objects_interval;
                        }
                    }
                }
                json_reader.end_array().map_err(to_io_error)?;
            }
            _ => {
                json_reader.skip_value().map_err(to_io_error)?;
            }
        }
    }
    json_reader.end_object().map_err(to_io_error)?;
    json_reader.consume_trailing_whitespace().map_err(to_io_error)?;
    if let Some(copy_writer) = compact_copy_writer.take() {
        copy_writer.finish(&mut writer)?;
    }
    dictionary_copy_sinks.finish(&mut writer)?;
    writer.flush()?;
    emit_progress(
        path,
        total_bytes,
        &compressed_bytes_read,
        &object_counts,
        true,
    );
    Ok(())
}

fn scan_compact(path: &Path) -> io::Result<()> {
    if env::var("HLTHPRT_PTG2_STRUSON_PATH_READER")
        .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
        .unwrap_or(false)
    {
        return scan_compact_struson(path);
    }
    let snapshot_id = env::var("HLTHPRT_PTG2_COMPACT_SNAPSHOT_ID").unwrap_or_default();
    let plan_id = env::var("HLTHPRT_PTG2_COMPACT_PLAN_ID").unwrap_or_default();
    let plan_month_id = env::var("HLTHPRT_PTG2_COMPACT_PLAN_MONTH_ID").unwrap_or_default();
    let source_trace_set_hash =
        env::var("HLTHPRT_PTG2_COMPACT_SOURCE_TRACE_SET_HASH").unwrap_or_default();
    let confidence_code = env::var("HLTHPRT_PTG2_COMPACT_CONFIDENCE_CODE")
        .unwrap_or_else(|_| "tic_rate_npi_tin".to_string());
    let requested = vec!["provider_references".to_string(), "in_network".to_string()];
    let mut targets: HashMap<Vec<u8>, String> = HashMap::new();
    for name in &requested {
        targets.insert(name.as_bytes().to_vec(), name.clone());
    }
    let total_bytes = path.metadata().map(|metadata| metadata.len()).unwrap_or(0);
    let compressed_bytes_read = Arc::new(AtomicU64::new(0));
    let mut reader = open_reader(path, Arc::clone(&compressed_bytes_read))?;
    let stdout = io::stdout();
    let mut writer = BufWriter::new(stdout.lock());
    let compact_copy_rotate_bytes = progress_interval(
        "HLTHPRT_PTG2_COMPACT_SERVING_COPY_ROTATE_BYTES",
        DEFAULT_COMPACT_COPY_ROTATE_BYTES,
    );
    let compact_copy_stdout = env::var("HLTHPRT_PTG2_COMPACT_SERVING_COPY_STDOUT")
        .map(|value| {
            let normalized = value.trim().to_ascii_lowercase();
            matches!(normalized.as_str(), "1" | "true" | "yes" | "on")
        })
        .unwrap_or(false);
    let mut compact_copy_writer: Option<CompactCopySink> = if compact_copy_stdout {
        Some(CompactCopySink::new_stdout(compact_copy_rotate_bytes)?)
    } else {
        match env::var("HLTHPRT_PTG2_COMPACT_SERVING_COPY_PATH") {
            Ok(copy_path) if !copy_path.trim().is_empty() => Some(CompactCopySink::new_file(
                copy_path,
                compact_copy_rotate_bytes,
            )?),
            _ => None,
        }
    };
    let mut buffer = vec![0u8; READ_BUF_SIZE];
    let progress_bytes_interval = progress_interval(
        "HLTHPRT_PTG2_SCANNER_PROGRESS_BYTES",
        DEFAULT_PROGRESS_BYTES,
    );
    let mut next_progress_bytes = progress_bytes_interval;
    let mut object_counts: HashMap<String, u64> = HashMap::new();
    let mut provider_map: HashMap<String, ProviderEntry> = HashMap::new();
    let mut emitted_price_sets: HashSet<String> = HashSet::new();
    let mut emitted_provider_sets: HashSet<String> = HashSet::new();
    let mut emitted_procedures: HashSet<String> = HashSet::new();
    let split_in_network = env::var("HLTHPRT_PTG2_RUST_SPLIT_IN_NETWORK")
        .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
        .unwrap_or(false);
    let negotiated_rate_chunk_size = split_interval(
        "HLTHPRT_PTG2_RUST_SPLIT_NEGOTIATED_RATES",
        DEFAULT_SPLIT_NEGOTIATED_RATES,
    );

    let mut depth: usize = 0;
    let mut active_name: Option<String> = None;
    let mut active_array_depth: usize = 0;
    let mut capture: Vec<u8> = Vec::new();
    let mut capture_depth: usize = 0;
    let mut in_string = false;
    let mut escape = false;
    let mut string_buffer: Option<Vec<u8>> = None;
    let mut candidate_key: Option<Vec<u8>> = None;
    let mut pending_key: Option<Vec<u8>> = None;
    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        for &byte in &buffer[..read] {
            if capture_depth > 0 {
                capture.push(byte);
            }
            if in_string {
                if let Some(ref mut string_buf) = string_buffer {
                    string_buf.push(byte);
                }
                if escape {
                    escape = false;
                } else if byte == b'\\' {
                    escape = true;
                } else if byte == b'"' {
                    in_string = false;
                    if let Some(mut string_buf) = string_buffer.take() {
                        if string_buf.last() == Some(&b'"') {
                            string_buf.pop();
                        }
                        candidate_key = Some(string_buf);
                    }
                }
                continue;
            }
            if byte == b'"' {
                in_string = true;
                escape = false;
                if depth == 1 && active_name.is_none() && capture_depth == 0 {
                    string_buffer = Some(Vec::new());
                } else {
                    string_buffer = None;
                }
                continue;
            }
            if let Some(candidate) = candidate_key.take() {
                if byte.is_ascii_whitespace() {
                    candidate_key = Some(candidate);
                    continue;
                }
                if byte == b':' {
                    pending_key = Some(candidate);
                    continue;
                }
            }
            if let Some(pending) = pending_key.take() {
                if byte.is_ascii_whitespace() {
                    pending_key = Some(pending);
                    continue;
                }
                if byte == b'[' && targets.contains_key(&pending) && depth == 1 {
                    depth += 1;
                    active_name = targets.get(&pending).cloned();
                    active_array_depth = depth;
                    continue;
                }
            }
            if active_name.is_some()
                && capture_depth == 0
                && byte == b'{'
                && depth == active_array_depth
            {
                capture.clear();
                capture.push(b'{');
                capture_depth = 1;
                depth += 1;
                continue;
            }
            match byte {
                b'{' | b'[' => {
                    if capture_depth > 0 {
                        capture_depth += 1;
                    }
                    depth += 1;
                }
                b'}' | b']' => {
                    if capture_depth > 0 {
                        capture_depth -= 1;
                        if capture_depth == 0 {
                            if let Some(ref name) = active_name {
                                let emitted = process_compact_object_maybe_split(
                                    &mut writer,
	                                    &mut compact_copy_writer,
	                                    &mut provider_map,
	                                    &mut emitted_price_sets,
	                                    &mut emitted_provider_sets,
	                                    &mut emitted_procedures,
	                                    name,
                                    &capture,
                                    &snapshot_id,
                                    &plan_id,
                                    &plan_month_id,
                                    &source_trace_set_hash,
                                    &confidence_code,
                                    split_in_network,
                                    negotiated_rate_chunk_size,
                                )?;
	                                *object_counts.entry(name.clone()).or_insert(0) += emitted as u64;
	                                if let Some(copy_writer) = compact_copy_writer.as_mut() {
	                                    copy_writer.maybe_rotate(&mut writer)?;
	                                }
	                                let bytes = compressed_bytes_read.load(Ordering::Relaxed);
                                if progress_bytes_interval > 0 && bytes >= next_progress_bytes {
                                    emit_progress(
                                        path,
                                        total_bytes,
                                        &compressed_bytes_read,
                                        &object_counts,
                                        false,
                                    );
                                    while bytes >= next_progress_bytes {
                                        next_progress_bytes += progress_bytes_interval;
                                    }
                                }
                            }
                            capture.clear();
                            depth = depth.saturating_sub(1);
                            continue;
                        }
                    }
                    if active_name.is_some() && byte == b']' && depth == active_array_depth {
                        active_name = None;
                        active_array_depth = 0;
                    }
                    depth = depth.saturating_sub(1);
                }
                _ => {}
            }
        }
    }
    if let Some(copy_writer) = compact_copy_writer.take() {
        copy_writer.finish(&mut writer)?;
    }
    writer.flush()?;
    emit_progress(
        path,
        total_bytes,
        &compressed_bytes_read,
        &object_counts,
        true,
    );
    Ok(())
}

fn main() -> io::Result<()> {
    let mut args = env::args().skip(1);
    let mut first_arg = args.next().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "usage: ptg2_scanner [--compact-serving|--split-in-network] <path> <top_level_array>...",
        )
    })?;
    if first_arg == "--compact-serving" {
        let compact_path = args.next().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "usage: ptg2_scanner --compact-serving <path>",
            )
        })?;
        return scan_compact(Path::new(&compact_path));
    }
    let mut split_in_network = false;
    if first_arg == "--split-in-network" {
        split_in_network = true;
        first_arg = args.next().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "usage: ptg2_scanner --split-in-network <path> <top_level_array>...",
            )
        })?;
    }
    let arrays: Vec<String> = args.collect();
    if arrays.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "at least one top-level array name is required",
        ));
    }
    scan(Path::new(&first_arg), &arrays, split_in_network)
}
