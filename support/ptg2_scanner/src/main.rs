use crossbeam_channel::{bounded, Receiver, Sender, TrySendError};
use flate2::write::ZlibEncoder;
use flate2::Compression;
use ptg2_scanner::address_canon::{canon_version_json, canonicalize_copy_file};
use ptg2_scanner::config::{
    env_bool, env_usize, progress_interval, split_interval, DEFAULT_COMPACT_COPY_ROTATE_BYTES,
    DEFAULT_COMPACT_RUST_WORKERS, DEFAULT_COMPACT_RUST_WORK_QUEUE, DEFAULT_PARSE_IN_WORKERS,
    DEFAULT_PROGRESS_BYTES, DEFAULT_PROGRESS_OBJECTS, DEFAULT_RAW_CHUNK_BYTES,
    DEFAULT_SPLIT_NEGOTIATED_RATES, READ_BUF_SIZE,
};
use ptg2_scanner::copy_format::{
    emit_compact_copy_row, emit_manifest_lean_serving_copy_row, emit_manifest_serving_copy_row,
    pg_text_array_field, pg_text_copy_field, write_copy_fields, CompactCopyRow,
    ManifestLeanServingCopyRow, ManifestServingCopyRow,
};
use ptg2_scanner::dedupe::{dedupe_summary_payload, emit_dedupe_summary, SharedDedupe};
use ptg2_scanner::hashing::{
    checksum_i64_list, finish_hash_hex, hash_i64_list, hash_string_list, hash_text, make_checksum,
    semantic_hash, update_hash_optional_str, update_hash_string_list, xxh3_63,
};
use ptg2_scanner::input::{open_json_reader, open_reader};
use ptg2_scanner::manifest::{
    normalized_sidecar_entries, price_set_global_id_from_atom_ids, procedure_global_id,
    provider_set_global_id_from_entry_hashes, write_dense_member_sidecar, write_global_sidecar,
    GlobalId128, SidecarEntry, GLOBAL_ID_BYTES,
};
use ptg2_scanner::normalize::{
    canonical_text_list, normalize_code, normalize_string, normalize_tin_type, normalize_tin_value,
    normalized_money_from_reader, normalized_scalar_from_reader,
    normalized_string_list_from_reader, npi_list,
};
use ptg2_scanner::output::{emit_json_record, emit_object};
use ptg2_scanner::progress::emit_progress;
use serde_json::{json, Map, Value};
use std::any::Any;
use std::cmp::Ordering as CmpOrdering;
use std::collections::{BTreeMap, BinaryHeap, HashMap, HashSet};
use std::env;
use std::fmt::Display;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::{self, BufRead, BufReader, BufWriter, Cursor, Read, Write};
use std::panic::{self, AssertUnwindSafe};
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Mutex,
};
use std::thread;
use std::time::Instant;
use struson::reader::{JsonReader, JsonStreamReader};
use struson::writer::{JsonStreamWriter, JsonWriter};
use xxhash_rust::xxh3::Xxh3;

const DEFAULT_PROVIDER_REF_CHUNK_ITEMS: usize = 1024;
const MAX_RETAINED_CAPTURE_BYTES: usize = READ_BUF_SIZE;

fn to_io_error(error: impl Display) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error.to_string())
}

fn scan(path: &Path, requested: &[String]) -> io::Result<()> {
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
    let started_at = Instant::now();
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
                                emit_object(&mut writer, name, &capture)?;
                                let count = object_counts.entry(name.clone()).or_insert(0);
                                *count += 1;
                                let objects: u64 = object_counts.values().sum();
                                let bytes = compressed_bytes_read.load(Ordering::Relaxed);
                                if progress_bytes_interval > 0 && bytes >= next_progress_bytes {
                                    emit_progress(
                                        path,
                                        total_bytes,
                                        &compressed_bytes_read,
                                        &object_counts,
                                        started_at,
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
                                        started_at,
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
        started_at,
        true,
    );
    Ok(())
}

#[derive(Clone, Debug)]
struct ProviderEntry {
    entry_hash: i64,
    provider_count: i64,
    provider_group_hashes: Vec<i64>,
    npi: Vec<i64>,
}

enum ProviderEntryView<'a> {
    Borrowed(&'a ProviderEntry),
    Owned(ProviderEntry),
}

impl<'a> ProviderEntryView<'a> {
    fn entry_hash(&self) -> i64 {
        match self {
            Self::Borrowed(entry) => entry.entry_hash,
            Self::Owned(entry) => entry.entry_hash,
        }
    }

    fn provider_count(&self) -> i64 {
        match self {
            Self::Borrowed(entry) => entry.provider_count,
            Self::Owned(entry) => entry.provider_count,
        }
    }

    fn provider_group_hashes(&self) -> &[i64] {
        match self {
            Self::Borrowed(entry) => &entry.provider_group_hashes,
            Self::Owned(entry) => &entry.provider_group_hashes,
        }
    }

    fn npi(&self) -> &[i64] {
        match self {
            Self::Borrowed(entry) => &entry.npi,
            Self::Owned(entry) => &entry.npi,
        }
    }
}

#[derive(Clone, Debug)]
struct RateLite {
    provider_refs: Vec<String>,
    provider_groups: Vec<Value>,
    network_names: Vec<String>,
    prices: Vec<PriceLite>,
}

#[derive(Clone, Debug)]
struct PriceLite {
    negotiated_type: Option<String>,
    negotiated_rate: String,
    expiration_date: Option<String>,
    service_code: Vec<String>,
    billing_class: Option<String>,
    setting: Option<String>,
    billing_code_modifier: Vec<String>,
    additional_information: Option<String>,
}

#[derive(Clone, Debug)]
struct PriceAtomLite {
    price_atom_hash: String,
    negotiated_type: Option<String>,
    negotiated_rate: String,
    expiration_date: Option<String>,
    service_code_set_hash: String,
    service_code: Vec<String>,
    billing_class: Option<String>,
    setting: Option<String>,
    billing_code_modifier_set_hash: String,
    billing_code_modifier: Vec<String>,
    additional_information: Option<String>,
}

#[derive(Clone, Debug)]
struct PriceSetLite {
    price_set_hash: String,
    atoms: Vec<PriceAtomLite>,
    price_atom_hashes: Vec<String>,
}

type PriceCodeSetHashCache = HashMap<Vec<String>, String>;
type ServingProviderEntries = Vec<(u64, u32)>;
type ServingProviderPattern = (ServingProviderEntries, Vec<i32>);
type ServingProviderPatternMap = HashMap<ServingProviderEntries, Vec<i32>>;

#[derive(Default)]
struct ServingProviderBlockStats {
    record_count: u64,
    block_count: u64,
    pattern_count: u64,
}

#[derive(Default)]
struct ManifestGlobalIdCache {
    provider_sets: HashMap<String, (GlobalId128, String)>,
    price_sets: HashMap<String, (GlobalId128, String)>,
}

impl ManifestGlobalIdCache {
    fn provider_set_id(
        &mut self,
        provider_set_hash: &str,
        sorted_provider_entry_hashes: &[i64],
    ) -> GlobalId128 {
        if let Some((global_id, _hex)) = self.provider_sets.get(provider_set_hash) {
            return *global_id;
        }
        let global_id = provider_set_global_id_from_entry_hashes(sorted_provider_entry_hashes);
        let global_id_hex = global_id.to_hex();
        self.provider_sets
            .insert(provider_set_hash.to_string(), (global_id, global_id_hex));
        global_id
    }

    fn provider_set_id_hex(
        &mut self,
        provider_set_hash: &str,
        sorted_provider_entry_hashes: &[i64],
    ) -> String {
        if let Some((_global_id, global_id_hex)) = self.provider_sets.get(provider_set_hash) {
            return global_id_hex.clone();
        }
        let global_id = provider_set_global_id_from_entry_hashes(sorted_provider_entry_hashes);
        let global_id_hex = global_id.to_hex();
        self.provider_sets.insert(
            provider_set_hash.to_string(),
            (global_id, global_id_hex.clone()),
        );
        global_id_hex
    }

    fn price_set_id(&mut self, price_set: &PriceSetLite) -> GlobalId128 {
        if let Some((global_id, _hex)) = self.price_sets.get(&price_set.price_set_hash) {
            return *global_id;
        }
        let global_id = price_set_global_id(price_set);
        let global_id_hex = global_id.to_hex();
        self.price_sets
            .insert(price_set.price_set_hash.clone(), (global_id, global_id_hex));
        global_id
    }

    fn price_set_id_hex(&mut self, price_set: &PriceSetLite) -> String {
        if let Some((_global_id, global_id_hex)) = self.price_sets.get(&price_set.price_set_hash) {
            return global_id_hex.clone();
        }
        let global_id = price_set_global_id(price_set);
        let global_id_hex = global_id.to_hex();
        self.price_sets.insert(
            price_set.price_set_hash.clone(),
            (global_id, global_id_hex.clone()),
        );
        global_id_hex
    }
}

#[derive(Clone, Default)]
struct CopyPathConfig {
    compact: Option<String>,
    manifest_serving: Option<String>,
    manifest_lean_serving: Option<String>,
    manifest_provider_forward_sidecar: Option<String>,
    manifest_provider_inverted_sidecar: Option<String>,
    manifest_provider_npi_sidecar: Option<String>,
    manifest_price_forward_sidecar: Option<String>,
    manifest_price_atom: Option<String>,
    manifest_price_set_atom: Option<String>,
    manifest_provider_group_member: Option<String>,
    manifest_code_count: Option<String>,
    manifest_provider_set_dictionary: Option<String>,
    procedure: Option<String>,
    price_code_set: Option<String>,
    price_atom: Option<String>,
    price_set_entry: Option<String>,
    provider_set: Option<String>,
    provider_set_component: Option<String>,
    provider_set_entry: Option<String>,
    provider_entry_component: Option<String>,
    provider_group_member: Option<String>,
    manifest_only: bool,
}

impl CopyPathConfig {
    fn from_env() -> Self {
        Self {
            compact: env_path("HLTHPRT_PTG2_COMPACT_SERVING_COPY_PATH"),
            manifest_serving: env_path("HLTHPRT_PTG2_MANIFEST_SERVING_COPY_PATH"),
            manifest_lean_serving: env_path("HLTHPRT_PTG2_MANIFEST_LEAN_SERVING_COPY_PATH"),
            manifest_provider_forward_sidecar: env_path(
                "HLTHPRT_PTG2_MANIFEST_PROVIDER_FORWARD_SIDECAR_PATH",
            ),
            manifest_provider_inverted_sidecar: env_path(
                "HLTHPRT_PTG2_MANIFEST_PROVIDER_INVERTED_SIDECAR_PATH",
            ),
            manifest_provider_npi_sidecar: env_path(
                "HLTHPRT_PTG2_MANIFEST_PROVIDER_NPI_SIDECAR_PATH",
            ),
            manifest_price_forward_sidecar: env_path(
                "HLTHPRT_PTG2_MANIFEST_PRICE_FORWARD_SIDECAR_PATH",
            ),
            manifest_price_atom: env_path("HLTHPRT_PTG2_MANIFEST_PRICE_ATOM_COPY_PATH"),
            manifest_price_set_atom: env_path("HLTHPRT_PTG2_MANIFEST_PRICE_SET_ATOM_COPY_PATH"),
            manifest_provider_group_member: env_path(
                "HLTHPRT_PTG2_MANIFEST_PROVIDER_GROUP_MEMBER_COPY_PATH",
            ),
            manifest_code_count: env_path("HLTHPRT_PTG2_MANIFEST_CODE_COUNT_COPY_PATH"),
            manifest_provider_set_dictionary: env_path(
                "HLTHPRT_PTG2_MANIFEST_PROVIDER_SET_DICTIONARY_COPY_PATH",
            ),
            procedure: env_path("HLTHPRT_PTG2_PROCEDURE_COPY_PATH"),
            price_code_set: env_path("HLTHPRT_PTG2_PRICE_CODE_SET_COPY_PATH"),
            price_atom: env_path("HLTHPRT_PTG2_PRICE_ATOM_COPY_PATH"),
            price_set_entry: env_path("HLTHPRT_PTG2_PRICE_SET_ENTRY_COPY_PATH"),
            provider_set: env_path("HLTHPRT_PTG2_PROVIDER_SET_COPY_PATH"),
            provider_set_component: env_path("HLTHPRT_PTG2_PROVIDER_SET_COMPONENT_COPY_PATH"),
            provider_set_entry: env_path("HLTHPRT_PTG2_PROVIDER_SET_ENTRY_COPY_PATH"),
            provider_entry_component: env_path("HLTHPRT_PTG2_PROVIDER_ENTRY_COMPONENT_COPY_PATH"),
            provider_group_member: env_path("HLTHPRT_PTG2_PROVIDER_GROUP_MEMBER_COPY_PATH"),
            manifest_only: env_bool("HLTHPRT_PTG2_MANIFEST_ONLY", false),
        }
    }

    fn has_file_paths(&self) -> bool {
        self.compact.is_some()
            || self.manifest_serving.is_some()
            || self.manifest_lean_serving.is_some()
            || self.has_manifest_sidecar_paths()
            || self.manifest_price_atom.is_some()
            || self.manifest_price_set_atom.is_some()
            || self.manifest_provider_group_member.is_some()
            || self.manifest_code_count.is_some()
            || self.manifest_provider_set_dictionary.is_some()
            || self.procedure.is_some()
            || self.price_code_set.is_some()
            || self.price_atom.is_some()
            || self.price_set_entry.is_some()
            || self.provider_set.is_some()
            || self.provider_set_component.is_some()
            || self.provider_set_entry.is_some()
            || self.provider_entry_component.is_some()
            || self.provider_group_member.is_some()
    }

    fn has_manifest_sidecar_paths(&self) -> bool {
        self.manifest_provider_forward_sidecar.is_some()
            || self.manifest_provider_inverted_sidecar.is_some()
            || self.manifest_provider_npi_sidecar.is_some()
            || self.manifest_price_forward_sidecar.is_some()
    }

    fn for_worker(&self, worker_id: usize) -> Self {
        let suffix = format!(".worker{:04}", worker_id);
        Self {
            compact: self.compact.as_ref().map(|path| format!("{path}{suffix}")),
            manifest_serving: self
                .manifest_serving
                .as_ref()
                .map(|path| format!("{path}{suffix}")),
            manifest_lean_serving: self
                .manifest_lean_serving
                .as_ref()
                .map(|path| format!("{path}{suffix}")),
            manifest_provider_forward_sidecar: self.manifest_provider_forward_sidecar.clone(),
            manifest_provider_inverted_sidecar: self.manifest_provider_inverted_sidecar.clone(),
            manifest_provider_npi_sidecar: self.manifest_provider_npi_sidecar.clone(),
            manifest_price_forward_sidecar: self.manifest_price_forward_sidecar.clone(),
            manifest_price_atom: self
                .manifest_price_atom
                .as_ref()
                .map(|path| format!("{path}{suffix}")),
            manifest_price_set_atom: self
                .manifest_price_set_atom
                .as_ref()
                .map(|path| format!("{path}{suffix}")),
            manifest_provider_group_member: self
                .manifest_provider_group_member
                .as_ref()
                .map(|path| format!("{path}{suffix}")),
            manifest_code_count: self
                .manifest_code_count
                .as_ref()
                .map(|path| format!("{path}{suffix}")),
            manifest_provider_set_dictionary: self
                .manifest_provider_set_dictionary
                .as_ref()
                .map(|path| format!("{path}{suffix}")),
            procedure: self
                .procedure
                .as_ref()
                .map(|path| format!("{path}{suffix}")),
            price_code_set: self
                .price_code_set
                .as_ref()
                .map(|path| format!("{path}{suffix}")),
            price_atom: self
                .price_atom
                .as_ref()
                .map(|path| format!("{path}{suffix}")),
            price_set_entry: self
                .price_set_entry
                .as_ref()
                .map(|path| format!("{path}{suffix}")),
            provider_set: self
                .provider_set
                .as_ref()
                .map(|path| format!("{path}{suffix}")),
            provider_set_component: self
                .provider_set_component
                .as_ref()
                .map(|path| format!("{path}{suffix}")),
            provider_set_entry: self
                .provider_set_entry
                .as_ref()
                .map(|path| format!("{path}{suffix}")),
            provider_entry_component: self
                .provider_entry_component
                .as_ref()
                .map(|path| format!("{path}{suffix}")),
            provider_group_member: self
                .provider_group_member
                .as_ref()
                .map(|path| format!("{path}{suffix}")),
            manifest_only: self.manifest_only,
        }
    }

    fn for_provider_refs(&self) -> Self {
        let suffix = ".provider_refs";
        Self {
            compact: None,
            manifest_serving: None,
            manifest_lean_serving: None,
            manifest_provider_forward_sidecar: None,
            manifest_provider_inverted_sidecar: None,
            manifest_provider_npi_sidecar: None,
            manifest_price_forward_sidecar: None,
            manifest_price_atom: None,
            manifest_price_set_atom: None,
            manifest_provider_group_member: self
                .manifest_provider_group_member
                .as_ref()
                .map(|path| format!("{path}{suffix}")),
            manifest_code_count: None,
            manifest_provider_set_dictionary: None,
            procedure: None,
            price_code_set: None,
            price_atom: None,
            price_set_entry: None,
            provider_set: None,
            provider_set_component: None,
            provider_set_entry: None,
            provider_entry_component: None,
            provider_group_member: self
                .provider_group_member
                .as_ref()
                .map(|path| format!("{path}{suffix}")),
            manifest_only: self.manifest_only,
        }
    }

    fn manifest_serving_copy_path(&self) -> Option<&String> {
        self.manifest_lean_serving
            .as_ref()
            .or(self.manifest_serving.as_ref())
    }

    fn manifest_serving_copy_layout(&self) -> ManifestServingCopyLayout {
        if self.manifest_lean_serving.is_some() {
            ManifestServingCopyLayout::Lean
        } else {
            ManifestServingCopyLayout::Full
        }
    }
}

fn env_path(name: &str) -> Option<String> {
    env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn env_json_text_list(name: &str) -> Vec<String> {
    let Some(raw) = env_path(name) else {
        return Vec::new();
    };
    let Ok(value) = serde_json::from_str::<Value>(&raw) else {
        return Vec::new();
    };
    let mut values = Vec::new();
    match value {
        Value::Array(items) => {
            for item in items {
                if let Some(text) = normalize_string(Some(&item)) {
                    values.push(text);
                }
            }
        }
        item => {
            if let Some(text) = normalize_string(Some(&item)) {
                values.push(text);
            }
        }
    }
    canonical_text_list(values, false)
}

struct CopyFileEvent {
    record_kind: String,
    path: String,
    bytes: u64,
    row_count: u64,
    final_file: bool,
}

fn emit_copy_file_event<W: Write>(writer: &mut W, event: &CopyFileEvent) -> io::Result<()> {
    emit_json_record(
        writer,
        &event.record_kind,
        &json!({
            "path": event.path,
            "bytes": event.bytes,
            "row_count": event.row_count,
            "final": event.final_file,
        }),
    )
}

fn drain_copy_file_events<W: Write>(
    event_rx: &Receiver<CopyFileEvent>,
    writer: &mut W,
) -> io::Result<()> {
    let mut emitted = false;
    for event in event_rx.try_iter() {
        emit_copy_file_event(writer, &event)?;
        emitted = true;
    }
    if emitted {
        writer.flush()?;
    }
    Ok(())
}

fn send_worker_job<W: Write>(
    tx: &Sender<WorkerJob>,
    event_rx: &Receiver<CopyFileEvent>,
    writer: &mut W,
    producer_blocked_micros: &mut u128,
    mut job: WorkerJob,
) -> io::Result<()> {
    let mut blocked_since: Option<Instant> = None;
    loop {
        match tx.try_send(job) {
            Ok(()) => {
                if let Some(started_at) = blocked_since.take() {
                    *producer_blocked_micros =
                        producer_blocked_micros.saturating_add(started_at.elapsed().as_micros());
                }
                return Ok(());
            }
            Err(TrySendError::Full(returned_job)) => {
                if blocked_since.is_none() {
                    blocked_since = Some(Instant::now());
                }
                job = returned_job;
                drain_copy_file_events(event_rx, writer)?;
                thread::yield_now();
            }
            Err(TrySendError::Disconnected(returned_job)) => {
                return Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    format!(
                        "compact worker queue closed while sending {}",
                        returned_job.name()
                    ),
                ));
            }
        }
    }
}

fn send_provider_ref_batch<W: Write>(
    tx: &Sender<RawRateChunk>,
    event_rx: &Receiver<CopyFileEvent>,
    writer: &mut W,
    producer_blocked_micros: &mut u128,
    mut batch: RawRateChunk,
) -> io::Result<()> {
    let mut blocked_since: Option<Instant> = None;
    loop {
        match tx.try_send(batch) {
            Ok(()) => {
                if let Some(started_at) = blocked_since.take() {
                    *producer_blocked_micros =
                        producer_blocked_micros.saturating_add(started_at.elapsed().as_micros());
                }
                return Ok(());
            }
            Err(TrySendError::Full(returned_batch)) => {
                if blocked_since.is_none() {
                    blocked_since = Some(Instant::now());
                }
                batch = returned_batch;
                drain_copy_file_events(event_rx, writer)?;
                thread::yield_now();
            }
            Err(TrySendError::Disconnected(_returned_batch)) => {
                return Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "provider-reference worker queue closed while sending raw batch",
                ));
            }
        }
    }
}

fn panic_payload_message(payload: &(dyn Any + Send + 'static)) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        return (*message).to_string();
    }
    if let Some(message) = payload.downcast_ref::<String>() {
        return message.clone();
    }
    "non-string panic payload".to_string()
}

fn emit_worker_failure<W: Write>(
    writer: &mut W,
    worker_id: usize,
    failure_type: &str,
    message: &str,
) -> io::Result<()> {
    eprintln!(
        "PTG2_SCANNER_WORKER_FAILED\tworker_id={worker_id}\ttype={failure_type}\terror={message}"
    );
    emit_json_record(
        writer,
        "scanner_worker_error",
        &json!({
            "worker_id": worker_id,
            "type": failure_type,
            "error": message,
        }),
    )?;
    writer.flush()
}

fn log_worker_failure(worker_id: usize, failure_type: &str, message: &str) {
    eprintln!(
        "PTG2_SCANNER_WORKER_FAILED\tworker_id={worker_id}\ttype={failure_type}\terror={message}"
    );
}

fn provider_group_hash(tin: &Value, npi: &[i64]) -> i64 {
    make_checksum(vec![
        json!("provider_group"),
        json!(normalize_tin_type(tin.get("type"))),
        json!(normalize_tin_value(tin.get("value"))),
        json!(npi),
    ])
}

fn provider_group_payload_canonical_json(
    provider_group_hash: i64,
    tin_type: &str,
    tin_value: &str,
    npi: &[i64],
) -> String {
    let tin_type_json = serde_json::to_string(tin_type).unwrap_or_else(|_| "\"\"".to_string());
    let tin_value_json = serde_json::to_string(tin_value).unwrap_or_else(|_| "\"\"".to_string());
    let npi_json = serde_json::to_string(npi).unwrap_or_else(|_| "[]".to_string());
    format!(
        "{{\"npi\":{npi_json},\"provider_group_hash\":{provider_group_hash},\"tin_type\":{tin_type_json},\"tin_value\":{tin_value_json}}}"
    )
}

fn provider_set_checksum_from_group_payloads(mut group_payload_jsons: Vec<String>) -> i64 {
    group_payload_jsons.sort_unstable();
    let mut payload = String::from("[\"provider_set\",[");
    for (idx, item) in group_payload_jsons.iter().enumerate() {
        if idx > 0 {
            payload.push(',');
        }
        payload.push_str(item);
    }
    payload.push_str("]]");
    xxh3_63(payload.as_bytes()) as i64
}

fn build_provider_entry(provider_ref: &Value, collect_npis: bool) -> Option<ProviderEntry> {
    let groups = provider_ref.get("provider_groups")?.as_array()?;
    let build_provider_set_payload = groups.len() > 1;
    let mut group_payload_jsons: Vec<String> = Vec::new();
    let mut group_hashes: Vec<i64> = Vec::new();
    let mut provider_npis: Vec<i64> = Vec::new();
    let mut provider_count = 0i64;
    for group in groups {
        let tin = group.get("tin").unwrap_or(&Value::Null);
        let npi = npi_list(group.get("npi"));
        let tin_type = normalize_tin_type(tin.get("type"));
        let tin_value = normalize_tin_value(tin.get("value"));
        let group_hash = make_checksum(vec![
            json!("provider_group"),
            json!(tin_type.clone()),
            json!(tin_value.clone()),
            json!(npi),
        ]);
        provider_count += npi.len() as i64;
        group_hashes.push(group_hash);
        if collect_npis {
            provider_npis.extend(npi.iter().copied());
        }
        if build_provider_set_payload {
            group_payload_jsons.push(provider_group_payload_canonical_json(
                group_hash, &tin_type, &tin_value, &npi,
            ));
        }
    }
    if group_hashes.is_empty() {
        return None;
    }
    group_hashes.sort_unstable();
    group_hashes.dedup();
    group_hashes.shrink_to_fit();
    if collect_npis {
        provider_npis.sort_unstable();
        provider_npis.dedup();
        provider_npis.shrink_to_fit();
    }
    let entry_hash = if build_provider_set_payload {
        provider_set_checksum_from_group_payloads(group_payload_jsons)
    } else {
        group_hashes[0]
    };
    Some(ProviderEntry {
        entry_hash,
        provider_count,
        provider_group_hashes: group_hashes,
        npi: provider_npis,
    })
}

fn provider_ref_key(value: &Value) -> Option<String> {
    normalize_string(Some(value))
}

fn provider_set_from_ref_keys(
    provider_map: &HashMap<String, ProviderEntry>,
    refs: &[String],
) -> Option<ProviderEntry> {
    let mut entry_hashes: HashSet<i64> = HashSet::new();
    let mut group_hashes: HashSet<i64> = HashSet::new();
    let mut provider_npis: HashSet<i64> = HashSet::new();
    let mut provider_count = 0i64;
    for key in refs {
        let entry = provider_map.get(key)?;
        if entry_hashes.insert(entry.entry_hash) {
            provider_count += entry.provider_count;
            for group_hash in &entry.provider_group_hashes {
                group_hashes.insert(*group_hash);
            }
            for npi in &entry.npi {
                provider_npis.insert(*npi);
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
    let mut sorted_provider_npis: Vec<i64> = provider_npis.into_iter().collect();
    sorted_provider_npis.sort_unstable();
    let entry_hash = if sorted_entry_hashes.len() == 1 {
        sorted_entry_hashes[0]
    } else {
        checksum_i64_list("provider_rate_provider_set", &sorted_entry_hashes)
    };
    Some(ProviderEntry {
        entry_hash,
        provider_count,
        provider_group_hashes: sorted_group_hashes,
        npi: sorted_provider_npis,
    })
}

fn provider_entry_view_from_ref_keys<'a>(
    provider_map: &'a HashMap<String, ProviderEntry>,
    refs: &[String],
) -> Option<ProviderEntryView<'a>> {
    if refs.len() == 1 {
        return provider_map.get(&refs[0]).map(ProviderEntryView::Borrowed);
    }
    provider_set_from_ref_keys(provider_map, refs).map(ProviderEntryView::Owned)
}

fn price_atom_from_lite(
    price: &PriceLite,
    price_code_set_hash_cache: &mut PriceCodeSetHashCache,
) -> PriceAtomLite {
    let price_atom_hash = price_atom_hash(price);
    let service_code_set_hash =
        price_code_set_hash_cached(&price.service_code, price_code_set_hash_cache);
    let billing_code_modifier_set_hash =
        price_code_set_hash_cached(&price.billing_code_modifier, price_code_set_hash_cache);
    PriceAtomLite {
        price_atom_hash,
        negotiated_type: price.negotiated_type.clone(),
        negotiated_rate: price.negotiated_rate.clone(),
        expiration_date: price.expiration_date.clone(),
        service_code_set_hash,
        service_code: price.service_code.clone(),
        billing_class: price.billing_class.clone(),
        setting: price.setting.clone(),
        billing_code_modifier_set_hash,
        billing_code_modifier: price.billing_code_modifier.clone(),
        additional_information: price.additional_information.clone(),
    }
}

fn price_atom_hash(price: &PriceLite) -> String {
    let mut hasher = Xxh3::new();
    hasher.update(b"price_atom");
    update_hash_optional_str(&mut hasher, price.negotiated_type.as_deref());
    update_hash_optional_str(&mut hasher, Some(price.negotiated_rate.as_str()));
    update_hash_optional_str(&mut hasher, price.expiration_date.as_deref());
    update_hash_string_list(&mut hasher, &price.service_code);
    update_hash_optional_str(&mut hasher, price.billing_class.as_deref());
    update_hash_optional_str(&mut hasher, price.setting.as_deref());
    update_hash_string_list(&mut hasher, &price.billing_code_modifier);
    update_hash_optional_str(&mut hasher, price.additional_information.as_deref());
    finish_hash_hex(hasher)
}

fn price_code_set_hash(codes: &[String]) -> String {
    hash_string_list("price_code_set", codes)
}

fn price_code_set_hash_cached(codes: &[String], cache: &mut PriceCodeSetHashCache) -> String {
    if let Some(hash) = cache.get(codes) {
        return hash.clone();
    }
    let hash = price_code_set_hash(codes);
    cache.insert(codes.to_vec(), hash.clone());
    hash
}

fn price_lite_set(
    prices: &[PriceLite],
    price_code_set_hash_cache: &mut PriceCodeSetHashCache,
) -> Option<PriceSetLite> {
    let mut atoms: Vec<PriceAtomLite> = Vec::new();
    for price in prices {
        atoms.push(price_atom_from_lite(price, price_code_set_hash_cache));
    }
    if atoms.is_empty() {
        return None;
    }
    let mut unique_hashes: Vec<String> = atoms
        .iter()
        .map(|atom| atom.price_atom_hash.clone())
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    unique_hashes.sort_unstable();
    let price_set_hash = hash_string_list("price_set", &unique_hashes);
    Some(PriceSetLite {
        price_set_hash,
        atoms,
        price_atom_hashes: unique_hashes,
    })
}

fn price_atom_global_id(atom: &PriceAtomLite) -> GlobalId128 {
    GlobalId128::from_price_atom_parts(
        atom.negotiated_type.as_deref(),
        Some(&atom.negotiated_rate),
        atom.expiration_date.as_deref(),
        &atom.service_code,
        atom.billing_class.as_deref(),
        atom.setting.as_deref(),
        &atom.billing_code_modifier,
        atom.additional_information.as_deref(),
    )
}

fn price_set_global_id(price_set: &PriceSetLite) -> GlobalId128 {
    let atom_ids: Vec<GlobalId128> = price_set.atoms.iter().map(price_atom_global_id).collect();
    price_set_global_id_from_atom_ids(&atom_ids)
}

fn provider_group_global_id_from_hash(provider_group_hash: i64) -> GlobalId128 {
    let hash_text = provider_group_hash.to_string();
    GlobalId128::from_parts("provider_group_manifest", &[&hash_text])
}

fn npi_member_id(npi: i64) -> GlobalId128 {
    let mut out = [0u8; 16];
    out[8..16].copy_from_slice(&(npi as u64).to_be_bytes());
    GlobalId128(out)
}

struct ManifestServingIdentityHex {
    serving_content_hash_128: String,
    procedure_global_id_128: String,
    provider_set_global_id_128: String,
    price_set_global_id_128: String,
}

struct ManifestLeanServingIdentityHex {
    provider_set_global_id_128: String,
    price_set_global_id_128: String,
}

#[derive(Clone, Copy)]
enum ManifestServingCopyLayout {
    Full,
    Lean,
}

impl ManifestServingCopyLayout {
    fn record_kind(self) -> &'static str {
        match self {
            ManifestServingCopyLayout::Full => "manifest_serving_copy_file",
            ManifestServingCopyLayout::Lean => "manifest_lean_serving_copy_file",
        }
    }
}

fn manifest_serving_identity_hex(
    plan_id: &str,
    procedure_payload: &Value,
    provider_set_hash: &str,
    sorted_provider_entry_hashes: &[i64],
    price_set: &PriceSetLite,
    cache: &mut ManifestGlobalIdCache,
) -> ManifestServingIdentityHex {
    let procedure_global_id = procedure_global_id(procedure_payload);
    let provider_set_global_id =
        cache.provider_set_id(provider_set_hash, sorted_provider_entry_hashes);
    let price_set_global_id = cache.price_set_id(price_set);
    let serving_content_hash = GlobalId128::serving_content(
        plan_id,
        procedure_global_id,
        provider_set_global_id,
        price_set_global_id,
    );

    ManifestServingIdentityHex {
        serving_content_hash_128: serving_content_hash.to_hex(),
        procedure_global_id_128: procedure_global_id.to_hex(),
        provider_set_global_id_128: provider_set_global_id.to_hex(),
        price_set_global_id_128: price_set_global_id.to_hex(),
    }
}

fn manifest_lean_serving_identity_hex(
    provider_set_hash: &str,
    sorted_provider_entry_hashes: &[i64],
    price_set: &PriceSetLite,
    cache: &mut ManifestGlobalIdCache,
) -> ManifestLeanServingIdentityHex {
    ManifestLeanServingIdentityHex {
        provider_set_global_id_128: cache
            .provider_set_id_hex(provider_set_hash, sorted_provider_entry_hashes),
        price_set_global_id_128: cache.price_set_id_hex(price_set),
    }
}

struct ManifestPairSpool {
    path: PathBuf,
    writer: BufWriter<File>,
    row_count: u64,
}

const MANIFEST_PAIR_RECORD_BYTES: usize = GLOBAL_ID_BYTES * 2;
const DEFAULT_MANIFEST_PAIR_SORT_CHUNK_BYTES: usize = 256 * 1024 * 1024;

impl ManifestPairSpool {
    fn new(kind: &str) -> io::Result<Self> {
        let base_dir = env::var_os("HLTHPRT_PTG2_MANIFEST_SPILL_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(env::temp_dir);
        std::fs::create_dir_all(&base_dir)?;
        let process_id = std::process::id();
        for attempt in 0..1000u32 {
            let path = base_dir.join(format!(
                "ptg2_manifest_{kind}_{process_id}_{:?}_{attempt}.pairs",
                thread::current().id()
            ));
            match OpenOptions::new().create_new(true).write(true).open(&path) {
                Ok(file) => {
                    return Ok(Self {
                        path,
                        writer: BufWriter::new(file),
                        row_count: 0,
                    });
                }
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(error),
            }
        }
        Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "unable to allocate PTG2 manifest spill file",
        ))
    }

    fn push(&mut self, owner: GlobalId128, member: GlobalId128) -> io::Result<()> {
        self.writer.write_all(&owner.0)?;
        self.writer.write_all(&member.0)?;
        self.row_count = self.row_count.saturating_add(1);
        Ok(())
    }

    fn entries(&mut self) -> io::Result<Vec<SidecarEntry>> {
        self.writer.flush()?;
        let file = File::open(&self.path)?;
        let mut reader = BufReader::new(file);
        let mut pairs = Vec::with_capacity(self.row_count.min(usize::MAX as u64) as usize);
        let mut buffer = [0u8; GLOBAL_ID_BYTES * 2];
        loop {
            match reader.read_exact(&mut buffer) {
                Ok(()) => {
                    let mut owner = [0u8; GLOBAL_ID_BYTES];
                    owner.copy_from_slice(&buffer[..GLOBAL_ID_BYTES]);
                    let mut member = [0u8; GLOBAL_ID_BYTES];
                    member.copy_from_slice(&buffer[GLOBAL_ID_BYTES..]);
                    pairs.push((GlobalId128(owner), vec![GlobalId128(member)]));
                }
                Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(error) => return Err(error),
            }
        }
        Ok(normalized_sidecar_entries(pairs))
    }

    fn write_standard_sidecar(&mut self, path: &str) -> io::Result<(u64, u64)> {
        let pairs = self.sorted_unique_pairs()?;
        let entry_count = count_pair_owners(&pairs);
        let member_count = pairs.len() as u64;

        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(path)?;
        let mut writer = BufWriter::new(file);
        writer.write_all(b"PTG2MNSC")?;
        writer.write_all(&1u32.to_le_bytes())?;
        writer.write_all(&entry_count.to_le_bytes())?;

        let mut offset = 0u64;
        let mut index = 0usize;
        while index < pairs.len() {
            let owner = &pairs[index][..GLOBAL_ID_BYTES];
            let start = index;
            index += 1;
            while index < pairs.len() && &pairs[index][..GLOBAL_ID_BYTES] == owner {
                index += 1;
            }
            let count = (index - start) as u32;
            writer.write_all(owner)?;
            writer.write_all(&offset.to_le_bytes())?;
            writer.write_all(&count.to_le_bytes())?;
            offset = offset.saturating_add(u64::from(count));
        }
        for pair in &pairs {
            writer.write_all(&pair[GLOBAL_ID_BYTES..])?;
        }
        writer.flush()?;
        Ok((entry_count, member_count))
    }

    fn write_dense_sidecar(&mut self, path: &str) -> io::Result<(u64, u64)> {
        let chunk_bytes = env_usize(
            "HLTHPRT_PTG2_MANIFEST_SIDECAR_SORT_CHUNK_BYTES",
            DEFAULT_MANIFEST_PAIR_SORT_CHUNK_BYTES,
        )
        .max(MANIFEST_PAIR_RECORD_BYTES);
        self.write_dense_sidecar_with_chunk_bytes(path, chunk_bytes)
    }

    fn write_dense_sidecar_with_chunk_bytes(
        &mut self,
        path: &str,
        chunk_bytes: usize,
    ) -> io::Result<(u64, u64)> {
        let spool_bytes = self
            .row_count
            .saturating_mul(MANIFEST_PAIR_RECORD_BYTES as u64);
        if spool_bytes <= chunk_bytes as u64 {
            return self.write_dense_sidecar_in_memory(path);
        }
        self.write_dense_sidecar_external(path, chunk_bytes)
    }

    fn write_dense_sidecar_in_memory(&mut self, path: &str) -> io::Result<(u64, u64)> {
        let pairs = self.sorted_unique_pairs()?;
        let entry_count = count_pair_owners(&pairs);
        let member_count = pairs.len() as u64;
        let mut member_ids: Vec<[u8; GLOBAL_ID_BYTES]> = pairs
            .iter()
            .map(|pair| {
                let mut member = [0u8; GLOBAL_ID_BYTES];
                member.copy_from_slice(&pair[GLOBAL_ID_BYTES..]);
                member
            })
            .collect();
        member_ids.sort_unstable();
        member_ids.dedup();

        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(path)?;
        let mut writer = BufWriter::new(file);
        writer.write_all(b"PTG2MNDS")?;
        writer.write_all(&1u32.to_le_bytes())?;
        writer.write_all(&entry_count.to_le_bytes())?;
        writer.write_all(&(member_ids.len() as u64).to_le_bytes())?;

        let mut offset = 0u64;
        let mut index = 0usize;
        while index < pairs.len() {
            let owner = &pairs[index][..GLOBAL_ID_BYTES];
            let start = index;
            index += 1;
            while index < pairs.len() && &pairs[index][..GLOBAL_ID_BYTES] == owner {
                index += 1;
            }
            let count = (index - start) as u32;
            writer.write_all(owner)?;
            writer.write_all(&offset.to_le_bytes())?;
            writer.write_all(&count.to_le_bytes())?;
            offset = offset.saturating_add(u64::from(count));
        }
        for member_id in &member_ids {
            writer.write_all(member_id)?;
        }
        for pair in &pairs {
            let mut member = [0u8; GLOBAL_ID_BYTES];
            member.copy_from_slice(&pair[GLOBAL_ID_BYTES..]);
            let local_id = member_ids.binary_search(&member).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "dense sidecar member is missing from local dictionary",
                )
            })? as u32;
            writer.write_all(&local_id.to_le_bytes())?;
        }
        writer.flush()?;
        Ok((entry_count, member_count))
    }

    fn write_dense_sidecar_external(
        &mut self,
        path: &str,
        chunk_bytes: usize,
    ) -> io::Result<(u64, u64)> {
        self.writer.flush()?;
        let mut temporary_files = ManifestPairTemporaryFiles::default();
        let chunk_paths = self.write_sorted_pair_chunks(chunk_bytes, &mut temporary_files)?;
        let merged = merge_sorted_pair_chunks(&self.path, &chunk_paths, &mut temporary_files)?;
        for chunk_path in &chunk_paths {
            let _ = std::fs::remove_file(chunk_path);
        }
        write_dense_sidecar_from_sorted_pairs(path, &merged)
    }

    fn write_sorted_pair_chunks(
        &self,
        chunk_bytes: usize,
        temporary_files: &mut ManifestPairTemporaryFiles,
    ) -> io::Result<Vec<PathBuf>> {
        let records_per_chunk = (chunk_bytes / MANIFEST_PAIR_RECORD_BYTES).max(1);
        let mut reader = BufReader::new(File::open(&self.path)?);
        let mut chunk_paths = Vec::new();
        loop {
            let mut pairs = Vec::with_capacity(records_per_chunk);
            while pairs.len() < records_per_chunk {
                let Some(pair) = read_manifest_pair(&mut reader)? else {
                    break;
                };
                pairs.push(pair);
            }
            if pairs.is_empty() {
                break;
            }
            pairs.sort_unstable();
            pairs.dedup();
            let chunk_path =
                manifest_pair_temporary_path(&self.path, "sorted-chunk", chunk_paths.len());
            let mut writer = BufWriter::new(File::create(&chunk_path)?);
            for pair in pairs {
                writer.write_all(&pair)?;
            }
            writer.flush()?;
            temporary_files.track(chunk_path.clone());
            chunk_paths.push(chunk_path);
        }
        Ok(chunk_paths)
    }

    fn sorted_unique_pairs(&mut self) -> io::Result<Vec<[u8; GLOBAL_ID_BYTES * 2]>> {
        self.writer.flush()?;
        let file = File::open(&self.path)?;
        let mut reader = BufReader::new(file);
        let mut pairs: Vec<[u8; GLOBAL_ID_BYTES * 2]> =
            Vec::with_capacity(self.row_count.min(usize::MAX as u64) as usize);
        loop {
            let mut pair = [0u8; GLOBAL_ID_BYTES * 2];
            match reader.read_exact(&mut pair) {
                Ok(()) => pairs.push(pair),
                Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(error) => return Err(error),
            }
        }
        pairs.sort_unstable();
        pairs.dedup();
        Ok(pairs)
    }
}

#[derive(Default)]
struct ManifestPairTemporaryFiles {
    paths: Vec<PathBuf>,
}

impl ManifestPairTemporaryFiles {
    fn track(&mut self, path: PathBuf) {
        self.paths.push(path);
    }
}

impl Drop for ManifestPairTemporaryFiles {
    fn drop(&mut self) {
        for path in &self.paths {
            let _ = std::fs::remove_file(path);
        }
    }
}

struct MergedManifestPairs {
    path: PathBuf,
    entry_count: u64,
    member_count: u64,
    member_ids: Vec<[u8; GLOBAL_ID_BYTES]>,
}

#[derive(Eq)]
struct ManifestPairMergeItem {
    pair: [u8; MANIFEST_PAIR_RECORD_BYTES],
    reader_index: usize,
}

impl Ord for ManifestPairMergeItem {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        other
            .pair
            .cmp(&self.pair)
            .then_with(|| other.reader_index.cmp(&self.reader_index))
    }
}

impl PartialOrd for ManifestPairMergeItem {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for ManifestPairMergeItem {
    fn eq(&self, other: &Self) -> bool {
        self.pair == other.pair && self.reader_index == other.reader_index
    }
}

fn manifest_pair_temporary_path(source_path: &Path, kind: &str, index: usize) -> PathBuf {
    let source_name = source_path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("ptg2-manifest-pairs");
    source_path.with_file_name(format!("{source_name}.{kind}.{index}"))
}

fn read_manifest_pair<R: Read>(
    reader: &mut R,
) -> io::Result<Option<[u8; MANIFEST_PAIR_RECORD_BYTES]>> {
    let mut pair = [0u8; MANIFEST_PAIR_RECORD_BYTES];
    let mut offset = 0usize;
    while offset < pair.len() {
        let bytes_read = reader.read(&mut pair[offset..])?;
        if bytes_read == 0 {
            if offset == 0 {
                return Ok(None);
            }
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "manifest pair spool ended with a partial record",
            ));
        }
        offset += bytes_read;
    }
    Ok(Some(pair))
}

fn merge_sorted_pair_chunks(
    source_path: &Path,
    chunk_paths: &[PathBuf],
    temporary_files: &mut ManifestPairTemporaryFiles,
) -> io::Result<MergedManifestPairs> {
    let merged_path = manifest_pair_temporary_path(source_path, "sorted-unique", 0);
    let mut output = BufWriter::new(File::create(&merged_path)?);
    temporary_files.track(merged_path.clone());
    let mut readers = Vec::with_capacity(chunk_paths.len());
    for chunk_path in chunk_paths {
        readers.push(BufReader::new(File::open(chunk_path)?));
    }
    let mut heap = BinaryHeap::new();
    for (reader_index, reader) in readers.iter_mut().enumerate() {
        if let Some(pair) = read_manifest_pair(reader)? {
            heap.push(ManifestPairMergeItem { pair, reader_index });
        }
    }

    let mut previous_pair = None;
    let mut previous_owner = None;
    let mut member_ids = HashSet::new();
    let mut entry_count = 0u64;
    let mut member_count = 0u64;
    while let Some(item) = heap.pop() {
        if previous_pair != Some(item.pair) {
            let mut owner = [0u8; GLOBAL_ID_BYTES];
            owner.copy_from_slice(&item.pair[..GLOBAL_ID_BYTES]);
            if previous_owner != Some(owner) {
                entry_count = entry_count.saturating_add(1);
                previous_owner = Some(owner);
            }
            let mut member = [0u8; GLOBAL_ID_BYTES];
            member.copy_from_slice(&item.pair[GLOBAL_ID_BYTES..]);
            member_ids.insert(member);
            output.write_all(&item.pair)?;
            member_count = member_count.saturating_add(1);
            previous_pair = Some(item.pair);
        }
        if let Some(pair) = read_manifest_pair(&mut readers[item.reader_index])? {
            heap.push(ManifestPairMergeItem {
                pair,
                reader_index: item.reader_index,
            });
        }
    }
    output.flush()?;
    let mut member_ids: Vec<[u8; GLOBAL_ID_BYTES]> = member_ids.into_iter().collect();
    member_ids.sort_unstable();
    Ok(MergedManifestPairs {
        path: merged_path,
        entry_count,
        member_count,
        member_ids,
    })
}

fn write_dense_sidecar_from_sorted_pairs(
    output_path: &str,
    merged: &MergedManifestPairs,
) -> io::Result<(u64, u64)> {
    let file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(output_path)?;
    let mut writer = BufWriter::new(file);
    writer.write_all(b"PTG2MNDS")?;
    writer.write_all(&1u32.to_le_bytes())?;
    writer.write_all(&merged.entry_count.to_le_bytes())?;
    writer.write_all(&(merged.member_ids.len() as u64).to_le_bytes())?;
    write_dense_sidecar_owner_index(&mut writer, &merged.path, merged.entry_count)?;
    for member_id in &merged.member_ids {
        writer.write_all(member_id)?;
    }
    let mut local_ids = HashMap::with_capacity(merged.member_ids.len());
    for (index, member_id) in merged.member_ids.iter().copied().enumerate() {
        let local_id = u32::try_from(index).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "dense sidecar member dictionary exceeds u32 capacity",
            )
        })?;
        local_ids.insert(member_id, local_id);
    }
    let mut reader = BufReader::new(File::open(&merged.path)?);
    while let Some(pair) = read_manifest_pair(&mut reader)? {
        let mut member = [0u8; GLOBAL_ID_BYTES];
        member.copy_from_slice(&pair[GLOBAL_ID_BYTES..]);
        let local_id = local_ids.get(&member).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "dense sidecar member is missing from local dictionary",
            )
        })?;
        writer.write_all(&local_id.to_le_bytes())?;
    }
    writer.flush()?;
    Ok((merged.entry_count, merged.member_count))
}

fn write_dense_sidecar_owner_index<W: Write>(
    writer: &mut W,
    pair_path: &Path,
    expected_entry_count: u64,
) -> io::Result<()> {
    let mut reader = BufReader::new(File::open(pair_path)?);
    let mut current_owner = None;
    let mut current_count = 0u32;
    let mut offset = 0u64;
    let mut written_entries = 0u64;
    while let Some(pair) = read_manifest_pair(&mut reader)? {
        let mut owner = [0u8; GLOBAL_ID_BYTES];
        owner.copy_from_slice(&pair[..GLOBAL_ID_BYTES]);
        if current_owner == Some(owner) {
            current_count = current_count.checked_add(1).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "dense sidecar owner exceeds u32 member capacity",
                )
            })?;
            continue;
        }
        if let Some(previous_owner) = current_owner.replace(owner) {
            write_dense_sidecar_owner(writer, &previous_owner, offset, current_count)?;
            offset = offset.saturating_add(u64::from(current_count));
            written_entries = written_entries.saturating_add(1);
        }
        current_count = 1;
    }
    if let Some(owner) = current_owner {
        write_dense_sidecar_owner(writer, &owner, offset, current_count)?;
        written_entries = written_entries.saturating_add(1);
    }
    if written_entries != expected_entry_count {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "dense sidecar owner count changed during external sort",
        ));
    }
    Ok(())
}

fn write_dense_sidecar_owner<W: Write>(
    writer: &mut W,
    owner: &[u8; GLOBAL_ID_BYTES],
    offset: u64,
    count: u32,
) -> io::Result<()> {
    writer.write_all(owner)?;
    writer.write_all(&offset.to_le_bytes())?;
    writer.write_all(&count.to_le_bytes())
}

fn count_pair_owners(pairs: &[[u8; GLOBAL_ID_BYTES * 2]]) -> u64 {
    let mut entry_count = 0u64;
    let mut previous_owner: Option<[u8; GLOBAL_ID_BYTES]> = None;
    for pair in pairs {
        let mut owner = [0u8; GLOBAL_ID_BYTES];
        owner.copy_from_slice(&pair[..GLOBAL_ID_BYTES]);
        if previous_owner != Some(owner) {
            entry_count = entry_count.saturating_add(1);
            previous_owner = Some(owner);
        }
    }
    entry_count
}

impl Drop for ManifestPairSpool {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

struct ManifestSidecarSpools {
    provider_forward: Option<ManifestPairSpool>,
    provider_inverted: Option<ManifestPairSpool>,
    provider_npi: Option<ManifestPairSpool>,
    price_forward: Option<ManifestPairSpool>,
}

impl ManifestSidecarSpools {
    fn for_paths(paths: &CopyPathConfig) -> io::Result<Self> {
        Ok(Self {
            provider_forward: manifest_pair_spool_if(
                paths.manifest_provider_forward_sidecar.is_some(),
                "provider_forward",
            )?,
            provider_inverted: manifest_pair_spool_if(
                paths.manifest_provider_inverted_sidecar.is_some(),
                "provider_inverted",
            )?,
            provider_npi: manifest_pair_spool_if(
                paths.manifest_provider_npi_sidecar.is_some(),
                "provider_npi",
            )?,
            price_forward: manifest_pair_spool_if(
                paths.manifest_price_forward_sidecar.is_some(),
                "price_forward",
            )?,
        })
    }

    #[cfg(test)]
    fn all() -> io::Result<Self> {
        Ok(Self {
            provider_forward: Some(ManifestPairSpool::new("provider_forward")?),
            provider_inverted: Some(ManifestPairSpool::new("provider_inverted")?),
            provider_npi: Some(ManifestPairSpool::new("provider_npi")?),
            price_forward: Some(ManifestPairSpool::new("price_forward")?),
        })
    }
}

fn manifest_pair_spool_if(enabled: bool, kind: &str) -> io::Result<Option<ManifestPairSpool>> {
    enabled.then(|| ManifestPairSpool::new(kind)).transpose()
}

#[derive(Default)]
struct ManifestSidecarCollector {
    provider_forward: BTreeMap<GlobalId128, Vec<GlobalId128>>,
    provider_inverted: BTreeMap<GlobalId128, Vec<GlobalId128>>,
    provider_npi: BTreeMap<GlobalId128, Vec<GlobalId128>>,
    price_forward: BTreeMap<GlobalId128, Vec<GlobalId128>>,
    spools: Option<ManifestSidecarSpools>,
}

impl ManifestSidecarCollector {
    fn for_import(paths: &CopyPathConfig) -> io::Result<Self> {
        if env_bool("HLTHPRT_PTG2_MANIFEST_SIDECAR_SPILL", true) {
            Ok(Self {
                spools: Some(ManifestSidecarSpools::for_paths(paths)?),
                ..Self::default()
            })
        } else {
            Ok(Self::default())
        }
    }

    fn record_provider_set(
        &mut self,
        provider_set_global_id: GlobalId128,
        provider_group_hashes: &[i64],
        provider_npis: &[i64],
    ) -> io::Result<()> {
        let mut provider_group_ids: Vec<GlobalId128> = provider_group_hashes
            .iter()
            .map(|hash| provider_group_global_id_from_hash(*hash))
            .collect();
        provider_group_ids.sort_unstable();
        provider_group_ids.dedup();
        if let Some(spools) = self.spools.as_mut() {
            for provider_group_id in provider_group_ids.iter().copied() {
                if let Some(spool) = spools.provider_forward.as_mut() {
                    spool.push(provider_set_global_id, provider_group_id)?;
                }
                if let Some(spool) = spools.provider_inverted.as_mut() {
                    spool.push(provider_group_id, provider_set_global_id)?;
                }
            }
        } else {
            self.provider_forward
                .entry(provider_set_global_id)
                .or_default()
                .extend(provider_group_ids.iter().copied());
            for provider_group_id in provider_group_ids {
                self.provider_inverted
                    .entry(provider_group_id)
                    .or_default()
                    .push(provider_set_global_id);
            }
        }
        let provider_npi_ids = provider_npis
            .iter()
            .copied()
            .filter(|npi| *npi > 0)
            .map(npi_member_id)
            .collect::<Vec<_>>();
        if let Some(spools) = self.spools.as_mut() {
            if let Some(spool) = spools.provider_npi.as_mut() {
                for provider_npi_id in provider_npi_ids {
                    spool.push(provider_set_global_id, provider_npi_id)?;
                }
            }
        } else {
            self.provider_npi
                .entry(provider_set_global_id)
                .or_default()
                .extend(provider_npi_ids);
        }
        Ok(())
    }

    fn record_price_set(&mut self, price_set: &PriceSetLite) -> io::Result<()> {
        let price_set_global_id = price_set_global_id(price_set);
        let price_atom_ids = price_set
            .atoms
            .iter()
            .map(price_atom_global_id)
            .collect::<Vec<_>>();
        if let Some(spools) = self.spools.as_mut() {
            if let Some(spool) = spools.price_forward.as_mut() {
                for price_atom_id in price_atom_ids {
                    spool.push(price_set_global_id, price_atom_id)?;
                }
            }
        } else {
            self.price_forward
                .entry(price_set_global_id)
                .or_default()
                .extend(price_atom_ids);
        }
        Ok(())
    }

    fn provider_forward_entries(&mut self) -> io::Result<Vec<SidecarEntry>> {
        if let Some(spool) = self
            .spools
            .as_mut()
            .and_then(|spools| spools.provider_forward.as_mut())
        {
            return spool.entries();
        }
        Ok(normalized_sidecar_entries(self.provider_forward.clone()))
    }

    fn provider_inverted_entries(&mut self) -> io::Result<Vec<SidecarEntry>> {
        if let Some(spool) = self
            .spools
            .as_mut()
            .and_then(|spools| spools.provider_inverted.as_mut())
        {
            return spool.entries();
        }
        Ok(normalized_sidecar_entries(self.provider_inverted.clone()))
    }

    fn provider_npi_entries(&mut self) -> io::Result<Vec<SidecarEntry>> {
        if let Some(spool) = self
            .spools
            .as_mut()
            .and_then(|spools| spools.provider_npi.as_mut())
        {
            return spool.entries();
        }
        Ok(normalized_sidecar_entries(self.provider_npi.clone()))
    }

    fn price_forward_entries(&mut self) -> io::Result<Vec<SidecarEntry>> {
        if let Some(spool) = self
            .spools
            .as_mut()
            .and_then(|spools| spools.price_forward.as_mut())
        {
            return spool.entries();
        }
        Ok(normalized_sidecar_entries(self.price_forward.clone()))
    }

    fn write_spooled_standard_sidecar(
        &mut self,
        sidecar_name: &str,
        path: &str,
        dense_members: bool,
    ) -> io::Result<Option<(u64, u64)>> {
        let Some(spools) = self.spools.as_mut() else {
            return Ok(None);
        };
        let spool = match sidecar_name {
            "provider_forward" => spools.provider_forward.as_mut(),
            "provider_inverted" => spools.provider_inverted.as_mut(),
            "provider_npi" => spools.provider_npi.as_mut(),
            "price_forward" => spools.price_forward.as_mut(),
            _ => return Ok(None),
        };
        let Some(spool) = spool else {
            return Ok(None);
        };
        let metrics = if dense_members {
            spool.write_dense_sidecar(path)?
        } else {
            spool.write_standard_sidecar(path)?
        };
        Ok(Some(metrics))
    }
}

struct ManifestSidecarWriteJob {
    order: usize,
    record_kind: &'static str,
    path: String,
    spool: ManifestPairSpool,
}

struct ManifestSidecarWriteResult {
    order: usize,
    record_kind: &'static str,
    path: String,
    entry_count: u64,
}

fn configured_spooled_manifest_sidecars(
    paths: &CopyPathConfig,
    collector: &mut ManifestSidecarCollector,
) -> io::Result<Option<Vec<ManifestSidecarWriteResult>>> {
    let Some(spools) = collector.spools.take() else {
        return Ok(None);
    };
    let ManifestSidecarSpools {
        provider_forward,
        provider_inverted,
        provider_npi,
        price_forward,
    } = spools;
    let mut jobs = Vec::new();
    push_manifest_sidecar_write_job(
        &mut jobs,
        0,
        "provider_forward",
        "manifest_provider_forward_sidecar_file",
        paths.manifest_provider_forward_sidecar.as_deref(),
        provider_forward,
    )?;
    push_manifest_sidecar_write_job(
        &mut jobs,
        1,
        "provider_inverted",
        "manifest_provider_inverted_sidecar_file",
        paths.manifest_provider_inverted_sidecar.as_deref(),
        provider_inverted,
    )?;
    push_manifest_sidecar_write_job(
        &mut jobs,
        2,
        "provider_npi",
        "manifest_provider_npi_sidecar_file",
        paths.manifest_provider_npi_sidecar.as_deref(),
        provider_npi,
    )?;
    push_manifest_sidecar_write_job(
        &mut jobs,
        3,
        "price_forward",
        "manifest_price_forward_sidecar_file",
        paths.manifest_price_forward_sidecar.as_deref(),
        price_forward,
    )?;
    let worker_count = env_usize("HLTHPRT_PTG2_MANIFEST_SIDECAR_WRITE_WORKERS", 4)
        .max(1)
        .min(jobs.len().max(1));
    write_manifest_sidecar_jobs(jobs, worker_count).map(Some)
}

fn push_manifest_sidecar_write_job(
    jobs: &mut Vec<ManifestSidecarWriteJob>,
    order: usize,
    sidecar_name: &'static str,
    record_kind: &'static str,
    path: Option<&str>,
    spool: Option<ManifestPairSpool>,
) -> io::Result<()> {
    match (path, spool) {
        (Some(path), Some(spool)) => jobs.push(ManifestSidecarWriteJob {
            order,
            record_kind,
            path: path.to_string(),
            spool,
        }),
        (Some(_), None) => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("configured manifest sidecar {sidecar_name} has no spool"),
            ));
        }
        (None, _) => {}
    }
    Ok(())
}

fn write_manifest_sidecar_jobs(
    jobs: Vec<ManifestSidecarWriteJob>,
    worker_count: usize,
) -> io::Result<Vec<ManifestSidecarWriteResult>> {
    let mut pending_jobs = jobs.into_iter();
    let mut results = Vec::new();
    loop {
        let batch: Vec<ManifestSidecarWriteJob> =
            pending_jobs.by_ref().take(worker_count.max(1)).collect();
        if batch.is_empty() {
            break;
        }
        if batch.len() == 1 {
            results.push(write_manifest_sidecar_job(
                batch.into_iter().next().unwrap(),
            )?);
            continue;
        }
        let handles: Vec<_> = batch
            .into_iter()
            .map(|job| thread::spawn(move || write_manifest_sidecar_job(job)))
            .collect();
        let mut first_error = None;
        for handle in handles {
            let result = match handle.join() {
                Ok(result) => result,
                Err(payload) => Err(io::Error::other(format!(
                    "manifest sidecar writer panicked: {}",
                    panic_payload_message(payload.as_ref())
                ))),
            };
            match result {
                Ok(result) => results.push(result),
                Err(error) if first_error.is_none() => first_error = Some(error),
                Err(_) => {}
            }
        }
        if let Some(error) = first_error {
            return Err(error);
        }
    }
    results.sort_unstable_by_key(|result| result.order);
    Ok(results)
}

fn write_manifest_sidecar_job(
    mut job: ManifestSidecarWriteJob,
) -> io::Result<ManifestSidecarWriteResult> {
    let (entry_count, _member_count) = job.spool.write_dense_sidecar(&job.path)?;
    Ok(ManifestSidecarWriteResult {
        order: job.order,
        record_kind: job.record_kind,
        path: job.path,
        entry_count,
    })
}

fn emit_manifest_sidecar_file<W: Write>(
    writer: &mut W,
    record_kind: &str,
    path: &str,
    entries: &[SidecarEntry],
    dense_members: bool,
) -> io::Result<()> {
    let file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(path)?;
    let mut sidecar_writer = BufWriter::new(file);
    if dense_members {
        write_dense_member_sidecar(&mut sidecar_writer, entries)?;
    } else {
        write_global_sidecar(&mut sidecar_writer, entries)?;
    }
    sidecar_writer.flush()?;
    let bytes = sidecar_writer.get_ref().metadata()?.len();
    drop(sidecar_writer);
    emit_copy_file_event(
        writer,
        &CopyFileEvent {
            record_kind: record_kind.to_string(),
            path: path.to_string(),
            bytes,
            row_count: entries.len() as u64,
            final_file: true,
        },
    )
}

fn emit_manifest_sidecar_path<W: Write>(
    writer: &mut W,
    record_kind: &str,
    path: &str,
    entry_count: u64,
) -> io::Result<()> {
    let bytes = std::fs::metadata(path)?.len();
    emit_copy_file_event(
        writer,
        &CopyFileEvent {
            record_kind: record_kind.to_string(),
            path: path.to_string(),
            bytes,
            row_count: entry_count,
            final_file: true,
        },
    )
}

fn emit_configured_manifest_sidecars<W: Write>(
    writer: &mut W,
    paths: &CopyPathConfig,
    collector: Option<&mut ManifestSidecarCollector>,
) -> io::Result<()> {
    let Some(collector) = collector else {
        return Ok(());
    };
    if let Some(results) = configured_spooled_manifest_sidecars(paths, collector)? {
        for result in results {
            emit_manifest_sidecar_path(
                writer,
                result.record_kind,
                &result.path,
                result.entry_count,
            )?;
        }
        return Ok(());
    }
    if let Some(path) = paths.manifest_provider_forward_sidecar.as_deref() {
        if let Some((entry_count, _member_count)) =
            collector.write_spooled_standard_sidecar("provider_forward", path, true)?
        {
            emit_manifest_sidecar_path(
                writer,
                "manifest_provider_forward_sidecar_file",
                path,
                entry_count,
            )?;
        } else {
            emit_manifest_sidecar_file(
                writer,
                "manifest_provider_forward_sidecar_file",
                path,
                &collector.provider_forward_entries()?,
                true,
            )?;
        }
    }
    if let Some(path) = paths.manifest_provider_inverted_sidecar.as_deref() {
        if let Some((entry_count, _member_count)) =
            collector.write_spooled_standard_sidecar("provider_inverted", path, true)?
        {
            emit_manifest_sidecar_path(
                writer,
                "manifest_provider_inverted_sidecar_file",
                path,
                entry_count,
            )?;
        } else {
            emit_manifest_sidecar_file(
                writer,
                "manifest_provider_inverted_sidecar_file",
                path,
                &collector.provider_inverted_entries()?,
                true,
            )?;
        }
    }
    if let Some(path) = paths.manifest_provider_npi_sidecar.as_deref() {
        if let Some((entry_count, _member_count)) =
            collector.write_spooled_standard_sidecar("provider_npi", path, true)?
        {
            emit_manifest_sidecar_path(
                writer,
                "manifest_provider_npi_sidecar_file",
                path,
                entry_count,
            )?;
        } else {
            emit_manifest_sidecar_file(
                writer,
                "manifest_provider_npi_sidecar_file",
                path,
                &collector.provider_npi_entries()?,
                true,
            )?;
        }
    }
    if let Some(path) = paths.manifest_price_forward_sidecar.as_deref() {
        if let Some((entry_count, _member_count)) =
            collector.write_spooled_standard_sidecar("price_forward", path, true)?
        {
            emit_manifest_sidecar_path(
                writer,
                "manifest_price_forward_sidecar_file",
                path,
                entry_count,
            )?;
        } else {
            emit_manifest_sidecar_file(
                writer,
                "manifest_price_forward_sidecar_file",
                path,
                &collector.price_forward_entries()?,
                true,
            )?;
        }
    }
    Ok(())
}

struct CompactCopySink {
    base_path: Option<String>,
    record_kind: String,
    manifest_serving_layout: ManifestServingCopyLayout,
    writer: Option<BufWriter<File>>,
    chunk_index: u64,
    row_count: u64,
    rotate_bytes: u64,
}

impl CompactCopySink {
    fn new_file(base_path: String, rotate_bytes: u64) -> io::Result<Self> {
        Ok(Self {
            writer: Some(Self::open_writer(&base_path)?),
            base_path: Some(base_path),
            record_kind: "compact_copy_file".to_string(),
            manifest_serving_layout: ManifestServingCopyLayout::Full,
            chunk_index: 0,
            row_count: 0,
            rotate_bytes,
        })
    }

    fn new_named_file(base_path: String, rotate_bytes: u64, record_kind: &str) -> io::Result<Self> {
        Ok(Self {
            writer: Some(Self::open_writer(&base_path)?),
            base_path: Some(base_path),
            record_kind: record_kind.to_string(),
            manifest_serving_layout: ManifestServingCopyLayout::Full,
            chunk_index: 0,
            row_count: 0,
            rotate_bytes,
        })
    }

    fn new_manifest_serving_file(
        base_path: String,
        rotate_bytes: u64,
        layout: ManifestServingCopyLayout,
    ) -> io::Result<Self> {
        Ok(Self {
            writer: Some(Self::open_writer(&base_path)?),
            base_path: Some(base_path),
            record_kind: layout.record_kind().to_string(),
            manifest_serving_layout: layout,
            chunk_index: 0,
            row_count: 0,
            rotate_bytes,
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

    fn write_row(&mut self, row: &CompactCopyRow<'_>) -> io::Result<()> {
        let writer = self.writer.as_mut().ok_or_else(|| {
            io::Error::new(io::ErrorKind::BrokenPipe, "compact copy writer is closed")
        })?;
        emit_compact_copy_row(writer, row)?;
        self.row_count += 1;
        Ok(())
    }

    fn write_manifest_serving_row(&mut self, row: &ManifestServingCopyRow<'_>) -> io::Result<()> {
        let writer = self.writer.as_mut().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::BrokenPipe,
                "manifest serving copy writer is closed",
            )
        })?;
        match self.manifest_serving_layout {
            ManifestServingCopyLayout::Full => emit_manifest_serving_copy_row(writer, row)?,
            ManifestServingCopyLayout::Lean => emit_manifest_lean_serving_copy_row(
                writer,
                &ManifestLeanServingCopyRow {
                    plan_id: row.plan_id,
                    reported_code_system: row.reported_code_system,
                    reported_code: row.reported_code,
                    provider_set_global_id_128: row.provider_set_global_id_128,
                    provider_count: row.provider_count,
                    price_set_global_id_128: row.price_set_global_id_128,
                },
            )?,
        }
        self.row_count += 1;
        Ok(())
    }

    fn is_manifest_serving_lean(&self) -> bool {
        matches!(
            self.manifest_serving_layout,
            ManifestServingCopyLayout::Lean
        )
    }

    fn write_manifest_lean_serving_row(
        &mut self,
        row: &ManifestLeanServingCopyRow<'_>,
    ) -> io::Result<()> {
        let writer = self.writer.as_mut().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::BrokenPipe,
                "manifest serving copy writer is closed",
            )
        })?;
        match self.manifest_serving_layout {
            ManifestServingCopyLayout::Full => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "full manifest serving layout requires full serving rows",
                ));
            }
            ManifestServingCopyLayout::Lean => emit_manifest_lean_serving_copy_row(writer, row)?,
        }
        self.row_count += 1;
        Ok(())
    }

    fn maybe_rotate_silent(&mut self) -> io::Result<Option<CopyFileEvent>> {
        if self.rotate_bytes == 0 {
            return Ok(None);
        }
        let writer = self.writer.as_mut().ok_or_else(|| {
            io::Error::new(io::ErrorKind::BrokenPipe, "compact copy writer is closed")
        })?;
        writer.flush()?;
        let bytes = writer.get_ref().metadata()?.len();
        if bytes < self.rotate_bytes {
            return Ok(None);
        }
        let base_path = self.base_path.as_ref().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "compact copy path is not configured",
            )
        })?;
        let ready_path = format!("{}.part{:06}.ready", base_path, self.chunk_index);
        self.chunk_index += 1;
        let old_writer = self.writer.take();
        drop(old_writer);
        std::fs::rename(base_path, &ready_path)?;
        self.writer = Some(Self::open_writer(base_path)?);
        let event = CopyFileEvent {
            record_kind: self.record_kind.clone(),
            path: ready_path,
            bytes,
            row_count: self.row_count,
            final_file: false,
        };
        self.row_count = 0;
        Ok(Some(event))
    }

    fn finish<W: Write>(mut self, stdout: &mut W) -> io::Result<()> {
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
                    "row_count": self.row_count,
                    "final": true,
                }),
            )?;
            stdout.flush()?;
        }
        Ok(())
    }

    fn finish_silent(mut self) -> io::Result<Option<CopyFileEvent>> {
        let mut writer = self.writer.take().ok_or_else(|| {
            io::Error::new(io::ErrorKind::BrokenPipe, "compact copy writer is closed")
        })?;
        writer.flush()?;
        let bytes = writer.get_ref().metadata()?.len();
        drop(writer);
        if bytes == 0 {
            return Ok(None);
        }
        Ok(Some(CopyFileEvent {
            record_kind: self.record_kind,
            path: self.base_path.unwrap_or_default(),
            bytes,
            row_count: self.row_count,
            final_file: true,
        }))
    }
}

struct DictionaryCopySinks {
    manifest_price_atom: Option<CompactCopySink>,
    manifest_price_set_atom: Option<CompactCopySink>,
    manifest_provider_group_member: Option<CompactCopySink>,
    manifest_code_count: Option<CompactCopySink>,
    manifest_provider_set_dictionary: Option<CompactCopySink>,
    procedure: Option<CompactCopySink>,
    price_code_set: Option<CompactCopySink>,
    price_atom: Option<CompactCopySink>,
    price_set_entry: Option<CompactCopySink>,
    provider_set: Option<CompactCopySink>,
    provider_set_component: Option<CompactCopySink>,
    provider_set_entry: Option<CompactCopySink>,
    provider_entry_component: Option<CompactCopySink>,
    provider_group_member: Option<CompactCopySink>,
}

impl DictionaryCopySinks {
    fn from_paths(paths: &CopyPathConfig, rotate_bytes: u64) -> io::Result<Self> {
        Ok(Self {
            manifest_price_atom: match &paths.manifest_price_atom {
                Some(path) => Some(CompactCopySink::new_named_file(
                    path.clone(),
                    rotate_bytes,
                    "manifest_price_atom_copy_file",
                )?),
                None => None,
            },
            manifest_price_set_atom: match &paths.manifest_price_set_atom {
                Some(path) => Some(CompactCopySink::new_named_file(
                    path.clone(),
                    rotate_bytes,
                    "manifest_price_set_atom_copy_file",
                )?),
                None => None,
            },
            manifest_provider_group_member: match &paths.manifest_provider_group_member {
                Some(path) => Some(CompactCopySink::new_named_file(
                    path.clone(),
                    rotate_bytes,
                    "manifest_provider_group_member_copy_file",
                )?),
                None => None,
            },
            manifest_code_count: match &paths.manifest_code_count {
                Some(path) => Some(CompactCopySink::new_named_file(
                    path.clone(),
                    rotate_bytes,
                    "manifest_code_count_copy_file",
                )?),
                None => None,
            },
            manifest_provider_set_dictionary: match &paths.manifest_provider_set_dictionary {
                Some(path) => Some(CompactCopySink::new_named_file(
                    path.clone(),
                    rotate_bytes,
                    "manifest_provider_set_dictionary_copy_file",
                )?),
                None => None,
            },
            procedure: match &paths.procedure {
                Some(path) => Some(CompactCopySink::new_named_file(
                    path.clone(),
                    rotate_bytes,
                    "procedure_copy_file",
                )?),
                None => None,
            },
            price_code_set: match &paths.price_code_set {
                Some(path) => Some(CompactCopySink::new_named_file(
                    path.clone(),
                    rotate_bytes,
                    "price_code_set_copy_file",
                )?),
                None => None,
            },
            price_atom: match &paths.price_atom {
                Some(path) => Some(CompactCopySink::new_named_file(
                    path.clone(),
                    rotate_bytes,
                    "price_atom_copy_file",
                )?),
                None => None,
            },
            price_set_entry: match (&paths.price_set_entry, paths.manifest_only) {
                (_, true) => None,
                (Some(path), false) => Some(CompactCopySink::new_named_file(
                    path.clone(),
                    rotate_bytes,
                    "price_set_entry_copy_file",
                )?),
                (None, false) => None,
            },
            provider_set: match &paths.provider_set {
                Some(path) => Some(CompactCopySink::new_named_file(
                    path.clone(),
                    rotate_bytes,
                    "provider_set_copy_file",
                )?),
                None => None,
            },
            provider_set_component: match (&paths.provider_set_component, paths.manifest_only) {
                (_, true) => None,
                (Some(path), false) => Some(CompactCopySink::new_named_file(
                    path.clone(),
                    rotate_bytes,
                    "provider_set_component_copy_file",
                )?),
                (None, false) => None,
            },
            provider_set_entry: match &paths.provider_set_entry {
                Some(path) => Some(CompactCopySink::new_named_file(
                    path.clone(),
                    rotate_bytes,
                    "provider_set_entry_copy_file",
                )?),
                None => None,
            },
            provider_entry_component: match &paths.provider_entry_component {
                Some(path) => Some(CompactCopySink::new_named_file(
                    path.clone(),
                    rotate_bytes,
                    "provider_entry_component_copy_file",
                )?),
                None => None,
            },
            provider_group_member: match &paths.provider_group_member {
                Some(path) => Some(CompactCopySink::new_named_file(
                    path.clone(),
                    rotate_bytes,
                    "provider_group_member_copy_file",
                )?),
                None => None,
            },
        })
    }

    fn maybe_rotate_silent(&mut self) -> io::Result<Vec<CopyFileEvent>> {
        let mut events = Vec::new();
        if let Some(sink) = self.manifest_price_atom.as_mut() {
            if let Some(event) = sink.maybe_rotate_silent()? {
                events.push(event);
            }
        }
        if let Some(sink) = self.manifest_price_set_atom.as_mut() {
            if let Some(event) = sink.maybe_rotate_silent()? {
                events.push(event);
            }
        }
        if let Some(sink) = self.manifest_provider_group_member.as_mut() {
            if let Some(event) = sink.maybe_rotate_silent()? {
                events.push(event);
            }
        }
        if let Some(sink) = self.manifest_code_count.as_mut() {
            if let Some(event) = sink.maybe_rotate_silent()? {
                events.push(event);
            }
        }
        if let Some(sink) = self.manifest_provider_set_dictionary.as_mut() {
            if let Some(event) = sink.maybe_rotate_silent()? {
                events.push(event);
            }
        }
        if let Some(sink) = self.procedure.as_mut() {
            if let Some(event) = sink.maybe_rotate_silent()? {
                events.push(event);
            }
        }
        if let Some(sink) = self.price_code_set.as_mut() {
            if let Some(event) = sink.maybe_rotate_silent()? {
                events.push(event);
            }
        }
        if let Some(sink) = self.price_atom.as_mut() {
            if let Some(event) = sink.maybe_rotate_silent()? {
                events.push(event);
            }
        }
        if let Some(sink) = self.price_set_entry.as_mut() {
            if let Some(event) = sink.maybe_rotate_silent()? {
                events.push(event);
            }
        }
        if let Some(sink) = self.provider_set.as_mut() {
            if let Some(event) = sink.maybe_rotate_silent()? {
                events.push(event);
            }
        }
        if let Some(sink) = self.provider_set_component.as_mut() {
            if let Some(event) = sink.maybe_rotate_silent()? {
                events.push(event);
            }
        }
        if let Some(sink) = self.provider_set_entry.as_mut() {
            if let Some(event) = sink.maybe_rotate_silent()? {
                events.push(event);
            }
        }
        if let Some(sink) = self.provider_entry_component.as_mut() {
            if let Some(event) = sink.maybe_rotate_silent()? {
                events.push(event);
            }
        }
        if let Some(sink) = self.provider_group_member.as_mut() {
            if let Some(event) = sink.maybe_rotate_silent()? {
                events.push(event);
            }
        }
        Ok(events)
    }

    fn finish<W: Write>(mut self, writer: &mut W) -> io::Result<()> {
        if let Some(sink) = self.manifest_price_atom.take() {
            sink.finish(writer)?;
        }
        if let Some(sink) = self.manifest_price_set_atom.take() {
            sink.finish(writer)?;
        }
        if let Some(sink) = self.manifest_provider_group_member.take() {
            sink.finish(writer)?;
        }
        if let Some(sink) = self.manifest_code_count.take() {
            sink.finish(writer)?;
        }
        if let Some(sink) = self.manifest_provider_set_dictionary.take() {
            sink.finish(writer)?;
        }
        if let Some(sink) = self.procedure.take() {
            sink.finish(writer)?;
        }
        if let Some(sink) = self.price_code_set.take() {
            sink.finish(writer)?;
        }
        if let Some(sink) = self.price_atom.take() {
            sink.finish(writer)?;
        }
        if let Some(sink) = self.price_set_entry.take() {
            sink.finish(writer)?;
        }
        if let Some(sink) = self.provider_set.take() {
            sink.finish(writer)?;
        }
        if let Some(sink) = self.provider_set_component.take() {
            sink.finish(writer)?;
        }
        if let Some(sink) = self.provider_set_entry.take() {
            sink.finish(writer)?;
        }
        if let Some(sink) = self.provider_entry_component.take() {
            sink.finish(writer)?;
        }
        if let Some(sink) = self.provider_group_member.take() {
            sink.finish(writer)?;
        }
        Ok(())
    }

    fn finish_silent(mut self) -> io::Result<Vec<CopyFileEvent>> {
        let mut events = Vec::new();
        if let Some(sink) = self.manifest_price_atom.take() {
            if let Some(event) = sink.finish_silent()? {
                events.push(event);
            }
        }
        if let Some(sink) = self.manifest_price_set_atom.take() {
            if let Some(event) = sink.finish_silent()? {
                events.push(event);
            }
        }
        if let Some(sink) = self.manifest_provider_group_member.take() {
            if let Some(event) = sink.finish_silent()? {
                events.push(event);
            }
        }
        if let Some(sink) = self.manifest_code_count.take() {
            if let Some(event) = sink.finish_silent()? {
                events.push(event);
            }
        }
        if let Some(sink) = self.manifest_provider_set_dictionary.take() {
            if let Some(event) = sink.finish_silent()? {
                events.push(event);
            }
        }
        if let Some(sink) = self.procedure.take() {
            if let Some(event) = sink.finish_silent()? {
                events.push(event);
            }
        }
        if let Some(sink) = self.price_code_set.take() {
            if let Some(event) = sink.finish_silent()? {
                events.push(event);
            }
        }
        if let Some(sink) = self.price_atom.take() {
            if let Some(event) = sink.finish_silent()? {
                events.push(event);
            }
        }
        if let Some(sink) = self.price_set_entry.take() {
            if let Some(event) = sink.finish_silent()? {
                events.push(event);
            }
        }
        if let Some(sink) = self.provider_set.take() {
            if let Some(event) = sink.finish_silent()? {
                events.push(event);
            }
        }
        if let Some(sink) = self.provider_set_component.take() {
            if let Some(event) = sink.finish_silent()? {
                events.push(event);
            }
        }
        if let Some(sink) = self.provider_set_entry.take() {
            if let Some(event) = sink.finish_silent()? {
                events.push(event);
            }
        }
        if let Some(sink) = self.provider_entry_component.take() {
            if let Some(event) = sink.finish_silent()? {
                events.push(event);
            }
        }
        if let Some(sink) = self.provider_group_member.take() {
            if let Some(event) = sink.finish_silent()? {
                events.push(event);
            }
        }
        Ok(events)
    }

    fn write_procedure(
        &mut self,
        procedure_hash: &str,
        procedure_value: &Value,
        _procedure_payload: &Value,
    ) -> io::Result<bool> {
        let Some(sink) = self.procedure.as_mut() else {
            return Ok(false);
        };
        let fields = [
            pg_text_copy_field(Some(procedure_hash)),
            pg_text_copy_field(
                normalize_string(procedure_value.get("billing_code_type")).as_deref(),
            ),
            pg_text_copy_field(
                normalize_string(procedure_value.get("billing_code_type_version")).as_deref(),
            ),
            pg_text_copy_field(normalize_string(procedure_value.get("billing_code")).as_deref()),
            pg_text_copy_field(normalize_string(procedure_value.get("name")).as_deref()),
            pg_text_copy_field(normalize_string(procedure_value.get("description")).as_deref()),
        ];
        let writer = sink.writer.as_mut().ok_or_else(|| {
            io::Error::new(io::ErrorKind::BrokenPipe, "procedure copy writer is closed")
        })?;
        write_copy_fields(writer, &fields)?;
        sink.row_count += 1;
        Ok(true)
    }

    fn write_price_atom(&mut self, atom: &PriceAtomLite) -> io::Result<bool> {
        let Some(sink) = self.price_atom.as_mut() else {
            return Ok(false);
        };
        let fields = [
            pg_text_copy_field(Some(&atom.price_atom_hash)),
            pg_text_copy_field(atom.negotiated_type.as_deref()),
            pg_text_copy_field(Some(&atom.negotiated_rate)),
            pg_text_copy_field(atom.expiration_date.as_deref()),
            pg_text_copy_field(Some(&atom.service_code_set_hash)),
            pg_text_copy_field(atom.billing_class.as_deref()),
            pg_text_copy_field(atom.setting.as_deref()),
            pg_text_copy_field(Some(&atom.billing_code_modifier_set_hash)),
            pg_text_copy_field(atom.additional_information.as_deref()),
        ];
        let writer = sink.writer.as_mut().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::BrokenPipe,
                "price atom copy writer is closed",
            )
        })?;
        write_copy_fields(writer, &fields)?;
        sink.row_count += 1;
        Ok(true)
    }

    fn write_manifest_price_atom(&mut self, atom: &PriceAtomLite) -> io::Result<bool> {
        let Some(sink) = self.manifest_price_atom.as_mut() else {
            return Ok(false);
        };
        let price_atom_global_id = price_atom_global_id(atom).to_hex();
        let fields = [
            pg_text_copy_field(Some(&price_atom_global_id)),
            pg_text_copy_field(atom.negotiated_type.as_deref()),
            pg_text_copy_field(Some(&atom.negotiated_rate)),
            pg_text_copy_field(atom.expiration_date.as_deref()),
            pg_text_array_field(&atom.service_code),
            pg_text_copy_field(atom.billing_class.as_deref()),
            pg_text_copy_field(atom.setting.as_deref()),
            pg_text_array_field(&atom.billing_code_modifier),
            pg_text_copy_field(atom.additional_information.as_deref()),
        ];
        let writer = sink.writer.as_mut().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::BrokenPipe,
                "manifest price atom copy writer is closed",
            )
        })?;
        write_copy_fields(writer, &fields)?;
        sink.row_count += 1;
        Ok(true)
    }

    fn write_manifest_price_set_atoms(&mut self, price_set: &PriceSetLite) -> io::Result<()> {
        let Some(sink) = self.manifest_price_set_atom.as_mut() else {
            return Ok(());
        };
        let price_set_global_id = price_set_global_id(price_set).to_hex();
        let writer = sink.writer.as_mut().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::BrokenPipe,
                "manifest price-set atom copy writer is closed",
            )
        })?;
        let mut rows_written = 0u64;
        for atom in &price_set.atoms {
            let price_atom_global_id = price_atom_global_id(atom).to_hex();
            let fields = [
                pg_text_copy_field(Some(&price_set_global_id)),
                pg_text_copy_field(Some(&price_atom_global_id)),
            ];
            write_copy_fields(writer, &fields)?;
            rows_written += 1;
        }
        sink.row_count += rows_written;
        Ok(())
    }

    fn write_price_code_set(&mut self, code_set_hash: &str, codes: &[String]) -> io::Result<()> {
        let Some(sink) = self.price_code_set.as_mut() else {
            return Ok(());
        };
        let fields = [
            pg_text_copy_field(Some(code_set_hash)),
            pg_text_array_field(codes),
        ];
        let writer = sink.writer.as_mut().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::BrokenPipe,
                "price code set copy writer is closed",
            )
        })?;
        write_copy_fields(writer, &fields)?;
        sink.row_count += 1;
        Ok(())
    }

    fn write_price_code_sets_for_atom(
        &mut self,
        atom: &PriceAtomLite,
        emitted_price_code_sets: &mut HashSet<String>,
    ) -> io::Result<()> {
        if emitted_price_code_sets.insert(atom.service_code_set_hash.clone()) {
            self.write_price_code_set(&atom.service_code_set_hash, &atom.service_code)?;
        }
        if emitted_price_code_sets.insert(atom.billing_code_modifier_set_hash.clone()) {
            self.write_price_code_set(
                &atom.billing_code_modifier_set_hash,
                &atom.billing_code_modifier,
            )?;
        }
        Ok(())
    }

    fn write_price_code_sets_for_atom_shared(
        &mut self,
        atom: &PriceAtomLite,
        dedupe: &SharedDedupe,
    ) -> io::Result<()> {
        if dedupe.insert_price_code_set(&atom.service_code_set_hash) {
            self.write_price_code_set(&atom.service_code_set_hash, &atom.service_code)?;
        }
        if dedupe.insert_price_code_set(&atom.billing_code_modifier_set_hash) {
            self.write_price_code_set(
                &atom.billing_code_modifier_set_hash,
                &atom.billing_code_modifier,
            )?;
        }
        Ok(())
    }

    fn write_price_atoms(
        &mut self,
        atoms: &[PriceAtomLite],
        emitted_price_code_sets: &mut HashSet<String>,
        emitted_price_atoms: &mut HashSet<String>,
    ) -> io::Result<()> {
        for atom in atoms {
            if emitted_price_atoms.insert(atom.price_atom_hash.clone()) {
                self.write_price_code_sets_for_atom(atom, emitted_price_code_sets)?;
                self.write_price_atom(atom)?;
                self.write_manifest_price_atom(atom)?;
            }
        }
        Ok(())
    }

    fn write_price_atoms_shared(
        &mut self,
        atoms: &[PriceAtomLite],
        dedupe: &SharedDedupe,
    ) -> io::Result<()> {
        for atom in atoms {
            if dedupe.insert_price_atom(&atom.price_atom_hash) {
                self.write_price_code_sets_for_atom_shared(atom, dedupe)?;
                self.write_price_atom(atom)?;
                self.write_manifest_price_atom(atom)?;
            }
        }
        Ok(())
    }

    fn write_price_set_entries(
        &mut self,
        price_set_hash: &str,
        price_atom_hashes: &[String],
        emitted_price_set_entries: &mut HashSet<(String, String)>,
    ) -> io::Result<()> {
        let Some(sink) = self.price_set_entry.as_mut() else {
            return Ok(());
        };
        let writer = sink.writer.as_mut().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::BrokenPipe,
                "price set entry copy writer is closed",
            )
        })?;
        let mut rows_written = 0u64;
        for price_atom_hash in price_atom_hashes {
            if !emitted_price_set_entries
                .insert((price_set_hash.to_string(), price_atom_hash.clone()))
            {
                continue;
            }
            let fields = [
                pg_text_copy_field(Some(price_set_hash)),
                pg_text_copy_field(Some(price_atom_hash)),
            ];
            write_copy_fields(writer, &fields)?;
            rows_written += 1;
        }
        sink.row_count += rows_written;
        Ok(())
    }

    fn write_price_set_entries_shared(
        &mut self,
        price_set_hash: &str,
        price_atom_hashes: &[String],
        dedupe: &SharedDedupe,
    ) -> io::Result<()> {
        let Some(sink) = self.price_set_entry.as_mut() else {
            return Ok(());
        };
        let writer = sink.writer.as_mut().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::BrokenPipe,
                "price set entry copy writer is closed",
            )
        })?;
        let mut rows_written = 0u64;
        for price_atom_hash in price_atom_hashes {
            if !dedupe.insert_price_set_entry(price_set_hash, price_atom_hash) {
                continue;
            }
            let fields = [
                pg_text_copy_field(Some(price_set_hash)),
                pg_text_copy_field(Some(price_atom_hash)),
            ];
            write_copy_fields(writer, &fields)?;
            rows_written += 1;
        }
        sink.row_count += rows_written;
        Ok(())
    }

    fn write_provider_set(
        &mut self,
        provider_set_hash: &str,
        provider_count: i64,
        _sorted_provider_hashes: &[i64],
    ) -> io::Result<bool> {
        let Some(sink) = self.provider_set.as_mut() else {
            return Ok(false);
        };
        let provider_count_text = provider_count.to_string();
        let fields = [
            pg_text_copy_field(Some(provider_set_hash)),
            pg_text_copy_field(Some(&provider_count_text)),
        ];
        let writer = sink.writer.as_mut().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::BrokenPipe,
                "provider set copy writer is closed",
            )
        })?;
        write_copy_fields(writer, &fields)?;
        sink.row_count += 1;
        Ok(true)
    }

    fn write_manifest_provider_set_dictionary(
        &mut self,
        provider_set_global_id_128: &str,
    ) -> io::Result<()> {
        let Some(sink) = self.manifest_provider_set_dictionary.as_mut() else {
            return Ok(());
        };
        let fields = [pg_text_copy_field(Some(provider_set_global_id_128))];
        let writer = sink.writer.as_mut().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::BrokenPipe,
                "manifest provider-set dictionary copy writer is closed",
            )
        })?;
        write_copy_fields(writer, &fields)?;
        sink.row_count += 1;
        Ok(())
    }

    fn write_manifest_code_count(
        &mut self,
        plan_id: &str,
        reported_code_system: Option<&str>,
        reported_code: Option<&str>,
        rate_count: usize,
    ) -> io::Result<()> {
        let Some(sink) = self.manifest_code_count.as_mut() else {
            return Ok(());
        };
        if rate_count == 0 {
            return Ok(());
        }
        let rate_count_text = rate_count.to_string();
        let fields = [
            pg_text_copy_field(Some(plan_id)),
            pg_text_copy_field(reported_code_system),
            pg_text_copy_field(reported_code),
            pg_text_copy_field(Some(&rate_count_text)),
        ];
        let writer = sink.writer.as_mut().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::BrokenPipe,
                "manifest code-count copy writer is closed",
            )
        })?;
        write_copy_fields(writer, &fields)?;
        sink.row_count += 1;
        Ok(())
    }

    fn write_provider_set_entries(
        &mut self,
        provider_set_hash: &str,
        provider_entry_hashes: &[i64],
        emitted_entries: &mut HashSet<(String, i64)>,
    ) -> io::Result<()> {
        let Some(sink) = self.provider_set_entry.as_mut() else {
            return Ok(());
        };
        let writer = sink.writer.as_mut().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::BrokenPipe,
                "provider set entry copy writer is closed",
            )
        })?;
        let mut rows_written = 0u64;
        for provider_entry_hash in provider_entry_hashes {
            if !emitted_entries.insert((provider_set_hash.to_string(), *provider_entry_hash)) {
                continue;
            }
            let entry_hash_text = provider_entry_hash.to_string();
            let fields = [
                pg_text_copy_field(Some(provider_set_hash)),
                pg_text_copy_field(Some(&entry_hash_text)),
            ];
            write_copy_fields(writer, &fields)?;
            rows_written += 1;
        }
        sink.row_count += rows_written;
        Ok(())
    }

    fn write_provider_set_components(
        &mut self,
        provider_set_hash: &str,
        provider_group_hashes: &[i64],
        emitted_components: &mut HashSet<(String, i64)>,
    ) -> io::Result<()> {
        let Some(sink) = self.provider_set_component.as_mut() else {
            return Ok(());
        };
        let writer = sink.writer.as_mut().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::BrokenPipe,
                "provider set component copy writer is closed",
            )
        })?;
        let mut rows_written = 0u64;
        for provider_group_hash in provider_group_hashes {
            if !emitted_components.insert((provider_set_hash.to_string(), *provider_group_hash)) {
                continue;
            }
            let group_hash_text = provider_group_hash.to_string();
            let fields = [
                pg_text_copy_field(Some(provider_set_hash)),
                pg_text_copy_field(Some(&group_hash_text)),
            ];
            write_copy_fields(writer, &fields)?;
            rows_written += 1;
        }
        sink.row_count += rows_written;
        Ok(())
    }

    fn write_provider_set_components_shared(
        &mut self,
        provider_set_hash: &str,
        provider_group_hashes: &[i64],
        dedupe: &SharedDedupe,
    ) -> io::Result<()> {
        let Some(sink) = self.provider_set_component.as_mut() else {
            return Ok(());
        };
        let writer = sink.writer.as_mut().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::BrokenPipe,
                "provider set component copy writer is closed",
            )
        })?;
        let mut rows_written = 0u64;
        for provider_group_hash in provider_group_hashes {
            if !dedupe.insert_provider_set_component(provider_set_hash, *provider_group_hash) {
                continue;
            }
            let group_hash_text = provider_group_hash.to_string();
            let fields = [
                pg_text_copy_field(Some(provider_set_hash)),
                pg_text_copy_field(Some(&group_hash_text)),
            ];
            write_copy_fields(writer, &fields)?;
            rows_written += 1;
        }
        sink.row_count += rows_written;
        Ok(())
    }

    fn write_provider_set_entries_shared(
        &mut self,
        provider_set_hash: &str,
        provider_entry_hashes: &[i64],
        dedupe: &SharedDedupe,
    ) -> io::Result<()> {
        let Some(sink) = self.provider_set_entry.as_mut() else {
            return Ok(());
        };
        let writer = sink.writer.as_mut().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::BrokenPipe,
                "provider set entry copy writer is closed",
            )
        })?;
        let mut rows_written = 0u64;
        for provider_entry_hash in provider_entry_hashes {
            if !dedupe.insert_provider_set_entry(provider_set_hash, *provider_entry_hash) {
                continue;
            }
            let entry_hash_text = provider_entry_hash.to_string();
            let fields = [
                pg_text_copy_field(Some(provider_set_hash)),
                pg_text_copy_field(Some(&entry_hash_text)),
            ];
            write_copy_fields(writer, &fields)?;
            rows_written += 1;
        }
        sink.row_count += rows_written;
        Ok(())
    }

    fn write_provider_entry_components(
        &mut self,
        provider_entry_hash: i64,
        provider_group_hashes: &[i64],
        emitted_components: &mut HashSet<(i64, i64)>,
    ) -> io::Result<()> {
        let Some(sink) = self.provider_entry_component.as_mut() else {
            return Ok(());
        };
        let writer = sink.writer.as_mut().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::BrokenPipe,
                "provider entry component copy writer is closed",
            )
        })?;
        let mut rows_written = 0u64;
        for provider_group_hash in provider_group_hashes {
            if !emitted_components.insert((provider_entry_hash, *provider_group_hash)) {
                continue;
            }
            let entry_hash_text = provider_entry_hash.to_string();
            let group_hash_text = provider_group_hash.to_string();
            let fields = [
                pg_text_copy_field(Some(&entry_hash_text)),
                pg_text_copy_field(Some(&group_hash_text)),
            ];
            write_copy_fields(writer, &fields)?;
            rows_written += 1;
        }
        sink.row_count += rows_written;
        Ok(())
    }

    fn write_provider_entry_components_shared(
        &mut self,
        provider_entry_hash: i64,
        provider_group_hashes: &[i64],
        dedupe: &SharedDedupe,
    ) -> io::Result<()> {
        let Some(sink) = self.provider_entry_component.as_mut() else {
            return Ok(());
        };
        let writer = sink.writer.as_mut().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::BrokenPipe,
                "provider entry component copy writer is closed",
            )
        })?;
        let mut rows_written = 0u64;
        for provider_group_hash in provider_group_hashes {
            if !dedupe.insert_provider_entry_component(provider_entry_hash, *provider_group_hash) {
                continue;
            }
            let entry_hash_text = provider_entry_hash.to_string();
            let group_hash_text = provider_group_hash.to_string();
            let fields = [
                pg_text_copy_field(Some(&entry_hash_text)),
                pg_text_copy_field(Some(&group_hash_text)),
            ];
            write_copy_fields(writer, &fields)?;
            rows_written += 1;
        }
        sink.row_count += rows_written;
        Ok(())
    }

    fn write_provider_group_members(
        &mut self,
        provider_ref: &Value,
        emitted_members: &mut HashSet<(i64, i64)>,
    ) -> io::Result<()> {
        let Some(groups) = provider_ref
            .get("provider_groups")
            .and_then(Value::as_array)
        else {
            return Ok(());
        };
        let mut rows_written = 0u64;
        let mut manifest_rows_written = 0u64;
        for group in groups {
            let tin = group.get("tin").unwrap_or(&Value::Null);
            let npi = npi_list(group.get("npi"));
            let group_hash = provider_group_hash(tin, &npi);
            let provider_group_global_id = provider_group_global_id_from_hash(group_hash).to_hex();
            for npi_value in &npi {
                if !emitted_members.insert((group_hash, *npi_value)) {
                    continue;
                }
                let npi_text = npi_value.to_string();
                if let Some(sink) = self.provider_group_member.as_mut() {
                    let writer = sink.writer.as_mut().ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::BrokenPipe,
                            "provider group member copy writer is closed",
                        )
                    })?;
                    let group_hash_text = group_hash.to_string();
                    let fields = [
                        pg_text_copy_field(Some(&group_hash_text)),
                        pg_text_copy_field(Some(&npi_text)),
                    ];
                    write_copy_fields(writer, &fields)?;
                    rows_written += 1;
                }
                if let Some(sink) = self.manifest_provider_group_member.as_mut() {
                    let writer = sink.writer.as_mut().ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::BrokenPipe,
                            "manifest provider group member copy writer is closed",
                        )
                    })?;
                    let fields = [
                        pg_text_copy_field(Some(&provider_group_global_id)),
                        pg_text_copy_field(Some(&npi_text)),
                    ];
                    write_copy_fields(writer, &fields)?;
                    manifest_rows_written += 1;
                }
            }
        }
        if let Some(sink) = self.provider_group_member.as_mut() {
            sink.row_count += rows_written;
        }
        if let Some(sink) = self.manifest_provider_group_member.as_mut() {
            sink.row_count += manifest_rows_written;
        }
        Ok(())
    }

    fn write_provider_group_members_shared(
        &mut self,
        provider_ref: &Value,
        dedupe: &SharedDedupe,
    ) -> io::Result<()> {
        let Some(groups) = provider_ref
            .get("provider_groups")
            .and_then(Value::as_array)
        else {
            return Ok(());
        };
        let mut rows_written = 0u64;
        let mut manifest_rows_written = 0u64;
        for group in groups {
            let tin = group.get("tin").unwrap_or(&Value::Null);
            let npi = npi_list(group.get("npi"));
            let group_hash = provider_group_hash(tin, &npi);
            if !dedupe.insert_provider_group(group_hash) {
                continue;
            }
            let provider_group_global_id = provider_group_global_id_from_hash(group_hash).to_hex();
            for npi_value in &npi {
                if !dedupe.insert_provider_group_member(group_hash, *npi_value) {
                    continue;
                }
                let npi_text = npi_value.to_string();
                if let Some(sink) = self.provider_group_member.as_mut() {
                    let writer = sink.writer.as_mut().ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::BrokenPipe,
                            "provider group member copy writer is closed",
                        )
                    })?;
                    let group_hash_text = group_hash.to_string();
                    let fields = [
                        pg_text_copy_field(Some(&group_hash_text)),
                        pg_text_copy_field(Some(&npi_text)),
                    ];
                    write_copy_fields(writer, &fields)?;
                    rows_written += 1;
                }
                if let Some(sink) = self.manifest_provider_group_member.as_mut() {
                    let writer = sink.writer.as_mut().ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::BrokenPipe,
                            "manifest provider group member copy writer is closed",
                        )
                    })?;
                    let fields = [
                        pg_text_copy_field(Some(&provider_group_global_id)),
                        pg_text_copy_field(Some(&npi_text)),
                    ];
                    write_copy_fields(writer, &fields)?;
                    manifest_rows_written += 1;
                }
            }
        }
        if let Some(sink) = self.provider_group_member.as_mut() {
            sink.row_count += rows_written;
        }
        if let Some(sink) = self.manifest_provider_group_member.as_mut() {
            sink.row_count += manifest_rows_written;
        }
        Ok(())
    }
}

type ParsedCompactRate = (PriceSetLite, i64, Vec<i64>, Vec<i64>, i64, Vec<String>);
type ProviderEntryComponents = BTreeMap<i64, Vec<i64>>;

struct GroupedPriceSet {
    price_set: PriceSetLite,
    provider_entry_hashes: HashSet<i64>,
    provider_group_hashes: HashSet<i64>,
    provider_npis: HashSet<i64>,
    provider_count: i64,
    network_names: HashSet<String>,
    provider_entry_components: ProviderEntryComponents,
}

impl
    From<(
        PriceSetLite,
        HashSet<i64>,
        HashSet<i64>,
        HashSet<i64>,
        i64,
        HashSet<String>,
        ProviderEntryComponents,
    )> for GroupedPriceSet
{
    fn from(
        value: (
            PriceSetLite,
            HashSet<i64>,
            HashSet<i64>,
            HashSet<i64>,
            i64,
            HashSet<String>,
            ProviderEntryComponents,
        ),
    ) -> Self {
        Self {
            price_set: value.0,
            provider_entry_hashes: value.1,
            provider_group_hashes: value.2,
            provider_npis: value.3,
            provider_count: value.4,
            network_names: value.5,
            provider_entry_components: value.6,
        }
    }
}

struct LocalCompactOutputs<'a, W: Write> {
    writer: &'a mut W,
    compact_copy_writer: &'a mut Option<CompactCopySink>,
    manifest_serving_copy_writer: &'a mut Option<CompactCopySink>,
    dictionary_copy_sinks: &'a mut DictionaryCopySinks,
    manifest_sidecars: Option<&'a mut ManifestSidecarCollector>,
    suppress_v2_serving_output: bool,
}

struct LocalCompactDedupe<'a> {
    price_code_sets: &'a mut HashSet<String>,
    price_atoms: &'a mut HashSet<String>,
    price_sets: &'a mut HashSet<String>,
    price_set_entries: &'a mut HashSet<(String, String)>,
    provider_sets: &'a mut HashSet<String>,
    provider_set_components: &'a mut HashSet<(String, i64)>,
    provider_set_entries: &'a mut HashSet<(String, i64)>,
    provider_entry_components: &'a mut HashSet<(i64, i64)>,
    procedures: &'a mut HashSet<String>,
    provider_group_members: &'a mut HashSet<(i64, i64)>,
}

struct CompactRateBatch<'a> {
    provider_map: &'a HashMap<String, ProviderEntry>,
    price_code_set_hash_cache: &'a mut PriceCodeSetHashCache,
    manifest_global_id_cache: &'a mut ManifestGlobalIdCache,
    rates: &'a [RateLite],
    procedure_value: &'a Value,
    context: &'a CompactContext,
}

fn process_compact_rate_lites<W: Write>(
    outputs: &mut LocalCompactOutputs<'_, W>,
    dedupe: &mut LocalCompactDedupe<'_>,
    batch: &mut CompactRateBatch<'_>,
) -> io::Result<()> {
    let writer = &mut outputs.writer;
    let compact_copy_writer = &mut outputs.compact_copy_writer;
    let manifest_serving_copy_writer = &mut outputs.manifest_serving_copy_writer;
    let dictionary_copy_sinks = &mut outputs.dictionary_copy_sinks;
    let provider_map = batch.provider_map;
    let price_code_set_hash_cache = &mut batch.price_code_set_hash_cache;
    let manifest_global_id_cache = &mut batch.manifest_global_id_cache;
    let rates = batch.rates;
    let procedure_value = batch.procedure_value;
    let context = batch.context;
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
    if dedupe.procedures.insert(procedure_hash.clone())
        && !dictionary_copy_sinks.write_procedure(
            &procedure_hash,
            procedure_value,
            &procedure_payload,
        )?
    {
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
                "canonical_payload": procedure_payload.clone(),
            }),
        )?;
    }

    let mut parsed_rates: Vec<ParsedCompactRate> = Vec::new();
    for rate in rates {
        let provider_entry = if !rate.provider_groups.is_empty() {
            let provider_ref = json!({"provider_groups": rate.provider_groups});
            dictionary_copy_sinks
                .write_provider_group_members(&provider_ref, dedupe.provider_group_members)?;
            match build_provider_entry(&provider_ref, outputs.manifest_sidecars.is_some()) {
                Some(entry) => entry,
                None => continue,
            }
        } else {
            match provider_set_from_ref_keys(provider_map, &rate.provider_refs) {
                Some(entry) => entry,
                None => continue,
            }
        };
        let Some(price_set) = price_lite_set(&rate.prices, price_code_set_hash_cache) else {
            continue;
        };
        parsed_rates.push((
            price_set,
            provider_entry.entry_hash,
            provider_entry.provider_group_hashes,
            provider_entry.npi,
            provider_entry.provider_count,
            rate_network_names(rate, context),
        ));
    }

    let group_negotiated_rate_chunks =
        env_bool("HLTHPRT_PTG2_RUST_GROUP_NEGOTIATED_RATE_CHUNKS", false);
    let grouped: Vec<GroupedPriceSet> = if group_negotiated_rate_chunks {
        let mut by_price_set: BTreeMap<String, GroupedPriceSet> = BTreeMap::new();
        for (
            price_set,
            provider_entry_hash,
            provider_group_hashes,
            provider_npis,
            provider_count,
            network_names,
        ) in parsed_rates
        {
            let group = by_price_set
                .entry(price_set.price_set_hash.clone())
                .or_insert_with(|| GroupedPriceSet {
                    price_set,
                    provider_entry_hashes: HashSet::new(),
                    provider_group_hashes: HashSet::new(),
                    provider_npis: HashSet::new(),
                    provider_count: 0,
                    network_names: HashSet::new(),
                    provider_entry_components: BTreeMap::new(),
                });
            for network_name in network_names {
                group.network_names.insert(network_name);
            }
            if group.provider_entry_hashes.insert(provider_entry_hash) {
                let mut sorted_components = provider_group_hashes;
                sorted_components.sort_unstable();
                sorted_components.dedup();
                for provider_group_hash in &sorted_components {
                    group.provider_group_hashes.insert(*provider_group_hash);
                }
                for npi in provider_npis {
                    if npi > 0 {
                        group.provider_npis.insert(npi);
                    }
                }
                group.provider_count += provider_count;
                group
                    .provider_entry_components
                    .insert(provider_entry_hash, sorted_components);
            } else {
                for provider_group_hash in provider_group_hashes {
                    group.provider_group_hashes.insert(provider_group_hash);
                }
                for npi in provider_npis {
                    if npi > 0 {
                        group.provider_npis.insert(npi);
                    }
                }
            }
        }
        by_price_set.into_values().collect()
    } else {
        parsed_rates
            .into_iter()
            .map(
                |(
                    price_set,
                    provider_entry_hash,
                    mut provider_group_hashes,
                    provider_npis,
                    provider_count,
                    network_names,
                )| {
                    provider_group_hashes.sort_unstable();
                    provider_group_hashes.dedup();
                    let mut provider_entry_hashes = HashSet::new();
                    provider_entry_hashes.insert(provider_entry_hash);
                    let provider_group_hashes_set = provider_group_hashes
                        .iter()
                        .copied()
                        .collect::<HashSet<_>>();
                    let provider_npis = provider_npis
                        .into_iter()
                        .filter(|npi| *npi > 0)
                        .collect::<HashSet<_>>();
                    let mut provider_entry_components = BTreeMap::new();
                    provider_entry_components.insert(provider_entry_hash, provider_group_hashes);
                    GroupedPriceSet {
                        price_set,
                        provider_entry_hashes,
                        provider_group_hashes: provider_group_hashes_set,
                        provider_npis,
                        provider_count,
                        network_names: network_names.into_iter().collect(),
                        provider_entry_components,
                    }
                },
            )
            .collect()
    };

    dictionary_copy_sinks.write_manifest_code_count(
        &context.plan_id,
        reported_code_system.as_deref(),
        reported_code.as_deref(),
        grouped.len(),
    )?;

    for group in grouped {
        let mut sorted_provider_entry_hashes: Vec<i64> =
            group.provider_entry_hashes.into_iter().collect();
        sorted_provider_entry_hashes.sort_unstable();
        let mut sorted_provider_hashes: Vec<i64> =
            group.provider_group_hashes.into_iter().collect();
        sorted_provider_hashes.sort_unstable();
        let mut sorted_provider_npis: Vec<i64> = group.provider_npis.into_iter().collect();
        sorted_provider_npis.sort_unstable();
        let mut network_names: Vec<String> = group.network_names.into_iter().collect();
        network_names.sort_unstable();
        let provider_set_hash = hash_i64_list("provider_set", &sorted_provider_entry_hashes);
        let rate_pack_hash = hash_text(
            "serving_rate_pack",
            &[
                context.snapshot_id.clone(),
                procedure_hash.clone(),
                provider_set_hash.clone(),
                group.price_set.price_set_hash.clone(),
            ],
        );
        let serving_rate_id = hash_text(
            "serving_rate_id",
            &[
                context.snapshot_id.clone(),
                context.plan_id.clone(),
                billing_code.clone(),
                rate_pack_hash.clone(),
            ],
        );
        let price_set_hash = group.price_set.price_set_hash.clone();
        if dedupe
            .price_sets
            .insert(group.price_set.price_set_hash.clone())
        {
            if let Some(sidecars) = outputs.manifest_sidecars.as_deref_mut() {
                sidecars.record_price_set(&group.price_set)?;
            }
            dictionary_copy_sinks.write_price_atoms(
                &group.price_set.atoms,
                dedupe.price_code_sets,
                dedupe.price_atoms,
            )?;
            dictionary_copy_sinks.write_manifest_price_set_atoms(&group.price_set)?;
            dictionary_copy_sinks.write_price_set_entries(
                &group.price_set.price_set_hash,
                &group.price_set.price_atom_hashes,
                dedupe.price_set_entries,
            )?;
        }
        if dedupe.provider_sets.insert(provider_set_hash.clone()) {
            let provider_set_global_id_128 = manifest_global_id_cache
                .provider_set_id_hex(&provider_set_hash, &sorted_provider_entry_hashes);
            dictionary_copy_sinks
                .write_manifest_provider_set_dictionary(&provider_set_global_id_128)?;
            if let Some(sidecars) = outputs.manifest_sidecars.as_deref_mut() {
                let provider_set_global_id = manifest_global_id_cache
                    .provider_set_id(&provider_set_hash, &sorted_provider_entry_hashes);
                sidecars.record_provider_set(
                    provider_set_global_id,
                    &sorted_provider_hashes,
                    &sorted_provider_npis,
                )?;
            }
            if !dictionary_copy_sinks.write_provider_set(
                &provider_set_hash,
                group.provider_count,
                &sorted_provider_hashes,
            )? {
                emit_json_record(
                    writer,
                    "provider_set",
                    &json!({
                        "provider_set_hash": provider_set_hash,
                        "hash_prefix": &provider_set_hash[..provider_set_hash.len().min(16)],
                        "provider_count": group.provider_count,
                        "npi": Value::Null,
                        "tin_type": "set",
                        "tin_value": Value::Null,
                        "canonical_payload": {
                            "provider_group_hashes": sorted_provider_hashes,
                            "provider_group_count": sorted_provider_hashes.len(),
                            "provider_count": group.provider_count,
                            "provider_count_mode": "summed_provider_groups",
                            "npi_inline": false,
                            "tin_type": "set",
                            "tin_value": Value::Null,
                        },
                    }),
                )?;
            }
            dictionary_copy_sinks.write_provider_set_entries(
                &provider_set_hash,
                &sorted_provider_entry_hashes,
                dedupe.provider_set_entries,
            )?;
            dictionary_copy_sinks.write_provider_set_components(
                &provider_set_hash,
                &sorted_provider_hashes,
                dedupe.provider_set_components,
            )?;
            for provider_entry_hash in &sorted_provider_entry_hashes {
                let components = group
                    .provider_entry_components
                    .get(provider_entry_hash)
                    .map(Vec::as_slice)
                    .unwrap_or(&[]);
                dictionary_copy_sinks.write_provider_entry_components(
                    *provider_entry_hash,
                    components,
                    dedupe.provider_entry_components,
                )?;
            }
        }
        if outputs.suppress_v2_serving_output {
            // Manifest-only imports keep source-scoped v2 publish rollback-safe, but
            // do not stream high-cardinality v2 serving rows that will be dropped.
        } else if let Some(copy_writer) = compact_copy_writer.as_mut() {
            copy_writer.write_row(&CompactCopyRow {
                serving_rate_id: &serving_rate_id,
                snapshot_id: &context.snapshot_id,
                plan_id: &context.plan_id,
                procedure_hash: &procedure_hash,
                procedure_code: None,
                reported_code_system: reported_code_system.as_deref(),
                reported_code: reported_code.as_deref(),
                provider_set_hash: &provider_set_hash,
                provider_count: group.provider_count,
                price_set_hash: &price_set_hash,
                source_trace_set_hash: &context.source_trace_set_hash,
                network_names: &network_names,
            })?;
        } else {
            emit_json_record(
                writer,
                "serving_rate_compact",
                &json!({
                    "serving_rate_id": serving_rate_id,
                    "snapshot_id": context.snapshot_id.clone(),
                    "plan_id": context.plan_id.clone(),
                    "plan_month_id": context.plan_month_id.clone(),
                    "procedure_hash": procedure_hash,
                    "procedure_code": Value::Null,
                    "reported_code_system": reported_code_system.clone(),
                    "reported_code": reported_code.clone(),
                    "billing_code": billing_code,
                    "billing_code_type": billing_code_type,
                    "rate_pack_hash": rate_pack_hash,
                    "provider_set_hash": provider_set_hash,
                    "provider_count": group.provider_count,
                    "price_set_hash": price_set_hash,
                    "source_trace_set_hash": context.source_trace_set_hash.clone(),
                    "network_names": network_names,
                    "confidence_code": context.confidence_code.clone(),
                }),
            )?;
        }
        if let Some(copy_writer) = manifest_serving_copy_writer.as_mut() {
            if copy_writer.is_manifest_serving_lean() {
                let identity = manifest_lean_serving_identity_hex(
                    &provider_set_hash,
                    &sorted_provider_entry_hashes,
                    &group.price_set,
                    manifest_global_id_cache,
                );
                copy_writer.write_manifest_lean_serving_row(&ManifestLeanServingCopyRow {
                    plan_id: &context.plan_id,
                    reported_code_system: reported_code_system.as_deref(),
                    reported_code: reported_code.as_deref(),
                    provider_set_global_id_128: &identity.provider_set_global_id_128,
                    provider_count: group.provider_count,
                    price_set_global_id_128: &identity.price_set_global_id_128,
                })?;
            } else {
                let identity = manifest_serving_identity_hex(
                    &context.plan_id,
                    &procedure_payload,
                    &provider_set_hash,
                    &sorted_provider_entry_hashes,
                    &group.price_set,
                    manifest_global_id_cache,
                );
                copy_writer.write_manifest_serving_row(&ManifestServingCopyRow {
                    serving_content_hash_128: &identity.serving_content_hash_128,
                    plan_id: &context.plan_id,
                    reported_code_system: reported_code_system.as_deref(),
                    reported_code: reported_code.as_deref(),
                    procedure_global_id_128: &identity.procedure_global_id_128,
                    provider_set_global_id_128: &identity.provider_set_global_id_128,
                    provider_count: group.provider_count,
                    price_set_global_id_128: &identity.price_set_global_id_128,
                    source_trace_set_hash: &context.source_trace_set_hash,
                    network_names: &network_names,
                })?;
            }
        }
    }
    Ok(())
}

#[derive(Clone)]
struct CompactContext {
    snapshot_id: String,
    plan_id: String,
    plan_month_id: String,
    source_trace_set_hash: String,
    confidence_code: String,
    source_network_names: Vec<String>,
}

fn rate_network_names(rate: &RateLite, context: &CompactContext) -> Vec<String> {
    if rate.network_names.is_empty() {
        context.source_network_names.clone()
    } else {
        rate.network_names.clone()
    }
}

enum WorkerJob {
    Rates {
        procedure: Map<String, Value>,
        rates: Vec<RateLite>,
    },
    RawRates {
        procedure: Map<String, Value>,
        raw_rates: RawRateChunk,
    },
}

#[derive(Clone, Copy)]
struct RawRateSpan {
    start: usize,
    end: usize,
}

struct RawRateChunk {
    bytes: Vec<u8>,
    spans: Vec<RawRateSpan>,
}

impl RawRateChunk {
    fn with_capacity(rate_capacity: usize, byte_capacity: usize) -> Self {
        Self {
            bytes: Vec::with_capacity(byte_capacity),
            spans: Vec::with_capacity(rate_capacity),
        }
    }

    fn is_empty(&self) -> bool {
        self.spans.is_empty()
    }

    fn len(&self) -> usize {
        self.spans.len()
    }

    fn byte_len(&self) -> usize {
        self.bytes.len()
    }

    fn push_current_value_span(&mut self, start: usize) {
        self.spans.push(RawRateSpan {
            start,
            end: self.bytes.len(),
        });
    }

    fn iter(&self) -> impl Iterator<Item = &[u8]> {
        self.spans
            .iter()
            .map(|span| &self.bytes[span.start..span.end])
    }
}

#[derive(Default)]
struct RawChunkStats {
    chunk_count: u64,
    total_bytes: u64,
    max_bytes: usize,
    max_rates: usize,
}

impl RawChunkStats {
    fn record(&mut self, rate_count: usize, byte_count: usize) {
        self.chunk_count = self.chunk_count.saturating_add(1);
        self.total_bytes = self.total_bytes.saturating_add(byte_count as u64);
        self.max_bytes = self.max_bytes.max(byte_count);
        self.max_rates = self.max_rates.max(rate_count);
    }
}

fn take_vec_replacing_with_capacity<T>(values: &mut Vec<T>, capacity: usize) -> Vec<T> {
    let mut replacement = Vec::with_capacity(capacity);
    std::mem::swap(values, &mut replacement);
    replacement
}

fn take_raw_rate_chunk_replacing_with_bounded_capacity(
    chunk: &mut RawRateChunk,
    rate_capacity: usize,
    max_retain_byte_capacity: usize,
) -> RawRateChunk {
    let retained_byte_capacity = chunk.bytes.capacity().min(max_retain_byte_capacity);
    let mut replacement = RawRateChunk::with_capacity(rate_capacity, retained_byte_capacity);
    std::mem::swap(chunk, &mut replacement);
    replacement
}

fn clear_vec_retain_bounded_capacity<T>(values: &mut Vec<T>, max_retain_capacity: usize) {
    if values.capacity() > max_retain_capacity {
        *values = Vec::with_capacity(max_retain_capacity);
    } else {
        values.clear();
    }
}

struct ProviderRefWorkerOutput {
    provider_map: HashMap<String, ProviderEntry>,
    events: Vec<CopyFileEvent>,
}

type ProviderRefWorkerSender = Sender<RawRateChunk>;
type ProviderRefWorkerHandle = thread::JoinHandle<io::Result<ProviderRefWorkerOutput>>;
type ProviderRefWorkerHandles = Vec<(usize, ProviderRefWorkerHandle)>;

impl WorkerJob {
    fn name(&self) -> &'static str {
        match self {
            WorkerJob::Rates { .. } => "rates",
            WorkerJob::RawRates { .. } => "raw rates",
        }
    }
}

fn process_provider_ref_raw_batch(
    raw_refs: &RawRateChunk,
    collect_provider_npis: bool,
    provider_map: &mut HashMap<String, ProviderEntry>,
    dictionary_copy_sinks: &mut DictionaryCopySinks,
    dedupe: &SharedDedupe,
) -> io::Result<u64> {
    let mut processed = 0u64;
    for raw_ref in raw_refs.iter() {
        let value = parse_json_value_from_raw_bytes(raw_ref)?;
        if let Some(key_value) = value.get("provider_group_id") {
            if let (Some(key), Some(entry)) = (
                provider_ref_key(key_value),
                build_provider_entry(&value, collect_provider_npis),
            ) {
                dictionary_copy_sinks.write_provider_group_members_shared(&value, dedupe)?;
                provider_map.insert(key, entry);
            }
        }
        processed = processed.saturating_add(1);
    }
    Ok(processed)
}

fn provider_ref_worker_loop(
    worker_id: usize,
    rx: Receiver<RawRateChunk>,
    dedupe: Arc<SharedDedupe>,
    copy_paths: CopyPathConfig,
    rotate_bytes: u64,
    collect_provider_npis: bool,
) -> io::Result<ProviderRefWorkerOutput> {
    let worker_paths = copy_paths.for_worker(worker_id);
    let mut dictionary_copy_sinks = DictionaryCopySinks::from_paths(&worker_paths, rotate_bytes)?;
    let mut provider_map = HashMap::new();
    let mut events = Vec::new();

    for raw_refs in rx.iter() {
        process_provider_ref_raw_batch(
            &raw_refs,
            collect_provider_npis,
            &mut provider_map,
            &mut dictionary_copy_sinks,
            &dedupe,
        )?;
        events.extend(dictionary_copy_sinks.maybe_rotate_silent()?);
    }
    events.extend(dictionary_copy_sinks.finish_silent()?);

    Ok(ProviderRefWorkerOutput {
        provider_map,
        events,
    })
}

struct PrefixReader<R: Read> {
    prefix: Cursor<Vec<u8>>,
    inner: R,
}

impl<R: Read> PrefixReader<R> {
    fn new(prefix: Vec<u8>, inner: R) -> Self {
        Self {
            prefix: Cursor::new(prefix),
            inner,
        }
    }
}

impl<R: Read> Read for PrefixReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let read = self.prefix.read(buf)?;
        if read > 0 {
            return Ok(read);
        }
        self.inner.read(buf)
    }
}

struct BufferedJsonByteReader<R: Read> {
    inner: R,
    buffer: Vec<u8>,
    pos: usize,
    filled: usize,
}

struct JsonDelimiterStack {
    inline: [u8; 64],
    len: usize,
    heap: Option<Vec<u8>>,
}

impl JsonDelimiterStack {
    fn new(first: u8) -> Self {
        let mut stack = Self {
            inline: [0; 64],
            len: 0,
            heap: None,
        };
        stack.push(match first {
            b'{' => b'}',
            b'[' => b']',
            _ => unreachable!(),
        });
        stack
    }

    fn push(&mut self, delimiter: u8) {
        if let Some(heap) = self.heap.as_mut() {
            heap.push(delimiter);
        } else if self.len < self.inline.len() {
            self.inline[self.len] = delimiter;
            self.len += 1;
        } else {
            let mut heap = Vec::with_capacity(self.inline.len() * 2);
            heap.extend_from_slice(&self.inline);
            heap.push(delimiter);
            self.heap = Some(heap);
        }
    }

    fn pop(&mut self) -> Option<u8> {
        if let Some(heap) = self.heap.as_mut() {
            return heap.pop();
        }
        if self.len == 0 {
            return None;
        }
        self.len -= 1;
        Some(self.inline[self.len])
    }

    fn is_empty(&self) -> bool {
        self.heap
            .as_ref()
            .map_or(self.len == 0, |heap| heap.is_empty())
    }
}

impl<R: Read> BufferedJsonByteReader<R> {
    fn new(inner: R) -> Self {
        Self {
            inner,
            buffer: vec![0u8; READ_BUF_SIZE],
            pos: 0,
            filled: 0,
        }
    }

    fn fill(&mut self) -> io::Result<bool> {
        if self.pos < self.filled {
            return Ok(true);
        }
        self.filled = self.inner.read(&mut self.buffer)?;
        self.pos = 0;
        Ok(self.filled > 0)
    }

    fn peek_byte(&mut self) -> io::Result<Option<u8>> {
        if !self.fill()? {
            return Ok(None);
        }
        Ok(Some(self.buffer[self.pos]))
    }

    fn next_byte(&mut self) -> io::Result<Option<u8>> {
        if !self.fill()? {
            return Ok(None);
        }
        let byte = self.buffer[self.pos];
        self.pos += 1;
        Ok(Some(byte))
    }

    fn skip_whitespace(&mut self) -> io::Result<()> {
        while matches!(self.peek_byte()?, Some(b' ' | b'\n' | b'\r' | b'\t')) {
            self.pos += 1;
        }
        Ok(())
    }

    fn consume_if(&mut self, expected: u8) -> io::Result<bool> {
        self.skip_whitespace()?;
        if self.peek_byte()? == Some(expected) {
            self.pos += 1;
            return Ok(true);
        }
        Ok(false)
    }

    fn expect_byte(&mut self, expected: u8) -> io::Result<()> {
        self.skip_whitespace()?;
        match self.next_byte()? {
            Some(actual) if actual == expected => Ok(()),
            Some(actual) => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "expected JSON byte {:?}, got {:?}",
                    expected as char, actual as char
                ),
            )),
            None => Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!("expected JSON byte {:?}", expected as char),
            )),
        }
    }

    fn read_string_bytes(&mut self) -> io::Result<Vec<u8>> {
        self.skip_whitespace()?;
        if self.next_byte()? != Some(b'"') {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "expected JSON string",
            ));
        }
        let mut bytes = vec![b'"'];
        let mut escape = false;
        loop {
            let Some(byte) = self.next_byte()? else {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "unterminated JSON string",
                ));
            };
            bytes.push(byte);
            if escape {
                escape = false;
            } else if byte == b'\\' {
                escape = true;
            } else if byte == b'"' {
                return Ok(bytes);
            }
        }
    }

    fn read_string(&mut self) -> io::Result<String> {
        let bytes = self.read_string_bytes()?;
        serde_json::from_slice(&bytes).map_err(to_io_error)
    }

    fn capture_value_bytes_into(&mut self, bytes: &mut Vec<u8>) -> io::Result<()> {
        bytes.clear();
        self.capture_value_bytes_append(bytes)
    }

    fn capture_value_bytes_append(&mut self, bytes: &mut Vec<u8>) -> io::Result<()> {
        self.skip_whitespace()?;
        let Some(first) = self.next_byte()? else {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "expected JSON value",
            ));
        };
        bytes.push(first);
        match first {
            b'"' => {
                self.capture_string_tail(bytes)?;
            }
            b'{' | b'[' => {
                self.capture_nested_tail(bytes, first)?;
            }
            _ => {
                while let Some(byte) = self.peek_byte()? {
                    if matches!(byte, b',' | b']' | b'}' | b' ' | b'\n' | b'\r' | b'\t') {
                        break;
                    }
                    bytes.push(byte);
                    self.pos += 1;
                }
            }
        }
        Ok(())
    }

    fn capture_string_tail(&mut self, bytes: &mut Vec<u8>) -> io::Result<()> {
        let mut escape = false;
        loop {
            let Some(byte) = self.next_byte()? else {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "unterminated JSON string",
                ));
            };
            bytes.push(byte);
            if escape {
                escape = false;
            } else if byte == b'\\' {
                escape = true;
            } else if byte == b'"' {
                return Ok(());
            }
        }
    }

    fn capture_nested_tail(&mut self, bytes: &mut Vec<u8>, first: u8) -> io::Result<()> {
        let mut stack = JsonDelimiterStack::new(first);
        let mut in_string = false;
        let mut escape = false;
        while let Some(byte) = self.next_byte()? {
            bytes.push(byte);
            if in_string {
                if escape {
                    escape = false;
                } else if byte == b'\\' {
                    escape = true;
                } else if byte == b'"' {
                    in_string = false;
                }
                continue;
            }
            match byte {
                b'"' => {
                    in_string = true;
                    escape = false;
                }
                b'{' => stack.push(b'}'),
                b'[' => stack.push(b']'),
                b'}' | b']' => {
                    if stack.pop() != Some(byte) {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "mismatched JSON delimiter",
                        ));
                    }
                    if stack.is_empty() {
                        return Ok(());
                    }
                }
                _ => {}
            }
        }
        Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "unterminated JSON value",
        ))
    }
}

fn next_array_value<R: Read>(
    reader: &mut BufferedJsonByteReader<R>,
    first: &mut bool,
) -> io::Result<bool> {
    reader.skip_whitespace()?;
    if reader.consume_if(b']')? {
        return Ok(false);
    }
    if *first {
        *first = false;
    } else {
        reader.expect_byte(b',')?;
        reader.skip_whitespace()?;
    }
    Ok(true)
}

fn join_provider_ref_workers<W: Write>(
    writer: &mut W,
    provider_handles: ProviderRefWorkerHandles,
) -> io::Result<(HashMap<String, ProviderEntry>, Vec<CopyFileEvent>)> {
    let mut provider_worker_error: Option<io::Error> = None;
    let mut provider_map = HashMap::new();
    let mut copy_file_events = Vec::new();
    for (worker_id, handle) in provider_handles {
        match handle.join() {
            Ok(Ok(output)) => {
                provider_map.extend(output.provider_map);
                copy_file_events.extend(output.events);
            }
            Ok(Err(err)) => {
                let message = err.to_string();
                emit_worker_failure(writer, worker_id, "provider_ref_error", &message)?;
                if provider_worker_error.is_none() {
                    provider_worker_error = Some(err);
                }
            }
            Err(payload) => {
                let message = panic_payload_message(payload.as_ref());
                emit_worker_failure(writer, worker_id, "provider_ref_panic", &message)?;
                if provider_worker_error.is_none() {
                    provider_worker_error = Some(io::Error::other(format!(
                        "provider-reference worker {worker_id} panicked: {message}"
                    )));
                }
            }
        }
    }
    if let Some(err) = provider_worker_error {
        return Err(err);
    }
    Ok((provider_map, copy_file_events))
}

fn spawn_provider_ref_workers(
    provider_ref_worker_count: usize,
    provider_ref_queue_size: usize,
    dedupe: Arc<SharedDedupe>,
    provider_ref_paths: CopyPathConfig,
    compact_copy_rotate_bytes: u64,
    collect_provider_npis: bool,
) -> (ProviderRefWorkerSender, ProviderRefWorkerHandles) {
    let (provider_tx, provider_rx) = bounded::<RawRateChunk>(provider_ref_queue_size);
    let mut provider_handles = Vec::with_capacity(provider_ref_worker_count);
    for worker_id in 0..provider_ref_worker_count {
        let worker_rx = provider_rx.clone();
        let worker_dedupe = Arc::clone(&dedupe);
        let worker_paths = provider_ref_paths.clone();
        provider_handles.push((
            worker_id,
            thread::spawn(move || {
                let result = panic::catch_unwind(AssertUnwindSafe(|| {
                    provider_ref_worker_loop(
                        worker_id,
                        worker_rx,
                        worker_dedupe,
                        worker_paths,
                        compact_copy_rotate_bytes,
                        collect_provider_npis,
                    )
                }));
                match result {
                    Ok(Ok(output)) => Ok(output),
                    Ok(Err(err)) => {
                        log_worker_failure(worker_id, "provider_ref_error", &err.to_string());
                        Err(err)
                    }
                    Err(payload) => {
                        let message = panic_payload_message(payload.as_ref());
                        log_worker_failure(worker_id, "provider_ref_panic", &message);
                        Err(io::Error::other(format!(
                            "provider-reference worker {worker_id} panicked: {message}"
                        )))
                    }
                }
            }),
        ));
    }
    drop(provider_rx);
    (provider_tx, provider_handles)
}

struct SharedCompactState<'a, W: Write> {
    writer: &'a mut W,
    compact_copy_writer: &'a mut Option<CompactCopySink>,
    manifest_serving_copy_writer: &'a mut Option<CompactCopySink>,
    dictionary_copy_sinks: &'a mut DictionaryCopySinks,
    manifest_sidecars: Option<Arc<Mutex<ManifestSidecarCollector>>>,
    suppress_v2_serving_output: bool,
    provider_map: &'a HashMap<String, ProviderEntry>,
    dedupe: &'a SharedDedupe,
    price_code_set_hash_cache: &'a mut PriceCodeSetHashCache,
    manifest_global_id_cache: &'a mut ManifestGlobalIdCache,
    context: &'a CompactContext,
}

fn process_compact_rate_lites_worker<W: Write>(
    state: &mut SharedCompactState<'_, W>,
    rates: &[RateLite],
    procedure_value: &Value,
) -> io::Result<()> {
    let writer = &mut state.writer;
    let compact_copy_writer = &mut state.compact_copy_writer;
    let manifest_serving_copy_writer = &mut state.manifest_serving_copy_writer;
    let dictionary_copy_sinks = &mut state.dictionary_copy_sinks;
    let manifest_sidecars = state.manifest_sidecars.as_ref();
    let provider_map = state.provider_map;
    let dedupe = state.dedupe;
    let price_code_set_hash_cache = &mut state.price_code_set_hash_cache;
    let manifest_global_id_cache = &mut state.manifest_global_id_cache;
    let context = state.context;
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
    if dedupe.insert_procedure(&procedure_hash)
        && !dictionary_copy_sinks.write_procedure(
            &procedure_hash,
            procedure_value,
            &procedure_payload,
        )?
    {
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
                "canonical_payload": procedure_payload.clone(),
            }),
        )?;
    }

    let group_negotiated_rate_chunks =
        env_bool("HLTHPRT_PTG2_RUST_GROUP_NEGOTIATED_RATE_CHUNKS", false);
    if !group_negotiated_rate_chunks {
        let mut code_count_rows = 0usize;
        for rate in rates {
            let Some(provider_entry) =
                provider_entry_view_from_ref_keys(provider_map, &rate.provider_refs)
            else {
                continue;
            };
            let Some(price_set) = price_lite_set(&rate.prices, price_code_set_hash_cache) else {
                continue;
            };
            let network_names = rate_network_names(rate, context);
            let sorted_provider_entry_hashes = [provider_entry.entry_hash()];
            let sorted_provider_hashes = provider_entry.provider_group_hashes();
            let sorted_provider_npis = provider_entry.npi();
            let provider_count = provider_entry.provider_count();
            let provider_set_hash = hash_i64_list("provider_set", &sorted_provider_entry_hashes);
            let rate_pack_hash = hash_text(
                "serving_rate_pack",
                &[
                    context.snapshot_id.clone(),
                    procedure_hash.clone(),
                    provider_set_hash.clone(),
                    price_set.price_set_hash.clone(),
                ],
            );
            let serving_rate_id = hash_text(
                "serving_rate_id",
                &[
                    context.snapshot_id.clone(),
                    context.plan_id.clone(),
                    billing_code.clone(),
                    rate_pack_hash.clone(),
                ],
            );
            let price_set_hash = price_set.price_set_hash.clone();
            if dedupe.insert_price_set(&price_set.price_set_hash) {
                if let Some(sidecars) = manifest_sidecars {
                    sidecars.lock().unwrap().record_price_set(&price_set)?;
                }
                dictionary_copy_sinks.write_price_atoms_shared(&price_set.atoms, dedupe)?;
                dictionary_copy_sinks.write_manifest_price_set_atoms(&price_set)?;
                dictionary_copy_sinks.write_price_set_entries_shared(
                    &price_set.price_set_hash,
                    &price_set.price_atom_hashes,
                    dedupe,
                )?;
            }
            if dedupe.insert_provider_set(&provider_set_hash) {
                let provider_set_global_id_128 = manifest_global_id_cache
                    .provider_set_id_hex(&provider_set_hash, &sorted_provider_entry_hashes);
                dictionary_copy_sinks
                    .write_manifest_provider_set_dictionary(&provider_set_global_id_128)?;
                if let Some(sidecars) = manifest_sidecars {
                    let provider_set_global_id = manifest_global_id_cache
                        .provider_set_id(&provider_set_hash, &sorted_provider_entry_hashes);
                    sidecars.lock().unwrap().record_provider_set(
                        provider_set_global_id,
                        sorted_provider_hashes,
                        sorted_provider_npis,
                    )?;
                }
                if !dictionary_copy_sinks.write_provider_set(
                    &provider_set_hash,
                    provider_count,
                    sorted_provider_hashes,
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
                dictionary_copy_sinks.write_provider_set_entries_shared(
                    &provider_set_hash,
                    &sorted_provider_entry_hashes,
                    dedupe,
                )?;
                dictionary_copy_sinks.write_provider_set_components_shared(
                    &provider_set_hash,
                    sorted_provider_hashes,
                    dedupe,
                )?;
                dictionary_copy_sinks.write_provider_entry_components_shared(
                    provider_entry.entry_hash(),
                    sorted_provider_hashes,
                    dedupe,
                )?;
            }
            if dedupe.insert_serving_rate(&serving_rate_id) {
                code_count_rows += 1;
                if state.suppress_v2_serving_output {
                    // See LocalCompactOutputs: manifest-only mode avoids emitting v2
                    // serving rows over either COPY files or JSON stdout.
                } else if let Some(copy_writer) = compact_copy_writer.as_mut() {
                    copy_writer.write_row(&CompactCopyRow {
                        serving_rate_id: &serving_rate_id,
                        snapshot_id: &context.snapshot_id,
                        plan_id: &context.plan_id,
                        procedure_hash: &procedure_hash,
                        procedure_code: None,
                        reported_code_system: reported_code_system.as_deref(),
                        reported_code: reported_code.as_deref(),
                        provider_set_hash: &provider_set_hash,
                        provider_count,
                        price_set_hash: &price_set_hash,
                        source_trace_set_hash: &context.source_trace_set_hash,
                        network_names: &network_names,
                    })?;
                } else {
                    emit_json_record(
                        writer,
                        "serving_rate_compact",
                        &json!({
                            "serving_rate_id": serving_rate_id,
                            "snapshot_id": context.snapshot_id.clone(),
                            "plan_id": context.plan_id.clone(),
                            "plan_month_id": context.plan_month_id.clone(),
                            "procedure_hash": procedure_hash,
                            "procedure_code": Value::Null,
                            "reported_code_system": reported_code_system.clone(),
                            "reported_code": reported_code.clone(),
                            "billing_code": billing_code,
                            "billing_code_type": billing_code_type,
                            "rate_pack_hash": rate_pack_hash,
                            "provider_set_hash": provider_set_hash,
                            "provider_count": provider_count,
                            "price_set_hash": price_set_hash,
                            "source_trace_set_hash": context.source_trace_set_hash.clone(),
                            "network_names": network_names.clone(),
                            "confidence_code": context.confidence_code.clone(),
                        }),
                    )?;
                }
                if let Some(copy_writer) = manifest_serving_copy_writer.as_mut() {
                    if copy_writer.is_manifest_serving_lean() {
                        let identity = manifest_lean_serving_identity_hex(
                            &provider_set_hash,
                            &sorted_provider_entry_hashes,
                            &price_set,
                            manifest_global_id_cache,
                        );
                        copy_writer.write_manifest_lean_serving_row(
                            &ManifestLeanServingCopyRow {
                                plan_id: &context.plan_id,
                                reported_code_system: reported_code_system.as_deref(),
                                reported_code: reported_code.as_deref(),
                                provider_set_global_id_128: &identity.provider_set_global_id_128,
                                provider_count,
                                price_set_global_id_128: &identity.price_set_global_id_128,
                            },
                        )?;
                    } else {
                        let identity = manifest_serving_identity_hex(
                            &context.plan_id,
                            &procedure_payload,
                            &provider_set_hash,
                            &sorted_provider_entry_hashes,
                            &price_set,
                            manifest_global_id_cache,
                        );
                        copy_writer.write_manifest_serving_row(&ManifestServingCopyRow {
                            serving_content_hash_128: &identity.serving_content_hash_128,
                            plan_id: &context.plan_id,
                            reported_code_system: reported_code_system.as_deref(),
                            reported_code: reported_code.as_deref(),
                            procedure_global_id_128: &identity.procedure_global_id_128,
                            provider_set_global_id_128: &identity.provider_set_global_id_128,
                            provider_count,
                            price_set_global_id_128: &identity.price_set_global_id_128,
                            source_trace_set_hash: &context.source_trace_set_hash,
                            network_names: &network_names,
                        })?;
                    }
                }
            }
        }
        dictionary_copy_sinks.write_manifest_code_count(
            &context.plan_id,
            reported_code_system.as_deref(),
            reported_code.as_deref(),
            code_count_rows,
        )?;
        return Ok(());
    }

    let parsed_rates: Vec<ParsedCompactRate> = rates
        .iter()
        .filter_map(|rate| {
            let provider_entry = provider_set_from_ref_keys(provider_map, &rate.provider_refs)?;
            let price_set = price_lite_set(&rate.prices, price_code_set_hash_cache)?;
            Some((
                price_set,
                provider_entry.entry_hash,
                provider_entry.provider_group_hashes,
                provider_entry.npi,
                provider_entry.provider_count,
                rate_network_names(rate, context),
            ))
        })
        .collect();

    let grouped: Vec<GroupedPriceSet> = if group_negotiated_rate_chunks {
        let mut by_price_set: BTreeMap<String, GroupedPriceSet> = BTreeMap::new();
        for (
            price_set,
            provider_entry_hash,
            provider_group_hashes,
            provider_npis,
            provider_count,
            network_names,
        ) in parsed_rates
        {
            let group = by_price_set
                .entry(price_set.price_set_hash.clone())
                .or_insert_with(|| GroupedPriceSet {
                    price_set,
                    provider_entry_hashes: HashSet::new(),
                    provider_group_hashes: HashSet::new(),
                    provider_npis: HashSet::new(),
                    provider_count: 0,
                    network_names: HashSet::new(),
                    provider_entry_components: BTreeMap::new(),
                });
            for network_name in network_names {
                group.network_names.insert(network_name);
            }
            if group.provider_entry_hashes.insert(provider_entry_hash) {
                let mut sorted_components = provider_group_hashes;
                sorted_components.sort_unstable();
                sorted_components.dedup();
                for provider_group_hash in &sorted_components {
                    group.provider_group_hashes.insert(*provider_group_hash);
                }
                for npi in provider_npis {
                    if npi > 0 {
                        group.provider_npis.insert(npi);
                    }
                }
                group.provider_count += provider_count;
                group
                    .provider_entry_components
                    .insert(provider_entry_hash, sorted_components);
            } else {
                for provider_group_hash in provider_group_hashes {
                    group.provider_group_hashes.insert(provider_group_hash);
                }
                for npi in provider_npis {
                    if npi > 0 {
                        group.provider_npis.insert(npi);
                    }
                }
            }
        }
        by_price_set.into_values().collect()
    } else {
        parsed_rates
            .into_iter()
            .map(
                |(
                    price_set,
                    provider_entry_hash,
                    mut provider_group_hashes,
                    provider_npis,
                    provider_count,
                    network_names,
                )| {
                    provider_group_hashes.sort_unstable();
                    provider_group_hashes.dedup();
                    let mut provider_entry_hashes = HashSet::new();
                    provider_entry_hashes.insert(provider_entry_hash);
                    let provider_group_hashes_set = provider_group_hashes
                        .iter()
                        .copied()
                        .collect::<HashSet<_>>();
                    let provider_npis = provider_npis
                        .into_iter()
                        .filter(|npi| *npi > 0)
                        .collect::<HashSet<_>>();
                    let mut provider_entry_components = BTreeMap::new();
                    provider_entry_components.insert(provider_entry_hash, provider_group_hashes);
                    GroupedPriceSet {
                        price_set,
                        provider_entry_hashes,
                        provider_group_hashes: provider_group_hashes_set,
                        provider_npis,
                        provider_count,
                        network_names: network_names.into_iter().collect(),
                        provider_entry_components,
                    }
                },
            )
            .collect()
    };

    let mut code_count_rows = 0usize;
    for group in grouped {
        let mut sorted_provider_entry_hashes: Vec<i64> =
            group.provider_entry_hashes.into_iter().collect();
        sorted_provider_entry_hashes.sort_unstable();
        let mut sorted_provider_hashes: Vec<i64> =
            group.provider_group_hashes.into_iter().collect();
        sorted_provider_hashes.sort_unstable();
        let mut sorted_provider_npis: Vec<i64> = group.provider_npis.into_iter().collect();
        sorted_provider_npis.sort_unstable();
        let mut network_names: Vec<String> = group.network_names.into_iter().collect();
        network_names.sort_unstable();
        let provider_set_hash = hash_i64_list("provider_set", &sorted_provider_entry_hashes);
        let rate_pack_hash = hash_text(
            "serving_rate_pack",
            &[
                context.snapshot_id.clone(),
                procedure_hash.clone(),
                provider_set_hash.clone(),
                group.price_set.price_set_hash.clone(),
            ],
        );
        let serving_rate_id = hash_text(
            "serving_rate_id",
            &[
                context.snapshot_id.clone(),
                context.plan_id.clone(),
                billing_code.clone(),
                rate_pack_hash.clone(),
            ],
        );
        let price_set_hash = group.price_set.price_set_hash.clone();
        if dedupe.insert_price_set(&group.price_set.price_set_hash) {
            if let Some(sidecars) = manifest_sidecars {
                sidecars
                    .lock()
                    .unwrap()
                    .record_price_set(&group.price_set)?;
            }
            dictionary_copy_sinks.write_price_atoms_shared(&group.price_set.atoms, dedupe)?;
            dictionary_copy_sinks.write_manifest_price_set_atoms(&group.price_set)?;
            dictionary_copy_sinks.write_price_set_entries_shared(
                &group.price_set.price_set_hash,
                &group.price_set.price_atom_hashes,
                dedupe,
            )?;
        }
        if dedupe.insert_provider_set(&provider_set_hash) {
            let provider_set_global_id_128 = manifest_global_id_cache
                .provider_set_id_hex(&provider_set_hash, &sorted_provider_entry_hashes);
            dictionary_copy_sinks
                .write_manifest_provider_set_dictionary(&provider_set_global_id_128)?;
            if let Some(sidecars) = manifest_sidecars {
                let provider_set_global_id = manifest_global_id_cache
                    .provider_set_id(&provider_set_hash, &sorted_provider_entry_hashes);
                sidecars.lock().unwrap().record_provider_set(
                    provider_set_global_id,
                    &sorted_provider_hashes,
                    &sorted_provider_npis,
                )?;
            }
            if !dictionary_copy_sinks.write_provider_set(
                &provider_set_hash,
                group.provider_count,
                &sorted_provider_hashes,
            )? {
                emit_json_record(
                    writer,
                    "provider_set",
                    &json!({
                        "provider_set_hash": provider_set_hash,
                        "hash_prefix": &provider_set_hash[..provider_set_hash.len().min(16)],
                        "provider_count": group.provider_count,
                        "npi": Value::Null,
                        "tin_type": "set",
                        "tin_value": Value::Null,
                        "canonical_payload": {
                            "provider_group_hashes": sorted_provider_hashes,
                            "provider_group_count": sorted_provider_hashes.len(),
                            "provider_count": group.provider_count,
                            "provider_count_mode": "summed_provider_groups",
                            "npi_inline": false,
                            "tin_type": "set",
                            "tin_value": Value::Null,
                        },
                    }),
                )?;
            }
            dictionary_copy_sinks.write_provider_set_entries_shared(
                &provider_set_hash,
                &sorted_provider_entry_hashes,
                dedupe,
            )?;
            dictionary_copy_sinks.write_provider_set_components_shared(
                &provider_set_hash,
                &sorted_provider_hashes,
                dedupe,
            )?;
            for provider_entry_hash in &sorted_provider_entry_hashes {
                let components = group
                    .provider_entry_components
                    .get(provider_entry_hash)
                    .map(Vec::as_slice)
                    .unwrap_or(&[]);
                dictionary_copy_sinks.write_provider_entry_components_shared(
                    *provider_entry_hash,
                    components,
                    dedupe,
                )?;
            }
        }
        if dedupe.insert_serving_rate(&serving_rate_id) {
            code_count_rows += 1;
            if state.suppress_v2_serving_output {
                // See LocalCompactOutputs: manifest-only mode avoids emitting v2
                // serving rows over either COPY files or JSON stdout.
            } else if let Some(copy_writer) = compact_copy_writer.as_mut() {
                copy_writer.write_row(&CompactCopyRow {
                    serving_rate_id: &serving_rate_id,
                    snapshot_id: &context.snapshot_id,
                    plan_id: &context.plan_id,
                    procedure_hash: &procedure_hash,
                    procedure_code: None,
                    reported_code_system: reported_code_system.as_deref(),
                    reported_code: reported_code.as_deref(),
                    provider_set_hash: &provider_set_hash,
                    provider_count: group.provider_count,
                    price_set_hash: &price_set_hash,
                    source_trace_set_hash: &context.source_trace_set_hash,
                    network_names: &network_names,
                })?;
            } else {
                emit_json_record(
                    writer,
                    "serving_rate_compact",
                    &json!({
                        "serving_rate_id": serving_rate_id,
                        "snapshot_id": context.snapshot_id.clone(),
                        "plan_id": context.plan_id.clone(),
                        "plan_month_id": context.plan_month_id.clone(),
                        "procedure_hash": procedure_hash,
                        "procedure_code": Value::Null,
                        "reported_code_system": reported_code_system.clone(),
                        "reported_code": reported_code.clone(),
                        "billing_code": billing_code,
                        "billing_code_type": billing_code_type,
                        "rate_pack_hash": rate_pack_hash,
                        "provider_set_hash": provider_set_hash,
                        "provider_count": group.provider_count,
                        "price_set_hash": price_set_hash,
                        "source_trace_set_hash": context.source_trace_set_hash.clone(),
                        "network_names": network_names,
                        "confidence_code": context.confidence_code.clone(),
                    }),
                )?;
            }
            if let Some(copy_writer) = manifest_serving_copy_writer.as_mut() {
                if copy_writer.is_manifest_serving_lean() {
                    let identity = manifest_lean_serving_identity_hex(
                        &provider_set_hash,
                        &sorted_provider_entry_hashes,
                        &group.price_set,
                        manifest_global_id_cache,
                    );
                    copy_writer.write_manifest_lean_serving_row(&ManifestLeanServingCopyRow {
                        plan_id: &context.plan_id,
                        reported_code_system: reported_code_system.as_deref(),
                        reported_code: reported_code.as_deref(),
                        provider_set_global_id_128: &identity.provider_set_global_id_128,
                        provider_count: group.provider_count,
                        price_set_global_id_128: &identity.price_set_global_id_128,
                    })?;
                } else {
                    let identity = manifest_serving_identity_hex(
                        &context.plan_id,
                        &procedure_payload,
                        &provider_set_hash,
                        &sorted_provider_entry_hashes,
                        &group.price_set,
                        manifest_global_id_cache,
                    );
                    copy_writer.write_manifest_serving_row(&ManifestServingCopyRow {
                        serving_content_hash_128: &identity.serving_content_hash_128,
                        plan_id: &context.plan_id,
                        reported_code_system: reported_code_system.as_deref(),
                        reported_code: reported_code.as_deref(),
                        procedure_global_id_128: &identity.procedure_global_id_128,
                        provider_set_global_id_128: &identity.provider_set_global_id_128,
                        provider_count: group.provider_count,
                        price_set_global_id_128: &identity.price_set_global_id_128,
                        source_trace_set_hash: &context.source_trace_set_hash,
                        network_names: &network_names,
                    })?;
                }
            }
        }
    }
    dictionary_copy_sinks.write_manifest_code_count(
        &context.plan_id,
        reported_code_system.as_deref(),
        reported_code.as_deref(),
        code_count_rows,
    )?;
    Ok(())
}

fn read_rate_lite_struson<R: Read>(
    json_reader: &mut JsonStreamReader<R>,
) -> io::Result<Option<RateLite>> {
    let mut provider_refs: Vec<String> = Vec::new();
    let mut provider_groups: Vec<Value> = Vec::new();
    let mut network_names: Vec<String> = Vec::new();
    let mut prices: Vec<PriceLite> = Vec::new();
    json_reader.begin_object().map_err(to_io_error)?;
    while json_reader.has_next().map_err(to_io_error)? {
        let field = match json_reader.next_name().map_err(to_io_error)? {
            "provider_groups" => 1,
            "provider_references" => 2,
            "negotiated_prices" => 3,
            "network_name" | "network_names" => 4,
            _ => 0,
        };
        match field {
            1 => {
                json_reader.begin_array().map_err(to_io_error)?;
                while json_reader.has_next().map_err(to_io_error)? {
                    let value: Value = json_reader.deserialize_next().map_err(to_io_error)?;
                    provider_groups.push(value);
                }
                json_reader.end_array().map_err(to_io_error)?;
            }
            2 => {
                json_reader.begin_array().map_err(to_io_error)?;
                while json_reader.has_next().map_err(to_io_error)? {
                    if let Some(key) = normalized_scalar_from_reader(json_reader)? {
                        provider_refs.push(key);
                    }
                }
                json_reader.end_array().map_err(to_io_error)?;
            }
            3 => {
                json_reader.begin_array().map_err(to_io_error)?;
                while json_reader.has_next().map_err(to_io_error)? {
                    if let Some(price) = read_price_lite_struson(json_reader)? {
                        prices.push(price);
                    }
                }
                json_reader.end_array().map_err(to_io_error)?;
            }
            4 => {
                network_names.extend(normalized_string_list_from_reader(json_reader)?);
            }
            _ => {
                json_reader.skip_value().map_err(to_io_error)?;
            }
        }
    }
    json_reader.end_object().map_err(to_io_error)?;
    if (provider_refs.is_empty() && provider_groups.is_empty()) || prices.is_empty() {
        return Ok(None);
    }
    Ok(Some(RateLite {
        provider_refs,
        provider_groups,
        network_names: canonical_text_list(network_names, false),
        prices,
    }))
}

fn transfer_next_value_to_bytes_append<R: Read>(
    json_reader: &mut JsonStreamReader<R>,
    bytes: &mut Vec<u8>,
) -> io::Result<()> {
    {
        let mut json_writer = JsonStreamWriter::new(bytes);
        json_reader
            .transfer_to(&mut json_writer)
            .map_err(to_io_error)?;
        json_writer.finish_document().map_err(to_io_error)?;
    }
    Ok(())
}

fn parse_json_value_from_raw_bytes(raw: &[u8]) -> io::Result<Value> {
    match serde_json::from_slice(raw) {
        Ok(value) => Ok(value),
        Err(_) if std::str::from_utf8(raw).is_err() => {
            let repaired = String::from_utf8_lossy(raw);
            serde_json::from_str(repaired.as_ref()).map_err(to_io_error)
        }
        Err(error) => Err(to_io_error(error)),
    }
}

fn read_rate_lite_bytes(raw: &[u8]) -> io::Result<Option<RateLite>> {
    if std::str::from_utf8(raw).is_err() {
        let repaired = String::from_utf8_lossy(raw);
        return read_rate_lite_from_reader(repaired.as_bytes());
    }
    read_rate_lite_from_reader(raw)
}

fn read_rate_lite_from_reader<R: Read>(reader: R) -> io::Result<Option<RateLite>> {
    let mut json_reader = JsonStreamReader::new(reader);
    let rate = read_rate_lite_struson(&mut json_reader)?;
    json_reader
        .consume_trailing_whitespace()
        .map_err(to_io_error)?;
    Ok(rate)
}

fn read_price_lite_struson<R: Read>(
    json_reader: &mut JsonStreamReader<R>,
) -> io::Result<Option<PriceLite>> {
    let mut negotiated_type: Option<String> = None;
    let mut negotiated_rate: Option<String> = None;
    let mut expiration_date: Option<String> = None;
    let mut service_code: Vec<String> = Vec::new();
    let mut billing_class: Option<String> = None;
    let mut setting: Option<String> = None;
    let mut billing_code_modifier: Vec<String> = Vec::new();
    let mut additional_information: Option<String> = None;

    json_reader.begin_object().map_err(to_io_error)?;
    while json_reader.has_next().map_err(to_io_error)? {
        let field = match json_reader.next_name().map_err(to_io_error)? {
            "negotiated_type" => 1,
            "negotiated_rate" => 2,
            "expiration_date" => 3,
            "service_code" => 4,
            "billing_class" => 5,
            "setting" => 6,
            "billing_code_modifier" => 7,
            "additional_information" => 8,
            _ => 0,
        };
        match field {
            1 => {
                negotiated_type = normalized_scalar_from_reader(json_reader)?;
            }
            2 => {
                negotiated_rate = normalized_money_from_reader(json_reader)?;
            }
            3 => {
                expiration_date = normalized_scalar_from_reader(json_reader)?;
            }
            4 => {
                service_code =
                    canonical_text_list(normalized_string_list_from_reader(json_reader)?, false);
            }
            5 => {
                billing_class = normalized_scalar_from_reader(json_reader)?;
            }
            6 => {
                setting = normalized_scalar_from_reader(json_reader)?;
            }
            7 => {
                billing_code_modifier =
                    canonical_text_list(normalized_string_list_from_reader(json_reader)?, true);
            }
            8 => {
                additional_information = normalized_scalar_from_reader(json_reader)?;
            }
            _ => {
                json_reader.skip_value().map_err(to_io_error)?;
            }
        }
    }
    json_reader.end_object().map_err(to_io_error)?;
    let Some(negotiated_rate) = negotiated_rate else {
        return Ok(None);
    };
    Ok(Some(PriceLite {
        negotiated_type,
        negotiated_rate,
        expiration_date,
        service_code,
        billing_class,
        setting,
        billing_code_modifier,
        additional_information,
    }))
}

struct InNetworkStreamState<'a, W: Write> {
    writer: &'a mut W,
    compact_copy_writer: &'a mut Option<CompactCopySink>,
    manifest_serving_copy_writer: &'a mut Option<CompactCopySink>,
    dictionary_copy_sinks: &'a mut DictionaryCopySinks,
    manifest_sidecars: Option<&'a mut ManifestSidecarCollector>,
    suppress_v2_serving_output: bool,
    provider_map: &'a HashMap<String, ProviderEntry>,
    dedupe: LocalCompactDedupe<'a>,
    price_code_set_hash_cache: &'a mut PriceCodeSetHashCache,
    manifest_global_id_cache: &'a mut ManifestGlobalIdCache,
    context: CompactContext,
    chunk_size: usize,
}

fn process_in_network_struson<R: Read, W: Write>(
    json_reader: &mut JsonStreamReader<R>,
    state: &mut InNetworkStreamState<'_, W>,
) -> io::Result<u64> {
    let mut procedure = Map::new();
    let mut rate_chunk: Vec<RateLite> = Vec::with_capacity(state.chunk_size);
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
                        if rate_chunk.len() >= state.chunk_size {
                            let procedure_value = Value::Object(procedure.clone());
                            let mut outputs = LocalCompactOutputs {
                                writer: state.writer,
                                compact_copy_writer: state.compact_copy_writer,
                                manifest_serving_copy_writer: state.manifest_serving_copy_writer,
                                dictionary_copy_sinks: state.dictionary_copy_sinks,
                                manifest_sidecars: state.manifest_sidecars.as_deref_mut(),
                                suppress_v2_serving_output: state.suppress_v2_serving_output,
                            };
                            let mut batch = CompactRateBatch {
                                provider_map: state.provider_map,
                                price_code_set_hash_cache: state.price_code_set_hash_cache,
                                manifest_global_id_cache: state.manifest_global_id_cache,
                                rates: &rate_chunk,
                                procedure_value: &procedure_value,
                                context: &state.context,
                            };
                            process_compact_rate_lites(
                                &mut outputs,
                                &mut state.dedupe,
                                &mut batch,
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
        let mut outputs = LocalCompactOutputs {
            writer: state.writer,
            compact_copy_writer: state.compact_copy_writer,
            manifest_serving_copy_writer: state.manifest_serving_copy_writer,
            dictionary_copy_sinks: state.dictionary_copy_sinks,
            manifest_sidecars: state.manifest_sidecars.as_deref_mut(),
            suppress_v2_serving_output: state.suppress_v2_serving_output,
        };
        let mut batch = CompactRateBatch {
            provider_map: state.provider_map,
            price_code_set_hash_cache: state.price_code_set_hash_cache,
            manifest_global_id_cache: state.manifest_global_id_cache,
            rates: &rate_chunk,
            procedure_value: &procedure_value,
            context: &state.context,
        };
        process_compact_rate_lites(&mut outputs, &mut state.dedupe, &mut batch)?;
    }
    Ok(rate_count)
}

#[derive(Clone, Copy)]
struct InNetworkEnqueueOptions {
    chunk_size: usize,
    raw_chunk_byte_limit: usize,
    parse_in_workers: bool,
}

struct InNetworkEnqueueIo<'a, W: Write> {
    tx: &'a Sender<WorkerJob>,
    event_rx: &'a Receiver<CopyFileEvent>,
    writer: &'a mut W,
    producer_blocked_micros: &'a mut u128,
    raw_chunk_stats: &'a mut RawChunkStats,
}

fn enqueue_in_network_raw_byte_scan<R: Read, W: Write>(
    reader: &mut BufferedJsonByteReader<R>,
    io_state: &mut InNetworkEnqueueIo<'_, W>,
    options: InNetworkEnqueueOptions,
) -> io::Result<u64> {
    let chunk_size = options.chunk_size;
    let raw_chunk_byte_limit = options.raw_chunk_byte_limit;
    let mut procedure = Map::new();
    let mut raw_rate_chunk =
        RawRateChunk::with_capacity(chunk_size, raw_chunk_byte_limit.min(READ_BUF_SIZE));
    let mut scratch_value = Vec::new();
    let mut rate_count = 0u64;
    reader.expect_byte(b'{')?;
    let mut first_field = true;
    loop {
        reader.skip_whitespace()?;
        if reader.consume_if(b'}')? {
            break;
        }
        if first_field {
            first_field = false;
        } else {
            reader.expect_byte(b',')?;
        }
        let name = reader.read_string()?;
        reader.expect_byte(b':')?;
        match name.as_str() {
            "billing_code_type"
            | "billing_code_type_version"
            | "billing_code"
            | "name"
            | "description" => {
                reader.capture_value_bytes_into(&mut scratch_value)?;
                let value = parse_json_value_from_raw_bytes(&scratch_value)?;
                procedure.insert(name, value);
                clear_vec_retain_bounded_capacity(&mut scratch_value, MAX_RETAINED_CAPTURE_BYTES);
            }
            "negotiated_rates" => {
                reader.expect_byte(b'[')?;
                let mut first_rate = true;
                while next_array_value(reader, &mut first_rate)? {
                    rate_count += 1;
                    let raw_start = raw_rate_chunk.byte_len();
                    reader.capture_value_bytes_append(&mut raw_rate_chunk.bytes)?;
                    raw_rate_chunk.push_current_value_span(raw_start);
                    if raw_rate_chunk.len() >= chunk_size
                        || raw_rate_chunk.byte_len() >= raw_chunk_byte_limit
                    {
                        let raw_rates = take_raw_rate_chunk_replacing_with_bounded_capacity(
                            &mut raw_rate_chunk,
                            chunk_size,
                            raw_chunk_byte_limit,
                        );
                        let raw_bytes = raw_rates.byte_len();
                        io_state.raw_chunk_stats.record(raw_rates.len(), raw_bytes);
                        send_worker_job(
                            io_state.tx,
                            io_state.event_rx,
                            io_state.writer,
                            io_state.producer_blocked_micros,
                            WorkerJob::RawRates {
                                procedure: procedure.clone(),
                                raw_rates,
                            },
                        )?;
                        drain_copy_file_events(io_state.event_rx, io_state.writer)?;
                    }
                }
            }
            _ => {
                reader.capture_value_bytes_into(&mut scratch_value)?;
                clear_vec_retain_bounded_capacity(&mut scratch_value, MAX_RETAINED_CAPTURE_BYTES);
            }
        }
    }
    if !raw_rate_chunk.is_empty() {
        io_state
            .raw_chunk_stats
            .record(raw_rate_chunk.len(), raw_rate_chunk.byte_len());
        send_worker_job(
            io_state.tx,
            io_state.event_rx,
            io_state.writer,
            io_state.producer_blocked_micros,
            WorkerJob::RawRates {
                procedure,
                raw_rates: raw_rate_chunk,
            },
        )?;
        drain_copy_file_events(io_state.event_rx, io_state.writer)?;
    }
    Ok(rate_count)
}

fn enqueue_in_network_struson<R: Read, W: Write>(
    json_reader: &mut JsonStreamReader<R>,
    io_state: &mut InNetworkEnqueueIo<'_, W>,
    options: InNetworkEnqueueOptions,
) -> io::Result<u64> {
    let chunk_size = options.chunk_size;
    let raw_chunk_byte_limit = options.raw_chunk_byte_limit;
    let parse_in_workers = options.parse_in_workers;
    let mut procedure = Map::new();
    let mut rate_chunk: Vec<RateLite> = Vec::with_capacity(chunk_size);
    let mut raw_rate_chunk =
        RawRateChunk::with_capacity(chunk_size, raw_chunk_byte_limit.min(READ_BUF_SIZE));
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
                    if parse_in_workers {
                        let raw_start = raw_rate_chunk.byte_len();
                        transfer_next_value_to_bytes_append(
                            json_reader,
                            &mut raw_rate_chunk.bytes,
                        )?;
                        raw_rate_chunk.push_current_value_span(raw_start);
                        if raw_rate_chunk.len() >= chunk_size
                            || raw_rate_chunk.byte_len() >= raw_chunk_byte_limit
                        {
                            let raw_rates = take_raw_rate_chunk_replacing_with_bounded_capacity(
                                &mut raw_rate_chunk,
                                chunk_size,
                                raw_chunk_byte_limit,
                            );
                            let raw_bytes = raw_rates.byte_len();
                            io_state.raw_chunk_stats.record(raw_rates.len(), raw_bytes);
                            send_worker_job(
                                io_state.tx,
                                io_state.event_rx,
                                io_state.writer,
                                io_state.producer_blocked_micros,
                                WorkerJob::RawRates {
                                    procedure: procedure.clone(),
                                    raw_rates,
                                },
                            )?;
                            drain_copy_file_events(io_state.event_rx, io_state.writer)?;
                        }
                    } else if let Some(rate) = read_rate_lite_struson(json_reader)? {
                        rate_chunk.push(rate);
                        if rate_chunk.len() >= chunk_size {
                            let rates =
                                take_vec_replacing_with_capacity(&mut rate_chunk, chunk_size);
                            send_worker_job(
                                io_state.tx,
                                io_state.event_rx,
                                io_state.writer,
                                io_state.producer_blocked_micros,
                                WorkerJob::Rates {
                                    procedure: procedure.clone(),
                                    rates,
                                },
                            )?;
                            drain_copy_file_events(io_state.event_rx, io_state.writer)?;
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
    if !raw_rate_chunk.is_empty() {
        io_state
            .raw_chunk_stats
            .record(raw_rate_chunk.len(), raw_rate_chunk.byte_len());
        send_worker_job(
            io_state.tx,
            io_state.event_rx,
            io_state.writer,
            io_state.producer_blocked_micros,
            WorkerJob::RawRates {
                procedure,
                raw_rates: raw_rate_chunk,
            },
        )?;
        drain_copy_file_events(io_state.event_rx, io_state.writer)?;
    } else if !rate_chunk.is_empty() {
        send_worker_job(
            io_state.tx,
            io_state.event_rx,
            io_state.writer,
            io_state.producer_blocked_micros,
            WorkerJob::Rates {
                procedure,
                rates: rate_chunk,
            },
        )?;
        drain_copy_file_events(io_state.event_rx, io_state.writer)?;
    }
    Ok(rate_count)
}

fn finish_worker_outputs(
    compact_copy_writer: Option<CompactCopySink>,
    manifest_serving_copy_writer: Option<CompactCopySink>,
    dictionary_copy_sinks: DictionaryCopySinks,
) -> io::Result<Vec<CopyFileEvent>> {
    let mut events = Vec::new();
    if let Some(copy_writer) = compact_copy_writer {
        if let Some(event) = copy_writer.finish_silent()? {
            events.push(event);
        }
    }
    if let Some(copy_writer) = manifest_serving_copy_writer {
        if let Some(event) = copy_writer.finish_silent()? {
            events.push(event);
        }
    }
    events.extend(dictionary_copy_sinks.finish_silent()?);
    Ok(events)
}

struct CompactWorkerConfig {
    event_tx: Sender<CopyFileEvent>,
    provider_map: Arc<HashMap<String, ProviderEntry>>,
    dedupe: Arc<SharedDedupe>,
    manifest_sidecars: Option<Arc<Mutex<ManifestSidecarCollector>>>,
    copy_paths: CopyPathConfig,
    rotate_bytes: u64,
    context: CompactContext,
}

fn compact_worker_loop(
    worker_id: usize,
    rx: Receiver<WorkerJob>,
    config: CompactWorkerConfig,
) -> io::Result<Vec<CopyFileEvent>> {
    let worker_paths = config.copy_paths.for_worker(worker_id);
    let mut compact_copy_writer = worker_paths
        .compact
        .as_ref()
        .map(|path| CompactCopySink::new_file(path.clone(), config.rotate_bytes))
        .transpose()?;
    let manifest_serving_layout = worker_paths.manifest_serving_copy_layout();
    let mut manifest_serving_copy_writer = worker_paths
        .manifest_serving_copy_path()
        .map(|path| {
            CompactCopySink::new_manifest_serving_file(
                path.clone(),
                config.rotate_bytes,
                manifest_serving_layout,
            )
        })
        .transpose()?;
    let mut dictionary_copy_sinks =
        DictionaryCopySinks::from_paths(&worker_paths, config.rotate_bytes)?;
    let mut sink = io::sink();
    let mut price_code_set_hash_cache: PriceCodeSetHashCache = HashMap::new();
    let mut manifest_global_id_cache = ManifestGlobalIdCache::default();

    for job in rx.iter() {
        match job {
            WorkerJob::Rates { procedure, rates } => {
                let procedure_value = Value::Object(procedure);
                let mut state = SharedCompactState {
                    writer: &mut sink,
                    compact_copy_writer: &mut compact_copy_writer,
                    manifest_serving_copy_writer: &mut manifest_serving_copy_writer,
                    dictionary_copy_sinks: &mut dictionary_copy_sinks,
                    manifest_sidecars: config.manifest_sidecars.clone(),
                    suppress_v2_serving_output: config.copy_paths.manifest_only,
                    provider_map: &config.provider_map,
                    dedupe: &config.dedupe,
                    price_code_set_hash_cache: &mut price_code_set_hash_cache,
                    manifest_global_id_cache: &mut manifest_global_id_cache,
                    context: &config.context,
                };
                process_compact_rate_lites_worker(&mut state, &rates, &procedure_value)?;
            }
            WorkerJob::RawRates {
                procedure,
                raw_rates,
            } => {
                let mut rates = Vec::with_capacity(raw_rates.len());
                for raw_rate in raw_rates.iter() {
                    if let Some(rate) = read_rate_lite_bytes(raw_rate)? {
                        rates.push(rate);
                    }
                }
                let procedure_value = Value::Object(procedure);
                let mut state = SharedCompactState {
                    writer: &mut sink,
                    compact_copy_writer: &mut compact_copy_writer,
                    manifest_serving_copy_writer: &mut manifest_serving_copy_writer,
                    dictionary_copy_sinks: &mut dictionary_copy_sinks,
                    manifest_sidecars: config.manifest_sidecars.clone(),
                    suppress_v2_serving_output: config.copy_paths.manifest_only,
                    provider_map: &config.provider_map,
                    dedupe: &config.dedupe,
                    price_code_set_hash_cache: &mut price_code_set_hash_cache,
                    manifest_global_id_cache: &mut manifest_global_id_cache,
                    context: &config.context,
                };
                process_compact_rate_lites_worker(&mut state, &rates, &procedure_value)?;
            }
        }
        if let Some(copy_writer) = compact_copy_writer.as_mut() {
            if let Some(event) = copy_writer.maybe_rotate_silent()? {
                config.event_tx.send(event).map_err(|err| {
                    io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        format!("compact copy event queue closed: {err}"),
                    )
                })?;
            }
        }
        if let Some(copy_writer) = manifest_serving_copy_writer.as_mut() {
            if let Some(event) = copy_writer.maybe_rotate_silent()? {
                config.event_tx.send(event).map_err(|err| {
                    io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        format!("manifest serving copy event queue closed: {err}"),
                    )
                })?;
            }
        }
        for event in dictionary_copy_sinks.maybe_rotate_silent()? {
            config.event_tx.send(event).map_err(|err| {
                io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    format!("dictionary copy event queue closed: {err}"),
                )
            })?;
        }
    }

    finish_worker_outputs(
        compact_copy_writer,
        manifest_serving_copy_writer,
        dictionary_copy_sinks,
    )
}

fn scan_compact_byte_top_level_parallel(
    path: &Path,
    context: CompactContext,
    worker_count: usize,
    queue_size: usize,
    copy_paths: CopyPathConfig,
    compact_copy_rotate_bytes: u64,
) -> io::Result<()> {
    let total_bytes = path.metadata().map(|metadata| metadata.len()).unwrap_or(0);
    let compressed_bytes_read = Arc::new(AtomicU64::new(0));
    let mut reader = open_reader(path, Arc::clone(&compressed_bytes_read))?;
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
    let started_at = Instant::now();
    let negotiated_rate_chunk_size = split_interval(
        "HLTHPRT_PTG2_RUST_SPLIT_NEGOTIATED_RATES",
        DEFAULT_SPLIT_NEGOTIATED_RATES,
    );
    let raw_chunk_byte_limit =
        env_usize("HLTHPRT_PTG2_RUST_RAW_CHUNK_BYTES", DEFAULT_RAW_CHUNK_BYTES);
    let provider_ref_worker_count =
        env_usize("HLTHPRT_PTG2_RUST_PROVIDER_REF_WORKERS", worker_count).max(1);
    let provider_ref_queue_size = env_usize(
        "HLTHPRT_PTG2_RUST_PROVIDER_REF_QUEUE",
        provider_ref_worker_count.max(queue_size).max(1),
    )
    .max(provider_ref_worker_count)
    .max(1);
    let provider_ref_chunk_items = env_usize(
        "HLTHPRT_PTG2_RUST_PROVIDER_REF_CHUNK_ITEMS",
        DEFAULT_PROVIDER_REF_CHUNK_ITEMS,
    )
    .max(1);
    let provider_ref_raw_chunk_byte_limit = env_usize(
        "HLTHPRT_PTG2_RUST_PROVIDER_REF_RAW_CHUNK_BYTES",
        raw_chunk_byte_limit,
    )
    .max(1);
    let parse_in_workers = env_bool(
        "HLTHPRT_PTG2_RUST_PARSE_IN_WORKERS",
        DEFAULT_PARSE_IN_WORKERS,
    );
    let bounded_queue_size = queue_size.max(worker_count).max(1);
    let event_queue_size = env_usize(
        "HLTHPRT_PTG2_RUST_EVENT_QUEUE",
        (bounded_queue_size * 2).max(worker_count),
    );
    let dedupe = Arc::new(SharedDedupe::new(worker_count));
    let manifest_sidecars = if copy_paths.has_manifest_sidecar_paths() {
        Some(Arc::new(Mutex::new(ManifestSidecarCollector::for_import(
            &copy_paths,
        )?)))
    } else {
        None
    };
    let (tx, rx) = bounded::<WorkerJob>(bounded_queue_size);
    let (event_tx, event_rx) = bounded::<CopyFileEvent>(event_queue_size);
    let stdout = io::stdout();
    let mut writer = BufWriter::new(stdout.lock());
    let mut producer_blocked_micros = 0u128;
    let mut provider_ref_producer_blocked_micros = 0u128;
    let mut raw_chunk_stats = RawChunkStats::default();
    let mut provider_ref_raw_chunk_stats = RawChunkStats::default();
    let mut provider_refs_seconds = 0.0f64;
    let mut in_network_enqueue_seconds = 0.0f64;
    let mut worker_join_seconds = 0.0f64;

    emit_json_record(
        &mut writer,
        "scanner_config",
        &json!({
            "worker_count": worker_count,
            "work_queue": bounded_queue_size,
            "event_queue": event_queue_size,
            "split_negotiated_rates": negotiated_rate_chunk_size,
            "raw_chunk_bytes": raw_chunk_byte_limit,
            "read_buffer_bytes": READ_BUF_SIZE,
            "parse_in_workers": parse_in_workers,
            "raw_rate_byte_capture": parse_in_workers,
            "provider_refs_in_workers": true,
            "provider_ref_workers": provider_ref_worker_count,
            "provider_ref_queue": provider_ref_queue_size,
            "provider_ref_chunk_items": provider_ref_chunk_items,
            "provider_ref_raw_chunk_bytes": provider_ref_raw_chunk_byte_limit,
            "top_level_byte_scan": true,
            "provider_npi_sidecar": copy_paths.manifest_provider_npi_sidecar.is_some(),
            "panic_strategy": "unwind",
        }),
    )?;
    writer.flush()?;

    let provider_ref_paths = copy_paths.for_provider_refs();
    let (provider_tx, mut provider_handles) = spawn_provider_ref_workers(
        provider_ref_worker_count,
        provider_ref_queue_size,
        Arc::clone(&dedupe),
        provider_ref_paths,
        compact_copy_rotate_bytes,
        copy_paths.manifest_provider_npi_sidecar.is_some(),
    );
    let mut raw_refs = RawRateChunk::with_capacity(
        provider_ref_chunk_items,
        provider_ref_raw_chunk_byte_limit.min(READ_BUF_SIZE),
    );
    let provider_refs_started_at = Instant::now();

    let mut buffer = vec![0u8; READ_BUF_SIZE];
    let mut depth: usize = 0;
    let mut in_provider_refs = false;
    let mut active_array_depth: usize = 0;
    let mut capture_depth: usize = 0;
    let mut capture_start: usize = 0;
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
        for (idx, &byte) in buffer[..read].iter().enumerate() {
            if capture_depth > 0 {
                raw_refs.bytes.push(byte);
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
                if depth == 1 && !in_provider_refs && capture_depth == 0 {
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
                if byte == b'[' && depth == 1 && pending == b"provider_references" {
                    depth += 1;
                    in_provider_refs = true;
                    active_array_depth = depth;
                    continue;
                }
                if byte == b'[' && depth == 1 && pending == b"in_network" {
                    if !raw_refs.is_empty() {
                        provider_ref_raw_chunk_stats.record(raw_refs.len(), raw_refs.byte_len());
                        let batch = take_raw_rate_chunk_replacing_with_bounded_capacity(
                            &mut raw_refs,
                            provider_ref_chunk_items,
                            provider_ref_raw_chunk_byte_limit,
                        );
                        send_provider_ref_batch(
                            &provider_tx,
                            &event_rx,
                            &mut writer,
                            &mut provider_ref_producer_blocked_micros,
                            batch,
                        )?;
                    }
                    drop(provider_tx);
                    provider_refs_seconds += provider_refs_started_at.elapsed().as_secs_f64();
                    let (provider_map, provider_events) = join_provider_ref_workers(
                        &mut writer,
                        std::mem::take(&mut provider_handles),
                    )?;
                    drain_copy_file_events(&event_rx, &mut writer)?;
                    for event in provider_events {
                        emit_copy_file_event(&mut writer, &event)?;
                    }
                    writer.flush()?;

                    let provider_map = Arc::new(provider_map);
                    let mut handles = Vec::with_capacity(worker_count);
                    for worker_id in 0..worker_count {
                        let worker_rx = rx.clone();
                        let worker_event_tx = event_tx.clone();
                        let worker_provider_map = Arc::clone(&provider_map);
                        let worker_dedupe = Arc::clone(&dedupe);
                        let worker_manifest_sidecars = manifest_sidecars.as_ref().map(Arc::clone);
                        let worker_copy_paths = copy_paths.clone();
                        let worker_context = context.clone();
                        handles.push((
                            worker_id,
                            thread::spawn(move || {
                                let result = panic::catch_unwind(AssertUnwindSafe(|| {
                                    compact_worker_loop(
                                        worker_id,
                                        worker_rx,
                                        CompactWorkerConfig {
                                            event_tx: worker_event_tx,
                                            provider_map: worker_provider_map,
                                            dedupe: worker_dedupe,
                                            manifest_sidecars: worker_manifest_sidecars,
                                            copy_paths: worker_copy_paths,
                                            rotate_bytes: compact_copy_rotate_bytes,
                                            context: worker_context,
                                        },
                                    )
                                }));
                                match result {
                                    Ok(Ok(events)) => Ok(events),
                                    Ok(Err(err)) => {
                                        log_worker_failure(worker_id, "error", &err.to_string());
                                        Err(err)
                                    }
                                    Err(payload) => {
                                        let message = panic_payload_message(payload.as_ref());
                                        log_worker_failure(worker_id, "panic", &message);
                                        Err(io::Error::other(format!(
                                            "compact worker {worker_id} panicked: {message}"
                                        )))
                                    }
                                }
                            }),
                        ));
                    }
                    drop(rx);

                    let mut prefix = Vec::with_capacity(read.saturating_sub(idx) + 1);
                    prefix.push(b'[');
                    prefix.extend_from_slice(&buffer[idx + 1..read]);
                    let in_network_started_at = Instant::now();
                    if parse_in_workers {
                        let prefixed_reader = PrefixReader::new(prefix, reader);
                        let mut byte_reader = BufferedJsonByteReader::new(prefixed_reader);
                        byte_reader.expect_byte(b'[')?;
                        let mut first_item = true;
                        while next_array_value(&mut byte_reader, &mut first_item)? {
                            let mut enqueue_io = InNetworkEnqueueIo {
                                tx: &tx,
                                event_rx: &event_rx,
                                writer: &mut writer,
                                producer_blocked_micros: &mut producer_blocked_micros,
                                raw_chunk_stats: &mut raw_chunk_stats,
                            };
                            let rate_count = enqueue_in_network_raw_byte_scan(
                                &mut byte_reader,
                                &mut enqueue_io,
                                InNetworkEnqueueOptions {
                                    chunk_size: negotiated_rate_chunk_size,
                                    raw_chunk_byte_limit,
                                    parse_in_workers,
                                },
                            )?;
                            drain_copy_file_events(&event_rx, &mut writer)?;
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
                                    started_at,
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
                                    started_at,
                                    false,
                                );
                                while objects >= next_progress_objects {
                                    next_progress_objects += progress_objects_interval;
                                }
                            }
                        }
                    } else {
                        let prefixed_reader = PrefixReader::new(prefix, reader);
                        let mut json_reader = JsonStreamReader::new(prefixed_reader);
                        json_reader.begin_array().map_err(to_io_error)?;
                        while json_reader.has_next().map_err(to_io_error)? {
                            let mut enqueue_io = InNetworkEnqueueIo {
                                tx: &tx,
                                event_rx: &event_rx,
                                writer: &mut writer,
                                producer_blocked_micros: &mut producer_blocked_micros,
                                raw_chunk_stats: &mut raw_chunk_stats,
                            };
                            let rate_count = enqueue_in_network_struson(
                                &mut json_reader,
                                &mut enqueue_io,
                                InNetworkEnqueueOptions {
                                    chunk_size: negotiated_rate_chunk_size,
                                    raw_chunk_byte_limit,
                                    parse_in_workers,
                                },
                            )?;
                            drain_copy_file_events(&event_rx, &mut writer)?;
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
                                    started_at,
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
                                    started_at,
                                    false,
                                );
                                while objects >= next_progress_objects {
                                    next_progress_objects += progress_objects_interval;
                                }
                            }
                        }
                        json_reader.end_array().map_err(to_io_error)?;
                    }
                    in_network_enqueue_seconds += in_network_started_at.elapsed().as_secs_f64();

                    drop(tx);
                    drop(event_tx);
                    let worker_join_started_at = Instant::now();
                    let mut worker_error: Option<io::Error> = None;
                    let mut copy_file_events = Vec::new();
                    for (worker_id, handle) in handles {
                        match handle.join() {
                            Ok(Ok(mut events)) => copy_file_events.append(&mut events),
                            Ok(Err(err)) => {
                                let message = err.to_string();
                                emit_worker_failure(&mut writer, worker_id, "error", &message)?;
                                if worker_error.is_none() {
                                    worker_error = Some(err);
                                }
                            }
                            Err(payload) => {
                                let message = panic_payload_message(payload.as_ref());
                                emit_worker_failure(&mut writer, worker_id, "panic", &message)?;
                                if worker_error.is_none() {
                                    worker_error = Some(io::Error::other(format!(
                                        "compact worker {worker_id} panicked: {message}"
                                    )));
                                }
                            }
                        }
                    }
                    worker_join_seconds += worker_join_started_at.elapsed().as_secs_f64();
                    if let Some(err) = worker_error {
                        return Err(err);
                    }
                    emit_dedupe_summary(&dedupe, &object_counts);
                    drain_copy_file_events(&event_rx, &mut writer)?;
                    for event in copy_file_events {
                        emit_copy_file_event(&mut writer, &event)?;
                    }
                    if let Some(sidecars) = manifest_sidecars.as_ref() {
                        let mut sidecars = sidecars.lock().unwrap();
                        emit_configured_manifest_sidecars(
                            &mut writer,
                            &copy_paths,
                            Some(&mut sidecars),
                        )?;
                    }
                    emit_json_record(
                        &mut writer,
                        "dedupe_summary",
                        &dedupe_summary_payload(&dedupe, &object_counts),
                    )?;
                    emit_json_record(
                        &mut writer,
                        "scanner_summary",
                        &json!({
                            "worker_count": worker_count,
                            "work_queue": bounded_queue_size,
                            "event_queue": event_queue_size,
                            "split_negotiated_rates": negotiated_rate_chunk_size,
                            "raw_chunk_bytes": raw_chunk_byte_limit,
                            "raw_chunk_count": raw_chunk_stats.chunk_count,
                            "raw_chunk_total_bytes": raw_chunk_stats.total_bytes,
                            "raw_chunk_max_bytes": raw_chunk_stats.max_bytes,
                            "raw_chunk_max_rates": raw_chunk_stats.max_rates,
                            "provider_ref_raw_chunk_count": provider_ref_raw_chunk_stats.chunk_count,
                            "provider_ref_raw_chunk_total_bytes": provider_ref_raw_chunk_stats.total_bytes,
                            "provider_ref_raw_chunk_max_bytes": provider_ref_raw_chunk_stats.max_bytes,
                            "provider_ref_raw_chunk_max_items": provider_ref_raw_chunk_stats.max_rates,
                            "parse_in_workers": parse_in_workers,
                            "producer_blocked_micros": producer_blocked_micros,
                            "provider_ref_producer_blocked_micros": provider_ref_producer_blocked_micros,
                            "provider_refs_seconds": provider_refs_seconds,
                            "in_network_enqueue_seconds": in_network_enqueue_seconds,
                            "worker_join_seconds": worker_join_seconds,
                            "top_level_byte_scan": true,
                            "raw_rate_byte_capture": parse_in_workers,
                            "elapsed_seconds": started_at.elapsed().as_secs_f64(),
                        }),
                    )?;
                    writer.flush()?;
                    emit_progress(
                        path,
                        total_bytes,
                        &compressed_bytes_read,
                        &object_counts,
                        started_at,
                        true,
                    );
                    return Ok(());
                }
            }

            if in_provider_refs && capture_depth == 0 && byte == b'{' && depth == active_array_depth
            {
                capture_start = raw_refs.byte_len();
                raw_refs.bytes.push(b'{');
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
                            raw_refs.push_current_value_span(capture_start);
                            *object_counts
                                .entry("provider_references".to_string())
                                .or_insert(0) += 1;
                            if raw_refs.len() >= provider_ref_chunk_items
                                || raw_refs.byte_len() >= provider_ref_raw_chunk_byte_limit
                            {
                                let batch = take_raw_rate_chunk_replacing_with_bounded_capacity(
                                    &mut raw_refs,
                                    provider_ref_chunk_items,
                                    provider_ref_raw_chunk_byte_limit,
                                );
                                provider_ref_raw_chunk_stats.record(batch.len(), batch.byte_len());
                                send_provider_ref_batch(
                                    &provider_tx,
                                    &event_rx,
                                    &mut writer,
                                    &mut provider_ref_producer_blocked_micros,
                                    batch,
                                )?;
                                drain_copy_file_events(&event_rx, &mut writer)?;
                            }
                            let bytes = compressed_bytes_read.load(Ordering::Relaxed);
                            let objects: u64 = object_counts.values().sum();
                            if progress_bytes_interval > 0 && bytes >= next_progress_bytes {
                                emit_progress(
                                    path,
                                    total_bytes,
                                    &compressed_bytes_read,
                                    &object_counts,
                                    started_at,
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
                                    started_at,
                                    false,
                                );
                                while objects >= next_progress_objects {
                                    next_progress_objects += progress_objects_interval;
                                }
                            }
                            depth = depth.saturating_sub(1);
                            continue;
                        }
                    }
                    if in_provider_refs && byte == b']' && depth == active_array_depth {
                        in_provider_refs = false;
                        active_array_depth = 0;
                    }
                    depth = depth.saturating_sub(1);
                }
                _ => {}
            }
        }
    }

    drop(provider_tx);
    let _ = join_provider_ref_workers(&mut writer, provider_handles)?;
    writer.flush()?;
    emit_progress(
        path,
        total_bytes,
        &compressed_bytes_read,
        &object_counts,
        started_at,
        true,
    );
    Ok(())
}

fn scan_compact_struson_parallel(
    path: &Path,
    context: CompactContext,
    worker_count: usize,
    queue_size: usize,
    copy_paths: CopyPathConfig,
    compact_copy_rotate_bytes: u64,
) -> io::Result<()> {
    let total_bytes = path.metadata().map(|metadata| metadata.len()).unwrap_or(0);
    let compressed_bytes_read = Arc::new(AtomicU64::new(0));
    let reader = open_json_reader(path, Arc::clone(&compressed_bytes_read))?;
    let mut json_reader = JsonStreamReader::new(reader);
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
    let started_at = Instant::now();
    let mut provider_map: HashMap<String, ProviderEntry> = HashMap::new();
    let negotiated_rate_chunk_size = split_interval(
        "HLTHPRT_PTG2_RUST_SPLIT_NEGOTIATED_RATES",
        DEFAULT_SPLIT_NEGOTIATED_RATES,
    );
    let raw_chunk_byte_limit =
        env_usize("HLTHPRT_PTG2_RUST_RAW_CHUNK_BYTES", DEFAULT_RAW_CHUNK_BYTES);
    let provider_refs_in_workers = env_bool("HLTHPRT_PTG2_RUST_PROVIDER_REFS_IN_WORKERS", true);
    let provider_ref_worker_count =
        env_usize("HLTHPRT_PTG2_RUST_PROVIDER_REF_WORKERS", worker_count).max(1);
    let provider_ref_queue_size = env_usize(
        "HLTHPRT_PTG2_RUST_PROVIDER_REF_QUEUE",
        provider_ref_worker_count.max(queue_size).max(1),
    )
    .max(provider_ref_worker_count)
    .max(1);
    let provider_ref_chunk_items = env_usize(
        "HLTHPRT_PTG2_RUST_PROVIDER_REF_CHUNK_ITEMS",
        DEFAULT_PROVIDER_REF_CHUNK_ITEMS,
    )
    .max(1);
    let provider_ref_raw_chunk_byte_limit = env_usize(
        "HLTHPRT_PTG2_RUST_PROVIDER_REF_RAW_CHUNK_BYTES",
        raw_chunk_byte_limit,
    )
    .max(1);
    let parse_in_workers = env_bool(
        "HLTHPRT_PTG2_RUST_PARSE_IN_WORKERS",
        DEFAULT_PARSE_IN_WORKERS,
    );
    let bounded_queue_size = queue_size.max(worker_count).max(1);
    let dedupe = Arc::new(SharedDedupe::new(worker_count));
    let manifest_sidecars = if copy_paths.has_manifest_sidecar_paths() {
        Some(Arc::new(Mutex::new(ManifestSidecarCollector::for_import(
            &copy_paths,
        )?)))
    } else {
        None
    };
    let event_queue_size = env_usize(
        "HLTHPRT_PTG2_RUST_EVENT_QUEUE",
        (bounded_queue_size * 2).max(worker_count),
    );
    let (tx, rx) = bounded::<WorkerJob>(bounded_queue_size);
    let (event_tx, event_rx) = bounded::<CopyFileEvent>(event_queue_size);
    let stdout = io::stdout();
    let mut writer = BufWriter::new(stdout.lock());
    let mut producer_blocked_micros = 0u128;
    let mut provider_ref_producer_blocked_micros = 0u128;
    let mut raw_chunk_stats = RawChunkStats::default();
    let mut provider_ref_raw_chunk_stats = RawChunkStats::default();
    let mut provider_refs_seconds = 0.0f64;
    let mut in_network_enqueue_seconds = 0.0f64;
    let mut worker_join_seconds = 0.0f64;

    emit_json_record(
        &mut writer,
        "scanner_config",
        &json!({
            "worker_count": worker_count,
            "work_queue": bounded_queue_size,
            "event_queue": event_queue_size,
            "split_negotiated_rates": negotiated_rate_chunk_size,
            "raw_chunk_bytes": raw_chunk_byte_limit,
            "read_buffer_bytes": READ_BUF_SIZE,
            "parse_in_workers": parse_in_workers,
            "provider_refs_in_workers": provider_refs_in_workers,
            "provider_ref_workers": provider_ref_worker_count,
            "provider_ref_queue": provider_ref_queue_size,
            "provider_ref_chunk_items": provider_ref_chunk_items,
            "provider_ref_raw_chunk_bytes": provider_ref_raw_chunk_byte_limit,
            "provider_npi_sidecar": copy_paths.manifest_provider_npi_sidecar.is_some(),
            "panic_strategy": "unwind",
        }),
    )?;
    writer.flush()?;

    json_reader.begin_object().map_err(to_io_error)?;
    while json_reader.has_next().map_err(to_io_error)? {
        let name = json_reader.next_name_owned().map_err(to_io_error)?;
        match name.as_str() {
            "provider_references" => {
                let provider_refs_started_at = Instant::now();
                let provider_ref_paths = copy_paths.for_provider_refs();
                let collect_provider_npis = copy_paths.manifest_provider_npi_sidecar.is_some();
                if provider_refs_in_workers && provider_ref_worker_count > 1 {
                    let (provider_tx, provider_rx) =
                        bounded::<RawRateChunk>(provider_ref_queue_size);
                    let mut provider_handles = Vec::with_capacity(provider_ref_worker_count);
                    for worker_id in 0..provider_ref_worker_count {
                        let worker_rx = provider_rx.clone();
                        let worker_dedupe = Arc::clone(&dedupe);
                        let worker_paths = provider_ref_paths.clone();
                        provider_handles.push((
                            worker_id,
                            thread::spawn(move || {
                                let result = panic::catch_unwind(AssertUnwindSafe(|| {
                                    provider_ref_worker_loop(
                                        worker_id,
                                        worker_rx,
                                        worker_dedupe,
                                        worker_paths,
                                        compact_copy_rotate_bytes,
                                        collect_provider_npis,
                                    )
                                }));
                                match result {
                                    Ok(Ok(output)) => Ok(output),
                                    Ok(Err(err)) => {
                                        log_worker_failure(
                                            worker_id,
                                            "provider_ref_error",
                                            &err.to_string(),
                                        );
                                        Err(err)
                                    }
                                    Err(payload) => {
                                        let message = panic_payload_message(payload.as_ref());
                                        log_worker_failure(
                                            worker_id,
                                            "provider_ref_panic",
                                            &message,
                                        );
                                        Err(io::Error::other(format!(
                                            "provider-reference worker {worker_id} panicked: {message}"
                                        )))
                                    }
                                }
                            }),
                        ));
                    }
                    drop(provider_rx);

                    let mut raw_refs = RawRateChunk::with_capacity(
                        provider_ref_chunk_items,
                        provider_ref_raw_chunk_byte_limit.min(READ_BUF_SIZE),
                    );
                    json_reader.begin_array().map_err(to_io_error)?;
                    while json_reader.has_next().map_err(to_io_error)? {
                        let raw_start = raw_refs.byte_len();
                        transfer_next_value_to_bytes_append(&mut json_reader, &mut raw_refs.bytes)?;
                        raw_refs.push_current_value_span(raw_start);
                        *object_counts
                            .entry("provider_references".to_string())
                            .or_insert(0) += 1;
                        if raw_refs.len() >= provider_ref_chunk_items
                            || raw_refs.byte_len() >= provider_ref_raw_chunk_byte_limit
                        {
                            let batch = take_raw_rate_chunk_replacing_with_bounded_capacity(
                                &mut raw_refs,
                                provider_ref_chunk_items,
                                provider_ref_raw_chunk_byte_limit,
                            );
                            provider_ref_raw_chunk_stats.record(batch.len(), batch.byte_len());
                            send_provider_ref_batch(
                                &provider_tx,
                                &event_rx,
                                &mut writer,
                                &mut provider_ref_producer_blocked_micros,
                                batch,
                            )?;
                            drain_copy_file_events(&event_rx, &mut writer)?;
                        }
                    }
                    json_reader.end_array().map_err(to_io_error)?;
                    if !raw_refs.is_empty() {
                        provider_ref_raw_chunk_stats.record(raw_refs.len(), raw_refs.byte_len());
                        send_provider_ref_batch(
                            &provider_tx,
                            &event_rx,
                            &mut writer,
                            &mut provider_ref_producer_blocked_micros,
                            raw_refs,
                        )?;
                        drain_copy_file_events(&event_rx, &mut writer)?;
                    }
                    drop(provider_tx);

                    let mut provider_worker_error: Option<io::Error> = None;
                    let mut copy_file_events = Vec::new();
                    for (worker_id, handle) in provider_handles {
                        match handle.join() {
                            Ok(Ok(output)) => {
                                provider_map.extend(output.provider_map);
                                copy_file_events.extend(output.events);
                            }
                            Ok(Err(err)) => {
                                let message = err.to_string();
                                emit_worker_failure(
                                    &mut writer,
                                    worker_id,
                                    "provider_ref_error",
                                    &message,
                                )?;
                                if provider_worker_error.is_none() {
                                    provider_worker_error = Some(err);
                                }
                            }
                            Err(payload) => {
                                let message = panic_payload_message(payload.as_ref());
                                emit_worker_failure(
                                    &mut writer,
                                    worker_id,
                                    "provider_ref_panic",
                                    &message,
                                )?;
                                if provider_worker_error.is_none() {
                                    provider_worker_error = Some(io::Error::other(format!(
                                        "provider-reference worker {worker_id} panicked: {message}"
                                    )));
                                }
                            }
                        }
                    }
                    if let Some(err) = provider_worker_error {
                        return Err(err);
                    }
                    drain_copy_file_events(&event_rx, &mut writer)?;
                    for event in copy_file_events {
                        emit_copy_file_event(&mut writer, &event)?;
                    }
                } else {
                    let mut provider_ref_copy_sinks = DictionaryCopySinks::from_paths(
                        &provider_ref_paths,
                        compact_copy_rotate_bytes,
                    )?;
                    let mut provider_refs_since_rotate = 0usize;
                    json_reader.begin_array().map_err(to_io_error)?;
                    while json_reader.has_next().map_err(to_io_error)? {
                        let value: Value = json_reader.deserialize_next().map_err(to_io_error)?;
                        if let Some(key_value) = value.get("provider_group_id") {
                            if let (Some(key), Some(entry)) = (
                                provider_ref_key(key_value),
                                build_provider_entry(&value, collect_provider_npis),
                            ) {
                                provider_ref_copy_sinks
                                    .write_provider_group_members_shared(&value, &dedupe)?;
                                provider_map.insert(key, entry);
                                provider_refs_since_rotate += 1;
                                if provider_refs_since_rotate >= 1024 {
                                    for event in provider_ref_copy_sinks.maybe_rotate_silent()? {
                                        emit_copy_file_event(&mut writer, &event)?;
                                    }
                                    provider_refs_since_rotate = 0;
                                }
                            }
                        }
                        *object_counts
                            .entry("provider_references".to_string())
                            .or_insert(0) += 1;
                    }
                    json_reader.end_array().map_err(to_io_error)?;
                    for event in provider_ref_copy_sinks.finish_silent()? {
                        emit_copy_file_event(&mut writer, &event)?;
                    }
                }
                writer.flush()?;
                provider_refs_seconds += provider_refs_started_at.elapsed().as_secs_f64();

                let provider_map = Arc::new(provider_map);
                let mut handles = Vec::with_capacity(worker_count);
                for worker_id in 0..worker_count {
                    let worker_rx = rx.clone();
                    let worker_event_tx = event_tx.clone();
                    let worker_provider_map = Arc::clone(&provider_map);
                    let worker_dedupe = Arc::clone(&dedupe);
                    let worker_manifest_sidecars = manifest_sidecars.as_ref().map(Arc::clone);
                    let worker_copy_paths = copy_paths.clone();
                    let worker_context = context.clone();
                    handles.push((
                        worker_id,
                        thread::spawn(move || {
                            let result = panic::catch_unwind(AssertUnwindSafe(|| {
                                compact_worker_loop(
                                    worker_id,
                                    worker_rx,
                                    CompactWorkerConfig {
                                        event_tx: worker_event_tx,
                                        provider_map: worker_provider_map,
                                        dedupe: worker_dedupe,
                                        manifest_sidecars: worker_manifest_sidecars,
                                        copy_paths: worker_copy_paths,
                                        rotate_bytes: compact_copy_rotate_bytes,
                                        context: worker_context,
                                    },
                                )
                            }));
                            match result {
                                Ok(Ok(events)) => Ok(events),
                                Ok(Err(err)) => {
                                    log_worker_failure(worker_id, "error", &err.to_string());
                                    Err(err)
                                }
                                Err(payload) => {
                                    let message = panic_payload_message(payload.as_ref());
                                    log_worker_failure(worker_id, "panic", &message);
                                    Err(io::Error::other(format!(
                                        "compact worker {worker_id} panicked: {message}"
                                    )))
                                }
                            }
                        }),
                    ));
                }
                drop(rx);

                while json_reader.has_next().map_err(to_io_error)? {
                    let name = json_reader.next_name_owned().map_err(to_io_error)?;
                    match name.as_str() {
                        "in_network" => {
                            let in_network_started_at = Instant::now();
                            json_reader.begin_array().map_err(to_io_error)?;
                            while json_reader.has_next().map_err(to_io_error)? {
                                let mut enqueue_io = InNetworkEnqueueIo {
                                    tx: &tx,
                                    event_rx: &event_rx,
                                    writer: &mut writer,
                                    producer_blocked_micros: &mut producer_blocked_micros,
                                    raw_chunk_stats: &mut raw_chunk_stats,
                                };
                                let rate_count = enqueue_in_network_struson(
                                    &mut json_reader,
                                    &mut enqueue_io,
                                    InNetworkEnqueueOptions {
                                        chunk_size: negotiated_rate_chunk_size,
                                        raw_chunk_byte_limit,
                                        parse_in_workers,
                                    },
                                )?;
                                drain_copy_file_events(&event_rx, &mut writer)?;
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
                                        started_at,
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
                                        started_at,
                                        false,
                                    );
                                    while objects >= next_progress_objects {
                                        next_progress_objects += progress_objects_interval;
                                    }
                                }
                            }
                            json_reader.end_array().map_err(to_io_error)?;
                            in_network_enqueue_seconds +=
                                in_network_started_at.elapsed().as_secs_f64();
                        }
                        _ => {
                            json_reader.skip_value().map_err(to_io_error)?;
                        }
                    }
                }

                drop(tx);
                drop(event_tx);
                let worker_join_started_at = Instant::now();
                let mut worker_error: Option<io::Error> = None;
                let mut copy_file_events = Vec::new();
                for (worker_id, handle) in handles {
                    match handle.join() {
                        Ok(Ok(mut events)) => copy_file_events.append(&mut events),
                        Ok(Err(err)) => {
                            let message = err.to_string();
                            emit_worker_failure(&mut writer, worker_id, "error", &message)?;
                            if worker_error.is_none() {
                                worker_error = Some(err);
                            }
                        }
                        Err(payload) => {
                            let message = panic_payload_message(payload.as_ref());
                            emit_worker_failure(&mut writer, worker_id, "panic", &message)?;
                            if worker_error.is_none() {
                                worker_error = Some(io::Error::other(format!(
                                    "compact worker {worker_id} panicked: {message}"
                                )));
                            }
                        }
                    }
                }
                worker_join_seconds += worker_join_started_at.elapsed().as_secs_f64();
                if let Some(err) = worker_error {
                    return Err(err);
                }
                emit_dedupe_summary(&dedupe, &object_counts);
                drain_copy_file_events(&event_rx, &mut writer)?;
                for event in copy_file_events {
                    emit_copy_file_event(&mut writer, &event)?;
                }
                if let Some(sidecars) = manifest_sidecars.as_ref() {
                    let mut sidecars = sidecars.lock().unwrap();
                    emit_configured_manifest_sidecars(
                        &mut writer,
                        &copy_paths,
                        Some(&mut sidecars),
                    )?;
                }
                emit_json_record(
                    &mut writer,
                    "dedupe_summary",
                    &dedupe_summary_payload(&dedupe, &object_counts),
                )?;
                emit_json_record(
                    &mut writer,
                    "scanner_summary",
                    &json!({
                        "worker_count": worker_count,
                        "work_queue": bounded_queue_size,
                        "event_queue": event_queue_size,
                        "split_negotiated_rates": negotiated_rate_chunk_size,
                        "raw_chunk_bytes": raw_chunk_byte_limit,
                        "raw_chunk_count": raw_chunk_stats.chunk_count,
                        "raw_chunk_total_bytes": raw_chunk_stats.total_bytes,
                        "raw_chunk_max_bytes": raw_chunk_stats.max_bytes,
                        "raw_chunk_max_rates": raw_chunk_stats.max_rates,
                        "provider_ref_raw_chunk_count": provider_ref_raw_chunk_stats.chunk_count,
                        "provider_ref_raw_chunk_total_bytes": provider_ref_raw_chunk_stats.total_bytes,
                        "provider_ref_raw_chunk_max_bytes": provider_ref_raw_chunk_stats.max_bytes,
                        "provider_ref_raw_chunk_max_items": provider_ref_raw_chunk_stats.max_rates,
                        "parse_in_workers": parse_in_workers,
                        "producer_blocked_micros": producer_blocked_micros,
                        "provider_ref_producer_blocked_micros": provider_ref_producer_blocked_micros,
                        "provider_refs_seconds": provider_refs_seconds,
                        "in_network_enqueue_seconds": in_network_enqueue_seconds,
                        "worker_join_seconds": worker_join_seconds,
                        "elapsed_seconds": started_at.elapsed().as_secs_f64(),
                    }),
                )?;
                writer.flush()?;
                json_reader.end_object().map_err(to_io_error)?;
                json_reader
                    .consume_trailing_whitespace()
                    .map_err(to_io_error)?;
                emit_progress(
                    path,
                    total_bytes,
                    &compressed_bytes_read,
                    &object_counts,
                    started_at,
                    true,
                );
                return Ok(());
            }
            _ => {
                json_reader.skip_value().map_err(to_io_error)?;
            }
        }
    }
    json_reader.end_object().map_err(to_io_error)?;
    json_reader
        .consume_trailing_whitespace()
        .map_err(to_io_error)?;
    drop(tx);
    emit_progress(
        path,
        total_bytes,
        &compressed_bytes_read,
        &object_counts,
        started_at,
        true,
    );
    Ok(())
}

fn compact_parallel_has_provider_references(path: &Path) -> io::Result<bool> {
    let compressed_bytes_read = Arc::new(AtomicU64::new(0));
    let reader = open_json_reader(path, Arc::clone(&compressed_bytes_read))?;
    let mut json_reader = JsonStreamReader::new(reader);
    json_reader.begin_object().map_err(to_io_error)?;
    while json_reader.has_next().map_err(to_io_error)? {
        let name = json_reader.next_name_owned().map_err(to_io_error)?;
        match name.as_str() {
            "provider_references" => return Ok(true),
            "in_network" => return Ok(false),
            _ => json_reader.skip_value().map_err(to_io_error)?,
        }
    }
    Ok(false)
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
    let reader = open_json_reader(path, Arc::clone(&compressed_bytes_read))?;
    let mut json_reader = JsonStreamReader::new(reader);
    let stdout = io::stdout();
    let mut writer = BufWriter::new(stdout.lock());
    let compact_copy_rotate_bytes = progress_interval(
        "HLTHPRT_PTG2_COMPACT_SERVING_COPY_ROTATE_BYTES",
        DEFAULT_COMPACT_COPY_ROTATE_BYTES,
    );
    let copy_paths = CopyPathConfig::from_env();
    let rust_worker_count = env_usize("HLTHPRT_PTG2_RUST_WORKERS", DEFAULT_COMPACT_RUST_WORKERS);
    let rust_queue_size = env_usize(
        "HLTHPRT_PTG2_RUST_WORK_QUEUE",
        DEFAULT_COMPACT_RUST_WORK_QUEUE,
    );
    let source_network_names = env_json_text_list("HLTHPRT_PTG2_SOURCE_NETWORK_NAMES_JSON");
    if rust_worker_count > 1
        && copy_paths.has_file_paths()
        && compact_parallel_has_provider_references(path)?
    {
        if env_bool("HLTHPRT_PTG2_RUST_TOP_LEVEL_BYTE_SCAN", false) {
            return scan_compact_byte_top_level_parallel(
                path,
                CompactContext {
                    snapshot_id,
                    plan_id,
                    plan_month_id,
                    source_trace_set_hash,
                    confidence_code,
                    source_network_names: source_network_names.clone(),
                },
                rust_worker_count,
                rust_queue_size,
                copy_paths,
                compact_copy_rotate_bytes,
            );
        }
        return scan_compact_struson_parallel(
            path,
            CompactContext {
                snapshot_id,
                plan_id,
                plan_month_id,
                source_trace_set_hash,
                confidence_code,
                source_network_names,
            },
            rust_worker_count,
            rust_queue_size,
            copy_paths,
            compact_copy_rotate_bytes,
        );
    }
    let mut compact_copy_writer: Option<CompactCopySink> = match copy_paths.compact.as_ref() {
        Some(copy_path) => Some(CompactCopySink::new_file(
            copy_path.clone(),
            compact_copy_rotate_bytes,
        )?),
        None => None,
    };
    let mut manifest_serving_copy_writer: Option<CompactCopySink> =
        match copy_paths.manifest_serving_copy_path() {
            Some(copy_path) => Some(CompactCopySink::new_manifest_serving_file(
                copy_path.clone(),
                compact_copy_rotate_bytes,
                copy_paths.manifest_serving_copy_layout(),
            )?),
            None => None,
        };
    let mut dictionary_copy_sinks =
        DictionaryCopySinks::from_paths(&copy_paths, compact_copy_rotate_bytes)?;
    let mut manifest_sidecars = if copy_paths.has_manifest_sidecar_paths() {
        Some(ManifestSidecarCollector::for_import(&copy_paths)?)
    } else {
        None
    };
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
    let started_at = Instant::now();
    let mut provider_map: HashMap<String, ProviderEntry> = HashMap::new();
    let mut emitted_price_code_sets: HashSet<String> = HashSet::new();
    let mut emitted_price_atoms: HashSet<String> = HashSet::new();
    let mut emitted_price_sets: HashSet<String> = HashSet::new();
    let mut emitted_price_set_entries: HashSet<(String, String)> = HashSet::new();
    let mut emitted_provider_sets: HashSet<String> = HashSet::new();
    let mut emitted_provider_set_components: HashSet<(String, i64)> = HashSet::new();
    let mut emitted_provider_set_entries: HashSet<(String, i64)> = HashSet::new();
    let mut emitted_provider_entry_components: HashSet<(i64, i64)> = HashSet::new();
    let mut emitted_procedures: HashSet<String> = HashSet::new();
    let mut emitted_provider_group_members: HashSet<(i64, i64)> = HashSet::new();
    let mut price_code_set_hash_cache: PriceCodeSetHashCache = HashMap::new();
    let mut manifest_global_id_cache = ManifestGlobalIdCache::default();
    let negotiated_rate_chunk_size = split_interval(
        "HLTHPRT_PTG2_RUST_SPLIT_NEGOTIATED_RATES",
        DEFAULT_SPLIT_NEGOTIATED_RATES,
    );

    json_reader.begin_object().map_err(to_io_error)?;
    while json_reader.has_next().map_err(to_io_error)? {
        let name = json_reader.next_name_owned().map_err(to_io_error)?;
        match name.as_str() {
            "provider_references" => {
                let collect_provider_npis = copy_paths.manifest_provider_npi_sidecar.is_some();
                json_reader.begin_array().map_err(to_io_error)?;
                while json_reader.has_next().map_err(to_io_error)? {
                    let value: Value = json_reader.deserialize_next().map_err(to_io_error)?;
                    if let Some(key_value) = value.get("provider_group_id") {
                        if let (Some(key), Some(entry)) = (
                            provider_ref_key(key_value),
                            build_provider_entry(&value, collect_provider_npis),
                        ) {
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
                    let mut stream_state = InNetworkStreamState {
                        writer: &mut writer,
                        compact_copy_writer: &mut compact_copy_writer,
                        manifest_serving_copy_writer: &mut manifest_serving_copy_writer,
                        dictionary_copy_sinks: &mut dictionary_copy_sinks,
                        manifest_sidecars: manifest_sidecars.as_mut(),
                        suppress_v2_serving_output: copy_paths.manifest_only,
                        provider_map: &provider_map,
                        dedupe: LocalCompactDedupe {
                            price_code_sets: &mut emitted_price_code_sets,
                            price_atoms: &mut emitted_price_atoms,
                            price_sets: &mut emitted_price_sets,
                            price_set_entries: &mut emitted_price_set_entries,
                            provider_sets: &mut emitted_provider_sets,
                            provider_set_components: &mut emitted_provider_set_components,
                            provider_set_entries: &mut emitted_provider_set_entries,
                            provider_entry_components: &mut emitted_provider_entry_components,
                            procedures: &mut emitted_procedures,
                            provider_group_members: &mut emitted_provider_group_members,
                        },
                        price_code_set_hash_cache: &mut price_code_set_hash_cache,
                        manifest_global_id_cache: &mut manifest_global_id_cache,
                        context: CompactContext {
                            snapshot_id: snapshot_id.clone(),
                            plan_id: plan_id.clone(),
                            plan_month_id: plan_month_id.clone(),
                            source_trace_set_hash: source_trace_set_hash.clone(),
                            confidence_code: confidence_code.clone(),
                            source_network_names: source_network_names.clone(),
                        },
                        chunk_size: negotiated_rate_chunk_size,
                    };
                    let rate_count =
                        process_in_network_struson(&mut json_reader, &mut stream_state)?;
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
                            started_at,
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
                            started_at,
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
    json_reader
        .consume_trailing_whitespace()
        .map_err(to_io_error)?;
    if let Some(copy_writer) = compact_copy_writer.take() {
        copy_writer.finish(&mut writer)?;
    }
    if let Some(copy_writer) = manifest_serving_copy_writer.take() {
        copy_writer.finish(&mut writer)?;
    }
    dictionary_copy_sinks.finish(&mut writer)?;
    emit_configured_manifest_sidecars(&mut writer, &copy_paths, manifest_sidecars.as_mut())?;
    writer.flush()?;
    emit_progress(
        path,
        total_bytes,
        &compressed_bytes_read,
        &object_counts,
        started_at,
        true,
    );
    Ok(())
}

fn scan_compact(path: &Path) -> io::Result<()> {
    scan_compact_struson(path)
}

fn manifest_copy_key(kind: &str, line: &[u8]) -> Vec<u8> {
    match kind {
        "manifest_lean_serving" => line.strip_suffix(b"\n").unwrap_or(line).to_vec(),
        "price_set_atom" => line.strip_suffix(b"\n").unwrap_or(line).to_vec(),
        "provider_group_member" => {
            let mut tabs_seen = 0;
            for (index, byte) in line.iter().enumerate() {
                if *byte == b'\t' {
                    tabs_seen += 1;
                    if tabs_seen == 2 {
                        return line[..index].to_vec();
                    }
                }
            }
            line.strip_suffix(b"\n").unwrap_or(line).to_vec()
        }
        _ => {
            let end = line
                .iter()
                .position(|byte| *byte == b'\t' || *byte == b'\n')
                .unwrap_or(line.len());
            line[..end].to_vec()
        }
    }
}

fn manifest_serving_trace_is_null(line: &[u8]) -> bool {
    let mut column = 0usize;
    let mut start = 0usize;
    for (index, byte) in line.iter().enumerate() {
        if *byte == b'\t' || *byte == b'\n' {
            if column == 8 {
                return &line[start..index] == b"\\N";
            }
            column += 1;
            start = index + 1;
        }
    }
    column <= 8
}

fn manifest_prefer_row(kind: &str, current: Vec<u8>, candidate: Vec<u8>) -> Vec<u8> {
    if kind == "manifest_serving"
        && manifest_serving_trace_is_null(&current)
        && !manifest_serving_trace_is_null(&candidate)
    {
        return candidate;
    }
    current
}

struct ManifestChunkRow {
    key: Vec<u8>,
    line: Vec<u8>,
}

fn manifest_sort_chunk(
    kind: &str,
    rows: Vec<Vec<u8>>,
    chunk_index: usize,
    temp_dir: &Path,
) -> io::Result<PathBuf> {
    let mut keyed_rows: Vec<ManifestChunkRow> = rows
        .into_iter()
        .map(|line| ManifestChunkRow {
            key: manifest_copy_key(kind, &line),
            line,
        })
        .collect();
    keyed_rows.sort_unstable_by(|left, right| {
        left.key
            .cmp(&right.key)
            .then_with(|| left.line.cmp(&right.line))
    });
    let path = temp_dir.join(format!(
        "ptg2_manifest_merge_{}_{}_{}.chunk",
        kind,
        std::process::id(),
        chunk_index
    ));
    let mut writer = BufWriter::new(File::create(&path)?);
    for row in keyed_rows {
        writer.write_all(&row.line)?;
    }
    writer.flush()?;
    Ok(path)
}

fn manifest_spawn_sort_chunk(
    kind: &str,
    rows: Vec<Vec<u8>>,
    chunk_index: usize,
    temp_dir: &Path,
) -> thread::JoinHandle<io::Result<(usize, PathBuf)>> {
    let kind = kind.to_string();
    let temp_dir = temp_dir.to_path_buf();
    thread::spawn(move || {
        manifest_sort_chunk(&kind, rows, chunk_index, &temp_dir).map(|path| (chunk_index, path))
    })
}

fn manifest_join_sort_chunk(
    handle: thread::JoinHandle<io::Result<(usize, PathBuf)>>,
) -> io::Result<(usize, PathBuf)> {
    match handle.join() {
        Ok(result) => result,
        Err(payload) => Err(io::Error::other(format!(
            "manifest copy merge sort worker panicked: {}",
            panic_payload_message(payload.as_ref())
        ))),
    }
}

fn manifest_flush_sort_handles(
    handles: &mut Vec<thread::JoinHandle<io::Result<(usize, PathBuf)>>>,
    sorted_chunks: &mut Vec<(usize, PathBuf)>,
    max_pending: usize,
) -> io::Result<()> {
    while handles.len() >= max_pending {
        let handle = handles.remove(0);
        sorted_chunks.push(manifest_join_sort_chunk(handle)?);
    }
    Ok(())
}

#[derive(Eq)]
struct ManifestMergeItem {
    key: Vec<u8>,
    line: Vec<u8>,
    reader_index: usize,
}

impl Ord for ManifestMergeItem {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        other
            .key
            .cmp(&self.key)
            .then_with(|| other.line.cmp(&self.line))
            .then_with(|| other.reader_index.cmp(&self.reader_index))
    }
}

impl PartialOrd for ManifestMergeItem {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for ManifestMergeItem {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.line == other.line && self.reader_index == other.reader_index
    }
}

fn manifest_merge_read_next(
    kind: &str,
    reader_index: usize,
    readers: &mut [BufReader<File>],
    heap: &mut BinaryHeap<ManifestMergeItem>,
) -> io::Result<()> {
    let mut line = Vec::new();
    let bytes = readers[reader_index].read_until(b'\n', &mut line)?;
    if bytes > 0 {
        heap.push(ManifestMergeItem {
            key: manifest_copy_key(kind, &line),
            line,
            reader_index,
        });
    }
    Ok(())
}

const PTG2_SERVING_BY_CODE_MAGIC: &[u8; 8] = b"PTG2SBC1";
const PTG2_SERVING_BY_PROVIDER_SET_MAGIC: &[u8; 8] = b"PTG2SBP1";
const PTG2_SERVING_BY_CODE_FORMAT: &str = "ptg2_serving_by_code_v1";
const PTG2_SERVING_BY_PROVIDER_SET_FORMAT: &str = "ptg2_serving_by_provider_set_v1";
const PTG2_SERVING_BINARY_TABLE_FORMAT: &str = "ptg2_serving_binary_blocks_v1";
const PTG2_SERVING_BINARY_BY_CODE_KIND: &str = "by_code";
const PTG2_SERVING_BINARY_BY_CODE_GROUPED_KIND: &str = "by_code_grouped";
const PTG2_SERVING_BINARY_BY_CODE_DICTIONARY_KIND: &str = "by_code_price_dictionary";
const PTG2_SERVING_BINARY_PROVIDER_COUNT_DICTIONARY_KIND: &str = "provider_set_count_dictionary";
const PTG2_SERVING_BINARY_BY_PROVIDER_SET_KIND: &str = "by_provider_set";
const PTG2_SERVING_BINARY_BY_PROVIDER_SET_DICTIONARY_KIND: &str =
    "by_provider_set_price_dictionary";
const PTG2_SERVING_BINARY_PRICE_SET_ATOMS_BY_ID_V2_KIND: &str = "price_set_atoms_by_id_v2";
const PTG2_SERVING_BINARY_PRICE_SET_ATOMS_BY_ID_V2_FORMAT: &str = "price_set_atoms_by_id_v2";
const PTG2_SERVING_BINARY_PRICE_SET_ATOM_ID_V2_PREFIX_BYTES: usize = 2;
const PTG2_SERVING_BINARY_PRICE_SET_ATOM_ID_V2_BUCKETS: usize =
    1 << (PTG2_SERVING_BINARY_PRICE_SET_ATOM_ID_V2_PREFIX_BYTES * 8);
const PTG2_SERVING_BINARY_BLOCK_BYTES_ENV: &str = "HLTHPRT_PTG2_SERVING_BINARY_BLOCK_BYTES";
const PTG2_SERVING_BINARY_PAYLOAD_COMPRESSION_ENV: &str =
    "HLTHPRT_PTG2_SERVING_BINARY_PAYLOAD_COMPRESSION";
const PTG2_SERVING_BINARY_PAYLOAD_COMPRESSION_LEVEL_ENV: &str =
    "HLTHPRT_PTG2_SERVING_BINARY_PAYLOAD_COMPRESSION_LEVEL";
const PTG2_SERVING_BINARY_PAYLOAD_COMPRESSION_MIN_BYTES_ENV: &str =
    "HLTHPRT_PTG2_SERVING_BINARY_PAYLOAD_COMPRESSION_MIN_BYTES";
const PTG2_SERVING_BINARY_TARGET_COPY_FORMAT_ENV: &str =
    "HLTHPRT_PTG2_SERVING_BINARY_TARGET_COPY_FORMAT";
const DEFAULT_SERVING_BINARY_BLOCK_BYTES: usize = 2 * 1024 * 1024;
const DEFAULT_SERVING_BINARY_COMPRESSION_MIN_BYTES: usize = 128;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ServingBinaryTargetCopyFormat {
    Text,
    Binary,
}

#[derive(Clone, Copy)]
struct ServingBlock {
    key: i32,
    offset: u64,
    count: u32,
}

fn serving_copy_fields(line: &[u8]) -> Vec<&[u8]> {
    let trimmed = line.strip_suffix(b"\n").unwrap_or(line);
    trimmed.split(|byte| *byte == b'\t').collect()
}

fn serving_parse_i32(field: &[u8], name: &str) -> io::Result<i32> {
    let text = std::str::from_utf8(field).map_err(to_io_error)?;
    text.parse::<i32>().map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid {name} integer in serving copy row: {error}"),
        )
    })
}

fn serving_parse_u64(field: &[u8], name: &str) -> io::Result<u64> {
    let text = std::str::from_utf8(field).map_err(to_io_error)?;
    text.parse::<u64>().map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid {name} integer in serving copy row: {error}"),
        )
    })
}

fn serving_parse_global_id(field: &[u8]) -> io::Result<[u8; GLOBAL_ID_BYTES]> {
    let text = std::str::from_utf8(field).map_err(to_io_error)?;
    let mut compact = String::with_capacity(GLOBAL_ID_BYTES * 2);
    for ch in text.trim().chars() {
        if ch != '-' {
            compact.push(ch);
        }
    }
    if compact.len() != GLOBAL_ID_BYTES * 2 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("serving copy 128-bit id must have 32 hex chars, got {text:?}"),
        ));
    }
    let mut out = [0u8; GLOBAL_ID_BYTES];
    for (index, slot) in out.iter_mut().enumerate() {
        let start = index * 2;
        *slot = u8::from_str_radix(&compact[start..start + 2], 16).map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("serving copy 128-bit id is not hex: {error}"),
            )
        })?;
    }
    Ok(out)
}

fn serving_parse_global_id_binary(field: &[u8]) -> io::Result<[u8; GLOBAL_ID_BYTES]> {
    if field.len() == GLOBAL_ID_BYTES {
        let mut out = [0u8; GLOBAL_ID_BYTES];
        out.copy_from_slice(field);
        return Ok(out);
    }
    serving_parse_global_id(field)
}

#[derive(Clone, Copy)]
struct ServingBinaryInputRow {
    first_key: i32,
    second_key: i32,
    provider_count: u64,
    price_set_id: [u8; GLOBAL_ID_BYTES],
}

#[derive(Clone, Copy)]
struct ServingBinaryPriceSetAtomInputRow {
    price_set_id: [u8; GLOBAL_ID_BYTES],
    price_atom_id: [u8; GLOBAL_ID_BYTES],
}

fn serving_binary_row_from_text_fields(
    fields: &[&[u8]],
    label: &str,
) -> io::Result<ServingBinaryInputRow> {
    if fields.len() < 4 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("serving-binary {label} COPY rows must have at least 4 fields"),
        ));
    }
    Ok(ServingBinaryInputRow {
        first_key: serving_parse_i32(fields[0], "first_key")?,
        second_key: serving_parse_i32(fields[1], "second_key")?,
        provider_count: serving_parse_u64(fields[2], "provider_count")?,
        price_set_id: serving_parse_global_id(fields[3])?,
    })
}

fn serving_binary_price_set_atom_row_from_text_fields(
    fields: &[&[u8]],
) -> io::Result<ServingBinaryPriceSetAtomInputRow> {
    if fields.len() != 2 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "price-set atom serving-binary COPY rows must have 2 fields, got {}",
                fields.len()
            ),
        ));
    }
    Ok(ServingBinaryPriceSetAtomInputRow {
        price_set_id: serving_parse_global_id(fields[0])?,
        price_atom_id: serving_parse_global_id(fields[1])?,
    })
}

fn read_exact_optional<R: Read>(reader: &mut R, buffer: &mut [u8]) -> io::Result<bool> {
    let mut offset = 0usize;
    while offset < buffer.len() {
        match reader.read(&mut buffer[offset..])? {
            0 if offset == 0 => return Ok(false),
            0 => {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "unexpected EOF inside PostgreSQL binary COPY stream",
                ));
            }
            read_bytes => offset += read_bytes,
        }
    }
    Ok(true)
}

fn read_i16_be<R: Read>(reader: &mut R) -> io::Result<Option<i16>> {
    let mut buffer = [0u8; 2];
    if !read_exact_optional(reader, &mut buffer)? {
        return Ok(None);
    }
    Ok(Some(i16::from_be_bytes(buffer)))
}

fn read_i32_be<R: Read>(reader: &mut R) -> io::Result<i32> {
    let mut buffer = [0u8; 4];
    reader.read_exact(&mut buffer)?;
    Ok(i32::from_be_bytes(buffer))
}

fn read_pg_binary_copy_header<R: Read>(reader: &mut R) -> io::Result<()> {
    let mut signature = [0u8; 11];
    reader.read_exact(&mut signature)?;
    if &signature != b"PGCOPY\n\xff\r\n\0" {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "PostgreSQL binary COPY stream has an invalid header",
        ));
    }
    let _flags = read_i32_be(reader)?;
    let extension_length = read_i32_be(reader)?;
    if extension_length < 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "PostgreSQL binary COPY stream has a negative extension length",
        ));
    }
    if extension_length > 0 {
        let mut extension = vec![0u8; extension_length as usize];
        reader.read_exact(&mut extension)?;
    }
    Ok(())
}

fn read_pg_binary_field<R: Read>(reader: &mut R) -> io::Result<Vec<u8>> {
    let field_length = read_i32_be(reader)?;
    if field_length < 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "serving-binary source COPY rows cannot contain NULL fields",
        ));
    }
    let mut field = vec![0u8; field_length as usize];
    reader.read_exact(&mut field)?;
    Ok(field)
}

fn pg_binary_i32(field: &[u8], name: &str) -> io::Result<i32> {
    if field.len() != 4 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "PostgreSQL binary COPY field {name} must be int4, got {} bytes",
                field.len()
            ),
        ));
    }
    let mut buffer = [0u8; 4];
    buffer.copy_from_slice(field);
    Ok(i32::from_be_bytes(buffer))
}

fn pg_binary_u64(field: &[u8], name: &str) -> io::Result<u64> {
    match field.len() {
        4 => {
            let mut buffer = [0u8; 4];
            buffer.copy_from_slice(field);
            let value = i32::from_be_bytes(buffer);
            if value < 0 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("PostgreSQL binary COPY field {name} cannot be negative"),
                ));
            }
            Ok(value as u64)
        }
        8 => {
            let mut buffer = [0u8; 8];
            buffer.copy_from_slice(field);
            let value = i64::from_be_bytes(buffer);
            if value < 0 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("PostgreSQL binary COPY field {name} cannot be negative"),
                ));
            }
            Ok(value as u64)
        }
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "PostgreSQL binary COPY field {name} must be int4 or int8, got {} bytes",
                field.len()
            ),
        )),
    }
}

fn read_pg_binary_serving_row<R: Read>(
    reader: &mut R,
) -> io::Result<Option<ServingBinaryInputRow>> {
    let Some(field_count) = read_i16_be(reader)? else {
        return Ok(None);
    };
    if field_count == -1 {
        return Ok(None);
    }
    if field_count != 4 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("serving-binary source COPY row must have 4 fields, got {field_count}"),
        ));
    }
    let first_key = read_pg_binary_field(reader)?;
    let second_key = read_pg_binary_field(reader)?;
    let provider_count = read_pg_binary_field(reader)?;
    let price_set_id = read_pg_binary_field(reader)?;
    Ok(Some(ServingBinaryInputRow {
        first_key: pg_binary_i32(&first_key, "first_key")?,
        second_key: pg_binary_i32(&second_key, "second_key")?,
        provider_count: pg_binary_u64(&provider_count, "provider_count")?,
        price_set_id: serving_parse_global_id_binary(&price_set_id)?,
    }))
}

fn read_pg_binary_price_set_atom_row<R: Read>(
    reader: &mut R,
) -> io::Result<Option<ServingBinaryPriceSetAtomInputRow>> {
    let Some(field_count) = read_i16_be(reader)? else {
        return Ok(None);
    };
    if field_count == -1 {
        return Ok(None);
    }
    if field_count != 2 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("price-set atom serving-binary COPY row must have 2 fields, got {field_count}"),
        ));
    }
    let price_set_id = read_pg_binary_field(reader)?;
    let price_atom_id = read_pg_binary_field(reader)?;
    Ok(Some(ServingBinaryPriceSetAtomInputRow {
        price_set_id: serving_parse_global_id_binary(&price_set_id)?,
        price_atom_id: serving_parse_global_id_binary(&price_atom_id)?,
    }))
}

fn serving_price_key(
    price_set_id: [u8; GLOBAL_ID_BYTES],
    price_set_to_key: &mut HashMap<[u8; GLOBAL_ID_BYTES], u32>,
    price_set_values: &mut Vec<[u8; GLOBAL_ID_BYTES]>,
) -> io::Result<u32> {
    if let Some(value) = price_set_to_key.get(&price_set_id) {
        return Ok(*value);
    }
    let key = u32::try_from(price_set_values.len()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "serving sidecar has more than u32::MAX price-set ids",
        )
    })?;
    price_set_to_key.insert(price_set_id, key);
    price_set_values.push(price_set_id);
    Ok(key)
}

fn write_uvarint_to_vec(out: &mut Vec<u8>, value: u64) {
    let mut encoded = value;
    loop {
        let byte = (encoded & 0x7f) as u8;
        encoded >>= 7;
        if encoded == 0 {
            out.push(byte);
            break;
        }
        out.push(byte | 0x80);
    }
}

fn serving_binary_block_bytes() -> usize {
    env_usize(
        PTG2_SERVING_BINARY_BLOCK_BYTES_ENV,
        DEFAULT_SERVING_BINARY_BLOCK_BYTES,
    )
    .max(64 * 1024)
}

fn serving_binary_payload_compression() -> String {
    let value = env::var(PTG2_SERVING_BINARY_PAYLOAD_COMPRESSION_ENV)
        .unwrap_or_else(|_| "zlib".to_string())
        .trim()
        .to_ascii_lowercase();
    if value == "none" || value == "zlib" {
        value
    } else {
        "zlib".to_string()
    }
}

fn serving_binary_payload_compression_level() -> u32 {
    env::var(PTG2_SERVING_BINARY_PAYLOAD_COMPRESSION_LEVEL_ENV)
        .ok()
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(6)
        .min(9)
}

fn serving_binary_payload_compression_min_bytes() -> usize {
    env_usize(
        PTG2_SERVING_BINARY_PAYLOAD_COMPRESSION_MIN_BYTES_ENV,
        DEFAULT_SERVING_BINARY_COMPRESSION_MIN_BYTES,
    )
}

fn serving_binary_payload_for_copy(payload: &[u8]) -> io::Result<(Vec<u8>, &'static str, usize)> {
    if serving_binary_payload_compression() != "zlib"
        || payload.len() < serving_binary_payload_compression_min_bytes()
    {
        return Ok((payload.to_vec(), "none", 0));
    }
    let mut encoder = ZlibEncoder::new(
        Vec::with_capacity(payload.len()),
        Compression::new(serving_binary_payload_compression_level()),
    );
    encoder.write_all(payload)?;
    let compressed = encoder.finish()?;
    if compressed.len() >= payload.len() {
        return Ok((payload.to_vec(), "none", 0));
    }
    Ok((compressed, "zlib", payload.len()))
}

fn serving_binary_target_copy_format() -> ServingBinaryTargetCopyFormat {
    let value = env::var(PTG2_SERVING_BINARY_TARGET_COPY_FORMAT_ENV)
        .unwrap_or_else(|_| "text".to_string())
        .trim()
        .to_ascii_lowercase();
    if value == "binary" {
        ServingBinaryTargetCopyFormat::Binary
    } else {
        ServingBinaryTargetCopyFormat::Text
    }
}

fn serving_binary_bytea_copy_field(payload: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut text = String::with_capacity(2 + payload.len() * 2);
    text.push('\\');
    text.push('x');
    for byte in payload {
        text.push(HEX[(byte >> 4) as usize] as char);
        text.push(HEX[(byte & 0x0f) as usize] as char);
    }
    pg_text_copy_field(Some(&text))
}

fn serving_binary_i32_count(value: usize, name: &str) -> io::Result<i32> {
    i32::try_from(value).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("PTG2 serving binary {name} exceeds PostgreSQL integer range"),
        )
    })
}

fn write_pg_binary_copy_header<W: Write>(writer: &mut W) -> io::Result<()> {
    writer.write_all(b"PGCOPY\n\xff\r\n\0")?;
    writer.write_all(&0i32.to_be_bytes())?;
    writer.write_all(&0i32.to_be_bytes())
}

fn write_pg_binary_copy_trailer<W: Write>(writer: &mut W) -> io::Result<()> {
    writer.write_all(&(-1i16).to_be_bytes())
}

fn write_pg_binary_copy_field<W: Write>(writer: &mut W, field: &[u8]) -> io::Result<()> {
    let field_len = i32::try_from(field.len()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "PostgreSQL binary COPY field exceeds int32 length",
        )
    })?;
    writer.write_all(&field_len.to_be_bytes())?;
    writer.write_all(field)
}

fn write_pg_binary_copy_i32_field<W: Write>(writer: &mut W, value: i32) -> io::Result<()> {
    write_pg_binary_copy_field(writer, &value.to_be_bytes())
}

fn write_serving_binary_copy_header<W: Write>(
    writer: &mut W,
    target_format: ServingBinaryTargetCopyFormat,
) -> io::Result<()> {
    if target_format == ServingBinaryTargetCopyFormat::Binary {
        write_pg_binary_copy_header(writer)?;
    }
    Ok(())
}

fn write_serving_binary_copy_trailer<W: Write>(
    writer: &mut W,
    target_format: ServingBinaryTargetCopyFormat,
) -> io::Result<()> {
    if target_format == ServingBinaryTargetCopyFormat::Binary {
        write_pg_binary_copy_trailer(writer)?;
    }
    Ok(())
}

struct CountingWriter<W: Write> {
    inner: W,
    byte_count: u64,
}

impl<W: Write> CountingWriter<W> {
    fn new(inner: W) -> Self {
        Self {
            inner,
            byte_count: 0,
        }
    }

    fn byte_count(&self) -> u64 {
        self.byte_count
    }
}

impl<W: Write> Write for CountingWriter<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let written = self.inner.write(buf)?;
        self.byte_count = self.byte_count.saturating_add(written as u64);
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

fn write_serving_binary_copy_record<W: Write>(
    writer: &mut W,
    target_format: ServingBinaryTargetCopyFormat,
    kind: &str,
    block_key: i32,
    block_no: usize,
    entry_count: usize,
    payload: &[u8],
) -> io::Result<()> {
    let (stored_payload, payload_compression, raw_payload_bytes) =
        serving_binary_payload_for_copy(payload)?;
    let block_no = serving_binary_i32_count(block_no, "block_no")?;
    let entry_count = serving_binary_i32_count(entry_count, "entry_count")?;
    let raw_payload_bytes = serving_binary_i32_count(raw_payload_bytes, "raw_payload_bytes")?;
    if target_format == ServingBinaryTargetCopyFormat::Binary {
        writer.write_all(&7i16.to_be_bytes())?;
        write_pg_binary_copy_field(writer, kind.as_bytes())?;
        write_pg_binary_copy_i32_field(writer, block_key)?;
        write_pg_binary_copy_i32_field(writer, block_no)?;
        write_pg_binary_copy_i32_field(writer, entry_count)?;
        write_pg_binary_copy_field(writer, &stored_payload)?;
        write_pg_binary_copy_field(writer, payload_compression.as_bytes())?;
        write_pg_binary_copy_i32_field(writer, raw_payload_bytes)?;
        return Ok(());
    }
    write_copy_fields(
        writer,
        &[
            pg_text_copy_field(Some(kind)),
            block_key.to_string(),
            block_no.to_string(),
            entry_count.to_string(),
            serving_binary_bytea_copy_field(&stored_payload),
            pg_text_copy_field(Some(payload_compression)),
            raw_payload_bytes.to_string(),
        ],
    )
}

fn uvarint_encoded_len(mut value: usize) -> usize {
    let mut encoded_len = 1usize;
    while value >= 0x80 {
        value >>= 7;
        encoded_len += 1;
    }
    encoded_len
}

struct ServingBinaryPriceSetAtomState {
    max_payload_bytes: usize,
    current_bucket_key: Option<i32>,
    current_block_no: usize,
    current_payload: Vec<u8>,
    current_entry_count: usize,
    current_price_set_id: Option<[u8; GLOBAL_ID_BYTES]>,
    current_atom_ids: Vec<[u8; GLOBAL_ID_BYTES]>,
    previous_atom_id: Option<[u8; GLOBAL_ID_BYTES]>,
    block_count: u64,
    price_set_count: u64,
    atom_ref_count: u64,
    record_count: u64,
}

impl ServingBinaryPriceSetAtomState {
    fn new(max_payload_bytes: usize) -> Self {
        Self {
            max_payload_bytes,
            current_bucket_key: None,
            current_block_no: 0,
            current_payload: Vec::with_capacity(max_payload_bytes.min(1024 * 1024)),
            current_entry_count: 0,
            current_price_set_id: None,
            current_atom_ids: Vec::new(),
            previous_atom_id: None,
            block_count: 0,
            price_set_count: 0,
            atom_ref_count: 0,
            record_count: 0,
        }
    }

    fn membership_payload_bytes(atom_count: usize) -> io::Result<usize> {
        atom_count
            .checked_mul(GLOBAL_ID_BYTES)
            .and_then(|atom_bytes| atom_bytes.checked_add(GLOBAL_ID_BYTES))
            .and_then(|payload_bytes| payload_bytes.checked_add(uvarint_encoded_len(atom_count)))
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "price-set atom membership payload length overflowed usize",
                )
            })
    }

    fn ensure_membership_fits(&self, atom_count: usize) -> io::Result<()> {
        let membership_bytes = Self::membership_payload_bytes(atom_count)?;
        if membership_bytes > self.max_payload_bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "price-set atom membership requires {membership_bytes} bytes, exceeding the maximum single-membership size of {} bytes",
                    self.max_payload_bytes
                ),
            ));
        }
        Ok(())
    }

    fn flush_block<W: Write>(
        &mut self,
        writer: &mut W,
        target_format: ServingBinaryTargetCopyFormat,
    ) -> io::Result<()> {
        let Some(bucket_key) = self.current_bucket_key else {
            return Ok(());
        };
        if self.current_entry_count == 0 {
            return Ok(());
        }
        write_serving_binary_copy_record(
            writer,
            target_format,
            PTG2_SERVING_BINARY_PRICE_SET_ATOMS_BY_ID_V2_KIND,
            bucket_key,
            self.current_block_no,
            self.current_entry_count,
            &self.current_payload,
        )?;
        self.current_payload.clear();
        self.current_entry_count = 0;
        self.current_block_no += 1;
        self.block_count = self.block_count.saturating_add(1);
        self.record_count = self.record_count.saturating_add(1);
        Ok(())
    }

    fn flush_membership<W: Write>(
        &mut self,
        writer: &mut W,
        target_format: ServingBinaryTargetCopyFormat,
    ) -> io::Result<()> {
        let Some(price_set_id) = self.current_price_set_id.take() else {
            return Ok(());
        };
        let atom_ids = std::mem::take(&mut self.current_atom_ids);
        self.ensure_membership_fits(atom_ids.len())?;
        let bucket_key = i32::from(u16::from_be_bytes([
            price_set_id[0],
            price_set_id[PTG2_SERVING_BINARY_PRICE_SET_ATOM_ID_V2_PREFIX_BYTES - 1],
        ]));
        let entry_bytes = Self::membership_payload_bytes(atom_ids.len())?;
        if self.current_bucket_key != Some(bucket_key) {
            self.flush_block(writer, target_format)?;
            self.current_bucket_key = Some(bucket_key);
            self.current_block_no = 0;
        } else if self.current_entry_count > 0
            && self
                .current_payload
                .len()
                .checked_add(entry_bytes)
                .is_none_or(|payload_bytes| payload_bytes > self.max_payload_bytes)
        {
            self.flush_block(writer, target_format)?;
        }
        self.current_payload.extend_from_slice(&price_set_id);
        write_uvarint_to_vec(&mut self.current_payload, atom_ids.len() as u64);
        for atom_id in atom_ids {
            self.current_payload.extend_from_slice(&atom_id);
        }
        self.current_entry_count += 1;
        self.price_set_count = self.price_set_count.saturating_add(1);
        self.previous_atom_id = None;
        Ok(())
    }

    fn push_row<W: Write>(
        &mut self,
        writer: &mut W,
        target_format: ServingBinaryTargetCopyFormat,
        row: ServingBinaryPriceSetAtomInputRow,
    ) -> io::Result<()> {
        if let Some(current_price_set_id) = self.current_price_set_id {
            if row.price_set_id < current_price_set_id {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "price-set atom rows must be ordered by price_set_global_id_128",
                ));
            }
            if row.price_set_id != current_price_set_id {
                self.flush_membership(writer, target_format)?;
            }
        }
        if self.current_price_set_id.is_none() {
            self.current_price_set_id = Some(row.price_set_id);
        }
        if let Some(previous_atom_id) = self.previous_atom_id {
            if row.price_atom_id < previous_atom_id {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "price-set atom rows must be ordered by price_atom_global_id_128 within each price set",
                ));
            }
        }
        self.ensure_membership_fits(self.current_atom_ids.len().saturating_add(1))?;
        self.current_atom_ids.push(row.price_atom_id);
        self.previous_atom_id = Some(row.price_atom_id);
        self.atom_ref_count = self.atom_ref_count.saturating_add(1);
        Ok(())
    }

    fn finish<W: Write>(
        &mut self,
        writer: &mut W,
        target_format: ServingBinaryTargetCopyFormat,
    ) -> io::Result<()> {
        self.flush_membership(writer, target_format)?;
        self.flush_block(writer, target_format)
    }
}

fn write_serving_binary_price_set_atoms_copy_from_rows<W: Write, F>(
    mut next_row: F,
    writer: &mut CountingWriter<W>,
    target_format: ServingBinaryTargetCopyFormat,
    source_copy_format: &str,
) -> io::Result<Value>
where
    F: FnMut() -> io::Result<Option<ServingBinaryPriceSetAtomInputRow>>,
{
    let max_payload_bytes = serving_binary_block_bytes();
    let mut state = ServingBinaryPriceSetAtomState::new(max_payload_bytes);
    write_serving_binary_copy_header(writer, target_format)?;
    while let Some(row) = next_row()? {
        state.push_row(writer, target_format, row)?;
    }
    state.finish(writer, target_format)?;
    write_serving_binary_copy_trailer(writer, target_format)?;
    writer.flush()?;
    Ok(json!({
        "name": "serving_binary_price_set_atoms_by_id_v2",
        "format": PTG2_SERVING_BINARY_PRICE_SET_ATOMS_BY_ID_V2_FORMAT,
        "artifact_kind": PTG2_SERVING_BINARY_PRICE_SET_ATOMS_BY_ID_V2_KIND,
        "id_prefix_bytes": PTG2_SERVING_BINARY_PRICE_SET_ATOM_ID_V2_PREFIX_BYTES,
        "id_bucket_count": PTG2_SERVING_BINARY_PRICE_SET_ATOM_ID_V2_BUCKETS,
        "id_block_count": state.block_count,
        "price_set_count": state.price_set_count,
        "atom_ref_count": state.atom_ref_count,
        "row_count": state.atom_ref_count,
        "copied_records": state.record_count,
        "copy_record_count": state.record_count,
        "byte_count": writer.byte_count(),
        "block_bytes": max_payload_bytes,
        "max_single_membership_bytes": max_payload_bytes,
        "source_copy_format": source_copy_format,
        "target_copy_format": if target_format == ServingBinaryTargetCopyFormat::Binary { "postgres_binary" } else { "text" },
    }))
}

fn write_serving_binary_price_set_atoms_copy_from_reader<R: BufRead, W: Write>(
    reader: &mut R,
    writer: &mut CountingWriter<W>,
    target_format: ServingBinaryTargetCopyFormat,
) -> io::Result<Value> {
    let mut line = Vec::new();
    write_serving_binary_price_set_atoms_copy_from_rows(
        || {
            line.clear();
            if reader.read_until(b'\n', &mut line)? == 0 {
                return Ok(None);
            }
            let fields = serving_copy_fields(&line);
            serving_binary_price_set_atom_row_from_text_fields(&fields).map(Some)
        },
        writer,
        target_format,
        "text",
    )
}

fn write_serving_binary_price_set_atoms_copy_from_pg_binary_reader<R: Read, W: Write>(
    reader: &mut R,
    writer: &mut CountingWriter<W>,
    target_format: ServingBinaryTargetCopyFormat,
) -> io::Result<Value> {
    read_pg_binary_copy_header(reader)?;
    write_serving_binary_price_set_atoms_copy_from_rows(
        || read_pg_binary_price_set_atom_row(reader),
        writer,
        target_format,
        "postgres_binary",
    )
}

fn serving_binary_dictionary_payload(price_set_values: &[[u8; GLOBAL_ID_BYTES]]) -> Vec<u8> {
    let mut payload = Vec::with_capacity(price_set_values.len() * GLOBAL_ID_BYTES);
    for price_set_id in price_set_values {
        payload.extend_from_slice(price_set_id);
    }
    payload
}

fn serving_binary_provider_count_payload(provider_counts: &BTreeMap<i32, u64>) -> Vec<u8> {
    let mut payload = Vec::with_capacity(provider_counts.len() * 2);
    let mut previous_provider_set_key = 0i32;
    for (provider_set_key, provider_count) in provider_counts {
        write_uvarint_to_vec(
            &mut payload,
            (provider_set_key - previous_provider_set_key) as u64,
        );
        write_uvarint_to_vec(&mut payload, *provider_count);
        previous_provider_set_key = *provider_set_key;
    }
    payload
}

fn flush_serving_binary_by_code_block<W: Write>(
    writer: &mut W,
    target_format: ServingBinaryTargetCopyFormat,
    current_code: Option<i32>,
    block_no: usize,
    current_entry_count: &mut usize,
    current_payload: &mut Vec<u8>,
    record_count: &mut u64,
) -> io::Result<()> {
    let Some(code_key) = current_code else {
        return Ok(());
    };
    if *current_entry_count == 0 {
        return Ok(());
    }
    write_serving_binary_copy_record(
        writer,
        target_format,
        PTG2_SERVING_BINARY_BY_CODE_GROUPED_KIND,
        code_key,
        block_no,
        *current_entry_count,
        current_payload,
    )?;
    *record_count = record_count.saturating_add(1);
    current_payload.clear();
    *current_entry_count = 0;
    Ok(())
}

fn encode_serving_by_code_group(
    provider_set_key: i32,
    previous_provider_set_key: i32,
    price_set_keys: &[u32],
) -> io::Result<Vec<u8>> {
    if provider_set_key < previous_provider_set_key {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "serving-binary grouped by-code rows must be ordered by provider_set_key within code_key",
        ));
    }
    let mut encoded = Vec::with_capacity(8 + price_set_keys.len() * 3);
    write_uvarint_to_vec(
        &mut encoded,
        (provider_set_key - previous_provider_set_key) as u64,
    );
    write_uvarint_to_vec(&mut encoded, price_set_keys.len() as u64);
    for price_set_key in price_set_keys {
        write_uvarint_to_vec(&mut encoded, u64::from(*price_set_key));
    }
    Ok(encoded)
}

struct ServingBinaryByCodeGroupState {
    current_code: Option<i32>,
    max_payload_bytes: usize,
    block_no: usize,
    current_entry_count: usize,
    previous_provider_set_key: i32,
    current_payload: Vec<u8>,
    current_provider_set_key: Option<i32>,
    current_price_keys: Vec<u32>,
    record_count: u64,
    group_count: u64,
}

impl ServingBinaryByCodeGroupState {
    fn new(max_payload_bytes: usize) -> Self {
        Self {
            current_code: None,
            max_payload_bytes,
            block_no: 0,
            current_entry_count: 0,
            previous_provider_set_key: 0,
            current_payload: Vec::with_capacity(max_payload_bytes.min(1024 * 1024)),
            current_provider_set_key: None,
            current_price_keys: Vec::new(),
            record_count: 0,
            group_count: 0,
        }
    }
}

fn append_serving_binary_by_code_group<W: Write>(
    writer: &mut W,
    state: &mut ServingBinaryByCodeGroupState,
    target_format: ServingBinaryTargetCopyFormat,
) -> io::Result<()> {
    let Some(provider_set_key) = state.current_provider_set_key else {
        state.current_price_keys.clear();
        return Ok(());
    };
    if state.current_price_keys.is_empty() {
        return Ok(());
    }
    let Some(_code_key) = state.current_code else {
        state.current_price_keys.clear();
        return Ok(());
    };
    let mut encoded = encode_serving_by_code_group(
        provider_set_key,
        state.previous_provider_set_key,
        &state.current_price_keys,
    )?;
    if state.current_entry_count > 0
        && state.current_payload.len() + encoded.len() > state.max_payload_bytes
    {
        flush_serving_binary_by_code_block(
            writer,
            target_format,
            state.current_code,
            state.block_no,
            &mut state.current_entry_count,
            &mut state.current_payload,
            &mut state.record_count,
        )?;
        state.block_no += 1;
        state.previous_provider_set_key = 0;
        encoded = encode_serving_by_code_group(provider_set_key, 0, &state.current_price_keys)?;
    }
    state.current_payload.extend_from_slice(&encoded);
    state.current_entry_count += 1;
    state.previous_provider_set_key = provider_set_key;
    state.group_count = state.group_count.saturating_add(1);
    state.current_price_keys.clear();
    Ok(())
}

fn write_serving_binary_by_code_copy_from_reader<R: BufRead, W: Write>(
    reader: &mut R,
    writer: &mut CountingWriter<W>,
    output_path: Option<&Path>,
    target_format: ServingBinaryTargetCopyFormat,
) -> io::Result<Value> {
    let max_payload_bytes = serving_binary_block_bytes();
    let mut price_set_to_key: HashMap<[u8; GLOBAL_ID_BYTES], u32> = HashMap::new();
    let mut price_set_values: Vec<[u8; GLOBAL_ID_BYTES]> = Vec::new();
    let mut provider_count_by_key: BTreeMap<i32, u64> = BTreeMap::new();
    let mut row_count = 0u64;
    let mut code_count = 0u64;
    let mut group_state = ServingBinaryByCodeGroupState::new(max_payload_bytes);

    write_serving_binary_copy_header(writer, target_format)?;
    let mut line = Vec::new();
    loop {
        line.clear();
        if reader.read_until(b'\n', &mut line)? == 0 {
            break;
        }
        let fields = serving_copy_fields(&line);
        let row = serving_binary_row_from_text_fields(&fields, "by-code")?;
        let code_key = row.first_key;
        let provider_set_key = row.second_key;
        let provider_count = row.provider_count;
        let price_set_id = row.price_set_id;
        if let Some(existing_provider_count) = provider_count_by_key.get(&provider_set_key) {
            if *existing_provider_count != provider_count {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "provider_count changed for provider_set_key {provider_set_key}: {existing_provider_count} != {provider_count}"
                    ),
                ));
            }
        } else {
            provider_count_by_key.insert(provider_set_key, provider_count);
        }
        if group_state.current_code != Some(code_key) {
            append_serving_binary_by_code_group(writer, &mut group_state, target_format)?;
            flush_serving_binary_by_code_block(
                writer,
                target_format,
                group_state.current_code,
                group_state.block_no,
                &mut group_state.current_entry_count,
                &mut group_state.current_payload,
                &mut group_state.record_count,
            )?;
            group_state.current_code = Some(code_key);
            group_state.previous_provider_set_key = 0;
            group_state.block_no = 0;
            code_count = code_count.saturating_add(1);
        } else if group_state.current_provider_set_key != Some(provider_set_key) {
            append_serving_binary_by_code_group(writer, &mut group_state, target_format)?;
        }
        let price_set_key =
            serving_price_key(price_set_id, &mut price_set_to_key, &mut price_set_values)?;
        group_state.current_provider_set_key = Some(provider_set_key);
        group_state.current_price_keys.push(price_set_key);
        row_count = row_count.saturating_add(1);
    }
    append_serving_binary_by_code_group(writer, &mut group_state, target_format)?;
    flush_serving_binary_by_code_block(
        writer,
        target_format,
        group_state.current_code,
        group_state.block_no,
        &mut group_state.current_entry_count,
        &mut group_state.current_payload,
        &mut group_state.record_count,
    )?;
    let dictionary_payload = serving_binary_dictionary_payload(&price_set_values);
    write_serving_binary_copy_record(
        writer,
        target_format,
        PTG2_SERVING_BINARY_BY_CODE_DICTIONARY_KIND,
        0,
        0,
        price_set_values.len(),
        &dictionary_payload,
    )?;
    group_state.record_count = group_state.record_count.saturating_add(1);
    let provider_count_payload = serving_binary_provider_count_payload(&provider_count_by_key);
    write_serving_binary_copy_record(
        writer,
        target_format,
        PTG2_SERVING_BINARY_PROVIDER_COUNT_DICTIONARY_KIND,
        0,
        0,
        provider_count_by_key.len(),
        &provider_count_payload,
    )?;
    group_state.record_count = group_state.record_count.saturating_add(1);
    write_serving_binary_copy_trailer(writer, target_format)?;
    writer.flush()?;
    let mut payload = json!({
        "name": "serving_binary_by_code",
        "format": PTG2_SERVING_BINARY_TABLE_FORMAT,
        "kind": PTG2_SERVING_BINARY_BY_CODE_GROUPED_KIND,
        "row_count": row_count,
        "group_count": group_state.group_count,
        "code_count": code_count,
        "block_count": group_state.record_count.saturating_sub(2),
        "price_set_count": price_set_values.len(),
        "provider_set_count": provider_count_by_key.len(),
        "copy_record_count": group_state.record_count,
        "byte_count": writer.byte_count(),
        "block_bytes": max_payload_bytes,
        "target_copy_format": if target_format == ServingBinaryTargetCopyFormat::Binary { "postgres_binary" } else { "text" },
    });
    if let Some(path) = output_path {
        payload["path"] = json!(path.display().to_string());
    }
    Ok(payload)
}

fn write_serving_binary_by_code_copy_from_pg_binary_reader<R: Read, W: Write>(
    reader: &mut R,
    writer: &mut CountingWriter<W>,
    output_path: Option<&Path>,
    target_format: ServingBinaryTargetCopyFormat,
) -> io::Result<Value> {
    let max_payload_bytes = serving_binary_block_bytes();
    let mut price_set_to_key: HashMap<[u8; GLOBAL_ID_BYTES], u32> = HashMap::new();
    let mut price_set_values: Vec<[u8; GLOBAL_ID_BYTES]> = Vec::new();
    let mut provider_count_by_key: BTreeMap<i32, u64> = BTreeMap::new();
    let mut row_count = 0u64;
    let mut code_count = 0u64;
    let mut group_state = ServingBinaryByCodeGroupState::new(max_payload_bytes);

    write_serving_binary_copy_header(writer, target_format)?;
    read_pg_binary_copy_header(reader)?;
    while let Some(row) = read_pg_binary_serving_row(reader)? {
        let code_key = row.first_key;
        let provider_set_key = row.second_key;
        let provider_count = row.provider_count;
        let price_set_id = row.price_set_id;
        if let Some(existing_provider_count) = provider_count_by_key.get(&provider_set_key) {
            if *existing_provider_count != provider_count {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "provider_count changed for provider_set_key {provider_set_key}: {existing_provider_count} != {provider_count}"
                    ),
                ));
            }
        } else {
            provider_count_by_key.insert(provider_set_key, provider_count);
        }
        if group_state.current_code != Some(code_key) {
            append_serving_binary_by_code_group(writer, &mut group_state, target_format)?;
            flush_serving_binary_by_code_block(
                writer,
                target_format,
                group_state.current_code,
                group_state.block_no,
                &mut group_state.current_entry_count,
                &mut group_state.current_payload,
                &mut group_state.record_count,
            )?;
            group_state.current_code = Some(code_key);
            group_state.previous_provider_set_key = 0;
            group_state.block_no = 0;
            code_count = code_count.saturating_add(1);
        } else if group_state.current_provider_set_key != Some(provider_set_key) {
            append_serving_binary_by_code_group(writer, &mut group_state, target_format)?;
        }
        let price_set_key =
            serving_price_key(price_set_id, &mut price_set_to_key, &mut price_set_values)?;
        group_state.current_provider_set_key = Some(provider_set_key);
        group_state.current_price_keys.push(price_set_key);
        row_count = row_count.saturating_add(1);
    }
    append_serving_binary_by_code_group(writer, &mut group_state, target_format)?;
    flush_serving_binary_by_code_block(
        writer,
        target_format,
        group_state.current_code,
        group_state.block_no,
        &mut group_state.current_entry_count,
        &mut group_state.current_payload,
        &mut group_state.record_count,
    )?;
    let dictionary_payload = serving_binary_dictionary_payload(&price_set_values);
    write_serving_binary_copy_record(
        writer,
        target_format,
        PTG2_SERVING_BINARY_BY_CODE_DICTIONARY_KIND,
        0,
        0,
        price_set_values.len(),
        &dictionary_payload,
    )?;
    group_state.record_count = group_state.record_count.saturating_add(1);
    let provider_count_payload = serving_binary_provider_count_payload(&provider_count_by_key);
    write_serving_binary_copy_record(
        writer,
        target_format,
        PTG2_SERVING_BINARY_PROVIDER_COUNT_DICTIONARY_KIND,
        0,
        0,
        provider_count_by_key.len(),
        &provider_count_payload,
    )?;
    group_state.record_count = group_state.record_count.saturating_add(1);
    write_serving_binary_copy_trailer(writer, target_format)?;
    writer.flush()?;
    let mut payload = json!({
        "name": "serving_binary_by_code",
        "format": PTG2_SERVING_BINARY_TABLE_FORMAT,
        "kind": PTG2_SERVING_BINARY_BY_CODE_GROUPED_KIND,
        "row_count": row_count,
        "group_count": group_state.group_count,
        "code_count": code_count,
        "block_count": group_state.record_count.saturating_sub(2),
        "price_set_count": price_set_values.len(),
        "provider_set_count": provider_count_by_key.len(),
        "copy_record_count": group_state.record_count,
        "byte_count": writer.byte_count(),
        "block_bytes": max_payload_bytes,
        "source_copy_format": "postgres_binary",
        "target_copy_format": if target_format == ServingBinaryTargetCopyFormat::Binary { "postgres_binary" } else { "text" },
    });
    if let Some(path) = output_path {
        payload["path"] = json!(path.display().to_string());
    }
    Ok(payload)
}

fn write_serving_binary_by_code_copy_from_key_copy(
    input_path: &Path,
    output_path: &Path,
) -> io::Result<Value> {
    if let Some(parent) = output_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut reader = BufReader::new(File::open(input_path).map_err(|error| {
        io::Error::new(
            error.kind(),
            format!(
                "failed to open serving-binary by-code input COPY file {}: {error}",
                input_path.display()
            ),
        )
    })?);
    let output = File::create(output_path).map_err(|error| {
        io::Error::new(
            error.kind(),
            format!(
                "failed to create serving-binary by-code COPY file {}: {error}",
                output_path.display()
            ),
        )
    })?;
    let mut writer = CountingWriter::new(BufWriter::new(output));
    write_serving_binary_by_code_copy_from_reader(
        &mut reader,
        &mut writer,
        Some(output_path),
        ServingBinaryTargetCopyFormat::Text,
    )
}

fn encode_serving_provider_pattern(
    entries: &ServingProviderEntries,
    code_keys: &[i32],
) -> io::Result<Vec<u8>> {
    let mut encoded = Vec::with_capacity(32 + code_keys.len() * 3 + entries.len() * 6);
    write_uvarint_to_vec(&mut encoded, code_keys.len() as u64);
    let mut previous_code_key = 0i32;
    for (index, code_key) in code_keys.iter().copied().enumerate() {
        let delta = if index == 0 {
            code_key
        } else {
            code_key - previous_code_key
        };
        if delta < 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "serving-binary by-provider-set code keys must be nondecreasing",
            ));
        }
        write_uvarint_to_vec(&mut encoded, delta as u64);
        previous_code_key = code_key;
    }
    write_uvarint_to_vec(&mut encoded, entries.len() as u64);
    for (provider_count, price_set_key) in entries {
        write_uvarint_to_vec(&mut encoded, *provider_count);
        write_uvarint_to_vec(&mut encoded, u64::from(*price_set_key));
    }
    Ok(encoded)
}

fn flush_serving_binary_provider_blocks<W: Write>(
    writer: &mut W,
    target_format: ServingBinaryTargetCopyFormat,
    provider_set_key: i32,
    max_payload_bytes: usize,
    current_patterns: &mut ServingProviderPatternMap,
    block_stats: &mut ServingProviderBlockStats,
) -> io::Result<()> {
    let mut ordered_patterns: Vec<ServingProviderPattern> = current_patterns.drain().collect();
    ordered_patterns.sort_unstable_by(|left, right| {
        let left_first = left.1.iter().min().copied().unwrap_or(-1);
        let right_first = right.1.iter().min().copied().unwrap_or(-1);
        left_first
            .cmp(&right_first)
            .then_with(|| left.0.cmp(&right.0))
    });
    let mut block_payload = Vec::with_capacity(max_payload_bytes.min(1024 * 1024));
    let mut block_pattern_count = 0usize;
    let mut block_no = 0usize;
    for (entries, mut code_keys) in ordered_patterns {
        code_keys.sort_unstable();
        code_keys.dedup();
        let encoded = encode_serving_provider_pattern(&entries, &code_keys)?;
        if block_pattern_count > 0 && block_payload.len() + encoded.len() > max_payload_bytes {
            write_serving_binary_copy_record(
                writer,
                target_format,
                PTG2_SERVING_BINARY_BY_PROVIDER_SET_KIND,
                provider_set_key,
                block_no,
                block_pattern_count,
                &block_payload,
            )?;
            block_stats.record_count = block_stats.record_count.saturating_add(1);
            block_stats.block_count = block_stats.block_count.saturating_add(1);
            block_stats.pattern_count = block_stats
                .pattern_count
                .saturating_add(block_pattern_count as u64);
            block_payload.clear();
            block_pattern_count = 0;
            block_no += 1;
        }
        block_payload.extend_from_slice(&encoded);
        block_pattern_count += 1;
    }
    if block_pattern_count > 0 {
        write_serving_binary_copy_record(
            writer,
            target_format,
            PTG2_SERVING_BINARY_BY_PROVIDER_SET_KIND,
            provider_set_key,
            block_no,
            block_pattern_count,
            &block_payload,
        )?;
        block_stats.record_count = block_stats.record_count.saturating_add(1);
        block_stats.block_count = block_stats.block_count.saturating_add(1);
        block_stats.pattern_count = block_stats
            .pattern_count
            .saturating_add(block_pattern_count as u64);
    }
    Ok(())
}

fn write_serving_binary_by_provider_set_copy_from_reader<R: BufRead, W: Write>(
    reader: &mut R,
    writer: &mut CountingWriter<W>,
    output_path: Option<&Path>,
    target_format: ServingBinaryTargetCopyFormat,
) -> io::Result<Value> {
    let max_payload_bytes = serving_binary_block_bytes();
    let mut price_set_to_key: HashMap<[u8; GLOBAL_ID_BYTES], u32> = HashMap::new();
    let mut price_set_values: Vec<[u8; GLOBAL_ID_BYTES]> = Vec::new();
    let mut code_keys_seen: HashSet<i32> = HashSet::new();
    let mut row_count = 0u64;
    let mut block_stats = ServingProviderBlockStats::default();
    let mut provider_set_count = 0u64;
    let mut current_provider_set: Option<i32> = None;
    let mut current_code: Option<i32> = None;
    let mut current_code_entries: ServingProviderEntries = Vec::new();
    let mut current_patterns: ServingProviderPatternMap = HashMap::new();

    write_serving_binary_copy_header(writer, target_format)?;
    let mut line = Vec::new();
    loop {
        line.clear();
        if reader.read_until(b'\n', &mut line)? == 0 {
            break;
        }
        let fields = serving_copy_fields(&line);
        let row = serving_binary_row_from_text_fields(&fields, "by-provider-set")?;
        let provider_set_key = row.first_key;
        let code_key = row.second_key;
        let provider_count = row.provider_count;
        let price_set_id = row.price_set_id;
        let price_set_key =
            serving_price_key(price_set_id, &mut price_set_to_key, &mut price_set_values)?;
        if current_provider_set != Some(provider_set_key) {
            if let Some(previous_provider_set) = current_provider_set {
                flush_serving_provider_code(
                    current_code,
                    &mut current_code_entries,
                    &mut current_patterns,
                );
                flush_serving_binary_provider_blocks(
                    writer,
                    target_format,
                    previous_provider_set,
                    max_payload_bytes,
                    &mut current_patterns,
                    &mut block_stats,
                )?;
            }
            current_provider_set = Some(provider_set_key);
            current_code = None;
            provider_set_count = provider_set_count.saturating_add(1);
        }
        if current_code != Some(code_key) {
            flush_serving_provider_code(
                current_code,
                &mut current_code_entries,
                &mut current_patterns,
            );
            current_code = Some(code_key);
        }
        current_code_entries.push((provider_count, price_set_key));
        code_keys_seen.insert(code_key);
        row_count = row_count.saturating_add(1);
    }
    if let Some(provider_set_key) = current_provider_set {
        flush_serving_provider_code(
            current_code,
            &mut current_code_entries,
            &mut current_patterns,
        );
        flush_serving_binary_provider_blocks(
            writer,
            target_format,
            provider_set_key,
            max_payload_bytes,
            &mut current_patterns,
            &mut block_stats,
        )?;
    }
    let dictionary_payload = serving_binary_dictionary_payload(&price_set_values);
    write_serving_binary_copy_record(
        writer,
        target_format,
        PTG2_SERVING_BINARY_BY_PROVIDER_SET_DICTIONARY_KIND,
        0,
        0,
        price_set_values.len(),
        &dictionary_payload,
    )?;
    block_stats.record_count = block_stats.record_count.saturating_add(1);
    write_serving_binary_copy_trailer(writer, target_format)?;
    writer.flush()?;
    let mut payload = json!({
        "name": "serving_binary_by_provider_set",
        "format": PTG2_SERVING_BINARY_TABLE_FORMAT,
        "kind": PTG2_SERVING_BINARY_BY_PROVIDER_SET_KIND,
        "row_count": row_count,
        "provider_set_count": provider_set_count,
        "code_count": code_keys_seen.len(),
        "block_count": block_stats.block_count,
        "pattern_count": block_stats.pattern_count,
        "price_set_count": price_set_values.len(),
        "copy_record_count": block_stats.record_count,
        "byte_count": writer.byte_count(),
        "block_bytes": max_payload_bytes,
        "target_copy_format": if target_format == ServingBinaryTargetCopyFormat::Binary { "postgres_binary" } else { "text" },
    });
    if let Some(path) = output_path {
        payload["path"] = json!(path.display().to_string());
    }
    Ok(payload)
}

fn write_serving_binary_by_provider_set_copy_from_pg_binary_reader<R: Read, W: Write>(
    reader: &mut R,
    writer: &mut CountingWriter<W>,
    output_path: Option<&Path>,
    target_format: ServingBinaryTargetCopyFormat,
) -> io::Result<Value> {
    let max_payload_bytes = serving_binary_block_bytes();
    let mut price_set_to_key: HashMap<[u8; GLOBAL_ID_BYTES], u32> = HashMap::new();
    let mut price_set_values: Vec<[u8; GLOBAL_ID_BYTES]> = Vec::new();
    let mut code_keys_seen: HashSet<i32> = HashSet::new();
    let mut row_count = 0u64;
    let mut block_stats = ServingProviderBlockStats::default();
    let mut provider_set_count = 0u64;
    let mut current_provider_set: Option<i32> = None;
    let mut current_code: Option<i32> = None;
    let mut current_code_entries: ServingProviderEntries = Vec::new();
    let mut current_patterns: ServingProviderPatternMap = HashMap::new();

    write_serving_binary_copy_header(writer, target_format)?;
    read_pg_binary_copy_header(reader)?;
    while let Some(row) = read_pg_binary_serving_row(reader)? {
        let provider_set_key = row.first_key;
        let code_key = row.second_key;
        let provider_count = row.provider_count;
        let price_set_id = row.price_set_id;
        let price_set_key =
            serving_price_key(price_set_id, &mut price_set_to_key, &mut price_set_values)?;
        if current_provider_set != Some(provider_set_key) {
            if let Some(previous_provider_set) = current_provider_set {
                flush_serving_provider_code(
                    current_code,
                    &mut current_code_entries,
                    &mut current_patterns,
                );
                flush_serving_binary_provider_blocks(
                    writer,
                    target_format,
                    previous_provider_set,
                    max_payload_bytes,
                    &mut current_patterns,
                    &mut block_stats,
                )?;
            }
            current_provider_set = Some(provider_set_key);
            current_code = None;
            provider_set_count = provider_set_count.saturating_add(1);
        }
        if current_code != Some(code_key) {
            flush_serving_provider_code(
                current_code,
                &mut current_code_entries,
                &mut current_patterns,
            );
            current_code = Some(code_key);
        }
        current_code_entries.push((provider_count, price_set_key));
        code_keys_seen.insert(code_key);
        row_count = row_count.saturating_add(1);
    }
    if let Some(provider_set_key) = current_provider_set {
        flush_serving_provider_code(
            current_code,
            &mut current_code_entries,
            &mut current_patterns,
        );
        flush_serving_binary_provider_blocks(
            writer,
            target_format,
            provider_set_key,
            max_payload_bytes,
            &mut current_patterns,
            &mut block_stats,
        )?;
    }
    let dictionary_payload = serving_binary_dictionary_payload(&price_set_values);
    write_serving_binary_copy_record(
        writer,
        target_format,
        PTG2_SERVING_BINARY_BY_PROVIDER_SET_DICTIONARY_KIND,
        0,
        0,
        price_set_values.len(),
        &dictionary_payload,
    )?;
    block_stats.record_count = block_stats.record_count.saturating_add(1);
    write_serving_binary_copy_trailer(writer, target_format)?;
    writer.flush()?;
    let mut payload = json!({
        "name": "serving_binary_by_provider_set",
        "format": PTG2_SERVING_BINARY_TABLE_FORMAT,
        "kind": PTG2_SERVING_BINARY_BY_PROVIDER_SET_KIND,
        "row_count": row_count,
        "provider_set_count": provider_set_count,
        "code_count": code_keys_seen.len(),
        "block_count": block_stats.block_count,
        "pattern_count": block_stats.pattern_count,
        "price_set_count": price_set_values.len(),
        "copy_record_count": block_stats.record_count,
        "byte_count": writer.byte_count(),
        "block_bytes": max_payload_bytes,
        "source_copy_format": "postgres_binary",
        "target_copy_format": if target_format == ServingBinaryTargetCopyFormat::Binary { "postgres_binary" } else { "text" },
    });
    if let Some(path) = output_path {
        payload["path"] = json!(path.display().to_string());
    }
    Ok(payload)
}

fn write_serving_binary_by_provider_set_copy_from_key_copy(
    input_path: &Path,
    output_path: &Path,
) -> io::Result<Value> {
    if let Some(parent) = output_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut reader = BufReader::new(File::open(input_path).map_err(|error| {
        io::Error::new(
            error.kind(),
            format!(
                "failed to open serving-binary by-provider-set input COPY file {}: {error}",
                input_path.display()
            ),
        )
    })?);
    let output = File::create(output_path).map_err(|error| {
        io::Error::new(
            error.kind(),
            format!(
                "failed to create serving-binary by-provider-set COPY file {}: {error}",
                output_path.display()
            ),
        )
    })?;
    let mut writer = CountingWriter::new(BufWriter::new(output));
    write_serving_binary_by_provider_set_copy_from_reader(
        &mut reader,
        &mut writer,
        Some(output_path),
        ServingBinaryTargetCopyFormat::Text,
    )
}

fn flush_reverse_serving_group(
    reverse_patterns_by_provider: &mut BTreeMap<i32, ServingProviderPatternMap>,
    current_provider_set: Option<i32>,
    current_code: Option<i32>,
    current_entries: &mut ServingProviderEntries,
) {
    let (Some(provider_set_key), Some(code_key)) = (current_provider_set, current_code) else {
        current_entries.clear();
        return;
    };
    if current_entries.is_empty() {
        return;
    }
    let entries = std::mem::take(current_entries);
    reverse_patterns_by_provider
        .entry(provider_set_key)
        .or_default()
        .entry(entries)
        .or_default()
        .push(code_key);
}

fn write_serving_binary_combined_copy_from_rows<W: Write, F>(
    mut next_row: F,
    writer: &mut CountingWriter<W>,
    output_path: Option<&Path>,
    target_format: ServingBinaryTargetCopyFormat,
    source_copy_format: Option<&str>,
) -> io::Result<Value>
where
    F: FnMut() -> io::Result<Option<ServingBinaryInputRow>>,
{
    let max_payload_bytes = serving_binary_block_bytes();
    let mut by_code_price_set_to_key: HashMap<[u8; GLOBAL_ID_BYTES], u32> = HashMap::new();
    let mut by_code_price_set_values: Vec<[u8; GLOBAL_ID_BYTES]> = Vec::new();
    let mut by_code_provider_count_by_key: BTreeMap<i32, u64> = BTreeMap::new();
    let mut by_code_state = ServingBinaryByCodeGroupState::new(max_payload_bytes);
    let mut by_code_count = 0u64;

    let mut reverse_price_set_to_key: HashMap<[u8; GLOBAL_ID_BYTES], u32> = HashMap::new();
    let mut reverse_price_set_values: Vec<[u8; GLOBAL_ID_BYTES]> = Vec::new();
    let mut reverse_code_keys_seen: HashSet<i32> = HashSet::new();
    let mut reverse_patterns_by_provider: BTreeMap<i32, ServingProviderPatternMap> =
        BTreeMap::new();
    let mut reverse_current_provider_set: Option<i32> = None;
    let mut reverse_current_code: Option<i32> = None;
    let mut reverse_current_entries: ServingProviderEntries = Vec::new();

    write_serving_binary_copy_header(writer, target_format)?;
    let mut row_count = 0u64;
    while let Some(row) = next_row()? {
        let code_key = row.first_key;
        let provider_set_key = row.second_key;
        let provider_count = row.provider_count;
        let price_set_id = row.price_set_id;

        if let Some(existing_provider_count) = by_code_provider_count_by_key.get(&provider_set_key)
        {
            if *existing_provider_count != provider_count {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "provider_count changed for provider_set_key {provider_set_key}: {existing_provider_count} != {provider_count}"
                    ),
                ));
            }
        } else {
            by_code_provider_count_by_key.insert(provider_set_key, provider_count);
        }

        if by_code_state.current_code != Some(code_key) {
            append_serving_binary_by_code_group(writer, &mut by_code_state, target_format)?;
            flush_serving_binary_by_code_block(
                writer,
                target_format,
                by_code_state.current_code,
                by_code_state.block_no,
                &mut by_code_state.current_entry_count,
                &mut by_code_state.current_payload,
                &mut by_code_state.record_count,
            )?;
            by_code_state.current_code = Some(code_key);
            by_code_state.previous_provider_set_key = 0;
            by_code_state.block_no = 0;
            by_code_count = by_code_count.saturating_add(1);
        } else if by_code_state.current_provider_set_key != Some(provider_set_key) {
            append_serving_binary_by_code_group(writer, &mut by_code_state, target_format)?;
        }
        let by_code_price_set_key = serving_price_key(
            price_set_id,
            &mut by_code_price_set_to_key,
            &mut by_code_price_set_values,
        )?;
        by_code_state.current_provider_set_key = Some(provider_set_key);
        by_code_state.current_price_keys.push(by_code_price_set_key);

        if reverse_current_provider_set != Some(provider_set_key)
            || reverse_current_code != Some(code_key)
        {
            flush_reverse_serving_group(
                &mut reverse_patterns_by_provider,
                reverse_current_provider_set,
                reverse_current_code,
                &mut reverse_current_entries,
            );
            reverse_current_provider_set = Some(provider_set_key);
            reverse_current_code = Some(code_key);
        }
        let reverse_price_set_key = serving_price_key(
            price_set_id,
            &mut reverse_price_set_to_key,
            &mut reverse_price_set_values,
        )?;
        reverse_current_entries.push((provider_count, reverse_price_set_key));
        reverse_code_keys_seen.insert(code_key);
        row_count = row_count.saturating_add(1);
    }

    append_serving_binary_by_code_group(writer, &mut by_code_state, target_format)?;
    flush_serving_binary_by_code_block(
        writer,
        target_format,
        by_code_state.current_code,
        by_code_state.block_no,
        &mut by_code_state.current_entry_count,
        &mut by_code_state.current_payload,
        &mut by_code_state.record_count,
    )?;
    let by_code_dictionary_payload = serving_binary_dictionary_payload(&by_code_price_set_values);
    write_serving_binary_copy_record(
        writer,
        target_format,
        PTG2_SERVING_BINARY_BY_CODE_DICTIONARY_KIND,
        0,
        0,
        by_code_price_set_values.len(),
        &by_code_dictionary_payload,
    )?;
    by_code_state.record_count = by_code_state.record_count.saturating_add(1);
    let provider_count_payload =
        serving_binary_provider_count_payload(&by_code_provider_count_by_key);
    write_serving_binary_copy_record(
        writer,
        target_format,
        PTG2_SERVING_BINARY_PROVIDER_COUNT_DICTIONARY_KIND,
        0,
        0,
        by_code_provider_count_by_key.len(),
        &provider_count_payload,
    )?;
    by_code_state.record_count = by_code_state.record_count.saturating_add(1);

    flush_reverse_serving_group(
        &mut reverse_patterns_by_provider,
        reverse_current_provider_set,
        reverse_current_code,
        &mut reverse_current_entries,
    );
    let mut reverse_block_stats = ServingProviderBlockStats::default();
    let reverse_provider_set_count = reverse_patterns_by_provider.len();
    for (provider_set_key, mut patterns) in reverse_patterns_by_provider {
        flush_serving_binary_provider_blocks(
            writer,
            target_format,
            provider_set_key,
            max_payload_bytes,
            &mut patterns,
            &mut reverse_block_stats,
        )?;
    }
    let reverse_dictionary_payload = serving_binary_dictionary_payload(&reverse_price_set_values);
    write_serving_binary_copy_record(
        writer,
        target_format,
        PTG2_SERVING_BINARY_BY_PROVIDER_SET_DICTIONARY_KIND,
        0,
        0,
        reverse_price_set_values.len(),
        &reverse_dictionary_payload,
    )?;
    reverse_block_stats.record_count = reverse_block_stats.record_count.saturating_add(1);
    write_serving_binary_copy_trailer(writer, target_format)?;
    writer.flush()?;

    let by_code_payload = json!({
        "name": "serving_binary_by_code",
        "format": PTG2_SERVING_BINARY_TABLE_FORMAT,
        "kind": PTG2_SERVING_BINARY_BY_CODE_GROUPED_KIND,
        "row_count": row_count,
        "group_count": by_code_state.group_count,
        "code_count": by_code_count,
        "block_count": by_code_state.record_count.saturating_sub(2),
        "price_set_count": by_code_price_set_values.len(),
        "provider_set_count": by_code_provider_count_by_key.len(),
        "copy_record_count": by_code_state.record_count,
        "block_bytes": max_payload_bytes,
    });
    let by_provider_set_payload = json!({
        "name": "serving_binary_by_provider_set",
        "format": PTG2_SERVING_BINARY_TABLE_FORMAT,
        "kind": PTG2_SERVING_BINARY_BY_PROVIDER_SET_KIND,
        "row_count": row_count,
        "provider_set_count": reverse_provider_set_count,
        "code_count": reverse_code_keys_seen.len(),
        "block_count": reverse_block_stats.block_count,
        "pattern_count": reverse_block_stats.pattern_count,
        "price_set_count": reverse_price_set_values.len(),
        "copy_record_count": reverse_block_stats.record_count,
        "block_bytes": max_payload_bytes,
    });
    let mut payload = json!({
        "name": "serving_binary_combined",
        "format": PTG2_SERVING_BINARY_TABLE_FORMAT,
        "row_count": row_count,
        "copy_record_count": by_code_state.record_count.saturating_add(reverse_block_stats.record_count),
        "byte_count": writer.byte_count(),
        "block_bytes": max_payload_bytes,
        "target_copy_format": if target_format == ServingBinaryTargetCopyFormat::Binary { "postgres_binary" } else { "text" },
        "by_code": by_code_payload,
        "by_provider_set": by_provider_set_payload,
    });
    if let Some(source_copy_format) = source_copy_format {
        payload["source_copy_format"] = json!(source_copy_format);
    }
    if let Some(path) = output_path {
        payload["path"] = json!(path.display().to_string());
    }
    Ok(payload)
}

fn write_serving_binary_combined_copy_from_reader<R: BufRead, W: Write>(
    reader: &mut R,
    writer: &mut CountingWriter<W>,
    output_path: Option<&Path>,
    target_format: ServingBinaryTargetCopyFormat,
) -> io::Result<Value> {
    let mut line = Vec::new();
    write_serving_binary_combined_copy_from_rows(
        || {
            line.clear();
            if reader.read_until(b'\n', &mut line)? == 0 {
                return Ok(None);
            }
            let fields = serving_copy_fields(&line);
            serving_binary_row_from_text_fields(&fields, "combined").map(Some)
        },
        writer,
        output_path,
        target_format,
        None,
    )
}

fn write_serving_binary_combined_copy_from_pg_binary_reader<R: Read, W: Write>(
    reader: &mut R,
    writer: &mut CountingWriter<W>,
    output_path: Option<&Path>,
    target_format: ServingBinaryTargetCopyFormat,
) -> io::Result<Value> {
    read_pg_binary_copy_header(reader)?;
    write_serving_binary_combined_copy_from_rows(
        || read_pg_binary_serving_row(reader),
        writer,
        output_path,
        target_format,
        Some("postgres_binary"),
    )
}

fn write_serving_binary_copy_from_key_copy(
    kind: &str,
    input_path: &Path,
    output_path: &Path,
) -> io::Result<()> {
    let payload = match kind {
        PTG2_SERVING_BINARY_BY_CODE_KIND => {
            write_serving_binary_by_code_copy_from_key_copy(input_path, output_path)?
        }
        PTG2_SERVING_BINARY_BY_PROVIDER_SET_KIND => {
            write_serving_binary_by_provider_set_copy_from_key_copy(input_path, output_path)?
        }
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "serving binary kind must be by_code or by_provider_set",
            ));
        }
    };
    emit_json_record(
        &mut io::stdout().lock(),
        "serving_binary_copy_file",
        &payload,
    )
}

fn write_serving_binary_copy_from_key_copy_stdio(kind: &str) -> io::Result<()> {
    let stdin = io::stdin();
    let stdout = io::stdout();
    let mut reader = BufReader::new(stdin.lock());
    let mut writer = CountingWriter::new(BufWriter::new(stdout.lock()));
    let target_format = serving_binary_target_copy_format();
    let payload = match kind {
        PTG2_SERVING_BINARY_BY_CODE_KIND => write_serving_binary_by_code_copy_from_reader(
            &mut reader,
            &mut writer,
            None,
            target_format,
        )?,
        "by_code_pg_binary" => write_serving_binary_by_code_copy_from_pg_binary_reader(
            &mut reader,
            &mut writer,
            None,
            target_format,
        )?,
        PTG2_SERVING_BINARY_BY_PROVIDER_SET_KIND => {
            write_serving_binary_by_provider_set_copy_from_reader(
                &mut reader,
                &mut writer,
                None,
                target_format,
            )?
        }
        "by_provider_set_pg_binary" => {
            write_serving_binary_by_provider_set_copy_from_pg_binary_reader(
                &mut reader,
                &mut writer,
                None,
                target_format,
            )?
        }
        "combined" => write_serving_binary_combined_copy_from_reader(
            &mut reader,
            &mut writer,
            None,
            target_format,
        )?,
        "combined_pg_binary" => write_serving_binary_combined_copy_from_pg_binary_reader(
            &mut reader,
            &mut writer,
            None,
            target_format,
        )?,
        PTG2_SERVING_BINARY_PRICE_SET_ATOMS_BY_ID_V2_KIND => {
            write_serving_binary_price_set_atoms_copy_from_reader(
                &mut reader,
                &mut writer,
                target_format,
            )?
        }
        "price_set_atoms_by_id_v2_pg_binary" => {
            write_serving_binary_price_set_atoms_copy_from_pg_binary_reader(
                &mut reader,
                &mut writer,
                target_format,
            )?
        }
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "serving binary kind must be by_code, by_code_pg_binary, by_provider_set, by_provider_set_pg_binary, combined, combined_pg_binary, price_set_atoms_by_id_v2, or price_set_atoms_by_id_v2_pg_binary",
            ));
        }
    };
    writeln!(io::stderr().lock(), "PTG2_SERVING_BINARY_COPY\t{}", payload)
}

fn copy_file_into_writer<W: Write>(path: &Path, writer: &mut W) -> io::Result<()> {
    let mut reader = BufReader::new(File::open(path).map_err(|error| {
        io::Error::new(
            error.kind(),
            format!(
                "failed to open {} for sidecar copy: {error}",
                path.display()
            ),
        )
    })?);
    let mut buffer = [0u8; 1024 * 1024];
    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        writer.write_all(&buffer[..read])?;
    }
    Ok(())
}

fn write_serving_sidecar_final(
    output_path: &Path,
    body_path: &Path,
    magic: &[u8; 8],
    metadata: &Value,
    price_set_values: &[[u8; GLOBAL_ID_BYTES]],
    blocks: &[ServingBlock],
) -> io::Result<u64> {
    let tmp_path = output_path.with_extension(format!(
        "{}.tmp",
        output_path
            .extension()
            .and_then(|value| value.to_str())
            .unwrap_or("sidecar")
    ));
    let mut writer = BufWriter::new(File::create(&tmp_path).map_err(|error| {
        io::Error::new(
            error.kind(),
            format!(
                "failed to create serving sidecar temp file {}: {error}",
                tmp_path.display()
            ),
        )
    })?);
    let header = serde_json::to_vec(metadata).map_err(to_io_error)?;
    writer.write_all(magic)?;
    writer.write_all(&(header.len() as u32).to_le_bytes())?;
    writer.write_all(&header)?;
    for price_set_id in price_set_values {
        writer.write_all(price_set_id)?;
    }
    for block in blocks {
        writer.write_all(&block.key.to_le_bytes())?;
        writer.write_all(&block.offset.to_le_bytes())?;
        writer.write_all(&block.count.to_le_bytes())?;
    }
    copy_file_into_writer(body_path, &mut writer)?;
    writer.flush()?;
    drop(writer);
    std::fs::rename(&tmp_path, output_path)?;
    let bytes = output_path.metadata()?.len();
    Ok(bytes)
}

fn write_serving_by_code_from_copy(input_path: &Path, output_path: &Path) -> io::Result<Value> {
    if let Some(parent) = output_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let body_path = output_path.with_extension("body.tmp");
    let mut body = BufWriter::new(File::create(&body_path).map_err(|error| {
        io::Error::new(
            error.kind(),
            format!(
                "failed to create serving-by-code body file {}: {error}",
                body_path.display()
            ),
        )
    })?);
    let mut price_set_to_key: HashMap<[u8; GLOBAL_ID_BYTES], u32> = HashMap::new();
    let mut price_set_values: Vec<[u8; GLOBAL_ID_BYTES]> = Vec::new();
    let mut blocks: Vec<ServingBlock> = Vec::new();
    let mut row_count = 0u64;
    let mut body_offset = 0u64;
    let mut current_code: Option<i32> = None;
    let mut current_block_count = 0u32;
    let mut previous_provider_set_key = 0i32;

    let mut reader = BufReader::new(File::open(input_path).map_err(|error| {
        io::Error::new(
            error.kind(),
            format!(
                "failed to open serving-by-code COPY file {}: {error}",
                input_path.display()
            ),
        )
    })?);
    let mut line = Vec::new();
    loop {
        line.clear();
        if reader.read_until(b'\n', &mut line)? == 0 {
            break;
        }
        let fields = serving_copy_fields(&line);
        if fields.len() < 4 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "serving-by-code COPY rows must have at least 4 fields",
            ));
        }
        let code_key = serving_parse_i32(fields[0], "code_key")?;
        let provider_set_key = serving_parse_i32(fields[1], "provider_set_key")?;
        let provider_count = serving_parse_u64(fields[2], "provider_count")?;
        let price_set_id = serving_parse_global_id(fields[3])?;
        if current_code != Some(code_key) {
            if let Some(block) = blocks.last_mut() {
                block.count = current_block_count;
            }
            blocks.push(ServingBlock {
                key: code_key,
                offset: body_offset,
                count: 0,
            });
            current_code = Some(code_key);
            current_block_count = 0;
            previous_provider_set_key = 0;
        }
        if provider_set_key < previous_provider_set_key {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "serving-by-code rows must be ordered by provider_set_key within code_key",
            ));
        }
        let price_set_key =
            serving_price_key(price_set_id, &mut price_set_to_key, &mut price_set_values)?;
        let mut encoded = Vec::with_capacity(16);
        write_uvarint_to_vec(
            &mut encoded,
            (provider_set_key - previous_provider_set_key) as u64,
        );
        write_uvarint_to_vec(&mut encoded, provider_count);
        write_uvarint_to_vec(&mut encoded, u64::from(price_set_key));
        body.write_all(&encoded)?;
        body_offset = body_offset.saturating_add(encoded.len() as u64);
        previous_provider_set_key = provider_set_key;
        current_block_count = current_block_count.saturating_add(1);
        row_count = row_count.saturating_add(1);
    }
    if let Some(block) = blocks.last_mut() {
        block.count = current_block_count;
    }
    body.flush()?;
    drop(body);

    let metadata = json!({
        "format": PTG2_SERVING_BY_CODE_FORMAT,
        "row_count": row_count,
        "code_count": blocks.len(),
        "price_set_count": price_set_values.len(),
        "body_bytes": body_offset,
        "price_dictionary_bytes": price_set_values.len() * GLOBAL_ID_BYTES,
        "block_index_bytes": blocks.len() * 16,
    });
    let byte_count = write_serving_sidecar_final(
        output_path,
        &body_path,
        PTG2_SERVING_BY_CODE_MAGIC,
        &metadata,
        &price_set_values,
        &blocks,
    )?;
    let _ = std::fs::remove_file(&body_path);
    Ok(json!({
        "name": "serving_by_code",
        "path": output_path.display().to_string(),
        "format": PTG2_SERVING_BY_CODE_FORMAT,
        "row_count": row_count,
        "byte_count": byte_count,
        "metadata": metadata,
    }))
}

fn flush_serving_provider_code(
    current_code: Option<i32>,
    current_code_entries: &mut ServingProviderEntries,
    current_patterns: &mut ServingProviderPatternMap,
) {
    if let Some(code_key) = current_code {
        let entries = std::mem::take(current_code_entries);
        current_patterns.entry(entries).or_default().push(code_key);
    }
}

fn write_serving_provider_block<W: Write>(
    body: &mut W,
    provider_set_key: i32,
    blocks: &mut Vec<ServingBlock>,
    body_offset: &mut u64,
    pattern_count: &mut u64,
    current_patterns: &mut ServingProviderPatternMap,
) -> io::Result<()> {
    let mut ordered_patterns: Vec<ServingProviderPattern> = current_patterns.drain().collect();
    ordered_patterns.sort_unstable_by(|left, right| {
        let left_first = left.1.iter().min().copied().unwrap_or(-1);
        let right_first = right.1.iter().min().copied().unwrap_or(-1);
        left_first
            .cmp(&right_first)
            .then_with(|| left.0.cmp(&right.0))
    });
    blocks.push(ServingBlock {
        key: provider_set_key,
        offset: *body_offset,
        count: 0,
    });
    let mut block_count = 0u32;
    for (entries, mut code_keys) in ordered_patterns {
        code_keys.sort_unstable();
        code_keys.dedup();
        let mut encoded = Vec::with_capacity(32 + code_keys.len() * 3 + entries.len() * 6);
        write_uvarint_to_vec(&mut encoded, code_keys.len() as u64);
        let mut previous_code_key = 0i32;
        for (index, code_key) in code_keys.iter().copied().enumerate() {
            let delta = if index == 0 {
                code_key
            } else {
                code_key - previous_code_key
            };
            if delta < 0 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "serving-by-provider-set code keys must be nondecreasing",
                ));
            }
            write_uvarint_to_vec(&mut encoded, delta as u64);
            previous_code_key = code_key;
        }
        write_uvarint_to_vec(&mut encoded, entries.len() as u64);
        for (provider_count, price_set_key) in entries {
            write_uvarint_to_vec(&mut encoded, provider_count);
            write_uvarint_to_vec(&mut encoded, u64::from(price_set_key));
        }
        body.write_all(&encoded)?;
        *body_offset = body_offset.saturating_add(encoded.len() as u64);
        block_count = block_count.saturating_add(1);
        *pattern_count = pattern_count.saturating_add(1);
    }
    if let Some(block) = blocks.last_mut() {
        block.count = block_count;
    }
    Ok(())
}

fn write_serving_by_provider_set_from_copy(
    input_path: &Path,
    output_path: &Path,
) -> io::Result<Value> {
    if let Some(parent) = output_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let body_path = output_path.with_extension("body.tmp");
    let mut body = BufWriter::new(File::create(&body_path).map_err(|error| {
        io::Error::new(
            error.kind(),
            format!(
                "failed to create serving-by-provider-set body file {}: {error}",
                body_path.display()
            ),
        )
    })?);
    let mut price_set_to_key: HashMap<[u8; GLOBAL_ID_BYTES], u32> = HashMap::new();
    let mut price_set_values: Vec<[u8; GLOBAL_ID_BYTES]> = Vec::new();
    let mut code_keys_seen: HashSet<i32> = HashSet::new();
    let mut blocks: Vec<ServingBlock> = Vec::new();
    let mut row_count = 0u64;
    let mut body_offset = 0u64;
    let mut pattern_count = 0u64;
    let mut current_provider_set: Option<i32> = None;
    let mut current_code: Option<i32> = None;
    let mut current_code_entries: ServingProviderEntries = Vec::new();
    let mut current_patterns: ServingProviderPatternMap = HashMap::new();

    let mut reader = BufReader::new(File::open(input_path).map_err(|error| {
        io::Error::new(
            error.kind(),
            format!(
                "failed to open serving-by-provider-set COPY file {}: {error}",
                input_path.display()
            ),
        )
    })?);
    let mut line = Vec::new();
    loop {
        line.clear();
        if reader.read_until(b'\n', &mut line)? == 0 {
            break;
        }
        let fields = serving_copy_fields(&line);
        if fields.len() < 4 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "serving-by-provider-set COPY rows must have at least 4 fields",
            ));
        }
        let provider_set_key = serving_parse_i32(fields[0], "provider_set_key")?;
        let code_key = serving_parse_i32(fields[1], "code_key")?;
        let provider_count = serving_parse_u64(fields[2], "provider_count")?;
        let price_set_id = serving_parse_global_id(fields[3])?;
        let price_set_key =
            serving_price_key(price_set_id, &mut price_set_to_key, &mut price_set_values)?;
        if current_provider_set != Some(provider_set_key) {
            if let Some(previous_provider_set) = current_provider_set {
                flush_serving_provider_code(
                    current_code,
                    &mut current_code_entries,
                    &mut current_patterns,
                );
                write_serving_provider_block(
                    &mut body,
                    previous_provider_set,
                    &mut blocks,
                    &mut body_offset,
                    &mut pattern_count,
                    &mut current_patterns,
                )?;
            }
            current_provider_set = Some(provider_set_key);
            current_code = None;
        }
        if current_code != Some(code_key) {
            flush_serving_provider_code(
                current_code,
                &mut current_code_entries,
                &mut current_patterns,
            );
            current_code = Some(code_key);
        }
        current_code_entries.push((provider_count, price_set_key));
        code_keys_seen.insert(code_key);
        row_count = row_count.saturating_add(1);
    }
    if let Some(provider_set_key) = current_provider_set {
        flush_serving_provider_code(
            current_code,
            &mut current_code_entries,
            &mut current_patterns,
        );
        write_serving_provider_block(
            &mut body,
            provider_set_key,
            &mut blocks,
            &mut body_offset,
            &mut pattern_count,
            &mut current_patterns,
        )?;
    }
    body.flush()?;
    drop(body);

    let metadata = json!({
        "format": PTG2_SERVING_BY_PROVIDER_SET_FORMAT,
        "row_count": row_count,
        "provider_set_count": blocks.len(),
        "code_count": code_keys_seen.len(),
        "price_set_count": price_set_values.len(),
        "pattern_count": pattern_count,
        "body_bytes": body_offset,
        "price_dictionary_bytes": price_set_values.len() * GLOBAL_ID_BYTES,
        "block_index_bytes": blocks.len() * 16,
    });
    let byte_count = write_serving_sidecar_final(
        output_path,
        &body_path,
        PTG2_SERVING_BY_PROVIDER_SET_MAGIC,
        &metadata,
        &price_set_values,
        &blocks,
    )?;
    let _ = std::fs::remove_file(&body_path);
    Ok(json!({
        "name": "serving_by_provider_set",
        "path": output_path.display().to_string(),
        "format": PTG2_SERVING_BY_PROVIDER_SET_FORMAT,
        "row_count": row_count,
        "byte_count": byte_count,
        "metadata": metadata,
    }))
}

fn write_serving_sidecar_from_key_copy(
    kind: &str,
    input_path: &Path,
    output_path: &Path,
) -> io::Result<()> {
    let payload = match kind {
        "by_code" => write_serving_by_code_from_copy(input_path, output_path)?,
        "by_provider_set" => write_serving_by_provider_set_from_copy(input_path, output_path)?,
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "serving sidecar kind must be by_code or by_provider_set",
            ));
        }
    };
    emit_json_record(&mut io::stdout().lock(), "serving_sidecar_file", &payload)
}

fn global_id_from_hex_bytes(value: &[u8]) -> io::Result<GlobalId128> {
    if value.len() != GLOBAL_ID_BYTES * 2 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "provider group global id must contain 32 hex characters",
        ));
    }
    let mut decoded = [0u8; GLOBAL_ID_BYTES];
    for (index, pair) in value.chunks_exact(2).enumerate() {
        let high = (pair[0] as char).to_digit(16).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid provider group global id",
            )
        })? as u8;
        let low = (pair[1] as char).to_digit(16).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid provider group global id",
            )
        })? as u8;
        decoded[index] = (high << 4) | low;
    }
    Ok(GlobalId128(decoded))
}

fn write_provider_membership_sidecars(
    group_npi_path: &Path,
    npi_group_path: &Path,
    npi_scope_copy_path: &Path,
    input_paths: &[String],
) -> io::Result<()> {
    let mut group_npi_pairs = ManifestPairSpool::new("provider_group_npi")?;
    let mut npi_group_pairs = ManifestPairSpool::new("provider_npi_group")?;
    let mut npis = HashSet::new();
    let mut input_rows = 0u64;
    for input_path in input_paths {
        let file = match File::open(input_path) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error),
        };
        let mut reader = BufReader::new(file);
        let mut line = Vec::new();
        while reader.read_until(b'\n', &mut line)? > 0 {
            while matches!(line.last().copied(), Some(b'\n' | b'\r')) {
                line.pop();
            }
            let mut fields = line.splitn(3, |byte| *byte == b'\t');
            let group_value = fields.next().unwrap_or_default();
            let npi_value = fields.next().unwrap_or_default();
            if group_value.is_empty() || npi_value.is_empty() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "provider membership COPY row must contain group id and NPI",
                ));
            }
            let provider_group_id = global_id_from_hex_bytes(group_value)?;
            let npi_text = std::str::from_utf8(npi_value).map_err(to_io_error)?;
            let npi = npi_text.parse::<i64>().map_err(to_io_error)?;
            if npi > 0 {
                let provider_npi_id = npi_member_id(npi);
                group_npi_pairs.push(provider_group_id, provider_npi_id)?;
                npi_group_pairs.push(provider_npi_id, provider_group_id)?;
                npis.insert(npi);
            }
            input_rows = input_rows.saturating_add(1);
            line.clear();
        }
    }

    let group_npi_text = group_npi_path.to_string_lossy();
    let npi_group_text = npi_group_path.to_string_lossy();
    let (group_count, membership_count) =
        group_npi_pairs.write_dense_sidecar(group_npi_text.as_ref())?;
    let (npi_count, reverse_membership_count) =
        npi_group_pairs.write_dense_sidecar(npi_group_text.as_ref())?;
    if membership_count != reverse_membership_count {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "provider membership sidecars contain different edge counts",
        ));
    }

    let mut sorted_npis: Vec<i64> = npis.into_iter().collect();
    sorted_npis.sort_unstable();
    let mut npi_scope_writer = BufWriter::new(File::create(npi_scope_copy_path)?);
    for npi in &sorted_npis {
        writeln!(npi_scope_writer, "{npi}")?;
    }
    npi_scope_writer.flush()?;

    emit_json_record(
        &mut io::stdout().lock(),
        "provider_membership_sidecars",
        &json!({
            "input_files": input_paths.len(),
            "input_rows": input_rows,
            "membership_count": membership_count,
            "provider_group_count": group_count,
            "provider_npi_count": npi_count,
            "npi_scope_count": sorted_npis.len(),
            "provider_group_npi_path": group_npi_path.display().to_string(),
            "provider_group_npi_bytes": group_npi_path.metadata()?.len(),
            "provider_npi_group_path": npi_group_path.display().to_string(),
            "provider_npi_group_bytes": npi_group_path.metadata()?.len(),
            "provider_npi_scope_copy_path": npi_scope_copy_path.display().to_string(),
        }),
    )
}

fn merge_manifest_copy_files(
    kind: &str,
    output_path: &Path,
    input_paths: &[String],
) -> io::Result<()> {
    let temp_dir = env::var_os("HLTHPRT_PTG2_MANIFEST_MERGE_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(env::temp_dir);
    std::fs::create_dir_all(&temp_dir)?;
    let chunk_max_bytes =
        env_usize("HLTHPRT_PTG2_MANIFEST_MERGE_CHUNK_BYTES", 512 * 1024 * 1024).max(1024 * 1024);
    let sort_workers = env_usize("HLTHPRT_PTG2_MANIFEST_MERGE_SORT_WORKERS", 1).max(1);
    let mut sort_handles: Vec<thread::JoinHandle<io::Result<(usize, PathBuf)>>> = Vec::new();
    let mut sorted_chunks: Vec<(usize, PathBuf)> = Vec::new();
    let mut chunk_rows: Vec<Vec<u8>> = Vec::new();
    let mut chunk_bytes = 0usize;
    let mut input_rows = 0u64;
    let mut chunk_count = 0usize;
    for raw_path in input_paths {
        let file = match File::open(raw_path) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error),
        };
        let mut reader = BufReader::new(file);
        loop {
            let mut line = Vec::new();
            let bytes = reader.read_until(b'\n', &mut line)?;
            if bytes == 0 {
                break;
            }
            chunk_bytes = chunk_bytes.saturating_add(bytes);
            input_rows = input_rows.saturating_add(1);
            chunk_rows.push(line);
            if chunk_bytes >= chunk_max_bytes {
                let rows = std::mem::take(&mut chunk_rows);
                sort_handles.push(manifest_spawn_sort_chunk(
                    kind,
                    rows,
                    chunk_count,
                    &temp_dir,
                ));
                chunk_count += 1;
                chunk_bytes = 0;
                manifest_flush_sort_handles(&mut sort_handles, &mut sorted_chunks, sort_workers)?;
            }
        }
    }
    if !chunk_rows.is_empty() {
        let rows = std::mem::take(&mut chunk_rows);
        sort_handles.push(manifest_spawn_sort_chunk(
            kind,
            rows,
            chunk_count,
            &temp_dir,
        ));
    }
    for handle in sort_handles {
        sorted_chunks.push(manifest_join_sort_chunk(handle)?);
    }
    sorted_chunks.sort_unstable_by_key(|(index, _)| *index);
    let chunk_paths: Vec<PathBuf> = sorted_chunks.into_iter().map(|(_, path)| path).collect();
    let mut output = BufWriter::new(File::create(output_path)?);
    let mut output_rows = 0u64;
    if !chunk_paths.is_empty() {
        let mut readers = Vec::with_capacity(chunk_paths.len());
        for chunk_path in &chunk_paths {
            readers.push(BufReader::new(File::open(chunk_path)?));
        }
        let mut heap = BinaryHeap::new();
        for reader_index in 0..readers.len() {
            manifest_merge_read_next(kind, reader_index, &mut readers, &mut heap)?;
        }
        let mut current_key: Option<Vec<u8>> = None;
        let mut selected_line: Option<Vec<u8>> = None;
        while let Some(item) = heap.pop() {
            if current_key.as_ref() != Some(&item.key) {
                if let Some(line) = selected_line.take() {
                    output.write_all(&line)?;
                    output_rows = output_rows.saturating_add(1);
                }
                current_key = Some(item.key.clone());
                selected_line = Some(item.line);
            } else if let Some(line) = selected_line.take() {
                selected_line = Some(manifest_prefer_row(kind, line, item.line));
            }
            manifest_merge_read_next(kind, item.reader_index, &mut readers, &mut heap)?;
        }
        if let Some(line) = selected_line.take() {
            output.write_all(&line)?;
            output_rows = output_rows.saturating_add(1);
        }
    }
    output.flush()?;
    for chunk_path in &chunk_paths {
        let _ = std::fs::remove_file(chunk_path);
    }
    emit_json_record(
        &mut io::stdout().lock(),
        "manifest_copy_merge_summary",
        &json!({
            "kind": kind,
            "input_files": input_paths.len(),
            "input_rows": input_rows,
            "output_rows": output_rows,
            "dropped_rows": input_rows.saturating_sub(output_rows),
            "chunk_count": chunk_paths.len(),
            "chunk_max_bytes": chunk_max_bytes,
            "sort_workers": sort_workers,
            "output_path": output_path.display().to_string(),
        }),
    )?;
    Ok(())
}

#[cfg(test)]
#[allow(clippy::items_after_test_module)]
mod tests {
    use super::*;
    use ptg2_scanner::manifest::GLOBAL_ID_BYTES;

    #[test]
    fn manifest_only_disables_high_cardinality_v2_dictionary_sinks() {
        let provider_set_entry_path = std::env::temp_dir().join(format!(
            "ptg2-provider-set-entry-{}.copy",
            std::process::id()
        ));
        let paths = CopyPathConfig {
            compact: None,
            manifest_serving: None,
            manifest_lean_serving: None,
            manifest_provider_forward_sidecar: None,
            manifest_provider_inverted_sidecar: None,
            manifest_provider_npi_sidecar: None,
            manifest_price_forward_sidecar: None,
            manifest_price_atom: None,
            manifest_price_set_atom: None,
            manifest_provider_group_member: None,
            manifest_code_count: None,
            manifest_provider_set_dictionary: None,
            procedure: None,
            price_code_set: None,
            price_atom: None,
            price_set_entry: Some("unused-price-set-entry.copy".to_string()),
            provider_set: None,
            provider_set_component: Some("unused-provider-set-component.copy".to_string()),
            provider_set_entry: Some(provider_set_entry_path.to_string_lossy().to_string()),
            provider_entry_component: None,
            provider_group_member: None,
            manifest_only: true,
        };

        let sinks = DictionaryCopySinks::from_paths(&paths, 0).unwrap();

        assert!(sinks.price_set_entry.is_none());
        assert!(sinks.provider_set_component.is_none());
        assert!(sinks.provider_set_entry.is_some());
        drop(sinks);
        let _ = std::fs::remove_file(provider_set_entry_path);
    }

    #[test]
    fn provider_reference_copy_sinks_are_suffix_isolated() {
        let base = std::env::temp_dir().join(format!(
            "ptg2-provider-ref-copy-test-{}",
            std::process::id()
        ));
        let _ = std::fs::create_dir_all(&base);
        let manifest_member_path = base.join("manifest-provider-group-member.copy");
        let member_path = base.join("provider-group-member.copy");
        let paths = CopyPathConfig {
            compact: Some(
                base.join("unused-compact.copy")
                    .to_string_lossy()
                    .to_string(),
            ),
            manifest_serving: Some(
                base.join("unused-serving.copy")
                    .to_string_lossy()
                    .to_string(),
            ),
            manifest_lean_serving: None,
            manifest_provider_forward_sidecar: None,
            manifest_provider_inverted_sidecar: None,
            manifest_provider_npi_sidecar: None,
            manifest_price_forward_sidecar: None,
            manifest_price_atom: Some(base.join("unused-price.copy").to_string_lossy().to_string()),
            manifest_price_set_atom: None,
            manifest_provider_group_member: Some(
                manifest_member_path.to_string_lossy().to_string(),
            ),
            manifest_code_count: None,
            manifest_provider_set_dictionary: None,
            procedure: Some(
                base.join("unused-procedure.copy")
                    .to_string_lossy()
                    .to_string(),
            ),
            price_code_set: None,
            price_atom: None,
            price_set_entry: None,
            provider_set: None,
            provider_set_component: None,
            provider_set_entry: None,
            provider_entry_component: None,
            provider_group_member: Some(member_path.to_string_lossy().to_string()),
            manifest_only: true,
        };

        let provider_ref_paths = paths.for_provider_refs();
        let mut sinks = DictionaryCopySinks::from_paths(&provider_ref_paths, 0).unwrap();
        let dedupe = SharedDedupe::new(1);
        let provider_ref = json!({
            "provider_groups": [{
                "tin": {"type": "ein", "value": "123456789"},
                "npi": [1234567890, 1234567891]
            }]
        });

        sinks
            .write_provider_group_members_shared(&provider_ref, &dedupe)
            .unwrap();
        sinks
            .write_provider_group_members_shared(&provider_ref, &dedupe)
            .unwrap();
        let events = sinks.finish_silent().unwrap();

        assert_eq!(events.len(), 2);
        assert!(events.iter().all(|event| {
            event.path.ends_with(".provider_refs")
                && event.row_count == 2
                && matches!(
                    event.record_kind.as_str(),
                    "manifest_provider_group_member_copy_file" | "provider_group_member_copy_file"
                )
        }));
        assert!(manifest_member_path
            .with_extension("copy.provider_refs")
            .exists());
        assert!(member_path.with_extension("copy.provider_refs").exists());
        assert!(!manifest_member_path.exists());
        assert!(!member_path.exists());
        let summary = dedupe_summary_payload(&dedupe, &HashMap::new());
        assert_eq!(summary["provider_group_attempted"], 2);
        assert_eq!(summary["provider_group_unique"], 1);
        assert_eq!(summary["provider_group_duplicate"], 1);
        assert_eq!(summary["provider_group_member_attempted"], 2);
        assert_eq!(summary["provider_group_member_unique"], 2);
        assert_eq!(summary["provider_group_member_duplicate"], 0);

        let _ = std::fs::remove_dir_all(base);
    }

    #[test]
    fn provider_membership_sidecars_preserve_both_edge_directions() {
        let base = std::env::temp_dir().join(format!(
            "ptg2-provider-membership-sidecar-test-{}",
            std::process::id()
        ));
        let _ = std::fs::create_dir_all(&base);
        let input_path = base.join("members.copy");
        let group_npi_path = base.join("group-npi.ptg2sc");
        let npi_group_path = base.join("npi-group.ptg2sc");
        let npi_scope_path = base.join("npi-scope.copy");
        std::fs::write(
            &input_path,
            b"00000000000000000000000000000001\t1003002106\n\
              00000000000000000000000000000001\t1003007311\n\
              00000000000000000000000000000002\t1003002106\n\
              00000000000000000000000000000002\t1003002106\n",
        )
        .unwrap();

        write_provider_membership_sidecars(
            &group_npi_path,
            &npi_group_path,
            &npi_scope_path,
            &[input_path.to_string_lossy().to_string()],
        )
        .unwrap();

        let group_payload = std::fs::read(&group_npi_path).unwrap();
        let npi_payload = std::fs::read(&npi_group_path).unwrap();
        assert_eq!(&group_payload[..8], b"PTG2MNDS");
        assert_eq!(&npi_payload[..8], b"PTG2MNDS");
        assert_eq!(
            u64::from_le_bytes(group_payload[12..20].try_into().unwrap()),
            2
        );
        assert_eq!(
            u64::from_le_bytes(npi_payload[12..20].try_into().unwrap()),
            2
        );
        assert_eq!(
            std::fs::read_to_string(&npi_scope_path).unwrap(),
            "1003002106\n1003007311\n"
        );

        let _ = std::fs::remove_dir_all(base);
    }

    #[test]
    fn provider_membership_sidecars_allow_empty_npi_scope() {
        let base = std::env::temp_dir().join(format!(
            "ptg2-empty-provider-membership-test-{}",
            std::process::id()
        ));
        let _ = std::fs::create_dir_all(&base);
        let group_npi_path = base.join("group-npi.ptg2sc");
        let npi_group_path = base.join("npi-group.ptg2sc");
        let npi_scope_path = base.join("npi-scope.copy");

        write_provider_membership_sidecars(&group_npi_path, &npi_group_path, &npi_scope_path, &[])
            .unwrap();

        for sidecar_path in [&group_npi_path, &npi_group_path] {
            let sidecar_payload = std::fs::read(sidecar_path).unwrap();
            assert_eq!(&sidecar_payload[..8], b"PTG2MNDS");
            assert_eq!(
                u64::from_le_bytes(sidecar_payload[12..20].try_into().unwrap()),
                0
            );
        }
        assert_eq!(std::fs::read_to_string(&npi_scope_path).unwrap(), "");

        let _ = std::fs::remove_dir_all(base);
    }

    #[test]
    fn provider_entry_can_skip_npi_retention_without_changing_hashes() {
        let provider_ref = json!({
            "provider_groups": [{
                "tin": {"type": "ein", "value": "123456789"},
                "npi": [1234567890, 1234567891]
            }]
        });

        let retained = build_provider_entry(&provider_ref, true).unwrap();
        let pruned = build_provider_entry(&provider_ref, false).unwrap();

        assert_eq!(retained.entry_hash, pruned.entry_hash);
        assert_eq!(retained.provider_count, 2);
        assert_eq!(pruned.provider_count, 2);
        assert_eq!(retained.provider_group_hashes, pruned.provider_group_hashes);
        assert_eq!(retained.npi, vec![1234567890, 1234567891]);
        assert!(pruned.npi.is_empty());
    }

    #[test]
    fn raw_provider_reference_batch_builds_provider_map_without_npi_retention() {
        let paths = CopyPathConfig {
            compact: None,
            manifest_serving: None,
            manifest_lean_serving: None,
            manifest_provider_forward_sidecar: None,
            manifest_provider_inverted_sidecar: None,
            manifest_provider_npi_sidecar: None,
            manifest_price_forward_sidecar: None,
            manifest_price_atom: None,
            manifest_price_set_atom: None,
            manifest_provider_group_member: None,
            manifest_code_count: None,
            manifest_provider_set_dictionary: None,
            procedure: None,
            price_code_set: None,
            price_atom: None,
            price_set_entry: None,
            provider_set: None,
            provider_set_component: None,
            provider_set_entry: None,
            provider_entry_component: None,
            provider_group_member: None,
            manifest_only: true,
        };
        let mut sinks = DictionaryCopySinks::from_paths(&paths, 0).unwrap();
        let dedupe = SharedDedupe::new(2);
        let mut provider_map = HashMap::new();
        let raw_ref_values = [
            br#"{"provider_group_id":"7","provider_groups":[{"tin":{"type":"ein","value":"123456789"},"npi":[1234567890,1234567891]}]}"#.to_vec(),
            br#"{"provider_group_id":8,"provider_groups":[{"tin":{"type":"npi","value":"9876543210"},"npi":["2222222222"]}]}"#.to_vec(),
            br#"{"provider_group_id":121591448686103182592848195376305442061,"provider_groups":[{"tin":{"type":"ein","value":"462560124"},"npi":[1265502504]}]}"#.to_vec(),
        ];
        let mut raw_refs = RawRateChunk::with_capacity(raw_ref_values.len(), 1024);
        for raw_ref in raw_ref_values {
            let start = raw_refs.byte_len();
            raw_refs.bytes.extend_from_slice(&raw_ref);
            raw_refs.push_current_value_span(start);
        }

        let processed = process_provider_ref_raw_batch(
            &raw_refs,
            false,
            &mut provider_map,
            &mut sinks,
            &dedupe,
        )
        .unwrap();

        assert_eq!(processed, 3);
        assert_eq!(provider_map.len(), 3);
        assert!(provider_map.contains_key("7"));
        assert!(provider_map.contains_key("8"));
        assert!(provider_map.contains_key("121591448686103182592848195376305442061"));
        assert_eq!(provider_map["7"].provider_count, 2);
        assert_eq!(provider_map["8"].provider_count, 1);
        assert_eq!(
            provider_map["121591448686103182592848195376305442061"].provider_count,
            1
        );
        assert!(provider_map["7"].npi.is_empty());
        assert!(!provider_map["7"].provider_group_hashes.is_empty());
    }

    #[test]
    fn lean_manifest_serving_sink_emits_lean_copy_event_and_columns() {
        let base = std::env::temp_dir().join(format!(
            "ptg2-lean-manifest-serving-{}.copy",
            std::process::id()
        ));
        let mut sink = CompactCopySink::new_manifest_serving_file(
            base.to_string_lossy().to_string(),
            0,
            ManifestServingCopyLayout::Lean,
        )
        .unwrap();

        sink.write_manifest_serving_row(&ManifestServingCopyRow {
            serving_content_hash_128: "unused_content_id",
            plan_id: "plan-a",
            reported_code_system: Some("CPT"),
            reported_code: Some("29888"),
            procedure_global_id_128: "unused_procedure_id",
            provider_set_global_id_128: "provider-set-id",
            provider_count: 3,
            price_set_global_id_128: "price-set-id",
            source_trace_set_hash: "unused_source_trace",
            network_names: &["C2".to_string()],
        })
        .unwrap();

        let event = sink.finish_silent().unwrap().unwrap();
        let body = std::fs::read_to_string(&base).unwrap();

        assert_eq!(event.record_kind, "manifest_lean_serving_copy_file");
        assert_eq!(event.row_count, 1);
        assert_eq!(
            body,
            "plan-a\tCPT\t29888\tprovider-set-id\t3\tprice-set-id\n"
        );
        let _ = std::fs::remove_file(base);
    }

    #[test]
    fn manifest_price_set_atom_sink_emits_global_id_pairs() {
        let base = std::env::temp_dir().join(format!(
            "ptg2-manifest-price-set-atom-{}.copy",
            std::process::id()
        ));
        let paths = CopyPathConfig {
            compact: None,
            manifest_serving: None,
            manifest_lean_serving: None,
            manifest_provider_forward_sidecar: None,
            manifest_provider_inverted_sidecar: None,
            manifest_provider_npi_sidecar: None,
            manifest_price_forward_sidecar: None,
            manifest_price_atom: None,
            manifest_price_set_atom: Some(base.to_string_lossy().to_string()),
            manifest_provider_group_member: None,
            manifest_code_count: None,
            manifest_provider_set_dictionary: None,
            procedure: None,
            price_code_set: None,
            price_atom: None,
            price_set_entry: None,
            provider_set: None,
            provider_set_component: None,
            provider_set_entry: None,
            provider_entry_component: None,
            provider_group_member: None,
            manifest_only: true,
        };
        let mut sinks = DictionaryCopySinks::from_paths(&paths, 0).unwrap();
        let mut price_code_set_hash_cache = PriceCodeSetHashCache::new();
        let atom = price_atom_from_lite(
            &PriceLite {
                negotiated_type: Some("negotiated".to_string()),
                negotiated_rate: "123.45".to_string(),
                expiration_date: Some("2026-12-31".to_string()),
                service_code: vec!["11".to_string()],
                billing_class: Some("professional".to_string()),
                setting: None,
                billing_code_modifier: vec![],
                additional_information: None,
            },
            &mut price_code_set_hash_cache,
        );
        let price_set = PriceSetLite {
            price_set_hash: "unused-hash".to_string(),
            price_atom_hashes: vec![atom.price_atom_hash.clone()],
            atoms: vec![atom],
        };

        sinks.write_manifest_price_set_atoms(&price_set).unwrap();
        let events = sinks.finish_silent().unwrap();
        let body = std::fs::read_to_string(&base).unwrap();
        let expected_price_set_id = price_set_global_id(&price_set).to_hex();
        let expected_atom_id = price_atom_global_id(&price_set.atoms[0]).to_hex();

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].record_kind, "manifest_price_set_atom_copy_file");
        assert_eq!(events[0].row_count, 1);
        assert_eq!(
            body,
            format!("{expected_price_set_id}\t{expected_atom_id}\n")
        );
        let _ = std::fs::remove_file(base);
    }

    #[test]
    fn raw_worker_parsers_repair_invalid_utf8_values() {
        let mut raw_rate = br#"{"provider_references":[7],"negotiated_prices":[{"negotiated_type":"negotiated","negotiated_rate":100,"additional_information":"A"#.to_vec();
        raw_rate.push(0xff);
        raw_rate.extend_from_slice(br#"B"}]}"#);
        let rate = read_rate_lite_bytes(&raw_rate).unwrap().unwrap();
        let expected = format!("A{}B", char::REPLACEMENT_CHARACTER);
        assert_eq!(
            rate.prices[0].additional_information.as_deref(),
            Some(expected.as_str())
        );

        let paths = CopyPathConfig {
            compact: None,
            manifest_serving: None,
            manifest_lean_serving: None,
            manifest_provider_forward_sidecar: None,
            manifest_provider_inverted_sidecar: None,
            manifest_provider_npi_sidecar: None,
            manifest_price_forward_sidecar: None,
            manifest_price_atom: None,
            manifest_price_set_atom: None,
            manifest_provider_group_member: None,
            manifest_code_count: None,
            manifest_provider_set_dictionary: None,
            procedure: None,
            price_code_set: None,
            price_atom: None,
            price_set_entry: None,
            provider_set: None,
            provider_set_component: None,
            provider_set_entry: None,
            provider_entry_component: None,
            provider_group_member: None,
            manifest_only: true,
        };
        let mut sinks = DictionaryCopySinks::from_paths(&paths, 0).unwrap();
        let dedupe = SharedDedupe::new(1);
        let mut provider_map = HashMap::new();
        let mut raw_ref = br#"{"provider_group_id":"9","provider_groups":[{"tin":{"type":"ein","value":"123456789"},"npi":[1234567890],"bad":"A"#.to_vec();
        raw_ref.push(0xff);
        raw_ref.extend_from_slice(br#"B"}]}"#);
        let mut raw_refs = RawRateChunk::with_capacity(1, raw_ref.len());
        let start = raw_refs.byte_len();
        raw_refs.bytes.extend_from_slice(&raw_ref);
        raw_refs.push_current_value_span(start);

        let processed = process_provider_ref_raw_batch(
            &raw_refs,
            false,
            &mut provider_map,
            &mut sinks,
            &dedupe,
        )
        .unwrap();

        assert_eq!(processed, 1);
        assert!(provider_map.contains_key("9"));
    }

    #[test]
    fn capture_value_bytes_into_reuses_scratch_buffer() {
        let input = br#" {"a":[1,{"b":"c"}]} "tail" "#;
        let mut reader = BufferedJsonByteReader::new(&input[..]);
        let mut scratch = Vec::with_capacity(64);

        reader.capture_value_bytes_into(&mut scratch).unwrap();
        let retained_capacity = scratch.capacity();
        assert_eq!(scratch, br#"{"a":[1,{"b":"c"}]}"#);

        reader.capture_value_bytes_into(&mut scratch).unwrap();

        assert_eq!(scratch, br#""tail""#);
        assert_eq!(scratch.capacity(), retained_capacity);
    }

    #[test]
    fn raw_rate_chunk_iter_returns_contiguous_spans() {
        let mut chunk = RawRateChunk::with_capacity(2, 64);
        let first = chunk.byte_len();
        chunk.bytes.extend_from_slice(br#"{"a":1}"#);
        chunk.push_current_value_span(first);
        let second = chunk.byte_len();
        chunk.bytes.extend_from_slice(br#"{"b":[2,3]}"#);
        chunk.push_current_value_span(second);

        let raw_values: Vec<&[u8]> = chunk.iter().collect();

        assert_eq!(chunk.len(), 2);
        assert_eq!(chunk.byte_len(), br#"{"a":1}{"b":[2,3]}"#.len());
        assert_eq!(
            raw_values,
            vec![br#"{"a":1}"#.as_slice(), br#"{"b":[2,3]}"#.as_slice()]
        );
    }

    #[test]
    fn provider_entry_compact_provider_set_hash_matches_json_payload_hash() {
        let provider_ref = json!({
            "provider_groups": [
                {
                    "tin": {"type": "ein", "value": "12-3456789"},
                    "npi": [1234567891, 1234567890, 1234567890, 114911247]
                },
                {
                    "tin": {"type": "npi", "value": " 9876543210 "},
                    "npi": ["2222222222", "1111111111", "263839538"]
                }
            ]
        });

        let entry = build_provider_entry(&provider_ref, true).unwrap();
        let mut group_payloads: Vec<Value> = Vec::new();
        for group in provider_ref
            .get("provider_groups")
            .and_then(Value::as_array)
            .unwrap()
        {
            let tin = group.get("tin").unwrap_or(&Value::Null);
            let npi = npi_list(group.get("npi"));
            let group_hash = provider_group_hash(tin, &npi);
            group_payloads.push(json!({
                "provider_group_hash": group_hash,
                "tin_type": normalize_tin_type(tin.get("type")),
                "tin_value": normalize_tin_value(tin.get("value")),
                "npi": npi,
            }));
        }
        group_payloads.sort_by_cached_key(ptg2_scanner::hashing::canonical_json);
        let expected = make_checksum(vec![json!("provider_set"), Value::Array(group_payloads)]);

        assert_eq!(entry.entry_hash, expected);
        assert_eq!(entry.provider_count, 4);
        assert_eq!(
            entry.npi,
            vec![1111111111, 1234567890, 1234567891, 2222222222]
        );
    }

    #[test]
    fn provider_entry_view_borrows_single_refs_and_owns_combined_refs() {
        let provider_ref_a = json!({
            "provider_groups": [{
                "tin": {"type": "ein", "value": "123456789"},
                "npi": [1234567890]
            }]
        });
        let provider_ref_b = json!({
            "provider_groups": [{
                "tin": {"type": "ein", "value": "987654321"},
                "npi": [1234567891]
            }]
        });
        let mut provider_map = HashMap::new();
        provider_map.insert(
            "1".to_string(),
            build_provider_entry(&provider_ref_a, false).unwrap(),
        );
        provider_map.insert(
            "2".to_string(),
            build_provider_entry(&provider_ref_b, false).unwrap(),
        );

        let single = provider_entry_view_from_ref_keys(&provider_map, &["1".to_string()])
            .expect("single ref should resolve");
        assert!(matches!(single, ProviderEntryView::Borrowed(_)));

        let combined =
            provider_entry_view_from_ref_keys(&provider_map, &["1".to_string(), "2".to_string()])
                .expect("combined refs should resolve");
        assert!(matches!(combined, ProviderEntryView::Owned(_)));
        assert_eq!(combined.provider_count(), 2);
    }

    #[test]
    fn serving_binary_combined_copy_emits_forward_and_reverse_blocks() {
        let input = concat!(
            "1\t10\t2\t000000000000000000000000000000a1\n",
            "1\t10\t2\t000000000000000000000000000000a2\n",
            "1\t20\t1\t000000000000000000000000000000a3\n",
            "2\t10\t2\t000000000000000000000000000000a1\n",
            "2\t30\t4\t000000000000000000000000000000a4\n",
        );
        let mut reader = BufReader::new(input.as_bytes());
        let mut writer = CountingWriter::new(Vec::new());

        let payload = write_serving_binary_combined_copy_from_reader(
            &mut reader,
            &mut writer,
            None,
            ServingBinaryTargetCopyFormat::Text,
        )
        .unwrap();
        let output = String::from_utf8(writer.inner).unwrap();
        let kinds: Vec<&str> = output
            .lines()
            .map(|line| line.split('\t').next().unwrap_or(""))
            .collect();

        assert_eq!(payload["name"], "serving_binary_combined");
        assert_eq!(payload["row_count"], 5);
        assert_eq!(payload["by_code"]["row_count"], 5);
        assert_eq!(payload["by_code"]["code_count"], 2);
        assert_eq!(payload["by_code"]["group_count"], 4);
        assert_eq!(payload["by_code"]["provider_set_count"], 3);
        assert_eq!(payload["by_provider_set"]["row_count"], 5);
        assert_eq!(payload["by_provider_set"]["provider_set_count"], 3);
        assert_eq!(payload["by_provider_set"]["code_count"], 2);
        assert_eq!(payload["by_provider_set"]["pattern_count"], 4);
        assert!(kinds.contains(&PTG2_SERVING_BINARY_BY_CODE_GROUPED_KIND));
        assert!(kinds.contains(&PTG2_SERVING_BINARY_BY_CODE_DICTIONARY_KIND));
        assert!(kinds.contains(&PTG2_SERVING_BINARY_PROVIDER_COUNT_DICTIONARY_KIND));
        assert!(kinds.contains(&PTG2_SERVING_BINARY_BY_PROVIDER_SET_KIND));
        assert!(kinds.contains(&PTG2_SERVING_BINARY_BY_PROVIDER_SET_DICTIONARY_KIND));
    }

    fn test_price_id(last_byte: u8) -> [u8; GLOBAL_ID_BYTES] {
        let mut out = [0u8; GLOBAL_ID_BYTES];
        out[GLOBAL_ID_BYTES - 1] = last_byte;
        out
    }

    fn append_pg_binary_field(payload: &mut Vec<u8>, field: &[u8]) {
        payload.extend_from_slice(&(field.len() as i32).to_be_bytes());
        payload.extend_from_slice(field);
    }

    fn pg_binary_serving_copy(rows: &[(i32, i32, i32, [u8; GLOBAL_ID_BYTES])]) -> Vec<u8> {
        let mut payload = Vec::new();
        payload.extend_from_slice(b"PGCOPY\n\xff\r\n\0");
        payload.extend_from_slice(&0i32.to_be_bytes());
        payload.extend_from_slice(&0i32.to_be_bytes());
        for (first_key, second_key, provider_count, price_set_id) in rows {
            payload.extend_from_slice(&4i16.to_be_bytes());
            append_pg_binary_field(&mut payload, &first_key.to_be_bytes());
            append_pg_binary_field(&mut payload, &second_key.to_be_bytes());
            append_pg_binary_field(&mut payload, &provider_count.to_be_bytes());
            append_pg_binary_field(&mut payload, price_set_id);
        }
        payload.extend_from_slice(&(-1i16).to_be_bytes());
        payload
    }

    fn pg_binary_price_set_atom_copy(
        rows: &[([u8; GLOBAL_ID_BYTES], [u8; GLOBAL_ID_BYTES])],
    ) -> Vec<u8> {
        let mut payload = Vec::new();
        payload.extend_from_slice(b"PGCOPY\n\xff\r\n\0");
        payload.extend_from_slice(&0i32.to_be_bytes());
        payload.extend_from_slice(&0i32.to_be_bytes());
        for (price_set_id, price_atom_id) in rows {
            payload.extend_from_slice(&2i16.to_be_bytes());
            append_pg_binary_field(&mut payload, price_set_id);
            append_pg_binary_field(&mut payload, price_atom_id);
        }
        payload.extend_from_slice(&(-1i16).to_be_bytes());
        payload
    }

    fn prefixed_test_id(prefix: u16, last_byte: u8) -> [u8; GLOBAL_ID_BYTES] {
        let mut value = [0u8; GLOBAL_ID_BYTES];
        value[..2].copy_from_slice(&prefix.to_be_bytes());
        value[GLOBAL_ID_BYTES - 1] = last_byte;
        value
    }

    fn test_id_hex(value: &[u8; GLOBAL_ID_BYTES]) -> String {
        value.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    #[test]
    fn serving_binary_by_code_pg_binary_copy_matches_text_shape() {
        let input = pg_binary_serving_copy(&[
            (1, 10, 2, test_price_id(0xa1)),
            (1, 10, 2, test_price_id(0xa2)),
            (1, 20, 1, test_price_id(0xa3)),
            (2, 10, 2, test_price_id(0xa1)),
        ]);
        let mut reader = Cursor::new(input);
        let mut writer = CountingWriter::new(Vec::new());

        let payload = write_serving_binary_by_code_copy_from_pg_binary_reader(
            &mut reader,
            &mut writer,
            None,
            ServingBinaryTargetCopyFormat::Text,
        )
        .unwrap();
        let output = String::from_utf8(writer.inner).unwrap();
        let kinds: Vec<&str> = output
            .lines()
            .map(|line| line.split('\t').next().unwrap_or(""))
            .collect();

        assert_eq!(payload["name"], "serving_binary_by_code");
        assert_eq!(payload["source_copy_format"], "postgres_binary");
        assert_eq!(payload["row_count"], 4);
        assert_eq!(payload["code_count"], 2);
        assert_eq!(payload["group_count"], 3);
        assert_eq!(payload["provider_set_count"], 2);
        assert_eq!(payload["price_set_count"], 3);
        assert!(kinds.contains(&PTG2_SERVING_BINARY_BY_CODE_GROUPED_KIND));
        assert!(kinds.contains(&PTG2_SERVING_BINARY_BY_CODE_DICTIONARY_KIND));
        assert!(kinds.contains(&PTG2_SERVING_BINARY_PROVIDER_COUNT_DICTIONARY_KIND));
    }

    #[test]
    fn serving_binary_by_provider_set_pg_binary_copy_matches_text_shape() {
        let input = pg_binary_serving_copy(&[
            (10, 1, 2, test_price_id(0xa1)),
            (10, 1, 2, test_price_id(0xa2)),
            (10, 2, 2, test_price_id(0xa1)),
            (20, 1, 1, test_price_id(0xa3)),
        ]);
        let mut reader = Cursor::new(input);
        let mut writer = CountingWriter::new(Vec::new());

        let payload = write_serving_binary_by_provider_set_copy_from_pg_binary_reader(
            &mut reader,
            &mut writer,
            None,
            ServingBinaryTargetCopyFormat::Text,
        )
        .unwrap();
        let output = String::from_utf8(writer.inner).unwrap();
        let kinds: Vec<&str> = output
            .lines()
            .map(|line| line.split('\t').next().unwrap_or(""))
            .collect();

        assert_eq!(payload["name"], "serving_binary_by_provider_set");
        assert_eq!(payload["source_copy_format"], "postgres_binary");
        assert_eq!(payload["row_count"], 4);
        assert_eq!(payload["provider_set_count"], 2);
        assert_eq!(payload["code_count"], 2);
        assert_eq!(payload["price_set_count"], 3);
        assert!(kinds.contains(&PTG2_SERVING_BINARY_BY_PROVIDER_SET_KIND));
        assert!(kinds.contains(&PTG2_SERVING_BINARY_BY_PROVIDER_SET_DICTIONARY_KIND));
    }

    #[test]
    fn serving_binary_combined_pg_binary_copy_emits_forward_and_reverse_blocks() {
        let input = pg_binary_serving_copy(&[
            (1, 10, 2, test_price_id(0xa1)),
            (1, 10, 2, test_price_id(0xa2)),
            (1, 20, 1, test_price_id(0xa3)),
            (2, 10, 2, test_price_id(0xa1)),
            (2, 30, 4, test_price_id(0xa4)),
        ]);
        let mut reader = Cursor::new(input);
        let mut writer = CountingWriter::new(Vec::new());

        let payload = write_serving_binary_combined_copy_from_pg_binary_reader(
            &mut reader,
            &mut writer,
            None,
            ServingBinaryTargetCopyFormat::Text,
        )
        .unwrap();
        let output = String::from_utf8(writer.inner).unwrap();
        let kinds: Vec<&str> = output
            .lines()
            .map(|line| line.split('\t').next().unwrap_or(""))
            .collect();

        assert_eq!(payload["name"], "serving_binary_combined");
        assert_eq!(payload["source_copy_format"], "postgres_binary");
        assert_eq!(payload["row_count"], 5);
        assert_eq!(payload["by_code"]["row_count"], 5);
        assert_eq!(payload["by_code"]["code_count"], 2);
        assert_eq!(payload["by_code"]["group_count"], 4);
        assert_eq!(payload["by_code"]["provider_set_count"], 3);
        assert_eq!(payload["by_provider_set"]["row_count"], 5);
        assert_eq!(payload["by_provider_set"]["provider_set_count"], 3);
        assert_eq!(payload["by_provider_set"]["code_count"], 2);
        assert_eq!(payload["by_provider_set"]["pattern_count"], 4);
        assert!(kinds.contains(&PTG2_SERVING_BINARY_BY_CODE_GROUPED_KIND));
        assert!(kinds.contains(&PTG2_SERVING_BINARY_BY_CODE_DICTIONARY_KIND));
        assert!(kinds.contains(&PTG2_SERVING_BINARY_PROVIDER_COUNT_DICTIONARY_KIND));
        assert!(kinds.contains(&PTG2_SERVING_BINARY_BY_PROVIDER_SET_KIND));
        assert!(kinds.contains(&PTG2_SERVING_BINARY_BY_PROVIDER_SET_DICTIONARY_KIND));
    }

    #[test]
    fn serving_binary_price_set_atoms_copy_matches_v2_payload_records() {
        let price_set_a = "123400000000000000000000000000a1";
        let price_set_b = "123400000000000000000000000000a2";
        let price_set_c = "123500000000000000000000000000a3";
        let atom_a = "000000000000000000000000000000c1";
        let atom_b = "000000000000000000000000000000c2";
        let atom_c = "000000000000000000000000000000c3";
        let input = format!(
            "{price_set_a}\t{atom_a}\n{price_set_a}\t{atom_b}\n{price_set_b}\t{atom_c}\n{price_set_c}\t{atom_c}\n"
        );
        let mut reader = BufReader::new(input.as_bytes());
        let mut writer = CountingWriter::new(Vec::new());

        let summary = write_serving_binary_price_set_atoms_copy_from_reader(
            &mut reader,
            &mut writer,
            ServingBinaryTargetCopyFormat::Text,
        )
        .unwrap();
        let output_bytes = writer.inner;
        let output = String::from_utf8(output_bytes.clone()).unwrap();
        let rows: Vec<Vec<&str>> = output
            .lines()
            .map(|line| line.split('\t').collect())
            .collect();

        assert_eq!(rows.len(), 2);
        assert_eq!(
            rows[0],
            vec![
                "price_set_atoms_by_id_v2".to_string(),
                "4660".to_string(),
                "0".to_string(),
                "2".to_string(),
                format!("\\\\x{price_set_a}02{atom_a}{atom_b}{price_set_b}01{atom_c}"),
                "none".to_string(),
                "0".to_string(),
            ]
        );
        assert_eq!(rows[1][0], "price_set_atoms_by_id_v2");
        assert_eq!(rows[1][1], "4661");
        assert_eq!(rows[1][2], "0");
        assert_eq!(rows[1][3], "1");
        assert_eq!(rows[1][4], format!("\\\\x{price_set_c}01{atom_c}"));
        assert_eq!(summary["format"], "price_set_atoms_by_id_v2");
        assert_eq!(summary["artifact_kind"], "price_set_atoms_by_id_v2");
        assert_eq!(summary["id_prefix_bytes"], 2);
        assert_eq!(summary["id_bucket_count"], 65536);
        assert_eq!(summary["id_block_count"], 2);
        assert_eq!(summary["price_set_count"], 3);
        assert_eq!(summary["atom_ref_count"], 4);
        assert_eq!(summary["copied_records"], 2);
        assert_eq!(summary["byte_count"], output_bytes.len());
    }

    #[test]
    fn serving_binary_price_set_atoms_pg_binary_matches_text_payload() {
        let rows = [
            (prefixed_test_id(0x1234, 0xa1), test_price_id(0xc1)),
            (prefixed_test_id(0x1234, 0xa1), test_price_id(0xc2)),
            (prefixed_test_id(0x1234, 0xa2), test_price_id(0xc3)),
        ];
        let text_input = rows
            .iter()
            .map(|(price_set_id, price_atom_id)| {
                format!(
                    "{}\t{}\n",
                    test_id_hex(price_set_id),
                    test_id_hex(price_atom_id)
                )
            })
            .collect::<String>();
        let mut text_reader = BufReader::new(text_input.as_bytes());
        let mut text_writer = CountingWriter::new(Vec::new());
        write_serving_binary_price_set_atoms_copy_from_reader(
            &mut text_reader,
            &mut text_writer,
            ServingBinaryTargetCopyFormat::Text,
        )
        .unwrap();
        let binary_input = pg_binary_price_set_atom_copy(&rows);
        let mut binary_reader = Cursor::new(binary_input);
        let mut binary_writer = CountingWriter::new(Vec::new());

        let summary = write_serving_binary_price_set_atoms_copy_from_pg_binary_reader(
            &mut binary_reader,
            &mut binary_writer,
            ServingBinaryTargetCopyFormat::Text,
        )
        .unwrap();

        assert_eq!(binary_writer.inner, text_writer.inner);
        assert_eq!(summary["source_copy_format"], "postgres_binary");
        assert_eq!(summary["price_set_count"], 2);
        assert_eq!(summary["atom_ref_count"], 3);
    }

    #[test]
    fn serving_binary_price_set_atoms_emits_exact_pg_binary_record() {
        let price_set_id = prefixed_test_id(0x1234, 0xa1);
        let price_atom_id = test_price_id(0xc1);
        let input = format!(
            "{}\t{}\n",
            test_id_hex(&price_set_id),
            test_id_hex(&price_atom_id)
        );
        let mut input_reader = BufReader::new(input.as_bytes());
        let mut output_writer = CountingWriter::new(Vec::new());

        let summary = write_serving_binary_price_set_atoms_copy_from_reader(
            &mut input_reader,
            &mut output_writer,
            ServingBinaryTargetCopyFormat::Binary,
        )
        .unwrap();
        let mut output_reader = Cursor::new(output_writer.inner);
        read_pg_binary_copy_header(&mut output_reader).unwrap();

        assert_eq!(read_i16_be(&mut output_reader).unwrap(), Some(7));
        assert_eq!(
            read_pg_binary_field(&mut output_reader).unwrap(),
            PTG2_SERVING_BINARY_PRICE_SET_ATOMS_BY_ID_V2_KIND.as_bytes()
        );
        assert_eq!(
            pg_binary_i32(
                &read_pg_binary_field(&mut output_reader).unwrap(),
                "block_key"
            )
            .unwrap(),
            0x1234
        );
        assert_eq!(
            pg_binary_i32(
                &read_pg_binary_field(&mut output_reader).unwrap(),
                "block_no"
            )
            .unwrap(),
            0
        );
        assert_eq!(
            pg_binary_i32(
                &read_pg_binary_field(&mut output_reader).unwrap(),
                "entry_count"
            )
            .unwrap(),
            1
        );
        let mut expected_payload = Vec::from(price_set_id);
        expected_payload.push(1);
        expected_payload.extend_from_slice(&price_atom_id);
        assert_eq!(
            read_pg_binary_field(&mut output_reader).unwrap(),
            expected_payload
        );
        assert_eq!(read_pg_binary_field(&mut output_reader).unwrap(), b"none");
        assert_eq!(
            pg_binary_i32(
                &read_pg_binary_field(&mut output_reader).unwrap(),
                "raw_payload_bytes"
            )
            .unwrap(),
            0
        );
        assert_eq!(read_i16_be(&mut output_reader).unwrap(), Some(-1));
        assert_eq!(summary["target_copy_format"], "postgres_binary");
    }

    #[test]
    fn serving_binary_price_set_atoms_rejects_oversized_membership() {
        let mut state = ServingBinaryPriceSetAtomState::new(32);
        let mut writer = Vec::new();
        let error = state
            .push_row(
                &mut writer,
                ServingBinaryTargetCopyFormat::Text,
                ServingBinaryPriceSetAtomInputRow {
                    price_set_id: prefixed_test_id(0x1234, 0xa1),
                    price_atom_id: test_price_id(0xc1),
                },
            )
            .unwrap_err();

        assert!(error
            .to_string()
            .contains("maximum single-membership size of 32 bytes"));
        assert!(state.current_atom_ids.is_empty());
    }

    #[test]
    fn serving_binary_price_set_atoms_rejects_unordered_rows() {
        let price_set_id = prefixed_test_id(0x1234, 0xa1);
        let input = format!(
            "{}\t{}\n{}\t{}\n",
            test_id_hex(&price_set_id),
            test_id_hex(&test_price_id(0xc2)),
            test_id_hex(&price_set_id),
            test_id_hex(&test_price_id(0xc1)),
        );
        let mut reader = BufReader::new(input.as_bytes());
        let mut writer = CountingWriter::new(Vec::new());

        let error = write_serving_binary_price_set_atoms_copy_from_reader(
            &mut reader,
            &mut writer,
            ServingBinaryTargetCopyFormat::Text,
        )
        .unwrap_err();

        assert!(error
            .to_string()
            .contains("ordered by price_atom_global_id_128"));
    }

    #[test]
    fn manifest_sidecar_collector_sorts_and_merges_members() {
        let provider_set_id = GlobalId128([5; GLOBAL_ID_BYTES]);
        let mut collector = ManifestSidecarCollector::default();

        collector
            .record_provider_set(provider_set_id, &[20, 10, 20], &[1003002106, 1003007311])
            .unwrap();

        let entries = collector.provider_forward_entries().unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].owner, provider_set_id);
        assert_eq!(entries[0].members.len(), 2);
        assert!(entries[0].members[0] < entries[0].members[1]);
        let inverted = collector.provider_inverted_entries().unwrap();
        assert_eq!(inverted.len(), 2);
        assert!(inverted
            .iter()
            .all(|entry| entry.members == vec![provider_set_id]));
        let provider_npi = collector.provider_npi_entries().unwrap();
        assert_eq!(provider_npi.len(), 1);
        assert_eq!(provider_npi[0].owner, provider_set_id);
        assert_eq!(provider_npi[0].members.len(), 2);
    }

    #[test]
    fn manifest_sidecar_collector_can_spill_members_before_sidecar_write() {
        let provider_set_id = GlobalId128([5; GLOBAL_ID_BYTES]);
        let mut collector = ManifestSidecarCollector {
            spools: Some(ManifestSidecarSpools::all().unwrap()),
            ..ManifestSidecarCollector::default()
        };

        collector
            .record_provider_set(provider_set_id, &[20, 10, 20], &[1003002106, 1003007311])
            .unwrap();

        assert!(collector.provider_forward.is_empty());
        assert!(collector.provider_inverted.is_empty());
        assert!(collector.provider_npi.is_empty());
        let entries = collector.provider_forward_entries().unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].owner, provider_set_id);
        assert_eq!(entries[0].members.len(), 2);
        let inverted = collector.provider_inverted_entries().unwrap();
        assert_eq!(inverted.len(), 2);
        assert!(inverted
            .iter()
            .all(|entry| entry.members == vec![provider_set_id]));
    }

    #[test]
    fn manifest_sidecar_spools_only_open_configured_artifacts() {
        let paths = CopyPathConfig {
            manifest_provider_forward_sidecar: Some("provider-forward.ptg2sc".to_string()),
            ..CopyPathConfig::default()
        };

        let spools = ManifestSidecarSpools::for_paths(&paths).unwrap();

        assert!(spools.provider_forward.is_some());
        assert!(spools.provider_inverted.is_none());
        assert!(spools.provider_npi.is_none());
        assert!(spools.price_forward.is_none());
    }

    #[test]
    fn configured_manifest_sidecars_write_independent_spools_in_parallel() {
        let base =
            std::env::temp_dir().join(format!("ptg2-parallel-sidecar-test-{}", std::process::id()));
        let forward_path = base.with_extension("forward.ptg2sc");
        let inverted_path = base.with_extension("inverted.ptg2sc");
        let paths = CopyPathConfig {
            manifest_provider_forward_sidecar: Some(forward_path.display().to_string()),
            manifest_provider_inverted_sidecar: Some(inverted_path.display().to_string()),
            ..CopyPathConfig::default()
        };
        let mut collector = ManifestSidecarCollector {
            spools: Some(ManifestSidecarSpools::for_paths(&paths).unwrap()),
            ..ManifestSidecarCollector::default()
        };
        collector
            .record_provider_set(
                GlobalId128([5; GLOBAL_ID_BYTES]),
                &[20, 10, 20],
                &[1003002106, 1003007311],
            )
            .unwrap();

        let results = configured_spooled_manifest_sidecars(&paths, &mut collector)
            .unwrap()
            .unwrap();

        assert_eq!(results.len(), 2);
        assert_eq!(
            results[0].record_kind,
            "manifest_provider_forward_sidecar_file"
        );
        assert_eq!(results[0].entry_count, 1);
        assert_eq!(
            results[1].record_kind,
            "manifest_provider_inverted_sidecar_file"
        );
        assert_eq!(results[1].entry_count, 2);
        assert_eq!(&std::fs::read(&forward_path).unwrap()[..8], b"PTG2MNDS");
        assert_eq!(&std::fs::read(&inverted_path).unwrap()[..8], b"PTG2MNDS");
        let _ = std::fs::remove_file(forward_path);
        let _ = std::fs::remove_file(inverted_path);
    }

    #[test]
    fn manifest_dense_sidecar_external_sort_matches_in_memory_format() {
        let output_prefix = std::env::temp_dir().join(format!(
            "ptg2-dense-sidecar-sort-test-{}",
            std::process::id()
        ));
        let in_memory_path = output_prefix.with_extension("memory.ptg2sc");
        let external_path = output_prefix.with_extension("external.ptg2sc");
        let owner_a = GlobalId128([1; GLOBAL_ID_BYTES]);
        let owner_b = GlobalId128([2; GLOBAL_ID_BYTES]);
        let member_a = GlobalId128([7; GLOBAL_ID_BYTES]);
        let member_b = GlobalId128([8; GLOBAL_ID_BYTES]);
        let member_c = GlobalId128([9; GLOBAL_ID_BYTES]);
        let pairs = [
            (owner_b, member_c),
            (owner_a, member_b),
            (owner_a, member_a),
            (owner_b, member_a),
            (owner_a, member_b),
            (owner_b, member_b),
        ];
        let mut in_memory_spool = ManifestPairSpool::new("dense_parity_memory").unwrap();
        let mut external_spool = ManifestPairSpool::new("dense_parity_external").unwrap();
        for (owner, member) in pairs {
            in_memory_spool.push(owner, member).unwrap();
            external_spool.push(owner, member).unwrap();
        }

        let in_memory_metrics = in_memory_spool
            .write_dense_sidecar_with_chunk_bytes(in_memory_path.to_str().unwrap(), usize::MAX)
            .unwrap();
        let external_metrics = external_spool
            .write_dense_sidecar_with_chunk_bytes(
                external_path.to_str().unwrap(),
                MANIFEST_PAIR_RECORD_BYTES * 2,
            )
            .unwrap();

        assert_eq!(external_metrics, in_memory_metrics);
        assert_eq!(external_metrics, (2, 5));
        assert_eq!(
            std::fs::read(&external_path).unwrap(),
            std::fs::read(&in_memory_path).unwrap()
        );
        let _ = std::fs::remove_file(in_memory_path);
        let _ = std::fs::remove_file(external_path);
    }

    #[test]
    fn manifest_copy_merge_dedupes_by_kind_key() {
        let base = std::env::temp_dir().join(format!("ptg2-merge-test-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&base);
        let input_a = base.join("a.copy");
        let input_b = base.join("b.copy");
        let output = base.join("out.copy");
        std::fs::write(&input_a, b"b\t2\nmanifest\t1\t2\t3\t4\t5\t6\t7\t\\N\n").unwrap();
        std::fs::write(&input_b, b"a\t1\nmanifest\t1\t2\t3\t4\t5\t6\t7\ttrace\n").unwrap();

        merge_manifest_copy_files(
            "manifest_serving",
            &output,
            &[
                input_a.to_string_lossy().to_string(),
                input_b.to_string_lossy().to_string(),
            ],
        )
        .unwrap();

        let merged = std::fs::read_to_string(&output).unwrap();
        assert_eq!(merged, "a\t1\nb\t2\nmanifest\t1\t2\t3\t4\t5\t6\t7\ttrace\n");
        let _ = std::fs::remove_dir_all(base);
    }

    #[test]
    fn manifest_copy_merge_dedupes_lean_serving_by_full_row() {
        let base =
            std::env::temp_dir().join(format!("ptg2-lean-merge-test-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&base);
        let input_a = base.join("a.copy");
        let input_b = base.join("b.copy");
        let output = base.join("out.copy");
        let row_a = "plan\tCPT\t29888\tprovider-set\t2\tprice-set-a\n";
        let row_b = "plan\tCPT\t29888\tprovider-set\t2\tprice-set-b\n";
        std::fs::write(&input_a, format!("{row_b}{row_b}")).unwrap();
        std::fs::write(&input_b, row_a).unwrap();

        merge_manifest_copy_files(
            "manifest_lean_serving",
            &output,
            &[
                input_a.to_string_lossy().to_string(),
                input_b.to_string_lossy().to_string(),
            ],
        )
        .unwrap();

        let merged = std::fs::read_to_string(&output).unwrap();
        assert_eq!(merged, format!("{row_a}{row_b}"));
        let _ = std::fs::remove_dir_all(base);
    }

    #[test]
    fn manifest_copy_merge_dedupes_price_set_atoms_by_full_pair() {
        let base = std::env::temp_dir().join(format!(
            "ptg2-price-set-atom-merge-test-{}",
            std::process::id()
        ));
        let _ = std::fs::create_dir_all(&base);
        let input_a = base.join("a.copy");
        let input_b = base.join("b.copy");
        let output = base.join("out.copy");
        let atom_a = "price-set-a\tatom-a\n";
        let atom_b = "price-set-a\tatom-b\n";
        std::fs::write(&input_a, format!("{atom_b}{atom_b}")).unwrap();
        std::fs::write(&input_b, atom_a).unwrap();

        merge_manifest_copy_files(
            "price_set_atom",
            &output,
            &[
                input_a.to_string_lossy().to_string(),
                input_b.to_string_lossy().to_string(),
            ],
        )
        .unwrap();

        let merged = std::fs::read_to_string(&output).unwrap();
        assert_eq!(merged, format!("{atom_a}{atom_b}"));
        let _ = std::fs::remove_dir_all(base);
    }

    #[test]
    fn manifest_copy_merge_parallel_chunk_sort_matches_serial_output() {
        let base =
            std::env::temp_dir().join(format!("ptg2-merge-parallel-test-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&base);
        let input_a = base.join("a.copy");
        let input_b = base.join("b.copy");
        let serial_output = base.join("serial.copy");
        let parallel_output = base.join("parallel.copy");
        let payload_a = "a".repeat(700_000);
        let payload_b = "b".repeat(700_000);
        let payload_c = "c".repeat(700_000);
        std::fs::write(
            &input_a,
            format!(
                "g2\t200\t{payload_a}\n\
                 g1\t100\t{payload_b}\n\
                 g3\t300\t{payload_c}\n\
                 g1\t100\t{payload_b}\n"
            ),
        )
        .unwrap();
        std::fs::write(
            &input_b,
            format!(
                "g4\t400\t{payload_a}\n\
                 g2\t200\t{payload_a}\n\
                 g5\t500\t{payload_c}\n"
            ),
        )
        .unwrap();

        std::env::remove_var("HLTHPRT_PTG2_MANIFEST_MERGE_SORT_WORKERS");
        std::env::remove_var("HLTHPRT_PTG2_MANIFEST_MERGE_CHUNK_BYTES");
        merge_manifest_copy_files(
            "provider_group_member",
            &serial_output,
            &[
                input_a.to_string_lossy().to_string(),
                input_b.to_string_lossy().to_string(),
            ],
        )
        .unwrap();

        std::env::set_var("HLTHPRT_PTG2_MANIFEST_MERGE_SORT_WORKERS", "2");
        std::env::set_var("HLTHPRT_PTG2_MANIFEST_MERGE_CHUNK_BYTES", "1");
        merge_manifest_copy_files(
            "provider_group_member",
            &parallel_output,
            &[
                input_a.to_string_lossy().to_string(),
                input_b.to_string_lossy().to_string(),
            ],
        )
        .unwrap();
        std::env::remove_var("HLTHPRT_PTG2_MANIFEST_MERGE_SORT_WORKERS");
        std::env::remove_var("HLTHPRT_PTG2_MANIFEST_MERGE_CHUNK_BYTES");

        let serial = std::fs::read_to_string(&serial_output).unwrap();
        let parallel = std::fs::read_to_string(&parallel_output).unwrap();
        assert_eq!(parallel, serial);
        assert_eq!(parallel.lines().count(), 5);
        let _ = std::fs::remove_dir_all(base);
    }

    #[test]
    fn serving_binary_by_code_copy_uses_compact_payload_and_dictionary() {
        let base = std::env::temp_dir().join(format!(
            "ptg2-serving-binary-code-test-{}",
            std::process::id()
        ));
        let _ = std::fs::create_dir_all(&base);
        let input = base.join("by_code_in.copy");
        let output = base.join("by_code_out.copy");
        let price_a = "0000000000000000000000000000000a";
        let price_b = "0000000000000000000000000000000b";
        std::fs::write(
            &input,
            format!("7\t10\t2\t{price_a}\n7\t12\t3\t{price_b}\n"),
        )
        .unwrap();

        let summary = write_serving_binary_by_code_copy_from_key_copy(&input, &output).unwrap();

        assert_eq!(summary["row_count"], 2);
        assert_eq!(summary["group_count"], 2);
        assert_eq!(summary["provider_set_count"], 2);
        assert_eq!(summary["copy_record_count"], 3);
        let output_text = std::fs::read_to_string(&output).unwrap();
        let rows: Vec<Vec<&str>> = output_text
            .lines()
            .map(|line| line.split('\t').collect())
            .collect();
        assert_eq!(rows.len(), 3);
        assert_eq!(
            rows[0],
            vec![
                "by_code_grouped",
                "7",
                "0",
                "2",
                "\\\\x0a0100020101",
                "none",
                "0"
            ]
        );
        assert_eq!(rows[1][0], "by_code_price_dictionary");
        assert_eq!(rows[1][1], "0");
        assert_eq!(rows[1][2], "0");
        assert_eq!(rows[1][3], "2");
        assert_eq!(rows[1][4], format!("\\\\x{price_a}{price_b}"));
        assert_eq!(rows[1][5], "none");
        assert_eq!(rows[1][6], "0");
        assert_eq!(
            rows[2],
            vec![
                "provider_set_count_dictionary",
                "0",
                "0",
                "2",
                "\\\\x0a020203",
                "none",
                "0"
            ]
        );
        let _ = std::fs::remove_dir_all(base);
    }

    #[test]
    fn serving_binary_provider_set_copy_groups_repeated_code_patterns() {
        let base = std::env::temp_dir().join(format!(
            "ptg2-serving-binary-provider-test-{}",
            std::process::id()
        ));
        let _ = std::fs::create_dir_all(&base);
        let input = base.join("by_provider_in.copy");
        let output = base.join("by_provider_out.copy");
        let price_a = "0000000000000000000000000000000a";
        std::fs::write(&input, format!("5\t7\t2\t{price_a}\n5\t8\t2\t{price_a}\n")).unwrap();

        let summary =
            write_serving_binary_by_provider_set_copy_from_key_copy(&input, &output).unwrap();

        assert_eq!(summary["row_count"], 2);
        assert_eq!(summary["pattern_count"], 1);
        let output_text = std::fs::read_to_string(&output).unwrap();
        let rows: Vec<Vec<&str>> = output_text
            .lines()
            .map(|line| line.split('\t').collect())
            .collect();
        assert_eq!(rows.len(), 2);
        assert_eq!(
            rows[0],
            vec![
                "by_provider_set",
                "5",
                "0",
                "1",
                "\\\\x020701010200",
                "none",
                "0"
            ]
        );
        assert_eq!(rows[1][0], "by_provider_set_price_dictionary");
        assert_eq!(rows[1][1], "0");
        assert_eq!(rows[1][2], "0");
        assert_eq!(rows[1][3], "1");
        assert_eq!(rows[1][4], format!("\\\\x{price_a}"));
        assert_eq!(rows[1][5], "none");
        assert_eq!(rows[1][6], "0");
        let _ = std::fs::remove_dir_all(base);
    }

    #[test]
    fn raw_rate_chunks_flush_when_byte_limit_is_reached() {
        let payload = br#"{
            "billing_code_type": "CPT",
            "billing_code": "99213",
            "negotiated_rates": [
                {"provider_references":[7],"negotiated_prices":[{"negotiated_type":"negotiated","negotiated_rate":100}]},
                {"provider_references":[7],"negotiated_prices":[{"negotiated_type":"negotiated","negotiated_rate":101}]},
                {"provider_references":[7],"negotiated_prices":[{"negotiated_type":"negotiated","negotiated_rate":102}]}
            ]
        }"#;
        let mut reader = JsonStreamReader::new(&payload[..]);
        let (tx, rx) = bounded::<WorkerJob>(10);
        let (_event_tx, event_rx) = bounded::<CopyFileEvent>(10);
        let mut writer = Vec::new();
        let mut producer_blocked_micros = 0u128;
        let mut stats = RawChunkStats::default();

        let mut enqueue_io = InNetworkEnqueueIo {
            tx: &tx,
            event_rx: &event_rx,
            writer: &mut writer,
            producer_blocked_micros: &mut producer_blocked_micros,
            raw_chunk_stats: &mut stats,
        };
        let rate_count = enqueue_in_network_struson(
            &mut reader,
            &mut enqueue_io,
            InNetworkEnqueueOptions {
                chunk_size: 100,
                raw_chunk_byte_limit: 1,
                parse_in_workers: true,
            },
        )
        .unwrap();

        drop(tx);
        let jobs: Vec<_> = rx.try_iter().collect();
        assert_eq!(rate_count, 3);
        assert_eq!(stats.chunk_count, 3);
        assert_eq!(stats.max_rates, 1);
        assert!(stats.max_bytes > 0);
        assert!(jobs.iter().all(|job| matches!(
            job,
            WorkerJob::RawRates { raw_rates, .. } if raw_rates.len() == 1
        )));
    }
}

fn main() -> io::Result<()> {
    let mut args = env::args().skip(1);
    let first_arg = args.next().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "usage: ptg2_scanner [--compact-serving] <path> <top_level_array>...",
        )
    })?;
    if first_arg == "--canon-version" {
        if args.next().is_some() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "usage: ptg2_scanner --canon-version",
            ));
        }
        println!("{}", canon_version_json());
        return Ok(());
    }
    if first_arg == "--compact-serving" {
        let compact_path = args.next().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "usage: ptg2_scanner --compact-serving <path>",
            )
        })?;
        return scan_compact(Path::new(&compact_path));
    }
    if first_arg == "--merge-manifest-copy" {
        let kind = args.next().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "usage: ptg2_scanner --merge-manifest-copy <kind> <output_path> <input_path>...",
            )
        })?;
        if !matches!(
            kind.as_str(),
            "manifest_serving"
                | "manifest_lean_serving"
                | "price_atom"
                | "price_set_atom"
                | "provider_group_member"
        ) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "manifest copy merge kind must be manifest_serving, manifest_lean_serving, price_atom, price_set_atom, or provider_group_member",
            ));
        }
        let output_path = args.next().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "usage: ptg2_scanner --merge-manifest-copy <kind> <output_path> <input_path>...",
            )
        })?;
        let input_paths: Vec<String> = args.collect();
        return merge_manifest_copy_files(&kind, Path::new(&output_path), &input_paths);
    }
    if first_arg == "--provider-membership-sidecars" {
        let group_npi_path = args.next().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "usage: ptg2_scanner --provider-membership-sidecars <group_npi_path> <npi_group_path> <npi_scope_copy_path> <member_copy_path>...",
            )
        })?;
        let npi_group_path = args.next().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "usage: ptg2_scanner --provider-membership-sidecars <group_npi_path> <npi_group_path> <npi_scope_copy_path> <member_copy_path>...",
            )
        })?;
        let npi_scope_copy_path = args.next().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "usage: ptg2_scanner --provider-membership-sidecars <group_npi_path> <npi_group_path> <npi_scope_copy_path> <member_copy_path>...",
            )
        })?;
        let input_paths: Vec<String> = args.collect();
        return write_provider_membership_sidecars(
            Path::new(&group_npi_path),
            Path::new(&npi_group_path),
            Path::new(&npi_scope_copy_path),
            &input_paths,
        );
    }
    if first_arg == "--serving-sidecar-from-key-copy" {
        let kind = args.next().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "usage: ptg2_scanner --serving-sidecar-from-key-copy <by_code|by_provider_set> <input_copy_path> <output_path>",
            )
        })?;
        let input_path = args.next().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "usage: ptg2_scanner --serving-sidecar-from-key-copy <by_code|by_provider_set> <input_copy_path> <output_path>",
            )
        })?;
        let output_path = args.next().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "usage: ptg2_scanner --serving-sidecar-from-key-copy <by_code|by_provider_set> <input_copy_path> <output_path>",
            )
        })?;
        if args.next().is_some() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "usage: ptg2_scanner --serving-sidecar-from-key-copy <by_code|by_provider_set> <input_copy_path> <output_path>",
            ));
        }
        return write_serving_sidecar_from_key_copy(
            &kind,
            Path::new(&input_path),
            Path::new(&output_path),
        );
    }
    if first_arg == "--serving-binary-copy-from-key-copy" {
        let kind = args.next().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "usage: ptg2_scanner --serving-binary-copy-from-key-copy <by_code|by_provider_set> <input_copy_path> <output_copy_path>",
            )
        })?;
        let input_path = args.next().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "usage: ptg2_scanner --serving-binary-copy-from-key-copy <by_code|by_provider_set> <input_copy_path> <output_copy_path>",
            )
        })?;
        let output_path = args.next().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "usage: ptg2_scanner --serving-binary-copy-from-key-copy <by_code|by_provider_set> <input_copy_path> <output_copy_path>",
            )
        })?;
        if args.next().is_some() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "usage: ptg2_scanner --serving-binary-copy-from-key-copy <by_code|by_provider_set> <input_copy_path> <output_copy_path>",
            ));
        }
        return write_serving_binary_copy_from_key_copy(
            &kind,
            Path::new(&input_path),
            Path::new(&output_path),
        );
    }
    if first_arg == "--serving-binary-copy-from-key-copy-stdio" {
        let kind = args.next().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "usage: ptg2_scanner --serving-binary-copy-from-key-copy-stdio <by_code|by_code_pg_binary|by_provider_set|by_provider_set_pg_binary|combined|combined_pg_binary|price_set_atoms_by_id_v2|price_set_atoms_by_id_v2_pg_binary>",
            )
        })?;
        if args.next().is_some() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "usage: ptg2_scanner --serving-binary-copy-from-key-copy-stdio <by_code|by_code_pg_binary|by_provider_set|by_provider_set_pg_binary|combined|combined_pg_binary|price_set_atoms_by_id_v2|price_set_atoms_by_id_v2_pg_binary>",
            ));
        }
        return write_serving_binary_copy_from_key_copy_stdio(&kind);
    }
    if first_arg == "--address-canonicalize-copy" {
        let input_path = args.next().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "usage: ptg2_scanner --address-canonicalize-copy <input_path> <output_path>",
            )
        })?;
        let output_path = args.next().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "usage: ptg2_scanner --address-canonicalize-copy <input_path> <output_path>",
            )
        })?;
        if args.next().is_some() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "usage: ptg2_scanner --address-canonicalize-copy <input_path> <output_path>",
            ));
        }
        return canonicalize_copy_file(Path::new(&input_path), Path::new(&output_path));
    }
    let arrays: Vec<String> = args.collect();
    if arrays.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "at least one top-level array name is required",
        ));
    }
    scan(Path::new(&first_arg), &arrays)
}
