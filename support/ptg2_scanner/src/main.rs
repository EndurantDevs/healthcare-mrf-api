use crossbeam_channel::{bounded, Receiver, Sender, TrySendError};
use flate2::read::MultiGzDecoder;
use serde_json::{json, Map, Value};
use std::any::Any;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::env;
use std::fmt::Display;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::panic::{self, AssertUnwindSafe};
use std::path::Path;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Mutex,
};
use std::thread;
use std::time::Instant;
use struson::reader::{JsonReader, JsonStreamReader, ValueType};
use xxhash_rust::xxh3::Xxh3;

const READ_BUF_SIZE: usize = 8 * 1024 * 1024;
const DEFAULT_PROGRESS_BYTES: u64 = 256 * 1024 * 1024;
const DEFAULT_PROGRESS_OBJECTS: u64 = 2_000_000;
const DEFAULT_SPLIT_NEGOTIATED_RATES: usize = 1024;
const DEFAULT_COMPACT_RUST_WORKERS: usize = 8;
const DEFAULT_COMPACT_RUST_WORK_QUEUE: usize = 16;
const DEFAULT_COMPACT_COPY_ROTATE_BYTES: u64 = 128 * 1024 * 1024;

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

fn progress_interval(name: &str, default_value: u64) -> u64 {
    env::var(name)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(default_value)
}

fn env_usize(name: &str, default_value: usize) -> usize {
    env::var(name)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default_value)
}

fn emit_progress(
    path: &Path,
    total_bytes: u64,
    compressed_bytes_read: &Arc<AtomicU64>,
    object_counts: &HashMap<String, u64>,
    started_at: Instant,
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
    let elapsed_seconds = started_at.elapsed().as_secs_f64();
    let compressed_mib_s = if elapsed_seconds > 0.0 {
        (bytes_read as f64 / (1024.0 * 1024.0)) / elapsed_seconds
    } else {
        0.0
    };
    let eta_seconds = if compressed_mib_s > 0.0 && total_bytes > bytes_read {
        Some(((total_bytes - bytes_read) as f64 / (1024.0 * 1024.0)) / compressed_mib_s)
    } else {
        None
    };
    let counts = object_counts
        .iter()
        .map(|(name, count)| format!("{}={}", name, count))
        .collect::<Vec<_>>()
        .join("\t");
    eprintln!(
        "PTG2_SCANNER_PROGRESS\tpath={}\tcompressed_bytes={}\ttotal_bytes={}\tpercent={:.2}\tcompressed_mib_s={:.2}\telapsed_seconds={:.0}\teta_seconds={}\tobjects={}\t{}\tdone={}",
        path.display(),
        bytes_read,
        total_bytes,
        percent,
        compressed_mib_s,
        elapsed_seconds,
        eta_seconds
            .map(|value| format!("{:.0}", value))
            .unwrap_or_else(|| "unknown".to_string()),
        total_objects,
        counts,
        if done { "true" } else { "false" },
    );
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
}

#[derive(Clone, Debug)]
struct RateLite {
    provider_refs: Vec<String>,
    provider_groups: Vec<Value>,
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

#[derive(Clone)]
struct CopyPathConfig {
    compact: Option<String>,
    procedure: Option<String>,
    price_set: Option<String>,
    provider_set: Option<String>,
    provider_group_member: Option<String>,
}

impl CopyPathConfig {
    fn from_env() -> Self {
        Self {
            compact: env_path("HLTHPRT_PTG2_COMPACT_SERVING_COPY_PATH"),
            procedure: env_path("HLTHPRT_PTG2_PROCEDURE_COPY_PATH"),
            price_set: env_path("HLTHPRT_PTG2_PRICE_SET_COPY_PATH"),
            provider_set: env_path("HLTHPRT_PTG2_PROVIDER_SET_COPY_PATH"),
            provider_group_member: env_path("HLTHPRT_PTG2_PROVIDER_GROUP_MEMBER_COPY_PATH"),
        }
    }

    fn has_file_paths(&self) -> bool {
        self.compact.is_some()
            || self.procedure.is_some()
            || self.price_set.is_some()
            || self.provider_set.is_some()
            || self.provider_group_member.is_some()
    }

    fn for_worker(&self, worker_id: usize) -> Self {
        let suffix = format!(".worker{:04}", worker_id);
        Self {
            compact: self.compact.as_ref().map(|path| format!("{path}{suffix}")),
            procedure: self
                .procedure
                .as_ref()
                .map(|path| format!("{path}{suffix}")),
            price_set: self
                .price_set
                .as_ref()
                .map(|path| format!("{path}{suffix}")),
            provider_set: self
                .provider_set
                .as_ref()
                .map(|path| format!("{path}{suffix}")),
            provider_group_member: self
                .provider_group_member
                .as_ref()
                .map(|path| format!("{path}{suffix}")),
        }
    }
}

fn env_path(name: &str) -> Option<String> {
    env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

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

struct SharedDedupe {
    serving_rate: ShardedDedupe64,
    procedure: ShardedDedupe64,
    price_set: ShardedDedupe64,
    provider_set: ShardedDedupe64,
    provider_group_member: ShardedDedupe128,
    serving_rate_counter: DedupeCounter,
    procedure_counter: DedupeCounter,
    price_set_counter: DedupeCounter,
    provider_set_counter: DedupeCounter,
    provider_group_member_counter: DedupeCounter,
}

impl SharedDedupe {
    fn new(worker_count: usize) -> Self {
        let shard_count = (worker_count.max(1) * 4).max(16);
        Self {
            serving_rate: ShardedDedupe64::new(shard_count),
            procedure: ShardedDedupe64::new(shard_count),
            price_set: ShardedDedupe64::new(shard_count),
            provider_set: ShardedDedupe64::new(shard_count),
            provider_group_member: ShardedDedupe128::new(shard_count),
            serving_rate_counter: DedupeCounter::new(),
            procedure_counter: DedupeCounter::new(),
            price_set_counter: DedupeCounter::new(),
            provider_set_counter: DedupeCounter::new(),
            provider_group_member_counter: DedupeCounter::new(),
        }
    }

    fn insert_serving_rate(&self, key: &str) -> bool {
        let inserted = self.serving_rate.insert_hash_text(key);
        self.serving_rate_counter.record(inserted);
        inserted
    }

    fn insert_procedure(&self, key: &str) -> bool {
        let inserted = self.procedure.insert_hash_text(key);
        self.procedure_counter.record(inserted);
        inserted
    }

    fn insert_price_set(&self, key: &str) -> bool {
        let inserted = self.price_set.insert_hash_text(key);
        self.price_set_counter.record(inserted);
        inserted
    }

    fn insert_provider_set(&self, key: &str) -> bool {
        let inserted = self.provider_set.insert_hash_text(key);
        self.provider_set_counter.record(inserted);
        inserted
    }

    fn insert_provider_group_member(&self, group_hash: i64, npi: i64) -> bool {
        let key = provider_group_member_key(group_hash, npi);
        let inserted = self.provider_group_member.insert(key);
        self.provider_group_member_counter.record(inserted);
        inserted
    }
}

fn dedupe_reduction_pct(attempted: u64, duplicate: u64) -> f64 {
    if attempted == 0 {
        0.0
    } else {
        (duplicate as f64 / attempted as f64) * 100.0
    }
}

fn dedupe_summary_payload(dedupe: &SharedDedupe, object_counts: &HashMap<String, u64>) -> Value {
    let negotiated_rates = object_counts.get("negotiated_rates").copied().unwrap_or(0);
    let (serving_attempted, serving_unique, serving_duplicate) =
        dedupe.serving_rate_counter.snapshot();
    let (procedure_attempted, procedure_unique, procedure_duplicate) =
        dedupe.procedure_counter.snapshot();
    let (price_attempted, price_unique, price_duplicate) = dedupe.price_set_counter.snapshot();
    let (provider_attempted, provider_unique, provider_duplicate) =
        dedupe.provider_set_counter.snapshot();
    let (pgm_attempted, pgm_unique, pgm_duplicate) =
        dedupe.provider_group_member_counter.snapshot();
    json!({
        "negotiated_rates": negotiated_rates,
        "serving_rate_attempted": serving_attempted,
        "serving_rate_unique": serving_unique,
        "serving_rate_duplicate": serving_duplicate,
        "serving_rate_reduction_pct": dedupe_reduction_pct(serving_attempted, serving_duplicate),
        "procedure_attempted": procedure_attempted,
        "procedure_unique": procedure_unique,
        "procedure_duplicate": procedure_duplicate,
        "procedure_reduction_pct": dedupe_reduction_pct(procedure_attempted, procedure_duplicate),
        "price_set_attempted": price_attempted,
        "price_set_unique": price_unique,
        "price_set_duplicate": price_duplicate,
        "price_set_reduction_pct": dedupe_reduction_pct(price_attempted, price_duplicate),
        "provider_set_attempted": provider_attempted,
        "provider_set_unique": provider_unique,
        "provider_set_duplicate": provider_duplicate,
        "provider_set_reduction_pct": dedupe_reduction_pct(provider_attempted, provider_duplicate),
        "provider_group_member_attempted": pgm_attempted,
        "provider_group_member_unique": pgm_unique,
        "provider_group_member_duplicate": pgm_duplicate,
        "provider_group_member_reduction_pct": dedupe_reduction_pct(pgm_attempted, pgm_duplicate),
    })
}

fn emit_dedupe_summary(dedupe: &SharedDedupe, object_counts: &HashMap<String, u64>) {
    let payload = dedupe_summary_payload(dedupe, object_counts);
    eprintln!(
        "PTG2_DEDUPE_SUMMARY\tnegotiated_rates={}\tserving_rate_attempted={}\tserving_rate_unique={}\tserving_rate_duplicate={}\tserving_rate_reduction_pct={:.2}\tprocedure_attempted={}\tprocedure_unique={}\tprocedure_duplicate={}\tprocedure_reduction_pct={:.2}\tprice_set_attempted={}\tprice_set_unique={}\tprice_set_duplicate={}\tprice_set_reduction_pct={:.2}\tprovider_set_attempted={}\tprovider_set_unique={}\tprovider_set_duplicate={}\tprovider_set_reduction_pct={:.2}\tprovider_group_member_attempted={}\tprovider_group_member_unique={}\tprovider_group_member_duplicate={}\tprovider_group_member_reduction_pct={:.2}",
        payload.get("negotiated_rates").and_then(Value::as_u64).unwrap_or(0),
        payload.get("serving_rate_attempted").and_then(Value::as_u64).unwrap_or(0),
        payload.get("serving_rate_unique").and_then(Value::as_u64).unwrap_or(0),
        payload.get("serving_rate_duplicate").and_then(Value::as_u64).unwrap_or(0),
        payload.get("serving_rate_reduction_pct").and_then(Value::as_f64).unwrap_or(0.0),
        payload.get("procedure_attempted").and_then(Value::as_u64).unwrap_or(0),
        payload.get("procedure_unique").and_then(Value::as_u64).unwrap_or(0),
        payload.get("procedure_duplicate").and_then(Value::as_u64).unwrap_or(0),
        payload.get("procedure_reduction_pct").and_then(Value::as_f64).unwrap_or(0.0),
        payload.get("price_set_attempted").and_then(Value::as_u64).unwrap_or(0),
        payload.get("price_set_unique").and_then(Value::as_u64).unwrap_or(0),
        payload.get("price_set_duplicate").and_then(Value::as_u64).unwrap_or(0),
        payload.get("price_set_reduction_pct").and_then(Value::as_f64).unwrap_or(0.0),
        payload.get("provider_set_attempted").and_then(Value::as_u64).unwrap_or(0),
        payload.get("provider_set_unique").and_then(Value::as_u64).unwrap_or(0),
        payload.get("provider_set_duplicate").and_then(Value::as_u64).unwrap_or(0),
        payload.get("provider_set_reduction_pct").and_then(Value::as_f64).unwrap_or(0.0),
        payload.get("provider_group_member_attempted").and_then(Value::as_u64).unwrap_or(0),
        payload.get("provider_group_member_unique").and_then(Value::as_u64).unwrap_or(0),
        payload.get("provider_group_member_duplicate").and_then(Value::as_u64).unwrap_or(0),
        payload.get("provider_group_member_reduction_pct").and_then(Value::as_f64).unwrap_or(0.0),
    );
}

fn hash_text_key(key: &str) -> u64 {
    u64::from_str_radix(key, 16).unwrap_or_else(|_| xxh3_63(key.as_bytes()))
}

fn provider_group_member_key(group_hash: i64, npi: i64) -> u128 {
    ((group_hash as u64 as u128) << 64) | (npi as u64 as u128)
}

fn shard_for_u64(key: u64, shard_count: usize) -> usize {
    (key as usize) % shard_count.max(1)
}

fn shard_for_u128(key: u128, shard_count: usize) -> usize {
    ((key ^ (key >> 64)) as usize) % shard_count.max(1)
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
    mut job: WorkerJob,
) -> io::Result<()> {
    loop {
        match tx.try_send(job) {
            Ok(()) => return Ok(()),
            Err(TrySendError::Full(returned_job)) => {
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

fn normalize_money_text(mut text: String) -> Option<String> {
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

fn normalized_scalar_from_reader<R: Read>(
    json_reader: &mut JsonStreamReader<R>,
) -> io::Result<Option<String>> {
    match json_reader.peek().map_err(to_io_error)? {
        ValueType::String => {
            let text = json_reader.next_string().map_err(to_io_error)?;
            let trimmed = text.trim();
            if trimmed.is_empty() {
                Ok(None)
            } else {
                Ok(Some(trimmed.to_string()))
            }
        }
        ValueType::Number => {
            let text = json_reader.next_number_as_string().map_err(to_io_error)?;
            if text.is_empty() {
                Ok(None)
            } else {
                Ok(Some(text))
            }
        }
        ValueType::Boolean => Ok(Some(
            json_reader.next_bool().map_err(to_io_error)?.to_string(),
        )),
        ValueType::Null => {
            json_reader.next_null().map_err(to_io_error)?;
            Ok(None)
        }
        ValueType::Array | ValueType::Object => {
            json_reader.skip_value().map_err(to_io_error)?;
            Ok(None)
        }
    }
}

fn normalized_money_from_reader<R: Read>(
    json_reader: &mut JsonStreamReader<R>,
) -> io::Result<Option<String>> {
    Ok(normalized_scalar_from_reader(json_reader)?.and_then(normalize_money_text))
}

fn normalized_string_list_from_reader<R: Read>(
    json_reader: &mut JsonStreamReader<R>,
) -> io::Result<Vec<String>> {
    let mut out = Vec::new();
    match json_reader.peek().map_err(to_io_error)? {
        ValueType::Array => {
            json_reader.begin_array().map_err(to_io_error)?;
            while json_reader.has_next().map_err(to_io_error)? {
                if let Some(text) = normalized_scalar_from_reader(json_reader)? {
                    out.push(text);
                }
            }
            json_reader.end_array().map_err(to_io_error)?;
        }
        ValueType::Object => {
            json_reader.skip_value().map_err(to_io_error)?;
        }
        _ => {
            if let Some(text) = normalized_scalar_from_reader(json_reader)? {
                out.push(text);
            }
        }
    }
    Ok(out)
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

fn price_rate_value(rate: &str) -> Value {
    rate.parse::<i64>()
        .map(Value::from)
        .or_else(|_| {
            rate.parse::<f64>()
                .ok()
                .filter(|value| value.is_finite())
                .and_then(serde_json::Number::from_f64)
                .map(Value::Number)
                .ok_or(())
        })
        .unwrap_or_else(|_| Value::String(rate.to_string()))
}

fn price_lite_key_and_payload(prices: &[PriceLite]) -> Option<(String, Value)> {
    let mut key_parts: Vec<Value> = Vec::new();
    let mut payload: Vec<Value> = Vec::new();
    for price in prices {
        let service_code = json!(price.service_code);
        let modifiers = json!(price.billing_code_modifier);
        let normalized = json!({
            "negotiated_type": price.negotiated_type,
            "negotiated_rate": price_rate_value(&price.negotiated_rate),
            "expiration_date": price.expiration_date,
            "service_code": service_code,
            "billing_class": price.billing_class,
            "setting": price.setting,
            "billing_code_modifier": modifiers,
            "additional_information": price.additional_information,
        });
        payload.push(normalized);
        key_parts.push(json!([
            price.negotiated_type,
            price.negotiated_rate,
            price.expiration_date,
            price.service_code,
            price.billing_class,
            price.setting,
            price.billing_code_modifier,
            price.additional_information,
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
        )?;
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

    fn from_paths(paths: &CopyPathConfig, rotate_bytes: u64) -> io::Result<Self> {
        Ok(Self {
            procedure: match &paths.procedure {
                Some(path) => Some(CompactCopySink::new_named_file(
                    path.clone(),
                    rotate_bytes,
                    "procedure_copy_file",
                )?),
                None => None,
            },
            price_set: match &paths.price_set {
                Some(path) => Some(CompactCopySink::new_named_file(
                    path.clone(),
                    rotate_bytes,
                    "price_set_copy_file",
                )?),
                None => None,
            },
            provider_set: match &paths.provider_set {
                Some(path) => Some(CompactCopySink::new_named_file(
                    path.clone(),
                    rotate_bytes,
                    "provider_set_copy_file",
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

    fn sink_from_env(
        name: &str,
        rotate_bytes: u64,
        record_kind: &str,
    ) -> io::Result<Option<CompactCopySink>> {
        match env::var(name) {
            Ok(copy_path) if !copy_path.trim().is_empty() => Ok(Some(
                CompactCopySink::new_named_file(copy_path, rotate_bytes, record_kind)?,
            )),
            _ => Ok(None),
        }
    }

    fn maybe_rotate_silent(&mut self) -> io::Result<Vec<CopyFileEvent>> {
        let mut events = Vec::new();
        if let Some(sink) = self.procedure.as_mut() {
            if let Some(event) = sink.maybe_rotate_silent()? {
                events.push(event);
            }
        }
        if let Some(sink) = self.price_set.as_mut() {
            if let Some(event) = sink.maybe_rotate_silent()? {
                events.push(event);
            }
        }
        if let Some(sink) = self.provider_set.as_mut() {
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

    fn finish_silent(mut self) -> io::Result<Vec<CopyFileEvent>> {
        let mut events = Vec::new();
        if let Some(sink) = self.procedure.take() {
            if let Some(event) = sink.finish_silent()? {
                events.push(event);
            }
        }
        if let Some(sink) = self.price_set.take() {
            if let Some(event) = sink.finish_silent()? {
                events.push(event);
            }
        }
        if let Some(sink) = self.provider_set.take() {
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
        procedure_payload: &Value,
    ) -> io::Result<bool> {
        let Some(sink) = self.procedure.as_mut() else {
            return Ok(false);
        };
        let fields = [
            pg_text_copy_field(Some(procedure_hash)),
            pg_text_copy_field(Some(&procedure_hash[..procedure_hash.len().min(16)])),
            pg_text_copy_field(
                normalize_string(procedure_value.get("billing_code_type")).as_deref(),
            ),
            pg_text_copy_field(
                normalize_string(procedure_value.get("billing_code_type_version")).as_deref(),
            ),
            pg_text_copy_field(normalize_string(procedure_value.get("billing_code")).as_deref()),
            pg_text_copy_field(normalize_string(procedure_value.get("name")).as_deref()),
            pg_text_copy_field(normalize_string(procedure_value.get("description")).as_deref()),
            pg_json_copy_field(procedure_payload),
        ];
        let writer = sink.writer.as_mut().ok_or_else(|| {
            io::Error::new(io::ErrorKind::BrokenPipe, "procedure copy writer is closed")
        })?;
        write_copy_fields(writer, &fields)?;
        sink.row_count += 1;
        Ok(true)
    }

    fn write_price_set(&mut self, price_set_hash: &str, price_payload: &Value) -> io::Result<bool> {
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
            pg_text_copy_field(
                single_price
                    .and_then(|price| normalize_string(price.get("negotiated_type")))
                    .as_deref(),
            ),
            pg_text_copy_field(negotiated_rate.as_deref()),
            pg_text_copy_field(
                single_price
                    .and_then(|price| normalize_string(price.get("expiration_date")))
                    .as_deref(),
            ),
            pg_text_array_value_field(single_price.and_then(|price| price.get("service_code"))),
            pg_text_copy_field(
                single_price
                    .and_then(|price| normalize_string(price.get("billing_class")))
                    .as_deref(),
            ),
            pg_text_copy_field(
                single_price
                    .and_then(|price| normalize_string(price.get("setting")))
                    .as_deref(),
            ),
            pg_text_array_value_field(
                single_price.and_then(|price| price.get("billing_code_modifier")),
            ),
            pg_text_copy_field(
                single_price
                    .and_then(|price| normalize_string(price.get("additional_information")))
                    .as_deref(),
            ),
            canonical_payload,
        ];
        let writer = sink.writer.as_mut().ok_or_else(|| {
            io::Error::new(io::ErrorKind::BrokenPipe, "price set copy writer is closed")
        })?;
        write_copy_fields(writer, &fields)?;
        sink.row_count += 1;
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
            io::Error::new(
                io::ErrorKind::BrokenPipe,
                "provider set copy writer is closed",
            )
        })?;
        write_copy_fields(writer, &fields)?;
        sink.row_count += 1;
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
        let Some(groups) = provider_ref
            .get("provider_groups")
            .and_then(Value::as_array)
        else {
            return Ok(());
        };
        let writer = sink.writer.as_mut().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::BrokenPipe,
                "provider group member copy writer is closed",
            )
        })?;
        let mut rows_written = 0u64;
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
                rows_written += 1;
            }
        }
        sink.row_count += rows_written;
        Ok(())
    }

    fn write_provider_group_members_shared(
        &mut self,
        provider_ref: &Value,
        dedupe: &SharedDedupe,
    ) -> io::Result<()> {
        let Some(sink) = self.provider_group_member.as_mut() else {
            return Ok(());
        };
        let Some(groups) = provider_ref
            .get("provider_groups")
            .and_then(Value::as_array)
        else {
            return Ok(());
        };
        let writer = sink.writer.as_mut().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::BrokenPipe,
                "provider group member copy writer is closed",
            )
        })?;
        let mut rows_written = 0u64;
        for group in groups {
            let tin = group.get("tin").unwrap_or(&Value::Null);
            let npi = int_list(group.get("npi"));
            let group_hash = provider_group_hash(tin, &npi);
            for (ordinal, npi_value) in npi.iter().enumerate() {
                if !dedupe.insert_provider_group_member(group_hash, *npi_value) {
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
                rows_written += 1;
            }
        }
        sink.row_count += rows_written;
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

fn process_compact_rate_lites<W: Write>(
    writer: &mut W,
    compact_copy_writer: &mut Option<CompactCopySink>,
    dictionary_copy_sinks: &mut DictionaryCopySinks,
    provider_map: &HashMap<String, ProviderEntry>,
    emitted_price_sets: &mut HashSet<String>,
    emitted_provider_sets: &mut HashSet<String>,
    emitted_procedures: &mut HashSet<String>,
    emitted_provider_group_members: &mut HashSet<(i64, i64)>,
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
        if !dictionary_copy_sinks.write_procedure(
            &procedure_hash,
            procedure_value,
            &procedure_payload,
        )? {
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

    let mut parsed_rates: Vec<(String, Value, i64, Vec<i64>, i64)> = Vec::new();
    for rate in rates {
        let provider_entry = if !rate.provider_groups.is_empty() {
            let provider_ref = json!({"provider_groups": rate.provider_groups});
            dictionary_copy_sinks
                .write_provider_group_members(&provider_ref, emitted_provider_group_members)?;
            match build_provider_entry(&provider_ref) {
                Some(entry) => entry,
                None => continue,
            }
        } else {
            match provider_set_from_ref_keys(provider_map, &rate.provider_refs) {
                Some(entry) => entry,
                None => continue,
            }
        };
        let Some((price_set_hash, price_payload)) = price_lite_key_and_payload(&rate.prices) else {
            continue;
        };
        parsed_rates.push((
            price_set_hash,
            price_payload,
            provider_entry.entry_hash,
            provider_entry.provider_group_hashes,
            provider_entry.provider_count,
        ));
    }

    let mut grouped: BTreeMap<String, (String, Value, HashSet<i64>, HashSet<i64>, i64)> =
        BTreeMap::new();
    for (
        price_set_hash,
        price_payload,
        provider_entry_hash,
        provider_group_hashes,
        provider_count,
    ) in parsed_rates
    {
        let group = grouped.entry(price_set_hash.clone()).or_insert_with(|| {
            (
                price_set_hash,
                price_payload,
                HashSet::new(),
                HashSet::new(),
                0,
            )
        });
        if group.2.insert(provider_entry_hash) {
            for provider_group_hash in provider_group_hashes {
                group.3.insert(provider_group_hash);
            }
            group.4 += provider_count;
        }
    }

    for (
        _key,
        (
            price_set_hash,
            price_payload,
            _provider_entry_hashes,
            provider_group_hashes,
            provider_count,
        ),
    ) in grouped
    {
        let mut sorted_provider_hashes: Vec<i64> = provider_group_hashes.into_iter().collect();
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
    Ok(())
}

#[derive(Clone)]
struct CompactContext {
    snapshot_id: String,
    plan_id: String,
    plan_month_id: String,
    source_trace_set_hash: String,
    confidence_code: String,
}

enum WorkerJob {
    ProviderRefs(Vec<Value>),
    Rates {
        procedure: Map<String, Value>,
        rates: Vec<RateLite>,
    },
}

impl WorkerJob {
    fn name(&self) -> &'static str {
        match self {
            WorkerJob::ProviderRefs(_) => "provider refs",
            WorkerJob::Rates { .. } => "rates",
        }
    }
}

fn process_provider_refs_worker(
    refs: &[Value],
    dictionary_copy_sinks: &mut DictionaryCopySinks,
    dedupe: &SharedDedupe,
) -> io::Result<()> {
    for provider_ref in refs {
        dictionary_copy_sinks.write_provider_group_members_shared(provider_ref, dedupe)?;
    }
    Ok(())
}

fn process_compact_rate_lites_worker<W: Write>(
    writer: &mut W,
    compact_copy_writer: &mut Option<CompactCopySink>,
    dictionary_copy_sinks: &mut DictionaryCopySinks,
    provider_map: &HashMap<String, ProviderEntry>,
    dedupe: &SharedDedupe,
    rates: &[RateLite],
    procedure_value: &Value,
    context: &CompactContext,
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
    if dedupe.insert_procedure(&procedure_hash) {
        if !dictionary_copy_sinks.write_procedure(
            &procedure_hash,
            procedure_value,
            &procedure_payload,
        )? {
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

    let parsed_rates: Vec<(String, Value, i64, Vec<i64>, i64)> = rates
        .iter()
        .filter_map(|rate| {
            let provider_entry = provider_set_from_ref_keys(provider_map, &rate.provider_refs)?;
            let (price_set_hash, price_payload) = price_lite_key_and_payload(&rate.prices)?;
            Some((
                price_set_hash,
                price_payload,
                provider_entry.entry_hash,
                provider_entry.provider_group_hashes,
                provider_entry.provider_count,
            ))
        })
        .collect();

    let mut grouped: BTreeMap<String, (String, Value, HashSet<i64>, HashSet<i64>, i64)> =
        BTreeMap::new();
    for (
        price_set_hash,
        price_payload,
        provider_entry_hash,
        provider_group_hashes,
        provider_count,
    ) in parsed_rates
    {
        let group = grouped.entry(price_set_hash.clone()).or_insert_with(|| {
            (
                price_set_hash,
                price_payload,
                HashSet::new(),
                HashSet::new(),
                0,
            )
        });
        if group.2.insert(provider_entry_hash) {
            for provider_group_hash in provider_group_hashes {
                group.3.insert(provider_group_hash);
            }
            group.4 += provider_count;
        }
    }

    for (
        _key,
        (
            price_set_hash,
            price_payload,
            _provider_entry_hashes,
            provider_group_hashes,
            provider_count,
        ),
    ) in grouped
    {
        let mut sorted_provider_hashes: Vec<i64> = provider_group_hashes.into_iter().collect();
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
                context.snapshot_id.clone(),
                procedure_hash.clone(),
                provider_set_hash.clone(),
                price_set_hash.clone(),
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
        if dedupe.insert_price_set(&price_set_hash) {
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
        if dedupe.insert_provider_set(&provider_set_hash) {
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
        if dedupe.insert_serving_rate(&serving_rate_id) {
            if let Some(copy_writer) = compact_copy_writer.as_mut() {
                copy_writer.write_row(
                    &serving_rate_id,
                    &context.snapshot_id,
                    &context.plan_id,
                    &context.plan_month_id,
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
                    &context.source_trace_set_hash,
                    &context.confidence_code,
                )?;
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
                        "reported_code_system": reported_code_system,
                        "reported_code": reported_code,
                        "billing_code": billing_code,
                        "billing_code_type": billing_code_type,
                        "rate_pack_hash": rate_pack_hash,
                        "provider_set_hash": provider_set_hash,
                        "provider_count": provider_count,
                        "price_set_hash": price_set_hash,
                        "source_trace_set_hash": context.source_trace_set_hash.clone(),
                        "confidence_code": context.confidence_code.clone(),
                    }),
                )?;
            }
        }
    }
    Ok(())
}

fn read_rate_lite_struson<R: Read>(
    json_reader: &mut JsonStreamReader<R>,
) -> io::Result<Option<RateLite>> {
    let mut provider_refs: Vec<String> = Vec::new();
    let mut provider_groups: Vec<Value> = Vec::new();
    let mut prices: Vec<PriceLite> = Vec::new();
    json_reader.begin_object().map_err(to_io_error)?;
    while json_reader.has_next().map_err(to_io_error)? {
        let field = match json_reader.next_name().map_err(to_io_error)? {
            "provider_groups" => 1,
            "provider_references" => 2,
            "negotiated_prices" => 3,
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
        prices,
    }))
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
                service_code = normalized_string_list_from_reader(json_reader)?;
            }
            5 => {
                billing_class = normalized_scalar_from_reader(json_reader)?;
            }
            6 => {
                setting = normalized_scalar_from_reader(json_reader)?;
            }
            7 => {
                billing_code_modifier = normalized_string_list_from_reader(json_reader)?;
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

fn process_in_network_struson<R: Read, W: Write>(
    json_reader: &mut JsonStreamReader<R>,
    writer: &mut W,
    compact_copy_writer: &mut Option<CompactCopySink>,
    dictionary_copy_sinks: &mut DictionaryCopySinks,
    provider_map: &HashMap<String, ProviderEntry>,
    emitted_price_sets: &mut HashSet<String>,
    emitted_provider_sets: &mut HashSet<String>,
    emitted_procedures: &mut HashSet<String>,
    emitted_provider_group_members: &mut HashSet<(i64, i64)>,
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
                                emitted_provider_group_members,
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
            emitted_provider_group_members,
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

fn enqueue_in_network_struson<R: Read>(
    json_reader: &mut JsonStreamReader<R>,
    tx: &Sender<WorkerJob>,
    event_rx: &Receiver<CopyFileEvent>,
    writer: &mut impl Write,
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
                            let rates =
                                std::mem::replace(&mut rate_chunk, Vec::with_capacity(chunk_size));
                            send_worker_job(
                                tx,
                                event_rx,
                                writer,
                                WorkerJob::Rates {
                                    procedure: procedure.clone(),
                                    rates,
                                },
                            )?;
                            drain_copy_file_events(event_rx, writer)?;
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
        send_worker_job(
            tx,
            event_rx,
            writer,
            WorkerJob::Rates {
                procedure,
                rates: rate_chunk,
            },
        )?;
        drain_copy_file_events(event_rx, writer)?;
    }
    Ok(rate_count)
}

fn finish_worker_outputs(
    compact_copy_writer: Option<CompactCopySink>,
    dictionary_copy_sinks: DictionaryCopySinks,
) -> io::Result<Vec<CopyFileEvent>> {
    let mut events = Vec::new();
    if let Some(copy_writer) = compact_copy_writer {
        if let Some(event) = copy_writer.finish_silent()? {
            events.push(event);
        }
    }
    events.extend(dictionary_copy_sinks.finish_silent()?);
    Ok(events)
}

fn compact_worker_loop(
    worker_id: usize,
    rx: Receiver<WorkerJob>,
    event_tx: Sender<CopyFileEvent>,
    provider_map: Arc<HashMap<String, ProviderEntry>>,
    dedupe: Arc<SharedDedupe>,
    copy_paths: CopyPathConfig,
    rotate_bytes: u64,
    context: CompactContext,
) -> io::Result<Vec<CopyFileEvent>> {
    let worker_paths = copy_paths.for_worker(worker_id);
    let mut compact_copy_writer = worker_paths
        .compact
        .as_ref()
        .map(|path| CompactCopySink::new_file(path.clone(), rotate_bytes))
        .transpose()?;
    let mut dictionary_copy_sinks = DictionaryCopySinks::from_paths(&worker_paths, rotate_bytes)?;
    let mut sink = io::sink();

    for job in rx.iter() {
        match job {
            WorkerJob::ProviderRefs(refs) => {
                process_provider_refs_worker(&refs, &mut dictionary_copy_sinks, &dedupe)?;
            }
            WorkerJob::Rates { procedure, rates } => {
                let procedure_value = Value::Object(procedure);
                process_compact_rate_lites_worker(
                    &mut sink,
                    &mut compact_copy_writer,
                    &mut dictionary_copy_sinks,
                    &provider_map,
                    &dedupe,
                    &rates,
                    &procedure_value,
                    &context,
                )?;
            }
        }
        if let Some(copy_writer) = compact_copy_writer.as_mut() {
            if let Some(event) = copy_writer.maybe_rotate_silent()? {
                event_tx.send(event).map_err(|err| {
                    io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        format!("compact copy event queue closed: {err}"),
                    )
                })?;
            }
        }
        for event in dictionary_copy_sinks.maybe_rotate_silent()? {
            event_tx.send(event).map_err(|err| {
                io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    format!("dictionary copy event queue closed: {err}"),
                )
            })?;
        }
    }

    finish_worker_outputs(compact_copy_writer, dictionary_copy_sinks)
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
    let reader = open_reader(path, Arc::clone(&compressed_bytes_read))?;
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
    let mut provider_ref_jobs: Vec<Vec<Value>> = Vec::new();
    let mut provider_ref_chunk: Vec<Value> = Vec::with_capacity(1024);
    let negotiated_rate_chunk_size = split_interval(
        "HLTHPRT_PTG2_RUST_SPLIT_NEGOTIATED_RATES",
        DEFAULT_SPLIT_NEGOTIATED_RATES,
    );
    let bounded_queue_size = queue_size.max(worker_count).max(1);
    let dedupe = Arc::new(SharedDedupe::new(worker_count));
    let event_queue_size = env_usize(
        "HLTHPRT_PTG2_RUST_EVENT_QUEUE",
        (bounded_queue_size * 2).max(worker_count),
    );
    let (tx, rx) = bounded::<WorkerJob>(bounded_queue_size);
    let (event_tx, event_rx) = bounded::<CopyFileEvent>(event_queue_size);
    let stdout = io::stdout();
    let mut writer = BufWriter::new(stdout.lock());

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
                            provider_map.insert(key, entry);
                            provider_ref_chunk.push(value);
                            if provider_ref_chunk.len() >= 1024 {
                                provider_ref_jobs.push(std::mem::replace(
                                    &mut provider_ref_chunk,
                                    Vec::with_capacity(1024),
                                ));
                            }
                        }
                    }
                    *object_counts
                        .entry("provider_references".to_string())
                        .or_insert(0) += 1;
                }
                json_reader.end_array().map_err(to_io_error)?;
                if !provider_ref_chunk.is_empty() {
                    provider_ref_jobs.push(std::mem::take(&mut provider_ref_chunk));
                }

                let provider_map = Arc::new(provider_map);
                let mut handles = Vec::with_capacity(worker_count);
                for worker_id in 0..worker_count {
                    let worker_rx = rx.clone();
                    let worker_event_tx = event_tx.clone();
                    let worker_provider_map = Arc::clone(&provider_map);
                    let worker_dedupe = Arc::clone(&dedupe);
                    let worker_copy_paths = copy_paths.clone();
                    let worker_context = context.clone();
                    handles.push((
                        worker_id,
                        thread::spawn(move || {
                            let result = panic::catch_unwind(AssertUnwindSafe(|| {
                                compact_worker_loop(
                                    worker_id,
                                    worker_rx,
                                    worker_event_tx,
                                    worker_provider_map,
                                    worker_dedupe,
                                    worker_copy_paths,
                                    compact_copy_rotate_bytes,
                                    worker_context,
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
                                    Err(io::Error::new(
                                        io::ErrorKind::Other,
                                        format!("compact worker {worker_id} panicked: {message}"),
                                    ))
                                }
                            }
                        }),
                    ));
                }
                drop(rx);

                for refs in provider_ref_jobs.drain(..) {
                    send_worker_job(&tx, &event_rx, &mut writer, WorkerJob::ProviderRefs(refs))?;
                }

                while json_reader.has_next().map_err(to_io_error)? {
                    let name = json_reader.next_name_owned().map_err(to_io_error)?;
                    match name.as_str() {
                        "in_network" => {
                            json_reader.begin_array().map_err(to_io_error)?;
                            while json_reader.has_next().map_err(to_io_error)? {
                                let rate_count = enqueue_in_network_struson(
                                    &mut json_reader,
                                    &tx,
                                    &event_rx,
                                    &mut writer,
                                    negotiated_rate_chunk_size,
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
                        _ => {
                            json_reader.skip_value().map_err(to_io_error)?;
                        }
                    }
                }

                drop(tx);
                drop(event_tx);
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
                                worker_error = Some(io::Error::new(
                                    io::ErrorKind::Other,
                                    format!("compact worker {worker_id} panicked: {message}"),
                                ));
                            }
                        }
                    }
                }
                if let Some(err) = worker_error {
                    return Err(err);
                }
                emit_dedupe_summary(&dedupe, &object_counts);
                drain_copy_file_events(&event_rx, &mut writer)?;
                for event in copy_file_events {
                    emit_copy_file_event(&mut writer, &event)?;
                }
                emit_json_record(
                    &mut writer,
                    "dedupe_summary",
                    &dedupe_summary_payload(&dedupe, &object_counts),
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
    let reader = open_reader(path, Arc::clone(&compressed_bytes_read))?;
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
    let reader = open_reader(path, Arc::clone(&compressed_bytes_read))?;
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
    if rust_worker_count > 1
        && copy_paths.has_file_paths()
        && compact_parallel_has_provider_references(path)?
    {
        return scan_compact_struson_parallel(
            path,
            CompactContext {
                snapshot_id,
                plan_id,
                plan_month_id,
                source_trace_set_hash,
                confidence_code,
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
    let started_at = Instant::now();
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
                        &mut emitted_provider_group_members,
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
    dictionary_copy_sinks.finish(&mut writer)?;
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

fn main() -> io::Result<()> {
    let mut args = env::args().skip(1);
    let first_arg = args.next().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "usage: ptg2_scanner [--compact-serving] <path> <top_level_array>...",
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
    let arrays: Vec<String> = args.collect();
    if arrays.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "at least one top-level array name is required",
        ));
    }
    scan(Path::new(&first_arg), &arrays)
}
