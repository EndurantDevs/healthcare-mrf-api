use flate2::read::MultiGzDecoder;
use serde_json::Value;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::env;
use std::fs::File;
use std::io::{self, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::time::Instant;
use struson::reader::{JsonReader, JsonStreamReader};

const READ_BUF_SIZE: usize = 8 * 1024 * 1024;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct TargetKey {
    npi: i64,
    code: String,
    requested_system: String,
    checked_system: String,
    pos: Option<String>,
    alternate_system: bool,
}

#[derive(Clone, Debug)]
struct TargetRow {
    npi: i64,
    code: String,
    code_system: String,
    pos: Option<String>,
    api_status: String,
}

#[derive(Default)]
struct Evidence {
    raw_npi_in_provider_refs: bool,
    raw_npi_code_present: bool,
    raw_npi_code_pos_present: bool,
    files: BTreeSet<String>,
    provider_reference_ids: BTreeSet<i64>,
    inline_provider_groups: u64,
    rate_rows: u64,
    pos_rate_rows: u64,
    service_codes_seen: BTreeSet<String>,
    rates_seen: BTreeSet<String>,
}

fn to_io_error(error: impl std::fmt::Display) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error.to_string())
}

fn open_reader(path: &Path) -> io::Result<Box<dyn Read>> {
    let fp = File::open(path)?;
    let is_gzip = path
        .extension()
        .and_then(|value| value.to_str())
        .map(|value| value.eq_ignore_ascii_case("gz"))
        .unwrap_or(false);
    if is_gzip {
        Ok(Box::new(MultiGzDecoder::new(BufReader::with_capacity(
            READ_BUF_SIZE,
            fp,
        ))))
    } else {
        Ok(Box::new(BufReader::with_capacity(READ_BUF_SIZE, fp)))
    }
}

fn normalize_code_system(value: &str) -> String {
    match value.trim().to_ascii_uppercase().as_str() {
        "HCPCS" => "HCPCS".to_string(),
        "CPT" => "CPT".to_string(),
        other => other.to_string(),
    }
}

fn alternate_code_system(value: &str) -> String {
    if normalize_code_system(value) == "CPT" {
        "HCPCS".to_string()
    } else {
        "CPT".to_string()
    }
}

fn normalize_pos(value: &str) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else if trimmed.len() == 1 {
        Some(format!("0{trimmed}"))
    } else {
        Some(trimmed.to_string())
    }
}

fn value_i64(value: &Value) -> Option<i64> {
    match value {
        Value::Number(number) => number.as_i64(),
        Value::String(text) => text.parse::<i64>().ok(),
        _ => None,
    }
}

fn value_string(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => Some(text.trim().to_string()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    }
    .filter(|value| !value.is_empty())
}

fn value_i64_list(value: Option<&Value>) -> Vec<i64> {
    match value {
        Some(Value::Array(items)) => items.iter().filter_map(value_i64).collect(),
        Some(item) => value_i64(item).into_iter().collect(),
        None => Vec::new(),
    }
}

fn value_string_list(value: Option<&Value>) -> Vec<String> {
    match value {
        Some(Value::Array(items)) => items.iter().filter_map(value_string).collect(),
        Some(item) => value_string(item).into_iter().collect(),
        None => Vec::new(),
    }
}

fn read_targets(path: &Path) -> io::Result<Vec<TargetRow>> {
    let text = std::fs::read_to_string(path)?;
    let mut lines = text.lines();
    let header = lines.next().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "target CSV is missing a header",
        )
    })?;
    let columns: Vec<&str> = header.split(',').collect();
    let find = |name: &str| -> io::Result<usize> {
        columns
            .iter()
            .position(|column| *column == name)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("target CSV is missing column {name}"),
                )
            })
    };
    let npi_idx = find("npi")?;
    let code_idx = find("code")?;
    let system_idx = find("code_system")?;
    let pos_idx = find("pos")?;
    let status_idx = find("api_status")?;
    let mut targets = Vec::new();
    for line in lines {
        if line.trim().is_empty() {
            continue;
        }
        let values: Vec<&str> = line.split(',').collect();
        let npi = values
            .get(npi_idx)
            .and_then(|value| value.parse::<i64>().ok())
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("invalid NPI row: {line}"),
                )
            })?;
        targets.push(TargetRow {
            npi,
            code: values.get(code_idx).unwrap_or(&"").trim().to_string(),
            code_system: normalize_code_system(values.get(system_idx).unwrap_or(&"")),
            pos: normalize_pos(values.get(pos_idx).unwrap_or(&"")),
            api_status: values.get(status_idx).unwrap_or(&"").trim().to_string(),
        });
    }
    Ok(targets)
}

fn build_target_keys(targets: &[TargetRow]) -> HashMap<(String, String), Vec<TargetKey>> {
    let mut by_code: HashMap<(String, String), Vec<TargetKey>> = HashMap::new();
    for target in targets {
        for (checked_system, alternate_system) in [
            (target.code_system.clone(), false),
            (alternate_code_system(&target.code_system), true),
        ] {
            let key = TargetKey {
                npi: target.npi,
                code: target.code.clone(),
                requested_system: target.code_system.clone(),
                checked_system: checked_system.clone(),
                pos: target.pos.clone(),
                alternate_system,
            };
            by_code
                .entry((target.code.clone(), checked_system))
                .or_default()
                .push(key);
        }
    }
    by_code
}

fn mark_provider_reference(
    provider_ref: &Value,
    target_npis: &HashSet<i64>,
    provider_ref_npis: &mut HashMap<i64, Vec<i64>>,
    evidence: &mut HashMap<TargetKey, Evidence>,
    target_keys_by_npi: &HashMap<i64, Vec<TargetKey>>,
) {
    let Some(provider_group_id) = provider_ref.get("provider_group_id").and_then(value_i64) else {
        return;
    };
    let mut matched_npis = BTreeSet::new();
    if let Some(Value::Array(groups)) = provider_ref.get("provider_groups") {
        for group in groups {
            for npi in value_i64_list(group.get("npi")) {
                if target_npis.contains(&npi) {
                    matched_npis.insert(npi);
                }
            }
        }
    }
    if matched_npis.is_empty() {
        return;
    }
    let npis: Vec<i64> = matched_npis.iter().copied().collect();
    provider_ref_npis.insert(provider_group_id, npis.clone());
    for npi in npis {
        if let Some(keys) = target_keys_by_npi.get(&npi) {
            for key in keys {
                let item = evidence.entry(key.clone()).or_default();
                item.raw_npi_in_provider_refs = true;
                item.provider_reference_ids.insert(provider_group_id);
            }
        }
    }
}

fn scan_provider_references<R: Read>(
    json_reader: &mut JsonStreamReader<R>,
    path_label: &str,
    target_npis: &HashSet<i64>,
    provider_ref_npis: &mut HashMap<i64, Vec<i64>>,
    evidence: &mut HashMap<TargetKey, Evidence>,
    target_keys_by_npi: &HashMap<i64, Vec<TargetKey>>,
) -> io::Result<u64> {
    let mut count = 0;
    json_reader.begin_array().map_err(to_io_error)?;
    while json_reader.has_next().map_err(to_io_error)? {
        let provider_ref: Value = json_reader.deserialize_next().map_err(to_io_error)?;
        count += 1;
        mark_provider_reference(
            &provider_ref,
            target_npis,
            provider_ref_npis,
            evidence,
            target_keys_by_npi,
        );
        if count % 250_000 == 0 {
            eprintln!("RAW_CLAIM_CHECK_PROVIDER_REFS path={path_label} count={count}");
        }
    }
    json_reader.end_array().map_err(to_io_error)?;
    Ok(count)
}

fn matched_inline_npis(provider_groups: Option<&Value>, target_npis: &HashSet<i64>) -> Vec<i64> {
    let mut matched = BTreeSet::new();
    if let Some(Value::Array(groups)) = provider_groups {
        for group in groups {
            for npi in value_i64_list(group.get("npi")) {
                if target_npis.contains(&npi) {
                    matched.insert(npi);
                }
            }
        }
    }
    matched.into_iter().collect()
}

fn scan_matching_rate<R: Read>(
    json_reader: &mut JsonStreamReader<R>,
    code_targets: &[TargetKey],
    target_npis: &HashSet<i64>,
    provider_ref_npis: &HashMap<i64, Vec<i64>>,
    evidence: &mut HashMap<TargetKey, Evidence>,
    path_label: &str,
) -> io::Result<()> {
    let rate: Value = json_reader.deserialize_next().map_err(to_io_error)?;
    let mut matched_npis = BTreeSet::new();
    for provider_ref in value_i64_list(rate.get("provider_references")) {
        if let Some(npis) = provider_ref_npis.get(&provider_ref) {
            for npi in npis {
                matched_npis.insert(*npi);
            }
        }
    }
    let inline_npis = matched_inline_npis(rate.get("provider_groups"), target_npis);
    for npi in &inline_npis {
        matched_npis.insert(*npi);
    }
    if matched_npis.is_empty() {
        return Ok(());
    }

    let mut service_codes = BTreeSet::new();
    let mut rates = BTreeSet::new();
    if let Some(Value::Array(prices)) = rate.get("negotiated_prices") {
        for price in prices {
            for service_code in value_string_list(price.get("service_code")) {
                service_codes.insert(normalize_pos(&service_code).unwrap_or(service_code));
            }
            if let Some(rate) = price.get("negotiated_rate").and_then(value_string) {
                rates.insert(rate);
            }
        }
    }

    for key in code_targets {
        if !matched_npis.contains(&key.npi) {
            continue;
        }
        let item = evidence.entry(key.clone()).or_default();
        item.raw_npi_code_present = true;
        item.rate_rows += 1;
        if inline_npis.contains(&key.npi) {
            item.inline_provider_groups += 1;
        }
        item.files.insert(path_label.to_string());
        item.service_codes_seen
            .extend(service_codes.iter().cloned());
        item.rates_seen.extend(rates.iter().cloned());
        if let Some(pos) = &key.pos {
            if service_codes.contains(pos) {
                item.raw_npi_code_pos_present = true;
                item.pos_rate_rows += 1;
            }
        }
    }
    Ok(())
}

fn scan_in_network<R: Read>(
    json_reader: &mut JsonStreamReader<R>,
    path_label: &str,
    target_keys_by_code: &HashMap<(String, String), Vec<TargetKey>>,
    target_npis: &HashSet<i64>,
    provider_ref_npis: &HashMap<i64, Vec<i64>>,
    evidence: &mut HashMap<TargetKey, Evidence>,
) -> io::Result<(u64, u64)> {
    let mut item_count = 0;
    let mut rate_count = 0;
    json_reader.begin_array().map_err(to_io_error)?;
    while json_reader.has_next().map_err(to_io_error)? {
        item_count += 1;
        let mut billing_code = String::new();
        let mut billing_code_type = String::new();
        let mut matched_targets: Vec<TargetKey> = Vec::new();

        json_reader.begin_object().map_err(to_io_error)?;
        while json_reader.has_next().map_err(to_io_error)? {
            let name = json_reader.next_name_owned().map_err(to_io_error)?;
            match name.as_str() {
                "billing_code" => {
                    let value: Value = json_reader.deserialize_next().map_err(to_io_error)?;
                    billing_code = value_string(&value).unwrap_or_default();
                }
                "billing_code_type" => {
                    let value: Value = json_reader.deserialize_next().map_err(to_io_error)?;
                    billing_code_type =
                        normalize_code_system(&value_string(&value).unwrap_or_default());
                }
                "negotiated_rates" => {
                    if matched_targets.is_empty() {
                        if let Some(targets) = target_keys_by_code
                            .get(&(billing_code.clone(), billing_code_type.clone()))
                        {
                            matched_targets = targets.clone();
                        }
                    }
                    json_reader.begin_array().map_err(to_io_error)?;
                    if matched_targets.is_empty() {
                        while json_reader.has_next().map_err(to_io_error)? {
                            rate_count += 1;
                            json_reader.skip_value().map_err(to_io_error)?;
                        }
                    } else {
                        while json_reader.has_next().map_err(to_io_error)? {
                            rate_count += 1;
                            scan_matching_rate(
                                json_reader,
                                &matched_targets,
                                target_npis,
                                provider_ref_npis,
                                evidence,
                                path_label,
                            )?;
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
        if item_count % 1_000 == 0 {
            eprintln!("RAW_CLAIM_CHECK_IN_NETWORK path={path_label} items={item_count} rates={rate_count}");
        }
    }
    json_reader.end_array().map_err(to_io_error)?;
    Ok((item_count, rate_count))
}

fn scan_file(
    path: &Path,
    target_keys_by_code: &HashMap<(String, String), Vec<TargetKey>>,
    target_npis: &HashSet<i64>,
    provider_ref_npis: &mut HashMap<i64, Vec<i64>>,
    evidence: &mut HashMap<TargetKey, Evidence>,
    target_keys_by_npi: &HashMap<i64, Vec<TargetKey>>,
) -> io::Result<()> {
    let path_label = path.to_string_lossy().to_string();
    let started = Instant::now();
    eprintln!("RAW_CLAIM_CHECK_FILE_START path={path_label}");
    let reader = open_reader(path)?;
    let mut json_reader = JsonStreamReader::new(reader);
    let mut provider_refs = 0;
    let mut in_network_items = 0;
    let mut negotiated_rates = 0;
    json_reader.begin_object().map_err(to_io_error)?;
    while json_reader.has_next().map_err(to_io_error)? {
        let name = json_reader.next_name_owned().map_err(to_io_error)?;
        match name.as_str() {
            "provider_references" => {
                provider_refs = scan_provider_references(
                    &mut json_reader,
                    &path_label,
                    target_npis,
                    provider_ref_npis,
                    evidence,
                    target_keys_by_npi,
                )?;
            }
            "in_network" => {
                let (items, rates) = scan_in_network(
                    &mut json_reader,
                    &path_label,
                    target_keys_by_code,
                    target_npis,
                    provider_ref_npis,
                    evidence,
                )?;
                in_network_items = items;
                negotiated_rates = rates;
            }
            _ => {
                json_reader.skip_value().map_err(to_io_error)?;
            }
        }
    }
    json_reader.end_object().map_err(to_io_error)?;
    eprintln!(
        "RAW_CLAIM_CHECK_FILE_DONE path={path_label} provider_refs={provider_refs} in_network={in_network_items} negotiated_rates={negotiated_rates} elapsed_seconds={:.2}",
        started.elapsed().as_secs_f64()
    );
    Ok(())
}

fn csv_field(value: &str) -> String {
    if value.contains(',') || value.contains('"') || value.contains('\n') {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_string()
    }
}

fn write_report<W: Write>(
    writer: &mut W,
    targets: &[TargetRow],
    evidence: &HashMap<TargetKey, Evidence>,
) -> io::Result<()> {
    writeln!(
        writer,
        "npi,code,requested_code_system,pos,api_status,checked_code_system,alternate_system,raw_npi_in_provider_refs,raw_npi_code_present,raw_npi_code_pos_present,rate_rows,pos_rate_rows,provider_reference_ids,inline_provider_groups,service_codes_seen,rates_seen,files"
    )?;
    for target in targets {
        for (checked_system, alternate_system) in [
            (target.code_system.clone(), false),
            (alternate_code_system(&target.code_system), true),
        ] {
            let key = TargetKey {
                npi: target.npi,
                code: target.code.clone(),
                requested_system: target.code_system.clone(),
                checked_system: checked_system.clone(),
                pos: target.pos.clone(),
                alternate_system,
            };
            let empty = Evidence::default();
            let item = evidence.get(&key).unwrap_or(&empty);
            let join_i64 = |values: &BTreeSet<i64>| {
                values
                    .iter()
                    .map(|value| value.to_string())
                    .collect::<Vec<_>>()
                    .join("|")
            };
            let join_string =
                |values: &BTreeSet<String>| values.iter().cloned().collect::<Vec<_>>().join("|");
            writeln!(
                writer,
                "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
                target.npi,
                csv_field(&target.code),
                csv_field(&target.code_system),
                csv_field(target.pos.as_deref().unwrap_or("")),
                csv_field(&target.api_status),
                csv_field(&checked_system),
                alternate_system,
                item.raw_npi_in_provider_refs,
                item.raw_npi_code_present,
                item.raw_npi_code_pos_present,
                item.rate_rows,
                item.pos_rate_rows,
                csv_field(&join_i64(&item.provider_reference_ids)),
                item.inline_provider_groups,
                csv_field(&join_string(&item.service_codes_seen)),
                csv_field(&join_string(&item.rates_seen)),
                csv_field(&join_string(&item.files)),
            )?;
        }
    }
    Ok(())
}

fn main() -> io::Result<()> {
    let mut args = env::args().skip(1);
    let target_path = PathBuf::from(args.next().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "usage: raw_claim_check <targets.csv> <raw-file.json.gz>...",
        )
    })?);
    let raw_paths: Vec<PathBuf> = args.map(PathBuf::from).collect();
    if raw_paths.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "at least one raw file path is required",
        ));
    }
    let targets = read_targets(&target_path)?;
    let target_npis: HashSet<i64> = targets.iter().map(|target| target.npi).collect();
    let target_keys_by_code = build_target_keys(&targets);
    let mut target_keys_by_npi: HashMap<i64, Vec<TargetKey>> = HashMap::new();
    for keys in target_keys_by_code.values() {
        for key in keys {
            target_keys_by_npi
                .entry(key.npi)
                .or_default()
                .push(key.clone());
        }
    }
    let mut provider_ref_npis: HashMap<i64, Vec<i64>> = HashMap::new();
    let mut evidence: HashMap<TargetKey, Evidence> = HashMap::new();
    for path in raw_paths {
        scan_file(
            &path,
            &target_keys_by_code,
            &target_npis,
            &mut provider_ref_npis,
            &mut evidence,
            &target_keys_by_npi,
        )?;
    }
    let stdout = io::stdout();
    let mut handle = stdout.lock();
    write_report(&mut handle, &targets, &evidence)?;
    Ok(())
}
