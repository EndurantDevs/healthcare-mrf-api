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
