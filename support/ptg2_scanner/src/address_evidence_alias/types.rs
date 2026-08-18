#[derive(Debug, Deserialize)]
struct RunConfig {
    run_id: String,
    state_code: Option<String>,
    zip_prefix: Option<String>,
    retry_shadow_run_id: Option<String>,
}

#[derive(Debug, Clone)]
struct RawAddress {
    first: Option<String>,
    second: Option<String>,
    city: Option<String>,
    state: Option<String>,
    postal: Option<String>,
    country: Option<String>,
}

#[derive(Debug)]
struct ArchiveInput {
    key: Option<String>,
    identity: Option<String>,
    precision: Option<String>,
    raw: RawAddress,
    strict_bits: i32,
    merged: bool,
    stored_state: Option<String>,
    stored_zip: Option<String>,
}

#[derive(Debug)]
struct ArchiveRow {
    key: Option<String>,
    identity: Option<String>,
    precision: Option<String>,
    raw: RawAddress,
    strict_bits: i32,
    merged: bool,
    stored_state: Option<String>,
    stored_zip: Option<String>,
    features: AddressEvidenceFeatures,
    marker_features: AddressEvidenceFeatures,
    raw_geo: Option<String>,
    stored_geo: Option<String>,
    global_valid: bool,
    visible_valid: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StreetRelation {
    Same,
    Direction,
    Suffix,
}

#[derive(Debug)]
struct PairMatch {
    npi: u64,
    source: u32,
    target: u32,
    rule: &'static str,
    effective_first: String,
    relation: StreetRelation,
    priority: u16,
}

#[derive(Debug)]
struct PreferredPair {
    source: u32,
    target: u32,
    rule: &'static str,
    evidence_npi: u64,
    evidence_npi_count: u32,
    marker_conflict: bool,
}

#[derive(Debug, Default, Clone)]
struct MarkerSet {
    direction_count: u8,
    suffix_count: u8,
    direction: Option<String>,
    suffix: Option<String>,
}

#[derive(Debug, Serialize)]
struct RunSummary {
    contract: &'static str,
    archive_rows: u64,
    membership_rows: u64,
    visible_memberships: u64,
    source_count: u64,
    active_skipped: u64,
    pair_count: u64,
    pair_match_count: u64,
    global_pair_count: u64,
    candidate_rows: u64,
    output_sha256: String,
    elapsed_ms: u128,
}

type ActiveAliases = (HashMap<String, Option<String>>, HashSet<String>);

struct MembershipInput {
    rows: Vec<(u64, u32)>,
    input_rows: u64,
    source_count: u64,
    active_skipped: u64,
}

struct SourceAssessment {
    source: (u32, String),
    markers: MarkerSet,
    related: Vec<u32>,
    examined: u64,
}

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

fn copy_fields(line: &str, expected: usize, kind: &str) -> io::Result<Vec<Option<String>>> {
    let fields: Vec<Option<String>> = line
        .trim_end_matches(['\r', '\n'])
        .split('\t')
        .map(decode_copy_field)
        .collect();
    if fields.len() != expected {
        return Err(invalid(format!(
            "{kind} COPY row has {} fields, expected {expected}",
            fields.len()
        )));
    }
    Ok(fields)
}

fn parse_archive(path: &Path) -> io::Result<Vec<ArchiveRow>> {
    let mut inputs = Vec::new();
    for line in BufReader::new(File::open(path)?).lines() {
        let fields = copy_fields(&line?, 13, "address evidence archive")?;
        let strict_bits = fields[9]
            .as_deref()
            .unwrap_or("0")
            .parse::<i32>()
            .map_err(|_| invalid("address evidence strict bits are not an integer"))?;
        if strict_bits < 0 {
            return Err(invalid("address evidence strict bits must be nonnegative"));
        }
        inputs.push(ArchiveInput {
            key: fields[0].clone(),
            identity: fields[1].clone(),
            precision: fields[2].clone(),
            raw: RawAddress {
                first: fields[3].clone(),
                second: fields[4].clone(),
                city: fields[5].clone(),
                state: fields[6].clone(),
                postal: fields[7].clone(),
                country: fields[8].clone(),
            },
            strict_bits,
            merged: fields[10].is_some(),
            stored_state: fields[11].clone(),
            stored_zip: fields[12].clone(),
        });
    }
    Ok(inputs.into_par_iter().map(build_archive_row).collect())
}

fn geo(state: Option<&str>, zip: Option<&str>, country: &str) -> Option<String> {
    Some(format!("{}|{}|{country}", state?, zip?))
}

fn identity_precision(identity: Option<&str>) -> Option<&str> {
    identity.and_then(|value| value.split('|').nth(7))
}

fn build_archive_row(input: ArchiveInput) -> ArchiveRow {
    let raw = &input.raw;
    let canonical = canonicalize_address(
        raw.first.as_deref(),
        raw.second.as_deref(),
        raw.city.as_deref(),
        raw.state.as_deref(),
        raw.postal.as_deref(),
        raw.country.as_deref(),
    );
    let features = address_evidence_features(raw.first.as_deref(), raw.second.as_deref());
    let marker_features = address_evidence_features(raw.first.as_deref(), None);
    let raw_geo = geo(
        canonical.state_code.as_deref(),
        canonical.zip5.as_deref(),
        &canonical.country_code,
    );
    let stored_geo = geo(
        input.stored_state.as_deref(),
        input.stored_zip.as_deref(),
        input.raw.country.as_deref().unwrap_or("US"),
    );
    let precision = input
        .precision
        .as_deref()
        .or_else(|| identity_precision(input.identity.as_deref()));
    let global_valid = !input.merged
        && precision == Some("street")
        && input.key.as_deref() == canonical.address_key.as_deref()
        && input.identity.as_deref() == canonical.identity_key.as_deref();
    let visible_valid = global_valid
        && input.stored_state == canonical.state_code
        && input.stored_zip == canonical.zip5
        && input.raw.country.as_deref().unwrap_or("US") == canonical.country_code
        && input.stored_state.is_some()
        && input.stored_zip.is_some()
        && canonical.line1_norm.is_some();
    ArchiveRow {
        key: input.key,
        identity: input.identity,
        precision: input.precision,
        raw: input.raw,
        strict_bits: input.strict_bits,
        merged: input.merged,
        stored_state: input.stored_state,
        stored_zip: input.stored_zip,
        features,
        marker_features,
        raw_geo,
        stored_geo,
        global_valid,
        visible_valid,
    }
}

fn in_scope(row: &ArchiveRow, config: &RunConfig) -> bool {
    config
        .state_code
        .as_deref()
        .is_none_or(|state| row.stored_state.as_deref() == Some(state))
        && config.zip_prefix.as_deref().is_none_or(|prefix| {
            row.stored_zip
                .as_deref()
                .is_some_and(|zip| zip.starts_with(prefix))
        })
}
