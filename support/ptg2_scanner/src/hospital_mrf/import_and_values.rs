#[derive(Debug, Serialize)]
pub struct CopyArtifactSummary {
    kind: &'static str,
    path: String,
    rows: u64,
    bytes: u64,
    sha256: String,
}

#[derive(Debug, Serialize)]
pub struct HospitalMrfSummary {
    contract: &'static str,
    version_id: String,
    schema_version: &'static str,
    schema_revision: &'static str,
    format: &'static str,
    compressed_input_bytes: u64,
    max_fanout_rows: usize,
    max_decompressed_bytes: u64,
    max_output_bytes: u64,
    artifacts: Vec<CopyArtifactSummary>,
}

struct BoundedDecompressedReader<R> {
    inner: R,
    remaining: u64,
}

impl<R: Read> BoundedDecompressedReader<R> {
    fn new(inner: R, max_bytes: u64) -> Self {
        Self {
            inner,
            remaining: max_bytes,
        }
    }
}

impl<R: Read> Read for BoundedDecompressedReader<R> {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        if buffer.is_empty() {
            return Ok(0);
        }
        if self.remaining == 0 {
            let mut extra = [0u8; 1];
            return match self.inner.read(&mut extra)? {
                0 => Ok(0),
                _ => Err(invalid(
                    "hospital MRF decompressed data exceeds configured limit",
                )),
            };
        }
        let allowed = usize::try_from(self.remaining.min(buffer.len() as u64))
            .map_err(|_| invalid("hospital MRF read size exceeds usize"))?;
        let read = self.inner.read(&mut buffer[..allowed])?;
        self.remaining -= read as u64;
        Ok(read)
    }
}

struct BoundedJsonStringReader<R> {
    inner: R,
    max_bytes: u64,
    string_bytes: u64,
    in_string: bool,
    escaped: bool,
}

impl<R: Read> BoundedJsonStringReader<R> {
    fn new(inner: R, max_bytes: u64) -> Self {
        Self {
            inner,
            max_bytes,
            string_bytes: 0,
            in_string: false,
            escaped: false,
        }
    }
}

impl<R: Read> Read for BoundedJsonStringReader<R> {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        let read = self.inner.read(buffer)?;
        for byte in &buffer[..read] {
            if !self.in_string {
                if *byte == b'"' {
                    self.in_string = true;
                    self.string_bytes = 0;
                }
                continue;
            }
            if !self.escaped && *byte == b'"' {
                self.in_string = false;
                continue;
            }
            self.string_bytes = self.string_bytes.saturating_add(1);
            if self.string_bytes > self.max_bytes {
                return Err(invalid("hospital MRF JSON string exceeds configured limit"));
            }
            if self.escaped {
                self.escaped = false;
            } else if *byte == b'\\' {
                self.escaped = true;
            }
        }
        Ok(read)
    }
}

struct BoundedCsvRecordReader<R> {
    inner: R,
    max_bytes: u64,
    record_bytes: u64,
    in_quotes: bool,
}

impl<R: Read> BoundedCsvRecordReader<R> {
    fn new(inner: R, max_bytes: u64) -> Self {
        Self {
            inner,
            max_bytes,
            record_bytes: 0,
            in_quotes: false,
        }
    }
}

impl<R: Read> Read for BoundedCsvRecordReader<R> {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        let read = self.inner.read(buffer)?;
        for byte in &buffer[..read] {
            self.record_bytes = self.record_bytes.saturating_add(1);
            if self.record_bytes > self.max_bytes {
                return Err(invalid("hospital MRF CSV record exceeds configured limit"));
            }
            if *byte == b'"' {
                self.in_quotes = !self.in_quotes;
            } else if *byte == b'\n' && !self.in_quotes {
                self.record_bytes = 0;
            }
        }
        Ok(read)
    }
}

pub fn run_hospital_mrf_cli(args: &[String]) -> io::Result<()> {
    if args.len() != 6 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "usage: ptg2_scanner --hospital-mrf-copy <json|csv-tall|csv-wide> <version_id> <input_path> <output_directory> <max_decompressed_bytes> <max_output_bytes>",
        ));
    }
    let format = InputFormat::parse(&args[0])?;
    let max_decompressed_bytes = parse_positive_bytes(
        &args[4], "max_decompressed_bytes"
    )?;
    let max_output_bytes = parse_max_output_bytes(&args[5])?;
    let summary = import_hospital_mrf_with_limits(
        format,
        &args[1],
        Path::new(&args[2]),
        Path::new(&args[3]),
        load_max_fanout_rows()?,
        max_decompressed_bytes,
        max_output_bytes,
    )?;
    let stdout = io::stdout();
    let mut writer = BufWriter::new(stdout.lock());
    serde_json::to_writer(&mut writer, &summary).map_err(to_io_error)?;
    writer.write_all(b"\n")?;
    writer.flush()
}

#[cfg(test)]
fn import_hospital_mrf(
    format: InputFormat,
    version_id: &str,
    input_path: &Path,
    output_directory: &Path,
    max_output_bytes: u64,
) -> io::Result<HospitalMrfSummary> {
    let max_fanout_rows = load_max_fanout_rows()?;
    import_hospital_mrf_with_limits(
        format,
        version_id,
        input_path,
        output_directory,
        max_fanout_rows,
        DEFAULT_MAX_DECOMPRESSED_BYTES,
        max_output_bytes,
    )
}

fn import_hospital_mrf_with_limits(
    format: InputFormat,
    version_id: &str,
    input_path: &Path,
    output_directory: &Path,
    max_fanout_rows: usize,
    max_decompressed_bytes: u64,
    max_output_bytes: u64,
) -> io::Result<HospitalMrfSummary> {
    if max_fanout_rows == 0 {
        return Err(invalid("hospital MRF max fanout rows must be positive"));
    }
    if max_output_bytes == 0 {
        return Err(invalid("hospital MRF max output bytes must be positive"));
    }
    if max_decompressed_bytes == 0 {
        return Err(invalid(
            "hospital MRF max decompressed bytes must be positive",
        ));
    }
    let version_id = required_text(version_id, "version_id")?;
    if version_id.len() > MAX_VERSION_ID_BYTES {
        return Err(invalid("version_id exceeds 64 UTF-8 bytes"));
    }
    let (compressed_input_bytes, artifacts) = if is_zip(input_path)? {
        import_zip_payload(
            format,
            version_id,
            input_path,
            output_directory,
            max_fanout_rows,
            max_decompressed_bytes,
            max_output_bytes,
        )?
    } else {
        let compressed_input_bytes = Arc::new(AtomicU64::new(0));
        let reader: Box<dyn Read> = match format {
            InputFormat::Json => open_full_scan_json_reader(
                input_path,
                Arc::clone(&compressed_input_bytes),
                &RapidgzipConfig::default(),
            )?,
            InputFormat::TallCsv | InputFormat::WideCsv => {
                strict_utf8_reader(open_full_scan_reader(
                    input_path,
                    Arc::clone(&compressed_input_bytes),
                    &RapidgzipConfig::default(),
                )?)
            }
        };
        let artifacts = parse_hospital_payload(
            format,
            reader,
            version_id,
            output_directory,
            max_fanout_rows,
            max_decompressed_bytes,
            max_output_bytes,
        )?;
        (compressed_input_bytes.load(Ordering::Relaxed), artifacts)
    };

    Ok(HospitalMrfSummary {
        contract: "hospital-mrf-copy-v3",
        version_id: version_id.to_owned(),
        schema_version: HOSPITAL_MRF_SCHEMA_VERSION,
        schema_revision: HOSPITAL_MRF_SCHEMA_REVISION,
        format: format.as_str(),
        compressed_input_bytes,
        max_fanout_rows,
        max_decompressed_bytes,
        max_output_bytes,
        artifacts,
    })
}

fn parse_hospital_payload<R: Read>(
    format: InputFormat,
    reader: R,
    version_id: &str,
    output_directory: &Path,
    max_fanout_rows: usize,
    max_decompressed_bytes: u64,
    max_output_bytes: u64,
) -> io::Result<Vec<CopyArtifactSummary>> {
    parse_hospital_payload_with_limits(
        format,
        reader,
        version_id,
        output_directory,
        (
            max_fanout_rows,
            max_decompressed_bytes,
            max_output_bytes,
            MAX_INPUT_VALUE_BYTES,
        ),
    )
}

fn parse_hospital_payload_with_limits<R: Read>(
    format: InputFormat,
    reader: R,
    version_id: &str,
    output_directory: &Path,
    limits: (usize, u64, u64, u64),
) -> io::Result<Vec<CopyArtifactSummary>> {
    let (max_fanout_rows, max_decompressed_bytes, max_output_bytes, max_input_value_bytes) =
        limits;
    let mut outputs = CopyOutputs::create(output_directory, max_output_bytes)?;
    let reader = BoundedDecompressedReader::new(reader, max_decompressed_bytes);
    match format {
        InputFormat::Json => parse_json(
            BoundedJsonStringReader::new(reader, max_input_value_bytes),
            version_id,
            max_fanout_rows,
            &mut outputs,
        )?,
        InputFormat::TallCsv => {
            parse_csv(
                BoundedCsvRecordReader::new(reader, max_input_value_bytes),
                version_id,
                false,
                max_fanout_rows,
                &mut outputs,
            )?
        }
        InputFormat::WideCsv => parse_csv(
            BoundedCsvRecordReader::new(reader, max_input_value_bytes),
            version_id,
            true,
            max_fanout_rows,
            &mut outputs,
        )?,
    }
    outputs.finish()
}

fn parse_max_output_bytes(value: &str) -> io::Result<u64> {
    parse_positive_bytes(value, "max_output_bytes")
}

fn parse_positive_bytes(value: &str, name: &str) -> io::Result<u64> {
    value
        .parse::<u64>()
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("{name} must be a positive integer"),
            )
        })
}

fn load_max_fanout_rows() -> io::Result<usize> {
    match std::env::var(MAX_FANOUT_ROWS_ENV) {
        Ok(value) => value
            .parse::<usize>()
            .ok()
            .filter(|value| *value > 0)
            .ok_or_else(|| invalid(format!("{MAX_FANOUT_ROWS_ENV} must be a positive integer"))),
        Err(std::env::VarError::NotPresent) => Ok(DEFAULT_MAX_FANOUT_ROWS),
        Err(error) => Err(invalid(format!(
            "cannot read {MAX_FANOUT_ROWS_ENV}: {error}"
        ))),
    }
}

fn hex_digest(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut text = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        text.push(HEX[(byte >> 4) as usize] as char);
        text.push(HEX[(byte & 0x0f) as usize] as char);
    }
    text
}

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

fn to_io_error(error: impl std::fmt::Display) -> io::Error {
    invalid(error.to_string())
}

fn required_text<'a>(value: &'a str, field: &str) -> io::Result<&'a str> {
    let value = value.trim();
    if value.is_empty() {
        Err(invalid(format!("{field} must be a non-empty string")))
    } else {
        Ok(value)
    }
}

fn optional_text(value: &str) -> Option<String> {
    let value = value.trim();
    (!value.is_empty()).then(|| value.to_owned())
}

fn positive_decimal(value: &str, field: &str) -> io::Result<String> {
    let canonical = canonical_decimal_text(value.trim())
        .ok_or_else(|| invalid(format!("{field} must be an exact decimal number")))?;
    if canonical == "0" || canonical.starts_with('-') {
        return Err(invalid(format!("{field} must be greater than zero")));
    }
    Ok(canonical)
}

fn optional_decimal(value: &str, field: &str) -> io::Result<Option<String>> {
    match optional_text(value) {
        Some(value) => positive_decimal(&value, field).map(Some),
        None => Ok(None),
    }
}

fn optional_json_decimal(value: Option<&Number>, field: &str) -> io::Result<Option<String>> {
    value
        .map(|number| positive_decimal(number.as_str(), field))
        .transpose()
}

fn allowed_count(value: &str, normalize_case: bool) -> io::Result<String> {
    let value = if normalize_case {
        required_text(value, "count")?
    } else if value.is_empty() {
        return Err(invalid("count must be a non-empty string"));
    } else {
        value
    };
    if value == "0" {
        return Ok(value.to_owned());
    }
    if (normalize_case && value.eq_ignore_ascii_case("1 through 10"))
        || (!normalize_case && value == "1 through 10")
    {
        return Ok("1 through 10".to_owned());
    }
    if value.starts_with('0') || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(invalid(
            "count must be 0, 1 through 10, or an integer greater than or equal to 11",
        ));
    }
    if value.len() == 1 || (value.len() == 2 && value.as_bytes()[0] == b'1' && value < "11") {
        return Err(invalid(
            "count values from 1 through 10 must use the literal 1 through 10",
        ));
    }
    Ok(value.to_owned())
}

fn canonical_setting(value: &str, normalize_case: bool) -> io::Result<String> {
    let value = if normalize_case {
        value.trim().to_ascii_lowercase()
    } else {
        value.to_owned()
    };
    match value.as_str() {
        "inpatient" => Ok("inpatient".to_owned()),
        "outpatient" => Ok("outpatient".to_owned()),
        "both" => Ok("both".to_owned()),
        _ => Err(invalid("setting must be inpatient, outpatient, or both")),
    }
}

fn canonical_methodology(value: &str, normalize_case: bool) -> io::Result<String> {
    let value = if normalize_case {
        value.trim().to_ascii_lowercase()
    } else {
        value.to_owned()
    };
    if matches!(
        value.as_str(),
        "case rate" | "fee schedule" | "percent of total billed charges" | "per diem" | "other"
    ) {
        Ok(value)
    } else {
        Err(invalid("invalid standard charge methodology"))
    }
}

fn canonical_code_type(value: &str, normalize_case: bool) -> io::Result<String> {
    let value = if normalize_case {
        value.trim().to_ascii_uppercase()
    } else {
        value.to_owned()
    };
    if matches!(
        value.as_str(),
        "CPT"
            | "HCPCS"
            | "ICD"
            | "DRG"
            | "MS-DRG"
            | "R-DRG"
            | "S-DRG"
            | "APS-DRG"
            | "AP-DRG"
            | "APR-DRG"
            | "TRIS-DRG"
            | "APC"
            | "NDC"
            | "HIPPS"
            | "LOCAL"
            | "EAPG"
            | "CDT"
            | "RC"
            | "CDM"
            | "CMG"
            | "MS-LTC-DRG"
    ) {
        Ok(value)
    } else {
        Err(invalid("invalid code type"))
    }
}

fn canonical_drug_type(value: &str, normalize_case: bool) -> io::Result<String> {
    let value = if normalize_case {
        value.trim().to_ascii_uppercase()
    } else {
        value.to_owned()
    };
    if matches!(
        value.as_str(),
        "GR" | "ML" | "ME" | "UN" | "F2" | "GM" | "EA"
    ) {
        Ok(value)
    } else {
        Err(invalid("invalid drug type of measurement"))
    }
}

fn canonical_billing_class(value: &str, normalize_case: bool) -> io::Result<String> {
    let value = if normalize_case {
        value.trim().to_ascii_lowercase()
    } else {
        value.to_owned()
    };
    if matches!(value.as_str(), "professional" | "facility" | "both") {
        Ok(value)
    } else {
        Err(invalid(
            "billing_class must be professional, facility, or both",
        ))
    }
}
