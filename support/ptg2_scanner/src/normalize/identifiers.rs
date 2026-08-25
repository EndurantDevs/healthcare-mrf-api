pub const NPI_MIN: i64 = 1_000_000_000;
pub const NPI_MAX: i64 = 9_999_999_999;

fn to_io_error(error: impl Display) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error.to_string())
}

pub fn normalize_string(value: Option<&Value>) -> Option<String> {
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

pub fn normalize_code(value: Option<&Value>) -> Option<String> {
    normalize_string(value).map(|value| value.to_uppercase())
}

pub fn normalize_code_system(value: Option<&Value>) -> Option<String> {
    normalize_code(value).map(|value| {
        match value.as_str() {
            "CLM_REV_CNTR_CD" | "REVENUE_CENTER" | "REVENUE_CODE" | "REV_CNTR" => "RC",
            "PLACE_OF_SERVICE" | "SERVICE_CODE" => "POS",
            "BILLING_CODE_MODIFIER" | "CPT_MODIFIER" | "HCPCS_MODIFIER" | "MOD" => "MODIFIER",
            "ICD-10-CM" | "ICD10" => "ICD10CM",
            "ICD-10-PCS" => "ICD10PCS",
            "MS-DRG" | "MSDRG" | "DRG" => "MS_DRG",
            "RXCUI" => "RXNORM",
            "SNOMED" | "SNOMEDCT" => "SNOMEDCT_US",
            _ => value.as_str(),
        }
        .to_string()
    })
}

pub fn normalize_catalog_code(value: Option<&Value>, code_system: Option<&str>) -> Option<String> {
    let code = normalize_code(value)?;
    match code_system {
        Some("RC") => Some(zero_pad_numeric_code(code, 4)),
        Some("POS") => Some(zero_pad_numeric_code(code, 2)),
        Some("MS_DRG") => Some(zero_pad_numeric_code(code, 3)),
        Some("ICD10CM" | "ICD10PCS") => Some(code.replace('.', "")),
        _ => Some(code),
    }
}

fn zero_pad_numeric_code(code: String, width: usize) -> String {
    let digits = code
        .chars()
        .filter(char::is_ascii_digit)
        .collect::<String>();
    if digits.is_empty() {
        code
    } else {
        format!("{digits:0>width$}")
    }
}

pub fn normalize_tin_type(value: Option<&Value>) -> String {
    normalize_string(value)
        .unwrap_or_default()
        .trim()
        .to_lowercase()
}

pub fn normalize_tin_value(value: Option<&Value>) -> String {
    normalize_string(value)
        .unwrap_or_default()
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric())
        .collect::<String>()
        .to_uppercase()
}

pub fn int_list(value: Option<&Value>) -> Vec<i64> {
    let mut out = Vec::new();
    match value {
        Some(Value::Array(items)) => {
            for item in items {
                if let Some(text) = normalize_string(Some(item)) {
                    if let Some(number) = parse_integer_text(text.trim()) {
                        out.push(number);
                    }
                }
            }
        }
        Some(item) => {
            if let Some(text) = normalize_string(Some(item)) {
                if let Some(number) = parse_integer_text(text.trim()) {
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

fn parse_integer_text(text: &str) -> Option<i64> {
    text.parse::<i64>().ok().or_else(|| {
        let canonical = canonical_decimal_text(text)?;
        (!canonical.contains('.'))
            .then(|| canonical.parse::<i64>().ok())
            .flatten()
    })
}

pub fn is_valid_npi(value: i64) -> bool {
    (NPI_MIN..=NPI_MAX).contains(&value)
}

pub fn npi_list(value: Option<&Value>) -> Vec<i64> {
    let mut out: Vec<i64> = int_list(value)
        .into_iter()
        .filter(|value| is_valid_npi(*value))
        .collect();
    out.sort_unstable();
    out.dedup();
    out
}

pub fn strict_integer_text(value: &Value, field_name: &str) -> io::Result<String> {
    let Value::Number(number) = value else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{field_name} must be a JSON integer"),
        ));
    };
    if let Some(integer) = number.as_i64() {
        return Ok(integer.to_string());
    }
    if let Some(integer) = number.as_u64() {
        return Ok(integer.to_string());
    }
    let canonical = canonical_decimal_text(&number.to_string()).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{field_name} must be a JSON integer"),
        )
    })?;
    if canonical.contains('.') {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{field_name} must be a JSON integer"),
        ));
    }
    Ok(canonical)
}

pub fn strict_integer(value: &Value, field_name: &str) -> io::Result<i64> {
    let Value::Number(number) = value else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{field_name} must be a JSON integer"),
        ));
    };
    if let Some(integer) = number.as_i64() {
        return Ok(integer);
    }
    if let Some(integer) = number.as_u64() {
        return i64::try_from(integer).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("{field_name} is outside the supported integer range"),
            )
        });
    }
    strict_integer_text(value, field_name)?
        .parse()
        .map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("{field_name} is outside the supported integer range"),
            )
        })
}

pub fn strict_npi_integer(value: &Value, field_name: &str) -> io::Result<i64> {
    if let Value::String(text) = value {
        let bytes = text.as_bytes();
        if text == "0"
            || (bytes.len() == 10 && bytes[0] != b'0' && bytes.iter().all(u8::is_ascii_digit))
        {
            return Ok(text.parse().expect("validated NPI string must fit in i64"));
        }
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{field_name} must be a JSON integer or an exact NPI string"),
        ));
    }
    strict_integer(value, field_name)
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct StrictNpiList {
    pub valid: Vec<i64>,
    pub quarantined: Vec<i64>,
    /// A publisher supplied `[]` instead of the TiC TIN-only marker `[0]`.
    /// The semantic NPI membership is still empty, but callers must surface
    /// this normalization in the scanner attestation.
    pub empty_array_normalized: bool,
}

pub fn strict_npi_partition(value: Option<&Value>) -> io::Result<StrictNpiList> {
    strict_npi_partition_with_policy(value, false)
}

pub fn strict_npi_partition_allow_empty_tin_only(
    value: Option<&Value>,
) -> io::Result<StrictNpiList> {
    strict_npi_partition_with_policy(value, true)
}

fn strict_npi_partition_with_policy(
    value: Option<&Value>,
    allow_empty_tin_only: bool,
) -> io::Result<StrictNpiList> {
    let Some(Value::Array(items)) = value else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "provider group npi must be an array of JSON integers or exact NPI strings",
        ));
    };
    let empty_array_normalized = items.is_empty();
    if empty_array_normalized && !allow_empty_tin_only {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "provider group npi must contain at least one JSON integer or exact NPI string",
        ));
    }
    let mut valid = Vec::with_capacity(items.len());
    let mut quarantined = Vec::new();
    for item in items {
        let npi = strict_npi_integer(item, "provider group npi element")?;
        if npi == 0 {
            // Zero is the TiC TIN-only marker, not an NPI. Some publishers
            // repeat it beside real NPIs; it must not create membership.
            continue;
        } else if is_valid_npi(npi) {
            valid.push(npi);
        } else {
            quarantined.push(npi);
        }
    }
    valid.sort_unstable();
    valid.dedup();
    quarantined.sort_unstable();
    Ok(StrictNpiList {
        valid,
        quarantined,
        empty_array_normalized,
    })
}

pub fn strict_npi_list(value: Option<&Value>) -> io::Result<Vec<i64>> {
    Ok(strict_npi_partition(value)?.valid)
}
