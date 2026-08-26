//! Authenticated normalized blocks for hospital-price services and charges.

use flate2::{read::ZlibDecoder, write::ZlibEncoder, Compression};
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use std::io::{Read, Write};

pub const HOSPITAL_PRICE_SERVICE_BLOCK_MAGIC: &[u8; 8] = b"HPTSERV\0";
pub const HOSPITAL_PRICE_SERVICE_BLOCK_VERSION: u32 = 1;
pub const HOSPITAL_PRICE_SERVICE_BLOCK_HEADER_BYTES: usize = 60;
pub const HOSPITAL_PRICE_SERVICE_BLOCK_MAX_CHARGES: usize = 512;
pub const HOSPITAL_PRICE_SERVICE_BLOCK_MAX_RAW_BYTES: usize = 4 * 1024 * 1024;
pub const HOSPITAL_PRICE_SERVICE_BLOCK_MAX_COMPRESSED_BYTES: usize =
    HOSPITAL_PRICE_SERVICE_BLOCK_MAX_RAW_BYTES + 64 * 1024;
pub const HOSPITAL_PRICE_SERVICE_BLOCK_MAX_DECODED_BYTES: usize = 64 * 1024 * 1024;
pub const HOSPITAL_PRICE_SERVICE_BLOCK_RAW_SIZE_ERROR: &str =
    "hospital price service block raw payload exceeds 4 MiB";

pub type HospitalPriceServiceBlockResult<T> = Result<T, String>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HospitalPriceServiceCode {
    pub code_type: String,
    pub code: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HospitalPriceChargeRow {
    /// Compact key referenced by final-fact blocks.
    pub charge_key: u32,
    pub charge_ordinal: u64,
    pub setting: String,
    pub billing_class: Option<String>,
    pub modifier_codes: Vec<String>,
    pub gross_charge: Option<String>,
    pub discounted_cash: Option<String>,
    pub minimum: Option<String>,
    pub maximum: Option<String>,
    pub additional_generic_notes: Option<String>,
    /// First final-fact ordinal for this charge within the hospital version.
    pub first_fact_ordinal: u64,
    pub fact_count: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HospitalPriceServiceRow {
    pub service_ordinal: u64,
    pub description: String,
    pub drug_unit: Option<String>,
    pub drug_type: Option<String>,
    /// Source-ordinal order is retained; a service may have multiple codes.
    pub codes: Vec<HospitalPriceServiceCode>,
    /// Strict charge-ordinal order; a service may have multiple charges.
    pub charges: Vec<HospitalPriceChargeRow>,
}

fn invalid(message: impl AsRef<str>) -> String {
    format!("hospital price service block {}", message.as_ref())
}

fn valid_decimal(value: &str) -> bool {
    let bytes = value.as_bytes();
    let digits = if bytes.first() == Some(&b'-') {
        &bytes[1..]
    } else {
        bytes
    };
    if digits.is_empty() {
        return false;
    }
    let mut parts = digits.split(|byte| *byte == b'.');
    let integer = parts.next().unwrap_or_default();
    let fraction = parts.next();
    !integer.is_empty()
        && integer.iter().all(u8::is_ascii_digit)
        && fraction.is_none_or(|part| !part.is_empty() && part.iter().all(u8::is_ascii_digit))
        && parts.next().is_none()
}

fn required_text(value: &str, field: &str) -> HospitalPriceServiceBlockResult<()> {
    if value.is_empty() {
        Err(invalid(format!("{field} must be non-empty")))
    } else {
        Ok(())
    }
}

fn optional_text(value: Option<&str>, field: &str) -> HospitalPriceServiceBlockResult<()> {
    if let Some(value) = value {
        required_text(value, field)?;
    }
    Ok(())
}

fn optional_decimal(value: Option<&str>, field: &str) -> HospitalPriceServiceBlockResult<()> {
    if value.is_some_and(|value| !valid_decimal(value)) {
        return Err(invalid(format!("{field} is not an exact lexical decimal")));
    }
    Ok(())
}

fn validate_services(
    services: &[HospitalPriceServiceRow],
) -> HospitalPriceServiceBlockResult<(usize, u64)> {
    if services.is_empty() || services.len() > HOSPITAL_PRICE_SERVICE_BLOCK_MAX_CHARGES {
        return Err(invalid("service count must be between 1 and 512"));
    }
    let mut previous_service = None;
    let mut charge_count = 0usize;
    let mut charge_keys = HashSet::new();
    let mut expected_fact = None;
    let mut first_fact = None;

    for service in services {
        if previous_service.is_some_and(|previous| previous >= service.service_ordinal) {
            return Err(invalid("service ordinals must be strictly increasing"));
        }
        previous_service = Some(service.service_ordinal);
        required_text(&service.description, "description")?;
        if service.drug_unit.is_some() != service.drug_type.is_some() {
            return Err(invalid("drug unit and type must be supplied together"));
        }
        optional_decimal(service.drug_unit.as_deref(), "drug unit")?;
        optional_text(service.drug_type.as_deref(), "drug type")?;
        if service.codes.is_empty() {
            return Err(invalid("service must contain at least one code"));
        }
        for code in &service.codes {
            required_text(&code.code_type, "code type")?;
            required_text(&code.code, "code")?;
        }
        if service.charges.is_empty() {
            return Err(invalid("service must contain at least one charge"));
        }
        charge_count += service.charges.len();
        if charge_count > HOSPITAL_PRICE_SERVICE_BLOCK_MAX_CHARGES {
            return Err(invalid("charge count exceeds 512"));
        }

        let mut previous_charge = None;
        for charge in &service.charges {
            if previous_charge.is_some_and(|previous| previous >= charge.charge_ordinal) {
                return Err(invalid(
                    "charge ordinals must be strictly increasing within a service",
                ));
            }
            previous_charge = Some(charge.charge_ordinal);
            if !charge_keys.insert(charge.charge_key) {
                return Err(invalid("charge key is duplicated"));
            }
            required_text(&charge.setting, "setting")?;
            optional_text(charge.billing_class.as_deref(), "billing class")?;
            for modifier in &charge.modifier_codes {
                required_text(modifier, "modifier code")?;
            }
            optional_decimal(charge.gross_charge.as_deref(), "gross charge")?;
            optional_decimal(charge.discounted_cash.as_deref(), "discounted cash")?;
            optional_decimal(charge.minimum.as_deref(), "minimum")?;
            optional_decimal(charge.maximum.as_deref(), "maximum")?;
            optional_text(
                charge.additional_generic_notes.as_deref(),
                "additional generic notes",
            )?;
            if charge.gross_charge.is_none()
                && charge.discounted_cash.is_none()
                && charge.fact_count == 0
            {
                return Err(invalid(
                    "charge requires gross, discounted cash, or final facts",
                ));
            }
            if let Some(expected) = expected_fact {
                if charge.first_fact_ordinal != expected {
                    return Err(invalid("final-fact ranges are not contiguous"));
                }
            } else {
                first_fact = Some(charge.first_fact_ordinal);
            }
            expected_fact = Some(
                charge
                    .first_fact_ordinal
                    .checked_add(u64::from(charge.fact_count))
                    .ok_or_else(|| invalid("final-fact range overflows u64"))?,
            );
        }
    }
    Ok((charge_count, first_fact.expect("non-empty charges")))
}

struct RawWriter {
    bytes: Vec<u8>,
    exceeded: bool,
}

impl RawWriter {
    fn new() -> Self {
        Self {
            bytes: Vec::new(),
            exceeded: false,
        }
    }

    fn put(&mut self, bytes: &[u8]) {
        if self.exceeded
            || bytes.len() > HOSPITAL_PRICE_SERVICE_BLOCK_MAX_RAW_BYTES - self.bytes.len()
        {
            self.exceeded = true;
            return;
        }
        self.bytes.extend_from_slice(bytes);
    }

    fn u8(&mut self, value: u8) {
        self.put(&[value])
    }

    fn u32(&mut self, value: u32) {
        self.put(&value.to_le_bytes())
    }

    fn u64(&mut self, value: u64) {
        self.put(&value.to_le_bytes())
    }

    fn text(&mut self, value: &str) {
        self.u32(value.len() as u32);
        self.put(value.as_bytes())
    }

    fn optional_text(&mut self, value: Option<&str>) {
        match value {
            None => self.u8(0),
            Some(value) => {
                self.u8(1);
                self.text(value)
            }
        }
    }
}

fn encode_raw(
    services: &[HospitalPriceServiceRow],
    first_fact_ordinal: u64,
) -> HospitalPriceServiceBlockResult<Vec<u8>> {
    let mut raw = RawWriter::new();
    raw.u64(first_fact_ordinal);
    for service in services {
        raw.u64(service.service_ordinal);
        raw.text(&service.description);
        raw.optional_text(service.drug_unit.as_deref());
        raw.optional_text(service.drug_type.as_deref());
        raw.u32(service.codes.len() as u32);
        for code in &service.codes {
            raw.text(&code.code_type);
            raw.text(&code.code);
        }
        raw.u32(service.charges.len() as u32);
        for charge in &service.charges {
            raw.u32(charge.charge_key);
            raw.u64(charge.charge_ordinal);
            raw.text(&charge.setting);
            raw.optional_text(charge.billing_class.as_deref());
            raw.u32(charge.modifier_codes.len() as u32);
            for modifier in &charge.modifier_codes {
                raw.text(modifier);
            }
            raw.optional_text(charge.gross_charge.as_deref());
            raw.optional_text(charge.discounted_cash.as_deref());
            raw.optional_text(charge.minimum.as_deref());
            raw.optional_text(charge.maximum.as_deref());
            raw.optional_text(charge.additional_generic_notes.as_deref());
            raw.u32(charge.fact_count);
        }
    }
    if raw.exceeded {
        Err(HOSPITAL_PRICE_SERVICE_BLOCK_RAW_SIZE_ERROR.to_owned())
    } else {
        Ok(raw.bytes)
    }
}

fn frame_raw(
    raw: &[u8],
    service_count: usize,
    charge_count: usize,
) -> HospitalPriceServiceBlockResult<Vec<u8>> {
    if raw.len() > HOSPITAL_PRICE_SERVICE_BLOCK_MAX_RAW_BYTES {
        return Err(HOSPITAL_PRICE_SERVICE_BLOCK_RAW_SIZE_ERROR.to_owned());
    }
    let mut encoder = ZlibEncoder::new(Vec::new(), Compression::new(6));
    encoder
        .write_all(raw)
        .expect("Vec-backed zlib writes cannot fail");
    let compressed = encoder
        .finish()
        .expect("Vec-backed zlib finalization cannot fail");
    let mut block =
        Vec::with_capacity(HOSPITAL_PRICE_SERVICE_BLOCK_HEADER_BYTES + compressed.len());
    block.extend_from_slice(HOSPITAL_PRICE_SERVICE_BLOCK_MAGIC);
    block.extend_from_slice(&HOSPITAL_PRICE_SERVICE_BLOCK_VERSION.to_le_bytes());
    block.extend_from_slice(&(service_count as u32).to_le_bytes());
    block.extend_from_slice(&(charge_count as u32).to_le_bytes());
    block.extend_from_slice(&(raw.len() as u32).to_le_bytes());
    block.extend_from_slice(&(compressed.len() as u32).to_le_bytes());
    block.extend_from_slice(&Sha256::digest(raw));
    block.extend_from_slice(&compressed);
    Ok(block)
}

pub fn encode_service_block(
    services: &[HospitalPriceServiceRow],
) -> HospitalPriceServiceBlockResult<Vec<u8>> {
    let (charge_count, first_fact_ordinal) = validate_services(services)?;
    let raw = encode_raw(services, first_fact_ordinal)?;
    frame_raw(&raw, services.len(), charge_count)
}

include!("hospital_price_service_block/decode.rs");
include!("hospital_price_service_block/tests.rs");
