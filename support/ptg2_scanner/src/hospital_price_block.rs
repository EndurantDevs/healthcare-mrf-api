//! Authenticated normalized blocks for final hospital-price fact rows.

use flate2::{read::ZlibDecoder, write::ZlibEncoder, Compression};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::io::{Read, Write};

pub const HOSPITAL_PRICE_FACT_BLOCK_MAGIC: &[u8; 8] = b"HPTFACT\0";
pub const HOSPITAL_PRICE_FACT_BLOCK_VERSION: u32 = 1;
pub const HOSPITAL_PRICE_FACT_BLOCK_HEADER_BYTES: usize = 56;
pub const HOSPITAL_PRICE_FACT_BLOCK_MAX_ROWS: usize = 512;
pub const HOSPITAL_PRICE_FACT_BLOCK_MAX_RAW_BYTES: usize = 4 * 1024 * 1024;
pub const HOSPITAL_PRICE_FACT_BLOCK_MAX_COMPRESSED_BYTES: usize =
    HOSPITAL_PRICE_FACT_BLOCK_MAX_RAW_BYTES + 64 * 1024;
pub const HOSPITAL_PRICE_FACT_BLOCK_MAX_DECODED_TEXT_BYTES: usize = 64 * 1024 * 1024;

const LANE_COUNT: usize = 17;
const RAW_HEADER_BYTES: usize = 4 + LANE_COUNT * 8;
const NONE_LENGTH: u32 = u32::MAX;

const PAYER_PLAN_DICTIONARY: usize = 0;
const ALGORITHM_DICTIONARY: usize = 1;
const METHODOLOGY_DICTIONARY: usize = 2;
const ALLOWED_COUNT_DICTIONARY: usize = 3;
const PAYER_NOTE_DICTIONARY: usize = 4;
const CHARGE_KEYS: usize = 5;
const PAYER_PLAN_IDS: usize = 6;
const NEGOTIATED_DOLLARS: usize = 7;
const NEGOTIATED_PERCENTAGES: usize = 8;
const ALGORITHM_IDS: usize = 9;
const METHODOLOGY_IDS: usize = 10;
const MEDIAN_AMOUNTS: usize = 11;
const PERCENTILE_10_AMOUNTS: usize = 12;
const PERCENTILE_90_AMOUNTS: usize = 13;
const ALLOWED_COUNT_IDS: usize = 14;
const PAYER_NOTE_IDS: usize = 15;
const COMPARISON_AMOUNTS: usize = 16;

pub type HospitalPriceBlockResult<T> = Result<T, String>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HospitalPriceFactRow {
    /// Compact key into the external relational charge/service tables.
    pub charge_key: u32,
    pub payer_name: String,
    pub plan_name: String,
    pub negotiated_dollar: Option<String>,
    pub negotiated_percentage: Option<String>,
    pub negotiated_algorithm: Option<String>,
    pub methodology: String,
    pub median_amount: Option<String>,
    pub percentile_10: Option<String>,
    pub percentile_90: Option<String>,
    pub allowed_count: Option<String>,
    pub additional_payer_notes: Option<String>,
    pub comparison_amount: Option<String>,
}

fn invalid(message: impl AsRef<str>) -> String {
    format!("hospital price fact block {}", message.as_ref())
}

#[derive(Default)]
struct TextDictionary {
    entries: Vec<String>,
    ids: HashMap<String, u16>,
}

impl TextDictionary {
    fn intern(&mut self, value: &str) -> HospitalPriceBlockResult<u16> {
        if let Some(id) = self.ids.get(value) {
            return Ok(*id);
        }
        if self.entries.len() >= HOSPITAL_PRICE_FACT_BLOCK_MAX_ROWS {
            return Err(invalid("dictionary has too many entries"));
        }
        let id = self.entries.len() as u16;
        self.entries.push(value.to_owned());
        self.ids.insert(value.to_owned(), id);
        Ok(id)
    }

    fn encode(&self) -> HospitalPriceBlockResult<Vec<u8>> {
        let mut lane = Vec::new();
        put_u16(&mut lane, self.entries.len() as u16);
        for entry in &self.entries {
            put_text(&mut lane, entry)?;
        }
        Ok(lane)
    }
}

#[derive(Default)]
struct PayerPlanDictionary {
    entries: Vec<(String, String)>,
    ids: HashMap<(String, String), u16>,
}

impl PayerPlanDictionary {
    fn intern(&mut self, payer: &str, plan: &str) -> HospitalPriceBlockResult<u16> {
        let key = (payer.to_owned(), plan.to_owned());
        if let Some(id) = self.ids.get(&key) {
            return Ok(*id);
        }
        if self.entries.len() >= HOSPITAL_PRICE_FACT_BLOCK_MAX_ROWS {
            return Err(invalid("payer-plan dictionary has too many entries"));
        }
        let id = self.entries.len() as u16;
        self.entries.push(key.clone());
        self.ids.insert(key, id);
        Ok(id)
    }

    fn encode(&self) -> HospitalPriceBlockResult<Vec<u8>> {
        let mut lane = Vec::new();
        put_u16(&mut lane, self.entries.len() as u16);
        for (payer, plan) in &self.entries {
            put_text(&mut lane, payer)?;
            put_text(&mut lane, plan)?;
        }
        Ok(lane)
    }
}

include!("hospital_price_block/encode.rs");
include!("hospital_price_block/decode.rs");
include!("hospital_price_block/tests.rs");
