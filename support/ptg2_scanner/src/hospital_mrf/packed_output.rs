const PG_BINARY_COPY_SIGNATURE: &[u8; 11] = b"PGCOPY\n\xff\r\n\0";
const PG_BINARY_COPY_FIELD_COUNT: i16 = 13;
const HOSPITAL_PRICE_SERVICE_BLOCK_KIND: i16 = 1;
const HOSPITAL_PRICE_FACT_BLOCK_KIND: i16 = 2;
const HOSPITAL_PRICE_CODE_SELECTOR_BLOCK_KIND: i16 = 3;
const HOSPITAL_PRICE_PAYER_PLAN_SELECTOR_BLOCK_KIND: i16 = 4;
const PACKED_FACT_TARGET_BYTES: usize = 128 * 1024;
const SELECTOR_SPOOL_RECORD_BYTES: usize = 13;
const SELECTOR_SORT_RECORD_LIMIT: usize = 262_144;
const SELECTOR_PACK_MAX_ROWS: usize = 256;
const MAX_SELECTOR_KEYS: usize = 1_000_000;
const MAX_SELECTOR_KEY_MEMORY_BYTES: u64 = 256 * 1024 * 1024;
const SELECTOR_KEY_MEMORY_OVERHEAD_BYTES: u64 = 256;

fn or_invalid<T>(value: Option<T>, message: &'static str) -> io::Result<T> {
    match value {
        Some(value) => Ok(value),
        None => Err(invalid(message)),
    }
}

fn map_invalid<T, E>(value: Result<T, E>, message: &'static str) -> io::Result<T> {
    match value {
        Ok(value) => Ok(value),
        Err(_) => Err(invalid(message)),
    }
}

#[derive(Debug, Serialize)]
struct PackedArtifactSummary {
    kind: &'static str,
    path: String,
    rows: u64,
    bytes: u64,
    sha256: String,
}

#[derive(Debug, Serialize)]
struct PackedOutputSummary {
    artifacts: Vec<PackedArtifactSummary>,
    root: PackedRootSummary,
}

#[derive(Debug, Serialize)]
struct PackedRootSummary {
    service_count: u64,
    charge_count: u64,
    fact_count: u64,
    code_selector_key_count: u64,
    payer_plan_selector_key_count: u64,
    code_selector_ref_count: u64,
    payer_plan_selector_ref_count: u64,
    code_selector_page_count: u64,
    payer_plan_selector_page_count: u64,
    service_block_count: u64,
    fact_block_count: u64,
    code_selector_block_count: u64,
    payer_plan_selector_block_count: u64,
    selector_spool_bytes: u64,
    peak_scratch_bytes: u64,
}

struct SelectorPreflight {
    page_counts: Vec<u32>,
    code_ref_count: u64,
    payer_plan_ref_count: u64,
    code_page_count: u64,
    payer_plan_page_count: u64,
}

#[derive(Clone, Copy)]
struct PackedRecordMetadata {
    block_kind: i16,
    block_ordinal: u64,
    logical_first: u64,
    logical_count: u32,
    secondary_first: u64,
    secondary_count: u32,
    page_index: u32,
    page_count: u32,
    key_sha256: Option<[u8; 32]>,
    parent_sha256: Option<[u8; 32]>,
}

struct PackedSink {
    kind: &'static str,
    partial_path: PathBuf,
    final_path: PathBuf,
    writer: Option<BufWriter<DigestWriter>>,
    version_id: String,
    rows: u64,
    keep: bool,
    final_owned: bool,
}

impl PackedSink {
    fn create(
        output_directory: &Path,
        kind: &'static str,
        version_id: &str,
        aggregate_bytes: Arc<AtomicU64>,
        max_output_bytes: u64,
    ) -> io::Result<Self> {
        let final_path = output_directory.join(format!("{kind}.copy"));
        let partial_path = output_directory.join(format!(".{kind}.copy.partial"));
        if path_entry_exists(&final_path)? || path_entry_exists(&partial_path)? {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!(
                    "hospital MRF packed output already exists: {}",
                    final_path.display()
                ),
            ));
        }
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&partial_path)?;
        let mut sink = Self {
            kind,
            partial_path,
            final_path,
            writer: Some(BufWriter::new(DigestWriter {
                file,
                digest: Sha256::new(),
                bytes: 0,
                aggregate_bytes,
                max_output_bytes,
            })),
            version_id: version_id.to_owned(),
            rows: 0,
            keep: false,
            final_owned: false,
        };
        let mut header = Vec::with_capacity(19);
        header.extend_from_slice(PG_BINARY_COPY_SIGNATURE);
        header.extend_from_slice(&0i32.to_be_bytes());
        header.extend_from_slice(&0i32.to_be_bytes());
        sink.writer_mut()?.write_all(&header)?;
        Ok(sink)
    }

    fn writer_mut(&mut self) -> io::Result<&mut BufWriter<DigestWriter>> {
        or_invalid(
            self.writer.as_mut(),
            "hospital MRF packed sink is already closed",
        )
    }

    fn write_record(&mut self, metadata: PackedRecordMetadata, payload: &[u8]) -> io::Result<()> {
        let block_ordinal = map_invalid(
            i64::try_from(metadata.block_ordinal),
            "hospital MRF packed block ordinal exceeds PostgreSQL int8",
        )?;
        let logical_first = map_invalid(
            i64::try_from(metadata.logical_first),
            "hospital MRF packed logical first exceeds PostgreSQL int8",
        )?;
        let logical_count = map_invalid(
            i32::try_from(metadata.logical_count),
            "hospital MRF packed logical count exceeds PostgreSQL int4",
        )?;
        let secondary_first = map_invalid(
            i64::try_from(metadata.secondary_first),
            "hospital MRF packed secondary first exceeds PostgreSQL int8",
        )?;
        let secondary_count = map_invalid(
            i32::try_from(metadata.secondary_count),
            "hospital MRF packed secondary count exceeds PostgreSQL int4",
        )?;
        let page_index = map_invalid(
            i32::try_from(metadata.page_index),
            "hospital MRF packed page index exceeds PostgreSQL int4",
        )?;
        let page_count = map_invalid(
            i32::try_from(metadata.page_count),
            "hospital MRF packed page count exceeds PostgreSQL int4",
        )?;
        let payload_sha256: [u8; 32] = Sha256::digest(payload).into();
        let version_id = self.version_id.as_bytes();
        let writer = or_invalid(
            self.writer.as_mut(),
            "hospital MRF packed sink is already closed",
        )?;
        writer.write_all(&PG_BINARY_COPY_FIELD_COUNT.to_be_bytes())?;
        write_binary_copy_field(writer, Some(version_id))?;
        write_binary_copy_field(writer, Some(&metadata.block_kind.to_be_bytes()))?;
        write_binary_copy_field(writer, Some(&block_ordinal.to_be_bytes()))?;
        write_binary_copy_field(writer, Some(&logical_first.to_be_bytes()))?;
        write_binary_copy_field(writer, Some(&logical_count.to_be_bytes()))?;
        write_binary_copy_field(writer, Some(&secondary_first.to_be_bytes()))?;
        write_binary_copy_field(writer, Some(&secondary_count.to_be_bytes()))?;
        write_binary_copy_field(writer, Some(&page_index.to_be_bytes()))?;
        write_binary_copy_field(writer, Some(&page_count.to_be_bytes()))?;
        write_binary_copy_field(
            writer,
            match metadata.key_sha256.as_ref() {
                Some(value) => Some(value.as_slice()),
                None => None,
            },
        )?;
        write_binary_copy_field(
            writer,
            match metadata.parent_sha256.as_ref() {
                Some(value) => Some(value.as_slice()),
                None => None,
            },
        )?;
        write_binary_copy_field(writer, Some(&payload_sha256))?;
        write_binary_copy_field(writer, Some(payload))?;
        self.rows = or_invalid(
            self.rows.checked_add(1),
            "hospital MRF packed record count overflows",
        )?;
        Ok(())
    }

    fn finish(&mut self) -> io::Result<PackedArtifactSummary> {
        let Some(mut writer) = self.writer.take() else {
            return Err(invalid("hospital MRF packed sink is already closed"));
        };
        writer.write_all(&(-1i16).to_be_bytes())?;
        writer.flush()?;
        writer.get_ref().file.sync_all()?;
        let bytes = writer.get_ref().bytes;
        let sha256 = hex_digest(writer.get_ref().digest.clone().finalize().as_slice());
        drop(writer);
        fs::hard_link(&self.partial_path, &self.final_path)?;
        self.final_owned = true;
        fs::remove_file(&self.partial_path)?;
        Ok(PackedArtifactSummary {
            kind: self.kind,
            path: self.final_path.display().to_string(),
            rows: self.rows,
            bytes,
            sha256,
        })
    }
}

fn write_binary_copy_field<W: Write>(writer: &mut W, value: Option<&[u8]>) -> io::Result<()> {
    let Some(value) = value else {
        return writer.write_all(&(-1i32).to_be_bytes());
    };
    let length = value.len() as i32;
    writer.write_all(&length.to_be_bytes())?;
    writer.write_all(value)
}

impl Drop for PackedSink {
    fn drop(&mut self) {
        if self.keep {
            return;
        }
        drop(self.writer.take());
        let _ = fs::remove_file(&self.partial_path);
        if self.final_owned {
            let _ = fs::remove_file(&self.final_path);
        }
    }
}

#[derive(Clone)]
struct PendingPackedService {
    service_ordinal: u64,
    description: String,
    drug_unit: Option<String>,
    drug_type: Option<String>,
    codes: Vec<crate::hospital_price_service_block::HospitalPriceServiceCode>,
    selector_code_indexes: Vec<usize>,
    charges: Vec<crate::hospital_price_service_block::HospitalPriceChargeRow>,
    had_charge: bool,
}

fn packed_service_code_indexes(row: &ServiceRow) -> io::Result<Vec<usize>> {
    let mut raw_bytes = 4usize;
    for code in &row.codes {
        for value in [&code.code_type, &code.code] {
            if value.len()
                > crate::hospital_price_selector_block::HOSPITAL_PRICE_SELECTOR_BLOCK_MAX_KEY_BYTES
            {
                return Err(invalid(
                    "hospital MRF packed selector key component exceeds 1 MiB",
                ));
            }
            raw_bytes += 4 + value.len();
        }
        if raw_bytes
            > crate::hospital_price_service_block::HOSPITAL_PRICE_SERVICE_BLOCK_MAX_RAW_BYTES
        {
            return Err(invalid(
                "hospital MRF packed service code data exceeds 4 MiB",
            ));
        }
    }
    let mut indexes = (0..row.codes.len()).collect::<Vec<_>>();
    indexes.sort_unstable_by(|left, right| {
        (&row.codes[*left].code_type, &row.codes[*left].code)
            .cmp(&(&row.codes[*right].code_type, &row.codes[*right].code))
    });
    indexes.dedup_by(|left, right| {
        row.codes[*left].code_type == row.codes[*right].code_type
            && row.codes[*left].code == row.codes[*right].code
    });
    Ok(indexes)
}

impl PendingPackedService {
    fn from_row(service_ordinal: u64, row: &ServiceRow) -> io::Result<Self> {
        let selector_code_indexes = packed_service_code_indexes(row)?;
        Ok(Self {
            service_ordinal,
            description: row.description.clone(),
            drug_unit: row.drug_unit.clone(),
            drug_type: row.drug_type.clone(),
            codes: row
                .codes
                .iter()
                .map(
                    |code| crate::hospital_price_service_block::HospitalPriceServiceCode {
                        code_type: code.code_type.clone(),
                        code: code.code.clone(),
                    },
                )
                .collect(),
            selector_code_indexes,
            charges: Vec::new(),
            had_charge: false,
        })
    }

    fn row_with_charges(
        &self,
        charges: Vec<crate::hospital_price_service_block::HospitalPriceChargeRow>,
    ) -> crate::hospital_price_service_block::HospitalPriceServiceRow {
        crate::hospital_price_service_block::HospitalPriceServiceRow {
            service_ordinal: self.service_ordinal,
            description: self.description.clone(),
            drug_unit: self.drug_unit.clone(),
            drug_type: self.drug_type.clone(),
            codes: self.codes.clone(),
            charges,
        }
    }
}

struct CurrentPackedCharge {
    service_ordinal: u64,
    charge_ordinal: u64,
    charge_key: u32,
    row: ChargeRow,
    first_fact_ordinal: u64,
}

struct PackedOutputBuilder {
    sinks: Vec<PackedSink>,
    max_output_bytes: u64,
    current_service: Option<PendingPackedService>,
    current_charge: Option<CurrentPackedCharge>,
    service_rows: Vec<crate::hospital_price_service_block::HospitalPriceServiceRow>,
    service_charge_count: usize,
    fact_rows: Vec<crate::hospital_price_block::HospitalPriceFactRow>,
    fact_first_ordinal: u64,
    next_charge_key: u32,
    next_fact_ordinal: u64,
    service_count: u64,
    written_charge_count: u64,
    written_fact_count: u64,
    selector_block_counts: [u64; 2],
    last_service_ordinal: Option<u64>,
    last_charge_ordinal: Option<u64>,
    selector_keys: Vec<crate::hospital_price_selector_block::HospitalPriceSelectorKey>,
    selector_key_ordinals:
        BTreeMap<crate::hospital_price_selector_block::HospitalPriceSelectorKey, u32>,
    selector_key_memory_bytes: u64,
    selector_spool_path: PathBuf,
    selector_sorted_path: PathBuf,
    selector_sort_directory: PathBuf,
    selector_spool: Option<BufWriter<File>>,
    selector_spool_bytes: u64,
    selector_sorted_owned: bool,
}

include!("packed_output/builder_ingest.rs");
include!("packed_output/builder_selectors.rs");
include!("packed_output/drop.rs");
include!("packed_output/selectors.rs");
include!("packed_output/tests.rs");
