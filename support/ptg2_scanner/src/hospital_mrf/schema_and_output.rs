pub const HOSPITAL_MRF_SCHEMA_VERSION: &str = "3.0.0";
pub const HOSPITAL_MRF_SCHEMA_REVISION: &str = "5333564a710f80d7740180b9ffab8dbdcba9b502";

pub const MRF_COPY_COLUMNS: &[&str] = &[
    "version_id",
    "hospital_name",
    "last_updated_on",
    "version",
    "attestation_text",
    "confirm_attestation",
    "attester_name",
    "financial_aid_policy",
];
pub const LOCATION_COPY_COLUMNS: &[&str] =
    &["version_id", "ordinal", "location_name", "hospital_address"];
pub const NPI_COPY_COLUMNS: &[&str] = &["version_id", "ordinal", "type_2_npi"];
pub const LICENSE_COPY_COLUMNS: &[&str] = &["version_id", "ordinal", "license_number", "state"];
pub const CONTRACT_PROVISION_COPY_COLUMNS: &[&str] = &[
    "version_id",
    "provision_ordinal",
    "payer_name",
    "plan_name",
    "provisions",
];
pub const SERVICE_COPY_COLUMNS: &[&str] = &[
    "version_id",
    "service_ordinal",
    "description",
    "drug_unit",
    "drug_type",
];
pub const CODE_COPY_COLUMNS: &[&str] = &[
    "version_id",
    "service_ordinal",
    "code_ordinal",
    "code_type",
    "code",
];
pub const CHARGE_COPY_COLUMNS: &[&str] = &[
    "version_id",
    "service_ordinal",
    "charge_ordinal",
    "setting",
    "modifier_codes",
    "gross_charge",
    "discounted_cash",
    "minimum",
    "maximum",
    "additional_generic_notes",
    "billing_class",
];
pub const PAYER_CHARGE_COPY_COLUMNS: &[&str] = &[
    "version_id",
    "service_ordinal",
    "charge_ordinal",
    "payer_ordinal",
    "payer_name",
    "plan_name",
    "standard_charge_dollar",
    "standard_charge_percentage",
    "standard_charge_algorithm",
    "median_amount",
    "percentile_10",
    "percentile_90",
    "allowed_count",
    "methodology",
    "additional_payer_notes",
];
pub const MODIFIER_COPY_COLUMNS: &[&str] = &[
    "version_id",
    "modifier_ordinal",
    "code",
    "description",
    "setting",
    "additional_generic_notes",
];
pub const MODIFIER_PAYER_COPY_COLUMNS: &[&str] = &[
    "version_id",
    "modifier_ordinal",
    "payer_ordinal",
    "payer_name",
    "plan_name",
    "description",
    "standard_charge_dollar",
    "standard_charge_percentage",
    "standard_charge_algorithm",
];

const ATTESTATION_TEXT: &str = "To the best of its knowledge and belief, this hospital has included all applicable standard charge information in accordance with the requirements of 45 CFR 180.50, and the information encoded is true, accurate, and complete as of the date in the file. This hospital has included all payer-specific negotiated charges in dollars that can be expressed as a dollar amount. For payer-specific negotiated charges that cannot be expressed as a dollar amount in the machine-readable file or not knowable in advance, the hospital attests that the payer-specific negotiated charge is based on a contractual algorithm, percentage or formula that precludes the provision of a dollar amount and has provided all necessary information available to the hospital for the public to be able to derive the dollar amount, including, but not limited to, the specific fee schedule or components referenced in such percentage, algorithm or formula.";
const MAX_VERSION_ID_BYTES: usize = 64;
const MAX_FANOUT_ROWS_ENV: &str = "HLTHPRT_HOSPITAL_MRF_MAX_FANOUT_ROWS";
const DEFAULT_MAX_FANOUT_ROWS: usize = 100_000;
#[cfg(test)]
const DEFAULT_MAX_DECOMPRESSED_BYTES: u64 = 64 * 1024 * 1024 * 1024;
const MAX_INPUT_VALUE_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum InputFormat {
    Json,
    TallCsv,
    WideCsv,
}

impl InputFormat {
    fn parse(value: &str) -> io::Result<Self> {
        match value {
            "json" => Ok(Self::Json),
            "csv-tall" => Ok(Self::TallCsv),
            "csv-wide" => Ok(Self::WideCsv),
            _ => Err(invalid(
                "hospital MRF format must be json, csv-tall, or csv-wide",
            )),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Json => "json",
            Self::TallCsv => "csv-tall",
            Self::WideCsv => "csv-wide",
        }
    }
}

#[derive(Clone, Copy, Debug)]
#[repr(usize)]
enum CopyKind {
    Mrf = 0,
    Location = 1,
    Npi = 2,
    License = 3,
    ContractProvision = 4,
    Service = 5,
    Code = 6,
    Charge = 7,
    PayerCharge = 8,
    Modifier = 9,
    ModifierPayer = 10,
}

impl CopyKind {
    const ALL: [Self; 11] = [
        Self::Mrf,
        Self::Location,
        Self::Npi,
        Self::License,
        Self::ContractProvision,
        Self::Service,
        Self::Code,
        Self::Charge,
        Self::PayerCharge,
        Self::Modifier,
        Self::ModifierPayer,
    ];

    fn name(self) -> &'static str {
        match self {
            Self::Mrf => "mrf",
            Self::Location => "location",
            Self::Npi => "npi",
            Self::License => "license",
            Self::ContractProvision => "contract_provision",
            Self::Service => "service",
            Self::Code => "code",
            Self::Charge => "charge",
            Self::PayerCharge => "payer_charge",
            Self::Modifier => "modifier",
            Self::ModifierPayer => "modifier_payer",
        }
    }
}

struct DigestWriter {
    file: File,
    digest: Sha256,
    bytes: u64,
    aggregate_bytes: Arc<AtomicU64>,
    max_output_bytes: u64,
}

impl Write for DigestWriter {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        let requested = match u64::try_from(buffer.len()) {
            Ok(requested) => requested,
            Err(_) => return Err(invalid("hospital MRF COPY write size exceeds u64")),
        };
        if self
            .aggregate_bytes
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |bytes| {
                bytes
                    .checked_add(requested)
                    .filter(|next| *next <= self.max_output_bytes)
            })
            .is_err()
        {
            return Err(invalid(format!(
                "hospital MRF COPY output exceeds configured limit {} bytes",
                self.max_output_bytes
            )));
        }
        let written = match self.file.write(buffer) {
            Ok(written) => written,
            Err(error) => {
                self.aggregate_bytes.fetch_sub(requested, Ordering::Relaxed);
                return Err(error);
            }
        };
        self.aggregate_bytes
            .fetch_sub(requested - written as u64, Ordering::Relaxed);
        self.digest.update(&buffer[..written]);
        self.bytes += written as u64;
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

struct CopySink {
    kind: CopyKind,
    partial_path: PathBuf,
    final_path: PathBuf,
    writer: Option<BufWriter<DigestWriter>>,
    rows: u64,
}

impl CopySink {
    fn create(
        output_directory: &Path,
        kind: CopyKind,
        aggregate_bytes: Arc<AtomicU64>,
        max_output_bytes: u64,
    ) -> io::Result<Self> {
        let final_path = output_directory.join(format!("{}.copy", kind.name()));
        let partial_path = output_directory.join(format!(".{}.copy.partial", kind.name()));
        if final_path.exists() || partial_path.exists() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!(
                    "hospital MRF output already exists: {}",
                    final_path.display()
                ),
            ));
        }
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&partial_path)?;
        Ok(Self {
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
            rows: 0,
        })
    }

    fn write_fields(&mut self, fields: &[Option<&str>]) -> io::Result<()> {
        let Some(writer) = self.writer.as_mut() else {
            return Err(invalid("hospital MRF COPY sink is already closed"));
        };
        write_copy_text_fields(writer, fields)?;
        self.rows = self.rows.saturating_add(1);
        Ok(())
    }

    fn finish(&mut self) -> io::Result<CopyArtifactSummary> {
        let Some(mut writer) = self.writer.take() else {
            return Err(invalid("hospital MRF COPY sink is already closed"));
        };
        writer.flush()?;
        writer.get_ref().file.sync_all()?;
        let bytes = writer.get_ref().bytes;
        let sha256 = hex_digest(writer.get_ref().digest.clone().finalize().as_slice());
        drop(writer);
        fs::hard_link(&self.partial_path, &self.final_path)?;
        fs::remove_file(&self.partial_path)?;
        Ok(CopyArtifactSummary {
            kind: self.kind.name(),
            path: self.final_path.display().to_string(),
            rows: self.rows,
            bytes,
            sha256,
        })
    }
}

struct CopyOutputs {
    sinks: Vec<CopySink>,
    committed: bool,
}

impl CopyOutputs {
    fn create(output_directory: &Path, max_output_bytes: u64) -> io::Result<Self> {
        let metadata = fs::symlink_metadata(output_directory)?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(invalid(
                "hospital MRF output path must be an existing non-symlink directory",
            ));
        }
        let mut outputs = Self {
            sinks: Vec::with_capacity(CopyKind::ALL.len()),
            committed: false,
        };
        let aggregate_bytes = Arc::new(AtomicU64::new(0));
        for (index, kind) in CopyKind::ALL.into_iter().enumerate() {
            if kind as usize != index {
                return Err(invalid("hospital MRF COPY sink order is inconsistent"));
            }
            outputs.sinks.push(CopySink::create(
                output_directory,
                kind,
                Arc::clone(&aggregate_bytes),
                max_output_bytes,
            )?);
        }
        Ok(outputs)
    }

    fn write(&mut self, kind: CopyKind, fields: &[Option<&str>]) -> io::Result<()> {
        if fields.iter().flatten().any(|value| value.contains('\0')) {
            return Err(invalid(format!(
                "hospital MRF {} COPY row contains NUL",
                kind.name()
            )));
        }
        self.sinks[kind as usize].write_fields(fields)
    }

    fn finish(mut self) -> io::Result<Vec<CopyArtifactSummary>> {
        let mut artifacts = Vec::with_capacity(self.sinks.len());
        for sink in &mut self.sinks {
            artifacts.push(sink.finish()?);
        }
        self.committed = true;
        Ok(artifacts)
    }
}

impl Drop for CopyOutputs {
    fn drop(&mut self) {
        if self.committed {
            return;
        }
        for sink in &mut self.sinks {
            drop(sink.writer.take());
            let _ = fs::remove_file(&sink.partial_path);
            let _ = fs::remove_file(&sink.final_path);
        }
    }
}
