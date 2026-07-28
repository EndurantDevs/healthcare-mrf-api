// Licensed under the HealthPorta Non-Commercial License (see LICENSE).

#[derive(Clone, Debug)]
pub struct UHCVerifiedReplayRequest {
    pub raw_path: PathBuf,
    pub manifest_path: PathBuf,
    pub expected_artifact_sha256: String,
    pub expected_artifact_byte_count: u64,
    pub expected_manifest_sha256: String,
    pub expected_range_set_sha256: String,
    pub expected_record_count: u64,
    pub expected_range_count: usize,
}

/// One descriptor-bound, fully verified retained artifact for semantic replay.
///
/// The source holds the exact raw and manifest file descriptors that were
/// verified at open time. Record visits use positional reads from those
/// descriptors and recheck their inode metadata before and after each range.
pub struct UHCVerifiedRetainedSource {
    raw_path: PathBuf,
    manifest_path: PathBuf,
    raw_file: File,
    manifest_file: File,
    raw_identity: FileIdentity,
    manifest_identity: FileIdentity,
    manifest: UHCRetainedManifest,
    manifest_sha256: String,
}

impl UHCVerifiedRetainedSource {
    pub fn manifest(&self) -> &UHCRetainedManifest {
        &self.manifest
    }

    pub fn manifest_sha256(&self) -> &str {
        &self.manifest_sha256
    }

    pub fn visit_verified_range_records<F>(
        &self,
        range_ordinal: usize,
        mut visitor: F,
    ) -> io::Result<()>
    where
        F: FnMut(u64, &[u8]) -> io::Result<()>,
    {
        let Some(range) = self.manifest.ranges.get(range_ordinal) else {
            return Err(invalid_input(
                "UHC retained replay range is out of bounds",
            ));
        };
        require_replay_path_identity(
            &self.raw_path,
            &self.raw_file,
            self.raw_identity,
            "raw artifact",
        )?;
        require_replay_path_identity(
            &self.manifest_path,
            &self.manifest_file,
            self.manifest_identity,
            "manifest",
        )?;

        let mut raw_digest = Sha256::new();
        let mut canonical_digest = Sha256::new();
        let mut canonical_byte_count = 0u64;
        let mut observed_record_count = 0u64;
        let mut framer =
            JsonObjectFramer::fragment(range.raw_byte_start, MAX_PROVIDER_RECORD_BYTES);
        let mut read_buffer = vec![0u8; READ_BUFFER_BYTES];
        let mut canonical = Vec::with_capacity(64 * 1024);
        let mut absolute_offset = range.raw_byte_start;
        while absolute_offset < range.raw_byte_end {
            let remaining =
                (range.raw_byte_end - absolute_offset).min(READ_BUFFER_BYTES as u64) as usize;
            let bytes_read = self
                .raw_file
                .read_at(&mut read_buffer[..remaining], absolute_offset)?;
            if bytes_read == 0 {
                return Err(invalid_data(
                    "UHC retained replay range ended before its boundary",
                ));
            }
            let chunk = &read_buffer[..bytes_read];
            raw_digest.update(chunk);
            framer.feed(chunk, |record, _record_start, _record_end| {
                validate_strict_json_object(record)?;
                canonical.clear();
                canonical.extend(
                    record
                        .iter()
                        .copied()
                        .filter(|byte| !matches!(*byte, b'\r' | b'\n')),
                );
                canonical.push(b'\n');
                canonical_digest.update(&canonical);
                canonical_byte_count = some_or_invalid_data(
                    canonical_byte_count.checked_add(canonical.len() as u64),
                    "UHC retained replay canonical byte count overflowed",
                )?;
                let occurrence_ordinal = some_or_invalid_data(
                    range.record_start.checked_add(observed_record_count),
                    "UHC retained replay occurrence ordinal overflowed",
                )?;
                visitor(occurrence_ordinal, record)?;
                observed_record_count = some_or_invalid_data(
                    observed_record_count.checked_add(1),
                    "UHC retained replay record count overflowed",
                )?;
                Ok(())
            })?;
            absolute_offset += bytes_read as u64;
        }
        framer.finish()?;
        if observed_record_count != range.record_count
            || canonical_byte_count != range.canonical_byte_count
            || sha256_hex(&finalize_sha256(raw_digest)) != range.raw_sha256
            || sha256_hex(&finalize_sha256(canonical_digest)) != range.canonical_sha256
        {
            return Err(invalid_data(
                "UHC retained replay range proof does not match its manifest",
            ));
        }
        require_replay_path_identity(
            &self.raw_path,
            &self.raw_file,
            self.raw_identity,
            "raw artifact",
        )?;
        require_replay_path_identity(
            &self.manifest_path,
            &self.manifest_file,
            self.manifest_identity,
            "manifest",
        )
    }
}

fn require_replay_path_identity(
    path: &Path,
    file: &File,
    expected: FileIdentity,
    label: &str,
) -> io::Result<()> {
    let descriptor_metadata = file.metadata()?;
    let descriptor_identity = FileIdentity::from_metadata(&descriptor_metadata);
    let path_metadata = fs::symlink_metadata(path)?;
    let path_identity = FileIdentity::from_metadata(&path_metadata);
    if !descriptor_metadata.is_file()
        || !path_metadata.is_file()
        || path_metadata.file_type().is_symlink()
        || descriptor_identity != expected
        || !same_inode(path_identity, expected)
        || descriptor_metadata.nlink() != 1
        || descriptor_metadata.mode() & 0o022 != 0
    {
        return Err(invalid_data(format!(
            "UHC retained replay {label} identity or permissions changed"
        )));
    }
    Ok(())
}

pub fn open_verified_uhc_replay(
    request: &UHCVerifiedReplayRequest,
) -> io::Result<UHCVerifiedRetainedSource> {
    let expected_artifact_sha256 = parse_sha256_hex(&request.expected_artifact_sha256)?;
    parse_sha256_hex(&request.expected_manifest_sha256)?;
    parse_sha256_hex(&request.expected_range_set_sha256)?;
    if request.expected_artifact_byte_count == 0
        || request.expected_artifact_byte_count > i64::MAX as u64
        || request.expected_record_count == 0
        || request.expected_record_count > MAX_RECORD_COUNT
        || !(MIN_RANGE_COUNT..=MAX_RANGE_COUNT).contains(&request.expected_range_count)
    {
        return Err(invalid_input(
            "UHC retained replay expected counts are invalid",
        ));
    }

    let raw_file = open_regular_nofollow(&request.raw_path, "replay raw artifact")?;
    let manifest_file = open_regular_nofollow(&request.manifest_path, "replay manifest")?;
    let raw_identity = FileIdentity::from_file(&raw_file)?;
    let manifest_identity = FileIdentity::from_file(&manifest_file)?;
    require_replay_path_identity(&request.raw_path, &raw_file, raw_identity, "raw artifact")?;
    require_replay_path_identity(
        &request.manifest_path,
        &manifest_file,
        manifest_identity,
        "manifest",
    )?;

    let manifest_bytes =
        read_bounded_stable_file(&manifest_file, MAX_MANIFEST_BYTES, "manifest")?;
    let manifest_sha256 = sha256_hex(&Sha256::digest(&manifest_bytes));
    if manifest_sha256 != request.expected_manifest_sha256 {
        return Err(invalid_data(
            "UHC retained replay manifest SHA-256 does not match",
        ));
    }
    let manifest = parse_strict_manifest(&manifest_bytes)?;
    let raw_file_name = match request.raw_path.file_name() {
        Some(value) => match value.to_str() {
            Some(value) => value,
            None => {
                return Err(invalid_input(
                    "UHC retained replay raw file name is invalid",
                ))
            }
        },
        None => {
            return Err(invalid_input(
                "UHC retained replay raw file name is invalid",
            ))
        }
    };
    if manifest.contract_id != UHC_RETAIN_CONTRACT_ID
        || manifest.contract_version != UHC_RETAIN_CONTRACT_VERSION
        || manifest.canonicalization_id != UHC_RETAIN_CANONICALIZATION_ID
        || manifest.raw_artifact.file_name != raw_file_name
        || manifest.raw_artifact.sha256 != request.expected_artifact_sha256
        || manifest.raw_artifact.byte_count != request.expected_artifact_byte_count
        || manifest.raw_artifact.record_count != request.expected_record_count
        || manifest.range_count != request.expected_range_count as u64
        || manifest.ranges.len() != request.expected_range_count
        || manifest.range_set_sha256 != request.expected_range_set_sha256
    {
        return Err(invalid_data(
            "UHC retained replay manifest identity does not match",
        ));
    }
    validate_range_sequence(
        &manifest.ranges,
        request.expected_record_count,
        request.expected_range_count,
    )?;
    if range_set_sha256(
        &expected_artifact_sha256,
        request.expected_artifact_byte_count,
        request.expected_record_count,
        &manifest.ranges,
    )? != request.expected_range_set_sha256
    {
        return Err(invalid_data(
            "UHC retained replay range-set proof does not match",
        ));
    }
    verify_whole_file_sha256(
        &raw_file,
        raw_identity,
        &expected_artifact_sha256,
        request.expected_artifact_byte_count,
        "replay raw artifact",
    )?;
    require_replay_path_identity(&request.raw_path, &raw_file, raw_identity, "raw artifact")?;
    require_replay_path_identity(
        &request.manifest_path,
        &manifest_file,
        manifest_identity,
        "manifest",
    )?;
    Ok(UHCVerifiedRetainedSource {
        raw_path: request.raw_path.clone(),
        manifest_path: request.manifest_path.clone(),
        raw_file,
        manifest_file,
        raw_identity,
        manifest_identity,
        manifest,
        manifest_sha256,
    })
}
