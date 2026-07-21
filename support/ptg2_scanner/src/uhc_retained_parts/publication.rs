// Licensed under the HealthPorta Non-Commercial License (see LICENSE).

fn encode_manifest(manifest: &UHCRetainedManifest) -> io::Result<Vec<u8>> {
    let mut encoded = serde_json::to_vec(manifest)
        .map_err(|error| invalid_data(format!("unable to encode UHC manifest: {error}")))?;
    encoded.push(b'\n');
    if encoded.len() as u64 > MAX_MANIFEST_BYTES {
        return Err(invalid_data("retained UHC manifest exceeds its byte limit"));
    }
    Ok(encoded)
}

fn read_bounded_stable_file(file: &File, maximum_bytes: u64, label: &str) -> io::Result<Vec<u8>> {
    let before = FileIdentity::from_file(file)?;
    if before.byte_count == 0 || before.byte_count > maximum_bytes {
        return Err(invalid_data(format!(
            "UHC retained {label} has an invalid byte count"
        )));
    }
    let byte_count = usize::try_from(before.byte_count)
        .map_err(|_| invalid_data(format!("UHC retained {label} is too large")))?;
    let mut payload = vec![0u8; byte_count];
    let mut consumed = 0usize;
    while consumed < payload.len() {
        let bytes_read = file.read_at(&mut payload[consumed..], consumed as u64)?;
        if bytes_read == 0 {
            return Err(invalid_data(format!(
                "UHC retained {label} ended unexpectedly"
            )));
        }
        consumed += bytes_read;
    }
    if FileIdentity::from_file(file)? != before {
        return Err(invalid_data(format!(
            "UHC retained {label} changed while it was read"
        )));
    }
    Ok(payload)
}

fn parse_strict_manifest(bytes: &[u8]) -> io::Result<UHCRetainedManifest> {
    validate_strict_json_object(bytes)
        .map_err(|error| invalid_data(format!("retained UHC manifest is invalid: {error}")))?;
    let manifest: UHCRetainedManifest = serde_json::from_slice(bytes)
        .map_err(|error| invalid_data(format!("retained UHC manifest is invalid: {error}")))?;
    validate_build_id(&manifest.producer_build_id)?;
    if encode_manifest(&manifest)? != bytes {
        return Err(invalid_data(
            "retained UHC manifest is not in its deterministic canonical encoding",
        ));
    }
    Ok(manifest)
}

fn validate_existing_manifest(
    manifest: &UHCRetainedManifest,
    expected_raw: &UHCRawArtifactManifest,
    expected_ranges: &[UHCRawRangeManifest],
    expected_range_set_sha256: &str,
) -> io::Result<()> {
    if manifest.contract_id != UHC_RETAIN_CONTRACT_ID
        || manifest.contract_version != UHC_RETAIN_CONTRACT_VERSION
        || manifest.canonicalization_id != UHC_RETAIN_CANONICALIZATION_ID
        || manifest.raw_artifact != *expected_raw
        || manifest.range_count != expected_ranges.len() as u64
        || manifest.ranges != expected_ranges
        || manifest.range_set_sha256 != expected_range_set_sha256
    {
        return Err(invalid_data(
            "existing retained UHC manifest does not match the rederived raw-range proof",
        ));
    }
    validate_range_sequence(
        &manifest.ranges,
        manifest.raw_artifact.record_count,
        manifest.range_count as usize,
    )?;
    let raw_sha = parse_sha256_hex(&manifest.raw_artifact.sha256)
        .map_err(|_| invalid_data("retained UHC manifest raw SHA-256 is invalid"))?;
    if range_set_sha256(
        &raw_sha,
        manifest.raw_artifact.byte_count,
        manifest.raw_artifact.record_count,
        &manifest.ranges,
    )? != manifest.range_set_sha256
    {
        return Err(invalid_data(
            "existing retained UHC manifest has an invalid range-set proof",
        ));
    }
    Ok(())
}
struct ManifestPublication {
    producer_build_id: String,
    encoded: Vec<u8>,
    reused: bool,
    final_identity: FileIdentity,
}

fn verify_existing_manifest_file(
    file: &File,
    expected_raw: &UHCRawArtifactManifest,
    expected_ranges: &[UHCRawRangeManifest],
    expected_range_set_sha256: &str,
) -> io::Result<ManifestPublication> {
    let encoded = read_bounded_stable_file(file, MAX_MANIFEST_BYTES, "manifest")?;
    let manifest = parse_strict_manifest(&encoded)?;
    validate_existing_manifest(
        &manifest,
        expected_raw,
        expected_ranges,
        expected_range_set_sha256,
    )?;
    Ok(ManifestPublication {
        producer_build_id: manifest.producer_build_id,
        encoded,
        reused: true,
        final_identity: FileIdentity::from_file(file)?,
    })
}

fn verify_existing_raw_file(
    file: &File,
    expected_sha256: &[u8; SHA256_BYTES],
    expected_byte_count: u64,
) -> io::Result<()> {
    let identity = FileIdentity::from_file(file)?;
    verify_whole_file_sha256(
        file,
        identity,
        expected_sha256,
        expected_byte_count,
        "raw artifact",
    )
}

fn path_text(path: &Path, label: &str) -> io::Result<String> {
    path.to_str()
        .map(str::to_owned)
        .ok_or_else(|| invalid_data(format!("UHC retained {label} path is not UTF-8")))
}

fn raw_file_name(expected_sha256: &str) -> String {
    format!("raw-{expected_sha256}.json")
}

fn manifest_file_name(expected_sha256: &str, range_count: usize) -> String {
    format!(
        "raw-{expected_sha256}-ranges-{range_count}-v{UHC_RETAIN_CONTRACT_VERSION}.manifest.json"
    )
}

fn build_manifest(
    producer_build_id: &str,
    raw_artifact: UHCRawArtifactManifest,
    ranges: Vec<UHCRawRangeManifest>,
    range_set_sha256: String,
) -> UHCRetainedManifest {
    UHCRetainedManifest {
        contract_id: UHC_RETAIN_CONTRACT_ID.to_owned(),
        contract_version: UHC_RETAIN_CONTRACT_VERSION,
        canonicalization_id: UHC_RETAIN_CANONICALIZATION_ID.to_owned(),
        producer_build_id: producer_build_id.to_owned(),
        raw_artifact,
        range_count: ranges.len() as u64,
        ranges,
        range_set_sha256,
    }
}

fn same_inode(left: FileIdentity, right: FileIdentity) -> bool {
    left.device == right.device && left.inode == right.inode
}

fn publish_or_verify_manifest(
    root: &Arc<RootDirectory>,
    manifest_name: &str,
    candidate: &UHCRetainedManifest,
    expected_raw: &UHCRawArtifactManifest,
    expected_ranges: &[UHCRawRangeManifest],
    expected_range_set_sha256: &str,
) -> io::Result<ManifestPublication> {
    if let Some(existing) = root.open_existing_regular(manifest_name)? {
        return verify_existing_manifest_file(
            &existing,
            expected_raw,
            expected_ranges,
            expected_range_set_sha256,
        );
    }

    let encoded = encode_manifest(candidate)?;
    let mut temporary = root.create_temporary("manifest")?;
    temporary.file_mut().write_all(&encoded)?;
    temporary.file().sync_all()?;
    let temporary_identity = FileIdentity::from_file(temporary.file())?;
    if temporary.publish_noclobber(manifest_name)? {
        root.sync()?;
        let final_file = some_or_invalid_data(
            root.open_existing_regular(manifest_name)?,
            "published retained UHC manifest is not visible in its root",
        )?;
        let final_identity = FileIdentity::from_file(&final_file)?;
        if !same_inode(temporary_identity, final_identity) {
            return Err(invalid_data(
                "published retained UHC manifest inode changed unexpectedly",
            ));
        }
        let observed = read_bounded_stable_file(&final_file, MAX_MANIFEST_BYTES, "manifest")?;
        if observed != encoded {
            return Err(invalid_data(
                "published retained UHC manifest bytes changed unexpectedly",
            ));
        }
        return Ok(ManifestPublication {
            producer_build_id: candidate.producer_build_id.clone(),
            encoded,
            reused: false,
            final_identity,
        });
    }

    let existing = some_or_invalid_data(
        root.open_existing_regular(manifest_name)?,
        "concurrently published retained UHC manifest is unavailable",
    )?;
    verify_existing_manifest_file(
        &existing,
        expected_raw,
        expected_ranges,
        expected_range_set_sha256,
    )
}

fn reverify_manifest_path(
    root: &RootDirectory,
    manifest_name: &str,
    publication: &ManifestPublication,
) -> io::Result<()> {
    let final_file = some_or_invalid_data(
        root.open_existing_regular(manifest_name)?,
        "retained UHC manifest disappeared after verification",
    )?;
    let identity = FileIdentity::from_file(&final_file)?;
    if identity != publication.final_identity {
        return Err(invalid_data(
            "retained UHC manifest identity changed after verification",
        ));
    }
    let observed = read_bounded_stable_file(&final_file, MAX_MANIFEST_BYTES, "manifest")?;
    if observed != publication.encoded {
        return Err(invalid_data(
            "retained UHC manifest bytes changed after verification",
        ));
    }
    Ok(())
}
