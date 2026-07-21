// Licensed under the HealthPorta Non-Commercial License (see LICENSE).

pub fn retain_uhc_artifact(request: &UHCRetainRequest) -> io::Result<UHCRetainSummary> {
    let total_started = Instant::now();
    let expected_sha256 = request.validate()?;
    let verifier_build_id = current_build_id();
    validate_build_id(verifier_build_id)?;
    let root = RootDirectory::open(&request.output_root)?;
    let raw_name = raw_file_name(&request.expected_sha256);
    let manifest_name = manifest_file_name(&request.expected_sha256, request.range_count);

    let existing_raw = root.open_existing_regular(&raw_name)?;
    let raw_was_present = existing_raw.is_some();
    let mut raw_temporary = if raw_was_present {
        None
    } else {
        Some(root.create_temporary("raw")?)
    };
    let input = if let Some(existing) = existing_raw {
        Arc::new(existing)
    } else {
        Arc::new(open_regular_nofollow(
            &request.source_path,
            "source artifact",
        )?)
    };
    let input_identity = FileIdentity::from_file(&input)?;
    if input_identity.byte_count != request.expected_byte_count {
        return Err(invalid_data(format!(
            "UHC retained input byte count changed: {} != {}",
            input_identity.byte_count, request.expected_byte_count
        )));
    }

    let mut offsets_temporary = root.create_temporary("offsets")?;
    let concurrent_range_input = if let Some(temporary) = raw_temporary.as_ref() {
        Arc::new(temporary.file().try_clone()?)
    } else {
        Arc::clone(&input)
    };
    let scan = scan_raw_and_build_ranges(
        &input,
        concurrent_range_input,
        raw_temporary.as_mut().map(RootTemporaryFile::file_mut),
        offsets_temporary.file_mut(),
        &expected_sha256,
        request.expected_byte_count,
        request.range_count,
    )?;
    require_stable_regular_file(
        &input,
        input_identity,
        request.expected_byte_count,
        "input artifact",
    )?;
    let RawScanResult {
        record_count,
        ranges: streaming_ranges,
        read_hash_copy,
        frame_offset_index,
        concurrent_range_worker_max,
        range_verification_tail,
        raw_fsync,
    } = scan;
    if let Some(temporary) = raw_temporary.as_ref() {
        let identity = FileIdentity::from_file(temporary.file())?;
        if identity.byte_count != request.expected_byte_count {
            return Err(invalid_data(
                "retained UHC raw temporary byte count does not match its source",
            ));
        }
    }

    let (ranges, range_verification) = if streaming_ranges.len() == request.range_count {
        (streaming_ranges, range_verification_tail)
    } else {
        let boundaries =
            build_range_boundaries(offsets_temporary.file(), record_count, request.range_count)?;
        let range_input = if let Some(temporary) = raw_temporary.as_ref() {
            let identity = FileIdentity::from_file(temporary.file())?;
            let range_file = temporary.file().try_clone()?;
            if FileIdentity::from_file(&range_file)? != identity {
                return Err(invalid_data(
                    "retained UHC range workers did not receive the copied raw inode",
                ));
            }
            Arc::new(range_file)
        } else {
            Arc::clone(&input)
        };
        let range_input_identity = FileIdentity::from_file(&range_input)?;
        let range_started = Instant::now();
        let ranges = verify_ranges_parallel(Arc::clone(&range_input), boundaries)?;
        let range_verification = range_started.elapsed();
        require_stable_regular_file(
            &range_input,
            range_input_identity,
            request.expected_byte_count,
            "range input artifact",
        )?;
        (ranges, range_verification_tail + range_verification)
    };
    validate_range_sequence(&ranges, record_count, request.range_count)?;
    let raw_reverification = Duration::ZERO;

    let raw_publish_started = Instant::now();
    let (raw_reused, authoritative_raw_identity) = if let Some(mut temporary) = raw_temporary.take()
    {
        let temporary_identity = FileIdentity::from_file(temporary.file())?;
        if temporary.publish_noclobber(&raw_name)? {
            root.sync()?;
            let final_file = some_or_invalid_data(
                root.open_existing_regular(&raw_name)?,
                "published retained UHC raw artifact is unavailable",
            )?;
            let final_identity = FileIdentity::from_file(&final_file)?;
            if !same_inode(temporary_identity, final_identity)
                || final_identity.byte_count != request.expected_byte_count
            {
                return Err(invalid_data(
                    "published retained UHC raw artifact inode changed unexpectedly",
                ));
            }
            (false, final_identity)
        } else {
            let final_file = some_or_invalid_data(
                root.open_existing_regular(&raw_name)?,
                "concurrently published retained UHC raw artifact is unavailable",
            )?;
            verify_existing_raw_file(&final_file, &expected_sha256, request.expected_byte_count)?;
            (true, FileIdentity::from_file(&final_file)?)
        }
    } else {
        (true, input_identity)
    };
    let raw_publish = raw_publish_started.elapsed();
    root.verify_path_identity()?;
    let final_raw_file = some_or_invalid_data(
        root.open_existing_regular(&raw_name)?,
        "retained UHC raw artifact disappeared before manifest publication",
    )?;
    let final_raw_identity = FileIdentity::from_file(&final_raw_file)?;
    if final_raw_identity != authoritative_raw_identity {
        return Err(invalid_data(
            "retained UHC raw artifact identity changed before manifest publication",
        ));
    }

    let raw_artifact = UHCRawArtifactManifest {
        file_name: raw_name.clone(),
        sha256: request.expected_sha256.clone(),
        byte_count: request.expected_byte_count,
        record_count,
    };
    let range_set_sha256 = range_set_sha256(
        &expected_sha256,
        request.expected_byte_count,
        record_count,
        &ranges,
    )?;
    let candidate_manifest = build_manifest(
        verifier_build_id,
        raw_artifact.clone(),
        ranges.clone(),
        range_set_sha256.clone(),
    );

    let manifest_started = Instant::now();
    let manifest_publication = publish_or_verify_manifest(
        &root,
        &manifest_name,
        &candidate_manifest,
        &raw_artifact,
        &ranges,
        &range_set_sha256,
    )?;
    root.verify_path_identity()?;
    require_stable_regular_file(
        &final_raw_file,
        authoritative_raw_identity,
        request.expected_byte_count,
        "published raw artifact",
    )?;
    let observed_raw_file = some_or_invalid_data(
        root.open_existing_regular(&raw_name)?,
        "retained UHC raw artifact disappeared after manifest publication",
    )?;
    if FileIdentity::from_file(&observed_raw_file)? != authoritative_raw_identity {
        return Err(invalid_data(
            "retained UHC raw artifact identity changed after manifest publication",
        ));
    }
    reverify_manifest_path(&root, &manifest_name, &manifest_publication)?;
    let manifest_verify_or_publish = manifest_started.elapsed();

    let manifest_digest = Sha256::digest(&manifest_publication.encoded);
    let mut timings = UHCRetainTimings {
        total: 0.0,
        raw_read_hash_copy: read_hash_copy.as_secs_f64(),
        frame_offset_index: frame_offset_index.as_secs_f64(),
        concurrent_range_worker_max: concurrent_range_worker_max.as_secs_f64(),
        raw_fsync: raw_fsync.as_secs_f64(),
        range_verification: range_verification.as_secs_f64(),
        raw_reverification: raw_reverification.as_secs_f64(),
        raw_publish: raw_publish.as_secs_f64(),
        manifest_verify_or_publish: manifest_verify_or_publish.as_secs_f64(),
    };
    timings.total = total_started.elapsed().as_secs_f64();
    Ok(UHCRetainSummary {
        record_kind: "uhc_retained_summary".to_owned(),
        contract_id: UHC_RETAIN_CONTRACT_ID.to_owned(),
        contract_version: UHC_RETAIN_CONTRACT_VERSION,
        canonicalization_id: UHC_RETAIN_CANONICALIZATION_ID.to_owned(),
        producer_build_id: manifest_publication.producer_build_id,
        verifier_build_id: verifier_build_id.to_owned(),
        raw_artifact_path: path_text(&root.output_path(&raw_name), "raw artifact")?,
        raw_artifact_sha256: request.expected_sha256.clone(),
        raw_artifact_byte_count: request.expected_byte_count,
        record_count,
        range_count: request.range_count as u64,
        manifest_path: path_text(&root.output_path(&manifest_name), "manifest")?,
        manifest_sha256: sha256_hex(&manifest_digest),
        manifest_byte_count: manifest_publication.encoded.len() as u64,
        raw_reused,
        manifest_reused: manifest_publication.reused,
        timings_seconds: timings,
    })
}

pub fn run_uhc_retain_cli(arguments: &[String]) -> io::Result<()> {
    if arguments.len() != 5 {
        return Err(invalid_input(
            "usage: ptg2_scanner --uhc-retain <source_path> <output_root> <expected_sha256> <expected_byte_count> <range_count>",
        ));
    }
    let expected_byte_count = arguments[3].parse::<u64>().map_err(|_| {
        invalid_input("expected UHC retained byte count must be an unsigned integer")
    })?;
    let range_count = arguments[4]
        .parse::<usize>()
        .map_err(|_| invalid_input("UHC retained range count must be an unsigned integer"))?;
    let summary = retain_uhc_artifact(&UHCRetainRequest {
        source_path: PathBuf::from(&arguments[0]),
        output_root: PathBuf::from(&arguments[1]),
        expected_sha256: arguments[2].clone(),
        expected_byte_count,
        range_count,
    })?;
    let stdout = io::stdout();
    let mut output = stdout.lock();
    serde_json::to_writer(&mut output, &summary)
        .map_err(|error| io::Error::other(error.to_string()))?;
    output.write_all(b"\n")?;
    output.flush()
}
