// Licensed under the HealthPorta Non-Commercial License (see LICENSE).

fn read_u64_at(file: &File, offset: u64) -> io::Result<u64> {
    let mut encoded = [0u8; 8];
    let mut consumed = 0usize;
    while consumed < encoded.len() {
        let bytes_read = file.read_at(&mut encoded[consumed..], offset + consumed as u64)?;
        if bytes_read == 0 {
            return Err(invalid_data(
                "retained UHC record-offset spool ended unexpectedly",
            ));
        }
        consumed += bytes_read;
    }
    Ok(u64::from_be_bytes(encoded))
}

fn ceil_partition_boundary(total: u64, ordinal: usize, count: usize) -> io::Result<u64> {
    let numerator = some_or_invalid_data(
        u128::from(total)
            .checked_mul(ordinal as u128)
            .and_then(|value| value.checked_add(count as u128 - 1)),
        "retained UHC range boundary overflowed",
    )?;
    u64::try_from(numerator / count as u128)
        .map_err(|_| invalid_data("retained UHC range boundary exceeds u64"))
}

fn build_range_boundaries(
    offsets_file: &File,
    record_count: u64,
    range_count: usize,
) -> io::Result<Vec<RawRangeBoundary>> {
    if record_count < range_count as u64 {
        return Err(invalid_data(format!(
            "retained UHC artifact has {record_count} records for {range_count} ranges"
        )));
    }
    let expected_offset_bytes = some_or_invalid_data(
        record_count.checked_mul(16),
        "retained UHC offset spool size overflowed",
    )?;
    if offsets_file.metadata()?.len() != expected_offset_bytes {
        return Err(invalid_data(
            "retained UHC record-offset spool has an invalid byte count",
        ));
    }
    let mut boundaries = Vec::with_capacity(range_count);
    for ordinal in 0..range_count {
        let record_start = ceil_partition_boundary(record_count, ordinal, range_count)?;
        let record_end = ceil_partition_boundary(record_count, ordinal + 1, range_count)?;
        if record_start >= record_end {
            return Err(invalid_data("retained UHC logical range would be empty"));
        }
        let first_offset = some_or_invalid_data(
            record_start.checked_mul(16),
            "retained UHC range offset overflowed",
        )?;
        let last_offset = some_or_invalid_data(
            record_end
                .checked_sub(1)
                .and_then(|value| value.checked_mul(16))
                .and_then(|value| value.checked_add(8)),
            "retained UHC range offset overflowed",
        )?;
        let raw_byte_start = read_u64_at(offsets_file, first_offset)?;
        let raw_byte_end = read_u64_at(offsets_file, last_offset)?;
        if raw_byte_start >= raw_byte_end {
            return Err(invalid_data(
                "retained UHC logical range has invalid byte boundaries",
            ));
        }
        boundaries.push(RawRangeBoundary {
            range_ordinal: ordinal as u64,
            raw_byte_start,
            raw_byte_end,
            record_start,
            record_end,
        });
    }
    for pair in boundaries.windows(2) {
        if pair[0].record_end != pair[1].record_start
            || pair[0].raw_byte_end >= pair[1].raw_byte_start
        {
            return Err(invalid_data(
                "retained UHC logical ranges are not ordered and disjoint",
            ));
        }
    }
    Ok(boundaries)
}

fn hash_raw_range(input: &File, boundary: RawRangeBoundary) -> io::Result<(String, u64)> {
    let raw_byte_count = some_or_invalid_data(
        boundary.raw_byte_end.checked_sub(boundary.raw_byte_start),
        "retained UHC raw range byte count underflowed",
    )?;
    if raw_byte_count == 0 {
        return Err(invalid_data("retained UHC raw range is empty"));
    }
    let mut digest = Sha256::new();
    let mut buffer = vec![0u8; READ_BUFFER_BYTES];
    let mut absolute_offset = boundary.raw_byte_start;
    while absolute_offset < boundary.raw_byte_end {
        let remaining = usize::try_from(
            (boundary.raw_byte_end - absolute_offset).min(READ_BUFFER_BYTES as u64),
        )
        .map_err(|_| invalid_data("retained UHC range read size overflowed"))?;
        let bytes_read = input.read_at(&mut buffer[..remaining], absolute_offset)?;
        if bytes_read == 0 {
            return Err(invalid_data(
                "retained UHC raw range ended before its declared boundary",
            ));
        }
        digest.update(&buffer[..bytes_read]);
        absolute_offset = some_or_invalid_data(
            absolute_offset.checked_add(bytes_read as u64),
            "retained UHC range offset overflowed",
        )?;
    }
    Ok((sha256_hex(&finalize_sha256(digest)), raw_byte_count))
}

fn verify_raw_range(input: &File, boundary: RawRangeBoundary) -> io::Result<UHCRawRangeManifest> {
    let raw_byte_count = some_or_invalid_data(
        boundary.raw_byte_end.checked_sub(boundary.raw_byte_start),
        "retained UHC raw range byte count underflowed",
    )?;
    let expected_record_count = some_or_invalid_data(
        boundary.record_end.checked_sub(boundary.record_start),
        "retained UHC range record count underflowed",
    )?;
    let mut raw_digest = Sha256::new();
    let mut canonical_digest = Sha256::new();
    let mut canonical_byte_count = 0u64;
    let mut observed_record_count = 0u64;
    let mut canonical = Vec::with_capacity(64 * 1024);
    let mut framer = JsonObjectFramer::fragment(boundary.raw_byte_start, MAX_PROVIDER_RECORD_BYTES);
    let mut buffer = vec![0u8; READ_BUFFER_BYTES];
    let mut absolute_offset = boundary.raw_byte_start;
    while absolute_offset < boundary.raw_byte_end {
        let remaining = usize::try_from(
            (boundary.raw_byte_end - absolute_offset).min(READ_BUFFER_BYTES as u64),
        )
        .map_err(|_| invalid_data("retained UHC range read size overflowed"))?;
        let bytes_read = input.read_at(&mut buffer[..remaining], absolute_offset)?;
        if bytes_read == 0 {
            return Err(invalid_data(
                "retained UHC raw range ended before its declared boundary",
            ));
        }
        let chunk = &buffer[..bytes_read];
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
                "retained UHC canonical range byte count overflowed",
            )?;
            observed_record_count = some_or_invalid_data(
                observed_record_count.checked_add(1),
                "retained UHC range record count overflowed",
            )?;
            Ok(())
        })?;
        absolute_offset = some_or_invalid_data(
            absolute_offset.checked_add(bytes_read as u64),
            "retained UHC range offset overflowed",
        )?;
    }
    framer.finish()?;
    if observed_record_count != expected_record_count {
        return Err(invalid_data(format!(
            "retained UHC range {} record count changed: {} != {}",
            boundary.range_ordinal, observed_record_count, expected_record_count
        )));
    }
    for (field_name, value) in [
        ("range ordinal", boundary.range_ordinal),
        ("range raw byte start", boundary.raw_byte_start),
        ("range raw byte end", boundary.raw_byte_end),
        ("range raw byte count", raw_byte_count),
        ("range record start", boundary.record_start),
        ("range record end", boundary.record_end),
        ("range record count", observed_record_count),
        ("range canonical byte count", canonical_byte_count),
    ] {
        checked_i64_domain(value, field_name)?;
    }
    Ok(UHCRawRangeManifest {
        range_ordinal: boundary.range_ordinal,
        raw_byte_start: boundary.raw_byte_start,
        raw_byte_end: boundary.raw_byte_end,
        raw_byte_count,
        raw_sha256: sha256_hex(&finalize_sha256(raw_digest)),
        record_start: boundary.record_start,
        record_end: boundary.record_end,
        record_count: observed_record_count,
        canonical_sha256: sha256_hex(&finalize_sha256(canonical_digest)),
        canonical_byte_count,
    })
}

fn verify_ranges_parallel(
    input: Arc<File>,
    boundaries: Vec<RawRangeBoundary>,
) -> io::Result<Vec<UHCRawRangeManifest>> {
    let worker_count = thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(4)
        .min(MAX_RANGE_WORKERS)
        .min(boundaries.len())
        .max(1);
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(worker_count)
        .thread_name(|index| format!("uhc-retain-{index}"))
        .build()
        .map_err(|error| io::Error::other(error.to_string()))?;
    let results = pool.install(|| {
        boundaries
            .into_par_iter()
            .map(|boundary| verify_raw_range(&input, boundary))
            .collect::<Vec<_>>()
    });
    results.into_iter().collect()
}

fn verify_whole_file_sha256(
    file: &File,
    expected_identity: FileIdentity,
    expected_sha256: &[u8; SHA256_BYTES],
    expected_byte_count: u64,
    label: &str,
) -> io::Result<()> {
    require_stable_regular_file(file, expected_identity, expected_byte_count, label)?;
    let mut digest = Sha256::new();
    let mut buffer = vec![0u8; READ_BUFFER_BYTES];
    let mut absolute_offset = 0u64;
    while absolute_offset < expected_byte_count {
        let remaining =
            usize::try_from((expected_byte_count - absolute_offset).min(READ_BUFFER_BYTES as u64))
                .map_err(|_| invalid_data("retained UHC verification read size overflowed"))?;
        let bytes_read = file.read_at(&mut buffer[..remaining], absolute_offset)?;
        if bytes_read == 0 {
            return Err(invalid_data(format!(
                "UHC retained {label} ended during SHA-256 verification"
            )));
        }
        digest.update(&buffer[..bytes_read]);
        absolute_offset += bytes_read as u64;
    }
    if finalize_sha256(digest) != *expected_sha256 {
        return Err(invalid_data(format!(
            "UHC retained {label} SHA-256 does not match"
        )));
    }
    require_stable_regular_file(file, expected_identity, expected_byte_count, label)
}

fn validate_range_sequence(
    ranges: &[UHCRawRangeManifest],
    expected_record_count: u64,
    expected_range_count: usize,
) -> io::Result<()> {
    if ranges.len() != expected_range_count {
        return Err(invalid_data(
            "retained UHC manifest range count does not match its range list",
        ));
    }
    let mut next_record = 0u64;
    let mut previous_raw_end = None;
    for (ordinal, range) in ranges.iter().enumerate() {
        if range.range_ordinal != ordinal as u64
            || range.record_start != next_record
            || range.record_end < range.record_start
            || range.record_end - range.record_start != range.record_count
            || range.record_count == 0
            || range.raw_byte_start >= range.raw_byte_end
            || range.raw_byte_end - range.raw_byte_start != range.raw_byte_count
            || range.raw_byte_count == 0
            || range.canonical_byte_count == 0
        {
            return Err(invalid_data(
                "retained UHC manifest contains an invalid logical range",
            ));
        }
        if previous_raw_end.is_some_and(|end| end >= range.raw_byte_start) {
            return Err(invalid_data(
                "retained UHC manifest raw ranges overlap or are unordered",
            ));
        }
        parse_sha256_hex(&range.raw_sha256)
            .map_err(|_| invalid_data("retained UHC range raw SHA-256 is invalid"))?;
        parse_sha256_hex(&range.canonical_sha256)
            .map_err(|_| invalid_data("retained UHC range canonical SHA-256 is invalid"))?;
        next_record = range.record_end;
        previous_raw_end = Some(range.raw_byte_end);
    }
    if next_record != expected_record_count {
        return Err(invalid_data(
            "retained UHC manifest ranges do not cover every record",
        ));
    }
    Ok(())
}

fn range_set_sha256(
    raw_sha256: &[u8; SHA256_BYTES],
    raw_byte_count: u64,
    record_count: u64,
    ranges: &[UHCRawRangeManifest],
) -> io::Result<String> {
    validate_range_sequence(ranges, record_count, ranges.len())?;
    let mut digest = Sha256::new();
    digest.update(UHC_RETAIN_CONTRACT_ID.as_bytes());
    digest.update(b"\0");
    digest.update(UHC_RETAIN_CONTRACT_VERSION.to_be_bytes());
    digest.update(UHC_RETAIN_CANONICALIZATION_ID.as_bytes());
    digest.update(b"\0");
    digest.update(raw_sha256);
    digest.update(raw_byte_count.to_be_bytes());
    digest.update(record_count.to_be_bytes());
    digest.update((ranges.len() as u64).to_be_bytes());
    for range in ranges {
        for value in [
            range.range_ordinal,
            range.raw_byte_start,
            range.raw_byte_end,
            range.raw_byte_count,
            range.record_start,
            range.record_end,
            range.record_count,
            range.canonical_byte_count,
        ] {
            digest.update(value.to_be_bytes());
        }
        digest.update(
            parse_sha256_hex(&range.raw_sha256)
                .map_err(|_| invalid_data("retained UHC range raw SHA-256 is invalid"))?,
        );
        digest.update(
            parse_sha256_hex(&range.canonical_sha256)
                .map_err(|_| invalid_data("retained UHC range canonical SHA-256 is invalid"))?,
        );
    }
    Ok(sha256_hex(&finalize_sha256(digest)))
}

