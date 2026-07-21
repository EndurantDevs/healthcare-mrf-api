// Licensed under the HealthPorta Non-Commercial License (see LICENSE).

#[derive(Debug)]
struct RawScanResult {
    record_count: u64,
    ranges: Vec<UHCRawRangeManifest>,
    read_hash_copy: Duration,
    frame_offset_index: Duration,
    concurrent_range_worker_max: Duration,
    range_verification_tail: Duration,
    raw_fsync: Duration,
}

struct StreamingRangeBoundary {
    range_ordinal: u64,
    raw_byte_start: u64,
    raw_byte_end: u64,
    record_start: u64,
    record_end: u64,
    record_count: u64,
}

impl StreamingRangeBoundary {
    fn new(range_ordinal: u64, record_start: u64, raw_byte_start: u64) -> Self {
        Self {
            range_ordinal,
            raw_byte_start,
            raw_byte_end: raw_byte_start,
            record_start,
            record_end: record_start,
            record_count: 0,
        }
    }

    fn add_record(&mut self, record_end: u64) -> io::Result<()> {
        self.raw_byte_end = record_end;
        self.record_end = some_or_invalid_data(
            self.record_end.checked_add(1),
            "retained UHC range record count overflowed",
        )?;
        self.record_count = some_or_invalid_data(
            self.record_count.checked_add(1),
            "retained UHC range record count overflowed",
        )?;
        Ok(())
    }

    fn finish(self) -> io::Result<RawRangeBoundary> {
        if self.raw_byte_start >= self.raw_byte_end
            || self.record_start >= self.record_end
            || self.record_end - self.record_start != self.record_count
        {
            return Err(invalid_data(
                "retained UHC streaming range boundary is invalid",
            ));
        }
        Ok(RawRangeBoundary {
            range_ordinal: self.range_ordinal,
            raw_byte_start: self.raw_byte_start,
            raw_byte_end: self.raw_byte_end,
            record_start: self.record_start,
            record_end: self.record_end,
        })
    }
}

enum RangeWorkerMessage {
    Records(Vec<Vec<u8>>),
    Finish(RawRangeBoundary),
}

#[derive(Default)]
struct PendingRecordBatch {
    records: Vec<Vec<u8>>,
    byte_count: usize,
}

fn run_range_worker(
    input: Arc<File>,
    messages: Receiver<RangeWorkerMessage>,
) -> io::Result<Option<UHCRawRangeManifest>> {
    let mut canonical_digest = Sha256::new();
    let mut canonical_byte_count = 0u64;
    let mut observed_record_count = 0u64;
    let boundary = loop {
        match messages.recv() {
            Ok(RangeWorkerMessage::Records(records)) => {
                for record in records {
                    validate_strict_json_object(&record)?;
                    let mut record_byte_count = 1u64;
                    let mut segment_start = 0usize;
                    for (index, byte) in record.iter().copied().enumerate() {
                        if matches!(byte, b'\r' | b'\n') {
                            canonical_digest.update(&record[segment_start..index]);
                            segment_start = index + 1;
                        } else {
                            record_byte_count = some_or_invalid_data(
                                record_byte_count.checked_add(1),
                                "retained UHC canonical range byte count overflowed",
                            )?;
                        }
                    }
                    canonical_digest.update(&record[segment_start..]);
                    canonical_digest.update(b"\n");
                    canonical_byte_count = some_or_invalid_data(
                        canonical_byte_count.checked_add(record_byte_count),
                        "retained UHC canonical range byte count overflowed",
                    )?;
                    observed_record_count = some_or_invalid_data(
                        observed_record_count.checked_add(1),
                        "retained UHC range record count overflowed",
                    )?;
                }
            }
            Ok(RangeWorkerMessage::Finish(boundary)) => break boundary,
            Err(_) => return Ok(None),
        }
    };
    let expected_record_count = some_or_invalid_data(
        boundary.record_end.checked_sub(boundary.record_start),
        "retained UHC range record count underflowed",
    )?;
    if observed_record_count != expected_record_count || canonical_byte_count == 0 {
        return Err(invalid_data(
            "retained UHC concurrent range record proof is incomplete",
        ));
    }
    let (raw_sha256, raw_byte_count) = hash_raw_range(&input, boundary)?;
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
    Ok(Some(UHCRawRangeManifest {
        range_ordinal: boundary.range_ordinal,
        raw_byte_start: boundary.raw_byte_start,
        raw_byte_end: boundary.raw_byte_end,
        raw_byte_count,
        raw_sha256,
        record_start: boundary.record_start,
        record_end: boundary.record_end,
        record_count: observed_record_count,
        canonical_sha256: sha256_hex(&finalize_sha256(canonical_digest)),
        canonical_byte_count,
    }))
}

struct ConcurrentRangeWorkers {
    senders: Vec<SyncSender<RangeWorkerMessage>>,
    pending: Vec<PendingRecordBatch>,
    handles: Vec<thread::JoinHandle<(io::Result<Option<UHCRawRangeManifest>>, Duration)>>,
}

impl ConcurrentRangeWorkers {
    fn spawn(input: Arc<File>, range_count: usize) -> io::Result<Self> {
        let mut workers = Self {
            senders: Vec::with_capacity(range_count),
            pending: (0..range_count)
                .map(|_| PendingRecordBatch::default())
                .collect(),
            handles: Vec::with_capacity(range_count),
        };
        for range_ordinal in 0..range_count {
            let (sender, receiver) = sync_channel(RANGE_BATCH_QUEUE_DEPTH);
            let worker_input = Arc::clone(&input);
            let handle = thread::Builder::new()
                .name(format!("uhc-range-{range_ordinal}"))
                .spawn(move || {
                    let started = Instant::now();
                    let result = run_range_worker(worker_input, receiver);
                    (result, started.elapsed())
                })?;
            workers.senders.push(sender);
            workers.handles.push(handle);
        }
        Ok(workers)
    }

    fn send_record(&mut self, range_ordinal: usize, record: &[u8]) -> io::Result<()> {
        let batch = some_or_invalid_data(
            self.pending.get_mut(range_ordinal),
            "retained UHC range worker is unavailable",
        )?;
        batch.byte_count = some_or_invalid_data(
            batch.byte_count.checked_add(record.len()),
            "retained UHC range worker batch overflowed",
        )?;
        batch.records.push(record.to_vec());
        if batch.byte_count >= RANGE_RECORD_BATCH_BYTES {
            self.flush(range_ordinal)?;
        }
        Ok(())
    }

    fn flush(&mut self, range_ordinal: usize) -> io::Result<()> {
        let batch = some_or_invalid_data(
            self.pending.get_mut(range_ordinal),
            "retained UHC range worker is unavailable",
        )?;
        if batch.records.is_empty() {
            return Ok(());
        }
        let records = std::mem::take(&mut batch.records);
        batch.byte_count = 0;
        self.senders[range_ordinal]
            .send(RangeWorkerMessage::Records(records))
            .map_err(|_| invalid_data("retained UHC range worker stopped unexpectedly"))
    }

    fn finish_range(&mut self, boundary: RawRangeBoundary) -> io::Result<()> {
        let range_ordinal = boundary.range_ordinal as usize;
        self.flush(range_ordinal)?;
        self.senders[range_ordinal]
            .send(RangeWorkerMessage::Finish(boundary))
            .map_err(|_| invalid_data("retained UHC range worker stopped unexpectedly"))
    }

    fn finish(mut self) -> io::Result<(Vec<UHCRawRangeManifest>, Duration)> {
        self.senders.clear();
        let mut ranges = Vec::with_capacity(self.handles.len());
        let mut worker_max = Duration::ZERO;
        for handle in self.handles.drain(..) {
            let (result, elapsed) = handle
                .join()
                .map_err(|_| io::Error::other("UHC concurrent range worker panicked"))?;
            worker_max = worker_max.max(elapsed);
            if let Some(range) = result? {
                ranges.push(range);
            }
        }
        ranges.sort_by_key(|range| range.range_ordinal);
        Ok((ranges, worker_max))
    }
}

impl Drop for ConcurrentRangeWorkers {
    fn drop(&mut self) {
        self.senders.clear();
        for handle in self.handles.drain(..) {
            let _ = handle.join();
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct RawRangeBoundary {
    range_ordinal: u64,
    raw_byte_start: u64,
    raw_byte_end: u64,
    record_start: u64,
    record_end: u64,
}

fn scan_raw_and_build_ranges(
    input: &File,
    range_input: Arc<File>,
    raw_output: Option<&mut File>,
    offsets_output: &mut File,
    expected_sha256: &[u8; SHA256_BYTES],
    expected_byte_count: u64,
    range_count: usize,
) -> io::Result<RawScanResult> {
    let mut raw_output = raw_output;
    let mut offset_writer = BufWriter::with_capacity(READ_BUFFER_BYTES, offsets_output);
    let mut raw_digest = Sha256::new();
    let concurrent_ranges = range_count <= MAX_RANGE_WORKERS;
    let mut framer = JsonObjectFramer::array(concurrent_ranges, MAX_PROVIDER_RECORD_BYTES);
    let mut buffer = vec![0u8; READ_BUFFER_BYTES];
    let mut absolute_offset = 0u64;
    let mut record_count = 0u64;
    let mut read_hash_copy = Duration::ZERO;
    let mut frame_offset_index = Duration::ZERO;
    let mut active_range: Option<StreamingRangeBoundary> = None;
    let mut boundary_count = 0usize;
    let mut range_workers = if concurrent_ranges {
        Some(ConcurrentRangeWorkers::spawn(range_input, range_count)?)
    } else {
        None
    };

    while absolute_offset < expected_byte_count {
        let remaining =
            usize::try_from((expected_byte_count - absolute_offset).min(READ_BUFFER_BYTES as u64))
                .map_err(|_| invalid_data("UHC retained raw read size overflowed"))?;
        let started = Instant::now();
        let bytes_read = input.read_at(&mut buffer[..remaining], absolute_offset)?;
        if bytes_read == 0 {
            return Err(invalid_data(
                "UHC retained raw artifact ended before its expected byte count",
            ));
        }
        let chunk = &buffer[..bytes_read];
        raw_digest.update(chunk);
        if let Some(output) = raw_output.as_deref_mut() {
            output.write_all(chunk)?;
        }
        read_hash_copy += started.elapsed();

        let frame_started = Instant::now();
        framer.feed(chunk, |record, record_start, record_end| {
            if record_count >= MAX_RECORD_COUNT {
                return Err(invalid_data(format!(
                    "retained UHC artifact exceeds the {MAX_RECORD_COUNT} record limit"
                )));
            }
            offset_writer.write_all(&record_start.to_be_bytes())?;
            offset_writer.write_all(&record_end.to_be_bytes())?;
            if let Some(workers) = range_workers.as_mut() {
                workers.send_record(boundary_count, record)?;
                let range = active_range.get_or_insert_with(|| {
                    StreamingRangeBoundary::new(boundary_count as u64, record_count, record_start)
                });
                range.add_record(record_end)?;
            }
            record_count += 1;
            if concurrent_ranges
                && boundary_count + 1 < range_count
                && record_end
                    >= ceil_partition_boundary(
                        expected_byte_count,
                        boundary_count + 1,
                        range_count,
                    )?
            {
                let completed = some_or_invalid_data(
                    active_range.take(),
                    "retained UHC range state disappeared",
                )?;
                some_or_invalid_data(
                    range_workers.as_mut(),
                    "retained UHC range workers disappeared",
                )?
                .finish_range(completed.finish()?)?;
                boundary_count += 1;
            }
            Ok(())
        })?;
        frame_offset_index += frame_started.elapsed();
        absolute_offset = some_or_invalid_data(
            absolute_offset.checked_add(bytes_read as u64),
            "UHC retained raw byte count overflowed",
        )?;
    }
    let mut extra = [0u8; 1];
    if input.read_at(&mut extra, expected_byte_count)? != 0 {
        return Err(invalid_data(
            "UHC retained raw artifact exceeds its expected byte count",
        ));
    }
    framer.finish()?;
    if let Some(range) = active_range.take() {
        some_or_invalid_data(
            range_workers.as_mut(),
            "retained UHC range workers disappeared",
        )?
        .finish_range(range.finish()?)?;
        boundary_count += 1;
    }
    offset_writer.flush()?;
    if finalize_sha256(raw_digest) != *expected_sha256 {
        return Err(invalid_data(
            "UHC retained raw artifact SHA-256 does not match its expected identity",
        ));
    }
    if record_count == 0 {
        return Err(invalid_data("retained UHC artifact contains no records"));
    }
    checked_i64_domain(record_count, "record count")?;
    let raw_sync = if let Some(output) = raw_output.as_deref() {
        let sync_file = output.try_clone()?;
        Some(thread::spawn(move || {
            let started = Instant::now();
            let result = sync_file.sync_data();
            (result, started.elapsed())
        }))
    } else {
        None
    };
    let range_tail_started = Instant::now();
    let (ranges, concurrent_range_worker_max) = if let Some(workers) = range_workers {
        workers.finish()?
    } else {
        (Vec::new(), Duration::ZERO)
    };
    let range_verification_tail = range_tail_started.elapsed();
    let raw_fsync = if let Some(handle) = raw_sync {
        let (result, elapsed) = handle
            .join()
            .map_err(|_| io::Error::other("UHC retained raw fsync worker panicked"))?;
        result?;
        elapsed
    } else {
        Duration::ZERO
    };
    if ranges.len() != boundary_count {
        return Err(invalid_data(
            "retained UHC concurrent range proof count is incomplete",
        ));
    }
    Ok(RawScanResult {
        record_count,
        ranges,
        read_hash_copy,
        frame_offset_index,
        concurrent_range_worker_max,
        range_verification_tail,
        raw_fsync,
    })
}
