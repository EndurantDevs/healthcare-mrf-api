// Licensed under the HealthPorta Non-Commercial License (see LICENSE).

#[test]
fn offset_and_cleanup_io_failures_are_explicit() {
    let fixture = Fixture::new(b"");
    let empty = File::open(&fixture.source).expect("open empty offset spool");
    assert!(read_u64_at(&empty, 0)
        .expect_err("truncated offset rejected")
        .to_string()
        .contains("ended unexpectedly"));
    assert!(build_range_boundaries(&empty, 1, 1)
        .expect_err("invalid offset byte count rejected")
        .to_string()
        .contains("invalid byte count"));
    let empty_range = RawRangeBoundary {
        range_ordinal: 0,
        raw_byte_start: 0,
        raw_byte_end: 0,
        record_start: 0,
        record_end: 0,
    };
    assert!(hash_raw_range(&empty, empty_range)
        .expect_err("empty raw range rejected")
        .to_string()
        .contains("raw range is empty"));
    assert!(hash_raw_range(
        &empty,
        RawRangeBoundary {
            raw_byte_start: 1,
            ..empty_range
        },
    )
    .expect_err("reversed raw range rejected")
    .to_string()
    .contains("underflowed"));
    let incomplete_range_error = verify_raw_range(
        &empty,
        RawRangeBoundary {
            record_end: 1,
            ..empty_range
        },
    )
    .expect_err("empty range cannot claim a record");
    assert!(
        incomplete_range_error.to_string().contains("frame is incomplete"),
        "{incomplete_range_error}"
    );

    let root = RootDirectory::open(&fixture.output).expect("open retained root");
    fs::create_dir(fixture.output.join("nested")).expect("create nested directory");
    assert!(root.unlink("nested").is_err());
}

#[test]
fn worker_and_partition_error_adapters_are_deterministic() {
    assert!(ceil_partition_boundary(u64::MAX, usize::MAX, 1)
        .expect_err("partition boundary outside u64 rejected")
        .to_string()
        .contains("exceeds u64"));

    let (flush_sender, flush_receiver) = sync_channel(1);
    drop(flush_receiver);
    let mut flush_workers = ConcurrentRangeWorkers {
        senders: vec![flush_sender],
        pending: vec![PendingRecordBatch {
            records: vec![br#"{"id":1}"#.to_vec()],
            byte_count: 8,
        }],
        handles: Vec::new(),
    };
    assert!(flush_workers
        .flush(0)
        .expect_err("stopped range worker rejects a record batch")
        .to_string()
        .contains("stopped unexpectedly"));

    let (finish_sender, finish_receiver) = sync_channel(1);
    drop(finish_receiver);
    let mut finish_workers = ConcurrentRangeWorkers {
        senders: vec![finish_sender],
        pending: vec![PendingRecordBatch::default()],
        handles: Vec::new(),
    };
    assert!(finish_workers
        .finish_range(RawRangeBoundary {
            range_ordinal: 0,
            raw_byte_start: 0,
            raw_byte_end: 1,
            record_start: 0,
            record_end: 1,
        })
        .expect_err("stopped range worker rejects its boundary")
        .to_string()
        .contains("stopped unexpectedly"));

    let panicked = thread::spawn(|| -> (io::Result<Option<UHCRawRangeManifest>>, Duration) {
        panic!("deterministic range-worker panic fixture");
    });
    let join_workers = ConcurrentRangeWorkers {
        senders: Vec::new(),
        pending: Vec::new(),
        handles: vec![panicked],
    };
    assert!(join_workers
        .finish()
        .expect_err("panicked range worker rejected")
        .to_string()
        .contains("worker panicked"));
}
