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

#[test]
fn background_worker_failures_join_every_descriptor_before_cleanup() {
    let fixture = Fixture::new(FIXTURE);
    let root = RootDirectory::open(&fixture.output).expect("open retained root");
    let mut temporary = root.create_temporary("worker-failure").expect("temporary file");
    temporary
        .file_mut()
        .write_all(FIXTURE)
        .expect("write temporary fixture");
    let temporary_path = fixture.output.join(&temporary.name);
    let cleanup_probe =
        probe_close_before_unlink(&mut temporary, temporary_path.clone());

    let failed_worker = thread::spawn(
        || -> (io::Result<Option<UHCRawRangeManifest>>, Duration) {
            (Err(invalid_data("first range worker failure")), Duration::ZERO)
        },
    );
    let delayed_worker_file = temporary.file().try_clone().expect("clone worker file");
    let worker_joined = Arc::new(AtomicBool::new(false));
    let worker_joined_signal = Arc::clone(&worker_joined);
    let (worker_started_sender, worker_started_receiver) = sync_channel(0);
    let (worker_release_sender, worker_release_receiver) = sync_channel(0);
    let delayed_worker = thread::spawn(move || {
        worker_started_sender
            .send(())
            .expect("signal descriptor-holding worker start");
        let _ = worker_release_receiver.recv();
        drop(delayed_worker_file);
        worker_joined_signal.store(true, Ordering::SeqCst);
        (Ok(None), Duration::ZERO)
    });
    let workers = ConcurrentRangeWorkers {
        senders: Vec::new(),
        pending: Vec::new(),
        handles: vec![failed_worker, delayed_worker],
    };

    let sync_file = temporary.file().try_clone().expect("clone sync file");
    let sync_joined = Arc::new(AtomicBool::new(false));
    let sync_joined_signal = Arc::clone(&sync_joined);
    let (sync_finished_sender, sync_finished_receiver) = sync_channel(0);
    let raw_sync = thread::spawn(move || {
        drop(sync_file);
        sync_joined_signal.store(true, Ordering::SeqCst);
        sync_finished_sender
            .send(())
            .expect("signal raw-sync worker finish");
        (
            Err(io::Error::other("raw sync failure")),
            Duration::ZERO,
        )
    });

    let (finisher_started_sender, finisher_started_receiver) = sync_channel(0);
    let (finish_result_sender, finish_result_receiver) = sync_channel(1);
    let finish_handle = thread::spawn(move || {
        finisher_started_sender
            .send(())
            .expect("signal background finisher start");
        let result = finish_scan_workers(Some(workers), Some(raw_sync));
        finish_result_sender
            .send(result)
            .expect("return background finisher result");
    });
    finisher_started_receiver
        .recv()
        .expect("background finisher started");
    worker_started_receiver
        .recv()
        .expect("descriptor-holding worker started");
    sync_finished_receiver
        .recv()
        .expect("raw-sync worker finished");
    assert!(
        matches!(
            finish_result_receiver.recv_timeout(Duration::from_millis(50)),
            Err(std::sync::mpsc::RecvTimeoutError::Timeout)
        ),
        "range-worker error detached a descriptor-holding peer"
    );
    worker_release_sender
        .send(())
        .expect("release descriptor-holding worker");
    let error = finish_result_receiver
        .recv()
        .expect("receive background finisher result")
        .expect_err("range failure must win after every join");
    finish_handle.join().expect("join background finisher");
    assert!(error.to_string().contains("first range worker failure"));
    assert!(worker_joined.load(Ordering::SeqCst));
    assert!(sync_joined.load(Ordering::SeqCst));
    drop(temporary);
    assert!(cleanup_probe.load(Ordering::SeqCst));
    assert!(!temporary_path.exists());
}
