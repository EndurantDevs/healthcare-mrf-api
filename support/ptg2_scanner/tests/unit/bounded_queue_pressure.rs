use super::*;

const QUEUE_FULL_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);

struct FlushSignalWriter(Sender<()>);

impl Write for FlushSignalWriter {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.0.send(()).expect("flush observer dropped");
        Ok(())
    }
}

fn wait_until_event_is_drained(event_rx: &Receiver<CopyFileEvent>, failure_message: &str) {
    let started_at = Instant::now();
    while !event_rx.is_empty() {
        assert!(
            started_at.elapsed() < QUEUE_FULL_HANDSHAKE_TIMEOUT,
            "{failure_message}"
        );
        thread::yield_now();
    }
}

pub(super) fn empty_copy_file_event() -> CopyFileEvent {
    CopyFileEvent {
        record_kind: "copy_file".to_owned(),
        path: "test.copy".to_owned(),
        bytes: 0,
        row_count: 0,
        final_file: true,
        partition: None,
        partition_count: None,
        format: None,
        version: None,
        sha256: None,
    }
}

pub(super) fn assert_worker_job_queue_pressure(
    blocked_micros: &mut u128,
    stats: &mut RawChunkStats,
) {
    let (job_tx, job_rx) = bounded(1);
    job_tx
        .send(WorkerJob::Rates {
            procedure: Map::new(),
            rates: Vec::new(),
        })
        .unwrap();
    let (event_tx, event_rx) = unbounded();
    event_tx.send(empty_copy_file_event()).unwrap();
    let event_observer_rx = event_rx.clone();
    let mut writer = Vec::new();
    let retry_event_tx = event_tx.clone();
    let receiver = thread::spawn(move || {
        wait_until_event_is_drained(&event_observer_rx, "worker-job queue never reported Full");
        retry_event_tx.send(empty_copy_file_event()).unwrap();
        wait_until_event_is_drained(
            &event_observer_rx,
            "worker-job queue never reported a second Full",
        );
        let _ = job_rx
            .recv_timeout(QUEUE_FULL_HANDSHAKE_TIMEOUT)
            .expect("prefilled worker job was not released");
        let _ = job_rx
            .recv_timeout(QUEUE_FULL_HANDSHAKE_TIMEOUT)
            .expect("pressured worker job was not delivered");
    });
    let blocked_sends_before = stats.queue_blocked_sends;
    let mut copy_file_event_gate = CopyFileEventGate::passthrough();
    let mut io_state = InNetworkEnqueueIo {
        tx: &job_tx,
        event_rx: &event_rx,
        writer: &mut writer,
        copy_file_event_gate: &mut copy_file_event_gate,
        cancelled: None,
        producer_blocked_micros: blocked_micros,
        raw_chunk_stats: stats,
    };
    send_worker_job(
        &mut io_state,
        WorkerJob::Rates {
            procedure: Map::new(),
            rates: Vec::new(),
        },
    )
    .unwrap();
    receiver.join().unwrap();
    assert!(queue_pressure_incremented(
        blocked_sends_before,
        stats.queue_blocked_sends
    ));

    event_tx.send(empty_copy_file_event()).unwrap();
    drain_copy_file_events_until_workers_finish::<_, ()>(
        &event_rx,
        &[],
        &mut writer,
        &mut copy_file_event_gate,
    )
    .unwrap();
}

pub(super) fn assert_provider_reference_queue_pressure(
    blocked_micros: &mut u128,
    stats: &mut RawChunkStats,
) {
    let (vec_batch_tx, vec_batch_rx) = bounded(1);
    vec_batch_tx
        .send(RawRateChunk::with_capacity(0, 0))
        .unwrap();
    let (vec_event_tx, vec_event_rx) = unbounded();
    vec_event_tx.send(empty_copy_file_event()).unwrap();
    let vec_event_observer_rx = vec_event_rx.clone();
    let vec_receiver = thread::spawn(move || {
        wait_until_event_is_drained(
            &vec_event_observer_rx,
            "provider-reference Vec queue never reported Full",
        );
        let _ = vec_batch_rx
            .recv_timeout(QUEUE_FULL_HANDSHAKE_TIMEOUT)
            .expect("prefilled provider-reference Vec batch was not released");
        let _ = vec_batch_rx
            .recv_timeout(QUEUE_FULL_HANDSHAKE_TIMEOUT)
            .expect("pressured provider-reference Vec batch was not delivered");
    });
    let mut vec_writer = Vec::new();
    let mut vec_gate = CopyFileEventGate::passthrough();
    send_provider_ref_batch(
        &vec_batch_tx,
        &vec_event_rx,
        &mut vec_writer,
        blocked_micros,
        stats,
        &mut vec_gate,
        RawRateChunk::with_capacity(0, 0),
    )
    .unwrap();
    vec_receiver.join().unwrap();

    let (batch_tx, batch_rx) = bounded(1);
    batch_tx.send(RawRateChunk::with_capacity(0, 0)).unwrap();
    let (event_tx, event_rx) = unbounded();
    event_tx.send(empty_copy_file_event()).unwrap();
    let (flush_tx, flush_rx) = bounded(0);
    let mut writer = FlushSignalWriter(flush_tx);
    let receiver = thread::spawn(move || {
        flush_rx
            .recv_timeout(QUEUE_FULL_HANDSHAKE_TIMEOUT)
            .expect("provider-reference queue never reported Full");
        event_tx.send(empty_copy_file_event()).unwrap();
        flush_rx
            .recv_timeout(QUEUE_FULL_HANDSHAKE_TIMEOUT)
            .expect("provider-reference queue never reported a second Full");
        let _ = batch_rx
            .recv_timeout(QUEUE_FULL_HANDSHAKE_TIMEOUT)
            .expect("prefilled provider-reference batch was not released");
        let _ = batch_rx
            .recv_timeout(QUEUE_FULL_HANDSHAKE_TIMEOUT)
            .expect("pressured provider-reference batch was not delivered");
    });
    let blocked_sends_before = stats.queue_blocked_sends;
    let mut copy_file_event_gate = CopyFileEventGate::passthrough();
    send_provider_ref_batch(
        &batch_tx,
        &event_rx,
        &mut writer,
        blocked_micros,
        stats,
        &mut copy_file_event_gate,
        RawRateChunk::with_capacity(0, 0),
    )
    .unwrap();
    receiver.join().unwrap();
    assert!(queue_pressure_incremented(
        blocked_sends_before,
        stats.queue_blocked_sends
    ));
}
