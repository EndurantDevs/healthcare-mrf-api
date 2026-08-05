use super::*;

const QUEUE_FULL_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);

struct QueueFullHandshakeWriter {
    full_seen_tx: Sender<()>,
}

impl Write for QueueFullHandshakeWriter {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.full_seen_tx
            .send(())
            .expect("queue-full handshake receiver closed");
        Ok(())
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
    let (full_seen_tx, full_seen_rx) = bounded(1);
    let (event_tx, event_rx) = unbounded();
    event_tx.send(empty_copy_file_event()).unwrap();
    let mut writer = QueueFullHandshakeWriter { full_seen_tx };
    let receiver = thread::spawn(move || {
        full_seen_rx
            .recv_timeout(QUEUE_FULL_HANDSHAKE_TIMEOUT)
            .expect("worker-job queue never reported Full");
        let _ = job_rx
            .recv_timeout(QUEUE_FULL_HANDSHAKE_TIMEOUT)
            .expect("prefilled worker job was not released");
        let _ = job_rx
            .recv_timeout(QUEUE_FULL_HANDSHAKE_TIMEOUT)
            .expect("pressured worker job was not delivered");
    });
    let blocked_sends_before = stats.queue_blocked_sends;
    send_worker_job(
        &job_tx,
        &event_rx,
        &mut writer,
        None,
        blocked_micros,
        stats,
        WorkerJob::Rates {
            procedure: Map::new(),
            rates: Vec::new(),
        },
    )
    .unwrap();
    receiver.join().unwrap();
    assert_eq!(stats.queue_blocked_sends, blocked_sends_before + 1);
}

pub(super) fn assert_provider_reference_queue_pressure(
    blocked_micros: &mut u128,
    stats: &mut RawChunkStats,
) {
    let (batch_tx, batch_rx) = bounded(1);
    batch_tx.send(RawRateChunk::with_capacity(0, 0)).unwrap();
    let (full_seen_tx, full_seen_rx) = bounded(1);
    let (event_tx, event_rx) = unbounded();
    event_tx.send(empty_copy_file_event()).unwrap();
    let mut writer = QueueFullHandshakeWriter { full_seen_tx };
    let receiver = thread::spawn(move || {
        full_seen_rx
            .recv_timeout(QUEUE_FULL_HANDSHAKE_TIMEOUT)
            .expect("provider-reference queue never reported Full");
        let _ = batch_rx
            .recv_timeout(QUEUE_FULL_HANDSHAKE_TIMEOUT)
            .expect("prefilled provider-reference batch was not released");
        let _ = batch_rx
            .recv_timeout(QUEUE_FULL_HANDSHAKE_TIMEOUT)
            .expect("pressured provider-reference batch was not delivered");
    });
    let blocked_sends_before = stats.queue_blocked_sends;
    send_provider_ref_batch(
        &batch_tx,
        &event_rx,
        &mut writer,
        blocked_micros,
        stats,
        RawRateChunk::with_capacity(0, 0),
    )
    .unwrap();
    receiver.join().unwrap();
    assert_eq!(stats.queue_blocked_sends, blocked_sends_before + 1);
}
