#[cfg(test)]
mod tests {
    use super::*;
    use crossbeam_channel::unbounded;

    #[test]
    fn semantic_progress_interval_stays_inside_five_second_contract() {
        assert!(SEMANTIC_PROGRESS_INTERVAL <= Duration::from_secs(4));
        assert!(SEMANTIC_PROGRESS_INTERVAL < MAX_SEMANTIC_PROGRESS_INTERVAL);
    }

    #[test]
    fn semantic_progress_advances_during_one_unfinished_in_network_item() {
        let progress = Arc::new(ScannerSemanticProgress::default());
        let (stop_tx, stop_rx) = bounded(1);
        let (frame_tx, frame_rx) = unbounded();
        let reporter_progress = Arc::clone(&progress);
        let reporter = thread::spawn(move || {
            run_semantic_progress_reporter(
                stop_rx,
                Duration::from_millis(5),
                reporter_progress,
                |snapshot| frame_tx.send(snapshot).unwrap(),
            );
        });

        progress.record_negotiated_rate_parsed();
        let first = frame_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(first.in_network_objects_completed, 0);

        progress.record_negotiated_rate_transform_started();
        progress.record_provider_group_union_visits(65_536);
        let second = frame_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(second.in_network_objects_completed, 0);
        assert!(second.semantic_work_completed > first.semantic_work_completed);
        assert!(second.provider_group_union_visits > first.provider_group_union_visits);

        stop_tx.send(()).unwrap();
        reporter.join().unwrap();
    }

    #[test]
    fn semantic_progress_frame_exposes_machine_readable_movement() {
        let compressed_bytes_read = Arc::new(AtomicU64::new(50));
        let snapshot = ScannerSemanticSnapshot {
            semantic_work_completed: 7,
            negotiated_rates_parsed: 2,
            negotiated_rates_transform_started: 1,
            provider_group_union_visits: 4,
            ..ScannerSemanticSnapshot::default()
        };
        let line = semantic_progress_line(
            Path::new("/tmp/rates.json.gz"),
            100,
            &compressed_bytes_read,
            snapshot,
            Instant::now(),
        );
        assert!(line.contains("progress_basis=semantic_work"));
        assert!(line.contains("semantic_work_completed=7"));
        assert!(line.contains("provider_group_union_visits=4"));
        assert!(line.contains("\tpercent=unknown\t"));
        assert!(line.ends_with("done=false"));
    }

    #[test]
    fn scan_finalize_progress_emits_movement_and_not_heartbeat_frames() {
        let progress = Arc::new(ScannerSemanticProgress::default());
        let (stop_tx, stop_rx) = bounded(1);
        let (frame_tx, frame_rx) = unbounded();
        let reporter_progress = Arc::clone(&progress);
        let reporter = thread::spawn(move || {
            run_semantic_progress_reporter(
                stop_rx,
                Duration::from_millis(5),
                reporter_progress,
                |snapshot| frame_tx.send(snapshot).unwrap(),
            );
        });

        progress.record_scan_finalize_job_started();
        let started = frame_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(started.scan_finalize_jobs_started, 1);
        assert_eq!(started.scan_finalize_pairs_processed, 0);
        assert!(frame_rx.recv_timeout(Duration::from_millis(20)).is_err());

        progress.record_scan_finalize_work(32 * 4_096, 4_096, 1, 0);
        let sorting = frame_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_ne!(sorting, started);
        assert_eq!(sorting.scan_finalize_pairs_processed, 4_096);
        assert_eq!(sorting.scan_finalize_chunks_sorted, 1);

        progress.record_scan_finalize_work(32 * 4_096, 4_096, 0, 1);
        progress.record_scan_finalize_job_completed();
        let mut merged = frame_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        while merged.scan_finalize_jobs_completed == 0 {
            merged = frame_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        }
        assert!(merged.scan_finalize_pairs_processed > sorting.scan_finalize_pairs_processed);
        assert_eq!(merged.scan_finalize_chunks_merged, 1);
        assert_eq!(merged.scan_finalize_jobs_completed, 1);

        stop_tx.send(()).unwrap();
        reporter.join().unwrap();
    }

    #[test]
    fn scan_finalize_progress_frame_names_the_phase_and_counters() {
        let compressed_bytes_read = Arc::new(AtomicU64::new(100));
        let snapshot = ScannerSemanticSnapshot {
            semantic_work_completed: 12,
            scan_finalize_jobs_started: 2,
            scan_finalize_jobs_completed: 1,
            scan_finalize_bytes_processed: 4_096,
            scan_finalize_pairs_processed: 128,
            scan_finalize_chunks_sorted: 3,
            scan_finalize_chunks_merged: 2,
            scan_finalize_sort_comparisons: 512,
            ..ScannerSemanticSnapshot::default()
        };
        let line = semantic_progress_line(
            Path::new("/tmp/rates.json.gz"),
            100,
            &compressed_bytes_read,
            snapshot,
            Instant::now(),
        );

        assert!(line.contains("\tphase=scan-finalize\t"));
        assert!(line.contains("\tscan_finalize_bytes_processed=4096\t"));
        assert!(line.contains("\tscan_finalize_pairs_processed=128\t"));
        assert!(line.contains("\tscan_finalize_chunks_sorted=3\t"));
        assert!(line.contains("\tscan_finalize_chunks_merged=2\t"));
        assert!(line.contains("\tscan_finalize_sort_comparisons=512\t"));
    }

    #[test]
    fn zero_semantic_updates_are_noops() {
        let progress = ScannerSemanticProgress::default();
        let before = progress.snapshot();

        progress.record_negotiated_rates_parsed(0);
        progress.record_provider_group_union_visits(0);
        progress.record_in_network_objects_completed(0);
        progress.record_scan_finalize_work(0, 0, 0, 0);
        progress.record_scan_finalize_sort_comparisons(0);

        assert_eq!(progress.snapshot(), before);
    }

    #[test]
    fn semantic_progress_frame_handles_zero_input_and_zero_elapsed_time() {
        let compressed_bytes_read = Arc::new(AtomicU64::new(0));
        let line = semantic_progress_line(
            Path::new("/tmp/empty.json"),
            0,
            &compressed_bytes_read,
            ScannerSemanticSnapshot::default(),
            Instant::now() + Duration::from_secs(1),
        );

        assert!(line.contains("\tphase=scan\t"));
        assert!(line.contains("\tpercent=unknown\t"));
        assert!(line.contains("\tcompressed_mib_s=0.00\t"));
    }
}
