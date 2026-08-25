//! Progress reporting for long-running scanner passes.

use crossbeam_channel::{bounded, Receiver, RecvTimeoutError, Sender};
use std::collections::HashMap;
use std::io::{self, Write};
use std::path::Path;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

/// The live-progress contract allows at most five seconds without a semantic
/// movement frame while scanner work is advancing. Four seconds leaves room
/// for normal scheduler jitter without turning the reporter into a hot loop.
pub const SEMANTIC_PROGRESS_INTERVAL: Duration = Duration::from_secs(4);
pub const MAX_SEMANTIC_PROGRESS_INTERVAL: Duration = Duration::from_secs(5);

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ScannerSemanticSnapshot {
    pub semantic_work_completed: u64,
    pub negotiated_rates_parsed: u64,
    pub negotiated_rates_transform_started: u64,
    pub provider_group_union_visits: u64,
    pub provider_npi_union_visits: u64,
    pub rate_chunks_completed: u64,
    pub in_network_objects_completed: u64,
    pub scan_finalize_jobs_started: u64,
    pub scan_finalize_jobs_completed: u64,
    pub scan_finalize_bytes_processed: u64,
    pub scan_finalize_pairs_processed: u64,
    pub scan_finalize_chunks_sorted: u64,
    pub scan_finalize_chunks_merged: u64,
    pub scan_finalize_sort_comparisons: u64,
}

impl ScannerSemanticSnapshot {
    fn has_movement(self) -> bool {
        self.semantic_work_completed > 0
    }
}

#[derive(Debug, Default)]
pub struct ScannerSemanticProgress {
    semantic_work_completed: AtomicU64,
    negotiated_rates_parsed: AtomicU64,
    negotiated_rates_transform_started: AtomicU64,
    provider_group_union_visits: AtomicU64,
    provider_npi_union_visits: AtomicU64,
    rate_chunks_completed: AtomicU64,
    in_network_objects_completed: AtomicU64,
    scan_finalize_jobs_started: AtomicU64,
    scan_finalize_jobs_completed: AtomicU64,
    scan_finalize_bytes_processed: AtomicU64,
    scan_finalize_pairs_processed: AtomicU64,
    scan_finalize_chunks_sorted: AtomicU64,
    scan_finalize_chunks_merged: AtomicU64,
    scan_finalize_sort_comparisons: AtomicU64,
}

impl ScannerSemanticProgress {
    fn record_work(&self, amount: u64) {
        self.semantic_work_completed
            .fetch_add(amount, Ordering::AcqRel);
    }

    pub fn record_negotiated_rate_parsed(&self) {
        self.record_negotiated_rates_parsed(1);
    }

    pub fn record_negotiated_rates_parsed(&self, amount: u64) {
        if amount == 0 {
            return;
        }
        self.negotiated_rates_parsed
            .fetch_add(amount, Ordering::AcqRel);
        self.record_work(amount);
    }

    pub fn record_negotiated_rate_transform_started(&self) {
        self.negotiated_rates_transform_started
            .fetch_add(1, Ordering::AcqRel);
        self.record_work(1);
    }

    pub fn record_provider_group_union_visits(&self, amount: u64) {
        if amount == 0 {
            return;
        }
        self.provider_group_union_visits
            .fetch_add(amount, Ordering::AcqRel);
        self.record_work(amount);
    }

    pub fn record_provider_npi_union_visits(&self, amount: u64) {
        if amount == 0 {
            return;
        }
        self.provider_npi_union_visits
            .fetch_add(amount, Ordering::AcqRel);
        self.record_work(amount);
    }

    pub fn record_rate_chunk_completed(&self) {
        self.rate_chunks_completed.fetch_add(1, Ordering::AcqRel);
        self.record_work(1);
    }

    pub fn record_in_network_object_completed(&self) {
        self.record_in_network_objects_completed(1);
    }

    pub fn record_in_network_objects_completed(&self, amount: u64) {
        if amount == 0 {
            return;
        }
        self.in_network_objects_completed
            .fetch_add(amount, Ordering::AcqRel);
        self.record_work(amount);
    }

    pub fn record_scan_finalize_job_started(&self) {
        self.scan_finalize_jobs_started
            .fetch_add(1, Ordering::AcqRel);
        self.record_work(1);
    }

    pub fn record_scan_finalize_job_completed(&self) {
        self.scan_finalize_jobs_completed
            .fetch_add(1, Ordering::AcqRel);
        self.record_work(1);
    }

    pub fn record_scan_finalize_work(
        &self,
        bytes_processed: u64,
        pairs_processed: u64,
        chunks_sorted: u64,
        chunks_merged: u64,
    ) {
        if bytes_processed == 0 && pairs_processed == 0 && chunks_sorted == 0 && chunks_merged == 0
        {
            return;
        }
        self.scan_finalize_bytes_processed
            .fetch_add(bytes_processed, Ordering::AcqRel);
        self.scan_finalize_pairs_processed
            .fetch_add(pairs_processed, Ordering::AcqRel);
        self.scan_finalize_chunks_sorted
            .fetch_add(chunks_sorted, Ordering::AcqRel);
        self.scan_finalize_chunks_merged
            .fetch_add(chunks_merged, Ordering::AcqRel);
        let semantic_units = pairs_processed
            .saturating_add(chunks_sorted)
            .saturating_add(chunks_merged)
            .max(1);
        self.record_work(semantic_units);
    }

    pub fn record_scan_finalize_sort_comparisons(&self, comparisons: u64) {
        if comparisons == 0 {
            return;
        }
        self.scan_finalize_sort_comparisons
            .fetch_add(comparisons, Ordering::AcqRel);
        self.record_work(comparisons);
    }

    pub fn snapshot(&self) -> ScannerSemanticSnapshot {
        ScannerSemanticSnapshot {
            semantic_work_completed: self.semantic_work_completed.load(Ordering::Acquire),
            negotiated_rates_parsed: self.negotiated_rates_parsed.load(Ordering::Acquire),
            negotiated_rates_transform_started: self
                .negotiated_rates_transform_started
                .load(Ordering::Acquire),
            provider_group_union_visits: self.provider_group_union_visits.load(Ordering::Acquire),
            provider_npi_union_visits: self.provider_npi_union_visits.load(Ordering::Acquire),
            rate_chunks_completed: self.rate_chunks_completed.load(Ordering::Acquire),
            in_network_objects_completed: self.in_network_objects_completed.load(Ordering::Acquire),
            scan_finalize_jobs_started: self.scan_finalize_jobs_started.load(Ordering::Acquire),
            scan_finalize_jobs_completed: self.scan_finalize_jobs_completed.load(Ordering::Acquire),
            scan_finalize_bytes_processed: self
                .scan_finalize_bytes_processed
                .load(Ordering::Acquire),
            scan_finalize_pairs_processed: self
                .scan_finalize_pairs_processed
                .load(Ordering::Acquire),
            scan_finalize_chunks_sorted: self.scan_finalize_chunks_sorted.load(Ordering::Acquire),
            scan_finalize_chunks_merged: self.scan_finalize_chunks_merged.load(Ordering::Acquire),
            scan_finalize_sort_comparisons: self
                .scan_finalize_sort_comparisons
                .load(Ordering::Acquire),
        }
    }
}

pub struct ScannerSemanticProgressReporter {
    stop_tx: Sender<()>,
    handle: Option<JoinHandle<()>>,
}

impl ScannerSemanticProgressReporter {
    pub fn start(
        path: &Path,
        total_bytes: u64,
        compressed_bytes_read: Arc<AtomicU64>,
        progress: Arc<ScannerSemanticProgress>,
        started_at: Instant,
    ) -> io::Result<Self> {
        debug_assert!(SEMANTIC_PROGRESS_INTERVAL <= MAX_SEMANTIC_PROGRESS_INTERVAL);
        let path = path.to_path_buf();
        let (stop_tx, stop_rx) = bounded(1);
        let handle = thread::Builder::new()
            .name("ptg2-semantic-progress".to_string())
            .spawn(move || {
                run_semantic_progress_reporter(
                    stop_rx,
                    SEMANTIC_PROGRESS_INTERVAL,
                    progress,
                    |snapshot| {
                        let line = semantic_progress_line(
                            &path,
                            total_bytes,
                            &compressed_bytes_read,
                            snapshot,
                            started_at,
                        );
                        let stderr = io::stderr();
                        let mut writer = stderr.lock();
                        let _ = writeln!(writer, "{line}");
                    },
                );
            })?;
        Ok(Self {
            stop_tx,
            handle: Some(handle),
        })
    }
}

impl Drop for ScannerSemanticProgressReporter {
    fn drop(&mut self) {
        let _ = self.stop_tx.try_send(());
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

fn run_semantic_progress_reporter(
    stop_rx: Receiver<()>,
    interval: Duration,
    progress: Arc<ScannerSemanticProgress>,
    mut emit: impl FnMut(ScannerSemanticSnapshot),
) {
    let mut last_emitted = ScannerSemanticSnapshot::default();
    loop {
        match stop_rx.recv_timeout(interval) {
            Ok(()) | Err(RecvTimeoutError::Disconnected) => break,
            Err(RecvTimeoutError::Timeout) => {
                let snapshot = progress.snapshot();
                if snapshot.has_movement() && snapshot != last_emitted {
                    emit(snapshot);
                    last_emitted = snapshot;
                }
            }
        }
    }
}

fn semantic_progress_line(
    path: &Path,
    total_bytes: u64,
    compressed_bytes_read: &Arc<AtomicU64>,
    snapshot: ScannerSemanticSnapshot,
    started_at: Instant,
) -> String {
    let bytes_read = compressed_bytes_read
        .load(Ordering::Relaxed)
        .min(total_bytes);
    let elapsed_seconds = started_at.elapsed().as_secs_f64();
    let compressed_mib_s = if elapsed_seconds > 0.0 {
        (bytes_read as f64 / (1024.0 * 1024.0)) / elapsed_seconds
    } else {
        0.0
    };
    let negotiated_rates = snapshot
        .negotiated_rates_parsed
        .max(snapshot.negotiated_rates_transform_started);
    let phase = if snapshot.scan_finalize_jobs_started > 0 {
        "scan-finalize"
    } else {
        "scan"
    };
    format!(
        "PTG2_SCANNER_PROGRESS\tpath={}\tphase={}\tprogress_basis=semantic_work\tsemantic_work_completed={}\tcompressed_bytes={}\ttotal_bytes={}\tpercent=unknown\tcompressed_mib_s={:.2}\telapsed_seconds={:.0}\teta_seconds=unknown\tobjects={}\tnegotiated_rates={}\tnegotiated_rates_parsed={}\tnegotiated_rates_transform_started={}\tprovider_group_union_visits={}\tprovider_npi_union_visits={}\trate_chunks_completed={}\tin_network={}\tscan_finalize_jobs_started={}\tscan_finalize_jobs_completed={}\tscan_finalize_bytes_processed={}\tscan_finalize_pairs_processed={}\tscan_finalize_chunks_sorted={}\tscan_finalize_chunks_merged={}\tscan_finalize_sort_comparisons={}\tdone=false",
        path.display(),
        phase,
        snapshot.semantic_work_completed,
        bytes_read,
        total_bytes,
        compressed_mib_s,
        elapsed_seconds,
        negotiated_rates,
        negotiated_rates,
        snapshot.negotiated_rates_parsed,
        snapshot.negotiated_rates_transform_started,
        snapshot.provider_group_union_visits,
        snapshot.provider_npi_union_visits,
        snapshot.rate_chunks_completed,
        snapshot.in_network_objects_completed,
        snapshot.scan_finalize_jobs_started,
        snapshot.scan_finalize_jobs_completed,
        snapshot.scan_finalize_bytes_processed,
        snapshot.scan_finalize_pairs_processed,
        snapshot.scan_finalize_chunks_sorted,
        snapshot.scan_finalize_chunks_merged,
        snapshot.scan_finalize_sort_comparisons,
    )
}

pub fn emit_progress(
    path: &Path,
    total_bytes: u64,
    compressed_bytes_read: &Arc<AtomicU64>,
    object_counts: &HashMap<String, u64>,
    started_at: Instant,
    done: bool,
) {
    let bytes_read = compressed_bytes_read
        .load(Ordering::Relaxed)
        .min(total_bytes);
    let percent = if total_bytes > 0 {
        (bytes_read as f64 / total_bytes as f64) * 100.0
    } else {
        0.0
    };
    let total_objects: u64 = object_counts.values().sum();
    let elapsed_seconds = started_at.elapsed().as_secs_f64();
    let compressed_mib_s = if elapsed_seconds > 0.0 {
        (bytes_read as f64 / (1024.0 * 1024.0)) / elapsed_seconds
    } else {
        0.0
    };
    let eta_seconds = if compressed_mib_s > 0.0 && total_bytes > bytes_read {
        Some(((total_bytes - bytes_read) as f64 / (1024.0 * 1024.0)) / compressed_mib_s)
    } else {
        None
    };
    let counts = object_counts
        .iter()
        .map(|(name, count)| format!("{}={}", name, count))
        .collect::<Vec<_>>()
        .join("\t");
    eprintln!(
        "PTG2_SCANNER_PROGRESS\tpath={}\tcompressed_bytes={}\ttotal_bytes={}\tpercent={:.2}\tcompressed_mib_s={:.2}\telapsed_seconds={:.0}\teta_seconds={}\tobjects={}\t{}\tdone={}",
        path.display(),
        bytes_read,
        total_bytes,
        percent,
        compressed_mib_s,
        elapsed_seconds,
        eta_seconds
            .map(|value| format!("{:.0}", value))
            .unwrap_or_else(|| "unknown".to_string()),
        total_objects,
        counts,
        if done { "true" } else { "false" },
    );
}

include!("progress_tests.rs");
