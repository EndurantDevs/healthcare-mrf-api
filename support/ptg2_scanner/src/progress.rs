//! Progress reporting for long-running scanner passes.

use std::collections::HashMap;
use std::path::Path;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use std::time::Instant;

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
