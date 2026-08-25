//! Input readers for compressed and plain TiC artifacts.

use crate::config::READ_BUF_SIZE;
use flate2::read::MultiGzDecoder;
use std::fs::File;
use std::io::{self, BufReader, Read, Seek, SeekFrom};
use std::path::Path;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

mod rapidgzip;

pub use rapidgzip::{
    open_full_scan_json_reader, open_full_scan_reader, open_full_scan_reader_exporting_index,
    open_indexed_ranges_reader, RapidgzipConfig,
};

include!("input/readers.rs");
include!("input/tests.rs");
