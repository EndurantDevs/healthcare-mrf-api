//! Streaming hospital machine-readable-file parser for the V3.0.0 contract.

use crate::copy_format::{pg_text_array_field, write_copy_text_fields};
use crate::input::{
    open_full_scan_json_reader, open_full_scan_reader, strict_utf8_reader, RapidgzipConfig,
};
use crate::normalize::{canonical_decimal_text, compare_canonical_decimal_text};
use csv::{ReaderBuilder, StringRecord};
use serde::de::{Error as _, SeqAccess, Visitor};
use serde::{Deserialize, Serialize};
use serde_json::Number;
use sha2::{Digest, Sha256};
use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufWriter, Read, Write};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use struson::reader::{JsonReader, JsonStreamReader};

include!("hospital_mrf/schema_and_output.rs");
include!("hospital_mrf/zip_input.rs");
include!("hospital_mrf/import_and_values.rs");
include!("hospital_mrf/rows_and_metadata.rs");
include!("hospital_mrf/validation_and_json_types.rs");
include!("hospital_mrf/json_and_csv_types.rs");
include!("hospital_mrf/csv_headers.rs");
include!("hospital_mrf/csv_rows.rs");
include!("hospital_mrf/csv_stream.rs");

#[cfg(test)]
mod tests {
    include!("hospital_mrf/tests_support.rs");
    include!("hospital_mrf/tests_import.rs");
    include!("hospital_mrf/tests_validation.rs");
}
