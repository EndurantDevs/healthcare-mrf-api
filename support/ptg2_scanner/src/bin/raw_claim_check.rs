use flate2::read::MultiGzDecoder;
use serde_json::Value;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::env;
use std::fs::File;
use std::io::{self, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::time::Instant;
use struson::reader::{JsonReader, JsonStreamReader};

include!("raw_claim_check/targets.rs");
include!("raw_claim_check/scan_and_report.rs");
