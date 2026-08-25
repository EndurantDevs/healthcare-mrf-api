use ptg2_scanner::tax_identity::TaxIdentityStateV2;
use ptg2_scanner::tax_identity_sidecar_v2::TaxIdentitySidecarV2StreamValidator;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

const RAW_MRF: &[u8] = include_bytes!("fixtures/compact_v4_mrf.json");
include!("compact_v4_cli/helpers.rs");
include!("compact_v4_cli/sidecar_tests.rs");
include!("compact_v4_cli/failure_and_factor_tests.rs");
