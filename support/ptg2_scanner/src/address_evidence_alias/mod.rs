//! Native derivation for reviewed evidence-gated address aliases.

use crate::address_canon::{
    address_evidence_features, address_evidence_route_marker,
    address_evidence_token_is_directional, address_evidence_token_is_suffix,
    address_evidence_unit_parts, address_evidence_unit_prefix, canonicalize_address,
    decode_copy_field, AddressEvidenceFeatures, CanonicalAddress,
};
use crate::copy_format::{pg_text_copy_field, write_copy_fields};
use crate::npi_identifier::{npi_validity, NpiValidity};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{self, BufRead, BufReader, BufWriter, Read, Write};
use std::path::Path;
use std::time::Instant;

pub const ADDRESS_EVIDENCE_ALIAS_NATIVE_CONTRACT: &str = "address_evidence_alias_native_v1";

include!("types.rs");
include!("matching.rs");
include!("pipeline.rs");
include!("runner.rs");

#[cfg(test)]
mod tests;
