//! Observe-only exact rate-schedule reuse counters.
//!
//! Assigned serving rows are already ordered by
//! `(code_key, provider_set_key, price_set_key, source_key)`.  Therefore each
//! provider set sees its own canonical tuple stream in sorted order.  Keeping
//! one bounded accumulator per charged provider set lets us measure exact
//! snapshot-local schedule reuse without another occurrence sort or scratch
//! file.  These counters do not select or publish a serving representation.

use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::io;
use std::mem::size_of;

include!("rate_schedule_observe/types.rs");
include!("rate_schedule_observe/observer.rs");
include!("rate_schedule_observe/tests.rs");
