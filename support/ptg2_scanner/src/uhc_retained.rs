//! Strict, immutable admission of retained UHC JSON arrays as logical byte ranges.
//!
//! The native helper deliberately does not acquire the higher-level retained-source
//! lock. The Python admission transaction owns that lock across this subprocess and
//! its database commit. Atomic no-clobber publication still makes this layer safe
//! when it is invoked concurrently without the outer lock.

include!("uhc_retained_parts/core.rs");
include!("uhc_retained_parts/filesystem.rs");
include!("uhc_retained_parts/json_framing.rs");
include!("uhc_retained_parts/range_build.rs");
include!("uhc_retained_parts/verification.rs");
include!("uhc_retained_parts/publication.rs");
include!("uhc_retained_parts/retain.rs");
include!("uhc_retained_parts/tests.rs");
