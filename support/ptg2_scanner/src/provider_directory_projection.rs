// Licensed under the HealthPorta Non-Commercial License (see LICENSE).

//! Offline, source-neutral materialization of retained Provider Directory FHIR.
//!
//! The stdin CLI deliberately stops before database or publication concerns. It
//! accepts one bounded retained block, validates strict FHIR resources, and
//! emits an exact PostgreSQL binary COPY spool. No path, URL, credential,
//! source identity, or database coordinate crosses this boundary.

#[path = "provider_directory_projection_v2/mod.rs"]
mod v2;

pub use v2::{
    decode_provider_directory_projection_copy_spool,
    run_provider_directory_materialization_stdio_v2_cli, ProviderDirectoryProjectionCopySpool,
    ProviderDirectoryProjectionCopySummary, PROVIDER_DIRECTORY_PROJECTION_COPY_MAGIC,
    PROVIDER_DIRECTORY_PROJECTION_COPY_MAX_OWNED_IO_BYTES,
};

/// One stdin mode of the scanner CLI: takes the remaining arguments.
pub type DirectoryCliMode = fn(&[String]) -> std::io::Result<()>;

/// The provider-directory stdin mode selected by the first CLI argument, if it names one.
pub fn provider_directory_cli(mode: &str) -> Option<DirectoryCliMode> {
    match mode {
        "--provider-directory-materialize-stdio-v2" => {
            Some(run_provider_directory_materialization_stdio_v2_cli)
        }
        "--cms-npd-typed-copy" => Some(crate::cms_npd_typed_copy::run_cli),
        _ => None,
    }
}
