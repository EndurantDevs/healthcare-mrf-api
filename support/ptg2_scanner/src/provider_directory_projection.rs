// Licensed under the HealthPorta Non-Commercial License (see LICENSE).

//! Offline, source-neutral framing for retained Provider Directory FHIR bytes.
//!
//! The stdin CLI deliberately stops before database or publication concerns. It
//! accepts one bounded retained block, validates strict FHIR resources, and
//! emits a length-framed spool whose records can be translated to PostgreSQL
//! binary COPY by the Python projection owner. No path, URL, credential, source
//! identity, or database coordinate crosses this boundary.

#[path = "provider_directory_projection_parts/contracts.rs"]
mod contracts;
#[path = "provider_directory_projection_parts/decode.rs"]
mod decode;
#[path = "provider_directory_projection_parts/encode.rs"]
mod encode;
#[path = "provider_directory_projection_parts/stdio.rs"]
mod stdio;
#[path = "provider_directory_projection_parts/strict_json.rs"]
mod strict_json;
#[path = "provider_directory_projection_v2/mod.rs"]
mod v2;
#[path = "provider_directory_projection_parts/validation.rs"]
mod validation;
#[path = "provider_directory_projection_parts/wire.rs"]
mod wire;

pub use contracts::{
    ProviderDirectoryInputFraming, ProviderDirectoryProjectionRecord,
    ProviderDirectoryProjectionSpool, ProviderDirectoryProjectionSummary,
    ProviderDirectoryProjectionTimings, PROVIDER_DIRECTORY_PROJECTION_DECODER_CONTRACT_ID,
    PROVIDER_DIRECTORY_PROJECTION_MAX_INPUT_BYTES,
    PROVIDER_DIRECTORY_PROJECTION_MAX_OWNED_IO_BYTES,
    PROVIDER_DIRECTORY_PROJECTION_MAX_RESOURCE_BYTES,
    PROVIDER_DIRECTORY_PROJECTION_MAX_RESOURCE_COUNT,
    PROVIDER_DIRECTORY_PROJECTION_MAX_SPOOL_BYTES,
    PROVIDER_DIRECTORY_PROJECTION_RECORD_CONTRACT_ID,
    PROVIDER_DIRECTORY_PROJECTION_SPOOL_CONTRACT_ID, PROVIDER_DIRECTORY_PROJECTION_SPOOL_MAGIC,
    PROVIDER_DIRECTORY_SUPPORTED_RESOURCE_TYPES,
};
pub use decode::decode_provider_directory_projection_spool;
pub use encode::project_provider_directory_bytes;
pub use stdio::run_provider_directory_projection_stdio_cli;
pub use v2::{
    decode_provider_directory_projection_copy_spool,
    run_provider_directory_materialization_stdio_v2_cli, ProviderDirectoryProjectionCopySpool,
    ProviderDirectoryProjectionCopySummary, PROVIDER_DIRECTORY_PROJECTION_COPY_MAGIC,
    PROVIDER_DIRECTORY_PROJECTION_COPY_MAX_OWNED_IO_BYTES,
};

#[cfg(test)]
#[path = "provider_directory_projection_parts/tests.rs"]
mod tests;
