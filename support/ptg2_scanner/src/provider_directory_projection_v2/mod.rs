// Licensed under the HealthPorta Non-Commercial License (see LICENSE).

//! Native semantic projection and PostgreSQL COPY framing for retained FHIR.

mod canonical;
mod contracts;
mod encode;
mod fhir_values;
mod pg_copy;
mod references;
mod semantics;
mod stdio;

pub use contracts::{
    ProviderDirectoryProjectionCopySpool, ProviderDirectoryProjectionCopySummary,
    PROVIDER_DIRECTORY_PROJECTION_COPY_MAGIC,
    PROVIDER_DIRECTORY_PROJECTION_COPY_MAX_OWNED_IO_BYTES,
};
pub use encode::decode_provider_directory_projection_copy_spool;
pub use stdio::run_provider_directory_materialization_stdio_v2_cli;

#[cfg(test)]
mod tests;
