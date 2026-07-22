// Licensed under the HealthPorta Non-Commercial License (see LICENSE).

use super::contracts::{invalid_input, is_sha256, ProjectionCopyContext};
use super::encode::project_provider_directory_copy;
use crate::provider_directory_projection::contracts::ProviderDirectoryInputFraming;
use crate::provider_directory_projection::wire::read_bounded_input;
use std::io::{self, BufWriter, Write};
use std::time::Instant;

const USAGE: &str = "usage: ptg2_scanner --provider-directory-materialize-stdio-v2 \
<recipe_id> <partition_id> <partition_ordinal> <ndjson|bundle>";

pub fn run_provider_directory_materialization_stdio_v2_cli(arguments: &[String]) -> io::Result<()> {
    let (context, framing) = parse_arguments(arguments)?;
    let read_started = Instant::now();
    let input = read_bounded_input(io::stdin().lock())?;
    let input_read_seconds = read_started.elapsed().as_secs_f64();
    let spool = project_provider_directory_copy(&input, framing, &context, input_read_seconds)?;
    let mut stdout = BufWriter::new(io::stdout().lock());
    stdout.write_all(&spool.header_bytes)?;
    stdout.write_all(&spool.copy_bytes)?;
    stdout.write_all(&[0xff])?;
    stdout.flush()
}

fn parse_arguments(
    arguments: &[String],
) -> io::Result<(ProjectionCopyContext, ProviderDirectoryInputFraming)> {
    if arguments.len() != 4 || !is_sha256(&arguments[0]) || !is_sha256(&arguments[1]) {
        return Err(invalid_input(USAGE));
    }
    let partition_ordinal = arguments[2]
        .parse::<u32>()
        .ok()
        .filter(|ordinal| *ordinal <= i32::MAX as u32)
        .ok_or_else(|| invalid_input(USAGE))?;
    let framing =
        ProviderDirectoryInputFraming::parse(&arguments[3]).map_err(|_| invalid_input(USAGE))?;
    Ok((
        ProjectionCopyContext {
            recipe_id: arguments[0].clone(),
            partition_id: arguments[1].clone(),
            partition_ordinal,
        },
        framing,
    ))
}

#[cfg(test)]
pub(super) fn test_parse_arguments(
    arguments: &[String],
) -> io::Result<(ProjectionCopyContext, ProviderDirectoryInputFraming)> {
    parse_arguments(arguments)
}
