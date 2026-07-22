// Licensed under the HealthPorta Non-Commercial License (see LICENSE).

use super::contracts::{invalid_input, ProviderDirectoryInputFraming};
use super::encode::project_provider_directory_bytes_with_read_time;
use super::wire::read_bounded_input;
use std::io::{self, BufWriter, Write};
use std::time::Instant;

pub fn run_provider_directory_projection_stdio_cli(arguments: &[String]) -> io::Result<()> {
    if arguments.len() != 1 {
        return Err(invalid_input(
            "usage: ptg2_scanner --provider-directory-project-stdio <ndjson|bundle>",
        ));
    }
    let framing = ProviderDirectoryInputFraming::parse(&arguments[0])?;
    let read_started = Instant::now();
    let input = read_bounded_input(io::stdin().lock())?;
    let input_read_seconds = read_started.elapsed().as_secs_f64();
    let spool =
        project_provider_directory_bytes_with_read_time(&input, framing, input_read_seconds)?;
    let mut stdout = BufWriter::new(io::stdout().lock());
    stdout.write_all(&spool.bytes)?;
    stdout.flush()
}
