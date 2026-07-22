// Licensed under the HealthPorta Non-Commercial License (see LICENSE).

use ptg2_scanner::provider_directory_projection::{
    decode_provider_directory_projection_spool, PROVIDER_DIRECTORY_PROJECTION_SPOOL_MAGIC,
};
use std::io::Write;
use std::process::{Command, Output, Stdio};

fn run_projection(framing: &str, input: &[u8]) -> Output {
    let mut child = Command::new(env!("CARGO_BIN_EXE_ptg2_scanner"))
        .args(["--provider-directory-project-stdio", framing])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child.stdin.take().unwrap().write_all(input).unwrap();
    child.wait_with_output().unwrap()
}

#[test]
fn stdio_cli_emits_a_verified_offline_spool() {
    let input = br#"{"resourceType":"Organization","id":"org-2"}
{"resourceType":"Practitioner","id":"npi-1"}
"#;
    let output = run_projection("ndjson", input);

    assert!(output.status.success(), "{:?}", output.stderr);
    assert!(output.stderr.is_empty());
    let (records, summary) = decode_provider_directory_projection_spool(&output.stdout).unwrap();
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].resource_type, "Organization");
    assert_eq!(records[1].resource_type, "Practitioner");
    assert_eq!(summary.resource_count, 2);
    assert_eq!(summary.input_framing, "ndjson");
}

#[test]
fn stdio_cli_fails_closed_without_a_terminal_spool() {
    let output = run_projection(
        "bundle",
        br#"{"resourceType":"Bundle","entry":[{"resource":{"resourceType":"Patient","id":"p-1"}}]}"#,
    );

    assert!(!output.status.success());
    assert!(!output
        .stdout
        .starts_with(PROVIDER_DIRECTORY_PROJECTION_SPOOL_MAGIC));
    assert!(String::from_utf8_lossy(&output.stderr).contains("unsupported resourceType"));
}
