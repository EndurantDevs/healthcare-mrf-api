// Licensed under the HealthPorta Non-Commercial License (see LICENSE).

use ptg2_scanner::provider_directory_projection::{
    decode_provider_directory_projection_copy_spool, PROVIDER_DIRECTORY_PROJECTION_COPY_MAGIC,
};
use std::io::Write;
use std::process::{Command, Output, Stdio};

const RECIPE_ID: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const PARTITION_ID: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

fn run_materialization(arguments: &[&str], input: &[u8]) -> Output {
    let mut child = Command::new(env!("CARGO_BIN_EXE_ptg2_scanner"))
        .arg("--provider-directory-materialize-stdio-v2")
        .args(arguments)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child.stdin.take().unwrap().write_all(input).unwrap();
    child.wait_with_output().unwrap()
}

#[test]
fn v2_stdio_cli_emits_verified_postgresql_copy_without_diagnostics() {
    let input = br#"{"resourceType":"Organization","id":"org-2","active":true}
{"resourceType":"Practitioner","id":"npi-1","active":false}
"#;
    let output = run_materialization(&[RECIPE_ID, PARTITION_ID, "17", "ndjson"], input);

    assert!(output.status.success(), "{:?}", output.stderr);
    assert!(output.stderr.is_empty());
    let spool = decode_provider_directory_projection_copy_spool(&output.stdout).unwrap();
    assert_eq!(spool.summary.resource_count, 2);
    assert_eq!(spool.summary.partition_ordinal, 17);
    assert_eq!(spool.summary.recipe_id, RECIPE_ID);
    assert!(spool.copy_bytes.starts_with(b"PGCOPY\n\xff\r\n\0"));
}

#[test]
fn v2_stdio_cli_accepts_a_searchset_bundle() {
    let input = br#"{"entry":[{"resource":{"active":true,"id":"org-1","resourceType":"Organization"}}],"resourceType":"Bundle","type":"searchset"}"#;
    let output = run_materialization(&[RECIPE_ID, PARTITION_ID, "0", "bundle"], input);

    assert!(output.status.success(), "{:?}", output.stderr);
    let spool = decode_provider_directory_projection_copy_spool(&output.stdout).unwrap();
    assert_eq!(spool.summary.input_framing, "bundle");
    assert_eq!(spool.summary.resource_count, 1);
}

#[test]
fn v2_stdio_cli_fails_closed_before_magic_for_bad_coordinates_or_semantics() {
    let invalid_coordinate = run_materialization(
        &["not-a-hash", PARTITION_ID, "0", "ndjson"],
        br#"{"resourceType":"Organization","id":"org-1"}
"#,
    );
    assert!(!invalid_coordinate.status.success());
    assert!(!invalid_coordinate
        .stdout
        .starts_with(PROVIDER_DIRECTORY_PROJECTION_COPY_MAGIC));

    let invalid_resource = run_materialization(
        &[RECIPE_ID, PARTITION_ID, "0", "ndjson"],
        br#"{"resourceType":"Organization","id":"org-1","active":"yes"}
"#,
    );
    assert!(!invalid_resource.status.success());
    assert!(!invalid_resource
        .stdout
        .starts_with(PROVIDER_DIRECTORY_PROJECTION_COPY_MAGIC));
}
