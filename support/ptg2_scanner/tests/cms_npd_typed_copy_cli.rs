// Licensed under the HealthPorta Non-Commercial License (see LICENSE).

mod support;

use std::process::{Command, Output, Stdio};
use support::write_stdin_and_wait;

const HEADER: &[u8] = b"PGCOPY\n\xff\r\n\0\0\0\0\0\0\0\0\0";
const TRAILER: &[u8] = &[0xff, 0xff];

fn run(arguments: &[&str], input: &[u8]) -> Output {
    let child = Command::new(env!("CARGO_BIN_EXE_ptg2_scanner"))
        .arg("--cms-npd-typed-copy")
        .args(arguments)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    write_stdin_and_wait(child, input)
}

#[test]
fn cms_npd_typed_copy_cli_writes_main_and_address_copy_files() {
    let directory = tempfile::tempdir().unwrap();
    let main = directory.path().join("practitioner.copy");
    let address = directory.path().join("practitioner_address.copy");
    let input = br#"{"resourceType":"Practitioner","id":"Practitioner-1234567890","meta":{"lastUpdated":"2026-01-01T00:00:00Z"},"address":[{"city":"Exampleville"},{"city":"Sampletown"}]}
{"resourceType":"Practitioner","id":"Practitioner-0987654321","meta":{"lastUpdated":"2026-01-01T00:00:00.5Z"}}
"#;
    let output = run(
        &[
            "Practitioner",
            main.to_str().unwrap(),
            address.to_str().unwrap(),
        ],
        input,
    );

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let summary = String::from_utf8(output.stdout).unwrap();
    assert_eq!(
        summary,
        format!(
            "{{\"resource_type\":\"Practitioner\",\"rows\":2,\"address_rows\":2,\"input_bytes\":{}}}\n",
            input.len()
        )
    );
    for path in [&main, &address] {
        let bytes = std::fs::read(path).unwrap();
        assert!(bytes.starts_with(HEADER) && bytes.ends_with(TRAILER));
    }
    // Each practitioner tuple starts with its 24-field count and the 8-byte NPI.
    let bytes = std::fs::read(&main).unwrap();
    let first_row = &bytes[HEADER.len()..];
    assert_eq!(&first_row[..6], &[0, 24, 0, 0, 0, 8]);
    assert_eq!(
        i64::from_be_bytes(first_row[6..14].try_into().unwrap()),
        1_234_567_890
    );
}

#[test]
fn cms_npd_typed_copy_cli_fails_closed_with_the_line_number() {
    let directory = tempfile::tempdir().unwrap();
    let main = directory.path().join("location.copy");
    let input = br#"{"resourceType":"Location","id":"Location-11111111-1111-4111-8111-111111111111","meta":{"lastUpdated":"2026-01-01T00:00:00Z"}}
{"resourceType":"Location","id":"Location-not-a-uuid","meta":{"lastUpdated":"2026-01-01T00:00:00Z"}}
"#;
    let output = run(&["Location", main.to_str().unwrap()], input);

    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("line 2: invalid uuid"), "{stderr}");
    assert!(!std::fs::read(&main).unwrap().ends_with(TRAILER));

    let usage = run(&["Organization", main.to_str().unwrap()], b"");
    assert!(!usage.status.success());
    assert!(
        String::from_utf8_lossy(&usage.stderr).contains("needs exactly one address output path")
    );
}
