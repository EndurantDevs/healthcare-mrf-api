mod support;

use std::fs;
use std::process::{Command, Output, Stdio};
use support::write_stdin_and_wait;

fn scanner() -> Command {
    Command::new(env!("CARGO_BIN_EXE_ptg2_scanner"))
}

fn postgres_copy(rows: &[Vec<Option<Vec<u8>>>]) -> Vec<u8> {
    let mut payload = b"PGCOPY\n\xff\r\n\0".to_vec();
    payload.extend_from_slice(&0i32.to_be_bytes());
    payload.extend_from_slice(&0i32.to_be_bytes());
    for row in rows {
        payload.extend_from_slice(&(row.len() as i16).to_be_bytes());
        for field in row {
            match field {
                Some(field) => {
                    payload.extend_from_slice(&(field.len() as i32).to_be_bytes());
                    payload.extend_from_slice(field);
                }
                None => payload.extend_from_slice(&(-1i32).to_be_bytes()),
            }
        }
    }
    payload.extend_from_slice(&(-1i16).to_be_bytes());
    payload
}

fn run_scanner_with_stdin(arguments: &[&str], stdin: &[u8]) -> Output {
    let child = scanner()
        .args(arguments)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    write_stdin_and_wait(child, stdin)
}

fn pg_i32(value: i32) -> Option<Vec<u8>> {
    Some(value.to_be_bytes().to_vec())
}

fn pg_numeric(weight: i16, scale: i16, digits: &[u16]) -> Vec<u8> {
    let mut payload = Vec::with_capacity(8 + digits.len() * 2);
    payload.extend_from_slice(&(digits.len() as i16).to_be_bytes());
    payload.extend_from_slice(&weight.to_be_bytes());
    payload.extend_from_slice(&0u16.to_be_bytes());
    payload.extend_from_slice(&scale.to_be_bytes());
    for digit in digits {
        payload.extend_from_slice(&digit.to_be_bytes());
    }
    payload
}

#[test]
fn selected_array_scan_preserves_nested_objects_and_reports_progress() {
    let temporary = tempfile::tempdir().unwrap();
    let input = temporary.path().join("selected-arrays.json");
    fs::write(
        &input,
        br#"{
            "ignored": [{"not": "emitted"}],
            "provider_references": [
                {"provider_group_id": 7, "provider_groups": [{"npi": [1234567890]}]},
                {"provider_group_id": 8, "nested": {"closing": "} ] \\\""}}
            ],
            "in_network": [
                {"billing_code_type": "CPT", "billing_code": "70553", "nested": [{"v": 1}]}
            ]
        }"#,
    )
    .unwrap();

    let completed = scanner()
        .arg(&input)
        .arg("provider_references")
        .arg("in_network")
        .env("HLTHPRT_PTG2_SCANNER_PROGRESS_OBJECTS", "1")
        .env("HLTHPRT_PTG2_SCANNER_PROGRESS_BYTES", "1")
        .output()
        .unwrap();
    assert!(
        completed.status.success(),
        "{}",
        String::from_utf8_lossy(&completed.stderr)
    );
    let stdout = String::from_utf8(completed.stdout).unwrap();
    assert_eq!(stdout.matches("provider_references\t").count(), 2);
    assert_eq!(stdout.matches("in_network\t").count(), 1);
    assert!(!stdout.contains("not\":\"emitted"));
    assert!(stdout.contains("\"70553\""));
    let stderr = String::from_utf8(completed.stderr).unwrap();
    assert!(stderr.contains("PTG2_SCANNER_PROGRESS"));
    assert!(stderr.contains("done=true"));
}

#[test]
fn selected_array_scan_cli_rejects_incomplete_requests() {
    let no_arguments = scanner().output().unwrap();
    assert!(!no_arguments.status.success());
    assert!(String::from_utf8_lossy(&no_arguments.stderr).contains("usage: ptg2_scanner"));

    let temporary = tempfile::tempdir().unwrap();
    let input = temporary.path().join("input.json");
    fs::write(&input, b"{}").unwrap();
    let no_array = scanner().arg(&input).output().unwrap();
    assert!(!no_array.status.success());
    assert!(String::from_utf8_lossy(&no_array.stderr)
        .contains("at least one top-level array name is required"));

    let missing_file = scanner()
        .arg(temporary.path().join("missing.json"))
        .arg("in_network")
        .output()
        .unwrap();
    assert!(!missing_file.status.success());
    assert!(String::from_utf8_lossy(&missing_file.stderr).contains("NotFound"));
}

#[test]
fn v4_provider_membership_cli_emits_exact_bidirectional_sidecars() {
    let temporary = tempfile::tempdir().unwrap();
    let input = temporary.path().join("provider-members.copy");
    let first_group = "01010101010101010101010101010101";
    let second_group = "02020202020202020202020202020202";
    fs::write(
        &input,
        format!(
            "{first_group}\t1234567890\n\
             {first_group}\t1234567890\n\
             {second_group}\t2222222222\n"
        ),
    )
    .unwrap();
    let group_npi = temporary.path().join("group-npi.sidecar");
    let npi_group = temporary.path().join("npi-group.sidecar");
    let npi_scope = temporary.path().join("npi-scope.copy");
    let completed = scanner()
        .arg("--provider-membership-sidecars")
        .arg(&group_npi)
        .arg(&npi_group)
        .arg(&npi_scope)
        .arg(&input)
        .output()
        .unwrap();
    assert!(
        completed.status.success(),
        "{}",
        String::from_utf8_lossy(&completed.stderr)
    );
    assert_eq!(&fs::read(&group_npi).unwrap()[..8], b"PTG2MNDS");
    assert_eq!(&fs::read(&npi_group).unwrap()[..8], b"PTG2MNDS");
    assert_eq!(
        fs::read_to_string(&npi_scope).unwrap(),
        "1234567890\n2222222222\n"
    );
    assert!(String::from_utf8_lossy(&completed.stdout).contains("membership_count\":2"));

    for missing_count in 0..3 {
        let mut command = scanner();
        command.arg("--provider-membership-sidecars");
        for argument in [&group_npi, &npi_group, &npi_scope]
            .into_iter()
            .take(missing_count)
        {
            command.arg(argument);
        }
        let rejected = command.output().unwrap();
        assert!(!rejected.status.success());
        assert!(String::from_utf8_lossy(&rejected.stderr)
            .contains("usage: ptg2_scanner --provider-membership-sidecars"));
    }
}

#[test]
fn strict_price_membership_cli_transcodes_postgres_copy() {
    let input = postgres_copy(&[
        vec![pg_i32(0), pg_i32(3)],
        vec![pg_i32(0), pg_i32(9)],
        vec![pg_i32(1), pg_i32(12)],
    ]);
    let completed = run_scanner_with_stdin(
        &[
            "--serving-binary-copy-from-key-copy-stdio",
            "price_set_atom_memberships_v3",
            "24",
        ],
        &input,
    );

    assert!(
        completed.status.success(),
        "{}",
        String::from_utf8_lossy(&completed.stderr)
    );
    assert_eq!(&completed.stdout[..11], b"PGCOPY\n\xff\r\n\0");
    let summary = String::from_utf8(completed.stderr).unwrap();
    assert!(summary.contains("PTG2_SERVING_BINARY_COPY"));
    assert!(summary.contains("\"price_set_count\":2"));
    assert!(summary.contains("\"atom_reference_count\":3"));
}

#[test]
fn strict_price_atom_cli_preserves_numeric_and_optional_attributes() {
    let mut first = vec![pg_i32(0), Some(pg_numeric(0, 2, &[15, 2500]))];
    first.extend(
        [Some(0), None, Some(2), None, None, Some(5), None].map(|value| value.and_then(pg_i32)),
    );
    let mut second = vec![pg_i32(1), Some(b"20.50".to_vec())];
    second.extend(vec![None; 7]);
    let completed = run_scanner_with_stdin(
        &[
            "--serving-binary-copy-from-key-copy-stdio",
            "price_atoms_v3",
            "24",
        ],
        &postgres_copy(&[first, second]),
    );

    assert!(
        completed.status.success(),
        "{}",
        String::from_utf8_lossy(&completed.stderr)
    );
    assert_eq!(&completed.stdout[..11], b"PGCOPY\n\xff\r\n\0");
    let summary = String::from_utf8(completed.stderr).unwrap();
    assert!(summary.contains("\"atom_count\":2"));
    assert!(summary.contains("\"attribute_count\":7"));
    assert!(summary.contains("\"source_copy_format\":\"postgres_binary\""));
}

#[test]
fn strict_price_copy_cli_rejects_unknown_kinds_and_widths() {
    for arguments in [
        vec!["--serving-binary-copy-from-key-copy-stdio", "unknown", "24"],
        vec![
            "--serving-binary-copy-from-key-copy-stdio",
            "price_atoms_v3",
            "16",
        ],
    ] {
        let completed = run_scanner_with_stdin(&arguments, &postgres_copy(&[]));
        assert!(!completed.status.success());
        let error = String::from_utf8_lossy(&completed.stderr);
        assert!(
            error.contains("serving-binary-copy-from-key-copy-stdio") || error.contains("PTG2 v3")
        );
    }
}

#[test]
fn manifest_merge_cli_prefers_complete_rows_and_deduplicates_pairs() {
    let temporary = tempfile::tempdir().unwrap();
    let first = temporary.path().join("first.copy");
    let second = temporary.path().join("second.copy");
    let output = temporary.path().join("merged.copy");
    fs::write(
        &first,
        b"b\t2\nmanifest\t1\t2\t3\t4\t5\t6\t7\t\\N\nmanifest\t1\t2\t3\t4\t5\t6\t7\t\\N\n",
    )
    .unwrap();
    fs::write(&second, b"a\t1\nmanifest\t1\t2\t3\t4\t5\t6\t7\ttrace\n").unwrap();

    let completed = scanner()
        .arg("--merge-manifest-copy")
        .arg("manifest_serving")
        .arg(&output)
        .arg(&first)
        .arg(&second)
        .output()
        .unwrap();
    assert!(
        completed.status.success(),
        "{}",
        String::from_utf8_lossy(&completed.stderr)
    );
    assert_eq!(
        fs::read_to_string(&output).unwrap(),
        "a\t1\nb\t2\nmanifest\t1\t2\t3\t4\t5\t6\t7\ttrace\n"
    );
    let summary = String::from_utf8(completed.stdout).unwrap();
    assert!(summary.contains("\"input_rows\":5"));
    assert!(summary.contains("\"output_rows\":3"));
}

#[test]
fn manifest_merge_cli_exercises_bounded_parallel_chunk_sort() {
    let temporary = tempfile::tempdir().unwrap();
    let input = temporary.path().join("members.copy");
    let output = temporary.path().join("members-merged.copy");
    let large_member = "x".repeat(1_100_000);
    fs::write(
        &input,
        format!("g2\t200\t{large_member}\ng1\t100\t{large_member}\ng2\t200\t{large_member}\n"),
    )
    .unwrap();

    let completed = scanner()
        .arg("--merge-manifest-copy")
        .arg("provider_group_member")
        .arg(&output)
        .arg(&input)
        .env("HLTHPRT_PTG2_MANIFEST_MERGE_DIR", temporary.path())
        .env("HLTHPRT_PTG2_MANIFEST_MERGE_CHUNK_BYTES", "1")
        .env("HLTHPRT_PTG2_MANIFEST_MERGE_SORT_WORKERS", "1")
        .output()
        .unwrap();
    assert!(
        completed.status.success(),
        "{}",
        String::from_utf8_lossy(&completed.stderr)
    );
    let merged = fs::read_to_string(&output).unwrap();
    assert_eq!(merged.lines().count(), 2);
    assert!(merged.starts_with("g1\t100\t"));
    assert!(String::from_utf8_lossy(&completed.stdout).contains("\"chunk_count\":3"));
}

#[test]
fn canonical_address_and_version_cli_emit_production_contracts() {
    let temporary = tempfile::tempdir().unwrap();
    let input = temporary.path().join("addresses.copy");
    let output = temporary.path().join("canonical.copy");
    fs::write(
        &input,
        "1\t(0,1)\t\\N\t27 Dr Mellichamp Dr Ste 100\t\\N\tBLUFFTON\tSC\t29910\tUS\n",
    )
    .unwrap();

    let canonicalized = scanner()
        .arg("--address-canonicalize-copy")
        .arg(&input)
        .arg(&output)
        .output()
        .unwrap();
    assert!(
        canonicalized.status.success(),
        "{}",
        String::from_utf8_lossy(&canonicalized.stderr)
    );
    let row = fs::read_to_string(&output).unwrap();
    assert!(row.contains("3e3ea29f-8c26-17ba-dcc8-74424e66fd32"));
    assert_eq!(row.split('\t').count(), 19);

    let version = scanner().arg("--canon-version").output().unwrap();
    assert!(version.status.success());
    let payload: serde_json::Value = serde_json::from_slice(&version.stdout).unwrap();
    assert_eq!(payload["identity_version"], 2);
    assert_eq!(payload["pub28_sha256"].as_str().unwrap().len(), 64);
}

#[test]
fn finalizer_and_merge_cli_reject_incomplete_requests() {
    for arguments in [
        vec!["--finalize-v3-runs"],
        vec!["--merge-manifest-copy"],
        vec!["--merge-manifest-copy", "not-a-kind"],
    ] {
        let completed = scanner().args(arguments).output().unwrap();
        assert!(!completed.status.success());
        let error = String::from_utf8_lossy(&completed.stderr);
        assert!(error.contains("usage") || error.contains("merge kind"));
    }
}

#[test]
fn scanner_mode_dispatch_rejects_every_missing_or_extra_coordinate() {
    for arguments in [
        vec!["--canon-version", "extra"],
        vec!["--compact-serving"],
        vec!["--serving-binary-copy-from-key-copy-stdio"],
        vec!["--merge-manifest-copy", "manifest_serving"],
        vec!["--provider-membership-sidecars"],
        vec!["--provider-membership-sidecars", "group"],
        vec!["--provider-membership-sidecars", "group", "npi"],
        vec!["--address-canonicalize-copy"],
        vec!["--address-canonicalize-copy", "input"],
        vec!["--address-canonicalize-copy", "input", "output", "extra"],
    ] {
        let completed = scanner().args(&arguments).output().unwrap();
        assert!(
            !completed.status.success(),
            "accepted invalid scanner coordinates: {arguments:?}",
        );
        assert!(String::from_utf8_lossy(&completed.stderr).contains("usage"));
    }
}
