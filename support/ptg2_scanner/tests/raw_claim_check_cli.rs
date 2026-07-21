use flate2::write::GzEncoder;
use flate2::Compression;
use std::fs;
use std::io::Write;
use std::path::Path;
use std::process::{Command, Output};

const TARGETS: &str = concat!(
    "npi,code,code_system,pos,api_status\n",
    "1234567890,99213,CPT,11,api_found\n",
    "2345678901,J1234,HCPCS,22,api_found\n",
    "3456789012,00000,CPT,,api_missing\n",
);

const RAW_MRF: &[u8] = br#"{
  "reporting_entity_name": "Coverage Fixture",
  "provider_references": [
    {
      "provider_group_id": 7,
      "provider_groups": [{"npi": [1234567890, "2345678901", true, null]}]
    },
    {
      "provider_group_id": "8",
      "provider_groups": [{"npi": "1234567890"}]
    },
    {
      "provider_group_id": "not-an-integer",
      "provider_groups": [{"npi": [3456789012]}]
    }
  ],
  "in_network": [
    {
      "billing_code": "99213",
      "billing_code_type": "cpt",
      "negotiated_rates": [
        {
          "provider_references": [7, 9999],
          "provider_groups": [{"npi": [2345678901]}],
          "negotiated_prices": [
            {"service_code": ["11", "2"], "negotiated_rate": 100.5},
            {"service_code": "3", "negotiated_rate": "101.00"}
          ]
        }
      ]
    },
    {
      "billing_code": "J1234",
      "billing_code_type": "HCPCS",
      "negotiated_rates": [
        {
          "provider_groups": [{"npi": "2345678901"}],
          "negotiated_prices": [
            {"service_code": "22", "negotiated_rate": 200}
          ]
        }
      ]
    },
    {
      "billing_code": "NOT-TARGETED",
      "billing_code_type": "CPT",
      "negotiated_rates": [{"ignored": true}]
    }
  ],
  "trailer": true
}"#;

fn run_raw_claim_check(targets: &Path, raw_paths: &[&Path]) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_raw_claim_check"));
    command.arg(targets);
    for raw_path in raw_paths {
        command.arg(raw_path);
    }
    command.output().expect("run raw_claim_check")
}

fn assert_success_report(output: Output, raw_path: &Path) {
    assert!(
        output.status.success(),
        "raw_claim_check failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("UTF-8 report");
    let rows: Vec<&str> = stdout.lines().collect();
    assert_eq!(rows.len(), 7, "header plus two rows per target");
    assert!(rows[0].starts_with("npi,code,requested_code_system,pos,api_status,"));

    let referenced = rows
        .iter()
        .find(|row| row.starts_with("1234567890,99213,CPT,11,api_found,CPT,false,"))
        .expect("requested CPT evidence row");
    assert!(referenced.contains(",true,true,true,1,1,7|8,0,02|03|11,100.5|101.00,"));
    assert!(referenced.ends_with(raw_path.to_string_lossy().as_ref()));

    let referenced_alternate = rows
        .iter()
        .find(|row| row.starts_with("1234567890,99213,CPT,11,api_found,HCPCS,true,"))
        .expect("alternate HCPCS evidence row");
    assert!(referenced_alternate.contains(",true,false,false,0,0,7|8,0,,,"));

    let inline = rows
        .iter()
        .find(|row| row.starts_with("2345678901,J1234,HCPCS,22,api_found,HCPCS,false,"))
        .expect("requested HCPCS evidence row");
    assert!(inline.contains(",true,true,true,1,1,7,1,22,200,"));

    let unmatched = rows
        .iter()
        .find(|row| row.starts_with("3456789012,00000,CPT,,api_missing,CPT,false,"))
        .expect("unmatched evidence row");
    assert!(unmatched.contains(",false,false,false,0,0,,0,,,"));
}

#[test]
fn raw_claim_check_scans_plain_and_gzip_provider_memberships() {
    let directory = tempfile::tempdir().expect("temporary fixture root");
    let targets = directory.path().join("targets.csv");
    let plain = directory.path().join("raw.json");
    let gzip = directory.path().join("raw.json.gz");
    fs::write(&targets, TARGETS).expect("write target fixture");
    fs::write(&plain, RAW_MRF).expect("write plain raw fixture");
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(RAW_MRF).expect("encode gzip fixture");
    fs::write(&gzip, encoder.finish().expect("finish gzip fixture"))
        .expect("write gzip raw fixture");

    assert_success_report(run_raw_claim_check(&targets, &[&plain]), &plain);
    assert_success_report(run_raw_claim_check(&targets, &[&gzip]), &gzip);
}

#[test]
fn raw_claim_check_rejects_incomplete_arguments_and_malformed_inputs() {
    let binary = env!("CARGO_BIN_EXE_raw_claim_check");
    let no_arguments = Command::new(binary)
        .output()
        .expect("run without arguments");
    assert!(!no_arguments.status.success());
    assert!(String::from_utf8_lossy(&no_arguments.stderr).contains("usage:"));

    let directory = tempfile::tempdir().expect("temporary failure fixture root");
    let targets = directory.path().join("targets.csv");
    let malformed = directory.path().join("malformed.json");
    fs::write(&targets, TARGETS).expect("write target fixture");
    fs::write(&malformed, b"{\"provider_references\":[").expect("write malformed JSON fixture");

    let no_raw_files = run_raw_claim_check(&targets, &[]);
    assert!(!no_raw_files.status.success());
    assert!(String::from_utf8_lossy(&no_raw_files.stderr)
        .contains("at least one raw file path is required"));

    let malformed_json = run_raw_claim_check(&targets, &[&malformed]);
    assert!(!malformed_json.status.success());
    assert!(
        !String::from_utf8_lossy(&malformed_json.stderr)
            .trim()
            .is_empty(),
        "malformed JSON must emit a diagnostic"
    );

    let bad_targets = directory.path().join("bad-targets.csv");
    fs::write(&bad_targets, "npi,code\nnot-an-npi,99213\n").expect("write invalid targets");
    let invalid_targets = run_raw_claim_check(&bad_targets, &[&malformed]);
    assert!(!invalid_targets.status.success());
    assert!(String::from_utf8_lossy(&invalid_targets.stderr)
        .contains("target CSV is missing column code_system"));
}
