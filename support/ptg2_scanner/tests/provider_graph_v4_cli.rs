use ptg2_scanner::manifest::{write_dense_member_sidecar, GlobalId128, SidecarEntry};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::fs::{self, File};
use std::io::BufWriter;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

const DENSE_FORMAT: &str = "magic8:uint32_le_version:uint64_le_entry_count:uint64_le_member_global_count:index(owner16:uint64_le_offset:uint32_le_count):member_globals16:members_uint32_le";

fn global(domain: u8, value: u64) -> GlobalId128 {
    let mut result = [0u8; 16];
    result[0] = domain;
    result[8..].copy_from_slice(&value.to_be_bytes());
    GlobalId128(result)
}

fn hex(value: GlobalId128) -> String {
    value.0.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn write_membership(path: &Path, name: &str, shard_id: &str, entries: Vec<SidecarEntry>) -> Value {
    let owner_count = entries.len() as u64;
    let member_count = entries
        .iter()
        .map(|entry| entry.members.len() as u64)
        .sum::<u64>();
    let mut distinct_members = entries
        .iter()
        .flat_map(|entry| entry.members.iter().copied())
        .collect::<Vec<_>>();
    distinct_members.sort_unstable();
    distinct_members.dedup();
    let file = File::create(path).expect("create dense membership");
    let mut writer = BufWriter::new(file);
    write_dense_member_sidecar(&mut writer, &entries).expect("write dense membership");
    drop(writer);
    let bytes = fs::read(path).expect("read dense membership");
    json!({
        "path": path,
        "metadata": {
            "record_format": DENSE_FORMAT,
            "sha256": Sha256::digest(&bytes).iter().map(|byte| format!("{byte:02x}")).collect::<String>(),
            "byte_count": bytes.len(),
            "owner_count": owner_count,
            "member_count": member_count,
            "member_global_count": distinct_members.len(),
            "name": name,
            "source_shard_id": shard_id,
        }
    })
}

fn write_manifest(root: &Path) -> PathBuf {
    let shard_id = "coverage-shard";
    let component = global(2, 1);
    let groups = (1..=64).map(|value| global(3, value)).collect::<Vec<_>>();
    let sets = (1..=16).map(|value| global(1, value)).collect::<Vec<_>>();
    let provider_npi = global(0, 1_234_567_890);

    let set_component = write_membership(
        &root.join("set-component.sidecar"),
        "provider_set_component",
        shard_id,
        sets.iter()
            .copied()
            .map(|owner| SidecarEntry {
                owner,
                members: vec![component],
            })
            .collect(),
    );
    let component_group = write_membership(
        &root.join("component-group.sidecar"),
        "provider_component_group",
        shard_id,
        vec![SidecarEntry {
            owner: component,
            members: groups.clone(),
        }],
    );
    let group_npi = write_membership(
        &root.join("group-npi.sidecar"),
        "provider_group_npi",
        shard_id,
        groups
            .iter()
            .copied()
            .map(|owner| SidecarEntry {
                owner,
                members: vec![provider_npi],
            })
            .collect(),
    );
    let npi_group = write_membership(
        &root.join("npi-group.sidecar"),
        "provider_npi_group",
        shard_id,
        vec![SidecarEntry {
            owner: provider_npi,
            members: groups,
        }],
    );
    let provider_map = root.join("provider-map.copy");
    let map = sets
        .iter()
        .enumerate()
        .map(|(index, provider_set)| format!("{}\t{}\n", hex(*provider_set), index + 1))
        .collect::<String>();
    fs::write(&provider_map, map).expect("write provider-set key map");

    let manifest_path = root.join("manifest.json");
    let manifest = json!({
        "shards": [{
            "shard_id": shard_id,
            "provider_set_component": set_component,
            "provider_component_group": component_group,
            "provider_group_npi": group_npi,
            "provider_npi_group": npi_group,
        }],
        "provider_set_key_map_path": provider_map,
        "output_directory": root.join("compiled"),
        "options": {
            "member_page_bytes": 64,
            "locator_page_bytes": 48,
            "heavy_owner_member_threshold": 8,
            "heavy_bitmap_minimum_savings_bytes": 0,
            "max_estimated_model_bytes": 16777216,
            "max_factor_edges": 4096,
        }
    });
    fs::write(
        &manifest_path,
        serde_json::to_vec_pretty(&manifest).expect("encode manifest"),
    )
    .expect("write manifest");
    manifest_path
}

fn write_direct_manifest(root: &Path) -> PathBuf {
    let shard_id = "direct-shard";
    let sets = [global(1, 1), global(1, 2)];
    let components = [global(2, 1), global(2, 2)];
    let groups = [global(3, 1), global(3, 2)];
    let npis = [global(0, 1_111_111_111), global(0, 2_222_222_222)];
    let set_component = write_membership(
        &root.join("direct-set-component.sidecar"),
        "provider_set_component",
        shard_id,
        sets.iter()
            .copied()
            .zip(components)
            .map(|(owner, member)| SidecarEntry {
                owner,
                members: vec![member],
            })
            .collect(),
    );
    let component_group = write_membership(
        &root.join("direct-component-group.sidecar"),
        "provider_component_group",
        shard_id,
        components
            .into_iter()
            .zip(groups)
            .map(|(owner, member)| SidecarEntry {
                owner,
                members: vec![member],
            })
            .collect(),
    );
    let group_npi = write_membership(
        &root.join("direct-group-npi.sidecar"),
        "provider_group_npi",
        shard_id,
        groups
            .into_iter()
            .zip(npis)
            .map(|(owner, member)| SidecarEntry {
                owner,
                members: vec![member],
            })
            .collect(),
    );
    let npi_group = write_membership(
        &root.join("direct-npi-group.sidecar"),
        "provider_npi_group",
        shard_id,
        npis.into_iter()
            .zip(groups)
            .map(|(owner, member)| SidecarEntry {
                owner,
                members: vec![member],
            })
            .collect(),
    );
    let provider_map = root.join("direct-provider-map.copy");
    fs::write(
        &provider_map,
        sets.iter()
            .enumerate()
            .map(|(index, provider_set)| format!("{}\t{index}\n", hex(*provider_set)))
            .collect::<String>(),
    )
    .expect("write direct provider-set key map");
    let manifest_path = root.join("direct-manifest.json");
    let manifest = json!({
        "shards": [{
            "shard_id": shard_id,
            "provider_set_component": set_component,
            "provider_component_group": component_group,
            "provider_group_npi": group_npi,
            "provider_npi_group": npi_group,
        }],
        "provider_set_key_map_path": provider_map,
        "output_directory": root.join("direct-compiled"),
        "options": {
            "member_page_bytes": 32,
            "locator_page_bytes": 24,
            "heavy_owner_member_threshold": 4096,
            "heavy_bitmap_minimum_savings_bytes": 512,
        }
    });
    fs::write(
        &manifest_path,
        serde_json::to_vec_pretty(&manifest).expect("encode direct manifest"),
    )
    .expect("write direct manifest");
    manifest_path
}

fn run(arguments: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_ptg2_provider_graph_v4"))
        .args(arguments)
        .output()
        .expect("run V4 graph compiler")
}

#[test]
fn compiler_cli_builds_pattern_projection_and_reports_progress() {
    let temporary = tempfile::tempdir().expect("temporary compiler fixture");
    let manifest = write_manifest(temporary.path());
    let completed = run(&[manifest.to_str().expect("UTF-8 manifest path")]);
    assert!(
        completed.status.success(),
        "compiler failed: {}",
        String::from_utf8_lossy(&completed.stderr),
    );
    let summary: Value = serde_json::from_slice(&completed.stdout).expect("compiler summary");
    assert_eq!(
        summary["format"],
        "ptg2_provider_graph_v4_factor_adaptive_v1"
    );
    assert_eq!(summary["selected_layout"], "pattern");
    assert_eq!(summary["observe"]["pattern_count"], 1);
    assert_eq!(summary["observe"]["group_count"], 64);
    assert_eq!(summary["observe"]["provider_set_count"], 16);
    assert!(
        summary["selected_encoded_bytes"].as_u64().unwrap()
            < summary["direct_complete_encoded_bytes"].as_u64().unwrap()
    );
    assert!(String::from_utf8_lossy(&completed.stderr)
        .lines()
        .any(|line| line.contains("PTG2_V4_PROGRESS") && line.contains("\"terminal\":true")));
    for field in [
        "block_copy_path",
        "reference_manifest_path",
        "group_copy_path",
        "component_copy_path",
        "npi_copy_path",
        "provider_set_audit_npi_copy_path",
        "pattern_copy_path",
        "summary_path",
    ] {
        assert!(
            fs::metadata(summary[field].as_str().expect("artifact path"))
                .expect("compiler artifact")
                .is_file()
        );
    }
}

#[test]
fn compiler_cli_rejects_missing_extra_and_malformed_manifests() {
    let no_arguments = run(&[]);
    assert!(!no_arguments.status.success());
    assert!(String::from_utf8_lossy(&no_arguments.stderr).contains("usage:"));

    let extra_arguments = run(&["one", "two"]);
    assert!(!extra_arguments.status.success());
    assert!(String::from_utf8_lossy(&extra_arguments.stderr).contains("usage:"));

    let temporary = tempfile::tempdir().expect("temporary malformed fixture");
    let malformed = temporary.path().join("malformed.json");
    fs::write(&malformed, b"not-json").expect("write malformed manifest");
    let malformed_output = run(&[malformed.to_str().expect("UTF-8 malformed path")]);
    assert!(!malformed_output.status.success());
    assert!(
        String::from_utf8_lossy(&malformed_output.stderr).contains("PTG2_PROVIDER_GRAPH_V4_ERROR")
    );
}

#[test]
fn compiler_cli_covers_direct_layout_and_fail_closed_admission() {
    let temporary = tempfile::tempdir().expect("temporary direct fixture");
    let direct_manifest = write_direct_manifest(temporary.path());
    let direct = run(&[direct_manifest.to_str().expect("UTF-8 direct manifest")]);
    assert!(
        direct.status.success(),
        "direct compiler failed: {}",
        String::from_utf8_lossy(&direct.stderr),
    );
    let summary: Value = serde_json::from_slice(&direct.stdout).expect("direct summary");
    assert_eq!(summary["selected_layout"], "direct");
    assert!(summary["pattern_copy_path"].is_null());

    let already_exists = run(&[direct_manifest.to_str().expect("UTF-8 direct manifest")]);
    assert!(!already_exists.status.success());
    assert!(String::from_utf8_lossy(&already_exists.stderr).contains("already exists"));

    let pattern_manifest = write_manifest(temporary.path());
    let original: Value = serde_json::from_slice(
        &fs::read(&pattern_manifest).expect("read pattern manifest for admission variants"),
    )
    .expect("parse pattern manifest");
    for (name, mutate, expected) in [
        (
            "edge-limit",
            ("max_factor_edges", json!(1)),
            "factor edge count",
        ),
        (
            "memory-limit",
            ("max_estimated_model_bytes", json!(1)),
            "estimated peak bytes",
        ),
    ] {
        let mut manifest = original.clone();
        manifest["options"][mutate.0] = mutate.1;
        manifest["output_directory"] = json!(temporary.path().join(format!("{name}-output")));
        let path = temporary.path().join(format!("{name}.json"));
        fs::write(&path, serde_json::to_vec(&manifest).unwrap()).unwrap();
        let output = run(&[path.to_str().expect("UTF-8 admission manifest")]);
        assert!(!output.status.success());
        assert!(String::from_utf8_lossy(&output.stderr).contains(expected));
    }

    let mut missing_map = original.clone();
    missing_map["provider_set_key_map_path"] = json!(temporary.path().join("missing-map"));
    missing_map["output_directory"] = json!(temporary.path().join("missing-map-output"));
    let missing_map_path = temporary.path().join("missing-map.json");
    fs::write(&missing_map_path, serde_json::to_vec(&missing_map).unwrap()).unwrap();
    let missing_map_output = run(&[missing_map_path.to_str().expect("UTF-8 missing map")]);
    assert!(!missing_map_output.status.success());
    assert!(String::from_utf8_lossy(&missing_map_output.stderr).contains("unavailable"));

    let mut empty = original;
    empty["shards"] = json!([]);
    empty["output_directory"] = json!(temporary.path().join("empty-output"));
    let empty_path = temporary.path().join("empty.json");
    fs::write(&empty_path, serde_json::to_vec(&empty).unwrap()).unwrap();
    let empty_output = run(&[empty_path.to_str().expect("UTF-8 empty manifest")]);
    assert!(!empty_output.status.success());
    assert!(String::from_utf8_lossy(&empty_output.stderr).contains("at least one shard"));
}
