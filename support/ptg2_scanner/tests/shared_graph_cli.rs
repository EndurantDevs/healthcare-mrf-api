use ptg2_scanner::manifest::{
    normalized_sidecar_entries, write_dense_member_sidecar, write_global_sidecar, GlobalId128,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::Path;
use std::process::Command;

const STANDARD_FORMAT: &str = "magic8:uint32_le_version:uint64_le_entry_count:index(owner16:uint64_le_offset:uint32_le_count):members16";
const DENSE_FORMAT: &str = "magic8:uint32_le_version:uint64_le_entry_count:uint64_le_member_global_count:index(owner16:uint64_le_offset:uint32_le_count):member_globals16:members_uint32_le";

type GlobalId = [u8; 16];

fn global(value: u128) -> GlobalId {
    value.to_be_bytes()
}

fn npi(value: u64) -> GlobalId {
    let mut result = [0u8; 16];
    result[8..].copy_from_slice(&value.to_be_bytes());
    result
}

fn hex(value: impl AsRef<[u8]>) -> String {
    value
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn reverse(mapping: &BTreeMap<GlobalId, Vec<GlobalId>>) -> BTreeMap<GlobalId, Vec<GlobalId>> {
    let mut result: BTreeMap<GlobalId, Vec<GlobalId>> = BTreeMap::new();
    for (owner, members) in mapping {
        for member in members {
            result.entry(*member).or_default().push(*owner);
        }
    }
    result
}

fn artifact(
    directory: &Path,
    file_name: &str,
    name: &str,
    shard_id: &str,
    mapping: &BTreeMap<GlobalId, Vec<GlobalId>>,
    dense: bool,
) -> Value {
    let entries = normalized_sidecar_entries(mapping.iter().map(|(owner, members)| {
        (
            GlobalId128(*owner),
            members.iter().copied().map(GlobalId128).collect(),
        )
    }));
    let path = directory.join(file_name);
    let mut output = File::create(&path).expect("create membership sidecar");
    if dense {
        write_dense_member_sidecar(&mut output, &entries).expect("write dense sidecar");
    } else {
        write_global_sidecar(&mut output, &entries).expect("write global sidecar");
    }
    drop(output);
    let payload = fs::read(&path).expect("read membership sidecar");
    let member_global_count = dense
        .then(|| u64::from_le_bytes(payload[20..28].try_into().expect("dense dictionary count")));
    json!({
        "path": path,
        "metadata": {
            "record_format": if dense { DENSE_FORMAT } else { STANDARD_FORMAT },
            "sha256": hex(Sha256::digest(&payload)),
            "byte_count": payload.len(),
            "owner_count": mapping.len(),
            "member_count": mapping.values().map(Vec::len).sum::<usize>(),
            "member_global_count": member_global_count,
            "name": name,
            "source_shard_id": shard_id,
            "shard_id": null,
        },
    })
}

fn shared_graph_manifest(root: &Path) -> Value {
    let input = root.join("input");
    fs::create_dir(&input).expect("create graph input");
    let group_a = global(0xa0);
    let group_b = global(0xb0);
    let provider_a = global(0x1000);
    let provider_b = global(0x2000);
    let group_npi = BTreeMap::from([
        (group_a, vec![npi(1_000_000_000), npi(1_000_000_001)]),
        (group_b, vec![npi(1_000_000_001)]),
    ]);
    let group_provider_set = BTreeMap::from([
        (group_a, vec![provider_a, provider_b]),
        (group_b, vec![provider_b]),
    ]);
    let npi_group = reverse(&group_npi);
    let provider_set_group = reverse(&group_provider_set);

    let provider_map = root.join("provider-set-key-map.copy");
    let mut provider_writer = BufWriter::new(File::create(&provider_map).unwrap());
    writeln!(provider_writer, "{}\t0", hex(provider_a)).unwrap();
    writeln!(provider_writer, "{}\t1", hex(provider_b)).unwrap();
    provider_writer.flush().unwrap();

    json!({
        "provider_set_key_map_path": provider_map,
        "output_directory": root.join("output"),
        "shards": [{
            "shard_id": "source-a",
            "group_npi": artifact(&input, "group-npi.bin", "provider_group_npi", "source-a", &group_npi, false),
            "npi_group": artifact(&input, "npi-group.bin", "provider_npi_group", "source-a", &npi_group, true),
            "group_provider_set": artifact(&input, "group-provider.bin", "provider_inverted", "source-a", &group_provider_set, true),
            "provider_set_group": artifact(&input, "provider-group.bin", "provider_forward", "source-a", &provider_set_group, false),
        }],
    })
}

fn run_shared_graph_converter(root: &Path, manifest: &Value) -> std::process::Output {
    let manifest_path = root.join("manifest.json");
    fs::write(
        &manifest_path,
        serde_json::to_vec(manifest).expect("encode graph manifest"),
    )
    .expect("write graph manifest");
    Command::new(env!("CARGO_BIN_EXE_ptg2_scanner"))
        .args(["--convert-shared-graph", manifest_path.to_str().unwrap()])
        .output()
        .expect("run shared graph converter")
}

#[test]
fn shared_graph_converter_cli_publishes_all_exact_directions() {
    let temporary = tempfile::tempdir().expect("temporary graph root");
    let manifest = shared_graph_manifest(temporary.path());
    let output = temporary.path().join("output");
    let completed = run_shared_graph_converter(temporary.path(), &manifest);
    assert!(
        completed.status.success(),
        "converter failed: {}\nstdout:\n{}",
        String::from_utf8_lossy(&completed.stderr),
        String::from_utf8_lossy(&completed.stdout),
    );
    let summary = String::from_utf8(completed.stdout)
        .expect("UTF-8 converter output")
        .lines()
        .find_map(|line| serde_json::from_str::<Value>(line).ok())
        .expect("shared graph JSON summary");
    assert_eq!(summary["format"], "ptg2_v3_shared_graph_summary_v1");
    assert_eq!(summary["provider_group_count"], 2);
    assert_eq!(summary["npi_count"], 2);
    assert_eq!(summary["direction_metrics"].as_array().unwrap().len(), 4);
    assert_eq!(summary["integrity"]["unique_edge_count"], 6);
    for name in [
        "graph-blocks.copy",
        "graph-owners.copy",
        "provider-groups.copy",
        "npi-scope.copy",
        "graph-blocks.spool",
        "graph-owners.spool",
        "provider-group.map",
        "graph-references.run",
    ] {
        assert!(output.join(name).is_file(), "missing {name}");
    }
}

#[test]
fn shared_graph_converter_cli_rejects_invalid_descriptor_boundaries() {
    fn run_case<F>(name: &str, mutate: F, expected: &str)
    where
        F: FnOnce(&mut Value),
    {
        let temporary = tempfile::tempdir().expect("temporary invalid graph root");
        let mut manifest = shared_graph_manifest(temporary.path());
        mutate(&mut manifest);
        let completed = run_shared_graph_converter(temporary.path(), &manifest);
        assert!(!completed.status.success(), "{name} unexpectedly succeeded");
        let stderr = String::from_utf8_lossy(&completed.stderr);
        assert!(
            stderr.contains(expected),
            "{name} stderr did not contain {expected:?}: {stderr}"
        );
    }

    run_case(
        "empty-shards",
        |manifest| manifest["shards"] = json!([]),
        "manifest.shards must contain at least one shard",
    );
    run_case(
        "missing-artifact",
        |manifest| {
            let existing = manifest["shards"][0]["group_npi"]["path"]
                .as_str()
                .expect("membership sidecar path");
            let missing = Path::new(existing).with_file_name("missing-sidecar.bin");
            manifest["shards"][0]["group_npi"]["path"] = json!(missing);
        },
        "membership sidecar is unavailable",
    );
    run_case(
        "invalid-provider-key",
        |manifest| {
            let provider_map = manifest["provider_set_key_map_path"]
                .as_str()
                .expect("provider-set key map path");
            fs::write(
                provider_map,
                b"00000000000000000000000000001000\tnot-a-key\n",
            )
            .expect("replace provider-set key map");
        },
        "provider-set dictionary export has an invalid row",
    );
    run_case(
        "duplicate-artifact",
        |manifest| {
            manifest["shards"][0]["npi_group"]["path"] =
                manifest["shards"][0]["group_npi"]["path"].clone();
        },
        "reuses an artifact for multiple directions",
    );
    run_case(
        "wrong-artifact-name",
        |manifest| {
            manifest["shards"][0]["group_npi"]["metadata"]["name"] = json!("provider_forward");
        },
        "wrong provider_group_npi artifact",
    );
    run_case(
        "contradictory-shard-identities",
        |manifest| {
            manifest["shards"][0]["group_npi"]["metadata"]["shard_id"] = json!("source-b");
        },
        "contradictory shard identities",
    );
    run_case(
        "mismatched-shard-identity",
        |manifest| {
            manifest["shards"][0]["group_npi"]["metadata"]["source_shard_id"] = json!("source-b");
        },
        "shared graph shard identity mismatch",
    );
    run_case(
        "sidecar-byte-count",
        |manifest| {
            let byte_count = manifest["shards"][0]["group_npi"]["metadata"]["byte_count"]
                .as_u64()
                .expect("sidecar byte count");
            manifest["shards"][0]["group_npi"]["metadata"]["byte_count"] = json!(byte_count + 1);
        },
        "membership sidecar byte count mismatch",
    );
    run_case(
        "missing-dense-dictionary-count",
        |manifest| {
            manifest["shards"][0]["npi_group"]["metadata"]["member_global_count"] = Value::Null;
        },
        "requires member_global_count",
    );
    run_case(
        "mismatched-dense-dictionary-count",
        |manifest| {
            let count = manifest["shards"][0]["npi_group"]["metadata"]["member_global_count"]
                .as_u64()
                .expect("dense dictionary count");
            manifest["shards"][0]["npi_group"]["metadata"]["member_global_count"] =
                json!(count + 1);
        },
        "dense membership dictionary count mismatch",
    );
}
