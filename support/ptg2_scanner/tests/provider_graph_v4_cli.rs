use ptg2_scanner::manifest::{write_dense_member_sidecar, GlobalId128, SidecarEntry};
use ptg2_scanner::provider_graph_v4::{
    compile_provider_graph_v4_manifest, ProviderGraphV4Error, ProviderGraphV4Layout,
    ProviderGraphV4Manifest,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::error::Error;
use std::fs::{self, File};
use std::io::BufWriter;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

const DENSE_FORMAT: &str = "magic8:uint32_le_version:uint64_le_entry_count:uint64_le_member_global_count:index(owner16:uint64_le_offset:uint32_le_count):member_globals16:members_uint32_le";
const TAX_POLICY_ID: &str = "ptg-tin-hmac-sha256-v1:test-1";
const COPY_HEADER: &[u8] = b"PGCOPY\n\xff\r\n\0\0\0\0\0\0\0\0\0";
const NPI_SCOPE_INPUT_DOMAIN: &[u8] = b"PTG2V4NPISCOPE\x01";
const NPI_SCOPE_BINDING_DOMAIN: &[u8] = b"ptg2:v4:provider-npi-scope-binding:v1\x00";
const NPI_SCOPE_SHARD_BINDING_DOMAIN: &[u8] = b"ptg2:v4:provider-npi-scope-shard-binding:v1\x00";
const TAXONOMY_RULE_SET_DOMAIN: &[u8] = b"ptg2:v4:inferred-taxonomy-rule-set:v1\x00";

type ManifestMutator = fn(&mut Value);
type CopyBytesMutator = fn(&mut Vec<u8>);

fn sha256_hex(payload: &[u8]) -> String {
    Sha256::digest(payload)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn decode_sha256(value: &str) -> [u8; 32] {
    assert_eq!(value.len(), 64);
    let mut result = [0u8; 32];
    for (index, byte) in result.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16).expect("SHA-256 hex");
    }
    result
}

fn update_length_prefixed(hasher: &mut Sha256, value: &[u8]) {
    hasher.update((value.len() as u32).to_be_bytes());
    hasher.update(value);
}

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

fn scope_copy_bytes(rows: &[(u32, u64)]) -> Vec<u8> {
    let mut bytes = COPY_HEADER.to_vec();
    for (key, npi) in rows {
        bytes.extend_from_slice(&2i16.to_be_bytes());
        bytes.extend_from_slice(&4i32.to_be_bytes());
        bytes.extend_from_slice(&(*key as i32).to_be_bytes());
        bytes.extend_from_slice(&8i32.to_be_bytes());
        bytes.extend_from_slice(&(*npi as i64).to_be_bytes());
    }
    bytes.extend_from_slice(&(-1i16).to_be_bytes());
    bytes
}

fn provider_scope_copy_bytes(npis: &[u64]) -> Vec<u8> {
    let mut bytes = COPY_HEADER.to_vec();
    for npi in npis {
        bytes.extend_from_slice(&1i16.to_be_bytes());
        bytes.extend_from_slice(&8i32.to_be_bytes());
        bytes.extend_from_slice(&(*npi as i64).to_be_bytes());
    }
    bytes.extend_from_slice(&(-1i16).to_be_bytes());
    bytes
}

fn write_provider_npi_scope(
    path: &Path,
    shard_id: &str,
    reciprocal: &Value,
    npis: &[u64],
) -> Value {
    let bytes = provider_scope_copy_bytes(npis);
    fs::write(path, &bytes).expect("write provider NPI scope");
    let reciprocal_metadata = &reciprocal["metadata"];
    let reciprocal_sha = reciprocal_metadata["sha256"]
        .as_str()
        .expect("reciprocal SHA");
    let reciprocal_format = reciprocal_metadata["record_format"]
        .as_str()
        .expect("reciprocal format");
    let reciprocal_bytes = reciprocal_metadata["byte_count"]
        .as_u64()
        .expect("reciprocal bytes");
    let reciprocal_owners = reciprocal_metadata["owner_count"]
        .as_u64()
        .expect("reciprocal owners");
    let reciprocal_members = reciprocal_metadata["member_count"]
        .as_u64()
        .expect("reciprocal members");
    let reciprocal_globals = reciprocal_metadata["member_global_count"]
        .as_u64()
        .expect("reciprocal global members");
    let mut binding = Sha256::new();
    binding.update(NPI_SCOPE_BINDING_DOMAIN);
    update_length_prefixed(&mut binding, b"ptg2_provider_npi_scope_pg_binary_int8_v1");
    binding.update(decode_sha256(&sha256_hex(&bytes)));
    binding.update((bytes.len() as u64).to_be_bytes());
    binding.update((npis.len() as u64).to_be_bytes());
    binding.update(decode_sha256(reciprocal_sha));
    update_length_prefixed(&mut binding, reciprocal_format.as_bytes());
    binding.update(reciprocal_bytes.to_be_bytes());
    binding.update(reciprocal_owners.to_be_bytes());
    binding.update(reciprocal_members.to_be_bytes());
    binding.update(reciprocal_globals.to_be_bytes());
    let binding_sha = binding.finalize();
    let mut shard_binding = Sha256::new();
    shard_binding.update(NPI_SCOPE_SHARD_BINDING_DOMAIN);
    shard_binding.update(binding_sha);
    update_length_prefixed(&mut shard_binding, shard_id.as_bytes());
    json!({
        "path": path,
        "metadata": {
            "record_format": "ptg2_provider_npi_scope_pg_binary_int8_v1",
            "sha256": sha256_hex(&bytes),
            "byte_count": bytes.len(),
            "row_count": npis.len(),
            "provider_npi_group_sha256": reciprocal_sha,
            "provider_npi_group_record_format": reciprocal_format,
            "provider_npi_group_byte_count": reciprocal_bytes,
            "provider_npi_group_owner_count": reciprocal_owners,
            "provider_npi_group_member_count": reciprocal_members,
            "provider_npi_group_member_global_count": reciprocal_globals,
            "binding_contract": "provider_npi_scope_to_provider_npi_group_v1",
            "binding_sha256": binding_sha.iter().map(|byte| format!("{byte:02x}")).collect::<String>(),
            "shard_binding_contract": "provider_npi_scope_shard_binding_v1",
            "shard_binding_sha256": shard_binding.finalize().iter().map(|byte| format!("{byte:02x}")).collect::<String>(),
            "retention_contract": "shared_v4_publication_scratch_v1",
            "name": "provider_npi_scope",
            "source_shard_id": shard_id,
        }
    })
}

fn refresh_provider_npi_scope_integrity(shard: &mut Value) {
    let shard_id = shard["shard_id"]
        .as_str()
        .expect("provider graph shard ID")
        .to_owned();
    let reciprocal = shard["provider_npi_group"]["metadata"].clone();
    let scope = &mut shard["provider_npi_scope"];
    let path = PathBuf::from(scope["path"].as_str().expect("provider NPI scope path"));
    let bytes = fs::read(path).expect("read mutated provider NPI scope");
    let metadata = &mut scope["metadata"];
    metadata["byte_count"] = json!(bytes.len());
    metadata["sha256"] = json!(sha256_hex(&bytes));

    let mut binding = Sha256::new();
    binding.update(NPI_SCOPE_BINDING_DOMAIN);
    update_length_prefixed(
        &mut binding,
        metadata["record_format"]
            .as_str()
            .expect("provider NPI scope format")
            .as_bytes(),
    );
    binding.update(decode_sha256(
        metadata["sha256"].as_str().expect("provider NPI scope SHA"),
    ));
    binding.update(
        metadata["byte_count"]
            .as_u64()
            .expect("provider NPI scope bytes")
            .to_be_bytes(),
    );
    binding.update(
        metadata["row_count"]
            .as_u64()
            .expect("provider NPI scope rows")
            .to_be_bytes(),
    );
    binding.update(decode_sha256(
        reciprocal["sha256"].as_str().expect("reciprocal graph SHA"),
    ));
    update_length_prefixed(
        &mut binding,
        reciprocal["record_format"]
            .as_str()
            .expect("reciprocal graph format")
            .as_bytes(),
    );
    for field in [
        "byte_count",
        "owner_count",
        "member_count",
        "member_global_count",
    ] {
        binding.update(
            reciprocal[field]
                .as_u64()
                .expect("reciprocal graph count")
                .to_be_bytes(),
        );
    }
    let binding_sha = binding.finalize();
    metadata["binding_sha256"] = json!(binding_sha
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>());
    let mut shard_binding = Sha256::new();
    shard_binding.update(NPI_SCOPE_SHARD_BINDING_DOMAIN);
    shard_binding.update(binding_sha);
    update_length_prefixed(&mut shard_binding, shard_id.as_bytes());
    metadata["shard_binding_sha256"] = json!(shard_binding
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>());
}

fn compiler_side_inputs(
    root: &Path,
    shard_id: &str,
    provider_npi_scope: &Value,
    npis: &[u64],
) -> (Value, Value) {
    let rows = npis
        .iter()
        .enumerate()
        .map(|(key, npi)| (key as u32, *npi))
        .collect::<Vec<_>>();
    let scope_path = root.join(format!("{shard_id}-scope-prepass.copy"));
    let scope_bytes = scope_copy_bytes(&rows);
    fs::write(&scope_path, &scope_bytes).expect("write compiler NPI scope");
    let scope_sha = sha256_hex(&scope_bytes);
    let metadata = &provider_npi_scope["metadata"];
    let mut input_digest = Sha256::new();
    input_digest.update(NPI_SCOPE_INPUT_DOMAIN);
    update_length_prefixed(&mut input_digest, shard_id.as_bytes());
    input_digest.update(decode_sha256(
        metadata["sha256"].as_str().expect("scope SHA"),
    ));
    input_digest.update(
        metadata["byte_count"]
            .as_u64()
            .expect("scope bytes")
            .to_be_bytes(),
    );
    input_digest.update(
        metadata["row_count"]
            .as_u64()
            .expect("scope rows")
            .to_be_bytes(),
    );
    input_digest.update(decode_sha256(
        metadata["binding_sha256"]
            .as_str()
            .expect("scope binding SHA"),
    ));
    let npi_scope = json!({
        "format": "ptg2_provider_graph_v4_npi_scope_v1",
        "row_count": rows.len(),
        "source_owner_count": metadata["row_count"],
        "input_byte_count": metadata["byte_count"],
        "input_sha256": input_digest.finalize().iter().map(|byte| format!("{byte:02x}")).collect::<String>(),
        "output_byte_count": scope_bytes.len(),
        "output_sha256": scope_sha,
        "output_path": scope_path,
    });

    let taxonomy_members_path = root.join(format!("{shard_id}-taxonomy-members.u32le"));
    let taxonomy_members = (0..npis.len() as u32)
        .flat_map(u32::to_le_bytes)
        .collect::<Vec<_>>();
    fs::write(&taxonomy_members_path, &taxonomy_members).expect("write taxonomy members");
    let rule_digest = Sha256::digest(format!("{shard_id}:rule").as_bytes());
    let catalog_digest = Sha256::digest(format!("{shard_id}:catalog").as_bytes());
    let mut rule_set_digest = Sha256::new();
    rule_set_digest.update(TAXONOMY_RULE_SET_DOMAIN);
    rule_set_digest.update(1u32.to_be_bytes());
    rule_set_digest.update(rule_digest);
    let inferred_taxonomy = json!({
        "contract": "ptg2_v4_inferred_taxonomy_compiler_input_v1",
        "catalog_contract": "snapshot_npi_live_catalog_individual_v1",
        "vector_format": "sorted_u32le_v1",
        "npi_scope_sha256": npi_scope["output_sha256"],
        "rule_set_digest": rule_set_digest.finalize().iter().map(|byte| format!("{byte:02x}")).collect::<String>(),
        "members": {
            "path": taxonomy_members_path,
            "byte_count": taxonomy_members.len(),
            "sha256": sha256_hex(&taxonomy_members),
        },
        "rules": [{
            "rule_digest": rule_digest.iter().map(|byte| format!("{byte:02x}")).collect::<String>(),
            "catalog_digest": catalog_digest.iter().map(|byte| format!("{byte:02x}")).collect::<String>(),
            "member_count": npis.len(),
            "member_offset_bytes": 0,
            "member_byte_count": taxonomy_members.len(),
        }],
    });
    (npi_scope, inferred_taxonomy)
}

fn write_missing_tax_identity(path: &Path, shard_id: &str, groups: &[GlobalId128]) -> Value {
    let mut groups = groups.to_vec();
    groups.sort_unstable();
    groups.dedup();
    let mut bytes = Vec::new();
    bytes.extend_from_slice(b"PTG2TAX1");
    bytes.extend_from_slice(&1u16.to_le_bytes());
    bytes.extend_from_slice(&65u16.to_le_bytes());
    bytes.push(TAX_POLICY_ID.len() as u8);
    bytes.extend_from_slice(TAX_POLICY_ID.as_bytes());
    for group in &groups {
        bytes.extend_from_slice(&group.0);
        bytes.push(2);
        bytes.extend_from_slice(&[0u8; 48]);
    }
    fs::write(path, &bytes).expect("write missing tax identities");
    json!({
        "path": path,
        "metadata": {
            "record_format": "ptg2_provider_group_tax_identity_v1",
            "sha256": sha256_hex(&bytes),
            "byte_count": bytes.len(),
            "row_count": groups.len(),
            "provider_group_count": groups.len(),
            "matched_ein_count": 0,
            "missing_count": groups.len(),
            "malformed_count": 0,
            "unsupported_type_count": 0,
            "version": 1,
            "record_bytes": 65,
            "token_policy_id": TAX_POLICY_ID,
            "normalization_contract": "ein_ascii_digits_or_2_7_hyphen_v1",
            "hmac_contract": "hmac_sha256_ptg_tin_v1",
            "final": true,
            "name": "provider_group_tax_identity",
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
            members: groups.clone(),
        }],
    );
    let npi_values = [1_234_567_890];
    let provider_npi_scope = write_provider_npi_scope(
        &root.join("provider-npi-scope.copy"),
        shard_id,
        &npi_group,
        &npi_values,
    );
    let (npi_scope, inferred_taxonomy) =
        compiler_side_inputs(root, shard_id, &provider_npi_scope, &npi_values);
    let group_tax_identity =
        write_missing_tax_identity(&root.join("group-tax-identity.sidecar"), shard_id, &groups);
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
            "provider_npi_scope": provider_npi_scope,
            "provider_group_tax_identity": group_tax_identity,
        }],
        "provider_set_key_map_path": provider_map,
        "npi_scope": npi_scope,
        "inferred_taxonomy": inferred_taxonomy,
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
    let npi_values = [1_111_111_111, 2_222_222_222];
    let npis = npi_values.map(|value| global(0, value));
    let set_component = write_membership(
        &root.join("direct-set-component.sidecar"),
        "provider_set_component",
        shard_id,
        sets.iter()
            .copied()
            .map(|owner| SidecarEntry {
                owner,
                members: components.to_vec(),
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
    let provider_npi_scope = write_provider_npi_scope(
        &root.join("direct-provider-npi-scope.copy"),
        shard_id,
        &npi_group,
        &npi_values,
    );
    let (npi_scope, inferred_taxonomy) =
        compiler_side_inputs(root, shard_id, &provider_npi_scope, &npi_values);
    let group_tax_identity = write_missing_tax_identity(
        &root.join("direct-group-tax-identity.sidecar"),
        shard_id,
        &groups,
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
            "provider_npi_scope": provider_npi_scope,
            "provider_group_tax_identity": group_tax_identity,
        }],
        "provider_set_key_map_path": provider_map,
        "npi_scope": npi_scope,
        "inferred_taxonomy": inferred_taxonomy,
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

fn mixed_pattern_compiler_manifest(root: &Path, groups_per_component: usize) -> Value {
    let shard_id = "mixed-pattern-shard";
    let sets = [global(1, 1), global(1, 2), global(1, 3)];
    let components = [global(2, 1), global(2, 2)];
    let first_groups = (0..groups_per_component)
        .map(|index| global(3, index as u64 + 1))
        .collect::<Vec<_>>();
    let second_groups = (0..groups_per_component)
        .map(|index| global(3, groups_per_component as u64 + index as u64 + 1))
        .collect::<Vec<_>>();
    let groups = first_groups
        .iter()
        .chain(&second_groups)
        .copied()
        .collect::<Vec<_>>();
    let provider_npi = global(0, 1_234_567_890);
    let npi_values = [1_234_567_890];
    let set_component = write_membership(
        &root.join("mixed-set-component.sidecar"),
        "provider_set_component",
        shard_id,
        vec![
            SidecarEntry {
                owner: sets[0],
                members: components.to_vec(),
            },
            SidecarEntry {
                owner: sets[1],
                members: vec![components[0]],
            },
            SidecarEntry {
                owner: sets[2],
                members: vec![components[1]],
            },
        ],
    );
    let component_group = write_membership(
        &root.join("mixed-component-group.sidecar"),
        "provider_component_group",
        shard_id,
        vec![
            SidecarEntry {
                owner: components[0],
                members: first_groups,
            },
            SidecarEntry {
                owner: components[1],
                members: second_groups,
            },
        ],
    );
    let group_npi = write_membership(
        &root.join("mixed-group-npi.sidecar"),
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
        &root.join("mixed-npi-group.sidecar"),
        "provider_npi_group",
        shard_id,
        vec![SidecarEntry {
            owner: provider_npi,
            members: groups.clone(),
        }],
    );
    let provider_npi_scope = write_provider_npi_scope(
        &root.join("mixed-provider-npi-scope.copy"),
        shard_id,
        &npi_group,
        &npi_values,
    );
    let (npi_scope, inferred_taxonomy) =
        compiler_side_inputs(root, shard_id, &provider_npi_scope, &npi_values);
    let group_tax_identity = write_missing_tax_identity(
        &root.join("mixed-group-tax-identity.sidecar"),
        shard_id,
        &groups,
    );
    let provider_map = root.join("mixed-provider-map.copy");
    fs::write(
        &provider_map,
        sets.iter()
            .enumerate()
            .map(|(index, provider_set)| format!("{}\t{}\n", hex(*provider_set), index + 1))
            .collect::<String>(),
    )
    .expect("write mixed provider-set key map");
    json!({
        "shards": [{
            "shard_id": shard_id,
            "provider_set_component": set_component,
            "provider_component_group": component_group,
            "provider_group_npi": group_npi,
            "provider_npi_group": npi_group,
            "provider_npi_scope": provider_npi_scope,
            "provider_group_tax_identity": group_tax_identity,
        }],
        "provider_set_key_map_path": provider_map,
        "npi_scope": npi_scope,
        "inferred_taxonomy": inferred_taxonomy,
        "output_directory": root.join("mixed-compiled"),
        "options": {
            "member_page_bytes": 64,
            "locator_page_bytes": 48,
            "heavy_owner_member_threshold": 8,
            "heavy_bitmap_minimum_savings_bytes": 0,
        }
    })
}

fn write_heavy_direct_manifest(root: &Path) -> PathBuf {
    let shard_id = "heavy-direct-shard";
    let component = global(2, 1);
    let group = global(3, 1);
    let sets = (1..=512).map(|value| global(1, value)).collect::<Vec<_>>();
    let npi_values = (1..=512)
        .map(|value| 1_000_000_000 + value)
        .collect::<Vec<_>>();
    let npis = npi_values
        .iter()
        .copied()
        .map(|value| global(0, value))
        .collect::<Vec<_>>();
    let set_component = write_membership(
        &root.join("heavy-direct-set-component.sidecar"),
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
        &root.join("heavy-direct-component-group.sidecar"),
        "provider_component_group",
        shard_id,
        vec![SidecarEntry {
            owner: component,
            members: vec![group],
        }],
    );
    let group_npi = write_membership(
        &root.join("heavy-direct-group-npi.sidecar"),
        "provider_group_npi",
        shard_id,
        vec![SidecarEntry {
            owner: group,
            members: npis.clone(),
        }],
    );
    let npi_group = write_membership(
        &root.join("heavy-direct-npi-group.sidecar"),
        "provider_npi_group",
        shard_id,
        npis.iter()
            .copied()
            .map(|owner| SidecarEntry {
                owner,
                members: vec![group],
            })
            .collect(),
    );
    let provider_npi_scope = write_provider_npi_scope(
        &root.join("heavy-direct-provider-npi-scope.copy"),
        shard_id,
        &npi_group,
        &npi_values,
    );
    let (npi_scope, inferred_taxonomy) =
        compiler_side_inputs(root, shard_id, &provider_npi_scope, &npi_values);
    let group_tax_identity = write_missing_tax_identity(
        &root.join("heavy-direct-group-tax-identity.sidecar"),
        shard_id,
        &[group],
    );
    let provider_map = root.join("heavy-direct-provider-map.copy");
    fs::write(
        &provider_map,
        sets.iter()
            .enumerate()
            .map(|(index, provider_set)| format!("{}\t{}\n", hex(*provider_set), index + 1))
            .collect::<String>(),
    )
    .expect("write heavy direct provider-set key map");
    let manifest_path = root.join("heavy-direct-manifest.json");
    let manifest = json!({
        "shards": [{
            "shard_id": shard_id,
            "provider_set_component": set_component,
            "provider_component_group": component_group,
            "provider_group_npi": group_npi,
            "provider_npi_group": npi_group,
            "provider_npi_scope": provider_npi_scope,
            "provider_group_tax_identity": group_tax_identity,
        }],
        "provider_set_key_map_path": provider_map,
        "npi_scope": npi_scope,
        "inferred_taxonomy": inferred_taxonomy,
        "output_directory": root.join("heavy-direct-compiled"),
        "options": {
            "member_page_bytes": 64,
            "locator_page_bytes": 48,
            "heavy_owner_member_threshold": 1,
            "heavy_bitmap_minimum_savings_bytes": 0,
            "npi_prefix_target": 201,
            "max_online_candidate_pattern_projection_members": 1,
        }
    });
    fs::write(
        &manifest_path,
        serde_json::to_vec_pretty(&manifest).expect("encode heavy direct manifest"),
    )
    .expect("write heavy direct manifest");
    manifest_path
}

fn run_manifest_variant(root: &Path, name: &str, mut manifest: Value) -> Output {
    manifest["output_directory"] = json!(root.join(format!("{name}-output")));
    let path = root.join(format!("{name}.json"));
    fs::write(
        &path,
        serde_json::to_vec(&manifest).expect("encode manifest variant"),
    )
    .expect("write manifest variant");
    run(&[path.to_str().expect("UTF-8 manifest variant path")])
}

fn run(arguments: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_ptg2_provider_graph_v4"))
        .args(arguments)
        .output()
        .expect("run V4 graph compiler")
}

fn write_npi_scope_manifest(root: &Path, name: &str, shards: Value) -> PathBuf {
    let path = root.join(format!("{name}-npi-scope-manifest.json"));
    fs::write(
        &path,
        serde_json::to_vec(&json!({
            "shards": shards,
            "output_path": root.join(format!("{name}-npi-scope.copy")),
        }))
        .expect("encode NPI scope manifest"),
    )
    .expect("write NPI scope manifest");
    path
}

fn compiler_manifest(root: &Path) -> Value {
    let path = write_manifest(root);
    serde_json::from_slice(&fs::read(path).expect("read provider graph manifest"))
        .expect("parse provider graph manifest")
}

#[test]
fn compiler_errors_preserve_typed_sources() {
    let io_error = ProviderGraphV4Error::from(std::io::Error::other("fixture"));
    assert!(io_error.source().is_some());
    let json_error = ProviderGraphV4Error::from(serde_json::from_str::<Value>("{").unwrap_err());
    assert!(json_error.source().is_some());
    let invalid_error = ProviderGraphV4Error::InvalidData("fixture".into());
    assert!(invalid_error.source().is_none());
}

#[test]
fn npi_scope_cli_merges_authenticated_shards_and_refuses_output_reuse() {
    let temporary = tempfile::tempdir().expect("temporary NPI scope fixture");
    let pattern_root = temporary.path().join("pattern");
    let direct_root = temporary.path().join("direct");
    fs::create_dir_all(&pattern_root).expect("create pattern fixture root");
    fs::create_dir_all(&direct_root).expect("create direct fixture root");
    let mut pattern = compiler_manifest(&pattern_root);
    let direct_path = write_direct_manifest(&direct_root);
    let mut direct: Value =
        serde_json::from_slice(&fs::read(direct_path).expect("read direct graph manifest"))
            .expect("parse direct graph manifest");
    let mut shards = pattern["shards"]
        .as_array_mut()
        .expect("pattern shards")
        .drain(..)
        .collect::<Vec<_>>();
    shards.append(direct["shards"].as_array_mut().expect("direct shards"));
    let manifest = write_npi_scope_manifest(temporary.path(), "merged", json!(shards));
    let completed = run(&[
        "--extract-npi-scope",
        manifest.to_str().expect("UTF-8 NPI scope manifest"),
    ]);
    assert!(
        completed.status.success(),
        "NPI scope extraction failed: {}",
        String::from_utf8_lossy(&completed.stderr),
    );
    let summary: Value = serde_json::from_slice(&completed.stdout).expect("NPI scope summary");
    assert_eq!(summary["format"], "ptg2_provider_graph_v4_npi_scope_v1");
    assert_eq!(summary["row_count"], 3);
    assert_eq!(summary["source_owner_count"], 3);
    let output_path = PathBuf::from(summary["output_path"].as_str().expect("scope output path"));
    assert_eq!(
        fs::metadata(&output_path).expect("NPI scope output").len(),
        summary["output_byte_count"].as_u64().expect("output bytes"),
    );
    assert_eq!(
        sha256_hex(&fs::read(&output_path).expect("read NPI scope output")),
        summary["output_sha256"].as_str().expect("output SHA"),
    );

    let repeated = run(&[
        "--extract-npi-scope",
        manifest.to_str().expect("UTF-8 NPI scope manifest"),
    ]);
    assert!(!repeated.status.success());
    assert!(String::from_utf8_lossy(&repeated.stderr).contains("already exists"));

    let missing_manifest = run(&["--extract-npi-scope"]);
    assert!(!missing_manifest.status.success());
    assert!(String::from_utf8_lossy(&missing_manifest.stderr).contains("usage:"));
}

#[test]
fn library_compiler_emits_multiple_observed_taxonomy_rules_and_matched_tax_identity() {
    let temporary = tempfile::tempdir().expect("temporary taxonomy and tax identity fixture");
    let path = write_heavy_direct_manifest(temporary.path());
    let mut compiler: Value =
        serde_json::from_slice(&fs::read(path).expect("read heavy direct manifest"))
            .expect("parse heavy direct manifest");

    let member_path = PathBuf::from(
        compiler["inferred_taxonomy"]["members"]["path"]
            .as_str()
            .expect("taxonomy member path"),
    );
    let member_bytes = [0u32, 1, 0, 1]
        .into_iter()
        .flat_map(u32::to_le_bytes)
        .collect::<Vec<_>>();
    fs::write(&member_path, &member_bytes).expect("write two taxonomy member ranges");
    let rule_digests = [[1u8; 32], [2u8; 32]];
    let mut rule_set_digest = Sha256::new();
    rule_set_digest.update(TAXONOMY_RULE_SET_DOMAIN);
    rule_set_digest.update(2u32.to_be_bytes());
    for digest in rule_digests {
        rule_set_digest.update(digest);
    }
    let encode_hex = |bytes: &[u8]| {
        bytes
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    };
    compiler["inferred_taxonomy"]["members"]["byte_count"] = json!(member_bytes.len());
    compiler["inferred_taxonomy"]["members"]["sha256"] = json!(sha256_hex(&member_bytes));
    compiler["inferred_taxonomy"]["rule_set_digest"] =
        json!(encode_hex(&rule_set_digest.finalize()));
    compiler["inferred_taxonomy"]["rules"] = json!([
        {
            "rule_digest": encode_hex(&rule_digests[0]),
            "catalog_digest": encode_hex(&[33u8; 32]),
            "member_count": 2,
            "member_offset_bytes": 0,
            "member_byte_count": 8,
        },
        {
            "rule_digest": encode_hex(&rule_digests[1]),
            "catalog_digest": encode_hex(&[34u8; 32]),
            "member_count": 2,
            "member_offset_bytes": 8,
            "member_byte_count": 8,
        },
    ]);
    compiler["options"]["max_online_inferred_taxonomy_candidates"] = json!(1);

    let tax_identity = &mut compiler["shards"][0]["provider_group_tax_identity"];
    let tax_path = PathBuf::from(
        tax_identity["path"]
            .as_str()
            .expect("provider tax identity path"),
    );
    let hmac = [0x5au8; 32];
    let mut tax_bytes = Vec::new();
    tax_bytes.extend_from_slice(b"PTG2TAX1");
    tax_bytes.extend_from_slice(&1u16.to_le_bytes());
    tax_bytes.extend_from_slice(&65u16.to_le_bytes());
    tax_bytes.push(TAX_POLICY_ID.len() as u8);
    tax_bytes.extend_from_slice(TAX_POLICY_ID.as_bytes());
    tax_bytes.extend_from_slice(&global(3, 1).0);
    tax_bytes.push(1);
    tax_bytes.extend_from_slice(&hmac[..16]);
    tax_bytes.extend_from_slice(&hmac);
    fs::write(&tax_path, &tax_bytes).expect("write matched tax identity");
    tax_identity["metadata"]["sha256"] = json!(sha256_hex(&tax_bytes));
    tax_identity["metadata"]["byte_count"] = json!(tax_bytes.len());
    tax_identity["metadata"]["row_count"] = json!(1);
    tax_identity["metadata"]["provider_group_count"] = json!(1);
    tax_identity["metadata"]["matched_ein_count"] = json!(1);
    tax_identity["metadata"]["missing_count"] = json!(0);

    let manifest: ProviderGraphV4Manifest =
        serde_json::from_value(compiler).expect("typed taxonomy and tax identity manifest");
    let summary =
        compile_provider_graph_v4_manifest(manifest).expect("compile taxonomy and tax identity");
    assert_eq!(summary.selected_layout, ProviderGraphV4Layout::Pattern);
    assert_eq!(summary.max_online_inferred_taxonomy_candidates, 1);
    assert_eq!(summary.tax_identity.tax_identity_count, 1);
    assert_eq!(summary.tax_identity.matched_ein_count, 1);
    assert!(summary.direct_inferred_taxonomy_eligible);
    assert!(summary.pattern_inferred_taxonomy_eligible);
    let candidates =
        fs::read(&summary.inferred_taxonomy_copy_path).expect("read inferred taxonomy candidates");
    assert_eq!(
        candidates
            .windows(b"observe_v1".len())
            .filter(|window| *window == b"observe_v1")
            .count(),
        2,
    );
}

#[test]
fn library_compiler_selects_bounded_mixed_representations() {
    let temporary = tempfile::tempdir().expect("temporary mixed compiler fixture");
    let original = mixed_pattern_compiler_manifest(temporary.path(), 128);
    let compile_variant = |name: &str, options: Value| {
        let mut manifest = original.clone();
        manifest["output_directory"] = json!(temporary.path().join(name));
        let option_map = manifest["options"]
            .as_object_mut()
            .expect("mixed compiler options");
        option_map.extend(
            options
                .as_object()
                .expect("variant options")
                .iter()
                .map(|(key, value)| (key.clone(), value.clone())),
        );
        compile_provider_graph_v4_manifest(
            serde_json::from_value(manifest).expect("typed mixed compiler manifest"),
        )
    };

    let component_fallback = compile_variant(
        "component-fallback",
        json!({
            "max_set_patterns_per_set": 1,
            "max_set_components_per_fallback_set": 2,
        }),
    )
    .expect("bounded component fallback");
    assert_eq!(
        component_fallback.selected_layout,
        ProviderGraphV4Layout::Pattern
    );
    assert_eq!(component_fallback.observe.pattern_overflow_set_count, 1);
    assert_eq!(
        component_fallback
            .observe
            .maximum_components_per_pattern_overflow_set,
        2
    );
    assert_eq!(
        component_fallback
            .observe
            .unsafe_pattern_component_set_count,
        0
    );

    let exact_prefix = compile_variant(
        "exact-prefix",
        json!({
            "max_set_patterns_per_set": 1,
            "max_set_components_per_fallback_set": 1,
            "max_online_source_owners_per_set": 1,
        }),
    )
    .expect("exact prefix fallback");
    assert_eq!(exact_prefix.selected_layout, ProviderGraphV4Layout::Pattern);
    assert_eq!(exact_prefix.observe.pattern_component_over_cap_set_count, 1);
    assert_eq!(
        exact_prefix
            .observe
            .pattern_component_over_cap_prefix_covered_set_count,
        1
    );

    let rejected = compile_variant(
        "unbounded",
        json!({
            "max_set_patterns_per_set": 1,
            "max_set_components_per_fallback_set": 1,
            "max_npi_prefix_override_bytes": 1,
        }),
    )
    .expect_err("unbounded layouts must fail closed");
    assert!(rejected
        .to_string()
        .contains("no bounded complete online representation"));

    let taxonomy_rejection = compile_variant(
        "taxonomy-rejection",
        json!({
            "max_online_candidate_pattern_projection_members": 1,
        }),
    )
    .expect("direct taxonomy fallback");
    assert_eq!(
        taxonomy_rejection.selected_layout,
        ProviderGraphV4Layout::Direct
    );
    assert!(taxonomy_rejection.direct_inferred_taxonomy_eligible);
    assert!(!taxonomy_rejection.pattern_inferred_taxonomy_eligible);
    assert_eq!(
        taxonomy_rejection.pattern_inferred_taxonomy_rejection_cap,
        Some(1)
    );
}

#[test]
fn npi_scope_cli_rejects_unbound_and_malformed_scope_artifacts() {
    let cases: [(&str, ManifestMutator); 10] = [
        ("blank-shard", |manifest| {
            manifest["shards"][0]["shard_id"] = json!(" ")
        }),
        ("contradictory-shard", |manifest| {
            manifest["shards"][0]["provider_npi_scope"]["metadata"]["shard_id"] =
                json!("other-shard")
        }),
        ("mismatched-shard", |manifest| {
            manifest["shards"][0]["provider_npi_scope"]["metadata"]["source_shard_id"] =
                json!("other-shard")
        }),
        ("reciprocal-format", |manifest| {
            manifest["shards"][0]["provider_npi_group"]["metadata"]["record_format"] =
                json!("not-dense")
        }),
        ("reciprocal-global-count", |manifest| {
            manifest["shards"][0]["provider_npi_group"]["metadata"]
                .as_object_mut()
                .expect("reciprocal metadata")
                .remove("member_global_count");
        }),
        ("binding-contract", |manifest| {
            manifest["shards"][0]["provider_npi_scope"]["metadata"]["binding_contract"] =
                json!("wrong")
        }),
        ("binding-digest", |manifest| {
            manifest["shards"][0]["provider_npi_scope"]["metadata"]["binding_sha256"] =
                json!("00".repeat(32))
        }),
        ("byte-count", |manifest| {
            manifest["shards"][0]["provider_npi_scope"]["metadata"]["byte_count"] = json!(1)
        }),
        ("checksum", |manifest| {
            manifest["shards"][0]["provider_npi_scope"]["metadata"]["sha256"] =
                json!("00".repeat(32))
        }),
        ("duplicate-shard", |manifest| {
            let duplicate = manifest["shards"][0].clone();
            manifest["shards"]
                .as_array_mut()
                .expect("provider graph shards")
                .push(duplicate);
        }),
    ];
    for (name, mutate) in cases {
        let temporary = tempfile::tempdir().expect("temporary invalid scope fixture");
        let mut manifest = compiler_manifest(temporary.path());
        mutate(&mut manifest);
        let scope_manifest =
            write_npi_scope_manifest(temporary.path(), name, manifest["shards"].clone());
        let completed = run(&[
            "--extract-npi-scope",
            scope_manifest
                .to_str()
                .expect("UTF-8 invalid scope manifest"),
        ]);
        assert!(
            !completed.status.success(),
            "{name} unexpectedly passed: {}",
            String::from_utf8_lossy(&completed.stdout),
        );
        assert!(
            !temporary
                .path()
                .join(format!("{name}-npi-scope.copy"))
                .exists(),
            "{name} retained a partial output",
        );
    }
}

#[test]
fn npi_scope_cli_rejects_authenticated_copy_wire_corruption() {
    let cases: [(&str, CopyBytesMutator); 6] = [
        ("header", |bytes| bytes[0] ^= 0xff),
        ("field-count", |bytes| {
            bytes[19..21].copy_from_slice(&2i16.to_be_bytes())
        }),
        ("field-width", |bytes| {
            bytes[21..25].copy_from_slice(&7i32.to_be_bytes())
        }),
        ("low-npi", |bytes| {
            bytes[25..33].copy_from_slice(&999_999_999u64.to_be_bytes())
        }),
        ("trailer", |bytes| {
            let last = bytes.len() - 1;
            bytes[last] = 0;
        }),
        ("trailing", |bytes| bytes.push(0)),
    ];
    for (name, mutate) in cases {
        let temporary = tempfile::tempdir().expect("temporary corrupted scope fixture");
        let mut manifest = compiler_manifest(temporary.path());
        let scope_path = PathBuf::from(
            manifest["shards"][0]["provider_npi_scope"]["path"]
                .as_str()
                .expect("provider NPI scope path"),
        );
        let mut bytes = fs::read(&scope_path).expect("read provider NPI scope");
        mutate(&mut bytes);
        fs::write(&scope_path, bytes).expect("write corrupted provider NPI scope");
        refresh_provider_npi_scope_integrity(&mut manifest["shards"][0]);
        let scope_manifest =
            write_npi_scope_manifest(temporary.path(), name, manifest["shards"].clone());
        let completed = run(&[
            "--extract-npi-scope",
            scope_manifest
                .to_str()
                .expect("UTF-8 corrupted scope manifest"),
        ]);
        assert!(
            !completed.status.success(),
            "{name} unexpectedly passed: {}",
            String::from_utf8_lossy(&completed.stdout),
        );
        assert!(
            !temporary
                .path()
                .join(format!("{name}-npi-scope.copy"))
                .exists(),
            "{name} retained a partial output",
        );
    }
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

    #[cfg(target_os = "linux")]
    {
        use std::ffi::OsString;
        use std::os::unix::ffi::OsStringExt;
        use std::os::unix::fs::symlink;

        let temporary = tempfile::tempdir().expect("temporary non-UTF-8 program fixture");
        let non_utf8_program = temporary
            .path()
            .join(OsString::from_vec(b"graph-\xff".to_vec()));
        symlink(
            env!("CARGO_BIN_EXE_ptg2_provider_graph_v4"),
            &non_utf8_program,
        )
        .expect("link compiler under non-UTF-8 name");
        let fallback_program = Command::new(&non_utf8_program)
            .output()
            .expect("run compiler under non-UTF-8 name");
        assert!(!fallback_program.status.success());
        assert!(String::from_utf8_lossy(&fallback_program.stderr)
            .contains("usage: ptg2_provider_graph_v4"));
    }

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

#[test]
fn compiler_cli_covers_heavy_direct_prefix_and_strict_manifest_boundaries() {
    let temporary = tempfile::tempdir().expect("temporary heavy direct fixture");
    let heavy_manifest = write_heavy_direct_manifest(temporary.path());
    let heavy = run(&[heavy_manifest
        .to_str()
        .expect("UTF-8 heavy direct manifest")]);
    assert!(
        heavy.status.success(),
        "heavy direct compiler failed: {}",
        String::from_utf8_lossy(&heavy.stderr),
    );
    let summary: Value = serde_json::from_slice(&heavy.stdout).expect("heavy direct summary");
    assert_eq!(summary["selected_layout"], "direct");
    assert_eq!(summary["observe"]["npi_prefix_worst_member_count"], 201);
    for relation in ["group_npis_exact", "group_sets_direct"] {
        assert!(summary["heavy_bitmaps"]
            .as_array()
            .expect("heavy bitmap summaries")
            .iter()
            .any(|bitmap| bitmap["relation"] == relation));
    }

    let boundary_root = temporary.path().join("boundaries");
    fs::create_dir(&boundary_root).expect("create boundary fixture");
    let boundary_manifest = write_manifest(&boundary_root);
    let original: Value =
        serde_json::from_slice(&fs::read(&boundary_manifest).expect("read boundary manifest"))
            .expect("parse boundary manifest");
    for field in [
        "member_page_bytes",
        "locator_page_bytes",
        "heavy_owner_member_threshold",
        "max_set_patterns_per_set",
        "max_set_components_per_fallback_set",
        "max_online_group_keys_per_set",
        "max_online_source_owners_per_set",
        "max_online_source_members_per_set",
        "max_online_source_pages_per_set",
        "max_online_source_bytes_per_set",
        "online_group_npi_batch_size",
        "max_online_group_npi_members_per_set",
        "max_online_group_npi_locator_pages_per_set",
        "max_online_group_npi_member_pages_per_set",
        "max_online_group_npi_bytes_per_set",
        "max_online_group_npi_batches_per_set",
        "provider_expansion_rate_page_rows",
        "max_online_provider_expansion_rate_rows",
        "max_online_provider_expansion_provider_sets",
        "max_online_provider_expansion_graph_batches",
        "npi_prefix_target",
        "max_npi_prefix_override_owners",
        "max_npi_prefix_override_bytes",
        "max_estimated_model_bytes",
        "max_factor_edges",
    ] {
        let mut invalid = original.clone();
        invalid["options"][field] = json!(0);
        let output = run_manifest_variant(&boundary_root, &format!("zero-{field}"), invalid);
        assert!(
            !output.status.success(),
            "zero-valued option unexpectedly succeeded: {field}"
        );
        assert!(String::from_utf8_lossy(&output.stderr).contains("must be"));
    }

    for (name, field, value) in [
        ("short-digest", "sha256", json!("short")),
        ("wrong-size", "byte_count", json!(0)),
        ("wrong-digest", "sha256", json!("00".repeat(32))),
        ("wrong-format", "record_format", json!("wrong")),
        ("wrong-owners", "owner_count", json!(0)),
        ("wrong-members", "member_count", json!(0)),
        ("wrong-dictionary", "member_global_count", json!(0)),
    ] {
        let mut invalid = original.clone();
        invalid["shards"][0]["provider_set_component"]["metadata"][field] = value;
        let output = run_manifest_variant(&boundary_root, name, invalid);
        assert!(
            !output.status.success(),
            "invalid membership metadata unexpectedly succeeded: {name}"
        );
        assert!(String::from_utf8_lossy(&output.stderr).contains("PTG2_PROVIDER_GRAPH_V4_ERROR"));
    }

    let mut missing = original.clone();
    missing["shards"][0]["provider_set_component"]["path"] =
        json!(boundary_root.join("missing.sidecar"));
    let missing_output = run_manifest_variant(&boundary_root, "missing-sidecar", missing);
    assert!(!missing_output.status.success());
    assert!(String::from_utf8_lossy(&missing_output.stderr).contains("unavailable"));

    let mut blank_shard = original.clone();
    blank_shard["shards"][0]["shard_id"] = json!(" ");
    let blank_output = run_manifest_variant(&boundary_root, "blank-shard", blank_shard);
    assert!(!blank_output.status.success());
    assert!(String::from_utf8_lossy(&blank_output.stderr).contains("non-empty and unique"));

    let mut duplicate_shard = original.clone();
    duplicate_shard["shards"] =
        json!([original["shards"][0].clone(), original["shards"][0].clone()]);
    let duplicate_output = run_manifest_variant(&boundary_root, "duplicate-shard", duplicate_shard);
    assert!(!duplicate_output.status.success());
    assert!(String::from_utf8_lossy(&duplicate_output.stderr).contains("non-empty and unique"));

    let mut contradictory = original.clone();
    contradictory["shards"][0]["provider_set_component"]["metadata"]["shard_id"] =
        json!("other-shard");
    let contradictory_output =
        run_manifest_variant(&boundary_root, "contradictory-shard", contradictory);
    assert!(!contradictory_output.status.success());
    assert!(
        String::from_utf8_lossy(&contradictory_output.stderr).contains("contradictory shard IDs")
    );

    let mut mismatched = original;
    mismatched["shards"][0]["provider_set_component"]["metadata"]["source_shard_id"] =
        json!("other-shard");
    let mismatched_output = run_manifest_variant(&boundary_root, "mismatched-shard", mismatched);
    assert!(!mismatched_output.status.success());
    assert!(String::from_utf8_lossy(&mismatched_output.stderr).contains("does not match bundle"));
}
