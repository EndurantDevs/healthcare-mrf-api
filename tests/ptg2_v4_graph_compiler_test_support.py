from __future__ import annotations

import hashlib
import os
from pathlib import Path
import struct
import subprocess
from typing import Any

from process.ptg_parts import ptg2_v4_graph_compiler as compiler

_STANDARD_FORMAT = (
    "magic8:uint32_le_version:uint64_le_entry_count:"
    "index(owner16:uint64_le_offset:uint32_le_count):members16"
)
_DENSE_FORMAT = (
    "magic8:uint32_le_version:uint64_le_entry_count:"
    "uint64_le_member_global_count:"
    "index(owner16:uint64_le_offset:uint32_le_count):"
    "member_globals16:members_uint32_le"
)
_PG_COPY_HEADER = b"PGCOPY\n\xff\r\n\0" + struct.pack(">II", 0, 0)
_NPI_SCOPE_FORMAT = "ptg2_provider_npi_scope_pg_binary_int8_v1"
_NPI_SCOPE_BINDING_CONTRACT = "provider_npi_scope_to_provider_npi_group_v1"
_NPI_SCOPE_BINDING_DOMAIN = b"ptg2:v4:provider-npi-scope-binding:v1\x00"
_NPI_SCOPE_SHARD_BINDING_CONTRACT = "provider_npi_scope_shard_binding_v1"
_NPI_SCOPE_SHARD_BINDING_DOMAIN = b"ptg2:v4:provider-npi-scope-shard-binding:v1\x00"
_NPI_SCOPE_RETENTION_CONTRACT = "shared_v4_publication_scratch_v1"
_TAXONOMY_RULE_SET_DOMAIN = b"ptg2:v4:inferred-taxonomy-rule-set:v1\x00"


def _global(domain: int, value: int) -> bytes:
    return bytes([domain]) + bytes(7) + value.to_bytes(8, "big")


def _npi(value: int) -> bytes:
    return bytes(8) + value.to_bytes(8, "big")


def _write_membership(
    path: Path,
    *,
    name: str,
    shard_id: str,
    pairs: list[tuple[bytes, bytes]],
    dense: bool = False,
) -> dict[str, object]:
    """Write one deterministic membership sidecar and return its manifest."""
    by_owner: dict[bytes, set[bytes]] = {}
    for owner, member in pairs:
        by_owner.setdefault(owner, set()).add(member)
    memberships = [
        (owner, sorted(members)) for owner, members in sorted(by_owner.items())
    ]
    member_dictionary = (
        sorted({member for _owner, members in memberships for member in members})
        if dense
        else []
    )
    membership_payload = bytearray(b"PTG2MNDS" if dense else b"PTG2MNSC")
    membership_payload.extend(struct.pack("<IQ", 1, len(memberships)))
    if dense:
        membership_payload.extend(struct.pack("<Q", len(member_dictionary)))
    offset = 0
    for owner, members in memberships:
        membership_payload.extend(owner)
        membership_payload.extend(struct.pack("<QI", offset, len(members)))
        offset += len(members)
    if dense:
        for member in member_dictionary:
            membership_payload.extend(member)
        member_key_by_global = {
            member_global: member_key
            for member_key, member_global in enumerate(member_dictionary)
        }
        for _, members in memberships:
            for member in members:
                membership_payload.extend(
                    struct.pack("<I", member_key_by_global[member])
                )
    else:
        for _, members in memberships:
            for member in members:
                membership_payload.extend(member)
    path.write_bytes(membership_payload)
    return {
        "name": name,
        "source_shard_id": shard_id,
        "path": str(path),
        "record_format": _DENSE_FORMAT if dense else _STANDARD_FORMAT,
        "sha256": hashlib.sha256(membership_payload).hexdigest(),
        "byte_count": len(membership_payload),
        "owner_count": len(memberships),
        "member_count": offset,
        **({"member_global_count": len(member_dictionary)} if dense else {}),
    }


def _update_length_prefixed(digest: Any, value: bytes) -> None:
    digest.update(len(value).to_bytes(4, "big"))
    digest.update(value)


def _write_npi_scope(
    path: Path,
    *,
    shard_id: str,
    reciprocal: dict[str, object],
    npis: list[int],
) -> dict[str, object]:
    scope_copy_payload = bytearray(_PG_COPY_HEADER)
    for npi in sorted(npis):
        scope_copy_payload.extend(struct.pack(">hIq", 1, 8, npi))
    scope_copy_payload.extend(struct.pack(">h", -1))
    path.write_bytes(scope_copy_payload)
    scope_manifest_by_field: dict[str, object] = {
        "name": "provider_npi_scope",
        "source_shard_id": shard_id,
        "path": str(path),
        "record_format": _NPI_SCOPE_FORMAT,
        "sha256": hashlib.sha256(scope_copy_payload).hexdigest(),
        "byte_count": len(scope_copy_payload),
        "row_count": len(npis),
        "provider_npi_group_sha256": reciprocal["sha256"],
        "provider_npi_group_record_format": reciprocal["record_format"],
        "provider_npi_group_byte_count": reciprocal["byte_count"],
        "provider_npi_group_owner_count": reciprocal["owner_count"],
        "provider_npi_group_member_count": reciprocal["member_count"],
        "provider_npi_group_member_global_count": reciprocal["member_global_count"],
        "binding_contract": _NPI_SCOPE_BINDING_CONTRACT,
        "shard_binding_contract": _NPI_SCOPE_SHARD_BINDING_CONTRACT,
        "retention_contract": _NPI_SCOPE_RETENTION_CONTRACT,
    }
    binding = hashlib.sha256()
    binding.update(_NPI_SCOPE_BINDING_DOMAIN)
    _update_length_prefixed(binding, _NPI_SCOPE_FORMAT.encode("ascii"))
    binding.update(bytes.fromhex(str(scope_manifest_by_field["sha256"])))
    binding.update(int(scope_manifest_by_field["byte_count"]).to_bytes(8, "big"))
    binding.update(int(scope_manifest_by_field["row_count"]).to_bytes(8, "big"))
    binding.update(bytes.fromhex(str(reciprocal["sha256"])))
    _update_length_prefixed(
        binding,
        str(reciprocal["record_format"]).encode("ascii"),
    )
    for field_name in (
        "byte_count",
        "owner_count",
        "member_count",
        "member_global_count",
    ):
        binding.update(int(reciprocal[field_name]).to_bytes(8, "big"))
    scope_manifest_by_field["binding_sha256"] = binding.hexdigest()
    shard_binding = hashlib.sha256()
    shard_binding.update(_NPI_SCOPE_SHARD_BINDING_DOMAIN)
    shard_binding.update(binding.digest())
    _update_length_prefixed(shard_binding, shard_id.encode("utf-8"))
    scope_manifest_by_field["shard_binding_sha256"] = shard_binding.hexdigest()
    return scope_manifest_by_field


def _write_tax_identity(
    path: Path,
    *,
    shard_id: str,
    tax_observations: list[tuple[bytes, int, bytes | None]],
    policy_id: str = "ptg-tin-hmac-sha256-v1:release-1",
) -> dict[str, object]:
    artifact_bytes = bytearray(b"PTG2TAX1")
    artifact_bytes.extend(struct.pack("<HHB", 1, 65, len(policy_id)))
    artifact_bytes.extend(policy_id.encode("ascii"))
    count_by_state = {1: 0, 2: 0, 3: 0, 4: 0}
    for group, state, hmac in sorted(tax_observations):
        token = hmac or bytes(32)
        artifact_bytes.extend(group)
        artifact_bytes.append(state)
        artifact_bytes.extend(token[:16])
        artifact_bytes.extend(token)
        count_by_state[state] += 1
    path.write_bytes(artifact_bytes)
    return {
        "name": "provider_group_tax_identity",
        "source_shard_id": shard_id,
        "path": str(path),
        "record_format": "ptg2_provider_group_tax_identity_v1",
        "sha256": hashlib.sha256(artifact_bytes).hexdigest(),
        "byte_count": len(artifact_bytes),
        "row_count": len(tax_observations),
        "provider_group_count": len(tax_observations),
        "matched_ein_count": count_by_state[1],
        "missing_count": count_by_state[2],
        "malformed_count": count_by_state[3],
        "unsupported_type_count": count_by_state[4],
        "version": 1,
        "record_bytes": 65,
        "token_policy_id": policy_id,
        "normalization_contract": "ein_ascii_digits_or_2_7_hyphen_v1",
        "hmac_contract": "hmac_sha256_ptg_tin_v1",
        "final": True,
    }


def compiler_fixture(
    tmp_path: Path,
    *,
    shard_id: str = "shard-a",
    tax_policy_id: str = "ptg-tin-hmac-sha256-v1:release-1",
) -> tuple[list[dict[str, object]], Path]:
    tmp_path.mkdir(parents=True, exist_ok=True)
    provider_set = _global(1, 1)
    component = _global(2, 1)
    groups = [_global(3, 1), _global(3, 2)]
    provider_npi = _npi(1_234_567_890)
    npi_group = _write_membership(
        tmp_path / "npi-group.sidecar",
        name="provider_npi_group",
        shard_id=shard_id,
        pairs=[(provider_npi, group) for group in groups],
        dense=True,
    )
    artifacts = [
        _write_membership(
            tmp_path / "set-component.sidecar",
            name="provider_set_component",
            shard_id=shard_id,
            pairs=[(provider_set, component)],
        ),
        _write_membership(
            tmp_path / "component-group.sidecar",
            name="provider_component_group",
            shard_id=shard_id,
            pairs=[(component, group) for group in groups],
        ),
        _write_membership(
            tmp_path / "group-npi.sidecar",
            name="provider_group_npi",
            shard_id=shard_id,
            pairs=[(group, provider_npi) for group in groups],
        ),
        npi_group,
        _write_npi_scope(
            tmp_path / "npi-scope.copy",
            shard_id=shard_id,
            reciprocal=npi_group,
            npis=[1_234_567_890],
        ),
        _write_tax_identity(
            tmp_path / "group-tax-identity.sidecar",
            shard_id=shard_id,
            policy_id=tax_policy_id,
            tax_observations=[
                (groups[0], 1, bytes.fromhex("11" * 32)),
                (groups[1], 2, None),
            ],
        ),
    ]
    provider_map = tmp_path / "provider-set-map.tsv"
    provider_map.write_text(f"{provider_set.hex()}\t1\n")
    return artifacts, provider_map


async def compiler_inputs(
    tmp_path: Path,
    artifacts: list[dict[str, object]],
) -> tuple[compiler.V4GraphNpiScopePreparation, dict[str, object]]:
    scope = await compiler.prepare_provider_graph_v4_npi_scope_rust(
        graph_artifact_entries=artifacts,
        output_path=tmp_path / "prepared-npi-scope.copy",
        binary_path=scanner_binary(),
    )
    rule_digest = hashlib.sha256(b"fixture-rule").digest()
    members_path = tmp_path / "taxonomy-members.u32le"
    member_payload = struct.pack("<I", 0)
    members_path.write_bytes(member_payload)
    rule_set = hashlib.sha256()
    rule_set.update(_TAXONOMY_RULE_SET_DOMAIN)
    rule_set.update((1).to_bytes(4, "big"))
    rule_set.update(rule_digest)
    taxonomy_input_by_field: dict[str, object] = {
        "contract": "ptg2_v4_inferred_taxonomy_compiler_input_v1",
        "catalog_contract": "snapshot_npi_live_catalog_individual_v1",
        "vector_format": "sorted_u32le_v1",
        "npi_scope_sha256": scope.manifest["output_sha256"],
        "rule_set_digest": rule_set.hexdigest(),
        "members": {
            "path": str(members_path),
            "byte_count": len(member_payload),
            "sha256": hashlib.sha256(member_payload).hexdigest(),
        },
        "rules": [
            {
                "rule_digest": rule_digest.hex(),
                "catalog_digest": hashlib.sha256(b"fixture-catalog").hexdigest(),
                "member_count": 1,
                "member_offset_bytes": 0,
                "member_byte_count": len(member_payload),
            }
        ],
    }
    return scope, taxonomy_input_by_field


def compiler_manifest_inputs(
    tmp_path: Path,
) -> tuple[dict[str, object], dict[str, object]]:
    scope_path = tmp_path / "manifest-npi-scope.copy"
    scope_payload = bytearray(_PG_COPY_HEADER)
    scope_payload.extend(struct.pack(">hIiIq", 2, 4, 0, 8, 1_234_567_890))
    scope_payload.extend(struct.pack(">h", -1))
    scope_path.write_bytes(scope_payload)
    scope_digest = hashlib.sha256(scope_payload).hexdigest()
    scope_manifest_by_field = {
        "format": "ptg2_provider_graph_v4_npi_scope_v1",
        "row_count": 1,
        "source_owner_count": 1,
        "input_byte_count": 1,
        "input_sha256": hashlib.sha256(b"fixture-scope-input").hexdigest(),
        "output_byte_count": len(scope_payload),
        "output_sha256": scope_digest,
        "output_path": str(scope_path),
    }
    rule_digest = hashlib.sha256(b"fixture-rule").digest()
    members_path = tmp_path / "manifest-taxonomy-members.u32le"
    member_payload = struct.pack("<I", 0)
    members_path.write_bytes(member_payload)
    rule_set = hashlib.sha256()
    rule_set.update(_TAXONOMY_RULE_SET_DOMAIN)
    rule_set.update((1).to_bytes(4, "big"))
    rule_set.update(rule_digest)
    taxonomy_manifest_by_field = {
        "contract": "ptg2_v4_inferred_taxonomy_compiler_input_v1",
        "catalog_contract": "snapshot_npi_live_catalog_individual_v1",
        "vector_format": "sorted_u32le_v1",
        "npi_scope_sha256": scope_digest,
        "rule_set_digest": rule_set.hexdigest(),
        "members": {
            "path": str(members_path),
            "byte_count": len(member_payload),
            "sha256": hashlib.sha256(member_payload).hexdigest(),
        },
        "rules": [
            {
                "rule_digest": rule_digest.hex(),
                "catalog_digest": hashlib.sha256(b"fixture-catalog").hexdigest(),
                "member_count": 1,
                "member_offset_bytes": 0,
                "member_byte_count": len(member_payload),
            }
        ],
    }
    return scope_manifest_by_field, taxonomy_manifest_by_field


def scanner_binary() -> Path:
    root = Path(__file__).resolve().parents[1]
    scanner_root = root / "support" / "ptg2_scanner"
    target_root = Path(os.getenv("CARGO_TARGET_DIR", scanner_root / "target"))
    if not target_root.is_absolute():
        target_root = root / target_root
    subprocess.run(
        [
            "cargo",
            "build",
            "--locked",
            "--bin",
            "ptg2_provider_graph_v4",
            "--manifest-path",
            str(scanner_root / "Cargo.toml"),
        ],
        check=True,
        cwd=root,
        timeout=120,
    )
    candidate = target_root / "debug" / "ptg2_provider_graph_v4"
    if not candidate.is_file() or not os.access(candidate, os.X_OK):
        raise RuntimeError("PTG2 V4 graph compiler test binary was not built")
    return candidate
