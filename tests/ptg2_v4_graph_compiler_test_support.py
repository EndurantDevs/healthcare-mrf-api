from __future__ import annotations

import hashlib
from pathlib import Path
import struct

_STANDARD_FORMAT = (
    "magic8:uint32_le_version:uint64_le_entry_count:"
    "index(owner16:uint64_le_offset:uint32_le_count):members16"
)


def _global(domain: int, value: int) -> bytes:
    return bytes([domain]) + bytes(7) + value.to_bytes(8, "big")


def _npi(value: int) -> bytes:
    return bytes(8) + value.to_bytes(8, "big")


def _write_membership(
    path: Path, *, name: str, shard_id: str, pairs: list[tuple[bytes, bytes]]
) -> dict[str, object]:
    by_owner: dict[bytes, set[bytes]] = {}
    for owner, member in pairs:
        by_owner.setdefault(owner, set()).add(member)
    memberships = [
        (owner, sorted(members)) for owner, members in sorted(by_owner.items())
    ]
    membership_payload = bytearray(b"PTG2MNSC")
    membership_payload.extend(struct.pack("<IQ", 1, len(memberships)))
    offset = 0
    for owner, members in memberships:
        membership_payload.extend(owner)
        membership_payload.extend(struct.pack("<QI", offset, len(members)))
        offset += len(members)
    for _, members in memberships:
        for member in members:
            membership_payload.extend(member)
    path.write_bytes(membership_payload)
    return {
        "name": name,
        "source_shard_id": shard_id,
        "path": str(path),
        "record_format": _STANDARD_FORMAT,
        "sha256": hashlib.sha256(membership_payload).hexdigest(),
        "byte_count": len(membership_payload),
        "owner_count": len(memberships),
        "member_count": offset,
    }


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


def compiler_fixture(tmp_path: Path) -> tuple[list[dict[str, object]], Path]:
    shard_id = "shard-a"
    provider_set = _global(1, 1)
    component = _global(2, 1)
    groups = [_global(3, 1), _global(3, 2)]
    provider_npi = _npi(1_234_567_890)
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
        _write_membership(
            tmp_path / "npi-group.sidecar",
            name="provider_npi_group",
            shard_id=shard_id,
            pairs=[(provider_npi, group) for group in groups],
        ),
        _write_tax_identity(
            tmp_path / "group-tax-identity.sidecar",
            shard_id=shard_id,
            tax_observations=[
                (groups[0], 1, bytes.fromhex("11" * 32)),
                (groups[1], 2, None),
            ],
        ),
    ]
    provider_map = tmp_path / "provider-set-map.tsv"
    provider_map.write_text(f"{provider_set.hex()}\t1\n")
    return artifacts, provider_map


def scanner_binary() -> Path:
    return (
        Path(__file__).resolve().parents[1]
        / "support"
        / "ptg2_scanner"
        / "target"
        / "debug"
        / "ptg2_provider_graph_v4"
    )
