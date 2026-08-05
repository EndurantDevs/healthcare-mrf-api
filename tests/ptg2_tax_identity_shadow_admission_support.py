from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any


POLICY_ID = "ptg-tin-hmac-sha256-v1:synthetic-shadow"
ROW_COUNT = 6
V1_COUNTS = {
    "matched_ein_count": 1,
    "missing_count": 1,
    "malformed_count": 1,
    "unsupported_type_count": 3,
}
V2_COUNTS = {
    "matched_ein_count": 1,
    "matched_npi_count": 1,
    "missing_count": 1,
    "malformed_count": 2,
    "unsupported_type_count": 1,
}


def sidecar_bytes(
    version: int,
    *,
    policy_id: str = POLICY_ID,
    row_count: int = ROW_COUNT,
    payload_seed: int = 0,
) -> bytes:
    """Build a framed synthetic sidecar; row semantics remain Rust-owned."""

    magic = b"PTG2TAX1" if version == 1 else b"PTG2TAX2"
    policy_bytes = policy_id.encode("ascii")
    header = (
        magic
        + version.to_bytes(2, "little")
        + (65).to_bytes(2, "little")
        + bytes((len(policy_bytes),))
        + policy_bytes
    )
    records = []
    for index in range(row_count):
        marker = (payload_seed + version + index) % 256
        records.append(index.to_bytes(4, "little") + bytes((marker,)) * 61)
    return header + b"".join(records)


def descriptor_for(
    path: Path,
    version: int,
    *,
    policy_id: str = POLICY_ID,
    counts: dict[str, int] | None = None,
) -> dict[str, Any]:
    """Describe one already-written synthetic artifact exactly."""

    artifact_bytes = path.read_bytes()
    descriptor_dict: dict[str, Any] = {
        "path": str(path),
        "bytes": len(artifact_bytes),
        "row_count": ROW_COUNT,
        "provider_group_count": ROW_COUNT,
        "format": f"ptg2_provider_group_tax_identity_v{version}",
        "version": version,
        "record_bytes": 65,
        "token_policy_id": policy_id,
        "hmac_contract": "hmac_sha256_ptg_tin_v1",
        "sha256": hashlib.sha256(artifact_bytes).hexdigest(),
        "final": True,
    }
    if version == 1:
        descriptor_dict.update(V1_COUNTS if counts is None else counts)
        descriptor_dict["normalization_contract"] = (
            "ein_ascii_digits_or_2_7_hyphen_v1"
        )
    else:
        descriptor_dict.update(V2_COUNTS if counts is None else counts)
        descriptor_dict.update(
            {
                "normalization_contract": (
                    "ein_ascii_digits_or_2_7_hyphen_and_npi_10_ascii_digits_"
                    "cms_80840_luhn_v2"
                ),
                "token_message_contract": (
                    "healthporta_ptg_tin_v1_nul_u16be_type_length_type_"
                    "u16be_value_length_value"
                ),
                "tin_id_128_contract": "first_16_bytes(tin_hmac_sha256)",
                "full_hmac_authority_contract": (
                    "tin_hmac_sha256_full_32_bytes_authoritative"
                ),
            }
        )
    return descriptor_dict


def make_sidecar_pair(
    tmp_path: Path,
    *,
    directory_name: str = "shadow",
    policy_id: str = POLICY_ID,
    payload_seed: int = 0,
) -> tuple[Path, dict[str, Any], dict[str, Any]]:
    """Create one current-UID private scratch directory and exact descriptors."""

    scratch_root = tmp_path / directory_name
    scratch_root.mkdir(mode=0o700)
    v1_path = scratch_root / "tax-v1.bin"
    v2_path = scratch_root / "tax-v2.bin"
    v1_path.write_bytes(sidecar_bytes(1, policy_id=policy_id, payload_seed=payload_seed))
    v2_path.write_bytes(sidecar_bytes(2, policy_id=policy_id, payload_seed=payload_seed))
    return (
        scratch_root,
        descriptor_for(v1_path, 1, policy_id=policy_id),
        descriptor_for(v2_path, 2, policy_id=policy_id),
    )


def refresh_descriptor(descriptor: dict[str, Any]) -> None:
    """Refresh only exact bytes and digest after a synthetic file mutation."""

    data = Path(descriptor["path"]).read_bytes()
    descriptor["bytes"] = len(data)
    descriptor["sha256"] = hashlib.sha256(data).hexdigest()
