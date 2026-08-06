# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Synthetic PTG2TAX1 fixture construction for PostgreSQL projector proofs."""

from __future__ import annotations

import hashlib
from pathlib import Path

POLICY = "ptg-tin-hmac-sha256-v1:2026-07"


def ordinal_digest(shard_ids: tuple[str, ...]) -> bytes:
    """Return the frozen source-ordinal map digest for synthetic shards."""

    digest = hashlib.sha256()
    digest.update(b"PTG2V4TAXORD\x01")
    digest.update(len(shard_ids).to_bytes(4, "big"))
    for ordinal, shard_id in enumerate(shard_ids):
        shard_bytes = shard_id.encode("ascii")
        digest.update(len(shard_bytes).to_bytes(4, "big"))
        digest.update(shard_bytes)
        digest.update(ordinal.to_bytes(4, "big"))
    return digest.digest()


def _sidecar_record(group_byte: int, state_code: int, full_hmac: bytes) -> bytes:
    provider_group_id = bytes((group_byte,)) * 16
    if state_code == 1:
        return provider_group_id + b"\x01" + full_hmac[:16] + full_hmac
    return provider_group_id + bytes((state_code,)) + bytes(48)


def write_sidecar(
    tmp_path: Path,
    *,
    source_key: int,
    shard_id: str,
    identity_digit: str,
    state_codes: tuple[int, ...],
    matched_hmac: bytes,
) -> dict[str, object]:
    """Write one synthetic authenticated source descriptor and sidecar."""

    sidecar_records = tuple(
        _sidecar_record(group_byte, state_code, matched_hmac)
        for group_byte, state_code in zip((0x11, 0x22, 0x33, 0x44), state_codes)
    )
    sidecar_bytes = (
        b"PTG2TAX1"
        + (1).to_bytes(2, "little")
        + (65).to_bytes(2, "little")
        + bytes((len(POLICY),))
        + POLICY.encode("ascii")
        + b"".join(sidecar_records)
    )
    sidecar_path = tmp_path / f"source-{source_key}.ptg2tax"
    sidecar_path.write_bytes(sidecar_bytes)
    counts_by_state = {
        "matched_ein_count": state_codes.count(1),
        "missing_count": state_codes.count(2),
        "malformed_count": state_codes.count(3),
        "unsupported_type_count": state_codes.count(4),
    }
    return {
        "name": "provider_group_tax_identity",
        "path": str(sidecar_path),
        "record_format": "ptg2_provider_group_tax_identity_v1",
        "sha256": hashlib.sha256(sidecar_bytes).hexdigest(),
        "byte_count": len(sidecar_bytes),
        "row_count": len(sidecar_records),
        "provider_group_count": len(sidecar_records),
        **counts_by_state,
        "version": 1,
        "record_bytes": 65,
        "token_policy_id": POLICY,
        "normalization_contract": "ein_ascii_digits_or_2_7_hyphen_v1",
        "hmac_contract": "hmac_sha256_ptg_tin_v1",
        "final": True,
        "source_shard_id": shard_id,
        "physical_source_binding": {
            "contract": "ptg2_tax_identity_rate_source_binding_v1",
            "source_type": "in_network",
            "identity_kind": "logical_json_sha256_v1",
            "identity_sha256": identity_digit * 64,
            "source_key": source_key,
        },
    }
