"""Canonical signature and partition validation for exact import waves."""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import re
from typing import Any


ATTESTATION_VERSION = "healthporta.ptg-import-wave-attestation.v1"
ATTESTATION_DOMAIN = b"healthporta.ptg-import-wave-attestation.v1\0"
_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,190}$")
_HEX_64 = re.compile(r"^[0-9a-f]{64}$")


def _canonical(value: Any) -> bytes:
    try:
        return json.dumps(
            value, sort_keys=True, separators=(",", ":"), ensure_ascii=True,
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise ValueError(
            "wave contract must contain only canonical JSON values"
        ) from exc


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _identifier(value: object, name: str, limit: int) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{name} must be a string")
    if not value or value != value.strip() or len(value) > limit:
        raise ValueError(
            f"{name} must be trimmed, non-empty, and no longer than {limit}"
        )
    if not _IDENTIFIER.fullmatch(value):
        raise ValueError(f"{name} must be a worker-wave-compatible identifier")
    return value


def _digest(value: object, name: str) -> str:
    if not isinstance(value, str) or not _HEX_64.fullmatch(value):
        raise ValueError(f"{name} must be a lowercase SHA-256 digest")
    return value


def _attestation_key(explicit_key: str | bytes | None) -> bytes:
    if explicit_key is None:
        explicit_key = str(os.getenv("HLTHPRT_CONTROL_API_TOKEN") or "").strip()
    if isinstance(explicit_key, str):
        key = explicit_key.encode("utf-8")
    elif isinstance(explicit_key, bytes):
        key = explicit_key
    else:
        raise ValueError("cohort attestation key is required")
    if not key:
        # Runtime must never downgrade signature verification because a node
        # was misconfigured. Tests pass an explicit disposable key.
        raise ValueError("cohort attestation key is required")
    return key


def sign_cohort_attestation(
    unsigned_attestation: dict[str, Any],
    *,
    key: str | bytes,
) -> str:
    """Return the HMAC required by the orchestrator-to-engine contract."""

    return hmac.new(
        _attestation_key(key),
        ATTESTATION_DOMAIN + _canonical(unsigned_attestation),
        hashlib.sha256,
    ).hexdigest()


def _verify_attestation(
    attestation: object,
    *,
    attestation_key: str | bytes | None,
) -> dict[str, Any]:
    if not isinstance(attestation, dict):
        raise ValueError("cohort_attestation must be an object")
    expected_attestation_fields = {
        "schema_version", "wave_id", "idempotency_key", "snapshot",
        "partition", "intents", "signature",
    }
    if set(attestation) != expected_attestation_fields:
        raise ValueError("cohort_attestation fields are not exact")
    signature = _digest(attestation["signature"], "cohort_attestation.signature")
    unsigned_attestation_map = {
        key: field_value
        for key, field_value in attestation.items()
        if key != "signature"
    }
    if unsigned_attestation_map["schema_version"] != ATTESTATION_VERSION:
        raise ValueError("cohort_attestation schema_version is unsupported")
    expected_signature = sign_cohort_attestation(
        unsigned_attestation_map,
        key=_attestation_key(attestation_key),
    )
    if not hmac.compare_digest(signature, expected_signature):
        raise ValueError("cohort_attestation signature is invalid")
    return {**unsigned_attestation_map, "signature": signature}


def _validate_snapshot(snapshot: object) -> dict[str, Any]:
    digest_fields = {
        "snapshot_digest", "membership_digest", "inventory_digest",
        "subscription_coverage_digest", "entitlement_coverage_digest",
        "catalog_generation",
    }
    expected_snapshot_fields = digest_fields | {"entitlement_coverage_count"}
    if not isinstance(snapshot, dict) or set(snapshot) != expected_snapshot_fields:
        raise ValueError("cohort_attestation snapshot fields are not exact")
    for field in digest_fields:
        _digest(snapshot[field], f"snapshot.{field}")
    count = snapshot["entitlement_coverage_count"]
    if not isinstance(count, int) or isinstance(count, bool) or count < 1:
        raise ValueError(
            "snapshot.entitlement_coverage_count must be a positive integer"
        )
    return dict(snapshot)


def _validate_partition(partition: object) -> dict[str, Any]:
    expected_partition_fields = {
        "complete", "physical_coordinate_count", "physical_coordinate_digest",
        "imported_coordinate_count", "imported_coordinate_digest",
        "reused_coordinate_count", "reused_coordinate_digest", "partition_digest",
    }
    if not isinstance(partition, dict) or set(partition) != expected_partition_fields:
        raise ValueError("cohort_attestation partition fields are not exact")
    if partition["complete"] is not True:
        raise ValueError("cohort_attestation must prove a complete cohort")
    count = partition["physical_coordinate_count"]
    if not isinstance(count, int) or isinstance(count, bool) or count < 1:
        raise ValueError("partition physical_coordinate_count must be positive")
    _digest(
        partition["physical_coordinate_digest"],
        "partition.physical_coordinate_digest",
    )
    imported_count = partition["imported_coordinate_count"]
    reused_count = partition["reused_coordinate_count"]
    if (
        not isinstance(imported_count, int) or isinstance(imported_count, bool)
        or imported_count < 1
        or not isinstance(reused_count, int) or isinstance(reused_count, bool)
        or reused_count < 0
        or count != imported_count + reused_count
    ):
        raise ValueError("partition must prove complete = imported + reused")
    _digest(
        partition["imported_coordinate_digest"],
        "partition.imported_coordinate_digest",
    )
    _digest(
        partition["reused_coordinate_digest"],
        "partition.reused_coordinate_digest",
    )
    _digest(partition["partition_digest"], "partition.partition_digest")
    return dict(partition)
