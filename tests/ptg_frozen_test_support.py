"""Shared deterministic fixtures for frozen multipart PTG tests."""

from __future__ import annotations

import hashlib

from process.ptg_parts.frozen_rate_files import (
    FROZEN_RATE_FILE_SET_CONTRACT,
    frozen_rate_file_set_sha256,
)
from process.ptg_parts.domain import (
    PTG2HeadMetadata,
    PTG2LogicalArtifact,
    PTG2RawArtifact,
)


def frozen_descriptor_by_ordinal(ordinal: int) -> dict[str, object]:
    return {
        "source_type": "in_network",
        "canonical_url": (
            f"https://rates.example.com/2026-07/part-{ordinal:03}.json.gz"
        ),
        "content_length": 10_000 + ordinal,
        "etag": f'"part-{ordinal:03}-v1"',
        "last_modified": "Mon, 27 Jul 2026 10:00:00 GMT",
        "raw_sha256": hashlib.sha256(f"raw:{ordinal}".encode()).hexdigest(),
        "logical_sha256": hashlib.sha256(
            f"logical:{ordinal}".encode()
        ).hexdigest(),
        "logical_hash_deferred": False,
        "engine_source_identity_hash": f"{ordinal:016x}",
        "engine_source_file_version_id": f"{ordinal + 1024:016x}",
        "ordinal": ordinal,
    }


def frozen_rate_file_set(
    count: int,
) -> tuple[list[dict[str, object]], str]:
    frozen_rate_files = [
        frozen_descriptor_by_ordinal(ordinal)
        for ordinal in range(1, count + 1)
    ]
    return (
        frozen_rate_files,
        frozen_rate_file_set_sha256(frozen_rate_files),
    )


def protected_control_payload(count: int = 2) -> dict[str, object]:
    frozen_rate_files, frozen_set_digest = frozen_rate_file_set(count)
    source_file_import_id = "source-file-import-001"
    params_by_name = {
        "source_file_import_id": source_file_import_id,
        "import_id": source_file_import_id,
        "source_key": "source-a",
        "import_month": "2026-07",
        "plan_ids": ["plan-b", "plan-a", "plan-a"],
        "plan_market_types": ["Group", "group"],
        "frozen_rate_file_set_contract": FROZEN_RATE_FILE_SET_CONTRACT,
        "frozen_rate_files": frozen_rate_files,
        "frozen_rate_file_set_sha256": frozen_set_digest,
        "frozen_rate_file_count": count,
    }
    return {
        "importer": "ptg",
        "source_file_import_id": source_file_import_id,
        "import_id": source_file_import_id,
        "params": params_by_name,
    }


def frozen_artifacts(
    descriptor_by_field: dict[str, object],
    temporary_path,
) -> tuple[PTG2RawArtifact, PTG2LogicalArtifact]:
    raw_path = temporary_path / "raw.json"
    logical_path = temporary_path / "logical.json"
    raw_path.write_bytes(b"raw")
    logical_path.write_bytes(b"logical")
    raw_artifact = PTG2RawArtifact(
        original_url=str(descriptor_by_field["canonical_url"]),
        canonical_url=str(descriptor_by_field["canonical_url"]),
        raw_path=str(raw_path),
        raw_storage_uri=str(raw_path),
        raw_sha256=str(descriptor_by_field["raw_sha256"]),
        byte_count=int(descriptor_by_field["content_length"]),
        head=PTG2HeadMetadata(
            url=str(descriptor_by_field["canonical_url"]),
            status=200,
            etag=str(descriptor_by_field["etag"]),
            content_length=int(descriptor_by_field["content_length"]),
            last_modified=str(descriptor_by_field["last_modified"]),
            supports_head=True,
        ),
    )
    logical_artifact = PTG2LogicalArtifact(
        logical_path=str(logical_path),
        logical_sha256=str(descriptor_by_field["logical_sha256"]),
        byte_count=20_000,
    )
    return raw_artifact, logical_artifact


__all__ = [
    "frozen_artifacts",
    "frozen_descriptor_by_ordinal",
    "frozen_rate_file_set",
    "protected_control_payload",
]
