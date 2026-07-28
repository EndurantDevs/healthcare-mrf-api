"""Shared deterministic fixtures for frozen multipart PTG tests."""

from __future__ import annotations

import hashlib

from process.ptg_parts.frozen_rate_files import (
    FROZEN_RATE_FILE_PROOF_CONTRACT,
    FROZEN_RATE_FILE_SET_CONTRACT,
    frozen_observed_logical_sha256,
    frozen_rate_file_proof_sha256,
    frozen_rate_file_set_sha256,
)
from process.ptg_parts.frozen_rate_binding import (
    FROZEN_RATE_FILE_BINDING_OPTION,
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
        logical_sha256=frozen_observed_logical_sha256(
            descriptor_by_field
        ),
        byte_count=20_000,
    )
    return raw_artifact, logical_artifact


def frozen_candidate_evidence(
    params_by_name: dict[str, object],
    binding_by_name: dict[str, object],
) -> tuple[dict[str, object], list[dict[str, object]]]:
    descriptors = params_by_name["frozen_rate_files"]
    assert isinstance(descriptors, list)
    proof_rows = [
        {
            "contract": FROZEN_RATE_FILE_PROOF_CONTRACT,
            **descriptor,
            "raw_byte_count": descriptor["content_length"],
            "verification_mode": "downloaded",
        }
        for descriptor in descriptors
    ]
    return (
        _frozen_candidate_manifest(
            params_by_name,
            binding_by_name,
            descriptors,
            proof_rows,
        ),
        _frozen_candidate_sources(descriptors),
    )


def _frozen_candidate_manifest(
    params_by_name,
    binding_by_name,
    descriptors,
    proof_rows,
) -> dict[str, object]:
    return {
        "source_file_import_id": params_by_name["source_file_import_id"],
        "frozen_rate_file_set_contract": params_by_name[
            "frozen_rate_file_set_contract"
        ],
        "frozen_rate_files": descriptors,
        "frozen_rate_file_set_sha256": params_by_name[
            "frozen_rate_file_set_sha256"
        ],
        "frozen_rate_file_count": params_by_name[
            "frozen_rate_file_count"
        ],
        "frozen_rate_file_proof": proof_rows,
        "frozen_rate_file_proof_sha256": (
            frozen_rate_file_proof_sha256(proof_rows)
        ),
        "source_file_versions": [
            {
                **descriptor,
                "url": descriptor["canonical_url"],
                "logical_sha256": frozen_observed_logical_sha256(
                    descriptor
                ),
                "raw_byte_count": descriptor["content_length"],
                "verification_mode": "downloaded",
            }
            for descriptor in descriptors
        ],
        FROZEN_RATE_FILE_BINDING_OPTION: binding_by_name,
    }


def _frozen_candidate_sources(
    descriptors,
) -> list[dict[str, object]]:
    return [
        {
            "source_key": ordinal,
            "raw_container_sha256": descriptor["raw_sha256"],
            "source_file_version_count": 1,
            "source_file_version_id": descriptor[
                "engine_source_file_version_id"
            ],
            "version_source_identity_hash": descriptor[
                "engine_source_identity_hash"
            ],
            "version_source_type": descriptor["source_type"],
            "version_canonical_url": descriptor["canonical_url"],
            "version_raw_sha256": descriptor["raw_sha256"],
            "version_logical_sha256": frozen_observed_logical_sha256(
                descriptor
            ),
            "version_content_length": descriptor["content_length"],
            "version_etag": descriptor["etag"],
            "version_last_modified": descriptor["last_modified"],
            "version_verification_mode": "downloaded",
            "version_payload": {
                "raw_byte_count": descriptor["content_length"],
                "logical_hash_deferred": descriptor[
                    "logical_hash_deferred"
                ],
            },
        }
        for ordinal, descriptor in enumerate(descriptors)
    ]


__all__ = [
    "frozen_artifacts",
    "frozen_candidate_evidence",
    "frozen_descriptor_by_ordinal",
    "frozen_rate_file_set",
    "protected_control_payload",
]
