# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Fixed-size admission receipts for finalized Provider Directory datasets.

The application validator is the admission authority for new writes. Legacy
rows take the slower path: one locked row is copied as raw JSON text and fully
revalidated without a PostgreSQL JSONB cast before its receipt is stored.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
import json
from pathlib import Path
import re
from typing import Any

from process.provider_directory_fhir_subset_canonical import (
    canonical_payload_json,
    canonical_payload_sha256,
)
from process.provider_directory_proof_store import (
    PROVIDER_DIRECTORY_CONTENT_PROOF_METADATA_KEY,
    PROVIDER_DIRECTORY_SEMANTIC_CONTENT_PROOF_CONTRACT_ID,
    PROVIDER_DIRECTORY_SEMANTIC_CONTENT_V4_PROOF_CONTRACT_ID,
)
from process.uhc_canonical_proof import (
    UHC_CANONICAL_CONTENT_PROOF_METADATA_KEY,
)


ADMISSION_SEAL_CONTRACT = "provider-directory-admission-seal-v1"
ADMISSION_SEAL_VERSION = 1
ADMISSION_KIND_GENERIC = "generic"
ADMISSION_KIND_UHC_CANONICAL = "uhc_canonical"
ADMISSION_GENERIC_PROOF_SUMMARY_KEY = (
    "provider_directory_content_proof_admission_summary_v1"
)
ADMISSION_METADATA_SUMMARY_MAX_BYTES = 1024 * 1024
ADMISSION_RAW_METADATA_MAX_BYTES = 256 * 1024 * 1024
ADMISSION_RESOURCE_TYPE_MAX_COUNT = 64
ADMISSION_RESOURCE_TYPE_MAX_BYTES = 64
ADMISSION_LEGACY_SHARD_MAX_COUNT = 1024
ADMISSION_LEGACY_METADATA_MAX_BYTES = 1024 * 1024
_CAPTURE_MAX_EVENTS = 1024 * 1024
_CAPTURE_MAX_ESTIMATED_BYTES = ADMISSION_METADATA_SUMMARY_MAX_BYTES

_COPY_SIGNATURE = b"PGCOPY\n\xff\r\n\x00"
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_PROOF_KEYS = frozenset(
    {
        PROVIDER_DIRECTORY_CONTENT_PROOF_METADATA_KEY,
        UHC_CANONICAL_CONTENT_PROOF_METADATA_KEY,
    }
)
_SEMANTIC_PROOF_CONTRACT_IDS = frozenset(
    {
        PROVIDER_DIRECTORY_SEMANTIC_CONTENT_PROOF_CONTRACT_ID,
        PROVIDER_DIRECTORY_SEMANTIC_CONTENT_V4_PROOF_CONTRACT_ID,
    }
)
_LEGACY_PROOF_FIELDS = frozenset(
    {
        "contract_id",
        "complete",
        "dataset_id",
        "endpoint_id",
        "acquisition_root_run_id",
        "source_ids",
        "selected_resources",
        "dataset_hash",
        "resource_count",
        "resource_hashes",
        "resource_counts",
        "source_metrics",
        "npi_set_sha256",
        "shard_count",
        "shard_set_sha256",
        "shards",
        "proof_sha256",
    }
)
_SEMANTIC_PROOF_FIELDS = _LEGACY_PROOF_FIELDS | {
    "proof_resource_scope",
    "resource_hash_contract",
    "semantic_projection_as_of",
    "semantic_union",
}
_DESCRIPTOR_FIELDS = frozenset(
    {
        "shard_id",
        "dataset_id",
        "endpoint_id",
        "acquisition_root_run_id",
        "source_ids",
        "resource_count",
        "resource_counts",
        "first_identity",
        "last_identity",
        "input_sha256",
        "artifact_sha256",
        "artifact_byte_count",
    }
)
_FINALIZED_STATUSES = frozenset(
    {
        "validated",
        "published",
        "superseded",
        "verification_baseline",
        "verification_mismatch",
    }
)


class AdmissionSealError(RuntimeError):
    """Fail closed when a fixed-size admission receipt cannot be proven."""


def _require_ascii_canonical_json(json_value: Any) -> None:
    """Match the legacy PostgreSQL proof canonicalizer's value domain."""

    if json_value is None or type(json_value) in {bool, int}:
        return
    if type(json_value) is str:
        try:
            json_value.encode("ascii")
        except UnicodeEncodeError as error:
            raise AdmissionSealError(
                "provider_directory_admission_proof_non_ascii"
            ) from error
        return
    if isinstance(json_value, list):
        for item in json_value:
            _require_ascii_canonical_json(item)
        return
    if isinstance(json_value, Mapping):
        for key, item in json_value.items():
            if type(key) is not str:
                raise AdmissionSealError(
                    "provider_directory_admission_proof_shape_invalid"
                )
            _require_ascii_canonical_json(key)
            _require_ascii_canonical_json(item)
        return
    raise AdmissionSealError("provider_directory_admission_proof_shape_invalid")


@dataclass(frozen=True)
class ProviderDirectoryAdmissionSeal:
    """Immutable fixed-size evidence retained after proof admission."""

    metadata_summary: dict[str, Any]
    metadata_sha256: str
    admission_version: int
    admission_kind: str
    proof_sha256: str
    resource_types: tuple[str, ...]

    def digest_envelope(self) -> dict[str, Any]:
        """Return the canonical fields bound by ``metadata_sha256``."""

        return _digest_envelope(
            self.metadata_summary,
            admission_version=self.admission_version,
            admission_kind=self.admission_kind,
            proof_sha256=self.proof_sha256,
            resource_types=self.resource_types,
        )


def _digest_envelope(
    metadata_summary: Mapping[str, Any],
    *,
    admission_version: int,
    admission_kind: str,
    proof_sha256: str,
    resource_types: tuple[str, ...],
) -> dict[str, Any]:
    return {
        "contract": ADMISSION_SEAL_CONTRACT,
        "metadata_summary": dict(metadata_summary),
        "admission_version": admission_version,
        "admission_kind": admission_kind,
        "proof_sha256": proof_sha256,
        "resource_types": list(resource_types),
    }


def _normalized_resource_types(raw_counts: Any) -> tuple[str, ...]:
    if not isinstance(raw_counts, Mapping):
        raise AdmissionSealError("provider_directory_admission_resource_types_invalid")
    if any(
        type(resource_type) is not str
        or not resource_type
        or len(resource_type.encode("utf-8")) > ADMISSION_RESOURCE_TYPE_MAX_BYTES
        for resource_type in raw_counts
    ):
        raise AdmissionSealError("provider_directory_admission_resource_types_invalid")
    resource_types = tuple(sorted(raw_counts))
    if len(resource_types) > ADMISSION_RESOURCE_TYPE_MAX_COUNT:
        raise AdmissionSealError("provider_directory_admission_resource_types_invalid")
    return resource_types


def _bounded_metadata_summary(metadata: Mapping[str, Any]) -> dict[str, Any]:
    try:
        summary = json.loads(
            json.dumps(
                {
                    key: json_value
                    for key, json_value in metadata.items()
                    if key not in _PROOF_KEYS
                },
                ensure_ascii=False,
                separators=(",", ":"),
            )
        )
        encoded = canonical_payload_json(summary).encode("utf-8")
    except (TypeError, ValueError) as error:
        raise AdmissionSealError(
            "provider_directory_admission_metadata_summary_invalid"
        ) from error
    if len(encoded) > ADMISSION_METADATA_SUMMARY_MAX_BYTES:
        raise AdmissionSealError(
            "provider_directory_admission_metadata_summary_unbounded"
        )
    return summary


def _generic_proof_summary(proof: Mapping[str, Any]) -> dict[str, Any]:
    proof_summary_by_field = {
        field_name: proof.get(field_name)
        for field_name in (
            "dataset_hash",
            "resource_count",
            "resource_hashes",
            "resource_counts",
        )
    }
    if (
        type(proof_summary_by_field["dataset_hash"]) is not str
        or _SHA256_RE.fullmatch(proof_summary_by_field["dataset_hash"]) is None
        or type(proof_summary_by_field["resource_count"]) is not int
        or proof_summary_by_field["resource_count"] < 0
        or not isinstance(proof_summary_by_field["resource_hashes"], Mapping)
        or not isinstance(proof_summary_by_field["resource_counts"], Mapping)
    ):
        raise AdmissionSealError("provider_directory_admission_proof_summary_invalid")
    return proof_summary_by_field


def _validated_descriptor_counts(
    raw_descriptor: Any,
    finalized_counts: Mapping[str, Any],
) -> tuple[int, Mapping[str, int]]:
    if not isinstance(raw_descriptor, Mapping):
        raise AdmissionSealError(
            "provider_directory_admission_shard_summary_invalid"
        )
    descriptor_count = raw_descriptor.get("resource_count")
    descriptor_counts = raw_descriptor.get("resource_counts")
    if (
        type(descriptor_count) is not int
        or descriptor_count <= 0
        or not isinstance(descriptor_counts, Mapping)
        or not descriptor_counts
        or any(
            type(resource_type) is not str
            or resource_type not in finalized_counts
            or type(count) is not int
            or count <= 0
            for resource_type, count in descriptor_counts.items()
        )
        or sum(descriptor_counts.values()) != descriptor_count
    ):
        raise AdmissionSealError(
            "provider_directory_admission_shard_summary_invalid"
        )
    return descriptor_count, descriptor_counts


def _require_generic_descriptor_coverage(
    proof: Mapping[str, Any],
) -> None:
    """Require shard occurrences to cover every finalized identity."""

    shards = proof.get("shards")
    resource_count = proof.get("resource_count")
    resource_counts = proof.get("resource_counts")
    if (
        not isinstance(shards, list)
        or not shards
        or type(resource_count) is not int
        or not isinstance(resource_counts, Mapping)
        or any(
            type(resource_type) is not str
            or not resource_type
            or type(count) is not int
            or count < 0
            for resource_type, count in resource_counts.items()
        )
    ):
        raise AdmissionSealError(
            "provider_directory_admission_shard_summary_invalid"
        )
    observed_count = 0
    observed_counts_by_resource: dict[str, int] = {}
    for raw_descriptor in shards:
        descriptor_count, descriptor_counts = _validated_descriptor_counts(
            raw_descriptor,
            resource_counts,
        )
        observed_count += descriptor_count
        for resource_type, count in descriptor_counts.items():
            observed_counts_by_resource[resource_type] = (
                observed_counts_by_resource.get(resource_type, 0) + count
            )
    is_semantic_proof = (
        proof.get("contract_id") in _SEMANTIC_PROOF_CONTRACT_IDS
    )
    if (
        observed_count < resource_count
        if is_semantic_proof
        else observed_count != resource_count
    ) or any(
        observed_counts_by_resource.get(resource_type, 0) < finalized_count
        if is_semantic_proof
        else observed_counts_by_resource.get(resource_type, 0)
        != finalized_count
        for resource_type, finalized_count in resource_counts.items()
    ):
        raise AdmissionSealError(
            "provider_directory_admission_shard_summary_invalid"
        )


def _receipt(
    metadata_summary: Mapping[str, Any],
    *,
    admission_kind: str,
    proof_sha256: Any,
    resource_counts: Any,
    proof_summary: Mapping[str, Any] | None = None,
) -> ProviderDirectoryAdmissionSeal:
    if (
        admission_kind not in {
            ADMISSION_KIND_GENERIC,
            ADMISSION_KIND_UHC_CANONICAL,
        }
        or type(proof_sha256) is not str
        or _SHA256_RE.fullmatch(proof_sha256) is None
    ):
        raise AdmissionSealError("provider_directory_admission_proof_receipt_invalid")
    if ADMISSION_GENERIC_PROOF_SUMMARY_KEY in metadata_summary:
        raise AdmissionSealError(
            "provider_directory_admission_reserved_metadata_key"
        )
    summary_input_by_key = dict(metadata_summary)
    if proof_summary is not None:
        summary_input_by_key[ADMISSION_GENERIC_PROOF_SUMMARY_KEY] = dict(
            proof_summary
        )
    summary = _bounded_metadata_summary(summary_input_by_key)
    resource_types = _normalized_resource_types(resource_counts)
    envelope = _digest_envelope(
        summary,
        admission_version=ADMISSION_SEAL_VERSION,
        admission_kind=admission_kind,
        proof_sha256=proof_sha256,
        resource_types=resource_types,
    )
    return ProviderDirectoryAdmissionSeal(
        metadata_summary=summary,
        metadata_sha256=canonical_payload_sha256(envelope),
        admission_version=ADMISSION_SEAL_VERSION,
        admission_kind=admission_kind,
        proof_sha256=proof_sha256,
        resource_types=resource_types,
    )


def admission_seal_from_validated_metadata(
    metadata: Mapping[str, Any],
) -> ProviderDirectoryAdmissionSeal | None:
    """Build fixed fields from proof bytes already validated by the writer."""

    if not isinstance(metadata, Mapping):
        raise AdmissionSealError("provider_directory_admission_metadata_invalid")
    present_keys = [key for key in _PROOF_KEYS if key in metadata]
    if not present_keys:
        return None
    if len(present_keys) != 1:
        raise AdmissionSealError("provider_directory_admission_proof_kind_invalid")
    proof_key = present_keys[0]
    proof = metadata.get(proof_key)
    if not isinstance(proof, Mapping):
        raise AdmissionSealError("provider_directory_admission_proof_invalid")
    admission_kind = (
        ADMISSION_KIND_GENERIC
        if proof_key == PROVIDER_DIRECTORY_CONTENT_PROOF_METADATA_KEY
        else ADMISSION_KIND_UHC_CANONICAL
    )
    if admission_kind == ADMISSION_KIND_GENERIC:
        _require_generic_descriptor_coverage(proof)
    return _receipt(
        metadata,
        admission_kind=admission_kind,
        proof_sha256=proof.get("proof_sha256"),
        resource_counts=proof.get("resource_counts"),
        proof_summary=(
            _generic_proof_summary(proof)
            if admission_kind == ADMISSION_KIND_GENERIC
            else None
        ),
    )


def validate_generic_admission_copy(
    copy_path: Path,
    *,
    dataset_id: str,
    endpoint_id: str,
    evidence_run_id: str,
    dataset_hash: str,
    resource_count: int,
    expected_resource_hashes: Mapping[str, Any] | None = None,
    expected_resource_counts: Mapping[str, Any] | None = None,
) -> ProviderDirectoryAdmissionSeal:
    """Fully validate one raw-COPY proof with bounded Python memory."""

    from process.provider_directory_admission_copy import (
        _validate_generic_admission_copy,
    )

    return _validate_generic_admission_copy(
        copy_path,
        dataset_id=dataset_id,
        endpoint_id=endpoint_id,
        evidence_run_id=evidence_run_id,
        dataset_hash=dataset_hash,
        resource_count=resource_count,
        scratch_directory=copy_path.parent,
        completion_summaries={
            "expected_resource_hashes": expected_resource_hashes,
            "expected_resource_counts": expected_resource_counts,
        },
    )


async def backfill_provider_directory_admission_seal(
    dataset_id: str,
    *,
    database: Any | None = None,
) -> dict[str, Any]:
    """Seal one exact legacy row in one repeatable-read transaction."""

    from process.provider_directory_admission_backfill import (
        _backfill_provider_directory_admission_seal,
    )

    return await _backfill_provider_directory_admission_seal(
        dataset_id,
        database=database,
    )


__all__ = [
    "ADMISSION_KIND_GENERIC",
    "ADMISSION_KIND_UHC_CANONICAL",
    "ADMISSION_GENERIC_PROOF_SUMMARY_KEY",
    "ADMISSION_LEGACY_METADATA_MAX_BYTES",
    "ADMISSION_LEGACY_SHARD_MAX_COUNT",
    "ADMISSION_METADATA_SUMMARY_MAX_BYTES",
    "ADMISSION_RAW_METADATA_MAX_BYTES",
    "ADMISSION_SEAL_CONTRACT",
    "ADMISSION_SEAL_VERSION",
    "AdmissionSealError",
    "ProviderDirectoryAdmissionSeal",
    "admission_seal_from_validated_metadata",
    "backfill_provider_directory_admission_seal",
    "validate_generic_admission_copy",
]
