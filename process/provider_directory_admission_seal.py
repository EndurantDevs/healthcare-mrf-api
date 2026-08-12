# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Fixed-size admission receipts for finalized Provider Directory datasets.

The application validator is the admission authority for new writes.  Legacy
rows take the slower path here: one locked row is copied as raw JSON text and
fully revalidated without a PostgreSQL JSONB cast before its receipt is stored.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal
import hashlib
import json
import math
import os
from pathlib import Path
import re
import struct
import tempfile
from typing import Any

import ijson
from ijson.backends import python as ijson_python

from process import provider_directory_proof_store as proof_store
from process.provider_directory_fhir_subset_canonical import (
    canonical_payload_json,
    canonical_payload_sha256,
)
from process.provider_directory_proof_store import (
    PROVIDER_DIRECTORY_CONTENT_PROOF_CONTRACT_ID,
    PROVIDER_DIRECTORY_CONTENT_PROOF_METADATA_KEY,
    PROVIDER_DIRECTORY_PROOF_RESOURCE_SCOPE_METADATA_KEY,
    PROVIDER_DIRECTORY_SEMANTIC_CONTENT_PROOF_CONTRACT_ID,
    PROVIDER_DIRECTORY_SEMANTIC_CONTENT_V4_PROOF_CONTRACT_ID,
    ProviderDirectoryProofStoreError,
)
from process.provider_directory_resource_hash import (
    LEGACY_RESOURCE_HASH_CONTRACT,
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


def _require_ascii_canonical_json(value: Any) -> None:
    """Match the legacy PostgreSQL proof canonicalizer's value domain."""

    if value is None or type(value) in {bool, int}:
        return
    if type(value) is str:
        try:
            value.encode("ascii")
        except UnicodeEncodeError as error:
            raise AdmissionSealError(
                "provider_directory_admission_proof_non_ascii"
            ) from error
        return
    if isinstance(value, list):
        for item in value:
            _require_ascii_canonical_json(item)
        return
    if isinstance(value, Mapping):
        for key, item in value.items():
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
    metadata_summary: dict[str, Any]
    metadata_sha256: str
    admission_version: int
    admission_kind: str
    proof_sha256: str
    resource_types: tuple[str, ...]

    def digest_envelope(self) -> dict[str, Any]:
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
            or len(resource_type.encode("utf-8"))
            > ADMISSION_RESOURCE_TYPE_MAX_BYTES
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
                    key: value
                    for key, value in metadata.items()
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
    summary = {
        field_name: proof.get(field_name)
        for field_name in (
            "dataset_hash",
            "resource_count",
            "resource_hashes",
            "resource_counts",
        )
    }
    if (
        type(summary["dataset_hash"]) is not str
        or _SHA256_RE.fullmatch(summary["dataset_hash"]) is None
        or type(summary["resource_count"]) is not int
        or summary["resource_count"] < 0
        or not isinstance(summary["resource_hashes"], Mapping)
        or not isinstance(summary["resource_counts"], Mapping)
    ):
        raise AdmissionSealError("provider_directory_admission_proof_summary_invalid")
    return summary


def _require_exact_generic_descriptor_aggregates(
    proof: Mapping[str, Any],
) -> None:
    """Retain the deployed SQL admission aggregate contract for new seals."""

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
    observed_counts: dict[str, int] = {}
    for descriptor in shards:
        if not isinstance(descriptor, Mapping):
            raise AdmissionSealError(
                "provider_directory_admission_shard_summary_invalid"
            )
        descriptor_count = descriptor.get("resource_count")
        descriptor_counts = descriptor.get("resource_counts")
        if (
            type(descriptor_count) is not int
            or descriptor_count <= 0
            or not isinstance(descriptor_counts, Mapping)
            or not descriptor_counts
            or any(
                type(resource_type) is not str
                or resource_type not in resource_counts
                or type(count) is not int
                or count <= 0
                for resource_type, count in descriptor_counts.items()
            )
            or sum(descriptor_counts.values()) != descriptor_count
        ):
            raise AdmissionSealError(
                "provider_directory_admission_shard_summary_invalid"
            )
        observed_count += descriptor_count
        for resource_type, count in descriptor_counts.items():
            observed_counts[resource_type] = (
                observed_counts.get(resource_type, 0) + count
            )
    if observed_count != resource_count or any(
        observed_counts.get(resource_type, 0) != finalized_count
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
    summary_input = dict(metadata_summary)
    if proof_summary is not None:
        summary_input[ADMISSION_GENERIC_PROOF_SUMMARY_KEY] = dict(
            proof_summary
        )
    summary = _bounded_metadata_summary(summary_input)
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
        _require_exact_generic_descriptor_aggregates(proof)
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


class _Capture:
    def __init__(self, *, allow_payload_numbers: bool = False) -> None:
        self.builder = ijson.common.ObjectBuilder()
        self.allow_payload_numbers = allow_payload_numbers
        self.depth = 0
        self.event_count = 0
        self.estimated_bytes = 0

    def feed(self, event: str, value: object) -> bool:
        if event == "number" and isinstance(value, Decimal):
            if not self.allow_payload_numbers:
                raise AdmissionSealError(
                    "provider_directory_admission_proof_shape_invalid"
                )
            if not value.is_finite():
                raise AdmissionSealError(
                    "provider_directory_admission_number_invalid"
                )
            if value == value.to_integral_value():
                if not value.is_zero() and value.adjusted() > 4095:
                    raise AdmissionSealError(
                        "provider_directory_admission_number_invalid"
                    )
                value = int(value)
            else:
                candidate = float(value)
                if (
                    not math.isfinite(candidate)
                    or Decimal(str(candidate)) != value
                ):
                    raise AdmissionSealError(
                        "provider_directory_admission_number_invalid"
                    )
                value = candidate
        self.event_count += 1
        if event in {"start_map", "start_array", "end_map", "end_array"}:
            self.estimated_bytes += 1
        elif event == "map_key":
            self.estimated_bytes += len(
                json.dumps(value, ensure_ascii=False).encode("utf-8")
            ) + 1
        else:
            self.estimated_bytes += len(
                json.dumps(
                    value,
                    ensure_ascii=False,
                    separators=(",", ":"),
                ).encode("utf-8")
            )
        if (
            self.event_count > _CAPTURE_MAX_EVENTS
            or self.estimated_bytes > _CAPTURE_MAX_ESTIMATED_BYTES
        ):
            raise AdmissionSealError(
                "provider_directory_admission_capture_unbounded"
            )
        self.builder.event(event, value)
        if event in {"start_map", "start_array"}:
            self.depth += 1
            return False
        if event in {"end_map", "end_array"}:
            self.depth -= 1
        return self.depth == 0


class _GenericProofStream:
    def __init__(self, scratch_directory: Path) -> None:
        descriptor = tempfile.NamedTemporaryFile(
            prefix="provider-admission-descriptors-",
            suffix=".jsonl",
            dir=scratch_directory,
            delete=False,
        )
        self.descriptor_path = Path(descriptor.name)
        os.chmod(self.descriptor_path, 0o600)
        self.descriptor_file = descriptor
        self.metadata: dict[str, Any] = {}
        self.proof_header: dict[str, Any] = {}
        self.mode = "expect_root"
        self.pending_key: str | None = None
        self.seen_root_keys: set[str] = set()
        self.seen_proof_keys: set[str] = set()
        self.capture: _Capture | None = None
        self.capture_target: tuple[str, str | None] | None = None
        self.shard_count = 0
        self.shard_set_digest = hashlib.sha256()
        self.resource_count = 0
        self.resource_counts: dict[str, int] = {}
        self.root_summary_bytes = 2
        self.previous_shard_id: str | None = None
        self.complete = False

    def close(self) -> None:
        if not self.descriptor_file.closed:
            self.descriptor_file.close()
        self.descriptor_path.unlink(missing_ok=True)

    def _store_capture(self) -> None:
        assert self.capture is not None and self.capture_target is not None
        target, key = self.capture_target
        value = self.capture.builder.value
        if target == "root":
            assert key is not None
            try:
                entry_size = (
                    len(canonical_payload_json(key).encode("utf-8"))
                    + 1
                    + len(canonical_payload_json(value).encode("utf-8"))
                    + bool(self.metadata)
                )
            except (TypeError, ValueError) as error:
                raise AdmissionSealError(
                    "provider_directory_admission_metadata_summary_invalid"
                ) from error
            self.root_summary_bytes += entry_size
            if self.root_summary_bytes > ADMISSION_METADATA_SUMMARY_MAX_BYTES:
                raise AdmissionSealError(
                    "provider_directory_admission_metadata_summary_unbounded"
                )
            self.metadata[key] = value
        elif target == "proof":
            assert key is not None
            _require_ascii_canonical_json(value)
            self.proof_header[key] = value
        else:
            self._descriptor(value)
        self.capture = None
        self.capture_target = None

    def _descriptor(self, value: object) -> None:
        if not isinstance(value, Mapping) or set(value) != _DESCRIPTOR_FIELDS:
            raise AdmissionSealError("provider_directory_admission_shard_shape_invalid")
        descriptor = dict(value)
        _require_ascii_canonical_json(descriptor)
        resource_count = descriptor.get("resource_count")
        resource_counts = descriptor.get("resource_counts")
        if (
            type(resource_count) is not int
            or resource_count <= 0
            or not isinstance(resource_counts, Mapping)
            or not resource_counts
            or any(
                type(resource_type) is not str
                or not resource_type
                or type(count) is not int
                or count <= 0
                for resource_type, count in resource_counts.items()
            )
            or sum(resource_counts.values()) != resource_count
        ):
            raise AdmissionSealError("provider_directory_admission_shard_count_invalid")
        descriptor_resource_types = _normalized_resource_types(resource_counts)
        if len(set(self.resource_counts).union(descriptor_resource_types)) > (
            ADMISSION_RESOURCE_TYPE_MAX_COUNT
        ):
            raise AdmissionSealError(
                "provider_directory_admission_resource_types_invalid"
            )
        stable = json.dumps(
            descriptor,
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        if self.shard_count:
            self.shard_set_digest.update(b"\n")
        self.shard_set_digest.update(stable)
        self.descriptor_file.write(stable + b"\n")
        shard_id = descriptor.get("shard_id")
        if (
            type(shard_id) is not str
            or _SHA256_RE.fullmatch(shard_id) is None
            or self.previous_shard_id is not None
            and shard_id <= self.previous_shard_id
        ):
            raise AdmissionSealError("provider_directory_admission_shard_order_invalid")
        self.previous_shard_id = shard_id
        self.resource_count += resource_count
        for resource_type, count in resource_counts.items():
            self.resource_counts[resource_type] = (
                self.resource_counts.get(resource_type, 0) + count
            )
        self.shard_count += 1

    def event(self, _prefix: str, event: str, value: object) -> None:
        if self.capture is not None:
            if self.capture.feed(event, value):
                self._store_capture()
            return
        if self.mode == "expect_root":
            if event != "start_map":
                raise AdmissionSealError("provider_directory_admission_metadata_invalid")
            self.mode = "root"
            return
        if self.mode == "root":
            if event == "map_key":
                key = str(value)
                if key in self.seen_root_keys:
                    raise AdmissionSealError("provider_directory_admission_metadata_duplicate")
                self.seen_root_keys.add(key)
                if key == ADMISSION_GENERIC_PROOF_SUMMARY_KEY:
                    raise AdmissionSealError(
                        "provider_directory_admission_reserved_metadata_key"
                    )
                self.pending_key = key
                return
            if event == "end_map":
                self.complete = True
                return
            key = self.pending_key
            self.pending_key = None
            if key == UHC_CANONICAL_CONTENT_PROOF_METADATA_KEY:
                raise AdmissionSealError("provider_directory_admission_uhc_backfill_unsupported")
            if key == PROVIDER_DIRECTORY_CONTENT_PROOF_METADATA_KEY:
                if event != "start_map":
                    raise AdmissionSealError("provider_directory_admission_proof_invalid")
                self.mode = "proof"
                return
            self.capture = _Capture(allow_payload_numbers=True)
            self.capture_target = ("root", key)
            if self.capture.feed(event, value):
                self._store_capture()
            return
        if self.mode == "proof":
            if event == "map_key":
                key = str(value)
                if key in self.seen_proof_keys:
                    raise AdmissionSealError("provider_directory_admission_proof_duplicate")
                self.seen_proof_keys.add(key)
                if key not in _SEMANTIC_PROOF_FIELDS:
                    raise AdmissionSealError(
                        "provider_directory_admission_proof_keyset_invalid"
                    )
                self.pending_key = key
                return
            if event == "end_map":
                self.mode = "root"
                return
            key = self.pending_key
            self.pending_key = None
            if key == "shards":
                if event != "start_array":
                    raise AdmissionSealError("provider_directory_admission_shards_invalid")
                self.mode = "shards"
                return
            self.capture = _Capture()
            self.capture_target = ("proof", key)
            if self.capture.feed(event, value):
                self._store_capture()
            return
        if self.mode == "shards":
            if event == "end_array":
                self.mode = "proof"
                return
            if event != "start_map":
                raise AdmissionSealError("provider_directory_admission_shard_shape_invalid")
            self.capture = _Capture()
            self.capture_target = ("shard", None)
            self.capture.feed(event, value)

    def _proof_digest(self) -> str:
        digest = hashlib.sha256()
        digest.update(b"{")
        unsigned = {
            key: value
            for key, value in self.proof_header.items()
            if key != "proof_sha256"
        }
        for index, key in enumerate(sorted(unsigned.keys() | {"shards"})):
            if index:
                digest.update(b",")
            digest.update(json.dumps(key).encode() + b":")
            if key != "shards":
                digest.update(
                    json.dumps(
                        unsigned[key],
                        sort_keys=True,
                        separators=(",", ":"),
                    ).encode()
                )
                continue
            digest.update(b"[")
            with self.descriptor_path.open("rb") as descriptors:
                for descriptor_index, line in enumerate(descriptors):
                    if descriptor_index:
                        digest.update(b",")
                    digest.update(line.rstrip(b"\n"))
            digest.update(b"]")
        digest.update(b"}")
        return digest.hexdigest()

    def finish(
        self,
        *,
        dataset_id: str,
        endpoint_id: str,
        evidence_run_id: str,
        dataset_hash: str,
        resource_count: int,
        expected_resource_hashes: Mapping[str, Any] | None = None,
        expected_resource_counts: Mapping[str, Any] | None = None,
    ) -> ProviderDirectoryAdmissionSeal:
        self.descriptor_file.close()
        if not self.complete or self.mode != "root":
            raise AdmissionSealError("provider_directory_admission_metadata_incomplete")
        contract_id = self.proof_header.get("contract_id")
        expected_fields = (
            _LEGACY_PROOF_FIELDS
            if contract_id == PROVIDER_DIRECTORY_CONTENT_PROOF_CONTRACT_ID
            else _SEMANTIC_PROOF_FIELDS
        )
        if set(self.proof_header).union({"shards"}) != expected_fields:
            raise AdmissionSealError("provider_directory_admission_proof_keyset_invalid")
        if (
            self.shard_count <= 0
            or type(self.proof_header.get("shard_count")) is not int
            or self.proof_header.get("shard_count") <= 0
            or self.proof_header.get("shard_count") != self.shard_count
            or self.proof_header.get("shard_set_sha256")
            != self.shard_set_digest.hexdigest()
            or self.proof_header.get("resource_count") != resource_count
            or self.proof_header.get("dataset_hash") != dataset_hash
            or self.proof_header.get("proof_sha256") != self._proof_digest()
        ):
            raise AdmissionSealError("provider_directory_admission_shard_summary_invalid")
        if (
            expected_resource_hashes is not None
            or expected_resource_counts is not None
        ) and (
            not isinstance(expected_resource_hashes, Mapping)
            or not isinstance(expected_resource_counts, Mapping)
            or self.proof_header.get("resource_hashes")
            != expected_resource_hashes
            or self.proof_header.get("resource_counts")
            != expected_resource_counts
        ):
            raise AdmissionSealError(
                "provider_directory_admission_completion_summary_invalid"
            )
        if (
            "dataset_hash" in self.metadata
            and self.metadata["dataset_hash"] != dataset_hash
        ) or (
            "resource_count" in self.metadata
            and (
                type(self.metadata["resource_count"]) is not int
                or self.metadata["resource_count"] != resource_count
            )
        ) or (
            "acquisition_root_run_id" in self.metadata
            and self.metadata["acquisition_root_run_id"] != evidence_run_id
        ):
            raise AdmissionSealError("provider_directory_admission_parent_identity_invalid")
        is_legacy_contract = (
            self.proof_header.get("contract_id")
            == PROVIDER_DIRECTORY_CONTENT_PROOF_CONTRACT_ID
        )
        proof_scope = self.metadata.get(
            PROVIDER_DIRECTORY_PROOF_RESOURCE_SCOPE_METADATA_KEY
        )
        try:
            if (
                not isinstance(self.metadata.get("source_ids"), list)
                or not isinstance(
                    self.metadata.get("selected_resources"), list
                )
                or (
                    not is_legacy_contract
                    and proof_scope is not None
                    and not isinstance(proof_scope, list)
                )
                or any(
                    type(scope_item) is not str
                    for scope in (
                        self.metadata.get("source_ids", ()),
                        self.metadata.get("selected_resources", ()),
                        (
                            proof_scope or ()
                            if not is_legacy_contract
                            else ()
                        ),
                    )
                    for scope_item in scope
                )
            ):
                raise ProviderDirectoryProofStoreError(
                    "provider directory proof finalization lineage is invalid"
                )
            if (
                "resource_hash_contract" in self.metadata
                and self.metadata["resource_hash_contract"] is None
            ):
                raise ProviderDirectoryProofStoreError(
                    "provider directory expected proof contract is invalid"
                )
            lineage = proof_store._validated_proof_lineage(
                dataset_id=dataset_id,
                endpoint_id=endpoint_id,
                acquisition_root_run_id=evidence_run_id,
                source_ids=self.metadata.get("source_ids", ()),
                selected_resources=self.metadata.get("selected_resources", ()),
                proof_resource_scope=(
                    None if is_legacy_contract else proof_scope
                ),
            )
            if (
                self.metadata["source_ids"] != lineage.source_ids
                or self.metadata["selected_resources"]
                != lineage.selected_resources
                or (
                    not is_legacy_contract
                    and proof_scope is not None
                    and proof_scope != lineage.proof_resource_scope
                )
            ):
                raise ProviderDirectoryProofStoreError(
                    "provider directory proof finalization lineage is invalid"
                )
            if (
                not is_legacy_contract
                and proof_scope is not None
                and proof_scope
                != self.proof_header.get(
                    PROVIDER_DIRECTORY_PROOF_RESOURCE_SCOPE_METADATA_KEY
                )
            ):
                raise ProviderDirectoryProofStoreError(
                    "provider directory proof resource scope changed"
                )
            proof_store._validate_metadata_lineage(self.proof_header, lineage)
            proof_store._validate_metadata_summary(self.proof_header, lineage)
            exact_resource_scope = set(
                lineage.proof_resource_scope or lineage.selected_resources
            )
            if (
                set(self.proof_header["resource_counts"])
                != exact_resource_scope
                or set(self.proof_header["resource_hashes"])
                != exact_resource_scope
            ):
                raise ProviderDirectoryProofStoreError(
                    "provider directory proof resource scope changed"
                )
            if (
                self.resource_count != resource_count
                or not set(self.resource_counts).issubset(
                    exact_resource_scope
                )
                or any(
                    self.resource_counts.get(resource_type, 0)
                    != finalized_count
                    for resource_type, finalized_count in
                    self.proof_header["resource_counts"].items()
                )
            ):
                raise ProviderDirectoryProofStoreError(
                    "provider directory proof shard resource total changed"
                )
            proof_store._assert_expected_proof_contract(
                self.proof_header,
                (
                    self.metadata["resource_hash_contract"]
                    if "resource_hash_contract" in self.metadata
                    else LEGACY_RESOURCE_HASH_CONTRACT
                ),
                (
                    None
                    if is_legacy_contract
                    else self.metadata.get("semantic_projection_as_of")
                ),
                lineage.proof_resource_scope,
            )
            with self.descriptor_path.open("rb") as descriptors:
                for line in descriptors:
                    descriptor = proof_store._validated_shard_descriptor(
                        json.loads(line),
                        dataset_id=dataset_id,
                        endpoint_id=endpoint_id,
                        acquisition_root_run_id=evidence_run_id,
                        source_ids=lineage.source_ids,
                    )
                    if not set(descriptor["resource_counts"]).issubset(
                        exact_resource_scope
                    ):
                        raise ProviderDirectoryProofStoreError(
                            "provider directory proof shard resource scope changed"
                        )
        except (ProviderDirectoryProofStoreError, TypeError, ValueError) as error:
            raise AdmissionSealError(
                f"provider_directory_admission_shard_validation_invalid:{error}"
            ) from error
        return _receipt(
            self.metadata,
            admission_kind=ADMISSION_KIND_GENERIC,
            proof_sha256=self.proof_header.get("proof_sha256"),
            resource_counts=self.proof_header.get("resource_counts"),
            proof_summary=_generic_proof_summary(self.proof_header),
        )


class _LimitedReader:
    def __init__(self, source: Any, remaining: int) -> None:
        self.source = source
        self.remaining = remaining

    def read(self, size: int = -1) -> bytes:
        if self.remaining <= 0:
            return b""
        if size < 0 or size > self.remaining:
            size = self.remaining
        data = self.source.read(size)
        self.remaining -= len(data)
        return data


def _copy_field_reader(copy_path: Path) -> tuple[Any, _LimitedReader]:
    source = copy_path.open("rb")
    try:
        header = source.read(19)
        if header != _COPY_SIGNATURE + struct.pack("!ii", 0, 0):
            raise AdmissionSealError("provider_directory_admission_copy_header_invalid")
        field_count_raw = source.read(2)
        if len(field_count_raw) != 2 or struct.unpack("!h", field_count_raw)[0] != 1:
            raise AdmissionSealError("provider_directory_admission_copy_shape_invalid")
        field_length_raw = source.read(4)
        if len(field_length_raw) != 4:
            raise AdmissionSealError("provider_directory_admission_copy_shape_invalid")
        field_length = struct.unpack("!i", field_length_raw)[0]
        if field_length < 0 or field_length > ADMISSION_RAW_METADATA_MAX_BYTES:
            raise AdmissionSealError("provider_directory_admission_copy_size_invalid")
        return source, _LimitedReader(source, field_length)
    except BaseException:
        source.close()
        raise


def validate_generic_admission_copy(
    copy_path: Path,
    *,
    dataset_id: str,
    endpoint_id: str,
    evidence_run_id: str,
    dataset_hash: str,
    resource_count: int,
    scratch_directory: Path,
    expected_resource_hashes: Mapping[str, Any] | None = None,
    expected_resource_counts: Mapping[str, Any] | None = None,
) -> ProviderDirectoryAdmissionSeal:
    """Fully validate one raw-COPY generic proof with bounded Python memory."""

    stream = _GenericProofStream(scratch_directory)
    source: Any = None
    try:
        source, field_reader = _copy_field_reader(copy_path)
        for event in ijson_python.parse(field_reader):
            stream.event(*event)
        if field_reader.remaining != 0 or source.read(2) != struct.pack("!h", -1):
            raise AdmissionSealError("provider_directory_admission_copy_trailer_invalid")
        if source.read(1):
            raise AdmissionSealError("provider_directory_admission_copy_trailer_invalid")
        return stream.finish(
            dataset_id=dataset_id,
            endpoint_id=endpoint_id,
            evidence_run_id=evidence_run_id,
            dataset_hash=dataset_hash,
            resource_count=resource_count,
            expected_resource_hashes=expected_resource_hashes,
            expected_resource_counts=expected_resource_counts,
        )
    except AdmissionSealError:
        raise
    except (ijson.JSONError, OSError, ValueError) as error:
        raise AdmissionSealError(
            "provider_directory_admission_copy_parse_invalid"
        ) from error
    finally:
        if source is not None:
            source.close()
        stream.close()


def _schema_name() -> str:
    schema = os.getenv("HLTHPRT_DB_SCHEMA") or os.getenv("DB_SCHEMA") or "mrf"
    if _IDENTIFIER_RE.fullmatch(schema) is None:
        raise AdmissionSealError("provider_directory_admission_schema_invalid")
    return schema


def _qualified_dataset_table() -> str:
    return f'"{_schema_name()}"."provider_directory_endpoint_dataset"'


async def backfill_provider_directory_admission_seal(
    dataset_id: str,
    *,
    database: Any | None = None,
) -> dict[str, Any]:
    """Seal one exact legacy row in one repeatable-read transaction."""

    if not dataset_id or dataset_id != dataset_id.strip():
        raise AdmissionSealError("provider_directory_admission_dataset_id_invalid")
    if database is None:
        from db.models import db as database

    dataset_ref = _qualified_dataset_table()
    async with database.acquire_driver() as connection:
        async with connection.transaction(isolation="repeatable_read"):
            row = await connection.fetchrow(
                f"""
                SELECT dataset_id, endpoint_id,
                       COALESCE(acquisition_root_run_id, import_run_id)
                           AS evidence_run_id,
                       dataset_hash, resource_count, status,
                       completion_proof_required_version,
                       CASE
                           WHEN completion_proof_required_version = 3
                           THEN completion_proof_json
                               -> 'dataset' -> 'resource_hashes'
                       END AS completion_resource_hashes,
                       CASE
                           WHEN completion_proof_required_version = 3
                           THEN completion_proof_json
                               -> 'dataset' -> 'resource_counts'
                       END AS completion_resource_counts,
                       octet_length(publication_metadata_json::text)
                           AS raw_metadata_bytes,
                       publication_metadata_summary_json,
                       publication_metadata_sha256,
                       content_proof_admission_version,
                       content_proof_admission_kind,
                       content_proof_admission_sha256,
                       content_proof_resource_types,
                       ctid::text AS row_ctid, xmin::text AS row_xmin
                  FROM {dataset_ref}
                 WHERE dataset_id = $1
                 FOR UPDATE
                """,
                dataset_id,
            )
            if row is None:
                raise AdmissionSealError("provider_directory_admission_dataset_missing")
            seal_values = tuple(
                row[field]
                for field in (
                    "publication_metadata_summary_json",
                    "publication_metadata_sha256",
                    "content_proof_admission_version",
                    "content_proof_admission_kind",
                    "content_proof_admission_sha256",
                    "content_proof_resource_types",
                )
            )
            if all(value is not None for value in seal_values):
                return {
                    "dataset_id": dataset_id,
                    "status": "already_sealed",
                    "admission_kind": row["content_proof_admission_kind"],
                }
            if any(value is not None for value in seal_values):
                raise AdmissionSealError("provider_directory_admission_partial_seal")
            if row["status"] not in _FINALIZED_STATUSES:
                raise AdmissionSealError("provider_directory_admission_status_invalid")
            completion_version = row["completion_proof_required_version"]
            if completion_version not in {None, 3} or (
                completion_version == 3
                and (
                    not isinstance(row["completion_resource_hashes"], Mapping)
                    or not isinstance(
                        row["completion_resource_counts"], Mapping
                    )
                )
            ):
                raise AdmissionSealError(
                    "provider_directory_admission_completion_summary_invalid"
                )
            raw_metadata_bytes = row["raw_metadata_bytes"]
            if (
                type(raw_metadata_bytes) is not int
                or raw_metadata_bytes <= 0
                or raw_metadata_bytes > ADMISSION_RAW_METADATA_MAX_BYTES
            ):
                raise AdmissionSealError("provider_directory_admission_metadata_size_invalid")
            if (
                not row["evidence_run_id"]
                or not row["dataset_hash"]
                or isinstance(row["resource_count"], bool)
                or not isinstance(row["resource_count"], int)
            ):
                raise AdmissionSealError("provider_directory_admission_parent_identity_invalid")
            with tempfile.TemporaryDirectory(
                prefix="provider-directory-admission-"
            ) as temporary:
                temporary_path = Path(temporary)
                copy_file = tempfile.NamedTemporaryFile(
                    prefix="metadata-",
                    suffix=".copy",
                    dir=temporary_path,
                    delete=False,
                )
                copy_path = Path(copy_file.name)
                os.chmod(copy_path, 0o600)
                copied_bytes = 0

                async def spool_copy(data: bytes) -> None:
                    nonlocal copied_bytes
                    copied_bytes += len(data)
                    if copied_bytes > ADMISSION_RAW_METADATA_MAX_BYTES + 128:
                        raise AdmissionSealError(
                            "provider_directory_admission_copy_size_invalid"
                        )
                    copy_file.write(data)

                try:
                    copy_status = await connection.copy_from_query(
                        f"""
                        SELECT publication_metadata_json::text
                          FROM {dataset_ref}
                         WHERE dataset_id = $1
                           AND ctid::text = $2
                           AND xmin::text = $3
                        """,
                        dataset_id,
                        row["row_ctid"],
                        row["row_xmin"],
                        output=spool_copy,
                        format="binary",
                    )
                    copy_file.flush()
                    os.fsync(copy_file.fileno())
                finally:
                    copy_file.close()
                if copy_status != "COPY 1":
                    raise AdmissionSealError("provider_directory_admission_copy_lost")
                seal = validate_generic_admission_copy(
                    copy_path,
                    dataset_id=row["dataset_id"],
                    endpoint_id=row["endpoint_id"],
                    evidence_run_id=row["evidence_run_id"],
                    dataset_hash=row["dataset_hash"],
                    resource_count=row["resource_count"],
                    scratch_directory=temporary_path,
                    expected_resource_hashes=(
                        row["completion_resource_hashes"]
                        if completion_version == 3
                        else None
                    ),
                    expected_resource_counts=(
                        row["completion_resource_counts"]
                        if completion_version == 3
                        else None
                    ),
                )
            update_status = await connection.execute(
                f"""
                UPDATE {dataset_ref}
                   SET publication_metadata_summary_json = $1::jsonb,
                       publication_metadata_sha256 = $2,
                       content_proof_admission_version = $3,
                       content_proof_admission_kind = $4,
                       content_proof_admission_sha256 = $5,
                       content_proof_resource_types = $6::varchar[]
                 WHERE dataset_id = $7
                   AND ctid::text = $8
                   AND xmin::text = $9
                   AND publication_metadata_summary_json IS NULL
                   AND publication_metadata_sha256 IS NULL
                   AND content_proof_admission_version IS NULL
                   AND content_proof_admission_kind IS NULL
                   AND content_proof_admission_sha256 IS NULL
                   AND content_proof_resource_types IS NULL
                """,
                json.dumps(seal.metadata_summary, ensure_ascii=False),
                seal.metadata_sha256,
                seal.admission_version,
                seal.admission_kind,
                seal.proof_sha256,
                list(seal.resource_types),
                dataset_id,
                row["row_ctid"],
                row["row_xmin"],
            )
            if update_status != "UPDATE 1":
                raise AdmissionSealError("provider_directory_admission_backfill_lost")
            return {
                "dataset_id": dataset_id,
                "status": "sealed",
                "admission_kind": seal.admission_kind,
                "metadata_sha256": seal.metadata_sha256,
                "proof_sha256": seal.proof_sha256,
                "resource_types": list(seal.resource_types),
                "raw_metadata_bytes": raw_metadata_bytes,
            }


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
