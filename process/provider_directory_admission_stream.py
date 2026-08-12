# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded raw-COPY streaming for Provider Directory admission proofs."""

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
import tempfile
from typing import Any

import ijson

from process.provider_directory_admission_seal import (
    ADMISSION_GENERIC_PROOF_SUMMARY_KEY,
    ADMISSION_METADATA_SUMMARY_MAX_BYTES,
    ADMISSION_RESOURCE_TYPE_MAX_COUNT,
    AdmissionSealError,
    ProviderDirectoryAdmissionSeal,
    _normalized_resource_types,
    _require_ascii_canonical_json,
)
from process.provider_directory_fhir_subset_canonical import canonical_payload_json
from process.provider_directory_proof_store import (
    PROVIDER_DIRECTORY_CONTENT_PROOF_METADATA_KEY,
)
from process.uhc_canonical_proof import (
    UHC_CANONICAL_CONTENT_PROOF_METADATA_KEY,
)


_CAPTURE_MAX_EVENTS = 1024 * 1024
_CAPTURE_MAX_ESTIMATED_BYTES = ADMISSION_METADATA_SUMMARY_MAX_BYTES
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
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


@dataclass(frozen=True)
class _AdmissionCopyRequest:
    copy_path: Path
    dataset_id: str
    endpoint_id: str
    evidence_run_id: str
    dataset_hash: str
    resource_count: int
    scratch_directory: Path
    expected_resource_hashes: Mapping[str, Any] | None
    expected_resource_counts: Mapping[str, Any] | None


class _Capture:
    def __init__(self, *, allow_payload_numbers: bool = False) -> None:
        self.builder = ijson.common.ObjectBuilder()
        self.allow_payload_numbers = allow_payload_numbers
        self.depth = 0
        self.event_count = 0
        self.estimated_bytes = 0

    def is_complete_after(self, event: str, event_value: object) -> bool:
        """Capture one parser event and report when its value is complete."""

        if event == "number" and isinstance(event_value, Decimal):
            if not self.allow_payload_numbers:
                raise AdmissionSealError(
                    "provider_directory_admission_proof_shape_invalid"
                )
            if not event_value.is_finite():
                raise AdmissionSealError(
                    "provider_directory_admission_number_invalid"
                )
            if event_value == event_value.to_integral_value():
                if not event_value.is_zero() and event_value.adjusted() > 4095:
                    raise AdmissionSealError(
                        "provider_directory_admission_number_invalid"
                    )
                event_value = int(event_value)
            else:
                float_candidate = float(event_value)
                if (
                    not math.isfinite(float_candidate)
                    or Decimal(str(float_candidate)) != event_value
                ):
                    raise AdmissionSealError(
                        "provider_directory_admission_number_invalid"
                    )
                event_value = float_candidate
        self.event_count += 1
        if event in {"start_map", "start_array", "end_map", "end_array"}:
            self.estimated_bytes += 1
        elif event == "map_key":
            self.estimated_bytes += len(
                json.dumps(event_value, ensure_ascii=False).encode("utf-8")
            ) + 1
        else:
            self.estimated_bytes += len(
                json.dumps(
                    event_value,
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
        self.builder.event(event, event_value)
        if event in {"start_map", "start_array"}:
            self.depth += 1
            return False
        if event in {"end_map", "end_array"}:
            self.depth -= 1
        return self.depth == 0


def _validated_descriptor_counts(
    descriptor_value: object,
) -> tuple[dict[str, Any], int, Mapping[str, int]]:
    if (
        not isinstance(descriptor_value, Mapping)
        or set(descriptor_value) != _DESCRIPTOR_FIELDS
    ):
        raise AdmissionSealError(
            "provider_directory_admission_shard_shape_invalid"
        )
    descriptor_by_field = dict(descriptor_value)
    _require_ascii_canonical_json(descriptor_by_field)
    resource_count = descriptor_by_field.get("resource_count")
    resource_counts_by_type = descriptor_by_field.get("resource_counts")
    if (
        type(resource_count) is not int
        or resource_count <= 0
        or not isinstance(resource_counts_by_type, Mapping)
        or not resource_counts_by_type
        or any(
            type(resource_type) is not str
            or not resource_type
            or type(count) is not int
            or count <= 0
            for resource_type, count in resource_counts_by_type.items()
        )
        or sum(resource_counts_by_type.values()) != resource_count
    ):
        raise AdmissionSealError(
            "provider_directory_admission_shard_count_invalid"
        )
    return descriptor_by_field, resource_count, resource_counts_by_type


class _GenericProofStream:
    def __init__(self, scratch_directory: Path) -> None:
        descriptor_file = tempfile.NamedTemporaryFile(
            prefix="provider-admission-descriptors-",
            suffix=".jsonl",
            dir=scratch_directory,
            delete=False,
        )
        self.descriptor_path = Path(descriptor_file.name)
        os.chmod(self.descriptor_path, 0o600)
        self.descriptor_file = descriptor_file
        self.metadata: dict[str, Any] = {}
        self.proof_header: dict[str, Any] = {}
        self.mode = "expect_root"
        self.pending_key: str | None = None
        self.seen_root_keys: set[str] = set()
        self.seen_proof_keys: set[str] = set()
        self.capture: _Capture | None = None
        self.capture_destination: tuple[str, str | None] | None = None
        self.shard_count = 0
        self.shard_set_digest = hashlib.sha256()
        self.resource_count = 0
        self.resource_counts: dict[str, int] = {}
        self.root_summary_bytes = 2
        self.previous_shard_id: str | None = None
        self.complete = False

    def close(self) -> None:
        """Close and remove the private descriptor spool."""

        if not self.descriptor_file.closed:
            self.descriptor_file.close()
        self.descriptor_path.unlink(missing_ok=True)

    def _store_capture(self) -> None:
        assert self.capture is not None and self.capture_destination is not None
        destination, field_name = self.capture_destination
        captured_value = self.capture.builder.value
        if destination == "root":
            assert field_name is not None
            try:
                entry_size = (
                    len(canonical_payload_json(field_name).encode("utf-8"))
                    + 1
                    + len(canonical_payload_json(captured_value).encode("utf-8"))
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
            self.metadata[field_name] = captured_value
        elif destination == "proof":
            assert field_name is not None
            _require_ascii_canonical_json(captured_value)
            self.proof_header[field_name] = captured_value
        else:
            self._store_descriptor(captured_value)
        self.capture = None
        self.capture_destination = None

    def _store_descriptor(self, descriptor_value: object) -> None:
        (
            descriptor_by_field,
            resource_count,
            resource_counts_by_type,
        ) = _validated_descriptor_counts(descriptor_value)
        descriptor_resource_types = _normalized_resource_types(
            resource_counts_by_type
        )
        if len(set(self.resource_counts).union(descriptor_resource_types)) > (
            ADMISSION_RESOURCE_TYPE_MAX_COUNT
        ):
            raise AdmissionSealError(
                "provider_directory_admission_resource_types_invalid"
            )
        stable_descriptor = json.dumps(
            descriptor_by_field,
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        if self.shard_count:
            self.shard_set_digest.update(b"\n")
        self.shard_set_digest.update(stable_descriptor)
        self.descriptor_file.write(stable_descriptor + b"\n")
        shard_id = descriptor_by_field.get("shard_id")
        if (
            type(shard_id) is not str
            or _SHA256_RE.fullmatch(shard_id) is None
            or self.previous_shard_id is not None
            and shard_id <= self.previous_shard_id
        ):
            raise AdmissionSealError(
                "provider_directory_admission_shard_order_invalid"
            )
        self.previous_shard_id = shard_id
        self.resource_count += resource_count
        for resource_type, count in resource_counts_by_type.items():
            self.resource_counts[resource_type] = (
                self.resource_counts.get(resource_type, 0) + count
            )
        self.shard_count += 1

    def event(self, _prefix: str, event: str, event_value: object) -> None:
        """Consume one ``ijson`` parser event."""

        if self.capture is not None:
            if self.capture.is_complete_after(event, event_value):
                self._store_capture()
            return
        if self.mode == "expect_root":
            if event != "start_map":
                raise AdmissionSealError(
                    "provider_directory_admission_metadata_invalid"
                )
            self.mode = "root"
            return
        if self.mode == "root":
            self._accept_root_event(event, event_value)
        elif self.mode == "proof":
            self._accept_proof_event(event, event_value)
        elif self.mode == "shards":
            self._accept_shard_event(event, event_value)

    def _accept_root_event(self, event: str, event_value: object) -> None:
        if event == "map_key":
            field_name = str(event_value)
            if field_name in self.seen_root_keys:
                raise AdmissionSealError(
                    "provider_directory_admission_metadata_duplicate"
                )
            self.seen_root_keys.add(field_name)
            if field_name == ADMISSION_GENERIC_PROOF_SUMMARY_KEY:
                raise AdmissionSealError(
                    "provider_directory_admission_reserved_metadata_key"
                )
            self.pending_key = field_name
            return
        if event == "end_map":
            self.complete = True
            return
        field_name = self.pending_key
        self.pending_key = None
        if field_name == UHC_CANONICAL_CONTENT_PROOF_METADATA_KEY:
            raise AdmissionSealError(
                "provider_directory_admission_uhc_backfill_unsupported"
            )
        if field_name == PROVIDER_DIRECTORY_CONTENT_PROOF_METADATA_KEY:
            if event != "start_map":
                raise AdmissionSealError(
                    "provider_directory_admission_proof_invalid"
                )
            self.mode = "proof"
            return
        self.capture = _Capture(allow_payload_numbers=True)
        self.capture_destination = ("root", field_name)
        if self.capture.is_complete_after(event, event_value):
            self._store_capture()

    def _accept_proof_event(self, event: str, event_value: object) -> None:
        if event == "map_key":
            field_name = str(event_value)
            if field_name in self.seen_proof_keys:
                raise AdmissionSealError(
                    "provider_directory_admission_proof_duplicate"
                )
            self.seen_proof_keys.add(field_name)
            if field_name not in _SEMANTIC_PROOF_FIELDS:
                raise AdmissionSealError(
                    "provider_directory_admission_proof_keyset_invalid"
                )
            self.pending_key = field_name
            return
        if event == "end_map":
            self.mode = "root"
            return
        field_name = self.pending_key
        self.pending_key = None
        if field_name == "shards":
            if event != "start_array":
                raise AdmissionSealError(
                    "provider_directory_admission_shards_invalid"
                )
            self.mode = "shards"
            return
        self.capture = _Capture()
        self.capture_destination = ("proof", field_name)
        if self.capture.is_complete_after(event, event_value):
            self._store_capture()

    def _accept_shard_event(self, event: str, event_value: object) -> None:
        if event == "end_array":
            self.mode = "proof"
            return
        if event != "start_map":
            raise AdmissionSealError(
                "provider_directory_admission_shard_shape_invalid"
            )
        self.capture = _Capture()
        self.capture_destination = ("shard", None)
        self.capture.is_complete_after(event, event_value)

    def _proof_digest(self) -> str:
        digest = hashlib.sha256()
        digest.update(b"{")
        unsigned_proof_by_field = {
            field_name: field_value
            for field_name, field_value in self.proof_header.items()
            if field_name != "proof_sha256"
        }
        digest_fields = sorted(unsigned_proof_by_field.keys() | {"shards"})
        for field_index, field_name in enumerate(digest_fields):
            if field_index:
                digest.update(b",")
            digest.update(json.dumps(field_name).encode() + b":")
            if field_name != "shards":
                digest.update(
                    json.dumps(
                        unsigned_proof_by_field[field_name],
                        sort_keys=True,
                        separators=(",", ":"),
                    ).encode()
                )
                continue
            digest.update(b"[")
            with self.descriptor_path.open("rb") as descriptor_lines:
                for descriptor_index, descriptor_line in enumerate(
                    descriptor_lines
                ):
                    if descriptor_index:
                        digest.update(b",")
                    digest.update(descriptor_line.rstrip(b"\n"))
            digest.update(b"]")
        digest.update(b"}")
        return digest.hexdigest()

    def finish(
        self,
        request: _AdmissionCopyRequest,
    ) -> ProviderDirectoryAdmissionSeal:
        """Validate the completed stream and build its admission receipt."""

        from process.provider_directory_admission_validation import (
            _finish_generic_proof_stream,
        )

        return _finish_generic_proof_stream(self, request)
