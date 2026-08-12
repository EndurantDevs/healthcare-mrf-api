# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded raw-COPY parser for Provider Directory admission proofs."""

from __future__ import annotations

from collections.abc import Mapping
from decimal import Decimal
import hashlib
import json
import math
import os
from pathlib import Path
import struct
import tempfile
from typing import Any

import ijson

from process.provider_directory_admission_seal import (
    ADMISSION_GENERIC_PROOF_SUMMARY_KEY,
    ADMISSION_METADATA_SUMMARY_MAX_BYTES,
    ADMISSION_RAW_METADATA_MAX_BYTES,
    ADMISSION_RESOURCE_TYPE_MAX_COUNT,
    AdmissionSealError,
    _CAPTURE_MAX_ESTIMATED_BYTES,
    _CAPTURE_MAX_EVENTS,
    _COPY_SIGNATURE,
    _DESCRIPTOR_FIELDS,
    _normalized_resource_types,
    _require_ascii_canonical_json,
    _SEMANTIC_PROOF_FIELDS,
    _SHA256_RE,
)
from process.provider_directory_fhir_subset_canonical import canonical_payload_json
from process.provider_directory_proof_store import (
    PROVIDER_DIRECTORY_CONTENT_PROOF_METADATA_KEY,
)
from process.uhc_canonical_proof import UHC_CANONICAL_CONTENT_PROOF_METADATA_KEY


class _Capture:
    def __init__(self, *, allow_payload_numbers: bool = False) -> None:
        self.builder = ijson.common.ObjectBuilder()
        self.allow_payload_numbers = allow_payload_numbers
        self.depth = 0
        self.event_count = 0
        self.estimated_bytes = 0

    def _normalized_event_value(
        self,
        event_name: str,
        event_value: object,
    ) -> object:
        if event_name != "number" or not isinstance(event_value, Decimal):
            return event_value
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
            return int(event_value)
        float_value = float(event_value)
        if (
            not math.isfinite(float_value)
            or Decimal(str(float_value)) != event_value
        ):
            raise AdmissionSealError(
                "provider_directory_admission_number_invalid"
            )
        return float_value

    def _record_event_size(self, event_name: str, event_value: object) -> None:
        self.event_count += 1
        if event_name in {"start_map", "start_array", "end_map", "end_array"}:
            self.estimated_bytes += 1
        elif event_name == "map_key":
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

    def _is_complete_after_event(
        self,
        event_name: str,
        event_value: object,
    ) -> bool:
        normalized_value = self._normalized_event_value(event_name, event_value)
        self._record_event_size(event_name, normalized_value)
        self.builder.event(event_name, normalized_value)
        if event_name in {"start_map", "start_array"}:
            self.depth += 1
            return False
        if event_name in {"end_map", "end_array"}:
            self.depth -= 1
        return self.depth == 0


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
        self.capture_target: tuple[str, str | None] | None = None
        self.shard_count = 0
        self.shard_set_digest = hashlib.sha256()
        self.resource_count = 0
        self.resource_counts: dict[str, int] = {}
        self.root_summary_bytes = 2
        self.previous_shard_id: str | None = None
        self.complete = False

    def close(self) -> None:
        """Close and unlink the private descriptor spool."""

        if not self.descriptor_file.closed:
            self.descriptor_file.close()
        self.descriptor_path.unlink(missing_ok=True)

    def _store_root_capture(self, key: str, captured_value: object) -> None:
        try:
            entry_size = (
                len(canonical_payload_json(key).encode("utf-8"))
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
        self.metadata[key] = captured_value

    def _store_capture(self) -> None:
        assert self.capture is not None and self.capture_target is not None
        capture_scope, key = self.capture_target
        captured_value = self.capture.builder.value
        if capture_scope == "root":
            assert key is not None
            self._store_root_capture(key, captured_value)
        elif capture_scope == "proof":
            assert key is not None
            _require_ascii_canonical_json(captured_value)
            self.proof_header[key] = captured_value
        else:
            self._descriptor(captured_value)
        self.capture = None
        self.capture_target = None

    def _validated_descriptor(
        self,
        raw_descriptor: object,
    ) -> tuple[dict[str, Any], int, Mapping[str, int]]:
        if (
            not isinstance(raw_descriptor, Mapping)
            or set(raw_descriptor) != _DESCRIPTOR_FIELDS
        ):
            raise AdmissionSealError(
                "provider_directory_admission_shard_shape_invalid"
            )
        descriptor_by_field = dict(raw_descriptor)
        _require_ascii_canonical_json(descriptor_by_field)
        resource_count = descriptor_by_field.get("resource_count")
        resource_counts = descriptor_by_field.get("resource_counts")
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
            raise AdmissionSealError(
                "provider_directory_admission_shard_count_invalid"
            )
        return descriptor_by_field, resource_count, resource_counts

    def _record_descriptor(
        self,
        descriptor_by_field: dict[str, Any],
        resource_count: int,
        resource_counts: Mapping[str, int],
    ) -> None:
        descriptor_resource_types = _normalized_resource_types(resource_counts)
        if len(set(self.resource_counts).union(descriptor_resource_types)) > (
            ADMISSION_RESOURCE_TYPE_MAX_COUNT
        ):
            raise AdmissionSealError(
                "provider_directory_admission_resource_types_invalid"
            )
        stable_bytes = json.dumps(
            descriptor_by_field,
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        if self.shard_count:
            self.shard_set_digest.update(b"\n")
        self.shard_set_digest.update(stable_bytes)
        self.descriptor_file.write(stable_bytes + b"\n")
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
        for resource_type, count in resource_counts.items():
            self.resource_counts[resource_type] = (
                self.resource_counts.get(resource_type, 0) + count
            )
        self.shard_count += 1

    def _descriptor(self, raw_descriptor: object) -> None:
        descriptor_by_field, resource_count, resource_counts = (
            self._validated_descriptor(raw_descriptor)
        )
        self._record_descriptor(
            descriptor_by_field,
            resource_count,
            resource_counts,
        )

    def _is_capture_event(self, event_name: str, event_value: object) -> bool:
        if self.capture is None:
            return False
        if self.capture._is_complete_after_event(event_name, event_value):
            self._store_capture()
        return True

    def _start_capture(
        self,
        capture_scope: str,
        key: str | None,
        event_name: str,
        event_value: object,
        *,
        allow_payload_numbers: bool = False,
    ) -> None:
        self.capture = _Capture(allow_payload_numbers=allow_payload_numbers)
        self.capture_target = (capture_scope, key)
        if self.capture._is_complete_after_event(event_name, event_value):
            self._store_capture()

    def _accept_root_event(self, event_name: str, event_value: object) -> None:
        if event_name == "map_key":
            key = str(event_value)
            if key in self.seen_root_keys:
                raise AdmissionSealError(
                    "provider_directory_admission_metadata_duplicate"
                )
            self.seen_root_keys.add(key)
            if key == ADMISSION_GENERIC_PROOF_SUMMARY_KEY:
                raise AdmissionSealError(
                    "provider_directory_admission_reserved_metadata_key"
                )
            self.pending_key = key
            return
        if event_name == "end_map":
            self.complete = True
            return
        key = self.pending_key
        self.pending_key = None
        if key == UHC_CANONICAL_CONTENT_PROOF_METADATA_KEY:
            self._start_capture("root", key, event_name, event_value)
        elif key == PROVIDER_DIRECTORY_CONTENT_PROOF_METADATA_KEY:
            if event_name != "start_map":
                raise AdmissionSealError(
                    "provider_directory_admission_proof_invalid"
                )
            self.mode = "proof"
        else:
            self._start_capture(
                "root",
                key,
                event_name,
                event_value,
                allow_payload_numbers=True,
            )

    def _accept_proof_event(self, event_name: str, event_value: object) -> None:
        if event_name == "map_key":
            key = str(event_value)
            if key in self.seen_proof_keys:
                raise AdmissionSealError(
                    "provider_directory_admission_proof_duplicate"
                )
            self.seen_proof_keys.add(key)
            if key not in _SEMANTIC_PROOF_FIELDS:
                raise AdmissionSealError(
                    "provider_directory_admission_proof_keyset_invalid"
                )
            self.pending_key = key
            return
        if event_name == "end_map":
            self.mode = "root"
            return
        key = self.pending_key
        self.pending_key = None
        if key == "shards":
            if event_name != "start_array":
                raise AdmissionSealError(
                    "provider_directory_admission_shards_invalid"
                )
            self.mode = "shards"
            return
        self._start_capture("proof", key, event_name, event_value)

    def _accept_shard_event(self, event_name: str, event_value: object) -> None:
        if event_name == "end_array":
            self.mode = "proof"
            return
        if event_name != "start_map":
            raise AdmissionSealError(
                "provider_directory_admission_shard_shape_invalid"
            )
        self._start_capture("shard", None, event_name, event_value)

    def event(self, _prefix: str, event_name: str, event_value: object) -> None:
        """Accept one ijson event while enforcing the bounded grammar."""

        if self._is_capture_event(event_name, event_value):
            return
        if self.mode == "expect_root":
            if event_name != "start_map":
                raise AdmissionSealError(
                    "provider_directory_admission_metadata_invalid"
                )
            self.mode = "root"
        elif self.mode == "root":
            self._accept_root_event(event_name, event_value)
        elif self.mode == "proof":
            self._accept_proof_event(event_name, event_value)
        elif self.mode == "shards":
            self._accept_shard_event(event_name, event_value)

    def _proof_digest(self) -> str:
        digest = hashlib.sha256()
        digest.update(b"{")
        unsigned_proof_by_key = {
            key: proof_value
            for key, proof_value in self.proof_header.items()
            if key != "proof_sha256"
        }
        proof_keys = sorted(unsigned_proof_by_key.keys() | {"shards"})
        for index, key in enumerate(proof_keys):
            if index:
                digest.update(b",")
            digest.update(json.dumps(key).encode() + b":")
            if key != "shards":
                digest.update(
                    json.dumps(
                        unsigned_proof_by_key[key],
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


class _LimitedReader:
    def __init__(self, source: Any, remaining: int) -> None:
        self.source = source
        self.remaining = remaining

    def read(self, size: int = -1) -> bytes:
        """Read no more than the COPY field's declared remaining bytes."""

        if self.remaining <= 0:
            return b""
        if size < 0 or size > self.remaining:
            size = self.remaining
        data = self.source.read(size)
        self.remaining -= len(data)
        return data


def _copy_field_reader(copy_path: Path) -> tuple[Any, _LimitedReader]:
    copy_source = copy_path.open("rb")
    try:
        header = copy_source.read(19)
        if header != _COPY_SIGNATURE + struct.pack("!ii", 0, 0):
            raise AdmissionSealError(
                "provider_directory_admission_copy_header_invalid"
            )
        field_count_raw = copy_source.read(2)
        if (
            len(field_count_raw) != 2
            or struct.unpack("!h", field_count_raw)[0] != 1
        ):
            raise AdmissionSealError(
                "provider_directory_admission_copy_shape_invalid"
            )
        field_length_raw = copy_source.read(4)
        if len(field_length_raw) != 4:
            raise AdmissionSealError(
                "provider_directory_admission_copy_shape_invalid"
            )
        field_length = struct.unpack("!i", field_length_raw)[0]
        if field_length < 0 or field_length > ADMISSION_RAW_METADATA_MAX_BYTES:
            raise AdmissionSealError(
                "provider_directory_admission_copy_size_invalid"
            )
        return copy_source, _LimitedReader(copy_source, field_length)
    except BaseException:
        copy_source.close()
        raise
