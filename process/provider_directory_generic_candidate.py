# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Complete-key preparation for isolated generic resources and CMS witnesses."""

import json
import re
from collections.abc import Callable, Iterable, Iterator, Mapping

RESOURCE_COLUMNS = (
    "dataset_id",
    "resource_type",
    "resource_id",
    "payload_hash",
    "payload_json",
    "acquired_resource_sha256",
)
WITNESS_COLUMNS = (
    "dataset_id",
    "source_id",
    "release_id",
    "resource_type",
    "resource_id",
    "raw_payload_sha256",
    "normalized_payload_hash",
    "raw_payload_json",
)
COPY_MAX_ROWS = 4096
COPY_MAX_BYTES = 32 * 1024 * 1024
COPY_FRAME_BYTES = 21
_SHA256 = re.compile(r"[0-9a-f]{64}")


class GenericCandidateError(ValueError):
    """The isolated candidate cannot be completed without changing its contract."""


def _require(condition, code):
    if not condition:
        raise GenericCandidateError("provider_directory_generic_candidate_" + code)


def _key(resource_row, dataset_id):
    _require(isinstance(resource_row, Mapping) and resource_row.get("dataset_id") == dataset_id, "dataset_mismatch")
    resource_keys = tuple(resource_row.get(name) for name in ("resource_type", "resource_id"))
    _require(
        type(dataset_id) is str
        and 1 <= len(dataset_id) <= 96
        and all(type(value) is str and 1 <= len(value) <= maximum for value, maximum in zip(resource_keys, (64, 256))),
        "resource_identity_invalid",
    )
    return resource_keys


def _is_hash(value):
    return type(value) is str and _SHA256.fullmatch(value) is not None


def reduce_sorted_resources(
    resource_rows: Iterable[Mapping],
    dataset_id: str,
    *,
    prepare_row: Callable[[dict], dict],
    merge_rows: Callable[[dict, dict], dict],
) -> Iterator[dict]:
    """Fold one ordered key at a time with the existing canonical semantic code.

    The owner supplies rows ordered by resource type/id and acquisition ordinal.
    Callbacks perform the existing pure semantic validation/union, never SQL.
    Only one accumulated identity is retained; no partial key is emitted.
    """
    previous_key, accumulated = None, None
    for resource_row in resource_rows:
        key = _key(resource_row, dataset_id)
        _require(previous_key is None or key >= previous_key, "resource_order_invalid")
        prepared = prepare_row(dict(resource_row))
        _require(_key(prepared, dataset_id) == key and _is_hash(prepared.get("payload_hash")), "prepared_row_invalid")
        if key == previous_key:
            accumulated = merge_rows(accumulated, prepared)
            _require(
                _key(accumulated, dataset_id) == key and _is_hash(accumulated.get("payload_hash")), "merged_row_invalid"
            )
        else:
            if accumulated is not None:
                yield accumulated
            accumulated = prepared
            previous_key = key
    if accumulated is not None:
        yield accumulated


def reduce_sorted_witnesses(resource_rows: Iterable[Mapping], dataset_id: str, release_id: str) -> Iterator[dict]:
    """Require exact raw replay facts across a complete CMS resource identity."""
    _require(_is_hash(release_id), "release_invalid")
    previous_key, accumulated, previous_value = None, None, None
    for resource_row in resource_rows:
        key = _key(resource_row, dataset_id)
        _require(previous_key is None or key >= previous_key, "witness_order_invalid")
        _require(
            set(resource_row) == set(WITNESS_COLUMNS)
            and resource_row["source_id"] == "cms-npd"
            and resource_row["release_id"] == release_id
            and _is_hash(resource_row["raw_payload_sha256"])
            and _is_hash(resource_row["normalized_payload_hash"])
            and isinstance(resource_row["raw_payload_json"], dict),
            "witness_identity_invalid",
        )
        try:
            field_value = json.dumps(
                dict(resource_row), sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
            )
        except TypeError, ValueError:
            raise GenericCandidateError("provider_directory_generic_candidate_witness_payload_invalid") from None
        if key == previous_key:
            _require(field_value == previous_value, "witness_replay_conflict")
        else:
            if accumulated is not None:
                yield accumulated
            accumulated, previous_key, previous_value = dict(resource_row), key, field_value
    if accumulated is not None:
        yield accumulated


def copy_record_bytes(record: tuple) -> int:
    """Conservatively charge native framing, UTF-8 text and JSONB version bytes."""
    _require(type(record) is tuple and bool(record), "copy_record_invalid")
    row_bytes = 2
    for value in record:
        _require(value is None or type(value) in (str, int, bool, bytes), "copy_value_invalid")
        if value is None:
            size = 0
        elif type(value) is str:
            size = len(value.encode("utf-8")) + 1
        elif type(value) is bytes:
            size = len(value)
        else:
            size = 8
        row_bytes += 4 + size
    _require(COPY_FRAME_BYTES + row_bytes <= COPY_MAX_BYTES, "copy_record_too_large")
    return row_bytes


def copy_record_batches(records: Iterable[tuple]) -> Iterator[list[tuple]]:
    """Bound encoded COPY records without splitting a row or reordering input."""
    batch, batch_bytes = [], COPY_FRAME_BYTES
    for record in records:
        row_bytes = copy_record_bytes(record)
        if batch and (len(batch) == COPY_MAX_ROWS or batch_bytes + row_bytes > COPY_MAX_BYTES):
            yield batch
            batch, batch_bytes = [], COPY_FRAME_BYTES
        batch.append(record)
        batch_bytes += row_bytes
    if batch:
        yield batch
