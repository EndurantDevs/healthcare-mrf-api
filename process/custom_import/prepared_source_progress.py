# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded immutable header counters; payload and lease checks remain separate."""

from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass, replace

from process.custom_import.definition import canonical_json
from process.custom_import.object_processing_policy import ObjectProcessingPolicy
from process.custom_import.prepared_source_metadata import (
    _EMPTY_SHA256,
    PreparedPartMetadata,
    PreparedSourceMetadataError,
    _policy_limits,
    _summary_limits,
)
from process.custom_import.prepared_source_metadata import (
    _integer as _metadata_integer,
)

PREPARED_PROGRESS_CONTRACT = "custom-import/prepared-source-progress/v1"
MAX_PROGRESS_BYTES = 16 * 1024
_MAX_BIGINT = (1 << 63) - 1
_MAX_PARTS = 131_072
_MAX_MANIFEST_BYTES = 2 * 1024 * 1024
_ERROR = "prepared_source_progress_invalid"
_COUNT = r"(?:0|[1-9][0-9]{0,18})"
_STREAM_WIRE = (
    r'\{"byte_count":'
    + _COUNT
    + r',"eof":(?:false|true),"manifest_byte_count":'
    + _COUNT
    + r',"part_count":'
    + _COUNT
    + r',"record_count":'
    + _COUNT
    + r',"stream_slot":[1-9]\}'
)
_PROGRESS_WIRE = re.compile(
    r'\{"contract":"custom-import/prepared-source-progress/v1","streams":\['
    + _STREAM_WIRE
    + r"(?:,"
    + _STREAM_WIRE
    + r"){0,8}\]\}"
)


class PreparedSourceProgressError(ValueError):
    """Retained progress has invalid framing, counters or stream transitions."""


def _integer(value, minimum=0, maximum=_MAX_BIGINT):
    try:
        _metadata_integer(value, minimum, maximum)
    except PreparedSourceMetadataError:
        raise PreparedSourceProgressError(_ERROR) from None


@dataclass(frozen=True)
class PreparedStreamProgress:
    stream_slot: int
    part_count: int
    record_count: int
    byte_count: int
    manifest_byte_count: int
    eof: bool

    def __post_init__(self):
        _integer(self.stream_slot, 1, 9)
        _integer(self.part_count, 0, _MAX_PARTS)
        for value in (self.record_count, self.byte_count, self.manifest_byte_count):
            _integer(value)
        if type(self.eof) is not bool:
            raise PreparedSourceProgressError(_ERROR)
        if self.part_count == 0:
            if any((self.record_count, self.byte_count, self.manifest_byte_count, self.eof)):
                raise PreparedSourceProgressError(_ERROR)
        else:
            _integer(self.manifest_byte_count, 2 * self.part_count, _MAX_MANIFEST_BYTES * self.part_count)
            if self.record_count == 0:
                if self.part_count != 1 or self.byte_count != 0:
                    raise PreparedSourceProgressError(_ERROR)
                empty = PreparedPartMetadata(self.stream_slot, 1, 1, 0, 0, _EMPTY_SHA256)
                if self.manifest_byte_count != len(empty.canonical.encode("utf-8")):
                    raise PreparedSourceProgressError(_ERROR)
            elif self.byte_count == 0 or self.part_count > self.record_count:
                raise PreparedSourceProgressError(_ERROR)


def totals(progress: tuple[PreparedStreamProgress, ...]) -> tuple[int, int, int, int]:
    """Validate the bounded stream tuple and return its four aggregate counters."""

    if type(progress) is not tuple or not 1 <= len(progress) <= 9:
        raise PreparedSourceProgressError(_ERROR)
    counts = [0, 0, 0, 0]
    for slot, stream in enumerate(progress, 1):
        if type(stream) is not PreparedStreamProgress or stream.stream_slot != slot:
            raise PreparedSourceProgressError(_ERROR)
        for index, value in enumerate(
            (stream.part_count, stream.record_count, stream.byte_count, stream.manifest_byte_count)
        ):
            counts[index] += value
            _integer(counts[index], 0, _MAX_PARTS if index == 0 else _MAX_BIGINT)
    return tuple(counts)


def _validate_policy(progress, policy):
    counts = totals(progress)
    try:
        _policy_limits(policy)
        for stream in progress:
            if stream.part_count:
                _summary_limits(stream.part_count, stream.record_count, stream.byte_count, policy)
        if counts[0]:
            _summary_limits(*counts[:3], policy)
    except PreparedSourceMetadataError:
        raise PreparedSourceProgressError(_ERROR) from None


def empty_progress(stream_count: int, policy: ObjectProcessingPolicy) -> tuple[PreparedStreamProgress, ...]:
    """Create explicit zero counters without claiming that any stream reached EOF."""

    _integer(stream_count, 1, 9)
    progress = tuple(PreparedStreamProgress(slot, 0, 0, 0, 0, False) for slot in range(1, stream_count + 1))
    _validate_policy(progress, policy)
    return progress


def canonical_progress(progress: tuple[PreparedStreamProgress, ...]) -> str:
    """Encode only the progress contract and independent per-stream counters."""

    totals(progress)
    canonical = canonical_json(
        {"contract": PREPARED_PROGRESS_CONTRACT, "streams": [asdict(stream) for stream in progress]}
    )
    if len(canonical.encode("utf-8")) > MAX_PROGRESS_BYTES:
        raise PreparedSourceProgressError(_ERROR)
    return canonical


def parse_progress(
    canonical: str, stream_count: int, policy: ObjectProcessingPolicy
) -> tuple[PreparedStreamProgress, ...]:
    """Reject noncanonical structure and allocation abuse before JSON decoding."""

    _integer(stream_count, 1, 9)
    # The closed ASCII grammar bounds depth, array length and integer digits before parsing.
    if (
        type(canonical) is not str
        or not 1 <= len(canonical) <= MAX_PROGRESS_BYTES
        or _PROGRESS_WIRE.fullmatch(canonical) is None
    ):
        raise PreparedSourceProgressError(_ERROR)
    try:
        document = json.loads(canonical)
        progress = tuple(PreparedStreamProgress(**entry) for entry in document["streams"])
        if len(progress) != stream_count or canonical_progress(progress) != canonical:
            raise PreparedSourceProgressError(_ERROR)
        _validate_policy(progress, policy)
        return progress
    except ValueError, TypeError:
        raise PreparedSourceProgressError(_ERROR) from None


def append_progress(
    progress: tuple[PreparedStreamProgress, ...], part: PreparedPartMetadata, policy: ObjectProcessingPolicy
) -> tuple[PreparedStreamProgress, ...]:
    """Append the next bounded part counters; a retained empty part is already final."""

    _validate_policy(progress, policy)
    if type(part) is not PreparedPartMetadata or part.stream_slot > len(progress):
        raise PreparedSourceProgressError(_ERROR)
    stream = progress[part.stream_slot - 1]
    if (
        stream.eof
        or (stream.part_count > 0 and stream.record_count == 0)
        or part.part_ordinal != stream.part_count + 1
        or part.first_source_ordinal != stream.record_count + 1
    ):
        raise PreparedSourceProgressError(_ERROR)
    _integer(part.record_count, 0, policy.prepared["part_row_limit"])
    _integer(part.byte_count, 0, policy.prepared["part_byte_limit"])
    updated = replace(
        stream,
        part_count=stream.part_count + 1,
        record_count=stream.record_count + part.record_count,
        byte_count=stream.byte_count + part.byte_count,
        manifest_byte_count=stream.manifest_byte_count + len(part.canonical.encode("utf-8")),
    )
    result = progress[: part.stream_slot - 1] + (updated,) + progress[part.stream_slot :]
    _validate_policy(result, policy)
    return result


def close_stream(
    progress: tuple[PreparedStreamProgress, ...], slot: int, policy: ObjectProcessingPolicy
) -> tuple[PreparedStreamProgress, ...]:
    """Mark proven decoder EOF once, requiring its nonempty or canonical empty part."""

    _validate_policy(progress, policy)
    _integer(slot, 1, len(progress))
    stream = progress[slot - 1]
    if stream.part_count == 0:
        raise PreparedSourceProgressError(_ERROR)
    if stream.eof:
        return progress
    return progress[: slot - 1] + (replace(stream, eof=True),) + progress[slot:]
