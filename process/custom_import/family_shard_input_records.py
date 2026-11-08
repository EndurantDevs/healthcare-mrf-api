# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Lossless owner-spool records; admission remains the complete family's job."""

from contextlib import closing
from dataclasses import dataclass, replace
from datetime import date, datetime, timezone
from decimal import Decimal
from zoneinfo import ZoneInfo

import msgpack

from process.custom_import.build_source import _source_row_shape, _StreamContext
from process.custom_import.family_raw_key import RawFamilyKeyError, raw_family_key_evidence
from process.custom_import.runner_codec import child_key_document, root_key_evidence_from_tuple
from process.custom_import.runner_types import CandidateRunnerError
from process.custom_import.snowflake_bundle_replay import _normalized_integer_replay_values, _stream_fields
from process.custom_import.source_decoded_preparation import DecodedSourcePreparation


@dataclass(frozen=True)
class ShardInputRecord:
    """One occurrence; SOURCE row/source ordinals stay zero based, parts one based."""

    stream_slot: int
    collection_slot: int
    part_ordinal: int
    row_ordinal: int
    source_ordinal: int
    values: tuple
    raw_parent_key: tuple | None
    typed_parent_key: tuple | None
    child_key: str | None
    rejection_code: str | None

    def normalized_values(self, definition, stream):
        """Keep the original Decimal spelling while sharing integer replay semantics."""
        return _normalized_integer_replay_values(dict(self.values), _stream_fields(definition, stream))


def prepare_record(
    definition, stream, original_values, *, stream_slot, collection_slot, part_ordinal, row_ordinal, source_ordinal
):
    """Reuse SOURCE rejection priority and both existing equality domains."""
    fields = _stream_fields(definition, stream)
    if tuple(original_values) != tuple(field.field_id for field in fields):
        raise ValueError("shard input fields differ from the retained stream")
    normalized = _normalized_integer_replay_values(dict(original_values), fields)
    _fields, key, rejection = _source_row_shape(definition, stream, normalized)
    raw = raw_family_key_evidence(key, maximum_canonical_bytes=268_435_456)
    typed = root_key_evidence_from_tuple(definition, key)
    if rejection is None and typed is None:
        rejection = "field_type_invalid"
    child_key = (
        child_key_document(definition, stream.child_collection, normalized)
        if rejection is None and stream.record_kind == "child"
        else None
    )
    return ShardInputRecord(
        stream_slot,
        collection_slot,
        part_ordinal,
        row_ordinal,
        source_ordinal,
        tuple(original_values.items()),
        raw,
        typed,
        child_key,
        rejection,
    )


def shard_for(record, shard_count):
    """Hash buckets retain full keys; no hash equality grants family admission."""
    return (
        None if record.typed_parent_key is None else int.from_bytes(record.typed_parent_key[1][:8], "big") % shard_count
    )


class PartitionPreparation(DecodedSourcePreparation):
    """Reuse bounded native batches without advancing SOURCE page admission."""

    def _serial(self, records):
        for record in records:
            self.check()
            yield record.original, None

    def _prepared(self, decoded_inputs, held_bytes=None):
        completed = 0
        rows = super()._prepared(decoded_inputs, held_bytes)
        try:
            for original, prepared in rows:
                completed += 1
                yield original, prepared
        except (CandidateRunnerError, RawFamilyKeyError) as error:
            if (type(error), error.args) not in (
                (CandidateRunnerError, ("one source occurrence exceeds the admitted page byte limit",)),
                (RawFamilyKeyError, ("raw family key exceeds the canonical byte limit",)),
            ):
                raise
            yield from self._serial(decoded_inputs[completed:])
        finally:
            rows.close()


def partition_preparations(request, registry, build_id, memory_bytes):
    """Bind existing CPU bounds to the original concrete SOURCE contexts."""
    if type(build_id) is not int or build_id <= 0 or type(memory_bytes) is not int or memory_bytes <= 0:
        raise ValueError("shard native preparation context differs")
    return {
        slot: PartitionPreparation(_StreamContext(request, registry, build_id, stream), memory_bytes=memory_bytes)
        for slot, stream in enumerate(request.definition.source_streams, 1)
    }


def _provisional_records(decoded, slot, collection_slot, part, source_start):
    for ordinal, record in enumerate(decoded):
        original = ShardInputRecord(
            slot,
            collection_slot,
            part,
            ordinal,
            source_start + ordinal,
            tuple(record.values.items()),
            None,
            None,
            None,
            None,
        )
        yield original, len(encode_record(original))


def partition_records(definition, stream, decoded, slot, collection_slot, part, source_start, preparation=None):
    """Keep original values and serial priority while borrowing native key work."""
    if preparation is None or preparation.native is None:
        for ordinal, decoded_record in enumerate(decoded):
            yield prepare_record(
                definition,
                stream,
                decoded_record.values,
                stream_slot=slot,
                collection_slot=collection_slot,
                part_ordinal=part,
                row_ordinal=ordinal,
                source_ordinal=source_start + ordinal,
            )
        return
    entries = _provisional_records(decoded, slot, collection_slot, part, source_start)
    with closing(preparation.rows(entries)) as prepared_rows:
        for original, prepared in prepared_rows:
            if prepared is None:
                yield prepare_record(
                    definition,
                    stream,
                    dict(original.values),
                    stream_slot=slot,
                    collection_slot=collection_slot,
                    part_ordinal=part,
                    row_ordinal=original.row_ordinal,
                    source_ordinal=original.source_ordinal,
                )
            else:
                yield replace(
                    original,
                    raw_parent_key=prepared.raw_key,
                    typed_parent_key=prepared.typed_key,
                    child_key=prepared.child_key if prepared.rejection is None else None,
                    rejection_code=None if prepared.rejection is None else prepared.rejection.code,
                )


def _encode_scalar(value):
    if type(value) is Decimal:
        return msgpack.ExtType(1, str(value).encode("ascii"))
    if type(value) is date:
        return msgpack.ExtType(2, value.isoformat().encode("ascii"))
    if type(value) is datetime:
        zone = getattr(value.tzinfo, "key", None) or getattr(value.tzinfo, "zone", None)
        return msgpack.ExtType(
            3, msgpack.packb([value.isoformat(), value.fold, zone, value.tzname()], use_bin_type=True)
        )
    raise TypeError("shard input contains an unsupported scalar")


def _decode_scalar(code, value):
    if code == 1:
        return Decimal(value.decode("ascii"))
    if code == 2:
        return date.fromisoformat(value.decode("ascii"))
    if code == 3:
        text, fold, zone, name = msgpack.unpackb(value, raw=False, use_list=False)
        if type(text) is not str or type(fold) is not int or fold not in (0, 1):
            raise ValueError("shard input timestamp is malformed")
        result = datetime.fromisoformat(text)
        if zone is not None:
            if type(zone) is not str or result.tzinfo is None:
                raise ValueError("shard input timestamp zone is malformed")
            result = result.astimezone(ZoneInfo(zone)).replace(fold=fold)
            if result.isoformat() != text:
                raise ValueError("shard input timestamp zone offset differs")
        elif result.tzinfo is not None and name is not None:
            if type(name) is not str:
                raise ValueError("shard input timestamp zone name is malformed")
            result = result.replace(tzinfo=timezone(result.utcoffset(), name))
        return result.replace(fold=fold)
    raise ValueError("shard input scalar extension is unsupported")


def encode_record(record):
    """Keep Decimal exponent, scalar types, presence and field order losslessly."""
    return msgpack.packb(tuple(getattr(record, field) for field in record.__dataclass_fields__), default=_encode_scalar)


def decode_record(payload):
    """Decode a prepared occurrence while preserving its original opaque values."""
    values = msgpack.unpackb(payload, raw=False, use_list=False, ext_hook=_decode_scalar, strict_map_key=True)
    if not isinstance(values, tuple) or len(values) != len(ShardInputRecord.__dataclass_fields__):
        raise ValueError("shard input record shape differs")
    record = ShardInputRecord(*values)
    if any(
        type(value) is not int or value < minimum for value, minimum in zip(values[:5], (1, 0, 1, 0, 0), strict=True)
    ):
        raise ValueError("shard input provenance differs")
    return record
