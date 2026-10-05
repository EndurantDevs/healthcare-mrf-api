# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Pure page-to-COPY conversion; part boundaries do not establish source EOF."""

from __future__ import annotations

import hashlib
import json
import re
import struct
from collections.abc import Iterable
from typing import TYPE_CHECKING, NamedTuple
from uuid import UUID

if TYPE_CHECKING:
    from process.custom_import.build_source import _PreparedRow, _SourcePage, _StreamContext

MAX_BATCH_ROWS = 100_000
MAX_BATCH_BYTES = 268_435_456
_MAX_BIGINT = (1 << 63) - 1
_MAX_INTEGER = (1 << 31) - 1


class LandingRow(NamedTuple):
    batch_id: UUID
    pack_ordinal: int
    pack_sha256: bytes
    source_part_ordinal: int
    part_row_ordinal: int
    source_ordinal: int
    raw_parent_key_canonical: str | None
    raw_parent_key_sha256: bytes | None
    canonical_logical_key: str | None
    logical_key_sha256: bytes | None
    canonical_payload: str | None
    payload_sha256: bytes | None
    canonical_child_key: str | None
    child_key_sha256: bytes | None
    rejection_code: str | None
    rejection_root_key_canonical: str | None
    rejection_root_key_sha256: bytes | None
    canonical_evidence: str | None


LANDING_COLUMNS = LandingRow._fields


class LandingBatch(NamedTuple):
    records: tuple[LandingRow, ...]
    byte_count: int
    pack_count: int
    manifest_sha256: bytes


def landing_manifest_sha256(records: Iterable[LandingRow]) -> bytes:
    """Fingerprint exact COPY values and positions, excluding the batch UUID."""

    digest = hashlib.sha256(b"custom-import/landing-manifest/v1\x00")
    for record in records:
        for value in record[1:]:
            if value is None:
                digest.update(b"n")
            elif type(value) is int:
                digest.update(b"i" + struct.pack("!q", value))
            else:
                encoded = value.encode("utf-8") if type(value) is str else value
                digest.update((b"s" if type(value) is str else b"b") + struct.pack("!Q", len(encoded)))
                digest.update(encoded)
    return digest.digest()


def _integer(value, label, minimum=0, maximum=_MAX_BIGINT):
    if type(value) is not int or not minimum <= value <= maximum:
        raise ValueError(f"{label} is outside its admitted integer range")


def _pair(text, digest, label):
    if text is None and digest is None:
        return
    if type(text) is not str or type(digest) is not bytes or len(digest) != 32:
        raise ValueError(f"{label} requires canonical text and a SHA-256 digest")


def _key_pair(key, label):
    if key is None:
        return None, None
    if type(key) is not tuple or len(key) != 2:
        raise ValueError(f"{label} is malformed")
    _pair(*key, label)
    if key[0] is None:
        raise ValueError(f"{label} is malformed")
    return key


def _rejection_values(rejection, context, typed_key):
    """Validate the original rejection identity/evidence without mutating it."""
    code = rejection.code
    rejection_text, rejection_hash = rejection.canonical_root_key, rejection.root_key_sha256
    evidence = rejection.canonical_evidence
    _pair(rejection_text, rejection_hash, "rejection root key")
    if type(code) is not str or re.fullmatch(r"[a-z][a-z0-9_]{0,62}", code) is None:
        raise ValueError("rejection code is malformed")
    if (rejection_text, rejection_hash) != typed_key:
        raise ValueError("rejection identity differs from its prepared typed key")
    if type(evidence) is not str or json.loads(evidence) != {
        "code": code,
        "contract": "custom-import-rejection/v1",
        "root_key_sha256": None if rejection_hash is None else rejection_hash.hex(),
    }:
        raise ValueError("rejection evidence differs from its prepared identity")
    root_only_codes = {"root_not_object", "root_key_missing", "entity_binding_invalid"}
    child_only_codes = {"child_not_object", "orphan_child", "child_key_missing"}
    if code in (child_only_codes if context.stream.record_kind == "root" else root_only_codes):
        raise ValueError("rejection shape differs from its stream")
    if (
        any(
            getattr(rejection, name) != getattr(context.request, name)
            for name in ("dataset_id", "definition_revision_id", "schema_revision_id", "execution_id")
        )
        or rejection.producing_fence != context.request.fence
    ):
        raise ValueError("rejection identity differs from its source context")
    return code, rejection_text, rejection_hash, evidence


def _row_values(prepared: _PreparedRow, context: _StreamContext, fields):
    """Preserve native accepted/rejected COPY shape and canonical byte accounting."""
    from process.custom_import.runner_codec import payload_values

    raw_text, raw_hash = _key_pair(prepared.raw_key, "raw key")
    typed_text, typed_hash = _key_pair(prepared.typed_key, "typed key")
    _pair(prepared.payload, prepared.payload_hash, "payload")
    _pair(prepared.child_key, prepared.child_hash, "child key")
    rejection = prepared.rejection
    if rejection is None:
        if raw_text is None or typed_text is None or prepared.payload is None:
            raise ValueError("accepted row lacks required identity or payload")
        if (prepared.child_key is not None) != (context.stream.record_kind == "child"):
            raise ValueError("prepared row shape differs from its stream")
    elif prepared.payload is not None or prepared.child_key is not None:
        raise ValueError("rejected row must not contain an accepted payload or child key")
    texts = [raw_text, typed_text, prepared.payload, prepared.child_key]
    if rejection is not None:
        texts.extend((rejection.canonical_root_key, rejection.canonical_evidence))
    elif context.stream.record_kind == "child":
        texts.append(typed_text)  # Native accepted-child accounting retains the parent twice.
    if any(type(document) is not str for document in texts if document is not None):
        raise ValueError("prepared canonical documents must be text")
    byte_count = sum(len(document.encode("utf-8")) for document in texts if document is not None)
    _integer(prepared.byte_count, "prepared byte count")
    if prepared.byte_count != byte_count:
        raise ValueError("prepared byte count differs from canonical UTF-8 documents")
    if byte_count > context.request.page_byte_limit:
        raise ValueError("one source occurrence exceeds its admitted page byte limit")
    rejection_values = (None, None, None, None)
    if rejection is None:
        payload_values(fields, prepared.payload, label="landing source payload")
    else:
        rejection_values = _rejection_values(rejection, context, (typed_text, typed_hash))
    landing_values = (
        raw_text,
        raw_hash,
        typed_text,
        typed_hash,
        prepared.payload,
        prepared.payload_hash,
        prepared.child_key,
        prepared.child_hash,
        *rejection_values,
    )
    return landing_values, byte_count


def _landing_fields(context, batch_id, first_pack_ordinal, row_limit, byte_limit):
    """Check hard batch/request/stream bounds before consuming prepared pages."""
    if type(batch_id) is not UUID:
        raise ValueError("a server-provided batch UUID is required")
    _integer(first_pack_ordinal, "first pack ordinal")
    _integer(row_limit, "batch row limit", 1, MAX_BATCH_ROWS)
    _integer(byte_limit, "batch byte limit", 1, MAX_BATCH_BYTES)
    _integer(context.request.page_row_limit, "page row limit", 1, 256)
    _integer(context.request.page_byte_limit, "page byte limit", 1, MAX_BATCH_BYTES)
    if context.stream not in context.request.definition.source_streams:
        raise ValueError("source stream differs from its definition")
    if context.stream.record_kind not in {"root", "child"} or (
        (context.stream.child_collection is None) != (context.stream.record_kind == "root")
    ):
        raise ValueError("source stream kind and collection differ")
    return tuple(
        field for field in context.request.definition.fields if field.collection == context.stream.child_collection
    )


def _source_page_bounds(context, page):
    """Validate native page positions and row bounds before continuity checks."""
    _integer(page.part_ordinal, "source part ordinal", 1, _MAX_INTEGER)
    _integer(page.first_row, "first part row ordinal")
    _integer(page.first_source, "first source ordinal")
    if type(page.records) is not tuple or not 1 <= len(page.records) <= context.request.page_row_limit:
        raise ValueError("source page is empty or exceeds its admitted row limit")
    _integer(page.first_row + len(page.records) - 1, "last part row ordinal")
    _integer(page.first_source + len(page.records) - 1, "last source ordinal")


def _page_landing_rows(context, page, fields, batch_id, pack_ordinal, remaining_bytes):
    """Convert one bounded native pack, retaining the original sorted hash."""
    from process.custom_import.runner_codec import pack_hash

    _integer(pack_ordinal, "pack ordinal")
    for prepared in page.records:
        _integer(prepared.byte_count, "prepared byte count")
        if prepared.byte_count > context.request.page_byte_limit:
            raise ValueError("one source occurrence exceeds its admitted page byte limit")
    page_bytes = sum(prepared.byte_count for prepared in page.records)
    if page_bytes > context.request.page_byte_limit:
        raise ValueError("source page exceeds its admitted byte limit")
    if page_bytes > remaining_bytes:
        raise ValueError("landing batch exceeds its admitted byte limit")
    page_values = [_row_values(prepared, context, fields) for prepared in page.records]
    pack_digest = pack_hash(
        context.stream.child_collection or "root",
        [prepared.payload_hash for prepared in page.records if prepared.payload_hash is not None],
    )
    landing_rows = [
        LandingRow(
            batch_id,
            pack_ordinal,
            pack_digest,
            page.part_ordinal,
            page.first_row + offset,
            page.first_source + offset,
            *landing_values,
        )
        for offset, (landing_values, _) in enumerate(page_values)
    ]
    return landing_rows, page_bytes


def encode_landing_batch(
    context: _StreamContext,
    pages: Iterable[_SourcePage],
    *,
    batch_id: UUID,
    first_pack_ordinal: int,
    row_limit: int = MAX_BATCH_ROWS,
    byte_limit: int = MAX_BATCH_BYTES,
) -> LandingBatch:
    """Keep native page packs intact inside one bounded landing batch.

    The server supplies ``batch_id`` and verifies the first durable position.
    This codec validates continuity inside the batch without allocating any
    database identity. ``byte_count`` is native canonical-document accounting,
    including both child parent-key copies, rather than COPY wire framing.
    Advancing a part ordinal neither finishes a part nor freezes the stream.
    """

    fields = _landing_fields(context, batch_id, first_pack_ordinal, row_limit, byte_limit)
    landing_records = []
    byte_count = pack_count = 0
    previous = None
    for page in pages:
        _source_page_bounds(context, page)
        if len(landing_records) + len(page.records) > row_limit:
            raise ValueError("landing batch exceeds its admitted row limit")
        if previous is not None:
            previous_part, next_row, next_source = previous
            if page.first_source != next_source or page.part_ordinal < previous_part:
                raise ValueError("source pages have a gap, duplicate position, or reversed part")
            if page.first_row != (next_row if page.part_ordinal == previous_part else 0):
                raise ValueError("source pages have a part-row gap or duplicate position")
        pack_ordinal = first_pack_ordinal + pack_count
        page_rows, page_bytes = _page_landing_rows(
            context, page, fields, batch_id, pack_ordinal, byte_limit - byte_count
        )
        landing_records.extend(page_rows)
        byte_count += page_bytes
        pack_count += 1
        previous = page.part_ordinal, page.first_row + len(page.records), page.first_source + len(page.records)
    if not landing_records:
        raise ValueError("landing batch must contain source occurrences")
    return LandingBatch(tuple(landing_records), byte_count, pack_count, landing_manifest_sha256(landing_records))
