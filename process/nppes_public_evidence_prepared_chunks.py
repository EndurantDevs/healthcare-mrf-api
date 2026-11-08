# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Retain canonical NPPES row payloads before COPY, then join without replay."""

import json
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from types import MappingProxyType

from process.nppes_public_evidence_rows import (
    COMMON_COLUMNS,
    MEMBER_COLUMNS,
    NPI_ENUMERATION_COLUMNS,
    SOURCE_LINK_COLUMNS,
    SOURCE_RECORD_COLUMNS,
)
from process.nppes_public_evidence_writer_contract import writer_error
from process.provider_directory_generic_candidate import COPY_MAX_BYTES, COPY_MAX_ROWS

COLUMNS = MappingProxyType(
    {
        "source_record": SOURCE_RECORD_COLUMNS,
        "member": MEMBER_COLUMNS,
        "common": COMMON_COLUMNS,
        "source_link": SOURCE_LINK_COLUMNS,
        "typed": NPI_ENUMERATION_COLUMNS,
    }
)
_SEAL = object()


@dataclass(frozen=True, slots=True, repr=False, init=False)
class PreparedNppesRow:
    """Factory-sealed immutable COPY values and their original full-row payload."""

    root: str
    values: tuple[object, ...]
    payload: bytes
    _seal: object = field(init=False, compare=False)

    def __new__(cls, *args, **kwargs):
        raise writer_error()


def _json_value(value):
    if value is None or type(value) is bool:
        return value
    if type(value) is str and "\x00" not in value:
        return value
    if type(value) is int and -(1 << 63) <= value < 1 << 63:
        return value
    if type(value) is bytes:
        return "\\x" + value.hex()
    if type(value) is datetime and value.tzinfo is not None and value.utcoffset() == timedelta(0):
        return value.isoformat()
    if type(value) is date:
        return value.isoformat()
    raise writer_error()


def prepare_nppes_row(root, values):
    """Validate one exact SQL row and encode every column once."""
    if type(root) is not str or root not in COLUMNS or type(values) is not tuple or len(values) != len(COLUMNS[root]):
        raise writer_error()
    try:
        payload = json.dumps(
            dict(zip(COLUMNS[root], map(_json_value, values), strict=True)),
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
        ).encode("utf-8")
    except TypeError, ValueError, OverflowError, UnicodeError:
        raise writer_error() from None
    if len(payload) + 2 * len(COLUMNS) > COPY_MAX_BYTES:
        raise writer_error()
    row = object.__new__(PreparedNppesRow)
    for name, value in (("root", root), ("values", values), ("payload", payload), ("_seal", _SEAL)):
        object.__setattr__(row, name, value)
    return row


def chunk_payloads(rows_by_root):
    """Join only sealed payload bytes within the complete five-root budget."""
    if type(rows_by_root) is not dict or rows_by_root.keys() != COLUMNS.keys():
        raise writer_error()
    size, payloads_by_root = 2 * len(COLUMNS), {}
    if size > COPY_MAX_BYTES:
        raise writer_error()
    for root in COLUMNS:
        rows = rows_by_root[root]
        if type(rows) is not list or len(rows) > COPY_MAX_ROWS:
            raise writer_error()
        pieces = []
        for row in rows:
            if type(row) is not PreparedNppesRow or getattr(row, "_seal", None) is not _SEAL or row.root != root:
                raise writer_error()
            payload = row.payload
            size += len(payload) + bool(pieces)
            if size > COPY_MAX_BYTES:
                raise writer_error()
            pieces.append(payload)
        payloads_by_root[root] = b"[" + b",".join(pieces) + b"]"
    return payloads_by_root
