# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pure full-row retention, factory sealing and bounded chunk contracts."""

import json
from dataclasses import FrozenInstanceError, replace
from datetime import UTC, date, datetime, timedelta, timezone

import pytest

from process import nppes_public_evidence_prepared_chunks as prepared
from process.nppes_public_evidence_rows import (
    COMMON_COLUMNS,
    MEMBER_COLUMNS,
    NPI_ENUMERATION_COLUMNS,
    SOURCE_LINK_COLUMNS,
    SOURCE_RECORD_COLUMNS,
)
from process.nppes_public_evidence_writer_contract import NppesPublicEvidenceWriterError


def _values(root, *prefix):
    return prefix + ("value",) * (len(prepared.COLUMNS[root]) - len(prefix))


def _roots():
    return {root: [] for root in prepared.COLUMNS}


@pytest.mark.parametrize("root", ("source_record", "member", "common", "source_link", "typed"))
def test_every_root_keeps_exact_column_order_and_all_values(root):
    values = tuple(None if index % 2 else 'status\t\n\r"\\雪' for index in range(len(prepared.COLUMNS[root])))
    row = prepared.prepare_nppes_row(root, values)
    assert row.root == root and row.values is values
    assert list(json.loads(row.payload)) == list(prepared.COLUMNS[root])
    assert json.loads(row.payload) == dict(zip(prepared.COLUMNS[root], values, strict=True))
    assert b'\\t\\n\\r\\"\\\\' in row.payload and "雪".encode() in row.payload
    rows = _roots()
    rows[root] = [row]
    assert prepared.chunk_payloads(rows) == {
        name: b"[" + row.payload + b"]" if name == root else b"[]" for name in rows
    }


def test_columns_and_primitive_encoding_are_exact():
    assert dict(prepared.COLUMNS) == dict(
        source_record=SOURCE_RECORD_COLUMNS,
        member=MEMBER_COLUMNS,
        common=COMMON_COLUMNS,
        source_link=SOURCE_LINK_COLUMNS,
        typed=NPI_ENUMERATION_COLUMNS,
    )
    primitive_values = _values(
        "member",
        None,
        b"\x00\xff",
        datetime(2026, 10, 18, 1, 2, 3, 456, tzinfo=UTC),
        date(2026, 10, 18),
        True,
        False,
        -(1 << 63),
        (1 << 63) - 1,
        "active",
    )
    expected = (
        None,
        "\\x00ff",
        "2026-10-18T01:02:03.000456+00:00",
        "2026-10-18",
        True,
        False,
        -(1 << 63),
        (1 << 63) - 1,
        "active",
    ) + ("value",) * (len(primitive_values) - 9)
    prepared_member = prepared.prepare_nppes_row("member", primitive_values)
    assert json.loads(prepared_member.payload) == dict(zip(MEMBER_COLUMNS, expected, strict=True))
    assert (
        prepared_member.payload
        == json.dumps(
            dict(zip(MEMBER_COLUMNS, expected, strict=True)), ensure_ascii=False, separators=(",", ":")
        ).encode()
    )
    with pytest.raises(TypeError):
        prepared.COLUMNS["member"] = ()


@pytest.mark.parametrize(
    "root,values",
    [
        ("unknown", ()),
        (None, ()),
        (1, ()),
        ("typed", []),
        ("typed", ()),
        ("typed", ("x",) * (len(NPI_ENUMERATION_COLUMNS) + 1)),
    ],
)
def test_root_and_exact_tuple_shape_are_required(root, values):
    with pytest.raises(NppesPublicEvidenceWriterError, match="^nppes_public_evidence_writer_invalid$"):
        prepared.prepare_nppes_row(root, values)


class _Text(str):
    pass


class _Integer(int):
    pass


@pytest.mark.parametrize(
    "value",
    [
        1.0,
        float("nan"),
        float("inf"),
        {},
        [],
        (),
        set(),
        bytearray(b"a"),
        memoryview(b"a"),
        object(),
        _Text("a"),
        _Integer(1),
        -(1 << 63) - 1,
        1 << 63,
        datetime(2026, 10, 18),
        datetime(2026, 10, 18, tzinfo=timezone(timedelta(hours=1))),
        "nul\x00text",
        "invalid\ud800text",
    ],
)
def test_non_sql_primitive_values_are_rejected_without_value_leaks(value):
    with pytest.raises(NppesPublicEvidenceWriterError, match="^nppes_public_evidence_writer_invalid$"):
        prepared.prepare_nppes_row("source_record", _values("source_record", value))


def test_rows_are_immutable_and_factory_sealed():
    row = prepared.prepare_nppes_row("typed", _values("typed"))
    for name, value in (("root", "member"), ("values", ()), ("payload", b"{}")):
        with pytest.raises(FrozenInstanceError):
            setattr(row, name, value)
    with pytest.raises(TypeError):
        row.values[0] = "changed"
    with pytest.raises(NppesPublicEvidenceWriterError):
        prepared.PreparedNppesRow("typed", row.values, row.payload)
    with pytest.raises(NppesPublicEvidenceWriterError):
        replace(row, payload=b"{}")
    rows = _roots()
    rows["typed"] = [object.__new__(prepared.PreparedNppesRow)]
    with pytest.raises(NppesPublicEvidenceWriterError):
        prepared.chunk_payloads(rows)


def test_join_never_reads_values_or_encodes_again(monkeypatch):
    rows_by_root = {root: [prepared.prepare_nppes_row(root, _values(root))] * 2 for root in prepared.COLUMNS}
    payloads_by_root = {
        root: b"[" + batch[0].payload + b"," + batch[1].payload + b"]" for root, batch in rows_by_root.items()
    }

    def forbidden(*args, **kwargs):
        pytest.fail("chunk joining reread or encoded a row")

    monkeypatch.setattr(prepared.json, "dumps", forbidden)
    monkeypatch.setattr(prepared.PreparedNppesRow, "values", property(forbidden))
    assert prepared.chunk_payloads(rows_by_root) == payloads_by_root


def test_chunk_has_all_roots_exact_lists_matching_rows_and_4096_limit():
    assert prepared.COPY_MAX_ROWS == 4096 and prepared.COPY_MAX_BYTES == 32 * 1024 * 1024
    rows_by_root = {root: [prepared.prepare_nppes_row(root, _values(root))] * 4096 for root in prepared.COLUMNS}
    assert all(len(json.loads(payload)) == 4096 for payload in prepared.chunk_payloads(rows_by_root).values())
    rows_by_root["typed"].append(rows_by_root["typed"][0])
    with pytest.raises(NppesPublicEvidenceWriterError):
        prepared.chunk_payloads(rows_by_root)
    malformed_inputs = [
        _roots() | {"extra": []},
        {key: [] for key in prepared.COLUMNS if key != "typed"},
        [],
        _roots() | {"typed": ()},
        _roots() | {"typed": [()]},
        _roots() | {"typed": [prepared.prepare_nppes_row("member", _values("member"))]},
    ]
    for invalid in malformed_inputs:
        with pytest.raises(NppesPublicEvidenceWriterError):
            prepared.chunk_payloads(invalid)


def test_total_byte_bound_counts_all_five_frames_commas_and_utf8(monkeypatch):
    row = prepared.prepare_nppes_row("source_record", _values("source_record", "雪"))
    rows = _roots() | {"source_record": [row, row]}
    exact = 10 + 2 * len(row.payload) + 1
    monkeypatch.setattr(prepared, "COPY_MAX_BYTES", exact)
    assert sum(map(len, prepared.chunk_payloads(rows).values())) == exact
    monkeypatch.setattr(prepared, "COPY_MAX_BYTES", exact - 1)
    with pytest.raises(NppesPublicEvidenceWriterError):
        prepared.chunk_payloads(rows)
    monkeypatch.setattr(prepared, "COPY_MAX_BYTES", 10 + len(row.payload))
    assert prepared.prepare_nppes_row("source_record", row.values).payload == row.payload
    monkeypatch.setattr(prepared, "COPY_MAX_BYTES", 9 + len(row.payload))
    with pytest.raises(NppesPublicEvidenceWriterError):
        prepared.prepare_nppes_row("source_record", row.values)
    monkeypatch.setattr(prepared, "COPY_MAX_BYTES", 9)
    with pytest.raises(NppesPublicEvidenceWriterError):
        prepared.chunk_payloads(_roots())
