# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact predecessor-row comparisons for closed NPPES evidence families."""

from __future__ import annotations

import asyncio
import json
import re
from collections import Counter
from dataclasses import replace
from datetime import date, datetime, timedelta
from types import MappingProxyType

import pytest

from process import nppes_public_evidence_prepared_chunks as chunks
from process import nppes_public_evidence_validation as validation
from process.nppes_public_evidence_archive import NppesPublicEvidenceArchiveError
from process.nppes_public_evidence_members import NppesPrimaryCsvRows
from process.nppes_public_evidence_prepared_chunks import COLUMNS, chunk_payloads, prepare_nppes_row
from process.nppes_public_evidence_replay import prepare_nppes_registry_replay
from process.nppes_public_evidence_rows import MEMBER_COLUMNS, NPI_ENUMERATION_COLUMNS, NppesRegistryDatabaseRowEncoder
from process.nppes_public_evidence_writer_contract import NppesPublicEvidenceWriterError
from public_evidence import nppes_registry_row_projection as row_projection
from public_evidence.nppes_registry_error import NppesRegistryReplayError
from public_evidence.nppes_registry_row_projection import _temporal_projection as temporal_reference
from public_evidence.nppes_registry_storage_contract import NppesRegistryMemberEncoder
from tests.nppes_public_evidence_process_support import prepared_archive
from tests.public_evidence_nppes_registry_support import (
    active_type_1_row,
    equal_day_reactivated_type_1_row,
    future_deactivation_type_1_row,
    future_last_update_type_1_row,
    orphan_reactivated_type_2_row,
    sparse_deactivated_row,
)
from tests.test_nppes_public_evidence_replay import _config

_TABLES = {root: f"candidate_{root}" for root in COLUMNS}


async def _prepared(tmp_path, rows=None):
    archive = prepared_archive(
        tmp_path,
        "NPPES_Data_Dissemination_July_2026_V2.zip",
        "20260712",
        (active_type_1_row(), sparse_deactivated_row()) if rows is None else rows,
    )
    return await prepare_nppes_registry_replay(archive, _config())


def _predecessor_rows(prepared):
    encoder = NppesRegistryMemberEncoder(prepared.manifest, prepared.header, prepared.archive_observation)
    database = NppesRegistryDatabaseRowEncoder(prepared.manifest.release, prepared.admission_row)
    rows_by_root = {root: [] for root in _TABLES}
    with NppesPrimaryCsvRows(prepared.archive) as source:
        for raw in source:
            encoded, member = encoder.encode(raw)
            rows_by_root["source_record"].append(database.source_record(encoded.source_record))
            rows_by_root["member"].append(database.member(member))
            projected = database.projected(encoded)
            if projected is not None:
                for root, row in zip(("common", "source_link", "typed"), projected):
                    rows_by_root[root].append(row)
    assert encoder.finish() == prepared.manifest
    return rows_by_root


def _prepared_chunks(rows_by_root, row_limit):
    retained_chunks = []
    for start in range(0, len(rows_by_root["member"]), row_limit):
        selected_members = rows_by_root["member"][start : start + row_limit]
        evidence_refs = {member[MEMBER_COLUMNS.index("evidence_ref")] for member in selected_members}
        prepared_by_root = {}
        for root, original_rows in rows_by_root.items():
            selected_rows = original_rows[start : start + row_limit]
            if root not in {"source_record", "member"}:
                selected_rows = [
                    record for record in original_rows if record[COLUMNS[root].index("evidence_ref")] in evidence_refs
                ]
            prepared_by_root[root] = [prepare_nppes_row(root, record) for record in selected_rows]
        retained_chunks.append(MappingProxyType(chunk_payloads(prepared_by_root)))
    return tuple(retained_chunks)


def _decoded_value(column, encoded):
    if encoded is None:
        return None
    if column.endswith("_sha256"):
        assert type(encoded) is str and encoded.startswith("\\x")
        return bytes.fromhex(encoded[2:])
    if column in {"provider_enumeration_date", "last_update_date", "npi_deactivation_date", "npi_reactivation_date"}:
        return date.fromisoformat(encoded)
    if column in {"observed_at", "effective_start_at", "effective_end_at"}:
        return datetime.fromisoformat(encoded)
    return encoded


def _expected_chunk_rows(root, payload):
    return [tuple(_decoded_value(column, record[column]) for column in COLUMNS[root]) for record in json.loads(payload)]


def _queried_root(sql):
    actual = sql.split("actual AS (", 1)[-1]
    physical = re.search(r'FROM "evidence"\."(candidate_[a-z_]+)"', actual).group(1)
    return next(root for root, table_name in _TABLES.items() if table_name == physical)


class _FamilyConnection:
    """Freeze initial chunks separately from mutable actual rows; simulate SQL only."""

    def __init__(self, rows, row_limit=chunks.COPY_MAX_ROWS):
        self.rows = rows
        self.chunks = _prepared_chunks(rows, row_limit)
        self.calls = []
        self.batches = []

    def _inventory_differs(self, root, parameters):
        columns, records = COLUMNS[root], self.rows[root]
        owner_column = "admission_ref" if root == "member" else "nppes_admission_ref"
        owner_columns = ("source_release_ref", "source_release_contract_sha256", "source_kind", owner_column)
        return len(records) != parameters[-1] or any(
            tuple(record[columns.index(column)] for column in owner_columns) != parameters[:4] for record in records
        )

    async def fetchval(self, sql, *parameters):
        self.calls.append((sql, parameters))
        if not parameters:
            return False
        if sql.lstrip().startswith("WITH eligible_dates"):
            return _has_lifecycle_drift(self.rows, *parameters)
        root = _queried_root(sql)
        if not sql.startswith("WITH expected"):
            return self._inventory_differs(root, parameters)
        assert len(parameters) == 2
        assert 'FROM "evidence"."candidate_prepared" WHERE chunk_ordinal=$1' in sql
        ordinal, admission_ref = parameters
        expected_rows = _expected_chunk_rows(root, self.chunks[ordinal - 1][root])
        columns = COLUMNS[root]
        key = columns.index(
            "source_row_ordinal"
            if root == "member"
            else "source_record_ref"
            if root == "source_record"
            else "evidence_ref"
        )
        owner = columns.index("admission_ref" if root == "member" else "nppes_admission_ref")
        keys = {record[key] for record in expected_rows}
        actual_rows = [record for record in self.rows[root] if record[key] in keys and record[owner] == admission_ref]
        self.batches.append((root, expected_rows, parameters))
        return Counter(actual_rows) != Counter(expected_rows)


def _has_lifecycle_drift(rows_by_root, snapshot_at, minimum_start):
    """Use the original projection as the transport oracle while callers may corrupt encoding."""
    snapshot = datetime.strptime(snapshot_at, "%Y-%m-%dT%H:%M:%SZ")
    common_by_ref = {
        member_record[COLUMNS["common"].index("evidence_ref")]: member_record
        for member_record in rows_by_root["common"]
    }
    typed_by_ref = {
        member_record[COLUMNS["typed"].index("evidence_ref")]: member_record for member_record in rows_by_root["typed"]
    }
    starts = []
    for member_record in rows_by_root["member"]:
        member_by_field = dict(zip(MEMBER_COLUMNS, member_record, strict=True))
        dates = []
        for name in ("provider_enumeration_date", "last_update_date", "npi_deactivation_date", "npi_reactivation_date"):
            source_date = member_by_field[name]
            if source_date is not None and type(source_date) is not date:
                return True
            dates.append(None if source_date is None else datetime.combine(source_date, datetime.min.time()))
        try:
            state, start, reason = temporal_reference(snapshot, tuple(dates), member_by_field["entity_type_code"])
        except NppesRegistryReplayError:
            return True
        if start is not None:
            starts.append(datetime.fromisoformat(start))
        if member_by_field["exclusion_reason"] != reason or member_by_field["projection_state"] != (
            "projected_v1" if reason is None else "excluded_v1"
        ):
            return True
        if reason is not None:
            continue
        common_by_field = dict(zip(COLUMNS["common"], common_by_ref.get(member_by_field["evidence_ref"], ())))
        typed_by_field = dict(zip(COLUMNS["typed"], typed_by_ref.get(member_by_field["evidence_ref"], ())))
        if (
            typed_by_field.get("enumeration_state") != state
            or common_by_field.get("observed_at") != datetime.fromisoformat(snapshot_at)
            or common_by_field.get("effective_start_at") != datetime.fromisoformat(start)
            or common_by_field.get("effective_end_at") != datetime.fromisoformat(snapshot_at)
        ):
            return True
    return min(starts, default=datetime.fromisoformat(snapshot_at)) != datetime.fromisoformat(minimum_start)


async def _validate(connection, prepared):
    return await validation.validate_nppes_evidence_family(
        connection,
        "evidence",
        _TABLES,
        prepared,
        prepared_chunks="candidate_prepared",
        chunk_count=len(connection.chunks),
    )


def _mutated(value):
    if value is None:
        return "unexpected"
    if isinstance(value, bool):
        return not value
    if isinstance(value, bytes):
        return bytes([value[0] ^ 1]) + value[1:]
    if isinstance(value, (date, datetime)):
        return value + timedelta(days=1)
    if type(value) is int:
        return value + 1
    return value + "x"


@pytest.mark.asyncio
async def test_valid_complete_family_matches_every_predecessor_column(tmp_path):
    prepared = await _prepared(tmp_path)
    connection = _FamilyConnection(_predecessor_rows(prepared))
    assert await _validate(connection, prepared) is None
    assert len(connection.calls) == 13
    assert {root for root, *_ in connection.batches} == set(_TABLES)
    for sql, _ in connection.calls:
        assert not re.search(r"\b(?:INSERT|UPDATE|DELETE|CREATE|ALTER|DROP|LOCK)\b", sql)
    for sql, parameters in connection.calls:
        if sql.startswith("WITH expected"):
            assert sql.count("EXCEPT ALL") == 2
            assert "jsonb_to_recordset" in sql and "convert_from(" in sql
            assert "IN (SELECT" in sql and "bytea" in sql
            assert parameters == (1, prepared.admission_row.admission_ref)
    assert any("GROUP BY source_release_ref, npi" in sql for sql, _ in connection.calls)
    assert any(sql.count("LEFT JOIN") == 8 for sql, _ in connection.calls)


@pytest.mark.asyncio
async def test_retained_chunk_comparison_never_reopens_or_reencodes_members(tmp_path, monkeypatch):
    prepared = await _prepared(tmp_path)
    connection = _FamilyConnection(_predecessor_rows(prepared))

    def forbidden(*args, **kwargs):
        pytest.fail("validation reopened or reencoded an already prepared member")

    monkeypatch.setattr(NppesPrimaryCsvRows, "__enter__", forbidden)
    monkeypatch.setattr(NppesRegistryMemberEncoder, "encode", forbidden)
    for method in ("source_record", "member", "projected"):
        monkeypatch.setattr(NppesRegistryDatabaseRowEncoder, method, forbidden)
    await _validate(connection, prepared)


@pytest.mark.asyncio
@pytest.mark.parametrize("root,column", [(root, column) for root, columns in COLUMNS.items() for column in columns])
async def test_any_persisted_column_drift_is_rejected(tmp_path, root, column):
    prepared = await _prepared(tmp_path)
    rows = _predecessor_rows(prepared)
    connection = _FamilyConnection(rows)
    changed_values = list(rows[root][0])
    index = COLUMNS[root].index(column)
    changed_values[index] = _mutated(changed_values[index])
    rows[root][0] = tuple(changed_values)
    with pytest.raises(NppesPublicEvidenceWriterError):
        await _validate(connection, prepared)


@pytest.mark.asyncio
@pytest.mark.parametrize("root", tuple(_TABLES))
@pytest.mark.parametrize("mutation", ("missing", "extra", "orphan"))
async def test_missing_extra_and_same_count_orphans_fail(tmp_path, root, mutation):
    prepared = await _prepared(tmp_path)
    rows = _predecessor_rows(prepared)
    connection = _FamilyConnection(rows)
    if mutation == "missing":
        rows[root].pop()
    elif mutation == "extra":
        rows[root].append(rows[root][0])
    else:
        changed_values = list(rows[root][0])
        column = "source_record_ref" if root in {"source_record", "member"} else "evidence_ref"
        index = COLUMNS[root].index(column)
        changed_values[index] = "unrelated_reference"
        rows[root][0] = tuple(changed_values)
    with pytest.raises(NppesPublicEvidenceWriterError):
        await _validate(connection, prepared)


@pytest.mark.asyncio
@pytest.mark.parametrize("column", ("source_row_ordinal", "npi", "source_record_ref", "evidence_ref"))
async def test_duplicate_member_natural_keys_fail_without_count_changes(tmp_path, column):
    prepared = await _prepared(tmp_path)
    rows = _predecessor_rows(prepared)
    connection = _FamilyConnection(rows)
    changed_values = list(rows["member"][1])
    index = MEMBER_COLUMNS.index(column)
    changed_values[index] = rows["member"][0][index]
    rows["member"][1] = tuple(changed_values)
    with pytest.raises(NppesPublicEvidenceWriterError):
        await _validate(connection, prepared)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "row",
    (
        equal_day_reactivated_type_1_row(),
        orphan_reactivated_type_2_row(),
        future_deactivation_type_1_row(),
        future_last_update_type_1_row(),
        ("1003022534", "1", "05/23/2005", "07/13/2026", "06/20/2026", "07/13/2026"),
        ("1003022534", "1", "05/23/2005", "07/12/2026", "06/20/2026", "06/15/2026"),
    ),
)
async def test_lifecycle_v2_tolerances_match_existing_projection(tmp_path, row):
    prepared = await _prepared(tmp_path, (row,))
    rows = _predecessor_rows(prepared)
    connection = _FamilyConnection(rows)
    await _validate(connection, prepared)
    changed_values = list(rows["typed"][0])
    index = NPI_ENUMERATION_COLUMNS.index("enumeration_state")
    changed_values[index] = "deactivated" if changed_values[index] == "active" else "active"
    rows["typed"][0] = tuple(changed_values)
    with pytest.raises(NppesPublicEvidenceWriterError):
        await _validate(connection, prepared)


@pytest.mark.asyncio
async def test_exclusion_precedence_and_each_exclusion_count_match(tmp_path):
    prepared = await _prepared(
        tmp_path,
        (
            active_type_1_row(),
            sparse_deactivated_row(),
            ("1003022534", "", "", "", "", ""),
        ),
    )
    assert dict(prepared.manifest.exclusion_counts) == {
        "effective_start_not_disclosed": 1,
        "entity_type_not_disclosed": 1,
    }
    rows = _predecessor_rows(prepared)
    connection = _FamilyConnection(rows)
    await _validate(connection, prepared)
    changed_values = list(rows["member"][-1])
    changed_values[MEMBER_COLUMNS.index("exclusion_reason")] = "entity_type_not_disclosed"
    rows["member"][-1] = tuple(changed_values)
    with pytest.raises(NppesPublicEvidenceWriterError):
        await _validate(connection, prepared)


@pytest.mark.asyncio
async def test_row_and_binary_parameter_byte_limits_bound_queries(tmp_path, monkeypatch):
    prepared = await _prepared(tmp_path)
    rows_by_root = _predecessor_rows(prepared)
    connection = _FamilyConnection(rows_by_root, row_limit=1)
    await _validate(connection, prepared)
    assert len(connection.batches) == 10
    assert all(len(expected) <= 1 and len(parameters) == 2 for _, expected, parameters in connection.batches)
    assert {parameters for _, _, parameters in connection.batches} == {
        (1, prepared.admission_row.admission_ref),
        (2, prepared.admission_row.admission_ref),
    }
    assert {root for root, expected, _ in connection.batches if not expected} == {"common", "source_link", "typed"}
    assert chunks.COPY_MAX_ROWS == 4096 and chunks.COPY_MAX_BYTES == 32 * 1024 * 1024
    assert all(sum(map(len, payloads.values())) <= chunks.COPY_MAX_BYTES for payloads in connection.chunks)
    maximum = max(sum(map(len, payloads.values())) for payloads in connection.chunks)
    monkeypatch.setattr(chunks, "COPY_MAX_BYTES", maximum)
    await _validate(_FamilyConnection(rows_by_root, row_limit=1), prepared)
    monkeypatch.setattr(chunks, "COPY_MAX_BYTES", maximum - 1)
    with pytest.raises(NppesPublicEvidenceWriterError):
        _FamilyConnection(rows_by_root, row_limit=1)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", (RuntimeError("synthetic database failure"), asyncio.CancelledError()))
async def test_database_errors_and_cancellation_propagate(tmp_path, failure):
    prepared = await _prepared(tmp_path)

    class FailingConnection:
        async def fetchval(self, *_args):
            raise failure

    with pytest.raises(type(failure)) as caught:
        await validation.validate_nppes_evidence_family(
            FailingConnection(),
            "evidence",
            _TABLES,
            prepared,
            prepared_chunks="candidate_prepared",
            chunk_count=1,
        )
    assert caught.value is failure


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "schema,tables",
    (
        ("bad-schema", _TABLES),
        ("evidence", {}),
        ("evidence", {**_TABLES, "member": 'candidate_member"; SELECT 1'}),
        ("evidence", {**_TABLES, "member": _TABLES["common"]}),
        ("evidence", {**_TABLES, "member": []}),
        ("evidence", _TABLES),
    ),
)
async def test_relation_identifiers_and_complete_family_are_required(schema, tables):
    with pytest.raises(NppesPublicEvidenceWriterError):
        await validation.validate_nppes_evidence_family(
            None,
            schema,
            tables,
            None,
            prepared_chunks="candidate_prepared",
            chunk_count=1,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("component", ("header", "admission", "manifest", "retained"))
async def test_prepared_proof_drift_is_rejected_before_database_reads(tmp_path, component):
    prepared = await _prepared(tmp_path)
    connection = _FamilyConnection(_predecessor_rows(prepared))
    if component == "header":
        prepared = replace(prepared, header=("wrong",))
    elif component == "admission":
        prepared = replace(prepared, admission_row=prepared.admission_row._replace(source_record_count=99))
    elif component == "manifest":
        prepared = replace(prepared, manifest=replace(prepared.manifest, evidence_root_sha256="00" * 32))
    else:
        path = prepared.archive.retained.path
        path.write_bytes(path.read_bytes() + b"changed")
    with pytest.raises(NppesPublicEvidenceArchiveError):
        await _validate(connection, prepared)
    assert not connection.calls


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "source_row,state,start",
    (
        (("1003000100", "1", "07/13/2026", "07/13/2026", "", ""), "active", "2026-06-15T00:00:00Z"),
        (("1003000100", "1", "05/23/2005", "07/12/2026", "05/22/2005", ""), "active", "2005-05-23T00:00:00Z"),
        (("1003000100", "1", "05/23/2005", "07/12/2026", "", "05/22/2005"), "active", "2005-05-23T00:00:00Z"),
        (active_type_1_row(), "deactivated", "2005-05-23T00:00:00Z"),
        (active_type_1_row(), "active", "2005-05-24T00:00:00Z"),
    ),
)
async def test_independent_lifecycle_refuses_agreeing_invalid_rows_and_chunks(
    tmp_path, monkeypatch, source_row, state, start
):
    monkeypatch.setattr(row_projection, "_temporal_projection", lambda *_args: (state, start, None))
    prepared = await _prepared(tmp_path, (source_row,))
    connection = _FamilyConnection(_predecessor_rows(prepared))
    assert all(
        Counter(connection.rows[root]) == Counter(_expected_chunk_rows(root, connection.chunks[0][root]))
        for root in _TABLES
    )
    with pytest.raises(NppesPublicEvidenceWriterError):
        await _validate(connection, prepared)
    sql, parameters = connection.calls[-1]
    assert sql.lstrip().startswith("WITH eligible_dates") and not connection.batches
    assert parameters == (prepared.admission_row.snapshot_at, prepared.admission_row.minimum_effective_start_at)
    assert "AT TIME ZONE 'UTC'" in sql and "member.provider_enumeration_date > member.deactivation" in sql
    assert "typed.enumeration_state IS DISTINCT FROM" in sql and "common.effective_start_at IS DISTINCT FROM" in sql
