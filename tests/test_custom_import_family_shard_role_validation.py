# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Real framed COPY spools; SQL transport and transaction identities are fakes."""

import asyncio
import os
from collections import Counter
from contextlib import contextmanager
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest
from sqlalchemy import (
    TIMESTAMP,
    BigInteger,
    Boolean,
    Column,
    Date,
    Integer,
    LargeBinary,
    MetaData,
    Numeric,
    Table,
    select,
)
from sqlalchemy.dialects.postgresql import dialect
from sqlalchemy.sql.util import find_tables

from process.custom_import import family_shard_role_validation as validation
from process.custom_import.family_block_binding import FamilyBlockBinding
from process.custom_import.family_block_codec import FamilyBlockResourceLimit
from process.custom_import.family_block_validate import FamilyBlockValidationError
from process.custom_import.family_shard_facts import SourceFactBudgetExceeded, SourceFactLimits, _Work
from process.custom_import.family_shard_output import FamilyShardOutput, _pack_row
from process.custom_import.family_shard_plan import PlanWork
from tests.test_custom_import_family_block_read_sql import context

_LIMITS = SourceFactLimits(16 * 1024**2, 1024**2, 128 * 1024)
_STAMP = datetime(2030, 1, 2, 3, 4, tzinfo=timezone.utc)


def _binding():
    original = context().family_block_binding
    return replace(original, oids_by_role={role: 1000 + index for index, role in enumerate(original.tables_by_role)})


def _cell(column, index):
    if isinstance(column.type, Boolean):
        return False
    if isinstance(column.type, Integer):
        return index
    if isinstance(column.type, LargeBinary):
        return bytes([index % 256]) * 32
    if isinstance(column.type, Numeric):
        return Decimal("1.200")
    if isinstance(column.type, TIMESTAMP):
        return _STAMP
    if isinstance(column.type, Date):
        return date(2030, 1, 2)
    return "synthetic λ e\u0301 " + str(index)


def _rows(binding, count=3, *, only=None):
    return {
        role: [tuple(_cell(column, index) for column in table.c) for index in reversed(range(1, count + 1))]
        if only is None or role == only
        else []
        for role, table in binding.tables_by_role.items()
        if role != "shard_receipt"
    }


@contextmanager
def _original_output(tmp_path, rows_by_role):
    work = _Work(tmp_path / "original", _LIMITS, lambda _path: None)
    counts_by_role = {role: len(rows) for role, rows in rows_by_role.items()}
    path = work.write((role, _pack_row(row)) for role, rows in rows_by_role.items() for row in rows)
    output = FamilyShardOutput(work, path, {"role_counts": counts_by_role})
    try:
        yield output
    finally:
        output.close()


class _Expected:
    def __init__(self, batches, *, failure=None):
        self.batches, self.failure = batches, failure
        self.complete = self.closed = False

    def __iter__(self):
        try:
            yield from self.batches
            if self.failure is not None:
                raise self.failure
            self.complete = True
        finally:
            self.closed = True


class _NativeCursor:
    def __init__(self, session, role, rows):
        self.session, self.role, self.rows = session, role, iter(rows)
        self.closed = self.paused = False

    def __aiter__(self):
        return self

    async def __anext__(self):
        pause = self.session.read_pause
        if pause is not None and not self.paused:
            self.paused = True
            pause[0].set()
            await pause[1].wait()
        try:
            return next(self.rows)
        except StopIteration:
            if self.session.late_error is not None:
                raise self.session.late_error
            raise StopAsyncIteration from None

    async def close(self):
        if self.session.close_pause is not None:
            self.session.close_pause[0].set()
            await self.session.close_pause[1].wait()
        self.closed = True
        if self.session.close_error is not None:
            raise self.session.close_error


class _NativeTransport:
    """No PostgreSQL is used; native authority/binding acceptance is not asserted."""

    def __init__(self, binding, rows_by_role, expected):
        self.binding, self.rows_by_role, self.expected = binding, rows_by_role, expected
        self.role_by_name = {table.name: role for role, table in binding.tables_by_role.items()}
        self.root = SimpleNamespace(is_active=True)
        self.connection_root = SimpleNamespace(is_active=True)
        self.nested = self.connection_nested = None
        self.observations, self.cursors = [], []
        self.census_by_role = {}
        self.isolation = ("repeatable read", "on")
        self.read_pause = self.close_pause = self.close_error = self.late_error = None

    def get_transaction(self):
        return self.root

    def get_nested_transaction(self):
        return self.nested

    async def connection(self):
        return SimpleNamespace(
            get_transaction=lambda: self.connection_root, get_nested_transaction=lambda: self.connection_nested
        )

    def _role(self, statement):
        tables = set(find_tables(statement, check_columns=True))
        roles = {
            self.role_by_name[table.name] for table in tables if table is not None and table.name in self.role_by_name
        }
        assert len(roles) == 1
        role = roles.pop()
        assert role != "shard_receipt"
        return role

    async def execute(self, statement):
        assert self.expected.complete and self.expected.closed
        sql = str(statement.compile(dialect=dialect()))
        if "transaction_isolation" in sql:
            self.observations.append(("isolation",))
            return SimpleNamespace(one=lambda: self.isolation)
        role = self._role(statement)
        self.observations.append(("census", role, sql))
        assert "max(" in sql and "count(" in sql and "ORDER BY" not in sql
        rows = self.rows_by_role[role]
        width = max(
            (1024 + sum(128 + (8 * len(cell) if isinstance(cell, str | bytes) else 0) for cell in row) for row in rows),
            default=0,
        )
        census = self.census_by_role.get(role, (len(rows), width))
        return SimpleNamespace(one=lambda: census)

    async def stream(self, statement):
        role = self._role(statement)
        table = self.binding.tables_by_role[role]
        names = tuple(table.c.keys())
        indices = tuple(names.index(name) for name in table.primary_key.columns.keys())
        order_names = tuple(column.name for column in statement._order_by_clauses)
        assert order_names == tuple(table.primary_key.columns.keys())
        fetch_rows = statement.get_execution_options()["yield_per"]
        assert type(fetch_rows) is int and 1 <= fetch_rows <= 4096
        self.observations.append(("cursor", role, fetch_rows))
        cursor = _NativeCursor(
            self, role, sorted(self.rows_by_role[role], key=lambda row: tuple(row[i] for i in indices))
        )
        self.cursors.append(cursor)
        return cursor


async def _compare(tmp_path, binding, expected, session, *, limits=_LIMITS, check=None, prepare=None):
    registered_paths = []
    result = await validation.compare_candidate_roles(
        session,
        binding,
        expected,
        work=PlanWork(tmp_path / "comparison", limits, registered_paths.append, check),
        prepare=prepare,
    )
    assert registered_paths == [tmp_path / "comparison"]
    assert not (tmp_path / "comparison").exists()
    return result


@pytest.mark.parametrize("count", [0, 3])
async def test_every_fixed_role_and_both_eof_are_compared(tmp_path, count):
    binding = _binding()
    rows_by_role = _rows(binding, count)
    with _original_output(tmp_path, rows_by_role) as original:
        expected = _Expected(original.batches(maximum_rows=2, maximum_bytes=1024**2))
        session = _NativeTransport(binding, rows_by_role, expected)
        result = await _compare(tmp_path, binding, expected, session)
        assert result.counts_by_role == {role: count for role in rows_by_role}
        assert len(result.sha256) == 32 and expected.complete and expected.closed
        assert Counter(event[0] for event in session.observations) == dict(isolation=2, census=13, cursor=13)
        assert all(cursor.closed for cursor in session.cursors)
        assert Path(original.path).exists()


async def test_all_native_scalar_types_and_lexical_bytes(tmp_path):
    binding, native_by_role = _binding(), {}
    rows_by_role = _rows(binding)
    for role, rows in rows_by_role.items():
        native_by_role[role] = [
            tuple(
                memoryview(cell)
                if isinstance(cell, bytes)
                else Decimal("1.200000000000")
                if isinstance(cell, Decimal)
                else cell.astimezone(timezone(timedelta(hours=5)))
                if isinstance(cell, datetime)
                else cell
                for cell in row
            )
            for row in rows
        ]
    with _original_output(tmp_path, rows_by_role) as original:
        expected = _Expected(original.batches(maximum_rows=1, maximum_bytes=1024**2))
        session = _NativeTransport(binding, native_by_role, expected)
        result = await _compare(tmp_path, binding, expected, session)
        assert all(count == 3 for count in result.counts_by_role.values())


async def test_result_is_independent_of_batch_and_input_order(tmp_path):
    binding = _binding()
    rows_by_role = _rows(binding)
    results = []
    for reverse in (False, True):
        batches = [{role: tuple(reversed(rows)) if reverse else rows} for role, rows in rows_by_role.items()]
        expected = _Expected(reversed(batches) if reverse else batches)
        session = _NativeTransport(binding, rows_by_role, expected)
        results.append(await _compare(tmp_path, binding, expected, session))
    assert results[0] == results[1]


@pytest.mark.parametrize(
    "damage", ["unknown", "receipt", "columns", "mapping_row", "false_counter", "duplicate", "late"]
)
async def test_bad_expected_output_never_reaches_sql(tmp_path, damage):
    binding = _binding()
    rows_by_role = _rows(binding, 1, only="root_record")
    row = rows_by_role["root_record"][0]
    batches, failure = [{"root_record": [row]}], None
    match damage:
        case "unknown":
            batches = [{"other": [row]}]
        case "receipt":
            batches = [{"shard_receipt": [row]}]
        case "columns":
            batches = [{"root_record": [row[:-1]]}]
        case "mapping_row":
            batches = [{"root_record": [{"id": 1}]}]
        case "false_counter":
            batches = [{"root_record": [(False, *row[1:])]}]
        case "duplicate":
            batches.append({"root_record": [row]})
        case "late":
            failure = RuntimeError("original producer late EOF failure")
    expected = _Expected(batches, failure=failure)
    session = _NativeTransport(binding, rows_by_role, expected)
    with pytest.raises((FamilyBlockValidationError, RuntimeError)) as caught:
        await _compare(tmp_path, binding, expected, session)
    if failure is not None:
        assert caught.value is failure
    assert expected.closed and session.observations == []
    assert not (tmp_path / "comparison").exists()


@pytest.mark.parametrize("column", ["raw_states", "raw_type_overrides", "raw_field_1", "raw_field_7", "raw_field_9"])
async def test_physical_presence_scalar_and_timestamp_changes_refuse(tmp_path, column):
    binding = _binding()
    rows_by_role = _rows(binding, 1, only="family")
    names = tuple(binding.tables_by_role["family"].c.keys())
    index = names.index(column)
    actual_cells = list(rows_by_role["family"][0])
    actual_cells[index] = None if actual_cells[index] is not None else b"changed"
    native_by_role = rows_by_role | {"family": [tuple(actual_cells)]}
    expected = _Expected([{"family": rows_by_role["family"]}])
    session = _NativeTransport(binding, native_by_role, expected)
    with pytest.raises(FamilyBlockValidationError, match="differs from complete owner spool"):
        await _compare(tmp_path, binding, expected, session)
    assert all(cursor.closed for cursor in session.cursors)
    assert not (tmp_path / "comparison").exists()


@pytest.mark.parametrize("change", [-1, 1])
async def test_missing_or_extra_native_rows_refuse(tmp_path, change):
    binding = _binding()
    rows_by_role = _rows(binding, 2, only="root_record")
    altered = rows_by_role["root_record"][:1] if change < 0 else rows_by_role["root_record"] * 2
    expected = _Expected([{"root_record": rows_by_role["root_record"]}])
    session = _NativeTransport(binding, rows_by_role | {"root_record": altered}, expected)
    with pytest.raises(FamilyBlockValidationError, match="native count"):
        await _compare(tmp_path, binding, expected, session)
    assert not session.cursors and not (tmp_path / "comparison").exists()


@pytest.mark.parametrize("change", [-1, 1])
async def test_native_cursor_eof_is_independent_of_census(tmp_path, change):
    binding = _binding()
    rows_by_role = _rows(binding, 2, only="root_record")
    altered = rows_by_role["root_record"][:1] if change < 0 else rows_by_role["root_record"] * 2
    expected = _Expected([{"root_record": rows_by_role["root_record"]}])
    session = _NativeTransport(binding, rows_by_role | {"root_record": altered}, expected)
    session.census_by_role["root_record"] = (2, 4096)
    with pytest.raises(FamilyBlockValidationError, match="owner spool"):
        await _compare(tmp_path, binding, expected, session)
    assert session.cursors[0].closed and not (tmp_path / "comparison").exists()


async def test_complete_original_spool_hash_is_mandatory(tmp_path):
    binding = _binding()
    rows_by_role = _rows(binding, 3, only="root_record")
    with _original_output(tmp_path, rows_by_role) as original:
        payload = bytearray(original.path.read_bytes())
        payload[payload.index(b"synthetic")] ^= 1
        original.path.write_bytes(payload)
        expected = _Expected(original.batches(maximum_rows=1, maximum_bytes=1024**2))
        session = _NativeTransport(binding, rows_by_role, expected)
        with pytest.raises(ValueError, match="completed family output bytes differ"):
            await _compare(tmp_path, binding, expected, session)
        assert not expected.complete and expected.closed and session.observations == []
        assert not (tmp_path / "comparison").exists()


async def test_exact_text_is_not_unicode_normalized(tmp_path):
    binding = _binding()
    rows_by_role = _rows(binding, 1, only="root_record")
    changed_rows = [
        tuple(cell.replace("e\u0301", "é") if isinstance(cell, str) else cell for cell in row)
        for row in rows_by_role["root_record"]
    ]
    expected = _Expected([{"root_record": rows_by_role["root_record"]}])
    session = _NativeTransport(binding, rows_by_role | {"root_record": changed_rows}, expected)
    with pytest.raises(FamilyBlockValidationError, match="differs from complete owner spool"):
        await _compare(tmp_path, binding, expected, session)
    assert session.cursors[0].closed


@pytest.mark.parametrize("census", [(False, 0), (0, False), (-1, 0), (1, 0), (0, 100), (1, -1), (1, Decimal(100))])
async def test_native_census_requires_exact_nonnegative_integers(tmp_path, census):
    binding = _binding()
    rows_by_role = _rows(binding, 0)
    expected = _Expected([])
    session = _NativeTransport(binding, rows_by_role, expected)
    session.census_by_role["root_record"] = census
    with pytest.raises(FamilyBlockValidationError, match="count or width"):
        await _compare(tmp_path, binding, expected, session)
    assert not session.cursors


async def test_observed_huge_cell_refuses_before_native_fetch(tmp_path):
    binding = _binding()
    rows_by_role = _rows(binding, 1, only="root_record")
    expected = _Expected([{"root_record": rows_by_role["root_record"]}])
    session = _NativeTransport(binding, rows_by_role, expected)
    session.census_by_role["root_record"] = (1, 2**40)
    with pytest.raises(FamilyBlockResourceLimit, match="width"):
        await _compare(tmp_path, binding, expected, session)
    assert not session.cursors
    sql = session.observations[-1][2]
    assert "CAST(coalesce(octet_length(" in sql and "AS BIGINT)" in sql


def test_fixed_width_census_keeps_filtered_native_from():
    table = Table("fixed_width", MetaData(), Column("id", BigInteger, primary_key=True))
    census = validation.census_statement(select(table).where(table.c.id > 7).order_by(table.c.id))
    sql = str(census.compile(dialect=dialect(), compile_kwargs={"literal_binds": True}))
    assert "FROM (SELECT fixed_width.id" in sql
    assert "FROM fixed_width" in sql and "WHERE fixed_width.id > 7" in sql
    assert "ORDER BY" not in sql


async def test_driver_fetch_uses_observed_width(tmp_path):
    binding = _binding()
    rows_by_role = _rows(binding, 100, only="root_record")
    expected = _Expected([{"root_record": rows_by_role["root_record"]}])
    session = _NativeTransport(binding, rows_by_role, expected)
    session.census_by_role["root_record"] = (100, 4096)
    await _compare(tmp_path, binding, expected, session)
    assert ("cursor", "root_record", _LIMITS.memory_bytes // 2 // 4096) in session.observations


async def test_order_uses_complete_native_primary_key(tmp_path):
    table = Table("ordered", MetaData(), Column("second", BigInteger), Column("first", BigInteger))
    from sqlalchemy import PrimaryKeyConstraint

    table.append_constraint(PrimaryKeyConstraint("first", "second"))
    key, _payload = validation._copy_record(table, (9, 3), _LIMITS)
    assert key == (3, 9)
    binding = _binding()
    rows_by_role = _rows(binding, 0)
    winner = binding.tables_by_role["winner"]
    names = tuple(winner.c.keys())
    values = list(tuple(_cell(column, 1) for column in winner.c))
    rows_by_role["winner"] = []
    for digest in (b"z" * 32, b"a" * 32):
        values[names.index("context_key_sha256")] = digest
        rows_by_role["winner"].append(tuple(values))
    expected = _Expected([{"winner": rows_by_role["winner"]}])
    session = _NativeTransport(binding, rows_by_role, expected)
    result = await _compare(tmp_path, binding, expected, session)
    assert result.counts_by_role["winner"] == 2


async def test_external_merge_and_callback_keep_original_eof(tmp_path):
    binding = _binding()
    rows_by_role = _rows(binding, 1024, only="root_record")
    expected = _Expected([{"root_record": rows_by_role["root_record"]}])
    session = _NativeTransport(binding, rows_by_role, expected)
    checks = []

    def check():
        checks.append(expected.complete)

    limits = replace(_LIMITS, memory_bytes=32 * 1024)
    result = await _compare(tmp_path, binding, expected, session, limits=limits, check=check)
    assert result.counts_by_role["root_record"] == 1024
    assert False in checks and True in checks and len(checks) > 4096


async def test_merge_cancellation_cleans_owned_work(tmp_path):
    binding = _binding()
    rows_by_role = _rows(binding, 1024, only="root_record")
    expected = _Expected([{"root_record": rows_by_role["root_record"]}])
    session = _NativeTransport(binding, rows_by_role, expected)
    failure = InterruptedError("synthetic merge cancelled")

    def check():
        if expected.complete and len(list((tmp_path / "comparison").iterdir())) >= 20:
            raise failure

    with pytest.raises(InterruptedError) as caught:
        await _compare(
            tmp_path, binding, expected, session, limits=replace(_LIMITS, memory_bytes=32 * 1024), check=check
        )
    assert caught.value is failure and expected.closed
    assert session.observations == [] and not (tmp_path / "comparison").exists()


@pytest.mark.parametrize("isolation", [("read committed", "on"), ("repeatable read", "off")])
async def test_only_readonly_repeatable_snapshot_is_allowed(tmp_path, isolation):
    binding = _binding()
    expected = _Expected([])
    session = _NativeTransport(binding, _rows(binding, 0), expected)
    session.isolation = isolation
    with pytest.raises(FamilyBlockValidationError, match="read-only repeatable"):
        await _compare(tmp_path, binding, expected, session)
    assert not session.cursors and not (tmp_path / "comparison").exists()


@pytest.mark.parametrize("part", ["root", "connection_root", "nested", "connection_nested"])
async def test_transaction_replacement_refuses_before_cursor(tmp_path, part):
    binding = _binding()
    expected = _Expected([])
    session = _NativeTransport(binding, _rows(binding, 0), expected)

    async def prepare(current):
        assert current is session
        setattr(session, part, SimpleNamespace(is_active=True))

    with pytest.raises(FamilyBlockValidationError, match="transaction or savepoint"):
        await _compare(tmp_path, binding, expected, session, prepare=prepare)
    assert session.observations == []


async def test_late_native_error_closes_cursor_and_spool(tmp_path):
    binding = _binding()
    rows_by_role = _rows(binding, 1, only="root_record")
    expected = _Expected([{"root_record": rows_by_role["root_record"]}])
    session = _NativeTransport(binding, rows_by_role, expected)
    session.late_error = RuntimeError("native cursor late EOF failure")
    with pytest.raises(RuntimeError) as caught:
        await _compare(tmp_path, binding, expected, session)
    assert caught.value is session.late_error
    assert session.cursors[0].closed and not (tmp_path / "comparison").exists()


async def test_native_close_failure_cannot_return_comparison(tmp_path):
    binding = _binding()
    rows_by_role = _rows(binding, 1, only="root_record")
    expected = _Expected([{"root_record": rows_by_role["root_record"]}])
    session = _NativeTransport(binding, rows_by_role, expected)
    session.close_error = RuntimeError("native cursor close failed")
    with pytest.raises(RuntimeError) as caught:
        await _compare(tmp_path, binding, expected, session)
    assert caught.value is session.close_error
    assert session.cursors[0].closed and not (tmp_path / "comparison").exists()


async def test_repeated_cancellation_drains_cursor_before_cleanup(tmp_path):
    binding = _binding()
    rows_by_role = _rows(binding, 1, only="root_record")
    expected = _Expected([{"root_record": rows_by_role["root_record"]}])
    session = _NativeTransport(binding, rows_by_role, expected)
    session.read_pause = (asyncio.Event(), asyncio.Event())
    session.close_pause = (asyncio.Event(), asyncio.Event())
    pending = asyncio.create_task(_compare(tmp_path, binding, expected, session))
    try:
        await asyncio.wait_for(session.read_pause[0].wait(), 5)
        pending.cancel()
        await asyncio.sleep(0)
        session.read_pause[1].set()
        await asyncio.wait_for(session.close_pause[0].wait(), 5)
        pending.cancel()
        await asyncio.sleep(0)
        assert not pending.done() and (tmp_path / "comparison").exists()
        session.close_pause[1].set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(pending, 5)
    finally:
        session.read_pause[1].set()
        session.close_pause[1].set()
        if not pending.done():
            pending.cancel()
        await asyncio.gather(pending, return_exceptions=True)
    assert session.cursors[0].closed and not (tmp_path / "comparison").exists()


async def test_spool_budget_failure_preserves_caller_output(tmp_path):
    binding = _binding()
    rows_by_role = _rows(binding, 3)
    with _original_output(tmp_path, rows_by_role) as original:
        payload = Path(original.path).read_bytes()
        expected = _Expected(original.batches(maximum_rows=1, maximum_bytes=1024**2))
        session = _NativeTransport(binding, rows_by_role, expected)
        # Reserve the root and staging file so exhaustion happens with a live producer.
        filesystem = os.statvfs(tmp_path)
        entry_charge = 2 * max(512, filesystem.f_frsize or filesystem.f_bsize)
        limits = SourceFactLimits(2 * entry_charge + 4096, 2048, 4096)
        with pytest.raises((SourceFactBudgetExceeded, FamilyBlockResourceLimit)):
            await _compare(tmp_path, binding, expected, session, limits=limits)
        assert not expected.complete and expected.closed and Path(original.path).exists()
        assert Path(original.path).read_bytes() == payload and session.observations == []
        assert not session.cursors and not (tmp_path / "comparison").exists()
