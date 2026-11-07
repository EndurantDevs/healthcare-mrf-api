# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Compare complete original COPY output with every non-receipt candidate role.

The caller supplies the closed native binding and independent original output.
This comparison grants no producer, closure, source-semantic or seal authority.
The native shard receipt census remains the store's separate responsibility.
"""

import asyncio
import hashlib
from collections.abc import Mapping, Sequence
from contextlib import ExitStack, closing
from dataclasses import dataclass

from sqlalchemy import Integer, LargeBinary, select

from process.custom_import.build_source import _close_iterator, _drain_cleanup, _source_cleanup_failure
from process.custom_import.family_block_binding import FamilyBlockBinding
from process.custom_import.family_block_codec import FamilyBlockResourceLimit
from process.custom_import.family_block_layout import ROLE_SPECS
from process.custom_import.family_block_store import _size
from process.custom_import.family_block_validate import (
    FamilyBlockValidationError,
    _add,
    compare_role_cursor,
    require_readonly_snapshot,
)
from process.custom_import.family_shard_base_values_sql import census_statement
from process.custom_import.family_shard_facts import _sorted
from process.custom_import.family_shard_output import _pack_row, _unpack_row
from process.custom_import.family_shard_plan import PlanWork, _cpu, _Scratch
from process.custom_import.read_identity import _is_same_read_transaction, _read_transaction_identity
from process.custom_import.runner_types import CancellationRequested

_ROLES = tuple(role for role in ROLE_SPECS if role != "shard_receipt")


@dataclass(frozen=True)
class RoleComparison:
    counts: tuple
    sha256: bytes

    @property
    def counts_by_role(self):
        """Return observed physical counts, not BuildVerification counts."""
        return dict(self.counts)


def _tables(binding):
    if type(binding) is not FamilyBlockBinding or set(binding.tables_by_role) != set(ROLE_SPECS):
        raise FamilyBlockValidationError("role comparison requires the complete closed layout")
    tables_by_role = {role: binding.tables_by_role[role] for role in _ROLES}
    for table in tables_by_role.values():
        if not table.primary_key.columns or any(
            not isinstance(column.type, Integer | LargeBinary) for column in table.primary_key.columns
        ):
            raise FamilyBlockValidationError("role comparison native primary key differs")
    return tables_by_role


def _copy_record(table, row, limits):
    if not isinstance(row, tuple | list) or len(row) != len(table.c):
        raise FamilyBlockValidationError("expected COPY columns differ")
    key_by_name = {}
    for column, cell in zip(table.c, row, strict=True):
        if cell is None and not column.nullable:
            raise FamilyBlockValidationError("expected COPY required column is null")
        if cell is not None and isinstance(column.type, Integer) and type(cell) is not int:
            raise FamilyBlockValidationError("expected COPY integer column differs")
        if column.primary_key:
            if isinstance(column.type, LargeBinary):
                if not isinstance(cell, bytes | bytearray | memoryview):
                    raise FamilyBlockValidationError("expected COPY binary primary key differs")
                cell = bytes(cell)
            key_by_name[column.name] = cell
    if 1024 + sum(_size(cell) for cell in row) > min(limits.max_record_bytes, limits.memory_bytes // 2):
        raise FamilyBlockResourceLimit("expected COPY row exceeds the owner comparison reservation")
    return tuple(key_by_name[name] for name in table.primary_key.columns.keys()), _pack_row(tuple(row))


def _write_copy_batch(work, tables_by_role, batch, target, path):
    if not isinstance(batch, Mapping) or not batch or set(batch) - set(tables_by_role):
        raise FamilyBlockValidationError("expected COPY role differs")
    for role, rows in batch.items():
        if not isinstance(rows, Sequence) or isinstance(rows, str | bytes | bytearray):
            raise FamilyBlockValidationError("expected COPY batch is not bounded")
        for row in rows:
            work.current()
            key, payload = _copy_record(tables_by_role[role], row, work.limits)
            work.frame(target, (role, key, payload), path)


def _stage_batches(work, tables_by_role, batches):
    path = work.path()
    source, primary = iter(batches), None
    try:
        with path.open("xb") as target:
            for batch in source:
                work.current()
                _write_copy_batch(work, tables_by_role, batch, target, path)
    except BaseException as error:
        primary = error
    close = getattr(source, "close", None)
    if close is not None:
        primary = _close_iterator(source, primary)
    if primary is not None:
        raise primary
    return path


def _sort_expected(work, tables_by_role, batches):
    staged = _stage_batches(work, tables_by_role, batches)
    paths_by_role = {role: work.path() for role in tables_by_role}
    counts_by_role = dict.fromkeys(tables_by_role, 0)
    ordered = _sorted(work, work.rows(staged), lambda entry: (ROLE_SPECS[entry[0]][0], entry[1]))
    previous = None
    with closing(ordered), ExitStack() as opened:
        files_by_role = {role: opened.enter_context(path.open("xb")) for role, path in paths_by_role.items()}
        for role, key, payload in ordered:
            if previous == (role, key):
                raise FamilyBlockValidationError("expected COPY repeats a native primary key")
            work.frame(files_by_role[role], payload, paths_by_role[role])
            counts_by_role[role] += 1
            previous = role, key
    work.remove(staged)
    return paths_by_role, counts_by_role


def _expected_rows(work, path):
    work.current()
    with closing(work.rows(path)) as payloads:
        for payload in payloads:
            work.current()
            yield _unpack_row(payload)
    work.current()


def _fetch_count(census, limits, expected_count):
    count, row_bytes = census
    if (
        any(type(value) is not int or value < 0 for value in (count, row_bytes))
        or (count == 0) != (row_bytes == 0)
        or count != expected_count
    ):
        raise FamilyBlockValidationError("candidate role native count or width differs")
    if count == 0:
        return 1
    # Keep the other half for the decoded expected row and comparison/hash work.
    buffer_bytes = limits.memory_bytes // 2
    if row_bytes > min(limits.max_record_bytes, buffer_bytes):
        raise FamilyBlockResourceLimit("candidate role width exceeds the owner comparison reservation")
    return min(4096, buffer_bytes // row_bytes)


async def _same_snapshot(session, transactions):
    if not _is_same_read_transaction(transactions, await _read_transaction_identity(session)):
        raise FamilyBlockValidationError("role comparison transaction or savepoint changed")


async def _joined_comparison(pending, work):
    try:
        await pending
    except CancellationRequested:
        if not work.stop.is_set():
            raise


async def _compare_stream(session, table, expected, fetch_rows, prepare, digest, work):
    pending = asyncio.create_task(
        compare_role_cursor(
            session,
            table,
            expected,
            order_by=tuple(table.primary_key.columns.keys()),
            fetch_rows=fetch_rows,
            prepare=prepare,
            digest=digest,
        )
    )
    try:
        return await asyncio.shield(pending)
    except asyncio.CancelledError as failure:
        work.stop.set()
        raise await _drain_cleanup(_joined_comparison(pending, work), failure)


async def _compare_all(session, binding, staged, work, prepare):
    transactions = await _read_transaction_identity(session)
    if transactions is None:
        raise FamilyBlockValidationError("role comparison requires an existing live transaction")

    async def ready(current):
        """Refresh caller SQL bounds inside the same pinned transaction."""
        if current is not session:
            raise FamilyBlockValidationError("role comparison session changed")
        work.current()
        await _same_snapshot(session, transactions)
        if prepare is not None:
            await prepare(session)
        await _same_snapshot(session, transactions)
        work.current()

    await ready(session)
    await require_readonly_snapshot(session)
    paths_by_role, counts_by_role = staged
    digest = hashlib.sha256(b"custom-import/physical-role-comparison/v1\0")
    _add(digest, "binding", (binding.family_id, binding.layout.digest, tuple(sorted(binding.oids_by_role.items()))))
    for role, path in paths_by_role.items():
        table = binding.tables_by_role[role]
        await ready(session)
        census = (await session.execute(census_statement(select(table)))).one()
        fetch_rows = _fetch_count(census, work.limits, counts_by_role[role])
        with closing(_expected_rows(work, path)) as expected:
            count = await _compare_stream(session, table, expected, fetch_rows, ready, digest, work)
        if count != counts_by_role[role]:
            raise FamilyBlockValidationError("candidate role cursor EOF count differs")
        _add(digest, "role_count", (role, count))
    await ready(session)
    await require_readonly_snapshot(session)
    await _same_snapshot(session, transactions)
    return RoleComparison(tuple(counts_by_role.items()), digest.digest())


async def compare_candidate_roles(session, binding, expected_batches, *, work, prepare=None):
    """Compare full physical roles without recording validation or publication.

    Expected COPY tuples follow the closed table column order. Caller resource
    limits cover staging and driver buffers; exhaustion is an operational refusal.
    Receipt timestamps/census and all native binding/authority proofs stay outside
    this leaf. The caller retains its pinned read-only transaction throughout.
    """
    if type(work) is not PlanWork or (prepare is not None and not callable(prepare)):
        raise ValueError("role comparison requires explicit work and SQL budgets")
    tables_by_role = _tables(binding)
    scratch, primary, result = _Scratch(work), None, None
    try:
        staged = await _cpu(scratch, _sort_expected, scratch, tables_by_role, expected_batches)
        result = await _compare_all(session, binding, staged, scratch, prepare)
    except BaseException as error:
        primary = error
    try:
        scratch.close()
    except BaseException as error:
        primary = _source_cleanup_failure(primary, error)
    if primary is not None:
        raise primary
    return result
