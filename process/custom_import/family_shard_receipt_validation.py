# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Compare every completed native shard with its original immutable commitment.

The caller separately proves the receipt producers and closed native binding.
This read-only leaf supplies neither source semantics nor structural acceptance.
"""

import hashlib
from contextlib import closing
from datetime import datetime

from sqlalchemy import select

from process.custom_import.build_source import _drain_cleanup, _source_cleanup_failure
from process.custom_import.family_block_codec import FamilyBlockResourceLimit
from process.custom_import.family_block_store import _checked_receipt, _size
from process.custom_import.family_block_validate import FamilyBlockValidationError, _add, require_readonly_snapshot
from process.custom_import.family_shard_base_values_sql import census_statement
from process.custom_import.family_shard_facts import _sorted
from process.custom_import.family_shard_output import _input_iterator, _pack_row, _unpack_row
from process.custom_import.family_shard_plan import PlanWork, _cpu, _Scratch
from process.custom_import.family_shard_role_validation import RoleComparison, _fetch_count, _same_snapshot, _tables
from process.custom_import.read_identity import _read_transaction_identity

_FIELDS = ("shard_id", "partition_sha256", "result_sha256", "source_record_count", "role_counts")
_MISSING = object()


def _receipt_cells(layout, receipt, limits):
    checked = _checked_receipt(layout, receipt)
    cells = tuple(checked[name] for name in _FIELDS)
    if 1024 + sum(_size(cell) for cell in cells) > min(limits.max_record_bytes, limits.memory_bytes // 2):
        raise FamilyBlockResourceLimit("shard receipt exceeds the owner comparison reservation")
    return cells


def _stage(work, binding, receipts):
    staged, ordered = work.path(), work.path()
    with staged.open("xb") as target, closing(_input_iterator(receipts)) as originals:
        for receipt in originals:
            work.current()
            cells = _receipt_cells(binding.layout, receipt, work.limits)
            work.frame(target, (cells[0], _pack_row(cells)), staged)
    count, previous = 0, None
    with (
        ordered.open("xb") as target,
        closing(_sorted(work, work.rows(staged), lambda row: row[0])) as rows,
    ):
        for shard_id, packed in rows:
            work.current()
            if previous is not None and shard_id <= previous:
                raise FamilyBlockValidationError("original shard receipts repeat their native identity")
            work.frame(target, packed, ordered)
            count += 1
            previous = shard_id
    work.remove(staged)
    return ordered, count


async def _compare(session, binding, staged, work, prepare):
    transactions = await _read_transaction_identity(session)
    if transactions is None:
        raise FamilyBlockValidationError("receipt comparison requires a live transaction")

    async def ready():
        """Refresh SQL bounds while retaining the caller's exact read transaction."""
        work.current()
        await _same_snapshot(session, transactions)
        if prepare is not None:
            await prepare(session)
        await _same_snapshot(session, transactions)
        work.current()

    await ready()
    await require_readonly_snapshot(session)
    path, count = staged
    table = binding.tables_by_role["shard_receipt"]
    await ready()
    census = (await session.execute(census_statement(select(table)))).one()
    fetch_rows = _fetch_count(census, work.limits, count)
    digest = hashlib.sha256(b"custom-import/physical-role-comparison/v1\0")
    _add(digest, "binding", (binding.family_id, binding.layout.digest, tuple(sorted(binding.oids_by_role.items()))))
    await ready()
    cursor = await session.stream(select(table).order_by(table.c.shard_id).execution_options(yield_per=fetch_rows))
    primary, seen = None, 0
    try:
        with closing(work.rows(path)) as expected:
            async for native_row in cursor:
                work.current()
                if seen % fetch_rows == 0:
                    await ready()
                actual_cells = tuple(native_row)
                if (
                    len(actual_cells) != len(_FIELDS) + 1
                    or not isinstance(actual_cells[-1], datetime)
                    or actual_cells[-1].utcoffset() is None
                ):
                    raise FamilyBlockValidationError("native shard completion timestamp differs")
                wanted = next(expected, _MISSING)
                if wanted is _MISSING or _receipt_cells(
                    binding.layout, dict(zip(_FIELDS, actual_cells[:-1], strict=True)), work.limits
                ) != _unpack_row(wanted):
                    raise FamilyBlockValidationError("native shard differs from its original commitment")
                _add(digest, table.name, actual_cells)
                seen += 1
            if next(expected, _MISSING) is not _MISSING or seen != count:
                raise FamilyBlockValidationError("native shard receipts omit original commitments")
    except BaseException as error:
        primary = error
    primary = await _drain_cleanup(cursor.close(), primary)
    if primary is not None:
        raise primary
    await ready()
    await require_readonly_snapshot(session)
    _add(digest, "role_count", ("shard_receipt", seen))
    return RoleComparison((("shard_receipt", seen),), digest.digest())


async def compare_candidate_receipts(session, binding, receipts, *, work, prepare=None):
    """Require complete two-way receipt equality in the caller's pinned snapshot.

    Native completion timestamps must exist and are included in the observed
    digest. Partition/result hashes and every role/source count compare exactly.
    """
    if type(work) is not PlanWork or (prepare is not None and not callable(prepare)):
        raise ValueError("receipt comparison requires explicit work and SQL budgets")
    _tables(binding)
    scratch, primary, result = _Scratch(work), None, None
    try:
        staged = await _cpu(scratch, _stage, scratch, binding, receipts)
        result = await _compare(session, binding, staged, scratch, prepare)
    except BaseException as error:
        primary = error
    try:
        scratch.close()
    except BaseException as error:
        primary = _source_cleanup_failure(primary, error)
    if primary is not None:
        raise primary
    return result
