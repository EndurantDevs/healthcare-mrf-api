# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Compare complete routing homes with actual original allocation observations.

The caller supplies SOURCE plus accepted retained IDs, including eliminated
SOURCE records. Selected family directories and sequence reservations are not
allocation evidence. This read-only comparison does not accept the candidate.
"""

import hashlib
from contextlib import closing
from dataclasses import dataclass

from sqlalchemy import case, func, select, true

from db.models.custom_import_storage import CustomImportRevisionHome
from process.custom_import.build_source import _drain_cleanup, _source_cleanup_failure
from process.custom_import.family_block_records import _integer
from process.custom_import.family_block_validate import (
    FamilyBlockValidationError,
    _add,
    check_revision_ranges,
    require_readonly_snapshot,
)
from process.custom_import.family_shard_base_values_sql import census_statement, fetch_rows
from process.custom_import.family_shard_facts import _sorted
from process.custom_import.family_shard_output import _input_iterator
from process.custom_import.family_shard_plan import PlanWork, _cpu, _Scratch
from process.custom_import.family_shard_role_validation import _same_snapshot, _tables
from process.custom_import.read_identity import _read_transaction_identity


@dataclass(frozen=True)
class RevisionHomeComparison:
    root_count: int
    child_count: int
    range_count: int
    sha256: bytes


def _allocations(work, observations):
    staged, ordered = work.path(), work.path()
    with staged.open("xb") as target, closing(_input_iterator(observations)) as rows:
        for cells in rows:
            work.current()
            if type(cells) is not tuple or len(cells) != 2 or type(cells[0]) is not int or cells[0] not in (1, 2):
                raise FamilyBlockValidationError("original revision home kind differs")
            _integer(cells[1], "original home revision")
            work.frame(target, cells, staged)
    counts, previous = [0, 0], None
    with ordered.open("xb") as target, closing(_sorted(work, work.rows(staged), lambda cells: cells)) as rows:
        for cells in rows:
            work.current()
            if previous is not None and cells <= previous:
                raise FamilyBlockValidationError("original revision home observation repeats")
            work.frame(target, cells, ordered)
            counts[cells[0] - 1] += 1
            previous = cells
    work.remove(staged)
    return ordered, tuple(counts)


def _range_statement(family_id):
    homes = CustomImportRevisionHome.__table__
    ranges = func.unnest(homes.c.revision_ids).table_valued("span").render_derived().lateral()
    first = func.lower(ranges.c.span)
    last = case((func.upper_inf(ranges.c.span), 2**63 - 1), else_=func.upper(ranges.c.span) - 1)
    return (
        select(homes.c.revision_kind, first.label("first_revision"), last.label("last_revision"))
        .select_from(homes.join(ranges, true()))
        .where(homes.c.family_id == family_id)
        .order_by(homes.c.revision_kind, first)
    )


def _write_ranges(work, target, path, rows, digest):
    for native in rows:
        work.current()
        cells = tuple(native)
        if len(cells) != 3:
            raise FamilyBlockValidationError("native home range columns differ")
        work.frame(target, cells, path)
        _add(digest, "revision_home", cells)
    return len(rows)


def _finish(work, actual, expected, counts):
    with closing(work.rows(actual)) as ranges, closing(work.rows(expected)) as identities:
        count = check_revision_ranges(ranges, identities)
    if count != sum(counts):
        raise FamilyBlockValidationError("native home allocation census differs")


async def _compare(session, binding, staged, work, prepare):
    transactions = await _read_transaction_identity(session)
    if transactions is None:
        raise FamilyBlockValidationError("home comparison requires a live transaction")

    async def ready():
        """Keep each bounded read inside the caller's original snapshot."""
        work.current()
        await _same_snapshot(session, transactions)
        if prepare is not None:
            await prepare(session)
        await _same_snapshot(session, transactions)
        work.current()

    await ready()
    await require_readonly_snapshot(session)
    expected, counts = staged
    statement = _range_statement(binding.family_id)
    await ready()
    census = (await session.execute(census_statement(statement.order_by(None)))).one()
    size = fetch_rows(census, work.limits)
    digest = hashlib.sha256(b"custom-import/physical-role-comparison/v1\0")
    _add(digest, "binding", (binding.family_id, binding.layout.digest, tuple(sorted(binding.oids_by_role.items()))))
    await ready()
    cursor = await session.stream(statement.execution_options(yield_per=size))
    actual, primary, seen = work.path(), None, 0
    try:
        with actual.open("xb") as range_output:
            async for native_ranges in cursor.partitions(size):
                await ready()
                seen += await _cpu(work, _write_ranges, work, range_output, actual, native_ranges, digest)
            await ready()
            if seen != census.row_count:
                raise FamilyBlockValidationError("native home range cursor EOF differs")
    except BaseException as error:
        primary = error
    primary = await _drain_cleanup(cursor.close(), primary)
    if primary is not None:
        raise primary
    await _cpu(work, _finish, work, actual, expected, counts)
    await ready()
    await require_readonly_snapshot(session)
    _add(digest, "home_counts", (counts, seen))
    return RevisionHomeComparison(*counts, seen, digest.digest())


async def compare_candidate_homes(session, binding, observations, *, work, prepare=None):
    """Require exact home coverage, without expanding native ranges into SQL rows.

    Complete originals are staged and checked before reading any homes. Every
    native inclusive range must equal the actual ordered allocation IDs; gaps,
    missing/extra IDs and cross-kind collisions cannot be hidden by totals.
    """
    if type(work) is not PlanWork or (prepare is not None and not callable(prepare)):
        raise ValueError("home comparison requires explicit work and SQL budgets")
    _tables(binding)
    scratch, primary, result = _Scratch(work), None, None
    try:
        staged = await _cpu(scratch, _allocations, scratch, observations)
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
