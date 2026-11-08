# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Exact frozen-source counts reconcile the complete sparse keyset scan."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import BigInteger, Column, MetaData, Table
from sqlalchemy.dialects.postgresql import dialect

from process.custom_import import retirement
from process.custom_import import retirement_inventory as inventory


def _table():
    return Table(
        "kept_rows",
        MetaData(schema="synthetic"),
        Column("bucket", BigInteger, primary_key=True),
        Column("position", BigInteger, primary_key=True),
        Column("root_ref", BigInteger),
        Column("child_ref", BigInteger),
    )


def _page(bucket, position, rows, *, references=None):
    return {
        "bucket": bucket,
        "position": position,
        "inventory_source_rows": rows,
        "inventory_rows": 1,
        "inventory_references": rows if references is None else references,
    }


def _session(source_rows, relevant_rows, pages):
    connection = SimpleNamespace(dialect=dialect(), info={})
    primary = SimpleNamespace(
        all=lambda: [SimpleNamespace(attname=name, attnotnull=True) for name in ("bucket", "position")]
    )
    results = [SimpleNamespace(mappings=lambda row=row: SimpleNamespace(one_or_none=lambda: row)) for row in pages]
    return SimpleNamespace(
        connection=AsyncMock(return_value=connection),
        scalar=AsyncMock(side_effect=[source_rows, relevant_rows]),
        execute=AsyncMock(side_effect=[primary, *results]),
    )


@pytest.mark.parametrize("source_rows,relevant_rows", [(0, 0), (17, 0), (1000, 3), (3, 3)])
async def test_exact_source_and_relevant_counts_reconcile_actual_pages(monkeypatch, source_rows, relevant_rows):
    monkeypatch.setattr(retirement, "REFERENCE_PAGE_ROWS", 2)
    pages = [] if relevant_rows == 0 else [_page(1, 1009, 2, references=3), _page(9, 7, 1)]
    session = _session(source_rows, relevant_rows, [*pages, None])

    rows, references, coverage = await inventory._copy_reference_pages(
        session, 1, _table(), 42, {"root_ref": 1, "child_ref": 2}
    )

    assert coverage == (len(pages), source_rows, relevant_rows)
    assert (rows, references) == ((2, 4) if pages else (0, 0))
    statements = [str(call.args[0]) for call in session.scalar.await_args_list]
    assert statements == [
        "SELECT count(*) FROM synthetic.kept_rows",
        "SELECT count(*) FROM synthetic.kept_rows WHERE root_ref IS NOT NULL OR child_ref IS NOT NULL",
    ]
    queries = session.execute.await_args_list[1:]
    assert len(queries) == len(pages) + 1  # Empty EOF is observed, never counted as a page.
    assert all("WHERE (root_ref IS NOT NULL OR child_ref IS NOT NULL)" in str(call.args[0]) for call in queries)
    if pages:
        assert "AND ROW(bucket,position) > ROW(:key_0,:key_1)" in str(queries[1].args[0])
        assert (queries[1].args[1]["key_0"], queries[1].args[1]["key_1"]) == (1, 1009)
        assert (queries[2].args[1]["key_0"], queries[2].args[1]["key_1"]) == (9, 7)


@pytest.mark.parametrize("source,relevant", [(-1, 0), (1, 2), (1, -1), (True, 0), (1, None)])
async def test_invalid_exact_counts_refuse_before_paging(source, relevant):
    session = _session(source, relevant, [])
    with pytest.raises(retirement.SnapshotRetirementRefused, match="source counts"):
        await inventory._copy_reference_pages(session, 1, _table(), 42, {"root_ref": 1})
    assert session.execute.await_count == 1


@pytest.mark.parametrize("relevant,pages", [(3, [_page(1, 4, 2), None]), (1, [_page(1, 4, 2)])])
async def test_premature_eof_or_excess_page_rows_cannot_certify_counts(monkeypatch, relevant, pages):
    monkeypatch.setattr(retirement, "REFERENCE_PAGE_ROWS", 2)
    session = _session(9, relevant, pages)
    with pytest.raises(retirement.SnapshotRetirementRefused, match="page census"):
        await inventory._copy_reference_pages(session, 1, _table(), 42, {"root_ref": 1})


@pytest.mark.parametrize("rows", [0, 3])
async def test_page_rows_still_obey_the_existing_bound(monkeypatch, rows):
    monkeypatch.setattr(retirement, "REFERENCE_PAGE_ROWS", 2)
    session = _session(9, 4, [_page(1, 4, rows)])
    with pytest.raises(retirement.SnapshotRetirementRefused, match="did not advance"):
        await inventory._copy_reference_pages(session, 1, _table(), 42, {"root_ref": 1})


async def test_repeated_composite_cursor_is_refused(monkeypatch):
    monkeypatch.setattr(retirement, "REFERENCE_PAGE_ROWS", 2)
    session = _session(9, 4, [_page(1, 1009, 2), _page(1, 1009, 2)])
    with pytest.raises(retirement.SnapshotRetirementRefused, match="did not advance"):
        await inventory._copy_reference_pages(session, 1, _table(), 42, {"root_ref": 1})


@pytest.mark.parametrize("during", ["source_count", "relevant_count", "page"])
async def test_cancellation_propagates_without_returning_coverage(during):
    session = _session(9, 1, [None])
    if during == "source_count":
        session.scalar.side_effect = asyncio.CancelledError
    elif during == "relevant_count":
        session.scalar.side_effect = [9, asyncio.CancelledError]
    else:
        session.execute.side_effect = [next(session.execute.side_effect), asyncio.CancelledError]
    with pytest.raises(asyncio.CancelledError):
        await inventory._copy_reference_pages(session, 1, _table(), 42, {"root_ref": 1})


async def test_transaction_ended_with_unknown_commit_outcome_cannot_reuse_inventory(monkeypatch):
    connection = SimpleNamespace(info={})
    old_transaction = SimpleNamespace(is_active=False)
    current_transaction = SimpleNamespace(is_active=True)
    connection.get_transaction = lambda: current_transaction
    connection.get_nested_transaction = lambda: None
    connection.info[inventory._KEY] = inventory._Inventory(
        connection, "synthetic", (1, 2, 3, 4, 5, 6), {}, {}, 0, 0, (old_transaction, None, old_transaction, None)
    )
    session = SimpleNamespace(
        connection=AsyncMock(return_value=connection),
        get_transaction=lambda: current_transaction,
        get_nested_transaction=lambda: None,
        execute=AsyncMock(),
    )
    monkeypatch.setattr(inventory, "_temporary_identity", AsyncMock(return_value=(1, 2, 3, 4, 5, 6)))
    monkeypatch.setattr(retirement, "_owner_schema", AsyncMock(return_value=("synthetic", '"synthetic"')))

    assert await inventory.can_prove_frozen_reference_inventory(session, SimpleNamespace(family_id=1), ()) is False
    session.execute.assert_not_awaited()
