# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""The real summary orchestrator preserves established reads before its atomic swap."""

import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import native_publication_read_access as access
from process import plan_summary as summary


@pytest.fixture
def publication_boundary(monkeypatch):
    statements, events = [], []
    state_by_name = {"active": False}
    native = SimpleNamespace(is_in_transaction=lambda: state_by_name["active"])
    raw_connection = SimpleNamespace(driver_connection=native)
    connection = SimpleNamespace(get_raw_connection=AsyncMock(return_value=raw_connection))

    async def execute(statement):
        assert state_by_name["active"]
        statements.append(str(statement))
        return SimpleNamespace(scalar=lambda: 3)

    session = SimpleNamespace(execute=AsyncMock(side_effect=execute), connection=AsyncMock(return_value=connection))
    preparation = SimpleNamespace(execute=AsyncMock())

    @asynccontextmanager
    async def prepare():
        yield preparation

    @asynccontextmanager
    async def transaction():
        state_by_name["active"] = True
        events.append("begin")
        try:
            yield session
        except BaseException:
            events.append("rollback")
            raise
        else:
            events.append("commit")
        finally:
            state_by_name["active"] = False

    database = SimpleNamespace(session=prepare, transaction=transaction, create_table=AsyncMock())
    monkeypatch.setattr(summary, "db", database)
    monkeypatch.setattr(summary, "ensure_database", AsyncMock())
    monkeypatch.setattr(summary, "_ensure_summary_columns", AsyncMock())
    monkeypatch.setattr(summary, "lock_publication_family", AsyncMock())
    monkeypatch.setattr(summary, "capture_summary_inputs", AsyncMock(return_value={"plan": 100}))
    monkeypatch.setattr(summary, "complete_publication", AsyncMock())
    return SimpleNamespace(
        session=session,
        connection=connection,
        native=native,
        preparation=preparation,
        statements=statements,
        events=events,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("completed", [False, True])
async def test_read_grants_replay_on_same_native_transaction_before_swap(publication_boundary, monkeypatch, completed):
    context = publication_boundary

    async def preserve(connection, **names):
        assert connection is context.native and connection.is_in_transaction()
        assert all(type(name) is str for name in names.values())
        assert len(context.statements) == 5
        assert context.statements[0].startswith("INSERT INTO")
        assert context.statements[1].startswith("SELECT count")
        assert all("plan_search_summary_refresh" in sql for sql in context.statements[2:])
        assert sum(sql.startswith("CREATE INDEX") for sql in context.statements) == 2
        assert context.statements[4].startswith("ANALYZE ")
        assert not any("IF NOT EXISTS" in sql for sql in context.statements[2:])
        assert not any(statement.startswith(("DROP", "ALTER")) for statement in context.statements)
        assert names == dict(
            schema=summary.summary_table.schema or "public",
            live_table="plan_search_summary",
            stage_table="plan_search_summary_refresh",
        )
        assert summary.complete_publication.await_count == int(completed)
        context.events.append("preserve")

    gate = AsyncMock(side_effect=preserve)
    monkeypatch.setattr(summary, "preserve_publication_reads", gate)
    publication = ("synthetic-attempt", object(), False) if completed else None
    assert await summary.rebuild_plan_search_summary(publication=publication) == 3
    assert context.events == ["begin", "preserve", "commit"]
    gate.assert_awaited_once()
    context.session.connection.assert_awaited_once()
    context.connection.get_raw_connection.assert_awaited_once()
    assert sum(statement.startswith("ALTER TABLE") for statement in context.statements) == 3
    assert sum(statement.startswith("DROP TABLE") for statement in context.statements) == 2
    if completed:
        summary.complete_publication.assert_awaited_once_with(
            context.session,
            summary.summary_table.schema or "public",
            publication[0],
            publication[1],
            {"plan": 100},
            False,
            refresh_summary=True,
        )
    else:
        summary.complete_publication.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["grant", "cancel"])
@pytest.mark.parametrize("completed", [False, True])
async def test_grant_failure_or_cancel_rolls_back_before_swap(publication_boundary, monkeypatch, failure, completed):
    context = publication_boundary
    error = asyncio.CancelledError() if failure == "cancel" else RuntimeError("publication grants changed")
    gate = AsyncMock(side_effect=error)
    monkeypatch.setattr(summary, "preserve_publication_reads", gate)
    publication = ("synthetic-attempt", object(), False) if completed else None
    with pytest.raises(type(error)):
        await summary.rebuild_plan_search_summary(publication=publication)
    gate.assert_awaited_once()
    assert context.events == ["begin", "rollback"]
    assert not any(statement.startswith(("DROP", "ALTER")) for statement in context.statements)
    context.preparation.execute.assert_awaited_once()
    assert summary.complete_publication.await_count == int(completed)


@pytest.mark.asyncio
async def test_missing_first_live_heap_uses_existing_native_preserver(publication_boundary, monkeypatch):
    context = publication_boundary
    schema = summary.summary_table.schema or "public"
    relations = [
        dict(
            name=name,
            oid=oid,
            relowner=19,
            owned=True,
            relkind="r",
            relpersistence="p",
            relrowsecurity=False,
            relforcerowsecurity=False,
        )
        for name, oid in (("plan_search_summary", None), ("plan_search_summary_refresh", 42))
    ]
    context.native.fetch = AsyncMock(side_effect=[relations, relations])
    context.native.execute = AsyncMock()
    monkeypatch.setattr(summary, "preserve_publication_reads", access.preserve_publication_reads)
    assert await summary.rebuild_plan_search_summary() == 3
    context.native.execute.assert_awaited_once_with(
        f'LOCK TABLE "{schema}"."plan_search_summary_refresh" IN ACCESS EXCLUSIVE MODE'
    )
    assert context.native.fetch.await_count == 2 and context.events == ["begin", "commit"]


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["first_index", "second_index", "analyze", "cancel"])
async def test_stage_index_failure_prevents_any_live_lock_or_swap(publication_boundary, monkeypatch, phase):
    context = publication_boundary
    original = context.session.execute.side_effect
    index_counts = [0]

    async def execute(statement):
        sql = str(statement)
        if sql.startswith("CREATE INDEX"):
            index_counts[0] += 1
        has_failed = (
            (phase == "first_index" and index_counts[0] == 1)
            or (phase in {"second_index", "cancel"} and index_counts[0] == 2)
            or (phase == "analyze" and sql.startswith("ANALYZE"))
        )
        if has_failed:
            raise asyncio.CancelledError() if phase == "cancel" else RuntimeError("candidate index failed")
        return await original(statement)

    context.session.execute.side_effect = execute
    preserve = AsyncMock()
    monkeypatch.setattr(summary, "preserve_publication_reads", preserve)
    with pytest.raises(asyncio.CancelledError if phase == "cancel" else RuntimeError):
        await summary.rebuild_plan_search_summary(publication=("attempt", object(), False))
    preserve.assert_not_awaited()
    context.session.connection.assert_not_awaited()
    summary.complete_publication.assert_not_awaited()
    assert context.events == ["begin", "rollback"]
    assert not any(sql.startswith(("ALTER", "DROP")) for sql in context.statements)


class IndexNames:
    """Track only actual emitted index/table renames to expose the next-build collision."""

    def __init__(self):
        self.indexes = {"plan_search_summary_pkey": "plan_search_summary"}
        self.tables = {"plan_search_summary"}

    async def create_table(self, table, *, checkfirst):
        name = str(table.name)
        if name in self.tables:
            assert checkfirst
            return
        key = str(table.primary_key.name)
        assert key == name + "_candidate_pkey" and key not in self.indexes
        self.tables.add(name)
        self.indexes[key] = name

    def apply(self, sql):
        import re

        if match := re.match(r'CREATE INDEX "([^"]+)" ON "[^"]+"."([^"]+)"', sql):
            name, table = match.groups()
            assert name not in self.indexes
            self.indexes[name] = table
            return
        if match := re.match(r'DROP TABLE IF EXISTS "[^"]+"."([^"]+)"', sql):
            table = match[1]
            self.tables.discard(table)
            self.indexes = {name: owner for name, owner in self.indexes.items() if owner != table}
            return
        if match := re.match(r'ALTER TABLE(?: IF EXISTS)? "[^"]+"."([^"]+)" RENAME TO "([^"]+)"', sql):
            old, new = match.groups()
            assert old in self.tables and new not in self.tables
            self.tables.remove(old)
            self.tables.add(new)
            self.indexes = {name: new if owner == old else owner for name, owner in self.indexes.items()}
            return
        if match := re.match(r'ALTER INDEX "[^"]+"."([^"]+)" RENAME TO "([^"]+)"', sql):
            self.rename(*match.groups())
            return
        if match := re.match(r'ALTER TABLE "[^"]+"."[^"]+" RENAME CONSTRAINT "([^"]+)" TO "([^"]+)"', sql):
            self.rename(*match.groups())

    def rename(self, old, new):
        assert old in self.indexes and new not in self.indexes
        self.indexes[new] = self.indexes.pop(old)


@pytest.mark.asyncio
@pytest.mark.parametrize("prior_primary_key", ["plan_search_summary_pkey", "plan_search_summary_refresh_pkey"])
async def test_two_successive_stages_keep_private_indexes_and_canonical_names(
    publication_boundary, monkeypatch, prior_primary_key
):
    context = publication_boundary
    names = IndexNames()
    names.indexes = {prior_primary_key: "plan_search_summary"}
    original = context.session.execute.side_effect

    async def execute(statement):
        names.apply(str(statement))
        return await original(statement)

    context.session.execute.side_effect = execute
    monkeypatch.setattr(summary.db, "create_table", names.create_table)
    monkeypatch.setattr(summary, "preserve_publication_reads", AsyncMock())
    for _ in range(2):
        start = len(context.statements)
        assert await summary.rebuild_plan_search_summary() == 3
        statements = context.statements[start:]
        creates = [sql for sql in statements if sql.startswith("CREATE INDEX")]
        assert len(creates) == 2 and all('ON "mrf"."plan_search_summary_refresh"' in sql for sql in creates)
        first_swap = next(index for index, sql in enumerate(statements) if sql.startswith("DROP"))
        assert all(not sql.startswith(("CREATE INDEX", "ANALYZE")) for sql in statements[first_swap:])
        assert names.indexes == {
            "plan_search_summary_pkey": "plan_search_summary",
            "plan_search_summary_state_year_idx": "plan_search_summary",
            "plan_search_summary_issuer_year_idx": "plan_search_summary",
        }
        assert names.tables == {"plan_search_summary"}
