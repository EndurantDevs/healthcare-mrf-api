# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Control SQL and transaction ordering; native concurrency is tested separately."""

from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_generic_source_policy as source
from process import provider_directory_rooted_graph_bulk as bulk
from process import provider_directory_rooted_graph_result_store as results
from process import provider_directory_rooted_graph_store as store
from process.provider_directory_rooted_graph_result_contract import build_provider_directory_rooted_graph_query_result
from process.provider_directory_rooted_graph_store_contract import ProviderDirectoryRootedGraphStoreError
from tests.provider_directory_rooted_graph_runtime_test_support import RuntimeHarness


@pytest.fixture
def scope():
    """Record one explicit unit collaborator, without claiming database acceptance."""
    entries, exits = [], []

    @asynccontextmanager
    async def transaction(database, **options):
        entries.append(options)
        try:
            yield SimpleNamespace(session=object(), connection=object())
        except BaseException as error:
            exits.append(error)
            raise
        else:
            exits.append(None)

    runtime = SimpleNamespace(
        transaction=transaction,
        candidate_schema="synthetic_candidate",
        relation_coordinates=lambda schema, root, scoped: ("synthetic_candidate", root),
    )
    token = source._FACTORY.set(SimpleNamespace(rooted_graph_runtime=runtime))
    claim = RuntimeHarness()._claims("baseline")["role"]
    query_result = build_provider_directory_rooted_graph_query_result(
        claim,
        (
            {
                "resourceType": "PractitionerRole",
                "id": "role-1",
                "practitioner": {"reference": "Practitioner/" + claim.reference_id},
            },
        ),
        advertised_total=1,
        terminal_page_count=1,
    )
    try:
        yield SimpleNamespace(
            database=SimpleNamespace(first=AsyncMock(return_value={}), status=AsyncMock(return_value=1)),
            claim=claim,
            result=query_result,
            entries=entries,
            exits=exits,
        )
    finally:
        source._FACTORY.reset(token)


@pytest.mark.asyncio
async def test_renewal_probes_unfiltered_row_before_strict_live_update(scope):
    assert await store.heartbeat_provider_directory_rooted_graph_work(scope.claim, database=scope.database) == 1
    assert scope.entries == [{"renewal": True}]
    lookup = scope.database.first.await_args.args[0]
    assert "FOR UPDATE NOWAIT" in lookup
    assert not any(predicate in lookup for predicate in ("lease_token", "status=", "clock_timestamp"))
    update = scope.database.status.await_args.args[0]
    assert all(
        predicate in update
        for predicate in (
            "status = 'leased'",
            "attempt_count = :attempt",
            "lease_token = :lease_token",
            "lease_expires_at > clock_timestamp()",
        )
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("code", ["55P03", "57014", "08006"])
async def test_only_busy_row_is_deferred_without_granting_a_renewal(scope, code):
    class DatabaseError(Exception):
        sqlstate = code

    error = DatabaseError("synthetic control failure")
    scope.database.first.side_effect = error
    if code == "55P03":
        assert await store.heartbeat_provider_directory_rooted_graph_work(scope.claim, database=scope.database) == 0
    else:
        with pytest.raises(DatabaseError) as failed:
            await store.heartbeat_provider_directory_rooted_graph_work(scope.claim, database=scope.database)
        assert failed.value is error
    scope.database.status.assert_not_awaited()
    assert scope.exits == [error]


@pytest.mark.asyncio
async def test_unlocked_stale_lease_is_refused(scope):
    scope.database.status.return_value = 0
    with pytest.raises(ProviderDirectoryRootedGraphStoreError, match="lease"):
        await store.heartbeat_provider_directory_rooted_graph_work(scope.claim, database=scope.database)


@pytest.mark.asyncio
@pytest.mark.parametrize("live", [False, True])
async def test_admission_locks_header_then_work_and_checks_clock_before_copy(scope, monkeypatch, live):
    events = []
    answers = iter(
        [{}, {}, {} if live else None, (len(scope.result.resources), 100, False), (len(scope.result.edges), False)]
    )

    async def first(sql, **parameters):
        events.append(sql)
        return next(answers)

    async def copy(*args):
        events.append("COPY")

    scope.database.first.side_effect = first
    monkeypatch.setattr(bulk, "_copy_named_result_stages", copy)
    if live:
        await bulk._named_result_admission(scope.database, object(), scope.claim, scope.result)
    else:
        with pytest.raises(ProviderDirectoryRootedGraphStoreError, match="lease"):
            await bulk._named_result_admission(scope.database, object(), scope.claim, scope.result)
    assert "acquisition" in events[0] and events[0].endswith("FOR UPDATE")
    assert "work" in events[1] and events[1].endswith("FOR UPDATE")
    assert "clock_timestamp()" not in events[1]
    assert "lease_expires_at>clock_timestamp()" in events[2] and "FOR UPDATE" not in events[2]
    assert ("COPY" in events) is live


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", [0, 1])
async def test_result_final_compare_and_set_retains_token_attempt_and_atomic_failure(scope, monkeypatch, changed):
    admission_events = []

    async def admit(*args):
        admission_events.append("admitted")

    async def status(sql, **parameters):
        assert admission_events == ["admitted"]
        assert "status = 'leased'" in sql and "attempt_count = :attempt" in sql
        assert "lease_token = :lease_token" in sql and "clock_timestamp" not in sql
        assert parameters["attempt"] == scope.claim.attempt and parameters["lease_token"] == scope.claim.lease_token
        return changed

    monkeypatch.setattr(results, "admit_result_witnesses", admit)
    monkeypatch.setattr(results, "register_rooted_graph_frontier", AsyncMock())
    scope.database.status.side_effect = status
    if changed:
        await results.complete_provider_directory_rooted_graph_result(
            scope.claim, scope.result, database=scope.database
        )
        assert scope.exits == [None]
    else:
        with pytest.raises(ProviderDirectoryRootedGraphStoreError, match="lease"):
            await results.complete_provider_directory_rooted_graph_result(
                scope.claim, scope.result, database=scope.database
            )
        assert isinstance(scope.exits[0], ProviderDirectoryRootedGraphStoreError)
        results.register_rooted_graph_frontier.assert_not_awaited()
    assert "lease_expires_at > clock_timestamp()" in results._complete_missing_sql()
