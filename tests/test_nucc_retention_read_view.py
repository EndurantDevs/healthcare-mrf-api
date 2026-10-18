# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""The fixed all-state projection must refuse unsafe or stale negative proofs."""

import asyncio
from contextlib import asynccontextmanager
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import nucc_retention_read_view as view


def native_catalog():
    owner, builder = 11, 12
    return dict(
        namespace=dict(oid=10, owner=owner, safe=True),
        ledgers=[dict(name=name, oid=20 + index, owner=owner, safe=True) for index, name in enumerate(view.LEDGERS)],
        view=dict(
            oid=30,
            owner=owner,
            safe=True,
            definition=view.CLAIMS_SQL + ";",
            columns=[["relation_oid", "bigint", "", "", False, None]],
            acl=sorted(
                [[owner, owner, privilege, False] for privilege in view.OWNER_PRIVILEGES]
                + [[builder, owner, "SELECT", False]]
            ),
            dependencies=[20, 21, 22, 23],
            other_dependencies=False,
        ),
    )


def test_all_four_claim_ledgers_are_unfiltered_and_indexed():
    assert len(view.LEDGERS) == 4 and view.CLAIMS_SQL.count("UNION ALL") == 3
    assert "WHERE" not in view.CLAIMS_SQL and "state" not in view.CLAIMS_SQL
    assert all(f"FROM hp_snapshot_retention.{name}" in view.CLAIMS_SQL for name in view.LEDGERS)
    assert "i.indisunique AND i.indisvalid AND i.indisready" in view.CATALOG_SQL
    assert "i.indkey[0]=a.attnum" in view.CATALOG_SQL
    assert "security_invoker=false" in view.CREATE_VIEW_SQL


@pytest.mark.parametrize(
    "path,replacement",
    [
        (("view", "owner"), 91),
        (("view", "definition"), "SELECT relation_oid FROM hp_snapshot_retention.relation WHERE false"),
        (("view", "dependencies"), [20, 21, 22, 23, 99]),
        (("view", "acl"), [[0, 11, "SELECT", False]]),
        (("view", "columns"), [["relation_oid", "bigint", "", "", False, "{reader=r/owner}"]]),
        (("view", "safe"), False),
        (("ledgers",), []),
        (("namespace", "safe"), False),
    ],
)
def test_projection_refuses_every_native_contract_change(path, replacement):
    captured = native_catalog()
    parent = captured
    for key in path[:-1]:
        parent = parent[key]
    parent[path[-1]] = replacement
    with pytest.raises(view.NuccRetentionReadError):
        view.validate_projection(captured, builder_oid=12)


def test_only_native_total_absence_preserves_legacy_catalog_branch():
    assert view.retention_catalog(dict(namespace=None, ledgers=[], view=None)) is None
    for catalog in (dict(namespace=None, ledgers=[], view={}), dict(namespace=None, ledgers=[{}], view=None)):
        with pytest.raises(view.NuccRetentionReadError):
            view.retention_catalog(catalog)
    first = native_catalog()
    identity = view.validate_projection(first, builder_oid=12)
    replacement = deepcopy(first)
    replacement["view"]["oid"] += 1
    with pytest.raises(view.NuccRetentionReadError, match="identity"):
        view.validate_projection(replacement, builder_oid=12, previous=identity)


class _Savepoint:
    def __init__(self, events):
        self.events = events

    async def __aenter__(self):
        self.events.append("savepoint")

    async def __aexit__(self, error_type, *_args):
        self.events.append("rollback" if error_type else "release")


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, "claimed", "uncertain", "actor", "replacement", "cancel"])
async def test_negative_check_uses_one_fresh_query_after_pinned_view(monkeypatch, failure):
    events, original = [], native_catalog()
    replacement = deepcopy(original)
    if failure == "replacement":
        replacement["view"]["oid"] += 1
    scalar_results = [
        dict(oid=12, safe=failure != "actor"),
        original,
        replacement,
        True if failure == "claimed" else None if failure == "uncertain" else False,
    ]

    async def scalar(statement, *_args):
        query = str(statement)
        events.append(query)
        if failure == "cancel" and "SELECT EXISTS" in query:
            raise asyncio.CancelledError
        return scalar_results.pop(0)

    session = SimpleNamespace(
        in_transaction=lambda: True,
        scalar=AsyncMock(side_effect=scalar),
        execute=AsyncMock(side_effect=lambda statement: events.append(str(statement))),
    )

    @asynccontextmanager
    async def search_path(_session):
        yield

    monkeypatch.setattr(view, "catalog_search_path", search_path)
    if failure:
        with pytest.raises(asyncio.CancelledError if failure == "cancel" else view.NuccRetentionReadError):
            await view.require_unretained_nucc_oids(session, [71, 72])
    else:
        await view.require_unretained_nucc_oids(session, [72, 71])
        assert session.scalar.await_args.args[1] == {"oids": [71, 72]}
    queries = [query for query in events if "SELECT EXISTS" in query]
    assert len(queries) == int(failure not in {"actor", "replacement"})
    if queries:
        lock_index = events.index(f"LOCK TABLE ONLY {view.VIEW} IN ACCESS SHARE MODE NOWAIT")
        assert lock_index < events.index(queries[0])
        assert "ANY(CAST(:oids AS bigint[]))" in queries[0]


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, RuntimeError, asyncio.CancelledError])
async def test_search_path_rollback_does_not_execute_restore_in_aborted_savepoint(failure):
    events = []
    session = SimpleNamespace(
        begin_nested=lambda: _Savepoint(events), scalar=AsyncMock(return_value='"$user", public'), execute=AsyncMock()
    )
    if failure:
        with pytest.raises(failure):
            async with view.catalog_search_path(session):
                raise failure()
        assert events == ["savepoint", "rollback"] and session.execute.await_count == 1
    else:
        async with view.catalog_search_path(session):
            assert events == ["savepoint"]
        assert events == ["savepoint", "release"] and session.execute.await_count == 2
        assert session.execute.await_args.args[1] == {"path": '"$user", public'}


@pytest.mark.asyncio
@pytest.mark.parametrize("oids", [[], [1, 2, 3], [True], [0], [2**32], ["1"]])
async def test_unbounded_or_ambiguous_oid_scope_refuses_before_catalogue(oids):
    session = SimpleNamespace(in_transaction=lambda: True, scalar=AsyncMock())
    with pytest.raises(view.NuccRetentionReadError, match="OID scope"):
        await view.require_unretained_nucc_oids(session, oids)
    session.scalar.assert_not_awaited()
