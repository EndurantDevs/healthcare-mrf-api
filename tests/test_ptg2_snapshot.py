# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import pytest

from api import ptg2_snapshot


class _Result:
    def __init__(self, *, scalar_value=None, rows=()):
        self.scalar_value = scalar_value
        self.rows = list(rows)

    def scalar(self):
        return self.scalar_value

    def __iter__(self):
        return iter(self.rows)


class _SnapshotSession:
    def __init__(
        self,
        *,
        snapshot_id: str,
        source_key: str,
        plan_ids=None,
        plan_market_type=None,
    ):
        self.snapshot_id = snapshot_id
        self.source_key = source_key
        self.plan_ids = plan_ids
        self.plan_market_type = plan_market_type
        self.calls = []

    async def execute(self, statement, params=None):
        sql = str(statement)
        query_params_by_name = dict(params or {})
        self.calls.append((sql, query_params_by_name))
        is_match = (
            query_params_by_name.get("snapshot_id") == self.snapshot_id
            and query_params_by_name.get("source_key") == self.source_key
            and (
                self.plan_ids is None
                or query_params_by_name.get("plan_ids") == self.plan_ids
            )
            and (
                self.plan_market_type is None
                or query_params_by_name.get("plan_market_type")
                == self.plan_market_type
            )
        )
        return _Result(scalar_value=self.snapshot_id if is_match else None)


@pytest.mark.asyncio
async def test_explicit_snapshot_source_and_plan_are_resolved_together():
    session = _SnapshotSession(
        snapshot_id="strict-v3",
        source_key="source-a",
        plan_ids=["12-3456789", "123456789"],
        plan_market_type="group",
    )

    resolved = await ptg2_snapshot.resolve_current_ptg2_snapshot_id(
        session,
        {
            "snapshot_id": "strict-v3",
            "source_key": " Source-A ",
            "plan_id": "12-3456789",
            "plan_market_type": "GROUP",
        },
    )

    assert resolved == "strict-v3"
    sql, params = session.calls[0]
    assert "ptg2_v3_candidate_audit_attestation source_attestation" in sql
    assert "source_attestation.source_key = :source_key" in sql
    assert "source_attestation.activated_at IS NOT NULL" in sql
    assert "ptg2_snapshot.manifest->'serving_index'->>'source_key'" not in sql
    assert "mrf.ptg2_v3_snapshot_plan_scope snapshot_scope" in sql
    assert "snapshot_scope.snapshot_id = ptg2_snapshot.snapshot_id" in sql
    assert "snapshot_scope.plan_id" in sql
    assert "snapshot_scope.plan_market_type" in sql
    assert params == {
        "snapshot_id": "strict-v3",
        "source_key": "source-a",
        "plan_ids": ["12-3456789", "123456789"],
        "plan_market_type": "group",
    }


@pytest.mark.asyncio
async def test_explicit_snapshot_resolution_is_published_only():
    session = _SnapshotSession(snapshot_id="strict-v3", source_key="source-a")

    await ptg2_snapshot.resolve_current_ptg2_snapshot_id(
        session,
        {"snapshot_id": "strict-v3", "source_key": "source-a"},
    )
    sql, params = session.calls[-1]
    assert "status = 'published'" in sql
    assert "status = 'validated'" not in sql
    assert "manifest->'activation'" not in sql
    assert params == {"snapshot_id": "strict-v3", "source_key": "source-a"}


@pytest.mark.asyncio
async def test_explicit_snapshot_source_mismatch_fails_closed():
    session = _SnapshotSession(snapshot_id="strict-v3", source_key="source-a")

    resolved = await ptg2_snapshot.resolve_current_ptg2_snapshot_id(
        session,
        {
            "snapshot_id": "strict-v3",
            "source_key": "source-b",
            "plan_id": "12-3456789",
        },
    )

    assert resolved is None
    assert len(session.calls) == 1


@pytest.mark.asyncio
async def test_explicit_snapshot_plan_mismatch_fails_closed():
    session = _SnapshotSession(
        snapshot_id="strict-v3",
        source_key="source-a",
        plan_ids=["98-7654321", "987654321"],
    )

    resolved = await ptg2_snapshot.resolve_current_ptg2_snapshot_id(
        session,
        {
            "snapshot_id": "strict-v3",
            "source_key": "source-a",
            "plan_id": "12-3456789",
        },
    )

    assert resolved is None
    assert len(session.calls) == 1


class _FailingSession:
    async def execute(self, _statement, _params=None):
        raise RuntimeError("database unavailable")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("resolver", "args"),
    [
        (ptg2_snapshot.current_source_snapshot_id, ("source-a",)),
        (ptg2_snapshot.current_source_snapshot_id_for_plan, ({"plan_id": "plan-a"},)),
        (ptg2_snapshot.current_network_snapshots_for_plan, ({"plan_id": "plan-a"},)),
    ],
)
async def test_source_pointer_query_exceptions_propagate(resolver, args):
    with pytest.raises(RuntimeError, match="database unavailable"):
        await resolver(_FailingSession(), *args)


class _EmptySession:
    async def execute(self, _statement, _params=None):
        return _Result()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("resolver", "args", "expected"),
    [
        (ptg2_snapshot.current_source_snapshot_id, ("source-a",), None),
        (ptg2_snapshot.current_source_snapshot_id_for_plan, ({"plan_id": "plan-a"},), None),
        (ptg2_snapshot.current_network_snapshots_for_plan, ({"plan_id": "plan-a"},), []),
    ],
)
async def test_successful_empty_source_pointer_queries_preserve_not_found(
    resolver,
    args,
    expected,
):
    assert await resolver(_EmptySession(), *args) == expected


class _RecordingSession:
    def __init__(self, result):
        self.result = result
        self.calls = []

    async def execute(self, statement, params=None):
        self.calls.append((str(statement), dict(params or {})))
        return self.result


@pytest.mark.asyncio
async def test_snapshot_resolvers_reject_empty_explicit_selectors():
    session = _RecordingSession(_Result(scalar_value="unused"))

    assert await ptg2_snapshot.current_snapshot_id(
        session,
        requested_snapshot_id="snapshot",
        requested_source_key="   ",
    ) is None
    assert await ptg2_snapshot.current_snapshot_id(
        session,
        requested_snapshot_id="snapshot",
        requested_plan_id="   ",
    ) is None
    assert await ptg2_snapshot.current_source_snapshot_id(session, "   ") is None
    assert await ptg2_snapshot.current_source_snapshot_id_for_plan(
        session,
        {},
    ) is None
    assert await ptg2_snapshot.current_network_snapshots_for_plan(
        session,
        {},
    ) == []
    assert session.calls == []


@pytest.mark.asyncio
async def test_global_snapshot_resolution_covers_found_and_missing_results():
    found_session = _RecordingSession(_Result(scalar_value="global-snapshot"))
    missing_session = _RecordingSession(_Result())

    assert await ptg2_snapshot.current_snapshot_id(found_session) == "global-snapshot"
    assert await ptg2_snapshot.resolve_current_ptg2_snapshot_id(
        missing_session,
        {},
    ) is None


@pytest.mark.asyncio
async def test_plan_snapshot_resolvers_bind_optional_market_and_source_filters():
    plan_session = _RecordingSession(_Result(scalar_value="plan-snapshot"))
    network_session = _RecordingSession(
        _Result(rows=(("source-a", "network-snapshot"),))
    )
    args_by_name = {
        "plan_id": "12-3456789",
        "plan_market_type": "GROUP",
        "source_key": " Source-A ",
    }

    assert await ptg2_snapshot.current_source_snapshot_id_for_plan(
        plan_session,
        args_by_name,
    ) == "plan-snapshot"
    assert await ptg2_snapshot.current_network_snapshots_for_plan(
        network_session,
        args_by_name,
    ) == [("source-a", "network-snapshot")]
    for _sql, params in (plan_session.calls[0], network_session.calls[0]):
        assert params == {
            "plan_ids": ["12-3456789", "123456789"],
            "plan_market_type": "group",
            "source_key": "source-a",
        }
