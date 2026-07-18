from __future__ import annotations

import hashlib
from contextlib import asynccontextmanager

import pytest

from process.ptg_parts import ptg2_shared_snapshot_publish as publication
from process.ptg_parts.ptg2_shared_reuse import (
    SharedLogicalPlanScope,
    SharedPhysicalArtifactIdentity,
    SharedSnapshotSourceAssignment,
    shared_source_set_metadata,
)


class _Rows:
    def __init__(self, rows=()):
        self.rows = list(rows)

    def __iter__(self):
        return iter(self.rows)

    def first(self):
        return self.rows[0] if self.rows else None


class _Session:
    def __init__(self, *, scope, observed, snapshot_state=None):
        self.scope = scope
        self.observed = observed
        self.snapshot_state = snapshot_state
        self.calls = []

    async def execute(self, statement, params=None):
        sql = str(statement)
        self.calls.append((sql, params))
        if "SELECT plan_id, plan_market_type" in sql:
            return _Rows([self.scope])
        if "SELECT snapshot_id, source_key" in sql:
            return _Rows(self.observed)
        if "SELECT snapshot.status" in sql:
            return _Rows([self.snapshot_state] if self.snapshot_state is not None else [])
        return _Rows()


def _assignment(*, trace_set_hash: str, raw_sha: str = "b" * 64):
    return SharedSnapshotSourceAssignment(
        source_key=0,
        identity=SharedPhysicalArtifactIdentity(
            "in_network",
            "logical_json_sha256_v1",
            "a" * 64,
        ),
        source_trace_set_hash=trace_set_hash,
        source_trace_hashes=("c" * 64,),
        raw_container_sha256=raw_sha,
        logical_json_sha256="a" * 64,
        logical_hash_deferred=False,
    )


def _row(snapshot_id: str, assignment: SharedSnapshotSourceAssignment):
    return {
        "snapshot_id": snapshot_id,
        "source_key": 0,
        **assignment.identity.as_dict(),
        "raw_container_sha256": assignment.raw_container_sha256,
        "logical_json_sha256": assignment.logical_json_sha256,
        "logical_hash_deferred": assignment.logical_hash_deferred,
        "source_trace_set_hash": assignment.source_trace_set_hash,
    }


def test_source_set_seal_is_order_independent_and_count_bound():
    raw_hashes = ["2" * 64, "1" * 64]
    expected_digest = hashlib.sha256(
        bytes.fromhex("1" * 64) + bytes.fromhex("2" * 64)
    ).hexdigest()

    assert shared_source_set_metadata(raw_hashes) == {
        "contract": "sorted_raw_container_sha256_bytes_v1",
        "source_count": 2,
        "raw_container_sha256_digest": expected_digest,
    }
    assert shared_source_set_metadata(list(reversed(raw_hashes))) == (
        shared_source_set_metadata(raw_hashes)
    )


def test_source_set_seal_rejects_duplicate_raw_containers():
    with pytest.raises(ValueError, match="duplicate raw containers"):
        shared_source_set_metadata(["1" * 64, "1" * 64])


@pytest.mark.asyncio
async def test_fresh_snapshot_source_publication_is_insert_only(monkeypatch):
    assignment = _assignment(trace_set_hash="d" * 64)
    session = _Session(
        scope={
            "plan_id": "plan-1",
            "plan_market_type": "group",
            "coverage_scope_id": b"s" * 32,
        },
        observed=[_row("snapshot-fresh", assignment)],
    )

    @asynccontextmanager
    async def transaction():
        yield session

    monkeypatch.setattr(publication.db, "transaction", transaction)
    rows = await publication.publish_shared_v3_snapshot_sources(
        schema_name="mrf",
        snapshot_id="snapshot-fresh",
        plan_scopes=[SharedLogicalPlanScope("plan-1", "ein", "group")],
        coverage_scope_id=b"s" * 32,
        assignments=[assignment],
    )

    assert rows == (_row("snapshot-fresh", assignment),)
    source_insert = next(sql for sql, _params in session.calls if "INSERT INTO \"mrf\".ptg2_v3_snapshot_source" in sql)
    assert "ON CONFLICT (snapshot_id, source_key) DO NOTHING" in source_insert
    assert "DO UPDATE" not in source_insert


@pytest.mark.asyncio
async def test_reused_layout_snapshots_keep_independent_trace_and_wrapper_rows(monkeypatch):
    first = _assignment(trace_set_hash="1" * 64, raw_sha="2" * 64)
    second = _assignment(trace_set_hash="3" * 64, raw_sha="4" * 64)
    sessions = [
        _Session(
            scope={"plan_id": "plan-1", "plan_market_type": "group", "coverage_scope_id": b"s" * 32},
            observed=[_row("snapshot-one", first)],
        ),
        _Session(
            scope={"plan_id": "plan-2", "plan_market_type": "group", "coverage_scope_id": b"s" * 32},
            observed=[_row("snapshot-two", second)],
        ),
    ]

    @asynccontextmanager
    async def transaction():
        yield sessions.pop(0)

    monkeypatch.setattr(publication.db, "transaction", transaction)
    first_rows = await publication.publish_shared_v3_snapshot_sources(
        schema_name="mrf",
        snapshot_id="snapshot-one",
        plan_scopes=[SharedLogicalPlanScope("plan-1", "ein", "group")],
        coverage_scope_id=b"s" * 32,
        assignments=[first],
    )
    second_rows = await publication.publish_shared_v3_snapshot_sources(
        schema_name="mrf",
        snapshot_id="snapshot-two",
        plan_scopes=[SharedLogicalPlanScope("plan-2", "ein", "group")],
        coverage_scope_id=b"s" * 32,
        assignments=[second],
    )

    assert first_rows[0]["identity_sha256"] == second_rows[0]["identity_sha256"]
    assert first_rows[0]["source_trace_set_hash"] != second_rows[0]["source_trace_set_hash"]
    assert first_rows[0]["raw_container_sha256"] != second_rows[0]["raw_container_sha256"]


@pytest.mark.asyncio
async def test_snapshot_source_conflict_fails_without_overwrite(monkeypatch):
    assignment = _assignment(trace_set_hash="d" * 64)
    conflicting = _row("snapshot-conflict", assignment)
    conflicting["source_trace_set_hash"] = "e" * 64
    session = _Session(
        scope={"plan_id": "plan-1", "plan_market_type": "group", "coverage_scope_id": b"s" * 32},
        observed=[conflicting],
    )

    @asynccontextmanager
    async def transaction():
        yield session

    monkeypatch.setattr(publication.db, "transaction", transaction)
    with pytest.raises(RuntimeError, match="conflicting source-key mapping"):
        await publication.publish_shared_v3_snapshot_sources(
            schema_name="mrf",
            snapshot_id="snapshot-conflict",
            plan_scopes=[SharedLogicalPlanScope("plan-1", "ein", "group")],
            coverage_scope_id=b"s" * 32,
            assignments=[assignment],
        )

    assert all("DO UPDATE" not in sql for sql, _params in session.calls)


@pytest.mark.asyncio
async def test_failed_snapshot_cleanup_removes_logical_sources_not_layouts(monkeypatch):
    session = _Session(
        scope={},
        observed=[],
        snapshot_state={"status": "building", "is_bound": False},
    )

    @asynccontextmanager
    async def transaction():
        yield session

    monkeypatch.setattr(publication.db, "transaction", transaction)
    await publication.delete_unpublished_snapshot_sources(
        schema_name="mrf",
        snapshot_id="snapshot-failed",
    )

    sql = "\n".join(statement for statement, _params in session.calls)
    assert "DELETE FROM \"mrf\".ptg2_v3_snapshot_source" in sql
    assert "DELETE FROM \"mrf\".ptg2_v3_snapshot_scope" in sql
    assert "ptg2_v3_snapshot_layout AS" not in sql


@pytest.mark.asyncio
async def test_failed_snapshot_cleanup_preserves_bound_candidate_sources(monkeypatch):
    session = _Session(
        scope={},
        observed=[],
        snapshot_state={"status": "validated", "is_bound": True},
    )

    @asynccontextmanager
    async def transaction():
        yield session

    monkeypatch.setattr(publication.db, "transaction", transaction)
    await publication.delete_unpublished_snapshot_sources(
        schema_name="mrf",
        snapshot_id="snapshot-candidate",
    )

    sql = "\n".join(statement for statement, _params in session.calls)
    assert "pg_advisory_xact_lock" in sql
    assert "DELETE FROM \"mrf\".ptg2_v3_snapshot_source" not in sql
    assert "DELETE FROM \"mrf\".ptg2_v3_snapshot_scope" not in sql
