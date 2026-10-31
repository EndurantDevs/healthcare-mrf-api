# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Long-prework ordering and truthful data comparisons, not native guard authority."""

import asyncio
from copy import deepcopy
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_cms_generic_seal as seal
from process import provider_directory_cms_prepared_candidate as candidate
from tests.test_provider_directory_cms_prepared_candidate import _header


def _connection(state):
    class Savepoint:
        async def start(self):
            self.before = deepcopy(state.header)
            state.events.append("savepoint")

        async def commit(self):
            state.events.append("release")

        async def rollback(self):
            state.header = self.before
            state.events.append("rollback")

    async def fetchrow(sql, *_arguments):
        if sql == seal.custody._ACTOR_SQL:
            return deepcopy(state.actor)
        if sql.startswith("UPDATE "):
            state.events.append("bulk_published")
            if state.failure == "published":
                raise RuntimeError("synthetic_published")
            assert state.header["status"] == "validated" and not state.header["is_current"]
            assert "statement_timestamp() AT TIME ZONE 'UTC'" in sql
            state.header.update(status="published", published_at=state.stamp)
        return deepcopy(state.header)

    async def execute(sql, *_arguments):
        assert "SET status='validated'" in sql
        state.events.append("bulk_validated")
        assert state.header["status"] == "acquiring"
        state.header.update(status="validated", validated_at=state.expected["validated_at"])
        return "UPDATE 1"

    state.connection = SimpleNamespace(
        fetchrow=AsyncMock(side_effect=fetchrow),
        execute=AsyncMock(side_effect=execute),
        transaction=Savepoint,
        is_closed=lambda: False,
    )
    return state.connection


def _configure_semantics(state, monkeypatch):
    async def is_original_admin(_statement):
        state.checks += 1
        return state.failure != ("admin_entry" if state.checks == 1 else "admin_exit")

    monkeypatch.setattr(seal, "NativeReceiptSession", lambda _connection: SimpleNamespace(scalar=is_original_admin))
    _configure_data(state, monkeypatch)
    _configure_coverage(state, monkeypatch)


def _configure_data(state, monkeypatch):
    async def private(*_arguments):
        state.events.append("private_semantics")
        if state.failure == "private":
            raise RuntimeError("synthetic_private")
        return deepcopy(state.private)

    async def identities(*_arguments):
        state.events.append("identities")
        return {}

    async def copied(*_arguments, **_keywords):
        state.events.append("four_copy")
        if state.failure == "copy":
            raise asyncio.CancelledError("synthetic_copy")
        return {"row_counts": deepcopy(state.counts)}

    async def relationship(_workspace, graph, *, create=False):
        state.events.append("relationship_receipt")
        assert state.header["status"] == "acquiring" and create
        assert graph == state.private["relationship_parity"]

    monkeypatch.setattr(seal, "read_prepared_cms_outputs", private)
    monkeypatch.setattr(seal, "publish_prepared_cms_identities", identities)
    monkeypatch.setattr(seal, "read_prepared_cms_identity_publication", AsyncMock(return_value={}))
    monkeypatch.setattr(seal.payloads, "copy_allocated_cms_generic_payloads", copied)
    monkeypatch.setattr(candidate, "_inputs", AsyncMock(return_value=('"private"."frozen"', state.expected)))
    monkeypatch.setattr(candidate, "_relationship_receipt", relationship)
    monkeypatch.setattr(seal.payloads.allocations, "read_cms_generic_copy_tables", AsyncMock(return_value=state.tables))


def _configure_coverage(state, monkeypatch):
    async def coverage(*_arguments):
        state.events.append("coverage")
        assert state.header["status"] == "validated"
        if state.failure == "coverage":
            raise RuntimeError("synthetic_coverage")
        return {"dataset_id": state.expected["dataset_id"]}

    monkeypatch.setattr(candidate.coverage, "prepare_cms_candidate_coverage_in_session", coverage)
    monkeypatch.setattr(candidate.coverage, "assert_sealed_cms_candidate_coverage", AsyncMock())
    monkeypatch.setattr(candidate.coverage, "_schema_name", lambda: "catalog")


@pytest.fixture
def seal_case(monkeypatch):
    expected = _header()
    state = SimpleNamespace(
        header={**expected, "status": "acquiring", "validated_at": None},
        events=[],
        failure=None,
        expected=expected,
        checks=0,
    )
    roots = candidate.storage.RELATIONS[:4]
    state.counts = dict.fromkeys(roots, 2)
    state.counts[candidate.storage.RESOURCE] = expected["resource_count"]
    state.tables = {
        root: dict(name="leaf_" + str(index), oid=100 + index, owner=41, layout_sha256="a" * 64)
        for index, root in enumerate(roots)
    }
    state.private = {"relationship_parity": {"dataset_id": expected["dataset_id"], "relationship_count": 3}}
    state.actor = {"session": "original", "current": "original", "transaction": "1/1"}
    state.stamp = datetime(2026, 1, 2, 0, 0)
    state.handoff = SimpleNamespace(
        document={"identity": {}, "admission": {"resource_count": expected["resource_count"]}}, document_sha256="a" * 64
    )

    state.connection = _connection(state)
    state.workspace = SimpleNamespace(
        connection=state.connection,
        catalog_schema="catalog",
        session=object(),
        identity={
            "dataset_id": expected["dataset_id"],
            "endpoint_id": expected["endpoint_id"],
            "metadata": {"source_release": {"vector_sha256": "c" * 64}},
        },
        verify_current=AsyncMock(),
    )
    _configure_semantics(state, monkeypatch)
    return state


async def test_complete_semantics_receipts_and_both_bulk_checks_precede_noncurrent_seal(seal_case):
    state = seal_case
    actual = await seal.seal_allocated_cms_generic_candidate(state.workspace, state.handoff, allocation={})
    assert state.events == [
        "savepoint",
        "private_semantics",
        "identities",
        "four_copy",
        "relationship_receipt",
        "bulk_validated",
        "coverage",
        "bulk_published",
        "release",
    ]
    assert actual["header"]["status"] == "published" and actual["header"]["is_current"] is False
    assert actual["completed_at"] == state.stamp.replace(tzinfo=timezone.utc)
    assert actual["tables"] == state.tables
    assert actual["private"] == state.private
    assert actual["copy"]["row_counts"] == state.counts
    assert state.header["superseded_at"] is None
    assert all("is_current=" not in str(call.args[0]) for call in state.connection.execute.await_args_list)


@pytest.mark.parametrize("failure", ["private", "copy", "coverage", "published", "admin_exit"])
async def test_late_failure_or_cancel_rolls_back_the_complete_preseal(seal_case, failure):
    state = seal_case
    state.failure = failure
    before = deepcopy(state.header)
    with pytest.raises((RuntimeError, ValueError, asyncio.CancelledError)):
        await seal.seal_allocated_cms_generic_candidate(state.workspace, state.handoff, allocation={})
    assert state.header == before and state.events[-1] == "rollback"


async def test_ordinary_or_assumed_actor_refuses_before_any_prework_or_write(seal_case):
    state = seal_case
    state.failure = "admin_entry"
    with pytest.raises(ValueError, match="original_admin_required"):
        await seal.seal_allocated_cms_generic_candidate(state.workspace, state.handoff, allocation={})
    assert state.events == []
    state.connection.execute.assert_not_awaited()


def native_data(state):
    return dict(
        contract="generic-source-canonical-copy-v1",
        dataset_id=state.expected["dataset_id"],
        endpoint_id=state.expected["endpoint_id"],
        run_id="synthetic-run",
        attempt_id="synthetic-attempt",
        source_handoff_sha256="a" * 64,
        frozen_sha256="d" * 64,
        validation_sha256="e" * 64,
        tables=deepcopy(state.tables),
        counts=deepcopy(state.counts),
        completed_at=state.stamp.replace(tzinfo=timezone.utc).isoformat(),
    )


async def test_new_header_reader_requires_real_state_and_full_native_four_data(seal_case):
    state = seal_case
    header_by_field = {**state.expected, "status": "published", "published_at": state.stamp}
    state.header = deepcopy(header_by_field)
    assert await candidate._read_header(state.workspace, state.expected) == header_by_field
    native = native_data(state)
    assert (
        await candidate._native_component(
            state.workspace, state.handoff, header_by_field, {"row_counts": state.counts}, native
        )
        == native
    )


@pytest.mark.parametrize("fault", ["missing", "extra", "counts", "boolean", "stamp", "zone", "handoff", "table"])
async def test_native_four_drift_refuses(seal_case, fault):
    state = seal_case
    native = native_data(state)
    header_by_field = {**state.expected, "status": "published", "published_at": state.stamp}
    match fault:
        case "missing":
            native.pop("frozen_sha256")
        case "extra":
            native["authority"] = True
        case "counts":
            native["counts"][candidate.storage.RESOURCE] += 1
        case "boolean":
            native["counts"][candidate.storage.RESOURCE] = True
        case "stamp":
            native["completed_at"] = "2026-01-03T00:00:00+00:00"
        case "zone":
            native["completed_at"] = "2026-01-02T00:00:00"
        case "handoff":
            native["source_handoff_sha256"] = "f" * 64
        case "table":
            next(iter(native["tables"].values()))["layout_sha256"] = "f" * 64
    with pytest.raises(ValueError, match="native_publication"):
        await candidate._native_component(
            state.workspace, state.handoff, header_by_field, {"row_counts": state.counts}, native
        )
