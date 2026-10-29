# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Address publication reports only acknowledged terminal state after cleanup."""

from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import control_lifecycle as lifecycle
from tests.test_entity_address_unified_no_live_mutation import _mock_shutdown_dependencies, _shutdown_context
from tests.test_geo_assurance_dependency_bindings import _example_bindings, native


def _terminal_harness(monkeypatch, *, event_failure=False, cleanup_failure=False, deferred=False, warmup=False):
    events, writes, emitted, contexts = [], [], [], []
    _mock_shutdown_dependencies(monkeypatch, events)
    monkeypatch.setenv("HLTHPRT_IMPORT_LIVE_PROGRESS_HEARTBEAT_SECONDS", "0")
    monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_DEFER_PUBLISH_VALIDATION", str(deferred).lower())
    monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_POST_PUBLISH_INDEX_PROFILE", "all" if warmup else "none")
    monkeypatch.setattr(native, "mark_control_run", lifecycle.mark_control_run)
    monkeypatch.setattr(lifecycle, "_should_update_control_run_db", AsyncMock(return_value=True))
    monkeypatch.setattr(lifecycle, "_flush_terminal_status_events", AsyncMock())
    monkeypatch.setattr(lifecycle, "read_live_progress", lambda _run_id: None)

    async def persist(statement, **_options):
        values_by_field = {
            getattr(key, "key", str(key)): getattr(value, "value", value) for key, value in statement._values.items()
        }
        writes.append(values_by_field)
        return 1

    def emit(**values):
        emitted.append(values)
        if (
            event_failure
            and values["status"] == "succeeded"
            and sum(item["status"] == "succeeded" for item in emitted) == 1
        ):
            raise RuntimeError("synthetic notification unavailable")

    @asynccontextmanager
    async def selected(*_args, **_options):
        yield _example_bindings()
        if cleanup_failure:
            raise RuntimeError("synthetic lock owner release failed")

    async def target(ctx, _task):
        setup = _shutdown_context(refresh_mode=native.ENTITY_ADDRESS_REFRESH_MODE_FULL)
        ctx["import_date"] = setup["import_date"]
        ctx["context"].update(setup["context"])
        contexts.append(ctx["context"])

    monkeypatch.setattr(lifecycle, "_execute_control_run_update", persist)
    monkeypatch.setattr(lifecycle, "write_live_progress", emit)
    monkeypatch.setattr(native, "selected_publication_dependencies", selected)
    monkeypatch.setattr(
        lifecycle, "import_module", lambda _name: SimpleNamespace(process_data=target, shutdown=native.shutdown)
    )
    return writes, emitted, contexts


async def _run():
    return await lifecycle.control_single_job_start(
        {},
        {
            "run_id": "run_test",
            "importer": "entity-address-unified",
            "target_module": "process.entity_address_unified",
            "target_function": "process_data",
            "run_shutdown": True,
            "task": {},
        },
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "event_failure,cleanup_failure,deferred", ((False, False, False), (False, True, False), (True, False, True))
)
async def test_real_shutdown_and_wrapper_preserve_acknowledged_terminal_success(
    monkeypatch, event_failure, cleanup_failure, deferred
):
    writes, emitted, contexts = _terminal_harness(
        monkeypatch, event_failure=event_failure, cleanup_failure=cleanup_failure, deferred=deferred
    )
    result = await _run()
    assert result["status"] == "succeeded"
    assert [entry["status"] for entry in writes] == (
        ["running", "running", "succeeded"] if deferred else ["running", "succeeded"]
    )
    context = contexts[0]
    assert context["control_run_terminal_committed"] is True
    assert (
        lifecycle._committed_control_timestamp(context["_control_committed_finished_at"]).replace(tzinfo=None)
        == writes[-1]["finished_at"]
    )
    assert emitted[-1]["status_event_payload"]["finished_at"] == context["_control_committed_finished_at"]
    assert emitted[-1]["phase"] == result["result"]["terminal_progress"]["phase"]


@pytest.mark.asyncio
@pytest.mark.parametrize("index_error", (False, True))
async def test_post_publish_warmup_projects_final_acknowledged_metrics(monkeypatch, index_error):
    writes, emitted, contexts = _terminal_harness(monkeypatch, warmup=True, cleanup_failure=True)

    async def warmup(_schema, *, context):
        context["post_publish_index_completed"] = 3
        if index_error:
            raise RuntimeError("synthetic warmup failure")

    monkeypatch.setattr(native, "_create_post_publish_indexes", warmup)
    result = await _run()
    assert result["status"] == "succeeded"
    assert result["result"]["post_publish_index_completed"] == 3
    assert bool(result["result"]["post_publish_index_error"]) is index_error
    assert writes[-1]["phase_detail"] == emitted[-1]["phase"]
    assert emitted[-1]["status_event_payload"]["metrics"]["post_publish_index_completed"] == 3
    assert contexts[0]["_control_committed_finished_at"] == lifecycle.isoformat_utc(
        writes[1]["finished_at"], timespec="microseconds"
    )
    assert "finished_at" not in writes[-1]
    for event in emitted:
        if event["status"] == "succeeded":
            assert event["finished_at"] == contexts[0]["_control_committed_finished_at"]
            assert event["status_event_payload"]["finished_at"] == contexts[0]["_control_committed_finished_at"]


@pytest.mark.asyncio
async def test_validation_failure_never_installs_committed_success(monkeypatch):
    writes, _emitted, contexts = _terminal_harness(monkeypatch, deferred=True)
    monkeypatch.setattr(
        native, "_validate_publish_integrity", AsyncMock(side_effect=RuntimeError("synthetic invalid stage"))
    )
    with pytest.raises(RuntimeError, match="synthetic invalid stage"):
        await _run()
    assert not contexts[0].get("control_run_terminal_committed")
    assert "succeeded" not in [entry["status"] for entry in writes]
    assert writes[-1]["status"] == "failed"


@pytest.mark.asyncio
async def test_rejected_terminal_transition_does_not_claim_commit(monkeypatch):
    context_by_field = {}
    monkeypatch.setattr(lifecycle, "_should_update_control_run_db", AsyncMock(return_value=True))
    monkeypatch.setattr(lifecycle, "_execute_control_run_update", AsyncMock(return_value=0))
    monkeypatch.setattr(lifecycle, "read_live_progress", lambda _run_id: None)
    assert (
        await lifecycle.mark_control_run(
            "run_test",
            status="succeeded",
            phase_detail="published",
            progress_message="succeeded",
            committed_context=context_by_field,
        )
        is False
    )
    assert context_by_field == {}


@pytest.mark.asyncio
async def test_uncommitted_outer_transaction_cannot_install_terminal_marker(monkeypatch):
    monkeypatch.setattr(lifecycle.db, "_transaction_binding", lambda: object())
    with pytest.raises(ValueError, match="owned terminal transition"):
        await lifecycle.mark_control_run(
            "run_test", status="succeeded", phase_detail="published", progress_message="succeeded", committed_context={}
        )
