# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import importlib
import types

import pytest


def _workflow_module():
    return importlib.import_module("process.ptg_address_entity_refresh")


@pytest.mark.asyncio
async def test_ptg_address_entity_refresh_runs_children_without_child_run_ids(monkeypatch):
    workflow = _workflow_module()
    calls = []
    marks = []

    def child_module(name, context_updates):
        async def startup(ctx):
            calls.append((name, "startup", None))
            ctx["context"] = {"run": 0}

        async def process_data(ctx, task):
            calls.append((name, "process_data", dict(task)))
            ctx["context"].update(context_updates)
            ctx["context"]["run"] = 1

        async def shutdown(ctx):
            calls.append((name, "shutdown", None))
            ctx["context"][f"{name}_shutdown"] = True

        return types.SimpleNamespace(startup=startup, process_data=process_data, shutdown=shutdown)

    monkeypatch.setattr(
        workflow,
        "ptg_address",
        child_module(
            "ptg",
            {
                "refresh_mode": "partial",
                "source_keys": ["source-a"],
                "snapshot_ids": ["snap-new"],
                "staged_rows": 3,
                "partial_refresh_reused_rows": 0,
                "partial_refresh_patched_rows": 3,
                "partial_refresh_patch_publish": True,
            },
        ),
    )
    monkeypatch.setattr(
        workflow,
        "entity_address_unified",
        child_module(
            "entity",
            {
                "refresh_mode": "ptg-partial",
                "partial_ptg_source_keys": ["source-a"],
                "staged_rows": 5,
                "partial_ptg_reused_rows": 0,
                "partial_ptg_patched_rows": 5,
                "support_counts": {"entity_address_ptg_bridge": 2},
            },
        ),
    )

    async def fake_mark(run_id, **kwargs):
        marks.append((run_id, kwargs))

    monkeypatch.setattr(workflow, "mark_control_run", fake_mark)

    result = await workflow.process_data(
        {"control_run_id": "run_wrapper", "context": {"control_run_id": "run_wrapper"}},
        {
            "source_key": "source-a",
            "snapshot_id": "snap-new",
            "test_mode": True,
            "limit_per_source": 25,
            "publish": True,
        },
    )

    assert calls == [
        ("ptg", "startup", None),
        (
            "ptg",
            "process_data",
            {
                "test_mode": True,
                "refresh_mode": "partial",
                "source_key": "source-a",
                "snapshot_id": "snap-new",
            },
        ),
        ("ptg", "shutdown", None),
        ("entity", "startup", None),
        (
            "entity",
            "process_data",
            {
                "test_mode": True,
                "refresh_mode": "ptg-partial",
                "ptg_source_key": "source-a",
                "limit_per_source": 25,
                "publish": True,
            },
        ),
        ("entity", "shutdown", None),
    ]
    assert [item[0] for item in marks] == ["run_wrapper", "run_wrapper"]
    assert marks[0][1]["phase_detail"] == "ptg-address refresh running"
    assert marks[1][1]["phase_detail"] == "entity-address-unified refresh running"
    assert result["rows"] == 5
    assert result["source_key"] == "source-a"
    assert result["snapshot_id"] == "snap-new"
    assert result["ptg_address"]["partial_refresh_patch_publish"] is True
    assert result["ptg_address"]["partial_refresh_patched_rows"] == 3
    assert result["entity_address_unified"]["partial_ptg_patched_rows"] == 5
    assert result["entity_address_unified"]["support_counts"] == {"entity_address_ptg_bridge": 2}


@pytest.mark.asyncio
async def test_ptg_address_entity_refresh_requires_source_key_for_default_partial_modes():
    workflow = _workflow_module()

    with pytest.raises(RuntimeError, match="requires source_key"):
        await workflow.process_data({}, {"test_mode": True})


@pytest.mark.asyncio
async def test_ptg_address_entity_refresh_allows_explicit_full_modes_without_source(monkeypatch):
    workflow = _workflow_module()
    tasks = []

    async def fake_run_child(_module, task):
        tasks.append(dict(task))
        return {"refresh_mode": task["refresh_mode"], "staged_rows": 1}

    async def fake_mark(*_args, **_kwargs):
        return None

    monkeypatch.setattr(workflow, "_run_child", fake_run_child)
    monkeypatch.setattr(workflow, "mark_control_run", fake_mark)

    result = await workflow.process_data(
        {"control_run_id": "run_full"},
        {"ptg_refresh_mode": "full", "entity_refresh_mode": "full", "test_mode": True},
    )

    assert tasks == [
        {"test_mode": True, "refresh_mode": "full"},
        {"test_mode": True, "refresh_mode": "full"},
    ]
    assert result["rows"] == 1
