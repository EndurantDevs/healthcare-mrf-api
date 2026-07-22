# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Stable response and validation edges for control-plane route wrappers."""

from __future__ import annotations

import json
import types

import pytest
from sanic.exceptions import BadRequest

from api import control


def request(*, payload=None, args=None):
    return types.SimpleNamespace(json=payload, args=args or {})


@pytest.fixture(autouse=True)
def allow_control_request(monkeypatch):
    monkeypatch.setattr(control, "_require_control_auth", lambda _request: None)


@pytest.mark.asyncio
async def test_control_health_worker_routes_keep_stable_envelopes(monkeypatch):
    async def fake_node_health():
        return {"database": "ready"}

    monkeypatch.setattr(control, "node_health", fake_node_health)
    monkeypatch.setattr(control, "worker_registry", lambda: [{"name": "finish"}])
    monkeypatch.setattr(
        control,
        "ensure_worker",
        lambda payload: {"name": payload["name"], "state": "starting"},
    )

    health = await control.control_node_health(request())
    workers = await control.control_workers(request())
    ensured = await control.control_ensure_worker(request(payload={"name": "finish"}))

    assert json.loads(health.body) == {"database": "ready"}
    assert json.loads(workers.body)["items"] == [{"name": "finish"}]
    assert ensured.status == 202
    assert json.loads(ensured.body)["state"] == "starting"


def raise_value_error(*_args, **_kwargs):
    raise ValueError("invalid control payload")


async def raise_value_error_async(*_args, **_kwargs):
    raise ValueError("invalid control payload")


@pytest.mark.asyncio
async def test_control_ptg_wrappers_translate_validation_errors(monkeypatch):
    monkeypatch.setattr(control, "parse_ptg_toc_preview", raise_value_error)
    with pytest.raises(BadRequest, match="invalid control payload"):
        await control.control_ptg_parse_toc_preview(request(payload={}))

    monkeypatch.setattr(control, "create_import_run", raise_value_error_async)
    with pytest.raises(BadRequest, match="invalid control payload"):
        await control.control_ptg_import_file(request(payload={}))
    with pytest.raises(BadRequest, match="invalid control payload"):
        await control.control_create_import(request(payload={}))

    monkeypatch.setattr(
        control,
        "record_candidate_audit_attestation",
        raise_value_error_async,
    )
    with pytest.raises(BadRequest, match="invalid control payload"):
        await control.control_ptg_source_snapshot_attest(
            request(payload={"report": {}})
        )


@pytest.mark.asyncio
async def test_control_snapshot_wrappers_translate_validation_errors(monkeypatch):
    endpoint_by_dependency = (
        (
            "build_ptg2_source_snapshot_remove_plan",
            control.control_ptg_source_snapshot_remove_plan,
        ),
        ("remove_ptg2_source_snapshot", control.control_ptg_source_snapshot_remove),
        ("retire_ptg2_source_snapshot", control.control_ptg_source_snapshot_retire),
    )

    for dependency_name, endpoint in endpoint_by_dependency:
        monkeypatch.setattr(control, dependency_name, raise_value_error_async)
        with pytest.raises(BadRequest, match="invalid control payload"):
            await endpoint(request(payload={"snapshot_id": "snapshot-1"}))


@pytest.mark.asyncio
async def test_control_list_cancel_and_finalize_translate_validation_errors(
    monkeypatch,
):
    monkeypatch.setattr(control, "list_import_runs_page", raise_value_error_async)
    with pytest.raises(BadRequest, match="invalid control payload"):
        await control.control_list_imports(request(args={"limit": "1"}))

    monkeypatch.setattr(control, "request_cancel", raise_value_error_async)
    with pytest.raises(BadRequest, match="invalid control payload"):
        await control.control_cancel_import(request(), "run-1")

    monkeypatch.setattr(control, "finalize_import_run", raise_value_error_async)
    with pytest.raises(BadRequest, match="invalid control payload"):
        await control.control_finalize_import(request(payload={}), "run-1")
