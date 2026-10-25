# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Authenticated HTTP readiness and closed complete-code import dispatch."""

from contextlib import asynccontextmanager
from dataclasses import asdict
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from urllib.parse import urlencode
from uuid import uuid4

import pytest
from sanic import Blueprint, Sanic

from api import control_imports as imports
from api import control_plan_pricing_code as routes
from api.plan_pricing_code_contract import CONTRACT, IMPORTER, identity_digest
from api.plan_release_serving import PlanReleaseSnapshotBinding


def _selectors():
    return {
        "plan_release_id": "hprelease_" + "1" * 26,
        "serving_revision_id": "hpserve_" + "2" * 26,
        "code_system": "CPT",
        "code": "27447",
    }


@pytest.fixture
def service(monkeypatch):
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "synthetic-token")
    binding = PlanReleaseSnapshotBinding(
        0, "synthetic-snapshot", "synthetic-source", "synthetic-plan", "group", "in_network", True, True
    )
    selection = SimpleNamespace(
        serving_revision_id=_selectors()["serving_revision_id"],
        binding_set_digest="a" * 64,
        in_network_bindings=(binding,),
    )
    identity_map = {"contract": CONTRACT, **_selectors(), "binding_set_digest": selection.binding_set_digest}
    state = SimpleNamespace(
        session=object(),
        events=[],
        binding=binding,
        selection=selection,
        identity=identity_map,
        resolve=AsyncMock(
            return_value=SimpleNamespace(state=routes.PLAN_RELEASE_RESOLUTION_READY, selection=selection)
        ),
        inputs=AsyncMock(return_value=(identity_map, ())),
        locate=AsyncMock(return_value=None),
        enqueue=AsyncMock(side_effect=AssertionError("readiness cannot enqueue")),
    )

    @asynccontextmanager
    async def transaction():
        state.events.append("opened")
        try:
            yield state.session
        finally:
            state.events.append("closed")

    monkeypatch.setattr(routes, "db", SimpleNamespace(transaction=transaction))
    monkeypatch.setattr(routes, "resolve_plan_release_serving_resolution", state.resolve)
    monkeypatch.setattr(routes, "resolve_inputs", state.inputs)
    monkeypatch.setattr(routes, "locate", state.locate)
    monkeypatch.setattr(imports, "_enqueue_import_start", state.enqueue)
    app = Sanic("code_readiness_" + uuid4().hex)
    blueprint = Blueprint("code_readiness_" + uuid4().hex, url_prefix="/control/v1")
    routes.register_code_pricing_routes(blueprint)
    app.blueprint(blueprint)
    state.app = app
    return state


async def _get(service, selectors=None, *, headers=None):
    return (
        await service.app.asgi_client.get(
            "/control/v1/plan-pricing/code-projection?" + urlencode(_selectors() if selectors is None else selectors),
            headers={"Authorization": "Bearer synthetic-token"} if headers is None else headers,
        )
    )[1]


@pytest.mark.asyncio
@pytest.mark.parametrize("token", (None, "", "wrong-private-token"))
async def test_authentication_refuses_before_storage_and_hides_credentials(service, token):
    headers = {} if token is None else {"Authorization": "Bearer " + token}
    returned = await _get(service, headers=headers)
    assert returned.status == 403 and "wrong-private-token" not in returned.text
    assert service.events == []
    service.resolve.assert_not_awaited()
    service.inputs.assert_not_awaited()
    service.locate.assert_not_awaited()
    service.enqueue.assert_not_awaited()


@pytest.mark.asyncio
async def test_unconfigured_authentication_fails_closed(service, monkeypatch):
    monkeypatch.delenv("HLTHPRT_CONTROL_API_TOKEN")
    assert (await _get(service)).status == 403
    assert service.events == []
    service.enqueue.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "selectors",
    (
        {},
        _selectors() | {"source_key": "raw-source"},
        _selectors() | {"code_system": "cpt"},
        _selectors() | {"code": " 27447"},
        list(_selectors().items()) + [("code", "99213")],
    ),
)
async def test_invalid_or_repeated_selectors_never_open_storage(service, selectors):
    assert (await _get(service, selectors)).status == 400
    assert service.events == []
    service.resolve.assert_not_awaited()
    service.inputs.assert_not_awaited()
    service.locate.assert_not_awaited()
    service.enqueue.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ("unpublished", "different-revision"))
async def test_only_current_published_revision_reaches_code_consumer(service, change):
    if change == "unpublished":
        service.resolve.return_value.state = "absent"
    else:
        service.selection.serving_revision_id = "hpserve_" + "3" * 26
    returned = await _get(service)
    assert returned.status == 400 and "exact current published release revision" in returned.text
    assert service.events == ["opened", "closed"]
    service.inputs.assert_not_awaited()
    service.locate.assert_not_awaited()
    service.enqueue.assert_not_awaited()


@pytest.mark.asyncio
async def test_absent_projection_returns_exact_digest_and_reads_current_identity(service):
    returned = await _get(service)
    assert returned.status == 200
    assert returned.json == {
        **_selectors(),
        "contract": CONTRACT,
        "state": "absent",
        "identity": service.identity,
        "expected_identity_digest": identity_digest(service.identity),
    }
    service.resolve.assert_awaited_once_with(service.session, _selectors()["plan_release_id"])
    service.inputs.assert_awaited_once_with(service.session, service.selection, ("CPT", "27447"))
    service.locate.assert_awaited_once_with(service.session, service.identity, allow_stale_provider=True)
    assert service.events == ["opened", "closed"]
    service.enqueue.assert_not_awaited()


@pytest.mark.asyncio
async def test_missing_membership_returns_exact_prerequisites_without_locating(service):
    service.inputs.side_effect = routes.CodeProjectionMembershipNotReady("not sealed")
    returned = await _get(service)
    assert returned.status == 200
    assert returned.json == {
        **_selectors(),
        "contract": CONTRACT,
        "state": "prerequisite_missing",
        "missing": "complete_source_procedure_membership",
        "binding_set_digest": "a" * 64,
        "bindings": [asdict(service.binding)],
        "required_membership": [
            {
                "source_key": "synthetic-source",
                "snapshot_id": "synthetic-snapshot",
                "code_system": "CPT",
                "code": "27447",
            }
        ],
    }
    service.locate.assert_not_awaited()
    assert service.events == ["opened", "closed"]
    service.enqueue.assert_not_awaited()


@pytest.mark.asyncio
async def test_available_projection_returns_storage_receipt(service, monkeypatch):
    found = object()
    service.locate.return_value = found
    receipt = Mock(return_value={"contract": CONTRACT, "state": "ready", "projection_id": "b" * 64})
    monkeypatch.setattr(routes, "receipt", receipt)
    returned = await _get(service)
    assert returned.status == 200 and returned.json == receipt.return_value
    receipt.assert_called_once_with(found)
    assert service.events == ["opened", "closed"]
    service.enqueue.assert_not_awaited()


@pytest.mark.asyncio
async def test_unexpected_storage_failure_is_redacted_and_transaction_closes(service):
    service.locate.side_effect = RuntimeError("synthetic-private-storage-detail")
    returned = await _get(service)
    assert returned.status == 500 and "synthetic-private-storage-detail" not in returned.text
    assert service.events == ["opened", "closed"]
    service.enqueue.assert_not_awaited()


@pytest.mark.asyncio
async def test_invalid_import_identity_never_mutates_or_enqueues(monkeypatch):
    database = SimpleNamespace(execute=AsyncMock(), acquire=Mock())
    enqueue = AsyncMock()
    lookup = AsyncMock()
    monkeypatch.setattr(imports, "db", database)
    monkeypatch.setattr(imports, "_enqueue_import_start", enqueue)
    monkeypatch.setattr(imports, "find_earliest_active_run_by_importer", lookup)
    monkeypatch.setattr(imports, "_idempotent_import_run", lookup)
    with pytest.raises(ValueError, match="expected identity digest"):
        await imports.create_import_run(
            {"importer": IMPORTER, "params": _selectors() | {"expected_identity_digest": "invalid"}}
        )
    database.execute.assert_not_awaited()
    database.acquire.assert_not_called()
    enqueue.assert_not_awaited()
    lookup.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "scope",
    [{}, {"scope_kind": "zip_radius", "zip5": "48104", "zip_radius_miles": "25"}],
)
async def test_valid_import_dispatches_complete_code_consumer(monkeypatch, scope):
    enqueue = AsyncMock(return_value=SimpleNamespace(job_id="plan_pricing_code_projection_run_synthetic"))
    execute = AsyncMock(return_value=SimpleNamespace(rowcount=1))
    monkeypatch.setattr(imports, "db", SimpleNamespace(execute=execute))
    monkeypatch.setattr(imports, "find_earliest_active_run_by_importer", AsyncMock(return_value=None))
    monkeypatch.setattr(imports, "create_pool", AsyncMock(return_value=SimpleNamespace(enqueue_job=enqueue)))
    monkeypatch.setattr(imports, "enqueue_status_event", Mock())
    monkeypatch.setattr(imports, "_write_run_live_progress", Mock())
    monkeypatch.setattr(imports, "read_live_progress", Mock(return_value=None))
    params = _selectors() | {"expected_identity_digest": "c" * 64} | scope
    returned, created = await imports.create_import_run(
        {"importer": IMPORTER, "run_id": "run_synthetic", "params": params}
    )
    assert created and returned["params"] == params
    assert execute.await_count == 2
    enqueue.assert_awaited_once_with(
        "control_single_job_start",
        {
            "run_id": "run_synthetic",
            "importer": IMPORTER,
            "family": "mrf",
            "target_module": "api.plan_pricing_code_build",
            "target_function": "control_start",
            "call_style": "ctx_task",
            "run_shutdown": False,
            "task": {"test_mode": False, **params},
        },
        _queue_name="arq:PTGCandidateAudit",
        _job_id="plan_pricing_code_projection_run_synthetic",
    )
