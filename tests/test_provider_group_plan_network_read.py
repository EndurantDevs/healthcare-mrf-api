# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Group HTTP orchestration and retained SQL boundaries; no native authority claim."""

import json
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import pytest
from sanic import Sanic
from sanic.exceptions import InvalidUsage, ServiceUnavailable

from api import plan_release_serving
from api import provider_group_plan_network_read as managed
from api.network_address_scope import network_address_read_scope
from api.plan_pricing_projection_contract import PlanPricingProjectionUnavailable
from process.network_serving_read import PinnedNetworkServingManifest
from process.registry_catalog_policy import RegistryOrganizationVisibility
from tests.test_pricing_api import (
    FakeResult,
    _canonical_directory_selection,
    _canonical_directory_tables,
    make_request,
    pricing_module,
)


@contextmanager
def held_scope():
    candidate = str(UUID(int=19))
    manifest = PinnedNetworkServingManifest(
        17, candidate, "network_candidate_" + UUID(candidate).hex, 1, {}, 23, "b" * 64, 99
    )
    with network_address_read_scope(
        manifest,
        None,
        access_scope_sha256="a" * 64,
        excluded_network_ids=(7,),
        organization_policy=RegistryOrganizationVisibility(),
    ):
        yield manifest


@pytest.fixture
def directory(monkeypatch):
    selection = _canonical_directory_selection()
    tables = _canonical_directory_tables()
    resolve = AsyncMock(return_value=selection)
    admit = AsyncMock(return_value=(selection.in_network_bindings[1],))
    native = AsyncMock(side_effect=lambda _session, snapshot: tables[snapshot])
    legacy = AsyncMock(side_effect=AssertionError("managed offices cannot use a current legacy head"))

    async def is_ready(session, binding, **options):
        assert admit.await_count == 1
        assert binding is selection.in_network_bindings[1]
        options["validated_serving_tables_by_snapshot_id"][binding.snapshot_id] = await native(
            session, binding.snapshot_id
        )
        return True

    monkeypatch.setattr(plan_release_serving, "is_release_binding_serving_ready", is_ready)
    monkeypatch.setattr(pricing_module, "resolve_plan_release_serving", resolve)
    monkeypatch.setattr(pricing_module, "snapshot_serving_tables", native)
    monkeypatch.setattr(pricing_module, "_group_plan_provider_address_source", legacy)
    monkeypatch.setattr(managed, "admitted_plan_release_bindings", admit)
    return SimpleNamespace(selection=selection, resolve=resolve, admit=admit, native=native, legacy=legacy)


def office(city="ALLOWED OFFICE", zip5="00002"):
    return {
        "npi": 1073913877,
        "type": "practice",
        "first_line": "2 EXAMPLE WAY",
        "second_line": None,
        "city_name": city,
        "state_name": "NY",
        "postal_code": zip5,
        "zip5": zip5,
        "phone_number": None,
        "address_precision": "street",
        "plan_coverage_match": True,
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("location", [{}, {"city": "Allowed Office"}, {"zip5": "00002", "zip_radius_miles": "0"}])
async def test_managed_group_handler_filters_before_page_count_and_hydration(directory, location):
    request = make_request(
        [
            FakeResult(rows=[SimpleNamespace(npi=1073913877)]),
            FakeResult(scalar=1),
            FakeResult(rows=[office()]),
        ],
        args={"plan_release_id": directory.selection.plan_release_id, "count": "true", "limit": "1", **location},
    )
    with held_scope() as manifest:
        document = json.loads((await pricing_module.group_plan_providers(request)).body)
    directory.admit.assert_awaited_once_with(request.ctx.sa_session, directory.selection)
    assert [call.args[1] for call in directory.native.await_args_list] == ["ptg2:release:b"]
    directory.legacy.assert_not_awaited()
    assert document["binding_set_digest"] == directory.selection.binding_set_digest
    assert document["plan_ids"] == ["99-0000001", "PLAN-B"]
    assert document["resolved_snapshot_ids"] == ["ptg2:release:b"]
    assert document["providers"]["total"] == 1
    assert document["providers"]["next_cursor"] == "1073913877"
    assert document["providers"]["items"][0]["address"]["city"] == "ALLOWED OFFICE"
    assert document["location_filter"]["count_exact"] is True
    executions = request.ctx.sa_session.executions
    assert len(executions) == 3
    for index, (arguments, _) in enumerate(executions):
        sql, parameters = str(arguments[0]), arguments[1]
        assert f'"{manifest.schema_name}".entity_address_unified' in sql
        assert "row_origin IN ('base','ptg_overlay') OR EXISTS" in sql
        assert parameters["_network_excluded_ids"] == [7]
        assert parameters["_network_approved_revision"] == 23
        assert "npi_address" not in sql
        if index == 0:
            assert parameters["page_snapshot_key_0"] == 72
            assert sql.index("addr.npi = gm.npi") < sql.index("LIMIT :limit")
        elif index == 1:
            assert "COUNT(DISTINCT gm.npi)" in sql and "LIMIT" not in sql
        else:
            assert "entity_address_plan_bridge" not in sql
            assert parameters["npis"] == [1073913877]


@pytest.mark.asyncio
async def test_zero_source_admission_returns_empty_without_member_or_address_sql(directory):
    directory.admit.return_value = ()
    request = make_request([], args={"plan_release_id": directory.selection.plan_release_id, "count": "true"})
    with held_scope():
        document = json.loads((await pricing_module.group_plan_providers(request)).body)
    assert document["resolved"] is True
    assert document["providers"]["items"] == []
    assert document["providers"]["total"] == 0
    assert document["resolved_snapshot_ids"] == []
    assert document["binding_set_digest"] == directory.selection.binding_set_digest
    directory.admit.assert_awaited_once()
    directory.native.assert_not_awaited()
    assert request.ctx.sa_session.executions == []


@pytest.mark.asyncio
async def test_source_admission_failure_never_uses_unfiltered_member_stream(directory):
    directory.admit.side_effect = PlanPricingProjectionUnavailable("stale review")
    request = make_request([], args={"plan_release_id": directory.selection.plan_release_id})
    with held_scope(), pytest.raises(ServiceUnavailable, match="sources"):
        await pricing_module.group_plan_providers(request)
    directory.native.assert_not_awaited()
    assert request.ctx.sa_session.executions == []


@pytest.mark.asyncio
@pytest.mark.parametrize("selector", ["plan_id", "source_key", "snapshot_id"])
async def test_managed_group_never_widens_conflicting_release_selectors(directory, selector):
    request = make_request([], args={"plan_release_id": directory.selection.plan_release_id, selector: "example"})
    with held_scope(), pytest.raises(InvalidUsage, match="cannot be combined"):
        await pricing_module.group_plan_providers(request)
    directory.resolve.assert_not_awaited()
    directory.admit.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("args", [{"plan_id": "123456789"}, {"plan_release_id": "hprelease_" + "0" * 26}])
async def test_managed_group_requires_actual_ready_release(directory, args):
    directory.resolve.return_value = None
    request = make_request([], args=args)
    with held_scope(), pytest.raises(ServiceUnavailable, match="release"):
        await pricing_module.group_plan_providers(request)
    assert request.ctx.sa_session.executions == []
    directory.native.assert_not_awaited()


@pytest.mark.parametrize(
    "name,value",
    [
        ("limit", "0"),
        ("limit", "1001"),
        ("cursor", "-1"),
        ("cursor", "10000000000"),
        ("cursor", "x"),
        ("limit", "１２"),
    ],
)
def test_managed_group_rejects_malformed_or_unbounded_windows(name, value):
    request = SimpleNamespace(args={"plan_release_id": "hprelease_" + "0" * 26, name: value})
    with held_scope(), pytest.raises(InvalidUsage):
        managed.group_plan_arguments(request, None)


def test_native_options_require_bound_managed_context():
    with pytest.raises(ServiceUnavailable, match="scope"):
        managed.group_plan_arguments(SimpleNamespace(args={}), {"plan_release_id": "example"})


@pytest.mark.asyncio
async def test_managed_group_http_uses_bound_dispatcher_native_options(directory):
    app = Sanic("managed-group-" + uuid4().hex)
    request = make_request([FakeResult(rows=[SimpleNamespace(npi=1073913877)]), FakeResult(rows=[office()])])
    native_by_name = {"plan_release_id": directory.selection.plan_release_id, "limit": "2"}

    @app.post("/group")
    async def group_http(actual_request):
        actual_request.ctx.sa_session = request.ctx.sa_session
        with held_scope():
            return await pricing_module.group_plan_providers(actual_request, native_args=native_by_name)

    _, reply = await app.asgi_client.post("/group")
    assert reply.status == 200
    assert reply.json["providers"]["items"][0]["address"]["zip5"] == "00002"
    directory.admit.assert_awaited_once_with(request.ctx.sa_session, directory.selection)


@pytest.mark.asyncio
async def test_admitted_physical_readiness_failure_is_unavailable_before_office_sql(directory, monkeypatch):
    readiness = AsyncMock(return_value=False)
    monkeypatch.setattr(plan_release_serving, "is_release_binding_serving_ready", readiness)
    request = make_request([], args={"plan_release_id": directory.selection.plan_release_id})
    with held_scope(), pytest.raises(ServiceUnavailable, match="sources"):
        await pricing_module.group_plan_providers(request)
    directory.admit.assert_awaited_once_with(request.ctx.sa_session, directory.selection)
    assert readiness.await_args.args[1] is directory.selection.in_network_bindings[1]
    directory.native.assert_not_awaited()
    assert request.ctx.sa_session.executions == []
