# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native batch HTTP scope precedes eligibility, paging and mutable enrichment."""

from contextlib import asynccontextmanager
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from sanic import Sanic

from api import network_address_scope
from api.endpoint import npi
from process import network_serving_read
from process.network_compact_layout import CompactAddressRelation
from tests.network_read_execution_support import mock_approved_control_metadata, with_read_session
from tests.npi_location_hydration_support import unified_location_mapping


def _install_catalog_snapshot(monkeypatch, session, driver, resolve):
    monkeypatch.setattr(network_serving_read, "resolve_network_serving_manifest", resolve)

    @asynccontextmanager
    async def snapshot(actual, generation_id=None, control_schema=None):
        assert actual is session and generation_id == 12
        pinned = await resolve(driver, generation_id=generation_id, control_schema=control_schema, api_read=True)
        yield pinned, CompactAddressRelation("registry_control", "registry_approved_record")

    monkeypatch.setattr("api.network_serving_api_read.api_catalog_read_snapshot", snapshot)


def _install_existing_snapshot(monkeypatch, session):
    @asynccontextmanager
    async def existing_snapshot(_database, request_session=None, *, reuse=False):
        assert request_session is session
        yield session

    monkeypatch.setattr("api.provider_profile_snapshot.provider_directory_read_snapshot", existing_snapshot)


def _batch_http_app(session):
    app = Sanic("native_network_batch_" + uuid4().hex)

    @app.middleware("request")
    async def bind_session(request):
        """Reuse the one synthetic request session throughout the real route."""
        request.ctx.sa_session = session

    app.add_route(npi.get_npi_batch, "/npi/id/batch", methods=["POST"])
    return app


def _install_batch_details(monkeypatch, member, nonmember, retained_by_field, outside_by_field):
    """Keep live overlays distinct from the retained office supplied to the route."""
    overlay = AsyncMock(return_value={member: [outside_by_field]})
    hydration = AsyncMock(return_value={member: [deepcopy(retained_by_field)]})
    monkeypatch.setattr(
        npi,
        "_build_npi_identity_details_map",
        AsyncMock(return_value={member: {"npi": member}, nonmember: {"npi": nonmember}}),
    )
    monkeypatch.setattr(
        npi, "_fetch_npi_location_candidates_map", AsyncMock(return_value={member: [retained_by_field]})
    )
    monkeypatch.setattr(npi, "_fetch_provider_directory_address_overlay_map", overlay)
    monkeypatch.setattr(npi, "_fetch_npi_address_rows_map", hydration)
    monkeypatch.setattr(npi, "_apply_location_statuses", AsyncMock())
    monkeypatch.setattr(npi, "_fetch_other_names_map", AsyncMock(return_value={}))
    monkeypatch.setattr(npi, "_fetch_provider_enrichment_summary_map", AsyncMock(return_value={}))
    return overlay, hydration


@pytest.fixture
def native_network_batch(monkeypatch):
    """Pin real HTTP scope/auth around synthetic retained and live office reads."""
    member, nonmember = 1234567890, 1098765432
    candidate_id = uuid4()
    manifest = network_serving_read.PinnedNetworkServingManifest(
        12,
        str(candidate_id),
        "network_candidate_" + candidate_id.hex,
        1,
        {},
        0,
        "c" * 64,
        CompactAddressRelation("network_candidate_" + candidate_id.hex, "entity_address_unified"),
    )
    session = with_read_session(SimpleNamespace(execute=AsyncMock()))

    _install_existing_snapshot(monkeypatch, session)
    driver = session.connection.return_value.get_raw_connection.return_value.driver_connection
    mock_approved_control_metadata(monkeypatch)
    bind_reader = AsyncMock(return_value=session)
    monkeypatch.setattr("api.network_serving_read_context.bind_network_request_reader", bind_reader)
    resolve = AsyncMock(return_value=manifest)

    _install_catalog_snapshot(monkeypatch, session, driver, resolve)
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "synthetic-network-batch-token")
    retained_by_field = {**unified_location_mapping(), "npi": member, "inferred_npi": None}
    retained_by_field["_base_row_identities"] = [npi._base_address_row_identity(retained_by_field)]
    outside_by_field = {
        **retained_by_field,
        "address_key": "00000000-0000-0000-0000-000000000099",
        "first_line": "999 Example Avenue",
        "source_count": 9,
        "_base_row_identities": [],
    }
    overlay, hydration = _install_batch_details(monkeypatch, member, nonmember, retained_by_field, outside_by_field)
    app = _batch_http_app(session)
    return SimpleNamespace(
        app=app,
        member=member,
        nonmember=nonmember,
        retained=retained_by_field,
        overlay=overlay,
        hydration=hydration,
        session=session,
        resolve=resolve,
        driver=driver,
        bind_reader=bind_reader,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("native_query", [{"extra_info": "true"}, {"extra_info": "true", "limit": "1"}])
async def test_retained_batch_never_ranks_or_pages_a_live_outside_office(native_network_batch, native_query):
    """Extra information cannot admit a different-key office from the live directory."""
    context = native_network_batch
    _, reply = await context.app.asgi_client.post(
        "/npi/id/batch?network_ids=42&network_generation=12",
        headers={"Authorization": "Bearer synthetic-network-batch-token"},
        json={"npis": [context.member], "native_query": native_query, "address_limit": 1},
    )
    assert reply.status == 200
    assert reply.headers["X-Network-Generation"] == "12"
    assert reply.headers["Cache-Control"] == "private, no-store"
    provider_by_field = reply.json["items"][0]["provider"]
    assert [office["address_key"] for office in provider_by_field["address_list"]] == [context.retained["address_key"]]
    assert provider_by_field["address_pagination"]["total"] == 1
    context.overlay.assert_not_awaited()
    assert context.hydration.await_args.kwargs["session"] is context.session
    context.resolve.assert_awaited_once()
    assert context.resolve.await_args.args == (context.driver,)
    assert {"control_schema": None, **context.resolve.await_args.kwargs} == {
        "generation_id": 12,
        "control_schema": None,
        "api_read": True,
    }
    assert network_address_scope.current_network_address_scope() is None


@pytest.mark.asyncio
@pytest.mark.parametrize("scoped", [True, False])
@pytest.mark.parametrize("native_query", [{}, {"limit": "1"}])
async def test_mixed_batch_excludes_only_scoped_identity_only_nonmembers(native_network_batch, scoped, native_query):
    """A known nonmember is a per-item absence; unscoped identity-only results remain."""
    context = native_network_batch
    context.overlay.return_value = {}
    suffix = "?network_ids=42&network_generation=12" if scoped else ""
    _, reply = await context.app.asgi_client.post(
        "/npi/id/batch" + suffix,
        headers={"Authorization": "Bearer synthetic-network-batch-token"},
        json={"npis": [context.nonmember, context.member], "native_query": native_query},
    )
    assert reply.status == 200
    assert (reply.json["found"], reply.json["not_found"], reply.json["pagination"]["total"]) == (
        (1, 1, 1) if scoped else (2, 0, 2)
    )
    if scoped:
        assert {entry["npi"]: entry["status"] for entry in reply.json["items"]} == {
            context.member: 200,
            context.nonmember: 404,
        }
        assert context.hydration.await_args.args[0] == [context.member]
        assert reply.headers["X-Network-Generation"] == "12"
    else:
        assert all(entry["status"] == 200 for entry in reply.json["items"])
        assert "X-Network-Generation" not in reply.headers
    assert network_address_scope.current_network_address_scope() is None


@pytest.mark.asyncio
@pytest.mark.parametrize("native_query", [None, {}], ids=("plain_body", "native_query"))
@pytest.mark.parametrize("scoped", [True, False], ids=("canonical", "unfiltered"))
async def test_plain_and_native_batches_share_scoped_metadata_projection(
    native_network_batch, monkeypatch, scoped, native_query
):
    """The public body shape must not bypass the existing exact-office metadata projection."""
    context = native_network_batch
    context.retained["plans_network_array"] = [-17]
    context.hydration.return_value = {context.member: [deepcopy(context.retained)]}
    monkeypatch.setattr(npi, "_attach_selected_address_source_details", AsyncMock())

    async def project(provider_rows, *, session, include_sources, include_evidence):
        assert session is context.session and include_sources is True and include_evidence is False
        scope = network_address_scope.current_network_address_scope()
        if scope is None:
            return provider_rows
        assert scope.network_ids == (42,) and scope.access_scope_sha256 == "a" * 64
        projected = deepcopy(provider_rows)
        for provider in projected:
            for office in provider["address_list"]:
                office.pop("plans_network_array", None)
                office["network_memberships"] = [{"network_id": 42, "location_id": context.retained["address_key"]}]
        return projected

    projection = AsyncMock(side_effect=project)
    monkeypatch.setattr(npi, "_project_provider_network_metadata", projection)
    suffix = "?network_ids=42&network_generation=12" if scoped else ""
    request_by_field = {"npis": [context.member, context.nonmember], "include_sources": True}
    if native_query is not None:
        request_by_field["native_query"] = native_query
    _, reply = await context.app.asgi_client.post(
        "/npi/id/batch" + suffix,
        headers={"Authorization": "Bearer synthetic-network-batch-token", "X-Network-Access-Scope": "a" * 64},
        json=request_by_field,
    )
    assert reply.status == 200
    office = reply.json["items"][0]["provider"]["address_list"][0]
    if scoped:
        context.bind_reader.assert_awaited_once()
        projection.assert_awaited_once()
        assert "plans_network_array" not in office
        assert office["network_memberships"] == [{"network_id": 42, "location_id": context.retained["address_key"]}]
        assert reply.headers["X-Network-Generation"] == "12"
    else:
        context.bind_reader.assert_not_awaited()
        assert office["plans_network_array"] == [-17] and "network_memberships" not in office
        assert "X-Network-Generation" not in reply.headers


@pytest.mark.asyncio
@pytest.mark.parametrize("refusal", ["authorization", "selector"])
async def test_canonical_batch_admission_precedes_reader_binding(native_network_batch, refusal):
    """A bad token or selector cannot open the Reader or resolve retained metadata."""
    context = native_network_batch
    headers = {"Authorization": "Bearer synthetic-network-batch-token"} if refusal == "selector" else {}
    generation = "01" if refusal == "selector" else "12"
    _, reply = await context.app.asgi_client.post(
        "/npi/id/batch?network_ids=42&network_generation=" + generation,
        headers=headers,
        json={"npis": [context.member]},
    )
    assert reply.status == (400 if refusal == "selector" else 403)
    context.bind_reader.assert_not_awaited()
    context.resolve.assert_not_awaited()
    context.hydration.assert_not_awaited()
