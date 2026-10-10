# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native batch HTTP scope precedes eligibility, paging and mutable enrichment."""

from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from sanic import Sanic

from api import network_address_scope
from api.endpoint import npi
from process import network_serving_read
from tests.npi_location_hydration_support import unified_location_mapping


@pytest.fixture
def native_network_batch(monkeypatch):
    """Pin real HTTP scope/auth around synthetic retained and live office reads."""
    member, nonmember = 1234567890, 1098765432
    candidate_id = uuid4()
    manifest = network_serving_read.PinnedNetworkServingManifest(
        12, str(candidate_id), "network_candidate_" + candidate_id.hex, 1, {}, 0, "c" * 64, 1
    )
    driver = object()
    connection = SimpleNamespace(get_raw_connection=AsyncMock(return_value=SimpleNamespace(driver_connection=driver)))
    session = SimpleNamespace(execute=AsyncMock(), connection=AsyncMock(return_value=connection))
    resolve = AsyncMock(return_value=manifest)
    monkeypatch.setattr(network_serving_read, "resolve_network_serving_manifest", resolve)
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
    app = Sanic("native_network_batch_" + uuid4().hex)

    @app.middleware("request")
    async def bind_session(request):
        """Reuse the one synthetic request session throughout the real route."""
        request.ctx.sa_session = session

    app.add_route(npi.get_npi_batch, "/npi/id/batch", methods=["POST"])
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
    context.resolve.assert_awaited_once_with(context.driver, generation_id=12)
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
