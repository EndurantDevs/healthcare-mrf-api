"""Raw immutable release selectors require the protected listener boundary."""

import uuid

import pytest
from sanic import Sanic, response

from api.plan_release_read_boundary import (
    PLAN_RELEASE_READ_BOUNDARY_HEADER,
    require_internal_plan_release_read,
)


@pytest.fixture
def boundary_app():
    app = Sanic("release-boundary-" + uuid.uuid4().hex)
    app.register_middleware(require_internal_plan_release_read, "request")

    @app.get("/<path:path>")
    async def native_endpoint(_request, path):
        return response.json({"path": path})

    return app


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "path",
    (
        "/api/v1/pricing/group-plan-providers",
        "/api/v1/pricing/providers/1234567893/procedures",
        "/api/v1/pricing/providers/audit-search-by-procedure",
        "/api/v1/npi/all",
        "/api/v1/npi/near/",
    ),
)
@pytest.mark.parametrize(
    "query",
    (
        "plan_release_id=example",
        "%70lan_release_id=example",
        "plan_release%5fid=example",
        "plan_release_id=&plan_release_id=example",
        "plan_release_id=example&plan_release_id=",
        "plan_release_id",
    ),
)
async def test_raw_selector_fails_closed_on_every_route(boundary_app, path, query):
    for headers in ({}, {PLAN_RELEASE_READ_BOUNDARY_HEADER: "external"}):
        _, result = await boundary_app.asgi_client.get(f"{path}?{query}", headers=headers)
        assert result.status == 403


@pytest.mark.asyncio
async def test_internal_read_and_native_public_selectors_keep_their_contract(boundary_app):
    _, internal = await boundary_app.asgi_client.get(
        "/api/v1/npi/all?plan_release_id=example",
        headers={PLAN_RELEASE_READ_BOUNDARY_HEADER: "internal"},
    )
    assert internal.status == 200
    for path in ("/api/v1/npi/near/", "/api/v1/pricing/providers/by-procedure"):
        _, public = await boundary_app.asgi_client.get(
            path + "?plan_id=123456789&source_key=example&snapshot_id=example"
        )
        assert public.status == 200


@pytest.mark.asyncio
@pytest.mark.parametrize("values", (("internal", "external"), ("internal", "internal"), ("INTERNAL",)))
async def test_ambiguous_or_unknown_boundary_is_not_internal(boundary_app, values):
    _, result = await boundary_app.asgi_client.get(
        "/api/v1/pricing/group-plan-providers?plan_release_id=example",
        headers=[(PLAN_RELEASE_READ_BOUNDARY_HEADER, value) for value in values],
    )
    assert result.status == 403
