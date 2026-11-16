# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Only the same successful response acknowledges a supported filter contract."""

from types import SimpleNamespace
from uuid import uuid4

import pytest
from sanic import Sanic
from sanic.compat import Header
from sanic.response import json

from api.address_source_filter import (
    ADDRESS_SOURCE_FILTER_CONTRACT,
    ADDRESS_SOURCE_FILTER_HEADER,
    acknowledge_address_source_filter,
)


@pytest.mark.parametrize(
    "tokens,status,expected",
    [
        ([], 200, []),
        (["v1"], 200, ["v1"]),
        (["v1"], 201, ["v1"]),
        (["v2"], 200, []),
        (["v1", "v1"], 200, []),
        (["v1,v1"], 200, []),
        (["v1"], 400, []),
        (["v1"], 503, []),
    ],
)
def test_only_supported_single_negotiation_acknowledges_success(tokens, status, expected):
    headers = Header((ADDRESS_SOURCE_FILTER_HEADER, token) for token in tokens)
    request = SimpleNamespace(headers=headers)
    response = json({"data": []}, status=status)
    response.headers.add(ADDRESS_SOURCE_FILTER_HEADER, "stale")
    response.headers.add(ADDRESS_SOURCE_FILTER_HEADER, "untrusted")

    acknowledge_address_source_filter(request, response)

    assert response.headers.get_all(ADDRESS_SOURCE_FILTER_HEADER) == expected
    assert response.body == b'{"data":[]}'


@pytest.mark.asyncio
async def test_http_acknowledgement_is_negotiated_per_response():
    app = Sanic("address_filter_contract_" + uuid4().hex)
    app.register_middleware(acknowledge_address_source_filter, "response")

    @app.get("/addresses")
    async def addresses(_request):
        return json({"addresses": []})

    _, default = await app.asgi_client.get("/addresses")
    _, supported = await app.asgi_client.get(
        "/addresses", headers={ADDRESS_SOURCE_FILTER_HEADER: ADDRESS_SOURCE_FILTER_CONTRACT}
    )
    _, duplicate = await app.asgi_client.get(
        "/addresses", headers=[(ADDRESS_SOURCE_FILTER_HEADER, "v1"), (ADDRESS_SOURCE_FILTER_HEADER, "v1")]
    )

    assert default.status == supported.status == duplicate.status == 200
    assert default.json == supported.json == duplicate.json == {"addresses": []}
    assert ADDRESS_SOURCE_FILTER_HEADER not in default.headers
    assert supported.headers[ADDRESS_SOURCE_FILTER_HEADER] == ADDRESS_SOURCE_FILTER_CONTRACT
    assert ADDRESS_SOURCE_FILTER_HEADER not in duplicate.headers
