import json
import types

import pytest

from api.endpoint.nucc import index_status_nucc


@pytest.mark.asyncio
async def test_nucc_endpoint():
    request = types.SimpleNamespace(
        app=types.SimpleNamespace(config={"RELEASE": "test", "ENVIRONMENT": "test"})
    )
    response = await index_status_nucc(request)
    payload = json.loads(response.body)
    assert payload == {}
