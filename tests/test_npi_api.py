import json
import types

import pytest

from api.endpoint.npi import npi_index_status


@pytest.mark.asyncio
async def test_npi_index(monkeypatch):
    async def fake_counts():
        return 10, 5

    monkeypatch.setattr("api.endpoint.npi._compute_npi_counts", fake_counts)

    request = types.SimpleNamespace(
        app=types.SimpleNamespace(config={"RELEASE": "test", "ENVIRONMENT": "test"})
    )

    response = await npi_index_status(request)
    payload = json.loads(response.body)
    assert payload["product_count"] == 10
    assert payload["import_log_errors"] == 5
