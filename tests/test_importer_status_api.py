import json
import types

import pytest

from api.endpoint.importer import last_import_stats


@pytest.mark.asyncio
async def test_importer_status_endpoint(monkeypatch):
    async def fake_stats():
        return {
            "plans_count": 42,
            "import_log_errors": 3,
            "import_date": "2024-01-01T00:00:00Z",
            "issuers_number": 7,
            "issuers_by_state": {"CA": 2},
            "plans_by_state": {"CA": 5},
        }

    monkeypatch.setattr("api.endpoint.importer._collect_import_stats", fake_stats)

    request = types.SimpleNamespace(
        app=types.SimpleNamespace(config={"RELEASE": "test", "ENVIRONMENT": "test"})
    )

    response = await last_import_stats(request)
    payload = json.loads(response.body)
    assert payload["plans_count"] == 42
    assert payload["import_log_errors"] == 3
