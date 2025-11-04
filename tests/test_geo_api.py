import json
import types

import pytest

from api.endpoint import geo as geo_module
from api.endpoint.geo import get_geo as get_geo_zip


class FakeSelect:
    def __init__(self, row=None):
        self._row = row
        self.gino = self

    def select_from(self, *_args, **_kwargs):
        return self

    def where(self, *_args, **_kwargs):
        return self

    async def first(self):
        return self._row


@pytest.mark.asyncio
async def test_geo_zip_not_found(monkeypatch):
    monkeypatch.setattr(geo_module.db, "select", lambda *_args, **_kwargs: FakeSelect())
    request = types.SimpleNamespace()
    response = await get_geo_zip(request, "99999")
    payload = json.loads(response.body)
    assert payload["error"] == "Not found"
