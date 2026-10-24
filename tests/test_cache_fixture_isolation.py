# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Cache isolation does not require loading an unrelated endpoint."""

import sys
from types import SimpleNamespace

import pytest

from tests import conftest


@pytest.mark.parametrize("state", ("absent", "existing", "late", "replaced", "removed"))
def test_detail_cache_fixture_clears_only_loaded_modules(monkeypatch, state):
    name = "api.endpoint.npi"
    original = SimpleNamespace(_NPI_DETAIL_RESPONSE_CACHE={"stale": "value"})
    replacement = SimpleNamespace(_NPI_DETAIL_RESPONSE_CACHE={"new": "value"})
    if state in {"existing", "replaced", "removed"}:
        monkeypatch.setitem(sys.modules, name, original)
    else:
        monkeypatch.delitem(sys.modules, name, raising=False)

    fixture = conftest.clear_npi_detail_response_cache.__wrapped__()
    next(fixture)
    if state in {"absent", "late"}:
        assert name not in sys.modules
    else:
        assert original._NPI_DETAIL_RESPONSE_CACHE == {}
        original._NPI_DETAIL_RESPONSE_CACHE["during"] = "value"
    if state in {"late", "replaced"}:
        monkeypatch.setitem(sys.modules, name, replacement)
    elif state == "removed":
        monkeypatch.delitem(sys.modules, name)

    with pytest.raises(StopIteration):
        next(fixture)
    if state in {"existing", "replaced", "removed"}:
        assert original._NPI_DETAIL_RESPONSE_CACHE == {}
    if state in {"late", "replaced"}:
        assert replacement._NPI_DETAIL_RESPONSE_CACHE == {}
