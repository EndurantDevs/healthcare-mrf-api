# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Synthetic handler protocols; optional fallbacks never swallow identity refusals."""

from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sanic.exceptions import ServiceUnavailable

from api import provider_profile_network_read as profile
from api.endpoint import npi
from tests.provider_profile_snapshot_test_support import (
    stub_provider_profile_snapshot as stub_provider_profile_snapshot,
)
from tests.test_npi_api_extended import (
    _install_profile_transition_cache_dependencies,
    _npi_details_builder,
    _profile_cache_transition_records,
)


@asynccontextmanager
async def _optional_scope(_session):
    yield


@pytest.mark.parametrize("terminal", [False, True])
async def test_optional_directory_profile_preserves_identity_refusals(monkeypatch, terminal):
    failure = ServiceUnavailable("guard refused") if terminal else RuntimeError("optional unavailable")
    monkeypatch.setattr(profile, "provider_read_savepoint", _optional_scope)
    loader = AsyncMock(side_effect=failure)
    monkeypatch.setattr(npi, "_fetch_provider_directory_profile_map", loader)
    if terminal:
        with pytest.raises(ServiceUnavailable) as caught:
            await profile.read_detail_directory_profile(None, 1000000001, include_evidence=False)
        assert caught.value is failure
    else:
        assert await profile.read_detail_directory_profile(None, 1000000001, include_evidence=False) is None
    loader.assert_awaited_once()


@pytest.mark.parametrize("phase", ["cache", "names", "enrichment", "retry"])
async def test_detail_optional_refusal_never_returns_a_partial_response(monkeypatch, phase):
    build = AsyncMock(side_effect=_npi_details_builder([{"checksum": 3, "lat": 40.0, "long": -80.0}]))
    _install_profile_transition_cache_dependencies(
        monkeypatch, AsyncMock(return_value=_profile_cache_transition_records()[0]), build
    )
    failure = ServiceUnavailable("guard refused")
    if phase == "cache":
        monkeypatch.setattr(npi, "_provider_directory_address_overlay_serving_identity", AsyncMock(side_effect=failure))
    else:
        monkeypatch.setattr(npi, "_NPI_DETAIL_RESPONSE_CACHE_TTL_SECONDS", 0)
        if phase == "names":
            monkeypatch.setattr(npi, "_fetch_other_names", AsyncMock(side_effect=failure))
        else:
            enrichment_failure = RuntimeError("optional unavailable") if phase == "retry" else failure
            for name in ("_fetch_provider_enrichment_detail", "_fetch_provider_enrichment_summary_detail"):
                monkeypatch.setattr(npi, name, AsyncMock(side_effect=enrichment_failure))
            if phase == "retry":
                monkeypatch.setattr(npi, "_fetch_other_names", AsyncMock(side_effect=[[], failure]))
    request = SimpleNamespace(args={}, app=SimpleNamespace(config={"NPI_API_UPDATE_GEOCODE": False}))
    with pytest.raises(ServiceUnavailable) as caught:
        await npi.get_npi(request, "1518379601")
    assert caught.value is failure and not npi._NPI_DETAIL_RESPONSE_CACHE
    if phase == "cache":
        build.assert_not_awaited()


@pytest.mark.parametrize("fail_closed", [False, True])
async def test_location_status_identity_refusal_escapes_optional_mode(monkeypatch, fail_closed):
    failure = ServiceUnavailable("guard refused")
    query = AsyncMock(side_effect=failure)
    monkeypatch.setattr(npi, "_execute_stmt", query)
    monkeypatch.setattr(npi.db, "session", lambda: _optional_scope(None))
    with pytest.raises(ServiceUnavailable) as caught:
        await npi._fetch_location_status_by_record_id(
            ["provider_directory_fhir:practitioner_role:example:role"], fail_closed=fail_closed
        )
    assert caught.value is failure
    query.assert_awaited_once()
