# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Provider Directory cache-identity and profile-date edge coverage."""

from __future__ import annotations

import json
import types
from unittest.mock import AsyncMock

import pytest

from api.endpoint import npi as npi_module
from tests.test_npi_api_extended import (
    _install_profile_transition_cache_dependencies,
    _npi_details_builder,
    _profile_cache_transition_records,
)


class _ScalarResult:
    def __init__(self, value):
        self.value = value

    def scalar(self):
        return self.value


@pytest.mark.parametrize(
    ("raw_identity", "expected"),
    (
        (None, "absent"),
        (424242, "oid:424242"),
    ),
)
@pytest.mark.asyncio
async def test_address_overlay_identity_serializes_relation_state(
    monkeypatch,
    raw_identity,
    expected,
):
    monkeypatch.setattr(
        npi_module,
        "_execute_stmt",
        AsyncMock(return_value=_ScalarResult(raw_identity)),
    )

    assert (
        await npi_module._provider_directory_address_overlay_serving_identity()
        == expected
    )


@pytest.mark.parametrize("raw_identity", (True, "not-an-oid", 0))
@pytest.mark.asyncio
async def test_address_overlay_identity_rejects_invalid_relation_oids(
    monkeypatch,
    raw_identity,
):
    monkeypatch.setattr(
        npi_module,
        "_execute_stmt",
        AsyncMock(return_value=_ScalarResult(raw_identity)),
    )

    with pytest.raises(
        RuntimeError,
        match="address_overlay_identity_invalid",
    ):
        await npi_module._provider_directory_address_overlay_serving_identity()


def test_profile_as_of_rejects_non_date_scalar():
    with pytest.raises(TypeError, match="ISO date string"):
        npi_module._serialize_provider_directory_profile_as_of(20260730)


@pytest.mark.asyncio
async def test_npi_detail_cache_bypasses_overlay_identity_failure(monkeypatch):
    profile_fetch = AsyncMock(
        return_value=_profile_cache_transition_records()[0]
    )
    build_details = AsyncMock(
        side_effect=_npi_details_builder(
            [{"checksum": 3, "lat": 40.0, "long": -80.0}]
        )
    )
    _install_profile_transition_cache_dependencies(
        monkeypatch,
        profile_fetch,
        build_details,
    )
    monkeypatch.setattr(
        npi_module,
        "_provider_directory_address_overlay_serving_identity",
        AsyncMock(side_effect=RuntimeError("transient identity failure")),
    )
    request = types.SimpleNamespace(
        args={},
        app=types.SimpleNamespace(config={"NPI_API_UPDATE_GEOCODE": False}),
    )

    first_response = await npi_module.get_npi(request, "1518379601")
    second_response = await npi_module.get_npi(request, "1518379601")

    assert json.loads(first_response.body)["npi"] == 1518379601
    assert second_response.body == first_response.body
    assert build_details.await_count == 2
    assert npi_module._NPI_DETAIL_RESPONSE_CACHE == {}


@pytest.mark.asyncio
async def test_profile_only_npi_detail_populates_identity_bound_cache(
    monkeypatch,
):
    profile_fetch = AsyncMock(
        return_value=_profile_cache_transition_records()[0]
    )
    build_details = AsyncMock(return_value={})
    _install_profile_transition_cache_dependencies(
        monkeypatch,
        profile_fetch,
        build_details,
    )
    request = types.SimpleNamespace(
        args={"sync_geocode": "false"},
        app=types.SimpleNamespace(config={"NPI_API_UPDATE_GEOCODE": False}),
    )

    first_response = await npi_module.get_npi(request, "1518379601")
    second_response = await npi_module.get_npi(request, "1518379601")

    assert second_response.body == first_response.body
    assert build_details.await_count == 1
    assert len(npi_module._NPI_DETAIL_RESPONSE_CACHE) == 1
