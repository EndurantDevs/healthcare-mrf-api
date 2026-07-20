# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import aiohttp
import pytest
from sanic.exceptions import SanicException
from sqlalchemy.exc import SQLAlchemyError

from api import uhc_provider_file_catalog as catalog_api
from process import uhc_provider_file_catalog as catalog_process


def test_catalog_routes_register_exact_read_and_refresh_methods():
    blueprint = Mock()

    catalog_api.register_uhc_provider_file_catalog_routes(blueprint)

    assert blueprint.add_route.call_count == 2
    read_call, refresh_call = blueprint.add_route.call_args_list
    assert read_call.args == (
        catalog_api.control_uhc_provider_directory_files,
        "/provider-directory/sources/uhc/files",
    )
    assert read_call.kwargs == {}
    assert refresh_call.args == (
        catalog_api.control_refresh_uhc_provider_directory_files,
        "/provider-directory/sources/uhc/files/refresh",
    )
    assert refresh_call.kwargs == {"methods": {"POST"}}


@pytest.mark.asyncio
async def test_refresh_route_authenticates_and_returns_catalog(monkeypatch):
    require_auth = Mock()
    refresh_listing = AsyncMock(return_value={"catalog_observed": True})
    monkeypatch.setattr(catalog_api, "require_control_auth", require_auth)
    monkeypatch.setattr(
        catalog_api,
        "refresh_uhc_provider_file_catalog_listing",
        refresh_listing,
    )
    request = SimpleNamespace()

    api_response = await catalog_api.control_refresh_uhc_provider_directory_files(
        request
    )

    assert api_response.status == 200
    assert json.loads(api_response.body) == {"catalog_observed": True}
    require_auth.assert_called_once_with(request)
    refresh_listing.assert_awaited_once_with()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "transport_error",
    [asyncio.TimeoutError(), aiohttp.ClientConnectionError("DNS unavailable")],
)
async def test_authenticated_refresh_route_maps_transport_failures_to_stable_503(
    monkeypatch,
    transport_error,
):
    class _ClientSession:
        def __init__(self, **_options):
            self.options_by_name = dict(_options)

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

    require_auth = Mock()
    monkeypatch.setattr(catalog_api, "require_control_auth", require_auth)
    monkeypatch.setattr(catalog_process.aiohttp, "ClientSession", _ClientSession)
    monkeypatch.setattr(
        catalog_process,
        "fetch_catalog_snapshot",
        AsyncMock(side_effect=transport_error),
    )
    request = SimpleNamespace()

    with pytest.raises(SanicException) as unavailable:
        await catalog_api.control_refresh_uhc_provider_directory_files(request)

    assert unavailable.value.status_code == 503
    assert str(unavailable.value) == "UHC catalog refresh is unavailable"
    require_auth.assert_called_once_with(request)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "database_error",
    [SQLAlchemyError("database unavailable"), ConnectionRefusedError("refused")],
)
async def test_catalog_wrappers_map_database_failures_to_stable_503(
    monkeypatch,
    database_error,
):
    monkeypatch.setattr(
        catalog_api,
        "uhc_provider_file_catalog",
        AsyncMock(side_effect=database_error),
    )
    with pytest.raises(SanicException) as read_unavailable:
        await catalog_api.read_uhc_provider_file_catalog({})
    assert read_unavailable.value.status_code == 503
    assert str(read_unavailable.value) == "UHC catalog proof is unavailable"

    monkeypatch.setattr(
        catalog_api,
        "refresh_uhc_provider_file_catalog",
        AsyncMock(side_effect=database_error),
    )
    with pytest.raises(SanicException) as refresh_unavailable:
        await catalog_api.refresh_uhc_provider_file_catalog_listing()
    assert refresh_unavailable.value.status_code == 503
    assert str(refresh_unavailable.value) == "UHC catalog refresh is unavailable"
