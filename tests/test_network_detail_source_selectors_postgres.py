# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Source-filtered native detail retains only identities matching the other office selectors."""

from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from api import network_provider_read as readers
from api.network_address_scope import NetworkAddressReadScope
from tests.test_network_provider_membership_page_postgres import membership_page_db as membership_page_db


@pytest.mark.asyncio
@pytest.mark.parametrize("location_lookup", [False, True])
@pytest.mark.parametrize("minimum", [None, 0, 3])
@pytest.mark.parametrize(
    "office_filters,visible",
    [
        ({"location_id": str(UUID(int=200))}, True),
        ({"location_id": str(UUID(int=999))}, False),
        ({"location_id": str(UUID(int=1))}, False),
        ({"location_id": str(UUID(int=101))}, False),
        ({"lat": 34, "long": -118, "radius_miles": 1}, True),
        ({"lat": 35, "long": -118, "radius_miles": 1}, False),
    ],
    ids=["selected-alias", "missing-office", "unselected-alias", "other-network", "inside-radius", "outside-radius"],
)
async def test_native_detail_preserves_other_office_selectors(
    monkeypatch, membership_page_db, location_lookup, minimum, office_filters, visible
):
    case = membership_page_db
    object.__setattr__(case.manifest, "_supports_location_lookup", location_lookup)
    await case.connection.execute(f"UPDATE {case.namespace}.base_address SET source_count=2")
    monkeypatch.setattr(readers, "resolve_network_serving_manifest", AsyncMock(return_value=case.manifest))
    selection = readers.NetworkOfficeSelection(
        **office_filters, **({"min_address_sources": minimum} if minimum is not None else {})
    )
    async with case.connection.transaction(isolation="repeatable_read", readonly=True):
        operation = readers.read_network_provider_detail(
            case.connection,
            NetworkAddressReadScope(case.manifest, (42,), None),
            provider_system="provider_directory",
            provider_id="provider-a",
            office_filters=selection,
        )
        if not visible:
            with pytest.raises(readers.NetworkProviderNotFound):
                await operation
            return
        result = await operation
    assert result["generation_id"] == case.manifest.generation_id
    provider = result["provider"]
    assert provider["provider_id"] == "provider-a"
    assert provider["display_name"] == "Provider provider-a"
    assert [office["location_id"] for office in provider["locations"]] == ([] if minimum else [str(UUID(int=200))])


@pytest.mark.asyncio
async def test_location_lookup_refuses_conflicting_binding_for_selected_office(monkeypatch, membership_page_db):
    case = membership_page_db
    object.__setattr__(case.manifest, "_supports_location_lookup", True)
    monkeypatch.setattr(readers, "resolve_network_serving_manifest", AsyncMock(return_value=case.manifest))
    await case.connection.execute(
        f"""INSERT INTO {case.namespace}.provider_location_binding
        VALUES ('provider_directory','other-provider',$1,$2,'provider_directory','other-provider')""",
        UUID(int=500),
        f"{100:064x}",
    )
    async with case.connection.transaction(isolation="repeatable_read", readonly=True):
        with pytest.raises(readers.NetworkProviderReadUnavailable):
            await readers.read_network_provider_detail(
                case.connection,
                NetworkAddressReadScope(case.manifest, (42,), None),
                provider_system="provider_directory",
                provider_id="provider-a",
            )
