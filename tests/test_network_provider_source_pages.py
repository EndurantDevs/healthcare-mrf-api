# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Qualified network pages retain exact offices and independent projection parameters."""

import re
from uuid import UUID

import pytest

from api import network_provider_read as readers
from api.network_address_scope import NetworkAddressReadScope
from tests.test_network_raw_provider_api_identity import _manifest, _office_spies


@pytest.mark.asyncio
@pytest.mark.parametrize("trusted", [False, True])
@pytest.mark.parametrize("compact", [False, True])
async def test_threshold_page_uses_only_the_resolved_capability(monkeypatch, trusted, compact):
    manifest = _manifest(compact=compact, artifacts=compact)
    object.__setattr__(manifest, "_supports_membership_pages", trusted)
    connection, query_read, events = _office_spies(monkeypatch, manifest)

    await readers.read_network_provider_page(
        connection,
        NetworkAddressReadScope(manifest, (11,), None),
        office_filters=readers.NetworkOfficeSelection(min_address_sources=3),
    )

    query, *arguments = connection.fetchrow.await_args.args
    assert query.startswith("WITH page_addresses") is trusted
    assert query.startswith("WITH selected_addresses") is not trusted
    assert max(map(int, re.findall(r"\$(\d+)", query))) == len(arguments)
    assert set(map(int, re.findall(r"\$(\d+)", query))) == set(range(1, len(arguments) + 1))
    query_read.assert_awaited_once()
    connection.acquire.assert_not_called()
    assert events == ["resolve", "data", "decode"]
    if trusted:
        discovery, hydration = query.split("selected_memberships AS MATERIALIZED", 1)
        assert "qualifying_addresses AS MATERIALIZED" in discovery
        assert "member.network_id=ANY(ARRAY[11]::integer[])" in discovery
        assert "address.source_count >= 3" in discovery
        assert "qualifying_memberships" not in query
        assert "JOIN ONLY" in hydration and "network_membership member USING(provider_system,provider_id)" in hydration
        assert "JOIN qualifying_addresses qualified_address" in hydration
        assert "CROSS JOIN LATERAL" not in hydration and "LEFT JOIN LATERAL" in hydration


@pytest.mark.asyncio
async def test_compact_threshold_projections_have_independent_bind_offsets(monkeypatch):
    manifest = _manifest(compact=True, artifacts=True)
    object.__setattr__(manifest, "_supports_membership_pages", True)
    connection, _, _ = _office_spies(monkeypatch, manifest)
    compiled_projections = []
    native_addresses = readers._provider_addresses

    def record_projection(
        actual, networks, *, location_lookup=False, first_parameter=13, visible_membership_sql=None, columns=None
    ):
        query, arguments = native_addresses(
            actual,
            networks,
            location_lookup=location_lookup,
            first_parameter=first_parameter,
            visible_membership_sql=visible_membership_sql,
            columns=columns,
        )
        compiled_projections.append(
            (query, arguments, location_lookup, first_parameter, networks, visible_membership_sql, columns)
        )
        return query, arguments

    monkeypatch.setattr(readers, "_provider_addresses", record_projection)
    await readers.read_network_provider_page(
        connection,
        NetworkAddressReadScope(manifest, (11, 22), None),
        office_filters=readers.NetworkOfficeSelection(min_address_sources=3),
    )

    query, *arguments = connection.fetchrow.await_args.args
    page, qualification = compiled_projections
    assert page[2:] == (True, 13, (11, 22), None, readers._PAGE_ADDRESS_COLUMNS)
    assert qualification[2:] == (False, 13 + len(page[1]), None, "TRUE", readers._QUALIFYING_ADDRESS_COLUMNS)
    assert tuple(arguments[12:]) == page[1] + qualification[1]
    assert query.count(page[0]) == query.count(qualification[0]) == 1
    assert query.count("page_addresses") == 2
    assert qualification[1] == ()
    assert "bucket_members OFFSET 0" in page[0]
    assert "bucket_members OFFSET 0" not in qualification[0]


@pytest.mark.asyncio
@pytest.mark.parametrize("filter_kind", ["location", "geo"])
async def test_threshold_with_other_office_filters_retains_full_query(monkeypatch, filter_kind):
    manifest = _manifest(compact=True, artifacts=True)
    object.__setattr__(manifest, "_supports_membership_pages", True)
    connection, _, _ = _office_spies(monkeypatch, manifest)
    selection = (
        readers.NetworkOfficeSelection(location_id=str(UUID(int=1)), min_address_sources=3)
        if filter_kind == "location"
        else readers.NetworkOfficeSelection(lat=10, long=20, radius_miles=1, min_address_sources=3)
    )

    await readers.read_network_provider_page(
        connection, NetworkAddressReadScope(manifest, (11,), None), office_filters=selection
    )

    query = connection.fetchrow.await_args.args[0]
    assert query.startswith("WITH selected_addresses")
    assert query.index("integrity AS") < query.index("selected.source_count >= 3")


@pytest.mark.parametrize("trusted", [False, True])
def test_zero_threshold_keeps_original_network_page_sql(trusted):
    manifest = _manifest(compact=True, artifacts=True)
    object.__setattr__(manifest, "_supports_membership_pages", trusted)
    addresses, expected_arguments = readers._provider_addresses(manifest, (11,), location_lookup=trusted)
    expected = (readers._MEMBERSHIP_PROVIDERS_SQL if trusted else readers._PROVIDERS_SQL).format(
        namespace='"' + manifest.schema_name + '"', addresses=addresses, address_membership="TRUE"
    )

    query, arguments = readers._provider_query(manifest, (11,), (None, None), (None,) * 4, 0)

    assert query == expected and arguments == expected_arguments


@pytest.mark.parametrize("networks", [(11,), (11, 22), (2147483647,)])
@pytest.mark.parametrize("minimum", [3, 2147483647])
def test_positive_discovery_plan_keeps_threshold_and_selected_networks_visible(networks, minimum):
    manifest = _manifest(compact=True, artifacts=True)
    object.__setattr__(manifest, "_supports_membership_pages", True)

    query, arguments = readers._provider_query(manifest, networks, (None, None), (None,) * 4, minimum)

    discovery, page = query.split("page_identities AS MATERIALIZED", 1)
    expected_ids = "ARRAY[" + ",".join(map(str, networks)) + "]::integer[]"
    assert "qualifying_addresses AS MATERIALIZED" in discovery
    assert f"address.source_count >= {minimum}" in discovery
    assert f"member.network_id=ANY({expected_ids})" in discovery
    assert "member.network_id=ANY($1::text::integer[])" in page
    assert "SELECT address.location_key,address.entity_type,address.entity_id\n" in discovery
    assert set(map(int, re.findall(r"\$(\d+)", query))) == set(range(1, 13 + len(arguments)))


@pytest.mark.parametrize(
    "networks",
    [
        (),
        (0,),
        (True,),
        ("11",),
        ("42,88",),
        (2147483648,),
        (11, 11),
        (22, 11),
        tuple(range(1, 102)),
        ("11); SELECT 1",),
    ],
)
@pytest.mark.parametrize("compact", [False, True])
def test_positive_discovery_rejects_network_values_outside_the_selector_contract(networks, compact):
    manifest = _manifest(compact=compact, artifacts=compact)
    object.__setattr__(manifest, "_supports_membership_pages", True)

    with pytest.raises(ValueError, match="canonical_network_ids_invalid|Canonical provider selection is invalid"):
        readers._provider_query(manifest, networks, (None, None), (None,) * 4, 3)
