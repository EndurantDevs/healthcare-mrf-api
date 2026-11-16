# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import json
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import provider_batch
from api.endpoint import npi
from tests.test_npi_location_paging import ADDRESS_A, ADDRESS_B, SITE_A, _address


def _address_only_batch(monkeypatch, precision, source_count):
    identity = 1234567890
    address = _address(ADDRESS_A, "100 Example Avenue", source_id="nppes", premise_key=SITE_A)
    address.update(address_precision=precision, source_count=source_count)
    monkeypatch.setattr(npi, "current_network_address_scope", lambda: None)
    monkeypatch.setattr(npi, "_build_npi_identity_details_map", AsyncMock(return_value={}))
    monkeypatch.setattr(npi, "_fetch_npi_location_candidates_map", AsyncMock(return_value={identity: [address]}))
    monkeypatch.setattr(npi, "_fetch_provider_directory_address_overlay_map", AsyncMock(return_value={}))
    monkeypatch.setattr(npi, "_apply_location_statuses", AsyncMock())
    monkeypatch.setattr(
        npi,
        "_hydrate_npi_batch_addresses",
        AsyncMock(side_effect=lambda _npis, addresses, **_options: addresses),
    )
    monkeypatch.setattr(npi, "_fetch_other_names_map", AsyncMock(return_value={}))
    monkeypatch.setattr(npi, "_fetch_provider_enrichment_summary_map", AsyncMock(return_value={}))
    monkeypatch.setattr(
        npi, "_project_provider_network_metadata", AsyncMock(side_effect=lambda providers, **_options: providers)
    )
    return identity


async def _read_address_only_batch(identity, minimum, *, native=False, extra_info=False, **selectors):
    options_by_name = {} if minimum is None else {"min_address_sources": minimum}
    params = npi._normalize_npi_batch_request({"npis": [identity], **options_by_name})
    if not native:
        return await provider_batch.build_native_batch_payload(params)
    native_query_by_name = {
        "extra_info": str(extra_info).lower(),
        **{name: str(value) for name, value in options_by_name.items()},
        **selectors,
    }
    return await provider_batch.read_native_batch(
        SimpleNamespace(),
        params,
        native_args=provider_batch.parse_native_batch_query(native_query_by_name),
        session=None,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("native", [False, True])
@pytest.mark.parametrize("precision", ["street", "city_zip"])
@pytest.mark.parametrize("source_count", [2, 3])
@pytest.mark.parametrize("minimum", [None, 0, 3])
async def test_batch_source_membership_preserves_original_public_visibility(
    monkeypatch, native, precision, source_count, minimum
):
    identity = _address_only_batch(monkeypatch, precision, source_count)
    payload = await _read_address_only_batch(identity, minimum, native=native)
    is_visible = precision == "street"
    assert (payload["found"], payload["not_found"]) == (int(is_visible), int(not is_visible))
    assert payload["items"][0]["status"] == (200 if is_visible else 404)
    if is_visible:
        provider = payload["items"][0]["provider"]
        address_count = int(not minimum or source_count >= minimum)
        assert provider["npi"] == identity
        assert len(provider["address_list"]) == provider["address_pagination"]["total"] == address_count
    else:
        assert "provider" not in payload["items"][0]


@pytest.mark.asyncio
@pytest.mark.parametrize("source_count", [2, 3])
@pytest.mark.parametrize("minimum", [None, 0, 3])
async def test_native_extra_info_keeps_coarse_address_membership(monkeypatch, source_count, minimum):
    identity = _address_only_batch(monkeypatch, "city_zip", source_count)
    payload = await _read_address_only_batch(identity, minimum, native=True, extra_info=True)
    assert (payload["found"], payload["not_found"]) == (1, 0)
    assert payload["items"][0]["status"] == 200
    provider = payload["items"][0]["provider"]
    address_count = int(not minimum or source_count >= minimum)
    assert len(provider["address_list"]) == provider["address_pagination"]["total"] == address_count
    if address_count:
        assert provider["address_list"][0]["address_precision"] == "city_zip"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "precision,extra_info,minimum", [("street", False, 0), ("street", False, 3), ("city_zip", True, 3)]
)
@pytest.mark.parametrize("source_count", [2, 3])
@pytest.mark.parametrize(
    "selector,value,matching",
    [
        ("address_key", ADDRESS_A, True),
        ("address_key", ADDRESS_B, False),
        ("address_site_key", SITE_A, True),
        ("address_site_key", "10000000-0000-0000-0000-000000000002", False),
    ],
)
async def test_native_batch_selector_precedes_source_membership(
    monkeypatch, precision, extra_info, source_count, minimum, selector, value, matching
):
    identity = _address_only_batch(monkeypatch, precision, source_count)
    candidates = npi._fetch_npi_location_candidates_map

    async def search(_request, **options):
        assert minimum == 0
        assert options["native_args"].get(selector) == value
        assert options["native_args"].get("min_address_sources") == "0"
        return SimpleNamespace(
            status=200,
            body=json.dumps({"rows": [{"npi": identity}] if matching else [], "total": int(matching)}).encode(),
        )

    provider_search = AsyncMock(side_effect=search)
    monkeypatch.setattr(npi, "list_providers", provider_search)
    payload = await _read_address_only_batch(identity, minimum, native=True, extra_info=extra_info, **{selector: value})
    assert (payload["found"], payload["not_found"]) == (int(matching), int(not matching))
    assert payload["items"][0]["status"] == (200 if matching else 404)
    if matching:
        provider = payload["items"][0]["provider"]
        address_count = int(not minimum or source_count >= minimum)
        assert provider["npi"] == identity
        assert len(provider["address_list"]) == provider["address_pagination"]["total"] == address_count
    candidates.assert_awaited_once()
    assert provider_search.await_count == int(minimum == 0 and matching)


@pytest.mark.asyncio
@pytest.mark.parametrize("minimum", [None, 0, 3])
@pytest.mark.parametrize("selector", ["address_key", "address_site_key"])
async def test_native_batch_missing_selector_keeps_profile_identity_absent(monkeypatch, minimum, selector):
    identity = _address_only_batch(monkeypatch, "street", 3)
    monkeypatch.setattr(npi, "_build_npi_identity_details_map", AsyncMock(return_value={identity: {"npi": identity}}))
    provider_search = AsyncMock(return_value=SimpleNamespace(status=200, body=b'{"rows":[],"total":0}'))
    monkeypatch.setattr(npi, "list_providers", provider_search)
    payload = await _read_address_only_batch(identity, minimum, native=True, **{selector: ADDRESS_B})
    assert (payload["found"], payload["not_found"]) == (0, 1)
    assert payload["items"][0]["status"] == 404
    assert provider_search.await_count == int(not minimum)
    npi._fetch_npi_location_candidates_map.assert_awaited_once()


@pytest.mark.asyncio
async def test_native_batch_substantive_filter_keeps_selectors_and_minimum_in_provider_search(monkeypatch):
    identity = 1234567890
    provider_search = AsyncMock(
        return_value=SimpleNamespace(status=200, body=json.dumps({"rows": [{"npi": identity}], "total": 1}).encode())
    )
    monkeypatch.setattr(npi, "list_providers", provider_search)
    selected = await provider_batch._batch_eligible_npis(
        SimpleNamespace(),
        provider_batch.parse_native_batch_query({"q": "Example", "address_key": ADDRESS_A, "min_address_sources": "3"}),
        None,
        [identity],
        [identity],
        None,
    )
    assert selected == [identity]
    forwarded = provider_search.await_args.kwargs["native_args"]
    assert forwarded.get("q") == "Example"
    assert forwarded.get("address_key") == ADDRESS_A
    assert forwarded.get("min_address_sources") == "3"


@pytest.mark.parametrize("selector", ["address_key", "address_site_key"])
def test_batch_fallback_membership_respects_missing_selector(selector):
    identity = 1234567890
    address = _address(ADDRESS_A, "100 Example Avenue", source_id="nppes", premise_key=SITE_A)
    state = provider_batch._NativeBatchState({}, {identity: [address]})
    filtered = provider_batch._filter_batch_address_state(state, {selector: ADDRESS_B, "min_address_sources": "3"})
    assert filtered.addresses[identity] == []
    assert not filtered.address_members


@pytest.mark.asyncio
@pytest.mark.parametrize("extra_info", [False, True])
@pytest.mark.parametrize("has_qualifying_alternate", [False, True])
async def test_batch_uses_persisted_counts_before_merging_source_labels(
    monkeypatch, extra_info, has_qualifying_alternate
):
    identity = 1234567890
    first = _address(ADDRESS_A, "100 Example Avenue", source_id="nppes")
    first.update(source_count=2, independent_source_count=2, address_sources=["nppes", "mrf"])
    second = deepcopy(first)
    second.update(checksum=2, address_sources=["nppes", "provider_directory_fhir"])
    raw_addresses = [first, second]
    if has_qualifying_alternate:
        alternate = _address(ADDRESS_B, "200 Example Avenue", source_id="nppes")
        alternate.update(
            source_count=3,
            independent_source_count=3,
            address_sources=["nppes", "mrf", "provider_directory_fhir"],
        )
        raw_addresses.append(alternate)
    monkeypatch.setattr(npi, "_build_npi_identity_details_map", AsyncMock(return_value={identity: {"npi": identity}}))
    monkeypatch.setattr(
        npi, "_fetch_npi_location_candidates_map", AsyncMock(return_value={identity: deepcopy(raw_addresses)})
    )
    overlays = AsyncMock(side_effect=AssertionError("overlay counts must not qualify a stored-count filter"))
    monkeypatch.setattr(npi, "_fetch_provider_directory_address_overlay_map", overlays)
    monkeypatch.setattr(npi, "_apply_location_statuses", AsyncMock())
    options_by_name = {"min_address_sources": "3", "extra_info": str(extra_info).lower()}

    state = await provider_batch._prepare_native_batch([identity], options_by_name, None)

    assert [address_row["address_key"] for address_row in state.addresses[identity]] == (
        [ADDRESS_B] if has_qualifying_alternate else []
    )
    state = provider_batch._filter_batch_address_state(state, options_by_name)
    assert identity in state.address_members
    assert all(address_row["source_count"] == 3 for address_row in state.addresses[identity])
    assert [address_row["source_count"] for address_row in raw_addresses[:2]] == [2, 2]
    overlays.assert_not_awaited()
