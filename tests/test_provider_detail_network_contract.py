# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Managed detail input boundaries preserve ordinary native provider semantics."""

from copy import deepcopy
from dataclasses import FrozenInstanceError

import pytest

from api.provider_detail_network_contract import parse_managed_detail_query
from process.registry_network_catalog_read import RegistryNetworkCatalogLegacyBatchSelector

NPI = "0000000049"
KEY = "11111111-1111-4111-8111-111111111111"


@pytest.mark.parametrize("npi", [NPI, "1000000491", "9999999999"])
def test_generic_native_npi_and_default_detail_window_are_preserved(npi):
    query = parse_managed_detail_query({"npi": npi})
    assert query.npi == npi and query.include == ()
    assert dict(query.native_args) == {"address_limit": "5"}
    assert query.selector is None and query.network_generation is None


def test_native_options_are_normalized_without_mutating_input_or_conveying_authority():
    query_by_field = {
        "npi": NPI,
        "include": ["taxonomy", "plans"],
        "include_sources": "false",
        "include_evidence": "true",
        "debug": "true",
        "extra_info": "false",
        "address_key": KEY,
        "address_site_key": KEY,
        "address_grouping": "flat",
        "address_limit": "1000",
        "address_offset": "1000000",
        "selector": {"namespace": "canonical", "values": [2147483647, 42]},
        "network_generation": "9223372036854775807",
    }
    original = deepcopy(query_by_field)
    query = parse_managed_detail_query(query_by_field)
    assert query_by_field == original
    assert query.include == ("plans", "taxonomy")
    assert dict(query.native_args) == {
        name: value for name, value in query_by_field.items() if name.startswith("address_")
    } | {"include_sources": "false", "include_evidence": "true", "debug": "true", "extra_info": "false"}
    assert query.selector == (42, 2147483647) and query.network_generation == 9223372036854775807
    query_by_field["selector"]["values"].clear()
    query_by_field["include"].clear()
    assert query.selector == (42, 2147483647) and query.include == ("plans", "taxonomy")
    with pytest.raises(FrozenInstanceError):
        query.npi = "1000000491"


@pytest.mark.parametrize("limit", ["all", "0"])
def test_existing_all_locations_alias_is_normalized_without_a_window(limit):
    query = parse_managed_detail_query({"npi": NPI, "address_limit": limit})
    assert dict(query.native_args) == {"address_limit": "all"}


@pytest.mark.parametrize(
    "options",
    [
        {"address_grouping": "premise"},
        {"address_grouping": "premise", "address_limit": "1", "address_offset": "1000000"},
    ],
)
def test_premise_defaults_and_bounded_windows_remain_available(options):
    query = parse_managed_detail_query({"npi": NPI} | options)
    assert dict(query.native_args) == {"address_limit": "5"} | options


@pytest.mark.parametrize(
    "options",
    [
        {"address_limit": "all", "address_offset": "0"},
        {"address_limit": "0", "address_offset": "1"},
        {"address_grouping": "premise", "address_key": KEY},
        {"address_grouping": "premise", "address_site_key": KEY},
        {"address_grouping": "premise", "address_limit": "6"},
        {"address_grouping": "premise", "address_limit": "all"},
        {"address_grouping": "premise", "address_limit": "0"},
    ],
)
def test_conflicting_location_controls_are_rejected(options):
    with pytest.raises(ValueError):
        parse_managed_detail_query({"npi": NPI} | options)


@pytest.mark.parametrize(
    "changes",
    [
        {"npi": None},
        {"npi": 1000000491},
        {"npi": "100000049"},
        {"npi": "１００００００４９１"},
        {"npi": " 1000000491"},
        {"include": None},
        {"include": "plans"},
        {"include": ["plans", "plans"]},
        {"include": ["custom_import"]},
        {"include": ["TAXONOMY"]},
        {"include": [None]},
        {"include_sources": True},
        {"include_evidence": None},
        {"debug": "1"},
        {"extra_info": "TRUE"},
        {"address_grouping": None},
        {"address_grouping": "PREMISE"},
        {"address_grouping": ""},
        {"address_key": None},
        {"address_key": KEY.replace("1", "A")},
        {"address_key": KEY.replace("-", "")},
        {"address_site_key": "invalid"},
        {"address_limit": 5},
        {"address_limit": None},
        {"address_limit": "-1"},
        {"address_limit": "1001"},
        {"address_limit": "05"},
        {"address_limit": "ALL"},
        {"address_limit": "+5"},
        {"address_offset": "1000001"},
        {"address_offset": "-1"},
        {"address_offset": "00"},
        {"address_offset": True},
        {"network_generation": "0"},
        {"network_generation": "01"},
        {"network_generation": 1},
        {"network_generation": None},
        {"network_generation": "9223372036854775808"},
        {"network_generation": "١"},
        {"selector": None},
        {"include_profile": "false"},
        {"network_ids": "42"},
        {"access_scope_sha256": "a" * 64},
        {"unknown": "value"},
    ],
)
def test_closed_query_rejects_noncanonical_or_untrusted_wire(changes):
    with pytest.raises(ValueError):
        parse_managed_detail_query({"npi": NPI} | changes)


@pytest.mark.parametrize("document", [None, [], {}, [("npi", NPI)]])
def test_query_requires_one_closed_object_and_required_identity(document):
    with pytest.raises(ValueError):
        parse_managed_detail_query(document)


@pytest.mark.parametrize(
    "selector",
    [
        {},
        {"namespace": "canonical"},
        {"namespace": "canonical", "values": [], "source_id": "synthetic"},
        {"namespace": None, "values": [1]},
        {"namespace": "canonical", "values": ()},
        {"namespace": "canonical", "values": []},
        {"namespace": "canonical", "values": [1, 1]},
        {"namespace": "canonical", "values": [True]},
        {"namespace": "canonical", "values": ["1"]},
        {"namespace": "canonical", "values": [0]},
        {"namespace": "canonical", "values": [2147483648]},
        {"namespace": "canonical", "values": list(range(1, 102))},
        {"namespace": "checksum_network", "values": ["-0"]},
        {"namespace": "checksum_network", "values": ["01"]},
        {"namespace": "checksum_network", "values": ["2147483648"]},
        {"namespace": "checksum_network", "values": [1]},
        {"namespace": "checksum_network", "values": ["1", "1"]},
        {"namespace": "checksum_network", "values": [str(value) for value in range(51)]},
        {"namespace": "legacy_fhir_uuid", "values": ["00000000-0000-0000-0000-000000000000"]},
        {"namespace": "legacy_fhir_uuid", "values": [KEY.replace("-", "")]},
        {"namespace": "other", "values": ["1"]},
    ],
)
def test_selector_is_closed_typed_bounded_and_unique(selector):
    with pytest.raises(ValueError):
        parse_managed_detail_query({"npi": NPI, "selector": selector})


def test_selector_and_generation_are_independent_from_default_discovery():
    default = parse_managed_detail_query({"npi": NPI, "network_generation": "9"})
    selected = parse_managed_detail_query(
        {"npi": NPI, "selector": {"namespace": "canonical", "values": list(range(100, 0, -1))}}
    )
    assert default.selector is None and default.network_generation == 9
    assert selected.selector == tuple(range(1, 101)) and selected.network_generation is None


@pytest.mark.parametrize(
    "namespace,values",
    [
        ("checksum_network", ["-2147483648", "0", "2147483647"]),
        ("checksum_network", [str(value) for value in range(50)]),
        ("legacy_fhir_uuid", [KEY]),
    ],
)
def test_legacy_selector_reuses_the_existing_typed_batch_contract(namespace, values):
    query = parse_managed_detail_query({"npi": NPI, "selector": {"namespace": namespace, "values": values}})
    assert query.selector == RegistryNetworkCatalogLegacyBatchSelector(namespace, tuple(values))
