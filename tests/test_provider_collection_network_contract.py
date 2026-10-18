# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Collection inputs preserve native options without accepting hidden authority."""

from copy import deepcopy
from dataclasses import FrozenInstanceError

import pytest

from api.provider_collection_network_contract import parse_managed_collection_query
from process.registry_network_catalog_read import RegistryNetworkCatalogLegacyBatchSelector

NPI = "0000000049"
KEY = "11111111-1111-4111-8111-111111111111"


def test_default_is_immutable_without_selected_ids_or_forced_totals():
    query_by_field = {
        "kind": "list",
        "native_query": {"name_like": ["one", "two"], "include_total": "false", "limit": "20000"},
    }
    original = deepcopy(query_by_field)
    query = parse_managed_collection_query(query_by_field)
    assert query_by_field == original
    assert dict(query.native_query) == {"name_like": ("one", "two"), "include_total": "false", "limit": "20000"}
    assert query.selector is None and query.network_generation is None and query.native_batch is None
    query_by_field["native_query"]["name_like"].clear()
    assert dict(query.native_query)["name_like"] == ("one", "two")
    with pytest.raises(FrozenInstanceError):
        query.kind = "geo"


@pytest.mark.parametrize(
    "kind,native",
    [
        ("list", {"count_only": "1", "format": "classification", "response_format": "all", "view": "sitemap"}),
        ("geo", {"lat": "50", "long": "14", "include_total": "false", "limit": "1000", "cursor": "value"}),
        ("list", {"limit": "20000", "page_size": "20000", "page": "1000000", "offset": "19999980000"}),
        ("list", {}),
        ("geo", {}),
        ("geo", {"count_only": "1"}),
        (
            "list",
            {
                "address_limit": "5",
                "address_offset": "10",
                "include_address_total": "false",
                "extra_info": "true",
                "address_grouping": "premise",
            },
        ),
    ],
)
def test_ordinary_native_options_limits_and_count_modes_are_preserved(kind, native):
    query = parse_managed_collection_query({"kind": kind, "native_query": native})
    assert dict(query.native_query) == native


@pytest.mark.parametrize(
    "namespace,values", [("canonical", [84, 42]), ("checksum_network", ["-42"]), ("legacy_fhir_uuid", [KEY])]
)
def test_selectors_and_retained_pin_are_separate_from_native_query(namespace, values):
    query = parse_managed_collection_query(
        {
            "kind": "geo",
            "native_query": {},
            "selector": {"namespace": namespace, "values": values},
            "network_generation": "9",
        }
    )
    expected = (
        (42, 84) if namespace == "canonical" else RegistryNetworkCatalogLegacyBatchSelector(namespace, tuple(values))
    )
    assert query.selector == expected and query.network_generation == 9


@pytest.mark.parametrize(
    "field",
    [
        "network_ids",
        "network_generation",
        "plan_network",
        "plan_network_checksum",
        "network_checksum",
        "checksum",
        "source",
        "path",
        "access_scope_sha256",
    ],
)
@pytest.mark.parametrize("kind", ["list", "geo", "batch"])
def test_hidden_selectors_source_routing_and_authority_are_forbidden(kind, field):
    query_by_field = {"kind": kind, "native_query": {field: "42"}}
    if kind == "batch":
        query_by_field["native_batch"] = {"npis": [NPI]}
    with pytest.raises(ValueError):
        parse_managed_collection_query(query_by_field)


@pytest.mark.parametrize(
    "query_by_field",
    [
        None,
        [],
        {},
        {"kind": "list"},
        {"native_query": {}},
        {"kind": "other", "native_query": {}},
        {"kind": True, "native_query": {}},
        {"kind": "batch", "native_query": {}},
        {"kind": "list", "native_query": {}, "native_batch": {}},
        {"kind": "geo", "native_query": {}, "native_batch": {}},
        {"kind": "list", "native_query": {}, "unknown": "value"},
        {"kind": "list", "native_query": []},
        {"kind": "list", "native_query": {"q": None}},
        {"kind": "list", "native_query": {"q": True}},
        {"kind": "list", "native_query": {"q": "\x00"}},
        {"kind": "list", "native_query": {"q": "\ud800"}},
        {"kind": "list", "native_query": {"q": "a" * 2049}},
        {"kind": "geo", "native_query": {"q": ["one"]}},
        {"kind": "list", "native_query": {"name_like": []}},
        {"kind": "list", "native_query": {"name_like": ["one"] * 17}},
        {"kind": "list", "native_query": {"name_like": ["é" * 2048] * 5}},
        {"kind": "list", "native_query": {}, "selector": {"namespace": "canonical", "values": [1, 1]}},
        {"kind": "list", "native_query": {}, "selector": {"namespace": "canonical", "values": [True]}},
        {"kind": "list", "native_query": {}, "network_generation": "01"},
        {"kind": "list", "native_query": {}, "network_generation": 9},
    ],
)
def test_closed_fields_types_utf8_and_byte_bounds_refuse(query_by_field):
    with pytest.raises((ValueError, TypeError)):
        parse_managed_collection_query(query_by_field)


def test_batch_reuses_native_defaults_and_freezes_normalized_options():
    query_by_field = {
        "kind": "batch",
        "native_query": {"view": "full"},
        "native_batch": {"npis": [NPI], "address_limit": "all"},
    }
    query = parse_managed_collection_query(query_by_field)
    assert dict(query.native_query) == {"view": "", "include_total": "true"}
    assert query.native_batch.document() == {
        "npis": [NPI],
        "address_limit": 0,
        "address_offset": 0,
        "include_sources": False,
        "include_evidence": False,
    }
    query_by_field["native_batch"]["npis"].clear()
    assert query.native_batch.npis == (NPI,)


@pytest.mark.parametrize(
    "changes",
    [
        {"npis": []},
        {"npis": [NPI, NPI]},
        {"npis": [True]},
        {"address_limit": True},
        {"address_limit": 1001},
        {"address_offset": -1},
        {"include_sources": "true"},
        {"unknown": 1},
    ],
)
def test_batch_keeps_native_type_limits_and_closed_fields(changes):
    with pytest.raises(ValueError):
        parse_managed_collection_query({"kind": "batch", "native_query": {}, "native_batch": {"npis": [NPI]} | changes})


_SERVICE_FIELDS = (
    "billing_code_modifier city classification code code_system cursor expand_codes include_allowed_amounts "
    "include_code_details include_debug include_details include_evidence include_legacy_fields include_providers "
    "include_sources include_subspecialties include_unverified_addresses lat limit long min_claims min_total_cost "
    "mode modifier modifiers negotiated_rate negotiated_rate_tolerance npi offset order order_by page place_of_service "
    "plan_release_id pos primary_only provider_sex_code q radius radius_miles rate rate_tolerance service_code specialty "
    "state taxonomy_classification taxonomy_code taxonomy_codes taxonomy_section taxonomy_specialization view year "
    "zip5 zip_radius_miles plan_id plan_external_id plan_id_type plan_market_type market_type source_key snapshot_id"
).split()


def _service_query(native_by_name, **changes):
    return {"kind": "by-service", "native_query": native_by_name} | changes


@pytest.mark.parametrize("field", _SERVICE_FIELDS)
def test_service_accepts_closed_ordinary_native_vocabulary(field):
    native_by_name = {"q": "needle", field: "2" if field in {"limit", "page", "offset"} else "native-value"}
    query = parse_managed_collection_query(_service_query(native_by_name))
    assert query.kind == "by-service" and dict(query.native_query) == native_by_name
    assert query.selector is None and query.native_batch is None and query.network_generation is None


@pytest.mark.parametrize(
    "native_by_name",
    [
        {"q": " Free text "},
        {"code": "native-code"},
        {"q": " ", "code": "native-code", "code_system": "Native-System"},
        {"q": "term", "lat": "not-normalized", "include_sources": "native-boolean", "mode": "native-mode"},
        {"code": "99213", "code_system": "CPT", "plan_id": "00-0000000", "source_key": "source", "snapshot_id": "snap"},
        {
            "q": "term",
            "view": "native-projection",
            "taxonomy_codes": "native,taxonomy",
            "cursor": "opaque_native_cursor",
        },
    ],
)
def test_service_preserves_handler_owned_domain_options_and_ordinary_code_inference(native_by_name):
    document = _service_query(native_by_name)
    original = deepcopy(document)
    query = parse_managed_collection_query(document)
    assert document == original and dict(query.native_query) == native_by_name
    native_by_name.clear()
    assert dict(query.native_query) == original["native_query"]
    with pytest.raises(FrozenInstanceError):
        query.kind = "list"


@pytest.mark.parametrize("native_by_name", [{}, {"q": ""}, {"code": " "}, {"q": "\t", "code": "\n"}])
def test_service_requires_nonempty_query_or_code(native_by_name):
    with pytest.raises(ValueError):
        parse_managed_collection_query(_service_query(native_by_name))


@pytest.mark.parametrize("value", ["0", "1", "25", "200", " 200 ", "0200", "+200", "", "null"])
def test_service_keeps_native_limit_defaults_and_string_spelling(value):
    native_by_name = {"q": "term", "limit": value}
    assert dict(parse_managed_collection_query(_service_query(native_by_name)).native_query) == native_by_name


@pytest.mark.parametrize("value", ["201", "+201", "0201", "-1", "1e2", "invalid"])
def test_service_refuses_oversized_or_invalid_native_limit(value):
    with pytest.raises(ValueError):
        parse_managed_collection_query(_service_query({"q": "term", "limit": value}))


@pytest.mark.parametrize(
    "paging,expected",
    [
        ({"limit": "2", "offset": "3"}, (2, 2, 3)),
        ({"limit": "2", "offset": "3", "page": "0"}, (2, 2, 3)),
        ({"limit": "2", "offset": "3", "page": ""}, (2, 2, 3)),
        ({"limit": "2", "offset": "2", "page": "2"}, (2, 2, 2)),
        ({"limit": "200", "page": "1000000"}, (1000000, 200, 199999800)),
        ({"offset": "1000001"}, (40001, 25, 1000001)),
    ],
)
def test_service_reuses_native_paging_including_nonaligned_offset(paging, expected):
    from api.endpoint.pagination import parse_pagination

    native_by_name = {"q": "term"} | paging
    query = parse_managed_collection_query(_service_query(native_by_name))
    preserved_by_name = dict(query.native_query)
    assert preserved_by_name == native_by_name
    normalized = parse_pagination(preserved_by_name, default_limit=25, max_limit=200)
    assert (normalized.page, normalized.limit, normalized.offset) == expected


@pytest.mark.parametrize(
    "paging",
    [{"limit": "2", "offset": "3", "page": "2"}, {"offset": "-1"}, {"page": "-1"}, {"offset": "bad"}, {"page": "bad"}],
)
def test_service_keeps_native_paging_consistency_refusals(paging):
    with pytest.raises(ValueError):
        parse_managed_collection_query(_service_query({"q": "term"} | paging))


@pytest.mark.parametrize("generation", ["1", "9223372036854775807"])
def test_service_accepts_canonical_positive_int64_generation_string(generation):
    query = parse_managed_collection_query(_service_query({"q": "term"}, network_generation=generation))
    assert query.network_generation == int(generation)
    assert "network_generation" not in dict(query.native_query)


@pytest.mark.parametrize(
    "generation", [0, 1, True, None, "0", "01", "+1", " 1", "1 ", "1.0", "1e1", "9223372036854775808"]
)
def test_service_refuses_noncanonical_or_out_of_range_generation(generation):
    with pytest.raises(ValueError):
        parse_managed_collection_query(_service_query({"q": "term"}, network_generation=generation))


@pytest.mark.parametrize(
    "extra",
    [
        {"selector": {"namespace": "canonical", "values": [42, 42]}},
        {"selector": {"namespace": "unknown", "values": ["42"]}},
        {"selector": None},
        {"native_batch": {"npis": [NPI]}},
        {"network_ids": [42]},
        {"managed_collection": {}},
        {"managed_detail": {}},
        {"network_binding": {}},
        {"billing_transport_context_sha256": "a" * 64},
        {"client_id": "sample-client"},
        {"policy_revision": "2"},
        {"target": {}},
    ],
)
def test_service_refuses_outer_selector_batch_billing_and_policy_mixes(extra):
    with pytest.raises(ValueError):
        parse_managed_collection_query(_service_query({"q": "term"}, **extra))


@pytest.mark.parametrize(
    "field",
    [
        "network_ids",
        "network_generation",
        "plan_network",
        "plan_network_checksum",
        "network_checksum",
        "checksum",
        "selector",
        "native_batch",
        "kind",
        "native_query",
        "network_binding",
        "managed_detail",
        "managed_collection",
        "client_id",
        "policy_revision",
        "access_scope_sha256",
        "excluded_network_ids",
        "excluded_company_ids",
        "excluded_group_ids",
        "organization_visibility_default",
        "contract_version",
        "billing_entity_ref",
        "billing_transport_context_sha256",
        "target",
        "path",
        "source",
        "include_medicare",
        "healthporta_plan_id",
        "page_size",
        "start",
        "include_total",
        "count_only",
        "unknown",
    ],
)
def test_service_refuses_hidden_native_authority_or_unlisted_options(field):
    with pytest.raises(ValueError):
        parse_managed_collection_query(_service_query({"q": "term", field: "42"}))


@pytest.mark.parametrize("value", [None, True, 2, ["term"], {"q": "term"}, "\x00", "\ud800", "x" * 2049])
def test_service_keeps_native_string_type_and_utf8_bounds(value):
    with pytest.raises((ValueError, TypeError)):
        parse_managed_collection_query(_service_query({"q": "term", "city": value}))


def test_service_keeps_aggregate_native_query_byte_bound():
    native_by_name = {"q": "term"} | {
        field: "é" * 2048 for field in ("city", "state", "specialty", "classification", "code")
    }
    with pytest.raises(ValueError):
        parse_managed_collection_query(_service_query(native_by_name))
