# See LICENSE.

from __future__ import annotations

import collections
import datetime as dt
import gzip
import json
import math
from dataclasses import replace
from decimal import Decimal
from pathlib import Path
from urllib.parse import parse_qs

import httpx
import pytest

from scripts.validation import ptg2_v3_source_api_audit as audit


NPIS = (1111111111, 2222222222, 3333333333)
DEFAULT_RAW_SHA256 = "b" * 64
DEFAULT_SOURCE_SET = audit.source_set_evidence([DEFAULT_RAW_SHA256])


def _source_identity_payload(
    *,
    raw_sha256=DEFAULT_RAW_SHA256,
    source_artifact_key=0,
    source_key=None,
    trace_id=None,
    trace_set_hash=None,
):
    trace_id = trace_id or f"source-file-{source_artifact_key}"
    trace_set_hash = trace_set_hash or audit.sha256_text(
        f"trace-set:{source_artifact_key}"
    )
    identity_by_field = {
        "source_artifact_key": source_artifact_key,
        "source_type": "in_network",
        "identity_kind": "logical_json_sha256_v1",
        "identity_sha256": "a" * 64,
        "raw_container_sha256": raw_sha256,
        "logical_json_sha256": "a" * 64,
        "logical_hash_deferred": False,
        "source_trace_set_hash": trace_set_hash,
        "source_trace": [{"source_file_version_id": trace_id}],
    }
    if source_key is not None:
        identity_by_field["source_key"] = source_key
    return identity_by_field


def _source_identity(**kwargs):
    identity_by_field = _source_identity_payload(**kwargs)
    return audit.extract_source_identity(identity_by_field, field_prefix="test")


def _negotiated_price(**overrides):
    price_by_field = {
        "negotiated_type": "negotiated",
        "negotiated_rate": 123.0,
        "expiration_date": "2027-12-31",
        "service_code": ["1", "01"],
        "billing_class": "professional",
        "setting": "office",
        "billing_code_modifier": ["tc", " TC "],
        "additional_information": "exact text",
    }
    price_by_field.update(overrides)
    return price_by_field


def _source_document(*, references_before: bool, duplicate_price: bool = True):
    provider_reference_by_field = {
        "provider_group_id": 7,
        "provider_groups": [{"npi": list(NPIS[:2])}],
    }
    referenced_prices = [_negotiated_price()]
    if duplicate_price:
        referenced_prices.append(_negotiated_price())
    in_network_entries = [
        {
            "negotiated_rates": [
                {
                    "negotiated_prices": referenced_prices,
                    "provider_references": [7],
                }
            ],
            "billing_code": "99213",
            "negotiation_arrangement": "ffs",
            "billing_code_type": "cpt",
        },
        {
            "billing_code_type": "HCPCS",
            "billing_code": "A1234",
            "negotiation_arrangement": "bundle",
            "negotiated_rates": [
                {
                    "provider_groups": [{"npi": [NPIS[2]]}],
                    "negotiated_prices": [
                        _negotiated_price(
                            negotiated_rate=45.67,
                            service_code=["22"],
                        )
                    ],
                }
            ],
        },
    ]
    source_document_by_field = {"reporting_entity_type": "issuer"}
    if references_before:
        source_document_by_field["provider_references"] = [
            provider_reference_by_field
        ]
        source_document_by_field["in_network"] = in_network_entries
    else:
        source_document_by_field["in_network"] = in_network_entries
        source_document_by_field["provider_references"] = [
            provider_reference_by_field
        ]
    return source_document_by_field


def _write_source_fixture(
    path: Path,
    *,
    references_before: bool,
    duplicate_price: bool = True,
    gzip_encoded: bool = True,
    source_document=None,
) -> Path:
    source_document = source_document or _source_document(
        references_before=references_before,
        duplicate_price=duplicate_price,
    )
    if gzip_encoded:
        with gzip.open(path, "wt", encoding="utf-8") as source_file:
            json.dump(source_document, source_file)
    else:
        path.write_text(json.dumps(source_document), encoding="utf-8")
    return path


def _open_source_index(tmp_path: Path, source_path: Path, *, target: int = 32):
    specs = audit.source_specs([source_path])
    index = audit.SourceIndex(
        tmp_path / "source.sqlite3",
        seed="test-seed",
        source_occurrence_sample_target=target,
        sqlite_cache_mb=1,
    )
    index.index(specs)
    return index


@pytest.mark.parametrize("references_before", [True, False])
@pytest.mark.parametrize("gzip_encoded", [True, False])
def test_source_index_supports_reference_order_and_magic_detected_encoding(
    tmp_path,
    references_before,
    gzip_encoded,
):
    misleading_suffix = ".json" if gzip_encoded else ".json.gz"
    source_path = _write_source_fixture(
        tmp_path / f"source{misleading_suffix}",
        references_before=references_before,
        gzip_encoded=gzip_encoded,
    )
    specs = audit.source_specs([source_path])
    assert specs[0].encoding == ("gzip" if gzip_encoded else "json")

    with _open_source_index(tmp_path, source_path) as index:
        expected = index.expected_tuples(audit.QueryKey("CPT", "99213", NPIS[0]))
        inline = index.expected_tuples(audit.QueryKey("HCPCS", "A1234", NPIS[2]))
        occurrences = index.source_occurrences(5)

        assert sum(expected.values()) == 2
        assert len(expected) == 1
        assert sum(inline.values()) == 1
        assert len(occurrences) == 5
        assert len({occurrence.occurrence_id for occurrence in occurrences}) == 5
        referenced_tuple = json.loads(next(iter(expected)))
        inline_tuple = json.loads(next(iter(inline)))
        assert referenced_tuple["negotiation_arrangement"] == "FFS"
        assert referenced_tuple["negotiated_rate"] == "123"
        assert referenced_tuple["service_code"] == ["01"]
        assert referenced_tuple["billing_code_modifier"] == ["TC"]
        assert inline_tuple["negotiation_arrangement"] == "BUNDLE"
        assert inline_tuple["service_code"] == ["22"]
        assert index.metrics["unresolved_provider_references"] == 0
        assert index.metrics["eligible_rates"] == 2


def test_source_index_rejects_wrong_json_field_types_from_coverage(tmp_path):
    payload = _source_document(references_before=True, duplicate_price=False)
    payload["in_network"][0]["negotiated_rates"][0]["negotiated_prices"][0][
        "negotiated_rate"
    ] = "123"
    source_path = _write_source_fixture(
        tmp_path / "source.json",
        references_before=True,
        gzip_encoded=False,
        source_document=payload,
    )

    with _open_source_index(tmp_path, source_path) as index:
        assert index.metrics["invalid_field_types"] == 1
        assert index.metrics["invalid_prices"] == 1
        assert sum(
            index.expected_tuples(audit.QueryKey("CPT", "99213", NPIS[0])).values()
        ) == 0


@pytest.mark.parametrize(
    ("field_name", "malformed_value"),
    [
        ("negotiated_type", {}),
        ("expiration_date", []),
        ("billing_class", {"value": "professional"}),
        ("setting", ["office"]),
        ("additional_information", {}),
        ("negotiated_rate", [123]),
        ("service_code", {}),
        ("service_code", [[]]),
        ("billing_code_modifier", {"value": "TC"}),
        ("billing_code_modifier", [["TC"]]),
    ],
)
def test_source_index_rejects_malformed_price_container_shapes(
    tmp_path,
    field_name,
    malformed_value,
):
    payload = _source_document(references_before=True, duplicate_price=False)
    payload["in_network"][0]["negotiated_rates"][0]["negotiated_prices"][0][
        field_name
    ] = malformed_value
    source_path = _write_source_fixture(
        tmp_path / f"malformed-{field_name}.json",
        references_before=True,
        gzip_encoded=False,
        source_document=payload,
    )

    with _open_source_index(tmp_path, source_path) as index:
        assert index.metrics["invalid_field_types"] >= 1
        assert index.metrics["invalid_prices"] == 1
        assert not index.expected_tuples(
            audit.QueryKey("CPT", "99213", NPIS[0])
        )


def test_source_index_strictly_validates_procedure_and_network_metadata_types(
    tmp_path,
):
    payload = _source_document(references_before=True, duplicate_price=False)
    payload["provider_references"][0]["network_name"] = "must-be-an-array"
    payload["in_network"][0]["name"] = 123
    payload["in_network"][1]["negotiated_rates"][0]["network_names"] = {
        "invalid": "object"
    }
    source_path = _write_source_fixture(
        tmp_path / "invalid-metadata-types.json",
        references_before=True,
        gzip_encoded=False,
        source_document=payload,
    )

    with _open_source_index(tmp_path, source_path) as index:
        assert index.metrics["invalid_field_types"] == 3
        assert not index.expected_tuples(
            audit.QueryKey("CPT", "99213", NPIS[0])
        )


def test_source_index_rejects_duplicate_provider_reference_ids(tmp_path):
    payload = _source_document(references_before=True, duplicate_price=False)
    payload["provider_references"].append(
        json.loads(json.dumps(payload["provider_references"][0]))
    )
    source_path = _write_source_fixture(
        tmp_path / "source.json",
        references_before=True,
        gzip_encoded=False,
        source_document=payload,
    )

    with pytest.raises(audit.SourceFormatError, match="provider_reference_id_is_duplicated"):
        _open_source_index(tmp_path, source_path)


@pytest.mark.parametrize(
    "provider_group_prefix",
    [
        "provider_references.item.provider_groups.item",
        "in_network.item.negotiated_rates.item.provider_groups.item",
    ],
)
def test_provider_tin_event_detection_covers_object_and_fields(
    provider_group_prefix,
):
    assert audit.SourceIndex._is_provider_tin_event(
        f"{provider_group_prefix}.tin",
        provider_group_prefix,
        "start_map",
    )
    assert audit.SourceIndex._is_provider_tin_event(
        f"{provider_group_prefix}.tin.type",
        provider_group_prefix,
        "string",
    )
    assert audit.SourceIndex._is_provider_tin_event(
        f"{provider_group_prefix}.tin.value",
        provider_group_prefix,
        "string",
    )
    assert not audit.SourceIndex._is_provider_tin_event(
        f"{provider_group_prefix}.npi.item",
        provider_group_prefix,
        "number",
    )


def test_source_index_counts_referenced_tin_only_group(tmp_path):
    payload = _source_document(references_before=True, duplicate_price=False)
    payload["provider_references"][0]["provider_groups"] = [
        {"npi": [], "tin": {"type": "ein", "value": "000000001"}}
    ]
    source_path = _write_source_fixture(
        tmp_path / "source.json",
        references_before=True,
        gzip_encoded=False,
        source_document=payload,
    )

    with _open_source_index(tmp_path, source_path) as index:
        # start_map, type, and value events still produce one reference marker.
        assert index.metrics["provider_reference_tin_markers"] == 1
        assert index.metrics["invalid_provider_npis"] == 0
        assert index.metrics["tin_only_rates"] == 1
        assert index.metrics["rates_with_tin_markers"] == 1
        assert index.metrics["rates_with_provider_structure"] == 2
        assert not index.expected_tuples(audit.QueryKey("CPT", "99213", NPIS[0]))


def test_source_index_counts_referenced_mixed_tin_and_valid_npi_group(tmp_path):
    payload = _source_document(references_before=True, duplicate_price=False)
    payload["provider_references"][0]["provider_groups"] = [
        {
            "npi": [NPIS[0]],
            "tin": {"type": "ein", "value": "000000001"},
        }
    ]
    source_path = _write_source_fixture(
        tmp_path / "mixed-tin-npi.json",
        references_before=True,
        gzip_encoded=False,
        source_document=payload,
    )

    with _open_source_index(tmp_path, source_path) as index:
        assert index.metrics["provider_reference_tin_markers"] == 1
        assert index.metrics["rates_with_tin_markers"] == 1
        assert index.metrics.get("tin_only_rates", 0) == 0
        assert index.expected_tuples(
            audit.QueryKey("CPT", "99213", NPIS[0])
        )


def test_source_index_counts_inline_tin_only_group(tmp_path):
    payload = _source_document(references_before=True, duplicate_price=False)
    payload["in_network"][1]["negotiated_rates"][0]["provider_groups"] = [
        {"npi": [], "tin": {"type": "ein", "value": "000000001"}}
    ]
    source_path = _write_source_fixture(
        tmp_path / "inline-tin-only.json",
        references_before=True,
        gzip_encoded=False,
        source_document=payload,
    )

    with _open_source_index(tmp_path, source_path) as index:
        # The TIN object and both fields are idempotent at rate scope.
        assert index.metrics["inline_rate_tin_markers"] == 1
        assert index.metrics["rates_with_tin_markers"] == 1
        assert index.metrics["tin_only_rates"] == 1
        assert not index.expected_tuples(
            audit.QueryKey("HCPCS", "A1234", NPIS[2])
        )


def test_source_index_counts_inline_mixed_tin_and_valid_npi_group(tmp_path):
    payload = _source_document(references_before=True, duplicate_price=False)
    payload["in_network"][1]["negotiated_rates"][0]["provider_groups"] = [
        {
            "npi": [NPIS[2]],
            "tin": {"type": "ein", "value": "000000001"},
        }
    ]
    source_path = _write_source_fixture(
        tmp_path / "inline-mixed-tin-npi.json",
        references_before=True,
        gzip_encoded=False,
        source_document=payload,
    )

    with _open_source_index(tmp_path, source_path) as index:
        assert index.metrics["inline_rate_tin_markers"] == 1
        assert index.metrics["rates_with_tin_markers"] == 1
        assert index.metrics.get("tin_only_rates", 0) == 0
        assert index.expected_tuples(
            audit.QueryKey("HCPCS", "A1234", NPIS[2])
        )


def test_source_index_does_not_publish_or_quarantine_zero_npi(tmp_path):
    payload = _source_document(references_before=True, duplicate_price=False)
    payload["provider_references"][0]["provider_groups"] = [{"npi": [0]}]
    source_path = _write_source_fixture(
        tmp_path / "zero-npi.json",
        references_before=True,
        gzip_encoded=False,
        source_document=payload,
    )

    with _open_source_index(tmp_path, source_path) as index:
        assert index.metrics.get("invalid_provider_npis", 0) == 0
        assert index.metrics.get("provider_reference_tin_markers", 0) == 0
        assert index.metrics.get("rates_with_tin_markers", 0) == 0
        quarantine = index.source_report()["provider_identifier_quarantine"]
        assert quarantine["occurrence_count"] == 0
        assert quarantine["entries"] == []


@pytest.mark.parametrize("provider_form", ["referenced", "inline"])
@pytest.mark.parametrize(
    "npi_values",
    [[0, NPIS[0]], [NPIS[0], 0], [0, 0]],
    ids=["zero-first", "zero-last", "zero-repeated"],
)
def test_source_index_rejects_non_singleton_zero_npi_marker(
    tmp_path,
    provider_form,
    npi_values,
):
    payload = _source_document(references_before=True, duplicate_price=False)
    provider_groups = [
        {
            "npi": npi_values,
            "tin": {"type": "ein", "value": "000000001"},
        }
    ]
    if provider_form == "referenced":
        payload["provider_references"][0]["provider_groups"] = provider_groups
    else:
        payload["in_network"][1]["negotiated_rates"][0][
            "provider_groups"
        ] = provider_groups
    source_path = _write_source_fixture(
        tmp_path / f"{provider_form}-invalid-zero.json",
        references_before=True,
        gzip_encoded=False,
        source_document=payload,
    )

    with pytest.raises(audit.SourceFormatError, match="zero_npi_marker_must_be_singleton"):
        _open_source_index(tmp_path, source_path)


def test_source_index_quarantines_malformed_integer_but_keeps_valid_npi(tmp_path):
    payload = _source_document(references_before=True, duplicate_price=False)
    payload["provider_references"][0]["provider_groups"] = [
        {
            "npi": [NPIS[0], 123456789],
            "tin": {"type": "ein", "value": "000000001"},
        }
    ]
    source_path = _write_source_fixture(
        tmp_path / "mixed-valid-malformed-npi.json",
        references_before=True,
        gzip_encoded=False,
        source_document=payload,
    )

    with _open_source_index(tmp_path, source_path) as index:
        assert index.metrics["invalid_provider_npis"] == 1
        assert index.expected_tuples(audit.QueryKey("CPT", "99213", NPIS[0]))
        quarantine = index.source_report()["provider_identifier_quarantine"]
        assert quarantine["occurrence_count"] == 1
        assert quarantine["entries"] == [
            {"value": "123456789", "occurrence_count": 1}
        ]


def test_source_index_does_not_mask_complementary_incomplete_rates(tmp_path):
    payload = _source_document(references_before=True, duplicate_price=False)
    first_rate = payload["in_network"][0]["negotiated_rates"][0]
    second_rate = payload["in_network"][1]["negotiated_rates"][0]
    first_rate["negotiated_prices"] = []
    second_rate["provider_groups"] = []
    source_path = _write_source_fixture(
        tmp_path / "source.json",
        references_before=True,
        gzip_encoded=False,
        source_document=payload,
    )

    with _open_source_index(tmp_path, source_path) as index:
        assert index.metrics["negotiated_rates"] == 2
        assert index.metrics["rates_with_valid_prices"] == 1
        assert index.metrics["rates_with_provider_structure"] == 1
        assert index.metrics["eligible_rates"] == 0


def test_same_rate_from_two_raw_source_containers_remains_two_occurrences(tmp_path):
    first_source_document = _source_document(
        references_before=True,
        duplicate_price=False,
    )
    second_source_document = _source_document(
        references_before=True,
        duplicate_price=False,
    )
    first_source_document["reporting_entity_name"] = "first"
    second_source_document["reporting_entity_name"] = "second"
    source_paths = [
        _write_source_fixture(
            tmp_path / "first.json",
            references_before=True,
            duplicate_price=False,
            gzip_encoded=False,
            source_document=first_source_document,
        ),
        _write_source_fixture(
            tmp_path / "second.json",
            references_before=True,
            duplicate_price=False,
            gzip_encoded=False,
            source_document=second_source_document,
        ),
    ]
    specs = audit.source_specs(source_paths)
    with audit.SourceIndex(
        tmp_path / "two-sources.sqlite3",
        seed="test-seed",
        source_occurrence_sample_target=16,
        sqlite_cache_mb=1,
    ) as index:
        index.index(specs)
        expected = index.expected_tuples(audit.QueryKey("CPT", "99213", NPIS[0]))

    assert len(expected) == 2
    assert sorted(expected.values()) == [1, 1]
    assert {
        json.loads(key)["raw_container_sha256"] for key in expected
    } == {spec.content_sha256 for spec in specs}


def test_rate_network_names_keep_otherwise_equal_occurrences_distinct(tmp_path):
    source_document = _source_document(references_before=True, duplicate_price=False)
    in_network_item = source_document["in_network"][0]
    first_rate = in_network_item["negotiated_rates"][0]
    second_rate = json.loads(json.dumps(first_rate))
    first_rate["network_name"] = ["Network One"]
    second_rate["network_names"] = "Network Two"
    in_network_item["negotiated_rates"] = [first_rate, second_rate]
    source_path = _write_source_fixture(
        tmp_path / "network-distinct.json",
        references_before=True,
        duplicate_price=False,
        gzip_encoded=False,
        source_document=source_document,
    )

    with _open_source_index(tmp_path, source_path) as index:
        query = audit.QueryKey("CPT", "99213", NPIS[0])
        expected = index.expected_tuples(query)
        sampled_network_names = {
            tuple(json.loads(occurrence.tuple_key)["network_names"])
            for occurrence in index.source_occurrences(10)
            if occurrence.query == query
        }

    assert sum(expected.values()) == 2
    assert len(expected) == 2
    assert {tuple(json.loads(tuple_key)["network_names"]) for tuple_key in expected} == {
        ("Network One",),
        ("Network Two",),
    }
    assert sampled_network_names == {("Network One",), ("Network Two",)}


def test_referenced_network_names_are_unioned_with_rate_names(tmp_path):
    payload = _source_document(references_before=True, duplicate_price=False)
    payload["provider_references"][0]["network_name"] = [
        " Reference Two ",
        "Reference One",
    ]
    payload["in_network"][0]["negotiated_rates"][0]["network_names"] = [
        "Rate Network",
        "Reference One",
    ]
    source_path = _write_source_fixture(
        tmp_path / "referenced-networks.json",
        references_before=True,
        duplicate_price=False,
        gzip_encoded=False,
        source_document=payload,
    )

    with _open_source_index(tmp_path, source_path) as index:
        expected = index.expected_tuples(audit.QueryKey("CPT", "99213", NPIS[0]))

    tuple_payload = json.loads(next(iter(expected)))
    assert tuple_payload["network_names"] == [
        "Rate Network",
        "Reference One",
        "Reference Two",
    ]


def test_procedure_source_metadata_mismatch_fails_exact_comparison(tmp_path):
    source_document = _source_document(references_before=True, duplicate_price=False)
    source_document["in_network"][0].update(
        {
            "billing_code_type_version": "2026",
            "name": "Source Procedure Label",
            "description": "Source Procedure Detail",
        }
    )
    source_path = _write_source_fixture(
        tmp_path / "procedure-metadata.json",
        references_before=True,
        duplicate_price=False,
        gzip_encoded=False,
        source_document=source_document,
    )
    query = audit.QueryKey("CPT", "99213", NPIS[0])

    with _open_source_index(tmp_path, source_path) as index:
        expected = index.expected_tuples(query)

    tuple_key = next(iter(expected))
    tuple_payload = json.loads(tuple_key)
    api_item = _api_item_for_tuple(
        _tuple_from_key(tuple_key),
        raw_sha256=tuple_payload["raw_container_sha256"],
    )
    api_item["procedure_description"] = "Different Procedure Detail"
    extracted = audit.extract_api_tuples([api_item])
    comparison = audit.compare_tuple_counters(query, expected, extracted.counter)

    assert comparison.failure_counts == {"altered": 1}
    assert comparison.examples[0]["mismatched_fields"] == ["description"]


def test_source_visible_names_are_redacted_from_failure_examples():
    query = audit.QueryKey("CPT", "99213", NPIS[0])
    source_marker = "private-source-label-should-not-leak"
    network_marker = "private-network-label-should-not-leak"
    canonical_tuple = audit.CanonicalTuple.from_parts(
        query,
        "FFS",
        _negotiated_price(negotiated_rate=100, service_code=["11"]),
        billing_code_type_version="2026",
        name=source_marker,
        description="private-description-should-not-leak",
        network_names=[network_marker],
    )
    altered_tuple = replace(canonical_tuple, name="different-private-label")

    comparison = audit.compare_tuple_counters(
        query,
        collections.Counter({canonical_tuple.stable_key: 1}),
        collections.Counter({altered_tuple.stable_key: 1}),
    )
    serialized_examples = json.dumps(comparison.examples)

    assert comparison.failure_counts == {"altered": 1}
    assert source_marker not in serialized_examples
    assert network_marker not in serialized_examples


def test_canonicalization_is_exact_and_rejects_invalid_dates():
    assert audit.canonical_decimal("+001.2300e1") == "12.3"
    assert audit.canonical_decimal("-0.000") == "0"
    assert audit.canonical_code_system(" ms-drg ") == "MS_DRG"
    assert audit.canonical_catalog_code("MS_DRG", "7") == "007"
    assert audit.canonical_npi("1111111111.0") == NPIS[0]
    assert audit.canonical_price_payload(_negotiated_price())["setting"] == "office"
    assert audit.canonical_price_payload(_negotiated_price())["service_code"] == [
        "01"
    ]
    with pytest.raises(ValueError, match="invalid_iso_date"):
        audit.canonical_date("2027-02-30")
    with pytest.raises(ValueError, match="non_finite_decimal"):
        audit.canonical_decimal("NaN")


def test_source_metadata_preserves_trimmed_empty_distinct_from_null():
    query = audit.QueryKey("CPT", "99213", NPIS[0])
    null_metadata = audit.CanonicalTuple.from_parts(
        query,
        "FFS",
        _negotiated_price(),
        name=None,
    )
    empty_metadata = audit.CanonicalTuple.from_parts(
        query,
        "FFS",
        _negotiated_price(),
        name="   ",
    )

    assert null_metadata.name is None
    assert empty_metadata.name == ""
    assert null_metadata.stable_key != empty_metadata.stable_key


def test_bottom_k_sampling_is_seeded_order_independent_and_bounded():
    first = audit.BottomKSampler(7, seed="same", namespace="test")
    second = audit.BottomKSampler(7, seed="same", namespace="test")
    different = audit.BottomKSampler(7, seed="different", namespace="test")
    values = [f"value-{index}" for index in range(100)]
    for value in values:
        first.offer(value, value)
    for value in reversed(values):
        second.offer(value, value)
        different.offer(value, value)

    assert first.values() == second.values()
    assert len(first.values()) == 7
    assert first.values() != different.values()


def test_random_api_request_plan_is_deterministic_and_samples_with_replacement():
    query_a = audit.QueryKey("CPT", "99213", NPIS[0])
    query_b = audit.QueryKey("HCPCS", "A1234", NPIS[1])
    occurrences = [
        audit.SourceOccurrence("a" * 64, query_a, "tuple-a"),
        audit.SourceOccurrence("b" * 64, query_b, "tuple-b"),
    ]

    first = audit.build_random_api_requests(
        occurrences, count=20, max_limit=7, seed="same-seed"
    )
    second = audit.build_random_api_requests(
        occurrences, count=20, max_limit=7, seed="same-seed"
    )

    assert first == second
    assert len(first) == 20
    assert len({request.query.stable_key for request in first}) <= len(occurrences)
    assert all(1 <= request.page_size <= 7 for request in first)
    assert sum(request.phase == "cold" for request in first) == len(
        {request.query.stable_key for request in first}
    )


def _canonical_tuple(
    query,
    *,
    rate="100",
    setting="office",
    arrangement="FFS",
    **overrides,
):
    price = _negotiated_price(
        negotiated_rate=rate,
        service_code=["11"],
        billing_code_modifier=[],
        setting=setting,
        additional_information=None,
    )
    price.update(overrides)
    return audit.CanonicalTuple.from_parts(query, arrangement, price)


@pytest.mark.parametrize(("source_count", "api_count"), [(2, 1), (1, 2)])
def test_counter_comparison_detects_duplicate_loss_and_injection(
    source_count,
    api_count,
):
    query = audit.QueryKey("CPT", "99213", NPIS[0])
    canonical_tuple = _canonical_tuple(query)
    comparison = audit.compare_tuple_counters(
        query,
        collections.Counter({canonical_tuple.stable_key: source_count}),
        collections.Counter({canonical_tuple.stable_key: api_count}),
    )

    assert comparison.failure_counts == {"duplicate_count": 1}
    assert comparison.examples[0]["source_count"] == source_count
    assert comparison.examples[0]["api_count"] == api_count


def test_counter_comparison_classifies_altered_missing_and_fake_tuples():
    query = audit.QueryKey("CPT", "99213", NPIS[0])
    altered_source = _canonical_tuple(query, rate="200")
    missing = _canonical_tuple(query, rate="300")
    altered_api = _canonical_tuple(query, rate="201")
    fake = _canonical_tuple(
        query,
        rate="999",
        setting="facility",
        arrangement="BUNDLE",
        negotiated_type="derived",
        billing_class="institutional",
    )
    comparison = audit.compare_tuple_counters(
        query,
        collections.Counter(
            {altered_source.stable_key: 1, missing.stable_key: 1}
        ),
        collections.Counter({altered_api.stable_key: 1, fake.stable_key: 1}),
    )

    assert comparison.failure_counts == {
        "altered": 1,
        "missing": 1,
        "extra_fake": 1,
    }
    assert all("exact text" not in json.dumps(example) for example in comparison.examples)


def _tuple_from_key(tuple_key: str) -> audit.CanonicalTuple:
    payload = json.loads(tuple_key)
    return audit.CanonicalTuple(
        code_system=payload["code_system"],
        code=payload["code"],
        npi=payload["npi"],
        negotiation_arrangement=payload["negotiation_arrangement"],
        billing_code_type_version=payload["billing_code_type_version"],
        name=payload["name"],
        description=payload["description"],
        network_names=tuple(payload["network_names"]),
        negotiated_type=payload["negotiated_type"],
        negotiated_rate=payload["negotiated_rate"],
        expiration_date=payload["expiration_date"],
        service_code=tuple(payload["service_code"]),
        billing_class=payload["billing_class"],
        setting=payload["setting"],
        billing_code_modifier=tuple(payload["billing_code_modifier"]),
        additional_information=payload["additional_information"],
    )


def _api_item_for_tuple(
    canonical_tuple: audit.CanonicalTuple,
    *,
    raw_sha256=DEFAULT_RAW_SHA256,
    source_artifact_key=0,
    source_key=None,
):
    price = canonical_tuple.payload.copy()
    for field in (
        "code_system",
        "code",
        "npi",
        "negotiation_arrangement",
        "billing_code_type_version",
        "name",
        "description",
        "network_names",
    ):
        price.pop(field)
    price["negotiated_rate"] = Decimal(price["negotiated_rate"])
    return {
        "reported_code_system": canonical_tuple.code_system,
        "reported_code": canonical_tuple.code,
        "npi": canonical_tuple.npi,
        "negotiation_arrangement": canonical_tuple.negotiation_arrangement,
        "billing_code_type_version": canonical_tuple.billing_code_type_version,
        "procedure_name": canonical_tuple.name,
        "procedure_description": canonical_tuple.description,
        "network_names": list(canonical_tuple.network_names),
        "provider_name": "forbidden-marker",
        "prices": [price],
        **_source_identity_payload(
            raw_sha256=raw_sha256,
            source_artifact_key=source_artifact_key,
            source_key=source_key,
        ),
    }


def test_api_tuple_extraction_requires_arrangement_and_strict_types():
    query = audit.QueryKey("CPT", "99213", NPIS[0])
    canonical_tuple = _canonical_tuple(query)
    missing_arrangement = _api_item_for_tuple(canonical_tuple)
    missing_arrangement.pop("negotiation_arrangement")
    string_npi = _api_item_for_tuple(canonical_tuple)
    string_npi["npi"] = str(NPIS[0])
    scalar_service_code = _api_item_for_tuple(canonical_tuple)
    scalar_service_code["prices"][0]["service_code"] = "11"
    string_rate = _api_item_for_tuple(canonical_tuple)
    string_rate["prices"][0]["negotiated_rate"] = "100"
    scalar_network_names = _api_item_for_tuple(canonical_tuple)
    scalar_network_names["network_names"] = "not-an-array"
    numeric_procedure_version = _api_item_for_tuple(canonical_tuple)
    numeric_procedure_version["billing_code_type_version"] = 2026

    for item in (
        missing_arrangement,
        string_npi,
        scalar_service_code,
        string_rate,
        scalar_network_names,
        numeric_procedure_version,
    ):
        extracted = audit.extract_api_tuples([item])
        assert not extracted.counter
        assert extracted.schema_errors


def test_wrong_standard_api_source_attribution_fails_exact_comparison():
    query = audit.QueryKey("CPT", "99213", NPIS[0])
    canonical_tuple = _canonical_tuple(query)
    expected = collections.Counter(
        {audit.canonical_occurrence_key(canonical_tuple, "1" * 64): 1}
    )
    item = _api_item_for_tuple(canonical_tuple, raw_sha256="2" * 64)

    extracted = audit.extract_api_tuples([item])
    comparison = audit.compare_tuple_counters(query, expected, extracted.counter)

    assert comparison.failure_counts == {"altered": 1}
    assert comparison.examples[0]["mismatched_fields"] == ["raw_container_sha256"]
    assert "1" * 64 not in json.dumps(comparison.examples)
    assert "2" * 64 not in json.dumps(comparison.examples)


def test_snapshot_wide_source_trace_union_is_rejected():
    query = audit.QueryKey("CPT", "99213", NPIS[0])
    canonical_tuple = _canonical_tuple(query)
    shared_source_traces = [
        {"source_file_version_id": "source-one"},
        {"source_file_version_id": "source-two"},
    ]
    first = _api_item_for_tuple(
        canonical_tuple,
        raw_sha256="1" * 64,
        source_artifact_key=0,
    )
    second = _api_item_for_tuple(
        canonical_tuple,
        raw_sha256="2" * 64,
        source_artifact_key=1,
    )
    first["source_trace"] = shared_source_traces
    second["source_trace"] = shared_source_traces
    second["source_trace_set_hash"] = "9" * 64

    extracted = audit.extract_api_tuples(
        [first, second],
        registry=audit.SourceIdentityRegistry(),
    )

    assert sum(extracted.counter.values()) == 1
    assert extracted.schema_errors == (
        "source_trace_union_mapped_to_multiple_source_artifact_keys",
    )


def test_source_artifact_key_and_raw_container_mapping_is_one_to_one():
    registry = audit.SourceIdentityRegistry()
    registry.register(
        _source_identity(raw_sha256="1" * 64, source_artifact_key=0)
    )

    with pytest.raises(
        audit.ApiSchemaError, match="source_artifact_key_mapping_changed"
    ):
        registry.register(
            _source_identity(raw_sha256="2" * 64, source_artifact_key=0)
        )
    with pytest.raises(audit.ApiSchemaError, match="raw_container_mapped"):
        registry.register(
            _source_identity(raw_sha256="1" * 64, source_artifact_key=1)
        )


def test_dense_artifact_key_is_required_and_logical_source_key_is_optional():
    without_logical_key = audit.extract_source_identity(
        _source_identity_payload(source_artifact_key=7),
        field_prefix="test",
    )
    with_logical_key = audit.extract_source_identity(
        _source_identity_payload(
            source_artifact_key=7,
            source_key="logical-source",
        ),
        field_prefix="test",
    )

    assert without_logical_key.source_artifact_key == 7
    assert without_logical_key.source_key is None
    assert with_logical_key.source_artifact_key == 7
    assert with_logical_key.source_key == "logical-source"

    missing_artifact_key = _source_identity_payload(source_key="logical-source")
    missing_artifact_key.pop("source_artifact_key")
    with pytest.raises(
        audit.ApiSchemaError, match="source_artifact_key_must_be_integer"
    ):
        audit.extract_source_identity(missing_artifact_key, field_prefix="test")

    numeric_logical_key = _source_identity_payload()
    numeric_logical_key["source_key"] = 7
    with pytest.raises(audit.ApiSchemaError, match="source_key_must_be_string"):
        audit.extract_source_identity(numeric_logical_key, field_prefix="test")


def test_optional_logical_source_key_does_not_replace_dense_mapping_key():
    registry = audit.SourceIdentityRegistry()
    registry.register(_source_identity(source_artifact_key=3))
    registry.register(
        _source_identity(source_artifact_key=3, source_key="logical-source")
    )

    with pytest.raises(
        audit.ApiSchemaError,
        match="logical_source_key_changed_within_report",
    ):
        registry.register(
            _source_identity(source_artifact_key=3, source_key="different-logical-source")
        )


@pytest.mark.parametrize(
    "changes",
    [
        {"source_type": "allowed_amount"},
        {"identity_sha256": "A" * 64},
        {"logical_hash_deferred": True},
        {"source_trace": []},
        {"source_trace": [{"url": "https://example.invalid/source"}]},
    ],
)
def test_source_identity_requires_exact_in_network_evidence(changes):
    payload = _source_identity_payload()
    payload.update(changes)

    with pytest.raises(audit.ApiSchemaError):
        audit.extract_source_identity(payload, field_prefix="test")


def _audit_config(**overrides):
    config_by_field = {
        "profile": "diagnostic",
        "api_base_url": "https://api.example.invalid",
        "api_path": audit.DEFAULT_API_PATH,
        "api_audit_path": audit.DEFAULT_API_AUDIT_PATH,
        "plan_id": "plan-value",
        "snapshot_id": "snapshot-value",
        "plan_market_type": "group",
        "source_key": None,
        "seed": "test-seed",
        "source_occurrence_samples": 3,
        "api_occurrence_samples": 3,
        "negative_samples": 2,
        "random_api_calls": 6,
        "random_api_max_limit": 5,
        "min_source_occurrence_checks": 1,
        "min_api_occurrence_checks": 1,
        "min_negative_checks": 1,
        "min_random_api_calls": 1,
        "min_resolved_rate_fraction": 1.0,
        "max_unresolved_provider_references": 0,
        "max_invalid_prices": 0,
        "max_invalid_npis": 0,
        "max_invalid_field_types": 0,
        "page_size": 100,
        "max_pages": 10,
        "api_audit_page_size": 100,
        "api_audit_max_pages": 10,
        "warm_repeats": 1,
        "concurrency": 1,
        "failure_example_limit": 20,
        "verify_tls": True,
        "max_first_page_first_observation_p95_ms": 40.0,
        "max_logical_query_p95_ms": 1_000.0,
    }
    config_by_field.update(overrides)
    return audit.AuditConfig(**config_by_field)


def _release_audit_config():
    return _audit_config(
        profile="release",
        api_path=audit.DEFAULT_CANDIDATE_API_PATH,
        validated_candidate=True,
        seed=audit.DEFAULT_SEED,
        source_occurrence_samples=2_500,
        api_occurrence_samples=2_500,
        negative_samples=500,
        random_api_calls=2_500,
        min_source_occurrence_checks=2_500,
        min_api_occurrence_checks=2_500,
        min_negative_checks=250,
        min_random_api_calls=2_500,
    )


def test_published_release_profile_requires_verified_capacity_evidence():
    with pytest.raises(
        audit.ConfigurationError,
        match="release_published_capacity_evidence_required",
    ):
        _audit_config(
            profile="release",
            seed=audit.DEFAULT_SEED,
            source_occurrence_samples=2_500,
            api_occurrence_samples=2_500,
            negative_samples=500,
            random_api_calls=2_500,
            min_source_occurrence_checks=2_500,
            min_api_occurrence_checks=2_500,
            min_negative_checks=250,
            min_random_api_calls=2_500,
        )


@pytest.mark.parametrize(
    "changes",
    [
        {"source_occurrence_samples": 2_499},
        {"min_source_occurrence_checks": 2_499},
        {"api_occurrence_samples": 2_499},
        {"min_api_occurrence_checks": 2_499},
        {"min_negative_checks": 249},
        {"random_api_calls": 2_499},
        {"min_random_api_calls": 2_499},
        {"negative_samples": 499},
        {"max_first_page_first_observation_p95_ms": 40.001},
        {"max_first_page_first_observation_p95_ms": float("nan")},
        {"max_logical_query_p95_ms": 1_000.001},
        {"max_invalid_field_types": 1},
        {"verify_tls": False},
        {"api_base_url": "http://api.example.invalid"},
        {"api_base_url": "https://api.example.invalid/prefix"},
        {"api_path": "/custom/pricing"},
        {"api_audit_path": "/custom/audit"},
        {"seed": "caller-searchable-seed"},
    ],
)
def test_release_profile_cannot_relax_gate_requirements(changes):
    with pytest.raises(audit.ConfigurationError):
        replace(_release_audit_config(), **changes)


def test_validated_candidate_release_allows_exact_nonempty_npi_quarantine():
    config = replace(_release_audit_config(), max_invalid_npis=3)

    assert config.validated_candidate is True
    assert config.max_invalid_npis == 3


def test_published_release_cannot_relax_invalid_npi_ceiling():
    config = _audit_config(max_invalid_npis=1, validated_candidate=False)

    assert config._release_rules_by_reason()["release_source_integrity_ceiling"] is True


def test_release_cli_rejects_caller_seed_search_and_uses_fixed_precommitment():
    parser = audit.build_argument_parser()
    required_arguments = [
        "source.json",
        "--report",
        "report.json",
        "--api-base-url",
        "https://api.example.invalid",
        "--plan-id",
        "plan-value",
        "--snapshot-id",
        "snapshot-value",
        "--api-path",
        audit.DEFAULT_CANDIDATE_API_PATH,
        "--plan-market-type",
        "group",
        "--source-key",
        "source-value",
        "--auth-token",
        "audit-token",
        "--validated-candidate",
    ]
    searched = parser.parse_args(
        [*required_arguments, "--seed", "caller-searched-seed"]
    )
    with pytest.raises(
        audit.ConfigurationError,
        match="release_seed_caller_override_forbidden",
    ):
        audit._validate_args(searched)

    fixed = parser.parse_args(required_arguments)
    audit._validate_args(fixed)
    config = audit._audit_config(fixed)
    evidence = audit.sampling_seed_evidence(
        config.profile,
        config.seed,
        config.capacity_evidence_trust,
    )

    assert config.seed == audit.DEFAULT_SEED
    assert evidence["contract"] == audit.RELEASE_FIXED_SEED_CONTRACT
    assert evidence["precommitted"] is True
    assert evidence["seed_sha256"] == audit.sha256_text(audit.DEFAULT_SEED)


def test_diagnostic_cli_retains_explicit_seed_for_reproduction():
    parser = audit.build_argument_parser()
    args = parser.parse_args(
        [
            "source.json",
            "--report",
            "report.json",
            "--profile",
            "diagnostic",
            "--api-base-url",
            "https://api.example.invalid",
            "--plan-id",
            "plan-value",
            "--snapshot-id",
            "snapshot-value",
            "--seed",
            "diagnostic-seed",
        ]
    )
    audit._validate_args(args)

    config = audit._audit_config(args)

    assert config.seed == "diagnostic-seed"
    assert audit.sampling_seed_evidence(
        config.profile,
        config.seed,
        None,
    )["contract"] == audit.DIAGNOSTIC_SEED_CONTRACT


def _page_contract(config: audit.AuditConfig, *, matched: bool) -> audit.PageContract:
    result_state = "matched" if matched else "no_matching_rates"
    return audit.PageContract(
        result_state=result_state,
        pricing_scope=audit.EXPECTED_PRICING_SCOPE,
        resolved_snapshot_id=config.snapshot_id,
        query_snapshot_id=config.snapshot_id,
        query_plan_id=config.plan_id,
        query_mode=audit.EXPECTED_QUERY_MODE,
        provenance_arch_version=audit.EXPECTED_ARCHITECTURE,
        provenance_storage_generation=audit.EXPECTED_STORAGE_GENERATION,
        provenance_database_backend=audit.EXPECTED_DATABASE_BACKEND,
        provenance_database_evidence_contract=audit.DATABASE_EVIDENCE_CONTRACT,
        provenance_postgres_server_version_num=160004,
        provenance_database_selected=True,
        provenance_backend_session_active=True,
        provenance_transaction_snapshot_observed=True,
        provenance_plan_id=config.plan_id,
        provenance_snapshot_id=config.snapshot_id,
        provenance_mode=audit.EXPECTED_QUERY_MODE,
        provenance_pricing_scope=audit.EXPECTED_PRICING_SCOPE,
    )


def test_contract_validation_binds_requested_logical_source_key():
    config = _audit_config(source_key="logical-source")
    unbound = _page_contract(config, matched=True)

    assert audit._validate_contracts((unbound,), config, positive=True) == [
        "query_source_key_mismatch",
        "provenance_source_key_mismatch",
    ]

    bound = replace(
        unbound,
        query_source_key="logical-source",
        provenance_source_key="logical-source",
    )
    assert audit._validate_contracts((bound,), config, positive=True) == []


def test_source_key_is_sent_to_standard_pricing_and_audit_routes():
    config = _audit_config(source_key="logical-source")
    query = audit.QueryKey("CPT", "99213", NPIS[0])

    assert config.api_params(query)["source_key"] == "logical-source"
    assert config.api_audit_params()["source_key"] == "logical-source"


class _MappingFetcher:
    def __init__(self, responses, config, *, cold_pages=(7.0,), cold_total=7.0):
        self.responses = responses
        self.config = config
        self.cold_pages = tuple(cold_pages)
        self.cold_total = cold_total
        self.calls = []
        self.detailed_calls = []
        self.request_count = 0

    def fetch_all(self, params, *, phase, page_size=None):
        query = audit.QueryKey(
            str(params["code_system"]),
            str(params["code"]),
            int(params["npi"]),
        )
        response_counter = self.responses.get(
            query.stable_key,
            collections.Counter(),
        )
        response_items = []
        for tuple_key, count in response_counter.items():
            occurrence_by_field = json.loads(tuple_key)
            response_item = _api_item_for_tuple(
                _tuple_from_key(tuple_key),
                raw_sha256=occurrence_by_field.get(
                    "raw_container_sha256",
                    DEFAULT_RAW_SHA256,
                ),
            )
            response_item["prices"] = response_item["prices"] * count
            response_items.append(response_item)
        self.calls.append((query, phase))
        self.detailed_calls.append((query, phase, page_size))
        if page_size is None:
            page_latencies = self.cold_pages if phase == "cold" else (2.0,)
            total_latency = self.cold_total if phase == "cold" else 2.0
        else:
            page_count = max(math.ceil(len(response_items) / page_size), 1)
            latency = 3.0 if phase == "cold" else 1.0
            page_latencies = (latency,) * page_count
            total_latency = latency * page_count
        self.request_count += len(page_latencies)
        return audit.FetchResult(
            items=tuple(response_items),
            contracts=tuple(
                _page_contract(self.config, matched=bool(response_items))
                for _page in page_latencies
            ),
            page_latencies_ms=page_latencies,
            total_latency_ms=total_latency,
            pages=len(page_latencies),
            retries=0,
            response_fingerprint=audit.sha256_text(
                audit.canonical_json(response_items)
            ),
        )


class _AdversarialLatencyFetcher(_MappingFetcher):
    def fetch_all(self, params, *, phase, page_size=None):
        result = super().fetch_all(params, phase=phase, page_size=page_size)
        if phase != "cold":
            latency_ms = 2.0
        elif page_size is not None:
            latency_ms = 0.1
        elif result.items:
            latency_ms = 60.0
        else:
            latency_ms = 0.1
        return replace(
            result,
            page_latencies_ms=(latency_ms,) * result.pages,
            total_latency_ms=latency_ms * result.pages,
        )


class _SlowNegativeFetcher(_MappingFetcher):
    def fetch_all(self, params, *, phase, page_size=None):
        result = super().fetch_all(params, phase=phase, page_size=page_size)
        if phase == "cold" and page_size is None and not result.items:
            return replace(
                result,
                page_latencies_ms=(2_000.0,) * result.pages,
                total_latency_ms=2_000.0 * result.pages,
            )
        return result


class _SlowRandomFirstPageFetcher(_MappingFetcher):
    def fetch_all(self, params, *, phase, page_size=None):
        result = super().fetch_all(params, phase=phase, page_size=page_size)
        if phase == "cold" and page_size is not None:
            page_latencies = (60.0,) + result.page_latencies_ms[1:]
            return replace(
                result,
                page_latencies_ms=page_latencies,
                total_latency_ms=sum(page_latencies),
            )
        return result


class _StaticApiOccurrenceSource:
    def __init__(self, occurrences):
        self.occurrences = tuple(occurrences)
        self.calls = []

    def validate_source_set(self):
        return True

    def sample_occurrences(self, *, sample_target, seed):
        self.calls.append((sample_target, seed))
        return audit.ApiOccurrenceSample(
            occurrences=self.occurrences[:sample_target],
            sample_count=len(self.occurrences),
            pages=1,
            retries=0,
            sample_digest="ab" * 32,
            sample_digest_validated=True,
            source_set_validated=True,
        )


def test_random_api_request_fails_on_multiplicity_change():
    config = _audit_config()
    query = audit.QueryKey("CPT", "99213", NPIS[0])
    canonical_tuple = _canonical_tuple(query)
    occurrence_key = audit.canonical_occurrence_key(
        canonical_tuple, DEFAULT_RAW_SHA256
    )
    expected = collections.Counter({occurrence_key: 2})
    fetcher = _MappingFetcher(
        {query.stable_key: collections.Counter({occurrence_key: 1})},
        config,
    )
    request = audit.RandomApiRequest(
        index=0,
        query=query,
        source_occurrence_id="a" * 64,
        page_size=3,
        phase="cold",
    )

    result = audit._audit_random_api_request(
        request, expected, fetcher=fetcher, config=config
    )

    assert result.failure_counts == {"duplicate_count": 1}
    assert result.response_occurrences == 1


def _multi_arrangement_source_document():
    return {
        "provider_references": [],
        "in_network": [
            {
                "billing_code_type": "CPT",
                "billing_code": "99213",
                "negotiation_arrangement": "ffs",
                "negotiated_rates": [
                    {
                        "provider_groups": [{"npi": [NPIS[0]]}],
                        "negotiated_prices": [
                            _negotiated_price(negotiated_rate=100),
                            _negotiated_price(negotiated_rate=100),
                        ],
                    }
                ],
            },
            {
                "billing_code_type": "CPT",
                "billing_code": "99213",
                "negotiation_arrangement": "bundle",
                "negotiated_rates": [
                    {
                        "provider_groups": [{"npi": [NPIS[0]]}],
                        "negotiated_prices": [
                            _negotiated_price(negotiated_rate=200)
                        ],
                    }
                ],
            },
        ],
    }


def test_random_api_request_accepts_multi_arrangement_items_and_source_multiplicity(
    tmp_path,
):
    config = _audit_config()
    query = audit.QueryKey("CPT", "99213", NPIS[0])
    source_path = _write_source_fixture(
        tmp_path / "multi-item-source.json",
        references_before=True,
        gzip_encoded=False,
        source_document=_multi_arrangement_source_document(),
    )
    with _open_source_index(tmp_path, source_path, target=3) as index:
        expected_occurrences = index.expected_tuples(query)
        fetcher = _MappingFetcher(
            {query.stable_key: expected_occurrences},
            config,
        )
        request = audit.RandomApiRequest(
            index=0,
            query=query,
            source_occurrence_id=index.source_occurrences(1)[0].occurrence_id,
            page_size=1,
            phase="cold",
        )
        audit_result = audit._audit_random_api_request(
            request,
            expected_occurrences,
            fetcher=fetcher,
            config=config,
        )

    assert len(expected_occurrences) == 2
    assert sorted(expected_occurrences.values()) == [1, 2]
    assert audit_result.failure_counts == {}
    assert audit_result.response_occurrences == 3
    assert audit_result.http_requests == 2
    assert fetcher.request_count == 2


def _api_occurrences_from_source(source_occurrences):
    raw_hashes = sorted(
        {json.loads(occurrence.tuple_key)["raw_container_sha256"] for occurrence in source_occurrences}
    )
    artifact_key_by_hash = {
        raw_sha: index for index, raw_sha in enumerate(raw_hashes)
    }
    return [
        audit.ApiOccurrence(
            occurrence_id=audit.sha256_text(f"api:{occurrence.occurrence_id}"),
            canonical_tuple=_tuple_from_key(occurrence.tuple_key),
            source_identity=_source_identity(
                raw_sha256=json.loads(occurrence.tuple_key)["raw_container_sha256"],
                source_artifact_key=artifact_key_by_hash[
                    json.loads(occurrence.tuple_key)["raw_container_sha256"]
                ],
            ),
        )
        for occurrence in source_occurrences
    ]


def _response_counters(index, queries):
    return {
        query.stable_key: index.expected_tuples(query)
        for query in {
            candidate.stable_key: candidate for candidate in queries
        }.values()
    }


def _audit_runner_dependencies(index, config, source_occurrences):
    response_counters = _response_counters(
        index,
        [occurrence.query for occurrence in source_occurrences],
    )
    return (
        _MappingFetcher(response_counters, config),
        _StaticApiOccurrenceSource(_api_occurrences_from_source(source_occurrences)),
    )


def test_runner_passes_independent_occurrence_checks_and_bounds_query_scans(tmp_path):
    """Ensure independent occurrence checks pass with bounded source query scans."""

    source_path = _write_source_fixture(
        tmp_path / "sensitive-input.json.gz",
        references_before=False,
        duplicate_price=False,
    )
    raw_source_sha256 = audit.source_specs([source_path])[0].content_sha256
    with _open_source_index(tmp_path, source_path) as index:
        config = _audit_config()
        source_occurrences = index.source_occurrences(3)
        fetcher, api_occurrence_source = _audit_runner_dependencies(
            index,
            config,
            source_occurrences,
        )
        scans_before = index.metrics["expected_tuple_query_scans"]
        report = audit.AuditRunner(
            index,
            fetcher,
            api_occurrence_source,
            config,
        ).execute_audit(started_at=dt.datetime.now(dt.timezone.utc))
        scans_after = index.metrics["expected_tuple_query_scans"]

    serialized = json.dumps(report, sort_keys=True)
    assert report["status"] == "pass"
    assert report["checks"]["source_occurrence_ids"] == 3
    assert report["checks"]["api_occurrence_ids"] == 3
    assert report["checks"]["source_to_api_exact_matches"] == 3
    assert report["checks"]["api_to_source_exact_matches"] == 3
    assert report["api_audit_sample"] == {
        "contract": audit.AUDIT_SAMPLE_CONTRACT,
        "method": audit.AUDIT_SAMPLE_METHOD,
            "sample_count": 3,
            "complete_population": False,
            "sample_digest": "ab" * 32,
            "sample_digest_validated": True,
        "source_set_validated": True,
        "selected_occurrences": 3,
    }
    assert report["reproducibility"]["sampling"]["api_sample_source"] == (
        "persisted_publish_time_served_occurrences"
    )
    assert any(
        "complete served occurrence population" in limitation
        for limitation in report["limitations"]
    )
    assert scans_after - scans_before >= report["checks"]["positive_queries"]
    assert scans_after - scans_before <= (
        report["checks"]["positive_queries"] + config.random_api_calls
    )
    expected_fetch_calls = (
        report["checks"]["positive_queries"] + report["checks"]["negative_queries"]
    ) * 2
    assert len(fetcher.calls) == expected_fetch_calls + config.random_api_calls
    assert api_occurrence_source.calls == [(3, "test-seed")]
    assert report["latency"]["cold"]["first_page_first_observation"][
        "count"
    ] == report["checks"]["positive_matched_first_observation_query_keys"]
    assert report["latency"]["cold"]["logical_query"]["count"] == report[
        "checks"
    ]["positive_matched_first_observation_query_keys"]
    assert report["random_api_requests"]["planned_logical_request_count"] == 6
    assert report["random_api_requests"]["actual_http_request_count"] >= 6
    assert report["http"]["standard_api_actual_http_requests"] == fetcher.request_count
    assert report["random_api_requests"]["response_occurrence_count"] > 0
    assert report["random_api_requests"]["latency"]["cold"]["count"] > 0
    assert len(report["random_api_requests"]["fingerprints"]["request_plan_sha256"]) == 64
    assert "forbidden-marker" not in serialized
    assert "sensitive-input" not in serialized
    assert config.plan_id not in serialized
    assert config.snapshot_id not in serialized
    assert raw_source_sha256 not in serialized


def test_runner_reserves_random_keys_until_true_first_observation(tmp_path):
    """Ensure random keys remain unused until their measured cold request."""

    source_path = _write_source_fixture(
        tmp_path / "source.json.gz",
        references_before=True,
        duplicate_price=False,
    )
    with _open_source_index(tmp_path, source_path) as index:
        config = _audit_config(concurrency=3)
        source_occurrences = index.source_occurrences(3)
        random_plan = audit.build_random_api_requests(
            source_occurrences,
            count=config.random_api_calls,
            max_limit=config.random_api_max_limit,
            seed=config.seed,
        )
        random_query_keys = {
            request.query.stable_key for request in random_plan
        }
        fetcher, api_occurrence_source = _audit_runner_dependencies(
            index,
            config,
            source_occurrences,
        )
        report = audit.AuditRunner(
            index,
            fetcher,
            api_occurrence_source,
            config,
        ).execute_audit(started_at=dt.datetime.now(dt.timezone.utc))

    seen_query_keys = set()
    for query, phase, _page_size in fetcher.detailed_calls:
        query_key = query.stable_key
        if phase == "cold":
            assert query_key not in seen_query_keys
        seen_query_keys.add(query_key)

    random_calls = [
        (position, query, phase)
        for position, (query, phase, page_size) in enumerate(
            fetcher.detailed_calls
        )
        if page_size is not None
    ]
    random_cold_positions = [
        position for position, _query, phase in random_calls if phase == "cold"
    ]
    random_warm_positions = [
        position for position, _query, phase in random_calls if phase == "warm"
    ]
    assert {
        query.stable_key
        for _position, query, phase in random_calls
        if phase == "cold"
    } == random_query_keys
    assert max(random_cold_positions) < min(random_warm_positions)

    for query_key in random_query_keys:
        cold_request = next(
            request
            for request in random_plan
            if request.query.stable_key == query_key
            and request.phase == "cold"
        )
        calls_for_key_list = [
            (position, phase, page_size)
            for position, (query, phase, page_size) in enumerate(
                fetcher.detailed_calls
            )
            if query.stable_key == query_key
        ]
        assert calls_for_key_list[0][1:] == ("cold", cold_request.page_size)
        assert any(
            phase == "warm" and page_size is None
            for _position, phase, page_size in calls_for_key_list
        )

    assert report["status"] == "pass"
    assert report["checks"]["positive_queries_executed"] == report["checks"][
        "positive_queries"
    ]
    assert report["checks"]["source_to_api_exact_matches"] == 3
    assert report["checks"]["api_to_source_exact_matches"] == 3
    assert report["checks"]["positive_matched_query_keys"] == report["checks"][
        "positive_queries"
    ]
    assert report["checks"]["positive_first_observation_query_keys"] + report[
        "checks"
    ]["positive_reserved_for_random_query_keys"] == report["checks"][
        "positive_queries"
    ]
    assert report["random_api_requests"]["first_observation_query_count"] == len(
        random_query_keys
    )
    assert report["random_api_requests"][
        "reserved_from_positive_first_observation_count"
    ] == len(random_query_keys)
    assert "reserved query key" in report["random_api_requests"]["latency"][
        "semantics"
    ]


def test_persisted_occurrence_with_wrong_source_fails_direct_source_comparison(tmp_path):
    source_path = _write_source_fixture(
        tmp_path / "source.json",
        references_before=True,
        duplicate_price=False,
        gzip_encoded=False,
    )
    with _open_source_index(tmp_path, source_path, target=3) as index:
        config = _audit_config()
        source_occurrences = index.source_occurrences(3)
        responses = _response_counters(
            index,
            [occurrence.query for occurrence in source_occurrences],
        )
        persisted = _api_occurrences_from_source(source_occurrences)
        wrong_persisted_occurrences = [
            replace(
                occurrence,
                source_identity=_source_identity(raw_sha256="f" * 64),
            )
            for occurrence in persisted
        ]
        report = audit.AuditRunner(
            index,
            _MappingFetcher(responses, config),
            _StaticApiOccurrenceSource(wrong_persisted_occurrences),
            config,
        ).execute_audit(started_at=dt.datetime.now(dt.timezone.utc))

    assert report["status"] == "fail"
    assert report["failures"]["counts"]["extra_fake"] >= 1
    assert "f" * 64 not in json.dumps(report)


def test_release_runner_enforces_observed_http_request_floor(tmp_path):
    source_path = _write_source_fixture(
        tmp_path / "source.json.gz",
        references_before=True,
        duplicate_price=False,
    )
    with _open_source_index(tmp_path, source_path, target=2_500) as index:
        config = _release_audit_config()
        source_occurrences = index.source_occurrences(3)
        responses = _response_counters(
            index,
            [occurrence.query for occurrence in source_occurrences],
        )
        fetcher = _MappingFetcher(responses, config)
        report = audit.AuditRunner(
            index,
            fetcher,
            _StaticApiOccurrenceSource(_api_occurrences_from_source(source_occurrences)),
            config,
        ).execute_audit(started_at=dt.datetime.now(dt.timezone.utc))

    assert report["http"]["standard_api_actual_http_requests"] < 3_000
    assert "actual_http_requests_below_release_minimum" in report["coverage"][
        "failures"
    ]
    assert audit.RELEASE_MIN_DISTINCT_MATCHED_QUERY_KEYS == 100
    assert (
        "distinct_matched_positive_query_keys_below_release_minimum"
        in report["coverage"]["failures"]
    )


@pytest.mark.parametrize(
    ("provider_form", "npi_values", "expected_tin_only", "marker_metric"),
    [
        pytest.param(
            "referenced",
            [],
            1,
            "provider_reference_tin_markers",
            id="referenced-tin-only",
        ),
        pytest.param(
            "referenced",
            [NPIS[0]],
            0,
            "provider_reference_tin_markers",
            id="referenced-mixed-tin-valid-npi",
        ),
        pytest.param(
            "inline",
            [],
            1,
            "inline_rate_tin_markers",
            id="inline-tin-only",
        ),
        pytest.param(
            "inline",
            [NPIS[2]],
            0,
            "inline_rate_tin_markers",
            id="inline-mixed-tin-valid-npi",
        ),
    ],
)
def test_release_runner_rejects_only_tin_only_rates(
    tmp_path,
    provider_form,
    npi_values,
    expected_tin_only,
    marker_metric,
):
    source_document = _source_document(references_before=True, duplicate_price=False)
    provider_groups = [
        {
            "npi": npi_values,
            "tin": {"type": "ein", "value": "000000001"},
        }
    ]
    if provider_form == "referenced":
        source_document["provider_references"][0]["provider_groups"] = provider_groups
    else:
        source_document["in_network"][1]["negotiated_rates"][0][
            "provider_groups"
        ] = provider_groups
    source_path = _write_source_fixture(
        tmp_path / f"{provider_form}-tin.json.gz",
        references_before=True,
        duplicate_price=False,
        source_document=source_document,
    )
    with _open_source_index(tmp_path, source_path, target=2_500) as index:
        config = _release_audit_config()
        source_occurrences = index.source_occurrences(3)
        responses = _response_counters(
            index,
            [occurrence.query for occurrence in source_occurrences],
        )
        report = audit.AuditRunner(
            index,
            _MappingFetcher(responses, config),
            _StaticApiOccurrenceSource(_api_occurrences_from_source(source_occurrences)),
            config,
        ).execute_audit(started_at=dt.datetime.now(dt.timezone.utc))

    assert report["source"]["totals"][marker_metric] == 1
    assert report["source"]["totals"].get("tin_only_rates", 0) == expected_tin_only
    assert report["source"]["totals"]["rates_with_tin_markers"] == 1
    assert (
        "tin_only_rates_not_npi_verifiable" in report["coverage"]["failures"]
    ) is bool(expected_tin_only)
    if expected_tin_only:
        assert report["release_gate_eligible"] is False


def test_api_only_tuple_is_detected_outside_source_sample(tmp_path):
    source_path = _write_source_fixture(
        tmp_path / "source.json",
        references_before=True,
        duplicate_price=False,
        gzip_encoded=False,
    )
    with _open_source_index(tmp_path, source_path, target=1) as index:
        config = _audit_config(
            source_occurrence_samples=1,
            api_occurrence_samples=1,
            min_negative_checks=0,
        )
        source_occurrence = index.source_occurrences(1)[0]
        fake_query = audit.QueryKey("CPT", "77777", NPIS[2])
        fake_tuple = _canonical_tuple(fake_query, rate="777")
        fake_occurrence = audit.ApiOccurrence(
            occurrence_id=audit.sha256_text("api-only-occurrence"),
            canonical_tuple=fake_tuple,
            source_identity=_source_identity(),
        )
        responses = _response_counters(index, [source_occurrence.query])
        responses[fake_query.stable_key] = collections.Counter(
            {fake_tuple.stable_key: 1}
        )
        fetcher = _MappingFetcher(responses, config)
        report = audit.AuditRunner(
            index,
            fetcher,
            _StaticApiOccurrenceSource([fake_occurrence]),
            config,
        ).execute_audit(started_at=dt.datetime.now(dt.timezone.utc))

    assert report["status"] == "fail"
    assert report["failures"]["counts"]["extra_fake"] == 1
    source_keys = {source_occurrence.query.stable_key}
    assert fake_query.stable_key not in source_keys
    assert any(call[0] == fake_query for call in fetcher.calls)


@pytest.mark.parametrize(
    ("cold_pages", "cold_total", "expected_failure"),
    [
        ((41.0, 1.0), 42.0, "first_page_latency"),
        ((40.0004,), 40.0004, "first_page_latency"),
        ((5.0, 5.0), 1_001.0, "logical_query_latency"),
    ],
)
def test_runner_gates_first_page_and_logical_query_latency_separately(
    tmp_path,
    cold_pages,
    cold_total,
    expected_failure,
):
    source_path = _write_source_fixture(
        tmp_path / "source.json.gz",
        references_before=True,
        duplicate_price=False,
    )
    with _open_source_index(tmp_path, source_path) as index:
        config = _audit_config(random_api_calls=1)
        source_occurrences = index.source_occurrences(3)
        responses = _response_counters(
            index,
            [occurrence.query for occurrence in source_occurrences],
        )
        report = audit.AuditRunner(
            index,
            _MappingFetcher(
                responses,
                config,
                cold_pages=cold_pages,
                cold_total=cold_total,
            ),
            _StaticApiOccurrenceSource(
                _api_occurrences_from_source(source_occurrences)
            ),
            config,
        ).execute_audit(started_at=dt.datetime.now(dt.timezone.utc))

    assert report["status"] == "fail"
    assert report["failures"]["counts"][expected_failure] == 1
    other = {
        "first_page_latency",
        "logical_query_latency",
    } - {expected_failure}
    assert not other.intersection(report["failures"]["counts"])


def test_positive_first_page_p95_excludes_negative_and_random_latency(
    tmp_path,
):
    source_path = _write_source_fixture(
        tmp_path / "source.json.gz",
        references_before=True,
        duplicate_price=False,
    )
    with _open_source_index(tmp_path, source_path) as index:
        config = _audit_config(
            negative_samples=20,
            random_api_calls=1,
            min_negative_checks=0,
        )
        source_occurrences = index.source_occurrences(3)
        responses = _response_counters(
            index,
            [occurrence.query for occurrence in source_occurrences],
        )
        report = audit.AuditRunner(
            index,
            _AdversarialLatencyFetcher(responses, config),
            _StaticApiOccurrenceSource(
                _api_occurrences_from_source(source_occurrences)
            ),
            config,
        ).execute_audit(started_at=dt.datetime.now(dt.timezone.utc))

    positive_latency = report["latency"]["cold"][
        "first_page_first_observation"
    ]
    assert positive_latency["count"] == report["checks"][
        "positive_matched_first_observation_query_keys"
    ]
    assert positive_latency["count"] < report["checks"][
        "positive_matched_query_keys"
    ]
    assert positive_latency["p95_ms"] == 60.0
    assert report["random_api_requests"]["latency"]["cold"]["p95_ms"] == 0.1
    assert report["failures"]["counts"]["first_page_latency"] == 1
    assert "random requests are reported in separate classes" in report[
        "latency"
    ]["semantics"]["first_page_first_observation"]


def test_slow_negative_queries_are_gated_without_polluting_positive_latency(
    tmp_path,
):
    source_path = _write_source_fixture(
        tmp_path / "source.json.gz",
        references_before=True,
        duplicate_price=False,
    )
    with _open_source_index(tmp_path, source_path) as index:
        config = _audit_config(random_api_calls=1)
        source_occurrences = index.source_occurrences(3)
        responses = _response_counters(
            index,
            [occurrence.query for occurrence in source_occurrences],
        )
        report = audit.AuditRunner(
            index,
            _SlowNegativeFetcher(responses, config),
            _StaticApiOccurrenceSource(
                _api_occurrences_from_source(source_occurrences)
            ),
            config,
        ).execute_audit(started_at=dt.datetime.now(dt.timezone.utc))

    failures = report["failures"]["counts"]
    assert failures["negative_first_page_latency"] == 1
    assert failures["negative_logical_query_latency"] == 1
    assert "first_page_latency" not in failures
    assert "logical_query_latency" not in failures
    by_class = report["latency"]["cold"]["logical_query"]["by_class"]
    assert by_class["matched_positive"]["p95_ms"] == 7.0
    assert by_class["negative"]["p95_ms"] == 2_000.0


def test_random_cold_first_pages_have_an_independent_40ms_gate(tmp_path):
    source_path = _write_source_fixture(
        tmp_path / "source.json.gz",
        references_before=True,
        duplicate_price=False,
    )
    with _open_source_index(tmp_path, source_path) as index:
        config = _audit_config(random_api_calls=1)
        source_occurrences = index.source_occurrences(3)
        responses = _response_counters(
            index,
            [occurrence.query for occurrence in source_occurrences],
        )
        report = audit.AuditRunner(
            index,
            _SlowRandomFirstPageFetcher(responses, config),
            _StaticApiOccurrenceSource(
                _api_occurrences_from_source(source_occurrences)
            ),
            config,
        ).execute_audit(started_at=dt.datetime.now(dt.timezone.utc))

    failures = report["failures"]["counts"]
    assert failures["random_first_page_latency"] == 1
    assert "first_page_latency" not in failures
    first_page_by_class = report["latency"]["cold"][
        "first_page_first_observation"
    ]["by_class"]
    assert first_page_by_class["matched_positive"]["p95_ms"] == 7.0
    assert first_page_by_class["random_positive"]["p95_ms"] == 60.0


def _provenance_payload(plan_id="plan-value", snapshot_id="snapshot-value"):
    return {
        "arch_version": audit.EXPECTED_ARCHITECTURE,
        "storage_generation": audit.EXPECTED_STORAGE_GENERATION,
        "database_backend": audit.EXPECTED_DATABASE_BACKEND,
        "database_evidence": {
            "contract": audit.DATABASE_EVIDENCE_CONTRACT,
            "server_version_num": 160004,
            "database_selected": True,
            "backend_session_active": True,
            "transaction_snapshot_observed": True,
        },
        "plan_id": plan_id,
        "snapshot_id": snapshot_id,
        "mode": audit.EXPECTED_QUERY_MODE,
        "pricing_scope": audit.EXPECTED_PRICING_SCOPE,
    }


def _api_page_document(
    page_items,
    *,
    offset,
    limit,
    total,
    plan_id="plan-value",
    snapshot_id="snapshot-value",
    matched=True,
    audit_sample=None,
    source_set=DEFAULT_SOURCE_SET,
):
    page_document_by_field = {
        "items": page_items,
        "pagination": {
            "offset": offset,
            "limit": limit,
            "total": total,
            "has_more": offset + len(page_items) < total,
        },
        "result_state": "matched" if matched else "no_matching_rates",
        "pricing_scope": audit.EXPECTED_PRICING_SCOPE,
        "resolved_snapshot_id": snapshot_id,
        "query": {
            "snapshot_id": snapshot_id,
            "plan_id": plan_id,
            "mode": audit.EXPECTED_QUERY_MODE,
        },
        "provenance": _provenance_payload(plan_id, snapshot_id),
        "source_set": source_set.payload,
    }
    if audit_sample is not None:
        page_document_by_field["audit_sample"] = audit_sample
    return page_document_by_field


def _persisted_sample_digest(items):
    digest = audit._new_audit_sample_digest()
    for item in sorted(items, key=lambda value: value["occurrence_id"]):
        occurrence = audit.extract_api_occurrence(item)
        digest.update(bytes.fromhex(occurrence.occurrence_id))
        digest.update(audit._audit_digest_coordinate_bytes(item, occurrence))
    return digest.hexdigest()


def _persisted_audit_sample(total, *, items=None, **overrides):
    sample_by_field = {
        "contract": audit.AUDIT_SAMPLE_CONTRACT,
        "format_version": audit.AUDIT_SAMPLE_FORMAT_VERSION,
        "method": audit.AUDIT_SAMPLE_METHOD,
        "sample_count": total,
        "sample_digest": (
            _persisted_sample_digest(items) if items is not None else "ab" * 32
        ),
        "occurrence_identity": audit.AUDIT_SAMPLE_OCCURRENCE_IDENTITY,
        "serving_multiplicity_semantics": audit.AUDIT_SAMPLE_MULTIPLICITY,
        "complete_population": False,
    }
    sample_by_field.update(overrides)
    return sample_by_field


def _http_api_fetcher(*, page_size=1, max_pages=5, api_path="/pricing"):
    return audit.HttpApiFetcher(
        audit.HttpApiFetcherOptions(
            base_url="https://api.example.invalid",
            api_path=api_path,
            headers={},
            page_size=page_size,
            max_pages=max_pages,
            timeout_seconds=1,
            retries=0,
            retry_backoff_seconds=0,
            max_response_bytes=1024 * 1024,
            verify_tls=True,
        )
    )


def test_http_fetcher_pages_by_offset_and_requires_provenance():
    offsets = []

    def handler(request):
        offset = int(parse_qs(request.url.query.decode())["offset"][0])
        offsets.append(offset)
        page_document = _api_page_document(
            [{"page": offset}],
            offset=offset,
            limit=1,
            total=2,
        )
        return httpx.Response(200, json=page_document)

    fetcher = _http_api_fetcher()
    fetcher.client.close()
    fetcher.client = httpx.Client(transport=httpx.MockTransport(handler))
    try:
        fetch_result = fetcher.fetch_all({"code": "99213"}, phase="cold")
    finally:
        fetcher.close()

    assert offsets == [0, 1]
    assert [page_item["page"] for page_item in fetch_result.items] == [0, 1]
    assert (
        fetch_result.contracts[0].provenance_arch_version
        == audit.EXPECTED_ARCHITECTURE
    )

    def missing_provenance(_request):
        page_document = _api_page_document(
            [],
            offset=0,
            limit=1,
            total=0,
            matched=False,
        )
        page_document.pop("provenance")
        return httpx.Response(200, json=page_document)

    fetcher = _http_api_fetcher()
    fetcher.client.close()
    fetcher.client = httpx.Client(transport=httpx.MockTransport(missing_provenance))
    try:
        with pytest.raises(audit.ApiSchemaError, match="provenance_must_be_object"):
            fetcher.fetch_all({}, phase="cold")
    finally:
        fetcher.close()


def test_http_fetcher_uses_random_page_size_for_complete_query_traversal():
    requested_pages = []
    response_items = [{"page": index} for index in range(3)]

    def handler(request):
        query = parse_qs(request.url.query.decode())
        offset = int(query["offset"][0])
        limit = int(query["limit"][0])
        requested_pages.append((limit, offset))
        page_items = response_items[offset : offset + limit]
        return httpx.Response(
            200,
            json=_api_page_document(
                page_items,
                offset=offset,
                limit=limit,
                total=len(response_items),
            ),
        )

    fetcher = _http_api_fetcher(page_size=10)
    fetcher.client.close()
    fetcher.client = httpx.Client(transport=httpx.MockTransport(handler))
    try:
        fetch_result = fetcher.fetch_all({}, phase="warm", page_size=2)
    finally:
        fetcher.close()

    assert requested_pages == [(2, 0), (2, 2)]
    assert fetch_result.items == tuple(response_items)
    assert fetch_result.pages == 2
    assert all(
        contract.provenance_storage_generation == audit.EXPECTED_STORAGE_GENERATION
        for contract in fetch_result.contracts
    )


@pytest.mark.parametrize(
    ("mutate", "error"),
    [
        (
            lambda payload: payload["pagination"].update({"total": "0"}),
            "pagination_total_must_be_integer",
        ),
        (
            lambda payload: payload["provenance"].update({"arch_version": True}),
            "provenance_arch_version_must_be_string",
        ),
    ],
)
def test_http_fetcher_rejects_coerced_pagination_and_provenance_types(
    mutate,
    error,
):
    def handler(_request):
        page_document = _api_page_document(
            [],
            offset=0,
            limit=1,
            total=0,
            matched=False,
        )
        mutate(page_document)
        return httpx.Response(200, json=page_document)

    fetcher = _http_api_fetcher()
    fetcher.client.close()
    fetcher.client = httpx.Client(transport=httpx.MockTransport(handler))
    try:
        with pytest.raises(audit.ApiSchemaError, match=error):
            fetcher.fetch_all({}, phase="cold")
    finally:
        fetcher.close()


@pytest.mark.parametrize("positive", [True, False])
def test_response_provenance_is_required_for_positive_and_negative_pages(positive):
    config = _audit_config()
    contract = _page_contract(config, matched=positive)
    assert audit._validate_contracts((contract,), config, positive=positive) == []

    invalid = replace(contract, provenance_database_backend="memory")
    assert "provenance_database_backend_mismatch" in audit._validate_contracts(
        (invalid,),
        config,
        positive=positive,
    )

    label_only = replace(contract, provenance_database_selected=False)
    assert "provenance_database_evidence_mismatch" in audit._validate_contracts(
        (label_only,),
        config,
        positive=positive,
    )


def _persisted_occurrence_item(index: int):
    query = audit.QueryKey("CPT", f"{90000 + index}", NPIS[index % len(NPIS)])
    canonical_tuple = _canonical_tuple(query, rate=str(100 + index))
    payload = canonical_tuple.payload
    payload["negotiated_rate"] = int(payload["negotiated_rate"])
    return {
        "occurrence_id": audit.sha256_text(f"api-occurrence-{index}"),
        "tuple": payload,
        "digest_coordinates": {
            "code_key": index,
            "provider_set_key": index + 10,
            "price_key": index + 20,
            "source_artifact_key": 0,
            "npi": canonical_tuple.npi,
            "atom_ordinal": index,
            "atom_key": index + 30,
        },
        **_source_identity_payload(),
    }


def test_release_auditor_digest_matches_sealed_v2_contract_vector():
    assert _persisted_sample_digest([_persisted_occurrence_item(0)]) == (
        "bb59fa17e792f7508bc4cae8d0e69a79"
        "bf1f7962c29319baf53f769d736f6d9d"
    )


def test_http_api_occurrence_source_streams_all_pages_with_bounded_calls():
    config = _audit_config(api_audit_page_size=2, api_audit_max_pages=3)
    occurrence_items = sorted(
        (_persisted_occurrence_item(index) for index in range(5)),
        key=lambda occurrence: occurrence["occurrence_id"],
    )
    offsets = []

    def handler(request):
        query = parse_qs(request.url.query.decode())
        offset = int(query["offset"][0])
        limit = int(query["limit"][0])
        offsets.append(offset)
        page = occurrence_items[offset : offset + limit]
        return httpx.Response(
            200,
            json=_api_page_document(
                page,
                offset=offset,
                limit=limit,
                total=len(occurrence_items),
                audit_sample=_persisted_audit_sample(
                    len(occurrence_items),
                    items=occurrence_items,
                ),
            ),
        )

    fetcher = _http_api_fetcher(page_size=2, max_pages=3, api_path="/audit")
    fetcher.client.close()
    fetcher.client = httpx.Client(transport=httpx.MockTransport(handler))
    try:
        sample = audit.HttpApiOccurrenceSource(
            fetcher, config, DEFAULT_SOURCE_SET
        ).sample_occurrences(
            sample_target=2,
            seed="sample-seed",
        )
    finally:
        fetcher.close()

    assert offsets == [0, 2, 4]
    assert sample.sample_count == 5
    assert sample.pages == math.ceil(5 / 2)
    assert len(sample.occurrences) == 2
    assert len(
        {occurrence.occurrence_id for occurrence in sample.occurrences}
    ) == 2
    assert sample.contract == audit.AUDIT_SAMPLE_CONTRACT
    assert sample.method == audit.AUDIT_SAMPLE_METHOD
    assert sample.sample_count == 5
    assert sample.complete_population is False
    assert sample.sample_digest_validated is True
    assert sample.source_set_validated is True


@pytest.mark.parametrize(
    "published_source_set",
    [
        audit.source_set_evidence(["b" * 64]),
        audit.source_set_evidence(["b" * 64, "c" * 64, "d" * 64]),
    ],
)
def test_http_api_occurrence_source_rejects_omitted_or_extra_source_files(
    published_source_set,
):
    config = _audit_config(api_audit_page_size=1, api_audit_max_pages=1)
    occurrence_item = _persisted_occurrence_item(0)
    expected_source_set = audit.source_set_evidence(["b" * 64, "c" * 64])

    def handler(_request):
        return httpx.Response(
            200,
            json=_api_page_document(
                [occurrence_item],
                offset=0,
                limit=1,
                total=1,
                audit_sample=_persisted_audit_sample(
                    1,
                    items=[occurrence_item],
                ),
                source_set=published_source_set,
            ),
        )

    fetcher = _http_api_fetcher(page_size=1, max_pages=1, api_path="/audit")
    fetcher.client.close()
    fetcher.client = httpx.Client(transport=httpx.MockTransport(handler))
    try:
        with pytest.raises(
            audit.ApiSchemaError,
            match="snapshot_source_set_does_not_match_supplied_sources",
        ):
            audit.HttpApiOccurrenceSource(
                fetcher,
                config,
                expected_source_set,
            ).sample_occurrences(sample_target=1, seed="sample-seed")
    finally:
        fetcher.close()


def test_source_set_preflight_precedes_local_occurrence_sampling(tmp_path):
    source_path = _write_source_fixture(
        tmp_path / "source.json",
        references_before=True,
        gzip_encoded=False,
    )
    specs = audit.source_specs([source_path])
    with _open_source_index(tmp_path, source_path) as index:
        assert index._occurrence_sample_prepared is False
        config = _audit_config(api_audit_page_size=1, api_audit_max_pages=1)

        def handler(_request):
            return httpx.Response(
                200,
                json=_api_page_document(
                    [],
                    offset=0,
                    limit=1,
                    total=0,
                    matched=False,
                    audit_sample=_persisted_audit_sample(0, items=[]),
                    source_set=DEFAULT_SOURCE_SET,
                ),
            )

        audit_fetcher = _http_api_fetcher(
            page_size=1,
            max_pages=1,
            api_path="/audit",
        )
        audit_fetcher.client.close()
        audit_fetcher.client = httpx.Client(
            transport=httpx.MockTransport(handler)
        )
        occurrence_source = audit.HttpApiOccurrenceSource(
            audit_fetcher,
            config,
            audit.source_set_evidence_from_specs(specs),
        )
        try:
            with pytest.raises(audit.ApiSchemaError, match="source_set"):
                audit.AuditRunner(
                    index,
                    _MappingFetcher({}, config),
                    occurrence_source,
                    config,
                ).execute_audit(started_at=dt.datetime.now(dt.timezone.utc))
        finally:
            audit_fetcher.close()

        assert index._occurrence_sample_prepared is False


def test_http_api_occurrence_source_rejects_same_count_digest_tampering():
    config = _audit_config(api_audit_page_size=2, api_audit_max_pages=1)
    occurrence_items = sorted(
        (_persisted_occurrence_item(index) for index in range(2)),
        key=lambda occurrence: occurrence["occurrence_id"],
    )

    def handler(_request):
        return httpx.Response(
            200,
            json=_api_page_document(
                occurrence_items,
                offset=0,
                limit=2,
                total=2,
                audit_sample=_persisted_audit_sample(
                    2,
                    items=occurrence_items,
                    sample_digest="00" * 32,
                ),
            ),
        )

    fetcher = _http_api_fetcher(page_size=2, max_pages=1, api_path="/audit")
    fetcher.client.close()
    fetcher.client = httpx.Client(transport=httpx.MockTransport(handler))
    try:
        with pytest.raises(
            audit.ApiSchemaError,
            match="audit_sample_digest_does_not_match_returned_rows",
        ):
            audit.HttpApiOccurrenceSource(
                fetcher, config, DEFAULT_SOURCE_SET
            ).sample_occurrences(
                sample_target=1,
                seed="sample-seed",
            )
    finally:
        fetcher.close()


@pytest.mark.parametrize(
    ("audit_sample", "error"),
    [
        (None, "audit_sample_must_be_object"),
        (
            _persisted_audit_sample(2, contract="complete_population_v1"),
            "audit_sample_contract_mismatch",
        ),
        (
            _persisted_audit_sample(2, method="runtime_enumeration_v1"),
            "audit_sample_method_mismatch",
        ),
        (
            _persisted_audit_sample(3),
            "audit_sample_count_does_not_match_pagination_total",
        ),
        (
            _persisted_audit_sample(2, sample_count="2"),
            "audit_sample_count_must_be_integer",
        ),
        (
            _persisted_audit_sample(2, complete_population=True),
            "audit_sample_must_not_claim_complete_population",
        ),
        (
            _persisted_audit_sample(2, complete_population="false"),
            "audit_sample_complete_population_must_be_boolean",
        ),
    ],
)
def test_http_api_occurrence_source_requires_persisted_sample_contract(
    audit_sample,
    error,
):
    config = _audit_config(api_audit_page_size=2, api_audit_max_pages=1)
    occurrence_items = sorted(
        (_persisted_occurrence_item(index) for index in range(2)),
        key=lambda occurrence: occurrence["occurrence_id"],
    )

    def handler(_request):
        return httpx.Response(
            200,
            json=_api_page_document(
                occurrence_items,
                offset=0,
                limit=2,
                total=2,
                audit_sample=audit_sample,
            ),
        )

    fetcher = _http_api_fetcher(page_size=2, max_pages=1, api_path="/audit")
    fetcher.client.close()
    fetcher.client = httpx.Client(transport=httpx.MockTransport(handler))
    try:
        with pytest.raises(audit.ApiSchemaError, match=error):
            audit.HttpApiOccurrenceSource(
                fetcher, config, DEFAULT_SOURCE_SET
            ).sample_occurrences(
                sample_target=1,
                seed="sample-seed",
            )
    finally:
        fetcher.close()


def test_http_api_occurrence_source_rejects_non_monotonic_ids():
    config = _audit_config(api_audit_page_size=2, api_audit_max_pages=1)
    occurrence_items = sorted(
        (_persisted_occurrence_item(index) for index in range(2)),
        key=lambda occurrence: occurrence["occurrence_id"],
        reverse=True,
    )

    def handler(_request):
        return httpx.Response(
            200,
            json=_api_page_document(
                occurrence_items,
                offset=0,
                limit=2,
                total=2,
                audit_sample=_persisted_audit_sample(2),
            ),
        )

    fetcher = _http_api_fetcher(page_size=2, max_pages=1, api_path="/audit")
    fetcher.client.close()
    fetcher.client = httpx.Client(transport=httpx.MockTransport(handler))
    try:
        with pytest.raises(audit.ApiSchemaError, match="not_strictly_increasing"):
            audit.HttpApiOccurrenceSource(
                fetcher, config, DEFAULT_SOURCE_SET
            ).sample_occurrences(
                sample_target=1,
                seed="sample-seed",
            )
    finally:
        fetcher.close()


def test_api_audit_page_size_defaults_to_persisted_sample_page_limit():
    args = audit.build_argument_parser().parse_args(
        [
            "source.json.gz",
            "--report",
            "report.json",
            "--api-base-url",
            "https://api.example.invalid",
            "--plan-id",
            "plan-value",
            "--snapshot-id",
            "snapshot-value",
        ]
    )

    assert args.api_audit_page_size == 100
    assert args.api_audit_page_size == audit.DEFAULT_API_AUDIT_PAGE_SIZE
    assert args.random_api_calls == audit.RELEASE_MIN_RANDOM_API_CALLS
    assert args.min_random_api_calls == audit.RELEASE_MIN_RANDOM_API_CALLS
    assert audit.AUDIT_SAMPLE_CONTRACT == "persisted_served_occurrence_sample_v2"
    assert audit.AUDIT_SAMPLE_FORMAT_VERSION == 2
    assert _audit_config().api_params(audit.QueryKey("CPT", "99213", NPIS[0]))[
        "include_sources"
    ] == "true"


def test_validated_candidate_cli_adds_bound_header_and_requires_exact_scope():
    parser = audit.build_argument_parser()
    args = parser.parse_args(
        [
            "source.json.gz",
            "--report",
            "report.json",
            "--api-base-url",
            "https://api.example.invalid",
            "--api-path",
            audit.DEFAULT_CANDIDATE_API_PATH,
            "--plan-id",
            "plan-value",
            "--snapshot-id",
            "snapshot-value",
            "--plan-market-type",
            "group",
            "--source-key",
            "source-value",
            "--auth-token",
            "operator-secret",
            "--validated-candidate",
        ]
    )

    audit._validate_args(args)
    config = audit._audit_config(args)
    headers = audit._request_headers(args)
    assert config.validated_candidate is True
    assert config.api_path == audit.DEFAULT_CANDIDATE_API_PATH
    assert config.redacted_target()["expected_snapshot_lifecycle"] == "validated"
    assert headers[audit.CANDIDATE_AUDIT_HEADER] == "snapshot-value"
    assert headers["Authorization"] == "Bearer operator-secret"

    missing_scope_args = parser.parse_args(
        [
            "source.json.gz",
            "--report",
            "report.json",
            "--api-base-url",
            "https://api.example.invalid",
            "--plan-id",
            "plan-value",
            "--snapshot-id",
            "snapshot-value",
            "--auth-token",
            "operator-secret",
            "--validated-candidate",
        ]
    )
    with pytest.raises(audit.ConfigurationError, match="auth_source_and_market"):
        audit._validate_args(missing_scope_args)


def test_release_cli_rejects_insecure_transport_and_nonstandard_paths():
    base_args = [
        "source.json.gz",
        "--report",
        "report.json",
        "--api-base-url",
        "https://api.example.invalid",
        "--plan-id",
        "plan-value",
        "--snapshot-id",
        "snapshot-value",
        "--api-path",
        audit.DEFAULT_CANDIDATE_API_PATH,
        "--plan-market-type",
        "group",
        "--source-key",
        "source-value",
        "--auth-token",
        "audit-token",
        "--validated-candidate",
    ]
    parser = audit.build_argument_parser()

    with pytest.raises(audit.ConfigurationError, match="tls_verification"):
        audit._audit_config(parser.parse_args([*base_args, "--insecure"]))
    with pytest.raises(audit.ConfigurationError, match="standard_api_path"):
        audit._audit_config(
            parser.parse_args([*base_args, "--api-path", "/custom/pricing"])
        )
    with pytest.raises(audit.ConfigurationError, match="audit_api_path"):
        audit._audit_config(
            parser.parse_args([*base_args, "--api-audit-path", "/custom/audit"])
        )


def test_fatal_report_contains_only_safe_error_metadata(tmp_path):
    source = _write_source_fixture(
        tmp_path / "forbidden-marker.json.gz",
        references_before=True,
    )
    specs = audit.source_specs([source])
    raw_source_sha256 = specs[0].content_sha256
    config = _audit_config(
        plan_id="forbidden-plan",
        snapshot_id="forbidden-snapshot",
    )

    report = audit.fatal_report(
        started_at=dt.datetime.now(dt.timezone.utc),
        exc=audit.SourceFormatError("forbidden-marker in exception"),
        config=config,
        specs=specs,
    )

    serialized = json.dumps(report, sort_keys=True)
    assert report["status"] == "error"
    assert report["failures"]["counts"] == {"source_format": 1}
    assert "forbidden-marker" not in serialized
    assert config.plan_id not in serialized
    assert config.snapshot_id not in serialized
    assert str(source) not in serialized
    assert raw_source_sha256 not in serialized
