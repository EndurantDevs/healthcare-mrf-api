# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Expanded metadata pages retain every resource without enlarging output limits."""

import json
from dataclasses import asdict

import pytest

from process.registry_fhir_catalog_extraction import (
    DEFAULT_BYTES,
    MAX_EDGES,
    MAX_ROWS,
    FHIRCatalogExtractionError,
    extract_fhir_catalog_batch,
    iter_fhir_catalog_batches,
)


def resource(kind, identifier, **payload):
    return {"resource_type": kind, "resource_id": identifier, "payload_hash": "a" * 64, "payload_json": payload}


def plan(identifier, references, **payload):
    return resource(
        "InsurancePlan",
        identifier,
        network_refs=[f"Organization/network-{position}" for position in range(references)],
        **payload,
    )


def assert_output_bounds(batches):
    for batch in batches:
        encoded = json.dumps(
            {
                "organizations": [asdict(row) for row in batch.organizations],
                "edges": [asdict(row) for row in batch.edges],
            },
            sort_keys=True,
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode()
        assert len(encoded) == batch.output_bytes <= DEFAULT_BYTES
        assert len(batch.edges) <= MAX_EDGES and 0 < batch.source_rows <= MAX_ROWS


@pytest.mark.parametrize("profile", ["raw_fhir", "published_normalized", "retained_normalized"])
def test_one_batch_preserves_existing_result_and_zero_output_resource_accounting(profile):
    rows = [
        resource("Organization", "first", name="Réseau"),
        resource("InsurancePlan", "plan", name="Choice"),
        resource("PractitionerRole", "empty"),
        resource("Organization", "second", name="Other"),
    ]
    if profile == "raw_fhir":
        for row in rows:
            row["payload_json"].update(resourceType=row["resource_type"], id=row["resource_id"])
    expected = extract_fhir_catalog_batch(rows, profile=profile)
    batches = list(iter_fhir_catalog_batches(rows, profile=profile))
    assert batches == [expected] and expected.source_rows == 4
    assert_output_bounds(batches)


def test_aggregate_edge_limit_splits_whole_resources_once_without_losing_order(monkeypatch):
    from process import registry_fhir_catalog_extraction as extraction

    rows = [plan("first", 6000), plan("second", 6000)]
    with pytest.raises(FHIRCatalogExtractionError, match="output_bounds") as caught:
        extract_fhir_catalog_batch(rows, profile="published_normalized")
    assert caught.value.bounds["limit"] == "edges"
    assert caught.value.bounds["edges"] == MAX_EDGES + 1
    assert caught.value.bounds["resource_edges"] < MAX_EDGES
    visited, original = [], extraction._resource_edges

    def once(row, *arguments):
        visited.append(row["resource_id"])
        return original(row, *arguments)

    monkeypatch.setattr(extraction, "_resource_edges", once)
    batches = list(iter_fhir_catalog_batches(rows, profile="published_normalized"))
    assert visited == ["first", "second"]
    assert [batch.source_rows for batch in batches] == [1, 1]
    assert [len(batch.edges) for batch in batches] == [6000, 6000]
    assert [
        (edge.owner_resource_id, edge.scope_path, edge.network_resource_id) for batch in batches for edge in batch.edges
    ] == [
        (identifier, f"/normalized_aggregate/{position}", f"network-{position}")
        for identifier in ("first", "second")
        for position in range(6000)
    ]
    assert_output_bounds(batches)


def test_aggregate_byte_limit_splits_without_changing_single_batch_refusal():
    rows = [plan(identifier, 3000, name="x" * 512) for identifier in ("first", "second", "third")]
    with pytest.raises(FHIRCatalogExtractionError, match="output_bounds") as caught:
        extract_fhir_catalog_batch(rows, profile="published_normalized")
    assert caught.value.bounds["limit"] == "bytes"
    assert caught.value.bounds["output_bytes"] > DEFAULT_BYTES
    assert caught.value.bounds["resource_output_bytes"] < DEFAULT_BYTES
    batches = list(iter_fhir_catalog_batches(rows, profile="published_normalized"))
    assert [batch.source_rows for batch in batches] == [2, 1]
    assert [len(batch.edges) for batch in batches] == [6000, 3000]
    assert sum(batch.source_rows for batch in batches) == len(rows)
    assert [edge.owner_resource_id for batch in batches for edge in batch.edges] == [
        identifier for identifier in ("first", "second", "third") for _ in range(3000)
    ]
    assert_output_bounds(batches)


@pytest.mark.parametrize("limit", ["edges", "bytes"])
def test_one_resource_overflow_is_explicit_and_diagnostics_never_contain_source_data(limit):
    row = (
        plan("private-example", MAX_EDGES + 1)
        if limit == "edges"
        else plan("private-example", 2000, benefits=[{"description": "sensitive-example" * 300}])
    )
    for extract in (
        extract_fhir_catalog_batch,
        lambda *args, **kwargs: list(iter_fhir_catalog_batches(*args, **kwargs)),
    ):
        with pytest.raises(FHIRCatalogExtractionError, match="output_bounds") as caught:
            extract([row], profile="published_normalized")
        assert caught.value.args == ("registry_fhir_catalog_output_bounds",)
        counters = caught.value.bounds
        assert counters["limit"] == limit and counters["input_rows"] == 1 and counters["resource_index"] == 0
        assert counters["edges"] == counters["resource_edges"]
        assert counters["output_bytes"] == counters["resource_output_bytes"]
        assert set(counters) == {
            "limit",
            "resource_index",
            "input_rows",
            "edges",
            "output_bytes",
            "resource_edges",
            "resource_output_bytes",
            "max_edges",
            "max_output_bytes",
        }
        assert all(type(value) is int and 0 <= value < 2**31 for key, value in counters.items() if key != "limit")


def test_duplicates_across_output_boundaries_are_rejected():
    first, second = resource("Organization", "first"), resource("Organization", "second")
    maximum = max(
        extract_fhir_catalog_batch([row], profile="published_normalized").output_bytes for row in (first, second)
    )
    batches = iter_fhir_catalog_batches(
        [first, second, first], profile="published_normalized", max_output_bytes=maximum
    )
    assert next(batches).organizations[0].resource_id == "first"
    with pytest.raises(FHIRCatalogExtractionError, match="duplicate_resource"):
        next(batches)


def test_resource_failure_counters_locate_the_original_input_row():
    rows = [resource("PractitionerRole", "empty"), resource("Organization", "large", name="x" * 512)]
    with pytest.raises(FHIRCatalogExtractionError, match="output_bounds") as caught:
        list(iter_fhir_catalog_batches(rows, profile="published_normalized", max_output_bytes=100))
    assert caught.value.bounds["resource_index"] == 1 and caught.value.bounds["input_rows"] == 2
    assert caught.value.bounds["resource_output_bytes"] > 100


@pytest.mark.parametrize("options", [{"max_rows": 1}, {"max_input_bytes": 1}])
def test_complete_input_bounds_are_checked_before_yield(options):
    batches = iter_fhir_catalog_batches(
        [resource("Organization", "first"), resource("Organization", "second")],
        profile="published_normalized",
        **options,
    )
    with pytest.raises(FHIRCatalogExtractionError, match="input_bounds"):
        next(batches)


def test_invalid_resource_and_empty_input_preserve_validation():
    rows = [resource("Organization", "valid"), resource("Organization", "invalid", name=7)]
    with pytest.raises(FHIRCatalogExtractionError, match="registry_fhir_catalog_invalid"):
        list(iter_fhir_catalog_batches(rows, profile="published_normalized"))
    assert list(iter_fhir_catalog_batches([], profile="published_normalized")) == []
    assert extract_fhir_catalog_batch([], profile="published_normalized").source_rows == 0
