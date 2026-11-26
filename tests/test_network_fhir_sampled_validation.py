# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Synthetic sampling and refusal checks; these confer no native capture custody."""

import copy
import hashlib
import heapq
import json
import random
from dataclasses import asdict
from datetime import datetime
from uuid import UUID

import pytest

from process import network_fhir_sampled_validation as sampled
from process import network_fhir_typed_capture as capture
from process.network_fhir_source_witness_contract import canonical, digest, source_origin
from process.network_fhir_typed_contract import TypedFHIRGraphError, TypedFHIRGraphStage, graph_document
from process.network_fhir_typed_source import _raw_projection
from tests.test_network_fhir_source_witness_contract import origin_fields

_SEED = "1" * 64
_NETWORKS = ("network-one",)


class _BottomPairs:
    """Frozen Python reference for native parity; production must require the native sampler."""

    def __init__(self, seed, network):
        self.seed, self.network = seed, network
        self.selected, self.heap = {}, []
        self.provenance_rows, self.overflow = 0, False

    def add(self, pair):
        """Count one provenance row while retaining only the distinct lowest ranked pairs."""
        self.provenance_rows += 1
        if pair in self.selected:
            return
        encoded = canonical([self.network, *map(str, pair)])
        rank = int.from_bytes(hashlib.sha256((sampled.CONTRACT + "\0" + self.seed + "\0" + encoded).encode()).digest())
        entry = (rank, encoded, pair)
        if len(self.selected) < sampled._LIMIT:
            self.selected[pair] = rank
            heapq.heappush_max(self.heap, entry)
        else:
            self.overflow = True
            if (rank, encoded) < self.heap[0][:2]:
                removed = heapq.heapreplace_max(self.heap, entry)[2]
                del self.selected[removed]
                self.selected[pair] = rank

    def receipt(self):
        """Describe exact small populations and the explicit large-population lower bound."""
        selected = len(self.selected)
        return {
            "unit": "distinct-provider-office-pair",
            "population": None if self.overflow else selected,
            "population_lower_bound": sampled._LIMIT + 1 if self.overflow else selected,
            "provenance_rows": self.provenance_rows,
            "selected": selected,
            "series_counts": sampled._series_counts(selected),
            "witness_rows": 0,
            "failures": 0,
        }


def _origin():
    fields = origin_fields()
    counts = (
        ("Location", 1),
        ("Organization", 1),
        ("OrganizationAffiliation", 0),
        ("Practitioner", 1),
        ("PractitionerRole", 2000),
    )
    fields.update(
        resource_counts=counts, descriptor_counts=counts, resource_hashes=tuple((kind, "a" * 64) for kind, _ in counts)
    )
    return source_origin(fields)


def _graph():
    counts_by_kind = dict.fromkeys(
        ("source_rows", "projected_rows", "pair_rows", "reference_rows", "provider_only_roles"), 0
    )
    return graph_document(_origin(), "2026-01-01", counts_by_kind)


def _capture(graph):
    from process.registry_fhir_catalog_extraction import RESOURCE_TYPES
    from process.registry_fhir_catalog_observed_source import build_fhir_catalog_observation_descriptor
    from process.registry_fhir_catalog_sources import FHIRCatalogSourceReadProfile

    origin = _origin()
    header_by_field = {
        "dataset_id": origin.dataset_id,
        "endpoint_id": origin.endpoint_id,
        "dataset_hash": origin.dataset_sha256,
        "root_run_id": origin.root_run_id,
        "published_at": datetime(2026, 1, 1),
        "resource_hash_contract": None,
        "resource_count": sum(dict(origin.resource_counts).values()),
        "previous_dataset_id": None,
    }
    descriptor = build_fhir_catalog_observation_descriptor(
        FHIRCatalogSourceReadProfile(origin.schema_name, origin.header_relation["schema"]),
        header_by_field,
        {"header": origin.header_relation, "resource": origin.resource_relation},
        (origin.source_uri, list(origin.source_aliases)),
        {kind: {"rows": 0} for kind in RESOURCE_TYPES},
    )
    capture_id = str(UUID(int=1))
    schema = capture.PREFIX + UUID(capture_id).hex
    capture_by_field = {
        "component": capture.COMPONENT,
        "descriptor_version": 5,
        "capture_id": capture_id,
        "observed_at": "2026-01-02T00:00:00.000000+00:00",
        "schema_name": schema,
        "tables": {kind: {"schema": schema, "name": name} for kind, name in capture.TABLES.items()},
        "content": {kind: {"rows": 0, "sha256": "a" * 64} for kind in capture.TABLES},
        "graph": graph,
        "catalog_descriptor": descriptor,
    }
    capture_by_field["capture_sha256"] = digest(capture_by_field)
    return capture_by_field


def _pair(index):
    return ("network-one", "npi", f"{index:010}", UUID(int=index + 1), UUID(int=index + 2), "synthetic|street")


def _direction(*, source, population=1, failures=0):
    selected = min(population, 1000)
    direction_by_field = {
        "unit": "source-role-witness" if source else "distinct-provider-office-pair",
        "population": population,
        "selected": selected,
        "series_counts": sampled._series_counts(selected),
        "failures": failures,
    }
    direction_by_field.update(
        {"derived_pair_rows": selected}
        if source
        else {"population_lower_bound": population, "provenance_rows": selected, "witness_rows": selected}
    )
    return direction_by_field


def _receipt():
    receipt_by_field = {
        "contract": sampled.CONTRACT,
        "mode": "sampled",
        "seed": _SEED,
        "algorithm": sampled.ALGORITHM,
        "series_count": 10,
        "items_per_series": 100,
        "source_origin_sha256": digest(asdict(_origin())),
        "capture_id": str(UUID(int=1)),
        "observed_at": "2026-01-02T00:00:00.000000+00:00",
        "capture_sha256": _capture(_graph())["capture_sha256"],
        "membership_as_of": "2026-01-01",
        "networks": [
            {
                "network_source_key": "network-one",
                "source_to_candidate": _direction(source=True),
                "candidate_to_source": _direction(source=False),
            }
        ],
        "limitations": list(sampled._LIMITATIONS),
    }
    receipt_by_field["receipt_sha256"] = digest(receipt_by_field)
    return receipt_by_field


def test_bottom_pairs_are_distinct_bounded_and_order_independent():
    rows = [_pair(index) for index in range(1400)] * 2
    first = _BottomPairs(_SEED, "network-one")
    for pair_by_field in rows:
        first.add(pair_by_field)
    random.Random(7).shuffle(rows)
    second = _BottomPairs(_SEED, "network-one")
    for pair_by_field in rows:
        second.add(pair_by_field)
    assert first.selected == second.selected and len(first.heap) == 1000
    assert first.receipt()["population"] is None
    assert first.receipt()["population_lower_bound"] == 1001
    assert first.receipt()["provenance_rows"] == 2800
    assert any(pair_by_field not in first.selected for pair_by_field in rows), (
        "A rare bad edge can remain outside the sample."
    )


def test_smaller_population_is_exhausted_without_duplicate_inflation():
    selection = _BottomPairs(_SEED, "network-one")
    for index in [1, 2, 1]:
        selection.add(_pair(index))
    assert selection.receipt() == {
        "unit": "distinct-provider-office-pair",
        "population": 2,
        "population_lower_bound": 2,
        "selected": 2,
        "series_counts": [2] + [0] * 9,
        "provenance_rows": 3,
        "witness_rows": 0,
        "failures": 0,
    }


def test_raw_dependencies_preserve_scoped_refs_dates_npi_and_site_identity():
    pair_by_field = {
        "resource_type": "PractitionerRole",
        "resource_id": "role-one",
        "payload_hash": "a" * 64,
        "site_id": None,
        "payload_json": json.dumps(
            {
                "practitioner_ref": "Practitioner/provider-one",
                "npi": "1234567893",
                "network_refs": [
                    "Organization/network-one",
                    "https://foreign.example.test/fhir/Organization/network-one",
                ],
                "location_refs": ["Location/office-one", "Location/office-one?version=1"],
                "period_end": "2025-12-31",
                "accepting_patients": [{"from_network_ref": "Organization/indirect"}],
            }
        ),
    }
    cells, checks = _raw_projection(pair_by_field, _origin().source_uri, sampled.policy_date("2026-01-01"))
    assert checks[3:7] == ("provider-one", ["network-one"], ["office-one"], False)
    assert checks[-1] is True and cells[-1] is None


def test_receipt_is_closed_bound_and_contains_no_provider_payload():
    receipt = _receipt()
    assert sampled.validate_sampled_semantic_receipt(receipt) == receipt
    assert "provider_id" not in json.dumps(receipt) and "payload_json" not in json.dumps(receipt)
    for key, value in [("mode", "exhaustive"), ("extra", "field"), ("capture_sha256", "3" * 64)]:
        changed = copy.deepcopy(receipt)
        changed[key] = value
        with pytest.raises(TypedFHIRGraphError, match="sample_receipt"):
            sampled.validate_sampled_semantic_receipt(changed)


@pytest.mark.asyncio
async def test_read_preserves_sampling_mode_and_checks_logical_content_bindings():
    graph = _graph()
    document = _capture(graph)
    stage = TypedFHIRGraphStage(dict(capture.TABLES), schema_name=document["schema_name"], document=graph)
    graph_read = capture.TypedGraphRead(
        stage, document, capture.TypedGraphAnchor(document["capture_id"], document["observed_at"])
    )
    receipt = _receipt()
    read = sampled.SampledTypedGraphRead(graph_read, json.dumps(receipt))
    read.receipt["seed"] = "changed"
    read.require_ready()
    assert read.receipt == receipt and stage.checked is False and not hasattr(read, "checked")
    assert read.relation("pairs") == stage.relation("pairs")
    changed_receipt_by_field = dict(receipt, capture_id=str(UUID(int=2)))
    changed_receipt_by_field["receipt_sha256"] = digest(
        {key: value for key, value in changed_receipt_by_field.items() if key != "receipt_sha256"}
    )
    with pytest.raises(TypedFHIRGraphError, match="sample_read"):
        sampled.SampledTypedGraphRead(graph_read, json.dumps(changed_receipt_by_field)).require_ready()


def test_legacy_source_origin_projects_named_provenance_and_preserves_content():
    original = origin_fields()
    projected = source_origin(original)
    assert projected.header_relation == {
        "schema": original["schema_name"],
        "name": "provider_directory_endpoint_dataset",
    }
    assert projected.resource_relation == {
        "schema": original["schema_name"],
        "name": "provider_directory_dataset_resource",
    }
    assert projected.dataset_sha256 == original["dataset_sha256"]
    assert projected.resource_counts == original["resource_counts"]
    assert projected.resource_hashes == original["resource_hashes"]
    named = asdict(projected)
    assert source_origin(named) == projected
    named["unexpected_scope"] = "other"
    with pytest.raises(ValueError):
        source_origin(named)
