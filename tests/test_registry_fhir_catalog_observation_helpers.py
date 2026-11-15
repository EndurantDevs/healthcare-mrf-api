# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Public observation mechanics retain existing bodies, SQL and refusal behavior."""

from contextlib import asynccontextmanager
from copy import deepcopy
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import registry_fhir_catalog_observed_source as observed
from process import registry_fhir_catalog_sources as sources
from process.registry_fhir_catalog_observation import FHIRCatalogObservationDigests
from tests.test_registry_fhir_catalog_observation import _receipt
from tests.test_registry_fhir_catalog_sources import descriptor, published_row, read_profile


def test_public_helpers_are_exact_existing_callables():
    for public, private in (
        ("read_fhir_catalog_observation_header", "_header"),
        ("read_fhir_catalog_observation_registration", "_registration"),
        ("collect_fhir_catalog_observation_facts", "_facts"),
        ("build_fhir_catalog_observation_descriptor", "_descriptor"),
    ):
        assert getattr(observed, public) is getattr(observed, private)


def test_public_descriptor_builder_preserves_receipt_and_serialized_fields():
    publication = _receipt()["publication"]
    header_by_field = {
        "dataset_id": "dataset-one",
        "endpoint_id": "endpoint-one",
        "dataset_hash": "a" * 64,
        "previous_dataset_id": None,
        "published_at": datetime.fromisoformat(publication["published_at"]),
        "resource_count": 0,
        **{
            key: field_value
            for key, field_value in publication.items()
            if key not in {"published_at", "claimed_resource_count"}
        },
    }
    original = descriptor()
    tables = deepcopy(original["tables"])
    families = FHIRCatalogObservationDigests().document()
    actual_descriptor = observed.build_fhir_catalog_observation_descriptor(
        read_profile(),
        header_by_field,
        tables,
        ("https://example.test/fhir", ["alias-a", "alias-b"]),
        families,
    )
    assert sources.validate_fhir_catalog_source_descriptor(actual_descriptor) == actual_descriptor
    assert set(actual_descriptor) == set(original)
    assert actual_descriptor["admission"]["families"] == families
    assert actual_descriptor["admission"]["publication"] == {**publication, "claimed_resource_count": 0}
    assert actual_descriptor["source_aliases"] == ["alias-a", "alias-b"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changes,reason",
    [
        ({"dataset_id": None}, "current_dataset_missing"),
        ({"alias_count": sources.MAX_ALIASES + 1}, "source_alias_bounds"),
        ({"current_count": 2}, "current_dataset_ambiguous"),
    ],
)
async def test_observation_inventory_keeps_unsupported_registration_explicit_without_collection(
    monkeypatch, changes, reason
):
    collect = AsyncMock(side_effect=AssertionError("unavailable inventory must not collect Source facts"))
    monkeypatch.setattr(observed, "collect_fhir_catalog_observation", collect)
    disposition = await sources._observed_disposition(object(), read_profile(), published_row() | changes)
    assert disposition.status == "unavailable" and disposition.reason == reason
    assert disposition.descriptor is None
    collect.assert_not_awaited()


@pytest.mark.asyncio
async def test_observation_collection_drift_does_not_fall_back_to_semantic_admission(monkeypatch):
    collect = AsyncMock(side_effect=ValueError("source changed during collection"))
    monkeypatch.setattr(observed, "collect_fhir_catalog_observation", collect)

    @asynccontextmanager
    async def transaction():
        yield

    connection = SimpleNamespace(transaction=transaction)
    disposition = await sources._observed_disposition(connection, read_profile(), published_row())
    assert disposition.status == "unavailable" and disposition.reason == "catalog_observation_unavailable"
    assert disposition.descriptor is None
    collect.assert_awaited_once()
