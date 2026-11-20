# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Independent Source replay routing; fake contexts establish no native authority."""

import asyncio
from contextlib import asynccontextmanager
from copy import deepcopy
from dataclasses import replace
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process import registry_fhir_catalog_grant as grants
from process import registry_fhir_catalog_validation as validation
from process.registry_fhir_catalog_observation import catalog_observation_descriptor_digest
from process.registry_fhir_catalog_sources import (
    fhir_catalog_source_generation,
    validate_fhir_catalog_source_descriptor,
)
from tests.test_registry_fhir_catalog_grant import CatalogConnection, command, snapshot, standing_grant
from tests.test_registry_fhir_catalog_grant import descriptor as admitted_content_descriptor
from tests.test_registry_fhir_catalog_observation import _receipt
from tests.test_registry_fhir_catalog_source_companies import source_company
from tests.test_registry_fhir_catalog_source_producer import _boundary
from tests.test_registry_fhir_catalog_sources import catalog_schema_descriptor, descriptor, read_profile


def observed_descriptor(profile=None, dataset="dataset-one"):
    profile = read_profile() if profile is None else profile
    selected = descriptor()
    selected["coordinates"].update(dataset_schema=profile.schema_name, dataset_id=dataset, edition_id=dataset)
    selected["tables"]["header"]["schema"] = profile.catalog_schema
    selected["tables"]["resource"]["schema"] = profile.schema_name
    selected.update(native_read_profile=profile.document(), admission=_receipt())
    digest = catalog_observation_descriptor_digest(selected)
    selected.update(
        accepted_receipt_id=grants.CATALOG_OBSERVATION_CONTRACT + ":" + digest, accepted_receipt_sha256=digest
    )
    return validate_fhir_catalog_source_descriptor(selected)


def scoped_observed_descriptor():
    """Keep the admitted dataset and observation projection on a portable named profile."""
    profile = read_profile()
    return observed_descriptor(profile=profile), profile


def compact_rows(selected):
    return (
        (
            fhir_catalog_source_generation(selected),
            "Organization",
            "network-one",
            "organization",
            0,
            '{"name":"Example"}',
        ),
    )


def verifier_case(monkeypatch, descriptors=None, profiles=None):
    descriptors = (observed_descriptor(),) if descriptors is None else descriptors
    profiles = (read_profile(),) if profiles is None else profiles
    grant = standing_grant(source_profiles=profiles, observation_contract=grants.CATALOG_OBSERVATION_CONTRACT)
    selected = snapshot(grant, descriptors)
    by_generation = {fhir_catalog_source_generation(descriptor): descriptor for descriptor in descriptors}
    factory, export, connection, events = _boundary()

    async def replay(value, collect):
        events.append(("replay", value["coordinates"]["dataset_id"]))
        await collect(compact_rows(value))
        return 1

    export["replay"].side_effect = replay
    monkeypatch.setattr(grants, "load_fhir_catalog_source_descriptors", AsyncMock(return_value=by_generation))
    pool = SimpleNamespace(acquire=Mock(side_effect=AssertionError("Reader fallback used")))
    copy_target = SimpleNamespace(schema_name=selected["schema_name"])
    return SimpleNamespace(
        grant=grant,
        snapshot=selected,
        descriptors=by_generation,
        factory=factory,
        export=export,
        connection=connection,
        events=events,
        pool=pool,
        target=copy_target,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["bounded_document", "source_identity", "vector_identity", "missing"])
async def test_protected_descriptor_inventory_refuses_bounds_missing_rows_or_identity_drift(monkeypatch, failure):
    source_descriptor = observed_descriptor()
    generation = fhir_catalog_source_generation(source_descriptor)
    grant = standing_grant(observation_contract=grants.CATALOG_OBSERVATION_CONTRACT)
    source_entries = [
        {
            "source_generation": generation,
            "source_id": "other" if failure == "vector_identity" else "alias-a",
            "status": "current",
        }
    ]
    selected = snapshot(grant, source_vector_json=source_entries)
    entry_by_field = {
        "source_generation": generation,
        "source_id": "other" if failure == "source_identity" else "alias-a",
        "document": None if failure == "bounded_document" else validation.canonical_json(source_descriptor),
    }
    writer = CatalogConnection((selected,))
    writer.fetch = AsyncMock(side_effect=[[] if failure == "missing" else [entry_by_field], []])
    monkeypatch.setattr(grants, "validate_fhir_catalog_snapshot_record", AsyncMock())
    runtime = grants.build_fhir_catalog_publication_runtime(grant, None)
    reason = {"bounded_document": "descriptor_bounds", "missing": "descriptor_unavailable"}.get(
        failure, "descriptor_changed"
    )
    with pytest.raises(grants.FHIRCatalogGrantError, match=reason):
        await runtime.authorize_metadata(command=command(grant, selected), snapshot=selected, connection=writer)


@pytest.mark.asyncio
@pytest.mark.parametrize("route", ["missing", "writer"])
async def test_semantic_source_fence_never_borrows_the_catalog_writer_as_its_reader(monkeypatch, route):
    selected_source = admitted_content_descriptor()
    grant = standing_grant()
    selected = snapshot(grant, (selected_source,))
    writer = CatalogConnection((selected,), (selected_source,))

    @asynccontextmanager
    async def acquire_writer():
        yield writer

    monkeypatch.setattr(grants, "validate_fhir_catalog_snapshot_record", AsyncMock())
    pool = None if route == "missing" else SimpleNamespace(acquire=acquire_writer)
    runtime = grants.build_fhir_catalog_publication_runtime(grant, pool)
    with pytest.raises(grants.FHIRCatalogGrantError, match="reader_required"):
        async with runtime.source_fence(snapshot=selected, connection=writer):
            pytest.fail("writer was accepted as a Source Reader")
    assert not writer.active


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["source_uri", "content", "edition"])
async def test_catalog_schema_publication_refuses_mutated_pinned_descriptor_before_replay(monkeypatch, damage):
    stored = catalog_schema_descriptor()
    admitted = validate_fhir_catalog_source_descriptor(stored)
    grant = standing_grant(observation_contract=grants.CATALOG_OBSERVATION_CONTRACT)
    selected = snapshot(grant, (admitted,))
    generation = selected["source_vector_json"][0]["source_generation"]
    if damage == "source_uri":
        stored["source_uri"] = "https://another-directory.example.test/fhir/"
    elif damage == "content":
        stored["admission"]["families"]["Organization"]["rows"] = 1
    else:
        stored["coordinates"].update(dataset_id="dataset-two", edition_id="dataset-two")
    assert fhir_catalog_source_generation(stored) != generation
    writer = CatalogConnection((selected,))
    writer.fetch = AsyncMock(
        side_effect=[
            [{"source_generation": generation, "source_id": "alias-a", "document": validation.canonical_json(stored)}],
            [],
        ]
    )
    monkeypatch.setattr(grants, "validate_fhir_catalog_snapshot_record", AsyncMock())
    factory = Mock()
    runtime = grants.build_fhir_catalog_publication_runtime(grant, None, source_export=factory)
    with pytest.raises(grants.FHIRCatalogGrantError, match="descriptor_changed"):
        await runtime.authorize_metadata(command=command(grant, selected), snapshot=selected, connection=writer)
    factory.assert_not_called()


def test_export_hook_is_validated_only_at_trusted_runtime_construction():
    with pytest.raises(grants.FHIRCatalogGrantError, match="source_export_invalid"):
        grants.build_fhir_catalog_publication_runtime(standing_grant(), None, source_export={})


@pytest.mark.asyncio
async def test_publication_fence_for_observations_stays_reader_free(monkeypatch):
    case = verifier_case(monkeypatch)
    writer = CatalogConnection((case.snapshot,), tuple(case.descriptors.values()))
    monkeypatch.setattr(grants, "validate_fhir_catalog_snapshot_record", AsyncMock())
    runtime = grants.build_fhir_catalog_publication_runtime(case.grant, case.pool, source_export=case.factory)
    async with runtime.source_fence(snapshot=case.snapshot, connection=writer):
        await runtime.authorize_metadata(
            command=command(case.grant, case.snapshot), snapshot=case.snapshot, connection=writer
        )
        assert await runtime.verify_sources(snapshot=case.snapshot, connection=writer) is True
    case.factory.assert_not_called()
    case.pool.acquire.assert_not_called()


@pytest.mark.parametrize("change", ["status", "generation", "source_id", "projector", "transform", "missing_labels"])
def test_metadata_only_input_guard_refuses_any_source_or_extractor_change(change):
    selected = snapshot(standing_grant(), (observed_descriptor(),))
    prior = deepcopy(selected)
    selected["source_vector_json"][0]["source_companies"] = [source_company()]
    entry = selected["source_vector_json"][0]
    if change == "projector":
        selected["projector_version"] = "fhir-catalog/v1"
    elif change == "transform":
        entry["extraction_transform"] = validation.TRANSFORM_VERSION
    elif change == "missing_labels":
        del entry["source_companies"]
    else:
        entry[{"status": "status", "generation": "source_generation", "source_id": "source_id"}[change]] = {
            "status": "last_good",
            "generation": "f" * 64,
            "source_id": "another-source",
        }[change]
    with pytest.raises(grants.FHIRCatalogGrantError, match="source_companies_history_changed"):
        grants.require_fhir_catalog_company_refresh_inputs(selected, prior)


@pytest.mark.asyncio
async def test_metadata_only_publication_requires_exact_previous_vector(monkeypatch):
    case = verifier_case(monkeypatch)
    prior = deepcopy(case.snapshot)
    prior["published_at"] = datetime(2026, 1, 1)
    case.snapshot["source_vector_json"][0]["source_companies"] = [source_company()]
    case.snapshot["metadata_generation"] = validation.metadata_generation(case.snapshot)
    writer = CatalogConnection((prior, case.snapshot), tuple(case.descriptors.values()))
    monkeypatch.setattr(grants, "validate_fhir_catalog_snapshot_record", AsyncMock())
    runtime = grants.build_fhir_catalog_publication_runtime(
        case.grant, case.pool, source_export=case.factory, source_companies_only=True
    )
    await runtime.authorize_metadata(
        command=command(case.grant, case.snapshot, previous=prior["metadata_generation"]),
        snapshot=case.snapshot,
        connection=writer,
    )
    with pytest.raises(grants.FHIRCatalogGrantError, match="source_companies_history_required"):
        await runtime.authorize_metadata(
            command=command(case.grant, case.snapshot), snapshot=case.snapshot, connection=writer
        )
    case.factory.assert_not_called()


@pytest.mark.asyncio
async def test_publication_cannot_drop_labels_from_a_native_retained_source(monkeypatch):
    selected = snapshot(standing_grant(), (observed_descriptor(),), status="last_good")
    prior = deepcopy(selected)
    prior["source_vector_json"][0]["source_companies"] = [source_company()]
    generation = selected["source_vector_json"][0]["source_generation"]
    monkeypatch.setattr(grants, "_protected_snapshot", AsyncMock(return_value=prior))
    with pytest.raises(grants.FHIRCatalogGrantError, match="source_companies_history_changed"):
        await grants._retained_sources(
            object(),
            command(standing_grant(), selected, previous=prior["metadata_generation"]),
            selected,
            standing_grant(),
            selected["source_vector_json"],
            {generation: {"profile": "retained_normalized"}},
        )
