# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Published retained recipes preserve capture identity and explicit policy."""

import copy
import json
from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process import registry_fhir_catalog_grant as grants
from process import registry_fhir_catalog_retained as retained
from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
from process.network_serving_read import PinnedNetworkServingManifest
from process.registry_fhir_catalog_extraction import extract_fhir_catalog_batch
from process.registry_fhir_catalog_projection import SourceCatalogContext
from process.registry_fhir_catalog_sources import (
    fhir_catalog_source_generation,
    validate_fhir_catalog_source_descriptor,
)
from process.registry_source_recipe_store import (
    RECIPE_DIGEST_KEY,
    RegistrySourceMembershipRecipe,
    canonical_registry_source_recipes,
    registry_source_recipes_sha256,
)
from tests.test_network_fhir_retained_capture import pin
from tests.test_network_fhir_retained_capture import retained as capture_fixture
from tests.test_registry_fhir_catalog_grant import CatalogConnection, command, standing_grant
from tests.test_registry_fhir_catalog_grant import custody as custody
from tests.test_registry_fhir_catalog_grant import snapshot as grant_snapshot
from tests.test_registry_fhir_catalog_projection import project
from tests.test_registry_fhir_catalog_sources import descriptor as published_descriptor


def snapshot(grant, descriptors, *, status="current"):
    source_entries = [
        {
            "source_generation": fhir_catalog_source_generation(descriptor),
            "source_id": descriptor["coordinates"]["source_id"],
            "status": status,
        }
        for descriptor in descriptors
    ]
    return grant_snapshot(
        grant, source_vector_json=sorted(source_entries, key=lambda entry: entry["source_generation"])
    )


def catalog_connection(selected, descriptor):
    connection = CatalogConnection((selected,))
    connection.descriptors = {fhir_catalog_source_generation(descriptor): descriptor}
    return connection


def recipe_fixture(value=None):
    value = capture_fixture() if value is None else value
    source = pin(value)
    coordinates = RegistryNetworkSourceCoordinates(
        "fhir", source.source_id, source.schema_name, source.dataset_id, retained.PRODUCER, value.capture_id
    )
    return RegistrySourceMembershipRecipe(source, coordinates)


def manifest_fixture(recipe):
    return PinnedNetworkServingManifest(
        9,
        "11111111-1111-4111-8111-111111111111",
        "serving_example",
        1,
        {RECIPE_DIGEST_KEY: registry_source_recipes_sha256(canonical_registry_source_recipes((recipe,)))},
        6,
        "a" * 64,
        90,
    )


async def descriptor_fixture(monkeypatch, *, source_uris=None, value=None):
    recipe = recipe_fixture(value)
    manifest = manifest_fixture(recipe)
    monkeypatch.setattr(retained, "resolve_retained_registry_source_recipes", AsyncMock(return_value=(recipe,)))
    connection = SimpleNamespace(fetchval=AsyncMock(return_value=104))
    (disposition,) = await retained.resolve_fhir_catalog_retained_sources(
        connection, manifest, control_schema="control_example", source_uris=source_uris
    )
    return disposition, recipe, manifest


async def test_retained_descriptor_preserves_origin_and_capture_edition_without_fabricated_uri(monkeypatch):
    disposition, recipe, _ = await descriptor_fixture(monkeypatch)
    descriptor = disposition.descriptor
    assert validate_fhir_catalog_source_descriptor(descriptor) == descriptor
    assert descriptor["coordinates"] == {
        "source_system": "fhir",
        "source_id": recipe.source_pin.source_id,
        "dataset_schema": recipe.source_pin.schema_name,
        "dataset_id": recipe.source_pin.dataset_id,
        "producer_id": retained.PRODUCER,
        "edition_id": recipe.source_pin.retained_capture.capture_id,
    }
    assert descriptor["native_read_profile"]["schema_name"] == recipe.source_pin.read_schema_name
    assert descriptor["resource_table_oid"] == recipe.source_pin.read_resource_table_oid
    assert descriptor["source_uri"] is None
    assert len(fhir_catalog_source_generation(descriptor)) == 64
    context = SourceCatalogContext(
        recipe.binding_coordinates,
        fhir_catalog_source_generation(descriptor),
        "f" * 64,
        disposition.accepted_at,
        descriptor["profile"],
        status="last_good",
        capture_sha256=descriptor["capture_sha256"],
    )
    binding = json.loads(project(context=context, relationships=()).binding_rows[0][-1])
    assert binding["edition_id"] == recipe.source_pin.retained_capture.capture_id
    assert binding["source_scope"] == {"organization_id": "network", "capture_sha256": descriptor["capture_sha256"]}
    assert binding["status"] == "last_good"


async def test_retained_generation_excludes_only_manifest_locator(monkeypatch):
    disposition, recipe, _ = await descriptor_fixture(monkeypatch)
    descriptor = disposition.descriptor
    generation = fhir_catalog_source_generation(descriptor)
    relocated = copy.deepcopy(descriptor)
    relocated["retained_manifest"].update(generation_id=10, manifest_sha256="e" * 64)
    assert fhir_catalog_source_generation(relocated) == generation
    changed, _, _ = await descriptor_fixture(monkeypatch)
    assert fhir_catalog_source_generation(changed.descriptor) != generation
    changed_recipe = replace(recipe, source_pin=replace(recipe.source_pin, as_of="2026-01-02"))
    changed_document = copy.deepcopy(descriptor)
    changed_document["retained_recipe"] = json.loads(canonical_registry_source_recipes((changed_recipe,)))
    assert fhir_catalog_source_generation(changed_document) != generation
    changed_uri = copy.deepcopy(descriptor)
    changed_uri["source_uri"] = "https://example.test/fhir/"
    assert fhir_catalog_source_generation(changed_uri) != generation
    native = published_descriptor()
    native_generation = fhir_catalog_source_generation(native)
    native["source_uri"] = "https://other.test/fhir/"
    assert fhir_catalog_source_generation(native) != native_generation


def test_missing_uri_is_only_accepted_for_retained_descriptors():
    descriptor = published_descriptor()
    descriptor["source_uri"] = None
    with pytest.raises(ValueError):
        validate_fhir_catalog_source_descriptor(descriptor)


@pytest.mark.parametrize("field", ["coordinates", "capture_sha256", "resource_table_oid", "retained_manifest"])
async def test_retained_descriptor_rejects_mixed_provenance(monkeypatch, field):
    disposition, _, _ = await descriptor_fixture(monkeypatch)
    descriptor = copy.deepcopy(disposition.descriptor)
    if field == "coordinates":
        descriptor[field]["edition_id"] = "another-edition"
    elif field == "retained_manifest":
        descriptor[field]["unknown"] = True
    elif field == "resource_table_oid":
        descriptor[field] += 1
    else:
        descriptor[field] = "f" * 64
    with pytest.raises(ValueError):
        validate_fhir_catalog_source_descriptor(descriptor)


async def test_absolute_network_reference_requires_trusted_endpoint_uri(monkeypatch):
    payload = {
        "resource_type": "InsurancePlan",
        "resource_id": "plan-one",
        "payload_hash": "c" * 64,
        "payload_json": {"network_refs": ["https://example.test/fhir/Organization/network-one"]},
    }
    missing, _, _ = await descriptor_fixture(monkeypatch)
    accepted, _, _ = await descriptor_fixture(monkeypatch, source_uris={"endpoint-one": "https://example.test/fhir"})
    unresolved = extract_fhir_catalog_batch((payload,), profile="retained_normalized", source_uri=None)
    resolved = extract_fhir_catalog_batch(
        (payload,), profile="retained_normalized", source_uri=accepted.descriptor["source_uri"]
    )
    assert missing.descriptor["source_uri"] is None
    assert unresolved.edges[0].network_resource_id is None
    assert unresolved.edges[0].resolution_reason == "reference_outside_source"
    assert resolved.edges[0].network_resource_id == "network-one"


async def test_published_manifest_and_recipe_are_rechecked(monkeypatch):
    disposition, recipe, manifest = await descriptor_fixture(monkeypatch)
    resolver = AsyncMock(return_value=manifest)
    monkeypatch.setattr(retained, "resolve_network_serving_manifest", resolver)
    assert await retained.require_retained_fhir_catalog_sources(
        object(), (disposition.descriptor,), control_schema="control_example"
    )
    assert resolver.await_args.kwargs["generation_id"] == manifest.generation_id
    retained.resolve_retained_registry_source_recipes.return_value = ()
    with pytest.raises(retained.FHIRCatalogRetainedError):
        await retained.require_retained_fhir_catalog_sources(
            object(), (disposition.descriptor,), control_schema="control_example"
        )
    retained.resolve_retained_registry_source_recipes.return_value = (recipe,)
    resolver.return_value = SimpleNamespace(generation_id=10)
    with pytest.raises(retained.FHIRCatalogRetainedError):
        await retained.require_retained_fhir_catalog_sources(
            object(), (disposition.descriptor,), control_schema="control_example"
        )


async def test_explicit_capture_is_available_without_directory_publication(monkeypatch):
    recipe = recipe_fixture()
    connection = SimpleNamespace(fetchval=AsyncMock(return_value=104))
    verifier = AsyncMock(return_value=recipe.source_pin.retained_capture)
    monkeypatch.setattr(retained, "require_retained_fhir_capture", verifier)
    (source,) = await retained.resolve_granted_fhir_catalog_sources(connection, (recipe,))
    assert source.descriptor["retained_manifest"] is None
    assert retained.retained_fhir_catalog_recipe(source.descriptor) == recipe
    assert await retained.require_retained_fhir_catalog_sources(
        connection, (source.descriptor,), control_schema="control_example"
    )
    assert verifier.await_count == 2
    assert all(call.kwargs == {"verify_content": False} for call in verifier.await_args_list)


async def test_explicit_capture_grant_is_exact_and_published_recipe_flag_cannot_substitute(custody, monkeypatch):
    recipe = recipe_fixture()
    connection = SimpleNamespace(fetchval=AsyncMock(return_value=104))
    monkeypatch.setattr(retained, "require_retained_fhir_capture", AsyncMock())
    (source,) = await retained.resolve_granted_fhir_catalog_sources(connection, (recipe,))
    verifier = AsyncMock(return_value=True)
    monkeypatch.setattr(grants, "require_retained_fhir_catalog_sources", verifier)
    altered = replace(recipe, source_pin=replace(recipe.source_pin, as_of="2026-01-02"))
    for configured, allowed in ((recipe, True), (altered, False), (None, False)):
        grant = standing_grant(
            retained_source_recipes=True,
            retained_sources=() if configured is None else (configured,),
        )
        assert grants.parse_fhir_catalog_standing_grant(grant.document()) == grant
        selected = snapshot(grant, (source.descriptor,), status="last_good")
        selected.update(network_generation=None, manifest_sha256=None)
        selected["metadata_generation"] = grants.metadata_generation(selected)
        runtime = grants.build_fhir_catalog_publication_runtime(grant, None)
        writer = catalog_connection(selected, source.descriptor)
        if allowed:
            assert (
                await runtime.authorize_metadata(command=command(grant, selected), snapshot=selected, connection=writer)
            )["authorized"]
        else:
            with pytest.raises(grants.FHIRCatalogGrantError, match="source_not_admitted"):
                await runtime.authorize_metadata(command=command(grant, selected), snapshot=selected, connection=writer)
    assert verifier.await_count == 1


def test_grant_explicitly_authorizes_retained_recipes_in_digest():
    disabled = standing_grant()
    enabled = standing_grant(retained_source_recipes=True)
    assert enabled.sha256 != disabled.sha256
    assert grants.parse_fhir_catalog_standing_grant(enabled.document()) == enabled
    legacy = disabled.document()
    legacy.pop("retained_source_recipes")
    legacy.pop("retained_source_uris")
    assert grants.parse_fhir_catalog_standing_grant(json.dumps(legacy)) == disabled
    with pytest.raises(grants.FHIRCatalogGrantError):
        standing_grant(retained_source_recipes=1)
    configured = standing_grant(retained_source_uris=(("endpoint-one", "https://example.test/fhir"),))
    normalized = standing_grant(retained_source_uris=(("endpoint-one", "https://example.test/fhir/"),))
    assert configured == normalized and configured.sha256 != disabled.sha256
    assert grants.parse_fhir_catalog_standing_grant(configured.document()) == configured
    retained_only = standing_grant(source_profiles=(), retained_source_recipes=True)
    assert grants.parse_fhir_catalog_standing_grant(retained_only.document()) == retained_only


@pytest.mark.parametrize("uri", ["https://example.test/fhir/", "https://other.test/fhir/", None])
async def test_retained_uri_must_match_trusted_grant_configuration(custody, monkeypatch, uri):
    disposition, _, _ = await descriptor_fixture(monkeypatch, source_uris={"endpoint-one": uri})
    grant = standing_grant(
        retained_source_recipes=True, retained_source_uris=(("endpoint-one", "https://example.test/fhir"),)
    )
    selected = snapshot(grant, (disposition.descriptor,), status="last_good")
    writer = catalog_connection(selected, disposition.descriptor)
    monkeypatch.setattr(grants, "require_retained_fhir_catalog_sources", AsyncMock(return_value=True))
    runtime = grants.build_fhir_catalog_publication_runtime(grant, None)
    if uri == "https://example.test/fhir/":
        assert (
            await runtime.authorize_metadata(command=command(grant, selected), snapshot=selected, connection=writer)
        )["authorized"]
    else:
        with pytest.raises(grants.FHIRCatalogGrantError, match="source_not_admitted"):
            await runtime.authorize_metadata(command=command(grant, selected), snapshot=selected, connection=writer)


@pytest.mark.parametrize("enabled", [False, True])
async def test_retained_bootstrap_requires_policy_and_published_recipe_not_prior_metadata(
    custody, monkeypatch, enabled
):
    disposition, _, _ = await descriptor_fixture(monkeypatch)
    grant = standing_grant(retained_source_recipes=enabled)
    selected = snapshot(grant, (disposition.descriptor,), status="last_good")
    writer = catalog_connection(selected, disposition.descriptor)
    verifier = AsyncMock(return_value=True)
    monkeypatch.setattr(grants, "require_retained_fhir_catalog_sources", verifier)
    runtime = grants.build_fhir_catalog_publication_runtime(grant, None)
    if enabled:
        assert (
            await runtime.authorize_metadata(command=command(grant, selected), snapshot=selected, connection=writer)
        )["authorized"]
        verifier.assert_awaited_once()
    else:
        with pytest.raises(grants.FHIRCatalogGrantError, match="source_not_admitted"):
            await runtime.authorize_metadata(command=command(grant, selected), snapshot=selected, connection=writer)
        verifier.assert_not_awaited()


async def test_retained_grant_keeps_capture_pin_through_publication(custody, monkeypatch):
    disposition, _, _ = await descriptor_fixture(monkeypatch)
    grant = standing_grant(retained_source_recipes=True)
    selected = snapshot(grant, (disposition.descriptor,), status="last_good")
    writer = catalog_connection(selected, disposition.descriptor)
    reader = Mock()
    reader.is_in_transaction.return_value = False
    transaction = Mock()
    transaction.start = AsyncMock(side_effect=lambda: reader.is_in_transaction.configure_mock(return_value=True))
    transaction.rollback = AsyncMock(side_effect=lambda: reader.is_in_transaction.configure_mock(return_value=False))
    reader.transaction.return_value = transaction

    @asynccontextmanager
    async def acquire():
        yield reader

    retained_read = SimpleNamespace(capture=object(), verify=AsyncMock())
    monkeypatch.setattr(grants, "_reader_identity", AsyncMock())
    monkeypatch.setattr(grants, "require_retained_fhir_catalog_sources", AsyncMock(return_value=True))
    pinner = AsyncMock(return_value=retained_read)
    monkeypatch.setattr(grants, "pin_retained_fhir_catalog_source", pinner)
    runtime = grants.build_fhir_catalog_publication_runtime(grant, SimpleNamespace(acquire=acquire))
    async with runtime.source_fence(snapshot=selected, connection=writer):
        await runtime.authorize_metadata(command=command(grant, selected), snapshot=selected, connection=writer)
        assert await runtime.verify_sources(snapshot=selected, connection=writer)
        assert reader.is_in_transaction()
    pinner.assert_awaited_once_with(reader, disposition.descriptor)
    retained_read.verify.assert_awaited_once_with(reader, retained_read.capture)
    assert not reader.is_in_transaction()


async def test_retained_capture_cannot_claim_current_source_freshness(custody, monkeypatch):
    disposition, _, _ = await descriptor_fixture(monkeypatch)
    grant = standing_grant(retained_source_recipes=True)
    selected = snapshot(grant, (disposition.descriptor,), status="current")
    writer = catalog_connection(selected, disposition.descriptor)
    runtime = grants.build_fhir_catalog_publication_runtime(grant, None)
    with pytest.raises(grants.FHIRCatalogGrantError, match="retained_freshness_invalid"):
        await runtime.authorize_metadata(command=command(grant, selected), snapshot=selected, connection=writer)
