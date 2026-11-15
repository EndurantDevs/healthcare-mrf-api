# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pure synthetic source recipe construction shared by host and native tests."""

import hashlib
import json
from dataclasses import replace
from uuid import UUID

from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
from process.network_fhir_membership_source import PinnedFHIRMembershipSource, fhir_physical_source, with_fhir_successor
from process.network_fhir_retained_capture_contract import SEMANTIC_PROFILE, capture_digest
from process.registry_source_contribution_store import fhir_fact_transform, recipe_contribution_identity
from process.registry_source_recipe_store import RegistrySourceMembershipRecipe, canonical_registry_source_recipes
from tests.test_network_fhir_retained_capture import pin, retained
from tests.test_registry_source_contributions import _descriptor as _legacy_descriptor
from tests.test_registry_source_office_summary_v3 import _derivation as _office_example


def _example(index=0):
    source = PinnedFHIRMembershipSource(
        "source_example",
        "source-example",
        "endpoint-example",
        f"dataset-{index}",
        "a" * 64,
        "release-one",
        123,
        "medical",
        "2026-10-01",
    )
    coordinates = RegistryNetworkSourceCoordinates(
        "fhir",
        source.source_id,
        source.schema_name,
        source.dataset_id,
        "producer-example",
        "edition-one",
    )
    return RegistrySourceMembershipRecipe(source, coordinates)


def _descriptor(recipe, anchored=False):
    identity, capture = recipe_contribution_identity(recipe)
    descriptor = _legacy_descriptor(recipe)
    descriptor.update(
        recipe_sha256=identity,
        capture_sha256=capture,
        transform_version=fhir_fact_transform(recipe.source_pin),
        fact_candidate_id=str(UUID(int=7)) if anchored else None,
        fact_schema_name="original_facts" if anchored else None,
        fact_table_name="network_source_contribution" if anchored else None,
        fact_generation_id="d" * 64 if anchored else None,
    )
    descriptor["source_generation"] = hashlib.sha256(
        json.dumps([capture, descriptor["mapping_sha256"]], sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    return descriptor


def _derivation(recipe, descriptor=None, *, facts=False):
    identity = hashlib.sha256(canonical_registry_source_recipes((recipe,)).encode()).hexdigest()
    derivation = _office_example(identity, mode="facts" if facts else "raw")
    physical = (
        fhir_physical_source(recipe.source_pin)
        if type(recipe.source_pin) is PinnedFHIRMembershipSource
        else recipe.source_pin
    )
    derivation["physical_source_sha256"] = physical.generation_id
    if descriptor is not None:
        derivation["mapping_sha256"] = descriptor["mapping_sha256"]
    if facts:
        derivation["fact_artifact"] = {
            field: descriptor[field]
            for field in (
                "fact_candidate_id",
                "fact_schema_name",
                "fact_table_name",
                "fact_generation_id",
                "capture_sha256",
                "transform_version",
                "fact_sha256",
                "fact_rows",
            )
        }
    return derivation


def _capture_recipe():
    captures = []
    for dataset in ("dataset-one", "dataset-two"):
        capture = retained()
        origin = replace(
            capture.origin,
            dataset_id=dataset,
            admission_profile=SEMANTIC_PROFILE,
            original_admission_refusal=None,
            admission_metadata_sha256="1" * 64,
            semantic_projection_as_of="2026-01-01",
            descriptor_counts=capture.origin.resource_counts,
        )
        capture = replace(capture, origin=origin)
        captures.append(replace(capture, capture_sha256=capture_digest(capture)))
    source = with_fhir_successor(pin(captures[0]), pin(captures[1]))
    coordinates = RegistryNetworkSourceCoordinates(
        "fhir", source.source_id, source.schema_name, source.dataset_id, "fixture-producer", captures[0].capture_id
    )
    return RegistrySourceMembershipRecipe(source, coordinates)
