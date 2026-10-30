# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Accepted generic family metadata and complete facts before a Network serving head."""

import json
from dataclasses import replace

import pytest

from process.network_fhir_membership_source import (
    FHIRMembershipSourceError,
    read_fhir_membership_fact_batch,
    with_fhir_successor,
)
from process.registry_membership_refresh_enrollment import require_complete_capture_enrollment
from process.registry_membership_refresh_generic import (
    observe_accepted_generic_recipes,
    resolve_accepted_generic_recipes,
)
from process.registry_membership_refresh_sources import _result
from process.registry_ptg_graph_reader import RegistryPTGGraphReadBudget
from process.registry_refresh_anchor_service import read_approved_refresh_bindings
from process.registry_source_recipe_composition import _require_recipe_custody
from tests import generic_fhir_capture_postgres_support as support
from tests.generic_fhir_capture_postgres_support import legacy_capture_db as legacy_capture_db
from tests.test_network_serving_schema_postgres import serving_schema as serving_schema
from tests.test_registry_generic_capture_continuation_postgres import _continue

pytestmark = pytest.mark.asyncio


async def _approved_bindings(driver, schema, approved, capture):
    row = await driver.fetchrow(
        f'SELECT record_key,record_revision,record_json::text FROM "{schema}".registry_approved_record '
        "WHERE approved_revision=$1 AND record_kind='network_binding'",
        approved.approved_revision,
    )
    record = json.loads(row["record_json"])
    scope_by_field = {
        "binding_id": row["record_key"],
        "binding_revision": row["record_revision"],
        "network_id": record["network_id"],
        "continuity_policy": "retained_successor",
        "source_scope": {
            **{
                key: record[key]
                for key in ("source_system", "source_id", "dataset_schema", "producer_id", "source_key")
            },
            "approved_binding_sha256": "0" * 64,
            "capture_generation": capture.capture_sha256,
        },
    }
    bindings = await read_approved_refresh_bindings(driver, approved, [scope_by_field], control_schema=schema)
    await require_complete_capture_enrollment(driver, bindings, approved, control_schema=schema)
    scope_by_field["source_scope"]["approved_binding_sha256"] = bindings[0].approved_binding_sha256
    return bindings, scope_by_field


@pytest.mark.parametrize("legacy_capture_db", ["modern"], indirect=True)
async def test_native_accepted_family_vector_and_facts_without_network_head(legacy_capture_db, serving_schema):
    fixture = legacy_capture_db
    capture = await support._capture(fixture)
    _, approved, network_id = await support._approved_capture_recipe(fixture, serving_schema, capture)
    admin, schema, _ = serving_schema
    reader = await fixture.connect("reader")
    async with reader.transaction(isolation="repeatable_read", readonly=True):
        bindings, enrollment = await _approved_bindings(reader, schema, approved, capture)
        scopes, recipes = await observe_accepted_generic_recipes(
            reader,
            bindings,
            read_budget=RegistryPTGGraphReadBudget(4 * 1024**2),
            control_schema=schema,
        )
        strict = await resolve_accepted_generic_recipes(
            reader,
            bindings,
            read_budget=RegistryPTGGraphReadBudget(4 * 1024**2),
            control_schema=schema,
        )
        assert strict == (scopes, recipes)
        assert scopes[bindings[0].binding_id] == enrollment["source_scope"]
        recipe = recipes[0]
        assert recipe.source_pin.retained_capture == capture and recipe.source_pin.retained_successor is None
        assert recipe.source_pin.as_of == capture.origin.semantic_projection_as_of
        vector = _result(recipes).accepted_sources
        assert vector == (
            {
                "source_id": fixture.origin.source_id,
                "capture_generation": capture.capture_sha256,
                "bindings_sha256": capture.content_sha256,
                "transform_version": "generic-fhir-source-facts-v1",
            },
        )
        batch = await read_fhir_membership_fact_batch(
            reader,
            recipe.source_pin,
            registry_schema=schema,
            approved_source=approved,
            binding_coordinates=recipe.binding_coordinates,
        )
        assert batch.membership_rows > 0 and batch.unresolved_rows == 0
        assert {member["network_id"] for member in json.loads(batch.input_bytes)} == {network_id}
        for fact in json.loads(batch.source_fact_bytes):
            if fact["omission_reason"] is None:
                assert fact["source_scope_json"]["capture_sha256"] == capture.capture_sha256
                assert fact["evidence_basis"][:3] == [
                    fixture.origin.dataset_id,
                    fixture.origin.source_id,
                    capture.release_id,
                ]
    assert await admin.fetchval(f'SELECT generation_id FROM "{schema}".network_serving_control') is None


@pytest.mark.parametrize("legacy_capture_db", ["modern"], indirect=True)
async def test_native_two_captures_preserve_original_scope_and_physical_evidence(legacy_capture_db, serving_schema):
    """Qualify two real retained captures; this does not claim a newer Source publication."""
    fixture = legacy_capture_db
    original = await support._capture(fixture)
    recipe, approved, _ = await support._approved_capture_recipe(fixture, serving_schema, original)
    physical = await _continue(fixture)
    physical_pin = replace(
        recipe.source_pin,
        retained_capture=physical,
        release_id=physical.release_id,
        as_of=physical.origin.semantic_projection_as_of,
    )
    wrapped = replace(recipe, source_pin=with_fhir_successor(recipe.source_pin, physical_pin))
    reader = await fixture.connect("reader")
    _, schema, _ = serving_schema
    async with reader.transaction(isolation="repeatable_read", readonly=True):
        reads = await _require_recipe_custody(reader, wrapped)
        batch = await read_fhir_membership_fact_batch(
            reader,
            wrapped.source_pin,
            registry_schema=schema,
            approved_source=approved,
            binding_coordinates=wrapped.binding_coordinates,
            retained_read=reads,
        )
        assert batch.membership_rows > 0 and batch.unresolved_rows == 0
        assert original.capture_sha256 != physical.capture_sha256
        for fact in json.loads(batch.source_fact_bytes):
            if fact["omission_reason"] is None:
                assert fact["source_scope_json"]["capture_sha256"] == original.capture_sha256
                assert fact["evidence_basis"][:3] == [
                    physical.origin.dataset_id,
                    physical.origin.source_id,
                    physical.release_id,
                ]
        with pytest.raises(FHIRMembershipSourceError):
            await read_fhir_membership_fact_batch(
                reader,
                wrapped.source_pin,
                registry_schema=schema,
                approved_source=approved,
                binding_coordinates=wrapped.binding_coordinates,
                retained_read=reads.physical,
            )
