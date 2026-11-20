# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Logical catalogue pairing and legacy SQL boundaries without database authority."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import registry_candidate_composition as composition
from process import registry_fhir_catalog_read as metadata
from process import registry_fhir_catalog_validation as validation
from process.network_provider_only_projection import _provider_only_binding_scope
from process.registry_effective_network_catalog import apply_effective_catalog_sql
from tests.test_registry_fhir_catalog_grant import snapshot, standing_grant


def test_candidate_identity_binds_prepared_catalogue_generation():
    generations_by_source = {}
    composition._bind_prepared_generations(
        generations_by_source,
        SimpleNamespace(generation_id="b" * 64),
        SimpleNamespace(generation_sha256="c" * 64),
        None,
        None,
        catalog_generation="a" * 64,
    )
    assert generations_by_source == {
        "fhir_catalog": "a" * 64,
        "custom_membership": "b" * 64,
        "unified_address": "c" * 64,
    }
    with pytest.raises(ValueError):
        composition._bind_prepared_generations(
            {},
            SimpleNamespace(generation_id="b" * 64),
            SimpleNamespace(generation_sha256="c" * 64),
            None,
            None,
            catalog_generation="invalid",
        )


def test_effective_relation_substitution_never_rewrites_its_own_native_sql():
    records = '(SELECT * FROM "example".registry_approved_record)'
    statement = (
        'SELECT * FROM ONLY "example".registry_approved_record a JOIN "example".registry_approved_record b ON true'
    )
    rewritten = apply_effective_catalog_sql(statement, records, "example")
    assert rewritten.count(records) == 2 and rewritten.count("(SELECT * FROM (SELECT") == 0
    assert apply_effective_catalog_sql(statement, None, "example") == statement


def test_legacy_provider_only_scope_remains_fhir_only():
    source, scope = _provider_only_binding_scope(False)
    assert source == "source.source_system='fhir'" and "uhc_plan_tier_v1" not in scope
    source, scope = _provider_only_binding_scope(True)
    assert "'aca','ptg'" in source and "source.fhir_scope_key IS NULL" in scope


@pytest.mark.parametrize("is_native", [False, True])
def test_retained_validation_changes_only_native_membership_association(is_native):
    snapshot_by_field = snapshot(standing_grant())
    if is_native:
        snapshot_by_field["accounting_json"] = {
            "imported_source_inventory": {
                "contract": "registry-imported-catalog-inputs-v1",
                "token": "e" * 64,
                "native_sources": [],
            }
        }
    snapshot_by_field["metadata_generation"] = validation.metadata_generation(snapshot_by_field)
    tables_by_name = {table: {"rows": 0, "sha256": "a" * 64, "batches": 0, "bytes": 0} for table in validation._TABLES}
    snapshot_by_field["accounting_json"] = {**snapshot_by_field.get("accounting_json", {}), **tables_by_name}
    validation_by_field = {
        "metadata_generation": snapshot_by_field["metadata_generation"],
        "inputs": validation.snapshot_inputs(snapshot_by_field),
        "tables": tables_by_name,
        "content_sha256": validation._content_digest(tables_by_name),
    }
    validation.verify_fhir_catalog_validation(snapshot_by_field, validation_by_field)
    attached_by_field = {**snapshot_by_field, "network_generation": 10, "manifest_sha256": "f" * 64}
    if is_native:
        validation.verify_fhir_catalog_validation(attached_by_field, validation_by_field)
        with pytest.raises(ValueError, match="inputs_changed"):
            validation.verify_fhir_catalog_validation(
                {**attached_by_field, "mapping_sha256": "d" * 64}, validation_by_field
            )
    else:
        with pytest.raises(ValueError, match="inputs_changed"):
            validation.verify_fhir_catalog_validation(attached_by_field, validation_by_field)


@pytest.mark.asyncio
async def test_retained_native_directory_pair_uses_immutable_manifest_token(monkeypatch):
    connection = SimpleNamespace(is_in_transaction=lambda: True, fetchval=AsyncMock(return_value=True))
    manifest = SimpleNamespace(
        generation_id=9,
        manifest_sha256="c" * 64,
        approved_custom_revision=0,
        source_generations={"fhir_catalog": "a" * 64},
    )
    snapshot_by_field = {
        "network_generation": 10,
        "manifest_sha256": "f" * 64,
        "metadata_generation": "a" * 64,
        "metadata_approved_revision": 0,
        "_directory_generation": 9,
    }
    monkeypatch.setattr(metadata, "read_published_api_pin", AsyncMock(return_value=(manifest, None, None)))
    query = metadata.FHIRCatalogQuery(generation_id=9, metadata_generation="a" * 64)
    assert await metadata._directory_manifest(connection, query, snapshot_by_field, "example") is manifest
    snapshot_by_field["metadata_generation"] = "b" * 64
    with pytest.raises(metadata.RegistryNetworkCatalogError, match="source_changed"):
        await metadata._directory_manifest(connection, query, snapshot_by_field, "example")
