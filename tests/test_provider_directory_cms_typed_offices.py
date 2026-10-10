# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Typed CMS offices share source selection without borrowing NPI identity."""

import importlib
from dataclasses import replace

import pytest

from process import entity_address_candidate_preparation as preparation

native = importlib.import_module("process.entity_address_unified")
from process.provider_directory_cms_overlay_projection import DesiredOverlayProjection
from process.provider_directory_cms_typed_offices import CMS_OFFICE_READ_TABLES, cms_typed_office_source


def _available():
    return {
        name: True
        for name in (
            *native.PROVIDER_DIRECTORY_DATASET_FENCE_TABLES,
            *CMS_OFFICE_READ_TABLES,
            "provider_directory_location",
        )
    }


def _inputs():
    return preparation.ProviderDirectoryAddressPreparationInput(
        (preparation.ProviderDirectoryAddressDatasetPin("cms-npd", "endpoint", "candidate", "a" * 64, "root"),),
        "desired_overlay",
        41,
        0,
        (("provider_directory_location", "desired_locations"),),
        "2026-01-02",
    )


def test_physical_source_binds_exact_provider_site_and_full_address_before_rows():
    token = preparation._PREPARATION.set(_inputs())
    try:
        source_selects = native._current_provider_directory_source_selects("fixture", _available(), [])
    finally:
        preparation._PREPARATION.reset(token)
    assert len(source_selects) == 2
    sql = source_selects[1]
    for required in (
        "proof.dataset_hash=dataset.dataset_hash AND proof.proof_version=2",
        "page_witness.release_id=page.release_id",
        "provider_witness.normalized_payload_hash=provider.payload_hash",
        "(page.payload->>'source_id' IS NULL OR page.payload->>'source_id'='cms-npd')",
        "site.resource_id=location_witness.resource_id",
        "physical.last_seen_run_id=selected.run_id",
        "fixture.desired_locations physical",
        "'provider_directory'::varchar AS entity_type",
        "NULL::bigint AS npi",
        "NULL::bigint AS inferred_npi",
        "'provider_directory_fhir:cms_typed:cms-npd:'",
        "native_address_key::uuid AS address_key",
        "native_address_key IS NULL OR NOT pg_input_is_valid(native_address_key,'uuid')",
        "OFFSET (SELECT accepted FROM complete)",
        "left(page.payload->>'period_start',10)<='2026-01-02'",
    ):
        assert required in sql
    for field in ("first_line", "second_line", "city_name", "state_name", "postal_code"):
        assert field + " IS DISTINCT FROM office->>'" + field + "'" in sql
    assert "country_code,''),'US') IS DISTINCT FROM" in sql
    assert "addr_key_v1" not in sql and "LIKE" not in sql


def test_virtual_observation_counts_same_typed_refs_without_asserting_native_keys():
    inputs = _inputs()
    query = preparation.ProviderDirectoryAddressSourceQueryInput(
        inputs.dataset_pins,
        inputs.relation_overrides,
        inputs.semantic_as_of,
        DesiredOverlayProjection("SELECT 1", "a" * 64, "b" * 64),
    )
    available_by_name = _available()
    available_by_name.pop("provider_directory_location")
    with preparation.source_query_scope(query):
        source_selects = native._current_provider_directory_source_selects("fixture", available_by_name, [])
    sql = source_selects[1]
    assert "NULL::uuid AS address_key" in sql and "native_address_key IS NULL" not in sql
    assert "OR false AS invalid" in sql
    assert "provider.resource_id=CASE" in sql and "reference ~ '^Location/" in sql
    assert "WHERE coalesce(page.payload->>'npi',provider.payload_json::jsonb->>'npi') IS NULL" in sql
    assert "NOT jsonb_path_exists(provider_witness.raw_payload_json" in sql
    assert native._insert_raw_from_source_sql("fixture", "raw_stage", sql)


def test_legacy_non_cms_sources_do_not_acquire_new_requirements():
    assert cms_typed_office_source(native, "fixture", {}, source_ids=("other-source",)) is None
    assert cms_typed_office_source(native, "fixture", {}) is None


def test_missing_cms_read_catalog_refuses_preparation_without_fallback():
    token = preparation._PREPARATION.set(_inputs())
    try:
        with pytest.raises(RuntimeError, match="read_inputs_unavailable"):
            cms_typed_office_source(native, "fixture", {})
    finally:
        preparation._PREPARATION.reset(token)


def test_partial_refresh_refuses_existing_typed_offices_instead_of_preserving_stale_rows():
    token = preparation._PREPARATION.set(replace(_inputs(), semantic_as_of="2026-01-03"))
    try:
        sql = cms_typed_office_source(native, "fixture", _available(), partial_refresh=True)
    finally:
        preparation._PREPARATION.reset(token)
    assert "OR EXISTS(SELECT 1 FROM checked)" in sql and "OFFSET (SELECT accepted FROM complete)" in sql
