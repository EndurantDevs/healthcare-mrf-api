# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import importlib
from unittest.mock import AsyncMock

import pytest


entity_address_unified = importlib.import_module("process.entity_address_unified")


def _provider_directory_available() -> dict[str, bool]:
    return {
        "provider_directory_practitioner": True,
        "provider_directory_organization": True,
        "provider_directory_location": True,
        "provider_directory_location.address_key": True,
        "provider_directory_practitioner_role": True,
        "provider_directory_insurance_plan": True,
        "provider_directory_network_catalog": True,
        "provider_directory_healthcare_service": True,
        "provider_directory_organization_affiliation": True,
    }


def _provider_directory_fenced_available() -> dict[str, bool]:
    available = _provider_directory_available()
    available.update(
        {
            "provider_directory_source": True,
            "provider_directory_address_overlay": True,
            "provider_directory_endpoint_dataset": True,
            "provider_directory_dataset_resource": True,
        }
    )
    return available


def test_provider_directory_fhir_source_identity_bits_are_stable():
    assert "WHEN src = 'provider_directory_fhir' THEN 6" in entity_address_unified._source_priority_expr("src")
    assert "WHEN src = 'provider_directory_fhir' THEN 8" in entity_address_unified._source_id_expr("src")
    assert "WHEN src = 'provider_directory_fhir' THEN 128::bigint" in entity_address_unified._source_mask_expr("src")

    sql = entity_address_unified._enrich_raw_stage_sql("mrf", "entity_address_unified_raw")

    assert "source_id IN (1, 2, 3, 4, 5, 6, 8)" in sql


def test_source_selects_add_provider_directory_practitioner_and_organization_paths():
    selects = entity_address_unified._source_selects("mrf", _provider_directory_available())
    sql = "\n".join(selects)

    assert len(selects) == 3
    assert "mrf.provider_directory_practitioner_role AS role" in sql
    assert "mrf.provider_directory_practitioner AS practitioner" in sql
    assert "mrf.provider_directory_organization_affiliation AS affiliation" in sql
    assert "mrf.provider_directory_organization AS organization" in sql
    assert "mrf.provider_directory_location AS loc" in sql
    assert "COALESCE(role.location_refs::jsonb, '[]'::jsonb)" in sql
    assert "COALESCE(affiliation.location_refs::jsonb, '[]'::jsonb)" in sql
    assert "mrf.provider_directory_healthcare_service AS healthcare_service" in sql
    assert "COALESCE(role.healthcare_service_refs::jsonb, '[]'::jsonb)" in sql
    assert "COALESCE(affiliation.healthcare_service_refs::jsonb, '[]'::jsonb)" in sql
    assert "COALESCE(healthcare_service.location_refs::jsonb, '[]'::jsonb)" in sql
    assert "healthcare_service.resource_id = service_ref_id.resource_id" in sql
    assert "healthcare_service.active IS DISTINCT FROM false" in sql
    assert "ORDER BY plan_identifier" in sql
    assert "ORDER BY network_name" in sql
    assert "'provider_directory_fhir'::varchar AS address_source" in sql
    assert "provider_directory_fhir:practitioner_role:" in sql
    assert "provider_directory_fhir:organization_affiliation:" in sql
    assert "provider_directory_fhir:organization_address:" in sql
    assert "jsonb_array_elements(\n                        COALESCE(organization.address_json::jsonb" in sql


def test_practitioner_role_resolves_plan_identifiers_and_network_organizations():
    selects = entity_address_unified._source_selects("mrf", _provider_directory_available())
    sql = selects[0]

    assert "COALESCE(role.insurance_plan_refs::jsonb, '[]'::jsonb)" in sql
    assert "JOIN mrf.provider_directory_insurance_plan AS insurance_plan" in sql
    assert "insurance_plan.source_id = role.source_id" in sql
    assert "insurance_plan.resource_id = NULLIF(BTRIM(CASE" in sql
    assert "COALESCE(insurance_plan.network_refs::jsonb, '[]'::jsonb)" in sql
    assert "LEFT JOIN mrf.provider_directory_network_catalog AS network_catalog" in sql
    assert "network_catalog.network_resource_id = network_ref.network_resource_id" in sql
    assert "LEFT JOIN mrf.provider_directory_organization AS network_organization" in sql
    assert "COALESCE(network_catalog.provider_directory_network_name, network_organization.name)" in sql
    assert "COALESCE(pd.plan_identifiers, ARRAY[]::varchar[])::varchar[] AS aca_plan_array" in sql
    assert "COALESCE(pd.network_names, ARRAY[]::varchar[])::varchar[] AS aca_network_array" in sql
    assert "network_ref.value::varchar" not in sql.split("AS aca_network_array", 1)[0][-500:]


def test_practitioner_role_network_resolution_falls_back_to_organization_table():
    available = _provider_directory_available()
    available["provider_directory_network_catalog"] = False

    sql = entity_address_unified._source_selects("mrf", available)[0]

    assert "provider_directory_network_catalog" not in sql
    assert "LEFT JOIN mrf.provider_directory_organization AS network_organization" in sql
    assert "NULLIF(BTRIM(COALESCE(network_organization.name)), '')" in sql


def test_provider_directory_source_selects_keep_keyable_address_and_phone_filters():
    selects = entity_address_unified._source_selects("mrf", _provider_directory_available())
    sql = "\n".join(selects)

    assert "loc.first_line" in sql
    assert "loc.city_name" in sql
    assert "COALESCE(loc.state_name, loc.state_code)" in sql
    assert "loc.postal_code" in sql
    assert "loc.address_key ~* '^[0-9a-f]{8}-" in sql
    assert (
        "WHEN loc.phone_number IS NOT NULL THEN loc.telephone_number" in sql
    )
    assert "role_phone.telephone_number" in sql
    assert "loc.telephone_number::varchar AS telephone_number" in sql
    assert "pd.latitude" in sql
    assert "pd.longitude" in sql
    assert "/ 1000000" in sql
    assert "ABS((pd.latitude)::numeric) < 0.0000001" in sql
    assert "BETWEEN 24 AND 50" in sql
    assert "practitioner.active IS DISTINCT FROM false" in sql
    assert "organization.active IS DISTINCT FROM false" in sql
    assert "NULLIF(TRIM(addr.value->'line'->>0), '')::varchar AS first_line" in sql
    assert "NULLIF(TRIM(addr.value->>'postalCode'), '')::varchar AS postal_code" in sql
    assert "pd.address_key AS address_key" in sql
    assert "mrf.addr_key_v1(" in sql
    assert "pd.first_line, pd.second_line, pd.city_name" in sql


def test_same_provider_address_backfill_only_uses_same_npi_and_address_key():
    sql = entity_address_unified._backfill_same_provider_address_fields_sql(
        "mrf",
        "entity_address_unified_stage_test",
    )

    assert "COALESCE(npi, inferred_npi" in sql
    assert "COALESCE(target_row.npi, target_row.inferred_npi" in sql
    assert "GROUP BY provider_npi, address_key" in sql
    assert "target_row.address_key = grouped_fields.address_key" in sql
    assert "= grouped_fields.provider_npi" in sql
    assert "telephone_number = COALESCE(target_row.telephone_number, grouped_fields.telephone_number)" in sql
    assert "phone_number = COALESCE(target_row.phone_number, grouped_fields.phone_number)" in sql
    assert "ABS(target_row.lat) < 0.0000001" in sql


def test_provider_directory_source_selects_precompute_primary_npi_attributes():
    available = _provider_directory_available()
    available["npi_address"] = True
    selects = entity_address_unified._source_selects("mrf", available)
    sql = "\n".join(selects)

    assert "provider_directory_primary_npi_address AS MATERIALIZED" in sql
    assert "SELECT DISTINCT ON (pa.npi)" in sql
    assert "SELECT DISTINCT provider_npi" in sql
    assert "FROM provider_directory_practitioner_locations" in sql
    assert "FROM provider_directory_organization_locations" in sql
    assert "FROM provider_directory_organization_addresses" in sql
    assert "LEFT JOIN provider_directory_primary_npi_address AS pa ON pa.npi = pd.provider_npi" in sql
    assert "FROM mrf.npi_address AS pa WHERE pa.npi = provider_npi" not in sql


def test_provider_directory_source_selects_normalize_fhir_refs_before_joining():
    selects = entity_address_unified._source_selects("mrf", _provider_directory_available())
    sql = "\n".join(selects)

    assert "practitioner.resource_id = NULLIF(" in sql
    assert "loc.resource_id = location_ref_id.resource_id" in sql
    assert "organization.resource_id = organization_ref.resource_id" in sql
    assert "SELECT DISTINCT normalized_ref AS resource_id" in sql
    assert "regexp_replace(COALESCE(role.practitioner_ref, ''), '^.*/', '')" in sql
    assert "role.practitioner_ref IN (" not in sql
    assert "location_ref.value IN (loc.resource_id" not in sql
    assert "OR affiliation.participating_organization_ref IN" not in sql


def test_provider_directory_source_selects_keep_direct_locations_without_healthcare_service_table():
    available = _provider_directory_available()
    available["provider_directory_healthcare_service"] = False
    selects = entity_address_unified._source_selects("mrf", available)
    sql = "\n".join(selects)

    assert "mrf.provider_directory_healthcare_service AS healthcare_service" not in sql
    assert "COALESCE(role.location_refs::jsonb, '[]'::jsonb)" in sql
    assert "COALESCE(affiliation.location_refs::jsonb, '[]'::jsonb)" in sql
    assert "COALESCE(role.healthcare_service_refs::jsonb" not in sql
    assert "COALESCE(affiliation.healthcare_service_refs::jsonb" not in sql


def test_provider_directory_partial_scope_selects_only_current_published_datasets():
    sql = entity_address_unified._latest_provider_directory_partial_scope_sql("mrf")

    assert "mrf.provider_directory_address_overlay AS overlay" in sql
    assert "dataset.is_current IS TRUE" in sql
    assert "dataset.status = 'published'" in sql
    assert "dataset.published_at IS NOT NULL" in sql
    assert "dataset.superseded_at IS NULL" in sql
    assert "HAVING COUNT(*) = 1" in sql
    assert "ORDER BY dataset.created_at" not in sql


@pytest.mark.asyncio
async def test_provider_directory_partial_scope_index_preflight_accepts_valid_index(monkeypatch):
    scalar = AsyncMock(return_value=True)
    monkeypatch.setattr(entity_address_unified.db, "scalar", scalar)

    await entity_address_unified._preflight_provider_directory_partial_scope_index("mrf")

    sql = scalar.await_args.args[0]
    assert "FROM pg_catalog.pg_index AS index_meta" in sql
    assert "index_meta.indisvalid IS TRUE" in sql
    assert "index_meta.indisready IS TRUE" in sql
    assert "provider_directory_address_overlay_source_run_resource_idx" in sql
    assert "ARRAY['source_id', 'last_seen_run_id', 'resource_type', 'resource_id']" in sql
    assert "CREATE INDEX" not in sql


@pytest.mark.asyncio
async def test_provider_directory_partial_scope_index_preflight_rejects_missing_index(monkeypatch):
    monkeypatch.setattr(entity_address_unified.db, "scalar", AsyncMock(return_value=False))

    with pytest.raises(RuntimeError) as exc_info:
        await entity_address_unified._preflight_provider_directory_partial_scope_index("mrf")

    message = str(exc_info.value)
    assert "mrf.provider_directory_address_overlay_source_run_resource_idx" in message
    assert "artifact publication" in message
    assert "repair the index online outside the import path" in message


def test_full_projection_replaces_compatibility_fhir_with_current_overlay():
    available = _provider_directory_fenced_available()
    compatibility_selects = entity_address_unified._source_selects("mrf", available)

    source_selects = entity_address_unified._current_provider_directory_source_selects(
        "mrf",
        available,
        compatibility_selects,
        test_limit_per_source=17,
    )
    sql = "\n".join(source_selects)

    assert sql.count("FROM mrf.provider_directory_address_overlay AS overlay") == 1
    assert "FROM mrf.provider_directory_practitioner_role AS role" not in sql
    assert "FROM mrf.provider_directory_organization_affiliation AS affiliation" not in sql
    assert "FROM mrf.provider_directory_organization AS organization" not in sql
    assert "WHERE source.endpoint_id IS NOT NULL" in sql
    assert "source.source_id = ANY(" not in sql
    assert "dataset.is_current IS TRUE" in sql
    assert "dataset.status = 'published'" in sql
    assert "dataset.published_at IS NOT NULL" in sql
    assert "overlay.last_seen_run_id = dataset.run_id" in sql
    assert "dataset_resource.resource_id = overlay.resource_id" in sql
    assert "LIMIT 17" in sql


def test_full_projection_fails_closed_without_dataset_fence():
    available = _provider_directory_fenced_available()
    available["provider_directory_dataset_resource"] = False
    compatibility_selects = entity_address_unified._source_selects("mrf", available)

    with pytest.raises(RuntimeError, match="current dataset fence relations"):
        entity_address_unified._current_provider_directory_source_selects(
            "mrf",
            available,
            compatibility_selects,
            has_compatibility_data=True,
            partial_refresh=False,
        )


def test_provider_directory_partial_overlay_excludes_stale_rows():
    sql = entity_address_unified._provider_directory_partial_overlay_source_select(
        "mrf",
        _provider_directory_available(),
    )

    assert "overlay.last_seen_run_id = dataset.run_id" in sql
    assert "COALESCE(dataset.acquisition_root_run_id, dataset.import_run_id)" in sql
    assert "current_overlay AS MATERIALIZED" in sql


def test_provider_directory_partial_overlay_requires_dataset_resource_membership():
    sql = entity_address_unified._provider_directory_partial_overlay_source_select(
        "mrf",
        _provider_directory_available(),
    )

    assert "FROM mrf.provider_directory_dataset_resource AS dataset_resource" in sql
    assert "dataset_resource.dataset_id = dataset.dataset_id" in sql
    assert "dataset_resource.resource_type = overlay.resource_type" in sql
    assert "dataset_resource.resource_id = overlay.resource_id" in sql


def test_provider_directory_partial_overlay_expands_selected_aliases_to_endpoint_siblings():
    sql = entity_address_unified._provider_directory_partial_overlay_source_select(
        "mrf",
        _provider_directory_available(),
        source_ids=["alias_a"],
    )
    live_filter = entity_address_unified._provider_directory_live_source_filter_sql(
        "mrf",
        "live",
        source_ids=["alias_a"],
    )

    assert "source.source_id = ANY(ARRAY['alias_a']::varchar[])" in sql
    assert "endpoint_aliases AS MATERIALIZED" in sql
    assert "selected_endpoint.endpoint_id = sibling.endpoint_id" in sql
    assert "selected_source.endpoint_id = sibling_source.endpoint_id" in live_filter


def test_provider_directory_partial_live_filter_preserves_unrelated_sources():
    live_filter = entity_address_unified._provider_directory_live_source_filter_sql(
        "mrf",
        "live",
        source_ids=["alias_a"],
    )

    assert "sibling_source.endpoint_id IS NOT NULL" in live_filter
    assert "selected_source.source_id = ANY(ARRAY['alias_a']::varchar[])" in live_filter
    assert "OR NOT EXISTS" not in live_filter


def test_provider_directory_source_selects_can_scope_by_source_and_run():
    selects = entity_address_unified._source_selects(
        "mrf",
        _provider_directory_available(),
        provider_directory_source_ids=["source_a", "source_b"],
        provider_directory_run_id="run_123",
    )
    sql = "\n".join(selects)

    assert "role.source_id = ANY(ARRAY['source_a', 'source_b']::varchar[])" in sql
    assert "role.last_seen_run_id = 'run_123'" in sql
    assert "affiliation.source_id = ANY(ARRAY['source_a', 'source_b']::varchar[])" in sql
    assert "affiliation.last_seen_run_id = 'run_123'" in sql
    assert "organization.source_id = ANY(ARRAY['source_a', 'source_b']::varchar[])" in sql
    assert "organization.last_seen_run_id = 'run_123'" in sql


def test_provider_directory_source_id_batches_are_bounded():
    assert entity_address_unified._provider_directory_source_id_batches(
        ["source_a", "source_b", "source_c", "source_d", "source_e"],
        2,
    ) == [["source_a", "source_b"], ["source_c", "source_d"], ["source_e"]]
    assert entity_address_unified._provider_directory_source_id_batches(
        ["source_a", "source_b"],
        0,
    ) == [["source_a", "source_b"]]
    assert entity_address_unified._provider_directory_source_id_batches(
        [],
        2,
    ) == [[]]


def test_provider_directory_source_batch_size_accepts_task_and_env(monkeypatch):
    monkeypatch.delenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_PROVIDER_DIRECTORY_SOURCE_BATCH_SIZE", raising=False)
    assert (
        entity_address_unified._entity_address_provider_directory_source_batch_size({})  # pylint: disable=protected-access
        == 100
    )

    assert (
        entity_address_unified._entity_address_provider_directory_source_batch_size(
            {"provider_directory_source_batch_size": 4}
        )
        == 4
    )

    monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_PROVIDER_DIRECTORY_SOURCE_BATCH_SIZE", "5")
    assert (
        entity_address_unified._entity_address_provider_directory_source_batch_size({})
        == 5
    )


def test_provider_directory_source_selects_are_guarded_by_table_availability():
    available = _provider_directory_available()
    available["provider_directory_location"] = False

    selects = entity_address_unified._source_selects("mrf", available)

    assert len(selects) == 1
    assert "provider_directory_fhir:organization_address:" in selects[0]
    assert "provider_directory_practitioner_role AS role" not in selects[0]
    assert "provider_directory_organization_affiliation AS affiliation" not in selects[0]


def test_serving_only_refresh_task_bool_overrides_env(monkeypatch):
    monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_SERVING_ONLY", "false")

    assert (
        entity_address_unified._task_bool_or_env(
            {"serving_only_refresh": True},
            "serving_only_refresh",
            "HLTHPRT_ENTITY_ADDRESS_UNIFIED_SERVING_ONLY",
            False,
        )
        is True
    )
    assert (
        entity_address_unified._task_bool_or_env(
            {},
            "serving_only_refresh",
            "HLTHPRT_ENTITY_ADDRESS_UNIFIED_SERVING_ONLY",
            False,
        )
        is False
    )
