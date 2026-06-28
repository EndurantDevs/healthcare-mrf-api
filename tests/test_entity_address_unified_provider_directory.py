# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import importlib


entity_address_unified = importlib.import_module("process.entity_address_unified")


def _provider_directory_available() -> dict[str, bool]:
    return {
        "provider_directory_practitioner": True,
        "provider_directory_organization": True,
        "provider_directory_location": True,
        "provider_directory_location.address_key": True,
        "provider_directory_practitioner_role": True,
        "provider_directory_organization_affiliation": True,
    }


def test_provider_directory_fhir_source_identity_bits_are_stable():
    assert "WHEN src = 'provider_directory_fhir' THEN 6" in entity_address_unified._source_priority_expr("src")
    assert "WHEN src = 'provider_directory_fhir' THEN 8" in entity_address_unified._source_id_expr("src")
    assert "WHEN src = 'provider_directory_fhir' THEN 128::bigint" in entity_address_unified._source_mask_expr("src")

    sql = entity_address_unified._enrich_raw_stage_sql("mrf", "entity_address_unified_raw")

    assert "source_id IN (1, 2, 3, 4, 5, 6, 8)" in sql


def test_source_selects_add_provider_directory_practitioner_and_organization_paths():
    selects = entity_address_unified._source_selects("mrf", _provider_directory_available())
    sql = "\n".join(selects)

    assert len(selects) == 2
    assert "mrf.provider_directory_practitioner_role AS role" in sql
    assert "mrf.provider_directory_practitioner AS practitioner" in sql
    assert "mrf.provider_directory_organization_affiliation AS affiliation" in sql
    assert "mrf.provider_directory_organization AS organization" in sql
    assert "mrf.provider_directory_location AS loc" in sql
    assert "COALESCE(role.location_refs::jsonb, '[]'::jsonb)" in sql
    assert "COALESCE(affiliation.location_refs::jsonb, '[]'::jsonb)" in sql
    assert "ORDER BY 1" in sql
    assert "'provider_directory_fhir'::varchar AS address_source" in sql
    assert "provider_directory_fhir:practitioner_role:" in sql
    assert "provider_directory_fhir:organization_affiliation:" in sql


def test_provider_directory_source_selects_keep_keyable_address_and_phone_filters():
    selects = entity_address_unified._source_selects("mrf", _provider_directory_available())
    sql = "\n".join(selects)

    assert "loc.first_line" in sql
    assert "loc.city_name" in sql
    assert "COALESCE(loc.state_name, loc.state_code)" in sql
    assert "loc.postal_code" in sql
    assert "loc.address_key ~* '^[0-9a-f]{8}-" in sql
    assert "COALESCE(role_phone.telephone_number, loc.telephone_number)::varchar AS telephone_number" in sql
    assert "loc.telephone_number::varchar AS telephone_number" in sql
    assert "pd.latitude::numeric BETWEEN -90 AND 90" in sql
    assert "pd.longitude::numeric BETWEEN -180 AND 180" in sql
    assert "practitioner.active IS DISTINCT FROM false" in sql
    assert "organization.active IS DISTINCT FROM false" in sql


def test_provider_directory_source_selects_are_guarded_by_table_availability():
    available = _provider_directory_available()
    available["provider_directory_location"] = False

    assert entity_address_unified._source_selects("mrf", available) == []


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
