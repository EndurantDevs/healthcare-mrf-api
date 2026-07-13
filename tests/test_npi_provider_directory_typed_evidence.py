# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from api.endpoint import npi as npi_module


def _typed_role_evidence_map():
    """Build one exact keyed PractitionerRole evidence row."""
    profiles = [f"https://example.test/profile/{number}" for number in range(40)]
    role_evidence_map = {
        "source_id": "pdfhir_aetna",
        "role_id": "role-100",
        "evidence_type": "role",
        "resource_id": "role-100",
        "identifier": None,
        "name": None,
        "reference": None,
        "provenance": "provider_directory_practitioner_role",
        "role_active": True,
        "role_organization_ref": "Organization/org-1",
        "role_healthcare_service_refs": ["HealthcareService/service-1"],
        "role_endpoint_refs": ["Endpoint/endpoint-1"],
        "role_specialty_codes": [{"code": "207Q00000X"}],
        "role_code_codes": [{"code": "primary-care"}],
        "role_telecom": [{"system": "phone", "value": "555-0100"}],
        "role_period_start": "2026-01-01",
        "role_period_end": "2026-12-31",
        "role_available_time": [{"daysOfWeek": ["mon"], "availableStartTime": "09:00"}],
        "role_not_available": [{"description": "Holiday"}],
        "role_availability_exceptions": "Closed federal holidays",
        "role_new_patient_acceptance": {"code": "accepting"},
        "role_telehealth": True,
        "role_fhir_meta": {
            "versionId": "7",
            "source": "https://user:secret@example.test/fhir?_token=secret",
            "profile": profiles,
            "security": [
                {
                    "system": "https://example.test/security",
                    "code": "restricted",
                    "display": "x" * 4096,
                    "ignored": "not exposed",
                }
            ] * 40,
            "tag": [{"code": "directory", "userSelected": True}],
            "ignored": "not exposed",
        },
        "role_fhir_self_url": "https://example.test/fhir/PractitionerRole/role-100",
        "role_fhir_fetch_mode": "bundle",
        "plan_returned": 1,
        "plan_total": 1,
        "plan_truncated": False,
        "catalog_complete": True,
    }
    return role_evidence_map


def _typed_plan_evidence_map():
    """Build one exact keyed name-only InsurancePlan evidence row."""
    return {
        "source_id": "pdfhir_aetna",
        "role_id": "role-100",
        "evidence_type": "insurance_plan",
        "resource_id": "plan-name-only",
        "identifier": None,
        "name": None,
        "reference": None,
        "provenance": "network-derived",
        "plan_status": "active",
        "plan_name": "Example Choice",
        "plan_aliases": ["Choice"],
        "plan_type_codes": [{"code": "medical"}],
        "plan_owned_by_ref": "Organization/owner-1",
        "plan_administered_by_ref": "Organization/admin-1",
        "plan_period_start": "2026-01-01",
        "plan_product_identifiers": [{"value": "product-1"}],
        "plan_backbones": [
            {"type": "plan", "identifier": [{"value": "backbone-a"}]},
            {"type": "plan"},
        ],
        "plan_coverage": {"type": "medical"},
        "plan_fhir_meta": {"lastUpdated": "2026-07-13T12:00:00Z"},
        "plan_fhir_fetch_url": "https://example.test/fhir/InsurancePlan/plan-name-only",
        "plan_fhir_fetch_mode": "read",
    }


def _typed_evidence_rows():
    return [_typed_role_evidence_map(), _typed_plan_evidence_map()]


def test_role_evidence_sql_projects_typed_details_without_catalog_refs():
    sql = npi_module._provider_directory_role_evidence_sql("mrf", has_catalog=True)

    for column in (
        "role.available_time::jsonb AS role_available_time",
        "role.new_patient_acceptance::jsonb AS role_new_patient_acceptance",
        "role.telehealth AS role_telehealth",
        "insurance_plan.product_identifiers::jsonb AS plan_product_identifiers",
        "insurance_plan.plan_backbones::jsonb AS plan_backbones",
        "insurance_plan.coverage::jsonb AS plan_coverage",
        "returned_plan_details AS MATERIALIZED",
        "LEFT JOIN returned_plan_details AS plan",
    ):
        assert column in sql
    assert "network_catalog.refs" not in sql
    assert "jsonb_array_elements(network_catalog.refs" not in sql


def test_current_insurance_plan_ctes_start_from_requested_sources():
    role_sql = npi_module._provider_directory_role_evidence_sql("mrf", has_catalog=True)
    affiliation_sql = npi_module._provider_directory_affiliation_evidence_sql(
        "mrf", has_catalog=True
    )

    for sql, source_cte, scope_name in (
        (role_sql, "roles", "current_role_insurance_plans"),
        (affiliation_sql, "affiliations", "current_affiliation_insurance_plans"),
    ):
        assert f"{scope_name}_sources AS MATERIALIZED" in sql
        assert f"FROM {source_cte}" in sql
        assert "JOIN current_resources AS current_plan" in sql
        assert "current_plan.source_id = requested_source.source_id" in sql
        assert f"JOIN {scope_name} AS current_insurance_plan" in sql
    assert "network_catalog.refs" not in role_sql
    assert "network_catalog.refs" not in affiliation_sql
    assert "plan.plan_backbones::jsonb AS plan_backbones" in affiliation_sql


def test_affiliation_evidence_maps_typed_plan_details():
    plan_evidence_map = _typed_plan_evidence_map()
    plan_evidence_map["affiliation_id"] = "affiliation-100"
    plan_evidence_map.pop("role_id")

    affiliation_evidence = npi_module._map_provider_directory_affiliation_evidence(
        [plan_evidence_map]
    )[("pdfhir_aetna", "affiliation-100")]

    plan_detail = affiliation_evidence["insurance_plans"][0]
    assert plan_detail["name"] == "Example Choice"
    assert len(plan_detail["plan_backbones"]) == 2
    assert plan_detail["fhir_provenance"]["fetch_mode"] == "read"


def test_role_evidence_mapper_emits_availability_and_name_only_plan_details():
    """Keep typed FHIR role and plan fields compatible with the evidence surface."""
    role_evidence = npi_module._map_provider_directory_role_evidence(
        _typed_evidence_rows()
    )[("pdfhir_aetna", "role-100")]
    role_detail = role_evidence["practitioner_role"]
    plan_detail = role_evidence["insurance_plans"][0]

    assert role_detail["available_time"][0]["availableStartTime"] == "09:00"
    assert role_detail["new_patient_acceptance"] == {"code": "accepting"}
    assert role_detail["telehealth"] is True
    assert role_detail["period"] == {"start": "2026-01-01", "end": "2026-12-31"}
    assert len(role_detail["fhir_provenance"]["meta"]["profiles"]) == 32
    assert role_detail["fhir_provenance"]["meta"]["source"] == "https://example.test/fhir"
    assert len(role_detail["fhir_provenance"]["meta"]["security"]) == 32
    assert len(role_detail["fhir_provenance"]["meta"]["security"][0]["display"]) == 2048
    assert role_detail["fhir_provenance"]["meta"]["tags"] == [
        {"code": "directory", "user_selected": True}
    ]
    assert plan_detail["identifier"] is None
    assert plan_detail["name"] == "Example Choice"
    assert len(plan_detail["plan_backbones"]) == 2
    assert plan_detail["period"] == {"start": "2026-01-01"}
    assert plan_detail["fhir_provenance"]["fetch_mode"] == "read"


def test_endpoint_evidence_keeps_role_details_keyed_to_the_address_role():
    source_id = "pdfhir_aetna"
    detail_by_id = {source_id: {"source_id": source_id, "endpoint_id": "endpoint-aetna"}}
    role_evidence_map = {
        (source_id, "role-a"): {
            "practitioner_role": {
                "resource_type": "PractitionerRole",
                "source_id": source_id,
                "resource_id": "role-a",
                "telehealth": True,
            },
            "insurance_plans": [],
            "networks": [],
        },
        (source_id, "role-b"): {
            "practitioner_role": {
                "resource_type": "PractitionerRole",
                "source_id": source_id,
                "resource_id": "role-b",
                "telehealth": False,
            },
            "insurance_plans": [],
            "networks": [],
        },
    }

    evidence = npi_module._provider_directory_endpoint_provenance(
        [source_id], detail_by_id, role_evidence_map, [(source_id, "role-a")]
    )[0]

    assert evidence["practitioner_role_ids"] == ["role-a"]
    assert evidence["practitioner_roles"] == [
        role_evidence_map[(source_id, "role-a")]["practitioner_role"]
    ]
