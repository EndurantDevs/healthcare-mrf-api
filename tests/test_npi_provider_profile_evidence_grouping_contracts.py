# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from api.endpoint import npi as npi_module


def _source_details_by_id():
    return {
        "source-a": {
            "source_id": "source-a",
            "endpoint_id": "endpoint-one",
            "canonical_api_base": "https://example.test/fhir",
        },
        "source-b": {
            "source_id": "source-b",
            "endpoint_id": "endpoint-one",
            "canonical_api_base": "https://example.test/fhir",
        },
        "source-c": {
            "source_id": "source-c",
            "endpoint_id": None,
            "canonical_api_base": "",
        },
    }


def _role_evidence_map(resource_id, *, provenance=None):
    return {
        "practitioner_role": {
            "source_id": "source-a",
            "resource_id": resource_id,
        },
        "insurance_plans": [
            {
                "resource_type": "InsurancePlan",
                "resource_id": "plan-a",
                "provenance": provenance,
            }
        ],
        "networks": [
            {
                "resource_type": "Organization",
                "resource_id": "network-a",
                "name": "Example Network",
                "reference": "Organization/network-a",
                "provenance": "provider-directory",
            }
        ],
        "insurance_plan_metadata": {
            "returned": 1,
            "total": 1,
            "truncated": False,
            "catalog_complete": True,
        },
        "evidence_metadata": {
            "returned": 3,
            "total": 3,
            "truncated": False,
        },
    }


def test_role_evidence_is_exact_scoped_and_merged_by_endpoint():
    source_details_by_id = _source_details_by_id()
    endpoint_key = ("endpoint_id", "endpoint-one")
    role_a = ("source-a", "role-a")
    role_b = ("source-b", "role-b")
    role_outside = ("source-c", "role-c")
    evidence_by_role = {
        role_a: _role_evidence_map("role-a", provenance="network-derived"),
        role_b: _role_evidence_map("role-b"),
        role_outside: _role_evidence_map("role-c"),
    }

    fields_map = npi_module._provider_directory_role_evidence_fields(
        ["source-a", "source-b", "source-c"],
        [role_a, role_a, role_outside, role_b],
        source_details_by_id,
        endpoint_key,
        evidence_by_role,
    )

    assert fields_map["source_ids"] == ["source-a", "source-b"]
    assert fields_map["practitioner_role_ids"] == ["role-a", "role-b"]
    assert [
        role_detail["resource_id"]
        for role_detail in fields_map["practitioner_roles"]
    ] == ["role-a", "role-b"]
    assert fields_map["insurance_plans"][0].get("provenance") is None
    assert len(fields_map["networks"]) == 1
    assert len(fields_map["insurance_plan_metadata_by_role"]) == 2
    assert fields_map["evidence_metadata"]["total"] == 3

    outside_fields_map = npi_module._provider_directory_role_evidence_fields(
        ["source-c"],
        [role_outside],
        source_details_by_id,
        endpoint_key,
        evidence_by_role,
    )
    assert outside_fields_map == {}


def test_single_role_evidence_uses_compact_plan_metadata():
    role_key = ("source-a", "role-a")
    fields_map = npi_module._provider_directory_role_evidence_fields(
        ["source-a"],
        [role_key],
        _source_details_by_id(),
        ("endpoint_id", "endpoint-one"),
        {role_key: _role_evidence_map("role-a")},
    )

    assert fields_map["insurance_plan_metadata"] == {
        "returned": 1,
        "total": 1,
        "truncated": False,
        "catalog_complete": True,
    }


def test_affiliation_evidence_is_exact_scoped_and_grouped_by_endpoint():
    source_details_by_id = _source_details_by_id()
    endpoint_key = ("endpoint_id", "endpoint-one")
    affiliation_a = ("source-a", "affiliation-a")
    affiliation_b = ("source-b", "affiliation-b")
    evidence_by_affiliation = {
        affiliation_a: _role_evidence_map("affiliation-a"),
        affiliation_b: _role_evidence_map("affiliation-b"),
    }

    fields_map = npi_module._provider_directory_affiliation_evidence_fields(
        ["source-a", "source-b"],
        [affiliation_a, affiliation_a, affiliation_b],
        source_details_by_id,
        endpoint_key,
        evidence_by_affiliation,
    )

    assert fields_map["organization_affiliation_ids"] == [
        "affiliation-a",
        "affiliation-b",
    ]
    assert len(fields_map["insurance_plan_metadata_by_affiliation"]) == 2
    outside_fields_map = (
        npi_module._provider_directory_affiliation_evidence_fields(
            ["source-c"],
            [("source-c", "missing")],
            source_details_by_id,
            endpoint_key,
            evidence_by_affiliation,
        )
    )
    assert outside_fields_map == {}


def test_affiliation_fields_merge_without_overwriting_role_metadata():
    endpoint_provenance_map = {
        "insurance_plans": [{"resource_id": "plan-a"}],
        "insurance_plan_metadata": {"returned": 9},
    }
    affiliation_fields_map = {
        "insurance_plans": [
            {"resource_id": "plan-a"},
            {"resource_id": "plan-b"},
        ],
        "networks": [{"resource_id": "network-a"}],
        "organization_affiliation_ids": ["affiliation-a"],
        "insurance_plan_metadata": {"returned": 1},
        "evidence_metadata": {"total": 2},
    }

    npi_module._merge_provider_directory_affiliation_fields(
        endpoint_provenance_map,
        affiliation_fields_map,
    )

    assert [
        plan_detail["resource_id"]
        for plan_detail in endpoint_provenance_map["insurance_plans"]
    ] == ["plan-a", "plan-b"]
    assert endpoint_provenance_map["organization_affiliation_ids"] == [
        "affiliation-a"
    ]
    assert endpoint_provenance_map["insurance_plan_metadata"] == {"returned": 9}
    assert endpoint_provenance_map["evidence_metadata"] == {"total": 2}
    assert npi_module._provider_directory_selected_endpoint_keys(
        ["missing", "source-a", "source-b", "source-c"],
        _source_details_by_id(),
    ) == [
        ("endpoint_id", "endpoint-one"),
        ("source_id", "source-c"),
    ]


def test_affiliation_mapper_deduplicates_optional_plan_and_network_evidence():
    base_evidence_map = {
        "source_id": "source-a",
        "affiliation_id": "affiliation-a",
        "identifier": None,
        "name": None,
        "reference": None,
        "provenance": None,
    }
    plan_evidence_map = {
        **base_evidence_map,
        "evidence_type": "insurance_plan",
        "resource_id": "plan-a",
    }
    network_evidence_map = {
        **base_evidence_map,
        "evidence_type": "network",
        "resource_id": "network-a",
        "name": "Example Network",
        "reference": "Organization/network-a",
        "provenance": "provider-directory",
        "evidence_row_total": 5,
    }
    evidence_by_affiliation = (
        npi_module._map_provider_directory_affiliation_evidence(
            [
                {
                    **base_evidence_map,
                    "evidence_type": "affiliation",
                    "resource_id": "affiliation-a",
                },
                plan_evidence_map,
                plan_evidence_map,
                network_evidence_map,
                network_evidence_map,
                {
                    **base_evidence_map,
                    "evidence_type": "unknown",
                    "resource_id": "ignored",
                },
            ]
        )
    )
    mapped_evidence = evidence_by_affiliation[("source-a", "affiliation-a")]

    assert len(mapped_evidence["insurance_plans"]) == 1
    assert len(mapped_evidence["networks"]) == 1
    assert "insurance_plan_metadata" not in mapped_evidence
    assert mapped_evidence["evidence_metadata"] == {
        "returned": 6,
        "total": 5,
        "truncated": False,
    }
