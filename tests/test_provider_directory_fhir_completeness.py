# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import importlib
import json
from pathlib import Path

import pytest

from db.models import (
    ProviderDirectoryEndpoint,
    ProviderDirectoryHealthcareService,
    ProviderDirectoryInsurancePlan,
    ProviderDirectoryLocation,
    ProviderDirectoryOrganization,
    ProviderDirectoryOrganizationAffiliation,
    ProviderDirectoryPractitioner,
    ProviderDirectoryPractitionerRole,
)


importer = importlib.import_module("process.provider_directory_fhir")

INSURANCE_PLAN_COMPLETENESS_RESOURCE = {
    "resourceType": "InsurancePlan",
    "id": "plan-1",
    "identifier": [
        {"system": "https://example.test/product", "value": "product-1"},
        {"value": "systemless-product"},
    ],
    "plan": [
        {
            "identifier": [
                {"system": "https://example.test/plan", "value": "gold-1"},
                {"value": "gold-alias"},
            ],
            "type": {"coding": [{"system": "urn:plan", "code": "gold"}]},
            "coverageArea": [{"reference": "Location/area-1"}],
            "network": [{"reference": "Organization/network-1"}],
        },
        {
            "identifier": [{"value": "silver-1"}],
            "coverageArea": [{"reference": "Location/area-2"}],
            "network": [{"reference": "Organization/network-2"}],
        },
    ],
    "coverage": [
        {
            "type": {"coding": [{"system": "urn:coverage", "code": "medical"}]},
            "network": [{"reference": "Organization/network-1"}],
            "benefit": [
                {
                    "type": {"coding": [{"code": "pcp"}]},
                    "requirement": "Referral required",
                    "limit": [
                        {
                            "value": {"value": 12, "unit": "visits"},
                            "code": {"coding": [{"code": "annual"}]},
                        }
                    ],
                }
            ],
        },
        {"network": [{"reference": "Organization/network-2"}]},
    ],
}
PRACTITIONER_ROLE_COMPLETENESS_RESOURCE = {
    "resourceType": "PractitionerRole",
    "id": "role-1",
    "availableTime": [
        {"daysOfWeek": ["mon", "tue"], "availableStartTime": "09:00:00"}
    ],
    "notAvailable": [
        {"description": "Annual closure", "during": {"start": "2026-12-24"}}
    ],
    "availabilityExceptions": "Call for holiday hours",
    "extension": [
        {
            "url": (
                "http://hl7.org/fhir/us/davinci-pdex-plan-net/"
                "StructureDefinition/newpatients"
            ),
            "extension": [
                {
                    "url": "acceptingPatients",
                    "valueCodeableConcept": {"coding": [{"code": "newpt"}]},
                },
                {
                    "url": "fromNetwork",
                    "valueReference": {"reference": "Organization/network-1"},
                },
            ],
        },
        {
            "url": "https://healthporta.com/fhir/provider-directory/telehealth",
            "valueBoolean": False,
        },
        {
            "url": (
                "https://healthporta.com/fhir/provider-directory/"
                "accepting-medicaid"
            ),
            "valueBoolean": True,
        },
        {
            "url": (
                "http://hl7.org/fhir/us/davinci-pdex-plan-net/"
                "StructureDefinition/delivery-method"
            ),
            "extension": [
                {
                    "url": "type",
                    "valueCodeableConcept": {"coding": [{"code": "virtual"}]},
                },
                {
                    "url": "virtualModalities",
                    "valueCodeableConcept": {"coding": [{"code": "video"}]},
                },
            ],
        },
        {"url": "https://unreviewed.example/telehealth", "valueString": "yes"},
        {"url": "https://unreviewed.example/arbitrary", "valueString": "discard"},
    ],
}


@pytest.mark.parametrize(
    ("fhir_resource", "expected_model"),
    [
        ({"resourceType": "InsurancePlan", "id": "plan-1"}, ProviderDirectoryInsurancePlan),
        ({"resourceType": "Practitioner", "id": "practitioner-1"}, ProviderDirectoryPractitioner),
        ({"resourceType": "Organization", "id": "organization-1"}, ProviderDirectoryOrganization),
        ({"resourceType": "Location", "id": "location-1"}, ProviderDirectoryLocation),
        ({"resourceType": "PractitionerRole", "id": "role-1"}, ProviderDirectoryPractitionerRole),
        ({"resourceType": "HealthcareService", "id": "service-1"}, ProviderDirectoryHealthcareService),
        (
            {"resourceType": "OrganizationAffiliation", "id": "affiliation-1"},
            ProviderDirectoryOrganizationAffiliation,
        ),
        ({"resourceType": "Endpoint", "id": "endpoint-1"}, ProviderDirectoryEndpoint),
    ],
)
def test_typed_fhir_rows_preserve_meta_and_sanitized_acquisition(
    fhir_resource,
    expected_model,
):
    fhir_resource["meta"] = {
        "versionId": "7",
        "lastUpdated": "2026-07-13T10:00:00Z",
        "profile": ["https://example.test/Profile/provider-directory"],
    }
    legacy_resource_url = "urn:legacy:resource-url"
    parsed_model, parsed_row = importer.parse_fhir_resource(
        "source-a",
        fhir_resource,
        resource_url=legacy_resource_url,
        acquisition=importer.FHIRAcquisitionContext(
            self_url=(
                "https://self-user:self-pass@payer.example/fhir/Resource/1"
                "?access_token=self-secret#history"
            ),
            fetch_url=(
                "https://fetch-user:fetch-pass@payer.example/fhir/Resource"
                "?_page_token=fetch-secret&api_key=also-secret"
            ),
            fetch_mode="rest_bundle",
        ),
    )

    assert parsed_model is expected_model
    assert parsed_row["resource_url"] == legacy_resource_url
    assert parsed_row["fhir_meta"] == fhir_resource["meta"]
    assert parsed_row["fhir_self_url"] == "https://payer.example/fhir/Resource/1"
    assert parsed_row["fhir_fetch_url"] == "https://payer.example/fhir/Resource"
    assert parsed_row["fhir_fetch_mode"] == "rest_bundle"
    serialized_row = json.dumps(parsed_row, default=str)
    assert "secret" not in serialized_row
    assert "self-pass" not in serialized_row
    assert "fetch-pass" not in serialized_row


def test_insurance_plan_normalizes_all_products_plans_and_coverage():
    parsed_model, plan_row = importer.parse_fhir_resource(
        "source-a",
        INSURANCE_PLAN_COMPLETENESS_RESOURCE,
    )

    assert parsed_model is ProviderDirectoryInsurancePlan
    assert [entry["value"] for entry in plan_row["product_identifiers"]] == [
        "product-1",
        "systemless-product",
    ]
    assert len(plan_row["plan_backbones"]) == 2
    assert [entry["value"] for entry in plan_row["plan_backbones"][0]["identifiers"]] == [
        "gold-1",
        "gold-alias",
    ]
    assert plan_row["plan_backbones"][0]["coverage_area_refs"] == ["Location/area-1"]
    assert plan_row["plan_backbones"][1]["network_refs"] == ["Organization/network-2"]
    assert plan_row["plan_json"]["identifier"][0]["value"] == "gold-1"
    assert len(plan_row["coverage"]) == 2
    assert plan_row["coverage"][0]["benefits"][0]["limits"][0]["value"]["value"] == 12
    assert plan_row["coverage"][1]["network_refs"] == ["Organization/network-2"]


def test_practitioner_role_separates_availability_new_patients_and_telehealth():
    parsed_model, role_row = importer.parse_fhir_resource(
        "source-a",
        PRACTITIONER_ROLE_COMPLETENESS_RESOURCE,
    )

    expected_new_patients = [
        {"code": "newpt", "from_network_ref": "Organization/network-1"}
    ]
    assert parsed_model is ProviderDirectoryPractitionerRole
    assert role_row["available_time"][0]["daysOfWeek"] == ["mon", "tue"]
    assert role_row["not_available"][0]["description"] == "Annual closure"
    assert role_row["availability_exceptions"] == "Call for holiday hours"
    assert role_row["new_patient_acceptance"] == expected_new_patients
    assert role_row["accepting_patients"] == expected_new_patients
    assert role_row["telehealth"][0] == {"supported": False}
    assert role_row["telehealth"][1]["virtual_modalities"][0]["code"] == "video"
    assert role_row["accepting_medicaid"] is True
    serialized_row = json.dumps(role_row, default=str)
    assert "unreviewed" not in serialized_row
    assert "arbitrary" not in serialized_row


def test_canonical_and_dataset_payloads_inherit_completeness_and_provenance():
    parsed_model, role_row = importer.parse_fhir_resource(
        "source-a",
        {
            "resourceType": "PractitionerRole",
            "id": "role-1",
            "meta": {"versionId": "2"},
            "availableTime": [{"allDay": True}],
        },
        acquisition=importer.FHIRAcquisitionContext(
            self_url="https://payer.example/fhir/PractitionerRole/role-1",
            fetch_url="https://payer.example/fhir/PractitionerRole?_count=100",
            fetch_mode="rest_bundle",
        ),
        run_id="run-1",
    )

    canonical_row = importer._canonical_resource_rows(
        parsed_model,
        [role_row],
        canonical_api_base="https://payer.example/fhir",
        run_id="run-1",
    )[0]
    dataset_row = importer._endpoint_dataset_resource_rows(
        parsed_model,
        [role_row],
        dataset_id="dataset-1",
    )[0]

    assert canonical_row["fhir_meta"] == {"versionId": "2"}
    assert canonical_row["fhir_self_url"].endswith("/PractitionerRole/role-1")
    assert canonical_row["fhir_fetch_url"].endswith("/PractitionerRole")
    assert canonical_row["fhir_fetch_mode"] == "rest_bundle"
    assert canonical_row["payload_json"]["available_time"] == [{"allDay": True}]
    assert dataset_row["payload_json"] == importer._canonical_resource_payload(role_row)
    assert dataset_row["payload_json"]["fhir_meta"] == {"versionId": "2"}


def test_canonical_backfill_syncs_provenance_columns_and_payload():
    canonical_sql, _edge_sql = importer._canonical_backfill_resource_sql(
        "PractitionerRole",
        ProviderDirectoryPractitionerRole.__tablename__,
    )

    for column_name in (
        "fhir_meta",
        "fhir_self_url",
        "fhir_fetch_url",
        "fhir_fetch_mode",
    ):
        assert f"r.{column_name}" in canonical_sql
        assert f"{column_name} = EXCLUDED.{column_name}" in canonical_sql
    assert "to_jsonb(r)" in canonical_sql


def test_rest_bundle_provenance_redacts_fetch_query():
    role_rows = importer._parse_practitioner_role_reverse_lookup_rows(
        "source-a",
        {
            "resourceType": "Bundle",
            "entry": [
                {
                    "fullUrl": (
                        "https://user:pass@payer.example/fhir/PractitionerRole/role-1"
                        "?access_token=secret#history"
                    ),
                    "resource": {"resourceType": "PractitionerRole", "id": "role-1"},
                }
            ],
        },
        "run-1",
        fetch_url="https://payer.example/fhir/PractitionerRole?_page_token=secret",
    )

    assert role_rows[0]["resource_url"].endswith("/PractitionerRole/role-1")
    assert role_rows[0]["fhir_self_url"].endswith("/PractitionerRole/role-1")
    assert role_rows[0]["fhir_fetch_url"] == "https://payer.example/fhir/PractitionerRole"
    assert role_rows[0]["fhir_fetch_mode"] == "rest_bundle"
    assert "secret" not in json.dumps(role_rows[0], default=str)
    assert "pass" not in role_rows[0]["resource_url"]


def test_bundle_urn_self_identity_is_preserved_without_query_data():
    acquisition = importer._rest_bundle_acquisition(
        {"fullUrl": "urn:uuid:5b0d89fd-4f4e-4c57-96ab-9f920b3d50ac?token=secret"},
        "https://payer.example/fhir/PractitionerRole?_page_token=secret",
    )
    _model, role_row = importer.parse_fhir_resource(
        "source-a",
        {"resourceType": "PractitionerRole", "id": "role-1"},
        acquisition=acquisition,
    )

    assert role_row["fhir_self_url"] == (
        "urn:uuid:5b0d89fd-4f4e-4c57-96ab-9f920b3d50ac"
    )
    assert "secret" not in role_row["fhir_self_url"]


@pytest.mark.asyncio
async def test_rest_read_call_site_records_sanitized_fetch_identity(monkeypatch):
    async def fake_fetch(_source, _url, *, timeout):
        del timeout
        return 200, {"resourceType": "Organization", "id": "org-1"}, None, 1

    monkeypatch.setattr(importer, "_fetch_source_json", fake_fetch)
    parsed_resource = await importer._fetch_linked_resource_row(
        {"source_id": "source-a", "api_base": "https://payer.example/fhir"},
        "Organization",
        "org-1",
        reference=(
            "https://user:password@payer.example/fhir/Organization/org-1"
            "?access_token=secret"
        ),
        timeout=3,
        run_id="run-1",
    )

    assert parsed_resource is not None
    linked_row = parsed_resource[1]
    assert linked_row["fhir_self_url"] == "https://payer.example/fhir/Organization/org-1"
    assert linked_row["fhir_fetch_url"] == "https://payer.example/fhir/Organization/org-1"
    assert linked_row["fhir_fetch_mode"] == "rest_read"
    assert "password" not in linked_row["fhir_fetch_url"]
    assert "secret" not in linked_row["fhir_fetch_url"]


class _BulkContent:
    def __init__(self, payload):
        self.payload = payload

    async def iter_chunked(self, _chunk_size):
        yield self.payload


class _BulkResponse:
    status = 200

    def __init__(self, payload):
        self.content = _BulkContent(payload)

    async def __aenter__(self):
        return self

    async def __aexit__(self, _exc_type, _exc, _traceback):
        return False


class _BulkSession:
    def __init__(self, payload):
        self.response = _BulkResponse(payload)

    def get(self, *_args, **_kwargs):
        return self.response


@pytest.mark.asyncio
async def test_bulk_ndjson_call_site_records_redacted_mode():
    output_url = (
        "https://storage.example/signed/output.ndjson"
        "?X-Amz-Credential=credential-secret&X-Amz-Signature=signature-secret"
    )
    ndjson_bytes = json.dumps(
        {"resourceType": "Practitioner", "id": "practitioner-1", "meta": {"versionId": "1"}}
    ).encode("utf-8")
    parsed_rows, fetched_count, written_count, is_limited, stream_error = (
        await importer._stream_bulk_export_output_rows(
            _BulkSession(ndjson_bytes),
            {
                "source_id": "source-a",
                "metadata_json": {
                    "provider_directory_bulk_export_output_hosts": [
                        "storage.example"
                    ]
                },
            },
            output_url,
            model=ProviderDirectoryPractitioner,
            resource_type="Practitioner",
            per_resource_limit=0,
            timeout=3,
            run_id="run-1",
            row_batch_handler=None,
            row_batch_size=10,
            retain_rows=True,
        )
    )

    assert (fetched_count, written_count, is_limited, stream_error) == (
        1,
        0,
        False,
        None,
    )
    assert parsed_rows[0]["fhir_fetch_mode"] == "bulk_ndjson"
    assert parsed_rows[0]["fhir_meta"] == {"versionId": "1"}
    serialized_row = json.dumps(parsed_rows[0], default=str)
    assert "credential-secret" not in serialized_row
    assert "signature-secret" not in serialized_row


def test_graphql_call_site_records_graphql_mode_without_fhir_self_url():
    rows_by_model = {}
    importer._append_alohr_parsed_resource(
        rows_by_model,
        "source-alohr",
        {"resourceType": "Practitioner", "id": "practitioner-1"},
        run_id="run-1",
    )

    practitioner_row = rows_by_model[ProviderDirectoryPractitioner][0]
    assert practitioner_row["resource_url"].startswith(importer.ALOHR_GRAPHQL_URL)
    assert practitioner_row["fhir_self_url"] is None
    assert practitioner_row["fhir_fetch_url"] == importer.ALOHR_GRAPHQL_URL
    assert practitioner_row["fhir_fetch_mode"] == "graphql"


def test_provider_directory_guide_documents_typed_evidence_contract():
    guide = (
        Path(__file__).resolve().parents[1]
        / "docs/imports/provider-directory-fhir.md"
    ).read_text(encoding="utf-8")
    for expected_text in (
        "Typed Directory Evidence API Contract",
        "include_sources=true&include_evidence=true",
        "compact provenance mode",
        "product_identifiers",
        "plan_backbones",
        "available_time",
        "new_patient_acceptance",
        "fhir_provenance",
        "source-scoped and current-dataset scoped",
    ):
        assert expected_text in guide
