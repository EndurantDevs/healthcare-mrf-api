# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Registered directory configuration retains source and checkpoint boundaries."""

import json
from unittest.mock import Mock

import pytest

from api.provider_directory_sources import provider_directory_source_catalog
from scripts.research import provider_directory_endpoint_acquisition_harness as harness
from tests.test_process_provider_directory_fhir_unit import importer

SOURCE_ID = "pdfhir_d09f9fa3d4c8e3ecee8c907b"


def _source():
    return importer._source_row_from_seed(importer.anthem.seed_rows()[0])


def test_registered_source_and_profile_catalog_have_one_exact_endpoint(monkeypatch):
    monkeypatch.setattr(importer, "_remote_supplemental_catalogs", lambda **_kwargs: ([], {}))
    rows, metrics = importer._seed_rows_from_supplemental_catalogs(source_query="Anthem")
    assert len(rows) == 1 and metrics["catalogs"]["anthem"]["rows"] == 1
    source_by_field = importer._source_row_from_seed(rows[0])
    entry = next(
        item for item in provider_directory_source_catalog()["items"] if item["entry_id"] == "anthem-provider-directory"
    )
    assert source_by_field["source_id"] == SOURCE_ID
    assert entry["source_ids"] == [SOURCE_ID]
    assert entry["canonical_base"] == source_by_field["canonical_api_base"] == importer.anthem.API_BASE
    assert entry["runnable"] and entry["profile_enabled"]
    assert entry["resources"] == list(importer.anthem.RESOURCES)
    assert SOURCE_ID in importer._configured_catalog_protected_source_ids({})
    assert source_by_field["requires_registration"] and not source_by_field["requires_api_key"]
    assert source_by_field["auth_type"] == "OAuth2 Client Credentials"
    assert source_by_field["plan_name"] is None
    assert all(
        source_by_field[field] is None for field in ("is_medicare_advantage", "is_medicaid_mco", "is_chip", "is_qhp")
    )
    assert not importer._is_seed_row_query_match(rows[0], "unrelated payer")


@pytest.mark.parametrize(
    "base",
    [
        "https://apps.availity.com/availity/public-fhir/fhir/v1/anthem/r4",
        "https://apps.availity.com/availity/public-fhir/fhir/v1/wellpoint/r4",
        "https://api.1up.health/fhir-r4/wellpointvariousstates",
    ],
)
def test_affiliate_and_legacy_sources_keep_their_identity(base):
    seed_by_field = {
        "org_name": "Anthem affiliate",
        "plan_name": "Medicare Advantage",
        "api_base": base,
        "portal_url": "https://www.anthem.com/developers",
        "source": "synthetic-catalog",
    }
    source_by_field = importer._source_row_from_seed(seed_by_field)
    assert source_by_field["source_id"] == importer._stable_source_id(seed_by_field) != SOURCE_ID
    assert source_by_field["api_base"] == base
    assert source_by_field["plan_name"] == seed_by_field["plan_name"]


def test_broad_imports_exclude_registered_endpoint_before_probes_including_catalog_aliases():
    source_by_field = _source()
    alias = importer._source_row_from_seed(
        {
            "org_name": "Synthetic endpoint alias",
            "api_base": importer.anthem.API_BASE + "/",
            "source": "synthetic-catalog",
        }
    )
    other_source_by_field = {"source_id": "other-source", "api_base": "https://payer.example/fhir"}
    assert importer._scope_source_rows([source_by_field, alias, other_source_by_field], []) == [other_source_by_field]
    protected = importer._configured_catalog_protected_source_ids({}, [source_by_field, alias, other_source_by_field])
    assert SOURCE_ID in protected and alias["source_id"] in protected
    assert other_source_by_field["source_id"] not in protected
    assert importer._scope_source_rows([source_by_field, alias, other_source_by_field], [SOURCE_ID]) == [
        source_by_field
    ]
    assert importer._scope_source_rows([source_by_field, alias, other_source_by_field], [alias["source_id"]]) == [alias]
    with pytest.raises(ValueError, match="source_id not found"):
        importer._scope_source_rows([other_source_by_field], [SOURCE_ID])


def test_resource_selection_excludes_unavailable_and_non_directory_resources():
    source_by_field = _source()
    for resource_type in importer.anthem.RESOURCES:
        count = 500 if resource_type == "PractitionerRole" else 1000
        assert importer._resource_start_url(source_by_field, resource_type, page_count=1000) == (
            f"{importer.anthem.API_BASE}/{resource_type}?_count={count}&_sort=_id"
        )
    for resource_type in ("Endpoint", "Specimen", "MedicationKnowledge", "Patient"):
        assert importer._resource_start_url(source_by_field, resource_type, page_count=100) is None
    assert not importer._is_source_bulk_export_effective(source_by_field, True, per_resource_limit=0)


def test_full_refresh_prefers_verified_batches_and_bounded_probes_keep_their_request(monkeypatch):
    source_by_field = _source()
    preferred = importer._source_full_refresh_page_count(source_by_field, 100, 0, 0)
    assert preferred == 1000
    assert importer._resource_start_url(source_by_field, "PractitionerRole", page_count=preferred).endswith(
        "?_count=500&_sort=_id"
    )
    for row_limit, page_limit in ((1, 0), (0, 1), (1, 1)):
        assert importer._source_full_refresh_page_count(source_by_field, 2, row_limit, page_limit) == 2
    assert importer._resource_start_url(source_by_field, "PractitionerRole", page_count=2).endswith(
        "?_count=2&_sort=_id"
    )
    source_by_field["metadata_json"]["provider_directory_resource_page_count_caps"]["PractitionerRole"] = 1000
    assert importer._source_resource_page_count(source_by_field, "PractitionerRole", 1000) == 500
    monkeypatch.setenv("HLTHPRT_PROVIDER_DIRECTORY_MAX_PAGE_COUNT", "250")
    assert importer._source_full_refresh_page_count(source_by_field, 100, 0, 0) == 250
    assert importer._source_resource_page_count(source_by_field, "InsurancePlan", 1000) == 250
    assert importer._source_resource_page_count(source_by_field, "PractitionerRole", 1000) == 250
    other_source_by_field = {"source_id": "other", "api_base": "https://directory.example.test/fhir"}
    assert importer._source_full_refresh_page_count(other_source_by_field, 100, 0, 0) == 100


def test_operator_plan_selects_registered_rest_access_without_resetting_checkpoints():
    manifest = harness.load_manifest()
    entry = next(item for item in manifest["entries"] if item["entry_id"] == "anthem-provider-directory")
    params = harness.entry_params(manifest, entry)
    assert params["source_ids"] == [SOURCE_ID]
    assert params["resources"] == ",".join(importer.anthem.RESOURCES)
    assert params["include_auth_required"] is True and params["open_only"] is False
    assert params["resource_scan_concurrency"] == 8
    assert params["allow_retained_pagination_reset"] is False
    assert not params["bulk_export"] and not params["publish_artifacts"]
    assert not params["publish_after_acquisition"] and not params["stale_cleanup"]
    other_source_by_field = next(item for item in manifest["entries"] if item["entry_id"] == "cigna")
    assert harness.entry_params(manifest, other_source_by_field)["open_only"] is True


def test_source_uses_shared_secret_configuration_and_retained_checkpoint(monkeypatch):
    monkeypatch.setenv("DIRECTORY_TEST_CLIENT_ID", "synthetic-client")
    monkeypatch.setenv("DIRECTORY_TEST_CLIENT_SECRET", "synthetic-value")
    monkeypatch.setattr(
        importer,
        "_load_credentials_config",
        lambda: {
            "sources": {
                SOURCE_ID: {
                    "oauth2": {
                        "token_url": importer.anthem.TOKEN_URL,
                        "client_id": "env:DIRECTORY_TEST_CLIENT_ID",
                        "client_secret": "env:DIRECTORY_TEST_CLIENT_SECRET",
                        "auth": "basic",
                    }
                }
            }
        },
    )
    token_fetch = Mock(return_value="synthetic-access")
    monkeypatch.setattr(importer, "_fetch_oauth2_client_token_sync", token_fetch)
    source_by_field = _source()
    options = importer._credential_request_options_for_source(
        source_by_field, importer.anthem.API_BASE + "/InsurancePlan"
    )
    assert options["headers"] == {"Authorization": "Bearer synthetic-access"}
    assert options["descriptor"] == {
        "matched_by": [f"sources:{SOURCE_ID}"],
        "header_names": ["Authorization"],
        "query_param_names": [],
    }
    assert "synthetic-access" not in json.dumps(options["descriptor"])
    assert token_fetch.call_args.args[0]["token_url"] == importer.anthem.TOKEN_URL
    context = importer._pagination_checkpoint_context(
        source_by_field, [SOURCE_ID], run_id="run-first", retry_of_run_id=None
    )
    assert context is not None
    assert context.canonical_api_base == importer.anthem.API_BASE and context.source_ids == (SOURCE_ID,)
    assert context.owner_run_id == context.acquisition_root_run_id == "run-first"
    assert importer._pagination_checkpoint_scope_identity(source_by_field, [SOURCE_ID]) == (
        importer.anthem.API_BASE,
        context.source_scope_hash,
    )


@pytest.mark.parametrize("resource_type", ["InsurancePlan", "PractitionerRole", "OrganizationAffiliation"])
def test_native_network_and_office_references_survive_shared_parsing(resource_type):
    resource_by_field = {
        "resourceType": resource_type,
        "id": "opaque-001",
        "name": "Synthetic plan",
        "active": True,
        "network": [{"reference": "Organization/network-1"}],
        "location": [{"reference": "Location/office-1"}],
    }
    expected_by_field = {"network_refs": ["Organization/network-1"]}
    if resource_type == "InsurancePlan":
        resource_by_field.update(
            ownedBy={"reference": "Organization/insurer-1"},
            coverageArea=[{"reference": "Location/area-1"}],
            type=[{"coding": [{"system": "https://payer.example/PlanTypeCode", "code": "MEDICARE"}]}],
        )
        expected_by_field.update(owned_by_ref="Organization/insurer-1", coverage_area_refs=["Location/area-1"])
    else:
        expected_by_field["location_refs"] = ["Location/office-1"]
    if resource_type == "PractitionerRole":
        resource_by_field.pop("network")
        resource_by_field.update(
            practitioner={"reference": "Practitioner/person-1"},
            extension=[
                {
                    "url": "http://hl7.org/fhir/us/davinci-pdex-plan-net/StructureDefinition/network-reference",
                    "valueReference": {"reference": "Organization/network-1"},
                }
            ],
        )
        expected_by_field["practitioner_ref"] = "Practitioner/person-1"
    model, parsed_row_by_field = importer.parse_fhir_resource(SOURCE_ID, resource_by_field, run_id="run-example")
    assert (
        parsed_row_by_field["resource_id"] == resource_by_field["id"] and parsed_row_by_field["source_id"] == SOURCE_ID
    )
    for field, expected_value in expected_by_field.items():
        assert parsed_row_by_field[field] == expected_value
    retained = importer._endpoint_dataset_resource_rows(
        model,
        [parsed_row_by_field],
        dataset_id="dataset-example",
        resource_hash_contract=importer.DEFAULT_RESOURCE_HASH_CONTRACT,
    )
    assert retained[0]["resource_id"] == resource_by_field["id"]
    for field, expected_value in expected_by_field.items():
        assert retained[0]["payload_json"][field] == expected_value
    resource_by_field.pop("network", None)
    resource_by_field.pop("extension", None)
    _, without_network = importer.parse_fhir_resource(SOURCE_ID, resource_by_field, run_id="run-example")
    assert without_network["network_refs"] == []
