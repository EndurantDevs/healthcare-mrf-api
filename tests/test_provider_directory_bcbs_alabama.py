# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""The carrier-native directory keeps its identity, scope and transport limits."""

import importlib
from urllib.parse import parse_qs, urlsplit

import pytest

from api.provider_directory_sources import provider_directory_source_catalog
from process import provider_directory_bcbs_alabama as alabama_directory
from scripts.research.provider_directory_endpoint_acquisition_harness import load_manifest

directory = importlib.import_module("process.provider_directory_fhir")


@pytest.fixture
def carrier_source():
    return directory._source_row_from_seed(alabama_directory.seed_rows(directory)[0])


def test_carrier_seed_and_normal_catalog_agree_without_hie_aliasing(carrier_source):
    entry = next(row for row in provider_directory_source_catalog()["items"] if row["entry_id"] == "bcbs-alabama")
    assert entry["source_ids"] == [carrier_source["source_id"]] == ["pdfhir_e0153166e75593bd07cb0525"]
    assert entry["canonical_base"] == carrier_source["canonical_api_base"] == alabama_directory.API_BASE
    assert entry["runnable"] and entry["profile_enabled"]
    assert carrier_source["auth_type"] == "none" and not carrier_source["requires_registration"]
    assert directory._source_seed_override(alabama_directory.seed_rows(directory)[0]) is None
    assert carrier_source["plan_name"] is None
    assert not directory._is_manual_only_source(carrier_source)
    directory._assert_resource_acquisition_allowed(carrier_source, entry["resources"])
    assert (
        next(row for row in load_manifest()["entries"] if row["entry_id"] == "bcbs-alabama")["resources"]
        == entry["resources"]
    )


def test_carrier_limits_and_expected_empty_endpoint(carrier_source):
    assert directory._source_request_interval_seconds(carrier_source) == 1.1
    assert directory._expected_nonempty_resource_types(carrier_source) == set(directory.DEFAULT_RESOURCES) - {
        "Endpoint"
    }
    for resource in directory.DEFAULT_RESOURCES:
        url = directory._resource_start_url(carrier_source, resource, page_count=1000)
        assert urlsplit(url).path.endswith("/" + resource)
        assert parse_qs(urlsplit(url).query) == {"_count": ["100"], "_total": ["accurate"]}
    assert directory._source_resource_fetch_order(carrier_source, list(directory.DEFAULT_RESOURCES)) == [
        "PractitionerRole",
        "Location",
        "HealthcareService",
        "Practitioner",
        "OrganizationAffiliation",
        "Organization",
        "InsurancePlan",
        "Endpoint",
    ]


@pytest.mark.parametrize("order", [["Location", "Location"], ["Patient"], [None], "Location"])
def test_invalid_resource_order_fails_before_acquisition(carrier_source, order):
    carrier_source["metadata_json"]["provider_directory_resource_fetch_order"] = order
    with pytest.raises(ValueError, match="resource_fetch_order_invalid"):
        directory._source_resource_fetch_order(carrier_source, list(directory.DEFAULT_RESOURCES))
