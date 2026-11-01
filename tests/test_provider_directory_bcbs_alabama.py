# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""The carrier-native directory keeps its identity, scope and transport limits."""

import dataclasses
import importlib
from urllib.parse import parse_qs, urlsplit

import pytest

from api.provider_directory_sources import provider_directory_source_catalog
from scripts.research.provider_directory_endpoint_acquisition_harness import load_manifest
from tests.test_process_provider_directory_fhir_unit import (
    _patch_resource_scan_storage,
    _resource_concurrency_options,
    _source_http_test_session,
)

directory = importlib.import_module("process.provider_directory_fhir")
source_storage = importlib.import_module("process.provider_directory_generic_source_storage")
BASE = "https://api-bcbsal-prd.safhir.io/v1/api/provider-directory"


@pytest.fixture
def carrier_seed():
    return next(row for row in directory.source_selection.configured_seed_rows() if row["api_base"] == BASE)


@pytest.fixture
def carrier_source(carrier_seed):
    return directory._source_row_from_seed(carrier_seed)


def test_carrier_seed_and_normal_catalog_agree_without_hie_aliasing(carrier_seed, carrier_source):
    entry = next(row for row in provider_directory_source_catalog()["items"] if row["entry_id"] == "bcbs-alabama")
    assert entry["source_ids"] == [carrier_source["source_id"]] == ["pdfhir_e0153166e75593bd07cb0525"]
    assert entry["canonical_base"] == carrier_source["canonical_api_base"] == BASE
    assert entry["runnable"] and entry["profile_enabled"]
    assert carrier_source["auth_type"] == "none" and not carrier_source["requires_registration"]
    assert directory._source_seed_override(carrier_seed) is None
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


@pytest.mark.parametrize("base", [directory.CIGNA_PROVIDER_DIRECTORY_BASE, "https://example.test/fhir"])
def test_configured_order_keeps_existing_priority_contract(base):
    source_by_field = {
        "api_base": base,
        "metadata_json": {"provider_directory_resource_fetch_order": ["PractitionerRole", "Organization"]},
    }
    assert directory._source_resource_fetch_order(
        source_by_field, ["Organization", "PractitionerRole", "PractitionerRole", "Location"]
    ) == ["PractitionerRole", "Organization", "Location"]


def test_issued_cursor_checkpoint_keeps_the_original_retry_scope(carrier_source):
    source_ids = [carrier_source["source_id"]]
    root = directory._pagination_checkpoint_context(
        carrier_source, source_ids, run_id="example-root", retry_of_run_id=None, allow_retained_pagination_reset=False
    )
    retry = directory._pagination_checkpoint_context(
        carrier_source,
        source_ids,
        run_id="example-retry",
        retry_of_run_id="example-root",
        pagination_root_run_id="example-root",
        allow_retained_pagination_reset=False,
    )
    assert root is not None and retry is not None
    assert root.canonical_api_base == retry.canonical_api_base == BASE
    assert root.source_ids == retry.source_ids == tuple(source_ids)
    assert root.source_scope_hash == retry.source_scope_hash
    assert retry.acquisition_root_run_id == root.owner_run_id == "example-root"
    assert root.allow_retained_pagination_reset is retry.allow_retained_pagination_reset is False
    following = f"{BASE}/PractitionerRole?ct=synthetic%2Bcursor%3D&_count=100"
    assert directory._resolved_fhir_next_url(carrier_source, f"{BASE}/PractitionerRole?_count=100", following) == (
        following
    )


@pytest.mark.parametrize(
    ("checkpointing", "lineage_verified", "expected"), [(True, True, 8), (False, True, 1), (True, False, 1)]
)
def test_eight_resource_chains_require_a_bound_checkpoint(
    monkeypatch, carrier_source, checkpointing, lineage_verified, expected
):
    checkpoint = directory._pagination_checkpoint_context(
        carrier_source, [carrier_source["source_id"]], run_id="example-root", retry_of_run_id=None
    )
    assert checkpoint is not None
    carrier_source.update(
        _endpoint_id="example-endpoint",
        _endpoint_dataset_id="example-dataset",
        _pagination_checkpoint_context=dataclasses.replace(
            checkpoint,
            endpoint_id="example-endpoint",
            dataset_id="example-dataset",
            lineage_verified=lineage_verified,
        ),
    )
    _patch_resource_scan_storage(monkeypatch, True)
    options = _resource_concurrency_options(
        resource_types=tuple(directory.DEFAULT_RESOURCES), requested_concurrency=8, checkpointing_enabled=checkpointing
    )
    with _source_http_test_session():
        assert directory._effective_resource_scan_concurrency(carrier_source, options) == expected


@pytest.fixture
def named_candidate(carrier_source):
    return {
        "contract": source_storage.NAMED_ISOLATED_CONTRACT,
        "identity": {
            "metadata": {
                "source_ids": [carrier_source["source_id"]],
                "selected_resources": list(directory.DEFAULT_RESOURCES),
                "source_release": None,
            }
        },
        "storage": {
            "catalog_schema": "catalog",
            "storage_schema": "source_candidate",
            "producer": ["a" * 40, "sha256:" + "b" * 64],
        },
        "phase": "loading",
        "auxiliary": None,
        "admission": None,
    }


@pytest.mark.parametrize("source_id", ["pdfhir_e0153166e75593bd07cb0525", "synthetic-public-directory"])
def test_named_storage_accepts_one_ordinary_source_with_all_eight_resources(named_candidate, source_id):
    named_candidate["identity"]["metadata"]["source_ids"] = [source_id]
    source_storage._validate_isolated_binding_storage(named_candidate, named_candidate["storage"])


@pytest.mark.parametrize(
    "metadata_change",
    [
        {"source_ids": []},
        {"source_ids": ["first", "second"]},
        {"source_ids": ["cms-npd"]},
        {"source_ids": [None]},
        {"source_ids": ["x" * 65]},
        {"source_release": "specialized-release"},
        {"selected_resources": []},
        {"selected_resources": ["Patient"]},
        {"selected_resources": [None]},
    ],
)
def test_named_storage_rejects_changed_ordinary_scope(named_candidate, metadata_change):
    named_candidate["identity"]["metadata"].update(metadata_change)
    with pytest.raises(source_storage.GenericCandidateError, match="isolated_kind_unsupported"):
        source_storage._validate_isolated_binding_storage(named_candidate, named_candidate["storage"])
