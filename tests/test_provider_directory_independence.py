# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""The public feed shares acquisition and preserves native product evidence."""

import importlib

import pytest

from api.provider_directory_sources import provider_directory_source_catalog

directory = importlib.import_module("process.provider_directory_fhir")
BASE = "https://eapics.ibx.com/provider/v1/fhir"
SOURCE_ID = "pdfhir_dd4d8d4cf1e5dd3e609ae0b3"


@pytest.fixture
def source():
    seed = next(row for row in directory.source_selection.configured_seed_rows() if row["api_base"] == BASE)
    return directory._source_row_from_seed(seed)


def test_shared_seed_catalog_and_profile_agree(source):
    entry = next(
        row
        for row in provider_directory_source_catalog()["items"]
        if row["entry_id"] == "independence-provider-directory"
    )
    assert entry["source_ids"] == [source["source_id"]] == [SOURCE_ID]
    assert entry["canonical_base"] == source["canonical_api_base"] == BASE
    assert entry["runnable"] and entry["profile_enabled"]
    assert "Independence Administrators" in source["plan_name"]
    assert source["auth_type"] == "none" and not source["requires_registration"]
    assert entry["resources"] == list(directory.DEFAULT_RESOURCES)
    directory._assert_resource_acquisition_allowed(source, entry["resources"])
    assert directory.source_selection.declared_probe_target(directory, source) == ("Practitioner", BASE)


def test_shared_start_urls_respect_collection_contract(source):
    for resource in directory.DEFAULT_RESOURCES:
        expected = f"{BASE}/{resource}" + ("" if resource == "Endpoint" else "?_count=20")
        assert directory._resource_start_url(source, resource, page_count=1000) == expected
    assert directory._resource_start_url(source, "Patient", page_count=100) is None
    assert (
        directory._resource_start_url(
            {**source, "api_base": "https://example.test/fhir", "canonical_api_base": "https://example.test/fhir"},
            "Endpoint",
            page_count=100,
        )
        == "https://example.test/fhir/Endpoint?_count=20"
    )


def test_shared_cursor_and_retry_keep_original_scope(source):
    first = f"{BASE}/Practitioner?_count=20"
    following = f"{BASE}/Practitioner?_count=20&_page_token=synthetic%2Bcursor%3D"
    assert directory._resolved_fhir_next_url(source, first, following) == following
    assert directory._source_fetch_candidate_urls(following) == [following]
    with pytest.raises(ValueError, match="untrusted_pagination_link"):
        directory._resolved_fhir_next_url(source, first, "https://other.example/fhir/Practitioner?_page_token=x")
    root = directory._pagination_checkpoint_context(source, [SOURCE_ID], run_id="example-root", retry_of_run_id=None)
    retry = directory._pagination_checkpoint_context(
        source,
        [SOURCE_ID],
        run_id="example-retry",
        retry_of_run_id="example-root",
        pagination_root_run_id="example-root",
    )
    assert root is not None and retry is not None
    assert retry.canonical_api_base == root.canonical_api_base == BASE
    assert retry.source_scope_hash == root.source_scope_hash
    assert retry.acquisition_root_run_id == root.owner_run_id == "example-root"
