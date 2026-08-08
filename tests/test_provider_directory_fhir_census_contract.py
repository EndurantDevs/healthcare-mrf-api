# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Generic admission proof for cutoff-bounded current-version FHIR census."""

from __future__ import annotations

import datetime
import urllib.parse

import pytest
from process.provider_directory_fhir_census_binding import (
    bind_current_version_census_contract,
    current_version_census_contract,
    current_version_census_count_url,
    validated_current_version_census_count_map,
)
from process.provider_directory_fhir_census_contract import (
    CURRENT_VERSION_CENSUS_CONTINUATION_STRATEGY_FIELD,
    CURRENT_VERSION_CENSUS_METADATA_STRATEGY_FIELD,
    CURRENT_VERSION_CENSUS_SMILE_CONTINUATION_STRATEGY,
    CURRENT_VERSION_CENSUS_START_URLS_FIELD,
    CURRENT_VERSION_CENSUS_STRATEGY_VERSION,
    CurrentVersionCensusRuntime,
    ProviderDirectoryFHIRAcquisitionStrategy,
    current_version_census_request,
    validate_current_version_census_runtime,
)
EXACT_STRATEGY = (
    ProviderDirectoryFHIRAcquisitionStrategy.CUTOFF_BOUNDED_CURRENT_VERSION_CENSUS.value
)
CUTOFF = "2026-08-01T12:00:00.000000Z"
RESOURCE_TYPES = ("Organization", "Practitioner")


def _request_task(**overrides):
    task_by_field = {
        "provider_directory_acquisition_strategy": EXACT_STRATEGY,
        "provider_directory_census_cutoff": CUTOFF,
        "source_ids": ["synthetic-source"],
        "resources": list(RESOURCE_TYPES),
        "import_resources": True,
    }
    task_by_field.update(overrides)
    return task_by_field


def _request(**overrides):
    return current_version_census_request(
        _request_task(**overrides),
        allowed_resources=RESOURCE_TYPES,
        now=datetime.datetime(2026, 8, 2, tzinfo=datetime.UTC),
    )


def _source_record(**metadata_overrides):
    start_url_by_resource = {
        "Organization": (
            "https://directory.example.test/fhir/Organization?"
            "status=active&identifier=urn%3Asynthetic&_count=20"
        ),
        "Practitioner": (
            "https://directory.example.test/fhir/Practitioner?active=true"
        ),
    }
    metadata_by_field = {
        "provider_directory_manual_only": True,
        CURRENT_VERSION_CENSUS_METADATA_STRATEGY_FIELD: EXACT_STRATEGY,
        CURRENT_VERSION_CENSUS_CONTINUATION_STRATEGY_FIELD: (
            CURRENT_VERSION_CENSUS_SMILE_CONTINUATION_STRATEGY
        ),
        "provider_directory_supported_resources": list(RESOURCE_TYPES),
        "provider_directory_fully_enumerable_resources": list(RESOURCE_TYPES),
        "provider_directory_expected_nonempty_resources": ["Organization"],
        CURRENT_VERSION_CENSUS_START_URLS_FIELD: start_url_by_resource,
    }
    metadata_by_field.update(metadata_overrides)
    return {
        "source_id": "synthetic-source",
        "api_base": "https://directory.example.test/fhir",
        "canonical_api_base": "https://directory.example.test/fhir",
        "auth_type": "none",
        "last_validated_status": "valid",
        "metadata_json": metadata_by_field,
    }


def _bound_contract(**metadata_overrides):
    request = _request()
    assert request is not None
    return bind_current_version_census_contract(
        request,
        [_source_record(**metadata_overrides)],
    )


def test_request_rejects_unknown_strategy_before_defaulting():
    with pytest.raises(ValueError, match="acquisition_strategy_unsupported"):
        _request(provider_directory_acquisition_strategy="guess")


@pytest.mark.parametrize(
    ("task_override", "error"),
    (
        ({"provider_directory_census_cutoff": "2026-08-01T12:00:00"}, "timezone_required"),
        ({"provider_directory_census_cutoff": "not-an-instant"}, "cutoff_invalid"),
        ({"provider_directory_census_cutoff": "2999-01-01T00:00:00Z"}, "cannot_be_future"),
        ({"resources": []}, "resources_must_not_be_empty"),
        ({"resources": ["Organization", "Organization"]}, "resources_must_be_unique"),
        ({"resources": ["UnknownResource"]}, "resources_unsupported"),
        ({"source_ids": []}, "exactly_one_source_required"),
    ),
)
def test_request_rejects_ambiguous_cutoff_resource_and_source_identity(
    task_override,
    error,
):
    with pytest.raises(ValueError, match=error):
        _request(**task_override)


def test_configured_strategy_rejects_silently_ignored_cutoff():
    with pytest.raises(ValueError, match="cutoff_without_strategy"):
        _request(provider_directory_acquisition_strategy="configured")


def _safe_runtime() -> CurrentVersionCensusRuntime:
    """Return the sole exhaustive, serial, nonpublishing runtime shape."""
    return CurrentVersionCensusRuntime(
        checkpointing_enabled=True,
        full_refresh=True,
        resource_limit=0,
        page_limit=0,
        stream_batch_size=5000,
        source_concurrency=1,
        resource_scan_concurrency=1,
        linked_resource_limit=0,
        linked_resource_deadline_seconds=0,
        resource_deadline_seconds=0,
        probe=True,
        seed_only=False,
        test_mode=False,
        dataset_rehydrate_only=False,
        dataset_followup_only=False,
        canonical_backfill_only=False,
        contact_backfill_only=False,
        publish_artifacts_only=False,
        local_seed_catalog=True,
        local_retest_catalog=False,
        supplemental_catalogs=False,
        local_supplemental_catalog_inputs=(),
        remote_catalog_inputs=(),
        bulk_export=False,
        stale_cleanup=False,
        publication_requested=False,
        defer_typed_materialization=True,
        bounded_source_selection=False,
        endpoint_scope_configured=False,
        credential_configured=False,
        open_only=True,
        include_auth_required=False,
    )


def test_runtime_requires_exhaustive_serial_unpublished_controls():
    """Reject value or type drift from the admitted runtime shape."""
    request = _request()
    assert request is not None
    safe_runtime = _safe_runtime()
    validate_current_version_census_runtime(request, safe_runtime)
    with pytest.raises(ValueError, match="resource_scan_concurrency"):
        validate_current_version_census_runtime(
            request,
            CurrentVersionCensusRuntime(
                **{
                    **safe_runtime.__dict__,
                    "resource_scan_concurrency": 2,
                }
            ),
        )
    with pytest.raises(ValueError, match="runtime_type_invalid"):
        validate_current_version_census_runtime(
            request,
            CurrentVersionCensusRuntime(
                **{
                    **safe_runtime.__dict__,
                    "source_concurrency": True,
                }
            ),
        )
    with pytest.raises(ValueError, match="local_supplemental_catalog_inputs"):
        validate_current_version_census_runtime(
            request,
            CurrentVersionCensusRuntime(
                **{
                    **safe_runtime.__dict__,
                    "local_supplemental_catalog_inputs": (
                        "cms_sma_endpoint_directory_path",
                    ),
                }
            ),
        )


@pytest.mark.parametrize(
    "task_override",
    (
        {"probe": "true"},
        {"source_concurrency": True},
        {"run_id": 123},
    ),
)
def test_request_rejects_runtime_type_lookalikes(task_override):
    with pytest.raises(ValueError, match="task_type_invalid"):
        _request(**task_override)


@pytest.mark.parametrize(
    ("metadata_override", "error"),
    (
        ({"provider_directory_manual_only": False}, "manual_source_required"),
        (
            {CURRENT_VERSION_CENSUS_METADATA_STRATEGY_FIELD: "configured"},
            "strategy_not_reviewed",
        ),
        (
            {"provider_directory_supported_resources": ["Organization"]},
            "must_match_requested_resources",
        ),
        (
            {"provider_directory_expected_nonempty_resources": []},
            "must_not_be_empty",
        ),
        (
            {"provider_directory_expected_nonempty_resources": ["Location"]},
            "unsupported",
        ),
        (
            {
                CURRENT_VERSION_CENSUS_START_URLS_FIELD: {
                    "Organization": "https://other.example.test/fhir/Organization",
                    "Practitioner": "https://directory.example.test/fhir/Practitioner",
                }
            },
            "origin_mismatch",
        ),
    ),
)
def test_reviewed_source_binding_is_strict(metadata_override, error):
    with pytest.raises(ValueError, match=error):
        _bound_contract(**metadata_override)


def test_reviewed_start_url_preserves_filters_and_adds_exclusive_cutoff():
    contract = _bound_contract()

    start_url = contract.start_url("Organization", 100)
    query_by_name = urllib.parse.parse_qs(
        urllib.parse.urlsplit(start_url).query,
        keep_blank_values=True,
    )

    assert query_by_name["status"] == ["active"]
    assert query_by_name["identifier"] == ["urn:synthetic"]
    assert query_by_name["_count"] == ["100"]
    assert query_by_name["_lastUpdated"] == [f"lt{CUTOFF}"]


def test_comma_separated_resources_and_transient_binding_are_explicit():
    request = _request(resources="Organization,Practitioner")
    assert request is not None
    source_record = _source_record()
    assert current_version_census_contract(source_record) is None

    contract = bind_current_version_census_contract(request, [source_record])

    assert current_version_census_contract(source_record) is contract


@pytest.mark.parametrize("page_count", (True, False, 0, -1, 1.5, "100"))
def test_reviewed_start_url_requires_positive_integer_page_count(page_count):
    with pytest.raises(ValueError, match="page_count_invalid"):
        _bound_contract().start_url("Organization", page_count)


def test_count_url_preserves_filters_and_requires_accurate_total():
    count_url = current_version_census_count_url(
        _bound_contract().start_url("Organization", 100)
    )
    query_by_name = urllib.parse.parse_qs(
        urllib.parse.urlsplit(count_url).query,
        keep_blank_values=True,
    )

    assert query_by_name["status"] == ["active"]
    assert query_by_name["identifier"] == ["urn:synthetic"]
    assert query_by_name["_lastUpdated"] == [f"lt{CUTOFF}"]
    assert query_by_name["_summary"] == ["count"]
    assert query_by_name["_total"] == ["accurate"]
    assert "_count" not in query_by_name


def test_identity_binds_source_strategy_cutoff_resources_without_raw_urls():
    identity_by_field = _bound_contract().identity()

    assert identity_by_field["source_id"] == "synthetic-source"
    assert identity_by_field["strategy"] == (
        CURRENT_VERSION_CENSUS_STRATEGY_VERSION
    )
    assert identity_by_field["cutoff"] == CUTOFF
    assert identity_by_field["resources"] == list(RESOURCE_TYPES)
    assert identity_by_field["semantics"] == (
        "cutoff-bounded-current-version-census"
    )
    assert "directory.example.test" not in str(identity_by_field)


def test_count_vector_rejects_all_zero_and_expected_nonempty_zero():
    contract = _bound_contract()
    with pytest.raises(ValueError, match="all_zero_rejected"):
        validated_current_version_census_count_map(
            contract,
            {"Organization": 0, "Practitioner": 0},
        )
    with pytest.raises(ValueError, match="expected_nonempty_zero"):
        validated_current_version_census_count_map(
            contract,
            {"Organization": 0, "Practitioner": 2},
        )
    assert validated_current_version_census_count_map(
        contract,
        {"Organization": 2, "Practitioner": 0},
    ) == {"Organization": 2, "Practitioner": 0}
