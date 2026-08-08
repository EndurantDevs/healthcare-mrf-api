# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Fail-closed boundaries for the manual current-version census."""

from __future__ import annotations

import datetime
import pytest

from process.provider_directory_fhir_census_binding import (
    bind_current_version_census_contract,
    validated_current_version_census_count_map,
)
from process.provider_directory_fhir_census_contract import (
    CURRENT_VERSION_CENSUS_CONTINUATION_STRATEGY_FIELD,
    CURRENT_VERSION_CENSUS_METADATA_STRATEGY_FIELD,
    CURRENT_VERSION_CENSUS_SMILE_CONTINUATION_STRATEGY,
    CURRENT_VERSION_CENSUS_START_URLS_FIELD,
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


def _request(resources=RESOURCE_TYPES, **overrides):
    task_by_field = {
        "provider_directory_acquisition_strategy": EXACT_STRATEGY,
        "provider_directory_census_cutoff": CUTOFF,
        "source_ids": ["synthetic-source"],
        "resources": list(resources) if isinstance(resources, tuple) else resources,
        "import_resources": True,
    }
    task_by_field.update(overrides)
    request = current_version_census_request(
        task_by_field,
        allowed_resources=RESOURCE_TYPES,
        now=datetime.datetime(2026, 8, 2, tzinfo=datetime.UTC),
    )
    assert request is not None
    return request


def _source_record():
    return {
        "source_id": "synthetic-source",
        "api_base": "https://directory.example.test/fhir",
        "canonical_api_base": "https://directory.example.test/fhir",
        "auth_type": "none",
        "last_validated_status": "valid",
        "metadata_json": {
            "provider_directory_manual_only": True,
            CURRENT_VERSION_CENSUS_METADATA_STRATEGY_FIELD: EXACT_STRATEGY,
            CURRENT_VERSION_CENSUS_CONTINUATION_STRATEGY_FIELD: (
                CURRENT_VERSION_CENSUS_SMILE_CONTINUATION_STRATEGY
            ),
            "provider_directory_supported_resources": list(RESOURCE_TYPES),
            "provider_directory_fully_enumerable_resources": list(
                RESOURCE_TYPES
            ),
            "provider_directory_expected_nonempty_resources": [
                "Organization"
            ],
            CURRENT_VERSION_CENSUS_START_URLS_FIELD: {
                "Organization": (
                    "https://directory.example.test/fhir/Organization?"
                    "status=active"
                ),
                "Practitioner": (
                    "https://directory.example.test/fhir/Practitioner?"
                    "active=true"
                ),
            },
        },
    }


def _bind_source_record(source_record):
    return bind_current_version_census_contract(_request(), [source_record])


def _bound_contract(start_url_map):
    source_record = _source_record()
    source_record["metadata_json"][CURRENT_VERSION_CENSUS_START_URLS_FIELD] = (
        start_url_map
    )
    return _bind_source_record(source_record)


@pytest.mark.parametrize(
    "alias_override",
    (
        {"source_id": "other-source"},
        {"provider_directory_source_id": "synthetic-source"},
        {"provider_directory_source_ids": ["synthetic-source"]},
    ),
)
def test_request_rejects_multiple_source_id_aliases(alias_override):
    with pytest.raises(ValueError, match="source_id_aliases_conflict"):
        _request(**alias_override)


@pytest.mark.parametrize(
    "invalid_base",
    (
        "https://127.0.0.1/fhir",
        "https://8.8.8.8/fhir",
        "https://[2001:db8::1]/fhir",
        "https://localhost/fhir",
        "https://service.localhost/fhir",
        "https://directory.local/fhir",
        "https://intranet/fhir",
        "https://user@directory.example.test/fhir",
        "https://directory.example.test:8443/fhir",
        "https://directory.example.test:/fhir",
        "https://directory.example.test:0443/fhir",
        "https://directory.example.test:99999/fhir",
        "https://directory.example.test/fhir#section",
    ),
)
def test_canonical_base_requires_unambiguous_public_https_host(invalid_base):
    source_record = _source_record()
    source_record["api_base"] = invalid_base
    source_record["canonical_api_base"] = invalid_base

    with pytest.raises(ValueError, match="base_url_invalid"):
        _bind_source_record(source_record)


@pytest.mark.parametrize(
    "invalid_start_url",
    (
        "https://127.0.0.1/fhir/Organization",
        "https://8.8.8.8/fhir/Organization",
        "https://[2001:db8::1]/fhir/Organization",
        "https://localhost/fhir/Organization",
        "https://service.localhost/fhir/Organization",
        "https://directory.local/fhir/Organization",
        "https://intranet/fhir/Organization",
        "https://user@directory.example.test/fhir/Organization",
        "https://directory.example.test:8443/fhir/Organization",
        "https://directory.example.test:/fhir/Organization",
        "https://directory.example.test:0443/fhir/Organization",
        "https://directory.example.test:bad/fhir/Organization",
        "https://directory.example.test/fhir/Organization#section",
    ),
)
def test_reviewed_start_url_uses_same_public_https_rules(invalid_start_url):
    start_url_map = dict(
        _source_record()["metadata_json"][CURRENT_VERSION_CENSUS_START_URLS_FIELD]
    )
    start_url_map["Organization"] = invalid_start_url

    with pytest.raises(ValueError, match="base_url_invalid"):
        _bound_contract(start_url_map)


def test_reviewed_start_url_allows_explicit_default_https_port():
    start_url_map = dict(
        _source_record()["metadata_json"][CURRENT_VERSION_CENSUS_START_URLS_FIELD]
    )
    start_url_map["Organization"] = (
        "https://directory.example.test:443/fhir/Organization?status=active"
    )

    assert _bound_contract(start_url_map).start_url(
        "Organization",
        100,
    ).startswith("https://directory.example.test:443/")


@pytest.mark.parametrize(
    "last_updated_filter",
    (
        "_lastUpdated=ge2025-01-01T00%3A00%3A00Z",
        "_LASTUPDATED=lt2026-01-01T00%3A00%3A00Z",
        "_lastUpdated%3Amissing=false",
    ),
)
def test_reviewed_start_url_rejects_inherited_cutoff(last_updated_filter):
    start_url_map = dict(
        _source_record()["metadata_json"][CURRENT_VERSION_CENSUS_START_URLS_FIELD]
    )
    start_url_map["Organization"] += f"&{last_updated_filter}"

    with pytest.raises(ValueError, match="contains_last_updated"):
        _bound_contract(start_url_map)


def test_request_accepts_json_resource_vector():
    assert _request(resources='["Organization"]'.strip()).resources == (
        "Organization",
    )


@pytest.mark.parametrize(
    ("task_override", "error"),
    (
        ({"resources": "["}, "resources_invalid_json"),
        ({"resources": 42}, "resources_must_be_sequence"),
        ({"resources": [42]}, "resources_entries_must_be_strings"),
        ({"resources": [" "]}, "resources_entries_must_not_be_empty"),
        ({"provider_directory_census_cutoff": None}, "cutoff_required"),
        ({"import_resources": False}, "import_resources_required"),
        ({"resources": None}, "resources_required"),
    ),
)
def test_request_rejects_malformed_required_fields(task_override, error):
    with pytest.raises(ValueError, match=error):
        _request(**task_override)


def test_configured_request_is_absent_without_census_fields():
    assert current_version_census_request(
        {"provider_directory_acquisition_strategy": "configured"},
        allowed_resources=RESOURCE_TYPES,
    ) is None


def test_request_rejects_naive_validation_clock():
    with pytest.raises(ValueError, match="now_timezone_required"):
        current_version_census_request(
            {
                "provider_directory_acquisition_strategy": EXACT_STRATEGY,
                "provider_directory_census_cutoff": CUTOFF,
                "source_ids": ["synthetic-source"],
                "resources": ["Organization"],
                "import_resources": True,
            },
            allowed_resources=RESOURCE_TYPES,
            now=datetime.datetime(2026, 8, 2),
        )


def test_runtime_validator_requires_typed_request():
    with pytest.raises(TypeError, match="request required"):
        validate_current_version_census_runtime(
            None,
            CurrentVersionCensusRuntime(
                checkpointing_enabled=True,
                full_refresh=True,
                resource_limit=0,
                page_limit=0,
                stream_batch_size=1,
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
            ),
        )


@pytest.mark.parametrize(
    ("record_override", "error"),
    (
        ({"source_id": "different-source"}, "source_identity_mismatch"),
        ({"metadata_json": None}, "source_metadata_required"),
        (
            {"api_base": None, "canonical_api_base": None},
            "base_url_invalid",
        ),
    ),
)
def test_source_binding_rejects_missing_identity_metadata_or_base(
    record_override,
    error,
):
    source_record = _source_record()
    source_record.update(record_override)

    with pytest.raises(ValueError, match=error):
        _bind_source_record(source_record)


@pytest.mark.parametrize(
    ("start_url_map", "error"),
    (
        (None, "start_urls_required"),
        (
            {"Organization": "https://directory.example.test/fhir/Organization"},
            "start_urls_must_match_resources",
        ),
        (
            {
                "Organization": None,
                "Practitioner": "https://directory.example.test/fhir/Practitioner",
            },
            "start_url_invalid",
        ),
        (
            {
                "Organization": "https://directory.example.test/fhir/Location",
                "Practitioner": "https://directory.example.test/fhir/Practitioner",
            },
            "start_url_path_mismatch",
        ),
        (
            {
                "Organization": "https://directory.example.test/fhir/Organization?_page=2",
                "Practitioner": "https://directory.example.test/fhir/Practitioner",
            },
            "contains_continuation",
        ),
        (
            {
                "Organization": "https://directory.example.test/fhir/Organization?_summary=count",
                "Practitioner": "https://directory.example.test/fhir/Practitioner",
            },
            "contains_count_control",
        ),
    ),
)
def test_reviewed_start_url_map_rejects_unbound_or_ambiguous_shapes(
    start_url_map,
    error,
):
    source_record = _source_record()
    source_record["metadata_json"][CURRENT_VERSION_CENSUS_START_URLS_FIELD] = (
        start_url_map
    )

    with pytest.raises(ValueError, match=error):
        _bind_source_record(source_record)


def test_binding_requires_exactly_one_source_record():
    request = _request()
    with pytest.raises(ValueError, match="source_resolution_ambiguous"):
        bind_current_version_census_contract(request, [])
    with pytest.raises(ValueError, match="source_resolution_ambiguous"):
        bind_current_version_census_contract(
            request,
            [_source_record(), _source_record()],
        )


def test_contract_rejects_unbound_resource_type():
    with pytest.raises(ValueError, match="resource_not_bound"):
        _bind_source_record(_source_record()).start_url("Location", 100)


@pytest.mark.parametrize("invalid_count", (True, -1, 1.5, "1"))
def test_count_vector_requires_exact_nonnegative_integers(invalid_count):
    contract = _bind_source_record(_source_record())
    with pytest.raises(ValueError, match="count_invalid"):
        validated_current_version_census_count_map(
            contract,
            {"Organization": invalid_count, "Practitioner": 1},
        )


def test_count_vector_requires_exact_resource_set():
    with pytest.raises(ValueError, match="count_resources_mismatch"):
        validated_current_version_census_count_map(
            _bind_source_record(_source_record()),
            {"Organization": 1},
        )
