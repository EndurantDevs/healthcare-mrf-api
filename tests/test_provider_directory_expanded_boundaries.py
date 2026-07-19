# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import datetime
import importlib
from unittest.mock import AsyncMock

import pytest

from api import provider_directory_sources


importer = importlib.import_module("process.provider_directory_fhir")


def _source_record(api_base: str, **metadata_by_name):
    return {
        "source_id": "source_a",
        "api_base": api_base,
        "canonical_api_base": api_base,
        "metadata_json": metadata_by_name,
    }


def _bundle_payload(*resource_items, total=None):
    bundle_by_field = {
        "resourceType": "Bundle",
        "type": "searchset",
        "entry": [
            resource_item
            if isinstance(resource_item, dict) and "resource" in resource_item
            else {"resource": resource_item}
            for resource_item in resource_items
        ],
    }
    if total is not None:
        bundle_by_field["total"] = total
    return bundle_by_field


def _resume_state(completeness_by_name, *, resumed=True, pages=1, rows=1):
    return importer.PaginationResumeState(
        next_url="https://example.test/next",
        pages_processed=pages,
        rows_processed=rows,
        recent_url_hashes=(),
        resumed=resumed,
        completeness=completeness_by_name,
    )


def test_public_catalog_rejects_duplicate_documented_resources():
    """Do not expose ambiguous resource support in the public catalog."""
    with pytest.raises(
        RuntimeError,
        match="provider_directory_source_manifest_invalid",
    ):
        provider_directory_sources._public_catalog_entry(
            {
                "entry_id": "candidate",
                "classification": "acquisition",
                "source_ids": ["pdfhir_candidate"],
                "resources": ["Practitioner"],
            },
            {
                "candidate": {
                    "documented_resources": ["Practitioner", "Practitioner"]
                }
            },
        )


def test_import_and_country_normalization_cover_fallbacks(monkeypatch):
    """Keep generated import IDs stable and preserve non-US country values."""
    monkeypatch.delenv("HLTHPRT_PROVIDER_DIRECTORY_IMPORT_ID", raising=False)
    monkeypatch.delenv("HLTHPRT_IMPORT_ID_OVERRIDE", raising=False)
    monkeypatch.setattr(
        importer,
        "_now",
        lambda: datetime.datetime(2026, 7, 20, 1, 2, 3),
    )

    assert importer._normalize_import_id("run ! 42") == "run42"
    assert importer._normalize_import_id("---") == "20260720010203"
    assert importer._normalize_import_id(None) == "20260720010203"
    monkeypatch.setenv("HLTHPRT_PROVIDER_DIRECTORY_IMPORT_ID", "env-run")
    assert importer._normalize_import_id(None) == "envrun"

    assert importer._normalize_country_code("ca") == "CA"
    assert importer._normalize_country_code("Cote d'Ivoire") == "COTE D'IVOIRE"


def test_resource_page_cap_reads_legacy_and_resource_specific_metadata():
    """Honor both documented cap shapes while rejecting non-positive values."""
    lower_case_source = _source_record(
        "https://example.test/fhir",
        provider_directory_resource_page_count_caps={"practitioner": "7"},
    )
    legacy_source = _source_record(
        "https://example.test/fhir",
        provider_directory_resource_page_count_caps={"Practitioner": "bad"},
        provider_directory_resource_page_count_cap={"practitioner": 5},
    )

    assert (
        importer._metadata_resource_page_count_cap(
            lower_case_source,
            "Practitioner",
        )
        == 7
    )
    assert (
        importer._metadata_resource_page_count_cap(legacy_source, "Practitioner")
        == 5
    )
    assert (
        importer._metadata_resource_page_count_cap(
            _source_record("https://example.test/fhir"),
            "Practitioner",
        )
        is None
    )


def test_uhc_location_partition_rejects_predicate_violations():
    """Fail closed when a UHC partition returns rows outside its state or city."""
    source_record = _source_record(importer.UHC_PROVIDER_DIRECTORY_BASE)
    base_url = importer.UHC_PROVIDER_DIRECTORY_BASE + "/Location?_count=100"

    assert (
        importer._uhc_location_partition_predicate_error(
            source_record,
            "Location",
            base_url,
            _bundle_payload(),
        )
        is None
    )
    assert (
        importer._uhc_location_partition_predicate_error(
            source_record,
            "Location",
            base_url + "&address-state=CA",
            _bundle_payload({"resourceType": "Patient"}),
        )
        is None
    )
    assert (
        importer._uhc_location_partition_predicate_error(
            source_record,
            "Location",
            base_url + "&address-state=CA",
            _bundle_payload({"resourceType": "Location", "address": None}),
        )
        == "uhc_location_address_state_predicate_rejected"
    )
    assert (
        importer._uhc_location_partition_predicate_error(
            source_record,
            "Location",
            base_url + "&address-state=CA&address-city=San",
            _bundle_payload(
                {
                    "resourceType": "Location",
                    "address": {"state": "ca", "city": "Los Angeles"},
                }
            ),
        )
        == "uhc_location_address_city_predicate_rejected"
    )
    assert (
        importer._uhc_location_partition_predicate_error(
            source_record,
            "Location",
            base_url + "&address-state=CA&address-city=San",
            _bundle_payload(
                {
                    "resourceType": "Location",
                    "address": {"state": "CA", "city": "San Mateo"},
                }
            ),
        )
        is None
    )


def test_profile_contact_normalizers_drop_invalid_and_duplicate_values():
    """Retain only usable organization contacts, telecom, and addresses."""
    assert importer._normalized_organization_contacts("invalid") == []
    assert importer._normalized_organization_contacts([None, {}]) == []
    assert importer._normalized_organization_contacts(
        {"telecom": [None, {"system": "phone", "value": "5550100"}]}
    ) == [{"telecom": [{"system": "phone", "value": "5550100"}]}]

    telecom_rows = importer._alohr_telecom(
        None,
        [None, {"system": "phone", "value": "5550100"}],
        {
            "contacts": [
                None,
                {"system": "email", "value": "directory@example.test"},
                {"system": "phone", "value": None},
            ]
        },
        "directory@example.test",
        "",
    )
    assert telecom_rows == [
        {"system": "phone", "value": "5550100"},
        {"system": "email", "value": "directory@example.test"},
    ]

    address_rows = importer._alohr_address_items(
        {
            "addresses": [
                None,
                {},
                {
                    "street1": "1 Main",
                    "city": "Town",
                    "state": "IA",
                    "county": "Story",
                },
                {
                    "street1": "1 Main",
                    "city": "Town",
                    "state": "IA",
                    "county": "Story",
                },
            ]
        }
    )
    assert len(address_rows) == 1
    assert address_rows[0]["district"] == "Story"


def test_candidate_artifact_relations_include_every_enabled_safe_target():
    """Prepare all requested relation aliases except an explicitly skipped profile."""
    enabled_targets = {
        "address_overlay",
        "profile",
        "network_catalog",
        "corroboration",
    }
    relation_names = importer._candidate_artifact_deferred_relations(
        enabled_targets,
        {"profile": {"skipped": True, "reason": "not ready"}},
    )

    assert importer.PROVIDER_DIRECTORY_ADDRESS_OVERLAY_TABLE in relation_names
    assert importer.PROVIDER_DIRECTORY_NETWORK_CATALOG_TABLE in relation_names
    assert importer.PROVIDER_DIRECTORY_ADDRESS_CORROBORATION_VIEW in relation_names
    assert importer.profile_artifact.PROFILE_TABLE not in relation_names


@pytest.mark.asyncio
async def test_network_catalog_prerequisites_fail_closed_and_complete(monkeypatch):
    """Report the first absent relation and accept a complete schema."""
    table_exists = AsyncMock(side_effect=[False])
    monkeypatch.setattr(importer, "_table_exists", table_exists)
    assert (
        await importer._network_catalog_missing_requirement("mrf")
        == "provider_directory_source_unavailable"
    )

    table_exists = AsyncMock(return_value=True)
    monkeypatch.setattr(importer, "_table_exists", table_exists)
    assert await importer._network_catalog_missing_requirement("mrf") is None
    assert table_exists.await_count == len(
        importer.PROVIDER_DIRECTORY_NETWORK_CATALOG_REQUIRED_TABLES
    )


def test_status_count_and_http_classification_cover_error_boundaries():
    """Normalize SQL counts and classify transport and status failures."""
    assert importer._status_row_count(4) == 4
    assert importer._status_row_count(None) == 0
    assert importer._status_row_count("UPDATE 12") == 12
    assert importer._status_row_count("UPDATE") == 0

    assert importer._classify_http(None, "request timeout", None) == "timeout"
    assert importer._classify_http(None, "SSL certificate failed", None) == "ssl_error"
    assert importer._classify_http(None, "getaddrinfo failed", None) == "dns_failure"
    assert importer._classify_http(None, "connection reset", None) == "unreachable"
    assert importer._classify_http(404, None, None) == "not_found"
    assert importer._classify_http(422, None, None) == "client_error"
    assert importer._classify_http(503, None, None) == "server_error"
    assert importer._classify_http(None, None, None) == "unreachable"


def test_source_error_and_retry_classes_cover_malformed_payloads():
    """Extract safe diagnostics without trusting arbitrary OperationOutcome items."""
    assert importer._source_error_payload_text(None) == ""
    assert importer._source_error_payload_text(
        {
            "message": "Top level",
            "issue": [
                None,
                {"diagnostics": "Diagnostic", "details": {"text": "Detail"}},
            ],
        }
    ) == "top level diagnostic detail"

    assert importer._source_fetch_response_class(None, "reset") == (
        "transient_transport_error"
    )
    assert importer._source_fetch_response_class(429, None) == (
        "transient_rate_limited"
    )
    assert importer._source_fetch_response_class(404, None) is None


def test_synthetic_pagination_rejects_unconfigured_and_malformed_urls():
    """Never synthesize continuation URLs outside reviewed position contracts."""
    unconfigured_source = _source_record("https://example.test/fhir")
    with pytest.raises(ValueError, match="synthetic skip pagination is not configured"):
        importer._synthetic_skip_pagination_next_url(
            unconfigured_source,
            "https://example.test/fhir/Practitioner?_count=10&_skip=0&_sort=_id",
            10,
        )
    with pytest.raises(ValueError, match="synthetic offset pagination is not configured"):
        importer._synthetic_offset_pagination_next_url(
            unconfigured_source,
            "https://example.test/fhir/Practitioner?_count=10&_offset=0&_sort=_id",
            10,
        )

    skip_source = _source_record(importer.IOWA_MEDICAID_PROVIDER_DIRECTORY_BASE)
    with pytest.raises(ValueError, match="invalid synthetic skip pagination URL"):
        importer._synthetic_skip_pagination_next_url(
            skip_source,
            importer.IOWA_MEDICAID_PROVIDER_DIRECTORY_BASE + "/Practitioner?_count=10",
            10,
        )
    offset_source = _source_record(
        importer.SAN_MATEO_COUNTY_PROVIDER_DIRECTORY_BASE
    )
    with pytest.raises(ValueError, match="invalid synthetic offset pagination URL"):
        importer._synthetic_offset_pagination_next_url(
            offset_source,
            importer.SAN_MATEO_COUNTY_PROVIDER_DIRECTORY_BASE
            + "/Practitioner?_count=10",
            10,
        )


def test_synthetic_page_identity_uses_id_url_and_payload_fallbacks():
    """Fingerprint every synthetic page row even when upstream IDs are absent."""
    source_record = _source_record(importer.IOWA_MEDICAID_PROVIDER_DIRECTORY_BASE)
    page_identity = importer._synthetic_position_page_identity(
        source_record,
        [
            None,
            {"resource": {"resourceType": "Practitioner", "id": "p1"}},
            {
                "resource": {"resourceType": "Practitioner"},
                "fullUrl": "https://example.test/Practitioner/p2",
            },
            {"resource": {"active": True}},
        ],
    )

    assert page_identity is not None
    resource_identities, page_fingerprint = page_identity
    assert resource_identities[0] == "Practitioner/p1"
    assert resource_identities[1] == "url:https://example.test/Practitioner/p2"
    assert resource_identities[2].startswith("payload:")
    assert len(page_fingerprint) == 64


def test_synthetic_resume_guard_rejects_malformed_and_accepts_valid_state():
    """Resume only from a versioned page guard with string identities and hashes."""
    guard_key = importer.SYNTHETIC_POSITION_PAGE_GUARD_KEY
    guard_version = importer.SYNTHETIC_POSITION_PAGE_GUARD_VERSION

    assert importer._synthetic_position_resume_guard(
        _resume_state({guard_key: {"strategy_version": "old"}})
    ) == ((), ())
    assert importer._synthetic_position_resume_guard(
        _resume_state(
            {
                guard_key: {
                    "strategy_version": guard_version,
                    "resource_identities": [1],
                    "recent_page_fingerprints": ["hash"],
                }
            }
        )
    ) == ((), ())
    assert importer._synthetic_position_resume_guard(
        _resume_state(
            {
                guard_key: {
                    "strategy_version": guard_version,
                    "resource_identities": ["Practitioner/p1"],
                    "recent_page_fingerprints": [""],
                }
            }
        )
    ) == ((), ())

    valid_state = _resume_state(
        {
            guard_key: {
                "strategy_version": guard_version,
                "resource_identities": ["Practitioner/p1"],
                "recent_page_fingerprints": ["hash"],
            }
        }
    )
    assert importer._synthetic_position_resume_guard(valid_state) == (
        ("Practitioner/p1",),
        ("hash",),
    )
    assert (
        importer._synthetic_position_resume_guard_error(
            _source_record(importer.IOWA_MEDICAID_PROVIDER_DIRECTORY_BASE),
            valid_state,
        )
        is None
    )


def test_synthetic_page_guard_detects_repetition_and_serializes_progress():
    """Reject duplicates within or across pages and persist the safe identity."""
    assert importer._synthetic_position_page_guard_error(
        (("Practitioner/p1", "Practitioner/p1"), "hash-a"),
        (),
        (),
    ) == "pagination_page_repeated"
    assert importer._synthetic_position_page_guard_error(
        (("Practitioner/p2",), "hash-b"),
        ("Practitioner/p2",),
        (),
    ) == "pagination_page_repeated"

    completeness_by_name = importer._synthetic_position_page_guard_completeness(
        (("Practitioner/p3",), "hash-c"),
        ("hash-a", "hash-b"),
    )
    assert completeness_by_name == {
        importer.SYNTHETIC_POSITION_PAGE_GUARD_KEY: {
            "strategy_version": importer.SYNTHETIC_POSITION_PAGE_GUARD_VERSION,
            "resource_identities": ["Practitioner/p3"],
            "page_fingerprint": "hash-c",
            "recent_page_fingerprints": ["hash-a", "hash-b"],
        }
    }
