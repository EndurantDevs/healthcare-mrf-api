import datetime as dt
from copy import deepcopy
from unittest.mock import AsyncMock

import pytest

from api import provider_directory_source_outcomes as outcomes


SOURCE_IDS = ("source-a", "source-b")
DATASET_ID = "pdds_current"
ENDPOINT_ID = "endpoint-current"
ROOT_RUN_ID = "root-current"
DATASET_HASH = "a" * 64
SELECTED_RESOURCES = (
    "InsurancePlan",
    "OrganizationAffiliation",
    "Practitioner",
)
RESOURCE_COUNTS = {
    "InsurancePlan": 2,
    "OrganizationAffiliation": 1,
    "Practitioner": 4,
}
TOTAL_RESOURCES = sum(RESOURCE_COUNTS.values())


class _MappingResult:
    def __init__(self, rows):
        self._rows = list(rows)

    def mappings(self):
        return self

    def all(self):
        return list(self._rows)


def _catalog(*, source_ids=SOURCE_IDS, canonical_base="https://old.test/fhir"):
    return {
        "schema_version": 1,
        "items": [
            {
                "entry_id": "example-directory",
                "source_ids": list(source_ids),
                "canonical_base": canonical_base,
                "runnable": True,
            }
        ],
    }


def _diagnostics(resource_counts=RESOURCE_COUNTS, *, overrides=None):
    diagnostic_by_resource = {
        resource_type: {
            "complete": True,
            "bounded": False,
            "error": None,
            "next_url_remaining": False,
            "rows_fetched": resource_count,
        }
        for resource_type, resource_count in resource_counts.items()
    }
    for resource_type, override in (overrides or {}).items():
        diagnostic_by_resource.setdefault(resource_type, {}).update(override)
    return diagnostic_by_resource


def _identity_proof(resource_counts=RESOURCE_COUNTS):
    return {
        "complete": True,
        "version": 1,
        "dataset_id": DATASET_ID,
        "endpoint_id": ENDPOINT_ID,
        "acquisition_root_run_id": ROOT_RUN_ID,
        "dataset_hash": DATASET_HASH,
        "source_ids": list(SOURCE_IDS),
        "selected_resources": list(SELECTED_RESOURCES),
        "resource_count": TOTAL_RESOURCES,
        "resource_counts": dict(resource_counts),
    }


def _relation_metadata():
    return {
        "dataset_network_plan": {
            "complete": True,
            "version": 1,
            "dataset_id": DATASET_ID,
            "acquisition_root_run_id": ROOT_RUN_ID,
            "insurance_plan_resource_count": 2,
            "edge_count": 5,
        },
        "dataset_affiliation_organization": {
            "complete": True,
            "version": 1,
            "dataset_id": DATASET_ID,
            "acquisition_root_run_id": ROOT_RUN_ID,
            "affiliation_resource_count": 1,
            "edge_count": 1,
        },
    }


def _metadata(*, source_ids=SOURCE_IDS, selected=SELECTED_RESOURCES, **extra):
    return {
        "source_ids": list(source_ids),
        "selected_resources": list(selected),
        **extra,
    }


def _dataset_row(**overrides):
    dataset_row_map = {
        "endpoint_id": ENDPOINT_ID,
        "dataset_id": DATASET_ID,
        "acquisition_root_run_id": ROOT_RUN_ID,
        "dataset_hash": DATASET_HASH,
        "status": "published",
        "is_current": True,
        "published_at": dt.datetime(2026, 7, 20, 8, 30),
        "resource_count": TOTAL_RESOURCES,
        "publication_metadata": _metadata(),
        "current_source_ids": SOURCE_IDS,
    }
    dataset_row_map.update(overrides)
    return dataset_row_map


def _expected_outcome_summary():
    return {
        "dataset_id": DATASET_ID,
        "status": "published",
        "is_current": True,
        "published_at": "2026-07-20T08:30:00+00:00",
        "total_resources": TOTAL_RESOURCES,
        "resource_counts": dict(sorted(RESOURCE_COUNTS.items())),
        "network_plan_links": 5,
        "organization_affiliation_links": 1,
    }


def test_outcome_normalizers_reject_invalid_and_internal_values():
    assert outcomes._clean_text(7) is None
    assert outcomes._utc_datetime("  ") is None
    assert outcomes._utc_datetime("not-a-timestamp") is None
    assert outcomes._utc_datetime(7) is None
    assert outcomes._utc_datetime(dt.datetime(2026, 7, 20, 10, tzinfo=dt.UTC)).hour == 10
    assert outcomes._valid_nonnegative_count(True) is None
    assert outcomes._normalized_text_tuple(["source-a", "source-a"]) is None
    assert outcomes._normalized_text_tuple(
        ["Practitioner", "LU:Practitioner:pass:1"],
        reject_internal_resources=True,
    ) is None


def test_current_dataset_query_is_metadata_only_and_uses_sealed_total():
    selected_sql = str(outcomes._current_published_dataset_statement())

    assert "provider_directory_endpoint_dataset" in selected_sql
    assert "provider_directory_dataset_resource" not in selected_sql
    assert "provider_directory_api_endpoint" not in selected_sql
    assert "provider_directory_source" in selected_sql
    assert "array_agg" in selected_sql.lower()
    assert "resource_count" in selected_sql
    assert "GROUP BY" not in selected_sql
    assert "count(" not in selected_sql.lower()


@pytest.mark.asyncio
async def test_empty_catalog_inputs_do_not_query_the_database(monkeypatch):
    database_execute = AsyncMock()
    monkeypatch.setattr(outcomes.db, "execute", database_execute)

    assert await outcomes._current_published_dataset_by_source_ids(set()) == {}
    assert await outcomes.enrich_provider_directory_source_catalog(
        {"items": None}
    ) == {"items": None}
    assert await outcomes.enrich_provider_directory_source_catalog(
        {"items": []}
    ) == {"items": []}
    database_execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_versioned_proof_adds_exact_current_dataset_outcome(monkeypatch):
    publication_metadata = _metadata(
        outcome_resource_counts_v1=_identity_proof(),
        **_relation_metadata(),
    )
    database_execute = AsyncMock(
        return_value=_MappingResult(
            [_dataset_row(publication_metadata=publication_metadata)]
        )
    )
    monkeypatch.setattr(outcomes.db, "execute", database_execute)
    catalog = _catalog()
    original_catalog = deepcopy(catalog)

    enriched_catalog = await outcomes.enrich_provider_directory_source_catalog(
        catalog
    )

    assert catalog == original_catalog
    assert enriched_catalog["items"][0]["outcome_summary"] == (
        _expected_outcome_summary()
    )
    database_execute.assert_awaited_once()


@pytest.mark.asyncio
async def test_versioned_proof_reports_linked_supported_resources(monkeypatch):
    linked_count_by_resource = {"Organization": 3, "Practitioner": 4}
    proof = _identity_proof(linked_count_by_resource)
    proof["selected_resources"] = ["Practitioner"]
    proof["resource_count"] = 7
    publication_metadata = _metadata(
        selected=("Practitioner",),
        outcome_resource_counts_v1=proof,
    )
    monkeypatch.setattr(
        outcomes.db,
        "execute",
        AsyncMock(
            return_value=_MappingResult(
                [_dataset_row(publication_metadata=publication_metadata)]
            )
        ),
    )

    summary = (
        await outcomes.enrich_provider_directory_source_catalog(_catalog())
    )["items"][0]["outcome_summary"]

    assert summary["resource_counts"] == linked_count_by_resource


@pytest.mark.asyncio
async def test_matched_twin_proof_precedes_unusable_diagnostics(monkeypatch):
    twin_proof = _identity_proof()
    twin_proof.pop("dataset_id")
    twin_proof.pop("complete")
    twin_proof.pop("version")
    publication_metadata = _metadata(
        twin_root_verification_v1={
            "result": "matched",
            "mismatch_fields": [],
            "proof": twin_proof,
        },
        resource_diagnostics=_diagnostics(
            overrides={"Practitioner": {"bounded": True}}
        ),
    )
    monkeypatch.setattr(
        outcomes.db,
        "execute",
        AsyncMock(
            return_value=_MappingResult(
                [_dataset_row(publication_metadata=publication_metadata)]
            )
        ),
    )

    summary = (
        await outcomes.enrich_provider_directory_source_catalog(_catalog())
    )["items"][0]["outcome_summary"]

    assert summary["resource_counts"] == dict(sorted(RESOURCE_COUNTS.items()))


@pytest.mark.asyncio
async def test_complete_reconciled_diagnostics_are_safe_legacy_fallback(
    monkeypatch,
):
    publication_metadata = _metadata(
        resource_diagnostics=_diagnostics(),
        **_relation_metadata(),
    )
    monkeypatch.setattr(
        outcomes.db,
        "execute",
        AsyncMock(
            return_value=_MappingResult(
                [_dataset_row(publication_metadata=publication_metadata)]
            )
        ),
    )

    summary = (
        await outcomes.enrich_provider_directory_source_catalog(_catalog())
    )["items"][0]["outcome_summary"]

    assert summary == _expected_outcome_summary()


@pytest.mark.parametrize(
    ("resource_count", "overrides"),
    [
        (TOTAL_RESOURCES + 1, {}),
        (TOTAL_RESOURCES, {"Practitioner": {"complete": False}}),
        (TOTAL_RESOURCES, {"Practitioner": {"bounded": True}}),
        (TOTAL_RESOURCES, {"Practitioner": {"next_url_remaining": True}}),
        (TOTAL_RESOURCES, {"Practitioner": {"error": "timed out"}}),
        (TOTAL_RESOURCES, {"Practitioner": {"rows_fetched": -1}}),
        (TOTAL_RESOURCES, {"Practitioner": {"rows_fetched": True}}),
    ],
)
@pytest.mark.asyncio
async def test_unreconciled_or_incomplete_diagnostics_return_total_only(
    monkeypatch,
    resource_count,
    overrides,
):
    publication_metadata = _metadata(
        resource_diagnostics=_diagnostics(overrides=overrides)
    )
    monkeypatch.setattr(
        outcomes.db,
        "execute",
        AsyncMock(
            return_value=_MappingResult(
                [
                    _dataset_row(
                        resource_count=resource_count,
                        publication_metadata=publication_metadata,
                    )
                ]
            )
        ),
    )

    summary = (
        await outcomes.enrich_provider_directory_source_catalog(_catalog())
    )["items"][0]["outcome_summary"]

    assert summary["total_resources"] == resource_count
    assert "resource_counts" not in summary
    assert "network_plan_links" not in summary


@pytest.mark.asyncio
async def test_internal_lookup_rows_never_surface_as_resources(monkeypatch):
    proof = _identity_proof(
        {
            **RESOURCE_COUNTS,
            "LU:Practitioner:pass:1": 1,
        }
    )
    monkeypatch.setattr(
        outcomes.db,
        "execute",
        AsyncMock(
            return_value=_MappingResult(
                [
                    _dataset_row(
                        publication_metadata=_metadata(
                            outcome_resource_counts_v1=proof
                        )
                    )
                ]
            )
        ),
    )

    summary = (
        await outcomes.enrich_provider_directory_source_catalog(_catalog())
    )["items"][0]["outcome_summary"]

    assert summary["total_resources"] == TOTAL_RESOURCES
    assert "resource_counts" not in summary


@pytest.mark.asyncio
async def test_catalog_maps_by_exact_source_ids_not_canonical_base(monkeypatch):
    newest_exact = _dataset_row(
        endpoint_id="endpoint-new",
        dataset_id="pdds-new",
        published_at=dt.datetime(2026, 7, 20, 10, 0),
        publication_metadata=_metadata(resource_diagnostics=_diagnostics()),
    )
    older_exact = _dataset_row(
        endpoint_id="endpoint-old",
        dataset_id="pdds-old",
        published_at=dt.datetime(2026, 7, 19, 10, 0),
        publication_metadata=_metadata(resource_diagnostics=_diagnostics()),
    )
    newer_wrong_alias = _dataset_row(
        endpoint_id="endpoint-wrong",
        dataset_id="pdds-wrong",
        published_at=dt.datetime(2026, 7, 21, 10, 0),
        publication_metadata=_metadata(
            source_ids=("source-a",),
            resource_diagnostics=_diagnostics(),
        ),
    )
    monkeypatch.setattr(
        outcomes.db,
        "execute",
        AsyncMock(
            return_value=_MappingResult(
                [newest_exact, older_exact, newer_wrong_alias]
            )
        ),
    )

    enriched_catalog = await outcomes.enrich_provider_directory_source_catalog(
        _catalog(canonical_base="https://redirected.example/fhir")
    )

    assert enriched_catalog["items"][0]["outcome_summary"]["dataset_id"] == (
        "pdds-new"
    )


@pytest.mark.asyncio
async def test_unknown_or_noncurrent_dataset_is_omitted(monkeypatch):
    monkeypatch.setattr(
        outcomes.db,
        "execute",
        AsyncMock(
            return_value=_MappingResult(
                [
                    _dataset_row(
                        publication_metadata=_metadata(
                            source_ids=("unknown",)
                        )
                    ),
                    _dataset_row(status="validated"),
                    _dataset_row(is_current=False),
                ]
            )
        ),
    )

    enriched_catalog = await outcomes.enrich_provider_directory_source_catalog(
        _catalog()
    )

    assert "outcome_summary" not in enriched_catalog["items"][0]


@pytest.mark.asyncio
async def test_mismatched_relation_proof_is_omitted(monkeypatch):
    relation_metadata = _relation_metadata()
    relation_metadata["dataset_network_plan"][
        "insurance_plan_resource_count"
    ] = 999
    relation_metadata["dataset_affiliation_organization"][
        "acquisition_root_run_id"
    ] = "another-root"
    publication_metadata = _metadata(
        resource_diagnostics=_diagnostics(),
        **relation_metadata,
    )
    monkeypatch.setattr(
        outcomes.db,
        "execute",
        AsyncMock(
            return_value=_MappingResult(
                [_dataset_row(publication_metadata=publication_metadata)]
            )
        ),
    )

    summary = (
        await outcomes.enrich_provider_directory_source_catalog(_catalog())
    )["items"][0]["outcome_summary"]

    assert "network_plan_links" not in summary
    assert "organization_affiliation_links" not in summary


@pytest.mark.asyncio
async def test_zero_resource_dataset_keeps_explicit_per_family_zeroes(monkeypatch):
    zero_counts = dict.fromkeys(SELECTED_RESOURCES, 0)
    publication_metadata = _metadata(
        resource_diagnostics=_diagnostics(zero_counts)
    )
    monkeypatch.setattr(
        outcomes.db,
        "execute",
        AsyncMock(
            return_value=_MappingResult(
                [
                    _dataset_row(
                        resource_count=0,
                        publication_metadata=publication_metadata,
                    )
                ]
            )
        ),
    )

    summary = (
        await outcomes.enrich_provider_directory_source_catalog(_catalog())
    )["items"][0]["outcome_summary"]

    assert summary["total_resources"] == 0
    assert summary["resource_counts"] == dict(sorted(zero_counts.items()))
