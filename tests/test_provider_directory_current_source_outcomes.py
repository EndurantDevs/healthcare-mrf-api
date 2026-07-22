"""Current published Provider Directory source outcome contract tests."""

import datetime as dt
from unittest.mock import AsyncMock

import pytest

from api import provider_directory_source_outcomes as outcomes
from tests.test_provider_directory_source_outcomes import (
    DATASET_HASH,
    DATASET_ID,
    ENDPOINT_ID,
    ROOT_RUN_ID,
    TOTAL_RESOURCES,
    _catalog,
    _dataset_row,
    _expected_outcome_summary,
    _identity_proof,
    _MappingResult,
    _metadata,
    _relation_metadata,
)


@pytest.mark.asyncio
async def test_current_outcome_exposes_exact_published_identity(monkeypatch):
    """Expose lineage identity without changing the static catalog digest."""

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
    catalog_map = {**_catalog(), "catalog_digest": "c" * 64}

    enriched_catalog = await outcomes.enrich_provider_directory_source_catalog(
        catalog_map
    )

    assert enriched_catalog["catalog_digest"] == catalog_map["catalog_digest"]
    assert enriched_catalog["items"][0]["current_outcome_summary"] == {
        **_expected_outcome_summary(),
        "endpoint_id": ENDPOINT_ID,
        "acquisition_root_run_id": ROOT_RUN_ID,
        "dataset_hash": DATASET_HASH,
    }
    assert database_execute.await_count == 2


@pytest.mark.asyncio
async def test_newer_validated_outcome_keeps_published_incumbent_current(
    monkeypatch,
):
    """Keep latest-candidate reporting separate from Profile current state."""

    validated_metadata = _metadata(
        outcome_resource_counts_v1={
            **_identity_proof(),
            "dataset_id": "pdds-validated",
        },
        **_relation_metadata(),
    )
    for relation_proof in (
        validated_metadata["dataset_network_plan"],
        validated_metadata["dataset_affiliation_organization"],
    ):
        relation_proof["dataset_id"] = "pdds-validated"
    dataset_rows = [
        _dataset_row(),
        _dataset_row(
            dataset_id="pdds-validated",
            status="validated",
            is_current=False,
            validated_at=dt.datetime(2026, 7, 21, 10, tzinfo=dt.UTC),
            published_at=None,
            publication_metadata=validated_metadata,
        ),
    ]
    monkeypatch.setattr(
        outcomes.db,
        "execute",
        AsyncMock(return_value=_MappingResult(dataset_rows)),
    )

    enriched_item = (
        await outcomes.enrich_provider_directory_source_catalog(_catalog())
    )["items"][0]

    assert enriched_item["outcome_summary"]["dataset_id"] == "pdds-validated"
    assert enriched_item["outcome_summary"]["status"] == "validated"
    assert enriched_item["current_outcome_summary"] == {
        "dataset_id": DATASET_ID,
        "status": "published",
        "is_current": True,
        "validated_at": "2026-07-20T08:00:00+00:00",
        "published_at": "2026-07-20T08:30:00+00:00",
        "total_resources": TOTAL_RESOURCES,
        "endpoint_id": ENDPOINT_ID,
        "acquisition_root_run_id": ROOT_RUN_ID,
        "dataset_hash": DATASET_HASH,
    }


@pytest.mark.asyncio
async def test_current_outcome_fails_closed_on_malformed_profile_identity(
    monkeypatch,
):
    """Never advertise malformed lineage as the authoritative incumbent."""

    malformed_rows = [
        _dataset_row(dataset_hash=7),
        _dataset_row(dataset_hash=" bad"),
        _dataset_row(dataset_hash="not-a-sha256"),
        _dataset_row(acquisition_root_run_id=None),
        _dataset_row(status="validated", is_current=False, published_at=None),
    ]
    monkeypatch.setattr(
        outcomes.db,
        "execute",
        AsyncMock(return_value=_MappingResult(malformed_rows)),
    )

    enriched_item = (
        await outcomes.enrich_provider_directory_source_catalog(_catalog())
    )["items"][0]

    assert "outcome_summary" in enriched_item
    assert "current_outcome_summary" not in enriched_item
