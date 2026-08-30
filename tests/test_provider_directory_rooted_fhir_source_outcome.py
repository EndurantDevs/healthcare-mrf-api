"""Strict rooted FHIR publication supplement tests."""

from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import provider_directory_source_catalog_outcomes as catalog_outcomes
from api import provider_directory_source_outcomes as outcomes
from api.provider_directory_rooted_fhir_publication import (
    ROOTED_FHIR_CATALOG_ENTRY_ID,
    ROOTED_FHIR_CATALOG_SOURCE_IDS,
    ROOTED_FHIR_PUBLICATION_FIELD,
    ROOTED_FHIR_PUBLICATION_SUMMARY_CONTRACT_ID,
    ROOTED_FHIR_SOURCE_ID_GROUP,
    rooted_fhir_publication_summary,
)
from api.provider_directory_sources import provider_directory_source_catalog
from process.provider_directory_rooted_graph_publication_contract import (
    PROVIDER_DIRECTORY_ROOTED_GRAPH_PUBLICATION_CONTRACT_ID,
)
from tests.provider_directory_rooted_graph_publication_test_support import (
    readiness,
)
from tests.test_provider_directory_source_outcomes import _MappingResult


PUBLISHED_AT = datetime(2026, 8, 10, 12, tzinfo=UTC)


def _current_dataset(proof=None, **overrides):
    rooted_proof = proof or readiness()
    field_map = {
        "source_ids": (rooted_proof.source_id,),
        "dataset_id": rooted_proof.dataset_id,
        "endpoint_id": rooted_proof.endpoint_id,
        "acquisition_root_run_id": rooted_proof.acquisition_root_run_id,
        "dataset_hash": rooted_proof.dataset_hash,
        "resource_count": rooted_proof.resource_count,
        "status": "published",
        "is_current": True,
        "published_at": PUBLISHED_AT,
    }
    field_map.update(overrides)
    return SimpleNamespace(**field_map)


def _rooted_dataset_row(proof=None):
    rooted_proof = proof or readiness()
    return {
        "endpoint_id": rooted_proof.endpoint_id,
        "dataset_id": rooted_proof.dataset_id,
        "acquisition_root_run_id": rooted_proof.acquisition_root_run_id,
        "dataset_hash": rooted_proof.dataset_hash,
        "status": "published",
        "is_current": True,
        "validated_at": PUBLISHED_AT,
        "published_at": PUBLISHED_AT,
        "superseded_at": None,
        "resource_count": rooted_proof.resource_count,
        "publication_metadata": {
            "source_ids": [rooted_proof.source_id],
            "selected_resources": list(rooted_proof.resource_counts),
        },
        "current_source_ids": [rooted_proof.source_id],
    }


def _direct_catalog():
    return {
        "schema_version": 1,
        "catalog_digest": "c" * 64,
        "items": [
            {
                "entry_id": ROOTED_FHIR_CATALOG_ENTRY_ID,
                "source_ids": list(ROOTED_FHIR_CATALOG_SOURCE_IDS),
                "classification": "probe_only",
                "runnable": False,
            }
        ],
    }


def test_static_catalog_distinguishes_unavailable_rooted_proof():
    catalog = provider_directory_source_catalog()
    entry = next(
        item
        for item in catalog["items"]
        if item["entry_id"] == ROOTED_FHIR_CATALOG_ENTRY_ID
    )

    assert tuple(entry["source_ids"]) == ROOTED_FHIR_CATALOG_SOURCE_IDS
    assert entry[ROOTED_FHIR_PUBLICATION_FIELD] == {
        "contract_id": ROOTED_FHIR_PUBLICATION_SUMMARY_CONTRACT_ID,
        "state": "unavailable",
    }


def test_rooted_summary_reports_pending_and_not_ready_states():
    assert rooted_fhir_publication_summary(None, None) == {
        "contract_id": ROOTED_FHIR_PUBLICATION_SUMMARY_CONTRACT_ID,
        "state": "not_published",
    }
    assert rooted_fhir_publication_summary(_current_dataset(), None) == {
        "contract_id": ROOTED_FHIR_PUBLICATION_SUMMARY_CONTRACT_ID,
        "state": "not_ready",
    }


@pytest.mark.parametrize(
    ("overrides"),
    [
        {"source_ids": ("pdfhir_synthetic_root",)},
        {"dataset_id": "pdrgpd_" + "0" * 48},
        {"endpoint_id": "endpoint-synthetic"},
        {"acquisition_root_run_id": "pdrgpr_" + "0" * 48},
        {"dataset_hash": "0" * 64},
        {"resource_count": 99},
        {"status": "validated"},
        {"is_current": False},
        {"published_at": None},
    ],
)
def test_rooted_summary_rejects_parent_identity_drift(overrides):
    proof = readiness()

    summary = rooted_fhir_publication_summary(
        _current_dataset(proof, **overrides),
        proof,
    )

    assert summary["state"] == "not_ready"


def test_rooted_summary_projects_the_closed_readiness_contract():
    proof = readiness()

    summary = rooted_fhir_publication_summary(_current_dataset(proof), proof)

    assert summary == {
        "contract_id": ROOTED_FHIR_PUBLICATION_SUMMARY_CONTRACT_ID,
        "state": "closed",
        "publication_contract_id": (
            PROVIDER_DIRECTORY_ROOTED_GRAPH_PUBLICATION_CONTRACT_ID
        ),
        "publication_kind": proof.publication_kind,
        "source_id": proof.source_id,
        "endpoint_id": proof.endpoint_id,
        "source_authority_id": proof.source_authority_id,
        "dataset_id": proof.dataset_id,
        "dataset_hash": proof.dataset_hash,
        "acquisition_root_run_id": proof.acquisition_root_run_id,
        "admission_id": proof.admission_id,
        "publication_acquisition_id": proof.publication_acquisition_id,
        "root_dataset_variant": proof.root_dataset_variant,
        "root_publication_contract_id": proof.root_publication_contract_id,
        "root_dataset_id": proof.root_dataset_id,
        "root_dataset_hash": proof.root_dataset_hash,
        "root_content_proof_sha256": proof.root_content_proof_sha256,
        "root_cohort_id": proof.root_cohort_id,
        "semantic_projection_as_of": proof.semantic_projection_as_of,
        "published_at": "2026-08-10T12:00:00+00:00",
        "total_resources": proof.resource_count,
        "resource_counts": proof.resource_counts,
        "cohort_complete": True,
        "rooted_graph_complete": True,
        "endpoint_collection_complete": False,
        "endpoint_complete": False,
    }


def test_rooted_summary_marks_retry_exhaustion_partial_not_closed():
    proof = readiness(retry_exhausted_count=8)

    summary = rooted_fhir_publication_summary(_current_dataset(proof), proof)

    assert summary["state"] == "partial"
    assert summary["cohort_complete"] is False
    assert summary["rooted_graph_complete"] is True
    assert summary["retry_exhausted_count"] == 8
    assert len(summary) == 27


@pytest.mark.asyncio
async def test_catalog_enrichment_joins_rooted_readiness_without_scope_drift(
    monkeypatch,
):
    proof = readiness()
    database_execute = AsyncMock(
        return_value=_MappingResult([_rooted_dataset_row(proof)])
    )
    readiness_loader = AsyncMock(return_value=proof)
    monkeypatch.setattr(outcomes.db, "execute", database_execute)
    monkeypatch.setattr(
        catalog_outcomes,
        "load_provider_directory_rooted_graph_dataset_readiness",
        readiness_loader,
    )
    catalog = _direct_catalog()

    enriched = await catalog_outcomes.enrich_provider_directory_source_catalog(catalog)

    entry = enriched["items"][0]
    assert tuple(entry["source_ids"]) == ROOTED_FHIR_CATALOG_SOURCE_IDS
    assert entry[ROOTED_FHIR_PUBLICATION_FIELD]["state"] == "closed"
    assert entry[ROOTED_FHIR_PUBLICATION_FIELD]["dataset_id"] == proof.dataset_id
    readiness_loader.assert_awaited_once_with(proof.dataset_id)
    assert database_execute.await_count == 2
    assert enriched["catalog_digest"] == catalog["catalog_digest"]


@pytest.mark.asyncio
async def test_catalog_enrichment_surfaces_partial_rooted_readiness(monkeypatch):
    proof = readiness(retry_exhausted_count=8)
    database_execute = AsyncMock(
        return_value=_MappingResult([_rooted_dataset_row(proof)])
    )
    readiness_loader = AsyncMock(return_value=proof)
    monkeypatch.setattr(outcomes.db, "execute", database_execute)
    monkeypatch.setattr(
        catalog_outcomes,
        "load_provider_directory_rooted_graph_dataset_readiness",
        readiness_loader,
    )

    enriched = await catalog_outcomes.enrich_provider_directory_source_catalog(
        _direct_catalog()
    )

    summary = enriched["items"][0][ROOTED_FHIR_PUBLICATION_FIELD]
    assert summary["state"] == "partial"
    assert summary["cohort_complete"] is False
    assert summary["rooted_graph_complete"] is True
    assert summary["retry_exhausted_count"] == 8
    readiness_loader.assert_awaited_once_with(proof.dataset_id)


@pytest.mark.asyncio
async def test_catalog_enrichment_reports_missing_rooted_publication(monkeypatch):
    database_execute = AsyncMock(return_value=_MappingResult([]))
    readiness_loader = AsyncMock()
    monkeypatch.setattr(outcomes.db, "execute", database_execute)
    monkeypatch.setattr(
        catalog_outcomes,
        "load_provider_directory_rooted_graph_dataset_readiness",
        readiness_loader,
    )

    enriched = await catalog_outcomes.enrich_provider_directory_source_catalog(
        _direct_catalog()
    )

    assert enriched["items"][0][ROOTED_FHIR_PUBLICATION_FIELD] == {
        "contract_id": ROOTED_FHIR_PUBLICATION_SUMMARY_CONTRACT_ID,
        "state": "not_published",
    }
    readiness_loader.assert_not_awaited()
    assert ROOTED_FHIR_SOURCE_ID_GROUP
