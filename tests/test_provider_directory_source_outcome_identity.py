# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import datetime as dt

import pytest

from api import provider_directory_source_outcomes as outcomes


SOURCE_IDS = ("source-a", "source-b")
ENDPOINT_ID = "endpoint-current"
DATASET_ID = "dataset-current"
ROOT_RUN_ID = "root-current"
DATASET_HASH = "a" * 64


def _dataset_row(*, current_source_ids=SOURCE_IDS, publication_metadata=None):
    return {
        "endpoint_id": ENDPOINT_ID,
        "dataset_id": DATASET_ID,
        "acquisition_root_run_id": ROOT_RUN_ID,
        "dataset_hash": DATASET_HASH,
        "status": "published",
        "is_current": True,
        "published_at": dt.datetime(2026, 7, 20, 8, tzinfo=dt.UTC),
        "resource_count": 2,
        "publication_metadata": publication_metadata
        or {
            "source_ids": list(SOURCE_IDS),
            "selected_resources": ["Organization", "Practitioner"],
        },
        "current_source_ids": current_source_ids,
    }


def _current_dataset(**row_overrides):
    return outcomes._current_dataset_from_row(
        _dataset_row(**row_overrides),
        {SOURCE_IDS},
    )


def _identity_proof_by_field(**overrides):
    proof_by_field = {
        "complete": True,
        "version": 1,
        "dataset_id": DATASET_ID,
        "endpoint_id": ENDPOINT_ID,
        "acquisition_root_run_id": ROOT_RUN_ID,
        "dataset_hash": DATASET_HASH,
        "source_ids": list(SOURCE_IDS),
        "selected_resources": ["Organization", "Practitioner"],
        "resource_count": 2,
        "resource_counts": {"Organization": 1, "Practitioner": 1},
    }
    proof_by_field.update(overrides)
    return proof_by_field


def test_current_dataset_query_uses_indexable_source_endpoint_association():
    selected_sql = str(outcomes._current_published_dataset_statement())

    assert "provider_directory_source.source_id" in selected_sql
    assert (
        "provider_directory_source.endpoint_id = "
        "mrf.provider_directory_endpoint_dataset.endpoint_id"
    ) in selected_sql
    assert "provider_directory_dataset_resource" not in selected_sql
    assert "GROUP BY" not in selected_sql


def test_current_dataset_accepts_every_source_bound_to_exact_endpoint():
    dataset = _current_dataset(
        current_source_ids=("unrelated-alias", *SOURCE_IDS)
    )

    assert dataset is not None
    assert dataset.endpoint_id == ENDPOINT_ID
    assert dataset.source_ids == SOURCE_IDS


def test_current_dataset_rejects_source_reassigned_to_another_endpoint():
    dataset = _current_dataset(
        current_source_ids=("source-a", "replacement-alias")
    )

    assert dataset is None


def test_current_dataset_rejects_stale_endpoint_without_current_sources():
    assert _current_dataset(current_source_ids=None) is None


@pytest.mark.parametrize(
    "publication_metadata",
    [
        {
            "source_ids": list(SOURCE_IDS),
            "selected_resources": ["Organization", "Practitioner"],
            "outcome_resource_counts_v1": _identity_proof_by_field(
                resource_counts=[1, 1]
            ),
        },
        {
            "source_ids": list(SOURCE_IDS),
            "selected_resources": ["Organization", "Practitioner"],
            "outcome_resource_counts_v1": _identity_proof_by_field(
                dataset_id="stale-dataset"
            ),
        },
        {"source_ids": list(SOURCE_IDS)},
    ],
)
def test_malformed_or_stale_count_proofs_remain_totals_only(
    publication_metadata,
):
    dataset = _current_dataset(publication_metadata=publication_metadata)

    assert dataset is not None
    assert outcomes._outcome_summary(dataset) == {
        "dataset_id": DATASET_ID,
        "status": "published",
        "is_current": True,
        "published_at": "2026-07-20T08:00:00+00:00",
        "total_resources": 2,
    }
