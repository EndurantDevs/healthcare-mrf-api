# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import datetime as dt

import pytest

from api import provider_directory_source_outcomes as outcomes


SOURCE_IDS = ("source-a", "source-b")
ENDPOINT_ID = "endpoint-current"
DATASET_ID = "dataset-current"
ROOT_RUN_ID = "root-current"
DATASET_HASH = "a" * 64


def _dataset_row(
    *,
    current_source_ids=SOURCE_IDS,
    publication_metadata=None,
    **overrides,
):
    dataset_row_map = {
        "endpoint_id": ENDPOINT_ID,
        "dataset_id": DATASET_ID,
        "acquisition_root_run_id": ROOT_RUN_ID,
        "dataset_hash": DATASET_HASH,
        "status": "published",
        "is_current": True,
        "validated_at": dt.datetime(2026, 7, 20, 7, tzinfo=dt.UTC),
        "published_at": dt.datetime(2026, 7, 20, 8, tzinfo=dt.UTC),
        "superseded_at": None,
        "resource_count": 2,
        "publication_metadata": publication_metadata
        or {
            "source_ids": list(SOURCE_IDS),
            "selected_resources": ["Organization", "Practitioner"],
        },
        "current_source_ids": current_source_ids,
    }
    dataset_row_map.update(overrides)
    return dataset_row_map


def _sealed_dataset(**row_overrides):
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


def test_sealed_dataset_query_uses_indexable_source_endpoint_association():
    selected_sql = str(
        outcomes._current_published_dataset_statement({SOURCE_IDS})
    )

    assert "provider_directory_source.source_id" in selected_sql
    assert (
        "provider_directory_source.endpoint_id = "
        "mrf.provider_directory_endpoint_dataset.endpoint_id"
    ) in selected_sql
    assert "provider_directory_dataset_resource" not in selected_sql
    assert "GROUP BY" in selected_sql


def test_current_dataset_accepts_every_source_bound_to_exact_endpoint():
    dataset = _sealed_dataset(
        current_source_ids=("unrelated-alias", *SOURCE_IDS)
    )

    assert dataset is not None
    assert dataset.endpoint_id == ENDPOINT_ID
    assert dataset.source_ids == SOURCE_IDS


def test_current_dataset_rejects_source_reassigned_to_another_endpoint():
    dataset = _sealed_dataset(
        current_source_ids=("source-a", "replacement-alias")
    )

    assert dataset is None


def test_current_dataset_rejects_stale_endpoint_without_current_sources():
    assert _sealed_dataset(current_source_ids=None) is None


def test_validated_dataset_is_source_local_while_source_binding_remains():
    dataset = _sealed_dataset(
        status="validated",
        is_current=False,
        validated_at=dt.datetime(2026, 7, 21, 9, tzinfo=dt.UTC),
        published_at=None,
    )

    assert dataset is not None
    assert dataset.status == "validated"
    assert dataset.is_current is False
    assert dataset.published_at is None


def test_published_outcome_omits_unavailable_validation_timestamp():
    dataset = _sealed_dataset(validated_at=None)

    assert dataset is not None
    assert outcomes._outcome_summary(dataset) == {
        "dataset_id": DATASET_ID,
        "status": "published",
        "is_current": True,
        "published_at": "2026-07-20T08:00:00+00:00",
        "total_resources": 2,
    }


def test_noncurrent_dataset_rejects_reassigned_or_deconfigured_source():
    validated_state_map = {
        "status": "validated",
        "is_current": False,
        "validated_at": dt.datetime(2026, 7, 21, 9, tzinfo=dt.UTC),
        "published_at": None,
    }

    assert _sealed_dataset(
        current_source_ids=None,
        **validated_state_map,
    ) is None
    assert _sealed_dataset(
        current_source_ids=("source-a", "replacement-alias"),
        **validated_state_map,
    ) is None


@pytest.mark.parametrize(
    "row_overrides",
    [
        {"status": "validated", "is_current": True},
        {
            "status": "validated",
            "is_current": False,
            "validated_at": None,
            "published_at": None,
        },
        {
            "status": "validated",
            "is_current": False,
            "published_at": dt.datetime(2026, 7, 21, tzinfo=dt.UTC),
        },
        {
            "status": "validated",
            "is_current": False,
            "superseded_at": dt.datetime(2026, 7, 21, tzinfo=dt.UTC),
        },
        {"status": "published", "is_current": False},
        {"status": "published", "published_at": None},
        {
            "status": "published",
            "superseded_at": dt.datetime(2026, 7, 21, tzinfo=dt.UTC),
        },
        {"status": "superseded", "is_current": True},
        {"status": "superseded", "is_current": False},
        {
            "status": "superseded",
            "is_current": False,
            "published_at": None,
        },
        {"status": "failed", "is_current": False},
    ],
)
def test_inconsistent_or_unsuccessful_dataset_state_is_rejected(row_overrides):
    assert _sealed_dataset(**row_overrides) is None


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
    dataset = _sealed_dataset(publication_metadata=publication_metadata)

    assert dataset is not None
    assert outcomes._outcome_summary(dataset) == {
        "dataset_id": DATASET_ID,
        "status": "published",
        "is_current": True,
        "validated_at": "2026-07-20T07:00:00+00:00",
        "published_at": "2026-07-20T08:00:00+00:00",
        "total_resources": 2,
    }
