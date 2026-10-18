# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Non-PostgreSQL storage tests for the official UHC NPI cohort."""

from contextlib import asynccontextmanager

import pytest

import process.uhc_flex_official_cohort_store as store
from process.uhc_canonical_proof import UHC_CANONICAL_CONTENT_PROOF_METADATA_KEY
from process.uhc_flex_official_cohort_contract import (
    UHCFlexOfficialCohortError,
)
from process.uhc_provider_file_source_identity import UHC_PROVIDER_FILE_SOURCE_ID

ENDPOINT_ID = "e" * 64
DATASET_ID = "official-dataset"
ROOT_RUN_ID = "r" * 64
DATASET_HASH = "d" * 64
PROOF_HASH = "c" * 64
PRACTITIONER_COUNT = 4_968_035
NPI_COUNT = 117_000
TOTAL_RESOURCE_COUNT = 5_100_000


def _source_row():
    return {
        "source_id": UHC_PROVIDER_FILE_SOURCE_ID,
        "endpoint_id": ENDPOINT_ID,
        "dataset_endpoint_id": ENDPOINT_ID,
        "dataset_id": DATASET_ID,
        "acquisition_root_run_id": ROOT_RUN_ID,
        "dataset_hash": DATASET_HASH,
        "status": "published",
        "is_current": True,
        "resource_count": TOTAL_RESOURCE_COUNT,
        "publication_metadata_json": {UHC_CANONICAL_CONTENT_PROOF_METADATA_KEY: {"sealed": True}},
    }


def _proof():
    return {
        "source_id": UHC_PROVIDER_FILE_SOURCE_ID,
        "dataset_hash": DATASET_HASH,
        "resource_count": TOTAL_RESOURCE_COUNT,
        "resource_counts": {"Practitioner": PRACTITIONER_COUNT},
        "proof_sha256": PROOF_HASH,
    }


def _validated_proof(raw_proof, **bindings):
    assert raw_proof == {"sealed": True}
    assert bindings == {
        "dataset_id": DATASET_ID,
        "endpoint_id": ENDPOINT_ID,
        "acquisition_root_run_id": ROOT_RUN_ID,
    }
    return _proof()


@pytest.mark.asyncio
async def test_sync_requires_source_factory_before_any_legacy_database_io():
    class NoIO:
        @asynccontextmanager
        async def transaction(self):
            raise AssertionError("ordinary transaction must not authorize fresh members")
            yield

    with pytest.raises(ValueError, match="publisher_connection_required"):
        await store.sync_uhc_flex_official_cohort(database=NoIO())


@pytest.mark.asyncio
async def test_sync_rejects_unissued_expected_cohort_before_database_io():
    from process.uhc_flex_official_cohort_contract import build_uhc_flex_official_cohort

    cohort = build_uhc_flex_official_cohort(
        official_endpoint_id=ENDPOINT_ID,
        official_dataset_id=DATASET_ID,
        official_acquisition_root_run_id=ROOT_RUN_ID,
        official_dataset_hash=DATASET_HASH,
        official_content_proof_sha256=PROOF_HASH,
        practitioner_resource_count=PRACTITIONER_COUNT,
        npi_count=NPI_COUNT,
    )
    with pytest.raises(UHCFlexOfficialCohortError):
        await store.sync_uhc_flex_official_cohort(database=object(), expected_cohort=cohort)


def test_snapshot_binds_current_official_dataset_and_content_proof(monkeypatch):
    monkeypatch.setattr(
        store,
        "validate_uhc_canonical_content_proof",
        _validated_proof,
    )

    snapshot = store._validated_snapshot(_source_row())

    assert snapshot.dataset_id == DATASET_ID
    assert snapshot.content_proof_sha256 == PROOF_HASH
    assert snapshot.practitioner_resource_count == PRACTITIONER_COUNT


def test_snapshot_rejects_proof_dataset_drift(monkeypatch):
    invalid_proof_by_field = {**_proof(), "dataset_hash": "0" * 64}
    monkeypatch.setattr(
        store,
        "validate_uhc_canonical_content_proof",
        lambda *args, **kwargs: invalid_proof_by_field,
    )

    with pytest.raises(UHCFlexOfficialCohortError) as error:
        store._validated_snapshot(_source_row())
    assert error.value.code == "evidence"
