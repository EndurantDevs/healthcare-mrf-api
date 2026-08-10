# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Non-PostgreSQL storage tests for the official UHC NPI cohort."""

from contextlib import asynccontextmanager

import pytest

import process.uhc_flex_official_cohort_store as store
from process.uhc_canonical_proof import UHC_CANONICAL_CONTENT_PROOF_METADATA_KEY
from process.uhc_flex_official_cohort_contract import (
    UHCFlexOfficialCohortError,
    UHC_FLEX_OFFICIAL_COHORT_CONTRACT_ID,
    UHC_FLEX_OFFICIAL_NPI_SYSTEM,
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
        "publication_metadata_json": {
            UHC_CANONICAL_CONTENT_PROOF_METADATA_KEY: {"sealed": True}
        },
    }


def _proof():
    return {
        "source_id": UHC_PROVIDER_FILE_SOURCE_ID,
        "dataset_hash": DATASET_HASH,
        "resource_count": TOTAL_RESOURCE_COUNT,
        "resource_counts": {"Practitioner": PRACTITIONER_COUNT},
        "proof_sha256": PROOF_HASH,
    }


class _FakeDatabase:
    def __init__(self, *, locked=True):
        self.locked = locked
        self.header = None
        self.events = []
        self.all_statements = []
        self.status_calls = []

    @asynccontextmanager
    async def transaction(self):
        self.events.append("transaction")
        yield self

    async def scalar(self, statement, **params):
        assert "pg_try_advisory_xact_lock" in statement
        assert UHC_PROVIDER_FILE_SOURCE_ID in params["lock_identity"]
        self.events.append("lock")
        return self.locked

    async def all(self, statement, **params):
        self.all_statements.append(statement)
        if '"provider_directory_source"' in statement:
            self.events.append("source")
            return [_source_row()]
        if '"provider_directory_uhc_flex_npi_cohort"' in statement:
            self.events.append("read_header")
            return [] if self.header is None else [dict(self.header)]
        if '"provider_directory_dataset_resource"' in statement:
            raise AssertionError("raw Practitioner rows must not be materialized")
        raise AssertionError(statement)

    async def first(self, statement, **params):
        assert '"provider_directory_dataset_resource"' in statement
        assert "count(DISTINCT npi)" in statement
        assert "jsonb_array_elements" in statement
        assert UHC_FLEX_OFFICIAL_NPI_SYSTEM in statement
        assert "jsonb_typeof(identifier -> 'value') = 'string'" in statement
        assert "identifier ->> 'value'" in statement
        self.events.append("count_members")
        return {
            "practitioner_resource_count": PRACTITIONER_COUNT,
            "invalid_npi_count": 0,
            "npi_count": NPI_COUNT,
        }

    async def status(self, statement, **params):
        self.status_calls.append((statement, dict(params)))
        if 'INSERT INTO "mrf"."provider_directory_uhc_flex_npi_member"' in statement:
            assert self.header is None
            assert "SELECT DISTINCT" in statement
            assert "jsonb_array_elements" in statement
            assert UHC_FLEX_OFFICIAL_NPI_SYSTEM in statement
            assert "jsonb_typeof(identifier -> 'value') = 'string'" in statement
            assert "identifier ->> 'value'" in statement
            assert "unnest" not in statement.lower()
            assert "npis" not in params
            self.events.append("write_members")
            return NPI_COUNT
        if 'INSERT INTO "mrf"."provider_directory_uhc_flex_npi_cohort"' in statement:
            self.header = dict(params)
            self.events.append("write_header")
            return 1
        raise AssertionError(statement)


def _validated_proof(raw_proof, **bindings):
    assert raw_proof == {"sealed": True}
    assert bindings == {
        "dataset_id": DATASET_ID,
        "endpoint_id": ENDPOINT_ID,
        "acquisition_root_run_id": ROOT_RUN_ID,
    }
    return _proof()


@pytest.mark.asyncio
async def test_sync_writes_members_before_header_then_replays_exactly(monkeypatch):
    database = _FakeDatabase()
    monkeypatch.setattr(
        store,
        "validate_uhc_canonical_content_proof",
        _validated_proof,
    )

    first_result = await store.sync_uhc_flex_official_cohort(database=database)
    second_result = await store.sync_uhc_flex_official_cohort(database=database)

    assert first_result.created is True
    assert second_result.created is False
    assert first_result.cohort == second_result.cohort
    assert database.header["contract_id"] == UHC_FLEX_OFFICIAL_COHORT_CONTRACT_ID
    assert database.events.index("write_members") < database.events.index("write_header")
    assert not any(
        '"provider_directory_dataset_resource"' in statement
        for statement in database.all_statements
    )
    assert all("npis" not in params for _statement, params in database.status_calls)


@pytest.mark.asyncio
async def test_sync_fails_before_source_read_when_advisory_lock_is_busy():
    database = _FakeDatabase(locked=False)

    with pytest.raises(UHCFlexOfficialCohortError) as error:
        await store.sync_uhc_flex_official_cohort(database=database)

    assert error.value.code == "busy"
    assert database.events == ["transaction", "lock"]


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
