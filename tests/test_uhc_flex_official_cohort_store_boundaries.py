# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Failure-boundary coverage for official Flex cohort persistence."""

from dataclasses import asdict
from types import SimpleNamespace

import pytest

from process import uhc_flex_official_cohort_store as store
from process.uhc_flex_official_cohort_contract import (
    UHCFlexOfficialCohortError,
    build_uhc_flex_official_cohort,
)


def _cohort(*, endpoint_id="e" * 64):
    return build_uhc_flex_official_cohort(
        official_endpoint_id=endpoint_id,
        official_dataset_id="dataset-boundary",
        official_acquisition_root_run_id="r" * 64,
        official_dataset_hash="d" * 64,
        official_content_proof_sha256="c" * 64,
        practitioner_resource_count=2,
        npi_count=1,
    )


def _snapshot():
    return store._OfficialPractitionerSnapshot(
        endpoint_id="e" * 64,
        dataset_id="dataset-boundary",
        acquisition_root_run_id="r" * 64,
        dataset_hash="d" * 64,
        content_proof_sha256="c" * 64,
        practitioner_resource_count=2,
    )


class _Database:
    def __init__(self, *, rows=(), first_row=None, status_result=1):
        self.rows = list(rows)
        self.first_row = first_row
        self.status_result = status_result

    async def all(self, _statement, **_params):
        return self.rows

    async def first(self, _statement, **_params):
        return self.first_row

    async def status(self, _statement, **_params):
        return self.status_result


@pytest.mark.parametrize(
    ("cohort", "created"),
    [(object(), True), (_cohort(), 1)],
)
def test_sync_result_requires_exact_types(cohort, created):
    with pytest.raises(ValueError, match="sync result is invalid"):
        store.UHCFlexOfficialCohortSyncResult(cohort, created)


def test_schema_row_and_json_helpers_reject_ambiguous_state(monkeypatch):
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "first")
    monkeypatch.setenv("DB_SCHEMA", "second")
    with pytest.raises(UHCFlexOfficialCohortError, match="state is invalid"):
        store._schema_name()

    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "unsafe-name")
    monkeypatch.delenv("DB_SCHEMA")
    with pytest.raises(UHCFlexOfficialCohortError, match="state is invalid"):
        store._schema_name()

    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "safe_schema")
    assert store._table("cohort") == '"safe_schema"."cohort"'
    assert store._row_fields(None) == {}
    assert store._row_fields(SimpleNamespace(_mapping={"value": 1})) == {"value": 1}
    assert store._json_object('{"value":1}') == {"value": 1}


@pytest.mark.parametrize("raw_document", ["{", [], None])
def test_json_helper_rejects_invalid_documents(raw_document):
    with pytest.raises(UHCFlexOfficialCohortError) as error_info:
        store._json_object(raw_document)
    assert error_info.value.code == "evidence"


@pytest.mark.parametrize("value", [None, "", " padded", "x" * 65, "bad\ntext"])
def test_store_text_helper_rejects_unsafe_values(value):
    with pytest.raises(UHCFlexOfficialCohortError):
        store._strict_text(value, 64)


@pytest.mark.parametrize("value", [True, 0, -1, 1 << 63])
def test_store_count_helper_rejects_invalid_values(value):
    with pytest.raises(UHCFlexOfficialCohortError):
        store._positive_count(value)


def test_snapshot_rejects_source_or_proof_validation_errors(monkeypatch):
    invalid_source_by_field = {
        "source_id": "wrong",
        "endpoint_id": "e" * 64,
        "dataset_endpoint_id": "e" * 64,
        "dataset_id": "dataset-boundary",
        "acquisition_root_run_id": "r" * 64,
        "dataset_hash": "d" * 64,
        "status": "published",
        "is_current": True,
        "publication_metadata_json": {},
    }
    with pytest.raises(UHCFlexOfficialCohortError):
        store._validated_snapshot(invalid_source_by_field)

    valid_source_by_field = {
        **invalid_source_by_field,
        "source_id": store.UHC_PROVIDER_FILE_SOURCE_ID,
    }
    monkeypatch.setattr(
        store,
        "validate_uhc_canonical_content_proof",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(ValueError("secret")),
    )
    with pytest.raises(UHCFlexOfficialCohortError) as error_info:
        store._validated_snapshot(valid_source_by_field)
    assert error_info.value.code == "evidence"
    assert "secret" not in str(error_info.value)


@pytest.mark.asyncio
async def test_current_snapshot_requires_exactly_one_row():
    with pytest.raises(UHCFlexOfficialCohortError) as missing_error:
        await store._current_official_snapshot(_Database())
    assert missing_error.value.code == "missing"

    with pytest.raises(UHCFlexOfficialCohortError) as duplicate_error:
        await store._current_official_snapshot(_Database(rows=[{}, {}]))
    assert duplicate_error.value.code == "state"


@pytest.mark.asyncio
async def test_official_npi_count_rejects_resource_or_npi_census_drift():
    database = _Database(
        first_row={
            "practitioner_resource_count": 1,
            "invalid_npi_count": 0,
            "npi_count": 1,
        }
    )
    with pytest.raises(UHCFlexOfficialCohortError) as error_info:
        await store._official_practitioner_npi_count(database, _snapshot())
    assert error_info.value.code == "evidence"


def test_stored_cohort_row_rejects_tampered_identity():
    cohort_row = asdict(_cohort())
    cohort_row["cohort_id"] = "pdufc_" + "0" * 48
    with pytest.raises(UHCFlexOfficialCohortError) as error_info:
        store._cohort_from_row(cohort_row)
    assert error_info.value.code == "state"


@pytest.mark.asyncio
async def test_stored_cohort_rejects_duplicate_headers():
    with pytest.raises(UHCFlexOfficialCohortError) as error_info:
        await store._stored_cohort_for_dataset(_Database(rows=[{}, {}]), "dataset")
    assert error_info.value.code == "state"


@pytest.mark.asyncio
async def test_header_insert_requires_exact_census():
    with pytest.raises(UHCFlexOfficialCohortError):
        await store._insert_header(_Database(status_result=0), _cohort())


@pytest.mark.asyncio
@pytest.mark.parametrize("expected", [object(), {}, True])
async def test_sync_rejects_constructible_noncohort_replay_before_io(expected):
    with pytest.raises(UHCFlexOfficialCohortError):
        await store.sync_uhc_flex_official_cohort(database=object(), expected_cohort=expected)
