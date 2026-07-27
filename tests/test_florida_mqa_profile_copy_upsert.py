# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import date, datetime
import importlib
import json
import os
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock
import uuid

from dotenv import load_dotenv
import pytest
from sqlalchemy import ARRAY, JSON, Column, Date, MetaData, String, TIMESTAMP, Table

from db.models.provider_profile import (
    ProviderProfileImportRun,
    ProviderProfileSourceRecord,
)

florida = importlib.import_module("process.florida_mqa_profile")


class _RecordingDriver:
    def __init__(self, error: Exception | None = None):
        self.error = error
        self.calls: list[dict[str, object]] = []

    async def copy_records_to_table(self, table_name, *, columns, records):
        materialized_records = list(records)
        self.calls.append(
            {
                "table_name": table_name,
                "columns": list(columns),
                "records": materialized_records,
            }
        )
        if self.error is not None:
            raise self.error


class _RecordingConnection:
    def __init__(self, driver):
        self.raw_connection = SimpleNamespace(driver_connection=driver)
        self.statements: list[str] = []

    async def status(self, statement):
        self.statements.append(statement)
        return 1


class _AcquireDb:
    def __init__(self, connection):
        self.connection = connection
        self.acquire_count = 0

    @asynccontextmanager
    async def acquire(self):
        self.acquire_count += 1
        yield self.connection


class _ClaimStatement:
    def __init__(self, claimed_run_id):
        self.claimed_run_id = claimed_run_id
        self.values_payload = None
        self.conflict_elements = None

    def values(self, values):
        self.values_payload = values
        return self

    def on_conflict_do_nothing(self, *, index_elements):
        self.conflict_elements = index_elements
        return self

    def returning(self, _column):
        return self

    async def scalar(self):
        return self.claimed_run_id


class _ClaimDb:
    def __init__(self, claimed_run_id, existing_status=None):
        self.statement = _ClaimStatement(claimed_run_id)
        self.existing_status = existing_status

    def insert(self, _table):
        return self.statement

    async def scalar(self, _statement):
        return self.existing_status


def _source_record(record_id: str = "r" * 64) -> dict[str, object]:
    return {
        "record_id": record_id,
        "run_id": "run-1",
        "artifact_id": "artifact-1",
        "source_key": "profile_master",
        "source_record_key": "master:1",
        "profession_code": "1501",
        "license_id": "42",
        "license_number": "ME\x0012345",
        "raw_payload": {
            "name": "Alex\x00 Example",
            "observed_on": date(2026, 7, 27),
        },
        "normalized_payload": {"name": "Alex Example"},
        "matched_npi": 1000000004,
        "match_status": "deterministic",
        "match_evidence": {"method": "exact_license"},
        "row_number": 1,
    }


@pytest.mark.asyncio
async def test_run_scope_claim_is_atomic_and_single_owner(monkeypatch):
    run_id = "a" * 32
    claim_db = _ClaimDb(run_id)
    monkeypatch.setattr(florida, "db", claim_db)

    await florida._claim_import_run({"run_id": run_id, "status": "running"})

    assert claim_db.statement.conflict_elements == ["run_id"]
    assert claim_db.statement.values_payload["status"] == "running"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("existing_status", "expected_error"),
    [
        ("completed", "provider_profile_run_already_completed"),
        ("failed", "provider_profile_run_scope_exists"),
        ("running", "provider_profile_run_scope_exists"),
    ],
)
async def test_run_scope_replay_never_overwrites_existing_rows(
    monkeypatch,
    existing_status,
    expected_error,
):
    run_id = "b" * 32
    monkeypatch.setattr(
        florida,
        "db",
        _ClaimDb(None, existing_status=existing_status),
    )

    with pytest.raises(RuntimeError, match=expected_error):
        await florida._claim_import_run(
            {"run_id": run_id, "status": "running"}
        )


def test_unmatched_rows_retain_rematch_evidence_without_orphan_facts():
    raw_payload = {"LIC_NBR": "ME12345", "FIRST_NAME": "Alex"}
    normalized_payload = {"lic_nbr": "ME12345", "first_name": "Alex"}
    match_evidence = {
        "method": "exact_license",
        "candidate_count": 2,
    }

    retained = florida._retained_source_record(
        record_id="r" * 64,
        run_id="run-1",
        artifact_id="artifact-1",
        source_key="profile_master",
        source_record_key="master:1",
        profession_code="1501",
        license_id="42",
        license_number="ME12345",
        raw_payload=raw_payload,
        normalized_payload=normalized_payload,
        matched_npi=None,
        match_status="ambiguous",
        match_evidence=match_evidence,
        row_number=1,
    )

    assert retained["raw_payload"] == raw_payload
    assert retained["normalized_payload"] == normalized_payload
    assert retained["match_status"] == "ambiguous"
    assert retained["match_evidence"] == match_evidence
    assert florida._projectable_fact_npi(None, "ambiguous") is None
    assert florida._projectable_fact_npi(
        1000000004,
        "identity_conflict",
    ) is None
    assert florida._projectable_fact_npi(
        1000000004,
        "deterministic",
    ) == 1000000004


def test_profile_supplement_without_master_identity_is_retained_without_fact():
    supplement_match = florida._profile_supplement_match(
        None,
        profession_code="1501",
        license_id="missing-master-42",
        only_matched=False,
    )

    assert supplement_match == (
        None,
        "unmatched_master_identity",
        {
            "method": "profile_master_profession_license_id",
            "profession_code": "1501",
            "license_id": "missing-master-42",
            "master_identity_found": False,
        },
    )
    npi, match_status, _evidence = supplement_match
    assert florida._projectable_fact_npi(npi, match_status) is None
    assert (
        florida._profile_supplement_match(
            None,
            profession_code="1501",
            license_id="missing-master-42",
            only_matched=True,
        )
        is None
    )


def test_copy_value_conversion_handles_json_array_dates_and_nuls():
    json_column = Column("payload", JSON)
    array_column = Column("tags", ARRAY(String))
    date_column = Column("occurred_on", Date)
    timestamp_column = Column("observed_at", TIMESTAMP(timezone=False))

    encoded_json = florida._copy_value_for_type(
        json_column.type,
        {
            "text": "A\x00B",
            "occurred_on": date(2026, 7, 27),
        },
    )

    assert json.loads(encoded_json) == {
        "occurred_on": "2026-07-27",
        "text": "AB",
    }
    assert florida._copy_value_for_type(
        array_column.type,
        '["A\\u0000B", "C"]',
    ) == ["AB", "C"]
    assert florida._copy_value_for_type(
        date_column.type,
        "2026-07-27",
    ) == date(2026, 7, 27)
    assert florida._copy_value_for_type(
        timestamp_column.type,
        "2026-07-27T12:30:00Z",
    ) == datetime(2026, 7, 27, 12, 30)


@pytest.mark.asyncio
async def test_copy_upsert_uses_one_bounded_transaction_and_exact_conflict_key():
    driver = _RecordingDriver()
    connection = _RecordingConnection(driver)

    await florida._copy_upsert_chunk_on_connection(
        connection,
        ProviderProfileSourceRecord,
        [_source_record()],
        "record_id",
    )

    assert len(driver.calls) == 1
    copy_call = driver.calls[0]
    assert str(copy_call["table_name"]).startswith(
        "fl_pp_provider_profile_source_record_"
    )
    assert copy_call["columns"] == [
        column.name for column in ProviderProfileSourceRecord.__table__.columns
    ]
    copied = copy_call["records"][0]
    copied_by_column = dict(zip(copy_call["columns"], copied, strict=True))
    assert copied_by_column["license_number"] == "ME12345"
    assert json.loads(copied_by_column["raw_payload"]) == {
        "name": "Alex Example",
        "observed_on": "2026-07-27",
    }

    assert len(connection.statements) == 2
    create_sql, upsert_sql = connection.statements
    assert 'LIKE "mrf"."provider_profile_source_record"' in create_sql
    assert 'ON CONFLICT ("record_id") DO UPDATE SET' in upsert_sql
    assert '"raw_payload" = EXCLUDED."raw_payload"' in upsert_sql
    assert '"record_id" = EXCLUDED."record_id"' not in upsert_sql


@pytest.mark.asyncio
async def test_copy_unavailable_falls_back_to_original_sqlalchemy_batches(
    monkeypatch,
):
    connection = _RecordingConnection(driver=object())
    fallback = AsyncMock()
    monkeypatch.setattr(florida, "db", _AcquireDb(connection))
    monkeypatch.setattr(florida, "_upsert_rows_values", fallback)
    monkeypatch.setenv("HLTHPRT_FL_MQA_COPY_UPSERT", "1")
    monkeypatch.setenv("HLTHPRT_FL_MQA_COPY_UPSERT_MIN_ROWS", "1")

    rows = [_source_record("a" * 64), _source_record("b" * 64)]
    await florida._upsert_rows(
        ProviderProfileSourceRecord,
        rows,
        "record_id",
    )

    fallback.assert_awaited_once_with(
        ProviderProfileSourceRecord,
        rows,
        "record_id",
    )


@pytest.mark.asyncio
async def test_copy_data_error_is_not_hidden_by_sqlalchemy_fallback(monkeypatch):
    copy_error = RuntimeError("synthetic COPY failure")
    connection = _RecordingConnection(_RecordingDriver(copy_error))
    fallback = AsyncMock()
    monkeypatch.setattr(florida, "db", _AcquireDb(connection))
    monkeypatch.setattr(florida, "_upsert_rows_values", fallback)
    monkeypatch.setenv("HLTHPRT_FL_MQA_COPY_UPSERT", "1")
    monkeypatch.setenv("HLTHPRT_FL_MQA_COPY_UPSERT_MIN_ROWS", "1")

    with pytest.raises(RuntimeError, match="synthetic COPY failure"):
        await florida._upsert_rows(
            ProviderProfileSourceRecord,
            [_source_record()],
            "record_id",
        )

    fallback.assert_not_awaited()


@pytest.mark.asyncio
async def test_copy_batches_are_bounded_by_configured_row_limit(monkeypatch):
    copy_upsert = AsyncMock()
    fallback = AsyncMock()
    monkeypatch.setattr(florida, "_copy_upsert_chunk", copy_upsert)
    monkeypatch.setattr(florida, "_upsert_rows_values", fallback)
    monkeypatch.setenv("HLTHPRT_FL_MQA_COPY_UPSERT", "1")
    monkeypatch.setenv("HLTHPRT_FL_MQA_COPY_UPSERT_MIN_ROWS", "1")
    monkeypatch.setenv("HLTHPRT_FL_MQA_COPY_UPSERT_BATCH_ROWS", "2")
    rows = [_source_record(f"{index:064x}") for index in range(5)]

    await florida._upsert_rows(
        ProviderProfileSourceRecord,
        rows,
        "record_id",
    )

    assert [
        len(call.args[1]) for call in copy_upsert.await_args_list
    ] == [2, 2, 1]
    fallback.assert_not_awaited()


@pytest.mark.asyncio
async def test_copy_is_scoped_away_from_run_and_publication_metadata(monkeypatch):
    copy_upsert = AsyncMock()
    fallback = AsyncMock()
    monkeypatch.setattr(florida, "_copy_upsert_chunk", copy_upsert)
    monkeypatch.setattr(florida, "_upsert_rows_values", fallback)
    monkeypatch.setenv("HLTHPRT_FL_MQA_COPY_UPSERT", "1")
    monkeypatch.setenv("HLTHPRT_FL_MQA_COPY_UPSERT_MIN_ROWS", "1")
    rows = [{"run_id": "run-1", "status": "running"}]

    await florida._upsert_rows(ProviderProfileImportRun, rows, "run_id")

    copy_upsert.assert_not_awaited()
    fallback.assert_awaited_once_with(
        ProviderProfileImportRun,
        rows,
        "run_id",
    )


def test_copy_rejects_untrusted_conflict_identifier_before_database_access():
    with pytest.raises(ValueError, match="unsafe PostgreSQL identifier"):
        florida._validated_identifier('record_id"; DROP TABLE provider; --')


class CannotConnectNowError(RuntimeError):
    pass


class _FailureStatusStatement:
    def __init__(self, outcomes):
        self.outcomes = iter(outcomes)
        self.values_payload = None
        self.where_criteria = ()
        self.calls = 0

    def where(self, *criteria):
        self.where_criteria = criteria
        return self

    def values(self, **values):
        self.values_payload = values
        return self

    async def status(self):
        self.calls += 1
        outcome = next(self.outcomes)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome


class _FailureStatusDb:
    def __init__(self, statement):
        self.statement = statement
        self.engine = SimpleNamespace(dispose=AsyncMock())

    def update(self, _table):
        return self.statement


@pytest.mark.asyncio
async def test_failure_status_retries_transient_recovery_without_publication_retry(
    monkeypatch,
):
    statement = _FailureStatusStatement(
        [CannotConnectNowError("database is recovering"), 1]
    )
    sleep = AsyncMock()
    monkeypatch.setattr(florida, "db", _FailureStatusDb(statement))
    monkeypatch.setattr(florida.asyncio, "sleep", sleep)
    monkeypatch.setenv("HLTHPRT_FL_MQA_FAILURE_STATUS_ATTEMPTS", "3")
    monkeypatch.setenv(
        "HLTHPRT_FL_MQA_FAILURE_STATUS_TIMEOUT_SECONDS",
        "1",
    )
    run_row = {"metrics": {"facts": 12}}

    status_error = await florida._mark_failed_run_status(
        run_id="run-1",
        run_row=run_row,
        original_error=RuntimeError("original import failure"),
        cleanup_error=None,
    )

    assert status_error is None
    assert statement.calls == 2
    assert len(statement.where_criteria) == 2
    assert statement.values_payload["status"] == "failed"
    assert statement.values_payload["error"] == {
        "type": "RuntimeError",
        "message": "original import failure",
    }
    assert run_row["status"] == "failed"
    sleep.assert_awaited_once()
    florida.db.engine.dispose.assert_awaited_once()


@pytest.mark.asyncio
async def test_failure_status_returns_secondary_error_without_masking_original(
    monkeypatch,
):
    statement = _FailureStatusStatement([ValueError("bad failure payload")])
    monkeypatch.setattr(florida, "db", _FailureStatusDb(statement))
    monkeypatch.setenv("HLTHPRT_FL_MQA_FAILURE_STATUS_ATTEMPTS", "3")

    status_error = await florida._mark_failed_run_status(
        run_id="run-1",
        run_row={"metrics": {}},
        original_error=RuntimeError("original import failure"),
        cleanup_error="cleanup also failed",
    )

    assert status_error == "ValueError: bad failure payload"
    assert statement.calls == 1


@pytest.mark.asyncio
async def test_failure_status_condition_cannot_overwrite_completed_run(monkeypatch):
    statement = _FailureStatusStatement([0])
    monkeypatch.setattr(florida, "db", _FailureStatusDb(statement))
    run_row = {"status": "completed", "metrics": {"published_providers": 12}}

    status_error = await florida._mark_failed_run_status(
        run_id="run-1",
        run_row=run_row,
        original_error=RuntimeError("post-publication cleanup failure"),
        cleanup_error=None,
    )

    assert status_error is None
    assert run_row == {
        "status": "completed",
        "metrics": {"published_providers": 12},
    }
    assert len(statement.where_criteria) == 2
    assert "status" in str(statement.where_criteria[1])
    assert statement.values_payload["status"] == "failed"


@pytest.mark.asyncio
async def test_retention_maintenance_is_best_effort_outside_success(monkeypatch):
    cleanup = AsyncMock(
        side_effect=RuntimeError("synthetic maintenance failure")
    )
    monkeypatch.setattr(florida, "_post_success_retention", cleanup)

    result = await florida._apply_retention_maintenance(
        run_id="c" * 32,
        artifact_root=Path("/synthetic/provider-profile"),
        failed_retention_days=7,
    )

    assert result["status"] == "failed"
    assert result["failed_retention_days"] == 7
    cleanup.assert_awaited_once()


@pytest.mark.asyncio
async def test_copy_upsert_postgres_temp_table_conflict_contract():
    if os.getenv("HLTHPRT_TEST_FL_MQA_COPY_POSTGRES") != "1":
        pytest.skip("set HLTHPRT_TEST_FL_MQA_COPY_POSTGRES=1 for PostgreSQL proof")

    root = Path(__file__).resolve().parents[1]
    load_dotenv(root / ".env", override=False)
    table_name = f"fl_pp_copy_e2e_{uuid.uuid4().hex[:12]}"
    metadata = MetaData()
    target = Table(
        table_name,
        metadata,
        Column("row_id", String(64), primary_key=True),
        Column("payload", JSON, nullable=False),
        Column("tags", ARRAY(String)),
        Column("occurred_on", Date),
        schema="pg_temp",
    )
    model = type("SyntheticCopyTarget", (), {"__table__": target})

    await florida.db.connect()
    try:
        async with florida.db.acquire() as connection:
            driver = getattr(
                connection.raw_connection,
                "driver_connection",
                connection.raw_connection,
            )
            if not callable(getattr(driver, "copy_records_to_table", None)):
                pytest.skip("active database driver does not expose binary COPY")
            await connection.status(
                f"""
                CREATE TEMP TABLE "{table_name}" (
                    row_id varchar(64) PRIMARY KEY,
                    payload jsonb NOT NULL,
                    tags varchar[],
                    occurred_on date
                ) ON COMMIT DROP;
                """
            )
            await florida._copy_upsert_chunk_on_connection(
                connection,
                model,
                [
                    {
                        "row_id": "same",
                        "payload": {"version": 1},
                        "tags": ["first"],
                        "occurred_on": "2026-07-26",
                    }
                ],
                "row_id",
            )
            await florida._copy_upsert_chunk_on_connection(
                connection,
                model,
                [
                    {
                        "row_id": "same",
                        "payload": {"version": 2},
                        "tags": ["updated"],
                        "occurred_on": "2026-07-27",
                    },
                    {
                        "row_id": "new",
                        "payload": {"version": 1},
                        "tags": ["new"],
                        "occurred_on": "2026-07-27",
                    },
                ],
                "row_id",
            )
            rows = await connection.all(
                f"""
                SELECT row_id, payload, tags, occurred_on
                  FROM "{table_name}"
                 ORDER BY row_id;
                """
            )
    finally:
        await florida.db.disconnect()

    assert [row._mapping["row_id"] for row in rows] == ["new", "same"]
    same = rows[1]._mapping
    assert same["payload"] == {"version": 2}
    assert same["tags"] == ["updated"]
    assert same["occurred_on"] == date(2026, 7, 27)
