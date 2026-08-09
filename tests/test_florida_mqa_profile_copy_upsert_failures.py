# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Focused tests split from a shared contract fixture module."""

from __future__ import annotations

from tests.test_florida_mqa_profile_copy_upsert import (
    ARRAY,
    AsyncMock,
    Column,
    Date,
    JSON,
    MetaData,
    Path,
    ProviderProfileImportRun,
    ProviderProfileSourceRecord,
    SimpleNamespace,
    String,
    Table,
    _AcquireDb,
    _RecordingConnection,
    _RecordingDriver,
    _source_record,
    date,
    florida,
    load_dotenv,
    os,
    pytest,
    uuid,
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
    run_row_by_key = {"metrics": {"facts": 12}}

    status_error = await florida._mark_failed_run_status(
        run_id="run-1",
        run_row=run_row_by_key,
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
    assert run_row_by_key["status"] == "failed"
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
    run_row_by_key = {"status": "completed", "metrics": {"published_providers": 12}}

    status_error = await florida._mark_failed_run_status(
        run_id="run-1",
        run_row=run_row_by_key,
        original_error=RuntimeError("post-publication cleanup failure"),
        cleanup_error=None,
    )

    assert status_error is None
    assert run_row_by_key == {
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


def _copy_contract_model(table_name: str):
    metadata = MetaData()
    publication_target = Table(
        table_name,
        metadata,
        Column("row_id", String(64), primary_key=True),
        Column("payload", JSON, nullable=False),
        Column("tags", ARRAY(String)),
        Column("occurred_on", Date),
        schema="pg_temp",
    )
    return type("SyntheticCopyTarget", (), {"__table__": publication_target})


async def _copy_contract_rows(connection, model, table_name: str):
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
    return await connection.all(
        f"""
        SELECT row_id, payload, tags, occurred_on
          FROM "{table_name}"
         ORDER BY row_id;
        """
    )


@pytest.mark.asyncio
async def test_copy_upsert_postgres_temp_table_conflict_contract():
    """Verify copy upsert postgres temp table conflict contract."""
    if os.getenv("HLTHPRT_TEST_FL_MQA_COPY_POSTGRES") != "1":
        pytest.skip("set HLTHPRT_TEST_FL_MQA_COPY_POSTGRES=1 for PostgreSQL proof")

    root = Path(__file__).resolve().parents[1]
    load_dotenv(root / ".env", override=False)
    table_name = f"fl_pp_copy_e2e_{uuid.uuid4().hex[:12]}"
    model = _copy_contract_model(table_name)

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
            source_rows = await _copy_contract_rows(connection, model, table_name)
    finally:
        await florida.db.disconnect()

    assert [source_row._mapping["row_id"] for source_row in source_rows] == ["new", "same"]
    same = source_rows[1]._mapping
    assert same["payload"] == {"version": 2}
    assert same["tags"] == ["updated"]
    assert same["occurred_on"] == date(2026, 7, 27)
