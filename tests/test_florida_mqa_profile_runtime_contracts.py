from __future__ import annotations

import hashlib
import importlib
import io
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import ARRAY, Date, DateTime, String

from db.models import ProviderProfileProjection, ProviderProfileSourceRecord

florida = importlib.import_module("process.florida_mqa_profile")


class _Row:
    def __init__(self, **mapping):
        self._mapping = mapping


class _Transaction:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        return False


def test_source_download_streams_to_artifact_and_returns_exact_digest(
    monkeypatch,
    tmp_path,
):
    payload = b"reviewed provider profile facts"
    client = florida.FloridaMQAClient(
        "https://example.invalid/",
        "test-user",
        "test-password",
    )
    monkeypatch.setattr(client, "_open", lambda _url: io.BytesIO(payload))
    target = tmp_path / "artifacts" / "profile.zip"

    digest, size = client.download(
        florida.FLORIDA_SOURCES["profile_master"],
        target,
    )

    assert target.read_bytes() == payload
    assert digest == hashlib.sha256(payload).hexdigest()
    assert size == len(payload)


def test_copy_value_normalization_rejects_invalid_typed_values():
    assert florida._strip_postgres_nuls(
        {"key\x00": ("value\x00", ["nested\x00"])}
    ) == {"key": ("value", ["nested"])}
    assert florida._copy_value_for_type(String(), None) is None
    assert florida._copy_value_for_type(
        DateTime(timezone=False),
        "2026-07-27T12:30:00Z",
    ) == datetime(2026, 7, 27, 12, 30)
    assert florida._copy_value_for_type(
        Date(),
        datetime(2026, 7, 27, 12, 30),
    ).isoformat() == "2026-07-27"
    assert florida._copy_value_for_type(
        ARRAY(String()),
        '["A", "B"]',
    ) == ["A", "B"]
    with pytest.raises(ValueError, match="JSON array"):
        florida._copy_value_for_type(ARRAY(String()), "not-json")
    with pytest.raises(ValueError, match="must be a sequence"):
        florida._copy_value_for_type(ARRAY(String()), 7)
    with pytest.raises(ValueError, match="timestamp value"):
        florida._copy_value_for_type(DateTime(), 7)
    with pytest.raises(ValueError, match="date value"):
        florida._copy_value_for_type(Date(), 7)


def test_copy_runtime_configuration_is_bounded_and_fail_closed(monkeypatch):
    monkeypatch.setenv("HLTHPRT_FL_MQA_COPY_UPSERT_MIN_ROWS", "invalid")
    monkeypatch.setenv("HLTHPRT_FL_MQA_COPY_UPSERT_BATCH_ROWS", "-2")
    assert florida._copy_upsert_min_rows() == florida.DEFAULT_COPY_UPSERT_MIN_ROWS
    assert florida._copy_upsert_batch_rows() == 1

    for disabled_value in ("0", "false", "NO", "off"):
        monkeypatch.setenv("HLTHPRT_FL_MQA_COPY_UPSERT", disabled_value)
        assert florida._is_copy_upsert_enabled() is False
    monkeypatch.setenv("HLTHPRT_FL_MQA_COPY_UPSERT", "yes")
    assert florida._is_copy_upsert_enabled() is True

    with pytest.raises(ValueError, match="unsafe PostgreSQL identifier"):
        florida._validated_identifier("x" * 64)
    assert florida._quoted_identifier("safe_name") == '"safe_name"'


@pytest.mark.asyncio
async def test_copy_connection_rejects_unknown_conflict_key_and_missing_driver():
    connection = type(
        "CopyConnection",
        (),
        {"raw_connection": object()},
    )()
    source_rows = [
        {
            "record_id": "a" * 64,
            "run_id": "b" * 32,
        }
    ]

    with pytest.raises(ValueError, match="is not a column"):
        await florida._copy_upsert_chunk_on_connection(
            connection,
            ProviderProfileSourceRecord,
            source_rows,
            "unknown_key",
        )
    with pytest.raises(
        florida._CopyUpsertUnavailable,
        match="lacks copy_records_to_table",
    ):
        await florida._copy_upsert_chunk_on_connection(
            connection,
            ProviderProfileSourceRecord,
            source_rows,
            "record_id",
        )


@pytest.mark.asyncio
async def test_copy_chunk_translates_driver_not_implemented(monkeypatch):
    class Acquire:
        async def __aenter__(self):
            return object()

        async def __aexit__(self, exc_type, exc, traceback):
            return False

    database = type(
        "CopyDb",
        (),
        {"acquire": lambda self: Acquire()},
    )()
    monkeypatch.setattr(florida, "db", database)
    monkeypatch.setattr(
        florida,
        "_copy_upsert_chunk_on_connection",
        AsyncMock(side_effect=NotImplementedError("COPY unavailable")),
    )

    with pytest.raises(florida._CopyUpsertUnavailable, match="COPY unavailable"):
        await florida._copy_upsert_chunk(
            ProviderProfileSourceRecord,
            [{"record_id": "a" * 64}],
            "record_id",
        )


@pytest.mark.asyncio
async def test_empty_upsert_is_a_noop_and_invalid_run_ids_are_rejected(
    monkeypatch,
):
    values_fallback = AsyncMock()
    monkeypatch.setattr(florida, "_upsert_rows_values", values_fallback)

    await florida._upsert_rows(
        ProviderProfileSourceRecord,
        [],
        "record_id",
    )

    values_fallback.assert_not_awaited()
    with pytest.raises(ValueError, match="run_id_invalid"):
        await florida._claim_import_run({"run_id": "not-a-run-id"})


@pytest.mark.asyncio
async def test_retained_counts_distinguish_unique_rows_from_physical_input(
    monkeypatch,
):
    database = type(
        "RetainedCountDb",
        (),
        {
            "first": AsyncMock(
                return_value=_Row(
                    source_records=1,
                    facts=7,
                    matched_records=1,
                    projectable_records=1,
                )
            )
        },
    )()
    monkeypatch.setattr(florida, "db", database)

    retained_counts_by_key = await florida._retained_import_counts("run-1")

    assert retained_counts_by_key == {
        "retained_source_records": 1,
        "retained_facts": 7,
        "retained_matched_records": 1,
        "retained_non_projectable_records": 0,
    }
    database.first.assert_awaited_once()


@pytest.mark.asyncio
async def test_projection_batches_page_distinct_npis_and_preserve_provenance(
    monkeypatch,
):
    fact_by_key = {
        "npi": 1000000004,
        "logical_fact_key": "license-key",
        "category": "licenses",
        "fact_type": "state_license",
        "display": "Florida physician license ME12345 is active",
        "value_json": {
            "license_number": "ME12345",
            "status": "CLEAR/ACTIVE",
        },
        "assertion_type": "state_reported",
        "verification_status": "government_source",
        "effective_start": "2024-01-01",
        "effective_end": "2026-01-01",
        "sensitive": False,
        "public_default": True,
        "source_record_id": "record-1",
        "source_json": {
            "source_key": florida.FL_MQA_SOURCE_KEY,
            "dataset": "licensure_current",
        },
    }
    database = type(
        "ProjectionDb",
        (),
        {
            "all": AsyncMock(
                side_effect=[
                    [_Row(npi=1000000004)],
                    [_Row(**fact_by_key)],
                    [],
                ]
            )
        },
    )()
    monkeypatch.setattr(florida, "db", database)

    batches = [
        batch
        async for batch in florida._projection_row_batches(
            "a" * 32,
            {"licenses", "education"},
            datetime(2026, 7, 27, tzinfo=UTC),
            npi_batch_size=1,
        )
    ]

    assert len(batches) == 1
    source_row = batches[0][0]
    assert source_row["npi"] == 1000000004
    assert source_row["generation_id"] == "a" * 32
    assert source_row["source_keys"] == [florida.FL_MQA_SOURCE_KEY]
    assert source_row["profile_json"]["categories"]["licenses"]["availability"] == "available"
    assert source_row["profile_json"]["categories"]["education"]["availability"] == "not_reported"
    assert source_row["evidence_json"]["records"] == [fact_by_key["source_json"]]
    assert database.all.await_count == 3


@pytest.mark.asyncio
async def test_projection_batches_emit_empty_loaded_categories_without_facts(
    monkeypatch,
):
    database = type(
        "ProjectionDb",
        (),
        {
            "all": AsyncMock(
                side_effect=[
                    [_Row(npi=1000000005)],
                    [],
                    [],
                ]
            )
        },
    )()
    monkeypatch.setattr(florida, "db", database)

    batches = [
        batch
        async for batch in florida._projection_row_batches(
            "b" * 32,
            {"licenses"},
            datetime(2026, 7, 27, tzinfo=UTC),
        )
    ]

    assert batches[0][0]["profile_json"]["categories"]["licenses"] == {
        "availability": "not_reported",
        "items": [],
    }
    assert batches[0][0]["evidence_json"]["records"] == []


@pytest.mark.asyncio
async def test_post_success_retention_protects_live_and_rollback_generations(
    monkeypatch,
    tmp_path,
):
    """Verify post success retention protects live and rollback generations."""
    live_run = "b" * 32
    rollback_run = "c" * 32
    completed_run = "a" * 32
    expired_failed_run = "d" * 32
    current_run = "e" * 32
    old_finished_at = florida._utcnow() - timedelta(days=30)
    live_name = ProviderProfileProjection.__tablename__
    responses = iter(
        (
            [_Row(tablename=live_name), _Row(tablename=f"{live_name}_old")],
            [_Row(generation_id=live_run)],
            [_Row(generation_id=rollback_run)],
            [
                _Row(
                    run_id=completed_run,
                    status="completed",
                    finished_at=old_finished_at,
                ),
                _Row(
                    run_id=expired_failed_run,
                    status="failed",
                    finished_at=old_finished_at,
                ),
                _Row(
                    run_id=live_run,
                    status="completed",
                    finished_at=old_finished_at,
                ),
                _Row(
                    run_id=current_run,
                    status="completed",
                    finished_at=old_finished_at,
                ),
            ],
        )
    )

    class RetentionDb:
        scalar = AsyncMock(return_value=1)

        def transaction(self):
            return _Transaction()

        async def all(self, _statement, **_parameters):
            return next(responses)

    delete_rows = AsyncMock(
        return_value={"facts": 4, "source_records": 5, "artifacts": 2}
    )
    monkeypatch.setattr(florida, "db", RetentionDb())
    monkeypatch.setattr(florida, "_delete_retained_payload_rows", delete_rows)
    for run_id in (completed_run, expired_failed_run, live_run, rollback_run):
        (tmp_path / run_id).mkdir()

    operation_result = await florida._post_success_retention(
        run_id=current_run,
        artifact_root=tmp_path,
        failed_retention_days=7,
    )

    assert operation_result["status"] == "completed"
    assert operation_result["protected_audit_run_ids"] == [live_run, rollback_run]
    assert operation_result["deleted_run_ids"] == [completed_run, expired_failed_run]
    assert operation_result["deleted_rows"]["facts"] == 4
    assert operation_result["artifact_directories"]["deleted"] == [
        completed_run,
        expired_failed_run,
    ]
    assert (tmp_path / live_run).is_dir()
    assert (tmp_path / rollback_run).is_dir()
    delete_rows.assert_awaited_once_with([completed_run, expired_failed_run])


@pytest.mark.asyncio
async def test_post_success_retention_reports_directory_cleanup_errors(
    monkeypatch,
    tmp_path,
):
    run_id = "f" * 32
    database = type(
        "RetentionDb",
        (),
        {
            "scalar": AsyncMock(return_value=1),
            "all": AsyncMock(side_effect=[[], []]),
            "transaction": lambda self: _Transaction(),
        },
    )()
    monkeypatch.setattr(florida, "db", database)
    monkeypatch.setattr(
        florida,
        "_remove_artifact_run_directories",
        lambda _root, _run_ids: {
            "deleted": [],
            "missing": [],
            "errors": {"a" * 32: "OSError: unavailable"},
        },
    )

    operation_result = await florida._post_success_retention(
        run_id=run_id,
        artifact_root=tmp_path,
        failed_retention_days=7,
    )

    assert operation_result["status"] == "completed_with_directory_errors"
    assert operation_result["deleted_run_ids"] == []
