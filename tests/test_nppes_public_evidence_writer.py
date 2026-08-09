# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Session cleanup and cancellation proof for NPPES evidence admission."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from dataclasses import replace
from unittest.mock import AsyncMock

import pytest

from process.nppes_public_evidence_writer import (
    NppesPublicEvidenceWriterError,
    _assert_no_partial_release,
    _assert_stage_counts,
    _analyze_admission_tables,
    _batch_size,
    _copy_stage,
    _drain_stage_cleanup,
    _drop_stages,
    _finalize,
    _has_existing_admission,
    _insert_single,
    _invoke,
    _schema_name,
    _stage_complete_replay,
    admit_nppes_registry_archive,
)
from process.nppes_public_evidence_rows import ADMISSION_COLUMNS, admission_values
from tests.test_nppes_public_evidence_replay import _config, _prepared
from process.nppes_public_evidence_replay import prepare_nppes_registry_replay
from public_evidence.nppes_registry_storage_contract import (
    NppesRegistryMemberEncoder as RealMemberEncoder,
)


class _Database:
    def __init__(self, connection) -> None:
        self.connection = connection

    @asynccontextmanager
    async def acquire_driver(self):
        yield self.connection


class _RecordingConnection:
    def __init__(self, *, fail_first_create: bool = False) -> None:
        self.statements: list[str] = []
        self.fail_first_create = fail_first_create

    async def execute(self, statement: str, *_args):
        self.statements.append(statement)
        if self.fail_first_create and statement.startswith("CREATE TEMP TABLE"):
            raise RuntimeError("PRIVATE-CREATE-MARKER")
        return "OK"


class _CopyConnection:
    def __init__(self, status: object) -> None:
        self.status = status

    async def copy_records_to_table(self, *_args, **_kwargs):
        return self.status


class _SuccessfulCopyConnection:
    def __init__(self) -> None:
        self.copy_counts: list[int] = []

    async def copy_records_to_table(self, *_args, **kwargs):
        record_count = len(kwargs["records"])
        self.copy_counts.append(record_count)
        return f"COPY {record_count}"


class _TransactionConnection:
    @asynccontextmanager
    async def transaction(self):
        yield

    async def execute(self, *_args):
        return "OK"


def _patch_admission_guards(monkeypatch) -> None:
    monkeypatch.setattr(
        "process.nppes_public_evidence_writer._assert_catalog",
        AsyncMock(),
    )
    monkeypatch.setattr(
        "process.nppes_public_evidence_writer._has_existing_admission",
        AsyncMock(return_value=False),
    )
    monkeypatch.setattr(
        "process.nppes_public_evidence_writer._assert_no_partial_release",
        AsyncMock(),
    )


@pytest.mark.asyncio
async def test_stage_cleanup_never_targets_a_permanent_search_path_table() -> None:
    connection = _RecordingConnection()
    await _drop_stages(connection)
    statement = connection.statements[0]
    assert statement.count("pg_temp.") == 5
    assert "DROP TABLE IF EXISTS \"nppes_stage" not in statement


@pytest.mark.asyncio
async def test_bulk_admission_refreshes_all_validator_table_statistics() -> None:
    connection = _RecordingConnection()
    await _analyze_admission_tables(connection, "mrf")
    assert connection.statements == [
        "ANALYZE \"mrf\".\"public_evidence_source_record\", "
        "\"mrf\".\"public_evidence_nppes_registry_member\", "
        "\"mrf\".\"public_evidence_record\", "
        "\"mrf\".\"public_evidence_record_source_link\", "
        "\"mrf\".\"public_evidence_npi_enumeration\""
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("status", (None, "INSERT 0 2", "COPY 1", "COPY -2"))
async def test_copy_stage_requires_an_exact_copy_count(status: object) -> None:
    with pytest.raises(NppesPublicEvidenceWriterError):
        await _copy_stage(
            _CopyConnection(status),
            "nppes_stage_member",
            [(1,), (2,)],
        )


@pytest.mark.asyncio
async def test_empty_copy_and_sync_callback_are_no_ops() -> None:
    connection = _CopyConnection("not-used")
    callback_values: list[int] = []

    await _copy_stage(connection, "nppes_stage_member", [])
    await _invoke(lambda value: callback_values.append(value), 7)

    assert callback_values == [7]


def test_schema_and_batch_size_contract(monkeypatch) -> None:
    with pytest.raises(NppesPublicEvidenceWriterError):
        _schema_name("bad-name")

    monkeypatch.delenv("HLTHPRT_NPPES_PUBLIC_EVIDENCE_BATCH_SIZE", raising=False)
    assert _batch_size() == 20_000
    for raw_value in ("not-a-number", "99", "100001"):
        monkeypatch.setenv("HLTHPRT_NPPES_PUBLIC_EVIDENCE_BATCH_SIZE", raw_value)
        with pytest.raises(NppesPublicEvidenceWriterError):
            _batch_size()
    for raw_value, expected in (("100", 100), ("100000", 100_000)):
        monkeypatch.setenv("HLTHPRT_NPPES_PUBLIC_EVIDENCE_BATCH_SIZE", raw_value)
        assert _batch_size() == expected


@pytest.mark.asyncio
async def test_cleanup_failure_cannot_swallow_caller_cancellation(monkeypatch) -> None:
    cleanup_started = asyncio.Event()
    finish_cleanup = asyncio.Event()

    async def failing_cleanup(_connection) -> None:
        cleanup_started.set()
        await finish_cleanup.wait()
        raise RuntimeError("PRIVATE-CLEANUP-MARKER")

    monkeypatch.setattr(
        "process.nppes_public_evidence_writer._drop_stages",
        failing_cleanup,
    )
    cleanup_owner = asyncio.create_task(_drain_stage_cleanup(object()))
    await cleanup_started.wait()
    cleanup_owner.cancel()
    finish_cleanup.set()
    with pytest.raises(asyncio.CancelledError):
        await cleanup_owner


@pytest.mark.asyncio
async def test_cleanup_failure_propagates_without_cancellation(monkeypatch) -> None:
    async def failing_cleanup(_connection) -> None:
        raise RuntimeError("PRIVATE-CLEANUP-MARKER")

    monkeypatch.setattr(
        "process.nppes_public_evidence_writer._drop_stages",
        failing_cleanup,
    )
    with pytest.raises(RuntimeError, match="PRIVATE-CLEANUP-MARKER"):
        await _drain_stage_cleanup(object())


@pytest.mark.asyncio
async def test_stage_replay_flushes_each_bounded_batch(
    monkeypatch,
    tmp_path,
) -> None:
    prepared = await prepare_nppes_registry_replay(_prepared(tmp_path), _config())
    connection = _SuccessfulCopyConnection()
    cancellation_checks: list[None] = []
    progress_counts: list[int] = []
    monkeypatch.setattr(
        "process.nppes_public_evidence_writer._batch_size",
        lambda: 1,
    )

    await _stage_complete_replay(
        connection,
        prepared,
        cancel_check=lambda: cancellation_checks.append(None),
        progress=lambda count: progress_counts.append(count),
    )

    assert cancellation_checks == [None, None]
    assert progress_counts == [1, 2]
    assert connection.copy_counts == [1, 1, 1, 1, 1, 1, 1]


@pytest.mark.asyncio
async def test_stage_replay_rejects_header_or_manifest_drift(
    monkeypatch,
    tmp_path,
) -> None:
    prepared = await prepare_nppes_registry_replay(_prepared(tmp_path), _config())

    class _WrongHeaderRows:
        def __init__(self, _archive) -> None:
            self.header = ("wrong",)

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

    monkeypatch.setattr(
        "process.nppes_public_evidence_writer.NppesPrimaryCsvRows",
        _WrongHeaderRows,
    )
    with pytest.raises(NppesPublicEvidenceWriterError):
        await _stage_complete_replay(
            _SuccessfulCopyConnection(),
            prepared,
            cancel_check=None,
            progress=None,
        )

    class _MismatchedEncoder:
        def __init__(self, *args) -> None:
            self._encoder = RealMemberEncoder(*args)

        def encode(self, row_values):
            return self._encoder.encode(row_values)

        def finish(self):
            completed = self._encoder.finish()
            return replace(completed, manifest_sha256=b"\x00" * 32)

    monkeypatch.setattr(
        "process.nppes_public_evidence_writer.NppesPrimaryCsvRows",
        __import__(
            "process.nppes_public_evidence_members",
            fromlist=["NppesPrimaryCsvRows"],
        ).NppesPrimaryCsvRows,
    )
    monkeypatch.setattr(
        "process.nppes_public_evidence_writer.NppesRegistryMemberEncoder",
        _MismatchedEncoder,
    )
    with pytest.raises(NppesPublicEvidenceWriterError):
        await _stage_complete_replay(
            _SuccessfulCopyConnection(),
            prepared,
            cancel_check=None,
            progress=None,
        )


@pytest.mark.asyncio
async def test_stage_and_release_census_mismatches_fail_closed(tmp_path) -> None:
    prepared = await prepare_nppes_registry_replay(_prepared(tmp_path), _config())

    class _CountConnection:
        async def fetchval(self, *_args):
            return -1

    with pytest.raises(NppesPublicEvidenceWriterError):
        await _assert_stage_counts(_CountConnection(), prepared)
    with pytest.raises(NppesPublicEvidenceWriterError):
        await _assert_no_partial_release(_CountConnection(), "mrf", prepared)


@pytest.mark.asyncio
async def test_existing_admission_requires_exact_row_and_seal(tmp_path) -> None:
    prepared = await prepare_nppes_registry_replay(_prepared(tmp_path), _config())
    exact_values = admission_values(prepared.admission_row)
    exact_row_by_column = dict(zip(ADMISSION_COLUMNS, exact_values, strict=True))

    class _ExistingConnection:
        def __init__(self, row_by_name, is_sealed: bool) -> None:
            self.row_by_name = row_by_name
            self.is_sealed = is_sealed

        async def fetchrow(self, *_args):
            return self.row_by_name

        async def fetchval(self, *_args):
            return self.is_sealed

    mismatched_row_by_column = dict(exact_row_by_column)
    mismatched_row_by_column[ADMISSION_COLUMNS[-1]] = not exact_values[-1]
    with pytest.raises(NppesPublicEvidenceWriterError):
        await _has_existing_admission(
            _ExistingConnection(mismatched_row_by_column, True),
            "mrf",
            prepared,
        )
    with pytest.raises(NppesPublicEvidenceWriterError):
        await _has_existing_admission(
            _ExistingConnection(exact_row_by_column, False),
            "mrf",
            prepared,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("stored_row", (None, {"identity_ref": "different"}))
async def test_identical_insert_replay_rejects_missing_or_different_row(
    stored_row,
) -> None:
    class _InsertConnection:
        async def execute(self, *_args):
            return "INSERT 0 0"

        async def fetchrow(self, *_args):
            return stored_row

    with pytest.raises(NppesPublicEvidenceWriterError):
        await _insert_single(
            _InsertConnection(),
            "mrf",
            "example",
            ("identity_ref",),
            ("expected",),
            allow_identical=True,
        )


@pytest.mark.asyncio
async def test_finalize_short_circuits_an_exact_concurrent_replay(
    monkeypatch,
    tmp_path,
) -> None:
    prepared = await prepare_nppes_registry_replay(_prepared(tmp_path), _config())
    monkeypatch.setattr(
        "process.nppes_public_evidence_writer._has_existing_admission",
        AsyncMock(return_value=True),
    )
    assert await _finalize(_TransactionConnection(), "mrf", prepared) == "already_present"


@pytest.mark.asyncio
async def test_public_writer_rejects_wrong_config_type(tmp_path) -> None:
    with pytest.raises(NppesPublicEvidenceWriterError):
        await admit_nppes_registry_archive(_prepared(tmp_path), object())


@pytest.mark.asyncio
async def test_partial_stage_creation_is_normalized_and_always_cleaned(
    monkeypatch,
    tmp_path,
) -> None:
    prepared = await prepare_nppes_registry_replay(_prepared(tmp_path), _config())
    connection = _RecordingConnection(fail_first_create=True)
    _patch_admission_guards(monkeypatch)

    with pytest.raises(NppesPublicEvidenceWriterError) as caught:
        await admit_nppes_registry_archive(
            prepared,
            _config(),
            database=_Database(connection),
        )

    drop_statements = [
        statement
        for statement in connection.statements
        if statement.startswith("DROP TABLE")
    ]
    assert len(drop_statements) == 2
    assert all(statement.count("pg_temp.") == 5 for statement in drop_statements)
    assert caught.value.__cause__ is None
    assert caught.value.__context__ is None


@pytest.mark.asyncio
async def test_exact_replay_returns_before_any_temporary_table_write(
    monkeypatch,
    tmp_path,
) -> None:
    prepared = await prepare_nppes_registry_replay(_prepared(tmp_path), _config())
    connection = _RecordingConnection()
    catalog_check = AsyncMock()
    existing_check = AsyncMock(return_value=True)
    drop_stages = AsyncMock()
    monkeypatch.setattr(
        "process.nppes_public_evidence_writer._assert_catalog",
        catalog_check,
    )
    monkeypatch.setattr(
        "process.nppes_public_evidence_writer._has_existing_admission",
        existing_check,
    )
    monkeypatch.setattr(
        "process.nppes_public_evidence_writer._drop_stages",
        drop_stages,
    )

    receipt = await admit_nppes_registry_archive(
        prepared,
        _config(),
        database=_Database(connection),
    )

    assert receipt.write_state == "already_present"
    assert repr(receipt) == "<nppes-registry-admission-receipt>"
    catalog_check.assert_awaited_once()
    existing_check.assert_awaited_once()
    drop_stages.assert_not_awaited()
    assert connection.statements == []


@pytest.mark.asyncio
async def test_cancellation_during_stage_replay_cleans_the_pooled_session(
    monkeypatch,
    tmp_path,
) -> None:
    prepared = await prepare_nppes_registry_replay(_prepared(tmp_path), _config())
    connection = _RecordingConnection()
    _patch_admission_guards(monkeypatch)
    create_stages = AsyncMock()
    cleanup_stages = AsyncMock()
    monkeypatch.setattr(
        "process.nppes_public_evidence_writer._create_stages",
        create_stages,
    )
    monkeypatch.setattr(
        "process.nppes_public_evidence_writer._drop_stages",
        cleanup_stages,
    )
    monkeypatch.setattr(
        "process.nppes_public_evidence_writer._stage_complete_replay",
        AsyncMock(side_effect=asyncio.CancelledError()),
    )

    with pytest.raises(asyncio.CancelledError):
        await admit_nppes_registry_archive(
            prepared,
            _config(),
            database=_Database(connection),
        )

    create_stages.assert_awaited_once()
    cleanup_stages.assert_awaited_once()
