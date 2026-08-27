# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio

import importlib

from pathlib import Path

from types import SimpleNamespace

from unittest.mock import AsyncMock

import os

import datetime

import uuid

from contextlib import asynccontextmanager

import pytest

from process.nppes_public_evidence_import import NPPES_RIGHTS_PROOF_SHA256

from tests.test_process_npi_unit import (
    ROOT,
    _AmbiguousPublicationConnection,
    _ShutdownRawConnection,
    _build_minimal_row,
    _fake_make_class_factory,
    _install_shutdown_success_collaborators,
    _shutdown_stage_classes,
    npi_module,
)

@pytest.mark.asyncio
async def test_npi_import_lease_rejects_a_missing_advisory_lock(npi_module):
    connection = SimpleNamespace(
        fetchval=AsyncMock(side_effect=[941, False])
    )
    worker_context_by_key = {
        npi_module._NPI_IMPORT_LEASE_KEY: npi_module._NpiImportLease(
            manager=object(),
            connection=connection,
            backend_pid=941,
        )
    }

    with pytest.raises(npi_module.NPIPrerequisiteError, match="lease was lost"):
        await npi_module._assert_npi_import_lease(worker_context_by_key)

    lock_query = connection.fetchval.await_args_list[1].args[0]
    assert "pg_catalog.pg_locks" in lock_query
    for required_fragment in (
        "held_lock.granted",
        "held_lock.mode = 'ExclusiveLock'",
        "held_lock.database",
        "current_database()",
        "held_lock.classid",
        "held_lock.objid",
        "held_lock.objsubid = 1",
        "held_lock.pid = pg_backend_pid()",
    ):
        assert required_fragment in lock_query

@pytest.mark.asyncio
async def test_nppes_runtime_accepts_the_proved_postgres_configuration(npi_module):
    connection = SimpleNamespace(
        fetchrow=AsyncMock(return_value=(180002, "on", "on", "on", "pglz"))
    )
    worker_context_by_key = {
        npi_module._NPI_IMPORT_LEASE_KEY: npi_module._NpiImportLease(
            manager=object(),
            connection=connection,
            backend_pid=941,
        )
    }

    await npi_module._assert_nppes_postgres_runtime(worker_context_by_key)

    assert "current_setting('wal_compression')" in connection.fetchrow.await_args.args[0]

@pytest.mark.parametrize(
    "settings",
    (
        (170999, "on", "on", "on", "pglz"),
        (180002, "off", "on", "on", "pglz"),
        (180002, "on", "off", "on", "pglz"),
        (180002, "on", "on", "off", "pglz"),
        (180002, "on", "on", "on", "off"),
    ),
)
@pytest.mark.asyncio
async def test_nppes_runtime_rejects_unproved_postgres_settings(
    npi_module,
    settings,
):
    connection = SimpleNamespace(fetchrow=AsyncMock(return_value=settings))
    worker_context_by_key = {
        npi_module._NPI_IMPORT_LEASE_KEY: npi_module._NpiImportLease(
            manager=object(),
            connection=connection,
            backend_pid=941,
        )
    }

    with pytest.raises(
        npi_module.NPIPrerequisiteError,
        match="durability configuration is invalid",
    ):
        await npi_module._assert_nppes_postgres_runtime(worker_context_by_key)

@pytest.mark.asyncio
async def test_nppes_catalog_preflight_uses_the_lease_connection(
    monkeypatch,
    npi_module,
):
    connection = object()
    worker_context_by_key = {
        npi_module._NPI_IMPORT_LEASE_KEY: npi_module._NpiImportLease(
            manager=object(),
            connection=connection,
            backend_pid=941,
        )
    }
    assert_catalog = AsyncMock()
    monkeypatch.setattr(npi_module, "assert_nppes_admission_catalog", assert_catalog)

    await npi_module._assert_nppes_storage_catalog(
        worker_context_by_key,
        "mrf",
    )

    assert_catalog.assert_awaited_once_with(connection, "mrf")

@pytest.mark.asyncio
async def test_npi_import_lease_rejects_a_parallel_attempt_and_closes_connection(
    monkeypatch,
    npi_module,
):
    connection = SimpleNamespace(fetchval=AsyncMock(side_effect=[False, 811]))
    exit_events: list[None] = []

    @asynccontextmanager
    async def lease_manager():
        try:
            yield connection
        finally:
            exit_events.append(None)

    monkeypatch.setattr(npi_module.db, "acquire_driver", lease_manager)
    worker_context_by_key: dict[str, object] = {}
    with pytest.raises(npi_module.NPIPrerequisiteError, match="already active"):
        await npi_module._acquire_npi_import_lease(worker_context_by_key)
    assert npi_module._NPI_IMPORT_LEASE_KEY not in worker_context_by_key
    assert exit_events == [None]

@pytest.mark.asyncio
async def test_staged_write_failure_cancels_and_drains_its_sibling(npi_module):
    sibling_started = asyncio.Event()

    async def waiting_write() -> None:
        sibling_started.set()
        await asyncio.Event().wait()

    async def failing_write() -> None:
        await sibling_started.wait()
        raise RuntimeError("synthetic write failure")

    waiting_task = asyncio.create_task(waiting_write())
    failing_task = asyncio.create_task(failing_write())
    owned_tasks = [waiting_task, failing_task]
    with pytest.raises(RuntimeError, match="synthetic write failure"):
        await npi_module._drain_npi_save_tasks(owned_tasks)
    assert waiting_task.cancelled()
    assert failing_task.done()
    assert owned_tasks == []

@pytest.mark.asyncio
async def test_controlled_test_mode_fails_before_database_or_staging(
    monkeypatch,
    npi_module,
):
    ensure_database = AsyncMock()
    staging_reset = AsyncMock()
    release_lease = AsyncMock()
    monkeypatch.setattr(npi_module, "ensure_database", ensure_database)
    monkeypatch.setattr(npi_module, "_prepare_npi_staging", staging_reset)
    monkeypatch.setattr(npi_module, "_release_npi_import_lease", release_lease)
    worker_context_by_key = {
        "context": {"control_run_id": "run_synthetic"},
        "import_date": "20260809",
    }
    with pytest.raises(npi_module.NPIPrerequisiteError, match="isolated publication"):
        await npi_module.process_data(
            worker_context_by_key,
            {"test_mode": True, "run_id": "run_synthetic"},
        )
    ensure_database.assert_not_awaited()
    staging_reset.assert_not_awaited()
    assert worker_context_by_key["context"]["run"] == 0

@pytest.mark.asyncio
async def test_shutdown_rejects_test_mode_before_database_or_publication(
    monkeypatch,
    npi_module,
):
    assert_lease = AsyncMock()
    release_lease = AsyncMock()
    ensure_database = AsyncMock()
    monkeypatch.setattr(npi_module, "_assert_npi_import_lease", assert_lease)
    monkeypatch.setattr(npi_module, "_release_npi_import_lease", release_lease)
    monkeypatch.setattr(npi_module, "ensure_database", ensure_database)
    worker_context_map = {
        "context": {
            "run": 1,
            "test_mode": True,
            "control_run_id": "run_test_mode",
            "_control_attempt_id": "run_test_mode:" + "a" * 32,
            "_control_attempt_started_at": "2026-08-09T00:00:00.000000+00:00",
        },
        "import_date": "20260809",
    }
    with pytest.raises(npi_module.NPIPrerequisiteError, match="cannot publish"):
        await npi_module.shutdown(worker_context_map)
    assert_lease.assert_awaited_once()
    ensure_database.assert_not_awaited()
    release_lease.assert_awaited_once()

@pytest.mark.asyncio
async def test_shutdown_normalizes_missing_required_evidence_receipt(
    monkeypatch,
    npi_module,
):
    monkeypatch.setenv("HLTHPRT_NPPES_PUBLIC_EVIDENCE_MODE", "required")
    monkeypatch.setenv(
        "HLTHPRT_NPPES_RIGHTS_PROOF_SHA256",
        NPPES_RIGHTS_PROOF_SHA256,
    )
    monkeypatch.setattr(npi_module, "_assert_npi_import_lease", AsyncMock())
    monkeypatch.setattr(npi_module, "_release_npi_import_lease", AsyncMock())
    ensure_database = AsyncMock()
    monkeypatch.setattr(npi_module, "ensure_database", ensure_database)
    worker_context_map = {
        "context": {
            "run": 1,
            "test_mode": False,
            "control_run_id": "run_missing_receipt",
            "_control_attempt_id": "run_missing_receipt:" + "b" * 32,
            "_control_attempt_started_at": "2026-08-09T00:00:00.000000+00:00",
        },
        "import_date": "20260809",
    }
    with pytest.raises(npi_module.NPIPrerequisiteError) as caught:
        await npi_module.shutdown(worker_context_map)
    assert str(caught.value) == "NPPES public-evidence admission receipt is invalid"
    assert caught.value.__cause__ is None
    assert caught.value.__context__ is None
    ensure_database.assert_not_awaited()

@pytest.mark.asyncio
async def test_process_data_failure_does_not_mark_run(monkeypatch, npi_module):

    monkeypatch.setenv("HLTHPRT_NPPES_DOWNLOAD_URL_DIR", "https://example.com/")
    monkeypatch.setenv("HLTHPRT_NPPES_DOWNLOAD_URL_FILE", "feed.html")

    monkeypatch.setattr(npi_module, "download_it", AsyncMock(side_effect=RuntimeError("boom")))
    monkeypatch.setattr(npi_module, "ensure_database", AsyncMock())
    monkeypatch.setattr(npi_module, "_ensure_required_extensions", AsyncMock())
    monkeypatch.setattr(npi_module, "_assert_nucc_ready", AsyncMock())
    monkeypatch.setattr(npi_module, "_assert_nppes_canonical_ready", AsyncMock())
    monkeypatch.setattr(npi_module, "_load_nucc_taxonomy_int_code_map", AsyncMock(return_value={}))
    monkeypatch.setattr(npi_module.db, "status", AsyncMock())
    monkeypatch.setattr(npi_module, "_acquire_npi_import_lease", AsyncMock())
    monkeypatch.setattr(npi_module, "_assert_npi_import_lease", AsyncMock())
    monkeypatch.setattr(npi_module, "_release_npi_import_lease", AsyncMock())

    worker_context_map = {
        "context": {},
        "redis": SimpleNamespace(enqueue_job=AsyncMock()),
        "import_date": "20251107",
    }

    with pytest.raises(RuntimeError):
        await npi_module.process_data(worker_context_map)

    assert worker_context_map["context"].get("run", 0) == 0

@pytest.mark.asyncio
async def test_startup_initializes_tables(monkeypatch, npi_module):

    monkeypatch.delenv("HLTHPRT_IMPORT_ID_OVERRIDE", raising=False)
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "testschema")

    my_init_mock = AsyncMock()
    monkeypatch.setattr(npi_module, "my_init_db", my_init_mock)
    monkeypatch.setattr(npi_module, "ensure_database", AsyncMock())

    make_mock = _fake_make_class_factory("testschema")
    monkeypatch.setattr(npi_module, "make_class", make_mock)

    create_mock = AsyncMock()
    status_mock = AsyncMock()
    monkeypatch.setattr(npi_module.db, "create_table", create_mock)
    monkeypatch.setattr(npi_module.db, "status", status_mock)
    staging_reset = AsyncMock()
    monkeypatch.setattr(npi_module, "_prepare_npi_staging", staging_reset)

    startup_context_map: dict[str, object] = {}
    await npi_module.startup(startup_context_map)

    assert startup_context_map["import_date"]
    assert startup_context_map["context"]["run"] == 0
    my_init_mock.assert_awaited_once()
    assert create_mock.await_count >= 1
    assert status_mock.await_count >= 1
    staging_reset.assert_not_awaited()

@pytest.mark.asyncio
async def test_startup_honors_import_id_override(monkeypatch, npi_module):

    monkeypatch.setenv("HLTHPRT_IMPORT_ID_OVERRIDE", "addrcanon_npi_timing")
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "testschema")

    monkeypatch.setattr(npi_module, "my_init_db", AsyncMock())
    monkeypatch.setattr(npi_module, "ensure_database", AsyncMock())
    monkeypatch.setattr(npi_module, "make_class", _fake_make_class_factory("testschema"))
    monkeypatch.setattr(npi_module.db, "create_table", AsyncMock())
    monkeypatch.setattr(npi_module.db, "status", AsyncMock())

    startup_context_map: dict[str, object] = {}
    await npi_module.startup(startup_context_map)

    assert startup_context_map["import_date"] == "addrcanon_npi_timing"

@pytest.mark.asyncio
async def test_publication_transaction_reconciles_only_an_exact_commit(
    monkeypatch,
    npi_module,
):
    receipt = SimpleNamespace(publication_ref="nppub1_" + "d" * 43)
    commit = npi_module.NpiCanonicalPublicationCommit(
        receipt,
        "2026-08-09T02:03:04.000000+00:00",
        "2026-08-09T02:03:04.000000+00:00",
    )
    lease = npi_module._NpiImportLease(
        object(),
        _AmbiguousPublicationConnection(),
        731,
    )
    state_by_name = {
        "commit": commit,
        "progress": {"phase": "npi published"},
        "metrics": {"npi_canonical_publication": {"publication_ref": receipt.publication_ref}},
    }
    reconcile = AsyncMock(return_value=commit)
    monkeypatch.setattr(
        npi_module,
        "_reconcile_npi_commit_after_error",
        reconcile,
    )
    committed_context_by_name = {}

    async with npi_module._npi_publication_transaction(
        lease=lease,
        schema="testschema",
        context=committed_context_by_name,
        publication_state_by_name=state_by_name,
    ):
        state_by_name["first_body_entered"] = True
    assert state_by_name["commit"] == commit
    assert committed_context_by_name["control_run_terminal_committed"] is True
    reconcile.assert_awaited_once()

    reconcile.return_value = None
    with pytest.raises(RuntimeError, match="npi_canonical_publication_invalid"):
        async with npi_module._npi_publication_transaction(
            lease=lease,
            schema="testschema",
            context={},
            publication_state_by_name=state_by_name,
        ):
            state_by_name["second_body_entered"] = True

@pytest.mark.asyncio
async def test_shutdown_handles_rotation(monkeypatch, npi_module):
    """Seal stage census, table rotation, receipt, and terminal state together."""
    stage_count_by_table = {
        f"{table_name}_20251108": ordinal
        for ordinal, table_name in enumerate(npi_module.NPI_CANONICAL_TABLES, 1)
    }
    raw_connection = _ShutdownRawConnection(stage_count_by_table)
    publication_receipt = _install_shutdown_success_collaborators(
        monkeypatch,
        npi_module,
        raw_connection,
    )
    lease = npi_module._NpiImportLease(object(), raw_connection, 731)
    shutdown_context_map = {
        "context": {
            "run": 1,
            "start": datetime.datetime.utcnow(),
            "control_run_id": "npi-run-1",
            "_control_attempt_id": "npi-run-1:" + "c" * 32,
            "_control_attempt_started_at": "2026-08-09T00:00:00.000000+00:00",
            npi_module._NPI_IMPORT_LEASE_KEY: lease,
        },
        "import_date": "20251108",
    }
    shutdown_result_by_name = await npi_module.shutdown(shutdown_context_map)

    receipt_mock = npi_module.insert_npi_publication_receipt
    publication_input = receipt_mock.await_args.kwargs["publication_input"]
    assert publication_input.row_counts == (1, 2, 3, 4, 5, 6)
    assert publication_input.relation_oids == (11, 12, 13, 14, 15, 16)
    npi_module.mark_npi_publication_succeeded.assert_awaited_once()
    npi_module.raise_if_cancelled.assert_awaited()
    first_swap = next(
        index for index, event in enumerate(raw_connection.events)
        if "DROP TABLE IF EXISTS testschema.npi_old" in event
    )
    final_count = max(
        index for index, event in enumerate(raw_connection.events)
        if "count(*)::bigint" in event
    )
    stage_lock = next(
        index for index, event in enumerate(raw_connection.events)
        if event.startswith("LOCK TABLE ")
    )
    projection_validation = next(index for index, event in enumerate(raw_connection.events) if "search_taxonomy_codes" in event and "FULL OUTER JOIN" in event)
    assert stage_lock < projection_validation < final_count < first_swap
    assert raw_connection.events[-1] == "transaction:commit"
    assert shutdown_context_map["context"][npi_module._NPI_CONTROL_TERMINAL_COMMITTED_KEY] is True
    assert shutdown_context_map["context"][
        npi_module._NPI_CONTROL_COMMITTED_FINISHED_AT_KEY
    ] == "2026-08-09T02:03:04.000000+00:00"
    assert shutdown_result_by_name["npi_canonical_publication"][
        "publication_ref"
    ] == publication_receipt.publication_ref
    npi_module._release_npi_import_lease.assert_awaited_once_with(
        shutdown_context_map["context"],
        suppress_errors=True,
    )

@pytest.mark.asyncio
async def test_resolve_npi_address_archive_skips_sql_stamp_when_keys_loaded(monkeypatch, npi_module):
    stamp_address_keys = AsyncMock()
    resolve_into_archive = AsyncMock(return_value=SimpleNamespace(staged=10, distinct_keys=5))

    monkeypatch.setattr(npi_module.db, "scalar", AsyncMock(return_value=0))
    monkeypatch.setattr(npi_module, "stamp_address_keys", stamp_address_keys)
    monkeypatch.setattr(npi_module, "resolve_into_archive", resolve_into_archive)

    stats = await npi_module.resolve_npi_address_archive(
        staging_table="npi_address_20260613",
        field_map={"first_line": "first_line"},
        schema="mrf",
        cancel_check=AsyncMock(),
    )

    assert stats.staged == 10
    stamp_address_keys.assert_not_awaited()
    resolve_into_archive.assert_awaited_once()
