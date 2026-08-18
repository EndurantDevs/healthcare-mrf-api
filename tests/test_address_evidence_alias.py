# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Evidence-specific reviewed address alias contracts."""

import asyncio
import importlib
import io
import json
import sys
from contextlib import ExitStack, asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from db.models import AddressAliasCandidateV1
from process import address_evidence_alias_native as native_alias
from process import address_evidence_alias_process as native_process
from process.address_numeric_grid_alias_support import NumericGridAliasRequest
from process.ext import address_alias_sql, address_evidence_alias_sql
from tests.test_address_numeric_grid_alias import _Recorder, _load_evidence_migration

alias_workflow = importlib.import_module("process.address_numeric_grid_alias")
alias_execution = importlib.import_module("process.address_numeric_grid_alias_execution")
alias_store = importlib.import_module("process.address_numeric_grid_alias_store")


@pytest.mark.asyncio
async def test_failure_mark_survives_a_second_task_cancellation(monkeypatch):
    runner = alias_workflow._NumericGridAliasRunner(
        NumericGridAliasRequest(
            mode="shadow",
            alias_kind=address_alias_sql.EVIDENCE_ADDRESS_MATCH_ALIAS_KIND,
        )
    )
    monkeypatch.setattr(runner, "_insert_execution_run", AsyncMock())
    monkeypatch.setattr(
        runner,
        "_execute_transaction",
        AsyncMock(side_effect=RuntimeError("synthetic failure")),
    )
    marker_started = asyncio.Event()
    marker_release = asyncio.Event()
    marker_completed = asyncio.Event()

    async def delayed_marker(*_args, **_kwargs):
        marker_started.set()
        await marker_release.wait()
        marker_completed.set()

    monkeypatch.setattr(alias_workflow, "_mark_failed", delayed_marker)
    task = asyncio.create_task(runner.execute())
    await marker_started.wait()
    task.cancel()
    await asyncio.sleep(0)
    assert not task.done()
    marker_release.set()

    with pytest.raises(asyncio.CancelledError):
        await task
    assert marker_completed.is_set()


def test_evidence_alias_sql_requires_visible_same_npi_exact_matches():
    sql = address_evidence_alias_sql.evidence_candidate_insert_sql(
        schema="mrf",
        archive='"mrf"."address_archive_v2"',
    )
    normalized = " ".join(sql.split()).lower()

    assert '"mrf"."entity_address_unified"' in sql
    assert "public_evidence_npi_valid" in sql
    assert "target.npi = source.npi" in sql
    assert "target.state_code = source.state_code" in sql
    assert "target.zip5 = source.zip5" in sql
    assert "target.country_code = source.country_code" in sql
    assert "count(distinct target_address_key)" in normalized
    assert "global_related_targets" in normalized
    assert "join \"mrf\".\"address_archive_v2\" as target" in normalized
    assert "target_strict_source_count < 2" in normalized
    assert "match_classification" in sql
    assert "'exact'" in sql
    assert "premise_only" not in normalized
    assert "similarity(" not in normalized
    assert "levenshtein" not in normalized
    fence_sql = address_evidence_alias_sql.evidence_input_stale_count_sql(
        schema="mrf"
    )
    assert "base_address_version" in fence_sql
    assert "alias-v1:g" in fence_sql


def test_evidence_migration_adds_auditable_exact_match_contract(monkeypatch):
    migration = _load_evidence_migration()
    recorder = _Recorder()
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "alias_contract")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    monkeypatch.setattr(migration, "op", recorder)

    migration.upgrade()
    normalized = " ".join(" ".join(sql.split()) for sql in recorder.statements)

    assert migration.revision == "20260816020000_address_evidence_alias"
    assert migration.down_revision == (
        "20260816010000_provider_directory_terminal_publication_guard"
    )
    assert "evidence_gated_address_match_v1" in normalized
    assert "match_rule varchar(64)" in normalized
    assert "match_classification varchar(16)" in normalized
    assert "evidence_npi bigint" in normalized
    assert "evidence_npi_count integer" in normalized
    assert "schema_version = 2" in normalized
    assert "generation = generation + 1" in normalized
    assert "num_nonnulls" in normalized
    assert "public_evidence_npi_valid(evidence_npi::text)" in normalized
    assert "match_classification = 'exact'" in normalized
    assert "addr_evidence_alias_match_v1" in normalized
    assert "candidate_confirmed_bare_unit" in normalized
    assert "unit_designator_punctuation" in normalized
    assert "candidate_confirmed_spaced_unit" in normalized
    assert "formatted_address_omits_descriptor" not in normalized
    assert "direction_relocation" in normalized
    assert "terminal_suffix_omission" in normalized
    assert "premise_only" not in normalized


def test_evidence_candidate_model_exposes_migrated_audit_columns():
    assert {
        "match_rule",
        "match_classification",
        "evidence_npi",
        "evidence_npi_count",
    } <= set(AddressAliasCandidateV1.__table__.columns.keys())


def test_native_evidence_settings_and_summary_are_fail_closed(monkeypatch, tmp_path):
    monkeypatch.setenv(native_alias.ADDRESS_EVIDENCE_ALIAS_NATIVE_THREADS_ENV, "8")
    assert native_alias._native_threads() == 8
    monkeypatch.setenv(native_alias.ADDRESS_EVIDENCE_ALIAS_NATIVE_THREADS_ENV, "9")
    with pytest.raises(RuntimeError, match="thread count"):
        native_alias._native_threads()

    output_path = tmp_path / "candidates.copy"
    output_path.write_bytes(b"candidate\n")
    summary_path = tmp_path / "summary.json"
    with output_path.open("rb") as output_file:
        output_sha256 = native_alias._sha256_file(output_file)
    summary_map = {
        "contract": "address_evidence_alias_native_v1",
        **{
            field: 0
            for field in (
                "archive_rows",
                "membership_rows",
                "visible_memberships",
                "source_count",
                "active_skipped",
                "pair_count",
                "pair_match_count",
                "global_pair_count",
                "candidate_rows",
                "elapsed_ms",
            )
        },
        "output_sha256": output_sha256,
    }
    summary_path.write_text(json.dumps(summary_map), encoding="utf-8")
    with summary_path.open("rb") as summary_file:
        assert native_alias._validated_summary_file(summary_file) == summary_map
    invalid_summaries = (
        {**summary_map, "contract": "stale"},
        {**summary_map, "archive_rows": -1},
        {**summary_map, "output_sha256": "short"},
    )
    for invalid_summary in invalid_summaries:
        with pytest.raises(RuntimeError):
            native_alias._validated_summary_file(
                io.BytesIO(json.dumps(invalid_summary).encode())
            )
    assert native_alias._copied_count(None) is None


@pytest.mark.asyncio
async def test_native_evidence_driver_requires_both_copy_methods():
    raw_connection = SimpleNamespace(driver_connection=object())
    connection = SimpleNamespace(
        get_raw_connection=AsyncMock(return_value=raw_connection)
    )
    session = SimpleNamespace(connection=AsyncMock(return_value=connection))
    assert await native_alias._driver(session) is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("digest_matches", "copy_status", "error_message"),
    (
        (False, "COPY 1", "digest differs"),
        (True, "COPY 0", "COPY count differs"),
    ),
)
async def test_native_evidence_copy_receipt_must_match(
    monkeypatch,
    digest_matches,
    copy_status,
    error_message,
):
    with ExitStack() as native_stack:
        native_files = tuple(
            native_stack.enter_context(native_alias.tempfile.TemporaryFile())
            for _ in range(6)
        )
        native_files[-2].write(b"candidate\n")
        digest = native_alias._sha256_file(native_files[-2])
        summary_by_field = {
            "output_sha256": digest if digest_matches else "0" * 64,
            "candidate_rows": 1,
        }
        monkeypatch.setattr(native_alias, "_run_scanner", AsyncMock())
        monkeypatch.setattr(
            native_alias, "_validated_summary_file", lambda _file: summary_by_field
        )
        driver = SimpleNamespace(copy_to_table=AsyncMock(return_value=copy_status))
        with pytest.raises(RuntimeError, match=error_message):
            await native_alias._copy_native_shadow_candidates(
                driver,
                Path("/scanner"),
                "mrf",
                tuple(Path(f"/proc/self/fd/{index}") for index in range(6)),
                native_files,
            )


@pytest.mark.asyncio
async def test_native_evidence_unavailable_transports_fall_back(monkeypatch, tmp_path):
    scope_by_field = {
        "schema": "mrf",
        "archive": '"mrf"."address_archive_v2"',
        "run_id": "00000000-0000-0000-0000-000000000001",
        "state_code": None,
        "zip_prefix": None,
        "retry_shadow_run_id": None,
    }
    monkeypatch.setattr(native_alias, "_is_native_enabled", lambda: True)
    monkeypatch.setattr(native_alias, "_ptg2_rust_scanner_binary", lambda: None)
    assert await native_alias.try_native_evidence_shadow(object(), **scope_by_field) is None

    monkeypatch.setattr(native_alias, "_ptg2_rust_scanner_binary", lambda: tmp_path)
    monkeypatch.setattr(native_alias, "_is_native_version_current", AsyncMock(return_value=True))
    monkeypatch.setattr(native_alias, "_driver", AsyncMock(return_value=None))
    assert await native_alias.try_native_evidence_shadow(object(), **scope_by_field) is None

    monkeypatch.setattr(native_alias, "_driver", AsyncMock(return_value=object()))
    monkeypatch.setattr(native_alias, "_native_scratch_directory", lambda: tmp_path)
    monkeypatch.setattr(native_alias, "_native_descriptor_root", lambda: None)
    assert await native_alias.try_native_evidence_shadow(object(), **scope_by_field) is None


@pytest.mark.asyncio
async def test_alias_runner_does_not_mark_failures_before_run_creation(monkeypatch):
    mark_failed = AsyncMock()
    monkeypatch.setattr(alias_execution, "_mark_failed_cancellation_resistant", mark_failed)
    for error in (RuntimeError("synthetic failure"), asyncio.CancelledError()):
        runner = alias_workflow._NumericGridAliasRunner(
            NumericGridAliasRequest(mode="shadow")
        )
        monkeypatch.setattr(
            runner,
            "_prepare_and_execute",
            AsyncMock(side_effect=error),
        )
        with pytest.raises(type(error)):
            await runner.execute()
    mark_failed.assert_not_awaited()


@pytest.mark.asyncio
async def test_alias_runner_off_delegates_without_creating_a_run(monkeypatch):
    runner = alias_workflow._NumericGridAliasRunner(NumericGridAliasRequest(mode="off"))
    off_result = object()
    monkeypatch.setattr(runner, "_off_result", AsyncMock(return_value=off_result))

    assert await runner.execute() is off_result


@pytest.mark.asyncio
async def test_failure_marker_rejects_an_elapsed_lifecycle_deadline():
    with pytest.raises(TimeoutError, match="deadline elapsed"):
        await alias_store._mark_failed(
            "mrf",
            "00000000-0000-0000-0000-000000000001",
            RuntimeError("synthetic failure"),
            deadline_monotonic=asyncio.get_running_loop().time(),
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("policy_by_field", "error_message"),
    (
        ({"alias_kind": "unsupported"}, "alias kind differs"),
        (
            {
                "alias_kind": address_alias_sql.NUMERIC_GRID_ALIAS_KIND,
                "ruleset_version": 2,
            },
            "ruleset differs",
        ),
    ),
)
async def test_reviewed_shadow_policy_must_match(
    monkeypatch,
    policy_by_field,
    error_message,
):
    shadow_record = SimpleNamespace(
        _mapping={"status": "sealed", "candidate_digest": "a" * 64, **policy_by_field}
    )
    monkeypatch.setattr(alias_store.db, "first", AsyncMock(return_value=shadow_record))
    with pytest.raises(ValueError, match=error_message):
        await alias_store._load_reviewed_shadow(
            schema="mrf",
            shadow_run_id="00000000-0000-0000-0000-000000000001",
            expected_digest="a" * 64,
        )


@pytest.mark.asyncio
async def test_native_evidence_cancellation_reaps_the_child(monkeypatch):
    process_control = native_process._ScannerProcessControl()
    monkeypatch.setattr(native_process, "_ScannerProcessControl", lambda: process_control)
    scanner_task = asyncio.create_task(
        native_alias._run_native_process(
            Path(sys.executable),
            ("-c", "import time; time.sleep(60)"),
            "synthetic scanner",
        )
    )
    try:
        for _ in range(100):
            if process_control._process is not None:
                break
            await asyncio.sleep(0.001)
        process = process_control._process
        assert process is not None
        scanner_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await scanner_task
        assert process.poll() is not None
    finally:
        process = process_control._process
        if process is not None and process.poll() is None:
            process.kill()
            process.wait()


def _install_short_evidence_deadlines(monkeypatch) -> None:
    monkeypatch.setattr(
        alias_execution,
        "_EVIDENCE_ALIAS_LIFECYCLE_TIMEOUT_SECONDS",
        0.18,
    )
    monkeypatch.setattr(
        alias_execution,
        "_EVIDENCE_ALIAS_CLEANUP_TIMEOUT_SECONDS",
        0.06,
    )
    monkeypatch.setattr(
        alias_execution,
        "_EVIDENCE_ALIAS_TERMINAL_TIMEOUT_SECONDS",
        0.06,
    )


def _install_blocked_anonymous_native(monkeypatch, tmp_path):
    opened_streams = []
    temporary_file = native_alias.tempfile.TemporaryFile

    def tracked_file(**options):
        opened_stream = temporary_file(**options)
        opened_streams.append(opened_stream)
        return opened_stream

    monkeypatch.setattr(native_alias.tempfile, "TemporaryFile", tracked_file)
    monkeypatch.setattr(native_alias, "_native_scratch_directory", lambda: tmp_path)
    monkeypatch.setattr(native_alias, "_native_descriptor_root", lambda: tmp_path)
    monkeypatch.setattr(native_alias, "_ptg2_rust_scanner_binary", lambda: tmp_path)
    monkeypatch.setattr(
        native_alias,
        "_is_native_version_current",
        AsyncMock(return_value=True),
    )
    monkeypatch.setattr(native_alias, "_driver", AsyncMock(return_value=object()))
    monkeypatch.setattr(
        native_alias,
        "_export_native_shadow_files",
        AsyncMock(),
    )

    async def blocked_candidate_copy(*_args, **_options):
        await asyncio.Event().wait()

    monkeypatch.setattr(
        native_alias,
        "_copy_native_shadow_candidates",
        blocked_candidate_copy,
    )
    return opened_streams


async def _execute_blocked_native(runner, session) -> None:
    await native_alias.try_native_evidence_shadow(
        session,
        schema="mrf",
        archive='"mrf"."address_archive_v2"',
        run_id=runner._required_execution().run_id,
        state_code=None,
        zip_prefix=None,
        retry_shadow_run_id=None,
        cleanup_deadline_monotonic=runner._cleanup_deadline_monotonic,
    )


@pytest.mark.asyncio
async def test_native_timeout_closes_files_before_terminal_failure(
    monkeypatch,
    tmp_path,
):
    _install_short_evidence_deadlines(monkeypatch)
    opened_streams = _install_blocked_anonymous_native(monkeypatch, tmp_path)
    session_closed = asyncio.Event()

    @asynccontextmanager
    async def transaction():
        try:
            yield object()
        finally:
            session_closed.set()

    monkeypatch.setattr(alias_workflow.db, "transaction", transaction)
    runner = alias_workflow._NumericGridAliasRunner(
        NumericGridAliasRequest(
            mode="shadow",
            alias_kind=address_alias_sql.EVIDENCE_ADDRESS_MATCH_ALIAS_KIND,
        )
    )
    monkeypatch.setattr(runner, "_insert_execution_run", AsyncMock())
    monkeypatch.setattr(
        runner,
        "_execute_locked",
        lambda session: _execute_blocked_native(runner, session),
    )
    terminal_state_by_field = {}

    async def mark_failed(*_args, **_kwargs):
        assert session_closed.is_set()
        assert opened_streams and all(stream.closed for stream in opened_streams)
        terminal_state_by_field["marked_at"] = asyncio.get_running_loop().time()
        terminal_state_by_field["status"] = "failed"
        return terminal_state_by_field["status"]

    monkeypatch.setattr(alias_workflow, "_mark_failed", mark_failed)

    with pytest.raises(TimeoutError):
        await runner.execute()

    assert terminal_state_by_field["status"] == "failed"
    assert runner._lifecycle_deadline_monotonic is not None
    assert terminal_state_by_field["marked_at"] <= runner._lifecycle_deadline_monotonic


@pytest.mark.asyncio
async def test_native_evidence_version_check_is_exact_and_uncached(monkeypatch):
    version_process = AsyncMock(
        side_effect=(
            b'{"current": true}',
            native_alias.ADDRESS_EVIDENCE_ALIAS_NATIVE_CONTRACT.encode(),
            b'{"current": true}',
            b"stale",
        )
    )
    monkeypatch.setattr(native_alias, "_is_canon_version_match", lambda _payload: True)
    monkeypatch.setattr(native_alias, "_run_native_process", version_process)
    binary = Path("/synthetic/scanner")

    assert await native_alias._is_native_version_current(binary)
    assert not await native_alias._is_native_version_current(binary)
    assert version_process.await_count == 4


@pytest.mark.asyncio
async def test_native_evidence_version_cleanup_timeout_does_not_fallback(monkeypatch):
    monkeypatch.setattr(
        native_alias,
        "_run_native_process",
        AsyncMock(side_effect=TimeoutError("cleanup deadline")),
    )
    with pytest.raises(TimeoutError, match="cleanup deadline"):
        await native_alias._is_native_version_current(Path("/synthetic/scanner"))
