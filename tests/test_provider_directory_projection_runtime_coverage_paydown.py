# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Source-free fail-closed boundaries for the native projection runtime."""

from __future__ import annotations

import asyncio
from dataclasses import replace

import pytest

import process.provider_directory_projection_materializer as materializer
import process.provider_directory_projection_materializer_config as config
import process.provider_directory_projection_native as native
import process.provider_directory_projection_stage as stage_runtime
import process.provider_directory_projection_worker_admission as worker_admission
from process.provider_directory_projection_types import (
    ProjectionStage,
    ProviderDirectoryProjectionError,
)
from tests.provider_directory_projection_materializer_context import (
    synthetic_projection_context,
)
from tests.provider_directory_projection_native_copy_support import stream_reader


@pytest.mark.parametrize("enabled", (0, "yes", object()))
def test_materializer_explicit_enablement_requires_a_boolean(enabled) -> None:
    with pytest.raises(ProviderDirectoryProjectionError, match="enabled_invalid"):
        config.is_projection_materializer_enabled(enabled)


def test_materializer_enablement_is_exact_and_off_by_default(monkeypatch) -> None:
    variable = "HLTHPRT_PROVIDER_DIRECTORY_PROJECTION_NATIVE_ENABLED"
    monkeypatch.delenv(variable, raising=False)
    assert config.is_projection_materializer_enabled() is False
    monkeypatch.setenv(variable, " YES ")
    assert config.is_projection_materializer_enabled() is True
    monkeypatch.setenv(variable, "enabled")
    assert config.is_projection_materializer_enabled() is False


@pytest.mark.parametrize("worker_count", (object(), "many", 0, 5))
def test_materializer_worker_count_is_bounded(worker_count) -> None:
    with pytest.raises(ProviderDirectoryProjectionError, match="worker_count_invalid"):
        config.projection_materializer_worker_count(worker_count)


@pytest.mark.parametrize("timeout", (object(), "long", 29, 3601, True))
def test_native_timeout_is_numeric_and_bounded(timeout) -> None:
    with pytest.raises(ProviderDirectoryProjectionError, match="timeout_invalid"):
        config.projection_native_timeout_seconds(timeout)


@pytest.mark.parametrize(
    "options",
    (
        {"unknown": True},
        {"lease_seconds": True},
        {"lease_seconds": 29},
        {"heartbeat_seconds": True},
        {"heartbeat_seconds": "30"},
        {"heartbeat_seconds": 0},
        {"lease_seconds": 30, "heartbeat_seconds": 16},
    ),
)
def test_materializer_options_fail_closed(options) -> None:
    with pytest.raises(ProviderDirectoryProjectionError):
        config.projection_materializer_settings(options)


def test_materializer_options_normalize_timing_values() -> None:
    settings = config.projection_materializer_settings(
        {"lease_seconds": 60, "heartbeat_seconds": 10}
    )
    assert settings.lease_seconds == 60
    assert settings.heartbeat_seconds == 10.0


@pytest.mark.parametrize(
    ("variable", "value", "message"),
    (
        ("HLTHPRT_DB_POOL_MAX_SIZE", "invalid", "pool_capacity_invalid"),
        ("HLTHPRT_DB_POOL_MAX_SIZE", "1", "pool_capacity_invalid"),
        (
            "HLTHPRT_PROVIDER_DIRECTORY_PROJECTION_GLOBAL_WORKERS",
            "invalid",
            "worker_limit_invalid",
        ),
        (
            "HLTHPRT_PROVIDER_DIRECTORY_PROJECTION_GLOBAL_WORKERS",
            "5",
            "worker_limit_invalid",
        ),
    ),
)
def test_global_worker_admission_rejects_invalid_environment(
    monkeypatch, variable, value, message
) -> None:
    monkeypatch.setenv(variable, value)
    with pytest.raises(ProviderDirectoryProjectionError, match=message):
        worker_admission.projection_global_worker_limit()


def test_global_worker_limit_reserves_one_pool_connection(monkeypatch) -> None:
    monkeypatch.setenv("HLTHPRT_DB_POOL_MAX_SIZE", "3")
    monkeypatch.setenv(
        "HLTHPRT_PROVIDER_DIRECTORY_PROJECTION_GLOBAL_WORKERS", "4"
    )
    assert worker_admission.projection_global_worker_limit() == 2


@pytest.mark.asyncio
async def test_worker_admission_is_loop_local_and_configuration_stable(
    monkeypatch,
) -> None:
    database = type("Database", (), {})()
    monkeypatch.setenv("HLTHPRT_DB_POOL_MAX_SIZE", "5")
    monkeypatch.setenv(
        "HLTHPRT_PROVIDER_DIRECTORY_PROJECTION_GLOBAL_WORKERS", "2"
    )
    admission = worker_admission.projection_worker_admission(database)
    assert worker_admission.projection_worker_admission(database) is admission
    monkeypatch.setenv(
        "HLTHPRT_PROVIDER_DIRECTORY_PROJECTION_GLOBAL_WORKERS", "3"
    )
    with pytest.raises(ProviderDirectoryProjectionError, match="limit_changed"):
        worker_admission.projection_worker_admission(database)


@pytest.mark.asyncio
async def test_worker_admission_requires_mutable_database_state() -> None:
    class SlottedDatabase:
        __slots__ = ()

    with pytest.raises(ProviderDirectoryProjectionError, match="unavailable"):
        worker_admission.projection_worker_admission(SlottedDatabase())


@pytest.mark.parametrize("raw_value", ("bad", "127", "16385"))
def test_copy_batch_size_is_bounded(monkeypatch, raw_value) -> None:
    monkeypatch.setenv(
        "HLTHPRT_PROVIDER_DIRECTORY_PROJECTION_COPY_BATCH_ROWS", raw_value
    )
    with pytest.raises(ProviderDirectoryProjectionError, match="batch_rows_invalid"):
        stage_runtime.projection_copy_batch_size()


def test_stage_binding_is_all_or_nothing() -> None:
    assert stage_runtime._bound_projection_stage({}) is None
    with pytest.raises(ProviderDirectoryProjectionError, match="binding_invalid"):
        stage_runtime._bound_projection_stage({"stage_schema": "mrf"})
    assert stage_runtime._bound_projection_stage(
        {
            "stage_schema": "mrf",
            "stage_relation": "projection_stage",
            "stage_relation_oid": "42",
        }
    ) == ProjectionStage("mrf", "projection_stage", 42)


class _RawConnection:
    def __init__(self, driver) -> None:
        self.driver_connection = driver


class _SQLConnection:
    def __init__(self, raw_connection) -> None:
        self.raw_connection = raw_connection

    async def get_raw_connection(self):
        return self.raw_connection


@pytest.mark.asyncio
async def test_copy_driver_unwraps_sync_and_async_connections() -> None:
    driver = object()
    raw_connection = _RawConnection(driver)
    sql_connection = _SQLConnection(raw_connection)

    sync_transaction = type(
        "SyncTransaction", (), {"connection": lambda self: sql_connection}
    )()
    assert await stage_runtime._copy_driver(sync_transaction) is driver

    async def async_connection(_self):
        return sql_connection

    async_transaction = type(
        "AsyncTransaction", (), {"connection": async_connection}
    )()
    assert await stage_runtime._copy_driver(async_transaction) is driver
    assert await stage_runtime._copy_driver(
        type("RawTransaction", (), {"raw_connection": raw_connection})()
    ) is driver


class _RecordCopyDriver:
    def __init__(self, status) -> None:
        self.status_value = status
        self.options = None

    async def copy_records_to_table(self, relation, **options):
        self.options = (relation, options)
        return self.status_value


@pytest.mark.asyncio
async def test_record_copy_checks_shape_status_and_count() -> None:
    stage = ProjectionStage("mrf", "projection_stage", 1)
    row = (None,) * len(stage_runtime.STAGE_COPY_COLUMNS)
    driver = _RecordCopyDriver(None)
    assert await stage_runtime.copy_projection_stage_records(
        stage, (row,), transaction=driver
    ) == 1
    assert driver.options[0] == stage.relation

    for invalid_rows in ((), ([None] * len(row),), ((None,),)):
        with pytest.raises(ProviderDirectoryProjectionError, match="batch_invalid"):
            await stage_runtime.copy_projection_stage_records(
                stage, invalid_rows, transaction=driver
            )
    for status, message in (("COPY many", "status_invalid"), ("COPY 2", "mismatch")):
        with pytest.raises(ProviderDirectoryProjectionError, match=message):
            await stage_runtime.copy_projection_stage_records(
                stage, (row,), transaction=_RecordCopyDriver(status)
            )


@pytest.mark.asyncio
async def test_copy_operations_require_valid_stage_and_matching_driver() -> None:
    invalid_stage = ProjectionStage("mrf", "projection_stage", 0)
    with pytest.raises(ProviderDirectoryProjectionError, match="stage_invalid"):
        await stage_runtime.copy_projection_stage_records(
            invalid_stage, ((None,) * 18,), transaction=object()
        )
    with pytest.raises(ProviderDirectoryProjectionError, match="driver_missing"):
        await stage_runtime.copy_projection_stage_records(
            ProjectionStage("mrf", "projection_stage", 1),
            ((None,) * 18,),
            transaction=object(),
        )
    with pytest.raises(ProviderDirectoryProjectionError, match="stage_invalid"):
        await stage_runtime.copy_projection_stage_binary_stream(
            invalid_stage, object(), transaction=object()
        )


def test_stage_census_helpers_reject_ambiguous_shapes() -> None:
    with pytest.raises(ProviderDirectoryProjectionError, match="census_invalid"):
        stage_runtime._stage_identity(["only-one"])
    with pytest.raises(ProviderDirectoryProjectionError, match="census_invalid"):
        stage_runtime._stage_resource_count_map({"Organization": True}, ())
    assert stage_runtime._stage_resource_count_map(
        {"Organization": 2}, ("Organization", "Practitioner")
    ) == {"Organization": 2, "Practitioner": 0}


@pytest.mark.parametrize("workers", (0, 5, True, "4"))
def test_native_thread_budget_requires_integer_worker_count(workers) -> None:
    with pytest.raises(ProviderDirectoryProjectionError, match="worker_count_invalid"):
        native.projection_native_thread_count(workers)


@pytest.mark.parametrize(
    ("threads", "total"),
    (("invalid", "8"), (0, "8"), (9, "64"), (2, "1"), (1, "65")),
)
def test_native_thread_budget_rejects_invalid_or_oversubscribed_values(
    monkeypatch, threads, total
) -> None:
    monkeypatch.setenv(
        "HLTHPRT_PROVIDER_DIRECTORY_PROJECTION_NATIVE_TOTAL_THREADS", total
    )
    with pytest.raises(ProviderDirectoryProjectionError, match="threads_invalid"):
        native.projection_native_thread_count(4, threads)


@pytest.mark.parametrize("threads", (0, 9, True, "2"))
def test_native_environment_requires_bounded_integer_threads(threads) -> None:
    with pytest.raises(ProviderDirectoryProjectionError, match="threads_invalid"):
        native.native_subprocess_environment(threads)


def test_native_execution_settings_reject_unknown_or_relative_executable() -> None:
    with pytest.raises(ProviderDirectoryProjectionError, match="options_invalid"):
        native._native_execution_settings(1, {"unknown": True})
    with pytest.raises(ProviderDirectoryProjectionError, match="executable_invalid"):
        native._native_execution_settings(1, {"executable": "relative"})


class _InputPipe:
    def __init__(self, close_error=None) -> None:
        self.payload = bytearray()
        self.closed = False
        self.close_error = close_error

    def write(self, chunk) -> None:
        self.payload.extend(chunk)

    async def drain(self) -> None:
        return None

    def close(self) -> None:
        self.closed = True

    async def wait_closed(self) -> None:
        if self.close_error is not None:
            raise self.close_error


async def _chunks(*chunks):
    for chunk in chunks:
        yield chunk


@pytest.mark.asyncio
async def test_retained_byte_pump_closes_pipe_on_all_failures() -> None:
    for chunks, expected_bytes, message in (
        ((b"",), 0, "chunk_invalid"),
        (("not-bytes",), 1, "chunk_invalid"),
        ((b"ab",), 1, "byte_count_mismatch"),
        ((b"a",), 2, "byte_count_mismatch"),
    ):
        pipe = _InputPipe()
        with pytest.raises(ProviderDirectoryProjectionError, match=message):
            await native._pump_retained_bytes(
                _chunks(*chunks), pipe, expected_bytes
            )
        assert pipe.closed is True
    await native._close_native_stdin(_InputPipe(BrokenPipeError()))
    await native._close_native_stdin(_InputPipe(ConnectionResetError()))


@pytest.mark.asyncio
async def test_stderr_drain_is_bounded_and_marks_overflow() -> None:
    diagnostics, exceeded = await native._drain_stderr(
        stream_reader(b"x" * (native._MAX_STDERR_BYTES + 1))
    )
    assert len(diagnostics) == native._MAX_STDERR_BYTES
    assert exceeded is True


class _Process:
    def __init__(self, returncode=None) -> None:
        self.returncode = returncode
        self.terminated = False
        self.killed = False
        self.wait_count = 0
        self.stdin = _InputPipe()
        self.stdout = stream_reader(b"")
        self.stderr = stream_reader(b"")

    async def wait(self):
        self.wait_count += 1
        return 0 if self.returncode is None else self.returncode

    def terminate(self) -> None:
        self.terminated = True

    def kill(self) -> None:
        self.killed = True


@pytest.mark.asyncio
async def test_process_stop_handles_exit_terminate_and_kill(monkeypatch) -> None:
    exited = _Process(0)
    await native._stop_process(exited)
    assert exited.terminated is False

    running = _Process()
    await native._stop_process(running)
    assert running.terminated is True
    assert running.killed is False

    async def timeout(awaitable, *, timeout):
        assert timeout == 5
        awaitable.close()
        raise asyncio.TimeoutError

    monkeypatch.setattr(native.asyncio, "wait_for", timeout)
    stuck = _Process()
    await native._stop_process(stuck)
    assert stuck.terminated is True
    assert stuck.killed is True


@pytest.mark.asyncio
@pytest.mark.parametrize("returncode,stderr_exceeded", ((1, False), (0, True)))
async def test_process_output_rejects_failure_or_stderr_overflow(
    returncode, stderr_exceeded
) -> None:
    process = _Process(returncode)
    stderr_task = asyncio.create_task(
        asyncio.sleep(0, result=("diagnostic", stderr_exceeded))
    )
    with pytest.raises(ProviderDirectoryProjectionError, match="native_failed"):
        await native._validated_process_output(process, stderr_task)


def test_native_input_contract_rejects_framing_lineage_and_recipe() -> None:
    context = synthetic_projection_context("ndjson")
    with pytest.raises(ProviderDirectoryProjectionError, match="input_invalid"):
        native._validate_native_copy_input(
            context.claim, context.child_lease, "xml"
        )
    other = synthetic_projection_context("ndjson", label="other")
    with pytest.raises(ProviderDirectoryProjectionError, match="input_invalid"):
        native._validate_native_copy_input(
            context.claim, other.child_lease, "ndjson"
        )
    invalid_recipe = replace(context.claim.recipe_lease.recipe, decoder_contract_id="bad")
    with pytest.raises(ProviderDirectoryProjectionError, match="input_invalid"):
        native.validate_native_projection_recipe(invalid_recipe)


@pytest.mark.asyncio
async def test_launch_rejects_missing_subprocess_pipes(monkeypatch) -> None:
    context = synthetic_projection_context("ndjson")
    process = _Process()
    process.stdin = None

    async def launch(*_args, **_kwargs):
        return process

    monkeypatch.setattr(native.asyncio, "create_subprocess_exec", launch)
    settings = native._NativeExecutionSettings(1, "/synthetic-runner")
    with pytest.raises(ProviderDirectoryProjectionError, match="pipe_missing"):
        await native._launch_native_copy_process(
            context.claim, "ndjson", settings
        )
    assert process.terminated is True


@pytest.mark.asyncio
async def test_native_result_wait_times_out_and_detects_heartbeat_exit() -> None:
    runner = asyncio.create_task(asyncio.Event().wait())
    heartbeat = asyncio.create_task(asyncio.Event().wait())
    with pytest.raises(ProviderDirectoryProjectionError, match="native_timeout"):
        await materializer._await_native_result(runner, heartbeat, 0)
    heartbeat.cancel()
    await asyncio.gather(heartbeat, return_exceptions=True)

    runner = asyncio.create_task(asyncio.Event().wait())
    heartbeat = asyncio.create_task(asyncio.sleep(0))
    with pytest.raises(ProviderDirectoryProjectionError, match="heartbeat_stopped"):
        await materializer._await_native_result(runner, heartbeat, 1)


@pytest.mark.asyncio
async def test_materializer_runtime_remains_off_by_default() -> None:
    context = synthetic_projection_context("ndjson")
    settings = config.projection_materializer_settings({"enabled": False})
    with pytest.raises(ProviderDirectoryProjectionError, match="native_disabled"):
        await materializer._materializer_runtime(
            context.claim.recipe_lease,
            object(),
            object(),
            object(),
            object(),
            "mrf",
        settings,
    )
