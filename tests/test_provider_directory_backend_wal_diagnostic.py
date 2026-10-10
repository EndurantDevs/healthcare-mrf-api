# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Command/custody orchestration only; actual PostgreSQL acceptance is separate."""

import asyncio
from dataclasses import FrozenInstanceError
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import asyncpg
import pytest

from process import provider_directory_backend_wal_diagnostic as diagnostic

_STAMP = datetime(2026, 1, 1, tzinfo=timezone.utc)


def _preflight(**changes):
    return {
        "pid": 4321,
        "server_version": 180002,
        "parallel_workers": 0,
        "parallel_gather": 0,
        "parallel_maintenance": 0,
        "debug_parallel": "off",
        "access_allowed": True,
        **changes,
    }


def _sample(**changes):
    return {
        **_preflight(),
        "backend_start": _STAMP,
        "database_oid": 123,
        "database_name": "synthetic_database",
        "system_identifier": "123456789",
        "leader_pid": None,
        "stats_reset": _STAMP,
        "wal_records": 2,
        "wal_fpi": 1,
        "wal_bytes": Decimal("128"),
        "wal_buffers_full": 0,
        "insert_lsn": "0/100",
        **changes,
    }


class BoundConnection:
    """An isolated raw-driver seam, with no transaction/lifecycle capabilities."""

    def __init__(self, *, samples=None, preflight=None):
        self.samples = list(
            samples
            or [
                _sample(),
                _sample(wal_records=5, wal_fpi=2, wal_bytes=Decimal("256"), wal_buffers_full=1, insert_lsn="0/200"),
            ]
        )
        self.preflight = _preflight() if preflight is None else preflight
        self.commands = []
        self.closed = False
        self.in_transaction = False
        self.pid = 4321
        self.fail_command = None
        self.error = None
        self.pause_command = None
        self.paused = asyncio.Event()
        self.release = asyncio.Event()
        self.enter_transaction_on = None

    def is_closed(self):
        return self.closed

    def is_in_transaction(self):
        return self.in_transaction

    def get_server_pid(self):
        return self.pid

    async def _command(self, name):
        self.commands.append(name)
        if self.fail_command == name:
            raise self.error
        if self.pause_command == name:
            self.paused.set()
            await self.release.wait()
        if self.enter_transaction_on == name:
            self.in_transaction = True

    async def fetchrow(self, sql, *parameters):
        if sql == diagnostic._PREFLIGHT_SQL:
            assert parameters == ()
            await self._command("preflight")
            return self.preflight
        if sql == diagnostic._OWNER_BOUNDARY_SAMPLE_SQL:
            assert parameters == (self.pid,)
            await self._command("owner_sample")
            return _sample(pid=self.pid)
        assert sql == diagnostic._SAMPLE_SQL and parameters == (self.pid,)
        await self._command("sample")
        return self.samples.pop(0)

    async def execute(self, sql):
        assert sql in {diagnostic._FORCE_SQL, diagnostic._CLEAR_SQL}
        await self._command("force" if sql == diagnostic._FORCE_SQL else "clear")
        return "SELECT 1"

    def __getattr__(self, name):
        if name in {"commit", "rollback", "close", "transaction", "acquire", "reset"}:
            pytest.fail(f"The diagnostic must not call owner capability {name}")
        raise AttributeError(name)


@pytest.mark.asyncio
async def test_separate_flush_snapshot_read_commands_and_raw_values():
    connection = BoundConnection()
    observation = await diagnostic.begin_backend_wal_diagnostic(connection)
    assert connection.commands == ["preflight", "clear", "force", "sample"]
    before = observation.baseline
    with pytest.raises(FrozenInstanceError):
        before.identity.pid = 1
    measured = await diagnostic.finish_backend_wal_diagnostic(observation)
    assert connection.commands == ["preflight", "clear", "force", "sample"] * 2
    assert measured.baseline is before and measured.final.identity == before.identity
    assert measured.wal_records_delta == 3 and measured.wal_fpi_delta == 1
    assert measured.wal_bytes_delta == Decimal("128") and measured.wal_buffers_full_delta == 1
    assert measured.global_insert_lsn_span == 256
    assert measured.global_insert_lsn_span != measured.wal_bytes_delta
    with pytest.raises(diagnostic.BackendWalDiagnosticUnavailable):
        await diagnostic.finish_backend_wal_diagnostic(observation)
    assert len(connection.commands) == 8


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["outer_transaction", "savepoint", "closed"])
async def test_active_or_closed_connection_refuses_without_commands(state):
    connection = BoundConnection()
    connection.in_transaction = state in {"outer_transaction", "savepoint"}
    connection.closed = state == "closed"
    with pytest.raises(diagnostic.BackendWalDiagnosticUnavailable):
        await diagnostic.begin_backend_wal_diagnostic(connection)
    assert connection.commands == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changes",
    [
        {"server_version": 170006},
        {"server_version": 190000},
        {"server_version": True},
        {"parallel_workers": 1},
        {"parallel_gather": 1},
        {"parallel_maintenance": 1},
        {"debug_parallel": "on"},
        {"access_allowed": False},
        {"access_allowed": None},
    ],
)
async def test_preflight_refusals_precede_force(changes):
    connection = BoundConnection(preflight=_preflight(**changes))
    with pytest.raises(diagnostic.BackendWalDiagnosticUnavailable):
        await diagnostic.begin_backend_wal_diagnostic(connection)
    assert connection.commands == ["preflight"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changes",
    [
        {"pid": 99},
        {"backend_start": _STAMP + timedelta(seconds=1)},
        {"database_oid": 124},
        {"database_name": "other_database"},
        {"system_identifier": "987654321"},
        {"stats_reset": _STAMP + timedelta(seconds=1)},
        {"wal_records": 1},
        {"wal_fpi": 0},
        {"wal_bytes": Decimal("127")},
        {"insert_lsn": "0/FF"},
        {"leader_pid": 123},
        {"parallel_gather": 1},
    ],
)
async def test_final_identity_reset_decrease_or_parallel_refuses(changes):
    connection = BoundConnection(samples=[_sample(), _sample(**changes)])
    observation = await diagnostic.begin_backend_wal_diagnostic(connection)
    with pytest.raises(diagnostic.BackendWalDiagnosticUnavailable):
        await diagnostic.finish_backend_wal_diagnostic(observation)
    with pytest.raises(diagnostic.BackendWalDiagnosticUnavailable):
        await diagnostic.finish_backend_wal_diagnostic(observation)
    assert len(connection.commands) == 8


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changes",
    [
        {"wal_records": None},
        {"wal_records": True},
        {"wal_fpi": -1},
        {"wal_buffers_full": None},
        {"wal_bytes": None},
        {"wal_bytes": 1.5},
        {"wal_bytes": Decimal("1.5")},
        {"wal_bytes": Decimal("NaN")},
        {"wal_bytes": Decimal("-1")},
        {"backend_start": _STAMP.replace(tzinfo=None)},
        {"database_oid": None},
        {"database_name": ""},
        {"system_identifier": "01"},
        {"insert_lsn": "0/001"},
        {"insert_lsn": "0/ff"},
    ],
)
async def test_missing_or_lossy_native_values_refuse(changes):
    connection = BoundConnection(samples=[_sample(**changes)])
    with pytest.raises(diagnostic.BackendWalDiagnosticUnavailable):
        await diagnostic.begin_backend_wal_diagnostic(connection)


@pytest.mark.asyncio
async def test_missing_backend_row_refuses():
    connection = BoundConnection()
    connection.samples = [None]
    with pytest.raises(diagnostic.BackendWalDiagnosticUnavailable):
        await diagnostic.begin_backend_wal_diagnostic(connection)


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["preflight", "clear", "force", "sample"])
async def test_accessor_denial_is_value_free_and_final_consumed(phase):
    connection = BoundConnection()
    observation = await diagnostic.begin_backend_wal_diagnostic(connection)
    connection.fail_command = phase
    connection.error = asyncpg.InsufficientPrivilegeError("synthetic denied accessor")
    with pytest.raises(diagnostic.BackendWalDiagnosticUnavailable) as refusal:
        await diagnostic.finish_backend_wal_diagnostic(observation)
    assert str(refusal.value) == "provider_directory_backend_wal_diagnostic_unavailable"
    before_retry_commands = list(connection.commands)
    with pytest.raises(diagnostic.BackendWalDiagnosticUnavailable):
        await diagnostic.finish_backend_wal_diagnostic(observation)
    assert connection.commands == before_retry_commands


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["pid", "closed", "transaction"])
async def test_final_driver_change_refuses_before_sql(change):
    connection = BoundConnection()
    observation = await diagnostic.begin_backend_wal_diagnostic(connection)
    if change == "pid":
        connection.pid += 1
    elif change == "closed":
        connection.closed = True
    else:
        connection.in_transaction = True
    with pytest.raises(diagnostic.BackendWalDiagnosticUnavailable):
        await diagnostic.finish_backend_wal_diagnostic(observation)
    assert connection.commands == ["preflight", "clear", "force", "sample"]


@pytest.mark.asyncio
async def test_new_transaction_between_commands_is_not_committed():
    connection = BoundConnection()
    connection.enter_transaction_on = "force"
    with pytest.raises(diagnostic.BackendWalDiagnosticUnavailable):
        await diagnostic.begin_backend_wal_diagnostic(connection)
    assert connection.in_transaction
    assert connection.commands == ["preflight", "clear", "force"]


@pytest.mark.asyncio
async def test_other_task_cannot_consume_retained_ticket():
    connection = BoundConnection()
    observation = await diagnostic.begin_backend_wal_diagnostic(connection)
    with pytest.raises(diagnostic.BackendWalDiagnosticUnavailable):
        await asyncio.create_task(diagnostic.finish_backend_wal_diagnostic(observation))
    assert len(connection.commands) == 4
    assert (await diagnostic.finish_backend_wal_diagnostic(observation)).wal_records_delta == 3


@pytest.mark.asyncio
async def test_cancelled_final_is_consumed_without_refund_or_owner_cleanup():
    connection = BoundConnection()
    observations = []

    async def worker():
        observation = await diagnostic.begin_backend_wal_diagnostic(connection)
        observations.append(observation)
        connection.pause_command = "force"
        await diagnostic.finish_backend_wal_diagnostic(observation)

    worker_task = asyncio.create_task(worker())
    await asyncio.wait_for(connection.paused.wait(), 1)
    worker_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await worker_task
    assert observations[0]._consumed
    assert not connection.closed and not connection.in_transaction
    assert connection.commands == ["preflight", "clear", "force", "sample", "preflight", "clear", "force"]


@pytest.mark.asyncio
async def test_connection_loss_after_commit_has_no_success_result():
    connection = BoundConnection()
    observation = await diagnostic.begin_backend_wal_diagnostic(connection)
    connection.fail_command = "sample"
    connection.error = ConnectionResetError("synthetic connection lost")
    with pytest.raises(diagnostic.BackendWalDiagnosticUnavailable):
        await diagnostic.finish_backend_wal_diagnostic(observation)
    assert observation._consumed


def test_invalid_owner_wrapper_has_no_connection_fallback():
    with pytest.raises(diagnostic.BackendWalDiagnosticUnavailable):
        diagnostic._outside_transaction(object())


@pytest.mark.asyncio
async def test_zero_backend_bytes_preserve_global_span():
    connection = BoundConnection(samples=[_sample(), _sample(insert_lsn="0/200")])
    observation = await diagnostic.begin_backend_wal_diagnostic(connection)
    measured = await diagnostic.finish_backend_wal_diagnostic(observation)
    assert measured.wal_bytes_delta == 0 and measured.wal_records_delta == 0
    assert measured.global_insert_lsn_span == 256


@pytest.mark.asyncio
async def test_buffer_counter_decrease_refuses():
    connection = BoundConnection(samples=[_sample(wal_buffers_full=2), _sample(wal_buffers_full=1)])
    observation = await diagnostic.begin_backend_wal_diagnostic(connection)
    with pytest.raises(diagnostic.BackendWalDiagnosticUnavailable):
        await diagnostic.finish_backend_wal_diagnostic(observation)


@pytest.mark.asyncio
async def test_error_reading_driver_pid_is_value_free():
    connection = BoundConnection()

    def lost_pid():
        raise asyncpg.InterfaceError("synthetic driver is no longer connected")

    connection.get_server_pid = lost_pid
    with pytest.raises(diagnostic.BackendWalDiagnosticUnavailable) as refusal:
        await diagnostic.begin_backend_wal_diagnostic(connection)
    assert str(refusal.value) == "provider_directory_backend_wal_diagnostic_unavailable"
    assert connection.commands == []


@pytest.mark.asyncio
async def test_fresh_backend_preserves_native_null_reset_timestamp():
    connection = BoundConnection(samples=[_sample(stats_reset=None), _sample(stats_reset=None)])
    observation = await diagnostic.begin_backend_wal_diagnostic(connection)
    measured = await diagnostic.finish_backend_wal_diagnostic(observation)
    assert measured.baseline.identity.stats_reset is None
    assert measured.final.identity.stats_reset is None


@pytest.mark.asyncio
@pytest.mark.parametrize("resets", [(None, _STAMP), (_STAMP, None)])
async def test_every_native_reset_state_transition_refuses(resets):
    connection = BoundConnection(samples=[_sample(stats_reset=resets[0]), _sample(stats_reset=resets[1])])
    observation = await diagnostic.begin_backend_wal_diagnostic(connection)
    with pytest.raises(diagnostic.BackendWalDiagnosticUnavailable):
        await diagnostic.finish_backend_wal_diagnostic(observation)


@pytest.mark.asyncio
async def test_missing_reset_column_is_not_native_null():
    sample_by_field = _sample()
    del sample_by_field["stats_reset"]
    connection = BoundConnection(samples=[sample_by_field])
    with pytest.raises(diagnostic.BackendWalDiagnosticUnavailable):
        await diagnostic.begin_backend_wal_diagnostic(connection)


@pytest.mark.asyncio
@pytest.mark.parametrize("sample_family", ["owner", "body"])
async def test_sample_forces_clear_command_work_before_reading_counters(sample_family):
    class ClearWorkConnection(BoundConnection):
        def __init__(self):
            super().__init__()
            self.pending_records = 0
            self.published_records = 0

        async def execute(self, sql):
            result = await super().execute(sql)
            if sql == diagnostic._CLEAR_SQL:
                self.pending_records += 17
            else:
                self.published_records = self.pending_records
            return result

        async def fetchrow(self, sql, *parameters):
            row = await super().fetchrow(sql, *parameters)
            if sql == diagnostic._PREFLIGHT_SQL:
                return row
            assert sql in {diagnostic._OWNER_BOUNDARY_SAMPLE_SQL, diagnostic._SAMPLE_SQL}
            return {**row, "wal_records": self.published_records}

    connection = ClearWorkConnection()
    if sample_family == "owner":
        sample = await diagnostic.sample_backend_wal_owner_boundary(connection, connection.pid)
        expected_commands = ["clear", "force", "owner_sample"]
    else:
        observation = await diagnostic.begin_backend_wal_diagnostic(connection)
        sample = observation.baseline
        expected_commands = ["preflight", "clear", "force", "sample"]
    assert sample.wal_records == 17
    assert connection.commands == expected_commands


def test_owner_boundary_native_cast_and_operator_bindings_survive_restored_path():
    # Native adversarial domain/operator execution remains a separate fixture gate.
    sql = diagnostic._OWNER_BOUNDARY_SAMPLE_SQL
    assert "::text" not in sql
    assert sql.count("::pg_catalog.text") == 2
    assert "activity.pid=$1" not in sql
    assert "activity.pid=pg_catalog.pg_backend_pid()" not in sql
    assert "activity.pid OPERATOR(pg_catalog.=) $1" in sql
    assert "activity.pid OPERATOR(pg_catalog.=) pg_catalog.pg_backend_pid()" in sql


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["clear", "force", "owner_sample"])
async def test_owner_boundary_failure_stops_before_later_commands(phase):
    connection = BoundConnection()
    connection.fail_command = phase
    connection.error = asyncpg.InsufficientPrivilegeError("synthetic denied accessor")
    with pytest.raises(diagnostic.BackendWalDiagnosticUnavailable) as refusal:
        await diagnostic.sample_backend_wal_owner_boundary(connection, connection.pid)
    assert str(refusal.value) == "provider_directory_backend_wal_diagnostic_unavailable"
    expected_commands = ["clear", "force", "owner_sample"]
    assert connection.commands == expected_commands[: expected_commands.index(phase) + 1]
    assert not connection.closed and not connection.in_transaction


@pytest.mark.asyncio
async def test_owner_boundary_new_transaction_after_clear_does_not_force_or_commit():
    connection = BoundConnection()
    connection.enter_transaction_on = "clear"
    with pytest.raises(diagnostic.BackendWalDiagnosticUnavailable):
        await diagnostic.sample_backend_wal_owner_boundary(connection, connection.pid)
    assert connection.commands == ["clear"]
    assert connection.in_transaction and not connection.closed
