from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace

import asyncpg
import pytest
from sqlalchemy.exc import DBAPIError

from process.ptg_parts import ptg2_shared_publish as publication


_STATEMENT_TIMEOUT = "canceling statement due to statement timeout"


def _driver_error(message=_STATEMENT_TIMEOUT, *, sqlstate="57014") -> asyncpg.PostgresError:
    error = asyncpg.PostgresError(message)
    error.message = message
    error.sqlstate = sqlstate
    return error


def _database_error(message=_STATEMENT_TIMEOUT, *, sqlstate="57014") -> DBAPIError:
    driver_error = _driver_error(message, sqlstate=sqlstate)
    adapter_error = RuntimeError(f"{type(driver_error)}: {message}")
    adapter_error.sqlstate = sqlstate
    adapter_error.pgcode = sqlstate
    adapter_error.__cause__ = driver_error
    return DBAPIError(_STATEMENT_TIMEOUT, {"message": _STATEMENT_TIMEOUT}, adapter_error)


def _diagnostic_error(message=_STATEMENT_TIMEOUT, *, sqlstate="57014") -> DBAPIError:
    error = RuntimeError(message)
    error.sqlstate = sqlstate
    error.diag = SimpleNamespace(message_primary=message)
    return DBAPIError(_STATEMENT_TIMEOUT, {"message": _STATEMENT_TIMEOUT}, error)


class _CASDriver:
    def __init__(self):
        self.attempts = []
        self.committed = []
        self.active = False
        self.inject = lambda phase: None
        self.validation_flags = [False, False, False]

    @property
    def current(self):
        return self.attempts[-1]

    def step(self, phase):
        self.current["events"].append(phase)
        self.inject(phase)

    @asynccontextmanager
    async def transaction(self):
        assert not self.active, "retry began before the previous transaction exited"
        self.active = True
        self.attempts.append({"events": [], "hashes": ()})
        try:
            yield self
        except BaseException:
            self.step("rollback")
            raise
        else:
            self.step("commit")
            self.committed.extend(self.current["hashes"])
        finally:
            self.active = False
            self.current["events"].append("exited")

    async def execute(self, statement, parameters):
        phase = "insert" if str(statement).lstrip().startswith("INSERT") else "validate"
        hashes = tuple(parameters["block_hashes"])
        if phase == "insert":
            self.current["hashes"] = hashes
        assert self.current["hashes"] == hashes
        self.step(phase)
        return SimpleNamespace(
            one=lambda: [len(hashes), 0, 0, 0, [], *self.validation_flags]
        )

    async def configure(self, session, **parameters):
        assert session is self
        assert parameters == {"lock_timeout": "500ms", "statement_timeout": "5s"}
        self.step("configure")

    async def lock(self, session, **parameters):
        assert session is self
        assert parameters == {
            "schema_name": "mrf",
            "snapshot_key": 7,
            "build_token": "build-7",
            "expected_generation": "shared_blocks_v4",
        }
        self.step("lock")

    async def is_lease_renewed(self, session, **parameters):
        assert session is self
        assert parameters == {
            "schema_name": "mrf",
            "snapshot_key": 7,
            "build_token": "build-7",
            "pin_token": "price_stage",
        }
        self.step("renew")
        return True


@pytest.fixture
def cas_driver(monkeypatch):
    driver = _CASDriver()
    monkeypatch.setattr(publication.db, "transaction", driver.transaction)
    monkeypatch.setattr(publication, "configure_ptg2_lifecycle_transaction", driver.configure)
    monkeypatch.setattr(publication, "lock_shared_layout_for_dense_write", driver.lock)
    monkeypatch.setattr(publication, "is_pin_lease_renewed", driver.is_lease_renewed)
    return driver


async def _publish(hashes):
    await publication._publish_durable_cas_batch(
        schema_name="mrf",
        schema='"mrf"',
        stage='"price_stage"',
        snapshot_key=7,
        build_token="build-7",
        expected_generation="shared_blocks_v4",
        block_hashes=hashes,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("timeout_phase", ["insert", "validate"])
@pytest.mark.parametrize("error_factory", [_database_error, _diagnostic_error])
async def test_cas_timeout_retries_after_rollback_without_skipping_hashes(
    cas_driver, timeout_phase, error_factory, caplog
):
    hashes = tuple(index.to_bytes(32, "big") for index in range(8))
    error = error_factory()

    def inject(phase):
        if phase == timeout_phase and len(cas_driver.current["hashes"]) > 2:
            raise error

    cas_driver.inject = inject
    await _publish(hashes)

    assert [len(attempt["hashes"]) for attempt in cas_driver.attempts] == [8, 4, 2, 2, 2, 2]
    assert cas_driver.committed == list(hashes)
    assert [record.args for record in caplog.records if record.name == publication.__name__] == [
        ('"price_stage"', 8, 4), ('"price_stage"', 4, 2)
    ]
    for attempt in cas_driver.attempts[:2]:
        assert attempt["events"][-2:] == ["rollback", "exited"]
        assert "renew" not in attempt["events"]
    for attempt in cas_driver.attempts[2:]:
        assert attempt["events"] == [
            "configure", "lock", "insert", "validate", "renew", "commit", "exited"
        ]


@pytest.mark.asyncio
async def test_cas_retry_width_stays_reduced_after_success(cas_driver):
    hashes = tuple(index.to_bytes(32, "big") for index in range(9))

    def inject(phase):
        attempt = cas_driver.current
        if phase == "insert" and (
            len(attempt["hashes"]) > 4
            or (attempt["hashes"][0] == hashes[4] and len(attempt["hashes"]) > 2)
        ):
            raise _database_error()

    cas_driver.inject = inject
    await _publish(hashes)

    assert [len(attempt["hashes"]) for attempt in cas_driver.attempts] == [9, 4, 4, 2, 2, 1]
    assert cas_driver.committed == list(hashes)


@pytest.mark.asyncio
async def test_cas_singleton_timeout_preserves_original_error(cas_driver):
    error = _database_error()
    hashes = tuple(index.to_bytes(32, "big") for index in range(4096))

    def inject(phase):
        if phase == "insert":
            raise error

    cas_driver.inject = inject
    with pytest.raises(DBAPIError) as caught:
        await _publish(hashes)

    assert caught.value is error
    assert [len(attempt["hashes"]) for attempt in cas_driver.attempts] == [
        4096, 2048, 1024, 512, 256, 128, 64, 32, 16, 8, 4, 2, 1
    ]
    assert cas_driver.committed == []
    assert all(attempt["events"][-2:] == ["rollback", "exited"] for attempt in cas_driver.attempts)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("phase", "error"),
    [
        (phase, _database_error())
        for phase in ("configure", "lock", "renew", "commit")
    ] + [
        ("lock", RuntimeError("build ownership lost")),
        ("commit", ConnectionError("commit outcome unknown")),
    ],
)
async def test_cas_does_not_retry_outside_cas_statements(cas_driver, phase, error):
    def inject(actual_phase):
        if actual_phase == phase:
            raise error

    cas_driver.inject = inject
    with pytest.raises(type(error)) as caught:
        await _publish((b"a" * 32, b"b" * 32))

    assert caught.value is error
    assert len(cas_driver.attempts) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["insert", "validate"])
@pytest.mark.parametrize(
    "error",
    [
        _database_error("canceling statement due to user request"),
        _database_error("unrecognized cancellation"),
        _database_error(_STATEMENT_TIMEOUT, sqlstate="55P03"),
        RuntimeError(_STATEMENT_TIMEOUT),
        asyncio.CancelledError(),
    ],
    ids=["user-cancel", "unknown-cancel", "lock-timeout", "untyped-message", "task-cancel"],
)
async def test_cas_does_not_retry_other_statement_errors(cas_driver, phase, error):
    def inject(actual_phase):
        if actual_phase == phase:
            raise error

    cas_driver.inject = inject
    with pytest.raises(type(error)) as caught:
        await _publish((b"a" * 32, b"b" * 32))

    assert caught.value is error
    assert len(cas_driver.attempts) == 1
    assert cas_driver.current["events"][-2:] == ["rollback", "exited"]


@pytest.mark.asyncio
@pytest.mark.parametrize("rollback_error", [_database_error(), asyncio.CancelledError()])
async def test_cas_rollback_failure_prevents_retry(cas_driver, rollback_error):
    def inject(phase):
        if phase == "insert":
            raise _database_error()
        if phase == "rollback":
            raise rollback_error

    cas_driver.inject = inject
    with pytest.raises(type(rollback_error)) as caught:
        await _publish((b"a" * 32, b"b" * 32))

    assert caught.value is rollback_error
    assert len(cas_driver.attempts) == 1
    assert not cas_driver.active


@pytest.mark.asyncio
@pytest.mark.parametrize("flag_index", [0, 1, 2])
async def test_cas_validation_conflicts_still_rollback(cas_driver, flag_index):
    cas_driver.validation_flags[flag_index] = True

    with pytest.raises(RuntimeError):
        await _publish((b"a" * 32, b"b" * 32))

    assert len(cas_driver.attempts) == 1
    assert cas_driver.committed == []
    assert cas_driver.current["events"][-2:] == ["rollback", "exited"]


def test_cas_timeout_detection_uses_primary_driver_message():
    error = _database_error("canceling statement due to user request")
    error.__context__ = _driver_error()
    assert not publication._is_cas_statement_timeout(error)
    assert publication._is_cas_statement_timeout(_driver_error())


@pytest.mark.parametrize(
    ("primary_message", "sqlstate", "expected"),
    [
        (_STATEMENT_TIMEOUT, "57014", True),
        ("canceling statement due to user request", "57014", False),
        ("unknown localized cancellation message", "57014", False),
        (_STATEMENT_TIMEOUT, "55P03", False),
        (None, "57014", False),
        (123, "57014", False),
    ],
)
def test_cas_timeout_detection_uses_diagnostic_primary_message(
    primary_message, sqlstate, expected
):
    error = _diagnostic_error(primary_message, sqlstate=sqlstate)
    assert publication._is_cas_statement_timeout(error) is expected
    error.orig.message = None
    assert publication._is_cas_statement_timeout(error) is expected
    error.orig.message = "canceling statement due to user request"
    assert not publication._is_cas_statement_timeout(error)


def test_cas_timeout_detection_handles_cyclic_unknown_wrappers():
    error = RuntimeError(_STATEMENT_TIMEOUT)
    error.orig = error
    assert not publication._is_cas_statement_timeout(error)


@pytest.mark.asyncio
@pytest.mark.parametrize("generation", ["shared", "v4"])
@pytest.mark.parametrize("fail_singleton", [False, True])
async def test_cas_stage_cursor_waits_for_committed_subbatches(
    cas_driver, monkeypatch, generation, fail_singleton
):
    hashes = tuple(index.to_bytes(32, "big") for index in range(4))
    cursors = []
    dropped_statements = []

    async def pin_stage(**parameters):
        return '"price_hash_index"'

    async def fetch_hashes(*, schema, stage, last_hash):
        cursors.append(last_hash)
        if last_hash is None:
            return hashes
        assert cas_driver.committed == list(hashes)
        return ()

    async def drop_index(statement):
        dropped_statements.append(statement)

    def inject(phase):
        if phase == "insert" and (
            len(cas_driver.current["hashes"]) > 2
            or (fail_singleton and cas_driver.current["hashes"][0] == hashes[2])
        ):
            raise _database_error()

    cas_driver.inject = inject
    monkeypatch.setattr(publication, "_pin_v4_cas_stage", pin_stage)
    monkeypatch.setattr(publication, "_v4_stage_hash_batch", fetch_hashes)
    monkeypatch.setattr(publication.db, "status", drop_index)
    publish_parameters_by_name = dict(schema_name="mrf", stage_table="price_stage", snapshot_key=7, build_token="build-7")
    if generation == "shared":
        operation = publication.prepare_shared_cas_block_stage
        publish_parameters_by_name["expected_generation"] = "shared_blocks_v4"
    else:
        operation = publication.prepare_v4_cas_block_stage
    if fail_singleton:
        with pytest.raises(DBAPIError):
            await operation(**publish_parameters_by_name)
        assert cursors == [None]
        assert cas_driver.committed == list(hashes[:2])
        assert dropped_statements == []
    else:
        await operation(**publish_parameters_by_name)
        assert cursors == [None, hashes[-1]]
        assert len(dropped_statements) == 1


@pytest.mark.asyncio
async def test_cas_lost_pin_lease_rolls_back_without_retry(cas_driver, monkeypatch):
    async def is_lease_renewed(session, **parameters):
        return False

    monkeypatch.setattr(publication, "is_pin_lease_renewed", is_lease_renewed)
    with pytest.raises(RuntimeError, match="lost ownership"):
        await _publish((b"a" * 32, b"b" * 32))

    assert len(cas_driver.attempts) == 1
    assert cas_driver.committed == []
    assert cas_driver.current["events"][-2:] == ["rollback", "exited"]


@pytest.mark.parametrize("explicit_unknown_cause", [False, True])
def test_cas_timeout_detection_ignores_stale_context(explicit_unknown_cause):
    error = RuntimeError("current operation failed")
    error.__context__ = _driver_error()
    if explicit_unknown_cause:
        error.__cause__ = RuntimeError("unknown current driver failure")
    assert not publication._is_cas_statement_timeout(error)
