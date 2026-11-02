# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""MRF acquisition refuses mutable work without its exact Source reservation."""

from contextlib import asynccontextmanager
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process import mrf_discovery_acquisition_guard as guard

DATASET = dict(zip(guard.DATASET_KEYS, range(11, 16)))
CONTROL_RELATION = '"discovery_control"."reservations"'


@pytest.fixture(autouse=True)
def configured_control_relation(monkeypatch):
    monkeypatch.setenv(guard.CONTROL_RELATION_ENV, "discovery_control.reservations")


def reserved_run(params=None):
    params = {"crawl": True} if params is None else params
    return {
        "run_id": "synthetic-mrf-run",
        "importer": guard.IMPORTER,
        "engine": guard.ENGINE,
        "status": "queued",
        "idempotency_key": "synthetic-key",
        "params": deepcopy(params),
        "metrics": {
            guard.MARKER: {
                "schema": "mrf-acquisition-v1",
                "run_id": "synthetic-mrf-run",
                "importer": guard.IMPORTER,
                "engine": guard.ENGINE,
                "idempotency_key": "synthetic-key",
                "dataset": dict(DATASET),
                "params_digest": guard.params_digest(params),
            }
        },
    }


class Driver:
    def __init__(self, source=None):
        self.source = reserved_run() if source is None else source
        self.local = deepcopy(self.source)
        self.old = None
        self.blocker = self.active = None
        self.identity = {**DATASET, "can_read_control": True}
        self.locked = False
        self.sql = []

    async def fetchrow(self, sql, *params):
        self.sql.append((sql, params))
        return dict(self.identity)

    async def fetchval(self, sql, *params):
        self.sql.append((sql, params))
        if "pg_try_advisory_lock" in sql:
            self.locked = True
            return True
        if "pg_advisory_unlock" in sql:
            self.locked = False
            return True
        if "SELECT r.run_id" in sql:
            return self.blocker
        if sql.startswith("SELECT run_id"):
            return self.active
        if "to_jsonb(r)" in sql:
            if CONTROL_RELATION in sql:
                return self.source if params[0] == self.source.get("run_id") else self.old
            return self.local
        raise AssertionError(sql)


def bind_driver(monkeypatch, driver):
    @asynccontextmanager
    async def acquire(*, detached=False):
        yield driver

    monkeypatch.setattr(guard, "db", SimpleNamespace(acquire_driver=acquire))


@pytest.mark.parametrize(
    "configured",
    [None, "", " ", "reservations", "a.b.c", "a-b.c", '"a"."b"', "a.b;SELECT 1", "a" * 64 + ".b", "a." + "b" * 64],
)
async def test_invalid_control_configuration_refuses_before_sql(monkeypatch, configured):
    if configured is None:
        monkeypatch.delenv(guard.CONTROL_RELATION_ENV)
    else:
        monkeypatch.setenv(guard.CONTROL_RELATION_ENV, configured)
    driver = Driver()
    with pytest.raises(ValueError, match="^mrf_acquisition_not_admitted$"):
        await guard.require_reservation(driver, driver.local)
    assert driver.sql == []


@pytest.mark.parametrize("changed", ["control_run_oid", "can_read_control"])
async def test_configured_control_requires_matching_native_identity_and_privilege(changed):
    driver = Driver()
    driver.identity[changed] = DATASET[changed] + 1 if changed in DATASET else False
    with pytest.raises(ValueError, match="mrf_acquisition_not_admitted"):
        await guard.require_reservation(driver, driver.local)


@pytest.mark.parametrize("catalog_status", ["", "running", "failed"])
async def test_pending_catalog_refuses_before_mutation_without_a_history_limit(catalog_status):
    driver = Driver()
    driver.blocker = "synthetic-unresolved-" + catalog_status
    before = deepcopy(driver.source)
    with pytest.raises(ValueError, match="mrf_acquisition_not_admitted"):
        await guard.require_reservation(driver, driver.local)
    query = next(sql for sql, _ in driver.sql if "SELECT r.run_id" in sql)
    assert "LIMIT 1" in query and "LIMIT 500" not in query
    assert "succeeded','superseded" in query
    assert driver.source == before


@pytest.mark.parametrize("changed", ["status", "dataset", "digest", "key", "params", "marker"])
async def test_reservation_identity_is_not_a_caller_supplied_bypass(changed):
    driver = Driver()
    marker = driver.source["metrics"][guard.MARKER]
    if changed == "status":
        driver.source["status"] = "succeeded"
    if changed == "dataset":
        marker["dataset"]["file_oid"] += 1
    if changed == "digest":
        marker["params_digest"] = "0" * 64
    if changed == "key":
        driver.local["idempotency_key"] = "different-key"
    if changed == "params":
        driver.local["params"]["probe_files"] = True
    if changed == "marker":
        driver.source["metrics"] = {}
    with pytest.raises(ValueError, match="mrf_acquisition_not_admitted"):
        await guard.require_reservation(driver, driver.local)


def replacement(driver):
    link_by_field = {"run_id": "synthetic-old", "attempt": 3, "old_digest": "a" * 64, "command_id": "b" * 64}
    driver.source["metrics"][guard.MARKER]["replacement_of"] = link_by_field
    driver.old = {
        "run_id": link_by_field["run_id"],
        "importer": guard.IMPORTER,
        "engine": guard.ENGINE,
        "status": "succeeded",
        "metrics": {
            "catalog_sync_status": "failed",
            "catalog_sync_attempt": 3,
            guard.REPLACEMENT: {
                **link_by_field,
                "schema": "mrf-replacement-v1",
                "run_id": driver.source["run_id"],
                "dataset": dict(DATASET),
            },
        },
    }


@pytest.mark.parametrize("mismatch", [False, True])
async def test_only_the_exact_reciprocal_failed_old_followup_is_excepted(mismatch):
    driver = Driver()
    replacement(driver)
    if mismatch:
        driver.old["metrics"][guard.REPLACEMENT]["command_id"] = "c" * 64
        with pytest.raises(ValueError, match="mrf_acquisition_not_admitted"):
            await guard.require_reservation(driver, driver.local)
    else:
        assert await guard.require_reservation(driver, driver.local) == DATASET
        _, query_args = next(entry for entry in driver.sql if "SELECT r.run_id" in entry[0])
        assert query_args[-1] == "synthetic-old"
        assert driver.old["metrics"]["catalog_sync_status"] == "failed"


async def test_native_lock_remains_held_across_command_and_releases_after_failure(monkeypatch):
    driver = Driver()
    bind_driver(monkeypatch, driver)

    async def command(crawl=False, dry_run=False, run_id=None):
        return None

    with pytest.raises(RuntimeError, match="synthetic interruption"):
        async with guard.acquisition_command(
            command, {"crawl": True, "dry_run": False, "run_id": driver.source["run_id"]}
        ):
            assert driver.locked
            raise RuntimeError("synthetic interruption")
    assert not driver.locked


@pytest.mark.parametrize("fail_command", [False, True])
async def test_unlock_error_preserves_command_result_and_detached_cleanup(monkeypatch, fail_command):
    driver, closed = Driver(), []
    original_fetch = driver.fetchval

    @asynccontextmanager
    async def acquire(*, detached=False):
        assert detached
        try:
            yield driver
        finally:
            closed.append(driver)
            driver.locked = False

    async def fetchval(sql, *params):
        if "pg_advisory_unlock" in sql:
            assert driver.locked
            raise ConnectionResetError("synthetic unlock failure")
        return await original_fetch(sql, *params)

    async def command(crawl=False, dry_run=False, run_id=None):
        return None

    monkeypatch.setattr(guard, "db", SimpleNamespace(acquire_driver=acquire))
    monkeypatch.setattr(driver, "fetchval", fetchval)
    original_error = RuntimeError("synthetic command failure")

    async def execute():
        async with guard.acquisition_command(
            command, {"crawl": True, "dry_run": False, "run_id": driver.source["run_id"]}
        ):
            assert driver.locked
            if fail_command:
                raise original_error

    if fail_command:
        with pytest.raises(RuntimeError) as raised:
            await execute()
        assert raised.value is original_error
    else:
        await execute()
    assert closed == [driver] and not driver.locked


async def test_command_captures_control_relation_before_lock_recheck(monkeypatch):
    driver = Driver()
    replacement(driver)
    bind_driver(monkeypatch, driver)
    original_fetch = driver.fetchval

    async def mutate_configuration(sql, *params):
        result = await original_fetch(sql, *params)
        if "pg_try_advisory_lock" in sql:
            monkeypatch.setenv(guard.CONTROL_RELATION_ENV, "changed_control.reservations")
        return result

    async def command(crawl=False, dry_run=False, run_id=None):
        return None

    monkeypatch.setattr(driver, "fetchval", mutate_configuration)
    async with guard.acquisition_command(command, {"crawl": True, "dry_run": False, "run_id": driver.local["run_id"]}):
        assert driver.locked
    identity_relations = [params[0] for sql, params in driver.sql if "AS control_run_oid" in sql]
    assert identity_relations == [CONTROL_RELATION, CONTROL_RELATION]
    control_reads = [sql for sql, _ in driver.sql if CONTROL_RELATION in sql]
    assert len(control_reads) == 3
    assert not driver.locked


async def test_command_recheck_cannot_change_the_locked_native_dataset(monkeypatch):
    driver = Driver()
    bind_driver(monkeypatch, driver)
    original_fetch = driver.fetchval

    async def replace_control_relation(sql, *params):
        result = await original_fetch(sql, *params)
        if "pg_try_advisory_lock" in sql:
            driver.identity["control_run_oid"] += 1
            for run in (driver.source, driver.local):
                run["metrics"][guard.MARKER]["dataset"]["control_run_oid"] = driver.identity["control_run_oid"]
        return result

    async def command(crawl=False, dry_run=False, run_id=None):
        return None

    monkeypatch.setattr(driver, "fetchval", replace_control_relation)
    with pytest.raises(ValueError, match="mrf_acquisition_not_admitted"):
        async with guard.acquisition_command(
            command, {"crawl": True, "dry_run": False, "run_id": driver.local["run_id"]}
        ):
            pytest.fail("changed dataset entered work under the previous lock")
    assert not driver.locked


@pytest.mark.parametrize("state", ["succeeded", "failed", "canceling"])
async def test_worker_cannot_rerun_a_terminal_or_nonrunnable_local_producer(state):
    driver = Driver()
    driver.local["status"] = state
    with pytest.raises(ValueError, match="mrf_acquisition_not_admitted"):
        await guard.require_reservation(driver, driver.local, require_local=True)


async def test_execution_cannot_change_the_reserved_command_params(monkeypatch):
    driver = Driver()
    bind_driver(monkeypatch, driver)

    async def command(crawl=False, dry_run=False, run_id=None):
        return None

    with pytest.raises(ValueError, match="mrf_acquisition_not_admitted"):
        async with guard.acquisition_command(
            command, {"crawl": False, "dry_run": False, "run_id": driver.source["run_id"]}
        ):
            pytest.fail("changed command entered mutable work")
    assert not driver.locked


async def test_dry_run_and_other_importers_do_not_open_the_control_connection(monkeypatch):
    monkeypatch.delenv(guard.CONTROL_RELATION_ENV)
    unavailable_database = object()
    monkeypatch.setattr(guard, "db", unavailable_database)
    await guard.require_acquisition({"importer": "synthetic-unrelated"})
    await guard.require_acquisition({"importer": guard.IMPORTER, "params": {"dry_run": True}})
    async with guard.acquisition_command(None, {"dry_run": True}):
        assert guard.db is unavailable_database


@pytest.mark.parametrize("importer", [guard.IMPORTER, " " + guard.IMPORTER + " "])
async def test_api_refuses_before_create_or_enqueue(monkeypatch, importer):
    from api import control_imports

    driver = Driver()
    bind_driver(monkeypatch, driver)
    enqueue = AsyncMock()
    admit = AsyncMock()
    monkeypatch.setattr(control_imports, "_enqueue_import_start", enqueue)
    monkeypatch.setattr(control_imports, "_admit_import_row", admit)
    with pytest.raises(ValueError, match="mrf_acquisition_not_admitted"):
        await control_imports.create_import_run(
            {"importer": importer, "params": {"force": True, "ignore_policy": True}}
        )
    assert driver.sql == []
    enqueue.assert_not_called()
    admit.assert_not_called()


async def test_retry_forwards_only_the_mrf_preallocated_child(monkeypatch):
    from api import control_imports

    monkeypatch.setattr(control_imports, "get_import_run", AsyncMock(return_value=reserved_run()))
    create = AsyncMock(return_value=({}, True))
    monkeypatch.setattr(control_imports, "create_import_run", create)
    await control_imports.retry_import_run("synthetic-parent", {"run_id": "synthetic-child", "idempotency_key": "key"})
    assert create.call_args.args[0]["run_id"] == "synthetic-child"
    assert create.call_args.args[0]["params"]["retry_of_run_id"] == "synthetic-parent"


@pytest.mark.parametrize(
    "payload", [{"queue": "arq:MRFSourceDiscovery"}, {"importer": guard.IMPORTER, "run_id": "synthetic-mrf-run"}]
)
async def test_worker_selectors_cannot_launch_unadmitted_acquisition(monkeypatch, payload):
    from api import control_imports, control_workers

    launch = Mock()
    monkeypatch.setattr(control_workers, "ensure_worker", launch)
    monkeypatch.setattr(control_imports, "get_import_run", AsyncMock(return_value=reserved_run()))
    monkeypatch.setattr(guard, "require_acquisition", AsyncMock(side_effect=ValueError("mrf_acquisition_not_admitted")))
    result = await control_workers.guarded_ensure_worker(payload)
    assert result["status"] == "failed"
    launch.assert_not_called()


@pytest.mark.parametrize("entry", ["main", "process_data"])
async def test_unmanaged_command_and_legacy_job_refuse_before_discovery(monkeypatch, entry):
    from process import mrf_source_discovery as discovery

    execute = AsyncMock()
    monkeypatch.setattr(discovery, "_execute_discovery_command", execute)
    with pytest.raises(ValueError, match="mrf_acquisition_not_admitted"):
        if entry == "main":
            await discovery.main(crawl=True)
        else:
            await discovery.process_data({}, {"crawl": True})
    execute.assert_not_called()


async def test_command_selects_its_configured_database_before_admission(monkeypatch):
    from process import mrf_source_discovery as discovery

    events = []

    async def select(test_mode):
        assert test_mode is True
        events.append("connection-selected")

    @asynccontextmanager
    async def admit(*args):
        events.append("admission")
        yield

    monkeypatch.setattr(discovery, "ensure_database", select)
    monkeypatch.setattr(discovery, "acquisition_command", admit)
    monkeypatch.setattr(discovery, "_execute_discovery_command", AsyncMock(return_value={}))
    await discovery.main(run_id="synthetic-mrf-run", test_mode=True)
    assert events == ["connection-selected", "admission"]


def test_subprocess_rechecks_native_owner_before_source_work(monkeypatch):
    from process import mrf_source_discovery as discovery

    process = AsyncMock()
    owner = AsyncMock(side_effect=ValueError("mrf_acquisition_not_admitted"))
    disconnected = AsyncMock()
    monkeypatch.setattr(discovery, "ensure_database", AsyncMock())
    monkeypatch.setattr(discovery, "require_acquisition_owner", owner)
    monkeypatch.setattr(discovery, "_process_discovery_source_record", process)
    monkeypatch.setattr(discovery, "db", SimpleNamespace(disconnect=disconnected))
    options = discovery.DiscoverySourceProcessingOptions(
        test_mode=False,
        check_urls=False,
        crawl=True,
        observation_run_id="synthetic-mrf-run",
        max_toc_bytes=1024,
        crawl_target_limit=None,
    )
    with pytest.raises(ValueError, match="mrf_acquisition_not_admitted"):
        discovery._run_discovery_source_process({}, options)
    owner.assert_awaited_once_with("synthetic-mrf-run")
    disconnected.assert_awaited_once()
    process.assert_not_called()


async def test_health_exposes_native_dataset_at_top_level_for_source_reservation(monkeypatch):
    from api import control_imports

    monkeypatch.setattr(
        control_imports, "_database_check", AsyncMock(return_value={"ok": True, "mrf_acquisition_dataset": DATASET})
    )
    monkeypatch.setattr(control_imports, "_redis_check", lambda: {"ok": True})
    monkeypatch.setattr(control_imports, "_worker_and_queue_health", lambda: ({}, {}, {}))
    monkeypatch.setattr(control_imports, "_ram_status", lambda: {})
    monkeypatch.setattr(control_imports, "_disk_status_by_name", lambda _: {})
    assert (await control_imports.node_health())["mrf_acquisition_dataset"] == DATASET


@pytest.mark.parametrize("failure", [None, "database", "acquisition"])
async def test_database_health_keeps_connection_and_acquisition_failures_distinct(monkeypatch, failure):
    from api import control_imports

    database = SimpleNamespace(execute=AsyncMock())
    acquire = AsyncMock(return_value=DATASET)
    if failure == "database":
        database.execute.side_effect = RuntimeError("synthetic database failure")
    if failure == "acquisition":
        acquire.side_effect = ValueError("mrf_acquisition_not_admitted")
    monkeypatch.setattr(control_imports, "db", database)
    monkeypatch.setattr(guard, "acquisition_dataset", acquire)
    report = await control_imports._database_check()
    database.execute.assert_awaited_once()
    if failure == "database":
        assert report == {"ok": False, "error": "synthetic database failure"}
        acquire.assert_not_awaited()
    else:
        assert report == {"ok": True, "mrf_acquisition_dataset": None if failure else DATASET}
        acquire.assert_awaited_once()


def test_foreign_dataset_requires_disjoint_exporters_across_all_relation_families():
    query = guard._foreign_dataset_sql()
    for known in ("source_oid", "payer_oid", "file_oid"):
        for target in ("source_oid", "payer_oid", "file_oid"):
            assert f"->'{known}' <> $2::jsonb->'{target}'" in query
    assert query.count(" <>") == 9


@pytest.mark.parametrize(
    "params,expected",
    [
        (
            {
                "limit": "5",
                "file_probe_limit": "7",
                "crawl_target_limit": "9",
                "max_toc_bytes": "4096",
                "concurrency": "2",
                "test": True,
                "crawl": 1,
                "check_urls": "false",
                "probe_files": 0,
                "retry_of_run_id": 123,
                "mrf_discovery_root_run_id": " root ",
            },
            {"test_mode": True, "crawl": True, "check_urls": True, "probe_files": False},
        ),
        ({"test": True, "test_mode": False}, {"test_mode": False}),
        ({"test": True, "test_mode": None}, {"test_mode": False}),
        ({"test": False, "test_mode": "false"}, {"test_mode": True}),
        ({"limit": "invalid", "retry_of_run_id": " ", "concurrency": 0}, {"limit": None, "retry_of_run_id": None}),
    ],
)
async def test_worker_normalization_preserves_raw_admission_and_alias_precedence(monkeypatch, params, expected):
    """Normalize execution exactly once without rewriting raw reservation identity."""
    from process import mrf_source_discovery as discovery

    driver = Driver(reserved_run(params))
    before = deepcopy(driver.source)
    dispatched = AsyncMock(return_value={"completed": True})
    monkeypatch.setattr(discovery, "main", dispatched)
    assert await discovery.process_data({}, {**params, "run_id": driver.local["run_id"]}) == {"completed": True}
    actual_params = dispatched.await_args.kwargs
    assert {key: actual_params[key] for key in expected} == expected
    if "file_probe_limit" in params:
        assert [actual_params[key] for key in ("limit", "file_probe_limit", "crawl_target_limit")] == [5, 7, 9]
        assert actual_params["max_toc_bytes"] == 4096 and actual_params["concurrency"] == 2
        assert actual_params["retry_of_run_id"] == "123" and actual_params["mrf_discovery_root_run_id"] == "root"
    assert (
        await guard.require_reservation(
            driver,
            driver.local,
            require_local=True,
            command=discovery.run_mrf_source_discovery_command,
            command_params=actual_params,
        )
        == DATASET
    )
    assert driver.source == before and driver.local["params"] == params
    assert before["metrics"][guard.MARKER]["params_digest"] == guard.params_digest(params)
    assert guard.params_digest(params) != guard.params_digest(discovery.discovery_command_params(params))


@pytest.mark.parametrize(
    "field,changed", [("limit", 6), ("crawl", False), ("test_mode", True), ("retry_of_run_id", "different")]
)
async def test_normalized_execution_still_refuses_changed_command(field, changed):
    """Execution normalization never changes the admitted raw request or masks drift."""
    from process import mrf_source_discovery as discovery

    driver = Driver(reserved_run({"limit": "5", "crawl": 1, "test": True, "test_mode": False}))
    actual_params_by_name = {
        **discovery.discovery_command_params(driver.local["params"]),
        "run_id": driver.local["run_id"],
    }
    actual_params_by_name[field] = changed
    with pytest.raises(ValueError, match="^mrf_acquisition_not_admitted$"):
        await guard.require_reservation(
            driver,
            driver.local,
            require_local=True,
            command=discovery.run_mrf_source_discovery_command,
            command_params=actual_params_by_name,
        )


async def test_normalized_equivalent_raw_rewrite_is_not_an_admission_bypass():
    """Raw reservation identity stays exact even when worker values would match."""
    from process import mrf_source_discovery as discovery

    driver = Driver(reserved_run({"limit": "5"}))
    driver.local["params"]["limit"] = 5
    assert discovery.discovery_command_params(driver.source["params"]) == discovery.discovery_command_params(
        driver.local["params"]
    )
    with pytest.raises(ValueError, match="^mrf_acquisition_not_admitted$"):
        await guard.require_reservation(
            driver,
            driver.local,
            require_local=True,
            command=discovery.run_mrf_source_discovery_command,
            command_params={
                **discovery.discovery_command_params(driver.local["params"]),
                "run_id": driver.local["run_id"],
            },
        )


@pytest.mark.parametrize("representation", ["missing", "null"])
async def test_empty_reservation_params_match_worker_defaults(representation):
    """Admission and execution use the same existing empty-parameter contract."""
    from process import mrf_source_discovery as discovery

    driver = Driver(reserved_run({}))
    for reservation_record in (driver.source, driver.local):
        if representation == "missing":
            reservation_record.pop("params")
        else:
            reservation_record["params"] = None
    before = deepcopy(driver.source)
    command_params_by_name = {**discovery.discovery_command_params({}), "run_id": driver.local["run_id"]}
    assert (
        await guard.require_reservation(
            driver,
            driver.local,
            require_local=True,
            command=discovery.run_mrf_source_discovery_command,
            command_params=command_params_by_name,
        )
        == DATASET
    )
    assert driver.source == before
    command_params_by_name["crawl"] = True
    with pytest.raises(ValueError, match="^mrf_acquisition_not_admitted$"):
        await guard.require_reservation(
            driver,
            driver.local,
            require_local=True,
            command=discovery.run_mrf_source_discovery_command,
            command_params=command_params_by_name,
        )
