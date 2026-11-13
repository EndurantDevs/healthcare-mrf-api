# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Build, publish, retire and roll back projection generations in the documented order."""

import json
import re
from contextlib import asynccontextmanager
from types import SimpleNamespace

import asyncpg
import pytest
from click.testing import CliRunner

import main
from process import provider_geo_search as builder

pytestmark = pytest.mark.provider_geo_search


class _Connection:
    """Answer the builder's statements from a script keyed by SQL fragments."""

    def __init__(self, **answers):
        self.answers = {
            "is_current": [False, True],
            "generation": 1,
            "inputs": [('["a", 1]', '["n", 1]')],
            "fresh": [True],
            "batch_lock_failures": 0,
            "publisher_free": [True],
            "cancel_wider_than": None,
            "cancel_single_block": False,
            **answers,
        }
        self.statements = []
        self.statement_timeout = None

    @asynccontextmanager
    async def transaction(self, **options):
        self.statements.append(("BEGIN", options))
        yield
        self.statements.append(("COMMIT", {}))

    async def execute(self, sql, *args):
        self.statements.append((sql, args))
        if sql.startswith("DROP TABLE") and self.answers.get("drop_fails"):
            raise RuntimeError("lock timeout")
        if sql.startswith("SET LOCAL statement_timeout"):
            self.statement_timeout = sql.split("=", 1)[1].strip().strip("'")
        if sql.startswith('INSERT INTO "mrf"."provider_geo_search_g'):
            if self.answers["batch_lock_failures"]:
                self.answers["batch_lock_failures"] -= 1
                raise asyncpg.LockNotAvailableError("canceling statement due to lock timeout")
            block_bounds = re.search(r"ctid >= '\((\d+),0\)'::tid AND \w\.ctid < '\((\d+),0\)'", sql).groups()
            lower, upper = int(block_bounds[0]), int(block_bounds[1])
            limit = self.answers["cancel_wider_than"]
            if limit is not None and upper - lower > limit:
                raise asyncpg.QueryCanceledError("canceling statement due to statement timeout")
            if self.answers["cancel_single_block"]:
                raise asyncpg.QueryCanceledError("canceling statement due to statement timeout")

    async def fetchval(self, sql, *args):
        self.statements.append((sql, args))
        if "attname = 'layout'" in sql:
            return self.answers.get("has_layout", True)
        if "SELECT EXISTS" in sql:
            return self._next("is_current")
        if "pg_try_advisory_xact_lock_shared(hashtextextended" in sql:
            return self._next("publisher_free")
        if "pg_relation_size" in sql:
            return self.answers.get("blocks", 1)
        if "pg_try_advisory_lock" in sql:
            return self.answers.get("lock_free", True)
        if "COALESCE(max(generation), 0) + 1" in sql:
            return self.answers["generation"]
        if "FOR UPDATE" in sql and "address_input" in sql:
            return self._next("fresh")
        if "WHERE is_current FOR UPDATE" in sql:
            return self.answers.get("current")
        if "published_at IS NOT NULL AND NOT is_current" in sql:
            return self.answers.get("previous")
        raise AssertionError(sql)

    async def fetchrow(self, sql, *args):
        self.statements.append((sql, args))
        if "AS address_input" in sql:
            address_input, npi_input = self._next("inputs")
            return {"address_input": address_input, "npi_input": npi_input}
        return self.answers.get(
            "counts", {"row_count": 4, "identity_count": 4, "location_count": 4, "split_identity_count": 4}
        )

    async def fetch(self, sql, *args):
        self.statements.append((sql, args))
        if "aclexplode" in sql:
            return [{"grantee": '"reader"'}]
        return self.answers.get("retired", [])

    def is_in_transaction(self):
        return self.answers.get("left_in_transaction", False)

    def _next(self, name):
        answers = self.answers[name]
        return answers.pop(0) if len(answers) > 1 else answers[0]

    def executed(self, fragment):
        return [sql for sql, _args in self.statements if fragment in sql]


@pytest.mark.asyncio
async def test_current_generation_is_left_alone():
    connection = _Connection(is_current=[True])
    assert await builder.rebuild_provider_geo_search(connection, uses_service_locations=True) == {
        "status": "current",
        "include_service_locations": True,
    }
    assert connection.executed("CREATE TABLE IF NOT EXISTS")
    assert not connection.executed('CREATE TABLE "mrf"."provider_geo_search_g')
    assert connection.statements[0][0] == "SELECT pg_advisory_lock($1)"
    assert connection.statements[-2][0] == "SELECT pg_advisory_unlock($1)"
    assert connection.statements[-1][0].startswith("SELECT EXISTS")


@pytest.mark.asyncio
async def test_build_indexes_and_analyzes_before_switching_the_pointer():
    connection = _Connection(generation=3, retired=[{"generation": 1, "table_name": "provider_geo_search_g1"}])
    published = await builder.rebuild_provider_geo_search(connection, uses_service_locations=False)
    assert published == {
        "status": "published",
        "generation": 3,
        "table": "mrf.provider_geo_search_g3",
        "include_service_locations": False,
    }
    executed_statements = [sql for sql, _args in connection.statements]
    load = executed_statements.index(
        next(
            sql
            for sql in executed_statements
            if sql.startswith('INSERT INTO "mrf"."provider_geo_search_g3_load_addresses"')
        )
    )
    build = executed_statements.index(
        next(sql for sql in executed_statements if sql.startswith('CREATE TABLE "mrf"."provider_geo_search_g3" AS'))
    )
    geo_index = executed_statements.index(
        'CREATE INDEX provider_geo_search_g3_geo ON "mrf"."provider_geo_search_g3" USING gist (geo)'
    )
    analyze = executed_statements.index('ANALYZE "mrf"."provider_geo_search_g3"')
    switch = executed_statements.index(
        'UPDATE "mrf"."provider_geo_search_state" SET is_current = false WHERE is_current'
    )
    assert load < build < geo_index < analyze < switch
    assert "a.type IN ('primary', 'secondary')" in executed_statements[load]
    assert "a.ctid >= '(0,0)'::tid AND a.ctid < '(1,0)'::tid" in executed_statements[load]
    assert "provider_geo_search_g3_load_npis" in executed_statements[build]
    assert "entity_address_unified" not in executed_statements[build]
    assert executed_statements[load - 3 : load] == [
        "SET LOCAL lock_timeout = '1s'",
        "SELECT pg_try_advisory_xact_lock_shared(hashtextextended($1, 0))",
        "SET LOCAL statement_timeout = '15ms'",
    ]
    assert connection.executed('GRANT SELECT ON "mrf"."provider_geo_search_g3" TO "reader"')
    assert connection.executed('DROP TABLE IF EXISTS "mrf"."provider_geo_search_g1"')
    assert connection.executed("SELECT pg_advisory_xact_lock($1)")
    insert = next(
        args
        for sql, args in connection.statements
        if sql.lstrip().startswith('INSERT INTO "mrf"."provider_geo_search_state"')
    )
    assert insert == (3, "provider_geo_search_g3", '["a", 1]', '["n", 1]', False, 4, True, builder.LAYOUT)
    assert not connection.executed("ADD COLUMN layout")


@pytest.mark.asyncio
async def test_a_state_table_from_before_layouts_gains_the_column_once():
    connection = _Connection(is_current=[True], has_layout=False)
    await builder.rebuild_provider_geo_search(connection, uses_service_locations=True)
    assert connection.executed(
        'ALTER TABLE "mrf"."provider_geo_search_state" ADD COLUMN layout integer NOT NULL DEFAULT 1'
    )
    assert connection.executed("SET LOCAL lock_timeout = '5s'")


@pytest.mark.asyncio
async def test_hook_skips_while_another_build_holds_the_lock():
    connection = _Connection(lock_free=False)
    assert await builder.rebuild_provider_geo_search(
        connection, uses_service_locations=True, waits_for_build=False
    ) == {
        "status": "busy",
        "include_service_locations": True,
    }
    assert [sql for sql, _args in connection.statements] == ["SELECT pg_try_advisory_lock($1)"]


@pytest.mark.asyncio
async def test_inputs_published_after_a_build_are_rebuilt_while_locked():
    connection = _Connection(is_current=[False, False, True], generation=2)
    assert (await builder.rebuild_provider_geo_search(connection, uses_service_locations=True))["status"] == "published"
    assert len(connection.executed("SET is_current = true")) == 2


@pytest.mark.asyncio
async def test_inputs_published_during_the_load_discard_it_and_load_again():
    connection = _Connection(
        inputs=[('["a", 1]', '["n", 1]'), ('["a", 2]', '["n", 1]'), ('["a", 2]', '["n", 1]')],
        is_current=[False, False, True],
    )
    assert (await builder.rebuild_provider_geo_search(connection, uses_service_locations=True))["status"] == "published"
    assert len(connection.executed('CREATE UNLOGGED TABLE "mrf"."provider_geo_search_g1_load_addresses"')) == 2
    assert len(connection.executed('CREATE TABLE "mrf"."provider_geo_search_g1" AS')) == 1
    insert = next(
        args
        for sql, args in connection.statements
        if sql.lstrip().startswith('INSERT INTO "mrf"."provider_geo_search_state"')
    )
    assert insert[2:4] == ('["a", 2]', '["n", 1]')


@pytest.mark.asyncio
async def test_a_batch_waits_briefly_behind_a_cutover_and_retries(monkeypatch):
    monkeypatch.setattr(builder, "_BATCH_LOCK_TIMEOUT_SECONDS", 0)
    connection = _Connection(batch_lock_failures=2)
    assert (await builder.rebuild_provider_geo_search(connection, uses_service_locations=True))["status"] == "published"
    assert len(connection.executed('INSERT INTO "mrf"."provider_geo_search_g1_load_addresses"')) == 3
    with pytest.raises(asyncpg.LockNotAvailableError):
        await builder.rebuild_provider_geo_search(
            _Connection(batch_lock_failures=builder._BATCH_LOCK_ATTEMPTS), uses_service_locations=True
        )


@pytest.mark.asyncio
async def test_block_ranges_that_overrun_the_statement_cap_are_split():
    connection = _Connection(blocks=100, cancel_wider_than=30)
    assert (await builder.rebuild_provider_geo_search(connection, uses_service_locations=True))["status"] == "published"
    ranges = [
        tuple(
            int(bound) for bound in re.search(r"ctid >= '\((\d+),0\)'::tid AND a\.ctid < '\((\d+),0\)'", sql).groups()
        )
        for sql in connection.executed('INSERT INTO "mrf"."provider_geo_search_g1_load_addresses"')
    ]
    loaded_ranges = [(lower, upper) for lower, upper in ranges if upper - lower <= 30]
    assert loaded_ranges[0][0] == 0 and loaded_ranges[-1][1] == 100
    assert all(previous[1] == following[0] for previous, following in zip(loaded_ranges, loaded_ranges[1:]))


@pytest.mark.asyncio
async def test_a_single_block_that_overruns_the_cap_fails_the_build():
    connection = _Connection(cancel_single_block=True)
    with pytest.raises(asyncpg.QueryCanceledError):
        await builder.rebuild_provider_geo_search(connection, uses_service_locations=True)
    assert connection.executed('DROP TABLE IF EXISTS "mrf"."provider_geo_search_g1_load_addresses"')
    assert connection.statements[-1][0] == "SELECT pg_advisory_unlock($1)"


@pytest.mark.asyncio
async def test_a_transaction_left_open_by_a_cancelled_commit_is_rolled_back():
    connection = _Connection(blocks=4, cancel_wider_than=1, left_in_transaction=True)
    assert (await builder.rebuild_provider_geo_search(connection, uses_service_locations=True))["status"] == "published"
    assert "ROLLBACK" in [sql for sql, _args in connection.statements]
    assert connection.executed("SET LOCAL statement_timeout = 0")


@pytest.mark.asyncio
async def test_a_refused_refresh_is_built_after_the_unlock():
    connection = _Connection(is_current=[False, True, False, False, True, True], fresh=[True])
    assert (await builder.rebuild_provider_geo_search(connection, uses_service_locations=True))["status"] == "published"
    assert len(connection.executed('CREATE TABLE "mrf"."provider_geo_search_g1" AS')) == 2
    assert connection.executed("SELECT pg_try_advisory_lock($1)")


@pytest.mark.asyncio
async def test_address_batches_wait_while_the_publisher_holds_its_cutover_key(monkeypatch):
    monkeypatch.setattr(builder, "_BATCH_LOCK_TIMEOUT_SECONDS", 0)
    connection = _Connection(publisher_free=[False, False, True])
    assert (await builder.rebuild_provider_geo_search(connection, uses_service_locations=True))["status"] == "published"
    key_checks = [args for sql, args in connection.statements if "hashtextextended" in sql]
    assert key_checks[:3] == [("entity-address-unified:mrf",)] * 3
    address_batches = connection.executed('INSERT INTO "mrf"."provider_geo_search_g1_load_addresses"')
    assert len(address_batches) == 1
    assert connection.executed("SET LOCAL statement_timeout = '15ms'")
    assert connection.executed("SET LOCAL statement_timeout = '40ms'")


@pytest.mark.asyncio
async def test_timed_out_address_batches_are_followed_by_the_pause(monkeypatch):
    events = []
    real_sleep = builder.asyncio.sleep

    async def recorded_sleep(seconds):
        events.append(("pause", seconds))
        await real_sleep(0)

    monkeypatch.setattr(builder.asyncio, "sleep", recorded_sleep)
    connection = _Connection(blocks=8, cancel_wider_than=1)
    original_execute = connection.execute

    async def recorded_execute(sql, *args):
        if sql.startswith('INSERT INTO "mrf"."provider_geo_search_g1_load_addresses"'):
            events.append(("address batch", None))
        await original_execute(sql, *args)

    connection.execute = recorded_execute
    assert (await builder.rebuild_provider_geo_search(connection, uses_service_locations=True))["status"] == "published"
    address_events = [event for event in events if event[0] == "address batch" or event[1] == 0.04]
    # 8, 4 and 2 blocks time out; each fast single-block load then doubles the width and that grown batch
    # times out again (6 more), so 9 timed-out and 8 loaded batches are each followed by the pause.
    assert address_events == [("address batch", None), ("pause", 0.04)] * 17


@pytest.mark.asyncio
async def test_inputs_that_keep_changing_are_rebuilt_then_refused():
    connection = _Connection(fresh=[False], is_current=[False])
    with pytest.raises(builder.ProviderGeoSearchError, match="kept changing"):
        await builder.rebuild_provider_geo_search(connection, uses_service_locations=True)
    assert len(connection.executed('CREATE TABLE "mrf"."provider_geo_search_g1" AS')) == builder._BUILD_ATTEMPTS
    assert len(connection.executed('DROP TABLE IF EXISTS "mrf"."provider_geo_search_g1"')) >= builder._BUILD_ATTEMPTS
    assert not connection.executed("SET is_current = true")
    assert connection.statements[-1][0] == "SELECT pg_advisory_unlock($1)"


@pytest.mark.asyncio
async def test_inputs_that_change_once_are_rebuilt():
    connection = _Connection(fresh=[False, True], is_current=[False, False, True])
    assert (await builder.rebuild_provider_geo_search(connection, uses_service_locations=True))["status"] == "published"
    assert len(connection.executed('CREATE TABLE "mrf"."provider_geo_search_g1" AS')) == 2


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("answers", "message"),
    (
        ({"inputs": [(None, '["n", 1]')]}, "must be published"),
        ({"counts": {"row_count": 0, "identity_count": 0, "location_count": 0}}, "one row per location"),
        ({"counts": {"row_count": 3, "identity_count": 3, "location_count": 2}}, "one row per location"),
        (
            {"counts": {"row_count": 3, "identity_count": 2, "location_count": 3, "split_identity_count": 3}},
            "shared-identity flags",
        ),
    ),
)
async def test_invalid_inputs_or_rows_never_publish(answers, message):
    connection = _Connection(**answers)
    with pytest.raises(builder.ProviderGeoSearchError, match=message):
        await builder.rebuild_provider_geo_search(connection, uses_service_locations=True)
    assert not connection.executed("SET is_current = true")
    assert connection.statements[-1][0] == "SELECT pg_advisory_unlock($1)"


@pytest.mark.asyncio
async def test_busy_retired_tables_are_kept_for_the_next_build(caplog):
    connection = _Connection(drop_fails=True)
    await builder._drop_generation(connection, "mrf", 2, "provider_geo_search_g2")
    assert "Could not drop provider geo search generation 2" in caplog.text
    with pytest.raises(builder.ProviderGeoSearchError, match="name is invalid"):
        await builder._drop_generation(connection, "mrf", 2, "npi")


@pytest.mark.asyncio
async def test_rollback_points_back_at_the_previous_generation():
    connection = _Connection(current=5, previous=4)
    assert await builder.rollback_provider_geo_search(connection) == {"status": "rolled_back", "from": 5, "to": 4}
    assert connection.executed("SET is_current = true WHERE generation = $1")
    with pytest.raises(builder.ProviderGeoSearchError, match="no earlier"):
        await builder.rollback_provider_geo_search(_Connection(current=5, previous=None))


@pytest.mark.asyncio
async def test_run_uses_the_configured_schema_and_driver(monkeypatch):
    calls = []

    @asynccontextmanager
    async def acquire_driver(**options):
        calls.append(options)
        yield "connection"

    async def ensure_database(test_mode):
        calls.append(test_mode)

    async def rebuild(connection, **options):
        return {"action": "rebuild", "connection": connection, **options}

    async def rollback(connection, **options):
        return {"action": "rollback", "connection": connection, **options}

    import db.connection
    import process.ext.utils

    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "tenant")
    monkeypatch.setattr(db.connection, "db", SimpleNamespace(acquire_driver=acquire_driver))
    monkeypatch.setattr(process.ext.utils, "ensure_database", ensure_database)
    monkeypatch.setattr(builder, "rebuild_provider_geo_search", rebuild)
    monkeypatch.setattr(builder, "rollback_provider_geo_search", rollback)
    assert await builder.run_provider_geo_search(force=True) == {
        "action": "rebuild",
        "connection": "connection",
        "schema": "tenant",
        "force": True,
    }
    assert await builder.run_provider_geo_search(rollback=True) == {
        "action": "rollback",
        "connection": "connection",
        "schema": "tenant",
    }
    assert calls == [False, {"detached": True}, False, {"detached": True}]


def test_manage_commands_print_the_outcome(monkeypatch):
    requests = []

    async def run(**options):
        requests.append(options)
        return {"status": "published" if not options.get("rollback") else "rolled_back"}

    monkeypatch.setattr(builder, "run_provider_geo_search", run)
    runner = CliRunner()
    rebuilt = runner.invoke(main.manage, ["rebuild-provider-geo-search", "--force"])
    rolled_back = runner.invoke(main.manage, ["rollback-provider-geo-search"])
    assert rebuilt.exit_code == 0 and json.loads(rebuilt.output) == {"status": "published"}
    assert rolled_back.exit_code == 0 and json.loads(rolled_back.output) == {"status": "rolled_back"}
    assert requests == [{"force": True}, {"rollback": True}]
