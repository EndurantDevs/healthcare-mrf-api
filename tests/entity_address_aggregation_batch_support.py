# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Composed aggregate tuples share one native label across checksum shards."""

from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest

from process import entity_address_aggregation_batch as aggregate
from process import entity_address_batch as batch
from process import entity_address_ordinary_preparation as ordinary
from tests import entity_address_rows_preparation_support as fixtures
from tests.entity_address_batch_postgres_support import database_fixture


def _protocol_sql(packet, *, bound=False, updated=1):
    """Observe transaction and progress contracts without requiring a native wheel."""
    observed = SimpleNamespace(queries=[], updates=[], events=[], timings=[], transactions=[])

    @asynccontextmanager
    async def transaction(kind, *_args):
        observed.transactions.append((kind, "enter"))
        try:
            yield
        except BaseException:
            observed.transactions.append((kind, "rollback"))
            raise
        else:
            observed.transactions.append((kind, "commit"))

    async def read_first(database, statement, **parameters):
        assert database is sql.db
        observed.queries.append((statement, parameters))
        return packet

    async def apply_settings():
        observed.transactions.append(("settings", "apply"))

    async def run_phase(statement, **options):
        observed.updates.append((statement, options))
        return updated

    sql = SimpleNamespace(
        db=SimpleNamespace(_transaction_binding=lambda: bound or None, transaction=lambda: transaction("owned")),
        preparation_admission=SimpleNamespace(read_first=read_first),
        entity_address_tuned_transaction=lambda *_args: transaction("borrowed"),
        _entity_address_sql_settings=lambda: [("work_mem", "8MB")],
        _sql_literal=str,
        logger=None,
        _apply_entity_address_transaction_settings=apply_settings,
        _record_phase_timing=lambda *args: observed.timings.append(args),
        enqueue_live_progress=lambda **event: observed.events.append(event),
        _run_sql_phase=run_phase,
    )
    return sql, observed


def _protocol_phase(*, run_id="synthetic-run"):
    return dict(
        context={}, run_id=run_id, phase="aggregating", unit="run", done=0, total=1, emit_start=True, emit_done=True
    )


async def assert_counted_packet_protocol(packet, error, bound):
    """Malformed accounting must roll back without a success event or timing."""
    sql, observed = _protocol_sql(packet, bound=bound)
    options = _protocol_phase()
    if error:
        with pytest.raises(RuntimeError, match=error):
            await aggregate._run_counted_insert(sql, "INSERT INTO candidate SELECT 'value';", options)
        assert not observed.timings and len(observed.events) == 1
    else:
        assert await aggregate._run_counted_insert(sql, "INSERT INTO candidate SELECT 'value';", options) == packet
        assert len(observed.timings) == 1 and observed.timings[0][3] == packet[0]
        assert observed.timings[0][0] is options["context"] and observed.timings[0][2] >= 0
        assert [event["message"] for event in observed.events] == ["aggregating", f"aggregating: {packet[0]:,} row(s)"]
    kind = "borrowed" if bound else "owned"
    expected_transactions = [(kind, "enter"), (kind, "rollback" if error else "commit")]
    assert observed.transactions == (
        expected_transactions if bound else [expected_transactions[0], ("settings", "apply"), expected_transactions[1]]
    )
    statement, parameters = observed.queries[0]
    assert not parameters and "SELECT count(*)::bigint,count(*) FILTER(WHERE pending)::bigint" in statement
    assert "RETURNING formatted_address_source=" in statement and "SELECT *" not in statement


async def assert_reuse_packet_protocol(packet, error):
    """Refuse stale or interrupted candidate reuse before publication can continue."""
    sql, observed = _protocol_sql(packet)
    if error:
        with pytest.raises(RuntimeError, match=error):
            await aggregate.checked_reused_stage_rows(sql, "candidate", "addresses", "version-two")
    else:
        assert await aggregate.checked_reused_stage_rows(sql, "candidate", "addresses", "version-two") == packet[0]
    assert len(observed.queries) == 1 and not observed.updates and not observed.events
    statement, parameters = observed.queries[0]
    assert parameters == {"base_version": "version-two"}
    assert "base_address_version IS DISTINCT FROM :base_version" in statement
    assert f"formatted_address_source='{batch.PENDING_LABEL_SOURCE}'" in statement


async def assert_direct_completion_protocol(monkeypatch, *, active, admitted):
    """Keep direct admission fallback and skip completion for entirely cached rows."""
    sql, observed = _protocol_sql((2, 0), bound=True, updated=2)
    monkeypatch.setattr(batch, "_can_stage", lambda: not admitted)
    builds, completions = [], []

    def build(source, **options):
        builds.append((source, options))
        return "INSERT INTO candidate SELECT 'value'"

    async def complete(*args, **options):
        completions.append((args, options))

    monkeypatch.setattr(aggregate, "_complete_pending", complete)
    phase = _protocol_phase()
    token = batch._TABLE.set("candidate.raw_fields" if active else None)
    try:
        assert await aggregate.materialize_direct(sql, "candidate", "addresses", "SELECT 'source'", build, phase) == 2
    finally:
        batch._TABLE.reset(token)
    if active and not admitted:
        assert builds == [("SELECT 'source'", {"native_prepared": True, "defer_labels": True})]
        assert phase["context"]["aggregate_label_pending_rows"] == 0
        assert not observed.updates
        assert not completions
    else:
        assert builds == [("SELECT 'source'", {"native_prepared": True})]
        assert len(observed.updates) == 1 and not observed.queries and not completions
        assert "aggregate_label_pending_rows" not in phase["context"]


async def seed_composed_rows(sql, database, schema, *, entities=16, composed=True, unique=False):
    """Make each group choose its longest street and city from different raw rows."""
    names = [field.split()[0] for field in fixtures.source_columns_sql().split(", ")]
    columns = ",".join(
        "('office-'||series.ordinal::text)::varchar AS entity_id" if name == "entity_id" else "source." + name
        for name in names
    )
    source = (
        f"SELECT {columns} FROM ({fixtures.source_select_sql(schema)}) source "
        f"CROSS JOIN generate_series(1,{entities}) series(ordinal) WHERE source.entity_id='office-one'"
    )
    await database.status(sql._insert_raw_from_source_sql(schema, "raw_actual", source))
    house = "split_part(entity_id,'-',2)" if unique else "'10'"
    await database.status(
        f"""UPDATE {schema}.raw_actual SET source_priority=1,
        first_line=({house}||CASE WHEN address_source='nppes' THEN ' Example St' ELSE ' Example Street' END),
        second_line=NULL,city_name=CASE WHEN address_source='nppes' OR NOT :composed THEN 'Long Example City' ELSE 'X' END,
        state_name='TX',postal_code='75001',country_code='US',location_key=entity_id,
        evidence_shard={sql._evidence_group_hash_expr_for_alias("raw_actual", 2)}""",
        composed=composed,
    )


def aggregate_options(*, inline=False, shards=2):
    return dict(
        aggregate_shards=shards, aggregate_concurrency=2, is_address_canon_available=True, inline_source_evidence=inline
    )


async def original_aggregation(sql, database, schema, options, target="direct_expected"):
    """Keep the existing one-statement-per-shard SQL path as the measured baseline."""
    for remainder in range(options["aggregate_shards"]):
        await database.status(
            sql._materialize_from_raw_sql(schema, target, "raw_actual", **aggregate._shard_options(options, remainder))
        )


async def assert_composed_labels_once(monkeypatch, *, inline, split, composed=True):
    """Compare all output fields and reject SQL rendering of accepted final tuples."""
    monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_SPLIT_ARRAY_AGGREGATES", split)
    async with database_fixture(monkeypatch) as (sql, database, schema):
        await seed_composed_rows(sql, database, schema, composed=composed)
        options = aggregate_options(inline=inline)
        await original_aggregation(sql, database, schema, options)
        original_field = batch.field_sql
        original_native = batch.stage.address_batch.prepare_address_batch
        native_sizes = []
        progress_events = []
        context_by_field = {}
        inserted_counts = []
        monkeypatch.setattr(sql, "enqueue_live_progress", lambda **event: progress_events.append(event))

        def reject_sql_label(field, fallback):
            if field == "formatted_address":
                fallback = "first_line::integer::text"
            return original_field(field, fallback)

        def record_native(rows):
            native_sizes.append(len(rows))
            return original_native(rows)

        @batch.scope
        async def materialize():
            await batch.prepare_raw(sql, schema, "raw_actual")
            assert batch._TABLE.get() is not None
            monkeypatch.setattr(batch, "field_sql", reject_sql_label)
            if composed:
                with pytest.raises(Exception, match="invalid input syntax for type integer"):
                    await original_aggregation(sql, database, schema, options, target="direct_actual")
            monkeypatch.setattr(batch.stage.address_batch, "prepare_address_batch", record_native)
            inserted_counts.extend(
                await aggregate.materialize(
                    sql,
                    schema,
                    "direct_actual",
                    "raw_actual",
                    options,
                    dict(context=context_by_field, run_id="synthetic-run"),
                )
            )

        async with ordinary.scope(schema, set()):
            await materialize()
        assert native_sizes == ([1] if composed else [])
        _assert_counted_progress(context_by_field, inserted_counts, progress_events, composed)
        expected = await database.all(f"SELECT * FROM {schema}.direct_expected ORDER BY location_key")
        actual = await database.all(f"SELECT * FROM {schema}.direct_actual ORDER BY location_key")
        assert len(actual) == 16 and [tuple(address) for address in actual] == [tuple(address) for address in expected]
        await fixtures.assert_no_row_scratch(database, schema)
        assert not await database.scalar(
            "SELECT EXISTS(SELECT FROM information_schema.tables "
            "WHERE table_schema=:schema AND table_name LIKE '%_aggregate_%')",
            schema=schema,
        )


def _assert_counted_progress(context_by_field, inserted_counts, progress_events, composed):
    """Keep inserted rows distinct from the pending-only completion count."""
    assert sum(inserted_counts) == 16 and context_by_field["aggregate_label_pending_rows"] == (16 if composed else 0)
    assert context_by_field["phase_timings"]["entity-address-unified aggregating"]["rows"] == 16
    events = [event for event in progress_events if event["phase"] == "entity-address-unified aggregating"]
    assert events[-1]["done"] == events[-1]["total"] == 2
    if composed:
        events = [
            event for event in progress_events if event["phase"] == "entity-address-unified completing aggregate labels"
        ]
        assert events[-1]["done"] == events[-1]["total"] == 16
    else:
        assert not any("aggregate labels" in name for name in context_by_field["phase_timings"])


async def assert_native_null_label_is_complete(monkeypatch):
    """An accepted NULL label is valid metadata, including on explicit stage reuse."""
    async with database_fixture(monkeypatch) as (sql, database, schema):
        await seed_composed_rows(sql, database, schema, entities=2, composed=False)
        await database.status(
            f"UPDATE {schema}.raw_actual SET " + ",".join(name + "=NULL" for name in batch.stage.ADDRESS_FIELDS)
        )
        await original_aggregation(sql, database, schema, aggregate_options())
        context_by_field = {}

        async def reject_completion(*_args):
            raise AssertionError("accepted NULL label must not enter pending completion")

        @batch.scope
        async def materialize():
            await batch.prepare_raw(sql, schema, "raw_actual")
            assert (
                await database.scalar(
                    f"SELECT count(*) FROM {batch._TABLE.get()} WHERE native_fields->'formatted_address'='null'::jsonb"
                )
                == 1
            )
            monkeypatch.setattr(aggregate, "_complete_pending", reject_completion)
            await aggregate.materialize(
                sql,
                schema,
                "direct_actual",
                "raw_actual",
                aggregate_options(),
                dict(context=context_by_field, run_id=""),
            )

        await materialize()
        assert context_by_field["aggregate_label_pending_rows"] == 0
        assert await database.scalar(
            f"SELECT bool_and(formatted_address IS NULL AND formatted_address_source='{batch.ADDRESS_FORMAT_SOURCE}') FROM {schema}.direct_actual"
        )
        version = await database.scalar(f"SELECT base_address_version FROM {schema}.direct_actual LIMIT 1")
        assert await aggregate.checked_reused_stage_rows(sql, schema, "direct_actual", version) == 2


async def assert_invalid_insert_packet_rolls_back(monkeypatch, packet):
    """Reject malformed counts before committing the counted INSERT itself."""
    async with database_fixture(monkeypatch) as (sql, database, schema):
        await seed_composed_rows(sql, database, schema)
        original_read = sql.preparation_admission.read_first

        async def changed_packet(*args, **options):
            await original_read(*args, **options)
            return packet

        @batch.scope
        async def materialize():
            await batch.prepare_raw(sql, schema, "raw_actual")
            monkeypatch.setattr(sql.preparation_admission, "read_first", changed_packet)
            await aggregate.materialize(
                sql, schema, "direct_actual", "raw_actual", aggregate_options(shards=1), dict(context={}, run_id="")
            )

        with pytest.raises(RuntimeError, match="aggregate insert accounting"):
            await materialize()
        assert await database.scalar(f"SELECT count(*) FROM {schema}.direct_actual") == 0


async def assert_pending_preparation_fallback(monkeypatch):
    """Retain exact SQL labels if final native preparation becomes unavailable."""
    async with database_fixture(monkeypatch) as (sql, database, schema):
        await seed_composed_rows(sql, database, schema)
        await original_aggregation(sql, database, schema, aggregate_options())

        async def unavailable(*_args, **_options):
            return None

        @batch.scope
        async def materialize():
            await batch.prepare_raw(sql, schema, "raw_actual")
            previous = batch._TABLE.get()
            monkeypatch.setattr(batch, "prepare_inputs", unavailable)
            await aggregate.materialize(
                sql, schema, "direct_actual", "raw_actual", aggregate_options(), dict(context={}, run_id="")
            )
            assert batch._TABLE.get() == previous

        await materialize()
        expected = await database.all(f"SELECT * FROM {schema}.direct_expected ORDER BY location_key")
        actual = await database.all(f"SELECT * FROM {schema}.direct_actual ORDER BY location_key")
        assert list(map(tuple, actual)) == list(map(tuple, expected))


async def _composed_direct_source(database, schema):
    """Use equal source priority so component aggregation forms an unseen tuple."""
    await database.status(f"""UPDATE {schema}.source_rows SET
        first_line=CASE WHEN address_source='nppes' THEN '10 Example St' ELSE '10 Example Street' END,
        city_name=CASE WHEN address_source='nppes' THEN 'Long Example City' ELSE 'X' END,
        state_name='TX',postal_code='75001',country_code='US',second_line=NULL,
        address_source='provider_directory_fhir' WHERE entity_id='office-one'""")
    return f"SELECT * FROM ({fixtures.source_select_sql(schema)}) source WHERE entity_id='office-one'"


def _observe_direct_transaction(monkeypatch, sql, database):
    """Verify TEMP visibility and restore the direct consumer's borrowed settings."""
    original = aggregate.materialize_direct

    async def run(*args):
        assert database._transaction_binding() is not None
        await database.status("SET LOCAL work_mem='8MB'")
        result = await original(*args)
        assert await database.scalar("SELECT current_setting('work_mem')") == "8MB"
        return result

    async def reject_committed(*_args, **_options):
        raise AssertionError("direct completion must not open a committed cache")

    monkeypatch.setattr(aggregate, "materialize_direct", run)
    monkeypatch.setattr(batch, "prepare_inputs", reject_committed)
    monkeypatch.setattr(sql, "_entity_address_sql_settings", lambda: [("work_mem", "16MB"), ("jit", "off")])


def _fail_direct_pending(monkeypatch, sql, failure):
    """Fail after final cache creation so its real transaction and cleanup run."""
    if failure == "cache":
        original = batch.stage.staged_address_fields

        @asynccontextmanager
        async def stop_cache(*args, **options):
            async with original(*args, **options) as table:
                if f"formatted_address_source='{batch.PENDING_LABEL_SOURCE}'" in args[3]:
                    raise RuntimeError("direct completion stopped")
                yield table

        monkeypatch.setattr(batch.stage, "staged_address_fields", stop_cache)
    else:
        original = sql._run_sql_phase

        async def stop_update(statement, **options):
            if options["phase"] == "entity-address-unified completing aggregate labels":
                raise RuntimeError("direct completion stopped")
            return await original(statement, **options)

        monkeypatch.setattr(sql, "_run_sql_phase", stop_update)


async def assert_direct_composed_labels(monkeypatch, failure):
    """Reuse counted completion for direct TEMP preparation without a second aggregate."""
    async with database_fixture(monkeypatch) as (sql, database, schema):
        source_select = await _composed_direct_source(database, schema)
        await database.status(sql._materialize_sql(schema, "direct_expected", [source_select]))
        original_field, original_native = batch.field_sql, batch.stage.address_batch.prepare_address_batch
        sizes = []

        def reject_sql(field, fallback):
            return original_field(field, "first_line::integer::text" if field == "formatted_address" else fallback)

        def record_native(rows):
            sizes.append(len(rows))
            return original_native(rows)

        async def phase(stage_table=None):
            return await sql._run_prepared_source_phase(
                schema,
                source_select,
                lambda prepared, **options: sql._materialize_sql(schema, "direct_actual", [prepared], **options),
                dict(context={}, phase="materializing synthetic direct source"),
                include_base_address_version=False,
                **({} if stage_table is None else {"stage_table": stage_table}),
            )

        monkeypatch.setattr(batch, "field_sql", reject_sql)
        with pytest.raises(Exception, match="invalid input syntax for type integer"):
            await phase()
        _observe_direct_transaction(monkeypatch, sql, database)
        monkeypatch.setattr(batch.stage.address_batch, "prepare_address_batch", record_native)
        if failure:

            async def stop(*_args, **_options):
                raise RuntimeError("direct completion stopped")

            if failure is True:
                monkeypatch.setattr(aggregate, "_complete_pending", stop)
            else:
                _fail_direct_pending(monkeypatch, sql, failure)
            with pytest.raises(RuntimeError, match="direct completion stopped"):
                await phase("direct_actual")
            assert await database.scalar(f"SELECT count(*) FROM {schema}.direct_actual") == 0
            assert batch._TABLE.get() is None
        else:
            assert await phase("direct_actual") == 1
            assert sizes == [2, 1]
            expected = await database.all(f"SELECT * FROM {schema}.direct_expected")
            actual = await database.all(f"SELECT * FROM {schema}.direct_actual")
            assert list(map(tuple, actual)) == list(map(tuple, expected))
        await fixtures.assert_no_row_scratch(database, schema)


async def assert_bound_aggregation_fallback(monkeypatch):
    """Keep the original single-statement path on a caller-owned transaction."""
    async with database_fixture(monkeypatch) as (sql, database, schema):
        await seed_composed_rows(sql, database, schema)
        options = aggregate_options(shards=1)
        await original_aggregation(sql, database, schema, options)

        @batch.scope
        async def materialize():
            await batch.prepare_raw(sql, schema, "raw_actual")
            assert batch._TABLE.get() is not None
            async with database.transaction():
                await aggregate.materialize(
                    sql, schema, "direct_actual", "raw_actual", options, dict(context={}, run_id="")
                )

        await materialize()
        expected = await database.all(f"SELECT * FROM {schema}.direct_expected ORDER BY location_key")
        actual = await database.all(f"SELECT * FROM {schema}.direct_actual ORDER BY location_key")
        assert [tuple(address) for address in actual] == [tuple(address) for address in expected]


async def assert_aggregate_failure_cleanup(monkeypatch, failure):
    """Keep an interrupted pending candidate unpublished and its caches retired."""
    async with database_fixture(monkeypatch) as (sql, database, schema):
        await seed_composed_rows(sql, database, schema)
        run_phase = sql._run_sql_phase
        seen_statements = []
        await database.status(f"CREATE TABLE {schema}.serving_sentinel(value text)")
        await database.status(f"INSERT INTO {schema}.serving_sentinel VALUES('old')")

        async def fail_insert(statement, **options):
            if options["phase"] == "entity-address-unified completing aggregate labels":
                seen_statements.append(statement)
                if failure == "update":
                    raise RuntimeError("aggregate update stopped")
                return (await run_phase(statement, **options)) - 1
            return await run_phase(statement, **options)

        async def fail_cache(*_args, **_options):
            """Fail only after every counted aggregate INSERT has committed."""
            seen_statements.append("cache")
            raise RuntimeError("aggregate cache stopped")

        @batch.scope
        async def materialize():
            await batch.prepare_raw(sql, schema, "raw_actual")
            if failure == "cache":
                monkeypatch.setattr(batch, "prepare_inputs", fail_cache)
            else:
                monkeypatch.setattr(sql, "_run_sql_phase", fail_insert)
            await aggregate.materialize(
                sql, schema, "direct_actual", "raw_actual", aggregate_options(), dict(context={}, run_id="")
            )

        message = (
            "aggregate pending label accounting differs" if failure == "accounting" else f"aggregate {failure} stopped"
        )
        with pytest.raises(RuntimeError, match=message):
            await materialize()
        assert seen_statements
        if failure != "accounting":
            with pytest.raises(RuntimeError, match="pending aggregate labels"):
                await aggregate.checked_reused_stage_rows(sql, schema, "direct_actual", "version-one")
        assert await database.scalar(f"SELECT value FROM {schema}.serving_sentinel") == "old"
        assert not await database.scalar(
            "SELECT EXISTS(SELECT FROM information_schema.tables WHERE table_schema=:schema "
            "AND (table_name LIKE '%_aggregate_%' OR table_name LIKE '%_address_batch_%'))",
            schema=schema,
        )


async def _wait_counted_insert_sleep(database):
    import asyncio

    async with asyncio.timeout(5):
        while not await database.scalar(
            "SELECT EXISTS(SELECT FROM pg_stat_activity WHERE datname=current_database() "
            "AND state='active' AND wait_event='PgSleep' AND query LIKE :marker)",
            marker="%counted-aggregate-barrier%",
        ):
            await asyncio.sleep(0.01)


def _observe_counted_shard_failure(monkeypatch, database):
    import asyncio

    original_insert, original_sql = aggregate._run_counted_insert, aggregate._counted_insert_sql
    barrier_by_field = dict(tasks=[], finished=[], packets=0, caches=[], dropped=[])

    def packet_sql(statement):
        prefix, returning = original_sql(statement).split("RETURNING ", 1)
        pending = returning.split(" AS pending)", 1)[0]
        expression = (
            f"(SELECT {pending} FROM pg_sleep(30))"
            if barrier_by_field["packets"] == 0
            else "(1/(length(location_key)-length(location_key)))=0"
        )
        barrier_by_field["packets"] += 1
        return "/* counted-aggregate-barrier */ " + prefix + "RETURNING " + returning.replace(pending, expression, 1)

    async def counted_insert(*args):
        task = asyncio.current_task()
        barrier_by_field["tasks"].append(task)
        try:
            if len(barrier_by_field["tasks"]) == 2:
                await _wait_counted_insert_sleep(database)
            return await original_insert(*args)
        finally:
            barrier_by_field["finished"].append(task)

    monkeypatch.setattr(aggregate, "_counted_insert_sql", packet_sql)
    monkeypatch.setattr(aggregate, "_run_counted_insert", counted_insert)
    return barrier_by_field


def _observe_drained_cache_cleanup(monkeypatch, barrier_by_field):
    original_drop = batch.stage._drop_committed_stage

    async def drop(database, table):
        assert len(barrier_by_field["finished"]) == 2
        assert all(task.done() for task in barrier_by_field["tasks"])
        assert await database.scalar(f"SELECT count(*) FROM {barrier_by_field['stage']}") == 0
        assert table in barrier_by_field["caches"]
        await original_drop(database, table)
        barrier_by_field["dropped"].append(table)

    monkeypatch.setattr(batch.stage, "_drop_committed_stage", drop)


async def assert_counted_shard_failure_drains(monkeypatch):
    """Cancel a real INSERT at RETURNING, rolling back before its cache retires."""
    import asyncio

    async with database_fixture(monkeypatch) as (sql, database, schema):
        await seed_composed_rows(sql, database, schema)
        await original_aggregation(sql, database, schema, aggregate_options())
        serving_before = await database.all(f"SELECT * FROM {schema}.direct_expected ORDER BY location_key")
        shard_expression = sql._aggregate_shard_expr("location_key", 2)
        assert await database.scalar(f"SELECT count(DISTINCT {shard_expression}) FROM {schema}.raw_actual") == 2
        barrier_by_field = _observe_counted_shard_failure(monkeypatch, database)
        barrier_by_field["stage"] = f"{schema}.direct_actual"
        _observe_drained_cache_cleanup(monkeypatch, barrier_by_field)
        context_by_field = {}
        publication_events = []

        @batch.scope
        async def materialize():
            await batch.prepare_raw(sql, schema, "raw_actual")
            barrier_by_field["caches"].append(batch._TABLE.get())
            await aggregate.materialize(
                sql,
                schema,
                "direct_actual",
                "raw_actual",
                aggregate_options(),
                dict(context=context_by_field, run_id=""),
            )
            publication_events.append(True)

        try:
            async with asyncio.timeout(10):
                with pytest.raises(Exception, match="division by zero"):
                    await materialize()
        finally:
            for task in barrier_by_field["tasks"]:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*barrier_by_field["tasks"], return_exceptions=True)
        assert not publication_events and "aggregate_label_pending_rows" not in context_by_field
        assert barrier_by_field["packets"] == 2 and barrier_by_field["dropped"] == barrier_by_field["caches"]
        for table in barrier_by_field["caches"]:
            assert await database.scalar("SELECT to_regclass(:table) IS NULL", table=table)
        assert await database.scalar(f"SELECT count(*) FROM {schema}.direct_actual") == 0
        assert not await database.scalar(
            "SELECT EXISTS(SELECT FROM pg_stat_activity WHERE datname=current_database() "
            "AND state='active' AND query LIKE :marker)",
            marker="%counted-aggregate-barrier%",
        )
        serving_after = await database.all(f"SELECT * FROM {schema}.direct_expected ORDER BY location_key")
        assert list(map(tuple, serving_after)) == list(map(tuple, serving_before))
        await fixtures.assert_no_row_scratch(database, schema)
