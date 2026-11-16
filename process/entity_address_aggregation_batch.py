# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Render distinct final aggregate tuples without repeating raw aggregation."""

from __future__ import annotations

import asyncio
import time
from contextlib import asynccontextmanager
from functools import partial
from uuid import uuid4

from db.plan_release_storage import storage_relation
from process import entity_address_batch as batch


def materialize_sql(sql, schema, stage_table, raw_table, options):
    """Keep the existing aggregate/evidence/shard expressions in their original order."""
    dedupe_key = sql._dedupe_key_expr(options["is_address_canon_available"])
    shard_filter = sql._raw_materialize_shard_filter_sql(
        dedupe_key,
        options["checksum_modulo"],
        options["checksum_remainder"],
        inline_source_evidence=options["inline_source_evidence"],
    )
    fragments = sql._raw_array_aggregation_sql(
        schema,
        raw_table,
        dedupe_key,
        shard_filter,
        should_split=sql._is_env_enabled(
            "HLTHPRT_ENTITY_ADDRESS_UNIFIED_SPLIT_ARRAY_AGGREGATES", sql.DEFAULT_SPLIT_ARRAY_AGGREGATES
        ),
    )
    raw_joins, array_columns, array_cte, array_join, array_selects = fragments
    statement = "".join(
        (
            sql._raw_materialize_insert_header_sql(schema, stage_table),
            sql._raw_materialize_insert_tail_sql(),
            sql._raw_materialize_aggregate_identity_sql(),
            sql._raw_materialize_aggregate_address_sql(
                schema,
                raw_table,
                dedupe_key,
                sql._source_record_ids_select_sql(),
                array_columns,
                raw_joins,
                shard_filter,
                array_cte,
            ),
            sql._raw_materialize_select_assurance_sql(array_selects),
            sql._raw_materialize_select_address_sql(schema, array_join, defer_labels=options["defer_labels"]),
        )
    )
    return sql._inline_source_evidence_sql(statement) if options["inline_source_evidence"] else statement


def _shard_options(options, remainder):
    """Preserve the unsharded and inline-evidence partition contracts."""
    options_by_name = {name: options[name] for name in ("is_address_canon_available", "inline_source_evidence")}
    if options["aggregate_shards"] > 1:
        options_by_name.update(checksum_modulo=options["aggregate_shards"], checksum_remainder=remainder)
    return options_by_name


def _phase_options(options, phase, shards):
    return dict(
        options,
        phase=phase,
        unit="shards" if shards > 1 else "run",
        done=0,
        total=shards,
        emit_start=True,
        emit_done=shards == 1,
    )


async def _run_shards(sql, operations, phase_options, concurrency):
    """Drain every projection/cache reader before its enclosing scratch closes."""
    semaphore, lock = asyncio.Semaphore(concurrency), asyncio.Lock()
    progress_by_field = {"done": 0}

    async def execute(ordinal, operation):
        """Bound one shard and preserve phase-local completion accounting."""
        async with semaphore:
            result = await operation(
                dict(
                    phase_options,
                    done=progress_by_field["done"],
                    message=f"{phase_options['phase']}: shard {ordinal + 1}/{len(operations)}",
                )
            )
            if len(operations) > 1 and phase_options.get("run_id"):
                async with lock:
                    progress_by_field["done"] += 1
                    sql.enqueue_live_progress(
                        run_id=phase_options["run_id"],
                        importer="entity-address-unified",
                        status="running",
                        phase=phase_options["phase"],
                        unit="shards",
                        done=progress_by_field["done"],
                        total=len(operations),
                        message=f"completed {progress_by_field['done']}/{len(operations)} shards",
                    )
            return result

    if len(operations) == 1:
        return [await execute(0, operations[0])]
    return await batch.gather(*(execute(ordinal, operation) for ordinal, operation in enumerate(operations)))


async def _run_statement(sql, statement, phase_options):
    return await sql._run_sql_phase(statement, **phase_options)


def _counted_insert_sql(statement):
    """Return only aggregate counts, never retain wide inserted rows."""
    return f"""WITH inserted AS ({statement.strip().removesuffix(";")}
        RETURNING formatted_address_source='{batch.PENDING_LABEL_SOURCE}' AS pending)
        SELECT count(*)::bigint,count(*) FILTER(WHERE pending)::bigint FROM inserted"""


def _emit_packet_progress(sql, options, *, complete=False, inserted_rows=0):
    """Keep the original phase event fields around a counted INSERT."""
    if not options.get("run_id") or not options.get("emit_done" if complete else "emit_start"):
        return
    payload = {name: options.get(name) for name in ("run_id", "phase", "unit", "done", "total", "pct")}
    message = options.get("message") or options["phase"]
    if complete:
        message += f": {inserted_rows:,} row(s)"
    sql.enqueue_live_progress(
        **payload,
        importer="entity-address-unified",
        status="running",
        message=message,
        source="entity-address-unified-sql-progress",
    )


async def _run_counted_insert(sql, statement, phase_options):
    """Retain native tuning and input guards while reading the narrow result packet."""
    started = time.monotonic()
    _emit_packet_progress(sql, phase_options)
    if sql.db._transaction_binding() is not None:
        async with sql.entity_address_tuned_transaction(
            sql.db, sql._entity_address_sql_settings(), sql._sql_literal, sql.logger
        ):
            inserted_rows, pending_rows = await _insert_packet(sql, statement)
    else:
        async with sql.db.transaction():
            await sql._apply_entity_address_transaction_settings()
            inserted_rows, pending_rows = await _insert_packet(sql, statement)
    sql._record_phase_timing(
        phase_options["context"], phase_options["phase"], time.monotonic() - started, inserted_rows
    )
    _emit_packet_progress(sql, phase_options, complete=True, inserted_rows=inserted_rows)
    return inserted_rows, pending_rows


async def _insert_packet(sql, statement):
    """Validate the narrow accounting packet inside its owning transaction."""
    packet = await sql.preparation_admission.read_first(sql.db, _counted_insert_sql(statement))
    if packet is None or len(packet) != 2 or any(type(value) is not int for value in packet):
        raise RuntimeError("aggregate insert accounting is unavailable")
    inserted_rows, pending_rows = tuple(packet)
    if not 0 <= pending_rows <= inserted_rows:
        raise RuntimeError("aggregate insert accounting differs")
    return inserted_rows, pending_rows


def _pending_update_sql(schema, stage_table):
    """Complete only the candidate rows carrying the explicit pending marker."""
    relation = storage_relation(schema, stage_table)
    columns = ",".join(f"target.{name}" for name in batch.stage.ADDRESS_FIELDS)
    fallback = f"{schema}.{batch.ADDRESS_FORMAT_FUNCTION}({columns})"
    table = batch._TABLE.get()
    label = batch.field_sql("formatted_address", fallback)
    from_clause = ""
    joined = ""
    if table is not None:
        from_clause = f"FROM (SELECT lookup_key,native_fields FROM {table}) batch_fields"
        lookup = batch.stage.lookup_key_sql([f"target.{name}" for name in batch.stage.ADDRESS_FIELDS])
        joined = f" AND batch_fields.lookup_key={lookup}"
    return f"""UPDATE {relation} target SET formatted_address={label},
        formatted_address_version={batch.ADDRESS_FORMAT_VERSION},
        formatted_address_source='{batch.ADDRESS_FORMAT_SOURCE}' {from_clause}
        WHERE target.formatted_address_source='{batch.PENDING_LABEL_SOURCE}' {joined}"""


@asynccontextmanager
async def _pending_cache(sql, schema, stage_table, pending_inputs, temporary):
    """Keep direct TEMP preparation on its borrowed backend and raw caches committed."""
    if temporary:
        name = batch.scratch_name(sql, stage_table, "_address_batch_" + uuid4().hex[:12])
        async with batch.stage.staged_address_fields(sql.db, schema, name, pending_inputs, temporary=True) as table:
            token = batch._TABLE.set(table)
            try:
                yield table
            finally:
                batch._TABLE.reset(token)
        return
    prepared = await batch.prepare_inputs(sql, schema, stage_table, pending_inputs, inputs_are_missing=True)
    token = batch._TABLE.set(None) if prepared is None else None
    try:
        yield prepared
    finally:
        if token is not None:
            batch._TABLE.reset(token)


async def _complete_pending(sql, schema, stage_table, pending_rows, phase_options, *, temporary=False):
    """Build one distinct final cache, then verify exact pending-row completion."""
    started = time.monotonic()
    relation = storage_relation(schema, stage_table)
    pending_inputs = f"SELECT {','.join(batch.stage.ADDRESS_FIELDS)} FROM {relation} WHERE formatted_address_source='{batch.PENDING_LABEL_SOURCE}'"
    # The pending marker already proves absence from the unchanged prior cache.
    async with _pending_cache(sql, schema, stage_table, pending_inputs, temporary):
        sql._record_phase_timing(
            phase_options["context"],
            "entity-address-unified preparing aggregate labels",
            time.monotonic() - started,
            None,
        )
        updated = await sql._run_sql_phase(
            _pending_update_sql(schema, stage_table),
            **dict(
                phase_options,
                phase="entity-address-unified completing aggregate labels",
                unit="rows",
                done=0,
                total=pending_rows,
                emit_start=True,
                emit_done=False,
                message="completing pending aggregate labels",
            ),
        )
        if updated != pending_rows:
            raise RuntimeError(f"aggregate pending label accounting differs: expected={pending_rows} updated={updated}")
        if phase_options.get("run_id"):
            sql.enqueue_live_progress(
                run_id=phase_options["run_id"],
                importer="entity-address-unified",
                status="running",
                phase="entity-address-unified completing aggregate labels",
                unit="rows",
                done=updated,
                total=pending_rows,
                message=f"completed {updated:,} pending aggregate labels",
            )


async def checked_reused_stage_rows(sql, schema, stage_table, base_version):
    """Fold reuse counts into one scan and reject an interrupted pending candidate."""
    relation = storage_relation(schema, stage_table)
    packet = await sql.preparation_admission.read_first(
        sql.db,
        f"""SELECT count(*)::bigint,
        count(*) FILTER(WHERE base_address_version IS DISTINCT FROM :base_version)::bigint,
        count(*) FILTER(WHERE formatted_address_source='{batch.PENDING_LABEL_SOURCE}')::bigint FROM {relation}""",
        base_version=base_version,
    )
    if packet is None or len(packet) != 3:
        raise RuntimeError("reused aggregate stage accounting is unavailable")
    if packet[2]:
        raise RuntimeError("reused entity-address stage contains pending aggregate labels; rebuild the isolated stage")
    if packet[1]:
        raise RuntimeError("address alias generation changed; a full entity-address-unified rebuild is required")
    return int(packet[0])


async def materialize(sql, schema, stage_table, raw_table, options, phase_options):
    """Defer only unseen final tuples; already cached labels keep the direct INSERT."""
    shards = options["aggregate_shards"]
    phase = _phase_options(phase_options, "entity-address-unified aggregating", shards)
    native = (
        batch._SCOPE.get() is not None
        and batch._TABLE.get() is not None
        and batch._can_stage()
        and batch.stage.address_batch.is_native_available()
        and all(getattr(sql.db, name, lambda: None)() is None for name in ("_transaction_binding", "_reader_binding"))
    )
    execute = _run_counted_insert if native else _run_statement
    operations = [
        partial(
            execute,
            sql,
            sql._materialize_from_raw_sql(
                schema,
                stage_table,
                raw_table,
                **_shard_options(options, remainder),
                **({"defer_labels": True} if native else {}),
            ),
        )
        for remainder in range(shards)
    ]
    shard_counts = await _run_shards(sql, operations, phase, options["aggregate_concurrency"])
    if not native:
        return shard_counts
    pending_rows = sum(packet[1] for packet in shard_counts)
    phase_options["context"]["aggregate_label_pending_rows"] = pending_rows
    if pending_rows:
        await _complete_pending(sql, schema, stage_table, pending_rows, phase_options)
    return [packet[0] for packet in shard_counts]


async def materialize_direct(sql, schema, stage_table, select_sql, build_sql, phase_options):
    """Complete composed direct tuples using only its current TEMP transaction."""
    if batch._TABLE.get() is None or not batch._can_stage():
        return await sql._run_sql_phase(build_sql(select_sql, native_prepared=True), **phase_options)
    inserted, pending = await _run_counted_insert(
        sql, build_sql(select_sql, native_prepared=True, defer_labels=True), phase_options
    )
    phase_options["context"]["aggregate_label_pending_rows"] = pending
    if pending:
        await _complete_pending(sql, schema, stage_table, pending, phase_options, temporary=True)
    return inserted
