# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded native address canonicalization against a fenced ZIP lookup snapshot."""

import asyncio
import hashlib
import inspect
import json
import sys
from contextlib import asynccontextmanager
from dataclasses import dataclass
from uuid import uuid4

from sqlalchemy import text

from db.plan_release_storage import storage_relation
from process import provider_directory_output_snapshot_custody as custody
from process import provider_directory_output_snapshot_journal as journal
from process import provider_directory_output_snapshot_stage as stages
from process.ext import address_fast
from process.provider_directory_dataset_rehydrate_pages import projected_pages
from process.provider_directory_dataset_rehydrate_pin import (
    _close_reader,
    _require_reader,
    _same_database,
    _SourceReader,
)
from process.provider_directory_generic_source_policy import bound_publisher, source_transaction
from process.provider_directory_output_snapshot_catalog import output_models, require_output
from process.uhc_flex_practitioner_async_safety import drain_operation

TARGET = "provider_directory_location"
GEO = "geo_zip_lookup"
LEDGER = "reference_family_result_generation"
ADDRESS_FIELDS = ("address_key", "zip5", "city_name", "state_name", "state_code", "city_norm", "country_code")


@dataclass(frozen=True)
class _PinnedInputs:
    reader: object
    operation: dict
    incumbent: object
    geo: dict
    decision_at: object
    expected_digest: str
    expected_changed: int
    native: object
    batch_size: int


def _selector(directory, db_schema, run_id, source_ids, seen_table, batch_size):
    """Value filters cannot authorize a scratch target or an ordinary seen heap."""
    require_output(db_schema is None or db_schema == directory._schema(), "schema_changed")
    require_output(seen_table is None, "address_seen_custody_required")
    require_output(
        TARGET not in directory._PROVIDER_DIRECTORY_ARTIFACT_RELATION_OVERRIDES.get(),
        "address_scratch_custody_required",
    )
    require_output(run_id is None or type(run_id) is str and bool(run_id), "address_run_invalid")
    require_output(batch_size is None or type(batch_size) is int and batch_size > 0, "address_batch_invalid")
    return {"run_id": run_id, "source_ids": sorted(set(directory._clean_source_id_list(source_ids))) or None}


def _native_module():
    module = address_fast._fast_module()
    require_output(
        module is not None and inspect.isbuiltin(getattr(module, "canonicalize_location_batch", None)),
        "address_native_required",
    )
    return module


async def _canonical_page(directory, native, rows, batch_size):
    canonical_rows = []
    for offset in range(0, len(rows), batch_size):
        batch_rows = rows[offset : offset + batch_size]
        result_rows = list(
            await asyncio.to_thread(native.canonicalize_location_batch, directory._location_key_inputs(batch_rows))
        )
        require_output(len(result_rows) == len(batch_rows), "address_native_count_mismatch")
        canonical_rows.extend(result_rows)
    return canonical_rows


def address_projection(directory, selector):
    """Resolve the old eligibility and ZIP enrichment using builtins alone."""
    schema = directory._schema()
    raw = "COALESCE(NULLIF(r.state_name,''),r.state_code)"
    state = directory._state_fips_restore_sql(raw)
    country = directory._country_restore_sql("r.country_code")
    needs = " OR ".join(
        [f"r.{field} IS NULL" for field in ("address_key", "zip5", "city_name", "state_code", "city_norm")]
        + [
            f"{raw} ~ '^[0-9]{{1,2}}$'",
            f"(NULLIF({country},'') IS NOT NULL AND r.country_code IS DISTINCT FROM {country})",
        ]
    )
    filters, params = [], {}
    if selector["run_id"] is not None:
        filters.append("r.last_seen_run_id=CAST(:run_id AS varchar)")
        params["run_id"] = selector["run_id"]
    if selector["source_ids"]:
        filters.append("r.source_id=ANY(CAST(:source_ids AS varchar[]))")
        params["source_ids"] = selector["source_ids"]
    eligible = " AND ".join(["(" + needs + ")", *filters])
    statement = f"""WITH inputs AS (
      SELECT r.*,COALESCE(({eligible}),FALSE) AS repair_eligible,{raw} AS raw_state,{state} AS normalized_state,
       {country} AS normalized_country,regexp_replace(COALESCE(r.postal_code,''),'[^0-9]','','g') AS digits
      FROM {storage_relation(schema, TARGET)} r), normalized AS (
      SELECT i.*,CASE WHEN length(digits)>=5 THEN left(digits,5)
       WHEN length(digits) IN (3,4) THEN lpad(digits,5,'0') ELSE NULL END AS computed_zip5 FROM inputs i)
      SELECT n.*,COALESCE(NULLIF(btrim(n.normalized_state),''),g.state) AS resolved_state,
       COALESCE(NULLIF(btrim(n.city_name),''),g.city) AS resolved_city,
       CASE WHEN NULLIF(btrim(COALESCE(n.city_name,'')),'') IS NULL THEN g.city END AS restored_city_name,
       (n.raw_state ~ '^[0-9]{{1,2}}$' OR (NULLIF(btrim(COALESCE(n.raw_state,'')),'') IS NULL
         AND NULLIF(btrim(COALESCE(n.normalized_state,'')),'') IS NULL AND g.state IS NOT NULL)) AS restore_state_name,
       COALESCE(NULLIF(n.normalized_country,''),'US') AS effective_country
      FROM normalized n LEFT JOIN {storage_relation(schema, GEO)} g ON g.zip_code=n.computed_zip5"""
    return statement, params


def repair_address_rows(directory, rows, canonical_rows, decision_at):
    """Preserve every original cell unless the exact old predicate changes a keyable projected_by_field."""
    require_output(len(rows) == len(canonical_rows), "address_native_count_mismatch")
    columns = tuple(str(column.name) for column in output_models()[TARGET].__table__.columns)
    repaired, changed = [], 0
    for original, canonical in zip(rows, canonical_rows, strict=True):
        projected_by_field = {name: original[name] for name in columns}
        require_output(type(original["repair_eligible"]) is bool, "address_eligibility_changed")
        if original["repair_eligible"]:
            resolved_by_field = {**original, **{"old_" + name: projected_by_field[name] for name in ADDRESS_FIELDS}}
            record, has_change = directory._build_location_key_record(resolved_by_field, canonical)
            if has_change:
                for name, value in zip(
                    ADDRESS_FIELDS,
                    (record[2], record[3], record[6], record[7], record[4], record[5], record[8]),
                    strict=True,
                ):
                    projected_by_field[name] = (
                        value if name == "address_key" or value is not None else projected_by_field[name]
                    )
                projected_by_field["updated_at"] = decision_at
                changed += 1
        repaired.append(projected_by_field)
    return repaired, changed


def _digest_rows(digest, rows):
    """Frame typed model tuples identically on application and native candidate rereads."""
    for row in rows:
        payload = json.dumps(row, sort_keys=True, separators=(",", ":"), default=str, allow_nan=False).encode()
        digest.update(len(payload).to_bytes(8, "big"))
        digest.update(payload)


async def _geo_snapshot(session, schema):
    """Require actual current origin, guarded revision and complete native lookup identity."""
    from db.models import GeoZipLookup
    from process import reference_family_archive as archive
    from process.mrf_address_publication import require_native_read_catalog
    from process.reference_family_result_generation import (
        capture_reference_family_serving_generation,
        read_reference_family_result_generation_authority,
    )

    relation = storage_relation(schema, GEO)
    oid = await session.scalar(text("SELECT to_regclass(:relation)::oid::bigint"), {"relation": relation})
    require_output(type(oid) is int and oid > 0, "address_geo_snapshot_required")
    await session.execute(text(f"LOCK TABLE ONLY {relation} IN ACCESS SHARE MODE NOWAIT"))
    await require_native_read_catalog(session, (oid,))
    await _geo_model_shape(session, oid, GeoZipLookup)
    await capture_reference_family_serving_generation(session, importer_id="geo", schema_name=schema)
    authority = await read_reference_family_result_generation_authority(session, importer_id="geo", schema_name=schema)
    require_output(authority.relation_oids == (oid,), "address_geo_changed")
    shape = await archive._table_receipt(session, importer_id="geo", schema_name=schema, model_type=GeoZipLookup)
    catalog = (
        (
            await session.execute(
                text(
                    "SELECT c.relowner::bigint AS owner,c.relfilenode::bigint AS filenode,"
                    "c.relacl::text AS acl,n.oid::bigint AS schema_oid,n.nspowner::bigint AS schema_owner,n.nspacl::text AS schema_acl "
                    "FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.oid=:oid"
                ),
                {"oid": oid},
            )
        )
        .mappings()
        .one()
    )
    return {"oid": oid, "catalog": dict(catalog), "shape": shape.as_dict(), "authority": authority.as_dict()}


async def _geo_model_shape(session, oid, model):
    from sqlalchemy.dialects import postgresql

    expected_columns = [
        (str(column.name), column.type.compile(dialect=postgresql.dialect()).lower(), not column.nullable)
        for column in model.__table__.columns
    ]
    actual = (
        await session.execute(
            text(
                "SELECT attname,format_type(atttypid,atttypmod),attnotnull "
                "FROM pg_attribute WHERE attrelid=:oid AND attnum>0 AND NOT attisdropped ORDER BY attnum"
            ),
            {"oid": oid},
        )
    ).all()
    normalize = lambda value: value.replace("character varying", "varchar").replace("float", "double precision")
    require_output(
        [(name, normalize(kind), required) for name, kind, required in actual]
        == [(name, normalize(kind), required) for name, kind, required in expected_columns],
        "address_geo_shape_changed",
    )
    keys = (
        (
            await session.execute(
                text(
                    "SELECT a.attname FROM pg_constraint c CROSS JOIN LATERAL "
                    "unnest(c.conkey) WITH ORDINALITY k(num,pos) JOIN pg_attribute a ON a.attrelid=c.conrelid AND a.attnum=k.num "
                    "WHERE c.conrelid=:oid AND c.contype='p' AND c.convalidated AND NOT c.condeferrable ORDER BY k.pos"
                ),
                {"oid": oid},
            )
        )
        .scalars()
        .all()
    )
    require_output(keys == ["zip_code"], "address_geo_shape_changed")


async def _require_geo_fence_rights(session, schema):
    """No grant workaround: existing native locks must be available before candidate DDL."""
    relation, ledger = storage_relation(schema, GEO), storage_relation(schema, LEDGER)
    require_output(
        await session.scalar(
            text("SELECT to_regclass(:geo) IS NOT NULL AND to_regclass(:ledger) IS NOT NULL"),
            {"geo": relation, "ledger": ledger},
        ),
        "address_geo_snapshot_required",
    )
    allowed = await session.scalar(
        text(
            "SELECT has_table_privilege(current_user,:geo,'MAINTAIN,UPDATE,DELETE,TRUNCATE') "
            "AND has_table_privilege(current_user,:ledger,'SELECT') AND (has_table_privilege(current_user,:ledger,'UPDATE') "
            "OR has_any_column_privilege(current_user,:ledger,'UPDATE'))"
        ),
        {"geo": relation, "ledger": ledger},
    )
    require_output(allowed is True, "address_geo_cut_fence_required")


async def _lock_geo_cut(capability, expected):
    """Keep mutation/DDL and one native revision row fenced through the original commit."""
    schema, session = capability.policy.schema, capability.session
    await session.execute(text(f"LOCK TABLE ONLY {storage_relation(schema, GEO)} IN SHARE MODE NOWAIT"))
    await session.execute(
        text(f"SELECT importer_id FROM {storage_relation(schema, LEDGER)} WHERE importer_id='geo' FOR SHARE NOWAIT")
    )
    require_output(await _geo_snapshot(session, schema) == expected, "address_geo_changed")


def _address_backfill_operation(incumbent, geo, selector, digest, decision_at):
    """Bind one backfill identity to its exact incumbent, lookup and projected rows."""
    return dict(
        kind="address_key_backfill",
        run_id="address-" + uuid4().hex,
        producer="address-key-v1",
        source_pins=[],
        resource_types=["Location"],
        source_fence_digest=custody._digest(
            {
                "incumbent": incumbent.to_payload(),
                "geo": geo,
                "selector": selector,
                "input_digest": digest.hexdigest(),
                "decision_at": decision_at.isoformat(),
            }
        ),
    )


@asynccontextmanager
async def _pinned_inputs(directory, selector, native, batch_size):
    """Hold the genuine independent native Reader before any output stage is created."""
    from process.provider_directory_output_snapshot_provision import _capture

    require_output(
        directory.db._transaction_binding() is None and not directory.db.has_reader_session(),
        "address_unbound_operation_required",
    )
    database = await directory.db._connect_reader()
    session, reader = database.session_factory(), None
    try:
        async with source_transaction(directory.db, isolation="repeatable read") as owner_session:
            publisher = bound_publisher()
            policy = publisher.policy
            require_output(directory._schema() == policy.schema, "schema_changed")
            catalog_by_target = await _capture(owner_session, policy)
            incumbent = custody._native_receipt(policy, catalog_by_target[TARGET], TARGET)
            await _require_geo_fence_rights(owner_session, policy.schema)
            driver = await _require_reader(session, database, policy)
            await _same_database(publisher.connection, driver)
            await driver.execute(
                f"LOCK TABLE ONLY {storage_relation(policy.schema, TARGET)} IN ACCESS SHARE MODE NOWAIT"
            )
            require_output(
                await driver.fetchval("SELECT to_regclass($1)::oid::bigint", storage_relation(policy.schema, TARGET))
                == incumbent.oid,
                "address_incumbent_changed",
            )
            geo = await _geo_snapshot(session, policy.schema)
            decision_at = await publisher.connection.fetchval("SELECT localtimestamp")
            reader = _SourceReader(session, policy.schema, {})
        projection, params = address_projection(directory, selector)
        digest, expected_digest, expected_changed = hashlib.sha256(), hashlib.sha256(), 0
        async for location_rows in projected_pages(reader, projection, params, output_models()[TARGET]):
            _digest_rows(digest, location_rows)
            canonical = await _canonical_page(directory, native, location_rows, batch_size)
            repaired, changed = repair_address_rows(directory, location_rows, canonical, decision_at)
            _digest_rows(expected_digest, repaired)
            expected_changed += changed
        operation_by_field = _address_backfill_operation(incumbent, geo, selector, digest, decision_at)
        yield _PinnedInputs(
            reader,
            operation_by_field,
            incumbent,
            geo,
            decision_at,
            expected_digest.hexdigest(),
            expected_changed,
            native,
            batch_size,
        )
    finally:
        if reader is not None:
            reader.deactivate()
        await _close_reader(session)


async def _verify_candidate(directory, capability, stage, expected_digest, copied, changed):
    relation, original = storage_relation(stage.schema, stage.relation), storage_relation(stage.schema, stage.target)
    columns = [str(column.name) for column in output_models()[TARGET].__table__.columns]
    preserved = ",".join('"' + name + '"' for name in columns if name not in (*ADDRESS_FIELDS, "updated_at"))
    statement = f"WITH expected AS MATERIALIZED (SELECT {preserved} FROM {original}), actual AS MATERIALIZED (SELECT {preserved} FROM {relation}), difference AS ((SELECT to_jsonb(e) FROM expected e EXCEPT ALL SELECT to_jsonb(a) FROM actual a) UNION ALL (SELECT to_jsonb(a) FROM actual a EXCEPT ALL SELECT to_jsonb(e) FROM expected e)) SELECT NOT EXISTS(SELECT FROM difference)"
    require_output((await capability.session.execute(text(statement))).scalar_one(), "address_parity_failed")
    digest, count = hashlib.sha256(), 0
    async for rows in projected_pages(directory.db, f"SELECT * FROM {relation}", {}, output_models()[TARGET]):
        _digest_rows(digest, rows)
        count += len(rows)
    require_output(count == copied and digest.hexdigest() == expected_digest, "address_parity_failed")
    change = " OR ".join(f'a."{name}" IS DISTINCT FROM b."{name}"' for name in ADDRESS_FIELDS)
    actual = (
        await capability.session.execute(
            text(f"SELECT count(*) FROM {relation} a JOIN {original} b USING(source_id,resource_id) WHERE {change}")
        )
    ).scalar_one()
    require_output(actual == changed, "address_changed_count_mismatch")


async def _fill(directory, inputs, stage, selector):
    projection, params = address_projection(directory, selector)
    digest, copied, changed = hashlib.sha256(), 0, 0
    async for location_rows in projected_pages(inputs.reader, projection, params, output_models()[TARGET]):
        canonical = await _canonical_page(directory, inputs.native, location_rows, inputs.batch_size)
        repaired, page_changed = repair_address_rows(directory, location_rows, canonical, inputs.decision_at)
        _digest_rows(digest, repaired)
        changed += page_changed
        async with custody.output_snapshot_transaction(
            directory, operation=inputs.operation, targets=(TARGET,)
        ) as capability:
            for offset in range(0, len(repaired), inputs.batch_size):
                copied += await stages.copy_output_rows(
                    directory, capability, stage, repaired[offset : offset + inputs.batch_size]
                )
            await journal.record_output_progress(capability, stage, copied)
    async with custody.output_snapshot_transaction(
        directory, operation=inputs.operation, targets=(TARGET,)
    ) as capability:
        await stages.finish_output_indexes(directory, capability, stage)
        require_output(
            digest.hexdigest() == inputs.expected_digest and changed == inputs.expected_changed,
            "address_native_changed",
        )
        await _verify_candidate(directory, capability, stage, inputs.expected_digest, copied, changed)
        await stages.seal_output_stage(capability, stage, expected_rows=copied)
        receipt = await custody.capture_prepared_output(
            capability, TARGET, stage.relation, expected_incumbent=stage.incumbent
        )
        await journal.record_output_prepared(capability, [receipt])
    return receipt, changed


async def _discard(directory, operation, prepared_stages):
    async with custody.output_snapshot_transaction(directory, operation=operation, targets=(TARGET,)) as capability:
        for stage in reversed(prepared_stages):
            if (
                await capability.connection.fetchval(
                    "SELECT to_regclass($1)::oid", storage_relation(stage.schema, stage.relation)
                )
                is not None
            ):
                await stages.discard_output_stage(capability, stage)


async def publish_location_address_keys(
    directory, db_schema=None, *, run_id=None, source_ids=None, seen_table=None, batch_size=None
):
    """Publish one verified immutable replacement; no SQL-function or scratch fallback."""
    from process.provider_directory_artifact_bundle_preparation import apply_prepared_output_bundle
    from process.provider_directory_artifact_scratch_inputs import prepared_scratch_enrichment

    prepared = await prepared_scratch_enrichment(
        directory,
        "address",
        db_schema=db_schema,
        run_id=run_id,
        source_ids=source_ids,
        seen_table=seen_table,
        batch_size=batch_size,
    )
    if prepared is not None:
        return prepared
    selector = _selector(directory, db_schema, run_id, source_ids, seen_table, batch_size)
    native, prepared_stages, operation = _native_module(), [], None
    try:
        async with _pinned_inputs(directory, selector, native, min(batch_size or 512, 512)) as pinned:
            operation = pinned.operation
            if pinned.expected_changed == 0:
                return 0
            async with custody.output_snapshot_transaction(
                directory, operation=operation, targets=(TARGET,)
            ) as capability:
                require_output(
                    (await custody._capture(capability, {TARGET: TARGET}))[TARGET] == pinned.incumbent,
                    "address_incumbent_changed",
                )
                stage = await stages.prepare_output_stage(directory, capability, TARGET)
                prepared_stages.append(stage)
                await journal.record_output_progress(capability, stage, 0)
            receipt, changed = await _fill(directory, pinned, stage, selector)

        async def append_history(capability, retained):
            """Record the protected output journal within the original atomic cut."""
            await journal.append_output_history(capability, [receipt], retained)

        async with custody.output_snapshot_transaction(directory, operation=operation, targets=(TARGET,)) as capability:
            await _lock_geo_cut(capability, pinned.geo)
            await apply_prepared_output_bundle(directory, capability, [receipt], after_output_publish=append_history)
        return changed
    finally:
        original_error = sys.exception()
        try:
            if prepared_stages:
                await drain_operation(_discard(directory, operation, prepared_stages), preserve_cancellation=True)
        except BaseException:
            if original_error is None:
                raise
            original_error.add_note("provider_directory_address_stage_cleanup_refused")
