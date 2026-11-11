# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Attempt-fenced publication into an explicitly selected mutable catalog."""

from __future__ import annotations

import asyncio
import json
from datetime import datetime

from sqlalchemy import text

from db.models import CodeCatalog
from process import code_sets_result_archive as codes
from process import reference_family_archive as native
from process import scoped_catalog_binding as binding
from process import scoped_catalog_publication as publication
from process.control_cancel import ImportCancelledError
from process.import_status_events import isoformat_utc
from process.scoped_catalog_retention import canonical_metadata, generation_value

CONTRACT = "code-sets-mutable-publication.v1"
METRIC = "code_sets_mutable_publication"
MAX_ROWS = 20_000
MAX_BYTES = 16 * 1024**2


async def _mutable_authority(session, schema):
    authority = (
        (
            await session.execute(
                text(
                    "SELECT d.oid AS database_oid,n.oid AS schema_oid,caller.oid AS owner_oid "
                    "FROM pg_catalog.pg_database d JOIN pg_catalog.pg_namespace n ON n.nspname=:schema "
                    "JOIN pg_catalog.pg_roles namespace_owner ON namespace_owner.oid=n.nspowner "
                    "JOIN pg_catalog.pg_roles caller ON caller.rolname=session_user "
                    "JOIN pg_catalog.pg_namespace retention ON retention.nspname='hp_snapshot_retention' "
                    "JOIN pg_catalog.pg_roles protected ON protected.oid=retention.nspowner "
                    "WHERE d.datname=pg_catalog.current_database() AND n.nspowner=d.datdba "
                    "AND session_user=current_user AND caller.rolcanlogin AND NOT caller.rolsuper "
                    "AND NOT caller.rolcreaterole AND NOT caller.rolcreatedb AND NOT caller.rolreplication "
                    "AND NOT caller.rolbypassrls AND NOT namespace_owner.rolcanlogin "
                    "AND NOT namespace_owner.rolsuper AND NOT namespace_owner.rolcreaterole "
                    "AND NOT namespace_owner.rolcreatedb AND NOT namespace_owner.rolreplication "
                    "AND NOT namespace_owner.rolbypassrls AND NOT protected.rolcanlogin "
                    "AND NOT protected.rolsuper AND NOT protected.rolcreaterole AND NOT protected.rolcreatedb "
                    "AND NOT protected.rolreplication AND NOT protected.rolbypassrls "
                    "AND caller.oid<>protected.oid AND n.nspowner<>protected.oid "
                    "AND NOT pg_catalog.pg_has_role(caller.oid,protected.oid,'MEMBER') "
                    "AND NOT pg_catalog.pg_has_role(caller.oid,namespace_owner.oid,'MEMBER') "
                    "AND pg_catalog.current_setting('session_replication_role')='origin'"
                ),
                {"schema": schema},
            )
        )
        .mappings()
        .one_or_none()
    )
    if authority is None:
        raise RuntimeError(
            "mutable code-set publication requires an ordinary login and authenticated mutable namespace"
        )
    return dict(authority)


async def _mutable_storage(session, schema):
    authority = await _mutable_authority(session, schema)
    names = ("code_catalog", codes.TABLE, "import_run")
    oids = tuple([await native._relation_oid(session, schema, name) for name in names])
    if any(type(oid) is not int or oid <= 0 for oid in oids):
        raise RuntimeError("mutable code-set relation inventory is incomplete")
    safe = await session.scalar(
        text(
            "SELECT count(*)=3 AND bool_and(c.relowner=:owner AND c.relnamespace=:schema "
            "AND c.relkind='r' AND c.relpersistence='p' AND NOT c.relispartition "
            "AND NOT c.relrowsecurity AND NOT c.relforcerowsecurity "
            "AND NOT EXISTS(SELECT 1 FROM pg_catalog.aclexplode(COALESCE(c.relacl,"
            "pg_catalog.acldefault('r',c.relowner))) a WHERE a.grantee<>c.relowner "
            "AND (a.is_grantable OR a.privilege_type<>'SELECT')) "
            "AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_attribute col,LATERAL pg_catalog.aclexplode(col.attacl) a "
            "WHERE col.attrelid=c.oid AND a.grantee<>c.relowner AND (a.is_grantable OR a.privilege_type<>'SELECT')) "
            "AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_rewrite WHERE ev_class=c.oid) "
            "AND (c.oid=:control OR NOT EXISTS(SELECT 1 FROM pg_catalog.pg_trigger WHERE tgrelid=c.oid AND NOT tgisinternal)) "
            "AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_constraint WHERE conrelid=c.oid AND contype='f') "
            "AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_inherits WHERE inhrelid=c.oid OR inhparent=c.oid)) "
            "FROM pg_catalog.pg_class c WHERE c.oid=ANY(CAST(:oids AS oid[]))"
        ),
        {"owner": authority["owner_oid"], "schema": authority["schema_oid"], "oids": list(oids), "control": oids[2]},
    )
    if safe is not True:
        raise RuntimeError("mutable code-set relation ownership, access or execution hooks differ")
    # The ordinary control heap retains its installed managed-run safety triggers.
    await native.require_native_read_catalog(session, oids[:2])
    return authority | {"relation_oids": oids}


async def _generation_shape(session, schema, oid):
    columns = (
        await session.execute(
            text(
                "SELECT a.attname,pg_catalog.format_type(a.atttypid,a.atttypmod),a.attnotnull,"
                "a.attgenerated::text,a.attidentity::text,d.oid IS NOT NULL "
                "FROM pg_catalog.pg_attribute a LEFT JOIN pg_catalog.pg_attrdef d "
                "ON d.adrelid=a.attrelid AND d.adnum=a.attnum "
                "WHERE a.attrelid=:oid AND a.attnum>0 AND NOT a.attisdropped ORDER BY a.attnum"
            ),
            {"oid": oid},
        )
    ).all()
    expected = (
        ("id", "smallint", True),
        ("local_lineage_id", "uuid", True),
        ("local_generation", "bigint", True),
        ("origin_lineage_id", "uuid", False),
        ("origin_generation", "bigint", False),
        ("published_at", "timestamp with time zone", False),
        ("code_catalog_oid", "bigint", False),
        ("row_count", "bigint", False),
        ("row_sha256", "text", False),
    )
    if tuple(tuple(column[:3]) for column in columns) != expected or any(
        column[3] or column[4] or column[5] and column[0] != "id" for column in columns
    ):
        raise RuntimeError("mutable code-set generation model differs")
    singleton = await session.scalar(text(f'SELECT count(*)=1 AND min(id)=1 FROM "{schema}".{codes.TABLE}'))
    if singleton is not True:
        raise RuntimeError("mutable code-set generation singleton differs")


async def _run(session, schema, attempt, *, lock="UPDATE", allow_cancel=False):
    row = (
        (await session.execute(text(f'SELECT * FROM "{schema}".import_run WHERE run_id=:run_id FOR {lock}'), attempt))
        .mappings()
        .one_or_none()
    )
    if (
        row is None
        or row["engine"] != "healthcare-mrf-api"
        or row["importer"] != "code-sets"
        or (row["progress"] or {}).get("attempt_id") != attempt["attempt_id"]
        or (row["progress"] or {}).get("attempt_started_at") != attempt["attempt_started_at"]
        or row["error"] is not None
        or (row["metrics"] or {}).get("scoped_catalog_handoff") is not None
    ):
        raise RuntimeError("mutable code-set managed attempt differs")
    if not allow_cancel and row["status"] in {"canceling", "canceled"}:
        raise ImportCancelledError("code-set managed attempt was canceled")
    return row


def _confirmed(row, attempt, storage, expected=None):
    result = (row["metrics"] or {}).get(METRIC)
    if (
        row["status"] != "succeeded"
        or row["phase_detail"] != "code sets published"
        or row["error"] is not None
        or not all(isinstance(row[name], datetime) for name in ("heartbeat_at", "finished_at"))
        or type(result) is not dict
        or result.get("contract") != CONTRACT
        or result.get("attempt") != attempt
        or result.get("storage") != generation_value(storage)
        or result.get("status") != "succeeded"
        or result.get("publication_mode") != "legacy-mutable"
        or expected is not None
        and result != expected
    ):
        raise RuntimeError("mutable code-set committed attempt differs")
    return result


async def _reconcile(database, schema, attempt, storage, expected):
    async with asyncio.timeout(40), database.transaction() as session:
        async with native._nucc_catalog_search_path(session):
            await session.execute(text("SET LOCAL lock_timeout='30s'"))
            await session.execute(text("SET LOCAL statement_timeout='30s'"))
            if await _mutable_storage(session, schema) != storage:
                raise RuntimeError("mutable code-set reconciliation storage changed")
            row = await _run(session, schema, attempt, lock="SHARE", allow_cancel=True)
            if await _mutable_storage(session, schema) != storage:
                raise RuntimeError("mutable code-set reconciliation storage changed")
            if row["status"] == "succeeded":
                _confirmed(row, attempt, storage, expected)
            elif (
                row["status"] not in {"running", "canceling", "canceled"}
                or (row["metrics"] or {}).get(METRIC) is not None
            ):
                raise RuntimeError("mutable code-set commit outcome differs")
            return row


async def _apply_rows(session, schema, payloads, storage):
    prior = await codes.read_generation(session, schema, lock=True)
    incoming = await publication.precreate_catalog_input(session, "code-sets", schema)
    await publication.copy_catalog_records(session, incoming, {"code_catalog": payloads})
    await publication.validate_catalog_semantics(session, incoming.spec, incoming.ownership.schema_name)
    qualified_catalog = f'"{schema}".code_catalog'
    qualified_incoming = f'"{incoming.ownership.schema_name}".code_catalog'
    foreign = await session.scalar(
        text(
            f"SELECT EXISTS(SELECT 1 FROM {qualified_catalog} live JOIN {qualified_incoming} incoming "
            "USING (code_system,code) WHERE live.source IS DISTINCT FROM incoming.source)"
        )
    )
    if foreign:
        raise RuntimeError("code-set foreign key ownership differs")
    columns = tuple(CodeCatalog.__table__.columns.keys())
    names = ",".join(native._quoted(name) for name in columns)
    updates = ",".join(
        f'"{name}"=excluded."{name}"'
        for name in ("display_name", "short_description", "long_description", "is_active", "source", "updated_at")
    )
    await session.execute(
        text(
            f"INSERT INTO {qualified_catalog} ({names}) SELECT {names} FROM {qualified_incoming} WHERE TRUE "
            f"ON CONFLICT (code_system,code) DO UPDATE SET {updates}"
        )
    )
    if await codes.read_generation(session, schema, lock=True) != prior:
        raise RuntimeError("mutable code-set generation changed")
    generation = await codes.publish_local_generation(session, schema)
    if await _mutable_storage(session, schema) != storage:
        raise RuntimeError("mutable code-set storage identity changed")
    await native.cleanup_model_family_stage(session, incoming.spec, incoming.ownership)
    return generation


def _committed(ctx, row):
    result = row["metrics"][METRIC]
    context = ctx.setdefault("context", {})
    context.update(control_run_terminal_committed=True, _control_committed_result=result)
    for field in ("heartbeat_at", "finished_at"):
        context["_control_committed_" + field] = isoformat_utc(row[field], timespec="microseconds")
    return result


def validate_mutable_attempt(database, ctx, schema, test_mode):
    """Reject invalid managed publication requests before database or source I/O."""
    native._schema_name(schema)
    context = (ctx or {}).get("context") or {}
    attempt_by_field = {
        "run_id": context.get("control_run_id") or (ctx or {}).get("control_run_id"),
        "attempt_id": context.get("_control_attempt_id"),
        "attempt_started_at": context.get("_control_attempt_started_at"),
    }
    if (
        ctx is None
        or test_mode
        or any(
            type(identifier) is not str or not 0 < len(identifier) <= 128 for identifier in attempt_by_field.values()
        )
    ):
        raise RuntimeError("mutable code-set publication requires a full managed attempt")
    if database._transaction_binding() is not None or database._reader_binding() is not None:
        raise RuntimeError("mutable code-set publication requires its own transaction")
    return attempt_by_field


def _validate_payloads(payloads):
    if (
        not 0 < len(payloads) <= MAX_ROWS
        or len(json.dumps(payloads, default=str).encode()) > MAX_BYTES
        or {(contribution.get("source"), contribution.get("code_system")) for contribution in payloads}
        != set(codes.SOURCES)
    ):
        raise RuntimeError("mutable code-set input scope or bound differs")


async def capture_mutable_code_sets(database, ctx, *, schema, test_mode=False):
    """Capture source freshness before acquisition, or return the same attempt's completion."""
    attempt = validate_mutable_attempt(database, ctx, schema, test_mode)
    async with asyncio.timeout(60), database.transaction() as session:
        async with native._nucc_catalog_search_path(session):
            storage = await _lock_mutable_storage(session, schema)
            run = await _run(session, schema, attempt)
            if run["status"] == "succeeded" and run["finished_at"] is not None:
                _confirmed(run, attempt, storage)
                return None, _committed(ctx, run)
            if run["status"] != "running" or run["finished_at"] is not None:
                raise RuntimeError("mutable code-set managed attempt is not running")
            generation = await codes.read_generation(session, schema, lock=True)
            return {"storage": generation_value(storage), "generation": generation_value(generation)}, None


async def _lock_mutable_storage(session, schema):
    await session.execute(text("SET LOCAL lock_timeout='2s'"))
    storage = await _mutable_storage(session, schema)
    await native._lock_family(session, schema, ("code_catalog", codes.TABLE), "SHARE ROW EXCLUSIVE", nowait=True)
    await native._lock_family(session, schema, ("import_run",), "ACCESS SHARE", nowait=True)
    if await _mutable_storage(session, schema) != storage:
        raise RuntimeError("mutable code-set storage identity changed")
    if await binding.pin_catalog_source(session, schema, (CodeCatalog,), codes.TABLE) is not False:
        raise RuntimeError("mutable code-set publication cannot target closed catalog storage")
    await _generation_shape(session, schema, storage["relation_oids"][1])
    return storage


def _publication_receipt(attempt, storage, generation, metrics, payload_count):
    return {
        **metrics,
        "status": "succeeded",
        "publication_mode": "legacy-mutable",
        "contract": CONTRACT,
        "attempt": attempt,
        "storage": generation_value(storage),
        "generation": generation_value(generation),
        "terminal_progress": {
            "phase": "code sets published",
            "message": "succeeded",
            "unit": "rows",
            "done": payload_count,
            "total": payload_count,
            "pct": 100,
        },
    }


async def _publish_attempt(session, schema, attempt, storage, payloads, metrics, expected):
    run = await _run(session, schema, attempt)
    if run["status"] == "succeeded" and run["finished_at"] is not None:
        _confirmed(run, attempt, storage)
        return run
    if run["status"] != "running" or run["finished_at"] is not None:
        raise RuntimeError("mutable code-set managed attempt is not running")
    generation = await codes.read_generation(session, schema, lock=True)
    if expected != {"storage": generation_value(storage), "generation": generation_value(generation)}:
        raise RuntimeError("mutable code-set source changed during acquisition")
    generation = await _apply_rows(session, schema, payloads, storage)
    receipt = _publication_receipt(attempt, storage, generation, metrics, len(payloads))
    committed_row = (
        (
            await session.execute(
                text(
                    f"UPDATE \"{schema}\".import_run SET status='succeeded',phase_detail='code sets published',"
                    "heartbeat_at=pg_catalog.timezone('UTC',pg_catalog.clock_timestamp()),"
                    "finished_at=pg_catalog.timezone('UTC',pg_catalog.clock_timestamp()),"
                    "metrics=(COALESCE(metrics::jsonb,'{}'::jsonb)||CAST(:metrics AS jsonb)"
                    "||jsonb_build_object(CAST(:metric AS text),CAST(:result AS jsonb)))::json,"
                    "progress=(COALESCE(progress::jsonb,'{}'::jsonb)||CAST(:progress AS jsonb))::json "
                    "WHERE run_id=:run_id AND status='running' AND finished_at IS NULL "
                    "AND (error IS NULL OR error::jsonb='null'::jsonb) "
                    "AND progress->>'attempt_id'=:attempt_id AND progress->>'attempt_started_at'=:attempt_started_at "
                    "RETURNING *"
                ),
                {
                    **attempt,
                    "metric": METRIC,
                    "result": canonical_metadata(receipt),
                    "metrics": canonical_metadata(metrics),
                    "progress": canonical_metadata(receipt["terminal_progress"]),
                },
            )
        )
        .mappings()
        .one_or_none()
    )
    if committed_row is None:
        raise RuntimeError("mutable code-set completion attempt changed")
    _confirmed(committed_row, attempt, storage, receipt)
    return committed_row


async def _await_reconciliation(database, schema, attempt, storage, expected):
    reconciliation = asyncio.create_task(_reconcile(database, schema, attempt, storage, expected))
    while True:
        try:
            return await asyncio.shield(reconciliation)
        except asyncio.CancelledError:
            if reconciliation.done():
                reconciliation.result()


async def publish_mutable_code_sets(database, ctx, *, schema, payloads, metrics, expected=None, test_mode=False):
    """Publish all three ordinary source slices and their genuine managed attempt atomically."""
    attempt = validate_mutable_attempt(database, ctx, schema, test_mode)
    _validate_payloads(payloads)
    committed_row = None
    try:
        async with asyncio.timeout(60), database.transaction() as session:
            async with native._nucc_catalog_search_path(session):
                storage = await _lock_mutable_storage(session, schema)
                committed_row = await _publish_attempt(session, schema, attempt, storage, payloads, metrics, expected)
    except BaseException:
        if committed_row is None:
            raise
        try:
            observed = await _await_reconciliation(database, schema, attempt, storage, committed_row["metrics"][METRIC])
        except BaseException:
            ctx.setdefault("context", {})["scoped_catalog_commit_unknown"] = True
            raise
        if observed["status"] in {"canceling", "canceled"}:
            raise ImportCancelledError("code-set managed attempt was canceled")
        if observed["status"] != "succeeded":
            raise
        committed_row = observed
    _confirmed(committed_row, attempt, storage)
    task = asyncio.current_task()
    while task is not None and task.cancelling():
        task.uncancel()
    return _committed(ctx, committed_row)
