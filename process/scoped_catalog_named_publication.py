# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Managed named candidates for the two compiled shared-catalog producers."""

from __future__ import annotations

import hashlib
from uuid import UUID, uuid4

from sqlalchemy import text

from process import reference_family_archive as native
from process import scoped_catalog_named_generation as generations
from process import scoped_catalog_named_storage as storage
from process import scoped_catalog_publication as publication
from process.scoped_catalog_layout import DATA_SCHEMA, STAGE_ROLE, catalog_schema, control_schema
from process.scoped_catalog_retention import canonical_metadata

CONTRACT = "scoped-catalog-handoff.v2"
METRIC = "scoped_catalog_handoff"
PHASE = "catalog native publication pending"
_CONTEXT_EXPECTED = "_scoped_catalog_expected"
_ATTEMPT_FIELDS = ("run_id", "attempt_id", "attempt_started_at", "node_id")
IMPORTERS = frozenset(("code-sets", "ms-drg"))

_HANDOFF_FIELDS = frozenset(
    {
        "contract",
        "importer_id",
        "schema_name",
        *_ATTEMPT_FIELDS,
        "input",
        "tables",
        "expected_generations",
        "include_relationships",
        "test_mode",
        "metrics",
        "source_contract_sha256",
        "handoff_sha256",
    }
)


def digest(value):
    """Hash canonical logical metadata without lossy value conversion."""
    return hashlib.sha256(canonical_metadata(value).encode()).hexdigest()


def attempt_from_context(ctx):
    """Extract the persisted control attempt selectors required by managed preparation."""
    context = (ctx or {}).get("context") or {}
    attempt_by_field = {
        "run_id": context.get("control_run_id") or (ctx or {}).get("control_run_id"),
        "attempt_id": context.get("_control_attempt_id"),
        "attempt_started_at": context.get("_control_attempt_started_at"),
    }
    if any(type(value) is not str or not 0 < len(value) <= 128 for value in attempt_by_field.values()):
        raise RuntimeError("catalog publication requires a managed attempt")
    return attempt_by_field


async def _run(session, schema, attempt, *, states, lock=True):
    record = (
        (
            await session.execute(
                text(
                    f"SELECT * FROM {storage.qualified(control_schema(schema), 'import_run')} WHERE run_id=:run_id"
                    + (" FOR UPDATE" if lock else "")
                ),
                {"run_id": attempt["run_id"]},
            )
        )
        .mappings()
        .one_or_none()
    )
    if (
        record is None
        or record["engine"] != "healthcare-mrf-api"
        or record["importer"] not in IMPORTERS
        or record["status"] not in states
        or record["error"] is not None
        or (record["progress"] or {}).get("attempt_id") != attempt["attempt_id"]
        or (record["progress"] or {}).get("attempt_started_at") != attempt["attempt_started_at"]
        or attempt.get("node_id", record["node_id"]) != record["node_id"]
    ):
        raise RuntimeError("managed catalog attempt changed")
    return record


async def capture_expected(database, ctx, *, importer, schema):
    """Capture the full logical family before acquisition without holding network-time locks."""
    if ctx is None:
        return None
    attempt = attempt_from_context(ctx)
    if importer not in IMPORTERS:
        raise RuntimeError("catalog producer is unsupported")
    schema = catalog_schema(schema)
    async with database.transaction() as session:
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
        run = await _run(
            session, schema, attempt, states=("running", "finalizing", "canceling", "succeeded", "canceled"), lock=False
        )
        if run["importer"] != importer:
            raise RuntimeError("catalog acquisition producer changed")
        existing = (run["metrics"] or {}).get(METRIC)
        if existing is not None:
            existing = validate_handoff(existing)
            if (
                any(existing[name] != field_value for name, field_value in attempt.items())
                or existing["node_id"] != run["node_id"]
            ):
                raise RuntimeError("catalog acquisition attempt changed")
            if run["status"] not in {"finalizing", "canceling", "succeeded", "canceled"}:
                raise RuntimeError("catalog acquisition handoff status changed")
            result_by_field = {**existing["metrics"], "status": run["status"], METRIC: existing}
            ctx.setdefault("context", {}).update(
                control_run_handoff_committed=True, _control_committed_result=result_by_field
            )
            return None
        if run["status"] != "running" or run["finished_at"] is not None:
            raise RuntimeError("catalog acquisition attempt is not running")
        models = await storage.current_models(session, schema)
        vector = await generations.read_generations(session, schema, importer=importer)
        await generations.validate_published_content(session, schema, vector)
        captured_by_field = {"generations": vector, "tables": await storage.table_receipts(session, schema, models)}
    ctx.setdefault("context", {})[_CONTEXT_EXPECTED] = captured_by_field
    return captured_by_field


async def _prepare_input(session, importer, schema, payloads):
    spec = publication.input_spec(importer)
    if set(payloads) != set(spec.table_names):
        raise RuntimeError("catalog input family differs")
    previous_role = None
    if schema == DATA_SCHEMA:
        previous_role = await session.scalar(text("SELECT current_user"))
        await session.execute(text(f"SET LOCAL ROLE {native._quoted(STAGE_ROLE)}"))
    dataset_id = uuid4()
    stage = await storage.create_stage(session, schema, spec.model_types, dataset_id)
    for model in spec.model_types:
        columns = tuple(model.__table__.columns.keys())
        model_rows = payloads[model.__tablename__]
        if any(set(row_payload) - set(columns) for row_payload in model_rows):
            raise RuntimeError("catalog input columns differ")
        for start in range(0, len(model_rows), 5000):
            batch_records = [
                tuple(row_payload.get(name) for name in columns) for row_payload in model_rows[start : start + 5000]
            ]
            await native.native_copy_record_batch(
                session,
                model,
                schema_name=stage,
                table_name=model.__tablename__,
                columns=columns,
                records=batch_records,
            )
    await storage.complete_stage(session, stage, spec.model_types)
    await publication.validate_catalog_semantics(session, spec, stage)
    receipts = await storage.table_receipts(session, stage, spec.model_types)
    if previous_role is not None:
        await session.execute(text(f"SET LOCAL ROLE {native._quoted(previous_role)}"))
    return {"dataset_id": str(dataset_id), "schema_name": stage}, receipts


def validate_tables(value, names):
    """Require the fixed table inventory and typed data and schema digests."""
    if type(value) is not list or len(value) != len(names):
        raise RuntimeError("catalog table receipt inventory differs")
    for entry, name in zip(value, names, strict=True):
        if (
            type(entry) is not dict
            or set(entry) != {"table", "row_count", "row_sha256", "schema_sha256"}
            or entry["table"] != name
            or type(entry["row_count"]) is not int
            or entry["row_count"] < 0
            or any(
                type(entry[key]) is not str or len(entry[key]) != 64 or set(entry[key]) - set("0123456789abcdef")
                for key in ("row_sha256", "schema_sha256")
            )
        ):
            raise RuntimeError("catalog table receipt differs")


def _require_input_name(field_value):
    incoming = field_value["input"]
    input_fields = {"dataset_id", "schema_name"}
    if field_value["contract"] != CONTRACT:
        input_fields |= {"schema_oid", "relation_oids"}
    if type(incoming) is not dict or set(incoming) != input_fields:
        raise RuntimeError("catalog handoff input fields differ")
    identifier = UUID(incoming["dataset_id"])
    if not identifier.int or incoming["schema_name"] != native.reference_family_stage_schema(identifier):
        raise RuntimeError("catalog input name differs")
    if incoming["schema_name"] == field_value["schema_name"]:
        raise RuntimeError("catalog input overlaps serving data")


def validate_handoff(field_value):
    """Parse logical source/attempt fields; historical physical fields confer no authority."""
    fields = _HANDOFF_FIELDS
    if type(field_value) is not dict or field_value.get("contract") not in {CONTRACT, "scoped-catalog-handoff.v1"}:
        raise RuntimeError("catalog handoff contract differs")
    extra = (
        {"expected_tables"}
        if field_value["contract"] == CONTRACT
        else {"database_oid", "import_run_oid", "input_owner_oid"}
    )
    if set(field_value) != fields | extra or field_value.get("importer_id") not in IMPORTERS:
        raise RuntimeError("catalog handoff producer or fields differ")
    for name in _ATTEMPT_FIELDS:
        if type(field_value.get(name)) is not str or not 0 < len(field_value[name]) <= 128:
            raise RuntimeError("catalog handoff attempt is invalid")
    native._schema_name(field_value["schema_name"])
    _require_input_name(field_value)
    expected = field_value["expected_generations"]
    if type(expected) is not dict or not set(expected) <= IMPORTERS or field_value["importer_id"] not in expected:
        raise RuntimeError("catalog handoff generation vector differs")
    for generation in expected.values():
        generations.logical_generation(generation)
    if (
        field_value.get("test_mode") is not False
        or type(field_value.get("include_relationships")) is not bool
        or type(field_value["metrics"]) is not dict
    ):
        raise RuntimeError("catalog handoff options differ")
    validate_tables(field_value["tables"], publication.input_spec(field_value["importer_id"]).table_names)
    if (
        type(field_value["source_contract_sha256"]) is not str
        or len(field_value["source_contract_sha256"]) != 64
        or set(field_value["source_contract_sha256"]) - set("0123456789abcdef")
    ):
        raise RuntimeError("catalog source digest differs")
    if field_value["contract"] == CONTRACT:
        names = tuple(
            model.__tablename__
            for model in (storage.MODELS if len(field_value["expected_tables"]) == 3 else storage.MODELS[:1])
        )
        validate_tables(field_value["expected_tables"], names)
        if field_value.get("handoff_sha256") != digest(
            {key: field_value for key, field_value in field_value.items() if key != "handoff_sha256"}
        ):
            raise RuntimeError("catalog handoff source content differs")
    if len(canonical_metadata(field_value).encode()) > 65536:
        raise RuntimeError("catalog handoff exceeds its bound")
    return field_value


async def input_receipts(session, handoff):
    """Verify old prepared inputs with their original logical schema encoding."""
    spec = publication.input_spec(handoff["importer_id"])
    schema = handoff["input"]["schema_name"]
    receipts = await storage.table_receipts(session, schema, spec.model_types)
    if handoff["contract"] == "scoped-catalog-handoff.v1":
        from process.ms_drg_result_generation import _table_shape

        for receipt, model in zip(receipts, spec.model_types, strict=True):
            oid = await native._relation_oid(session, schema, model.__tablename__)
            receipt["schema_sha256"] = await _table_shape(session, oid, model)
    return receipts


async def _record_handoff(session, schema, attempt, handoff_by_field):
    """Attach the validated input only to its still-running attempt in this transaction."""
    changed = await session.scalar(
        text(
            f"UPDATE {storage.qualified(control_schema(schema), 'import_run')} SET status='finalizing',phase_detail=:phase,"
            "heartbeat_at=timezone('UTC',clock_timestamp()),metrics=(COALESCE(metrics::jsonb,'{}'::jsonb)||"
            "jsonb_build_object('scoped_catalog_handoff',CAST(:handoff AS jsonb)))::json "
            "WHERE run_id=:run_id AND status='running' AND finished_at IS NULL "
            "AND progress->>'attempt_id'=:attempt_id AND progress->>'attempt_started_at'=:attempt_started_at RETURNING run_id"
        ),
        {**attempt, "phase": PHASE, "handoff": canonical_metadata(handoff_by_field)},
    )
    if changed != attempt["run_id"]:
        raise RuntimeError("catalog preparation attempt changed")


async def _recover_preparation(database, ctx, handoff_by_field):
    """Reconcile an ambiguous COMMIT without letting the ordinary worker retry it."""
    from process.scoped_catalog_handoff import _reconcile_catalog_handoff

    try:
        return await _reconcile_catalog_handoff(database, handoff_by_field)
    except BaseException:
        ctx.setdefault("context", {})["scoped_catalog_commit_unknown"] = True
        raise


async def prepare(database, ctx, *, importer, schema, payloads, options, metrics):
    """Commit one validated input and its managed continuation without publishing live rows."""
    if importer not in IMPORTERS:
        raise RuntimeError("catalog producer is unsupported")
    schema = catalog_schema(schema)
    if ctx is None:
        async with database.transaction() as session:
            incoming, tables = await _prepare_input(session, importer, schema, payloads)
        return {**metrics, "status": "prepared", "scoped_catalog_preparation": {"input": incoming, "tables": tables}}
    if options["test_mode"]:
        raise RuntimeError("bounded catalog runs cannot publish a managed generation")
    attempt = attempt_from_context(ctx)
    expected = ctx.get("context", {}).get(_CONTEXT_EXPECTED)
    if type(expected) is not dict:
        raise RuntimeError("catalog generation must be captured before acquisition")
    handoff_by_field = None
    try:
        async with database.transaction() as session:
            run = await _run(session, schema, attempt, states=("running",))
            if (
                run["importer"] != importer
                or run["finished_at"] is not None
                or (run["metrics"] or {}).get(METRIC) is not None
            ):
                raise RuntimeError("catalog preparation attempt changed")
            incoming, tables = await _prepare_input(session, importer, schema, payloads)
            handoff_by_field = {
                "contract": CONTRACT,
                "importer_id": importer,
                "schema_name": schema,
                **attempt,
                "node_id": run["node_id"],
                "input": incoming,
                "tables": tables,
                "expected_generations": expected["generations"],
                "expected_tables": expected["tables"],
                "include_relationships": options["include_relationships"],
                "test_mode": False,
                "metrics": metrics,
                "source_contract_sha256": digest(
                    {"importer": importer, "params": run["params"], "options": options, "metrics": metrics}
                ),
            }
            handoff_by_field["handoff_sha256"] = digest(handoff_by_field)
            validate_handoff(handoff_by_field)
            await _record_handoff(session, schema, attempt, handoff_by_field)
    except BaseException:
        if handoff_by_field is None or not await _recover_preparation(database, ctx, handoff_by_field):
            raise
    result_by_field = {**metrics, "status": "finalizing", METRIC: handoff_by_field}
    ctx.setdefault("context", {}).update(control_run_handoff_committed=True, _control_committed_result=result_by_field)
    return result_by_field


async def publish_managed_attempt(session, attempt, settings, *, source_copy):
    """Read the actual managed handoff; command input never supplies tables or publication JSON."""
    request_by_field = {name: getattr(attempt, name) for name in _ATTEMPT_FIELDS}
    run = await _run(
        session,
        settings.schema,
        request_by_field,
        states=("finalizing", "canceling", "canceled", "succeeded"),
        lock=False,
    )
    handoff = validate_handoff((run["metrics"] or {}).get(METRIC))
    if handoff["schema_name"] != settings.schema or handoff["importer_id"] != run["importer"]:
        raise RuntimeError("catalog continuation source differs")
    for name in _ATTEMPT_FIELDS:
        if handoff[name] != request_by_field[name]:
            raise RuntimeError("catalog continuation attempt differs")
    generation = generations.logical_generation(handoff["expected_generations"][run["importer"]])
    if (generation["local_lineage_id"], generation["local_generation"]) != (
        attempt.expected_lineage_id,
        attempt.expected_generation,
    ):
        raise RuntimeError("catalog controller generation differs")
    if run["status"] in {"succeeded", "canceled"}:
        from process.scoped_catalog_handoff import read_catalog_handoff_outcome

        await read_catalog_handoff_outcome(session, handoff)
        return {"status": run["status"], "run_id": run["run_id"], "replayed": True}
    from process.scoped_catalog_named_cutover import cancel, publish

    if run["status"] == "canceling":
        await cancel(session, run, handoff, settings)
        return {"status": "canceled", "run_id": run["run_id"], "replayed": False}
    published_generation = await publish(session, run, handoff, settings, source_copy=source_copy)
    return {"status": "succeeded", "run_id": run["run_id"], "generation": published_generation, "replayed": False}
