# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Ordinary catalog preparation and attempt-fenced native publisher handoff."""

from __future__ import annotations

import asyncio
import hashlib
from datetime import datetime
from uuid import UUID

from sqlalchemy import text

from process import reference_family_archive as native
from process import scoped_catalog_publication as publication
from process.entity_address_snapshot_receipt import _projected_row_identity
from process.scoped_catalog_retention import canonical_metadata, generation_value
from process.source_profile_result_archive import _native_control_run

CONTRACT = "scoped-catalog-handoff.v1"
PHASE = "catalog native publication pending"
METRIC = "scoped_catalog_handoff"


def _digest(value):
    return hashlib.sha256(canonical_metadata(value).encode()).hexdigest()


def _ownership_value(ownership):
    return {
        "dataset_id": str(ownership.dataset_id),
        "schema_name": ownership.schema_name,
        "schema_oid": ownership.schema_oid,
        "relation_oids": [list(pair) for pair in ownership.relation_oids],
    }


def _input_from_handoff(handoff):
    spec = publication.input_spec(handoff["importer_id"])
    owned = handoff["input"]
    if type(owned) is not dict or set(owned) != {"dataset_id", "schema_name", "schema_oid", "relation_oids"}:
        raise native.ReferenceFamilyArchiveError("catalog handoff input differs")
    dataset_id = UUID(owned["dataset_id"])
    pairs = tuple(tuple(pair) for pair in owned["relation_oids"])
    if (
        owned["schema_name"] != native.reference_family_stage_schema(dataset_id)
        or type(owned["schema_oid"]) is not int
        or owned["schema_oid"] <= 0
        or tuple(name for name, _ in pairs) != tuple(sorted(spec.table_names))
        or any(type(oid) is not int or oid <= 0 for _, oid in pairs)
        or len({oid for _, oid in pairs}) != len(pairs)
    ):
        raise native.ReferenceFamilyArchiveError("catalog handoff inventory differs")
    return publication.CatalogInput(
        spec,
        native.ReferenceFamilyStageOwnership(
            spec.importer_id,
            dataset_id,
            owned["schema_name"],
            owned["schema_oid"],
            pairs,
            (),
            None,
        ),
    )


def _require_input_receipts(handoff):
    """Keep control JSON typed and closed before authenticating actual native content."""
    tables = handoff["tables"]
    names = publication.input_spec(handoff["importer_id"]).table_names
    if not isinstance(tables, list) or len(tables) != len(names):
        raise native.ReferenceFamilyArchiveError("catalog handoff table set differs")
    for table, name in zip(tables, names, strict=True):
        if (
            type(table) is not dict
            or set(table) != {"table", "row_count", "row_sha256", "schema_sha256"}
            or table["table"] != name
            or type(table["row_count"]) is not int
            or table["row_count"] < 0
            or any(not _is_digest(table[key]) for key in ("row_sha256", "schema_sha256"))
        ):
            raise native.ReferenceFamilyArchiveError("catalog handoff table receipt differs")
    if not _is_digest(handoff["source_contract_sha256"]):
        raise native.ReferenceFamilyArchiveError("catalog handoff source digest differs")


def _is_digest(value):
    return isinstance(value, str) and len(value) == 64 and set(value) <= set("0123456789abcdef")


def validate_catalog_handoff(handoff):
    """Parse only the bounded closed managed-attempt and native input contract."""
    fields = {
        "contract",
        "importer_id",
        "run_id",
        "attempt_id",
        "attempt_started_at",
        "node_id",
        "schema_name",
        "database_oid",
        "import_run_oid",
        "input",
        "input_owner_oid",
        "tables",
        "expected_generations",
        "include_relationships",
        "test_mode",
        "metrics",
        "source_contract_sha256",
        "handoff_sha256",
    }
    if type(handoff) is not dict or set(handoff) != fields or handoff["contract"] != CONTRACT:
        raise native.ReferenceFamilyArchiveError("catalog handoff contract differs")
    native._schema_name(handoff["schema_name"])
    _input_from_handoff(handoff)
    _require_input_receipts(handoff)
    if (
        any(
            not isinstance(handoff[key], str) or not 0 < len(handoff[key]) <= 128
            for key in ("run_id", "attempt_id", "attempt_started_at", "node_id")
        )
        or any(
            type(handoff[key]) is not int or not 0 < handoff[key] < 2**32
            for key in ("database_oid", "import_run_oid", "input_owner_oid")
        )
        or type(handoff["include_relationships"]) is not bool
        or type(handoff["test_mode"]) is not bool
        or handoff["test_mode"]
        or type(handoff["metrics"]) is not dict
        or type(handoff["expected_generations"]) is not dict
        or set(handoff["expected_generations"]) - {"code-sets", "ms-drg"}
        or handoff["importer_id"] not in handoff["expected_generations"]
        or handoff["handoff_sha256"]
        != _digest({key: field_value for key, field_value in handoff.items() if key != "handoff_sha256"})
        or len(canonical_metadata(handoff).encode()) > 65536
    ):
        raise native.ReferenceFamilyArchiveError("catalog handoff identity differs")
    return handoff


async def _input_receipts(session, incoming):
    from process.ms_drg_result_generation import _table_shape

    await native._lock_family(session, incoming.ownership.schema_name, incoming.spec.table_names, "SHARE", nowait=True)
    await native.verify_model_family_stage_ownership(session, incoming.spec, incoming.ownership)
    await native.require_native_read_catalog(session, tuple(oid for _, oid in incoming.ownership.relation_oids))
    receipts = []
    for model in incoming.spec.model_types:
        name = model.__tablename__
        count, digest = await _projected_row_identity(
            session, incoming.ownership.schema_name, name, row_json_sql="pg_catalog.to_jsonb(row_value)"
        )
        receipts.append(
            {
                "table": name,
                "row_count": count,
                "row_sha256": digest,
                "schema_sha256": await _table_shape(session, dict(incoming.ownership.relation_oids)[name], model),
            }
        )
    return receipts


async def record_catalog_handoff(session, ctx, *, importer, schema, incoming, options, metrics):
    """Store the exact real attempt, candidate and incumbent authority before returning."""
    context = (ctx or {}).get("context") or {}
    handoff_by_field = {
        "contract": CONTRACT,
        "importer_id": importer,
        "schema_name": schema,
        "run_id": context.get("control_run_id") or (ctx or {}).get("control_run_id"),
        "attempt_id": context.get("_control_attempt_id"),
        "attempt_started_at": context.get("_control_attempt_started_at"),
    }
    if any(
        not isinstance(handoff_by_field[key], str) or not handoff_by_field[key]
        for key in ("run_id", "attempt_id", "attempt_started_at")
    ):
        raise native.ReferenceFamilyArchiveError("catalog publication requires a real managed attempt")
    run = await _native_control_run(session, handoff_by_field, ("running",))
    await publication._current_family(session, schema, importer, read_only=True)
    previous = await publication._current_generations(session, schema, importer, read_only=True)
    handoff_by_field.update(
        node_id=run["node_id"],
        database_oid=await session.scalar(text("SELECT oid::bigint FROM pg_database WHERE datname=current_database()")),
        import_run_oid=await native._relation_oid(session, schema, "import_run"),
        input=_ownership_value(incoming.ownership),
        input_owner_oid=await session.scalar(
            text("SELECT nspowner FROM pg_namespace WHERE oid=:oid"), {"oid": incoming.ownership.schema_oid}
        ),
        tables=await _input_receipts(session, incoming),
        expected_generations={name: generation_value(generation) for name, generation in previous.items()},
        include_relationships=options["include_relationships"],
        test_mode=options["test_mode"],
        metrics=metrics,
        source_contract_sha256=_digest(
            {"importer": importer, "params": run["params"], "options": options, "metrics": metrics}
        ),
    )
    handoff_by_field["handoff_sha256"] = _digest(handoff_by_field)
    validate_catalog_handoff(handoff_by_field)
    changed = await session.scalar(
        text(
            f"UPDATE {native._quoted(schema)}.import_run SET status='finalizing',phase_detail=:phase,heartbeat_at=clock_timestamp(),"
            f"metrics=(COALESCE(metrics::jsonb,'{{}}'::jsonb)||jsonb_build_object('{METRIC}',CAST(:handoff AS jsonb)))::json "
            "WHERE run_id=:run_id AND status='running' AND finished_at IS NULL "
            "AND progress->>'attempt_id'=:attempt_id AND progress->>'attempt_started_at'=:attempt_started_at "
            f"AND metrics->'{METRIC}' IS NULL RETURNING run_id"
        ),
        {**handoff_by_field, "phase": PHASE, "handoff": canonical_metadata(handoff_by_field)},
    )
    if changed != handoff_by_field["run_id"]:
        raise native.ReferenceFamilyArchiveError("catalog handoff attempt changed")
    return handoff_by_field


async def require_catalog_handoff(session, value, *, canceling=False):
    """Authenticate the actual persisted attempt, parameters, location and locked candidate."""
    handoff = validate_catalog_handoff(value)
    run = await _native_control_run(session, handoff, ("canceling",) if canceling else ("finalizing",))
    options_by_field = {"include_relationships": handoff["include_relationships"], "test_mode": handoff["test_mode"]}
    if (
        (not canceling and run["phase_detail"] != PHASE)
        or run["node_id"] != handoff["node_id"]
        or (run["metrics"] or {}).get(METRIC) != handoff
        or _digest(
            {
                "importer": handoff["importer_id"],
                "params": run["params"],
                "options": options_by_field,
                "metrics": handoff["metrics"],
            }
        )
        != handoff["source_contract_sha256"]
        or await session.scalar(text("SELECT oid::bigint FROM pg_database WHERE datname=current_database()"))
        != handoff["database_oid"]
        or await native._relation_oid(session, handoff["schema_name"], "import_run") != handoff["import_run_oid"]
    ):
        raise native.ReferenceFamilyArchiveError("catalog handoff source or location differs")
    incoming = _input_from_handoff(handoff)
    if await _input_receipts(session, incoming) != handoff["tables"]:
        raise native.ReferenceFamilyArchiveError("catalog handoff candidate changed")
    await native._verify_stage_owner(session, incoming.ownership, handoff["input_owner_oid"])
    return handoff, incoming


def has_catalog_handoff(run):
    """Recognize only the two compiled scoped producers' durable handoff marker."""
    return (
        run.get("importer") in {"code-sets", "ms-drg"}
        and isinstance(run.get("metrics"), dict)
        and run["metrics"].get(METRIC) is not None
    )


async def request_catalog_cancel(session, current):
    """An ordinary caller requests cancellation; only the publisher cleans owned heaps."""
    handoff = validate_catalog_handoff((current["metrics"] or {}).get(METRIC))
    run = await _native_control_run(session, handoff, ("finalizing", "canceling"))
    if (
        run["run_id"] != current["run_id"]
        or run["node_id"] != handoff["node_id"]
        or (run["metrics"] or {}).get(METRIC) != handoff
    ):
        raise native.ReferenceFamilyArchiveError("catalog cancellation attempt differs")
    await session.execute(
        text(
            f"UPDATE {native._quoted(handoff['schema_name'])}.import_run SET status='canceling',"
            "phase_detail='catalog publication cancellation requested',heartbeat_at=clock_timestamp() WHERE run_id=:run_id"
        ),
        {"run_id": handoff["run_id"]},
    )


async def cancel_catalog_handoff(session, value):
    """Clean the exact never-published input and terminalize its locked canceled attempt."""
    await native.protected_publisher_owner(session)
    handoff, incoming = await require_catalog_handoff(session, value, canceling=True)
    await native.cleanup_model_family_stage(session, incoming.spec, incoming.ownership)
    receipt_by_field = {"handoff": handoff, "status": "canceled"}
    changed = await session.scalar(
        text(
            f"UPDATE {native._quoted(handoff['schema_name'])}.import_run SET status='canceled',"
            "phase_detail='catalog preparation abandoned',finished_at=clock_timestamp(),heartbeat_at=clock_timestamp(),"
            "metrics=(metrics::jsonb||jsonb_build_object('scoped_catalog_cancellation',CAST(:receipt AS jsonb)))::json "
            "WHERE run_id=:run_id AND status='canceling' AND finished_at IS NULL "
            "AND progress->>'attempt_id'=:attempt_id AND progress->>'attempt_started_at'=:attempt_started_at "
            f"AND metrics::jsonb->'{METRIC}'=CAST(:handoff AS jsonb) RETURNING run_id"
        ),
        {**handoff, "handoff": canonical_metadata(handoff), "receipt": canonical_metadata(receipt_by_field)},
    )
    if changed != handoff["run_id"]:
        raise native.ReferenceFamilyArchiveError("catalog cancellation attempt changed")
    return receipt_by_field


async def prepare_catalog_handoff(database, ctx, *, importer, schema, payloads, options, metrics):
    """Prepare native heaps and commit finalizing; a stage-only worker never reports success."""
    handoff = None
    if ctx is None:
        return await _prepare_standalone(database, importer, schema, payloads, options, metrics)
    if options["test_mode"]:
        raise native.ReferenceFamilyArchiveError("bounded catalog runs cannot publish a managed source generation")
    try:
        async with database.transaction() as session:
            incoming = await publication.precreate_catalog_input(session, importer, schema)
            await publication.copy_catalog_records(session, incoming, payloads)
            handoff = await record_catalog_handoff(
                session, ctx, importer=importer, schema=schema, incoming=incoming, options=options, metrics=metrics
            )
    except BaseException:
        if handoff is None:
            raise
        try:
            persisted = await _reconcile_catalog_handoff(database, handoff)
        except BaseException:
            ctx.setdefault("context", {})["scoped_catalog_commit_unknown"] = True
            raise
        if not persisted:
            raise
    result_by_field = {**metrics, "status": "finalizing", METRIC: handoff}
    context = ctx.setdefault("context", {})
    context["_control_committed_result"] = result_by_field
    context["control_run_handoff_committed"] = True
    return result_by_field


async def _prepare_standalone(database, importer, schema, payloads, options, metrics):
    """Retain an explicitly unsealed preparation; no stage-only standalone success claim."""
    from process.scoped_catalog_retention import GENERATION_TABLES, copy_generation_authority

    async with database.transaction() as session:
        incoming = await publication.precreate_catalog_input(session, importer, schema)
        await publication.copy_catalog_records(session, incoming, payloads)
        read_only = await publication.binding.pin_catalog_source(
            session, schema, incoming.spec.model_types, GENERATION_TABLES[importer]
        )
        control_schema = native.reference_family_predecessor_schema(incoming.ownership.dataset_id)
        await session.execute(text(f"CREATE SCHEMA {native._quoted(control_schema)}"))
        previous = await copy_generation_authority(session, importer, schema, control_schema, read_only=read_only)
        receipt_by_field = {
            "contract": "scoped-catalog-prepared.v1",
            "importer_id": importer,
            "schema_name": schema,
            "input": _ownership_value(incoming.ownership),
            "tables": await _input_receipts(session, incoming),
            "control_schema": control_schema,
            "control_schema_oid": await native._schema_oid(session, control_schema),
            "generation_oid": await native._relation_oid(session, control_schema, GENERATION_TABLES[importer]),
            "previous_generation": previous,
            "options": options,
            "metrics": metrics,
        }
        receipt_by_field["preparation_sha256"] = _digest(receipt_by_field)
        await session.execute(
            text(
                f"UPDATE {native._quoted(control_schema)}.{GENERATION_TABLES[importer]} SET retained_family=CAST(:receipt AS jsonb) WHERE id=1"
            ),
            {"receipt": canonical_metadata(receipt_by_field)},
        )
    return {**metrics, "status": "prepared", "scoped_catalog_preparation": receipt_by_field}


async def _reconcile_catalog_handoff(database, handoff):
    """Resolve uncertain COMMIT on a fresh locked connection before cleanup or failure."""

    async def _is_handoff_committed():
        async with asyncio.timeout(40), database.transaction() as session:
            await session.execute(text("SET LOCAL lock_timeout='30s'"))
            persisted = await session.scalar(
                text(
                    f"SELECT metrics::jsonb->'{METRIC}' FROM {native._quoted(handoff['schema_name'])}.import_run WHERE run_id=:run_id FOR SHARE"
                ),
                {"run_id": handoff["run_id"]},
            )
            return persisted == handoff

    pending = asyncio.create_task(_is_handoff_committed())
    while True:
        try:
            return await asyncio.shield(pending)
        except asyncio.CancelledError:
            if pending.cancelled():
                raise


async def publish_catalog_handoff(session, candidate, *, source_copy):
    """Publish only the genuine finalizing attempt in its admitted publisher transaction."""
    from process import code_sets_result_archive as codes
    from process import ms_drg_result_generation as drg

    await native.protected_publisher_owner(session)
    handoff, incoming = await require_catalog_handoff(session, candidate)
    importer, schema = handoff["importer_id"], handoff["schema_name"]
    rules = publication.contributions(
        importer, include_relationships=handoff["include_relationships"], upsert=importer == "code-sets"
    )
    prepared = await publication.compose_catalog_family(session, schema, importer, incoming, rules, source_copy)
    if canonical_metadata(
        {name: generation_value(generation) for name, generation in prepared.generations.items()}
    ) != canonical_metadata(handoff["expected_generations"]):
        raise native.ReferenceFamilyArchiveError("catalog handoff predecessor changed")

    async def _publish_generation():
        if importer == "code-sets":
            return await codes.publish_local_generation(session, schema)
        return await drg.publish_local_generation(
            session, schema, include_relationships=handoff["include_relationships"]
        )

    _current, retained = await publication.activate_catalog_family(
        session, prepared, _publish_generation, publication_handoff_sha256=handoff["handoff_sha256"]
    )
    receipt_by_field = {"handoff": handoff, "retained": retained}
    await _finish_catalog_attempt(session, receipt_by_field)
    await native.cleanup_model_family_stage(session, incoming.spec, incoming.ownership)
    return receipt_by_field


async def _finish_catalog_attempt(session, receipt):
    """Publication, retained receipt and terminal status share the caller's COMMIT."""
    handoff = receipt["handoff"]
    changed = await session.scalar(
        text(
            f"UPDATE {native._quoted(handoff['schema_name'])}.import_run SET status='succeeded',phase_detail='catalog published',"
            "finished_at=clock_timestamp(),heartbeat_at=clock_timestamp(),error=NULL,"
            "metrics=(metrics::jsonb||CAST(:metrics AS jsonb)||jsonb_build_object('scoped_catalog_publication',CAST(:receipt AS jsonb)))::json "
            "WHERE run_id=:run_id AND node_id=:node_id AND importer=:importer_id AND engine='healthcare-mrf-api' "
            "AND status='finalizing' AND phase_detail=:phase AND finished_at IS NULL "
            "AND progress->>'attempt_id'=:attempt_id AND progress->>'attempt_started_at'=:attempt_started_at "
            f"AND metrics::jsonb->'{METRIC}'=CAST(:handoff AS jsonb) RETURNING run_id"
        ),
        {
            **handoff,
            "phase": PHASE,
            "handoff": canonical_metadata(handoff),
            "receipt": canonical_metadata(receipt),
            "metrics": canonical_metadata(handoff["metrics"]),
        },
    )
    if changed != handoff["run_id"]:
        raise native.ReferenceFamilyArchiveError("catalog publication attempt changed")


async def _read_outcome_run(session, candidate):
    schema = candidate["schema_name"]
    if (
        await session.scalar(text("SELECT oid::bigint FROM pg_database WHERE datname=current_database()"))
        != candidate["database_oid"]
        or await native._relation_oid(session, schema, "import_run") != candidate["import_run_oid"]
    ):
        raise native.ReferenceFamilyArchiveError("catalog outcome location differs")
    await native.require_native_read_catalog(session, (candidate["import_run_oid"],))
    query = text(f"SELECT * FROM {native._quoted(schema)}.import_run WHERE run_id=:run_id FOR UPDATE")
    run = (await session.execute(query, {"run_id": candidate["run_id"]})).mappings().one_or_none()
    if (
        run is None
        or run["engine"] != "healthcare-mrf-api"
        or run["error"] is not None
        or any(
            run[key] != candidate[field]
            for key, field in (("run_id", "run_id"), ("importer", "importer_id"), ("node_id", "node_id"))
        )
        or any((run["progress"] or {}).get(key) != candidate[key] for key in ("attempt_id", "attempt_started_at"))
        or (run["metrics"] or {}).get(METRIC) != candidate
        or _digest(
            {
                "importer": candidate["importer_id"],
                "params": run["params"],
                "options": {
                    "include_relationships": candidate["include_relationships"],
                    "test_mode": candidate["test_mode"],
                },
                "metrics": candidate["metrics"],
            }
        )
        != candidate["source_contract_sha256"]
    ):
        raise native.ReferenceFamilyArchiveError("catalog outcome attempt differs")
    return run


async def read_catalog_handoff_outcome(session, candidate):
    """Resolve COMMIT only from the real attempt and protected physical publication state."""
    native._require_transaction(session)
    await native.protected_publisher_owner(session)
    validate_catalog_handoff(candidate)
    run = await _read_outcome_run(session, candidate)
    if run["status"] in {"finalizing", "canceling"} and run["finished_at"] is None:
        await require_catalog_handoff(session, candidate, canceling=run["status"] == "canceling")
        return None
    if run["finished_at"] is None:
        raise native.ReferenceFamilyArchiveError("catalog terminal outcome is unfinished")
    if (
        await session.scalar(
            text("SELECT oid FROM pg_catalog.pg_namespace WHERE nspname=:schema"),
            {"schema": candidate["input"]["schema_name"]},
        )
        is not None
    ):
        raise native.ReferenceFamilyArchiveError("catalog terminal input still exists")
    if run["status"] == "canceled" and run["phase_detail"] == "catalog preparation abandoned":
        cancellation_by_field = {"handoff": candidate, "status": "canceled"}
        if run["metrics"].get("scoped_catalog_cancellation") != cancellation_by_field:
            raise native.ReferenceFamilyArchiveError("catalog cancellation receipt differs")
        return cancellation_by_field
    if run["status"] != "succeeded" or run["phase_detail"] != "catalog published":
        raise native.ReferenceFamilyArchiveError("catalog terminal outcome differs")
    receipt = run["metrics"].get("scoped_catalog_publication")
    if (
        not isinstance(receipt, dict)
        or receipt.get("handoff") != candidate
        or any(run["metrics"].get(key) != metric for key, metric in candidate["metrics"].items())
    ):
        raise native.ReferenceFamilyArchiveError("catalog publication receipt differs")
    await require_catalog_publication(session, receipt)
    return receipt


async def require_catalog_publication(session, receipt):
    """Bind durable retained authority to the exact logical source still serving natively."""
    from process.scoped_catalog_retention import require_retained_catalog, validate_retained_catalog

    native._require_transaction(session)
    if type(receipt) is not dict or set(receipt) != {"handoff", "retained"}:
        raise native.ReferenceFamilyArchiveError("catalog publication contract differs")
    candidate = validate_catalog_handoff(receipt["handoff"])
    importer, schema = candidate["importer_id"], candidate["schema_name"]
    retained = validate_retained_catalog(receipt["retained"], importer=importer, live_schema=schema)
    if retained["publication_handoff_sha256"] != candidate["handoff_sha256"]:
        raise native.ReferenceFamilyArchiveError("catalog publication handoff differs")
    if canonical_metadata(retained["previous_generations"]) != canonical_metadata(candidate["expected_generations"]):
        raise native.ReferenceFamilyArchiveError("catalog publication predecessor differs")
    await require_retained_catalog(session, retained)
    spec, incumbent, owner = await publication._current_family(session, schema, importer)
    if owner != retained["owner_oid"]:
        raise native.ReferenceFamilyArchiveError("catalog publication owner differs")
    for entry in retained["tables"]:
        await publication.require_preserved_catalog_schema(
            session,
            (retained["schema_name"], entry["relation_oid"]),
            (schema, dict(incumbent.relation_oids)[entry["table"]]),
        )
    await _require_published_origin(session, candidate, retained)
    return receipt


async def _require_published_origin(session, candidate, retained):
    from process import code_sets_result_archive as codes
    from process import scoped_catalog_binding as binding

    importer, schema = candidate["importer_id"], candidate["schema_name"]
    installed_by_field = dict(retained["current_generation"])
    previous = candidate["expected_generations"][importer]
    if (
        type(installed_by_field["local_generation"]) is not int
        or type(installed_by_field["origin_generation"]) is not int
        or type(previous["local_generation"]) is not int
        or installed_by_field["local_lineage_id"] != previous["local_lineage_id"]
        or installed_by_field["local_generation"] != previous["local_generation"] + 1
        or installed_by_field["origin_lineage_id"] != installed_by_field["local_lineage_id"]
        or installed_by_field["origin_generation"] != installed_by_field["local_generation"]
    ):
        raise native.ReferenceFamilyArchiveError("catalog ordinary publication origin differs")
    installed_by_field["published_at"] = datetime.fromisoformat(installed_by_field["published_at"])
    generation_oid = await binding._lock_generation(session, schema, retained["generation_table"], required=True)
    await binding.require_closed_catalog_binding(session, schema, ((retained["generation_table"], generation_oid),))
    if importer == "code-sets":
        shape = await codes._column_signature(session, retained["tables"][0]["relation_oid"])
        await binding.require_code_sets_binding(
            session, schema, codes._generation(installed_by_field), schema_sha256=codes._schema_digest(shape)
        )
    else:
        if installed_by_field["include_relationships"] is not candidate["include_relationships"]:
            raise native.ReferenceFamilyArchiveError("catalog publication relationship option differs")
        for key in ("local_lineage_id", "origin_lineage_id"):
            installed_by_field[key] = UUID(installed_by_field[key])
        await binding.require_ms_drg_binding(session, schema, installed_by_field)
