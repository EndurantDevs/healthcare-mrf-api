# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Atomic, name-based continuation of the fixed shared-catalog producers."""

from __future__ import annotations

import asyncio
from datetime import datetime
from types import SimpleNamespace
from uuid import uuid4

from sqlalchemy import text

from process import reference_family_archive as native
from process import reference_family_composition as composition
from process import scoped_catalog_named_generation as generations
from process import scoped_catalog_named_retention as retention
from process import scoped_catalog_named_storage as storage
from process import scoped_catalog_publication as publication
from process.scoped_catalog_retention import canonical_metadata


async def _require_input(session, handoff, settings, *, nowait=True):
    schema = handoff["input"]["schema_name"]
    from process.scoped_catalog_named_publication import input_receipts

    spec = publication.input_spec(handoff["importer_id"])
    await storage.protect_tables(session, schema, spec.model_types, settings, candidate=True, nowait=nowait)
    await storage.require_inventory(session, schema, spec.table_names)
    if await input_receipts(session, handoff) != handoff["tables"]:
        raise RuntimeError("catalog prepared source content or structure differs")
    await publication.validate_catalog_semantics(session, spec, schema)
    return spec


async def _require_current(session, handoff, models, *, lock=False):
    schema, importer = handoff["schema_name"], handoff["importer_id"]
    await storage.require_tables(session, schema, models, lock="ACCESS EXCLUSIVE" if lock else "SHARE")
    vector = await generations.read_generations(session, schema, lock=True, importer=importer)
    expected_by_field = generations.compatible_expected_generations(
        vector, handoff["expected_generations"], contract=handoff.get("contract"), importer=importer
    )
    if vector != expected_by_field:
        raise RuntimeError("catalog complete source generation vector changed")
    await generations.validate_published_content(session, schema, vector)
    receipts = await storage.table_receipts(session, schema, models)
    if handoff.get("expected_tables") is not None and receipts != handoff["expected_tables"]:
        raise RuntimeError("catalog predecessor data or structure changed")
    return vector, receipts


async def _composition_inputs(session, live, incoming, models, rules):
    """Copy the declared legacy field and supply NULL only for older prepared inputs."""
    from process.scoped_catalog_binding import catalog_projection_model

    by_name = {rule.model_type.__tablename__: rule for rule in rules}
    projected_models = tuple([await catalog_projection_model(session, live, model) for model in models])
    projections = []
    for model, projected in zip(models, projected_models, strict=True):
        incoming_relation = None
        if projected is not model:
            incoming_model = await catalog_projection_model(session, incoming, model)
            if incoming_model is model:
                incoming_relation = f'SELECT *,NULL::integer AS "effective_year" FROM {storage.qualified(incoming, model.__tablename__)}'
                incoming_relation = "(" + incoming_relation + ")"
        projections.append(
            composition._composition_projection(
                projected, live, incoming, by_name.get(model.__tablename__), incoming_relation=incoming_relation
            )
        )
    return projected_models, projections


async def _compose(session, handoff, models, source_copy, *, rules=None):
    if not isinstance(source_copy, native.ReferenceFamilySourceCopy):
        raise RuntimeError("catalog bounded native COPY is required")
    live, incoming = handoff["schema_name"], handoff["input"]["schema_name"]
    identifier = uuid4()
    schema = await storage.create_stage(session, live, models, identifier)
    rules = rules or publication.contributions(
        handoff["importer_id"],
        include_relationships=handoff["include_relationships"],
        upsert=handoff["importer_id"] == "code-sets",
    )
    incumbent = SimpleNamespace(schema_name=live)
    for rule in rules:
        await composition._validate_composition_scope(session, rule, incumbent, incoming)
    projected_models, projections = await _composition_inputs(session, live, incoming, models, rules)
    async with asyncio.timeout(source_copy.timeout) as deadline:
        remaining = source_copy.max_bytes
        for model, (projection, parameters) in zip(projected_models, projections, strict=True):
            if "run_id" in handoff:
                await _require_pending(session, handoff)
            remaining = await native._copy_source_projection(
                session,
                source_copy,
                text(projection).bindparams(**parameters),
                schema,
                model.__tablename__,
                tuple(model.__table__.columns.keys()),
                remaining,
                deadline.when(),
            )
            if "run_id" in handoff:
                await _require_pending(session, handoff)
        await storage.complete_stage(session, schema, models)
        for model, (projection, parameters) in zip(projected_models, projections, strict=True):
            if not await composition._is_model_projection_equal(
                session,
                model,
                f"({projection})",
                storage.qualified(schema, model.__tablename__),
                parameters,
            ):
                raise RuntimeError("catalog composed rows differ")
    spec = native.ReferenceFamilySpec("scoped-catalog", models)
    await publication.validate_catalog_semantics(session, spec, schema)
    for model in models:
        name = model.__tablename__
        await publication.require_preserved_catalog_schema(
            session,
            (live, await native._relation_oid(session, live, name)),
            (schema, await native._relation_oid(session, schema, name)),
        )
    return identifier, schema, spec


async def _require_pending(session, handoff, *, lock=False):
    from process.scoped_catalog_named_publication import _run

    run = await _run(session, handoff["schema_name"], handoff, states=("finalizing",), lock=lock)
    if run["finished_at"] is not None or (run["metrics"] or {}).get("scoped_catalog_handoff") != handoff:
        raise RuntimeError("catalog publication was canceled or changed")
    return run


async def _drop_input(session, handoff):
    schema = handoff["input"]["schema_name"]
    spec = publication.input_spec(handoff["importer_id"])
    await storage.require_inventory(session, schema, spec.table_names)
    await native._lock_family(session, schema, tuple(sorted(spec.table_names)), "ACCESS EXCLUSIVE", nowait=True)
    for name in spec.table_names:
        await session.execute(text(f"DROP TABLE {storage.qualified(schema, name)} RESTRICT"))
    await session.execute(text(f"DROP SCHEMA {native._quoted(schema)} RESTRICT"))


async def _finish(session, handoff, *, status, metric, receipt):
    phase = "catalog published" if status == "succeeded" else "catalog preparation abandoned"
    before = "finalizing" if status == "succeeded" else "canceling"
    metrics_by_field = dict(handoff["metrics"])
    if status == "succeeded" and handoff["importer_id"] == "code-sets":
        metrics_by_field["code_sets_mutable_publication"] = {
            "contract": "code-sets-named-publication.v1",
            "status": status,
            **{name: handoff[name] for name in ("run_id", "attempt_id", "attempt_started_at", "node_id")},
        }
    changed = await session.scalar(
        text(
            f"UPDATE {storage.qualified(handoff['schema_name'], 'import_run')} SET status=:status,phase_detail=:phase,"
            "finished_at=timezone('UTC',clock_timestamp()),heartbeat_at=timezone('UTC',clock_timestamp()),error=NULL,"
            "metrics=(metrics::jsonb||CAST(:metrics AS jsonb)||jsonb_build_object(CAST(:metric AS text),CAST(:receipt AS jsonb)))::json "
            "WHERE run_id=:run_id AND node_id=:node_id AND importer=:importer_id AND engine='healthcare-mrf-api' "
            "AND status=:before AND finished_at IS NULL AND progress->>'attempt_id'=:attempt_id "
            "AND progress->>'attempt_started_at'=:attempt_started_at "
            "AND metrics::jsonb->'scoped_catalog_handoff'=CAST(:handoff AS jsonb) RETURNING run_id"
        ),
        {
            **handoff,
            "status": status,
            "phase": phase,
            "before": before,
            "metric": metric,
            "handoff": canonical_metadata(handoff),
            "metrics": canonical_metadata(metrics_by_field),
            "receipt": canonical_metadata(receipt),
        },
    )
    if changed != handoff["run_id"]:
        raise RuntimeError("catalog terminal attempt changed")


async def publish(session, run, handoff, settings, *, source_copy):
    """Validate and exchange the whole family in the same transaction as terminal status."""
    from process.scoped_catalog_named_publication import digest

    if (
        digest(
            {
                "importer": run["importer"],
                "params": run["params"],
                "options": {
                    "include_relationships": handoff["include_relationships"],
                    "test_mode": False,
                },
                "metrics": handoff["metrics"],
            }
        )
        != handoff["source_contract_sha256"]
    ):
        raise RuntimeError("catalog request source changed")
    await _require_input(session, handoff, settings)
    models = await storage.current_models(session, settings.schema)
    if handoff["importer_id"] == "ms-drg" and models != storage.MODELS:
        raise RuntimeError("complete shared catalog family is required")
    vector, before = await _require_current(session, handoff, models)
    identifier, schema, spec = await _compose(session, handoff, models, source_copy)
    await storage.protect_tables(session, schema, models, settings, candidate=True)

    async def advance():
        """Advance this source only inside the whole-family cutover transaction."""
        return await generations.publish_generation(
            session, settings.schema, handoff["importer_id"], include_relationships=handoff["include_relationships"]
        )

    current, retained_by_field = await activate_prepared(
        session, handoff, (identifier, schema, spec), settings, advance
    )
    receipt_by_field = {"handoff": handoff, "retained": retained_by_field}
    await _drop_input(session, handoff)
    await _finish(session, handoff, status="succeeded", metric="scoped_catalog_publication", receipt=receipt_by_field)
    return current


async def activate_prepared(session, handoff, candidate, settings, publish_generation):
    """Share whole-family CAS, native exchange and logical retention with archive callers."""
    identifier, schema, spec = candidate
    models = spec.model_types
    generations_by_importer, before = await _require_current(session, handoff, models, lock=True)
    if "run_id" in handoff:
        await _require_pending(session, handoff, lock=True)
    pairs = await storage.require_tables(session, schema, models)
    # These transient local handles are used only by the existing native DDL primitive.
    stage = native.ReferenceFamilyStageOwnership(
        spec.importer_id, identifier, schema, await native._schema_oid(session, schema), pairs
    )
    incumbent = native.ReferenceFamilyIncumbent(
        spec.importer_id, settings.schema, await storage.require_tables(session, settings.schema, models)
    )
    predecessor = await native._rotate_family_relations(session, spec, stage, incumbent)
    await retention.copy_generations(session, settings.schema, predecessor, generations_by_importer)
    current = await publish_generation()
    published = await generations.read_generations(session, settings.schema, importer=handoff["importer_id"])
    retained_by_field = {
        "contract": retention.CONTRACT,
        "importer": handoff["importer_id"],
        "dataset_id": str(identifier),
        "schema_name": predecessor,
        "live_schema": settings.schema,
        "generation_table": generations.TABLES[handoff["importer_id"]],
        "tables": before,
        "previous_generations": generations_by_importer,
        "published_generations": published,
        "publication_handoff_sha256": handoff.get("handoff_sha256"),
    }
    await retention.record_retained(session, retained_by_field, settings)
    controls = tuple(SimpleNamespace(__tablename__=generations.TABLES[name]) for name in generations_by_importer)
    await storage.protect_tables(session, settings.schema, controls, settings)
    await session.execute(text(f"DROP SCHEMA {native._quoted(schema)} RESTRICT"))
    return current, retained_by_field


async def cancel(session, run, handoff, settings):
    """Cancellation only removes the persisted attempt's never-published candidate."""
    await _require_input(session, handoff, settings, nowait=False)
    await _drop_input(session, handoff)
    await _finish(
        session,
        handoff,
        status="canceled",
        metric="scoped_catalog_cancellation",
        receipt={"handoff": handoff, "status": "canceled"},
    )


async def _restore_generation(session, logical, current, importer):
    """Advance the local revision while restoring the retained source content and origin."""
    previous = logical["previous_generations"][importer]
    table = storage.qualified(logical["live_schema"], generations.TABLES[importer])
    if current[importer]["local_generation"] >= 2**63 - 1:
        raise RuntimeError("catalog rollback generation is exhausted")
    assignments = [
        "local_generation=local_generation+1",
        "origin_lineage_id=:origin_lineage_id",
        "origin_generation=:origin_generation",
        "published_at=:published_at",
    ]
    if importer == "code-sets":
        count, digest, _ = await generations.codes._slice_identity(
            session, logical["live_schema"], complete=previous["origin_generation"] is not None
        )
        assignments += ["code_catalog_oid=NULL", "row_count=:row_count", "row_sha256=:row_sha256"]
        values_by_field = {
            **previous,
            "row_count": count if previous["origin_generation"] else None,
            "row_sha256": digest if previous["origin_generation"] else None,
        }
    else:
        old = (
            (
                await session.execute(
                    text(
                        f"SELECT include_relationships,receipt FROM {storage.qualified(logical['schema_name'], generations.TABLES[importer])} WHERE id=1"
                    )
                )
            )
            .mappings()
            .one()
        )
        assignments += ["include_relationships=:include_relationships", "receipt=CAST(:receipt AS jsonb)"]
        values_by_field = {
            **previous,
            "include_relationships": old["include_relationships"],
            "receipt": None if old["receipt"] is None else canonical_metadata(old["receipt"]),
        }
    if isinstance(values_by_field["published_at"], str):
        values_by_field["published_at"] = datetime.fromisoformat(values_by_field["published_at"])
    await session.execute(text(f"UPDATE {table} SET " + ",".join(assignments) + " WHERE id=1"), values_by_field)


async def rollback(session, receipt, settings):
    """Restore a retained family atomically, refusing any later source generation."""
    retained = receipt["retained"]
    logical = await retention.require_retained(session, retained)
    if logical["live_schema"] != settings.schema:
        raise RuntimeError("catalog rollback destination differs")
    models = storage.MODELS if len(logical["tables"]) == 3 else storage.MODELS[:1]
    await storage.require_tables(session, settings.schema, models, lock="ACCESS EXCLUSIVE")
    current = await generations.read_generations(session, settings.schema, lock=True, importer=logical["importer"])
    expected_by_field = generations.compatible_expected_generations(
        current, logical["published_generations"], contract=retained["contract"], importer=logical["importer"]
    )
    if current != expected_by_field:
        raise RuntimeError("catalog rollback source generation changed")
    await generations.validate_published_content(session, settings.schema, current)
    await storage.require_tables(session, logical["schema_name"], models, lock="ACCESS EXCLUSIVE")
    for model in models:
        name = model.__tablename__
        await session.execute(text(f"DROP TABLE {storage.qualified(settings.schema, name)} RESTRICT"))
        await session.execute(
            text(
                f"ALTER TABLE {storage.qualified(logical['schema_name'], name)} SET SCHEMA {native._quoted(settings.schema)}"
            )
        )
    for importer in retention.retained_generation_names(retained):
        await _restore_generation(session, logical, current, importer)
    for importer in retention.retained_generation_names(retained):
        await session.execute(
            text(f"DROP TABLE {storage.qualified(logical['schema_name'], generations.TABLES[importer])} RESTRICT")
        )
    await session.execute(text(f"DROP SCHEMA {native._quoted(logical['schema_name'])} RESTRICT"))
    return await generations.read_generations(session, settings.schema, importer=logical["importer"])
