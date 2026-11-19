# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Managed shared-catalog preparation, cancellation and portable outcome readback."""

from __future__ import annotations

import asyncio

from sqlalchemy import text

from process import scoped_catalog_named_generation as generations
from process import scoped_catalog_named_publication as named
from process import scoped_catalog_named_retention as retention
from process import scoped_catalog_named_storage as storage
from process import scoped_catalog_publication as publication
from process.scoped_catalog_layout import control_schema

CONTRACT, PHASE, METRIC = named.CONTRACT, named.PHASE, named.METRIC
_digest = named.digest
validate_catalog_handoff = named.validate_handoff
capture_catalog_expected = named.capture_expected
prepare_catalog_handoff = named.prepare


def has_catalog_handoff(run):
    """Identify managed catalog continuations without altering their persisted records."""
    return (
        run.get("importer") in named.IMPORTERS
        and isinstance(run.get("metrics"), dict)
        and run["metrics"].get(METRIC) is not None
    )


async def _read_outcome_run(session, candidate):
    candidate = validate_catalog_handoff(candidate)
    run = await named._run(
        session,
        candidate["schema_name"],
        candidate,
        states=("running", "finalizing", "canceling", "succeeded", "canceled"),
    )
    if (
        run["importer"] != candidate["importer_id"]
        or run["node_id"] != candidate["node_id"]
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
        raise RuntimeError("catalog outcome attempt differs")
    return run


async def request_catalog_cancel(session, current):
    """The ordinary application requests cancellation without touching serving or staged data."""
    candidate = validate_catalog_handoff((current["metrics"] or {}).get(METRIC))
    run = await _read_outcome_run(session, candidate)
    if (
        run["run_id"] != current["run_id"]
        or run["status"] not in {"finalizing", "canceling"}
        or run["finished_at"] is not None
    ):
        raise RuntimeError("catalog cancellation attempt differs")
    await session.execute(
        text(
            f"UPDATE {storage.qualified(control_schema(candidate['schema_name']), 'import_run')} SET status='canceling',"
            "phase_detail='catalog publication cancellation requested',heartbeat_at=timezone('UTC',clock_timestamp()) WHERE run_id=:run_id"
        ),
        {"run_id": candidate["run_id"]},
    )


async def _reconcile_catalog_handoff(database, candidate):
    """Read the actual committed attempt after a lost COMMIT response, including cancellation."""

    async def _is_persisted():
        async with asyncio.timeout(40), database.transaction() as session:
            await session.execute(text("SET LOCAL lock_timeout='30s'"))
            observed = await session.scalar(
                text(
                    f"SELECT metrics::jsonb->'{METRIC}' FROM "
                    f"{storage.qualified(control_schema(candidate['schema_name']), 'import_run')} "
                    "WHERE run_id=:run_id FOR SHARE"
                ),
                {"run_id": candidate["run_id"]},
            )
            return observed == candidate

    pending = asyncio.create_task(_is_persisted())
    while True:
        try:
            return await asyncio.shield(pending)
        except asyncio.CancelledError:
            if pending.done():
                return pending.result()


async def read_catalog_handoff_outcome(session, candidate):
    """Validate pending input or the native terminal result for the exact managed attempt."""
    run = await _read_outcome_run(session, candidate)
    if run["status"] in {"finalizing", "canceling"} and run["finished_at"] is None:
        spec = publication.input_spec(candidate["importer_id"])
        schema = candidate["input"]["schema_name"]
        await storage.require_inventory(session, schema, spec.table_names)
        if await named.input_receipts(session, candidate) != candidate["tables"]:
            raise RuntimeError("catalog pending source content differs")
        return None
    if run["finished_at"] is None or run["error"] is not None:
        raise RuntimeError("catalog terminal outcome is unfinished")
    if await session.scalar(
        text("SELECT to_regnamespace(:schema) IS NOT NULL"), {"schema": candidate["input"]["schema_name"]}
    ):
        raise RuntimeError("catalog terminal input still exists")
    if run["status"] == "canceled" and run["phase_detail"] == "catalog preparation abandoned":
        receipt_by_field = {"handoff": candidate, "status": "canceled"}
        if run["metrics"].get("scoped_catalog_cancellation") != receipt_by_field:
            raise RuntimeError("catalog cancellation receipt differs")
        return receipt_by_field
    if run["status"] != "succeeded" or run["phase_detail"] != "catalog published":
        raise RuntimeError("catalog terminal outcome differs")
    receipt_by_field = run["metrics"].get("scoped_catalog_publication")
    if (
        type(receipt_by_field) is not dict
        or receipt_by_field.get("handoff") != candidate
        or any(run["metrics"].get(key) != field_value for key, field_value in candidate["metrics"].items())
    ):
        raise RuntimeError("catalog publication receipt differs")
    await require_catalog_publication(session, receipt_by_field)
    return receipt_by_field


async def require_catalog_publication(session, receipt):
    """Check retained data and the complete logical publication vector by name."""
    if type(receipt) is not dict or set(receipt) != {"handoff", "retained"}:
        raise RuntimeError("catalog publication contract differs")
    candidate = validate_catalog_handoff(receipt["handoff"])
    retained = await retention.require_retained(session, receipt["retained"])
    if retained["live_schema"] != candidate["schema_name"] or retained["importer"] != candidate["importer_id"]:
        raise RuntimeError("catalog publication destination differs")
    if retained["publication_handoff_sha256"] != candidate["handoff_sha256"]:
        raise RuntimeError("catalog publication handoff differs")
    current = await generations.read_generations(session, candidate["schema_name"], importer=candidate["importer_id"])
    expected = retained.get("published_generations")
    if expected is not None:
        expected = generations.compatible_expected_generations(
            current, expected, contract=receipt["retained"]["contract"], importer=candidate["importer_id"]
        )
        if set(current) != set(expected):
            raise RuntimeError("catalog publication source vector differs")
        for name in current:
            observed, published = current[name], expected[name]
            if observed["local_generation"] < published["local_generation"] or any(
                observed[field] != published[field] for field in generations.FIELDS if field != "local_generation"
            ):
                raise RuntimeError("catalog published source was replaced")
    await generations.validate_published_content(session, candidate["schema_name"], current)
    return receipt


async def publish_catalog_handoff(session, candidate, *, source_copy, settings=None):
    """Compatibility library entry; settings must be explicitly operator supplied."""
    from process.scoped_catalog_named_cutover import publish

    if settings is None or settings.schema != candidate["schema_name"]:
        raise RuntimeError("catalog protected publisher settings are required")
    run = await _read_outcome_run(session, candidate)
    if run["status"] != "finalizing" or run["finished_at"] is not None:
        raise RuntimeError("catalog publication attempt changed")
    await publish(session, run, candidate, settings, source_copy=source_copy)
    return (await _read_outcome_run(session, candidate))["metrics"]["scoped_catalog_publication"]


async def cancel_catalog_handoff(session, candidate, *, settings=None):
    """Abandon only the persisted candidate using explicitly supplied publisher settings."""
    from process.scoped_catalog_named_cutover import cancel

    if settings is None or settings.schema != candidate["schema_name"]:
        raise RuntimeError("catalog protected publisher settings are required")
    run = await _read_outcome_run(session, candidate)
    if run["status"] != "canceling" or run["finished_at"] is not None:
        raise RuntimeError("catalog cancellation attempt changed")
    await cancel(session, run, candidate, settings)
    return {"handoff": candidate, "status": "canceled"}
