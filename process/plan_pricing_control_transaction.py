# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Keep managed control admission and its native write in one owned transaction."""

from contextlib import asynccontextmanager

from sqlalchemy import text

from db.models import db
from db.plan_release_storage import plan_release_schema, storage_relation
from process.plan_pricing_admission import is_reserved_pricing_key, require, require_managed_pricing_admission
from process.plan_pricing_control import pricing_control_digest
from process.plan_release_storage_custody import _document


async def pricing_driver(session):
    """Use the physical connection belonging to this owned writer transaction."""
    await session.execute(text("SELECT 1"))
    connection = await session.connection()
    return (await connection.get_raw_connection()).driver_connection


@asynccontextmanager
async def pricing_admission_transaction(run_by_field, *, action):
    """Commit a managed admission and its run insertion or replay together."""
    if run_by_field.get("importer") != "plan-pricing-projection" or not (
        is_reserved_pricing_key(run_by_field.get("idempotency_key")) or run_by_field.get("retry_of_run_id")
    ):
        yield None
        return
    async with db.transaction() as session:
        yield await require_managed_pricing_admission(await pricing_driver(session), run_by_field, action=action)


@asynccontextmanager
async def pricing_worker_claim_transaction(control_task):
    """Authenticate the actual persisted importer before its initial claim."""
    if not control_task.get("run_id"):
        yield
        return
    async with db.transaction() as session:
        driver = await pricing_driver(session)
        row = await driver.fetchrow(
            f"SELECT * FROM {storage_relation(plan_release_schema(), 'import_run')} WHERE run_id=$1 AND importer='plan-pricing-projection'",
            control_task["run_id"],
        )
        if row is None:
            yield
            return
        run_by_field = dict(row)
        run_by_field["params"] = _document(run_by_field["params"])
        admission = await require_managed_pricing_admission(driver, run_by_field, action="claim")
        if admission is not None:
            expected_by_field = {"test_mode": False, **run_by_field["params"]}
            require(
                control_task.get("importer") == "plan-pricing-projection"
                and control_task.get("target_module") == "api.plan_pricing_projection"
                and control_task.get("target_function") == "build_plan_pricing_projection"
                and control_task.get("call_style") == "kwargs"
                and control_task.get("run_shutdown") is False
                and pricing_control_digest(control_task.get("task")) == pricing_control_digest(expected_by_field),
                "worker_request_changed",
            )
        yield
