# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Keep a pricing worker claim and its reserved-run refusal in one owned transaction."""

from contextlib import asynccontextmanager

from sqlalchemy import text

from db.models import db
from db.plan_release_storage import plan_release_schema, storage_relation
from process.plan_pricing_admission import require_unreserved_pricing_run


async def pricing_driver(session):
    """Use the physical connection belonging to this owned writer transaction."""
    await session.execute(text("SELECT 1"))
    connection = await session.connection()
    return (await connection.get_raw_connection()).driver_connection


@asynccontextmanager
async def pricing_worker_claim_transaction(control_task):
    """Read the actual persisted importer before its initial claim."""
    if not control_task.get("run_id"):
        yield
        return
    async with db.transaction() as session:
        driver = await pricing_driver(session)
        row = await driver.fetchrow(
            f"SELECT * FROM {storage_relation(plan_release_schema(), 'import_run')} WHERE run_id=$1 AND importer='plan-pricing-projection'",
            control_task["run_id"],
        )
        if row is not None:
            require_unreserved_pricing_run(dict(row))
        yield
