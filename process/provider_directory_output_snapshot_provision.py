# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Freeze the fixed output family through native PostgreSQL permissions."""

from db.plan_release_storage import storage_relation
from process.provider_directory_generic_source_policy import GenericSourcePolicy
from process.provider_directory_generic_source_provision import freeze_fhir_source_snapshot
from process.provider_directory_output_snapshot_catalog import (
    OUTPUT_CATALOG_SQL,
    OUTPUT_ROOTS,
    require_output,
    require_output_shape,
)
from process.provider_directory_output_snapshot_custody import _native_receipt
from process.provider_directory_projection_stage import _copy_driver


async def _capture(session, policy, *, legacy=False):
    driver = await _copy_driver(session)
    rows = await driver.fetch(OUTPUT_CATALOG_SQL, policy.schema, list(OUTPUT_ROOTS))
    require_output(
        len(rows) == len(OUTPUT_ROOTS) and {row["relname"] for row in rows} == set(OUTPUT_ROOTS), "roots_missing"
    )
    for row in rows:
        require_output_shape(row, row["relname"])
    return {row["relname"]: dict(row) for row in rows}


def require_output_adoption_delta(before, after, policy):
    """Changing native grants preserves every named relation and its logical layout."""
    require_output(before.keys() == after.keys() == set(OUTPUT_ROOTS), "adoption_roots_changed")
    for name in before:
        require_output(
            _native_receipt(policy, before[name], name) == _native_receipt(policy, after[name], name),
            "adoption_layout_changed",
        )


async def provision_output_snapshot_storage(session, policy):
    """Freeze the complete family atomically; PostgreSQL authorizes each DDL operation."""
    require_output(type(policy) is GenericSourcePolicy and session.in_transaction(), "transaction_required")
    driver = await _copy_driver(session)
    async with driver.transaction():
        await driver.execute("SET LOCAL lock_timeout='5s'")
        await driver.execute(
            "LOCK TABLE "
            + ",".join(storage_relation(policy.schema, name) for name in OUTPUT_ROOTS)
            + " IN ACCESS SHARE MODE"
        )
        before = await _capture(session, policy)
        await freeze_fhir_source_snapshot(
            driver,
            policy.schema,
            namespace_owner=policy.owner_role,
            reader_roles=(policy.owner_role, policy.publisher_role, *policy.runtime_roles),
            relation_names=OUTPUT_ROOTS,
            freeze_schema=False,
        )
        after = await _capture(session, policy)
        require_output_adoption_delta(before, after, policy)
        return tuple(_native_receipt(policy, after[name], name) for name in OUTPUT_ROOTS)
