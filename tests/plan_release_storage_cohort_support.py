# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Prepared native cohorts exercise storage publication and rollback."""

import datetime

from db.plan_release_storage import storage_relation, storage_snapshot_index_sql, storage_table_sql
from tests.plan_release_storage_postgres_support import NEW_REVISION, OLD_REVISION, PLAN, RELEASE

CAPTURED_TIME = datetime.datetime(2026, 10, 1, tzinfo=datetime.UTC)


async def _finish_native_cohort(case, admin):
    """Finish native indexes and close both immutable heaps before validation."""
    for table, key in (
        ("cohort_binding", "serving_revision_id,role,binding_ordinal"),
        ("cohort_pin", "serving_revision_id,snapshot_id"),
    ):
        relation = storage_relation(case.preparation_schema, table)
        await admin.execute(f"ALTER TABLE {relation} ADD PRIMARY KEY ({key})")
        await admin.execute(storage_snapshot_index_sql(case.preparation_schema, table, table + "_snapshot_idx"))
        await admin.execute(f"ALTER TABLE {relation} ADD CHECK(serving_revision_id='{NEW_REVISION}')")
        await admin.execute(f'ALTER TABLE {relation} OWNER TO "{case.owner}"')
        await admin.execute(f'GRANT SELECT ON {relation} TO "{case.reader}"')


async def prepare_native_cohort(case):
    """Load detached heaps, build indexes, then close native write custody."""
    admin = await case.connect()
    binding = storage_relation(case.preparation_schema, "cohort_binding")
    pin = storage_relation(case.preparation_schema, "cohort_pin")
    async with admin.transaction():
        for table, is_pin in (("cohort_binding", False), ("cohort_pin", True)):
            await admin.execute(storage_table_sql(case.preparation_schema, table, pin=is_pin, primary_key=False))
        await admin.copy_records_to_table(
            "cohort_binding",
            schema_name=case.preparation_schema,
            records=[
                (
                    NEW_REVISION,
                    ordinal,
                    "new-snapshot",
                    "synthetic_source",
                    "logical-plan",
                    "group",
                    "in_network",
                    True,
                    "{}",
                    CAPTURED_TIME,
                )
                for ordinal in range(2000)
            ],
        )
        await admin.copy_records_to_table(
            "cohort_pin",
            schema_name=case.preparation_schema,
            records=[(NEW_REVISION, "new-snapshot", "captured", CAPTURED_TIME)],
        )
        await _finish_native_cohort(case, admin)
        await admin.execute(f"INSERT INTO {case.relation('ptg2_snapshot')} VALUES('new-snapshot','published','{{}}')")
        await admin.execute(
            f"INSERT INTO {case.relation('ptg2_v3_snapshot_plan_scope')} VALUES('new-snapshot','logical-plan','group')"
        )
        assert not await admin.fetchval(
            f"SELECT EXISTS(SELECT FROM {binding} b LEFT JOIN {case.relation('ptg2_snapshot')} s USING(snapshot_id) "
            "WHERE s.snapshot_id IS NULL OR s.status<>'published')"
        )
        assert not await admin.fetchval(
            f"SELECT EXISTS(SELECT FROM {binding} b LEFT JOIN {pin} p USING(serving_revision_id,snapshot_id) "
            "WHERE p.snapshot_id IS NULL)"
        )
        assert (
            await admin.fetchval(
                "SELECT count(*) FROM pg_constraint WHERE conrelid=ANY($1::regclass[]) AND contype='f'",
                [binding, pin],
            )
            == 0
        )
    return binding, pin


async def attach_native_cohort(case, publisher, binding, pin):
    """Perform only the native attach and exact current-header CAS in one caller transaction."""
    await publisher.execute(f'SET LOCAL ROLE "{case.owner}"')
    changed = await publisher.execute(
        f"UPDATE {case.relation('plan_release_serving_revision')} SET is_current=false,serving_status='retired',retired_at=transaction_timestamp() "
        "WHERE serving_revision_id=$1 AND is_current AND binding_set_digest=$2",
        OLD_REVISION,
        "a" * 64,
    )
    assert changed == "UPDATE 1"
    await publisher.execute(
        f"INSERT INTO {case.relation('plan_release_serving_revision')} (serving_revision_id,plan_release_id,healthporta_plan_id,release_month,"
        "release_status,serving_status,is_current,expected_binding_count,binding_set_digest,created_at,published_at) "
        "VALUES($1,$2,$3,'2026-10','published','published',true,2000,$4,$5,$5)",
        NEW_REVISION,
        RELEASE,
        PLAN,
        "b" * 64,
        CAPTURED_TIME,
    )
    for root, heap in (("plan_release_snapshot_binding", binding), ("plan_release_snapshot_pin", pin)):
        await publisher.execute(
            f"ALTER TABLE {case.relation(root)} ATTACH PARTITION {heap} FOR VALUES IN ('{NEW_REVISION}')"
        )
    await publisher.execute(f"RESET ROLE")
