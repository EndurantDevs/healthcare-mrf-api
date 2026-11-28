# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native snapshot GC keeps snapshots held only by a dedicated plan-release pin."""

import pytest

from db.plan_release_storage import storage_table_sql
from tests.plan_release_storage_postgres_support import storage_case


@pytest.mark.asyncio
async def test_both_native_gc_sets_retain_a_dedicated_pin_without_a_binding():
    from process.ptg_parts import ptg2_source_snapshot_gc, snapshot_cleanup

    async with storage_case(provisioned=True) as case:
        admin = await case.connect()
        detached = case.relation("pin_only")
        revision_id = "hpserve_" + "9" * 26
        async with admin.transaction():
            await admin.execute(storage_table_sql(case.schema, "pin_only", pin=True))
            await admin.execute(
                f"INSERT INTO {detached}(serving_revision_id,snapshot_id) VALUES($1,'pin-only-snapshot')", revision_id
            )
            await admin.execute(
                f"ALTER TABLE {case.relation('plan_release_snapshot_pin')} ATTACH PARTITION {detached} FOR VALUES IN ('{revision_id}')"
            )
            await admin.execute(
                f"INSERT INTO {case.relation('ptg2_snapshot')} VALUES('pin-only-snapshot','published','{{}}')"
            )
            await admin.execute(
                f'UPDATE {case.relation("ptg2_snapshot")} SET manifest=\'{{"serving_index":{{"source_key":"synthetic_source"}}}}\''
            )
        ordinary_query = ptg2_source_snapshot_gc._CURRENT_SNAPSHOT_IDS_SQL.replace("__SCHEMA__", case.schema)
        source_query = snapshot_cleanup._CURRENT_SOURCE_POINTER_SNAPSHOT_IDS_SQL.replace(
            "__SCHEMA__", case.schema
        ).replace(":source_key", "$1")
        assert set(
            await admin.fetchval(
                "SELECT array_agg(snapshot_id ORDER BY snapshot_id) FROM (" + ordinary_query + ") retained"
            )
        ) == {"old-snapshot", "pin-only-snapshot"}
        assert {member["snapshot_id"] for member in await admin.fetch(source_query, "synthetic_source")} == {
            "old-snapshot",
            "pin-only-snapshot",
        }
