# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Provisioning an empty legacy cohort creates no plan-release pins."""

import pytest

from tests.plan_release_storage_postgres_support import storage_case


@pytest.mark.asyncio
async def test_empty_legacy_provision_does_not_invent_a_pin_cohort():
    async with storage_case(migrated=False) as case:
        admin = await case.connect()
        await admin.execute(f"DELETE FROM {case.relation('plan_release_serving_revision')}")
        await case.migrate()
        await case.provision()
        await case.provision()
        assert await admin.fetchval("SELECT to_regclass($1)", case.relation("plan_release_snapshot_pin_legacy")) is None
        assert not await admin.fetchval(f"SELECT EXISTS(SELECT FROM {case.relation('plan_release_snapshot_pin')})")
        assert await admin.fetchval(f"SELECT count(*) FROM {case.relation('ptg2_snapshot_pin')}") == 1
