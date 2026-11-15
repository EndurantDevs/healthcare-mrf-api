# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native named input compatibility preserves scope and database-enforced access."""

from copy import deepcopy

from tests.test_provider_directory_cms_prepared_context_postgres import native_case, observe, publisher


async def test_native_unrelated_candidate_data_does_not_change_the_named_input_group(native_case):
    case = native_case
    async with publisher(case) as (session, driver), observe(case, session, driver) as original:
        expected = deepcopy(original)
    await case.admin.execute("CREATE SCHEMA another_candidate")
    await case.admin.execute("CREATE TABLE another_candidate.rows(id integer PRIMARY KEY)")
    await case.admin.execute("INSERT INTO another_candidate.rows VALUES(1),(2)")
    async with publisher(case) as (session, driver), observe(case, session, driver) as current:
        assert current == expected
        assert len(current["frozen_inventory"]["relations"]) == 8


async def test_native_recreated_input_uses_the_same_recorded_names_without_metadata_rewrite(native_case):
    case = native_case
    expected = deepcopy(case.inventory)
    async with publisher(case) as (session, driver), observe(case, session, driver) as original:
        names = deepcopy(original)
    await case.admin.execute("DROP TABLE private_source.frozen_input_0")
    await case.admin.execute("CREATE TABLE private_source.frozen_input_0(id integer PRIMARY KEY)")
    await case.admin.execute("INSERT INTO private_source.frozen_input_0 VALUES(42)")
    await case.admin.execute(f'GRANT SELECT ON private_source.frozen_input_0 TO "{case.policy.publisher_role}"')
    async with publisher(case) as (session, driver), observe(case, session, driver) as current:
        assert current == names and case.inventory == expected
        assert await driver.fetchval("SELECT id FROM private_source.frozen_input_0") == 42
