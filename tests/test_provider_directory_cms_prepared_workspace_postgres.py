# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native private workspace proof; no issuer, canonical or finisher authority."""

import asyncio
from datetime import datetime, timezone
from uuid import uuid4

import asyncpg
import pytest
import pytest_asyncio

from process import provider_directory_cms_prepared_workspace as workspace
from process import provider_directory_generic_source_policy as source
from process.provider_directory_cms_identity_candidate import CMS_IDENTITY_COLUMNS
from process.provider_directory_generic_candidate import RESOURCE_COLUMNS, WITNESS_COLUMNS
from tests.test_provider_directory_cms_prepared_context_postgres import native_case, observe, publisher
from tests.test_provider_directory_cms_prepared_workspace import handoff_for


@pytest_asyncio.fixture
async def native_workspace(native_case):
    case = native_case
    await case.admin.execute(
        f'GRANT "{case.roles["prepared_owner"]}" TO "{case.roles["prepared_publisher"]}" WITH INHERIT TRUE,SET TRUE'
    )
    await case.admin.execute(f'GRANT USAGE,CREATE ON SCHEMA private_source TO "{case.roles["prepared_owner"]}"')
    await case.admin.execute(f'GRANT USAGE ON SCHEMA private_source TO "{case.roles["reader"]}"')
    old_names = [row["relation_name"] for row in case.inventory["relations"]]
    case.handoff = handoff_for(case.storage, case.inventory)
    for old_name, row in zip(old_names, case.inventory["relations"], strict=True):
        await case.admin.execute(f'ALTER TABLE private_source."{old_name}" RENAME TO "{row["relation_name"]}"')
        await case.admin.execute(f'INSERT INTO private_source."{row["relation_name"]}" VALUES(42)')
    case.preparation_id, case.admission = str(uuid4()), "f" * 64
    yield case
    assert all(
        [
            await case.admin.fetchval(f'SELECT id=42 FROM private_source."{row["relation_name"]}"')
            for row in case.inventory["relations"]
        ]
    )


def scope(case, session, driver, inputs, **changes):
    options_by_name = dict(
        source_binding=case.storage,
        prepared_inputs=inputs,
        handoff=case.handoff,
        preparation_id=case.preparation_id,
        admission_sha256=case.admission,
    )
    options_by_name.update(changes)
    return workspace.prepared_cms_workspace(case.database, session, driver, **options_by_name)


def rows_by_root(identity):
    now, organization, network, resource = datetime.now(timezone.utc), uuid4(), uuid4(), uuid4()
    source_id, release, dataset = (
        "cms-npd",
        identity["metadata"]["source_release"]["vector_sha256"],
        identity["dataset_id"],
    )
    return {
        workspace.storage.CMS_IDENTITY_ROOTS[0]: [(source_id, "Organization", "org", organization, None, now)],
        workspace.storage.CMS_IDENTITY_ROOTS[1]: [
            (source_id, "Organization", "org", release, "a" * 64, '{"id":"org"}', now)
        ],
        workspace.storage.CMS_IDENTITY_ROOTS[2]: [(source_id, "Organization", "org", network, now)],
        workspace.storage.CMS_IDENTITY_ROOTS[3]: [
            (source_id, release, "Organization", "org", "plan", "[]", None, None, "b" * 64, '{"id":"plan"}', now)
        ],
        workspace.storage.CMS_RESOURCE_IDENTITY_ROOT: [(source_id, "InsurancePlan", "plan", resource, now)],
        workspace.storage.RELATIONS[1]: [
            (dataset, "plan", "c" * 64, '{"status":"active","plan_identifier":"example"}')
        ],
        workspace.storage.RELATIONS[2]: [(dataset, "org", "plan")],
        workspace.storage.RELATIONS[3]: [(dataset, "org", "affiliation")],
        workspace.storage.RESOURCE: [
            (dataset, "InsurancePlan", "plan", "c" * 64, '{"status":"active","plan_identifier":"example"}', None)
        ],
        workspace.storage.WITNESS: [
            (dataset, source_id, release, "InsurancePlan", "plan", "d" * 64, "c" * 64, '{"id":"plan"}')
        ],
        workspace.storage.RELATIONSHIP: [
            (
                dataset,
                source_id,
                release,
                "InsurancePlan",
                "plan",
                "c" * 64,
                "d" * 64,
                "network",
                0,
                1,
                "Organization",
                "Organization/org",
                "org",
                "resolved",
                None,
                None,
            )
        ],
    }


async def assert_no_outputs(case):
    assert (
        await case.admin.fetchval(
            "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
            "WHERE n.nspname='private_source' AND c.relkind='r'"
        )
        == 8
    )


async def assert_payload_snapshot_bounds(current):
    """Native bounds support later attachment; they do not seal mutable custody."""
    literal = await current.connection.fetchval(
        "SELECT pg_catalog.quote_literal($1::text)", current.identity["dataset_id"]
    )
    for root in workspace.storage.RELATIONS:
        relation = workspace.storage_relation(current.policy.schema, current.receipts[root]["name"])
        bound = await current.connection.fetchrow(
            "SELECT convalidated,conenforced,pg_get_expr(conbin,conrelid,true) AS expression "
            "FROM pg_constraint WHERE conrelid=to_regclass($1) AND conname='pd_dataset_bound' AND contype='c'",
            relation,
        )
        assert bound is not None and bound["convalidated"] and bound["conenforced"]
        assert bound["expression"] == f"dataset_id::text = {literal}::text"
        assert (
            await current.connection.fetchval(
                "SELECT count(*) FROM pg_constraint WHERE conrelid=to_regclass($1) AND contype='f'", relation
            )
            == 0
        )


async def assert_payload_storage_types(current, driver, receipts):
    """Check actual jsonb columns and absence of added foreign keys after native COPY."""
    for root, field in (
        (workspace.storage.RESOURCE, "payload_json"),
        (workspace.storage.RELATIONS[1], "payload_json"),
        (workspace.storage.WITNESS, "raw_payload_json"),
    ):
        relation = f'private_source."{receipts[root]["name"]}"'
        assert await driver.fetchval(f"SELECT pg_typeof({field})::text FROM {relation}") == "jsonb"
        assert (
            await driver.fetchval(
                "SELECT count(*) FROM pg_constraint WHERE conrelid=to_regclass($1) AND contype='f'",
                workspace.storage_relation(current.policy.schema, receipts[root]["name"]),
            )
            == 0
        )


async def test_native_fixed_outputs_copy_and_indexes_keep_publisher_and_source_binding(native_workspace):
    case = native_workspace
    ambient = (source._FACTORY.get(), source._PUBLISHER.get(), source._OWNER.get())
    async with publisher(case) as (session, driver), observe(case, session, driver) as inputs:
        async with scope(case, session, driver, inputs) as current:
            original_binding = case.storage
            before = current.receipts
            assert current.source_binding is case.storage
            assert await driver.fetchval("SELECT current_user") == case.policy.publisher_role
            assert len(before) == 11
            assert current.binding["contract"] == "cms-prepared-private-workspace-v2"
            await assert_payload_snapshot_bounds(current)
            for root, identity_rows in rows_by_root(current.identity).items():
                assert await current.copy_rows(root, workspace._layouts()[root][1], identity_rows) == 1
                assert await driver.fetchval("SELECT current_user") == case.policy.publisher_role
                assert (
                    await driver.fetchval(f'SELECT count(*) FROM private_source."{current.receipts[root]["name"]}"')
                    == 1
                )
            # Map keys exist while loading; lookups need no growing-heap scans.
            for root in workspace.storage.AUXILIARY_ROOTS:
                assert (
                    await driver.fetchval(
                        "SELECT count(*) FROM pg_index WHERE indrelid=to_regclass($1) AND indisprimary",
                        workspace.storage_relation(case.policy.schema, before[root]["name"]),
                    )
                    == 1
                )
            after = await current.finish_indexes()
            assert before == after
            for receipt in after.values():
                assert (
                    await driver.fetchval(
                        "SELECT count(*) FROM pg_index WHERE indrelid=to_regclass($1) AND indisvalid AND indisready",
                        workspace.storage_relation(case.policy.schema, receipt["name"]),
                    )
                    > 0
                )
            assert current.binding["indexed"]
            await assert_payload_snapshot_bounds(current)
            assert case.storage == original_binding
            plan = current.receipts[workspace.storage.RELATIONS[1]]["name"]
            assert dict(await driver.fetchrow(f'SELECT plan_active,plan_identifier FROM private_source."{plan}"')) == {
                "plan_active": True,
                "plan_identifier": "example",
            }
            assert not await driver.fetchval(
                "SELECT EXISTS(SELECT FROM pg_trigger WHERE tgrelid=ANY(ARRAY(SELECT to_regclass(name) FROM unnest($1::text[]) name)))",
                [workspace.storage_relation(case.policy.schema, receipt["name"]) for receipt in after.values()],
            )
            await assert_payload_storage_types(current, driver, after)
            assert (source._FACTORY.get(), source._PUBLISHER.get(), source._OWNER.get()) == ambient
        with pytest.raises(ValueError, match="scope_ended"):
            await current.verify_current()


async def test_native_existing_name_is_preserved_and_refuses_whole_workspace(native_workspace):
    case = native_workspace
    async with publisher(case) as (session, driver), observe(case, session, driver) as inputs:
        binding = workspace._binding(case.storage, inputs, case.handoff, case.preparation_id, case.admission)
        name = next(iter(workspace._names(binding).values()))
        await case.admin.execute(f'CREATE TABLE private_source."{name}"(sentinel integer)')
        await case.admin.execute(f'INSERT INTO private_source."{name}" VALUES(99)')
        oid = await case.admin.fetchval("SELECT to_regclass($1)::oid", "private_source." + name)
        with pytest.raises(ValueError, match="output_exists"):
            async with scope(case, session, driver, inputs):
                pytest.fail("existing object was reused")
        assert await case.admin.fetchval(f'SELECT sentinel FROM private_source."{name}"') == 99
        assert await case.admin.fetchval("SELECT to_regclass($1)::oid", "private_source." + name) == oid


async def test_native_default_reader_acl_is_closed_before_private_copy(native_workspace):
    case = native_workspace
    await case.admin.execute(
        f'ALTER DEFAULT PRIVILEGES FOR ROLE "{case.policy.owner_role}" IN SCHEMA private_source '
        f'GRANT SELECT ON TABLES TO "{case.roles["reader"]}"'
    )
    async with publisher(case) as (session, driver), observe(case, session, driver) as inputs:
        async with scope(case, session, driver, inputs) as current:
            for receipt in current.receipts.values():
                assert not await driver.fetchval(
                    "SELECT has_table_privilege($1,$2,'SELECT,INSERT,UPDATE,DELETE,TRUNCATE')",
                    case.roles["reader"],
                    workspace.storage_relation(case.policy.schema, receipt["name"]),
                )
            assert await driver.fetchval("SELECT current_user") == case.policy.publisher_role


async def test_native_failed_copy_rolls_back_its_batch_and_restores_publisher(native_workspace):
    case = native_workspace
    async with publisher(case) as (session, driver), observe(case, session, driver) as inputs:
        async with scope(case, session, driver, inputs) as current:
            root = workspace.storage.CMS_IDENTITY_ROOTS[0]
            rows = rows_by_root(current.identity)[root]
            with pytest.raises(asyncpg.UniqueViolationError):
                await current.copy_rows(root, CMS_IDENTITY_COLUMNS[root], rows * 2)
            assert await driver.fetchval("SELECT current_user") == case.policy.publisher_role
            assert await driver.fetchval(f'SELECT count(*) FROM private_source."{current.receipts[root]["name"]}"') == 0
            assert await current.copy_rows(root, CMS_IDENTITY_COLUMNS[root], rows) == 1


@pytest.mark.parametrize("error", [RuntimeError("caller failure"), asyncio.CancelledError()])
async def test_native_caught_failure_rolls_back_every_output_and_keeps_outer_publisher(native_workspace, error):
    case = native_workspace
    async with publisher(case) as (session, driver), observe(case, session, driver) as inputs:
        with pytest.raises(type(error)):
            async with scope(case, session, driver, inputs) as current:
                for root, columns in (
                    (workspace.storage.RESOURCE, RESOURCE_COLUMNS),
                    (workspace.storage.WITNESS, WITNESS_COLUMNS),
                ):
                    assert await current.copy_rows(root, columns, rows_by_root(current.identity)[root]) == 1
                raise error
        assert await driver.fetchval("SELECT current_user") == case.policy.publisher_role
    await assert_no_outputs(case)


async def test_native_workspace_rejects_child_use(native_workspace):
    case = native_workspace
    async with publisher(case) as (session, driver), observe(case, session, driver) as inputs:
        async with scope(case, session, driver, inputs) as current:
            with pytest.raises(ValueError, match="original_task_required"):
                await asyncio.create_task(current.verify_current())
            await current.verify_current()


async def test_native_late_input_scope_change_rolls_back_outputs(native_workspace):
    case = native_workspace
    async with publisher(case) as (session, driver), observe(case, session, driver) as inputs:
        original = inputs["frozen_inventory"]["relations"][0]["relation_name"]
        try:
            with pytest.raises(ValueError, match="prepared_inputs_changed"):
                async with scope(case, session, driver, inputs) as current:
                    assert len(current.receipts) == 11
                    inputs["frozen_inventory"]["relations"][0]["relation_name"] = "another_input"
        finally:
            inputs["frozen_inventory"]["relations"][0]["relation_name"] = original
        assert await driver.fetchval("SELECT current_user") == case.policy.publisher_role
    await assert_no_outputs(case)


def reopen(case, session, driver, inputs, binding):
    return workspace.reopen_prepared_cms_workspace(
        case.database,
        session,
        driver,
        source_binding=case.storage,
        prepared_inputs=inputs,
        handoff=case.handoff,
        expected_workspace=binding,
    )


async def test_native_committed_workspace_reopens_without_writes(native_workspace):
    case = native_workspace
    async with publisher(case) as (session, driver), observe(case, session, driver) as inputs:
        async with scope(case, session, driver, inputs) as current:
            await current.finish_indexes()
            binding = current.binding
    async with publisher(case) as (session, driver), observe(case, session, driver) as inputs:
        async with reopen(case, session, driver, inputs, binding) as current:
            assert current.binding == binding
            with pytest.raises(ValueError, match="readonly_workspace"):
                await current.finish_indexes()
            with pytest.raises(ValueError, match="readonly_workspace"):
                await current.copy_rows("invalid", (), ())
            with pytest.raises(ValueError, match="original_task_required"):
                await asyncio.create_task(current.verify_current())
        with pytest.raises(ValueError, match="scope_ended"):
            await current.verify_current()
    for row in binding["tables"].values():
        assert await case.admin.fetchval(
            "SELECT to_regclass($1) IS NOT NULL", workspace.storage_relation(row["schema"], row["name"])
        )


async def test_native_stale_nine_output_binding_cannot_reopen_v2_workspace(native_workspace):
    case = native_workspace
    async with publisher(case) as (session, driver), observe(case, session, driver) as inputs:
        async with scope(case, session, driver, inputs) as current:
            await current.finish_indexes()
            binding = current.binding
    binding["contract"] = "cms-prepared-private-workspace-v1"
    for root in (workspace.storage.RESOURCE, workspace.storage.WITNESS):
        del binding["tables"][root]
    assert len(binding["tables"]) == 9
    async with publisher(case) as (session, driver), observe(case, session, driver) as inputs:
        with pytest.raises(ValueError):
            async with reopen(case, session, driver, inputs, binding):
                pytest.fail("stale nine-output workspace was accepted")
        assert await driver.fetchval("SELECT current_user") == case.policy.publisher_role


async def assert_peer_update_blocked(peer, update):
    with pytest.raises(asyncpg.LockNotAvailableError):
        async with peer.transaction():
            await peer.execute("SET LOCAL lock_timeout='50ms'")
            await peer.execute(update)


async def test_native_retained_outputs_exclude_peer_writes_through_outer_commit(native_workspace):
    """An original publisher peer keeps reads/unrelated writes, but cannot mutate pinned outputs."""
    case = native_workspace
    async with publisher(case) as (session, driver), observe(case, session, driver) as inputs:
        async with scope(case, session, driver, inputs) as current:
            await current.finish_indexes()
            binding = current.binding
    receipt = binding["tables"][workspace.OUTPUT_ROOTS[0]]
    relation = workspace.storage_relation(case.policy.schema, receipt["name"])
    update = f"UPDATE {relation} SET source_id=source_id WHERE false"
    peer = None
    try:
        peer = await asyncpg.connect(
            case.url.render_as_string(hide_password=False),
            user=case.policy.publisher_role,
            password=case.password,
            command_timeout=3,
        )
        assert await peer.execute(update) == "UPDATE 0"
        async with publisher(case) as (session, driver), observe(case, session, driver) as inputs:
            async with reopen(case, session, driver, inputs, binding) as current:
                async with peer.transaction():
                    await peer.execute(f"LOCK TABLE ONLY {relation} IN SHARE MODE NOWAIT")
                    assert await peer.fetchval(f"SELECT count(*) FROM {relation}") == 0
                await assert_peer_update_blocked(peer, update)
                await peer.execute("INSERT INTO accepted_catalog.transaction_probe VALUES(73)")
                await current.verify_current()
            await assert_peer_update_blocked(peer, update)
        assert await peer.execute(update) == "UPDATE 0"
    finally:
        if peer is not None:
            await peer.close()


@pytest.mark.parametrize("change", ["access", "missing_name"])
async def test_native_reopen_requires_real_access_to_every_retained_name(native_workspace, change):
    case = native_workspace
    async with publisher(case) as (session, driver), observe(case, session, driver) as inputs:
        async with scope(case, session, driver, inputs) as current:
            await current.finish_indexes()
            binding = current.binding
    receipt = next(iter(binding["tables"].values()))
    relation = workspace.storage_relation(case.policy.schema, receipt["name"])
    if change == "access":
        await case.admin.execute(f'ALTER TABLE {relation} OWNER TO "{case.roles["source_owner"]}"')
    else:
        await case.admin.execute(f'ALTER TABLE {relation} RENAME TO "missing_output"')
    error = asyncpg.InsufficientPrivilegeError if change == "access" else asyncpg.UndefinedTableError
    with pytest.raises(error):
        async with publisher(case) as (session, driver), observe(case, session, driver) as inputs:
            async with reopen(case, session, driver, inputs, binding):
                pytest.fail("unreadable retained output reopened")
