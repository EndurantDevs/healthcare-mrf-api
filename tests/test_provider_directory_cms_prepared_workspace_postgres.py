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
        oid = current.receipts[root]["oid"]
        bound = await current.connection.fetchrow(
            "SELECT convalidated,conenforced,pg_get_expr(conbin,conrelid,true) AS expression "
            "FROM pg_constraint WHERE conrelid=$1 AND conname='pd_dataset_bound' AND contype='c'",
            oid,
        )
        assert bound is not None and bound["convalidated"] and bound["conenforced"]
        assert bound["expression"] == f"dataset_id::text = {literal}::text"
        assert (
            await current.connection.fetchval(
                "SELECT count(*) FROM pg_constraint WHERE conrelid=$1 AND contype='f'", oid
            )
            == 0
        )


async def test_native_fixed_outputs_copy_and_indexes_keep_publisher_and_source_binding(native_workspace):
    case = native_workspace
    ambient = (source._FACTORY.get(), source._PUBLISHER.get(), source._OWNER.get())
    async with publisher(case) as (session, driver), observe(case, session, driver) as inputs:
        async with scope(case, session, driver, inputs) as current:
            original_owner = case.storage.owner_oid
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
                        "SELECT count(*) FROM pg_index WHERE indrelid=$1 AND indisprimary", before[root]["oid"]
                    )
                    == 1
                )
            after = await current.finish_indexes()
            assert all(before[root]["oid"] == after[root]["oid"] for root in before)
            assert all(before[root]["layout_sha256"] != after[root]["layout_sha256"] for root in before)
            assert current.binding["indexed"]
            await assert_payload_snapshot_bounds(current)
            assert case.storage.owner_oid == original_owner
            plan = current.receipts[workspace.storage.RELATIONS[1]]["name"]
            assert dict(await driver.fetchrow(f'SELECT plan_active,plan_identifier FROM private_source."{plan}"')) == {
                "plan_active": True,
                "plan_identifier": "example",
            }
            assert not await driver.fetchval(
                "SELECT EXISTS(SELECT FROM pg_trigger WHERE tgrelid=ANY($1::oid[]))",
                [receipt["oid"] for receipt in after.values()],
            )
            for root, field in (
                (workspace.storage.RESOURCE, "payload_json"),
                (workspace.storage.RELATIONS[1], "payload_json"),
                (workspace.storage.WITNESS, "raw_payload_json"),
            ):
                relation = f'private_source."{after[root]["name"]}"'
                assert await driver.fetchval(f"SELECT pg_typeof({field})::text FROM {relation}") == "jsonb"
                assert (
                    await driver.fetchval(
                        "SELECT count(*) FROM pg_constraint WHERE conrelid=$1 AND contype='f'", after[root]["oid"]
                    )
                    == 0
                )
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


async def test_native_default_reader_acl_is_refused_without_grant_repair(native_workspace):
    case = native_workspace
    await case.admin.execute(
        f'ALTER DEFAULT PRIVILEGES FOR ROLE "{case.policy.owner_role}" IN SCHEMA private_source '
        f'GRANT SELECT ON TABLES TO "{case.roles["reader"]}"'
    )
    async with publisher(case) as (session, driver), observe(case, session, driver) as inputs:
        with pytest.raises(ValueError, match="storage_custody_changed"):
            async with scope(case, session, driver, inputs):
                pytest.fail("default reader grant was accepted")
        assert await driver.fetchval("SELECT current_user") == case.policy.publisher_role
    await assert_no_outputs(case)


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


async def test_native_workspace_rejects_child_use_and_owner_scope_leaks(native_workspace):
    case = native_workspace
    async with publisher(case) as (session, driver), observe(case, session, driver) as inputs:
        with pytest.raises(ValueError, match="original_publisher_changed"):
            async with scope(case, session, driver, inputs) as current:
                with pytest.raises(ValueError, match="original_task_required"):
                    await asyncio.create_task(current.verify_current())
                await driver.execute(f'SET LOCAL ROLE "{case.policy.owner_role}"')
                await current.verify_current()
        assert await driver.fetchval("SELECT current_user") == case.policy.publisher_role
    await assert_no_outputs(case)


async def test_native_late_frozen_drift_at_observer_exit_removes_outputs(native_workspace):
    """Caller-caught exit rejection must roll back allocation and frozen changes."""
    case = native_workspace
    frozen = case.inventory["relations"][0]
    relation = f'private_source."{frozen["relation_name"]}"'
    async with publisher(case) as (session, driver), observe(case, session, driver) as inputs:
        with pytest.raises(ValueError, match="frozen_inventory_changed"):
            async with scope(case, session, driver, inputs) as current:
                assert len(current.receipts) == 11
                await driver.execute(f"TRUNCATE TABLE {relation}")
                assert (
                    await driver.fetchval(
                        "SELECT relfilenode::bigint FROM pg_class WHERE oid=$1", frozen["relation_oid"]
                    )
                    != frozen["relfilenode"]
                )
                # The normal body can finish; only the observer's exit recheck
                # sees this input drift after the inner allocation savepoint.
                await current.verify_current()
        assert await driver.fetchval("SELECT current_user") == case.policy.publisher_role
        assert (
            await driver.fetchval("SELECT relfilenode::bigint FROM pg_class WHERE oid=$1", frozen["relation_oid"])
            == frozen["relfilenode"]
        )
        assert all(
            [
                await driver.fetchval(f'SELECT id=42 FROM private_source."{inventory_relation["relation_name"]}"')
                for inventory_relation in case.inventory["relations"]
            ]
        )
        assert (
            await driver.fetchval(
                "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
                "WHERE n.nspname='private_source' AND c.relkind='r'"
            )
            == 8
        )


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
    assert {row["oid"] for row in binding["tables"].values()} == {
        await case.admin.fetchval("SELECT to_regclass($1)::oid", "private_source." + row["name"])
        for row in binding["tables"].values()
    }


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


@pytest.mark.parametrize("change", ["owner", "index"])
async def test_native_reopen_refuses_retained_object_drift(native_workspace, change):
    case = native_workspace
    async with publisher(case) as (session, driver), observe(case, session, driver) as inputs:
        async with scope(case, session, driver, inputs) as current:
            await current.finish_indexes()
            binding = current.binding
    receipt = next(iter(binding["tables"].values()))
    if change == "owner":
        await case.admin.execute(
            f'ALTER TABLE private_source."{receipt["name"]}" OWNER TO "{case.roles["source_owner"]}"'
        )
    else:
        index = await case.admin.fetchval(
            "SELECT i.indexrelid::regclass::text FROM pg_index i WHERE i.indrelid=$1 AND NOT i.indisprimary LIMIT 1",
            receipt["oid"],
        )
        assert index
        await case.admin.execute("DROP INDEX " + index)
    with pytest.raises(ValueError, match="output_changed|storage_"):
        async with publisher(case) as (session, driver), observe(case, session, driver) as inputs:
            async with reopen(case, session, driver, inputs, binding):
                pytest.fail("drifted retained output reopened")
