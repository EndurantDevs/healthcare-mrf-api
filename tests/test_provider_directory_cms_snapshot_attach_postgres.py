# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Two-witness attachment mechanics; no CMS authority or complete cut proof."""

import asyncio
from contextlib import asynccontextmanager

import asyncpg
import pytest
import pytest_asyncio

from db.plan_release_storage import storage_identifier, storage_relation
from process import provider_directory_cms_prepared_workspace as workspace
from process import provider_directory_cms_snapshot_attach as attach
from tests.test_provider_directory_cms_snapshot_attachment_postgres import _index_oids, _parent
from tests.test_provider_directory_cms_snapshot_custody_postgres import (
    _check,
    _relation,
    closed_payload_case,
    native_case,
    native_workspace,
    private_payload_case,
    publisher,
    seal_options,
)


async def _parents(case):
    oids_by_root = {}
    for ordinal, root in enumerate(workspace.storage.RELATIONS):
        old = await _parent(case.admin, case.storage.catalog_schema, root, ordinal)
        await case.admin.execute(f"ALTER TABLE {old} RENAME TO {storage_identifier(root)}")
        relation = _root(case, root)
        await case.admin.execute(f"ALTER TABLE {relation} OWNER TO {storage_identifier(case.roles['namespace'])}")
        await case.admin.execute(f"GRANT SELECT ON {relation} TO {storage_identifier(case.policy.publisher_role)}")
        oids_by_root[root] = await case.admin.fetchval("SELECT $1::regclass::oid::bigint", relation)
    return {root: oids_by_root[root] for root in attach.ROOTS}


def _root(case, root=workspace.storage.RESOURCE):
    return storage_relation(case.storage.catalog_schema, root)


@pytest_asyncio.fixture
async def attachment_case(closed_payload_case):
    case = closed_payload_case
    case.parent_oids = await _parents(case)
    async with publisher(case) as (_session, driver):
        await driver.execute("SET LOCAL search_path=pg_catalog,pg_temp")
        case.sealed = await _check(case, driver)
    return case


async def _attach(case, driver=None, **changes):
    options_by_name = dict(expected_seal=case.sealed, expected_parent_oids=case.parent_oids)
    options_by_name.update(changes)
    return await attach.attach_sealed_cms_payload_snapshots(driver or case.admin, seal_options(case), **options_by_name)


@asynccontextmanager
async def _admin(case):
    async with case.admin.transaction():
        await case.admin.execute("SET LOCAL search_path=pg_catalog,pg_temp")
        await case.admin.execute("SET LOCAL lock_timeout='500ms'")
        await case.admin.execute("SET LOCAL statement_timeout='2s'")
        yield case.admin


@asynccontextmanager
async def _peer(case):
    """Use an independent genuine login without borrowing a bound app session."""
    driver = await asyncpg.connect(
        case.url.set(username=case.policy.publisher_role, password=case.password).render_as_string(hide_password=False),
        command_timeout=5,
    )
    try:
        async with driver.transaction():
            await driver.execute("SET LOCAL search_path=pg_catalog,pg_temp")
            await driver.execute("SET LOCAL statement_timeout='2s'")
            yield driver
    finally:
        await driver.close()
        assert driver.is_closed()


async def _private(case, driver):
    """Compare genuine Publisher custody again after a rolled-back attempt."""
    assert await driver.fetchval("SELECT 1") == 1
    async with _peer(case) as fresh:
        assert await _check(case, fresh) == case.sealed


async def test_two_attach_preserves_indexes_and_reader(attachment_case):
    case = attachment_case
    before_by_root = {
        root: await _index_oids(case.admin, entry["oid"]) for root, entry in case.sealed["tables"].items()
    }
    async with _admin(case):
        result = await _attach(case)
        assert result["contract"] == attach.CONTRACT
        assert result["parents"] == case.parent_oids
        assert set(result["tables"]) == set(attach.ROOTS)
        for root, proof in result["tables"].items():
            assert proof["oid"] == case.sealed["tables"][root]["oid"]
            assert await _index_oids(case.admin, proof["oid"]) == before_by_root[root]
        for root, proof in case.sealed["tables"].items():
            if root not in attach.ROOTS:
                assert not await case.admin.fetchval(
                    "SELECT EXISTS(SELECT FROM pg_inherits WHERE inhrelid=$1)", proof["oid"]
                )
                assert await _index_oids(case.admin, proof["oid"]) == before_by_root[root]
    async with publisher(case) as (_session, driver):
        for root in workspace.storage.RELATIONS:
            assert await driver.fetchval("SELECT count(*) FROM " + _root(case, root)) == (
                1 if root in attach.ROOTS else 0
            )
        with pytest.raises(asyncpg.InsufficientPrivilegeError):
            async with driver.transaction():
                await driver.execute("DELETE FROM " + _root(case))


async def test_outer_rollback_restores_private_group(attachment_case):
    case = attachment_case
    async with _admin(case):
        outer = case.admin.transaction()
        await outer.start()
        await _attach(case)
        await outer.rollback()
        await _private(case, case.admin)


async def _old_child(case, root, dataset, *, default=False):
    relation = storage_relation(case.storage.catalog_schema, "retained_" + root)
    bound = (
        "DEFAULT"
        if default
        else "FOR VALUES IN (" + await case.admin.fetchval("SELECT quote_literal($1::text)", dataset) + ")"
    )
    await case.admin.execute(f"CREATE TABLE {relation} PARTITION OF {_root(case, root)} {bound}")
    return relation


async def test_retained_reader_and_disjoint_child_writer_continue(attachment_case):
    case = attachment_case
    old_dataset = "a" * 64
    child = await _old_child(case, workspace.storage.RESOURCE, old_dataset)
    columns = workspace._layouts()[workspace.storage.RESOURCE][1]
    selected = "$1::varchar," + ",".join(storage_identifier(column) for column in columns[1:])
    await case.admin.execute(f"INSERT INTO {child} SELECT {selected} FROM {_relation(case)}", old_dataset)
    await case.admin.execute(
        f"GRANT SELECT,INSERT,UPDATE,MAINTAIN ON {child} TO {storage_identifier(case.policy.publisher_role)}"
    )
    async with _peer(case) as reader, _peer(case) as writer:
        await writer.execute("LOCK TABLE ONLY " + child + " IN SHARE UPDATE EXCLUSIVE MODE NOWAIT")
        query = "SELECT payload_hash FROM " + _root(case) + " WHERE dataset_id=$1"
        assert await reader.fetchval(query, old_dataset) == "c" * 64
        async with _admin(case):
            await _attach(case)
            assert await reader.fetchval(query, old_dataset) == "c" * 64
            assert await writer.execute("UPDATE " + child + " SET payload_hash=repeat('e',64)") == "UPDATE 1"
        assert await reader.fetchval(query, old_dataset) == "c" * 64
    assert await case.admin.fetchval(query, old_dataset) == "e" * 64


async def test_busy_parent_refuses_nowait_and_releases_own_pins(attachment_case):
    case = attachment_case
    parent = _root(case, attach.ROOTS[0])
    async with publisher(case) as (_session, peer):
        await case.admin.execute(f"GRANT MAINTAIN ON {parent} TO {storage_identifier(case.policy.publisher_role)}")
        await peer.execute("LOCK TABLE ONLY " + parent + " IN SHARE UPDATE EXCLUSIVE MODE NOWAIT")
        async with _admin(case):
            with pytest.raises(asyncpg.LockNotAvailableError):
                await _attach(case)
            await _private(case, case.admin)
            assert not await case.admin.fetchval(
                "SELECT EXISTS(SELECT FROM pg_locks WHERE pid=pg_backend_pid() AND relation=ANY($1::oid[]))",
                list(case.parent_oids.values()) + [entry["oid"] for entry in case.sealed["tables"].values()],
            )


async def test_late_bound_rolls_back_both(attachment_case):
    case = attachment_case
    await _old_child(case, sorted(attach.ROOTS)[-1], case.snapshot_workspace["identity"]["dataset_id"])
    async with _admin(case):
        with pytest.raises(asyncpg.InvalidObjectDefinitionError, match="overlap"):
            await _attach(case)
        await _private(case, case.admin)


@pytest.mark.parametrize("drift", ["default", "index", "parent_oid", "row_hook"])
async def test_parent_drift_refuses_before_attachment(attachment_case, drift):
    case = attachment_case
    root = attach.ROOTS[0]
    parent = _root(case, root)
    if drift == "default":
        await _old_child(case, root, "", default=True)
    elif drift == "index":
        await case.admin.execute("CREATE INDEX unmatched_payload_idx ON " + parent + "(dataset_id)")
    elif drift == "parent_oid":
        case.parent_oids[root] += 1
    else:
        await case.admin.execute(
            "CREATE TRIGGER row_hook BEFORE UPDATE ON "
            + parent
            + " FOR EACH ROW EXECUTE FUNCTION pg_catalog.suppress_redundant_updates_trigger()"
        )
    async with _admin(case):
        with pytest.raises(ValueError, match="partition_shape_changed|parents_changed|index_changed"):
            await _attach(case)
        await _private(case, case.admin)


async def test_default_attached_between_preflight_and_final_locks_refuses(attachment_case, monkeypatch):
    case = attachment_case
    original = attach._compatible
    parent = _root(case, attach.ROOTS[0])
    observations = []
    peer = await asyncpg.connect(case.url.render_as_string(hide_password=False), command_timeout=5)
    default = storage_relation(case.storage.catalog_schema, "racing_default")

    async def attach_default_after_first_check(*args):
        result = await original(*args)
        observations.append(result)
        if len(observations) == 1:
            async with peer.transaction():
                await peer.execute("SET LOCAL lock_timeout='500ms'")
                await peer.execute(f"CREATE TABLE {default} (LIKE {parent} INCLUDING ALL)")
                await peer.execute(f"ALTER TABLE {parent} ATTACH PARTITION {default} DEFAULT")
        return result

    monkeypatch.setattr(attach, "_compatible", attach_default_after_first_check)
    try:
        async with _admin(case):
            with pytest.raises(ValueError, match="partition_shape_changed"):
                await _attach(case)
            assert len(observations) == 1
            await _private(case, case.admin)
            assert (
                await case.admin.fetchval(
                    "SELECT partdefid=$1::regclass FROM pg_catalog.pg_partitioned_table WHERE partrelid=$2::regclass",
                    default,
                    parent,
                )
                is True
            )
    finally:
        await peer.close(timeout=5)


async def test_unsealed_payloads_refuse(private_payload_case):
    case = private_payload_case
    case.parent_oids = await _parents(case)
    case.sealed = {}
    async with _admin(case):
        with pytest.raises(ValueError, match="rights_changed"):
            await _attach(case)


async def test_ordinary_publisher_cannot_invoke_admin_attachment(attachment_case):
    case = attachment_case
    async with publisher(case) as (_session, driver):
        await driver.execute("SET LOCAL search_path=pg_catalog,pg_temp")
        with pytest.raises(ValueError, match="independent_original_admin_required"):
            await _attach(case, driver)
        assert await _check(case, driver) == case.sealed


@pytest.mark.parametrize("failure", [RuntimeError, asyncio.CancelledError])
async def test_late_failure_or_cancellation_rolls_back_attached_group(attachment_case, monkeypatch, failure):
    case = attachment_case
    original = attach._attached

    async def fail_after_attach(*args):
        await original(*args)
        raise failure("injected after two native attachments")

    monkeypatch.setattr(attach, "_attached", fail_after_attach)
    async with _admin(case):
        with pytest.raises(failure):
            await _attach(case)
        await _private(case, case.admin)
