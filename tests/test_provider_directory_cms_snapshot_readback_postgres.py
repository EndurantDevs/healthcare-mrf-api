# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Attached metadata components only; no protected CMS finisher or writer authority."""

import asyncio
from copy import deepcopy

import asyncpg
import pytest
import pytest_asyncio

from db.plan_release_storage import storage_identifier, storage_relation
from process import provider_directory_cms_snapshot_readback as readback
from tests.test_provider_directory_cms_snapshot_attach_postgres import (
    _admin,
    _attach,
    _old_child,
    _peer,
    _relation,
    _root,
    attachment_case,
    closed_payload_case,
    native_case,
    native_workspace,
    private_payload_case,
    publisher,
    seal_options,
)


@pytest_asyncio.fixture
async def attached_case(attachment_case):
    case = attachment_case
    async with _admin(case):
        case.postimage = await _attach(case)
    return case


async def _read(case, connection, expected=None):
    return await readback.read_attached_cms_payload_snapshots(
        connection, seal_options(case), expected_attachment=case.postimage if expected is None else expected
    )


async def _payload_locks(connection, case):
    return await connection.fetchval(
        "SELECT count(*) FROM pg_catalog.pg_locks WHERE pid=pg_catalog.pg_backend_pid() AND relation=ANY($1::oid[])",
        list(case.parent_oids.values()) + [proof["oid"] for proof in case.postimage["tables"].values()],
    )


async def test_fresh_admin_reads_attached_postimage_without_rewriting_private_receipts(attached_case):
    case = attached_case
    original = deepcopy(case.snapshot_workspace)
    fresh = await asyncpg.connect(case.url.render_as_string(hide_password=False), command_timeout=5)
    try:
        async with fresh.transaction():
            await fresh.execute("SET LOCAL search_path=pg_catalog,pg_temp")
            actual = await _read(case, fresh)
            assert actual == case.postimage and actual is not case.postimage
            assert await _read(case, fresh) == actual
            assert await _payload_locks(fresh, case) == 4
            for root, proof in actual["tables"].items():
                assert proof["owner"] != original["tables"][root]["owner"]
                assert proof["layout_sha256"] != original["tables"][root]["layout_sha256"]
        assert case.snapshot_workspace == original
    finally:
        await fresh.close(timeout=5)


@pytest.mark.parametrize("change", ["parent", "owner", "layout", "missing", "private_receipt"])
async def test_forged_or_private_postimage_refuses(attached_case, change):
    case = attached_case
    expected = deepcopy(case.postimage)
    root = readback.attach.ROOTS[0]
    if change == "parent":
        expected["parents"][root] += 1
    elif change == "owner":
        expected["tables"][root]["owner"] += 1
    elif change == "layout":
        expected["tables"][root]["layout_sha256"] = "0" * 64
    elif change == "missing":
        del expected["tables"][root]
    else:
        expected["tables"] = deepcopy(case.snapshot_workspace["tables"])
        expected["tables"] = {name: expected["tables"][name] for name in case.postimage["tables"]}
    async with _admin(case):
        with pytest.raises(ValueError):
            await _read(case, case.admin, expected)
        assert await _payload_locks(case.admin, case) == 0
        assert await _read(case, case.admin) == case.postimage


@pytest.mark.parametrize("change", ["detach", "write_grant", "column"])
async def test_native_attached_drift_refuses(attached_case, change):
    case = attached_case
    root = readback.attach.ROOTS[0]
    relation = _relation(case, root)
    if change == "detach":
        await case.admin.execute(f"ALTER TABLE {_root(case, root)} DETACH PARTITION {relation}")
    elif change == "write_grant":
        await case.admin.execute(
            f"GRANT UPDATE(raw_payload_sha256) ON {relation} TO {storage_identifier(case.policy.publisher_role)}"
        )
    else:
        await case.admin.execute(f"ALTER TABLE {_root(case, root)} ADD COLUMN injected integer")
    async with _admin(case):
        with pytest.raises(ValueError):
            await _read(case, case.admin)
        assert await _payload_locks(case.admin, case) == 0


async def test_ordinary_publisher_cannot_use_admin_readback(attached_case):
    case = attached_case
    async with publisher(case) as (_session, connection):
        await connection.execute("SET LOCAL search_path=pg_catalog,pg_temp")
        with pytest.raises(ValueError, match="independent_original_admin_required"):
            await _read(case, connection)
        assert await _payload_locks(connection, case) == 0


async def test_busy_attached_heap_refuses_nowait_and_releases_partial_pins(attached_case):
    case = attached_case
    last = max(proof["name"] for proof in case.postimage["tables"].values())
    relation = storage_relation(case.storage.storage_schema, last)
    peer = await asyncpg.connect(case.url.render_as_string(hide_password=False), command_timeout=5)
    try:
        async with peer.transaction():
            await peer.execute("LOCK TABLE ONLY " + relation + " IN ACCESS EXCLUSIVE MODE NOWAIT")
            async with _admin(case):
                with pytest.raises(asyncpg.LockNotAvailableError):
                    await _read(case, case.admin)
                assert await _payload_locks(case.admin, case) == 0
                assert await case.admin.fetchval("SELECT 1") == 1
        async with _admin(case):
            assert await _read(case, case.admin) == case.postimage
    finally:
        await peer.close(timeout=5)


@pytest.mark.parametrize("failure", [RuntimeError, asyncio.CancelledError])
async def test_readback_failure_releases_only_its_pins_and_preserves_outer_work(attached_case, monkeypatch, failure):
    case = attached_case
    original = readback._observe

    async def fail_after_observation(*args):
        await original(*args)
        raise failure("after native attached observation")

    monkeypatch.setattr(readback, "_observe", fail_after_observation)
    async with _admin(case):
        await case.admin.execute("CREATE TEMP TABLE caller_probe(id integer)")
        await case.admin.execute("INSERT INTO caller_probe VALUES(1)")
        with pytest.raises(failure):
            await _read(case, case.admin)
        assert await _payload_locks(case.admin, case) == 0
        assert await case.admin.fetchval("SELECT count(*) FROM caller_probe") == 1
        monkeypatch.setattr(readback, "_observe", original)
        assert await _read(case, case.admin) == case.postimage


async def test_readback_pins_leave_retained_reader_and_disjoint_writer_available(attached_case):
    case = attached_case
    root = readback.workspace.storage.RESOURCE
    old = await _old_child(case, root, "incumbent")
    await case.admin.execute(
        f"INSERT INTO {old} SELECT 'incumbent',resource_type,resource_id,payload_hash,payload_json FROM {_relation(case)}"
    )
    await case.admin.execute(f"GRANT SELECT,UPDATE ON {old} TO {storage_identifier(case.policy.publisher_role)}")
    async with _peer(case) as peer, _admin(case):
        assert await _read(case, case.admin) == case.postimage
        assert await peer.fetchval(f"SELECT count(*) FROM {_root(case)} WHERE dataset_id='incumbent'") == 1
        assert await peer.execute(f"UPDATE {old} SET payload_hash=repeat('e',64)") == "UPDATE 1"
        assert await _read(case, case.admin) == case.postimage
