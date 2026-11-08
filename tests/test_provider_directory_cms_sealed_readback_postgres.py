# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Full private report after seal/ATTACH; no protected canonical publication proof."""

import asyncio
from copy import deepcopy

import pytest
import pytest_asyncio

from db.plan_release_storage import storage_identifier, storage_relation
from process import provider_directory_cms_prepared_outputs as outputs
from process import provider_directory_cms_snapshot_attach as attachment
from process import provider_directory_cms_snapshot_readback as readback
from process import provider_directory_cms_snapshot_seal as seal
from tests.test_provider_directory_cms_prepared_outputs_postgres import (
    full_private_case as full_private_case,
)
from tests.test_provider_directory_cms_prepared_outputs_postgres import (
    native_case as native_case,
)
from tests.test_provider_directory_cms_prepared_outputs_postgres import (
    native_derived_case as native_derived_case,
)
from tests.test_provider_directory_cms_prepared_outputs_postgres import (
    native_frozen_case as native_frozen_case,
)
from tests.test_provider_directory_cms_prepared_outputs_postgres import (
    observe,
    prepare,
    publisher,
)
from tests.test_provider_directory_cms_snapshot_attach_postgres import _admin, _old_child, _parents, _peer, _root
from tests.test_provider_directory_cms_snapshot_candidate_postgres import _target_admin_session
from tests.test_provider_directory_cms_snapshot_custody_postgres import _check, _relation, seal_options


@pytest_asyncio.fixture
async def sealed_report_case(full_private_case):
    """Commit real eleven-output preparation, six owner transfers and two attachments."""
    case = full_private_case
    async with publisher(case) as (session, driver), observe(case, session, driver) as inputs:
        async with prepare(case, session, driver, inputs) as (current, report):
            case.snapshot_workspace, case.snapshot_inputs = current.binding, deepcopy(inputs)
            case.private_report = deepcopy(report)
    case.parent_oids = await _parents(case)
    maintenance = "cms_sealed_reader_" + case.preparation_id.replace("-", "")[:12]
    has_created = False
    try:
        await case.admin.execute(
            f"CREATE ROLE {storage_identifier(maintenance)} LOGIN NOSUPERUSER NOCREATEDB "
            "NOCREATEROLE NOREPLICATION NOBYPASSRLS"
        )
        has_created = True
        await case.admin.execute(
            f"GRANT {storage_identifier(case.roles['namespace'])} TO {storage_identifier(maintenance)}"
        )
        case.snapshot_migration = maintenance
        async with _admin(case):
            await seal.seal_prepared_cms_payload_snapshots(case.admin, **seal_options(case))
        async with publisher(case) as (_session, driver):
            await driver.execute("SET LOCAL search_path=pg_catalog,pg_temp")
            case.sealed = await _check(case, driver)
        async with _admin(case):
            case.postimage = await attachment.attach_sealed_cms_payload_snapshots(
                case.admin, seal_options(case), expected_seal=case.sealed, expected_parent_oids=case.parent_oids
            )
        yield case
    finally:
        if has_created:
            await case.admin.execute("DROP ROLE " + storage_identifier(maintenance))


async def _read_report(case, session, driver):
    return await readback.read_sealed_cms_outputs(
        case.database,
        session,
        driver,
        seal_options(case),
        expected_seal=case.sealed,
        expected_attachment=case.postimage,
    )


async def _output_pins(case, driver):
    return await driver.fetchval(
        "SELECT count(DISTINCT relation) FROM pg_catalog.pg_locks "
        "WHERE pid=pg_catalog.pg_backend_pid() AND granted AND relation=ANY($1::oid[])",
        [proof["oid"] for proof in case.snapshot_workspace["tables"].values()],
    )


async def test_full_sealed_report_matches_original_eleven_output_receipts(sealed_report_case, monkeypatch):
    case = sealed_report_case
    original = deepcopy(case.snapshot_workspace)
    original_read = outputs.read_prepared_cms_outputs
    workspaces = []

    async def read_without_write_rights(current, handoff):
        workspaces.append(current)
        for operation in (current.copy_rows("invalid", (), ()), current.finish_indexes()):
            with pytest.raises(ValueError, match="readonly_workspace"):
                await operation
        return await original_read(current, handoff)

    monkeypatch.setattr(outputs, "read_prepared_cms_outputs", read_without_write_rights)
    async with _target_admin_session(case) as (session, driver):
        actual = await _read_report(case, session, driver)
        assert actual == case.private_report
        assert len(actual["tables"]) == 11
        assert actual["resource_counts"] == {family: len(case.raw[family]) for family in outputs._FAMILIES}
        assert len(case.raw) == 8 and set(actual["relationship_parity"]["family_counts"]) == set(case.raw)
        assert len(actual["frozen_content"]["content"]) == 8
        assert await _output_pins(case, driver) == 11
        assert await _read_report(case, session, driver) == actual
        assert all(not current._active for current in workspaces)
    assert case.snapshot_workspace == original
    actual["tables"].clear()
    assert case.snapshot_workspace == original


@pytest.mark.parametrize("change", ["owner", "layout", "parent", "acl", "descendant"])
async def test_current_sealed_postimage_drift_refuses_before_full_content(sealed_report_case, monkeypatch, change):
    case = sealed_report_case
    root = readback.workspace.storage.RESOURCE
    if change == "owner":
        await case.admin.execute(
            f"ALTER TABLE {_relation(case, root)} OWNER TO {storage_identifier(case.policy.owner_role)}"
        )
    elif change == "layout":
        await case.admin.execute(f"ALTER TABLE {_relation(case, root)} ADD COLUMN injected integer")
    elif change == "parent":
        root = attachment.ROOTS[0]
        await case.admin.execute(f"ALTER TABLE {_root(case, root)} DETACH PARTITION {_relation(case, root)}")
    elif change == "acl":
        await case.admin.execute(
            f"GRANT UPDATE(payload_hash) ON {_relation(case, root)} TO {storage_identifier(case.policy.publisher_role)}"
        )
    else:
        injected = storage_relation(case.policy.schema, "injected_child")
        await case.admin.execute(f"CREATE TABLE {injected}() INHERITS ({_relation(case, root)})")

    async def reject_content(*_args):
        pytest.fail("postimage refusal must precede content scans")

    monkeypatch.setattr(outputs, "read_prepared_cms_outputs", reject_content)
    async with _target_admin_session(case) as (session, driver):
        with pytest.raises(ValueError):
            await _read_report(case, session, driver)
        assert await _output_pins(case, driver) == 0
        assert await driver.fetchval("SELECT 1") == 1


async def test_prepared_publisher_cannot_reopen_as_original_admin(sealed_report_case):
    case = sealed_report_case
    async with publisher(case) as (session, driver):
        await driver.execute("SET LOCAL search_path=pg_catalog,pg_temp")
        with pytest.raises(ValueError, match="independent_original_admin_required"):
            await _read_report(case, session, driver)
        assert await _output_pins(case, driver) == 0


@pytest.mark.parametrize("failure", [RuntimeError, asyncio.CancelledError])
async def test_full_content_failure_releases_pins_and_allows_same_transaction_retry(
    sealed_report_case, monkeypatch, failure
):
    case = sealed_report_case
    original = outputs.read_prepared_cms_outputs

    async def fail_after_content(*args):
        await original(*args)
        raise failure("after genuine full private report")

    async with _target_admin_session(case) as (session, driver):
        await driver.execute("CREATE TEMP TABLE caller_probe(id integer)")
        await driver.execute("INSERT INTO caller_probe VALUES(1)")
        monkeypatch.setattr(outputs, "read_prepared_cms_outputs", fail_after_content)
        with pytest.raises(failure):
            await _read_report(case, session, driver)
        assert await _output_pins(case, driver) == 0
        assert await driver.fetchval("SELECT count(*) FROM caller_probe") == 1
        monkeypatch.setattr(outputs, "read_prepared_cms_outputs", original)
        assert await _read_report(case, session, driver) == case.private_report


async def test_full_read_keeps_retained_reader_and_disjoint_writer_available(sealed_report_case):
    case = sealed_report_case
    root = readback.workspace.storage.RESOURCE
    old = await _old_child(case, root, "incumbent")
    await case.admin.execute(
        f"INSERT INTO {old} SELECT 'incumbent',resource_type,resource_id,payload_hash,payload_json FROM {_relation(case, root)}"
    )
    await case.admin.execute(f"GRANT SELECT,UPDATE ON {old} TO {storage_identifier(case.policy.publisher_role)}")
    async with _target_admin_session(case) as (session, driver), _peer(case) as peer:
        assert await _read_report(case, session, driver) == case.private_report
        assert await peer.fetchval(f"SELECT count(*) FROM {_root(case, root)} WHERE dataset_id='incumbent'") > 0
        assert await peer.execute(f"UPDATE {old} SET payload_hash=repeat('e',64)") != "UPDATE 0"
        assert await _read_report(case, session, driver) == case.private_report
