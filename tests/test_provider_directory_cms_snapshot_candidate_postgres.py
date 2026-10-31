# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Atomic snapshot/header mechanics; no profile, claim or CMS activation authority."""

import asyncio
from contextlib import asynccontextmanager
from copy import deepcopy
from types import SimpleNamespace

import asyncpg
import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from db import migration_cms_snapshot_allocator as allocator
from db.migration_generic_source_candidates import _canonical_copy_guard_bodies
from db.migration_provider_directory_dataset_candidates import _guard_partition_writes
from db.plan_release_storage import storage_identifier, storage_relation
from process import provider_directory_cms_prepared_payload_publication as payload_copy
from process import provider_directory_cms_prepared_workspace as workspace
from process import provider_directory_cms_snapshot_candidate as candidate
from process import provider_directory_cms_snapshot_custody as custody
from process import provider_directory_cms_snapshot_seal as seal
from process.provider_directory_cms_admin_workspace import open_admin_cms_workspace
from process.provider_directory_cms_prepared_allocation import allocate_prepared_cms_candidate
from tests.test_provider_directory_cms_prepared_candidate_postgres import (
    _canonical_controls,
    _complete_frozen_header,
    full_private_case,
    native_case,
    native_derived_case,
    native_frozen_case,
    observe,
    prepare_outputs,
    publication_case,
    publisher,
)
from tests.test_provider_directory_cms_prepared_relationships_postgres import input_snapshots
from tests.test_provider_directory_cms_snapshot_attach_postgres import _admin, _parents, _peer, _private
from tests.test_provider_directory_cms_snapshot_custody_postgres import _check, seal_options


async def _allocator(case):
    guard = storage_relation(case.storage.catalog_schema, "guard_pd_dataset_candidate_parent")
    guard_body = _canonical_copy_guard_bodies(case.storage.catalog_schema, case.policy.schema)[1]
    await case.admin.execute(
        f"CREATE FUNCTION {guard}() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER "
        "SET search_path=pg_catalog AS $fn$" + guard_body + "$fn$;"
    )
    await case.admin.execute(f"ALTER FUNCTION {guard}() OWNER TO {storage_identifier(case.roles['namespace'])}")
    await case.admin.execute(f"REVOKE ALL ON FUNCTION {guard}() FROM PUBLIC")
    relation = storage_relation(case.storage.catalog_schema, "prepare_pd_generic_dataset_storage")
    body = allocator.allocator_bodies(case.storage.catalog_schema)[0]
    await case.admin.execute(
        f"CREATE FUNCTION {relation}() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER "
        "SET search_path=pg_catalog AS $fn$" + body + "$fn$;"
    )
    await case.admin.execute(f"ALTER FUNCTION {relation}() OWNER TO {storage_identifier(case.roles['namespace'])}")
    await case.admin.execute(f"REVOKE ALL ON FUNCTION {relation}() FROM PUBLIC")
    header = storage_relation(case.storage.catalog_schema, workspace.storage.HEADER)
    await case.admin.execute(
        f"CREATE TRIGGER pd_dataset_storage AFTER INSERT ON {header} FOR EACH ROW EXECUTE FUNCTION {relation}()"
    )
    expected_by_field = dict(
        await case.admin.fetchrow(
            "SELECT oid::bigint,proowner::bigint AS owner_oid FROM pg_catalog.pg_proc WHERE oid=$1::regprocedure",
            relation + "()",
        )
    )
    return expected_by_field


async def _retained_rows(case, connection=None):
    driver = connection or case.admin
    return {
        root: await driver.fetchval(
            "SELECT coalesce(jsonb_agg(to_jsonb(r) ORDER BY to_jsonb(r)::text),'[]'::jsonb) FROM "
            + storage_relation(case.storage.catalog_schema, root)
            + " r WHERE dataset_id='incumbent'"
        )
        for root in workspace.storage.RELATIONS
    }


async def _retained_resource(case):
    """Seed an incumbent in the future native parent class before the tested cut."""
    parent = storage_relation(case.storage.catalog_schema, workspace.storage.RESOURCE)
    history = storage_relation(case.storage.catalog_schema, "retained_resource")
    await case.admin.execute(f"CREATE TABLE {history} PARTITION OF {parent} FOR VALUES IN ('incumbent')")
    await case.admin.execute(f"ALTER TABLE {history} OWNER TO {storage_identifier(case.roles['namespace'])}")
    await case.admin.execute(
        f"INSERT INTO {parent}(dataset_id,resource_type,resource_id,payload_hash,payload_json) "
        "VALUES('incumbent','Practitioner','incumbent',repeat('a',64),'{}'::jsonb)"
    )


@pytest_asyncio.fixture
async def snapshot_case(publication_case, monkeypatch):
    case = publication_case
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", case.storage.catalog_schema)
    async with case.admin.transaction():
        frozen = await _complete_frozen_header(case)
        await _canonical_controls(case, frozen)
    case.input_snapshot = await input_snapshots(case)
    async with publisher(case) as (session, driver), observe(case, session, driver) as inputs:
        async with prepare_outputs(case, session, driver, inputs) as (current, _report):
            case.snapshot_workspace, case.snapshot_inputs = current.binding, deepcopy(inputs)
    case.parent_oids = await _parents(case)
    await _retained_resource(case)
    case.retained_payloads = await _retained_rows(case)
    case.allocator = await _allocator(case)
    maintenance = "cms_snapshot_maintenance_" + case.preparation_id.replace("-", "")[:12]
    has_created = False
    try:
        await case.admin.execute(
            f"CREATE ROLE {storage_identifier(maintenance)} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS"
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
        yield case
    finally:
        if has_created:
            await case.admin.execute("DROP ROLE " + storage_identifier(maintenance))


async def _attach(case, connection=None):
    return await candidate.attach_cms_snapshot_candidate(
        connection or case.admin,
        seal_options(case),
        expected_seal=case.sealed,
        expected_parent_oids=case.parent_oids,
    )


async def _new_header(case):
    return await case.admin.fetchrow(
        "SELECT * FROM "
        + storage_relation(case.storage.catalog_schema, workspace.storage.HEADER)
        + " WHERE dataset_id=$1",
        case.handoff.document["identity"]["dataset_id"],
    )


async def test_two_snapshots_allocate_four_empty_generic_leaves(snapshot_case):
    case = snapshot_case
    header_table = storage_relation(case.storage.catalog_schema, workspace.storage.HEADER)
    incumbent = await case.admin.fetchrow(f"SELECT * FROM {header_table} WHERE dataset_id='incumbent'")
    async with _admin(case):
        evidence = await _attach(case)
        assert evidence["contract"] == candidate.CONTRACT
        assert evidence["header"]["status"] == "acquiring" and evidence["header"]["is_current"] is False
    assert await _new_header(case) is not None
    assert await case.admin.fetchrow(f"SELECT * FROM {header_table} WHERE dataset_id='incumbent'") == incumbent
    assert await _retained_rows(case) == case.retained_payloads
    assert (
        await case.admin.fetchval(
            "SELECT count(*) FROM pg_catalog.pg_class WHERE relnamespace=pg_catalog.to_regnamespace($1) "
            "AND relname=ANY(ARRAY(SELECT 'pd_generic_'||md5(root||':'||$2) FROM unnest($3::text[]) root))",
            case.storage.catalog_schema,
            case.handoff.document["identity"]["dataset_id"],
            list(workspace.storage.RELATIONS[:4]),
        )
        == 4
    )
    assert (
        await case.admin.fetchval("SELECT prosrc FROM pg_proc WHERE oid=$1::oid", case.allocator["oid"])
        == allocator.allocator_bodies(case.storage.catalog_schema)[0]
    )
    for root in workspace.storage.RELATIONS[:4]:
        assert (
            await case.admin.fetchval(
                "SELECT count(*) FROM " + storage_relation(case.storage.catalog_schema, root) + " WHERE dataset_id=$1",
                evidence["header"]["dataset_id"],
            )
            == 0
        )
    async with publisher(case) as (_session, driver):
        for root, proof in evidence["snapshots"]["tables"].items():
            expected = await driver.fetchval(
                "SELECT count(*) FROM " + storage_relation(case.policy.schema, proof["name"])
            )
            assert expected > 0
            assert (
                await driver.fetchval(
                    "SELECT count(*) FROM "
                    + storage_relation(case.storage.catalog_schema, root)
                    + " WHERE dataset_id=$1",
                    evidence["header"]["dataset_id"],
                )
                == expected
            )


@pytest.mark.parametrize("failure", [RuntimeError, asyncio.CancelledError])
async def test_late_header_failure_restores_private_snapshots(snapshot_case, monkeypatch, failure):
    case = snapshot_case
    original = candidate._header

    async def fail_after_header(*args):
        await original(*args)
        raise failure("after real header insertion")

    monkeypatch.setattr(candidate, "_header", fail_after_header)
    async with _admin(case):
        with pytest.raises(failure):
            await _attach(case)
        assert await _new_header(case) is None
        await _private(case, case.admin)


async def test_outer_rollback_restores_private_snapshots_and_removes_header(snapshot_case):
    case = snapshot_case
    async with _admin(case):
        savepoint = case.admin.transaction()
        await savepoint.start()
        await _attach(case)
        await savepoint.rollback()
        assert await _new_header(case) is None
        await _private(case, case.admin)


async def test_original_publisher_cannot_allocate_snapshot_header(snapshot_case):
    case = snapshot_case
    async with publisher(case) as (_session, driver):
        await driver.execute("SET LOCAL search_path=pg_catalog,pg_temp")
        with pytest.raises(ValueError, match="independent_original_admin_required"):
            await _attach(case, driver)
        assert await _check(case, driver) == case.sealed
    assert await _new_header(case) is None


async def _guarded_target_setup(case):
    """Install the existing parent hooks and their minimal catalog inputs in this fixture."""
    catalog, private = map(storage_identifier, (case.storage.catalog_schema, case.policy.schema))
    await case.admin.execute(
        f"CREATE TABLE {private}.preparation(importer_id varchar(128),state varchar(16),handoff jsonb,publication jsonb)"
    )
    await case.admin.execute(f"ALTER TABLE {private}.preparation OWNER TO {storage_identifier(case.policy.owner_role)}")
    await case.admin.execute(
        f"GRANT SELECT(importer_id,state,handoff,publication) ON {private}.preparation "
        f"TO {storage_identifier(case.roles['namespace'])}"
    )
    await case.admin.execute(f"CREATE TABLE {catalog}.pd_dataset_migration_plan(singleton boolean,plan jsonb)")
    await case.admin.execute(
        f"ALTER TABLE {catalog}.pd_dataset_migration_plan OWNER TO {storage_identifier(case.roles['namespace'])}"
    )
    for root in workspace.storage.RELATIONS[:4]:
        relation = storage_relation(case.storage.catalog_schema, root)
        statements = []
        _guard_partition_writes(SimpleNamespace(execute=statements.append), case.storage.catalog_schema, relation, root)
        for statement in statements:
            await case.admin.execute(statement)
        await case.admin.execute(
            f"GRANT INSERT,UPDATE,DELETE ON {relation} TO {storage_identifier(case.policy.publisher_role)}"
        )


@asynccontextmanager
async def _target_admin_session(case):
    """Borrow one genuine original-admin SQLAlchemy/native outer transaction."""
    engine = create_async_engine(case.url.set(drivername="postgresql+asyncpg"))
    try:
        async with AsyncSession(bind=engine) as session, session.begin(), case.database.bind_existing_session(session):
            await session.execute(text("SET LOCAL search_path=pg_catalog,pg_temp"))
            await session.execute(text("SET LOCAL lock_timeout='500ms'"))
            await session.execute(text("SET LOCAL statement_timeout='2s'"))
            driver = (await (await session.connection()).get_raw_connection()).driver_connection
            yield session, driver
    finally:
        await engine.dispose()
        assert case.database._transaction_binding() is None


async def _copy_four_allocated_targets(case, session, driver):
    """Exercise the existing bounded COPY/parity against all four real allocator leaves."""
    request, retained = candidate.attachment._request(seal_options(case))
    actor = await candidate.attachment._actor(driver, request, retained)

    async def verify():
        assert await candidate.attachment._actor(driver, request, retained) == actor
        assert (
            await custody._observe(
                driver,
                case.storage,
                retained,
                request["owner_role"],
                request["owner_oid"],
                request["migration_role"],
            )
            == case.sealed
        )

    current = SimpleNamespace(
        connection=driver,
        session=session,
        policy=case.policy,
        catalog_schema=case.storage.catalog_schema,
        identity=retained["identity"],
        verify_current=verify,
    )
    counts_by_root = {}
    for root in workspace.storage.RELATIONS[:4]:
        receipt = case.sealed["tables"][root]
        counts_by_root[root] = await payload_copy._copy_table(current, root, receipt)
        assert counts_by_root[root] == await payload_copy._parity(current, root, receipt) and counts_by_root[root] > 0
    await verify()
    return counts_by_root


async def _refuse_peer_target_writes(case, peer):
    """Both parent and direct-child paths must fail under the candidate-only pins."""
    root = workspace.storage.RESOURCE
    parent = storage_relation(case.storage.catalog_schema, root)
    child = await case.admin.fetchval(
        "SELECT c.relname FROM pg_class c JOIN pg_inherits i ON i.inhrelid=c.oid "
        "WHERE i.inhparent=$1::regclass AND c.relname='pd_generic_'||md5($2::text||':'||$3::text)",
        parent,
        root,
        case.handoff.document["identity"]["dataset_id"],
    )
    for relation in (parent, storage_relation(case.storage.catalog_schema, child)):
        for sql in (
            f"UPDATE {relation} SET resource_id=resource_id WHERE dataset_id=$1",
            f"DELETE FROM {relation} WHERE dataset_id=$1",
        ):
            with pytest.raises(asyncpg.LockNotAvailableError):
                async with peer.transaction():
                    await peer.execute("SET LOCAL lock_timeout='75ms'")
                    await peer.execute(sql, case.handoff.document["identity"]["dataset_id"])


async def _target_children(case, roots):
    children = await case.admin.fetch(
        "SELECT c.oid::bigint,c.relname FROM pg_class c WHERE c.relnamespace=to_regnamespace($1) "
        "AND c.relname=ANY(SELECT 'pd_generic_'||md5(root||':'||$2) FROM unnest($3::text[]) root) ORDER BY c.relname",
        case.storage.catalog_schema,
        case.handoff.document["identity"]["dataset_id"],
        list(roots),
    )
    assert len(children) == 4
    for child in children:
        await case.admin.execute(
            f"GRANT SELECT,INSERT,UPDATE,DELETE ON {storage_relation(case.storage.catalog_schema, child['relname'])} "
            f"TO {storage_identifier(case.policy.publisher_role)}"
        )
    return children


async def _pin_target_preflight(case, driver, children, pin):
    names = ",".join("ONLY " + storage_relation(case.storage.catalog_schema, row["relname"]) for row in children)
    mode = "SHARE UPDATE EXCLUSIVE" if pin == "update_exclusive" else "SHARE ROW EXCLUSIVE"
    await driver.execute("LOCK TABLE " + names + " IN " + mode + " MODE NOWAIT")
    header = storage_relation(case.storage.catalog_schema, workspace.storage.HEADER)
    await driver.fetchrow(
        f"SELECT dataset_id FROM {header} WHERE dataset_id=$1 FOR UPDATE NOWAIT",
        case.handoff.document["identity"]["dataset_id"],
    )


async def _parallel_target_reader_and_allocator(case, peer, counts):
    header = storage_relation(case.storage.catalog_schema, workspace.storage.HEADER)
    for root, count in counts.items():
        relation = storage_relation(case.storage.catalog_schema, root)
        assert (
            await peer.fetchval(
                f"SELECT count(*) FROM {relation} WHERE dataset_id=$1",
                case.handoff.document["identity"]["dataset_id"],
            )
            == count
        )
    assert (
        await peer.execute(
            f"INSERT INTO {header} SELECT (jsonb_populate_record(NULL::{header},to_jsonb(h)||"
            "jsonb_build_object('dataset_id','parallel','endpoint_id','parallel-endpoint'))).* "
            f"FROM {header} h WHERE dataset_id=$1",
            case.handoff.document["identity"]["dataset_id"],
        )
        == "INSERT 0 1"
    )
    assert (
        await peer.fetchval(
            "SELECT count(*) FROM pg_class c WHERE c.relnamespace=to_regnamespace($1) "
            "AND c.relname=ANY(SELECT 'pd_generic_'||md5(root||':parallel') FROM unnest($2::text[]) root)",
            case.storage.catalog_schema,
            list(counts),
        )
        == 4
    )


@pytest.mark.parametrize("pin", ["update_exclusive", "row_exclusive"])
async def test_committed_allocation_pins_keep_four_copies_stable_and_disjoint_attach_available(
    snapshot_case, monkeypatch, pin, record_property
):
    """Native target mechanics only; no CMS authority, retained accounting or history cut."""
    case = snapshot_case
    await _guarded_target_setup(case)
    async with _admin(case):
        await candidate._header(case.admin, seal_options(case))
    monkeypatch.setattr(payload_copy, "COPY_MAX_ROWS", 2)
    async with _target_admin_session(case) as (session, driver):
        counts = await _copy_four_allocated_targets(case, session, driver)
    children = await _target_children(case, counts)
    async with _target_admin_session(case) as (_session, driver), _peer(case) as peer:
        await _pin_target_preflight(case, driver, children, pin)
        await _refuse_peer_target_writes(case, peer)
        await _parallel_target_reader_and_allocator(case, peer, counts)
        assert not await driver.fetchval(
            "SELECT EXISTS(SELECT FROM pg_locks l JOIN pg_class c ON c.oid=l.relation WHERE l.pid=pg_backend_pid() "
            "AND c.relnamespace=to_regnamespace($1) AND c.relname=ANY($2::text[]) "
            "AND l.mode IN ('ShareUpdateExclusiveLock','ShareLock','ShareRowExclusiveLock','ExclusiveLock','AccessExclusiveLock'))",
            case.storage.catalog_schema,
            list(counts),
        )
    record_property("genuine_four_target_copy_counts", counts)
    record_property("pin_kind", pin)
    assert await _retained_rows(case) == case.retained_payloads


@asynccontextmanager
async def _open_copy_workspace(case, session, driver):
    async with open_admin_cms_workspace(
        case.database,
        session,
        driver,
        source_binding=case.storage,
        prepared_inputs=case.admin_inputs,
        handoff=case.handoff,
        expected_workspace=case.snapshot_workspace,
        migration_role="synthetic_migration",
    ) as current:
        yield current


@pytest_asyncio.fixture
async def allocated_generic_copy_case(publication_case, monkeypatch):
    """Commit genuine native allocation data; no protected CMS record is manufactured."""
    case = publication_case
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", case.storage.catalog_schema)
    async with case.admin.transaction():
        frozen = await _complete_frozen_header(case)
        await _canonical_controls(case, frozen)
    case.input_snapshot = await input_snapshots(case)
    async with publisher(case) as (session, driver), observe(case, session, driver) as inputs:
        async with prepare_outputs(case, session, driver, inputs) as (current, _report):
            case.snapshot_workspace = current.binding
            case.admin_inputs = {
                key: deepcopy(inputs[key])
                for key in ("source_storage", "source_policy", "prepared_policy", "frozen_inventory")
            }
    await _parents(case)
    await _retained_resource(case)
    case.retained_payloads = await _retained_rows(case)
    await _allocator(case)
    await _guarded_target_setup(case)
    async with _target_admin_session(case) as (session, driver), _open_copy_workspace(case, session, driver) as current:
        case.generic_allocation = await allocate_prepared_cms_candidate(current, case.handoff)
    return case


async def _assert_copy_candidate_pins(case, driver):
    child_tables = [row for row in case.generic_allocation["inventory"] if row["relkind"] == "r"]
    assert await driver.fetchval(
        "SELECT count(DISTINCT relation)=4 FROM pg_locks WHERE pid=pg_backend_pid() "
        "AND granted AND mode='ShareUpdateExclusiveLock' AND relation=ANY($1::oid[])",
        [row["oid"] for row in child_tables],
    )
    assert not await driver.fetchval(
        "SELECT EXISTS(SELECT FROM pg_locks l JOIN pg_class c ON c.oid=l.relation "
        "WHERE l.pid=pg_backend_pid() AND c.relnamespace=to_regnamespace($1) "
        "AND c.relname=ANY($2::text[]) AND l.mode IN "
        "('ShareUpdateExclusiveLock','ShareLock','ShareRowExclusiveLock','ExclusiveLock','AccessExclusiveLock'))",
        case.storage.catalog_schema,
        list(workspace.storage.RELATIONS[:4]),
    )


async def test_original_admin_four_copy_holds_candidate_pins_through_rollback_and_retry(
    allocated_generic_copy_case, monkeypatch, record_property
):
    """Actual workspace/COPY/locks; protected allocation provenance and finalization stay separate."""
    case = allocated_generic_copy_case
    original_copy = payload_copy._copy_table

    async def checked_copy(current, root, receipt):
        await _assert_copy_candidate_pins(case, current.connection)
        return await original_copy(current, root, receipt)

    monkeypatch.setattr(payload_copy, "_copy_table", checked_copy)
    monkeypatch.setattr(payload_copy, "COPY_MAX_ROWS", 2)
    for rollback in (True, False):

        class CallerRollback(Exception):
            pass

        try:
            async with (
                _target_admin_session(case) as (session, driver),
                _open_copy_workspace(case, session, driver) as current,
                _peer(case) as peer,
            ):
                report = await payload_copy.copy_allocated_cms_generic_payloads(
                    current, case.handoff, allocation=case.generic_allocation
                )
                assert set(report["row_counts"]) == set(workspace.storage.RELATIONS[:4])
                assert all(count > 0 for count in report["row_counts"].values())
                await _assert_copy_candidate_pins(case, driver)
                await _parallel_target_reader_and_allocator(case, peer, dict.fromkeys(report["row_counts"], 0))
                if rollback:
                    raise CallerRollback()
        except CallerRollback:
            assert rollback
        for root, count in report["row_counts"].items():
            assert await case.admin.fetchval(
                f"SELECT count(*) FROM {storage_relation(case.storage.catalog_schema, root)} WHERE dataset_id=$1",
                case.handoff.document["identity"]["dataset_id"],
            ) == (0 if rollback else count)
        assert await _retained_rows(case) == case.retained_payloads
    record_property("actual_four_generic_copy_counts", report["row_counts"])


async def test_original_admin_four_copy_allows_disjoint_commit_during_copy(allocated_generic_copy_case, monkeypatch):
    """Commit a disjoint allocation before COPY's final custody recheck."""
    case = allocated_generic_copy_case
    original_copy = payload_copy._copy_table

    async def overlapping_copy(current, root, receipt):
        if root == workspace.storage.RELATIONS[0]:
            async with _peer(case) as peer:
                await _parallel_target_reader_and_allocator(
                    case, peer, dict.fromkeys(workspace.storage.RELATIONS[:4], 0)
                )
        return await original_copy(current, root, receipt)

    monkeypatch.setattr(payload_copy, "_copy_table", overlapping_copy)
    async with _target_admin_session(case) as (session, driver), _open_copy_workspace(case, session, driver) as current:
        report = await payload_copy.copy_allocated_cms_generic_payloads(
            current, case.handoff, allocation=case.generic_allocation
        )
        assert all(count > 0 for count in report["row_counts"].values())
    assert await _retained_rows(case) == case.retained_payloads
