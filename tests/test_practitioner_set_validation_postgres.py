# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Native staged admission and direct-write closure on acquisition partitions."""

import asyncio
import hashlib
import json
import uuid

import asyncpg
import pytest
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import create_async_engine

from process import uhc_flex_practitioner_stage as staging
from process.uhc_flex_practitioner_result_store import _terminalize_query_result
from process.uhc_flex_practitioner_stage import COLUMNS, STAGE
from process.uhc_flex_practitioner_store import (
    _insert_acquisition_header,
    claim_uhc_flex_practitioner_work,
    complete_uhc_flex_practitioner_result,
    initialize_uhc_flex_practitioner_acquisition,
    seal_uhc_flex_practitioner_acquisition,
)
from process.uhc_flex_practitioner_store_contract import (
    canonical_resource_fields_list,
    terminal_record_sha256,
)
from process.uhc_flex_practitioner_store_support import set_store_action, table_ref
from tests.formulary_fhir_twin_admission_pg_support import (
    connect,
    database_url,
    drop_schema,
    load_migration,
    quoted,
    run_migration,
)
from tests.provider_directory_uhc_flex_npi_cohort_pg_support import MEMBER_NPIS
from tests.test_provider_directory_uhc_flex_practitioner_acquisition_postgres import (
    VERSIONS,
    _assert_manifest,
    _complete_baseline,
    _configure_database,
    _initialize_roles,
    _matched,
    _prepare_schema,
    _role_identities,
    _unmatched,
)


async def _assert_empty_legacy_scope(database, schema):
    """Empty legacy heaps retain a false scope without joining the new parents."""
    for suffix in ("work", "resource"):
        legacy = f"{quoted(schema)}.provider_directory_uhc_flex_practitioner_{suffix}_legacy"
        assert (
            await database.scalar(
                "SELECT pg_get_expr(conbin,conrelid) FROM pg_constraint "
                "WHERE conrelid=CAST(:relation AS regclass) AND conname='pd_uhc_flex_legacy_scope'",
                relation=legacy,
            )
            == "false"
        )
        assert not await database.scalar(
            "SELECT EXISTS(SELECT FROM pg_inherits WHERE inhrelid=CAST(:relation AS regclass))",
            relation=legacy,
        )


@pytest.mark.asyncio
async def test_practitioner_native_stage_and_partition_admission(monkeypatch):
    """The real empty-history migration admits and seals its first acquisition."""
    url = database_url()
    schema = f"fhir_twin_test_{uuid.uuid4().hex}"
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
    monkeypatch.setenv("DB_SCHEMA", schema)
    engine = create_async_engine(url.set(drivername="postgresql+asyncpg"))
    database = _configure_database(monkeypatch, url)
    migration = load_migration(
        VERSIONS / "20261005120000_practitioner_set_validation.py",
        "practitioner_sets",
    )
    try:
        await _prepare_schema(engine, url, schema, latest=False)
        await database.connect()
        assert (
            await database.scalar(
                f"SELECT count(*) FROM {quoted(schema)}.provider_directory_uhc_flex_practitioner_acquisition"
            )
            == 0
        )
        await run_migration(engine, migration, "upgrade")
        await _assert_empty_legacy_scope(database, schema)
        baseline, candidate = _role_identities()
        await _initialize_roles(database, baseline, candidate)
        await _complete_baseline(database, baseline)
        await _assert_manifest(database, baseline.acquisition_id)
        connection = await connect(url)
        try:
            table = f"{quoted(schema)}.provider_directory_uhc_flex_practitioner_resource"
            assert (
                await connection.fetchval(
                    "SELECT relkind FROM pg_class WHERE oid=$1::regclass",
                    table,
                )
                == b"p"
            )
            assert await connection.fetchval(
                f"SELECT tableoid <> $1::regclass FROM {table} LIMIT 1",
                table + "_legacy",
            )
            assert (
                await connection.fetchval(
                    "SELECT count(*) FROM pg_trigger WHERE tgrelid=$1::regclass "
                    "AND NOT tgisinternal AND tgtype & 1 = 1",
                    table,
                )
                == 0
            )
        finally:
            await connection.close()
    finally:
        await database.disconnect()
        await drop_schema(engine, schema)
        await engine.dispose()


@pytest.mark.asyncio
async def test_practitioner_work_load_is_detached_and_rejects_missing_member(monkeypatch):
    """Load an unchecked detached heap, then reject incomplete cohort membership."""
    url = database_url()
    schema = f"fhir_twin_test_{uuid.uuid4().hex}"
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
    monkeypatch.setenv("DB_SCHEMA", schema)
    engine = create_async_engine(url.set(drivername="postgresql+asyncpg"))
    database = _configure_database(monkeypatch, url)
    copy_work = staging.copy_work_candidate
    baseline, _ = _role_identities()
    candidate_name = "pd_uhc_flex_pw_" + hashlib.sha256(baseline.acquisition_id.encode()).hexdigest()[:40]
    candidate_relation = f"{quoted(schema)}.{quoted(candidate_name)}"

    async def corrupt_member(database, transaction, identity):
        assert await copy_work(database, transaction, identity)
        relation = candidate_relation
        assert not await database.scalar(
            "SELECT EXISTS(SELECT FROM pg_inherits WHERE inhrelid=CAST(:relation AS regclass))", relation=relation
        )
        assert not await database.scalar(
            "SELECT EXISTS(SELECT FROM pg_index WHERE indrelid=CAST(:relation AS regclass))", relation=relation
        )
        assert not await database.scalar(
            "SELECT EXISTS(SELECT FROM pg_constraint WHERE conrelid=CAST(:relation AS regclass) AND contype='c')",
            relation=relation,
        )
        await database.status(f"UPDATE {relation} SET npi=2999999999 WHERE npi=:npi", npi=MEMBER_NPIS[0])
        return candidate_name

    try:
        await _prepare_schema(engine, url, schema)
        await database.connect()
        with monkeypatch.context() as patch:
            patch.setattr(staging, "copy_work_candidate", corrupt_member)
            with pytest.raises(DBAPIError, match="practitioner_workset_invalid"):
                await initialize_uhc_flex_practitioner_acquisition(baseline, database=database)
        assert await database.scalar("SELECT to_regclass(:relation)", relation=candidate_relation) is None
        await initialize_uhc_flex_practitioner_acquisition(baseline, database=database)
        index_census = await database.first(
            "SELECT count(*) AS total,count(inheritance.inhrelid) AS attached FROM pg_index index "
            "LEFT JOIN pg_inherits inheritance ON inheritance.inhrelid=index.indexrelid "
            "WHERE index.indrelid=CAST(:relation AS regclass)",
            relation=candidate_relation,
        )
        assert tuple(index_census) == (3, 3)
        for suffix in ("work", "resource"):
            relation = f"{quoted(schema)}.provider_directory_uhc_flex_practitioner_{suffix}"
            assert not await database.scalar(
                "SELECT EXISTS(SELECT FROM pg_constraint WHERE contype='f' AND conrelid IN "
                "(SELECT relid FROM pg_partition_tree(CAST(:relation AS regclass))))",
                relation=relation,
            )
    finally:
        await database.disconnect()
        await drop_schema(engine, schema)
        await engine.dispose()


@pytest.mark.asyncio
async def test_practitioner_work_preserves_native_lease_state_checks(monkeypatch):
    """A lease update must retain native state integrity after check-free COPY."""
    url = database_url()
    schema = f"fhir_twin_test_{uuid.uuid4().hex}"
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
    monkeypatch.setenv("DB_SCHEMA", schema)
    engine = create_async_engine(url.set(drivername="postgresql+asyncpg"))
    database = _configure_database(monkeypatch, url)
    try:
        await _prepare_schema(engine, url, schema)
        await database.connect()
        baseline, _ = _role_identities()
        await initialize_uhc_flex_practitioner_acquisition(baseline, database=database)
        claim = await claim_uhc_flex_practitioner_work(baseline.acquisition_id, database=database)
        with pytest.raises(DBAPIError, match="work_state_check"):
            async with database.transaction():
                await set_store_action(database, "heartbeat", claim.acquisition_id, claim.lease_token)
                await database.status(
                    f"UPDATE {table_ref('provider_directory_uhc_flex_practitioner_work')} "
                    "SET lease_expires_at=NULL,updated_at=transaction_timestamp() "
                    "WHERE acquisition_id=:aid AND npi=:npi",
                    aid=claim.acquisition_id,
                    npi=claim.requested_npi,
                )
        assert await database.scalar(
            f"SELECT lease_expires_at IS NOT NULL FROM {table_ref('provider_directory_uhc_flex_practitioner_work')} "
            "WHERE acquisition_id=:aid AND npi=:npi",
            aid=claim.acquisition_id,
            npi=claim.requested_npi,
        )
    finally:
        await database.disconnect()
        await drop_schema(engine, schema)
        await engine.dispose()


@pytest.mark.asyncio
async def test_practitioner_work_load_does_not_lock_other_initializations(monkeypatch):
    url = database_url()
    schema = f"fhir_twin_test_{uuid.uuid4().hex}"
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
    monkeypatch.setenv("DB_SCHEMA", schema)
    engine = create_async_engine(url.set(drivername="postgresql+asyncpg"))
    database = _configure_database(monkeypatch, url)
    baseline, candidate = _role_identities()
    loaded, release = asyncio.Event(), asyncio.Event()
    copy_work = staging.copy_work_candidate
    task = None

    async def hold_loaded_work(database, transaction, identity):
        candidate_table = await copy_work(database, transaction, identity)
        if identity.acquisition_id == baseline.acquisition_id:
            loaded.set()
            await release.wait()
        return candidate_table

    try:
        await _prepare_schema(engine, url, schema)
        await database.connect()
        monkeypatch.setattr(staging, "copy_work_candidate", hold_loaded_work)
        task = asyncio.create_task(initialize_uhc_flex_practitioner_acquisition(baseline, database=database))
        await asyncio.wait_for(loaded.wait(), 5)
        assert (
            await asyncio.wait_for(initialize_uhc_flex_practitioner_acquisition(candidate, database=database), 3) == 1
        )
        release.set()
        assert await task == 1
    finally:
        release.set()
        if task is not None:
            await asyncio.gather(task, return_exceptions=True)
        await database.disconnect()
        await drop_schema(engine, schema)
        await engine.dispose()


async def _legacy_acquisition(database, identity, *, sealed, include_resources=True):
    async with database.transaction():
        await _insert_acquisition_header(database, identity)
        await set_store_action(database, "initialize", identity.acquisition_id)
        await database.status(
            f"INSERT INTO {table_ref('provider_directory_uhc_flex_practitioner_work')} "
            "(acquisition_id,cohort_id,npi,status,attempt_count) SELECT :aid,cohort_id,npi,'pending',0 "
            f"FROM {table_ref('provider_directory_uhc_flex_npi_member')} WHERE cohort_id=:cid",
            aid=identity.acquisition_id,
            cid=identity.cohort_id,
        )
    if not include_resources:
        return
    claim = await claim_uhc_flex_practitioner_work(
        identity.acquisition_id,
        requested_npi=MEMBER_NPIS[0],
        database=database,
    )
    query_result = _matched(claim.requested_npi)
    fields = canonical_resource_fields_list(query_result)[0]
    async with database.transaction():
        await set_store_action(database, "resource", claim.acquisition_id, claim.lease_token)
        await database.status(
            f"INSERT INTO {table_ref('provider_directory_uhc_flex_practitioner_resource')} "
            "(acquisition_id,cohort_id,npi,attempt,resource_id,payload_sha256,payload_json_text) "
            "VALUES (:aid,:cid,:npi,:attempt,:resource_id,:payload_sha256,:payload_json_text)",
            aid=claim.acquisition_id,
            cid=claim.cohort_id,
            npi=claim.requested_npi,
            attempt=claim.attempt,
            **fields,
        )
        await _terminalize_query_result(
            database,
            claim,
            query_result,
            terminal_record_sha256(
                claim,
                status=query_result.outcome,
                result_sha256=query_result.result_sha256,
                resource_count=query_result.resource_count,
                error_code=None,
            ),
        )
    if sealed:
        await _finish_remaining(database, identity)


async def _finish_remaining(database, identity):
    claim = await claim_uhc_flex_practitioner_work(
        identity.acquisition_id,
        requested_npi=MEMBER_NPIS[1],
        database=database,
    )
    await complete_uhc_flex_practitioner_result(claim, _unmatched(claim.requested_npi), database=database)
    return await seal_uhc_flex_practitioner_acquisition(identity, database=database)


@pytest.mark.asyncio
@pytest.mark.parametrize("include_resources", (False, True))
async def test_practitioner_migration_preserves_history_and_requires_rerun(monkeypatch, include_resources):
    url = database_url()
    schema = f"fhir_twin_test_{uuid.uuid4().hex}"
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
    monkeypatch.setenv("DB_SCHEMA", schema)
    engine = create_async_engine(url.set(drivername="postgresql+asyncpg"))
    database = _configure_database(monkeypatch, url)
    connection = await connect(url)
    resource = f"{quoted(schema)}.provider_directory_uhc_flex_practitioner_resource"
    try:
        await _prepare_schema(engine, url, schema, latest=False)
        await database.connect()
        active, sealed = _role_identities()
        await _legacy_acquisition(database, active, sealed=False, include_resources=include_resources)
        await _legacy_acquisition(database, sealed, sealed=True)
        old_oid = await connection.fetchval("SELECT $1::regclass::oid", resource)
        old_index = await connection.fetchval(
            "SELECT indexrelid FROM pg_index WHERE indrelid=$1 AND indisprimary",
            old_oid,
        )
        old_rows = await connection.fetch(f"SELECT * FROM {resource} ORDER BY acquisition_id,npi")
        migration = load_migration(
            VERSIONS / "20261005120000_practitioner_set_validation.py",
            "practitioner_active_sets",
        )
        await run_migration(engine, migration, "upgrade")
        assert await connection.fetch(f"SELECT * FROM {resource} ORDER BY acquisition_id,npi") == old_rows
        assert await connection.fetchval("SELECT $1::regclass::oid", resource + "_legacy") == old_oid
        assert await connection.fetchval("SELECT indrelid FROM pg_index WHERE indexrelid=$1", old_index) == old_oid
        assert await connection.fetchval(
            f"SELECT tableoid=$1 FROM {resource} WHERE acquisition_id=$2",
            old_oid,
            sealed.acquisition_id,
        )
        if include_resources:
            assert await connection.fetchval(
                f"SELECT tableoid=$1 FROM {resource} WHERE acquisition_id=$2",
                old_oid,
                active.acquisition_id,
            )
        with pytest.raises(DBAPIError, match="practitioner_acquisition_rerun_required"):
            await initialize_uhc_flex_practitioner_acquisition(active, database=database)
        await initialize_uhc_flex_practitioner_acquisition(sealed, database=database)
    finally:
        await database.disconnect()
        await connection.close()
        await drop_schema(engine, schema)
        await engine.dispose()


async def _reject_invalid_sets(connection, schema, claim):
    prepare = f"{quoted(schema)}.prepare_pd_uhc_flex_practitioner_stage()"
    admit = f"SELECT {quoted(schema)}.admit_pd_uhc_flex_practitioner_stage($1,$2,$3,$4,$5)"
    valid_fields = canonical_resource_fields_list(_matched(claim.requested_npi))[0]
    invalid_by_field = dict(valid_fields)
    payload_by_field = json.loads(invalid_by_field["payload_json_text"])
    payload_by_field["identifier"][0]["value"] = str(MEMBER_NPIS[1])
    invalid_by_field["payload_json_text"] = json.dumps(payload_by_field, sort_keys=True, separators=(",", ":"))
    invalid_by_field["payload_sha256"] = hashlib.sha256(invalid_by_field["payload_json_text"].encode()).hexdigest()
    for resource_rows in ([invalid_by_field], [valid_fields] * 17, [valid_fields, valid_fields]):
        with pytest.raises(asyncpg.CheckViolationError, match="resource_invalid"):
            async with connection.transaction():
                await connection.execute(f"SELECT {prepare}")
                await connection.copy_records_to_table(
                    STAGE,
                    schema_name="pg_temp",
                    columns=COLUMNS,
                    records=[tuple(fields[name] for name in COLUMNS) for fields in resource_rows],
                )
                await connection.fetchval(
                    admit, claim.acquisition_id, claim.cohort_id, claim.requested_npi, claim.attempt, claim.lease_token
                )


async def _grant_runtime_writer(connection, schema, role, resource, role_password):
    await connection.execute(f"CREATE ROLE {quoted(role)} LOGIN PASSWORD '{role_password}'")
    await connection.execute(f"GRANT USAGE ON SCHEMA {quoted(schema)} TO {quoted(role)}")
    await connection.execute(f"GRANT SELECT,INSERT,UPDATE ON ALL TABLES IN SCHEMA {quoted(schema)} TO {quoted(role)}")
    await connection.execute(f"GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA {quoted(schema)} TO {quoted(role)}")
    await connection.execute(
        f"ALTER DEFAULT PRIVILEGES IN SCHEMA {quoted(schema)} GRANT INSERT,UPDATE,DELETE,TRUNCATE ON TABLES TO {quoted(role)}"
    )
    await connection.execute(f"GRANT INSERT(payload_json_text) ON {resource} TO PUBLIC")


async def _assert_terminal_rollback(monkeypatch, database, claim):
    async def fail_terminal(*_args):
        raise RuntimeError("synthetic terminal failure")

    with monkeypatch.context() as patch:
        patch.setattr(
            "process.uhc_flex_practitioner_result_store._terminalize_query_result",
            fail_terminal,
        )
        with pytest.raises(RuntimeError, match="synthetic terminal failure"):
            await complete_uhc_flex_practitioner_result(
                claim,
                _matched(claim.requested_npi),
                database=database,
            )


async def _assert_global_write_denied(connection, schema, role, resource, leaf, work):
    """Reject inherited broad writes to frozen data and private migration metadata."""
    await connection.execute("RESET ROLE")
    await connection.execute(f"GRANT pg_write_all_data TO {quoted(role)}")
    await connection.execute(f"SET ROLE {quoted(role)}")
    work_leaf = await connection.fetchval(
        "SELECT inhrelid::regclass::text FROM pg_inherits WHERE inhparent=$1::regclass "
        "AND inhrelid::regclass::text NOT LIKE '%_legacy'",
        work,
    )
    for relation in (resource, leaf, work, work_leaf):
        with pytest.raises(asyncpg.InsufficientPrivilegeError, match="practitioner_direct_write_forbidden"):
            await connection.execute(f"INSERT INTO {relation} DEFAULT VALUES")
    with pytest.raises(asyncpg.InsufficientPrivilegeError, match="practitioner_migration_metadata_immutable"):
        await connection.execute(f"DELETE FROM {quoted(schema)}.pd_practitioner_storage_migration")


@pytest.mark.asyncio
async def test_practitioner_restricted_writer_and_invalid_set(monkeypatch):
    url = database_url()
    schema = f"fhir_twin_test_{uuid.uuid4().hex}"
    role = "practitioner_test_" + uuid.uuid4().hex
    role_password = uuid.uuid4().hex
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
    monkeypatch.setenv("DB_SCHEMA", schema)
    engine = create_async_engine(url.set(drivername="postgresql+asyncpg"))
    database = _configure_database(monkeypatch, url.set(username=role, password=role_password))
    resource = f"{quoted(schema)}.provider_directory_uhc_flex_practitioner_resource"
    connection = await connect(url)
    try:
        await _prepare_schema(engine, url, schema, latest=False)
        await _grant_runtime_writer(connection, schema, role, resource, role_password)
        await run_migration(
            engine,
            load_migration(
                VERSIONS / "20261005120000_practitioner_set_validation.py",
                "practitioner_restricted_sets",
            ),
            "upgrade",
        )
        await database.connect()
        baseline, _ = _role_identities()
        await initialize_uhc_flex_practitioner_acquisition(baseline, database=database)
        claim = await claim_uhc_flex_practitioner_work(
            baseline.acquisition_id,
            requested_npi=MEMBER_NPIS[0],
            database=database,
        )
        await connection.execute(f"SET ROLE {quoted(role)}")
        leaf = await connection.fetchval(
            "SELECT inhrelid::regclass::text FROM pg_inherits WHERE inhparent=$1::regclass "
            "AND inhrelid::regclass::text NOT LIKE '%_legacy'",
            resource,
        )
        work = f"{quoted(schema)}.provider_directory_uhc_flex_practitioner_work"
        for relation in (resource, leaf, work):
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                await connection.execute(f"INSERT INTO {relation}(acquisition_id) VALUES ('invalid')")
        await _assert_global_write_denied(connection, schema, role, resource, leaf, work)
        await _reject_invalid_sets(connection, schema, claim)
        assert await connection.fetchval(f"SELECT count(*) FROM {resource}") == 0

        await _assert_terminal_rollback(monkeypatch, database, claim)
        assert await connection.fetchval(f"SELECT count(*) FROM {resource}") == 0
        await complete_uhc_flex_practitioner_result(claim, _matched(claim.requested_npi), database=database)
        assert await connection.fetchval(f"SELECT count(*) FROM {resource}") == 1
    finally:
        await database.disconnect()
        await connection.execute("RESET ROLE")
        await drop_schema(engine, schema)
        await connection.execute(f"DROP ROLE IF EXISTS {quoted(role)}")
        await connection.close()
        await engine.dispose()
