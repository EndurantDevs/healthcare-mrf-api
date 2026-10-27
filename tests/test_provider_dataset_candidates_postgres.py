# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Native candidate permissions, historical identities and generic lifecycle."""

import json
import uuid
from contextlib import asynccontextmanager

import asyncpg
import pytest
from sqlalchemy.exc import DBAPIError

from db.migration_generic_source_candidates import (
    upgrade_generic_source_candidates,
    upgrade_generic_source_canonical_copy_guard,
    verify_generic_source_canonical_copy_guard,
)
from db.migration_provider_directory_dataset_candidates import TABLES
from db.migration_provider_directory_terminal_root_retirement_guards import (
    CHILD_GUARD,
    CHILD_TRIGGER_SUFFIXES,
    child_guard_function_sql,
)
from process import uhc_flex_practitioner_publication_materialization as practitioner_materialization
from process import uhc_flex_practitioner_publication_store as practitioner_store
from process.uhc_flex_practitioner_publication import build_uhc_flex_practitioner_dataset_identity
from process.uhc_flex_practitioner_publication_store import _insert_building_headers
from tests.formulary_fhir_twin_admission_pg_support import (
    assert_sqlstate,
    connect,
    database_url,
    load_migration,
    quoted,
    run_migration,
)
from tests.provider_directory_historical_publication_pg_support import (
    _copy_historical_candidate as _copy_historical_dataset,
)
from tests.provider_directory_historical_publication_pg_support import (
    _historical_candidate as _historical_dataset_storage,
)
from tests.test_provider_directory_rooted_graph_bulk_postgres import _claim
from tests.test_provider_directory_rooted_graph_publication_postgres import _lifecycle_scope, _publish_legacy_root
from tests.test_provider_directory_uhc_flex_practitioner_publication_postgres import ENDPOINT_ID, VERSIONS, _sealed_pair


def _candidate_migration():
    return load_migration(VERSIONS / "20261005130000_provider_dataset_candidates.py", "dataset_candidate_security")


@asynccontextmanager
async def _candidate_test_scope(monkeypatch):
    roles_by_kind = {
        kind: f"candidate_{uuid.uuid4().hex}_{kind}"
        for kind in (
            "owner",
            "catalog",
            "generic",
            "practitioner",
            "rooted",
            "column",
            "leak",
            "member",
        )
    }
    administrator = await connect(database_url())
    try:
        for role in roles_by_kind.values():
            await administrator.execute(f"CREATE ROLE {quoted(role)}")
        async with _lifecycle_scope(monkeypatch, dataset_candidates=False) as context:
            await _seed_permissions(context, roles_by_kind)
            yield context, roles_by_kind
    finally:
        for role in roles_by_kind.values():
            await administrator.execute(f"DROP ROLE IF EXISTS {quoted(role)}")
        await administrator.close()


async def _seed_permissions(context, roles_by_kind):
    connection, schema = context.connection, context.schema
    for role in roles_by_kind.values():
        await connection.execute(f"GRANT USAGE ON SCHEMA {schema} TO {quoted(role)}")
    for table in TABLES:
        await connection.execute(f"ALTER TABLE {schema}.{table} OWNER TO {quoted(roles_by_kind['owner'])}")
    for kind in ("generic", "practitioner", "rooted", "column"):
        await connection.execute(f"GRANT INSERT ON {schema}.{TABLES[0]} TO {quoted(roles_by_kind[kind])}")
    for kind, table in (("practitioner", TABLES[4]), ("rooted", TABLES[5])):
        await connection.execute(f"GRANT INSERT ON {schema}.{table} TO {quoted(roles_by_kind[kind])}")
        await connection.execute(f"GRANT INSERT(dataset_id) ON {schema}.{table} TO {quoted(roles_by_kind['column'])}")
    await connection.execute(f"GRANT SELECT(payload_json) ON {schema}.{TABLES[0]} TO {quoted(roles_by_kind['column'])}")
    await connection.execute(f"GRANT {quoted(roles_by_kind['practitioner'])} TO {quoted(roles_by_kind['member'])}")
    await connection.execute(
        f"ALTER DEFAULT PRIVILEGES IN SCHEMA {schema} GRANT ALL ON TABLES TO {quoted(roles_by_kind['leak'])}"
    )
    await connection.execute(
        f"ALTER DEFAULT PRIVILEGES IN SCHEMA {schema} GRANT EXECUTE ON FUNCTIONS TO {quoted(roles_by_kind['leak'])}"
    )


async def _assert_wrapper_permissions(context, roles_by_kind):
    connection, schema = context.connection, context.schema
    for kind in ("practitioner", "rooted"):
        allowed = {"owner", kind} | ({"member"} if kind == "practitioner" else set())
        for action in ("prepare", "finish"):
            signature = f"{schema}.{action}_pd_{kind}_dataset_candidate(text)"
            for role_kind, role in roles_by_kind.items():
                assert await connection.fetchval("SELECT has_function_privilege($1,$2,'EXECUTE')", role, signature) is (
                    role_kind in allowed
                )
    for role in roles_by_kind.values():
        for action in ("prepare", "finish"):
            signature = f"{schema}.{action}_pd_dataset_candidate(text,text)"
            assert not await connection.fetchval("SELECT has_function_privilege($1,$2,'EXECUTE')", role, signature)
    await connection.execute(f"SET ROLE {quoted(roles_by_kind['generic'])}")
    try:
        with pytest.raises(asyncpg.InsufficientPrivilegeError):
            await connection.fetchval(f"SELECT {schema}.prepare_pd_practitioner_dataset_candidate('synthetic')")
    finally:
        await connection.execute("RESET ROLE")


@pytest.mark.asyncio
async def test_candidate_owner_and_scoped_permissions(monkeypatch):
    async with _candidate_test_scope(monkeypatch) as (context, roles_by_kind):
        await run_migration(context.engine, _candidate_migration(), "upgrade")
        connection, schema = context.connection, context.schema
        for table in TABLES:
            relation = f"{schema}.{table}"
            assert (
                await connection.fetchval(
                    "SELECT pg_get_userbyid(relowner) FROM pg_class WHERE oid=$1::regclass", relation
                )
                == roles_by_kind["owner"]
            )
            for privilege in ("SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE"):
                assert not await connection.fetchval(
                    "SELECT has_table_privilege($1,$2,$3)", roles_by_kind["leak"], relation, privilege
                )
        resource = f"{schema}.{TABLES[0]}"
        assert not await connection.fetchval(
            "SELECT has_table_privilege($1,$2,'SELECT')", roles_by_kind["column"], resource
        )
        assert await connection.fetchval(
            "SELECT has_column_privilege($1,$2,'payload_json','SELECT')", roles_by_kind["column"], resource
        )
        assert not await connection.fetchval(
            "SELECT has_column_privilege($1,$2,'resource_id','SELECT')", roles_by_kind["column"], resource
        )
        await _assert_wrapper_permissions(context, roles_by_kind)
        await _assert_private_candidate_acl(context, roles_by_kind)


@pytest.mark.asyncio
async def test_candidate_writer_membership_remains_revocable(monkeypatch):
    async with _candidate_test_scope(monkeypatch) as (context, roles_by_kind):
        connection, schema = context.connection, context.schema
        member = roles_by_kind["member"]
        practitioner = roles_by_kind["practitioner"]
        await connection.execute(f"GRANT pg_write_all_data TO {quoted(roles_by_kind['column'])}")
        await run_migration(context.engine, _candidate_migration(), "upgrade")
        for action in ("prepare", "finish"):
            signature = f"{schema}.{action}_pd_practitioner_dataset_candidate(text)"
            assert await connection.fetchval("SELECT has_function_privilege($1,$2,'EXECUTE')", member, signature)
            assert not await connection.fetchval(
                "SELECT has_function_privilege($1,$2,'EXECUTE')", "pg_write_all_data", signature
            )
        await connection.execute(f"REVOKE {quoted(practitioner)} FROM {quoted(member)}")
        for action in ("prepare", "finish"):
            signature = f"{schema}.{action}_pd_practitioner_dataset_candidate(text)"
            assert not await connection.fetchval("SELECT has_function_privilege($1,$2,'EXECUTE')", member, signature)


async def _assert_candidate_authority(connection, schema, *, is_allowed):
    """A missing dataset distinguishes authorization from the later state guard."""
    for action in ("prepare", "finish"):
        with pytest.raises(asyncpg.PostgresError) as failure:
            await connection.fetchval(
                f"SELECT {schema}.{action}_pd_practitioner_dataset_candidate('synthetic-missing')"
            )
        assert failure.value.sqlstate == ("55000" if is_allowed else "42501")


@pytest.mark.asyncio
async def test_candidate_split_group_authority_checks_actual_caller(monkeypatch):
    async with _candidate_test_scope(monkeypatch) as (context, roles_by_kind):
        connection, schema = context.connection, context.schema
        member = roles_by_kind["member"]
        provenance = roles_by_kind["practitioner"]
        resource = roles_by_kind["generic"]
        await connection.execute(f"REVOKE INSERT ON {schema}.{TABLES[0]} FROM {quoted(provenance)}")
        await connection.execute(f"GRANT {quoted(resource)} TO {quoted(member)}")
        await run_migration(context.engine, _candidate_migration(), "upgrade")
        password = uuid.uuid4().hex
        await connection.execute(f"ALTER ROLE {quoted(member)} LOGIN PASSWORD '{password}'")
        direct = await connect(database_url().set(username=member, password=password))
        try:
            await _assert_candidate_authority(direct, schema, is_allowed=True)
            await connection.execute(f"REVOKE {quoted(resource)} FROM {quoted(member)}")
            await _assert_candidate_authority(direct, schema, is_allowed=False)
            await connection.execute(f"SET ROLE {quoted(member)}")
            try:
                await _assert_candidate_authority(connection, schema, is_allowed=False)
            finally:
                await connection.execute("RESET ROLE")
            await connection.execute(f"GRANT {quoted(resource)} TO {quoted(member)}")
            await _assert_candidate_authority(direct, schema, is_allowed=True)
            await connection.execute(f"REVOKE {quoted(provenance)} FROM {quoted(member)}")
            await _assert_candidate_authority(direct, schema, is_allowed=False)
        finally:
            await direct.close()


async def _assert_private_candidate_acl(context, roles_by_kind):
    admission = await _sealed_pair(context.database, operation_key="e" * 64, matched=True)
    identity = build_uhc_flex_practitioner_dataset_identity(admission, endpoint_id=ENDPOINT_ID)
    async with context.database.transaction():
        await _insert_building_headers(context.database, identity, admission, None, 0)
    connection, schema = context.connection, context.schema
    async with connection.transaction():
        await connection.execute(f"SET LOCAL ROLE {quoted(roles_by_kind['practitioner'])}")
        relation_names = json.loads(
            await connection.fetchval(
                f"SELECT {schema}.prepare_pd_practitioner_dataset_candidate($1)",
                identity.dataset_id,
            )
        )
        for table_name in relation_names.values():
            relation = f"{schema}.{quoted(table_name)}"
            assert not await connection.fetchval(
                "SELECT EXISTS(SELECT FROM pg_index WHERE indrelid=$1::regclass) OR "
                "EXISTS(SELECT FROM pg_constraint WHERE conrelid=$1::regclass AND contype IN ('f','c'))",
                relation,
            )
            assert await connection.fetchval(
                "SELECT has_table_privilege($1,$2,'INSERT')", roles_by_kind["practitioner"], relation
            )
            for privilege in ("SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE"):
                assert not await connection.fetchval(
                    "SELECT has_table_privilege($1,$2,$3)", roles_by_kind["leak"], relation, privilege
                )


@pytest.mark.asyncio
async def test_candidate_history_indexes_and_foreign_keys(monkeypatch):
    async with _candidate_test_scope(monkeypatch) as (context, _roles_by_kind):
        connection, schema = context.connection, context.schema
        resource = f"{schema}.{TABLES[0]}"
        await connection.execute(f"CREATE INDEX candidate_payload_hash_idx ON {resource}(payload_hash)")
        await connection.execute(
            f"CREATE TABLE {schema}.synthetic_resource_reference(dataset_id text,resource_type text,resource_id text, CONSTRAINT candidate_resource_reference FOREIGN KEY(dataset_id,resource_type,resource_id) REFERENCES {resource} ON DELETE CASCADE)"
        )
        await connection.execute(
            f"INSERT INTO {schema}.synthetic_resource_reference SELECT dataset_id,resource_type,resource_id FROM {resource} LIMIT 1"
        )
        old_oid = await connection.fetchval("SELECT $1::regclass::oid", resource)
        old_indexes = await connection.fetch(
            "SELECT indexrelid,indrelid FROM pg_index WHERE indrelid=$1 ORDER BY indexrelid", old_oid
        )
        old_rows = await connection.fetch(
            f"SELECT tableoid,* FROM {resource} ORDER BY dataset_id,resource_type,resource_id"
        )
        await run_migration(context.engine, _candidate_migration(), "upgrade")
        assert await connection.fetchval("SELECT $1::regclass::oid", resource) != old_oid
        assert (
            await connection.fetch(
                "SELECT indexrelid,indrelid FROM pg_index WHERE indrelid=$1 ORDER BY indexrelid", old_oid
            )
            == old_indexes
        )
        assert (
            await connection.fetch(f"SELECT tableoid,* FROM {resource} ORDER BY dataset_id,resource_type,resource_id")
            == old_rows
        )
        assert await connection.fetchval(
            "SELECT confrelid=$1::regclass FROM pg_constraint WHERE conrelid=$2::regclass AND conname='candidate_resource_reference'",
            resource,
            f"{schema}.synthetic_resource_reference",
        )
        assert await connection.fetchval(f"SELECT count(*) FROM {schema}.synthetic_resource_reference") == 1


@pytest.mark.asyncio
async def test_generic_dataset_delete_references_and_set_cascade(monkeypatch):
    async with _candidate_test_scope(monkeypatch) as (context, roles_by_kind):
        await run_migration(context.engine, _candidate_migration(), "upgrade")
        connection, schema = context.connection, context.schema
        dataset_id = "synthetic-generic-candidate"
        await connection.execute(
            f"INSERT INTO {schema}.provider_directory_endpoint_dataset(dataset_id,endpoint_id,status,is_current,resource_count,publication_metadata_json) VALUES($1,'endpoint-official','building',false,0,'{{}}')",
            dataset_id,
        )
        await connection.execute(f"SET ROLE {quoted(roles_by_kind['generic'])}")
        try:
            await connection.execute(
                f"INSERT INTO {schema}.{TABLES[0]}(dataset_id,resource_type,resource_id,payload_hash,payload_json) VALUES($1,'Organization','synthetic',repeat('a',64),'{{}}')",
                dataset_id,
            )
        finally:
            await connection.execute("RESET ROLE")
        await connection.execute(
            f"INSERT INTO {schema}.{TABLES[1]}(dataset_id,resource_id,payload_hash,payload_json) VALUES($1,'synthetic',repeat('a',64),'{{}}')",
            dataset_id,
        )
        await connection.execute(
            f"INSERT INTO {schema}.{TABLES[2]} VALUES($1,'synthetic-network','synthetic')", dataset_id
        )
        await connection.execute(
            f"INSERT INTO {schema}.{TABLES[3]} VALUES($1,'synthetic-organization','synthetic-affiliation')", dataset_id
        )
        with pytest.raises(asyncpg.ForeignKeyViolationError, match="provider_directory_dataset_reference_exists"):
            await connection.execute(
                f"DELETE FROM {schema}.provider_directory_endpoint_dataset WHERE dataset_id=$1", dataset_id
            )
        with pytest.raises(asyncpg.ForeignKeyViolationError, match="provider_directory_dataset_reference_exists"):
            await connection.execute(
                f"UPDATE {schema}.provider_directory_endpoint_dataset SET dataset_id='synthetic-new-key' WHERE dataset_id=$1",
                dataset_id,
            )
        await connection.execute(f"DELETE FROM {schema}.{TABLES[0]} WHERE dataset_id=$1", dataset_id)
        await connection.execute(
            f"DELETE FROM {schema}.provider_directory_endpoint_dataset WHERE dataset_id=$1", dataset_id
        )
        for table in TABLES[:4]:
            assert (
                await connection.fetchval(f"SELECT count(*) FROM {schema}.{table} WHERE dataset_id=$1", dataset_id) == 0
            )


async def _seed_ordinary_resource_leaf(context, storage, status, proof_version):
    """Seed the same two-row dataset before or after ordinary leaf migration."""
    connection, schema = context.connection, context.schema
    dataset_id = "synthetic-ordinary-delete"
    resource = f"{schema}.{TABLES[0]}"
    if storage == "generic":
        await run_migration(context.engine, _candidate_migration(), "upgrade")
    await connection.execute(
        f"INSERT INTO {schema}.provider_directory_endpoint_dataset(dataset_id,endpoint_id,status,is_current,resource_count,publication_metadata_json) VALUES($1,'endpoint-official','building',false,2,'{{}}')",
        dataset_id,
    )
    await connection.execute(
        f"INSERT INTO {resource}(dataset_id,resource_type,resource_id,payload_hash,payload_json) "
        "SELECT $1,'Organization','synthetic-'||item,repeat('a',64),'{}' FROM generate_series(1,2) item",
        dataset_id,
    )
    await connection.execute(
        f"UPDATE {schema}.provider_directory_endpoint_dataset SET status=$2,completion_proof_required_version=$3 WHERE dataset_id=$1",
        dataset_id,
        status,
        proof_version,
    )
    if storage == "history":
        await connection.execute(
            f"INSERT INTO {schema}.provider_directory_endpoint_dataset(dataset_id,endpoint_id,status,is_current,resource_count,publication_metadata_json) VALUES('synthetic-mutable-target','endpoint-official','building',false,0,'{{}}')"
        )
        await run_migration(context.engine, _candidate_migration(), "upgrade")
    return dataset_id


@pytest.mark.asyncio
@pytest.mark.parametrize("storage", ["generic", "history"])
@pytest.mark.parametrize(
    ("status", "proof_version", "immutable"),
    [
        ("validated", None, True),
        ("published", None, True),
        ("superseded", 3, True),
        ("verification_baseline", 3, True),
        ("acquiring", None, False),
        ("superseded", None, False),
        ("verification_baseline", None, False),
    ],
)
async def test_ordinary_leaf_delete_preserves_lifecycle_without_provenance(
    monkeypatch, storage, status, proof_version, immutable
):
    """Broad writers cannot erase frozen sets, while ordinary cleanup stays legal."""
    async with _candidate_test_scope(monkeypatch) as (context, roles_by_kind):
        connection, schema = context.connection, context.schema
        dataset_id = await _seed_ordinary_resource_leaf(context, storage, status, proof_version)
        resource = f"{schema}.{TABLES[0]}"
        leaf = await connection.fetchval(
            f"SELECT tableoid::regclass::text FROM {resource} WHERE dataset_id=$1 LIMIT 1", dataset_id
        )
        assert ("pd_generic_" if storage == "generic" else "pd_dataset_history_") in leaf
        original = await connection.fetch(
            f"SELECT tableoid,* FROM {resource} WHERE dataset_id=$1 ORDER BY resource_id", dataset_id
        )
        for table in TABLES[4:]:
            assert (
                await connection.fetchval(f"SELECT count(*) FROM {schema}.{table} WHERE dataset_id=$1", dataset_id) == 0
            )
        role = quoted(roles_by_kind["column"])
        await connection.execute(f"GRANT pg_write_all_data,pg_read_all_data TO {role}")
        async with connection.transaction():
            await connection.execute(f"SET LOCAL ROLE {role}")
            if storage == "history" and (immutable or status == "superseded"):
                await assert_sqlstate(
                    connection,
                    "55000",
                    f"UPDATE {leaf} SET dataset_id='synthetic-mutable-target' WHERE dataset_id='{dataset_id}'",
                )
            if immutable:
                await assert_sqlstate(connection, "55000", f"DELETE FROM {leaf} WHERE dataset_id='{dataset_id}'")
            else:
                assert await connection.execute(f"DELETE FROM {leaf} WHERE dataset_id=$1", dataset_id) == "DELETE 2"
        assert await connection.fetch(
            f"SELECT tableoid,* FROM {resource} WHERE dataset_id=$1 ORDER BY resource_id", dataset_id
        ) == (original if immutable else [])
        assert not await connection.fetchval(
            "SELECT EXISTS(SELECT FROM pg_trigger WHERE tgrelid=$1::regclass AND NOT tgisinternal AND tgtype & 1=1)",
            leaf,
        )


async def _install_retirement_child_guards(context):
    connection, schema = context.connection, context.schema
    await connection.execute(child_guard_function_sql(context.schema_name))
    for table in TABLES[:4]:
        suffix = CHILD_TRIGGER_SUFFIXES[table]
        await connection.execute(
            f"CREATE TRIGGER pd_trr_{suffix}_row BEFORE INSERT OR UPDATE OR DELETE ON {schema}.{table} FOR EACH ROW EXECUTE FUNCTION {schema}.{CHILD_GUARD}()"
        )
        await connection.execute(f"ALTER TABLE {schema}.{table} ENABLE ALWAYS TRIGGER pd_trr_{suffix}_row")
        await connection.execute(
            f"CREATE TRIGGER pd_trr_{suffix}_truncate BEFORE TRUNCATE ON {schema}.{table} FOR EACH STATEMENT EXECUTE FUNCTION {schema}.{CHILD_GUARD}()"
        )


async def _seed_retired_generic_dataset(context, dataset_id):
    connection, schema = context.connection, context.schema
    await connection.execute(
        f"INSERT INTO {schema}.provider_directory_endpoint_dataset(dataset_id,endpoint_id,status,is_current,resource_count,publication_metadata_json) VALUES($1,'endpoint-official','building',false,1,'{{}}')",
        dataset_id,
    )
    await connection.execute(
        f"INSERT INTO {schema}.{TABLES[0]}(dataset_id,resource_type,resource_id,payload_hash,payload_json) VALUES($1,'Organization','synthetic',repeat('a',64),'{{}}')",
        dataset_id,
    )
    await connection.execute(
        f"UPDATE {schema}.provider_directory_endpoint_dataset SET status='acquisition_retired' WHERE dataset_id=$1",
        dataset_id,
    )


async def _assert_retired_resource_immutable(context, dataset_id):
    connection, schema = context.connection, context.schema
    resource = f"{schema}.{TABLES[0]}"
    leaf = await connection.fetchval(f"SELECT tableoid::regclass::text FROM {resource} WHERE dataset_id=$1", dataset_id)
    statements = (
        f"UPDATE {resource} SET payload_hash=repeat('b',64) WHERE dataset_id=$1",
        f"DELETE FROM {resource} WHERE dataset_id=$1",
        f"INSERT INTO {resource}(dataset_id,resource_type,resource_id,payload_hash,payload_json) VALUES($1,'Organization','late',repeat('a',64),'{{}}')",
        f"DELETE FROM {leaf} WHERE dataset_id=$1",
    )
    for statement in statements:
        with pytest.raises(asyncpg.PostgresError) as failure:
            async with connection.transaction():
                await connection.execute(statement, dataset_id)
        assert failure.value.sqlstate == "55000"


@pytest.mark.asyncio
async def test_retirement_guard_replaced_with_statement_checks(monkeypatch):
    async with _candidate_test_scope(monkeypatch) as (context, _roles_by_kind):
        await _install_retirement_child_guards(context)
        await _seed_retired_generic_dataset(context, "synthetic-retired-history")
        await run_migration(context.engine, _candidate_migration(), "upgrade")
        await _seed_retired_generic_dataset(context, "synthetic-retired-candidate")
        for dataset_id in ("synthetic-retired-history", "synthetic-retired-candidate"):
            await _assert_retired_resource_immutable(context, dataset_id)
        assert (
            await context.connection.fetchval(
                "SELECT count(*) FROM pg_trigger WHERE tgfoid=$1::regprocedure AND tgtype & 1 = 1",
                f"{context.schema}.{CHILD_GUARD}()",
            )
            == 0
        )


async def _prepare_copy_guard_catalog(context, roles):
    """Model only the existing protected preparation columns in this disposable schema."""
    connection, schema = context.connection, context.schema
    await connection.execute(
        f"CREATE TABLE {schema}.preparation(importer_id varchar(128),state varchar(16),handoff jsonb,publication jsonb)"
    )
    await connection.execute(f"ALTER TABLE {schema}.preparation OWNER TO {quoted(roles['catalog'])}")
    await connection.execute(f"REVOKE ALL ON {schema}.preparation FROM PUBLIC,{quoted(roles['leak'])}")
    await connection.execute(
        f"GRANT SELECT(importer_id,state,handoff,publication) ON {schema}.preparation TO {quoted(roles['owner'])}"
    )
    await connection.execute(
        f"GRANT SELECT,UPDATE ON {schema}.provider_directory_endpoint_dataset TO {quoted(roles['owner'])}"
    )
    await connection.execute(f"GRANT SELECT ON {schema}.pd_dataset_migration_plan TO {quoted(roles['owner'])}")
    await connection.execute(
        f"ALTER FUNCTION {schema}.guard_pd_dataset_candidate_parent() OWNER TO {quoted(roles['owner'])}"
    )


async def _update_canonical_copy_guard(context, *, add_column=False):
    async with context.engine.begin() as transaction:
        await transaction.exec_driver_sql("SET LOCAL lock_timeout='100ms'")
        if add_column:
            await transaction.run_sync(
                lambda connection: upgrade_generic_source_candidates(connection, context.schema_name)
            )
        await transaction.run_sync(
            lambda connection: upgrade_generic_source_canonical_copy_guard(
                connection, context.schema_name, preparation_schema=context.schema_name
            )
        )


async def _candidate_guard_metadata(context):
    return await context.connection.fetchrow(
        "SELECT to_jsonb(p)-'prosrc' AS identity,prosrc,pg_get_functiondef(p.oid) AS definition,"
        "obj_description(p.oid,'pg_proc') AS description FROM pg_proc p WHERE p.oid=$1::regprocedure",
        f"{context.schema}.guard_pd_dataset_candidate_parent()",
    )


async def _candidate_triggers(context):
    return await context.connection.fetch(
        "SELECT t.oid,t.tgrelid,t.tgfoid,t.tgtype,t.tgenabled,pg_get_triggerdef(t.oid) AS definition "
        "FROM pg_trigger t JOIN pg_class relation ON relation.oid=t.tgrelid "
        "WHERE relation.relnamespace=$1::regnamespace AND NOT t.tgisinternal ORDER BY t.oid",
        context.schema,
    )


async def _verify_copy_guard(context):
    async with context.connection.transaction(readonly=True):
        await verify_generic_source_canonical_copy_guard(
            context.connection, context.schema_name, preparation_schema=context.schema_name
        )


async def _assert_copy_guard_drift_refusals(context, roles, after):
    """Reject altered readers, triggers, session role and guard body."""
    await context.connection.execute(
        f"REVOKE SELECT(publication) ON {context.schema}.preparation FROM {quoted(roles['owner'])}"
    )
    with pytest.raises(RuntimeError, match="preparation_reader_required"):
        await _update_canonical_copy_guard(context)
    with pytest.raises(RuntimeError, match="preparation_reader_required"):
        await _verify_copy_guard(context)
    await context.connection.execute(
        f"GRANT SELECT(publication) ON {context.schema}.preparation TO {quoted(roles['owner'])}"
    )
    await context.connection.execute(
        f"ALTER TABLE {context.schema}.{TABLES[2]} DISABLE TRIGGER a_pd_candidate_update_old"
    )
    with pytest.raises(RuntimeError, match="statement_guards_required"):
        await _update_canonical_copy_guard(context)
    with pytest.raises(RuntimeError, match="statement_guards_required"):
        await _verify_copy_guard(context)
    await context.connection.execute(
        f"ALTER TABLE {context.schema}.{TABLES[2]} ENABLE TRIGGER a_pd_candidate_update_old"
    )
    assert await _candidate_guard_metadata(context) == after
    async with context.connection.transaction():
        await context.connection.execute("SET LOCAL session_replication_role=replica")
        with pytest.raises(RuntimeError, match="statement_guards_required"):
            await verify_generic_source_canonical_copy_guard(
                context.connection, context.schema_name, preparation_schema=context.schema_name
            )
    with pytest.raises(RuntimeError, match="guard_transaction_required"):
        await verify_generic_source_canonical_copy_guard(
            context.connection, context.schema_name, preparation_schema=context.schema_name
        )
    await context.connection.execute(after["definition"].replace("RETURN NULL;", "RETURN NULL; -- unexpected body", 1))
    changed = await _candidate_guard_metadata(context)
    with pytest.raises(RuntimeError, match="canonical_copy_guard_changed"):
        await _update_canonical_copy_guard(context)
    with pytest.raises(RuntimeError, match="canonical_copy_guard_changed"):
        await _verify_copy_guard(context)
    assert await _candidate_guard_metadata(context) == changed


@pytest.mark.asyncio
async def test_canonical_copy_guard_update_preserves_native_identity_and_refuses_drift(monkeypatch):
    async with _candidate_test_scope(monkeypatch) as (context, roles):
        await _install_retirement_child_guards(context)
        await run_migration(context.engine, _candidate_migration(), "upgrade")
        await _prepare_copy_guard_catalog(context, roles)
        async with context.engine.begin() as transaction:
            await transaction.run_sync(
                lambda connection: upgrade_generic_source_candidates(connection, context.schema_name)
            )
        function = f"{context.schema}.guard_pd_dataset_candidate_parent()"
        await context.connection.execute(f"ALTER FUNCTION {function} COST 37 PARALLEL RESTRICTED")
        await context.connection.execute(f"COMMENT ON FUNCTION {function} IS 'synthetic existing guard'")
        await context.connection.execute(f"GRANT EXECUTE ON FUNCTION {function} TO {quoted(roles['generic'])}")
        before, triggers = await _candidate_guard_metadata(context), await _candidate_triggers(context)
        with pytest.raises(RuntimeError, match="canonical_copy_guard_changed"):
            await _verify_copy_guard(context)
        async with context.connection.transaction():
            await context.connection.fetchval(f"SELECT count(*) FROM {context.schema}.{TABLES[0]}")
            await _update_canonical_copy_guard(context)
        after = await _candidate_guard_metadata(context)
        assert after["identity"] == before["identity"] and after["description"] == before["description"]
        assert after["prosrc"] != before["prosrc"]
        assert await _candidate_triggers(context) == triggers
        async with context.connection.transaction(readonly=True):
            await context.connection.execute(f"SET LOCAL ROLE {quoted(roles['generic'])}")
            await verify_generic_source_canonical_copy_guard(
                context.connection, context.schema_name, preparation_schema=context.schema_name
            )
        await _update_canonical_copy_guard(context)
        assert await _candidate_guard_metadata(context) == after
        await _assert_copy_guard_drift_refusals(context, roles, after)


_COPY_GUARD_COLUMNS = {
    TABLES[0]: ("resource_id", "resource_type", "payload_hash", "payload_json"),
    TABLES[1]: ("resource_id", "payload_hash", "payload_json"),
    TABLES[2]: ("network_resource_id", "insurance_plan_resource_id"),
    TABLES[3]: ("participating_organization_resource_id", "affiliation_resource_id"),
}


async def _insert_copy_guard_row(connection, relation, root, dataset_id, key):
    columns = _COPY_GUARD_COLUMNS[root]
    row = _copy_guard_record(root, dataset_id, key)
    values = ",".join("$" + str(index) for index in range(1, len(row) + 1))
    return await connection.execute(f"INSERT INTO {relation}(dataset_id,{','.join(columns)}) VALUES({values})", *row)


def _copy_guard_record(root, dataset_id, key):
    if root == TABLES[0]:
        return dataset_id, key, "Organization", "a" * 64, "{}"
    if root == TABLES[1]:
        return dataset_id, key, "a" * 64, "{}"
    return dataset_id, key, "synthetic-related"


async def _seed_copy_guard_dataset(context, dataset_id, status, protected):
    connection, schema = context.connection, context.schema
    marker = json.dumps({"contract": "generic-source-canonical-copy-v1"}) if protected else None
    await connection.execute(
        f"INSERT INTO {schema}.provider_directory_endpoint_dataset"
        "(dataset_id,endpoint_id,status,is_current,resource_count,publication_metadata_json,candidate_storage_json) "
        "VALUES($1,'endpoint-official','acquiring',false,0,'{}',$2::jsonb)",
        dataset_id,
        marker,
    )
    for root in _COPY_GUARD_COLUMNS:
        await _insert_copy_guard_row(connection, f"{schema}.{root}", root, dataset_id, "seed")
    await connection.execute(
        f"UPDATE {schema}.provider_directory_endpoint_dataset SET status=$2 WHERE dataset_id=$1", dataset_id, status
    )


async def _record_copy_guard_publication(
    context,
    dataset_id,
    *,
    state="published",
    importer="provider-directory-fhir",
    contract="generic-source-canonical-copy-v1",
):
    await context.connection.execute(
        f"INSERT INTO {context.schema}.preparation(importer_id,state,handoff,publication) VALUES($1,$2,$3::jsonb,$4::jsonb)",
        importer,
        state,
        json.dumps({"identity": {"dataset_id": dataset_id}}),
        json.dumps({"native_publication": {"contract": contract}}) if state == "published" else None,
    )


async def _assert_copy_guard_rejects_changes(connection, relation, root, dataset_id):
    with pytest.raises(asyncpg.ObjectNotInPrerequisiteStateError, match="candidate_immutable"):
        async with connection.transaction():
            await _insert_copy_guard_row(connection, relation, root, dataset_id, "late")
    key = _COPY_GUARD_COLUMNS[root][0]
    for statement in (
        f"UPDATE {relation} SET {key}={key} WHERE dataset_id=$1",
        f"DELETE FROM {relation} WHERE dataset_id=$1",
    ):
        with pytest.raises(asyncpg.ObjectNotInPrerequisiteStateError, match="candidate_immutable"):
            async with connection.transaction():
                await connection.execute(statement, dataset_id)


async def _seed_copy_guard_writer_cases(context, roles):
    connection, schema = context.connection, context.schema
    statuses = (
        "validated",
        "published",
        "superseded",
        "verification_baseline",
        "verification_mismatch",
        "acquisition_abandoned",
        "acquisition_retired",
    )
    protected_ids = ["synthetic-protected-" + status for status in statuses]
    for dataset_id, status in zip(protected_ids, statuses, strict=True):
        await _seed_copy_guard_dataset(context, dataset_id, status, True)
        await _record_copy_guard_publication(context, dataset_id)
    # Mutable canonical metadata cannot reopen a protected publication.
    await connection.execute(
        f"UPDATE {schema}.provider_directory_endpoint_dataset SET candidate_storage_json=NULL WHERE dataset_id=$1",
        protected_ids[0],
    )
    await connection.execute(
        f"UPDATE {schema}.provider_directory_endpoint_dataset SET status='acquiring' WHERE dataset_id=$1",
        protected_ids[1],
    )
    await connection.execute(
        f"UPDATE {schema}.provider_directory_endpoint_dataset SET status='acquiring',candidate_storage_json=NULL WHERE dataset_id=$1",
        protected_ids[2],
    )
    mutable = (
        "synthetic-active-copy",
        "synthetic-independent",
        "synthetic-legacy-backfill",
        "synthetic-other-importer",
        "synthetic-other-contract",
    )
    for dataset_id, status, marker in zip(
        mutable,
        ("acquiring", "acquiring", "published", "acquiring", "acquiring"),
        (True, False, False, True, True),
        strict=True,
    ):
        await _seed_copy_guard_dataset(context, dataset_id, status, marker)
    await _record_copy_guard_publication(context, mutable[0], state="validated")
    await _record_copy_guard_publication(context, mutable[3], importer="synthetic-importer")
    await _record_copy_guard_publication(context, mutable[4], contract="synthetic-other-contract")
    await connection.execute(f"GRANT pg_write_all_data,pg_read_all_data TO {quoted(roles['column'])}")
    await connection.execute(f"GRANT {quoted(roles['column'])} TO {quoted(roles['member'])}")
    return protected_ids, mutable


async def _grant_copy_guard_relation_access(context, roles):
    connection, schema = context.connection, context.schema
    relations = await connection.fetch(
        "SELECT oid,oid::regclass::text AS relation FROM pg_class WHERE relnamespace=$1::regnamespace "
        "AND (relname=ANY($2::text[]) OR oid IN (SELECT inhrelid FROM pg_inherits WHERE inhparent="
        "ANY(SELECT oid FROM pg_class WHERE relnamespace=$1::regnamespace AND relname=ANY($2::text[]))))",
        schema,
        list(_COPY_GUARD_COLUMNS),
    )
    for relation_row in relations:
        await connection.execute(
            f"GRANT SELECT,INSERT,UPDATE,DELETE ON {relation_row['relation']} TO {quoted(roles['generic'])}"
        )
    return relations


async def _assert_protected_copy_guard_rows(connection, parent, root, key, protected_ids, mutable):
    for dataset_id in protected_ids:
        child = await connection.fetchval(
            f"SELECT tableoid::regclass::text FROM {parent} WHERE dataset_id=$1", dataset_id
        )
        for relation in (parent, child):
            await _assert_copy_guard_rejects_changes(connection, relation, root, dataset_id)
        for origin, destination in ((dataset_id, mutable[0]), (mutable[0], dataset_id)):
            with pytest.raises(asyncpg.ObjectNotInPrerequisiteStateError, match="candidate_immutable"):
                async with connection.transaction():
                    await connection.execute(
                        f"UPDATE {parent} SET dataset_id=$2,{key}='moved' WHERE dataset_id=$1",
                        origin,
                        destination,
                    )


async def _assert_mutable_copy_guard_rows(connection, parent, root, key, mutable):
    for dataset_id in mutable:
        if root == TABLES[0] and dataset_id == mutable[2]:
            continue  # Existing published-resource protection remains unchanged.
        assert (
            await connection.execute(f"UPDATE {parent} SET {key}={key} WHERE dataset_id=$1", dataset_id) == "UPDATE 1"
        )
        await _insert_copy_guard_row(connection, parent, root, dataset_id, "backfill")
        assert (
            await connection.execute(f"DELETE FROM {parent} WHERE dataset_id=$1 AND {key}='backfill'", dataset_id)
            == "DELETE 1"
        )


async def _assert_guarded_copy_batch(connection, context, root, columns, parent, key, mutable, protected_ids):
    copy_records = [_copy_guard_record(root, dataset_id, "copy-batch") for dataset_id in (mutable[0], protected_ids[1])]
    with pytest.raises(asyncpg.ObjectNotInPrerequisiteStateError, match="candidate_immutable"):
        async with connection.transaction():
            await connection.copy_records_to_table(
                root, schema_name=context.schema_name, columns=("dataset_id", *columns), records=copy_records
            )
    assert not await connection.fetchval(f"SELECT EXISTS(SELECT FROM {parent} WHERE {key}='copy-batch')")


async def _assert_preparation_writes_denied(connection, schema):
    for statement in (
        f"UPDATE {schema}.preparation SET state='validated'",
        f"UPDATE {schema}.preparation SET handoff='{{}}',publication='{{}}'",
        f"DELETE FROM {schema}.preparation",
    ):
        with pytest.raises(asyncpg.InsufficientPrivilegeError):
            async with connection.transaction():
                await connection.execute(statement)


@pytest.mark.asyncio
async def test_canonical_copy_guard_closes_only_finalized_copies_across_writer_paths(monkeypatch):
    async with _candidate_test_scope(monkeypatch) as (context, roles):
        await _install_retirement_child_guards(context)
        await run_migration(context.engine, _candidate_migration(), "upgrade")
        await _prepare_copy_guard_catalog(context, roles)
        await _update_canonical_copy_guard(context, add_column=True)
        connection, schema = context.connection, context.schema
        protected_ids, mutable = await _seed_copy_guard_writer_cases(context, roles)
        relations = await _grant_copy_guard_relation_access(context, roles)
        for role in (roles["generic"], roles["member"]):
            async with connection.transaction():
                await connection.execute(f"SET LOCAL ROLE {quoted(role)}")
                for root, columns in _COPY_GUARD_COLUMNS.items():
                    parent, key = f"{schema}.{root}", columns[0]
                    await _assert_protected_copy_guard_rows(connection, parent, root, key, protected_ids, mutable)
                    await _assert_mutable_copy_guard_rows(connection, parent, root, key, mutable)
                    await _assert_guarded_copy_batch(
                        connection, context, root, columns, parent, key, mutable, protected_ids
                    )
        relation_oids = {relation_row["oid"] for relation_row in relations}
        assert all(
            not (trigger["tgtype"] & 1)
            for trigger in await _candidate_triggers(context)
            if trigger["tgrelid"] in relation_oids
        )
        # Global-write membership itself cannot be part of the protected catalog
        # boundary; ordinary and guard-owner roles retain only their scoped rights.
        await connection.execute(f"REVOKE pg_write_all_data,pg_read_all_data FROM {quoted(roles['column'])}")
        for role in (roles["generic"], roles["member"], roles["owner"]):
            async with connection.transaction():
                await connection.execute(f"SET LOCAL ROLE {quoted(role)}")
                await _assert_preparation_writes_denied(connection, schema)
        with pytest.raises(asyncpg.ObjectNotInPrerequisiteStateError, match="candidate_immutable"):
            await connection.execute(
                f"INSERT INTO {schema}.{TABLES[0]}(dataset_id,resource_type,resource_id,payload_hash,payload_json) "
                "VALUES($1,'Organization','late',repeat('a',64),'{}')",
                mutable[2],
            )
        with pytest.raises(asyncpg.ObjectNotInPrerequisiteStateError, match="child_truncate_forbidden"):
            await connection.execute(f"TRUNCATE {schema}.{TABLES[2]}")


async def _install_candidate_index_probe(context):
    schema = context.schema
    event_name = context.schema_name + "_indexes"
    await context.connection.execute(f"""
        CREATE TABLE {schema}.candidate_index_audit(
            relation_oid oid, attached boolean, index_oids oid[], PRIMARY KEY(relation_oid,attached));
        CREATE FUNCTION {schema}.observe_candidate_indexes() RETURNS event_trigger LANGUAGE plpgsql AS $proof$
        BEGIN
            IF EXISTS (SELECT FROM pg_event_trigger_ddl_commands() command
                JOIN pg_index index_record ON index_record.indexrelid=command.objid
                JOIN pg_class candidate ON candidate.oid=index_record.indrelid
                WHERE command.command_tag='CREATE INDEX' AND candidate.relnamespace='{schema}'::regnamespace
                  AND (candidate.relname LIKE 'pd_ds_%' OR candidate.relname LIKE 'pdgs_%'))
               AND EXISTS (SELECT FROM pg_locks lock JOIN pg_class parent ON parent.oid=lock.relation
                WHERE lock.pid=pg_backend_pid() AND lock.granted AND parent.relnamespace='{schema}'::regnamespace
                  AND parent.relname LIKE 'provider_directory_%'
                  AND lock.mode IN ('ShareUpdateExclusiveLock','ShareLock','ShareRowExclusiveLock',
                                    'ExclusiveLock','AccessExclusiveLock')) THEN
                RAISE EXCEPTION 'candidate_index_build_holds_parent_write_lock';
            END IF;
            INSERT INTO {schema}.candidate_index_audit
                SELECT relation.oid, EXISTS(SELECT FROM pg_inherits WHERE inhrelid=relation.oid),
                       ARRAY(SELECT indexrelid FROM pg_index WHERE indrelid=relation.oid ORDER BY indexrelid)
                  FROM pg_class relation WHERE relnamespace='{schema}'::regnamespace AND relkind='r'
                   AND (relname LIKE 'pd_ds_%' OR relname LIKE 'pdgs_%' OR relname LIKE 'pd_uhc_flex_pr_%'
                        OR relname LIKE 'pdrgw_%' OR relname LIKE 'pdrgr_%' OR relname LIKE 'pdrge_%')
                   AND EXISTS(SELECT FROM pg_index WHERE indrelid=relation.oid)
                ON CONFLICT(relation_oid,attached) DO UPDATE SET index_oids=
                    CASE WHEN EXCLUDED.attached THEN candidate_index_audit.index_oids ELSE EXCLUDED.index_oids END;
        END; $proof$;
        CREATE EVENT TRIGGER "{event_name}" ON ddl_command_end
            EXECUTE FUNCTION {schema}.observe_candidate_indexes();
    """)
    if hasattr(context, "component"):
        for role in (context.component.owner, context.component.builder):
            await context.connection.execute(
                f"GRANT SELECT,INSERT,UPDATE ON TABLE {schema}.candidate_index_audit TO {quoted(role)}"
            )
    return event_name


async def _indexed_candidate_relations(context):
    """Inventory real candidate heaps independently of the event-trigger observations."""
    return {
        row["oid"]
        for row in await context.connection.fetch(
            "SELECT oid FROM pg_class WHERE relnamespace=$1::regnamespace AND relkind='r' "
            "AND relname ~ '^(pd_ds_|pdgs_|pd_uhc_flex_pr_|pdrgw_|pdrgr_|pdrge_)' "
            "AND EXISTS(SELECT FROM pg_index WHERE indrelid=pg_class.oid)",
            context.schema_name,
        )
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("current_source", (False, True), ids=("historical", "current-source"))
async def test_candidate_attach_reuses_every_prepared_native_index(monkeypatch, current_source):
    """Preserve historical index variants and every supported current Source heap's OIDs."""
    async with _lifecycle_scope(monkeypatch, request_failure_budget=current_source) as context:
        if not current_source:
            resource = f"{context.schema}.{TABLES[0]}"
            await context.connection.execute(
                f"ALTER TABLE {resource} ADD CONSTRAINT synthetic_resource_identity UNIQUE(dataset_id,resource_type,resource_id,payload_hash) DEFERRABLE INITIALLY DEFERRED"
            )
            await context.connection.execute(f"CREATE INDEX synthetic_resource_hash ON {resource}(payload_hash)")
        initial_relations = await _indexed_candidate_relations(context)
        event_name = await _install_candidate_index_probe(context)
        try:
            await _claim(context)
        finally:
            await context.connection.execute(f'DROP EVENT TRIGGER "{event_name}"')
        observations = await context.connection.fetch(f"""
            SELECT before.relation_oid, before.relation_oid::regclass::text AS relation, native.relname,
                   before.index_oids AS before_indexes, after.index_oids AS after_indexes
              FROM {context.schema}.candidate_index_audit before
              JOIN pg_class native ON native.oid=before.relation_oid
              LEFT JOIN {context.schema}.candidate_index_audit after
                ON after.relation_oid=before.relation_oid AND after.attached
             WHERE NOT before.attached ORDER BY relation
        """)
        expected_relations = await _indexed_candidate_relations(context) - initial_relations
        assert len(expected_relations) >= 7
        assert {observation["relation_oid"] for observation in observations} == expected_relations
        dataset_prefix = "pdgs_" if current_source else "pd_ds_"
        assert sum(observation["relname"].startswith(dataset_prefix) for observation in observations) == (
            5 if current_source else 2
        )
        for observation in observations:
            assert observation["before_indexes"]
            assert observation["before_indexes"] == observation["after_indexes"], observation["relation"]


@pytest.mark.asyncio
async def test_candidate_attach_has_bounded_lock_wait(monkeypatch):
    async with _lifecycle_scope(monkeypatch) as context:
        async with context.connection.transaction():
            await context.connection.execute(
                f"LOCK TABLE ONLY {context.schema}.{TABLES[0]} IN SHARE UPDATE EXCLUSIVE MODE"
            )
            with pytest.raises(asyncpg.LockNotAvailableError) as failure:
                async with context.database.transaction():
                    await context.database.scalar("SELECT set_config('statement_timeout','4s',true)")
                    await _publish_legacy_root(context.database)
            assert failure.value.sqlstate == "55P03"
        assert await context.database.scalar("SELECT current_setting('lock_timeout')") == "0"
        assert await _publish_legacy_root(context.database)


@pytest.mark.asyncio
async def test_generic_finalization_checks_snapshot_without_copy_checks(monkeypatch):
    async with _candidate_test_scope(monkeypatch) as (context, _roles_by_kind):
        connection, schema = context.connection, context.schema
        await connection.execute(
            f"ALTER TABLE {schema}.{TABLES[0]} ADD CONSTRAINT synthetic_payload_object CHECK(jsonb_typeof(payload_json)='object')"
        )
        await run_migration(context.engine, _candidate_migration(), "upgrade")
        dataset_id = "synthetic-generic-shape"
        await connection.execute(
            f"INSERT INTO {schema}.provider_directory_endpoint_dataset(dataset_id,endpoint_id,status,is_current,resource_count,publication_metadata_json) VALUES($1,'endpoint-official','building',false,1,'{{}}')",
            dataset_id,
        )
        await connection.copy_records_to_table(
            TABLES[0],
            schema_name=context.schema_name,
            columns=("dataset_id", "resource_type", "resource_id", "payload_hash", "payload_json"),
            records=[(dataset_id, "Organization", "synthetic", "a" * 64, "[]")],
        )
        with pytest.raises(asyncpg.CheckViolationError, match="provider_directory_dataset_candidate_content"):
            await connection.execute(
                f"UPDATE {schema}.provider_directory_endpoint_dataset SET status='validated' WHERE dataset_id=$1",
                dataset_id,
            )
        assert (
            await connection.fetchval(
                f"SELECT status FROM {schema}.provider_directory_endpoint_dataset WHERE dataset_id=$1", dataset_id
            )
            == "building"
        )
        await connection.execute(f"UPDATE {schema}.{TABLES[0]} SET payload_json='{{}}' WHERE dataset_id=$1", dataset_id)
        await connection.execute(
            f"UPDATE {schema}.provider_directory_endpoint_dataset SET status='validated' WHERE dataset_id=$1",
            dataset_id,
        )


@pytest.mark.asyncio
async def test_generic_header_delete_serializes_with_snapshot_insert(monkeypatch):
    async with _candidate_test_scope(monkeypatch) as (context, _roles_by_kind):
        connection, schema = context.connection, context.schema
        await run_migration(context.engine, _candidate_migration(), "upgrade")
        dataset_id = "synthetic-generic-race"
        header = f"{schema}.provider_directory_endpoint_dataset"
        await connection.execute(
            f"INSERT INTO {header}(dataset_id,endpoint_id,status,is_current,resource_count,publication_metadata_json) VALUES($1,'endpoint-official','building',false,1,'{{}}')",
            dataset_id,
        )
        independent = await connect(database_url())
        try:
            await independent.execute("SET lock_timeout='100ms'")
            async with connection.transaction():
                await connection.execute(
                    f"INSERT INTO {schema}.{TABLES[0]}(dataset_id,resource_type,resource_id,payload_hash,payload_json) VALUES($1,'Organization','synthetic',repeat('a',64),'{{}}')",
                    dataset_id,
                )
                with pytest.raises(asyncpg.LockNotAvailableError):
                    await independent.execute(f"DELETE FROM {header} WHERE dataset_id=$1", dataset_id)
            with pytest.raises(asyncpg.ForeignKeyViolationError, match="provider_directory_dataset_reference_exists"):
                await independent.execute(f"DELETE FROM {header} WHERE dataset_id=$1", dataset_id)
        finally:
            await independent.close()
        await connection.execute(f"DELETE FROM {schema}.{TABLES[0]} WHERE dataset_id=$1", dataset_id)
        await connection.execute(f"DELETE FROM {header} WHERE dataset_id=$1", dataset_id)
        with pytest.raises(asyncpg.ForeignKeyViolationError, match="provider_directory_dataset_candidate_relationship"):
            await connection.execute(
                f"INSERT INTO {schema}.{TABLES[0]}(dataset_id,resource_type,resource_id,payload_hash,payload_json) VALUES($1,'Organization','late',repeat('a',64),'{{}}')",
                dataset_id,
            )


@pytest.mark.asyncio
async def test_sealed_provider_candidate_rejects_global_write_role(monkeypatch):
    async with _candidate_test_scope(monkeypatch) as (context, roles_by_kind):
        connection, schema = context.connection, context.schema
        await run_migration(context.engine, _candidate_migration(), "upgrade")
        await _publish_legacy_root(context.database)
        candidate = await connection.fetchval(
            "SELECT oid::regclass::text FROM pg_class WHERE relnamespace=$1::regnamespace AND relname LIKE 'pd_ds_%' AND relkind='r' LIMIT 1",
            schema,
        )
        assert candidate
        role = roles_by_kind["column"]
        await connection.execute(f"GRANT pg_write_all_data TO {quoted(role)}")
        async with connection.transaction():
            await connection.execute(f"SET LOCAL ROLE {quoted(role)}")
            with pytest.raises(asyncpg.InsufficientPrivilegeError, match="migration_unauthorized"):
                async with connection.transaction():
                    await connection.execute(f"UPDATE {schema}.pd_dataset_migration_plan SET plan='{{}}'")
            with pytest.raises(asyncpg.ObjectNotInPrerequisiteStateError, match="candidate_immutable"):
                async with connection.transaction():
                    await connection.execute(f"INSERT INTO {candidate} DEFAULT VALUES")


@pytest.mark.asyncio
async def test_legacy_resource_deletion_keeps_provenance_references(monkeypatch):
    async with _candidate_test_scope(monkeypatch) as (context, _roles_by_kind):
        with monkeypatch.context() as patch:
            patch.setattr(practitioner_store, "prepare_dataset_candidate", _historical_dataset_storage)
            patch.setattr(practitioner_materialization, "copy_dataset_candidate_rows", _copy_historical_dataset)
            await _publish_legacy_root(context.database)
        connection, schema = context.connection, context.schema
        dataset_id = await connection.fetchval(
            f"SELECT dataset_id FROM {schema}.provider_directory_uhc_flex_practitioner_dataset WHERE is_current"
        )
        assert dataset_id
        await run_migration(context.engine, _candidate_migration(), "upgrade")
        with pytest.raises(asyncpg.ForeignKeyViolationError, match="provider_directory_dataset_reference_exists"):
            await connection.execute(f"DELETE FROM {schema}.{TABLES[0]} WHERE dataset_id=$1", dataset_id)
        assert (
            await connection.fetchval(f"SELECT count(*) FROM {schema}.{TABLES[0]} WHERE dataset_id=$1", dataset_id) == 1
        )
