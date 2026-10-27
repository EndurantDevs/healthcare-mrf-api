# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native private bootstrap and rollback; never enrollment or activation proof."""

from contextlib import AsyncExitStack
from types import SimpleNamespace
from uuid import uuid4

import asyncpg
import pytest
import pytest_asyncio
from sqlalchemy import MetaData, text
from sqlalchemy.dialects.postgresql import dialect
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.schema import CreateTable

from db.models.system import ProviderDirectoryEndpointDataset
from db.plan_release_storage import storage_identifier as quote
from db.plan_release_storage import storage_relation
from process import provider_directory_generic_source_provision as provision
from process.provider_directory_generic_candidate import GenericCandidateError
from process.provider_directory_generic_source_policy import GenericSourcePolicy
from tests.cms_npd_admission_postgres_support import _database_url, _owned_database
from tests.provider_directory_source_postgres_support import verify_native_source_profile
from tests.test_provider_directory_subset_completion_migration import _load_migration


def _dsn(url):
    return url.set(drivername="postgresql").render_as_string(hide_password=False)


async def _templates(connection):
    """Real header definitions plus small native templates exercise PostgreSQL LIKE."""
    await connection.execute('CREATE SCHEMA "catalog"')
    table = ProviderDirectoryEndpointDataset.__table__.to_metadata(MetaData(), schema="catalog")
    for constraint in tuple(table.foreign_key_constraints):
        table.constraints.remove(constraint)
    await connection.execute(str(CreateTable(table).compile(dialect=dialect())))
    header = storage_relation("catalog", provision.HEADER)
    migration = _load_migration()
    for name, arguments in (
        ("provider_directory_subset_completion_proof_pair_valid", "jsonb,text"),
        ("provider_directory_subset_completion_proof_shape_valid", "jsonb,text,bigint"),
    ):
        await connection.execute(
            f"CREATE FUNCTION catalog.{name}({arguments}) RETURNS boolean LANGUAGE sql IMMUTABLE AS 'SELECT true'"
        )
    for name, expression in (
        ("pd_endpoint_dataset_completion_digest_check", migration._completion_digest_check("catalog")),
        (
            "pd_endpoint_dataset_completion_shape_check",
            migration._subset_proof_shape_check(
                "catalog", reviewed_subset_profile_aware=True, reviewed_subset_terminal_window_profile_aware=True
            ),
        ),
    ):
        await connection.execute(f"ALTER TABLE {header} ADD CONSTRAINT {quote(name)} CHECK ({expression})")
    for ordinal, name in enumerate(provision.ISOLATED_FHIR_ROOTS[1:]):
        relation = storage_relation("catalog", name)
        await connection.execute(
            f"CREATE TABLE {relation} (dataset_id varchar(96) NOT NULL REFERENCES {header}(dataset_id),"
            "item text NOT NULL, value integer NOT NULL DEFAULT 0 CHECK (value>=0),"
            "folded text GENERATED ALWAYS AS (lower(item)) STORED, PRIMARY KEY(dataset_id,item),"
            "UNIQUE(dataset_id,value))" + (" PARTITION BY LIST(dataset_id)" if ordinal == 0 else "")
        )
        await connection.execute(f"CREATE INDEX ON {relation} (folded) INCLUDE(value) WHERE value>0")
        if ordinal == 0:
            await connection.execute(f"CREATE TABLE catalog.payload_leaf PARTITION OF {relation} DEFAULT")
    await connection.execute(
        f"ALTER TABLE {storage_relation('catalog', provision.RELATIONS[-1])} "
        "ADD CONSTRAINT native_array_check CHECK (item <> ALL(ARRAY['forbidden'::varchar,'invalid'::varchar]::text[]))"
    )
    await connection.execute("CREATE FUNCTION catalog.extra(text) RETURNS text LANGUAGE sql IMMUTABLE AS 'SELECT $1'")
    await connection.execute(
        "CREATE FUNCTION catalog.guard() RETURNS trigger LANGUAGE plpgsql AS 'BEGIN RETURN NEW; END'"
    )
    await connection.execute(
        f"CREATE TRIGGER keep_legacy BEFORE INSERT ON {header} FOR EACH ROW EXECUTE FUNCTION catalog.guard()"
    )
    for root in (provision.HEADER, "provider_directory_pagination_checkpoint"):
        await connection.execute(
            f"CREATE CONSTRAINT TRIGGER keep_deferred AFTER INSERT OR UPDATE ON {storage_relation('catalog', root)} "
            "DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION catalog.guard()"
        )


@pytest_asyncio.fixture
async def native_bootstrap():
    """Reuse UUID database cleanup; register every role before creation and verify removal."""
    source_url = _database_url().set(database="postgres")
    admin = await asyncpg.connect(_dsn(source_url))
    roles, registered = (
        {
            key: "private_source_" + key + "_" + uuid4().hex[:16]
            for key in ("owner", "publisher", "reader", "namespace", "outsider")
        },
        [],
    )
    try:
        async with _owned_database(source_url) as (url, _):
            for key, name in roles.items():
                assert not await admin.fetchval("SELECT EXISTS(SELECT FROM pg_roles WHERE rolname=$1)", name)
                registered.append(name)
                await admin.execute(
                    f"CREATE ROLE {quote(name)} " + ("NOLOGIN" if key in {"owner", "namespace"} else "LOGIN")
                )
            await admin.execute(f"GRANT {quote(roles['owner'])} TO {quote(roles['publisher'])}")
            connection = await asyncpg.connect(_dsn(url))
            try:
                await _templates(connection)
                yield SimpleNamespace(
                    connection=connection,
                    url=url,
                    roles=roles,
                    policy=GenericSourcePolicy(
                        "private_source", roles["owner"], roles["publisher"], (roles["reader"],)
                    ),
                )
            finally:
                await connection.close()
    finally:
        try:
            for name in reversed(registered):
                await admin.execute("DROP ROLE IF EXISTS " + quote(name))
                assert not await admin.fetchval("SELECT EXISTS(SELECT FROM pg_roles WHERE rolname=$1)", name)
        finally:
            await admin.close()


async def _provision(case, **overrides):
    arguments_by_name = dict(
        catalog_schema="catalog",
        namespace_owner=case.roles["namespace"],
        verify_role_profile=verify_native_source_profile,
    )
    arguments_by_name.update(overrides)
    return await provision.provision_isolated_fhir_source_storage(case.connection, case.policy, **arguments_by_name)


async def _canonical_state(connection):
    return await connection.fetch(
        "SELECT c.oid,c.relname,c.relowner,c.relacl::text,"
        "ARRAY(SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid=c.oid ORDER BY conname) AS constraints,"
        "ARRAY(SELECT jsonb_build_array(oid,tgfoid,pg_get_triggerdef(oid)) FROM pg_trigger "
        "WHERE tgrelid=c.oid ORDER BY tgname) AS triggers "
        "FROM pg_class c WHERE relnamespace='catalog'::regnamespace ORDER BY c.oid"
    )


async def _assert_copied_native_layouts(connection, schema):
    from process.provider_directory_source_storage_binding import _PRIVATE_LAYOUT_SQL, _isolated_layout_shape

    rows = await connection.fetch(_PRIVATE_LAYOUT_SQL, ["catalog", schema], list(provision.ISOLATED_FHIR_ROOTS))
    layouts_by_key = {(row["nspname"], row["relname"]): row for row in rows}
    for root in provision.ISOLATED_FHIR_ROOTS:
        assert _isolated_layout_shape(layouts_by_key[schema, root], template=False) == _isolated_layout_shape(
            layouts_by_key["catalog", root],
            template=True,
        )


@pytest.mark.parametrize("inherited_role", ["pg_read_all_data", "pg_write_all_data"])
async def test_private_storage_refuses_inherited_runtime_access(native_bootstrap, inherited_role):
    from process.provider_directory_generic_source_storage import require_private_source_readers_closed

    case, connection = native_bootstrap, native_bootstrap.connection
    async with connection.transaction():
        evidence = await _provision(case)
        relation_oids = [entry["oid"] for entry in evidence["inventory"]]
        reader_oids = [evidence["role_oids"][case.roles["reader"]]]
        await require_private_source_readers_closed(connection, relation_oids, reader_oids)
        await connection.execute(f"GRANT {quote(inherited_role)} TO {quote(case.roles['reader'])}")
        with pytest.raises(GenericCandidateError, match="private_reader_access"):
            await require_private_source_readers_closed(connection, relation_oids, reader_oids)


@pytest.mark.parametrize(
    "root,columns",
    [
        ("provider_directory_bulk_acquisition_checkpoint", ("manifest_json",)),
        ("provider_directory_pagination_checkpoint", ("source_ids", "recent_cursor_hashes")),
    ],
)
async def test_only_new_checkpoint_heaps_normalize_json_storage(native_bootstrap, root, columns):
    case, connection = native_bootstrap, native_bootstrap.connection
    canonical = storage_relation("catalog", root)
    for column in columns:
        await connection.execute(f"ALTER TABLE {canonical} ADD COLUMN {quote(column)} json DEFAULT '[]'::jsonb")
    before = await _canonical_state(connection)
    async with connection.transaction():
        await _provision(case)
        assert await _canonical_state(connection) == before
        actual = await connection.fetch(
            "SELECT n.nspname,a.attname,t.typname FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid "
            "JOIN pg_namespace n ON n.oid=c.relnamespace JOIN pg_type t ON t.oid=a.atttypid "
            "WHERE c.relname=$1 AND a.attname=ANY($2::text[]) AND n.nspname=ANY($3::text[])",
            root,
            list(columns),
            ["catalog", case.policy.schema],
        )
        assert {(row["nspname"], row["attname"], row["typname"]) for row in actual} == {
            (schema, column, kind)
            for schema, kind in (("catalog", "json"), (case.policy.schema, "jsonb"))
            for column in columns
        }


async def test_native_private_parent_fence_blocks_delete_not_other_candidate(native_bootstrap):
    from process.provider_directory_source_checkpoint_control import fence_checkpoint_dataset_parents

    case, setup = native_bootstrap, native_bootstrap.connection
    async with setup.transaction():
        await _provision(case)
    header = storage_relation(case.policy.schema, provision.HEADER)
    canonical = storage_relation("catalog", provision.HEADER)
    await setup.execute(f"GRANT USAGE ON SCHEMA catalog TO {quote(case.roles['reader'])}")
    await setup.execute(f"GRANT SELECT ON {canonical} TO {quote(case.roles['reader'])}")
    async with AsyncExitStack() as cleanup:
        connections_by_name = {}
        for name, role in (("writer", "publisher"), ("other", "publisher"), ("reader", "reader")):
            connection = await asyncpg.connect(_dsn(case.url.set(username=case.roles[role])))
            cleanup.push_async_callback(connection.close)
            connections_by_name[name] = connection
        writer = connections_by_name["writer"]
        other = connections_by_name["other"]
        reader = connections_by_name["reader"]
        await writer.execute(
            f"INSERT INTO {header}(dataset_id,endpoint_id,acquisition_root_run_id,status,is_current,resource_count) "
            "VALUES('first','endpoint-a','root-a','acquiring',false,0),('second','endpoint-b','root-b','acquiring',false,0)"
        )
        async with writer.transaction():
            await writer.execute("SET LOCAL ROLE " + quote(case.roles["owner"]))
            assert await writer.fetchval("SELECT session_user") == case.roles["publisher"]
            await fence_checkpoint_dataset_parents(
                SimpleNamespace(connection=writer, policy=case.policy), ["first"], ["root-a"]
            )
            async with reader.transaction():
                await reader.execute("SET LOCAL statement_timeout='100ms'")
                assert await reader.fetchval(f"SELECT count(*) FROM {canonical}") == 0
            async with other.transaction():
                await other.execute("SET LOCAL statement_timeout='100ms'")
                assert (
                    await other.execute(f"UPDATE {header} SET resource_count=1 WHERE dataset_id='second'") == "UPDATE 1"
                )
            with pytest.raises(asyncpg.LockNotAvailableError):
                async with other.transaction():
                    await other.execute("SET LOCAL lock_timeout='50ms'")
                    await other.execute(f"DELETE FROM {header} WHERE dataset_id='first'")
        assert await other.execute(f"DELETE FROM {header} WHERE dataset_id='first'") == "DELETE 1"
        assert await other.fetchval(f"SELECT resource_count FROM {header} WHERE dataset_id='second'") == 1


async def test_native_private_observation_is_readonly_on_original_actor(native_bootstrap):
    from process import provider_directory_source_observation as observation
    from process.provider_directory_source_storage_binding import FHIRSourceStorageBinding

    case = native_bootstrap
    async with case.connection.transaction():
        prepared = await _provision(case)
    coordinate_by_name = {
        name: prepared[name] for name in FHIRSourceStorageBinding.__dataclass_fields__ if name in prepared
    }
    binding = FHIRSourceStorageBinding(**coordinate_by_name, enrollment_sha256="a" * 64)
    factory = SimpleNamespace(policy=case.policy, storage_binding=binding)
    directory = SimpleNamespace(_schema=lambda: "catalog")
    engine = create_async_engine(case.url.set(drivername="postgresql+asyncpg", username=case.roles["publisher"]))
    token = observation.source._FACTORY.set(factory)
    try:
        async with AsyncSession(engine) as session, session.begin():
            await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
            await session.execute(text("SET LOCAL ROLE " + quote(case.roles["owner"])))
            driver = await observation._copy_driver(session)
            origin = await observation._native_origin(directory, factory, session, driver, "repeatable read")
            header = storage_relation(case.policy.schema, provision.HEADER)
            assert (await session.execute(text(f"SELECT count(*) FROM {header}"))).scalar_one() == 0
            with pytest.raises(DBAPIError, match="read-only transaction"):
                async with session.begin_nested():
                    await session.execute(text(f"DELETE FROM {header}"))
            assert await observation._native_origin(directory, factory, session, driver, "repeatable read") == origin
    finally:
        observation.source._FACTORY.reset(token)
        await engine.dispose()


async def _assert_complete_handoff_copies(writer, policy, binding):
    from process import provider_directory_generic_source_completion as completion

    names = await completion._prepare_handoff_controls(writer, policy, binding)
    binding["tables"].update({root: {"name": name} for root, name in names.items()})
    await completion._verify_handoff_controls(writer, policy, binding)
    header = storage_relation(policy.schema, names[provision.HEADER])
    proof = storage_relation(policy.schema, names[provision.PROOF_ROOT])
    assert await writer.fetchval(f"SELECT folded FROM {proof}") == "upper"
    assert await writer.fetchval(f"SELECT payload FROM {proof}") == b"\x00\xff\x01"
    for statement in (
        f"UPDATE {header} SET publication_metadata_summary_json='{{\"changed\":true}}'::jsonb",
        f"UPDATE {header} SET publication_metadata_json='{{\"full\": true}}'::json",
        f"UPDATE {proof} SET payload=decode('00ff02','hex')",
    ):
        with pytest.raises(GenericCandidateError, match="control_snapshot_changed"):
            async with writer.transaction():
                await writer.execute(statement)
                await completion._verify_handoff_controls(writer, policy, binding)
        await completion._verify_handoff_controls(writer, policy, binding)
    captured = await completion.storage._capture(writer, policy, list(names.values()))
    assert len(captured) == 2
    assert not await writer.fetchval(
        "SELECT EXISTS(SELECT FROM pg_constraint WHERE conrelid=ANY($1::oid[]) AND contype='f')",
        [entry["oid"] for entry in captured.values()],
    )


async def test_native_handoff_copies_full_controls_without_blocking_other_dataset(native_bootstrap):
    case, setup = native_bootstrap, native_bootstrap.connection
    await setup.execute(
        f"ALTER TABLE {storage_relation('catalog', provision.HEADER)} "
        "ALTER COLUMN publication_metadata_json TYPE json USING publication_metadata_json::json"
    )
    await setup.execute(
        f"ALTER TABLE {storage_relation('catalog', provision.PROOF_ROOT)} ADD payload bytea, ADD annotation jsonb"
    )
    async with setup.transaction():
        await _provision(case)
    header = storage_relation(case.policy.schema, provision.HEADER)
    proof = storage_relation(case.policy.schema, provision.PROOF_ROOT)
    canonical = storage_relation("catalog", provision.HEADER)
    await setup.execute(f"GRANT USAGE ON SCHEMA catalog TO {quote(case.roles['reader'])}")
    await setup.execute(f"GRANT SELECT ON {canonical} TO {quote(case.roles['reader'])}")
    binding_by_field = dict(
        identity={"dataset_id": "selected"},
        tables={
            provision.RELATIONS[0]: {"name": "pdgs_" + "b" * 32 + "_r"},
        },
    )
    async with AsyncExitStack() as cleanup:
        connections = []
        for role in ("publisher", "publisher", "reader"):
            connection = await asyncpg.connect(_dsn(case.url.set(username=case.roles[role])))
            cleanup.push_async_callback(connection.close)
            connections.append(connection)
        writer, other, reader = connections
        await writer.execute(
            f"INSERT INTO {header}(dataset_id,endpoint_id,status,is_current,resource_count,publication_metadata_json) "
            "VALUES('selected','a','validated',false,1,'{\"full\":true}'),('other','b','acquiring',false,0,'{}')"
        )
        await writer.execute(
            f"INSERT INTO {proof}(dataset_id,item,value,payload,annotation) "
            "VALUES('selected','UPPER',1,$1,'{\"full\":true}')",
            b"\x00\xff\x01",
        )
        async with writer.transaction():
            await writer.execute("SET LOCAL ROLE " + quote(case.roles["owner"]))
            await writer.fetchval(f"SELECT dataset_id FROM {header} WHERE dataset_id='selected' FOR UPDATE")
            await _assert_complete_handoff_copies(writer, case.policy, binding_by_field)
            async with reader.transaction():
                await reader.execute("SET LOCAL statement_timeout='100ms'")
                assert await reader.fetchval(f"SELECT count(*) FROM {canonical}") == 0
            async with other.transaction():
                await other.execute("SET LOCAL statement_timeout='100ms'")
                assert (
                    await other.execute(f"UPDATE {header} SET resource_count=2 WHERE dataset_id='other'") == "UPDATE 1"
                )


async def test_native_bootstrap_preserves_guards_reads_and_fk_free_storage(native_bootstrap):
    case, connection = native_bootstrap, native_bootstrap.connection
    before = await _canonical_state(connection)
    await connection.execute("ALTER DEFAULT PRIVILEGES GRANT ALL ON TABLES TO " + quote(case.roles["reader"]))
    reader = await asyncpg.connect(_dsn(case.url))
    try:
        async with connection.transaction():
            receipt = await _provision(case)
            assert receipt["preparation_only"] is True
            assert set(receipt["required_application_checks"]) == set(provision._ISOLATED_APPLICATION_CHECKS)
            assert receipt["owner_oid"] != receipt["namespace_owner_oid"]
            assert receipt["role_oids"][case.roles["publisher"]] == receipt["publisher_oid"]
            assert receipt["role_oids"][case.roles["owner"]] == receipt["owner_oid"]
            for check in receipt["required_application_checks"].values():
                assert len(check["function_oids"]) == 1 and check["function_oids"][0] > 0
            assert receipt["catalog_schema_oid"] != receipt["storage_schema_oid"]
            assert len(receipt["inventory"]) == len(provision.ISOLATED_FHIR_ROOTS)
            await _assert_copied_native_layouts(connection, case.policy.schema)
            for table in receipt["inventory"]:
                assert table["schema_oid"] == receipt["storage_schema_oid"]
                assert table["schema_owner_oid"] == receipt["namespace_owner_oid"]
                assert table["owner"] == receipt["owner_oid"] and table["relfilenode"] > 0
            locks = await connection.fetch(
                "SELECT c.relname,l.mode FROM pg_locks l JOIN pg_class c ON c.oid=l.relation "
                "WHERE l.pid=pg_backend_pid() AND c.relnamespace='catalog'::regnamespace"
            )
            assert {lock["mode"] for lock in locks} == {"AccessShareLock"}
            assert "payload_leaf" not in {lock["relname"] for lock in locks}
            async with reader.transaction():
                await reader.execute("SET LOCAL statement_timeout='100ms'")
                assert (
                    await reader.fetchval(f"SELECT count(*) FROM {storage_relation('catalog', provision.HEADER)}") == 0
                )
            assert not await connection.fetchval(
                "SELECT EXISTS(SELECT FROM pg_constraint k JOIN pg_class c ON c.oid=k.conrelid "
                "WHERE c.relnamespace=$1::oid AND k.contype='f') OR EXISTS(SELECT FROM pg_trigger t "
                "JOIN pg_class c ON c.oid=t.tgrelid WHERE c.relnamespace=$1::oid) "
                "OR EXISTS(SELECT FROM pg_proc WHERE pronamespace=$1::oid)",
                receipt["storage_schema_oid"],
            )
            assert await _canonical_state(connection) == before
        private = storage_relation(case.policy.schema, provision.RELATIONS[0])
        await connection.execute(f"INSERT INTO {private}(dataset_id,item,value) VALUES('unreferenced','UPPER',1)")
        assert await connection.fetchval(f"SELECT folded FROM {private}") == "upper"
        with pytest.raises(asyncpg.CheckViolationError):
            await connection.execute(f"INSERT INTO {private}(dataset_id,item,value) VALUES('other','bad',-1)")
        with pytest.raises(asyncpg.UniqueViolationError):
            await connection.execute(f"INSERT INTO {private}(dataset_id,item,value) VALUES('unreferenced','other',1)")
        with pytest.raises(asyncpg.NotNullViolationError):
            await connection.execute(f"INSERT INTO {private}(dataset_id,item,value) VALUES('other',NULL,1)")
        assert not await connection.fetchval(
            "SELECT has_table_privilege($1,$2,'SELECT,INSERT,UPDATE,DELETE,TRUNCATE')", case.roles["reader"], private
        )
    finally:
        await reader.close()


@pytest.mark.parametrize("kind", ["check", "default", "index", "sequence", "same_named_check"])
async def test_other_custom_dependencies_are_rejected_without_private_state(native_bootstrap, kind):
    case, connection = native_bootstrap, native_bootstrap.connection
    relation = storage_relation("catalog", provision.RELATIONS[1])
    statements_by_kind = {
        "check": f"ALTER TABLE {relation} ADD CHECK (catalog.extra(item)<>'')",
        "default": f"ALTER TABLE {relation} ALTER COLUMN item SET DEFAULT catalog.extra('value')",
        "index": f"CREATE INDEX ON {relation} (catalog.extra(item))",
        "sequence": f"CREATE SEQUENCE catalog.extra_sequence; ALTER TABLE {relation} ALTER COLUMN value SET DEFAULT nextval('catalog.extra_sequence')",
        "same_named_check": f"ALTER TABLE {relation} ADD CONSTRAINT pd_endpoint_dataset_completion_digest_check CHECK (catalog.extra(item)<>'')",
    }
    await connection.execute(statements_by_kind[kind])
    async with connection.transaction():
        with pytest.raises(GenericCandidateError, match="template_dependency_unsupported"):
            await _provision(case)
        assert not await connection.fetchval("SELECT EXISTS(SELECT FROM pg_namespace WHERE nspname='private_source')")


@pytest.mark.parametrize(
    "change", ["missing_check", "changed_check", "no_primary", "missing_root", "existing_namespace"]
)
async def test_partial_or_changed_preimages_are_rejected(native_bootstrap, change):
    case, connection = native_bootstrap, native_bootstrap.connection
    header = storage_relation("catalog", provision.HEADER)
    if change in {"missing_check", "changed_check"}:
        await connection.execute(f"ALTER TABLE {header} DROP CONSTRAINT pd_endpoint_dataset_completion_digest_check")
        if change == "changed_check":
            await connection.execute(
                f"ALTER TABLE {header} ADD CONSTRAINT pd_endpoint_dataset_completion_digest_check CHECK (true)"
            )
    elif change == "no_primary":
        relation = storage_relation("catalog", provision.RELATIONS[1])
        constraint = await connection.fetchval(
            "SELECT conname FROM pg_constraint WHERE conrelid=$1::regclass AND contype='p'", relation
        )
        await connection.execute(f"ALTER TABLE {relation} DROP CONSTRAINT {quote(constraint)}")
    elif change == "missing_root":
        await connection.execute("DROP TABLE " + storage_relation("catalog", provision.RELATIONS[1]))
    else:
        await connection.execute("CREATE SCHEMA private_source")
    async with connection.transaction():
        with pytest.raises((GenericCandidateError, asyncpg.UndefinedTableError)):
            await _provision(case)
        assert await connection.fetchval("SELECT EXISTS(SELECT FROM pg_namespace WHERE nspname='private_source')") is (
            change == "existing_namespace"
        )


async def test_late_failure_rolls_back_all_private_ddl_and_preserves_outer_transaction(native_bootstrap, monkeypatch):
    case, connection = native_bootstrap, native_bootstrap.connection
    original, calls = provision._close_grants, []

    async def fail_after_creation(*args, **kwargs):
        await original(*args, **kwargs)
        calls.append(args[2])
        if len(calls) == 3:
            raise RuntimeError("injected private bootstrap failure")

    monkeypatch.setattr(provision, "_close_grants", fail_after_creation)
    async with connection.transaction():
        with pytest.raises(RuntimeError, match="injected private bootstrap"):
            await _provision(case)
        assert connection.is_in_transaction()
        assert not await connection.fetchval("SELECT EXISTS(SELECT FROM pg_namespace WHERE nspname='private_source')")
        assert await connection.fetchval("SELECT 1") == 1


@pytest.mark.parametrize(
    "change",
    ["owner_alias", "schema_alias", "reserved_schema", "source_reaches_namespace", "reader_reaches_source", "set_role"],
)
async def test_collapsed_or_reachable_authority_never_leaves_a_private_namespace(native_bootstrap, change):
    case, connection = native_bootstrap, native_bootstrap.connection
    overrides_by_change = {
        "owner_alias": {"namespace_owner": case.roles["owner"]},
        "schema_alias": {"catalog_schema": case.policy.schema},
        "reserved_schema": {"catalog_schema": "pg_catalog"},
    }
    if change == "source_reaches_namespace":
        await connection.execute(f"GRANT {quote(case.roles['namespace'])} TO {quote(case.roles['owner'])}")
    elif change == "reader_reaches_source":
        await connection.execute(f"GRANT {quote(case.roles['owner'])} TO {quote(case.roles['reader'])}")
    async with connection.transaction():
        if change == "set_role":
            await connection.execute("SET LOCAL ROLE " + quote(case.roles["publisher"]))
        with pytest.raises((GenericCandidateError, ValueError)):
            await _provision(case, **overrides_by_change.get(change, {}))
        assert not await connection.fetchval("SELECT EXISTS(SELECT FROM pg_namespace WHERE nspname='private_source')")


async def test_original_superuser_and_outer_transaction_are_mandatory(native_bootstrap):
    case = native_bootstrap
    with pytest.raises(GenericCandidateError, match="transaction_required"):
        await _provision(case)
    publisher = await asyncpg.connect(_dsn(case.url.set(username=case.roles["publisher"])))
    try:
        case.connection = publisher
        async with publisher.transaction():
            with pytest.raises(GenericCandidateError, match="independent_admin_required"):
                await _provision(case)
    finally:
        await publisher.close()
    outsider = await asyncpg.connect(_dsn(case.url.set(username=case.roles["outsider"])))
    try:
        case.connection = outsider
        async with outsider.transaction():
            with pytest.raises(GenericCandidateError, match="native_admin_required"):
                await _provision(case)
    finally:
        await outsider.close()
