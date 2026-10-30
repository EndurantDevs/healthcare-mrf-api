# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native callback certification in a newly created disposable database."""

import importlib.util
import json
import os
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import asyncpg
import pytest
import pytest_asyncio
from sqlalchemy import MetaData, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.ext.asyncio import create_async_engine

from db.models.registry_aca_source_notification import RegistryACASourceNotification
from db.registry_fhir_catalog import SNAPSHOT_COLUMNS, table_sql
from process import registry_execution_catalog as catalog
from process.registry_management_permissions import _MODELS
from tests.test_network_fhir_retained_capture_postgres import legacy_capture_db as legacy_capture_db

pytestmark = pytest.mark.asyncio(loop_scope="module")


@pytest.mark.parametrize("namespace", ["source_control", "mrf", "public", "pg_catalog", "information_schema"])
async def test_control_namespace_cannot_alias_protected_storage(namespace):
    with pytest.raises(catalog.RegistryExecutionCatalogError, match="control_separation"):
        catalog.registry_execution_catalog_parameters(
            namespace,
            "public_owner",
            "migration",
            "queue_owner",
            "custody_owner",
            ("lifecycle", "execution", "loading", "custody_owner", "keeper"),
            "reader",
            protected_control_schema="source_control",
        )


async def test_fixed_model_roster_includes_complete_catalog_snapshot():
    columns_by_table = catalog.registry_execution_columns()
    assert len(columns_by_table) == 42
    assert set(columns_by_table) == set(catalog._TABLES) | {
        "registry_cms_source_notification",
        "registry_aca_source_notification",
    }
    snapshot = columns_by_table["registry_fhir_catalog_snapshot"]
    assert tuple(column[0] for column in snapshot) == SNAPSHOT_COLUMNS
    assert snapshot[0] == ("metadata_generation", "25", -1, True)
    assert snapshot[1] == ("schema_name", "1043", 67, True)
    assert {name for name, _oid, _modifier, required in snapshot if not required} == {
        "network_generation",
        "manifest_sha256",
        "published_at",
    }


@pytest.mark.parametrize("change", ("missing_model", "missing_table", "extra_table", "missing_column", "extra_column"))
async def test_fixed_model_roster_refuses_missing_extra_or_column_drift(monkeypatch, change):
    if change == "missing_model":
        monkeypatch.setattr(catalog, "_MODELS", catalog._MODELS[:-1])
    elif change in {"missing_table", "extra_table"}:
        tables_by_name = dict(catalog._TABLES)
        if change == "missing_table":
            tables_by_name.pop("registry_fhir_catalog_snapshot")
        else:
            tables_by_name["unexpected_control_table"] = ("id",)
        monkeypatch.setattr(catalog, "_TABLES", tables_by_name)
    else:
        columns = SNAPSHOT_COLUMNS[:-1] if change == "missing_column" else (*SNAPSHOT_COLUMNS, "unexpected")
        monkeypatch.setattr(catalog, "SNAPSHOT_COLUMNS", columns)
    with pytest.raises(catalog.RegistryExecutionCatalogError, match="model_(?:roster|catalog_columns)$"):
        catalog.registry_execution_columns()


async def test_storage_plan_provisions_the_complete_certified_roster():
    """Inspect canonical creation statements without opening a database or issuing privileges."""
    statements, model_tables = [], set()

    async def record(statement):
        statements.append(str(statement))

    async def model_plan(create_all):
        model_tables.update(table.name for table in create_all.__self__.tables.values())

    connection = SimpleNamespace(execute=record, exec_driver_sql=record, run_sync=model_plan)
    await _storage(connection, "registry_plan", {"queue": "queue_owner", "custody": "custody_owner"})
    created_tables = {
        statement.split("(", 1)[0].rsplit(".", 1)[1].strip().strip('"')
        for statement in statements
        if statement.startswith("CREATE TABLE ")
    }
    assert model_tables | created_tables == set(catalog.registry_execution_columns())
    for statement in table_sql('"registry_plan"', "registry_fhir_catalog_snapshot"):
        assert statements.count(statement) == 1
    assert statements.count('ALTER TABLE "registry_plan"."registry_fhir_catalog_snapshot" OWNER TO "queue_owner"') == 1


def _migration(filename):
    path = Path(__file__).parents[1] / "alembic" / "versions" / filename
    spec = importlib.util.spec_from_file_location("execution_fixture_" + path.stem, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


async def _storage(connection, schema, roles_by_kind):
    await connection.execute(text(f'CREATE SCHEMA "{schema}" AUTHORIZATION "{roles_by_kind["queue"]}"'))
    metadata = MetaData()
    for model in (*_MODELS, RegistryACASourceNotification):
        copied = model.__table__.to_metadata(metadata, schema=schema, referred_schema_fn=lambda *_: schema)
        for column in copied.columns:
            if column.identity is None and model.__tablename__ != "registry_approval_history":
                # Other frozen registry migrations allocate ids explicitly.
                column.autoincrement = False
    await connection.run_sync(metadata.create_all)
    for filename in (
        "20261009010000_registry_ptg_producer_scope.py",
        "20261009040000_registry_ptg_published_plan_scope.py",
        "20261010010000_registry_ptg_office_approval.py",
        "20261010020000_registry_ptg_office_retention.py",
        "20261017010000_registry_cms_source_notifications.py",
    ):
        for statement in _migration(filename)._ddl(schema):
            await connection.exec_driver_sql(statement)
    for statement in table_sql(f'"{schema}"', "registry_fhir_catalog_snapshot"):
        await connection.exec_driver_sql(statement)
    for table in catalog.registry_execution_columns():
        owner = roles_by_kind["custody"] if table == "network_candidate_custody" else roles_by_kind["queue"]
        await connection.execute(text(f'ALTER TABLE "{schema}"."{table}" OWNER TO "{owner}"'))


async def _catalog_fixture(connection, execution_password=None):
    await connection.execute(text("SET LOCAL search_path=pg_catalog,public"))
    roles_by_kind = {
        name: "catalog_" + name + "_" + uuid4().hex[:12]
        for name in (
            "queue",
            "migration",
            "lifecycle",
            "execution",
            "loading",
            "custody",
            "keeper",
            "reader",
        )
    }
    for name, role in roles_by_kind.items():
        login = "LOGIN" if name in {"execution", "lifecycle", "keeper", "migration", "reader"} else "NOLOGIN"
        password = f" PASSWORD '{execution_password}'" if name == "execution" and execution_password else ""
        await connection.execute(text(f'CREATE ROLE "{role}" {login}{password}'))
    await connection.execute(text("CREATE EXTENSION intarray WITH SCHEMA public"))
    await connection.execute(text("REVOKE ALL ON SCHEMA public FROM PUBLIC,pg_database_owner"))
    await connection.execute(text(f'ALTER SCHEMA public OWNER TO "{roles_by_kind["queue"]}"'))
    schema = "registry_fixture"
    await _storage(connection, schema, roles_by_kind)
    workers = tuple(roles_by_kind[name] for name in ("lifecycle", "execution", "loading", "custody", "keeper"))
    parameters = catalog.registry_execution_catalog_parameters(
        schema,
        roles_by_kind["queue"],
        roles_by_kind["migration"],
        roles_by_kind["queue"],
        roles_by_kind["custody"],
        workers,
        roles_by_kind["reader"],
        protected_control_schema="source_control",
    )
    owner_oid_by_name = dict(
        (
            await connection.execute(
                text("SELECT rolname,oid::text FROM pg_roles WHERE rolname=ANY(CAST(:names AS text[]))"),
                {"names": list(roles_by_kind.values())},
            )
        ).all()
    )
    return SimpleNamespace(
        connection=connection,
        schema=schema,
        roles_by_kind=roles_by_kind,
        parameters=parameters,
        owner_oid_by_name=owner_oid_by_name,
    )


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def native_catalog():
    supplied = os.getenv("NETWORK_REGISTRY_TEST_DSN")
    if not supplied:
        pytest.skip("NETWORK_REGISTRY_TEST_DSN must name a disposable native test service")
    source_url = make_url(supplied)
    admin = await asyncpg.connect(source_url.set(drivername="postgresql").render_as_string(hide_password=False))
    database = "registry_execution_test_" + uuid4().hex
    engine = None
    is_created = False
    try:
        assert await admin.fetchval(
            "SELECT current_user=session_user AND rolsuper FROM pg_roles WHERE rolname=current_user"
        )
        assert not await admin.fetchval("SELECT EXISTS(SELECT FROM pg_database WHERE datname=$1)", database)
        await admin.execute(f'CREATE DATABASE "{database}"')
        is_created = True
        engine = create_async_engine(source_url.set(drivername="postgresql+asyncpg", database=database))
        async with engine.connect() as connection:
            transaction = await connection.begin()
            try:
                yield await _catalog_fixture(connection)
            finally:
                await transaction.rollback()
    finally:
        if engine is not None:
            await engine.dispose()
        if is_created:
            await admin.execute(f'DROP DATABASE "{database}"')
            assert not await admin.fetchval("SELECT EXISTS(SELECT FROM pg_database WHERE datname=$1)", database)
        await admin.close()


async def _capture(fixture):
    value = await fixture.connection.scalar(text(catalog.REGISTRY_EXECUTION_CATALOG_SQL), fixture.parameters)
    return json.loads(value) if isinstance(value, str) else value


def _verify(fixture, observed):
    return catalog.require_registry_execution_catalog(
        observed,
        control_schema=fixture.schema,
        queue_owner_oid=fixture.owner_oid_by_name[fixture.roles_by_kind["queue"]],
        custody_owner_oid=fixture.owner_oid_by_name[fixture.roles_by_kind["custody"]],
    )


async def test_fixed_models_extensions_and_twenty_native_ri_hooks(native_catalog):
    observed = await _capture(native_catalog)
    assert {native_row["relname"] for native_row in observed["relations"] if native_row["relkind"] == "S"} == {
        "network_registry_identity_network_id_seq",
        "network_serving_manifest_generation_id_seq",
        "registry_approval_history_approved_revision_seq",
    }
    assert _verify(native_catalog, observed)["contract"] == catalog.CONTRACT
    assert len(observed["triggers"]) == 20
    assert len(catalog.registry_execution_columns()) == 42
    assert not any(native_row["prosecdef"] for native_row in observed["functions"])


def _change_extension_catalog(changed, public_function, mutation):
    extension_oid = next(
        native_row["refobjid"]
        for native_row in changed["dependencies"]
        if native_row["classid"] == "1255"
        and native_row["objid"] == public_function["oid"]
        and native_row["deptype"] == "e"
    )
    extension = next(native_row for native_row in changed["extensions"] if native_row["oid"] == extension_oid)
    field, field_value = {
        "extension_name": ("extname", "unreviewed_extension"),
        "extension_schema": ("extnamespace", changed["namespace"]["oid"]),
        "extension_version": ("extversion", ""),
    }[mutation]
    extension[field] = field_value


@pytest.mark.parametrize(
    "mutation",
    (
        "function_owner",
        "function_definer",
        "extension_member",
        "extension_name",
        "extension_schema",
        "extension_version",
        "foreign_key",
        "hook",
        "type",
        "extra_table",
        "missing_callback",
        "event_hook",
        "unsafe",
    ),
)
async def test_native_catalog_tampering_is_refused(native_catalog, mutation):
    changed = deepcopy(await _capture(native_catalog))
    public_function = next(
        native_row
        for native_row in changed["functions"]
        if native_row["pronamespace"] == changed["public_namespace"]["oid"]
    )
    match mutation:
        case "function_owner":
            public_function["proowner"] = native_catalog.owner_oid_by_name[native_catalog.roles_by_kind["execution"]]
        case "function_definer":
            public_function["prosecdef"] = True
        case "extension_member":
            changed["dependencies"] = [
                native_row
                for native_row in changed["dependencies"]
                if not (
                    native_row["classid"] == "1255"
                    and native_row["objid"] == public_function["oid"]
                    and native_row["deptype"] == "e"
                )
            ]
        case "extension_name" | "extension_schema" | "extension_version":
            _change_extension_catalog(changed, public_function, mutation)
        case "foreign_key":
            next(native_row for native_row in changed["constraints"] if native_row["contype"] == "f")["confdeltype"] = (
                "c"
            )
        case "hook":
            changed["triggers"][0]["tgfoid"] = public_function["oid"]
        case "type":
            next(native_row for native_row in changed["attributes"] if native_row["attnum"] > 0)["atttypid"] = "25"
        case "extra_table":
            changed["relations"].append(
                {
                    **next(native_row for native_row in changed["relations"] if native_row["relkind"] == "r"),
                    "oid": "4294967000",
                    "relname": "unexpected",
                }
            )
        case "missing_callback":
            changed["functions"] = [native_row for native_row in changed["functions"] if native_row["oid"] != "1644"]
        case "event_hook":
            changed["event_triggers"].append({"oid": "99999"})
        case _:
            changed["native_safe"] = False
    with pytest.raises(catalog.RegistryExecutionCatalogError):
        _verify(native_catalog, changed)


async def test_changed_native_body_is_part_of_exact_catalog(native_catalog):
    before = await _capture(native_catalog)
    async with native_catalog.connection.begin_nested() as transaction:
        await native_catalog.connection.execute(
            text("CREATE FUNCTION public.catalog_unreviewed() RETURNS integer LANGUAGE sql AS 'SELECT 1'")
        )
        changed = await _capture(native_catalog)
        assert changed != before
        with pytest.raises(catalog.RegistryExecutionCatalogError, match="function_provenance"):
            _verify(native_catalog, changed)
        await transaction.rollback()
    assert await _capture(native_catalog) == before


async def test_unreviewed_schema_or_reader_reach_is_refused(native_catalog):
    before = await _capture(native_catalog)
    for statement in (
        f'GRANT "{native_catalog.roles_by_kind["reader"]}" TO "{native_catalog.roles_by_kind["execution"]}"',
        f'GRANT USAGE ON SCHEMA pg_toast TO "{native_catalog.roles_by_kind["execution"]}"',
    ):
        async with native_catalog.connection.begin_nested() as transaction:
            await native_catalog.connection.execute(text(statement))
            assert (await _capture(native_catalog))["native_safe"] is False
            await transaction.rollback()
    assert await _capture(native_catalog) == before


async def test_complete_fixed_native_extension_surface(native_catalog):
    available_names = set(
        (await native_catalog.connection.execute(text("SELECT name FROM pg_catalog.pg_available_extensions"))).scalars()
    )
    if not catalog._EXTENSIONS <= available_names:
        pytest.skip("The fixed native extension packages are not all installed on this test service")
    before = await _capture(native_catalog)
    async with native_catalog.connection.begin_nested() as transaction:
        for name in sorted(catalog._EXTENSIONS - {"intarray"}):
            await native_catalog.connection.execute(text(f'CREATE EXTENSION "{name}" WITH SCHEMA public'))
        observed = await _capture(native_catalog)
        assert _verify(native_catalog, observed)["contract"] == catalog.CONTRACT
        assert len(observed["functions"]) > len(before["functions"])
        assert any(native_row["relkind"] == "v" for native_row in observed["relations"])
        await transaction.rollback()
    assert await _capture(native_catalog) == before


async def _install_trusted_extensions(fixture):
    """Use a real nonsuperuser installer, then remove its temporary installation grants."""
    connection = fixture.connection
    installer = "catalog_installer_" + uuid4().hex[:12]
    await connection.execute(text(f'CREATE ROLE "{installer}" NOSUPERUSER NOCREATEDB NOCREATEROLE'))
    database = await connection.scalar(text("SELECT current_database()"))
    await connection.execute(text(f'GRANT CREATE ON DATABASE "{database}" TO "{installer}"'))
    await connection.execute(text(f'GRANT USAGE,CREATE ON SCHEMA public TO "{installer}"'))
    await connection.execute(text(f'SET LOCAL ROLE "{installer}"'))
    assert await connection.scalar(
        text(
            "SELECT current_user<>session_user AND NOT (rolsuper OR rolcreatedb OR rolcreaterole) "
            "FROM pg_roles WHERE rolname=current_user"
        )
    )
    for extension in ("pgcrypto", "btree_gist"):
        await connection.execute(text(f'CREATE EXTENSION "{extension}" WITH SCHEMA public'))
    await connection.execute(text("RESET ROLE"))
    await connection.execute(text(f'REVOKE USAGE,CREATE ON SCHEMA public FROM "{installer}"'))
    await connection.execute(text(f'REVOKE CREATE ON DATABASE "{database}" FROM "{installer}"'))
    return installer


async def _require_installer_refusal(fixture, installer, statement):
    async with fixture.connection.begin_nested() as transaction:
        await fixture.connection.execute(text(f'GRANT USAGE,CREATE ON SCHEMA public TO "{installer}"'))
        await fixture.connection.execute(text(f'SET LOCAL ROLE "{installer}"'))
        assert await fixture.connection.scalar(
            text(
                "SELECT has_schema_privilege(current_user,'public','USAGE') "
                "AND has_schema_privilege(current_user,'public','CREATE')"
            )
        )
        with pytest.raises(ProgrammingError) as error:
            await fixture.connection.execute(text(statement))
        assert error.value.orig.sqlstate == "42501"
        assert "owner of function" in str(error.value.orig)
        await transaction.rollback()


async def test_trusted_extension_installer_is_distinct_from_protected_members(native_catalog):
    before = await _capture(native_catalog)
    async with native_catalog.connection.begin_nested() as transaction:
        installer = await _install_trusted_extensions(native_catalog)
        observed = await _capture(native_catalog)
        installer_oid = await native_catalog.connection.scalar(
            text("SELECT oid::text FROM pg_roles WHERE rolname=:role"), {"role": installer}
        )
        extension_by_oid = {
            native_row["oid"]: native_row
            for native_row in observed["extensions"]
            if native_row["extname"] in {"pgcrypto", "btree_gist"}
        }
        assert len(extension_by_oid) == 2 and all(
            native_row["extowner"] == installer_oid != "10" for native_row in extension_by_oid.values()
        )
        member_oids = {
            native_row["objid"]
            for native_row in observed["dependencies"]
            if native_row["classid"] == "1255"
            and native_row["deptype"] == "e"
            and native_row["refobjid"] in extension_by_oid
        }
        members = [native_row for native_row in observed["functions"] if native_row["oid"] in member_oids]
        assert len(members) == len(member_oids) > 0
        assert all(native_row["proowner"] == "10" and native_row["prosecdef"] is False for native_row in members)
        assert _verify(native_catalog, observed)["contract"] == catalog.CONTRACT
        for statement in (
            "CREATE OR REPLACE FUNCTION public.digest(text,text) RETURNS bytea LANGUAGE sql AS 'SELECT NULL::bytea'",
            "ALTER FUNCTION public.digest(text,text) IMMUTABLE",
            "ALTER EXTENSION pgcrypto DROP FUNCTION public.digest(text,text)",
        ):
            await _require_installer_refusal(native_catalog, installer, statement)
        assert await _capture(native_catalog) == observed
        await transaction.rollback()
    assert await _capture(native_catalog) == before


async def test_trusted_extension_grafted_installer_owned_function_is_refused(native_catalog):
    before = await _capture(native_catalog)
    async with native_catalog.connection.begin_nested() as transaction:
        installer = await _install_trusted_extensions(native_catalog)
        connection = native_catalog.connection
        await connection.execute(text(f'GRANT USAGE,CREATE ON SCHEMA public TO "{installer}"'))
        await connection.execute(text(f'SET LOCAL ROLE "{installer}"'))
        await connection.execute(
            text("CREATE FUNCTION public.catalog_grafted() RETURNS integer LANGUAGE sql AS 'SELECT 1'")
        )
        await connection.execute(text("ALTER EXTENSION pgcrypto ADD FUNCTION public.catalog_grafted()"))
        await connection.execute(text("RESET ROLE"))
        await connection.execute(text(f'REVOKE USAGE,CREATE ON SCHEMA public FROM "{installer}"'))
        changed = await _capture(native_catalog)
        graft = next(row for row in changed["functions"] if row["proname"] == "catalog_grafted")
        assert graft["proowner"] != "10" and graft["prosecdef"] is False
        assert any(
            row["classid"] == "1255" and row["objid"] == graft["oid"] and row["deptype"] == "e"
            for row in changed["dependencies"]
        )
        with pytest.raises(catalog.RegistryExecutionCatalogError, match="function_provenance"):
            _verify(native_catalog, changed)
        await transaction.rollback()
    assert await _capture(native_catalog) == before


async def test_trusted_extension_body_and_lifecycle_changes_remain_observed(native_catalog):
    before = await _capture(native_catalog)
    async with native_catalog.connection.begin_nested() as transaction:
        await _install_trusted_extensions(native_catalog)
        installed = await _capture(native_catalog)
        assert _verify(native_catalog, installed)["contract"] == catalog.CONTRACT
        function_oid = await native_catalog.connection.scalar(
            text("SELECT 'public.digest(text,text)'::regprocedure::oid::text")
        )
        await native_catalog.connection.execute(
            text(
                "CREATE OR REPLACE FUNCTION public.digest(text,text) RETURNS bytea LANGUAGE sql AS 'SELECT NULL::bytea'"
            )
        )
        changed = await _capture(native_catalog)
        original = next(row for row in installed["functions"] if row["oid"] == function_oid)
        current = next(row for row in changed["functions"] if row["oid"] == original["oid"])
        assert current["proowner"] == original["proowner"] == "10"
        assert current["prosrc"] != original["prosrc"] and changed != installed
        await native_catalog.connection.execute(text("DROP EXTENSION pgcrypto"))
        removed = await _capture(native_catalog)
        assert removed != changed and all(row["extname"] != "pgcrypto" for row in removed["extensions"])
        await transaction.rollback()
    assert await _capture(native_catalog) == before


async def _unreviewed_public_view(fixture, name="catalog_private_view"):
    await fixture.connection.execute(text(f"CREATE VIEW public.{name} AS SELECT 1 AS value"))
    return await fixture.connection.scalar(text(f"SELECT 'public.{name}'::regclass::oid::text"))


def _view_reachability(observed, view_oid):
    return next(row for row in observed["public_view_reachability"]["views"] if row["oid"] == view_oid)


async def test_inaccessible_public_view_keeps_complete_metadata_and_transition_census(native_catalog):
    before = await _capture(native_catalog)
    async with native_catalog.connection.begin_nested() as transaction:
        view_oid = await _unreviewed_public_view(native_catalog)
        observed = await _capture(native_catalog)
        assert _view_reachability(observed, view_oid) == {"oid": view_oid, "no_privilege": True, "no_inbound": True}
        assert any(row["oid"] == view_oid for row in observed["relations"])
        assert any(row["ev_class"] == view_oid for row in observed["rules"])
        assert any(str(edge[1]) == view_oid for edge in observed["callback_edges"])
        assert _verify(native_catalog, observed)["contract"] == catalog.CONTRACT
        roles = native_catalog.roles_by_kind
        for parent, member, inherit, assume in (
            ("loading", "execution", True, True),
            ("execution", "keeper", True, True),
            ("custody", "keeper", False, True),
        ):
            await native_catalog.connection.execute(
                text(
                    f'GRANT "{roles[parent]}" TO "{roles[member]}" WITH INHERIT {str(inherit).upper()}, SET {str(assume).upper()}'
                )
            )
        assert await _capture(native_catalog) == observed
        await native_catalog.connection.execute(text(f'GRANT USAGE ON SCHEMA public TO "{roles["loading"]}"'))
        after_usage = await _capture(native_catalog)
        assert after_usage["public_view_reachability"] == observed["public_view_reachability"]
        assert _verify(native_catalog, after_usage)["contract"] == catalog.CONTRACT
        await transaction.rollback()
    assert await _capture(native_catalog) == before


async def _grant_public_view_access(fixture, access):
    connection = fixture.connection
    execution = fixture.roles_by_kind["execution"]
    if access in fixture.roles_by_kind:
        await connection.execute(
            text(f'GRANT SELECT ON public.catalog_private_view TO "{fixture.roles_by_kind[access]}"')
        )
    elif access == "column":
        await connection.execute(text(f'GRANT SELECT(value) ON public.catalog_private_view TO "{execution}"'))
    elif access == "public":
        await connection.execute(text("GRANT SELECT ON public.catalog_private_view TO PUBLIC"))
    else:
        group = "catalog_view_group_" + uuid4().hex[:12]
        await connection.execute(text(f'CREATE ROLE "{group}" NOLOGIN'))
        await connection.execute(text(f'GRANT SELECT ON public.catalog_private_view TO "{group}"'))
        inherit, assume = ("TRUE", "FALSE") if access == "inherited" else ("FALSE", "TRUE")
        await connection.execute(text(f'GRANT "{group}" TO "{execution}" WITH INHERIT {inherit}, SET {assume}'))


@pytest.mark.parametrize(
    "access", ["lifecycle", "execution", "loading", "custody", "keeper", "column", "public", "inherited", "set_only"]
)
async def test_public_view_rights_refuse_without_schema_usage(native_catalog, access):
    before = await _capture(native_catalog)
    async with native_catalog.connection.begin_nested() as transaction:
        view_oid = await _unreviewed_public_view(native_catalog)
        await _grant_public_view_access(native_catalog, access)
        execution = native_catalog.roles_by_kind["execution"]
        assert await native_catalog.connection.scalar(
            text("SELECT NOT has_schema_privilege(CAST(:role AS name),'public','USAGE')"), {"role": execution}
        )
        observed = await _capture(native_catalog)
        assert _view_reachability(observed, view_oid)["no_privilege"] is False
        if access == "set_only":
            assert await native_catalog.connection.scalar(
                text("SELECT NOT has_table_privilege(CAST(:role AS name),CAST(:oid AS oid),'SELECT')"),
                {"role": execution, "oid": int(view_oid)},
            )
            assert len(observed["public_view_reachability"]["principals"]) > 5
        with pytest.raises(catalog.RegistryExecutionCatalogError, match="public_relation_provenance"):
            _verify(native_catalog, observed)
        await transaction.rollback()
    assert await _capture(native_catalog) == before


async def test_public_view_external_return_rule_is_not_a_self_dependency(native_catalog):
    before = await _capture(native_catalog)
    async with native_catalog.connection.begin_nested() as transaction:
        view_oid = await _unreviewed_public_view(native_catalog)
        await native_catalog.connection.execute(
            text("CREATE VIEW public.catalog_outer_view AS SELECT value FROM public.catalog_private_view")
        )
        await native_catalog.connection.execute(text("ALTER EXTENSION intarray ADD VIEW public.catalog_outer_view"))
        await native_catalog.connection.execute(
            text(f'GRANT SELECT ON public.catalog_outer_view TO "{native_catalog.roles_by_kind["execution"]}"')
        )
        observed = await _capture(native_catalog)
        assert _view_reachability(observed, view_oid) == {"oid": view_oid, "no_privilege": True, "no_inbound": False}
        with pytest.raises(catalog.RegistryExecutionCatalogError, match="public_relation_provenance"):
            _verify(native_catalog, observed)
        await transaction.rollback()
    assert await _capture(native_catalog) == before


async def test_inaccessible_public_view_does_not_hide_unsafe_callbacks(native_catalog):
    before = await _capture(native_catalog)
    async with native_catalog.connection.begin_nested() as transaction:
        await native_catalog.connection.execute(
            text("CREATE FUNCTION public.catalog_view_callback() RETURNS integer LANGUAGE sql AS 'SELECT 1'")
        )
        await native_catalog.connection.execute(
            text("CREATE VIEW public.catalog_private_view AS SELECT public.catalog_view_callback() AS value")
        )
        observed = await _capture(native_catalog)
        assert observed["public_view_reachability"]["views"][-1]["no_privilege"] is True
        assert any(row["proname"] == "catalog_view_callback" for row in observed["functions"])
        with pytest.raises(catalog.RegistryExecutionCatalogError, match="function_provenance"):
            _verify(native_catalog, observed)
        await transaction.rollback()
    assert await _capture(native_catalog) == before


@pytest.mark.parametrize(
    "mutation",
    [
        "missing",
        "duplicate_worker",
        "missing_worker",
        "missing_principal",
        "duplicate_view",
        "non_boolean",
        "extra_field",
    ],
)
async def test_public_view_native_proof_requires_closed_complete_census(native_catalog, mutation):
    async with native_catalog.connection.begin_nested() as transaction:
        await _unreviewed_public_view(native_catalog)
        changed = deepcopy(await _capture(native_catalog))
        proof = changed["public_view_reachability"]
        match mutation:
            case "missing":
                del changed["public_view_reachability"]
            case "duplicate_worker":
                proof["workers"][-1] = proof["workers"][0]
            case "missing_worker":
                proof["workers"].pop()
            case "missing_principal":
                proof["principals"].remove(proof["workers"][0])
            case "duplicate_view":
                proof["views"].append(proof["views"][0])
            case "non_boolean":
                proof["views"][0]["no_privilege"] = 1
            case _:
                proof["unexpected"] = True
        with pytest.raises(catalog.RegistryExecutionCatalogError, match="public_view_(reachability|census)"):
            _verify(native_catalog, changed)
        await transaction.rollback()


async def _require_private_oracle(fixture, observed):
    """Compare a privately supplied historical SQL oracle without tracking a duplicate query."""
    path = os.getenv("NETWORK_REGISTRY_CATALOG_ORACLE_SQL")
    if path:
        value = await fixture.connection.scalar(text(Path(path).read_text()), fixture.parameters)
        assert (json.loads(value) if isinstance(value, str) else value) == observed


async def test_duplicate_subobject_edges_and_native_type_cycle_remain_exact(native_catalog):
    observed = await _capture(native_catalog)
    await _require_private_oracle(native_catalog, observed)
    edges = [tuple(map(int, row)) for row in observed["callback_edges"]]
    assert len(edges) == len(set(edges))
    duplicate = (
        await native_catalog.connection.execute(
            text(
                "SELECT d.classid::bigint,d.objid::bigint,d.refclassid::bigint,d.refobjid::bigint "
                "FROM pg_depend d JOIN pg_class c ON c.oid=d.objid "
                "WHERE d.classid='pg_class'::regclass AND c.relnamespace=CAST(:schema AS regnamespace) "
                "GROUP BY d.classid,d.objid,d.refclassid,d.refobjid HAVING count(*)>1 LIMIT 1"
            ),
            {"schema": native_catalog.schema},
        )
    ).one()
    assert edges.count(tuple(duplicate)) == 1
    # Native varchar/array type links are a genuine cycle; closure must terminate and retain both directions.
    assert (1247, 1043, 1247, 1015) in edges
    assert (1247, 1015, 1247, 1043) in edges
    assert observed["closure_count"] <= 10000 and observed["relation_count"] <= 512
    assert _verify(native_catalog, observed)["contract"] == catalog.CONTRACT


async def test_unrelated_native_catalog_growth_preserves_complete_certificate(native_catalog):
    before = await _capture(native_catalog)
    await _require_private_oracle(native_catalog, before)
    async with native_catalog.connection.begin_nested() as transaction:
        await native_catalog.connection.execute(text("CREATE SCHEMA catalog_unrelated_growth"))
        columns = ",".join(f"value_{index} integer" for index in range(32))
        for index in range(192):
            await native_catalog.connection.execute(
                text(f"CREATE TABLE catalog_unrelated_growth.member_{index}({columns})")
            )
        assert (
            await native_catalog.connection.scalar(
                text(
                    "SELECT count(*) FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid "
                    "WHERE c.relnamespace='catalog_unrelated_growth'::regnamespace AND a.attnum>0"
                )
            )
            == 6144
        )
        observed = await _capture(native_catalog)
        assert observed == before
        await _require_private_oracle(native_catalog, observed)
        assert _verify(native_catalog, observed)["contract"] == catalog.CONTRACT
        await transaction.rollback()
    assert await _capture(native_catalog) == before


@pytest_asyncio.fixture
async def retained_catalog_input(request, monkeypatch):
    """Keep every committed fixture object in one dedicated disposable database."""
    supplied = os.getenv("NETWORK_REGISTRY_TEST_DSN")
    if not supplied:
        pytest.skip("NETWORK_REGISTRY_TEST_DSN must select a disposable native service")
    source_url = make_url(supplied).set(drivername="postgresql")
    admin = await asyncpg.connect(source_url.render_as_string(hide_password=False))
    database = "retained_catalog_test_" + uuid4().hex
    selected = source_url.set(database=database).render_as_string(hide_password=False)
    source_fixture = None
    catalog_roles = []
    is_created = False
    try:
        assert await admin.fetchval(
            "SELECT rolsuper AND current_user=session_user FROM pg_roles WHERE rolname=current_user"
        )
        assert not await admin.fetchval("SELECT EXISTS(SELECT FROM pg_database WHERE datname=$1)", database)
        await admin.execute(f'CREATE DATABASE "{database}"')
        is_created = True
        monkeypatch.setenv("NETWORK_REGISTRY_TEST_DSN", selected)
        source_fixture = legacy_capture_db.__wrapped__(SimpleNamespace(param=request.param))
        capture_fixture = await anext(source_fixture)
        yield SimpleNamespace(capture_fixture=capture_fixture, dsn=selected, catalog_roles=catalog_roles)
    finally:
        try:
            if source_fixture is not None:
                await source_fixture.aclose()
        finally:
            if is_created:
                await admin.execute(f'DROP DATABASE "{database}"')
            for role in catalog_roles:
                await admin.execute(f'DROP ROLE IF EXISTS "{role}"')
            assert not await admin.fetchval("SELECT EXISTS(SELECT FROM pg_database WHERE datname=$1)", database)
            await admin.close()


@pytest.mark.asyncio(loop_scope="function")
@pytest.mark.parametrize("retained_catalog_input", ["legacy", "modern"], indirect=True)
async def test_retained_fhir_capture_is_certified_only_with_complete_native_custody(retained_catalog_input):
    """Use genuine capture and execution logins inside a dedicated native database."""
    from tests.test_network_fhir_retained_capture_postgres import _capture as capture_retained

    prepared = retained_catalog_input
    engine = create_async_engine(make_url(prepared.dsn).set(drivername="postgresql+asyncpg"))
    password = uuid4().hex + uuid4().hex
    try:
        async with engine.connect() as connection:
            async with connection.begin():
                fixture = await _catalog_fixture(connection, password)
                prepared.catalog_roles.extend(fixture.roles_by_kind.values())
            execution = fixture.roles_by_kind["execution"]
            capture = await capture_retained(
                prepared.capture_fixture,
                runtime_roles=tuple(sorted((execution, prepared.capture_fixture.role_by_kind["reader"]))),
            )
            async with connection.begin():
                observed = await _capture(fixture)
                assert _verify(fixture, observed)["contract"] == catalog.CONTRACT
                certificate = observed["retained_fhir_catalogs"][str(capture.schema_oid)]
                assert certificate["closed"] is True and len(certificate["catalog"]) == 5
                assert (
                    sum(len(json.loads(native_relation["indexes"])) for native_relation in certificate["catalog"]) == 9
                )
                await _retained_fhir_execution_read(prepared.dsn, execution, capture, password)
                await _retained_fhir_refusals(fixture, capture)
                await _retained_fhir_forged_receipts(fixture, capture)
                await _retained_fhir_unsupported_deparsers(fixture, capture)
                await _retained_fhir_deparser_tripwire(fixture, capture)
                await _retained_fhir_oversized_comment(fixture, capture)
    finally:
        await engine.dispose()


async def _retained_fhir_execution_read(supplied, execution, capture, password):
    from process.network_fhir_retained_capture import require_retained_fhir_capture

    connection = await asyncpg.connect(
        make_url(supplied)
        .set(drivername="postgresql", username=execution, password=password)
        .render_as_string(hide_password=False)
    )
    try:
        async with connection.transaction(isolation="repeatable_read", readonly=True):
            assert await connection.fetchval("SELECT session_user=current_user AND current_user=$1", execution)
            assert await require_retained_fhir_capture(connection, capture) == capture
    finally:
        await connection.close()


async def _retained_fhir_refusals(fixture, capture):
    schema, role = capture.schema_name, fixture.roles_by_kind["execution"]
    mutations = (
        f'GRANT CREATE ON SCHEMA "{schema}" TO "{role}"',
        f'GRANT USAGE ON SCHEMA "{schema}" TO PUBLIC',
        f'GRANT USAGE ON SCHEMA "{schema}" TO "{fixture.roles_by_kind["reader"]}"',
        f'GRANT SELECT ON ALL TABLES IN SCHEMA "{schema}" TO "{fixture.roles_by_kind["reader"]}"',
        f'GRANT SELECT ON ALL TABLES IN SCHEMA "{schema}" TO PUBLIC',
        f'GRANT SELECT ON ALL TABLES IN SCHEMA "{schema}" TO "{role}" WITH GRANT OPTION',
        f'GRANT UPDATE ON ALL TABLES IN SCHEMA "{schema}" TO "{role}"',
        f'GRANT "{capture.owner_role}" TO "{role}" WITH INHERIT FALSE, SET TRUE',
        f'ALTER DEFAULT PRIVILEGES IN SCHEMA "{schema}" GRANT SELECT ON TABLES TO PUBLIC',
        f'CREATE TABLE "{schema}".unexpected(value integer)',
        f'CREATE SEQUENCE "{schema}".unexpected',
        f'ALTER TABLE "{schema}".provider_directory_entity_source_binding ENABLE ROW LEVEL SECURITY',
        f'GRANT UPDATE(resource_id) ON "{schema}".provider_directory_entity_source_binding TO "{role}"',
        f'GRANT MAINTAIN ON ALL TABLES IN SCHEMA "{schema}" TO "{role}"',
        f'ALTER TABLE "{schema}".provider_directory_entity_source_binding ADD COLUMN unexpected integer',
        f'DROP INDEX "{schema}".retained_lookup_1_0',
        f"CREATE FUNCTION \"{schema}\".unexpected() RETURNS integer LANGUAGE sql AS 'SELECT 1'",
        f"COMMENT ON SCHEMA \"{schema}\" IS 'retained-normalized-fhir-v1:{{}}'",
    )
    for statement in mutations:
        transaction = await fixture.connection.begin_nested()
        try:
            await fixture.connection.execute(text(statement))
            with pytest.raises(ValueError):
                _verify(fixture, await _capture(fixture))
        finally:
            await transaction.rollback()
    assert _verify(fixture, await _capture(fixture))["contract"] == catalog.CONTRACT


async def _retained_fhir_forged_receipts(fixture, capture):
    from dataclasses import replace

    from process.network_fhir_retained_capture_contract import canonical, capture_digest

    for changes in (
        {"schema_oid": capture.schema_oid + 1},
        {"owner_role": fixture.roles_by_kind["queue"]},
        {"catalog_sha256": "0" * 64},
        {"relation_oids": tuple((name, oid + 1) for name, oid in capture.relation_oids)},
    ):
        changed = replace(capture, **changes)
        changed = replace(changed, capture_sha256=capture_digest(changed))
        document = "retained-normalized-fhir-v1:" + canonical(changed.as_dict())
        transaction = await fixture.connection.begin_nested()
        try:
            await fixture.connection.exec_driver_sql(
                f'COMMENT ON SCHEMA "{capture.schema_name}" IS ' + "'" + document.replace("'", "''") + "'"
            )
            with pytest.raises(ValueError):
                _verify(fixture, await _capture(fixture))
        finally:
            await transaction.rollback()
    assert _verify(fixture, await _capture(fixture))["contract"] == catalog.CONTRACT


async def _retained_fhir_deparser_tripwire(fixture, capture):
    """A harmless type-output trap must run only in the explicit unsafe oracle."""
    from sqlalchemy.exc import DBAPIError

    schema = capture.schema_name
    transaction = await fixture.connection.begin_nested()
    try:
        for statement in (
            f'CREATE TYPE "{schema}".output_tripwire',
            f'CREATE FUNCTION "{schema}".tripwire_in(cstring) RETURNS "{schema}".output_tripwire LANGUAGE internal IMMUTABLE AS \'int4in\'',
            f"CREATE FUNCTION \"{schema}\".tripwire_out(\"{schema}\".output_tripwire) RETURNS cstring LANGUAGE internal IMMUTABLE SECURITY DEFINER SET role='none' AS 'int4out'",
            f'CREATE TYPE "{schema}".output_tripwire (INPUT="{schema}".tripwire_in,OUTPUT="{schema}".tripwire_out,INTERNALLENGTH=4,PASSEDBYVALUE,ALIGNMENT=int4)',
            f'ALTER TABLE "{schema}".provider_directory_entity_source_binding ADD COLUMN tripwire "{schema}".output_tripwire',
            f'ALTER TABLE "{schema}".provider_directory_entity_source_binding ADD CONSTRAINT deparser_tripwire CHECK (coalesce(tripwire,\'1\'::"{schema}".output_tripwire) IS NOT NULL)',
        ):
            await fixture.connection.exec_driver_sql(statement)
        probe = await fixture.connection.begin_nested()
        try:
            with pytest.raises(DBAPIError, match="role"):
                await fixture.connection.exec_driver_sql(
                    f"SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conname='deparser_tripwire' AND conrelid='\"{schema}\".provider_directory_entity_source_binding'::regclass"
                )
        finally:
            await probe.rollback()
        observed = await _capture(fixture)
        native = observed["retained_fhir_catalogs"][str(capture.schema_oid)]["catalog"]
        assert all(index["definition"] is None for relation in native for index in json.loads(relation["indexes"]))
        assert all(key[3] is None for relation in native for key in json.loads(relation["constraints"]))
        with pytest.raises(ValueError):
            _verify(fixture, observed)
    finally:
        await transaction.rollback()


async def _retained_fhir_unsupported_deparsers(fixture, capture):
    schema = capture.schema_name
    for statements in (
        (
            f'ALTER TABLE "{schema}".provider_directory_entity_source_binding ADD CONSTRAINT unsupported_check CHECK (length(resource_id)>0) NOT VALID',
        ),
        (
            f'DROP INDEX "{schema}".retained_lookup_1_0',
            f'CREATE INDEX retained_lookup_1_0 ON "{schema}".provider_directory_dataset_resource (dataset_id,(lower(resource_type)),resource_id COLLATE "C")',
        ),
    ):
        transaction = await fixture.connection.begin_nested()
        try:
            for statement in statements:
                await fixture.connection.exec_driver_sql(statement)
            observed = await _capture(fixture)
            native = observed["retained_fhir_catalogs"][str(capture.schema_oid)]["catalog"]
            assert all(index["definition"] is None for relation in native for index in json.loads(relation["indexes"]))
            assert all(key[3] is None for relation in native for key in json.loads(relation["constraints"]))
            with pytest.raises(ValueError):
                _verify(fixture, observed)
        finally:
            await transaction.rollback()


async def _retained_fhir_oversized_comment(fixture, capture):
    document = "retained-normalized-fhir-v1:" + json.dumps(
        {"runtime_roles": list(capture.runtime_roles), "padding": "x" * 65537}
    )
    transaction = await fixture.connection.begin_nested()
    try:
        await fixture.connection.exec_driver_sql(
            f'COMMENT ON SCHEMA "{capture.schema_name}" IS ' + "'" + document + "'"
        )
        observed = await _capture(fixture)
        assert observed["retained_fhir_catalogs"][str(capture.schema_oid)]["closed"] is False
        with pytest.raises(ValueError):
            _verify(fixture, observed)
    finally:
        await transaction.rollback()
