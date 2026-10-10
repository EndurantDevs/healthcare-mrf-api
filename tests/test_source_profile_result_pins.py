# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Closed statement guards preserve model keys and historical pin contracts."""

import hashlib
import importlib.util
import json
from io import StringIO
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import PrimaryKeyConstraint, UniqueConstraint

from process import source_profile_result_archive as archive
from process import source_profile_result_pins as pins


def role_policy():
    return {
        "owner_role": "synthetic_namespace_owner",
        "migration_role": "synthetic_migration",
        "preparation_owner_role": "synthetic_snapshot_owner",
        "runtime_roles": ["synthetic_worker", "synthetic_reader"],
    }


@pytest.fixture
def policy_file(monkeypatch, tmp_path):
    filename = tmp_path / "roles.json"
    filename.write_text(json.dumps(role_policy()))
    monkeypatch.setenv(pins.ROLE_POLICY_ENVIRONMENT, str(filename))
    return filename


@pytest.mark.parametrize(
    "fault", [None, "missing", "duplicate", "overflow", "extra", "no_owner", "empty", "overlap", "control"]
)
def test_role_policy_is_independent_closed_and_bounded(policy_file, fault):
    policy = role_policy()
    if fault == "missing":
        policy_file.unlink()
    elif fault == "duplicate":
        policy_file.write_text('{"runtime_roles": [], "runtime_roles": []}')
    elif fault == "overflow":
        policy_file.write_bytes(b" " * 8193)
    else:
        if fault == "extra":
            policy["receipt"] = {"runtime_roles": ["synthetic_worker"]}
        if fault == "no_owner":
            del policy["preparation_owner_role"]
        if fault == "empty":
            policy["runtime_roles"] = []
        if fault == "overlap":
            policy["runtime_roles"].append(policy["preparation_owner_role"])
        if fault == "control":
            policy["control_runtime_roles"] = ["unconfigured_worker"]
        policy_file.write_text(json.dumps(policy))
    if fault is None:
        assert pins.load_role_policy() == ("synthetic_snapshot_owner", ("synthetic_worker", "synthetic_reader"))
    else:
        with pytest.raises(ValueError, match="role policy is unavailable or invalid"):
            pins.load_role_policy()


def test_role_policy_accepts_the_existing_optional_fields_at_the_byte_limit(policy_file):
    policy_by_field = {
        **role_policy(),
        "credential_broker_role": "synthetic_broker",
        "catalog_pin_role": "synthetic_pin",
        "control_runtime_roles": ["synthetic_worker"],
        "preparation_owner_schema_create": True,
    }
    policy_file.write_bytes(json.dumps(policy_by_field).encode().ljust(8192, b" "))
    assert pins.load_role_policy() == ("synthetic_snapshot_owner", ("synthetic_worker", "synthetic_reader"))


def role_closure():
    return [
        {
            "role_oid": oid,
            "principal_oid": oid,
            "owner_member": False,
            "database_owner_member": False,
            "elevated_member": False,
        }
        for oid in (42, 43)
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault",
    [None, "owner", "missing_role", "owner_member", "database_owner_member", "elevated_member", "closure_bound"],
)
async def test_role_closure_checks_every_configured_principal(fault):
    owner_by_field = dict.fromkeys(
        ("rolcanlogin", "rolsuper", "rolcreaterole", "rolcreatedb", "rolreplication", "rolbypassrls"), False
    )
    memberships = role_closure()
    if fault == "owner":
        owner_by_field["rolcanlogin"] = True
    if fault == "missing_role":
        memberships.pop()
    if fault in {"owner_member", "database_owner_member", "elevated_member"}:
        memberships[-1][fault] = True
    if fault == "closure_bound":
        memberships.extend({**memberships[0], "principal_oid": oid} for oid in range(100, 165))
    connection = SimpleNamespace(
        fetchrow=AsyncMock(return_value=owner_by_field), fetch=AsyncMock(return_value=memberships)
    )
    if fault is None:
        assert await pins.require_role_principals(connection, 41, [42, 43]) == [42, 43]
        query, owner_oid, runtime_oids = connection.fetch.await_args.args
        assert "'MEMBER'" in query and "LIMIT 1025" in query
        assert (owner_oid, runtime_oids) == (41, [42, 43])
    else:
        with pytest.raises(ValueError, match="snapshot_generation_owner_unprotected|snapshot_runtime_role_unprotected"):
            await pins.require_role_principals(connection, 41, [42, 43])


@pytest.mark.asyncio
@pytest.mark.parametrize("scope", [[], [True], [0], [2**32], [43, 42], [42, 42], list(range(1, 18))])
async def test_role_scope_is_bounded_before_catalog_work(scope):
    connection = SimpleNamespace(fetchrow=AsyncMock())
    with pytest.raises(ValueError, match="snapshot_runtime_role_unprotected"):
        await pins.require_role_principals(connection, 41, scope)
    connection.fetchrow.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("owner_oid", [None, True, 0, 2**32])
async def test_missing_or_invalid_owner_fails_before_catalog_work(owner_oid):
    connection = SimpleNamespace(fetchrow=AsyncMock())
    with pytest.raises(ValueError, match="snapshot_generation_owner_unprotected"):
        await pins.require_role_principals(connection, owner_oid, [42, 43])
    connection.fetchrow.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", [None, "owner", "rls", "force_rls", "unsafe", "payload_rls", "missing"])
async def test_pin_table_authority_checks_owners_rls_and_inherited_privileges(fault):
    relations = [
        {
            "relname": name,
            "relowner": 41,
            "relkind": "r",
            "relpersistence": "p",
            "relispartition": False,
            "relrowsecurity": name == pins.TABLE,
            "relforcerowsecurity": name == pins.TABLE,
            "unsafe": False,
        }
        for name in (*pins.PAYLOAD_TABLES, pins.TABLE)
    ]
    if fault == "missing":
        relations.pop()
    elif fault == "payload_rls":
        relations[0]["relrowsecurity"] = True
    elif fault is not None:
        field, changed = {
            "owner": ("relowner", 42),
            "rls": ("relrowsecurity", False),
            "force_rls": ("relforcerowsecurity", False),
            "unsafe": ("unsafe", True),
        }[fault]
        relations[-1][field] = changed
    connection = SimpleNamespace(fetch=AsyncMock(return_value=relations))
    if fault is None:
        await pins.require_pin_tables(connection, "mrf", 41, [42, 43])
        query, schema, principals, names = connection.fetch.await_args.args
        assert schema == "mrf" and principals == [42, 43] and len(names) == 5
        assert "TRIGGER,MAINTAIN" in query and "'TRUNCATE'" in query and "'MEMBER'" in query
    else:
        with pytest.raises(ValueError, match="ownership is unprotected"):
            await pins.require_pin_tables(connection, "mrf", 41, [42, 43])


def pin_policies():
    owner = "( SELECT pg_class.relowner\n   FROM pg_class\n  WHERE (pg_class.oid = ('mrf.provider_profile_source_pin'::regclass)::oid))"
    adoption = f"(((purpose)::text = 'adoption'::text) AND (CURRENT_USER <> pg_get_userbyid({owner})) AND pg_has_role(CURRENT_USER, {owner}, 'USAGE'::text))"
    export = "((purpose)::text = 'export'::text)"
    fields = ("polname", "polcmd", "polpermissive", "polroles", "qual", "check_expr")
    return [
        dict(zip(fields, values, strict=True))
        for values in (
            ("source_profile_pin_adoption", b"*", True, [0], adoption, adoption),
            ("source_profile_pin_export", b"*", True, [0], export, export),
            ("source_profile_pin_read", b"r", True, [0], "true", None),
        )
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault", [None, "missing", "extra", "qual", "check_expr", "polroles", "polpermissive", "polcmd"]
)
async def test_pin_policies_require_the_exact_migration_contract(fault):
    policies = pin_policies()
    if fault == "missing":
        policies.pop()
    elif fault == "extra":
        policies.append(dict(policies[0]))
    elif fault is not None:
        policies[0][fault] = {
            "qual": "true",
            "check_expr": "true",
            "polroles": [42],
            "polpermissive": False,
            "polcmd": b"r",
        }[fault]
    connection = SimpleNamespace(fetch=AsyncMock(return_value=policies))
    if fault is None:
        await pins.require_pin_policies(connection, "mrf")
        assert connection.fetch.await_args.args[1] == '"mrf".provider_profile_source_pin'
    else:
        with pytest.raises(ValueError, match="pin policy differs"):
            await pins.require_pin_policies(connection, "mrf")


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", [None, "owner", "security", "configuration", "volatility", "body", "missing"])
async def test_pin_functions_require_every_original_invoker_definition(fault):
    async def fetchrow(_query, name):
        definition = next(
            statement
            for statement in pins.statement_pin_guard_statements("mrf")
            if statement.startswith("CREATE OR REPLACE FUNCTION " + name + " RETURNS")
        )
        function_by_field = {
            "proowner": 41,
            "prosecdef": False,
            "proconfig": ["search_path=pg_catalog, pg_temp"],
            "provolatile": "v",
            "prosrc": definition.split("AS $$", 1)[1].rsplit("$$", 1)[0],
        }
        if fault == "missing":
            return None
        if fault is not None:
            field, value = {
                "owner": ("proowner", 42),
                "security": ("prosecdef", True),
                "configuration": ("proconfig", None),
                "volatility": ("provolatile", "s"),
                "body": ("prosrc", "changed"),
            }[fault]
            function_by_field[field] = value
        return function_by_field

    connection = SimpleNamespace(fetchrow=AsyncMock(side_effect=fetchrow))
    if fault is None:
        await pins.require_pin_functions(connection, "mrf", 41)
        assert connection.fetchrow.await_count == 3
    else:
        with pytest.raises(ValueError, match="guard authority differs"):
            await pins.require_pin_functions(connection, "mrf", 41)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault", [None, "assumed", "unknown_worker", "replica", "other_owner", "omitted_role", "missing_policy"]
)
async def test_worker_authority_uses_policy_not_receipt_or_current_role_alone(monkeypatch, policy_file, fault):
    identity_by_field = {"session": "synthetic_worker", "current": "synthetic_worker", "replication": "origin"}
    if fault == "assumed":
        identity_by_field["session"] = "synthetic_admin"
    if fault == "unknown_worker":
        identity_by_field["session"] = identity_by_field["current"] = "unconfigured_worker"
    if fault == "replica":
        identity_by_field["replication"] = "replica"
    if fault == "missing_policy":
        policy_file.unlink()
    driver = SimpleNamespace(
        fetchval=AsyncMock(return_value=40 if fault == "other_owner" else 41),
        fetch=AsyncMock(return_value=[{"oid": 42}] if fault == "omitted_role" else [{"oid": 43}, {"oid": 42}]),
        fetchrow=AsyncMock(return_value=identity_by_field),
    )
    connection = SimpleNamespace(get_raw_connection=AsyncMock(return_value=SimpleNamespace(driver_connection=driver)))
    session = SimpleNamespace(
        in_transaction=lambda: True, execute=AsyncMock(), connection=AsyncMock(return_value=connection)
    )
    principals = AsyncMock(return_value=[42, 43, 44])
    monkeypatch.setattr(pins, "require_role_principals", principals)
    checks = [AsyncMock() for _ in range(3)]
    for name, check in zip(
        ("require_pin_tables", "require_pin_policies", "require_pin_functions"), checks, strict=True
    ):
        monkeypatch.setattr(pins, name, check)
    if fault is None:
        await pins.require_worker_authority(session, "mrf", 41)
        principals.assert_awaited_once_with(driver, 41, [42, 43])
        checks[0].assert_awaited_once_with(driver, "mrf", 41, [42, 43, 44])
        checks[1].assert_awaited_once_with(driver, "mrf")
        checks[2].assert_awaited_once_with(driver, "mrf", 41)
    else:
        with pytest.raises(ValueError):
            await pins.require_worker_authority(session, "mrf", 41)
        assert all(check.await_count == 0 for check in checks)


def test_historical_pin_statements_keep_their_original_digest():
    assert hashlib.sha256("\n".join(pins.pin_guard_statements("fixture")).encode()).hexdigest() == (
        "d8c97782387576c62ac6e45a391a7c1d057f2bb8592531171ba8ec9ee0f389f4"
    )


def test_attached_collision_keys_cover_each_installed_model_unique_constraint():
    expected_keys_by_table = {
        model.__tablename__: sorted(
            tuple(constraint.columns.keys())
            for constraint in model.__table__.constraints
            if isinstance(constraint, (PrimaryKeyConstraint, UniqueConstraint))
        )
        for model in archive.MODELS
    }
    assert {name: sorted(keys) for name, keys in pins._UNIQUE_KEYS_BY_TABLE.items()} == expected_keys_by_table
    statements = "\n".join(pins.statement_pin_guard_statements("fixture"))
    assert "FOR EACH ROW" not in statements
    assert statements.count("FOR EACH STATEMENT") == 19
    assert statements.count("stored.tableoid<>TG_RELID") == sum(map(len, expected_keys_by_table.values()))
    assert "FROM profile_guard_new incoming" in statements
    assert "EXISTS(SELECT 1 FROM pg_catalog.pg_inherits WHERE inhparent=TG_RELID)" in statements
    assert "source profile attached key conflicts" in statements
    assert "hashtext('profile-run-seal:'" in statements
    assert "md5(" not in statements and "to_jsonb(" not in statements


def test_attachment_seal_guard_uses_old_metadata_and_exact_child_oids():
    statements = tuple(pins.statement_pin_guard_statements("fixture"))
    function = next(
        statement for statement in statements if 'FUNCTION "fixture".provider_profile_attached_pin_guard' in statement
    )
    assert "SET search_path = pg_catalog, pg_temp" in function
    assert "SECURITY DEFINER" not in function and " STABLE" not in function
    assert "FROM profile_pin_old" in function
    assert "authority_json::jsonb->'created_here'='true'::jsonb" in function
    assert "source-profile-attachment.v2" in function
    assert "inherited.inhrelid::text=child.value->>2" in function
    assert function.index("require read committed") < function.index("pg_catalog.pg_inherits")
    assert any(
        "BEFORE TRUNCATE" in statement and '"fixture".provider_profile_source_pin' in statement
        for statement in statements
    )
    assert any("OLD TABLE AS profile_pin_old NEW TABLE AS profile_pin_new" in statement for statement in statements)


@pytest.mark.parametrize("action", ("upgrade", "downgrade"))
def test_statement_migration_compiles_complete_offline_checks(monkeypatch, action):
    path = Path(__file__).resolve().parents[1] / "alembic/versions/20261005030000_source_profile_statement_pins.py"
    spec = importlib.util.spec_from_file_location("statement_pin_migration", path)
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    output = StringIO()
    context = MigrationContext.configure(url="postgresql://", opts={"as_sql": True, "output_buffer": output})
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "fixture")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    with Operations.context(context):
        getattr(migration, action)()
    sql = output.getvalue()
    assert '"fixture".provider_profile_source_pin IN ACCESS EXCLUSIVE MODE' in sql
    if action == "upgrade":
        assert sql.index("DO $preflight$") < sql.index('FUNCTION "fixture".provider_profile_pinned_run_guard')
        assert "source profile historical pin guard missing' USING ERRCODE='55000'" in sql
        assert "INTO STRICT guard_owner" in sql
        assert "namespace.nspname='fixture'" in sql
        assert "'fixture', guard_owner" in sql
        assert sql.index('FUNCTION "fixture".provider_profile_attached_pin_guard') < sql.index("DO $owner$")
    else:
        assert "pg_catalog.pg_inherits" in sql
        assert "namespace.nspname='fixture'" in sql
        guard = "RAISE EXCEPTION 'source profile attached tables remain' USING ERRCODE='55000'"
        assert sql.index(guard) < sql.index("DROP TRIGGER provider_profile_attached_pin_guard_")
        assert 'DROP FUNCTION "fixture".provider_profile_attached_pin_guard() RESTRICT' in sql
        assert sql.count("DROP TRIGGER provider_profile_attached_pin_guard_") == 3
        assert "FOR EACH ROW" in sql
