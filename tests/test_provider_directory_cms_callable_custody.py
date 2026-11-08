# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Existing callable custody preserves SQL while rejecting executable drift."""

import asyncio
import importlib.util
import json
import re
from contextlib import asynccontextmanager
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_cms_callable_custody as custody
from process.provider_directory_generic_source_policy import GenericSourcePolicy


def _migration_functions():
    path = Path(__file__).parents[1] / "alembic/versions/20260930100000_cms_npd_serving_receipt.py"
    spec = importlib.util.spec_from_file_location("callable_custody_migration", path)
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    statements = []
    migration.op = SimpleNamespace(execute=statements.append)
    for name in (
        "_create_native_readers",
        "_create_table",
        "_create_archive_check",
        "_create_result_check",
        "_create_current_check",
        "_create_snapshot_check",
        "_create_insert_guard",
        "_create_fresh_guard",
        "_create_native_transition_guards",
        "_create_source_transition_guards",
        "_create_truncate_guards",
    ):
        getattr(migration, name)('"snapshot_schema"')
    function_by_name = {}
    for statement in statements:
        match = re.search(
            r'CREATE FUNCTION "snapshot_schema"\.(cms_serving_\w+)\((.*?)\) RETURNS (\w+)\s+'
            r"LANGUAGE (\w+)(.*?)AS \$\$(.*)\$\$",
            statement,
            re.S,
        )
        if match:
            name, arguments, returns, language, options, body = match.groups()
            args = tuple(tuple(arg.strip().split()) for arg in arguments.split(",")) if arguments else ()
            function_by_name[name] = (args, returns, language, "s" if "STABLE" in options else "v", body)
    repair_path = path.with_name("20261016030000_cms_source_guard_argument_types.py")
    repair_spec = importlib.util.spec_from_file_location("callable_custody_argument_types", repair_path)
    repair = importlib.util.module_from_spec(repair_spec)
    repair_spec.loader.exec_module(repair)
    old_call, new_call = repair._calls("snapshot_schema")
    for name, previous_hash, next_hash in repair._CONTRACTS:
        *attributes, body = function_by_name[name]
        assert custody._body_digest(body, "snapshot_schema") == previous_hash
        assert body.count(old_call) == 1
        replacement = body.replace(old_call, new_call)
        assert custody._body_digest(replacement, "snapshot_schema") == next_hash
        function_by_name[name] = (*attributes, replacement)
    _repair_archive_body(path, function_by_name)
    return function_by_name


def _repair_archive_body(path, function_by_name):
    archive_path = path.with_name("20261019130000_cms_archive_transaction_guard.py")
    archive_spec = importlib.util.spec_from_file_location("archive_transaction_guard", archive_path)
    archive_guard = importlib.util.module_from_spec(archive_spec)
    archive_spec.loader.exec_module(archive_guard)
    name = "cms_serving_archive_result_matches"
    *attributes, body = function_by_name[name]
    contract = next(item for item in archive_guard._CONTRACTS if item[0] == name)
    assert custody._body_digest(body, "snapshot_schema") == contract[1]
    for previous, current in archive_guard._replacements("snapshot_schema")[name]:
        assert body.count(previous) == 1
        body = body.replace(previous, current)
    assert custody._body_digest(body, "snapshot_schema") == contract[2]
    function_by_name[name] = (*attributes, body)


def _definition(name, spec, body, oid, owner):
    arguments, returns, _language, volatility, _digest = spec
    return dict(
        oid=str(oid),
        pronamespace="42",
        proname=name,
        proowner=str(owner),
        proacl=None,
        prokind="f",
        prorettype=str(custody._TYPE_OIDS[returns]),
        pronargs=len(arguments),
        proargtypes=[str(custody._TYPE_OIDS[kind]) for _, kind in arguments],
        proargnames=[name for name, _ in arguments] or None,
        proargmodes=None,
        proallargtypes=None,
        proargdefaults=None,
        pronargdefaults=0,
        provariadic="0",
        prosecdef=False,
        proleakproof=False,
        proisstrict=False,
        proretset=False,
        prosupport="-",
        probin=None,
        prosqlbody=None,
        protrftypes=None,
        procost=100,
        prorows=0,
        provolatile=volatility,
        proparallel="u",
        proconfig=["search_path=pg_catalog"],
        prosrc=body,
        native_extra="unchanged",
    )


def _catalog_function_rows(policy):
    bodies = _migration_functions()
    function_rows = []
    for oid, (name, spec) in enumerate(custody._CONTRACTS.items(), 100):
        owner = 19
        function_rows.append(
            dict(
                signature=custody._signatures(policy.schema)[len(function_rows)],
                language=spec[2],
                definition=_definition(name, spec, bodies[name][-1], oid, owner),
                acl=[[0, owner, "EXECUTE", False], [owner, owner, "EXECUTE", False]],
                access=[
                    [10, True, False, True, True, True],
                    [17, False, False, False, False, True],
                    [18, False, True, True, True, True],
                    [19, False, True, True, True, True],
                    [20, False, True, False, False, True],
                ],
            )
        )
    return function_rows


@pytest.fixture
def catalog(monkeypatch):
    policy = GenericSourcePolicy(
        "snapshot_schema", "source_owner", "source_publisher", ("legacy_builder", "source_reader"), ("legacy_builder",)
    )
    graph_by_field = dict(
        roles=[
            dict(oid=oid, rolname=name)
            for oid, name in (
                (10, "native_admin"),
                (17, "source_owner"),
                (18, "source_publisher"),
                (19, "legacy_builder"),
                (20, "source_reader"),
            )
        ],
        memberships=[],
    )
    function_rows = _catalog_function_rows(policy)
    events = []

    @asynccontextmanager
    async def transaction():
        original = deepcopy(function_rows)
        try:
            yield
        except BaseException:
            function_rows[:] = original
            events.append("rollback")
            raise

    async def execute(sql):
        if sql.startswith("ALTER FUNCTION"):
            row = next(row for row in function_rows if row["signature"] in sql)
            row["definition"]["proowner"] = "17"
            row["acl"] = [[0, 17, "EXECUTE", False], [17, 17, "EXECUTE", False]]
            for access in row["access"]:
                access[3] = access[4] = access[0] in {10, 17, 18, 21}

    driver = SimpleNamespace(
        is_in_transaction=lambda: True,
        transaction=transaction,
        fetch=AsyncMock(side_effect=lambda *_: deepcopy(function_rows)),
        execute=AsyncMock(side_effect=execute),
        fetchval=AsyncMock(
            side_effect=lambda sql, *_: deepcopy(graph_by_field) if sql == custody._ROLE_GRAPH_SQL else 42
        ),
        fetchrow=AsyncMock(
            return_value=dict(session="native_admin", current="native_admin", backend=7, xid="9", native_admin=True)
        ),
    )
    monkeypatch.setattr(custody, "storage_roles", AsyncMock(return_value=(17, [19, 20])))
    return SimpleNamespace(policy=policy, driver=driver, rows=function_rows, graph=graph_by_field, events=events)


def test_exact_sixteen_contracts_match_current_authentic_migration_chain():
    functions = _migration_functions()
    assert functions.keys() == custody._CONTRACTS.keys() and len(functions) == 16
    for name, (arguments, returns, language, volatility, body) in functions.items():
        assert custody._CONTRACTS[name] == (
            arguments,
            returns,
            language,
            volatility,
            custody._body_digest(body, "snapshot_schema"),
        )
    versions = Path(__file__).parents[1] / "alembic/versions"
    for path in versions.glob("*.py"):
        if path.name > "20260930100000_cms_npd_serving_receipt.py":
            source = path.read_text()
            assert not ("cms_serving_" in source and re.search(r"(?:CREATE|ALTER).*FUNCTION", source)), path.name


@pytest.mark.asyncio
async def test_real_capture_is_default_execute_only_and_raw_json_compatible(catalog):
    captured = await custody.capture_cms_callable_custody(catalog.driver, catalog.policy, legacy=True)
    assert len(captured["functions"]) == 16
    for row in catalog.rows:
        for key in ("definition", "acl", "access"):
            row[key] = json.dumps(row[key])
    assert await custody.capture_cms_callable_custody(catalog.driver, catalog.policy, legacy=True) == captured


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mutation", ["body", "settings", "secdef", "volatility", "arguments", "language", "foreign_owner", "extra_acl"]
)
async def test_preexisting_drift_never_reaches_owner_transfer(catalog, mutation):
    row = catalog.rows[0]
    match mutation:
        case "body":
            row["definition"]["prosrc"] += " SELECT 1"
        case "settings":
            row["definition"]["proconfig"] = ["search_path=public"]
        case "secdef":
            row["definition"]["prosecdef"] = True
        case "volatility":
            row["definition"]["provolatile"] = "i"
        case "arguments":
            row["definition"]["proargdefaults"] = "changed"
        case "language":
            row["language"] = "custom_language"
        case "foreign_owner":
            row["definition"]["proowner"] = "20"
            row["acl"] = [[0, 20, "EXECUTE", False], [20, 20, "EXECUTE", False]]
        case _:
            row["acl"].append([20, 19, "EXECUTE", False])
    with pytest.raises(ValueError, match="cms_callable_.*changed"):
        await custody.provision_cms_callable_custody(catalog.driver, catalog.policy, verify_role_profile=AsyncMock())
    catalog.driver.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_only_sixteen_owner_changes_preserve_every_native_field_and_ordinary_execution(catalog):
    before = await custody.capture_cms_callable_custody(catalog.driver, catalog.policy, legacy=True)
    verify = AsyncMock()
    after = await custody.provision_cms_callable_custody(catalog.driver, catalog.policy, verify_role_profile=verify)
    custody.require_cms_callable_adoption_delta(before, after)
    assert verify.await_count == 2
    statements = [call.args[0] for call in catalog.driver.execute.await_args_list]
    assert len(statements) == 16 and all(sql.startswith("ALTER FUNCTION") for sql in statements)
    assert not any(word in sql for sql in statements for word in ("CREATE", "REVOKE", "GRANT", "DROP"))
    catalog.driver.execute.reset_mock()
    assert (
        await custody.provision_cms_callable_custody(catalog.driver, catalog.policy, verify_role_profile=AsyncMock())
        == after
    )
    catalog.driver.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_unexpected_ordinary_replacement_reach_refuses_protected_capture(catalog):
    await custody.provision_cms_callable_custody(catalog.driver, catalog.policy, verify_role_profile=AsyncMock())
    catalog.rows[0]["access"][-1][3] = True
    with pytest.raises(ValueError, match="replacement_authority"):
        await custody.capture_cms_callable_custody(catalog.driver, catalog.policy)


@pytest.mark.asyncio
@pytest.mark.parametrize("mutation", ["body", "oid", "extra_native_field", "role_graph", "execute", "raw_acl"])
async def test_native_delta_cannot_baseline_postcut_substitution(catalog, mutation):
    before = await custody.capture_cms_callable_custody(catalog.driver, catalog.policy, legacy=True)
    after = await custody.provision_cms_callable_custody(
        catalog.driver, catalog.policy, verify_role_profile=AsyncMock()
    )
    row = next(iter(after["functions"].values()))
    match mutation:
        case "body":
            row["definition"]["prosrc"] += " altered"
        case "oid":
            row["definition"]["oid"] = "999"
        case "extra_native_field":
            row["definition"]["native_extra"] = "changed"
        case "role_graph":
            after["role_graph"]["memberships"].append({"member": 20, "roleid": 17})
        case "execute":
            row["access"][-1][-1] = False
        case _:
            row["definition"]["proacl"] = ["unreviewed representation"]
    with pytest.raises(ValueError, match="cms_callable_.*changed"):
        custody.require_cms_callable_adoption_delta(before, after)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [RuntimeError("late_failure"), asyncio.CancelledError()])
async def test_caught_late_failure_and_cancel_restore_all_sixteen_owners(catalog, failure):
    before = deepcopy(catalog.rows)
    with pytest.raises(type(failure)):
        await custody.provision_cms_callable_custody(
            catalog.driver, catalog.policy, verify_role_profile=AsyncMock(side_effect=[None, failure])
        )
    assert catalog.rows == before and catalog.events == ["rollback"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "actor",
    [
        ("source_publisher", "source_publisher", False),
        ("native_admin", "source_owner", True),
        ("source_owner", "source_owner", True),
    ],
)
async def test_no_publisher_or_role_substitution_can_run_the_admin_cut(catalog, actor):
    catalog.driver.fetchrow.return_value.update(session=actor[0], current=actor[1], native_admin=actor[2])
    verify = AsyncMock()
    with pytest.raises(ValueError, match="independent_admin_required"):
        await custody.provision_cms_callable_custody(catalog.driver, catalog.policy, verify_role_profile=verify)
    verify.assert_not_awaited()


@pytest.mark.asyncio
async def test_extra_overload_cannot_redirect_unknown_typed_native_calls(catalog):
    overload = deepcopy(catalog.rows[0])
    overload["signature"] = None
    overload["definition"]["oid"] = "999"
    catalog.rows.append(overload)
    with pytest.raises(ValueError, match="function_set_changed"):
        await custody.provision_cms_callable_custody(catalog.driver, catalog.policy, verify_role_profile=AsyncMock())
    catalog.driver.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_enrolled_privileged_migrator_is_not_misclassified_as_ordinary_builder(catalog):
    catalog.graph["roles"].append(dict(oid=21, rolname="migration_actor"))
    for row in catalog.rows:
        row["access"].append([21, False, False, False, False, True])
    after = await custody.provision_cms_callable_custody(
        catalog.driver, catalog.policy, verify_role_profile=AsyncMock()
    )
    # Privileged migration membership remains independently bound by the full profile.
    assert await custody.capture_cms_callable_custody(catalog.driver, catalog.policy) == after
    assert after["role_graph"] == catalog.graph
