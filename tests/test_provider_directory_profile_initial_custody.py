# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Five-callable component contract tests; simulated catalog, no native/profile claim."""

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

from process import provider_directory_profile_initial_custody as custody
from process.provider_directory_generic_source_policy import GenericSourcePolicy


def _migration_functions():
    path = Path(__file__).parents[1] / "alembic/versions/20261001110000_profile_initial_publication.py"
    spec = importlib.util.spec_from_file_location("initial_custody_authentic_migration", path)
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    statements = []
    migration.op = SimpleNamespace(execute=statements.append)
    migration._create_receipt("snapshot_schema")
    migration._create_deferred_guards("snapshot_schema")
    function_by_name = {}
    for statement in statements:
        found = re.search(
            r'CREATE FUNCTION "snapshot_schema"\."(pd_profile_initial_\w+)"\((.*?)\) RETURNS (\w+) '
            r"LANGUAGE (\w+).*?AS \$\$(.*)\$\$",
            statement,
            re.S,
        )
        if found:
            name, arguments, returns, language, body = found.groups()
            function_by_name[name] = dict(arguments=arguments, returns=returns, language=language, body=body)
    return function_by_name


def _definition(name, spec, oid, owner):
    return dict(
        oid=str(oid),
        pronamespace="42",
        proname=name,
        proowner=str(owner),
        proacl=None if name == custody.INITIAL_CALLABLES[-1] else ["owner=EXECUTE"],
        prokind="f",
        prorettype=spec["returns"],
        pronargs=len(spec["arguments"]),
        proargtypes=spec["arguments"],
        proargnames=spec["argument_names"],
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
        provolatile="v",
        proparallel="u",
        proconfig=["search_path=pg_catalog"],
        prosrc=spec["body"],
        native_extra="unchanged",
    )


def _function_rows(policy):
    rows = []
    for oid, (name, spec) in enumerate(custody._specifications(policy.schema, 70).items(), 100):
        is_public = name == custody.INITIAL_CALLABLES[-1]
        rows.append(
            dict(
                signature=custody._signatures(policy.schema)[len(rows)],
                language=spec["language"],
                definition=_definition(name, spec, oid, 19),
                acl=[[19, 19, "EXECUTE", False]] + ([[0, 19, "EXECUTE", False]] if is_public else []),
                access=[
                    [10, True, False, True, True, True],
                    [17, False, False, False, False, is_public],
                    [18, False, False, True, True, is_public],
                    [19, False, True, True, True, True],
                    [20, False, True, False, False, is_public],
                    [21, False, False, False, False, is_public],
                    [22, False, False, False, False, is_public],
                ],
            )
        )
    return rows


def _relation_rows():
    return [
        dict(
            relname=name,
            relation=dict(
                oid=str(oid),
                reltype=str(oid + 10),
                relnamespace="42",
                relkind="r",
                relpersistence="p",
                relrowsecurity=False,
                relforcerowsecurity=False,
                relowner="19",
            ),
            composite=dict(oid=str(oid + 10), typrelid=str(oid), typnamespace="42", typtype="c"),
            attributes=[],
            defaults=[],
            indexes=[],
            constraints=[],
            triggers=[],
            inherited=False,
        )
        for name, oid in zip(custody._RELATIONS, (60, 61), strict=True)
    ]


def _trigger_rows():
    return [
        dict(
            tgname=name,
            tgfoid=str(oid),
            tgrelid=str(60 if oid < 103 else 61),
            tgtype=mask,
            tgenabled="A",
            tgdeferrable=deferred,
            tginitdeferred=deferred,
            tgisinternal=False,
            tgnargs=0,
            columns="",
            arguments="",
            no_condition=True,
            tgoldtable=None,
            tgnewtable=None,
        )
        for name, oid, mask, deferred in zip(
            custody.INITIAL_CALLABLES[:4], range(100, 104), (26, 34, 5, 21), (False, False, True, True), strict=True
        )
    ]


def _execute_function(sql, functions, events):
    events.append(sql)
    if sql.startswith("ALTER FUNCTION"):
        row = next(row for row in functions if row["signature"] in sql)
        row["definition"]["proowner"] = "17"
        row["acl"] = [
            [17 if grantee == 19 else grantee, 17 if grantor == 19 else grantor, privilege, grantable]
            for grantee, grantor, privilege, grantable in row["acl"]
        ]
        for access in row["access"]:
            access[3] = access[4] = access[0] in {10, 17, 18, 21}
            access[-1] = access[0] in {10, 17, 18, 21} or any(grantee == 0 for grantee, *_ in row["acl"])
    if sql.startswith("GRANT EXECUTE"):
        row = next(row for row in functions if row["signature"] in sql)
        row["acl"].append([19, 17, "EXECUTE", False])
        row["definition"]["proacl"] = ["source_owner=EXECUTE", "legacy_builder=EXECUTE"]
        next(access for access in row["access"] if access[0] == 19)[-1] = True


def _role_graph():
    return dict(
        roles=[
            dict(oid=oid, rolname=name)
            for oid, name in (
                (10, "native_admin"),
                (17, "source_owner"),
                (18, "source_publisher"),
                (19, "legacy_builder"),
                (20, "source_reader"),
                (21, "protected_owner_inheritor"),
                (22, "unrelated_role"),
            )
        ],
        memberships=[],
    )


_RECEIPT_CHECK_ROWS = [
    {"definition": check, "key_default": custody.guards._LEGACY_KEY_DEFAULT}
    for check in sorted(custody.guards._RECEIPT_CHECKS)
]


@pytest.fixture
def catalog(monkeypatch):
    policy = GenericSourcePolicy(
        "snapshot_schema", "source_owner", "source_publisher", ("legacy_builder", "source_reader"), ("legacy_builder",)
    )
    role_graph_by_field = _role_graph()
    owner_usage_rows = [[role["oid"], role["oid"] in {10, 17, 18, 21}] for role in role_graph_by_field["roles"]]
    functions, relations, events = _function_rows(policy), _relation_rows(), []
    for trigger in _trigger_rows():
        relations[int(trigger["tgrelid"]) - 60]["triggers"].append(trigger)

    @asynccontextmanager
    async def transaction():
        before = deepcopy(functions)
        try:
            yield
        except BaseException:
            functions[:] = before
            events.append("rollback")
            raise

    async def execute(sql):
        _execute_function(sql, functions, events)

    async def fetch(sql, *_):
        if sql == custody.guards._RECEIPT_CHECKS_SQL.replace(":schema", "$1"):
            return deepcopy(_RECEIPT_CHECK_ROWS)
        return deepcopy(relations if sql == custody._RELATION_SQL else functions)

    async def fetchrow(sql):
        if sql.startswith("WITH rows"):
            return dict(count=1, bytes=42, sha256="a" * 64, **({"largest_bytes": 42} if "largest_bytes" in sql else {}))
        return dict(session="native_admin", current="native_admin", backend=7, xid="9", native_admin=True)

    async def fetchval(sql, *arguments):
        if sql == custody._ROLE_GRAPH_SQL:
            return deepcopy(role_graph_by_field)
        if "pg_has_role(r.oid,$1::oid,'USAGE')" in sql:
            assert arguments == (17,), "USAGE must bind the fixed protected owner"
            return deepcopy(owner_usage_rows)
        return 8

    driver = SimpleNamespace(
        is_in_transaction=lambda: True,
        transaction=transaction,
        execute=AsyncMock(side_effect=execute),
        fetch=AsyncMock(side_effect=fetch),
        fetchrow=AsyncMock(side_effect=fetchrow),
        fetchval=AsyncMock(side_effect=fetchval),
    )
    monkeypatch.setattr(custody, "storage_roles", AsyncMock(return_value=(17, [19, 20])))
    return SimpleNamespace(
        policy=policy,
        driver=driver,
        functions=functions,
        relations=relations,
        events=events,
        role_graph_by_field=role_graph_by_field,
        owner_usage=owner_usage_rows,
    )


def test_five_bodies_and_composite_signature_match_authentic_migration():
    functions = _migration_functions()
    specifications = custody._specifications("snapshot_schema", 70)
    assert set(functions) == set(specifications) and len(functions) == 5
    for name, spec in specifications.items():
        actual = functions[name]
        assert actual["body"] == spec["body"] and actual["language"] == spec["language"]
        assert actual["returns"] == ("boolean" if spec["returns"] == "16" else "trigger")
    assert (
        functions[custody.INITIAL_CALLABLES[-1]]["arguments"]
        == 'receipt "snapshot_schema"."provider_directory_profile_initial_receipt"'
    )


@pytest.mark.asyncio
async def test_capture_decodes_native_json_and_binds_current_composite(catalog):
    before = await custody.capture_initial_callable_custody(catalog.driver, catalog.policy, legacy=True)
    assert before["relations"][custody._RELATIONS[0]]["composite"]["oid"] == "70"
    for row in catalog.functions:
        for key in ("definition", "acl", "access"):
            row[key] = json.dumps(row[key])
    assert await custody.capture_initial_callable_custody(catalog.driver, catalog.policy, legacy=True) == before


@pytest.mark.asyncio
async def test_adoption_preserves_data_types_oids_and_builder_execution(catalog):
    before = await custody.capture_initial_callable_custody(catalog.driver, catalog.policy, legacy=True)
    verify = AsyncMock()
    after = await custody.provision_initial_callable_custody(catalog.driver, catalog.policy, verify_role_profile=verify)
    custody.require_initial_callable_delta(before, after)
    assert verify.await_count == 2
    assert sum(sql.startswith("ALTER FUNCTION") for sql in catalog.events) == 5
    assert sum(sql.startswith("GRANT EXECUTE") for sql in catalog.events) == 4
    assert before["relations"] == after["relations"] and before["rows"] == after["rows"]
    assert all(next(row for row in value["access"] if row[0] == 19)[-1] for value in after["functions"].values())
    assert (
        await custody.provision_initial_callable_custody(
            catalog.driver, catalog.policy, verify_role_profile=AsyncMock()
        )
        == after
    )
    assert sum(sql.startswith("ALTER FUNCTION") for sql in catalog.events) == 5


@pytest.mark.asyncio
async def test_native_fixed_owner_usage_admits_only_inherited_owner_execution(catalog):
    before = await custody.capture_initial_callable_custody(catalog.driver, catalog.policy, legacy=True)
    after = await custody.provision_initial_callable_custody(
        catalog.driver, catalog.policy, verify_role_profile=AsyncMock()
    )
    assert before["owner_usage"] == after["owner_usage"] == catalog.owner_usage
    first = custody.INITIAL_CALLABLES[0]
    assert next(row for row in before["functions"][first]["access"] if row[0] == 21)[-1] is False
    assert next(row for row in after["functions"][first]["access"] if row[0] == 21)[-1] is True
    custody.require_initial_callable_delta(before, after)


@pytest.mark.asyncio
@pytest.mark.parametrize("prior_execute", [False, True])
async def test_inherited_owner_usage_requires_current_execution(catalog, prior_execute):
    before = await custody.capture_initial_callable_custody(catalog.driver, catalog.policy, legacy=True)
    after = await custody.provision_initial_callable_custody(
        catalog.driver, catalog.policy, verify_role_profile=AsyncMock()
    )
    previous = before["functions"][custody.INITIAL_CALLABLES[0]]
    current = after["functions"][custody.INITIAL_CALLABLES[0]]
    next(row for row in previous["access"] if row[0] == 21)[-1] = prior_execute
    next(row for row in current["access"] if row[0] == 21)[-1] = False
    assert next(row for row in before["owner_usage"] if row[0] == 21)[-1] is True
    assert before["owner_usage"] == after["owner_usage"]
    with pytest.raises(ValueError, match="execute_changed"):
        custody.require_initial_callable_delta(before, after)


@pytest.mark.asyncio
@pytest.mark.parametrize("boundary", [17, 18])
async def test_only_fixed_owner_and_publisher_are_explicit_execution_boundaries(catalog, boundary):
    before = await custody.capture_initial_callable_custody(catalog.driver, catalog.policy, legacy=True)
    after = await custody.provision_initial_callable_custody(
        catalog.driver, catalog.policy, verify_role_profile=AsyncMock()
    )
    for receipt in (before, after):
        for function in receipt["functions"].values():
            next(row for row in function["access"] if row[0] == boundary)[2] = True
    custody.require_initial_callable_delta(before, after)


@pytest.mark.asyncio
@pytest.mark.parametrize("mutation", ["ordinary_gain", "ordinary_loss", "unexpected_gain", "set_only_gain"])
async def test_execution_change_cannot_use_an_unnamed_role_waiver(catalog, mutation):
    before = await custody.capture_initial_callable_custody(catalog.driver, catalog.policy, legacy=True)
    after = await custody.provision_initial_callable_custody(
        catalog.driver, catalog.policy, verify_role_profile=AsyncMock()
    )
    previous = before["functions"][custody.INITIAL_CALLABLES[0]]
    current = after["functions"][custody.INITIAL_CALLABLES[0]]
    if mutation == "ordinary_loss":
        next(row for row in current["access"] if row[0] == 19)[-1] = False
    else:
        role = 20 if mutation == "ordinary_gain" else 22
        if mutation == "ordinary_gain":
            for receipt in (before, after):
                next(row for row in receipt["owner_usage"] if row[0] == 20)[-1] = True
        next(row for row in previous["access"] if row[0] == role)[-1] = False
        next(row for row in current["access"] if row[0] == role)[-1] = True
        if mutation == "set_only_gain":
            next(row for row in current["access"] if row[0] == role)[4] = True
    with pytest.raises(ValueError, match="execute_changed"):
        custody.require_initial_callable_delta(before, after)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mutation", ["changed", "missing", "duplicate", "not_boolean", "before_not_boolean", "access_missing", "classifier"]
)
async def test_fixed_owner_usage_and_all_role_coverage_are_exact(catalog, mutation):
    before = await custody.capture_initial_callable_custody(catalog.driver, catalog.policy, legacy=True)
    after = await custody.provision_initial_callable_custody(
        catalog.driver, catalog.policy, verify_role_profile=AsyncMock()
    )
    match mutation:
        case "changed":
            after["owner_usage"][0][-1] = False
        case "missing":
            for receipt in (before, after):
                receipt["owner_usage"].pop()
        case "duplicate":
            for receipt in (before, after):
                receipt["owner_usage"].append(receipt["owner_usage"][0])
        case "not_boolean":
            for receipt in (before, after):
                receipt["owner_usage"][0][-1] = 1
        case "before_not_boolean":
            before["owner_usage"][0][-1] = 1
        case "access_missing":
            for receipt in (before, after):
                receipt["functions"][custody.INITIAL_CALLABLES[0]]["access"].pop()
        case _:
            after["functions"][custody.INITIAL_CALLABLES[0]]["access"][0][2] = True
    with pytest.raises(ValueError, match="adoption_scope_changed|role_coverage_changed|role_classifier_changed"):
        custody.require_initial_callable_delta(before, after)


@pytest.mark.asyncio
@pytest.mark.parametrize("mutation", ["missing", "duplicate", "not_boolean", "foreign_role"])
async def test_native_usage_coverage_refuses_before_any_transfer(catalog, mutation):
    match mutation:
        case "missing":
            catalog.owner_usage.pop()
        case "duplicate":
            catalog.owner_usage.append(catalog.owner_usage[0])
        case "not_boolean":
            catalog.owner_usage[0][-1] = 1
        case _:
            catalog.owner_usage[-1][0] = 999
    with pytest.raises(ValueError, match="role_coverage_changed"):
        await custody.provision_initial_callable_custody(
            catalog.driver, catalog.policy, verify_role_profile=AsyncMock()
        )
    assert not any(sql.startswith(("ALTER FUNCTION", "GRANT")) for sql in catalog.events)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mutation",
    [
        "body",
        "argument",
        "overload",
        "owner",
        "acl",
        "secdef",
        "settings",
        "composite",
        "rls",
        "default",
        "variadic",
        "trigger_oid",
    ],
)
async def test_preexisting_drift_refuses_before_any_function_mutation(catalog, mutation):
    function_row = catalog.functions[0]
    match mutation:
        case "body":
            function_row["definition"]["prosrc"] += " SELECT 1"
        case "argument":
            catalog.functions[-1]["definition"]["proargtypes"] = ["999"]
        case "overload":
            catalog.functions.append({**deepcopy(function_row), "signature": "extra(integer)"})
        case "owner":
            function_row["definition"]["proowner"] = "20"
        case "acl":
            function_row["acl"].append([20, 19, "EXECUTE", False])
        case "secdef":
            function_row["definition"]["prosecdef"] = True
        case "settings":
            function_row["definition"]["proconfig"] = ["search_path=public"]
        case "default":
            function_row["definition"]["pronargdefaults"] = 1
        case "variadic":
            function_row["definition"]["provariadic"] = "25"
        case "trigger_oid":
            catalog.relations[0]["triggers"][0]["tgfoid"] = "999"
        case "composite":
            catalog.relations[0]["composite"]["typrelid"] = "999"
        case _:
            catalog.relations[0]["relation"]["relrowsecurity"] = True
    with pytest.raises(ValueError, match="profile_initial_callable_.*changed"):
        await custody.provision_initial_callable_custody(
            catalog.driver, catalog.policy, verify_role_profile=AsyncMock()
        )
    assert not any(sql.startswith(("ALTER FUNCTION", "GRANT")) for sql in catalog.events)


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["relations", "rows", "database_oid", "schema_oid", "role_graph"])
async def test_delta_refuses_unrelated_scope_or_data_change(catalog, field):
    before = await custody.capture_initial_callable_custody(catalog.driver, catalog.policy, legacy=True)
    after = await custody.provision_initial_callable_custody(
        catalog.driver, catalog.policy, verify_role_profile=AsyncMock()
    )
    after[field] = "changed"
    with pytest.raises(ValueError, match="adoption_scope_changed"):
        custody.require_initial_callable_delta(before, after)


@pytest.mark.asyncio
@pytest.mark.parametrize("mutation", ["function_oid", "body", "ordinary_execute", "extra_acl", "table_hook"])
async def test_delta_refuses_oid_body_privilege_and_table_hook_drift(catalog, mutation):
    before = await custody.capture_initial_callable_custody(catalog.driver, catalog.policy, legacy=True)
    after = await custody.provision_initial_callable_custody(
        catalog.driver, catalog.policy, verify_role_profile=AsyncMock()
    )
    row = after["functions"][custody.INITIAL_CALLABLES[0]]
    match mutation:
        case "function_oid":
            row["definition"]["oid"] = "999"
        case "body":
            row["definition"]["prosrc"] += " SELECT 1"
        case "ordinary_execute":
            next(access for access in row["access"] if access[0] == 19)[-1] = False
        case "extra_acl":
            row["acl"].append([20, 17, "EXECUTE", False])
        case _:
            after["relations"][custody._RELATIONS[0]]["triggers"] = ["changed"]
    with pytest.raises(ValueError, match="profile_initial_callable_.*changed"):
        custody.require_initial_callable_delta(before, after)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["profile", "late_acl", "cancel"])
async def test_late_failure_and_cancellation_roll_back_all_five(catalog, failure):
    original = deepcopy(catalog.functions)

    async def verify(*_, **__):
        if any(sql.startswith("ALTER FUNCTION") for sql in catalog.events):
            if failure == "cancel":
                raise asyncio.CancelledError()
            raise RuntimeError("late complete-profile failure")

    if failure == "late_acl":
        execute = catalog.driver.execute.side_effect

        async def changed(sql):
            await execute(sql)
            if sql.startswith("GRANT EXECUTE"):
                catalog.functions[0]["acl"].append([20, 17, "EXECUTE", True])

        catalog.driver.execute.side_effect = changed
    with pytest.raises(
        asyncio.CancelledError if failure == "cancel" else (ValueError, RuntimeError),
        match=None if failure == "cancel" else "changed|profile",
    ):
        await custody.provision_initial_callable_custody(catalog.driver, catalog.policy, verify_role_profile=verify)
    assert catalog.functions == original and "rollback" in catalog.events


@pytest.mark.asyncio
async def test_missing_verifier_or_original_transaction_never_starts_adoption(catalog):
    for verifier in (None, False):
        with pytest.raises(ValueError, match="explicit_admin_required"):
            await custody.provision_initial_callable_custody(
                catalog.driver, catalog.policy, verify_role_profile=verifier
            )
    catalog.driver.is_in_transaction = lambda: False
    with pytest.raises(ValueError, match="explicit_admin_required"):
        await custody.provision_initial_callable_custody(
            catalog.driver, catalog.policy, verify_role_profile=AsyncMock()
        )
    catalog.driver.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("bound", [(2, 42, None), (1, 16777217, None), (1, 42, None)])
async def test_data_bound_refuses_before_any_function_transfer(catalog, bound):
    fetchrow = catalog.driver.fetchrow.side_effect

    async def bounded(sql):
        if sql.startswith("WITH rows"):
            observed_by_field = dict(zip(("count", "bytes", "sha256"), bound, strict=True))
            if "largest_bytes" in sql:
                observed_by_field["largest_bytes"] = bound[1]
            return observed_by_field
        return await fetchrow(sql)

    catalog.driver.fetchrow.side_effect = bounded
    with pytest.raises(ValueError, match="rows_unbounded"):
        await custody.provision_initial_callable_custody(
            catalog.driver, catalog.policy, verify_role_profile=AsyncMock()
        )
    assert not any(sql.startswith(("ALTER FUNCTION", "GRANT")) for sql in catalog.events)


@pytest.mark.asyncio
async def test_protected_capture_refuses_ordinary_set_role_replacement(catalog):
    await custody.provision_initial_callable_custody(catalog.driver, catalog.policy, verify_role_profile=AsyncMock())
    next(access for access in catalog.functions[0]["access"] if access[0] == 19)[4] = True
    with pytest.raises(ValueError, match="replacement_authority"):
        await custody.capture_initial_callable_custody(catalog.driver, catalog.policy)


@pytest.mark.asyncio
async def test_overload_created_between_captures_rolls_back_all_owner_changes(catalog):
    original = deepcopy(catalog.functions)
    execute = catalog.driver.execute.side_effect

    async def late_overload(sql):
        await execute(sql)
        if sql.startswith("GRANT EXECUTE") and len(catalog.functions) == 5:
            catalog.functions.append({**deepcopy(catalog.functions[0]), "signature": "extra(text DEFAULT NULL)"})

    catalog.driver.execute.side_effect = late_overload
    with pytest.raises(ValueError, match="overloads_changed"):
        await custody.provision_initial_callable_custody(
            catalog.driver, catalog.policy, verify_role_profile=AsyncMock()
        )
    assert catalog.functions == original and "rollback" in catalog.events


@pytest.mark.asyncio
async def test_matcher_default_acl_cannot_be_replaced_by_an_explicit_equivalent(catalog):
    before = await custody.capture_initial_callable_custody(catalog.driver, catalog.policy, legacy=True)
    after = await custody.provision_initial_callable_custody(
        catalog.driver, catalog.policy, verify_role_profile=AsyncMock()
    )
    after["functions"][custody.INITIAL_CALLABLES[-1]]["definition"]["proacl"] = ["explicit equivalent"]
    with pytest.raises(ValueError, match="adoption_acl_changed"):
        custody.require_initial_callable_delta(before, after)


@pytest.mark.asyncio
@pytest.mark.parametrize("actor", ["legacy_builder", "source_owner"])
async def test_nonadmin_or_declared_actor_cannot_begin_mutation(catalog, actor):
    catalog.driver.fetchrow = AsyncMock(
        return_value=dict(session=actor, current=actor, backend=7, xid="9", native_admin=False)
    )
    with pytest.raises(ValueError, match="independent_admin_required"):
        await custody.provision_initial_callable_custody(
            catalog.driver, catalog.policy, verify_role_profile=AsyncMock()
        )
    catalog.driver.execute.assert_not_awaited()
