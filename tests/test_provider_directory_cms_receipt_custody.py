# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""One-table ownership closure retains exact reads and the existing legacy append surface."""

import ast
import json
import re
from contextlib import asynccontextmanager
from copy import deepcopy
from unittest.mock import AsyncMock

import pytest

from db.cms_registry_source_pair_slot import NATIVE_CHECK_SQL
from process import provider_directory_cms_callable_custody as callables
from process import provider_directory_cms_receipt_custody as custody
from tests.test_provider_directory_cms_callable_custody import catalog as catalog

MIGRATION_ROLE = "source_migrator"


def test_shared_catalog_accepts_builtin_checkpoint_timestamp_index_only():
    from db.models.system import ProviderDirectoryProfileBuildCheckpoint

    model = ProviderDirectoryProfileBuildCheckpoint
    assert model.updated_at.type.timezone is False
    assert any(entry["index_elements"] == ("state", "updated_at") for entry in model.__my_additional_indexes__)
    native_classes = re.search(
        r"WHERE n\.nspname<>'pg_catalog' OR o\.opcname NOT IN\s*(\([^)]*\))",
        custody._CATALOG_SQL,
    )
    assert native_classes is not None
    assert ast.literal_eval(native_classes[1]) == (
        "text_ops",
        "int8_ops",
        "uuid_ops",
        "xid8_ops",
        "bool_ops",
        "timestamp_ops",
        "timestamptz_ops",
        "oid_ops",
        "int4_ops",
    )
    assert "NOT EXISTS(SELECT FROM unnest(i.indclass) x(oid) JOIN pg_opclass o ON o.oid=x.oid" in custody._CATALOG_SQL
    assert "JOIN pg_namespace n ON n.oid=o.opcnamespace" in custody._CATALOG_SQL


def _attributes():
    columns = (
        ("receipt_id", 1043, 68, ""),
        ("payload", 3802, -1, ""),
        ("publication_xid", 5069, -1, "pg_current_xact_id()"),
        ("predecessor_receipt_id", 1043, 68, "((payload ->> 'predecessor_receipt_id'::text))"),
        ("address_lineage_id", 2950, -1, "((payload -> 'address'::text) ->> 'local_lineage_id'::text)::uuid"),
        ("address_generation", 20, -1, "((payload -> 'address'::text) ->> 'local_generation'::text)::bigint"),
        ("profile_generation_id", 25, -1, "(payload -> 'profile'::text) ->> 'generation_id'::text"),
        ("created_at", 1184, -1, "clock_timestamp()"),
        ("registry_source_pair_json", 3802, -1, ""),
    )
    attributes = [
        dict(
            attrelid="50",
            attnum=n,
            attname=name,
            atttypid=str(kind),
            atttypmod=mod,
            default_expression=default,
            attnotnull=n not in {4, 9},
            attgenerated="s" if 4 <= n <= 7 else "",
            attidentity="",
            attisdropped=False,
            attacl=None,
            attcollation=str(100 if n in {1, 4, 7} else 0),
        )
        for n, (name, kind, mod, default) in enumerate(columns, 1)
    ]
    return attributes


def _indexes():
    keys = ("1", "3", "4", "5 6 7", "0", "7 8 1", "0")
    indexes = [
        dict(
            indexrelid=str(60 + n),
            indrelid="50",
            index_name=name,
            indkey=[int(x) for x in key.split()],
            indisunique=n != 5,
            indisprimary=n == 0,
            indisexclusion=False,
            indimmediate=True,
            indisvalid=True,
            indisready=True,
            indislive=True,
            method="btree",
            native=True,
            indnatts=len(key.split()),
            indnkeyatts=len(key.split()),
            indoption=[0] * len(key.split()),
            indcollation=[str(100 if x in {1, 4, 7} or (x == 0 and n == 6) else 0) for x in map(int, key.split())],
            indisclustered=False,
            indisreplident=False,
            indnullsnotdistinct=False,
            index_expressions=(
                "true" if n == 4 else "registry_source_pair_json ->> 'capture_id'::text" if n == 6 else ""
            ),
            index_predicate=(
                "predecessor_receipt_id IS NULL"
                if n == 4
                else "registry_source_pair_json IS NOT NULL"
                if n == 6
                else ""
            ),
        )
        for n, (name, key) in enumerate(
            zip(
                ("primary", "xid", "previous", "generation", "root", "history", "cms_serving_registry_capture_unique"),
                keys,
                strict=True,
            )
        )
    ]
    return indexes


def _constraints():
    constraints = [
        dict(contype=kind, conkey=list(key), convalidated=True, constraint_definition="native key")
        for kind, key in (("p", (1,)), ("u", (3,)), ("u", (4,)), ("u", (5, 6, 7)))
    ]
    constraints.extend(
        dict(contype="n", conkey=[n], convalidated=True, constraint_definition="native not null")
        for n in (1, 2, 3, 5, 6, 7, 8)
    )
    constraints.append(dict(contype="t", convalidated=True, constraint_definition="native deferred trigger"))
    constraints.extend(
        [
            dict(
                contype="c",
                convalidated=True,
                constraint_definition="CHECK (receipt_id::text = encode(sha256(convert_to(payload::text, 'UTF8'::name)), 'hex'::text))",
            ),
            dict(contype="c", convalidated=True, constraint_definition=NATIVE_CHECK_SQL),
            dict(
                contype="f",
                convalidated=True,
                confrelid="50",
                conkey=[4],
                confkey=[1],
                confupdtype="a",
                confdeltype="a",
                confmatchtype="s",
                condeferrable=False,
                constraint_definition="FOREIGN KEY (predecessor_receipt_id) REFERENCES snapshot_schema.provider_directory_cms_serving_receipt(receipt_id)",
            ),
        ]
    )
    return constraints


def _hooks():
    hooks = [
        dict(
            tgname=name,
            tgisinternal=False,
            tgtype=event,
            tgdeferrable=deferred,
            tginitdeferred=deferred,
            tgenabled="O",
            tgqual=None,
            tgnargs=0,
            tgattr=[],
            tgargs="\\x",
            tgparentid="0",
            tgnewtable=None,
            tgoldtable=None,
            tgfoid=str(oid),
        )
        for name, event, deferred, oid in (
            ("cms_serving_receipt_immutable", 58, False, 110),
            ("cms_serving_receipt_insert", 5, True, 111),
        )
    ]
    return hooks


def _native_catalog():
    root_by_field = dict(
        oid="50",
        relname=custody.ROOT,
        relowner="19",
        relacl=None,
        relkind="r",
        relpersistence="p",
        relam="2",
        relnamespace="42",
        relrowsecurity=False,
        relforcerowsecurity=False,
        relispartition=False,
        relreplident="d",
        reloptions=None,
    )
    attributes, indexes, constraints, hooks = _attributes(), _indexes(), _constraints(), _hooks()
    return dict(
        classes=[
            root_by_field,
            *[
                dict(oid=entry["indexrelid"], relname=entry["index_name"], relowner="19", relacl=None)
                for entry in indexes
            ],
        ],
        attributes=attributes,
        indexes=indexes,
        constraints=constraints,
        triggers=hooks,
        rules=None,
        policies=None,
        inheritance=None,
        table_acl=[[19, 19, right, False] for right in sorted(custody._RIGHTS)],
        column_acl=[[3, 20, 19, "SELECT", False]],
        access=[],
    )


def _access(catalog):
    is_protected = catalog["classes"][0]["relowner"] == "17"
    catalog["access"] = [
        dict(
            role=role,
            superuser=role == 10,
            builtin=False,
            owner_reach=role in {10, 17, 18, 21},
            owner_usage=role in {10, 17, 18, 21},
            owner_set=role in {10, 17, 18, 21},
            builder_reach=role in {10, 18, 19},
            table=(
                sorted(custody._RIGHTS)
                if role in {10, 17, 18, 21} or (role == 19 and not is_protected)
                else ["SELECT", "MAINTAIN"]
                if role == 19
                else []
            ),
            columns=[
                [
                    n,
                    (["SELECT", "INSERT"] if n in {1, 2} else ["SELECT"])
                    if is_protected and role == 19
                    else ["SELECT", "INSERT", "UPDATE", "REFERENCES"]
                    if role == 19 and not is_protected
                    else ["SELECT"]
                    if role == 20 and n == 3
                    else [],
                ]
                for n in range(1, 10)
            ],
        )
        for role in (10, 17, 18, 19, 21, 20)
    ]


def _add_migration_identity(catalog):
    catalog.graph["roles"].append(
        dict(
            oid="21",
            rolname=MIGRATION_ROLE,
            rolcanlogin=True,
            rolinherit=True,
            rolsuper=False,
            rolcreaterole=False,
            rolcreatedb=False,
            rolreplication=False,
            rolbypassrls=False,
        )
    )
    catalog.graph["memberships"].append(
        dict(
            member="21",
            roleid="17",
            grantor="10",
            admin_option=False,
            inherit_option=True,
            set_option=True,
        )
    )


@pytest.fixture
async def receipt_catalog(catalog):
    _add_migration_identity(catalog)
    await callables.provision_cms_callable_custody(catalog.driver, catalog.policy, verify_role_profile=AsyncMock())
    native_catalog = _native_catalog()
    _access(native_catalog)
    original_fetchval = catalog.driver.fetchval.side_effect
    catalog.driver.fetchval.side_effect = lambda sql, *args: (
        deepcopy(native_catalog) if sql == custody._CATALOG_SQL else original_fetchval(sql, *args)
    )

    @asynccontextmanager
    async def savepoint():
        prior = deepcopy(native_catalog)
        try:
            yield
        except BaseException:
            native_catalog.clear()
            native_catalog.update(prior)
            raise

    async def execute(sql):
        if sql.startswith("ALTER TABLE"):
            for entry in native_catalog["classes"]:
                entry["relowner"] = "17"
            native_catalog["table_acl"] = [
                [17 if grantee == 19 else grantee, 17 if grantor == 19 else grantor, right, x]
                for grantee, grantor, right, x in native_catalog["table_acl"]
            ]
            native_catalog["column_acl"] = [
                [n, 17 if grantee == 19 else grantee, 17 if grantor == 19 else grantor, right, x]
                for n, grantee, grantor, right, x in native_catalog["column_acl"]
            ]
        elif sql.startswith("GRANT SELECT"):
            native_catalog["table_acl"].extend([[19, 17, right, False] for right in ("SELECT", "MAINTAIN")])
        elif sql.startswith("GRANT INSERT"):
            native_catalog["column_acl"].extend([[n, 19, 17, "INSERT", False] for n in (1, 2)])
        if sql.startswith(("ALTER TABLE", "GRANT")):
            _access(native_catalog)

    catalog.driver.transaction = savepoint
    catalog.driver.execute = AsyncMock(side_effect=execute)
    catalog.rows_receipt = native_catalog
    return catalog


@pytest.mark.asyncio
async def test_native_owner_closure_preserves_partial_reader_and_legacy_writer(receipt_catalog):
    case = receipt_catalog
    before = await custody.capture_cms_serving_receipt_custody(
        case.driver, case.policy, migration_role=MIGRATION_ROLE, legacy=True
    )
    after = await custody.provision_cms_serving_receipt_custody(
        case.driver, case.policy, migration_role=MIGRATION_ROLE, verify_role_profile=AsyncMock()
    )
    custody.require_cms_serving_receipt_adoption_delta(before, after)
    assert [3, 20, 17, "SELECT", False] in after["catalog"]["column_acl"]
    assert not any(entry[0] == 20 for entry in after["catalog"]["table_acl"])
    assert [1, 19, 17, "INSERT", False] in after["catalog"]["column_acl"]
    assert not any(entry[0] == 9 and entry[3] == "INSERT" for entry in after["catalog"]["column_acl"])
    calls = [call.args[0] for call in case.driver.execute.await_args_list]
    assert len(calls) == 4 and calls[0].startswith("LOCK TABLE ONLY")
    assert not any(word in sql for sql in calls for word in ("CREATE", "DROP", "UPDATE", "DELETE", "TRUNCATE"))
    case.driver.execute.reset_mock()
    assert (
        await custody.provision_cms_serving_receipt_custody(
            case.driver, case.policy, migration_role=MIGRATION_ROLE, verify_role_profile=AsyncMock()
        )
        == after
    )
    assert case.driver.execute.await_count == 1  # Idempotence still fences the existing native heap.


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mutation",
    [
        "foreign_owner",
        "column_dml",
        "table_dml",
        "grant_option",
        "hook",
        "index",
        "nullable",
        "inherited",
        "owner_reach",
        "callable_body",
        "missing_writer",
    ],
)
async def test_existing_drift_refuses_before_owner_transfer(receipt_catalog, mutation):
    case = receipt_catalog
    native_catalog = case.rows_receipt
    match mutation:
        case "foreign_owner":
            native_catalog["classes"][0]["relowner"] = "20"
        case "column_dml":
            native_catalog["column_acl"].append([9, 20, 19, "INSERT", False])
        case "table_dml":
            native_catalog["table_acl"].append([20, 19, "TRIGGER", False])
        case "grant_option":
            native_catalog["column_acl"][0][-1] = True
        case "hook":
            native_catalog["triggers"][0]["tgenabled"] = "D"
        case "index":
            native_catalog["indexes"][0]["native"] = False
        case "nullable":
            native_catalog["attributes"][0]["attnotnull"] = False
        case "inherited":
            native_catalog["access"][-1]["builder_reach"] = True
        case "owner_reach":
            native_catalog["access"][-1]["owner_reach"] = True
        case "missing_writer":
            native_builder = next(entry for entry in case.rows_receipt["access"] if entry["role"] == 19)
            native_builder["table"] = ["SELECT"]
        case "callable_body":
            case.rows[0]["definition"]["prosrc"] += "changed"
    with pytest.raises((ValueError, RuntimeError), match="changed|authority|inherited|missing"):
        await custody.provision_cms_serving_receipt_custody(
            case.driver, case.policy, migration_role=MIGRATION_ROLE, verify_role_profile=AsyncMock()
        )
    assert not any(call.args[0].startswith("ALTER") for call in case.driver.execute.await_args_list)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mutation", ["oid", "index_owner", "trigger_oid", "raw_attribute", "reader_expansion", "slot_insert", "role_graph"]
)
async def test_delta_never_baselines_postcut_changes(receipt_catalog, mutation):
    case = receipt_catalog
    before = await custody.capture_cms_serving_receipt_custody(
        case.driver, case.policy, migration_role=MIGRATION_ROLE, legacy=True
    )
    after = await custody.provision_cms_serving_receipt_custody(
        case.driver, case.policy, migration_role=MIGRATION_ROLE, verify_role_profile=AsyncMock()
    )
    match mutation:
        case "oid":
            after["catalog"]["classes"][0]["oid"] = "999"
        case "index_owner":
            after["catalog"]["classes"][1]["relowner"] = "19"
        case "trigger_oid":
            after["catalog"]["triggers"][0]["tgfoid"] = "999"
        case "raw_attribute":
            after["catalog"]["attributes"][0]["attcompression"] = "changed"
        case "reader_expansion":
            after["catalog"]["table_acl"].append([20, 17, "SELECT", False])
        case "slot_insert":
            after["catalog"]["column_acl"].append([9, 19, 17, "INSERT", False])
        case "role_graph":
            after["functions"]["role_graph"]["memberships"].append({"member": 20, "roleid": 17})
    with pytest.raises((ValueError, StopIteration), match="changed|^$"):
        custody.require_cms_serving_receipt_adoption_delta(before, after)


@pytest.mark.asyncio
async def test_postprofile_failure_restores_owner_acl_and_native_session(receipt_catalog):
    case = receipt_catalog
    before = deepcopy(case.rows_receipt)
    verify = AsyncMock(side_effect=[None, RuntimeError("late_profile_failure")])
    with pytest.raises(RuntimeError, match="late_profile_failure"):
        await custody.provision_cms_serving_receipt_custody(
            case.driver, case.policy, migration_role=MIGRATION_ROLE, verify_role_profile=verify
        )
    assert case.rows_receipt == before and verify.await_count == 2


@pytest.mark.asyncio
async def test_raw_json_capture_has_the_same_native_receipt(receipt_catalog):
    case = receipt_catalog
    before = await custody.capture_cms_serving_receipt_custody(
        case.driver, case.policy, migration_role=MIGRATION_ROLE, legacy=True
    )
    original = case.driver.fetchval.side_effect
    case.driver.fetchval.side_effect = lambda sql, *args: (
        json.dumps(original(sql, *args)) if sql == custody._CATALOG_SQL else original(sql, *args)
    )
    assert (
        await custody.capture_cms_serving_receipt_custody(
            case.driver, case.policy, migration_role=MIGRATION_ROLE, legacy=True
        )
        == before
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "migration_role",
    [None, "", "source_owner", "source_publisher", "legacy_builder", "source_reader", "unconfigured_role"],
)
async def test_migration_identity_must_be_explicit_distinct_and_present(receipt_catalog, migration_role):
    case = receipt_catalog
    with pytest.raises(ValueError):
        await custody.provision_cms_serving_receipt_custody(
            case.driver, case.policy, migration_role=migration_role, verify_role_profile=AsyncMock()
        )
    assert not any(call.args[0].startswith("ALTER") for call in case.driver.execute.await_args_list)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mutation",
    ["missing", "duplicate", "admin_option", "inherit_option", "set_option", "nologin", "superuser", "usage", "set"],
)
async def test_migration_native_membership_and_effective_reach_cannot_drift(receipt_catalog, mutation):
    case = receipt_catalog
    membership = case.graph["memberships"][0]
    role = next(entry for entry in case.graph["roles"] if entry["rolname"] == MIGRATION_ROLE)
    access = next(entry for entry in case.rows_receipt["access"] if entry["role"] == 21)
    if mutation == "missing":
        case.graph["memberships"].clear()
    elif mutation == "duplicate":
        case.graph["memberships"].append(deepcopy(membership))
    elif mutation in {"admin_option", "inherit_option", "set_option"}:
        membership[mutation] = mutation == "admin_option"
    elif mutation in {"nologin", "superuser"}:
        role["rolcanlogin" if mutation == "nologin" else "rolsuper"] = mutation == "superuser"
    else:
        access["owner_" + mutation] = False
    with pytest.raises(ValueError, match="migration_.*changed"):
        await custody.provision_cms_serving_receipt_custody(
            case.driver, case.policy, migration_role=MIGRATION_ROLE, verify_role_profile=AsyncMock()
        )
    assert not any(call.args[0].startswith("ALTER") for call in case.driver.execute.await_args_list)


@pytest.mark.asyncio
@pytest.mark.parametrize("is_builtin", [False, True])
async def test_migration_allowance_never_exempts_other_owner_reach(receipt_catalog, is_builtin):
    case = receipt_catalog
    access = deepcopy(next(entry for entry in case.rows_receipt["access"] if entry["role"] == 21))
    access.update(role=22, builtin=is_builtin)
    case.rows_receipt["access"].append(access)
    with pytest.raises(ValueError, match="owner_authority"):
        await custody.provision_cms_serving_receipt_custody(
            case.driver, case.policy, migration_role=MIGRATION_ROLE, verify_role_profile=AsyncMock()
        )
    assert not any(call.args[0].startswith("ALTER") for call in case.driver.execute.await_args_list)


async def _read_receipt_cut(case):
    from process.provider_directory_cms_control_privileges import cms_control_read_edges, cms_control_read_oids

    await custody.provision_cms_serving_receipt_custody(
        case.driver, case.policy, migration_role=MIGRATION_ROLE, verify_role_profile=AsyncMock()
    )
    before = await custody.capture_cms_serving_receipt_custody(case.driver, case.policy, migration_role=MIGRATION_ROLE)
    after = deepcopy(before)
    catalog = after["catalog"]
    root = next(entry for entry in catalog["classes"] if entry["relname"] == custody.ROOT)
    attributes = [entry for entry in catalog["attributes"] if entry["attrelid"] == root["oid"]]
    grantees = cms_control_read_oids(case.policy, before["functions"]["role_graph"])
    additions = cms_control_read_edges(custody.ROOT, attributes, grantees, before["owner"])
    catalog["column_acl"] = sorted({tuple(entry) for entry in catalog["column_acl"]} | additions)
    for attribute in attributes:
        if any(edge[0] == attribute["attnum"] for edge in additions):
            attribute["attacl"] = ["native_selected_acl"]
    return before, after


@pytest.mark.asyncio
async def test_fixed_publisher_and_reader_metadata_columns_preserve_whole_receipt_custody(receipt_catalog):
    case = receipt_catalog
    before, after = await _read_receipt_cut(case)
    root = next(entry for entry in after["catalog"]["classes"] if entry["relname"] == custody.ROOT)
    custody._require_acl(after["catalog"], root, (17, 18, 19, 21), {19, 20}, legacy=False)
    custody.require_cms_serving_receipt_read_delta(before, after, case.policy)
    custody.require_cms_serving_receipt_read_delta(after, after, case.policy)


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["table", "column", "grantor", "grant_option", "write", "body", "oid", "roles"])
async def test_read_delta_rejects_every_other_catalog_acl_and_role_change(receipt_catalog, change):
    case = receipt_catalog
    before, after = await _read_receipt_cut(case)
    after["catalog"]["column_acl"] = [list(entry) for entry in after["catalog"]["column_acl"]]
    edge = next(entry for entry in after["catalog"]["column_acl"] if entry[1] == 18)
    if change in {"column", "grantor", "grant_option", "write"}:
        position, value = {"column": (0, 4), "grantor": (2, 19), "grant_option": (4, True), "write": (3, "INSERT")}[
            change
        ]
        edge[position] = value
    elif change == "table":
        after["catalog"]["table_acl"].append([18, 17, "SELECT", False])
    elif change == "body":
        after["catalog"]["triggers"][0]["tgargs"] = "substituted"
    elif change == "oid":
        after["catalog"]["classes"][0]["oid"] = "unplanned"
    else:
        after["functions"]["role_graph"]["roles"][0]["rolname"] = "substituted"
    with pytest.raises(ValueError, match="cms_receipt_read"):
        custody.require_cms_serving_receipt_read_delta(before, after, case.policy)


@pytest.mark.asyncio
@pytest.mark.parametrize("column", [4, 8])
async def test_publisher_direct_read_is_limited_to_the_fixed_metadata_projection(receipt_catalog, column):
    _before, after = await _read_receipt_cut(receipt_catalog)
    after["catalog"]["column_acl"].append([column, 18, 17, "SELECT", False])
    root = next(entry for entry in after["catalog"]["classes"] if entry["relname"] == custody.ROOT)
    with pytest.raises(ValueError, match="cms_receipt_column_acl_changed"):
        custody._require_acl(after["catalog"], root, (17, 18, 19, 21), {19, 20}, legacy=False)
