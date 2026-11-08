# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""One-table ownership closure retains exact reads and the existing legacy append surface."""

import ast
import asyncio
import json
import re
from contextlib import asynccontextmanager
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from db.cms_registry_source_pair_slot import NATIVE_CHECK_SQL
from process import provider_directory_cms_callable_custody as callables
from process import provider_directory_cms_receipt_custody as custody
from process import provider_directory_cms_receipt_policy as receipt_policy
from tests.test_provider_directory_cms_callable_custody import catalog as catalog

MIGRATION_ROLE = "source_migrator"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "roles,protected,expected_roles",
    [
        ((), None, ()),
        (("executor", "preparer"), False, ()),
        (("executor", "preparer"), True, ("executor", "preparer")),
        (("executor", "preparer"), None, None),
    ],
)
async def test_component_receipt_adoption_preserves_explicit_readers(monkeypatch, roles, protected, expected_roles):
    """Check fixture ordering only; native privilege and custody proofs remain separate."""
    from tests import nppes_candidate_postgres_support as fixture

    driver = SimpleNamespace(fetchval=AsyncMock(return_value=protected))
    policy = SimpleNamespace(schema="fixture_source")
    provision = AsyncMock()
    monkeypatch.setattr(fixture, "provision_cms_serving_receipt_custody", provision)
    if expected_roles is None:
        with pytest.raises(AssertionError):
            await fixture._provision_component_receipt(driver, policy, MIGRATION_ROLE, roles)
        provision.assert_not_awaited()
    else:
        await fixture._provision_component_receipt(driver, policy, MIGRATION_ROLE, roles)
        provision.assert_awaited_once_with(
            driver,
            policy,
            migration_role=MIGRATION_ROLE,
            verify_role_profile=fixture.verify_native_source_profile,
            metadata_read_roles=expected_roles,
        )
    if not roles:
        driver.fetchval.assert_not_awaited()


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
    inherited_access_entries = [entry for entry in catalog["access"] if entry["role"] not in {10, 17, 18, 19, 20, 21}]
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
    builder = next(entry for entry in catalog["access"] if entry["role"] == 19)
    for entry in inherited_access_entries:
        if entry["builder_reach"]:
            entry["table"], entry["columns"] = deepcopy(builder["table"]), deepcopy(builder["columns"])
    catalog["access"].extend(inherited_access_entries)


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


def _native_policies():
    return [
        dict(
            oid=str(80 + number),
            polname=name,
            polrelid="50",
            polcmd=command,
            polpermissive=True,
            polroles=["0"],
            polqual="native true" if command == "r" else None,
            polwithcheck="native identities" if command == "a" else None,
        )
        for number, (name, command) in enumerate(
            ((receipt_policy.READ_POLICY, "r"), (receipt_policy.INSERT_POLICY, "a"))
        )
    ]


def _policy_proofs(catalog):
    return [
        dict(
            policy=deepcopy(policy),
            using_expression="true" if policy["polcmd"] == "r" else None,
            check_expression=receipt_policy.insert_expression("legacy_builder") if policy["polcmd"] == "a" else None,
            native_dependencies=True,
        )
        for policy in catalog["policies"] or ()
    ] or None


def _execute_receipt(sql, native_catalog, lock_state):
    if sql.startswith("LOCK TABLE ONLY"):
        lock_state.is_held = True
    if sql.startswith("ALTER TABLE") and "ENABLE ROW LEVEL SECURITY" in sql:
        native_catalog["classes"][0]["relrowsecurity"] = True
    if sql.startswith("CREATE POLICY"):
        native_catalog["policies"] = [
            *(native_catalog["policies"] or []),
            next(policy for policy in _native_policies() if policy["polname"] == sql.split()[2]),
        ]
    if sql.startswith("ALTER TABLE") and "OWNER TO" in sql:
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
    if sql.startswith("GRANT SELECT,"):
        native_catalog["table_acl"].extend([[19, 17, right, False] for right in ("SELECT", "MAINTAIN")])
    if sql.startswith("GRANT INSERT"):
        native_catalog["column_acl"].extend([[n, 19, 17, "INSERT", False] for n in (1, 2)])
    if sql.startswith(("ALTER TABLE", "GRANT")):
        _access(native_catalog)


@pytest.fixture
async def receipt_catalog(catalog):
    _add_migration_identity(catalog)
    for role in catalog.graph["roles"]:
        role.setdefault("rolbypassrls", False)
    await callables.provision_cms_callable_custody(catalog.driver, catalog.policy, verify_role_profile=AsyncMock())
    native_catalog = _native_catalog()
    _access(native_catalog)
    original_fetchval = catalog.driver.fetchval.side_effect
    lock_state = SimpleNamespace(is_held=False)

    def fetchval(sql, *args):
        if sql == custody._CATALOG_SQL:
            return deepcopy(native_catalog)
        if sql == custody._LOCK_SQL:
            return lock_state.is_held
        if sql == receipt_policy.POLICY_SQL:
            return _policy_proofs(native_catalog)
        return original_fetchval(sql, *args)

    catalog.driver.fetchval.side_effect = fetchval

    @asynccontextmanager
    async def savepoint():
        prior = deepcopy(native_catalog)
        try:
            yield
        except BaseException:
            native_catalog.clear()
            native_catalog.update(prior)
            raise

    catalog.driver.transaction = savepoint
    catalog.driver.execute = AsyncMock(side_effect=lambda sql: _execute_receipt(sql, native_catalog, lock_state))
    catalog.rows_receipt = native_catalog
    return catalog


METADATA_READ_ROLES = ("independent_executor", "independent_preparer")


async def _metadata_reader_fixture(case):
    """Model existing identities/schema USAGE, before any receipt read grant."""
    for oid, name in zip((22, 23), METADATA_READ_ROLES, strict=True):
        case.graph["roles"].append(
            dict(
                oid=str(oid),
                rolname=name,
                rolcanlogin=True,
                rolinherit=True,
                rolsuper=False,
                rolcreatedb=False,
                rolcreaterole=False,
                rolreplication=False,
                rolbypassrls=False,
            )
        )
        case.rows_receipt["access"].append(
            dict(
                role=oid,
                superuser=False,
                builtin=False,
                owner_reach=False,
                owner_usage=False,
                owner_set=False,
                builder_reach=False,
                table=[],
                columns=[[n, []] for n in range(1, 10)],
            )
        )
    case.metadata = _metadata_reader_observation(case.policy.schema)
    case.graph["memberships"].append(
        dict(member="23", roleid="22", grantor="10", admin_option=False, inherit_option=True, set_option=True)
    )
    original_fetchval = case.driver.fetchval.side_effect
    case.driver.fetchval.side_effect = lambda sql, *args: (
        deepcopy(case.metadata) if sql == custody._METADATA_READERS_SQL else original_fetchval(sql, *args)
    )
    original_fetch = case.driver.fetch.side_effect
    case.driver.fetch.side_effect = lambda sql, *args: (
        deepcopy(case.metadata["role_access"]) if sql == custody.ROLE_SQL else original_fetch(sql, *args)
    )
    original_execute = case.driver.execute.side_effect

    case.driver.execute.side_effect = lambda sql: _execute_metadata_grants(case, sql, original_execute)
    await custody.provision_cms_serving_receipt_custody(
        case.driver, case.policy, migration_role=MIGRATION_ROLE, verify_role_profile=AsyncMock()
    )
    case.driver.execute.reset_mock()
    return case


def _metadata_reader_observation(schema):
    return dict(
        namespace=dict(oid="42", nspname=schema, nspowner="17", nspacl=["existing_usage"]),
        access=[dict(role=oid, usage=True, create=False, source_reach=False) for oid in (22, 23)],
        beneficiaries=[[23, 22]],
        role_access=[
            dict(
                rolname=name,
                oid=oid,
                namespace_usage=True,
                namespace_owner_protected=True,
                elevated=False,
                namespace_create=False,
                namespace_owner_reachable=False,
                owner_set=False,
                publisher_set=False,
            )
            for oid, name in zip((22, 23), METADATA_READ_ROLES, strict=True)
        ],
    )


def _execute_metadata_grants(case, sql, original_execute):
    if not sql.startswith("GRANT SELECT("):
        return original_execute(sql)
    role = 22 if sql.endswith('"independent_executor"') else 23
    for attribute in case.rows_receipt["attributes"]:
        if attribute["attname"] not in custody.ORIGIN_METADATA_COLUMNS:
            continue
        column = attribute["attnum"]
        acl_fields = [column, role, 17, "SELECT", False]
        if acl_fields not in case.rows_receipt["column_acl"]:
            case.rows_receipt["column_acl"].append(acl_fields)
        attribute["attacl"] = sorted(str(entry) for entry in case.rows_receipt["column_acl"] if entry[0] == column)
        access = next(entry for entry in case.rows_receipt["access"] if entry["role"] == role)
        access["columns"][column - 1][1] = ["SELECT"]


async def _capture_metadata(case):
    return await custody.capture_cms_serving_receipt_custody(
        case.driver, case.policy, migration_role=MIGRATION_ROLE, metadata_read_roles=METADATA_READ_ROLES
    )


async def _grant_metadata(case, verify=None):
    return await custody.provision_cms_origin_metadata_readers(
        case.driver,
        case.policy,
        migration_role=MIGRATION_ROLE,
        metadata_read_roles=METADATA_READ_ROLES,
        verify_role_profile=verify or AsyncMock(),
    )


@pytest.mark.asyncio
async def test_origin_metadata_cut_preserves_declared_identities_and_complete_custody(receipt_catalog):
    case = await _metadata_reader_fixture(receipt_catalog)
    before = await _capture_metadata(case)
    verify = AsyncMock()
    after = await _grant_metadata(case, verify)
    custody.require_cms_serving_receipt_read_delta(before, after, case.policy, metadata_read_roles=METADATA_READ_ROLES)
    assert after["metadata_readers"] == before["metadata_readers"]
    assert after["metadata_readers"]["roles"] == [["independent_executor", 22], ["independent_preparer", 23]]
    assert json.loads(json.dumps(after["metadata_readers"])) == after["metadata_readers"]
    assert len([edge for edge in after["catalog"]["column_acl"] if edge[1] in {22, 23}]) == 10
    assert verify.await_count == 2
    assert [call.args[0].split()[0] for call in case.driver.execute.await_args_list] == ["LOCK", "GRANT", "GRANT"]
    assert await _grant_metadata(case) == after
    assert (
        await custody.provision_cms_serving_receipt_custody(
            case.driver,
            case.policy,
            migration_role=MIGRATION_ROLE,
            metadata_read_roles=METADATA_READ_ROLES,
            verify_role_profile=AsyncMock(),
        )
        == after
    )
    with pytest.raises(ValueError, match="column_acl_changed"):
        await custody.capture_cms_serving_receipt_custody(case.driver, case.policy, migration_role=MIGRATION_ROLE)


@pytest.mark.asyncio
async def test_origin_metadata_declaration_cannot_accompany_legacy_adoption(receipt_catalog):
    case = receipt_catalog
    before = deepcopy(case.rows_receipt)
    with pytest.raises(ValueError, match="metadata_reader_protected_receipt_required"):
        await custody.provision_cms_serving_receipt_custody(
            case.driver,
            case.policy,
            migration_role=MIGRATION_ROLE,
            metadata_read_roles=METADATA_READ_ROLES,
            verify_role_profile=AsyncMock(),
        )
    assert case.rows_receipt == before
    assert not any(call.args[0].startswith(("GRANT", "ALTER")) for call in case.driver.execute.await_args_list)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "actor",
    [
        ("independent_executor", "independent_executor", False),
        ("source_publisher", "source_publisher", False),
        ("native_admin", "source_owner", True),
    ],
)
async def test_origin_metadata_cut_requires_original_independent_admin(receipt_catalog, actor):
    case = await _metadata_reader_fixture(receipt_catalog)
    case.driver.fetchrow.return_value.update(session=actor[0], current=actor[1], native_admin=actor[2])
    verify = AsyncMock()
    with pytest.raises(ValueError, match="independent_admin_required"):
        await _grant_metadata(case, verify)
    verify.assert_not_awaited()
    case.driver.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change",
    [
        "missing_usage",
        "create",
        "source_reach",
        "undeclared_beneficiary",
        "superuser",
        "missing_role",
        "set_namespace_owner",
        "set_create_role",
        "set_elevated_role",
    ],
)
async def test_origin_metadata_preexisting_authority_refused_before_grant(receipt_catalog, change):
    case = await _metadata_reader_fixture(receipt_catalog)
    match change:
        case "missing_usage":
            case.metadata["access"][0]["usage"] = False
        case "create" | "source_reach":
            case.metadata["access"][0][change] = True
        case "undeclared_beneficiary":
            case.metadata["beneficiaries"].append([24, 22])
        case "superuser":
            case.graph["roles"][-1]["rolsuper"] = True
        case "missing_role":
            case.graph["roles"].pop()
        case _:
            role_by_field = {**case.graph["roles"][-1], "oid": "24", "rolname": "reachable_authority"}
            role_by_field["rolcreaterole"] = change == "set_elevated_role"
            case.graph["roles"].append(role_by_field)
            case.graph["memberships"].append(
                dict(member="22", roleid="24", grantor="10", admin_option=False, inherit_option=False, set_option=True)
            )
            key = {
                "set_namespace_owner": "namespace_owner_reachable",
                "set_create_role": "namespace_create",
                "set_elevated_role": "elevated",
            }[change]
            case.metadata["role_access"][0][key] = True
    with pytest.raises(ValueError, match="metadata_read"):
        await _grant_metadata(case)
    assert not any(call.args[0].startswith("GRANT") for call in case.driver.execute.await_args_list)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change",
    [
        "table",
        "column",
        "grant_option",
        "grantor",
        "write",
        "undeclared",
        "owner",
        "schema",
        "role_graph",
        "declaration",
        "effective_beneficiary",
    ],
)
async def test_origin_metadata_delta_closes_every_other_privilege_and_identity(receipt_catalog, change):
    case = await _metadata_reader_fixture(receipt_catalog)
    before = await _capture_metadata(case)
    after = await _grant_metadata(case)
    edge = next(entry for entry in after["catalog"]["column_acl"] if entry[1] == 22)
    mutations_by_field = {
        "column": (0, 4),
        "grant_option": (4, True),
        "grantor": (2, 19),
        "write": (3, "INSERT"),
        "undeclared": (1, 24),
    }
    if change in mutations_by_field:
        position, changed_field = mutations_by_field[change]
        edge[position] = changed_field
    else:
        _mutate_metadata_identity(after, change)
    with pytest.raises(ValueError, match="read_.*changed"):
        custody.require_cms_serving_receipt_read_delta(
            before, after, case.policy, metadata_read_roles=METADATA_READ_ROLES
        )


def _mutate_metadata_identity(after, change):
    match change:
        case "table":
            after["catalog"]["table_acl"].append([22, 17, "SELECT", False])
        case "owner":
            after["catalog"]["classes"][0]["relowner"] = "22"
        case "schema":
            after["metadata_readers"]["namespace"]["nspacl"].append("new_usage")
        case "role_graph":
            after["functions"]["role_graph"]["memberships"].append({"member": 22, "roleid": 20})
        case "declaration":
            after["metadata_readers"]["roles"][0][1] = 24
        case _:
            next(entry for entry in after["catalog"]["access"] if entry["role"] == 20)["columns"][0][1] = ["SELECT"]


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [RuntimeError("late_profile_failure"), asyncio.CancelledError()])
async def test_origin_metadata_failure_and_cancellation_restore_exact_preimage(receipt_catalog, failure):
    case = await _metadata_reader_fixture(receipt_catalog)
    before = await _capture_metadata(case)
    with pytest.raises(type(failure)):
        await _grant_metadata(case, AsyncMock(side_effect=[None, failure]))
    assert await _capture_metadata(case) == before


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
    assert len(calls) == 7 and calls[0].startswith("LOCK TABLE ONLY")
    assert not any(word in sql for sql in calls for word in ("DROP", "UPDATE", "DELETE", "TRUNCATE"))
    assert sum(sql.startswith("CREATE POLICY") for sql in calls) == 2
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
    custody._require_acl(
        after["catalog"], root, (17, 18, 19, 21), {19, 20}, legacy=False, role_graph=after["functions"]["role_graph"]
    )
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
        custody._require_acl(
            after["catalog"],
            root,
            (17, 18, 19, 21),
            {19, 20},
            legacy=False,
            role_graph=after["functions"]["role_graph"],
        )


@pytest.mark.asyncio
async def test_runtime_legacy_guard_still_refuses_inherited_writer(receipt_catalog):
    case = receipt_catalog
    case.rows_receipt["access"][-1]["builder_reach"] = True
    with pytest.raises(ValueError, match="cms_receipt_inherited_writer"):
        await custody.capture_cms_serving_receipt_custody(
            case.driver, case.policy, migration_role=MIGRATION_ROLE, legacy=True
        )


@pytest.mark.asyncio
async def test_admin_transition_requires_its_real_lock_and_original_identity(receipt_catalog):
    case = receipt_catalog
    actor = await custody._admin_identity(case.driver, case.policy)
    with pytest.raises(ValueError, match="transition_lock_required"):
        await custody._capture_locked_transition(case.driver, case.policy, MIGRATION_ROLE, actor)
    await case.driver.execute(
        'LOCK TABLE ONLY "snapshot_schema"."provider_directory_cms_serving_receipt" IN ACCESS EXCLUSIVE MODE NOWAIT'
    )
    with pytest.raises(ValueError, match="admin_transaction_changed"):
        await custody._capture_locked_transition(case.driver, case.policy, MIGRATION_ROLE, dict(actor, backend=99))


@pytest.mark.asyncio
async def test_source_owned_prepolicy_receipt_gets_only_policy_cut(receipt_catalog):
    case = receipt_catalog
    await custody.provision_cms_serving_receipt_custody(
        case.driver, case.policy, migration_role=MIGRATION_ROLE, verify_role_profile=AsyncMock()
    )
    case.rows_receipt["classes"][0]["relrowsecurity"] = False
    case.rows_receipt["policies"] = None
    case.driver.execute.reset_mock()
    result = await custody.provision_cms_serving_receipt_custody(
        case.driver, case.policy, migration_role=MIGRATION_ROLE, verify_role_profile=AsyncMock()
    )
    statements = [call.args[0] for call in case.driver.execute.await_args_list]
    assert result["catalog"]["classes"][0]["relrowsecurity"] is True
    assert len(statements) == 4 and not any(
        "OWNER TO" in statement or statement.startswith("GRANT") for statement in statements
    )


def _add_inheriting_actor(case, *, can_login, bypass):
    role = deepcopy(next(entry for entry in case.graph["roles"] if entry["rolname"] == MIGRATION_ROLE))
    role.update(oid="22", rolname="inherited_publisher", rolcanlogin=can_login, rolbypassrls=bypass)
    case.graph["roles"].append(role)
    access = deepcopy(next(entry for entry in case.rows_receipt["access"] if entry["role"] == 19))
    access.update(role=22, builder_reach=True)
    case.rows_receipt["access"].append(access)


@pytest.mark.asyncio
@pytest.mark.parametrize("can_login", [False, True])
async def test_session_bound_policy_accepts_inherited_rights_without_membership_allowlist(receipt_catalog, can_login):
    case = receipt_catalog
    _add_inheriting_actor(case, can_login=can_login, bypass=False)
    with pytest.raises(ValueError, match="inherited_writer"):
        await custody.capture_cms_serving_receipt_custody(
            case.driver, case.policy, migration_role=MIGRATION_ROLE, legacy=True
        )
    result = await custody.provision_cms_serving_receipt_custody(
        case.driver, case.policy, migration_role=MIGRATION_ROLE, verify_role_profile=AsyncMock()
    )
    actor = next(entry for entry in result["catalog"]["access"] if entry["role"] == 22)
    assert actor["table"] == ["SELECT", "MAINTAIN"]
    assert actor["columns"][:2] == [[1, ["SELECT", "INSERT"]], [2, ["SELECT", "INSERT"]]]


@pytest.mark.asyncio
@pytest.mark.parametrize("can_login", [False, True])
async def test_inherited_bypassrls_actor_cannot_be_admitted_even_without_login(receipt_catalog, can_login):
    case = receipt_catalog
    _add_inheriting_actor(case, can_login=can_login, bypass=True)
    before = deepcopy(case.rows_receipt)
    with pytest.raises(ValueError, match="inherited_writer"):
        await custody.provision_cms_serving_receipt_custody(
            case.driver, case.policy, migration_role=MIGRATION_ROLE, verify_role_profile=AsyncMock()
        )
    assert case.rows_receipt == before
    assert not any("OWNER TO" in call.args[0] for call in case.driver.execute.await_args_list)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mutation",
    ["expression", "grouping", "dependency", "roles", "command", "restrictive", "relation", "extra", "raw_policy"],
)
async def test_exact_native_policy_contract_cannot_be_substituted(receipt_catalog, mutation):
    case = receipt_catalog
    receipt = await custody.provision_cms_serving_receipt_custody(
        case.driver, case.policy, migration_role=MIGRATION_ROLE, verify_role_profile=AsyncMock()
    )
    catalog = receipt["catalog"]
    root = catalog["classes"][0]
    proof = next(entry for entry in catalog["receipt_policies"] if entry["policy"]["polcmd"] == "a")
    raw = next(entry for entry in catalog["policies"] if entry["polcmd"] == "a")
    if mutation in {"expression", "grouping"}:
        proof["check_expression"] = "true" if mutation == "expression" else "(" + proof["check_expression"] + ")"
    elif mutation == "dependency":
        proof["native_dependencies"] = False
    elif mutation == "extra":
        catalog["policies"].append(deepcopy(raw))
    elif mutation == "raw_policy":
        proof["policy"]["polwithcheck"] = "substituted"
    else:
        key, value = {
            "roles": ("polroles", ["19"]),
            "command": ("polcmd", "*"),
            "restrictive": ("polpermissive", False),
            "relation": ("polrelid", "999"),
        }[mutation]
        raw[key] = proof["policy"][key] = value
    with pytest.raises(ValueError, match="cms_receipt_polic"):
        receipt_policy.require_receipt_policies(catalog, root, case.policy.control_runtime_roles[0])


@pytest.mark.asyncio
async def test_policy_failure_rolls_back_the_complete_admin_transition(receipt_catalog):
    case = receipt_catalog
    before = deepcopy(case.rows_receipt)
    execute = case.driver.execute.side_effect

    def substituted_policy(sql):
        execute(sql)
        if sql.startswith("CREATE POLICY " + receipt_policy.INSERT_POLICY):
            case.rows_receipt["policies"][-1]["polroles"] = ["19"]

    case.driver.execute.side_effect = substituted_policy
    with pytest.raises(ValueError, match="cms_receipt_policy_changed"):
        await custody.provision_cms_serving_receipt_custody(
            case.driver, case.policy, migration_role=MIGRATION_ROLE, verify_role_profile=AsyncMock()
        )
    assert case.rows_receipt == before


@pytest.mark.asyncio
@pytest.mark.parametrize("mutation", ["force", "roles", "extra"])
async def test_idempotent_replay_refuses_policy_drift_before_any_repair(receipt_catalog, mutation):
    case = receipt_catalog
    await custody.provision_cms_serving_receipt_custody(
        case.driver, case.policy, migration_role=MIGRATION_ROLE, verify_role_profile=AsyncMock()
    )
    if mutation == "force":
        case.rows_receipt["classes"][0]["relforcerowsecurity"] = True
    elif mutation == "roles":
        case.rows_receipt["policies"][-1]["polroles"] = ["19"]
    else:
        case.rows_receipt["policies"].append(deepcopy(case.rows_receipt["policies"][0]))
    before = deepcopy(case.rows_receipt)
    case.driver.execute.reset_mock()
    with pytest.raises(ValueError, match="cms_receipt_(layout|polic)"):
        await custody.provision_cms_serving_receipt_custody(
            case.driver, case.policy, migration_role=MIGRATION_ROLE, verify_role_profile=AsyncMock()
        )
    assert case.rows_receipt == before and case.driver.execute.await_count == 1


def test_policy_shared_dependencies_are_scoped_to_the_current_database():
    assert "d.dbid=(SELECT oid FROM pg_database WHERE datname=current_database())" in receipt_policy.POLICY_SQL
