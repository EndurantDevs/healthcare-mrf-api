# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""An installed native counter needs full catalog and effective-role proof."""

from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_native_counter_catalog as counter
from process import provider_directory_source_callable_custody as source
from process.provider_directory_cms_native_inputs import _advance_body
from tests.test_cms_native_input_registration import NativeProtocol
from tests.test_provider_directory_source_callable_custody import _entry, _roles

_CONSTRAINTS = [
    ("p", 1, "PRIMARY KEY (relation_oid)"),
    ("c", 1, "CHECK (((relation_oid >= 1) AND (relation_oid <= '4294967295'::bigint))))"),
    ("c", 4, "CHECK ((revision >= 0))"),
    ("n", 1, "NOT NULL relation_oid"),
    ("n", 2, "NOT NULL schema_name"),
    ("n", 3, "NOT NULL table_name"),
    ("n", 4, "NOT NULL revision"),
]

_COLUMNS = (
    ("relation_oid", 20, 0),
    ("schema_name", 19, 950),
    ("table_name", 19, 950),
    ("revision", 20, 0),
    ("mutation_xid", 5069, 0),
)


def native_layout(owner):
    """Declare the exact current counter layout with a nullable full transaction stamp."""
    catalog_by_field = dict(
        classes=[
            dict(oid="11", relname=counter.ROOT, relowner=str(owner), relacl=None, relnamespace="201", reltype="13"),
            dict(oid="12", relname="native_pk", relowner=str(owner), relacl=None),
        ],
        attributes=[dict(attrelid="11", attnum=number, attacl=None) for number in range(1, 6)],
        table_acl=[[owner, owner, right, False] for right in sorted(counter._RIGHTS)],
        column_acl=[],
        access=[],
        triggers=[],
    )
    catalog_by_field["classes"][0].update(
        relkind="r",
        relpersistence="p",
        relam="2",
        reloptions=None,
        relreplident="d",
        relispartition=False,
        relrowsecurity=False,
        relforcerowsecurity=False,
    )
    catalog_by_field.update(rules=None, policies=None, inheritance=None)
    for attribute, (name, kind, collation) in zip(
        catalog_by_field["attributes"],
        _COLUMNS,
        strict=True,
    ):
        attribute.update(
            attname=name,
            atttypid=str(kind),
            attcollation=str(collation),
            attnotnull=name != "mutation_xid",
            attisdropped=False,
            attidentity="",
            attgenerated="",
            atthasmissing=False,
            attmissingval=None,
            default_expression="0" if name == "revision" else "",
        )
    catalog_by_field["constraints"] = [
        dict(
            contype=kind,
            conkey=[key],
            convalidated=True,
            conenforced=True,
            condeferrable=False,
            condeferred=False,
            conislocal=True,
            coninhcount=0,
            conparentid="0",
            connoinherit=kind == "p",
            constraint_definition=definition,
        )
        for kind, key, definition in _CONSTRAINTS
    ]
    catalog_by_field["indexes"] = native_indexes()
    return catalog_by_field


def native_indexes():
    """Use the original native primary-index catalog encoding."""
    return [
        dict(
            indrelid="11",
            indkey=[1],
            indisprimary=True,
            indisunique=True,
            native=True,
            method="btree",
            indisvalid=True,
            indisready=True,
            indislive=True,
            indimmediate=True,
            indisexclusion=False,
            indisclustered=False,
            indisreplident=False,
            indnullsnotdistinct=False,
            indnatts=1,
            indnkeyatts=1,
            indoption=[0],
            index_expressions="",
            index_predicate="",
        )
    ]


def effective_rows(catalog, functions_by_name, owner):
    access = []
    for role in range(101, 107):
        rights = counter._RIGHTS if role == owner else {"SELECT"} if role == 103 else set()
        catalog["access"].append(
            dict(
                role=role,
                superuser=False,
                builtin=False,
                owner_usage=role == 101,
                builder_usage=role == 103,
                table=sorted(rights),
                columns=[
                    [
                        n,
                        sorted(
                            (rights & {"INSERT", "SELECT", "UPDATE", "REFERENCES"})
                            | ({"INSERT"} if role == 103 and n <= 3 else set())
                        ),
                    ]
                    for n in (1, 2, 3, 4, 5)
                ],
            )
        )
        for name, entry in functions_by_name.items():
            access.append(
                dict(
                    function_oid=int(entry["definition"]["oid"]),
                    role=role,
                    rolsuper=False,
                    owner_usage=role == owner,
                    builder_usage=role == 103,
                    can_execute=name == counter.FUNCTIONS[1] or role in {owner, 103},
                )
            )
    return access


def native_snapshot(owner):
    catalog = native_layout(owner)
    if owner != 103:
        catalog["table_acl"].append([103, owner, "SELECT", False])
        catalog["column_acl"] = [[n, 103, owner, "INSERT", False] for n in (1, 2, 3)]
    functions_by_name = {}
    for oid, name in enumerate(counter.FUNCTIONS, 40):
        entry = _entry("prepare_pd_uhc_flex_practitioner_stage()", owner)
        entry["signature"] = f'"sample"."{name}"()'
        entry["definition"].update(
            oid=str(oid),
            proname=name,
            prolang="99",
            proargtypes=[],
            proargnames=None,
            prorettype="2279",
            pronargs=0,
            proargmodes=None,
            proallargtypes=None,
            prosrc=_advance_body("sample") if name == counter.FUNCTIONS[0] else counter._GUARD,
            prosecdef=name == counter.FUNCTIONS[0],
        )
        entry["acl"] = [[owner, owner, "EXECUTE", False]]
        if name == counter.FUNCTIONS[1]:
            entry["acl"].append([0, owner, "EXECUTE", False])
        elif owner != 103:
            entry["acl"].append([103, owner, "EXECUTE", False])
        functions_by_name[name] = entry
    catalog["triggers"] = [
        dict(
            tgname=name,
            tgtype=kind,
            tgenabled="A",
            tgisinternal=False,
            tgfoid="41",
            tgnargs=0,
            tgqual=None,
            tgconstraint="0",
            tgdeferrable=False,
            tginitdeferred=False,
        )
        for name, kind in [("cms_native_input_revision_immutable", 27), ("cms_native_input_revision_no_truncate", 34)]
    ]
    access = effective_rows(catalog, functions_by_name, owner)
    return catalog, functions_by_name, access


class CounterProtocol(NativeProtocol):
    """Return native-shaped metadata, refusing every non-catalog operation."""

    def __init__(self, owner=101, *, absent=False):
        super().__init__()
        self.catalog, self.functions, self.access = native_snapshot(owner)
        self.census = (
            dict(relation_oid=None, function_oids=[]) if absent else dict(relation_oid=11, function_oids=[40, 41])
        )
        self.ledger.update(ledger_oid=11, owner_oid=owner)
        self.function.update(function_oid=40, owner_oid=owner, prosrc=_advance_body("sample"))
        self.identity = dict(session="admin", current="admin", backend=7, xid="9", administrator=True)
        self.composite = dict(oid="13", typtype="c", typowner=str(owner), typrelid="11", typnamespace="201")

    async def fetchrow(self, sql, *args):
        if sql in (counter._CENSUS_SQL, source._IDENTITY_SQL):
            self.calls.append(("fetchrow", sql, args))
            return deepcopy(self.census if sql == counter._CENSUS_SQL else self.identity)
        return await super().fetchrow(sql, *args)

    async def fetchval(self, sql, *args):
        from process.provider_directory_cms_control_custody import _NATIVE_DEPENDENCIES_SQL, _TYPE_SQL
        from process.provider_directory_cms_receipt_custody import _CATALOG_SQL

        self.calls.append(("fetchval", sql, args))
        if sql == _NATIVE_DEPENDENCIES_SQL:
            return True
        if sql == _TYPE_SQL:
            return deepcopy(self.composite)
        if sql == _CATALOG_SQL.replace(
            "'builder_reach',", "'builder_usage',pg_has_role(p.oid,$3::oid,'USAGE'),'builder_reach',"
        ):
            return deepcopy(self.catalog)
        if sql == "SELECT oid FROM pg_language WHERE lanname='plpgsql'":
            return 99
        raise AssertionError(sql)

    async def fetch(self, sql, *args):
        if sql in (counter._ACCESS_SQL, source._FUNCTION_SQL):
            self.calls.append(("fetch", sql, args))
            return deepcopy(self.access if sql == counter._ACCESS_SQL else list(self.functions.values()))
        return await super().fetch(sql, *args)

    async def execute(self, sql, *args):
        raise AssertionError("catalog capability must not mutate or lock")


def policy():
    return source.GenericSourcePolicy("sample", "owner", "publisher", ("builder", "reader"), ("builder",))


async def capability(native):
    return await counter._counter_callable_capability(
        native,
        policy(),
        _roles(),
        dict(oid=201, identifier="sample"),
        dict(roles=[dict(oid=str(role)) for role in range(101, 107)]),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("owner,expected", [(101, (40,)), (103, ())])
async def test_only_completed_full_native_counter_adds_authority(owner, expected):
    native = CounterProtocol(owner)
    assert await capability(native) == expected
    assert all(kind != "execute" for kind, *_ in native.calls)
    assert sum(sql == counter._CENSUS_SQL for _, sql, _ in native.calls) == 2


@pytest.mark.asyncio
async def test_whole_absence_is_valid_for_unrelated_sources():
    native = CounterProtocol(absent=True)
    assert await capability(native) == ()
    assert [sql for _, sql, _ in native.calls] == [counter._CENSUS_SQL]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change",
    [
        "partial",
        "overload",
        "namespace",
        "owner",
        "body",
        "native_attribute",
        "extra_trigger",
        "constraint",
        "grantable",
        "public_execute",
        "effective_execute",
        "effective_write",
        "missing_role",
        "guard",
    ],
)
async def test_installed_counter_never_uses_a_name_only_exemption(change):
    native = CounterProtocol()
    if change == "partial":
        native.census["relation_oid"] = None
    if change == "overload":
        native.census["function_oids"].append(42)
    if change == "namespace":
        native.catalog["classes"][0]["relnamespace"] = "202"
    if change == "owner":
        native.functions[counter.FUNCTIONS[0]]["definition"]["proowner"] = "103"
    if change in {"body", "native_attribute"}:
        native.functions[counter.FUNCTIONS[0]]["definition"]["prosrc" if change == "body" else "proargdefaults"] = (
            "changed"
        )
    if change == "extra_trigger":
        native.catalog["triggers"].append(dict(native.catalog["triggers"][0], tgname="unknown"))
    if change == "constraint":
        native.catalog["constraints"].pop()
    if change == "grantable":
        native.functions[counter.FUNCTIONS[0]]["acl"][0][-1] = True
    if change == "public_execute":
        native.functions[counter.FUNCTIONS[0]]["acl"].append([0, 101, "EXECUTE", False])
    if change == "effective_execute":
        next(
            access_row for access_row in native.access if access_row["role"] == 106 and access_row["function_oid"] == 40
        )["can_execute"] = True
    if change == "effective_write":
        native.catalog["access"][-1]["columns"][0][1].append("INSERT")
    if change == "missing_role":
        native.access.pop()
    if change == "guard":
        native.ledger_guards[0]["enabled"] = "O"
    with pytest.raises((counter.NativeCounterCatalogError, RuntimeError, ValueError)):
        await capability(native)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change",
    [
        "legacy_four_columns",
        "type",
        "not_null",
        "default",
        "has_missing",
        "missing_value",
        "collation",
        "identity",
        "generated",
        "extra_column",
        "insert",
        "reader",
    ],
)
async def test_transaction_stamp_requires_exact_shape_and_closed_rights(change):
    """The new stamp has one nullable native type and no additional writer or reader grants."""
    native = CounterProtocol()
    attribute = native.catalog["attributes"][-1]
    if change == "legacy_four_columns":
        native.catalog["attributes"].pop()
    if change in {"type", "not_null", "default", "has_missing", "missing_value", "collation", "identity", "generated"}:
        key, value = {
            "type": ("atttypid", "20"),
            "not_null": ("attnotnull", True),
            "default": ("default_expression", "pg_current_xact_id()"),
            "has_missing": ("atthasmissing", True),
            "missing_value": ("attmissingval", ["17"]),
            "collation": ("attcollation", "950"),
            "identity": ("attidentity", "a"),
            "generated": ("attgenerated", "s"),
        }[change]
        attribute[key] = value
    if change == "extra_column":
        native.catalog["attributes"].append(dict(attribute, attnum=6, attname="unexpected"))
    if change == "insert":
        native.catalog["column_acl"].append([5, 103, 101, "INSERT", False])
    if change == "reader":
        native.catalog["access"][-1]["columns"][-1][1].append("SELECT")
    with pytest.raises(counter.NativeCounterCatalogError):
        await capability(native)


@pytest.mark.asyncio
@pytest.mark.parametrize("legacy", [False, True])
async def test_capability_composes_counter_without_changing_custody_receipt(monkeypatch, legacy):
    receipt_by_field = dict(
        schema="sample",
        schema_oid=201,
        roles=_roles(),
        role_graph=dict(roles=[dict(oid=str(role)) for role in range(101, 107)]),
        functions={"fixed": _entry("prepare_pd_uhc_flex_practitioner_stage()", 101)},
    )
    original = deepcopy(receipt_by_field)
    monkeypatch.setattr(source, "capture_source_callable_custody", AsyncMock(return_value=receipt_by_field))
    native = CounterProtocol()
    capture = (
        source.capture_source_callable_adoption_capability if legacy else source.capture_source_callable_capability
    )
    assert await capture(native, policy(), migration_role="migration", namespace_owner_role="namespace") == (40, 501)
    assert receipt_by_field == original and "native_counter" not in receipt_by_field
    assert sum(sql == source._IDENTITY_SQL for _, sql, _ in native.calls) == 2


@pytest.mark.asyncio
async def test_counter_proof_is_inside_original_identity_bracket(monkeypatch):
    native = CounterProtocol()
    receipt_by_field = dict(
        schema="sample",
        schema_oid=201,
        roles=_roles(),
        role_graph=dict(roles=[dict(oid=str(role)) for role in range(101, 107)]),
        functions={},
    )
    monkeypatch.setattr(source, "capture_source_callable_custody", AsyncMock(return_value=receipt_by_field))
    fetch = native.fetch

    async def changed(sql, *args):
        result = await fetch(sql, *args)
        if sql == counter._ACCESS_SQL:
            native.identity["backend"] = 8
        return result

    native.fetch = changed
    with pytest.raises(ValueError, match="transaction_changed"):
        await source.capture_source_callable_capability(
            native, policy(), migration_role="migration", namespace_owner_role="namespace"
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid", ["policy", "transaction"])
async def test_capability_validates_before_catalog_queries(invalid):
    native = CounterProtocol()
    if invalid == "transaction":
        native.active = False
    with pytest.raises(ValueError, match="policy_required"):
        await source.capture_source_callable_capability(
            native,
            object() if invalid == "policy" else policy(),
            migration_role="migration",
            namespace_owner_role="namespace",
        )
    assert not native.calls
