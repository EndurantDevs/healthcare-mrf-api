# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fixed parent rendering stays separate from execution and shared transition authority."""

from copy import deepcopy
from types import SimpleNamespace
from uuid import uuid4

import pytest

from process import provider_directory_cms_snapshot_parents as parents
from process.provider_directory_source_storage_binding import FHIRSourceStorageBinding
from tests.test_provider_directory_cms_prepared_context_postgres import native_case


@pytest.mark.parametrize("root", parents.workspace.storage.RELATIONS)
def test_empty_parent_statements_preserve_checks_and_use_distinct_private_indexes(root):
    statements = parents.cms_snapshot_parent_statements("future_storage", root, "prepared_parent")
    assert statements[0].startswith('CREATE TABLE "future_storage"."prepared_parent"(')
    assert statements[0].endswith("PARTITION BY LIST(dataset_id)")
    assert 'CONSTRAINT "prepared_parent_pkey" PRIMARY KEY' in statements[0]
    assert all(statement.startswith(("CREATE TABLE ", "CREATE INDEX ")) for statement in statements)
    assert "FOREIGN KEY" not in " ".join(statements) and "TRIGGER" not in " ".join(statements)
    for name, kind, definition, _columns in parents.workspace._LAYOUTS[root]["constraints"]:
        if kind == "c":
            expected = (
                "CHECK (resolution_status IN ('resolved', 'unresolved', 'ambiguous'))"
                if name == "cms_npd_relationship_status_check"
                else definition
            )
            assert 'CONSTRAINT "' + name + '" ' + expected in statements[0]


@pytest.mark.parametrize("schema,name", [("bad;schema", "parent"), ("future", "bad name"), ("future", "a" * 63)])
def test_invalid_identifiers_refuse_before_any_execution(schema, name):
    with pytest.raises(ValueError, match="identifier_invalid"):
        parents.cms_snapshot_parent_statements(schema, parents.workspace.storage.RELATIONSHIP, name)


def test_unknown_root_refuses():
    with pytest.raises(ValueError, match="root_required"):
        parents.cms_snapshot_parent_statements("future_storage", "unrelated", "parent")


def test_temporary_parent_names_bind_one_preparation_and_fixed_family():
    preparation = str(uuid4())
    roots = parents.workspace.storage.RELATIONS[-2:]
    names = [parents.cms_snapshot_parent_name(preparation, root) for root in roots]
    assert len(set(names)) == 2 and all(len(name + "_pkey") < 63 for name in names)
    assert names == [parents.cms_snapshot_parent_name(preparation, root) for root in roots]
    assert names[0] != parents.cms_snapshot_parent_name(str(uuid4()), roots[0])


@pytest.mark.parametrize("preparation,root", [(None, "unrelated"), ("invalid", "unrelated"), ("0" * 32, "unrelated")])
def test_invalid_preparation_or_family_cannot_name_a_future_parent(preparation, root):
    with pytest.raises(ValueError):
        parents.cms_snapshot_parent_name(preparation, root)


def _readback_binding():
    return FHIRSourceStorageBinding("catalog", "private_source", 1, 2, 3, 4, 6, "a" * 64)


def _parent_inventory(binding, preparation):
    inventory, roots, extras = [], {}, {}
    for ordinal, root in enumerate(parents.workspace.storage.RELATIONS[-2:]):
        name = parents.cms_snapshot_parent_name(preparation, root)
        oid = 100 + ordinal
        inventory.append(
            {
                "oid": oid,
                "nspname": binding.catalog_schema,
                "relname": name,
                "relowner": binding.namespace_owner_oid,
                "relkind": "p",
                "relpersistence": "p",
                "native": {
                    "namespace": [2, "catalog", 6, None],
                    "relation": [oid, name, "p", "p", 6, False, False, False, 0, None, None, None, "LIST (dataset_id)"],
                    "parents": [],
                    "triggers": [],
                    "rules": [],
                    "indexes": [{"class": [oid + 10, 6, None, None]}],
                },
            }
        )
        roots[oid] = {
            "oid": oid,
            "relname": name,
            "relkind": "p",
            "amname": None,
            "relreplident": "d",
            **{
                field: False
                for field in (
                    "relispartition",
                    "relrowsecurity",
                    "relforcerowsecurity",
                    "inherited",
                    "dropped",
                    "rules",
                    "policies",
                )
            },
        }
        extras[oid] = {
            "oid": oid,
            "owner_protected": True,
            "acl_closed": True,
            "no_policies": True,
            "default_storage": True,
            "index_topology": [[oid + 10, [], 403, "p", 0]],
        }
    return inventory, roots, extras


def _readback_connection(case):
    async def fetchrow(query, *_args):
        assert query == parents._ENVIRONMENT_SQL
        return deepcopy(case.environment)

    async def fetch(query, oids, *_args):
        if query == parents._EXTRA_SQL:
            return list(case.extras.values())
        if query == parents.layout._ROOTS_SQL:
            return list(case.roots.values())
        assert query in {parents.layout._COLUMNS_SQL, parents.layout._CONSTRAINTS_SQL, parents.layout._INDEXES_SQL}
        return [(query, oids[0])]

    async def fetchval(query, *_args):
        return {
            parents._FOREIGN_SQL: case.foreign,
            parents._PARTITIONS_SQL: case.partitions,
            parents.layout._DEPENDENCIES_SQL: case.dependencies,
        }[query]

    return SimpleNamespace(
        is_in_transaction=lambda: case.transaction,
        is_closed=lambda: case.closed,
        fetchrow=fetchrow,
        fetch=fetch,
        fetchval=fetchval,
    )


@pytest.fixture
def readback_case(monkeypatch):
    binding, preparation = _readback_binding(), str(uuid4())
    inventory, roots, extras = _parent_inventory(binding, preparation)
    case = SimpleNamespace(
        binding=binding,
        preparation=preparation,
        inventory=inventory,
        roots=roots,
        extras=extras,
        transaction=True,
        closed=False,
        foreign=True,
        dependencies=True,
        partitions=True,
        reads=0,
        drift=False,
        layout_calls=[],
        environment={
            "version": 180002,
            "search_path": "pg_catalog",
            "database_oid": 1,
            "schema_oid": 2,
            "namespace_owner_oid": 6,
            "schema_sql": "catalog",
        },
    )

    async def inventory_read(connection, schema, names):
        assert connection is case.connection and schema == "catalog"
        assert set(names) == {entry["relname"] for entry in case.inventory[:2]}
        case.reads += 1
        result = deepcopy(case.inventory)
        if case.drift and case.reads == 2:
            result[0]["relowner"] += 1
        return result

    case.connection = _readback_connection(case)
    monkeypatch.setattr(parents.custody, "_inventory", inventory_read)
    for method in ("_require_columns", "_require_constraints", "_require_indexes"):

        def checked(*args, method=method):
            case.layout_calls.append((method, args))

        monkeypatch.setattr(parents.layout, method, checked)
    return case


@pytest.mark.asyncio
@pytest.mark.parametrize("search_path", ["pg_catalog", "pg_catalog, pg_temp"])
async def test_readback_returns_exact_inventory_and_calls_fixed_layout_verifiers(readback_case, search_path):
    case = readback_case
    case.environment["search_path"] = search_path
    result = await parents.read_cms_snapshot_parents(case.connection, case.binding, case.preparation)
    assert result == dict(zip(parents.workspace.storage.RELATIONS[-2:], case.inventory, strict=True))
    assert case.reads == 2 and len(case.layout_calls) == 6
    for root, entry in result.items():
        calls = [args for _method, args in case.layout_calls if root in args]
        assert len(calls) == 3
        constraints = next(
            args[1] for method, args in case.layout_calls if method == "_require_constraints" and args[3] == root
        )
        assert all(kind != "f" for _name, kind, _definition, _columns in constraints)
        assert any(name == entry["relname"] + "_pkey" for name, *_rest in constraints)


@pytest.mark.asyncio
async def test_readback_rejects_untyped_binding_and_invalid_preparation_before_catalog_io(readback_case):
    case = readback_case
    with pytest.raises(ValueError, match="source_binding_required"):
        await parents.read_cms_snapshot_parents(case.connection, object(), case.preparation)
    with pytest.raises(ValueError):
        await parents.read_cms_snapshot_parents(case.connection, case.binding, "invalid")
    assert case.reads == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(("field", "value"), [("transaction", False), ("closed", True)])
async def test_readback_requires_open_explicit_transaction_before_catalog_io(readback_case, field, value):
    case = readback_case
    setattr(case, field, value)
    with pytest.raises(ValueError, match="transaction_required"):
        await parents.read_cms_snapshot_parents(case.connection, case.binding, case.preparation)
    assert case.reads == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("version", 170000),
        ("search_path", "public,pg_catalog"),
        ("database_oid", 7),
        ("schema_oid", 7),
        ("namespace_owner_oid", 7),
    ],
)
async def test_readback_rejects_wrong_environment_or_coordinates(readback_case, field, value):
    case = readback_case
    case.environment[field] = value
    with pytest.raises(ValueError, match="catalog_changed"):
        await parents.read_cms_snapshot_parents(case.connection, case.binding, case.preparation)
    assert case.reads == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["foreign", "dependencies", "partitions", "drift"])
async def test_readback_rejects_foreign_edges_dependencies_defaults_and_drift(readback_case, field):
    case = readback_case
    setattr(case, field, field == "drift")
    with pytest.raises(ValueError):
        await parents.read_cms_snapshot_parents(case.connection, case.binding, case.preparation)


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["owner_protected", "acl_closed", "no_policies", "default_storage"])
async def test_readback_requires_real_owner_and_native_acl_storage_checks(readback_case, field):
    case = readback_case
    case.extras[100][field] = False
    with pytest.raises(ValueError, match="parent_custody_changed"):
        await parents.read_cms_snapshot_parents(case.connection, case.binding, case.preparation)


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["parents", "triggers", "rules"])
async def test_readback_refuses_existing_parent_ancestry_hooks_and_rules(readback_case, field):
    case = readback_case
    case.inventory[0]["native"][field] = [123]
    with pytest.raises(ValueError, match="parent_changed"):
        await parents.read_cms_snapshot_parents(case.connection, case.binding, case.preparation)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field", ["relispartition", "relrowsecurity", "relforcerowsecurity", "inherited", "dropped", "rules", "policies"]
)
async def test_readback_rejects_nonfixed_native_parent_flags(readback_case, field):
    case = readback_case
    case.roots[100][field] = True
    with pytest.raises(ValueError, match="parent_native_changed"):
        await parents.read_cms_snapshot_parents(case.connection, case.binding, case.preparation)


@pytest.mark.asyncio
async def test_readback_requires_exact_protected_parent_owner(readback_case):
    case = readback_case
    case.inventory[0]["relowner"] = case.binding.owner_oid
    with pytest.raises(ValueError, match="parent_changed"):
        await parents.read_cms_snapshot_parents(case.connection, case.binding, case.preparation)


@pytest.mark.asyncio
async def test_readback_refuses_descendants_and_index_owner_drift(readback_case):
    case = readback_case
    case.inventory.append(deepcopy(case.inventory[0]))
    with pytest.raises(ValueError, match="descendants_changed"):
        await parents.read_cms_snapshot_parents(case.connection, case.binding, case.preparation)
    case.inventory.pop()
    case.inventory[0]["native"]["indexes"][0]["class"][1] += 1
    with pytest.raises(ValueError, match="index_custody_changed"):
        await parents.read_cms_snapshot_parents(case.connection, case.binding, case.preparation)


@pytest.mark.asyncio
async def test_readback_propagates_fixed_layout_refusal(readback_case, monkeypatch):
    case = readback_case

    def reject(*_args):
        raise ValueError("cms_canonical_layout_index_native")

    monkeypatch.setattr(parents.layout, "_require_indexes", reject)
    with pytest.raises(ValueError, match="cms_canonical_layout_index_native"):
        await parents.read_cms_snapshot_parents(case.connection, case.binding, case.preparation)


@pytest.mark.asyncio
@pytest.mark.parametrize("drift", [None, "index", "grant", "child", "owner"])
async def test_native_prep_derived_parent_readback(native_case, drift):
    """Native coordinates/layout only; the fixture issues no Source/profile or creation authority."""
    case, preparation = native_case, str(uuid4())
    async with case.admin.transaction():
        await case.admin.execute("SET LOCAL search_path=pg_catalog")
        await case.admin.execute("SET LOCAL statement_timeout='5s'")
        names_by_root = {
            root: parents.cms_snapshot_parent_name(preparation, root)
            for root in parents.workspace.storage.RELATIONS[-2:]
        }
        for root, name in names_by_root.items():
            for statement in parents.cms_snapshot_parent_statements(case.storage.catalog_schema, root, name):
                await case.admin.execute(statement)
            await case.admin.execute(
                f'ALTER TABLE "{case.storage.catalog_schema}"."{name}" OWNER TO "{case.roles["namespace"]}"'
            )
        observed = await parents.read_cms_snapshot_parents(case.admin, case.storage, preparation)
        assert observed == await parents.read_cms_snapshot_parents(case.admin, case.storage, preparation)
        assert {entry["relname"] for entry in observed.values()} == set(names_by_root.values())
        if drift is None:
            return
        relation = '"' + case.storage.catalog_schema + '"."' + next(iter(names_by_root.values())) + '"'
        statements_by_drift = {
            "index": f"CREATE INDEX unexpected_parent_idx ON {relation}(dataset_id)",
            "grant": f'GRANT SELECT ON {relation} TO "{case.roles["source_publisher"]}"',
            "child": f"CREATE TABLE \"{case.storage.catalog_schema}\".unexpected_child PARTITION OF {relation} FOR VALUES IN ('other')",
            "owner": f'ALTER TABLE {relation} OWNER TO "{case.roles["source_publisher"]}"',
        }
        await case.admin.execute(statements_by_drift[drift])
        with pytest.raises(ValueError):
            await parents.read_cms_snapshot_parents(case.admin, case.storage, preparation)
