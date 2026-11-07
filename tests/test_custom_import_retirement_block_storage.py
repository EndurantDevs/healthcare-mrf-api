# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Retirement resolves actual layouts and validates bounded compressed references."""

from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from db.models.custom_import import CustomImportDefinitionRevision
from db.models.custom_import_family_block import CustomImportFamilyBlockLayout, CustomImportFamilyBlockRelation
from db.models.custom_import_storage import CustomImportSnapshotFamily, CustomImportSnapshotRelation
from process.custom_import import retirement
from process.custom_import import retirement_storage as storage
from process.custom_import.family_block_binding import checked_binding
from process.custom_import.family_block_codec import FamilyBlockError
from process.custom_import.family_block_directory import iter_revision_memberships
from process.custom_import.family_block_layout import FAMILY_BLOCK_LAYOUT_CONTRACT, ROLE_SPECS
from tests.test_custom_import_family_block_storage import _catalog, _layout


class _Result:
    def __init__(self, rows):
        self.rows = rows

    def all(self):
        return self.rows

    def mappings(self):
        return self

    def one(self):
        assert len(self.rows) == 1
        return self.rows[0]


def _block_fixture():
    layout = _layout()
    family = SimpleNamespace(family_id=11, dataset_id=7, definition_revision_id=17, schema_revision_id=13)
    descriptor = CustomImportFamilyBlockLayout(
        family_id=11,
        layout_contract=FAMILY_BLOCK_LAYOUT_CONTRACT,
        definition_sha256=bytes.fromhex(layout.definition.digest),
        layout_sha256=layout.digest,
        collection_slots=dict(layout.collection_slots),
        phase="loading",
    )
    revision = CustomImportDefinitionRevision(
        dataset_id=7,
        schema_revision_id=13,
        canonical_definition=layout.definition.canonical,
        definition_sha256=bytes.fromhex(layout.definition.digest),
    )
    binding = checked_binding(
        11, layout.definition, layout.collection_slots, layout.comment, _catalog(layout), persistence="candidate"
    )
    return SimpleNamespace(layout=layout, family=family, descriptor=descriptor, revision=revision, binding=binding)


async def test_legacy_retirement_never_reads_additive_block_tables():
    session = SimpleNamespace(
        execute=AsyncMock(return_value=_Result([(slot, 1000 + slot) for slot in range(1, 16)])),
        get=AsyncMock(side_effect=AssertionError("legacy schema has no block tables")),
    )
    resolved = await storage.resolve_storage(session, '"synthetic"', SimpleNamespace(family_id=11))
    assert not resolved.is_block and resolved.registry_model is CustomImportSnapshotRelation
    assert len(resolved.tables_by_name) == 15
    session.get.assert_not_awaited()


async def test_block_retirement_reuses_original_typed_native_binding(monkeypatch):
    case = _block_fixture()
    session = SimpleNamespace(
        execute=AsyncMock(return_value=_Result([(slot, slot + 1000) for slot, _ in ROLE_SPECS.values()])),
        get=AsyncMock(side_effect=[case.descriptor, case.revision]),
    )
    native = AsyncMock(return_value=case.binding)
    monkeypatch.setattr(storage, "resolve_family_block_binding", native)
    resolved = await storage.resolve_storage(session, '"synthetic"', case.family)
    assert resolved.is_block and resolved.registry_model is CustomImportFamilyBlockRelation
    assert set(resolved.tables_by_name) == {name for _, name in ROLE_SPECS.values()}
    assert native.await_args.kwargs == {"persistence": "candidate"}
    assert "custom_import_block_revision_run" in storage.reference_tables(resolved)
    assert storage.reference_columns(resolved, case.binding.tables_by_role["family"]) == {"root_revision_id": 1}
    assert storage.reference_columns(resolved, case.binding.tables_by_role["query_record"]) == {"child_revision_id": 2}


@pytest.mark.parametrize(
    "problem", ["missing_descriptor", "foreign_dataset", "definition_hash", "layout_hash", "missing_binding"]
)
async def test_block_retirement_refuses_changed_native_scope(monkeypatch, problem):
    case = _block_fixture()
    if problem == "missing_descriptor":
        case.descriptor = None
    elif problem == "foreign_dataset":
        case.revision.dataset_id += 1
    elif problem == "definition_hash":
        case.descriptor.definition_sha256 = b"f" * 32
    elif problem == "layout_hash":
        case.descriptor.layout_sha256 = b"f" * 32
    else:
        case.binding = None
    session = SimpleNamespace(
        execute=AsyncMock(return_value=_Result([(slot, slot + 1000) for slot, _ in ROLE_SPECS.values()])),
        get=AsyncMock(side_effect=[case.descriptor, case.revision]),
    )
    monkeypatch.setattr(storage, "resolve_family_block_binding", AsyncMock(return_value=case.binding))
    with pytest.raises(retirement.SnapshotRetirementRefused):
        await storage.resolve_storage(session, '"synthetic"', case.family)


def _sparse_row():
    membership = tuple(iter_revision_memberships(7, [(4, 2), (8, 1)], chunk_records=3))[0]
    return dict(
        zip(
            (
                "collection_slot",
                "first_revision_id",
                "last_revision_id",
                "record_count",
                "first_record_ordinal",
                "encoded_membership",
            ),
            membership.cells(),
            strict=True,
        ),
        family_revision_id=31,
    )


class _MembershipSession:
    def __init__(self, row, *, retained_ids=()):
        self.row = row
        self.retained_ids = set(retained_ids)
        self.statements = []
        self.pages = 0
        self.probes = []

    async def execute(self, statement):
        self.statements.append(statement)
        if "encoded_bytes" in statement.selected_columns.keys():
            self.pages += 1
            if self.pages > 1:
                return _Result([])
            return _Result(
                [
                    {name: self.row[name] for name in storage._MEMBERSHIP_KEYS}
                    | {"encoded_bytes": len(self.row["encoded_membership"])}
                ]
            )
        return _Result([deepcopy(self.row)])

    async def scalar(self, statement, parameters=None):
        if parameters is None:
            self.statements.append(statement)
            return False
        self.probes.append(deepcopy(parameters))
        return bool(set(parameters["ids"]) & self.retained_ids)


@pytest.mark.parametrize("retained,refused", [((), False), ((4,), True), ((8,), True), ((6,), False)])
async def test_sparse_reference_census_uses_actual_ids_and_exact_eof(retained, refused):
    case = _block_fixture()
    resolved = storage.RetirementStorage(
        {table.name: table for table in case.binding.tables_by_role.values()}, case.binding
    )
    session = _MembershipSession(_sparse_row(), retained_ids=retained)
    if refused:
        with pytest.raises(retirement.SnapshotRetirementRefused, match="sparse"):
            await storage.assert_no_membership_references(session, '"synthetic"', resolved, 99)
    else:
        await storage.assert_no_membership_references(session, '"synthetic"', resolved, 99)
    assert session.probes == [{"ids": [4, 8], "family": 99, "kind": 2}]


@pytest.mark.parametrize("change", ["trailing", "bounds", "truncated"])
async def test_sparse_reference_census_rejects_malformed_membership_before_ddl(change):
    row = _sparse_row()
    if change == "trailing":
        row["encoded_membership"] += b"\x00"
    elif change == "bounds":
        row["last_revision_id"] += 1
    else:
        row["encoded_membership"] = row["encoded_membership"][:-1]
    case = _block_fixture()
    resolved = storage.RetirementStorage(
        {table.name: table for table in case.binding.tables_by_role.values()}, case.binding
    )
    session = _MembershipSession(row)
    with pytest.raises(FamilyBlockError):
        await storage.assert_no_membership_references(session, '"synthetic"', resolved, 99)
    assert not session.probes
    assert all("DROP" not in str(statement) for statement in session.statements)


async def test_sparse_census_respects_native_id_batch_budget(monkeypatch):
    case = _block_fixture()
    resolved = storage.RetirementStorage(
        {table.name: table for table in case.binding.tables_by_role.values()}, case.binding
    )
    session = _MembershipSession(_sparse_row())
    monkeypatch.setattr(storage, "MAX_MEMBERSHIP_IDS", 1)
    await storage.assert_no_membership_references(session, '"synthetic"', resolved, 99)
    assert [probe["ids"] for probe in session.probes] == [[4], [8]]
    assert session.pages == 2


async def test_sparse_census_refuses_oversize_before_fetching_bytes(monkeypatch):
    table = _block_fixture().binding.tables_by_role["revision_run"]
    session = _MembershipSession(_sparse_row())
    monkeypatch.setattr(storage, "MAX_MEMBERSHIP_PAGE_BYTES", 257)
    with pytest.raises(retirement.SnapshotRetirementRefused, match="memory"):
        await storage._membership_page(session, table, None)
    assert len(session.statements) == 1


@pytest.mark.parametrize("installed", [(None, None), (11, 12), (11, None)])
async def test_final_census_closes_exact_available_native_registries(installed):
    session = SimpleNamespace(execute=AsyncMock(return_value=_Result([installed])))
    if installed == (11, None):
        with pytest.raises(retirement.SnapshotRetirementRefused, match="incomplete"):
            await retirement._registry_targets(session, '"synthetic"')
    else:
        targets = await retirement._registry_targets(session, '"synthetic"')
        assert ("custom_import_family_block_relation" in targets) == (installed == (11, 12))


async def test_custom_reference_columns_reach_existing_indexed_and_paged_probe(monkeypatch):
    table = _block_fixture().binding.tables_by_role["family"]
    columns_by_name = {"root_revision_id": 1}
    session = object()
    indexed = AsyncMock(return_value={})
    monkeypatch.setattr(retirement, "_indexed_revision_references", indexed)
    await retirement._reference_pages(session, table, 99, columns_by_name)
    indexed.assert_awaited_once_with(session, table, 99, columns_by_name)


@pytest.mark.parametrize("is_block", [False, True])
async def test_physical_drop_uses_actual_roles_and_preserves_block_audit(monkeypatch, is_block):
    case = _block_fixture()
    resolved = (
        storage.RetirementStorage({table.name: table for table in case.binding.tables_by_role.values()}, case.binding)
        if is_block
        else storage.RetirementStorage(dict(retirement.snapshot_tables(11)))
    )
    monkeypatch.setattr(storage, "resolve_storage", AsyncMock(return_value=resolved))
    statements = []

    async def execute(statement, _parameters=None):
        statements.append(statement)
        result = _Result([])
        result.rowcount = 15
        return result

    session = SimpleNamespace(execute=execute, scalar=AsyncMock(return_value=False))
    family = SimpleNamespace(family_id=11, landing_table_oid=None, origin_table_oid=None)
    await retirement._physical_drop(session, '"synthetic"', family)
    dropped_statements = [str(statement) for statement in statements if str(statement).startswith("DROP TABLE")]
    assert len(dropped_statements) == 1
    assert all('"ci_snapshot_11"."' + name + '"' in dropped_statements[0] for name in resolved.tables_by_name)
    assert ("custom_import_child_revision" in dropped_statements[0]) is not is_block
    deleted_tables = {statement.table.name for statement in statements if getattr(statement, "is_delete", False)}
    assert deleted_tables == (
        {"custom_import_revision_home"}
        if is_block
        else {"custom_import_revision_home", "custom_import_snapshot_relation"}
    )


async def test_block_relation_locks_do_not_name_nonexistent_legacy_heaps(monkeypatch):
    case = _block_fixture()
    resolved = storage.RetirementStorage(
        {table.name: table for table in case.binding.tables_by_role.values()}, case.binding
    )
    resolve = AsyncMock(return_value=resolved)
    monkeypatch.setattr(storage, "resolve_storage", resolve)
    monkeypatch.setattr(retirement, "lock_historical_projection", AsyncMock(return_value=()))
    session = SimpleNamespace(execute=AsyncMock())
    family = SimpleNamespace(family_id=11, landing_table_oid=None, origin_table_oid=None, frozen_at=None)
    await retirement._locked_relations(session, '"synthetic"', family, [family])
    statement = str(session.execute.await_args.args[0])
    assert "ACCESS EXCLUSIVE MODE NOWAIT" in statement and "custom_import_block_fragment" in statement
    assert "custom_import_child_revision" not in statement and "custom_import_family_child" not in statement
    assert resolve.await_count == 2


async def test_inventory_uses_block_audit_without_cross_joining_legacy_registry(monkeypatch):
    from process.custom_import import retirement_inventory as inventory

    case = _block_fixture()
    resolved = storage.RetirementStorage(
        {table.name: table for table in case.binding.tables_by_role.values()}, case.binding
    )
    monkeypatch.setattr(storage, "resolve_storage", AsyncMock(return_value=resolved))
    monkeypatch.setattr(retirement, "_close_frozen_reference_heaps", AsyncMock())
    family = CustomImportSnapshotFamily(
        family_id=11,
        dataset_id=7,
        definition_revision_id=17,
        schema_revision_id=13,
        execution_id=19,
        capture_bundle_id=23,
        producing_fence=3,
        producing_token_sha256=b"t" * 32,
    )
    registry_queries = []

    async def execute(statement, parameters=None):
        if getattr(statement, "is_select", False):
            assert statement.get_final_froms() == [CustomImportFamilyBlockRelation.__table__]
            registry_queries.append(statement)
            return _Result(
                [(slot, 11, name, slot + 1000, 91, b"c" * 32, b"k" * 32) for name, (slot, _table) in ROLE_SPECS.items()]
            )
        if "relfilenode" in str(statement):
            assert set(parameters["names"]) == set(resolved.tables_by_name)
            return _Result([(name, index + 1000, 91, index + 2000) for index, name in enumerate(parameters["names"])])
        return _Result([])

    binding = await inventory._pin_family(SimpleNamespace(execute=execute), '"synthetic"', family)
    assert len(registry_queries) == 1 and len(binding[1]) == len(binding[2]) == 14
