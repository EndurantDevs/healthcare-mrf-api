# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Raw scalar data layout and schema-qualified snapshot bindings."""

import importlib.util
import json
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest
from sqlalchemy.dialects.postgresql import dialect
from sqlalchemy.schema import CreateTable

from process.custom_import.definition import CustomImportDefinition
from process.custom_import.family_block_binding import (
    FamilyBlockUnavailable,
    checked_binding,
    resolve_pinned_family_block_binding,
)
from process.custom_import.family_block_layout import (
    READ_CAPABILITIES,
    ROLE_SPECS,
    FamilyBlockLayout,
    index_specifications,
)
from process.custom_import.family_block_scalar_layout import pack_query_scalars, query_scalar_fields, scalar_column
from process.custom_import.materialization import TypedScalar
from process.custom_import.read_contracts import PinnedReadTarget
from process.custom_import.read_identity import GenerationSnapshotBinding, resolve_generation_snapshot_binding


def _definition(*, child=True, hidden=True):
    document = json.loads((Path(__file__).parent / "fixtures/custom_import/v1_valid.json").read_text())
    if hidden:
        document["schema"]["children"].append(
            {
                "name": "notes",
                "parent_key": [{"child": "note_npi", "root": "npi"}],
                "child_key": ["note_key"],
                "fields": [
                    {"id": "note_npi", "slot": 6, "type": "string", "nullable": False},
                    {"id": "note_key", "slot": 7, "type": "string", "nullable": False, "projection_slot": 5},
                    {"id": "note_value", "slot": 8, "type": "integer", "nullable": True, "projection_slot": 6},
                ],
            }
        )
        document["streams"].append(
            {
                "id": "notes",
                "kind": "child",
                "child": "notes",
                "format": "csv",
                "compression": "none",
                "snapshot_token": "snapshot_id",
            }
        )
        document["aliases"]["notes"] = {"Provider": "note_npi", "Key": "note_key", "Value": "note_value"}
    if not child:
        document["query"].pop("child")
        document["query"]["order"] = []
        document["selection_profiles"] = [
            {
                "id": "default",
                "selection": [{"field": "display_name", "direction": "asc", "nulls": "last"}],
                "context_dimensions": [],
            }
        ]
    return CustomImportDefinition.from_mapping(document)


def _layout(*, child=True):
    return FamilyBlockLayout(_definition(child=child), {"rates": 7, "notes": 9})


def test_only_exposed_root_and_query_child_fields_get_native_cells():
    layout = _layout()
    tables = layout.tables(11001)
    root, child = tables["family"], tables["query_record"]
    assert len(tables) == 13 and list(tables) == list(ROLE_SPECS)
    assert list(root.c.keys())[-4:] == ["raw_states", "raw_type_overrides", "raw_field_1", "raw_field_2"]
    assert list(child.c.keys()) == [
        "child_revision_id",
        "family_revision_id",
        "collection_slot",
        "key_rank",
        "key_variant",
        "raw_states",
        "raw_type_overrides",
        "raw_field_4",
        "raw_field_5",
    ]
    assert list(child.primary_key.columns.keys()) == ["child_revision_id"]
    assert not child.foreign_keys
    ddl = str(CreateTable(child).compile(dialect=dialect()))
    assert "collection_slot = 7" in ddl and "NUMERIC(30, 12)" in ddl
    assert all(f"raw_field_{slot}" not in ddl for slot in (3, 6, 7, 8))
    assert "pack_id" not in child.c and "source_ordinal" not in child.c
    spec_items = [row for row in index_specifications(11001, layout) if row[0] == "query_record"]
    assert len(spec_items) == 1 and spec_items[0][2] == ("family_revision_id", "collection_slot", "key_rank")
    assert all(
        not any(name.startswith("raw_field_") for name in names) for _, _, names in index_specifications(11001, layout)
    )


def test_no_declared_query_child_has_no_child_cells_and_refuses_rows():
    layout = _layout(child=False)
    table = layout.tables(11001)["query_record"]
    assert not any(name.startswith("raw_field_") for name in table.c.keys())
    assert "CHECK (false)" in str(CreateTable(table).compile(dialect=dialect()))


def test_copy_cells_keep_missing_null_and_retained_actual_kind_independent():
    layout = _layout()
    table = layout.tables(11001)["query_record"]
    fields = query_scalar_fields(layout.definition, "rates")
    packed = pack_query_scalars(fields, {"amount": TypedScalar("string", "value", string_value="opaque original")})
    family_by_column = dict(zip(list(table.c.keys())[5:], packed, strict=True))
    assert family_by_column == {
        "raw_states": b"\x08",
        "raw_type_overrides": b"\x10",
        "raw_field_4": None,
        "raw_field_5": None,
    }
    # Field declaration nullability does not turn an original explicit null into missing.
    fields = query_scalar_fields(layout.definition)
    packed = pack_query_scalars(fields, {"npi": TypedScalar("string", "null")})
    assert packed == (b"\x01", None, None, None)
    assert tuple(scalar_column(field) for field in fields) == ("raw_field_1", "raw_field_2")


def test_declared_roles_and_read_sources_match_the_native_layout():
    path = Path(__file__).resolve().parents[1] / "alembic/versions/20261104020000_custom_import_family_blocks.py"
    spec = importlib.util.spec_from_file_location("raw_role_migration", path)
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    assert {role: (slot, name) for slot, role, name in migration._ROLES} == dict(ROLE_SPECS)
    assert "custom_import_root_scalar" in READ_CAPABILITIES["family"]
    assert READ_CAPABILITIES["query_record"] == (
        "custom_import_family_child",
        "custom_import_child_revision",
        "custom_import_child_scalar",
    )
    assert len(migration._DDL) == 1 and "custom_import_family_block_layout" in migration._DDL[0]


class _Result:
    def __init__(self, rows):
        self.rows = rows

    def mappings(self):
        return self

    def all(self):
        return self.rows

    def one(self):
        assert len(self.rows) == 1
        return self.rows[0]


class _Session:
    def __init__(self, layout):
        self.layout, self.calls = layout, []

    async def connection(self):
        return SimpleNamespace(
            dialect=dialect(),
            sync_connection=SimpleNamespace(get_execution_options=lambda: {}),
        )

    async def execute(self, statement, parameters):
        self.calls.append((str(statement), parameters))
        assert "resolve_custom_import_generation_snapshot" in str(statement)
        return _Result([(11001, False, "ci_snapshot_11001")])


async def _pin(session):
    return await resolve_generation_snapshot_binding(session, PinnedReadTarget(1, 2, 3, 4, "default"))


async def _pinned_binding(session, pin):
    return await resolve_pinned_family_block_binding(
        session, pin, session.layout.definition, session.layout.collection_slots
    )


@pytest.mark.asyncio
async def test_published_pin_resolves_all_tables_by_qualified_name():
    session = _Session(_layout())
    pin = await _pin(session)
    assert pin == GenerationSnapshotBinding(11001, False, "ci_snapshot_11001")
    binding = await _pinned_binding(session, pin)
    assert set(binding.tables_by_role) == set(ROLE_SPECS)
    assert len(session.calls) == 1
    assert all(table.schema == pin.schema_name for table in binding.tables_by_role.values())
    assert all(table.name == ROLE_SPECS[role][1] for role, table in binding.tables_by_role.items())


@pytest.mark.asyncio
@pytest.mark.parametrize("schema_name", ("ci_snapshot_11001", "copied published schema"))
async def test_published_names_serve_from_another_session_without_metadata_rewrites(schema_name):
    original = _Session(_layout())
    pin = replace(await _pin(original), schema_name=schema_name)
    other = _Session(original.layout)
    binding = await _pinned_binding(other, pin)
    assert all(table.schema == schema_name for table in binding.tables_by_role.values())
    assert not other.calls


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "pin", (11001, GenerationSnapshotBinding(11001, True, "ci_snapshot_11001"), GenerationSnapshotBinding(None, False))
)
async def test_block_reader_requires_a_named_block_layout(pin):
    session = _Session(_layout())
    with pytest.raises(FamilyBlockUnavailable, match="generation binding"):
        await _pinned_binding(session, pin)
    assert not session.calls
