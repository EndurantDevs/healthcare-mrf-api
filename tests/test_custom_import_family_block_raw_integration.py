# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Raw scalar layout integration and complete batched native catalog observations."""

import asyncio
import importlib.util
import json
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest
from sqlalchemy.dialects.postgresql import dialect
from sqlalchemy.schema import CreateTable

from db.models.custom_import_storage import CustomImportSnapshotFamily
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.family_block_binding import (
    _CATALOG_IDENTITIES,
    FamilyBlockUnavailable,
    _column_shape,
    resolve_family_block_binding,
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
from process.custom_import.query_projection_binding import _CATALOG_IDENTITY
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
    tables = layout.tables(11)
    root, child = tables["family"], tables["query_record"]
    assert len(tables) == 14 and list(tables) == list(ROLE_SPECS)
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
    spec_items = [row for row in index_specifications(11, layout) if row[0] == "query_record"]
    assert len(spec_items) == 1 and spec_items[0][2] == ("family_revision_id", "collection_slot", "key_rank")
    assert all(
        not any(name.startswith("raw_field_") for name in names) for _, _, names in index_specifications(11, layout)
    )


def test_no_declared_query_child_has_no_child_cells_and_refuses_rows():
    layout = _layout(child=False)
    table = layout.tables(11)["query_record"]
    assert not any(name.startswith("raw_field_") for name in table.c.keys())
    assert "CHECK (false)" in str(CreateTable(table).compile(dialect=dialect()))


def test_copy_cells_keep_missing_null_and_retained_actual_kind_independent():
    layout = _layout()
    table = layout.tables(11)["query_record"]
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


def test_updated_registry_roles_and_acl_sources_match_the_native_layout():
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
    assert "family_id=f.family_id) <> 14" in migration._RESOLVER


def test_catalog_batch_keeps_every_original_custody_expression():
    restored = (
        str(_CATALOG_IDENTITIES)
        .replace(
            "SELECT c.relname::text AS table_name, pg_catalog.obj_description(c.oid,'pg_class') AS table_comment, c.oid::bigint AS table_oid",
            "SELECT c.oid::bigint AS table_oid",
        )
        .replace("c.relname=ANY(CAST(:table_names AS text[]))", "c.relname=:table_name")
    )
    assert restored == str(_CATALOG_IDENTITY)


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


class _Session:
    def __init__(self, layout, *, corrupt=None, legacy=False):
        self.layout, self.corrupt, self.legacy, self.calls = layout, corrupt, legacy, []
        self.transactions = [SimpleNamespace(is_active=True), None, SimpleNamespace(is_active=True), None]
        self.pin_role_oids = [[slot, 1000 + slot] for slot, _ in ROLE_SPECS.values()]
        self.connection_value = SimpleNamespace(
            dialect=dialect(),
            sync_connection=SimpleNamespace(
                get_execution_options=lambda: {
                    "schema_translate_map": {CustomImportSnapshotFamily.__table__.schema: "synthetic_control"}
                }
            ),
            get_transaction=lambda: self.transactions[2],
            get_nested_transaction=lambda: self.transactions[3],
        )

    def get_transaction(self):
        return self.transactions[0]

    def get_nested_transaction(self):
        return self.transactions[1]

    async def connection(self):
        return self.connection_value

    def _catalog_result(self):
        family_rows = []
        for role, table in self.layout.tables(11).items():
            slot, name = ROLE_SPECS[role]
            family_rows.append(
                {
                    "table_name": name,
                    "table_comment": self.layout.comment if role == "family" else None,
                    "table_oid": 1000 + slot,
                    "table_owner": 91,
                    "schema_owner": 91,
                    "relkind": "r",
                    "relpersistence": "p",
                    "inherited": False,
                    "foreign_keys": False,
                    "triggers": False,
                    "columns_sha256": bytes([slot % 256]) * 32,
                    "columns": [_column_shape(column, i) for i, column in enumerate(table.c, 1)],
                    "primary_key": list(table.primary_key.columns.keys()),
                }
            )
        match self.corrupt:
            case "missing":
                family_rows.pop()
            case "duplicate":
                family_rows[-1] = family_rows[0]
            case "columns":
                family_rows[-1]["columns"] = []
            case "comment":
                family_rows[6]["table_comment"] = "wrong"
            case "oid":
                family_rows[-1]["table_oid"] += 1
        return _Result(family_rows)

    def _index_result(self):
        family_rows = []
        for role, name, columns in index_specifications(11, self.layout):
            table = self.layout.tables(11)[role]
            collations = [
                "pg_catalog.C"
                if column == "canonical_value"
                else ("pg_catalog.default" if hasattr(table.c[column].type, "collation") else None)
                for column in columns
            ]
            family_rows.append(
                {
                    "index_name": name,
                    "table_oid": 1000 + ROLE_SPECS[role][0],
                    "index_owner": 91,
                    "index_kind": "i",
                    "access_method": "btree",
                    "key_names": list(columns),
                    "key_count": len(columns),
                    "attribute_count": len(columns),
                    "collation_names": collations,
                    "uncollated": not any(collations),
                    "unique_index": False,
                    "primary_index": False,
                    "exclusion_index": False,
                    **{
                        key: True
                        for key in ("valid", "ready", "live", "plain", "complete", "default_options", "default_ops")
                    },
                }
            )
        if self.corrupt == "index":
            family_rows[-1]["ready"] = False
        return _Result(family_rows)

    async def execute(self, statement, parameters):
        sql = str(statement)
        self.calls.append((sql, parameters))
        if "resolve_custom_import_generation_snapshot" in sql:
            return _Result([(11, self.legacy, [] if self.legacy else self.pin_role_oids)])
        if "resolve_custom_import_snapshot_relations" in sql:
            return _Result(
                [
                    (slot, 1000 + slot)
                    for slot in (range(1, 16) if self.legacy else [slot for slot, _ in ROLE_SPECS.values()])
                ]
            )
        if "table_names" in parameters:
            return self._catalog_result()
        return self._index_result()


def _resolve(session, *, persistence="logged"):
    return asyncio.run(
        resolve_family_block_binding(
            session, 11, session.layout.definition, session.layout.collection_slots, persistence=persistence
        )
    )


def test_logged_binding_uses_three_fresh_queries_without_a_writer_timer():
    session = _Session(_layout())
    binding = _resolve(session)
    assert len(binding.tables_by_role) == 14 and len(session.calls) == 3
    assert set(session.calls[1][1]["table_names"]) == {name for _, name in ROLE_SPECS.values()}
    assert "index_names" in session.calls[2][1]
    assert not hasattr(session, "info")
    # A second transaction/context must observe everything again.
    _resolve(session)
    assert len(session.calls) == 6


def test_loading_candidate_uses_two_queries_before_indexes_exist():
    session = _Session(_layout())
    _resolve(session, persistence="candidate")
    assert len(session.calls) == 2


async def _pin(session):
    return await resolve_generation_snapshot_binding(session, PinnedReadTarget(1, 2, 3, 4, "default"))


async def _pinned_binding(session, pin):
    return await resolve_pinned_family_block_binding(
        session, pin, session.layout.definition, session.layout.collection_slots
    )


@pytest.mark.asyncio
async def test_pinned_reader_keeps_all_catalog_checks_without_direct_registry_execution():
    session = _Session(_layout())
    pin = await _pin(session)
    assert set(dict(pin.block_role_oids)) == {slot for slot, _ in ROLE_SPECS.values()}
    binding = await _pinned_binding(session, pin)
    assert len(binding.tables_by_role) == 14
    assert dict(binding.oids_by_role) == {role: 1000 + slot for role, (slot, _) in ROLE_SPECS.items()}
    assert len(session.calls) == 3
    assert session.calls[0][0].count("resolve_custom_import_generation_snapshot(") == 1
    assert "table_names" in session.calls[1][1] and "index_names" in session.calls[2][1]
    assert not any("resolve_custom_import_snapshot_relations" in sql for sql, _ in session.calls)
    assert not hasattr(session, "info")
    # A new pin must observe OIDs and all catalog/index facts again.
    await _pinned_binding(session, await _pin(session))
    assert len(session.calls) == 6


@pytest.mark.asyncio
@pytest.mark.parametrize("corrupt", ("missing", "duplicate", "columns", "comment", "oid", "index"))
async def test_pinned_reader_refuses_every_existing_catalog_custody_failure(corrupt):
    session = _Session(_layout(), corrupt=corrupt)
    with pytest.raises(FamilyBlockUnavailable):
        await _pinned_binding(session, await _pin(session))
    assert not any("resolve_custom_import_snapshot_relations" in sql for sql, _ in session.calls)


@pytest.mark.asyncio
async def test_pin_refuses_another_session_in_same_transaction():
    session = _Session(_layout())
    pin = await _pin(session)
    other = _Session(session.layout)
    other.transactions = session.transactions
    with pytest.raises(FamilyBlockUnavailable, match="generation pin"):
        await _pinned_binding(other, pin)
    assert not other.calls


@pytest.mark.asyncio
@pytest.mark.parametrize("when", ["before", "catalog", "index"])
@pytest.mark.parametrize("change", [0, 1, 2, 3, "nested_rollback", "connection_ended"])
async def test_pinned_reader_refuses_transaction_replacement_or_rollback(when, change):
    session = _Session(_layout())
    session.transactions[1] = SimpleNamespace(is_active=True)
    session.transactions[3] = SimpleNamespace(is_active=True)
    pin = await _pin(session)

    def change_transaction():
        match change:
            case "nested_rollback":
                for slot in (1, 3):
                    session.transactions[slot].is_active = False
                    session.transactions[slot] = None
            case "connection_ended":
                session.transactions[2].is_active = False
            case _:
                session.transactions[change] = SimpleNamespace(is_active=True)

    execute = session.execute

    async def after_query(statement, parameters):
        result = await execute(statement, parameters)
        if (when == "catalog" and "table_names" in parameters) or (when == "index" and "index_names" in parameters):
            change_transaction()
        return result

    session.execute = after_query
    if when == "before":
        change_transaction()
    with pytest.raises(FamilyBlockUnavailable, match="generation pin"):
        await _pinned_binding(session, pin)
    assert len(session.calls) == {"before": 1, "catalog": 2, "index": 3}[when]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change",
    [
        "bare_id",
        "no_session",
        "no_transaction",
        "short_transaction",
        "incomplete",
        "duplicate_oid",
        "mutable",
        "legacy",
    ],
)
async def test_reader_cannot_use_a_bare_identity_or_incomplete_pin_receipt(change):
    session = _Session(_layout())
    pin = await _pin(session)
    match change:
        case "bare_id":
            pin = pin.family_id
        case "no_session":
            pin = replace(pin, _session=None)
        case "no_transaction":
            pin = replace(pin, _transactions=None)
        case "short_transaction":
            pin = replace(pin, _transactions=pin._transactions[:3])
        case "incomplete":
            pin = replace(pin, block_role_oids=pin.block_role_oids[:-1])
        case "duplicate_oid":
            rows = list(pin.block_role_oids)
            rows[-1] = (rows[-1][0], rows[0][1])
            pin = replace(pin, block_role_oids=tuple(rows))
        case "mutable":
            pin = replace(pin, block_role_oids=list(pin.block_role_oids))
        case _:
            pin = replace(pin, legacy_snapshot=True)
    with pytest.raises(FamilyBlockUnavailable, match="generation pin"):
        await _pinned_binding(session, pin)
    assert len(session.calls) == 1


@pytest.mark.asyncio
async def test_unbound_generation_receipt_does_not_authorize_a_reader():
    session = _Session(_layout())
    pin = GenerationSnapshotBinding(11, False, tuple(map(tuple, session.pin_role_oids)))
    with pytest.raises(FamilyBlockUnavailable, match="generation pin"):
        await _pinned_binding(session, pin)
    assert not session.calls


@pytest.mark.parametrize("corrupt", ("missing", "duplicate", "columns", "comment", "oid", "index"))
def test_batched_observation_refuses_missing_drifted_or_invalid_custody(corrupt):
    with pytest.raises(FamilyBlockUnavailable):
        _resolve(_Session(_layout(), corrupt=corrupt))


def test_exact_original_census_keeps_legacy_behavior_without_new_catalog_queries():
    session = _Session(_layout(), legacy=True)
    assert _resolve(session) is None and len(session.calls) == 1
