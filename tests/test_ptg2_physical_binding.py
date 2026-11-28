# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Physical family declarations preserve installed semantics without admission."""

import hashlib
import importlib.util
import inspect
import json
import re
from copy import deepcopy
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch
from uuid import UUID

import pytest
from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateIndex, CreateTable

from api import ptg2_tables
from api.ptg2_types import PTG2ServingTables
from db import models
from db.models import ptg_snapshot_local as sidecars
from process import reference_family_archive as archive
from process.ptg_parts import ptg2_physical_binding as binding_module
from process.ptg_parts.ptg2_physical_binding import (
    PHYSICAL_BINDING_CONTRACT,
    PTG2PhysicalBinding,
    PTG2PhysicalBindingError,
    local_data_family_spec,
)
from process.ptg_parts.ptg2_snapshot_candidates import snapshot_candidate_relation
from process.ptg_parts.result_archive_adoption import _REKEYED_TABLES
from process.ptg_parts.result_archive_candidate_preparation import _ALLOWED_AMOUNT_TABLES, physical_family_spec
from tests.ptg2_manifest_tables_support import FakeSession, strict_snapshot_row


def test_local_marker_probes_cast_the_historical_json_snapshot_column():
    resolver_sql = inspect.getsource(ptg2_tables.snapshot_serving_tables)
    declared_sql = ptg2_tables.local_physical_binding_declared_sql("snapshot", "layout")
    assert "snapshot.manifest::jsonb ? 'physical_binding_contract'" in declared_sql
    assert "snapshot.manifest::jsonb ? 'physical_binding_contract'" in resolver_sql
    assert 'local_physical_binding_declared_sql("snapshot", "layout")' in resolver_sql
    assert "snapshot.manifest ? 'physical_binding_contract'" not in resolver_sql


def test_local_control_join_uses_actual_native_layout_columns_for_both_keys():
    query = binding_module._local_candidate_control_query(
        "mrf", _binding(), snapshot_parameter=":snapshot_id", payload_parameter=":payload_key"
    )
    for column in ("logical_byte_count", "storage_shard_id"):
        assert column in models.PTG2V3SnapshotLayout.__table__.c
        assert f"layout.{column}" in query and f"payload_layout.{column}" in query
    assert "layout.logical_bytes" not in query and "layout.storage_shard " not in query
    assert "payload_layout.snapshot_key=:payload_key" in query
    assert "layout.snapshot_key=binding.snapshot_key" in query
    assert "attestation ON attestation.snapshot_id=snapshot.snapshot_id" in query
    assert "attestation USING(snapshot_id)" not in query


def _historical_statements():
    """Capture only installed migration DDL, without a database or migration write."""
    statements = []
    for revision in (
        "20260723100000_ptg2_v4_snapshot_map_pack",
        "20260724120000_ptg2_v4_taxonomy_candidates",
        "20260727100000_ptg2_provider_tax_identity",
        "20260806100000_ptg2_tax_identity_source",
    ):
        path = Path(__file__).resolve().parents[1] / "alembic" / "versions" / f"{revision}.py"
        module_spec = importlib.util.spec_from_file_location(revision, path)
        module = importlib.util.module_from_spec(module_spec)
        module_spec.loader.exec_module(module)
        with (
            patch.object(module.op, "execute", side_effect=statements.append),
            patch.object(module, "_schema", return_value=models.PTG2V3Block.__table__.schema),
        ):
            module.upgrade()
    return statements


def _normalized_sql(statement):
    """Preserve PostgreSQL newline-concatenated literals before whitespace folding."""
    statement = re.sub(r"'([^']*)'\s*\n\s*'([^']*)'", lambda match: "'" + match[1] + match[2] + "'", statement)
    statement = " ".join(statement.replace('"', "").lower().split()).rstrip(";")
    return re.sub(r"\s*([(),])\s*", r"\1", statement)


def _table_shape(statement):
    """Compare columns and named native constraints independently of DDL ordering."""
    statement = _normalized_sql(statement)
    body = statement[statement.index("(") + 1 : statement.rindex(")")].strip()
    column_definitions = body.split("constraint ", 1)[0].rstrip(", ").split(",")
    columns = []
    for definition in column_definitions:
        definition = definition.strip().replace("timestamp with time zone", "timestamptz")
        name, native_type, *rest = definition.split()
        suffix = " ".join(rest)
        default = suffix.replace("not null", "").strip()
        columns.append((name, native_type, "not null" in suffix, default))
    constraints_by_name = {}
    for definition in body.split("constraint ")[1:]:
        name, expression = definition.rstrip(", ").split(" ", 1)
        constraints_by_name[name] = expression
    return columns, constraints_by_name


@pytest.mark.parametrize("model_name", sidecars.__all__)
def test_sidecar_declarations_equal_historical_native_columns_keys_checks_and_indexes(model_name):
    model = getattr(sidecars, model_name)
    assert model.__runtime_schema_sync__ is False
    statements = _historical_statements()
    table_name = model.__tablename__
    historical = next(
        statement
        for statement in statements
        if re.match(r'\s*CREATE TABLE "[^"]+"\."' + table_name + r'"\s*\(', statement)
    )
    declared = str(CreateTable(model.__table__).compile(dialect=postgresql.dialect()))
    assert _table_shape(declared) == _table_shape(historical)
    historical_indexes = {
        _normalized_sql(statement)
        for statement in statements
        if re.match(r'\s*CREATE (?:UNIQUE )?INDEX .*? ON "[^"]+"\."' + table_name + r'"\s*\(', statement, re.S)
    }
    declared_indexes = {
        _normalized_sql(str(CreateIndex(index).compile(dialect=postgresql.dialect())))
        for index in model.__table__.indexes
    }
    assert declared_indexes == historical_indexes


def _binding():
    """Keep destination metadata IDs distinct from unchanged source payload IDs."""
    return PTG2PhysicalBinding(
        PHYSICAL_BINDING_CONTRACT,
        "synthetic-destination",
        701,
        "synthetic-source",
        19,
        UUID(int=1),
        "reference_family_archive_" + UUID(int=1).hex,
        tuple(
            ("reference_family_archive_" + UUID(int=1).hex, name)
            for name in sorted(local_data_family_spec().table_names)
        ),
        (
            (
                "reference_family_archive_" + UUID(int=1).hex,
                "ptg2_v3_snapshot_layout_snapshot_key_seq",
                "ptg2_v3_snapshot_layout",
                "snapshot_key",
            ),
        ),
    )


def test_complete_payload_family_includes_serving_target_and_cas():
    family = physical_family_spec()
    assert family.table_names == (
        _REKEYED_TABLES[0],
        "ptg2_v3_price_attr",
        *_REKEYED_TABLES[1:],
        "ptg2_v4_snapshot_map_root",
        *_ALLOWED_AMOUNT_TABLES,
        "ptg2_v3_block",
    )
    assert len(family.model_types) == 32
    assert "ptg2_v4_finalizer_map_target" in family.table_names
    assert all(model.__table__.primary_key.columns for model in family.model_types)
    assert "ptg2_v3_snapshot_layout" not in family.table_names
    assert "ptg2_snapshot" not in family.table_names


def test_local_model_digest_is_stable_and_binds_native_column_shape(monkeypatch):
    first = binding_module.local_data_model_digest()
    assert re.fullmatch(r"[0-9a-f]{64}", first)
    assert binding_module.local_data_model_digest() == first
    column = models.PTG2V3Code.__table__.c.code_key
    monkeypatch.setattr(column, "nullable", not column.nullable)
    assert binding_module.local_data_model_digest() != first


def test_local_indexes_have_only_exact_model_owned_partial_predicates():
    predicate_by_index = {}
    for model in local_data_family_spec().model_types:
        for index in model.__table__.indexes:
            assert all(getattr(element, "table", None) is model.__table__ for element in index.expressions)
            predicate = index.dialect_options["postgresql"].get("where")
            if predicate is not None:
                predicate_by_index[index.name] = _normalized_sql(str(predicate))
    assert predicate_by_index == {
        "ptg2_provider_group_tax_identity_tin_group_idx": "tax_identity_state = 'matched_ein'",
        "ptg2_provider_group_tax_identity_source_tin_idx": "tin_key is not null",
        "ptg2_v3_snapshot_layout_sealed_mapping_idx": (
            "state = 'sealed' and mapping_digest is not null and support_digest is not null"
        ),
    }


def test_binding_keeps_local_metadata_and_payload_coordinates_separate():
    binding = _binding()
    assert binding.snapshot_id != binding.payload_snapshot_id
    assert binding.destination_layout_key != binding.payload_snapshot_key
    assert binding.relation("ptg2_v3_block") == ('"reference_family_archive_' + UUID(int=1).hex + '"."ptg2_v3_block"')
    assert len(binding.relations) == 42 and len(binding.sequences) == 1
    assert binding.relation("ptg2_source_trace") == (
        '"reference_family_archive_' + UUID(int=1).hex + '"."ptg2_source_trace"'
    )
    with pytest.raises(PTG2PhysicalBindingError, match="outside"):
        binding.relation("unreviewed_relation")


@pytest.mark.parametrize(
    "change",
    (
        {"contract": "unknown"},
        {"destination_layout_key": True},
        {"payload_snapshot_key": 0},
        {"snapshot_id": " synthetic-destination"},
        {"payload_snapshot_id": "synthetic\nsource"},
        {"schema_name": "invalid.schema"},
        {"dataset_id": "untrusted-schema"},
        {"relations": ()},
        {"sequences": ()},
        {
            "sequences": (
                (
                    "reference_family_archive_" + UUID(int=1).hex,
                    "unreviewed_sequence",
                    "ptg2_v3_snapshot_layout",
                    "snapshot_key",
                ),
            )
        },
    ),
)
def test_binding_rejects_partial_or_malformed_identity(change):
    with pytest.raises(PTG2PhysicalBindingError):
        replace(_binding(), **change)


def test_binding_rejects_duplicate_missing_or_arbitrary_model_names():
    binding = _binding()
    for inventory in (
        (binding.relations[0], *binding.relations),
        binding.relations[1:],
        ((binding.schema_name, "unreviewed_relation"), *binding.relations[1:]),
        (("different_schema", binding.relations[0][1]), *binding.relations[1:]),
    ):
        with pytest.raises(PTG2PhysicalBindingError, match="inventory"):
            replace(binding, relations=inventory)


def test_internal_carrier_never_authorizes_serving_or_candidate_resolution():
    session = SimpleNamespace(info={"ptg_snapshot_physical_binding": _binding()})
    with pytest.raises(PTG2PhysicalBindingError, match="not available"):
        snapshot_candidate_relation(session, '"synthetic"', "ptg2_v3_block")
    session.info.clear()
    assert snapshot_candidate_relation(session, '"synthetic"', "ptg2_v3_block") == '"synthetic"."ptg2_v3_block"'
    with pytest.raises(PTG2PhysicalBindingError, match="not available"):
        PTG2ServingTables(physical_binding=_binding())


@pytest.mark.asyncio
@pytest.mark.parametrize("declared", [True, False])
async def test_ordinary_activation_refuses_declared_local_candidate(declared):
    from process.ptg_parts import source_pointers

    session = SimpleNamespace(scalar=AsyncMock(return_value=declared))
    if declared:
        with pytest.raises(ValueError, match="require protected installed publication"):
            await source_pointers._require_canonical_candidate_activation(session, "mrf", "synthetic-destination")
    else:
        await source_pointers._require_canonical_candidate_activation(session, "mrf", "synthetic-destination")
    query, parameters = session.scalar.await_args.args
    assert "manifest::jsonb ? 'physical_binding_contract'" in str(query)
    assert "manifest::jsonb ? 'local_data_preparation'" in str(query)
    assert parameters == {"snapshot_id": "synthetic-destination"}


@pytest.mark.asyncio
@pytest.mark.parametrize("declaration", (None, {}, {"contract": PHYSICAL_BINDING_CONTRACT}))
async def test_serving_declaration_never_falls_back_to_canonical_relations(declaration):
    row = strict_snapshot_row()
    row["layout_serving_index"]["physical_binding"] = declaration
    session = FakeSession([row])
    with pytest.raises(ptg2_tables.PTG2ManifestArtifactError, match="not available"):
        await ptg2_tables.snapshot_serving_tables(session, "synthetic-destination")
    assert len(session.calls) == 1


@pytest.mark.asyncio
async def test_persisted_local_layout_declaration_is_unavailable_before_canonical_validation():
    row = strict_snapshot_row()
    row["has_local_physical_binding"] = True
    session = FakeSession([row])
    with pytest.raises(ptg2_tables.PTG2ManifestArtifactError, match="not available"):
        await ptg2_tables.snapshot_serving_tables(session, "synthetic-destination")
    assert len(session.calls) == 1


@pytest.mark.asyncio
async def test_complete_models_use_shared_deferred_heap_engine_without_partition_strategy():
    session = SimpleNamespace(execute=AsyncMock())
    family = physical_family_spec()
    original_table_ddls = tuple(
        str(CreateTable(model.__table__).compile(dialect=postgresql.dialect())) for model in family.model_types
    )
    await archive._create_model_family(
        session, family, _binding().schema_name, create_indexes=False, ordinary_heaps=True
    )
    statements = [str(call.args[0]) for call in session.execute.await_args_list]
    assert len(statements) == 33
    assert sum("CREATE TABLE" in statement for statement in statements) == 32
    assert all(
        not any(
            fragment in statement
            for fragment in ("PARTITION BY", "FOREIGN KEY", "PRIMARY KEY", "UNIQUE", "CREATE INDEX")
        )
        for statement in statements
    )
    assert any("ptg2_v3_block" in statement and "payload BYTEA NOT NULL" in statement for statement in statements)
    assert original_table_ddls == tuple(
        str(CreateTable(model.__table__).compile(dialect=postgresql.dialect())) for model in family.model_types
    )
    assert models.PTG2V3Block.__table__.dialect_options["postgresql"]["partition_by"] == "HASH (block_hash)"


@pytest.mark.asyncio
async def test_existing_clone_contract_preserves_partition_strategy_by_default():
    session = SimpleNamespace(execute=AsyncMock())
    await archive._create_model_family(
        session,
        archive.ReferenceFamilySpec("synthetic", (models.PTG2V3Block,)),
        "synthetic_stage",
        create_indexes=False,
    )
    assert "PARTITION BY HASH (block_hash)" in str(session.execute.await_args.args[0])


def test_two_payload_roots_close_every_declared_family_relationship():
    family = physical_family_spec()
    models_by_name = {model.__tablename__: model for model in family.model_types}
    roots = (models.PTG2V3SnapshotLayout, models.PTG2Snapshot)
    external_targets = {
        element.column.table.name
        for model in family.model_types
        for constraint in model.__table__.foreign_key_constraints
        for element in constraint.elements
        if element.column.table.name not in models_by_name
    }
    assert external_targets == {model.__tablename__ for model in roots}
    models_by_name.update((model.__tablename__, model) for model in roots)
    evidence = (
        models.PTG2SourceIdentity,
        models.PTG2ContentIdentity,
        models.PTG2SourceFileVersion,
        models.PTG2SourceTrace,
        models.PTG2SourceTraceSet,
        models.PTG2V3SnapshotSource,
        models.PTG2V3SnapshotScope,
        models.PTG2V3SnapshotPlanScope,
    )
    models_by_name.update((model.__tablename__, model) for model in evidence)
    assert len(models_by_name) == 42
    assert all(
        element.column.table.name in models_by_name
        for model in models_by_name.values()
        for constraint in model.__table__.foreign_key_constraints
        for element in constraint.elements
    )
    assert not models.PTG2Snapshot.__table__.c.import_run_id.foreign_keys
    assert models.PTG2V3SnapshotLayout.__table__.c.snapshot_key.identity is not None
    assert local_data_family_spec().model_types == (*family.model_types, *roots, *evidence)


@pytest.mark.asyncio
async def test_closed_family_heaps_keep_root_identity():
    session = SimpleNamespace(execute=AsyncMock())
    await archive._create_model_family(
        session,
        local_data_family_spec(),
        _binding().schema_name,
        create_indexes=False,
        ordinary_heaps=True,
    )
    statements = [str(call.args[0]) for call in session.execute.await_args_list]
    assert len(statements) == 43
    assert "GENERATED BY DEFAULT AS IDENTITY" in next(
        statement for statement in statements if "CREATE TABLE" in statement and "ptg2_v3_snapshot_layout" in statement
    )
    assert all("PARTITION BY" not in statement and "FOREIGN KEY" not in statement for statement in statements)


def test_local_fingerprint_uses_ordinary_heap_columns_without_changing_models(monkeypatch):
    from sqlalchemy import schema

    compiled_columns = []
    create_column = schema.CreateColumn

    def record_column(column):
        compiled_columns.append(column)
        return create_column(column)

    monkeypatch.setattr(schema, "CreateColumn", record_column)
    binding_module.local_data_model_digest()
    assert all(column.autoincrement is False for column in compiled_columns if column.identity is None)
    assert [column.table.name for column in compiled_columns if column.identity is not None] == [
        "ptg2_v3_snapshot_layout"
    ]
    assert models.PTG2V3SourceAuditWitness.__table__.c.snapshot_key.autoincrement == "auto"
