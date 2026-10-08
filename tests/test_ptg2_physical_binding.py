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
    physical_family_spec,
)
from process.ptg_parts.ptg2_snapshot_candidates import snapshot_candidate_relation
from process.ptg_parts.result_archive_adoption import _REKEYED_TABLES
from process.ptg_parts.result_archive_candidate_preparation import _ALLOWED_AMOUNT_TABLES
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


def _serving_scope():
    return {
        "contract": binding_module.SERVING_SCOPE_CONTRACT,
        "snapshot_id": "synthetic-source",
        "source_key": "synthetic_source",
        "coverage_scope_id": "a" * 64,
        "primary_plan": ["plan-a", "individual"],
        "plan_scopes": [["plan-a", "individual"], ["plan-b", "individual"]],
        "source_assignments": [
            {
                "source_key": 0,
                "source_type": "in_network",
                "identity_kind": "logical_json_sha256_v1",
                "identity_sha256": "b" * 64,
                "raw_container_sha256": "c" * 64,
                "logical_json_sha256": "b" * 64,
                "logical_hash_deferred": False,
                "source_trace_set_hash": "d" * 64,
            }
        ],
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("refusal", [None, "unprotected", "different-owner"])
async def test_local_control_catalog_preserves_only_initial_insert_exceptions(refusal):
    calls = []

    async def execute(statement, parameters=None):
        calls.append((str(statement), parameters))
        if str(statement).startswith("LOCK"):
            return None
        table = parameters["table"]
        proof_by_field = {
            "owner_oid": 102 if refusal != "different-owner" or table.endswith("preparation") else 103,
            "protected": refusal != "unprotected",
        }
        return SimpleNamespace(mappings=lambda: SimpleNamespace(one_or_none=lambda: proof_by_field))

    session = SimpleNamespace(in_transaction=lambda: True, execute=execute)
    if refusal:
        with pytest.raises(PTG2PhysicalBindingError, match="control catalog"):
            await binding_module._local_preparation_owner(session)
    else:
        assert await binding_module._local_preparation_owner(session) == 102
        probes = [(query, parameters) for query, parameters in calls if not query.startswith("LOCK")]
        assert [parameters["child"] for _query, parameters in probes] == [False, True, True]
        assert all(
            "validation" not in parameters["initial_columns"] and "state" not in parameters["initial_columns"]
            for _query, parameters in probes
        )
        assert all(
            "a.is_grantable" in query and "pg_trigger" in query and "pg_inherits" in query
            for query, _parameters in probes
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("drift", [None, "extra-relation", "sequence", "digest"])
async def test_native_preparation_inventory_binds_exact_children_and_identity(drift):
    physical = _binding()
    ownership = archive.ReferenceFamilyStageOwnership(
        local_data_family_spec().importer_id,
        physical.dataset_id,
        physical.schema_name,
        physical.schema_oid,
        tuple(sorted(physical.relation_oids)),
        physical.sequence_oids,
    )
    relations = [
        {"ordinal": ordinal, "table_name": name, "relation_oid": oid}
        for ordinal, (name, oid) in enumerate(ownership.relation_oids)
    ]
    sequences = [
        {"ordinal": ordinal, "sequence_name": name, "sequence_oid": oid, "owner_table": table, "owner_column": column}
        for ordinal, (name, oid, table, column) in enumerate(ownership.sequence_oids)
    ]
    inventory_by_field = {
        "schema_name": ownership.schema_name,
        "schema_oid": ownership.schema_oid,
        "relations": [{key: field for key, field in relation.items() if key != "ordinal"} for relation in relations],
        "sequences": [{key: field for key, field in sequence.items() if key != "ordinal"} for sequence in sequences],
    }
    inventory_sha = hashlib.sha256(
        json.dumps(inventory_by_field, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    if drift == "extra-relation":
        relations.append({"ordinal": 41, "table_name": "unexpected_table", "relation_oid": 999})
    if drift == "sequence":
        sequences[0]["sequence_oid"] = 998
    preparation_by_field = {
        "operation_id": UUID(int=3),
        "inventory_sha256": "0" * 64 if drift == "digest" else inventory_sha,
    }
    session = SimpleNamespace(
        execute=AsyncMock(
            side_effect=[
                SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: relations)),
                SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: sequences)),
            ]
        )
    )
    if drift:
        with pytest.raises(PTG2PhysicalBindingError, match="inventory"):
            await binding_module._require_local_preparation_inventory(session, preparation_by_field, ownership)
    else:
        await binding_module._require_local_preparation_inventory(session, preparation_by_field, ownership)
    assert all(call.args[1] == {"operation_id": UUID(int=3)} for call in session.execute.await_args_list)


@pytest.mark.asyncio
@pytest.mark.parametrize("refusal", [None, "lease", "publisher", "owner", "contract"])
async def test_called_frozen_preparation_rejoins_actual_lease_and_native_custody(monkeypatch, refusal):
    operation_by_field = {
        "operation_id": UUID(int=3),
        "lease_token": UUID(int=4),
        "attempt": 1,
        "package_id": "a" * 64,
        "node_id": "synthetic",
        "importer_id": "ptg",
        "dataset_key": "ptg.synthetic",
    }
    proof_by_field = {
        **operation_by_field,
        "publisher": True,
        "state": "frozen",
        "profile_contract": "ptg_result.postgres.v2",
        "frozen_owner_oid": 102,
        "stage_schema": "synthetic",
        "stage_schema_oid": 103,
    }
    match refusal:
        case "lease":
            proof_by_field["lease_token"] = UUID(int=5)
        case "publisher":
            proof_by_field["publisher"] = False
        case "owner":
            proof_by_field["frozen_owner_oid"] = 104
        case "contract":
            proof_by_field["profile_contract"] = "ptg_result.postgres.v1"
    session = SimpleNamespace(
        execute=AsyncMock(
            return_value=SimpleNamespace(mappings=lambda: SimpleNamespace(one_or_none=lambda: proof_by_field))
        )
    )
    ownership = SimpleNamespace(schema_name="synthetic", schema_oid=103)
    monkeypatch.setattr(binding_module, "_local_preparation_owner", AsyncMock(return_value=102))
    inventory_read, native_read, closed_read = AsyncMock(), AsyncMock(), AsyncMock()
    monkeypatch.setattr(binding_module, "_require_local_preparation_inventory", inventory_read)
    monkeypatch.setattr(binding_module, "verify_local_data_family", native_read)
    monkeypatch.setattr(binding_module, "_require_closed_local_custody", closed_read)
    if refusal:
        with pytest.raises(PTG2PhysicalBindingError, match="custody differs"):
            await binding_module.require_frozen_local_preparation(
                session, operation=operation_by_field, ownership=ownership
            )
    else:
        assert (
            await binding_module.require_frozen_local_preparation(
                session, operation=operation_by_field, ownership=ownership
            )
            == 102
        )
        closed_read.assert_awaited_once_with(session, ownership, 102)
    assert native_read.await_count == inventory_read.await_count == (0 if refusal else 1)
    query = str(session.execute.await_args.args[0])
    assert "JOIN" not in query
    assert "FOR SHARE OF p NOWAIT" in query and "FOR SHARE OF p,operation" not in query


@pytest.mark.asyncio
@pytest.mark.parametrize("missing_columns", [False, True])
async def test_native_catalog_digest_preserves_protected_receipt_encoding(missing_columns):
    objects = [{"kind": "relation", "oid": 101, "name": "synthetic_table", "owner": 102, "definition": "r:p:103:0"}]
    columns = (
        []
        if missing_columns
        else [
            {
                "table_name": "synthetic_table",
                "column_name": "identity",
                "data_type": "bigint",
                "attnotnull": True,
                "attidentity": "",
                "attgenerated": "",
            }
        ]
    )
    session = SimpleNamespace(
        in_transaction=lambda: True,
        execute=AsyncMock(
            side_effect=[
                SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: objects)),
                SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: columns)),
            ]
        ),
    )
    ownership = SimpleNamespace(schema_name="synthetic", schema_oid=104, relation_oids=(("synthetic_table", 101),))
    if missing_columns:
        with pytest.raises(PTG2PhysicalBindingError, match="columns differ"):
            await binding_module.local_data_catalog_digest(session, ownership)
    else:
        expected_by_field = {
            "objects": objects,
            "columns": [
                {
                    "table": "synthetic_table",
                    "column": "identity",
                    "type": "bigint",
                    "not_null": True,
                    "identity": "",
                    "generated": "",
                }
            ],
        }
        expected_sha = hashlib.sha256(
            json.dumps(
                expected_by_field, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False
            ).encode("ascii")
        ).hexdigest()
        assert await binding_module.local_data_catalog_digest(session, ownership) == expected_sha
    assert "pg_index" in str(session.execute.await_args_list[0].args[0])
    assert "pg_attribute" in str(session.execute.await_args_list[1].args[0])


@pytest.mark.asyncio
async def test_driver_catalog_digest_uses_identical_protected_encoding():
    """Retained driver callers use the same catalog statements and encoding."""
    objects = [{"kind": "relation", "oid": 101, "name": "synthetic_table", "owner": 102, "definition": "r:p:103:0"}]
    columns = [
        {
            "table_name": "synthetic_table",
            "column_name": "identity",
            "data_type": "bigint",
            "attnotnull": True,
            "attidentity": "",
            "attgenerated": "",
        }
    ]
    ownership = SimpleNamespace(schema_name="synthetic", schema_oid=104, relation_oids=(("synthetic_table", 101),))
    connection = SimpleNamespace(is_in_transaction=lambda: True, fetch=AsyncMock(side_effect=[objects, columns]))
    assert await binding_module.local_data_driver_catalog_digest(
        connection, ownership
    ) == binding_module._local_catalog_digest(ownership, objects, columns)
    assert connection.fetch.await_args_list[0].args == (binding_module._local_catalog_queries("$1", "$1", "$2")[0], 104)
    assert connection.fetch.await_args_list[1].args[1:] == ("synthetic", ["synthetic_table"])
    connection.is_in_transaction = lambda: False
    with pytest.raises(PTG2PhysicalBindingError, match="requires a transaction"):
        await binding_module.local_data_driver_catalog_digest(connection, ownership)
    assert connection.fetch.await_count == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("drift", [None, "write-grant", "missing-object"])
async def test_driver_custody_checks_all_nonowner_acl_paths(drift):
    """Unknown grantees and missing native objects cannot pass retained custody."""
    ownership = SimpleNamespace(
        schema_oid=104,
        relation_oids=(("synthetic_table", 101),),
        sequence_oids=(("id_seq", 102, "synthetic_table", "id"),),
    )
    proof_by_field = {"object_count": 1 if drift == "missing-object" else 2, "closed": drift != "write-grant"}
    connection = SimpleNamespace(is_in_transaction=lambda: True, fetchrow=AsyncMock(return_value=proof_by_field))
    if drift:
        with pytest.raises(PTG2PhysicalBindingError, match="not closed"):
            await binding_module.require_closed_local_driver_custody(connection, ownership, 105)
    else:
        await binding_module.require_closed_local_driver_custody(connection, ownership, 105)
    query, *arguments = connection.fetchrow.await_args.args
    assert arguments == [105, 104, [101], [102], [101, 102]]
    assert "a.grantee<>c.relowner" in query and "a.privilege_type<>'SELECT'" in query
    assert "aclexplode(col.attacl)" in query and "c.relkind='S'" in query
    assert "contype='f'" in query and "pg_inherits" in query


@pytest.mark.parametrize(
    "refusal", [None, "missing-primary", "duplicate", "truncated", "source-semantics", "unbounded"]
)
def test_local_scope_requires_complete_bounded_plans_and_native_source_semantics(refusal):
    scope = _serving_scope()
    match refusal:
        case "missing-primary":
            scope["primary_plan"] = ["another-plan", "individual"]
        case "duplicate":
            scope["plan_scopes"].append(scope["plan_scopes"][0])
        case "truncated":
            scope["plan_scopes"] = []
        case "source-semantics":
            scope["source_assignments"][0]["logical_hash_deferred"] = True
        case "unbounded":
            scope["source_assignments"] *= 257
    if refusal:
        with pytest.raises(PTG2PhysicalBindingError):
            binding_module.validate_local_serving_scope(scope)
    else:
        assert binding_module.validate_local_serving_scope(scope) == scope


@pytest.mark.asyncio
async def test_local_scope_capture_reads_fixed_columns_and_refuses_overflow():
    scope = _serving_scope()
    primary_by_field = {
        "plan_id": scope["primary_plan"][0],
        "plan_market_type": scope["primary_plan"][1],
        "coverage_scope_id": scope["coverage_scope_id"],
    }
    plans = [{"plan_id": plan, "plan_market_type": market} for plan, market in scope["plan_scopes"]]
    results = [
        SimpleNamespace(mappings=lambda: SimpleNamespace(one=lambda: primary_by_field)),
        SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: plans)),
        SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: deepcopy(scope["source_assignments"]))),
    ]
    session = SimpleNamespace(in_transaction=lambda: True, execute=AsyncMock(side_effect=results))
    assert (
        await binding_module.capture_local_serving_scope(
            session, schema_name="synthetic", snapshot_id=scope["snapshot_id"], source_key=scope["source_key"]
        )
        == scope
    )
    assert all(call.args[1] == {"snapshot_id": scope["snapshot_id"]} for call in session.execute.await_args_list)
    assert all("FOR KEY SHARE" in str(call.args[0]) for call in session.execute.await_args_list)
    assert all("LIMIT 257" in str(call.args[0]) for call in session.execute.await_args_list[1:])
    plans[:] = [{"plan_id": f"plan-{ordinal:03}", "plan_market_type": "individual"} for ordinal in range(257)]
    session.execute.side_effect = results
    with pytest.raises(PTG2PhysicalBindingError):
        await binding_module.capture_local_serving_scope(
            session, schema_name="synthetic", snapshot_id=scope["snapshot_id"], source_key=scope["source_key"]
        )


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
        101,
        102,
        tuple((name, 200 + ordinal) for ordinal, name in enumerate(local_data_family_spec().table_names)),
        (("ptg2_v3_snapshot_layout_snapshot_key_seq", 300, "ptg2_v3_snapshot_layout", "snapshot_key"),),
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
    assert len(binding.relation_oids) == 42 and len(binding.sequence_oids) == 1
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
        {"schema_oid": False},
        {"owner_oid": 0},
        {"dataset_id": "untrusted-schema"},
        {"relation_oids": ()},
        {"sequence_oids": ()},
        {"sequence_oids": (("unreviewed_sequence", 300, "ptg2_v3_snapshot_layout", "snapshot_key"),)},
        {
            "sequence_oids": (
                ("ptg2_v3_snapshot_layout_snapshot_key_seq", 200, "ptg2_v3_snapshot_layout", "snapshot_key"),
            )
        },
    ),
)
def test_binding_rejects_partial_or_malformed_identity(change):
    with pytest.raises(PTG2PhysicalBindingError):
        replace(_binding(), **change)


def test_binding_rejects_duplicate_heap_oids_and_reordered_or_arbitrary_model_names():
    binding = _binding()
    for inventory in (
        tuple((name, 300) for name, _oid in binding.relation_oids),
        tuple(reversed(binding.relation_oids)),
        (("unreviewed_relation", 300), *binding.relation_oids[1:]),
    ):
        with pytest.raises(PTG2PhysicalBindingError, match="inventory"):
            replace(binding, relation_oids=inventory)


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


@pytest.mark.asyncio
async def test_complete_data_family_finishes_all_native_constraints_before_relationship_checks(monkeypatch):
    statements = []

    async def execute(statement):
        statements.append(str(statement.compile(dialect=postgresql.dialect())))

    async def has_missing_parent(_statement):
        for model in local_data_family_spec().model_types:
            assert any(
                f"ALTER TABLE {_binding().schema_name}.{model.__tablename__} ADD" in statement
                and "PRIMARY KEY" in statement
                for statement in statements
            ), model.__tablename__
        assert any("ptg2_v3_snapshot_layout_state_check" in statement for statement in statements)
        assert any(
            f"{_binding().schema_name}.ptg2_snapshot(import_month)" in _normalized_sql(statement)
            for statement in statements
        )
        return False

    session = SimpleNamespace(in_transaction=lambda: True, execute=execute, scalar=has_missing_parent)
    ownership = archive.ReferenceFamilyStageOwnership(
        local_data_family_spec().importer_id,
        _binding().dataset_id,
        _binding().schema_name,
        200,
        tuple((name, oid) for oid, name in enumerate(sorted(local_data_family_spec().table_names), 300)),
        (("ptg2_v3_snapshot_layout_snapshot_key_seq", 900, "ptg2_v3_snapshot_layout", "snapshot_key"),),
    )
    monkeypatch.setattr(archive, "_capture_model_family_ownership", AsyncMock(return_value=ownership))
    await binding_module.complete_local_data_family(session, ownership)
    assert all("FOREIGN KEY" not in statement for statement in statements)


@pytest.mark.asyncio
async def test_closed_data_custody_includes_only_the_exact_model_owned_identity_sequence(monkeypatch):
    names = local_data_family_spec().table_names
    oid_by_table = {name: oid for oid, name in enumerate(names, 300)}
    sequence = ("ptg2_v3_snapshot_layout_snapshot_key_seq", 900, "ptg2_v3_snapshot_layout", "snapshot_key")
    rows = [{"oid": oid, "relname": name, "relkind": "r"} for name, oid in oid_by_table.items()]
    rows.append({"oid": 900, "relname": sequence[0], "relkind": "S"})
    session = SimpleNamespace(in_transaction=lambda: True)
    monkeypatch.setattr(archive, "_schema_oid", AsyncMock(return_value=200))
    monkeypatch.setattr(
        archive, "_relation_oid", AsyncMock(side_effect=lambda _session, _schema, name: oid_by_table[name])
    )
    sequences = AsyncMock(return_value=(sequence,))
    monkeypatch.setattr(archive, "_owned_sequences", sequences)
    monkeypatch.setattr(archive, "_namespace_relations", AsyncMock(return_value=rows))
    ownership = await binding_module.capture_local_data_family(session, _binding().dataset_id)
    assert ownership.sequence_oids == (sequence,)
    sequences.assert_awaited_once_with(session, 200, include_identity=True)
    await binding_module.verify_local_data_family(session, ownership)
    sequences.return_value = (("unreviewed_sequence", 901, "ptg2_snapshot", "snapshot_id"),)
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="sequence set"):
        await binding_module.capture_local_data_family(session, ownership.dataset_id)


@pytest.mark.asyncio
@pytest.mark.parametrize("refusal", [None, "scope", "count", "extra-cas", "layout"])
async def test_local_semantic_validation_reuses_canonical_closure_and_rejects_extra_rows(monkeypatch, refusal):
    from process.ptg_parts import result_archive_closure as closure

    names = local_data_family_spec().table_names
    counts = dict.fromkeys(names, 1)
    counts.update(ptg2_v3_block=2, ptg2_v4_snapshot_map_pack=3, ptg2_v4_finalizer_map_pack=4)
    metadata = {
        "source_snapshot_id": "synthetic-source",
        "source_snapshot_key": 19,
        "row_counts": counts,
        "layout_mapping_sha256": "a" * 64,
        "map_sha256": "b" * 64,
        "finalizer_map_sha256": "c" * 64,
    }
    observed_count_by_table = dict(counts)
    if refusal in {"count", "extra-cas"}:
        observed_count_by_table["ptg2_v3_block"] += 1
        if refusal == "extra-cas":
            metadata["row_counts"] = observed_count_by_table

    async def scalar(statement, _parameters=None):
        if str(statement).startswith("SELECT EXISTS"):
            return refusal == "scope"
        return next(value for name, value in observed_count_by_table.items() if str(statement).endswith(f'."{name}"'))

    session = SimpleNamespace(in_transaction=lambda: True, scalar=scalar)
    ownership = SimpleNamespace(schema_name=_binding().schema_name)
    monkeypatch.setattr(binding_module, "verify_local_data_family", AsyncMock())
    monkeypatch.setattr(binding_module, "_validate_local_root_coordinates", lambda *_args: _serving_scope())
    monkeypatch.setattr(binding_module, "capture_local_serving_scope", AsyncMock(return_value=_serving_scope()))
    monkeypatch.setattr(binding_module, "_validate_local_graph_reachability", AsyncMock())
    locked = AsyncMock(return_value={"sealed": "data-not-authority"})
    blocks = AsyncMock(return_value=((b"1", b"2"), 3, 4))
    monkeypatch.setattr(closure, "_locked_layout", locked)
    monkeypatch.setattr(closure, "_archive_block_selection", blocks)
    monkeypatch.setattr(
        closure,
        "_validate_layout",
        lambda _layout: (
            20 if refusal == "layout" else 19,
            bytes.fromhex("a" * 64),
            bytes.fromhex("b" * 64),
            bytes.fromhex("c" * 64),
        ),
    )
    if refusal:
        with pytest.raises(PTG2PhysicalBindingError):
            await binding_module.validate_local_data_family(session, ownership, metadata)
        if refusal != "extra-cas":
            blocks.assert_not_awaited()
    else:
        receipt = await binding_module.validate_local_data_family(session, ownership, metadata)
        assert receipt["payload_snapshot_id"] == "synthetic-source"
        assert receipt["payload_snapshot_key"] == 19 and receipt["row_counts"] == counts
        assert receipt["model_sha256"] == binding_module.local_data_model_digest()
        blocks.assert_awaited_once()
    assert locked.await_args.kwargs["payload_snapshot_key"] == 19


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


@pytest.mark.parametrize(
    "refusal",
    [
        None,
        "manifest",
        "snapshot",
        "source",
        "coverage",
        "layout-coverage",
        "source-count",
        "source-set",
        "missing-set",
        "published-set",
    ],
)
def test_local_root_coordinates_recheck_captured_scope_and_source_semantics(refusal):
    from process.ptg_parts.result_archive_source_authority import result_archive_manifest_sha256

    scope = _serving_scope()
    source_set_by_field = ptg2_tables._validated_published_source_set(
        scope["source_assignments"], expected_source_count=1
    )
    manifest_by_field = {
        "serving_index": {
            "coverage_scope_id": scope["coverage_scope_id"],
            "source_count": 1,
            "source_set": source_set_by_field,
        }
    }
    layout_by_field = {"manifest": manifest_by_field, "layout_manifest": deepcopy(manifest_by_field)}
    metadata = {
        "source_snapshot_id": scope["snapshot_id"],
        "closure_metadata": {"serving_scope": scope},
        "source_publication": {
            "source_key": scope["source_key"],
            "snapshot_manifest_sha256": result_archive_manifest_sha256(manifest_by_field),
        },
    }
    match refusal:
        case "manifest":
            manifest_by_field["another_field"] = "different"
        case "snapshot":
            metadata["source_snapshot_id"] = "another-snapshot"
        case "source":
            metadata["source_publication"]["source_key"] = "another_source"
        case "layout-coverage":
            layout_by_field["layout_manifest"]["serving_index"]["coverage_scope_id"] = "0" * 64
        case "coverage":
            manifest_by_field["serving_index"]["coverage_scope_id"] = "0" * 64
        case "source-count":
            manifest_by_field["serving_index"]["source_count"] = 2
        case "source-set":
            manifest_by_field["serving_index"]["source_set"] = {
                **source_set_by_field,
                "raw_container_sha256_digest": "0" * 64,
            }
        case "missing-set":
            manifest_by_field["serving_index"].pop("source_set")
        case "published-set":
            metadata["source_publication"]["identity"] = {
                **metadata["source_publication"],
                "source_set_digest": "0" * 64,
            }
    if refusal in {"coverage", "source-count", "source-set", "missing-set"}:
        metadata["source_publication"]["snapshot_manifest_sha256"] = result_archive_manifest_sha256(manifest_by_field)
    if refusal:
        with pytest.raises(PTG2PhysicalBindingError, match="root coordinates differ"):
            binding_module._validate_local_root_coordinates(layout_by_field, metadata)
    else:
        assert binding_module._validate_local_root_coordinates(layout_by_field, metadata) == scope


@pytest.mark.asyncio
@pytest.mark.parametrize("refusal", [None, "orphan", "indeterminate"])
async def test_selected_graph_reachability_rejects_unreferenced_rows(refusal):
    session = SimpleNamespace(scalar=AsyncMock(return_value=False if refusal is None else refusal == "orphan" or None))
    if refusal:
        with pytest.raises(PTG2PhysicalBindingError, match="graph reachability"):
            await binding_module._validate_local_graph_reachability(session, "synthetic_graph", "synthetic-source")
    else:
        await binding_module._validate_local_graph_reachability(session, "synthetic_graph", "synthetic-source")
        assert session.scalar.await_count == 5
        assert all(call.args[1] == {"snapshot_id": "synthetic-source"} for call in session.scalar.await_args_list)
        statements = [str(call.args[0]) for call in session.scalar.await_args_list]
        assert all(
            "IS NOT TRUE" in statement and "WHERE snapshot_id = :snapshot_id" in statement for statement in statements
        )
        assert all("synthetic_graph" in statement for statement in statements)
