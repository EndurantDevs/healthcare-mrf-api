# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fixed complete-set validation and candidate-only index lifecycle contracts."""

import importlib.util
import json
import re
import sqlite3
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from db.models.custom_import import (
    CustomImportBuildCandidateContext,
    CustomImportFamilyChild,
    CustomImportGenerationFamily,
)
from process.custom_import import build_source as source
from process.custom_import.storage_layout import snapshot_phase_index_statements, snapshot_serving_index_statements


def _migration():
    path = Path(__file__).resolve().parents[1] / "alembic/versions/20261005060000_custom_import_snapshot_finality.py"
    spec = importlib.util.spec_from_file_location("snapshot_finality_contract", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_fixed_index_definitions_match_the_model_and_are_candidate_only():
    migration = _migration()
    specifications = json.loads(migration._INDEX_SPECIFICATIONS)
    for phase in ("admission", "graph", "output", "serving"):
        phase_specifications = list(specifications)
        if phase == "admission":
            child_index = next(item for item in specifications if item["name"] == "custom_import_build_graph_child_idx")
            phase_specifications.append({**child_index, "phase": "admission"})
        expected_statements = (
            snapshot_serving_index_statements(1) if phase == "serving" else snapshot_phase_index_statements(1, phase)
        )
        actual_statements = tuple(
            item["ddl"].replace("__CANDIDATE__", "ci_snapshot_1")
            for item in sorted(phase_specifications, key=lambda item: item["name"] if phase != "serving" else "")
            if item["phase"] == phase
        )
        assert actual_statements == expected_statements
    assert len(specifications) == 20
    assert "RETURN false" in migration._INDEX_BODY
    assert "custom_import_snapshot_index_mismatch" in migration._INDEX_BODY
    assert "DROP INDEX" not in migration._INDEX_BODY and "IF NOT EXISTS" not in migration._INDEX_BODY


def test_upgrade_preserves_existing_oids_and_acls_and_closes_new_defaults(monkeypatch):
    migration = _migration()
    statements = []
    monkeypatch.setattr(migration, "_schema", lambda: "synthetic_control")
    monkeypatch.setattr(migration.op, "execute", statements.append)
    migration.upgrade()
    function_definitions = [statement for statement in statements if statement.startswith("CREATE")]
    existing_definitions = [
        statement for statement in function_definitions if statement.startswith("CREATE OR REPLACE")
    ]
    assert len(existing_definitions) == 2
    assert any(".verify_custom_import_build_structure(" in statement for statement in existing_definitions)
    assert any(".guard_custom_import_generation_seal_insert(" in statement for statement in existing_definitions)
    verify = next(
        statement for statement in function_definitions if ".verify_custom_import_snapshot_indexes(" in statement
    )
    assert "EXECUTE replace(item->>'ddl'" not in verify and "custom_import_snapshot_index_missing" in verify
    prepare = next(
        statement for statement in function_definitions if ".prepare_custom_import_snapshot_indexes(" in statement
    )
    assert "ANALYZE" not in verify
    statistics = prepare.index("EXECUTE 'ANALYZE '")
    assert prepare.index("IF p_phase='serving' THEN", prepare.index("    END LOOP;")) < statistics
    assert "table_oid::oid::regclass::text" in prepare
    assert "custom_import_snapshot_relation WHERE family_id=f.family_id" in prepare
    assert statistics < prepare.rindex("lock_custom_import_snapshot_attempt")
    assert len([statement for statement in statements if statement.lstrip().startswith("DO ")]) == 3
    assert not any("__CONTROL__" in statement or "__SCHEMA__" in statement for statement in statements)
    assert not any(statement.lstrip().startswith(("INSERT", "UPDATE", "DELETE", "GRANT")) for statement in statements)


def test_missing_membership_probe_preserves_full_scope_and_uses_nonnull_primary_key():
    query = _migration()._resource("graph_relationships").split("'family_missing_generation_membership'", 1)[1]
    assert "m.generation_id=$1 AND m.root_record_id=f.root_record_id" in query
    assert (
        "ROW(m.generation_id,m.dataset_id,m.definition_revision_id,m.schema_revision_id,m.root_record_id,m.family_revision_id)"
        in query
    )
    assert "IS NOT DISTINCT FROM ROW($1,$2,$3,$4,f.root_record_id,f.family_revision_id)" in query
    primary_key = CustomImportGenerationFamily.__table__.primary_key
    assert tuple(primary_key.columns.keys()) == ("generation_id", "root_record_id")
    assert all(not column.nullable for column in primary_key.columns)


def test_complete_structure_has_no_per_family_or_per_child_database_progress():
    migration = _migration()
    assert "verify_custom_import_snapshot_structure(b.generation_id)" in migration._BUILD_BODY
    assert "LIMIT 1" not in migration._BUILD_BODY and "LOOP" not in migration._BUILD_BODY
    assert "current_family_revision_id=NULL" in migration._BUILD_BODY
    assert "source_frozen_at" in migration._BUILD_BODY and "page_sequence=v.page_sequence+1" in migration._BUILD_BODY
    assert "base_generation_id" in migration._VALIDATE_BODY
    assert "lock_custom_import_snapshot_finality" in migration._VALIDATE_BODY
    assert "EXCEPTION WHEN" not in migration._VALIDATE_BODY
    for name in migration._QUERY_NAMES:
        query = migration._resource(name)
        assert "SELECT violation.* FROM" in query
        assert "__SCHEMA__" not in query
        assert "FOR UPDATE" not in query and "LOOP" not in query


def _rejection_reference_query():
    """Use the actual failure branch without the unrelated finality checks."""
    marker = "SELECT 'rejection_missing_occurrence'"
    path = Path(__file__).resolve().parents[1] / "alembic/versions/20261007000000_custom_import_rejection_anti_joins.py"
    spec = importlib.util.spec_from_file_location("rejection_probe_contract", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    query = module._source_query()
    return marker + query.split(marker, 1)[1].split("UNION ALL", 1)[0]


def test_rejection_reference_probe_keeps_two_scoped_equality_anti_joins():
    query = _rejection_reference_query()
    assert query.count("NOT EXISTS (") == 2
    assert query.count("JOIN expected_build b ON b.build_id=o.build_id") == 2
    assert "WHERE EXISTS(SELECT 1 FROM expected_build)" in query
    assert "WHERE o.rejection_id=r.rejection_id" in query
    assert "WHERE o.resolved_rejection_id=r.rejection_id" in query
    assert " OR " not in query


@pytest.mark.parametrize(
    ("build_ids", "rejection_ids", "occurrences", "missing"),
    (
        pytest.param((7,), (1, 2), (), {1, 2}, id="no-occurrences"),
        pytest.param((7,), (1, 2), ((7, "source", 1, None),), {2}, id="initial-only"),
        pytest.param((7,), (1, 2), ((7, "source", None, 1),), {2}, id="resolved-only"),
        pytest.param((7,), (1, 2), ((7, "source", 1, 2),), set(), id="both-distinct"),
        pytest.param((7,), (1, 2), ((7, "source", 1, 1),), {2}, id="same-reference"),
        pytest.param((7,), (1, 2), ((7, "source", 1, None), (7, "source", None, 1)), {2}, id="duplicate-reference"),
        pytest.param((7,), (1, 2), ((7, "source", None, None),), {1, 2}, id="null-references"),
        pytest.param((7,), (1, 2), ((8, "source", 1, None),), {1, 2}, id="foreign-initial"),
        pytest.param((7,), (1, 2), ((8, "source", None, 1),), {1, 2}, id="foreign-resolved"),
        pytest.param((7,), (1, 2), ((8, "source", 1, 2), (7, "source", 1, None)), {2}, id="mixed-builds"),
        pytest.param((7,), (1, 2), ((7, "retained", 1, 2),), set(), id="retained-origin"),
        pytest.param((), (1, 2), ((7, "source", 1, None),), set(), id="no-expected-build"),
        pytest.param((7,), (), ((7, "source", 1, 2),), set(), id="no-rejections"),
    ),
)
def test_rejection_reference_probe_preserves_complete_failure_set(build_ids, rejection_ids, occurrences, missing):
    current = _rejection_reference_query().replace("__CANDIDATE__.", "")
    previous = """
        SELECT 'rejection_missing_occurrence',r.rejection_id FROM custom_import_rejection r
        WHERE EXISTS(SELECT 1 FROM expected_build) AND NOT EXISTS (
            SELECT 1 FROM custom_import_build_occurrence o
            JOIN expected_build b ON b.build_id=o.build_id
            WHERE o.rejection_id=r.rejection_id OR o.resolved_rejection_id=r.rejection_id
        )
    """
    with sqlite3.connect(":memory:") as connection:
        connection.executescript("""
            CREATE TABLE expected_build(build_id INTEGER PRIMARY KEY);
            CREATE TABLE custom_import_rejection(rejection_id INTEGER PRIMARY KEY);
            CREATE TABLE custom_import_build_occurrence(
                build_id INTEGER NOT NULL, origin TEXT NOT NULL,
                rejection_id INTEGER, resolved_rejection_id INTEGER);
        """)
        connection.executemany("INSERT INTO expected_build VALUES (?)", ((value,) for value in build_ids))
        connection.executemany("INSERT INTO custom_import_rejection VALUES (?)", ((value,) for value in rejection_ids))
        connection.executemany("INSERT INTO custom_import_build_occurrence VALUES (?,?,?,?)", occurrences)
        expected_failures = {("rejection_missing_occurrence", value) for value in missing}
        assert set(connection.execute(previous)) == set(connection.execute(current)) == expected_failures


def test_legacy_finality_fallback_is_explicit_and_never_handles_a_registered_error():
    body = _migration()._bulk()._storage()._FINALITY_RESOLVER
    assert body.index("lock_custom_import_snapshot_finality") < body.index("RETURN NULL")
    assert "generation_id=g.generation_id" in body and "execution_id=g.execution_id" in body
    assert "custom_import_snapshot_existing_build_requires_migration" in body
    assert "IN ACCESS SHARE MODE" in body and "EXCEPTION WHEN" not in body


@pytest.mark.parametrize(
    ("alias", "model", "index_name", "key_columns"),
    (
        (
            "m",
            CustomImportGenerationFamily,
            "custom_import_generation_family_member_key",
            ("generation_id", "dataset_id", "family_revision_id"),
        ),
        (
            "e",
            CustomImportFamilyChild,
            "custom_import_family_child_pkey",
            ("family_revision_id", "collection_slot", "child_revision_id"),
        ),
        (
            "c",
            CustomImportBuildCandidateContext,
            "custom_import_build_context_order_idx",
            ("profile_slot", "entity_binding_id", "context_key_sha256"),
        ),
    ),
)
def test_output_lookup_predicates_preserve_complete_identity(alias, model, index_name, key_columns):
    query = _migration()._resource("output_relationships")
    match = re.search(
        rf"(?P<keys>{alias}\.\w+=w\.\w+(?:\s+AND\s+{alias}\.\w+=w\.\w+)*)\s+AND\s+"
        rf"(?P<row>ROW\({alias}\.[^)]*\)\s+IS NOT DISTINCT FROM ROW\([^)]*\))",
        query,
    )
    assert match is not None
    key_pairs = re.findall(r"(\w+\.\w+)=(\w+\.\w+)", match["keys"])
    assert tuple(left.split(".")[1] for left, _right in key_pairs) == key_columns
    assert all(not model.__table__.c[column].nullable for column in key_columns)
    index = next(
        constraint
        for constraint in (*model.__table__.constraints, *model.__table__.indexes)
        if constraint.name == index_name
    )
    index_prefix = (("build_id",) if alias == "c" else ()) + key_columns
    assert tuple(index.columns.keys())[: len(index_prefix)] == index_prefix

    left, right = re.findall(r"ROW\(([^)]+)\)", match["row"])
    row_pairs = tuple(zip(left.split(","), (column.strip() for column in right.split(",")), strict=True))
    assert set(key_pairs).issubset(row_pairs)
    assert len(row_pairs) == {"m": 5, "e": 6, "c": 5}[alias]
    if alias == "c":
        assert model.__table__.c.context_child_revision_id.nullable
        assert "context_child_revision_id" not in key_columns
    _assert_output_predicate_parity(alias, match, row_pairs)


def _assert_output_predicate_parity(alias, predicates, row_pairs):
    fields_by_alias = {}
    parameters_by_field = {}
    for ordinal, pair in enumerate(row_pairs, 1):
        field_value = b"x" * 32 if "sha256" in pair[0] else ordinal
        for field in pair:
            qualifier, column = field.split(".")
            fields_by_alias.setdefault(qualifier, []).append(column)
            parameters_by_field[field.replace(".", "_")] = field_value
    tables = ",".join(
        f"{qualifier}({','.join(columns)}) AS (VALUES ({','.join(':' + qualifier + '_' + column for column in columns)}))"
        for qualifier, columns in fields_by_alias.items()
    )
    # SQLite's row-value syntax omits ROW; its null-safe IS NOT DISTINCT FROM
    # evaluates the exact resource predicates without a native service or DDL.
    comparison = predicates["row"].replace("ROW(", "(")
    statement = f"WITH {tables} SELECT ({comparison}), ({predicates['keys']}) AND ({comparison}) FROM {','.join(fields_by_alias)}"
    cases = [(parameters_by_field, True)]
    for field, _expected in row_pairs:
        key = field.replace(".", "_")
        different = b"y" * 32 if isinstance(parameters_by_field[key], bytes) else parameters_by_field[key] + 1
        cases.append(({**parameters_by_field, key: different}, False))
    if alias in ("e", "c"):
        cases.append(({**parameters_by_field, "w_context_child_revision_id": None}, False))
    if alias == "c":
        cases.append(
            ({**parameters_by_field, "c_context_child_revision_id": None, "w_context_child_revision_id": None}, True)
        )
    with sqlite3.connect(":memory:") as connection:
        for parameters, expected in cases:
            assert connection.execute(statement, parameters).fetchone() == (expected, expected)


@pytest.mark.asyncio
async def test_index_preparation_uses_one_fenced_transaction_per_index(monkeypatch):
    entered_pages = []
    results = iter((False, False, True))

    @asynccontextmanager
    async def page(*_args):
        entered_pages.append(object())
        yield entered_pages[-1], SimpleNamespace(phase="admission")

    async def call(session, name, arguments):
        assert session is entered_pages[-1]
        assert name == "prepare_custom_import_snapshot_indexes"
        assert arguments == (("bigint", 7), ("text", "admission"))
        return SimpleNamespace(scalar_one=lambda: next(results))

    monkeypatch.setattr(source, "_page_session", page)
    monkeypatch.setattr(source, "_resolve_build_snapshot", AsyncMock(return_value=7))
    monkeypatch.setattr(source, "_call", call)
    await source._prepare_snapshot_indexes(object(), object(), 3, "admission")
    assert len(entered_pages) == 3


@pytest.mark.asyncio
@pytest.mark.parametrize("completion", [None, 1, "true"])
async def test_index_completion_requires_an_actual_native_boolean(monkeypatch, completion):
    @asynccontextmanager
    async def page(*_args):
        yield object(), SimpleNamespace(phase="admission")

    monkeypatch.setattr(source, "_page_session", page)
    monkeypatch.setattr(source, "_resolve_build_snapshot", AsyncMock(return_value=7))
    monkeypatch.setattr(source, "_call", AsyncMock(return_value=SimpleNamespace(scalar_one=lambda: completion)))
    with pytest.raises(source.CandidateRunnerError, match="invalid completion state"):
        await source._prepare_snapshot_indexes(object(), object(), 3, "admission")
