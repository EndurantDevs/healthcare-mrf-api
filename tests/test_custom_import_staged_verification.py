# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Preserve exact set checks while checkpointing only completed groups."""

import importlib.util
from pathlib import Path
from types import SimpleNamespace

import pytest

_REVISION = "20261020150000_custom_import_staged_verification"


def _migration():
    path = Path(__file__).resolve().parents[1] / "alembic/versions" / f"{_REVISION}.py"
    spec = importlib.util.spec_from_file_location("staged_verification_test", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_queries_preserve_corrected_validation_verbatim_and_in_order():
    migration = _migration()
    finality = migration._finality()
    corrected = migration._previous("20261007000000_custom_import_rejection_anti_joins")
    assert migration.down_revision == "20261019010000_custom_import_build_continuation"
    assert migration._queries(finality) == (
        finality._resource("graph_relationships"),
        corrected._source_query(),
        finality._resource("retained_lineage"),
        corrected._output_query(),
        finality._resource("legacy_origins"),
    )
    schema = """synthetic "quoted' control"""
    body = migration._body(schema)
    bulk, storage = finality._bulk(), finality._bulk()._storage()
    positions = [
        body.index(storage._literal(bulk._control_sql(storage, schema, query)))
        for query in (*migration._queries(finality), finality._COUNTS_QUERY)
    ]
    assert positions == sorted(positions)
    assert "__CONTROL__" not in body and "__CONTROL_LITERAL__" not in body
    assert "verify_custom_import_snapshot_structure(b.generation_id)" not in body
    assert "statement_timeout" not in body


def test_progress_rejects_old_cursors_and_keeps_complete_transition_last():
    migration = _migration()
    body = migration._body("synthetic_control")
    assert migration._STAGES == ("capture", "profiles", "families", "root_scalars", "child_scalars", "winners")
    assert "v.page_sequence IS DISTINCT FROM (stage-1)::bigint" in body
    assert all(f"v.{name}" in body for name in (*migration._CURSORS, *migration._COUNTS))
    assert "ROW(v.generation_id,v.source_frozen_at,v.graph_frozen_at,v.output_frozen_at)" in body
    assert "position('__ORIGIN_QUERY__' IN query)>0 AND b.build_id IS NOT NULL" in body
    assert all(
        f"verify_custom_import_snapshot_indexes(snapshot_id,'{phase}')" in body
        for phase in ("serving", "admission", "graph", "output")
    )
    checkpoint = body.index("SET scan_stage=stages[stage+1],page_sequence=v.page_sequence+1")
    assert body.rfind("lock_custom_import_snapshot_finality", 0, checkpoint) > body.index("EXECUTE query INTO counts")
    assert checkpoint < body.index("(counts->>'family_count')::bigint<>b.selected_family_count")
    assert body.index("verification_state='complete'", checkpoint) < body.index("SET phase='verified'", checkpoint)


@pytest.mark.parametrize("updated,incomplete", ((False, False), (True, False), (True, True), (False, True)))
def test_upgrade_rejects_historical_partial_proofs_without_rewriting_rows(monkeypatch, updated, incomplete):
    migration = _migration()
    schema = "synthetic_control"
    finality = migration._finality()
    bulk, storage = finality._bulk(), finality._bulk()._storage()
    previous = " " + bulk._control_sql(storage, schema, finality._BUILD_BODY) + " "
    replacement = " " + migration._body(schema) + " "
    installed = replacement if updated else previous
    statements, refreshes = [], []

    def execute(statement, parameters=None):
        statements.append(str(statement))
        return SimpleNamespace(
            scalar_one_or_none=lambda: installed,
            scalar_one=lambda: incomplete,
        )

    original = migration._previous
    continuation = SimpleNamespace(_refresh=lambda *args: refreshes.append(args))
    monkeypatch.setattr(migration, "_schema", lambda: schema)
    monkeypatch.setattr(
        migration,
        "_previous",
        lambda name: continuation if name == "20261019010000_custom_import_build_continuation" else original(name),
    )
    monkeypatch.setattr(migration.op, "get_bind", lambda: SimpleNamespace(execute=execute))
    monkeypatch.setattr(migration.op, "execute", lambda statement: statements.append(str(statement)))
    if incomplete and not updated:
        with pytest.raises(RuntimeError, match="incompatible_progress"):
            migration.upgrade()
        assert refreshes == []
    else:
        migration.upgrade()
        assert refreshes == [(f'"{schema}".verify_custom_import_build_structure(bigint)', installed, replacement)]
    assert not any(statement.startswith(("UPDATE", "INSERT", "DELETE", "ALTER", "CREATE")) for statement in statements)
    assert any(statement.startswith("LOCK TABLE") for statement in statements)


def test_unrecognized_body_is_not_replaced(monkeypatch):
    migration = _migration()
    statements = []
    monkeypatch.setattr(migration, "_schema", lambda: "synthetic_control")
    monkeypatch.setattr(
        migration.op,
        "get_bind",
        lambda: SimpleNamespace(execute=lambda *args: SimpleNamespace(scalar_one_or_none=lambda: "unrecognized")),
    )
    monkeypatch.setattr(migration.op, "execute", statements.append)
    with pytest.raises(RuntimeError, match="identity_mismatch"):
        migration.upgrade()
    assert not statements
