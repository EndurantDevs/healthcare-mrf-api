# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Exact SQL decomposition and compatibility of frozen check checkpoints."""

import importlib.util
import re
from pathlib import Path
from types import SimpleNamespace

import pytest

_REVISION = "20261020180000_custom_import_verification_arms"


def _migration():
    path = Path(__file__).resolve().parents[1] / "alembic/versions" / f"{_REVISION}.py"
    spec = importlib.util.spec_from_file_location("verification_arms_test", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_all_corrected_predicates_and_shared_ctes_are_preserved_exactly():
    migration = _migration()
    staged = migration._staged()
    groups = staged._queries(staged._finality())
    assert tuple(map(len, migration._LABELS)) == (25, 17, 12, 6)
    for query, labels in zip(groups[:4], migration._LABELS, strict=True):
        arms = migration._split(query, labels)
        first = re.search(r"(?m)^( +)SELECT '[a-z_]+'", query)
        assert first is not None
        is_materialized = "WITH violations AS MATERIALIZED (" in query
        closing = ") SELECT * FROM violations LIMIT 1;" if is_materialized else ") violation LIMIT 1;"
        prefix, suffix = query[: first.start()], query[query.index(closing) :]
        bodies = []
        for arm in arms:
            restored = arm.replace(
                "WITH violations(failure_code,row_identity) AS MATERIALIZED (", "WITH violations AS MATERIALIZED ("
            )
            restored = restored.replace(") violation(failure_code,row_identity) LIMIT 1;", ") violation LIMIT 1;")
            assert restored.startswith(prefix) and restored.endswith(suffix)
            bodies.append(restored[len(prefix) : -len(suffix)])
            assert "failure_code,row_identity" in arm
        assert prefix + (f"\n{first[1]}UNION ALL\n").join(bodies) + suffix == query
    assert len(migration._queries(staged)) == 61
    assert migration._queries(staged)[-1] == groups[-1]


@pytest.mark.parametrize("change", ("missing", "extra", "reordered", "wrapper", "delimiter"))
def test_query_drift_fails_closed(change):
    migration = _migration()
    staged = migration._staged()
    query = staged._queries(staged._finality())[0]
    if change == "missing":
        query = query.replace("SELECT 'generation_scope'", "SELECT 'changed_scope'")
    elif change == "extra":
        query = query.replace(
            "        UNION ALL\n        SELECT 'generation_scope'",
            "        UNION ALL\n        SELECT 'extra_scope',1\n        UNION ALL\n        SELECT 'generation_scope'",
        )
    elif change == "reordered":
        query = query.replace("SELECT 'generation_scope'", "SELECT 'temporary_scope'")
        query = query.replace("SELECT 'generation_target_required'", "SELECT 'generation_scope'")
        query = query.replace("SELECT 'temporary_scope'", "SELECT 'generation_target_required'")
    elif change == "wrapper":
        query = query.replace(") violation LIMIT 1;", ") violation;")
    else:
        query = query.replace(
            "        UNION ALL\n        SELECT 'generation_scope'", "        UNION\n        SELECT 'generation_scope'"
        )
    with pytest.raises(RuntimeError, match="query_source_mismatch"):
        migration._split(query, migration._LABELS[0])


def test_body_keeps_authority_counts_and_exact_stage_sequence():
    migration = _migration()
    staged = migration._staged()
    schema = """synthetic "quoted' control"""
    body = migration._body(schema)
    assert len(migration._STAGES) == 62
    assert migration._STAGES[24:26] == ("capture", "profiles")
    assert migration._STAGES[-3:] == ("root_scalars", "child_scalars", "winners")
    assert "stage:=v.page_sequence::integer+1" in body
    assert "v.scan_stage IS DISTINCT FROM stages[stage]" in body
    assert body.count("IF stage<cardinality(stages) THEN") == 2
    assert "statement_timeout" not in body and "__CONTROL__" not in body
    assert all(f"v.{name}" in body for name in (*staged._CURSORS, *staged._COUNTS))
    assert "ROW(v.generation_id,v.source_frozen_at,v.graph_frozen_at,v.output_frozen_at)" in body
    checkpoint = body.index("SET scan_stage=stages[stage+1],page_sequence=v.page_sequence+1")
    assert body.rfind("lock_custom_import_snapshot_finality", 0, checkpoint) > body.index("EXECUTE query INTO counts")
    assert body.rfind("lock_custom_import_build", 0, checkpoint) > body.index("EXECUTE query INTO counts")
    assert checkpoint < body.index("(counts->>'family_count')::bigint<>b.selected_family_count")
    assert all(
        f"verify_custom_import_snapshot_indexes(snapshot_id,'{phase}')" in body
        for phase in ("serving", "admission", "graph", "output")
    )
    assert "position('__ORIGIN_QUERY__' IN query)>0 AND b.build_id IS NOT NULL" in body


def test_materialized_plan_check_changes_only_its_outer_wrapper():
    migration = _migration()
    staged = migration._staged()
    bulk = staged._finality()._bulk()
    storage = bulk._storage()
    schema = """synthetic "quoted' control"""
    query = migration._queries(staged)[38]
    assert "SELECT 'expected_family_plan_missing',expected.root_record_id" in query
    materialized = query.replace(
        "SELECT violation.* FROM (", "WITH violations(failure_code,row_identity) AS MATERIALIZED ("
    ).replace(") violation(failure_code,row_identity) LIMIT 1;", ") SELECT * FROM violations LIMIT 1;")
    restored = materialized.replace(
        "WITH violations(failure_code,row_identity) AS MATERIALIZED (", "SELECT violation.* FROM ("
    ).replace(") SELECT * FROM violations LIMIT 1;", ") violation(failure_code,row_identity) LIMIT 1;")
    assert restored == query
    previous = migration._checkpoint_body(schema)
    old_literal = storage._literal(bulk._control_sql(storage, schema, query))
    new_literal = storage._literal(bulk._control_sql(storage, schema, materialized))
    assert old_literal != new_literal and previous.count(old_literal) == 1
    assert migration._plan_body(schema) == previous.replace(old_literal, new_literal, 1)
    assert materialized.count("LIMIT 1") == 1 and materialized.rstrip().endswith("FROM violations LIMIT 1;")


def test_all_resource_wrappers_preserve_inner_queries_and_other_calls():
    migration = _migration()
    staged = migration._staged()
    bulk = staged._finality()._bulk()
    storage = bulk._storage()
    schema = "synthetic_control"
    expected = migration._checkpoint_body(schema)
    actual = migration._resource_body(schema)
    changed = 0
    for index, query in enumerate(migration._queries(staged)):
        materialized = query
        if 25 <= index < 60 and "WITH violations(failure_code,row_identity) AS MATERIALIZED (" not in query:
            materialized = query.replace(
                "SELECT violation.* FROM (", "WITH violations(failure_code,row_identity) AS MATERIALIZED ("
            ).replace(") violation(failure_code,row_identity) LIMIT 1;", ") SELECT * FROM violations LIMIT 1;")
            restored = materialized.replace(
                "WITH violations(failure_code,row_identity) AS MATERIALIZED (", "SELECT violation.* FROM ("
            ).replace(") SELECT * FROM violations LIMIT 1;", ") violation(failure_code,row_identity) LIMIT 1;")
            assert restored == query
            changed += 1
        if 25 <= index < 60:
            assert "WITH violations(failure_code,row_identity) AS MATERIALIZED (" in materialized
        old_literal = storage._literal(bulk._control_sql(storage, schema, query))
        new_literal = storage._literal(bulk._control_sql(storage, schema, materialized))
        assert expected.count(old_literal) == 1 and actual.count(new_literal) == 1
        expected = expected.replace(old_literal, new_literal, 1)
    assert changed == 29 and actual == expected


def test_grouped_children_change_only_the_two_accepted_edge_ctes():
    migration = _migration()
    staged = migration._staged()
    bulk = staged._finality()._bulk()
    storage = bulk._storage()
    replace = staged._previous("20261019010000_custom_import_build_continuation")._replace
    schema = "synthetic_control"
    expected = migration._resource_body(schema)
    actual = migration._body(schema)
    changed_checks = []
    for index, query in enumerate(migration._queries(staged)):
        if 25 <= index < 60 and "WITH violations(failure_code,row_identity) AS MATERIALIZED (" not in query:
            query = migration._materialized_query(query, replace)
        grouped = query
        if index in (40, 41):
            assert query.count(migration._ACCEPTED_CHILDREN) == 1
            grouped = query.replace(migration._ACCEPTED_CHILDREN, migration._GROUPED_CHILDREN, 1)
            assert grouped.replace(migration._GROUPED_CHILDREN, migration._ACCEPTED_CHILDREN, 1) == query
            changed_checks.append(index + 1)
        old_literal = storage._literal(bulk._control_sql(storage, schema, query))
        new_literal = storage._literal(bulk._control_sql(storage, schema, grouped))
        assert expected.count(old_literal) == 1 and actual.count(new_literal) == 1
        expected = expected.replace(old_literal, new_literal, 1)
    assert changed_checks == [41, 42] and actual == expected
    assert migration._GROUPED_CHILDREN.count("resolved_rejection_id") == 1
    assert "o.rejection_id" not in migration._GROUPED_CHILDREN
    assert "IS NOT DISTINCT FROM" not in migration._GROUPED_CHILDREN
    assert "max(source_ordinal) AS last_source_ordinal,true AS has_later" in migration._GROUPED_CHILDREN
    assert (
        "GROUP BY build_id,stream_slot,root_record_id,collection_slot,raw_parent_key_sha256,child_key_sha256\n"
        "            HAVING count(*)>1"
    ) in migration._GROUPED_CHILDREN
    assert "later.last_source_ordinal>o.source_ordinal" in migration._GROUPED_CHILDREN
    assert "s.duplicate_policy<>'collapse_identical' OR later.has_later IS NULL" in migration._GROUPED_CHILDREN


@pytest.mark.parametrize("body_kind", ("staged", "checkpoint", "plan", "resource", "updated"))
@pytest.mark.parametrize("incompatible", (False, True))
def test_upgrade_preserves_new_progress_and_refuses_old_noninitial_progress(monkeypatch, body_kind, incompatible):
    migration = _migration()
    schema = "synthetic_control"
    previous = " " + migration._staged()._body(schema) + " "
    checkpoint = " " + migration._checkpoint_body(schema) + " "
    plan = " " + migration._plan_body(schema) + " "
    resource = " " + migration._resource_body(schema) + " "
    replacement = " " + migration._body(schema) + " "
    installed = {
        "staged": previous,
        "checkpoint": checkpoint,
        "plan": plan,
        "resource": resource,
        "updated": replacement,
    }[body_kind]
    statements, refreshes = [], []

    def execute(statement, parameters=None):
        statements.append(str(statement))
        return SimpleNamespace(scalar_one_or_none=lambda: installed, scalar_one=lambda: incompatible)

    staged = migration._staged()
    original = staged._previous
    continuation = SimpleNamespace(_refresh=lambda *args: refreshes.append(args))
    monkeypatch.setattr(
        staged,
        "_previous",
        lambda name: continuation if name == "20261019010000_custom_import_build_continuation" else original(name),
    )
    monkeypatch.setattr(migration, "_staged", lambda: staged)
    monkeypatch.setattr(migration, "_schema", lambda: schema)
    monkeypatch.setattr(migration, "_checkpoint_body", lambda _: checkpoint[1:-1])
    monkeypatch.setattr(migration, "_plan_body", lambda _: plan[1:-1])
    monkeypatch.setattr(migration, "_resource_body", lambda _: resource[1:-1])
    monkeypatch.setattr(migration, "_body", lambda _: replacement[1:-1])
    monkeypatch.setattr(migration.op, "get_bind", lambda: SimpleNamespace(execute=execute))
    monkeypatch.setattr(migration.op, "execute", lambda statement: statements.append(str(statement)))
    if incompatible and body_kind == "staged":
        with pytest.raises(RuntimeError, match="incompatible_progress"):
            migration.upgrade()
        assert refreshes == []
    else:
        migration.upgrade()
        assert refreshes == [(f'"{schema}".verify_custom_import_build_structure(bigint)', installed, replacement)]
    assert any(statement.startswith("LOCK TABLE") for statement in statements)
    assert not any(statement.startswith(("UPDATE", "INSERT", "DELETE", "ALTER", "CREATE")) for statement in statements)
    if body_kind == "staged":
        progress = statements[-1]
        assert "scan_stage='capture' AND page_sequence=0" in progress
        assert "num_nonnulls(" in progress and "root_count=0" in progress


def test_unrecognized_body_and_downgrade_are_rejected(monkeypatch):
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
    with pytest.raises(RuntimeError, match="requires_forward_migration"):
        migration.downgrade()
