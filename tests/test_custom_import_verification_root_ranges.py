# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Root ranges preserve fixed check predicates and the existing progress boundary."""

import importlib.util
from pathlib import Path
from types import SimpleNamespace

import pytest


def _migration():
    path = (
        Path(__file__).resolve().parents[1]
        / "alembic/versions/20261020190000_custom_import_verification_root_ranges.py"
    )
    spec = importlib.util.spec_from_file_location("root_ranges_test", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_only_edge_queries_are_root_scoped_and_all_other_checks_remain():
    migration = _migration()
    arms = migration._arms()
    staged = arms._staged()
    replace = staged._previous("20261019010000_custom_import_build_continuation")._replace
    for index in (40, 41):
        query = arms._materialized_query(arms._queries(staged)[index], replace)
        query = replace(query, arms._ACCEPTED_CHILDREN, arms._GROUPED_CHILDREN)
        ranged = migration._range_query(arms, query)
        restored = ranged.replace(migration._RANGE, "WITH expected_build AS (")
        restored = restored.replace("range_occurrences", "__CANDIDATE__.custom_import_build_occurrence")
        restored = restored.replace(
            "WHERE plan.selection_kind='source' AND plan.build_id=$11\n"
            "          AND plan.root_record_id=ANY($10::bigint[]) AND NOT EXISTS (",
            "WHERE plan.selection_kind='source' AND NOT EXISTS (",
        )
        assert restored == query
    body = migration._body("synthetic_control")
    bulk = staged._finality()._bulk()
    storage = bulk._storage()
    previous = arms._body("synthetic_control")
    for index, query in enumerate(arms._queries(staged)):
        if index in (40, 41):
            continue
        if 25 <= index < 60 and "WITH violations(failure_code,row_identity) AS MATERIALIZED (" not in query:
            query = arms._materialized_query(query, replace)
        literal = storage._literal(bulk._control_sql(storage, "synthetic_control", query))
        assert previous.count(literal) == 1 and body.count(literal) == 1
    assert "stage IN (41,42)" in body and "v.scan_stage::text,v.page_sequence,cardinality(root_ids)" in body
    assert "EXECUTE query INTO root_ids USING b.build_id,v.after_root_record_id,16384;" in body
    assert "EXECUTE query INTO root_ids USING b.build_id,v.after_root_record_id,8;" in migration._body(
        "synthetic_control", root_batch_size=8
    )
    assert "after_root_record_id=NULL" in body
    assert body.count("lock_custom_import_snapshot_finality") == arms._body("synthetic_control").count(
        "lock_custom_import_snapshot_finality"
    )
    assert body.count("verify_custom_import_snapshot_indexes") == 4
    assert "SELECT DISTINCT root_record_id" in migration._ROOTS
    assert "custom_import_root_record" not in migration._ROOTS
    assert "custom_import_generation_family" not in migration._ROOTS


def test_same_arm_guard_accepts_only_strict_root_only_progress():
    migration = _migration()
    previous, updated = (
        migration._guard("synthetic_control", ranged=False),
        migration._guard("synthetic_control", ranged=True),
    )
    assert previous != updated
    assert "NEW.page_sequence=OLD.page_sequence+1" in updated
    assert "OLD.page_sequence IN (40,41)" in updated
    assert "NEW.after_root_record_id>OLD.after_root_record_id" in updated
    assert "to_jsonb(NEW)-'after_root_record_id'=to_jsonb(OLD)-'after_root_record_id'" in updated
    assert "OLD.verification_state='complete' OR b.phase<>'verifying'" in updated
    with pytest.raises(RuntimeError, match="requires_forward_migration"):
        migration.downgrade()


@pytest.mark.parametrize("matching_guard", (False, True))
def test_parent_recognizes_only_the_exact_root_body_and_matching_guard(monkeypatch, matching_guard):
    migration = _migration()
    arms = migration._arms()
    staged = arms._staged()
    previous = staged._previous
    continuation = previous("20261019010000_custom_import_build_continuation")
    staged._previous = lambda name: (
        migration if name == migration.revision else continuation if name == continuation.revision else previous(name)
    )
    schema = "synthetic_control"
    installed = " " + migration._body(schema) + " "
    statements, refreshes = [], []
    monkeypatch.setattr(migration, "_arms", lambda: arms)
    monkeypatch.setattr(arms, "_staged", lambda: staged)
    monkeypatch.setattr(arms, "_schema", lambda: schema)
    monkeypatch.setattr(continuation, "_refresh", lambda *args: refreshes.append(args))
    monkeypatch.setattr(
        arms.op,
        "get_bind",
        lambda: SimpleNamespace(execute=lambda *args: SimpleNamespace(scalar_one_or_none=lambda: installed)),
    )
    monkeypatch.setattr(arms.op, "execute", lambda statement: statements.append(str(statement)))
    monkeypatch.setattr(
        migration,
        "_installed_guard",
        lambda *args: SimpleNamespace(prosrc=migration._guard(schema, ranged=True) if matching_guard else "changed"),
    )
    if matching_guard:
        arms.upgrade()
        assert len(statements) == 1 and statements[0].startswith("LOCK TABLE")
        assert refreshes == [(f'"{schema}".verify_custom_import_build_structure(bigint)', installed, installed)]
    else:
        with pytest.raises(RuntimeError, match="root_guard_mismatch"):
            arms.upgrade()
        assert statements == []
        assert refreshes == []
