# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Candidate-only schedules retain native definitions and frozen verification."""

import importlib.util
import json
from pathlib import Path

import pytest

from process.custom_import.storage_layout import snapshot_phase_index_statements, snapshot_serving_index_statements


def _migration():
    path = Path(__file__).resolve().parents[1] / "alembic/versions/20261010000000_custom_import_admission_indexes.py"
    specification = importlib.util.spec_from_file_location("admission_index_schedule_test", path)
    assert specification and specification.loader
    migration = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(migration)
    return migration


def test_current_schedule_matches_models_without_changing_existing_index_shapes():
    migration = _migration()
    finality = migration._previous()._bulk()._previous("20261005060000_custom_import_snapshot_finality")
    previous = json.loads(migration._specifications(finality, corrected=False))
    corrected = json.loads(migration._specifications(finality, corrected=True))
    assert {item["name"]: {key: value for key, value in item.items() if key != "phase"} for item in previous} == {
        item["name"]: {key: value for key, value in item.items() if key != "phase"} for item in corrected
    }
    assert [item["name"] for item in corrected if item not in previous] == ["custom_import_build_graph_child_idx"]
    for phase in ("admission", "graph", "output", "serving"):
        expected = (
            snapshot_serving_index_statements(7) if phase == "serving" else snapshot_phase_index_statements(7, phase)
        )
        assert (
            tuple(item["ddl"].replace("__CANDIDATE__", "ci_snapshot_7") for item in corrected if item["phase"] == phase)
            == expected
        )


@pytest.mark.parametrize("corrected", (False, True))
def test_index_operations_keep_fences_registry_scope_and_readonly_verifier(corrected):
    migration = _migration()
    finality = migration._previous()._bulk()._previous("20261005060000_custom_import_snapshot_finality")
    prepare = migration._definition(
        finality, "synthetic_control", "prepare_custom_import_snapshot_indexes", corrected=corrected
    )
    verify = migration._definition(
        finality, "synthetic_control", "verify_custom_import_snapshot_indexes", corrected=corrected
    )
    assert "LOCK TABLE" not in prepare and "DROP INDEX" not in prepare
    assert "RETURN false" in prepare and "custom_import_snapshot_index_mismatch" in prepare
    assert "custom_import_snapshot_index_missing" in verify and "EXECUTE replace(item->>'ddl'" not in verify
    assert "ANALYZE" not in verify
    assert "custom_import_snapshot_relation WHERE family_id=f.family_id" in prepare
    assert (
        prepare.index("    END LOOP;")
        < prepare.index("EXECUTE 'ANALYZE '")
        < prepare.rindex("lock_custom_import_snapshot_attempt")
    )
    assert ("IF p_phase IN ('admission','serving') THEN" in prepare) is corrected
    assert "b.phase IS DISTINCT FROM p_phase" in prepare
    assert "lock_custom_import_snapshot_finality" in prepare and "lock_custom_import_snapshot_finality" in verify
    assert "__CONTROL__" not in prepare and "__CONTROL__" not in verify
    with pytest.raises(ValueError, match="unsupported"):
        migration._definition(finality, "synthetic_control", "arbitrary_sql", corrected=corrected)
