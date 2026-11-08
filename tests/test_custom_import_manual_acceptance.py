# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Manual acceptance preserves the existing automated and materialization contracts."""

import importlib.util
from pathlib import Path

import pytest

from db.models.custom_import import CustomImportBuildVerification


def _migration():
    path = Path(__file__).resolve().parents[1] / "alembic/versions/20261020200000_custom_import_manual_acceptance.py"
    spec = importlib.util.spec_from_file_location("manual_acceptance_test", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_manual_transition_is_privileged_attributed_and_leaves_counts_to_native_worker():
    migration = _migration()
    guard = migration._guard("synthetic_control")
    previous = migration._ranges()._guard("synthetic_control", ranged=True)
    assert "rolname=session_user AND rolsuper" in guard
    assert "current_user<>owner_name" in guard
    assert "OLD.page_sequence NOT IN (40,41)" in guard
    assert "NEW.scan_stage<>'winners' OR NEW.page_sequence<>61" in guard
    assert "'previous_proof',to_jsonb(OLD),'frozen_build',to_jsonb(b)" in guard
    assert "generate_series(OLD.page_sequence::integer+1,61)" in guard
    assert "'accepted_by',session_user,'accepted_at',clock_timestamp()" in guard
    assert "NEW.manual_acceptance IS DISTINCT FROM OLD.manual_acceptance" in guard
    assert "lock_custom_import_build(b.build_id)" in guard
    assert "lock_custom_import_snapshot_finality" in guard
    assert "custom_import_generation_seal WHERE generation_id=b.generation_id" in guard
    normal = previous.split("            allowed:=ARRAY['verification_state'", 1)[1]
    assert guard.endswith("            allowed:=ARRAY['verification_state'" + normal)
    assert not any(token in guard for token in ("GRANT ", "DISABLE TRIGGER", "set_config("))


def test_model_and_migration_keep_the_same_acceptance_shape():
    migration = _migration()
    constraint = next(
        value
        for value in CustomImportBuildVerification.__table__.constraints
        if value.name == "custom_import_build_verification_acceptance"
    )
    assert str(constraint.sqltext) == migration._ACCEPTANCE_CHECK
    previous = "CHECK ((((verification_contract)::text = 'custom-import/build-structure/v1'::text) AND preserved))"
    updated = migration._shape(previous)
    assert "AND preserved" in updated and migration._MANUAL in updated
    with pytest.raises(RuntimeError, match="shape_identity_mismatch"):
        migration._shape("CHECK (changed)")
    with pytest.raises(RuntimeError, match="forward_migration"):
        migration.downgrade()
