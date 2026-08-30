# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""DDL contract for exact partial-root lineage propagation."""

from __future__ import annotations

import importlib.util
from pathlib import Path
from unittest.mock import Mock

import sqlalchemy as sa

from db.models import ProviderDirectoryRootedGraphDataset


MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "alembic/versions"
    / ("20260830100000_provider_directory_rooted_partial_lineage.py")
)
SCHEMA = "provider_directory_rooted_partial_test"


def _migration():
    spec = importlib.util.spec_from_file_location(
        "provider_directory_rooted_partial_lineage_migration",
        MIGRATION_PATH,
    )
    assert spec is not None and spec.loader is not None
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    return migration


def _normalized(value: str) -> str:
    return " ".join(value.split())


def test_revision_and_partial_guards_bind_exact_inherited_evidence() -> None:
    migration = _migration()

    assert migration.revision == (
        "20260830100000_provider_directory_rooted_partial_lineage"
    )
    assert migration.down_revision == "20260830090000_uhc_flex_retry_exhaustion"

    acquisition = _normalized(migration._acquisition_guard_sql(SCHEMA, partial=True))
    single_root = _normalized(migration._single_root_guard_sql(SCHEMA, partial=True))
    intrinsic = _normalized(migration._intrinsic_valid_sql(SCHEMA, partial=True))
    for sql in (acquisition, single_root, intrinsic):
        assert "retry_exhausted_count" in sql
        assert "cohort_complete" in sql
    assert "root_header.cohort_complete" in acquisition
    assert "current_root.cohort_complete" in single_root
    assert "current_root.retry_exhausted_count = 0" in single_root
    assert "root_header.cohort_complete = header.cohort_complete" in intrinsic
    assert (
        "header.cohort_complete IS TRUE OR admitted.admission_contract_id = "
        in intrinsic
    )
    assert "root_parent.publication_metadata_json" in intrinsic
    assert "'cohort_complete', false" in intrinsic


def test_complete_identity_and_metadata_branches_remain_byte_stable() -> None:
    migration = _migration()
    single = migration._single_root()

    current_json = _normalized(migration._current_root_json_sql("current_root"))
    complete_json = _normalized(single._rooted_current_root_json_sql("current_root"))
    metadata = _normalized(migration._publication_metadata_sql("header", "admitted"))
    complete_metadata = _normalized(single._rooted_metadata_sql("header", "admitted"))

    assert "cohort_complete IS TRUE THEN" in current_json
    assert complete_json in current_json
    assert "cohort_complete IS TRUE THEN" in metadata
    assert complete_metadata in metadata
    assert "retry_exhausted_count" not in complete_json
    assert "retry_exhausted_count" not in complete_metadata


def test_existing_ready_predicate_surfaces_intrinsically_valid_partial_root() -> None:
    migration = _migration()
    ready = _normalized(migration._rooted()._rooted_ready_function_sql(SCHEMA))

    assert "provider_directory_rooted_graph_dataset_valid" in ready
    assert "cohort_complete" not in ready
    assert "header.cohort_complete =" in _normalized(
        migration._intrinsic_valid_sql(SCHEMA, partial=True)
    )


def test_upgrade_locks_before_replacing_current_functions(monkeypatch) -> None:
    migration = _migration()
    operation = Mock()
    operation.execute = Mock()
    monkeypatch.setattr(migration, "op", operation)

    migration.upgrade()

    statements = [call.args[0] for call in operation.execute.call_args_list]
    assert statements[0] == "SET LOCAL lock_timeout = '5s';"
    assert statements[1].lstrip().startswith("LOCK TABLE ")
    assert statements[1].rstrip().endswith("IN ACCESS EXCLUSIVE MODE;")
    sql = _normalized(" ".join(statements))
    assert sql.count("CREATE OR REPLACE FUNCTION") == 3
    assert "cohort_complete IN (TRUE, FALSE)" in sql


def test_downgrade_is_fenced_and_model_matches_partial_check() -> None:
    migration = _migration()
    fence = _normalized(migration._downgrade_fence_sql(SCHEMA))
    assert "cohort_complete IS FALSE" in fence
    assert "downgrade_blocked" in fence
    assert "cohort_complete IS TRUE" in _normalized(
        migration._dataset_check_sql(SCHEMA, partial=False)
    )

    constraint = next(
        candidate
        for candidate in ProviderDirectoryRootedGraphDataset.__table__.constraints
        if isinstance(candidate, sa.CheckConstraint)
        and candidate.name == "pd_rooted_graph_dataset_check"
    )
    assert "cohort_complete IN (TRUE, FALSE)" in _normalized(str(constraint.sqltext))
