# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""DDL contract for bounded UHC Flex retry exhaustion."""

from __future__ import annotations

import importlib.util
from pathlib import Path
from unittest.mock import Mock

import sqlalchemy as sa

from db.models import ProviderDirectoryUHCFlexPractitionerAcquisition
from db.models import ProviderDirectoryUHCFlexPractitionerDataset


MIGRATION_PATH = Path(__file__).resolve().parents[1] / "alembic/versions" / (
    "20260830090000_uhc_flex_retry_exhaustion.py"
)
SCHEMA = "uhc_flex_retry_exhaustion_test"


def _migration():
    spec = importlib.util.spec_from_file_location(
        "uhc_flex_retry_exhaustion_migration",
        MIGRATION_PATH,
    )
    assert spec is not None and spec.loader is not None
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    return migration


def _normalized(value: str) -> str:
    return " ".join(value.split())


def test_revision_is_linear_and_exhaustion_is_narrow() -> None:
    migration = _migration()

    assert migration.revision == "20260830090000_uhc_flex_retry_exhaustion"
    assert migration.down_revision == (
        "20260828120000_plan_pricing_factorized_projection"
    )
    assert migration._MAX_ATTEMPTS == 8
    assert migration._ERROR_CODE == "retry_exhausted_transport"

    acquisition = _normalized(
        migration._acquisition_guard_sql(SCHEMA, partial=True)
    )
    single_root = _normalized(
        migration._single_root_guard_sql(SCHEMA, partial=True)
    )
    publication = _normalized(
        migration._publication_valid_sql(SCHEMA, partial=True)
    )
    for sql in (acquisition, single_root, publication):
        assert "retry_exhausted_transport" in sql
        assert "attempt_count < 8" in sql
    assert "actual_matched_count + actual_unmatched_count + actual_error_count" in (
        acquisition
    )
    assert "candidate.matched_count + candidate.unmatched_count +" in publication
    assert "retry_exhausted_count" in publication
    assert "'cohort_complete', false" in publication


def test_upgrade_sets_a_bounded_lock_timeout_before_locking(monkeypatch) -> None:
    migration = _migration()
    operation = Mock()
    operation.execute = Mock()
    monkeypatch.setattr(migration, "op", operation)

    migration.upgrade()

    calls = [call.args[0] for call in operation.execute.call_args_list]
    assert calls[0] == "SET LOCAL lock_timeout = '5s';"
    assert calls[1].lstrip().startswith("LOCK TABLE ")
    assert "SET LOCAL" not in calls[1]
    assert calls[1].rstrip().endswith("IN ACCESS EXCLUSIVE MODE;")


def test_constraints_keep_exact_rows_and_mark_partial_rows_incomplete() -> None:
    migration = _migration()
    acquisition = _normalized(
        migration._acquisition_state_sql(SCHEMA, partial=True)
    )
    publication = _normalized(
        migration._publication_state_sql(SCHEMA, partial=True)
    )

    assert "matched_count + unmatched_count + error_count" in acquisition
    assert "cohort_complete = (error_count = 0)" in acquisition
    assert "cohort_complete IN (TRUE, FALSE)" in publication
    assert "cohort_complete IS TRUE" in _normalized(
        migration._publication_state_sql(SCHEMA, partial=False)
    )


def test_downgrade_fails_closed_after_partial_evidence() -> None:
    migration = _migration()
    fence = _normalized(migration._downgrade_fence_sql(SCHEMA))

    assert "status = 'sealed' AND error_count > 0" in fence
    assert "cohort_complete IS FALSE" in fence
    assert "downgrade_blocked" in fence


def test_models_match_partial_root_constraints() -> None:
    acquisition_constraints_by_name = {
        constraint.name: _normalized(str(constraint.sqltext))
        for constraint in (
            ProviderDirectoryUHCFlexPractitionerAcquisition.__table__.constraints
        )
        if isinstance(constraint, sa.CheckConstraint)
    }
    publication_constraints_by_name = {
        constraint.name: _normalized(str(constraint.sqltext))
        for constraint in (
            ProviderDirectoryUHCFlexPractitionerDataset.__table__.constraints
        )
        if isinstance(constraint, sa.CheckConstraint)
    }

    acquisition = acquisition_constraints_by_name[
        "pd_uhc_flex_practitioner_acquisition_state_check"
    ]
    publication = publication_constraints_by_name[
        "pd_uhc_flex_practitioner_dataset_check"
    ]
    assert "matched_count + unmatched_count + error_count" in acquisition
    assert "cohort_complete = (error_count = 0)" in acquisition
    assert "cohort_complete IN (TRUE, FALSE)" in publication
