# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Offline compilation preserves the historical prefix and rejects online cutovers."""

import os
from pathlib import Path
import subprocess
import sys

import pytest


ROOT = Path(__file__).resolve().parents[1]
HISTORICAL_START = "20260713233000_provider_directory_resource_identifiers"
HISTORICAL_END = "20261005030000_source_profile_statement_pins"
ONLINE_REVISIONS = (
    ("20261005100000_rooted_graph_set_validation", "rooted_graph_migration_requires_online_connection"),
    ("20261005110000_ptg_set_validation", "ptg_snapshot_migration_requires_online_connection"),
    ("20261005120000_practitioner_set_validation", "practitioner_migration_requires_online_connection"),
    ("20261005130000_provider_dataset_candidates", "provider_dataset_migration_requires_online_connection"),
)


def _compile_offline(revision_range):
    """Compile without a database and retain both SQL and the failure diagnostic."""
    environment = os.environ.copy()
    environment.update(
        {
            "HLTHPRT_DB_HOST": "offline.invalid",
            "HLTHPRT_DB_PORT": "5432",
            "HLTHPRT_DB_USER": "offline",
            "HLTHPRT_DB_PASSWORD": "offline",
            "HLTHPRT_DB_DATABASE": "offline",
            "HLTHPRT_DB_SCHEMA": "mrf",
            "DB_SCHEMA": "mrf",
        }
    )
    return subprocess.run(
        [
            sys.executable,
            "-m",
            "alembic",
            "upgrade",
            revision_range,
            "--sql",
        ],
        cwd=ROOT,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
        timeout=60,
    )


def test_provider_directory_adoption_migrations_compile_offline_sql():
    offline_sql_compile_process = _compile_offline(f"{HISTORICAL_START}:{HISTORICAL_END}")
    assert offline_sql_compile_process.returncode == 0, offline_sql_compile_process.stderr
    migration_sql = offline_sql_compile_process.stdout
    assert f"SET version_num='{HISTORICAL_END}'" in migration_sql
    assert "provider_directory_dataset_resource_plan_lookup_idx" in migration_sql
    assert "import_run_provider_directory_retry_child_idx" in migration_sql
    composite_create = (
        'CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS '
        '"import_run_importer_active_idempotency_idx"'
    )
    legacy_drop = (
        'DROP INDEX CONCURRENTLY IF EXISTS '
        '"mrf"."import_run_active_idempotency_idx"'
    )
    assert migration_sql.index(composite_create) < migration_sql.index(legacy_drop)
    coverage_guard = "cms_npd_coverage_v1_current_requires_staged_upgrade"
    coverage_column = 'ALTER TABLE "mrf".provider_directory_cms_serving_coverage ADD COLUMN proof_version'
    assert coverage_guard in migration_sql
    assert migration_sql.index(coverage_guard) < migration_sql.index(coverage_column)


@pytest.mark.parametrize(
    ("revision_range", "error"),
    [(f"{HISTORICAL_START}:head", ONLINE_REVISIONS[0][1])]
    + [
        (f"{previous}:{revision}", error)
        for previous, (revision, error) in zip(
            [HISTORICAL_END] + [revision for revision, _ in ONLINE_REVISIONS[:-1]],
            ONLINE_REVISIONS,
            strict=True,
        )
    ],
)
def test_online_candidate_migrations_reject_offline_sql_without_stamping(revision_range, error):
    """A failed catalog conversion cannot be represented as an applied revision."""
    compiled = _compile_offline(revision_range)
    assert compiled.returncode != 0
    assert compiled.stderr.rstrip().endswith(f"RuntimeError: {error}")
    for revision, _ in ONLINE_REVISIONS:
        assert f"'{revision}'" not in compiled.stdout
    if revision_range.endswith(":head"):
        assert f"SET version_num='{HISTORICAL_END}'" in compiled.stdout
    else:
        statements = [line for line in compiled.stdout.splitlines() if line.strip() and not line.startswith("--")]
        assert statements == ["BEGIN;"]
