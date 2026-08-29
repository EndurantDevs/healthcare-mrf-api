# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Offline compilation contract for the complete adoption migration chain."""

import os
from pathlib import Path
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[1]


def test_provider_directory_adoption_migrations_compile_offline_sql():
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
    offline_sql_compile_process = subprocess.run(
        [
            sys.executable,
            "-m",
            "alembic",
            "upgrade",
            "20260713233000_provider_directory_resource_identifiers:head",
            "--sql",
        ],
        cwd=ROOT,
        env=environment,
        check=True,
        capture_output=True,
        text=True,
    )
    migration_sql = offline_sql_compile_process.stdout
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
