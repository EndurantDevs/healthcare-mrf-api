# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Migration and model contracts for bounded attempt lookup."""

from __future__ import annotations

from db.models import PTG2Snapshot
from tests.ptg2_v4_stale_metadata_postgres_support import (
    _load_attempt_migration,
)


class _OnlineMigrationContext:
    as_sql = False

    def autocommit_block(self):
        raise AssertionError("an adopted index must not run concurrent DDL")


class _AdoptionOnlyOperations:
    def get_context(self):
        return _OnlineMigrationContext()


def test_attempt_index_migration_adopts_every_exact_existing_index(
    monkeypatch,
) -> None:
    migration = _load_attempt_migration(
        "20260724103000_ptg2_v4_attempt_indexes.py"
    )
    adopted_shapes = []
    monkeypatch.setattr(migration, "op", _AdoptionOnlyOperations())
    monkeypatch.setattr(
        migration,
        "has_matching_index",
        lambda _op, name, table, columns, *, schema: (
            adopted_shapes.append((name, table, columns, schema)) or True
        ),
    )

    migration.upgrade()

    assert (
        "ptg2_snapshot_attempt_run_idx",
        "ptg2_snapshot",
        ("import_run_id",),
        "mrf",
    ) in adopted_shapes
    assert "CREATE INDEX CONCURRENTLY IF NOT EXISTS" in (
        migration._create_index_sql(
            "mrf",
            "ptg2_snapshot_attempt_run_idx",
            "ptg2_snapshot",
            "import_run_id",
            concurrently=True,
        )
    )


def test_snapshot_model_declares_the_attempt_run_index() -> None:
    assert {
        "index_elements": ("import_run_id",),
        "name": "ptg2_snapshot_attempt_run_idx",
    } in PTG2Snapshot.__my_additional_indexes__
