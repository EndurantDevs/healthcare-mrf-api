# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Focused contract for the packed selector range index migration."""

from __future__ import annotations

import importlib.util
from pathlib import Path


MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "alembic"
    / "versions"
    / "20260826200000_hospital_price_selector_range_index.py"
)


class _Recorder:
    def __init__(self) -> None:
        self.statements: list[str] = []

    def execute(self, statement: str) -> None:
        self.statements.append(" ".join(str(statement).split()))


def _migration():
    spec = importlib.util.spec_from_file_location(
        "selector_range_index", MIGRATION_PATH
    )
    assert spec is not None and spec.loader is not None
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    return migration


def test_selector_range_index_upgrade_and_downgrade(monkeypatch) -> None:
    migration = _migration()
    recorder = _Recorder()
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "hospital_price_index_test")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    migration.op = recorder

    migration.upgrade()
    assert migration.down_revision == "20260825150000_plan_pricing_card_projection"
    assert recorder.statements == [
        'CREATE INDEX "hospital_price_data_block_selector_secondary_lookup_idx" '
        'ON "hospital_price_index_test"."hospital_price_data_block" '
        "(version_id, block_kind, key_sha256, secondary_first) "
        "WHERE block_kind IN (3, 4);"
    ]

    recorder.statements.clear()
    migration.downgrade()
    assert recorder.statements == [
        'DROP INDEX "hospital_price_index_test".'
        '"hospital_price_data_block_selector_secondary_lookup_idx";'
    ]
