# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import importlib.util
from pathlib import Path
from unittest.mock import Mock


MIGRATION_PATH = Path(__file__).resolve().parents[1] / "alembic/versions" / (
    "20260811130000_provider_directory_exact_practitioner_resource_order_repair.py"
)


def _migration():
    spec = importlib.util.spec_from_file_location(
        "provider_directory_exact_practitioner_resource_order_repair",
        MIGRATION_PATH,
    )
    assert spec is not None and spec.loader is not None
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    return migration


def test_resource_order_repair_is_forward_only_and_collation_explicit(
    monkeypatch,
) -> None:
    migration = _migration()
    operation = Mock()
    operation.execute = Mock()
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "exact_practitioner_repair_test")
    monkeypatch.setattr(migration, "op", operation)

    migration.upgrade()

    statements = [call.args[0] for call in operation.execute.call_args_list]
    normalized_sql = " ".join(" ".join(statements).split())
    assert migration.revision == (
        "20260811130000_provider_directory_exact_practitioner_resource_order_repair"
    )
    assert migration.down_revision == (
        "20260811120000_provider_directory_reviewed_subset_v5_http410_disposition"
    )
    assert "LOCK TABLE" in normalized_sql
    assert "provider_directory_uhc_flex_practitioner_resource" not in statements[0]
    assert "CREATE OR REPLACE FUNCTION" in normalized_sql
    assert 'ORDER BY resource.resource_id COLLATE pg_catalog."C"' in normalized_sql
    assert normalized_sql.count("provider_directory_exact_practitioner_order_changed") == 2
    assert normalized_sql.index("LOCK TABLE") < normalized_sql.index(
        "CREATE OR REPLACE FUNCTION"
    )

    operation.execute.reset_mock()
    migration.downgrade()
    operation.execute.assert_not_called()


def test_resource_order_repair_rejects_ambiguous_predecessor_rendering(
    monkeypatch,
) -> None:
    migration = _migration()
    predecessor = migration._predecessor()
    monkeypatch.setattr(
        predecessor,
        "_work_guard_function_sql",
        lambda _schema: "CREATE FUNCTION without expected ordering",
    )

    try:
        migration._replacement_sql("exact_practitioner_repair_test")
    except RuntimeError as error:
        assert str(error) == "exact practitioner resource ordering changed"
    else:
        raise AssertionError("ambiguous predecessor rendering was accepted")
