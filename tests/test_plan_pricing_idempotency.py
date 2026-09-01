# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""All-status idempotency contracts for durable plan-pricing work."""

from __future__ import annotations

import importlib.util
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.exc import IntegrityError

from api import control_imports
from db.models import ImportRun


def _durable_import_request(importer: str, run_id: str) -> dict:
    if importer == "plan-pricing-prewarm":
        params_by_name = {
            "plan_release_id": "hprelease_" + "2" * 26,
            "serving_revision_id": "hpserve_" + "3" * 26,
            "projection_id": "a" * 64,
        }
    elif importer == "plan-pricing-em-distance":
        params_by_name = {
            "plan_release_id": "hprelease_" + "2" * 26,
            "serving_revision_id": "hpserve_" + "3" * 26,
        }
    else:
        params_by_name = {
            "binding_manifest_digest": "b" * 64,
            "bindings": [{"snapshot_id": "synthetic"}],
        }
    return {
        "run_id": run_id,
        "importer": importer,
        "idempotency_key": "plan-pricing-exact-replay",
        "params": params_by_name,
    }


def _projection_migration():
    migration_path = (
        Path(__file__).resolve().parents[1]
        / "alembic"
        / "versions"
        / "20260901103000_plan_pricing_em_distance.py"
    )
    module_spec = importlib.util.spec_from_file_location(
        "plan_pricing_idempotency_migration",
        migration_path,
    )
    assert module_spec is not None and module_spec.loader is not None
    migration = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(migration)
    return migration


def test_plan_pricing_all_status_index_matches_model_and_migration(
    monkeypatch,
) -> None:
    """Keep startup adoption and current-head migration predicates exact."""

    index_spec = next(
        index_by_field
        for index_by_field in ImportRun.__my_additional_indexes__
        if index_by_field["name"]
        == "import_run_plan_pricing_idempotency_idx"
    )
    assert index_spec == {
        "index_elements": ("importer", "idempotency_key"),
        "name": "import_run_plan_pricing_idempotency_idx",
        "unique": True,
        "where": (
            "importer IN ('plan-pricing-projection', "
            "'plan-pricing-prewarm', 'plan-pricing-em-distance') "
            "AND idempotency_key IS NOT NULL"
        ),
    }
    migration = _projection_migration()
    statements = []
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "projection_test")
    monkeypatch.setattr(migration.op, "execute", statements.append)

    migration.upgrade()

    statement = " ".join(" ".join(statements).split())
    assert "CREATE UNIQUE INDEX" in statement
    assert "import_run_plan_pricing_idempotency_idx" in statement
    assert (
        'ON "projection_test"."import_run" (importer, idempotency_key)'
        in statement
    )
    assert "'plan-pricing-em-distance'" in statement
    assert "AND idempotency_key IS NOT NULL" in statement


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "importer",
    (
        "plan-pricing-projection",
        "plan-pricing-prewarm",
        "plan-pricing-em-distance",
    ),
)
async def test_terminal_plan_pricing_idempotency_replays_succeeded_run(
    monkeypatch,
    importer,
) -> None:
    """Return an exact succeeded owner without active-only lookup."""

    terminal_run_by_field = {
        "run_id": "run_succeeded",
        "importer": importer,
        "status": "succeeded",
    }
    terminal_lookup = AsyncMock(return_value=terminal_run_by_field)
    monkeypatch.setattr(
        control_imports,
        "find_importer_run_by_idempotency_key",
        terminal_lookup,
    )
    monkeypatch.setattr(
        control_imports,
        "find_active_run_by_idempotency_key",
        AsyncMock(side_effect=AssertionError("active-only lookup is unsafe")),
    )

    replayed_run, created = await control_imports.create_import_run(
        _durable_import_request(importer, "run_replay")
    )

    assert created is False
    assert replayed_run == terminal_run_by_field
    terminal_lookup.assert_awaited_once_with(
        importer,
        "plan-pricing-exact-replay",
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "importer",
    (
        "plan-pricing-projection",
        "plan-pricing-prewarm",
        "plan-pricing-em-distance",
    ),
)
async def test_plan_pricing_integrity_race_replays_terminal_owner(
    monkeypatch,
    importer,
) -> None:
    """Resolve a unique-index loser to the terminal durable owner."""

    terminal_run_by_field = {
        "run_id": "run_race_winner",
        "importer": importer,
        "status": "succeeded",
    }
    terminal_lookup = AsyncMock(
        side_effect=(None, terminal_run_by_field)
    )
    failing_database = SimpleNamespace(
        execute=AsyncMock(
            side_effect=IntegrityError("insert", {}, Exception("duplicate"))
        )
    )
    monkeypatch.setattr(control_imports, "db", failing_database)
    monkeypatch.setattr(
        control_imports,
        "find_importer_run_by_idempotency_key",
        terminal_lookup,
    )
    monkeypatch.setattr(
        control_imports,
        "find_earliest_active_run_by_importer",
        AsyncMock(return_value=None),
    )

    replayed_run, created = await control_imports.create_import_run(
        _durable_import_request(importer, "run_race_loser")
    )

    assert created is False
    assert replayed_run == terminal_run_by_field
    assert terminal_lookup.await_count == 2
