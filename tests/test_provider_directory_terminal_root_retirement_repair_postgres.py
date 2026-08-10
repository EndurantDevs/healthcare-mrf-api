# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Real-PostgreSQL proof for the additive retirement evidence repair."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from typing import Any

import pytest

from db import (
    migration_provider_directory_terminal_root_retirement_evidence as evidence,
)
from tests.provider_directory_terminal_root_retirement_pg_support import (
    SqlCapture,
    TARGET_DATASET_ID,
    RetirementPostgres,
    retirement_postgres,
)


ROOT = Path(__file__).resolve().parents[1]
REPAIR_MIGRATION_PATH = (
    ROOT
    / "alembic/versions"
    / (
        "20260810100000_provider_directory_terminal_root_retirement_"
        "resource_count_repair.py"
    )
)
_CORRECTED_COUNT_SQL = (
    "COALESCE(pg_catalog.sum(grouped.row_count), 0)::bigint\n"
    "                   AS actual_count"
)


def _load_repair_migration() -> Any:
    module_spec = importlib.util.spec_from_file_location(
        "terminal_root_retirement_count_repair_postgres_migration",
        REPAIR_MIGRATION_PATH,
    )
    assert module_spec is not None and module_spec.loader is not None
    migration = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(migration)
    return migration


async def _run_migration(
    scenario: RetirementPostgres,
    migration: Any,
    action: str,
) -> None:
    capture = SqlCapture()
    migration.op = capture
    getattr(migration, action)()
    async with scenario.connection.transaction():
        for statement in capture.statements:
            await scenario.connection.execute(statement)


def _deployed_group_count_sql(schema_name: str) -> str:
    function_sql = evidence.evidence_function_sql(schema_name)
    assert function_sql.count(_CORRECTED_COUNT_SQL) == 1
    return function_sql.replace(
        _CORRECTED_COUNT_SQL,
        "pg_catalog.count(*)::bigint AS actual_count",
        1,
    ).replace("CREATE FUNCTION", "CREATE OR REPLACE FUNCTION", 1)


def _drifted_group_count_sql(schema_name: str) -> str:
    deployed_sql = _deployed_group_count_sql(schema_name)
    deployed_count = "pg_catalog.count(*)::bigint AS actual_count"
    assert deployed_sql.count(deployed_count) == 1
    return deployed_sql.replace(
        deployed_count,
        "(pg_catalog.count(*) + 0)::bigint AS actual_count",
        1,
    )


async def _stored_evidence(scenario: RetirementPostgres) -> dict[str, Any]:
    payload = await scenario.connection.fetchval(
        f"SELECT {scenario.schema}."
        "provider_directory_terminal_root_retirement_evidence($1)::text",
        TARGET_DATASET_ID,
    )
    return json.loads(payload)


@pytest.mark.asyncio
async def test_additive_migration_repairs_retained_resource_total(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Repair an applied count-of-families body without changing persisted rows."""

    async with retirement_postgres(monkeypatch) as scenario:
        await scenario.connection.execute(
            _deployed_group_count_sql(scenario.schema_name)
        )
        before = await scenario.snapshot()
        broken_evidence = await _stored_evidence(scenario)
        assert broken_evidence["actual_resource_count"] == 2
        assert broken_evidence["resource_counts"] == {
            "Organization": 2,
            "Practitioner": 1,
        }

        repair = _load_repair_migration()
        await _run_migration(scenario, repair, "upgrade")

        repaired_evidence = await _stored_evidence(scenario)
        assert repaired_evidence["actual_resource_count"] == 3
        assert (
            repaired_evidence["resource_counts"] == broken_evidence["resource_counts"]
        )
        assert await scenario.snapshot() == before

        await _run_migration(scenario, repair, "downgrade")
        assert await _stored_evidence(scenario) == repaired_evidence


@pytest.mark.asyncio
async def test_additive_migration_rejects_unknown_function_body(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Refuse to replace a same-shaped function with unknown semantics."""

    async with retirement_postgres(monkeypatch) as scenario:
        await scenario.connection.execute(
            _drifted_group_count_sql(scenario.schema_name)
        )
        before = await scenario.snapshot()
        broken_evidence = await _stored_evidence(scenario)
        repair = _load_repair_migration()

        with pytest.raises(
            Exception,
            match="provider_directory_terminal_root_retirement_evidence_function_changed",
        ) as error:
            await _run_migration(scenario, repair, "upgrade")

        assert getattr(error.value, "sqlstate", None) == "55000"
        assert await scenario.snapshot() == before
        assert await _stored_evidence(scenario) == broken_evidence
