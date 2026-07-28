# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""PostgreSQL savepoint proof for best-effort PTG stage DDL."""

from __future__ import annotations

import pytest

from process.ptg_parts import table_setup
from process.ptg_parts.ptg2_legacy_orphan_store_common import (
    _MRF_OPTIONAL_TABLES,
)
from tests.test_ptg2_legacy_orphan_postgres_support import (
    _has_relation,
    _prepared_case,
    _q,
)


async def _drop_optional_stage_tables(case) -> None:
    async with case.database.acquire() as connection:
        for table_name in _MRF_OPTIONAL_TABLES:
            await connection.status(
                f"DROP TABLE {_q(case.mrf_schema)}.{_q(table_name)}"
            )


@pytest.mark.asyncio
async def test_optional_stage_ddl_error_rolls_back_only_its_savepoint(
    monkeypatch,
) -> None:
    """Commit required and later DDL after one real PostgreSQL error."""

    async with _prepared_case(monkeypatch) as case:
        await _drop_optional_stage_tables(case)
        original_status = case.database.status
        failed_statements: list[str] = []

        async def status_with_database_failure(statement, **parameters):
            sql = str(statement)
            if (
                not failed_statements
                and "ADD COLUMN IF NOT EXISTS plan_name" in sql
            ):
                failed_statements.append(sql)
                return await original_status(
                    f"ALTER TABLE {_q(case.mrf_schema)}."
                    f"{_q('ptg2_serving_rate_stage')} "
                    "ADD COLUMN snapshot_id text"
                )
            return await original_status(statement, **parameters)

        monkeypatch.setattr(table_setup, "db", case.database)
        monkeypatch.setattr(
            case.database,
            "status",
            status_with_database_failure,
        )
        monkeypatch.setenv(table_setup.PTG2_UNLOGGED_STAGE_ENV, "false")
        monkeypatch.setenv(table_setup.PTG2_STAGE_INDEXES_ENV, "true")

        await table_setup._ensure_ptg2_serving_rate_stage_table(
            case.mrf_schema
        )

        async with case.database.acquire() as connection:
            has_table = await _has_relation(
                connection,
                case.mrf_schema,
                "ptg2_serving_rate_stage",
            )
            has_later_index = await _has_relation(
                connection,
                case.mrf_schema,
                "ptg2_serving_rate_stage_snapshot_idx",
            )

        assert len(failed_statements) == 1
        assert has_table is True
        assert has_later_index is True
