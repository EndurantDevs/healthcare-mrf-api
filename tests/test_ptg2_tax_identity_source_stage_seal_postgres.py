# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""PostgreSQL proof that the source-local stage is a plain temp table."""

from __future__ import annotations

import pytest
import sqlalchemy as sa

from process.ptg_parts import ptg2_tax_identity_source_preflight as preflight
from process.ptg_parts import ptg2_tax_identity_source_stage as source_stage
from tests.ptg2_provider_tax_identity_postgres_support import quoted
from tests.test_ptg2_tax_identity_source_preflight_postgres import (
    _prepared_projection_database,
)
from tests.test_ptg2_tax_identity_source_projection_postgres import (
    _fresh_projection,
)


async def _assert_unsealed_stage(session, prepared) -> None:
    staged = await source_stage.stage_tax_identity_source_projection(
        session,
        prepared,
    )
    seal_relation, guard_function = (
        await session.execute(
            sa.text("SELECT to_regclass(:seal_relation), to_regprocedure('pg_temp.ptg2_tax_source_stage_guard()')"),
            {"seal_relation": f"pg_temp.{staged.table_name}_seal"},
        )
    ).one()
    assert (seal_relation, guard_function) == (None, None)
    stage = f"{quoted('pg_temp')}.{quoted(staged.table_name)}"
    async with session.begin_nested() as unfrozen_write:
        await session.execute(sa.text(f"DELETE FROM {stage}"))
        await unfrozen_write.rollback()
    assert await preflight.validate_staged_tax_identity_source_projection(
        session,
        staged=staged,
        prepared=prepared,
    ) == (stage, staged.provider_group_count)
    await source_stage._drop_staged_tax_identity_source_projection(session, staged)
    assert (
        await session.scalar(
            sa.text("SELECT to_regclass(:stage)"),
            {"stage": f"pg_temp.{staged.table_name}"},
        )
        is None
    )


@pytest.mark.asyncio
async def test_stage_has_no_seal_or_freeze_and_rows_still_validate(
    monkeypatch,
    tmp_path,
) -> None:
    """Prove staging creates no seal table, guard function or write freeze."""

    async with _prepared_projection_database(monkeypatch, tmp_path) as prepared_db:
        database, _schema_name, prepare_projection = prepared_db
        with _fresh_projection(prepare_projection) as prepared:
            async with database.transaction() as session:
                await _assert_unsealed_stage(session, prepared)
