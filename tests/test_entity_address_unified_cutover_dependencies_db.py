# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import uuid

import pytest

from tests.test_entity_address_unified_publication_db import (
    _StageTable,
    _prepare_live_and_stage,
    _temporary_schema,
    entity_address_unified,
)


@pytest.mark.asyncio
async def test_real_postgres_dependency_preflight_blocks_live_and_old_views(monkeypatch):
    """Reject cross-schema views before cutover, then allow explicit cleanup."""
    async with _temporary_schema() as (database, schema):
        monkeypatch.setattr(entity_address_unified, "db", database)
        await _prepare_live_and_stage(database, schema)
        await database.status(
            f"CREATE TABLE {schema}.entity_address_unified_old (marker text NOT NULL);"
        )
        await database.status(
            f"INSERT INTO {schema}.entity_address_unified_old (marker) VALUES ('older');"
        )
        view_schema = f"eau_cutover_view_{uuid.uuid4().hex[:12]}"
        await database.status(f"CREATE SCHEMA {view_schema};")
        try:
            await database.status(
                f"CREATE VIEW {view_schema}.live_view AS "
                f"SELECT marker FROM {schema}.entity_address_unified;"
            )
            await database.status(
                f"CREATE MATERIALIZED VIEW {view_schema}.old_view AS "
                f"SELECT marker FROM {schema}.entity_address_unified_old;"
            )

            with pytest.raises(RuntimeError) as error_info:
                await entity_address_unified._assert_cutover_has_no_dependent_views(
                    schema, ["entity_address_unified"]
                )
            error_message = str(error_info.value)
            assert (
                f"{schema}.entity_address_unified -> {view_schema}.live_view"
                in error_message
            )
            assert (
                f"{schema}.entity_address_unified_old -> {view_schema}.old_view"
                in error_message
            )
            assert await database.scalar(
                f"SELECT marker FROM {schema}.entity_address_unified;"
            ) == "old"
            assert await database.scalar(
                f"SELECT marker FROM {schema}.entity_address_unified_stage;"
            ) == "new"

            await database.status(f"DROP SCHEMA {view_schema} CASCADE;")
            await entity_address_unified._assert_cutover_has_no_dependent_views(
                schema, ["entity_address_unified"]
            )
            await entity_address_unified._publish_staged_entity_address_tables(
                schema,
                _StageTable,
                {},
                partial_support_patch=False,
                affected_group_table="",
                context={},
            )
            assert await database.scalar(
                f"SELECT marker FROM {schema}.entity_address_unified;"
            ) == "new"
        finally:
            await database.status(f"DROP SCHEMA IF EXISTS {view_schema} CASCADE;")


@pytest.mark.asyncio
async def test_real_postgres_cutover_rechecks_dependencies_under_lock(monkeypatch):
    """Catch a view created after the early materialization preflight."""
    async with _temporary_schema() as (database, schema):
        monkeypatch.setattr(entity_address_unified, "db", database)
        await _prepare_live_and_stage(database, schema)
        await entity_address_unified._assert_cutover_has_no_dependent_views(
            schema, ["entity_address_unified"]
        )
        view_schema = f"eau_cutover_race_{uuid.uuid4().hex[:12]}"
        await database.status(f"CREATE SCHEMA {view_schema};")
        try:
            await database.status(
                f"CREATE VIEW {view_schema}.late_view AS "
                f"SELECT marker FROM {schema}.entity_address_unified;"
            )

            with pytest.raises(RuntimeError, match="cutover has dependent views"):
                await entity_address_unified._publish_staged_entity_address_tables(
                    schema,
                    _StageTable,
                    {},
                    partial_support_patch=False,
                    affected_group_table="",
                    context={},
                )

            assert await database.scalar(
                f"SELECT marker FROM {schema}.entity_address_unified;"
            ) == "old"
            assert await database.scalar(
                f"SELECT marker FROM {schema}.entity_address_unified_stage;"
            ) == "new"
            assert await database.scalar(
                "SELECT to_regclass(:relation_name) IS NULL;",
                relation_name=f"{schema}.entity_address_unified_old",
            ) is True

            await database.status(f"DROP SCHEMA {view_schema} CASCADE;")
            await entity_address_unified._publish_staged_entity_address_tables(
                schema,
                _StageTable,
                {},
                partial_support_patch=False,
                affected_group_table="",
                context={},
            )
            assert await database.scalar(
                f"SELECT marker FROM {schema}.entity_address_unified;"
            ) == "new"
        finally:
            await database.status(f"DROP SCHEMA IF EXISTS {view_schema} CASCADE;")
