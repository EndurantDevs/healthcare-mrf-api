# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Actual composed migrations, SOURCE homes, candidate index lifecycle and plans."""

from __future__ import annotations

import json
import os
import re
from contextlib import asynccontextmanager
from pathlib import Path
from uuid import uuid4

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from process.custom_import import build_source as source
from process.custom_import.storage_layout import snapshot_schema
from tests.custom_import_postgres_support import (
    _migration,
    _seed_publication_identity,
    isolated_publication_case,
    lease_digest,
)
from tests.test_custom_import_bulk_snapshot_writers_postgres import _bootstrap
from tests.test_custom_import_materialization_postgres import _live_generation

_ROOT = Path(__file__).resolve().parents[1]
_CHAIN = (
    "20261005040000_custom_import_bulk_snapshot_writers",
    "20261005050000_custom_import_legacy_snapshot_writers",
    "20261005060000_custom_import_snapshot_finality",
    "20261005070000_custom_import_materialization_storage",
)


def _install(connection, schema):
    for name in _CHAIN:
        migration = _migration(_ROOT / "alembic/versions" / f"{name}.py", f"source_homes_{name}")
        migration._schema = lambda: schema
        migration.op = Operations(MigrationContext.configure(connection))
        migration.upgrade()


@asynccontextmanager
async def _case():
    async with isolated_publication_case(migration_through="20261005030000") as case:
        async with case.engine.begin() as connection:
            await connection.run_sync(_install, case.schema_name)
        yield case


async def test_native_source_fresh_home_replay_and_missing_home_rejection():
    async with _case() as case:
        context, page, family_id = await _bootstrap(case)
        assert await source._store_single_page(case.sessions, context, page) == 1
        await source._compare_committed_page(case.sessions, context, page)
        async with source._page_session(case.sessions, context.request, context.build_id) as (session, _):
            homes = await session.scalar(text(f'SELECT count(*) FROM "{case.schema_name}".custom_import_revision_home'))
            assert homes == 1
            transaction = await session.begin_nested()
            await session.execute(
                text(f'DELETE FROM "{case.schema_name}".custom_import_revision_home WHERE family_id=:family'),
                dict(family=family_id),
            )
            with pytest.raises(DBAPIError, match="source_replay_home_mismatch"):
                await source._call(
                    session,
                    "check_custom_import_source_replay_homes",
                    (
                        ("bigint", context.build_id),
                        ("smallint", context.stream_slot),
                        ("integer", page.part_ordinal),
                        ("bigint", page.first_row),
                        ("integer", len(page.records)),
                    ),
                )
            await transaction.rollback()
        await source._compare_committed_page(case.sessions, context, page)
        outcome = await source.stage_segmented_source(case.sessions, context.request)
        assert outcome.phase == "graph"
        async with case.sessions() as session:
            assert (
                await session.scalar(text(f'SELECT count(*) FROM "{case.schema_name}".custom_import_revision_home'))
                == homes
            )
            assert (
                await session.scalar(
                    text(
                        "SELECT count(*) FROM pg_indexes WHERE schemaname=:schema AND indexname LIKE 'custom_import_build_%_idx'"
                    ),
                    dict(schema=snapshot_schema(family_id)),
                )
                >= 5
            )


async def test_native_all_five_finality_queries_and_serving_index_lifecycle():
    async with _case() as case:
        async with case.sessions() as session, session.begin():
            seed = await _seed_publication_identity(session, uuid4().hex)
            graph, attempt = await _live_generation(session, seed, uuid4().hex)
            await session.execute(text("SET LOCAL statement_timeout='2000ms'"))
            family_id = await session.scalar(
                text(f'SELECT "{case.schema_name}".resolve_custom_import_legacy_generation_snapshot(:generation)'),
                dict(generation=attempt.generation_id),
            )
            await session.execute(
                text(
                    f"SELECT \"{case.schema_name}\".freeze_custom_import_snapshot_family(:execution,:fence,sha256(convert_to(:token,'UTF8')))"
                ),
                dict(execution=attempt.execution_id, fence=attempt.fence, token=attempt.token),
            )
        namespace = snapshot_schema(family_id)
        finality = _migration(_ROOT / "alembic/versions" / f"{_CHAIN[2]}.py", "source_home_finality_plan")
        serving_indexes = await _prepare_serving_indexes(case, family_id, finality)
        async with case.sessions() as session, session.begin():
            await session.execute(text("SET LOCAL statement_timeout='2000ms'"))
            assert (
                await session.scalar(
                    text(f"SELECT \"{case.schema_name}\".verify_custom_import_snapshot_indexes(:family,'serving')"),
                    dict(family=family_id),
                )
                is True
            )
            with pytest.raises(DBAPIError, match="custom_import_snapshot_index_mismatch"):
                async with session.begin_nested():
                    name = serving_indexes[0]["name"]
                    await session.execute(text(f'DROP INDEX "{namespace}"."{name}"'))
                    await session.execute(
                        text(f'CREATE INDEX "{name}" ON "{namespace}".custom_import_root_scalar(root_revision_id)')
                    )
                    await session.scalar(
                        text(f"SELECT \"{case.schema_name}\".verify_custom_import_snapshot_indexes(:family,'serving')"),
                        dict(family=family_id),
                    )
            await _explain_finality_queries(session, case, finality, namespace, graph, attempt)
            counts = await session.scalar(
                text(f'SELECT "{case.schema_name}".verify_custom_import_snapshot_structure(:generation)'),
                dict(generation=attempt.generation_id),
            )
            assert counts["root_count"] == counts["winner_count"] == 0


async def _prepare_serving_indexes(case, family_id, finality):
    specifications = json.loads(finality._INDEX_SPECIFICATIONS)
    serving_indexes = [entry for entry in specifications if entry["phase"] == "serving"]
    counts = []
    for _ in serving_indexes:
        async with case.sessions() as session, session.begin():
            await session.execute(text("SET LOCAL statement_timeout='2000ms'"))
            is_complete = await session.scalar(
                text(f"SELECT \"{case.schema_name}\".prepare_custom_import_snapshot_indexes(:family,'serving')"),
                dict(family=family_id),
            )
            counts.append(
                await session.scalar(
                    text("SELECT count(*) FROM pg_indexes WHERE schemaname=:schema AND indexname LIKE '%scalar%_idx'"),
                    dict(schema=snapshot_schema(family_id)),
                )
            )
    assert counts == list(range(1, len(serving_indexes) + 1)) and is_complete is True
    return serving_indexes


async def _explain_finality_queries(session, case, finality, namespace, graph, attempt):
    bulk = finality._bulk()
    plans_by_name = {}
    arguments_by_name = dict(
        p1=attempt.generation_id,
        p2=graph.dataset_id,
        p3=graph.definition_revision_id,
        p4=graph.schema_revision_id,
        p5=attempt.execution_id,
        p6=graph.capture_bundle_id,
        p7=attempt.fence,
        p8=lease_digest(attempt.token),
        p9=None,
    )
    for name in finality._QUERY_NAMES:
        sql = bulk._control_sql(bulk._storage(), case.schema_name, finality._resource(name))
        sql = (
            sql.replace("__CANDIDATE__", f'"{namespace}"')
            .replace("__BASE__", f'"{case.schema_name}"')
            .replace("__ORIGIN_QUERY__", "")
        )
        sql = re.sub(r"\$(\d+)", _typed_plan_binding, sql)
        plan = await session.scalar(text("EXPLAIN (ANALYZE, BUFFERS, VERBOSE, FORMAT JSON) " + sql), arguments_by_name)
        plans_by_name[name] = json.loads(plan) if isinstance(plan, str) else plan
        assert plans_by_name[name][0]["Plan"]["Node Type"] == "Limit"
    assert len(plans_by_name) == 5
    if directory := os.getenv("CUSTOM_IMPORT_TEST_PLAN_DIRECTORY"):
        (Path(directory) / "finality-plans.json").write_text(json.dumps(plans_by_name, indent=2, sort_keys=True) + "\n")


def _typed_plan_binding(match):
    native_type = "bytea" if match[1] == "8" else "bigint"
    return f"CAST(:p{match[1]} AS {native_type})"
