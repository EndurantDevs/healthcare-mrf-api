# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Genuine protected NUCC candidate COPY and late set-validation rollback."""

from contextlib import asynccontextmanager
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker

from db.connection import db
from process import nucc_protected_control as control
from process import nucc_protected_stage as stage
from tests.test_entity_address_alias_guard_postgres import _owned_guard_database


@asynccontextmanager
async def _native_candidates(monkeypatch):
    """Reuse original authenticated custody and cleanup; no publication ledger is fabricated."""
    async with _owned_guard_database(monkeypatch) as resources:
        await resources.admin.execute("CREATE EXTENSION btree_gin")
        database = await resources.admin.fetchval("SELECT quote_ident(current_database())")
        assert await resources.admin.fetchval("SELECT current_setting('server_version_num')::int >= 180000")
        await resources.admin.execute(
            f'GRANT CREATE ON DATABASE {database} TO "{resources.roles["owner"]}","{resources.roles["publisher"]}"'
        )
        yield resources


def _records(count, *, invalid=False):
    model = stage.native.reference_family_spec("nucc").model_types[0]
    columns = tuple(model.__table__.columns.keys())
    return [
        tuple(
            {"code": "invalid" if invalid else f"{number:09d}A", "int_code": number, "display_name": "Example"}.get(
                name
            )
            for name in columns
        )
        for number in range(count)
    ]


async def _candidate(session, dataset):
    actor = await control._publisher_transaction(session)
    ownership = await stage.precreate_nucc_stage(session, dataset_id=dataset)
    records = _records(1001)
    assert await stage.copy_nucc_stage(session, ownership, records[:500]) == 500
    assert await stage.copy_nucc_stage(session, ownership, records[500:]) == 501
    prepared = await stage.finish_nucc_stage(session, ownership)
    assert prepared["validation"]["row_count"] == 1001
    await control._recheck_publisher_transaction(session, actor)
    return ownership, prepared


@pytest.mark.asyncio
async def test_native_candidate_bounded_copy_closes_writes_and_keeps_exact_indexed_set(monkeypatch):
    async with _native_candidates(monkeypatch) as resources:
        dataset = uuid4()
        async with resources.publisher.transaction() as session:
            assert await session.scalar(text("SELECT session_user=current_user")) is True
            ownership, prepared = await _candidate(session, dataset)
        async with resources.publisher.transaction() as session:
            assert await stage.revalidate_nucc_stage(session, ownership) == prepared
        relation = dict(ownership.relation_oids)["nucc_taxonomy"]
        assert await resources.admin.fetchval("SELECT count(*) FROM pg_index WHERE indrelid=$1", relation) == 4
        assert (
            await resources.admin.fetchval(
                "SELECT NOT has_schema_privilege($1,CAST($2 AS oid),'CREATE') "
                "AND NOT has_table_privilege($1,CAST($3 AS oid),'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER,MAINTAIN') "
                "AND NOT has_any_column_privilege($1,CAST($3 AS oid),'INSERT,UPDATE,REFERENCES')",
                resources.roles["writer"],
                ownership.schema_oid,
                relation,
            )
            is True
        )
        assert await resources.admin.fetchval(f'SELECT count(*) FROM "{ownership.schema_name}".nucc_taxonomy') == 1001


@pytest.mark.asyncio
async def test_native_set_refusal_rolls_back_candidate_and_preserves_predecessor(monkeypatch):
    async with _native_candidates(monkeypatch) as resources:
        async with resources.publisher.transaction() as session:
            original, prepared = await _candidate(session, uuid4())
        dataset = uuid4()
        schema = stage.native.reference_family_stage_schema(dataset)
        with pytest.raises(stage.native.ReferenceFamilyArchiveError, match="NUCC indexed set is invalid"):
            async with resources.publisher.transaction() as session:
                rejected = await stage.precreate_nucc_stage(session, dataset_id=dataset)
                assert await stage.copy_nucc_stage(session, rejected, _records(1, invalid=True)) == 1
                # The native aggregate validator refuses after COPY and actual index creation.
                await stage.finish_nucc_stage(session, rejected)
        assert await resources.admin.fetchval("SELECT to_regnamespace($1)::oid", schema) is None
        async with resources.publisher.transaction() as session:
            assert await stage.revalidate_nucc_stage(session, original) == prepared
        assert await resources.admin.fetchval(f'SELECT count(*) FROM "{original.schema_name}".nucc_taxonomy') == 1001


@pytest.mark.asyncio
async def test_original_unprivileged_login_cannot_create_a_protected_candidate(monkeypatch):
    async with _native_candidates(monkeypatch) as resources:
        dataset = uuid4()
        writer_sessions = async_sessionmaker(db.engine, expire_on_commit=False)
        with pytest.raises(stage.native.ReferenceFamilyArchiveError, match="native protected publisher is unavailable"):
            async with writer_sessions.begin() as session:
                assert await session.scalar(text("SELECT session_user=current_user")) is True
                await stage.precreate_nucc_stage(session, dataset_id=dataset)
        schema = stage.native.reference_family_stage_schema(dataset)
        assert await resources.admin.fetchval("SELECT to_regnamespace($1)::oid", schema) is None
