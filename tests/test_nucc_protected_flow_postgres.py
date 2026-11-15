# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Genuine protected NUCC candidate COPY and late set-validation rollback."""

import asyncio
import json
from contextlib import asynccontextmanager
from pathlib import Path
from tempfile import TemporaryDirectory
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
        from process.source_profile_result_pins import ROLE_POLICY_ENVIRONMENT

        with TemporaryDirectory(prefix="taxonomy-policy-") as directory:
            policy = Path(directory) / "roles.json"
            policy.write_text(
                json.dumps(
                    {
                        "owner_role": resources.admin_role,
                        "migration_role": resources.roles["publisher"],
                        "preparation_owner_role": resources.roles["owner"],
                        "runtime_roles": [resources.roles["writer"]],
                    }
                )
            )
            monkeypatch.setenv(ROLE_POLICY_ENVIRONMENT, str(policy))
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
    relation = f'"{ownership.schema_name}"."nucc_taxonomy"'
    assert (
        await session.scalar(
            text("SELECT has_table_privilege(current_user,:relation,'INSERT')"), {"relation": relation}
        )
        is False
    )
    assert await stage.copy_nucc_stage(session, ownership, records[:500]) == 500
    assert (
        await session.scalar(
            text("SELECT has_table_privilege(current_user,:relation,'INSERT')"), {"relation": relation}
        )
        is False
    )
    assert await stage.copy_nucc_stage(session, ownership, records[500:]) == 501
    assert (
        await session.scalar(
            text("SELECT has_table_privilege(current_user,:relation,'INSERT')"), {"relation": relation}
        )
        is False
    )
    prepared = await stage.finish_nucc_stage(session, ownership)
    assert prepared["validation"]["row_count"] == 1001
    await control._recheck_publisher_transaction(session, actor)
    return ownership, prepared


@pytest.mark.asyncio
async def test_native_candidate_bounded_copy_closes_writes_and_keeps_exact_indexed_set(monkeypatch):
    async with _native_candidates(monkeypatch) as resources:
        dataset = uuid4()
        async with resources.publisher.transaction() as session:
            ownership, prepared = await _candidate(session, dataset)
        async with resources.publisher.transaction() as session:
            assert await stage.revalidate_nucc_stage(session, ownership) == prepared
        relation = f'"{ownership.schema_name}"."nucc_taxonomy"'
        assert (
            await resources.admin.fetchval("SELECT count(*) FROM pg_index WHERE indrelid=to_regclass($1)", relation)
            == 4
        )
        for role in resources.roles.values():
            assert (
                await resources.admin.fetchval(
                    "SELECT NOT has_table_privilege($1,$2,'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER,MAINTAIN') "
                    "AND NOT has_any_column_privilege($1,$2,'INSERT,UPDATE,REFERENCES')",
                    role,
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
        with pytest.raises(Exception, match="permission denied|must be owner"):
            async with writer_sessions.begin() as session:
                await stage.precreate_nucc_stage(session, dataset_id=dataset)
        schema = stage.native.reference_family_stage_schema(dataset)
        assert await resources.admin.fetchval("SELECT to_regnamespace($1)::oid", schema) is None


@pytest.mark.asyncio
@pytest.mark.parametrize("cancel", (False, True))
async def test_interrupted_publisher_copy_rolls_back_insert_grant_and_rows(monkeypatch, cancel):
    async with _native_candidates(monkeypatch) as resources:
        original = stage.native.native_copy_record_batch

        async def interrupted(*args, **kwargs):
            assert await original(*args, **kwargs) == 1
            if cancel:
                raise asyncio.CancelledError()
            raise RuntimeError("interrupted candidate COPY")

        async with resources.publisher.transaction() as session:
            ownership = await stage.precreate_nucc_stage(session, dataset_id=uuid4())
            monkeypatch.setattr(stage.native, "native_copy_record_batch", interrupted)
            with pytest.raises(asyncio.CancelledError if cancel else RuntimeError):
                await stage.copy_nucc_stage(session, ownership, _records(1))
            relation = f'"{ownership.schema_name}"."nucc_taxonomy"'
            assert (
                await session.scalar(
                    text("SELECT has_table_privilege(current_user,:relation,'INSERT')"), {"relation": relation}
                )
                is False
            )
            assert await session.scalar(text(f"SELECT count(*) FROM {relation}")) == 0
