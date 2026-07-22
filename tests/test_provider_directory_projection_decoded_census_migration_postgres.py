# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Disposable PostgreSQL proofs for decoded child-census migration fences."""

from __future__ import annotations

import importlib.util

import pytest

from process.provider_directory_physical_projection import claim_projection_shard
from process.provider_directory_projection_child_read import (
    claim_projection_child_read_lease,
)
from tests.provider_directory_projection_foundation_postgres_support import (
    DECODED_CENSUS_MIGRATION_PATH,
    projection_foundation_postgres,
)
from tests.test_provider_directory_projection_foundation_postgres import (
    _registered_projection_context,
)


def _load_migration():
    module_spec = importlib.util.spec_from_file_location(
        "projection_child_decoded_census_test_migration",
        DECODED_CENSUS_MIGRATION_PATH,
    )
    assert module_spec is not None and module_spec.loader is not None
    migration_module = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(migration_module)
    return migration_module


def test_decoded_census_migration_locks_before_adoption_and_rewrite() -> None:
    """Fence concurrent child writes before checking or replacing the guard."""

    migration_sql = _load_migration()._rewrite_guard_sql("mrf", upgrade=True)
    lock_offset = migration_sql.index("LOCK TABLE")
    adoption_offset = migration_sql.index("IF EXISTS")
    rewrite_offset = migration_sql.index("pg_get_functiondef")
    assert lock_offset < adoption_offset < rewrite_offset
    assert "IN SHARE ROW EXCLUSIVE MODE" in migration_sql


def test_decoded_census_migration_quotes_apostrophe_schema() -> None:
    """Schema text remains an identifier and a safe regprocedure literal."""

    migration_sql = _load_migration()._rewrite_guard_sql(
        "mrf'oops",
        upgrade=True,
    )
    assert 'LOCK TABLE "mrf\'oops".' in migration_sql
    assert (
        "CAST('\"mrf''oops\"."
        "guard_provider_directory_projection_child_read_lease()' AS regprocedure)"
        in migration_sql
    )


def test_decoded_census_schema_contract_rejects_legacy_runtime_drift(
    monkeypatch,
) -> None:
    """Keep the production schema-alias fence explicit in isolated tests."""

    migration = _load_migration()
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "isolated_schema")
    monkeypatch.setenv("DB_SCHEMA", "isolated_schema")
    assert migration._schema() == "isolated_schema"
    monkeypatch.setenv("DB_SCHEMA", "drifted_schema")
    with pytest.raises(
        RuntimeError,
        match="DB_SCHEMA and HLTHPRT_DB_SCHEMA must identify the same schema",
    ):
        migration._schema()


async def _set_decoded_resource_count(
    postgres,
    recipe_id: str,
    decoded_resource_count: int,
) -> None:
    relation = (
        f'"{postgres.schema}".provider_directory_projection_input_block'
    )
    await postgres.database.status(f"ALTER TABLE {relation} DISABLE TRIGGER USER;")
    try:
        updated_count = await postgres.database.status(
            f"""
            UPDATE {relation}
               SET summary_json = jsonb_set(
                       summary_json,
                       '{{resource_count}}',
                       to_jsonb(CAST(:decoded_resource_count AS bigint)),
                       true
                   )
             WHERE recipe_id = :recipe_id;
            """,
            recipe_id=recipe_id,
            decoded_resource_count=decoded_resource_count,
        )
        assert updated_count == 1
    finally:
        await postgres.database.status(f"ALTER TABLE {relation} ENABLE TRIGGER USER;")


async def _restore_physical_child_census(postgres, recipe_id: str) -> None:
    child_relation = (
        f'"{postgres.schema}".provider_directory_projection_child_read_lease'
    )
    block_relation = (
        f'"{postgres.schema}".provider_directory_projection_input_block'
    )
    await postgres.database.status(
        f"ALTER TABLE {child_relation} DISABLE TRIGGER USER;"
    )
    try:
        updated_count = await postgres.database.status(
            f"""
            UPDATE {child_relation} AS child
               SET expected_record_count = block.record_count
              FROM {block_relation} AS block
             WHERE child.recipe_id = :recipe_id
               AND block.recipe_id = child.recipe_id
               AND block.block_id = child.block_id;
            """,
            recipe_id=recipe_id,
        )
        assert updated_count == 1
    finally:
        await postgres.database.status(
            f"ALTER TABLE {child_relation} ENABLE TRIGGER USER;"
        )


async def _child_guard_definition(postgres) -> str:
    return str(
        await postgres.database.scalar(
            "SELECT pg_get_functiondef(CAST(:guard AS regprocedure));",
            guard=(
                f'"{postgres.schema}".'
                "guard_provider_directory_projection_child_read_lease()"
            ),
        )
    )


@pytest.mark.asyncio
async def test_decoded_census_upgrade_and_downgrade_are_fail_closed(
    monkeypatch,
) -> None:
    """Adopt decoded Bundle counts while refusing a lossy downgrade."""

    async with projection_foundation_postgres(monkeypatch) as postgres:
        await postgres.upgrade()
        await postgres.upgrade_child_read()
        lease, _retained, block, shard, admission_id = (
            await _registered_projection_context(postgres, "decoded-census-migration")
        )
        decoded_resource_count = block.block.record_count + 7
        await _set_decoded_resource_count(
            postgres,
            lease.recipe.recipe_id,
            decoded_resource_count,
        )
        await postgres.upgrade_decoded_census()
        claim = await claim_projection_shard(
            lease,
            admission_id=admission_id,
            database=postgres.database,
            schema=postgres.schema,
        )
        assert claim is not None and claim.shard == shard
        child_lease = await claim_projection_child_read_lease(
            claim,
            database=postgres.database,
            schema=postgres.schema,
        )
        assert child_lease.expected_record_count == decoded_resource_count
        upgraded_definition = await _child_guard_definition(postgres)
        assert "block.summary_json ->> 'resource_count'" in upgraded_definition

        with pytest.raises(
            Exception,
            match="provider_directory_projection_child_census_adoption_blocked",
        ):
            await postgres.downgrade_decoded_census()
        assert await _child_guard_definition(postgres) == upgraded_definition

        await _restore_physical_child_census(
            postgres,
            lease.recipe.recipe_id,
        )
        await postgres.downgrade_decoded_census()
        downgraded_definition = await _child_guard_definition(postgres)
        assert "block.record_count = NEW.expected_record_count" in downgraded_definition
        assert "block.summary_json ->> 'resource_count'" not in downgraded_definition
