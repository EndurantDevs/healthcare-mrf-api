# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""PostgreSQL cases for reviewed numeric-grid address aliases."""

from tests.test_address_numeric_grid_alias_db import (
    ROOT,
    _insert_archive_address,
    _load_module,
    _mark_failed,
    _prepare_serving_fence_schema,
    _requires_test_database,
    _reset_alias_data,
    address_alias_sql,
    address_canon,
    address_strict_source_backfill_sql,
    asyncio,
    asyncpg,
    db,
    entity_address_unified,
    json,
    os,
    provider_directory,
    pytest,
    resolve_into_archive,
    revoke_numeric_grid_alias,
    run_numeric_grid_alias,
    run_strict_source_backfill,
    stamp_address_keys,
    suppress,
)


@pytest.mark.asyncio(loop_scope="session")
async def test_alias_change_blocks_unified_cutover_and_stale_overlay_next_build():
    _requires_test_database()
    schema = "address_alias_unified_generation_probe"
    await _prepare_serving_fence_schema(
        schema,
        generation=1,
        overlay_generation=1,
        corroboration_generation=1,
    )
    source_selects = [f"SELECT * FROM {schema}.provider_directory_address_overlay AS overlay"]
    fence_context_by_field = {"address_alias_generation": 1}
    try:
        await entity_address_unified._capture_provider_directory_overlay_alias_fence(
            schema,
            source_selects,
            fence_context_by_field,
        )
        await db.status(
            f"""
            UPDATE "{schema}".address_alias_state_v1
               SET generation = 2
             WHERE singleton = true;
            """
        )

        with pytest.raises(RuntimeError, match="alias generation changed"):
            await entity_address_unified._run_entity_address_cutover(
                schema,
                [],
                [],
                ["cutover_probe"],
                ["cutover_probe"],
                fence_context_by_field,
            )
        assert await db.scalar(f'SELECT marker FROM "{schema}".cutover_probe;') == "untouched"

        with pytest.raises(RuntimeError, match="stale address alias generation"):
            await entity_address_unified._capture_provider_directory_overlay_alias_fence(
                schema,
                source_selects,
                {"address_alias_generation": 2},
            )
    finally:
        await db.status(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE;')


@pytest.mark.asyncio(loop_scope="session")
async def test_new_overlay_publication_blocks_unified_cutover():
    _requires_test_database()
    schema = "address_alias_unified_publication_probe"
    await _prepare_serving_fence_schema(
        schema,
        generation=2,
        overlay_generation=2,
        corroboration_generation=2,
    )
    fence_context_by_field = {"address_alias_generation": 2}
    try:
        await entity_address_unified._capture_provider_directory_overlay_alias_fence(
            schema,
            [f"SELECT * FROM {schema}.provider_directory_address_overlay AS overlay"],
            fence_context_by_field,
        )
        assert (
            fence_context_by_field["provider_directory_overlay_relation"]
            == f"{schema}.provider_directory_address_overlay"
        )
        await db.status(
            f'UPDATE "{schema}".address_alias_artifact_state_v1 '
            "SET updated_at=updated_at + interval '1 second' "
            "WHERE artifact_name='provider_directory_address_overlay'"
        )

        with pytest.raises(RuntimeError, match="overlay changed"):
            await entity_address_unified._run_entity_address_cutover(
                schema,
                [],
                [],
                ["cutover_probe"],
                ["cutover_probe"],
                fence_context_by_field,
            )
        assert await db.scalar(f'SELECT marker FROM "{schema}".cutover_probe;') == "untouched"
    finally:
        await db.status(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE;')


@pytest.mark.asyncio(loop_scope="session")
async def test_corroboration_full_rebuild_rejects_stale_overlay_receipt():
    _requires_test_database()
    schema = "address_alias_corroboration_generation_probe"
    await _prepare_serving_fence_schema(
        schema,
        generation=2,
        overlay_generation=1,
        corroboration_generation=2,
    )
    try:
        with pytest.raises(RuntimeError, match="overlay uses a stale address alias"):
            await provider_directory.publish_provider_directory_address_corroboration_table(
                schema,
                refresh_network_catalog=False,
                network_catalog_metrics_override={},
            )
        stage_count = await db.scalar(
            """
            SELECT count(*)
            FROM pg_class AS relation
            JOIN pg_namespace AS namespace
              ON namespace.oid = relation.relnamespace
            WHERE namespace.nspname = :schema
              AND relation.relname LIKE 'provider_directory_address_corroboration_stage%';
            """,
            schema=schema,
        )
        assert stage_count == 0
    finally:
        await db.status(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE;')


async def _rename_noop(_schema: str, _stage: str) -> None:
    """Leave probe indexes unchanged during a synthetic cutover."""


async def _prepare_corroboration_cutover_probe(
    schema: str,
    stage: str,
) -> provider_directory.ProviderDirectoryArtifactBuildFence:
    """Create a staged corroboration row and capture its dependency fence."""
    await db.status(f'CREATE TABLE "{schema}"."{stage}" (marker text NOT NULL);')
    await db.status(f'INSERT INTO "{schema}"."{stage}" VALUES (\'corroboration-new\');')
    target_oid = await provider_directory._provider_directory_relation_oid(
        schema,
        provider_directory.PROVIDER_DIRECTORY_ADDRESS_CORROBORATION_VIEW,
    )
    publication = await provider_directory._address_overlay_publication(schema)
    assert target_oid is not None and publication is not None
    return provider_directory._ProviderDirectoryAliasBuildFence(
        target_oid=target_oid,
        alias_generation=2,
        dependency_relation=provider_directory.PROVIDER_DIRECTORY_ADDRESS_OVERLAY_TABLE,
        dependency_publication=publication,
    )


async def _assert_rejected_corroboration_cutover(schema: str, stage: str) -> None:
    """Assert a stale dependency leaves the old target and removes the stage."""
    assert (
        await db.scalar(f'SELECT marker FROM "{schema}".provider_directory_address_corroboration;')
        == "corroboration-old"
    )
    assert await db.scalar(
        "SELECT to_regclass(:relation_name) IS NULL;",
        relation_name=f"{schema}.provider_directory_address_corroboration_old",
    )
    assert await db.scalar(
        "SELECT to_regclass(:relation_name) IS NULL;",
        relation_name=f"{schema}.{stage}",
    )


@pytest.mark.asyncio(loop_scope="session")
async def test_corroboration_cutover_rejects_new_overlay_publication():
    """A newer logical overlay publication invalidates corroboration cutover."""
    _requires_test_database()
    schema = "address_alias_corroboration_oid_probe"
    stage = "provider_directory_address_corroboration_stage_probe"
    await _prepare_serving_fence_schema(
        schema,
        generation=2,
        overlay_generation=2,
        corroboration_generation=2,
    )

    try:
        build_fence = await _prepare_corroboration_cutover_probe(schema, stage)
        await db.status(
            f"UPDATE \"{schema}\".address_alias_artifact_state_v1 SET updated_at=updated_at+interval '1 second' "
            "WHERE artifact_name='provider_directory_address_overlay'"
        )

        with pytest.raises(
            provider_directory.ProviderDirectoryArtifactBuildStale,
            match="dependency_relation",
        ):
            await provider_directory._cutover_provider_directory_artifact_stage(
                schema=schema,
                stage_table=stage,
                target_relation=(provider_directory.PROVIDER_DIRECTORY_ADDRESS_CORROBORATION_VIEW),
                rename_stage_indexes=_rename_noop,
                build_fence=build_fence,
            )
        await _assert_rejected_corroboration_cutover(schema, stage)
    finally:
        await db.status(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE;')
