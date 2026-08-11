# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""PostgreSQL lifecycle tests for reviewed numeric-grid address aliases."""

from __future__ import annotations

import asyncio
import importlib.util
import json
import os
from pathlib import Path

import asyncpg
import pytest

from db.models import db
from process.address_numeric_grid_alias import _mark_failed, run_numeric_grid_alias
from process.address_numeric_grid_alias_revoke import revoke_numeric_grid_alias
from process.address_strict_source_backfill import run_strict_source_backfill
from process.ext import address_alias_sql
from process.ext import address_canon
from process.ext import address_strict_source_backfill_sql
from process.ext.address_canon import resolve_into_archive, stamp_address_keys


entity_address_unified = __import__(
    "process.entity_address_unified",
    fromlist=["entity_address_unified"],
)
provider_directory = __import__(
    "process.provider_directory_fhir",
    fromlist=["provider_directory_fhir"],
)


ROOT = Path(__file__).resolve().parents[1]


def _load_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _requires_test_database() -> None:
    if "test" not in os.getenv("HLTHPRT_DB_DATABASE", ""):
        pytest.skip("numeric-grid alias tests require a disposable test database")


async def _reset_alias_data(schema: str) -> None:
    await db.status(
        f"""
        TRUNCATE TABLE
            {schema}.address_alias_v1,
            {schema}.address_alias_candidate_v1,
            {schema}.address_alias_run_v1
        RESTART IDENTITY CASCADE;
        """
    )
    await db.status(f"TRUNCATE TABLE {schema}.address_archive_v2 CASCADE;")
    await db.status(
        f"""
        UPDATE {schema}.address_alias_state_v1
        SET generation = 0,
            updated_at = now()
        WHERE singleton = true;
        """
    )
    await db.status(
        f"""
        UPDATE {schema}.address_alias_artifact_state_v1
        SET generation = 0,
            updated_at = now();
        """
    )


async def _prepare_serving_fence_schema(
    schema: str,
    *,
    generation: int,
    overlay_generation: int,
    corroboration_generation: int,
) -> None:
    await db.status(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE;')
    await db.status(f'CREATE SCHEMA "{schema}";')
    await db.status(
        f"""
        CREATE TABLE "{schema}".address_alias_state_v1 (
            singleton boolean PRIMARY KEY,
            schema_version smallint NOT NULL,
            active_ruleset_version smallint NOT NULL,
            generation bigint NOT NULL
        );
        """
    )
    await db.status(
        f"""
        INSERT INTO "{schema}".address_alias_state_v1
            (singleton, schema_version, active_ruleset_version, generation)
        VALUES (true, 1, 1, :generation);
        """,
        generation=generation,
    )
    await db.status(
        f"""
        CREATE TABLE "{schema}".address_alias_artifact_state_v1 (
            artifact_name varchar(128) PRIMARY KEY,
            generation bigint NOT NULL,
            updated_at timestamptz NOT NULL DEFAULT now()
        );
        """
    )
    await db.status(
        f"""
        INSERT INTO "{schema}".address_alias_artifact_state_v1
            (artifact_name, generation)
        VALUES
            ('provider_directory_address_overlay', :overlay_generation),
            ('provider_directory_address_corroboration', :corroboration_generation);
        """,
        overlay_generation=overlay_generation,
        corroboration_generation=corroboration_generation,
    )
    await db.status(
        f'CREATE TABLE "{schema}".provider_directory_address_overlay '
        "(marker text NOT NULL);"
    )
    await db.status(
        f'INSERT INTO "{schema}".provider_directory_address_overlay '
        "VALUES ('overlay-old');"
    )
    await db.status(
        f'CREATE TABLE "{schema}".provider_directory_address_corroboration '
        "(marker text NOT NULL);"
    )
    await db.status(
        f'INSERT INTO "{schema}".provider_directory_address_corroboration '
        "VALUES ('corroboration-old');"
    )
    await db.status(
        f'CREATE TABLE "{schema}".cutover_probe (marker text NOT NULL);'
    )
    await db.status(
        f'INSERT INTO "{schema}".cutover_probe VALUES (\'untouched\');'
    )


async def _replace_overlay_relation(schema: str) -> None:
    async with db.transaction():
        await db.scalar(address_alias_sql.alias_advisory_xact_lock_sql())
        await db.status(
            f'ALTER TABLE "{schema}".provider_directory_address_overlay '
            "RENAME TO provider_directory_address_overlay_replaced;"
        )
        await db.status(
            f'CREATE TABLE "{schema}".provider_directory_address_overlay '
            "(marker text NOT NULL);"
        )
        await db.status(
            f'INSERT INTO "{schema}".provider_directory_address_overlay '
            "VALUES ('overlay-new');"
        )


async def _insert_archive_address(
    schema: str,
    *,
    first_line: str,
    second_line: str | None,
    city: str = "Example City",
    state: str = "TX",
    postal_code: str = "75001",
    strict_source_bits: int,
) -> str:
    row = await db.first(
        f"""
        INSERT INTO {schema}.address_archive_v2 (
            address_key,
            identity_key,
            identity_version,
            precision,
            premise_key,
            line1_norm,
            unit_norm,
            city_norm,
            state_code,
            zip5,
            country_code,
            first_line,
            second_line,
            city_name,
            state_name,
            postal_code,
            source_bits,
            strict_source_bits
        )
        SELECT
            {schema}.addr_key_v1(
                :first_line, :second_line, :city, :state, :postal_code, 'US'
            ),
            {schema}.addr_identity_key_v1(
                :first_line, :second_line, :city, :state, :postal_code, 'US'
            ),
            2,
            'street',
            {schema}.addr_premise_key_v1(
                :first_line, :second_line, :city, :state, :postal_code, 'US'
            ),
            {schema}.addr_street_norm_v1(:first_line, :second_line),
            {schema}.addr_unit_norm_v1(:first_line, :second_line),
            {schema}.addr_city_norm_v1(:city),
            {schema}.addr_state_code_v1(:state),
            left(:postal_code, 5),
            'US',
            :first_line,
            :second_line,
            :city,
            :state,
            :postal_code,
            :strict_source_bits,
            :strict_source_bits
        RETURNING address_key::text;
        """,
        first_line=first_line,
        second_line=second_line,
        city=city,
        state=state,
        postal_code=postal_code,
        strict_source_bits=strict_source_bits,
    )
    assert row is not None
    return str(row[0])


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
    source_selects = [
        f"SELECT * FROM {schema}.provider_directory_address_overlay AS overlay"
    ]
    context = {"address_alias_generation": 1}
    try:
        await entity_address_unified._capture_provider_directory_overlay_alias_fence(
            schema,
            source_selects,
            context,
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
                context,
            )
        assert await db.scalar(
            f'SELECT marker FROM "{schema}".cutover_probe;'
        ) == "untouched"

        with pytest.raises(RuntimeError, match="stale address alias generation"):
            await entity_address_unified._capture_provider_directory_overlay_alias_fence(
                schema,
                source_selects,
                {"address_alias_generation": 2},
            )
    finally:
        await db.status(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE;')


@pytest.mark.asyncio(loop_scope="session")
async def test_same_generation_overlay_oid_swap_blocks_unified_cutover():
    _requires_test_database()
    schema = "address_alias_unified_oid_probe"
    await _prepare_serving_fence_schema(
        schema,
        generation=2,
        overlay_generation=2,
        corroboration_generation=2,
    )
    context = {"address_alias_generation": 2}
    try:
        await entity_address_unified._capture_provider_directory_overlay_alias_fence(
            schema,
            [
                f"SELECT * FROM {schema}.provider_directory_address_overlay AS overlay"
            ],
            context,
        )
        old_oid = context["provider_directory_overlay_relation_oid"]
        await _replace_overlay_relation(schema)
        new_oid = await provider_directory._provider_directory_relation_oid(
            schema,
            provider_directory.PROVIDER_DIRECTORY_ADDRESS_OVERLAY_TABLE,
        )
        assert new_oid is not None and new_oid != old_oid

        with pytest.raises(RuntimeError, match="overlay changed"):
            await entity_address_unified._run_entity_address_cutover(
                schema,
                [],
                [],
                ["cutover_probe"],
                ["cutover_probe"],
                context,
            )
        assert await db.scalar(
            f'SELECT marker FROM "{schema}".cutover_probe;'
        ) == "untouched"
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


@pytest.mark.asyncio(loop_scope="session")
async def test_corroboration_cutover_rejects_changed_overlay_oid():
    _requires_test_database()
    schema = "address_alias_corroboration_oid_probe"
    stage = "provider_directory_address_corroboration_stage_probe"
    await _prepare_serving_fence_schema(
        schema,
        generation=2,
        overlay_generation=2,
        corroboration_generation=2,
    )

    async def _rename_noop(_schema: str, _stage: str) -> None:
        return None

    try:
        await db.status(
            f'CREATE TABLE "{schema}"."{stage}" (marker text NOT NULL);'
        )
        await db.status(
            f'INSERT INTO "{schema}"."{stage}" VALUES (\'corroboration-new\');'
        )
        target_oid = await provider_directory._provider_directory_relation_oid(
            schema,
            provider_directory.PROVIDER_DIRECTORY_ADDRESS_CORROBORATION_VIEW,
        )
        overlay_oid = await provider_directory._provider_directory_relation_oid(
            schema,
            provider_directory.PROVIDER_DIRECTORY_ADDRESS_OVERLAY_TABLE,
        )
        assert target_oid is not None and overlay_oid is not None
        build_fence = provider_directory.ProviderDirectoryArtifactBuildFence(
            target_oid=target_oid,
            alias_generation=2,
            dependency_relation=(
                provider_directory.PROVIDER_DIRECTORY_ADDRESS_OVERLAY_TABLE
            ),
            dependency_relation_oid=overlay_oid,
        )
        await _replace_overlay_relation(schema)

        with pytest.raises(
            provider_directory.ProviderDirectoryArtifactBuildStale,
            match="dependency_relation",
        ):
            await provider_directory._cutover_provider_directory_artifact_stage(
                schema=schema,
                stage_table=stage,
                target_relation=(
                    provider_directory.PROVIDER_DIRECTORY_ADDRESS_CORROBORATION_VIEW
                ),
                rename_stage_indexes=_rename_noop,
                build_fence=build_fence,
            )
        assert await db.scalar(
            f'SELECT marker FROM "{schema}".provider_directory_address_corroboration;'
        ) == "corroboration-old"
        assert await db.scalar(
            "SELECT to_regclass(:relation_name) IS NULL;",
            relation_name=(
                f"{schema}.provider_directory_address_corroboration_old"
            ),
        )
        assert await db.scalar(
            "SELECT to_regclass(:relation_name) IS NULL;",
            relation_name=f"{schema}.{stage}",
        )
    finally:
        await db.status(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE;')


@pytest.mark.asyncio(loop_scope="session")
async def test_shadow_apply_retry_and_revoke_are_generation_safe():
    _requires_test_database()
    schema = os.getenv("HLTHPRT_DB_SCHEMA", "mrf")
    await _reset_alias_data(schema)
    source_key = await _insert_archive_address(
        schema,
        first_line="1548 E 4500",
        second_line="Suite 202",
        strict_source_bits=1,
    )
    target_key = await _insert_archive_address(
        schema,
        first_line="1548 E 4500 S",
        second_line="Suite 202",
        strict_source_bits=6,
    )

    shadow = await run_numeric_grid_alias(mode="shadow", schema=schema)
    assert shadow.status == "sealed"
    assert shadow.source_count == 1
    assert shadow.candidate_sources == 1
    assert shadow.eligible == 1
    assert shadow.no_candidate == 0
    assert shadow.active_skipped == 0
    assert shadow.candidate_digest

    applied = await run_numeric_grid_alias(
        mode="apply",
        schema=schema,
        alias_run_id=shadow.run_id,
        expected_candidate_sha256=shadow.candidate_digest,
        reviewed_by="ci-reviewer",
    )
    assert applied.promoted == 1
    assert applied.generation == 1
    alias_row = await db.first(
        f"""
        SELECT source_address_key::text, target_address_key::text
        FROM {schema}.address_alias_v1
        WHERE revoked_at IS NULL;
        """
    )
    assert alias_row is not None
    assert (alias_row[0], alias_row[1]) == (source_key, target_key)

    retried = await run_numeric_grid_alias(
        mode="apply",
        schema=schema,
        alias_run_id=shadow.run_id,
        expected_candidate_sha256=shadow.candidate_digest,
        reviewed_by="ci-reviewer",
    )
    assert retried.promoted == 0
    assert retried.generation == 1
    await db.status(
        f"UPDATE {schema}.address_alias_v1 SET updated_at = now();"
    )
    assert await db.scalar(
        f"SELECT generation FROM {schema}.address_alias_state_v1 WHERE singleton = true;"
    ) == 1

    revoked = await revoke_numeric_grid_alias(
        schema=schema,
        source_address_key=source_key,
        expected_target_address_key=target_key,
        reason="synthetic rollback",
        reviewed_by="ci-reviewer",
    )
    assert revoked.status == "revoked"
    assert revoked.generation == 2
    assert await db.scalar(
        f"SELECT generation FROM {schema}.address_alias_state_v1 WHERE singleton = true;"
    ) == 2
    with pytest.raises(Exception, match="revocation is immutable"):
        await db.status(
            f"""
            UPDATE {schema}.address_alias_v1
            SET revoked_at = NULL,
                revoked_reason = NULL,
                revoked_by = NULL,
                revoke_run_id = NULL
            WHERE source_address_key = CAST(:source_key AS uuid);
            """,
            source_key=source_key,
        )
    with pytest.raises(Exception, match="must be revoked, not deleted"):
        await db.status(
            f"""
            DELETE FROM {schema}.address_alias_v1
            WHERE source_address_key = CAST(:source_key AS uuid);
            """,
            source_key=source_key,
        )
    with pytest.raises(RuntimeError, match="revoked alias"):
        await run_numeric_grid_alias(
            mode="apply",
            schema=schema,
            alias_run_id=shadow.run_id,
            expected_candidate_sha256=shadow.candidate_digest,
            reviewed_by="ci-reviewer",
        )
    assert await db.scalar(
        f"SELECT count(*) FROM {schema}.address_alias_v1 WHERE revoked_at IS NULL;"
    ) == 0
    assert await db.scalar(
        f"SELECT generation FROM {schema}.address_alias_state_v1 WHERE singleton = true;"
    ) == 2


@pytest.mark.asyncio(loop_scope="session")
async def test_ambiguity_unit_mismatch_and_strict_evidence_fail_closed():
    _requires_test_database()
    schema = os.getenv("HLTHPRT_DB_SCHEMA", "mrf")
    await _reset_alias_data(schema)
    await _insert_archive_address(
        schema,
        first_line="1022 E 220",
        second_line=None,
        strict_source_bits=1,
    )
    await _insert_archive_address(
        schema,
        first_line="1022 E 220 N",
        second_line=None,
        strict_source_bits=6,
    )
    await _insert_archive_address(
        schema,
        first_line="1022 E 220 S",
        second_line=None,
        strict_source_bits=6,
    )
    ambiguous = await run_numeric_grid_alias(mode="shadow", schema=schema)
    assert ambiguous.ambiguous == 1
    assert ambiguous.eligible == 0
    assert ambiguous.candidate_rows == 2

    await _reset_alias_data(schema)
    await _insert_archive_address(
        schema,
        first_line="400 W 700",
        second_line="Suite 2",
        strict_source_bits=1,
    )
    await _insert_archive_address(
        schema,
        first_line="400 W 700 N",
        second_line="Suite 3",
        strict_source_bits=6,
    )
    unit_mismatch = await run_numeric_grid_alias(mode="shadow", schema=schema)
    assert unit_mismatch.candidate_sources == 0
    assert unit_mismatch.no_candidate == 1

    await _reset_alias_data(schema)
    await _insert_archive_address(
        schema,
        first_line="500 S 900",
        second_line="Unit A",
        strict_source_bits=1,
    )
    await _insert_archive_address(
        schema,
        first_line="500 S 900 E",
        second_line="Unit A",
        strict_source_bits=2,
    )
    insufficient = await run_numeric_grid_alias(mode="shadow", schema=schema)
    assert insufficient.insufficient_provenance == 1
    assert insufficient.eligible == 0


@pytest.mark.asyncio(loop_scope="session")
async def test_sql_parser_matches_python_range_guard():
    _requires_test_database()
    schema = os.getenv("HLTHPRT_DB_SCHEMA", "mrf")
    positive = await db.scalar(
        f"SELECT {schema}.addr_numeric_grid_parts_v1('1548 E 4500 S', 'Suite 202');"
    )
    hyphenated = await db.scalar(
        f"SELECT {schema}.addr_numeric_grid_parts_v1('123-125 N', NULL);"
    )
    slashed = await db.scalar(
        f"SELECT {schema}.addr_numeric_grid_parts_v1('123/125 N', NULL);"
    )

    assert list(positive) == ["1548", "e", "4500", "s"]
    assert hyphenated is None
    assert slashed is None


@pytest.mark.asyncio(loop_scope="session")
async def test_persisted_alias_preserves_strict_source_provenance(monkeypatch):
    _requires_test_database()
    schema = os.getenv("HLTHPRT_DB_SCHEMA", "mrf")
    await _reset_alias_data(schema)
    source_key = await _insert_archive_address(
        schema,
        first_line="1548 E 4500",
        second_line="Suite 202",
        strict_source_bits=1,
    )
    target_key = await _insert_archive_address(
        schema,
        first_line="1548 E 4500 S",
        second_line="Suite 202",
        strict_source_bits=6,
    )
    shadow = await run_numeric_grid_alias(mode="shadow", schema=schema)
    await run_numeric_grid_alias(
        mode="apply",
        schema=schema,
        alias_run_id=shadow.run_id,
        expected_candidate_sha256=shadow.candidate_digest,
        reviewed_by="ci-reviewer",
    )
    stage = "numeric_grid_alias_resolve_stage"
    await db.status(f"DROP TABLE IF EXISTS {schema}.{stage};")
    await db.status(
        f"""
        CREATE TABLE {schema}.{stage} (
            address_key uuid,
            first_line text,
            second_line text,
            city text,
            state text,
            zip_code text,
            country text
        );
        """
    )
    await db.status(
        f"""
        INSERT INTO {schema}.{stage} (
            address_key, first_line, second_line, city, state, zip_code, country
        ) VALUES (
            CAST(:source_key AS uuid),
            '1548 E 4500',
            'Suite 202',
            'Example City',
            'TX',
            '75001',
            'US'
        );
        """,
        source_key=source_key,
    )
    monkeypatch.setenv("HLTHPRT_ADDRESS_CANON_RUST_MATERIALIZE", "false")
    await resolve_into_archive(
        stage,
        {
            "first_line": "first_line",
            "second_line": "second_line",
            "city": "city",
            "state": "state",
            "zip": "zip_code",
            "country": "country",
        },
        source_bit=8,
        priority=4,
        schema=schema,
        strict_source_predicate="TRUE",
    )
    target = await db.first(
        f"""
        SELECT source_bits, strict_source_bits
        FROM {schema}.address_archive_v2
        WHERE address_key = CAST(:target_key AS uuid);
        """,
        target_key=target_key,
    )
    source = await db.first(
        f"""
        SELECT source_bits, strict_source_bits
        FROM {schema}.address_archive_v2
        WHERE address_key = CAST(:source_key AS uuid);
        """,
        source_key=source_key,
    )

    assert target is not None and source is not None
    assert target.source_bits & 8
    assert target.strict_source_bits == 6
    assert source.source_bits & 8
    assert source.strict_source_bits & 8


@pytest.mark.asyncio(loop_scope="session")
async def test_coordinate_restored_zip_is_not_strict_source_evidence(monkeypatch):
    """A derived ZIP may produce a key, but cannot certify raw-source lineage."""
    _requires_test_database()
    schema = os.getenv("HLTHPRT_DB_SCHEMA", "mrf")
    await _reset_alias_data(schema)
    stage = "numeric_grid_inferred_zip_stage"
    await db.status(f"DROP TABLE IF EXISTS {schema}.{stage};")
    await db.status(
        f"""
        CREATE TABLE {schema}.{stage} (
            address_key uuid,
            first_line text,
            second_line text,
            city text,
            state text,
            zip_code text,
            country text,
            lat numeric,
            long numeric
        );
        """
    )
    await db.status(
        f"""
        INSERT INTO {schema}.{stage} (
            first_line, second_line, city, state, zip_code, country, lat, long
        ) VALUES (
            '1548 E 4500 S', 'Suite 202', 'Example City', 'TX', NULL, 'US',
            32.9500, -96.8300
        );
        """
    )

    async def _restore_derived_zip(
        staging_table,
        _field_map,
        *,
        schema=None,
        **_kwargs,
    ):
        return await db.status(
            f"""
            UPDATE {schema}.{staging_table}
               SET zip_code = '75001'
             WHERE zip_code IS NULL;
            """
        )

    monkeypatch.setattr(
        address_canon,
        "restore_missing_zip_from_tiger_zcta",
        _restore_derived_zip,
    )
    monkeypatch.setenv("HLTHPRT_ADDRESS_CANON_RUST_MATERIALIZE", "false")
    field_map = {
        "first_line": "first_line",
        "second_line": "second_line",
        "city": "city",
        "state": "state",
        "zip": "zip_code",
        "country": "country",
    }
    await stamp_address_keys(stage, field_map, schema=schema)
    await resolve_into_archive(
        stage,
        field_map,
        source_bit=8,
        priority=4,
        schema=schema,
    )

    evidence = await db.first(
        f"""
        SELECT archive.source_bits, archive.strict_source_bits
          FROM {schema}.{stage} AS stage
          JOIN {schema}.address_archive_v2 AS archive
            ON archive.address_key = stage.address_key;
        """
    )
    assert evidence is not None
    assert evidence.source_bits & 8
    assert not (evidence.strict_source_bits & 8)


@pytest.mark.asyncio(loop_scope="session")
async def test_offline_serving_materializers_rewrite_aliases():
    _requires_test_database()
    schema = os.getenv("HLTHPRT_DB_SCHEMA", "mrf")
    await _reset_alias_data(schema)
    source_key = await _insert_archive_address(
        schema,
        first_line="1548 E 4500",
        second_line="Suite 202",
        strict_source_bits=1,
    )
    target_key = await _insert_archive_address(
        schema,
        first_line="1548 E 4500 S",
        second_line="Suite 202",
        strict_source_bits=6,
    )
    shadow = await run_numeric_grid_alias(mode="shadow", schema=schema)
    await run_numeric_grid_alias(
        mode="apply",
        schema=schema,
        alias_run_id=shadow.run_id,
        expected_candidate_sha256=shadow.candidate_digest,
        reviewed_by="ci-reviewer",
    )

    overlay_stage = "numeric_grid_alias_overlay_stage"
    await db.status(f"DROP TABLE IF EXISTS {schema}.{overlay_stage};")
    await db.status(
        provider_directory.provider_directory_address_overlay_table_sql(
            schema,
            overlay_stage,
        )
    )
    await db.status(
        f"""
        INSERT INTO {schema}.{overlay_stage} (
            source_record_id,
            source_id,
            resource_type,
            resource_id,
            npi,
            address_key,
            first_line,
            second_line,
            city_name,
            state_name,
            state_code,
            postal_code,
            country_code
        ) VALUES (
            'synthetic:overlay:1',
            'synthetic-source',
            'Location',
            'synthetic-location',
            1000000001,
            CAST(:source_key AS uuid),
            '1548 E 4500',
            'Suite 202',
            'Example City',
            'TX',
            'TX',
            '75001',
            'US'
        );
        """,
        source_key=source_key,
    )
    overlay_metrics = await provider_directory._materialize_address_overlay_aliases(
        schema,
        f'"{schema}"."{overlay_stage}"',
    )
    assert overlay_metrics["alias_candidates"] == 1
    assert overlay_metrics["aliases_materialized"] == 1
    assert overlay_metrics["alias_residual_source_keys"] == 0
    assert await db.scalar(
        f"SELECT address_key::text FROM {schema}.{overlay_stage};"
    ) == target_key

    raw_table = "numeric_grid_alias_unified_raw"
    await db.status(f"DROP TABLE IF EXISTS {schema}.{raw_table};")
    await db.status(
        entity_address_unified._prepare_raw_stage_sql(schema, raw_table)
    )
    source_sql = f"""
        SELECT
            'npi'::varchar AS entity_type,
            '1000000001'::varchar AS entity_id,
            1000000001::bigint AS npi,
            NULL::bigint AS inferred_npi,
            NULL::float8 AS inference_confidence,
            NULL::varchar AS inference_method,
            'Synthetic Provider'::varchar AS entity_name,
            NULL::varchar AS entity_subtype,
            'primary'::varchar AS type,
            ARRAY[0]::int[] AS taxonomy_array,
            ARRAY[0]::int[] AS plans_network_array,
            ARRAY[0]::int[] AS procedures_array,
            ARRAY[0]::int[] AS medications_array,
            ARRAY[]::varchar[] AS aca_plan_array,
            ARRAY[]::varchar[] AS aca_network_array,
            ARRAY[]::varchar[] AS ptg_plan_array,
            ARRAY[]::varchar[] AS ptg_source_array,
            ARRAY[]::varchar[] AS group_plan_array,
            'address_archive_v2:v2'::varchar AS base_address_version,
            '1548 E 4500'::varchar AS first_line,
            'Suite 202'::varchar AS second_line,
            'Example City'::varchar AS city_name,
            'TX'::varchar AS state_name,
            '75001'::varchar AS postal_code,
            'US'::varchar AS country_code,
            NULL::varchar AS telephone_number,
            NULL::varchar AS fax_number,
            NULL::varchar AS formatted_address,
            NULL::numeric AS lat,
            NULL::numeric AS long,
            NULL::date AS date_added,
            NULL::varchar AS place_id,
            CAST('{source_key}' AS uuid) AS address_key,
            NOW()::timestamp AS updated_at,
            'synthetic'::varchar AS address_source,
            'synthetic:1'::varchar AS source_record_id
    """
    await db.status(
        entity_address_unified._insert_raw_from_source_sql(
            schema,
            raw_table,
            source_sql,
        )
    )
    await entity_address_unified._validate_raw_alias_integrity(
        schema,
        raw_table,
        is_address_canon_available=True,
    )
    await db.status(
        entity_address_unified._enrich_raw_stage_sql(
            schema,
            raw_table,
            archive_available=True,
            is_address_canon_available=True,
        )
    )
    unified_row = await db.first(
        f"""
        SELECT address_key::text, base_address_version
        FROM {schema}.{raw_table};
        """
    )
    assert unified_row is not None
    assert unified_row.address_key == target_key
    assert unified_row.base_address_version.endswith("+alias-v1:g1")


@pytest.mark.asyncio(loop_scope="session")
async def test_reviewed_target_backfill_uses_exact_independent_source_evidence():
    _requires_test_database()
    probe_schema = "address_alias_backfill_probe"
    foundation = _load_module(
        ROOT / "alembic/versions/20260611100000_address_canonical_foundation.py",
        "address_alias_backfill_foundation",
    )
    migration = _load_module(
        ROOT / "alembic/versions/20260811100000_address_numeric_grid_alias.py",
        "address_alias_backfill_migration",
    )
    connection = await asyncpg.connect(
        user=os.getenv("HLTHPRT_DB_USER", "postgres"),
        password=os.getenv("HLTHPRT_DB_PASSWORD", ""),
        host=os.getenv("HLTHPRT_DB_HOST", "127.0.0.1"),
        port=int(os.getenv("HLTHPRT_DB_PORT", "5432")),
        database=os.getenv("HLTHPRT_DB_DATABASE"),
    )
    try:
        await connection.execute(f'DROP SCHEMA IF EXISTS "{probe_schema}" CASCADE;')
        await connection.execute(f'CREATE SCHEMA "{probe_schema}";')
        await connection.execute(foundation._create_functions_sql(probe_schema))
        await connection.execute(foundation._create_archive_sql(probe_schema))
        await connection.execute(migration._numeric_grid_function_sql(probe_schema))
        for statement in migration._split_sql_statements(
            migration._alias_schema_sql(probe_schema)
        ):
            await connection.execute(statement)
    finally:
        await connection.close()
    try:
        source_key = await _insert_archive_address(
            probe_schema,
            first_line="1548 E 4500",
            second_line="Suite 202",
            strict_source_bits=1,
        )
        target_key = await _insert_archive_address(
            probe_schema,
            first_line="1548 E 4500 S",
            second_line="Suite 202",
            strict_source_bits=0,
        )
        await db.status(
            f"""
            UPDATE {probe_schema}.address_archive_v2
            SET source_bits = 64
            WHERE address_key = CAST(:target_key AS uuid);
            """,
            target_key=target_key,
        )
        first_shadow = await run_numeric_grid_alias(
            mode="shadow",
            schema=probe_schema,
        )
        assert first_shadow.insufficient_provenance == 1
        assert first_shadow.candidate_digest

        with pytest.raises(Exception, match="terminal address alias run evidence"):
            await db.status(
                f"""
                UPDATE {probe_schema}.address_alias_run_v1
                   SET status = 'running'
                 WHERE run_id = CAST(:run_id AS uuid);
                """,
                run_id=first_shadow.run_id,
            )
        with pytest.raises(Exception, match="run audit rows are immutable"):
            await db.status(
                f"""
                DELETE FROM {probe_schema}.address_alias_run_v1
                WHERE run_id = CAST(:run_id AS uuid);
                """,
                run_id=first_shadow.run_id,
            )
        with pytest.raises(Exception, match="only be inserted into a running run"):
            await db.status(
                f"""
                INSERT INTO {probe_schema}.address_alias_candidate_v1
                SELECT *
                FROM {probe_schema}.address_alias_candidate_v1
                WHERE run_id = CAST(:run_id AS uuid);
                """,
                run_id=first_shadow.run_id,
            )

        original_decision = await db.scalar(
            f"""
            SELECT decision
            FROM {probe_schema}.address_alias_candidate_v1
            WHERE run_id = CAST(:run_id AS uuid);
            """,
            run_id=first_shadow.run_id,
        )
        with pytest.raises(Exception, match="candidate evidence is immutable"):
            await db.status(
                f"""
                UPDATE {probe_schema}.address_alias_candidate_v1
                   SET decision = 'ambiguous'
                 WHERE run_id = CAST(:run_id AS uuid);
                """,
                run_id=first_shadow.run_id,
            )
        await db.status(
            f"""
            ALTER TABLE {probe_schema}.address_alias_candidate_v1
            DISABLE TRIGGER address_alias_candidate_v1_guard_trg;
            """
        )
        await db.status(
            f"""
            UPDATE {probe_schema}.address_alias_candidate_v1
               SET decision = 'ambiguous'
             WHERE run_id = CAST(:run_id AS uuid);
            """,
            run_id=first_shadow.run_id,
        )
        await db.status(
            f"""
            ALTER TABLE {probe_schema}.address_alias_candidate_v1
            ENABLE TRIGGER address_alias_candidate_v1_guard_trg;
            """
        )
        with pytest.raises(RuntimeError, match="sealed digest"):
            await run_strict_source_backfill(
                schema=probe_schema,
                alias_run_id=first_shadow.run_id or "",
                expected_candidate_sha256=first_shadow.candidate_digest,
                reviewed_by="ci-reviewer",
            )
        await db.status(
            f"""
            ALTER TABLE {probe_schema}.address_alias_candidate_v1
            DISABLE TRIGGER address_alias_candidate_v1_guard_trg;
            """
        )
        await db.status(
            f"""
            UPDATE {probe_schema}.address_alias_candidate_v1
               SET decision = :decision
             WHERE run_id = CAST(:run_id AS uuid);
            """,
            run_id=first_shadow.run_id,
            decision=original_decision,
        )
        await db.status(
            f"""
            ALTER TABLE {probe_schema}.address_alias_candidate_v1
            ENABLE TRIGGER address_alias_candidate_v1_guard_trg;
            """
        )

        await db.status(
            f"""
            UPDATE {probe_schema}.address_archive_v2
               SET merged_into = CAST(:source_key AS uuid)
             WHERE address_key = CAST(:target_key AS uuid);
            """,
            source_key=source_key,
            target_key=target_key,
        )
        with pytest.raises(RuntimeError, match="target identity or merge state"):
            await run_strict_source_backfill(
                schema=probe_schema,
                alias_run_id=first_shadow.run_id or "",
                expected_candidate_sha256=first_shadow.candidate_digest,
                reviewed_by="ci-reviewer",
            )
        await db.status(
            f"""
            UPDATE {probe_schema}.address_archive_v2
               SET merged_into = NULL
             WHERE address_key = CAST(:target_key AS uuid);
            """,
            target_key=target_key,
        )

        empty_evidence = await run_strict_source_backfill(
            schema=probe_schema,
            alias_run_id=first_shadow.run_id or "",
            expected_candidate_sha256=first_shadow.candidate_digest,
            reviewed_by="ci-reviewer",
        )
        assert empty_evidence.target_count == 1
        assert empty_evidence.evidence_target_count == 0
        assert empty_evidence.evidence_pair_count == 0
        assert empty_evidence.updated_target_count == 0
        empty_evidence_retry = await run_strict_source_backfill(
            schema=probe_schema,
            alias_run_id=first_shadow.run_id or "",
            expected_candidate_sha256=first_shadow.candidate_digest,
            reviewed_by="ci-reviewer",
        )
        assert empty_evidence_retry.evidence_digest == empty_evidence.evidence_digest
        assert empty_evidence_retry.evidence_target_count == 0

        await db.status(
            f"""
            CREATE TABLE {probe_schema}.npi_address (
                row_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                address_key uuid,
                first_line text,
                second_line text,
                city_name text,
                state_name text,
                postal_code text,
                country_code text
            );
            """
        )
        await db.status(
            f"CREATE INDEX ON {probe_schema}.npi_address (address_key);"
        )
        await db.status(
            f"""
            CREATE TABLE {probe_schema}.provider_directory_address_overlay (
                source_record_id text PRIMARY KEY,
                address_key uuid,
                first_line text,
                second_line text,
                city_name text,
                state_name text,
                state_code text,
                postal_code text,
                country_code text
            );
            """
        )
        await db.status(
            f"""
            CREATE INDEX ON {probe_schema}.provider_directory_address_overlay (
                address_key
            );
            """
        )
        await db.status(
            f"""
            CREATE TABLE {probe_schema}.facility_anchor (
                id text PRIMARY KEY,
                address_key uuid,
                address_line1 text,
                city text,
                state text,
                zip_code text,
                latitude double precision,
                longitude double precision
            );
            """
        )
        await db.status(
            f"CREATE INDEX ON {probe_schema}.facility_anchor (address_key);"
        )
        await db.status(
            f"""
            INSERT INTO {probe_schema}.npi_address (
                address_key, first_line, second_line, city_name,
                state_name, postal_code, country_code
            ) VALUES
                (
                    CAST(:target_key AS uuid), '1548 E 4500 S', 'Suite 202',
                    'Example City', 'TX', '75001', 'US'
                ),
                (
                    CAST(:target_key AS uuid), '1548 E 4500 S', 'Suite 202',
                    'Example City', 'TX', '75001', 'US'
                );
            """,
            target_key=target_key,
        )
        await db.status(
            f"""
            INSERT INTO {probe_schema}.provider_directory_address_overlay (
                source_record_id, address_key, first_line, second_line,
                city_name, state_name, state_code, postal_code, country_code
            ) VALUES
                (
                    'exact', CAST(:target_key AS uuid), '1548 E 4500 S',
                    'Suite 202', 'Example City', 'TX', 'TX', '75001', 'US'
                ),
                (
                    'alias-projected', CAST(:target_key AS uuid), '1548 E 4500',
                    'Suite 202', 'Example City', 'TX', 'TX', '75001', 'US'
                ),
                (
                    'different-unit', CAST(:target_key AS uuid), '1548 E 4500 S',
                    'Suite 203', 'Example City', 'TX', 'TX', '75001', 'US'
                );
            """,
            target_key=target_key,
        )
        await db.status(
            f"""
            INSERT INTO {probe_schema}.facility_anchor (
                id, address_key, address_line1, city, state, zip_code,
                latitude, longitude
            ) VALUES (
                'coordinate-restored', CAST(:target_key AS uuid),
                '1548 E 4500 S', 'Example City', 'TX', '75001',
                32.9500, -96.8300
            );
            """,
            target_key=target_key,
        )

        backfilled = await run_strict_source_backfill(
            schema=probe_schema,
            alias_run_id=first_shadow.run_id or "",
            expected_candidate_sha256=first_shadow.candidate_digest,
            reviewed_by="ci-reviewer",
        )
        assert backfilled.target_count == 1
        assert backfilled.evidence_target_count == 1
        assert backfilled.evidence_pair_count == 2
        assert backfilled.updated_target_count == 1
        assert backfilled.source_target_counts == {
            "nppes": 1,
            "provider_directory_overlay": 1,
        }
        assert "partd_pharmacy" not in backfilled.source_target_counts
        assert "facility_anchor" not in backfilled.source_target_counts
        assert await _mark_failed(
            probe_schema,
            backfilled.run_id,
            RuntimeError("synthetic ambiguous commit result"),
        ) == "backfilled"
        assert await db.scalar(
            f"""
            SELECT status
            FROM {probe_schema}.address_alias_run_v1
            WHERE run_id = CAST(:run_id AS uuid);
            """,
            run_id=backfilled.run_id,
        ) == "backfilled"
        target = await db.first(
            f"""
            SELECT source_bits, strict_source_bits
            FROM {probe_schema}.address_archive_v2
            WHERE address_key = CAST(:target_key AS uuid);
            """,
            target_key=target_key,
        )
        assert target is not None
        assert target.source_bits == 193
        assert target.strict_source_bits == 129
        assert target.strict_source_bits & 64 == 0

        retried = await run_strict_source_backfill(
            schema=probe_schema,
            alias_run_id=first_shadow.run_id or "",
            expected_candidate_sha256=first_shadow.candidate_digest,
            reviewed_by="ci-reviewer",
        )
        assert retried.evidence_digest == backfilled.evidence_digest
        assert retried.updated_target_count == 0

        await db.status(
            f"""
            CREATE TABLE {probe_schema}.doctor_clinician_address (
                address_key uuid,
                address_line1 text,
                address_line2 text,
                city text,
                state text,
                zip_code text
            );
            """
        )
        with pytest.raises(RuntimeError, match="leading address_key index"):
            await run_strict_source_backfill(
                schema=probe_schema,
                alias_run_id=first_shadow.run_id or "",
                expected_candidate_sha256=first_shadow.candidate_digest,
                reviewed_by="ci-reviewer",
            )
        await db.status(
            f"DROP TABLE {probe_schema}.doctor_clinician_address;"
        )
        unchanged = await db.first(
            f"""
            SELECT source_bits, strict_source_bits
            FROM {probe_schema}.address_archive_v2
            WHERE address_key = CAST(:target_key AS uuid);
            """,
            target_key=target_key,
        )
        assert unchanged is not None
        assert unchanged.source_bits == 193
        assert unchanged.strict_source_bits == 129

        fresh_shadow = await run_numeric_grid_alias(
            mode="shadow",
            schema=probe_schema,
        )
        assert fresh_shadow.eligible == 1
        assert fresh_shadow.insufficient_provenance == 0
        candidate = await db.first(
            f"""
            SELECT source_address_key::text, target_address_key::text,
                   target_strict_source_bits, target_strict_source_count
            FROM {probe_schema}.address_alias_candidate_v1
            WHERE run_id = CAST(:run_id AS uuid);
            """,
            run_id=fresh_shadow.run_id,
        )
        assert candidate is not None
        assert candidate.source_address_key == source_key
        assert candidate.target_address_key == target_key
        assert candidate.target_strict_source_bits == 129
        assert candidate.target_strict_source_count == 2
    finally:
        await db.status(f'DROP SCHEMA IF EXISTS "{probe_schema}" CASCADE;')


@pytest.mark.asyncio(loop_scope="session")
async def test_strict_backfill_uses_index_probes_for_uuid_and_text_keys():
    """The target-scoped evidence query must not scan an entire source table."""
    _requires_test_database()
    probe_schema = "address_alias_backfill_plan_probe"
    foundation = _load_module(
        ROOT / "alembic/versions/20260611100000_address_canonical_foundation.py",
        "address_alias_backfill_plan_foundation",
    )
    connection = await asyncpg.connect(
        user=os.getenv("HLTHPRT_DB_USER", "postgres"),
        password=os.getenv("HLTHPRT_DB_PASSWORD", ""),
        host=os.getenv("HLTHPRT_DB_HOST", "127.0.0.1"),
        port=int(os.getenv("HLTHPRT_DB_PORT", "5432")),
        database=os.getenv("HLTHPRT_DB_DATABASE"),
    )

    def _nodes(value):
        if isinstance(value, dict):
            yield value
            for child in value.values():
                yield from _nodes(child)
        elif isinstance(value, list):
            for child in value:
                yield from _nodes(child)

    try:
        await connection.execute(f'DROP SCHEMA IF EXISTS "{probe_schema}" CASCADE;')
        await connection.execute(f'CREATE SCHEMA "{probe_schema}";')
        await connection.execute(foundation._create_functions_sql(probe_schema))
        await connection.execute(
            """
            CREATE TEMP TABLE address_strict_backfill_targets (
                address_key uuid PRIMARY KEY,
                identity_key text NOT NULL
            ) ON COMMIT PRESERVE ROWS;
            CREATE TEMP TABLE address_strict_backfill_evidence (
                target_address_key uuid NOT NULL,
                source_bit integer NOT NULL,
                source_name text NOT NULL,
                PRIMARY KEY (target_address_key, source_bit, source_name)
            ) ON COMMIT PRESERVE ROWS;
            """
        )
        await connection.execute(
            f"""
            INSERT INTO address_strict_backfill_targets (
                address_key, identity_key
            )
            SELECT
                "{probe_schema}".addr_key_v1(
                    '1548 E 4500 S', 'Suite 202', 'Example City',
                    'TX', '75001', 'US'
                ),
                "{probe_schema}".addr_identity_key_v1(
                    '1548 E 4500 S', 'Suite 202', 'Example City',
                    'TX', '75001', 'US'
                );
            CREATE TABLE "{probe_schema}".npi_address (
                address_key uuid,
                first_line text,
                second_line text,
                city_name text,
                state_name text,
                postal_code text,
                country_code text
            );
            CREATE INDEX ON "{probe_schema}".npi_address (address_key);
            CREATE TABLE "{probe_schema}".provider_directory_location (
                address_key text,
                first_line text,
                second_line text,
                city_name text,
                state_name text,
                state_code text,
                postal_code text,
                country_code text
            );
            CREATE INDEX ON "{probe_schema}".provider_directory_location (address_key);
            """
        )
        await connection.execute(
            f"""
            INSERT INTO "{probe_schema}".npi_address
            SELECT md5('npi-decoy-' || value::text)::uuid,
                   'Decoy ' || value::text, NULL, 'Example City',
                   'TX', '75001', 'US'
            FROM generate_series(1, 20000) AS value;
            INSERT INTO "{probe_schema}".provider_directory_location
            SELECT md5('location-decoy-' || value::text)::uuid::text,
                   'Decoy ' || value::text, NULL, 'Example City',
                   'TX', 'TX', '75001', 'US'
            FROM generate_series(1, 20000) AS value;
            INSERT INTO "{probe_schema}".npi_address
            SELECT target.address_key, '1548 E 4500 S', 'Suite 202',
                   'Example City', 'TX', '75001', 'US'
            FROM address_strict_backfill_targets AS target;
            INSERT INTO "{probe_schema}".provider_directory_location
            SELECT target.address_key::text, '1548 E 4500 S', 'Suite 202',
                   'Example City', 'TX', 'TX', '75001', 'US'
            FROM address_strict_backfill_targets AS target;
            ANALYZE address_strict_backfill_targets;
            ANALYZE "{probe_schema}".npi_address;
            ANALYZE "{probe_schema}".provider_directory_location;
            """
        )

        projections = {
            projection.name: projection
            for projection in address_strict_source_backfill_sql.SOURCE_PROJECTIONS
        }
        for projection_name in ("nppes", "provider_directory_location"):
            projection = projections[projection_name]
            evidence_sql = address_strict_source_backfill_sql.evidence_insert_sql(
                schema=probe_schema,
                projection=projection,
            ).strip().rstrip(";")
            encoded_plan = await connection.fetchval(
                f"EXPLAIN (FORMAT JSON) {evidence_sql}"
            )
            plan = json.loads(encoded_plan) if isinstance(encoded_plan, str) else encoded_plan
            source_nodes = [
                node
                for node in _nodes(plan)
                if node.get("Relation Name") == projection.table
            ]
            assert source_nodes, projection_name
            assert all(node.get("Node Type") != "Seq Scan" for node in source_nodes), (
                projection_name,
                source_nodes,
            )
            assert all(
                node.get("Node Type")
                in {"Index Scan", "Index Only Scan", "Bitmap Heap Scan"}
                for node in source_nodes
            ), (
                projection_name,
                source_nodes,
            )
    finally:
        await connection.execute(f'DROP SCHEMA IF EXISTS "{probe_schema}" CASCADE;')
        await connection.close()


@pytest.mark.asyncio(loop_scope="session")
async def test_apply_rereads_archive_after_waiting_for_resolver_lock():
    """A target committed while apply waits must invalidate the reviewed set."""
    _requires_test_database()
    probe_schema = "address_alias_apply_race_probe"
    foundation = _load_module(
        ROOT / "alembic/versions/20260611100000_address_canonical_foundation.py",
        "address_alias_apply_race_foundation",
    )
    migration = _load_module(
        ROOT / "alembic/versions/20260811100000_address_numeric_grid_alias.py",
        "address_alias_apply_race_migration",
    )
    connection = await asyncpg.connect(
        user=os.getenv("HLTHPRT_DB_USER", "postgres"),
        password=os.getenv("HLTHPRT_DB_PASSWORD", ""),
        host=os.getenv("HLTHPRT_DB_HOST", "127.0.0.1"),
        port=int(os.getenv("HLTHPRT_DB_PORT", "5432")),
        database=os.getenv("HLTHPRT_DB_DATABASE"),
    )
    transaction = None
    apply_task = None
    try:
        await connection.execute(f'DROP SCHEMA IF EXISTS "{probe_schema}" CASCADE;')
        await connection.execute(f'CREATE SCHEMA "{probe_schema}";')
        await connection.execute(foundation._create_functions_sql(probe_schema))
        await connection.execute(foundation._create_archive_sql(probe_schema))
        await connection.execute(migration._numeric_grid_function_sql(probe_schema))
        for statement in migration._split_sql_statements(
            migration._alias_schema_sql(probe_schema)
        ):
            await connection.execute(statement)

        await _insert_archive_address(
            probe_schema,
            first_line="1548 E 4500",
            second_line="Suite 202",
            strict_source_bits=1,
        )
        await _insert_archive_address(
            probe_schema,
            first_line="1548 E 4500 S",
            second_line="Suite 202",
            strict_source_bits=6,
        )
        shadow = await run_numeric_grid_alias(mode="shadow", schema=probe_schema)
        assert shadow.eligible == 1
        assert shadow.candidate_digest

        transaction = connection.transaction()
        await transaction.start()
        await connection.fetchval(
            "SELECT pg_advisory_xact_lock(hashtext($1));",
            address_canon._archive_lock_key(
                probe_schema,
                "address_archive_v2",
                "resolve",
            ),
        )
        apply_task = asyncio.create_task(
            run_numeric_grid_alias(
                mode="apply",
                schema=probe_schema,
                alias_run_id=shadow.run_id,
                expected_candidate_sha256=shadow.candidate_digest,
                reviewed_by="ci-reviewer",
                timeout="10s",
            )
        )
        for _ in range(200):
            waiting = await connection.fetchval(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM pg_locks
                    WHERE pid <> pg_backend_pid()
                      AND locktype = 'advisory'
                      AND NOT granted
                );
                """
            )
            if waiting:
                break
            if apply_task.done():
                await apply_task
            await asyncio.sleep(0.01)
        assert waiting, "apply did not wait on the archive resolver lock"

        await connection.fetchval(
            f"""
            INSERT INTO "{probe_schema}".address_archive_v2 (
                address_key, identity_key, identity_version, precision,
                premise_key, line1_norm, unit_norm, city_norm, state_code,
                zip5, country_code, first_line, second_line, city_name,
                state_name, postal_code, source_bits, strict_source_bits
            )
            SELECT
                "{probe_schema}".addr_key_v1($1, $2, $3, $4, $5, 'US'),
                "{probe_schema}".addr_identity_key_v1($1, $2, $3, $4, $5, 'US'),
                2, 'street',
                "{probe_schema}".addr_premise_key_v1($1, $2, $3, $4, $5, 'US'),
                "{probe_schema}".addr_street_norm_v1($1, $2),
                "{probe_schema}".addr_unit_norm_v1($1, $2),
                "{probe_schema}".addr_city_norm_v1($3),
                "{probe_schema}".addr_state_code_v1($4),
                left($5, 5), 'US', $1, $2, $3, $4, $5, 6, 6
            RETURNING address_key;
            """,
            "1548 E 4500 N",
            "Suite 202",
            "Example City",
            "TX",
            "75001",
        )
        await transaction.commit()
        transaction = None

        with pytest.raises(RuntimeError, match="candidate set changed after review"):
            await apply_task
        apply_task = None
        assert await db.scalar(
            f"""
            SELECT count(*)
            FROM {probe_schema}.address_alias_v1
            WHERE revoked_at IS NULL;
            """
        ) == 0
    finally:
        if transaction is not None:
            await transaction.rollback()
        if apply_task is not None and not apply_task.done():
            apply_task.cancel()
            try:
                await apply_task
            except asyncio.CancelledError:
                pass
        await connection.execute(f'DROP SCHEMA IF EXISTS "{probe_schema}" CASCADE;')
        await connection.close()


@pytest.mark.asyncio(loop_scope="session")
async def test_strict_source_predicate_excludes_npi_filled_partd_address(monkeypatch):
    _requires_test_database()
    schema = os.getenv("HLTHPRT_DB_SCHEMA", "mrf")
    await _reset_alias_data(schema)
    stage = "numeric_grid_partd_lineage_stage"
    await db.status(f"DROP TABLE IF EXISTS {schema}.{stage};")
    await db.status(
        f"""
        CREATE TABLE {schema}.{stage} (
            address_key uuid,
            first_line text,
            second_line text,
            city text,
            state text,
            zip_code text,
            address_observed_in_source boolean NOT NULL
        );
        """
    )
    field_map = {
        "first_line": "first_line",
        "second_line": "second_line",
        "city": "city",
        "state": "state",
        "zip": "zip_code",
        "country": "'US'",
    }
    monkeypatch.setenv("HLTHPRT_ADDRESS_CANON_RUST_MATERIALIZE", "false")
    try:
        await db.status(
            f"""
            INSERT INTO {schema}.{stage} VALUES (
                NULL, '10 E 20 N', 'Suite 5', 'Example City', 'TX', '75001', false
            );
            """
        )
        await stamp_address_keys(stage, field_map, schema=schema)
        await resolve_into_archive(
            stage,
            field_map,
            source_bit=64,
            priority=7,
            schema=schema,
            strict_source_predicate=(
                "strict_source.address_observed_in_source IS TRUE"
            ),
        )
        derived = await db.first(
            f"""
            SELECT source_bits, strict_source_bits
            FROM {schema}.address_archive_v2
            WHERE address_key = {schema}.addr_key_v1(
                '10 E 20 N', 'Suite 5', 'Example City', 'TX', '75001', 'US'
            );
            """
        )
        assert derived is not None
        assert derived.source_bits == 64
        assert derived.strict_source_bits == 0

        await db.status(f"TRUNCATE TABLE {schema}.{stage};")
        await db.status(
            f"""
            INSERT INTO {schema}.{stage} VALUES (
                NULL, '10 E 20 N', 'Suite 5', 'Example City', 'TX', '75001', true
            );
            """
        )
        await stamp_address_keys(stage, field_map, schema=schema)
        await resolve_into_archive(
            stage,
            field_map,
            source_bit=64,
            priority=7,
            schema=schema,
            strict_source_predicate=(
                "strict_source.address_observed_in_source IS TRUE"
            ),
        )
        direct = await db.first(
            f"""
            SELECT strict_source_bits
            FROM {schema}.address_archive_v2
            WHERE address_key = {schema}.addr_key_v1(
                '10 E 20 N', 'Suite 5', 'Example City', 'TX', '75001', 'US'
            );
            """
        )
        assert direct is not None
        assert direct.strict_source_bits == 64
    finally:
        await db.status(f"DROP TABLE IF EXISTS {schema}.{stage};")


@pytest.mark.asyncio(loop_scope="session")
async def test_migration_upgrade_and_downgrade_execute_on_postgresql():
    _requires_test_database()
    probe_schema = "address_alias_migration_probe"
    foundation = _load_module(
        ROOT / "alembic/versions/20260611100000_address_canonical_foundation.py",
        "address_alias_foundation_probe",
    )
    migration = _load_module(
        ROOT / "alembic/versions/20260811100000_address_numeric_grid_alias.py",
        "address_alias_migration_probe",
    )
    connection = await asyncpg.connect(
        user=os.getenv("HLTHPRT_DB_USER", "postgres"),
        password=os.getenv("HLTHPRT_DB_PASSWORD", ""),
        host=os.getenv("HLTHPRT_DB_HOST", "127.0.0.1"),
        port=int(os.getenv("HLTHPRT_DB_PORT", "5432")),
        database=os.getenv("HLTHPRT_DB_DATABASE"),
    )
    try:
        await connection.execute(f'DROP SCHEMA IF EXISTS "{probe_schema}" CASCADE;')
        await connection.execute(f'CREATE SCHEMA "{probe_schema}";')
        await connection.execute(foundation._create_functions_sql(probe_schema))
        await connection.execute(foundation._create_archive_sql(probe_schema))
        await connection.execute(
            f'CREATE TABLE "{probe_schema}".partd_pharmacy_activity_stage_v2 '
            "(id bigint PRIMARY KEY);"
        )
        await connection.execute(migration._numeric_grid_function_sql(probe_schema))
        for statement in migration._split_sql_statements(
            migration._alias_schema_sql(probe_schema)
        ):
            await connection.execute(statement)
        assert await connection.fetchval(
            "SELECT to_regclass($1) IS NOT NULL;",
            f"{probe_schema}.address_alias_v1",
        )
        artifact_rows = await connection.fetch(
            f"""
            SELECT artifact_name, generation
            FROM "{probe_schema}".address_alias_artifact_state_v1
            ORDER BY artifact_name;
            """
        )
        assert [
            (row["artifact_name"], row["generation"])
            for row in artifact_rows
        ] == [
            ("provider_directory_address_corroboration", 0),
            ("provider_directory_address_overlay", 0),
        ]
        assert await connection.fetchval(
            """
            SELECT is_nullable = 'NO'
            FROM information_schema.columns
            WHERE table_schema = $1
              AND table_name = 'partd_pharmacy_activity_stage_v2'
              AND column_name = 'address_observed_in_source';
            """,
            probe_schema,
        )

        for statement in migration._downgrade_statements(probe_schema):
            await connection.execute(statement)

        assert not await connection.fetchval(
            "SELECT to_regclass($1) IS NOT NULL;",
            f"{probe_schema}.address_alias_v1",
        )
        assert not await connection.fetchval(
            """
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema = $1
                  AND table_name = 'address_archive_v2'
                  AND column_name = 'strict_source_bits'
            );
            """,
            probe_schema,
        )
        assert not await connection.fetchval(
            """
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema = $1
                  AND table_name = 'partd_pharmacy_activity_stage_v2'
                  AND column_name = 'address_observed_in_source'
            );
            """,
            probe_schema,
        )
    finally:
        await connection.execute(f'DROP SCHEMA IF EXISTS "{probe_schema}" CASCADE;')
        await connection.close()
