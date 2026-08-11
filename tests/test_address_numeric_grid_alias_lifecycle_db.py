# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""PostgreSQL cases for reviewed numeric-grid address aliases."""

from tests.test_address_numeric_grid_alias_db import (
    ROOT,
    _insert_archive_address,
    _load_module,
    _mark_failed,
    _prepare_serving_fence_schema,
    _replace_overlay_relation,
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

async def _prepare_alias_lifecycle() -> tuple[str, str, str, object]:
    """Seed one eligible pair and return its sealed shadow."""
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
    return schema, source_key, target_key, shadow


async def _apply_reviewed_alias(
    schema: str,
    source_key: str,
    target_key: str,
    shadow,
) -> None:
    """Apply the reviewed candidate and verify the active mapping."""
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


async def _assert_alias_retry(schema: str, shadow) -> None:
    """Prove a repeated apply preserves generation and promotes nothing."""
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


async def _revoke_reviewed_alias(
    schema: str,
    source_key: str,
    target_key: str,
    shadow,
) -> None:
    """Revoke once and prove the reviewed run cannot resurrect its mapping."""
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


@pytest.mark.asyncio(loop_scope="session")
async def test_shadow_apply_retry_and_revoke_are_generation_safe():
    """Promotion, retry, and revocation advance generation exactly once."""
    _requires_test_database()
    schema, source_key, target_key, shadow = await _prepare_alias_lifecycle()
    await _apply_reviewed_alias(schema, source_key, target_key, shadow)
    await _assert_alias_retry(schema, shadow)
    await _revoke_reviewed_alias(schema, source_key, target_key, shadow)
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
    """Ambiguous, cross-unit, and weakly sourced pairs all fail closed."""
    _requires_test_database()
    schema = os.getenv("HLTHPRT_DB_SCHEMA", "mrf")
    await _assert_ambiguous_grid_pair(schema)
    await _assert_unit_mismatch(schema)
    await _assert_insufficient_provenance(schema)


async def _assert_ambiguous_grid_pair(schema: str) -> None:
    """Prove two terminal directions remain ambiguous."""
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


async def _assert_unit_mismatch(schema: str) -> None:
    """Prove distinct units do not become alias candidates."""
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


async def _assert_insufficient_provenance(schema: str) -> None:
    """Prove one strict target source cannot authorize an alias."""
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


async def _apply_provenance_alias(schema: str) -> tuple[str, str]:
    """Create and apply the canonical incomplete-to-complete alias pair."""
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
    return source_key, target_key


async def _create_provenance_stage(schema: str, stage: str, source_key: str) -> None:
    """Create a raw stage that directly observes the approved source key."""
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
            CAST(:source_key AS uuid), '1548 E 4500', 'Suite 202',
            'Example City', 'TX', '75001', 'US'
        );
        """,
        source_key=source_key,
    )


async def _resolve_provenance_stage(schema: str, stage: str, monkeypatch) -> None:
    """Resolve a direct stage while preserving strict evidence on its source."""
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


async def _assert_preserved_provenance(
    schema: str,
    source_key: str,
    target_key: str,
) -> None:
    """Verify projected and direct evidence remain attached to the right keys."""
    target_archive_row = await db.first(
        f"""
        SELECT source_bits, strict_source_bits FROM {schema}.address_archive_v2
        WHERE address_key = CAST(:target_key AS uuid);
        """,
        target_key=target_key,
    )
    source_archive_row = await db.first(
        f"""
        SELECT source_bits, strict_source_bits FROM {schema}.address_archive_v2
        WHERE address_key = CAST(:source_key AS uuid);
        """,
        source_key=source_key,
    )
    assert target_archive_row is not None and source_archive_row is not None
    assert target_archive_row.source_bits & 8
    assert target_archive_row.strict_source_bits == 6
    assert source_archive_row.source_bits & 8
    assert source_archive_row.strict_source_bits & 8


@pytest.mark.asyncio(loop_scope="session")
async def test_persisted_alias_preserves_strict_source_provenance(monkeypatch):
    """Alias projection must not transfer direct provenance to its target."""
    _requires_test_database()
    schema = os.getenv("HLTHPRT_DB_SCHEMA", "mrf")
    source_key, target_key = await _apply_provenance_alias(schema)
    stage = "numeric_grid_alias_resolve_stage"
    await _create_provenance_stage(schema, stage, source_key)
    await _resolve_provenance_stage(schema, stage, monkeypatch)
    await _assert_preserved_provenance(schema, source_key, target_key)


async def _create_inferred_zip_stage(schema: str, stage: str) -> None:
    """Create a coordinate-bearing row whose source omitted the ZIP."""
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
    """Simulate coordinate-to-ZIP restoration without raw-source lineage."""
    return await db.status(
        f"""
        UPDATE {schema}.{staging_table}
           SET zip_code = '75001'
         WHERE zip_code IS NULL;
        """
    )


async def _resolve_inferred_zip_stage(schema: str, stage: str, monkeypatch) -> None:
    """Stamp and resolve an inferred-ZIP row without a strict predicate."""
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


async def _assert_inferred_zip_evidence(schema: str, stage: str) -> None:
    """Verify derived identity contributes ordinary but not strict evidence."""
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
async def test_coordinate_restored_zip_is_not_strict_source_evidence(monkeypatch):
    """A derived ZIP may produce a key, but cannot certify raw-source lineage."""
    _requires_test_database()
    schema = os.getenv("HLTHPRT_DB_SCHEMA", "mrf")
    await _reset_alias_data(schema)
    stage = "numeric_grid_inferred_zip_stage"
    await _create_inferred_zip_stage(schema, stage)
    await _resolve_inferred_zip_stage(schema, stage, monkeypatch)
    await _assert_inferred_zip_evidence(schema, stage)
