# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""PostgreSQL proof for reviewed strict-source provenance backfill."""

from dataclasses import dataclass

from tests.test_address_numeric_grid_alias_db import (
    _insert_archive_address,
    _mark_failed,
    _requires_test_database,
    asyncpg,
    db,
    os,
    pytest,
    run_numeric_grid_alias,
    run_strict_source_backfill,
)
from tests.test_address_numeric_grid_alias_runtime_db import (
    _create_alias_probe_schema,
)


@dataclass(frozen=True)
class _BackfillProbe:
    schema: str
    source_key: str
    target_key: str
    shadow: object


async def _connect_probe_database():
    """Open the disposable PostgreSQL connection used for schema bootstrap."""
    return await asyncpg.connect(
        user=os.getenv("HLTHPRT_DB_USER", "postgres"),
        password=os.getenv("HLTHPRT_DB_PASSWORD", ""),
        host=os.getenv("HLTHPRT_DB_HOST", "127.0.0.1"),
        port=int(os.getenv("HLTHPRT_DB_PORT", "5432")),
        database=os.getenv("HLTHPRT_DB_DATABASE"),
    )


async def _create_backfill_schema(schema: str) -> None:
    """Create canonical and alias relations in an isolated probe schema."""
    connection = await _connect_probe_database()
    try:
        await _create_alias_probe_schema(connection, schema, "backfill")
    finally:
        await connection.close()


async def _seed_backfill_probe(schema: str) -> _BackfillProbe:
    """Create one insufficient candidate whose target lacks strict evidence."""
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
        strict_source_bits=0,
    )
    await db.status(
        f"""
        UPDATE {schema}.address_archive_v2
        SET source_bits = 64
        WHERE address_key = CAST(:target_key AS uuid);
        """,
        target_key=target_key,
    )
    shadow = await run_numeric_grid_alias(mode="shadow", schema=schema)
    assert shadow.insufficient_provenance == 1
    assert shadow.candidate_digest
    return _BackfillProbe(schema, source_key, target_key, shadow)


async def _assert_terminal_audit_guards(probe: _BackfillProbe) -> None:
    """Prove sealed runs and their candidate set cannot be rewritten."""
    with pytest.raises(Exception, match="terminal address alias run evidence"):
        await db.status(
            f"""
            UPDATE {probe.schema}.address_alias_run_v1 SET status = 'running'
            WHERE run_id = CAST(:run_id AS uuid);
            """,
            run_id=probe.shadow.run_id,
        )
    with pytest.raises(Exception, match="run audit rows are immutable"):
        await db.status(
            f"""
            DELETE FROM {probe.schema}.address_alias_run_v1
            WHERE run_id = CAST(:run_id AS uuid);
            """,
            run_id=probe.shadow.run_id,
        )
    with pytest.raises(Exception, match="only be inserted into a running run"):
        await db.status(
            f"""
            INSERT INTO {probe.schema}.address_alias_candidate_v1
            SELECT * FROM {probe.schema}.address_alias_candidate_v1
            WHERE run_id = CAST(:run_id AS uuid);
            """,
            run_id=probe.shadow.run_id,
        )
    with pytest.raises(Exception, match="candidate evidence is immutable"):
        await db.status(
            f"""
            UPDATE {probe.schema}.address_alias_candidate_v1
            SET decision = 'ambiguous'
            WHERE run_id = CAST(:run_id AS uuid);
            """,
            run_id=probe.shadow.run_id,
        )


async def _set_candidate_decision_unchecked(
    probe: _BackfillProbe,
    decision: str,
) -> None:
    """Change a sealed candidate only for the digest-tamper regression."""
    table = f"{probe.schema}.address_alias_candidate_v1"
    await db.status(
        f"ALTER TABLE {table} DISABLE TRIGGER address_alias_candidate_v1_guard_trg;"
    )
    await db.status(
        f"""
        UPDATE {table} SET decision = :decision
        WHERE run_id = CAST(:run_id AS uuid);
        """,
        run_id=probe.shadow.run_id,
        decision=decision,
    )
    await db.status(
        f"ALTER TABLE {table} ENABLE TRIGGER address_alias_candidate_v1_guard_trg;"
    )


async def _assert_digest_tamper_guard(probe: _BackfillProbe) -> None:
    """Prove backfill recomputes the sealed candidate digest before mutation."""
    original_decision = await db.scalar(
        f"""
        SELECT decision FROM {probe.schema}.address_alias_candidate_v1
        WHERE run_id = CAST(:run_id AS uuid);
        """,
        run_id=probe.shadow.run_id,
    )
    await _set_candidate_decision_unchecked(probe, "ambiguous")
    with pytest.raises(RuntimeError, match="sealed digest"):
        await _run_backfill(probe)
    await _set_candidate_decision_unchecked(probe, original_decision)


async def _run_backfill(probe: _BackfillProbe):
    """Run the reviewed target-only provenance backfill."""
    return await run_strict_source_backfill(
        schema=probe.schema,
        alias_run_id=probe.shadow.run_id or "",
        expected_candidate_sha256=probe.shadow.candidate_digest,
        reviewed_by="ci-reviewer",
    )


async def _assert_target_drift_guard(probe: _BackfillProbe) -> None:
    """Prove a merged target invalidates the reviewed candidate snapshot."""
    await db.status(
        f"""
        UPDATE {probe.schema}.address_archive_v2
        SET merged_into = CAST(:source_key AS uuid)
        WHERE address_key = CAST(:target_key AS uuid);
        """,
        source_key=probe.source_key,
        target_key=probe.target_key,
    )
    with pytest.raises(RuntimeError, match="target identity or merge state"):
        await _run_backfill(probe)
    await db.status(
        f"""
        UPDATE {probe.schema}.address_archive_v2 SET merged_into = NULL
        WHERE address_key = CAST(:target_key AS uuid);
        """,
        target_key=probe.target_key,
    )


async def _assert_empty_backfill_retry(probe: _BackfillProbe) -> None:
    """Prove zero-evidence receipts are complete and retry-idempotent."""
    empty_evidence = await _run_backfill(probe)
    assert empty_evidence.target_count == 1
    assert empty_evidence.evidence_target_count == 0
    assert empty_evidence.evidence_pair_count == 0
    assert empty_evidence.updated_target_count == 0
    empty_retry = await _run_backfill(probe)
    assert empty_retry.evidence_digest == empty_evidence.evidence_digest
    assert empty_retry.evidence_target_count == 0


_SOURCE_TABLES_SQL = """
    CREATE TABLE {schema}.npi_address (
        row_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        address_key uuid, first_line text, second_line text, city_name text,
        state_name text, postal_code text, country_code text
    );
    CREATE INDEX ON {schema}.npi_address (address_key);
    CREATE TABLE {schema}.provider_directory_address_overlay (
        source_record_id text PRIMARY KEY, address_key uuid, first_line text,
        second_line text, city_name text, state_name text, state_code text,
        postal_code text, country_code text
    );
    CREATE INDEX ON {schema}.provider_directory_address_overlay (address_key);
    CREATE TABLE {schema}.facility_anchor (
        id text PRIMARY KEY, address_key uuid, address_line1 text, city text,
        state text, zip_code text, latitude double precision,
        longitude double precision
    );
    CREATE INDEX ON {schema}.facility_anchor (address_key);
"""


_SOURCE_ROWS_SQL = """
    INSERT INTO {schema}.npi_address (
        address_key, first_line, second_line, city_name,
        state_name, postal_code, country_code
    ) VALUES
        (CAST(:target_key AS uuid), '1548 E 4500 S', 'Suite 202',
         'Example City', 'TX', '75001', 'US'),
        (CAST(:target_key AS uuid), '1548 E 4500 S', 'Suite 202',
         'Example City', 'TX', '75001', 'US');
    INSERT INTO {schema}.provider_directory_address_overlay (
        source_record_id, address_key, first_line, second_line,
        city_name, state_name, state_code, postal_code, country_code
    ) VALUES
        ('exact', CAST(:target_key AS uuid), '1548 E 4500 S',
         'Suite 202', 'Example City', 'TX', 'TX', '75001', 'US'),
        ('alias-projected', CAST(:target_key AS uuid), '1548 E 4500',
         'Suite 202', 'Example City', 'TX', 'TX', '75001', 'US'),
        ('different-unit', CAST(:target_key AS uuid), '1548 E 4500 S',
         'Suite 203', 'Example City', 'TX', 'TX', '75001', 'US');
    INSERT INTO {schema}.facility_anchor (
        id, address_key, address_line1, city, state, zip_code,
        latitude, longitude
    ) VALUES (
        'coordinate-restored', CAST(:target_key AS uuid), '1548 E 4500 S',
        'Example City', 'TX', '75001', 32.9500, -96.8300
    );
"""


async def _seed_independent_source_evidence(probe: _BackfillProbe) -> None:
    """Create exact independent rows plus rejected derived and projected rows."""
    for statement in _SOURCE_TABLES_SQL.format(schema=probe.schema).split(";"):
        if statement.strip():
            await db.status(statement + ";")
    for statement in _SOURCE_ROWS_SQL.format(schema=probe.schema).split(";"):
        if statement.strip():
            await db.status(statement + ";", target_key=probe.target_key)


async def _assert_successful_backfill(probe: _BackfillProbe):
    """Verify only exact independent source families become strict evidence."""
    backfilled = await _run_backfill(probe)
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
        probe.schema,
        backfilled.run_id,
        RuntimeError("synthetic ambiguous commit result"),
    ) == "backfilled"
    assert await db.scalar(
        f"""
        SELECT status FROM {probe.schema}.address_alias_run_v1
        WHERE run_id = CAST(:run_id AS uuid);
        """,
        run_id=backfilled.run_id,
    ) == "backfilled"
    return backfilled


async def _assert_archive_evidence(probe: _BackfillProbe) -> None:
    """Verify exact masks retain historical ordinary but not strict lineage."""
    target_archive_row = await db.first(
        f"""
        SELECT source_bits, strict_source_bits
        FROM {probe.schema}.address_archive_v2
        WHERE address_key = CAST(:target_key AS uuid);
        """,
        target_key=probe.target_key,
    )
    assert target_archive_row is not None
    assert target_archive_row.source_bits == 193
    assert target_archive_row.strict_source_bits == 129
    assert target_archive_row.strict_source_bits & 64 == 0


async def _assert_backfill_retry(probe: _BackfillProbe, backfilled) -> None:
    """Prove a successful evidence OR is retry-idempotent."""
    retried = await _run_backfill(probe)
    assert retried.evidence_digest == backfilled.evidence_digest
    assert retried.updated_target_count == 0


async def _assert_missing_index_guard(probe: _BackfillProbe) -> None:
    """Prove an unindexed optional source aborts without mutating evidence."""
    await db.status(
        f"""
        CREATE TABLE {probe.schema}.doctor_clinician_address (
            address_key uuid, address_line1 text, address_line2 text,
            city text, state text, zip_code text
        );
        """
    )
    with pytest.raises(RuntimeError, match="leading address_key index"):
        await _run_backfill(probe)
    await db.status(f"DROP TABLE {probe.schema}.doctor_clinician_address;")
    await _assert_archive_evidence(probe)


async def _assert_fresh_shadow_eligible(probe: _BackfillProbe) -> None:
    """Verify a new shadow observes the two independently proven source bits."""
    fresh_shadow = await run_numeric_grid_alias(mode="shadow", schema=probe.schema)
    assert fresh_shadow.eligible == 1
    assert fresh_shadow.insufficient_provenance == 0
    candidate = await db.first(
        f"""
        SELECT source_address_key::text, target_address_key::text,
               target_strict_source_bits, target_strict_source_count
        FROM {probe.schema}.address_alias_candidate_v1
        WHERE run_id = CAST(:run_id AS uuid);
        """,
        run_id=fresh_shadow.run_id,
    )
    assert candidate is not None
    assert candidate.source_address_key == probe.source_key
    assert candidate.target_address_key == probe.target_key
    assert candidate.target_strict_source_bits == 129
    assert candidate.target_strict_source_count == 2


@pytest.mark.asyncio(loop_scope="session")
async def test_reviewed_target_backfill_uses_exact_independent_source_evidence():
    """Reviewed backfill is exact, auditable, bounded, and retry-safe."""
    _requires_test_database()
    probe_schema = "address_alias_backfill_probe"
    await _create_backfill_schema(probe_schema)
    try:
        probe = await _seed_backfill_probe(probe_schema)
        await _assert_terminal_audit_guards(probe)
        await _assert_digest_tamper_guard(probe)
        await _assert_target_drift_guard(probe)
        await _assert_empty_backfill_retry(probe)
        await _seed_independent_source_evidence(probe)
        backfilled = await _assert_successful_backfill(probe)
        await _assert_archive_evidence(probe)
        await _assert_backfill_retry(probe, backfilled)
        await _assert_missing_index_guard(probe)
        await _assert_fresh_shadow_eligible(probe)
    finally:
        await db.status(f'DROP SCHEMA IF EXISTS "{probe_schema}" CASCADE;')
