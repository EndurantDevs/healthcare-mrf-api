# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Evidence alias review and database trust-boundary guards."""

import pytest

from process.address_numeric_grid_alias import run_numeric_grid_alias
from tests.address_evidence_alias_test_support import _insert_visible_address
from tests.test_address_numeric_grid_alias_runtime_db import (
    _connect_runtime_database,
    _create_alias_probe_schema,
    _insert_archive_address,
    _requires_test_database,
    db,
)


_ALIAS_KIND = "evidence_gated_address_match_v1"
_EVIDENCE_RUN = "00000000-0000-0000-0000-000000000021"
_NUMERIC_RUN = "00000000-0000-0000-0000-000000000022"


async def _seed_stale_shadow(schema):
    source_key = await _insert_archive_address(
        schema,
        first_line="4007 Clarksville Pike 301",
        second_line=None,
        strict_source_bits=1,
    )
    target_key = await _insert_archive_address(
        schema,
        first_line="4007 Clarksville Pike",
        second_line="Suite 301",
        strict_source_bits=6,
    )
    for role, address_key in (("source", source_key), ("target", target_key)):
        await _insert_visible_address(
            schema,
            location_key=f"stale-{role}",
            npi=1234567893,
            address_key=address_key,
        )
    shadow = await run_numeric_grid_alias(
        mode="shadow",
        schema=schema,
        alias_kind=_ALIAS_KIND,
    )
    assert shadow.eligible == 1
    return source_key, target_key, shadow


async def _assert_changed_evidence_rejected(schema, source_key, target_key, shadow):
    with pytest.raises(Exception, match="candidate evidence is immutable"):
        await db.status(
            f"""
            UPDATE {schema}.address_alias_candidate_v1
            SET evidence_npi = 1234567885
            WHERE run_id = CAST(:run_id AS uuid);
            """,
            run_id=shadow.run_id,
        )
    await db.status(
        f"""
        UPDATE {schema}.entity_address_unified
        SET npi = 1234567885
        WHERE address_key = ANY(CAST(:address_keys AS uuid[]));
        """,
        address_keys=[source_key, target_key],
    )
    with pytest.raises(RuntimeError, match="candidate set changed after review"):
        await run_numeric_grid_alias(
            mode="apply",
            schema=schema,
            alias_kind=_ALIAS_KIND,
            alias_run_id=shadow.run_id,
            expected_candidate_sha256=shadow.candidate_digest,
            reviewed_by="ci-reviewer",
        )
    assert await db.scalar(
        f"SELECT count(*) FROM {schema}.address_alias_v1 "
        "WHERE revoked_at IS NULL;"
    ) == 0


async def _assert_stale_generation_rejected(schema):
    await db.status(
        f"UPDATE {schema}.entity_address_unified "
        "SET base_address_version = 'address_archive_v2:v2+fmt-v2+alias-v1:g9';"
    )
    with pytest.raises(RuntimeError, match="current full entity-address-unified"):
        await run_numeric_grid_alias(
            mode="shadow",
            schema=schema,
            alias_kind=_ALIAS_KIND,
        )


@pytest.mark.asyncio(loop_scope="session")
async def test_evidence_alias_rejects_pre_upgrade_unified_artifact():
    """The schema upgrade invalidates unified rows built by the old policy."""
    _requires_test_database()
    schema = "address_evidence_alias_upgrade_fence_probe"
    connection = await _connect_runtime_database()
    try:
        await _create_alias_probe_schema(connection, schema, "evidence_upgrade_fence")
        await db.status(
            f"INSERT INTO {schema}.entity_address_unified "
            "(location_key, npi, type, base_address_version) VALUES "
            "('pre-upgrade', 1234567893, 'practice', "
            "'address_archive_v2:v2+fmt-v2+alias-v1:g0');"
        )
        assert await db.scalar(
            f"SELECT generation FROM {schema}.address_alias_state_v1 "
            "WHERE singleton = true;"
        ) == 1
        with pytest.raises(RuntimeError, match="current full entity-address-unified"):
            await run_numeric_grid_alias(
                mode="shadow",
                schema=schema,
                alias_kind=_ALIAS_KIND,
            )
    finally:
        await connection.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE;')
        await connection.close()


@pytest.mark.asyncio(loop_scope="session")
async def test_evidence_alias_apply_rejects_changed_review_evidence():
    """The sealed digest binds the NPI witness and serving generation."""
    _requires_test_database()
    schema = "address_evidence_alias_stale_probe"
    connection = await _connect_runtime_database()
    try:
        await _create_alias_probe_schema(connection, schema, "evidence_stale")
        source_key, target_key, shadow = await _seed_stale_shadow(schema)
        await _assert_changed_evidence_rejected(
            schema,
            source_key,
            target_key,
            shadow,
        )
        await _assert_stale_generation_rejected(schema)
    finally:
        await connection.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE;')
        await connection.close()


def _candidate_insert_sql(schema):
    return f"""
        INSERT INTO {schema}.address_alias_candidate_v1 (
            run_id, source_address_key, source_identity_key,
            target_address_key, target_identity_key, candidate_count,
            target_strict_source_bits, target_strict_source_count,
            decision, review_status, match_rule, match_classification,
            evidence_npi, evidence_npi_count
        ) VALUES (
            CAST(:run_id AS uuid), CAST(:source_key AS uuid),
            :source_identity, CAST(:target_key AS uuid), :target_identity,
            1, 6, 2, 'eligible', 'pending', :match_rule,
            :match_classification, :evidence_npi, :evidence_npi_count
        );
    """


async def _seed_candidate_guard(schema):
    source_key = await _insert_archive_address(
        schema,
        first_line="10 Guard Road",
        second_line=None,
        strict_source_bits=1,
    )
    target_key = await _insert_archive_address(
        schema,
        first_line="10 Guard Road",
        second_line="Suite 1",
        strict_source_bits=6,
    )
    identity_records = await db.all(
        f"""
        SELECT address_key::text, identity_key
        FROM {schema}.address_archive_v2
        WHERE address_key = ANY(CAST(:address_keys AS uuid[]));
        """,
        address_keys=[source_key, target_key],
    )
    identity_by_key = {
        str(identity_record.address_key): identity_record.identity_key
        for identity_record in identity_records
    }
    await db.status(
        f"""
        INSERT INTO {schema}.address_alias_run_v1
            (run_id, alias_kind, ruleset_version, mode, status)
        VALUES
            (CAST(:evidence_run AS uuid), '{_ALIAS_KIND}', 1, 'shadow', 'running'),
            (CAST(:numeric_run AS uuid), 'numeric_grid_direction_v1',
             1, 'shadow', 'running');
        """,
        evidence_run=_EVIDENCE_RUN,
        numeric_run=_NUMERIC_RUN,
    )
    return {
        "source_key": source_key,
        "source_identity": identity_by_key[source_key],
        "target_key": target_key,
        "target_identity": identity_by_key[target_key],
    }


async def _insert_guard_candidate(schema, candidate_by_field):
    await db.status(_candidate_insert_sql(schema), **candidate_by_field)


async def _assert_evidence_candidate_guard(schema, address_by_field):
    candidate_by_field = {
        **address_by_field,
        "run_id": _EVIDENCE_RUN,
        "match_rule": "candidate_confirmed_bare_unit",
        "match_classification": "exact",
        "evidence_npi": 1234567893,
        "evidence_npi_count": 1,
    }
    await _insert_guard_candidate(schema, candidate_by_field)
    with pytest.raises(Exception, match="require exact match evidence"):
        await db.status(
            f"""
            UPDATE {schema}.address_alias_candidate_v1
            SET match_rule = NULL, match_classification = NULL,
                evidence_npi = NULL, evidence_npi_count = NULL
            WHERE run_id = CAST(:run_id AS uuid);
            """,
            run_id=_EVIDENCE_RUN,
        )


async def _assert_numeric_candidate_guard(schema, address_by_field):
    candidate_by_field = {
        **address_by_field,
        "run_id": _NUMERIC_RUN,
        "match_rule": None,
        "match_classification": None,
        "evidence_npi": None,
        "evidence_npi_count": None,
    }
    await _insert_guard_candidate(schema, candidate_by_field)
    with pytest.raises(Exception, match="cannot carry address match evidence"):
        await db.status(
            f"""
            UPDATE {schema}.address_alias_candidate_v1
            SET match_rule = 'candidate_confirmed_bare_unit',
                match_classification = 'exact',
                evidence_npi = 1234567893, evidence_npi_count = 1
            WHERE run_id = CAST(:run_id AS uuid);
            """,
            run_id=_NUMERIC_RUN,
        )


async def _assert_invalid_evidence_inserts(schema, address_by_field):
    reversed_address_by_field = {
        "source_key": address_by_field["target_key"],
        "source_identity": address_by_field["target_identity"],
        "target_key": address_by_field["source_key"],
        "target_identity": address_by_field["source_identity"],
    }
    invalid_evidence_by_field = {
        **reversed_address_by_field,
        "run_id": _EVIDENCE_RUN,
        "match_rule": "candidate_confirmed_bare_unit",
        "match_classification": None,
        "evidence_npi": None,
        "evidence_npi_count": None,
    }
    with pytest.raises(Exception, match="require exact match evidence"):
        await _insert_guard_candidate(schema, invalid_evidence_by_field)
    invalid_evidence_by_field.update(
        match_classification="exact",
        evidence_npi=1234567890,
        evidence_npi_count=1,
    )
    with pytest.raises(Exception, match="match_evidence_ck"):
        await _insert_guard_candidate(schema, invalid_evidence_by_field)


@pytest.mark.asyncio(loop_scope="session")
async def test_candidate_guard_binds_evidence_to_the_parent_policy():
    """Running candidates cannot cross the closed policy/evidence boundary."""
    _requires_test_database()
    schema = "address_evidence_alias_guard_probe"
    connection = await _connect_runtime_database()
    try:
        await _create_alias_probe_schema(connection, schema, "evidence_guard")
        address_by_field = await _seed_candidate_guard(schema)
        await _assert_evidence_candidate_guard(schema, address_by_field)
        await _assert_numeric_candidate_guard(schema, address_by_field)
        await _assert_invalid_evidence_inserts(schema, address_by_field)
    finally:
        await connection.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE;')
        await connection.close()
