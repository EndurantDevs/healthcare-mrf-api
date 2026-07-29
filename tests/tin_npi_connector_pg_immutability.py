# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Published generation immutability and controlled-retirement proofs."""

from __future__ import annotations

from tests.tin_npi_connector_pg_lifecycle_model import TOKEN_POLICY_ID
from tests.tin_npi_connector_postgres_support import expect_postgres_error


async def prove_generation_immutability(scenario, retired_generation_key):
    await _prove_controlled_retirement(scenario, retired_generation_key)
    await _reject_child_mutations(scenario)
    await _reject_late_child_insert(scenario)
    await _reject_truncate_and_current_retirement(scenario)


async def _prove_controlled_retirement(scenario, generation_key):
    await expect_postgres_error(
        scenario.connection,
        "tin_npi_connector_generation_retire_forbidden",
        f"""
        UPDATE {scenario.quoted_schema}.tin_npi_connector_generation
           SET state = 'retired',
               gc_after = transaction_timestamp() + interval '1 hour'
         WHERE generation_key = $1
        """,
        generation_key,
    )
    await expect_postgres_error(
        scenario.connection,
        "tin_npi_connector_generation_not_retirable",
        f"""
        SELECT {scenario.quoted_schema}.
               retire_tin_npi_connector_generation(
                   $1,
                   clock_timestamp() + interval '23 hours'
               )
        """,
        generation_key,
    )
    retired_key = await scenario.connection.fetchval(
        f"""
        SELECT {scenario.quoted_schema}.
               retire_tin_npi_connector_generation(
                   $1,
                   clock_timestamp() + interval '25 hours'
               )
        """,
        generation_key,
    )
    assert retired_key == generation_key
    await expect_postgres_error(
        scenario.connection,
        "tin_npi_connector_generation_delete_forbidden",
        f"""
        DELETE FROM {scenario.quoted_schema}.tin_npi_connector_generation
         WHERE generation_key = $1
        """,
        generation_key,
    )


async def _reject_child_mutations(scenario):
    await expect_postgres_error(
        scenario.connection,
        "tin_npi_connector_child_immutable",
        f"""
        DELETE FROM {scenario.quoted_schema}.tin_npi_connector_evidence
         WHERE generation_key = $1
           AND evidence_id = $2
        """,
        scenario.generation_key,
        scenario.model.evidence_rows[0].evidence_id,
    )
    await expect_postgres_error(
        scenario.connection,
        "tin_npi_connector_child_immutable",
        f"""
        UPDATE {scenario.quoted_schema}.tin_npi_connector_lookup
           SET evidence_count = 4
         WHERE generation_key = $1
        """,
        scenario.generation_key,
    )


async def _reject_late_child_insert(scenario):
    await expect_postgres_error(
        scenario.connection,
        "tin_npi_connector_generation_not_loadable",
        f"""
        INSERT INTO {scenario.quoted_schema}.tin_npi_connector_lookup (
            generation_key, token_policy_id, tin_id_128, tin_hmac_sha256,
            npis, evidence_count, source_bitmap, npi_source_bitmap_matrix,
            source_evidence_counts
        ) VALUES ($1, $2, $3, $4, $5, 1, $6, $7, $8)
        """,
        scenario.generation_key,
        TOKEN_POLICY_ID,
        b"\xcc" * 16,
        b"\xcc" * 32,
        [1000000004],
        b"\x01",
        b"\x01",
        [1],
    )


async def _reject_truncate_and_current_retirement(scenario):
    await expect_postgres_error(
        scenario.connection,
        "tin_npi_connector_truncate_forbidden",
        (
            f"TRUNCATE {scenario.quoted_schema}.tin_npi_connector_evidence, "
            f"{scenario.quoted_schema}.tin_npi_connector_lookup"
        ),
    )
    await expect_postgres_error(
        scenario.connection,
        "tin_npi_connector_generation_retire_forbidden",
        f"""
        UPDATE {scenario.quoted_schema}.tin_npi_connector_generation
           SET state = 'retired',
               gc_after = transaction_timestamp()
         WHERE generation_key = $1
        """,
        scenario.generation_key,
    )
