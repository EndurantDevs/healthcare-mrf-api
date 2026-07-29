# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Canonical digest, loading, and generation-seal PostgreSQL proofs."""

from __future__ import annotations

from process import tin_npi_connector as connector
from tests.tin_npi_connector_pg_generation_support import (
    insert_evidence_rows,
    insert_generation_policies,
    insert_lookup_rows,
    set_build_token,
)
from tests.tin_npi_connector_pg_lifecycle_model import TOKEN_POLICY_ID
from tests.tin_npi_connector_postgres_support import expect_postgres_error


FUTURE_BUILD_INSERT = """
    INSERT INTO {quoted_schema}.tin_npi_connector_generation
    OVERRIDING SYSTEM VALUE
    SELECT (
        jsonb_populate_record(
            NULL::{quoted_schema}.tin_npi_connector_generation,
            to_jsonb(candidate)
            || jsonb_build_object(
                'generation_key',
                candidate.generation_key + 1000000000,
                'evidence_as_of',
                '2999-01-01T00:00:00.000000Z',
                'created_at',
                transaction_timestamp(),
                'build_lease_expires_at',
                transaction_timestamp() + interval '1 hour',
                'state',
                'building',
                'completed_at',
                NULL,
                'failed_at',
                NULL,
                'retired_at',
                NULL,
                'gc_after',
                NULL
            )
        )
    ).*
      FROM {quoted_schema}.tin_npi_connector_generation AS candidate
     WHERE candidate.generation_key = $1
"""


async def load_and_verify_complete_generation(scenario):
    await _verify_descriptor_functions(scenario)
    await scenario.register_model()
    await scenario.insert_build()
    await _reject_future_and_unscoped_loads(scenario)
    await set_build_token(scenario.connection, scenario.build_token)
    await insert_generation_policies(
        scenario.connection,
        scenario.quoted_schema,
        scenario.generation_key,
        (TOKEN_POLICY_ID,),
    )
    await _reject_invalid_lookup_payload(scenario)
    await insert_lookup_rows(
        scenario.connection,
        scenario.quoted_schema,
        scenario.generation_key,
        scenario.model.lookup_rows,
    )
    await _verify_sql_evidence_ids(scenario)
    await insert_evidence_rows(
        scenario.connection,
        scenario.quoted_schema,
        scenario.generation_key,
        scenario.model.evidence_rows,
    )
    await _verify_stored_digests(scenario)
    await scenario.seal_build()


async def _verify_descriptor_functions(scenario):
    sql_descriptor = await scenario.connection.fetchval(
        f"""
        SELECT {scenario.quoted_schema}.
               tin_npi_connector_token_policy_descriptor_sha256($1)
        """,
        TOKEN_POLICY_ID,
    )
    assert bytes(sql_descriptor).hex() == (
        "a0c06f5494f80663686be6861038a880" "4d9509d0fdc2d2c8cc56c259e53d761c"
    )
    sql_rule_digest = await scenario.connection.fetchval(
        f"""
        SELECT {scenario.quoted_schema}.
               tin_npi_connector_identifier_rule_sha256($1::jsonb)
        """,
        scenario.identifier_rule.descriptor_canonical_json,
    )
    assert bytes(sql_rule_digest) == bytes.fromhex(
        scenario.identifier_rule.descriptor_sha256
    )
    await _verify_identifier_policy_descriptor(scenario)


async def _verify_identifier_policy_descriptor(scenario):
    descriptor_json = scenario.identifier_policy.descriptor_canonical_json
    is_valid_descriptor = await scenario.connection.fetchval(
        f"""
        SELECT {scenario.quoted_schema}.
               tin_npi_connector_valid_identifier_policy($1, $2)
        """,
        descriptor_json,
        scenario.identifier_policy.policy_id,
    )
    assert is_valid_descriptor
    sql_policy_digest = await scenario.connection.fetchval(
        """
        SELECT sha256(
            convert_to(
                'healthporta.tin-npi.fhir-identifier-policy.v2',
                'UTF8'
            )
            || decode('00', 'hex')
            || convert_to($1, 'UTF8')
        )
        """,
        descriptor_json,
    )
    assert bytes(sql_policy_digest) == bytes.fromhex(
        scenario.identifier_policy.descriptor_sha256
    )


async def _reject_future_and_unscoped_loads(scenario):
    await expect_postgres_error(
        scenario.connection,
        "tin_npi_connector_generation_must_start_building",
        FUTURE_BUILD_INSERT.format(quoted_schema=scenario.quoted_schema),
        scenario.generation_key,
    )
    await expect_postgres_error(
        scenario.connection,
        "tin_npi_connector_generation_not_loadable",
        f"""
        INSERT INTO {scenario.quoted_schema}.tin_npi_connector_generation_policy (
            generation_key,
            token_policy_id
        ) VALUES ($1, $2)
        """,
        scenario.generation_key,
        TOKEN_POLICY_ID,
    )


async def _reject_invalid_lookup_payload(scenario):
    await expect_postgres_error(
        scenario.connection,
        "tin_npi_connector_lookup_payload_check",
        f"""
        INSERT INTO {scenario.quoted_schema}.tin_npi_connector_lookup (
            generation_key, token_policy_id, tin_id_128, tin_hmac_sha256,
            npis, evidence_count, source_bitmap, npi_source_bitmap_matrix,
            source_evidence_counts
        ) VALUES ($1, $2, $3, $4, $5, 2, $6, $7, $8)
        """,
        scenario.generation_key,
        TOKEN_POLICY_ID,
        b"\xee" * 16,
        b"\xee" * 32,
        [1000000004, 1234567893],
        b"\x01",
        b"\x01\x00",
        [2],
    )


async def _verify_sql_evidence_ids(scenario):
    for evidence_row in scenario.model.evidence_rows:
        sql_evidence_id = await scenario.connection.fetchval(
            f"""
            SELECT {scenario.quoted_schema}.
                   tin_npi_connector_evidence_id_sha256(
                       $1, $2, $3, $4, $5, $6, $7, $8, $9
                   )
            """,
            evidence_row.token.token_policy_id,
            evidence_row.token.tin_hmac_sha256,
            evidence_row.npi,
            evidence_row.relationship_class,
            evidence_row.source_record_hmac_sha256,
            evidence_row.source_record_identity_sha256,
            bytes.fromhex(evidence_row.source_record_payload_hash),
            bytes.fromhex(evidence_row.identifier_policy_sha256),
            bytes.fromhex(evidence_row.identifier_rule_sha256),
        )
        assert bytes(sql_evidence_id) == evidence_row.evidence_id
    assert scenario.model.evidence_rows[0].evidence_id.hex() == (
        "526e2237cf6f4e3c192672fffbeeda81" "a6f96f547a52f0dbb59b3417458c4359"
    )


async def _verify_stored_digests(scenario):
    candidate_count = await scenario.connection.fetchval(
        f"""
        SELECT COUNT(*)
          FROM {scenario.quoted_schema}.tin_npi_connector_lookup
         WHERE generation_key = $1
           AND token_policy_id = $2
           AND tin_id_128 = $3
        """,
        scenario.generation_key,
        TOKEN_POLICY_ID,
        scenario.token.tin_id_128,
    )
    assert candidate_count == 2
    sql_lookup_digest = await scenario.connection.fetchval(
        f"""
        SELECT {scenario.quoted_schema}.
               tin_npi_connector_lookup_set_sha256($1)
        """,
        scenario.generation_key,
    )
    assert bytes(sql_lookup_digest) == scenario.model.lookup_digest
    sql_evidence_digest = await scenario.connection.fetchval(
        f"""
        SELECT {scenario.quoted_schema}.
               tin_npi_connector_evidence_set_sha256($1, 0)
        """,
        scenario.generation_key,
    )
    assert bytes(sql_evidence_digest) == (
        connector.canonical_fhir_evidence_set_digest(scenario.model.evidence_rows)
    )
