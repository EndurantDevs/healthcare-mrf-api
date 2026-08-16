# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Candidate snapshot SQL shared by reviewed address alias policies."""

from __future__ import annotations

from process.ext.address_alias_sql import ADDRESS_ALIAS_CANDIDATE_TABLE, _relation


def candidate_metrics_sql(*, schema: str) -> str:
    """Return source-level candidate counts for one durable run."""
    candidates = _relation(schema, ADDRESS_ALIAS_CANDIDATE_TABLE)
    return f"""
        SELECT jsonb_build_object(
            'candidate_rows', count(*),
            'candidate_sources', count(DISTINCT source_address_key),
            'eligible', count(DISTINCT source_address_key)
                FILTER (WHERE decision = 'eligible'),
            'ambiguous', count(DISTINCT source_address_key)
                FILTER (WHERE decision = 'ambiguous'),
            'insufficient_provenance', count(DISTINCT source_address_key)
                FILTER (WHERE decision = 'insufficient_provenance')
        )
        FROM {candidates}
        WHERE run_id = CAST(:run_id AS uuid);
    """

def candidate_rows_sql(*, schema: str) -> str:
    """Stream deterministic candidate rows for digesting or review."""
    candidates = _relation(schema, ADDRESS_ALIAS_CANDIDATE_TABLE)
    return f"""
        SELECT
            source_address_key::text AS source_address_key,
            source_identity_key,
            target_address_key::text AS target_address_key,
            target_identity_key,
            candidate_count,
            target_strict_source_bits,
            target_strict_source_count,
            decision
        FROM {candidates}
        WHERE run_id = CAST(:run_id AS uuid)
        ORDER BY source_address_key, target_address_key;
    """


def evidence_candidate_rows_sql(*, schema: str) -> str:
    """Stream evidence-bearing candidate rows for review and digesting."""
    candidates = _relation(schema, ADDRESS_ALIAS_CANDIDATE_TABLE)
    return f"""
        SELECT
            source_address_key::text AS source_address_key,
            source_identity_key,
            target_address_key::text AS target_address_key,
            target_identity_key,
            candidate_count,
            target_strict_source_bits,
            target_strict_source_count,
            decision,
            match_rule,
            match_classification,
            evidence_npi,
            evidence_npi_count
        FROM {candidates}
        WHERE run_id = CAST(:run_id AS uuid)
        ORDER BY source_address_key, target_address_key;
    """
