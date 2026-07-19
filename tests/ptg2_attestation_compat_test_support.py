# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Shared canonical evidence for PostgreSQL attestation compatibility tests."""

from __future__ import annotations

import datetime
import hashlib

from process.ptg_parts import ptg2_candidate_attestation
from process.ptg_parts.ptg2_provider_quarantine import (
    provider_identifier_quarantine_payload,
)


def quoted_identifier(identifier: str) -> str:
    """Quote one generated PostgreSQL identifier."""

    return '"' + identifier.replace('"', '""') + '"'


def writer_source_witness_by_field(
    identity_by_field: dict[str, object],
) -> dict[str, object]:
    """Return the full canonical V4 source-witness fixture."""

    return {
        "contract": "ptg2_v3_source_witness_payload_v5",
        "format_version": 5,
        "selection_method": "bottom_k_independent_occurrence_provider_cohorts_v3",
        "population_semantics": "queryable_emitted_price_provider_occurrence_v1",
        "unqueryable_rate_policy": (
            "count_but_exclude_from_npi_api_challenges_v1"
        ),
        "source_count": 1,
        "source_set_digest": identity_by_field["source_set_digest"].hex(),
        "occurrence_target": 10_000,
        "total_target": 11_000,
        "provider_quota": 1_000,
        "queryable_occurrence_population_count": 2,
        "provider_population_count": 1,
        "emitted_rate_row_count": 2,
        "unqueryable_rate_row_count": 0,
        "occurrence_witness_count": 2,
        "provider_witness_count": 1,
        "record_count": 3,
        "evidence_dictionary_count": 2,
        "evidence_dictionary_raw_bytes": 10_000,
        "evidence_dictionary_stored_bytes": 5_000,
        "sample_digest": "e" * 64,
        "payload_sha256": identity_by_field["source_witness_digest"].hex(),
        "payload_bytes": 123_456,
        "compression": "per_record_zlib_shared_evidence_dictionary_v1",
    }


def writer_audit_sample_by_field(
    identity_by_field: dict[str, object],
) -> dict[str, object]:
    """Return the canonical public audit-sample fixture."""

    return {
        "contract": "persisted_served_occurrence_sample_v2",
        "format_version": 2,
        "method": "publish_time_stratified_v1",
        "sample_count": 1,
        "maximum_rows": 2_560,
        "sample_digest": identity_by_field["audit_sample_digest"].hex(),
        "source_count": 1,
        "occurrence_identity": "sha256_candidate_ordinal_source_key_v2",
        "complete_population": False,
        "serving_multiplicity_semantics": "source_multiset_v1",
    }


def writer_identity_by_field() -> dict[str, object]:
    """Return the immutable candidate identity used by writer proofs."""

    identity_by_field = {
        "snapshot_key": 17,
        "source_key": "source-a",
        "plan_id": "12-3456789",
        "plan_market_type": "group",
        "coverage_scope_id": b"c" * 32,
        "source_set_digest": b"s" * 32,
        "ordered_source_ordinal_digest": "d" * 64,
        "audit_sample_digest": b"a" * 32,
        "source_witness_digest": b"w" * 32,
        "provider_identifier_quarantine": (
            provider_identifier_quarantine_payload({})
        ),
    }
    identity_by_field["source_witness_manifest"] = (
        writer_source_witness_by_field(identity_by_field)
    )
    identity_by_field["audit_sample_public"] = (
        writer_audit_sample_by_field(identity_by_field)
    )
    identity_by_field["provider_identifier_quarantine_evidence"] = {
        "contract": "ptg2_provider_identifier_quarantine_v1",
        "distinct_value_count": 0,
        "occurrence_count": 0,
        "sha256": identity_by_field["provider_identifier_quarantine"][
            "sha256"
        ],
    }
    return identity_by_field


def writer_report_by_field(schema_version: int) -> dict[str, object]:
    """Return a canonical rolling-deploy report for one schema version."""

    identity_by_field = writer_identity_by_field()
    report_by_field = {
        "schema_version": schema_version,
        "source": {
            "provider_identifier_quarantine": identity_by_field[
                (
                    "provider_identifier_quarantine_evidence"
                    if schema_version == 4
                    else "provider_identifier_quarantine"
                )
            ],
            "witness": identity_by_field["source_witness_manifest"],
        },
    }
    if schema_version == 4:
        report_by_field["batch"] = {
            "ordered_source_ordinal_digest": identity_by_field[
                "ordered_source_ordinal_digest"
            ]
        }
        report_by_field["api_audit_sample"] = {
            **identity_by_field["audit_sample_public"],
            "sample_digest_validated": True,
            "source_set_validated": True,
        }
    return report_by_field


def writer_evidence_by_field(
    report_by_field: dict[str, object],
    *,
    completed_at: datetime.datetime,
) -> dict[str, object]:
    """Return normalized writer evidence for the selected report version."""

    identity_by_field = writer_identity_by_field()
    is_v4 = report_by_field["schema_version"] == 4
    report_bytes = ptg2_candidate_attestation._canonical_report_bytes(
        report_by_field
    )
    evidence_by_field = {
        "contract": (
            ptg2_candidate_attestation.PTG2_CANDIDATE_ATTESTATION_CONTRACT_V4
            if is_v4
            else ptg2_candidate_attestation.PTG2_CANDIDATE_ATTESTATION_CONTRACT_V3
        ),
        "tool_name": (
            ptg2_candidate_attestation.PTG2_BATCH_AUDIT_TOOL
            if is_v4
            else ptg2_candidate_attestation.PTG2_FAST_AUDIT_TOOL
        ),
        "tool_version": "4.0.0" if is_v4 else "3.0.0",
        "report_digest": hashlib.sha256(report_bytes).digest(),
        "report_json": report_bytes.decode("utf-8"),
        "completed_at": completed_at,
        "audit_sample_digest": identity_by_field["audit_sample_digest"],
        "source_set_digest": identity_by_field["source_set_digest"],
        "source_witness_digest": identity_by_field["source_witness_digest"],
        "provider_identifier_quarantine": identity_by_field[
            "provider_identifier_quarantine"
        ],
        "checks": {},
    }
    if is_v4:
        evidence_by_field["ordered_source_ordinal_digest"] = identity_by_field[
            "ordered_source_ordinal_digest"
        ]
        evidence_by_field["batch_api_actual_http_requests"] = 1
        evidence_by_field["source_witness_manifest"] = identity_by_field[
            "source_witness_manifest"
        ]
        evidence_by_field["audit_sample_public"] = identity_by_field[
            "audit_sample_public"
        ]
        evidence_by_field[
            "provider_identifier_quarantine_evidence"
        ] = identity_by_field[
            "provider_identifier_quarantine_evidence"
        ]
    else:
        evidence_by_field["standard_api_actual_http_requests"] = 2
    return evidence_by_field


__all__ = [
    "quoted_identifier",
    "writer_evidence_by_field",
    "writer_identity_by_field",
    "writer_report_by_field",
]
