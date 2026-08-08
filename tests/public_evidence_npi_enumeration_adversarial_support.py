# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Coherently rehashed adversarial NPI-enumeration row families."""

from __future__ import annotations

from typing import Mapping

from public_evidence import evidence_record_contract as record_contract
from public_evidence import evidence_record_primitives as record_primitives
from public_evidence import record_persistence_candidate_contract as candidate_contract
from public_evidence import (
    record_persistence_candidate_primitives as candidate_primitives,
)
from tests.public_evidence_npi_enumeration_postgres_support import (
    TABLE_NAMES,
    _digest_bytes,
)


def _rehashed_record(
    persistence_candidate: (
        candidate_primitives.PublicEvidenceRecordPersistenceCandidate
    ),
    record_updates_by_field: Mapping[str, object],
) -> record_contract.PublicEvidenceRecord:
    provisional = persistence_candidate.record._replace(
        **record_updates_by_field, evidence_ref="", contract_sha256=""
    )
    payload = record_contract._record_payload(
        provisional.release,
        provisional.source_records,
        provisional.observed_at,
        provisional.effective_interval,
        provisional.record_type,
        provisional.evidence,
        provisional.authority_state,
    )
    return provisional._replace(
        evidence_ref=record_primitives._derived_ref(
            record_primitives.PUBLIC_EVIDENCE_RECORD_REF_PREFIX,
            "evidence_record",
            payload,
        ),
        contract_sha256=record_primitives._canonical_sha256(
            "evidence_record_contract", payload
        ),
    )


def _row_map(row: object) -> dict[str, object]:
    return {
        field_name: _digest_bytes(field_name, field_value)
        for field_name, field_value in row._asdict().items()
    }


def _finished_source_links(
    evidence_record: record_contract.PublicEvidenceRecord,
    evidence_ref: str,
    link_updates_by_field: Mapping[str, object],
) -> tuple[candidate_primitives.PublicEvidenceRecordSourceLinkRow, ...]:
    source_link_rows, _source_vector = candidate_contract._source_link_rows(
        evidence_record
    )
    return tuple(
        candidate_contract._finished_row(
            source_link._replace(
                **link_updates_by_field,
                evidence_ref=evidence_ref,
                row_sha256="",
            )
        )
        for source_link in source_link_rows
    )


def _source_link_vector_digest(
    persistence_candidate: (
        candidate_primitives.PublicEvidenceRecordPersistenceCandidate
    ),
    source_link_rows: tuple[
        candidate_primitives.PublicEvidenceRecordSourceLinkRow, ...
    ],
) -> str:
    vector_payload_by_field = {
        "ordering_contract_id": (
            persistence_candidate.common_row.source_link_ordering_contract_id
        ),
        "source_record_count": len(source_link_rows),
        "links": [
            {
                "source_record_ordinal": source_link.source_record_ordinal,
                "source_record_ref": source_link.source_record_ref,
                "row_sha256": source_link.row_sha256,
            }
            for source_link in source_link_rows
        ],
    }
    return candidate_contract._candidate_sha256(
        "source_link_vector", vector_payload_by_field
    )


def coherent_adversarial_rows(
    persistence_candidate: (
        candidate_primitives.PublicEvidenceRecordPersistenceCandidate
    ),
    *,
    record_updates_by_field: Mapping[str, object] | None = None,
    typed_updates_by_field: Mapping[str, object] | None = None,
    link_updates_by_field: Mapping[str, object] | None = None,
    common_updates_by_field: Mapping[str, object] | None = None,
    row_evidence_ref: str | None = None,
) -> dict[str, list[dict[str, object]]]:
    """Rebuild an internally coherent row family around one targeted mutation."""

    evidence_record = _rehashed_record(
        persistence_candidate, record_updates_by_field or {}
    )
    typed_row = candidate_contract._typed_row(evidence_record)
    evidence_ref = row_evidence_ref or evidence_record.evidence_ref
    typed_row = candidate_contract._finished_row(
        typed_row._replace(
            **(typed_updates_by_field or {}),
            evidence_ref=evidence_ref,
            row_sha256="",
        )
    )
    source_link_rows = _finished_source_links(
        evidence_record,
        evidence_ref,
        link_updates_by_field or {},
    )
    vector_digest = _source_link_vector_digest(persistence_candidate, source_link_rows)
    common_row = candidate_contract._common_row(
        evidence_record, vector_digest, typed_row.row_sha256
    )
    common_row = candidate_contract._finished_row(
        common_row._replace(
            **(common_updates_by_field or {}),
            evidence_ref=evidence_ref,
            row_sha256="",
        )
    )
    return {
        TABLE_NAMES[0]: [_row_map(common_row)],
        TABLE_NAMES[1]: [_row_map(source_link) for source_link in source_link_rows],
        TABLE_NAMES[2]: [_row_map(typed_row)],
    }


def coherent_link_owner_mismatch(
    persistence_candidate: (
        candidate_primitives.PublicEvidenceRecordPersistenceCandidate
    ),
) -> tuple[dict[str, list[dict[str, object]]], dict[str, object]]:
    """Build a fully rehashed link whose source root has a different owner."""

    release = persistence_candidate.record.release
    source_record = persistence_candidate.record.source_records[0]
    alternate_release_digest = "a5" * 32
    table_rows_by_name = coherent_adversarial_rows(
        persistence_candidate,
        link_updates_by_field={
            "source_release_contract_sha256": alternate_release_digest,
        },
    )
    source_root_by_field = {
        "source_record_ref": source_record.source_record_ref,
        "source_release_ref": release.source_release_ref,
        "source_release_contract_sha256": bytes.fromhex(alternate_release_digest),
        "source_kind": release.source_kind,
        "record_kind": source_record.record_kind,
        "identity_contract_id": source_record.identity_contract_id,
        "record_hmac_sha256": bytes.fromhex(source_record.record_hmac_sha256),
        "payload_sha256": bytes.fromhex(source_record.payload_sha256),
    }
    return table_rows_by_name, source_root_by_field
