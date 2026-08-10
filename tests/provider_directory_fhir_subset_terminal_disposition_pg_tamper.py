# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Deep proof-tamper assertions for the mixed terminal disposition."""

from __future__ import annotations

from copy import deepcopy
import json

from process.provider_directory_fhir_subset_terminal_disposition_contract import (
    canonical_evidence_sha256,
)
from process.provider_directory_fhir_subset_terminal_disposition_evidence import (
    expected_subset_coverage,
)
from tests.provider_directory_fhir_subset_terminal_disposition_pg_lifecycle import (
    _assert_postgres_error,
    _selected_terminal_evidence,
    _write_terminal_state,
)


def _mutated_resource_evidence(
    selection,
    checkpoint_records,
    resource_type: str,
    mutate_proof,
) -> tuple[dict, dict, dict]:
    """Build synchronized candidate, diagnostic, and checkpoint tampering."""
    candidate_metadata = deepcopy(selection.observed_candidate_metadata)
    candidate_checkpoint = next(
        checkpoint
        for checkpoint in checkpoint_records
        if checkpoint["resource_type"] == resource_type
    )
    checkpoint_proof = deepcopy(candidate_checkpoint["completeness_json"])
    mutate_proof(checkpoint_proof)
    diagnostic = candidate_metadata["resource_diagnostics"][resource_type]
    diagnostic_proof_by_field = {
        field_name: field_value
        for field_name, field_value in checkpoint_proof.items()
        if field_name != "continuation_hop_sha256"
    }
    diagnostic["server_issued_subset_completeness"] = diagnostic_proof_by_field
    diagnostic["server_issued_subset_coverage"] = expected_subset_coverage(
        diagnostic_proof_by_field
    )
    diagnostics_by_type = candidate_metadata["resource_diagnostics"]
    candidate_metadata["completion_proof_v1"]["resource_diagnostics"] = deepcopy(
        diagnostics_by_type
    )
    return candidate_metadata, checkpoint_proof, diagnostic


def _tampered_marker(
    selection,
    candidate_metadata: dict,
    checkpoint_proof: dict,
    diagnostic: dict,
    source_import: dict,
    resource_type: str,
) -> dict:
    """Rehash every affected marker commitment after one proof mutation."""
    marker = deepcopy(selection.marker_by_field)
    resource_marker = marker["resource_dispositions"][resource_type]
    resource_marker["diagnostic_sha256"] = canonical_evidence_sha256(diagnostic)
    resource_marker["checkpoint_proof_sha256"] = canonical_evidence_sha256(
        checkpoint_proof
    )
    resource_marker["advertised_pre"] = checkpoint_proof.get("pre_count")
    diagnostics_by_type = candidate_metadata["resource_diagnostics"]
    marker["source_diagnostics_sha256"] = canonical_evidence_sha256(
        diagnostics_by_type
    )
    marker["source_import_sha256"] = canonical_evidence_sha256(source_import)
    marker["candidate_metadata_sha256"] = canonical_evidence_sha256(
        candidate_metadata
    )
    return marker


async def _persist_proof_tamper(
    scenario,
    selection,
    candidate_metadata: dict,
    checkpoint_proof: dict,
    source_metadata: dict,
    resource_type: str,
) -> None:
    """Persist synchronized pre-terminal evidence inside a rollback scope."""
    await scenario.connection.execute(
        f"""
        UPDATE {scenario.quoted_schema}.provider_directory_source
           SET metadata_json = $1::jsonb
         WHERE source_id = 'source-a'
        """,
        json.dumps(source_metadata),
    )
    await scenario.connection.execute(
        f"""
        UPDATE {scenario.quoted_schema}.provider_directory_endpoint_dataset
           SET publication_metadata_json = $1::jsonb
         WHERE dataset_id = $2
        """,
        json.dumps(candidate_metadata),
        selection.dataset_id,
    )
    await scenario.connection.execute(
        f"""
        UPDATE {scenario.quoted_schema}.provider_directory_pagination_checkpoint
           SET completeness_json = $1::jsonb
         WHERE dataset_id = $2 AND resource_type = $3
        """,
        json.dumps(checkpoint_proof),
        selection.dataset_id,
        resource_type,
    )


async def _write_proof_tamper(
    scenario,
    migration,
    selection,
    checkpoint_records,
    resource_type: str,
    mutate_proof,
) -> None:
    """Propagate one proof mutation so only its semantic guard can reject it."""
    candidate_metadata, checkpoint_proof, diagnostic = (
        _mutated_resource_evidence(
            selection,
            checkpoint_records,
            resource_type,
            mutate_proof,
        )
    )
    source_metadata_text = await scenario.connection.fetchval(
        f"""
        SELECT metadata_json::text
          FROM {scenario.quoted_schema}.provider_directory_source
         WHERE source_id = 'source-a'
        """
    )
    source_metadata = json.loads(source_metadata_text)
    source_import = source_metadata["last_resource_import"]
    source_import["resources"] = deepcopy(
        candidate_metadata["resource_diagnostics"]
    )
    marker = _tampered_marker(
        selection,
        candidate_metadata,
        checkpoint_proof,
        diagnostic,
        source_import,
        resource_type,
    )
    await _persist_proof_tamper(
        scenario,
        selection,
        candidate_metadata,
        checkpoint_proof,
        source_metadata,
        resource_type,
    )
    await _write_terminal_state(scenario, migration, selection, marker)


async def assert_retryable_precount_rejected(
    scenario,
    migration,
    database,
) -> None:
    """Reject retryable retained rows that exceed the advertised pre-count."""
    selection, checkpoint_records = await _selected_terminal_evidence(database)
    await _assert_postgres_error(
        scenario.connection,
        "provider_directory_subset_terminal_disposition_checkpoint_invalid",
        lambda: _write_proof_tamper(
            scenario,
            migration,
            selection,
            checkpoint_records,
            "HealthcareService",
            lambda proof: proof.__setitem__("pre_count", 0),
        ),
    )


async def assert_shared_proof_identity_rejected(
    scenario,
    migration,
    database,
) -> None:
    """Reject one resource whose cutoff differs from the other six proofs."""
    selection, checkpoint_records = await _selected_terminal_evidence(database)
    await _assert_postgres_error(
        scenario.connection,
        "provider_directory_subset_terminal_disposition_checkpoint_invalid",
        lambda: _write_proof_tamper(
            scenario,
            migration,
            selection,
            checkpoint_records,
            "InsurancePlan",
            lambda proof: proof.__setitem__(
                "cutoff", "2026-08-09T12:00:01.000000Z"
            ),
        ),
    )


async def assert_terminal_geometry_tamper_rejected(
    scenario,
    migration,
    database,
) -> None:
    """Reject a fully rehashed terminal geometry with an invalid version."""
    selection, checkpoint_records = await _selected_terminal_evidence(database)

    def replace_geometry_version(proof: dict) -> None:
        proof["terminal_page_geometry"]["version"] = 999

    await _assert_postgres_error(
        scenario.connection,
        "provider_directory_subset_terminal_disposition_checkpoint_invalid",
        lambda: _write_proof_tamper(
            scenario,
            migration,
            selection,
            checkpoint_records,
            "Organization",
            replace_geometry_version,
        ),
    )


async def assert_completion_envelope_rejected(
    scenario,
    migration,
    database,
) -> None:
    """Reject an otherwise valid completion copy with an extra field."""
    selection, _checkpoint_records = await _selected_terminal_evidence(database)
    candidate_metadata = deepcopy(selection.observed_candidate_metadata)
    candidate_metadata["completion_proof_v1"]["unexpected"] = False
    marker = deepcopy(selection.marker_by_field)
    marker["candidate_metadata_sha256"] = canonical_evidence_sha256(
        candidate_metadata
    )

    async def write_extra_completion_field() -> None:
        await scenario.connection.execute(
            f"""
            UPDATE {scenario.quoted_schema}.provider_directory_endpoint_dataset
               SET publication_metadata_json = $1::jsonb
             WHERE dataset_id = $2
            """,
            json.dumps(candidate_metadata),
            selection.dataset_id,
        )
        await _write_terminal_state(scenario, migration, selection, marker)

    await _assert_postgres_error(
        scenario.connection,
        "provider_directory_subset_terminal_disposition_transition_invalid",
        write_extra_completion_field,
    )


async def _write_source_import_tamper(
    scenario,
    migration,
    selection,
    mutate_source_import,
) -> None:
    """Rehash and write one malformed current source-import envelope."""
    source_metadata_text = await scenario.connection.fetchval(
        f"""
        SELECT metadata_json::text
          FROM {scenario.quoted_schema}.provider_directory_source
         WHERE source_id = 'source-a'
        """
    )
    source_metadata = json.loads(source_metadata_text)
    source_import = source_metadata["last_resource_import"]
    mutate_source_import(source_import)
    marker = deepcopy(selection.marker_by_field)
    marker["source_import_sha256"] = canonical_evidence_sha256(source_import)
    await scenario.connection.execute(
        f"""
        UPDATE {scenario.quoted_schema}.provider_directory_source
           SET metadata_json = $1::jsonb
         WHERE source_id = 'source-a'
        """,
        json.dumps(source_metadata),
    )
    await _write_terminal_state(scenario, migration, selection, marker)


async def assert_source_import_envelope_rejected(
    scenario,
    migration,
    database,
) -> None:
    """Reject extra source-import fields and non-producer timestamps."""
    selection, _checkpoint_records = await _selected_terminal_evidence(database)

    def add_source_import_field(source_import: dict) -> None:
        source_import["unexpected"] = False

    def replace_observed_at(source_import: dict) -> None:
        source_import["observed_at"] = "2026-08-09T12:30:00.000000Z"

    def replace_observed_at_with_invalid_date(source_import: dict) -> None:
        source_import["observed_at"] = "2026-02-30T12:30:00Z"

    for mutate_source_import in (
        add_source_import_field,
        replace_observed_at,
        replace_observed_at_with_invalid_date,
    ):
        await _assert_postgres_error(
            scenario.connection,
            "provider_directory_subset_terminal_disposition_transition_invalid",
            lambda mutation=mutate_source_import: _write_source_import_tamper(
                scenario,
                migration,
                selection,
                mutation,
            ),
        )


__all__ = (
    "assert_completion_envelope_rejected",
    "assert_retryable_precount_rejected",
    "assert_shared_proof_identity_rejected",
    "assert_source_import_envelope_rejected",
    "assert_terminal_geometry_tamper_rejected",
)
