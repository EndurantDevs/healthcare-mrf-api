# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Read-only neutral evidence rendering for reviewed subset activation."""

from __future__ import annotations

import asyncio
import importlib
import json
from typing import Any, Mapping, Sequence

from process.provider_directory_fhir_subset_activation_contract import (
    STATE_SYNC_TIMEOUT_SECONDS,
    VERIFIED_STATUS,
    ReviewedSubsetActivationError,
    ReviewedSubsetActivationEvidence,
    _quoted_relation,
    _row_mapping,
    _text,
    reviewed_subset_source_contract_sha256,
)
from process import provider_directory_fhir_subset_activation_selection as selection
from process.provider_directory_fhir_subset_identity import (
    subset_source_endpoint_identity,
)


_PROOF_BEARING_STATUSES = (
    "verification_baseline",
    "verification_mismatch",
    "validated",
    "published",
    "superseded",
)


def _derived_activation_evidence(
    source_rows: Sequence[Mapping[str, Any]],
    dataset_rows: Sequence[Mapping[str, Any]],
    expected_source_id: str,
) -> ReviewedSubsetActivationEvidence:
    """Derive neutral evidence, then revalidate it through full selection."""

    if len(source_rows) != 1:
        raise ReviewedSubsetActivationError("evidence")
    source_by_field = dict(source_rows[0])
    baseline, candidate = selection._activation_roots(dataset_rows)
    try:
        importer = importlib.import_module("process.provider_directory_fhir")
        importer._twin_root_baseline_proof(baseline)
        importer._assert_matched_twin_root_dataset_proof(candidate)
        baseline_pair = importer._validated_parent_subset_completion_pair(
            baseline
        )
        candidate_pair = importer._validated_parent_subset_completion_pair(
            candidate
        )
        baseline_metadata = selection._metadata(baseline)
        candidate_metadata = selection._metadata(candidate)
        if (
            baseline_pair is None
            or candidate_pair is None
            or baseline_pair != candidate_pair
        ):
            raise ValueError("completion pair")
        scope_sha256 = candidate_metadata.get(
            "verification_source_scope_hash"
        )
        if scope_sha256 != baseline_metadata.get(
            "verification_source_scope_hash"
        ):
            raise ValueError("scope")
        evidence = ReviewedSubsetActivationEvidence(
            source_contract_sha256=(
                reviewed_subset_source_contract_sha256(source_by_field)
            ),
            cutoff=baseline_pair[0]["cutoff"],
            verification_source_scope_sha256=scope_sha256,
            completion_proof_sha256=baseline_pair[1],
        )
    except (AttributeError, KeyError, RuntimeError, TypeError, ValueError):
        raise ReviewedSubsetActivationError("evidence") from None
    selection.validated_reviewed_subset_activation_selection(
        source_rows=source_rows,
        dataset_rows=dataset_rows,
        expected_source_id=expected_source_id,
        evidence=evidence,
    )
    return evidence


async def _initial_evidence_identity(
    database: Any,
    expected_source_id: str,
) -> tuple[str, str]:
    """Resolve the fixed endpoint and campaign inside the active snapshot."""

    initial_source_rows = await database.all(
        f"""
        SELECT source.source_id, source.endpoint_id, source.metadata_json
          FROM {_quoted_relation('provider_directory_source')} AS source
         WHERE source.source_id = :source_id
         ORDER BY source.endpoint_id
         LIMIT 2;
        """,
        source_id=expected_source_id,
    )
    if len(initial_source_rows) != 1:
        raise ReviewedSubsetActivationError("evidence")
    initial_source = _row_mapping(initial_source_rows[0])
    source_metadata = initial_source.get("metadata_json")
    try:
        _, configured_endpoint_id = subset_source_endpoint_identity(
            initial_source
        )
    except ValueError:
        raise ReviewedSubsetActivationError("evidence") from None
    campaign_id = (
        _text(
            source_metadata.get("provider_directory_verification_campaign_id")
        )
        if isinstance(source_metadata, Mapping)
        else None
    )
    if campaign_id is None:
        raise ReviewedSubsetActivationError("evidence")
    return configured_endpoint_id, campaign_id


async def _evidence_source_rows(
    database: Any,
    expected_source_id: str,
    configured_endpoint_id: str,
) -> tuple[dict[str, Any], ...]:
    source_rows = await database.all(
        f"""
        SELECT source.*
          FROM {_quoted_relation('provider_directory_source')} AS source
         WHERE source.source_id = :source_id
            OR source.endpoint_id = :endpoint_id
            OR source.metadata_json::jsonb
                   ->> 'provider_directory_configured_endpoint_id'
                   = :endpoint_id
         ORDER BY source.source_id;
        """,
        source_id=expected_source_id,
        endpoint_id=configured_endpoint_id,
    )
    return tuple(_row_mapping(source_row) for source_row in source_rows)


async def _evidence_dataset_rows(
    database: Any,
    configured_endpoint_id: str,
    campaign_id: str,
) -> tuple[dict[str, Any], ...]:
    dataset_rows = await database.all(
        f"""
        SELECT dataset.*
          FROM {_quoted_relation('provider_directory_endpoint_dataset')}
               AS dataset
         WHERE dataset.endpoint_id = :endpoint_id
           AND dataset.completion_proof_required_version = 3
           AND dataset.status = ANY(:proof_statuses)
           AND dataset.publication_metadata_json::jsonb
                   ->> 'verification_campaign_id' = :campaign_id
         ORDER BY dataset.dataset_id;
        """,
        endpoint_id=configured_endpoint_id,
        campaign_id=campaign_id,
        proof_statuses=list(_PROOF_BEARING_STATUSES),
    )
    return tuple(_row_mapping(dataset_row) for dataset_row in dataset_rows)


async def _read_activation_evidence(
    database: Any,
    expected_source_id: str,
) -> ReviewedSubsetActivationEvidence:
    """Read one stable source/twin snapshot without taking write locks."""

    async with database.transaction():
        await database.status(
            "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY;"
        )
        configured_endpoint_id, campaign_id = await _initial_evidence_identity(
            database,
            expected_source_id,
        )
        source_rows = await _evidence_source_rows(
            database,
            expected_source_id,
            configured_endpoint_id,
        )
        dataset_rows = await _evidence_dataset_rows(
            database,
            configured_endpoint_id,
            campaign_id,
        )
        return _derived_activation_evidence(
            source_rows,
            dataset_rows,
            expected_source_id,
        )


async def reviewed_subset_activation_evidence(
    *,
    database: Any | None = None,
) -> ReviewedSubsetActivationEvidence:
    """Render the sole selector-free reviewed evidence from a read-only DB."""

    try:
        from process.provider_directory_fhir_manual_catalog import (
            reviewed_manual_census_source_id,
        )

        expected_source_id = reviewed_manual_census_source_id()
    except (OSError, RuntimeError, TypeError, ValueError):
        raise ReviewedSubsetActivationError("evidence") from None
    try:
        runtime_database = database
        if runtime_database is None:
            from db.connection import db

            runtime_database = db
        async with asyncio.timeout(STATE_SYNC_TIMEOUT_SECONDS):
            return await _read_activation_evidence(
                runtime_database,
                expected_source_id,
            )
    except (asyncio.CancelledError, TimeoutError):
        raise
    except ReviewedSubsetActivationError:
        raise
    except Exception:
        raise ReviewedSubsetActivationError("state") from None


def reviewed_subset_activation_verified_manifest_json(
    evidence: ReviewedSubsetActivationEvidence,
) -> str:
    """Render a complete neutral desired-state document for code review."""

    if type(evidence) is not ReviewedSubsetActivationEvidence:
        raise ReviewedSubsetActivationError("evidence")
    return json.dumps(
        {
            "schema_version": 1,
            "importer": "provider-directory-fhir",
            "operation": "reviewed-subset-source-state-sync",
            "desired_candidate_status": VERIFIED_STATUS,
            "evidence": evidence.evidence_document(),
        },
        sort_keys=True,
        separators=(",", ":"),
    )
