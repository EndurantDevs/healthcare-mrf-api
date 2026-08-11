# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Exact proof checks for one direct-v5 HTTP-410 disposition."""

from __future__ import annotations

import re
from typing import Any, Mapping, Sequence

from process.provider_directory_fhir_census_execution import (
    CURRENT_VERSION_CENSUS_BLOCKED_ERROR,
)
from process.provider_directory_fhir_census_page_geometry import (
    validate_current_version_census_checkpoint_geometry,
)
from process.provider_directory_fhir_subset_execution import (
    has_valid_reviewed_subset_counts,
    has_valid_subset_completed_fields,
    reviewed_subset_completion_constraints,
)
from process.provider_directory_fhir_subset_terminal_disposition_contract import (
    ReviewedSubsetTerminalDispositionError,
)
from process.provider_directory_fhir_subset_terminal_disposition_evidence import (
    terminal_resource_marker,
    validated_terminal_resource_proof,
)
from process.provider_directory_fhir_subset_terminal_disposition_profile import (
    DIRECT_V5_CAMPAIGN_ID,
    DIRECT_V5_CANONICALIZATION_VERSION,
    DIRECT_V5_COMPLETION_SCOPES,
    DIRECT_V5_CUTOFF,
    DIRECT_V5_DISPOSITION_BY_RESOURCE_TYPE,
    DIRECT_V5_PAGE_COUNT,
    DIRECT_V5_PROOF_CONTRACT_VERSION,
    DIRECT_V5_SEMANTICS,
    DIRECT_V5_STRATEGY_VERSION,
    DIRECT_V5_TRAVERSAL_VERSION,
    EXPECTED_RESOURCE_TYPES,
    TERMINAL_HTTP_410_DISPOSITION,
    VERIFIED_COMPLETE_DISPOSITION,
)
from process.provider_directory_fhir_subset_terminal_disposition_shapes import (
    validate_terminal_sequence,
)
from process.provider_directory_fhir_subset_terminal_disposition_util import (
    clean_text,
    json_object,
)


_SHA256 = re.compile(r"^[0-9a-f]{64}$")


def _proof_identity(
    proof: Mapping[str, Any],
    resource_type: str,
) -> tuple[Any, ...]:
    identity = (
        proof.get("cutoff"),
        proof.get("contract_identity"),
        proof.get("page_count"),
        proof.get("campaign_id"),
    )
    if (
        proof.get("contract_version") != DIRECT_V5_PROOF_CONTRACT_VERSION
        or proof.get("strategy_version") != DIRECT_V5_STRATEGY_VERSION
        or proof.get("semantics") != DIRECT_V5_SEMANTICS
        or proof.get("traversal_version") != DIRECT_V5_TRAVERSAL_VERSION
        or proof.get("canonicalization_version")
        != DIRECT_V5_CANONICALIZATION_VERSION
        or proof.get("completion_scopes") != list(DIRECT_V5_COMPLETION_SCOPES)
        or proof.get("resource_type") != resource_type
        or identity[0] != DIRECT_V5_CUTOFF
        or clean_text(identity[1]) is None
        or _SHA256.fullmatch(identity[1]) is None
        or identity[2] != DIRECT_V5_PAGE_COUNT
        or identity[3] != DIRECT_V5_CAMPAIGN_ID
        or type(proof.get("pre_count")) is not int
        or proof["pre_count"] < 0
    ):
        raise ReviewedSubsetTerminalDispositionError("evidence")
    return identity


def _completed_counts(proof: Mapping[str, Any]) -> dict[str, Any]:
    return {
        field_name: proof.get(field_name)
        for field_name in (
            "pre_count",
            "post_count",
            "processed_rows",
            "unique_candidate_rows",
        )
    }


def _verified_complete_disposition(
    diagnostic: Mapping[str, Any],
    checkpoint: Mapping[str, Any],
    proof: Mapping[str, Any],
) -> str:
    count_by_name = _completed_counts(proof)
    try:
        maximum_decrease, requires_terminal_window = (
            reviewed_subset_completion_constraints(proof)
        )
    except ValueError:
        raise ReviewedSubsetTerminalDispositionError("evidence") from None
    if (
        not requires_terminal_window
        or not has_valid_reviewed_subset_counts(
            count_by_name,
            maximum_decrease,
        )
        or not has_valid_subset_completed_fields(
            proof,
            count_by_name,
            DIRECT_V5_PAGE_COUNT,
            is_terminal_count_window_required=True,
        )
        or diagnostic.get("complete") is not True
        or diagnostic.get("error") is not None
        or diagnostic.get("traversal_complete") is not True
        or diagnostic.get("source_continuation_exhausted") is not True
        or diagnostic.get("next_url_remaining") is not False
        or diagnostic.get("retry_not_before") is not None
        or checkpoint.get("state") != "complete"
        or checkpoint.get("next_url") is not None
        or proof.get("verified") is not True
        or proof.get("failure") is not None
        or proof.get("processed_rows") != checkpoint.get("rows_processed")
        or diagnostic.get("rows_fetched") != checkpoint.get("rows_processed")
        or diagnostic.get("rows_written") != checkpoint.get("rows_processed")
        or diagnostic.get("pages_fetched") != checkpoint.get("pages_processed")
    ):
        raise ReviewedSubsetTerminalDispositionError("evidence")
    validate_terminal_sequence(
        proof,
        checkpoint["pages_processed"],
        checkpoint["rows_processed"],
        terminal_checkpointed=True,
    )
    return VERIFIED_COMPLETE_DISPOSITION


def _terminal_http_410_disposition(
    diagnostic: Mapping[str, Any],
    checkpoint: Mapping[str, Any],
    proof: Mapping[str, Any],
) -> str:
    is_valid = bool(
        diagnostic.get("complete") is False
        and diagnostic.get("error")
        == f"{CURRENT_VERSION_CENSUS_BLOCKED_ERROR}:http_410"
        and diagnostic.get("traversal_complete") is False
        and diagnostic.get("source_continuation_exhausted") is False
        and diagnostic.get("next_url_remaining") is False
        and diagnostic.get("retry_not_before") is None
        and diagnostic.get("pagination_cooldown_retries") == 0
        and diagnostic.get("pagination_cooldown_recovered") is False
        and diagnostic.get("pagination_cooldown_exhausted") is False
        and diagnostic.get("pagination_cooldown_deadline_blocked") is False
        and checkpoint.get("state") == "active"
        and clean_text(checkpoint.get("next_url")) is not None
        and proof.get("verified") is False
        and proof.get("pre_count") > 0
        and checkpoint.get("rows_processed") <= proof["pre_count"]
        and diagnostic.get("pages_fetched") == checkpoint.get("pages_processed")
        and diagnostic.get("rows_fetched") == checkpoint.get("rows_processed")
        and diagnostic.get("rows_written") == checkpoint.get("rows_processed")
    )
    if not is_valid:
        raise ReviewedSubsetTerminalDispositionError("evidence")
    try:
        validate_current_version_census_checkpoint_geometry(
            proof,
            pages_processed=checkpoint["pages_processed"],
            rows_processed=checkpoint["rows_processed"],
            expected_page_count=DIRECT_V5_PAGE_COUNT,
        )
    except ValueError:
        raise ReviewedSubsetTerminalDispositionError("evidence") from None
    return TERMINAL_HTTP_410_DISPOSITION


def _resource_disposition(
    resource_type: str,
    diagnostic: Mapping[str, Any],
    checkpoint: Mapping[str, Any],
    expected_start_url_sha256: str,
) -> tuple[tuple[Any, ...], dict[str, Any]]:
    proof, start_url_sha256, recent_cursor_hashes_sha256 = (
        validated_terminal_resource_proof(
            diagnostic,
            checkpoint,
            expected_start_url_sha256,
        )
    )
    identity = _proof_identity(proof, resource_type)
    expected_disposition = DIRECT_V5_DISPOSITION_BY_RESOURCE_TYPE[resource_type]
    disposition = (
        _verified_complete_disposition(diagnostic, checkpoint, proof)
        if expected_disposition == VERIFIED_COMPLETE_DISPOSITION
        else _terminal_http_410_disposition(diagnostic, checkpoint, proof)
    )
    if disposition != expected_disposition:
        raise ReviewedSubsetTerminalDispositionError("evidence")
    return identity, terminal_resource_marker(
        diagnostic,
        checkpoint,
        proof,
        disposition,
        start_url_sha256,
        recent_cursor_hashes_sha256,
        direct_v4=False,
    )


def validated_v5_resource_dispositions(
    diagnostics: Mapping[str, Any],
    checkpoint_rows: Sequence[Mapping[str, Any]],
    candidate_metadata: Mapping[str, Any],
    *,
    expected_start_hash_by_type: Mapping[str, str],
) -> dict[str, dict[str, Any]]:
    """Validate the exact six-complete, one-HTTP-410 v5 packet."""

    checkpoint_by_resource_type = {
        clean_text(checkpoint.get("resource_type")): checkpoint
        for checkpoint in checkpoint_rows
    }
    expected_resource_set = set(EXPECTED_RESOURCE_TYPES)
    if (
        set(diagnostics) != expected_resource_set
        or set(checkpoint_by_resource_type) != expected_resource_set
        or set(expected_start_hash_by_type) != expected_resource_set
        or candidate_metadata.get("verification_campaign_id")
        != DIRECT_V5_CAMPAIGN_ID
    ):
        raise ReviewedSubsetTerminalDispositionError("evidence")
    identities = set()
    resources_by_type = {}
    for resource_type in EXPECTED_RESOURCE_TYPES:
        identity, resource = _resource_disposition(
            resource_type,
            json_object(diagnostics[resource_type]),
            checkpoint_by_resource_type[resource_type],
            expected_start_hash_by_type[resource_type],
        )
        identities.add(identity)
        resources_by_type[resource_type] = resource
    if len(identities) != 1:
        raise ReviewedSubsetTerminalDispositionError("evidence")
    return resources_by_type


__all__ = ("validated_v5_resource_dispositions",)
