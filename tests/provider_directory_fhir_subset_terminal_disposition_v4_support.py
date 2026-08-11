# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Neutral direct-v4 fixtures for the existing terminal-disposition path."""

from __future__ import annotations

from contextlib import asynccontextmanager
from copy import deepcopy
from datetime import datetime
import hashlib
import json
from typing import Any

from process.provider_directory_fhir_census_binding import (
    CurrentVersionCensusContract,
)
from process.provider_directory_fhir_census_execution import (
    CURRENT_VERSION_CENSUS_BLOCKED_ERROR,
    SERVER_ISSUED_SUBSET_FETCH_MODE,
    current_version_census_completed_proof,
    current_version_census_initial_proof,
)
from process.provider_directory_fhir_census_page_geometry import (
    current_version_census_checkpoint_proof,
)
from process.provider_directory_fhir_root_policy import (
    POLICY_PENDING_STATUS,
    REVIEWED_ROOT_POLICY_METADATA_KEY,
    reviewed_root_policy_document,
)
from process.provider_directory_fhir_subset_completion import canonical_sha256
from process.provider_directory_fhir_subset_identity import (
    server_issued_subset_source_scope_payload,
)
from process.provider_directory_fhir_subset_terminal_disposition_contract import (
    TERMINAL_DISPOSITION_METADATA_KEY,
)
from process.provider_directory_fhir_subset_terminal_disposition_evidence import (
    expected_subset_coverage,
)
from process.provider_directory_fhir_subset_terminal_disposition_profile import (
    DIRECT_V4_CAMPAIGN_ID,
    DIRECT_V4_CANONICALIZATION_VERSION,
    DIRECT_V4_COMPLETION_SCOPES,
    DIRECT_V4_CONTINUATION_STRATEGY,
    DIRECT_V4_DRIFT_RESOURCE_TYPES,
    DIRECT_V4_PAGE_COUNT,
    DIRECT_V4_PROOF_CONTRACT_VERSION,
    DIRECT_V4_SEMANTICS,
    DIRECT_V4_STRATEGY_VERSION,
    DIRECT_V4_TRAVERSAL_VERSION,
    EXPECTED_RESOURCE_TYPES,
    SOURCE_PROFILE_RESOURCE_TYPES,
)
from process.provider_directory_resource_hash import (
    TRANSPORT_NEUTRAL_RESOURCE_HASH_CONTRACT,
)
from tests.provider_directory_subset_completion_pg_support import (
    valid_source_metadata,
)


CUTOFF = "2026-08-10T04:35:38.000000Z"
CHECKPOINT_SCOPE_SHA256 = "4" * 64
POLICY = reviewed_root_policy_document(1)


def direct_v4_contract() -> CurrentVersionCensusContract:
    """Return the frozen synthetic direct-v4 census contract."""

    return CurrentVersionCensusContract(
        source_id="source-a",
        cutoff=CUTOFF,
        resources=SOURCE_PROFILE_RESOURCE_TYPES,
        expected_nonempty_resources=SOURCE_PROFILE_RESOURCE_TYPES,
        start_urls=tuple(
            (
                resource_type,
                f"https://directory.example.test/fhir/{resource_type}",
            )
            for resource_type in SOURCE_PROFILE_RESOURCE_TYPES
        ),
        continuation_strategy=DIRECT_V4_CONTINUATION_STRATEGY,
        contract_version=DIRECT_V4_PROOF_CONTRACT_VERSION,
        page_count=DIRECT_V4_PAGE_COUNT,
        strategy_version=DIRECT_V4_STRATEGY_VERSION,
        traversal_version=DIRECT_V4_TRAVERSAL_VERSION,
        canonicalization_version=DIRECT_V4_CANONICALIZATION_VERSION,
        completion_scopes=DIRECT_V4_COMPLETION_SCOPES,
        campaign_id=DIRECT_V4_CAMPAIGN_ID,
        semantics=DIRECT_V4_SEMANTICS,
    )


def _safe_proof(proof_by_field: dict[str, Any]) -> dict[str, Any]:
    return {
        field_name: field_value
        for field_name, field_value in proof_by_field.items()
        if field_name != "continuation_hop_sha256"
    }


def _terminal_proof(resource_type: str) -> dict[str, Any]:
    initial_proof = current_version_census_initial_proof(
        direct_v4_contract(),
        resource_type,
        502,
        expected_page_count=DIRECT_V4_PAGE_COUNT,
    )
    active_proof = current_version_census_checkpoint_proof(
        initial_proof,
        pages_processed=1,
        rows_processed=DIRECT_V4_PAGE_COUNT,
        page_entry_count=DIRECT_V4_PAGE_COUNT,
        expected_page_count=DIRECT_V4_PAGE_COUNT,
        continuation_identity_sha256="5" * 64,
        continuation_shape_sha256="6" * 64,
    )
    return current_version_census_completed_proof(
        active_proof,
        post_count=(
            499 if resource_type in DIRECT_V4_DRIFT_RESOURCE_TYPES else 501
        ),
        processed_rows=DIRECT_V4_PAGE_COUNT,
        unique_candidate_rows=DIRECT_V4_PAGE_COUNT,
        pages_processed=2,
        expected_page_count=DIRECT_V4_PAGE_COUNT,
        terminal_page_entry_count=0,
    )


def _diagnostic(
    resource_type: str,
    proof_by_field: dict[str, Any],
) -> dict[str, Any]:
    is_drift = resource_type in DIRECT_V4_DRIFT_RESOURCE_TYPES
    safe_proof_by_field = _safe_proof(proof_by_field)
    return {
        "complete": not is_drift,
        "collection_complete": False,
        "traversal_complete": not is_drift,
        "source_continuation_exhausted": not is_drift,
        "absence_semantics": "unknown_under_subset",
        "plan_graph_complete": False,
        "bounded": False,
        "error": (
            f"{CURRENT_VERSION_CENSUS_BLOCKED_ERROR}:census_drift"
            if is_drift
            else None
        ),
        "fetch_mode": SERVER_ISSUED_SUBSET_FETCH_MODE,
        "pages_fetched": 2,
        "rows_fetched": DIRECT_V4_PAGE_COUNT,
        "rows_written": 0,
        "resource_scan_concurrency_requested": 1,
        "resource_scan_concurrency_effective": 1,
        "source_fetch_elapsed_ms": 1,
        "stream_write_elapsed_seconds": 0.01,
        "checkpoint_persist_elapsed_seconds": 0.01,
        "page_prefetch_eligible": True,
        "page_prefetch_started": 1,
        "page_prefetch_consumed": 1,
        "page_prefetch_discarded": 0,
        "page_prefetch_wait_seconds": 0.01,
        "row_limit_reached": False,
        "page_limit_reached": False,
        "hard_page_limit_reached": False,
        "deadline_reached": False,
        "next_url_remaining": False,
        "retry_not_before": None,
        "source_fetch": None,
        "last_updated_completeness": None,
        "caresource_opaque_cursor_completeness": None,
        "current_version_census_completeness": None,
        "server_issued_subset_completeness": safe_proof_by_field,
        "server_issued_subset_coverage": expected_subset_coverage(
            safe_proof_by_field
        ),
        "pagination_cooldown_retries": 0,
        "pagination_cooldown_wait_seconds": 0.0,
        "pagination_cooldown_recovered": False,
        "pagination_cooldown_exhausted": False,
        "pagination_cooldown_deadline_blocked": False,
    }


def _checkpoint(
    resource_type: str,
    ordinal: int,
    proof_by_field: dict[str, Any],
) -> dict[str, Any]:
    is_drift = resource_type in DIRECT_V4_DRIFT_RESOURCE_TYPES
    start_hash = hashlib.sha256(
        direct_v4_contract()
        .start_url(resource_type, DIRECT_V4_PAGE_COUNT)
        .encode("utf-8")
    ).hexdigest()
    return {
        "canonical_api_base": "https://directory.example.test/fhir",
        "resource_type": resource_type,
        "source_scope_hash": CHECKPOINT_SCOPE_SHA256,
        "dataset_id": "dataset-a",
        "source_ids": ["source-a"],
        "acquisition_root_run_id": "root-a",
        "owner_run_id": "root-a",
        "retry_of_run_id": None,
        "start_url_hash": start_hash,
        "next_url": (
            f"https://directory.example.test/fhir/next/{ordinal}"
            if is_drift
            else None
        ),
        "state": "active" if is_drift else "complete",
        "pages_processed": 1 if is_drift else 2,
        "rows_processed": DIRECT_V4_PAGE_COUNT,
        "recent_cursor_hashes": [start_hash],
        "completeness_json": proof_by_field,
        "updated_at": datetime(2026, 8, 10, 1, ordinal),
    }


def _diagnostics_and_checkpoints() -> tuple[dict[str, dict], tuple[dict, ...]]:
    diagnostics_by_type: dict[str, dict[str, Any]] = {}
    checkpoint_rows = []
    for ordinal, resource_type in enumerate(EXPECTED_RESOURCE_TYPES, start=1):
        proof_by_field = _terminal_proof(resource_type)
        diagnostics_by_type[resource_type] = _diagnostic(
            resource_type,
            proof_by_field,
        )
        checkpoint_rows.append(
            _checkpoint(resource_type, ordinal, proof_by_field)
        )
    return diagnostics_by_type, tuple(checkpoint_rows)


def _source_by_field(diagnostics_by_type: dict[str, dict]) -> dict[str, Any]:
    source_metadata_by_field = valid_source_metadata(
        POLICY_PENDING_STATUS,
        contract=direct_v4_contract(),
    )
    source_metadata_by_field.update(
        {
            "provider_directory_supported_resources": list(
                SOURCE_PROFILE_RESOURCE_TYPES
            ),
            "provider_directory_expected_nonempty_resources": list(
                SOURCE_PROFILE_RESOURCE_TYPES
            ),
            "provider_directory_server_issued_subset_resources": list(
                SOURCE_PROFILE_RESOURCE_TYPES
            ),
            "provider_directory_resource_page_count_caps": {
                resource_type: DIRECT_V4_PAGE_COUNT
                for resource_type in EXPECTED_RESOURCE_TYPES
            },
            REVIEWED_ROOT_POLICY_METADATA_KEY: deepcopy(POLICY),
            "last_resource_import": {
                "run_id": "root-a",
                "observed_at": "2026-08-10T17:23:22Z",
                "resources": deepcopy(diagnostics_by_type),
            },
        }
    )
    source_by_field = {
        "source_id": "source-a",
        "endpoint_id": "endpoint-a",
        "canonical_api_base": "https://directory.example.test/fhir",
        "requires_registration": False,
        "requires_api_key": False,
        "auth_type": "none",
        "metadata_json": source_metadata_by_field,
    }
    return source_by_field


def _candidate_metadata(
    source_by_field: dict[str, Any],
    diagnostics_by_type: dict[str, dict],
) -> dict[str, Any]:
    verification_scope = canonical_sha256(
        server_issued_subset_source_scope_payload(
            source_by_field,
            ("source-a",),
            CUTOFF,
            source_by_field["canonical_api_base"],
        )
    )
    completion_by_field = {
        "acquisition_root_run_id": "root-a",
        "terminal_run_id": "root-a",
        "source_ids": ["source-a"],
        "selected_resources": list(EXPECTED_RESOURCE_TYPES),
        "resource_diagnostics": deepcopy(diagnostics_by_type),
        "verification_campaign_id": DIRECT_V4_CAMPAIGN_ID,
        "verification_source_scope_hash": verification_scope,
    }
    return {
        "acquisition_root_run_id": "root-a",
        "source_ids": ["source-a"],
        "selected_resources": list(EXPECTED_RESOURCE_TYPES),
        "expected_resources": list(EXPECTED_RESOURCE_TYPES),
        "requires_twin_root_verification": False,
        "verification_campaign_id": DIRECT_V4_CAMPAIGN_ID,
        "verification_source_scope_hash": verification_scope,
        "completion_proof_required_version": 3,
        REVIEWED_ROOT_POLICY_METADATA_KEY: deepcopy(POLICY),
        "resource_hash_contract": TRANSPORT_NEUTRAL_RESOURCE_HASH_CONTRACT,
        "reused_from_checkpoint": False,
        "resource_diagnostics": deepcopy(diagnostics_by_type),
        "completion_proof_v1": completion_by_field,
        "error": "synthetic-terminal-failure",
    }


def _candidate_by_field(
    source_by_field: dict[str, Any],
    diagnostics_by_type: dict[str, dict],
    checkpoint_rows: tuple[dict, ...],
) -> dict[str, Any]:
    """Build the synthetic failed-root row used by direct-v4 tests."""

    candidate_metadata_by_field = _candidate_metadata(
        source_by_field,
        diagnostics_by_type,
    )
    resource_count = sum(
        checkpoint_by_field["rows_processed"]
        for checkpoint_by_field in checkpoint_rows
    )
    return {
        "dataset_id": "dataset-a",
        "endpoint_id": "endpoint-a",
        "import_run_id": "root-a",
        "acquisition_root_run_id": "root-a",
        "status": "failed",
        "is_current": False,
        "previous_dataset_id": None,
        "dataset_hash": None,
        "resource_count": resource_count,
        "validated_at": None,
        "published_at": None,
        "superseded_at": None,
        "completion_proof_required_version": 3,
        "completion_proof_json": None,
        "completion_proof_sha256": None,
        "publication_metadata_json": candidate_metadata_by_field,
    }


def direct_v4_inputs() -> tuple[dict, dict, tuple[dict, ...]]:
    """Build one exact synthetic failed direct-v4 root."""

    diagnostics_by_type, checkpoint_rows = _diagnostics_and_checkpoints()
    source_by_field = _source_by_field(diagnostics_by_type)
    candidate_by_field = _candidate_by_field(
        source_by_field,
        diagnostics_by_type,
        checkpoint_rows,
    )
    return source_by_field, candidate_by_field, checkpoint_rows


class DirectV4TerminalDatabase:
    """Dispatch the existing one-transaction path against neutral rows."""

    def __init__(self) -> None:
        (
            self.source_row,
            candidate_by_field,
            self.checkpoint_rows,
        ) = direct_v4_inputs()
        self.candidate_rows = [candidate_by_field]
        self.calls: list[tuple[str, str, dict[str, Any]]] = []
        self.valid = True
        self.invalid_proof_shard_count = 0

    @property
    def candidate_row(self) -> dict[str, Any]:
        return self.candidate_rows[-1]

    @asynccontextmanager
    async def transaction(self):
        self.calls.append(("transaction", "begin", {}))
        yield self
        self.calls.append(("transaction", "end", {}))

    async def scalar(self, statement: str, **parameters: Any):
        self.calls.append(("scalar", statement, parameters))
        if "transaction_isolation" in statement:
            return "read committed"
        if "pg_try_advisory_xact_lock" in statement:
            return True
        if "bulk_acquisition_checkpoint" in statement:
            return 0
        if "provider_directory_subset_terminal_disposition_valid" in statement:
            return self.valid
        if "invalid_lineage_count" in statement:
            raise AssertionError("proof totals must use all()")
        if "provider_directory_dataset_proof_shard" in statement:
            return self.invalid_proof_shard_count
        if "SELECT count(*)" in statement:
            return 0
        raise AssertionError(f"unexpected scalar statement: {statement}")

    async def all(self, statement: str, **parameters: Any):
        self.calls.append(("all", statement, parameters))
        if "provider_directory_api_endpoint" in statement:
            return [{"endpoint_id": parameters["endpoint_id"]}]
        if "SELECT source.*" in statement:
            return [deepcopy(self.source_row)]
        if "SELECT dataset.*" in statement:
            selected_rows = []
            for candidate_by_field in self.candidate_rows:
                metadata = candidate_by_field["publication_metadata_json"]
                is_failed_match = bool(
                    candidate_by_field["status"] == parameters.get("prior_status")
                    and candidate_by_field["endpoint_id"]
                    == parameters.get("endpoint_id")
                    and metadata.get("verification_campaign_id")
                    == parameters.get("campaign_id")
                    and TERMINAL_DISPOSITION_METADATA_KEY not in metadata
                )
                marker = metadata.get(TERMINAL_DISPOSITION_METADATA_KEY, {})
                is_replay_match = bool(
                    candidate_by_field["status"]
                    == parameters.get("disposed_status")
                    and marker.get("contract_version")
                    == parameters.get("contract_version")
                )
                if is_failed_match or is_replay_match:
                    selected_rows.append(deepcopy(candidate_by_field))
            return selected_rows
        if "provider_directory_pagination_checkpoint" in statement:
            return deepcopy(self.checkpoint_rows)
        if "raw_count.resource_type" in statement:
            return self._count_rows()
        if "invalid_lineage_count" in statement:
            return [
                {
                    "shard_count": len(self.checkpoint_rows),
                    "proof_row_count": sum(
                        checkpoint_by_field["rows_processed"]
                        for checkpoint_by_field in self.checkpoint_rows
                    ),
                    "invalid_lineage_count": 0,
                }
            ]
        if "provider_directory_dataset_resource" in statement:
            return self._count_rows()
        raise AssertionError(f"unexpected all statement: {statement}")

    def _count_rows(self) -> list[dict[str, Any]]:
        return [
            {
                "resource_type": checkpoint_by_field["resource_type"],
                "resource_count": checkpoint_by_field["rows_processed"],
            }
            for checkpoint_by_field in self.checkpoint_rows
        ]

    async def status(self, statement: str, **parameters: Any):
        self.calls.append(("status", statement, parameters))
        if "LOCK TABLE" in statement or "SET CONSTRAINTS" in statement:
            return None
        if "provider_directory_pagination_checkpoint" in statement:
            return 1
        if "provider_directory_endpoint_dataset" in statement:
            self.candidate_row["status"] = parameters["disposed_status"]
            marker = json.loads(parameters["disposition_marker"])
            self.candidate_row["publication_metadata_json"][
                TERMINAL_DISPOSITION_METADATA_KEY
            ] = marker
            return 1
        raise AssertionError(f"unexpected status statement: {statement}")


__all__ = (
    "DirectV4TerminalDatabase",
    "direct_v4_contract",
    "direct_v4_inputs",
)
