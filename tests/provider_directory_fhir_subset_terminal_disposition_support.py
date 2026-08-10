# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Neutral fake-DB fixtures for reviewed terminal disposition tests."""

from __future__ import annotations

from contextlib import asynccontextmanager
from copy import deepcopy
from datetime import datetime
import hashlib
import json

from process.provider_directory_fhir_census_binding import (
    CurrentVersionCensusContract,
)
from process.provider_directory_fhir_census_execution import (
    CURRENT_VERSION_CENSUS_BLOCKED_ERROR,
    CURRENT_VERSION_CENSUS_RETRYABLE_ERROR,
    SERVER_ISSUED_SUBSET_FETCH_MODE,
    current_version_census_completed_proof,
    current_version_census_initial_proof,
    current_version_census_proof_identity,
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
    SERVER_ISSUED_SUBSET_CANONICALIZATION_VERSION,
    SERVER_ISSUED_SUBSET_EXACT_COMPLETION_SCOPES,
    SERVER_ISSUED_SUBSET_EXACT_STRATEGY_VERSION,
    SERVER_ISSUED_SUBSET_SEMANTICS,
    SERVER_ISSUED_SUBSET_SMILE_CONTINUATION_STRATEGY,
    SERVER_ISSUED_SUBSET_TRAVERSAL_VERSION,
    server_issued_subset_source_scope_payload,
)
from process.provider_directory_fhir_subset_terminal_disposition_contract import (
    COUNT_DRIFT_RESOURCE_TYPES,
    EXPECTED_RESOURCE_TYPES,
    STABLE_COMPLETE_RESOURCE_TYPES,
    TERMINAL_DISPOSITION_METADATA_KEY,
)
from process.provider_directory_fhir_subset_terminal_disposition_profile import (
    SOURCE_PROFILE_RESOURCE_TYPES,
)
from process.provider_directory_fhir_subset_terminal_disposition_evidence import (
    expected_subset_coverage,
)
from process.provider_directory_resource_hash import (
    TRANSPORT_NEUTRAL_RESOURCE_HASH_CONTRACT,
)
from tests.provider_directory_subset_completion_pg_support import (
    valid_source_metadata,
)


VERIFICATION_SCOPE_SHA256 = "2" * 64
PAGE_COUNT = 2
CUTOFF = "2026-08-09T12:00:00.000000Z"
CAMPAIGN_ID = "reviewed-subset-synthetic-v3"
POLICY = reviewed_root_policy_document(1)


def _contract() -> CurrentVersionCensusContract:
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
        continuation_strategy=SERVER_ISSUED_SUBSET_SMILE_CONTINUATION_STRATEGY,
        strategy_version=SERVER_ISSUED_SUBSET_EXACT_STRATEGY_VERSION,
        contract_version=3,
        semantics=SERVER_ISSUED_SUBSET_SEMANTICS,
        page_count=PAGE_COUNT,
        traversal_version=SERVER_ISSUED_SUBSET_TRAVERSAL_VERSION,
        canonicalization_version=SERVER_ISSUED_SUBSET_CANONICALIZATION_VERSION,
        completion_scopes=SERVER_ISSUED_SUBSET_EXACT_COMPLETION_SCOPES,
        campaign_id=CAMPAIGN_ID,
    )


CONTRACT_IDENTITY_SHA256 = current_version_census_proof_identity(_contract())


def _source_profile() -> dict:
    metadata_by_field = valid_source_metadata(
        POLICY_PENDING_STATUS,
        contract=_contract(),
    )
    metadata_by_field.update(
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
                resource_type: PAGE_COUNT
                for resource_type in EXPECTED_RESOURCE_TYPES
            },
            "provider_directory_current_version_census_start_urls": dict(
                _contract().start_urls
            ),
        }
    )
    metadata_by_field[REVIEWED_ROOT_POLICY_METADATA_KEY] = deepcopy(POLICY)
    return {
        "source_id": "source-a",
        "endpoint_id": "endpoint-serving",
        "canonical_api_base": "https://directory.example.test/fhir",
        "requires_registration": False,
        "requires_api_key": False,
        "auth_type": "none",
        "metadata_json": metadata_by_field,
    }


SOURCE_SCOPE_SHA256 = canonical_sha256(
    server_issued_subset_source_scope_payload(
        _source_profile(),
        ("source-a",),
        CUTOFF,
        "https://directory.example.test/fhir",
    )
)


def _active_proof(resource_type: str) -> dict:
    proof = current_version_census_initial_proof(
        _contract(),
        resource_type,
        4,
        expected_page_count=PAGE_COUNT,
    )
    assert proof["contract_identity"] == CONTRACT_IDENTITY_SHA256
    return current_version_census_checkpoint_proof(
        proof,
        pages_processed=1,
        rows_processed=2,
        page_entry_count=2,
        expected_page_count=PAGE_COUNT,
        continuation_identity_sha256="4" * 64,
        continuation_shape_sha256="5" * 64,
    )


def _terminal_proof(resource_type: str, *, drift: bool) -> dict:
    return current_version_census_completed_proof(
        _active_proof(resource_type),
        post_count=3 if drift else 4,
        processed_rows=2 if drift else 3,
        unique_candidate_rows=2 if drift else 3,
        pages_processed=2,
        expected_page_count=PAGE_COUNT,
        terminal_page_entry_count=0 if drift else 1,
    )


def _safe_proof(proof: dict) -> dict:
    return {
        field_name: field_value
        for field_name, field_value in proof.items()
        if field_name != "continuation_hop_sha256"
    }


def _checkpoint_by_field(
    resource_type: str,
    ordinal: int,
    proof_by_field: dict,
    *,
    is_complete: bool,
    row_count: int,
) -> dict:
    return {
        "canonical_api_base": "https://directory.example.test/fhir",
        "resource_type": resource_type,
        "source_scope_hash": SOURCE_SCOPE_SHA256,
        "dataset_id": "dataset-a",
        "source_ids": ["source-a"],
        "acquisition_root_run_id": "root-a",
        "owner_run_id": "owner-a",
        "retry_of_run_id": "owner-prior",
        "start_url_hash": hashlib.sha256(
            _contract().start_url(resource_type, PAGE_COUNT).encode("utf-8")
        ).hexdigest(),
        "next_url": (
            None
            if is_complete
            else f"https://directory.example.test/next/{ordinal}"
        ),
        "state": "complete" if is_complete else "active",
        "pages_processed": 2 if is_complete else 1,
        "rows_processed": row_count,
        "recent_cursor_hashes": [
            hashlib.sha256(
                _contract().start_url(resource_type, PAGE_COUNT).encode(
                    "utf-8"
                )
            ).hexdigest()
        ],
        "completeness_json": proof_by_field,
        "updated_at": datetime(2026, 8, 9, 1, ordinal),
    }


def _diagnostic(resource_type: str, ordinal: int) -> tuple[dict, dict]:
    """Build one exact diagnostic and its retained checkpoint."""

    is_complete = resource_type in STABLE_COMPLETE_RESOURCE_TYPES
    is_drift = resource_type in COUNT_DRIFT_RESOURCE_TYPES
    proof_by_field = (
        _terminal_proof(resource_type, drift=is_drift)
        if is_complete or is_drift
        else _active_proof(resource_type)
    )
    page_count = 2 if is_complete or is_drift else 1
    row_count = 3 if is_complete else 2
    diagnostic_by_field = _diagnostic_by_field(
        proof_by_field,
        is_complete=is_complete,
        is_drift=is_drift,
        page_count=page_count,
        row_count=row_count,
    )
    checkpoint_by_field = _checkpoint_by_field(
        resource_type,
        ordinal,
        proof_by_field,
        is_complete=is_complete,
        row_count=row_count,
    )
    return diagnostic_by_field, checkpoint_by_field


def _diagnostic_by_field(
    proof_by_field: dict,
    *,
    is_complete: bool,
    is_drift: bool,
    page_count: int,
    row_count: int,
) -> dict:
    error = None
    if not is_complete:
        error = (
            f"{CURRENT_VERSION_CENSUS_BLOCKED_ERROR}:census_drift"
            if is_drift
            else f"{CURRENT_VERSION_CENSUS_RETRYABLE_ERROR}:http_500"
        )
    safe_proof_by_field = _safe_proof(proof_by_field)
    return {
        "complete": is_complete,
        "collection_complete": False,
        "traversal_complete": is_complete,
        "source_continuation_exhausted": is_complete,
        "absence_semantics": "unknown_under_subset",
        "bounded": False,
        "error": error,
        "fetch_mode": SERVER_ISSUED_SUBSET_FETCH_MODE,
        "pages_fetched": page_count,
        "rows_fetched": row_count,
        "rows_written": 0,
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
        "next_url_remaining": not is_complete and not is_drift,
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
        "plan_graph_complete": False,
    }


def _candidate_metadata_by_field(
    diagnostics_by_type: dict[str, dict],
) -> dict:
    completion_copy_by_field = {
        "acquisition_root_run_id": "root-a",
        "terminal_run_id": "owner-a",
        "source_ids": ["source-a"],
        "selected_resources": list(EXPECTED_RESOURCE_TYPES),
        "resource_diagnostics": deepcopy(diagnostics_by_type),
        "verification_campaign_id": CAMPAIGN_ID,
        "verification_source_scope_hash": VERIFICATION_SCOPE_SHA256,
    }
    return {
        "acquisition_root_run_id": "root-a",
        "source_ids": ["source-a"],
        "selected_resources": list(EXPECTED_RESOURCE_TYPES),
        "expected_resources": list(EXPECTED_RESOURCE_TYPES),
        "requires_twin_root_verification": False,
        "verification_campaign_id": CAMPAIGN_ID,
        "verification_source_scope_hash": VERIFICATION_SCOPE_SHA256,
        "completion_proof_required_version": 3,
        REVIEWED_ROOT_POLICY_METADATA_KEY: deepcopy(POLICY),
        "resource_hash_contract": TRANSPORT_NEUTRAL_RESOURCE_HASH_CONTRACT,
        "reused_from_checkpoint": True,
        "resource_diagnostics": deepcopy(diagnostics_by_type),
        "completion_proof_v1": completion_copy_by_field,
        "error": "synthetic-terminal-failure",
    }


def _source_by_field(diagnostics_by_type: dict[str, dict]) -> dict:
    source_by_field = _source_profile()
    source_by_field["metadata_json"]["last_resource_import"] = {
        "run_id": "owner-a",
        "observed_at": "2026-08-09T12:30:00Z",
        "resources": deepcopy(diagnostics_by_type),
    }
    return source_by_field


def terminal_disposition_inputs() -> tuple[dict, dict, tuple[dict, ...]]:
    """Build the exact neutral retained-root fixture."""

    diagnostics_by_type = {}
    checkpoint_rows = []
    for ordinal, resource_type in enumerate(EXPECTED_RESOURCE_TYPES, start=1):
        diagnostic_by_field, checkpoint_by_field = _diagnostic(
            resource_type,
            ordinal,
        )
        diagnostics_by_type[resource_type] = diagnostic_by_field
        checkpoint_rows.append(checkpoint_by_field)
    candidate_metadata_by_field = _candidate_metadata_by_field(
        diagnostics_by_type
    )
    source_by_field = _source_by_field(diagnostics_by_type)
    resource_count = sum(
        checkpoint_row["rows_processed"] for checkpoint_row in checkpoint_rows
    )
    candidate_by_field = {
        "dataset_id": "dataset-a",
        "endpoint_id": "endpoint-a",
        "import_run_id": "owner-a",
        "acquisition_root_run_id": "root-a",
        "status": "failed",
        "is_current": False,
        "resource_count": resource_count,
        "validated_at": None,
        "published_at": None,
        "superseded_at": None,
        "completion_proof_required_version": 3,
        "completion_proof_json": None,
        "completion_proof_sha256": None,
        "publication_metadata_json": candidate_metadata_by_field,
    }
    return source_by_field, candidate_by_field, tuple(checkpoint_rows)


class TerminalDispositionDatabase:
    """Dispatch exact selection/store statements against neutral fake rows."""

    def __init__(self) -> None:
        self.source_row, self.candidate_row, self.checkpoint_rows = (
            terminal_disposition_inputs()
        )
        self.calls: list[tuple[str, str, dict]] = []
        self.valid = True

    @asynccontextmanager
    async def transaction(self):
        self.calls.append(("transaction", "begin", {}))
        yield self
        self.calls.append(("transaction", "end", {}))

    async def scalar(self, statement, **parameters):
        self.calls.append(("scalar", statement, parameters))
        if "transaction_isolation" in statement:
            return "read committed"
        if "pg_try_advisory_xact_lock" in statement:
            return True
        if "bulk_acquisition_checkpoint" in statement:
            return 0
        if "provider_directory_subset_terminal_disposition_valid" in statement:
            return self.valid
        raise AssertionError("unexpected scalar statement")

    async def all(self, statement, **parameters):
        self.calls.append(("all", statement, parameters))
        if "provider_directory_api_endpoint" in statement:
            return [{"endpoint_id": parameters["endpoint_id"]}]
        if "SELECT source.*" in statement:
            return [deepcopy(self.source_row)]
        if "SELECT dataset.*" in statement:
            expected_status = parameters.get(
                "prior_status",
                parameters.get("disposed_status"),
            )
            return (
                [deepcopy(self.candidate_row)]
                if self.candidate_row["status"] == expected_status
                else []
            )
        if "provider_directory_pagination_checkpoint" in statement:
            if self.candidate_row["status"] == "acquisition_abandoned":
                return []
            return deepcopy(self.checkpoint_rows)
        if "raw_count.resource_type" in statement:
            return self._count_rows()
        if "invalid_lineage_count" in statement:
            return [{
                "shard_count": 3,
                "proof_row_count": self.candidate_row["resource_count"],
                "invalid_lineage_count": 0,
            }]
        if "provider_directory_dataset_resource" in statement:
            return self._count_rows()
        raise AssertionError("unexpected all statement")

    def _count_rows(self) -> list[dict]:
        return [
            {
                "resource_type": row["resource_type"],
                "resource_count": row["rows_processed"],
            }
            for row in self.checkpoint_rows
        ]

    async def status(self, statement, **parameters):
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
        raise AssertionError("unexpected status statement")
