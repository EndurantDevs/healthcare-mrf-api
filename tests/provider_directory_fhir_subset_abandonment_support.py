# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Neutral unit fixtures for reviewed subset abandonment."""

from __future__ import annotations

from contextlib import asynccontextmanager
from copy import deepcopy
from datetime import datetime

from process.provider_directory_fhir_census_execution import (
    CURRENT_VERSION_CENSUS_BLOCKED_ERROR,
    SERVER_ISSUED_SUBSET_FETCH_MODE,
)
from process.provider_directory_fhir_census_contract import (
    SERVER_ISSUED_SUBSET_RESOURCE_TYPES,
)
from process.provider_directory_fhir_subset_abandonment_contract import (
    ABANDONED_STATUS,
    ABANDONMENT_METADATA_KEY,
    abandonment_marker,
)
from process.provider_directory_fhir_subset_activation_contract import (
    PENDING_STATUS,
)

RESOURCE_TYPES = tuple(sorted(SERVER_ISSUED_SUBSET_RESOURCE_TYPES))
SOURCE_SCOPE_SHA256 = "1" * 64
VERIFICATION_SCOPE_SHA256 = "2" * 64
SERVING_ENDPOINT_ID = "endpoint-serving"


def _source_fixture() -> dict:
    return {
        "source_id": "source-a",
        "endpoint_id": SERVING_ENDPOINT_ID,
        "canonical_api_base": "https://directory.example.test/fhir",
        "metadata_json": {
            "provider_directory_candidate_status": PENDING_STATUS,
            "provider_directory_configured_endpoint_id": "endpoint-a",
            "provider_directory_verification_campaign_id": "campaign-a",
            "last_resource_import": {
                "run_id": "owner-a",
                "resources": {
                    resource_type: {
                        "bounded": False,
                        "complete": False,
                        "error": (f"{CURRENT_VERSION_CENSUS_BLOCKED_ERROR}:http_410"),
                        "fetch_mode": SERVER_ISSUED_SUBSET_FETCH_MODE,
                    }
                    for resource_type in RESOURCE_TYPES
                },
            },
        },
    }


def _candidate_fixture() -> dict:
    return {
        "dataset_id": "dataset-a",
        "endpoint_id": "endpoint-a",
        "import_run_id": "owner-a",
        "acquisition_root_run_id": "root-a",
        "status": "failed",
        "is_current": False,
        "resource_count": 0,
        "completion_proof_required_version": 3,
        "completion_proof_json": None,
        "completion_proof_sha256": None,
        "publication_metadata_json": {
            "source_ids": ["source-a"],
            "selected_resources": list(RESOURCE_TYPES),
            "verification_source_scope_hash": VERIFICATION_SCOPE_SHA256,
            "verification_campaign_id": "campaign-a",
        },
    }


def _checkpoint_fixtures(source_by_field: dict) -> tuple[dict, ...]:
    return tuple(
        {
            "canonical_api_base": source_by_field["canonical_api_base"],
            "resource_type": resource_type,
            "source_scope_hash": SOURCE_SCOPE_SHA256,
            "dataset_id": "dataset-a",
            "source_ids": ["source-a"],
            "acquisition_root_run_id": "root-a",
            "owner_run_id": "owner-a",
            "retry_of_run_id": "owner-prior",
            "start_url_hash": f"start-{ordinal}",
            "next_url": f"https://directory.example.test/next/{ordinal}",
            "state": "active",
            "pages_processed": ordinal,
            "rows_processed": ordinal + 1,
            "recent_cursor_hashes": [f"cursor-{ordinal}"],
            "completeness_json": {"verified": False},
            "updated_at": datetime(2026, 8, 9, 1, ordinal),
        }
        for ordinal, resource_type in enumerate(RESOURCE_TYPES, start=1)
    )


def abandonment_inputs():
    """Return one exact proofless retained root and its evidence rows."""

    source_by_field = _source_fixture()
    candidate_by_field = _candidate_fixture()
    checkpoint_rows = _checkpoint_fixtures(source_by_field)
    return source_by_field, candidate_by_field, checkpoint_rows


def abandoned_candidate(candidate_row, checkpoint_rows):
    """Return the exact idempotent parent state derived from the fixture."""

    candidate = deepcopy(candidate_row)
    resource_count = sum(row["rows_processed"] for row in checkpoint_rows)
    marker = abandonment_marker(
        source_scope_sha256=SOURCE_SCOPE_SHA256,
        resource_types=RESOURCE_TYPES,
        checkpoint_count=len(checkpoint_rows),
        pages_processed=sum(row["pages_processed"] for row in checkpoint_rows),
        rows_processed=resource_count,
        resource_count=resource_count,
        proof_shard_count=2,
        proof_row_count=resource_count,
    )
    candidate["status"] = ABANDONED_STATUS
    candidate["resource_count"] = resource_count
    candidate["publication_metadata_json"][ABANDONMENT_METADATA_KEY] = marker
    return candidate


class AbandonmentDatabase:
    """Dispatch the exact selection/store statements against neutral rows."""

    def __init__(self, *, already_applied: bool = False) -> None:
        source_row, candidate_row, checkpoint_rows = abandonment_inputs()
        if already_applied:
            candidate_row = abandoned_candidate(candidate_row, checkpoint_rows)
        self.source_row = source_row
        self.candidate_row = candidate_row
        self.checkpoint_rows = checkpoint_rows
        self.already_applied = already_applied
        self.calls: list[tuple[str, str, dict]] = []
        self.lock_acquired = True
        self.valid = True

    @asynccontextmanager
    async def transaction(self):
        self.calls.append(("transaction", "begin", {}))
        try:
            yield self
        finally:
            self.calls.append(("transaction", "end", {}))

    async def scalar(self, statement, **parameters):
        self.calls.append(("scalar", statement, parameters))
        if "transaction_isolation" in statement:
            return "read committed"
        if "pg_try_advisory_xact_lock" in statement:
            return self.lock_acquired
        if "provider_directory_bulk_acquisition_checkpoint" in statement:
            return 0
        if "provider_directory_subset_abandonment_valid" in statement:
            return self.valid
        raise AssertionError("unexpected scalar statement")

    async def all(self, statement, **parameters):
        self.calls.append(("all", statement, parameters))
        if "provider_directory_api_endpoint" in statement:
            return [{"endpoint_id": "endpoint-a"}]
        if "SELECT source.*" in statement:
            return [deepcopy(self.source_row)]
        if "SELECT dataset.*" in statement:
            return [deepcopy(self.candidate_row)]
        if "provider_directory_pagination_checkpoint" in statement:
            return [] if self.already_applied else deepcopy(self.checkpoint_rows)
        if "raw_count.resource_type" in statement:
            return [
                {
                    "resource_type": checkpoint_row["resource_type"],
                    "resource_count": checkpoint_row["rows_processed"],
                }
                for checkpoint_row in self.checkpoint_rows
            ]
        if "invalid_lineage_count" in statement:
            return [
                {
                    "shard_count": 2,
                    "proof_row_count": sum(
                        checkpoint_row["rows_processed"]
                        for checkpoint_row in self.checkpoint_rows
                    ),
                    "invalid_lineage_count": 0,
                }
            ]
        if "provider_directory_dataset_resource" in statement:
            return [
                {
                    "resource_type": checkpoint_row["resource_type"],
                    "resource_count": checkpoint_row["rows_processed"],
                }
                for checkpoint_row in self.checkpoint_rows
            ]
        raise AssertionError("unexpected all statement")

    async def status(self, statement, **parameters):
        self.calls.append(("status", statement, parameters))
        if "LOCK TABLE" in statement or "SET CONSTRAINTS" in statement:
            return None
        if "UPDATE" in statement:
            return 1
        raise AssertionError("unexpected status statement")
