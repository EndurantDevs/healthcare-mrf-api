# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Focused locked-selection tests for terminal root retirement."""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from typing import Any

import pytest

from process import provider_directory_terminal_root_retirement_contract as contract
from process.provider_directory_terminal_root_retirement_selection import (
    selected_terminal_root_retirement,
)

SHA = "a" * 64


def evidence() -> dict[str, Any]:
    evidence_by_relation = {
        name: {"row_count": 0, "row_sha256": SHA}
        for name in contract.REQUIRED_CHILD_RELATIONS
    }
    evidence_by_relation["provider_directory_dataset_resource"]["row_count"] = 7
    evidence_by_relation["provider_directory_dataset_proof_shard"]["row_count"] = 2
    return {
        "actual_resource_count": 7,
        "child_relations": evidence_by_relation,
        "lineage_finished_at": "2026-08-10T00:00:00+00:00",
        "lineage_sha256": SHA,
        "parent_identity_sha256": SHA,
        "parent_resource_count": 7,
        "predecessor_identity_sha256": SHA,
        "prior_status": "acquiring",
        "proof_row_count": 9,
        "proof_shard_count": 2,
        "resource_counts": {"Organization": 3, "PractitionerRole": 4},
        "source_identity_sha256": SHA,
        "target_identity_sha256": SHA,
        "terminal_run_count": 2,
    }


def request(**overrides: Any) -> contract.TerminalRootRetirementRequest:
    request_by_field = {
        "source_id": "source-synthetic",
        "endpoint_id": "endpoint-synthetic",
        "dataset_id": "dataset-candidate",
        "acquisition_root_run_id": "run-root",
        "owner_run_id": "run-owner",
        "expected_current_dataset_id": "dataset-current",
    }
    request_by_field.update(overrides)
    return contract.TerminalRootRetirementRequest(**request_by_field)


def target_row() -> dict[str, Any]:
    return {
        "dataset_id": "dataset-candidate",
        "endpoint_id": "endpoint-synthetic",
        "import_run_id": "run-owner",
        "acquisition_root_run_id": "run-root",
        "previous_dataset_id": "dataset-current",
        "dataset_hash": None,
        "status": "acquiring",
        "is_current": False,
        "resource_count": 7,
        "created_at": datetime(2026, 8, 1, tzinfo=timezone.utc),
        "validated_at": None,
        "published_at": None,
        "superseded_at": None,
        "publication_metadata_json": {
            "source_ids": ["source-synthetic"],
        },
        "completion_proof_required_version": None,
        "completion_proof_json": None,
        "completion_proof_sha256": None,
    }


class SelectionDatabase:
    """Route exact selector queries to mutable synthetic rows."""

    def __init__(self) -> None:
        self.endpoint = {
            "endpoint_id": "endpoint-synthetic",
            "canonical_api_base": "https://synthetic.invalid/fhir",
        }
        self.source: dict[str, Any] | None = {
            "source_id": "source-synthetic",
            "endpoint_id": "endpoint-synthetic",
            "canonical_api_base": "https://synthetic.invalid/fhir",
        }
        self.target = target_row()
        self.predecessor: dict[str, Any] | None = {
            "dataset_id": "dataset-current",
            "endpoint_id": "endpoint-synthetic",
            "dataset_hash": "b" * 64,
            "status": "published",
            "is_current": True,
            "validated_at": datetime(2026, 7, 1, tzinfo=timezone.utc),
            "published_at": datetime(2026, 7, 2, tzinfo=timezone.utc),
            "superseded_at": None,
        }
        self.lineage = [
            {
                "run_id": "run-root",
                "retry_of_run_id": None,
                "importer": "provider-directory-fhir",
                "status": "failed",
                "finished_at": datetime(2026, 8, 1, tzinfo=timezone.utc),
                "depth": 0,
                "terminal_age_satisfied": True,
            },
            {
                "run_id": "run-owner",
                "retry_of_run_id": "run-root",
                "importer": "provider-directory-fhir",
                "status": "failed",
                "finished_at": datetime(2026, 8, 2, tzinfo=timezone.utc),
                "depth": 1,
                "terminal_age_satisfied": True,
            },
        ]
        self.evidence = evidence()
        self.competing_rows: list[dict[str, Any]] = []
        self.lock_results = [True, True]
        self.calls: list[tuple[str, str, dict[str, Any]]] = []

    async def all(self, sql: str, **params: Any) -> list[dict[str, Any]]:
        self.calls.append(("all", sql, params))
        if "provider_directory_api_endpoint" in sql:
            return [deepcopy(self.endpoint)]
        if "dataset.import_run_id = :owner_run_id" in sql:
            return [deepcopy(self.target)]
        if "FROM \"mrf\".\"provider_directory_source\"" in sql:
            return [] if self.source is None else [deepcopy(self.source)]
        if "dataset.dataset_id = :predecessor_id" in sql:
            return [] if self.predecessor is None else [deepcopy(self.predecessor)]
        if "dataset.dataset_id <> :dataset_id" in sql:
            return deepcopy(self.competing_rows)
        if "WITH RECURSIVE lineage" in sql:
            return deepcopy(self.lineage)
        raise AssertionError(sql)

    async def scalar(self, sql: str, **params: Any) -> Any:
        self.calls.append(("scalar", sql, params))
        if "pg_try_advisory_xact_lock" in sql:
            return self.lock_results.pop(0)
        if contract.RETIREMENT_EVIDENCE_FUNCTION in sql:
            return deepcopy(self.evidence)
        if "pg_catalog.to_char(" in sql:
            return "2026-08-10 12:00:00+00"
        raise AssertionError(sql)

    async def status(self, sql: str, **params: Any) -> Any:
        self.calls.append(("status", sql, params))
        if sql.startswith("LOCK TABLE"):
            return None
        raise AssertionError(sql)


@pytest.mark.asyncio
async def test_selection_locks_scope_inventory_and_exact_legacy_state() -> None:
    database = SelectionDatabase()

    selection = await selected_terminal_root_retirement(database, request())

    assert selection.prior_status == "acquiring"
    assert selection.marker_by_field["evidence"] == evidence()
    advisory_calls = [
        call for call in database.calls if "pg_try_advisory_xact_lock" in call[1]
    ]
    assert [call[2]["lock_identity"] for call in advisory_calls] == [
        "provider-directory-pagination:https://synthetic.invalid/fhir",
        "endpoint-synthetic",
    ]
    share_sql = next(call[1] for call in database.calls if call[0] == "status")
    assert '"mrf"."import_run"' in share_sql
    assert '"mrf"."provider_directory_endpoint_dataset"' in share_sql
    assert "provider_directory_dataset_resource" in share_sql
    assert "provider_directory_endpoint_dataset_previous_reference" not in share_sql
    lineage_call = next(call for call in database.calls if "WITH RECURSIVE" in call[1])
    assert lineage_call[2]["minimum_age"] == 900

    explicit = SelectionDatabase()
    explicit.target["publication_metadata_json"]["resource_hash_contract"] = (
        contract.RETIREMENT_RESOURCE_HASH_CONTRACT
    )
    assert (
        await selected_terminal_root_retirement(explicit, request())
    ).prior_status == "acquiring"


@pytest.mark.asyncio
async def test_selection_fails_closed_on_busy_drift_and_competitor() -> None:
    busy = SelectionDatabase()
    busy.lock_results[0] = False
    with pytest.raises(contract.TerminalRootRetirementError, match="busy"):
        await selected_terminal_root_retirement(busy, request())

    drifted = SelectionDatabase()
    drifted.target["publication_metadata_json"]["resource_hash_contract"] = (
        "semantic_content_v4"
    )
    with pytest.raises(contract.TerminalRootRetirementError, match="evidence_invalid"):
        await selected_terminal_root_retirement(drifted, request())

    null_contract = SelectionDatabase()
    null_contract.target["publication_metadata_json"]["resource_hash_contract"] = None
    with pytest.raises(contract.TerminalRootRetirementError, match="evidence_invalid"):
        await selected_terminal_root_retirement(null_contract, request())

    competing = SelectionDatabase()
    competing.competing_rows = [{"dataset_id": "dataset-other"}]
    with pytest.raises(contract.TerminalRootRetirementError, match="evidence_invalid"):
        await selected_terminal_root_retirement(competing, request())

    referenced = SelectionDatabase()
    referenced.evidence["child_relations"][
        "provider_directory_endpoint_dataset_previous_reference"
    ]["row_count"] = 1
    with pytest.raises(contract.TerminalRootRetirementError, match="evidence_invalid"):
        await selected_terminal_root_retirement(referenced, request())


@pytest.mark.asyncio
async def test_selection_requires_contiguous_aged_terminal_root_to_owner() -> None:
    database = SelectionDatabase()
    database.lineage[-1]["terminal_age_satisfied"] = False
    with pytest.raises(contract.TerminalRootRetirementError, match="evidence_invalid"):
        await selected_terminal_root_retirement(database, request())

    database = SelectionDatabase()
    database.lineage[-1]["retry_of_run_id"] = "run-unexpected"
    with pytest.raises(contract.TerminalRootRetirementError, match="evidence_invalid"):
        await selected_terminal_root_retirement(database, request())


@pytest.mark.asyncio
async def test_selection_binds_apply_to_preview_evidence_token() -> None:
    database = SelectionDatabase()
    token = contract.canonical_json_sha256(database.evidence)

    selection = await selected_terminal_root_retirement(
        database, request(expected_evidence_sha256=token)
    )

    assert contract.canonical_json_sha256(selection.marker_by_field["evidence"]) == token
    with pytest.raises(contract.TerminalRootRetirementError, match="evidence_changed"):
        await selected_terminal_root_retirement(
            SelectionDatabase(), request(expected_evidence_sha256="f" * 64)
        )


@pytest.mark.asyncio
async def test_replay_uses_marker_without_mutable_source_or_predecessor() -> None:
    database = SelectionDatabase()
    marker = contract.retirement_marker(
        database.evidence,
        minimum_terminal_age_seconds=900,
        retired_at="2026-08-10T12:00:00+00:00",
    )
    database.target["status"] = contract.RETIREMENT_STATUS
    database.target["publication_metadata_json"][
        contract.RETIREMENT_METADATA_KEY
    ] = marker
    database.source = None
    database.predecessor = None
    token = contract.canonical_json_sha256(database.evidence)

    selection = await selected_terminal_root_retirement(
        database, request(expected_evidence_sha256=token)
    )

    assert selection.prior_status == contract.RETIREMENT_STATUS
    assert not any("provider_directory_source" in call[1] for call in database.calls)
    assert not any(
        "WHERE dataset.dataset_id = :predecessor_id" in call[1]
        for call in database.calls
    )
    assert not any("WITH RECURSIVE" in call[1] for call in database.calls)
