# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Fail-closed edge coverage for terminal-root selection and storage."""

from __future__ import annotations

from copy import deepcopy
from typing import Any, Callable

import pytest

from process import provider_directory_terminal_root_retirement_contract as contract
from process import provider_directory_terminal_root_retirement_store as store
from process.provider_directory_terminal_root_retirement_selection import (
    selected_terminal_root_retirement,
)
from tests.test_provider_directory_terminal_root_retirement_selection import (
    SelectionDatabase,
    request,
)
from tests.test_provider_directory_terminal_root_retirement_store import StoreDatabase


SelectionMutation = Callable[[SelectionDatabase], None]


def _bad_initial_endpoint(database: SelectionDatabase) -> None:
    database.endpoint["canonical_api_base"] = ""


def _bad_source_scope(database: SelectionDatabase) -> None:
    database.target["publication_metadata_json"]["source_ids"] = ["source-other"]


def _bad_target(database: SelectionDatabase) -> None:
    database.target["resource_count"] = -1


def _bad_source(database: SelectionDatabase) -> None:
    assert database.source is not None
    database.source["endpoint_id"] = "endpoint-other"


def _bad_predecessor(database: SelectionDatabase) -> None:
    assert database.predecessor is not None
    database.predecessor["status"] = "validated"


def _empty_lineage(database: SelectionDatabase) -> None:
    database.lineage = []


def _wrong_owner(database: SelectionDatabase) -> None:
    database.lineage[-1]["run_id"] = "run-other-owner"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mutation",
    [
        _bad_initial_endpoint,
        _bad_source_scope,
        _bad_target,
        _bad_source,
        _bad_predecessor,
        _empty_lineage,
        _wrong_owner,
    ],
)
async def test_selection_rejects_each_locked_shape(mutation: SelectionMutation) -> None:
    database = SelectionDatabase()
    mutation(database)
    with pytest.raises(contract.TerminalRootRetirementError, match="evidence_invalid"):
        await selected_terminal_root_retirement(database, request())


class _DriftingEndpointDatabase(SelectionDatabase):
    def __init__(self) -> None:
        super().__init__()
        self.endpoint_read_count = 0

    async def all(self, sql: str, **params: Any) -> list[dict[str, Any]]:
        if "provider_directory_api_endpoint" not in sql:
            return await super().all(sql, **params)
        self.endpoint_read_count += 1
        endpoint = deepcopy(self.endpoint)
        if self.endpoint_read_count == 2:
            endpoint["canonical_api_base"] = "https://drift.invalid/fhir"
        self.calls.append(("all", sql, params))
        return [endpoint]


@pytest.mark.asyncio
async def test_selection_rejects_endpoint_drift_after_scope_locks() -> None:
    with pytest.raises(contract.TerminalRootRetirementError, match="evidence_invalid"):
        await selected_terminal_root_retirement(
            _DriftingEndpointDatabase(),
            request(),
        )


@pytest.mark.asyncio
async def test_selection_rejects_replay_token_drift_and_wrong_request_type() -> None:
    database = SelectionDatabase()
    marker = contract.retirement_marker(
        database.evidence,
        minimum_terminal_age_seconds=contract.MINIMUM_TERMINAL_AGE_SECONDS,
        retired_at="2026-08-10T12:00:00+00:00",
    )
    database.target["status"] = contract.RETIREMENT_STATUS
    database.target["publication_metadata_json"][
        contract.RETIREMENT_METADATA_KEY
    ] = marker

    with pytest.raises(contract.TerminalRootRetirementError, match="evidence_changed"):
        await selected_terminal_root_retirement(
            database,
            request(expected_evidence_sha256="f" * 64),
        )
    with pytest.raises(contract.TerminalRootRetirementError, match="request_invalid"):
        await selected_terminal_root_retirement(SelectionDatabase(), object())


@pytest.mark.asyncio
async def test_preview_rejects_apply_token() -> None:
    with pytest.raises(contract.TerminalRootRetirementError, match="request_invalid"):
        await store.preview_terminal_root_retirement_transaction(
            StoreDatabase(),
            request(expected_evidence_sha256="a" * 64),
        )


@pytest.mark.asyncio
async def test_apply_rechecks_token_after_selection(monkeypatch) -> None:
    database = StoreDatabase()
    marker = contract.retirement_marker(
        database.evidence,
        minimum_terminal_age_seconds=contract.MINIMUM_TERMINAL_AGE_SECONDS,
        retired_at="2026-08-10T12:00:00+00:00",
    )

    async def selected(_database, selected_request):
        return contract.TerminalRootRetirementSelection(
            request=selected_request,
            canonical_api_base="https://synthetic.invalid/fhir",
            prior_status="acquiring",
            observed_metadata={"source_ids": [selected_request.source_id]},
            marker_by_field=marker,
        )

    monkeypatch.setattr(store, "selected_terminal_root_retirement", selected)
    with pytest.raises(contract.TerminalRootRetirementError, match="evidence_changed"):
        await store.apply_terminal_root_retirement_transaction(
            database,
            request(expected_evidence_sha256="f" * 64),
        )
