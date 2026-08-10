# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Focused transactional-store tests for terminal root retirement."""

from __future__ import annotations

from contextlib import AbstractAsyncContextManager
from copy import deepcopy
import json
from typing import Any

import pytest

from process import provider_directory_terminal_root_retirement_contract as contract
from process.provider_directory_terminal_root_retirement_store import (
    apply_terminal_root_retirement_transaction,
    preview_terminal_root_retirement_transaction,
)
from tests.test_provider_directory_terminal_root_retirement_selection import (
    SelectionDatabase,
    request,
)


class _Transaction(AbstractAsyncContextManager[None]):
    def __init__(self, database: "StoreDatabase") -> None:
        self.database = database

    async def __aenter__(self) -> None:
        self.database.transaction_entries += 1

    async def __aexit__(self, *error: Any) -> None:
        return None


class StoreDatabase(SelectionDatabase):
    def __init__(self) -> None:
        super().__init__()
        self.transaction_entries = 0
        self.isolation = "read committed"
        self.valid_retirement = True
        self.update_result = 1

    def transaction(self) -> _Transaction:
        return _Transaction(self)

    async def scalar(self, sql: str, **params: Any) -> Any:
        if "current_setting('transaction_isolation')" in sql:
            self.calls.append(("scalar", sql, params))
            return self.isolation
        if contract.RETIREMENT_VALID_FUNCTION in sql:
            self.calls.append(("scalar", sql, params))
            return self.valid_retirement
        return await super().scalar(sql, **params)

    async def status(self, sql: str, **params: Any) -> Any:
        if sql.lstrip().startswith("UPDATE"):
            self.calls.append(("status", sql, params))
            if self.update_result == 1:
                self.target["status"] = params["retirement_status"]
                self.target["publication_metadata_json"] = {
                    **self.target["publication_metadata_json"],
                    params["marker_key"]: json.loads(params["marker_json"]),
                }
            return self.update_result
        return await super().status(sql, **params)


@pytest.mark.asyncio
async def test_preview_returns_closed_token_without_parent_write() -> None:
    database = StoreDatabase()
    before = deepcopy(database.target)

    evidence_sha256 = await preview_terminal_root_retirement_transaction(
        database, request()
    )

    assert evidence_sha256 == contract.canonical_json_sha256(database.evidence)
    assert database.target == before
    assert database.transaction_entries == 1
    assert not any(
        call[0] == "status" and call[1].lstrip().startswith("UPDATE")
        for call in database.calls
    )


@pytest.mark.asyncio
async def test_apply_mutates_only_status_and_marker_with_exact_cas() -> None:
    database = StoreDatabase()
    before = deepcopy(database.target)
    evidence_sha256 = contract.canonical_json_sha256(database.evidence)

    retirement_result = await apply_terminal_root_retirement_transaction(
        database,
        request(expected_evidence_sha256=evidence_sha256),
    )

    assert retirement_result.retired is True
    assert database.target["status"] == contract.RETIREMENT_STATUS
    marker = database.target["publication_metadata_json"].pop(
        contract.RETIREMENT_METADATA_KEY
    )
    before_metadata = before.pop("publication_metadata_json")
    after_metadata = database.target.pop("publication_metadata_json")
    before["status"] = contract.RETIREMENT_STATUS
    assert database.target == before
    assert after_metadata == before_metadata
    assert marker["evidence"] == database.evidence
    update = next(
        call for call in database.calls
        if call[0] == "status" and call[1].lstrip().startswith("UPDATE")
    )
    assert "resource_count =" not in update[1]
    assert "completion_proof_required_version IS NULL" in update[1]
    assert update[2]["metadata_json"] == json.dumps(
        before_metadata, sort_keys=True, separators=(",", ":")
    )


@pytest.mark.asyncio
async def test_replay_is_write_free_and_ignores_mutable_source_state() -> None:
    database = StoreDatabase()
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

    result = await apply_terminal_root_retirement_transaction(
        database, request(expected_evidence_sha256=token)
    )

    assert result.retired is False
    assert result.marker_sha256 == contract.canonical_json_sha256(marker)
    assert not any(
        call[0] == "status" and call[1].lstrip().startswith("UPDATE")
        for call in database.calls
    )
    assert any(contract.RETIREMENT_VALID_FUNCTION in call[1] for call in database.calls)


@pytest.mark.asyncio
async def test_apply_rejects_missing_or_changed_evidence_before_write() -> None:
    with pytest.raises(contract.TerminalRootRetirementError, match="request_invalid"):
        await apply_terminal_root_retirement_transaction(StoreDatabase(), request())

    database = StoreDatabase()
    with pytest.raises(contract.TerminalRootRetirementError, match="evidence_changed"):
        await apply_terminal_root_retirement_transaction(
            database, request(expected_evidence_sha256="f" * 64)
        )
    assert not any(
        call[0] == "status" and call[1].lstrip().startswith("UPDATE")
        for call in database.calls
    )


@pytest.mark.asyncio
async def test_store_fails_closed_on_isolation_cas_or_guard_drift() -> None:
    database = StoreDatabase()
    database.isolation = "repeatable read"
    with pytest.raises(contract.TerminalRootRetirementError, match="state_invalid"):
        await preview_terminal_root_retirement_transaction(database, request())

    token = contract.canonical_json_sha256(StoreDatabase().evidence)
    database = StoreDatabase()
    database.update_result = 0
    with pytest.raises(contract.TerminalRootRetirementError, match="state_invalid"):
        await apply_terminal_root_retirement_transaction(
            database, request(expected_evidence_sha256=token)
        )

    database = StoreDatabase()
    database.valid_retirement = False
    with pytest.raises(contract.TerminalRootRetirementError, match="state_invalid"):
        await apply_terminal_root_retirement_transaction(
            database, request(expected_evidence_sha256=token)
        )
