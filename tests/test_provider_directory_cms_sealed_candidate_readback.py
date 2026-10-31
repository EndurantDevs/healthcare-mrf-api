# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Mocked data delegation only; no native publication receipt is constructed here."""

import asyncio
from copy import deepcopy
from datetime import datetime
from unittest.mock import AsyncMock

import pytest
import pytest_asyncio

from process import provider_directory_cms_prepared_candidate as candidate
from process import provider_directory_cms_snapshot_readback as readback
from tests.test_provider_directory_cms_sealed_readback import (
    case as case,
)
from tests.test_provider_directory_cms_sealed_readback import (
    retained as retained,
)
from tests.test_provider_directory_cms_sealed_readback import (
    sealed_case as sealed_case,
)


@pytest_asyncio.fixture
async def candidate_read_case(sealed_case, monkeypatch):
    """An empty native placeholder tests routing; the real receipt validator rejects it."""
    state = sealed_case
    state.header = dict(status="published", is_current=False, published_at=datetime(2026, 1, 1), superseded_at=None)
    state.native_input = {}
    state.identity_data, state.relationship_data = {"rows": 1}, {"rows": 2}
    original_private_read = state.read_content.side_effect

    async def read_private(current, handoff):
        report = original_private_read(current, handoff)
        return {
            **report,
            "identity_parity": deepcopy(state.identity_data),
            "relationship_parity": deepcopy(state.relationship_data),
        }

    async def read_canonical(current, handoff, *, native_publication):
        assert current is state.current and handoff is state.handoff
        assert native_publication == {} and native_publication is not state.native_input
        return dict(
            header=deepcopy(state.header),
            identities=dict(identity_parity=deepcopy(state.identity_data)),
            relationships=deepcopy(state.relationship_data),
            payloads=dict(native_publication=deepcopy(native_publication)),
        )

    state.read_content.side_effect = read_private
    state.inputs_read = AsyncMock(return_value=("unused-frozen-name", {}))
    state.header_read = AsyncMock(side_effect=lambda *_args: deepcopy(state.header))
    state.canonical_read = AsyncMock(side_effect=read_canonical)
    monkeypatch.setattr(candidate, "_inputs", state.inputs_read)
    monkeypatch.setattr(candidate, "_read_header", state.header_read)
    monkeypatch.setattr(candidate, "read_prepared_cms_candidate", state.canonical_read)
    return state


async def _read(state, *, native_publication):
    case = state.case
    return await readback.read_sealed_cms_candidate(
        case.database,
        case.session,
        case.driver,
        state.request,
        expected_seal=state.sealed,
        expected_attachment=state.attached,
        native_publication=native_publication,
    )


async def test_delegation_keeps_original_receipts_and_returns_detached_data(candidate_read_case):
    state, case = candidate_read_case, candidate_read_case.case
    original = deepcopy(state.binding)
    async with case.session.begin(), case.database.bind_existing_session(case.session):
        transaction = case.session.get_transaction()
        result = await _read(state, native_publication=state.native_input)
        assert set(result) == {"workspace", "private", "canonical"}
        assert result["workspace"] == original and result["private"]["tables"] == original["tables"]
        assert result["canonical"]["identities"]["identity_parity"] == result["private"]["identity_parity"]
        assert result["canonical"]["relationships"] == result["private"]["relationship_parity"]
        assert state.current._readonly is True and state.current._active is False
        assert case.session.get_transaction() is transaction
        state.read_content.assert_awaited_once()
        state.canonical_read.assert_awaited_once()
        assert state.header_read.await_count == 2
        for operation in (state.current.copy_rows("invalid", (), ()), state.current.finish_indexes()):
            with pytest.raises(ValueError, match="readonly_workspace"):
                await operation
    result["workspace"]["tables"].clear()
    result["private"]["tables"].clear()
    assert state.binding == original
    statements = [call.args[0] for call in case.driver.execute.await_args_list]
    assert all(statement.startswith("LOCK TABLE ") for statement in statements)
    assert not state.native_input


@pytest.mark.parametrize("change", ["validated", "current", "no_timestamp", "superseded"])
async def test_truthful_published_noncurrent_preflight_precedes_content(candidate_read_case, change):
    state, case = candidate_read_case, candidate_read_case.case
    match change:
        case "validated":
            state.header.update(status="validated", published_at=None)
        case "current":
            state.header["is_current"] = True
        case "no_timestamp":
            state.header["published_at"] = None
        case "superseded":
            state.header["superseded_at"] = datetime(2026, 1, 2)
    async with case.session.begin(), case.database.bind_existing_session(case.session):
        with pytest.raises(ValueError, match="published_noncurrent_required"):
            await _read(state, native_publication=state.native_input)
        state.read_content.assert_not_awaited()
        state.canonical_read.assert_not_awaited()
        state.savepoint.rollback.assert_awaited_once()


async def test_missing_native_input_refuses_before_content(candidate_read_case):
    state, case = candidate_read_case, candidate_read_case.case
    async with case.session.begin(), case.database.bind_existing_session(case.session):
        with pytest.raises(ValueError, match="native_publication_required"):
            await _read(state, native_publication=None)
        state.read_content.assert_not_awaited()
        state.canonical_read.assert_not_awaited()


async def test_empty_native_placeholder_is_rejected_by_existing_validator(candidate_read_case):
    state, case = candidate_read_case, candidate_read_case.case

    async def validate_placeholder(current, handoff, *, native_publication):
        counts_by_root = dict.fromkeys(candidate.storage.RELATIONS[:4], 0)
        return await candidate._native_component(
            current, handoff, state.header, {"row_counts": counts_by_root}, native_publication
        )

    state.canonical_read.side_effect = validate_placeholder
    async with case.session.begin(), case.database.bind_existing_session(case.session):
        with pytest.raises(ValueError, match="native_publication_changed"):
            await _read(state, native_publication=state.native_input)
        assert state.current._active is False
        state.savepoint.rollback.assert_awaited_once()


@pytest.mark.parametrize("section", ["tables", "header", "identity", "relationships", "native"])
async def test_cross_report_mismatch_refuses(candidate_read_case, section):
    state, case = candidate_read_case, candidate_read_case.case
    original_read = state.canonical_read.side_effect

    async def change_report(*args, **kwargs):
        report = await original_read(*args, **kwargs)
        match section:
            case "tables":
                state.current._receipts.clear()
            case "header":
                report["header"]["published_at"] = datetime(2026, 1, 2)
            case "identity":
                report["identities"]["identity_parity"].clear()
            case "relationships":
                report["relationships"].clear()
            case "native":
                report["payloads"]["native_publication"] = {"invalid": True}
        return report

    state.canonical_read.side_effect = change_report
    async with case.session.begin(), case.database.bind_existing_session(case.session):
        with pytest.raises(ValueError, match="reports_changed"):
            await _read(state, native_publication=state.native_input)
        assert state.current._active is False


@pytest.mark.parametrize("change", ["header", "native_input", "seal", "attachment", "request"])
async def test_inputs_changed_during_canonical_read_refuse(candidate_read_case, change):
    state, case = candidate_read_case, candidate_read_case.case
    original_read = state.canonical_read.side_effect

    async def change_after_read(*args, **kwargs):
        report = await original_read(*args, **kwargs)
        match change:
            case "header":
                state.header["published_at"] = datetime(2026, 1, 2)
            case "native_input":
                state.native_input["invalid"] = True
            case "seal":
                state.sealed["owner_oid"] += 1
            case "attachment":
                state.attached["parents"][readback.attach.ROOTS[0]] += 1
            case "request":
                state.request["migration_role"] = "changed_migration"
        return report

    state.canonical_read.side_effect = change_after_read
    async with case.session.begin(), case.database.bind_existing_session(case.session):
        with pytest.raises(ValueError, match="postimage_changed" if change == "attachment" else "inputs_changed"):
            await _read(state, native_publication=state.native_input)
        assert state.current._active is False
        state.savepoint.rollback.assert_awaited_once()


@pytest.mark.parametrize("failure", [RuntimeError, asyncio.CancelledError])
async def test_canonical_failure_preserves_outer_transaction(candidate_read_case, failure):
    state, case = candidate_read_case, candidate_read_case.case
    state.canonical_read.side_effect = failure("canonical read failed")
    async with case.session.begin(), case.database.bind_existing_session(case.session):
        transaction = case.session.get_transaction()
        with pytest.raises(failure):
            await _read(state, native_publication=state.native_input)
        assert case.session.get_transaction() is transaction and state.current._active is False
        state.savepoint.rollback.assert_awaited_once()
        state.savepoint.commit.assert_not_awaited()
