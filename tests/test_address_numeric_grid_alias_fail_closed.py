# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Focused failure-path coverage for reviewed address alias operations."""

from __future__ import annotations

import importlib
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process.address_numeric_grid_alias_support import NumericGridAliasRequest
from process.ext import address_alias_sql, address_strict_source_backfill_sql


alias_workflow = importlib.import_module("process.address_numeric_grid_alias")
alias_revoke = importlib.import_module("process.address_numeric_grid_alias_revoke")
alias_store = importlib.import_module("process.address_numeric_grid_alias_store")
alias_support = importlib.import_module("process.address_numeric_grid_alias_support")
strict_backfill = importlib.import_module("process.address_strict_source_backfill")


def _query_result(*, first=None, rows=(), scalar=None):
    query_result = Mock()
    query_result.first.return_value = first
    query_result.all.return_value = list(rows)
    query_result.scalar.return_value = scalar
    return query_result


def test_alias_sql_and_runtime_values_reject_unsafe_inputs():
    with pytest.raises(ValueError, match="mode must be one of"):
        address_alias_sql.numeric_grid_alias_mode("fuzzy")
    with pytest.raises(ValueError, match="Invalid SQL identifier"):
        address_alias_sql.active_alias_generation_sql(schema="unsafe-name")
    with pytest.raises(ValueError, match="Invalid SQL identifier"):
        address_strict_source_backfill_sql.create_reviewed_candidates_sql(
            schema="unsafe-name"
        )
    with pytest.raises(ValueError, match="invalid SQL identifier"):
        alias_support._quote_ident("unsafe-name")
    with pytest.raises(ValueError, match="positive PostgreSQL duration"):
        alias_support._statement_timeout("eventually")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("state_record", "error_message"),
    (
        (None, "singleton state is missing"),
        (SimpleNamespace(schema_version=3, active_ruleset_version=1, generation=0), "schema version"),
        (SimpleNamespace(schema_version=2, active_ruleset_version=2, generation=0), "ruleset"),
    ),
)
async def test_alias_state_rejects_missing_or_unknown_contracts(
    state_record,
    error_message,
):
    session = Mock()
    session.execute = AsyncMock(return_value=_query_result(first=state_record))

    with pytest.raises(RuntimeError, match=error_message):
        await alias_store._alias_state(session, schema="mrf", lock=True)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("shadow_mapping", "error_message"),
    (
        (None, "not found"),
        ({"status": "running", "candidate_digest": "a" * 64}, "must be sealed"),
        ({"status": "sealed", "candidate_digest": "b" * 64}, "does not match"),
    ),
)
async def test_reviewed_shadow_loader_rejects_unsealed_or_changed_evidence(
    monkeypatch,
    shadow_mapping,
    error_message,
):
    shadow_record = (
        None if shadow_mapping is None else SimpleNamespace(_mapping=shadow_mapping)
    )
    monkeypatch.setattr(alias_store.db, "first", AsyncMock(return_value=shadow_record))

    with pytest.raises(ValueError, match=error_message):
        await alias_store._load_reviewed_shadow(
            schema="mrf",
            shadow_run_id="00000000-0000-0000-0000-000000000001",
            expected_digest="a" * 64,
        )


@pytest.mark.asyncio
async def test_alias_runner_rejects_ambiguous_requests_and_unprepared_state():
    request = NumericGridAliasRequest(mode="off")
    runner = alias_workflow._NumericGridAliasRunner(request)

    with pytest.raises(RuntimeError, match="was not prepared"):
        runner._required_execution()
    with pytest.raises(TypeError, match="request object or keyword options"):
        await alias_workflow.run_numeric_grid_alias(request, mode="off")

    apply_runner = alias_workflow._NumericGridAliasRunner(
        NumericGridAliasRequest(mode="apply")
    )
    with pytest.raises(ValueError, match="valid alias_run_id"):
        await apply_runner._prepare_execution("apply", "mrf")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("request_options", "error_message"),
    (
        ({"state_code": "CA"}, "state scope differs"),
        ({"zip_prefix": "90"}, "ZIP scope differs"),
    ),
)
async def test_apply_rejects_scope_changes_after_review(
    monkeypatch,
    request_options,
    error_message,
):
    monkeypatch.setattr(
        alias_workflow,
        "_load_reviewed_shadow",
        AsyncMock(
            return_value={"scope_state_code": "UT", "scope_zip_prefix": "84"}
        ),
    )
    request = NumericGridAliasRequest(
        mode="apply",
        alias_run_id="00000000-0000-0000-0000-000000000001",
        expected_candidate_sha256="a" * 64,
        reviewed_by="synthetic-reviewer",
        **request_options,
    )

    with pytest.raises(ValueError, match=error_message):
        await alias_workflow._NumericGridAliasRunner(request)._prepare_execution(
            "apply", "mrf"
        )


async def _prepared_alias_runner(operation="shadow"):
    request = NumericGridAliasRequest(mode=operation)
    runner = alias_workflow._NumericGridAliasRunner(request)
    runner.execution = await runner._prepare_execution("shadow", "mrf")
    runner.execution.operation = operation
    return runner


@pytest.mark.asyncio
async def test_alias_runner_rejects_lost_run_and_changed_shadow():
    runner = await _prepared_alias_runner()
    session = Mock()
    session.execute = AsyncMock(return_value=_query_result(first=None))
    with pytest.raises(RuntimeError, match="no longer running"):
        await runner._lock_owned_run(session)

    runner.execution.operation = "apply"
    runner.execution.shadow_run_id = "00000000-0000-0000-0000-000000000001"
    runner.execution.reviewed_digest = "a" * 64
    with pytest.raises(RuntimeError, match="reviewed shadow changed"):
        await runner._validate_reviewed_shadow(session)


@pytest.mark.asyncio
async def test_alias_runner_rejects_mutated_candidate_rows_and_active_conflict():
    runner = await _prepared_alias_runner("apply")
    runner.execution.shadow_run_id = "00000000-0000-0000-0000-000000000001"
    runner.execution.reviewed_digest = "a" * 64
    valid_shadow = SimpleNamespace(
        status="sealed",
        candidate_digest="a" * 64,
        alias_kind=runner.execution.alias_kind,
        ruleset_version=runner.execution.ruleset_version,
    )
    session = Mock()
    session.execute = AsyncMock(
        side_effect=(
            _query_result(first=valid_shadow),
            _query_result(first=None),
            _query_result(rows=()),
        )
    )
    with pytest.raises(RuntimeError, match="candidate rows no longer match"):
        await runner._validate_reviewed_shadow(session)

    conflict = SimpleNamespace(
        source_address_key="00000000-0000-0000-0000-000000000001",
        active_target_address_key="00000000-0000-0000-0000-000000000002",
        candidate_target_address_key="00000000-0000-0000-0000-000000000003",
    )
    session.execute = AsyncMock(return_value=_query_result(first=conflict))
    with pytest.raises(RuntimeError, match="target conflicts"):
        await runner._promote_reviewed_candidates(session)


@pytest.mark.asyncio
async def test_alias_runner_invokes_requested_cancellation_check():
    cancel_check = AsyncMock()
    runner = alias_workflow._NumericGridAliasRunner(
        NumericGridAliasRequest(cancel_check=cancel_check)
    )

    await runner._check_cancelled()

    cancel_check.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("active_alias", "error_message"),
    (
        (None, "was not found"),
        (
            SimpleNamespace(
                target_address_key="00000000-0000-0000-0000-000000000003"
            ),
            "differs from expected",
        ),
    ),
)
async def test_revoke_lock_requires_the_exact_active_target(
    monkeypatch,
    active_alias,
    error_message,
):
    monkeypatch.setattr(alias_revoke, "_alias_state", AsyncMock(return_value=(1, 1, 0)))
    session = Mock()
    session.execute = AsyncMock(
        side_effect=(
            _query_result(),
            _query_result(),
            _query_result(),
            _query_result(first=active_alias),
        )
    )

    with pytest.raises(RuntimeError, match=error_message):
        await alias_revoke._lock_active_alias(
            session,
            schema="mrf",
            aliases='"mrf"."address_alias_v1"',
            source_key="00000000-0000-0000-0000-000000000001",
            target_key="00000000-0000-0000-0000-000000000002",
            timeout="30s",
        )


def _backfill_runner():
    request = strict_backfill.StrictSourceBackfillRequest(
        alias_run_id="00000000-0000-0000-0000-000000000001",
        expected_candidate_sha256="a" * 64,
        reviewed_by="synthetic-reviewer",
        max_targets=1,
    )
    runner = strict_backfill._StrictSourceBackfillRunner(request)
    runner.execution = strict_backfill._BackfillExecution(
        request=request,
        schema="mrf",
        shadow_run_id=request.alias_run_id,
        reviewed_digest=request.expected_candidate_sha256,
        reviewer=request.reviewed_by,
        target_limit=1,
        timeout="10min",
        shadow_by_field={},
        run_id="00000000-0000-0000-0000-000000000002",
        archive='"mrf"."address_archive_v2"',
    )
    return runner


@pytest.mark.asyncio
async def test_backfill_runner_rejects_changed_shadow_and_target_overflow():
    runner = _backfill_runner()
    session = Mock()
    session.execute = AsyncMock(return_value=_query_result(first=None))
    with pytest.raises(RuntimeError, match="reviewed shadow changed"):
        await runner._validate_reviewed_shadow(session)

    session.execute = AsyncMock(
        side_effect=(
            _query_result(),
            _query_result(),
            _query_result(),
            _query_result(scalar=2),
        )
    )
    with pytest.raises(RuntimeError, match="exceeds max_targets"):
        await runner._materialize_targets(session)


@pytest.mark.asyncio
async def test_backfill_runner_requires_preparation_and_checks_cancellation():
    unprepared = strict_backfill._StrictSourceBackfillRunner(
        strict_backfill.StrictSourceBackfillRequest(
            alias_run_id="00000000-0000-0000-0000-000000000001",
            expected_candidate_sha256="a" * 64,
            reviewed_by="synthetic-reviewer",
        )
    )
    with pytest.raises(RuntimeError, match="was not prepared"):
        unprepared._required_execution()

    runner = _backfill_runner()
    cancel_check = AsyncMock()
    runner.request = strict_backfill.StrictSourceBackfillRequest(
        alias_run_id=runner.request.alias_run_id,
        expected_candidate_sha256=runner.request.expected_candidate_sha256,
        reviewed_by=runner.request.reviewed_by,
        cancel_check=cancel_check,
    )
    await runner._check_cancelled()
    cancel_check.assert_awaited_once()
