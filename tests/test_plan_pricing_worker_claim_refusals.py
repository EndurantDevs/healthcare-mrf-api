# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Persisted pricing admission fences worker claims with explicit SQL transports."""

import json
from contextlib import asynccontextmanager
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from api import control_imports
from db.models import db
from process import control_lifecycle
from process.live_progress import current_live_progress_context
from process.plan_pricing_control_transaction import pricing_worker_claim_transaction
from tests.test_plan_pricing_admission import _managed_case, _managed_connection


def _dispatch(run):
    adapter = control_imports._SINGLE_JOB_ADAPTERS[run["importer"]]
    return control_imports._adapter_payload(adapter, run, deepcopy(run["params"]))


def _persisted_run_transport(connection, case):
    admission_fetchrow = connection.fetchrow.side_effect

    async def fetchrow(statement, *parameters):
        if "importer='plan-pricing-projection'" in statement:
            assert parameters == (case.run["run_id"],)
            return {**case.run, "params": json.dumps(case.run["params"])}
        return await admission_fetchrow(statement, *parameters)

    connection.fetchrow.side_effect = fetchrow


@pytest.fixture
def worker_claim(monkeypatch):
    """Reuse real admission validation; native catalog qualification remains doubled."""
    case = _managed_case(monkeypatch, retry=True)
    connection, observe = _managed_connection(monkeypatch, case)
    _persisted_run_transport(connection, case)
    events = []
    transaction_state = SimpleNamespace(is_active=False)
    raw_connection = SimpleNamespace(driver_connection=connection)
    sql_connection = SimpleNamespace(get_raw_connection=AsyncMock(return_value=raw_connection))
    session = SimpleNamespace(execute=AsyncMock(), connection=AsyncMock(return_value=sql_connection))

    @asynccontextmanager
    async def transaction():
        events.append("begin")
        transaction_state.is_active = True
        try:
            yield session
        except BaseException:
            events.append("rollback")
            raise
        else:
            events.append("commit")
        finally:
            transaction_state.is_active = False

    connection.is_in_transaction = lambda: transaction_state.is_active
    monkeypatch.setattr(db, "transaction", transaction)
    writes = AsyncMock(side_effect=AssertionError("unverified worker claim written"))
    import_target = Mock(side_effect=AssertionError("unverified worker target imported"))
    progress = Mock(side_effect=AssertionError("unverified worker claim announced"))
    monkeypatch.setattr(db, "execute", writes)
    monkeypatch.setattr(control_lifecycle, "import_module", import_target)
    monkeypatch.setattr(control_lifecycle, "write_live_progress", progress)
    return SimpleNamespace(
        case=case,
        connection=connection,
        observe=observe,
        events=events,
        writes=writes,
        import_target=import_target,
        progress=progress,
    )


@pytest.mark.asyncio
async def test_exact_persisted_retry_request_enters_one_owned_claim_transaction(worker_claim):
    original = deepcopy(worker_claim.case.run)
    async with pricing_worker_claim_transaction(_dispatch(original)):
        assert worker_claim.connection.is_in_transaction()
        worker_claim.events.append("claim allowed")
    assert worker_claim.events == ["begin", "claim allowed", "commit"]
    assert not worker_claim.connection.is_in_transaction()
    assert worker_claim.observe.await_count == 2
    assert worker_claim.case.run == original
    worker_claim.writes.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("replacement", ["binding", "module", "function"])
async def test_substituted_dispatch_cannot_claim_or_start_persisted_pricing_run(worker_claim, replacement):
    original_run = deepcopy(worker_claim.case.run)
    original_admission = deepcopy(worker_claim.case.admission.record_by_field)
    prior_live_context = current_live_progress_context()
    dispatch = _dispatch(original_run)
    if replacement == "binding":
        dispatch["task"]["binding_manifest_digest"] = "f" * 64
    elif replacement == "module":
        dispatch["target_module"] = "api.another_projection"
    else:
        dispatch["target_function"] = "build_another_projection"

    with pytest.raises(ValueError, match="pricing_control_worker_request_changed"):
        await control_lifecycle.control_single_job_start({}, dispatch)

    assert worker_claim.events == ["begin", "rollback"]
    assert not worker_claim.connection.is_in_transaction()
    assert worker_claim.observe.await_count == 2
    assert worker_claim.case.run == original_run
    assert worker_claim.case.admission.record_by_field == original_admission
    worker_claim.writes.assert_not_awaited()
    worker_claim.import_target.assert_not_called()
    worker_claim.progress.assert_not_called()
    assert current_live_progress_context() == prior_live_context
