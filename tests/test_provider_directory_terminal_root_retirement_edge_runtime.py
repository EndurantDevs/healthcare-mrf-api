# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Fail-closed edge coverage for terminal-root selection and storage."""

from __future__ import annotations

from copy import deepcopy
from types import SimpleNamespace
from typing import Any, Callable
from unittest.mock import AsyncMock

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
    database.target["publication_metadata_json"][contract.RETIREMENT_METADATA_KEY] = marker

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
    from tests.test_provider_directory_generic_source_policy import synthetic_source_database

    database = StoreDatabase()
    async with synthetic_source_database(monkeypatch, database) as database:
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


def _terminal_run() -> dict[str, Any]:
    return {
        "run_id": "run_" + "a" * 32,
        "importer": "provider-directory-fhir",
        "status": "canceled",
        "metrics": {
            "job_id": "b" * 32,
            "queue": "arq:ProviderDirectoryFHIR",
            "function": "control_single_job_start",
        },
    }


def _install_queue_presence(monkeypatch, present=(), worker=None):
    from api import control_imports, control_workers

    probes = []

    class PresencePipeline:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_error):
            return None

        def zscore(self, queue, job_id):
            self.queue, self.job_id = queue, job_id

        def exists(self, _key):
            return None

        async def execute(self):
            probes.append((self.queue, self.job_id))
            return [
                0 if (self.job_id, "queue_member") in present else None,
                *[
                    int((self.job_id, field) in present)
                    for field, _key in control_imports._arq_evidence_keys(self.job_id)
                ],
            ]

    redis = SimpleNamespace(pipeline=lambda **_kwargs: PresencePipeline(), aclose=AsyncMock())
    monkeypatch.setattr(control_imports, "create_pool", AsyncMock(return_value=redis))
    monkeypatch.setattr(
        control_workers,
        "exact_worker_presence",
        lambda _payload: worker if worker is not None else {"enabled": True, "stopped": True},
    )
    return probes


@pytest.mark.asyncio
async def test_retirement_checks_canonical_and_legacy_terminal_jobs(monkeypatch):
    from process.provider_directory_terminal_root_retirement_runtime import require_stopped_lineage

    run = _terminal_run()
    before = deepcopy(run)
    probes = _install_queue_presence(monkeypatch)

    await require_stopped_lineage((run,))

    assert probes == [
        ("arq:ProviderDirectoryFHIR", "provider_directory_start_" + run["run_id"]),
        ("arq:ProviderDirectoryFHIR", run["metrics"]["job_id"]),
    ]
    assert run == before


@pytest.mark.asyncio
@pytest.mark.parametrize("identity", ["canonical", "missing", "no_metrics"])
async def test_retirement_preserves_canonical_job_lookup(monkeypatch, identity):
    from process.provider_directory_terminal_root_retirement_runtime import require_stopped_lineage

    run = _terminal_run()
    canonical = "provider_directory_start_" + run["run_id"]
    if identity == "canonical":
        run["metrics"]["job_id"] = canonical
    elif identity == "missing":
        del run["metrics"]["job_id"]
    else:
        run["metrics"] = None
    probes = _install_queue_presence(monkeypatch)

    await require_stopped_lineage((run,))

    assert probes == [("arq:ProviderDirectoryFHIR", canonical)]


@pytest.mark.asyncio
@pytest.mark.parametrize("legacy", [False, True])
@pytest.mark.parametrize("field", ["queue_member", "job", "retry", "in_progress"])
async def test_retirement_rejects_executable_state_under_either_job_id(monkeypatch, legacy, field):
    from process.provider_directory_terminal_root_retirement_runtime import require_stopped_lineage

    run = _terminal_run()
    job_id = run["metrics"]["job_id"] if legacy else "provider_directory_start_" + run["run_id"]
    _install_queue_presence(monkeypatch, present={(job_id, field)})

    with pytest.raises(contract.TerminalRootRetirementError, match="busy"):
        await require_stopped_lineage((run,))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("section", "field", "value"),
    [
        ("metrics", "queue", "arq:Other"),
        ("metrics", "function", "other_start"),
        ("run", "status", "running"),
        ("run", "importer", "nucc"),
        ("run", "run_id", ""),
        ("run", "run_id", None),
        ("run", "run_id", " padded "),
    ],
)
async def test_retirement_rejects_unbound_legacy_job_identity(monkeypatch, section, field, value):
    from process.provider_directory_terminal_root_retirement_runtime import require_stopped_lineage

    run = _terminal_run()
    (run["metrics"] if section == "metrics" else run)[field] = value
    probes = _install_queue_presence(monkeypatch)

    with pytest.raises(contract.TerminalRootRetirementError, match="state_invalid"):
        await require_stopped_lineage((run,))
    assert probes == []


@pytest.mark.asyncio
async def test_retirement_rejects_unavailable_legacy_adapter(monkeypatch):
    from api import control_imports
    from process.provider_directory_terminal_root_retirement_runtime import require_stopped_lineage

    probes = _install_queue_presence(monkeypatch)
    monkeypatch.setattr(control_imports, "_adapter_for_import_row", lambda _run: None)

    with pytest.raises(contract.TerminalRootRetirementError, match="state_invalid"):
        await require_stopped_lineage((_terminal_run(),))
    assert probes == []


@pytest.mark.asyncio
async def test_retirement_retains_terminal_results_and_requires_stopped_worker(monkeypatch):
    from process.provider_directory_terminal_root_retirement_runtime import require_stopped_lineage

    run = _terminal_run()
    _install_queue_presence(monkeypatch, present={(run["metrics"]["job_id"], "result")})
    await require_stopped_lineage((run,))

    _install_queue_presence(monkeypatch, worker={"enabled": True, "stopped": False})
    with pytest.raises(contract.TerminalRootRetirementError, match="busy"):
        await require_stopped_lineage((run,))


@pytest.mark.asyncio
async def test_canonical_worker_lookup_still_rejects_conflicting_legacy_id(monkeypatch):
    from api.control_imports import StaleWorkerReconciliationConflict, _arq_worker_presence

    probes = _install_queue_presence(monkeypatch)
    with pytest.raises(StaleWorkerReconciliationConflict, match="persisted ARQ job ID conflicts"):
        await _arq_worker_presence(_terminal_run())
    assert probes == []
