# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Timeout terminals drain known work without claiming graph completeness."""

from __future__ import annotations

from dataclasses import replace
import sqlite3

import pytest

from process.provider_directory_rooted_graph_acquisition_runtime import (
    ProviderDirectoryRootedGraphAcquisitionError,
    provider_directory_rooted_graph_census_state,
)
from process.provider_directory_rooted_graph_http import (
    ProviderDirectoryRootedGraphHTTPError,
    fetch_provider_directory_rooted_graph_query,
)
from process.provider_directory_rooted_graph_persistence_sql import (
    _terminal_work_sql,
    root_closure_sql,
)
from tests.provider_directory_rooted_graph_acquisition_test_support import identity
from tests.provider_directory_rooted_graph_runtime_test_support import RuntimeHarness
from tests.test_provider_directory_rooted_graph_acquisition_boundaries import _Database
from tests.test_provider_directory_rooted_graph_worker_boundaries import _runner
from tests.test_provider_directory_rooted_graph_http_adaptive_pages import (
    FakeSession,
    request_sizes,
    timeout_response,
)


@pytest.mark.asyncio
async def test_exhausted_page_ladder_terminalizes_without_outer_retry() -> None:
    harness = RuntimeHarness()
    session = FakeSession([timeout_response() for _ in range(7)])
    runner = _runner(
        harness,
        max_attempts=8,
        dependencies=replace(
            harness.dependencies(), fetch=fetch_provider_directory_rooted_graph_query
        ),
    )
    assert (
        await runner.process_claim(session, harness._claims("baseline")["role"]) is None
    )
    assert request_sizes(session) == [100, 50, 25, 12, 6, 3, 1]
    assert harness.events == [("error", identity().acquisition_id, "transport_timeout")]


@pytest.mark.asyncio
async def test_timeout_retries_create_one_error_and_drain_remaining_known_work() -> (
    None
):
    harness = RuntimeHarness()
    root = identity()
    claims = harness._claims("baseline")
    harness.generic_pending[root.acquisition_id].extend(
        [claims["role"], claims["direct"]]
    )

    async def timeout_role(session, api_base, claim, *, bounds):
        response = await harness.fetch(session, api_base, claim, bounds=bounds)
        if claim.query_id == claims["role"].query_id:
            raise ProviderDirectoryRootedGraphHTTPError(
                "transport_timeout", retryable=True
            )
        return response

    runner = _runner(
        harness,
        max_attempts=2,
        dependencies=replace(harness.dependencies(), fetch=timeout_role),
    )
    await runner.drain_generic_frontier({"session_id": 1})

    assert harness.fetch_attempts[claims["role"].query_id] == 2
    assert harness.fetch_attempts[claims["direct"].query_id] == 1
    assert harness.generic_pending[root.acquisition_id] == []
    assert [event for event in harness.events if event[0] == "error"] == [
        ("error", root.acquisition_id, "transport_timeout")
    ]
    assert sum(event[0] == "release" for event in harness.events) == 1
    assert sum(event[0] == "missing" for event in harness.events) == 1
    assert not any(event[0] in {"complete", "seal"} for event in harness.events)


@pytest.mark.asyncio
@pytest.mark.parametrize("claim_name", ("role", "direct", "census"))
async def test_timeout_terminal_materializes_without_release_or_descendants(
    claim_name,
) -> None:
    harness = RuntimeHarness()

    async def timeout_fetch(*_args, **_kwargs):
        raise ProviderDirectoryRootedGraphHTTPError("transport_timeout", retryable=True)

    runner = _runner(
        harness,
        max_attempts=1,
        dependencies=replace(harness.dependencies(), fetch=timeout_fetch),
    )
    outcome = await runner.process_claim(
        {"session_id": 1}, harness._claims("baseline")[claim_name]
    )
    assert outcome is None
    assert harness.events == [("error", identity().acquisition_id, "transport_timeout")]
    assert not harness.generic_pending


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("error_code", "retryable", "terminal_code"),
    (
        ("http_transient", True, "retry_exhausted"),
        ("content_type_invalid", True, "retry_exhausted"),
        ("resource_limit", False, "resource_limit"),
    ),
)
async def test_non_timeout_terminal_still_aborts(
    error_code, retryable, terminal_code
) -> None:
    harness = RuntimeHarness()

    async def fail_fetch(*_args, **_kwargs):
        raise ProviderDirectoryRootedGraphHTTPError(error_code, retryable=retryable)

    runner = _runner(
        harness,
        max_attempts=1,
        dependencies=replace(harness.dependencies(), fetch=fail_fetch),
    )
    with pytest.raises(ProviderDirectoryRootedGraphAcquisitionError):
        await runner.process_claim(
            {"session_id": 1}, harness._claims("baseline")["role"]
        )
    assert harness.events == [("error", identity().acquisition_id, terminal_code)]


@pytest.mark.asyncio
async def test_census_resume_accepts_only_proven_timeout_skip_without_refetch() -> None:
    harness = RuntimeHarness()
    harness.census_status[identity().acquisition_id] = "timeout_skipped"
    await _runner(harness).process_census({"session_id": 1})
    assert harness.events == [("claim_census", "baseline", "timeout_skipped")]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("error_code", "expected_state"),
    (
        ("transport_timeout", "timeout_skipped"),
        ("retry_exhausted", "error"),
        (None, "error"),
    ),
)
async def test_census_state_preserves_timeout_provenance(
    error_code, expected_state
) -> None:
    state = await provider_directory_rooted_graph_census_state(
        identity().acquisition_id,
        database=_Database(
            {
                "census_count": 1,
                "census_status": "error",
                "census_error_code": error_code,
            }
        ),
    )
    assert state == expected_state


@pytest.mark.parametrize(
    ("status", "error_code", "expected"),
    (
        ("completed", None, 1),
        ("error", "transport_timeout", 1),
        ("error", "retry_exhausted", 0),
        ("error", "response_invalid", 0),
        ("error", None, 0),
        ("pending", "transport_timeout", 0),
        ("leased", "transport_timeout", 0),
    ),
)
def test_drained_predicate_accepts_only_complete_or_proven_timeout(
    status, error_code, expected
) -> None:
    with sqlite3.connect(":memory:") as database:
        observed = database.execute(
            f"SELECT {_terminal_work_sql('work')} FROM "
            "(SELECT ? AS status, ? AS error_code) AS work",
            (status, error_code),
        ).fetchone()[0]
    assert observed == expected


def test_root_frontier_proof_covers_known_queries_without_claiming_completeness() -> (
    None
):
    sql = root_closure_sql()
    assert "AS root_frontier_drained" in sql
    assert "root_closure_complete" not in sql
    for alias in ("root_query", "target_query", "affiliation_query"):
        assert _terminal_work_sql(alias) in sql
    assert "source_query.status = 'completed'" in sql
    assert "organization_query.status = 'completed'" in sql
