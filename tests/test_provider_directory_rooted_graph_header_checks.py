# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Header CHECK handling keeps exact callable custody and PostgreSQL CHECK semantics."""

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_rooted_graph_header_checks as checks
from process.provider_directory_generic_candidate import GenericCandidateError

SCHEMA = "snapshot_schema"


@pytest.fixture
def header_snapshot(monkeypatch):
    # A small synthetic CHECK isolates orchestration from the native full-header proof.
    """Use a complete named header and a small synthetic data predicate."""
    expression = "request_attempt_count >= request_failure_count"
    monkeypatch.setattr(checks, "_header_expression", lambda schema: expression if schema == SCHEMA else "false")
    return {
        "storage": {"catalog_schema": SCHEMA, "storage_schema": "candidate"},
        "tables": {checks.HEADER: {"schema": "candidate", "name": "new_header"}},
        "identity": {"dataset_id": "dataset"},
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", [False])
async def test_deferred_check_is_removed_only_from_exact_new_private_copy(header_snapshot, changed):
    connection = SimpleNamespace(execute=AsyncMock())
    candidate_name = header_snapshot["tables"][checks.HEADER]["name"]
    await checks.defer_rooted_header_check(connection, SCHEMA, "candidate", candidate_name)
    connection.execute.assert_awaited_once_with(
        'ALTER TABLE "candidate"."new_header" DROP CONSTRAINT "pd_rooted_graph_dataset_check"'
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("accepted", [True, False, None])
async def test_complete_header_check_replay_requires_one_matching_row(header_snapshot, accepted):
    connection = SimpleNamespace(fetchval=AsyncMock(return_value=accepted))
    if accepted:
        await checks.validate_rooted_header_check(connection, header_snapshot)
    else:
        with pytest.raises(GenericCandidateError, match="header_check_invalid"):
            await checks.validate_rooted_header_check(connection, header_snapshot)
    statement, dataset_id = connection.fetchval.await_args.args
    assert "count(*)=1" in statement and "IS NOT FALSE" in statement and dataset_id == "dataset"
    header_snapshot["tables"][checks.HEADER]["name"] = "invalid;header"
    connection.fetchval.reset_mock()
    with pytest.raises(ValueError):
        await checks.validate_rooted_header_check(connection, header_snapshot)
    connection.fetchval.assert_not_awaited()
