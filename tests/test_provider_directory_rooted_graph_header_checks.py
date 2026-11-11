# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Header CHECK handling keeps exact callable custody and PostgreSQL CHECK semantics."""

import copy
import json
from functools import lru_cache
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_rooted_graph_header_checks as checks
from process.provider_directory_generic_candidate import GenericCandidateError
from tests.test_acquisition_validator_argument_types_migration import _current_functions
from tests.test_provider_directory_source_callable_custody import _entry

SCHEMA = "snapshot_schema"


@lru_cache
def _coverage_body():
    bodies = [attributes[-1] for (name, _), attributes in _current_functions().items() if name == checks.COVERAGE]
    assert len(bodies) == 1
    return bodies[0]


@pytest.fixture
def header_snapshot(monkeypatch):
    # A small synthetic CHECK isolates orchestration from the native full-header proof.
    expression = f'"{SCHEMA}"."{checks.COVERAGE}"(request_failure_coverage)'
    definition = "CHECK (" + expression + ")"
    entry = _entry(checks.COVERAGE + "(jsonb)")
    entry["definition"]["prosrc"] = _coverage_body()
    snapshot_by_field = {
        "definition": definition,
        "expression": expression,
        "function": entry["definition"],
        "language": entry["language"],
        "owner": 103,
        "namespace": 201,
    }
    monkeypatch.setattr(checks, "CHECK_SHA256", checks._constraint_digest(definition, SCHEMA))
    return snapshot_by_field


@pytest.mark.parametrize("qualifier", ['"snapshot_schema"."', '"snapshot_schema".', "snapshot_schema.", ""])
def test_constraint_digest_normalizes_only_the_known_coverage_callable(qualifier):
    qualified = qualifier + checks.COVERAGE + ('"' if qualifier.endswith('."') else "")
    definition = "CHECK (" + qualified + "(value))"
    reference = checks._constraint_digest("CHECK (" + checks.COVERAGE + "(value))", SCHEMA)
    assert checks._constraint_digest(definition, SCHEMA) == reference
    assert checks._constraint_digest(definition + " AND true", SCHEMA) != reference


@pytest.mark.asyncio
@pytest.mark.parametrize("encoded", [False, True])
async def test_header_check_capture_authenticates_actual_existing_coverage_callable(header_snapshot, encoded):
    value = json.dumps(header_snapshot) if encoded else header_snapshot
    connection = SimpleNamespace(fetchval=AsyncMock(return_value=value))
    assert await checks.capture_rooted_header_check(connection, SCHEMA) == header_snapshot


@pytest.mark.asyncio
@pytest.mark.parametrize("mutation", ["body", "owner", "settings", "security"])
async def test_header_capture_refuses_callable_or_owner_drift(header_snapshot, mutation):
    snapshot = copy.deepcopy(header_snapshot)
    if mutation == "body":
        snapshot["function"]["prosrc"] += "\n-- changed body\n"
    elif mutation == "owner":
        snapshot["owner"] = 999
    elif mutation == "settings":
        snapshot["function"]["proconfig"] = ["search_path=public"]
    else:
        snapshot["function"]["prosecdef"] = not snapshot["function"]["prosecdef"]
    connection = SimpleNamespace(fetchval=AsyncMock(return_value=snapshot))
    with pytest.raises(ValueError, match="(callable_definition|header_check_owner)_changed"):
        await checks.capture_rooted_header_check(connection, SCHEMA)


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", [False, True])
async def test_deferred_check_is_removed_only_from_exact_new_private_copy(header_snapshot, changed):
    connection = SimpleNamespace(
        fetchval=AsyncMock(side_effect=[header_snapshot, "changed" if changed else header_snapshot["definition"]]),
        execute=AsyncMock(),
    )
    if changed:
        with pytest.raises(GenericCandidateError, match="header_check_changed"):
            await checks.defer_rooted_header_check(connection, SCHEMA, "candidate", "new_header")
        connection.execute.assert_not_awaited()
    else:
        await checks.defer_rooted_header_check(connection, SCHEMA, "candidate", "new_header")
        statement = connection.execute.call_args.args[0]
        assert statement == 'ALTER TABLE "candidate"."new_header" DROP CONSTRAINT "pd_rooted_graph_dataset_check"'


@pytest.mark.asyncio
@pytest.mark.parametrize("accepted", [True, False, None])
async def test_complete_header_check_replay_requires_one_matching_row(header_snapshot, accepted):
    handoff_by_field = {
        "storage": {"catalog_schema": SCHEMA, "storage_schema": "candidate"},
        "snapshot": {"header_check": header_snapshot},
        "tables": {checks.HEADER: {"name": "new_header"}},
        "identity": {"dataset_id": "dataset"},
    }
    connection = SimpleNamespace(fetchval=AsyncMock(side_effect=[header_snapshot, accepted]))
    if accepted:
        await checks.validate_rooted_header_check(connection, handoff_by_field)
    else:
        with pytest.raises(GenericCandidateError, match="header_check_invalid"):
            await checks.validate_rooted_header_check(connection, handoff_by_field)
    statement, dataset_id = connection.fetchval.call_args.args
    assert "count(*)=1" in statement and "IS NOT FALSE" in statement and dataset_id == "dataset"
    handoff_by_field["snapshot"]["header_check"] = {}
    connection.fetchval.side_effect = [header_snapshot]
    with pytest.raises(GenericCandidateError, match="header_check_changed"):
        await checks.validate_rooted_header_check(connection, handoff_by_field)
