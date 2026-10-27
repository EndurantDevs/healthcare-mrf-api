# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Failure marking counts only exact isolated committed keys and never publishes."""

import asyncio
import datetime
from contextlib import asynccontextmanager
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_generic_source_policy as source_policy
from process import provider_directory_source_failure_control as failure
from process.provider_directory_generic_candidate import GenericCandidateError
from process.provider_directory_generic_source_policy import GenericSourcePolicy
from tests.test_provider_directory_generic_source_storage import _case


@pytest.mark.asyncio
async def test_missing_genuine_factory_refuses_before_any_target_session():
    candidate, directory, _ = _case()
    directory.db = SimpleNamespace(session=AsyncMock(side_effect=AssertionError("target reached")))
    with pytest.raises(GenericCandidateError, match="publisher_connection_required"):
        await failure.mark_source_candidate(directory, candidate, "failed", {})
    directory.db.session.assert_not_awaited()


@pytest.fixture
def case(monkeypatch):
    candidate, directory, header = _case()
    directory.db = object()
    directory._json_default = lambda value: value.isoformat()
    directory.IMMUTABLE_ENDPOINT_DATASET_STATUSES = ("validated", "published", "superseded")
    connection = SimpleNamespace(
        fetchrow=AsyncMock(return_value=header),
        fetchval=AsyncMock(return_value=3),
        execute=AsyncMock(return_value="UPDATE 1"),
    )
    publisher = SimpleNamespace(
        connection=connection,
        policy=GenericSourcePolicy("source_schema", "source_owner", "source_publisher", ("source_reader",)),
        storage_binding=None,
    )
    events = []

    @asynccontextmanager
    async def original_transaction(database):
        assert database is directory.db
        events.append("original_transaction")
        try:
            yield
        except BaseException:
            events.append("rollback")
            raise

    monkeypatch.setattr(failure, "source_transaction", original_transaction)
    monkeypatch.setattr(failure, "bound_publisher", lambda: publisher)
    monkeypatch.setattr(source_policy, "bound_publisher", lambda: publisher)
    monkeypatch.setattr(failure.storage, "_verify_tables", AsyncMock())
    return SimpleNamespace(
        candidate=candidate,
        directory=directory,
        header=header,
        native=connection,
        publisher=publisher,
        events=events,
    )


@pytest.mark.asyncio
async def test_isolated_failure_marks_with_trusted_source_schema(case):
    case.directory._schema = lambda: "catalog"
    with pytest.raises(GenericCandidateError, match="schema_changed"):
        await failure.mark_source_candidate(case.directory, case.candidate, "failed", {})
    case.native.fetchrow.assert_not_awaited()

    case.publisher.storage_binding = SimpleNamespace(catalog_schema="catalog", storage_schema="other")
    with pytest.raises(GenericCandidateError, match="schema_changed"):
        await failure.mark_source_candidate(case.directory, case.candidate, "failed", {})

    case.publisher.storage_binding = SimpleNamespace(catalog_schema="catalog", storage_schema="source_schema")
    await failure.mark_source_candidate(case.directory, case.candidate, "failed", {})
    case.native.execute.assert_awaited_once()
    assert "publication_metadata_json::jsonb IS NOT DISTINCT FROM" in case.native.execute.await_args.args[0]


@pytest.mark.asyncio
async def test_failure_uses_one_complete_key_aggregate_with_resets_and_no_preparation(case):
    binding = case.header["candidate_storage_json"]
    binding.update(ordinal=8, resets={"*": 2, "Organization": 5})
    await failure.mark_source_candidate(case.directory, case.candidate, "failed", {"error": "bounded failure"})
    query, dataset_id, global_cutoff, resets, ordinal = case.native.fetchval.await_args.args
    assert "DISTINCT ON" in query and "batch_ordinal DESC,row_ordinal DESC" in query
    assert "greatest" in query and "NOT LIKE 'LU:%:pass:%'" in query
    assert (dataset_id, global_cutoff, ordinal) == ("dataset", 2, 8)
    assert failure.storage._json(resets) == {"*": 2, "Organization": 5}
    case.native.fetchval.assert_awaited_once()
    assert case.native.execute.await_args.args[3] == 3
    assert case.events == ["original_transaction"]
    assert "CREATE" not in case.native.execute.await_args.args[0]


@pytest.mark.asyncio
async def test_failure_diagnostics_keep_the_existing_metadata_encoder(case):
    observed_at = datetime.datetime(2026, 1, 1, 12)
    await failure.mark_source_candidate(case.directory, case.candidate, "failed", {"observed_at": observed_at})
    assert failure.storage._json(case.native.execute.await_args.args[4]) == {"observed_at": observed_at.isoformat()}


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["absent", "endpoint", "root", "current", "validated", "published"])
async def test_historical_noop_cannot_count_or_change_an_immutable_or_other_candidate(case, change):
    match change:
        case "absent":
            case.native.fetchrow.return_value = None
        case "endpoint":
            case.header["endpoint_id"] = "other"
        case "root":
            case.header["acquisition_root_run_id"] = "other"
        case "current":
            case.header["is_current"] = True
        case _:
            case.header["status"] = change
    await failure.mark_source_candidate(case.directory, case.candidate, "failed", {})
    case.native.fetchval.assert_not_awaited()
    case.native.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["binding_identity", "metadata", "compare", "cancel"])
async def test_mismatched_identity_cas_or_cancellation_rolls_back_before_any_partial_marker(case, change):
    match change:
        case "binding_identity":
            case.header["candidate_storage_json"] = deepcopy(case.header["candidate_storage_json"])
            case.header["candidate_storage_json"]["identity"]["metadata"]["resource_hash_contract"] = "other"
        case "metadata":
            case.header["publication_metadata_json"] = {"source_ids": ["other"]}
        case "compare":
            case.native.execute.return_value = "UPDATE 0"
        case "cancel":
            case.native.fetchval.side_effect = asyncio.CancelledError
    expected = asyncio.CancelledError if change == "cancel" else GenericCandidateError
    with pytest.raises(expected):
        await failure.mark_source_candidate(case.directory, case.candidate, "incomplete", {})
    assert case.events[-1] == "rollback"
    if change != "compare":
        case.native.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_prepared_count_uses_verified_completed_output_without_raw_duplicates(case):
    binding = case.header["candidate_storage_json"]
    binding["phase"] = "prepared"
    for index, root in enumerate(failure.storage.RELATIONS):
        binding["tables"][root] = dict(
            name="pdgs_" + "b" * 32 + "_" + failure.storage._SUFFIXES[root],
            oid=20 + index,
            owner=100,
            layout_sha256="c" * 64,
        )
    await failure.mark_source_candidate(case.directory, case.candidate, "incomplete", {})
    query, dataset_id = case.native.fetchval.await_args.args
    assert '"pdgs_' + "b" * 32 + '_r"' in query and dataset_id == "dataset" and "DISTINCT" not in query
    failure.storage._verify_tables.assert_awaited_once()
