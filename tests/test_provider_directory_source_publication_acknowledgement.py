# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Protected publication completion closes only the exact mutable private parent."""

import asyncio
import importlib
from contextlib import asynccontextmanager
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_generic_source_completion as completion
from process import provider_directory_source_failure_control as control
from process.provider_directory_generic_candidate import GenericCandidateError
from process.provider_directory_source_storage_binding import FHIRSourceCandidateHandoff
from tests.test_provider_directory_generic_source_completion import _prepared_detached
from tests.test_provider_directory_generic_source_storage import harness


@asynccontextmanager
async def _acknowledgement_case(harness, monkeypatch):
    async with _prepared_detached(harness, monkeypatch) as (_, binding):
        handoff = await completion.prepare_generic_source_handoff(completion.bound_publisher(), binding)
        frozen = deepcopy(harness.header)
        frozen.update(
            dataset_id="dataset", validated_at="2026-01-01T00:00:00+00:00", published_at=None, superseded_at=None
        )
        harness.directory.db = object()
        events = []

        @asynccontextmanager
        async def transaction(database):
            assert database is harness.directory.db
            events.append("begin")
            try:
                yield
            except BaseException:
                events.append("rollback")
                raise
            else:
                events.append("commit")

        monkeypatch.setattr(control, "source_transaction", transaction)
        harness.connection.fetchrow = AsyncMock(return_value={"status": "validated", "matches": True})
        harness.connection.execute = AsyncMock(return_value="UPDATE 1")
        yield SimpleNamespace(harness=harness, handoff=handoff, frozen=frozen, events=events)


@pytest.mark.asyncio
async def test_status_only_acknowledgement_is_exact_and_replayable(harness, monkeypatch):
    async with _acknowledgement_case(harness, monkeypatch) as case:
        original = deepcopy(case.frozen)
        await control.acknowledge_source_publication(harness.directory, case.handoff, case.frozen)
        query, dataset_id, native_header = harness.connection.fetchrow.await_args.args
        assert "jsonb_populate_record" in query and "to_jsonb(parent)-'status'" in query
        assert "FOR UPDATE OF parent" in query and "jsonb_object_keys" in query
        assert dataset_id == "dataset" and control.storage._json(native_header) == original
        assert harness.connection.execute.await_args.args == (
            'UPDATE "source_schema"."provider_directory_endpoint_dataset" '
            "SET status='publication_acknowledged' WHERE dataset_id=$1 AND status='validated'",
            "dataset",
        )
        harness.connection.fetchrow.return_value = {"status": "publication_acknowledged", "matches": True}
        await control.acknowledge_source_publication(harness.directory, case.handoff, case.frozen)
        harness.connection.execute.assert_awaited_once()
        assert case.frozen == original and case.frozen["candidate_storage_json"] == case.handoff.document
        assert case.events == ["begin", "commit", "begin", "commit"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("status", "publication_acknowledged"),
        ("is_current", True),
        ("validated_at", None),
        ("published_at", "2026-01-01T00:00:00+00:00"),
        ("superseded_at", "2026-01-01T00:00:00+00:00"),
        ("dataset_id", "other"),
        ("endpoint_id", "other"),
        ("acquisition_root_run_id", "other"),
        ("dataset_hash", "d" * 64),
        ("resource_count", 3),
        ("content_proof_admission_kind", "unverified"),
        ("content_proof_admission_version", None),
        ("candidate_storage_json", {}),
        ("publication_metadata_json", {"source_ids": ["other"]}),
    ],
)
async def test_unverified_frozen_header_refuses_before_parent_lock(harness, monkeypatch, field, value):
    async with _acknowledgement_case(harness, monkeypatch) as case:
        case.frozen[field] = value
        with pytest.raises(GenericCandidateError, match="publication_acknowledgement_header_changed"):
            await control.acknowledge_source_publication(harness.directory, case.handoff, case.frozen)
        harness.connection.fetchrow.assert_not_awaited()
        harness.connection.execute.assert_not_awaited()
        assert case.events == ["begin", "rollback"]


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["missing", "row", "status", "compare", "cancel"])
async def test_parent_mismatch_or_cancel_never_commits_acknowledgement(harness, monkeypatch, change):
    async with _acknowledgement_case(harness, monkeypatch) as case:
        match change:
            case "missing":
                harness.connection.fetchrow.return_value = None
            case "row":
                harness.connection.fetchrow.return_value["matches"] = False
            case "status":
                harness.connection.fetchrow.return_value["status"] = "failed"
            case "compare":
                harness.connection.execute.return_value = "UPDATE 0"
            case _:
                harness.connection.execute.side_effect = asyncio.CancelledError
        exception = asyncio.CancelledError if change == "cancel" else GenericCandidateError
        with pytest.raises(exception):
            await control.acknowledge_source_publication(harness.directory, case.handoff, case.frozen)
        assert case.events == ["begin", "rollback"]
        if change not in {"compare", "cancel"}:
            harness.connection.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_acknowledgement_requires_exact_handoff_and_original_issued_producer(harness, monkeypatch):
    async with _acknowledgement_case(harness, monkeypatch) as case:
        with pytest.raises(GenericCandidateError, match="publication_acknowledgement_handoff_required"):
            await control.acknowledge_source_publication(harness.directory, case.handoff.document, case.frozen)
        assert case.events == []
        document = case.handoff.document
        document["storage"]["producer"][0] = "b" * 40
        other = FHIRSourceCandidateHandoff(control.storage._encoded(document))
        with pytest.raises(GenericCandidateError, match="storage_binding_changed"):
            await control.acknowledge_source_publication(harness.directory, other, case.frozen)
        harness.connection.fetchrow.assert_not_awaited()
        harness.connection.execute.assert_not_awaited()


def test_acknowledged_parent_is_immutable_to_existing_fhir_writers():
    directory = importlib.import_module("process.provider_directory_fhir")

    assert directory.ENDPOINT_DATASET_PUBLICATION_ACKNOWLEDGED == "publication_acknowledged"
    assert directory.ENDPOINT_DATASET_PUBLICATION_ACKNOWLEDGED in directory.IMMUTABLE_ENDPOINT_DATASET_STATUSES
