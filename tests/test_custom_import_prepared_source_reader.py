# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Prepared SOURCE oracle parity, alias refusal and bounded detached reads."""

from __future__ import annotations

import hashlib
from dataclasses import replace
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process.custom_import import build_source as staging
from process.custom_import import prepared_source_reader as reader
from process.custom_import import source_batch
from process.custom_import.capture import DecodedRecord
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.prepared_source_codec import PREPARED_SOURCE_CONTRACT, iter_prepared_source_parts
from process.custom_import.prepared_source_metadata import PreparedPartMetadata
from process.custom_import.runner_types import CandidateRunnerError
from tests.test_custom_import_build_source import _request
from tests.test_custom_import_definition import _raw_definition
from tests.test_custom_import_prepared_source_metadata import _policy


def _parts(values, policy):
    return tuple(
        iter_prepared_source_parts(
            (DecodedRecord(index, row) for index, row in enumerate(values, 1)),
            limits=policy.capture_limits,
            part_byte_limit=policy.prepared["part_byte_limit"],
            part_row_limit=policy.prepared["part_row_limit"],
        )
    )


def _context():
    request = _request(definition=CustomImportDefinition.from_mapping(_raw_definition()), page_row_limit=1)
    return staging._StreamContext(request, None, 11, request.definition.source_streams[0])


@pytest.mark.parametrize("name", ["Synthetic", 12, True, Decimal("12.00"), None, "missing"])
def test_mapped_original_values_preserve_canonical_rows_and_family_rejections(name):
    context, policy = _context(), _policy()
    values_by_label = {"Provider ID": "1234567893"}
    values_by_field = {"npi": "1234567893"}
    if name != "missing":
        values_by_label["Provider Name"], values_by_field["display_name"] = name, name
    encoded = _parts([values_by_label], policy)[0]
    part = reader.PreparedReplayPart(1, 1, 1, encoded.payload, 1)
    pages = list(reader.source_pages(context, part, policy, source_batch.SourceCursor(1, 0, 0, 0)))
    assert len(pages) == 1 and (pages[0].first_row, pages[0].first_source) == (0, 0)
    expected = replace(pages[0], records=(staging._prepare_row(context.request, context.stream, values_by_field),))
    assert staging._prepared_fingerprint(pages[0], 0) == staging._prepared_fingerprint(expected, 0)
    assert pages[0].records[0].byte_count == expected.records[0].byte_count


def test_alias_collision_refuses_before_exposing_the_previous_full_page():
    policy = _policy()
    encoded = _parts(
        [
            {"Provider ID": "1234567893", "Provider Name": "Synthetic"},
            {"Provider ID": "1234567893", "npi": "1234567893", "Provider Name": "Synthetic"},
        ],
        policy,
    )[0]
    pages = reader.source_pages(
        _context(),
        reader.PreparedReplayPart(1, 1, 2, encoded.payload, 1),
        policy,
        source_batch.SourceCursor(1, 0, 0, 0),
    )
    with pytest.raises(CandidateRunnerError, match="alias_collision"):
        next(pages)


def test_late_part_offset_rejects_forged_cursor():
    context, policy = _context(), _policy(part_row_limit=1)
    parts = _parts([{"Provider ID": "1234567893", "Provider Name": str(index)} for index in range(3)], policy)
    part = reader.PreparedReplayPart(3, 3, 1, parts[2].payload, 3)
    pages = list(reader.source_pages(context, part, policy, source_batch.SourceCursor(3, 0, 2, 2)))
    assert len(pages) == 1 and (pages[0].part_ordinal, pages[0].first_row, pages[0].first_source) == (3, 0, 2)
    with pytest.raises(CandidateRunnerError, match="replay_invalid"):
        list(reader.source_pages(context, part, policy, source_batch.SourceCursor(3, 0, 1, 2)))


@pytest.mark.parametrize("oversized", [False, True])
async def test_reader_fetches_only_the_exact_part_payload_after_metadata_bounds(monkeypatch, oversized):
    policy, part_payload = _policy(), b"synthetic bounded payload"
    context = SimpleNamespace(
        execution_id=4, dataset_id=1, definition_revision_id=2, schema_revision_id=3, policy=policy
    )
    request = SimpleNamespace(prepared_source=context, statement_timeout_ms=1000)
    byte_count = policy.prepared["part_byte_limit"] + 1 if oversized else len(part_payload)
    metadata = PreparedPartMetadata(1, 2, 3, 1, byte_count, hashlib.sha256(part_payload).digest())
    metadata_row = (
        1,
        2,
        3,
        1,
        2,
        3,
        1,
        byte_count,
        metadata.payload_sha256,
        PREPARED_SOURCE_CONTRACT,
        metadata.canonical,
        metadata.digest,
    )
    session = SimpleNamespace(
        execute=AsyncMock(return_value=SimpleNamespace(one_or_none=lambda: metadata_row)),
        scalar=AsyncMock(return_value=part_payload),
    )
    monkeypatch.setattr(reader, "sealed_header", AsyncMock(return_value=(None, (SimpleNamespace(part_count=2),))))
    timeout = AsyncMock()
    monkeypatch.setattr(source_batch, "_read_timeout", timeout)
    call = reader.open_prepared_cursor_part(
        session, request=request, stream_slot=1, expected_cursor=source_batch.SourceCursor(2, 0, 2, 2), deadline=100
    )
    if oversized:
        with pytest.raises(CandidateRunnerError, match="replay_invalid"):
            await call
        session.scalar.assert_not_awaited()
    else:
        assert await call == reader.PreparedReplayPart(2, 3, 1, part_payload, 2)
        assert timeout.await_count == 2
        payload_query = session.scalar.await_args.args[0]
        assert [column.name for column in payload_query.selected_columns] == ["payload"]
        params = payload_query.compile().params
        assert params["part_ordinal_1"] == 2 and params["execution_id_1"] == 4 and params["stream_slot_1"] == 1
        assert params["payload_sha256_1"] == metadata.payload_sha256
    metadata_query = session.execute.await_args.args[0]
    assert "payload" not in {column.name for column in metadata_query.selected_columns}
    assert metadata_query.compile().params == {"execution_id_1": 4, "stream_slot_1": 1, "part_ordinal_1": 2}
