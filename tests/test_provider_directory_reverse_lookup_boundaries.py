# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import importlib
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest


importer = importlib.import_module("process.provider_directory_fhir")


@pytest.mark.asyncio
async def test_reverse_lookup_state_deduplicates_and_bounds_streamed_roles():
    """Count, retain, and stream each valid PractitionerRole at most once."""
    streamed_rows = SimpleNamespace(add=AsyncMock())
    state = importer._PractitionerRoleReverseLookupState(
        result_model=object,
        retained_resource_rows=[],
        streamed_rows=streamed_rows,
        retain_rows=True,
        page_limit=1,
        seen_role_ids=set(),
    )

    await state.add_seed()
    await state.mark_deadline_reached()
    await state.record_reverse_lookup_error("timeout")
    await state.add_practitioner_role_page(
        [
            {},
            {"resource_id": "role_a"},
            {"resource_id": "role_a"},
            {"resource_id": "role_b"},
        ]
    )
    await state.add_practitioner_role_page([{}, {"resource_id": "role_a"}])

    assert state.seed_count == 1
    assert state.reverse_error_count == 1
    assert state.error_message == "reverse_lookup_errors_1_last_timeout"
    assert state.is_deadline_reached is True
    assert await state.has_reached_page_limit() is True
    assert state.fetched_count == 2
    assert state.retained_resource_rows == [
        {"resource_id": "role_a"},
        {"resource_id": "role_b"},
    ]
    assert [call.args[0] for call in streamed_rows.add.await_args_list] == (
        state.retained_resource_rows
    )

    unretained_stream = SimpleNamespace(add=AsyncMock())
    unretained_state = importer._PractitionerRoleReverseLookupState(
        result_model=object,
        retained_resource_rows=[],
        streamed_rows=unretained_stream,
        retain_rows=False,
        page_limit=0,
        seen_role_ids=None,
    )
    await unretained_state.add_practitioner_role_page(
        [{"resource_id": "role_c"}]
    )
    assert unretained_state.retained_resource_rows == []
    unretained_stream.add.assert_awaited_once_with({"resource_id": "role_c"})


@pytest.mark.asyncio
async def test_reverse_lookup_checkpoints_skip_disabled_and_flush_enabled(
    monkeypatch,
):
    """Persist completed reverse seeds only when resumability is enabled."""
    upsert = AsyncMock()
    monkeypatch.setattr(importer, "_upsert_rows", upsert)
    monkeypatch.setattr(
        importer,
        "_reverse_lookup_checkpoint_flush_rows",
        lambda: 1,
    )
    monkeypatch.setattr(
        importer,
        "_reverse_lookup_checkpoint_row",
        lambda _source, resource_type, resource_id, run_id: {
            "resource_type": resource_type,
            "resource_id": resource_id,
            "run_id": run_id,
        },
    )
    state = importer._PractitionerRoleReverseLookupState(
        result_model=object,
        retained_resource_rows=[],
        streamed_rows=SimpleNamespace(add=AsyncMock()),
        retain_rows=False,
        page_limit=0,
    )
    disabled_request = SimpleNamespace(
        source={"source_id": "source_a"},
        options=SimpleNamespace(resume_completed_seeds=False, run_id="run_a"),
    )
    enabled_request = SimpleNamespace(
        source={"source_id": "source_a"},
        options=SimpleNamespace(resume_completed_seeds=True, run_id="run_a"),
    )

    await state.add_completed_seed(
        disabled_request,
        "Practitioner",
        "practitioner_disabled",
    )
    upsert.assert_not_awaited()
    await state.add_completed_seed(
        enabled_request,
        "Practitioner",
        "practitioner_enabled",
    )
    upsert.assert_awaited_once()
    assert state.completed_seed_rows == []

    state.completed_seed_rows.append({"resource_id": "pending"})
    await state.flush_completed_seed_rows()
    await state.flush_completed_seed_rows()
    assert upsert.await_count == 2


@pytest.mark.asyncio
async def test_streamed_resource_buffer_honors_writer_and_batch_boundary():
    """Flush only complete batches and count the writer's accepted rows."""
    writer = AsyncMock(return_value=2)
    buffer = importer._StreamedResourceRowBuffer(
        model=dict,
        row_batch_handler=writer,
        row_batch_size=2,
        pending_row_items=[],
    )

    await buffer.add({"resource_id": "role_a"})
    writer.assert_not_awaited()
    await buffer.add({"resource_id": "role_b"})
    writer.assert_awaited_once_with(
        dict,
        [
            {"resource_id": "role_a"},
            {"resource_id": "role_b"},
        ],
    )
    assert buffer.rows_written == 2
    await buffer.flush()
    assert writer.await_count == 1

    disabled = importer._StreamedResourceRowBuffer(
        model=dict,
        row_batch_handler=None,
        row_batch_size=1,
        pending_row_items=[],
    )
    await disabled.add({"resource_id": "ignored"})
    await disabled.flush()
    assert disabled.pending_row_items == []


def test_fhir_acquisition_context_rejects_unknown_fetch_mode():
    """Reject fetch modes outside the maintained acquisition strategies."""
    with pytest.raises(ValueError, match="unsupported FHIR fetch mode"):
        importer.FHIRAcquisitionContext(fetch_mode="unknown")
