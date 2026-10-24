# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Mocked preparation transactions and COPY wiring; no native writer proof."""

from __future__ import annotations

import datetime as dt
import hashlib
import json
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process.custom_import import prepared_source_store as store
from process.custom_import import runner_registry
from process.custom_import.capture_store import CaptureBundleConflict
from process.custom_import.execution import lease_token_sha256
from process.custom_import.object_processing_policy import ObjectProcessingPolicy
from process.custom_import.prepared_source_codec import PREPARED_SOURCE_CONTRACT, PreparedSourcePart
from process.custom_import.prepared_source_metadata import (
    PreparedPartMetadata,
    prepared_source_summary,
    summarize_prepared_stream,
)
from process.custom_import.prepared_source_progress import (
    PreparedSourceProgressError,
    append_progress,
    canonical_progress,
    close_stream,
    empty_progress,
    totals,
)
from process.custom_import.runner_types import CancellationRequested, CandidateRunnerError, LeaseAuthorityLost
from tests.test_custom_import_object_processing_policy import _document as _policy_document
from tests.test_custom_import_prepared_source_context import _load, _page_guards
from tests.test_custom_import_prepared_source_context import case as context_case


def _part(*, first=1, count=1, value="synthetic"):
    payload = b"".join(
        (json.dumps([ordinal, [["source", "string", value]]], separators=(",", ":")) + "\n").encode()
        for ordinal in range(first, first + count)
    )
    return PreparedSourcePart(first, count, payload)


def _metadata(part, *, slot=1, ordinal=1):
    return PreparedPartMetadata(
        slot,
        ordinal,
        part.first_source_ordinal,
        part.record_count,
        len(part.payload),
        hashlib.sha256(part.payload).digest(),
    )


def _counters(header):
    return header.part_count, header.record_count, header.byte_count, header.manifest_byte_count


def _install_header(case, *, progress=None, state="pending", version=5):
    context = case.context
    if progress is None:
        progress = empty_progress(len(context.stream_slots), context.policy)
    part_count, record_count, byte_count, manifest_byte_count = totals(progress)
    case.header = store.CustomImportPreparedSource(
        **vars(store.prepared_source_identity(context)),
        canonical_processing_policy=context.policy.canonical,
        encoding_contract=PREPARED_SOURCE_CONTRACT,
        state=state,
        version=version,
        producer_fence=context.fence,
        producer_token_sha256=lease_token_sha256(context.lease_token),
        preparation_deadline_at=context.build_deadline_at,
        canonical_progress=canonical_progress(progress),
        part_count=part_count,
        record_count=record_count,
        byte_count=byte_count,
        manifest_byte_count=manifest_byte_count,
    )
    return case.header


def _committed_part(case, *, part=None):
    part = _part() if part is None else part
    metadata = _metadata(part)
    progress = append_progress(
        empty_progress(len(case.context.stream_slots), case.context.policy), metadata, case.context.policy
    )
    _install_header(case, progress=progress)
    case.observed_part = _observed_row(case, metadata)
    return part, progress


def _observed_row(case, metadata):
    return (
        case.context.dataset_id,
        case.context.definition_revision_id,
        case.context.schema_revision_id,
        metadata.first_source_ordinal,
        metadata.record_count,
        metadata.byte_count,
        metadata.payload_sha256,
        PREPARED_SOURCE_CONTRACT,
        metadata.canonical,
        metadata.digest,
    )


def _complete_header(case, state):
    progress = empty_progress(len(case.context.stream_slots), case.context.policy)
    summaries = []
    for slot in range(1, len(progress) + 1):
        metadata = _metadata(_part(count=0), slot=slot)
        progress = append_progress(progress, metadata, case.context.policy)
        progress = close_stream(progress, slot, case.context.policy)
        summaries.append(summarize_prepared_stream([metadata], stream_slot=slot, policy=case.context.policy))
    header = _install_header(case, progress=progress, state=state)
    if state == "sealed":
        summary = prepared_source_summary(
            store.prepared_source_identity(case.context),
            summaries,
            stream_count=len(progress),
            policy=case.context.policy,
        )
        header.canonical_summary, header.prepared_sha256 = summary.canonical, summary.digest
        header.sealed_at = case.permit.expires_at - dt.timedelta(seconds=1)
    return header


@pytest.fixture
async def case(context_case, monkeypatch):
    """Keep real page, scope and quota checks; replace only their database endpoints."""

    context = await _load(context_case)
    _page_guards(context_case, monkeypatch, context)
    now = context_case.permit.expires_at - dt.timedelta(seconds=1)
    runner_registry.database_now.side_effect = None
    runner_registry.database_now.return_value = now
    case = SimpleNamespace(
        **vars(context_case),
        context=context,
        header=None,
        orphan_part=False,
        observed_part=None,
        retained_bytes=0,
        quota_extra=0,
        quota_calls=[],
        copied=[],
        copy_before_counts=[],
        header_queries=[],
        observed_queries=[],
        orphan_queries=[],
        now=now,
        lease=store.staging.lock_lease.return_value,
    )
    _install_store_read_endpoints(case)
    _install_store_write_endpoints(case, monkeypatch)
    case.events.clear()
    case.sessions.reset_mock()
    return case


def _install_store_read_endpoints(case):
    original_execute = case.session.execute.side_effect

    async def scalar(statement, *_args, **_kwargs):
        descriptions = getattr(statement, "column_descriptions", ())
        entity = descriptions[0].get("entity") if descriptions else None
        if entity is store.CustomImportPreparedSource:
            case.events.append("header")
            case.header_queries.append(statement)
            return case.header
        if entity is store.CustomImportPreparedSourcePart:
            case.events.append("orphan-check")
            case.orphan_queries.append(statement)
            return case.context.execution_id if case.orphan_part else None
        case.events.append("prepared-owner")
        return True

    async def execute(statement, *args, **kwargs):
        if "custom_import_capture_usage" in str(statement):
            case.events.append("retention-query")
            return SimpleNamespace(one=lambda: ("read committed", case.retained_bytes + case.quota_extra))
        descriptions = getattr(statement, "column_descriptions", ())
        if descriptions and descriptions[0].get("entity") is store.CustomImportPreparedSourcePart:
            case.events.append("observe")
            case.observed_queries.append(statement)
            return SimpleNamespace(one_or_none=lambda: case.observed_part)
        return await original_execute(statement, *args, **kwargs)

    case.session.scalar.side_effect = scalar
    case.session.execute.side_effect = execute


def _install_store_write_endpoints(case, monkeypatch):
    actual_quota = store.validate_dataset_retained_bytes

    async def quota(session, **arguments):
        case.events.append("quota")
        case.quota_calls.append(arguments)
        case.quota_extra = arguments["new_byte_count"]
        await actual_quota(session, **arguments)

    async def copy_records(table, **arguments):
        case.events.append("copy")
        case.copy_before_counts.append(_counters(case.header))
        case.copied.append((table, arguments))

    def add(header):
        case.events.append("add")
        case.header = header

    case.copy = AsyncMock(side_effect=copy_records)
    case.connection.sync_connection.get_execution_options = lambda: {"schema_translate_map": {"mrf": "prepared_test"}}
    case.connection.get_raw_connection = AsyncMock(
        return_value=SimpleNamespace(driver_connection=SimpleNamespace(copy_records_to_table=case.copy))
    )
    case.session.add = Mock(side_effect=add)
    monkeypatch.setattr(store, "validate_dataset_retained_bytes", AsyncMock(side_effect=quota))


async def _append(case, *, part=None, stream_slot=1, part_ordinal=1, context=None):
    return await store.append_prepared_part(
        case.sessions,
        case.context if context is None else context,
        stream_slot=stream_slot,
        part_ordinal=part_ordinal,
        part=_part() if part is None else part,
    )


async def test_begin_retains_owner_pins_and_quota_checks(case):
    result = await store.begin_prepared_source(case.sessions, case.context)
    header = case.header
    assert result == store.PreparedSourceState(
        "pending", 1, empty_progress(len(case.stream_slots), case.context.policy)
    )
    assert _counters(header) == (0, 0, 0, 0)
    assert header.preparation_deadline_at == case.context.build_deadline_at
    assert header.producer_token_sha256 == lease_token_sha256(case.token)
    assert vars(store.prepared_source_identity(case.context)).items() <= vars(header).items()
    assert case.quota_calls == [
        {
            "dataset_id": case.context.dataset_id,
            "new_byte_count": len(header.canonical_processing_policy.encode())
            + len(header.canonical_progress.encode()),
            "maximum_retained_bytes": case.context.policy.maximum_dataset_retained_bytes,
        }
    ]
    assert case.events.index("live-check") < case.events.index("prepared-owner") < case.events.index("header")
    assert case.events.index("orphan-check") < case.events.index("quota") < case.events.index("add")
    assert case.events[-3:] == ["live-check", "commit", "close"]
    assert case.header_queries[0]._for_update_arg is not None
    assert case.header_queries[0].get_execution_options()["populate_existing"] is True
    assert case.orphan_queries[0]._limit_clause.value == 1
    assert tuple(case.orphan_queries[0].selected_columns.keys()) == ("execution_id",)
    case.copy.assert_not_awaited()


async def test_begin_refuses_orphan_parts_instead_of_restarting_preparation(case):
    case.orphan_part = True
    with pytest.raises(CandidateRunnerError, match="prepared_source_state_invalid"):
        await store.begin_prepared_source(case.sessions, case.context)
    case.session.add.assert_not_called()
    assert not case.quota_calls
    assert case.events[-2:] == ["rollback", "close"]


async def test_begin_adopts_new_fence_without_resetting_progress(case):
    part, progress = _committed_part(case)
    original_deadline = case.header.preparation_deadline_at
    assert await store.begin_prepared_source(case.sessions, case.context) == store.PreparedSourceState(
        "pending", 5, progress
    )
    resumed = replace(case.context, fence=8, lease_token=b"synthetic-new-owner")
    case.lease.fence, case.lease.token_sha256 = resumed.fence, lease_token_sha256(resumed.lease_token)
    assert await store.begin_prepared_source(case.sessions, resumed) == store.PreparedSourceState(
        "pending", 6, progress
    )
    assert case.header.producer_fence == 8
    assert case.header.producer_token_sha256 == lease_token_sha256(resumed.lease_token)
    assert case.header.preparation_deadline_at == original_deadline == resumed.build_deadline_at
    assert _counters(case.header) == totals(progress)
    assert await _append(case, context=resumed, part=part) == store.PreparedSourceState("pending", 6, progress)
    assert not case.quota_calls
    case.session.add.assert_not_called()
    case.copy.assert_not_awaited()


@pytest.mark.parametrize("operation", ["begin", "append", "eof"])
async def test_store_entrypoints_refuse_untyped_scope_before_opening_a_session(case, operation):
    untyped = SimpleNamespace(**vars(case.context))
    with pytest.raises(CandidateRunnerError, match="prepared_source_state_invalid"):
        if operation == "begin":
            await store.begin_prepared_source(case.sessions, untyped)
        elif operation == "append":
            await _append(case, context=untyped)
        else:
            await store.mark_prepared_stream_eof(case.sessions, untyped, stream_slot=1)
    case.sessions.assert_not_called()
    case.copy.assert_not_awaited()


@pytest.mark.parametrize("field,value", [("producer_fence", 8), ("producer_token_sha256", b"x" * 32)])
async def test_begin_refuses_newer_producer_or_same_fence_with_another_token(case, field, value):
    header = _install_header(case)
    setattr(header, field, value)
    with pytest.raises(CandidateRunnerError, match="prepared_source_state_invalid"):
        await store.begin_prepared_source(case.sessions, case.context)
    assert header.version == 5 and getattr(header, field) == value
    assert not case.quota_calls


@pytest.mark.parametrize(
    "field",
    [
        "execution_id",
        "capture_bundle_id",
        "dataset_id",
        "definition_revision_id",
        "schema_revision_id",
        "source_binding_revision_id",
        "source_binding_sha256",
        "request_identity_sha256",
        "raw_manifest_sha256",
        "processing_policy_sha256",
        "canonical_processing_policy",
        "preparation_deadline_at",
        "encoding_contract",
    ],
)
async def test_existing_header_requires_every_exact_original_binding_policy_and_deadline_pin(case, field):
    header = _install_header(case)
    value = getattr(header, field)
    changed = (
        value + 1
        if type(value) is int
        else (
            b"x" * 32
            if type(value) is bytes
            else value + dt.timedelta(seconds=1)
            if isinstance(value, dt.datetime)
            else value + " "
        )
    )
    setattr(header, field, changed)
    with pytest.raises(CandidateRunnerError, match="prepared_source_state_invalid"):
        await store.begin_prepared_source(case.sessions, case.context)
    assert header.version == 5 and not case.quota_calls
    case.copy.assert_not_awaited()


async def test_changed_retained_original_fails_scope_recheck_before_prepared_header_access(case):
    _install_header(case)
    case.bundle.manifest_sha256 = b"x" * 32
    with pytest.raises(CandidateRunnerError, match="prepared_source_scope_invalid"):
        await _append(case)
    assert not case.header_queries and not case.quota_calls
    case.copy.assert_not_awaited()


async def test_append_orders_copy_and_atomic_counters(case):
    header = _install_header(case)
    part = _part(count=2, value="é")
    metadata = _metadata(part)
    old_progress_bytes = len(header.canonical_progress.encode())
    appended_state = await _append(case, part=part)
    assert appended_state.state == "pending" and appended_state.version == 6
    assert case.copy_before_counts == [(0, 0, 0, 0)]
    table, arguments = case.copied[0]
    assert table == "custom_import_prepared_source_part" and arguments["schema_name"] == "prepared_test"
    assert len(arguments["records"]) == 1
    assert dict(zip(arguments["columns"], arguments["records"][0], strict=True)) == {
        "execution_id": case.context.execution_id,
        "stream_slot": 1,
        "part_ordinal": 1,
        "dataset_id": case.context.dataset_id,
        "definition_revision_id": case.context.definition_revision_id,
        "schema_revision_id": case.context.schema_revision_id,
        "first_source_ordinal": 1,
        "record_count": 2,
        "payload_contract": PREPARED_SOURCE_CONTRACT,
        "byte_count": len(part.payload),
        "payload": part.payload,
        "payload_sha256": hashlib.sha256(part.payload).digest(),
        "canonical_manifest": metadata.canonical,
        "manifest_sha256": metadata.digest,
    }
    assert _counters(header) == (1, 2, len(part.payload), len(metadata.canonical.encode()))
    assert case.quota_calls[0]["new_byte_count"] == (
        len(part.payload)
        + len(metadata.canonical.encode())
        + len(header.canonical_progress.encode())
        - old_progress_bytes
    )
    assert case.events.index("quota") < case.events.index("copy") < case.events.index("commit")
    assert case.events[-3:] == ["live-check", "commit", "close"]
    case.copy.assert_awaited_once()


@pytest.mark.parametrize("invalid", ["bytes", "rows", "slot", "ordinal", "mutable-payload"])
async def test_part_bounds_fail_before_acquiring_authority_or_copying(case, invalid):
    context = replace(case.context, policy=ObjectProcessingPolicy.from_mapping(_policy_document()))
    part, slot, ordinal = _part(), 1, 1
    if invalid == "bytes":
        part = part._replace(payload=b"x" * (context.policy.prepared["part_byte_limit"] + 1))
    elif invalid == "rows":
        part = part._replace(record_count=context.policy.prepared["part_row_limit"] + 1)
    elif invalid == "slot":
        slot = len(context.stream_slots) + 1
    elif invalid == "ordinal":
        ordinal = context.policy.prepared["maximum_parts"] + 1
    else:
        part = part._replace(payload=bytearray(part.payload))
    with pytest.raises(CandidateRunnerError, match="prepared_source_state_invalid"):
        await _append(case, context=context, part=part, stream_slot=slot, part_ordinal=ordinal)
    case.sessions.assert_not_called()
    case.copy.assert_not_awaited()


@pytest.mark.parametrize("part,ordinal", [(_part(), 2), (_part(first=2), 1)])
async def test_new_part_requires_exact_next_part_and_source_ordinals_before_quota(case, part, ordinal):
    header = _install_header(case)
    with pytest.raises(PreparedSourceProgressError):
        await _append(case, part=part, part_ordinal=ordinal)
    assert _counters(header) == (0, 0, 0, 0) and header.version == 5
    assert not case.quota_calls
    case.copy.assert_not_awaited()


async def test_quota_refusal_precedes_copy_and_progress_mutation(case):
    header = _install_header(case)
    case.retained_bytes = case.context.policy.maximum_dataset_retained_bytes
    with pytest.raises(CaptureBundleConflict, match="quota"):
        await _append(case)
    assert _counters(header) == (0, 0, 0, 0) and header.version == 5
    case.copy.assert_not_awaited()
    assert case.events[-2:] == ["rollback", "close"]


async def test_copy_failure_does_not_advance_counters_and_leaves_transaction_to_rollback(case):
    header = _install_header(case)
    failure = RuntimeError("synthetic COPY failure")
    case.copy.side_effect = failure
    with pytest.raises(RuntimeError) as caught:
        await _append(case)
    assert caught.value is failure
    assert _counters(header) == (0, 0, 0, 0) and header.version == 5
    assert case.quota_calls and "commit" not in case.events
    assert case.events[-2:] == ["rollback", "close"]


async def test_lost_ack_observes_metadata_without_recharging(case):
    part, progress = _committed_part(case)
    result = await _append(case, part=part)
    assert result == store.PreparedSourceState("pending", 5, progress)
    assert _counters(case.header) == totals(progress)
    assert not case.quota_calls and case.header.version == 5
    case.copy.assert_not_awaited()
    statement = case.observed_queries[0]
    assert tuple(statement.selected_columns.keys()) == (
        "dataset_id",
        "definition_revision_id",
        "schema_revision_id",
        "first_source_ordinal",
        "record_count",
        "byte_count",
        "payload_sha256",
        "payload_contract",
        "canonical_manifest",
        "manifest_sha256",
    )
    assert set(statement.compile().params.values()) == {case.context.execution_id, 1}


@pytest.mark.parametrize("mismatch", ["missing", "payload", "owner", "manifest"])
async def test_committed_prefix_mismatch_is_a_conflict_and_never_overwrites_or_recharges(case, mismatch):
    part, progress = _committed_part(case)
    if mismatch == "missing":
        case.observed_part = None
    elif mismatch == "payload":
        part = _part(value="different")
    else:
        part_values = list(case.observed_part)
        part_values[0 if mismatch == "owner" else 8] = 99 if mismatch == "owner" else "{}"
        case.observed_part = tuple(part_values)
    with pytest.raises(CandidateRunnerError, match="prepared_source_state_invalid"):
        await _append(case, part=part)
    assert _counters(case.header) == totals(progress) and case.header.version == 5
    assert not case.quota_calls
    case.copy.assert_not_awaited()


@pytest.mark.parametrize("state", ["closed", "sealed"])
async def test_sealed_header_allows_only_exact_retained_observation(case, state):
    header = _complete_header(case, state)
    original = (header.version, header.canonical_progress)
    with pytest.raises(CandidateRunnerError, match="prepared_source_state_invalid"):
        await _append(case, part_ordinal=2)
    case.observed_part = _observed_row(case, _metadata(_part(count=0)))
    observed = await _append(case, part=_part(count=0))
    assert observed.state == state and (header.version, header.canonical_progress) == original
    assert not case.quota_calls
    case.copy.assert_not_awaited()


async def test_empty_header_cannot_claim_eof_without_the_canonical_empty_part(case):
    header = _install_header(case)
    with pytest.raises(PreparedSourceProgressError):
        await store.mark_prepared_stream_eof(case.sessions, case.context, stream_slot=1)
    assert header.state == "pending" and header.version == 5
    assert not case.quota_calls and _counters(header) == (0, 0, 0, 0)


async def test_stream_eof_replay_does_not_recharge(case):
    _install_header(case)
    count = len(case.context.stream_slots)
    assert count > 1
    for slot in range(1, count + 1):
        await _append(case, part=_part(count=0), stream_slot=slot)
        before_counts = _counters(case.header)
        result = await store.mark_prepared_stream_eof(case.sessions, case.context, stream_slot=slot)
        assert result.state == ("closed" if slot == count else "pending")
        assert _counters(case.header) == before_counts
        before_version, charged = case.header.version, len(case.quota_calls)
        assert await store.mark_prepared_stream_eof(case.sessions, case.context, stream_slot=slot) == result
        assert case.header.version == before_version and len(case.quota_calls) == charged
    assert all(stream.eof for stream in result.progress)
    assert all(call["new_byte_count"] >= 0 for call in case.quota_calls)


async def test_closed_stream_refuses_suffix_even_when_other_streams_remain_pending(case):
    part, _ = _committed_part(case)
    state = await store.mark_prepared_stream_eof(case.sessions, case.context, stream_slot=1)
    assert state.state == "pending"
    charged = len(case.quota_calls)
    with pytest.raises(PreparedSourceProgressError):
        await _append(case, part=_part(first=part.record_count + 1), part_ordinal=2)
    assert case.header.version == state.version and len(case.quota_calls) == charged
    case.copy.assert_not_awaited()


@pytest.mark.parametrize("corruption", ["counter", "premature-close", "premature-summary"])
async def test_inconsistent_retained_progress_cannot_be_adopted(case, corruption):
    header = _install_header(case)
    if corruption == "counter":
        header.record_count = 1
    elif corruption == "premature-close":
        header.state = "closed"
    else:
        header.canonical_summary = "{}"
    with pytest.raises(CandidateRunnerError, match="prepared_source_state_invalid"):
        await store.begin_prepared_source(case.sessions, case.context)
    assert header.version == 5 and not case.quota_calls


@pytest.mark.parametrize("changed", ["fence", "token", "lease-expiry", "cancellation"])
async def test_live_authority_loss_prevents_header_access_and_copy(case, changed):
    _install_header(case)
    if changed == "fence":
        case.lease.fence += 1
    elif changed == "token":
        case.lease.token_sha256 = b"x" * 32
    elif changed == "lease-expiry":
        case.lease.expires_at = case.now
    else:
        case.execution.state = "canceling"
    with pytest.raises(CancellationRequested if changed == "cancellation" else LeaseAuthorityLost):
        await _append(case)
    assert not case.header_queries and not case.quota_calls
    case.copy.assert_not_awaited()
    assert case.events[-2:] == ["rollback", "close"]


@pytest.mark.parametrize("boundary", ["original-deadline", "permit-expiry", "fence"])
async def test_page_rechecks_authority_after_copy_and_refuses_commit_at_original_boundary(case, boundary):
    if boundary == "original-deadline":
        case.execution.started_at = case.permit.expires_at - dt.timedelta(minutes=90)
        case.context = replace(
            case.context,
            build_deadline_at=case.execution.started_at
            + dt.timedelta(seconds=case.context.policy.build.build_deadline_seconds),
        )
        runner_registry.database_now.side_effect = [
            case.context.build_deadline_at - dt.timedelta(seconds=1),
            case.context.build_deadline_at,
        ]
    elif boundary == "permit-expiry":
        runner_registry.database_now.side_effect = [case.now, case.permit.expires_at]
    else:
        original_copy = case.copy.side_effect

        async def copy_then_lose_fence(*args, **kwargs):
            await original_copy(*args, **kwargs)
            case.lease.fence += 1

        case.copy.side_effect = copy_then_lose_fence
    _install_header(case)
    with pytest.raises(LeaseAuthorityLost):
        await _append(case)
    assert len(case.copied) == 1 and case.events.count("live-check") == 2
    assert "commit" not in case.events and case.events[-2:] == ["rollback", "close"]


async def test_current_fence_can_reuse_sealed_identity_without_rewriting_its_historical_producer(case):
    header = _complete_header(case, "sealed")
    original = (header.version, header.producer_fence, header.producer_token_sha256, header.prepared_sha256)
    resumed = replace(case.context, fence=8, lease_token=b"synthetic-resumed-owner")
    case.lease.fence, case.lease.token_sha256 = resumed.fence, lease_token_sha256(resumed.lease_token)
    state = await store.begin_prepared_source(case.sessions, resumed)
    assert state.state == "sealed"
    assert (header.version, header.producer_fence, header.producer_token_sha256, header.prepared_sha256) == original
    assert header.preparation_deadline_at == resumed.build_deadline_at
    assert not case.quota_calls
    case.copy.assert_not_awaited()
