# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Preparation failures cannot gain writer authority or advance durable offsets."""

import asyncio
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager, nullcontext
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process import provider_directory_cms_resource_batch as cms_batch
from process import provider_directory_generic_source_adapters as adapters
from process import provider_directory_generic_source_policy as policy
from process import provider_directory_generic_source_storage as storage
from tests.test_provider_directory_bulk_preparation import _context, _fields, _identity, fhir, parallel
from tests.test_provider_directory_generic_source_policy import synthetic_source_database
from tests.test_provider_directory_generic_source_transition import _census_connection, case


def _prepared_rows():
    rows, fetched, error = parallel.prepare_lines(
        [b'{"resourceType":"PractitionerRole","id":"example"}'], _fields(), [], _identity(), True
    )
    assert fetched == 1 and error is None
    return rows


def _writer_arguments():
    identity_by_field = _identity()
    return {
        field_name: identity_by_field[field_name]
        for field_name in ("dataset_id", "resource_hash_contract", "semantic_projection_as_of")
    }


@pytest.mark.parametrize("callback", ["row_batch_handler", "row_progress_handler"])
def test_worker_callbacks_refuse_writer_io(monkeypatch, callback):
    streams = []
    original = fhir._BulkOutputStreamState

    def capture(*args, **kwargs):
        stream = original(*args, **kwargs)
        streams.append(stream)
        return stream

    monkeypatch.setattr(fhir, "_BulkOutputStreamState", capture)
    _prepared_rows()
    context = streams[0].context
    handler = getattr(context if callback == "row_batch_handler" else context.resume_options, callback)
    with pytest.raises(RuntimeError, match="^bulk_preparation_io_forbidden$"):
        handler(object(), [])


@pytest.mark.parametrize("context_name", ["_FACTORY", "_PUBLISHER"])
def test_worker_refuses_inherited_publisher_context(monkeypatch, context_name):
    parser = Mock(side_effect=AssertionError("parser reached"))
    monkeypatch.setattr(fhir, "_BulkOutputStreamState", parser)
    variable = getattr(policy, context_name)
    token = variable.set(object())
    try:
        with pytest.raises(RuntimeError, match="^bulk_preparation_publisher_bound$"):
            parallel.prepare_lines([], _fields(), [], _identity(), True)
    finally:
        variable.reset(token)
    parser.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("oversized", [False, True])
async def test_malformed_final_line_never_commits_valid_prefix(monkeypatch, oversized):
    line = b'{"resourceType":"PractitionerRole","id":"example"}'
    raw = line + b"\n{"
    if oversized:
        monkeypatch.setattr(parallel, "PREPARATION_MAX_BYTES", len(line) + 1)

    async def chunks(_size):
        yield raw

    response = SimpleNamespace(
        status=200,
        headers={"etag": '"etag"', "content-length": str(len(raw))},
        content=SimpleNamespace(iter_chunked=chunks),
    )
    with ThreadPoolExecutor(max_workers=1) as executor:
        preparation_by_field = {
            "executor": executor,
            "slots": asyncio.Semaphore(1),
            "large_batch": asyncio.Semaphore(1),
        }
        source_by_field = {
            **_fields()["source"],
            "_bulk_preparation_identity": _identity(),
            "_provider_directory_bulk_parallel": {"preparation": preparation_by_field},
        }
        state = fhir._BulkOutputStreamState(_context(_fields(), source=source_by_field, length=len(raw)))
        try:
            stream_result = await fhir._read_bulk_output_response(response, state)
            assert stream_result == ([], 1, 0, False, "invalid_ndjson")
            assert state.committed_bytes == state.rows_written == 0
            state.context.row_batch_handler.assert_not_awaited()
            state.context.resume_options.row_progress_handler.assert_not_awaited()
        finally:
            state.release_preparation_slot()
        assert preparation_by_field["slots"]._value == preparation_by_field["large_batch"]._value == 1


@pytest.fixture
def source_writer(monkeypatch):
    """Exercise real transaction guards over catalog doubles, without native privilege claims."""
    directory = SimpleNamespace(
        RESOURCE_MODELS_BY_TYPE=fhir.RESOURCE_MODELS_BY_TYPE,
        _endpoint_dataset_resource_rows=Mock(wraps=fhir._endpoint_dataset_resource_rows),
        _assert_practitioner_semantic_projection_as_of=Mock(wraps=fhir._assert_practitioner_semantic_projection_as_of),
        _schema=lambda: "source_schema",
        persist_dataset_proof_shard=AsyncMock(),
    )
    writer = SimpleNamespace(directory=directory, rows=_prepared_rows(), copy=AsyncMock(), built_proof=AsyncMock())
    monkeypatch.setattr(adapters, "persist_source_rows", writer.copy)
    monkeypatch.setattr(cms_batch, "persist_built_dataset_proof_shard", writer.built_proof)

    @asynccontextmanager
    async def transaction():
        async with synthetic_source_database(monkeypatch, SimpleNamespace()) as database:
            directory.db = database
            async with policy.source_transaction(database) as session:
                writer.session = session
                begin = Mock(side_effect=AssertionError("original transaction restarted"))
                monkeypatch.setattr(policy._FACTORY.get(), "transaction", begin)
                yield
                begin.assert_not_called()

    writer.transaction = transaction
    return writer


@pytest.mark.asyncio
@pytest.mark.parametrize("field_name", ["dataset_id", "resource_hash_contract", "semantic_projection_as_of", "model"])
async def test_prepared_writer_refuses_identity_change_before_copy(source_writer, field_name):
    arguments_by_field = _writer_arguments()
    model = fhir.RESOURCE_MODELS_BY_TYPE["PractitionerRole"]
    if field_name == "model":
        model = fhir.RESOURCE_MODELS_BY_TYPE["Location"]
    else:
        arguments_by_field[field_name] = "different"
    async with source_writer.transaction():
        with pytest.raises(ValueError, match="^bulk_preparation_identity_mismatch$"):
            await cms_batch.persist_dataset_rows(
                source_writer.directory, model, source_writer.rows, **arguments_by_field
            )
    source_writer.copy.assert_not_awaited()
    source_writer.built_proof.assert_not_awaited()
    source_writer.directory._endpoint_dataset_resource_rows.assert_not_called()
    source_writer.directory._assert_practitioner_semantic_projection_as_of.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("prepared", [False, True])
async def test_dataset_writer_preserves_rows_proof_and_original_transaction(source_writer, prepared):
    directory, batch = source_writer.directory, source_writer.rows
    resource_rows = batch if prepared else list(batch)
    model = fhir.RESOURCE_MODELS_BY_TYPE[batch.resource_type]
    arguments_by_field = _writer_arguments()
    observed_sessions = []

    async def observe(*_args, **_kwargs):
        observed_sessions.append(policy.source_copy_session())

    source_writer.copy.side_effect = source_writer.built_proof.side_effect = observe
    directory.persist_dataset_proof_shard.side_effect = observe
    async with source_writer.transaction():
        dataset_rows = await cms_batch.persist_dataset_rows(directory, model, resource_rows, **arguments_by_field)
    assert observed_sessions == [source_writer.session, source_writer.session]
    if prepared:
        assert dataset_rows is batch.dataset_rows
        directory._endpoint_dataset_resource_rows.assert_not_called()
        source_writer.built_proof.assert_awaited_once_with(
            directory.db,
            directory._schema(),
            *batch.proof,
            dataset_id=arguments_by_field["dataset_id"],
            expected_resource_hash_contract=arguments_by_field["resource_hash_contract"],
        )
        directory.persist_dataset_proof_shard.assert_not_awaited()
    else:
        assert len(dataset_rows) == len(batch.dataset_rows) == 1
        directory._endpoint_dataset_resource_rows.assert_called_once_with(
            model,
            resource_rows,
            dataset_id=arguments_by_field["dataset_id"],
            resource_hash_contract=arguments_by_field["resource_hash_contract"],
        )
        directory.persist_dataset_proof_shard.assert_awaited_once_with(
            directory.db,
            directory._schema(),
            dataset_rows,
            dataset_id=arguments_by_field["dataset_id"],
            expected_resource_hash_contract=arguments_by_field["resource_hash_contract"],
        )
        source_writer.built_proof.assert_not_awaited()
    source_writer.copy.assert_awaited_once_with(
        directory,
        dataset_rows,
        resource_hash_contract=arguments_by_field["resource_hash_contract"],
        semantic_projection_as_of=arguments_by_field["semantic_projection_as_of"],
    )
    directory._assert_practitioner_semantic_projection_as_of.assert_called_once_with(
        model,
        resource_rows,
        arguments_by_field["resource_hash_contract"],
        arguments_by_field["semantic_projection_as_of"],
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("prepared", [False, True])
async def test_empty_dataset_batch_skips_copy_and_proof(source_writer, prepared):
    batch = parallel.PreparedRows([], [], ({}, b""), _identity(), "PractitionerRole") if prepared else []
    async with source_writer.transaction():
        rows = await cms_batch.persist_dataset_rows(
            source_writer.directory, fhir.RESOURCE_MODELS_BY_TYPE["PractitionerRole"], batch, **_writer_arguments()
        )
    assert rows == []
    source_writer.copy.assert_not_awaited()
    source_writer.built_proof.assert_not_awaited()
    source_writer.directory.persist_dataset_proof_shard.assert_not_awaited()
    source_writer.directory._assert_practitioner_semantic_projection_as_of.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("prepared", [False, True])
@pytest.mark.parametrize("failure", ["copy", "proof"])
async def test_dataset_failure_propagates_before_stream_checkpoint(monkeypatch, source_writer, prepared, failure):
    directory, batch = source_writer.directory, source_writer.rows
    proof = source_writer.built_proof if prepared else directory.persist_dataset_proof_shard
    failing = source_writer.copy if failure == "copy" else proof
    failing.side_effect = RuntimeError(f"{failure}_failed")
    progress = AsyncMock()
    state = fhir._BulkOutputStreamState(_context(_fields(), progress=progress, offset=13))
    state.pending_rows = batch if prepared else list(batch)
    original_pending = state.pending_rows

    async def write(model, rows):
        return len(await cms_batch.persist_dataset_rows(directory, model, rows, **_writer_arguments()))

    object.__setattr__(state.context, "row_batch_handler", write)
    monkeypatch.setattr(
        fhir, "source_checkpoint_transaction", lambda *_args, **_kwargs: policy.source_transaction(directory.db)
    )
    monkeypatch.setattr(parallel, "write_slot", lambda *_args: nullcontext())
    async with source_writer.transaction():
        with pytest.raises(RuntimeError, match=f"^{failure}_failed$"):
            await state.flush_pending_rows(99)
    progress.assert_not_awaited()
    assert state.committed_bytes == 13 and state.rows_written == 0
    assert state.pending_rows is original_pending and len(state.pending_rows) == 1
    source_writer.copy.assert_awaited_once()
    if failure == "copy":
        proof.assert_not_awaited()
    else:
        proof.assert_awaited_once()


@pytest.mark.asyncio
async def test_closed_census_backend_needs_no_rollback_or_termination():
    connection = SimpleNamespace(is_closed=lambda: True, terminate=Mock(), _transport=Mock())
    transaction = SimpleNamespace(rollback=AsyncMock())
    await storage._rollback_transition_census(connection, transaction)
    transaction.rollback.assert_not_awaited()
    connection.terminate.assert_not_called()
    connection._transport.abort.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("body_fails", [False, True])
@pytest.mark.parametrize("has_transport", [False, True])
async def test_census_cleanup_error_preserves_original_failure(monkeypatch, case, body_fails, has_transport):
    context, evidence = _census_connection(case)
    evidence.connection.terminate = Mock()
    evidence.connection._transport = Mock() if has_transport else None
    rollback = AsyncMock(side_effect=RuntimeError("rollback_failed"))
    monkeypatch.setattr(evidence.transaction, "rollback", rollback)
    failure = ValueError("body_failed") if body_fails else None
    with pytest.raises(ValueError if body_fails else RuntimeError) as observed:
        async with context as census:
            if failure is not None:
                raise failure
    if body_fails:
        assert observed.value is failure
    else:
        assert str(observed.value) == "rollback_failed"
    assert census.active is False
    rollback.assert_awaited_once_with()
    evidence.connection.terminate.assert_called_once_with()
    if has_transport:
        evidence.connection._transport.abort.assert_called_once_with()
    assert case.writes == []


@pytest.mark.parametrize("rows", [[], [{}] * 4097])
def test_cms_preparation_rejects_out_of_bound_batches_before_row_building(rows):
    directory = Mock()
    with pytest.raises(RuntimeError, match="^cms_npd_resource_batch_size_invalid$"):
        cms_batch.prepare_cms_dataset_rows(directory, object(), rows, object())
    directory._endpoint_dataset_resource_rows.assert_not_called()


@pytest.mark.asyncio
async def test_expired_bulk_status_deadline_never_starts_a_request():
    now = fhir._bulk_export_now_utc()
    context = SimpleNamespace(pending_deadline=now - timedelta(seconds=1))
    directory = SimpleNamespace(
        _bulk_export_now_utc=lambda: now,
        _bulk_http_get_json=AsyncMock(),
        BULK_EXPORT_STATUS_DEADLINE_EXCEEDED="deadline",
    )
    assert await parallel.poll_response(directory, object(), context, 5) == (None, {}, None, "deadline")
    directory._bulk_http_get_json.assert_not_awaited()


def test_cms_preparation_reuses_the_same_canonical_rows_and_proof():
    rows = _prepared_rows()
    model = fhir.RESOURCE_MODELS_BY_TYPE["PractitionerRole"]
    candidate = SimpleNamespace(**_identity())
    assert cms_batch.prepare_cms_dataset_rows(fhir, model, rows, candidate) == cms_batch.prepare_dataset_rows(
        fhir, model, rows, candidate
    )
