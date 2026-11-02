# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Source-wide batch publication retains genuine custody and pinned readers."""

import hashlib
import json
from dataclasses import asdict
from uuid import UUID

import pytest

from process.network_serving_read import resolve_network_serving_manifest
from process.network_source_binding_store import (
    NetworkSourceBindingBatchCommand,
    apply_network_source_binding_batch,
)
from process.registry_publication_queue import _enqueue_validated_publication
from process.registry_source_binding_publication import selection_identity, stored_source_batch
from tests.test_network_fhir_retained_capture_postgres import _binding_command
from tests.test_network_fhir_source_witness_publication_postgres import (
    _assert_serving_parity,
    _consume_expected_success,
    _consumer,
    _exact_request_arguments,
    _manifest,
    _publication_case,
    _publish_once,
)
from tests.test_registry_approval_store_postgres import _command
from tests.test_registry_publication_consumer_postgres import _head, _status
from tests.test_registry_publication_worker_permissions_postgres import _login

pytestmark = pytest.mark.asyncio


async def _rebind_source_binding_component(context):
    """Save a genuine binding successor with its unchanged Source capture and target."""
    worker, schema = context.worker, context.worker.fixture.control_schema
    admin, actor = worker.fixture.connection, worker.seed.actor
    prior = await admin.fetchrow(
        f'SELECT binding_id,revision FROM "{schema}".registry_network_binding '
        "WHERE source_id=$1 AND edition_id=$2 AND source_key='network-one' AND network_id=$3",
        context.recipe.binding_coordinates.source_id,
        context.capture.capture_id,
        context.network_id,
    )
    original = _binding_command(
        context.recipe.binding_coordinates, context.network_id, "network-one", context.capture.capture_sha256
    )
    binding_rows = json.loads(original.input_bytes)
    binding_rows[0].update(
        binding_id=str(prior["binding_id"]),
        operation="rebind",
        expected_revision=prior["revision"],
        expected_network_id=context.network_id,
        evidence_id="synthetic-source-review-successor",
    )
    batch = NetworkSourceBindingBatchCommand(
        json.dumps(binding_rows).encode(), original.reason, original.idempotency_key
    )
    async with admin.transaction():
        receipt = await apply_network_source_binding_batch(admin, batch, actor, control_schema=schema)
    return prior, batch, receipt


async def _queue_source_binding_component(context, head):
    """Start at the authenticated expansion seam; catalog resolution has a separate native proof."""
    worker, schema = context.worker, context.worker.fixture.control_schema
    admin, actor = worker.fixture.connection, worker.seed.actor
    prior, batch, receipt = await _rebind_source_binding_component(context)
    command = await _command(admin, schema, *receipt["records"])
    batch_hash = await admin.fetchval(
        f'SELECT request_sha256 FROM "{schema}".registry_network_binding_batch WHERE idempotency_key=$1',
        batch.idempotency_key,
    )
    selected, digest = selection_identity(command.selection)
    compact_by_name = {
        "operation": "publish_source_binding_batch",
        "batch_references": [
            {
                "idempotency_key": batch.idempotency_key,
                "request_sha256": batch_hash,
                "custom_revision": receipt["custom_revision"],
            }
        ],
        "source_coordinates": asdict(context.recipe.binding_coordinates),
        # This opaque reference is consumed only after the independently tested catalog resolver.
        "metadata_generation": hashlib.sha256(b"source-batch-consumer-component").hexdigest(),
        "selection_count": len(selected),
        "selection_sha256": digest,
        "expected_draft_revision": command.expected_draft_revision,
        "expected_approved_revision": command.expected_approved_revision,
        "expected_serving_generation": head,
        "reason": command.reason,
        "selection": selected,
    }
    async with _login(worker, "api") as connection, connection.transaction():
        queued = await _enqueue_validated_publication(
            connection,
            command,
            actor,
            session_token_sha256="a" * 64,
            expected_serving_generation=head,
            control_schema=schema,
            command_document=compact_by_name,
        )
    stored = await admin.fetchrow(
        f'SELECT * FROM "{schema}".registry_publication_request WHERE request_id=$1', UUID(queued["request_id"])
    )
    retained, actual_command = stored_source_batch(stored)
    assert actual_command == command and retained["selection_sha256"] == digest
    assert selected == [
        {"record_kind": "network_binding", "record_id": str(prior["binding_id"]), "revision": prior["revision"] + 1}
    ]
    return queued


async def test_source_batch_consumer_publishes_once_and_preserves_pinned_source(monkeypatch):
    """Exercise the real consumer after resolution without inventing catalog custody."""
    async with _publication_case(monkeypatch) as context:
        first = await _publish_once(context)
        worker, schema = context.worker, context.worker.fixture.control_schema
        count_sql = (
            f'SELECT (SELECT count(*) FROM "{schema}".registry_approval_history),'
            f'(SELECT count(*) FROM "{schema}".network_serving_manifest)'
        )
        prior_counts = tuple(await worker.fixture.connection.fetchrow(count_sql))
        async with (
            _login(worker, "execution") as old_reader,
            old_reader.transaction(isolation="repeatable_read", readonly=True),
        ):
            pinned = await resolve_network_serving_manifest(
                old_reader, generation_id=first.generation_id, control_schema=schema
            )
            original_rows = await _assert_serving_parity(context, pinned, old_reader)
            queued = await _queue_source_binding_component(context, first.generation_id)
            assert await _head(worker) == first.generation_id
            async with _consumer(worker) as (arguments, calls):
                arguments["initial_address_sources"] = None
                completed = await _consume_expected_success(
                    context, _exact_request_arguments(arguments, queued), queued
                )
                assert completed["result"]["selected_count"] == 1 and len(calls) >= 4
                assert all(len(call["selection"]) == 1 for call in calls)
                assert await _consume_expected_success(context, arguments, queued) is None
            assert await _assert_serving_parity(context, pinned, old_reader) == original_rows
        current = await _manifest(context)
        assert current.generation_id > first.generation_id
        assert current.approved_custom_revision > first.approved_custom_revision
        assert tuple(await worker.fixture.connection.fetchrow(count_sql)) == tuple(count + 1 for count in prior_counts)
        async with (
            _login(worker, "execution") as reader,
            reader.transaction(isolation="repeatable_read", readonly=True),
        ):
            assert await _assert_serving_parity(context, current, reader) == original_rows
        status = await _status(worker, queued)
        assert status["state"] == "completed" and status["operation"] == "publish_source_binding_batch"
        assert status["selection_count"] == 1 and status["result"]["serving_generation"] == current.generation_id
