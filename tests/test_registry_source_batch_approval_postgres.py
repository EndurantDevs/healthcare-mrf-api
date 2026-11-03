# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Terminal Source approval uses the existing native catalog, request and lease contracts."""

import hashlib
import json
from uuid import UUID, uuid4

import pytest

from process import registry_publication_execution as execution
from process import registry_source_binding_publication as source_batch
from process.registry_approval_store import RegistryApprovalConflict
from process.registry_publication_queue import claim_registry_publication_request, read_registry_publication_request
from tests.test_network_serving_schema_postgres import _candidate
from tests.test_network_serving_schema_postgres import serving_schema as serving_schema
from tests.test_network_source_binding_store_postgres import metadata_custody as metadata_custody
from tests.test_network_source_binding_store_postgres import source_batch_case as source_batch_case
from tests.test_registry_approval_store_postgres import _create, _draft

pytestmark = pytest.mark.asyncio


async def _state(case):
    return tuple(
        await case.connection.fetchrow(f'''SELECT draft_revision,approved_revision,
      (SELECT count(*) FROM "{case.schema}".registry_approval_history),
      (SELECT count(*) FROM "{case.schema}".network_membership_candidate),
      (SELECT count(*) FROM "{case.schema}".network_serving_manifest),
      (SELECT generation_id FROM "{case.schema}".network_serving_control)
      FROM "{case.schema}".registry_revision_control WHERE id=1''')
    )


async def _queue_and_claim(case):
    document_by_field = dict(case.command, operation=source_batch.APPROVAL_OPERATION)
    async with case.connection.transaction(isolation="repeatable_read", readonly=True):
        compact, command = await source_batch.resolve_source_batch(
            case.connection, document_by_field, case.actor, control_schema=case.schema, allow_unbound=True
        )
    async with case.connection.transaction():
        queued = await source_batch.enqueue_source_batch(
            case.connection, compact, case.actor, "b" * 64, control_schema=case.schema
        )
        lease = await claim_registry_publication_request(
            case.connection,
            operation_family="publication",
            request_id=UUID(queued["request_id"]),
            control_schema=case.schema,
        )
    assert lease is not None
    return compact, command, queued, lease


def _authority(calls, *, refuse_at=None):
    async def authorize(**arguments):
        calls.append(arguments)
        if len(calls) == refuse_at:
            raise PermissionError("synthetic grant revoked")
        return {
            "authorized": True,
            "actor": arguments["actor"],
            "policy_revision": 7,
            "selection_sha256": hashlib.sha256(
                json.dumps(arguments["selection"], sort_keys=True, separators=(",", ":")).encode()
            ).hexdigest(),
        }

    return authorize


async def _complete(case, lease, authorize):
    return await execution.approve_queued_registry_publication(
        case.connection, lease["request_id"], lease["lease_id"], authorize=authorize, control_schema=case.schema
    )


async def _drift_serving_head(case):
    candidate_id = uuid4()
    await _candidate(case.connection, case.schema, candidate_id)
    generation = await case.connection.fetchval(
        f'''INSERT INTO "{case.schema}".network_serving_manifest
      (candidate_id,schema_revision,source_generations,approved_custom_revision,manifest_sha256)
      VALUES($1,1,'{{}}',0,repeat('a',64)) RETURNING generation_id''',
        candidate_id,
    )
    await case.connection.execute(f'UPDATE "{case.schema}".network_serving_control SET generation_id=$1', generation)


@pytest.mark.parametrize("source_batch_case", [1301], indirect=True)
async def test_complete_source_approval_is_terminal_without_candidate_or_serving_write(source_batch_case):
    case = source_batch_case
    compact, command, queued, lease = await _queue_and_claim(case)
    before, calls = await _state(case), []
    result = await _complete(case, lease, _authority(calls))
    after = await _state(case)
    assert after[:2] == (result["approved_revision"],) * 2 and after[2] == before[2] + 1
    assert after[3:] == before[3:] == (0, 0, None)
    assert result["selected_count"] == 2602 and result["selection_sha256"] == compact["selection_sha256"]
    assert len(calls) == 4 * 27 and all(1 <= len(call["selection"]) <= 100 for call in calls)
    status = await read_registry_publication_request(case.connection, lease["request_id"], control_schema=case.schema)
    assert status["state"] == "completed" and status["operation"] == source_batch.APPROVAL_OPERATION
    assert status["result"] == result
    assert tuple(
        await case.connection.fetchrow(
            f'''SELECT lease_id,lease_expires_at
      FROM "{case.schema}".registry_publication_request WHERE request_id=$1''',
            lease["request_id"],
        )
    ) == (None, None)
    async with case.connection.transaction():
        replay = await source_batch.enqueue_source_batch(
            case.connection, compact, case.actor, "b" * 64, control_schema=case.schema
        )
    assert replay == {**queued, "state": "completed", "replayed": True}
    assert await _state(case) == after


@pytest.mark.parametrize("failure", ["finish", "revoked", "stale", "head"])
async def test_source_approval_refusal_rolls_back_before_terminal_retry(source_batch_case, monkeypatch, failure):
    case = source_batch_case
    _, _, _, lease = await _queue_and_claim(case)
    if failure == "stale":
        await _draft(case.engine, case.schema, _create(), case.actor)
    if failure == "head":
        await _drift_serving_head(case)
    before, calls = await _state(case), []
    original_finish = execution.finish_registry_publication_request

    async def refuse_finish(*arguments, **options):
        raise RuntimeError("synthetic terminal failure")

    if failure == "finish":
        monkeypatch.setattr(execution, "finish_registry_publication_request", refuse_finish)
    error = (
        RuntimeError if failure == "finish" else PermissionError if failure == "revoked" else RegistryApprovalConflict
    )
    with pytest.raises(error):
        await _complete(case, lease, _authority(calls, refuse_at=4 if failure == "revoked" else None))
    assert await _state(case) == before
    status = await read_registry_publication_request(case.connection, lease["request_id"], control_schema=case.schema)
    assert status["state"] == "running" and status["result"] is None
    if failure in {"finish", "revoked"}:
        monkeypatch.setattr(execution, "finish_registry_publication_request", original_finish)
        retries = []
        result = await _complete(case, lease, _authority(retries))
        assert len(retries) == 4 and result["approved_revision"] == before[0] + 1
        assert (await _state(case))[2] == before[2] + 1


async def test_source_approval_partial_selection_is_refused_by_complete_catalog_resolution(source_batch_case):
    case = source_batch_case
    async with case.connection.transaction(isolation="repeatable_read", readonly=True):
        compact, command = await source_batch.resolve_source_batch(
            case.connection,
            dict(case.command, operation=source_batch.APPROVAL_OPERATION),
            case.actor,
            control_schema=case.schema,
            allow_unbound=True,
        )
    partial, digest = source_batch.selection_identity(command.selection[:2])
    before = await _state(case)
    with pytest.raises(ValueError, match="registry_source_batch_invalid"):
        async with case.connection.transaction():
            await source_batch.enqueue_source_batch(
                case.connection,
                dict(compact, selection_count=len(partial), selection_sha256=digest),
                case.actor,
                "b" * 64,
                control_schema=case.schema,
            )
    assert await _state(case) == before
    assert await case.connection.fetchval(f'SELECT count(*) FROM "{case.schema}".registry_publication_request') == 0
