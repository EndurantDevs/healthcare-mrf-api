# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Reserve immutable office intent through an already verified management writer."""

import hashlib
import json
from datetime import datetime
from uuid import UUID, uuid4

from process.network_cms_registry_address_capture import native_driver
from process.registry_ptg_office_preparation_contract import _line, _positive, _selections, validated_prepare_selector
from process.registry_ptg_published_plan_contract import validated_command
from process.registry_ptg_scope_engine import _object, _sha256, _uuid
from process.registry_publication_queue import (
    _administrator_record_actor,
    _document,
    _json,
    _reactivation_actor,
    _request_metadata,
)
from process.registry_source_observation_store import _namespace

OPERATION = "prepare_ptg_office_capture"
VERSION = 1
_SELECTOR_FIELDS = {"scope_id", "client_id", "scope_approval_sha256", "retained_generation_id", "reason", "selections"}
_COMMAND_FIELDS = {"operation", "version", "semantic_sha256", "source_command", "selector"}
_COLUMNS = (
    "request_id,actor_key,session_token_sha256,actor_json,command_json,idempotency_key,request_sha256,state,created_at"
)


def _digest(document):
    return hashlib.sha256(_json(document).encode()).hexdigest()


def _intent(source_command, selector, actor):
    source_command = validated_command(source_command)
    _object(selector, _SELECTOR_FIELDS)
    _uuid(selector["scope_id"])
    _sha256(selector["scope_approval_sha256"])
    _line(selector["client_id"], 64)
    _positive(selector["retained_generation_id"])
    _line(selector["reason"], 1000)
    _selections(selector["selections"])
    if any(selector[name] != source_command[name] for name in ("scope_id", "client_id")):
        raise ValueError("registry_ptg_office_intent_source_changed")
    actor_document = _reactivation_actor(actor)
    semantic_by_field = {
        "operation": OPERATION,
        "version": VERSION,
        "actor": actor_document,
        "source_command": source_command,
        "selector": selector,
    }
    if len(json.dumps(semantic_by_field, ensure_ascii=False, allow_nan=False).encode()) > 131072:
        raise ValueError("registry_ptg_office_intent_invalid")
    return _document(_json(semantic_by_field))


def _key(semantic_sha256):
    return "office-prepare:v1:" + semantic_sha256


def _stored_receipt(request_record, expected_semantic, is_replay):
    """Verify original immutable audit before returning any stored handles."""
    try:
        command_by_field = _document(request_record["command_json"])
        _object(command_by_field, _COMMAND_FIELDS)
        if (
            command_by_field["operation"] != OPERATION
            or type(command_by_field["version"]) is not int
            or command_by_field["version"] != VERSION
        ):
            raise ValueError
        selector = validated_prepare_selector(command_by_field["selector"])
        actor = _administrator_record_actor(request_record)
        semantic_by_field = _intent(
            command_by_field["source_command"], {name: selector[name] for name in _SELECTOR_FIELDS}, actor
        )
        semantic_sha256 = _digest(semantic_by_field)
        if semantic_by_field != expected_semantic or command_by_field["semantic_sha256"] != semantic_sha256:
            raise ValueError
        _sha256(request_record["session_token_sha256"])
        metadata = _request_metadata(
            command_by_field, semantic_by_field["actor"], request_record["session_token_sha256"], _key(semantic_sha256)
        )
        if request_record["idempotency_key"] != _key(semantic_sha256) or any(
            request_record[name] != metadata[name] for name in ("actor_key", "request_sha256")
        ):
            raise ValueError
        request_id = request_record["request_id"]
        if (
            not isinstance(request_id, UUID)
            or not request_id.int
            or request_record["state"] not in {"queued", "running", "completed", "rejected"}
        ):
            raise ValueError
        if not isinstance(request_record["created_at"], datetime) or request_record["created_at"].utcoffset() is None:
            raise ValueError
        return {
            "request_id": str(request_id),
            "state": request_record["state"],
            "created_at": request_record["created_at"].isoformat(),
            "replayed": is_replay,
            "semantic_sha256": semantic_sha256,
            "command_sha256": _digest(command_by_field),
            "command": _document(_json(command_by_field)),
        }
    except KeyError, TypeError, ValueError, AttributeError:
        raise ValueError("registry_ptg_office_intent_stored_invalid") from None


def _new_command(semantic_by_field, semantic_sha256):
    command_by_field = {
        "operation": OPERATION,
        "version": VERSION,
        "semantic_sha256": semantic_sha256,
        "source_command": semantic_by_field["source_command"],
        "selector": validated_prepare_selector(
            {**semantic_by_field["selector"], "capture_id": str(uuid4()), "idempotency_key": uuid4().hex}
        ),
    }
    if len(json.dumps(command_by_field, ensure_ascii=False, allow_nan=False).encode()) > 131072:
        raise ValueError("registry_ptg_office_intent_invalid")
    return command_by_field


async def reserve_registry_ptg_office_prepare_intent(
    session, source_command, selector, actor, *, session_token_sha256, control_schema=None
):
    """Reserve only; caller verifies management custody and fresh source/session authority.

    The caller must genuinely re-read the approved source and verify its approval
    before this call. It owns commit/rollback and must commit before preparation
    handoff. Replay preserves the original audit session; current authorization
    is a separate coordinator responsibility. Office Publisher cannot call this
    through its accepted privilege profile. No session, grants or commit is made.
    """
    semantic_by_field = _intent(source_command, selector, actor)
    _sha256(session_token_sha256)
    if hasattr(session, "is_in_transaction"):
        connection = session
    else:
        if not session.in_transaction():
            raise ValueError("registry_queue_requires_caller_transaction")
        connection = await native_driver(session)
    if not connection.is_in_transaction():
        raise ValueError("registry_queue_requires_caller_transaction")
    if await connection.fetchval("SELECT current_setting('transaction_isolation')") != "read committed":
        raise ValueError("registry_ptg_office_intent_requires_read_committed")
    semantic_sha256 = _digest(semantic_by_field)
    idempotency_key = _key(semantic_sha256)
    command_by_field = _new_command(semantic_by_field, semantic_sha256)
    metadata = _request_metadata(command_by_field, semantic_by_field["actor"], session_token_sha256, idempotency_key)
    namespace = _namespace(control_schema)
    async with connection.transaction():
        request_record = await connection.fetchrow(
            f"INSERT INTO {namespace}.registry_publication_request "
            "(request_id,actor_key,session_token_sha256,actor_json,command_json,idempotency_key,request_sha256) "
            "VALUES($1,$2,$3,$4::jsonb,$5::jsonb,$6,$7) "
            f"ON CONFLICT(actor_key,idempotency_key) DO NOTHING RETURNING {_COLUMNS}",
            uuid4(),
            metadata["actor_key"],
            session_token_sha256,
            metadata["actor_json"],
            metadata["command_json"],
            idempotency_key,
            metadata["request_sha256"],
        )
        is_replay = request_record is None
        if is_replay:
            request_record = await connection.fetchrow(
                f"SELECT request_id,actor_key,session_token_sha256,"
                "CASE WHEN octet_length(actor_json::text)<=2048 THEN actor_json END AS actor_json,"
                "CASE WHEN octet_length(command_json::text)<=131072 THEN command_json END AS command_json,"
                f"idempotency_key,request_sha256,state,created_at FROM {namespace}.registry_publication_request "
                "WHERE actor_key=$1 AND idempotency_key=$2",
                metadata["actor_key"],
                idempotency_key,
            )
            if request_record is None:
                raise ValueError("registry_ptg_office_intent_retry_same_semantic_key")
        return _stored_receipt(request_record, semantic_by_field, is_replay)
