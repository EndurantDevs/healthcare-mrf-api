# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded office intent discovery/projection; syntax never supplies authority."""

from datetime import datetime
from uuid import UUID

from process.registry_ptg_office_capture import _digest as capture_digest
from process.registry_ptg_office_intent import (
    _COMMAND_FIELDS,
    _SELECTOR_FIELDS,
    OPERATION,
    VERSION,
    _digest,
    _intent,
    _stored_receipt,
)
from process.registry_ptg_office_preparation import _checked_command
from process.registry_ptg_office_preparation_contract import validated_prepare_selector
from process.registry_ptg_published_office_scope import _graph_identity
from process.registry_ptg_published_plan_contract import validated_command
from process.registry_ptg_scope_engine import _object, _sha256, _uuid
from process.registry_publication_queue import _document, _json, _reactivation_actor
from process.registry_record_store import RegistryActor
from process.registry_source_observation_store import _namespace

MAXIMUM_WIRE_BYTES = 245760
_RECEIPT_FIELDS = {"request_id", "state", "created_at", "replayed", "semantic_sha256", "command_sha256", "command"}
_LIST_FIELDS = {"scope_id", "client_id", "scope_approval_sha256", "after", "limit"}


def _list_scope(source_command, approval_sha256, after, limit):
    source_command = validated_command(source_command)
    _sha256(approval_sha256)
    if after is not None:
        _uuid(after)
    if type(limit) is not int or not 1 <= limit <= 20:
        raise ValueError("registry_ptg_office_intent_list_invalid")
    return source_command


def validated_office_intent_list_envelope(document):
    """Close a signed transport request; the caller still authorizes the live session."""
    _object(document, {"actor", "session_token_sha256", "source_command", "selector"})
    actor_by_field = _object(document["actor"], {"kind", "user_id", "client_id"})
    actor = RegistryActor(actor_by_field["kind"], _uuid(actor_by_field["user_id"]), actor_by_field["client_id"])
    _reactivation_actor(actor)
    _sha256(document["session_token_sha256"])
    selector = _object(document["selector"], _LIST_FIELDS)
    source_command = _list_scope(
        document["source_command"], selector["scope_approval_sha256"], selector["after"], selector["limit"]
    )
    if any(selector[name] != source_command[name] for name in ("scope_id", "client_id")):
        raise ValueError("registry_ptg_office_intent_source_changed")
    return _document(_json(document))


def _wire_item(receipt, actor, source_command, approval_sha256):
    """Validate a trusted audited receipt without exposing its actor/session document."""
    _object(receipt, _RECEIPT_FIELDS)
    _uuid(receipt["request_id"])
    if receipt["state"] not in {"queued", "running", "completed", "rejected"} or type(receipt["replayed"]) is not bool:
        raise ValueError("registry_ptg_office_intent_stored_invalid")
    created_at = receipt["created_at"]
    if type(created_at) is not str:
        raise ValueError("registry_ptg_office_intent_stored_invalid")
    timestamp = datetime.fromisoformat(created_at)
    if timestamp.utcoffset() is None or timestamp.isoformat() != created_at:
        raise ValueError("registry_ptg_office_intent_stored_invalid")
    command = _object(receipt["command"], _COMMAND_FIELDS)
    selector = validated_prepare_selector(command["selector"])
    semantic = _intent(source_command, {name: selector[name] for name in _SELECTOR_FIELDS}, actor)
    if (
        command["operation"] != OPERATION
        or type(command["version"]) is not int
        or command["version"] != VERSION
        or command["source_command"] != source_command
        or selector["scope_approval_sha256"] != approval_sha256
        or command["semantic_sha256"] != _digest(semantic)
        or receipt["semantic_sha256"] != command["semantic_sha256"]
        or receipt["command_sha256"] != _digest(command)
    ):
        raise ValueError("registry_ptg_office_intent_stored_invalid")
    return {
        **{name: receipt[name] for name in ("request_id", "state", "created_at", "semantic_sha256", "command_sha256")},
        "selector": selector,
    }


def _page(source_command, approval_sha256, items, next_cursor):
    return {
        "source_command_sha256": capture_digest(source_command),
        "scope_approval_sha256": approval_sha256,
        "items": items,
        "next_cursor": next_cursor,
    }


def _bounded_page(source_command, approval_sha256, items, limit):
    retained_items = []
    for item in items[:limit]:
        proposed = _page(source_command, approval_sha256, [*retained_items, item], item["request_id"])
        if len(_json(proposed).encode()) > MAXIMUM_WIRE_BYTES:
            break
        retained_items.append(item)
    if items and not retained_items:
        raise ValueError("registry_ptg_office_intent_wire_too_large")
    cursor = retained_items[-1]["request_id"] if len(retained_items) < len(items) else None
    return _page(source_command, approval_sha256, retained_items, cursor)


async def list_registry_ptg_office_prepare_intents(
    connection, actor, source_command, *, scope_approval_sha256, after=None, limit=20, control_schema=None
):
    """Read only in an already authorized, native-profile-verified caller transaction.

    The caller genuinely re-reads the approved source and approval, verifies this
    connection's native identity/read custody, and checks fresh session authority
    before and after this call. A hash/actor syntax check supplies none of that.
    No transaction, privilege, handle, session, physical read or commit is made.
    """
    source_command = _list_scope(source_command, scope_approval_sha256, after, limit)
    actor_key = _digest(_reactivation_actor(actor))
    if not connection.is_in_transaction():
        raise ValueError("registry_queue_requires_caller_transaction")
    request_records = await connection.fetch(
        "SELECT request_id,actor_key,session_token_sha256,"
        "CASE WHEN octet_length(actor_json::text)<=2048 THEN actor_json END AS actor_json,"
        "CASE WHEN octet_length(command_json::text)<=131072 THEN command_json END AS command_json,"
        f"idempotency_key,request_sha256,state,created_at FROM {_namespace(control_schema)}.registry_publication_request "
        "WHERE actor_key=$1 AND command_json->'source_command'=$2::jsonb "
        "AND command_json->>'operation'=$3 AND command_json->'version'=$4::jsonb "
        "AND command_json->'selector'->>'scope_id'=$5 AND command_json->'selector'->>'client_id'=$6 "
        "AND command_json->'selector'->>'scope_approval_sha256'=$7 "
        "AND ($8::uuid IS NULL OR request_id>$8::uuid) ORDER BY request_id LIMIT $9",
        actor_key,
        _json(source_command),
        OPERATION,
        _json(VERSION),
        source_command["scope_id"],
        source_command["client_id"],
        scope_approval_sha256,
        None if after is None else UUID(after),
        limit + 1,
    )
    if len(request_records) > limit + 1:
        raise ValueError("registry_ptg_office_intent_list_invalid")
    wire_intents, previous = [], 0 if after is None else UUID(after).int
    for request_record in request_records:
        command = _object(_document(request_record["command_json"]), _COMMAND_FIELDS)
        selector = validated_prepare_selector(command["selector"])
        semantic = _intent(source_command, {name: selector[name] for name in _SELECTOR_FIELDS}, actor)
        receipt = _stored_receipt(request_record, semantic, True)
        wire_intent = _wire_item(receipt, actor, source_command, scope_approval_sha256)
        identifier = UUID(wire_intent["request_id"]).int
        if identifier <= previous:
            raise ValueError("registry_ptg_office_intent_list_invalid")
        previous = identifier
        wire_intents.append(wire_intent)
    return _bounded_page(source_command, scope_approval_sha256, wire_intents, limit)


def _prepared_source(prepared, source_command, selector):
    source = prepared["command"]["source"]
    scope = source["source_scope"]
    expected_source_by_field = {
        **{name: source_command[name] for name in ("coordinates", "legal_company_id", "file_versions", "network_id")},
        **{name: source_command["source"][name] for name in ("binding_source_key", "snapshot_id")},
    }
    # The source review stays immutable; genuine preparation verifies the current approved map.
    if (
        source["approved_revision"] < source_command["approved_revision"]
        or prepared["command"]["input_row_count"] != len(selector["selections"])
        or prepared["command"]["office_evidence_kind"] != "reviewed_exact_office"
        or source["evidence"]["graph_identity"] != _graph_identity(source_command["published_identity"])
        or any(source[name] != value for name, value in expected_source_by_field.items())
        or any(
            scope[name] != source_command[name]
            for name in ("review_type", "scope_id", "plan_id", "plan_market_type", "selection_mode")
        )
    ):
        raise ValueError("registry_ptg_office_intent_result_changed")


def _prepared_receipt(prepared, receipt):
    _object(prepared, {"capture_id", "state", "command", "command_sha256", "manifest_sha256"})
    selector = receipt["command"]["selector"]
    if prepared["state"] != "prepared" or prepared["capture_id"] != selector["capture_id"]:
        raise ValueError("registry_ptg_office_intent_result_changed")
    command = _checked_command(prepared["command"], selector)
    if capture_digest(command) != prepared["command_sha256"] or any(
        command[name] != selector[name] for name in ("reason", "retained_generation_id")
    ):
        raise ValueError("registry_ptg_office_intent_result_changed")
    for name in ("command_sha256", "manifest_sha256"):
        _sha256(prepared[name])


def project_registry_ptg_office_prepare_result(result, actor, source_command, *, scope_approval_sha256):
    """Project only a trusted coordinator result after its original audit verification.

    This pure projection neither authenticates callers nor proves capture custody.
    The coordinator owns genuine capture verification and current authority checks.
    """
    source_command = _list_scope(source_command, scope_approval_sha256, None, 1)
    _object(result, {"intent", "preparation"})
    receipt, prepared = result["intent"], result["preparation"]
    item = _wire_item(receipt, actor, source_command, scope_approval_sha256)
    if prepared is None:
        if item["state"] not in {"queued", "running"}:
            raise ValueError("registry_ptg_office_intent_result_changed")
    else:
        if item["state"] != "completed":
            raise ValueError("registry_ptg_office_intent_result_changed")
        _prepared_receipt(prepared, receipt)
        _prepared_source(prepared, source_command, item["selector"])
    projected_by_field = {
        "source_command_sha256": capture_digest(source_command),
        "scope_approval_sha256": scope_approval_sha256,
        "intent": item,
        "preparation": prepared,
    }
    if len(_json(projected_by_field).encode()) > MAXIMUM_WIRE_BYTES:
        raise ValueError("registry_ptg_office_intent_wire_too_large")
    return _document(_json(projected_by_field))
