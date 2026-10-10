# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Prepare exact historical editable content as a separately reviewed draft.

Preparation writes nothing and grants no authority. The route must verify literal
edit grants, then apply the returned command through the normal correction store.
Archive state and specialized source-network bindings require separate actions.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from uuid import UUID

from sqlalchemy import String, case, cast, func, select, true

from db.models.registry_revision import RegistryRecordHistory
from process.company_registry_assertion_values import CONTRACT
from process.registry_record_store import (
    _RECORD_MODELS,
    RegistryAddressUnavailable,
    RegistryRecordCommand,
    RegistryRecordConflict,
    _bounded_text,
    _record_identity,
    _record_table,
    _table,
    _validate_network_evidence_snapshot,
    _validated_actor,
    _validated_command,
)

MAX_UNDO_SNAPSHOT_BYTES = 1048576


@dataclass(frozen=True)
class RegistryManualUndoCommand:
    record_kind: str
    record_id: UUID | int
    expected_revision: int
    target_revision: int
    reason: str
    idempotency_key: str


@dataclass(frozen=True)
class RegistryManualUndoProvenance:
    record_kind: str
    record_id: UUID | int
    target_revision: int
    target_custom_revision: int
    target_request_sha256: str
    target_archived: bool
    current_revision: int


@dataclass(frozen=True)
class RegistryManualUndoPreparation:
    command: RegistryRecordCommand
    provenance: RegistryManualUndoProvenance


def _validate_undo(command, actor):
    if (
        not isinstance(command, RegistryManualUndoCommand)
        or type(command.record_kind) is not str
        or command.record_kind not in _RECORD_MODELS
        or command.record_kind == "network_binding"
        or type(command.expected_revision) is not int
        or not 1 < command.expected_revision < 9223372036854775807
        or type(command.target_revision) is not int
        or not 0 < command.target_revision < command.expected_revision
    ):
        raise ValueError("registry_manual_undo_request_invalid")
    _record_identity(command.record_kind, command.record_id)
    _validated_actor(actor)
    _bounded_text(command.reason, 1000, "reason")
    if _bounded_text(command.idempotency_key, 128, "idempotency_key") != command.idempotency_key:
        raise ValueError("registry_idempotency_key_invalid")


async def _read_undo_history(session, command, schema):
    head, identity_column = _record_table(command.record_kind, schema)
    history = _table(RegistryRecordHistory, schema)
    target_history, expected_history = history.alias("undo_target"), history.alias("undo_expected")
    statement = (
        select(
            head.c.revision.label("current_revision"),
            case(
                (
                    func.octet_length(cast(target_history.c.record_json, String)) <= MAX_UNDO_SNAPSHOT_BYTES,
                    target_history.c.record_json,
                ),
                else_=None,
            ).label("record_json"),
            target_history.c.custom_revision,
            target_history.c.request_sha256,
            expected_history.c.custom_revision.label("expected_custom_revision"),
            expected_history.c.record_json[identity_column].astext.label("expected_identity"),
            func.jsonb_typeof(expected_history.c.record_json[identity_column]).label("expected_identity_type"),
            expected_history.c.record_json["revision"].astext.label("expected_record_revision"),
            func.jsonb_typeof(expected_history.c.record_json["revision"]).label("expected_revision_type"),
        )
        .select_from(
            head.outerjoin(
                target_history,
                (target_history.c.record_kind == command.record_kind)
                & (target_history.c.record_key == str(command.record_id))
                & (target_history.c.revision == command.target_revision),
            ).outerjoin(
                expected_history,
                (expected_history.c.record_kind == command.record_kind)
                & (expected_history.c.record_key == str(command.record_id))
                & (expected_history.c.revision == command.expected_revision),
            )
        )
        .where(head.c[identity_column] == command.record_id)
    )
    history_by_field = (await session.execute(statement)).mappings().one_or_none()
    if (
        history_by_field is None
        or history_by_field["expected_identity"] != str(command.record_id)
        or history_by_field["expected_identity_type"] != ("number" if type(command.record_id) is int else "string")
        or history_by_field["expected_record_revision"] != str(command.expected_revision)
        or history_by_field["expected_revision_type"] != "number"
        or history_by_field["current_revision"] < command.expected_revision
        or history_by_field["custom_revision"] is None
        or history_by_field["expected_custom_revision"] is None
        or not 0
        < history_by_field["custom_revision"]
        < history_by_field["expected_custom_revision"]
        <= 9223372036854775807
    ):
        raise RegistryRecordConflict("registry_manual_undo_history_conflict")
    return history_by_field, identity_column


def _prepare_correction(command, history_by_field, identity_column):
    snapshot = history_by_field["record_json"]
    if (
        type(snapshot) is not dict
        or type(snapshot.get("revision")) is not int
        or snapshot["revision"] != command.target_revision
        or type(snapshot.get(identity_column)) is not (int if type(command.record_id) is int else str)
        or snapshot.get(identity_column)
        != (command.record_id if type(command.record_id) is int else str(command.record_id))
        or type(snapshot.get("archived")) is not bool
        or type(history_by_field["request_sha256"]) is not str
        or re.fullmatch(r"[0-9a-f]{64}", history_by_field["request_sha256"]) is None
    ):
        raise ValueError("registry_manual_undo_history_invalid")
    editable_fields = _RECORD_MODELS[command.record_kind][2]
    if not editable_fields <= snapshot.keys():
        raise ValueError("registry_manual_undo_history_invalid")
    fields_by_name = {field: snapshot[field] for field in editable_fields}
    if command.record_kind == "company":
        fields_by_name["assertions"] = {
            "contract": CONTRACT,
            "company_id": str(command.record_id),
            "expected_revision": command.expected_revision,
            "role_assertions": snapshot.get("role_assertions", []),
            "identifier_assertions": snapshot.get("identifier_assertions", []),
        }
    if command.record_kind == "company_links":
        fields_by_name["network_assertions"] = snapshot.get("network_assertions", [])
    if command.record_kind == "network":
        fields_by_name["catalog_evidence_json"] = _network_undo_evidence(snapshot, command)
    reason = f"Undo revision {command.target_revision} [{history_by_field['request_sha256']}]: {command.reason.strip()}"
    if len(reason) > 1000:
        raise ValueError("registry_manual_undo_reason_invalid")
    correction = RegistryRecordCommand(
        command.record_kind,
        command.record_id,
        "correct",
        command.expected_revision,
        fields_by_name,
        reason,
        command.idempotency_key,
    )
    _validated_command(correction)
    return RegistryManualUndoPreparation(
        correction,
        RegistryManualUndoProvenance(
            command.record_kind,
            command.record_id,
            command.target_revision,
            history_by_field["custom_revision"],
            history_by_field["request_sha256"],
            snapshot["archived"],
            history_by_field["current_revision"],
        ),
    )


def _network_undo_evidence(snapshot, command):
    """Restore reviewed historical references under the new correction context."""
    evidence = snapshot.get("catalog_evidence_json")
    if evidence is None:
        return None
    if (
        type(evidence) is not dict
        or type(evidence.get("network_id")) is not int
        or evidence["network_id"] != command.record_id
        or type(evidence.get("expected_record_revision")) is not int
        or not 0 < evidence["expected_record_revision"] < snapshot["revision"]
    ):
        raise ValueError("registry_manual_undo_history_invalid")
    return {**evidence, "network_id": command.record_id, "expected_record_revision": command.expected_revision}


async def prepare_registry_manual_undo(session, command: RegistryManualUndoCommand, actor, *, schema=None):
    """Reconstruct a correction and durable target reason without mutating history.

    The caller owns a clean transaction and subsequently uses
    ``apply_registry_record_command`` for semantic checks and CAS. An advanced
    head is allowed here so a completed retry reconstructs the same command;
    generic actor-bound idempotency decides replay versus a stale new request.
    Historical archive state is provenance only, never an implicit restore.
    """
    _validate_undo(command, actor)
    if not session.in_transaction() or session.new or session.dirty or session.deleted:
        raise ValueError("registry_requires_clean_caller_transaction")
    row, identity_column = await _read_undo_history(session, command, schema)
    return _prepare_correction(command, row, identity_column)


async def _read_history_page(session, kind, record_id, limit, offset, schema):
    history = _table(RegistryRecordHistory, schema)
    page = (
        select(
            history.c.revision,
            history.c.custom_revision,
            history.c.reason,
            history.c.record_json,
        )
        .where(history.c.record_kind == kind, history.c.record_key == str(record_id))
        .order_by(history.c.revision.desc())
        .limit(limit + 1)
        .offset(offset)
        .cte("history_page")
    )
    ranked = select(page, func.row_number().over(order_by=page.c.revision.desc()).label("ordinal")).cte("ranked_page")
    bounds = select(
        func.coalesce(
            func.sum(
                case(
                    (
                        ranked.c.ordinal <= limit,
                        func.octet_length(cast(ranked.c.record_json, String))
                        + func.octet_length(ranked.c.reason)
                        + 256,
                    ),
                    else_=0,
                )
            ),
            0,
        ).label("bytes")
    ).cte("page_bounds")
    history_query_result = await session.execute(
        select(
            ranked.c.revision,
            ranked.c.custom_revision,
            ranked.c.reason,
            case(
                ((ranked.c.ordinal <= limit) & (bounds.c.bytes <= MAX_UNDO_SNAPSHOT_BYTES), ranked.c.record_json),
                else_=None,
            ).label("record"),
        )
        .select_from(ranked.join(bounds, true()))
        .order_by(ranked.c.ordinal)
    )
    return history_query_result.mappings().all()


async def read_registry_manual_history(session, kind, record_id, *, limit=50, offset=0, schema=None):
    """Read one bounded manual history page; the caller owns a pinned transaction."""
    if (
        type(kind) is not str
        or kind not in _RECORD_MODELS
        or kind == "network_binding"
        or type(limit) is not int
        or not 1 <= limit <= 50
        or type(offset) is not int
        or not 0 <= offset <= 1000000
        or not session.in_transaction()
        or session.new
        or session.dirty
        or session.deleted
    ):
        raise ValueError("registry_manual_history_request_invalid")
    _record_identity(kind, record_id)
    head, identity_column = _record_table(kind, schema)
    current_revision = await session.scalar(select(head.c.revision).where(head.c[identity_column] == record_id))
    if current_revision is None:
        return None
    history_rows = await _read_history_page(session, kind, record_id, limit, offset, schema)
    history_records = [dict(history_row) for history_row in history_rows[:limit]]
    for history_record in history_records:
        snapshot = history_record["record"]
        if (
            type(snapshot) is not dict
            or type(snapshot.get("revision")) is not int
            or snapshot.get("revision") != history_record["revision"]
            or type(snapshot.get(identity_column)) is not (int if type(record_id) is int else str)
            or snapshot.get(identity_column) != (record_id if type(record_id) is int else str(record_id))
        ):
            raise RegistryAddressUnavailable("registry_manual_history_unavailable")
        if kind == "network":
            _validate_network_evidence_snapshot(snapshot)
    history_by_field = {
        "record_kind": kind,
        "record_id": record_id if type(record_id) is int else str(record_id),
        "current_revision": current_revision,
        "limit": limit,
        "offset": offset,
        "has_more": len(history_rows) > limit,
        "records": history_records,
    }
    if len(json.dumps(history_by_field, ensure_ascii=False, separators=(",", ":")).encode()) > MAX_UNDO_SNAPSHOT_BYTES:
        raise RegistryAddressUnavailable("registry_manual_history_unavailable")
    return history_by_field
