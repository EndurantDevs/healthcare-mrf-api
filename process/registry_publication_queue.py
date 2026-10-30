# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Low-volume idempotent requests and fenced controller leases."""

import hashlib
import json
import re
from dataclasses import dataclass
from typing import Literal
from uuid import UUID, uuid4, uuid5

from process.registry_approval_store import (
    RegistryApprovalCommand,
    RegistryApprovalConflict,
    _canonical_selection,
    _validated_command,
)
from process.registry_record_store import RegistryActor, _bounded_text, _validated_actor
from process.registry_source_observation_store import _namespace

_CLAIM_OPERATIONS = {
    "publication": (
        "reactivate_generation",
        "prepare_source_rollback",
        "publish_source_rollback",
        "publish_initial_inventory",
        "publish_fhir_catalog",
    ),
    "office": ("prepare_ptg_office_capture", "approve_ptg_office_capture"),
}
SOURCE_ROLLBACK_PROVENANCE = "registry_source_rollback"


def publication_json(document):
    """Serialize publication identity deterministically and reject non-finite values."""
    return json.dumps(document, sort_keys=True, separators=(",", ":"), allow_nan=False)


_json = publication_json


def publication_document(value):
    """Decode stored JSON text while preserving already-decoded queue documents."""
    return json.loads(value) if isinstance(value, str) else value


_document = publication_document


def _wire_actor(value):
    actor_by_name = dict(_document(value))
    if actor_by_name.get("impersonator_id") is None:
        actor_by_name.pop("impersonator_id", None)
    return actor_by_name


def _head(value):
    if type(value) is not int or not 0 <= value < 2**63:
        raise ValueError("registry_serving_generation_invalid")
    return value


@dataclass(frozen=True)
class RegistryReactivationCommand:
    target_generation: int
    expected_serving_generation: int
    expected_approved_revision: int
    reason: str
    idempotency_key: str


def _reactivation_document(command):
    if type(command) is not RegistryReactivationCommand:
        raise ValueError("registry_reactivation_command_invalid")
    for generation in (command.target_generation, command.expected_serving_generation):
        if _head(generation) == 0:
            raise ValueError("registry_reactivation_command_invalid")
    _head(command.expected_approved_revision)
    reason = _bounded_text(command.reason, 512, "reason")
    if reason != command.reason or not reason.isprintable() or len(reason.encode("utf-8")) > 512:
        raise ValueError("registry_reactivation_reason_invalid")
    if _bounded_text(command.idempotency_key, 128, "idempotency_key") != command.idempotency_key:
        raise ValueError("registry_idempotency_key_invalid")
    return {
        "operation": "reactivate_generation",
        "target_generation": command.target_generation,
        "expected_serving_generation": command.expected_serving_generation,
        "expected_approved_revision": command.expected_approved_revision,
        "reason": reason,
    }


def validated_registry_reactivation_command(document_by_name):
    """Return a fresh exact six-field command shared by trusted transports."""
    if (
        type(document_by_name) is not dict
        or set(document_by_name)
        != {
            "operation",
            "target_generation",
            "expected_serving_generation",
            "expected_approved_revision",
            "reason",
            "idempotency_key",
        }
        or document_by_name["operation"] != "reactivate_generation"
    ):
        raise ValueError("registry_reactivation_command_invalid")
    command = RegistryReactivationCommand(
        document_by_name["target_generation"],
        document_by_name["expected_serving_generation"],
        document_by_name["expected_approved_revision"],
        document_by_name["reason"],
        document_by_name["idempotency_key"],
    )
    return {**_reactivation_document(command), "idempotency_key": command.idempotency_key}


def _reactivation_actor(actor):
    actor_by_name = _validated_actor(actor)
    if actor.kind != "platform_admin" or actor.client_id != "system" or actor.impersonator_id is not None:
        raise ValueError("registry_reactivation_actor_invalid")
    return actor_by_name


def validated_registry_initial_inventory_command(document_by_name):
    """Bind initial publication to one genuine retained bootstrap and zero inventory."""
    try:
        if type(document_by_name) is not dict or set(document_by_name) != {
            "operation",
            "bootstrap_id",
            "bootstrap_generation_sha256",
            "expected_serving_generation",
            "expected_approved_revision",
            "reason",
            "idempotency_key",
        }:
            raise ValueError
        identity = UUID(document_by_name["bootstrap_id"])
        if (
            document_by_name["operation"] != "publish_initial_inventory"
            or not identity.int
            or str(identity) != document_by_name["bootstrap_id"]
            or type(document_by_name["bootstrap_generation_sha256"]) is not str
            or re.fullmatch(r"[0-9a-f]{64}", document_by_name["bootstrap_generation_sha256"]) is None
            or _head(document_by_name["expected_serving_generation"]) != 0
            or _head(document_by_name["expected_approved_revision"]) != 0
        ):
            raise ValueError
        _reactivation_document(
            RegistryReactivationCommand(
                1,
                1,
                0,
                document_by_name["reason"],
                document_by_name["idempotency_key"],
            )
        )
        return dict(document_by_name)
    except ValueError, TypeError, AttributeError, UnicodeError:
        raise ValueError("registry_initial_inventory_command_invalid") from None


def _initial_inventory_record(request_record):
    stored = _document(request_record["command_json"])
    if type(stored) is not dict or "idempotency_key" in stored:
        raise ValueError("registry_initial_inventory_command_invalid")
    return validated_registry_initial_inventory_command(
        {
            **stored,
            "idempotency_key": request_record["idempotency_key"],
        }
    ), _administrator_record_actor(request_record)


def _initial_inventory_result(result_by_name, command_by_name, state):
    if state in {"queued", "running", "rejected"}:
        _reactivation_result(result_by_name, command_by_name, state)
        return
    try:
        if type(result_by_name) is not dict or set(result_by_name) != {
            "operation",
            "candidate_id",
            "previous_generation_id",
            "generation_id",
            "approved_custom_revision",
            "membership_rows",
            "manifest_sha256",
            "policy_revision",
        }:
            raise ValueError
        identity = UUID(result_by_name["candidate_id"])
        if not identity.int or str(identity) != result_by_name["candidate_id"]:
            raise ValueError
        if (
            result_by_name["operation"] != "publish_initial_inventory"
            or any(
                _head(result_by_name[field]) != 0
                for field in (
                    "previous_generation_id",
                    "approved_custom_revision",
                    "membership_rows",
                )
            )
            or _head(result_by_name["generation_id"]) == 0
            or type(result_by_name["manifest_sha256"]) is not str
            or re.fullmatch(r"[0-9a-f]{64}", result_by_name["manifest_sha256"]) is None
        ):
            raise ValueError
        _head(result_by_name["policy_revision"])
    except ValueError, TypeError, AttributeError:
        raise ValueError("registry_initial_inventory_result_invalid") from None


def _retained_operation_record(request_record, operation):
    command_by_name = _document(request_record["command_json"])
    if (
        type(command_by_name) is not dict
        or set(command_by_name)
        != {"operation", "target_generation", "expected_serving_generation", "expected_approved_revision", "reason"}
        or command_by_name["operation"] != operation
    ):
        raise ValueError("registry_reactivation_command_invalid")
    command = RegistryReactivationCommand(
        command_by_name["target_generation"],
        command_by_name["expected_serving_generation"],
        command_by_name["expected_approved_revision"],
        command_by_name["reason"],
        request_record["idempotency_key"],
    )
    if {**_reactivation_document(command), "operation": operation} != command_by_name:
        raise ValueError("registry_reactivation_command_invalid")
    return command, _administrator_record_actor(request_record)


async def require_registry_source_rollback_composition(
    connection, request_id, source_manifest, approved_revision, expected_head, *, control_schema=None
):
    """Authenticate the existing exact historical-source operation, never a head mismatch."""
    request_record = await connection.fetchrow(
        f"SELECT * FROM {_namespace(control_schema)}.registry_publication_request WHERE request_id=$1",
        request_id,
    )
    if (
        request_record is None
        or _document(request_record["command_json"]).get("operation") != "prepare_source_rollback"
    ):
        if source_manifest.generation_id != expected_head:
            raise ValueError("registry_source_rollback_operation_required")
        return None
    command, actor = _retained_operation_record(request_record, "prepare_source_rollback")
    metadata = _request_metadata(
        {**_reactivation_document(command), "operation": "prepare_source_rollback"},
        _reactivation_actor(actor),
        request_record["session_token_sha256"],
        command.idempotency_key,
    )
    if (command.target_generation, command.expected_serving_generation, command.expected_approved_revision) != (
        source_manifest.generation_id,
        expected_head,
        approved_revision,
    ) or metadata["request_sha256"] != request_record["request_sha256"]:
        raise ValueError("registry_source_rollback_operation_conflict")
    if (
        request_record["state"] != "running"
        or request_record["lease_id"] is None
        or not await connection.fetchval("SELECT $1::timestamptz>clock_timestamp()", request_record["lease_expires_at"])
    ):
        raise ValueError("registry_source_rollback_operation_conflict")
    return request_record["request_sha256"]


async def registry_source_rollback_composition_provenance(
    connection, request_id, source_manifest, approved_revision, expected_head, *, control_schema=None
):
    """Bind the existing reviewed operation even when its historical target has no FHIR."""
    digest = await require_registry_source_rollback_composition(
        connection, request_id, source_manifest, approved_revision, expected_head, control_schema=control_schema
    )
    if digest is None:
        return None
    return {
        "component": SOURCE_ROLLBACK_PROVENANCE,
        "revision": 1,
        "request_id": str(request_id),
        "request_sha256": digest,
        "target_generation": source_manifest.generation_id,
        "manifest_sha256": source_manifest.manifest_sha256,
        "approved_revision": approved_revision,
        "expected_head": expected_head,
    }


def registry_source_rollback_provenance_sha256(receipt):
    """Hash the protected historical-operation identity retained by a candidate."""
    return hashlib.sha256(_json(receipt).encode()).hexdigest()


def verify_registry_source_rollback_provenance(candidate):
    """Check paired immutable metadata; native operation authority is rechecked separately."""
    report = _document(candidate.get("validation_json") or {})
    generations_by_name = _document(candidate["source_generations"])
    if SOURCE_ROLLBACK_PROVENANCE not in report and SOURCE_ROLLBACK_PROVENANCE not in generations_by_name:
        return None
    receipt = report.get(SOURCE_ROLLBACK_PROVENANCE)
    fields = {
        "component",
        "revision",
        "request_id",
        "request_sha256",
        "target_generation",
        "manifest_sha256",
        "approved_revision",
        "expected_head",
    }
    if type(receipt) is not dict or set(receipt) != fields:
        raise ValueError("registry_source_rollback_provenance_invalid")
    try:
        if (
            receipt["component"] != SOURCE_ROLLBACK_PROVENANCE
            or type(receipt["revision"]) is not int
            or receipt["revision"] != 1
            or type(receipt["request_id"]) is not str
            or str(UUID(receipt["request_id"])) != receipt["request_id"]
            or not UUID(receipt["request_id"]).int
            or any(
                type(receipt[key]) is not str or re.fullmatch("[0-9a-f]{64}", receipt[key]) is None
                for key in ("request_sha256", "manifest_sha256")
            )
            or any(
                type(receipt[key]) is not int or not 0 <= receipt[key] < 2**63
                for key in ("approved_revision", "expected_head", "target_generation")
            )
            or not receipt["target_generation"]
            or not receipt["expected_head"]
            or (receipt["approved_revision"], receipt["expected_head"])
            != (candidate["approved_custom_revision"], candidate["expected_head"])
            or generations_by_name.get("retained_network_source") != receipt["manifest_sha256"]
            or str(candidate["dataset_id"]) != str(uuid5(UUID(receipt["request_id"]), "dataset"))
            or generations_by_name.get(SOURCE_ROLLBACK_PROVENANCE)
            != registry_source_rollback_provenance_sha256(receipt)
        ):
            raise ValueError
    except ValueError, TypeError, AttributeError:
        raise ValueError("registry_source_rollback_provenance_invalid") from None
    return dict(receipt)


def _administrator_record_actor(request_record):
    actor_by_name = _document(request_record["actor_json"])
    if type(actor_by_name) is not dict or set(actor_by_name) != {"kind", "user_id", "client_id", "impersonator_id"}:
        raise ValueError("registry_reactivation_actor_invalid")
    actor = RegistryActor(
        actor_by_name["kind"],
        UUID(actor_by_name["user_id"]),
        actor_by_name["client_id"],
        actor_by_name["impersonator_id"],
    )
    if _reactivation_actor(actor) != actor_by_name:
        raise ValueError("registry_reactivation_actor_invalid")
    return actor


def _reactivation_record(request_record):
    return _retained_operation_record(request_record, "reactivate_generation")


def validated_registry_source_rollback_preview_command(document_by_name):
    """Validate the distinct administrator preparation command without normalization."""
    if type(document_by_name) is not dict or document_by_name.get("operation") != "prepare_source_rollback":
        raise ValueError("registry_source_rollback_command_invalid")
    validated = validated_registry_reactivation_command({**document_by_name, "operation": "reactivate_generation"})
    return {**validated, "operation": "prepare_source_rollback"}


def validated_registry_source_rollback_publication_command(document_by_name):
    """Bind an explicit publication to the exact reviewed preview and digest."""
    required_fields = {
        "operation",
        "preview_request_id",
        "candidate_sha256",
        "expected_serving_generation",
        "expected_approved_revision",
        "reason",
        "idempotency_key",
    }
    try:
        if type(document_by_name) is not dict or set(document_by_name) != required_fields:
            raise ValueError
        if document_by_name["operation"] != "publish_source_rollback":
            raise ValueError
        identity = UUID(document_by_name["preview_request_id"])
        if not identity.int or str(identity) != document_by_name["preview_request_id"]:
            raise ValueError
        if (
            type(document_by_name["candidate_sha256"]) is not str
            or re.fullmatch(r"[0-9a-f]{64}", document_by_name["candidate_sha256"]) is None
        ):
            raise ValueError
        _reactivation_document(
            RegistryReactivationCommand(
                1,
                document_by_name["expected_serving_generation"],
                document_by_name["expected_approved_revision"],
                document_by_name["reason"],
                document_by_name["idempotency_key"],
            )
        )
        return dict(document_by_name)
    except ValueError, TypeError, AttributeError, UnicodeError:
        raise ValueError("registry_source_rollback_publication_command_invalid") from None


def _source_rollback_publication_record(request_record):
    stored = _document(request_record["command_json"])
    if type(stored) is not dict or "idempotency_key" in stored:
        raise ValueError("registry_source_rollback_publication_command_invalid")
    command_by_name = validated_registry_source_rollback_publication_command(
        {
            **stored,
            "idempotency_key": request_record["idempotency_key"],
        }
    )
    return command_by_name, _administrator_record_actor(request_record)


def _source_rollback_publication_result(result_by_name, command_by_name, state):
    if state in {"queued", "running", "rejected"}:
        _reactivation_result(result_by_name, command_by_name, state)
        return
    required_fields = {
        "operation",
        "preview_request_id",
        "candidate_id",
        "candidate_sha256",
        "previous_generation_id",
        "generation_id",
        "approved_custom_revision",
        "manifest_sha256",
    }
    try:
        if type(result_by_name) is not dict or set(result_by_name) != required_fields:
            raise ValueError
        for field in ("preview_request_id", "candidate_id"):
            identity = UUID(result_by_name[field])
            if not identity.int or str(identity) != result_by_name[field]:
                raise ValueError
        for field in ("candidate_sha256", "manifest_sha256"):
            if type(result_by_name[field]) is not str or re.fullmatch(r"[0-9a-f]{64}", result_by_name[field]) is None:
                raise ValueError
        for field in ("previous_generation_id", "generation_id", "approved_custom_revision"):
            _head(result_by_name[field])
        if (
            result_by_name["operation"] != "publish_source_rollback"
            or result_by_name["preview_request_id"] != command_by_name["preview_request_id"]
            or result_by_name["candidate_sha256"] != command_by_name["candidate_sha256"]
            or result_by_name["previous_generation_id"] != command_by_name["expected_serving_generation"]
            or result_by_name["approved_custom_revision"] != command_by_name["expected_approved_revision"]
            or result_by_name["generation_id"] <= result_by_name["previous_generation_id"]
        ):
            raise ValueError
    except ValueError, TypeError, AttributeError:
        raise ValueError("registry_source_rollback_publication_result_invalid") from None


def _source_rollback_diff(result_by_name):
    if type(result_by_name["diff_version"]) is not int or result_by_name["diff_version"] != 1:
        raise ValueError
    if type(result_by_name["changes"]) is not int or not 0 <= result_by_name["changes"] <= 2**53 - 1:
        raise ValueError
    _source_rollback_samples(result_by_name)
    _source_rollback_validation(result_by_name["validation"])


def _source_rollback_samples(result_by_name):
    samples_by_category = result_by_name["samples"]
    if type(samples_by_category) is not dict or set(samples_by_category) != {"additions", "removals", "changes"}:
        raise ValueError
    seen_units = set()
    for category, units in samples_by_category.items():
        if type(units) is not list or len(units) > min(20, result_by_name[category]):
            raise ValueError
        previous = None
        for unit in units:
            if type(unit) is not list or len(unit) != 4:
                raise ValueError
            network, system, provider, location = unit
            if (
                type(network) is not int
                or not 0 < network < 2**31
                or system not in ("npi", "provider_directory", "manual")
                or type(provider) is not str
                or not provider.strip(" ")
                or len(provider.encode("utf-8")) > 1024
                or type(location) is not str
                or str(UUID(location)) != location
                or not UUID(location).int
                or previous is not None
                and tuple(unit) <= previous
                or tuple(unit) in seen_units
            ):
                raise ValueError
            previous = tuple(unit)
            seen_units.add(previous)
    if len(json.dumps(samples_by_category, sort_keys=True, allow_nan=False).encode("utf-8")) > 64 * 1024:
        raise ValueError


def _source_rollback_validation(validation_by_name):
    counters = {
        "membership_rows",
        "distinct_memberships",
        "projected_locations",
        "orphan_bindings",
        "unknown_network_rows",
    }
    if type(validation_by_name) is not dict or set(validation_by_name) != counters | {
        "state",
        "validated",
        "index_ready",
        "writer_closed",
    }:
        raise ValueError
    if validation_by_name["state"] != "ready" or any(
        validation_by_name[field] is not True for field in ("validated", "index_ready", "writer_closed")
    ):
        raise ValueError
    if any(
        type(validation_by_name[field]) is not int or not 0 <= validation_by_name[field] <= 2**53 - 1
        for field in counters
    ):
        raise ValueError
    if (
        not validation_by_name["projected_locations"]
        <= validation_by_name["distinct_memberships"]
        <= validation_by_name["membership_rows"]
        or validation_by_name["unknown_network_rows"] != 0
    ):
        raise ValueError


def _source_rollback_result(result_by_name, command, state):
    if state in {"queued", "running", "rejected"}:
        _reactivation_result(result_by_name, command, state)
        return
    required_fields = {
        "operation",
        "candidate_id",
        "source_generation_id",
        "expected_serving_generation",
        "approved_custom_revision",
        "candidate_sha256",
        "additions",
        "removals",
        "unresolved_rows",
        "source_selection",
    }
    extended_fields = required_fields | {"diff_version", "changes", "samples", "validation"}
    if type(result_by_name) is not dict or set(result_by_name) not in (required_fields, extended_fields):
        raise ValueError("registry_source_rollback_result_invalid")
    try:
        if set(result_by_name) == extended_fields:
            _source_rollback_diff(result_by_name)
        candidate_id = UUID(result_by_name["candidate_id"])
        if not candidate_id.int or str(candidate_id) != result_by_name["candidate_id"]:
            raise ValueError
        selection = result_by_name["source_selection"]
        if type(selection) is not dict or set(selection) != {"mapped_rows", "omitted_rows"}:
            raise ValueError
        counters = [result_by_name[field] for field in ("additions", "removals", "unresolved_rows")]
        counters.extend(selection.values())
        if any(type(count) is not int or not 0 <= count <= 2**53 - 1 for count in counters):
            raise ValueError
        if sum(selection.values()) > 2**53 - 1 or result_by_name["unresolved_rows"] != 0:
            raise ValueError
        for field in ("source_generation_id", "expected_serving_generation", "approved_custom_revision"):
            _head(result_by_name[field])
        if (
            result_by_name["operation"] != "prepare_source_rollback"
            or result_by_name["source_generation_id"] != command.target_generation
            or result_by_name["expected_serving_generation"] != command.expected_serving_generation
            or result_by_name["approved_custom_revision"] != command.expected_approved_revision
            or type(result_by_name["candidate_sha256"]) is not str
            or re.fullmatch(r"[0-9a-f]{64}", result_by_name["candidate_sha256"]) is None
        ):
            raise ValueError
    except ValueError, TypeError, AttributeError:
        raise ValueError("registry_source_rollback_result_invalid") from None


def _reactivation_result(result_by_name, command, state):
    if state in {"queued", "running"}:
        if result_by_name is not None:
            raise ValueError("registry_reactivation_result_invalid")
        return
    if state == "rejected":
        if result_by_name != {"error": {"code": "registry_publication_rejected"}}:
            raise ValueError("registry_reactivation_result_invalid")
        return
    if type(result_by_name) is not dict or set(result_by_name) != {
        "operation",
        "previous_generation_id",
        "generation_id",
        "approved_custom_revision",
        "manifest_sha256",
        "changed",
    }:
        raise ValueError("registry_reactivation_result_invalid")
    for name in ("previous_generation_id", "generation_id", "approved_custom_revision"):
        _head(result_by_name[name])
    if (
        result_by_name["operation"] != "reactivate_generation"
        or result_by_name["previous_generation_id"] != command.expected_serving_generation
        or result_by_name["generation_id"] != command.target_generation
        or result_by_name["approved_custom_revision"] != command.expected_approved_revision
        or type(result_by_name["changed"]) is not bool
        or result_by_name["changed"] != (command.target_generation != command.expected_serving_generation)
        or type(result_by_name["manifest_sha256"]) is not str
        or re.fullmatch(r"[0-9a-f]{64}", result_by_name["manifest_sha256"]) is None
    ):
        raise ValueError("registry_reactivation_result_invalid")


def queued_publication_receipt(row, replayed):
    """Return only the request identity, state, timestamp and replay flag."""
    return {
        "request_id": str(row["request_id"]),
        "state": row["state"],
        "created_at": row["created_at"].isoformat(),
        "replayed": replayed,
    }


_queued_receipt = queued_publication_receipt


def _queue_metadata(command, actor_document, selected_versions_json, session_hash, expected_generation):
    command_by_name = {
        "expected_draft_revision": command.expected_draft_revision,
        "expected_approved_revision": command.expected_approved_revision,
        "expected_serving_generation": expected_generation,
        "selection": json.loads(selected_versions_json),
        "reason": command.reason.strip(),
    }

    return _request_metadata(command_by_name, actor_document, session_hash, command.idempotency_key)


def publication_request_metadata(command_by_name, actor_document, session_hash, idempotency_key):
    """Bind the command, actor, session hash and idempotency key to stable digests."""
    actor_json = _json(actor_document)
    request_json = _json(
        {
            "command": command_by_name,
            "actor": actor_document,
            "session_token_sha256": session_hash,
            "idempotency_key": idempotency_key,
        }
    )
    return {
        "actor_key": hashlib.sha256(actor_json.encode()).hexdigest(),
        "actor_json": actor_json,
        "command_json": _json(command_by_name),
        "request_sha256": hashlib.sha256(request_json.encode()).hexdigest(),
    }


_request_metadata = publication_request_metadata


async def enqueue_registry_reactivation(connection, command, actor, *, session_token_sha256, control_schema=None):
    """Queue a distinct administrator operation without approving any records."""
    return await _enqueue_retained_operation(
        connection, command, actor, session_token_sha256=session_token_sha256, control_schema=control_schema
    )


async def enqueue_registry_source_rollback_preview(
    connection, document_by_name, actor, *, session_token_sha256, control_schema=None
):
    """Queue preparation only; the ordinary API never assumes publisher authority."""
    validated = validated_registry_source_rollback_preview_command(document_by_name)
    command = RegistryReactivationCommand(**{key: value for key, value in validated.items() if key != "operation"})
    return await _enqueue_retained_operation(
        connection,
        command,
        actor,
        session_token_sha256=session_token_sha256,
        control_schema=control_schema,
        operation="prepare_source_rollback",
    )


async def _enqueue_retained_operation(
    connection, command, actor, *, session_token_sha256, control_schema=None, operation="reactivate_generation"
):
    """Queue a distinct administrator operation without approving any records."""
    return await _enqueue_administrator_document(
        connection,
        {**_reactivation_document(command), "operation": operation},
        actor,
        session_token_sha256=session_token_sha256,
        idempotency_key=command.idempotency_key,
        control_schema=control_schema,
    )


async def enqueue_registry_source_rollback_publication(
    connection, document_by_name, actor, *, session_token_sha256, control_schema=None
):
    """Queue explicit publication only after the administrator reviewed a preview."""
    command_by_name = validated_registry_source_rollback_publication_command(document_by_name)
    return await _enqueue_administrator_document(
        connection,
        {key: value for key, value in command_by_name.items() if key != "idempotency_key"},
        actor,
        session_token_sha256=session_token_sha256,
        idempotency_key=command_by_name["idempotency_key"],
        control_schema=control_schema,
    )


async def enqueue_registry_initial_inventory(
    connection, document_by_name, actor, *, session_token_sha256, control_schema=None
):
    """Queue initial inventory without creating an approval or changing drafts."""
    command_by_name = validated_registry_initial_inventory_command(document_by_name)
    return await _enqueue_administrator_document(
        connection,
        {key: value for key, value in command_by_name.items() if key != "idempotency_key"},
        actor,
        session_token_sha256=session_token_sha256,
        idempotency_key=command_by_name["idempotency_key"],
        control_schema=control_schema,
    )


async def _enqueue_administrator_document(
    connection, command_by_name, actor, *, session_token_sha256, idempotency_key, control_schema=None
):
    if not connection.is_in_transaction():
        raise ValueError("registry_queue_requires_caller_transaction")
    if type(session_token_sha256) is not str or re.fullmatch(r"[0-9a-f]{64}", session_token_sha256) is None:
        raise ValueError("registry_session_hash_invalid")
    metadata = _request_metadata(command_by_name, _reactivation_actor(actor), session_token_sha256, idempotency_key)
    namespace = _namespace(control_schema)
    async with connection.transaction():
        replay = await _existing_request(connection, namespace, metadata, idempotency_key)
        if replay is not None:
            return replay
        current = await connection.fetchrow(
            f"SELECT approved_revision,coalesce(generation_id,0) AS generation_id FROM {namespace}.registry_revision_control "
            f"CROSS JOIN {namespace}.network_serving_control WHERE registry_revision_control.id=1 AND network_serving_control.id=1"
        )
        if current is None or tuple(current) != (
            command_by_name["expected_approved_revision"],
            command_by_name["expected_serving_generation"],
        ):
            raise RegistryApprovalConflict("registry_queue_revision_conflict")
        if command_by_name["operation"] == "publish_initial_inventory":
            await _require_empty_initial_inventory(connection, namespace)
        inserted = await connection.fetchrow(
            f"INSERT INTO {namespace}.registry_publication_request "
            "(request_id,actor_key,session_token_sha256,actor_json,command_json,idempotency_key,request_sha256) "
            "VALUES($1,$2,$3,$4::jsonb,$5::jsonb,$6,$7) ON CONFLICT(actor_key,idempotency_key) DO NOTHING RETURNING *",
            uuid4(),
            metadata["actor_key"],
            session_token_sha256,
            metadata["actor_json"],
            metadata["command_json"],
            idempotency_key,
            metadata["request_sha256"],
        )
        if inserted is None:
            replay = await _existing_request(connection, namespace, metadata, idempotency_key)
            if replay is None:
                raise RegistryApprovalConflict("registry_queue_idempotency_conflict")
            return replay
        return _queued_receipt(inserted, False)


async def _require_empty_initial_inventory(connection, namespace):
    if await connection.fetchval(
        f"SELECT EXISTS(SELECT 1 FROM {namespace}.registry_approved_record WHERE approved_revision=0)"
    ):
        raise RegistryApprovalConflict("registry_initial_inventory_not_empty")


async def existing_publication_request(connection, namespace, metadata, idempotency_key):
    """Return an exact idempotent replay or reject a conflicting request body."""
    existing = await connection.fetchrow(
        f"SELECT * FROM {namespace}.registry_publication_request WHERE actor_key=$1 AND idempotency_key=$2",
        metadata["actor_key"],
        idempotency_key,
    )
    if existing is None:
        return None
    if existing["request_sha256"] != metadata["request_sha256"]:
        raise RegistryApprovalConflict("registry_queue_idempotency_conflict")
    return _queued_receipt(existing, True)


_existing_request = existing_publication_request


async def enqueue_registry_publication(
    connection,
    command: RegistryApprovalCommand,
    actor: RegistryActor,
    *,
    session_token_sha256,
    expected_serving_generation,
    control_schema=None,
):
    """The authorization layer authenticates every request, including exact retries."""
    if not connection.is_in_transaction():
        raise ValueError("registry_queue_requires_caller_transaction")
    if type(session_token_sha256) is not str or re.fullmatch(r"[0-9a-f]{64}", session_token_sha256) is None:
        raise ValueError("registry_session_hash_invalid")
    _reason, selection_json = _validated_command(command)
    if len(command.selection) > 100:
        raise ValueError("registry_queue_selection_invalid")
    expected_generation = _head(expected_serving_generation)
    actor_document = _validated_actor(actor)
    namespace = _namespace(control_schema)
    async with connection.transaction():
        selection_json = await _canonical_selection(connection, selection_json)
        metadata = _queue_metadata(command, actor_document, selection_json, session_token_sha256, expected_generation)
        replay = await _existing_request(connection, namespace, metadata, command.idempotency_key)
        if replay is not None:
            return replay
        current = await connection.fetchrow(
            f"SELECT draft_revision,approved_revision,coalesce(generation_id,0) AS generation_id FROM {namespace}.registry_revision_control CROSS JOIN {namespace}.network_serving_control WHERE registry_revision_control.id=1 AND network_serving_control.id=1"
        )
        if current is None or tuple(current) != (
            command.expected_draft_revision,
            command.expected_approved_revision,
            expected_generation,
        ):
            raise RegistryApprovalConflict("registry_queue_revision_conflict")
        inserted = await connection.fetchrow(
            f"""INSERT INTO {namespace}.registry_publication_request
            (request_id,actor_key,session_token_sha256,actor_json,command_json,idempotency_key,request_sha256)
            VALUES($1,$2,$3,$4::jsonb,$5::jsonb,$6,$7) ON CONFLICT(actor_key,idempotency_key) DO NOTHING RETURNING *""",
            uuid4(),
            metadata["actor_key"],
            session_token_sha256,
            metadata["actor_json"],
            metadata["command_json"],
            command.idempotency_key,
            metadata["request_sha256"],
        )
        if inserted is None:
            replay = await _existing_request(connection, namespace, metadata, command.idempotency_key)
            if replay is None:
                raise RegistryApprovalConflict("registry_queue_idempotency_conflict")
            return replay
        return _queued_receipt(inserted, False)


def _publication_operation_command(request_record, command_by_name, status_by_name):
    """Validate operation-specific identity and results before exposing their digest."""
    operation = command_by_name["operation"]
    if operation == "refresh_memberships":
        from process.registry_membership_refresh_queue import validated_membership_refresh_request_status

        return validated_membership_refresh_request_status(request_record)
    if operation == "publish_fhir_catalog":
        from process.registry_fhir_catalog_publication import validated_fhir_catalog_request_status

        return validated_fhir_catalog_request_status(request_record)
    if operation == "publish_initial_inventory":
        operation_command_by_name, _actor = _initial_inventory_record(request_record)
        _initial_inventory_result(status_by_name["result"], operation_command_by_name, status_by_name["state"])
    elif operation == "publish_source_rollback":
        operation_command_by_name, _actor = _source_rollback_publication_record(request_record)
        _source_rollback_publication_result(
            status_by_name["result"], operation_command_by_name, status_by_name["state"]
        )
    elif operation in {"reactivate_generation", "prepare_source_rollback"}:
        parsed, _actor = _retained_operation_record(request_record, operation)
        validator = _reactivation_result if operation == "reactivate_generation" else _source_rollback_result
        validator(status_by_name["result"], parsed, status_by_name["state"])
        operation_command_by_name = {
            **_reactivation_document(parsed),
            "operation": operation,
            "idempotency_key": parsed.idempotency_key,
        }
    else:
        raise ValueError("registry_reactivation_command_invalid")
    return operation_command_by_name


async def read_registry_publication_request(connection, request_id: UUID, *, control_schema=None):
    """Return a bounded status receipt without the session hash or raw command."""
    if not isinstance(request_id, UUID) or not request_id.int:
        raise ValueError("registry_request_identity_invalid")
    request_record = await connection.fetchrow(
        f"SELECT request_id,state,actor_json,command_json,idempotency_key,result_json,created_at,updated_at,"
        "actor_key,session_token_sha256,request_sha256 "
        f"FROM {_namespace(control_schema)}.registry_publication_request WHERE request_id=$1",
        request_id,
    )
    if request_record is None:
        return None
    status_by_name = {
        "request_id": str(request_record["request_id"]),
        "state": request_record["state"],
        "actor": _wire_actor(request_record["actor_json"]),
        "result": _document(request_record["result_json"]),
        "created_at": request_record["created_at"].isoformat(),
        "updated_at": request_record["updated_at"].isoformat(),
    }
    command_by_name = _document(request_record["command_json"])
    if "operation" in command_by_name:
        operation = command_by_name["operation"]
        operation_command_by_name = _publication_operation_command(request_record, command_by_name, status_by_name)
        status_by_name.update(
            operation=operation, command_sha256=hashlib.sha256(_json(operation_command_by_name).encode()).hexdigest()
        )
    else:
        status_by_name["selection"] = command_by_name["selection"]
    return status_by_name


async def claim_registry_publication_request(
    connection,
    *,
    operation_family: Literal["publication", "office"],
    lease_seconds=300,
    control_schema=None,
    allow_metadata=False,
    metadata_only=False,
    request_id=None,
    membership_refresh_only=False,
):
    """Claim only the explicit worker family, including expired running requests."""
    if type(operation_family) is not str or operation_family not in _CLAIM_OPERATIONS:
        raise ValueError("registry_queue_operation_family_invalid")
    if type(allow_metadata) is not bool:
        raise ValueError("registry_queue_operation_family_invalid")
    if type(metadata_only) is not bool or metadata_only and (not allow_metadata or operation_family != "publication"):
        raise ValueError("registry_queue_operation_family_invalid")
    if request_id is not None and (type(request_id) is not UUID or not request_id.int):
        raise ValueError("registry_queue_request_identity_invalid")
    if type(membership_refresh_only) is not bool:
        raise ValueError("registry_queue_operation_family_invalid")
    if membership_refresh_only and (operation_family != "publication" or metadata_only or request_id is None):
        raise ValueError("registry_queue_operation_family_invalid")
    if not connection.is_in_transaction() or type(lease_seconds) is not int or not 30 <= lease_seconds <= 900:
        raise ValueError("registry_queue_lease_invalid")
    request_record = await connection.fetchrow(
        f"""WITH selected AS (
        SELECT request_id FROM {_namespace(control_schema)}.registry_publication_request
        WHERE (state='queued' OR state='running' AND lease_expires_at<=clock_timestamp())
          AND ($5::uuid IS NULL OR request_id=$5)
          AND jsonb_typeof(command_json)='object'
          AND (($3::boolean AND NOT (command_json ? 'operation'))
            OR (jsonb_typeof(command_json->'operation')='string'
              AND command_json->>'operation'=ANY($4::text[])))
        ORDER BY created_at,request_id FOR UPDATE SKIP LOCKED LIMIT 1)
        UPDATE {_namespace(control_schema)}.registry_publication_request request
        SET state='running',lease_id=$1,lease_expires_at=clock_timestamp()+$2::integer*interval '1 second',updated_at=now()
        FROM selected WHERE request.request_id=selected.request_id RETURNING request.*""",
        uuid4(),
        lease_seconds,
        operation_family == "publication" and not metadata_only and not membership_refresh_only,
        tuple(
            operation
            for operation in _CLAIM_OPERATIONS[operation_family]
            + (("refresh_memberships",) if membership_refresh_only else ())
            if (allow_metadata or operation != "publish_fhir_catalog")
            and (not metadata_only or operation == "publish_fhir_catalog")
            and (not membership_refresh_only or operation == "refresh_memberships")
        ),
        request_id,
    )
    return None if request_record is None else dict(request_record)


async def renew_registry_publication_lease(connection, request_id, lease_id, *, lease_seconds=300, control_schema=None):
    """Return the request UUID for an exact active lease, or None when fenced out."""
    if not isinstance(request_id, UUID) or not isinstance(lease_id, UUID) or not connection.is_in_transaction():
        raise ValueError("registry_queue_lease_invalid")
    if type(lease_seconds) is not int or not 30 <= lease_seconds <= 900:
        raise ValueError("registry_queue_lease_invalid")
    return await connection.fetchval(
        f"""UPDATE {_namespace(control_schema)}.registry_publication_request
        SET lease_expires_at=clock_timestamp()+$3::integer*interval '1 second',updated_at=now()
        WHERE request_id=$1 AND lease_id=$2 AND state='running' AND lease_expires_at>clock_timestamp()
        RETURNING request_id""",
        request_id,
        lease_id,
        lease_seconds,
    )


async def finish_registry_publication_request(connection, request_id, lease_id, *, state, result, control_schema=None):
    """Complete one active controller lease in the caller's atomic transaction."""
    if state not in {"completed", "rejected"} or type(result) is not dict or len(_json(result).encode()) > 65536:
        raise ValueError("registry_queue_result_invalid")
    if not isinstance(request_id, UUID) or not isinstance(lease_id, UUID) or not connection.is_in_transaction():
        raise ValueError("registry_queue_lease_invalid")
    changed = await connection.fetchval(
        f"""UPDATE {_namespace(control_schema)}.registry_publication_request
        SET state=$3,result_json=$4::jsonb,lease_id=NULL,lease_expires_at=NULL,updated_at=now()
        WHERE request_id=$1 AND lease_id=$2 AND state='running' AND lease_expires_at>clock_timestamp() RETURNING request_id""",
        request_id,
        lease_id,
        state,
        _json(result),
    )
    if changed is None:
        raise RegistryApprovalConflict("registry_queue_lease_conflict")
