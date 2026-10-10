# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fresh whole-office approval; retained composition admission remains separate."""

import json
from dataclasses import dataclass
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from db.registry_schema import registry_schema
from process.network_address_projection import _identifier
from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
from process.registry_company_approval_fence import (
    registry_company_approval_transaction,
)
from process.registry_ptg_graph_reader import RegistryPTGGraphReadBudget
from process.registry_ptg_office_capture import (
    RegistryPTGOfficeCaptureContext,
    RegistryPTGOfficeCaptureDescriptor,
    RegistryPTGOfficeCaptureRequest,
    _canonical,
    _custody,
    _digest,
    _office_driver,
)
from process.registry_ptg_office_review_contract import validated_office_review_command
from process.registry_ptg_office_witness import verify_registry_ptg_office_witness
from process.registry_ptg_producer_scope import _protected_store
from process.registry_ptg_published_office_scope import _graph_identity
from process.registry_ptg_published_plan_contract import (
    RegistryPTGPublishedPlanSourceSpecification,
)

TABLE = "registry_ptg_office_approval"


@dataclass(frozen=True)
class RegistryPTGOfficeApprovalContext(RegistryPTGOfficeCaptureContext):
    """Internal approval-role context; native ACL proof is still mandatory."""


def validated_office_envelope(document):
    """Close actor, whole command and bounded read hints before opening sessions."""
    from process.registry_ptg_scope_engine import _object, _sha256, _uuid

    _object(document, {"actor", "session_token_sha256", "command", "read_limits"})
    actor_by_field = _object(document["actor"], {"kind", "user_id", "client_id"})
    if actor_by_field["kind"] != "platform_admin" or actor_by_field["client_id"] != "system":
        raise PermissionError("registry_ptg_scope_actor_invalid")
    _uuid(actor_by_field["user_id"])
    _sha256(document["session_token_sha256"])
    limits_by_field = _object(document["read_limits"], {"maximum_bytes", "maximum_pages", "maximum_coordinates"})
    RegistryPTGGraphReadBudget(**limits_by_field)
    return {
        **document,
        "actor": dict(actor_by_field),
        "command": validated_office_review_command(document["command"]),
        "read_limits": dict(limits_by_field),
    }


async def _approved_source_review(session, *, scope_id, client_id, approval_sha256, store):
    from process.registry_ptg_published_plan_scope import TABLE as SOURCE_TABLE
    from process.registry_ptg_published_plan_scope import _read_protected_published_scope

    table = await _protected_store(session, store, write=True, table_name=SOURCE_TABLE)
    return await _read_protected_published_scope(session, table, scope_id, client_id, approval_sha256)


def _reader_roles(service):
    sessions = service.reader_sessions
    if type(sessions) is not async_sessionmaker or type(sessions.kw.get("bind")) is not AsyncEngine:
        raise ValueError("registry_ptg_office_service_unconfigured")
    engine = sessions.kw["bind"]
    role = engine.url.username
    if (
        engine.dialect.name != "postgresql"
        or engine.dialect.driver != "asyncpg"
        or role in {None, service.store.owner_role, service.store.approval_role}
    ):
        raise ValueError("registry_ptg_office_service_unconfigured")
    _identifier(role)
    return tuple(sorted({role, service.store.approval_role}))


async def _context(session, service, command_by_field, control_schema):
    document_by_field = await _approved_source_review(
        session,
        scope_id=command_by_field["scope_id"],
        client_id=command_by_field["client_id"],
        approval_sha256=command_by_field["scope_approval_sha256"],
        store=service.store,
    )
    profile = service.office_custody
    if profile is None:
        raise ValueError("registry_ptg_office_profile_unavailable")
    source_command = document_by_field["command"]
    identity_by_field = source_command["published_identity"]
    specification = RegistryPTGPublishedPlanSourceSpecification(
        source_command["scope_id"],
        service.schema_name,
        identity_by_field["snapshot_id"],
        identity_by_field["source_key"],
    )
    context = RegistryPTGOfficeApprovalContext(
        UUID(source_command["scope_id"]),
        command_by_field["client_id"],
        command_by_field["scope_approval_sha256"],
        RegistryNetworkSourceCoordinates(**source_command["coordinates"]),
        specification,
        document_by_field["evidence"]["source_authority"],
        _graph_identity(identity_by_field),
        service.store,
        control_schema,
        profile.owner_role,
        _reader_roles(service),
        profile,
    )
    from process.registry_ptg_office_capture import _driver
    from process.registry_ptg_office_custody import verify_registry_ptg_office_custody

    await verify_registry_ptg_office_custody(await _driver(session), context, publisher=False)
    return context


def _request(command_by_field):
    return RegistryPTGOfficeCaptureRequest(
        UUID(command_by_field["capture_id"]),
        command_by_field["canonical_input_sha256"],
        command_by_field["input_row_count"],
        command_by_field["office_evidence_kind"],
        command_by_field["retained_generation_id"],
        command_by_field["reason"],
        command_by_field["idempotency_key"],
    )


async def _descriptor(session, context, request):
    driver = await _office_driver(session, context)
    schema_name = "registry_ptg_office_" + request.capture_id.hex
    namespace = _identifier(schema_name)
    await driver.execute(
        f"LOCK TABLE {namespace}.office_assertion,{namespace}.capture_manifest IN ACCESS SHARE MODE NOWAIT"
    )
    custody = await _custody(driver, schema_name, context)
    records = await driver.fetch(
        f"SELECT id,manifest_sha256,CASE WHEN octet_length(manifest_json::text)<=8388608 THEN manifest_json::text END AS manifest_json FROM {namespace}.capture_manifest LIMIT 2"
    )
    if len(records) != 1 or records[0]["id"] != 1 or type(records[0]["manifest_json"]) is not str:
        raise ValueError("registry_ptg_office_manifest_changed")
    manifest_by_field = json.loads(records[0]["manifest_json"])
    return RegistryPTGOfficeCaptureDescriptor(
        request.capture_id,
        schema_name,
        records[0]["manifest_sha256"],
        _canonical(manifest_by_field),
        _canonical(custody),
    )


async def _physical_review(session, service, command_by_field, limits_by_field, control_schema):
    context = await _context(session, service, command_by_field, control_schema)
    request = _request(command_by_field)
    descriptor = await _descriptor(session, context, request)
    if json.loads(descriptor.manifest_json)["command"] != command_by_field:
        raise ValueError("registry_ptg_office_command_changed")
    witness = await verify_registry_ptg_office_witness(
        session, context, request, descriptor, read_budget=RegistryPTGGraphReadBudget(**limits_by_field)
    )
    if witness.as_dict()["command_sha256"] != _digest(command_by_field):
        raise ValueError("registry_ptg_office_command_changed")
    return descriptor, witness


def _document(envelope, descriptor, witness, source_pin):
    document_by_field = {
        "contract": "registry_ptg_office_approval.v1",
        "capture_id": envelope["command"]["capture_id"],
        "client_id": envelope["command"]["client_id"],
        "actor": envelope["actor"],
        "command": envelope["command"],
        "manifest_sha256": descriptor.manifest_sha256,
        "witness": witness.as_dict(),
        "source_pin": source_pin,
    }
    if len(_canonical(document_by_field)) > 131072:
        raise ValueError("registry_ptg_office_approval_bounds")
    return document_by_field


async def _retain(session, table, document_by_field):
    command_by_field = document_by_field["command"]
    parameters_by_name = {
        "capture_id": command_by_field["capture_id"],
        "actor_key": _digest(document_by_field["actor"]),
        "idempotency_key": command_by_field["idempotency_key"],
        "approval_sha256": _digest(document_by_field),
        "document": _canonical(document_by_field).decode(),
    }
    await session.execute(
        text(
            f"INSERT INTO {table}(capture_id,actor_key,idempotency_key,approval_sha256,approval_json) VALUES(CAST(:capture_id AS uuid),:actor_key,:idempotency_key,:approval_sha256,CAST(:document AS jsonb)) ON CONFLICT DO NOTHING"
        ),
        parameters_by_name,
    )
    stored_reviews = (
        (
            await session.execute(
                text(
                    f"SELECT approval_sha256,approval_json FROM {table} WHERE capture_id=CAST(:capture_id AS uuid) OR (actor_key=:actor_key AND idempotency_key=:idempotency_key) LIMIT 2"
                ),
                parameters_by_name,
            )
        )
        .mappings()
        .all()
    )
    if (
        len(stored_reviews) != 1
        or stored_reviews[0]["approval_sha256"] != parameters_by_name["approval_sha256"]
        or stored_reviews[0]["approval_json"] != document_by_field
    ):
        raise ValueError("registry_ptg_scope_idempotency_conflict")
    return parameters_by_name["approval_sha256"]


async def approve_offices(service, envelope, deadline, state_by_field):
    """Reverify, freshly authorize and append under one actual fenced transaction.

    The returned descriptive recipe binds its immutable approval and hold.
    Admission still requires the genuine composition reader and fresh proof.
    """
    from process.registry_ptg_office_membership_contract import approved_office_recipe_receipt
    from process.registry_ptg_office_retention import retain_registry_ptg_office_capture
    from process.registry_ptg_scope_engine import _remaining

    control_schema = service.store.control_schema or registry_schema()
    async with registry_company_approval_transaction(
        service.approval_sessions, control_schema=control_schema
    ) as session:
        original_path = (await session.execute(text("SELECT pg_catalog.current_setting('search_path')"))).scalar_one()
        await session.execute(text("SELECT pg_catalog.set_config('search_path','pg_catalog,pg_temp',true)"))
        _remaining(deadline)
        table = await _protected_store(session, service.store, write=True, table_name=TABLE)
        descriptor, witness = await _physical_review(
            session, service, envelope["command"], envelope["read_limits"], control_schema
        )
        _remaining(deadline)
        authority_envelope_by_field = {name: envelope[name] for name in ("actor", "session_token_sha256", "command")}
        await service.authority.authorize(authority_envelope_by_field, deadline=deadline)
        _remaining(deadline)
        source_pin = await _pin_source(session, service.schema_name, envelope["command"])
        document_by_field = _document(envelope, descriptor, witness, source_pin)
        approval_sha256 = await _retain(session, table, document_by_field)
        hold_sha256 = await retain_registry_ptg_office_capture(
            session, service.store, descriptor, witness, approval_sha256
        )
        recipe_by_field = approved_office_recipe_receipt(
            envelope["command"], descriptor.manifest_sha256, approval_sha256, hold_sha256
        )
        await session.execute(
            text("SELECT pg_catalog.set_config('search_path',:original_path,true)"), {"original_path": original_path}
        )
        _remaining(deadline)
        state_by_field["commit_started"] = True
    _remaining(deadline)
    return {
        "capture_id": envelope["command"]["capture_id"],
        "approval_sha256": approval_sha256,
        "state": "reviewed",
        "command_sha256": _digest(envelope["command"]),
        **recipe_by_field,
    }


async def _pin_source(session, schema_name, command_by_field):
    """Retain an independent office-operation source pin after fresh authorization."""
    from process.ptg_parts.result_archive_published_authority import prepare_ptg_published_result_source_authority
    from process.ptg_parts.result_archive_source_authority import commit_ptg_result_archive_source_authority

    expected_by_field = command_by_field["source"]["evidence"]["source_authority"]
    operation_id = "registry_ptg_office_review_" + UUID(command_by_field["capture_id"]).hex
    prepared = await prepare_ptg_published_result_source_authority(
        session,
        schema_name=schema_name,
        operation_id=operation_id,
        snapshot_id=command_by_field["source"]["snapshot_id"],
    )
    source_pin = prepared.as_dict()
    if (
        source_pin["identity"] != expected_by_field["identity"]
        or source_pin["source_key"] != command_by_field["source"]["binding_source_key"]
    ):
        raise ValueError("registry_ptg_office_source_changed")
    retained = await commit_ptg_result_archive_source_authority(session, schema_name=schema_name, authority=source_pin)
    if retained.as_dict() != source_pin:
        raise ValueError("registry_ptg_office_source_changed")
    return source_pin
