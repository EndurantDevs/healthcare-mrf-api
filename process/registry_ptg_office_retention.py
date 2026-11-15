# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Immutable office holds and exact cleanup under the protected publisher owner."""

import asyncio
import json
import re
from dataclasses import asdict
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from process import registry_ptg_office_capture as capture
from process.network_address_projection import _identifier
from process.registry_company_approval_fence import (
    _company_approval_snapshot,
)
from process.registry_ptg_office_capture import (
    RegistryPTGOfficeCaptureDescriptor,
    _canonical,
    _digest,
    resolve_network_serving_manifest,
)
from process.registry_ptg_office_witness import (
    RegistryPTGOfficeWitnessEvidence,
)
from process.registry_ptg_producer_scope import (
    _protected_store,
)

TABLE = "registry_ptg_office_hold"
MAX_HOLD_BYTES = 32768


class RegistryPTGOfficeReleaseOutcomeUnknown(RuntimeError):
    """Cleanup reached its original owner commit; its acknowledgement was lost."""


def _sha256(value):
    if type(value) is not str or re.fullmatch(r"[0-9a-f]{64}", value) is None:
        raise ValueError("registry_ptg_office_retention_invalid")
    return value


def is_office_hold_match(hold, document, approval_sha256):
    """Compare logical held facts; historical physical fields and editions are not authority."""
    evidence = document["witness"]
    command = document["command"]
    named_evidence = capture.named_office_evidence(evidence, command)
    schema = "registry_ptg_office_" + UUID(command["capture_id"]).hex
    if (
        named_evidence["capture_id"] != command["capture_id"]
        or named_evidence["schema_name"] != schema
        or named_evidence["manifest_sha256"] != document["manifest_sha256"]
        or named_evidence["scope_id"] != command["scope_id"]
        or named_evidence["client_id"] != command["client_id"]
        or named_evidence["scope_approval_sha256"] != command["scope_approval_sha256"]
        or named_evidence["source"] != command["source"]
        or named_evidence["accounting"]["input_row_count"] != command["input_row_count"]
        or named_evidence["accounting"]["canonical_input_sha256"] != command["canonical_input_sha256"]
    ):
        return False
    expected_by_field = {
        "contract": "registry_ptg_office_hold.v1",
        "capture_id": command["capture_id"],
        "approval_sha256": _sha256(approval_sha256),
        "manifest_sha256": document["manifest_sha256"],
        "schema_name": schema,
        "storage": capture.office_storage(schema, document["manifest_sha256"]),
        "retained_serving": command["retained_serving"],
    }
    if type(hold) is not dict:
        return False
    is_legacy = "custody" in hold
    if is_legacy:
        if type(hold["custody"]) is not dict or set(hold["custody"]) != capture._LEGACY_CUSTODY_FIELDS:
            return False
        normalized_by_field = {
            name: field_value for name, field_value in hold.items() if name not in {"custody", "witness_sha256"}
        }
        normalized_by_field["storage"] = capture.office_storage(hold["schema_name"], hold["manifest_sha256"])
        _sha256(hold["witness_sha256"])
    else:
        normalized_by_field = {name: field_value for name, field_value in hold.items() if name != "witness_sha256"}
        if hold.get("witness_sha256") != _digest(evidence):
            return False
    from process.registry_ptg_office_review_contract import named_office_review_command

    normalized_by_field["retained_serving"] = capture.named_office_evidence(normalized_by_field, command)[
        "retained_serving"
    ]
    expected_by_field["retained_serving"] = named_office_review_command(command)["retained_serving"]
    return normalized_by_field == expected_by_field


def office_hold_document(descriptor, witness, approval_sha256):
    """Bind verified named office data to its real immutable approval."""
    if (
        type(descriptor) is not RegistryPTGOfficeCaptureDescriptor
        or type(witness) is not RegistryPTGOfficeWitnessEvidence
    ):
        raise ValueError("registry_ptg_office_retention_invalid")
    if (
        len(descriptor.manifest_json) > capture.MAX_INPUT_BYTES
        or len(descriptor.storage_json) > 16384
        or len(witness.evidence_json) > capture.MAX_INPUT_BYTES
    ):
        raise ValueError("registry_ptg_office_retention_invalid")
    evidence = witness.as_dict()
    manifest = json.loads(descriptor.manifest_json)
    try:
        named = capture.named_office_evidence(evidence, manifest["command"])
    except ValueError as error:
        raise ValueError("registry_ptg_office_retention_invalid") from error
    from process.registry_ptg_office_review_contract import named_office_review_command

    if (
        "storage" in manifest
        and _digest(manifest) != descriptor.manifest_sha256
        or named["contract"] != "registry_ptg_office_witness.v1"
        or named["capture_id"] != str(descriptor.capture_id)
        or named["schema_name"] != descriptor.schema_name
        or named["manifest_sha256"] != descriptor.manifest_sha256
        or named["command_sha256"] != manifest["command_sha256"]
        or named["storage"] != capture.descriptor_storage(descriptor)
        or named["retained_serving"] != named_office_review_command(manifest["command"])["retained_serving"]
    ):
        raise ValueError("registry_ptg_office_retention_invalid")
    hold_by_field = {
        "contract": "registry_ptg_office_hold.v1",
        "capture_id": str(descriptor.capture_id),
        "approval_sha256": _sha256(approval_sha256),
        "manifest_sha256": _sha256(descriptor.manifest_sha256),
        "witness_sha256": _digest(evidence),
        "schema_name": descriptor.schema_name,
        "storage": capture.descriptor_storage(descriptor),
        "retained_serving": named_office_review_command(manifest["command"])["retained_serving"],
    }
    if len(_canonical(hold_by_field)) > MAX_HOLD_BYTES:
        raise ValueError("registry_ptg_office_retention_invalid")
    return hold_by_field


async def retain_registry_ptg_office_capture(session, store, descriptor, witness, approval_sha256):
    """Append/verify one hold in the original approval transaction, never commit."""
    hold_by_field = office_hold_document(descriptor, witness, approval_sha256)
    table = await _protected_store(session, store, write=True, table_name=TABLE)
    parameters_by_name = {
        "capture_id": hold_by_field["capture_id"],
        "approval_sha256": approval_sha256,
        "hold_sha256": _digest(hold_by_field),
        "hold_by_field": _canonical(hold_by_field).decode(),
    }
    await session.execute(
        text(f"""INSERT INTO {table}(capture_id,approval_sha256,hold_sha256,hold_json)
      VALUES(CAST(:capture_id AS uuid),:approval_sha256,:hold_sha256,CAST(:hold_by_field AS jsonb)) ON CONFLICT DO NOTHING"""),
        parameters_by_name,
    )
    retained_rows = (
        (
            await session.execute(
                text(f"""SELECT approval_sha256,hold_sha256,
      CASE WHEN octet_length(hold_json::text)<=32768 THEN hold_json END AS hold_json
      FROM {table} WHERE capture_id=CAST(:capture_id AS uuid) LIMIT 2"""),
                parameters_by_name,
            )
        )
        .mappings()
        .all()
    )
    if len(retained_rows) != 1 or dict(retained_rows[0]) != {
        "approval_sha256": approval_sha256,
        "hold_sha256": parameters_by_name["hold_sha256"],
        "hold_json": hold_by_field,
    }:
        raise ValueError("registry_ptg_office_retention_conflict")
    return parameters_by_name["hold_sha256"]


async def _require_unheld(session, tables, capture_id):
    """Only a freshly fenced absence of both actual approval and hold allows release."""
    for table in tables:
        held = (
            await session.execute(
                text(f"SELECT EXISTS(SELECT FROM {table} WHERE capture_id=CAST(:capture_id AS uuid))"),
                {"capture_id": str(capture_id)},
            )
        ).scalar_one()
        if held is not False:
            raise ValueError("registry_ptg_office_retention_release_unavailable")


async def _require_idle_intent(session, control_schema, capture_id):
    from process.registry_ptg_office_intent import OPERATION

    active = (
        await session.execute(
            text(
                f"SELECT EXISTS(SELECT FROM {_identifier(control_schema)}.registry_publication_request "
                "WHERE state IN ('queued','running') AND command_json->>'operation'=:operation "
                "AND command_json->'selector'->>'capture_id'=:capture_id)"
            ),
            {"operation": OPERATION, "capture_id": str(capture_id)},
        )
    ).scalar_one()
    if active is not False:
        raise ValueError("registry_ptg_office_retention_release_unavailable")


async def _check_release_serving(driver, document, control_schema):
    from process.registry_ptg_office_review_contract import is_legacy_office_review_command, named_office_review_command

    namespace = _identifier(document["storage_schema"])
    manifest = await driver.fetchval(f"SELECT manifest_json FROM {namespace}.capture_manifest WHERE id=1")
    manifest = json.loads(manifest) if type(manifest) is str else manifest
    serving = asdict(
        await resolve_network_serving_manifest(
            driver, generation_id=manifest["command"]["retained_generation_id"], control_schema=control_schema
        )
    )
    if is_legacy_office_review_command(manifest["command"]):
        serving.pop("address_relation", None)
    if serving != named_office_review_command(manifest["command"])["retained_serving"]:
        raise ValueError("registry_ptg_office_retention_owner_invalid")


def _check_office_cancellation(cancelled, document, identity, control_schema, storage_schema):
    if type(cancelled) is not dict or set(cancelled) != {"handoff", "state"} or cancelled["state"] != "abandoned":
        raise ValueError("registry_ptg_office_retention_owner_invalid")
    handoff = cancelled["handoff"]
    if (
        type(handoff) is not dict
        or set(handoff) != {"kind", "candidate_id", "catalog_schema", "storage_schema", "tables", "snapshot"}
        or handoff["kind"] != "office"
        or handoff["candidate_id"] != str(identity)
        or handoff["catalog_schema"] != control_schema
        or handoff["storage_schema"] != storage_schema
        or handoff["tables"] != capture.office_storage(storage_schema)["tables"]
        or document is not None
        and handoff != document
    ):
        raise ValueError("registry_ptg_office_retention_owner_invalid")


async def _release_office_candidate(session, driver, identity, control_schema, storage_schema, cancel_preparation):
    from process.registry_ptg_office_preparation import load_named_office_preparation

    tables = [_identifier(control_schema) + "." + _identifier(name) for name in ("registry_ptg_office_approval", TABLE)]
    await _require_unheld(session, tables, identity)
    await _require_idle_intent(session, control_schema, identity)
    document = await load_named_office_preparation(driver, str(identity), control_schema=control_schema)
    if document is not None:
        await _check_release_serving(driver, document, control_schema)
    _check_office_cancellation(
        await cancel_preparation(driver, document), document, identity, control_schema, storage_schema
    )
    await _require_unheld(session, tables, identity)
    await _require_idle_intent(session, control_schema, identity)
    if document is not None:
        namespace = _identifier(storage_schema)
        await driver.execute(
            f"LOCK TABLE {namespace}.office_assertion,{namespace}.capture_manifest IN ACCESS EXCLUSIVE MODE NOWAIT"
        )
        if await load_named_office_preparation(driver, str(identity), control_schema=control_schema) != document:
            raise ValueError("registry_ptg_office_manifest_changed")
        await driver.execute(f"DROP TABLE {namespace}.office_assertion,{namespace}.capture_manifest RESTRICT")
        await driver.execute(f"DROP SCHEMA {namespace} RESTRICT")


async def release_named_office_preparation(
    connection, candidate_id, *, control_schema, storage_schema, cancel_preparation
):
    """Release on the same administrator after company fencing and exact cancellation CAS."""
    from process.uhc_flex_practitioner_async_safety import drain_operation

    identity = UUID(str(candidate_id))
    if (
        not identity.int
        or str(identity) != str(candidate_id)
        or storage_schema != "registry_ptg_office_" + identity.hex
        or control_schema == storage_schema
        or connection.in_transaction()
        or not callable(cancel_preparation)
    ):
        raise ValueError("registry_ptg_office_retention_owner_invalid")
    _identifier(control_schema)
    sessions = async_sessionmaker(class_=AsyncSession, expire_on_commit=False, autoflush=False)
    has_commit_started, body_error = False, None
    try:
        async with asyncio.timeout(15):
            async with _company_approval_snapshot(
                sessions, connection, control_schema=control_schema, is_exclusive=True
            ) as session:
                try:
                    await session.execute(text("SET LOCAL statement_timeout='3s'"))
                    await session.execute(text("SET LOCAL lock_timeout='1s'"))
                    await session.execute(text("SELECT pg_catalog.set_config('search_path','pg_catalog,pg_temp',true)"))
                    driver = (await (await session.connection()).get_raw_connection()).driver_connection
                    await _release_office_candidate(
                        session, driver, identity, control_schema, storage_schema, cancel_preparation
                    )
                    has_commit_started = True
                except BaseException as error:
                    body_error = error
                    raise
    except BaseException as error:
        primary = body_error if body_error is not None and not isinstance(error, TimeoutError) else error
        try:
            await drain_operation(connection.invalidate(), preserve_cancellation=False)
        except BaseException as cleanup_error:
            raise primary from cleanup_error
        if has_commit_started and body_error is None and isinstance(primary, Exception):
            raise RegistryPTGOfficeReleaseOutcomeUnknown("registry_ptg_office_release_outcome_unknown") from primary
        if primary is not error:
            raise primary from error
        raise
    return {"preparation_id": str(identity), "state": "abandoned"}


async def release_registry_ptg_office_capture(connection, *, store, context, request, descriptor):
    """Use the existing independently configured administrator operator, never Publisher SET."""
    from directory_catalog_runtime import release_registry_generation

    from process.registry_ptg_office_custody import office_preparation_roles
    from process.registry_ptg_office_witness import _descriptor

    schema_name, _ = _descriptor(descriptor, request, context)
    if (store.control_schema or context.control_schema) != context.control_schema:
        raise ValueError("registry_ptg_office_retention_owner_invalid")
    await release_registry_generation(
        connection,
        {
            "kind": "office",
            "candidate_id": str(request.capture_id),
            "catalog_schema": context.control_schema,
            "storage_schema": schema_name,
            "preparation_roles": office_preparation_roles(context),
        },
        str(request.capture_id),
    )
    return {"capture_id": str(request.capture_id), "manifest_sha256": descriptor.manifest_sha256, "state": "released"}
