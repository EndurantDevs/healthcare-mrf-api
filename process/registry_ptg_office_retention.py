# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Immutable office holds and exact cleanup under the protected publisher owner."""

import json
import re
from dataclasses import asdict

from sqlalchemy import text

from process import registry_ptg_office_capture as capture
from process.network_address_projection import _identifier
from process.network_membership_writer_closure import _check_roles
from process.registry_company_approval_fence import (
    registry_company_approval_transaction,
    require_registry_company_approval_fence,
)
from process.registry_ptg_office_capture import (
    RegistryPTGOfficeCaptureDescriptor,
    _canonical,
    _digest,
    _office_driver,
    _same_transaction,
    resolve_network_serving_manifest,
)
from process.registry_ptg_office_witness import (
    RegistryPTGOfficeWitnessEvidence,
    _descriptor,
    _family,
)
from process.registry_ptg_producer_scope import (
    _PERMISSIONS_SQL,
    RegistryPTGProducerScopeStore,
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


def office_hold_document(descriptor, witness, approval_sha256):
    """Bind the real verified physical family to its exact immutable approval."""
    if (
        type(descriptor) is not RegistryPTGOfficeCaptureDescriptor
        or type(witness) is not RegistryPTGOfficeWitnessEvidence
    ):
        raise ValueError("registry_ptg_office_retention_invalid")
    if (
        len(descriptor.manifest_json) > capture.MAX_INPUT_BYTES
        or len(descriptor.custody_json) > 16384
        or len(witness.evidence_json) > capture.MAX_INPUT_BYTES
    ):
        raise ValueError("registry_ptg_office_retention_invalid")
    evidence = witness.as_dict()
    manifest = json.loads(descriptor.manifest_json)
    command = manifest["command"]
    custody = json.loads(descriptor.custody_json)
    if (
        _digest(manifest) != descriptor.manifest_sha256
        or evidence["contract"] != "registry_ptg_office_witness.v1"
        or evidence["capture_id"] != str(descriptor.capture_id)
        or evidence["schema_name"] != descriptor.schema_name
        or evidence["manifest_sha256"] != descriptor.manifest_sha256
        or evidence["command_sha256"] != _digest(command)
        or evidence["custody"] != custody
        or evidence["retained_serving"] != command["retained_serving"]
    ):
        raise ValueError("registry_ptg_office_retention_invalid")
    if any(
        type(custody.get(name)) is not int or not 0 < custody[name] < 2**32
        for name in ("schema_oid", "owner_oid", "table_oid", "manifest_table_oid")
    ):
        raise ValueError("registry_ptg_office_retention_invalid")
    hold_by_field = {
        "contract": "registry_ptg_office_hold.v1",
        "capture_id": str(descriptor.capture_id),
        "approval_sha256": _sha256(approval_sha256),
        "manifest_sha256": _sha256(descriptor.manifest_sha256),
        "witness_sha256": _digest(evidence),
        "schema_name": descriptor.schema_name,
        "custody": custody,
        "retained_serving": command["retained_serving"],
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


async def _owner_tables(session, store, driver, context):
    """Attest runtime ACLs separately from the trusted publisher's SET capability."""
    if type(store) is not RegistryPTGProducerScopeStore or store.control_schema not in (
        None,
        context.control_schema,
    ):
        raise ValueError("registry_ptg_office_retention_owner_invalid")
    if capture.is_published_office_context(context):
        from process.registry_ptg_office_custody import (
            verify_registry_ptg_office_custody,
        )

        await verify_registry_ptg_office_custody(driver, context, publisher=True)
    elif store.owner_role != context.owner_role:
        raise ValueError("registry_ptg_office_retention_owner_invalid")
    await require_registry_company_approval_fence(session, context.control_schema, is_exclusive=True)
    await _check_roles(
        driver,
        {
            "owner_role": context.owner_role,
            "loader_roles": [],
            "reader_roles": list(context.reader_roles),
        },
    )
    namespace = _identifier(store.control_schema or context.control_schema)
    tables = []
    for table_name in ("registry_ptg_office_approval", TABLE):
        query = _PERMISSIONS_SQL.replace("'registry_ptg_producer_scope'", "'" + table_name + "'")
        query = query.replace("r.rolname=current_user", "r.rolname=:approval_role")
        closed = (
            await session.execute(
                text(query),
                {
                    "schema_name": namespace[1:-1],
                    "owner_role": store.owner_role,
                    "approval_role": store.approval_role,
                    "write": True,
                },
            )
        ).scalar_one_or_none()
        if closed is not True:
            raise ValueError("registry_ptg_office_retention_store_unprotected")
        tables.append(namespace + "." + _identifier(table_name))
    return tables


async def _require_unheld(session, tables, capture_id):
    """Never turn missing future obligation tracking into release authority."""
    for table in tables:
        held = (
            await session.execute(
                text(f"SELECT EXISTS(SELECT FROM {table} WHERE capture_id=CAST(:capture_id AS uuid))"),
                {"capture_id": str(capture_id)},
            )
        ).scalar_one()
        if held is not False:
            # Approved families stay held until real recipe/candidate/read releases exist.
            raise ValueError("registry_ptg_office_retention_release_unavailable")


async def _drop_unheld(session, store, context, request, descriptor):
    schema_name, manifest = _descriptor(descriptor, request, context)
    driver = await _office_driver(session, context)
    transaction = session.get_transaction()
    tables = await _owner_tables(session, store, driver, context)
    serving = await resolve_network_serving_manifest(
        driver,
        generation_id=request.retained_generation_id,
        control_schema=context.control_schema,
    )
    if asdict(serving) != manifest["command"]["retained_serving"]:
        raise ValueError("registry_ptg_office_retention_owner_invalid")
    await _require_unheld(session, tables, request.capture_id)
    original_role = await driver.fetchval("SELECT quote_ident(current_user)")
    await driver.execute(f"SET LOCAL ROLE {_identifier(context.owner_role)}")
    _same_transaction(session, driver, transaction)
    await _family(driver, schema_name, descriptor, context)
    namespace = _identifier(schema_name)
    await driver.execute(
        f"LOCK TABLE {namespace}.office_assertion,{namespace}.capture_manifest IN ACCESS EXCLUSIVE MODE NOWAIT"
    )
    _same_transaction(session, driver, transaction)
    await driver.execute(f"DROP TABLE {namespace}.office_assertion,{namespace}.capture_manifest RESTRICT")
    await driver.execute(f"DROP SCHEMA {namespace} RESTRICT")
    await driver.execute(f"SET LOCAL ROLE {original_role}")
    _same_transaction(session, driver, transaction)


async def release_registry_ptg_office_capture(sessions, *, store, context, request, descriptor):
    """Release only an exact unapproved prepared family through its real owner.

    The exclusive company fence is acquired before RR and excludes all original
    approval owners. Native ACCESS EXCLUSIVE/NOWAIT and RESTRICT preserve readers
    and external dependencies. Approved/uncertain families remain unsupported and
    held; no caller-supplied absence flags or SOURCE pin release are accepted.
    Publisher capability is trusted. Runtime ACLs cannot assume that owner.
    """
    has_commit_started, body_error = False, None
    try:
        async with registry_company_approval_transaction(
            sessions, control_schema=context.control_schema, is_exclusive=True
        ) as session:
            try:
                original_path = (
                    await session.execute(text("SELECT pg_catalog.current_setting('search_path')"))
                ).scalar_one()
                await session.execute(text("SELECT pg_catalog.set_config('search_path','pg_catalog,pg_temp',true)"))
                await _drop_unheld(session, store, context, request, descriptor)
                await session.execute(
                    text("SELECT pg_catalog.set_config('search_path',:original_path,true)"),
                    {"original_path": original_path},
                )
                has_commit_started = True
            except BaseException as error:
                body_error = error
                raise
    except BaseException as error:
        if body_error is not None and error is not body_error:
            raise body_error from error
        if has_commit_started and isinstance(error, Exception):
            raise RegistryPTGOfficeReleaseOutcomeUnknown("registry_ptg_office_release_outcome_unknown") from error
        raise
    return {"capture_id": str(request.capture_id), "manifest_sha256": descriptor.manifest_sha256, "state": "released"}
