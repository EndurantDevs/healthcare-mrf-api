# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Protected preparation/recovery with an installed genuine office Publisher."""

import asyncio
import hashlib
import json
from dataclasses import dataclass, field, replace
from uuid import UUID

from sqlalchemy import text

from db.registry_schema import registry_schema
from process import registry_ptg_office_capture as capture
from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
from process.registry_company_approval_fence import _source_engine, registry_company_approval_transaction
from process.registry_ptg_graph_reader import RegistryPTGGraphReadBudget
from process.registry_ptg_office_approval import _descriptor, _reader_roles, _request
from process.registry_ptg_office_custody import office_preparation_roles
from process.registry_ptg_office_preparation_contract import validated_preparation_envelope
from process.registry_ptg_office_preparation_producer import produce_reviewed_office_rows
from process.registry_ptg_office_review_contract import validated_office_review_command
from process.registry_ptg_office_witness import verify_registry_ptg_office_witness
from process.registry_ptg_published_office_scope import _graph_identity
from process.registry_ptg_published_plan_contract import RegistryPTGPublishedPlanSourceSpecification
from process.registry_ptg_published_plan_scope import read_registry_ptg_published_plan_scope
from process.registry_ptg_scope_engine import (
    RegistryPTGScopeDeadlineExpired,
    RegistryPTGScopeEngineService,
    _deadline,
    _remaining,
)

_MANIFEST_FIELDS = {"contract", "state", "command", "command_sha256", "accounting"}


def named_office_preparation_roles(configured):
    """Validate the existing office owner, publisher and application role names."""
    from process.network_address_projection import _identifier

    if type(configured) is not dict or set(configured) != {
        "namespace_owner_role",
        "publisher_role",
        "application_roles",
    }:
        raise ValueError("registry_ptg_office_profile_unavailable")
    owner = configured["namespace_owner_role"]
    publisher = configured["publisher_role"]
    applications = configured["application_roles"]
    if (
        type(applications) not in {list, tuple}
        or not 1 <= len(applications) <= 128
        or list(applications) != sorted(set(applications))
    ):
        raise ValueError("registry_ptg_office_profile_unavailable")
    for role in (owner, publisher, *applications):
        _identifier(role)
    if owner == publisher or owner in applications or publisher not in applications:
        raise ValueError("registry_ptg_office_profile_unavailable")
    return {"namespace_owner_role": owner, "publisher_role": publisher, "application_roles": list(applications)}


def office_manifest_data(manifest, manifest_sha256, candidate_id, control_schema):
    """Decode completed loading and names; this grants no freeze, review or data admission."""
    from process.network_address_projection import _identifier
    from process.registry_ptg_scope_engine import _sha256

    identity = UUID(str(candidate_id))
    if not identity.int or str(identity) != str(candidate_id):
        raise ValueError("registry_ptg_office_manifest_changed")
    _identifier(control_schema)
    schema = "registry_ptg_office_" + identity.hex
    fields = set(manifest) if type(manifest) is dict else set()
    is_named = fields == _MANIFEST_FIELDS | {"storage", "preparation_request_sha256"}
    if fields != _MANIFEST_FIELDS and not is_named:
        raise ValueError("registry_ptg_office_manifest_changed")
    if manifest["contract"] != "registry_ptg_office_capture.v1" or manifest["state"] != "prepared":
        raise ValueError("registry_ptg_office_manifest_changed")
    _sha256(manifest_sha256)
    _sha256(manifest["command_sha256"])
    command = manifest["command"]
    if type(command) is not dict or command.get("capture_id") != str(identity):
        raise ValueError("registry_ptg_office_manifest_changed")
    from process.registry_ptg_office_review_contract import named_office_review_command

    named_office_review_command(command)
    accounting = manifest["accounting"]
    if type(accounting) is not dict or set(accounting) != {
        "input_row_count",
        "canonical_input_sha256",
        "retained_site_rows_sha256",
        "retained_site_identity",
    }:
        raise ValueError("registry_ptg_office_manifest_changed")
    if (
        type(accounting["input_row_count"]) is not int
        or not 1 <= accounting["input_row_count"] <= capture.MAX_TOTAL_ROWS
        or accounting["input_row_count"] != command["input_row_count"]
        or accounting["canonical_input_sha256"] != command["canonical_input_sha256"]
        or command["retained_sites"]
        != {"identity": accounting["retained_site_identity"], "rows_sha256": accounting["retained_site_rows_sha256"]}
    ):
        raise ValueError("registry_ptg_office_manifest_changed")
    for digest_field in ("canonical_input_sha256", "retained_site_rows_sha256"):
        _sha256(accounting[digest_field])
    if is_named and (
        manifest["storage"] != capture.office_storage(schema)
        or manifest_sha256 != capture._digest(manifest)
        or manifest["command_sha256"] != capture._digest(command)
    ):
        raise ValueError("registry_ptg_office_manifest_changed")
    # Historical editions stay opaque; the complete witness reconstructs their actual data.
    request_sha256 = manifest.get("preparation_request_sha256", manifest["command_sha256"])
    _sha256(request_sha256)
    return _office_preparation_document(identity, control_schema, manifest, manifest_sha256)


def _office_preparation_document(identity, control_schema, manifest, manifest_sha256):
    from process.registry_ptg_office_review_contract import named_office_review_command

    schema = "registry_ptg_office_" + identity.hex
    named_command = named_office_review_command(manifest["command"])
    accounting = manifest["accounting"]
    request_sha256 = manifest.get("preparation_request_sha256", manifest["command_sha256"])
    return {
        "kind": "office",
        "candidate_id": str(identity),
        "catalog_schema": control_schema,
        "storage_schema": schema,
        "tables": capture.office_storage(schema)["tables"],
        "snapshot": {
            "manifest_sha256": manifest_sha256,
            "command_sha256": manifest["command_sha256"],
            "request_sha256": request_sha256,
            "canonical_input_sha256": accounting["canonical_input_sha256"],
            "input_row_count": accounting["input_row_count"],
            "retained_site_rows_sha256": accounting["retained_site_rows_sha256"],
            "scope_id": named_command["scope_id"],
            "client_id": named_command["client_id"],
            "scope_approval_sha256": named_command["scope_approval_sha256"],
            "source_coordinates": named_command["source"]["coordinates"],
            "retained_generation_id": named_command["retained_generation_id"],
            "retained_serving_sha256": capture._digest(named_command["retained_serving"]),
        },
    }


async def load_named_office_preparation(connection, candidate_id, *, control_schema):
    """Load a real singleton manifest after completed bounded COPY, never schema existence."""
    identity = UUID(str(candidate_id))
    if not identity.int or str(identity) != str(candidate_id):
        raise ValueError("registry_ptg_office_manifest_changed")
    schema = "registry_ptg_office_" + identity.hex
    if await connection.fetchval("SELECT to_regclass($1)", f'"{schema}".capture_manifest') is None:
        return None
    rows = await connection.fetch(
        f"SELECT id,manifest_sha256,CASE WHEN octet_length(manifest_json::text)<=8388608 "
        f'THEN manifest_json::text END AS manifest_json FROM "{schema}".capture_manifest LIMIT 2'
    )
    if not rows:
        return None
    if len(rows) != 1 or rows[0]["id"] != 1 or type(rows[0]["manifest_json"]) is not str:
        raise ValueError("registry_ptg_office_manifest_changed")
    manifest = json.loads(rows[0]["manifest_json"])
    result = office_manifest_data(manifest, rows[0]["manifest_sha256"], candidate_id, control_schema)
    await capture._require_office_layout(
        connection, schema, is_published="source_scope" in manifest["command"]["source"]
    )
    await capture._validate_rows(
        connection, schema, manifest["command"]["source"], result["snapshot"]["input_row_count"]
    )
    return result


async def admit_office_preparation(connection, candidate_id, *, context):
    """Use the existing lifecycle for the completed office candidate and exact same UUID."""
    from directory_catalog_runtime import NamedPreparationStore, NativePreparationAdmission

    handoff = await load_named_office_preparation(connection, str(candidate_id), control_schema=context.control_schema)
    if handoff is None:
        raise ValueError("registry_ptg_office_manifest_changed")
    identity = handoff["candidate_id"]
    request = NativePreparationAdmission(
        preparation_id=identity,
        node_id=context.control_schema,
        importer_id="office",
        dataset_key=identity,
        run_id=identity,
        generation_id=identity,
        publication_fence=identity,
        idempotency_key="office:" + identity,
        request_sha256=capture._digest(handoff),
    )
    store = NamedPreparationStore(
        connection, named_office_preparation_roles(office_preparation_roles(context)), kind="office"
    )
    row, claim = await store.admit_named(request, handoff, attempt_id=identity)
    return store, row, claim


async def require_office_preparation_frozen(connection, candidate_id, *, control_schema):
    """New captures need the real administrator phase; historical manifests remain readable."""
    from process.network_address_projection import _identifier

    handoff = await load_named_office_preparation(connection, candidate_id, control_schema=control_schema)
    if handoff is None:
        raise ValueError("registry_ptg_office_manifest_changed")
    metadata = await connection.fetchval("SELECT to_regclass($1)", "hp_snapshot_retention.preparation")
    if metadata is None:
        manifest = await connection.fetchval(
            f"SELECT manifest_json FROM {_identifier(handoff['storage_schema'])}.capture_manifest WHERE id=1"
        )
        manifest = json.loads(manifest) if type(manifest) is str else manifest
        if type(manifest) is not dict or "storage" in manifest:
            raise ValueError("registry_ptg_office_freeze_required")
        return handoff, None
    preparation_by_field = await connection.fetchrow(
        "SELECT state,handoff FROM hp_snapshot_retention.preparation WHERE preparation_id=$1 AND importer_id='office'",
        UUID(str(candidate_id)),
    )
    if preparation_by_field is None:
        manifest = await connection.fetchval(
            f"SELECT manifest_json FROM {_identifier(handoff['storage_schema'])}.capture_manifest WHERE id=1"
        )
        manifest = json.loads(manifest) if type(manifest) is str else manifest
        if type(manifest) is not dict or "storage" in manifest:
            raise ValueError("registry_ptg_office_freeze_required")
        return handoff, None
    recorded = (
        json.loads(preparation_by_field["handoff"])
        if type(preparation_by_field["handoff"]) is str
        else preparation_by_field["handoff"]
    )
    if recorded != handoff:
        raise ValueError("registry_ptg_office_manifest_changed")
    if preparation_by_field["state"] not in {"frozen", "validated", "published"}:
        raise ValueError("registry_ptg_office_freeze_required")
    return handoff, preparation_by_field["state"]


async def _source_context(session, service, selector, *, publisher):
    engine = service.scope_service
    document = await read_registry_ptg_published_plan_scope(
        session,
        scope_id=selector["scope_id"],
        client_id=selector["client_id"],
        approval_sha256=selector["scope_approval_sha256"],
        store=engine.store,
    )
    command = document["command"]
    identity = command["published_identity"]
    profile = engine.office_custody
    context = capture.RegistryPTGOfficeCaptureContext(
        UUID(command["scope_id"]),
        selector["client_id"],
        selector["scope_approval_sha256"],
        RegistryNetworkSourceCoordinates(**command["coordinates"]),
        RegistryPTGPublishedPlanSourceSpecification(
            command["scope_id"], engine.schema_name, identity["snapshot_id"], identity["source_key"]
        ),
        document["evidence"]["source_authority"],
        _graph_identity(identity),
        engine.store,
        engine.store.control_schema or registry_schema(),
        profile.owner_role,
        _reader_roles(engine),
        profile,
    )
    await capture._driver(session)
    office_preparation_roles(context)
    return context, command


async def _authorize(service, envelope, command, deadline):
    _remaining(deadline)
    await service.scope_service.authority.authorize(
        {"actor": envelope["actor"], "session_token_sha256": envelope["session_token_sha256"], "command": command},
        deadline=deadline,
    )
    _remaining(deadline)


def _checked_command(command, selector):
    command = validated_office_review_command(command)
    for name in ("capture_id", "scope_id", "client_id", "scope_approval_sha256", "idempotency_key"):
        if command[name] != selector[name]:
            raise ValueError("registry_ptg_scope_idempotency_conflict")
    return command


def _reply(descriptor):
    manifest = json.loads(descriptor.manifest_json)
    if (
        type(manifest) is not dict
        or set(manifest) not in (_MANIFEST_FIELDS, _MANIFEST_FIELDS | {"storage", "preparation_request_sha256"})
        or manifest["contract"] != "registry_ptg_office_capture.v1"
        or manifest["state"] != "prepared"
    ):
        raise ValueError("registry_ptg_office_manifest_changed")
    from process.registry_ptg_scope_engine import _sha256

    _sha256(descriptor.manifest_sha256)
    _sha256(manifest["command_sha256"])
    command = validated_office_review_command(manifest["command"])
    if "storage" in manifest and (
        capture._digest(manifest) != descriptor.manifest_sha256
        or manifest["command_sha256"] != capture._digest(command)
    ):
        raise ValueError("registry_ptg_office_manifest_changed")
    return {
        "capture_id": command["capture_id"],
        "state": "prepared",
        "command": command,
        "command_sha256": manifest["command_sha256"],
        "manifest_sha256": descriptor.manifest_sha256,
    }


async def _read_capture(session, context, selector, read_budget):
    driver = await capture._office_driver(session, context)
    schema = "registry_ptg_office_" + UUID(selector["capture_id"]).hex
    namespace = '"' + schema + '"'
    await driver.execute(
        f"LOCK TABLE {namespace}.office_assertion,{namespace}.capture_manifest IN ACCESS SHARE MODE NOWAIT"
    )
    await require_office_preparation_frozen(driver, selector["capture_id"], control_schema=context.control_schema)
    command_text = await driver.fetchval(
        f"SELECT CASE WHEN octet_length((manifest_json->'command')::text)<=131072 "
        f"THEN (manifest_json->'command')::text END "
        f"FROM {namespace}.capture_manifest WHERE id=1"
    )
    if type(command_text) is not str:
        raise ValueError("registry_ptg_office_manifest_changed")
    command = _checked_command(json.loads(command_text), selector)
    request = _request(command)
    descriptor = await _descriptor(session, context, request)
    witness = await verify_registry_ptg_office_witness(session, context, request, descriptor, read_budget=read_budget)
    return descriptor, witness


async def _one_batch(raw):
    yield raw


async def _prepare_capture(session, context, selector, read_budget):
    driver = await capture._office_driver(session, context, publisher=True)
    lock_key = int.from_bytes(
        hashlib.sha256(("office-prepare:" + selector["capture_id"]).encode()).digest()[:8], "big", signed=True
    )
    if await driver.fetchval("SELECT pg_catalog.pg_try_advisory_xact_lock($1)", lock_key) is not True:
        raise ValueError("registry_ptg_office_preparation_busy")
    state = await driver.fetchval(
        "SELECT state FROM hp_snapshot_retention.preparation WHERE preparation_id=$1 AND importer_id='office'",
        UUID(selector["capture_id"]),
    )
    if state == "abandoned":
        raise ValueError("registry_ptg_office_preparation_abandoned")
    existing = await load_named_office_preparation(
        driver, selector["capture_id"], control_schema=context.control_schema
    )
    if existing is not None:
        if existing["snapshot"]["request_sha256"] != capture._digest(selector):
            raise ValueError("registry_ptg_scope_idempotency_conflict")
        if state == "handed_off":
            command_text = await driver.fetchval(
                f"SELECT manifest_json->'command' FROM {capture._identifier(existing['storage_schema'])}.capture_manifest WHERE id=1"
            )
            command = json.loads(command_text) if type(command_text) is str else command_text
            return (
                {
                    "capture_id": selector["capture_id"],
                    "state": "handed_off",
                    "command": _checked_command(command, selector),
                    "command_sha256": existing["snapshot"]["command_sha256"],
                    "manifest_sha256": existing["snapshot"]["manifest_sha256"],
                },
                None,
                None,
            )
        if state not in {"frozen", "validated", "published"}:
            raise ValueError("registry_ptg_office_freeze_required")
        return None, None, None
    context = replace(context, preparation_request_sha256=capture._digest(selector))
    scope = await capture._scope(session, context)
    raw, digest, count = await produce_reviewed_office_rows(session, context, scope, selector, read_budget)
    request = capture.RegistryPTGOfficeCaptureRequest(
        UUID(selector["capture_id"]),
        digest,
        count,
        "reviewed_exact_office",
        selector["retained_generation_id"],
        selector["reason"],
        selector["idempotency_key"],
    )
    namespace = "registry_ptg_office_" + request.capture_id.hex
    if await driver.fetchval("SELECT pg_catalog.to_regnamespace($1)", namespace) is not None:
        raise ValueError("registry_ptg_office_capture_exists")
    descriptor = await capture.prepare_registry_ptg_office_capture(session, context, request, _one_batch(raw))
    return descriptor, digest, count


def _checked_selection(descriptor, selector, digest, count):
    command = _reply(descriptor)["command"]
    for name in ("reason", "retained_generation_id"):
        if command[name] != selector[name]:
            raise ValueError("registry_ptg_scope_idempotency_conflict")
    if digest is not None and (command["canonical_input_sha256"] != digest or command["input_row_count"] != count):
        raise ValueError("registry_ptg_scope_idempotency_conflict")
    return descriptor


async def _read_transaction(service, envelope, budget, deadline):
    """Read the frozen family in a fresh configured Reader snapshot."""
    schema = service.scope_service.store.control_schema or registry_schema()
    async with registry_company_approval_transaction(
        service.scope_service.reader_sessions, control_schema=schema
    ) as session:
        original = (await session.execute(text("SELECT pg_catalog.current_setting('search_path')"))).scalar_one()
        await session.execute(text("SELECT pg_catalog.set_config('search_path','pg_catalog,pg_temp',true)"))
        context, source_command = await _source_context(session, service, envelope["selector"], publisher=False)
        await _authorize(service, envelope, source_command, deadline)
        descriptor, witness = await _read_capture(session, context, envelope["selector"], budget)
        await _authorize(service, envelope, _reply(descriptor)["command"], deadline)
        await session.execute(
            text("SELECT pg_catalog.set_config('search_path',:original,true)"), {"original": original}
        )
        return descriptor, witness


async def _choices_transaction(service, envelope, budget, deadline):
    from process.registry_ptg_office_choices import read_registry_ptg_office_choices

    schema = service.scope_service.store.control_schema or registry_schema()
    async with registry_company_approval_transaction(
        service.scope_service.reader_sessions, control_schema=schema
    ) as session:
        original = (await session.execute(text("SELECT pg_catalog.current_setting('search_path')"))).scalar_one()
        await session.execute(text("SELECT pg_catalog.set_config('search_path','pg_catalog,pg_temp',true)"))
        context, command = await _source_context(session, service, envelope["selector"], publisher=False)
        await _authorize(service, envelope, command, deadline)
        scope = await capture._scope(session, context)
        result = await read_registry_ptg_office_choices(session, context, scope, command, envelope["selector"], budget)
        final_context, final_command = await _source_context(session, service, envelope["selector"], publisher=False)
        if final_command != command or await capture._scope(session, final_context) != scope:
            raise ValueError("registry_ptg_office_source_changed")
        await _authorize(service, envelope, final_command, deadline)
        await session.execute(
            text("SELECT pg_catalog.set_config('search_path',:original,true)"), {"original": original}
        )
        _remaining(deadline)
        return result


async def _reserve_capacity(service, session, context, selector):
    """Serialize owned preparation and count on a fresh genuine Reader snapshot."""
    driver = await capture._driver(session)
    identity = "office-capacity:" + context.owner_role
    lock_key = int.from_bytes(hashlib.sha256(identity.encode()).digest()[:8], "big", signed=True)
    if await driver.fetchval("SELECT pg_catalog.pg_try_advisory_xact_lock($1)", lock_key) is not True:
        raise ValueError("registry_ptg_office_preparation_busy")
    async with registry_company_approval_transaction(
        service.scope_service.reader_sessions, control_schema=context.control_schema
    ) as reader:
        await reader.execute(text("SELECT pg_catalog.set_config('search_path','pg_catalog,pg_temp',true)"))
        native = await capture._driver(reader)
        room = await native.fetchrow(
            "SELECT count(*) AS families,coalesce(bool_or(preparation_id=$2),false) AS existing "
            "FROM hp_snapshot_retention.preparation WHERE node_id=$1 AND importer_id='office' AND state<>'abandoned'",
            context.control_schema,
            UUID(selector["capture_id"]),
        )
        if room is None or type(room["families"]) is not int or type(room["existing"]) is not bool:
            raise ValueError("registry_ptg_office_profile_unavailable")
        if not 0 <= room["families"] <= 128 or room["families"] == 128 and not room["existing"]:
            raise ValueError("registry_ptg_office_capacity_exhausted")


async def _prepare_transaction(service, session, envelope, budget, deadline):
    original = (await session.execute(text("SELECT pg_catalog.current_setting('search_path')"))).scalar_one()
    await session.execute(text("SELECT pg_catalog.set_config('search_path','pg_catalog,pg_temp',true)"))
    context, source_command = await _source_context(session, service, envelope["selector"], publisher=True)
    await _authorize(service, envelope, source_command, deadline)
    await _reserve_capacity(service, session, context, envelope["selector"])
    descriptor, digest, count = await _prepare_capture(session, context, envelope["selector"], budget)
    is_created = descriptor is not None
    if is_created:
        await _authorize(service, envelope, descriptor["command"], deadline)
    await session.execute(text("SELECT pg_catalog.set_config('search_path',:original,true)"), {"original": original})
    _remaining(deadline)
    return descriptor, digest, count, is_created


def office_validation_report(descriptor, witness):
    """Keep the completed full-data witness stable across caller budget usage."""
    evidence = capture.named_office_evidence(witness.as_dict(), json.loads(descriptor.manifest_json)["command"])
    stable_by_field = {name: value for name, value in evidence.items() if name != "graph_budget"}
    return {
        "manifest_sha256": descriptor.manifest_sha256,
        "command_sha256": evidence["command_sha256"],
        "witness_sha256": capture._digest(stable_by_field),
    }


async def _retain_validation(service, envelope, descriptor, witness, deadline):
    schema = service.scope_service.store.control_schema or registry_schema()
    async with registry_company_approval_transaction(service.publisher_sessions, control_schema=schema) as session:
        await session.execute(text("SELECT pg_catalog.set_config('search_path','pg_catalog,pg_temp',true)"))
        context, command = await _source_context(session, service, envelope["selector"], publisher=True)
        await _authorize(service, envelope, command, deadline)
        driver = await capture._driver(session)
        _, state = await require_office_preparation_frozen(driver, str(descriptor.capture_id), control_schema=schema)
        if state is None:
            return
        store, row, claim = await admit_office_preparation(driver, descriptor.capture_id, context=context)
        report_by_field = {
            "kind": "office",
            "candidate_id": str(descriptor.capture_id),
            "data": office_validation_report(descriptor, witness),
        }
        if row["state"] == "published":
            if row["validation"]["evidence"] != report_by_field:
                raise ValueError("registry_ptg_office_validation_changed")
            return
        if claim is None:
            raise ValueError("registry_ptg_office_freeze_required")
        await store.validate_named(claim, report_by_field)
        await store.yield_named_claim(claim)
        _remaining(deadline)


async def publish_office_preparation(driver, context, descriptor, witness, approval_sha256, hold_sha256):
    """Terminalize only the real approval/hold event in that same caller transaction."""
    _, state = await require_office_preparation_frozen(
        driver, str(descriptor.capture_id), control_schema=context.control_schema
    )
    if state is None:
        raise ValueError("registry_ptg_office_freeze_required")
    store, preparation_by_field, claim = await admit_office_preparation(driver, descriptor.capture_id, context=context)
    report_by_field = {
        "kind": "office",
        "candidate_id": str(descriptor.capture_id),
        "data": office_validation_report(descriptor, witness),
    }
    publication_by_field = {
        "kind": "office",
        "candidate_id": str(descriptor.capture_id),
        "fence_token": str(preparation_by_field["publication_fence"]),
        "data": {
            "capture_id": str(descriptor.capture_id),
            "manifest_sha256": descriptor.manifest_sha256,
            "approval_sha256": approval_sha256,
            "hold_sha256": hold_sha256,
        },
    }
    if preparation_by_field["state"] == "published":
        if (
            preparation_by_field["validation"]["evidence"] != report_by_field
            or preparation_by_field["publication"] != publication_by_field
        ):
            raise ValueError("registry_ptg_office_publication_changed")
        return
    if claim is None:
        raise ValueError("registry_ptg_office_freeze_required")
    await store.validate_named(claim, report_by_field)
    await store.publish_named(claim, publication_by_field)


@dataclass(frozen=True)
class RegistryPTGOfficePreparationService:
    """One installed Publisher factory; requests cannot select credentials or roles."""

    scope_service: RegistryPTGScopeEngineService
    publisher_sessions: object = field(repr=False)

    def __post_init__(self):
        if type(self.scope_service) is not RegistryPTGScopeEngineService or self.scope_service.office_custody is None:
            raise ValueError("registry_ptg_office_service_unconfigured")
        engine = _source_engine(self.publisher_sessions)
        _source_engine(self.scope_service.reader_sessions)
        if (
            engine.url.username != self.scope_service.office_custody.publisher_role
            or engine.url.username in _reader_roles(self.scope_service)
        ):
            raise ValueError("registry_ptg_office_service_unconfigured")

    async def prepare(self, envelope, *, deadline=None):
        """Prepare or replay one exact selection; never approve, retain or admit it."""
        return await self._run_operation(envelope, read=False, deadline=deadline)

    async def read(self, envelope, *, deadline=None):
        """Recover a closed command under fresh source and actual target authority."""
        return await self._run_operation(envelope, read=True, deadline=deadline)

    async def office_choices(self, envelope, *, deadline=None):
        """Read genuine approved offices through the installed restricted Reader."""
        from process.registry_ptg_office_preparation_contract import validated_office_choices_envelope

        envelope = validated_office_choices_envelope(envelope)
        deadline = _deadline(deadline)
        try:
            async with asyncio.timeout_at(deadline):
                budget = RegistryPTGGraphReadBudget(**envelope["read_limits"])
                return await _choices_transaction(self, envelope, budget, deadline)
        except TimeoutError:
            raise RegistryPTGScopeDeadlineExpired(outcome_unknown=False) from None

    async def _run_operation(self, envelope, *, read, deadline):
        envelope = validated_preparation_envelope(envelope, read=read)
        deadline = _deadline(deadline)
        schema = self.scope_service.store.control_schema or registry_schema()
        has_commit_started = False
        try:
            async with asyncio.timeout_at(deadline):
                budget = RegistryPTGGraphReadBudget(**envelope["read_limits"])
                if read:
                    descriptor, witness = await _read_transaction(self, envelope, budget, deadline)
                    await _retain_validation(self, envelope, descriptor, witness, deadline)
                    _remaining(deadline)
                    return _reply(descriptor)
                async with registry_company_approval_transaction(
                    self.publisher_sessions, control_schema=schema
                ) as session:
                    descriptor, digest, count, has_commit_started = await _prepare_transaction(
                        self, session, envelope, budget, deadline
                    )
                if descriptor is not None:
                    _remaining(deadline)
                    return descriptor
                # Release the Publisher's source locks before genuine Reader recovery.
                descriptor, witness = await _read_transaction(self, envelope, budget, deadline)
                _checked_selection(descriptor, envelope["selector"], digest, count)
                await _retain_validation(self, envelope, descriptor, witness, deadline)
                _remaining(deadline)
                return _reply(descriptor)
        except TimeoutError:
            raise RegistryPTGScopeDeadlineExpired(outcome_unknown=has_commit_started) from None
        except Exception:
            if has_commit_started:
                raise RegistryPTGScopeDeadlineExpired(outcome_unknown=True) from None
            raise
