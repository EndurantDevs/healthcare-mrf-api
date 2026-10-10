# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Protected preparation/recovery with an installed genuine office Publisher."""

import asyncio
import hashlib
import json
from dataclasses import dataclass, field
from uuid import UUID

from sqlalchemy import text

from db.registry_schema import registry_schema
from process import registry_ptg_office_capture as capture
from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
from process.registry_company_approval_fence import _source_engine, registry_company_approval_transaction
from process.registry_ptg_graph_reader import RegistryPTGGraphReadBudget
from process.registry_ptg_office_approval import _descriptor, _reader_roles, _request
from process.registry_ptg_office_custody import verify_registry_ptg_office_custody
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
    driver = await capture._driver(session)
    await verify_registry_ptg_office_custody(driver, context, publisher=publisher)
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
    if capture._digest(manifest) != descriptor.manifest_sha256:
        raise ValueError("registry_ptg_office_manifest_changed")
    command = validated_office_review_command(manifest["command"])
    if manifest["command_sha256"] != capture._digest(command):
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
    await capture._custody(driver, schema, context)
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
    await verify_registry_ptg_office_witness(session, context, request, descriptor, read_budget=read_budget)
    return descriptor


async def _one_batch(raw):
    yield raw


async def _prepare_capture(session, context, selector, read_budget):
    driver = await capture._office_driver(session, context, publisher=True)
    lock_key = int.from_bytes(
        hashlib.sha256(("office-prepare:" + selector["capture_id"]).encode()).digest()[:8], "big", signed=True
    )
    if await driver.fetchval("SELECT pg_catalog.pg_try_advisory_xact_lock($1)", lock_key) is not True:
        raise ValueError("registry_ptg_office_preparation_busy")
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
        return None, digest, count
    descriptor = await capture.prepare_registry_ptg_office_capture(session, context, request, _one_batch(raw))
    return descriptor, digest, count


def _checked_selection(descriptor, selector, digest, count):
    command = _reply(descriptor)["command"]
    for name in ("reason", "retained_generation_id"):
        if command[name] != selector[name]:
            raise ValueError("registry_ptg_scope_idempotency_conflict")
    if command["canonical_input_sha256"] != digest or command["input_row_count"] != count:
        raise ValueError("registry_ptg_scope_idempotency_conflict")
    return descriptor


async def _read_transaction(service, envelope, budget, deadline):
    """The Publisher cannot SELECT a closed family; use the genuine Reader."""
    schema = service.scope_service.store.control_schema or registry_schema()
    async with registry_company_approval_transaction(
        service.scope_service.reader_sessions, control_schema=schema
    ) as session:
        original = (await session.execute(text("SELECT pg_catalog.current_setting('search_path')"))).scalar_one()
        await session.execute(text("SELECT pg_catalog.set_config('search_path','pg_catalog,pg_temp',true)"))
        context, source_command = await _source_context(session, service, envelope["selector"], publisher=False)
        await _authorize(service, envelope, source_command, deadline)
        descriptor = await _read_capture(session, context, envelope["selector"], budget)
        await _authorize(service, envelope, _reply(descriptor)["command"], deadline)
        await session.execute(
            text("SELECT pg_catalog.set_config('search_path',:original,true)"), {"original": original}
        )
        return descriptor


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
            "SELECT pg_catalog.count(*) AS families,pg_catalog.to_regnamespace($2) IS NOT NULL AS existing "
            "FROM pg_catalog.pg_namespace WHERE nspowner=(SELECT oid FROM pg_catalog.pg_roles WHERE rolname=$1)",
            context.owner_role,
            "registry_ptg_office_" + UUID(selector["capture_id"]).hex,
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
        await _authorize(service, envelope, _reply(descriptor)["command"], deadline)
    await session.execute(text("SELECT pg_catalog.set_config('search_path',:original,true)"), {"original": original})
    _remaining(deadline)
    return descriptor, digest, count, is_created


def _database_identity(engine):
    url = engine.url
    return url.host, url.port, url.database, url.query


@dataclass(frozen=True)
class RegistryPTGOfficePreparationService:
    """One installed Publisher factory; requests cannot select credentials or roles."""

    scope_service: RegistryPTGScopeEngineService
    publisher_sessions: object = field(repr=False)

    def __post_init__(self):
        if type(self.scope_service) is not RegistryPTGScopeEngineService or self.scope_service.office_custody is None:
            raise ValueError("registry_ptg_office_service_unconfigured")
        engine = _source_engine(self.publisher_sessions)
        reader = _source_engine(self.scope_service.reader_sessions)
        if (
            engine is reader
            or _database_identity(engine) != _database_identity(reader)
            or engine.url.username != self.scope_service.office_custody.publisher_role
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
                    descriptor = await _read_transaction(self, envelope, budget, deadline)
                    _remaining(deadline)
                    return _reply(descriptor)
                async with registry_company_approval_transaction(
                    self.publisher_sessions, control_schema=schema
                ) as session:
                    descriptor, digest, count, has_commit_started = await _prepare_transaction(
                        self, session, envelope, budget, deadline
                    )
                if descriptor is None:
                    # Release the Publisher's source locks before genuine Reader recovery.
                    descriptor = await _read_transaction(self, envelope, budget, deadline)
                    _checked_selection(descriptor, envelope["selector"], digest, count)
                _remaining(deadline)
                return _reply(descriptor)
        except TimeoutError:
            raise RegistryPTGScopeDeadlineExpired(outcome_unknown=has_commit_started) from None
        except Exception:
            if has_commit_started:
                raise RegistryPTGScopeDeadlineExpired(outcome_unknown=True) from None
            raise
