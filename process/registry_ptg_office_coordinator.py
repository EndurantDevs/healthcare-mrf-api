# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Durable exact-request office preparation through installed distinct writers."""

import asyncio
from dataclasses import dataclass, field
from uuid import UUID, uuid4

import asyncpg
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from db.registry_schema import registry_schema
from process.network_address_projection import _identifier
from process.network_cms_registry_address_capture import native_driver
from process.registry_company_approval_fence import (
    registry_company_approval_transaction,
)
from process.registry_ptg_graph_reader import RegistryPTGGraphReadBudget
from process.registry_ptg_office_intent import (
    OPERATION,
    VERSION,
    _intent,
    _stored_receipt,
    reserve_registry_ptg_office_prepare_intent,
)
from process.registry_ptg_office_preparation import (
    RegistryPTGOfficePreparationService,
)
from process.registry_ptg_published_plan_scope import (
    read_registry_ptg_published_plan_scope,
)
from process.registry_ptg_scope_engine import (
    RegistryPTGScopeDeadlineExpired,
    _deadline,
    _object,
    _remaining,
    _sha256,
    _uuid,
)
from process.registry_publication_permissions import (
    LIFECYCLE_COLUMNS,
)
from process.registry_publication_queue import (
    _document,
    _json,
    finish_registry_publication_request,
)
from process.registry_record_store import RegistryActor
from process.registry_source_observation_store import _namespace

_MUTABLE = LIFECYCLE_COLUMNS
_COLUMNS = (
    "request_id,actor_key,session_token_sha256,"
    "CASE WHEN octet_length(actor_json::text)<=2048 THEN actor_json END AS actor_json,"
    "CASE WHEN octet_length(command_json::text)<=131072 THEN command_json END AS command_json,"
    "idempotency_key,request_sha256,state,created_at,"
    "CASE WHEN octet_length(result_json::text)<=65536 THEN result_json END AS result_json"
)


def _envelope(document):
    _object(
        document,
        {"actor", "session_token_sha256", "source_command", "selector", "read_limits"},
    )
    actor = _object(document["actor"], {"kind", "user_id", "client_id"})
    _uuid(actor["user_id"])
    record_actor = RegistryActor(actor["kind"], UUID(actor["user_id"]), actor["client_id"])
    semantic = _intent(document["source_command"], document["selector"], record_actor)
    _sha256(document["session_token_sha256"])
    RegistryPTGGraphReadBudget(
        **_object(
            document["read_limits"],
            {"maximum_bytes", "maximum_pages", "maximum_coordinates"},
        )
    )
    return _document(_json(document)), record_actor, semantic


def _writer_engine(sessions):
    if type(sessions) is not async_sessionmaker or sessions.class_ is not AsyncSession or sessions.kw.get("binds"):
        raise ValueError("registry_ptg_office_coordinator_unconfigured")
    engine = sessions.kw.get("bind")
    if type(engine) is not AsyncEngine or engine.dialect.name != "postgresql" or engine.dialect.driver != "asyncpg":
        raise ValueError("registry_ptg_office_coordinator_unconfigured")
    return engine


async def _approved_source(coordinator, envelope, deadline):
    scope = coordinator.preparation.scope_service
    async with registry_company_approval_transaction(
        scope.reader_sessions, control_schema=coordinator.schema
    ) as session:
        await session.execute(text("SELECT pg_catalog.set_config('search_path','pg_catalog,pg_temp',true)"))
        selector = envelope["selector"]
        approved_scope = await read_registry_ptg_published_plan_scope(
            session,
            scope_id=selector["scope_id"],
            client_id=selector["client_id"],
            approval_sha256=selector["scope_approval_sha256"],
            store=scope.store,
        )
        if (
            approved_scope["command"] != envelope["source_command"]
            or approved_scope["approval_sha256"] != selector["scope_approval_sha256"]
        ):
            raise ValueError("registry_ptg_office_intent_source_changed")
        await scope.authority.authorize(
            {
                "actor": envelope["actor"],
                "session_token_sha256": envelope["session_token_sha256"],
                "command": approved_scope["command"],
            },
            deadline=deadline,
        )
        _remaining(deadline)
        return approved_scope


async def _reserve(coordinator, envelope, actor, approved_scope, deadline, progress):
    async with coordinator.management_sessions() as session, session.begin():
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL READ COMMITTED, READ WRITE"))
        await session.execute(text("SELECT pg_catalog.set_config('search_path','pg_catalog,pg_temp',true)"))
        connection = await native_driver(session)
        # Re-read the source authorization before the intent INSERT.
        if await _approved_source(coordinator, envelope, deadline) != approved_scope:
            raise ValueError("registry_ptg_office_intent_source_changed")
        receipt = await reserve_registry_ptg_office_prepare_intent(
            connection,
            envelope["source_command"],
            envelope["selector"],
            actor,
            session_token_sha256=envelope["session_token_sha256"],
            control_schema=coordinator.schema,
        )
        if await _approved_source(coordinator, envelope, deadline) != approved_scope:
            raise ValueError("registry_ptg_office_intent_source_changed")
        progress["commit_started"] = True
    return receipt


async def _request(connection, coordinator, request_id, semantic):
    record = await connection.fetchrow(
        f"SELECT {_COLUMNS} FROM {_namespace(coordinator.schema)}.registry_publication_request WHERE request_id=$1",
        UUID(request_id),
    )
    if record is None:
        raise ValueError("registry_ptg_office_intent_unavailable")
    return record, _stored_receipt(record, semantic, True)


async def _claim(coordinator, receipt, semantic):
    async with (
        coordinator.queue_pool.acquire() as connection,
        connection.transaction(isolation="read_committed"),
    ):
        await connection.execute("SET LOCAL search_path TO pg_catalog,pg_temp")
        request_record, current = await _request(connection, coordinator, receipt["request_id"], semantic)
        if current["command_sha256"] != receipt["command_sha256"]:
            raise ValueError("registry_ptg_office_intent_stored_invalid")
        if current["state"] == "completed":
            completed = _document(request_record["result_json"])
            _object(
                completed,
                {
                    "operation",
                    "capture_id",
                    "state",
                    "command_sha256",
                    "manifest_sha256",
                },
            )
            if completed["operation"] != OPERATION:
                raise ValueError("registry_ptg_office_intent_result_changed")
            return current, None, completed
        if current["state"] == "rejected":
            raise ValueError("registry_ptg_office_intent_rejected")
        lease = await connection.fetchval(
            f"UPDATE {_namespace(coordinator.schema)}.registry_publication_request "
            "SET state='running',lease_id=$2,lease_expires_at=pg_catalog.clock_timestamp()+interval '300 seconds',updated_at=pg_catalog.now() "
            "WHERE request_id=$1 AND request_sha256=$3 AND command_json=$4::jsonb "
            "AND command_json->>'operation'=$5 AND command_json->>'version'=$6 "
            "AND command_json->>'semantic_sha256'=$7 "
            "AND (state='queued' OR state='running' AND lease_expires_at<=pg_catalog.clock_timestamp()) RETURNING lease_id",
            UUID(receipt["request_id"]),
            uuid4(),
            request_record["request_sha256"],
            _json(current["command"]),
            OPERATION,
            str(VERSION),
            current["semantic_sha256"],
        )
        if lease is not None and (not isinstance(lease, UUID) or not lease.int):
            raise ValueError("registry_queue_lease_invalid")
        current["state"] = "running"
        return current, lease, None


def _physical_envelope(envelope, receipt, *, read=False):
    selection_by_field = receipt["command"]["selector"]
    if read:
        selection_by_field = {
            name: selection_by_field[name]
            for name in (
                "capture_id",
                "scope_id",
                "client_id",
                "scope_approval_sha256",
                "idempotency_key",
            )
        }
    return {name: envelope[name] for name in ("actor", "session_token_sha256", "read_limits")} | {
        "selector": selection_by_field
    }


def _completion(prepared, receipt):
    _object(
        prepared,
        {"capture_id", "state", "command", "command_sha256", "manifest_sha256"},
    )
    if prepared["state"] != "prepared" or prepared["capture_id"] != receipt["command"]["selector"]["capture_id"]:
        raise ValueError("registry_ptg_office_intent_result_changed")
    from process.registry_ptg_office_capture import _digest
    from process.registry_ptg_office_preparation import _checked_command
    from process.registry_ptg_office_review_contract import is_legacy_office_review_command

    command = _checked_command(prepared["command"], receipt["command"]["selector"])
    if not is_legacy_office_review_command(prepared["command"]) and _digest(command) != prepared["command_sha256"]:
        raise ValueError("registry_ptg_office_intent_result_changed")
    if any(command[name] != receipt["command"]["selector"][name] for name in ("reason", "retained_generation_id")):
        raise ValueError("registry_ptg_office_intent_result_changed")
    for name in ("command_sha256", "manifest_sha256"):
        _sha256(prepared[name])
    return {
        "operation": OPERATION,
        **{name: prepared[name] for name in ("capture_id", "state", "command_sha256", "manifest_sha256")},
    }


async def _finish(coordinator, receipt, semantic, lease, result):
    async with (
        coordinator.queue_pool.acquire() as connection,
        connection.transaction(isolation="read_committed"),
    ):
        await connection.execute("SET LOCAL search_path TO pg_catalog,pg_temp")
        _, current = await _request(connection, coordinator, receipt["request_id"], semantic)
        if current["command_sha256"] != receipt["command_sha256"]:
            raise ValueError("registry_ptg_office_intent_stored_invalid")
        await finish_registry_publication_request(
            connection,
            UUID(receipt["request_id"]),
            lease,
            state="completed",
            result=result,
            control_schema=coordinator.schema,
        )


async def _yield_pending(coordinator, receipt, semantic, lease):
    """Keep the exact durable request resumable after the independent administrator step."""
    async with coordinator.queue_pool.acquire() as connection, connection.transaction(isolation="read_committed"):
        await connection.execute("SET LOCAL search_path TO pg_catalog,pg_temp")
        _, current = await _request(connection, coordinator, receipt["request_id"], semantic)
        if current["command_sha256"] != receipt["command_sha256"]:
            raise ValueError("registry_ptg_office_intent_stored_invalid")
        updated = await connection.fetchval(
            f"UPDATE {_namespace(coordinator.schema)}.registry_publication_request SET lease_expires_at=clock_timestamp(),updated_at=now() "
            "WHERE request_id=$1 AND lease_id=$2 AND state='running' RETURNING lease_id",
            UUID(receipt["request_id"]),
            lease,
        )
        if updated != lease:
            raise ValueError("registry_queue_lease_invalid")


@dataclass(frozen=True)
class RegistryPTGOfficeIntentCoordinator:
    """Use configured management and queue factories with fresh client/source authorization."""

    preparation: RegistryPTGOfficePreparationService
    management_sessions: object = field(repr=False)
    queue_pool: object = field(repr=False)
    management_role: str
    queue_role: str
    owner_role: str

    @property
    def schema(self):
        """Use the installed source service namespace; requests cannot select it."""
        return self.preparation.scope_service.store.control_schema or registry_schema()

    def __post_init__(self):
        if (
            type(self.preparation) is not RegistryPTGOfficePreparationService
            or type(self.queue_pool) is not asyncpg.Pool
        ):
            raise ValueError("registry_ptg_office_coordinator_unconfigured")
        roles = (self.management_role, self.queue_role, self.owner_role)
        for role in roles:
            _identifier(role)
        scope = self.preparation.scope_service
        forbidden_roles = {
            scope.store.owner_role,
            scope.store.approval_role,
            scope.office_custody.owner_role,
            scope.office_custody.publisher_role,
        }
        writer = _writer_engine(self.management_sessions)
        reader = _writer_engine(scope.reader_sessions)
        forbidden_roles.add(reader.url.username)
        if len(set(roles)) != 3 or set(roles[:2]) & forbidden_roles or writer.url.username != self.management_role:
            raise ValueError("registry_ptg_office_coordinator_unconfigured")

    async def list(self, document, *, deadline=None):
        """Read one source/actor page under fresh client/source authorization."""
        from process.registry_ptg_office_intent_view import (
            list_registry_ptg_office_prepare_intents,
            validated_office_intent_list_envelope,
        )

        envelope = validated_office_intent_list_envelope(document)
        actor = envelope["actor"]
        actor = RegistryActor(actor["kind"], UUID(actor["user_id"]), actor["client_id"])
        deadline = _deadline(deadline)
        try:
            async with asyncio.timeout_at(deadline):
                approved_scope = await _approved_source(self, envelope, deadline)
                async with (
                    self.queue_pool.acquire() as connection,
                    connection.transaction(isolation="read_committed", readonly=True),
                ):
                    await connection.execute("SET LOCAL search_path TO pg_catalog,pg_temp")
                    selection = envelope["selector"]
                    page = await list_registry_ptg_office_prepare_intents(
                        connection,
                        actor,
                        envelope["source_command"],
                        scope_approval_sha256=selection["scope_approval_sha256"],
                        after=selection["after"],
                        limit=selection["limit"],
                        control_schema=self.schema,
                    )
                if await _approved_source(self, envelope, deadline) != approved_scope:
                    raise ValueError("registry_ptg_office_intent_source_changed")
                _remaining(deadline)
                return page
        except TimeoutError:
            raise RegistryPTGScopeDeadlineExpired(outcome_unknown=False) from None

    async def prepare(self, document, *, deadline=None):
        """Commit intent and its exact lease before preparation; retries retain handles."""
        envelope, actor, semantic = _envelope(document)
        deadline, progress = _deadline(deadline), {"commit_started": False}
        try:
            async with asyncio.timeout_at(deadline):
                approved_scope = await _approved_source(self, envelope, deadline)
                receipt = await _reserve(self, envelope, actor, approved_scope, deadline, progress)
                if await _approved_source(self, envelope, deadline) != approved_scope:
                    raise ValueError("registry_ptg_office_intent_source_changed")
                receipt, lease, completed = await _claim(self, receipt, semantic)
                if lease is None and receipt["state"] != "completed":
                    return {"intent": receipt, "preparation": None}
                method = self.preparation.read if completed is not None else self.preparation.prepare
                prepared = await method(
                    _physical_envelope(envelope, receipt, read=completed is not None),
                    deadline=deadline,
                )
                if prepared.get("state") == "handed_off":
                    if completed is not None:
                        raise ValueError("registry_ptg_office_intent_result_changed")
                    from process.registry_ptg_office_preparation import _checked_command

                    _object(prepared, {"capture_id", "state", "command", "command_sha256", "manifest_sha256"})
                    _checked_command(prepared["command"], receipt["command"]["selector"])
                    if any(
                        prepared["command"][name] != receipt["command"]["selector"][name]
                        for name in ("reason", "retained_generation_id")
                    ):
                        raise ValueError("registry_ptg_office_intent_result_changed")
                    if await _approved_source(self, envelope, deadline) != approved_scope:
                        raise ValueError("registry_ptg_office_intent_source_changed")
                    await _yield_pending(self, receipt, semantic, lease)
                    _remaining(deadline)
                    return {"intent": receipt, "preparation": prepared}
                completion_receipt = _completion(prepared, receipt)
                if await _approved_source(self, envelope, deadline) != approved_scope:
                    raise ValueError("registry_ptg_office_intent_source_changed")
                if completed is not None:
                    if completed != completion_receipt:
                        raise ValueError("registry_ptg_office_intent_result_changed")
                else:
                    await _finish(self, receipt, semantic, lease, completion_receipt)
                _remaining(deadline)
                return {
                    "intent": {**receipt, "state": "completed"},
                    "preparation": prepared,
                }
        except TimeoutError:
            raise RegistryPTGScopeDeadlineExpired(outcome_unknown=progress["commit_started"]) from None
        except Exception:
            if progress["commit_started"]:
                raise RegistryPTGScopeDeadlineExpired(outcome_unknown=True) from None
            raise
