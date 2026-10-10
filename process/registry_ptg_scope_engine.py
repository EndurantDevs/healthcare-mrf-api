# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Server-bound source review: native source proof and fresh operator authority."""

from __future__ import annotations

import asyncio
import json
import math
import re
import time
from dataclasses import asdict, dataclass, field
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener
from uuid import UUID

from sqlalchemy import text

from db.registry_schema import registry_schema
from process.network_address_projection import _identifier
from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
from process.ptg_parts.result_archive_source_authority import (
    PTG_PUBLISHED_RESULT_SOURCE_AUTHORITY_CONTRACT,
    PTG_RESULT_ARCHIVE_SOURCE_AUTHORITY_CONTRACT,
    prepare_ptg_result_archive_source_authority,
)
from process.registry_company_approval_fence import (
    registry_company_approval_transaction,
)
from process.registry_ptg_cohort_authority import (
    _GRAPH_FIELDS,
    _resolved_source_state,
    _source_specification,
)
from process.registry_ptg_producer_scope import (
    RegistryPTGOperatorReviewCommand,
    RegistryPTGProducerFileVersion,
    RegistryPTGProducerScopeError,
    RegistryPTGProducerScopeStore,
    RegistryPTGSourceOwnershipWitness,
    _approval_document,
    _canonical,
    _command_document,
    _digest,
    _engine_identity,
    _evidence,
    _protected_store,
    _require_current_approved_company,
    _required_text,
    _retain_approval,
    _sha256,
)
from process.registry_record_store import RegistryActor

INTENT_FIELDS = frozenset(
    {
        "scope_id",
        "statement_id",
        "client_id",
        "legal_company_id",
        "approved_revision",
        "source_file_import_id",
        "file_versions",
        "company_key",
        "cohort_id",
        "reason",
        "idempotency_key",
    }
)
FULL_FIELDS = INTENT_FIELDS | {"operation", "coordinates", "ownership", "source"}
SCOPE_DEADLINE_HEADER = "X-Request-Deadline-Ms"
SCOPE_REQUEST_TIMEOUT_SECONDS = 9.0


class RegistryPTGScopeDeadlineExpired(TimeoutError):
    """Expiry before commit differs from an uncertain commit or lost reply."""

    def __init__(self, *, outcome_unknown=False):
        super().__init__("registry_ptg_scope_deadline_expired")
        self.outcome_unknown = outcome_unknown


def registry_ptg_scope_deadline(encoded=None):
    """Convert a transport hint to a capped monotonic budget, never authority."""
    remaining = SCOPE_REQUEST_TIMEOUT_SECONDS
    if encoded is not None:
        if type(encoded) is not str or len(encoded) != 13 or re.fullmatch(r"[1-9][0-9]{12}", encoded) is None:
            raise ValueError("registry_ptg_scope_request_invalid")
        remaining = min(remaining, int(encoded) / 1000 - time.time())
    return asyncio.get_running_loop().time() + max(0, remaining)


def _deadline(deadline):
    if deadline is None:
        return registry_ptg_scope_deadline()
    if type(deadline) not in {int, float} or not math.isfinite(deadline):
        raise ValueError("registry_ptg_scope_request_invalid")
    return min(deadline, registry_ptg_scope_deadline())


def _remaining(deadline):
    remaining = deadline - asyncio.get_running_loop().time()
    if remaining <= 0:
        raise TimeoutError
    return remaining


def _object(document, fields):
    if type(document) is not dict or set(document) != fields:
        raise ValueError("registry_ptg_scope_request_invalid")
    return document


def _uuid(identifier):
    if type(identifier) is not str:
        raise ValueError("registry_ptg_scope_request_invalid")
    parsed = UUID(identifier)
    if not parsed.int or str(parsed) != identifier:
        raise ValueError("registry_ptg_scope_request_invalid")
    return parsed


def _intent(document):
    if type(document) is dict and "review_type" in document:
        from process.registry_ptg_published_plan_contract import validated_intent

        return validated_intent(document)
    _object(document, INTENT_FIELDS)
    for name in ("scope_id", "statement_id", "legal_company_id"):
        _uuid(document[name])
    if type(document["approved_revision"]) is not int or not 1 <= document["approved_revision"] < 2**63:
        raise ValueError("registry_ptg_scope_request_invalid")
    for name, maximum in {
        "client_id": 64,
        "source_file_import_id": 64,
        "company_key": 512,
        "cohort_id": 128,
        "reason": 1000,
        "idempotency_key": 128,
    }.items():
        _required_text(document[name], maximum)
    versions = document["file_versions"]
    if type(versions) is not list or not 1 <= len(versions) <= 128:
        raise ValueError("registry_ptg_scope_request_invalid")
    for version in versions:
        _object(version, {"source_file_version_id", "source_identity_sha256", "raw_sha256"})
        _required_text(version["source_file_version_id"], 128)
        _engine_identity(version["source_identity_sha256"])
        _sha256(version["raw_sha256"])
    if len({version["source_file_version_id"] for version in versions}) != len(versions):
        raise ValueError("registry_ptg_scope_request_invalid")
    return {**document, "file_versions": sorted(versions, key=lambda version: version["source_file_version_id"])}


def _ownership(document):
    _object(document, set(RegistryPTGSourceOwnershipWitness.__dataclass_fields__))
    for name, field_value in document.items():
        if name == "engine_source_identity_hash":
            _engine_identity(field_value)
        elif name == "status" and type(field_value) is str and field_value == "":
            continue
        else:
            maximum = {"import_month": 16, "status": 32, "snapshot_id": 96, "source_key": 128}.get(name, 64)
            _required_text(field_value, maximum * 4)
            if len(field_value) > maximum:
                raise ValueError("registry_ptg_scope_request_invalid")
    return dict(document)


def _full(document):
    if type(document) is dict and document.get("operation") == "approve_ptg_published_plan_scope":
        from process.registry_ptg_published_plan_contract import validated_command

        return validated_command(document)
    _object(document, FULL_FIELDS)
    intent = _intent({name: document[name] for name in INTENT_FIELDS})
    if document["operation"] != "approve_ptg_source_scope":
        raise ValueError("registry_ptg_scope_request_invalid")
    coordinates = RegistryNetworkSourceCoordinates(
        **_object(document["coordinates"], set(RegistryNetworkSourceCoordinates.__dataclass_fields__))
    )
    if coordinates.source_system != "ptg":
        raise ValueError("registry_ptg_scope_request_invalid")
    ownership = _ownership(document["ownership"])
    source = _object(document["source"], {"binding_source_key", "snapshot_id", "ptg_schema_name"})
    _identifier(source["ptg_schema_name"])
    _required_text(source["snapshot_id"], 96)
    _required_text(source["binding_source_key"], 512)
    if source["ptg_schema_name"] != coordinates.dataset_schema or source["snapshot_id"] != ownership["snapshot_id"]:
        raise ValueError("registry_ptg_scope_request_invalid")
    return {
        **intent,
        "operation": document["operation"],
        "coordinates": asdict(coordinates),
        "ownership": ownership,
        "source": dict(source),
    }


def validated_registry_ptg_scope_envelope(document, operation):
    """Close control input before acquiring server service context or sessions."""
    fields = {"actor", "session_token_sha256", "command"} | ({"ownership"} if operation == "preview" else set())
    _object(document, fields)
    actor = _object(document["actor"], {"kind", "user_id", "client_id"})
    if actor["kind"] != "platform_admin" or actor["client_id"] != "system":
        raise PermissionError("registry_ptg_scope_actor_invalid")
    _uuid(actor["user_id"])
    _sha256(document["session_token_sha256"])
    command = _intent(document["command"]) if operation == "preview" else _full(document["command"])
    ownership = _ownership(document["ownership"]) if operation == "preview" else command["ownership"]
    if any(ownership[name] != command[name] for name in ("client_id", "source_file_import_id")):
        raise ValueError("registry_ptg_scope_request_invalid")
    return {
        **document,
        "actor": dict(actor),
        "command": command,
        **({"ownership": ownership} if operation == "preview" else {}),
    }


def validated_registry_ptg_choices_envelope(document):
    """Close an administrative read without accepting asserted cohort choices."""
    _object(document, {"actor", "selection", "ownership"})
    actor = _object(document["actor"], {"kind", "user_id", "client_id"})
    if actor["kind"] != "platform_admin" or actor["client_id"] != "system":
        raise PermissionError("registry_ptg_scope_actor_invalid")
    _uuid(actor["user_id"])
    selection = _object(document["selection"], {"client_id", "source_file_import_id", "after", "limit"})
    _required_text(selection["client_id"], 64)
    _required_text(selection["source_file_import_id"], 64)
    if selection["after"] is not None:
        _uuid(selection["after"])
    if type(selection["limit"]) is not int or not 1 <= selection["limit"] <= 20:
        raise ValueError("registry_ptg_scope_request_invalid")
    ownership = _ownership(document["ownership"])
    if any(selection[name] != ownership[name] for name in ("client_id", "source_file_import_id")):
        raise ValueError("registry_ptg_scope_request_invalid")
    return json.loads(_canonical(document))


class _NoRedirects(HTTPRedirectHandler):
    def redirect_request(self, *_args, **_kwargs):
        """Refuse forwarding authenticated authority requests to redirects."""
        return None


def _unique_object(pairs):
    document_by_field = {}
    for name, field_value in pairs:
        if name in document_by_field:
            raise ValueError("registry_ptg_scope_authority_invalid")
        document_by_field[name] = field_value
    return document_by_field


@dataclass(frozen=True)
class RegistryPTGScopeAuthorityClient:
    """One fixed server-configured app origin; never a caller URL or token."""

    base_url: str
    token: str = field(repr=False)
    timeout_seconds: float = 10

    def __post_init__(self):
        origin = urlsplit(self.base_url)
        origin.port  # Validate a configured port before any request is possible.
        _required_text(self.token, 4096)
        if (
            origin.scheme not in {"https", "http"}
            or not origin.hostname
            or origin.username
            or origin.password
            or origin.path not in {"", "/"}
            or origin.query
            or origin.fragment
            or not self.token
            or not self.token.isascii()
            or any(character.isspace() for character in self.base_url)
            or not 0 < self.timeout_seconds <= 10
        ):
            raise ValueError("registry_ptg_scope_authority_unconfigured")

    def _request(self, envelope, *, timeout_seconds=None):
        encoded = _canonical(envelope).encode("utf-8")
        if len(encoded) > 131072:
            raise ValueError("registry_ptg_scope_request_invalid")
        request = Request(
            self.base_url.rstrip("/") + "/internal/v1/registry/ptg-source-scopes/authorize",
            data=encoded,
            headers={
                "Authorization": "Bearer " + self.token,
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
            method="POST",
        )
        timeout = self.timeout_seconds if timeout_seconds is None else min(self.timeout_seconds, timeout_seconds)
        if type(timeout) not in {int, float} or not math.isfinite(timeout) or timeout <= 0:
            raise TimeoutError
        with build_opener(ProxyHandler({}), _NoRedirects()).open(request, timeout=timeout) as response:
            raw = response.read(4097)
            if response.status != 200 or len(raw) > 4096:
                raise PermissionError("registry_ptg_scope_authority_unavailable")
        return json.loads(raw, object_pairs_hook=_unique_object)

    async def authorize(self, envelope, *, deadline=None):
        """Fetch fresh live authority and bind its actor and entire command."""
        if deadline is None:
            receipt = await asyncio.to_thread(self._request, envelope)
        else:
            remaining = _remaining(deadline)
            async with asyncio.timeout_at(deadline):
                receipt = await asyncio.to_thread(self._request, envelope, timeout_seconds=remaining)
            _remaining(deadline)
        _object(receipt, {"authorized", "actor", "command_sha256", "policy_revision"})
        if (
            receipt["authorized"] is not True
            or receipt["actor"] != envelope["actor"]
            or receipt["command_sha256"] != _digest(envelope["command"])
            or type(receipt["policy_revision"]) is not int
            or not 0 <= receipt["policy_revision"] < 2**63
        ):
            raise PermissionError("registry_ptg_scope_authority_changed")
        return receipt


@dataclass(frozen=True)
class RegistryPTGScopeSourceSpecification:
    capture_id: str
    ptg_schema_name: str
    snapshot_id: str
    binding_source_key: str
    company_key: str
    cohort_id: str


def _resolved_command(specification, frozen, schema_name, intent, ownership, actor):
    coordinates = RegistryNetworkSourceCoordinates(
        "ptg",
        frozen.source_key,
        schema_name,
        frozen.snapshot_id,
        PTG_RESULT_ARCHIVE_SOURCE_AUTHORITY_CONTRACT,
        frozen.snapshot_manifest_sha256,
    )
    full_by_field = {
        **intent,
        "operation": "approve_ptg_source_scope",
        "coordinates": asdict(coordinates),
        "ownership": ownership,
        "source": {
            "binding_source_key": frozen.source_key,
            "snapshot_id": frozen.snapshot_id,
            "ptg_schema_name": schema_name,
        },
    }
    command = RegistryPTGOperatorReviewCommand(
        scope_id=_uuid(intent["scope_id"]),
        coordinates=coordinates,
        client_id=intent["client_id"],
        legal_company_id=_uuid(intent["legal_company_id"]),
        approved_revision=intent["approved_revision"],
        source_file_import_id=intent["source_file_import_id"],
        file_versions=tuple(RegistryPTGProducerFileVersion(**version) for version in intent["file_versions"]),
        reason=intent["reason"],
        idempotency_key=intent["idempotency_key"],
        statement_id=_uuid(intent["statement_id"]),
        ownership=RegistryPTGSourceOwnershipWitness(**ownership),
    )
    return full_by_field, _command_document(command, specification, actor)


async def _resolved_source(session, schema_name, intent, ownership, actor):
    if intent.get("review_type") == "published_complete_snapshot_plan":
        from process.registry_ptg_published_plan_scope import resolve_published_plan

        return await resolve_published_plan(session, schema_name, intent, ownership, actor)
    specification = RegistryPTGScopeSourceSpecification(
        intent["scope_id"],
        schema_name,
        ownership["snapshot_id"],
        ownership["source_key"],
        intent["company_key"],
        intent["cohort_id"],
    )
    frozen = await prepare_ptg_result_archive_source_authority(
        session,
        schema_name=schema_name,
        operation_id=_source_specification(specification),
        snapshot_id=specification.snapshot_id,
    )
    authority = frozen.as_dict()
    if authority["contract"] == PTG_PUBLISHED_RESULT_SOURCE_AUTHORITY_CONTRACT:
        # shortcut: published results lack controller-import ownership; require a separate typed producer review.
        raise RegistryPTGProducerScopeError("registry_ptg_published_scope_unavailable")
    if (
        authority["contract"] != PTG_RESULT_ARCHIVE_SOURCE_AUTHORITY_CONTRACT
        or frozen.source_file_import_id != ownership["source_file_import_id"]
        or frozen.source_key != ownership["source_key"]
    ):
        raise ValueError("registry_ptg_scope_source_changed")
    full, document = _resolved_command(specification, frozen, schema_name, intent, ownership, actor)
    source_by_field, assignments, _ = await _resolved_source_state(session, specification)
    if source_by_field["import_run_id"] != ownership["engine_run_id"]:
        raise ValueError("registry_ptg_scope_source_changed")
    graph_by_field = {name: source_by_field[name] for name in _GRAPH_FIELDS if name != "source_assignments_sha256"}
    graph_by_field["source_assignments_sha256"] = _digest(assignments)
    evidence = await _evidence(session, specification, document, authority, graph_by_field)
    return full, document, evidence


async def _approved_company(session, table, document):
    await _require_current_approved_company(session, table, document)


@dataclass(frozen=True)
class RegistryPTGScopeEngineService:
    """Dedicated prebound source and approval sessions; no ambient HTTP DB pool."""

    schema_name: str
    reader_sessions: object = field(repr=False)
    approval_sessions: object = field(repr=False)
    store: RegistryPTGProducerScopeStore
    authority: RegistryPTGScopeAuthorityClient
    office_custody: object = field(default=None, repr=False)

    def __post_init__(self):
        _identifier(self.schema_name)
        if (
            not callable(self.reader_sessions)
            or not callable(self.approval_sessions)
            or type(self.store) is not RegistryPTGProducerScopeStore
            or type(self.authority) is not RegistryPTGScopeAuthorityClient
        ):
            raise ValueError("registry_ptg_scope_service_unconfigured")

        if self.office_custody is not None:
            from process.registry_ptg_office_custody import RegistryPTGOfficeCustodyProfile

            if type(self.office_custody) is not RegistryPTGOfficeCustodyProfile:
                raise ValueError("registry_ptg_office_profile_invalid")

    async def preview(self, envelope, *, deadline=None):
        """Resolve source facts with the SELECT-only scope-store reader role."""
        envelope = validated_registry_ptg_scope_envelope(envelope, "preview")
        deadline = _deadline(deadline)
        try:
            async with asyncio.timeout_at(deadline):
                _remaining(deadline)
                async with self.reader_sessions() as session, session.begin():
                    await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
                    _remaining(deadline)
                    await _protected_store(session, self.store, write=False)
                    actor = RegistryActor(**{**envelope["actor"], "user_id": _uuid(envelope["actor"]["user_id"])})
                    _remaining(deadline)
                    full, _, _ = await _resolved_source(
                        session, self.schema_name, envelope["command"], envelope["ownership"], actor
                    )
                    _remaining(deadline)
                _remaining(deadline)
                return {"command": full}
        except TimeoutError:
            raise RegistryPTGScopeDeadlineExpired() from None

    async def choices(self, envelope, *, deadline=None):
        """Read available retained choices; neither authorize nor append approvals."""
        from process.registry_ptg_source_choices import read_registry_ptg_source_choices

        envelope = validated_registry_ptg_choices_envelope(envelope)
        deadline = _deadline(deadline)
        try:
            async with asyncio.timeout_at(deadline):
                _remaining(deadline)
                async with self.reader_sessions() as session, session.begin():
                    await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
                    result = await read_registry_ptg_source_choices(
                        session, self.schema_name, envelope["ownership"], envelope["selection"], self.store
                    )
                    _remaining(deadline)
                return result
        except TimeoutError:
            raise RegistryPTGScopeDeadlineExpired() from None

    async def approve(self, envelope, *, deadline=None):
        """Recompute, freshly authorize and append in one protected transaction."""
        envelope = validated_registry_ptg_scope_envelope(envelope, "approve")
        deadline = _deadline(deadline)
        state_by_field = {"commit_started": False}
        try:
            async with asyncio.timeout_at(deadline):
                return await self._approve(envelope, deadline, state_by_field)
        except TimeoutError:
            raise RegistryPTGScopeDeadlineExpired(outcome_unknown=state_by_field["commit_started"]) from None
        except Exception:
            if state_by_field["commit_started"]:
                raise RegistryPTGScopeDeadlineExpired(outcome_unknown=True) from None
            raise

    async def _approve(self, envelope, deadline, state_by_field):
        if envelope["command"].get("review_type") == "published_complete_snapshot_plan":
            from process.registry_ptg_published_plan_scope import approve_published_plan

            return await approve_published_plan(self, envelope, deadline, state_by_field)
        _remaining(deadline)
        async with registry_company_approval_transaction(
            self.approval_sessions, control_schema=self.store.control_schema or registry_schema()
        ) as session:
            await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
            _remaining(deadline)
            table = await _protected_store(session, self.store, write=True)
            actor = RegistryActor(**{**envelope["actor"], "user_id": _uuid(envelope["actor"]["user_id"])})
            intent_by_field = {name: envelope["command"][name] for name in INTENT_FIELDS}
            _remaining(deadline)
            full, document, evidence = await _resolved_source(
                session, self.schema_name, intent_by_field, envelope["command"]["ownership"], actor
            )
            if full != envelope["command"]:
                raise ValueError("registry_ptg_scope_source_changed")
            approval = _approval_document(document, evidence)
            _remaining(deadline)
            await _approved_company(session, table, approval)
            _remaining(deadline)
            await self.authority.authorize(envelope, deadline=deadline)
            _remaining(deadline)
            retained = await _retain_approval(session, table, approval)
            receipt_by_field = {
                "scope_id": full["scope_id"],
                "statement_id": full["statement_id"],
                "client_id": full["client_id"],
                "command_sha256": _digest(full),
                "approval_sha256": _digest(retained),
                "producer_statement_sha256": retained["producer_statement_sha256"],
            }
            _remaining(deadline)
            state_by_field["commit_started"] = True
        _remaining(deadline)
        return receipt_by_field

    async def approve_offices(self, envelope, *, deadline=None):
        """Verify complete physical offices and fresh actor authority before append."""
        from process.registry_ptg_office_approval import approve_offices, validated_office_envelope

        envelope = validated_office_envelope(envelope)
        deadline = _deadline(deadline)
        state_by_field = {"commit_started": False}
        try:
            async with asyncio.timeout_at(deadline):
                return await approve_offices(self, envelope, deadline, state_by_field)
        except TimeoutError:
            raise RegistryPTGScopeDeadlineExpired(outcome_unknown=state_by_field["commit_started"]) from None
        except Exception:
            if state_by_field["commit_started"]:
                raise RegistryPTGScopeDeadlineExpired(outcome_unknown=True) from None
            raise
