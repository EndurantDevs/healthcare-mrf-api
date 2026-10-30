# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fixed server capabilities for observing and refreshing approved memberships."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
from contextlib import asynccontextmanager
from dataclasses import dataclass, field, replace
from pathlib import PurePosixPath
from uuid import UUID, uuid4, uuid5

from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import ArgumentError

from process import network_address_artifact as address
from process.network_address_projection import PinnedAddressSource, _identifier
from process.network_serving_read import resolve_network_serving_manifest
from process.registry_aca_source_notifications import RegistryACASourceNotificationStore
from process.registry_candidate_composition import RegistryCompositionAddressSources
from process.registry_cms_source_notifications import RegistryCMSSourceNotificationReader
from process.registry_company_approval_fence import registry_company_approval_transaction
from process.registry_membership_refresh_authority import RegistryMembershipRefreshAuthorityClient
from process.registry_membership_refresh_contract import (
    SERVICE_ACTOR,
    RegistryMembershipRefreshUnavailable,
    frozen_membership_refresh_envelope,
    membership_refresh_command_sha256,
)
from process.registry_membership_refresh_execution import RegistryMembershipRefreshRuntime
from process.registry_membership_refresh_queue import (
    RegistryMembershipRefreshAddressProfile,
    validated_membership_refresh_request_status,
)
from process.registry_ptg_graph_reader import RegistryPTGGraphReadBudget
from process.registry_ptg_producer_scope import RegistryPTGProducerScopeStore
from process.registry_publication_authority import _header, _origin, _reject_number, _unique_object
from process.registry_publication_ptg_runtime import RegistryPublicationSQLLimits
from process.registry_publication_worker import _worker_sessions
from process.registry_refresh_anchor_contract import (
    _counter,
    _hash,
    _identity,
    _object,
    encoded_document,
    validated_scope,
)
from process.registry_refresh_anchor_service import RegistryRefreshAnchorService
from process.registry_source_observation_store import _namespace

CONFIG_ENV = "HLTHPRT_REGISTRY_MEMBERSHIP_REFRESH_RUNTIME"
_MAX_CONFIG_BYTES = 32768
_ENROLLMENT_PATH = "/internal/v1/registry/membership-refresh/enrollments"
_REQUIRED = {
    "enabled",
    "app_origin",
    "service_token",
    "control_schema",
    "fence_dsn",
    "source_reader_dsn",
    "address_reader_dsn",
    "address_schema",
    "runtime_roles",
    "source_profiles",
}
_BUDGET_FIELDS = ("maximum_bytes", "maximum_pages", "maximum_coordinates")
_BUDGET_MAXIMUM = (16777216, 16384, 65536)


def _unavailable():
    return RegistryMembershipRefreshUnavailable("registry_membership_refresh_runtime_unavailable")


def _roles(configured):
    if type(configured) is not list:
        raise _unavailable()
    return address._roles(tuple(configured))


def _native_url(encoded):
    if (
        type(encoded) is not str
        or not 0 < len(encoded.encode()) <= 4096
        or any(char.isspace() or not char.isprintable() for char in encoded)
    ):
        raise _unavailable()
    url = make_url(encoded)
    if (
        url.drivername not in {"postgresql", "postgresql+asyncpg"}
        or not url.username
        or not url.database
        or set(url.query) - {"ssl", "host", "port"}
    ):
        raise _unavailable()
    _identifier(url.username)
    if "ssl" in url.query and url.query["ssl"] not in {"require", "verify-full"}:
        raise _unavailable()
    if "host" in url.query:
        socket = url.query["host"]
        if (
            url.host
            or url.port
            or type(socket) is not str
            or not socket.startswith("/")
            or str(PurePosixPath(socket)) != socket
            or ".." in PurePosixPath(socket).parts
        ):
            raise _unavailable()
        port = url.query.get("port", "5432")
        if type(port) is not str or not port.isdecimal() or not 0 < int(port) <= 65535:
            raise _unavailable()
    elif not url.host or "port" in url.query or url.port is not None and not 0 < url.port <= 65535:
        raise _unavailable()
    return url.set(drivername="postgresql+asyncpg")


def _target(url):
    return (
        url.query.get("host") or url.host.lower(),
        int(url.query.get("port", url.port or 5432)),
        url.database,
        url.query.get("ssl"),
    )


def _profiles(configured, reader_role):
    if type(configured) is not dict or set(configured) - {"cms", "aca", "ptg", "generic"}:
        raise _unavailable()
    profiles_by_family = {}
    for family, profile in configured.items():
        if family == "generic":
            _object(profile, {"head_fence_dsn"})
            _native_url(profile["head_fence_dsn"])
            continue
        if family == "cms":
            _object(profile, {"control_schema", "owner_role", "publisher_role", "reader_roles"})
            reader = RegistryCMSSourceNotificationReader(**{**profile, "reader_roles": _roles(profile["reader_roles"])})
            if reader_role not in reader.reader_roles:
                raise _unavailable()
        elif family == "aca":
            required_fields = {"control_schema", "publisher_role", "reader_roles", "source_schema"}
            if type(profile) is not dict or set(profile) not in (required_fields, required_fields | {"head_fence_dsn"}):
                raise _unavailable()
            _identifier(profile["source_schema"])
            reader = RegistryACASourceNotificationStore(
                **{name: profile[name] for name in ("control_schema", "publisher_role")},
                reader_roles=_roles(profile["reader_roles"]),
            )
            if reader_role not in reader.reader_roles:
                raise _unavailable()
        else:
            _object(profile, {"control_schema", "owner_role", "approval_role"})
            for role in profile.values():
                _identifier(role)
            reader = RegistryPTGProducerScopeStore(**profile)
        profiles_by_family[family] = reader
    return profiles_by_family


@dataclass(frozen=True, repr=False)
class _Settings:
    app_origin: str
    service_token: str
    control_schema: str
    fence_url: object
    source_reader_url: object
    address_reader_url: object
    address_profile: RegistryMembershipRefreshAddressProfile
    source_profiles: dict
    read_budget: tuple
    sql_limits: RegistryPublicationSQLLimits | None
    aca_source_schema: str
    aca_head_fence_url: object | None = None
    generic_head_fence_url: object | None = None


def _settings(configured):
    """Validate fixed server configuration before creating native capabilities."""
    try:
        if type(configured) is str:
            if not 0 < len(configured.encode()) <= _MAX_CONFIG_BYTES:
                raise _unavailable()
            configured = json.loads(configured, object_pairs_hook=_unique_object, parse_constant=_reject_number)
        if type(configured) is not dict or configured.get("enabled") is not True:
            raise _unavailable()
        if not _REQUIRED <= set(configured) or set(configured) - _REQUIRED - {"sql_limits", "read_budget"}:
            raise _unavailable()
        if len(encoded_document(configured)) > _MAX_CONFIG_BYTES:
            raise _unavailable()
        _identifier(configured["control_schema"])
        urls = tuple(_native_url(configured[name]) for name in ("fence_dsn", "source_reader_dsn", "address_reader_dsn"))
        aca = configured["source_profiles"].get("aca", {}) if type(configured["source_profiles"]) is dict else {}
        head_url = _native_url(aca["head_fence_dsn"]) if type(aca) is dict and "head_fence_dsn" in aca else None
        generic = (
            configured["source_profiles"].get("generic", {}) if type(configured["source_profiles"]) is dict else {}
        )
        generic_url = (
            _native_url(generic["head_fence_dsn"]) if type(generic) is dict and "head_fence_dsn" in generic else None
        )
        for selected_head_url in (head_url, generic_url):
            if selected_head_url is not None and _target(selected_head_url) != _target(urls[0]):
                raise _unavailable()
        if generic_url is not None and generic_url.username in {url.username for url in urls}:
            raise _unavailable()
        if len({_target(url) for url in urls}) != 1:
            raise _unavailable()
        profile = RegistryMembershipRefreshAddressProfile(
            configured["address_schema"], _roles(configured["runtime_roles"])
        )
        token = configured["service_token"]
        if type(token) is not str or not 0 < len(token) <= 4096 or re.fullmatch(r"[A-Za-z0-9._~+/-]+=*", token) is None:
            raise _unavailable()
        budget = configured.get("read_budget", dict(zip(_BUDGET_FIELDS, _BUDGET_MAXIMUM, strict=True)))
        _object(budget, set(_BUDGET_FIELDS))
        limits = tuple(budget[name] for name in _BUDGET_FIELDS)
        if any(
            type(limit) is not int or not 0 < limit <= cap for limit, cap in zip(limits, _BUDGET_MAXIMUM, strict=True)
        ):
            raise _unavailable()
        sql_limits = RegistryPublicationSQLLimits(**configured["sql_limits"]) if "sql_limits" in configured else None
        return _Settings(
            _origin(configured["app_origin"]),
            token,
            configured["control_schema"],
            *urls,
            profile,
            _profiles(configured["source_profiles"], urls[1].username),
            limits,
            sql_limits,
            configured["source_profiles"].get("aca", {}).get("source_schema", "mrf"),
            head_url,
            generic_url,
        )
    except ValueError, TypeError, KeyError, AttributeError, UnicodeError, RecursionError, ArgumentError:
        raise _unavailable() from None


async def _enrollment_page(authority, after):
    query = "?limit=100" + ("&after_binding_id=" + after if after is not None else "")
    outgoing = authority._client.build_request(
        "GET", authority._origin + _ENROLLMENT_PATH + query, headers=authority._headers
    )
    outgoing.headers.pop("cookie", None)
    try:
        async with asyncio.timeout(15):
            reply = await authority._client.send(outgoing, stream=True, follow_redirects=False)
            try:
                return await _page_body(reply)
            finally:
                await reply.aclose()
    finally:
        authority._client.cookies.clear()


async def _page_body(reply):
    if (
        reply.status_code != 200
        or _header(reply, "content-type") not in {"application/json", "application/json; charset=utf-8"}
        or _header(reply, "cache-control") not in {"no-store", "private, no-store"}
        or _header(reply, "content-encoding") not in {None, "identity"}
        or reply.headers.get_list("set-cookie")
    ):
        raise _unavailable()
    declared, transfer = _header(reply, "content-length"), _header(reply, "transfer-encoding")
    if (
        transfer not in {None, "chunked"}
        or declared is not None
        and (transfer is not None or not declared.isdecimal() or len(declared) > 7 or not 0 < int(declared) <= 1048576)
    ):
        raise _unavailable()
    encoded = bytearray()
    async for chunk in reply.aiter_raw(chunk_size=1048577):
        if len(encoded) + len(chunk) > 1048576:
            raise _unavailable()
        encoded.extend(chunk)
    if not encoded or declared is not None and len(encoded) != int(declared):
        raise _unavailable()
    return json.loads(encoded.decode(), object_pairs_hook=_unique_object, parse_constant=_reject_number)


async def _enrollments(authority):
    scopes, policy_revision, after = [], None, None
    while True:
        page = await _enrollment_page(authority, after)
        _object(page, {"policy_revision", "enrolled_scopes", "next_binding_id"})
        _counter(page["policy_revision"])
        if policy_revision is not None and policy_revision != page["policy_revision"]:
            raise _unavailable()
        policy_revision = page["policy_revision"]
        selected = page["enrolled_scopes"]
        if type(selected) is not list or len(selected) > 100:
            raise _unavailable()
        identities = [validated_scope(scope)["binding_id"] for scope in selected]
        if (
            identities != sorted(set(identities))
            or after is not None
            and any(identity <= after for identity in identities)
        ):
            raise _unavailable()
        scopes.extend(selected)
        if len(scopes) > 1000:
            raise _unavailable()
        encoded_document(scopes)
        following = page["next_binding_id"]
        if following is None:
            return scopes
        _identity(following)
        if len(selected) != 100 or following != identities[-1] or len(scopes) >= 1000:
            raise _unavailable()
        after = following


async def _driver(session):
    from process.network_cms_registry_address_capture import native_driver

    return await native_driver(session)


async def _native_identity(driver, schema):
    return tuple(
        await driver.fetchrow(
            "SELECT current_database(),oid::bigint,pg_postmaster_start_time(),inet_server_addr()::text,"
            "current_setting('port'),to_regnamespace($1)::oid::bigint FROM pg_database WHERE datname=current_database()",
            schema,
        )
    )


async def _qualify_native_actor(driver, expected_login):
    """Qualify the genuine fixed login without assuming a role or granting privileges."""
    safe = await driver.fetchval(
        "SELECT current_user=$1 AND session_user=$1 AND current_setting('role')='none' "
        "AND NOT (r.rolsuper OR r.rolcreatedb OR r.rolcreaterole OR r.rolreplication OR r.rolbypassrls) "
        "AND NOT EXISTS(SELECT FROM pg_roles elevated WHERE "
        "(elevated.rolsuper OR elevated.rolcreatedb OR elevated.rolcreaterole OR elevated.rolreplication OR elevated.rolbypassrls) "
        "AND (pg_has_role(r.oid,elevated.oid,'MEMBER') OR pg_has_role(r.oid,elevated.oid,'SET'))) "
        "FROM pg_roles r WHERE r.rolname=current_user",
        expected_login,
    )
    if safe is not True:
        raise _unavailable()


async def _qualify_reader(sessions, url, schema):
    async with sessions() as session, session.begin():
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
        driver = await _driver(session)
        await _qualify_native_actor(driver, url.username)
        identity = await _native_identity(driver, schema)
        if identity[-1] is None:
            raise _unavailable()
        return identity


@dataclass(frozen=True)
class _Resources:
    settings: _Settings = field(repr=False)
    fence_sessions: object = field(repr=False)
    source_sessions: object = field(repr=False)
    address_sessions: object = field(repr=False)
    identity: tuple = field(repr=False)
    readers: object = field(repr=False)

    @asynccontextmanager
    async def address(self):
        """Open a fresh independent RO snapshot only after the caller owns source fences."""
        async with self.address_sessions() as session, session.begin():
            await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
            driver = await _driver(session)
            if await _native_identity(driver, self.settings.control_schema) != self.identity:
                raise _unavailable()
            authority = await address.generation.read_entity_address_result_generation_authority(
                address._CatalogSession(driver), schema_name=self.settings.address_profile.schema_name
            )
            generation = address._digest(address._authority_document(authority))
            pin = PinnedAddressSource(self.settings.address_profile.schema_name, "entity_address_unified", generation)
            descriptor = await address.capture_network_address_artifact(
                driver, pin, runtime_roles=self.settings.address_profile.runtime_roles
            )
            yield driver, pin, descriptor


def _source_body(accepted_sources):
    from process.registry_membership_refresh_contract import accepted_membership_source_sha256

    accepted = json.loads(encoded_document(list(accepted_sources)))
    if accepted:
        accepted_membership_source_sha256(accepted)
    return accepted


def _semantic_digest(accepted, revision, generation, artifact, scopes):
    """Match the fixed IC v2 recipe; publication head and request identity never coalesce work."""
    document_by_field = {
        "sources": accepted,
        "approved_revision": revision,
        "address_generation": generation,
        "address_artifact_sha256": artifact,
        "enrollment_sha256": hashlib.sha256(encoded_document(scopes)).hexdigest(),
    }
    return hashlib.sha256(b"registry-membership-followup-v2:" + encoded_document(document_by_field)).hexdigest()


async def _heads(driver, schema):
    native = await driver.fetchrow(
        f"SELECT approved.approved_revision,coalesce(control.generation_id,0) AS generation_id,"
        "manifest.manifest_sha256,manifest.eligible "
        f"FROM {_namespace(schema)}.registry_revision_control approved CROSS JOIN {_namespace(schema)}.network_serving_control control "
        f"LEFT JOIN {_namespace(schema)}.network_serving_manifest manifest ON manifest.generation_id=control.generation_id "
        "WHERE approved.id=1 AND control.id=1"
    )
    if native is None or native["generation_id"] and native["eligible"] is not True:
        raise _unavailable()
    _counter(native["approved_revision"])
    _counter(native["generation_id"])
    if native["generation_id"]:
        _hash(native["manifest_sha256"])
    elif native["manifest_sha256"] is not None:
        raise _unavailable()
    return native


def _actor_key():
    from process.registry_publication_queue import _request_metadata

    return _request_metadata({}, SERVICE_ACTOR, None, "")["actor_key"]


async def _recovery(driver, schema, digest):
    native = await driver.fetchrow(
        f"SELECT * FROM {_namespace(schema)}.registry_publication_request WHERE actor_key=$1 AND idempotency_key=$2",
        _actor_key(),
        "registry-membership-refresh:" + digest,
    )
    if native is None:
        return None
    command = validated_membership_refresh_request_status(native)
    return native, command


def _decoded(document):
    return json.loads(document) if type(document) is str else document


async def _published_request(driver, schema, manifest):
    """Associate one native sealed generation with its complete typed completed service request."""
    if manifest is None:
        return None
    native = await driver.fetch(
        f"SELECT * FROM {_namespace(schema)}.registry_publication_request WHERE state='completed' AND actor_key=$1 "
        "AND result_json->>'candidate_id'=$2 LIMIT 2",
        _actor_key(),
        manifest.candidate_id,
    )
    if not native:
        return None
    if len(native) != 1:
        raise _unavailable()
    request = native[0]
    command = validated_membership_refresh_request_status(request)
    receipt_by_field = _decoded(request["result_json"])
    candidate = await driver.fetchrow(
        f"SELECT dataset_id::text,schema_id::text,producer_id::text,approved_custom_revision,expected_head,"
        "source_generations->>'unified_address' AS address_generation,"
        "source_generations->>'network_address_origin' AS address_artifact_sha256 "
        f"FROM {_namespace(schema)}.network_membership_candidate WHERE candidate_id=$1",
        UUID(manifest.candidate_id),
    )
    if candidate is None or any(
        candidate[name + "_id"] != str(uuid5(request["request_id"], name)) for name in ("dataset", "schema", "producer")
    ):
        raise _unavailable()
    if (
        receipt_by_field["generation_id"] != manifest.generation_id
        or receipt_by_field["manifest_sha256"] != manifest.manifest_sha256
        or receipt_by_field["approved_revision"] != manifest.approved_custom_revision
        or candidate["approved_custom_revision"] != receipt_by_field["approved_revision"]
        or candidate["expected_head"] != command["expected_serving_generation"]
        or candidate["address_artifact_sha256"] != command["address_artifact_sha256"]
        or manifest.source_generations.get("network_address_origin") != command["address_artifact_sha256"]
        or manifest.source_generations.get("unified_address") != candidate["address_generation"]
    ):
        raise _unavailable()
    generation = candidate["address_generation"]
    if type(generation) is not str or not 0 < len(generation.encode()) <= 256:
        raise _unavailable()
    digest = _semantic_digest(
        command["accepted_sources"],
        command["expected_approved_revision"],
        generation,
        command["address_artifact_sha256"],
        command["enrolled_scopes"],
    )
    return digest, command


async def _manifest(driver, schema, head):
    if not head["generation_id"]:
        return None
    manifest = await resolve_network_serving_manifest(
        driver, generation_id=head["generation_id"], control_schema=schema
    )
    if manifest.manifest_sha256 != head["manifest_sha256"]:
        raise _unavailable()
    return manifest


async def _approved(driver, schema, revision):
    from process.network_approved_membership_source import pin_approved_membership_source

    return await pin_approved_membership_source(driver, approved_revision=revision, control_schema=schema)


class _SourceResolver:
    """Bind full native source fences to one fixed address profile and execution role."""

    def __init__(self, resources, execution_role=None):
        self.resources, self.execution_role = resources, execution_role

    @asynccontextmanager
    async def pin_sources(self, connection, envelope, source_manifest):
        """Hold company/source fences through the parent's final publication transaction."""
        from process.registry_membership_refresh_sources import pin_registry_membership_refresh_sources

        envelope = frozen_membership_refresh_envelope(envelope)
        resources = self.resources
        if await _native_identity(connection, resources.settings.control_schema) != resources.identity:
            raise _unavailable()
        if self.execution_role is not None and await connection.fetchval("SELECT current_user") != self.execution_role:
            raise _unavailable()
        command = envelope["command"]
        async with registry_company_approval_transaction(
            resources.fence_sessions, control_schema=resources.settings.control_schema
        ) as session:
            async with pin_registry_membership_refresh_sources(
                session,
                command["enrolled_scopes"],
                approved_revision=command["expected_approved_revision"],
                readers=resources.readers,
                source_manifest=source_manifest,
            ) as selected:
                if _source_body(selected.accepted_sources) != command["accepted_sources"]:
                    raise _unavailable()
                async with resources.address() as (_driver, pin, descriptor):
                    if descriptor["descriptor_sha256"] != command["address_artifact_sha256"]:
                        raise _unavailable()
                    yield self._composition(pin, selected.recipes, source_manifest)

    async def resolve_sources(self, connection, envelope, source_manifest):
        """Preparation also rederives complete native source pins; a stored receipt grants nothing."""
        async with self.pin_sources(connection, envelope, source_manifest) as sources:
            return sources

    def _composition(self, address_pin, recipes, source_manifest):
        from process.registry_ptg_graph_reader import RegistryPTGGraphReadBudget

        arguments_by_phase = {
            "initial_source_recipes" if source_manifest is None else "replacement_source_recipes": recipes
        }
        return RegistryCompositionAddressSources(
            address_pin,
            office_scope_store=self.resources.readers.ptg_store,
            office_read_budget=RegistryPTGGraphReadBudget(*self.resources.settings.read_budget),
            compact=True,
            **arguments_by_phase,
        )


@dataclass(repr=False)
class RegistryMembershipRefreshInputsService:
    """Scheduling observations retain one exact readiness hint, never rows or permission."""

    resources: _Resources
    authorize: RegistryMembershipRefreshAuthorityClient
    _readiness_key: str | None = field(default=None, init=False, repr=False)
    _is_ready: bool = field(default=False, init=False, repr=False)

    async def __call__(self):
        """Return one complete bounded native observation; failures produce no partial vector."""
        try:
            async with asyncio.timeout(25):
                scopes = await _enrollments(self.authorize)
                async with registry_company_approval_transaction(
                    self.resources.fence_sessions, control_schema=self.resources.settings.control_schema
                ) as session:
                    return await self._observe(session, scopes)
        except asyncio.CancelledError, PermissionError:
            self._readiness_key = None
            raise
        except Exception:
            self._readiness_key = None
            raise _unavailable() from None

    async def _selected_sources(self, session, scopes, approved, manifest):
        from process.registry_membership_refresh_sources import observe_registry_membership_refresh_sources

        if not scopes:
            return None
        return await observe_registry_membership_refresh_sources(
            session,
            scopes,
            approved_revision=approved.approved_revision,
            readers=self.resources.readers,
            source_manifest=manifest,
        )

    async def _observe(self, session, scopes):
        schema = self.resources.settings.control_schema
        driver = await _driver(session)
        head = await _heads(driver, schema)
        approved = await _approved(driver, schema, head["approved_revision"])
        manifest = await _manifest(driver, schema, head)
        selected = await self._selected_sources(session, scopes, approved, manifest)
        accepted = [] if selected is None else _source_body(selected.accepted_sources)
        async with self.resources.address() as (_address_driver, pin, descriptor):
            native_address = pin, descriptor
        ready = None
        if selected is None:
            self._readiness_key = None
        if selected is not None:
            native_address, ready = await self._readiness(session, approved, selected, scopes, manifest, native_address)
        pin, descriptor = native_address
        digest = _semantic_digest(
            accepted, approved.approved_revision, pin.generation_id, descriptor["descriptor_sha256"], scopes
        )
        published = await _published_request(driver, schema, manifest)
        recovery = await _recovery(driver, schema, digest)
        document_by_field = {
            "accepted_sources": accepted,
            "approved_revision": approved.approved_revision,
            "address_generation": pin.generation_id,
            "address_artifact_sha256": descriptor["descriptor_sha256"],
            "ready_address_generation": ready,
            "expected_serving_generation": head["generation_id"],
            "expected_serving_manifest_sha256": head["manifest_sha256"],
            "enrolled_scopes": scopes,
            "published_inputs_digest": None if published is None else published[0],
            "refresh_request": None,
        }
        if recovery is not None:
            request, command = recovery
            recovered_digest = _semantic_digest(
                command["accepted_sources"],
                command["expected_approved_revision"],
                pin.generation_id,
                command["address_artifact_sha256"],
                command["enrolled_scopes"],
            )
            if recovered_digest != digest:
                raise _unavailable()
            document_by_field.update(
                expected_serving_generation=command["expected_serving_generation"],
                expected_serving_manifest_sha256=command["expected_serving_manifest_sha256"],
                refresh_request={
                    "request_id": str(request["request_id"]),
                    "state": request["state"],
                    "command_sha256": membership_refresh_command_sha256(command),
                    "inputs_digest": digest,
                },
            )
        encoded_document(document_by_field)
        return document_by_field

    def _key(self, approved, selected, scopes, descriptor):
        return hashlib.sha256(
            encoded_document(
                {
                    "approved_fingerprint": approved.generation_id,
                    "accepted_sources": _source_body(selected.accepted_sources),
                    "address_artifact_sha256": descriptor["descriptor_sha256"],
                    "enrollment_sha256": hashlib.sha256(encoded_document(scopes)).hexdigest(),
                }
            )
        ).hexdigest()

    async def _readiness(self, session, approved, selected, scopes, manifest, native_address):
        from process.registry_membership_refresh_sources import (
            _recipe_commitment,
            pin_registry_membership_refresh_sources,
        )

        pin, descriptor = native_address
        if self._readiness_key == self._key(approved, selected, scopes, descriptor):
            return native_address, pin.generation_id if self._is_ready else None
        self._readiness_key = None
        self._is_ready = False
        async with pin_registry_membership_refresh_sources(
            session,
            scopes,
            approved_revision=approved.approved_revision,
            readers=self.resources.readers,
            source_manifest=manifest,
        ) as full:
            if _source_body(full.accepted_sources) != _source_body(selected.accepted_sources) or _recipe_commitment(
                full.recipes
            ) != _recipe_commitment(selected.recipes):
                raise _unavailable()
            async with self.resources.address() as (_address_driver, pin, descriptor):
                native_address = pin, descriptor
                if manifest is not None and (
                    "registry_source_contributions" in manifest.source_generations
                    or _uses_witness_office_policy(approved, full.recipes)
                ):
                    is_ready = await self._has_office_correspondence(session, approved, full, pin, manifest)
                else:
                    is_ready = await self._has_office_correspondence(session, approved, full, pin)
                if type(is_ready) is not bool:
                    raise _unavailable()
                self._readiness_key = self._key(approved, full, scopes, descriptor)
                self._is_ready = is_ready
                return native_address, pin.generation_id if is_ready else None
        return native_address, None

    async def _has_office_correspondence(self, session, approved, selected, pin, manifest=None):
        from process.registry_initial_source_composition import OfficeSummaryRequest, prepare_source_offices
        from process.registry_source_office_errors import SourceOfficeNotReady

        if _uses_witness_office_policy(approved, selected.recipes):
            return await self._has_witness_office_correspondence(session, approved, selected, pin, manifest)
        try:
            async with session.begin_nested():
                await prepare_source_offices(
                    await _driver(session),
                    selected.recipes,
                    approved,
                    pin,
                    OfficeSummaryRequest(
                        self.resources.settings.address_profile.runtime_roles,
                        self.resources.settings.control_schema,
                        (
                            session,
                            self.resources.readers.ptg_store,
                            RegistryPTGGraphReadBudget(*self.resources.settings.read_budget),
                        ),
                        source_manifest=manifest,
                        revision=4,
                        derivation_sink=lambda _derivation: None,
                    ),
                )
            return True
        except SourceOfficeNotReady as error:
            if type(error) is not SourceOfficeNotReady:
                raise
            return False

    async def _has_witness_office_correspondence(self, session, approved, selected, pin, manifest):
        """After full Source/address pins, check policy; the worker and custodian derive and validate rows."""
        from process.network_approved_source_bindings import (
            ApprovedNetworkSourceBindingError,
            require_approved_network_source_bindings,
        )
        from process.network_fhir_membership_source import FHIRMembershipSourceError, _reviewed_descriptor
        from process.registry_verified_witness_offices import _policy_context

        driver = await _driver(session)
        for recipe in selected.recipes:
            _reviewed_descriptor(recipe.source_pin, approved, recipe.binding_coordinates, True)
            try:
                await require_approved_network_source_bindings(
                    driver, approved, recipe.binding_coordinates, control_schema=self.resources.settings.control_schema
                )
            except ApprovedNetworkSourceBindingError as error:
                raise FHIRMembershipSourceError("Reviewed FHIR bindings are unavailable") from error
        await _policy_context(
            driver,
            pin,
            self.resources.settings.address_profile.runtime_roles,
            approved,
            self.resources.settings.control_schema,
        )
        return True


def _uses_witness_office_policy(approved, recipes):
    """Select exactly the same compact retained-FHIR branch as composition."""
    from process.network_approved_membership_source import ApprovedMembershipSource
    from process.registry_candidate_composition import _retained_fhir_only

    return type(approved) is ApprovedMembershipSource and _retained_fhir_only(recipes)


@dataclass(frozen=True)
class RegistryMembershipRefreshRuntimeBundle:
    runtime: RegistryMembershipRefreshRuntime
    inputs_service: RegistryMembershipRefreshInputsService
    anchors_service: RegistryRefreshAnchorService


async def _head_fence_sessions(settings, cleanup, url, identity):
    """Qualify each fixed metadata pool against the already authenticated database."""
    if url is None:
        return None
    sessions = _worker_sessions(settings, cleanup, url)
    if await _qualify_reader(sessions, url, settings.control_schema) != identity:
        raise _unavailable()
    return sessions


async def _build_native_bundle(settings, cleanup, execution_role=None):
    from process.registry_membership_refresh_source_fence import RegistryMembershipRefreshHeadFences
    from process.registry_membership_refresh_sources import RegistryMembershipRefreshSourceReaders

    settings = replace(settings, sql_limits=settings.sql_limits or RegistryPublicationSQLLimits())
    factories = tuple(
        _worker_sessions(settings, cleanup, url)
        for url in (settings.fence_url, settings.source_reader_url, settings.address_reader_url)
    )
    identities = [
        await _qualify_reader(sessions, url, settings.control_schema)
        for sessions, url in zip(
            factories, (settings.fence_url, settings.source_reader_url, settings.address_reader_url), strict=True
        )
    ]
    if len(set(identities)) != 1:
        raise _unavailable()
    head_sessions = await _head_fence_sessions(settings, cleanup, settings.aca_head_fence_url, identities[0])
    generic_head_sessions = await _head_fence_sessions(
        settings, cleanup, settings.generic_head_fence_url, identities[0]
    )
    profiles = settings.source_profiles
    readers = RegistryMembershipRefreshSourceReaders(
        settings.control_schema,
        factories[1],
        cms_reader=profiles.get("cms"),
        aca_store=profiles.get("aca"),
        ptg_store=profiles.get("ptg"),
        read_budget_limits=settings.read_budget,
        aca_source_schema=settings.aca_source_schema,
        head_fences=RegistryMembershipRefreshHeadFences(head_sessions, generic_head_sessions),
    )
    resources = _Resources(settings, *factories, identities[0], readers)
    authority = await cleanup.enter_async_context(
        RegistryMembershipRefreshAuthorityClient(settings.app_origin, settings.service_token, enabled=True)
    )
    resolver = _SourceResolver(resources, execution_role)
    runtime = RegistryMembershipRefreshRuntime(
        authority, settings.address_profile, resolver.resolve_sources, resolver.pin_sources
    )
    anchors = RegistryRefreshAnchorService(
        factories[0],
        settings.control_schema,
        factories[1],
        profiles.get("cms"),
        profiles.get("aca"),
        profiles.get("ptg"),
        settings.read_budget,
    )
    return RegistryMembershipRefreshRuntimeBundle(
        runtime, RegistryMembershipRefreshInputsService(resources, authority), anchors
    )


def _api_settings(configured):
    if configured is None:
        return None
    if type(configured) is str:
        if not 0 < len(configured.encode()) <= _MAX_CONFIG_BYTES:
            raise _unavailable()
        configured = json.loads(configured, object_pairs_hook=_unique_object, parse_constant=_reject_number)
    if type(configured) is dict and configured.get("enabled") is False:
        if configured != {"enabled": False}:
            _settings({**configured, "enabled": True})
        return None
    return _settings(configured)


async def _api_identity(schema):
    from db.models import db

    async with asyncio.timeout(5):
        rows = await db.all(
            text(
                "SELECT current_database(),oid::bigint,pg_postmaster_start_time(),inet_server_addr()::text,"
                "current_setting('port'),to_regnamespace(:schema)::oid::bigint FROM pg_database WHERE datname=current_database()"
            ),
            schema=schema,
        )
    if len(rows) != 1:
        raise _unavailable()
    return tuple(rows[0])


async def build_registry_membership_refresh_runtime(configured, cleanup):
    """Install only closed server configuration aligned with the actual ordinary API registry."""
    from db.registry_schema import registry_schema

    try:
        settings = _api_settings(configured)
        if settings is None:
            return None
        if settings.control_schema != registry_schema():
            raise _unavailable()
        bundle = await _build_native_bundle(settings, cleanup)
        if await _api_identity(settings.control_schema) != bundle.inputs_service.resources.identity:
            raise _unavailable()
        return bundle
    except asyncio.CancelledError:
        raise
    except Exception:
        raise _unavailable() from None


async def membership_refresh_worker_runtime(worker_config, cleanup):
    """Use the fixed environment profile, exact composition census and bounded worker limits."""
    from db.registry_schema import registry_schema
    from process.registry_publication_worker import RegistryPublicationWorkerConfig

    try:
        if type(worker_config) is not RegistryPublicationWorkerConfig:
            raise _unavailable()
        settings = _settings(os.environ.get(CONFIG_ENV))
        if (
            settings.control_schema != registry_schema()
            or settings.control_schema != worker_config.consumer.control_schema
        ):
            raise _unavailable()
        if _target(settings.fence_url) != _target(worker_config.execution_url):
            raise _unavailable()
        roles = tuple(sorted(set(worker_config.consumer.loader_roles + worker_config.consumer.reader_roles)))
        if roles != settings.address_profile.runtime_roles:
            raise _unavailable()
        limits = worker_config.sql_limits
        if settings.sql_limits is not None:
            for name in RegistryPublicationSQLLimits.__dataclass_fields__:
                if getattr(settings.sql_limits, name) > getattr(limits, name):
                    raise _unavailable()
            limits = settings.sql_limits
        bundle = await _build_native_bundle(
            replace(settings, sql_limits=limits), cleanup, worker_config.consumer.execution_role
        )
        return bundle.runtime
    except asyncio.CancelledError:
        raise
    except Exception:
        raise _unavailable() from None
