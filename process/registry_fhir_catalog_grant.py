# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Explicit metadata-only standing policy and owned Source publication fences."""

from __future__ import annotations

import asyncio
import hashlib
import json
import re
from contextlib import AsyncExitStack, asynccontextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field, replace
from datetime import datetime
from uuid import UUID

from db.plan_release_storage import storage_identifier
from db.registry_fhir_catalog import PROJECTOR_VERSION, SNAPSHOT_COLUMNS
from process.network_fhir_membership_source import PinnedFHIRMembershipSource
from process.registry_fhir_catalog_custody import verify_fhir_catalog_snapshot
from process.registry_fhir_catalog_observation import CATALOG_OBSERVATION_CONTRACT
from process.registry_fhir_catalog_publication import (
    OPERATION,
    FHIRCatalogPublicationRuntime,
    validated_fhir_catalog_command,
)
from process.registry_fhir_catalog_published_read import fhir_catalog_published_reads
from process.registry_fhir_catalog_retained import (
    normalize_fhir_catalog_retained_source_uri,
    pin_retained_fhir_catalog_source,
    require_retained_fhir_catalog_sources,
    retained_fhir_catalog_read_source,
    retained_fhir_catalog_recipe,
)
from process.registry_fhir_catalog_source_fence import fhir_catalog_source_publication_fence
from process.registry_fhir_catalog_source_producer import open_source_owned_catalog_export
from process.registry_fhir_catalog_sources import (
    MAX_PAGE,
    FHIRCatalogSourceReadProfile,
    fhir_catalog_source_generation,
    require_current_fhir_catalog_sources,
    validate_fhir_catalog_source_descriptor,
)
from process.registry_fhir_catalog_sources import parse_fhir_catalog_source_profile as source_profile
from process.registry_fhir_catalog_validation import (
    canonical_json,
    fhir_catalog_observation_digest,
    metadata_generation,
    snapshot_inputs,
)
from process.registry_source_recipe_store import (
    RegistrySourceMembershipRecipe,
    canonical_registry_source_recipes,
    decode_registry_source_recipes,
)
from process.uhc_flex_practitioner_async_safety import drain_operation

MAX_CONFIG_BYTES = 1048576
MAX_CROSSWALKS = 5000
_CONTEXT = ContextVar("registry_fhir_catalog_grant_source_snapshot", default=None)
_IDENTITY_SQL = """SELECT pg_catalog.current_database()::text AS database_name,
 (SELECT oid::bigint FROM pg_catalog.pg_database WHERE datname=pg_catalog.current_database()) AS database_oid,
 (pg_catalog.pg_control_system()).system_identifier::text AS system_identifier,
 pg_catalog.pg_postmaster_start_time() AS postmaster_started,pg_catalog.pg_is_in_recovery() AS recovering"""
_ROLE_SQL = """SELECT r.rolname,r.rolcanlogin,r.rolsuper,r.rolcreatedb,r.rolcreaterole,r.rolreplication,r.rolbypassrls,
 EXISTS(SELECT FROM pg_catalog.pg_roles elevated WHERE
   (elevated.rolsuper OR elevated.rolcreatedb OR elevated.rolcreaterole OR elevated.rolreplication OR elevated.rolbypassrls
    OR elevated.rolname IN ('pg_read_all_data','pg_write_all_data','pg_read_server_files','pg_write_server_files',
       'pg_execute_server_program','pg_signal_backend','pg_checkpoint','pg_create_subscription'))
   AND pg_catalog.pg_has_role(r.oid,elevated.oid,'MEMBER')) AS elevated_membership
 FROM pg_catalog.pg_roles r WHERE r.rolname=current_user"""
_PROFILES_SQL = """SELECT p.schema_name,
 CASE WHEN p.published_read_scope IS NULL THEN
   EXISTS(SELECT FROM pg_catalog.pg_roles allowed WHERE allowed.rolname=ANY(p.runtime_roles)
     AND pg_catalog.pg_has_role(current_user,allowed.oid,'USAGE'))
 ELSE session_user=current_user AND current_user::text=p.published_read_scope->>'reader_role'
   AND (SELECT oid::bigint FROM pg_catalog.pg_roles WHERE rolname=current_user)=
     (p.published_read_scope->>'reader_oid')::bigint END AS admitted_role,
 owner.oid IS NOT NULL AND NOT (pg_catalog.pg_has_role(current_user,owner.oid,'MEMBER')
   OR pg_catalog.pg_has_role(current_user,owner.oid,'SET')) AS no_owner_path,
 NOT coalesce(pg_catalog.has_schema_privilege(current_user,
   pg_catalog.to_regnamespace(pg_catalog.format('%I',p.schema_name)),'CREATE'),true)
   AS no_schema_create
 FROM jsonb_to_recordset($1::jsonb) p(schema_name text,owner_role text,runtime_roles text[],published_read_scope jsonb)
 LEFT JOIN pg_catalog.pg_roles owner ON owner.rolname=p.owner_role"""
_DESCRIPTORS_SQL = """WITH page AS MATERIALIZED (
 SELECT source_generation,source_id,descriptor_json::text AS document
 FROM {namespace}.registry_fhir_catalog_source
 WHERE source_generation=ANY($1::text[]) AND source_generation>$2::text COLLATE "C"
 ORDER BY source_generation LIMIT 1000
), bounded AS (
 SELECT *,octet_length(document) AS row_bytes,sum(octet_length(document)+256) OVER(ORDER BY source_generation) AS bytes,
 row_number() OVER(ORDER BY source_generation) AS ordinal FROM page
) SELECT source_generation,source_id,CASE WHEN row_bytes<=8388608 THEN document END AS document
 FROM bounded WHERE ordinal=1 OR bytes<=8388608 ORDER BY source_generation"""


class FHIRCatalogGrantError(ValueError):
    """The configured grant or exact publication/source scope is unavailable."""


def _fail(reason="invalid"):
    raise FHIRCatalogGrantError("registry_fhir_catalog_grant_" + reason)


def _text(text, maximum):
    if (
        type(text) is not str
        or not text
        or text != text.strip()
        or len(text.encode()) > maximum
        or not text.isprintable()
    ):
        _fail()


def _digest(encoded):
    if type(encoded) is not str or re.fullmatch(r"[0-9a-f]{64}", encoded) is None:
        _fail()


def _crosswalk(entry):
    if type(entry) is not tuple or len(entry) != 3:
        _fail()
    source_id, organization_id, company_id = entry
    _text(source_id, 128)
    if type(organization_id) is not str or re.fullmatch(r"[A-Za-z0-9.-]{1,64}", organization_id) is None:
        _fail()
    try:
        parsed = UUID(company_id) if type(company_id) is str else None
    except ValueError:
        _fail()
    if parsed is None or not parsed.int or str(parsed) != company_id or source_id == "registry-manual":
        _fail()


@dataclass(frozen=True)
class FHIRCatalogStandingGrant:
    grant_id: str
    version: int
    control_schema: str
    source_profiles: tuple[FHIRCatalogSourceReadProfile, ...]
    company_crosswalks: tuple[tuple[str, str, str], ...] = ()
    retained_source_recipes: bool = False
    retained_source_uris: tuple[tuple[str, str], ...] = ()
    retained_sources: tuple[RegistrySourceMembershipRecipe, ...] = ()
    observation_contract: str | None = None
    _sha256: str = field(init=False, repr=False)

    def __post_init__(self):
        _text(self.grant_id, 128)
        storage_identifier(self.control_schema)
        if type(self.version) is not int or not 0 < self.version < 2**63:
            _fail()
        if type(self.retained_source_recipes) is not bool:
            _fail()
        if self.observation_contract is not None and (
            type(self.observation_contract) is not str or self.observation_contract != CATALOG_OBSERVATION_CONTRACT
        ):
            _fail()
        if type(self.retained_sources) is not tuple or any(
            type(recipe) is not RegistrySourceMembershipRecipe
            or type(recipe.source_pin) is not PinnedFHIRMembershipSource
            or recipe.source_pin.retained_capture is None
            for recipe in self.retained_sources
        ):
            _fail()
        object.__setattr__(
            self,
            "retained_sources",
            decode_registry_source_recipes(canonical_registry_source_recipes(self.retained_sources)),
        )
        if type(self.retained_source_uris) is not tuple or len(self.retained_source_uris) > MAX_PAGE:
            _fail()
        uris = []
        for entry in self.retained_source_uris:
            if type(entry) is not tuple or len(entry) != 2:
                _fail()
            endpoint, uri = entry
            _text(endpoint, 64)
            _text(uri, 2048)
            uris.append((endpoint, normalize_fhir_catalog_retained_source_uri(uri)))
        if len({endpoint for endpoint, _ in uris}) != len(uris):
            _fail()
        object.__setattr__(self, "retained_source_uris", tuple(sorted(uris)))
        if type(self.source_profiles) is not tuple or len(self.source_profiles) > 64:
            _fail()
        if any(type(profile) is not FHIRCatalogSourceReadProfile for profile in self.source_profiles):
            _fail()
        profiles = [canonical_json(profile.document()) for profile in self.source_profiles]
        if len(set(profiles)) != len(profiles):
            _fail()
        if type(self.company_crosswalks) is not tuple or len(self.company_crosswalks) > MAX_CROSSWALKS:
            _fail()
        for entry in self.company_crosswalks:
            _crosswalk(entry)
        if len({entry[:2] for entry in self.company_crosswalks}) != len(self.company_crosswalks):
            _fail()
        encoded = canonical_json(self.document()).encode()
        if len(encoded) > MAX_CONFIG_BYTES:
            _fail()
        object.__setattr__(self, "_sha256", hashlib.sha256(encoded).hexdigest())

    @property
    def sha256(self):
        """Return the canonical digest bound into every service command and snapshot."""
        return self._sha256

    def document(self):
        """Return only the explicit metadata policy, without credentials or write grants."""
        document_by_field = {
            "operation": OPERATION,
            "grant_id": self.grant_id,
            "version": self.version,
            "control_schema": self.control_schema,
            "source_profiles": sorted((profile.document() for profile in self.source_profiles), key=canonical_json),
            "company_crosswalks": [
                {"source_id": source, "organization_id": organization, "company_id": company}
                for source, organization, company in sorted(self.company_crosswalks)
            ],
            "retained_source_recipes": self.retained_source_recipes,
            "retained_source_uris": [
                {"endpoint_id": endpoint, "source_uri": uri} for endpoint, uri in self.retained_source_uris
            ],
        }
        if self.retained_sources:
            document_by_field["retained_sources"] = json.loads(canonical_registry_source_recipes(self.retained_sources))
        if self.observation_contract is not None:
            document_by_field["observation_contract"] = self.observation_contract
        return document_by_field

    def crosswalk_for(self, source_id):
        """Return this source's approved explicit mappings without allocating entities."""
        _text(source_id, 128)
        return tuple(
            (organization, company)
            for source, organization, company in sorted(self.company_crosswalks)
            if source == source_id
        )


def _unique(pairs):
    document_by_key = {}
    for key, value in pairs:
        if key in document_by_key:
            _fail()
        document_by_key[key] = value
    return document_by_key


def parse_fhir_catalog_standing_grant(document):
    """Parse the closed trusted configuration; no record mutation can be authorized."""
    try:
        if type(document) is str:
            if not 0 < len(document.encode()) <= MAX_CONFIG_BYTES:
                _fail()
            document = json.loads(document, object_pairs_hook=_unique)
        required_fields = {
            "operation",
            "grant_id",
            "version",
            "control_schema",
            "source_profiles",
            "company_crosswalks",
        }
        if (
            type(document) is not dict
            or not required_fields
            <= set(document)
            <= required_fields
            | {"retained_source_recipes", "retained_source_uris", "retained_sources", "observation_contract"}
            or document["operation"] != OPERATION
        ):
            _fail()
        if type(document["source_profiles"]) is not list or type(document["company_crosswalks"]) is not list:
            _fail()
        crosswalks = []
        for entry in document["company_crosswalks"]:
            if type(entry) is not dict or set(entry) != {"source_id", "organization_id", "company_id"}:
                _fail()
            crosswalks.append((entry["source_id"], entry["organization_id"], entry["company_id"]))
        uris = document.get("retained_source_uris", [])
        if type(uris) is not list or any(
            type(entry) is not dict or set(entry) != {"endpoint_id", "source_uri"} for entry in uris
        ):
            _fail()
        return FHIRCatalogStandingGrant(
            document["grant_id"],
            document["version"],
            document["control_schema"],
            tuple(source_profile(profile) for profile in document["source_profiles"]),
            tuple(crosswalks),
            document.get("retained_source_recipes", False),
            tuple((entry["endpoint_id"], entry["source_uri"]) for entry in uris),
            decode_registry_source_recipes(document.get("retained_sources", [])),
            document.get("observation_contract"),
        )
    except KeyError, TypeError, ValueError, UnicodeError:
        _fail()


@dataclass(frozen=True)
class _SourceSnapshot:
    task: object
    writer: object
    reader: object | None
    metadata_generation: str
    grant_sha256: str
    current: tuple[dict, ...]
    retained: tuple[dict, ...] = ()
    retained_reads: tuple[object, ...] = ()
    authorized: bool = False


async def _protected_snapshot(connection, proposed, grant, *, generation=None, published=False):
    selected_generation = generation or proposed["metadata_generation"]
    _digest(selected_generation)
    stored = await connection.fetchrow(
        f"SELECT {','.join(SNAPSHOT_COLUMNS)} FROM {storage_identifier(grant.control_schema)}.registry_fhir_catalog_snapshot "
        "WHERE metadata_generation=$1" + (" AND published_at IS NOT NULL" if published else ""),
        selected_generation,
    )
    if stored is None:
        _fail("snapshot_unavailable")
    await verify_fhir_catalog_snapshot(connection, stored)
    if generation is None and (
        stored["schema_name"] != proposed["schema_name"] or metadata_generation(proposed) != selected_generation
    ):
        _fail("snapshot_changed")
    return stored


async def _descriptors(connection, snapshot, vector):
    expected_by_generation = {entry["source_generation"]: entry for entry in vector}
    descriptors_by_generation = {}
    generations = sorted(expected_by_generation)
    for position in range(0, len(generations), MAX_PAGE):
        batch, after = generations[position : position + MAX_PAGE], ""
        while True:
            entries = await connection.fetch(
                _DESCRIPTORS_SQL.format(namespace=storage_identifier(snapshot["schema_name"])), batch, after
            )
            if not entries:
                break
            for entry in entries:
                if entry["document"] is None:
                    _fail("descriptor_bounds")
                descriptor = validate_fhir_catalog_source_descriptor(json.loads(entry["document"]))
                generation = fhir_catalog_source_generation(descriptor)
                if (
                    generation != entry["source_generation"]
                    or descriptor["coordinates"]["source_id"] != entry["source_id"]
                ):
                    _fail("descriptor_changed")
                if expected_by_generation[generation]["source_id"] != entry["source_id"]:
                    _fail("descriptor_changed")
                descriptors_by_generation[generation] = descriptor
            after = entries[-1]["source_generation"]
    if set(descriptors_by_generation) != set(expected_by_generation):
        _fail("descriptor_unavailable")
    return descriptors_by_generation


def _admitted_current(grant, vector, descriptors):
    allowed_profiles = {canonical_json(profile.document()) for profile in grant.source_profiles}
    selected_descriptors = []
    for entry in vector:
        if entry["status"] == "current":
            descriptor = descriptors[entry["source_generation"]]
            if descriptor["profile"] == "retained_normalized":
                continue
            if (
                canonical_json(descriptor["native_read_profile"]) not in allowed_profiles
                or descriptor["complete"] is not True
            ):
                _fail("source_not_admitted")
            if descriptor["admission"]["kind"] == "catalog_observation":
                if grant.observation_contract != CATALOG_OBSERVATION_CONTRACT:
                    _fail("source_not_admitted")
                continue
            selected_descriptors.append(descriptor)
    if len(selected_descriptors) > MAX_PAGE:
        _fail("current_source_limit")
    return tuple(selected_descriptors)


def _admitted_retained(grant, vector, descriptors):
    if any(
        entry["status"] != "last_good" and descriptors[entry["source_generation"]]["profile"] == "retained_normalized"
        for entry in vector
    ):
        _fail("retained_freshness_invalid")
    retained_descriptors = tuple(
        descriptors[entry["source_generation"]]
        for entry in vector
        if descriptors[entry["source_generation"]]["profile"] == "retained_normalized"
    )
    granted_recipes = {canonical_registry_source_recipes((recipe,)) for recipe in grant.retained_sources}
    for descriptor in retained_descriptors:
        if descriptor["retained_manifest"] is None:
            if canonical_registry_source_recipes((retained_fhir_catalog_recipe(descriptor),)) not in granted_recipes:
                _fail("source_not_admitted")
        elif not grant.retained_source_recipes:
            _fail("source_not_admitted")
    uri_by_endpoint = dict(grant.retained_source_uris)
    if any(
        descriptor["source_uri"] != uri_by_endpoint.get(descriptor["endpoint_id"])
        for descriptor in retained_descriptors
    ):
        _fail("source_not_admitted")
    if len(retained_descriptors) > MAX_PAGE:
        _fail("current_source_limit")
    return retained_descriptors


async def _retained_sources(connection, command, snapshot, grant, vector, descriptors):
    retained_entries = [
        entry
        for entry in vector
        if entry["status"] == "last_good"
        and descriptors[entry["source_generation"]]["profile"] != "retained_normalized"
    ]
    if not retained_entries:
        return
    previous = command["expected_metadata_generation"]
    if previous is None:
        _fail("history_required")
    prior = await _protected_snapshot(connection, snapshot, grant, generation=previous, published=True)
    admitted_sources = {
        (entry["source_generation"], entry["source_id"]) for entry in snapshot_inputs(prior)["source_vector_json"]
    }
    if any((entry["source_generation"], entry["source_id"]) not in admitted_sources for entry in retained_entries):
        _fail("history_changed")


def _active_context(connection, snapshot, grant, *, required=True):
    context = _CONTEXT.get()
    if context is None and not required:
        return None
    if (
        type(context) is not _SourceSnapshot
        or context.task is not asyncio.current_task()
        or context.writer is not connection
    ):
        _fail("source_context_required")
    if context.metadata_generation != snapshot["metadata_generation"] or context.grant_sha256 != grant.sha256:
        _fail("source_context_changed")
    return context


async def _reader_identity(reader, writer, descriptors):
    reader_identity_by_field = dict(await reader.fetchrow(_IDENTITY_SQL))
    writer_identity_by_field = dict(await writer.fetchrow(_IDENTITY_SQL))
    if reader_identity_by_field != writer_identity_by_field or reader_identity_by_field["recovering"] is not False:
        _fail("database_changed")
    if any(descriptor["database_oid"] != reader_identity_by_field["database_oid"] for descriptor in descriptors):
        _fail("database_changed")
    role = await reader.fetchrow(_ROLE_SQL)
    if (
        role is None
        or role["rolcanlogin"] is not True
        or any(
            role[field] is not False
            for field in (
                "rolsuper",
                "rolcreatedb",
                "rolcreaterole",
                "rolreplication",
                "rolbypassrls",
                "elevated_membership",
            )
        )
    ):
        _fail("reader_role_unsafe")
    profiles_by_document = {
        canonical_json(descriptor["native_read_profile"]): descriptor["native_read_profile"]
        for descriptor in descriptors
    }
    for descriptor in descriptors:
        if descriptor["profile"] == "retained_normalized":
            capture = retained_fhir_catalog_read_source(descriptor).retained_capture
            profile = FHIRCatalogSourceReadProfile(
                capture.schema_name, capture.owner_role, capture.runtime_roles
            ).document()
            profiles_by_document[canonical_json(profile)] = profile
    permissions = await reader.fetch(_PROFILES_SQL, canonical_json(list(profiles_by_document.values())))
    if len(permissions) != len(profiles_by_document) or any(
        entry[field] is not True
        for entry in permissions
        for field in ("admitted_role", "no_owner_path", "no_schema_create")
    ):
        _fail("reader_role_unsafe")


@asynccontextmanager
async def _readonly_snapshot(reader):
    transaction = reader.transaction(isolation="repeatable_read", readonly=True)
    original_error = None
    try:
        await drain_operation(transaction.start(), preserve_cancellation=True)
        await reader.execute("SET LOCAL search_path = pg_catalog, pg_temp, public")
        with fhir_catalog_published_reads(reader):
            yield
    except BaseException as error:
        original_error = error
        raise
    finally:
        try:
            if reader.is_in_transaction():
                await drain_operation(transaction.rollback(), preserve_cancellation=False)
        except BaseException:
            reader.terminate()
            if original_error is None:
                raise


@dataclass(frozen=True)
class _GrantCallbacks:
    grant: FHIRCatalogStandingGrant
    source_reader_pool: object = field(repr=False)

    async def authorize_metadata(self, *, command, snapshot, connection):
        """Bind one service command to protected metadata and explicit standing policy."""
        command = validated_fhir_catalog_command(command)
        stored = await _protected_snapshot(connection, snapshot, self.grant)
        if (
            command["standing_grant_sha256"] != self.grant.sha256
            or stored["standing_grant_sha256"] != self.grant.sha256
        ):
            _fail("policy_changed")
        if (
            stored["projector_version"] != PROJECTOR_VERSION
            or command["metadata_generation"] != stored["metadata_generation"]
            or command["expected_serving_generation"] != stored["network_generation"]
            or command["expected_approved_revision"] != stored["metadata_approved_revision"]
        ):
            _fail("snapshot_changed")
        vector = snapshot_inputs(stored)["source_vector_json"]
        descriptors = await _descriptors(connection, stored, vector)
        _admitted_current(self.grant, vector, descriptors)
        retained = _admitted_retained(self.grant, vector, descriptors)
        if retained:
            await require_retained_fhir_catalog_sources(connection, retained, control_schema=self.grant.control_schema)
        await _retained_sources(connection, command, stored, self.grant, vector, descriptors)
        context = _active_context(connection, stored, self.grant, required=False)
        if context is not None:
            _CONTEXT.set(replace(context, authorized=True))
        return {
            "authorized": True,
            "operation": OPERATION,
            "command_sha256": hashlib.sha256(canonical_json(command).encode()).hexdigest(),
            "standing_grant_sha256": self.grant.sha256,
        }

    @asynccontextmanager
    async def source_fence(self, *, snapshot, connection):
        """Own the SourceReader and pre-snapshot endpoint locks through writer commit."""
        if connection.is_in_transaction() or _CONTEXT.get() is not None:
            _fail("source_context_changed")
        async with connection.transaction(isolation="repeatable_read", readonly=True):
            stored = await _protected_snapshot(connection, snapshot, self.grant)
            vector = snapshot_inputs(stored)["source_vector_json"]
            descriptors = await _descriptors(connection, stored, vector)
            current = _admitted_current(self.grant, vector, descriptors)
            retained = _admitted_retained(self.grant, vector, descriptors)
            if retained:
                await require_retained_fhir_catalog_sources(
                    connection, retained, control_schema=self.grant.control_schema
                )
        if not current and not retained:
            async with self._brand(connection, None, stored, current):
                yield
            return
        if not callable(getattr(self.source_reader_pool, "acquire", None)):
            _fail("reader_required")
        async with self.source_reader_pool.acquire() as reader:
            if reader is connection:
                _fail("reader_required")
            async with AsyncExitStack() as stack:
                if current:
                    await stack.enter_async_context(fhir_catalog_source_publication_fence(reader, current))
                async with _readonly_snapshot(reader):
                    await _reader_identity(reader, connection, current + retained)
                    reads = tuple(
                        [
                            await pin_retained_fhir_catalog_source(reader, retained_descriptor)
                            for retained_descriptor in retained
                        ]
                    )
                    async with self._brand(connection, reader, stored, current, retained, reads):
                        yield

    @asynccontextmanager
    async def _brand(self, writer, reader, snapshot, current, retained=(), reads=()):
        token = _CONTEXT.set(
            _SourceSnapshot(
                asyncio.current_task(),
                writer,
                reader,
                snapshot["metadata_generation"],
                self.grant.sha256,
                current,
                retained,
                reads,
            )
        )
        try:
            yield
        finally:
            _CONTEXT.reset(token)

    async def is_source_snapshot_current(self, *, snapshot, connection):
        """Recheck only admitted current Sources inside this owned, authenticated fence."""
        context = _active_context(connection, snapshot, self.grant)
        if context.authorized is not True:
            _fail("source_authorization_required")
        if context.current:
            if context.reader is None or not context.reader.is_in_transaction():
                _fail("source_context_changed")
            for position in range(0, len(context.current), MAX_PAGE):
                await require_current_fhir_catalog_sources(
                    context.reader, context.current[position : position + MAX_PAGE], lock=True
                )
        if context.retained:
            await require_retained_fhir_catalog_sources(
                connection, context.retained, control_schema=self.grant.control_schema
            )
            for retained_read in context.retained_reads:
                await retained_read.verify(context.reader, retained_read.capture)
        return True


def build_fhir_catalog_publication_runtime(grant, source_reader_pool, *, source_export=None):
    """Build existing publication callbacks; the caller owns pool credentials/lifetime."""
    if type(grant) is not FHIRCatalogStandingGrant:
        _fail()
    if source_export is not None and not callable(source_export):
        _fail("source_export_invalid")
    callbacks = _GrantCallbacks(grant, source_reader_pool)
    return FHIRCatalogPublicationRuntime(
        authorize_metadata=callbacks.authorize_metadata,
        verify_sources=callbacks.is_source_snapshot_current,
        source_fence=callbacks.source_fence,
    )


class _ObservationReplayDigest:
    def __init__(self):
        self.digest = hashlib.sha256()
        self.rows = 0
        self.previous_key = None

    async def collect(self, rows):
        """Hash the exact PK-ordered documents emitted by the shared producer."""
        from process.registry_fhir_catalog_store import TABLE_COLUMNS

        for row in rows:
            key = row[:5]
            if self.previous_key is not None and key <= self.previous_key:
                _fail("observation_order_changed")
            document_by_field = dict(zip(TABLE_COLUMNS["registry_fhir_catalog_observation"], row, strict=True))
            document_by_field["document_json"] = json.loads(document_by_field["document_json"])
            self.digest.update(canonical_json(document_by_field).encode() + b"\n")
            self.rows += 1
            self.previous_key = key


def _is_observed_source(descriptor):
    return descriptor["profile"] != "retained_normalized" and descriptor["admission"]["kind"] == "catalog_observation"


async def _observation_accounting(connection, schema_name, generation):
    record = await connection.fetchrow(
        f"SELECT source_rows,observation_rows,accepted_at,created_at FROM {storage_identifier(schema_name)}."
        "registry_fhir_catalog_source WHERE source_generation=$1",
        generation,
    )
    if record is None:
        _fail("observation_accounting_changed")
    return dict(record)


async def _prior_observations(connection, snapshot, grant):
    previous = await connection.fetchval(
        f"SELECT metadata_generation FROM {storage_identifier(grant.control_schema)}.network_serving_control WHERE id=1"
    )
    if previous is None:
        return None, {}
    prior = await _protected_snapshot(connection, snapshot, grant, generation=previous, published=True)
    if await connection.fetchval(
        f"SELECT EXISTS(SELECT FROM {storage_identifier(prior['schema_name'])}.registry_fhir_catalog_network old "
        f"LEFT JOIN {storage_identifier(snapshot['schema_name'])}.registry_fhir_catalog_network current "
        "USING(network_id) WHERE current.network_id IS NULL)"
    ):
        _fail("observation_history_changed")
    descriptors = await _descriptors(connection, prior, snapshot_inputs(prior)["source_vector_json"])
    return prior, {
        generation: descriptor for generation, descriptor in descriptors.items() if _is_observed_source(descriptor)
    }


async def _verify_carried_observation(connection, snapshot, prior, generation):
    current = await _observation_accounting(connection, snapshot["schema_name"], generation)
    previous = await _observation_accounting(connection, prior["schema_name"], generation)
    if current != previous:
        _fail("observation_history_changed")
    digests = []
    for selected in (snapshot, prior):
        digests.append(
            await fhir_catalog_observation_digest(
                connection, schema_name=selected["schema_name"], source_generation=generation
            )
        )
    if any(digests[0][field] != digests[1][field] for field in ("rows", "sha256")):
        _fail("observation_history_changed")


async def _verify_fresh_observation(reader, connection, snapshot, generation, descriptor, workspace):
    from process.registry_fhir_catalog_observed_source import collect_fhir_catalog_observation
    from process.registry_fhir_catalog_sync import replay_fhir_catalog_observations

    actual = await collect_fhir_catalog_observation(
        reader,
        source_profile(descriptor["native_read_profile"]),
        endpoint_id=descriptor["endpoint_id"],
        dataset_id=descriptor["coordinates"]["dataset_id"],
        require_current=False,
    )
    if actual.descriptor != descriptor:
        _fail("observation_source_changed")
    expected = _ObservationReplayDigest()
    source_rows = await replay_fhir_catalog_observations(
        reader, descriptor, expected.collect, reference_workspace=workspace
    )
    await _verify_observation_replay(connection, snapshot, generation, descriptor, source_rows, expected)


async def _verify_observation_replay(connection, snapshot, generation, descriptor, source_rows, expected):
    """Compare independent replay with the exact sealed facts and source accounting."""
    stored = await fhir_catalog_observation_digest(
        connection, schema_name=snapshot["schema_name"], source_generation=generation
    )
    accounting = await _observation_accounting(connection, snapshot["schema_name"], generation)
    if (
        source_rows != sum(family["rows"] for family in descriptor["admission"]["families"].values())
        or accounting["source_rows"] != source_rows
        or accounting["observation_rows"] != expected.rows
        or accounting["accepted_at"] != datetime.fromisoformat(descriptor["admission"]["publication"]["published_at"])
        or stored["rows"] != expected.rows
        or stored["sha256"] != expected.digest.hexdigest()
    ):
        _fail("observation_facts_changed")


async def _verify_retained_product_observations(connection, snapshot, grant, reader, workspace, descriptors):
    from process.registry_fhir_catalog_extraction import RESOURCE_TYPES
    from process.registry_fhir_catalog_products import selected_reviewed_fhir_catalog_product_sources
    from process.registry_fhir_catalog_projection_validation import require_fhir_catalog_registry_reader
    from process.registry_fhir_catalog_sync import (
        replay_fhir_catalog_observations,
        resolve_fhir_catalog_projection_generations,
    )

    await require_fhir_catalog_registry_reader(connection, reader, snapshot, grant)
    active = await resolve_fhir_catalog_projection_generations(
        reader if reader is not None else connection, snapshot, grant=grant, descriptors=descriptors
    )

    selected = await selected_reviewed_fhir_catalog_product_sources(
        connection,
        observation_schema=snapshot["schema_name"],
        snapshot=snapshot,
        control_schema=grant.control_schema,
        source_generations=active,
    )
    if not selected:
        return
    if reader is None or workspace is None:
        _fail("retained_reader_required")
    source_entries = [
        entry for entry in snapshot_inputs(snapshot)["source_vector_json"] if entry["source_generation"] in selected
    ]
    retained = _admitted_retained(grant, source_entries, selected)
    await require_retained_fhir_catalog_sources(reader, retained, control_schema=grant.control_schema)
    for generation, descriptor in sorted(selected.items()):
        expected = _ObservationReplayDigest()
        source_rows = await replay_fhir_catalog_observations(
            reader, descriptor, expected.collect, reference_workspace=workspace
        )
        counts_by_family = dict(retained_fhir_catalog_read_source(descriptor).retained_capture.origin.resource_counts)
        accounting = await _observation_accounting(connection, snapshot["schema_name"], generation)
        stored = await fhir_catalog_observation_digest(
            connection, schema_name=snapshot["schema_name"], source_generation=generation
        )
        if (
            source_rows != sum(counts_by_family.get(family, 0) for family in RESOURCE_TYPES)
            or accounting["source_rows"] != source_rows
            or accounting["observation_rows"] != expected.rows
            or stored["rows"] != expected.rows
            or stored["sha256"] != expected.digest.hexdigest()
        ):
            _fail("observation_facts_changed")


def build_fhir_catalog_observation_verifier(
    grant, source_reader_pool, *, registry_reader=None, registry_workspace=None, source_export=None
):
    """Bind trusted collection/replay to writer closure, independently of Loader claims."""
    if type(grant) is not FHIRCatalogStandingGrant:
        _fail()
    if source_export is not None and not callable(source_export):
        _fail("source_export_invalid")

    async def verify(*, connection, copy_target, snapshot, standing_grant):
        """Verify fresh facts or exact prior custody only after candidate writes close."""
        if standing_grant.sha256 != grant.sha256 or copy_target.schema_name != snapshot["schema_name"]:
            _fail("policy_changed")
        vector = snapshot_inputs(snapshot)["source_vector_json"]
        descriptors = await _descriptors(connection, snapshot, vector)
        prior, previous = await _prior_observations(connection, snapshot, grant)
        if not set(previous) <= set(descriptors):
            _fail("observation_history_changed")
        fresh_sources = []
        for entry in vector:
            generation = entry["source_generation"]
            descriptor = descriptors[generation]
            if not _is_observed_source(descriptor):
                continue
            if generation in previous:
                if descriptor != previous[generation]:
                    _fail("observation_history_changed")
                await _verify_carried_observation(connection, snapshot, prior, generation)
            else:
                if entry["status"] != "current":
                    _fail("observation_history_required")
                fresh_sources.append((generation, descriptor))
        if fresh_sources:
            await _replay_registered_observations(
                source_reader_pool, connection, snapshot, grant, fresh_sources, source_export=source_export
            )
        await _verify_retained_product_observations(
            connection, snapshot, grant, registry_reader, registry_workspace, descriptors
        )
        from process.registry_fhir_catalog_validation import validate_fhir_catalog_observed_projection

        await validate_fhir_catalog_observed_projection(
            connection, snapshot, standing_grant=grant, prior_snapshot=prior, registry_reader=registry_reader
        )

    return verify


async def _replay_registered_observations(source_pool, connection, snapshot, grant, sources, *, source_export=None):
    from process.registry_fhir_catalog_reference_rollup import fhir_catalog_reference_workspace

    if grant.observation_contract != CATALOG_OBSERVATION_CONTRACT or (
        source_export is None and not callable(getattr(source_pool, "acquire", None))
    ):
        _fail("reader_required")
    allowed_profiles = {canonical_json(profile.document()) for profile in grant.source_profiles}
    if any(canonical_json(descriptor["native_read_profile"]) not in allowed_profiles for _, descriptor in sources):
        _fail("source_not_admitted")
    exported_sources, native_sources = [], []
    for source in sources:
        if source_export is not None and "published_read_scope" not in source[1]["native_read_profile"]:
            exported_sources.append(source)
        else:
            native_sources.append(source)
    if native_sources and not callable(getattr(source_pool, "acquire", None)):
        _fail("reader_required")
    if exported_sources:
        await _replay_source_owned_observations(source_export, connection, snapshot, exported_sources)
    if not native_sources:
        return
    async with source_pool.acquire() as reader:
        if reader is connection:
            _fail("reader_required")
        async with fhir_catalog_reference_workspace(reader) as workspace, _readonly_snapshot(reader):
            await _reader_identity(reader, connection, tuple(descriptor for _, descriptor in native_sources))
            for generation, descriptor in native_sources:
                await _verify_fresh_observation(reader, connection, snapshot, generation, descriptor, workspace)


async def _replay_source_owned_observations(source_export, connection, snapshot, sources):
    """Reopen each admitted profile under Source ownership for independent sealing replay."""
    sources_by_profile = {}
    for generation, descriptor in sources:
        profile_key = canonical_json(descriptor["native_read_profile"])
        sources_by_profile.setdefault(profile_key, []).append((generation, descriptor))
    for profile_key, selected in sources_by_profile.items():
        profile = source_profile(json.loads(profile_key))
        async with open_source_owned_catalog_export(source_export, profile, connection) as export:
            for generation, descriptor in selected:
                if descriptor["database_oid"] != export["database_identity"]["database_oid"]:
                    _fail("database_changed")
                expected = _ObservationReplayDigest()
                source_rows = await export["replay"](descriptor, expected.collect)
                if type(source_rows) is not int:
                    _fail("observation_facts_changed")
                await _verify_observation_replay(connection, snapshot, generation, descriptor, source_rows, expected)
