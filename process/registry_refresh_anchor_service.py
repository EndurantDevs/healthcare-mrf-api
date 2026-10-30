# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Host-bound native approved anchors, with source-specific protected read capabilities."""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker
from sqlalchemy.pool import AsyncAdaptedQueuePool

from process.network_address_projection import _identifier
from process.network_approved_membership_source import (
    ApprovedMembershipSource,
    pin_approved_membership_source,
)
from process.network_membership_copy import MAX_INPUT_BYTES
from process.registry_company_approval_fence import _source_engine, registry_company_approval_transaction
from process.registry_ptg_graph_reader import RegistryPTGGraphReadBudget
from process.registry_refresh_anchor_contract import (
    GENERIC_FHIR_PRODUCER,
    ApprovedRefreshBinding,
    RegistryRefreshAnchorMismatch,
    RegistryRefreshAnchorStale,
    RegistryRefreshAnchorUnavailable,
    encoded_document,
    validated_anchor_request,
    validated_enrollment,
    validated_scope,
)

_BINDINGS_SQL = """WITH wanted AS MATERIALIZED (
 SELECT * FROM jsonb_to_recordset($2::jsonb) AS selected(binding_id text,binding_revision bigint,network_id integer)
), matched AS MATERIALIZED (
 SELECT binding.record_kind,binding.record_key,binding.record_revision,binding.custom_revision,binding.record_json,
   binding.record_key IS NOT NULL AND binding.record_revision=wanted.binding_revision
   AND binding.record_json->'archived'='false'::jsonb
   AND jsonb_typeof(binding.record_json->'network_id')='number'
   AND binding.record_json->>'network_id'=wanted.network_id::text
   AND network.record_key IS NOT NULL AS is_matched
 FROM wanted LEFT JOIN {namespace}.registry_approved_record binding
   ON binding.approved_revision=$1 AND binding.record_kind='network_binding' AND binding.record_key=wanted.binding_id
 LEFT JOIN {namespace}.registry_approved_record network
   ON network.approved_revision=$1 AND network.record_kind='network' AND network.record_key=wanted.network_id::text
   AND network.record_json->'archived'='false'::jsonb
   AND jsonb_typeof(network.record_json->'network_id')='number'
   AND network.record_json->>'network_id'=wanted.network_id::text
), bounds AS (
 SELECT count(*) AS row_count,bool_and(coalesce(is_matched,false)) AS is_matched,
   coalesce(sum(octet_length(record_json::text)+1024),0)<=$3 AS is_bounded FROM matched
)
SELECT bounds.*,CASE WHEN bounds.is_bounded AND bounds.is_matched THEN (
 SELECT jsonb_agg(jsonb_build_object('record_kind',record_kind,'record_key',record_key,
   'record_revision',record_revision,'custom_revision',custom_revision,'record_json',record_json,
   'approved_binding_sha256',encode(sha256(convert_to(jsonb_build_array(record_kind,record_key,record_revision,
     custom_revision,record_json)::text,'UTF8')),'hex')) ORDER BY record_key COLLATE "C")::text FROM matched
) END AS bindings_json FROM bounds"""


async def read_approved_refresh_bindings(driver, approved_source, scopes, *, control_schema):
    """Batch-load exact current approved records; hash each using the existing native recipe."""
    if type(approved_source) is not ApprovedMembershipSource or not driver.is_in_transaction():
        raise RegistryRefreshAnchorUnavailable("registry_refresh_approved_pin_unavailable")
    validated_enrollment(scopes)
    selected_bindings = [
        {name: scope[name] for name in ("binding_id", "binding_revision", "network_id")} for scope in scopes
    ]
    native = await driver.fetchrow(
        _BINDINGS_SQL.format(namespace=_identifier(control_schema)),
        approved_source.approved_revision,
        json.dumps(selected_bindings, separators=(",", ":")),
        MAX_INPUT_BYTES,
    )
    if native is None or native["row_count"] != len(scopes) or native["is_matched"] is not True:
        raise RegistryRefreshAnchorMismatch("registry_refresh_approved_binding_mismatch")
    if native["is_bounded"] is not True or native["bindings_json"] is None:
        raise RegistryRefreshAnchorUnavailable("registry_refresh_approved_bindings_exceed_bound")
    return tuple(ApprovedRefreshBinding(**binding) for binding in json.loads(native["bindings_json"]))


def _database_target(engine):
    url = engine.url
    return (
        url.host,
        url.port,
        url.database,
        tuple(
            sorted(
                (key, value)
                for key, value in url.query.items()
                if key not in {"user", "username", "password", "passfile", "sslpassword"}
            )
        ),
    )


def _read_engine(sessions):
    if type(sessions) is not async_sessionmaker or sessions.class_ is not AsyncSession or sessions.kw.get("binds"):
        raise RegistryRefreshAnchorUnavailable("registry_refresh_reader_unconfigured")
    engine = sessions.kw.get("bind")
    if (
        type(engine) is not AsyncEngine
        or engine.dialect.name != "postgresql"
        or engine.dialect.driver != "asyncpg"
        or type(engine.pool) is not AsyncAdaptedQueuePool
        or engine.pool.size() != 1
        or engine.pool._max_overflow != 0
    ):
        raise RegistryRefreshAnchorUnavailable("registry_refresh_reader_unconfigured")
    return engine


async def _native_context(session):
    await session.execute(text("SET LOCAL statement_timeout='5s'"))
    await session.execute(text("SET LOCAL lock_timeout='1s'"))
    connection = await session.connection()
    driver = (await connection.get_raw_connection()).driver_connection
    identity = await driver.fetchrow(
        "SELECT current_database() AS name,oid,pg_catalog.pg_postmaster_start_time() AS started FROM pg_catalog.pg_database WHERE datname=current_database()"
    )
    return driver, tuple(identity)


async def _current_pin(driver, revision, schema):
    current = await driver.fetchval(
        f"SELECT approved_revision FROM {_identifier(schema)}.registry_revision_control WHERE id=1"
    )
    if type(current) is not int:
        raise RegistryRefreshAnchorUnavailable("registry_refresh_approved_control_unavailable")
    if current != revision:
        raise RegistryRefreshAnchorStale("registry_refresh_approved_revision_changed")
    return await pin_approved_membership_source(driver, approved_revision=revision, control_schema=schema)


@dataclass(frozen=True)
class RegistryRefreshAnchorService:
    """Installed factories and native adapters cannot be selected by request JSON."""

    reader_sessions: object = field(repr=False)
    control_schema: str
    source_reader_sessions: object = field(default=None, repr=False)
    cms_reader: object = field(default=None, repr=False)
    aca_store: object = field(default=None, repr=False)
    ptg_store: object = field(default=None, repr=False)
    read_budget: tuple[int, int, int] = (16 * 1024 * 1024, 16384, 65536)

    def __post_init__(self):
        _identifier(self.control_schema)
        outer = _source_engine(self.reader_sessions)
        if type(self.read_budget) is not tuple or len(self.read_budget) != 3:
            raise RegistryRefreshAnchorUnavailable("registry_refresh_read_budget_invalid")
        RegistryPTGGraphReadBudget(*self.read_budget)
        if self.source_reader_sessions is not None:
            child = _read_engine(self.source_reader_sessions)
            if child is outer or child.pool is outer.pool or _database_target(child) != _database_target(outer):
                raise RegistryRefreshAnchorUnavailable("registry_refresh_reader_database_mismatch")

    async def verify(self, document_by_field):
        """Hold the genuine approval fence through complete native source derivation."""
        validated_anchor_request(document_by_field)
        try:
            async with asyncio.timeout(8):
                return await self._verify_transaction(document_by_field)
        except RegistryRefreshAnchorMismatch, RegistryRefreshAnchorStale:
            raise
        except Exception as error:
            raise RegistryRefreshAnchorUnavailable("registry_refresh_native_proof_unavailable") from error

    async def _verify_transaction(self, document_by_field):
        async with registry_company_approval_transaction(
            self.reader_sessions, control_schema=self.control_schema
        ) as session:
            driver, identity = await _native_context(session)
            approved = await _current_pin(driver, document_by_field["approved_revision"], self.control_schema)
            scopes = document_by_field["enrolled_scopes"]
            bindings = await read_approved_refresh_bindings(
                driver, approved, scopes, control_schema=self.control_schema
            )
            budget = RegistryPTGGraphReadBudget(*self.read_budget)
            derived_by_id = {}
            ptg_bindings = tuple(binding for binding in bindings if binding.record_json["source_system"] == "ptg")
            if ptg_bindings:
                from process.registry_ptg_refresh_anchors import resolve_ptg_refresh_anchors

                if self.ptg_store is None:
                    raise RegistryRefreshAnchorUnavailable("registry_refresh_ptg_unconfigured")
                _merge_anchors(
                    derived_by_id,
                    ptg_bindings,
                    await resolve_ptg_refresh_anchors(
                        session, ptg_bindings, store=self.ptg_store, read_budget=budget, approved_source=approved
                    ),
                )
            native_bindings = tuple(binding for binding in bindings if binding.record_json["source_system"] != "ptg")
            if native_bindings:
                derived_by_id.update(await self._read_source_anchors(native_bindings, approved, identity, budget))
            return _verified_receipt(document_by_field, approved, derived_by_id)

    async def _read_source_anchors(self, bindings, approved, identity, budget):
        if self.source_reader_sessions is None:
            raise RegistryRefreshAnchorUnavailable("registry_refresh_source_reader_unconfigured")
        async with self.source_reader_sessions() as session, session.begin():
            await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
            driver, child_identity = await _native_context(session)
            if child_identity != identity:
                raise RegistryRefreshAnchorUnavailable("registry_refresh_reader_database_mismatch")
            child_pin = await _current_pin(driver, approved.approved_revision, self.control_schema)
            if child_pin != approved:
                raise RegistryRefreshAnchorUnavailable("registry_refresh_approved_fingerprint_mismatch")
            return await _derive_sources(self, session, bindings, budget)


async def _derive_sources(service, session, bindings, budget):
    derived_by_id = {}
    generic_bindings = tuple(
        binding
        for binding in bindings
        if binding.record_json["source_system"] == "fhir"
        and binding.record_json["producer_id"] == GENERIC_FHIR_PRODUCER
    )
    if generic_bindings:
        from process.registry_generic_refresh_anchors import resolve_generic_refresh_anchors

        _merge_anchors(
            derived_by_id,
            generic_bindings,
            await resolve_generic_refresh_anchors(session, generic_bindings, read_budget=budget),
        )
    cms_bindings = tuple(
        binding
        for binding in bindings
        if binding.record_json["source_system"] == "fhir"
        and binding.record_json["producer_id"] != GENERIC_FHIR_PRODUCER
    )
    if cms_bindings:
        from process.registry_cms_refresh_anchors import resolve_cms_refresh_anchors

        if service.cms_reader is None:
            raise RegistryRefreshAnchorUnavailable("registry_refresh_cms_unconfigured")
        _merge_anchors(
            derived_by_id,
            cms_bindings,
            await resolve_cms_refresh_anchors(session, cms_bindings, reader=service.cms_reader, read_budget=budget),
        )
    aca_bindings = tuple(binding for binding in bindings if binding.record_json["source_system"] == "aca")
    if aca_bindings:
        from process.registry_aca_refresh_anchors import resolve_aca_refresh_anchors

        if service.aca_store is None:
            raise RegistryRefreshAnchorUnavailable("registry_refresh_aca_unconfigured")
        _merge_anchors(
            derived_by_id,
            aca_bindings,
            await resolve_aca_refresh_anchors(session, aca_bindings, store=service.aca_store, read_budget=budget),
        )
    return derived_by_id


def _verified_receipt(document_by_field, approved, derived_by_id):
    scopes = document_by_field["enrolled_scopes"]
    if type(derived_by_id) is not dict or set(derived_by_id) != {scope["binding_id"] for scope in scopes}:
        raise RegistryRefreshAnchorUnavailable("registry_refresh_incomplete_source_proof")
    for scope in scopes:
        source_scope = derived_by_id[scope["binding_id"]]
        actual_by_field = {
            **scope,
            "source_scope": source_scope,
            "continuity_policy": "retained_successor"
            if source_scope["source_system"] == "fhir" and source_scope.get("producer_id") == GENERIC_FHIR_PRODUCER
            else {
                "fhir": "accepted_edition",
                "aca": "retained_successor",
                "ptg": "exact_capture",
            }[source_scope["source_system"]],
        }
        try:
            validated_scope(actual_by_field)
        except (ValueError, TypeError) as error:
            raise RegistryRefreshAnchorUnavailable("registry_refresh_native_source_scope_invalid") from error
        if actual_by_field != scope:
            raise RegistryRefreshAnchorMismatch("registry_refresh_native_source_scope_mismatch")
    receipt_by_field = {
        "approved_revision": approved.approved_revision,
        "approved_membership_sha256": approved.generation_id,
        "enrolled_scopes": scopes,
    }
    encoded_document(receipt_by_field)
    return receipt_by_field


def _merge_anchors(derived_by_id, bindings, native_scopes_by_id):
    """An adapter must derive exactly its full native batch before any merge succeeds."""
    if type(native_scopes_by_id) is not dict or set(native_scopes_by_id) != {
        binding.binding_id for binding in bindings
    }:
        raise RegistryRefreshAnchorUnavailable("registry_refresh_incomplete_source_proof")
    for binding in bindings:
        source_scope = native_scopes_by_id[binding.binding_id]
        if type(source_scope) is not dict or any(
            source_scope.get(name) != binding.record_json[name]
            for name in ("source_system", "source_id", "dataset_schema", "producer_id")
        ):
            raise RegistryRefreshAnchorUnavailable("registry_refresh_native_source_coordinates_invalid")
        if (
            binding.record_json["source_system"] in {"aca", "ptg"}
            or binding.record_json["producer_id"] == GENERIC_FHIR_PRODUCER
        ) and (
            source_scope.get("source_key") != binding.record_json["source_key"]
            or source_scope.get("approved_binding_sha256") != binding.approved_binding_sha256
        ):
            raise RegistryRefreshAnchorUnavailable("registry_refresh_native_binding_fingerprint_invalid")
    derived_by_id.update(native_scopes_by_id)
