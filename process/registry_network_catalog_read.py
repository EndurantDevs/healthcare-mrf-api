# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded approved network metadata reads on a caller-owned pinned transaction."""

from __future__ import annotations

import json
import re
from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from uuid import UUID

import asyncpg

from db.models import GeoZipLookup, Plan, PlanNetworkTierRaw, PlanRatingAreas
from db.registry_schema import registry_schema
from db.tiger_models import ZipState
from process.company_registry_assertion_values import COMPANY_ROLES
from process.network_address_projection import _identifier
from process.network_approved_membership_source import (
    pin_retained_approved_membership_source,
)
from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
from process.network_fhir_retained_capture_contract import PRODUCER as RETAINED_FHIR_PRODUCER
from process.network_fhir_retained_capture_contract import is_sha256
from process.network_membership_candidate_indexes import _catalog_readiness
from process.network_membership_serving_indexes import _index_digest, _serving_plan
from process.network_read_identity import NetworkReadIdentityError, verify_consumed_network_relations
from process.network_serving_read import resolve_network_serving_manifest
from process.registry_catalog_policy import RegistryOrganizationVisibility

MAX_PAGE = 100
MAX_OFFSET = 1_000_000
MAX_EXCLUSIONS = 50_000
MAX_RESPONSE_BYTES = 1_048_576
_BENEFIT_CODES = ("medical", "dental", "vision")
_PINNED_READ = ContextVar("registry_catalog_pinned_read", default=None)


class RegistryNetworkCatalogError(ValueError):
    """The request, retained proof or approved metadata is unavailable."""


def catalog_integer(number, maximum, *, minimum=1):
    """Require an actual integer within the inclusive request bounds."""
    if type(number) is not int or not minimum <= number <= maximum:
        raise RegistryNetworkCatalogError("registry_network_catalog_request_invalid")
    return number


_integer = catalog_integer


def catalog_text(text, maximum):
    """Require bounded, trimmed UTF-8 request text without control characters."""
    if (
        type(text) is not str
        or not text
        or text.strip() != text
        or len(text) > maximum
        or any(ord(char) < 32 or ord(char) == 127 for char in text)
    ):
        raise RegistryNetworkCatalogError("registry_network_catalog_request_invalid")
    try:
        text.encode("utf-8")
    except UnicodeError:
        raise RegistryNetworkCatalogError("registry_network_catalog_request_invalid") from None
    return text


_text = catalog_text


def catalog_uuid(identifier):
    """Require a nonzero UUID encoded in its canonical string form."""
    try:
        parsed = UUID(identifier) if type(identifier) is str else None
    except ValueError:
        parsed = None
    if parsed is None or not parsed.int or str(parsed) != identifier:
        raise RegistryNetworkCatalogError("registry_network_catalog_document_invalid")
    return identifier


_uuid = catalog_uuid


def _retained_fhir_scope(scope, producer_id):
    """Admit only registry-owned captures, without synthesizing legacy aliases."""
    if (
        producer_id != RETAINED_FHIR_PRODUCER
        or type(scope["organization_id"]) is not str
        or re.fullmatch(r"[A-Za-z0-9.-]{1,64}", scope["organization_id"]) is None
        or not is_sha256(scope["capture_sha256"])
    ):
        raise RegistryNetworkCatalogError("registry_network_catalog_document_invalid")


def _retained_product_scope(scope, producer_id):
    if (
        producer_id != RETAINED_FHIR_PRODUCER
        or type(scope["insurance_plan_id"]) is not str
        or re.fullmatch(r"[A-Za-z0-9.-]{1,64}", scope["insurance_plan_id"]) is None
        or not is_sha256(scope["capture_sha256"])
        or type(scope["product_network_id"]) is not int
        or not 1 <= scope["product_network_id"] <= 2147483647
    ):
        raise RegistryNetworkCatalogError("registry_network_catalog_document_invalid")


def _source_scope(system, scope, *, producer_id=None):
    if type(scope) is not dict:
        raise RegistryNetworkCatalogError("registry_network_catalog_document_invalid")
    if system == "fhir" and set(scope) == {"organization_id", "capture_sha256"}:
        _retained_fhir_scope(scope, producer_id)
        return scope
    if system == "fhir" and set(scope) == {"insurance_plan_id", "capture_sha256", "product_network_id"}:
        _retained_product_scope(scope, producer_id)
        return scope
    if system == "ptg" and "review_type" in scope:
        from process.registry_published_plan_binding_refs import (
            validated_published_plan_binding_scope,
        )

        try:
            validated_published_plan_binding_scope(scope)
        except ValueError, TypeError:
            raise RegistryNetworkCatalogError("registry_network_catalog_document_invalid") from None
        return scope
    if system == "ptg" and set(scope) == {"cohort_id", "company_key", "snapshot_id"}:
        for key, entry in scope.items():
            _text(entry, 512 if key == "company_key" else 128)
    elif system == "fhir" and set(scope) == {"organization_id", "legacy_uuid", "alias_scope"}:
        if (
            type(scope["organization_id"]) is not str
            or re.fullmatch(r"[A-Za-z0-9.-]{1,64}", scope["organization_id"]) is None
        ):
            raise RegistryNetworkCatalogError("registry_network_catalog_document_invalid")
        _uuid(scope["legacy_uuid"])
        _text(scope["alias_scope"], 512)
    elif system == "aca" and set(scope) == {
        "issuer_id",
        "plan_id",
        "plan_year",
        "state",
        "checksum_network",
    }:
        states = "AL AK AZ AR CA CO CT DE DC FL GA HI ID IL IN IA KS KY LA ME MD MA MI MN MS MO MT NE NV NH NJ NM NY NC ND OH OK OR PA RI SC SD TN TX UT VT VA WA WV WI WY AS GU MP PR VI".split()
        if (
            type(scope["issuer_id"]) is not str
            or re.fullmatch(r"[0-9]{5}", scope["issuer_id"]) is None
            or scope["issuer_id"] == "00000"
            or type(scope["state"]) is not str
            or scope["state"] not in states
            or type(scope["plan_id"]) is not str
            or re.fullmatch(
                scope["issuer_id"] + scope["state"] + r"[0-9]{7}(-[0-9]{2})?",
                scope["plan_id"],
            )
            is None
        ):
            raise RegistryNetworkCatalogError("registry_network_catalog_document_invalid")
        _integer(scope["plan_year"], 2100, minimum=2010)
        _integer(scope["checksum_network"], 2147483647, minimum=-2147483648)
    else:
        raise RegistryNetworkCatalogError("registry_network_catalog_document_invalid")
    return scope


def catalog_exclusions(network_ids):
    """Require bounded, strictly increasing positive network identifiers."""
    if type(network_ids) is not tuple or len(network_ids) > MAX_EXCLUSIONS:
        raise RegistryNetworkCatalogError("registry_network_catalog_policy_invalid")
    previous = 0
    for network_id in network_ids:
        _integer(network_id, 2147483647)
        if network_id <= previous:
            raise RegistryNetworkCatalogError("registry_network_catalog_policy_invalid")
        previous = network_id
    return network_ids


_exclusions = catalog_exclusions


@dataclass(frozen=True)
class RegistryNetworkCatalogQuery:
    limit: int = 25
    offset: int = 0
    generation_id: int | None = None
    search: str | None = None
    archived: bool | None = False
    source: RegistryNetworkSourceCoordinates | None = None
    benefit_code: str | None = None

    def __post_init__(self):
        _integer(self.limit, MAX_PAGE)
        _integer(self.offset, MAX_OFFSET, minimum=0)
        if self.generation_id is not None:
            _integer(self.generation_id, 9223372036854775807)
        if self.search is not None:
            _text(self.search, 512)
        if self.archived is not None and type(self.archived) is not bool:
            raise RegistryNetworkCatalogError("registry_network_catalog_request_invalid")
        if self.benefit_code is not None and (
            type(self.benefit_code) is not str or self.benefit_code not in _BENEFIT_CODES
        ):
            raise RegistryNetworkCatalogError("registry_network_catalog_request_invalid")
        if self.source is not None and type(self.source) is not RegistryNetworkSourceCoordinates:
            raise RegistryNetworkCatalogError("registry_network_catalog_request_invalid")


def catalog_closed_json(encoded, maximum):
    """Decode bounded JSON while rejecting duplicate keys and nonfinite constants."""
    if type(encoded) is not bytes or len(encoded) > maximum:
        raise RegistryNetworkCatalogError("registry_network_catalog_document_invalid")

    def _object_pairs(pairs):
        document_by_key = {}
        for key, entry in pairs:
            if key in document_by_key:
                raise RegistryNetworkCatalogError("registry_network_catalog_document_invalid")
            document_by_key[key] = entry
        return document_by_key

    def _invalid_constant(_constant):
        raise RegistryNetworkCatalogError("registry_network_catalog_document_invalid")

    try:
        return json.loads(encoded, object_pairs_hook=_object_pairs, parse_constant=_invalid_constant)
    except ValueError, UnicodeError:
        raise RegistryNetworkCatalogError("registry_network_catalog_document_invalid") from None


_closed_json = catalog_closed_json


@dataclass(frozen=True)
class RegistryNetworkCatalogLegacySelector:
    source: RegistryNetworkSourceCoordinates
    namespace: str
    value: str
    source_scope_json: bytes

    def __post_init__(self):
        if type(self.source) is not RegistryNetworkSourceCoordinates:
            raise RegistryNetworkCatalogError("registry_network_catalog_request_invalid")
        scope = _closed_json(self.source_scope_json, 16384)
        _source_scope(self.source.source_system, scope, producer_id=self.source.producer_id)
        if self.namespace == "checksum_network" and self.source.source_system == "aca":
            if set(scope) != {"issuer_id", "plan_id", "plan_year", "state", "checksum_network"}:
                raise RegistryNetworkCatalogError("registry_network_catalog_request_invalid")
            if type(self.value) is not str or re.fullmatch(r"-?(0|[1-9][0-9]{0,9})", self.value) is None:
                raise RegistryNetworkCatalogError("registry_network_catalog_request_invalid")
            if self.value == "-0" or not -2147483648 <= int(self.value) <= 2147483647:
                raise RegistryNetworkCatalogError("registry_network_catalog_request_invalid")
            if type(scope["checksum_network"]) is not int or scope["checksum_network"] != int(self.value):
                raise RegistryNetworkCatalogError("registry_network_catalog_request_invalid")
        elif self.namespace == "legacy_fhir_uuid" and self.source.source_system == "fhir":
            if set(scope) != {"organization_id", "legacy_uuid", "alias_scope"}:
                raise RegistryNetworkCatalogError("registry_network_catalog_request_invalid")
            try:
                identifier = UUID(self.value)
            except ValueError, TypeError, AttributeError:
                raise RegistryNetworkCatalogError("registry_network_catalog_request_invalid") from None
            if not identifier.int or str(identifier) != self.value or scope["legacy_uuid"] != self.value:
                raise RegistryNetworkCatalogError("registry_network_catalog_request_invalid")
            _text(scope["alias_scope"], 512)
        else:
            raise RegistryNetworkCatalogError("registry_network_catalog_request_invalid")


@dataclass(frozen=True)
class RegistryNetworkCatalogLegacyBatchSelector:
    """Resolve a bounded set of typed legacy values across approved sources."""

    namespace: str
    values: tuple[str, ...]

    def __post_init__(self):
        if type(self.namespace) is not str or self.namespace not in {"checksum_network", "legacy_fhir_uuid"}:
            raise RegistryNetworkCatalogError("registry_network_catalog_request_invalid")
        if type(self.values) is not tuple or not 1 <= len(self.values) <= 50:
            raise RegistryNetworkCatalogError("registry_network_catalog_request_invalid")
        for value in self.values:
            if self.namespace == "checksum_network":
                if type(value) is not str or re.fullmatch(r"-?(0|[1-9][0-9]{0,9})", value) is None:
                    raise RegistryNetworkCatalogError("registry_network_catalog_request_invalid")
                if value == "-0" or not -2147483648 <= int(value) <= 2147483647:
                    raise RegistryNetworkCatalogError("registry_network_catalog_request_invalid")
            elif self.namespace == "legacy_fhir_uuid":
                _uuid(value)
            else:
                raise RegistryNetworkCatalogError("registry_network_catalog_request_invalid")
        if len(set(self.values)) != len(self.values):
            raise RegistryNetworkCatalogError("registry_network_catalog_request_invalid")


_BASE_SQL = """
WITH approved AS MATERIALIZED (
  SELECT record_kind,record_key,record_revision,record_json
  FROM {control}.registry_approved_record WHERE approved_revision=$1
), visible AS MATERIALIZED (
  SELECT record_key::integer AS network_id,record_revision,record_json FROM approved
  WHERE record_kind='network' AND NOT record_key::integer=ANY($2::integer[])
), declared_assertions AS MATERIALIZED (
  SELECT DISTINCT network.network_id, assertion->>'benefit_domain' AS benefit_code
  FROM visible network JOIN approved links ON links.record_kind='company_links'
    AND links.record_json->'archived'='false'::jsonb
    AND links.record_json->'network_ids' @> jsonb_build_array(network.network_id)
  JOIN approved company ON company.record_kind='company' AND company.record_key=links.record_key
    AND company.record_json->'archived'='false'::jsonb
  CROSS JOIN LATERAL jsonb_array_elements(coalesce(links.record_json->'network_assertions','[]'::jsonb)) assertion
  WHERE assertion->>'company_id'=links.record_key
    AND assertion->>'network_id'=network.network_id::text
    AND assertion->>'benefit_domain' IN ('medical','dental','vision')
), declared_benefits AS MATERIALIZED (
  SELECT network_id,array_agg(benefit_code ORDER BY CASE benefit_code
    WHEN 'medical' THEN 1 WHEN 'dental' THEN 2 ELSE 3 END) AS benefit_codes
  FROM declared_assertions GROUP BY network_id
), filtered AS MATERIALIZED (
  SELECT network.* FROM visible network
  WHERE ($3::boolean IS NULL OR record_json->'archived'=to_jsonb($3::boolean))
    AND ($4::text IS NULL OR strpos(lower(record_json->>'display_name'),lower($4))>0
      OR EXISTS(SELECT 1 FROM jsonb_array_elements_text(record_json->'aliases') alias
        WHERE strpos(lower(alias),lower($4))>0))
    AND ($5::text[] IS NULL OR EXISTS(SELECT 1 FROM approved binding
      WHERE binding.record_kind='network_binding' AND binding.record_json->'archived'='false'::jsonb
        AND binding.record_json->>'network_id'=network.network_id::text
        AND ARRAY[binding.record_json->>'source_system',binding.record_json->>'source_id',
          binding.record_json->>'dataset_schema',binding.record_json->>'dataset_id',
          binding.record_json->>'producer_id',binding.record_json->>'edition_id']=$5::text[]))
    AND ($6::integer[] IS NULL OR network_id=ANY($6::integer[]))
    AND ($10::text IS NULL OR EXISTS(SELECT 1 FROM declared_benefits declaration
      WHERE declaration.network_id=network.network_id AND $10=ANY(declaration.benefit_codes)))
), page AS MATERIALIZED (
  SELECT * FROM filtered ORDER BY lower(record_json->>'display_name') COLLATE "C",network_id
  OFFSET $7 LIMIT $8
), companies AS MATERIALIZED (
  SELECT page.network_id,jsonb_build_object('company_id',company.record_key,
    'display_name',company.record_json->'display_name','roles',company.record_json->'roles',
    'revision',company.record_revision,'link_revision',links.record_revision,
    'group_id',links.record_json->'group_id') AS document
  FROM page JOIN approved links ON links.record_kind='company_links'
    AND links.record_json->'archived'='false'::jsonb
    AND links.record_json->'network_ids' @> jsonb_build_array(page.network_id)
  JOIN approved company ON company.record_kind='company' AND company.record_key=links.record_key
    AND company.record_json->'archived'='false'::jsonb
), bindings AS MATERIALIZED (
  SELECT page.network_id,jsonb_build_object('namespace','source_binding',
    'binding_id',binding.record_key,'revision',binding.record_revision,
    'source_system',binding.record_json->'source_system','source_id',binding.record_json->'source_id',
    'dataset_schema',binding.record_json->'dataset_schema','dataset_id',binding.record_json->'dataset_id',
    'producer_id',binding.record_json->'producer_id','edition_id',binding.record_json->'edition_id',
    'source_key',binding.record_json->'source_key','source_scope',binding.record_json->'source_scope_json',
    'evidence_id',binding.record_json->'evidence_id','evidence_sha256',binding.record_json->'evidence_sha256') AS document
  FROM page JOIN approved binding ON binding.record_kind='network_binding'
    AND binding.record_json->'archived'='false'::jsonb
    AND binding.record_json->>'network_id'=page.network_id::text
), bounds AS (
  SELECT coalesce((SELECT sum(octet_length(record_json::text)) FROM page),0)
    +coalesce((SELECT sum(octet_length(document::text)) FROM companies),0)
    +coalesce((SELECT sum(octet_length(document::text)) FROM bindings),0)
    +$8::bigint*1024 <= $9 AS bounded
), documents AS (
  SELECT jsonb_build_object('network_id',page.network_id,'display_name',page.record_json->'display_name',
    'aliases',page.record_json->'aliases','archived',page.record_json->'archived',
    'revision',page.record_revision,
    'directory_available',EXISTS(SELECT 1 FROM {candidate}.network_membership membership
      JOIN {candidate}.provider_location_binding location
        ON (location.provider_system,location.provider_id,location.location_id)=
          (membership.provider_system,membership.provider_id,membership.location_id)
      JOIN {addresses} address
        ON (address.location_key,address.entity_type,address.entity_id)=
          (location.location_key,location.entity_type,location.entity_id)
      WHERE membership.network_id=page.network_id),
    'priceable',NULL,'benefit_codes',(SELECT to_jsonb(benefit_codes) FROM declared_benefits
      WHERE network_id=page.network_id),
    'companies',coalesce((SELECT jsonb_agg(document ORDER BY document->>'company_id')
      FROM companies WHERE network_id=page.network_id),'[]'::jsonb),
    'source_bindings',coalesce((SELECT jsonb_agg(document ORDER BY document->>'binding_id')
      FROM bindings WHERE network_id=page.network_id),'[]'::jsonb)) AS document,
    page.network_id,page.record_json
  FROM page,bounds WHERE bounds.bounded
)
SELECT (SELECT count(*) FROM filtered)::bigint AS total,bounds.bounded,
  coalesce((SELECT jsonb_agg(document ORDER BY lower(record_json->>'display_name') COLLATE "C",network_id)
    FROM documents),'[]'::jsonb)::text AS rows_json FROM bounds
"""


_ORGANIZATION_SQL = _BASE_SQL.replace(
    "AND company.record_json->'archived'='false'::jsonb",
    "AND company.record_json->'archived'='false'::jsonb AND NOT company.record_key=ANY($11::text[])",
).replace(
    "'group_id',links.record_json->'group_id'",
    """'group_id',CASE WHEN NOT (links.record_json->>'group_id')=ANY($12::text[])
      AND EXISTS(SELECT 1 FROM approved target WHERE target.record_kind='group'
        AND target.record_key=links.record_json->>'group_id'
        AND target.record_json->'archived'='false'::jsonb)
      THEN links.record_json->'group_id' ELSE 'null'::jsonb END""",
)


def catalog_directory_relation(manifest, *, first_parameter):
    """Render addresses and consumed pins from an already authenticated catalog manifest."""
    if manifest.compact_layout is None:
        return _identifier(manifest.schema_name) + ".entity_address_unified", (), ()
    from api.network_compact_address import compact_address_native_sql, compact_address_read_relations
    from process.network_compact_layout import CompactAddressRelation

    pins = manifest.candidate_relation_pins
    if type(pins) is not tuple or any(type(pin) is not CompactAddressRelation for pin in pins):
        raise RegistryNetworkCatalogError("registry_network_catalog_source_changed")
    relations = []
    for name in ("network_membership", "provider_location_binding"):
        matches = [pin for pin in pins if pin.schema_name == manifest.schema_name and pin.table_name == name]
        if len(matches) != 1:
            raise RegistryNetworkCatalogError("registry_network_catalog_source_changed")
        relations.append(matches[0])
    # The enclosing EXISTS proves this network's exact office membership.
    addresses, parameters = compact_address_native_sql(
        manifest.compact_layout, first_parameter=first_parameter, visible_membership_sql="TRUE"
    )
    return addresses, parameters, tuple(relations) + compact_address_read_relations(manifest.compact_layout)


async def catalog_directory_fetchrow(connection, sql, parameters, relations):
    """Guard consumed compact relations on the page's original backend before exposing its result."""
    backend_pid = connection.get_server_pid() if relations else None
    entry = await connection.fetchrow(sql, *parameters)
    if relations:
        await verify_consumed_network_relations(connection, relations, expected_backend_pid=backend_pid)
    return entry


async def catalog_pin(connection, generation_id, control_schema):
    """Pin a verified catalog generation and approved membership in the caller-owned read-only snapshot."""
    if not connection.is_in_transaction():
        raise RegistryNetworkCatalogError("registry_network_catalog_transaction_required")
    transaction = await connection.fetchrow(
        "SELECT current_setting('transaction_isolation') AS isolation,"
        "current_setting('transaction_read_only') AS read_only"
    )
    if transaction["isolation"] not in ("repeatable read", "serializable") or transaction["read_only"] != "on":
        raise RegistryNetworkCatalogError("registry_network_catalog_transaction_required")
    manifest = await resolve_network_serving_manifest(
        connection, generation_id=generation_id, control_schema=control_schema
    )
    approved = await pin_retained_approved_membership_source(
        connection, approved_revision=manifest.approved_custom_revision, control_schema=control_schema
    )
    if manifest.source_generations.get("custom_membership") != approved.generation_id:
        raise RegistryNetworkCatalogError("registry_network_catalog_source_changed")
    seal_sha256 = getattr(manifest, "_custody_seal_sha256", None)
    if seal_sha256 is not None:
        if type(seal_sha256) is not str or re.fullmatch(r"[0-9a-f]{64}", seal_sha256) is None:
            raise RegistryNetworkCatalogError("registry_network_catalog_directory_unavailable")
        return manifest
    return await _verified_directory(connection, manifest, control_schema)


async def _directory_index_digest(connection, schema_name):
    """Match the publisher's index deparsing context without changing the caller."""
    original_search_path = await connection.fetchval("SELECT current_setting('search_path')")
    await connection.execute("SET LOCAL search_path TO pg_catalog,public")
    try:
        await _catalog_readiness(connection, schema_name)
        return await _index_digest(connection, schema_name, _serving_plan(schema_name))
    finally:
        await connection.execute("SELECT set_config('search_path',$1,true)", original_search_path)


async def _verified_directory(connection, manifest, control_schema):
    """Read immutable publication proof; full row parity stays in publication validation."""
    candidate = _identifier(manifest.schema_name)
    await connection.execute(
        f"LOCK TABLE {candidate}.network_membership,{candidate}.provider_location_binding,"
        f"{candidate}.entity_address_unified IN ACCESS SHARE MODE"
    )
    # Keep native identity checks under the same relation locks as the following page.
    locked_manifest = await resolve_network_serving_manifest(
        connection, generation_id=manifest.generation_id, control_schema=control_schema
    )
    if locked_manifest != manifest:
        raise RegistryNetworkCatalogError("registry_network_catalog_source_changed")
    expected = await connection.fetchrow(
        f"SELECT validation_json::text AS validation_json FROM {_identifier(control_schema)}.network_membership_candidate "
        "WHERE candidate_id=$1::uuid AND state='published'",
        UUID(manifest.candidate_id),
    )
    report = _closed_json(expected["validation_json"].encode(), MAX_RESPONSE_BYTES) if expected else None
    if type(report) is not dict or type(report.get("serving_readiness")) is not dict:
        raise RegistryNetworkCatalogError("registry_network_catalog_directory_unavailable")
    digest = await _directory_index_digest(connection, manifest.schema_name)
    if digest != report["serving_readiness"].get("index_definition_sha256"):
        raise RegistryNetworkCatalogError("registry_network_catalog_directory_unavailable")
    # shortcut: trusted owner/admin DML requires explicit full validation, not a catalog read.
    return manifest


_pin = catalog_pin


@asynccontextmanager
async def registry_catalog_read_snapshot(connection, generation_id, control_schema):
    """Reuse only a manifest verified on this connection in the active read scope."""
    manifest = await _pin(connection, generation_id, control_schema)
    token = _PINNED_READ.set((connection, manifest, control_schema))
    try:
        yield manifest
    finally:
        _PINNED_READ.reset(token)


def _reuse_manifest(connection, manifest, generation_id, control_schema):
    pinned = _PINNED_READ.get()
    if (
        pinned is None
        or pinned[0] is not connection
        or pinned[1] is not manifest
        or pinned[2] != control_schema
        or not connection.is_in_transaction()
        or (generation_id is not None and generation_id != manifest.generation_id)
    ):
        raise RegistryNetworkCatalogError("registry_network_catalog_source_changed")
    return manifest


async def _catalog_manifest(connection, generation_id, control_schema, manifest):
    """Require a verified active proof when reusing an already pinned manifest."""
    if manifest is None:
        return await _pin(connection, generation_id, control_schema)
    return _reuse_manifest(connection, manifest, generation_id, control_schema)


def _company_document(company):
    fields = {"company_id", "display_name", "roles", "revision", "link_revision", "group_id"}
    if type(company) is not dict or set(company) != fields:
        raise RegistryNetworkCatalogError("registry_network_catalog_document_invalid")
    _uuid(company["company_id"])
    _text(company["display_name"], 512)
    _integer(company["revision"], 9223372036854775807)
    _integer(company["link_revision"], 9223372036854775807)
    if company["group_id"] is not None:
        _uuid(company["group_id"])
    roles = company["roles"]
    if (
        type(roles) is not list
        or not 1 <= len(roles) <= len(COMPANY_ROLES)
        or any(type(role) is not str or role not in COMPANY_ROLES for role in roles)
        or len(set(roles)) != len(roles)
    ):
        raise RegistryNetworkCatalogError("registry_network_catalog_document_invalid")


def catalog_binding_document(binding, *, network_id=None):
    """Validate closed coordinates and evidence, requiring the enclosing target for a product scope."""
    coordinates = {"source_system", "source_id", "dataset_schema", "dataset_id", "producer_id", "edition_id"}
    fields = coordinates | {
        "namespace",
        "binding_id",
        "revision",
        "source_key",
        "source_scope",
        "evidence_id",
        "evidence_sha256",
    }
    if type(binding) is not dict or set(binding) != fields or binding["namespace"] != "source_binding":
        raise RegistryNetworkCatalogError("registry_network_catalog_document_invalid")
    _uuid(binding["binding_id"])
    _integer(binding["revision"], 9223372036854775807)
    RegistryNetworkSourceCoordinates(**{key: binding[key] for key in coordinates})
    _text(binding["source_key"], 512)
    _text(binding["evidence_id"], 512)
    if type(binding["evidence_sha256"]) is not str or re.fullmatch(r"[0-9a-f]{64}", binding["evidence_sha256"]) is None:
        raise RegistryNetworkCatalogError("registry_network_catalog_document_invalid")
    _source_scope(binding["source_system"], binding["source_scope"], producer_id=binding["producer_id"])
    scope = binding["source_scope"]
    if "insurance_plan_id" in scope and (
        binding["source_id"] == "cms-npd"
        or binding["source_key"] != "InsurancePlan/" + scope["insurance_plan_id"]
        or type(network_id) is not int
        or scope["product_network_id"] != network_id
    ):
        raise RegistryNetworkCatalogError("registry_network_catalog_document_invalid")


_binding_document = catalog_binding_document


def catalog_network_document(document):
    """Validate the closed network response and its nested companies and source bindings."""
    fields = {
        "network_id",
        "display_name",
        "aliases",
        "archived",
        "revision",
        "directory_available",
        "priceable",
        "benefit_codes",
        "companies",
        "source_bindings",
    }
    if type(document) is not dict or set(document) != fields:
        raise RegistryNetworkCatalogError("registry_network_catalog_document_invalid")
    _integer(document["network_id"], 2147483647)
    _integer(document["revision"], 9223372036854775807)
    _text(document["display_name"], 512)
    if type(document["aliases"]) is not list or len(document["aliases"]) > 100:
        raise RegistryNetworkCatalogError("registry_network_catalog_document_invalid")
    for alias in document["aliases"]:
        _text(alias, 512)
    if len(set(document["aliases"])) != len(document["aliases"]):
        raise RegistryNetworkCatalogError("registry_network_catalog_document_invalid")
    if type(document["archived"]) is not bool or type(document["directory_available"]) is not bool:
        raise RegistryNetworkCatalogError("registry_network_catalog_document_invalid")
    codes = document["benefit_codes"]
    if codes is not None and (
        type(codes) is not list or not codes or codes != [code for code in _BENEFIT_CODES if code in codes]
    ):
        raise RegistryNetworkCatalogError("registry_network_catalog_document_invalid")
    if document["priceable"] is not None:
        raise RegistryNetworkCatalogError("registry_network_catalog_unproven_capability")
    if type(document["companies"]) is not list or type(document["source_bindings"]) is not list:
        raise RegistryNetworkCatalogError("registry_network_catalog_document_invalid")
    for company in document["companies"]:
        _company_document(company)
    for binding in document["source_bindings"]:
        _binding_document(binding, network_id=document["network_id"])
    for entries, identity in ((document["companies"], "company_id"), (document["source_bindings"], "binding_id")):
        if len({entry[identity] for entry in entries}) != len(entries):
            raise RegistryNetworkCatalogError("registry_network_catalog_document_invalid")
    return document


_network_document = catalog_network_document


async def _page(connection, manifest, query, exclusions, control_schema, network_ids=None, organization_policy=None):
    if organization_policy is not None and type(organization_policy) is not RegistryOrganizationVisibility:
        raise RegistryNetworkCatalogError("registry_network_catalog_policy_invalid")
    sql = _BASE_SQL if organization_policy is None else _ORGANIZATION_SQL
    parameters = (
        manifest.approved_custom_revision,
        exclusions,
        query.archived,
        query.search,
        query.source.sql_parameters if query.source else None,
        network_ids,
        query.offset,
        query.limit,
        MAX_RESPONSE_BYTES,
        query.benefit_code,
    )
    if organization_policy is not None:
        parameters += (organization_policy.excluded_company_ids, organization_policy.excluded_group_ids)
    addresses, address_parameters, relations = catalog_directory_relation(manifest, first_parameter=len(parameters) + 1)
    entry = await catalog_directory_fetchrow(
        connection,
        sql.format(
            control=_identifier(control_schema), candidate=_identifier(manifest.schema_name), addresses=addresses
        ),
        parameters + address_parameters,
        relations,
    )
    if entry is None or entry["bounded"] is not True:
        raise RegistryNetworkCatalogError("registry_network_catalog_response_bounds")
    documents = _closed_json(entry["rows_json"].encode(), MAX_RESPONSE_BYTES)
    if type(documents) is not list or len(documents) > query.limit:
        raise RegistryNetworkCatalogError("registry_network_catalog_document_invalid")
    networks = tuple(_network_document(document) for document in documents)
    if query.benefit_code is not None and any(
        query.benefit_code not in (network["benefit_codes"] or []) for network in networks
    ):
        raise RegistryNetworkCatalogError("registry_network_catalog_document_invalid")
    if organization_policy is not None and any(
        company["company_id"] in organization_policy.excluded_company_ids
        or company["group_id"] in organization_policy.excluded_group_ids
        for network in networks
        for company in network["companies"]
    ):
        raise RegistryNetworkCatalogError("registry_network_catalog_policy_invalid")
    total = _integer(entry["total"], 9223372036854775807, minimum=0)
    if len(networks) != min(query.limit, max(total - query.offset, 0)) or len(
        {network["network_id"] for network in networks}
    ) != len(networks):
        raise RegistryNetworkCatalogError("registry_network_catalog_document_invalid")
    if any(network["network_id"] in exclusions for network in networks):
        raise RegistryNetworkCatalogError("registry_network_catalog_policy_invalid")
    return {
        "generation": str(manifest.generation_id),
        "approved_custom_revision": str(manifest.approved_custom_revision),
        "total": total,
        "offset": query.offset,
        "limit": query.limit,
        "items": networks,
    }


async def read_registry_network_catalog(
    connection, query, *, excluded_network_ids, organization_policy=None, control_schema=None
):
    """Read authorized approved totals and pages without opening a transaction."""
    if type(query) is not RegistryNetworkCatalogQuery:
        raise RegistryNetworkCatalogError("registry_network_catalog_request_invalid")
    exclusions = _exclusions(excluded_network_ids)
    control_schema = control_schema if control_schema is not None else registry_schema()
    _identifier(control_schema)
    try:
        manifest = await _pin(connection, query.generation_id, control_schema)
        return await _page(
            connection, manifest, query, exclusions, control_schema, organization_policy=organization_policy
        )
    except (NetworkReadIdentityError, asyncpg.PostgresError, KeyError, TypeError, ValueError) as error:
        if isinstance(error, RegistryNetworkCatalogError):
            raise
        raise RegistryNetworkCatalogError("registry_network_catalog_unavailable") from error


async def read_registry_network_catalog_detail(
    connection, network_id, *, excluded_network_ids, organization_policy=None, generation_id=None, control_schema=None
):
    """Return one permitted exact canonical identity or a value-free refusal."""
    _integer(network_id, 2147483647)
    exclusions = _exclusions(excluded_network_ids)
    if network_id in exclusions:
        raise RegistryNetworkCatalogError("registry_network_catalog_detail_denied")
    query = RegistryNetworkCatalogQuery(limit=1, generation_id=generation_id, archived=None)
    control_schema = control_schema if control_schema is not None else registry_schema()
    _identifier(control_schema)
    try:
        manifest = await _pin(connection, generation_id, control_schema)
        page = await _page(connection, manifest, query, exclusions, control_schema, (network_id,), organization_policy)
        if page["total"] != 1 or len(page["items"]) != 1 or page["items"][0]["network_id"] != network_id:
            raise RegistryNetworkCatalogError("registry_network_catalog_detail_denied")
        return page
    except (NetworkReadIdentityError, asyncpg.PostgresError, KeyError, TypeError, ValueError) as error:
        if isinstance(error, RegistryNetworkCatalogError):
            raise
        raise RegistryNetworkCatalogError("registry_network_catalog_unavailable") from error


async def resolve_registry_network_catalog_legacy(
    connection,
    selector,
    *,
    excluded_network_ids,
    organization_policy=None,
    generation_id=None,
    control_schema=None,
    manifest=None,
):
    """Resolve an exact approved source-scoped legacy reference, never a name."""
    if type(selector) is RegistryNetworkCatalogLegacyBatchSelector:
        return await _resolve_legacy_batch(
            connection,
            selector,
            excluded_network_ids=excluded_network_ids,
            organization_policy=organization_policy,
            generation_id=generation_id,
            control_schema=control_schema,
            manifest=manifest,
        )
    if type(selector) is not RegistryNetworkCatalogLegacySelector:
        raise RegistryNetworkCatalogError("registry_network_catalog_request_invalid")
    exclusions = _exclusions(excluded_network_ids)
    query = RegistryNetworkCatalogQuery(limit=1, generation_id=generation_id, archived=None)
    control_schema = control_schema if control_schema is not None else registry_schema()
    control = _identifier(control_schema)
    try:
        manifest = await _catalog_manifest(connection, generation_id, control_schema, manifest)
        match = await connection.fetchrow(
            _LEGACY_SCOPE_SQL.format(control=control),
            manifest.approved_custom_revision,
            selector.source.sql_parameters,
            selector.source_scope_json.decode("utf-8"),
            exclusions,
        )
        if match is None or match["ambiguous"] is not False or match["permitted_count"] != 1:
            raise RegistryNetworkCatalogError("registry_network_catalog_detail_denied")
        network_id = _integer(match["network_id"], 2147483647)
        page = await _page(connection, manifest, query, exclusions, control_schema, (network_id,), organization_policy)
        if page["total"] != 1 or page["items"][0]["network_id"] != network_id:
            raise RegistryNetworkCatalogError("registry_network_catalog_detail_denied")
        return page
    except (NetworkReadIdentityError, asyncpg.PostgresError, KeyError, TypeError, ValueError) as error:
        if isinstance(error, RegistryNetworkCatalogError):
            raise
        raise RegistryNetworkCatalogError("registry_network_catalog_unavailable") from error


_LEGACY_SCOPE_SQL = """WITH matches AS MATERIALIZED (
              SELECT DISTINCT (binding.record_json->>'network_id')::integer AS network_id
              FROM {control}.registry_approved_record binding
              JOIN {control}.registry_approved_record network
                ON network.approved_revision=binding.approved_revision AND network.record_kind='network'
                AND network.record_key=binding.record_json->>'network_id'
              WHERE binding.approved_revision=$1 AND binding.record_kind='network_binding'
                AND binding.record_json->'archived'='false'::jsonb
                AND network.record_json->'archived'='false'::jsonb
                AND ARRAY[binding.record_json->>'source_system',binding.record_json->>'source_id',
                  binding.record_json->>'dataset_schema',binding.record_json->>'dataset_id',
                  binding.record_json->>'producer_id',binding.record_json->>'edition_id']=$2::text[]
                AND binding.record_json->'source_scope_json'=$3::jsonb
            ), permitted AS (
              SELECT network_id FROM matches WHERE NOT network_id=ANY($4::integer[])
            ) SELECT EXISTS(SELECT 1 FROM matches OFFSET 1) AS ambiguous,
              count(*)::bigint AS permitted_count,min(network_id) AS network_id FROM permitted"""

_LEGACY_BATCH_SQL = """WITH selected AS (SELECT unnest($2::text[]) AS value), matches AS MATERIALIZED (
  SELECT DISTINCT selected.value,(binding.record_json->>'network_id')::integer AS network_id
  FROM selected JOIN {control}.registry_approved_record binding ON binding.approved_revision=$1
    AND binding.record_kind='network_binding' AND binding.record_json->'archived'='false'::jsonb
  JOIN {control}.registry_approved_record network ON network.approved_revision=$1
    AND network.record_kind='network' AND network.record_key=binding.record_json->>'network_id'
    AND network.record_json->'archived'='false'::jsonb
  WHERE ($3='checksum_network' AND binding.record_json->>'source_system'='aca'
      AND jsonb_typeof(binding.record_json->'source_scope_json'->'checksum_network')='number'
      AND binding.record_json->'source_scope_json'->>'checksum_network'=selected.value)
    OR ($3='legacy_fhir_uuid' AND binding.record_json->>'source_system'='fhir'
      AND jsonb_typeof(binding.record_json->'source_scope_json'->'legacy_uuid')='string'
      AND binding.record_json->'source_scope_json'->>'legacy_uuid'=selected.value)
), resolved AS (
  SELECT selected.value,count(DISTINCT matches.network_id) AS targets,min(matches.network_id) AS network_id
  FROM selected LEFT JOIN matches USING(value) GROUP BY selected.value
)
SELECT bool_and(targets=1 AND NOT network_id=ANY($4::integer[])) AS complete,
  array_agg(DISTINCT network_id ORDER BY network_id) FILTER(WHERE targets=1) AS network_ids FROM resolved"""


async def _resolve_legacy_batch(
    connection, selector, *, excluded_network_ids, organization_policy, generation_id, control_schema, manifest=None
):
    """Require unique global mappings before excluding any client-hidden target."""
    exclusions = _exclusions(excluded_network_ids)
    control_schema = control_schema if control_schema is not None else registry_schema()
    control = _identifier(control_schema)
    try:
        manifest = await _catalog_manifest(connection, generation_id, control_schema, manifest)
        matches = await connection.fetchrow(
            _LEGACY_BATCH_SQL.format(control=control),
            manifest.approved_custom_revision,
            selector.values,
            selector.namespace,
            exclusions,
        )
        if matches is None or matches["complete"] is not True:
            raise RegistryNetworkCatalogError("registry_network_catalog_detail_denied")
        network_ids = tuple(matches["network_ids"])
        if (
            not 1 <= len(network_ids) <= len(selector.values)
            or any(type(network_id) is not int or not 0 < network_id <= 2147483647 for network_id in network_ids)
            or tuple(sorted(set(network_ids))) != network_ids
        ):
            raise RegistryNetworkCatalogError("registry_network_catalog_document_invalid")
        query = RegistryNetworkCatalogQuery(limit=len(network_ids), generation_id=generation_id)
        page = await _page(connection, manifest, query, exclusions, control_schema, network_ids, organization_policy)
        if page["total"] != len(network_ids) or {network["network_id"] for network in page["items"]} != set(
            network_ids
        ):
            raise RegistryNetworkCatalogError("registry_network_catalog_detail_denied")
        return page
    except (NetworkReadIdentityError, asyncpg.PostgresError, KeyError, TypeError, ValueError) as error:
        if isinstance(error, RegistryNetworkCatalogError):
            raise
        raise RegistryNetworkCatalogError("registry_network_catalog_unavailable") from error


@dataclass(frozen=True)
class RegistryNetworkAutocompleteQuery:
    """Closed display search with one optional retained generation and ZIP."""

    query: str
    limit: int = 10
    zip_code: str | None = None
    generation_id: int | None = None

    def __post_init__(self):
        import unicodedata

        if (
            type(self.query) is not str
            or not 1 <= len(self.query) <= 128
            or self.query.strip() != self.query
            or any(unicodedata.category(char) == "Cc" for char in self.query)
        ):
            raise RegistryNetworkCatalogError("registry_network_catalog_request_invalid")
        try:
            self.query.encode("utf-8")
        except UnicodeError:
            raise RegistryNetworkCatalogError("registry_network_catalog_request_invalid") from None
        _integer(self.limit, 50)
        if self.generation_id is not None:
            _integer(self.generation_id, 9223372036854775807)
        if self.zip_code is not None and (
            type(self.zip_code) is not str or re.fullmatch(r"[0-9]{5}", self.zip_code) is None
        ):
            raise RegistryNetworkCatalogError("registry_network_catalog_request_invalid")


_AUTOCOMPLETE_SQL = """WITH bindings AS MATERIALIZED (
  SELECT binding.record_json->'source_scope_json' AS scope,
    (binding.record_json->>'network_id')::integer AS network_id
  FROM {control}.registry_approved_record binding JOIN {control}.registry_approved_record network
    ON network.approved_revision=binding.approved_revision AND network.record_kind='network'
    AND network.record_key=binding.record_json->>'network_id' AND network.record_json->'archived'='false'::jsonb
  WHERE binding.approved_revision=$1 AND binding.record_kind='network_binding'
    AND binding.record_json->'archived'='false'::jsonb AND binding.record_json->>'source_system'='aca'
    AND jsonb_typeof(binding.record_json->'source_scope_json'->'checksum_network')='number'
), unique_checksums AS MATERIALIZED (
  SELECT (scope->>'checksum_network')::bigint AS checksum,min(network_id) AS network_id
  FROM bindings GROUP BY (scope->>'checksum_network')::bigint HAVING count(DISTINCT network_id)=1
), qualified AS MATERIALIZED (
  SELECT plan.plan_id,plan.year,plan.issuer_id,plan.state,plan.marketing_name,tier.checksum_network,tier.network_tier
  FROM {plans} plan JOIN {tiers} tier ON tier.plan_id=plan.plan_id AND tier.year=plan.year
    AND tier.issuer_id=plan.issuer_id
  WHERE (plan.marketing_name ILIKE $3 OR plan.plan_id ILIKE $3)
    AND ($5::text[] IS NULL OR plan.state=ANY($5::text[]))
    AND tier.checksum_network BETWEEN -2147483648 AND 2147483647
    AND EXISTS(SELECT 1 FROM bindings binding JOIN unique_checksums unique_checksum
      ON unique_checksum.checksum=(binding.scope->>'checksum_network')::bigint
      WHERE NOT unique_checksum.network_id=ANY($2::integer[])
        AND binding.scope->>'plan_id'=plan.plan_id AND (binding.scope->>'plan_year')::integer=plan.year
        AND (binding.scope->>'issuer_id')::integer=plan.issuer_id AND binding.scope->>'state'=plan.state
        AND (binding.scope->>'checksum_network')::bigint=tier.checksum_network)
), page AS MATERIALIZED (
  SELECT * FROM (SELECT DISTINCT plan_id,year,issuer_id,state,marketing_name FROM qualified) eligible
  ORDER BY plan_id COLLATE "C",year,issuer_id,state LIMIT $4
), selected_tiers AS MATERIALIZED (
  SELECT qualified.* FROM qualified JOIN page USING(plan_id,year,issuer_id,state)
), choice AS (SELECT count(*)::integer AS plan_count,NOT $7::boolean OR count(*)=$4 AS emit FROM page),
bounds AS (
  SELECT CASE WHEN NOT emit THEN true ELSE
    coalesce((SELECT sum(octet_length(to_json(plan_id)::text)+octet_length(to_json(state)::text)
      +coalesce(octet_length(to_json(marketing_name)::text),4)+32) FROM page),0)
    +coalesce((SELECT sum(octet_length(to_json(checksum_network::text)::text)
      +coalesce(octet_length(to_json(network_tier)::text),4)+4) FROM selected_tiers),0)
    +$4::bigint*1024 <= $6 END AS bounded,emit,plan_count FROM choice
), checksum_maps AS (
  SELECT plan_id,year,issuer_id,state,jsonb_object_agg(checksum_network::text,network_tier) AS checksums
  FROM selected_tiers,bounds WHERE bounded AND emit GROUP BY plan_id,year,issuer_id,state
), documents AS (
  SELECT page.plan_id,page.year,jsonb_build_object('plan_id',page.plan_id,'marketing_name',page.marketing_name,
    'state',page.state,'year',page.year,'network_checksum',checksum_maps.checksums) AS document
  FROM page JOIN checksum_maps USING(plan_id,year,issuer_id,state)
)
SELECT plan_count AS plan_count,bounded,
  coalesce((SELECT jsonb_agg(document ORDER BY plan_id COLLATE "C",year) FROM documents),'[]'::jsonb)::text
    AS plans_json FROM bounds"""


def _autocomplete_table(model):
    """Quote only the source model's server-owned physical table identity."""
    return _identifier(str(model.__table__.schema)) + "." + _identifier(str(model.__table__.name))


async def _autocomplete_zip_states(connection, zip_code):
    """Probe optional lookup tables in savepoints without aborting the pinned read."""
    for model, column, selector, selected in (
        (GeoZipLookup, "state", "zip_code", zip_code),
        (PlanRatingAreas, "state", "zip3", zip_code[:3]),
        (ZipState, "stusps", "zip", zip_code),
    ):
        try:
            async with connection.transaction():
                states = await connection.fetch(
                    f"SELECT DISTINCT {_identifier(column)} AS state FROM {_autocomplete_table(model)} "
                    f"WHERE {_identifier(selector)}=$1 AND {_identifier(column)} IS NOT NULL "
                    f"ORDER BY {_identifier(column)} LIMIT $2",
                    selected,
                    MAX_PAGE + 1,
                )
        except asyncpg.UndefinedTableError:
            continue
        if len(states) > MAX_PAGE:
            raise RegistryNetworkCatalogError("registry_network_catalog_response_bounds")
        if states:
            return tuple(entry["state"] for entry in states)
    return ()


def _autocomplete_document(document):
    fields = {"plan_id", "marketing_name", "state", "year", "network_checksum"}
    if type(document) is not dict or set(document) != fields:
        raise RegistryNetworkCatalogError("registry_network_catalog_document_invalid")
    _text(document["plan_id"], 17)
    _text(document["state"], 2)
    _integer(document["year"], 2100, minimum=2010)
    if document["marketing_name"] is not None and type(document["marketing_name"]) is not str:
        raise RegistryNetworkCatalogError("registry_network_catalog_document_invalid")
    checksums = document["network_checksum"]
    if type(checksums) is not dict or not checksums:
        raise RegistryNetworkCatalogError("registry_network_catalog_document_invalid")
    for checksum, tier in checksums.items():
        if str(int(checksum)) != checksum or not -2147483648 <= int(checksum) <= 2147483647:
            raise RegistryNetworkCatalogError("registry_network_catalog_document_invalid")
        if tier is not None and type(tier) is not str:
            raise RegistryNetworkCatalogError("registry_network_catalog_document_invalid")
    return document


async def _autocomplete_page(connection, manifest, query, exclusions, control_schema, states):
    entry = await connection.fetchrow(
        _AUTOCOMPLETE_SQL.format(
            control=_identifier(control_schema),
            plans=_autocomplete_table(Plan),
            tiers=_autocomplete_table(PlanNetworkTierRaw),
        ),
        manifest.approved_custom_revision,
        exclusions,
        "%" + query.query.lower() + "%",
        query.limit,
        states,
        MAX_RESPONSE_BYTES,
        states is not None,
    )
    if entry is None or entry["bounded"] is not True:
        raise RegistryNetworkCatalogError("registry_network_catalog_response_bounds")
    count = _integer(entry["plan_count"], query.limit, minimum=0)
    if states is not None and count < query.limit:
        return None
    plans = _closed_json(entry["plans_json"].encode(), MAX_RESPONSE_BYTES)
    if type(plans) is not list or len(plans) != count:
        raise RegistryNetworkCatalogError("registry_network_catalog_document_invalid")
    return [_autocomplete_document(document) for document in plans]


async def read_registry_network_autocomplete(connection, query, *, excluded_network_ids, control_schema=None):
    """Search source-qualified visible metadata in one pinned request snapshot."""
    if type(query) is not RegistryNetworkAutocompleteQuery:
        raise RegistryNetworkCatalogError("registry_network_catalog_request_invalid")
    exclusions = _exclusions(excluded_network_ids)
    control_schema = control_schema if control_schema is not None else registry_schema()
    _identifier(control_schema)
    try:
        manifest = await _pin(connection, query.generation_id, control_schema)
        states = await _autocomplete_zip_states(connection, query.zip_code) if query.zip_code is not None else None
        plans = await _autocomplete_page(connection, manifest, query, exclusions, control_schema, states)
        if plans is None:
            plans = await _autocomplete_page(connection, manifest, query, exclusions, control_schema, None)
        return {
            "generation": str(manifest.generation_id),
            "approved_custom_revision": str(manifest.approved_custom_revision),
            "plans": plans,
        }
    except (asyncpg.PostgresError, KeyError, TypeError, ValueError) as error:
        if isinstance(error, RegistryNetworkCatalogError):
            raise
        raise RegistryNetworkCatalogError("registry_network_catalog_unavailable") from error
