# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded approved network metadata reads on a caller-owned pinned transaction."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from uuid import UUID

import asyncpg

from db.registry_schema import registry_schema
from process.network_address_projection import _identifier, _membership_accounting
from process.network_approved_membership_source import (
    pin_retained_approved_membership_source,
)
from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
from process.network_serving_read import resolve_network_serving_manifest

MAX_PAGE = 100
MAX_OFFSET = 1_000_000
MAX_EXCLUSIONS = 50_000
MAX_RESPONSE_BYTES = 1_048_576


class RegistryNetworkCatalogError(ValueError):
    """The request, retained proof or approved metadata is unavailable."""


def _integer(number, maximum, *, minimum=1):
    if type(number) is not int or not minimum <= number <= maximum:
        raise RegistryNetworkCatalogError("registry_network_catalog_request_invalid")
    return number


def _text(text, maximum):
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


def _uuid(identifier):
    try:
        parsed = UUID(identifier) if type(identifier) is str else None
    except ValueError:
        parsed = None
    if parsed is None or not parsed.int or str(parsed) != identifier:
        raise RegistryNetworkCatalogError("registry_network_catalog_document_invalid")
    return identifier


def _source_scope(system, scope):
    if type(scope) is not dict:
        raise RegistryNetworkCatalogError("registry_network_catalog_document_invalid")
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
    elif system == "fhir" and set(scope) == {
        "organization_id",
        "legacy_uuid",
        "alias_scope",
    }:
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


def _exclusions(network_ids):
    if type(network_ids) is not tuple or len(network_ids) > MAX_EXCLUSIONS:
        raise RegistryNetworkCatalogError("registry_network_catalog_policy_invalid")
    previous = 0
    for network_id in network_ids:
        _integer(network_id, 2147483647)
        if network_id <= previous:
            raise RegistryNetworkCatalogError("registry_network_catalog_policy_invalid")
        previous = network_id
    return network_ids


@dataclass(frozen=True)
class RegistryNetworkCatalogQuery:
    limit: int = 25
    offset: int = 0
    generation_id: int | None = None
    search: str | None = None
    archived: bool | None = False
    source: RegistryNetworkSourceCoordinates | None = None

    def __post_init__(self):
        _integer(self.limit, MAX_PAGE)
        _integer(self.offset, MAX_OFFSET, minimum=0)
        if self.generation_id is not None:
            _integer(self.generation_id, 9223372036854775807)
        if self.search is not None:
            _text(self.search, 512)
        if self.archived is not None and type(self.archived) is not bool:
            raise RegistryNetworkCatalogError("registry_network_catalog_request_invalid")
        if self.source is not None and type(self.source) is not RegistryNetworkSourceCoordinates:
            raise RegistryNetworkCatalogError("registry_network_catalog_request_invalid")


def _closed_json(encoded, maximum):
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
        _source_scope(self.source.source_system, scope)
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


_BASE_SQL = """
WITH approved AS MATERIALIZED (
  SELECT record_kind,record_key,record_revision,record_json
  FROM {control}.registry_approved_record WHERE approved_revision=$1
), visible AS MATERIALIZED (
  SELECT record_key::integer AS network_id,record_revision,record_json FROM approved
  WHERE record_kind='network' AND NOT record_key::integer=ANY($2::integer[])
), filtered AS MATERIALIZED (
  SELECT * FROM visible network
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
    AND ($6::integer IS NULL OR network_id=$6)
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
      JOIN {candidate}.entity_address_unified address
        ON (address.location_key,address.entity_type,address.entity_id)=
          (location.location_key,location.entity_type,location.entity_id)
      WHERE membership.network_id=page.network_id),
    'priceable',NULL,'benefit_codes',NULL,
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


async def _pin(connection, generation_id, control_schema):
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
    candidate = _identifier(manifest.schema_name)
    counts = await _membership_accounting(
        connection,
        candidate + ".network_membership",
        candidate + ".provider_location_binding",
        candidate + ".entity_address_unified",
    )
    expected = await connection.fetchrow(
        f"SELECT validation_json::text AS validation_json FROM {_identifier(control_schema)}.network_membership_candidate "
        "WHERE candidate_id=$1::uuid AND state='published'",
        UUID(manifest.candidate_id),
    )
    report = _closed_json(expected["validation_json"].encode(), MAX_RESPONSE_BYTES) if expected else None
    if type(report) is not dict or any(
        type(report.get(key)) is not int or counts[key] != report[key]
        for key in ("membership_rows", "distinct_memberships", "projected_locations", "orphan_bindings")
    ):
        raise RegistryNetworkCatalogError("registry_network_catalog_directory_unavailable")
    parity = await connection.fetchrow(
        f"""WITH expected AS (
          SELECT location.location_key,array_agg(DISTINCT membership.network_id ORDER BY membership.network_id) AS ids
          FROM {candidate}.network_membership membership JOIN {candidate}.provider_location_binding location
            ON (location.provider_system,location.provider_id,location.location_id)=
              (membership.provider_system,membership.provider_id,membership.location_id)
          GROUP BY location.location_key)
        SELECT count(*)::bigint AS address_rows,count(*) FILTER(WHERE
          address.canonical_network_ids IS DISTINCT FROM coalesce(expected.ids,'{{}}'::integer[]))::bigint AS changed_arrays
        FROM {candidate}.entity_address_unified address LEFT JOIN expected USING(location_key)"""
    )
    if parity["changed_arrays"] != 0 or parity["address_rows"] != report["candidate_readiness"]["address_rows"]:
        raise RegistryNetworkCatalogError("registry_network_catalog_directory_unavailable")
    return manifest


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
        or not 1 <= len(roles) <= 3
        or any(type(role) is not str or role not in {"insurer", "employer", "network_operator"} for role in roles)
        or len(set(roles)) != len(roles)
    ):
        raise RegistryNetworkCatalogError("registry_network_catalog_document_invalid")


def _binding_document(binding):
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
    _source_scope(binding["source_system"], binding["source_scope"])


def _network_document(document):
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
    if document["priceable"] is not None or document["benefit_codes"] is not None:
        raise RegistryNetworkCatalogError("registry_network_catalog_unproven_capability")
    if type(document["companies"]) is not list or type(document["source_bindings"]) is not list:
        raise RegistryNetworkCatalogError("registry_network_catalog_document_invalid")
    for company in document["companies"]:
        _company_document(company)
    for binding in document["source_bindings"]:
        _binding_document(binding)
    for entries, identity in ((document["companies"], "company_id"), (document["source_bindings"], "binding_id")):
        if len({entry[identity] for entry in entries}) != len(entries):
            raise RegistryNetworkCatalogError("registry_network_catalog_document_invalid")
    return document


async def _page(connection, manifest, query, exclusions, control_schema, network_id=None):
    entry = await connection.fetchrow(
        _BASE_SQL.format(control=_identifier(control_schema), candidate=_identifier(manifest.schema_name)),
        manifest.approved_custom_revision,
        exclusions,
        query.archived,
        query.search,
        query.source.sql_parameters if query.source else None,
        network_id,
        query.offset,
        query.limit,
        MAX_RESPONSE_BYTES,
    )
    if entry is None or entry["bounded"] is not True:
        raise RegistryNetworkCatalogError("registry_network_catalog_response_bounds")
    documents = _closed_json(entry["rows_json"].encode(), MAX_RESPONSE_BYTES)
    if type(documents) is not list or len(documents) > query.limit:
        raise RegistryNetworkCatalogError("registry_network_catalog_document_invalid")
    networks = tuple(_network_document(document) for document in documents)
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


async def read_registry_network_catalog(connection, query, *, excluded_network_ids, control_schema=None):
    """Read authorized approved totals and pages without opening a transaction."""
    if type(query) is not RegistryNetworkCatalogQuery:
        raise RegistryNetworkCatalogError("registry_network_catalog_request_invalid")
    exclusions = _exclusions(excluded_network_ids)
    control_schema = control_schema if control_schema is not None else registry_schema()
    _identifier(control_schema)
    try:
        manifest = await _pin(connection, query.generation_id, control_schema)
        return await _page(connection, manifest, query, exclusions, control_schema)
    except (asyncpg.PostgresError, KeyError, TypeError, ValueError) as error:
        if isinstance(error, RegistryNetworkCatalogError):
            raise
        raise RegistryNetworkCatalogError("registry_network_catalog_unavailable") from error


async def read_registry_network_catalog_detail(
    connection, network_id, *, excluded_network_ids, generation_id=None, control_schema=None
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
        page = await _page(connection, manifest, query, exclusions, control_schema, network_id)
        if page["total"] != 1 or len(page["items"]) != 1 or page["items"][0]["network_id"] != network_id:
            raise RegistryNetworkCatalogError("registry_network_catalog_detail_denied")
        return page
    except (asyncpg.PostgresError, KeyError, TypeError, ValueError) as error:
        if isinstance(error, RegistryNetworkCatalogError):
            raise
        raise RegistryNetworkCatalogError("registry_network_catalog_unavailable") from error


async def resolve_registry_network_catalog_legacy(
    connection, selector, *, excluded_network_ids, generation_id=None, control_schema=None
):
    """Resolve an exact approved source-scoped legacy reference, never a name."""
    if type(selector) is not RegistryNetworkCatalogLegacySelector:
        raise RegistryNetworkCatalogError("registry_network_catalog_request_invalid")
    exclusions = _exclusions(excluded_network_ids)
    query = RegistryNetworkCatalogQuery(limit=1, generation_id=generation_id, archived=None)
    control_schema = control_schema if control_schema is not None else registry_schema()
    control = _identifier(control_schema)
    try:
        manifest = await _pin(connection, generation_id, control_schema)
        match = await connection.fetchrow(
            f"""WITH matches AS MATERIALIZED (
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
              count(*)::bigint AS permitted_count,min(network_id) AS network_id FROM permitted""",
            manifest.approved_custom_revision,
            selector.source.sql_parameters,
            selector.source_scope_json.decode("utf-8"),
            exclusions,
        )
        if match is None or match["ambiguous"] is not False or match["permitted_count"] != 1:
            raise RegistryNetworkCatalogError("registry_network_catalog_detail_denied")
        network_id = _integer(match["network_id"], 2147483647)
        page = await _page(connection, manifest, query, exclusions, control_schema, network_id)
        if page["total"] != 1 or page["items"][0]["network_id"] != network_id:
            raise RegistryNetworkCatalogError("registry_network_catalog_detail_denied")
        return page
    except (asyncpg.PostgresError, KeyError, TypeError, ValueError) as error:
        if isinstance(error, RegistryNetworkCatalogError):
            raise
        raise RegistryNetworkCatalogError("registry_network_catalog_unavailable") from error
