# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pinned, indexed FHIR metadata reads with bounded visible relationships."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import asdict, dataclass, replace
from datetime import date, datetime
from uuid import UUID

import asyncpg

from db.plan_release_storage import storage_identifier
from db.registry_fhir_catalog import MAX_CORE_BYTES, NESTED_LIMIT, SNAPSHOT_COLUMNS, VIEW
from db.registry_schema import registry_schema
from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
from process.registry_catalog_policy import RegistryOrganizationVisibility
from process.registry_network_catalog_read import (
    MAX_OFFSET,
    RegistryNetworkCatalogError,
    RegistryNetworkCatalogQuery,
    catalog_binding_document,
    catalog_closed_json,
    catalog_exclusions,
    catalog_integer,
    catalog_network_document,
    catalog_pin,
    catalog_text,
    catalog_uuid,
)

_DIGEST = re.compile(r"[0-9a-f]{64}\Z")
_FHIR_ID = re.compile(r"[A-Za-z0-9.-]{1,64}\Z")
_RELATIONSHIP_FIELDS = {
    "relationship_id",
    "source_id",
    "plan",
    "organization_reference",
    "company_role",
    "company",
    "group",
    "resolution_status",
    "resolution_reason",
}
_BINDING_METADATA = {"metadata_sha256", "accepted_at", "status"}
_NETWORK_FIELDS = {
    "network_id",
    "display_name",
    "aliases",
    "archived",
    "revision",
    "directory_available",
    "priceable",
    "benefit_codes",
    "matching_relationships",
    "source_bindings",
}


def _invalid(reason="document_invalid"):
    raise RegistryNetworkCatalogError("registry_network_catalog_" + reason)


def _digest(encoded):
    if type(encoded) is not str or _DIGEST.fullmatch(encoded) is None:
        _invalid()
    return encoded


def _search_text(text):
    if type(text) is not str:
        _invalid("request_invalid")
    text = text.strip()
    catalog_text(text, 512)
    if len(text) < 3:
        _invalid("request_invalid")
    return text


@dataclass(frozen=True)
class FHIRCatalogQuery:
    limit: int = 25
    offset: int = 0
    generation_id: int | None = None
    metadata_generation: str | None = None
    search: str | None = None
    archived: bool | None = False
    source: RegistryNetworkSourceCoordinates | None = None
    benefit_code: str | None = None
    company_id: str | None = None
    group_id: str | None = None
    company_search: str | None = None
    plan_search: str | None = None
    relationship_offset: int = 0
    binding_offset: int = 0
    continuation_scope_sha256: str | None = None

    def __post_init__(self):
        RegistryNetworkCatalogQuery(
            self.limit, self.offset, self.generation_id, self.search, self.archived, self.source, self.benefit_code
        )
        try:
            if self.metadata_generation is not None:
                _digest(self.metadata_generation)
            if self.continuation_scope_sha256 is not None:
                _digest(self.continuation_scope_sha256)
            for identifier in (self.company_id, self.group_id):
                if identifier is not None:
                    catalog_uuid(identifier)
        except RegistryNetworkCatalogError:
            _invalid("request_invalid")
        for field in ("company_search", "plan_search"):
            if getattr(self, field) is not None:
                object.__setattr__(self, field, _search_text(getattr(self, field)))
        for offset in (self.relationship_offset, self.binding_offset):
            catalog_integer(offset, MAX_OFFSET, minimum=0)
        if any((self.offset, self.relationship_offset, self.binding_offset)) and (
            self.metadata_generation is None or self.continuation_scope_sha256 is None
        ):
            _invalid("request_invalid")


def _literal_pattern(text):
    if text is None:
        return None
    return "%" + text.lower().replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_") + "%"


def _object(document, fields):
    if type(document) is not dict or set(document) != fields:
        _invalid()


def _nullable_text(text, maximum):
    if text is not None:
        catalog_text(text, maximum)


def _plan_document(plan):
    if plan is None:
        return
    _object(plan, {"resource_id", "scope_path", "display_name", "valid_from", "valid_to"})
    if type(plan["resource_id"]) is not str or _FHIR_ID.fullmatch(plan["resource_id"]) is None:
        _invalid()
    catalog_text(plan["scope_path"], 512)
    _nullable_text(plan["display_name"], 512)
    for field in ("valid_from", "valid_to"):
        _nullable_text(plan[field], 64)


def _group_document(group, resolution_status):
    if group is None:
        return
    _object(group, {"group_id", "display_name", "group_kind", "relationship_kind", "valid_from", "valid_to"})
    if group["group_kind"] not in {"corporate_parent", "naic_group"} or group["relationship_kind"] not in {
        "reported_affiliation",
        "verified_ownership",
        "verified_no_affiliation",
    }:
        _invalid()
    if group["group_id"] is not None:
        catalog_uuid(group["group_id"])
        catalog_text(group["display_name"], 512)
    elif group["display_name"] is not None or (
        group["relationship_kind"] != "verified_no_affiliation" and resolution_status == "resolved"
    ):
        _invalid()
    if group["relationship_kind"] == "verified_no_affiliation" and group["group_id"] is not None:
        _invalid()
    for field in ("valid_from", "valid_to"):
        if group[field] is not None and (
            type(group[field]) is not str or date.fromisoformat(group[field]).isoformat() != group[field]
        ):
            _invalid()
    if group["valid_from"] is not None and group["valid_to"] is not None and group["valid_to"] < group["valid_from"]:
        _invalid()


def fhir_catalog_relationship_document(document):
    """Validate a closed relationship while preserving explicit unresolved and manual links."""
    _object(document, _RELATIONSHIP_FIELDS)
    _digest(document["relationship_id"])
    catalog_text(document["source_id"], 128)
    _plan_document(document["plan"])
    _nullable_text(document["organization_reference"], 2048)
    if document["company_role"] not in {None, "insurer", "administrator"}:
        _invalid()
    if document["resolution_status"] not in {"resolved", "unresolved", "conflict"}:
        _invalid()
    _nullable_text(document["resolution_reason"], 128)
    company = document["company"]
    if company is not None:
        _object(company, {"company_id", "display_name"})
        catalog_uuid(company["company_id"])
        catalog_text(company["display_name"], 512)
        is_manual_link = (
            document["source_id"] == "registry-manual"
            and document["plan"] is None
            and document["organization_reference"] is None
        )
        if document["company_role"] is None and not is_manual_link:
            _invalid()
    _group_document(document["group"], document["resolution_status"])
    return document


_relationship_document = fhir_catalog_relationship_document


def _binding_metadata(binding):
    _digest(binding["metadata_sha256"])
    accepted_at = binding["accepted_at"]
    if type(accepted_at) is not str or not accepted_at.endswith("Z") or len(accepted_at) > 40:
        _invalid()
    parsed = datetime.fromisoformat(accepted_at)
    if parsed.utcoffset() is None or parsed.utcoffset().total_seconds() != 0:
        _invalid()
    if binding["status"] not in {"current", "last_good"}:
        _invalid()


def fhir_catalog_binding_document(binding):
    """Validate enriched binding metadata and its exact legacy or retained source scope."""
    if type(binding) is not dict or not _BINDING_METADATA <= binding.keys():
        _invalid()
    legacy_by_field = {key: entry for key, entry in binding.items() if key not in _BINDING_METADATA}
    scope = legacy_by_field.get("source_scope")
    if legacy_by_field.get("producer_id") == "registry-fhir-catalog-v1":
        _catalog_binding_document(legacy_by_field)
    elif (
        legacy_by_field.get("source_system") == "fhir"
        and type(scope) is dict
        and set(scope) == {"organization_id", "capture_sha256"}
    ):
        if legacy_by_field.get("producer_id") != "registry-retained-normalized-fhir-v1":
            _invalid()
        if type(scope["organization_id"]) is not str or _FHIR_ID.fullmatch(scope["organization_id"]) is None:
            _invalid()
        _digest(scope["capture_sha256"])
        _fhir_binding_fields(legacy_by_field)
    else:
        catalog_binding_document(legacy_by_field)
    _binding_metadata(binding)
    return binding


_fhir_binding_document = fhir_catalog_binding_document


def _catalog_binding_document(binding):
    _fhir_binding_fields(binding)
    if binding["source_system"] != "fhir" or binding["producer_id"] != "registry-fhir-catalog-v1":
        _invalid()
    scope = binding["source_scope"]
    _object(scope, {"organization_id", "source_generation"})
    organization_id = scope["organization_id"]
    if type(organization_id) is not str or _FHIR_ID.fullmatch(organization_id) is None:
        _invalid()
    _digest(scope["source_generation"])
    if (
        binding["source_key"] != organization_id
        or binding["evidence_id"] != scope["source_generation"] + ":Organization/" + organization_id
    ):
        _invalid()


def _fhir_binding_fields(binding):
    coordinates = {"source_system", "source_id", "dataset_schema", "dataset_id", "producer_id", "edition_id"}
    _object(
        binding,
        coordinates
        | {"namespace", "binding_id", "revision", "source_key", "source_scope", "evidence_id", "evidence_sha256"},
    )
    if binding["namespace"] != "source_binding":
        _invalid()
    RegistryNetworkSourceCoordinates(**{key: binding[key] for key in coordinates})
    catalog_uuid(binding["binding_id"])
    catalog_integer(binding["revision"], 9223372036854775807)
    catalog_text(binding["source_key"], 512)
    catalog_text(binding["evidence_id"], 512)
    _digest(binding["evidence_sha256"])


def _nested_document(document, validator, offset):
    _object(document, {"total", "offset", "limit", "items", "next_offset"})
    total = catalog_integer(document["total"], 9223372036854775807, minimum=0)
    catalog_integer(document["offset"], MAX_OFFSET, minimum=0)
    catalog_integer(document["limit"], NESTED_LIMIT)
    if document["offset"] != offset or document["limit"] != NESTED_LIMIT or type(document["items"]) is not list:
        _invalid()
    if len(document["items"]) != min(NESTED_LIMIT, max(total - offset, 0)):
        _invalid()
    expected_next = offset + len(document["items"]) if offset + len(document["items"]) < total else None
    if document["next_offset"] is not None:
        catalog_integer(document["next_offset"], MAX_OFFSET + NESTED_LIMIT, minimum=0)
    if document["next_offset"] != expected_next:
        _invalid()
    for entry in document["items"]:
        validator(entry)


def _network_item(document, query):
    _object(document, _NETWORK_FIELDS)
    legacy_by_field = {
        key: entry for key, entry in document.items() if key not in {"matching_relationships", "source_bindings"}
    }
    catalog_network_document({**legacy_by_field, "companies": [], "source_bindings": []})
    _nested_document(document["matching_relationships"], _relationship_document, query.relationship_offset)
    _nested_document(document["source_bindings"], _fhir_binding_document, query.binding_offset)
    for field, identity in (("matching_relationships", "relationship_id"), ("source_bindings", "binding_id")):
        entries = document[field]["items"]
        if len({entry[identity] for entry in entries}) != len(entries):
            _invalid()
    return document


async def _metadata_pin(connection, query, namespace):
    from process.registry_fhir_catalog_custody import verify_fhir_catalog_snapshot

    snapshot = await connection.fetchrow(
        f"SELECT {','.join(SNAPSHOT_COLUMNS)} "
        f"FROM {namespace}.registry_fhir_catalog_snapshot WHERE metadata_generation=coalesce($1::text,"
        f"(SELECT metadata_generation FROM {namespace}.network_serving_control WHERE id=1)) AND published_at IS NOT NULL",
        query.metadata_generation,
    )
    if snapshot is None or query.generation_id is not None and snapshot["network_generation"] != query.generation_id:
        _invalid("source_changed")
    _digest(snapshot["metadata_generation"])
    catalog_integer(snapshot["metadata_approved_revision"], 9223372036854775807, minimum=0)
    storage_identifier(snapshot["schema_name"])
    await verify_fhir_catalog_snapshot(connection, snapshot)
    return snapshot


async def _directory_manifest(connection, query, snapshot, control_schema):
    generation = snapshot["network_generation"]
    digest = snapshot["manifest_sha256"]
    if (
        (generation is None) != (digest is None)
        or query.generation_id is not None
        and query.generation_id != generation
    ):
        _invalid("source_changed")
    if generation is None:
        if (
            not connection.is_in_transaction()
            or await connection.fetchval(
                "SELECT current_setting('transaction_isolation') IN ('repeatable read','serializable') "
                "AND current_setting('transaction_read_only')='on'"
            )
            is not True
        ):
            _invalid("transaction_required")
        return None
    catalog_integer(generation, 9223372036854775807)
    _digest(digest)
    manifest = await catalog_pin(connection, generation, control_schema)
    if manifest.manifest_sha256 != digest:
        _invalid("source_changed")
    return manifest


def _query_parameters(query, metadata_generation, exclusions, policy, network_id):
    return (
        metadata_generation,
        exclusions,
        tuple(UUID(identifier) for identifier in policy.excluded_company_ids),
        tuple(UUID(identifier) for identifier in policy.excluded_group_ids),
        UUID(query.company_id) if query.company_id else None,
        UUID(query.group_id) if query.group_id else None,
        _literal_pattern(query.company_search),
        _literal_pattern(query.plan_search),
        query.archived,
        _literal_pattern(query.search),
        query.benefit_code,
        query.source.sql_parameters if query.source else None,
        network_id,
        query.offset,
        1 if network_id is not None else query.limit,
        query.relationship_offset,
        query.binding_offset,
        MAX_CORE_BYTES - 2048,
    )


def _continuation_scope(query, manifest, snapshot, exclusions, policy, access_scope_sha256):
    if access_scope_sha256 is None:
        policy_by_field = {
            "excluded_network_ids": exclusions,
            "excluded_company_ids": policy.excluded_company_ids,
            "excluded_group_ids": policy.excluded_group_ids,
        }
        access_scope_sha256 = hashlib.sha256(
            json.dumps(policy_by_field, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
    _digest(access_scope_sha256)
    scope_by_field = {
        "view": VIEW,
        "network_generation": str(manifest.generation_id) if manifest is not None else None,
        "metadata_generation": snapshot["metadata_generation"],
        "access_scope_sha256": access_scope_sha256,
        "query": {
            "search": query.search.lower() if query.search else None,
            "archived": query.archived,
            "source": asdict(query.source) if query.source else None,
            "benefit_code": query.benefit_code,
            "company_id": query.company_id,
            "group_id": query.group_id,
            "company_search": query.company_search.lower() if query.company_search else None,
            "plan_search": query.plan_search.lower() if query.plan_search else None,
        },
    }
    actual = hashlib.sha256(
        json.dumps(scope_by_field, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode()
    ).hexdigest()
    if query.continuation_scope_sha256 is not None and query.continuation_scope_sha256 != actual:
        _invalid("source_changed")
    return actual


_DIRECTORY_AVAILABLE_SQL = """EXISTS(SELECT 1 FROM {candidate}.network_membership membership
     JOIN {candidate}.provider_location_binding location
       ON (location.provider_system,location.provider_id,location.location_id)=
          (membership.provider_system,membership.provider_id,membership.location_id)
     JOIN {candidate}.entity_address_unified address
       ON (address.location_key,address.entity_type,address.entity_id)=
          (location.location_key,location.entity_type,location.entity_id)
     WHERE membership.network_id=page.network_id)"""


_PAGE_SQL = r"""
WITH matching_edges AS NOT MATERIALIZED (
 SELECT edge.network_id,edge.relationship_id,
   CASE WHEN edge.group_id=ANY($4::uuid[]) THEN
     (edge.document_json-'relationship_id') || jsonb_build_object('group',NULL)
   ELSE edge.document_json-'relationship_id' END AS visible_document,
   edge.group_id=ANY($4::uuid[]) AS redacted
 FROM {control}.registry_fhir_catalog_relationship edge
 LEFT JOIN {control}.registry_fhir_catalog_binding binding
   ON (binding.metadata_generation,binding.network_id,binding.binding_id,binding.source_id)=
      (edge.metadata_generation,edge.network_id,edge.binding_id,edge.source_id)
 WHERE edge.metadata_generation=$1
   AND (edge.source_id='registry-manual' OR binding.binding_id IS NOT NULL)
   AND (edge.company_id IS NULL OR NOT edge.company_id=ANY($3::uuid[]))
   AND ($5::uuid IS NULL OR edge.company_id=$5)
   AND ($6::uuid IS NULL OR (edge.group_id=$6 AND NOT edge.group_id=ANY($4::uuid[])))
   AND ($7::text IS NULL OR edge.company_search LIKE $7 ESCAPE '\')
   AND ($8::text IS NULL OR edge.plan_search LIKE $8 ESCAPE '\')
   AND ($12::text[] IS NULL OR ARRAY[binding.document_json->>'source_system',binding.document_json->>'source_id',
     binding.document_json->>'dataset_schema',binding.document_json->>'dataset_id',
     binding.document_json->>'producer_id',binding.document_json->>'edition_id']=$12)
), visible_edges AS NOT MATERIALIZED (
 SELECT DISTINCT ON (network_id,visible_document) network_id,
   visible_document || jsonb_build_object('relationship_id',CASE WHEN redacted THEN
     encode(sha256(convert_to(visible_document::text,'UTF8')),'hex') ELSE relationship_id END) AS document
 FROM matching_edges ORDER BY network_id,visible_document,redacted DESC,relationship_id
), filtered AS MATERIALIZED (
 SELECT network.* FROM {control}.registry_fhir_catalog_network network
 WHERE network.metadata_generation=$1 AND NOT network.network_id=ANY($2::integer[])
   AND ($9::boolean IS NULL OR network.archived=$9)
   AND ($10::text IS NULL OR network.search_name LIKE $10 ESCAPE '\')
   AND ($11::text IS NULL OR $11=ANY(network.benefit_codes))
   AND ($12::text[] IS NULL OR EXISTS(SELECT 1 FROM {control}.registry_fhir_catalog_binding binding
     WHERE binding.metadata_generation=$1 AND binding.network_id=network.network_id
       AND ARRAY[binding.document_json->>'source_system',binding.document_json->>'source_id',
         binding.document_json->>'dataset_schema',binding.document_json->>'dataset_id',
         binding.document_json->>'producer_id',binding.document_json->>'edition_id']=$12))
   AND ($13::integer IS NULL OR network.network_id=$13)
   AND (($5::uuid IS NULL AND $6::uuid IS NULL AND $7::text IS NULL AND $8::text IS NULL)
     OR EXISTS(SELECT 1 FROM visible_edges edge WHERE edge.network_id=network.network_id))
), page AS MATERIALIZED (
 SELECT * FROM filtered ORDER BY lower(display_name) COLLATE "C",network_id OFFSET $14 LIMIT $15
), documents AS MATERIALIZED (
 SELECT page.network_id,page.display_name,jsonb_build_object(
   'network_id',page.network_id,'display_name',page.display_name,'aliases',page.aliases,
   'archived',page.archived,'revision',page.revision,'priceable',NULL,'benefit_codes',page.benefit_codes,
   'directory_available',{directory_available},
   'matching_relationships',jsonb_build_object('total',relationships.total,'offset',$16::integer,
     'limit',5,'items',relationships.documents,'next_offset',
       CASE WHEN $16+jsonb_array_length(relationships.documents)<relationships.total
         THEN $16+jsonb_array_length(relationships.documents) END),
   'source_bindings',jsonb_build_object('total',bindings.total,'offset',$17::integer,
     'limit',5,'items',bindings.documents,'next_offset',
       CASE WHEN $17+jsonb_array_length(bindings.documents)<bindings.total
         THEN $17+jsonb_array_length(bindings.documents) END)) AS document
 FROM page
 CROSS JOIN LATERAL (
   SELECT (SELECT count(*) FROM visible_edges edge WHERE edge.network_id=page.network_id) AS total,
     coalesce((SELECT jsonb_agg(selected.document ORDER BY selected.document->>'relationship_id')
       FROM (SELECT document FROM visible_edges edge WHERE edge.network_id=page.network_id
         ORDER BY document->>'relationship_id' OFFSET $16 LIMIT 5) selected),'[]'::jsonb) AS documents
 ) relationships
 CROSS JOIN LATERAL (
   SELECT (SELECT count(*) FROM {control}.registry_fhir_catalog_binding binding
     WHERE binding.metadata_generation=$1 AND binding.network_id=page.network_id) AS total,
     coalesce((SELECT jsonb_agg(selected.document_json ORDER BY selected.binding_id)
       FROM (SELECT binding_id,document_json FROM {control}.registry_fhir_catalog_binding binding
         WHERE binding.metadata_generation=$1 AND binding.network_id=page.network_id
         ORDER BY binding_id OFFSET $17 LIMIT 5) selected),'[]'::jsonb) AS documents
 ) bindings
), sized AS (
 SELECT *,sum(octet_length(document::text)+1) OVER(
   ORDER BY lower(display_name) COLLATE "C",network_id) AS bytes FROM documents
)
SELECT (SELECT count(*) FROM filtered)::bigint AS total,
 coalesce((SELECT jsonb_agg(document ORDER BY lower(display_name) COLLATE "C",network_id)
   FROM sized WHERE bytes<=$18),'[]'::jsonb)::text AS rows_json
"""


def _catalog_page_sql(snapshot, manifest):
    directory_available = (
        _DIRECTORY_AVAILABLE_SQL.format(candidate=storage_identifier(manifest.schema_name))
        if manifest is not None
        else "false"
    )
    return _PAGE_SQL.format(
        control=storage_identifier(snapshot["schema_name"]), directory_available=directory_available
    )


def _visible_items(documents, query, exclusions, policy):
    networks = tuple(_network_item(document, query) for document in documents)
    if len({network["network_id"] for network in networks}) != len(networks):
        _invalid()
    for network in networks:
        if network["network_id"] in exclusions:
            _invalid("policy_invalid")
        for relationship in network["matching_relationships"]["items"]:
            company, group = relationship["company"], relationship["group"]
            if company is not None and company["company_id"] in policy.excluded_company_ids:
                _invalid("policy_invalid")
            if group is not None and group["group_id"] in policy.excluded_group_ids:
                _invalid("policy_invalid")
    return networks


def _page_document(entry, manifest, snapshot, query, exclusions, policy, continuation_scope_sha256):
    total = catalog_integer(entry["total"], 9223372036854775807, minimum=0)
    documents = catalog_closed_json(entry["rows_json"].encode(), MAX_CORE_BYTES)
    if type(documents) is not list or len(documents) > min(query.limit, max(total - query.offset, 0)):
        _invalid()
    if not documents and query.offset < total:
        _invalid("response_bounds")
    networks = _visible_items(documents, query, exclusions, policy)
    if manifest is None and any(network["directory_available"] for network in networks):
        _invalid()
    page_by_field = {
        "generation": str(manifest.generation_id) if manifest is not None else None,
        "approved_custom_revision": str(manifest.approved_custom_revision) if manifest is not None else None,
        "view": VIEW,
        "metadata_generation": snapshot["metadata_generation"],
        "metadata_approved_revision": str(snapshot["metadata_approved_revision"]),
        "continuation_scope_sha256": continuation_scope_sha256,
        "total": total,
        "offset": query.offset,
        "limit": query.limit,
        "next_offset": query.offset + len(networks) if query.offset + len(networks) < total else None,
        "items": networks,
    }
    if (
        len(json.dumps(page_by_field, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode())
        > MAX_CORE_BYTES
    ):
        _invalid("response_bounds")
    return page_by_field


async def read_fhir_catalog(
    connection,
    query,
    *,
    excluded_network_ids,
    organization_policy=None,
    control_schema=None,
    network_id=None,
    access_scope_sha256=None,
):
    """Pin retained metadata and its optional directory attachment without publication locks."""
    if type(query) is not FHIRCatalogQuery:
        _invalid("request_invalid")
    if network_id is not None:
        catalog_integer(network_id, 2147483647)
        if query.offset:
            _invalid("request_invalid")
        query = replace(query, limit=1)
    elif query.relationship_offset or query.binding_offset:
        _invalid("request_invalid")
    exclusions = catalog_exclusions(excluded_network_ids)
    policy = organization_policy if organization_policy is not None else RegistryOrganizationVisibility()
    if type(policy) is not RegistryOrganizationVisibility:
        _invalid("policy_invalid")
    namespace = storage_identifier(control_schema if control_schema is not None else registry_schema())
    try:
        snapshot = await _metadata_pin(connection, query, namespace)
        manifest = await _directory_manifest(connection, query, snapshot, control_schema or registry_schema())
        continuation_scope_sha256 = _continuation_scope(
            query, manifest, snapshot, exclusions, policy, access_scope_sha256
        )
        entry = await connection.fetchrow(
            _catalog_page_sql(snapshot, manifest),
            *_query_parameters(query, snapshot["metadata_generation"], exclusions, policy, network_id),
        )
        if entry is None:
            _invalid()
        page = _page_document(entry, manifest, snapshot, query, exclusions, policy, continuation_scope_sha256)
        if network_id is not None and (page["total"] != 1 or len(page["items"]) != 1):
            _invalid("detail_denied")
        return page
    except (asyncpg.PostgresError, KeyError, TypeError, ValueError) as error:
        if isinstance(error, RegistryNetworkCatalogError):
            raise
        raise RegistryNetworkCatalogError("registry_network_catalog_unavailable") from error
