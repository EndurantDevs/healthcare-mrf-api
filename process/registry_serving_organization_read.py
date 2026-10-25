# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Explicitly scoped organization metadata from one retained serving approval."""

from dataclasses import dataclass

import asyncpg

from db.registry_schema import registry_schema
from process.network_address_projection import _identifier
from process.registry_catalog_policy import RegistryOrganizationVisibility
from process.registry_network_catalog_read import (
    MAX_RESPONSE_BYTES,
    RegistryNetworkCatalogError,
    _closed_json,
    _integer,
    _pin,
    _text,
    _uuid,
)

_FIELDS = {
    "group": ("group_id", "group_kind", "display_name", "aliases", "archived", "revision"),
    "company": (
        "company_id",
        "display_name",
        "display_name_origin",
        "roles",
        "aliases",
        "archived",
        "revision",
        "role_assertions",
        "identifier_assertions",
        "group_assertions",
    ),
}


_READ_SQL = """WITH scoped AS MATERIALIZED (
              SELECT record_key,record_json FROM {namespace}.registry_approved_record
              WHERE approved_revision=$1 AND record_kind=$2 AND record_key=ANY($3::text[])
                AND record_json->'archived'='false'::jsonb
                AND $4::text[] IS NOT NULL
            ), page AS MATERIALIZED (
              SELECT * FROM scoped ORDER BY record_key OFFSET $5 LIMIT $6
            ), bounds AS (
              SELECT coalesce(sum(octet_length(record_json::text)),0)+$6::bigint*1024<=$7 AS bounded FROM page
            ) SELECT (SELECT count(*) FROM scoped)::bigint AS total,bounds.bounded,
              CASE WHEN bounds.bounded THEN coalesce((SELECT jsonb_agg(document ORDER BY record_key)
                FROM (SELECT record_key,{projection} AS document FROM page) documents),'[]'::jsonb)::text END AS rows_json
              FROM bounds"""


def _ids(values):
    if type(values) not in (tuple, list) or len(values) > 5000:
        raise ValueError("registry_serving_organization_scope_invalid")
    identities = tuple(values)
    for identity in identities:
        _uuid(identity)
    if len(set(identities)) != len(identities):
        raise ValueError("registry_serving_organization_scope_invalid")
    return identities


def _projection(record_kind, namespace, *, exclusions=False):
    fields = []
    for name in _FIELDS[record_kind]:
        value = f"page.record_json->'{name}'"
        if name in ("aliases", "role_assertions", "identifier_assertions"):
            value = f"coalesce({value},'[]'::jsonb)"
        elif name == "display_name_origin":
            value = f"coalesce({value},'\"unknown\"'::jsonb)"
        elif name == "group_assertions":
            value = f"""coalesce((SELECT jsonb_agg(assertion ORDER BY ordinal)
              FROM jsonb_array_elements(coalesce(page.record_json->'group_assertions','[]'::jsonb))
                WITH ORDINALITY item(assertion,ordinal)
              LEFT JOIN {namespace}.registry_approved_record target ON target.approved_revision=$1
                AND target.record_kind='group' AND target.record_key=assertion->>'group_id'
                AND target.record_json->'archived'='false'::jsonb
                AND target.record_json->>'group_kind'=assertion->>'group_kind'
              WHERE assertion->>'company_id'=page.record_key AND (
                (assertion->>'relationship_kind'='verified_no_affiliation' AND assertion->>'group_id' IS NULL)
                OR (target.record_key IS NOT NULL AND assertion->>'group_id'=ANY($4::text[])))),'[]'::jsonb)"""
        fields.extend((f"'{name}'", value))
    projection = "jsonb_build_object(" + ",".join(fields) + ")"
    return (
        projection.replace("assertion->>'group_id'=ANY($4::text[])", "NOT (assertion->>'group_id')=ANY($4::text[])")
        if exclusions
        else projection
    )


def _items(document, record_kind, record_ids, excluded_ids=()):
    organization_rows = _closed_json(document.encode(), MAX_RESPONSE_BYTES)
    if type(organization_rows) is not list:
        raise ValueError("registry_serving_organization_document_invalid")
    identities = set()
    for organization_by_field in organization_rows:
        if type(organization_by_field) is not dict or set(organization_by_field) != set(_FIELDS[record_kind]):
            raise ValueError("registry_serving_organization_document_invalid")
        identity = organization_by_field[record_kind + "_id"]
        _uuid(identity)
        if (
            (record_ids is not None and identity not in record_ids)
            or identity in excluded_ids
            or identity in identities
            or organization_by_field["archived"] is not False
        ):
            raise ValueError("registry_serving_organization_document_invalid")
        identities.add(identity)
        _text(organization_by_field["display_name"], 512)
        _integer(organization_by_field["revision"], 9223372036854775807)
        if type(organization_by_field["aliases"]) is not list or len(organization_by_field["aliases"]) > 100:
            raise ValueError("registry_serving_organization_document_invalid")
        for alias in organization_by_field["aliases"]:
            _text(alias, 512)
        if record_kind == "group":
            if organization_by_field["group_kind"] not in ("corporate_parent", "naic_group"):
                raise ValueError("registry_serving_organization_document_invalid")
        elif any(
            type(organization_by_field[name]) is not list
            for name in ("roles", "role_assertions", "identifier_assertions", "group_assertions")
        ):
            raise ValueError("registry_serving_organization_document_invalid")
    return organization_rows


@dataclass(frozen=True)
class RegistryOrganizationCatalogQuery:
    record_kind: str
    record_ids: tuple[str, ...] | None = None
    allowed_group_ids: tuple[str, ...] = ()
    generation_id: int | None = None
    limit: int = 50
    offset: int = 0

    def __post_init__(self):
        if type(self.record_kind) is not str or self.record_kind not in _FIELDS:
            raise ValueError("registry_serving_organization_kind_invalid")
        if self.record_ids is not None:
            object.__setattr__(self, "record_ids", _ids(self.record_ids))
        object.__setattr__(self, "allowed_group_ids", _ids(self.allowed_group_ids))
        _integer(self.limit, 100)
        _integer(self.offset, 1000000, minimum=0)
        if self.generation_id is not None:
            _integer(self.generation_id, 9223372036854775807)


def _read_scope(query, organization_policy):
    if type(query) is not RegistryOrganizationCatalogQuery:
        raise ValueError("registry_serving_organization_scope_invalid")
    if organization_policy is None:
        return _ids(query.record_ids), _ids(query.allowed_group_ids), ()
    if type(organization_policy) is not RegistryOrganizationVisibility or query.allowed_group_ids:
        raise ValueError("registry_serving_organization_scope_invalid")
    return query.record_ids, organization_policy.excluded_group_ids, organization_policy.exclusions(query.record_kind)


async def read_registry_serving_organizations(connection, query, *, organization_policy=None, control_schema=None):
    """Read selected metadata with independent visibility in one retained approval.

    Trusted v2 callers supply default-visible typed exceptions. The prepared
    explicit-record scope remains available to callers that supply no v2 policy.
    Groups never confer company/network access; the caller owns the snapshot.
    """
    record_ids, group_ids, excluded_ids = _read_scope(query, organization_policy)
    control_schema = control_schema if control_schema is not None else registry_schema()
    namespace = _identifier(control_schema)
    projection = _projection(query.record_kind, namespace, exclusions=organization_policy is not None)
    sql = _READ_SQL
    if organization_policy is not None:
        sql = sql.replace("record_key=ANY($3::text[])", "NOT record_key=ANY($3::text[])")
        if record_ids is not None:
            sql = sql.replace("AND $4::text[] IS NOT NULL", "AND $4::text[] IS NOT NULL AND record_key=ANY($8::text[])")
    try:
        manifest = await _pin(connection, query.generation_id, control_schema)
        parameters = (
            manifest.approved_custom_revision,
            query.record_kind,
            record_ids if organization_policy is None else excluded_ids,
            group_ids,
            query.offset,
            query.limit,
            MAX_RESPONSE_BYTES,
        )
        if organization_policy is not None and record_ids is not None:
            parameters += (record_ids,)
        page_record = await connection.fetchrow(sql.format(namespace=namespace, projection=projection), *parameters)
        if page_record is None or page_record["bounded"] is not True:
            raise ValueError("registry_serving_organization_response_bounds")
        organization_records = _items(page_record["rows_json"], query.record_kind, record_ids, excluded_ids)
        total = _integer(page_record["total"], 9223372036854775807, minimum=0)
        if len(organization_records) != min(query.limit, max(total - query.offset, 0)):
            raise ValueError("registry_serving_organization_document_invalid")
        return {
            "generation": str(manifest.generation_id),
            "approved_custom_revision": str(manifest.approved_custom_revision),
            "record_kind": query.record_kind,
            "total": total,
            "offset": query.offset,
            "limit": query.limit,
            "items": organization_records,
        }
    except (asyncpg.PostgresError, KeyError, TypeError, ValueError) as error:
        raise RegistryNetworkCatalogError("registry_serving_organization_unavailable") from error
