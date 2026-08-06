# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Paginated FHIR-only and explicit dual-store formulary listings."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from sanic import response
from sanic.exceptions import InvalidUsage, NotFound
from sqlalchemy import text

from api.endpoint.pagination import parse_pagination
from api.formulary_fhir_serving_common import DEFAULT_PAGE_SIZE
from api.formulary_fhir_serving_common import FHIR_SOURCE_TYPE
from api.formulary_fhir_serving_common import MAX_PAGE_SIZE
from api.formulary_fhir_serving_common import current_join
from api.formulary_fhir_serving_common import dataset_payload
from api.formulary_fhir_serving_common import iso_value
from api.formulary_fhir_serving_common import json_value
from api.formulary_fhir_serving_common import row_mapping
from api.formulary_fhir_serving_common import source_conditions
from api.formulary_fhir_serving_common import table_name


def _pagination(args):
    return parse_pagination(
        args,
        default_limit=DEFAULT_PAGE_SIZE,
        max_limit=MAX_PAGE_SIZE,
        default_page=1,
        allow_offset=True,
        allow_start=True,
        allow_page_size=True,
    )


def _fhir_list_query_parts(args, pagination):
    query_params_by_name: dict[str, Any] = {
        "limit": pagination.limit,
        "offset": pagination.offset,
    }
    conditions = source_conditions(args, query_params_by_name)
    plan_query = str(args.get("plan_id") or "").strip()
    if plan_query:
        query_params_by_name["plan_query"] = f"%{plan_query}%"
        conditions.append("cp.public_id ILIKE :plan_query")
    drug_query = str(args.get("drug") or "").strip()
    if drug_query:
        query_params_by_name["drug_query"] = f"%{drug_query}%"
        query_params_by_name["drug_exact"] = drug_query
        conditions.append(
            "EXISTS (SELECT 1 FROM "
            f"{table_name('fhir_formulary_alias_membership')} fm "
            f"JOIN {table_name('fhir_formulary_medication')} med "
            "ON med.medication_version_id = fm.medication_version_id "
            "WHERE fm.alias_version_id = av.alias_version_id "
            "AND (med.drug_name ILIKE :drug_query "
            "OR med.rxnorm_id = :drug_exact))"
        )
    return query_params_by_name, conditions


def _fhir_grouped_query(conditions: list[str]) -> str:
    where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""
    return (
        "SELECT cp.public_id, cp.source_id, cp.upstream_list_id, cpv.title, "
        "cpv.name, cpv.status, cpv.upstream_version_id, "
        "cpv.upstream_last_updated, cpv.metadata_json, d.dataset_id, "
        "d.cutoff_at, d.published_at, d.coverage_hash, d.membership_hash, "
        "COUNT(DISTINCT m.rxnorm_id) AS drug_count, "
        "array_agg(DISTINCT a.source_plan_identifier "
        "ORDER BY a.source_plan_identifier) AS source_plan_identifiers "
        + current_join()
        + f"LEFT JOIN {table_name('fhir_formulary_alias_membership')} m "
        "ON m.alias_version_id = av.alias_version_id "
        + where_clause
        + " GROUP BY cp.public_id, cp.source_id, cp.upstream_list_id, "
        "cpv.title, cpv.name, cpv.status, cpv.upstream_version_id, "
        "cpv.upstream_last_updated, cpv.metadata_json, d.dataset_id, "
        "d.cutoff_at, d.published_at, d.coverage_hash, d.membership_hash"
    )


def _fhir_list_item(database_row) -> dict[str, Any]:
    formulary_by_field = row_mapping(database_row)
    return {
        "formulary_id": formulary_by_field["public_id"],
        "formulary_uri": formulary_by_field["public_id"],
        "source_type": FHIR_SOURCE_TYPE,
        "source_id": formulary_by_field["source_id"],
        "plan_id": None,
        "year": None,
        "marketing_name": formulary_by_field.get("title")
        or formulary_by_field.get("name"),
        "state": None,
        "issuer": {
            "issuer_id": None,
            "issuer_name": None,
            "issuer_marketing_name": None,
        },
        "drug_count": int(formulary_by_field.get("drug_count") or 0),
        "last_updated": iso_value(
            formulary_by_field.get("upstream_last_updated")
        ),
        "upstream": {
            "resource_type": "List",
            "id": formulary_by_field["upstream_list_id"],
            "version_id": formulary_by_field.get("upstream_version_id"),
            "status": formulary_by_field.get("status"),
        },
        "source_plan_identifiers": list(
            formulary_by_field.get("source_plan_identifiers") or []
        ),
        "coverage_plan": json_value(
            formulary_by_field.get("metadata_json"),
            {},
        ),
        "dataset": dataset_payload(formulary_by_field),
    }


async def list_fhir_formularies(request):
    """Return one page from the atomically published FHIR generation."""

    pagination = _pagination(request.args)
    query_params_by_name, conditions = _fhir_list_query_parts(
        request.args,
        pagination,
    )
    grouped_query = _fhir_grouped_query(conditions)
    session = request.ctx.sa_session
    total = (
        await session.execute(
            text(f"SELECT COUNT(*) FROM ({grouped_query}) q"),
            query_params_by_name,
        )
    ).scalar() or 0
    page_rows = (
        await session.execute(
            text(
                grouped_query
                + " ORDER BY cp.public_id ASC LIMIT :limit OFFSET :offset"
            ),
            query_params_by_name,
        )
    ).all()
    return response.json(
        {
            "items": [_fhir_list_item(page_row) for page_row in page_rows],
            "page": pagination.page,
            "page_size": pagination.limit,
            "limit": pagination.limit,
            "offset": pagination.offset,
            "total": int(total),
        }
    )


def _positive_filter(args, name: str) -> int | None:
    raw_filter = args.get(name)
    if raw_filter in (None, "", "null"):
        return None
    try:
        parsed_filter = int(raw_filter)
    except (TypeError, ValueError) as exc:
        raise InvalidUsage(f"Parameter '{name}' must be an integer") from exc
    if parsed_filter < 0:
        raise InvalidUsage(f"Parameter '{name}' must be non-negative")
    return parsed_filter


@dataclass
class _UnionQueryParts:
    query_params_by_name: dict[str, Any]
    legacy_conditions: list[str]
    fhir_conditions: list[str]


def _apply_annual_filters(args, query_parts: _UnionQueryParts) -> None:
    issuer_id = _positive_filter(args, "issuer_id")
    year = _positive_filter(args, "year")
    if issuer_id is not None:
        query_parts.query_params_by_name["issuer_id"] = issuer_id
        query_parts.legacy_conditions.append("p.issuer_id = :issuer_id")
        query_parts.fhir_conditions.append("FALSE")
    if year is not None:
        query_parts.query_params_by_name["year"] = year
        query_parts.legacy_conditions.append("p.year = :year")
        query_parts.fhir_conditions.append("FALSE")
    state = str(args.get("state") or "").strip().upper()
    if state:
        query_parts.query_params_by_name["state"] = state
        query_parts.legacy_conditions.append("p.state = :state")
        query_parts.fhir_conditions.append("FALSE")


def _apply_search_filters(args, query_parts: _UnionQueryParts) -> None:
    plan_query = str(args.get("plan_id") or "").strip()
    if plan_query:
        query_parts.query_params_by_name["plan_query"] = f"%{plan_query}%"
        query_parts.legacy_conditions.append("p.plan_id ILIKE :plan_query")
        query_parts.fhir_conditions.append("cp.public_id ILIKE :plan_query")
    drug_query = str(args.get("drug") or "").strip()
    if not drug_query:
        return
    query_parts.query_params_by_name["drug_query"] = f"%{drug_query}%"
    query_parts.query_params_by_name["drug_exact"] = drug_query
    query_parts.legacy_conditions.append(
        "EXISTS (SELECT 1 FROM "
        f"{table_name('plan_drug_raw')} ldr WHERE ldr.plan_id = p.plan_id "
        "AND (ldr.drug_name ILIKE :drug_query OR ldr.rxnorm_id = :drug_exact))"
    )
    query_parts.fhir_conditions.append(
        "EXISTS (SELECT 1 FROM "
        f"{table_name('fhir_formulary_alias_membership')} fm "
        f"JOIN {table_name('fhir_formulary_medication')} fmed "
        "ON fmed.medication_version_id = fm.medication_version_id "
        "WHERE fm.alias_version_id = av.alias_version_id "
        "AND (fmed.drug_name ILIKE :drug_query "
        "OR fmed.rxnorm_id = :drug_exact))"
    )


def _apply_source_filters(args, query_parts: _UnionQueryParts) -> None:
    source_id = str(args.get("source_id") or "").strip()
    if source_id:
        query_parts.query_params_by_name["source_id"] = source_id
        query_parts.legacy_conditions.append("FALSE")
        query_parts.fhir_conditions.append("cp.source_id = :source_id")
    source_plan_identifier = str(
        args.get("source_plan_identifier") or ""
    ).strip()
    if source_plan_identifier:
        query_parts.query_params_by_name["source_plan_identifier"] = (
            source_plan_identifier
        )
        query_parts.legacy_conditions.append("FALSE")
        query_parts.fhir_conditions.append(
            "a.source_plan_identifier = :source_plan_identifier"
        )


def _union_query_parts(args, pagination) -> _UnionQueryParts:
    query_parts = _UnionQueryParts(
        query_params_by_name={
            "limit": pagination.limit,
            "offset": pagination.offset,
        },
        legacy_conditions=[],
        fhir_conditions=[],
    )
    _apply_annual_filters(args, query_parts)
    _apply_search_filters(args, query_parts)
    _apply_source_filters(args, query_parts)
    return query_parts


def _where_clause(conditions: list[str]) -> str:
    return "WHERE " + " AND ".join(conditions) if conditions else ""


def _legacy_union_query(conditions: list[str]) -> str:
    return (
        "SELECT p.plan_id || ':' || p.year::text AS formulary_id, "
        "p.plan_id || '/' || p.year::text AS formulary_uri, "
        "'legacy'::text AS source_type, NULL::text AS source_id, "
        "p.plan_id::text AS plan_id, p.year::integer AS year, "
        "p.marketing_name::text AS marketing_name, p.state::text AS state, "
        "p.issuer_id::integer AS issuer_id, i.issuer_name::text AS issuer_name, "
        "i.issuer_marketing_name::text AS issuer_marketing_name, "
        "COALESCE(ds.total_drugs, 0)::bigint AS drug_count, "
        "ds.last_updated_on::timestamptz AS last_updated, "
        "NULL::text AS upstream_list_id, NULL::text AS upstream_version_id, "
        "NULL::text AS upstream_status, '[]'::jsonb AS source_plan_identifiers, "
        "NULL::jsonb AS coverage_plan, NULL::text AS dataset_id, "
        "NULL::timestamptz AS cutoff_at, NULL::timestamptz AS published_at, "
        "NULL::text AS coverage_hash, NULL::text AS membership_hash "
        f"FROM {table_name('plan')} p JOIN {table_name('issuer')} i "
        "ON i.issuer_id = p.issuer_id "
        f"LEFT JOIN {table_name('plan_drug_stats')} ds "
        "ON ds.plan_id = p.plan_id "
        + _where_clause(conditions)
    )


def _fhir_union_query(conditions: list[str]) -> str:
    return (
        "SELECT cp.public_id::text AS formulary_id, "
        "cp.public_id::text AS formulary_uri, 'fhir'::text AS source_type, "
        "cp.source_id::text AS source_id, NULL::text AS plan_id, "
        "NULL::integer AS year, COALESCE(cpv.title, cpv.name)::text "
        "AS marketing_name, NULL::text AS state, NULL::integer AS issuer_id, "
        "NULL::text AS issuer_name, NULL::text AS issuer_marketing_name, "
        "COUNT(DISTINCT m.rxnorm_id)::bigint AS drug_count, "
        "cpv.upstream_last_updated AS last_updated, "
        "cp.upstream_list_id::text AS upstream_list_id, "
        "cpv.upstream_version_id::text AS upstream_version_id, "
        "cpv.status::text AS upstream_status, "
        "to_jsonb(array_agg(DISTINCT a.source_plan_identifier "
        "ORDER BY a.source_plan_identifier)) AS source_plan_identifiers, "
        "cpv.metadata_json AS coverage_plan, d.dataset_id::text AS dataset_id, "
        "d.cutoff_at, d.published_at, d.coverage_hash::text, "
        "d.membership_hash::text "
        + current_join()
        + f"LEFT JOIN {table_name('fhir_formulary_alias_membership')} m "
        "ON m.alias_version_id = av.alias_version_id "
        + _where_clause(conditions)
        + " GROUP BY cp.public_id, cp.source_id, cp.upstream_list_id, "
        "cpv.title, cpv.name, cpv.upstream_last_updated, "
        "cpv.upstream_version_id, cpv.status, cpv.metadata_json, "
        "d.dataset_id, d.cutoff_at, d.published_at, d.coverage_hash, "
        "d.membership_hash"
    )


def _legacy_union_item(formulary_by_field: dict[str, Any]) -> dict[str, Any]:
    return {
        "formulary_id": formulary_by_field["formulary_id"],
        "formulary_uri": formulary_by_field["formulary_uri"],
        "source_type": "legacy",
        "source_id": None,
        "plan_id": formulary_by_field["plan_id"],
        "year": formulary_by_field["year"],
        "marketing_name": formulary_by_field.get("marketing_name"),
        "state": formulary_by_field.get("state"),
        "issuer": {
            "issuer_id": formulary_by_field.get("issuer_id"),
            "issuer_name": formulary_by_field.get("issuer_name"),
            "issuer_marketing_name": formulary_by_field.get(
                "issuer_marketing_name"
            ),
        },
        "drug_count": int(formulary_by_field.get("drug_count") or 0),
        "last_updated": iso_value(formulary_by_field.get("last_updated")),
        "upstream": None,
        "coverage_plan": None,
        "dataset": None,
    }


def _fhir_union_item(formulary_by_field: dict[str, Any]) -> dict[str, Any]:
    return {
        "formulary_id": formulary_by_field["formulary_id"],
        "formulary_uri": formulary_by_field["formulary_uri"],
        "source_type": FHIR_SOURCE_TYPE,
        "source_id": formulary_by_field["source_id"],
        "plan_id": None,
        "year": None,
        "marketing_name": formulary_by_field.get("marketing_name"),
        "state": None,
        "issuer": {
            "issuer_id": None,
            "issuer_name": None,
            "issuer_marketing_name": None,
        },
        "drug_count": int(formulary_by_field.get("drug_count") or 0),
        "last_updated": iso_value(formulary_by_field.get("last_updated")),
        "upstream": {
            "resource_type": "List",
            "id": formulary_by_field["upstream_list_id"],
            "version_id": formulary_by_field.get("upstream_version_id"),
            "status": formulary_by_field.get("upstream_status"),
        },
        "source_plan_identifiers": json_value(
            formulary_by_field.get("source_plan_identifiers"),
            [],
        ),
        "coverage_plan": json_value(
            formulary_by_field.get("coverage_plan"),
            {},
        ),
        "dataset": dataset_payload(formulary_by_field),
    }


def _union_item(database_row) -> dict[str, Any]:
    formulary_by_field = row_mapping(database_row)
    if formulary_by_field["source_type"] == "legacy":
        return _legacy_union_item(formulary_by_field)
    return _fhir_union_item(formulary_by_field)


async def list_all_formularies(request):
    """Return one correctly paginated union without assigning FHIR years."""

    pagination = _pagination(request.args)
    query_parts = _union_query_parts(request.args, pagination)
    union_query = (
        f"({_legacy_union_query(query_parts.legacy_conditions)}) UNION ALL "
        f"({_fhir_union_query(query_parts.fhir_conditions)})"
    )
    session = request.ctx.sa_session
    total = (
        await session.execute(
            text(f"SELECT COUNT(*) FROM ({union_query}) u"),
            query_parts.query_params_by_name,
        )
    ).scalar() or 0
    page_query = (
        f"SELECT * FROM ({union_query}) u ORDER BY formulary_id, source_type "
        "LIMIT :limit OFFSET :offset"
    )
    page_rows = (
        await session.execute(
            text(page_query),
            query_parts.query_params_by_name,
        )
    ).all()
    return response.json(
        {
            "items": [_union_item(page_row) for page_row in page_rows],
            "page": pagination.page,
            "page_size": pagination.limit,
            "limit": pagination.limit,
            "offset": pagination.offset,
            "total": int(total),
        }
    )


def _response_payload(http_response) -> dict[str, Any] | None:
    if http_response is None:
        return None
    response_body = getattr(http_response, "body", None)
    if isinstance(response_body, bytes):
        return json.loads(response_body)
    if isinstance(response_body, str):
        return json.loads(response_body)
    return None


def merge_cross_formulary_responses(
    rxnorm_id,
    legacy_response,
    fhir_response,
):
    """Merge optional store responses while preserving legacy ID contracts."""

    legacy_payload = _response_payload(legacy_response)
    fhir_payload = _response_payload(fhir_response)
    formularies = [
        *(legacy_payload.get("formularies", []) if legacy_payload else []),
        *(fhir_payload.get("formularies", []) if fhir_payload else []),
    ]
    if not formularies:
        raise NotFound("Drug not present in any known formulary")
    formularies.sort(key=lambda formulary: formulary["formulary_id"])
    return response.json({"rxnorm_id": rxnorm_id, "formularies": formularies})
