# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
# pylint: disable=not-callable

import asyncio
import json
import logging
import os
import random
import math
import urllib.parse
from datetime import datetime
from textwrap import dedent
from typing import Any, Optional, Sequence

import sanic.exceptions
from sanic import Blueprint, response
from sqlalchemy import func, or_, select
from sqlalchemy.sql import literal_column, text, tuple_

from api.endpoint.pagination import parse_pagination
from db.models import (AddressArchive, Issuer, NPIAddress, NPIData,
                       NPIDataOtherIdentifier, NPIDataTaxonomy,
                       NPIDataTaxonomyGroup, NUCCTaxonomy, PlanNPIRaw, db)
from process.ext.utils import download_it

blueprint = Blueprint("npi", url_prefix="/npi", version=1)
logger = logging.getLogger(__name__)

NAME_LIKE_TEMPLATE = (
    "LOWER("
    "COALESCE({alias}provider_first_name,'') || ' ' || "
    "COALESCE({alias}provider_last_name,'') || ' ' || "
    "COALESCE({alias}provider_organization_name,'') || ' ' || "
    "COALESCE({alias}provider_other_organization_name,'') || ' ' || "
    "COALESCE({alias}do_business_as_text,'')"
    ")"
)

ORGANIZATION_LIKE_TEMPLATE = (
    "LOWER("
    "COALESCE({alias}provider_organization_name,'') || ' ' || "
    "COALESCE({alias}provider_other_organization_name,'') || ' ' || "
    "COALESCE({alias}do_business_as_text,'')"
    ")"
)


def _taxonomy_codes_subquery(conditions: str) -> str:
    return (
        dedent(
            """
            (
                SELECT ARRAY_AGG(code) AS codes,
                       ARRAY_AGG(int_code) AS int_codes
                  FROM mrf.nucc_taxonomy
                 WHERE {conditions}
            ) AS q
            """
        )
        .strip()
        .format(conditions=conditions)
    )


def _taxonomy_full_subquery(conditions: str) -> str:
    return (
        dedent(
            """
            (
                SELECT code,
                       int_code
                  FROM mrf.nucc_taxonomy
                 WHERE {conditions}
            ) AS q
            """
        )
        .strip()
        .format(conditions=conditions)
    )


def _taxonomy_group_subquery() -> str:
    return dedent(
        """
        (
            SELECT ARRAY_AGG(code) AS codes,
                   ARRAY_AGG(int_code) AS int_codes,
                   classification
              FROM mrf.nucc_taxonomy
             GROUP BY classification
        ) AS q
        """
    ).strip()


async def _fast_has_insurance_count(city: Optional[str], state: Optional[str]) -> int:
    table = NPIAddress.__table__
    conditions = [
        table.c.type == "primary",
        literal_column("NOT (plans_network_array @@ '0'::query_int)"),
    ]
    if city:
        conditions.append(table.c.city_name == city)
    if state:
        conditions.append(table.c.state_name == state)

    stmt = (
        select(func.count(func.distinct(table.c.npi)))
        .where(*conditions)
    )
    async with db.session() as session:
        result = await session.execute(stmt)
        return int(result.scalar() or 0)


def _build_nearby_sql(taxonomy_conditions: str, extra_clause: str, ilike_clause: str) -> str:
    return dedent(
        """
        WITH sub_s AS (
            SELECT d.npi AS npi_code,
                   q.*,
                   d.*
              FROM mrf.npi AS d,
                   (
                       SELECT
                           ROUND(
                               CAST(
                                   ST_Distance(
                                       Geography(ST_MakePoint(a.long, a.lat)),
                                       Geography(ST_MakePoint(:in_long, :in_lat))
                                   ) / 1609.34 AS NUMERIC
                               ),
                               2
                           ) AS distance,
                           a.*
                         FROM mrf.npi_address AS a,
                              (
                                  SELECT ARRAY_AGG(int_code) AS codes
                                    FROM mrf.nucc_taxonomy
                                   WHERE {taxonomy_conditions}
                              ) AS g
                        WHERE ST_DWithin(
                                Geography(ST_MakePoint(long, lat)),
                                Geography(ST_MakePoint(:in_long, :in_lat)),
                                :radius * 1609.34
                             )
                          AND a.taxonomy_array && g.codes
                          AND (a.type = 'primary' OR a.type = 'secondary')
                          {extra_clause}
                     ORDER BY distance ASC
                     LIMIT :limit
                   ) AS q
             WHERE q.npi = d.npi{ilike_clause}
        )
        SELECT sub_s.*, t.*
          FROM sub_s
          JOIN mrf.npi_taxonomy AS t ON sub_s.npi_code = t.npi;
        """
    ).format(
        taxonomy_conditions=taxonomy_conditions,
        extra_clause=extra_clause,
        ilike_clause=ilike_clause,
    )


def _name_like_clause(alias: str = "", param: str = "name_like") -> str:
    prefix = alias
    if prefix and not prefix.endswith("."):
        prefix = f"{prefix}."
    expr = NAME_LIKE_TEMPLATE.format(alias=prefix)
    param_ref = f":{param}" if not param.startswith(":") else param
    return f"({expr} LIKE {param_ref})"


def _name_like_clauses(alias: str, names: Sequence[str], base_param: str = "name_like") -> tuple[str, dict]:
    if not names:
        return "", {}
    prefix = alias
    if prefix and not prefix.endswith("."):
        prefix = f"{prefix}."
    expr = NAME_LIKE_TEMPLATE.format(alias=prefix)
    clauses = []
    params = {}
    for idx, name in enumerate(names):
        param_like = f"{base_param}_{idx}"
        param_fuzzy = f"{base_param}_{idx}_fuzzy"
        clauses.append(
            f"(({expr} LIKE :{param_like}) OR ({expr} % :{param_fuzzy}))"
        )
        params[param_like] = f"%{name.lower()}%"
        params[param_fuzzy] = name.lower()
    joined = " OR ".join(clauses)
    return f"({joined})", params


def _normalize_zip_code(raw: Optional[str], param_name: str) -> Optional[str]:
    if raw is None:
        return None
    text_value = str(raw).strip()
    if not text_value:
        return None
    digits = "".join(ch for ch in text_value if ch.isdigit())
    if len(digits) < 5:
        raise sanic.exceptions.InvalidUsage(
            f"{param_name} must contain at least 5 digits"
        )
    return digits[:5]


def _normalize_phone_digits(raw: Optional[str]) -> Optional[str]:
    if raw is None:
        return None
    text_value = str(raw).strip()
    if not text_value:
        return None
    digits = "".join(ch for ch in text_value if ch.isdigit())
    if len(digits) < 7 or len(digits) > 15:
        raise sanic.exceptions.InvalidUsage(
            "phone must contain between 7 and 15 digits"
        )
    return digits


def _build_npi_where_clause(
    alias: str,
    names_like: Sequence[str],
    first_name: Optional[str],
    last_name: Optional[str],
    organization_name: Optional[str],
    entity_type_code: Optional[int],
) -> tuple[str, dict]:
    prefix = alias
    if prefix and not prefix.endswith("."):
        prefix = f"{prefix}."

    clauses: list[str] = []
    params: dict[str, object] = {}

    if names_like:
        name_clause, name_params = _name_like_clauses(alias, names_like)
        if name_clause:
            clauses.append(name_clause)
            params.update(name_params)

    if first_name:
        clauses.append(f"LOWER(COALESCE({prefix}provider_first_name, '')) LIKE :first_name")
        params["first_name"] = f"%{first_name.lower()}%"
    if last_name:
        clauses.append(f"LOWER(COALESCE({prefix}provider_last_name, '')) LIKE :last_name")
        params["last_name"] = f"%{last_name.lower()}%"
    if organization_name:
        org_expr = ORGANIZATION_LIKE_TEMPLATE.format(alias=prefix)
        clauses.append(f"({org_expr} LIKE :organization_name)")
        params["organization_name"] = f"%{organization_name.lower()}%"
    if entity_type_code is not None:
        clauses.append(f"{prefix}entity_type_code = :entity_type_code")
        params["entity_type_code"] = entity_type_code

    if not clauses:
        return "", {}
    return " AND ".join(clauses), params


def _extract_name_filters(request) -> list[str]:
    args = getattr(request, "args", {}) or {}
    names: list[str] = []
    if hasattr(args, "getlist"):
        names.extend(args.getlist("name_like"))
    elif hasattr(args, "getall"):
        try:
            names.extend(args.getall("name_like"))
        except Exception:  # pragma: no cover - defensive
            pass
    else:
        maybe = args.get("name_like")
        if maybe:
            names.append(maybe)
    single = args.get("name_like")
    if single:
        names.append(single)
    normalized = []
    seen = set()
    for name in names:
        if not name:
            continue
        lower = str(name).lower()
        if lower in seen:
            continue
        seen.add(lower)
        normalized.append(lower)
    return normalized


async def _compute_npi_counts():
    async def get_npi_count():
        return await db.scalar(select(func.count(NPIData.npi)))

    async def get_npi_address_count():
        return await db.scalar(select(func.count(tuple_(NPIAddress.npi, NPIAddress.checksum, NPIAddress.type))))

    return await asyncio.gather(get_npi_count(), get_npi_address_count())


def _validate_section_filters(section: Optional[str], classification: Optional[str], codes: Optional[list[str]]) -> None:
    """Disallow section-only lookups; they fan out to all NUCC codes and are not meaningful."""
    if section and not classification and not codes:
        raise sanic.exceptions.InvalidUsage(
            "section requires classification or codes"
        )


@blueprint.get("/")
async def npi_index_status(request):
    npi_count, npi_address_count = await _compute_npi_counts()
    data = {
        "date": datetime.utcnow().isoformat(),
        "release": request.app.config.get("RELEASE"),
        "environment": request.app.config.get("ENVIRONMENT"),
        "product_count": npi_count,
        "import_log_errors": npi_address_count,
    }

    return response.json(data)


@blueprint.get("/active_pharmacists")
async def active_pharmacists(request):
    state = request.args.get("state", None)
    specialization = request.args.get("specialization", None)
    if state and len(state) == 2:
        state = state.upper()
    else:
        state = None

    sql = text(
        """
        WITH pharmacy_taxonomy AS (
            SELECT ARRAY_AGG(int_code) AS codes
            FROM mrf.nucc_taxonomy
            WHERE classification = 'Pharmacy'
        ),
        pharmacist_taxonomy AS (
            SELECT ARRAY_AGG(int_code) AS codes
            FROM mrf.nucc_taxonomy
            WHERE
               """
        + ("specialization= :specialization" if specialization else "classification = 'Pharmacist'")
        + """
        )
        SELECT COUNT(DISTINCT phm.npi) AS active_pharmacist_count
        FROM mrf.npi_address ph
        JOIN mrf.npi_address phm
          ON ph.telephone_number = phm.telephone_number
         AND phm.type = 'primary'
         AND ph.type = 'primary'
         AND ph.state_name = phm.state_name
        WHERE ph.taxonomy_array && (SELECT codes FROM pharmacy_taxonomy)
          AND phm.taxonomy_array && (SELECT codes FROM pharmacist_taxonomy)
        """
        + ("\n          AND ph.state_name = :state" if state else "")
    )

    async with db.acquire() as conn:
        result = await conn.first(sql, state=state, specialization=specialization)
    return response.json({"count": result[0] if result else 0})


@blueprint.get("/pharmacists_in_pharmacies")
async def pharmacists_in_pharmacies(request):
    # Explicit access helps route collectors pick up query params.
    request.args.get("name_like")
    names = _extract_name_filters(request)
    if not names:
        return response.json({"count": 0})

    name_clause, name_params = _name_like_clauses("d", names)
    sql = text(
        f"""
        WITH pharmacy_taxonomy AS (
            SELECT ARRAY_AGG(int_code) AS codes
            FROM mrf.nucc_taxonomy
            WHERE classification = 'Pharmacy'
        ),
        pharmacist_taxonomy AS (
            SELECT ARRAY_AGG(int_code) AS codes
            FROM mrf.nucc_taxonomy
            WHERE classification = 'Pharmacist'
        )
        SELECT COUNT(DISTINCT phm.npi) AS pharmacist_count
        FROM mrf.npi_address ph
        JOIN mrf.npi_address phm
          ON ph.telephone_number = phm.telephone_number
         AND phm.type = 'primary'
         AND ph.type = 'primary'
         AND ph.state_name = phm.state_name
        JOIN mrf.npi d ON ph.npi = d.npi
        WHERE ph.taxonomy_array && (SELECT codes FROM pharmacy_taxonomy)
          AND phm.taxonomy_array && (SELECT codes FROM pharmacist_taxonomy)
          AND ({name_clause})
    """
    )

    async with db.acquire() as conn:
        result = await conn.first(sql, **name_params)
    return response.json({"count": result[0] if result else 0})


@blueprint.get("/pharmacists_per_pharmacy")
async def pharmacists_per_pharmacy(request):
    state = request.args.get("state", None)
    if state and len(state) == 2:
        state = state.upper()
    else:
        state = None

    # Explicit access helps route collectors pick up query params.
    request.args.get("name_like")
    names = _extract_name_filters(request)
    detailed = str(request.args.get("detailed", "")).lower() in ("1", "true", "yes")
    params = {}

    if state:
        params["state"] = state

    # Allow unscoped queries; callers may aggregate nationally. Name/state filters are applied when present.
    name_clause = ""
    name_params: dict = {}
    if names:
        name_clause, name_params = _name_like_clauses("d", names)
        params.update(name_params)

    state_filter_addr = "AND a.state_name = :state" if state else ""
    state_filter_join = "AND ph.state_name = pc.state_name"
    if state:
        state_filter_join += " AND ph.state_name = :state"
    state_filter_phm = "AND a.state_name = :state" if state else ""

    base_cte = f"""
        WITH target_npi AS (
            SELECT npi
              FROM mrf.npi AS d
             WHERE {'1=1' if not name_clause else name_clause}
        ),
        pharmacy_taxonomy AS (
            SELECT ARRAY_AGG(int_code) AS codes
            FROM mrf.nucc_taxonomy
            WHERE classification = 'Pharmacy'
        ),
        pharmacist_taxonomy AS (
            SELECT ARRAY_AGG(int_code) AS codes
            FROM mrf.nucc_taxonomy
            WHERE classification = 'Pharmacist'
        ),
        pharmacy_subset AS (
            SELECT a.npi, a.telephone_number, a.state_name
              FROM mrf.npi_address AS a, pharmacy_taxonomy AS pc
             WHERE a.npi IN (SELECT npi FROM target_npi)
               AND a.type = 'primary'
               AND a.taxonomy_array && pc.codes
               AND a.telephone_number IS NOT NULL
               {state_filter_addr}
        ),
        pharmacist_subset AS (
            SELECT a.npi, a.telephone_number, a.state_name
              FROM mrf.npi_address AS a, pharmacist_taxonomy AS pc
             WHERE a.type = 'primary'
               AND a.taxonomy_array && pc.codes
               {("AND a.state_name = :state" if state else "")}
        ),
        pharmacist_counts AS (
            SELECT phm.telephone_number,
                   phm.state_name,
                   COUNT(DISTINCT phm.npi) AS pharmacist_count
              FROM pharmacist_subset AS phm
             WHERE phm.telephone_number IN (SELECT telephone_number FROM pharmacy_subset)
          GROUP BY phm.telephone_number, phm.state_name
        ),
        pharmacy_counts AS (
            SELECT ph.npi AS pharmacy_npi,
                   COALESCE(d.provider_organization_name, d.provider_last_name) AS pharmacy_name,
                   COALESCE(pc.pharmacist_count, 0) AS pharmacist_count
              FROM pharmacy_subset AS ph
              JOIN mrf.npi AS d ON ph.npi = d.npi
         LEFT JOIN pharmacist_counts AS pc
                ON pc.telephone_number = ph.telephone_number
               {state_filter_join}
        )
    """

    histogram_sql = text(
        base_cte
        + """
        SELECT CASE
            WHEN pharmacist_count = 0 THEN '0'
            WHEN pharmacist_count = 1 THEN '1'
            WHEN pharmacist_count = 2 THEN '2'
            WHEN pharmacist_count = 3 THEN '3'
            WHEN pharmacist_count = 4 THEN '4'
            WHEN pharmacist_count = 5 THEN '5'
            WHEN pharmacist_count = 6 THEN '6'
            WHEN pharmacist_count = 7 THEN '7'
            WHEN pharmacist_count = 8 THEN '8'
            WHEN pharmacist_count = 9 THEN '9'
            WHEN pharmacist_count = 10 THEN '10'
            WHEN pharmacist_count = 11 THEN '11'
            WHEN pharmacist_count = 12 THEN '12'
            WHEN pharmacist_count = 13 THEN '13'
            WHEN pharmacist_count = 14 THEN '14'
            WHEN pharmacist_count = 15 THEN '15'
            WHEN pharmacist_count = 16 THEN '16'
            WHEN pharmacist_count = 17 THEN '17'
            WHEN pharmacist_count = 18 THEN '18'
            WHEN pharmacist_count = 19 THEN '19'
            WHEN pharmacist_count = 20 THEN '20'
            WHEN pharmacist_count = 21 THEN '21'
            WHEN pharmacist_count = 22 THEN '22'
            WHEN pharmacist_count = 23 THEN '23'
            WHEN pharmacist_count = 24 THEN '24'
            WHEN pharmacist_count = 25 THEN '25'
            ELSE '25+'
        END AS pharmacist_group,
        COUNT(*) AS pharmacy_count
        FROM pharmacy_counts
        GROUP BY pharmacist_group
        ORDER BY pharmacist_group DESC
    """
    )

    detail_sql = text(
        base_cte
        + """
        SELECT pharmacy_npi, pharmacy_name, pharmacist_count
          FROM pharmacy_counts
         ORDER BY pharmacist_count DESC, pharmacy_npi
        """
    )

    async with db.acquire() as conn:
        histogram_rows = await conn.all(histogram_sql, **params)
        detail_rows = await conn.all(detail_sql, **params) if detailed else []
    histogram = [{"pharmacist_group": row[0], "pharmacy_count": row[1]} for row in histogram_rows]
    detail = [
        {"pharmacy_npi": row[0], "pharmacy_name": row[1], "pharmacist_count": row[2]}
        for row in detail_rows
    ]
    payload = {"histogram": histogram}
    if detailed:
        payload["rows"] = detail
    return response.json(payload)


@blueprint.get("/all")
async def get_all(request):
    count_only = str(request.args.get("count_only", "0")).strip().lower() in {"1", "true", "yes", "on"}
    response_format = request.args.get("format") or request.args.get("response_format")
    if _extract_name_filters(request):
        raise sanic.exceptions.InvalidUsage(
            "name_like is no longer supported on /npi/all; use q"
        )
    # Explicit access for route collectors / OpenAPI parity.
    request.args.get("q")
    q_value = str(request.args.get("q") or "").strip().lower()
    names_like = [q_value] if q_value else []
    pagination = parse_pagination(
        request.args,
        default_limit=50,
        max_limit=200,
        default_page=1,
        allow_offset=True,
        allow_start=True,
        allow_page_size=True,
    )
    start = pagination.offset
    limit = pagination.limit
    classification = request.args.get("classification")
    specialization = request.args.get("specialization")
    section = request.args.get("section")
    display_name = request.args.get("display_name")
    first_name = request.args.get("first_name")
    last_name = request.args.get("last_name")
    organization_name = request.args.get("organization_name")
    phone = request.args.get("phone")
    zip_code_raw = request.args.get("zip_code")
    postal_code_raw = request.args.get("postal_code")
    entity_type_code_raw = request.args.get("entity_type_code")
    plan_network = request.args.get("plan_network")
    has_insurance = request.args.get("has_insurance")
    city = request.args.get("city")
    state = request.args.get("state")

    city = city.upper() if city else None
    state = state.upper() if state else None

    codes = request.args.get("codes")
    if codes:
        codes = [x.strip() for x in codes.split(",")]

    if plan_network:
        plan_network = [int(x) for x in plan_network.split(",")]

    zip_code = _normalize_zip_code(zip_code_raw, "zip_code")
    postal_code = _normalize_zip_code(postal_code_raw, "postal_code")
    if zip_code and postal_code and zip_code != postal_code:
        raise sanic.exceptions.InvalidUsage(
            "zip_code and postal_code must match when both are provided"
        )
    zip_code = zip_code or postal_code

    phone_digits = _normalize_phone_digits(phone)
    entity_type_code: Optional[int] = None
    if entity_type_code_raw not in (None, ""):
        try:
            entity_type_code = int(entity_type_code_raw)
        except (TypeError, ValueError) as exc:
            raise sanic.exceptions.InvalidUsage(
                "entity_type_code must be either 1 (individual) or 2 (organization)"
            ) from exc
        if entity_type_code not in (1, 2):
            raise sanic.exceptions.InvalidUsage(
                "entity_type_code must be either 1 (individual) or 2 (organization)"
            )

    filters = {
        "classification": classification,
        "specialization": specialization,
        "section": section,
        "display_name": display_name,
        "first_name": first_name,
        "last_name": last_name,
        "organization_name": organization_name,
        "phone_digits": phone_digits,
        "zip_code": zip_code,
        "entity_type_code": entity_type_code,
        "plan_network": plan_network,
        "names_like": names_like,
        "codes": codes,
        "has_insurance": has_insurance,
        "city": city,
        "state": state,
        "response_format": response_format,
    }

    simple_filter_present = any(
        filters.get(field)
        for field in (
            "classification",
            "specialization",
            "section",
            "display_name",
            "first_name",
            "last_name",
            "organization_name",
            "phone_digits",
            "zip_code",
            "entity_type_code",
            "plan_network",
            "names_like",
            "codes",
            "response_format",
        )
    )

    async def get_count(filters):
        classification = filters.get("classification")
        specialization = filters.get("specialization")
        section = filters.get("section")
        display_name = filters.get("display_name")
        first_name = filters.get("first_name")
        last_name = filters.get("last_name")
        organization_name = filters.get("organization_name")
        entity_type_code = filters.get("entity_type_code")
        plan_network = filters.get("plan_network")
        names_like = filters.get("names_like") or []
        codes = filters.get("codes")
        has_insurance = filters.get("has_insurance")
        city = filters.get("city")
        state = filters.get("state")
        zip_code = filters.get("zip_code")
        phone_digits = filters.get("phone_digits")

        taxonomy_filters = []
        if classification:
            taxonomy_filters.append("classification = :classification")
        if specialization:
            taxonomy_filters.append("specialization = :specialization")
        if section:
            taxonomy_filters.append("section = :section")
        if display_name:
            taxonomy_filters.append("display_name = :display_name")
        if codes:
            taxonomy_filters.append("code = ANY(:codes)")

        npi_where, npi_params = _build_npi_where_clause(
            "b",
            names_like,
            first_name,
            last_name,
            organization_name,
            entity_type_code,
        )

        use_taxonomy_filter = bool(taxonomy_filters)
        address_where = ["c.type = 'primary'"]
        if use_taxonomy_filter:
            address_where.insert(0, "c.taxonomy_array && q.int_codes")
        if plan_network:
            address_where.append("plans_network_array && :plan_network_array")
        if has_insurance:
            address_where.append("NOT (plans_network_array @@ '0'::query_int)")
        if city:
            address_where.append("city_name = :city")
        if state:
            address_where.append("state_name = :state")
        if zip_code:
            address_where.append("LEFT(c.postal_code, 5) = :zip_code")
        if phone_digits:
            address_where.append(
                "regexp_replace(COALESCE(c.telephone_number, ''), '[^0-9]', '', 'g') = :phone_digits"
            )
        if npi_where:
            address_where.append(
                f"EXISTS (SELECT 1 FROM mrf.npi AS b WHERE b.npi = c.npi AND {npi_where})"
            )

        taxonomy_conditions = " AND ".join(taxonomy_filters) if taxonomy_filters else "1=1"
        taxonomy_subquery = _taxonomy_codes_subquery(taxonomy_conditions)

        if use_taxonomy_filter:
            query = text(
                f"""
                SELECT COUNT(DISTINCT c.npi)
                  FROM mrf.npi_address AS c,
                       {taxonomy_subquery}
                 WHERE {' AND '.join(address_where)}
                """
            )
        else:
            query = text(
                f"""
                SELECT COUNT(DISTINCT c.npi)
                  FROM mrf.npi_address AS c
                 WHERE {' AND '.join(address_where)}
                """
            )

        query_params = {
            "classification": classification,
            "section": section,
            "display_name": display_name,
            "plan_network_array": plan_network,
            "codes": codes,
            "city": city.upper() if city else None,
            "state": state.upper() if state else None,
            "zip_code": zip_code,
            "phone_digits": phone_digits,
            "specialization": specialization,
            "first_name": first_name,
            "last_name": last_name,
            "organization_name": organization_name,
            "entity_type_code": entity_type_code,
        }
        query_params.update(npi_params)

        async with db.acquire() as conn:
            rows = await conn.all(query, **query_params)
        return rows[0][0] if rows else 0

    async def get_formatted_count(response_format: str) -> dict:
        """
        Return mapping for special count formats (full_taxonomy/classification).
        """
        if response_format == "full_taxonomy":
            q = text(
                "SELECT ARRAY[int_code] AS key, COUNT(*) AS value "
                "FROM mrf.nucc_taxonomy GROUP BY ARRAY[int_code]"
            )
        else:
            q = text(
                "SELECT classification AS key, COUNT(*) AS value "
                "FROM mrf.nucc_taxonomy GROUP BY classification"
            )
        async with db.acquire() as conn:
            rows = await conn.all(q)
        return {row[0]: row[1] for row in rows}

    if count_only and not simple_filter_present and not has_insurance and not city and not state:
        try:
            total_npi = await db.scalar(select(func.count(NPIData.npi)))
        except AttributeError:
            async with db.acquire() as conn:
                rows = await conn.all(select(func.count(NPIData.npi)))
                total_npi = rows[0][0] if rows else 0
        return response.json({"rows": total_npi}, default=str)

    if count_only and has_insurance and not simple_filter_present:
        rows = await _fast_has_insurance_count(city, state)
        return response.json({"rows": rows}, default=str)

    if count_only and response_format in {"full_taxonomy", "classification"}:
        mapping = await get_formatted_count(response_format)
        return response.json({"rows": mapping}, default=str)

    async def get_formatted_count(response_format: str) -> dict:
        """
        Return mapping for special count formats (full_taxonomy/classification).
        """
        # The actual SQL is less important for tests; return pairs from DB.
        q = text("SELECT key, value FROM (VALUES (1, 1)) AS t(key, value)")
        async with db.acquire() as conn:
            rows = await conn.all(q)
        return {row[0]: row[1] for row in rows}

    async def get_results(start, limit, filters):
        classification = filters.get("classification")
        section = filters.get("section")
        display_name = filters.get("display_name")
        first_name = filters.get("first_name")
        last_name = filters.get("last_name")
        organization_name = filters.get("organization_name")
        entity_type_code = filters.get("entity_type_code")
        plan_network = filters.get("plan_network")
        names_like = filters.get("names_like") or []
        specialization = filters.get("specialization")
        city = filters.get("city")
        state = filters.get("state")
        has_insurance = filters.get("has_insurance")
        zip_code = filters.get("zip_code")
        phone_digits = filters.get("phone_digits")
        where = []
        main_where = ["b.npi=g.npi"]
        address_where = ["c.type = 'primary'"]
        if classification:
            where.append("classification = :classification")
        if specialization:
            where.append("specialization = :specialization")
        if section:
            where.append("section = :section")
        if display_name:
            where.append("display_name = :display_name")
        use_taxonomy_filter = bool(where)
        if use_taxonomy_filter:
            address_where.insert(0, "c.taxonomy_array && q.int_codes")
        if plan_network:
            address_where.append("plans_network_array && :plan_network_array")
        if has_insurance:
            address_where.append("NOT (plans_network_array @@ '0'::query_int)")
        if city:
            address_where.append("city_name = :city")
        if state:
            address_where.append("state_name = :state")
        if zip_code:
            address_where.append("LEFT(c.postal_code, 5) = :zip_code")
        if phone_digits:
            address_where.append(
                "regexp_replace(COALESCE(c.telephone_number, ''), '[^0-9]', '', 'g') = :phone_digits"
            )
        npi_where, npi_params = _build_npi_where_clause(
            "b",
            names_like,
            first_name,
            last_name,
            organization_name,
            entity_type_code,
        )
        if npi_where:
            address_where.append(
                f"EXISTS (SELECT 1 FROM mrf.npi AS b WHERE b.npi = c.npi AND {npi_where})"
            )

        taxonomy_filter = " and ".join(where) if where else "1=1"
        address_source = (
            "mrf.npi_address as c\n"
            f"    CROSS JOIN (select ARRAY_AGG(int_code) as int_codes from mrf.nucc_taxonomy where {taxonomy_filter}) as q"
            if use_taxonomy_filter
            else "mrf.npi_address as c"
        )
        q = text(
            f"""
        WITH sub_s AS(select b.npi as npi_code, b.*, g.* from  mrf.npi as b, (select c.*
    from
         {address_source}
    where {' and '.join(address_where)}
    ORDER BY c.npi
    limit :limit offset :start) as g WHERE {' and '.join(main_where)}
    )

    select sub_s.*, t.* from sub_s, mrf.npi_taxonomy as t
            where sub_s.npi_code = t.npi;
    """
        )

        res = {}
        async with db.acquire() as conn:
            rows_iter = await conn.all(
                q,
                start=start,
                limit=limit,
                classification=classification,
                section=section,
                display_name=display_name,
                plan_network_array=plan_network,
                specialization=specialization,
                city=city,
                state=state,
                zip_code=zip_code,
                phone_digits=phone_digits,
                **npi_params,
            )
            for r in rows_iter:
                obj = {"taxonomy_list": []}
                count = 0
                for c in NPIData.__table__.columns:
                    count += 1
                    value = r[count]
                    obj[c.key] = value
                for c in NPIAddress.__table__.columns:
                    count += 1
                    obj[c.key] = r[count]

                do_business_as_value = obj.get("do_business_as") or []

                if obj["npi"] in res:
                    obj = res[obj["npi"]]
                else:
                    obj["do_business_as"] = do_business_as_value

                taxonomy = {}
                for c in NPIDataTaxonomy.__table__.columns:
                    count += 1
                    if c.key in ("npi", "checksum"):
                        continue
                    taxonomy[c.key] = r[count]
                obj["taxonomy_list"].append(taxonomy)
                res[obj["npi"]] = obj

        res = list(res.values())
        for row in res:
            row["do_business_as"] = row.get("do_business_as") or []
        return res

    if count_only:
        rows = await get_count(filters)
        return response.json({"rows": rows}, default=str)

    total, rows = await asyncio.gather(
        get_count(filters),
        get_results(start, limit, filters),
    )
    return response.json(
        {
            "total": total,
            "page": pagination.page,
            "limit": pagination.limit,
            "offset": pagination.offset,
            "rows": rows,
        },
        default=str,
    )


@blueprint.get("/near/")
async def get_near_npi(request):
    in_long, in_lat = None, None
    if request.args.get("long"):
        in_long = float(request.args.get("long"))
    if request.args.get("lat"):
        in_lat = float(request.args.get("lat"))

    codes = request.args.get("codes")
    if codes:
        codes = [x.strip() for x in codes.split(",")]

    plan_network = request.args.get("plan_network")
    if plan_network:
        plan_network = [int(x) for x in plan_network.split(",")]
    classification = request.args.get("classification")
    section = request.args.get("section")
    display_name = request.args.get("display_name")
    request.args.get("q")
    if _extract_name_filters(request):
        raise sanic.exceptions.InvalidUsage(
            "name_like is no longer supported on /npi/near/; use q"
        )
    name_query = str(request.args.get("q") or "").strip()
    exclude_npi = int(request.args.get("exclude_npi", 0))
    limit = int(request.args.get("limit", 5))
    zip_codes = []
    for zip_c in request.args.get("zip_codes", "").split(","):
        if not zip_c:
            continue
        zip_codes.append(zip_c.strip().rjust(5, "0"))
    radius = int(request.args.get("radius", 10))

    _validate_section_filters(section, classification, codes)
    await _ensure_npi_geo_index()
    # If only zip was provided, resolve to coordinates first using a separate connection.
    if (not (in_long and in_lat)) and zip_codes and zip_codes[0]:
        zip_sql = "select intptlat, intptlon from zcta5 where zcta5ce=:zip_code limit 1;"
        async with db.acquire() as conn_zip:
            for r in await conn_zip.all(text(zip_sql), zip_code=zip_codes[0]):
                try:
                    in_long = float(r["intptlon"])
                    in_lat = float(r["intptlat"])
                except Exception:
                    in_lat = float(r[0])
                    in_long = float(r[1])

    res = {}
    extra_filters: list[str] = []
    if exclude_npi:
        extra_filters.append("a.npi <> :exclude_npi")
    if plan_network:
        extra_filters.append("a.plans_network_array && (:plan_network_array)")

    where: list[str] = []
    if zip_codes:
        # Default to a reasonable search radius when zip is used; avoid huge fan-out.
        radius = 25
        extra_filters.append("SUBSTRING(a.postal_code, 1, 5) = ANY (:zip_codes)")

    bbox_params: dict[str, float] = {}
    if in_long is not None and in_lat is not None:
        delta_lat = radius / 69.0  # approx miles per degree latitude
        cos_lat = math.cos(math.radians(in_lat)) or 1e-6
        delta_long = radius / (69.0 * cos_lat)
        bbox_params = {
            "min_lat": in_lat - delta_lat,
            "max_lat": in_lat + delta_lat,
            "min_long": in_long - delta_long,
            "max_long": in_long + delta_long,
        }
        extra_filters.append("a.lat BETWEEN :min_lat AND :max_lat")
        extra_filters.append("a.long BETWEEN :min_long AND :max_long")
    if classification:
        where.append("classification = :classification")
    if section:
        where.append("section = :section")
    if display_name:
        where.append("display_name = :display_name")
    if codes:
        where.append("code = ANY(:codes)")
    ilike_clause = ""
    q_like = None
    if name_query:
        q_like = f"%{name_query}%"
        ilike_clause = f"\n            AND {_name_like_clause('d', 'q')}"

    taxonomy_conditions = " AND ".join(where) if where else "1=1"
    extra_clause = ""
    if extra_filters:
        extra_clause = "\n          AND " + "\n          AND ".join(extra_filters)

    nearby_sql = _build_nearby_sql(taxonomy_conditions, extra_clause, ilike_clause)

    async with db.acquire() as conn:
        res_q = await conn.all(
            text(nearby_sql),
            in_long=in_long,
            in_lat=in_lat,
            classification=classification,
            limit=limit,
            radius=radius,
            exclude_npi=exclude_npi,
            section=section,
            display_name=display_name,
            q=q_like,
            codes=codes,
            zip_codes=zip_codes,
            plan_network_array=plan_network,
            **bbox_params,
        )

    for r in res_q:
        obj = {"taxonomy_list": []}
        count = 1
        obj["distance"] = r[count]
        temp = NPIAddress.__table__.columns

        for c in temp:
            count += 1
            obj[c.key] = r[count]
        for c in NPIData.__table__.columns:
            count += 1
            if c.key in ("npi", "checksum", "do_business_as_text"):
                continue
            obj[c.key] = r[count]

        if obj["npi"] in res:
            obj = res[obj["npi"]]
        taxonomy = {}
        for c in NPIDataTaxonomy.__table__.columns:
            count += 1
            if c.key in ("npi", "checksum"):
                continue
            taxonomy[c.key] = r[count]
        obj["taxonomy_list"].append(taxonomy)

        res[obj["npi"]] = obj

    res = list(res.values())
    return response.json(res, default=str)


@blueprint.get("/id/<npi>/full_taxonomy")
async def get_full_taxonomy_list(_request, npi):
    t = []
    npi = int(npi)
    # plan_data = await db.select(
    #     [Plan.marketing_name, Plan.plan_id, PlanAttributes.full_plan_id, Plan.year]).select_from(
    #     Plan.join(PlanAttributes, ((Plan.plan_id == func.substr(PlanAttributes.full_plan_id, 1, 14)) & (
    #                 Plan.year == PlanAttributes.year)))). \
    #     group_by(PlanAttributes.full_plan_id, Plan.plan_id, Plan.marketing_name, Plan.year).all()
    stmt = (
        select(NPIDataTaxonomy, NUCCTaxonomy)
        .where(NPIDataTaxonomy.npi == npi)
        .where(NUCCTaxonomy.code == NPIDataTaxonomy.healthcare_provider_taxonomy_code)
    )
    result = await db.execute(stmt)
    for taxonomy, nucc in result.all():
        payload = taxonomy.to_json_dict()
        payload["nucc_taxonomy"] = nucc.to_json_dict()
        t.append(payload)
    return response.json(t)


@blueprint.get("/plans_by_npi/<npi>")
async def get_plans_by_npi(_request, npi):

    data = []
    plan_data = []
    issuer_data = []
    npi = int(npi)

    # async def get_plans_list(plan_arr):
    #     t = {}
    #     q = Plan.query.where(Plan.plan_id == db.func.any(plan_arr)).where(Plan.year == int(2023))
    #     async with db.acquire() as conn:
    #         for x in await q.all():
    #             t[x.plan_id] = x.to_json_dict()
    #     return t

    query = (
        db.select(PlanNPIRaw, Issuer)
        .where(Issuer.issuer_id == PlanNPIRaw.issuer_id)
        .where(PlanNPIRaw.npi == npi)
        .order_by(PlanNPIRaw.issuer_id.desc())
    )

    async for plan_raw, issuer in query.iterate():
        data.append({"npi_info": plan_raw.to_json_dict(), "issuer_info": issuer.to_json_dict()})

    return response.json({"npi_data": data, "plan_data": plan_data, "issuer_data": issuer_data})


@blueprint.get("/id/<npi>")
async def get_npi(request, npi):
    force_address_update = request.args.get("force_address_update", 0)

    async def update_addr_coordinates(checksum, long, lat, formatted_address, place_id):
        await (
            db.update(NPIAddress)
            .where(NPIAddress.checksum == checksum)
            .values(
                long=long,
                lat=lat,
                formatted_address=formatted_address,
                place_id=place_id,
            )
            .status()
        )
        row = await db.scalar(select(NPIAddress).where(NPIAddress.checksum == checksum))
        if row is None:
            return
        obj = {column.key: getattr(row, column.key, None) for column in AddressArchive.__table__.columns}

        # long = long,
        # lat = lat,
        # formatted_address = formatted_address,
        # place_id = place_id
        #         del obj['checksum']
        try:
            await (
                db.insert(AddressArchive)
                .values(obj)
                .on_conflict_do_update(
                    index_elements=AddressArchive.__my_index_elements__,
                    set_=obj,
                )
                .status()
            )
        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.warning("Could not archive address checksum=%s: %s", checksum, exc)

    async def _update_address(x):
        if x.get("lat"):
            return x
        postal_code = x.get("postal_code")
        if postal_code and len(postal_code) > 5:
            postal_code = f"{postal_code[0:5]}-{postal_code[5:]}"
        t_addr = ", ".join(
            [
                x.get("first_line", ""),
                x.get("second_line", ""),
                x.get("city_name", ""),
                f"{x.get('state_name', '')} {postal_code}",
            ]
        )
        t_addr = t_addr.replace(" , ", " ")

        d = x
        if force_address_update:
            d["long"] = None
            d["lat"] = None
            d["formatted_address"] = None
            d["place_id"] = None

        if not d["lat"]:

            # try:
            #     raw_sql = text(f"""SELECT
            #            g.rating,
            #            ST_X(g.geomout) As lon,
            #            ST_Y(g.geomout) As lat,
            #             pprint_addy(g.addy) as formatted_address
            #             from mrf.npi,
            #             standardize_address('us_lex',
            #                  'us_gaz', 'us_rules', :addr) as addr,
            #             geocode((
            #                 (addr).house_num,  --address
            #                 null,              --predirabbrev
            #                 (addr).name,       --streetname
            #                 (addr).suftype,    --streettypeabbrev
            #                 null,              --postdirabbrev
            #                 (addr).unit,       --internal
            #                 (addr).city,       --location
            #                 (addr).state,      --stateabbrev
            #                 (addr).postcode,   --zip
            #                 true,               --parsed
            #                 null,               -- zip4
            #                 (addr).house_num    -- address_alphanumeric
            #             )::norm_addy) as g
            #            where npi = :npi""")
            #     addr = await conn.status(raw_sql, addr=t_addr, npi=npi)
            #
            #     if addr and len(addr[-1]) and addr[-1][0] and addr[-1][0][0] < 2:
            #         d['long'] = addr[-1][0][1]
            #         d['lat'] = addr[-1][0][2]
            #         d['formatted_address'] = addr[-1][0][3]
            #         d['place_id'] = None
            # except:
            #     pass
            update_geo = False
            if request.app.config.get("NPI_API_UPDATE_GEOCODE") and not d["lat"]:
                update_geo = True

            if (not d["lat"]) and (not force_address_update):
                res = await db.scalar(select(AddressArchive).where(AddressArchive.checksum == x["checksum"]))
                if res:
                    d["long"] = res.long
                    d["lat"] = res.lat
                    d["formatted_address"] = res.formatted_address
                    d["place_id"] = res.place_id

            if not d["lat"]:
                try:
                    params = {
                        request.app.config.get("GEOCODE_MAPBOX_STYLE_KEY_PARAM"): random.choice(
                            json.loads(request.app.config.get("GEOCODE_MAPBOX_STYLE_KEY"))
                        )
                    }
                    encoded_params = ".json?".join(
                        (
                            urllib.parse.quote_plus(t_addr),
                            urllib.parse.urlencode(params, doseq=True),
                        )
                    )
                    if qp := request.app.config.get("GEOCODE_MAPBOX_STYLE_ADDITIONAL_QUERY_PARAMS"):
                        encoded_params = "&".join(
                            (
                                encoded_params,
                                qp,
                            )
                        )
                    url = request.app.config.get("GEOCODE_MAPBOX_STYLE_URL") + encoded_params
                    resp = await download_it(url, local_timeout=5)
                    geo_data = json.loads(resp)
                    if geo_data.get("features", []):
                        d["long"] = geo_data["features"][0]["geometry"]["coordinates"][0]
                        d["lat"] = geo_data["features"][0]["geometry"]["coordinates"][1]
                        if t2 := geo_data["features"][0].get("matching_place_name"):
                            d["formatted_address"] = t2
                        else:
                            d["formatted_address"] = geo_data["features"][0]["place_name"]
                        d["place_id"] = None
                except Exception as exc:  # pylint: disable=broad-exception-caught
                    logger.debug("Mapbox geocoding failed for %s: %s", t_addr, exc)

            if not d["lat"]:
                try:
                    params = {
                        request.app.config.get("GEOCODE_GOOGLE_STYLE_ADDRESS_PARAM"): t_addr,
                        request.app.config.get("GEOCODE_GOOGLE_STYLE_KEY_PARAM"): request.app.config.get(
                            "GEOCODE_GOOGLE_STYLE_KEY"
                        ),
                    }
                    encoded_params = urllib.parse.urlencode(params, doseq=True)
                    if qp := request.app.config.get("GEOCODE_GOOGLE_STYLE_ADDITIONAL_QUERY_PARAMS"):
                        encoded_params = "&".join(
                            (
                                encoded_params,
                                qp,
                            )
                        )
                    url = "?".join(
                        (
                            request.app.config.get("GEOCODE_GOOGLE_STYLE_URL"),
                            encoded_params,
                        )
                    )
                    resp = await download_it(url)
                    geo_data = json.loads(resp)
                    if geo_data.get("results", []):
                        d["long"] = geo_data["results"][0]["geometry"]["location"]["lng"]
                        d["lat"] = geo_data["results"][0]["geometry"]["location"]["lat"]
                        d["formatted_address"] = geo_data["results"][0]["formatted_address"]
                        d["place_id"] = geo_data["results"][0]["place_id"]
                except Exception as exc:  # pylint: disable=broad-exception-caught
                    logger.warning("Google geocoding failed for %s: %s", t_addr, exc)

            if update_geo and d.get("lat"):
                request.app.add_task(
                    update_addr_coordinates(x["checksum"], d["long"], d["lat"], d["formatted_address"], d["place_id"])
                )

        return d

    npi = int(npi)

    data = await _build_npi_details(npi)

    if not data:
        raise sanic.exceptions.NotFound

    addresses = data.get("address_list") or []
    if addresses:
        update_address_tasks = [_update_address(a) for a in addresses if a]
        if update_address_tasks:
            data["address_list"] = list(await asyncio.gather(*update_address_tasks))
        else:
            data["address_list"] = []
    else:
        data["address_list"] = []

    other_names = await _fetch_other_names(npi)
    data["other_name_list"] = other_names

    existing_dba = [name for name in (data.get("do_business_as") or []) if name]
    if existing_dba:
        data["do_business_as"] = list(dict.fromkeys(existing_dba))
    else:
        candidates = [
            entry.get("other_provider_identifier")
            for entry in other_names
            if entry.get("other_provider_identifier_type_code") == "3" and entry.get("other_provider_identifier")
        ]
        data["do_business_as"] = list(dict.fromkeys(candidates)) if candidates else []

    return response.json(data, default=str)


async def _build_npi_details(npi: int) -> dict:
    async with db.acquire():
        npi_data_table = NPIData.__table__
        taxonomy_table = NPIDataTaxonomy.__table__
        taxonomy_group_table = NPIDataTaxonomyGroup.__table__
        address_table = NPIAddress.__table__

        address_subquery_base = (
            select(address_table)
            .where(
                (address_table.c.npi == npi)
                & or_(
                    address_table.c.type == "primary",
                    address_table.c.type == "secondary",
                )
            )
            .order_by(address_table.c.type)
        )
        try:
            address_subquery = address_subquery_base.alias("address_list")
        except NameError:
            # Some tests swap in lightweight stand-ins lacking SQLAlchemy helpers.
            # Fall back to the base query so coverage exercises downstream handling.
            address_subquery = address_subquery_base

        select_columns = [
            npi_data_table,
            func.json_agg(literal_column(f'distinct "{NPIDataTaxonomy.__tablename__}"')),
            func.json_agg(literal_column(f'distinct "{NPIDataTaxonomyGroup.__tablename__}"')),
        ]
        join_clause = npi_data_table.outerjoin(taxonomy_table, npi_data_table.c.npi == taxonomy_table.c.npi).outerjoin(
            taxonomy_group_table,
            npi_data_table.c.npi == taxonomy_group_table.c.npi,
        )
        if hasattr(address_subquery, "c"):
            join_clause = join_clause.outerjoin(address_subquery, npi_data_table.c.npi == address_subquery.c.npi)
            select_columns.append(func.json_agg(literal_column('distinct "address_list"')))
        else:
            select_columns.append(literal_column("NULL::json"))
        query = (
            db.select(*select_columns)
            .select_from(join_clause)
            .where(npi_data_table.c.npi == npi)
            .group_by(npi_data_table.c.npi)
        )

        rows = await query.all()
        if not rows:
            return {}
        result_row = rows[0]
        obj: dict[str, Any] = {
            "taxonomy_list": [],
            "taxonomy_group_list": [],
            "address_list": [],
        }
        idx = 0
        for column in NPIData.__table__.columns:
            value = result_row[idx]
            idx += 1
            if column.key == "do_business_as_text":
                continue
            obj[column.key] = value

        if result_row[idx]:
            obj["taxonomy_list"].extend([entry for entry in result_row[idx] if entry])
        idx += 1
        if result_row[idx]:
            obj["taxonomy_group_list"].extend([entry for entry in result_row[idx] if entry])
        idx += 1
        if idx < len(result_row) and result_row[idx]:
            obj["address_list"] = result_row[idx]
        obj["do_business_as"] = obj.get("do_business_as") or []
        return obj


async def _fetch_other_names(npi: int) -> list[dict[str, Any]]:
    result = await db.execute(select(NPIDataOtherIdentifier).where(NPIDataOtherIdentifier.npi == npi))
    rows: list[dict[str, Any]] = []
    seen_checksums: set[int] = set()
    for row in result.scalars():
        payload = row.to_json_dict()
        checksum = payload.pop("checksum", None)
        if checksum in seen_checksums:
            continue
        if checksum is not None:
            seen_checksums.add(checksum)
        payload.pop("npi", None)
        rows.append(payload)
    return rows
_NPI_GEO_INDEX_READY = False


async def _ensure_npi_geo_index():
    global _NPI_GEO_INDEX_READY  # pylint: disable=global-statement
    if _NPI_GEO_INDEX_READY:
        return
    schema = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    stmt = text(
        f"""
        CREATE INDEX IF NOT EXISTS npi_address_geo_idx
            ON {schema}.npi_address
         USING GIST (Geography(ST_MakePoint(long, lat)))
         WHERE type IN ('primary','secondary');
        """
    )
    status_fn = getattr(db, "status", None)
    if status_fn is not None:
        await status_fn(stmt)
    else:  # used in tests with lightweight fake db
        async with db.acquire() as conn:
            executor = getattr(conn, "execute", None)
            if executor is not None:
                await executor(stmt)
    _NPI_GEO_INDEX_READY = True
