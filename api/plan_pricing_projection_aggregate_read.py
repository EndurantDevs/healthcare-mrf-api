# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Read bounded immutable ZIP2 aggregate packs."""

from __future__ import annotations

import hashlib
import hmac
import re
from decimal import Decimal
from typing import Any, Mapping

import orjson
from sqlalchemy import Integer, bindparam, text
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.exc import DBAPIError

from api.plan_pricing_aggregate_pack import (
    MAX_AGGREGATE_PACK_RECORDS,
    AggregateCodeIdentity,
    AggregatePackError,
    AggregatePackKey,
    AggregateZipRecord,
    aggregate_logical_digest,
    aggregate_pack_raw_byte_count,
    decode_aggregate_pack,
)
from api.plan_pricing_projection_contract import (
    PlanPricingProjectionUnavailable,
    PlanPricingProjectionUnsupported,
    table,
)
from api.plan_pricing_projection_materialize import rate_fragment
from api.plan_pricing_projection_v3_aggregate import _aggregate_zip_records

MAX_AGGREGATE_REQUEST_PREFIXES = 16
MAX_AGGREGATE_REQUEST_STORED_BYTES = 9 * 1024 * 1024


def _aggregate_fragment(record: AggregateZipRecord) -> orjson.Fragment:
    return orjson.Fragment(
        orjson.dumps(
            {
                "geo_cell": record.zip5,
                "provider_count": record.provider_count,
                "rate_count": record.rate_count,
                "minimum_negotiated_rate": rate_fragment(record.minimum_negotiated_rate),
                "median_negotiated_rate": rate_fragment(record.median_negotiated_rate),
                "maximum_negotiated_rate": rate_fragment(record.maximum_negotiated_rate),
            }
        )
    )


def _verified_payload(aggregate_row_by_field: Mapping[str, Any]) -> bytes:
    encoded_payload = bytes(aggregate_row_by_field["payload"])
    if int(aggregate_row_by_field["stored_byte_count"]) != len(encoded_payload) or not hmac.compare_digest(
        bytes(aggregate_row_by_field["payload_sha256"]),
        hashlib.sha256(encoded_payload).digest(),
    ):
        raise PlanPricingProjectionUnavailable("the selected aggregate projection payload is invalid")
    return encoded_payload


def _decoded_records(
    request: Any,
    code_identity: AggregateCodeIdentity,
    aggregate_row_by_field: Mapping[str, Any],
) -> tuple[AggregateZipRecord, ...]:
    encoded_payload = _verified_payload(aggregate_row_by_field)
    aggregate_pack_key = AggregatePackKey(
        request.projection_id,
        code_identity,
        str(aggregate_row_by_field["zip_prefix_2"]),
    )
    try:
        aggregate_pack_raw_byte_count(
            encoded_payload,
            expected_raw_byte_count=int(aggregate_row_by_field["raw_byte_count"]),
        )
        decoded_pack = decode_aggregate_pack(
            encoded_payload,
            expected_key=aggregate_pack_key,
        )
    except AggregatePackError as exc:
        raise PlanPricingProjectionUnavailable("the selected aggregate projection is invalid") from exc
    if len(decoded_pack.records) != int(aggregate_row_by_field["entry_count"]) or aggregate_logical_digest(
        code_identity, decoded_pack.records
    ) != str(aggregate_row_by_field["logical_digest"]):
        raise PlanPricingProjectionUnavailable("the selected aggregate projection receipt is invalid")
    return decoded_pack.records


def _selected_records(
    aggregate_rows: Any,
    request: Any,
    selected_geo_cells: frozenset[str],
) -> list[AggregateZipRecord]:
    code_identity = AggregateCodeIdentity(request.code_system, request.code)
    selected_records: list[AggregateZipRecord] = []
    selected_payload_bytes = 0
    for raw_aggregate_row in aggregate_rows:
        aggregate_row_by_field = dict(raw_aggregate_row)
        selected_payload_bytes += int(aggregate_row_by_field["stored_byte_count"])
        if selected_payload_bytes > MAX_AGGREGATE_REQUEST_STORED_BYTES:
            raise PlanPricingProjectionUnsupported("aggregate projection exceeds its request byte bound")
        selected_records.extend(
            record
            for record in _decoded_records(request, code_identity, aggregate_row_by_field)
            if record.zip5 in selected_geo_cells
        )
    return selected_records


async def read_aggregate_pack_page(
    session: Any,
    request: Any,
    geo_cells: list[str],
    args: Mapping[str, Any],
    pagination: Any,
    *,
    binding_ordinals: tuple[int, ...] | None = None,
) -> tuple[list[orjson.Fragment], int]:
    """Read one exact page while retaining only requested decoded cells."""

    if binding_ordinals is not None:
        page_records = await _partial_aggregate_records(session, request, geo_cells, binding_ordinals)
        return _aggregate_page(page_records, args, pagination)
    zip_prefixes = sorted({geo_cell[:2] for geo_cell in geo_cells})
    if len(zip_prefixes) > MAX_AGGREGATE_REQUEST_PREFIXES:
        raise PlanPricingProjectionUnsupported("aggregate projection spans too many ZIP prefixes")
    aggregate_result = await session.execute(
        text(
            f"""
            SELECT zip_prefix_2, entry_count, raw_byte_count,
                   stored_byte_count, logical_digest, payload_sha256, payload
              FROM {table("plan_pricing_aggregate_pack")}
             WHERE projection_id = :projection_id
               AND code_system = :code_system AND code = :code
               AND zip_prefix_2 = ANY(CAST(:zip_prefixes AS varchar[]))
             ORDER BY zip_prefix_2
            """
        ),
        {
            "projection_id": request.projection_id,
            "code_system": request.code_system,
            "code": request.code,
            "zip_prefixes": zip_prefixes,
        },
    )
    page_records = _selected_records(aggregate_result.mappings(), request, frozenset(geo_cells))
    return _aggregate_page(page_records, args, pagination)


def _aggregate_page(
    page_records: list[AggregateZipRecord], args: Mapping[str, Any], pagination: Any
) -> tuple[list[orjson.Fragment], int]:
    """Retain the packed reader's deterministic price, ZIP and page semantics."""

    is_descending_order = str(args.get("order") or "asc").strip().lower() == "desc"
    page_records.sort(key=lambda aggregate_record: aggregate_record.zip5)
    page_records.sort(
        key=lambda aggregate_record: aggregate_record.minimum_negotiated_rate,
        reverse=is_descending_order,
    )
    result_count = len(page_records)
    page_start = int(pagination.offset)
    page_records = page_records[page_start : page_start + int(pagination.limit)]
    return [_aggregate_fragment(aggregate_record) for aggregate_record in page_records], result_count


_MAX_PARTIAL_PROFILES = 65_536
_MAX_PARTIAL_MEMBERSHIPS = 65_536
_MAX_PARTIAL_RATE_VALUES = 1_048_576
_PARTIAL_AGGREGATE_SQL = """
WITH profile_window AS MATERIALIZED (
    SELECT binding_ordinal, provider_set_key, membership_count,
           cardinality(negotiated_rates)::bigint AS rate_value_count
      FROM {profile_table}
     WHERE projection_id = :projection_id
       AND code_system = :code_system AND code = :code
       AND binding_ordinal = ANY(CAST(:binding_ordinals AS integer[]))
     ORDER BY binding_ordinal, provider_set_key
     LIMIT :profile_probe_limit
), profile_admission AS MATERIALIZED (
    SELECT COUNT(*)::bigint AS profile_count,
           COALESCE(BOOL_AND(COALESCE(membership_count > 0 AND rate_value_count > 0, FALSE)), TRUE) AS memberships_valid
      FROM profile_window
), member_window AS MATERIALIZED (
    SELECT member.npi, member.binding_ordinal, member.provider_set_key,
           provider.geo_cell
      FROM profile_window profile
      JOIN {membership_table} member
        ON member.projection_id = :projection_id
       AND member.binding_ordinal = profile.binding_ordinal
       AND member.provider_set_key = profile.provider_set_key
      JOIN {provider_table} provider
        ON provider.projection_id = member.projection_id AND provider.npi = member.npi
     CROSS JOIN profile_admission
     WHERE profile_admission.profile_count <= :profile_limit
       AND member.binding_ordinal = ANY(CAST(:binding_ordinals AS integer[]))
       AND provider.geo_cell = ANY(CAST(:geo_cells AS varchar[]))
     ORDER BY member.binding_ordinal, member.provider_set_key, member.npi, provider.geo_cell
     LIMIT :membership_probe_limit
), member_admission AS MATERIALIZED (
    SELECT COUNT(*)::bigint AS membership_count FROM member_window
), set_cells AS MATERIALIZED (
    SELECT DISTINCT binding_ordinal, provider_set_key, geo_cell
      FROM member_window CROSS JOIN member_admission
     WHERE member_admission.membership_count <= :membership_limit
), profile_scope AS MATERIALIZED (
    SELECT profile.*, cell.geo_cell
      FROM set_cells cell JOIN profile_window profile
        USING (binding_ordinal, provider_set_key)
), rate_admission AS MATERIALIZED (
    SELECT COALESCE(SUM(rate_value_count), 0)::bigint AS rate_value_count
      FROM profile_scope
), admitted_profiles AS MATERIALIZED (
    SELECT profile.*, scope.geo_cell
      FROM profile_scope scope JOIN {profile_table} profile
        ON profile.projection_id = :projection_id
       AND profile.code_system = :code_system AND profile.code = :code
       AND profile.binding_ordinal = scope.binding_ordinal
       AND profile.provider_set_key = scope.provider_set_key
     CROSS JOIN rate_admission
     WHERE rate_admission.rate_value_count <= :rate_value_limit
), profile_validation AS MATERIALIZED (
    SELECT COALESCE(BOOL_AND(COALESCE(
        cardinality(negotiated_rates) > 0
        AND array_ndims(negotiated_rates) = 1 AND array_lower(negotiated_rates, 1) = 1
        AND array_ndims(rate_multiplicities) = 1 AND array_lower(rate_multiplicities, 1) = 1
        AND cardinality(negotiated_rates) = cardinality(rate_multiplicities)
        AND minimum_negotiated_rate = negotiated_rates[1]
        AND maximum_negotiated_rate = negotiated_rates[cardinality(negotiated_rates)]
        AND rate_count = (SELECT SUM(multiplicity) FROM unnest(rate_multiplicities) AS listed(multiplicity))
        AND NOT EXISTS (
            SELECT 1 FROM unnest(negotiated_rates) AS listed(rate)
             WHERE rate IS NULL OR rate < 0 OR rate::text IN ('NaN', 'Infinity', '-Infinity')
        )
        AND NOT EXISTS (
            SELECT 1 FROM unnest(rate_multiplicities) AS listed(multiplicity)
             WHERE multiplicity IS NULL OR multiplicity <= 0
        )
        AND NOT EXISTS (
            SELECT 1 FROM generate_subscripts(negotiated_rates, 1) AS indexes(rate_index)
             WHERE rate_index > 1 AND negotiated_rates[rate_index] <= negotiated_rates[rate_index - 1]
        )
    , FALSE)), TRUE) AS profiles_valid FROM admitted_profiles
), metadata AS MATERIALIZED (
    SELECT profile_count, member_admission.membership_count, rate_value_count,
           memberships_valid AND profiles_valid AS profiles_valid
      FROM profile_admission CROSS JOIN member_admission
     CROSS JOIN rate_admission CROSS JOIN profile_validation
), provider_stats AS MATERIALIZED (
    SELECT geo_cell, COUNT(DISTINCT npi)::bigint AS provider_count
      FROM member_window GROUP BY geo_cell
), rate_frequency AS MATERIALIZED (
    SELECT profile.geo_cell, rate.negotiated_rate,
           SUM(rate.multiplicity)::bigint AS frequency
      FROM admitted_profiles profile CROSS JOIN metadata
     CROSS JOIN LATERAL unnest(profile.negotiated_rates, profile.rate_multiplicities)
          AS rate(negotiated_rate, multiplicity)
     WHERE metadata.profiles_valid
     GROUP BY profile.geo_cell, rate.negotiated_rate
), ranked AS MATERIALIZED (
    SELECT geo_cell, negotiated_rate, frequency,
           SUM(frequency) OVER (PARTITION BY geo_cell)::bigint AS total,
           SUM(frequency) OVER (
               PARTITION BY geo_cell ORDER BY negotiated_rate
               ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
           )::bigint AS cumulative FROM rate_frequency
), aggregates AS MATERIALIZED (
    SELECT ranked.geo_cell, provider.provider_count, MAX(ranked.total)::bigint AS rate_count,
           MIN(ranked.negotiated_rate) AS minimum_negotiated_rate,
           MIN(ranked.negotiated_rate) FILTER (
               WHERE ranked.cumulative >= (ranked.total + 1) / 2
           ) AS median_lower,
           MIN(ranked.negotiated_rate) FILTER (
               WHERE ranked.cumulative >= (ranked.total + 2) / 2
           ) AS median_upper,
           MAX(ranked.negotiated_rate) AS maximum_negotiated_rate
      FROM ranked JOIN provider_stats provider USING (geo_cell)
     GROUP BY ranked.geo_cell, provider.provider_count
)
 , result_admission AS MATERIALIZED (
    SELECT COALESCE(SUM(octet_length(row_to_json(aggregates)::text)), 0)::bigint AS result_byte_count
      FROM aggregates
)
SELECT metadata.*, result_admission.result_byte_count, aggregates.*
  FROM metadata CROSS JOIN result_admission
  LEFT JOIN aggregates ON result_admission.result_byte_count <= :result_byte_limit
 ORDER BY aggregates.geo_cell LIMIT :result_probe_limit
"""


def _partial_aggregate_query(binding_ordinals: tuple[int, ...]) -> Any:
    """Bind original contributor ordinals before either native work probe."""

    if (
        type(binding_ordinals) is not tuple
        or len(binding_ordinals) > 5000
        or any(type(ordinal) is not int or not 0 <= ordinal <= 2_147_483_647 for ordinal in binding_ordinals)
        or len(set(binding_ordinals)) != len(binding_ordinals)
    ):
        raise PlanPricingProjectionUnavailable("aggregate binding ordinals are invalid")
    return text(
        _PARTIAL_AGGREGATE_SQL.format(
            profile_table=table("plan_pricing_rate_profile"),
            membership_table=table("plan_pricing_provider_membership"),
            provider_table=table("plan_pricing_provider_cell"),
        )
    ).bindparams(bindparam("binding_ordinals", list(binding_ordinals), type_=ARRAY(Integer)))


def _partial_aggregate_receipt(aggregate_rows: list[Mapping[str, Any]]) -> None:
    """Refuse incomplete work explicitly rather than returning a partial total."""

    if not aggregate_rows:
        raise PlanPricingProjectionUnavailable("aggregate admission receipt is missing")
    metadata_fields = ("profile_count", "membership_count", "rate_value_count", "result_byte_count", "profiles_valid")
    metadata = tuple(aggregate_rows[0].get(field) for field in metadata_fields)
    if any(
        tuple(aggregate_row.get(field) for field in metadata_fields) != metadata for aggregate_row in aggregate_rows
    ):
        raise PlanPricingProjectionUnavailable("aggregate admission receipt is inconsistent")
    if any(type(count) is not int or count < 0 for count in metadata[:4]):
        raise PlanPricingProjectionUnavailable("aggregate admission receipt is invalid")
    limits = (_MAX_PARTIAL_PROFILES, _MAX_PARTIAL_MEMBERSHIPS, _MAX_PARTIAL_RATE_VALUES)
    if any(count > limit for count, limit in zip(metadata[:3], limits, strict=True)):
        raise PlanPricingProjectionUnsupported("aggregate projection exceeds its native work bound")
    if metadata[3] > MAX_AGGREGATE_REQUEST_STORED_BYTES:
        raise PlanPricingProjectionUnsupported("aggregate projection exceeds its request byte bound")
    if metadata[4] is not True:
        raise PlanPricingProjectionUnavailable("aggregate rate profiles are invalid")


def _partial_aggregate_rows(aggregate_result: Any, geo_cells: frozenset[str]) -> list[AggregateZipRecord]:
    """Validate bounded scalar aggregate rows using the existing exact decoder."""

    aggregate_rows: list[Mapping[str, Any]] = []
    for aggregate_row in aggregate_result.mappings():
        if len(aggregate_rows) >= max(len(geo_cells), 1):
            raise PlanPricingProjectionUnsupported("aggregate projection exceeds its result row bound")
        aggregate_rows.append(dict(aggregate_row))
    _partial_aggregate_receipt(aggregate_rows)
    if len(aggregate_rows) == 1 and aggregate_rows[0].get("geo_cell") is None:
        if aggregate_rows[0]["membership_count"] or aggregate_rows[0]["rate_value_count"]:
            raise PlanPricingProjectionUnavailable("aggregate result is incomplete")
        return []
    try:
        for aggregate_row in aggregate_rows:
            if any(type(aggregate_row.get(field)) is not int for field in ("provider_count", "rate_count")):
                raise ValueError("invalid aggregate count")
            raw_rates = tuple(
                aggregate_row.get(field)
                for field in ("minimum_negotiated_rate", "median_lower", "median_upper", "maximum_negotiated_rate")
            )
            if any(isinstance(rate, (bool, float)) for rate in raw_rates):
                raise ValueError("inexact aggregate rate")
            exact_rates = tuple(Decimal(rate) for rate in raw_rates)
            if not all(rate.is_finite() for rate in exact_rates) or tuple(sorted(exact_rates)) != exact_rates:
                raise ValueError("invalid aggregate rates")
        aggregate_records = list(_aggregate_zip_records(aggregate_rows))
        if len({aggregate_record.zip5 for aggregate_record in aggregate_records}) != len(aggregate_records):
            raise ValueError("duplicate aggregate cell")
        if any(aggregate_record.zip5 not in geo_cells for aggregate_record in aggregate_records):
            raise ValueError("foreign aggregate cell")
        response_bytes = sum(
            len(orjson.dumps(_aggregate_fragment(aggregate_record))) for aggregate_record in aggregate_records
        )
    except AggregatePackError, KeyError, TypeError, ValueError, ArithmeticError:
        raise PlanPricingProjectionUnavailable("aggregate result is invalid") from None
    if response_bytes > MAX_AGGREGATE_REQUEST_STORED_BYTES:
        raise PlanPricingProjectionUnsupported("aggregate projection exceeds its request byte bound")
    return aggregate_records


async def _partial_aggregate_records(
    session: Any, request: Any, geo_cells: list[str], binding_ordinals: tuple[int, ...]
) -> list[AggregateZipRecord]:
    """Compute one exact bounded attributed aggregate in the supplied Reader."""

    statement = _partial_aggregate_query(binding_ordinals)
    if len(geo_cells) > MAX_AGGREGATE_REQUEST_PREFIXES * MAX_AGGREGATE_PACK_RECORDS or any(
        type(cell) is not str or re.fullmatch(r"[0-9]{5}", cell, flags=re.ASCII) is None for cell in geo_cells
    ):
        raise PlanPricingProjectionUnsupported("aggregate geographic cells are invalid")
    if len({cell[:2] for cell in geo_cells}) > MAX_AGGREGATE_REQUEST_PREFIXES:
        raise PlanPricingProjectionUnsupported("aggregate projection spans too many ZIP prefixes")
    try:
        AggregateCodeIdentity(request.code_system, request.code)
        if re.fullmatch(r"[0-9a-f]{64}", request.projection_id, flags=re.ASCII) is None:
            raise AggregatePackError("invalid projection")
    except AggregatePackError, TypeError:
        raise PlanPricingProjectionUnavailable("aggregate request identity is invalid") from None
    if not binding_ordinals or not geo_cells:
        return []
    try:
        aggregate_result = await session.execute(
            statement,
            {
                "projection_id": request.projection_id,
                "code_system": request.code_system,
                "code": request.code,
                "geo_cells": sorted(set(geo_cells)),
                "profile_limit": _MAX_PARTIAL_PROFILES,
                "profile_probe_limit": _MAX_PARTIAL_PROFILES + 1,
                "membership_limit": _MAX_PARTIAL_MEMBERSHIPS,
                "membership_probe_limit": _MAX_PARTIAL_MEMBERSHIPS + 1,
                "rate_value_limit": _MAX_PARTIAL_RATE_VALUES,
                "result_probe_limit": len(set(geo_cells)) + 1,
                "result_byte_limit": MAX_AGGREGATE_REQUEST_STORED_BYTES,
            },
        )
    except DBAPIError:
        raise PlanPricingProjectionUnavailable("aggregate native read is unavailable") from None
    return _partial_aggregate_rows(aggregate_result, frozenset(geo_cells))
