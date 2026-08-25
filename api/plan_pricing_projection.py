# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Immutable provider-card and cell-aggregate serving projection."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
import time
from collections import defaultdict
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable, Mapping

import orjson
from sqlalchemy import text

from api import ptg2_geo_projection as geo_projection
from api.code_systems import canonical_catalog_code, normalize_code_system
from api.plan_release_serving import (
    PlanReleaseServingSelection,
    annotate_plan_release_response,
)
from api.ptg2_response import _is_request_flag_enabled
from api.ptg2_tables import snapshot_serving_tables
from db.connection import db


PROJECTION_CONTRACT = "plan_pricing_card_v2"
_SCHEMA = os.getenv("HLTHPRT_DB_SCHEMA") or os.getenv("DB_SCHEMA") or "mrf"
_HEX_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_ZIP5 = re.compile(r"^[0-9]{5}$")
_PROVIDER_BATCH_SIZE = 5_000
_INSERT_BATCH_SIZE = 1_000
_MAX_GEO_CELLS = 512
_COST_ORDER_FIELDS = frozenset(
    {
        "total_allowed_amount",
        "total_drug_cost",
        "cost",
        "price",
        "rate",
        "negotiated_rate",
        "amount",
    }
)
_PROVIDER_RELATIONS = (
    "npi",
    "npi_taxonomy",
    "nucc_taxonomy",
    "entity_address_unified",
    "entity_address_geo_assurance_state",
)


class PlanPricingProjectionUnsupported(ValueError):
    """The requested card shape cannot be answered without changing semantics."""


class PlanPricingProjectionUnavailable(RuntimeError):
    """The selected immutable release has no ready pricing projection."""


def _table(name: str) -> str:
    return f'"{_SCHEMA}"."{name}"'


def _row_mapping(row: Any) -> dict[str, Any]:
    return dict(getattr(row, "_mapping", row))


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _projection_id(binding_digest: str, provider_signature: str) -> str:
    payload = f"{PROJECTION_CONTRACT}\0{binding_digest}\0{provider_signature}"
    return hashlib.sha256(payload.encode("ascii")).hexdigest()


def _normalized_bindings(bindings: Any) -> list[dict[str, Any]]:
    if not isinstance(bindings, list) or not bindings:
        raise ValueError("pricing projection bindings must be a non-empty array")
    normalized: list[dict[str, Any]] = []
    for raw_binding in bindings:
        if not isinstance(raw_binding, Mapping):
            raise ValueError("pricing projection bindings must be objects")
        binding = dict(raw_binding)
        if not all(
            str(binding.get(field) or "").strip()
            for field in ("snapshot_id", "source_key", "plan_id", "role")
        ):
            raise ValueError("pricing projection binding is incomplete")
        try:
            ordinal = int(binding.get("ordinal", binding.get("binding_ordinal")))
        except (TypeError, ValueError) as exc:
            raise ValueError("pricing projection binding ordinal is invalid") from exc
        if ordinal < 0:
            raise ValueError("pricing projection binding ordinal is invalid")
        normalized.append(binding)
    return normalized


async def _provider_signature(session: Any) -> str:
    """Bind a candidate to the atomically published provider-side relations."""

    signature_result = await session.execute(
        text(
            f"""
            SELECT jsonb_build_object(
                'npi', jsonb_build_array(
                    to_regclass(:npi_relation)::oid,
                    pg_relation_filenode(to_regclass(:npi_relation))
                ),
                'taxonomy', jsonb_build_array(
                    to_regclass(:taxonomy_relation)::oid,
                    pg_relation_filenode(to_regclass(:taxonomy_relation))
                ),
                'vocabulary', jsonb_build_array(
                    to_regclass(:vocabulary_relation)::oid,
                    pg_relation_filenode(to_regclass(:vocabulary_relation))
                ),
                'address', jsonb_build_array(
                    to_regclass(:address_relation)::oid,
                    pg_relation_filenode(to_regclass(:address_relation))
                ),
                'zip', jsonb_build_array(
                    to_regclass(:zip_relation)::oid,
                    pg_relation_filenode(to_regclass(:zip_relation))
                ),
                'geo_assurance', COALESCE((
                    SELECT jsonb_build_object(
                        'version', active_geo_assurance_version,
                        'table_oid', active_table_oid,
                        'signature', active_relation_signature
                    )
                    FROM {_table('entity_address_geo_assurance_state')}
                    WHERE singleton
                ), '{{}}'::jsonb),
                'geo_assurance_ready',
                {geo_projection.projection_state_available_sql(_SCHEMA)}
            )::text
            """
        ),
        {
            "npi_relation": f"{_SCHEMA}.npi",
            "taxonomy_relation": f"{_SCHEMA}.npi_taxonomy",
            "vocabulary_relation": f"{_SCHEMA}.nucc_taxonomy",
            "address_relation": f"{_SCHEMA}.entity_address_unified",
            "zip_relation": f"{_SCHEMA}.geo_zip_lookup",
        },
    )
    signature_payload = signature_result.scalar_one()
    try:
        parsed_signature = json.loads(str(signature_payload))
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise ValueError(
            "pricing projection provider relations are incomplete"
        ) from exc
    if not isinstance(parsed_signature, dict):
        raise ValueError("pricing projection provider relations are incomplete")
    relation_signatures = (
        parsed_signature.get(name)
        for name in ("npi", "taxonomy", "vocabulary", "address", "zip")
    )
    if parsed_signature.get("geo_assurance_ready") is not True or any(
        not isinstance(signature, list)
        or len(signature) != 2
        or any(value is None for value in signature)
        for signature in relation_signatures
    ):
        raise ValueError("pricing projection provider relations are incomplete")
    return hashlib.sha256(
        _canonical_json(parsed_signature).encode("utf-8")
    ).hexdigest()


async def _lock_provider_generation(session: Any) -> None:
    await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
    await session.execute(
        text(geo_projection.projection_dependency_lock_sql(_SCHEMA))
    )
    await session.execute(
        text(
            "LOCK TABLE "
            + ", ".join(_table(relation) for relation in _PROVIDER_RELATIONS)
            + " IN ACCESS SHARE MODE"
        )
    )


@dataclass(frozen=True)
class _BindingProjection:
    binding: dict[str, Any]
    serving_tables: Any
    code_rows_by_identity: dict[tuple[str, str], list[dict[str, Any]]]


async def _binding_projection(
    session: Any,
    binding: dict[str, Any],
) -> _BindingProjection:
    from api import ptg2_serving as serving

    serving_tables = await snapshot_serving_tables(
        session,
        str(binding["snapshot_id"]),
    )
    serving._require_strict_shared_v3(serving_tables)
    scope_join_sql, filters, params, plan_order = (
        serving._shared_v3_code_scope_sql(
            serving_tables,
            requested_plan=str(binding["plan_id"]),
            plan_market_type=str(
                binding.get("market_type")
                or binding.get("plan_market_type")
                or ""
            ),
        )
    )
    filters.append("code_metadata.snapshot_key = :shared_snapshot_key")
    params["shared_snapshot_key"] = serving._required_shared_snapshot_key(
        serving_tables
    )
    code_result = await session.execute(
        text(
            f"""
            SELECT code_metadata.code_key,
                   logical_scope.plan_id,
                   logical_scope.plan_market_type,
                   code_metadata.reported_code_system,
                   code_metadata.reported_code,
                   code_metadata.negotiation_arrangement,
                   code_metadata.billing_code_type_version,
                   code_metadata.source_name,
                   code_metadata.source_description,
                   code_metadata.rate_count
              FROM {serving._shared_v3_code_table()} code_metadata
              {scope_join_sql}
             WHERE {' AND '.join(filters)}
             ORDER BY {plan_order}, code_metadata.code_key
            """
        ),
        params,
    )
    rows_by_identity: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for raw_row in code_result:
        code_row = serving._canonical_code_metadata_row(raw_row)
        code_system = str(code_row.get("reported_code_system") or "").strip()
        code = serving.canonical_catalog_code(
            code_system,
            code_row.get("reported_code"),
        )
        if code_system and code:
            rows_by_identity[(code_system, code)].append(code_row)
    return _BindingProjection(binding, serving_tables, dict(rows_by_identity))


async def _projection_provider_rows_for_npis(
    session: Any,
    npis: Iterable[int],
) -> dict[int, tuple[dict[str, Any], ...]]:
    """Freeze one assured service-location card per NPI and ZIP cell."""

    from api import ptg2_serving as serving

    normalized_npis = sorted({int(npi) for npi in npis if int(npi) > 0})
    provider_rows: dict[int, list[dict[str, Any]]] = defaultdict(list)
    assurance_sql = serving._ptg2_geo_assured_address_sql("addr")
    taxonomy_sql = serving._provider_taxonomy_summary_lateral_sql(
        "source_npis.npi"
    )
    for start in range(0, len(normalized_npis), _PROVIDER_BATCH_SIZE):
        batch = normalized_npis[start : start + _PROVIDER_BATCH_SIZE]
        provider_result = await session.execute(
            text(
                f"""
                WITH source_npis AS MATERIALIZED (
                    SELECT UNNEST(CAST(:npis AS bigint[])) AS npi
                ), ranked_addresses AS MATERIALIZED (
                    SELECT addr.*,
                           COALESCE(
                               addr.zip5,
                               LEFT(COALESCE(addr.postal_code, ''), 5)
                           ) AS projected_zip5,
                           ROW_NUMBER() OVER (
                               PARTITION BY addr.npi, COALESCE(
                                   addr.zip5,
                                   LEFT(COALESCE(addr.postal_code, ''), 5)
                               )
                               ORDER BY CASE addr.type
                                            WHEN 'practice' THEN 0
                                            WHEN 'primary' THEN 1
                                            WHEN 'secondary' THEN 2
                                            WHEN 'site' THEN 3
                                            ELSE 4
                                        END,
                                        addr.checksum,
                                        addr.location_key
                           ) AS address_rank
                      FROM {_table('entity_address_unified')} addr
                      JOIN source_npis ON source_npis.npi = addr.npi
                     WHERE addr.type IN (
                               'practice', 'primary', 'secondary', 'site'
                           )
                       AND {assurance_sql}
                       AND COALESCE(
                               addr.zip5,
                               LEFT(COALESCE(addr.postal_code, ''), 5)
                           ) ~ '^[0-9]{{5}}$'
                )
                SELECT source_npis.npi,
                       {serving._ptg2_provider_name_sql('n')} AS provider_name,
                       n.entity_type_code,
                       n.provider_credential_text AS credential,
                       COALESCE(tax.taxonomy_codes, ARRAY[]::varchar[])
                           AS taxonomy_codes,
                       COALESCE(tax.classifications, ARRAY[]::varchar[])
                           AS classifications,
                       tax.primary_specialty,
                       addr.city_name AS city,
                       addr.state_name AS state,
                       addr.projected_zip5 AS zip5
                  FROM source_npis
                  LEFT JOIN {_table('npi')} n ON n.npi = source_npis.npi
                  JOIN ranked_addresses addr
                    ON addr.npi = source_npis.npi
                   AND addr.address_rank = 1
                  {taxonomy_sql}
                 ORDER BY source_npis.npi, addr.projected_zip5
                """
            ),
            {"npis": batch},
        )
        for raw_row in provider_result:
            provider = _row_mapping(raw_row)
            npi = int(provider["npi"])
            zip5 = str(provider.get("zip5") or "")[:5]
            if not _ZIP5.fullmatch(zip5):
                continue
            provider["zip5"] = zip5
            provider["state"] = (
                str(provider.get("state") or "").strip().upper() or None
            )
            provider_rows[npi].append(provider)
    return {npi: tuple(rows) for npi, rows in provider_rows.items()}


def _numeric_rates(prices: Iterable[Mapping[str, Any]]) -> tuple[Decimal, ...]:
    rates: list[Decimal] = []
    for price in prices:
        raw_rate = price.get("negotiated_rate")
        try:
            rate = Decimal(str(raw_rate).strip())
        except (InvalidOperation, TypeError, ValueError):
            continue
        if rate.is_finite() and rate >= 0:
            rates.append(rate)
    return tuple(rates)


def _eligible_projection_providers(
    providers: Iterable[dict[str, Any]],
    code_identity: tuple[str, str],
) -> list[dict[str, Any]]:
    """Apply the serving reader's inferred-taxonomy rule before projection."""

    from api import ptg2_serving as serving

    code_system, code = code_identity
    rule = serving._inferred_provider_taxonomy_rule(
        {"code_system": code_system, "code": code}
    )
    provider_rows = list(providers)
    if rule is None:
        return provider_rows
    eligible_taxonomy_codes = frozenset(rule.taxonomy_codes)
    return [
        provider
        for provider in provider_rows
        if provider.get("entity_type_code") == 1
        and eligible_taxonomy_codes.intersection(
            str(taxonomy_code or "").strip().upper()
            for taxonomy_code in provider.get("taxonomy_codes") or ()
        )
    ]


def _rate_fragment(rate: Decimal) -> orjson.Fragment:
    expanded = format(rate, "f")
    if "." in expanded:
        expanded = expanded.rstrip("0").rstrip(".")
    return orjson.Fragment((expanded or "0").encode("ascii"))


@dataclass
class _CardStats:
    provider: dict[str, Any]
    minimum: Decimal
    maximum: Decimal
    rate_count: int

    def add(self, rates: tuple[Decimal, ...]) -> None:
        self.minimum = min(self.minimum, min(rates))
        self.maximum = max(self.maximum, max(rates))
        self.rate_count += len(rates)


def _card_fragment(stats: _CardStats) -> bytes:
    provider = stats.provider
    taxonomy_codes = list(provider.get("taxonomy_codes") or [])
    classifications = list(provider.get("classifications") or [])
    return orjson.dumps(
        {
            "npi": int(provider["npi"]),
            "provider_name": provider.get("provider_name") or "TiC provider",
            "entity_type_code": provider.get("entity_type_code"),
            "credential": provider.get("credential"),
            "taxonomy_code": taxonomy_codes[0] if taxonomy_codes else None,
            "primary_specialty": provider.get("primary_specialty"),
            "classification": classifications[0] if classifications else None,
            "city": provider.get("city"),
            "state": provider.get("state"),
            "zip5": provider["zip5"],
            "minimum_negotiated_rate": _rate_fragment(stats.minimum),
            "maximum_negotiated_rate": _rate_fragment(stats.maximum),
            "rate_count": stats.rate_count,
        }
    )


def _aggregate_fragment(
    geo_cell: str,
    provider_count: int,
    rates: list[Decimal],
) -> tuple[bytes, Decimal, Decimal, Decimal]:
    ordered_rates = sorted(rates)
    minimum = ordered_rates[0]
    midpoint = len(ordered_rates) // 2
    median = ordered_rates[midpoint]
    if len(ordered_rates) % 2 == 0:
        median = (ordered_rates[midpoint - 1] + median) / 2
    maximum = ordered_rates[-1]
    return (
        orjson.dumps(
            {
                "geo_cell": geo_cell,
                "provider_count": provider_count,
                "rate_count": len(ordered_rates),
                "minimum_negotiated_rate": _rate_fragment(minimum),
                "median_negotiated_rate": _rate_fragment(median),
                "maximum_negotiated_rate": _rate_fragment(maximum),
            }
        ),
        minimum,
        median,
        maximum,
    )


def _digest_row(
    digest: Any,
    kind: str,
    key: tuple[Any, ...],
    fragment: bytes,
) -> None:
    key_bytes = _canonical_json([kind, *key]).encode("utf-8")
    digest.update(len(key_bytes).to_bytes(4, "big"))
    digest.update(key_bytes)
    digest.update(len(fragment).to_bytes(8, "big"))
    digest.update(fragment)


async def _insert_batches(
    session: Any,
    statement: str,
    rows: list[dict[str, Any]],
) -> None:
    for start in range(0, len(rows), _INSERT_BATCH_SIZE):
        await session.execute(
            text(statement),
            rows[start : start + _INSERT_BATCH_SIZE],
        )


async def _project_code(
    session: Any,
    projection_id: str,
    code_identity: tuple[str, str],
    bindings: list[_BindingProjection],
    content_digest: Any,
) -> tuple[int, int, int]:
    from api import ptg2_serving as serving

    card_stats: dict[tuple[str, int], _CardStats] = {}
    aggregate_rates: dict[str, list[Decimal]] = defaultdict(list)
    aggregate_npis: dict[str, set[int]] = defaultdict(set)

    for binding_projection in bindings:
        code_rows = binding_projection.code_rows_by_identity.get(code_identity)
        if not code_rows:
            continue
        tables = binding_projection.serving_tables
        serving_rows = await serving._merge_manifest_code_variant_rows(
            session,
            tables,
            code_rows=code_rows,
            provider_set_keys=None,
            source_trace_set_hash=None,
            network_names=tables.network_names or [],
            limit=None,
            offset=0,
        )
        if serving_rows is None:
            raise ValueError("pricing projection could not read a sealed rate layout")
        price_key_by_set_id = {
            serving._ptg2_manifest_id(row.get("price_set_global_id_128")): int(
                row["price_key"]
            )
            for row in serving_rows
            if row.get("price_key") is not None
            and serving._ptg2_manifest_id(row.get("price_set_global_id_128"))
        }
        prices_by_set = await serving._prices_for_price_sets(
            session,
            tables,
            list(price_key_by_set_id),
            price_key_by_set_id=price_key_by_set_id,
        )
        provider_set_ids = sorted(
            {
                serving._ptg2_manifest_id(
                    row.get("provider_set_global_id_128")
                )
                for row in serving_rows
                if serving._ptg2_manifest_id(
                    row.get("provider_set_global_id_128")
                )
            }
        )
        npis_by_set = await serving._provider_npis_for_sets(
            session,
            tables,
            provider_set_ids,
            limit_per_set=None,
        )
        provider_by_npi = await _projection_provider_rows_for_npis(
            session,
            (npi for npis in npis_by_set.values() for npi in npis),
        )
        for serving_row in serving_rows:
            provider_set_id = serving._ptg2_manifest_id(
                serving_row.get("provider_set_global_id_128")
            )
            price_set_id = serving._ptg2_manifest_id(
                serving_row.get("price_set_global_id_128")
            )
            rates = _numeric_rates(prices_by_set.get(price_set_id, ()))
            if not provider_set_id or not rates:
                continue
            providers = _eligible_projection_providers(
                (
                    provider
                    for npi in npis_by_set.get(provider_set_id, ())
                    for provider in provider_by_npi.get(npi, ())
                ),
                code_identity,
            )
            providers_by_cell: dict[str, list[dict[str, Any]]] = defaultdict(list)
            for provider in providers:
                providers_by_cell[provider["zip5"]].append(provider)
                card_key = (provider["zip5"], int(provider["npi"]))
                existing = card_stats.get(card_key)
                if existing is None:
                    card_stats[card_key] = _CardStats(
                        provider,
                        min(rates),
                        max(rates),
                        len(rates),
                    )
                else:
                    existing.add(rates)
            for geo_cell, cell_providers in providers_by_cell.items():
                aggregate_rates[geo_cell].extend(rates)
                aggregate_npis[geo_cell].update(
                    int(provider["npi"]) for provider in cell_providers
                )

    code_system, code = code_identity
    card_rows: list[dict[str, Any]] = []
    fragment_bytes = 0
    for (geo_cell, npi), stats in sorted(card_stats.items()):
        fragment = _card_fragment(stats)
        fragment_bytes += len(fragment)
        _digest_row(
            content_digest,
            "card",
            (code_system, code, geo_cell, npi),
            fragment,
        )
        card_rows.append(
            {
                "projection_id": projection_id,
                "code_system": code_system,
                "code": code,
                "geo_cell": geo_cell,
                "npi": npi,
                "minimum_rate": stats.minimum,
                "maximum_rate": stats.maximum,
                "rate_count": stats.rate_count,
                "fragment": fragment,
            }
        )
    await _insert_batches(
        session,
        f"""
        INSERT INTO {_table('plan_pricing_card')} (
            projection_id, code_system, code, geo_cell, npi,
            minimum_negotiated_rate, maximum_negotiated_rate,
            rate_count, fragment
        ) VALUES (
            :projection_id, :code_system, :code, :geo_cell, :npi,
            :minimum_rate, :maximum_rate, :rate_count, :fragment
        )
        """,
        card_rows,
    )

    aggregate_rows: list[dict[str, Any]] = []
    for geo_cell, rates in sorted(aggregate_rates.items()):
        fragment, minimum, median, maximum = _aggregate_fragment(
            geo_cell,
            len(aggregate_npis[geo_cell]),
            rates,
        )
        fragment_bytes += len(fragment)
        _digest_row(
            content_digest,
            "aggregate",
            (code_system, code, geo_cell),
            fragment,
        )
        aggregate_rows.append(
            {
                "projection_id": projection_id,
                "code_system": code_system,
                "code": code,
                "geo_cell": geo_cell,
                "provider_count": len(aggregate_npis[geo_cell]),
                "rate_count": len(rates),
                "minimum_rate": minimum,
                "median_rate": median,
                "maximum_rate": maximum,
                "fragment": fragment,
            }
        )
    await _insert_batches(
        session,
        f"""
        INSERT INTO {_table('plan_pricing_cell_aggregate')} (
            projection_id, code_system, code, geo_cell, provider_count,
            rate_count, minimum_negotiated_rate, median_negotiated_rate,
            maximum_negotiated_rate, fragment
        ) VALUES (
            :projection_id, :code_system, :code, :geo_cell, :provider_count,
            :rate_count, :minimum_rate, :median_rate, :maximum_rate, :fragment
        )
        """,
        aggregate_rows,
    )
    return len(card_rows), len(aggregate_rows), fragment_bytes


def _receipt(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "contract": PROJECTION_CONTRACT,
        "projection_id": str(row["projection_id"]),
        "binding_manifest_digest": str(row["binding_manifest_digest"]),
        "provider_signature": str(row["provider_signature"]),
        "content_digest": str(row["content_digest"]),
        "card_row_count": int(row["card_row_count"]),
        "aggregate_row_count": int(row["aggregate_row_count"]),
        "fragment_byte_count": int(row["fragment_byte_count"]),
        "build_seconds": float(row["build_seconds"]),
        "state": "ready",
    }


async def _build_plan_pricing_projection(
    session: Any,
    *,
    binding_manifest_digest: str,
    bindings: Any,
) -> dict[str, Any]:
    if not _HEX_DIGEST.fullmatch(binding_manifest_digest):
        raise ValueError("pricing projection binding digest is invalid")
    binding_manifest = _normalized_bindings(bindings)
    provider_signature = await _provider_signature(session)
    projection_id = _projection_id(
        binding_manifest_digest,
        provider_signature,
    )
    await session.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(:key, 0))"),
        {"key": projection_id},
    )
    existing_result = await session.execute(
        text(
            f"""
            SELECT *
              FROM {_table('plan_pricing_projection_candidate')}
             WHERE projection_id = :projection_id
            """
        ),
        {"projection_id": projection_id},
    )
    existing = existing_result.mappings().one_or_none()
    if existing is not None and existing.get("state") == "ready":
        if (
            existing.get("binding_manifest") != binding_manifest
            or existing.get("binding_manifest_digest")
            != binding_manifest_digest
            or existing.get("provider_signature") != provider_signature
        ):
            raise ValueError("pricing projection identity collision")
        return _receipt(existing)
    if existing is not None:
        await session.execute(
            text(
                f"""
                DELETE FROM {_table('plan_pricing_projection_candidate')}
                 WHERE projection_id = :projection_id
                """
            ),
            {"projection_id": projection_id},
        )
    await session.execute(
        text(
            f"""
            INSERT INTO {_table('plan_pricing_projection_candidate')} (
                projection_id, contract_version, binding_manifest_digest,
                binding_manifest, provider_signature, state
            ) VALUES (
                :projection_id, :contract_version, :binding_manifest_digest,
                CAST(:binding_manifest AS jsonb), :provider_signature, 'building'
            )
            """
        ),
        {
            "projection_id": projection_id,
            "contract_version": PROJECTION_CONTRACT,
            "binding_manifest_digest": binding_manifest_digest,
            "binding_manifest": _canonical_json(binding_manifest),
            "provider_signature": provider_signature,
        },
    )
    started_at = time.perf_counter()
    in_network_bindings = [
        binding
        for binding in binding_manifest
        if str(binding.get("role")) == "in_network"
    ]
    if not in_network_bindings:
        raise ValueError("pricing projection requires an in-network binding")
    binding_projections = [
        await _binding_projection(session, binding)
        for binding in in_network_bindings
    ]
    code_identities = sorted(
        {
            code_identity
            for binding_projection in binding_projections
            for code_identity in binding_projection.code_rows_by_identity
        }
    )
    content_digest = hashlib.sha256()
    card_row_count = 0
    aggregate_row_count = 0
    fragment_byte_count = 0
    for code_identity in code_identities:
        card_count, aggregate_count, fragment_bytes = await _project_code(
            session,
            projection_id,
            code_identity,
            binding_projections,
            content_digest,
        )
        card_row_count += card_count
        aggregate_row_count += aggregate_count
        fragment_byte_count += fragment_bytes
        await asyncio.sleep(0)
    build_seconds = time.perf_counter() - started_at
    ready_result = await session.execute(
        text(
            f"""
            UPDATE {_table('plan_pricing_projection_candidate')}
               SET state = 'ready',
                   content_digest = :content_digest,
                   card_row_count = :card_row_count,
                   aggregate_row_count = :aggregate_row_count,
                   fragment_byte_count = :fragment_byte_count,
                   build_seconds = :build_seconds,
                   completed_at = transaction_timestamp()
             WHERE projection_id = :projection_id
         RETURNING *
            """
        ),
        {
            "projection_id": projection_id,
            "content_digest": content_digest.hexdigest(),
            "card_row_count": card_row_count,
            "aggregate_row_count": aggregate_row_count,
            "fragment_byte_count": fragment_byte_count,
            "build_seconds": build_seconds,
        },
    )
    return _receipt(ready_result.mappings().one())


async def build_plan_pricing_projection(
    *,
    binding_manifest_digest: str,
    bindings: Any,
) -> dict[str, Any]:
    """Build or reuse one complete invisible candidate atomically."""

    async with db.transaction() as session:
        await _lock_provider_generation(session)
        return await _build_plan_pricing_projection(
            session,
            binding_manifest_digest=binding_manifest_digest,
            bindings=bindings,
        )


def projection_result_type(args: Mapping[str, Any]) -> str | None:
    view = str(args.get("view") or "full").strip().lower()
    if view != "card":
        return None
    include_providers = _is_request_flag_enabled(
        args.get("include_providers"),
        default=True,
    )
    if not include_providers:
        return "rate_aggregates"
    return "provider_cards"


def _unsupported_projection_fields(args: Mapping[str, Any]) -> tuple[str, ...]:
    unsupported = [
        field
        for field in (
            "q",
            "npi",
            "specialty",
            "provider_type",
            "classification",
            "taxonomy_codes",
            "taxonomy_code",
            "taxonomy_classification",
            "taxonomy_specialization",
            "taxonomy_section",
            "provider_sex_code",
            "pos",
            "place_of_service",
            "service_code",
            "modifier",
            "modifiers",
            "billing_code_modifier",
            "rate",
            "negotiated_rate",
            "rate_tolerance",
            "negotiated_rate_tolerance",
        )
        if args.get(field) not in (None, "", "null", False)
    ]
    unsupported.extend(
        field
        for field in (
            "include_code_details",
            "include_sources",
            "include_evidence",
            "include_unverified_addresses",
            "include_details",
            "include_debug",
        )
        if _is_request_flag_enabled(args.get(field), default=False)
    )
    order_by = str(args.get("order_by") or "total_allowed_amount").strip().lower()
    if order_by not in _COST_ORDER_FIELDS:
        unsupported.append("order_by")
    order = str(args.get("order") or "asc").strip().lower()
    if order not in {"asc", "desc"}:
        unsupported.append("order")
    return tuple(unsupported)


async def _geo_cells(
    session: Any,
    args: Mapping[str, Any],
    *,
    result_type: str,
) -> list[str]:
    zip5 = str(args.get("zip5") or args.get("zip") or "").strip()
    city = str(args.get("city") or "").strip().lower()
    state = str(args.get("state") or "").strip().upper()
    latitude = args.get("lat")
    longitude = args.get("long")
    raw_radius = (
        args.get("zip_radius_miles")
        if zip5
        else args.get("radius_miles")
    )
    try:
        radius = max(float(raw_radius or 0), 0.0)
    except (TypeError, ValueError) as exc:
        raise PlanPricingProjectionUnsupported(
            "card projection radius is invalid"
        ) from exc
    if city or state:
        raise PlanPricingProjectionUnsupported(
            "card projection supports ZIP5 or coordinates, not city/state"
        )
    if zip5 and not _ZIP5.fullmatch(zip5):
        raise PlanPricingProjectionUnsupported(
            "card projection requires a valid ZIP5"
        )
    if zip5 and radius <= 0:
        return [zip5]
    if zip5:
        center_sql = f"""
            SELECT latitude, longitude
              FROM {_table('geo_zip_lookup')}
             WHERE zip_code = :zip5
        """
        center_params: dict[str, Any] = {"zip5": zip5}
    else:
        try:
            requested_latitude = float(latitude)
            requested_longitude = float(longitude)
        except (TypeError, ValueError) as exc:
            raise PlanPricingProjectionUnsupported(
                "card projection requires ZIP5 or coordinates"
            ) from exc
        center_sql = """
            SELECT CAST(:latitude AS double precision) AS latitude,
                   CAST(:longitude AS double precision) AS longitude
        """
        center_params = {
            "latitude": requested_latitude,
            "longitude": requested_longitude,
        }
    cell_result = await session.execute(
        text(
            f"""
            WITH center AS MATERIALIZED (
                {center_sql}
            )
            SELECT cells.zip_code
              FROM center
              CROSS JOIN LATERAL (
                  SELECT zip_code
                    FROM {_table('geo_zip_lookup')} candidate
                   WHERE candidate.latitude BETWEEN
                             center.latitude - :radius / 69.0
                         AND center.latitude + :radius / 69.0
                     AND candidate.longitude BETWEEN
                             center.longitude - :radius / (
                                 69.0 * greatest(
                                     abs(cos(radians(center.latitude))), 0.1
                                 )
                             )
                         AND center.longitude + :radius / (
                                 69.0 * greatest(
                                     abs(cos(radians(center.latitude))), 0.1
                                 )
                             )
                     AND 69.0 * sqrt(
                         power(candidate.latitude - center.latitude, 2)
                         + power(
                             (candidate.longitude - center.longitude)
                             * cos(radians(
                                 (candidate.latitude + center.latitude) / 2.0
                             )),
                             2
                         )
                     ) <= :radius
                   ORDER BY 69.0 * sqrt(
                         power(candidate.latitude - center.latitude, 2)
                         + power(
                             (candidate.longitude - center.longitude)
                             * cos(radians(
                                 (candidate.latitude + center.latitude) / 2.0
                             )),
                             2
                         )
                   ), candidate.zip_code
                   LIMIT :limit
              ) cells
            """
        ),
        {
            **center_params,
            "radius": radius,
            "limit": _MAX_GEO_CELLS + 1,
        },
    )
    cells = [str(cell) for cell in cell_result.scalars().all()]
    if len(cells) > _MAX_GEO_CELLS:
        raise PlanPricingProjectionUnsupported(
            f"card projection radius exceeds {_MAX_GEO_CELLS} ZIP cells"
        )
    if not cells and result_type == "provider_cards":
        return []
    return cells


def _empty_pagination(pagination: Any) -> dict[str, Any]:
    return {
        "total": 0,
        "total_is_exact": True,
        "total_lower_bound": 0,
        "limit": int(pagination.limit),
        "offset": int(pagination.offset),
        "page": int(pagination.page),
        "has_more": False,
    }


def _projection_query(
    args: Mapping[str, Any],
    *,
    result_type: str,
) -> dict[str, Any]:
    include_providers = result_type == "provider_cards"
    return {
        "code": args.get("code") or None,
        "code_system": args.get("code_system") or None,
        "zip5": args.get("zip5") or None,
        "zip_radius_miles": args.get("zip_radius_miles"),
        "lat": args.get("lat"),
        "long": args.get("long"),
        "radius_miles": args.get("radius_miles"),
        "state": args.get("state") or None,
        "city": args.get("city") or None,
        "view": str(args.get("view") or "full").strip().lower(),
        "include_providers": include_providers,
        "projection_contract": PROJECTION_CONTRACT,
        "source": "plan_pricing_projection",
    }


async def search_plan_pricing_projection(
    session: Any,
    selection: PlanReleaseServingSelection,
    args: Mapping[str, Any],
    pagination: Any,
) -> dict[str, Any] | None:
    """Read one card/aggregate page from its immutable ZIP-cell projection."""

    result_type = projection_result_type(args)
    if result_type is None:
        return None
    unsupported_fields = _unsupported_projection_fields(args)
    if unsupported_fields:
        if result_type == "rate_aggregates":
            return None
        raise PlanPricingProjectionUnsupported(
            "view=card does not support filters: "
            + ", ".join(unsupported_fields)
        )
    code_system = normalize_code_system(args.get("code_system"))
    code = canonical_catalog_code(code_system, args.get("code"))
    if not code_system or not code:
        if result_type == "rate_aggregates":
            return None
        raise PlanPricingProjectionUnsupported(
            "view=card requires code_system and code"
        )
    projection_id = selection.pricing_projection_id
    if not projection_id:
        raise PlanPricingProjectionUnavailable(
            "the selected release has no ready card projection"
        )
    try:
        geo_cells = await _geo_cells(
            session,
            args,
            result_type=result_type,
        )
    except PlanPricingProjectionUnsupported:
        if result_type == "rate_aggregates":
            return None
        raise
    if not geo_cells:
        response = {
            "result_type": result_type,
            "result_state": "no_match_in_radius",
            "pricing_scope": "plan_scoped_ptg",
            "resolved": True,
            "items": [],
            "pagination": _empty_pagination(pagination),
            "query": _projection_query(args, result_type=result_type),
        }
        return annotate_plan_release_response(response, selection) or response

    table_name = (
        "plan_pricing_card"
        if result_type == "provider_cards"
        else "plan_pricing_cell_aggregate"
    )
    order_direction = (
        "DESC"
        if str(args.get("order") or "asc").strip().lower() == "desc"
        else "ASC"
    )
    order_sql = (
        f"projected.minimum_negotiated_rate {order_direction}, projected.npi"
        if result_type == "provider_cards"
        else f"projected.minimum_negotiated_rate {order_direction}, "
        "projected.geo_cell"
    )
    matched_rank_sql = (
        ", ROW_NUMBER() OVER (PARTITION BY item.npi "
        "ORDER BY cells.ordinal, item.geo_cell) AS address_rank"
        if result_type == "provider_cards"
        else ""
    )
    projected_source_sql = (
        "(SELECT * FROM matched WHERE address_rank = 1) matched"
        if result_type == "provider_cards"
        else "matched"
    )
    projected_result = await session.execute(
        text(
            f"""
            WITH cells AS MATERIALIZED (
                SELECT geo_cell, ordinal
                  FROM unnest(CAST(:geo_cells AS varchar[]))
                       WITH ORDINALITY AS selected(geo_cell, ordinal)
            ), matched AS MATERIALIZED (
                SELECT item.*, cells.ordinal{matched_rank_sql}
                  FROM cells
                  JOIN {_table(table_name)} item
                    ON item.geo_cell = cells.geo_cell
                 WHERE item.projection_id = :projection_id
                   AND item.code_system = :code_system
                   AND item.code = :code
            ), projected AS MATERIALIZED (
                SELECT matched.*
                  FROM {projected_source_sql}
            ), page AS MATERIALIZED (
                SELECT projected.fragment,
                       ROW_NUMBER() OVER (ORDER BY {order_sql}) AS page_rank
                  FROM projected
                 ORDER BY {order_sql}
                 LIMIT :limit OFFSET :offset
            )
            SELECT page.fragment, totals.total
              FROM (SELECT count(*) AS total FROM projected) totals
              LEFT JOIN page ON TRUE
             ORDER BY page.page_rank NULLS LAST
            """
        ),
        {
            "geo_cells": geo_cells,
            "projection_id": projection_id,
            "code_system": code_system,
            "code": code,
            "limit": int(pagination.limit),
            "offset": int(pagination.offset),
        },
    )
    projected_rows = projected_result.all()
    total = int(projected_rows[0][1]) if projected_rows else 0
    items = [
        orjson.Fragment(bytes(row[0]))
        for row in projected_rows
        if row[0] is not None
    ]
    response = {
        "result_type": result_type,
        "result_state": "matched" if total else "no_matching_rates",
        "pricing_scope": "plan_scoped_ptg",
        "resolved": True,
        "items": items,
        "pagination": {
            "total": total,
            "total_is_exact": True,
            "total_lower_bound": total,
            "limit": int(pagination.limit),
            "offset": int(pagination.offset),
            "page": int(pagination.page),
            "has_more": int(pagination.offset) + len(items) < total,
        },
        "query": _projection_query(args, result_type=result_type),
    }
    return annotate_plan_release_response(response, selection) or response


__all__ = [
    "PROJECTION_CONTRACT",
    "PlanPricingProjectionUnavailable",
    "PlanPricingProjectionUnsupported",
    "build_plan_pricing_projection",
    "projection_result_type",
    "search_plan_pricing_projection",
]
