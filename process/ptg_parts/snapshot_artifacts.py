# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import json
import logging
import os
import sys
from typing import Any

from db.connection import db
from db.models import PTG2ArtifactManifest
from process.ptg_parts.artifacts import PTG2ArtifactStore, sha256_file
from process.ptg_parts.canonical import money_number, semantic_hash
from process.ptg_parts.copy_load import _json_default
from process.ptg_parts.domain import (
    PTG2_ARTIFACT_SNAPSHOT_INDEX,
    PTG2_CONFIDENCE_NPPES_PRACTICE_LOCATION,
    PTG2_CONFIDENCE_TIC_RATE_NPI_TIN,
    ptg2_confidence_statement,
)
from process.ptg_parts.progress import _utcnow
from process.ptg_parts.row_helpers import _as_int_list


logger = logging.getLogger(__name__)


async def _push_ptg2_objects_from_facade(rows: list[dict[str, Any]], cls, *, rewrite: bool = True) -> None:
    ptg_module = sys.modules.get("process.ptg")
    if ptg_module is None:
        raise RuntimeError("process.ptg facade is not loaded")
    await ptg_module._push_ptg2_objects(rows, cls, rewrite=rewrite)


def _row_mapping(row: Any) -> dict[str, Any]:
    mapping = getattr(row, "_mapping", None)
    if mapping is not None:
        return dict(mapping)
    if isinstance(row, dict):
        return dict(row)
    return dict(row)


async def build_ptg2_snapshot_index_artifact(
    classes: dict[str, type],
    snapshot_id: str,
    import_run_id: str,
) -> dict[str, Any] | None:
    item_cls = classes["PTGInNetworkItem"]
    rate_cls = classes["PTGNegotiatedRate"]
    price_cls = classes["PTGNegotiatedPrice"]
    provider_cls = classes["PTGProviderGroup"]
    file_cls = classes["PTGFile"]
    schema = item_cls.__table__.schema or os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    limit_clause = ""
    raw_limit = os.getenv("HLTHPRT_PTG2_SNAPSHOT_ARTIFACT_ROW_LIMIT", "").strip()
    if raw_limit:
        try:
            row_limit = max(int(raw_limit), 1)
            limit_clause = f" LIMIT {row_limit}"
        except ValueError:
            logger.warning("Ignoring invalid HLTHPRT_PTG2_SNAPSHOT_ARTIFACT_ROW_LIMIT=%s", raw_limit)
    sql = f"""
        SELECT
            i.plan_id,
            i.plan_name,
            i.plan_id_type,
            i.plan_market_type,
            i.issuer_name,
            i.plan_sponsor_name,
            i.billing_code,
            i.billing_code_type,
            i.name AS procedure_name,
            i.description AS procedure_description,
            pg.npi AS provider_npi,
            pg.tin_type,
            pg.tin_value,
            pg.tin_business_name,
            p.negotiated_type,
            p.negotiated_rate::text AS negotiated_rate,
            p.expiration_date::text AS expiration_date,
            p.service_code,
            p.billing_class,
            p.setting,
            p.billing_code_modifier,
            p.additional_information,
            f.url AS source_url
        FROM {schema}.{item_cls.__tablename__} i
        JOIN {schema}.{rate_cls.__tablename__} r ON r.item_hash = i.item_hash
        JOIN {schema}.{price_cls.__tablename__} p ON p.rate_hash = r.rate_hash
        JOIN {schema}.{provider_cls.__tablename__} pg ON pg.provider_group_hash = r.provider_group_hash
        LEFT JOIN {schema}.{file_cls.__tablename__} f ON f.file_id = i.file_id
        WHERE i.plan_id IS NOT NULL
          AND i.billing_code IS NOT NULL
        ORDER BY i.plan_id, i.billing_code
        {limit_clause}
    """
    rows = await db.all(sql)
    if not rows:
        return None

    plans: dict[str, Any] = {}
    procedures: dict[str, Any] = {}
    providers: dict[str, Any] = {}
    provider_ordinals: dict[int, int] = {}
    rates: dict[str, dict[str, list[dict[str, Any]]]] = {}

    def _provider_ordinal(npi: int, row: dict[str, Any]) -> int:
        if npi in provider_ordinals:
            return provider_ordinals[npi]
        ordinal = len(provider_ordinals) + 1
        provider_ordinals[npi] = ordinal
        providers[str(ordinal)] = {
            "provider_ordinal": ordinal,
            "npi": npi,
            "provider_name": row.get("tin_business_name"),
            "tin_type": row.get("tin_type"),
            "tin_value": row.get("tin_value"),
        }
        return ordinal

    for raw_row in rows:
        row = _row_mapping(raw_row)
        plan_id = str(row.get("plan_id") or "").strip()
        code = str(row.get("billing_code") or "").strip().upper()
        if not plan_id or not code:
            continue
        plans.setdefault(
            plan_id,
            {
                "plan_id": plan_id,
                "plan_name": row.get("plan_name"),
                "plan_id_type": row.get("plan_id_type"),
                "plan_market_type": row.get("plan_market_type"),
                "issuer_name": row.get("issuer_name"),
                "plan_sponsor_name": row.get("plan_sponsor_name"),
            },
        )
        procedures.setdefault(
            code,
            {
                "code": code,
                "billing_code": code,
                "billing_code_type": row.get("billing_code_type"),
                "name": row.get("procedure_name"),
                "description": row.get("procedure_description"),
            },
        )
        npi_values = _as_int_list(row.get("provider_npi"))
        for npi in npi_values:
            ordinal = _provider_ordinal(npi, row)
            rate_payload = {
                "provider_ordinal": ordinal,
                "npi": npi,
                "billing_code_type": row.get("billing_code_type"),
                "prices": [
                    {
                        "negotiated_type": row.get("negotiated_type"),
                        "negotiated_rate": money_number(row.get("negotiated_rate")),
                        "expiration_date": row.get("expiration_date"),
                        "service_code": row.get("service_code") or [],
                        "billing_class": row.get("billing_class"),
                        "setting": row.get("setting"),
                        "billing_code_modifier": row.get("billing_code_modifier") or [],
                        "additional_information": row.get("additional_information"),
                    }
                ],
                "source_trace": [
                    {
                        "url": row.get("source_url"),
                        "statement": "Published negotiated rate from Transparency in Coverage source file.",
                    }
                ],
                "confidence": {
                    "network": PTG2_CONFIDENCE_TIC_RATE_NPI_TIN,
                    "location": PTG2_CONFIDENCE_NPPES_PRACTICE_LOCATION,
                    "acceptance_statement": ptg2_confidence_statement(PTG2_CONFIDENCE_TIC_RATE_NPI_TIN),
                },
            }
            rates.setdefault(plan_id, {}).setdefault(code, []).append(rate_payload)

    payload = {
        "version": 1,
        "snapshot_id": snapshot_id,
        "generated_at": _utcnow().isoformat(),
        "plans": plans,
        "procedures": procedures,
        "providers": providers,
        "rates": rates,
    }
    store = PTG2ArtifactStore()
    target = store.root / PTG2_ARTIFACT_SNAPSHOT_INDEX / f"{snapshot_id}.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp_target = target.with_suffix(".json.tmp")
    tmp_target.write_text(json.dumps(payload, sort_keys=True, default=_json_default), encoding="utf-8")
    artifact_sha, byte_count = sha256_file(tmp_target)
    os.replace(tmp_target, target)
    artifact_uri = store.storage_uri(target)
    await _push_ptg2_objects_from_facade(
        [
            {
                "artifact_id": semantic_hash(
                    {"kind": PTG2_ARTIFACT_SNAPSHOT_INDEX, "snapshot_id": snapshot_id, "storage_uri": artifact_uri},
                    domain="artifact_manifest",
                )[:32],
                "snapshot_id": snapshot_id,
                "import_run_id": import_run_id,
                "artifact_kind": PTG2_ARTIFACT_SNAPSHOT_INDEX,
                "storage_uri": artifact_uri,
                "sha256": artifact_sha,
                "byte_count": byte_count,
                "payload": {
                    "plan_count": len(plans),
                    "procedure_count": len(procedures),
                    "provider_count": len(providers),
                },
                "created_at": _utcnow(),
            }
        ],
        PTG2ArtifactManifest,
        rewrite=True,
    )
    return {
        "storage_uri": artifact_uri,
        "sha256": artifact_sha,
        "byte_count": byte_count,
        "plan_count": len(plans),
        "procedure_count": len(procedures),
        "provider_count": len(providers),
    }


async def build_ptg2_compact_snapshot_index_artifact(
    snapshot_id: str,
    import_run_id: str,
) -> dict[str, Any] | None:
    schema = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    limit_clause = ""
    raw_limit = (
        os.getenv("HLTHPRT_PTG2_COMPACT_SNAPSHOT_ARTIFACT_ROW_LIMIT", "").strip()
        or os.getenv("HLTHPRT_PTG2_SNAPSHOT_ARTIFACT_ROW_LIMIT", "").strip()
    )
    if raw_limit:
        try:
            row_limit = max(int(raw_limit), 1)
            limit_clause = f" LIMIT {row_limit}"
        except ValueError:
            logger.warning("Ignoring invalid PTG2 compact artifact row limit=%s", raw_limit)
    sql = f"""
        WITH selected_rates AS (
            SELECT
                p.plan_id,
                p.plan_name,
                p.plan_id_type,
                p.plan_market_type,
                p.issuer_name,
                p.plan_sponsor_name,
                proc.billing_code,
                proc.billing_code_type,
                proc.name AS procedure_name,
                proc.description AS procedure_description,
                rp.rate_pack_hash,
                rp.provider_set_hash,
                rp.price_set_hash,
                rp.source_trace_set_hash,
                rp.canonical_payload::jsonb AS rate_payload,
                prices.prices,
                traces.source_trace,
                providers.provider_set_count,
                providers.provider_count
            FROM {schema}.ptg2_plan_month pm
            JOIN {schema}.ptg2_plan p ON p.plan_hash = pm.plan_hash
            JOIN {schema}.ptg2_plan_rate_set prs ON prs.plan_month_id = pm.plan_month_id
            JOIN {schema}.ptg2_rate_set rs ON rs.rate_set_hash = prs.rate_set_hash
            JOIN LATERAL unnest(rs.chunk_hashes) AS chunk_ref(fact_chunk_hash) ON true
            JOIN {schema}.ptg2_fact_chunk fc ON fc.fact_chunk_hash = chunk_ref.fact_chunk_hash
            JOIN {schema}.ptg2_procedure proc ON proc.procedure_hash = fc.procedure_hash
            JOIN LATERAL unnest(fc.rate_pack_hashes) AS pack_ref(rate_pack_hash) ON true
            JOIN {schema}.ptg2_rate_pack rp ON rp.rate_pack_hash = pack_ref.rate_pack_hash
            LEFT JOIN {schema}.ptg2_price_set price_set ON price_set.price_set_hash = rp.price_set_hash
            LEFT JOIN LATERAL (
                SELECT COALESCE(
                    jsonb_agg(
                        jsonb_build_object(
                            'negotiated_type', pa.negotiated_type,
                            'negotiated_rate',
                                CASE
                                    WHEN pa.negotiated_rate ~ '^-?[0-9]+(\\.[0-9]+)?$'
                                        THEN (pa.negotiated_rate)::numeric
                                    ELSE NULL
                                END,
                            'expiration_date', pa.expiration_date::text,
                            'service_code', COALESCE(pa.service_code, ARRAY[]::text[]),
                            'billing_class', pa.billing_class,
                            'setting', pa.setting,
                            'billing_code_modifier', COALESCE(pa.billing_code_modifier, ARRAY[]::text[]),
                            'additional_information', pa.additional_information
                        )
                        ORDER BY pa.price_atom_hash
                    ),
                    '[]'::jsonb
                ) AS prices
                FROM jsonb_array_elements_text(COALESCE(price_set.canonical_payload::jsonb->'price_atom_hashes', '[]'::jsonb)) AS pah(price_atom_hash)
                JOIN {schema}.ptg2_price_atom pa ON pa.price_atom_hash = pah.price_atom_hash
            ) prices ON true
            LEFT JOIN LATERAL (
                SELECT COALESCE(
                    jsonb_agg(
                        jsonb_build_object(
                            'url', st.original_url,
                            'canonical_url', st.canonical_url,
                            'statement', 'Published negotiated rate from Transparency in Coverage source file.'
                        )
                        ORDER BY st.source_trace_hash
                    ),
                    '[]'::jsonb
                ) AS source_trace
                FROM {schema}.ptg2_source_trace_set sts
                JOIN LATERAL jsonb_array_elements_text(COALESCE(sts.canonical_payload::jsonb->'source_trace_hashes', '[]'::jsonb)) AS sth(source_trace_hash) ON true
                JOIN {schema}.ptg2_source_trace st ON st.source_trace_hash = sth.source_trace_hash
                WHERE sts.source_trace_set_hash = rp.source_trace_set_hash
            ) traces ON true
            LEFT JOIN LATERAL (
                SELECT
                    count(*)::int AS provider_set_count,
                    COALESCE(sum(ps.provider_count), 0)::int AS provider_count
                FROM jsonb_array_elements_text(COALESCE(rp.canonical_payload::jsonb->'provider_set_hashes', '[]'::jsonb)) AS psh(provider_set_hash)
                LEFT JOIN {schema}.ptg2_provider_set ps ON ps.provider_set_hash = psh.provider_set_hash
            ) providers ON true
            WHERE pm.snapshot_id = :snapshot_id
              AND p.plan_id IS NOT NULL
              AND proc.billing_code IS NOT NULL
            ORDER BY p.plan_id, proc.billing_code, rp.rate_pack_hash
            {limit_clause}
        )
        SELECT * FROM selected_rates
    """
    rows = await db.all(sql, snapshot_id=snapshot_id)
    if not rows:
        return None

    plans: dict[str, Any] = {}
    procedures: dict[str, Any] = {}
    providers: dict[str, Any] = {}
    rates: dict[str, dict[str, list[dict[str, Any]]]] = {}

    for raw_row in rows:
        row = _row_mapping(raw_row)
        plan_id = str(row.get("plan_id") or "").strip()
        code = str(row.get("billing_code") or "").strip().upper()
        provider_key = str(row.get("provider_set_hash") or row.get("rate_pack_hash") or "").strip()
        if not plan_id or not code or not provider_key:
            continue
        provider_count = int(row.get("provider_count") or 0)
        provider_set_count = int(row.get("provider_set_count") or 0)
        plans.setdefault(
            plan_id,
            {
                "plan_id": plan_id,
                "plan_name": row.get("plan_name"),
                "plan_id_type": row.get("plan_id_type"),
                "plan_market_type": row.get("plan_market_type"),
                "issuer_name": row.get("issuer_name"),
                "plan_sponsor_name": row.get("plan_sponsor_name"),
            },
        )
        procedures.setdefault(
            code,
            {
                "code": code,
                "billing_code": code,
                "billing_code_type": row.get("billing_code_type"),
                "name": row.get("procedure_name"),
                "description": row.get("procedure_description"),
            },
        )
        provider_payload = providers.setdefault(
            provider_key,
            {
                "provider_ordinal": provider_key,
                "provider_set_hash": provider_key,
                "provider_name": "TiC provider set",
                "provider_count": provider_count,
                "provider_set_count": provider_set_count,
            },
        )
        provider_payload["provider_count"] = max(int(provider_payload.get("provider_count") or 0), provider_count)
        provider_payload["provider_set_count"] = max(
            int(provider_payload.get("provider_set_count") or 0),
            provider_set_count,
        )
        rate_payload_json = row.get("rate_payload") or {}
        if isinstance(rate_payload_json, str):
            try:
                rate_payload_json = json.loads(rate_payload_json)
            except json.JSONDecodeError:
                rate_payload_json = {}
        prices_json = row.get("prices") or []
        if isinstance(prices_json, str):
            try:
                prices_json = json.loads(prices_json)
            except json.JSONDecodeError:
                prices_json = []
        source_trace_json = row.get("source_trace") or []
        if isinstance(source_trace_json, str):
            try:
                source_trace_json = json.loads(source_trace_json)
            except json.JSONDecodeError:
                source_trace_json = []
        rates.setdefault(plan_id, {}).setdefault(code, []).append(
            {
                "provider_ordinal": provider_key,
                "provider_set_hash": provider_key,
                "provider_set_hashes": rate_payload_json.get("provider_set_hashes") or [],
                "provider_count": provider_count,
                "provider_set_count": provider_set_count,
                "billing_code_type": row.get("billing_code_type"),
                "prices": prices_json,
                "price_set_hash": row.get("price_set_hash"),
                "source_trace": source_trace_json,
                "confidence": {
                    "network": PTG2_CONFIDENCE_TIC_RATE_NPI_TIN,
                    "location": PTG2_CONFIDENCE_NPPES_PRACTICE_LOCATION,
                    "acceptance_statement": ptg2_confidence_statement(PTG2_CONFIDENCE_TIC_RATE_NPI_TIN),
                },
            }
        )

    payload = {
        "version": 2,
        "snapshot_id": snapshot_id,
        "generated_at": _utcnow().isoformat(),
        "plans": plans,
        "procedures": procedures,
        "providers": providers,
        "rates": rates,
    }
    store = PTG2ArtifactStore()
    target = store.root / PTG2_ARTIFACT_SNAPSHOT_INDEX / f"{snapshot_id}.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp_target = target.with_suffix(".json.tmp")
    tmp_target.write_text(json.dumps(payload, sort_keys=True, default=_json_default), encoding="utf-8")
    artifact_sha, byte_count = sha256_file(tmp_target)
    os.replace(tmp_target, target)
    artifact_uri = store.storage_uri(target)
    await _push_ptg2_objects_from_facade(
        [
            {
                "artifact_id": semantic_hash(
                    {"kind": PTG2_ARTIFACT_SNAPSHOT_INDEX, "snapshot_id": snapshot_id, "storage_uri": artifact_uri},
                    domain="artifact_manifest",
                )[:32],
                "snapshot_id": snapshot_id,
                "import_run_id": import_run_id,
                "artifact_kind": PTG2_ARTIFACT_SNAPSHOT_INDEX,
                "storage_uri": artifact_uri,
                "sha256": artifact_sha,
                "byte_count": byte_count,
                "payload": {
                    "plan_count": len(plans),
                    "procedure_count": len(procedures),
                    "provider_count": len(providers),
                    "rate_count": sum(len(items) for plan_rates in rates.values() for items in plan_rates.values()),
                    "provider_granularity": "provider_set",
                },
                "created_at": _utcnow(),
            }
        ],
        PTG2ArtifactManifest,
        rewrite=True,
    )
    return {
        "storage_uri": artifact_uri,
        "sha256": artifact_sha,
        "byte_count": byte_count,
        "plan_count": len(plans),
        "procedure_count": len(procedures),
        "provider_count": len(providers),
        "rate_count": sum(len(items) for plan_rates in rates.values() for items in plan_rates.values()),
        "provider_granularity": "provider_set",
    }
