# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio
import logging
import os
import sys
from typing import Any

from db.connection import db
from db.models import (
    PTG2PriceAtom,
    PTG2PriceSet,
    PTG2Procedure,
    PTG2ProviderGroup,
    PTG2ProviderGroupMember,
    PTG2ProviderLocation,
    PTG2ProviderSet,
    PTG2ProviderSetComponent,
    PTG2RatePack,
    PTG2ServingRate,
    PTG2ServingRateCompact,
    PTG2SourceTrace,
    PTG2SourceTraceSet,
)
from process.ptg_parts.compact_state import _compact_add_unique, _compact_streaming_dedupe_tables
from process.ptg_parts.config import (
    PTG2_ASYNC_WRITE_TASKS_ENV,
    PTG2_STAGE_PRICE_SETS_ENV,
    PTG2_STAGE_SERVING_RATES_ENV,
    PTG2_STREAMING_DEDUPE_ENV,
    _env_bool,
    _env_int,
    _use_compact_serving_table,
)
from process.ptg_parts.copy_load import (
    _copy_compact_serving_rate_rows,
    _copy_stage_price_set_rows,
    _copy_stage_serving_rate_rows,
)
from process.ptg_parts.domain import PTG2_DOMAIN_IN_NETWORK
from process.ptg_parts.progress import _utcnow
from process.ptg_parts.serving_only import _normalize_serving_price_payload
from process.ptg_parts.serving_rows import _ptg2_serving_rate_row
from process.ptg_parts.source_jobs import _dedupe_rows_by
from process.ptg_parts.values import (
    build_provider_set_collection,
    build_rate_pack_procedure_group,
    provider_hash_bucket,
    ptg2_provider_bucket_count,
)


logger = logging.getLogger(__name__)


async def _push_ptg2_objects_from_facade(rows: list[dict[str, Any]], cls, *, rewrite: bool = True) -> None:
    ptg_module = sys.modules.get("process.ptg")
    if ptg_module is None:
        raise RuntimeError("process.ptg facade is not loaded")
    await ptg_module._push_ptg2_objects(rows, cls, rewrite=rewrite)


async def _existing_price_set_hashes() -> set[str]:
    schema_name = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    rows = await db.all(f"SELECT price_set_hash FROM {schema_name}.ptg2_price_set")
    return {str(row[0]) for row in rows if row and row[0]}


async def _flush_compact_rows(state: dict[str, Any], *, force: bool = False) -> None:
    specs = (
        ("provider_group", PTG2ProviderGroup),
        ("provider_group_member", PTG2ProviderGroupMember),
        ("provider_set", PTG2ProviderSet),
        ("provider_set_component", PTG2ProviderSetComponent),
        ("provider_location", PTG2ProviderLocation),
        ("procedure", PTG2Procedure),
        ("price_atom", PTG2PriceAtom),
        ("price_set", PTG2PriceSet),
        ("source_trace", PTG2SourceTrace),
        ("source_trace_set", PTG2SourceTraceSet),
        ("rate_pack", PTG2RatePack),
        ("serving_rate_compact", PTG2ServingRateCompact),
        ("serving_rate", PTG2ServingRate),
    )
    streaming_dedupe_tables = _compact_streaming_dedupe_tables()
    conflict_keys = {
        "provider_group": "provider_group_hash",
        "provider_group_member": ("provider_group_hash", "npi"),
        "provider_set": "provider_set_hash",
        "provider_set_component": ("provider_set_hash", "provider_group_hash"),
        "provider_location": "location_hash",
        "procedure": "procedure_hash",
        "price_atom": "price_atom_hash",
        "price_set": "price_set_hash",
        "source_trace": "source_trace_hash",
        "source_trace_set": "source_trace_set_hash",
        "rate_pack": "rate_pack_hash",
        "serving_rate_compact": "serving_rate_id",
        "serving_rate": "serving_rate_id",
    }
    for table_key, cls in specs:
        rows = state["rows"][table_key]
        if rows and (force or len(rows) >= state["batch_rows"]):
            state["rows"][table_key] = []
            if table_key in streaming_dedupe_tables:
                rows = _dedupe_rows_by(rows, conflict_keys[table_key])
            if (
                table_key == "serving_rate_compact"
                and _use_compact_serving_table()
                and state.get("snapshot_id")
            ):
                await _copy_compact_serving_rate_rows(rows, state["snapshot_id"])
                continue
            if (
                table_key == "price_set"
                and _env_bool(PTG2_STAGE_PRICE_SETS_ENV, True)
                and state.get("snapshot_id")
            ):
                await _copy_stage_price_set_rows(rows, state["snapshot_id"])
                continue
            if (
                table_key == "serving_rate"
                and _env_bool(PTG2_STAGE_SERVING_RATES_ENV, True)
                and state.get("snapshot_id")
            ):
                try:
                    await _copy_stage_serving_rate_rows(rows, state["snapshot_id"])
                    continue
                except Exception as exc:
                    logger.warning("PTG2 serving_rate stage fallback to direct write: %s", exc)
            await _schedule_compact_write(state, rows, cls)


async def _schedule_compact_write(state: dict[str, Any], rows: list[dict[str, Any]], cls) -> None:
    if not rows:
        return
    if cls is PTG2PriceSet and _env_bool(PTG2_STREAMING_DEDUPE_ENV, False):
        await _push_ptg2_objects_from_facade(rows, cls, rewrite=True)
        return
    max_pending = max(_env_int(PTG2_ASYNC_WRITE_TASKS_ENV, 1), 1)
    if max_pending <= 1:
        await _push_ptg2_objects_from_facade(rows, cls, rewrite=True)
        return
    task = asyncio.create_task(_push_ptg2_objects_from_facade(rows, cls, rewrite=True))
    pending = state.setdefault("pending_writes", [])
    pending.append(task)
    if len(pending) < max_pending:
        return
    done, waiting = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
    state["pending_writes"] = list(waiting)
    for completed in done:
        await completed


async def _drain_compact_writes(state: dict[str, Any]) -> None:
    pending = state.get("pending_writes") or []
    if not pending:
        return
    state["pending_writes"] = []
    await asyncio.gather(*pending)


async def _flush_compact_rate_pack_groups(state: dict[str, Any], context_hash: str) -> None:
    if not state["rate_pack_groups"]:
        return
    procedure_rate_pack_groups: dict[tuple[str, str, str], dict[str, Any]] = {}
    for (procedure_hash, price_set_hash, grouped_source_trace_set_hash), provider_set_hashes in state[
        "rate_pack_groups"
    ].items():
        provider_collection = build_provider_set_collection(list(provider_set_hashes))
        procedure_group_key = (
            provider_collection["provider_set_collection_hash"],
            price_set_hash,
            grouped_source_trace_set_hash,
        )
        grouped = procedure_rate_pack_groups.setdefault(
            procedure_group_key,
            {
                "procedure_hashes": set(),
                "provider_set_hashes": set(provider_collection["provider_set_hashes"]),
                "price_set_hash": price_set_hash,
                "source_trace_set_hash": grouped_source_trace_set_hash,
            },
        )
        grouped["procedure_hashes"].add(procedure_hash)

    for grouped in procedure_rate_pack_groups.values():
        procedure_hashes = sorted(grouped["procedure_hashes"])
        rate_pack = build_rate_pack_procedure_group(
            context_hash,
            PTG2_DOMAIN_IN_NETWORK,
            procedure_hashes,
            list(grouped["provider_set_hashes"]),
            grouped["price_set_hash"],
            grouped["source_trace_set_hash"],
        )
        rate_pack_row = {
            **rate_pack,
            "created_at": _utcnow(),
        }
        if _compact_add_unique(state, "rate_pack", "rate_pack_hash", rate_pack_row):
            state["counts"]["rate_packs"] += 1
            provider_bucket = provider_hash_bucket(
                rate_pack_row["provider_set_hash"],
                bucket_count=ptg2_provider_bucket_count(),
            )
            provider_set_hashes = list(grouped["provider_set_hashes"])
            provider_count = sum(int(state["provider_set_counts"].get(provider_hash, 0) or 0) for provider_hash in provider_set_hashes)
            price_payload = _normalize_serving_price_payload(state["price_payloads"].get(grouped["price_set_hash"], []))
            source_trace_payload = state.get("source_trace_payload") or []
            for procedure_hash in procedure_hashes:
                state["chunk_rate_packs"].setdefault((procedure_hash, provider_bucket), set()).add(
                    rate_pack_row["rate_pack_hash"]
                )
                procedure_payload = state["procedure_payloads"].get(procedure_hash)
                if procedure_payload and state.get("snapshot_id") and state.get("plan_fields"):
                    serving_row = _ptg2_serving_rate_row(
                        snapshot_id=state["snapshot_id"],
                        plan_fields=state["plan_fields"],
                        procedure_payload=procedure_payload,
                        rate_pack_row=rate_pack_row,
                        provider_set_hashes=provider_set_hashes,
                        provider_count=provider_count,
                        provider_set_count=len(provider_set_hashes),
                        prices=price_payload,
                        source_trace=source_trace_payload,
                    )
                    if _compact_add_unique(state, "serving_rate", "serving_rate_id", serving_row):
                        state["counts"]["serving_rates"] += 1
        await _flush_compact_rows(state)
    state["rate_pack_groups"] = {}
    await _flush_compact_rows(state, force=True)
