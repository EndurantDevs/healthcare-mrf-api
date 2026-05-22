# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import logging
import os
import sys
from typing import Any

from db.connection import db
from process.ptg_parts.config import (
    PTG2_COMPACT_BULK_DROP_INDEXES_ENV,
    PTG2_FAST_FINAL_REBUILD_ENV,
    PTG2_UNLOGGED_FINAL_ENV,
    _env_bool,
)
from process.ptg_parts.source_jobs import _dedupe_rows_by


logger = logging.getLogger(__name__)


async def _push_objects_from_facade(rows: list[dict[str, Any]], cls) -> None:
    ptg_module = sys.modules.get("process.ptg")
    if ptg_module is None:
        raise RuntimeError("process.ptg facade is not loaded")
    await ptg_module.push_objects(rows, cls)


async def prepare_ptg2_compact_bulk_load() -> None:
    if not _env_bool(PTG2_COMPACT_BULK_DROP_INDEXES_ENV, True):
        return
    schema = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    fast_rebuild = _env_bool(PTG2_FAST_FINAL_REBUILD_ENV, False)
    unlogged_final = _env_bool(PTG2_UNLOGGED_FINAL_ENV, fast_rebuild)
    statements = [
        f"ALTER TABLE {schema}.ptg2_serving_rate_compact SET (autovacuum_enabled = false, toast.autovacuum_enabled = false)",
        f"ALTER TABLE {schema}.ptg2_provider_set_component SET (autovacuum_enabled = false)",
        f"ALTER TABLE {schema}.ptg2_provider_group_member SET (autovacuum_enabled = false)",
        f"ALTER TABLE {schema}.ptg2_price_set SET (autovacuum_enabled = false, toast.autovacuum_enabled = false)",
        f"ALTER TABLE {schema}.ptg2_serving_rate_compact DROP CONSTRAINT IF EXISTS ptg2_serving_rate_compact_pkey",
        f"DROP INDEX IF EXISTS {schema}.ptg2_serving_rate_compact_billing_idx",
        f"DROP INDEX IF EXISTS {schema}.ptg2_serving_rate_compact_billing_order_idx",
        f"DROP INDEX IF EXISTS {schema}.ptg2_serving_rate_compact_reported_idx",
        f"DROP INDEX IF EXISTS {schema}.ptg2_serving_rate_compact_reported_order_idx",
        f"DROP INDEX IF EXISTS {schema}.ptg2_serving_rate_compact_hp_idx",
        f"DROP INDEX IF EXISTS {schema}.ptg2_serving_rate_compact_provider_idx",
        f"DROP INDEX IF EXISTS {schema}.ptg2_serving_rate_compact_price_idx",
        f"DROP INDEX IF EXISTS {schema}.ptg2_serving_rate_compact_idx_primary",
        f"DROP INDEX IF EXISTS {schema}.ptg2_provider_set_component_group_idx",
        f"DROP INDEX IF EXISTS {schema}.ptg2_provider_set_component_idx_primary",
        f"DROP INDEX IF EXISTS {schema}.ptg2_provider_group_member_npi_idx",
        f"DROP INDEX IF EXISTS {schema}.ptg2_provider_group_member_idx_primary",
        f"DROP INDEX IF EXISTS {schema}.ptg2_price_set_idx_primary",
    ]
    if fast_rebuild:
        statements = [
            f"TRUNCATE {schema}.ptg2_serving_rate_compact",
            f"TRUNCATE {schema}.ptg2_provider_set_component",
            f"TRUNCATE {schema}.ptg2_provider_group_member",
            f"TRUNCATE {schema}.ptg2_provider_set",
            f"TRUNCATE {schema}.ptg2_price_set",
        ] + statements + [
            f"DROP INDEX IF EXISTS {schema}.ptg2_serving_rate_compact_pkey",
            f"DROP INDEX IF EXISTS {schema}.ptg2_provider_set_component_pkey",
            f"DROP INDEX IF EXISTS {schema}.ptg2_provider_group_member_pkey",
            f"DROP INDEX IF EXISTS {schema}.ptg2_provider_set_pkey",
            f"DROP INDEX IF EXISTS {schema}.ptg2_price_set_pkey",
        ]
    if unlogged_final:
        statements = [
            f"ALTER TABLE {schema}.ptg2_serving_rate_compact SET UNLOGGED",
            f"ALTER TABLE {schema}.ptg2_provider_set_component SET UNLOGGED",
            f"ALTER TABLE {schema}.ptg2_provider_group_member SET UNLOGGED",
            f"ALTER TABLE {schema}.ptg2_provider_set SET UNLOGGED",
            f"ALTER TABLE {schema}.ptg2_price_set SET UNLOGGED",
        ] + statements
    for statement in statements:
        try:
            await db.status(statement)
        except Exception as exc:
            logger.debug("Skipping compact bulk-load prep statement %s: %s", statement, exc)


async def _flush_in_network_rows(
    item_rows: list[dict[str, Any]],
    billing_rows: list[dict[str, Any]],
    rate_rows: list[dict[str, Any]],
    price_rows: list[dict[str, Any]],
    item_cls,
    billing_cls,
    rate_cls,
    price_cls,
    *,
    force: bool = False,
    item_batch_rows: int = 1000,
    billing_batch_rows: int = 5000,
    rate_batch_rows: int = 5000,
    price_batch_rows: int = 10000,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    if item_rows and (force or len(item_rows) >= item_batch_rows):
        await _push_objects_from_facade(_dedupe_rows_by(item_rows, "item_hash"), item_cls)
        item_rows = []
    if billing_rows and (force or len(billing_rows) >= billing_batch_rows):
        await _push_objects_from_facade(_dedupe_rows_by(billing_rows, "code_hash"), billing_cls)
        billing_rows = []
    if rate_rows and (force or len(rate_rows) >= rate_batch_rows):
        await _push_objects_from_facade(_dedupe_rows_by(rate_rows, "rate_hash"), rate_cls)
        rate_rows = []
    if price_rows and (force or len(price_rows) >= price_batch_rows):
        await _push_objects_from_facade(_dedupe_rows_by(price_rows, "price_hash"), price_cls)
        price_rows = []
    return item_rows, billing_rows, rate_rows, price_rows
