"""Load durable evidence for one direct allowed-amount blank result."""

from __future__ import annotations

import logging
from typing import Any

from sqlalchemy import select

from db.models import PTG2ImportRun, PTG2Snapshot, db
from process.ptg_allowed_amount_blank import (
    allowed_amount_blank_metrics,
    is_allowed_amount_blank_error,
)

logger = logging.getLogger(__name__)


async def load_blank_failure_metrics(
    params_by_name: dict[str, Any],
    failure_error_by_name: dict[str, Any],
) -> dict[str, Any]:
    """Return exact durable metrics without replacing the original failure."""

    try:
        source_file_import_id = str(
            params_by_name.get("source_file_import_id") or ""
        ).strip()
        if (
            not source_file_import_id
            or not is_allowed_amount_blank_error(failure_error_by_name)
            or params_by_name.get("source_file_import_id")
            != source_file_import_id
            or params_by_name.get("import_id") != source_file_import_id
            or params_by_name.get("max_files") != 1
            or not params_by_name.get("allowed_url")
            or params_by_name.get("in_network_url") is not None
        ):
            return {}
        engine_run_result = await db.execute(
            select(PTG2ImportRun)
            .where(
                PTG2ImportRun.import_run_id
                == f"ptg2:{source_file_import_id}"
            )
            .limit(1)
        )
        engine_run = engine_run_result.scalar_one_or_none()
        report_by_name = (
            engine_run.report
            if engine_run is not None and isinstance(engine_run.report, dict)
            else {}
        )
        snapshot_id = report_by_name.get("snapshot_id")
        if not isinstance(snapshot_id, str) or not snapshot_id:
            return {}
        snapshot_result = await db.execute(
            select(PTG2Snapshot)
            .where(PTG2Snapshot.snapshot_id == snapshot_id)
            .limit(1)
        )
        return allowed_amount_blank_metrics(
            source_file_import_id=source_file_import_id,
            source_key=str(params_by_name.get("source_key") or ""),
            import_month=params_by_name.get("import_month"),
            plan_ids=params_by_name.get("plan_ids") or [],
            plan_market_types=params_by_name.get("plan_market_types") or [],
            outer_error=failure_error_by_name,
            engine_run=engine_run,
            engine_snapshot=snapshot_result.scalar_one_or_none(),
        ) or {}
    except Exception:
        logger.exception("Failed to load durable allowed-amount blank evidence")
        return {}
