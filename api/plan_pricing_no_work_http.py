# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded read-only evidence on the existing authenticated control GET."""

import asyncio
import json
from contextlib import asynccontextmanager
from datetime import UTC, datetime

from sanic import response
from sanic.exceptions import BadRequest, SanicException

from api.plan_pricing_no_work_backends import pricing_kubernetes_no_work, pricing_redis_no_work
from db.connection import ConnectionProxy
from db.models import db
from process.control_lifecycle import acquire_control_run_worker_action_lock
from process.plan_pricing_admission import require, require_managed_pricing_admission
from process.plan_pricing_control_transaction import pricing_driver
from process.plan_pricing_no_work import (
    CONTRACT,
    _expected_runs,
    native_lineage_witness,
    native_run,
    native_run_coordinates,
    require_native_control_shape,
)
from process.plan_pricing_retained_inputs import require_retained_pricing_inputs


@asynccontextmanager
async def managed_pricing_no_work(run_id):
    """Hold owned control fences through fresh evidence, final reread and encoding."""
    async with asyncio.timeout(8), db.transaction() as session:
        driver = await pricing_driver(session)
        await driver.execute(
            "SET LOCAL lock_timeout='500ms'; SET LOCAL statement_timeout='5s'; SET LOCAL TIME ZONE 'UTC'"
        )
        current = await native_run(driver, run_id)
        inspected = await require_managed_pricing_admission(driver, current, action="inspect")
        require(inspected is not None, "managed_admission_required")
        known = sorted(_expected_runs(inspected))
        for known_id in known:
            await acquire_control_run_worker_action_lock(ConnectionProxy(db, session, None), known_id)
        admitted = await require_managed_pricing_admission(driver, current, action="replay")
        require(admitted is not None and sorted(_expected_runs(admitted)) == known, "native_lineage_changed")
        await require_native_control_shape(driver)
        await require_retained_pricing_inputs(driver, admitted)
        runs, lineage_digest = await native_lineage_witness(driver, admitted)
        coordinates = []
        for run_by_field in runs:
            kubernetes = await asyncio.to_thread(pricing_kubernetes_no_work, run_by_field)
            redis = await pricing_redis_no_work(run_by_field)
            coordinates.append({**native_run_coordinates(run_by_field), "redis": redis, "kubernetes": kubernetes})
        final = await require_managed_pricing_admission(driver, current, action="replay")
        _runs, final_digest = await native_lineage_witness(driver, final)
        require(
            final_digest == lineage_digest and dict(final.record_by_field) == dict(admitted.record_by_field),
            "native_control_changed",
        )
        receipt_by_field = {
            "contract": CONTRACT,
            "node_id": admitted.record_by_field["node_id"],
            "preparation_id": str(admitted.record_by_field["preparation_id"]),
            **{
                name: admitted.record_by_field[name]
                for name in ("scope_digest", "publication_identity", "admission_key")
            },
            "publication_attempt_identity": admitted.record_by_field["attempt_identity"],
            "admission_status": admitted.record_by_field["status"],
            "preparation_status": admitted.record_by_field["preparation_status"],
            "observed_at": datetime.now(UTC).isoformat(timespec="microseconds"),
            "lineage_digest": lineage_digest,
            "runs": coordinates,
        }
        require(len(json.dumps(receipt_by_field, ensure_ascii=True).encode()) <= 65536, "evidence_wire_limit")
        yield receipt_by_field


async def pricing_no_work_response(request, run_id):
    """Authenticate in the caller, then accept only one fixed evidence selector."""
    require_values = request.args.getlist("evidence")
    if require_values != [CONTRACT]:
        raise BadRequest("unsupported pricing control evidence selector")
    try:
        async with managed_pricing_no_work(run_id) as receipt_by_field:
            return response.json(receipt_by_field)
    except ValueError as error:
        raise SanicException("pricing no-work evidence is not qualified", status_code=409) from error
    except Exception as error:
        raise SanicException("pricing no-work evidence is unavailable", status_code=503) from error
