# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Register modular control-plane routes on the shared blueprint."""

from __future__ import annotations

from sanic import response
from sanic.exceptions import BadRequest, NotFound, SanicException
from sanic.response import HTTPResponse

from api.control_auth import require_control_auth
from api.control_imports import (
    reconcile_terminal_queue_residue,
    reconcile_stale_worker_failure,
    StaleWorkerReconciliationConflict,
    StaleWorkerReconciliationUnavailable,
)
from api.control_wave_routes import register_control_wave_routes
from api.hospital_price_status import (
    hospital_price_page_limit,
    list_hospital_price_status_page,
)


async def control_reconcile_stale_worker(request, run_id: str) -> HTTPResponse:
    """Fail one exact stale run after proving its worker state is absent."""

    require_control_auth(request)
    request_by_field = request.json if isinstance(request.json, dict) else {}
    try:
        receipt_by_field = await reconcile_stale_worker_failure(
            run_id,
            request_by_field,
        )
    except StaleWorkerReconciliationConflict as exc:
        raise SanicException(str(exc), status_code=409) from exc
    except StaleWorkerReconciliationUnavailable as exc:
        raise SanicException(str(exc), status_code=503) from exc
    except ValueError as exc:
        raise BadRequest(str(exc)) from exc
    if receipt_by_field is None:
        raise NotFound("import run not found")
    return response.json(receipt_by_field, default=str)


async def control_reconcile_terminal_queue_residue(
    request,
    run_id: str,
) -> HTTPResponse:
    """Remove one exact orphaned queue member for a terminal import run."""

    require_control_auth(request)
    request_by_field = request.json if isinstance(request.json, dict) else {}
    try:
        receipt_by_field = await reconcile_terminal_queue_residue(
            run_id,
            request_by_field,
        )
    except StaleWorkerReconciliationConflict as exc:
        raise SanicException(str(exc), status_code=409) from exc
    except StaleWorkerReconciliationUnavailable as exc:
        raise SanicException(str(exc), status_code=503) from exc
    except ValueError as exc:
        raise BadRequest(str(exc)) from exc
    if receipt_by_field is None:
        raise NotFound("import run not found")
    return response.json(receipt_by_field, default=str)


def register_control_routes(blueprint):
    """Register wave and hospital-price control routes."""

    register_control_wave_routes(blueprint)
    blueprint.add_route(
        control_reconcile_stale_worker,
        "/imports/<run_id>/reconcile-stale-worker",
        methods={"POST"},
    )
    blueprint.add_route(
        control_reconcile_terminal_queue_residue,
        "/imports/<run_id>/reconcile-terminal-queue-residue",
        methods={"POST"},
    )

    @blueprint.get("/hospital-prices")
    async def control_hospital_prices(request) -> HTTPResponse:
        """List hospital registry rows with attempt and LKG status."""

        require_control_auth(request)
        try:
            status_page = await list_hospital_price_status_page(
                query=request.args.get("q"),
                status=request.args.get("status"),
                cursor=request.args.get("cursor"),
                limit=hospital_price_page_limit(request.args.get("limit")),
            )
        except ValueError as exc:
            raise BadRequest(str(exc)) from exc
        return response.json(status_page, default=str)

    return blueprint
