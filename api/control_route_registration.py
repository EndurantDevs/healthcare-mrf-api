# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Register modular control-plane routes on the shared blueprint."""

from __future__ import annotations

from sanic import response
from sanic.exceptions import BadRequest

from api.control_auth import require_control_auth
from api.control_wave_routes import register_control_wave_routes
from api.hospital_price_status import (
    hospital_price_page_limit,
    list_hospital_price_status_page,
)


def register_control_routes(blueprint):
    """Register wave and hospital-price control routes."""

    register_control_wave_routes(blueprint)

    @blueprint.get("/hospital-prices")
    async def control_hospital_prices(request):
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
