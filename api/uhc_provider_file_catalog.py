# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Sanic-facing error contract for UHC catalog-only discovery."""

from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Any

from sanic import response
from sanic.exceptions import BadRequest, NotFound, SanicException
from sqlalchemy.exc import SQLAlchemyError

from api.control_auth import require_control_auth
from process.uhc_provider_file_catalog import (
    refresh_uhc_provider_file_catalog,
    uhc_provider_file_catalog,
)
from process.uhc_provider_file_catalog_types import (
    UHCFileCatalogError,
    UHCFileCatalogNotFound,
)


logger = logging.getLogger(__name__)


async def read_uhc_provider_file_catalog(
    request_arguments: Mapping[str, Any],
) -> dict[str, Any]:
    """Read one semantic or raw catalog observation with stable errors."""

    if request_arguments.get("root_run_id") is not None:
        raise NotFound("UHC acquisition-root history is not available")
    try:
        return await uhc_provider_file_catalog(
            catalog_set_sha256=request_arguments.get("catalog_set_sha256"),
            raw_set_sha256=request_arguments.get("raw_set_sha256"),
        )
    except ValueError as error:
        raise BadRequest(str(error)) from error
    except UHCFileCatalogNotFound as error:
        raise NotFound("UHC catalog observation was not found") from error
    except (UHCFileCatalogError, SQLAlchemyError, ConnectionRefusedError) as error:
        logger.warning("UHC catalog read failed", exc_info=True)
        raise SanicException(
            "UHC catalog proof is unavailable",
            status_code=503,
        ) from error


async def refresh_uhc_provider_file_catalog_listing() -> dict[str, Any]:
    """Refresh UHC's bounded listings with a stable external error."""

    try:
        return await refresh_uhc_provider_file_catalog()
    except (UHCFileCatalogError, SQLAlchemyError, ConnectionRefusedError) as error:
        logger.warning("UHC catalog refresh failed", exc_info=True)
        raise SanicException(
            "UHC catalog refresh is unavailable",
            status_code=503,
        ) from error


async def control_uhc_provider_directory_files(request):
    """Return authenticated catalog-only UHC file discovery."""

    require_control_auth(request)
    return response.json(
        await read_uhc_provider_file_catalog(request.args),
        default=str,
    )


async def control_refresh_uhc_provider_directory_files(request):
    """Refresh only UHC's bounded public listing documents."""

    require_control_auth(request)
    return response.json(
        await refresh_uhc_provider_file_catalog_listing(),
        default=str,
    )


def register_uhc_provider_file_catalog_routes(blueprint) -> None:
    """Register authenticated UHC catalog-only routes on the control blueprint."""

    blueprint.add_route(
        control_uhc_provider_directory_files,
        "/provider-directory/sources/uhc/files",
    )
    blueprint.add_route(
        control_refresh_uhc_provider_directory_files,
        "/provider-directory/sources/uhc/files/refresh",
        methods={"POST"},
    )
