# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Authenticated Provider Directory Profile capacity control preflights.

The route does not acquire source data or publish a Profile generation.  It
either projects authority facts in a read-only transaction or durably records
(or exactly replays) one immutable, single-use signing receipt.
"""

from __future__ import annotations

import ipaddress

from sanic import response
from sanic.exceptions import BadRequest, Forbidden, SanicException

from api.control_auth import require_control_auth
from process.provider_directory_fhir import (
    ProviderDirectoryArtifactBuildStale,
    provider_directory_profile_capacity_authority_projection,
    provider_directory_profile_capacity_preflight,
)
from process.provider_directory_profile_capacity_preflight_contract import (
    ProviderDirectoryProfileCapacityPreflightError,
    validated_capacity_authority_projection_request,
    validated_capacity_preflight_request,
)
from process.provider_directory_profile_capacity_runtime import (
    ProviderDirectoryProfileCapacityConfigurationError,
)
from process.provider_directory_profile_selection import (
    ProviderDirectoryProfileSelectionError,
    ProviderDirectoryProfileSelectionStale,
)
from process.provider_directory_profile_runtime_observation import (
    ProviderDirectoryProfileRuntimeObservationError,
)


async def control_provider_directory_profile_capacity_preflight(request):
    """Return and durably record one exact replay-fenced signing receipt."""

    require_control_auth(request)
    request_payload = request.json if isinstance(request.json, dict) else {}
    try:
        preflight_request = validated_capacity_preflight_request(request_payload)
        preflight = await provider_directory_profile_capacity_preflight(
            preflight_request
        )
    except (
        ProviderDirectoryProfileCapacityConfigurationError,
        ProviderDirectoryProfileCapacityPreflightError,
        ProviderDirectoryProfileSelectionError,
    ) as exc:
        raise BadRequest(str(exc)) from exc
    except (
        ProviderDirectoryArtifactBuildStale,
        ProviderDirectoryProfileSelectionStale,
        ProviderDirectoryProfileRuntimeObservationError,
    ) as exc:
        raise SanicException(str(exc), status_code=409) from exc
    return response.json(preflight)


def _require_socket_loopback(request) -> None:
    """Require the peer socket itself to be loopback; ignore proxy headers."""

    peername = getattr(getattr(request, "conn_info", None), "peername", None)
    host = peername[0] if isinstance(peername, tuple) and peername else None
    try:
        address = ipaddress.ip_address(host) if isinstance(host, str) else None
    except ValueError:
        address = None
    if address is not None and address.version == 6 and address.ipv4_mapped:
        address = address.ipv4_mapped
    if address is None or not address.is_loopback:
        raise Forbidden("loopback control transport is required")


async def control_profile_capacity_authority_projection(request):
    """Return receipt-free capacity geometry over authenticated loopback."""

    require_control_auth(request)
    _require_socket_loopback(request)
    request_payload = request.json if isinstance(request.json, dict) else {}
    try:
        projection_request = validated_capacity_authority_projection_request(
            request_payload
        )
        projection = (
            await provider_directory_profile_capacity_authority_projection(
                projection_request
            )
        )
    except (
        ProviderDirectoryProfileCapacityConfigurationError,
        ProviderDirectoryProfileCapacityPreflightError,
        ProviderDirectoryProfileSelectionError,
    ) as exc:
        raise BadRequest(str(exc)) from exc
    except (
        ProviderDirectoryArtifactBuildStale,
        ProviderDirectoryProfileSelectionStale,
        ProviderDirectoryProfileRuntimeObservationError,
    ) as exc:
        raise SanicException(str(exc), status_code=409) from exc
    return response.json(projection)


def register_profile_capacity_preflight_route(control_blueprint) -> None:
    """Register the authenticated capacity preflight control endpoint."""

    control_blueprint.add_route(
        control_provider_directory_profile_capacity_preflight,
        "/provider-directory/profile-capacity-preflight",
        methods=("POST",),
    )
    control_blueprint.add_route(
        control_profile_capacity_authority_projection,
        "/provider-directory/profile-capacity-authority-projection",
        methods=("POST",),
    )
