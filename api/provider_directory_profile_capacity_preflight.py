# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Authenticated Provider Directory Profile capacity receipt preflight.

The route does not acquire source data or publish a Profile generation.  It
does durably record (or exactly replay) one immutable, single-use signing
receipt after the closed preflight checks succeed.
"""

from __future__ import annotations

from sanic import response
from sanic.exceptions import BadRequest, SanicException

from api.control_auth import require_control_auth
from process.provider_directory_fhir import (
    ProviderDirectoryArtifactBuildStale,
    provider_directory_profile_capacity_preflight,
)
from process.provider_directory_profile_capacity_preflight_contract import (
    ProviderDirectoryProfileCapacityPreflightError,
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


def register_profile_capacity_preflight_route(control_blueprint) -> None:
    """Register the authenticated capacity preflight control endpoint."""

    control_blueprint.add_route(
        control_provider_directory_profile_capacity_preflight,
        "/provider-directory/profile-capacity-preflight",
        methods=("POST",),
    )
