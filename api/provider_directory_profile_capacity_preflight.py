# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Authenticated read-only Provider Directory Profile capacity preflight."""

from __future__ import annotations

from sanic import response
from sanic.exceptions import BadRequest, SanicException

from api.control_auth import require_control_auth
from process.provider_directory_fhir import (
    ProviderDirectoryArtifactBuildStale,
    provider_directory_profile_capacity_preflight,
)
from process.provider_directory_profile_selection import (
    ProviderDirectoryProfileSelectionError,
    ProviderDirectoryProfileSelectionStale,
    validated_profile_execution,
)


async def control_provider_directory_profile_capacity_preflight(request):
    """Return exact signed-lease geometry without creating durable work."""

    require_control_auth(request)
    request_payload = request.json if isinstance(request.json, dict) else {}
    if (
        request_payload.get(
            "provider_directory_profile_capacity_attestation"
        )
        != {}
    ):
        raise BadRequest(
            "Profile capacity preflight attestation must be empty"
        )
    try:
        execution = validated_profile_execution(request_payload)
        preflight = await provider_directory_profile_capacity_preflight(
            execution
        )
    except ProviderDirectoryProfileSelectionError as exc:
        raise BadRequest(str(exc)) from exc
    except (
        ProviderDirectoryArtifactBuildStale,
        ProviderDirectoryProfileSelectionStale,
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
