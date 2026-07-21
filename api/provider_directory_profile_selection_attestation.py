# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Authenticated Provider Directory Profile selection attestation route."""

from __future__ import annotations

from sanic import response
from sanic.exceptions import BadRequest, SanicException

from api.control_auth import require_control_auth
from api.provider_directory_sources import provider_directory_source_catalog
from process.provider_directory_profile_selection import (
    ProviderDirectoryProfileSelectionDrift,
    ProviderDirectoryProfileSelectionError,
    attest_profile_selection,
)


async def control_provider_directory_profile_selection_attestation(request):
    """Attest one exact global Profile selection from a locked DB snapshot."""

    require_control_auth(request)
    request_payload = request.json if isinstance(request.json, dict) else {}
    try:
        attestation_payload = await attest_profile_selection(
            request_payload,
            provider_directory_source_catalog(),
        )
    except ProviderDirectoryProfileSelectionError as exc:
        raise BadRequest(str(exc)) from exc
    except ProviderDirectoryProfileSelectionDrift as exc:
        raise SanicException(str(exc), status_code=409) from exc
    return response.json(attestation_payload)


def register_profile_selection_route(
    control_blueprint,
) -> None:
    """Register the authenticated proof endpoint on the control blueprint."""

    control_blueprint.add_route(
        control_provider_directory_profile_selection_attestation,
        "/provider-directory/profile-selection-attestation",
        methods=("POST",),
    )
