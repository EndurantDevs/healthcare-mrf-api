# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Authenticated read-only readiness for the fixed membership importer."""

from sanic import response
from sanic.exceptions import BadRequest

from api.control_auth import require_control_auth
from api.ptg_membership_contract import CONTRACT, validate_params
from api.ptg_membership_source import source_identity
from api.ptg_membership_storage import locate, receipt
from db.connection import db
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError


def register_membership_projection_routes(blueprint):
    """Register authenticated, read-only readiness for the fixed importer."""

    @blueprint.get("/ptg/provider-membership-projection")
    async def membership_projection_readiness(request):
        """Report absent or protected ready state for an exact audited source/code."""
        require_control_auth(request)
        params_by_name = dict(request.args)
        try:
            if any(len(request.args.getlist(name)) != 1 for name in params_by_name):
                raise ValueError("membership readiness requires one value per parameter")
            params_by_name = {name: request.args.get(name) for name in params_by_name}
            validate_params(params_by_name)
            async with db.transaction() as session:
                _, _, identity = await source_identity(session, **params_by_name)
                found = await locate(session, identity)
                result = dict(params_by_name, contract=CONTRACT, state="absent") if found is None else receipt(found)
        except PTG2ManifestArtifactError:
            raise
        except ValueError as error:
            raise BadRequest(str(error)) from error
        return response.json(result)
