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
    @blueprint.get("/ptg/provider-membership-projection")
    async def membership_projection_readiness(request):
        require_control_auth(request)
        params = dict(request.args)
        try:
            if any(len(request.args.getlist(name)) != 1 for name in params):
                raise ValueError("membership readiness requires one value per parameter")
            params = {name: request.args.get(name) for name in params}
            validate_params(params)
            async with db.transaction() as session:
                _, _, identity = await source_identity(session, **params)
                found = await locate(session, identity)
                result = dict(params, contract=CONTRACT, state="absent") if found is None else receipt(found)
        except PTG2ManifestArtifactError:
            raise
        except ValueError as error:
            raise BadRequest(str(error)) from error
        return response.json(result)
