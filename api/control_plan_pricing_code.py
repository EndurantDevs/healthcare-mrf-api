# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Authenticated complete-code pricing readiness for normal import orchestration."""

from dataclasses import asdict
from typing import Any

from sanic import response
from sanic.exceptions import BadRequest

from api.control_auth import require_control_auth
from api.plan_pricing_code_build import CodeProjectionMembershipNotReady, resolve_inputs
from api.plan_pricing_code_contract import CONTRACT, ZIP_CONTRACT, identity_digest, scope_selectors, validate_params
from api.plan_pricing_code_storage import locate, receipt
from api.plan_pricing_projection_contract import _lock_provider_relations
from api.plan_release_serving_resolution import PLAN_RELEASE_RESOLUTION_READY, resolve_plan_release_serving_resolution
from db.connection import db


def code_pricing_registry_entry(entry: dict[str, Any]) -> dict[str, Any]:
    """Describe the closed code-pricing selectors on a fresh membership entry."""
    entry["name"] = "plan-pricing-code-projection"
    for field, name in zip(entry["params_schema"][:2], ("plan_release_id", "serving_revision_id"), strict=True):
        field.update(
            name=name, opts=["--" + name.replace("_", "-")], help="Exact current immutable release coordinate."
        )
    entry["params_schema"].append(
        {
            "name": "expected_identity_digest",
            "opts": ["--expected-identity-digest"],
            "required": True,
            "multiple": False,
            "is_flag": False,
            "type": "string",
            "default": None,
            "help": "Exact complete identity digest reported by readiness.",
        }
    )
    for name in ("scope_kind", "zip5", "zip_radius_miles", "operation"):
        entry["params_schema"].append(
            {
                "name": name,
                "opts": ["--" + name.replace("_", "-")],
                "required": False,
                "multiple": False,
                "is_flag": False,
                "type": "string",
                "default": None,
                "help": (
                    "Operator-signed nonpublishing calibration; only calibrate is accepted."
                    if name == "operation"
                    else "Optional complete ZIP aggregate scope; supply all three canonical scope selectors together."
                ),
            }
        )
    return entry


def _readiness_result(selectors_by_field, contract, identity, found, scope):
    if found is not None:
        return receipt(found)
    result_by_field = {
        **selectors_by_field,
        "contract": contract,
        "state": "absent",
        "identity": identity,
        "expected_identity_digest": identity_digest(identity),
    }
    if scope is not None:
        from api.plan_pricing_code_zip_build import work_limits

        try:
            work_limits()
        except ValueError:
            result_by_field.update(state="unqualified", missing="scoped_card_resource_calibration")
    return result_by_field


def register_code_pricing_routes(blueprint):
    """Register authenticated immutable procedure readiness without preparing data."""

    @blueprint.get("/plan-pricing/code-projection")
    async def code_projection_readiness(request):
        """Resolve the exact current release through its request-owned Reader."""
        require_control_auth(request)
        try:
            selectors_by_field = {name: request.args.get(name) for name in request.args}
            if any(len(request.args.getlist(name)) != 1 for name in selectors_by_field):
                raise ValueError("code pricing readiness requires one value per selector")
            validate_params(selectors_by_field, include_identity=False)
            scope = scope_selectors(selectors_by_field)
            contract = CONTRACT if scope is None else ZIP_CONTRACT
            async with db.transaction() as session:
                if scope is not None:
                    await _lock_provider_relations(session)
                selected = await resolve_plan_release_serving_resolution(session, selectors_by_field["plan_release_id"])
                if (
                    selected.state != PLAN_RELEASE_RESOLUTION_READY
                    or selected.selection.serving_revision_id != selectors_by_field["serving_revision_id"]
                ):
                    raise ValueError("code pricing requires the exact current published release revision")
                try:
                    identity, _inputs = await resolve_inputs(
                        session,
                        selected.selection,
                        (selectors_by_field["code_system"], selectors_by_field["code"]),
                        **({} if scope is None else {"scope": scope}),
                    )
                except CodeProjectionMembershipNotReady:
                    return response.json(
                        {
                            **selectors_by_field,
                            "contract": contract,
                            "state": "prerequisite_missing",
                            "missing": "complete_source_procedure_membership",
                            "binding_set_digest": selected.selection.binding_set_digest,
                            "bindings": [asdict(binding) for binding in selected.selection.in_network_bindings],
                            "required_membership": [
                                {
                                    "source_key": binding.source_key,
                                    "snapshot_id": binding.snapshot_id,
                                    "code_system": selectors_by_field["code_system"],
                                    "code": selectors_by_field["code"],
                                }
                                for binding in selected.selection.in_network_bindings
                            ],
                        }
                    )
                found = await locate(session, identity, allow_stale_provider=True)
                readiness_by_field = _readiness_result(selectors_by_field, contract, identity, found, scope)
        except ValueError as error:
            raise BadRequest(str(error)) from error
        return response.json(readiness_by_field)
