# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Extend the existing nonprofile plan without changing historical CMS payloads."""

import json
from dataclasses import dataclass, fields

from process.provider_directory_cms_preparation import NonprofileAdmissionPlan
from process.provider_directory_serving_capacity_contract import (
    GEOMETRY,
    PURPOSE,
    canonical,
    extend_plan_values,
    require,
    require_ordinary_selection,
    validate_geometry,
)


@dataclass(frozen=True)
class OrdinaryServingAdmissionPlan(NonprofileAdmissionPlan):
    ordinary_scope_json: str = ""

    @property
    def payload(self):
        """Encode common geometry with the closed ordinary retention scope."""
        values_by_field = {field.name: getattr(self, field.name) for field in fields(NonprofileAdmissionPlan)}
        values_by_field.update(
            contract_id=GEOMETRY,
            admission_purpose=PURPOSE,
            ordinary_scope=json.loads(self.ordinary_scope_json),
        )
        return validate_geometry(json.loads(canonical(values_by_field)))


def ordinary_plan(base_plan, scope, *, paired_profile_wal):
    """Add retention to an engine-produced plan; no lease is converted or reused."""
    require(type(base_plan) is NonprofileAdmissionPlan, "base_plan_type")
    value = extend_plan_values(base_plan.payload, scope, paired_profile_wal=paired_profile_wal)
    value.pop("contract_id")
    value.pop("admission_purpose")
    scope_json = canonical(value.pop("ordinary_scope"))
    for name in ("publish_targets", "resource_types", "native_address_targets"):
        value[name] = tuple(value[name])
    value["reservation_bytes"] = tuple(tuple(entry) for entry in value["reservation_bytes"])
    return OrdinaryServingAdmissionPlan(**value, ordinary_scope_json=scope_json)


def validate_plan_scope(plan):
    """Accept only the two reviewed plan types before shared admission."""
    require(type(plan) in {NonprofileAdmissionPlan, OrdinaryServingAdmissionPlan}, "plan_type")
    if type(plan) is OrdinaryServingAdmissionPlan:
        validate_geometry(plan.payload)


def plan_purpose(plan):
    """Keep ordinary and CMS ledger purposes distinct."""
    validate_plan_scope(plan)
    return PURPOSE if type(plan) is OrdinaryServingAdmissionPlan else "cms_nonprofile"


def require_selection(plan, attestation):
    """Bind each plan to its own validated selection kind and date."""
    validate_plan_scope(plan)
    if type(plan) is OrdinaryServingAdmissionPlan:
        require(plan.desired_profile_as_of == require_ordinary_selection(attestation.payload), "selection_date")
    else:
        require(attestation.operation == "publish" and attestation.desired_cms_dataset is not None, "cms_selection")
