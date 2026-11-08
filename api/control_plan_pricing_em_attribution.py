# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Closed control capability for immutable release-attributed E&M prices."""

from process.network_bootstrap_sources import _roles

IMPORTER = "plan-pricing-em-attribution"
ADAPTER = {
    "queue": "arq:PTGCandidateAudit",
    "function": "control_single_job_start",
    "payload": "control_wrapped_kwargs",
    "target_module": "api.plan_pricing_em_attribution",
    "target_function": "build_plan_pricing_em_attribution",
    "job_prefix": "plan_pricing_em_attribution",
}
_PARAM_NAMES = frozenset({"plan_release_id", "serving_revision_id", "owner_role", "runtime_roles"})


def validate_em_attribution_params(importer, params_by_name):
    """Refuse malformed requests before admission, persistence or enqueue work."""
    if importer != IMPORTER:
        return
    if type(params_by_name) is not dict or set(params_by_name) != _PARAM_NAMES:
        raise ValueError(
            "plan-pricing-em-attribution requires exactly plan_release_id, serving_revision_id, owner_role, runtime_roles"
        )
    if any(
        type(params_by_name[name]) is not str
        or not params_by_name[name]
        or params_by_name[name] != params_by_name[name].strip()
        for name in ("plan_release_id", "serving_revision_id", "owner_role")
    ):
        raise ValueError("plan-pricing-em-attribution identity fields must be non-empty trimmed strings")
    role_names = params_by_name["runtime_roles"]
    if (
        type(role_names) is not list
        or not 1 <= len(role_names) <= 32
        or any(type(role) is not str for role in role_names)
    ):
        raise ValueError("plan-pricing-em-attribution runtime_roles must be a bounded JSON list of strings")
    _roles(params_by_name["owner_role"], tuple(role_names))


def em_attribution_registry_entry(engine_name):
    """Expose the existing single-job worker with a closed four-parameter schema."""
    return {
        "name": IMPORTER,
        "engine": engine_name,
        "family": "mrf",
        "kind": "control",
        "lifecycle": "single",
        "schedulable": False,
        "cancelable": False,
        "retryable": True,
        "enqueue_adapter": "arq_single_job",
        "queue": ADAPTER["queue"],
        "depends_on": [],
        "params_schema": [
            {
                "name": name,
                "opts": ["--" + name.replace("_", "-")],
                "required": True,
                "multiple": False,
                "is_flag": False,
                "type": kind,
                "default": None,
                "help": help_text,
            }
            for name, kind, help_text in (
                ("plan_release_id", "string", "Exact current immutable plan release ID."),
                ("serving_revision_id", "string", "Exact current serving revision ID."),
                ("owner_role", "string", "Existing protected artifact owner role."),
                ("runtime_roles", "array", "Sorted unique JSON array of existing Reader role names."),
            )
        ],
    }
