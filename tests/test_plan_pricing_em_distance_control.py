# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Control lifecycle contract for the exact E&M distance build."""

import pytest

from api import control_imports, control_workers


def test_em_distance_build_uses_the_durable_projection_worker() -> None:
    importer_by_name = next(
        entry_by_field
        for entry_by_field in control_imports.importer_registry()
        if entry_by_field["name"] == "plan-pricing-em-distance"
    )
    adapter_by_field = control_imports._SINGLE_JOB_ADAPTERS[
        "plan-pricing-em-distance"
    ]
    task_params_by_name = {
        "plan_release_id": "hprelease_" + "2" * 26,
        "serving_revision_id": "hpserve_" + "3" * 26,
    }
    worker_payload_by_field = control_imports._adapter_payload(
        adapter_by_field,
        {
            "run_id": "run_em_distance",
            "importer": "plan-pricing-em-distance",
            "family": "mrf",
        },
        task_params_by_name,
    )

    assert importer_by_name["schedulable"] is False
    assert [
        parameter_by_field["name"]
        for parameter_by_field in importer_by_name["params_schema"]
    ] == ["plan_release_id", "serving_revision_id"]
    assert adapter_by_field["target_module"] == (
        "api.plan_pricing_em_distance_build"
    )
    assert adapter_by_field["target_function"] == (
        "build_plan_pricing_em_distance"
    )
    assert worker_payload_by_field["call_style"] == "kwargs"
    assert worker_payload_by_field["task"] == {
        "test_mode": False,
        **task_params_by_name,
    }
    worker = control_workers._BY_IMPORTER_ROLE[
        ("plan-pricing-em-distance", "start")
    ]
    assert worker.worker_class == "process.PTGCandidateAudit"
    assert control_workers._single_job_worker_target(
        worker,
        {"importer": "plan-pricing-em-distance", "run_id": "run_em_distance"},
    ) == "plan_pricing_em_distance_run_em_distance"


@pytest.mark.parametrize(
    "params_by_name",
    (
        {"plan_release_id": "hprelease_" + "2" * 26},
        {
            "plan_release_id": "hprelease_" + "2" * 26,
            "serving_revision_id": "hpserve_" + "3" * 26,
            "extra": "forbidden",
        },
        {
            "plan_release_id": " hprelease_" + "2" * 26,
            "serving_revision_id": "hpserve_" + "3" * 26,
        },
    ),
)
def test_em_distance_control_rejects_non_exact_params(params_by_name) -> None:
    with pytest.raises(ValueError, match="plan-pricing-em-distance params"):
        control_imports._validate_plan_pricing_em_distance_params(
            "plan-pricing-em-distance",
            params_by_name,
        )
