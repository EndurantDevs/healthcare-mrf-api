"""Worker selection and response identity contracts."""

from api import control_workers


RUN_ID = "synthetic-run-coverage"


def test_worker_selection_and_response_preserve_run_identity():
    selection = control_workers._worker_action_selection(
        "",
        [
            control_workers._BY_QUEUE["arq:ClaimsPricing"],
            control_workers._BY_QUEUE["arq:ClaimsPricing_finish"],
        ],
    )
    assert selection.request_importer is None
    assert selection.allowed_importers == frozenset(
        {"claims-pricing", "claims-procedures"}
    )
    assert selection.allowed_roles == frozenset({"start", "finish"})
    response = control_workers._failed_worker_admission(
        {"run_id": RUN_ID},
        "synthetic failure",
    )
    assert (
        response["contract_id"]
        == control_workers.WORKER_ENSURE_RUN_IDENTITY_CONTRACT
    )
    assert response["run_id"] == RUN_ID
    assert control_workers._worker_ensure_response(
        {}, status="inactive", items=[]
    )["status"] == "inactive"
