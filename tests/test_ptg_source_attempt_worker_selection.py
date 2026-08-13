"""Worker selection and response identity contracts."""

import pytest

from api import control_workers
from process.ptg_parts import ptg_source_attempt_actions as actions


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


@pytest.mark.parametrize(
    ("total_chunks", "accepted"),
    ((5, True), (0, True), (None, False), (False, False), (-1, False)),
)
def test_running_chunked_import_selects_finish_worker(total_chunks, accepted):
    outer_run_by_field = {"importer": "provider-quality", "status": "running"}
    if total_chunks is not None:
        outer_run_by_field["metrics"] = {"total_chunks": total_chunks}
    selection = actions.PTGWorkerActionSelection(
        request_importer="provider-quality",
        allowed_importers=frozenset({"provider-quality"}),
        allowed_roles=frozenset({"finish"}),
    )
    if accepted:
        actions._validate_worker_selection(outer_run_by_field, selection)
    else:
        with pytest.raises(actions.PTGSourceAttemptIdentityError):
            actions._validate_worker_selection(outer_run_by_field, selection)
