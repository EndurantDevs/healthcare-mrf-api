# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import pytest

from process import control_lifecycle
from process.control_lifecycle import control_single_job_start
from process.ptg_control_failures import ptg_failure_error
from process.ptg_parts.ptg2_shared_audit import ReusableLayoutAuditCorruption


@pytest.mark.parametrize(
    "message",
    (
        "reused strict V3 layout is missing its manifest",
        "reused strict V3 layout is missing its audit sample contract",
        "reused shared PTG layout audit rows disagree with its manifest",
        "strict V3 audit snapshot source dictionary is not complete and dense",
    ),
)
def test_ptg_control_classifies_verified_reusable_layout_corruption(message):
    assert ptg_failure_error(ReusableLayoutAuditCorruption(message)) == {
        "code": "ptg_reusable_layout_audit_corrupt",
        "message": message,
        "retryable": False,
    }


def test_ptg_control_does_not_classify_untyped_runtime_errors_as_layout_corruption():
    message = "reused shared PTG layout audit rows disagree with its manifest"

    assert ptg_failure_error(RuntimeError(message)) == {
        "code": "ptg_import_failed",
        "message": message,
    }


@pytest.mark.asyncio
async def test_control_job_persists_failure_audit_metrics(monkeypatch):
    run_marks = []

    async def is_control_run_marked(run_id, **kwargs):
        run_marks.append((run_id, kwargs))
        return True

    async def fake_target(ctx, _task):
        ctx.setdefault("context", {})["audit"] = {
            "provider_directory_retry_not_before": "2026-07-12T00:01:00Z",
            "pagination_resume_required": ["pdfhir_molina:Location"],
        }
        raise TimeoutError

    class FakeModule:
        process_data = staticmethod(fake_target)

    monkeypatch.setattr(
        control_lifecycle,
        "mark_control_run",
        is_control_run_marked,
    )
    monkeypatch.setattr(control_lifecycle, "import_module", lambda _name: FakeModule)

    with pytest.raises(TimeoutError):
        await control_single_job_start(
            {},
            {
                "run_id": "run_molina",
                "target_module": "fake.module",
                "target_function": "process_data",
            },
        )

    assert [run_mark[1]["status"] for run_mark in run_marks] == ["running", "failed"]
    assert run_marks[-1][1]["error"] == {
        "code": "import_failed",
        "message": "TimeoutError",
    }
    assert run_marks[-1][1]["metrics"] == {
        "audit": {
            "provider_directory_retry_not_before": "2026-07-12T00:01:00Z",
            "pagination_resume_required": ["pdfhir_molina:Location"],
        }
    }


@pytest.mark.asyncio
async def test_control_job_preserves_importer_retry_contract(monkeypatch):
    run_marks = []

    class DeterministicFailure(RuntimeError):
        control_error_code = "deterministic_failure"
        retryable = False

    async def is_control_run_marked(run_id, **kwargs):
        run_marks.append((run_id, kwargs))
        return True

    async def fake_target(_ctx, _task):
        raise DeterministicFailure("evidence mismatch")

    class FakeModule:
        process_data = staticmethod(fake_target)

    monkeypatch.setattr(
        control_lifecycle,
        "mark_control_run",
        is_control_run_marked,
    )
    monkeypatch.setattr(control_lifecycle, "import_module", lambda _name: FakeModule)

    with pytest.raises(DeterministicFailure):
        await control_single_job_start(
            {},
            {
                "run_id": "run_deterministic_failure",
                "target_module": "fake.module",
                "target_function": "process_data",
            },
        )

    assert run_marks[-1][1]["error"] == {
        "code": "deterministic_failure",
        "message": "evidence mismatch",
        "retryable": False,
    }


@pytest.mark.asyncio
async def test_control_job_persists_hospital_failure_metrics(monkeypatch):
    run_marks = []

    async def is_control_run_marked(run_id, **kwargs):
        run_marks.append((run_id, kwargs))
        return True

    async def fake_target(ctx, _task):
        ctx.setdefault("context", {})["hospital_price_metrics"] = {
            "selected": 2,
            "published": 0,
            "failed": 2,
        }
        raise RuntimeError("nothing published")

    class FakeModule:
        process_data = staticmethod(fake_target)

    monkeypatch.setattr(control_lifecycle, "mark_control_run", is_control_run_marked)
    monkeypatch.setattr(control_lifecycle, "import_module", lambda _name: FakeModule)

    with pytest.raises(RuntimeError, match="nothing published"):
        await control_single_job_start(
            {},
            {
                "run_id": "run_hospital_failure",
                "target_module": "fake.module",
                "target_function": "process_data",
            },
        )

    assert run_marks[-1][1]["metrics"] == {
        "hospital_price_metrics": {
            "selected": 2,
            "published": 0,
            "failed": 2,
        }
    }
