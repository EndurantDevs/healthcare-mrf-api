# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import pytest

from process import control_lifecycle
from process.control_lifecycle import control_single_job_start


@pytest.mark.asyncio
async def test_control_job_persists_failure_audit_metrics(monkeypatch):
    run_marks = []

    async def fake_mark(run_id, **kwargs):
        run_marks.append((run_id, kwargs))

    async def fake_target(ctx, _task):
        ctx.setdefault("context", {})["audit"] = {
            "provider_directory_retry_not_before": "2026-07-12T00:01:00Z",
            "pagination_resume_required": ["pdfhir_molina:Location"],
        }
        raise RuntimeError("provider_directory_pagination_resume_required")

    class FakeModule:
        process_data = staticmethod(fake_target)

    monkeypatch.setattr(control_lifecycle, "mark_control_run", fake_mark)
    monkeypatch.setattr(control_lifecycle, "import_module", lambda _name: FakeModule)

    with pytest.raises(
        RuntimeError,
        match="provider_directory_pagination_resume_required",
    ):
        await control_single_job_start(
            {},
            {
                "run_id": "run_molina",
                "target_module": "fake.module",
                "target_function": "process_data",
            },
        )

    assert [run_mark[1]["status"] for run_mark in run_marks] == ["running", "failed"]
    assert run_marks[-1][1]["metrics"] == {
        "audit": {
            "provider_directory_retry_not_before": "2026-07-12T00:01:00Z",
            "pagination_resume_required": ["pdfhir_molina:Location"],
        }
    }
