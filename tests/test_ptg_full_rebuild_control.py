# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio
import importlib
from unittest.mock import AsyncMock

import pytest
from click.testing import CliRunner

from process import ptg_control
from process.control_cancel import ImportCancelledError


process_pkg = importlib.import_module("process")
_REBUILD_TOKEN = "123e4567-e89b-42d3-a456-426614174000"
_INTERNAL_SCOPE_DIGEST = "ab" * 32
_PROOF_METRICS_BY_NAME = {
    "full_rebuild_requested": True,
    "raw_artifact_reuse_forced_off": True,
    "partial_artifact_retention_forced_off": True,
}


async def _allow_active_run(_run_id):
    return None


@pytest.fixture
def control_observations(monkeypatch):
    ptg_calls = []
    run_marks = []

    async def fake_ptg_main(**kwargs):
        ptg_calls.append(kwargs)
        return {"snapshot_id": "ptg2:test"}

    async def fake_mark_control_run(*args, **kwargs):
        run_marks.append((args, kwargs))

    async def fake_flush_terminal_status_events():
        return None

    monkeypatch.setattr(ptg_control, "ptg_main", fake_ptg_main)
    monkeypatch.setattr(ptg_control, "mark_control_run", fake_mark_control_run)
    monkeypatch.setattr(
        ptg_control,
        "_flush_terminal_status_events",
        fake_flush_terminal_status_events,
    )
    monkeypatch.setattr(ptg_control, "_stale_ptg_job_result", _allow_active_run)
    return ptg_calls, run_marks


def _full_rebuild_task():
    return {
        "run_id": "run_ptg",
        "params": {
            "test_mode": True,
            "source_key": "test_source",
            "reuse_raw_artifacts": True,
            "keep_partial_artifacts": True,
            "_full_rebuild_scope_digest": _INTERNAL_SCOPE_DIGEST,
        },
    }


def _expected_ptg_call(scope_digest):
    return {
        "test_mode": True,
        "toc_urls": None,
        "toc_list": None,
        "in_network_url": None,
        "allowed_url": None,
        "provider_ref_url": None,
        "import_id": None,
        "source_key": "test_source",
        "import_month": None,
        "max_files": None,
        "max_items": None,
        "plan_ids": None,
        "plan_name_contains": None,
        "plan_market_types": None,
        "file_url_contains": None,
        "source_network_names": None,
        "reuse_raw_artifacts": False,
        "keep_partial_artifacts": False,
        "control_run_id": "run_ptg",
        "full_rebuild_scope_digest": scope_digest,
    }


@pytest.mark.asyncio
async def test_ptg_control_preserves_default_call_surface(control_observations):
    """Keep the ordinary import call and result free of rebuild-only fields."""

    ptg_calls, _run_marks = control_observations
    control_response = await ptg_control.ptg_control_start(
        {},
        {"run_id": "run_ptg", "params": {"source_key": "test_source"}},
    )

    assert ptg_calls[0]["reuse_raw_artifacts"] is True
    assert ptg_calls[0]["keep_partial_artifacts"] is None
    assert "full_rebuild_scope_digest" not in ptg_calls[0]
    assert not (_PROOF_METRICS_BY_NAME.keys() & control_response.keys())


@pytest.mark.asyncio
async def test_ordinary_failure_ignores_attached_rebuild_proof(
    monkeypatch,
    control_observations,
):
    """Keep runtime rebuild proof off the ordinary import failure path."""

    _ptg_calls, run_marks = control_observations

    async def fail_ptg_main(**_kwargs):
        error = RuntimeError("ordinary test import failed")
        error.ptg_full_rebuild_metrics_by_name = {
            "full_rebuild": True,
            "raw_artifacts_total": 4,
        }
        raise error

    monkeypatch.setattr(ptg_control, "ptg_main", fail_ptg_main)
    with pytest.raises(RuntimeError, match="ordinary test import failed"):
        await ptg_control.ptg_control_start(
            {},
            {"run_id": "run_ptg", "params": {"source_key": "test_source"}},
        )

    failed_mark_by_name = run_marks[-1][1]
    assert failed_mark_by_name["status"] == "failed"
    assert "metrics" not in failed_mark_by_name
    assert failed_mark_by_name["error"] == {
        "code": "ptg_import_failed",
        "message": "ordinary test import failed",
    }


@pytest.mark.asyncio
async def test_ptg_control_applies_private_full_rebuild_scope(control_observations):
    """Force fresh artifact policy while exposing only safe rebuild proof fields."""

    ptg_calls, run_marks = control_observations
    control_response = await ptg_control.ptg_control_start(
        {},
        _full_rebuild_task(),
    )
    expected_metrics_by_name = {
        "snapshot_id": "ptg2:test",
        **_PROOF_METRICS_BY_NAME,
    }

    assert ptg_calls == [_expected_ptg_call(_INTERNAL_SCOPE_DIGEST)]
    assert run_marks[-1][1]["metrics"] == expected_metrics_by_name
    assert control_response == {
        **expected_metrics_by_name,
        "status": "succeeded",
        "run_id": "run_ptg",
    }
    assert _REBUILD_TOKEN not in repr(control_response)
    assert _REBUILD_TOKEN not in repr(run_marks)


@pytest.mark.asyncio
async def test_failed_full_rebuild_records_only_safe_proof_metrics(
    monkeypatch,
    control_observations,
):
    """Retain rebuild policy proof on failure without recording its scope."""

    ptg_calls, run_marks = control_observations
    sensitive_url = "https://private.test/failed-artifact.json"

    async def fail_ptg_main(**_kwargs):
        error = RuntimeError(
            f"test import failed for {sensitive_url} in {_INTERNAL_SCOPE_DIGEST}"
        )
        error.ptg_full_rebuild_metrics_by_name = {
            "full_rebuild": True,
            "artifacts_observed": 3,
            "raw_artifacts_total": 3,
            "raw_artifacts_reused": 0,
            "unsafe_url": sensitive_url,
        }
        raise error

    monkeypatch.setattr(ptg_control, "ptg_main", fail_ptg_main)
    with pytest.raises(RuntimeError, match="test import failed"):
        await ptg_control.ptg_control_start({}, _full_rebuild_task())

    failed_mark_by_name = run_marks[-1][1]
    assert failed_mark_by_name["status"] == "failed"
    assert failed_mark_by_name["error"] == {
        "code": "ptg_full_rebuild_failed",
        "message": "controlled PTG full rebuild failed",
    }
    assert failed_mark_by_name["metrics"] == {
        "full_rebuild": True,
        "artifacts_observed": 3,
        "raw_artifacts_total": 3,
        "raw_artifacts_reused": 0,
        **_PROOF_METRICS_BY_NAME,
    }
    assert _REBUILD_TOKEN not in repr(failed_mark_by_name)
    assert _INTERNAL_SCOPE_DIGEST not in repr(failed_mark_by_name)
    assert sensitive_url not in repr(failed_mark_by_name)
    assert ptg_calls == []


@pytest.mark.asyncio
async def test_reused_full_rebuild_work_records_a_safe_freshness_failure(
    monkeypatch,
    control_observations,
):
    """Persist reusable proof without leaking freshness-error internals."""

    _ptg_calls, run_marks = control_observations
    sensitive_url = "https://private.test/internal-artifact.json"

    async def reject_reused_work(**_kwargs):
        error = ptg_control.PTG2FullRebuildFreshnessError(
            f"reused {sensitive_url} in {_INTERNAL_SCOPE_DIGEST}",
            {
                "full_rebuild": True,
                "raw_artifacts_total": 2,
                "raw_artifacts_reused": 1,
                "raw_artifacts_duplicate_identities": 1,
                "logical_artifacts_duplicate_identities": 1,
                "logical_artifacts_deferred_hashes": 2,
                "shared_layout_reused": True,
                "unsafe_url": sensitive_url,
            },
        )
        error.ptg_full_rebuild_metrics_by_name = {
            "artifacts_observed": 3,
            "raw_artifacts_total": 3,
            "logical_artifacts_total": 2,
            "unsafe_url": sensitive_url,
        }
        raise error

    monkeypatch.setattr(ptg_control, "ptg_main", reject_reused_work)
    with pytest.raises(ptg_control.PTG2FullRebuildFreshnessError):
        await ptg_control.ptg_control_start({}, _full_rebuild_task())

    failed_mark_by_name = run_marks[-1][1]
    assert failed_mark_by_name["status"] == "failed"
    assert failed_mark_by_name["error"] == {
        "code": "ptg_full_rebuild_reuse_detected",
        "message": "controlled PTG full rebuild reused prior work",
    }
    assert failed_mark_by_name["metrics"] == {
        "full_rebuild": True,
        "artifacts_observed": 3,
        "raw_artifacts_total": 3,
        "raw_artifacts_reused": 1,
        "raw_artifacts_duplicate_identities": 1,
        "logical_artifacts_total": 2,
        "logical_artifacts_duplicate_identities": 1,
        "logical_artifacts_deferred_hashes": 2,
        "shared_layout_reused": True,
        **_PROOF_METRICS_BY_NAME,
    }
    assert sensitive_url not in repr(failed_mark_by_name)
    assert _INTERNAL_SCOPE_DIGEST not in repr(failed_mark_by_name)


@pytest.mark.asyncio
async def test_canceled_full_rebuild_records_only_safe_proof_metrics(
    monkeypatch,
    control_observations,
):
    """Retain rebuild policy proof when cancellation wins before import work."""

    ptg_calls, run_marks = control_observations
    sensitive_url = "https://private.test/canceled-artifact.json"

    async def cancel_before_import(*_args, **_kwargs):
        error = ImportCancelledError("test import canceled")
        error.ptg_full_rebuild_metrics_by_name = {
            "full_rebuild": True,
            "artifacts_observed": 2,
            "raw_artifacts_total": 2,
            "unsafe_url": sensitive_url,
        }
        raise error

    monkeypatch.setattr(ptg_control, "raise_if_cancelled", cancel_before_import)
    control_response = await ptg_control.ptg_control_start(
        {},
        _full_rebuild_task(),
    )

    canceled_mark_by_name = run_marks[-1][1]
    assert control_response == {"status": "canceled", "run_id": "run_ptg"}
    assert canceled_mark_by_name["status"] == "canceled"
    assert canceled_mark_by_name["metrics"] == {
        "full_rebuild": True,
        "artifacts_observed": 2,
        "raw_artifacts_total": 2,
        **_PROOF_METRICS_BY_NAME,
    }
    assert _REBUILD_TOKEN not in repr(canceled_mark_by_name)
    assert _INTERNAL_SCOPE_DIGEST not in repr(canceled_mark_by_name)
    assert sensitive_url not in repr(canceled_mark_by_name)
    assert ptg_calls == []


@pytest.mark.asyncio
async def test_interrupted_full_rebuild_records_safe_runtime_proof(
    monkeypatch,
    control_observations,
):
    """Retain observed counts when the worker task itself is canceled."""

    _ptg_calls, run_marks = control_observations

    async def interrupt_ptg_main(**_kwargs):
        error = asyncio.CancelledError()
        error.ptg_full_rebuild_metrics_by_name = {
            "full_rebuild": True,
            "artifacts_observed": 1,
            "logical_artifacts_total": 1,
        }
        raise error

    monkeypatch.setattr(ptg_control, "ptg_main", interrupt_ptg_main)
    with pytest.raises(asyncio.CancelledError):
        await ptg_control.ptg_control_start({}, _full_rebuild_task())

    interrupted_mark_by_name = run_marks[-1][1]
    assert interrupted_mark_by_name["status"] == "failed"
    assert interrupted_mark_by_name["error"]["code"] == "import_interrupted"
    assert interrupted_mark_by_name["metrics"] == {
        "full_rebuild": True,
        "artifacts_observed": 1,
        "logical_artifacts_total": 1,
        **_PROOF_METRICS_BY_NAME,
    }
    assert _INTERNAL_SCOPE_DIGEST not in repr(interrupted_mark_by_name)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("untrusted_params_by_name", "expected_message"),
    [
        (
            {"_full_rebuild_token": _REBUILD_TOKEN},
            "PTG workers accept only an internal full rebuild scope",
        ),
        (
            {"_full_rebuild_scope_digest": None},
            "private PTG full rebuild scope digest is invalid",
        ),
        (
            {"_full_rebuild_scope_digest": ""},
            "private PTG full rebuild scope digest is invalid",
        ),
        (
            {"_full_rebuild_scope_digest": "AB" * 32},
            "private PTG full rebuild scope digest is invalid",
        ),
        (
            {"_full_rebuild_scope_digest": "g" * 64},
            "private PTG full rebuild scope digest is invalid",
        ),
        (
            {"_full_rebuild_scope_digest": 123},
            "private PTG full rebuild scope digest is invalid",
        ),
    ],
)
async def test_ptg_control_rejects_untrusted_rebuild_control(
    control_observations,
    untrusted_params_by_name,
    expected_message,
):
    """Accept only a lowercase internal digest at the worker boundary."""

    ptg_calls, run_marks = control_observations
    with pytest.raises(
        ValueError,
        match=expected_message,
    ) as exc_info:
        await ptg_control.ptg_control_start(
            {},
            {
                "run_id": "run_ptg",
                "params": untrusted_params_by_name,
            },
        )

    assert ptg_calls == []
    assert [kwargs["status"] for _args, kwargs in run_marks] == [
        "running",
        "failed",
    ]
    assert run_marks[-1][1]["error"] == {
        "code": "ptg_import_failed",
        "message": expected_message,
    }
    assert _REBUILD_TOKEN not in str(exc_info.value)
    assert _REBUILD_TOKEN not in repr(run_marks)


def test_ptg_cli_advertises_hidden_full_rebuild_capability():
    """Advertise the private schema field without displaying it in CLI help."""

    from api.control_imports import importer_registry

    full_rebuild_option = next(
        parameter
        for parameter in process_pkg.ptg.params
        if parameter.name == "_full_rebuild_token"
    )
    assert full_rebuild_option.opts == ["--_full-rebuild-token"]
    assert full_rebuild_option.hidden is True
    assert full_rebuild_option.type.name == "text"
    help_response = CliRunner().invoke(process_pkg.ptg, ["--help"])
    assert help_response.exit_code == 0
    assert "--_full-rebuild-token" not in help_response.output

    ptg_registry_entry = next(
        entry for entry in importer_registry() if entry["name"] == "ptg"
    )
    advertised_parameter = next(
        parameter
        for parameter in ptg_registry_entry["params_schema"]
        if parameter["name"] == "_full_rebuild_token"
    )
    assert advertised_parameter["opts"] == ["--_full-rebuild-token"]
    assert advertised_parameter["type"] == "text"
    assert advertised_parameter["default"] is None


def test_ptg_cli_rejects_direct_full_rebuild_token(monkeypatch):
    """Keep the private rebuild capability unusable from the local CLI."""

    fake_initiate = AsyncMock()
    monkeypatch.setattr(process_pkg, "initiate_ptg", fake_initiate)

    cli_response = CliRunner().invoke(
        process_pkg.ptg,
        ["--_full-rebuild-token", _REBUILD_TOKEN],
    )

    assert cli_response.exit_code == 2
    assert (
        "controlled PTG rebuilds must be requested through import control"
        in cli_response.output
    )
    assert _REBUILD_TOKEN not in cli_response.output
    fake_initiate.assert_not_called()
