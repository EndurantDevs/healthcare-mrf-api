# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""S5 (review round 2): a canonical-address resolve failure inside the
per-state loop must mark the WHOLE pharmacy-license run failed — both in
`_upsert_run` and `mark_control_run` — and re-raise, never falling through to
the completed/succeeded path (the original R11 swallow)."""

import importlib

import pytest

pharmacy_license = importlib.import_module("process.pharmacy_license")


class _Recorder:
    def __init__(self):
        self.calls = []

    async def __call__(self, *args, **kwargs):
        self.calls.append((args, kwargs))


async def _successful_state_import(*_args, **_kwargs):
    """Return one deterministic successful state-file result."""
    return pharmacy_license.StateImportStats(
        supported=True,
        status="imported",
        source_url="https://example.test/source.zip",
        unsupported_reason=None,
        error_text=None,
        row_count_parsed=5,
        row_count_matched=5,
        row_count_dropped=0,
        row_count_inserted=0,
        metadata={},
    )


async def _fail_license_materialization(*_args, **_kwargs):
    raise pharmacy_license.PharmacyLicenseCanonicalAddressError(
        "pharmacy_license canonical address resolve failed"
    )


async def _async_noop(*_args, **_kwargs):
    return None


def _install_failing_license_run(monkeypatch, status_by_name):
    """Install a complete run whose canonical materialization fails."""
    async def fake_upsert_run(payload):
        status_by_name["run"].append(
            (payload.get("status"), payload.get("error_text"))
        )

    async def fake_mark_control_run(run_id, **kwargs):
        status_by_name["control"].append(kwargs.get("status"))

    async def fake_upsert_snapshot(payload):
        status_by_name["snapshot"].append(payload.get("status"))

    async def fake_ensure_tables():
        return "mrf"

    async def fake_download_it(*args, **kwargs):
        return "<html></html>"

    replacement_map = {
        "_upsert_run": fake_upsert_run,
        "mark_control_run": fake_mark_control_run,
        "_upsert_snapshot": fake_upsert_snapshot,
        "_upsert_coverage": _Recorder(),
        "_import_state_source": _successful_state_import,
        "_materialize_snapshot": _fail_license_materialization,
        "ensure_database": _async_noop,
        "_ensure_tables": fake_ensure_tables,
        "_truncate_stage_table": _async_noop,
        "_drop_secondary_indexes": _async_noop,
        "_ensure_secondary_indexes": _async_noop,
        "_analyze_tables": _async_noop,
        "download_it": fake_download_it,
        "_parse_fda_state_sources": lambda _html: [
            pharmacy_license.StateSource(
                state_code="ZZ",
                state_name="Teststate",
                board_url="https://example.test/board",
            )
        ],
        "enqueue_live_progress": lambda *_args, **_kwargs: None,
    }
    for name, replacement in replacement_map.items():
        monkeypatch.setattr(pharmacy_license, name, replacement)


async def test_canonical_resolve_failure_marks_run_failed(monkeypatch):
    """Verify canonical resolve failure marks run failed."""
    status_by_name = {"run": [], "control": [], "snapshot": []}
    _install_failing_license_run(monkeypatch, status_by_name)

    with pytest.raises(pharmacy_license.PharmacyLicenseCanonicalAddressError):
        await pharmacy_license.pharmacy_license_start(
            None, task={"run_id": "run-test-failure", "test_mode": True}
        )

    # The run must be recorded as failed in both registries, with the typed
    # error preserved, and must never reach the completed/succeeded path.
    run_statuses = status_by_name["run"]
    assert ("failed" in [status for status, _ in run_statuses]), run_statuses
    assert "completed" not in [status for status, _ in run_statuses], run_statuses
    failed_errors = [err for status, err in run_statuses if status == "failed"]
    assert any(
        err and "canonical address resolve failed" in err for err in failed_errors
    ), run_statuses
    assert "failed" in status_by_name["control"], status_by_name["control"]
    assert "succeeded" not in status_by_name["control"], status_by_name["control"]
    # The state's snapshot is individually marked failed as well.
    assert "failed" in status_by_name["snapshot"], status_by_name["snapshot"]
