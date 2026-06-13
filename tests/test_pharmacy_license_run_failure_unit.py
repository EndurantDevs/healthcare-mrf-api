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


async def test_canonical_resolve_failure_marks_run_failed(monkeypatch):
    run_statuses = []
    control_statuses = []
    snapshot_statuses = []

    async def fake_upsert_run(payload):
        run_statuses.append((payload.get("status"), payload.get("error_text")))

    async def fake_mark_control_run(run_id, **kwargs):
        control_statuses.append(kwargs.get("status"))

    async def fake_upsert_snapshot(payload):
        snapshot_statuses.append(payload.get("status"))

    async def fake_import_state_source(session, source, **kwargs):
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

    async def fake_materialize_snapshot(schema, snapshot_id, run_id):
        raise pharmacy_license.PharmacyLicenseCanonicalAddressError(
            "pharmacy_license canonical address resolve failed"
        )

    async def fake_ensure_tables():
        return "mrf"

    async def fake_async_noop(*args, **kwargs):
        return None

    async def fake_download_it(*args, **kwargs):
        return "<html></html>"

    monkeypatch.setattr(pharmacy_license, "_upsert_run", fake_upsert_run)
    monkeypatch.setattr(pharmacy_license, "mark_control_run", fake_mark_control_run)
    monkeypatch.setattr(pharmacy_license, "_upsert_snapshot", fake_upsert_snapshot)
    monkeypatch.setattr(pharmacy_license, "_upsert_coverage", _Recorder())
    monkeypatch.setattr(pharmacy_license, "_import_state_source", fake_import_state_source)
    monkeypatch.setattr(pharmacy_license, "_materialize_snapshot", fake_materialize_snapshot)
    monkeypatch.setattr(pharmacy_license, "ensure_database", fake_async_noop)
    monkeypatch.setattr(pharmacy_license, "_ensure_tables", fake_ensure_tables)
    monkeypatch.setattr(pharmacy_license, "_truncate_stage_table", fake_async_noop)
    monkeypatch.setattr(pharmacy_license, "_drop_secondary_indexes", fake_async_noop)
    monkeypatch.setattr(pharmacy_license, "_ensure_secondary_indexes", fake_async_noop)
    monkeypatch.setattr(pharmacy_license, "_analyze_tables", fake_async_noop)
    monkeypatch.setattr(pharmacy_license, "download_it", fake_download_it)
    monkeypatch.setattr(
        pharmacy_license,
        "_parse_fda_state_sources",
        lambda html: [
            pharmacy_license.StateSource(
                state_code="ZZ",
                state_name="Teststate",
                board_url="https://example.test/board",
            )
        ],
    )
    monkeypatch.setattr(
        pharmacy_license, "enqueue_live_progress", lambda *args, **kwargs: None
    )

    with pytest.raises(pharmacy_license.PharmacyLicenseCanonicalAddressError):
        await pharmacy_license.pharmacy_license_start(
            None, task={"run_id": "run-test-failure", "test_mode": True}
        )

    # The run must be recorded as failed in both registries, with the typed
    # error preserved, and must never reach the completed/succeeded path.
    assert ("failed" in [status for status, _ in run_statuses]), run_statuses
    assert "completed" not in [status for status, _ in run_statuses], run_statuses
    failed_errors = [err for status, err in run_statuses if status == "failed"]
    assert any(
        err and "canonical address resolve failed" in err for err in failed_errors
    ), run_statuses
    assert "failed" in control_statuses, control_statuses
    assert "succeeded" not in control_statuses, control_statuses
    # The state's snapshot is individually marked failed as well.
    assert "failed" in snapshot_statuses, snapshot_statuses
