# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Process-signal boundaries for the projection-v3 census receipt."""

from __future__ import annotations

import json
from itertools import count
import os
from pathlib import Path
import signal
import subprocess
import sys
from types import SimpleNamespace

import pytest

from scripts.research import plan_pricing_projection_v3_census as census
from scripts.research import (
    plan_pricing_projection_v3_census_diagnostics as diagnostics,
)
from tests.test_plan_pricing_projection_v3_census_diagnostics import (
    _set_census_argv,
)

_RESTORE_RECEIPT_ENV = "HLTHPRT_TEST_CENSUS_RESTORE_RECEIPT"


async def _complete_census(_args, receipt_by_field):
    receipt_by_field.update(
        status="complete",
        accepted=True,
        cap_calibration_admissible=True,
        resource_proof_admissible=True,
    )
    return 0


def _install_signal_boundary_injections(
    monkeypatch,
    signal_point: str,
    signal_number: int,
):
    original_signal = diagnostics.signal.signal
    original_apply = diagnostics._CensusSignalState.apply_interruption
    original_utc_now = diagnostics.utc_now_text
    install_counts = count(1)
    apply_counts = count(1)
    utc_counts = count(1)

    def install_then_signal(number, handler):
        previous = original_signal(number, handler)
        if signal_point == "first_install" and next(install_counts) == 1:
            os.kill(os.getpid(), signal_number)
        return previous

    def apply_then_signal(state, exit_code):
        updated = original_apply(state, exit_code)
        if signal_point == "first_apply" and next(apply_counts) == 1:
            os.kill(os.getpid(), signal_number)
        return updated

    def utc_then_signal():
        if signal_point == "failure_seal" and next(utc_counts) == 1:
            os.kill(os.getpid(), signal_number)
        return original_utc_now()

    monkeypatch.setattr(diagnostics.signal, "signal", install_then_signal)
    monkeypatch.setattr(
        diagnostics._CensusSignalState,
        "apply_interruption",
        apply_then_signal,
    )
    monkeypatch.setattr(diagnostics, "utc_now_text", utc_then_signal)


def _run_after_sigterm_restore_child() -> None:
    receipt_path = Path(os.environ[_RESTORE_RECEIPT_ENV])
    original_signal = diagnostics.signal.signal
    handler_sets = []

    def signal_after_restore(number, handler):
        handler_sets.append((number, handler))
        previous_handler = original_signal(number, handler)
        if len(handler_sets) == 4:
            os.kill(os.getpid(), signal.SIGTERM)
        return previous_handler

    diagnostics.signal.signal = signal_after_restore
    receipt_by_field = {}
    diagnostics.run_census_process(
        SimpleNamespace(receipt=receipt_path),
        receipt_by_field,
        _complete_census,
        lambda _args: {},
    )


def test_sigterm_after_restore_leaves_only_provisional_receipt(tmp_path) -> None:
    """Default SIGTERM before final commit must leave no admissible receipt."""

    receipt_path = tmp_path / "restore-boundary.json"
    process = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "from tests.test_plan_pricing_projection_v3_census_process_signals "
                "import _run_after_sigterm_restore_child; "
                "_run_after_sigterm_restore_child()"
            ),
        ],
        env={**os.environ, _RESTORE_RECEIPT_ENV: str(receipt_path)},
        cwd=Path(__file__).resolve().parents[1],
        check=False,
        capture_output=True,
        text=True,
    )

    assert process.returncode == -signal.SIGTERM
    receipt_by_field = json.loads(receipt_path.read_text(encoding="utf-8"))
    assert receipt_by_field["status"] == "finalizing"
    assert receipt_by_field["accepted"] is False
    assert receipt_by_field["cap_calibration_admissible"] is False
    assert receipt_by_field["resource_proof_admissible"] is False


@pytest.mark.parametrize("signal_number", [signal.SIGINT, signal.SIGTERM])
def test_process_signal_cancels_then_seals_after_cleanup(
    monkeypatch,
    tmp_path,
    signal_number,
) -> None:
    receipt_path = tmp_path / f"signal-{signal_number}.json"
    _set_census_argv(monkeypatch, receipt_path)
    monkeypatch.setattr(census, "_source_identity", lambda _args: {})

    async def wait_for_signal(_args, receipt_by_field):
        census.asyncio.get_running_loop().call_soon(
            os.kill,
            os.getpid(),
            signal_number,
        )
        try:
            await census.asyncio.Event().wait()
        finally:
            os.kill(os.getpid(), signal_number)
            receipt_by_field["rollback_complete"] = True
            receipt_by_field["temporary_relations_after_rollback"] = []

    monkeypatch.setattr(census, "run_census", wait_for_signal)
    previous_handler = signal.getsignal(signal_number)

    assert census.census_main() == 128 + signal_number
    assert signal.getsignal(signal_number) == previous_handler
    receipt_by_field = json.loads(receipt_path.read_text(encoding="utf-8"))
    assert receipt_by_field["rollback_complete"] is True
    assert receipt_by_field["temporary_relations_after_rollback"] == []
    assert receipt_by_field["error"] == {
        "type": "_CensusInterrupted",
        "signal": signal.Signals(signal_number).name,
    }


@pytest.mark.parametrize(
    "signal_point",
    ["first_write", "second_apply", "first_restore", "after_restore"],
)
def test_process_late_signals_keep_failed_receipt(
    monkeypatch,
    tmp_path,
    signal_point,
) -> None:
    """Late first signals must replace any accepted final receipt."""

    receipt_path = tmp_path / "late-signal.json"
    _set_census_argv(monkeypatch, receipt_path)
    writes = []
    applies = []
    handler_sets = []
    original_apply = diagnostics._CensusSignalState.apply_interruption
    original_signal = diagnostics.signal.signal

    def signal_during_each_write(path, value):
        writes.append(json.loads(json.dumps(value)))
        if signal_point == "first_write" and len(writes) == 1:
            os.kill(os.getpid(), signal.SIGTERM)
        path.write_text(json.dumps(value), encoding="utf-8")

    def apply_then_signal(signal_state, exit_code):
        updated_exit_code = original_apply(signal_state, exit_code)
        applies.append(updated_exit_code)
        if signal_point == "second_apply" and len(applies) == 2:
            os.kill(os.getpid(), signal.SIGTERM)
        return updated_exit_code

    def signal_then_set_handler(number, handler):
        handler_sets.append((number, handler))
        if signal_point == "first_restore" and len(handler_sets) == 3:
            os.kill(os.getpid(), signal.SIGTERM)
        previous_handler = original_signal(number, handler)
        if signal_point == "after_restore" and len(handler_sets) == 3:
            os.kill(os.getpid(), signal.SIGINT)
        return previous_handler

    monkeypatch.setattr(census, "run_census", _complete_census)
    monkeypatch.setattr(diagnostics, "write_json", signal_during_each_write)
    monkeypatch.setattr(
        diagnostics._CensusSignalState,
        "apply_interruption",
        apply_then_signal,
    )
    monkeypatch.setattr(diagnostics.signal, "signal", signal_then_set_handler)

    expected_signal = (
        signal.SIGINT if signal_point == "after_restore" else signal.SIGTERM
    )
    assert census.census_main() == 128 + expected_signal
    assert len(writes) == 2
    final_receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    assert final_receipt["cap_calibration_admissible"] is False
    assert final_receipt["resource_proof_admissible"] is False
    assert final_receipt["error"] == {
        "type": "_CensusInterrupted",
        "signal": signal.Signals(expected_signal).name,
    }


@pytest.mark.parametrize(
    ("signal_point", "signal_number"),
    [
        ("first_install", signal.SIGINT),
        ("first_install", signal.SIGTERM),
        ("first_apply", signal.SIGINT),
        ("first_apply", signal.SIGTERM),
        ("failure_seal", signal.SIGINT),
        ("failure_seal", signal.SIGTERM),
    ],
)
def test_process_signals_cover_the_complete_owned_handler_lifetime(
    monkeypatch,
    tmp_path,
    signal_number,
    signal_point,
) -> None:
    """Every first signal under an owned handler must seal a failed receipt."""

    receipt_path = tmp_path / f"{signal_point}-{signal_number}.json"
    receipt_by_field = {
        "status": "complete",
        "accepted": True,
        "cap_calibration_admissible": True,
        "resource_proof_admissible": True,
    }

    async def runner(_args, _receipt_by_field):
        if signal_point == "failure_seal":
            raise RuntimeError("private failure")
        return await _complete_census(_args, _receipt_by_field)

    _install_signal_boundary_injections(monkeypatch, signal_point, signal_number)
    previous_handler_by_signal = {
        number: signal.getsignal(number) for number in (signal.SIGINT, signal.SIGTERM)
    }

    assert (
        diagnostics.run_census_process(
            SimpleNamespace(receipt=receipt_path),
            receipt_by_field,
            runner,
            lambda _args: {},
        )
        == 128 + signal_number
    )
    assert all(
        signal.getsignal(number) == previous_handler
        for number, previous_handler in previous_handler_by_signal.items()
    )
    final_receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    assert final_receipt["status"] == "failed"
    assert final_receipt["accepted"] is False
    assert final_receipt["cap_calibration_admissible"] is False
    assert final_receipt["resource_proof_admissible"] is False
