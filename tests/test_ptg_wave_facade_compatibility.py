"""Compatibility hooks retained by the split exact-wave facades."""

from __future__ import annotations

import types
from unittest.mock import AsyncMock, Mock

import pytest

import process.ptg_wave_controller as controller
import process.ptg_wave_failure as failure


@pytest.mark.asyncio
async def test_reconcile_uses_legacy_preclaim_and_terminal_hooks(monkeypatch):
    wave = types.SimpleNamespace(state="executing", wave_id="wave-unit")
    bundle = controller.PTGWaveBundle(wave=wave, intents=())
    monkeypatch.setattr(
        controller, "load_capacity_owning_wave", AsyncMock(return_value=bundle)
    )
    monkeypatch.setattr(controller, "read_only_recovery_plan", Mock(return_value=None))
    monkeypatch.setattr(controller, "restore_wave_manifest", Mock(return_value=object()))
    preclaim_hook = AsyncMock(return_value=False)
    terminal_hook = AsyncMock(return_value=False)
    monkeypatch.setattr(controller, "_maybe_snapshot_preclaim_failure", preclaim_hook)
    monkeypatch.setattr(controller, "_all_wave_runs_terminal", terminal_hook)

    reconciliation_result = await controller.reconcile_ptg_wave_once(
        object(), image="unused", runtime_image="unused"
    )

    assert reconciliation_result == "executing"
    preclaim_hook.assert_awaited_once()
    terminal_hook.assert_awaited_once_with(bundle)


@pytest.mark.asyncio
async def test_terminalizing_uses_legacy_early_stop_hook(monkeypatch):
    wave = types.SimpleNamespace(
        state="terminalizing",
        wave_id="wave-unit",
        kubernetes_delete_evidence_digest=None,
    )
    bundle = controller.PTGWaveBundle(wave=wave, intents=())
    monkeypatch.setattr(
        controller, "load_capacity_owning_wave", AsyncMock(return_value=bundle)
    )
    monkeypatch.setattr(controller, "read_only_recovery_plan", Mock(return_value=None))
    monkeypatch.setattr(controller, "restore_wave_manifest", Mock(return_value=object()))
    early_stop_hook = Mock(return_value=False)
    terminal_proof = AsyncMock()
    monkeypatch.setattr(controller, "_requires_early_kubernetes_stop", early_stop_hook)
    monkeypatch.setattr(controller, "_persist_terminal_proof", terminal_proof)

    reconciliation_result = await controller.reconcile_ptg_wave_once(
        object(), image="unused", runtime_image="unused"
    )

    assert reconciliation_result == "terminal-proof-persisted"
    early_stop_hook.assert_called_once_with(wave)
    terminal_proof.assert_awaited_once()


@pytest.mark.asyncio
async def test_start_uses_exported_controller_enabled_hook(monkeypatch):
    enabled_hook = Mock(return_value=False)
    monkeypatch.setattr(controller, "controller_enabled", enabled_hook)
    monkeypatch.setattr(
        controller,
        "is_controller_enabled",
        Mock(side_effect=AssertionError("private implementation bypassed hook")),
    )

    await controller.start_ptg_wave_controller(
        types.SimpleNamespace(ctx=types.SimpleNamespace())
    )

    enabled_hook.assert_called_once_with()


def test_failure_facade_retains_pre_split_private_names():
    assert failure._outcome_digest is failure._single_outcome_digest
    assert failure._prestart_run_is_pristine is failure._is_prestart_run_pristine
