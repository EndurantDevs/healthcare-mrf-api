# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Runtime attestation for explicit reviewed-root policy manifests."""

from __future__ import annotations

from process import provider_directory_fhir_subset_activation_contract as contract
from process.provider_directory_fhir_root_policy import (
    POLICY_PENDING_STATUS,
    ReviewedRootPolicy,
)
from scripts.smoke import provider_directory_runtime_contract as runtime_contract


def test_runtime_contract_accepts_policy_pending_manifest(monkeypatch):
    manifest = contract.ReviewedSubsetActivationManifest(
        desired_candidate_status=POLICY_PENDING_STATUS,
        evidence=None,
        root_policy=ReviewedRootPolicy(1),
    )
    monkeypatch.setattr(
        contract,
        "reviewed_subset_activation_manifest",
        lambda _path: manifest,
    )
    monkeypatch.setattr(
        runtime_contract,
        "_disabled_reviewed_subset_operation_report",
        lambda _command, _environment: {
            "exit_code": 1,
            "output_matches": True,
        },
    )

    report = runtime_contract._reviewed_subset_state_sync_report()

    assert report["ok"] is True
    assert report["desired_state"] == POLICY_PENDING_STATUS
    assert report["evidence_present"] is False


def test_terminal_root_retirement_operator_is_default_off(monkeypatch) -> None:
    from process.provider_directory_terminal_root_retirement_contract import (
        RETIREMENT_ENABLED_ENV,
    )

    monkeypatch.delenv(RETIREMENT_ENABLED_ENV, raising=False)

    report = runtime_contract._terminal_root_retirement_gate_report()

    assert report == {
        "default_off": True,
        "ok": True,
        "stderr_matches": True,
        "stdout_empty": True,
    }
