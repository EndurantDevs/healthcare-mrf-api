

"""Fail-closed edge contracts for the exact twelve-slot Kubernetes wave."""


from __future__ import annotations


import copy


from dataclasses import replace


import pytest


from api import ptg_wave_kubernetes as kubernetes


from api import ptg_wave_kubernetes_attestation as live


from api import ptg_wave_kubernetes_failure_attestation as failure


from api import ptg_wave_kubernetes_terminal_attestation as terminal


from api.ptg_wave_kubernetes_receipt_attestation import (
    attest_ptg_wave_slot_receipts,
)


from tests.test_ptg_wave_kubernetes import _receipts


from tests.test_ptg_wave_kubernetes_failure_attestation import (
    _actual_job,
    _failed_job,
    _failed_pods,
    _initial_attestation,
    _initial_pods,
    _manifest,
)


from tests.test_ptg_wave_kubernetes_terminal import (
    _initial_attestation as _terminal_initial_attestation,
    _terminal_job,
    _terminal_pods,
)


class _ShortIterationList(list):
    """Looks complete to a length check but omits the final member when read."""

    def __iter__(self):
        return iter(list.__getitem__(self, slice(0, 11)))


def _contract():
    manifest = _manifest()
    return manifest, kubernetes.validate_ptg_wave_job_manifest(manifest)


def _contract_values(contract):
    return {
        "wave_digest": contract.wave_digest,
        "queue": contract.queue,
        "manifest_digest": contract.manifest_digest,
        "jobs_digest": contract.jobs_digest,
        "job_count": contract.job_count,
        "config_identity": contract.config_identity,
        "manifest_identity": contract.manifest_identity,
        "runtime_image_identity": contract.runtime_image_identity,
    }
