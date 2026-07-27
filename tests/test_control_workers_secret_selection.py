# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Worker-class boundaries for Kubernetes secret mounts."""

from __future__ import annotations

import json

import pytest

from api import control_workers


_SCANNER_CLASSES = [
    "process.PTG",
    "process.PTGSmall",
    "process.PTGNormal",
    "process.PTGLarge",
    "process.PTGHuge",
]
_SELECTOR_MOUNT_SPECS = [
    {
        "name": "global-secret",
        "secretName": "global-secret",
        "mountPath": "/secrets/global",
    },
    {
        "name": "ptg-token",
        "secretName": "ptg-token",
        "mountPath": "/secrets/ptg-token",
        "workerClasses": _SCANNER_CLASSES,
    },
    {
        "name": "invalid-selector",
        "secretName": "invalid-selector",
        "mountPath": "/secrets/invalid",
        "workerClasses": "process.PTG",
    },
    {
        "name": "null-selector",
        "secretName": "null-selector",
        "mountPath": "/secrets/null",
        "workerClasses": None,
    },
    {
        "name": "ambiguous-selector",
        "secretName": "ambiguous-selector",
        "mountPath": "/secrets/ambiguous",
        "workerClasses": ["process.PTG"],
        "worker_classes": ["process.PTGCandidateAudit"],
    },
    {
        "name": "audit-only",
        "secretName": "audit-only",
        "mountPath": "/secrets/audit-only",
        "worker_classes": ["process.PTGCandidateAudit"],
    },
]


def _worker_spec(worker_class: str, role: str) -> control_workers.WorkerSpec:
    return control_workers.WorkerSpec(
        f"arq:{worker_class.removeprefix('process.')}",
        worker_class,
        (role,),
    )


def _secret_volume_names(spec: control_workers.WorkerSpec) -> set[str]:
    return {
        volume_spec["volume"]["name"]
        for volume_spec in control_workers._worker_job_secret_volumes(spec)
    }


def _worker_pod(
    spec: control_workers.WorkerSpec,
    run_id: str,
) -> dict:
    job = control_workers._worker_job_manifest(
        spec,
        {"run_id": run_id},
        "example.invalid/worker:test",
    )
    return job["spec"]["template"]["spec"]


def test_secret_volumes_select_exact_worker_classes(monkeypatch):
    monkeypatch.setenv(
        "HLTHPRT_WORKER_JOB_SECRET_VOLUME_MOUNTS_JSON",
        json.dumps(_SELECTOR_MOUNT_SPECS),
    )
    scanner = _worker_spec("process.PTGLarge", "ptg")
    audit = _worker_spec("process.PTGCandidateAudit", "ptg-candidate-audit")

    assert _secret_volume_names(scanner) == {"global-secret", "ptg-token"}
    assert _secret_volume_names(audit) == {"global-secret", "audit-only"}


def test_candidate_audit_job_never_receives_scanner_token_secret(monkeypatch):
    scanner_mount_by_field = {
        "name": "ptg-token",
        "secretName": "ptg-token",
        "mountPath": "/secrets/ptg-token",
        "defaultMode": 0o440,
        "workerClasses": _SCANNER_CLASSES,
    }
    monkeypatch.setenv(
        "HLTHPRT_WORKER_JOB_SECRET_VOLUME_MOUNTS_JSON",
        json.dumps([scanner_mount_by_field]),
    )
    monkeypatch.delenv("HLTHPRT_WORKER_JOB_PVC_NAME", raising=False)
    monkeypatch.delenv("HLTHPRT_WORKER_JOB_PVC_MOUNT_PATH", raising=False)

    scanner_pod = _worker_pod(
        _worker_spec("process.PTGHuge", "ptg"),
        "scanner-run",
    )
    audit_pod = _worker_pod(
        _worker_spec("process.PTGCandidateAudit", "ptg-candidate-audit"),
        "audit-run",
    )

    assert scanner_pod["volumes"] == [
        {
            "name": "ptg-token",
            "secret": {
                "secretName": "ptg-token",
                "defaultMode": 0o440,
            },
        }
    ]
    assert scanner_pod["containers"][0]["volumeMounts"] == [
        {
            "name": "ptg-token",
            "mountPath": "/secrets/ptg-token",
            "readOnly": True,
        }
    ]
    assert scanner_pod["securityContext"]["fsGroup"] == 65534
    assert (
        scanner_pod["securityContext"]["fsGroupChangePolicy"]
        == "OnRootMismatch"
    )
    assert "volumes" not in audit_pod
    assert "volumeMounts" not in audit_pod["containers"][0]


@pytest.mark.parametrize(
    "default_mode",
    [True, -1, 0o1000, "0400"],
)
def test_secret_volumes_reject_invalid_default_mode(
    monkeypatch,
    default_mode,
):
    monkeypatch.setenv(
        "HLTHPRT_WORKER_JOB_SECRET_VOLUME_MOUNTS_JSON",
        json.dumps(
            [
                {
                    "secretName": "provider-a",
                    "mountPath": "/secrets/a",
                    "defaultMode": default_mode,
                },
            ]
        ),
    )
    spec = _worker_spec("process.PTG", "ptg")
    assert control_workers._worker_job_secret_volumes(spec) == []


def test_secret_volumes_reject_ambiguous_default_mode_aliases(monkeypatch):
    monkeypatch.setenv(
        "HLTHPRT_WORKER_JOB_SECRET_VOLUME_MOUNTS_JSON",
        json.dumps(
            [
                {
                    "secretName": "provider-a",
                    "mountPath": "/secrets/a",
                    "defaultMode": 0o400,
                    "default_mode": 0o400,
                },
            ]
        ),
    )
    spec = _worker_spec("process.PTG", "ptg")
    assert control_workers._worker_job_secret_volumes(spec) == []
