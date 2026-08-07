"""Shared builders for PTG wave controller tests."""


def claimed_prestart_wave(wave_factory):
    return wave_factory(
        state="executing",
        uncertainty_resume_state=None,
        kubernetes_job_uid="job-unit",
        kubernetes_ready_attestation={"slots": [
            {"slot": slot, "pod_uid": f"pod-{slot}"}
            for slot in range(12)
        ]},
        kubernetes_ready_attestation_digest="a" * 64,
    )


def failed_job_object():
    return {
        "status": {
            "conditions": [{"type": "Failed", "status": "True"}],
        },
    }
