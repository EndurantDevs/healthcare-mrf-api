"""Pure attestation boundaries for V13 post-ready abandonment."""

from __future__ import annotations

import copy
from dataclasses import replace
from types import SimpleNamespace

import pytest

from api.ptg_wave_kubernetes import PTGWaveContractError
from api.ptg_wave_kubernetes_retained_failure_attestation import (
    attest_retained_preclaim_failure_kubernetes_objects,
)
from process.ptg_wave_materialized_preclaim_supersession_contract import (
    PTGWaveMaterializedPreclaimConflict,
)
from process.ptg_wave_receipt_contract import (
    PTGWaveReceiptContractError,
    ordinary_cutover_id,
)
from process.ptg_wave_v13_post_ready_abandonment import (
    attest_v13_post_ready_abandonment,
    validate_v13_abandonment_proof,
    validate_v13_abandonment_request,
)
from process.ptg_wave_v13_post_ready_abandonment_contract import _DATABASE_FIELDS
from process.ptg_wave_v13_post_ready_abandonment_validation import (
    _expected_runtime_identity_digest,
    _validate_database,
)
from tests.ptg_wave_v12_pristine_abandonment_support import boundary
from tests.ptg_wave_v13_post_ready_boundary_support import (
    mutate_failure,
    mutate_job_receipt,
    mutate_proof_job_receipt,
    mutate_retained_slot,
    mutate_retained_termination,
    mutate_wave,
    observation_boundary,
    termination_status,
    worker_status,
)
from tests.test_ptg_wave_v13_post_ready_abandonment import (
    _failed_job,
    _proof,
    _request,
    _retained_failed_pods,
)


@pytest.mark.parametrize(
    "mutate",
    [
        pytest.param(
            lambda job, _pods: job["status"].update(extra=True),
            id="job-status-fields",
        ),
        pytest.param(
            lambda job, _pods: job["status"].update(ready=False),
            id="job-status-values",
        ),
        pytest.param(
            lambda job, _pods: job["status"]["conditions"].__setitem__(
                slice(None),
                [job["status"]["conditions"][-1]],
            ),
            id="condition-count",
        ),
        pytest.param(
            lambda job, _pods: job["status"]["conditions"][0].pop("message"),
            id="condition-fields",
        ),
        pytest.param(
            lambda job, _pods: job["status"]["conditions"][0].update(type="Other"),
            id="condition-type",
        ),
        pytest.param(
            lambda job, _pods: job["status"]["conditions"][0].update(status="False"),
            id="condition-values",
        ),
        pytest.param(
            lambda job, _pods: job["status"]["conditions"][0].update(type="Failed"),
            id="condition-uniqueness",
        ),
        pytest.param(lambda _job, pods: pods.clear(), id="retained-pods-empty"),
        pytest.param(
            lambda _job, pods: pods.__setitem__(slice(None), ["not-an-object"]),
            id="retained-pod-object",
        ),
        pytest.param(
            lambda _job, pods: pods[0]["status"].update(phase="Running"),
            id="retained-pod-phase",
        ),
        pytest.param(
            lambda _job, pods: pods.__setitem__(
                slice(None), [copy.deepcopy(pods[0]), copy.deepcopy(pods[0])]
            ),
            id="retained-pod-uniqueness",
        ),
        pytest.param(
            lambda _job, pods: worker_status(pods[0]).update(extra=True),
            id="worker-fields",
        ),
        pytest.param(
            lambda _job, pods: worker_status(pods[0]).update(ready=True),
            id="worker-values",
        ),
        pytest.param(
            lambda _job, pods: worker_status(pods[0])["state"].update(running={}),
            id="worker-state",
        ),
        pytest.param(
            lambda _job, pods: termination_status(pods[0]).update(extra=True),
            id="termination-fields",
        ),
        pytest.param(
            lambda _job, pods: termination_status(pods[0]).update(reason="Completed"),
            id="termination-values",
        ),
        pytest.param(
            lambda _job, pods: termination_status(pods[0]).update(
                startedAt="not-a-time"
            ),
            id="timestamp-shape",
        ),
        pytest.param(
            lambda _job, pods: termination_status(pods[0]).update(
                startedAt="2026-13-17T00:06:03Z"
            ),
            id="timestamp-calendar",
        ),
    ],
)
def test_retained_kubernetes_rejects_noncanonical_observations(mutate):
    """Raw Job and retained-Pod evidence must remain exact and internally bound."""

    wave, _intents, _runs, _admission = boundary()
    job = _failed_job(wave.kubernetes_manifest)
    pods = _retained_failed_pods(wave.kubernetes_manifest)
    mutate(job, pods)

    with pytest.raises(PTGWaveContractError):
        attest_retained_preclaim_failure_kubernetes_objects(
            wave.kubernetes_manifest,
            job,
            pods,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mutate",
    [
        pytest.param(
            lambda observation: replace(observation, claims="not-a-sequence"),
            id="observation-sequences",
        ),
        pytest.param(
            lambda observation: replace(observation, claims=(object(),)),
            id="zero-work",
        ),
        pytest.param(
            lambda observation: replace(observation, logical_supersession=object()),
            id="legacy-recovery-row",
        ),
        pytest.param(
            lambda observation: mutate_wave(observation, state="released"),
            id="wave-state",
        ),
        pytest.param(
            lambda observation: mutate_wave(observation, k8s_post_ticket=None),
            id="job-receipt-presence",
        ),
        pytest.param(
            lambda observation: mutate_wave(
                observation,
                kubernetes_ready_attestation={},
            ),
            id="no-ready-receipt",
        ),
        pytest.param(mutate_job_receipt, id="stored-job-receipt"),
        pytest.param(
            lambda observation: mutate_wave(
                observation,
                redis_release_ticket="release",
            ),
            id="later-lifecycle",
        ),
    ],
)
async def test_v13_attestation_rejects_nonpristine_boundaries(mutate):
    """The proof rejects work, legacy recovery, and later lifecycle state."""

    observation, admission, _redis = await observation_boundary()
    changed = mutate(observation)

    with pytest.raises(PTGWaveMaterializedPreclaimConflict):
        attest_v13_post_ready_abandonment(
            changed,
            cutover_id=ordinary_cutover_id(admission["wave_id"]),
            admission=admission,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mutate",
    [
        pytest.param(lambda request: request.pop("admission"), id="fields"),
        pytest.param(
            lambda request: request.update(key_id="different-key"),
            id="identity",
        ),
    ],
)
async def test_v13_request_rejects_nonexact_coordinates(mutate):
    """The public proof builder rejects malformed and conflicting requests."""

    observation, admission, _redis = await observation_boundary()
    request = _request(admission)
    mutate(request)

    with pytest.raises(PTGWaveReceiptContractError):
        validate_v13_abandonment_request(
            request,
            wave=observation.predecessor_wave,
            admission=admission,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("mutate", "call_overrides"),
    [
        pytest.param(lambda proof: proof.pop("schema_version"), {}, id="proof-fields"),
        pytest.param(
            lambda proof: proof.update(schema_version="unsupported"),
            {},
            id="proof-family",
        ),
        pytest.param(
            lambda proof: proof.update(operation_id="not-a-digest"),
            {},
            id="operation-digest",
        ),
        pytest.param(
            lambda proof: proof.update(cutover_id="0" * 64),
            {},
            id="cutover-binding",
        ),
        pytest.param(
            lambda _proof: None,
            {"operation_id": "0" * 64},
            id="expected-operation",
        ),
        pytest.param(
            lambda _proof: None,
            {"cutover_id": "0" * 64},
            id="expected-cutover",
        ),
        pytest.param(
            lambda _proof: None,
            {"admission": {"different": "admission"}},
            id="expected-admission",
        ),
        pytest.param(
            lambda proof: proof.update(proof_digest="0" * 64),
            {},
            id="proof-digest",
        ),
        pytest.param(lambda proof: proof.update(database={}), {}, id="database-fields"),
        pytest.param(
            lambda proof: proof["database"].update(state="released"),
            {},
            id="database-state",
        ),
        pytest.param(
            lambda proof: proof.update(kubernetes={}),
            {},
            id="kubernetes-fields",
        ),
        pytest.param(
            lambda proof: proof["kubernetes"].update(job_receipt={}),
            {},
            id="job-receipt-fields",
        ),
        pytest.param(
            lambda proof: proof["kubernetes"].update(ready_attestation={}),
            {},
            id="ready-attestation",
        ),
        pytest.param(mutate_proof_job_receipt, {}, id="job-receipt-binding"),
        pytest.param(
            lambda proof: proof["kubernetes"].update(failure={}),
            {},
            id="failure-fields",
        ),
        pytest.param(lambda proof: proof.update(redis={}), {}, id="redis-fields"),
        pytest.param(
            lambda proof: proof["redis"]["ready_slots"].pop(),
            {},
            id="ready-slot-count",
        ),
        pytest.param(
            lambda proof: proof["redis"]["ready_slots"][0].update(extra=True),
            {},
            id="ready-slot-fields",
        ),
        pytest.param(
            lambda proof: proof["redis"]["ready_slots"][0].update(slot=1),
            {},
            id="ready-slot-identity",
        ),
        pytest.param(
            lambda proof: proof["redis"]["ready_slots"][0].update(pod_uid=""),
            {},
            id="ready-slot-binding",
        ),
        pytest.param(
            lambda proof: proof["redis"].update(attestation_digest="0" * 64),
            {},
            id="redis-digest",
        ),
        pytest.param(
            lambda proof: mutate_failure(proof, retained_failed_slots=[]),
            {},
            id="retained-subset-required",
        ),
        pytest.param(
            lambda proof: mutate_retained_slot(proof, extra=True),
            {},
            id="retained-slot-fields",
        ),
        pytest.param(
            lambda proof: mutate_retained_termination(proof, reason="Completed"),
            {},
            id="retained-slot-identity",
        ),
        pytest.param(
            lambda proof: proof["kubernetes"]["failure"]["job_conditions"].pop(),
            {},
            id="failure-condition-count",
        ),
        pytest.param(
            lambda proof: proof["kubernetes"]["failure"]["job_conditions"][0].update(
                type="FailureTarget"
            ),
            {},
            id="failure-condition-types",
        ),
        pytest.param(
            lambda proof: proof["kubernetes"]["failure"]["job_conditions"][0].update(
                status="False"
            ),
            {},
            id="failure-condition-values",
        ),
        pytest.param(
            lambda proof: proof["kubernetes"]["failure"].update(
                start_time="not-a-time"
            ),
            {},
            id="failure-timestamp-shape",
        ),
        pytest.param(
            lambda proof: proof["kubernetes"]["failure"].update(
                start_time="2026-13-17T00:06:01Z"
            ),
            {},
            id="failure-timestamp-calendar",
        ),
    ],
)
async def test_v13_proof_rejects_cross_boundary_drift(mutate, call_overrides):
    """Proof coordinates and each external evidence family are exact."""

    proof, admission = await _proof()
    changed = copy.deepcopy(proof)
    mutate(changed)
    override_by_field = dict(call_overrides)
    if "admission" in override_by_field:
        override_by_field["admission"] = {
            **admission,
            "receipt_key_id": "different-key",
        }

    with pytest.raises(PTGWaveMaterializedPreclaimConflict):
        validate_v13_abandonment_proof(changed, **override_by_field)


def test_v13_defenses_reject_unreachable_inputs():
    """Defensive helpers reject inputs a valid admission cannot create."""

    with pytest.raises(PTGWaveMaterializedPreclaimConflict, match="intent count"):
        _validate_database(dict.fromkeys(_DATABASE_FIELDS), 0)
    with pytest.raises(PTGWaveMaterializedPreclaimConflict, match="runtime identity"):
        _expected_runtime_identity_digest(
            SimpleNamespace(
                kubernetes_config_identity="invalid",
                kubernetes_manifest_identity="invalid",
                pinned_image_reference="invalid",
                runtime_image_identity="invalid",
            )
        )
