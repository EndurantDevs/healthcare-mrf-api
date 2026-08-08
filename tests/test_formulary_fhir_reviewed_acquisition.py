# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Unit contracts for the fixed reviewed formulary twin acquisition."""

from __future__ import annotations

import asyncio
import datetime as dt
from dataclasses import replace
import json

import pytest

import process.formulary_fhir.reviewed_acquisition as acquisition_module
from process.formulary_fhir.repository_admission import TwinAdmissionError
from process.formulary_fhir.repository_admission_types import AlternativeProof
from process.formulary_fhir.repository_admission_types import TwinAdmissionResult
from process.formulary_fhir.repository_shared import DatasetVerification
from process.formulary_fhir.repository_shared import stable_id
from process.formulary_fhir.reviewed_operation import ACQUISITION_ENABLED_ENV
from process.formulary_fhir.reviewed_operation import PUBLICATION_ENABLED_ENV
from process.formulary_fhir.reviewed_operation import ReviewedOperationError
from process.formulary_fhir.reviewed_source import ReviewedSourceError


CUTOFF = dt.datetime(2026, 8, 7, 12, 34, 56, tzinfo=dt.UTC)
BASELINE_VERIFIED_AT = CUTOFF + dt.timedelta(minutes=1)
CANDIDATE_VERIFIED_AT = CUTOFF + dt.timedelta(minutes=2)
ADMITTED_AT = CUTOFF + dt.timedelta(minutes=3)


def _set_gates(monkeypatch, acquisition: str | None, publication: str | None):
    gate_values = (
        (ACQUISITION_ENABLED_ENV, acquisition),
        (PUBLICATION_ENABLED_ENV, publication),
    )
    for variable_name, value in gate_values:
        if value is None:
            monkeypatch.delenv(variable_name, raising=False)
        else:
            monkeypatch.setenv(variable_name, value)


def _identities():
    return acquisition_module.reviewed_run_identities(CUTOFF)


def _admission() -> TwinAdmissionResult:
    manifest = acquisition_module.reviewed_source_manifest()
    identities = _identities()
    baseline_dataset_id = stable_id(
        "ffd_",
        manifest.source_id,
        identities.baseline_run_id,
    )
    candidate_dataset_id = stable_id(
        "ffd_",
        manifest.source_id,
        identities.candidate_run_id,
    )
    return TwinAdmissionResult(
        source_id=manifest.source_id,
        baseline_dataset_id=baseline_dataset_id,
        baseline_run_id=identities.baseline_run_id,
        candidate_dataset_id=candidate_dataset_id,
        candidate_run_id=identities.candidate_run_id,
        predecessor_dataset_id=None,
        cutoff_at=CUTOFF,
        source_configuration_hash="a" * 64,
        acquisition_contract_hash="b" * 64,
        verification=DatasetVerification(
            manifest.source_id,
            candidate_dataset_id,
            2,
            3,
            5,
            "c" * 64,
            "d" * 64,
        ),
        alternative=AlternativeProof(1, "e" * 64),
        baseline_verified_at=BASELINE_VERIFIED_AT,
        candidate_verified_at=CANDIDATE_VERIFIED_AT,
        admitted_at=ADMITTED_AT,
    )


@pytest.mark.parametrize(
    "acquisition_gate,publication_gate,expected_code",
    [
        (None, None, "disabled"),
        (None, "true", "disabled"),
        ("true", "true", "gate_conflict"),
    ],
)
@pytest.mark.asyncio
async def test_acquisition_gate_precedes_identity_client_and_database(
    monkeypatch,
    acquisition_gate,
    publication_gate,
    expected_code,
):
    _set_gates(monkeypatch, acquisition_gate, publication_gate)
    downstream_calls: list[str] = []

    def forbidden_identities(_cutoff):
        downstream_calls.append("identity")
        raise AssertionError("identities derived")

    async def forbidden_verifier(**_kwargs):
        downstream_calls.append("verifier")
        raise AssertionError("verifier entered")

    def forbidden_client(*_args, **_kwargs):
        downstream_calls.append("client")
        raise AssertionError("client created")

    monkeypatch.setattr(
        acquisition_module,
        "reviewed_run_identities",
        forbidden_identities,
    )
    monkeypatch.setattr(
        acquisition_module,
        "verify_reviewed_source_twins",
        forbidden_verifier,
    )

    with pytest.raises(ReviewedOperationError) as caught:
        await acquisition_module.acquire_reviewed_twins(
            cutoff=CUTOFF,
            database=object(),
            client_factory=forbidden_client,
        )

    assert caught.value.code == expected_code
    assert downstream_calls == []


@pytest.mark.asyncio
async def test_acquisition_uses_only_fixed_roots_and_returns_admission(monkeypatch):
    _set_gates(monkeypatch, "true", None)
    admission = _admission()
    database = object()
    client_factory = object()
    observed_kwargs_by_name: dict[str, object] = {}

    async def verify_twins(**kwargs):
        observed_kwargs_by_name.update(kwargs)
        return admission

    monkeypatch.setattr(
        acquisition_module,
        "verify_reviewed_source_twins",
        verify_twins,
    )

    acquisition_result = await acquisition_module.acquire_reviewed_twins(
        cutoff=CUTOFF,
        database=database,
        client_factory=client_factory,
    )
    identities = _identities()

    assert observed_kwargs_by_name == {
        "baseline_run_id": identities.baseline_run_id,
        "candidate_run_id": identities.candidate_run_id,
        "cutoff": CUTOFF,
        "database": database,
        "client_factory": client_factory,
    }
    assert acquisition_result.baseline_run_id == identities.baseline_run_id
    assert acquisition_result.candidate_run_id == identities.candidate_run_id
    assert acquisition_result.baseline_dataset_id == admission.baseline_dataset_id
    assert acquisition_result.candidate_dataset_id == admission.candidate_dataset_id
    assert acquisition_result.admitted_at == ADMITTED_AT
    assert (
        acquisition_result.list_count,
        acquisition_result.alias_count,
        acquisition_result.medication_count,
    ) == (
        2,
        3,
        5,
    )


def test_acquisition_result_rejects_root_cutoff_and_type_drift():
    identities = _identities()
    candidate_dataset_drift = _admission()
    object.__setattr__(
        candidate_dataset_drift,
        "candidate_dataset_id",
        "ffd_" + ("8" * 48),
    )
    for drifted_admission in (
        object(),
        replace(_admission(), baseline_run_id="different-baseline-root"),
        replace(_admission(), candidate_run_id="different-candidate-root"),
        replace(_admission(), baseline_dataset_id="ffd_" + ("7" * 48)),
        candidate_dataset_drift,
        replace(_admission(), cutoff_at=CUTOFF - dt.timedelta(days=1)),
    ):
        with pytest.raises(ReviewedOperationError) as caught:
            acquisition_module._acquisition_result(
                drifted_admission,
                identities,
            )
        assert caught.value.code == "evidence"


@pytest.mark.parametrize(
    "evidence_name,field_name,invalid_value",
    [
        ("verification", "list_count", 0),
        ("alternative", "count", -1),
    ],
)
def test_acquisition_result_rejects_tampered_admission_evidence(
    evidence_name,
    field_name,
    invalid_value,
):
    admission = _admission()
    evidence = getattr(admission, evidence_name)
    object.__setattr__(evidence, field_name, invalid_value)

    with pytest.raises(ReviewedOperationError) as caught:
        acquisition_module._acquisition_result(admission, _identities())

    assert caught.value.code == "evidence"


@pytest.mark.parametrize(
    "upstream_error,expected_code",
    [
        (ReviewedSourceError("busy"), "busy"),
        (ReviewedSourceError("invalid_request"), "invalid_request"),
        (ReviewedSourceError("catalog"), "acquisition"),
        (TwinAdmissionError("mismatch"), "mismatch"),
        (TwinAdmissionError("missing"), "acquisition"),
    ],
)
@pytest.mark.asyncio
async def test_acquisition_maps_domain_errors(
    monkeypatch,
    upstream_error,
    expected_code,
):
    _set_gates(monkeypatch, "true", None)

    async def fail_verification(**_kwargs):
        raise upstream_error

    monkeypatch.setattr(
        acquisition_module,
        "verify_reviewed_source_twins",
        fail_verification,
    )
    with pytest.raises(ReviewedOperationError) as caught:
        await acquisition_module.acquire_reviewed_twins(cutoff=CUTOFF)
    assert caught.value.code == expected_code


@pytest.mark.asyncio
async def test_acquisition_sanitizes_private_failure(monkeypatch):
    _set_gates(monkeypatch, "true", None)

    async def fail_verification(**_kwargs):
        raise RuntimeError("https://private.invalid/fhir?token=secret")

    monkeypatch.setattr(
        acquisition_module,
        "verify_reviewed_source_twins",
        fail_verification,
    )
    with pytest.raises(ReviewedOperationError) as caught:
        await acquisition_module.acquire_reviewed_twins(cutoff=CUTOFF)
    assert caught.value.code == "acquisition"
    assert "private" not in str(caught.value)


@pytest.mark.asyncio
async def test_acquisition_preserves_reviewed_operation_error(monkeypatch):
    _set_gates(monkeypatch, "true", None)
    domain_error = ReviewedOperationError("evidence")

    async def fail_verification(**_kwargs):
        raise domain_error

    monkeypatch.setattr(
        acquisition_module,
        "verify_reviewed_source_twins",
        fail_verification,
    )
    with pytest.raises(ReviewedOperationError) as caught:
        await acquisition_module.acquire_reviewed_twins(cutoff=CUTOFF)

    assert caught.value is domain_error


@pytest.mark.parametrize("interrupt", [TimeoutError(), asyncio.CancelledError()])
@pytest.mark.asyncio
async def test_acquisition_preserves_timeout_and_cancellation(monkeypatch, interrupt):
    _set_gates(monkeypatch, "true", None)

    async def interrupt_verification(**_kwargs):
        raise interrupt

    monkeypatch.setattr(
        acquisition_module,
        "verify_reviewed_source_twins",
        interrupt_verification,
    )
    with pytest.raises(type(interrupt)):
        await acquisition_module.acquire_reviewed_twins(cutoff=CUTOFF)


def test_acquisition_json_is_exact_and_rejects_other_types():
    result = acquisition_module._acquisition_result(_admission(), _identities())
    rendered = json.loads(acquisition_module.acquisition_result_json(result))

    assert rendered == {
        "status": "admitted",
        "baseline_run_id": result.baseline_run_id,
        "candidate_run_id": result.candidate_run_id,
        "baseline_dataset_id": result.baseline_dataset_id,
        "candidate_dataset_id": result.candidate_dataset_id,
        "cutoff": "2026-08-07T12:34:56Z",
        "source_configuration_hash": "a" * 64,
        "acquisition_contract_hash": "b" * 64,
        "list_count": 2,
        "alias_count": 3,
        "medication_count": 5,
        "coverage_hash": "c" * 64,
        "membership_hash": "d" * 64,
        "alternative_count": 1,
        "alternative_hash": "e" * 64,
        "admitted_at": "2026-08-07T12:37:56Z",
    }
    with pytest.raises(ReviewedOperationError) as caught:
        acquisition_module.acquisition_result_json(object())
    assert caught.value.code == "evidence"


@pytest.mark.parametrize(
    "field_name,invalid_value",
    [("list_count", 0), ("alternative_count", -1)],
)
def test_acquisition_json_revalidates_tampered_result(
    field_name,
    invalid_value,
):
    acquisition_result = acquisition_module._acquisition_result(
        _admission(),
        _identities(),
    )
    object.__setattr__(acquisition_result, field_name, invalid_value)

    with pytest.raises(ReviewedOperationError) as caught:
        acquisition_module.acquisition_result_json(acquisition_result)

    assert caught.value.code == "evidence"
