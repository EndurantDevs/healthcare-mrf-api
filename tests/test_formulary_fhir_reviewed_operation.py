# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Unit contracts for fixed reviewed formulary operation gates and roots."""

from __future__ import annotations

import datetime as dt
import hashlib

import pytest

import process.formulary_fhir.reviewed_operation as operation_module
from process.formulary_fhir.reviewed_operation import ReviewedOperationError


CUTOFF = dt.datetime(2026, 8, 7, 12, 34, 56, tzinfo=dt.UTC)


def _set_gates(monkeypatch, acquisition: str | None, publication: str | None):
    for variable_name, value in (
        (operation_module.ACQUISITION_ENABLED_ENV, acquisition),
        (operation_module.PUBLICATION_ENABLED_ENV, publication),
    ):
        if value is None:
            monkeypatch.delenv(variable_name, raising=False)
        else:
            monkeypatch.setenv(variable_name, value)


def test_operation_contract_uses_exact_default_off_gate_names():
    assert operation_module.ACQUISITION_ENABLED_ENV == (
        "HLTHPRT_FHIR_FORMULARY_REVIEWED_ACQUISITION_ENABLED"
    )
    assert operation_module.PUBLICATION_ENABLED_ENV == (
        "HLTHPRT_FHIR_FORMULARY_REVIEWED_PUBLICATION_ENABLED"
    )
    assert operation_module.OPERATION_CONTRACT_VERSION == "reviewed-twin-v1"


@pytest.mark.parametrize("disabled_value", [None, "", "false", "TRUE", "1"])
def test_each_gate_requires_exact_lowercase_true(monkeypatch, disabled_value):
    _set_gates(monkeypatch, disabled_value, disabled_value)

    for gate in (
        operation_module.require_acquisition_gate,
        operation_module.require_publication_gate,
    ):
        with pytest.raises(ReviewedOperationError) as caught:
            gate()
        assert caught.value.code == "disabled"


def test_acquisition_and_publication_gates_are_mutually_exclusive(monkeypatch):
    _set_gates(monkeypatch, "true", None)
    operation_module.require_acquisition_gate()
    with pytest.raises(ReviewedOperationError) as caught:
        operation_module.require_publication_gate()
    assert caught.value.code == "disabled"

    _set_gates(monkeypatch, None, "true")
    operation_module.require_publication_gate()
    with pytest.raises(ReviewedOperationError) as caught:
        operation_module.require_acquisition_gate()
    assert caught.value.code == "disabled"

    _set_gates(monkeypatch, "true", "true")
    for gate in (
        operation_module.require_acquisition_gate,
        operation_module.require_publication_gate,
    ):
        with pytest.raises(ReviewedOperationError) as caught:
            gate()
        assert caught.value.code == "gate_conflict"


def test_unknown_internal_operation_is_rejected(monkeypatch):
    _set_gates(monkeypatch, None, "true")
    with pytest.raises(ReviewedOperationError) as caught:
        operation_module._require_gate("unknown")
    assert caught.value.code == "invalid_request"


def test_run_roots_are_deterministic_distinct_and_cutoff_bound():
    offset_cutoff = CUTOFF.astimezone(dt.timezone(dt.timedelta(hours=2)))
    identities = operation_module.reviewed_run_identities(offset_cutoff)
    source_id = operation_module.reviewed_source_manifest().source_id
    identity_text = "\x1f".join(
        (source_id, operation_module.OPERATION_CONTRACT_VERSION, identities.cutoff_text)
    )
    digest = hashlib.sha256(identity_text.encode("utf-8")).hexdigest()[:48]

    assert identities.cutoff_at == CUTOFF
    assert identities.cutoff_text == "2026-08-07T12:34:56Z"
    assert identities.baseline_run_id == f"ffra_{digest}"
    assert identities.candidate_run_id == f"ffrb_{digest}"
    assert len(identities.baseline_run_id) == 53
    assert identities.baseline_run_id[5:] == identities.candidate_run_id[5:]
    assert operation_module.reviewed_run_identities(CUTOFF) == identities
    assert digest not in repr(identities)
    assert "roots=<redacted>" in repr(identities)


@pytest.mark.parametrize(
    "invalid_cutoff",
    [
        "2026-08-07T12:34:56Z",
        dt.datetime(2026, 8, 7, 12, 34, 56),
        object(),
    ],
)
def test_run_roots_reject_invalid_cutoffs(invalid_cutoff):
    with pytest.raises(ReviewedOperationError) as caught:
        operation_module.reviewed_run_identities(invalid_cutoff)
    assert caught.value.code == "invalid_request"


def test_run_roots_reject_future_cutoff():
    future_cutoff = dt.datetime.now(dt.UTC) + dt.timedelta(days=1)
    with pytest.raises(ReviewedOperationError) as caught:
        operation_module.reviewed_run_identities(future_cutoff)
    assert caught.value.code == "invalid_request"


def test_operation_error_unknown_code_is_sanitized():
    error = ReviewedOperationError("private-detail")
    assert error.code == "evidence"
    assert "private" not in str(error)
