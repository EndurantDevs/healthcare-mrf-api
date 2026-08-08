# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Publication contracts for mandatory ordinary twin admission."""

from __future__ import annotations

import datetime as dt
from unittest.mock import AsyncMock

import pytest

import process.formulary_fhir.repository_publish as publish_module
from process.formulary_fhir.repository_admission import TwinAdmissionError
from process.formulary_fhir.repository_shared import DatasetRef


CUTOFF = dt.datetime(2026, 8, 8, tzinfo=dt.UTC)


def _dataset(*, intent: str) -> DatasetRef:
    return DatasetRef(
        "source-alpha",
        "ffd_" + "1" * 48,
        "candidate-run",
        None,
        CUTOFF,
        "a" * 64,
        intent,
        "verified",
    )


@pytest.mark.asyncio
async def test_seed_publication_retains_single_dataset_lock(monkeypatch):
    dataset_by_field = {"status": "verified", "seed_eligible": True}
    lock_dataset = AsyncMock(return_value=dataset_by_field)
    verify_admission = AsyncMock(
        side_effect=AssertionError("seed must not use twin admission")
    )
    monkeypatch.setattr(publish_module, "lock_dataset", lock_dataset)
    monkeypatch.setattr(
        publish_module,
        "verify_twin_admission_for_publication",
        verify_admission,
    )

    observed = await publish_module._locked_publication_dataset(
        "database",
        "source-alpha",
        _dataset(intent="seed"),
        seed_proof=True,
    )

    assert observed == dataset_by_field
    assert lock_dataset.await_args.kwargs["allowed_statuses"] == {
        "verified",
        "published",
    }
    verify_admission.assert_not_called()


@pytest.mark.asyncio
async def test_ordinary_publication_requires_locked_reverified_admission(
    monkeypatch,
):
    dataset = _dataset(intent="requested")
    dataset_by_field = {"status": "verified", "publish_requested": True}
    verify_admission = AsyncMock(return_value=("admission", dataset_by_field))
    lock_dataset = AsyncMock(
        side_effect=AssertionError("ordinary path must lock the exact pair")
    )
    monkeypatch.setattr(
        publish_module,
        "verify_twin_admission_for_publication",
        verify_admission,
    )
    monkeypatch.setattr(publish_module, "lock_dataset", lock_dataset)

    observed = await publish_module._locked_publication_dataset(
        "database",
        "source-alpha",
        dataset,
        seed_proof=False,
    )

    assert observed == dataset_by_field
    assert verify_admission.await_args.args == (
        "database",
        "source-alpha",
        dataset,
    )
    lock_dataset.assert_not_called()


@pytest.mark.asyncio
async def test_ordinary_publication_propagates_sanitized_missing_admission(
    monkeypatch,
):
    monkeypatch.setattr(
        publish_module,
        "verify_twin_admission_for_publication",
        AsyncMock(side_effect=TwinAdmissionError("missing")),
    )

    with pytest.raises(TwinAdmissionError) as caught:
        await publish_module._locked_publication_dataset(
            "database",
            "source-alpha",
            _dataset(intent="requested"),
            seed_proof=False,
        )

    assert caught.value.code == "missing"
    assert "source-alpha" not in str(caught.value)
