# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Focused coverage for bounded Provider Directory admission writes."""

from __future__ import annotations

import importlib
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from tests.test_provider_directory_twin_root_verification import _candidate


importer = importlib.import_module("process.provider_directory_fhir")


@pytest.mark.asyncio
async def test_dataset_writers_wrap_invalid_admission_seals(monkeypatch):
    def reject_seal(_metadata):
        raise importer.AdmissionSealError("invalid")

    monkeypatch.setattr(
        importer,
        "admission_seal_from_validated_metadata",
        reject_seal,
    )
    candidate = _candidate()

    with pytest.raises(RuntimeError, match="admission_seal_invalid"):
        await importer._store_validated_endpoint_dataset(
            AsyncMock(),
            candidate,
            candidate.previous_dataset_id,
            "d" * 64,
            2,
            {"verification": "matched"},
        )
    with pytest.raises(RuntimeError, match="admission_seal_invalid"):
        await importer._store_baseline_payload_retirement(
            AsyncMock(),
            candidate,
            "dataset_baseline",
            {"verification": "matched"},
        )


@pytest.mark.asyncio
async def test_baseline_retirement_uses_bounded_admission_receipt(monkeypatch):
    admission_receipt = SimpleNamespace(
        metadata_summary={"verification": "matched"},
        metadata_sha256="a" * 64,
        admission_version=1,
        admission_kind="generic",
        proof_sha256="b" * 64,
        resource_types=("Organization",),
    )
    monkeypatch.setattr(
        importer,
        "admission_seal_from_validated_metadata",
        lambda _metadata: admission_receipt,
    )
    connection = AsyncMock()
    connection.status.return_value = "UPDATE 1"

    await importer._store_baseline_payload_retirement(
        connection,
        _candidate(),
        "dataset_baseline",
        {"verification": "matched"},
    )

    query = connection.status.await_args.args[0]
    assert "SET publication_metadata_summary_json" in query
    assert "SET publication_metadata_json" not in query
    assert connection.status.await_args.kwargs["publication_metadata_sha256"] == (
        "a" * 64
    )

    monkeypatch.setattr(
        importer,
        "admission_seal_from_validated_metadata",
        lambda _metadata: None,
    )
    connection = AsyncMock()
    connection.status.return_value = "UPDATE 1"
    await importer._store_baseline_payload_retirement(
        connection,
        _candidate(),
        "dataset_baseline",
        {"verification": "matched"},
    )
    assert "SET publication_metadata_json" in connection.status.await_args.args[0]
