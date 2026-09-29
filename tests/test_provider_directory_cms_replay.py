# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Historical result binding and early dispatch boundaries."""

import asyncio
import dataclasses
import importlib
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_cms_replay as replay

fhir = importlib.import_module("process.provider_directory_fhir")
from tests.test_provider_directory_profile_selection_desired import _desired, _proof


def _bound_result(*, purge=False):
    """Build fixed scalar receipt inputs independently of live serving state."""
    attestation = _proof(_desired())
    pairs = () if purge else attestation.pairs
    attestation = dataclasses.replace(attestation, operation="purge" if purge else "publish", pairs=pairs)
    execution = SimpleNamespace(attestation=attestation)
    delta_by_field = {
        "operation": attestation.operation,
        "control_generation": 1,
        "generation_id": "pdprofile_" + "1" * 32,
        "selection_proof_id": attestation.proof_id,
        "authority_revision": 1,
        "profile_as_of": "2026-09-29",
        "executable_plan_hash": "b" * 64,
        "evidence_target_oid": 1,
        "profile_target_oid": 2,
        "evidence_rows": 0,
        "profile_rows": 0,
        "to_source_context_vector_hash": "c" * 64,
        "to_source_vector_hash": fhir._provider_directory_profile_source_vector_hash(
            tuple((pair["source_id"], pair["dataset_id"]) for pair in pairs)
        ),
    }
    profile_by_field = {key: field_value for key, field_value in delta_by_field.items() if not key.startswith("to_")}
    profile_by_field.update(
        status="purged" if purge else "published",
        profile_schema_version=attestation.profile_schema_version,
        profile_strategy_version=attestation.profile_strategy_version,
        source_vector_hash=delta_by_field["to_source_vector_hash"],
        source_context_vector_hash=delta_by_field["to_source_context_vector_hash"],
    )
    pin = lambda pair: {key: pair[key] for key in replay.common._PIN_FIELDS}
    payload_by_field = {
        "profile": profile_by_field,
        "selection": {
            "proof_id": attestation.proof_id,
            "fingerprint": attestation.selection_fingerprint,
            "catalog_digest": attestation.catalog_digest,
        },
        "desired_datasets": [pin(pair) for pair in pairs],
        "cms": {**pin(attestation.desired_cms_dataset), "proof_version": 2},
        "expected_incumbent": pin(attestation.expected_cms_incumbent),
    }
    return execution, delta_by_field, {"payload": payload_by_field}


@pytest.mark.parametrize("purge", [False, True])
def test_historical_result_binds_exact_publish_or_empty_purge_vector(purge):
    execution, delta_by_field, receipt = _bound_result(purge=purge)
    vector = replay._assert_common_execution(fhir, execution, delta_by_field, receipt)
    assert bool(vector) is not purge


@pytest.mark.parametrize(
    "section,key,field_value",
    [
        ("profile", "control_generation", 2),
        ("profile", "source_context_vector_hash", "0" * 64),
        ("profile", "profile_as_of", "2026-09-30"),
        ("profile", "evidence_rows", 1),
        ("selection", "proof_id", "0" * 64),
        ("cms", "acquisition_root_run_id", "another-root"),
        ("cms", "dataset_hash", "0" * 64),
        ("cms", "proof_version", 1),
    ],
)
def test_historical_result_rejects_changed_execution_or_cms_pin(section, key, field_value):
    execution, delta_by_field, receipt = _bound_result()
    receipt["payload"][section][key] = field_value
    with pytest.raises(fhir.ProviderDirectoryArtifactBuildStale):
        replay._assert_common_execution(fhir, execution, delta_by_field, receipt)


@pytest.mark.asyncio
async def test_historical_result_precedes_current_selection_and_does_not_build(monkeypatch):
    """A successful historical result must bypass both mutable selection and new admission."""
    expected_by_field = {"profile": {"generation_id": "historical"}}
    monkeypatch.setattr(replay, "replay_committed_cms_profile", AsyncMock(return_value=expected_by_field))
    current = AsyncMock(side_effect=AssertionError("historical result consulted current selection"))
    monkeypatch.setattr(fhir, "assert_registered_profile_selection_current", current)
    result = await fhir._publish_attested_provider_directory_profile(
        run_id="run_" + "1" * 32, control_run_id="run_" + "1" * 32, metrics={}, execution=object()
    )
    assert result is expected_by_field
    current.assert_not_called()


@pytest.mark.asyncio
async def test_cancelled_historical_lookup_never_falls_through_to_build(monkeypatch):
    monkeypatch.setattr(replay, "replay_committed_cms_profile", AsyncMock(side_effect=asyncio.CancelledError))
    current = AsyncMock()
    monkeypatch.setattr(fhir, "assert_registered_profile_selection_current", current)
    with pytest.raises(asyncio.CancelledError):
        await fhir._publish_attested_provider_directory_profile(
            run_id="run_" + "1" * 32, control_run_id="run_" + "1" * 32, metrics={}, execution=object()
        )
    current.assert_not_called()


@pytest.mark.asyncio
async def test_purge_without_original_delta_cannot_report_historical_completion():
    """A current empty Profile pointer alone must never manufacture a committed purge."""
    stub = SimpleNamespace(
        _schema=lambda: "synthetic",
        _replay_control_run=AsyncMock(),
        _unscoped_qt=lambda *_args: "consumption",
        ProviderDirectoryProfileCapacityLeaseConsumption=SimpleNamespace(__tablename__="consumption"),
        _replay_current_consumption=AsyncMock(return_value=None),
        _replay_exact_receipt=AsyncMock(return_value=None),
    )
    execution = SimpleNamespace(attestation=SimpleNamespace(operation="purge"))
    assert await replay._replay(stub, execution, "run_" + "1" * 32, {}) is None
