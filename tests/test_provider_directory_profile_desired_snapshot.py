# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Desired date changes must rebuild facts and preserve the incumbent fence."""

from __future__ import annotations

import dataclasses
import importlib
from types import SimpleNamespace

import pytest

from tests.provider_directory_profile_delta_coverage_support import _matching_delta_serving_state
from tests.provider_directory_profile_delta_test_support import _prepared_delta
from tests.provider_directory_profile_execution_test_support import _wal_tracker_admission

importer = importlib.import_module("process.provider_directory_fhir")


def _desired_execution(selected_date):
    return SimpleNamespace(attestation=SimpleNamespace(desired_profile_as_of=selected_date, proof_id="a" * 64))


@pytest.mark.parametrize("selected_date,expected", [(None, ()), ("2026-07-30", ()), ("2026-07-31", ("source-a",))])
def test_unchanged_dataset_refreshes_facts_when_desired_date_changes(selected_date, expected):
    serving = _matching_delta_serving_state(_prepared_delta())
    token = importer._PROVIDER_DIRECTORY_PROFILE_SELECTION_EXECUTION.set(_desired_execution(selected_date))
    try:
        refreshed, removed = importer._provider_directory_profile_delta_sources(
            serving, serving.source_vector, serving.source_context_vector
        )
    finally:
        importer._PROVIDER_DIRECTORY_PROFILE_SELECTION_EXECUTION.reset(token)
    assert refreshed == expected
    assert removed == ()


@pytest.mark.parametrize("mode", ["source_delta", "full_swap"])
def test_build_coordinates_use_desired_date_instead_of_incumbent_or_clock(mode):
    admission = _wal_tracker_admission()
    serving = _matching_delta_serving_state(_prepared_delta())
    identity = SimpleNamespace(resume_lineage_hash="a" * 64, materialization_mode=mode, serving_state=serving)
    selection_token = importer._PROVIDER_DIRECTORY_PROFILE_SELECTION_EXECUTION.set(_desired_execution("2026-07-31"))
    capacity_token = importer._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.set(admission)
    try:
        coordinates = importer._profile_build_coordinates(identity)
    finally:
        importer._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.reset(capacity_token)
        importer._PROVIDER_DIRECTORY_PROFILE_SELECTION_EXECUTION.reset(selection_token)
    assert coordinates.profile_as_of == "2026-07-31"


def test_capacity_consumption_binding_uses_the_same_desired_date():
    identity = SimpleNamespace(
        resume_lineage_hash="a" * 64,
        batch_plan=SimpleNamespace(fingerprint="b" * 64),
        desired_source_vector_hash="c" * 64,
        desired_source_context_vector_hash="d" * 64,
        serving_state=SimpleNamespace(profile_as_of="2026-07-30"),
    )
    _build_id, binding = importer._profile_admission_binding("run-a", identity, _desired_execution("2026-07-31"))
    assert binding.profile_as_of == "2026-07-31"


def test_desired_proof_refuses_checkpoint_materialized_for_another_date(monkeypatch):
    monkeypatch.setattr(importer, "_is_checkpoint_core_lineage_matching", lambda *_args: True)
    monkeypatch.setattr(importer, "_is_checkpoint_source_lineage_matching", lambda *_args: True)
    monkeypatch.setattr(importer, "_is_profile_checkpoint_geometry_matching", lambda *_args, **_kwargs: True)
    build = SimpleNamespace(profile_as_of="2026-07-31")
    token = importer._PROVIDER_DIRECTORY_PROFILE_SELECTION_EXECUTION.set(_desired_execution("2026-07-31"))
    try:
        assert not importer._is_profile_build_checkpoint_lineage_matching(
            {"profile_as_of": "2026-07-30"}, build, None, None
        )
        assert importer._is_profile_build_checkpoint_lineage_matching(
            {"profile_as_of": "2026-07-31"}, build, None, None
        )
    finally:
        importer._PROVIDER_DIRECTORY_PROFILE_SELECTION_EXECUTION.reset(token)


def test_date_delta_requires_the_original_serving_date():
    delta = _prepared_delta(profile_as_of="2026-07-31", from_profile_as_of="2026-07-30")
    serving = _matching_delta_serving_state(delta)
    serving = dataclasses.replace(serving, profile_as_of="2026-07-30")
    importer._validate_profile_delta_serving_state(serving, delta)
    with pytest.raises(importer.ProviderDirectoryArtifactBuildStale, match="serving_generation_changed"):
        importer._validate_profile_delta_serving_state(dataclasses.replace(serving, profile_as_of="2026-07-29"), delta)
