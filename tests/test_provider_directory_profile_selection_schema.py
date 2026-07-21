# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import importlib
from pathlib import Path

from process import provider_directory_profile_selection as selection


importer = importlib.import_module("process.provider_directory_fhir")


def test_generic_profile_followup_descriptor_is_source_local_and_versioned():
    descriptor = importer._provider_directory_global_profile_followup(
        source_id="pdfhir_payer",
        dataset_id="dataset-1",
        parent_run_id="run-root-1",
    )

    assert descriptor["kind"] == "provider_directory_global_profile"
    assert descriptor["source_id"] == "pdfhir_payer"
    assert descriptor["dataset_id"] == "dataset-1"
    assert descriptor["parent_run_id"] == "run-root-1"
    assert descriptor["params"]["source_ids"] == []
    assert descriptor["params"]["require_complete_global_profile_fence"] is True


def test_migration_and_models_define_durable_monotonic_authority():
    migration_path = (
        Path(__file__).resolve().parents[1]
        / "alembic/versions/20260721100000_provider_directory_profile_selection_attestation.py"
    )
    migration_source = migration_path.read_text(encoding="utf-8")

    assert "20260720130000_uhc_retained_artifact_admission" in migration_source
    assert "last_revision = last_revision + 1" not in migration_source
    assert "authority_revision" in migration_source
    assert "provider_directory_profile_selection_observation_pkey" in migration_source
    assert "pd_profile_selection_observation_input_fkey" in migration_source
    assert migration_source.count("create_table_or_validate(") == 3
    assert "create_index_if_missing(" in migration_source
    assert (
        selection.ProviderDirectoryProfileSelectionProof.__tablename__
        == "provider_directory_profile_selection_proof"
    )
    assert (
        selection.ProviderDirectoryProfileSelectionObservation.__tablename__
        == "provider_directory_profile_selection_observation"
    )
    assert (
        selection.ProviderDirectoryProfileSelectionObservation.authority_revision
        .property.columns[0]
        .autoincrement
        is False
    )
