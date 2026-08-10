# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import importlib.util
from pathlib import Path

from db.models.provider_directory_uhc_flex_practitioner_publication import (
    ProviderDirectoryUHCFlexPractitionerDataset,
    ProviderDirectoryUHCFlexPractitionerDatasetResource,
)


MIGRATION_PATH = Path(__file__).resolve().parents[1] / "alembic/versions" / (
    "20260810080000_provider_directory_uhc_flex_practitioner_publication.py"
)
GENERIC_GUARD_PATH = Path(__file__).resolve().parents[1] / "alembic/versions" / (
    "20260808190000_provider_directory_subset_completion_proof.py"
)
PUBLICATION_STORE_PATH = Path(__file__).resolve().parents[1] / "process" / (
    "uhc_flex_practitioner_publication_store.py"
)
PUBLICATION_MATERIALIZATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "process/uhc_flex_practitioner_publication_materialization.py"
)


def _migration():
    spec = importlib.util.spec_from_file_location(
        "flex_practitioner_publication_migration",
        MIGRATION_PATH,
    )
    assert spec is not None and spec.loader is not None
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    return migration


def test_publication_revision_follows_shifted_twin_admission() -> None:
    migration = _migration()
    assert migration.revision == (
        "20260810080000_provider_directory_uhc_flex_practitioner_publication"
    )
    assert migration.down_revision == (
        "20260810070000_provider_directory_uhc_flex_practitioner_twin_admission"
    )


def test_publication_uses_companion_provenance_without_replacing_generic_guards() -> None:
    migration = _migration()
    source = MIGRATION_PATH.read_text()
    assert migration._HEADER == (
        "provider_directory_uhc_flex_practitioner_dataset"
    )
    assert migration._PROVENANCE == (
        "provider_directory_uhc_flex_practitioner_dataset_resource"
    )
    assert "guard_tin_npi_connector_endpoint_dataset" not in source
    assert "guard_tin_npi_connector_dataset_resource" not in source
    assert "completion_proof_required_version IS NULL" in (
        migration._valid_function_sql("fhir_twin_test_static")
    )


def test_generic_guard_markers_remain_compatible_with_exact_cohort_rows() -> None:
    generic_guard_source = GENERIC_GUARD_PATH.read_text()
    store_source = PUBLICATION_STORE_PATH.read_text()
    materialization_source = PUBLICATION_MATERIALIZATION_PATH.read_text()

    assert "dataset.completion_proof_required_version = 3" in generic_guard_source
    assert "resource.acquired_resource_sha256 IS NOT NULL" in generic_guard_source
    assert "completion_proof_required_version, completion_proof_json" in store_source
    assert "CAST(:metadata_json AS jsonb), NULL, NULL, NULL" in store_source
    assert "input.payload_hash, input.payload_json, NULL" in materialization_source


def test_readiness_binds_admission_source_content_and_subset_semantics() -> None:
    migration = _migration()
    sql = " ".join(
        migration._valid_function_sql("fhir_twin_test_static").split()
    )
    for required_fragment in (
        "admission.publication_authority IS TRUE",
        "admission.semantic_projection_as_of = header.semantic_projection_as_of",
        "admission.operation_key = header.operation_key",
        "candidate.error_count = 0",
        "candidate.status = 'sealed'",
        "official_dataset.dataset_id = cohort.official_dataset_id",
        "official_dataset.acquisition_root_run_id = cohort.official_acquisition_root_run_id",
        "source.metadata_json::jsonb -> 'provider_directory_acquisition_enabled' = 'false'::jsonb",
        "resource.acquired_resource_sha256 IS NOT NULL",
        "provenance.candidate_acquisition_id",
        "header.endpoint_collection_complete IS FALSE",
        "header.endpoint_complete IS FALSE",
        "pg_catalog.string_agg",
    ):
        assert required_fragment in sql
    readiness_sql = " ".join(
        migration._ready_function_sql("fhir_twin_test_static").split()
    )
    assert "official_dataset.status = 'published'" in readiness_sql
    assert "official_dataset.is_current IS TRUE" in readiness_sql
    assert "NEW.status = 'published'" in " ".join(
        migration._header_guard_sql("fhir_twin_test_static").split()
    )
    metadata_sql = migration._metadata_sql("header", "admission")
    assert "'selected_resources'" in metadata_sql
    assert "'expected_resources'" in metadata_sql
    assert "'Practitioner'" in metadata_sql
    assert "'cohort_complete', true" in metadata_sql
    assert "'endpoint_collection_complete', false" in metadata_sql
    assert "'endpoint_complete', false" in metadata_sql


def test_publication_models_expose_current_header_and_exact_resource_provenance() -> None:
    header = ProviderDirectoryUHCFlexPractitionerDataset.__table__
    provenance = ProviderDirectoryUHCFlexPractitionerDatasetResource.__table__

    assert tuple(column.name for column in header.primary_key.columns) == (
        "dataset_id",
    )
    assert tuple(column.name for column in provenance.primary_key.columns) == (
        "dataset_id",
        "resource_id",
    )
    assert {
        "admission_id",
        "candidate_acquisition_id",
        "cohort_id",
        "dataset_intent_id",
        "semantic_projection_as_of",
        "operation_key",
        "dataset_hash",
        "resource_count",
        "source_authority_id",
    }.issubset(set(header.c.keys()))
    assert {
        "requested_npi",
        "candidate_acquisition_id",
        "payload_hash",
        "acquired_resource_sha256",
    }.issubset(set(provenance.c.keys()))
    assert any(
        index["name"] == "pd_uhc_flex_practitioner_dataset_current_idx"
        and index["unique"] is True
        for index in (
            ProviderDirectoryUHCFlexPractitionerDataset.__my_additional_indexes__
        )
    )


def test_publication_guards_are_immutable_truncate_and_downgrade_closed() -> None:
    migration = _migration()
    header_guard = migration._header_guard_sql("fhir_twin_test_static")
    resource_guard = migration._provenance_guard_sql(
        "fhir_twin_test_static"
    )
    endpoint_guard = migration._endpoint_guard_sql(
        "fhir_twin_test_static"
    )
    source = MIGRATION_PATH.read_text()
    assert "dataset_truncate_forbidden" in header_guard
    assert "dataset_delete_forbidden" in header_guard
    assert "resource_truncate_forbidden" in resource_guard
    assert "resource_immutable" in resource_guard
    assert "endpoint_drift" in endpoint_guard
    assert "endpoint_truncate_forbidden" in endpoint_guard
    assert "publication_downgrade_blocked" in source
