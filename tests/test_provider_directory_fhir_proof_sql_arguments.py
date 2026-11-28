# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Source call sites select exact text arguments without changing proof semantics."""

import importlib

import pytest

fhir = importlib.import_module("process.provider_directory_fhir")


def test_seal_predicate_selects_the_exact_digest_signature():
    sql = fhir._artifact_admission_seal_shape_valid_sql("candidate")
    assert "candidate.content_proof_admission_kind::text," in sql
    assert "candidate.content_proof_admission_sha256::text," in sql
    assert "candidate.content_proof_admission_version," in sql
    assert "candidate.content_proof_resource_types" in sql


def test_sealed_metadata_update_keeps_digest_and_predicate_typed():
    sql = fhir._sealed_publication_proof_sql("candidate", "digest", "proof", "identity")
    assert sql.count("dataset.content_proof_admission_kind::text,") == 2
    assert sql.count("dataset.content_proof_admission_sha256::text,") == 2
    assert "UPDATE candidate AS dataset" in sql
    assert "WHERE identity" in sql


@pytest.mark.parametrize("marker", [None, "admitted"])
def test_stored_proof_selects_exact_identity_arguments_in_every_branch(marker):
    sql = fhir._artifact_stored_content_proof_sql("candidate", "metadata", "proof", marker)
    for column in ("dataset_id", "endpoint_id", "acquisition_root_run_id", "dataset_hash"):
        assert f"candidate.{column}::text," in sql
    assert "candidate.resource_count," in sql
    if marker is not None:
        assert "ELSE (" in sql


def test_legacy_proof_casts_the_complete_root_run_expression():
    expression = "COALESCE(dataset.acquisition_root_run_id,dataset.import_run_id)"
    sql = fhir._artifact_legacy_content_proof_valid_sql("metadata", "proof", expression)
    assert f"({expression})::text," in sql
    for column in ("dataset_id", "endpoint_id", "dataset_hash"):
        assert f"dataset.{column}::text," in sql
    assert "dataset.resource_count," in sql
