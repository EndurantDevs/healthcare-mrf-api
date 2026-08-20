# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from types import SimpleNamespace

import pytest

from tests import provider_directory_retained_benchmark as benchmark


def test_retained_benchmark_records_absolute_and_phase_contracts() -> None:
    observations = SimpleNamespace(
        seconds_by_phase={
            "acquisition": [2.0],
            "post_validation_to_publication": [10.0],
            "semantic": [6.0],
            "publication": [3.0],
            "cleanup": [1.0],
        },
        stage_relation_snapshots=[
            {"stage_a": {"oid": 101}},
            {"stage_b": {"oid": 202}},
        ],
    )
    stage = SimpleNamespace(
        phase_metrics={
            "canonical_materialization_seconds": 4.0,
            "fact_decode_copy_seconds": 1.0,
            "plan_materialize_copy_seconds": 0.5,
            "identity_proof_merge_seconds": 0.25,
            "deferred_index_seconds": 1.5,
            "npi_merge_summary_seconds": 0.75,
            "canonical_rows_per_second": 85.0,
            "npi_evidence_rows_per_second": 42.5,
        }
    )

    input_identity = {
        "contract_id": "healthporta.uhc.retained-summary-input.v1",
        "complete": True,
        "source_id": "source-1",
        "catalog_set_sha256": "a" * 64,
        "semantic_contract_id": "healthporta.uhc.semantic-facts.v3",
        "semantic_contract_version": 3,
        "canonical_contract_id": "healthporta.uhc.provider-directory-canonical.v2",
        "semantic_build_ids": ["1" * 64],
        "semantic_set_sha256": "2" * 64,
        "input_set_sha256": "b" * 64,
        "layout_set_sha256": "3" * 64,
        "encoder_digest": "4" * 64,
        "quarantine_proof_sha256": "5" * 64,
        "count_by_field": {"raw_provider_records": 340},
        "count_by_category": {"accepted": {"Practitioner": 340}},
        "input_sha256": "6" * 64,
    }
    publication_lineage = {
        "source_id": "source-1",
        "endpoint_id": "endpoint-1",
        "dataset_id": "dataset-1",
        "acquisition_root_run_id": "run-1",
        "import_run_id": "run-1",
        "selected_resources": ["Practitioner"],
    }
    publication_identity = {
        "contract_id": "healthporta.uhc.retained-publication.v1",
        "complete": True,
        "source_id": "source-1",
        "dataset_id": "dataset-1",
        "acquisition_root_run_id": "run-1",
        "catalog_set_sha256": "a" * 64,
        "semantic_contract_id": "healthporta.uhc.semantic-facts.v3",
        "semantic_contract_version": 3,
        "semantic_set_sha256": "2" * 64,
        "canonical_contract_id": "healthporta.uhc.provider-directory-canonical.v2",
        "summary_input_sha256": "6" * 64,
    }
    publication_receipt = {
        "publication_metadata_sha256": "e" * 64,
        "publication_identity": publication_identity,
    }
    source_summary = {
        "contract_id": "healthporta.provider-directory.source-summary.v1",
        "contract_version": 1,
        "source_ids": ["source-1"],
        "endpoint_id": "endpoint-1",
        "dataset_id": "dataset-1",
        "acquisition_root_run_id": "run-1",
        "semantic_contract_id": "healthporta.uhc.semantic-facts.v3",
        "summary_sha256": "f" * 64,
    }
    publication_proof = {
        "lineage": publication_lineage,
        "canonical_proof": {"proof_sha256": "d" * 64},
        "receipt": publication_receipt,
        "source_summary": source_summary,
    }
    live_relation_state = {
        "provider_directory_endpoint_dataset": {
            "oid": 303,
            "persistence": "p",
            "index_count": 1,
            "indexes_valid_ready_live": True,
        }
    }
    cleanup_proof = {
        "canonical_stages_removed": True,
        **benchmark._stage_lifecycle_proof(observations),
        **benchmark._live_relation_lifecycle_proof(
            live_relation_state, live_relation_state
        ),
    }

    event = benchmark._benchmark_event_map(
        observations,
        {"official_catalog_files": 2, "official_files_reused": 2},
        input_identity,
        {
            "dataset_hash": "c" * 64,
            "resource_count": 340,
            "resource_counts": {"Practitioner": 340},
            "status": "published",
            "is_current": True,
        },
        publication_proof,
        cleanup_proof,
        stage,
        12.0,
        0.5,
    )

    assert event["correctness"]["performance_contract"] == {
        "maximum_publication_seconds": 1_800,
        "minimum_resources_per_second": 18_920,
    }
    assert event["correctness"]["dataset"]["canonical_proof"] == {
        "proof_sha256": "d" * 64
    }
    assert event["correctness"]["input_identity"] == input_identity
    assert event["correctness"]["publication"]["lineage"] == publication_lineage
    assert event["correctness"]["publication"]["receipt"] == publication_receipt
    assert event["correctness"]["publication"]["source_summary"] == source_summary
    assert event["correctness"]["cleanup"] == cleanup_proof
    assert {
        name: event["metrics"][name]
        for name in (
            "resources_per_second",
            "fact_decode_copy_seconds",
            "plan_materialize_copy_seconds",
            "identity_proof_merge_seconds",
            "deferred_index_seconds",
            "npi_merge_summary_seconds",
            "canonical_rows_per_second",
            "npi_evidence_rows_per_second",
        )
    } == {
        "resources_per_second": 34.0,
        "fact_decode_copy_seconds": 1.0,
        "plan_materialize_copy_seconds": 0.5,
        "identity_proof_merge_seconds": 0.25,
        "deferred_index_seconds": 1.5,
        "npi_merge_summary_seconds": 0.75,
        "canonical_rows_per_second": 85.0,
        "npi_evidence_rows_per_second": 42.5,
    }


@pytest.mark.asyncio
async def test_retained_benchmark_requires_database_quiescence(monkeypatch) -> None:
    class Connection:
        closed = False

        async def fetchrow(self, query):
            assert "pg_catalog.pg_stat_activity" in query
            assert "pg_catalog.pg_locks" in query
            return {"other_client_sessions": 0, "ungranted_locks": 0}

        async def close(self):
            self.closed = True

    connection = Connection()

    async def connect(**kwargs):
        assert kwargs["database"] == "retained_benchmark_test"
        return connection

    monkeypatch.setenv("HLTHPRT_DB_HOST", "127.0.0.1")
    monkeypatch.setenv("HLTHPRT_DB_PORT", "5440")
    monkeypatch.setenv("HLTHPRT_DB_USER", "postgres")
    monkeypatch.setenv("HLTHPRT_DB_PASSWORD", "postgres")
    modules = SimpleNamespace(
        importer=SimpleNamespace(
            asyncpg=SimpleNamespace(connect=connect),
        )
    )

    assert await benchmark._assert_database_quiescent(
        modules, "retained_benchmark_test"
    ) == {
        "other_client_sessions": 0,
        "ungranted_locks": 0,
        "database_quiescent": True,
    }
    assert connection.closed is True
