# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Migration and fixture support for compact terminal publication."""

from __future__ import annotations

import importlib
import importlib.util
import json
from pathlib import Path
import re
import time

from tests.provider_directory_subset_completion_pg_setup import (
    run_subset_migration,
)
from tests.provider_directory_subset_completion_pg_source_concurrency import (
    _relation_trigger_states,
    _set_relation_triggers_enabled,
)


MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "alembic/versions/20260818020000_provider_directory_terminal_publication_compact_guard.py"
)
importer = importlib.import_module("process.provider_directory_fhir")


class _Recorder:
    def __init__(self) -> None:
        self.statements: list[str] = []

    def execute(self, statement: str) -> None:
        self.statements.append(statement)


def _load():
    spec = importlib.util.spec_from_file_location(
        "provider_directory_terminal_publication_compact_guard_migration",
        MIGRATION_PATH,
    )
    assert spec is not None and spec.loader is not None
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    return migration


def _raised_errors(sql: str) -> set[str]:
    return set(re.findall(r"RAISE EXCEPTION\s+'([^']+)'", sql))


def test_compact_guard_removes_only_duplicate_replay_validation() -> None:
    migration = _load()
    predecessor = migration._endpoint_guard_sql("compact_guard_test", compact=False)
    compact = migration._endpoint_guard_sql("compact_guard_test", compact=True)

    assert predecessor.count(migration._DUPLICATE_REPLAY_ERROR) == 1
    assert migration._DUPLICATE_REPLAY_ERROR not in compact
    assert _raised_errors(compact) == _raised_errors(predecessor) - {
        migration._DUPLICATE_REPLAY_ERROR
    }
    for marker in (
        "provider_directory_subset_dataset_content_invalid",
        "provider_directory_subset_matched_twin_invalid",
        "provider_directory_subset_published_source_invalid",
        "provider_directory_subset_source_isolation_invalid",
    ):
        assert marker in compact
    assert compact.count("LOCK TABLE") == predecessor.count("LOCK TABLE")
    assert "content_proof_admission_kind = 'generic'" in compact
    assert "'required_root_count', 1" in compact
    assert "NEW.completion_proof_sha256" in compact
    assert migration._SOURCE_SUMMARY_CONTRACT_ID in compact
    assert migration._SOURCE_SUMMARY_SEMANTIC_CONTRACT_ID in compact
    for column_name in migration._admission()._SEAL_COLUMNS:
        assert f"OLD.{column_name}" in compact
        assert f"NEW.{column_name} IS NULL" in compact
        assert f"OLD.{column_name} IS NULL" in compact
    assert "THEN NEW.publication_metadata_json::jsonb" in compact
    assert "ELSE NULL::jsonb" in compact


def test_upgrade_and_downgrade_replace_only_endpoint_guard(monkeypatch) -> None:
    migration = _load()
    recorder = _Recorder()
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "compact_guard_test")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    migration.op = recorder

    migration.upgrade()
    compact = migration._endpoint_guard_sql("compact_guard_test", compact=True)
    predecessor = migration._endpoint_guard_sql("compact_guard_test", compact=False)
    assert migration.down_revision == "20260816020000_address_evidence_alias"
    assert recorder.statements[0].startswith("LOCK TABLE")
    assert compact in recorder.statements
    assert predecessor not in recorder.statements
    assert sum("REVOKE ALL ON FUNCTION" in sql for sql in recorder.statements) == 1
    assert not any("ALTER COLUMN" in sql for sql in recorder.statements)

    recorder.statements.clear()
    migration.downgrade()
    assert predecessor in recorder.statements
    assert compact not in recorder.statements


def test_function_fence_binds_replay_body_security_and_acl() -> None:
    migration = _load()
    fence = migration._function_body_fence_sql("compact_guard_test", compact=True)
    assert migration._normalized_body_md5(
        migration._endpoint_guard_sql("compact_guard_test", compact=True)
    ) in fence
    assert migration._normalized_body_md5(
        migration._admission()._replay_guard_function_sql("compact_guard_test")
    ) in fence
    for marker in (
        "guard_pd_endpoint_dataset_subset_replay_evidence",
        "function_row.prosecdef IS TRUE",
        "has_function_privilege",
        "regexp_replace",
    ):
        assert marker in fence


async def _run_migration(scenario, migration, action: str) -> None:
    async with scenario.connection.transaction():
        await run_subset_migration(migration, action, scenario.connection)


def _publish_sql(scenario) -> str:
    return f"""
        UPDATE {scenario.quoted_schema}.provider_directory_endpoint_dataset
           SET status = 'published', is_current = true,
               published_at = pg_catalog.transaction_timestamp()
         WHERE dataset_id = 'dataset-candidate'
    """


async def _rollback_publish(scenario, *, timeout: str = "1s") -> float:
    transaction = scenario.connection.transaction()
    await transaction.start()
    started = time.monotonic()
    try:
        await scenario.connection.execute(
            f"SET LOCAL statement_timeout = '{timeout}'"
        )
        await scenario.connection.execute(_publish_sql(scenario))
        assert await scenario.connection.fetchval(
            f"SELECT status = 'published' AND is_current "
            f"FROM {scenario.quoted_schema}.provider_directory_endpoint_dataset "
            "WHERE dataset_id = 'dataset-candidate'"
        )
    finally:
        await transaction.rollback()
    return time.monotonic() - started


async def _set_triggers(scenario, relation_name: str, *, enabled: bool):
    trigger_states = await _relation_trigger_states(scenario, relation_name)
    await _set_relation_triggers_enabled(
        scenario,
        relation_name,
        trigger_states,
        enabled=enabled,
    )
    return trigger_states


async def _restore_triggers(scenario, relation_name: str, trigger_states) -> None:
    await _set_relation_triggers_enabled(
        scenario,
        relation_name,
        trigger_states,
        enabled=True,
    )


async def _pad_large_metadata(
    scenario,
    dataset_table: str,
    source_table: str,
) -> None:
    """Match the incompressible live dataset and source metadata sizes."""

    dataset_triggers = await _set_triggers(
        scenario, "provider_directory_endpoint_dataset", enabled=False
    )
    source_triggers = await _set_triggers(
        scenario, "provider_directory_source", enabled=False
    )
    try:
        for table, path, count, predicate in (
            (
                dataset_table,
                "{resource_diagnostics,test_padding}",
                360000,
                "dataset_id = 'dataset-candidate'",
            ),
            (source_table, "{test_padding}", 30000, "source_id = 'synthetic-source'"),
        ):
            column = (
                "publication_metadata_json::jsonb"
                if table == dataset_table
                else "metadata_json"
            )
            await scenario.connection.execute(
                f"UPDATE {table} SET {column.split('::', 1)[0]} = "
                f"pg_catalog.jsonb_set({column}, '{path}', pg_catalog.to_jsonb(("
                "SELECT pg_catalog.string_agg(pg_catalog.md5(item::text), '' "
                f"ORDER BY item) FROM pg_catalog.generate_series(1, {count}) item"
                f")), true) WHERE {predicate}"
            )
    finally:
        await _restore_triggers(
            scenario,
            "provider_directory_endpoint_dataset",
            dataset_triggers,
        )
        await _restore_triggers(
            scenario,
            "provider_directory_source",
            source_triggers,
        )


def _source_summary_counts(resource_counts: dict[str, int]) -> dict[str, int]:
    return {
        "distinct_npis": 0,
        "individual_practitioners": resource_counts.get("Practitioner", 0),
        "organization_resources": resource_counts.get("Organization", 0),
        "address_records": 0,
        "addressed_locations": 0,
        "geocoded_locations": 0,
        "practitioner_role_resources": resource_counts.get("PractitionerRole", 0),
        "network_plan_links": 0,
        "organization_affiliation_links": resource_counts.get(
            "OrganizationAffiliation", 0
        ),
    }


def _generic_content_proof(candidate_record, publication_metadata, completion_proof):
    stored_proof = publication_metadata[
        importer.PROVIDER_DIRECTORY_CONTENT_PROOF_METADATA_KEY
    ]
    return importer.EndpointDatasetContentProof(
        dataset_hash=stored_proof["dataset_hash"],
        resource_count=stored_proof["resource_count"],
        resource_hashes=dict(stored_proof["resource_hashes"]),
        resource_counts=dict(stored_proof["resource_counts"]),
        source_metrics=dict(stored_proof.get("source_metrics") or {}),
        proof_metadata=dict(stored_proof),
        completion_proof=completion_proof,
        completion_proof_sha256=candidate_record["completion_proof_sha256"],
    )


def _generic_candidate(candidate_record, publication_metadata):
    return importer.EndpointDatasetCandidate(
        endpoint_id=candidate_record["endpoint_id"],
        dataset_id="dataset-candidate",
        acquisition_root_run_id=candidate_record["acquisition_root_run_id"],
        source_ids=tuple(publication_metadata["source_ids"]),
        selected_resources=tuple(publication_metadata["selected_resources"]),
        expected_resources=tuple(publication_metadata["expected_resources"]),
        import_run_id=None,
        previous_dataset_id=None,
        completion_proof_required_version=3,
    )


def _generic_seal(candidate_record):
    publication_metadata = json.loads(candidate_record["metadata"])
    completion_proof = json.loads(candidate_record["completion_proof"])
    content_proof = _generic_content_proof(
        candidate_record,
        publication_metadata,
        completion_proof,
    )
    candidate = _generic_candidate(candidate_record, publication_metadata)
    publication_metadata[
        importer.PROVIDER_DIRECTORY_OUTCOME_RESOURCE_COUNTS_METADATA_KEY
    ] = importer._outcome_resource_count_proof(candidate, content_proof)
    publication_metadata[importer.SOURCE_SUMMARY_METADATA_KEY] = (
        importer._build_endpoint_dataset_source_summary(
            candidate,
            content_proof,
            _source_summary_counts(content_proof.resource_counts),
            candidate_record["acquisition_root_run_id"],
        )
    )
    publication_metadata[importer.PROVIDER_DIRECTORY_SUBSET_ADMISSION_SUMMARY_KEY] = (
        importer._subset_admission_summary_projection(
            publication_metadata,
            completion_proof,
            candidate_record["completion_proof_sha256"],
        )
    )
    seal = importer.admission_seal_from_validated_metadata(
        importer._subset_admission_seal_metadata(
            publication_metadata,
            (completion_proof, candidate_record["completion_proof_sha256"]),
        )
    )
    selection_receipt = importer._artifact_selection_receipt(publication_metadata)
    assert seal is not None and selection_receipt is not None
    return publication_metadata, seal, selection_receipt


async def _write_generic_seal(
    scenario,
    dataset_table,
    publication_metadata,
    seal,
    selection_receipt,
) -> None:
    trigger_states = await _set_triggers(
        scenario, "provider_directory_endpoint_dataset", enabled=False
    )
    try:
        await scenario.connection.execute(
            f"""
            UPDATE {dataset_table}
               SET publication_metadata_json = $1::jsonb,
                   publication_metadata_summary_json = $2::jsonb,
                   publication_metadata_sha256 = $3,
                   content_proof_admission_version = $4,
                   content_proof_admission_kind = $5,
                   content_proof_admission_sha256 = $6,
                   content_proof_resource_types = $7::varchar[],
                   artifact_selection_receipt_json = $8::jsonb
             WHERE dataset_id = 'dataset-candidate'
            """,
            json.dumps(publication_metadata),
            json.dumps(seal.metadata_summary),
            seal.metadata_sha256,
            seal.admission_version,
            seal.admission_kind,
            seal.proof_sha256,
            list(seal.resource_types),
            json.dumps(selection_receipt),
        )
    finally:
        await _restore_triggers(
            scenario,
            "provider_directory_endpoint_dataset",
            trigger_states,
        )


async def _assert_metadata_sizes(scenario, dataset_table, source_table) -> None:
    sizes = await scenario.connection.fetchrow(
        f"""
        SELECT pg_column_size(publication_metadata_json) AS raw_bytes,
               pg_catalog.octet_length(publication_metadata_json::text)
                   AS raw_text_bytes,
               pg_column_size(publication_metadata_summary_json) AS summary_bytes,
               (SELECT pg_column_size(metadata_json) FROM {source_table}
                 WHERE source_id = 'synthetic-source') AS source_bytes
          FROM {dataset_table}
         WHERE dataset_id = 'dataset-candidate'
        """
    )
    assert sizes["raw_bytes"] > 6_000_000
    assert sizes["raw_text_bytes"] > 11_000_000
    assert sizes["summary_bytes"] < 100_000
    assert sizes["source_bytes"] > 900_000


async def _install_large_generic_seal(scenario):
    dataset_table = (
        f"{scenario.quoted_schema}.provider_directory_endpoint_dataset"
    )
    source_table = f"{scenario.quoted_schema}.provider_directory_source"
    await _pad_large_metadata(scenario, dataset_table, source_table)
    candidate_record = await scenario.connection.fetchrow(
        f"""
        SELECT publication_metadata_json::text AS metadata,
               completion_proof_json::text AS completion_proof,
               completion_proof_sha256, endpoint_id, acquisition_root_run_id
          FROM {dataset_table}
         WHERE dataset_id = 'dataset-candidate'
        """
    )
    publication_metadata, seal, selection_receipt = _generic_seal(
        candidate_record
    )
    await _write_generic_seal(
        scenario,
        dataset_table,
        publication_metadata,
        seal,
        selection_receipt,
    )
    await _assert_metadata_sizes(scenario, dataset_table, source_table)
    return seal, selection_receipt
