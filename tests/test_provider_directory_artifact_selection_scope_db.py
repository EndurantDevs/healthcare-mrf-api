# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""PostgreSQL resource proof for explicit artifact dataset selection."""

from __future__ import annotations

import pytest

from api.provider_directory_source_catalog_outcomes import (
    _canonical_validated_datasets_by_source_id,
)
from tests.test_provider_directory_artifact_eligibility_db import (
    DATASET_HASH,
    ENDPOINT_ID,
    _candidate_database,
    _set_all_source_profiles,
    importer,
)
from tests.test_provider_directory_dataset_artifact_db import (
    _dataset_database,
    _insert_validated_shared_dataset,
)


async def _insert_endpoint_scope_probe_rows(database, schema: str) -> None:
    """Insert one serving candidate and many unrelated incumbents."""

    await database.status(
        f"ALTER TABLE {schema}.provider_directory_endpoint_dataset "
        "ADD COLUMN import_run_id varchar(64), "
        "ADD COLUMN previous_dataset_id varchar(96), "
        "ADD COLUMN validated_at timestamp, "
        "ADD COLUMN published_at timestamp;"
    )
    await database.status(
        f"CREATE INDEX provider_directory_endpoint_dataset_endpoint_idx "
        f"ON {schema}.provider_directory_endpoint_dataset (endpoint_id);"
    )
    await database.status(
        f"INSERT INTO {schema}.provider_directory_source "
        "(source_id, endpoint_id, metadata_json) VALUES "
        "('source_plain', 'serving_plain', '{}'::jsonb);"
    )
    await database.status(
        f"""
        INSERT INTO {schema}.provider_directory_endpoint_dataset (
            dataset_id, endpoint_id, acquisition_root_run_id, dataset_hash,
            status, is_current, resource_count, publication_metadata_json
        ) VALUES (
            'dataset_plain', 'serving_plain', 'root_plain', :dataset_hash,
            :validated, false, 1,
            '{{"source_ids":["source_plain"]}}'::jsonb
        );
        """,
        dataset_hash=DATASET_HASH,
        validated=importer.ENDPOINT_DATASET_VALIDATED,
    )
    await database.status(
        f"""
        INSERT INTO {schema}.provider_directory_endpoint_dataset (
            dataset_id, endpoint_id, status, is_current,
            publication_metadata_json
        )
        SELECT 'unrelated-' || sequence_id, 'unrelated-' || sequence_id,
               :published, true,
               jsonb_build_object(
                   'source_ids', jsonb_build_array('unrelated-' || sequence_id),
                   'synthetic_padding', repeat('x', 8192)
               )
          FROM generate_series(1, 2000) AS sequence_id;
        """,
        published=importer.ENDPOINT_DATASET_PUBLISHED,
    )
    await database.status(
        f"ANALYZE {schema}.provider_directory_endpoint_dataset;"
    )


def _plan_nodes(plan: dict[str, object]):
    """Yield every node in one PostgreSQL JSON plan."""

    yield plan
    for child in plan.get("Plans", []):
        yield from _plan_nodes(child)


def _assert_bounded_plan(plan: dict[str, object]) -> None:
    """Require bounded indexed dataset reads without temporary writes."""

    dataset_scan_list = [
        plan_node
        for plan_node in _plan_nodes(plan)
        if plan_node.get("Relation Name")
        == "provider_directory_endpoint_dataset"
    ]
    assert dataset_scan_list
    assert all(
        plan_node["Node Type"] != "Seq Scan"
        for plan_node in dataset_scan_list
    )
    assert sum(
        plan_node.get("Actual Rows", 0) * plan_node.get("Actual Loops", 0)
        for plan_node in dataset_scan_list
    ) < 100
    assert sum(
        plan_node.get("Temp Written Blocks", 0)
        for plan_node in _plan_nodes(plan)
    ) == 0
    assert any(
        plan_node.get("Index Name")
        == "provider_directory_endpoint_dataset_endpoint_idx"
        for plan_node in dataset_scan_list
    )


@pytest.mark.asyncio
async def test_requested_sources_bound_artifact_dataset_scans(monkeypatch):
    """Keep production selection off unrelated endpoint datasets."""

    async with _candidate_database(monkeypatch) as (database, schema):
        await _set_all_source_profiles(
            database,
            schema,
            importer.PROVIDER_DIRECTORY_TWIN_ROOT_VERIFIED,
        )
        await _insert_endpoint_scope_probe_rows(database, schema)
        selection_sql = importer._provider_directory_artifact_dataset_selection_sql(
            ["source_a", "source_plain"],
            should_select_validated_candidates=True,
        )
        parameters_by_name = {
            "source_ids": ["source_a", "source_plain"],
            "published_status": importer.ENDPOINT_DATASET_PUBLISHED,
            "validated_status": importer.ENDPOINT_DATASET_VALIDATED,
            "select_validated_candidates": True,
        }
        selection_entry_list = await database.all(
            selection_sql,
            **parameters_by_name,
        )
        explain_entry_list = await database.all(
            f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {selection_sql}",
            **parameters_by_name,
        )

        assert {
            (
                selection_entry._mapping["source_id"],
                selection_entry._mapping["dataset_id"],
                selection_entry._mapping["endpoint_id"],
            )
            for selection_entry in selection_entry_list
        } == {
            ("source_a", "dataset_exact_matched", ENDPOINT_ID),
            ("source_plain", "dataset_plain", "serving_plain"),
        }
        plan = explain_entry_list[0]._mapping["QUERY PLAN"][0]["Plan"]
        _assert_bounded_plan(plan)


@pytest.mark.asyncio
async def test_global_current_selection_hashes_only_bound_current_data(monkeypatch):
    """Do not hash candidates or current datasets outside the source graph."""
    async with _dataset_database(monkeypatch) as (database, schema):
        await _insert_validated_shared_dataset(database, schema)
        await database.status(
            f"INSERT INTO {schema}.provider_directory_api_endpoint "
            "(endpoint_id) VALUES ('endpoint_current_probe');"
        )
        await database.status(
            f"""
            INSERT INTO {schema}.provider_directory_endpoint_dataset (
                dataset_id, endpoint_id, acquisition_root_run_id, dataset_hash,
                status, is_current, resource_count, published_at,
                publication_metadata_json
            ) VALUES (
                'dataset_current_probe', 'endpoint_current_probe',
                'root_current_probe', repeat('f', 64), :published, true, 1,
                now(), jsonb_build_object(
                    'source_ids', jsonb_build_array('source_current_probe'),
                    'unselected_current_probe', true
                )
            );
            """,
            published=importer.ENDPOINT_DATASET_PUBLISHED,
        )
        await database.status(
            f"UPDATE {schema}.provider_directory_endpoint_dataset "
            "SET publication_metadata_json = CAST(publication_metadata_json::jsonb "
            "|| jsonb_build_object('unselected_candidate_probe', true) AS json) "
            "WHERE dataset_id = 'dataset_candidate';"
        )
        await database.status(
            f"ALTER FUNCTION {schema}.provider_directory_subset_payload_sha256(jsonb) "
            "RENAME TO provider_directory_subset_payload_sha256_original;"
        )
        await database.status(
            f"""CREATE FUNCTION {schema}.provider_directory_subset_payload_sha256(candidate jsonb)
            RETURNS text LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE
            AS $function$ BEGIN
                IF candidate ?| ARRAY[
                    'unselected_candidate_probe', 'unselected_current_probe'
                ] THEN RAISE EXCEPTION 'unselected_dataset_evaluated'; END IF;
                RETURN {schema}.provider_directory_subset_payload_sha256_original(candidate);
            END; $function$;"""
        )

        fence = await importer._resolve_provider_directory_artifact_datasets(
            None,
            should_select_validated_candidates=False,
        )

        assert fence.source_ids == ["source_primary", "source_sibling"]
        assert {dataset.dataset_id for dataset in fence.datasets} == {
            "dataset_shared"
        }


@pytest.mark.asyncio
async def test_candidate_catalog_skips_unselected_current_dataset(
    monkeypatch,
):
    """Do not hash the incumbent when catalog selection keeps its candidate."""

    async with _dataset_database(monkeypatch) as (database, schema):
        await _insert_validated_shared_dataset(database, schema)
        await database.status(
            f"UPDATE {schema}.provider_directory_endpoint_dataset "
            "SET publication_metadata_json = CAST(publication_metadata_json::jsonb "
            "|| jsonb_build_object('unselected_current_probe', true) AS json) "
            "WHERE dataset_id = 'dataset_shared';"
        )
        await database.status(
            f"ALTER FUNCTION {schema}.provider_directory_subset_payload_sha256(jsonb) "
            "RENAME TO provider_directory_subset_payload_sha256_original;"
        )
        await database.status(
            f"""CREATE FUNCTION {schema}.provider_directory_subset_payload_sha256(candidate jsonb)
            RETURNS text LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE
            AS $function$ BEGIN
                IF candidate ? 'unselected_current_probe' THEN RAISE EXCEPTION
                    'unselected_current_dataset_evaluated'; END IF;
                RETURN {schema}.provider_directory_subset_payload_sha256_original(candidate);
            END; $function$;"""
        )

        selected = await _canonical_validated_datasets_by_source_id(
            ["source_primary"]
        )

        assert selected["source_primary"].dataset_id == "dataset_candidate"
        assert (
            selected["source_primary"].expected_incumbent_dataset_id
            == "dataset_shared"
        )
