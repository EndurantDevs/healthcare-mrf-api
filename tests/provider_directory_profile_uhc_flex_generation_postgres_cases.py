# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""PostgreSQL generation proof cases for dataset-scoped Profile enrichment."""

from __future__ import annotations

import json
from typing import Any

import pytest

from process import provider_directory_profile as profile
from process.provider_directory_rooted_graph_source_contract import (
    PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ID,
)
from process.uhc_flex_practitioner_contract import UHC_FLEX_PRACTITIONER_SOURCE_ID
from tests.test_provider_directory_profile_affiliations_db import _profile_database
from tests.test_provider_directory_profile_uhc_flex_postgres import (
    FOREIGN_GRAPH_DATASET_ID,
    FOREIGN_GRAPH_NPI,
    GRAPH_DATASET_ID,
    GRAPH_NPI,
    NPI,
    OFFICIAL_SOURCE_ID,
    OLD_GRAPH_DATASET_ID,
    OLD_GRAPH_NPI,
    _build_first_generation,
    _build_replacement_without_flex_row,
    _evidence_sql,
    _ref,
    _seed_graph_dataset,
    _seed_graph_typed_leak_rows,
    _seed_sources_and_rows,
)


@pytest.mark.asyncio
async def test_flex_profile_uses_exact_dataset_and_removes_stale_source(
    monkeypatch,
):
    async with _profile_database(monkeypatch) as (database, schema):
        await _seed_sources_and_rows(database, schema)
        await _build_first_generation(database, schema)

        flex_evidence = await database.all(
            f"""
            SELECT fact_type, value_json
              FROM {_ref(schema, 'profile_evidence')}
             WHERE source_id = :source_id AND npi = :npi
             ORDER BY fact_type, value_json::text;
            """,
            source_id=UHC_FLEX_PRACTITIONER_SOURCE_ID,
            npi=NPI,
        )
        assert flex_evidence
        assert "Stale Typed Flex" not in json.dumps(
            [dict(evidence_record._mapping) for evidence_record in flex_evidence],
            default=str,
        )
        assert any(
            evidence_record._mapping["fact_type"] == "language"
            for evidence_record in flex_evidence
        )

        first_profile = await database.first(
            f"SELECT profile_json FROM {_ref(schema, 'profile')} WHERE npi = :npi;",
            npi=NPI,
        )
        first_json = first_profile._mapping["profile_json"]
        assert first_json["source_count"] == 2
        assert first_json["independent_source_count"] == 1
        name_item = first_json["facts"]["name"]["items"][0]
        assert name_item["source_count"] == 2
        assert name_item["independent_source_count"] == 1

        await _build_replacement_without_flex_row(database, schema)
        await _assert_flex_evidence_removed(database, schema)


async def _assert_flex_evidence_removed(database: Any, schema: str) -> None:
    flex_count = await database.scalar(
        f"SELECT count(*) FROM {_ref(schema, 'profile_evidence_v2')} "
        "WHERE source_id = :source_id;",
        source_id=UHC_FLEX_PRACTITIONER_SOURCE_ID,
    )
    assert flex_count == 0
    second_profile = await database.first(
        f"SELECT profile_json FROM {_ref(schema, 'profile_v2')} WHERE npi = :npi;",
        npi=NPI,
    )
    second_json = second_profile._mapping["profile_json"]
    assert second_json["source_count"] == 1
    assert second_json["independent_source_count"] == 1
    assert "language" not in second_json["facts"]


@pytest.mark.asyncio
async def test_rooted_profile_reads_only_selected_graph_rows(monkeypatch):
    async with _profile_database(monkeypatch) as (database, schema):
        await _seed_sources_and_rows(database, schema)
        await _seed_graph_dataset(
            database,
            schema,
            dataset_id=GRAPH_DATASET_ID,
            marker="selected-graph",
            npi=GRAPH_NPI,
        )
        await _seed_graph_dataset(
            database,
            schema,
            dataset_id=FOREIGN_GRAPH_DATASET_ID,
            marker="foreign-graph",
            npi=FOREIGN_GRAPH_NPI,
        )
        await _seed_graph_typed_leak_rows(database, schema)
        await database.status(
            profile.profile_evidence_table_sql(
                schema,
                "profile_evidence_graph",
                logged=True,
            )
        )
        await database.status(
            _evidence_sql(schema, "profile_evidence_graph"),
            source_ids=[PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ID],
            dataset_ids=[GRAPH_DATASET_ID],
            profile_as_of="2026-08-10",
        )
        evidence_records = await _graph_evidence_records(database, schema)
        _assert_selected_graph_evidence(evidence_records)
        await _assert_non_evidence_resources_retained(
            database,
            schema,
            evidence_records,
        )


async def _graph_evidence_records(
    database: Any,
    schema: str,
) -> list[dict[str, Any]]:
    database_rows = await database.all(
        f"""
        SELECT npi, fact_type, dataset_id, resource_type, value_json
          FROM {_ref(schema, 'profile_evidence_graph')}
         ORDER BY fact_type, resource_type, value_json::text;
        """
    )
    return [dict(database_row._mapping) for database_row in database_rows]


def _assert_selected_graph_evidence(
    evidence_records: list[dict[str, Any]],
) -> None:
    assert evidence_records
    assert {record["npi"] for record in evidence_records} == {GRAPH_NPI}
    assert {record["dataset_id"] for record in evidence_records} == {GRAPH_DATASET_ID}
    assert {
        "name",
        "role",
        "specialty",
        "role_identifier",
        "role_context",
        "organization",
        "affiliation",
        "service",
        "endpoint",
    }.issubset({record["fact_type"] for record in evidence_records})
    serialized_evidence = json.dumps(evidence_records, default=str, sort_keys=True)
    assert "selected-graph" in serialized_evidence
    assert "typed-leak" not in serialized_evidence
    assert "foreign-graph" not in serialized_evidence


async def _assert_non_evidence_resources_retained(
    database: Any,
    schema: str,
    evidence_records: list[dict[str, Any]],
) -> None:
    retained_resource_records = await database.all(
        f"""
        SELECT DISTINCT resource_type
          FROM {_ref(schema, 'provider_directory_dataset_resource')}
         WHERE dataset_id = :dataset_id
           AND resource_type IN ('InsurancePlan', 'Location')
         ORDER BY resource_type;
        """,
        dataset_id=GRAPH_DATASET_ID,
    )
    assert [
        resource_record._mapping["resource_type"]
        for resource_record in retained_resource_records
    ] == ["InsurancePlan", "Location"]
    assert not {"insurance_plan", "location"}.intersection(
        {record["fact_type"] for record in evidence_records}
    )


@pytest.mark.asyncio
async def test_v5_to_v6_promotion_refreshes_both_variant_npis(monkeypatch):
    async with _profile_database(monkeypatch) as (database, schema):
        await _seed_sources_and_rows(database, schema)
        await _seed_graph_dataset(
            database,
            schema,
            dataset_id=OLD_GRAPH_DATASET_ID,
            marker="old-graph",
            npi=OLD_GRAPH_NPI,
        )
        await _seed_graph_dataset(
            database,
            schema,
            dataset_id=GRAPH_DATASET_ID,
            marker="selected-graph",
            npi=GRAPH_NPI,
        )
        await _create_promotion_tables(database, schema)
        await _seed_promotion_evidence(database, schema)
        await _materialize_promoted_evidence(database, schema)
        await _assert_promoted_evidence(database, schema)


async def _create_promotion_tables(database: Any, schema: str) -> None:
    for evidence_table in (
        "profile_evidence_old",
        "profile_evidence_delta",
        "profile_evidence_promoted",
    ):
        await database.status(
            profile.profile_evidence_table_sql(
                schema,
                evidence_table,
                logged=True,
            )
        )
    await database.status(
        f"""
        CREATE TABLE {_ref(schema, 'affected_npi')} (
            npi bigint PRIMARY KEY
        );
        """
    )


async def _seed_promotion_evidence(database: Any, schema: str) -> None:
    await database.status(
        _evidence_sql(schema, "profile_evidence_old"),
        source_ids=[
            UHC_FLEX_PRACTITIONER_SOURCE_ID,
            PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ID,
        ],
        dataset_ids=["flex-dataset-v1", OLD_GRAPH_DATASET_ID],
        profile_as_of="2026-08-09",
    )
    await database.status(
        _evidence_sql(schema, "profile_evidence_delta"),
        source_ids=[PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ID],
        dataset_ids=[GRAPH_DATASET_ID],
        profile_as_of="2026-08-10",
    )


async def _materialize_promoted_evidence(database: Any, schema: str) -> None:
    for source_id in (
        UHC_FLEX_PRACTITIONER_SOURCE_ID,
        PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ID,
    ):
        await database.status(
            profile.affected_npi_source_insert_sql(
                evidence_ref=_ref(schema, "profile_evidence_old"),
                affected_npi_ref=_ref(schema, "affected_npi"),
            ),
            source_id=source_id,
        )
    await database.status(
        profile.affected_npi_delta_insert_sql(
            evidence_stage_ref=_ref(schema, "profile_evidence_delta"),
            affected_npi_ref=_ref(schema, "affected_npi"),
        )
    )
    await _copy_promoted_evidence(database, schema)


async def _copy_promoted_evidence(database: Any, schema: str) -> None:
    await database.status(
        profile.copy_existing_evidence_sql(
            source_ref=_ref(schema, "profile_evidence_old"),
            target_ref=_ref(schema, "profile_evidence_promoted"),
        ),
        source_ids=[
            UHC_FLEX_PRACTITIONER_SOURCE_ID,
            PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ID,
        ],
        retained_source_ids=[
            OFFICIAL_SOURCE_ID,
            UHC_FLEX_PRACTITIONER_SOURCE_ID,
            PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ID,
        ],
        profile_as_of="2026-08-10",
    )
    await database.status(
        _evidence_sql(schema, "profile_evidence_promoted"),
        source_ids=[PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ID],
        dataset_ids=[GRAPH_DATASET_ID],
        profile_as_of="2026-08-10",
    )


async def _assert_promoted_evidence(database: Any, schema: str) -> None:
    affected_npi_records = await database.all(
        f"SELECT npi FROM {_ref(schema, 'affected_npi')} ORDER BY npi;"
    )
    assert [
        affected_record._mapping["npi"] for affected_record in affected_npi_records
    ] == sorted((NPI, OLD_GRAPH_NPI, GRAPH_NPI))
    old_source_records = await database.all(
        f"""
        SELECT DISTINCT source_id
          FROM {_ref(schema, 'profile_evidence_old')}
         ORDER BY source_id;
        """
    )
    assert {
        source_record._mapping["source_id"] for source_record in old_source_records
    } == {
        UHC_FLEX_PRACTITIONER_SOURCE_ID,
        PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ID,
    }
    promoted_records = await database.all(
        f"""
        SELECT DISTINCT source_id, dataset_id, npi
          FROM {_ref(schema, 'profile_evidence_promoted')}
         ORDER BY source_id, dataset_id, npi;
        """
    )
    assert {
        (
            promoted_record._mapping["source_id"],
            promoted_record._mapping["dataset_id"],
            promoted_record._mapping["npi"],
        )
        for promoted_record in promoted_records
    } == {
        (
            PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ID,
            GRAPH_DATASET_ID,
            GRAPH_NPI,
        )
    }
