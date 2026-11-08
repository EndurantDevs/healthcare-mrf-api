# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import pytest

from process.provider_directory_generic_source_adapters import completed_source_scope
from process.provider_directory_generic_source_policy import source_transaction
from process.provider_directory_proof_store import (
    PROVIDER_DIRECTORY_PROOF_SHARD_TABLE,
)
from tests.test_provider_directory_proof_store_db import (
    DATASET_ID,
    ROOT_RUN_ID,
    _active_resource_count,
    _candidate,
    _proof_database,
    importer,
)


def _synthetic_practitioner_rows(
    resource_id_prefix: str,
    npi_start: int,
) -> list[dict[str, object]]:
    return [
        {
            "source_id": "source-a",
            "resource_id": f"{resource_id_prefix}-{index}",
            "npi": npi_start + index,
            "addresses": [{"city": "Chicago"}],
        }
        for index in range(8)
    ]


async def _legacy_counts_by_table(database, schema: str) -> dict[str, int]:
    typed_count, canonical_count, source_edge_count = await database.first(
        f"""
        SELECT
            (SELECT count(*) FROM "{schema}".provider_directory_practitioner),
            (SELECT count(*) FROM "{schema}".provider_directory_canonical_resource),
            (SELECT count(*) FROM "{schema}".provider_directory_source_resource);
        """
    )
    return {
        "canonical": int(canonical_count),
        "source_edge": int(source_edge_count),
        "typed": int(typed_count),
    }


async def _write_practitioner_batch(
    resource_rows: list[dict[str, object]],
    *,
    should_defer_typed_materialization: bool,
) -> int:
    async with source_transaction(importer.db):
        if should_defer_typed_materialization:
            return await importer._upsert_deferred_resource_rows(
                importer.ProviderDirectoryPractitioner,
                resource_rows,
                dataset_id=DATASET_ID,
                track_seen=False,
                resource_hash_contract=importer.LEGACY_RESOURCE_HASH_CONTRACT,
            )
        return await importer._upsert_resource_rows(
            importer.ProviderDirectoryPractitioner,
            resource_rows,
            options=importer.ProviderDirectoryResourceWriteOptions(
                run_id=ROOT_RUN_ID,
                track_seen=False,
                canonical_api_base="https://directory.example.test/fhir",
                source_ids=["source-a"],
                dataset_scope=importer.EndpointDatasetWriteScope(
                    DATASET_ID,
                    importer.LEGACY_RESOURCE_HASH_CONTRACT,
                ),
            ),
        )


@pytest.mark.asyncio
async def test_postgres_deferred_fhir_batch_retains_small_batch_without_legacy_mirrors(
    monkeypatch,
):
    """Persist a synthetic page without copying retained rows to legacy tables."""

    async with _proof_database(monkeypatch) as (database, schema):
        monkeypatch.setattr(importer, "db", database)
        normal_rows = _synthetic_practitioner_rows("practitioner-normal", 1000000000)
        deferred_rows = _synthetic_practitioner_rows(
            "practitioner-deferred",
            1000000100,
        )

        normal_written = await _write_practitioner_batch(
            normal_rows,
            should_defer_typed_materialization=False,
        )
        normal_counts_by_table = await _legacy_counts_by_table(database, schema)
        assert await _active_resource_count(database, "Practitioner") == len(normal_rows)
        deferred_written = await _write_practitioner_batch(
            deferred_rows,
            should_defer_typed_materialization=True,
        )

        assert normal_written == len(normal_rows)
        assert deferred_written == len(deferred_rows)
        assert normal_counts_by_table == {
            "canonical": 0,
            "source_edge": 0,
            "typed": 0,
        }
        assert await _legacy_counts_by_table(database, schema) == normal_counts_by_table
        assert (
            await database.scalar(
                f"""
            SELECT count(*)
              FROM "{schema}".provider_directory_dataset_resource
             WHERE resource_type='Practitioner';
            """
            )
            == 0
        )
        assert await _active_resource_count(database, "Practitioner") == len(normal_rows) + len(deferred_rows)
        assert (
            await database.scalar(
                f"""
            SELECT count(*)
              FROM "{schema}"."{PROVIDER_DIRECTORY_PROOF_SHARD_TABLE}"
             WHERE resource_counts_json ? 'Practitioner';
            """
            )
            == 2
        )
        await _assert_completed_payload_parity(database, schema, [*normal_rows, *deferred_rows])


async def _assert_completed_payload_parity(database, schema, resource_rows):
    """Complete the real private input and retain exact payload/hash identity without serving writes."""
    expected_rows = importer._endpoint_dataset_resource_rows(
        importer.ProviderDirectoryPractitioner,
        resource_rows,
        dataset_id=DATASET_ID,
        resource_hash_contract=importer.LEGACY_RESOURCE_HASH_CONTRACT,
    )
    expected_by_id = {row["resource_id"]: (row["payload_hash"], row["payload_json"]) for row in expected_rows}
    async with (
        source_transaction(database, isolation="repeatable read"),
        completed_source_scope(importer, _candidate()),
    ):
        relation = importer._qt(schema, importer.ProviderDirectoryDatasetResource.__tablename__)
        rows = await database.all(
            f"SELECT resource_id,payload_hash,payload_json FROM {relation} WHERE dataset_id=:dataset",
            dataset=DATASET_ID,
        )
        assert {row[0]: (row[1], row[2]) for row in rows} == expected_by_id
    assert await _legacy_counts_by_table(database, schema) == dict(canonical=0, source_edge=0, typed=0)
