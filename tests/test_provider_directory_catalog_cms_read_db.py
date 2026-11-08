# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Native composition proof for the singleton CMS catalog consumers."""

import json

import pytest

from api import provider_directory_cms_candidate_catalog as candidate
from api import provider_directory_cms_generation as generation
from tests.test_provider_directory_cms_candidate_catalog import _dataset
from tests.test_provider_directory_source_local_outcomes import (
    _outcome_selection_schema,
    _seal_outcome_selection_datasets,
)


async def _cms_dataset(database, schema, dataset_id, endpoint_id, *, current):
    """Insert a complete CMS-shaped seal; unrelated rows use the same shape."""
    metadata = _dataset()["publication_metadata"]
    await database.status(
        f"INSERT INTO {schema}.provider_directory_endpoint_dataset "
        "(dataset_id,endpoint_id,acquisition_root_run_id,import_run_id,dataset_hash,"
        "status,is_current,validated_at,published_at,resource_count,publication_metadata_json) "
        "VALUES (:dataset_id,:endpoint_id,:owner,:owner,:hash,:status,:current,now(),"
        "CASE WHEN :current THEN now() ELSE NULL END,8,CAST(:metadata AS jsonb));",
        dataset_id=dataset_id,
        endpoint_id=endpoint_id,
        owner=dataset_id + "-owner",
        hash="a" * 64,
        status="published" if current else "validated",
        current=current,
        metadata=json.dumps(metadata),
    )


async def _cms_fixture(database, schema):
    """Extend only the owned component schema with production query columns."""
    await database.status(
        f"ALTER TABLE {schema}.provider_directory_endpoint_dataset ADD COLUMN import_run_id varchar(64);"
    )
    await database.status(
        f"CREATE TABLE {schema}.provider_directory_cms_npd_stale_candidate (dataset_id varchar(96) PRIMARY KEY);"
    )
    await database.status(
        f"INSERT INTO {schema}.provider_directory_source (source_id,endpoint_id) VALUES ('cms-npd','cms-endpoint');"
    )


async def _seal_cms_fixture(database, schema):
    """Use the genuine component digest after setting the complete resource set."""
    await _seal_outcome_selection_datasets(database, schema)
    await database.status(
        f"UPDATE {schema}.provider_directory_endpoint_dataset "
        "SET content_proof_resource_types=CAST(:resources AS varchar[]) "
        "WHERE dataset_id LIKE 'cms-%' OR dataset_id LIKE 'other-cms-%';",
        resources=sorted(generation.RESOURCE_TYPES),
    )
    await database.status(
        f"UPDATE {schema}.provider_directory_endpoint_dataset "
        f"SET publication_metadata_sha256={schema}."
        "provider_directory_endpoint_dataset_admission_metadata_sha256("
        "publication_metadata_summary_json,content_proof_admission_version,"
        "content_proof_admission_kind,content_proof_admission_sha256,"
        "content_proof_resource_types) "
        "WHERE dataset_id LIKE 'cms-%' OR dataset_id LIKE 'other-cms-%';"
    )


@pytest.mark.asyncio
async def test_cms_singleton_filters_own_rows_and_detects_two_candidates(monkeypatch):
    async with _outcome_selection_schema(monkeypatch) as (database, schema):
        await _cms_fixture(database, schema)
        await _cms_dataset(database, schema, "cms-first", "cms-endpoint", current=False)
        await _cms_dataset(database, schema, "other-cms-first", "unrelated", current=False)
        await _cms_dataset(database, schema, "other-cms-second", "unrelated", current=False)
        await _seal_cms_fixture(database, schema)
        original_availability = candidate.candidate_available_sql
        monkeypatch.setattr(
            candidate, "candidate_available_sql", lambda alias, _schema: original_availability(alias, schema)
        )

        async def selected():
            statement = candidate._dataset_statement(current=False)
            assert len(statement.get_final_froms()) == 1
            return (
                (await database.execute(statement.execution_options(schema_translate_map={"mrf": schema})))
                .mappings()
                .all()
            )

        dataset_records = await selected()
        assert [
            (dataset_record["dataset_id"], dataset_record["import_run_id"]) for dataset_record in dataset_records
        ] == [("cms-first", "cms-first-owner")]
        await _cms_dataset(database, schema, "cms-second", "cms-endpoint", current=False)
        await _seal_cms_fixture(database, schema)
        assert {dataset_record["dataset_id"] for dataset_record in await selected()} == {"cms-first", "cms-second"}
        await database.status(
            f"INSERT INTO {schema}.provider_directory_cms_npd_stale_candidate (dataset_id) VALUES ('cms-second');"
        )
        assert [dataset_record["dataset_id"] for dataset_record in await selected()] == ["cms-first"]


@pytest.mark.asyncio
async def test_accepted_cms_generation_retains_its_scoped_current_row(monkeypatch):
    async with _outcome_selection_schema(monkeypatch) as (database, schema):
        await _cms_fixture(database, schema)
        await _cms_dataset(database, schema, "cms-current", "cms-endpoint", current=True)
        await _cms_dataset(database, schema, "other-cms-current", "unrelated", current=True)
        await _seal_cms_fixture(database, schema)
        async with database.transaction() as session:
            await session.connection(execution_options={"schema_translate_map": {"mrf": schema}})
            actual = await generation.accepted_cms_generation(session, None)
        assert actual["dataset_id"] == "cms-current"
        assert actual["release_id"] == "c" * 64
