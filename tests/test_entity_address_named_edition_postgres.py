# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Address follow-ups serve a dataset that is stored only in its named edition schema."""

import importlib
import json
from contextlib import asynccontextmanager
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import create_async_engine

from process import provider_directory_published_replacement_inputs as serving_inputs
from process import provider_directory_serving_recovery as serving_recovery
from process.entity_address_cutover_contract import postgres_sqlstate
from process.provider_directory_named_edition import (
    HEADER,
    POINTER_SCHEMA,
    POINTER_TABLE,
    address_editions,
    edition_schema_name,
    selected_editions,
)
from tests.provider_directory_entities_postgres_support import _database_url
from tests.test_provider_directory_published_replacement_postgres import _markers
from tests.test_provider_directory_published_replacement_recipe_postgres import (
    _address_child_task,
    _assert_addresses,
    _publish,
    _recipe_case,
    _rows,
)

native = importlib.import_module("process.entity_address_unified")
pytestmark = pytest.mark.asyncio


@asynccontextmanager
async def _named_edition(case, dataset_id, endpoint_id, *, legacy_dataset_id):
    """Move one dataset into its own edition behind the pointer while a superseded catalog header stays current."""
    database, schema, edition = case.database, case.schema, edition_schema_name(dataset_id)
    pointer = f"{POINTER_SCHEMA}.{POINTER_TABLE}"
    administrator = create_async_engine(_database_url())
    is_pointer_created = False
    try:
        async with administrator.begin() as connection:
            is_pointer_created = await connection.scalar(
                text("SELECT to_regclass(:pointer) IS NULL"), {"pointer": pointer}
            )
            await connection.execute(text(f'CREATE SCHEMA "{edition}" AUTHORIZATION "{case.role}"'))
            await connection.execute(text(f"CREATE SCHEMA IF NOT EXISTS {POINTER_SCHEMA}"))
            await connection.execute(
                text(
                    f"CREATE TABLE IF NOT EXISTS {pointer} (catalog_schema text NOT NULL, endpoint_id text NOT NULL, "
                    "dataset_id text NOT NULL, schema_name text NOT NULL, previous_dataset_id text, "
                    "previous_schema_name text, PRIMARY KEY (catalog_schema, endpoint_id))"
                )
            )
            # Address readers get SELECT only, so they cannot lock or rewrite the pointer.
            await connection.execute(text(f'GRANT USAGE ON SCHEMA {POINTER_SCHEMA} TO "{case.role}"'))
            await connection.execute(text(f'GRANT SELECT ON {pointer} TO "{case.role}"'))
        # Named headers are read with the model's full column shape.
        await database.status(f"ALTER TABLE {schema}.{HEADER} ADD COLUMN IF NOT EXISTS candidate_storage_json jsonb")
        for table in (HEADER, "provider_directory_dataset_resource"):
            await database.status(f'CREATE TABLE "{edition}".{table} (LIKE {schema}.{table} INCLUDING ALL)')
            await database.status(
                f'INSERT INTO "{edition}".{table} SELECT * FROM {schema}.{table} WHERE dataset_id=:dataset_id',
                dataset_id=dataset_id,
            )
            await database.status(f"DELETE FROM {schema}.{table} WHERE dataset_id=:dataset_id", dataset_id=dataset_id)
        async with administrator.begin() as connection:
            await connection.execute(
                text(
                    f"INSERT INTO {pointer} VALUES (:catalog_schema,:endpoint_id,:dataset_id,:edition,:legacy,:catalog_schema)"
                ),
                dict(
                    catalog_schema=schema,
                    endpoint_id=endpoint_id,
                    dataset_id=dataset_id,
                    edition=edition,
                    legacy=legacy_dataset_id,
                ),
            )
        yield edition
    finally:
        async with administrator.begin() as connection:
            if await connection.scalar(text("SELECT to_regclass(:pointer) IS NOT NULL"), {"pointer": pointer}):
                await connection.execute(
                    text(f"DELETE FROM {pointer} WHERE catalog_schema=:schema"), {"schema": schema}
                )
            await connection.execute(text(f'DROP SCHEMA IF EXISTS "{edition}" CASCADE'))
            if is_pointer_created:
                await connection.execute(text(f"DROP SCHEMA IF EXISTS {POINTER_SCHEMA} CASCADE"))
        await administrator.dispose()


async def _hold_named_source_on_its_endpoint(case):
    """Serve source_primary from endpoint_shared, whose superseded legacy header still reads as current."""
    database, schema = case.database, case.schema
    await database.status(
        f"UPDATE {schema}.provider_directory_source SET endpoint_id='endpoint_shared' WHERE source_id='source_primary'"
    )
    await database.status(
        f"UPDATE {schema}.provider_directory_endpoint_dataset SET endpoint_id='endpoint_shared' "
        "WHERE dataset_id='dataset_serving'"
    )


async def _probe_held_endpoint(observed_by_field):
    """Try the pointer writer's endpoint lock from another connection while the cut is open."""
    original = serving_recovery.prepare_address

    async def probe(database, schema, swaps, context):
        administrator = create_async_engine(_database_url())
        try:
            async with administrator.begin() as connection:
                observed_by_field["writer_locked"] = await connection.scalar(
                    text("SELECT pg_try_advisory_xact_lock(hashtextextended('endpoint_shared',0))")
                )
        finally:
            await administrator.dispose()
        return await original(database, schema, swaps, context)

    return probe


def _probe_publication_writer(schema, observed_by_field):
    """Try a pointer writer's first step, its source-table lock, while the replacement's final check holds."""
    original = serving_inputs.lock_and_check

    async def probe(directory, snapshot, **options):
        await original(directory, snapshot, **options)
        administrator = create_async_engine(_database_url())
        try:
            async with administrator.begin() as connection:
                await connection.execute(text("SET LOCAL lock_timeout = '200ms'"))
                await connection.execute(text(f"LOCK TABLE {schema}.provider_directory_source IN ROW EXCLUSIVE MODE"))
        except DBAPIError as error:
            observed_by_field["writer_sqlstate"] = postgres_sqlstate(error)
        finally:
            await administrator.dispose()

    return probe


def _observe_partial_projection(monkeypatch, case, edition, dataset_id, *, prepared):
    """Require the real partial cache, pinned named scope and reader cleanup on both recipes."""
    projection = native.partial_projection
    original = projection.scoped_projection
    observed_paths = []

    @asynccontextmanager
    async def require_cache(sql, schema, stage_table, available, **scope):
        assert (sql.candidate_preparation.current() is not None) is prepared
        assert f'"{edition}"."{HEADER}"' in projection._scope_sql(sql, schema)
        assert f'"{edition}"."provider_directory_dataset_resource"' in projection._raw_sql(sql, schema)
        async with original(sql, schema, stage_table, available, **scope) as relation:
            assert relation is not None
            active = projection._ACTIVE.get()
            assert active is not None and active.relation == relation
            assert await case.database.scalar(
                f"SELECT EXISTS(SELECT FROM {active.scope_relation} WHERE dataset_id=:dataset)", dataset=dataset_id
            )
            observed_paths.append(prepared)
            yield relation
        for relation in active.created_relations:
            assert await case.database.scalar("SELECT to_regclass(:relation) IS NULL", relation=relation)

    monkeypatch.setattr(projection, "scoped_projection", require_cache)
    return observed_paths


async def _assert_named_followup_through_cutover(monkeypatch, edition_factory):
    """Both follow-up stages read the edition, not the superseded catalog header, and hold the pointer at cut."""
    async with _recipe_case(monkeypatch, archive_complete=True) as case:
        unrelated = next(row for row in await _rows(case, "entity_address_unified") if row["npi"] == 1000000004)
        await _hold_named_source_on_its_endpoint(case)
        async with edition_factory(
            case, "dataset_shared", "endpoint_shared", legacy_dataset_id="dataset_serving"
        ) as edition:
            overlay_metrics = await _publish(case, _markers())
            assert overlay_metrics["address_overlay"]["published"] is True
            overlay_runs = await case.database.scalar(
                f"SELECT array_agg(DISTINCT last_seen_run_id) FROM {case.schema}.provider_directory_address_overlay "
                "WHERE source_id='source_primary'"
            )
            assert overlay_runs == ["root-shared"]
            with address_editions(await selected_editions(case.directory)):
                scope = await native._latest_provider_directory_partial_scope(case.schema)
            assert scope == ("root-shared", ["source_primary"], ["current_overlay"])
            observed_by_field = {}
            monkeypatch.setattr(serving_recovery, "prepare_address", await _probe_held_endpoint(observed_by_field))
            cached = _observe_partial_projection(monkeypatch, case, edition, "dataset_shared", prepared=False)
            job_by_field = {}
            await native.startup(job_by_field)
            job_by_field["import_date"] = "pdn" + uuid4().hex[:20]
            await native.process_entity_address_unified_data(job_by_field, _address_child_task(False))
            await native.publish_entity_address_unified_generation(job_by_field)
            assert cached == [False]
            assert observed_by_field == {"writer_locked": False}
            await _assert_addresses(case, unrelated, archive_complete=True)


async def _assert_replacement_keeps_named_peer(monkeypatch, edition_factory):
    """A catalog replacement pins its named peer's edition, not the peer's superseded catalog header."""
    async with _recipe_case(monkeypatch, archive_complete=True) as case:
        unrelated = next(
            address for address in await _rows(case, "entity_address_unified") if address["npi"] == 1000000004
        )
        await case.database.status(
            f"UPDATE {case.schema}.provider_directory_endpoint_dataset SET publication_metadata_json=CAST(:metadata AS json) "
            "WHERE dataset_id='dataset_unpublished'",
            metadata=json.dumps({"source_ids": ["source_no_current"]}),
        )
        await case.database.status(
            f"INSERT INTO {case.schema}.provider_directory_endpoint_dataset (dataset_id,endpoint_id,import_run_id,"
            "acquisition_root_run_id,dataset_hash,status,is_current,resource_count,validated_at,published_at,"
            "publication_metadata_json) VALUES ('dataset_peer_legacy','endpoint_unpublished','peer-legacy-run',"
            "'peer-legacy-root',repeat('d',64),'published',true,0,timestamp '2025-12-01',timestamp '2025-12-01',"
            "CAST(:metadata AS json))",
            metadata=json.dumps({"source_ids": ["source_no_current"]}),
        )
        async with edition_factory(
            case, "dataset_unpublished", "endpoint_unpublished", legacy_dataset_id="dataset_peer_legacy"
        ) as edition:
            observed_by_field = {}
            monkeypatch.setattr(
                serving_inputs, "lock_and_check", _probe_publication_writer(case.schema, observed_by_field)
            )
            cached = _observe_partial_projection(monkeypatch, case, edition, "dataset_unpublished", prepared=True)
            metrics = await _publish(case, _markers())
            assert metrics["serving_recovery"]["phase"] == "published"
            assert cached == [True]
            assert observed_by_field == {"writer_sqlstate": "55P03"}
            await _assert_addresses(case, unrelated, archive_complete=True)


async def test_named_followup_runs_overlay_and_partial_address_through_cutover(monkeypatch):
    """Keep the plain named-edition scenario available under its existing test identity."""
    await _assert_named_followup_through_cutover(monkeypatch, _named_edition)


async def test_published_replacement_keeps_a_named_peer(monkeypatch):
    """Keep the plain peer scenario available under its existing test identity."""
    await _assert_replacement_keeps_named_peer(monkeypatch, _named_edition)
