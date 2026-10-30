# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native reference aggregation preserves genuine witnesses without provider payload transfer."""

import asyncio
import hashlib
import json
import os
from types import SimpleNamespace
from uuid import uuid4

import asyncpg
import pytest

from process import registry_fhir_catalog_reference_rollup as rollup
from process.registry_fhir_catalog_extraction import iter_fhir_catalog_pages
from tests.test_registry_fhir_catalog_observed_source_postgres import observed_source_db as observed_source_db


@pytest.fixture
async def reference_database():
    dsn = os.getenv("NETWORK_REGISTRY_TEST_DSN")
    if not dsn:
        pytest.skip("NETWORK_REGISTRY_TEST_DSN must select an isolated database")
    connection = await asyncpg.connect(dsn.replace("postgresql+asyncpg://", "postgresql://"))
    schema = "fc_reference_" + uuid4().hex
    try:
        await connection.execute(f'CREATE SCHEMA "{schema}"')
        await connection.execute(f"""CREATE TABLE "{schema}".provider_directory_dataset_resource(
          dataset_id text NOT NULL,resource_type text NOT NULL,resource_id text NOT NULL,
          payload_hash text NOT NULL,payload_json jsonb NOT NULL,PRIMARY KEY(dataset_id,resource_type,resource_id))""")
        await connection.execute(
            f'CREATE INDEX reference_cursor_idx ON "{schema}".provider_directory_dataset_resource(dataset_id,resource_type COLLATE "C",resource_id COLLATE "C")'
        )
        yield SimpleNamespace(connection=connection, schema=schema)
    finally:
        if not connection.is_closed():
            if connection.is_in_transaction():
                await connection.execute("ROLLBACK")
            await connection.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
            assert await connection.fetchval("SELECT to_regnamespace($1)", schema) is None
            await connection.close()


def resource_row(resource_type, resource_id, payload):
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return ("dataset", resource_type, resource_id, hashlib.sha256(encoded.encode()).hexdigest(), encoded)


async def copy_resources(fixture, resources):
    await fixture.connection.copy_records_to_table(
        "provider_directory_dataset_resource",
        schema_name=fixture.schema,
        columns=("dataset_id", "resource_type", "resource_id", "payload_hash", "payload_json"),
        records=resources,
    )


@pytest.mark.asyncio
async def test_many_roles_retain_only_exact_reference_representatives_with_complete_counts(reference_database):
    """Collapse repeated roles while retaining genuine witnesses and complete accounting."""
    fixture = reference_database
    resources = [
        resource_row(
            "PractitionerRole",
            f"role-{index:05}",
            {"network_refs": ["Organization/shared"], "other_provider_metadata": "x" * 300},
        )
        for index in range(10_000)
    ]
    resources.extend(
        [
            resource_row("PractitionerRole", "without-network", {}),
            resource_row(
                "OrganizationAffiliation",
                "affiliation-one",
                {
                    "network_refs": [
                        "Organization/shared",
                        "https://foreign.test/Organization/shared",
                        "Organization/shared",
                        None,
                    ]
                },
            ),
        ]
    )
    await copy_resources(fixture, resources)
    async with fixture.connection.transaction(isolation="repeatable_read"):
        async with rollup.staged_fhir_catalog_references(
            fixture.connection,
            schema_name=fixture.schema,
            dataset_id="dataset",
            page_rows=500,
            source_uri="https://example.test/fhir",
        ) as stage:
            batches = [batch async for batch in stage.iter_batches(page_rows=2)]
            observations = tuple(observation for batch in batches for observation in batch)
            assert stage.accounting.source_rows == 10_002
            assert stage.accounting.reference_rows == 10_004
            assert stage.accounting.distinct_references == 4 and stage.accounting.source_without_references == 1
            assert stage.accounting.resource_counts == {"PractitionerRole": 10_001, "OrganizationAffiliation": 1}
            assert len(observations) == 4 and all(len(batch) <= 2 for batch in batches)
            representative = next(
                observation
                for observation in observations
                if observation.edge.owner_resource_type == "PractitionerRole"
            )
            assert representative.reference_rows == 10_000 and representative.reference_ordinal == 0
            assert (
                representative.edge.owner_resource_id == "role-00000"
                and representative.edge.source_hash == resources[0][3]
            )
            assert representative.edge.plan_resource_id is None and representative.edge.owner_ref is None
            assert any(observation.edge.resolution_reason == "reference_outside_source" for observation in observations)
            assert any(observation.edge.resolution_reason == "reference_missing" for observation in observations)
            table_name = stage.table_name
        assert await fixture.connection.fetchval("SELECT to_regclass($1)", "pg_temp." + table_name) is None


@pytest.mark.asyncio
async def test_reference_stage_cannot_be_reused_after_scope_cleanup(reference_database):
    fixture = reference_database
    async with fixture.connection.transaction(isolation="repeatable_read"):
        async with rollup.staged_fhir_catalog_references(
            fixture.connection, schema_name=fixture.schema, dataset_id="dataset"
        ) as stage:
            assert stage.accounting.source_rows == 0
        with pytest.raises(rollup.FHIRCatalogReferenceRollupError, match="stage_closed"):
            await stage.iter_batches().__anext__()


@pytest.mark.asyncio
async def test_metadata_iterator_can_exclude_provider_roles_entirely(reference_database):
    fixture = reference_database
    await copy_resources(
        fixture,
        [
            resource_row("Organization", "network", {"name": "Example"}),
            resource_row("InsurancePlan", "plan", {"name": "Choice"}),
            resource_row("PractitionerRole", "role", {"network_refs": ["Organization/network"]}),
        ],
    )
    async with fixture.connection.transaction(isolation="repeatable_read", readonly=True):
        pages = [
            page
            async for page in iter_fhir_catalog_pages(
                fixture.connection,
                schema_name=fixture.schema,
                dataset_id="dataset",
                resource_types=("InsurancePlan", "Organization"),
            )
        ]
    assert {row["resource_type"] for page in pages for row in page} == {"InsurancePlan", "Organization"}


@pytest.mark.asyncio
async def test_reference_expansion_shrinks_native_pages_without_double_counting(reference_database, monkeypatch):
    fixture = reference_database
    monkeypatch.setattr(rollup, "MAX_PAGE_REFERENCES", 4)
    await copy_resources(
        fixture,
        [
            resource_row("PractitionerRole", f"role-{index:02}", {"network_refs": ["Organization/shared"]})
            for index in range(30)
        ],
    )
    async with fixture.connection.transaction(isolation="repeatable_read"):
        async with rollup.staged_fhir_catalog_references(
            fixture.connection, schema_name=fixture.schema, dataset_id="dataset", page_rows=30
        ) as stage:
            observations = [observation async for batch in stage.iter_batches() for observation in batch]
            assert stage.accounting.source_rows == stage.accounting.reference_rows == 30
            assert len(observations) == 1 and observations[0].reference_rows == 30


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "payload",
    [
        {"network_refs": [{"reference": "Organization/network"}]},
        {"network_refs": "Organization/network"},
        {"network_refs": ["x" * 2049]},
        ["not-a-normalized-object"],
    ],
)
async def test_malformed_normalized_references_refuse_without_mutating_source(reference_database, payload):
    fixture = reference_database
    await copy_resources(fixture, [resource_row("PractitionerRole", "role", payload)])
    async with fixture.connection.transaction(isolation="repeatable_read"):
        with pytest.raises(rollup.FHIRCatalogReferenceRollupError, match="source_page_invalid"):
            async with rollup.staged_fhir_catalog_references(
                fixture.connection, schema_name=fixture.schema, dataset_id="dataset"
            ):
                pytest.fail("malformed page was admitted")
        assert (
            await fixture.connection.fetchval(
                f'SELECT count(*) FROM "{fixture.schema}".provider_directory_dataset_resource'
            )
            == 1
        )
        assert (
            await fixture.connection.fetchval(
                "SELECT count(*) FROM pg_class WHERE relnamespace=pg_my_temp_schema() AND relname LIKE 'fhir_catalog_refs_%'"
            )
            == 0
        )


@pytest.mark.asyncio
async def test_temp_staging_keeps_source_read_only(reference_database):
    fixture = reference_database
    await copy_resources(
        fixture, [resource_row("PractitionerRole", "role-one", {"network_refs": ["Organization/network-one"]})]
    )
    async with rollup.fhir_catalog_reference_workspace(fixture.connection) as workspace:
        async with fixture.connection.transaction(isolation="repeatable_read", readonly=True):
            for _ in range(2):
                async with rollup.staged_fhir_catalog_references(
                    fixture.connection, schema_name=fixture.schema, dataset_id="dataset", workspace=workspace
                ) as stage:
                    observations = [row async for batch in stage.iter_batches() for row in batch]
                    assert len(observations) == 1
                    assert observations[0].edge.network_resource_id == "network-one"
                    assert await fixture.connection.fetchval("SHOW transaction_read_only") == "on"
            with pytest.raises(asyncpg.ReadOnlySQLTransactionError):
                async with fixture.connection.transaction():
                    await fixture.connection.execute(
                        f'DELETE FROM "{fixture.schema}".provider_directory_dataset_resource'
                    )
    assert await fixture.connection.fetchval("SELECT to_regclass($1)", "pg_temp." + workspace.table_name) is None
    assert (
        await fixture.connection.fetchval(
            f'SELECT count(*) FROM "{fixture.schema}".provider_directory_dataset_resource'
        )
        == 1
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("observed_source_db", [("SCOPED", 2)], indirect=True)
async def test_published_leaves_share_one_global_keyset_and_reference_accounting(observed_source_db):
    """Interleave physical leaves without resetting cursors or duplicating rollup counts."""
    from process.registry_fhir_catalog_published_read import pin_fhir_catalog_published_read

    fixture = observed_source_db
    await fixture.admin.execute(f'DELETE FROM "{fixture.schema}".resources_one')
    resources = [
        (
            "dataset-one",
            resource_type,
            f"resource-{index:02}",
            ("1" if index % 2 else "f") * 64,
            json.dumps({"name": "Example", "network_refs": [f"Organization/network-{index % 3}"]}),
        )
        for resource_type in ("Organization", "InsurancePlan", "OrganizationAffiliation", "PractitionerRole")
        for index in range(9)
    ]
    await fixture.admin.copy_records_to_table(
        "provider_directory_dataset_resource", schema_name=fixture.schema, records=resources
    )
    reader = await fixture.connect()
    async with rollup.fhir_catalog_reference_workspace(reader) as workspace:
        async with reader.transaction(isolation="repeatable_read", readonly=True):
            published = await pin_fhir_catalog_published_read(
                reader, fixture.profile, endpoint_id="endpoint-one", dataset_id="dataset-one"
            )
            pages = [
                page
                async for page in iter_fhir_catalog_pages(
                    reader,
                    schema_name=fixture.schema,
                    dataset_id="dataset-one",
                    source_read=published,
                    page_rows=3,
                    page_bytes=1800,
                )
            ]
            keys = [(resource["resource_type"], resource["resource_id"]) for page in pages for resource in page]
            assert keys == sorted((resource[1], resource[2]) for resource in resources)
            assert all(len(page) <= 3 and len(json.dumps(page).encode()) <= 1800 for page in pages)
            async with rollup.staged_fhir_catalog_references(
                reader,
                schema_name=fixture.schema,
                dataset_id="dataset-one",
                source_read=published,
                workspace=workspace,
                page_rows=3,
                page_bytes=1800,
            ) as stage:
                edges = [edge async for batch in stage.iter_batches(page_rows=2) for edge in batch]
                assert stage.accounting.source_rows == stage.accounting.reference_rows == 18
                assert stage.accounting.resource_counts == {"OrganizationAffiliation": 9, "PractitionerRole": 9}
                assert stage.accounting.distinct_references == len(edges) == 6
                assert sum(edge.reference_rows for edge in edges) == 18
            with pytest.raises(asyncpg.ReadOnlySQLTransactionError):
                async with reader.transaction():
                    await reader.execute(
                        f'INSERT INTO "{fixture.schema}".resources_low VALUES(NULL,NULL,NULL,NULL,NULL)'
                    )
    assert await reader.fetchval("SELECT to_regclass($1)", "pg_temp." + workspace.table_name) is None


@pytest.mark.asyncio
@pytest.mark.parametrize("observed_source_db", [("SCOPED", 2)], indirect=True)
async def test_published_cross_leaf_duplicate_is_refused_before_keyset_traversal(observed_source_db):
    """Detect duplicate logical keys even when a cursor would skip their second leaf."""
    from process.registry_fhir_catalog_published_read import pin_fhir_catalog_published_read

    fixture = observed_source_db
    await fixture.admin.execute(
        f'INSERT INTO "{fixture.schema}".resources_low '
        f"VALUES('dataset-one','Organization','network',repeat('1',64),'{{\"name\":\"Duplicate\"}}')"
    )
    assert (
        await fixture.admin.fetchval(
            f'SELECT count(DISTINCT tableoid) FROM "{fixture.schema}".resources_one '
            f"WHERE resource_type='Organization' AND resource_id='network'"
        )
        == 2
    )
    reader = await fixture.connect()
    async with reader.transaction(isolation="repeatable_read", readonly=True):
        with pytest.raises(ValueError):
            await pin_fhir_catalog_published_read(
                reader, fixture.profile, endpoint_id="endpoint-one", dataset_id="dataset-one"
            )


@pytest.mark.asyncio
@pytest.mark.parametrize("observed_source_db", [("SCOPED", 2)], indirect=True)
async def test_published_consumers_preserve_byte_bounds(observed_source_db):
    """Both metadata pages and native reference pages reject one oversized source row."""
    from process.registry_fhir_catalog_published_read import pin_fhir_catalog_published_read

    fixture = observed_source_db
    await fixture.admin.execute(
        f"UPDATE \"{fixture.schema}\".resources_one SET payload_json=payload_json||jsonb_build_object('name',repeat('x',2000))"
    )
    reader = await fixture.connect()
    async with rollup.fhir_catalog_reference_workspace(reader) as workspace:
        async with reader.transaction(isolation="repeatable_read", readonly=True):
            published = await pin_fhir_catalog_published_read(
                reader, fixture.profile, endpoint_id="endpoint-one", dataset_id="dataset-one"
            )
            with pytest.raises(ValueError):
                async for _ in iter_fhir_catalog_pages(
                    reader,
                    schema_name=fixture.schema,
                    dataset_id="dataset-one",
                    source_read=published,
                    page_bytes=100,
                ):
                    pytest.fail("oversized metadata was yielded")
            with pytest.raises(rollup.FHIRCatalogReferenceRollupError, match="source_page_bounds"):
                async with rollup.staged_fhir_catalog_references(
                    reader,
                    schema_name=fixture.schema,
                    dataset_id="dataset-one",
                    source_read=published,
                    workspace=workspace,
                    page_bytes=100,
                ):
                    pytest.fail("oversized references were staged")
    assert await reader.fetchval("SELECT to_regclass($1)", "pg_temp." + workspace.table_name) is None


@pytest.mark.asyncio
@pytest.mark.parametrize("observed_source_db", [("SCOPED", 2)], indirect=True)
async def test_published_reference_cancellation_closes_workspace_snapshot_and_leaf_locks(observed_source_db):
    """Cancel real scoped staging and prove its exact TEMP and transaction locks close."""
    from process.registry_fhir_catalog_published_read import pin_fhir_catalog_published_read

    fixture, entered, state = observed_source_db, asyncio.Event(), {}
    reader = await fixture.connect()

    async def stage_until_cancelled():
        """Keep the real task-bound reader and workspace alive until cancellation."""
        async with rollup.fhir_catalog_reference_workspace(reader) as workspace:
            state["workspace"] = workspace
            async with reader.transaction(isolation="repeatable_read", readonly=True):
                published = await pin_fhir_catalog_published_read(
                    reader, fixture.profile, endpoint_id="endpoint-one", dataset_id="dataset-one"
                )
                async with rollup.staged_fhir_catalog_references(
                    reader,
                    schema_name=fixture.schema,
                    dataset_id="dataset-one",
                    source_read=published,
                    workspace=workspace,
                ) as stage:
                    assert [batch async for batch in stage.iter_batches()]
                    entered.set()
                    await asyncio.Event().wait()

    operation = asyncio.create_task(stage_until_cancelled())
    try:
        await asyncio.wait_for(entered.wait(), 5)
    finally:
        operation.cancel()
        with pytest.raises(asyncio.CancelledError):
            await operation
    assert not reader.is_in_transaction()
    assert await reader.fetchval("SELECT to_regclass($1)", "pg_temp." + state["workspace"].table_name) is None
    async with fixture.admin.transaction():
        await fixture.admin.execute(f'LOCK TABLE ONLY "{fixture.schema}".resources_low IN ACCESS EXCLUSIVE MODE NOWAIT')


@pytest.mark.asyncio
@pytest.mark.parametrize("observed_source_db", [("SCOPED", 2)], indirect=True)
@pytest.mark.parametrize("consumer", ["metadata", "references"])
async def test_published_consumers_refuse_unbranded_source_read(observed_source_db, consumer):
    """An unknown read object cannot select payloads or fall back to the denied parent."""
    fixture, reader = observed_source_db, await observed_source_db.connect()
    async with (
        rollup.fhir_catalog_reference_workspace(reader) as workspace,
        reader.transaction(isolation="repeatable_read", readonly=True),
    ):
        with pytest.raises(ValueError):
            if consumer == "metadata":
                async for _ in iter_fhir_catalog_pages(
                    reader,
                    schema_name=fixture.schema,
                    dataset_id="dataset-one",
                    source_read=object(),
                ):
                    pytest.fail("unbranded metadata scope was accepted")
            else:
                async with rollup.staged_fhir_catalog_references(
                    reader,
                    schema_name=fixture.schema,
                    dataset_id="dataset-one",
                    source_read=object(),
                    workspace=workspace,
                ):
                    pytest.fail("unbranded reference scope was accepted")
