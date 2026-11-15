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
from process.registry_fhir_catalog_observed_source import catalog_observation_tables, collect_fhir_catalog_observation
from tests.test_registry_fhir_catalog_observed_source_postgres import observed_source_db as observed_source_db


@pytest.fixture
async def reference_database(request):
    dsn = os.getenv("NETWORK_REGISTRY_TEST_DSN")
    if not dsn:
        pytest.skip("NETWORK_REGISTRY_TEST_DSN must select an isolated database")
    connection = await asyncpg.connect(dsn.replace("postgresql+asyncpg://", "postgresql://"))
    payload_storage = getattr(request, "param", "jsonb")
    assert payload_storage in {"jsonb", "json"}
    schema = "fc_reference_" + uuid4().hex
    try:
        await connection.execute(f'CREATE SCHEMA "{schema}"')
        await connection.execute(f"""CREATE TABLE "{schema}".provider_directory_dataset_resource(
          dataset_id text NOT NULL,resource_type text NOT NULL,resource_id text NOT NULL,
          payload_hash text NOT NULL,payload_json {payload_storage} NOT NULL,PRIMARY KEY(dataset_id,resource_type,resource_id))""")
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
@pytest.mark.parametrize("reference_database", ("jsonb", "json"), indirect=True)
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
@pytest.mark.parametrize(
    "field,value",
    [
        (2, ""),
        (2, "x" * 65),
        (2, "role/one"),
        (2, "rôle"),
        (2, "role\n"),
        (3, "a" * 63),
        (3, "a" * 65),
        (3, "A" * 64),
        (3, "g" * 64),
        (3, "a" * 63 + "\n"),
    ],
)
async def test_reference_resource_identity_validation_survives_page_aggregation(reference_database, field, value):
    fixture = reference_database
    record_values = list(resource_row("PractitionerRole", "role-one", {"network_refs": ["Organization/network"]}))
    record_values[field] = value
    await copy_resources(fixture, [tuple(record_values)])
    async with fixture.connection.transaction(isolation="repeatable_read"):
        with pytest.raises(rollup.FHIRCatalogReferenceRollupError, match="source_page_invalid"):
            async with rollup.staged_fhir_catalog_references(
                fixture.connection, schema_name=fixture.schema, dataset_id="dataset"
            ):
                pytest.fail("invalid source identity was aggregated")


@pytest.mark.asyncio
async def test_reference_resource_identity_accepts_ascii_boundaries(reference_database):
    fixture = reference_database
    resource_id = "Az09.-" + "x" * 58
    payload_hash = "0123456789abcdef" * 4
    await copy_resources(
        fixture,
        [("dataset", "PractitionerRole", resource_id, payload_hash, '{"network_refs":["Organization/network"]}')],
    )
    async with fixture.connection.transaction(isolation="repeatable_read"):
        async with rollup.staged_fhir_catalog_references(
            fixture.connection, schema_name=fixture.schema, dataset_id="dataset"
        ) as stage:
            edges = [edge async for batch in stage.iter_batches() for edge in batch]
            assert len(edges) == 1 and stage.accounting.source_rows == stage.accounting.reference_rows == 1
            assert (edges[0].edge.owner_resource_id, edges[0].edge.source_hash) == (resource_id, payload_hash)


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
async def test_named_partitioned_resources_share_one_keyset_and_complete_reference_accounting(observed_source_db):
    """Interleave named partitioned data without resetting cursors or double counting."""
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
            tables = await catalog_observation_tables(reader, fixture.profile, "dataset-one")
            pages = [
                page
                async for page in iter_fhir_catalog_pages(
                    reader,
                    schema_name=fixture.schema,
                    dataset_id="dataset-one",
                    resource_relation=tables["resource"],
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
                resource_relation=tables["resource"],
                workspace=workspace,
                page_rows=3,
                page_bytes=1800,
            ) as stage:
                edges = [edge async for batch in stage.iter_batches(page_rows=2) for edge in batch]
                assert stage.accounting.source_rows == stage.accounting.reference_rows == 18
                assert stage.accounting.resource_counts == {"OrganizationAffiliation": 9, "PractitionerRole": 9}
                assert stage.accounting.distinct_references == len(edges) == 6
                assert sum(edge.reference_rows for edge in edges) == 18
            with pytest.raises((asyncpg.InsufficientPrivilegeError, asyncpg.ReadOnlySQLTransactionError)):
                async with reader.transaction():
                    await reader.execute(
                        f'INSERT INTO "{fixture.schema}".provider_directory_dataset_resource VALUES(NULL,NULL,NULL,NULL,NULL)'
                    )
    assert await reader.fetchval("SELECT to_regclass($1)", "pg_temp." + workspace.table_name) is None


@pytest.mark.asyncio
@pytest.mark.parametrize("observed_source_db", [("SCOPED", 2)], indirect=True)
async def test_duplicate_resource_keys_are_refused_by_complete_observation(observed_source_db):
    """A complete observation refuses duplicate keys that a keyset cursor could skip."""
    fixture = observed_source_db
    await fixture.admin.execute(
        f'INSERT INTO "{fixture.schema}".resources_low '
        f"VALUES('dataset-one','Organization','network',repeat('1',64),'{{\"name\":\"Duplicate\"}}')"
    )
    assert (
        await fixture.admin.fetchval(
            f'SELECT count(*) FROM "{fixture.schema}".resources_one '
            f"WHERE resource_type='Organization' AND resource_id='network'"
        )
        == 2
    )
    reader = await fixture.connect()
    async with reader.transaction(isolation="repeatable_read", readonly=True):
        with pytest.raises(ValueError):
            await collect_fhir_catalog_observation(
                reader, fixture.profile, endpoint_id="endpoint-one", dataset_id="dataset-one"
            )


@pytest.mark.asyncio
@pytest.mark.parametrize("observed_source_db", [("SCOPED", 2)], indirect=True)
async def test_named_consumers_preserve_byte_bounds(observed_source_db):
    """Both metadata pages and native reference pages reject one oversized source row."""
    fixture = observed_source_db
    await fixture.admin.execute(
        f"UPDATE \"{fixture.schema}\".resources_one SET payload_json=payload_json||jsonb_build_object('name',repeat('x',2000))"
    )
    reader = await fixture.connect()
    async with rollup.fhir_catalog_reference_workspace(reader) as workspace:
        async with reader.transaction(isolation="repeatable_read", readonly=True):
            tables = await catalog_observation_tables(reader, fixture.profile, "dataset-one")
            with pytest.raises(ValueError):
                async for _ in iter_fhir_catalog_pages(
                    reader,
                    schema_name=fixture.schema,
                    dataset_id="dataset-one",
                    resource_relation=tables["resource"],
                    page_bytes=100,
                ):
                    pytest.fail("oversized metadata was yielded")
            with pytest.raises(rollup.FHIRCatalogReferenceRollupError, match="source_page_bounds"):
                async with rollup.staged_fhir_catalog_references(
                    reader,
                    schema_name=fixture.schema,
                    dataset_id="dataset-one",
                    resource_relation=tables["resource"],
                    workspace=workspace,
                    page_bytes=100,
                ):
                    pytest.fail("oversized references were staged")
    assert await reader.fetchval("SELECT to_regclass($1)", "pg_temp." + workspace.table_name) is None


@pytest.mark.asyncio
@pytest.mark.parametrize("observed_source_db", [("SCOPED", 2)], indirect=True)
async def test_named_reference_cancellation_closes_workspace_snapshot_and_read_locks(observed_source_db):
    """Cancel real staging and prove its temporary data and read transaction close."""
    fixture, entered, state = observed_source_db, asyncio.Event(), {}
    reader = await fixture.connect()

    async def stage_until_cancelled():
        """Keep the actual reader snapshot and workspace alive until cancellation."""
        async with rollup.fhir_catalog_reference_workspace(reader) as workspace:
            state["workspace"] = workspace
            async with reader.transaction(isolation="repeatable_read", readonly=True):
                tables = await catalog_observation_tables(reader, fixture.profile, "dataset-one")
                async with rollup.staged_fhir_catalog_references(
                    reader,
                    schema_name=fixture.schema,
                    dataset_id="dataset-one",
                    resource_relation=tables["resource"],
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
        await fixture.admin.execute(
            f'LOCK TABLE "{fixture.schema}".provider_directory_dataset_resource IN ACCESS EXCLUSIVE MODE NOWAIT'
        )


async def _consume_unreadable_relation(reader, fixture, consumer, resource_relation, workspace):
    if consumer == "metadata":
        async for _ in iter_fhir_catalog_pages(
            reader,
            schema_name=fixture.schema,
            dataset_id="dataset-one",
            resource_relation=resource_relation,
        ):
            pytest.fail("invalid or unreadable metadata relation was accepted")
    else:
        async with rollup.staged_fhir_catalog_references(
            reader,
            schema_name=fixture.schema,
            dataset_id="dataset-one",
            resource_relation=resource_relation,
            workspace=workspace,
        ):
            pytest.fail("invalid or unreadable reference relation was accepted")


@pytest.mark.asyncio
@pytest.mark.parametrize("observed_source_db", [("SCOPED", 2)], indirect=True)
@pytest.mark.parametrize("consumer", ["metadata", "references"])
@pytest.mark.parametrize("relation_kind", ["invalid", "private"])
async def test_named_consumers_require_valid_names_and_actual_select_privileges(
    observed_source_db, consumer, relation_kind
):
    """A name map cannot replace validation or native SELECT permission on its data."""
    fixture, reader = observed_source_db, await observed_source_db.connect()
    resource_relation = (
        object() if relation_kind == "invalid" else {"schema": fixture.schema, "name": "resources_detached"}
    )
    error = ValueError if relation_kind == "invalid" else asyncpg.InsufficientPrivilegeError
    async with (
        rollup.fhir_catalog_reference_workspace(reader) as workspace,
        reader.transaction(isolation="repeatable_read", readonly=True),
    ):
        with pytest.raises(error):
            async with reader.transaction():
                await _consume_unreadable_relation(reader, fixture, consumer, resource_relation, workspace)
    assert not reader.is_in_transaction()
    assert await reader.fetchval("SELECT to_regclass($1)", "pg_temp." + workspace.table_name) is None


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["invalid_reference", "invalid_identity", "numeric_range"])
async def test_discarded_tail_is_eventually_validated_before_stage_exposure(reference_database, failure):
    fixture = reference_database
    connection = fixture.connection
    relation = f'"{fixture.schema}".provider_directory_dataset_resource'
    await connection.execute(f"ALTER TABLE {relation} ALTER COLUMN payload_json TYPE json USING payload_json::json")
    resource_rows = [
        resource_row(
            "PractitionerRole",
            f"role-{number:05}",
            {"network_refs": ["Organization/network"], "metadata": "x" * 1000},
        )
        for number in range(30)
    ]
    last_values = list(resource_rows[-1])
    expected_error = rollup.FHIRCatalogReferenceRollupError
    if failure == "invalid_reference":
        last_values[-1] = json.dumps({"network_refs": [{"invalid": True}], "metadata": "x" * 1000})
    elif failure == "invalid_identity":
        last_values[3] = "g" * 64
    else:
        last_values[-1] = '{"network_refs":["Organization/network"],"number":1e1000000}'
        expected_error = asyncpg.NumericValueOutOfRangeError
    if failure != "invalid_identity":
        last_values[3] = hashlib.sha256(last_values[-1].encode()).hexdigest()
    resource_rows[-1] = tuple(last_values)
    await copy_resources(fixture, resource_rows)
    is_exposed = False
    with pytest.raises(expected_error):
        async with connection.transaction(isolation="repeatable_read"):
            async with rollup.staged_fhir_catalog_references(
                connection, schema_name=fixture.schema, dataset_id="dataset", page_rows=5000, page_bytes=4000
            ):
                is_exposed = True
    assert not is_exposed
    assert await connection.fetchval(f"SELECT count(*) FROM {relation}") == 30
    assert (
        await connection.fetchval(
            "SELECT count(*) FROM pg_class WHERE relnamespace=pg_my_temp_schema() "
            "AND relname LIKE 'fhir_catalog_refs_%'"
        )
        == 0
    )
