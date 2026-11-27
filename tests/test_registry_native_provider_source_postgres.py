# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native COPY provider captures and current binding maps never manufacture office evidence."""

import asyncio
import json
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import asyncpg
import pytest

from process import registry_imported_catalog_bindings as bindings
from process import registry_imported_source_inventory as inventory
from process import registry_native_provider_source as providers
from process.network_approved_membership_source import ApprovedMembershipSource
from process.network_fhir_source_witness_contract import digest
from process.registry_imported_catalog_preparation import stage_native_catalog_source
from process.registry_imported_source_preparation import _prepare_native_recipes
from process.registry_native_network_catalog import native_catalog_descriptor
from tests.test_registry_fhir_catalog_grant import standing_grant
from tests.test_registry_fhir_catalog_store_postgres import storage_database as storage_database
from tests.test_registry_fhir_catalog_store_postgres import target
from tests.test_registry_fhir_catalog_validation import snapshot
from tests.test_registry_imported_source_inventory import inventory_case as inventory_case
from tests.test_registry_imported_source_inventory import native_aca_source, observe
from tests.test_registry_native_network_catalog_postgres import native_control


async def _native_provider_descriptor(fixture):
    writer = fixture.connection
    source_schema = "native_provider_source_" + uuid4().hex
    fixture.schemas.append(source_schema)
    await writer.execute(f'CREATE SCHEMA "{source_schema}"')
    await writer.execute(f'''CREATE TABLE "{source_schema}".mrf_publication_receipt(singleton boolean,import_id text,state text);
      INSERT INTO "{source_schema}".mrf_publication_receipt VALUES(true,'edition-one','complete');
      CREATE TABLE "{source_schema}".plan_npi_raw(npi bigint,checksum_network bigint,issuer_id integer,year integer);
      INSERT INTO "{source_schema}".plan_npi_raw VALUES(1234567893,23,10001,2027),(1999999984,23,10001,2027),
        (1234567893,99,10001,2027),(1999999984,23,10002,2027)''')
    source_document = native_aca_source()
    source_document["schema_name"] = source_schema
    descriptor = native_catalog_descriptor(source_document)
    return descriptor


async def _native_provider_catalog(fixture, descriptor):
    writer = fixture.connection
    await native_control(fixture)
    catalog = await target(fixture)
    selected = snapshot(standing_grant())
    async with writer.transaction():
        await stage_native_catalog_source(
            writer,
            catalog,
            SimpleNamespace(descriptor=descriptor, accepted_at=datetime.now(timezone.utc)),
            selected,
            loader_role=fixture.loader,
            control_schema=fixture.control,
        )
    await writer.execute(f'''ALTER TABLE "{fixture.control}".registry_fhir_catalog_snapshot
        ADD COLUMN metadata_approved_revision bigint DEFAULT 6,ADD COLUMN source_vector_json jsonb,
        ADD COLUMN accounting_json jsonb DEFAULT '{{}}';
        ''')
    await writer.execute(
        f"INSERT INTO \"{fixture.control}\".registry_fhir_catalog_snapshot VALUES($1,$2,now(),6,$3::jsonb,'{{}}')",
        catalog.schema_name,
        selected["metadata_generation"],
        json.dumps(
            [
                {
                    "source_generation": digest(descriptor),
                    "source_id": descriptor["coordinates"]["source_id"],
                    "status": "current",
                }
            ]
        ),
    )
    return catalog, selected


@pytest.fixture
async def native_provider_case(storage_database, monkeypatch):
    fixture = storage_database
    writer = fixture.connection
    descriptor = await _native_provider_descriptor(fixture)
    schema = (
        providers.PREFIX
        + providers.uuid5(providers._NAMESPACE, digest(providers.native_provider_source_data(descriptor))).hex
    )
    fixture.schemas.append(schema)

    @asynccontextmanager
    async def source_reader():
        driver = await asyncpg.connect(fixture.dsn.replace("postgresql+asyncpg://", "postgresql://"))
        try:
            async with driver.transaction(isolation="repeatable_read", readonly=True):
                yield driver
        finally:
            await driver.close()

    from process import registry_imported_source_inventory

    monkeypatch.setattr(registry_imported_source_inventory, "native_imported_source_reader", source_reader)

    async with writer.transaction():
        recipe, created = await providers.prepare_native_provider_recipe(
            writer, descriptor, owner_role=fixture.loader, runtime_roles=()
        )
        assert created
    catalog, selected = await _native_provider_catalog(fixture, descriptor)
    approved = ApprovedMembershipSource(6, "c" * 64)
    monkeypatch.setattr(bindings, "pin_approved_membership_source", AsyncMock(return_value=approved))
    yield SimpleNamespace(
        fixture=fixture,
        writer=writer,
        source_reader=source_reader,
        descriptor=descriptor,
        recipe=recipe,
        catalog=catalog,
        selected=selected,
        approved=approved,
        catalog_scope=bindings.PublishedFHIRCatalogBindingScope(selected["metadata_generation"]),
    )


@pytest.mark.asyncio
async def test_native_capture_and_mapping_preserve_only_source_proven_providers(native_provider_case):
    case = native_provider_case
    assert case.recipe.source_pin.capture["rows"] == 2
    async with case.writer.transaction():
        mapped = await providers.rebuild_native_provider_stage(
            case.writer,
            case.recipe,
            case.approved,
            catalog_scope=case.catalog_scope,
            control_schema=case.fixture.control,
            verify_source=True,
        )
        rows = await case.writer.fetch("SELECT * FROM " + mapped.provider_only_table + " ORDER BY provider_id")
        assert [row["provider_id"] for row in rows] == ["1234567893", "1999999984"]
        assert all(row["provider_system"] == "npi" and len(row["evidence_id"]) == 64 for row in rows)
        assert all("location_id" not in row for row in rows)
        await case.writer.execute("DROP TABLE " + mapped.provider_only_table)


@pytest.mark.asyncio
async def test_plans_sharing_a_network_map_each_provider_once(native_provider_case):
    case = native_provider_case
    relation = f'"{case.catalog.schema_name}".registry_fhir_catalog_binding'
    async with case.writer.transaction():
        # A second plan on the same network checksum, as ACA issuers publish one binding per plan.
        await case.writer.execute(
            f"INSERT INTO {relation} SELECT metadata_generation,network_id,$1,source_id,"
            f"document_json||jsonb_build_object('binding_id',$1::uuid,'source_key',document_json->>'source_key'||'-plan-b') "
            f"FROM {relation} LIMIT 1",
            uuid4(),
        )
        mapped = await providers.rebuild_native_provider_stage(
            case.writer,
            case.recipe,
            case.approved,
            catalog_scope=case.catalog_scope,
            control_schema=case.fixture.control,
        )
        rows = await case.writer.fetch(
            "SELECT provider_id FROM " + mapped.provider_only_table + " ORDER BY provider_id"
        )
        assert [row["provider_id"] for row in rows] == ["1234567893", "1999999984"]
        await case.writer.execute("DROP TABLE " + mapped.provider_only_table)


@pytest.mark.asyncio
async def test_duplicate_catalog_key_rejects_native_mapping_and_rolls_back_its_table(native_provider_case):
    case = native_provider_case
    relation = f'"{case.catalog.schema_name}".registry_fhir_catalog_binding'
    tables = "SELECT count(*) FROM pg_class WHERE relnamespace=pg_my_temp_schema() AND relname LIKE 'native_provider_mapped_%'"
    async with case.writer.transaction():
        before = await case.writer.fetchval(tables)
        await case.writer.execute(
            f"INSERT INTO {relation} SELECT metadata_generation,network_id,$1,source_id,"
            f"document_json||jsonb_build_object('binding_id',$1::uuid) FROM {relation} LIMIT 1",
            uuid4(),
        )
        with pytest.raises(ValueError, match="registry_imported_catalog_binding_conflict"):
            await providers.rebuild_native_provider_stage(
                case.writer,
                case.recipe,
                case.approved,
                catalog_scope=case.catalog_scope,
                control_schema=case.fixture.control,
            )
        assert await case.writer.fetchval(tables) == before


@pytest.mark.asyncio
async def test_native_codec_and_committed_recovery_keep_exact_capture(native_provider_case):
    case = native_provider_case
    from process.registry_source_recipe_store import canonical_registry_source_recipes, decode_registry_source_recipes

    wire = canonical_registry_source_recipes((case.recipe,))
    assert decode_registry_source_recipes(wire) == (case.recipe,)
    assert len(wire) < 10000 and '"source_kind":"native_provider"' in wire
    async with case.writer.transaction():
        recovered, created = await providers.prepare_native_provider_recipe(
            case.writer, case.descriptor, owner_role=case.fixture.loader, runtime_roles=()
        )
        assert not created and recovered == case.recipe


@pytest.mark.asyncio
async def test_imported_native_preparation_reuses_the_exact_completed_capture(
    native_provider_case, inventory_case, monkeypatch
):
    case = native_provider_case
    observed = (await observe(inventory_case)).document
    observed["native_sources"] = [case.descriptor["input"]]
    selected_inventory = inventory.ImportedSourceInventory(json.dumps(observed))
    scope = bindings.PublishedFHIRCatalogBindingScope(
        case.selected["metadata_generation"], prepared_inventory_token=selected_inventory.token
    )
    runtime = SimpleNamespace(catalog_bindings=scope, capture_owner_role=case.fixture.loader, runtime_roles=())
    monkeypatch.setattr(inventory, "current_imported_source_inventory", AsyncMock(return_value=selected_inventory))

    @asynccontextmanager
    async def capture_transaction():
        async with case.writer.transaction():
            yield case.writer

    assert await _prepare_native_recipes(capture_transaction, runtime, selected_inventory.token, (case.recipe,)) == (
        (case.recipe, False),
    )
    assert await _prepare_native_recipes(capture_transaction, runtime, selected_inventory.token, ()) == (
        (case.recipe, False),
    )
    with pytest.raises(ValueError, match="catalog_changed"):
        await _prepare_native_recipes(capture_transaction, runtime, "f" * 64, (case.recipe,))
    with pytest.raises(ValueError, match="predecessor_ambiguous"):
        await _prepare_native_recipes(
            capture_transaction, runtime, selected_inventory.token, (case.recipe, case.recipe)
        )
    await providers.require_native_provider_capture(case.writer, case.recipe.source_pin)


@pytest.mark.asyncio
async def test_changed_native_source_is_rejected_by_independent_provider_check(native_provider_case):
    case = native_provider_case
    schema = case.descriptor["coordinates"]["dataset_schema"]
    await case.writer.execute(f'DELETE FROM "{schema}".plan_npi_raw WHERE npi=1999999984')
    async with case.writer.transaction():
        with pytest.raises(ValueError, match="source_changed"):
            await providers.rebuild_native_provider_stage(
                case.writer,
                case.recipe,
                case.approved,
                catalog_scope=case.catalog_scope,
                control_schema=case.fixture.control,
                verify_source=True,
            )


@pytest.mark.asyncio
async def test_creator_failure_preserves_completed_capture_for_concurrent_reuser(native_provider_case):
    import asyncio

    from process.registry_membership_refresh_runtime import _SourceResolver

    case = native_provider_case
    creator = _SourceResolver(object())
    creator.prepared_recipes = (case.recipe,)
    creator.created_schemas = (case.recipe.source_pin.capture["schema_name"],)
    creator.prepared_command = "a" * 64
    reuser = await asyncpg.connect(case.fixture.dsn.replace("postgresql+asyncpg://", "postgresql://"))

    async def recover():
        async with reuser.transaction():
            return await providers.prepare_native_provider_recipe(
                reuser, case.descriptor, owner_role=case.fixture.loader, runtime_roles=()
            )

    try:
        recovered, _ = await asyncio.gather(recover(), creator.discard_sources(case.writer, {"request_id": uuid4()}))
        assert recovered == (case.recipe, False)
        assert creator.prepared_recipes == creator.created_schemas == ()
        assert await reuser.fetchval(
            "SELECT to_regnamespace($1) IS NOT NULL", case.recipe.source_pin.capture["schema_name"]
        )
        await providers.require_native_provider_capture(reuser, case.recipe.source_pin)
    finally:
        await reuser.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("failed", [False, True])
async def test_native_mapping_survives_phase_commit_and_is_cleaned_exactly(native_provider_case, failed):
    from process.registry_candidate_composition import _drop_composition_stages
    from process.registry_source_recipe_composition import RegistrySourceRecipeStage

    case = native_provider_case
    async with case.writer.transaction():
        mapped = await providers.rebuild_native_provider_stage(
            case.writer,
            case.recipe,
            case.approved,
            catalog_scope=case.catalog_scope,
            control_schema=case.fixture.control,
        )
    assert await case.writer.fetchval("SELECT to_regclass($1) IS NOT NULL", mapped.provider_only_table)
    async with case.writer.transaction():
        assert await case.writer.fetchval("SELECT count(*) FROM " + mapped.provider_only_table) == 2
    stage = RegistrySourceRecipeStage("unused_ordinary_stage", 0, 0, "f" * 64, native_provider_stages=(mapped,))
    prepared = (None, None, None, stage, SimpleNamespace(add_table=None, remove_table=None))
    await _drop_composition_stages(case.writer, prepared, ValueError("composition failed") if failed else None)
    assert await case.writer.fetchval("SELECT to_regclass($1) IS NULL", mapped.provider_only_table)
    assert await case.writer.fetchval(
        "SELECT to_regnamespace($1) IS NOT NULL", case.recipe.source_pin.capture["schema_name"]
    )


async def test_shared_native_rows_survive_commit_and_copy_only_the_requested_source(native_provider_case, monkeypatch):
    from process import network_provider_only_projection as projection
    from process.registry_source_recipe_composition import RegistrySourceRecipeStage

    case = native_provider_case
    read_context = SimpleNamespace(
        source_witness_runtime=SimpleNamespace(catalog_bindings=case.catalog_scope), source_manifest=None
    )
    empty_stage = RegistrySourceRecipeStage("unused", 0, 0, "f" * 64)
    stage_arguments = (case.writer, (case.recipe,), case.approved, empty_stage)
    stage_options_by_name = dict(read_context=read_context, control_schema=case.fixture.control, verify_source=False)
    table_count_sql = "SELECT count(*) FROM pg_class WHERE relnamespace=pg_my_temp_schema() AND relname LIKE 'native_provider_batch_%'"
    async with case.writer.transaction():
        before = await case.writer.fetchval(table_count_sql)
        with monkeypatch.context() as patch:
            patch.setattr(providers, "network_source_mapped", AsyncMock(side_effect=asyncio.CancelledError))
            with pytest.raises(asyncio.CancelledError):
                await providers.append_native_provider_stages(*stage_arguments, **stage_options_by_name)
        assert await case.writer.fetchval(table_count_sql) == before
        stage = await providers.append_native_provider_stages(*stage_arguments, **stage_options_by_name)
        (native,) = stage.native_provider_stages
        assert native.source_rows == 2
    async with case.writer.transaction():
        relation = native.provider_only_table
        keys = providers.native_provider_batch_keys(relation)
        assert await case.writer.fetchval(table_count_sql) == before + 2
        await case.writer.execute(f"INSERT INTO {keys} VALUES (1,decode(repeat('c',64),'hex'))")
        await case.writer.execute(f"INSERT INTO {relation} SELECT 1,'npi','1999999992',repeat('b',64),1")
        expected = await case.writer.fetch(
            f"SELECT {projection.COLUMNS} FROM {relation} WHERE source_slot=0 ORDER BY provider_id",
        )
        destination = 'pg_temp."native_selected_' + uuid4().hex + '"'
        await projection._create_evidence_heap(case.writer, destination, temporary=True)
        await projection._append_native_staged_sources(
            case.writer, destination, (case.recipe,), stage, case.approved, case.fixture.control
        )
        assert (
            await case.writer.fetch(f"SELECT {projection.COLUMNS} FROM {destination} ORDER BY provider_id") == expected
        )
        assert [fact["provider_id"] for fact in expected] == ["1234567893", "1999999984"]
        await case.writer.execute(f"TRUNCATE {destination}")
        for change in (
            f"UPDATE {keys} SET source_slot=0 WHERE source_slot=1",
            f"DELETE FROM {keys} WHERE source_slot=0",
        ):
            async with case.writer.transaction():
                await case.writer.execute(change)
                with pytest.raises(
                    projection.ProviderOnlyProjectionError, match="network_provider_only_projection_differs"
                ):
                    await projection._append_native_staged_sources(
                        case.writer, destination, (case.recipe,), stage, case.approved, case.fixture.control
                    )
                assert await case.writer.fetchval(f"SELECT count(*) FROM {destination}") == 0
        await providers.drop_native_provider_stages(case.writer, stage)
        await case.writer.execute(f"DROP TABLE {destination}")
        assert await case.writer.fetchval(table_count_sql) == before
