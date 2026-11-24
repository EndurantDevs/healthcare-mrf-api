# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Synthetic native Source inputs cross real composition, publication and pinned API reads."""

import json
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import asdict, replace
from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import UUID, uuid4

import asyncpg
import pytest

from api.network_address_scope import network_address_read_scope
from api.network_provider_only_read import read_network_provider_only_page
from api.network_provider_read import read_network_provider_page
from process import registry_native_provider_source as native
from process.network_fhir_source_witness_contract import digest
from process.network_membership_pipeline import prepare_network_candidate
from process.network_membership_publication import publish_network_candidate
from process.network_serving_read import resolve_network_serving_manifest
from process.registry_candidate_composition import (
    RegistryCompositionAddressSources,
    compose_registry_membership_candidate,
)
from process.registry_effective_network_catalog import (
    effective_catalog_networks_sql,
    effective_catalog_read_scope,
    effective_catalog_sql,
    pair_network_catalog_generation,
)
from process.registry_fhir_catalog_custody import FHIRCatalogTarget
from process.registry_fhir_catalog_extraction import extract_fhir_catalog_batch
from process.registry_fhir_catalog_fingerprints import approved_fhir_catalog_fingerprints
from process.registry_fhir_catalog_sources import FHIRCatalogSourceDisposition, fhir_catalog_source_generation
from process.registry_fhir_catalog_store import (
    admit_catalog_preparation,
    checkpoint_fhir_catalog_snapshot,
    create_fhir_catalog_candidate,
    named_catalog_preparation,
)
from process.registry_fhir_catalog_sync import project_fhir_catalog_source, stage_fhir_catalog_source
from process.registry_fhir_catalog_validation import (
    canonical_json,
    metadata_generation,
    validate_fhir_catalog_candidate,
)
from process.registry_imported_catalog_bindings import PublishedFHIRCatalogBindingScope
from process.registry_imported_catalog_preparation import select_preparation_fhir_sources, stage_native_catalog_source
from process.registry_network_catalog_read import RegistryNetworkCatalogQuery, read_registry_network_catalog
from process.registry_publication_consumer import RegistryPublicationConsumerConfig
from process.registry_publication_worker import _configured_worker_runtime
from process.registry_source_recipe_composition import SourceWitnessRuntime
from tests.registry_restricted_publisher_support import restricted_source_publisher
from tests.test_delta_dental_directory_normalization import delta_positive_descriptor as delta_positive_descriptor
from tests.test_network_compact_publication_postgres import (
    _native_execution_url,
    _reader,
    _seal,
)
from tests.test_network_compact_publication_postgres import (
    compact_publication_db as compact_publication_db,
)
from tests.test_registry_fhir_catalog_enrichment import source_row
from tests.test_registry_fhir_catalog_grant import snapshot, standing_grant
from tests.test_registry_fhir_catalog_sources import read_profile
from tests.test_registry_native_provider_source_postgres import _native_provider_descriptor
from tests.test_registry_native_ptg_scope_postgres import source_scope

pytestmark = pytest.mark.asyncio
_DELTA_NETWORK = "Example Dental Network"


async def _native_inputs(fixture, schemas, monkeypatch):
    """Replace only the external native Source session with this synthetic raw Source database."""
    source_fixture = SimpleNamespace(connection=fixture.connection, schemas=schemas)
    descriptors = [await _native_provider_descriptor(source_fixture), (await source_scope(source_fixture, "v4"))[0]]
    ptg_schema = descriptors[1]["input"]["schema_name"]
    await fixture.connection.execute(
        f'CREATE TABLE "{ptg_schema}".ptg2_current_source_snapshot(source_key text,snapshot_id text); '
        f"INSERT INTO \"{ptg_schema}\".ptg2_current_source_snapshot VALUES('source-graph','snapshot-one')"
    )

    @asynccontextmanager
    async def source_reader():
        connection = await asyncpg.connect(
            fixture.engine.url.set(drivername="postgresql").render_as_string(hide_password=False)
        )
        try:
            async with connection.transaction(isolation="repeatable_read", readonly=True):
                yield connection
        finally:
            await connection.close()

    from process import registry_imported_source_inventory

    monkeypatch.setattr(registry_imported_source_inventory, "native_imported_source_reader", source_reader)
    recipes = []
    for descriptor in descriptors:
        schema = (
            native.PREFIX + native.uuid5(native._NAMESPACE, digest(native.native_provider_source_data(descriptor))).hex
        )
        schemas.append(schema)
        async with fixture.connection.transaction():
            recipe, created = await native.prepare_native_provider_recipe(
                fixture.connection, descriptor, owner_role=fixture.roles["owner"], runtime_roles=fixture.runtime_roles
            )
        assert created
        recipes.append(recipe)
    return tuple(recipes), descriptors


async def _catalogue_mapping_pins(fixture, selected):
    async with fixture.connection.transaction(readonly=True):
        fingerprints = await approved_fhir_catalog_fingerprints(
            fixture.connection, (selected["metadata_approved_revision"],), control_schema=fixture.control_schema
        )
    selected["membership_mapping_sha256"] = fingerprints[selected["metadata_approved_revision"]]["membership"]
    selected["mapping_sha256"] = fingerprints[selected["metadata_approved_revision"]]["metadata"]


async def _catalogue(fixture, schemas, descriptors, typed_recipe=None, *, delta_descriptor=None):
    """Seed prepared metadata from the real native set-based catalogue producer."""
    identity = uuid4()
    copy_target = FHIRCatalogTarget(*(str(uuid4()) for _ in range(3)), str(identity), "fhir_catalog_" + identity.hex)
    schemas.append(copy_target.schema_name)
    descriptors = [*descriptors, *([typed_recipe.source_pin.catalog_descriptor] if typed_recipe else [])]
    if delta_descriptor is not None:
        descriptors.append(delta_descriptor)
    selected = snapshot(
        standing_grant(),
        descriptors,
        metadata_approved_revision=getattr(fixture, "approved_revision", 0),
        network_generation=None,
        manifest_sha256=None,
    )
    selected["schema_name"] = copy_target.schema_name
    await _catalogue_mapping_pins(fixture, selected)
    selected["accounting_json"] = {
        "imported_source_inventory": {
            "contract": "registry-imported-catalog-inputs-v1",
            "token": "e" * 64,
            "native_sources": [],
        }
    }
    selected["metadata_generation"] = metadata_generation(selected)
    async with fixture.connection.transaction():
        await create_fhir_catalog_candidate(
            fixture.connection,
            copy_target,
            loader_role=fixture.writer_roles["owner_role"],
            control_schema=fixture.control_schema,
        )
        for descriptor in descriptors:
            if descriptor is delta_descriptor:
                await _delta_catalogue(fixture, copy_target, selected, descriptor)
                continue
            if descriptor.get("contract") != "registry-native-network-source-v1":
                await _typed_catalogue(fixture, copy_target, selected, typed_recipe)
                continue
            await stage_native_catalog_source(
                fixture.connection,
                copy_target,
                SimpleNamespace(descriptor=descriptor, accepted_at=datetime.now(timezone.utc)),
                selected,
                loader_role=fixture.writer_roles["owner_role"],
                control_schema=fixture.control_schema,
            )
        await checkpoint_fhir_catalog_snapshot(
            fixture.connection, copy_target, selected, None, {}, control_schema=fixture.control_schema
        )
        for role in fixture.runtime_roles:
            await fixture.connection.execute(f'GRANT USAGE ON SCHEMA "{copy_target.schema_name}" TO "{role}"')
            await fixture.connection.execute(
                f'GRANT SELECT ON ALL TABLES IN SCHEMA "{copy_target.schema_name}" TO "{role}"'
            )
    return selected


async def _delta_catalogue(fixture, copy_target, selected, descriptor):
    """Use the ordinary selection and production projection with a synthetic Source replay."""
    coordinates = descriptor["coordinates"]
    disposition = FHIRCatalogSourceDisposition(
        descriptor["endpoint_id"],
        coordinates["dataset_id"],
        tuple(descriptor["source_aliases"]),
        1,
        "accepted",
        None,
        descriptor,
        datetime.now(timezone.utc),
    )
    inventory = SimpleNamespace(fhir_descriptors=(descriptor,), carried_fhir_descriptors=())
    [preferred_disposition] = select_preparation_fhir_sources(inventory, (disposition,))
    generation = fhir_catalog_source_generation(descriptor)
    organization = source_row("Organization", "network-one", {"name": _DELTA_NETWORK, "type_codes": [{"code": "ntwk"}]})
    [observed] = extract_fhir_catalog_batch((organization,), profile=descriptor["profile"]).organizations

    async def replay(replayed_descriptor, write_observations):
        assert replayed_descriptor == descriptor
        await write_observations(
            [(generation, "Organization", observed.resource_id, "organization", 0, canonical_json(asdict(observed)))]
        )
        return descriptor["admission"]["publication"]["claimed_resource_count"]

    loader = fixture.writer_roles["owner_role"]
    await stage_fhir_catalog_source(
        None,
        fixture.connection,
        copy_target,
        preferred_disposition,
        loader_role=loader,
        control_schema=fixture.control_schema,
        source_export={"replay": replay},
    )
    await project_fhir_catalog_source(
        fixture.connection,
        copy_target,
        preferred_disposition,
        selected,
        grant=standing_grant(control_schema=fixture.control_schema),
        loader_role=loader,
    )


def _runtime(fixture, selected):
    @asynccontextmanager
    async def unused_fhir_export(**_options):
        raise AssertionError("native-only publication must not request a FHIR export")
        yield

    return SourceWitnessRuntime(
        unused_fhir_export,
        (read_profile(),),
        fixture.roles["owner"],
        fixture.runtime_roles,
        catalog_bindings=PublishedFHIRCatalogBindingScope(selected["metadata_generation"], "e" * 64),
    )


@asynccontextmanager
async def _native_sealer(fixture, runtime):
    original = fixture.seal_candidate
    async with AsyncExitStack() as stack:
        consumer = getattr(fixture, "consumer", None) or RegistryPublicationConsumerConfig(
            fixture.control_schema,
            fixture.custody_roles["custodian"],
            fixture.executor_role,
            fixture.roles["reader"],
            fixture.roles["owner"],
            fixture.custody_roles["loading"],
            reader_roles=fixture.writer_roles["reader_roles"],
        )
        settings = SimpleNamespace(
            consumer=consumer,
            execution_url=_native_execution_url(fixture.execution_url),
            sql_limits=fixture.sql_limits,
            ptg=None,
            preparation_roles=fixture.preparation_roles,
            initial_address_sources=RegistryCompositionAddressSources(
                fixture.source, compact=True, source_witness_runtime=runtime
            ),
        )

        source_capabilities_by_name = await stack.enter_async_context(restricted_source_publisher(fixture))
        source_freeze = source_capabilities_by_name["freeze_preparation"]
        await _freeze_prepared_catalogue(fixture, runtime, source_freeze, stack)

        async def freeze_preparation(handoff, preparation_roles):
            result = await source_freeze(handoff, preparation_roles)
            await _assert_frozen_application_writes(fixture, handoff)
            return result

        configured = await _configured_worker_runtime(settings, stack, freeze_preparation=freeze_preparation)
        fixture.seal_candidate = configured.seal_candidate
        try:
            yield
        finally:
            fixture.seal_candidate = original


async def _remove_catalogue_preparation(fixture, candidate_id):
    async with fixture.connection.transaction():
        for table in ("preparation_relation", "preparation_claim", "preparation"):
            await fixture.connection.execute(
                "DELETE FROM hp_snapshot_retention." + table + " WHERE preparation_id=$1", UUID(candidate_id)
            )


async def _freeze_prepared_catalogue(fixture, runtime, source_freeze, stack):
    snapshot_by_field = dict(
        await fixture.connection.fetchrow(
            f'SELECT * FROM "{fixture.control_schema}".registry_fhir_catalog_snapshot WHERE metadata_generation=$1',
            runtime.catalog_bindings.metadata_generation,
        )
    )
    for name in ("custody_json", "accounting_json", "source_vector_json"):
        if isinstance(snapshot_by_field[name], str):
            snapshot_by_field[name] = json.loads(snapshot_by_field[name])
    handoff = named_catalog_preparation(snapshot_by_field, fixture.control_schema)
    stack.push_async_callback(_remove_catalogue_preparation, fixture, handoff["candidate_id"])
    async with fixture.connection.transaction():
        store, _row, claim = await admit_catalog_preparation(
            fixture.connection,
            snapshot_by_field,
            control_schema=fixture.control_schema,
            roles=fixture.preparation_roles,
        )
        if claim is not None:
            await store.yield_named_claim(claim)
    frozen = await source_freeze(handoff, fixture.preparation_roles)
    assert frozen == {"preparation_id": handoff["candidate_id"], "state": "frozen"}
    await _assert_frozen_application_writes(fixture, handoff)


async def _assert_frozen_application_writes(fixture, handoff):
    table = next(
        table
        for table in handoff["tables"]
        if table["name"] == ("registry_fhir_catalog_network" if handoff["kind"] == "catalog" else "network_membership")
    )
    relation = '"' + table["schema"] + '"."' + table["name"] + '"'
    async with fixture.execution_connection.transaction():
        with pytest.raises(asyncpg.InsufficientPrivilegeError):
            await fixture.execution_connection.execute("DELETE FROM " + relation + " WHERE false")
    reader_url = fixture.reader_url if hasattr(fixture, "reader_url") else fixture.reader_engine.url
    reader = await asyncpg.connect(
        user=reader_url.username,
        password=reader_url.password,
        database=reader_url.database,
        host=reader_url.host or reader_url.query.get("host"),
        port=reader_url.port or int(reader_url.query.get("port", 5432)),
    )
    try:
        async with reader.transaction():
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                await reader.execute("DELETE FROM " + relation + " WHERE false")
    finally:
        await reader.close()


async def _compose(fixture, recipes, runtime, *, previous=None):
    copy_target, addresses = await compose_registry_membership_candidate(
        fixture.execution_connection,
        request_id=uuid4(),
        approved_revision=getattr(fixture, "approved_revision", 0),
        expected_head=0 if previous is None else previous.generation_id,
        address_sources=RegistryCompositionAddressSources(
            fixture.source,
            compact=True,
            **({"initial_source_recipes": recipes} if previous is None else {"replacement_source_recipes": recipes}),
            source_witness_runtime=runtime,
        ),
        source_manifest=previous,
        writer_roles=fixture.writer_roles,
        control_schema=fixture.control_schema,
    )
    fixture.preparation_ids.append(copy_target.candidate_id)
    await prepare_network_candidate(
        fixture.execution_connection,
        copy_target,
        addresses,
        **fixture.writer_roles,
        control_schema=fixture.control_schema,
        compact=True,
        source_witness_runtime=runtime,
    )
    await _seal(fixture, copy_target)
    return copy_target


async def _publish(fixture, copy_target, selected, *, fail=False):
    async with fixture.execution_connection.transaction(isolation="repeatable_read"):
        result = await publish_network_candidate(
            fixture.execution_connection,
            copy_target,
            control_schema=fixture.control_schema,
            preparation_roles=fixture.preparation_roles,
        )
        await pair_network_catalog_generation(
            fixture.execution_connection,
            metadata_generation=selected["metadata_generation"],
            generation_id=result["generation_id"],
            manifest_sha256=result["manifest_sha256"],
            approved_revision=getattr(fixture, "approved_revision", 0),
            control_schema=fixture.control_schema,
        )
        if fail:
            raise RuntimeError("synthetic failure after both pointer updates")
    return result


async def _api(fixture, connection, generation=None):
    manifest = await resolve_network_serving_manifest(
        connection, generation_id=generation, control_schema=fixture.control_schema, api_read=True
    )
    page = await read_registry_network_catalog(
        connection,
        RegistryNetworkCatalogQuery(generation_id=manifest.generation_id),
        excluded_network_ids=(),
        control_schema=fixture.control_schema,
    )
    networks = tuple(sorted(entry["network_id"] for entry in page["items"]))
    is_mixed = hasattr(fixture, "typed_network_id")
    assert len(networks) == 2 + is_mixed
    records = await effective_catalog_sql(connection, manifest, fixture.control_schema)
    identities = await effective_catalog_networks_sql(connection, manifest, fixture.control_schema)
    with effective_catalog_read_scope(records, identities), network_address_read_scope(manifest, networks) as scope:
        providers = await read_network_provider_only_page(connection, scope, control_schema=fixture.control_schema)
        offices = await read_network_provider_page(connection, scope, control_schema=fixture.control_schema)
    assert providers["total_count"] == 2 + is_mixed and offices["total_count"] == int(is_mixed)
    assert {entry["provider_id"] for entry in providers["providers"]} == (
        {"1234567893", "1999999984"} | ({"1000000004"} if is_mixed else set())
    )
    memberships = await connection.fetch(
        f'SELECT network_id,provider_id FROM "{manifest.schema_name}".network_membership'
    )
    assert [tuple(row) for row in memberships] == ([(fixture.typed_network_id, "1234567893")] if is_mixed else [])
    return manifest, providers


async def test_native_aca_ptg_composition_pairs_catalogue_and_preserves_pinned_reads(
    compact_publication_db, monkeypatch
):
    fixture, schemas = compact_publication_db, []
    try:
        recipes, descriptors = await _native_inputs(fixture, schemas, monkeypatch)
        selected = await _catalogue(fixture, schemas, descriptors)
        runtime = _runtime(fixture, selected)
        async with _native_sealer(fixture, runtime):
            await _publication_rounds(fixture, recipes, runtime, selected)
    finally:
        for schema in reversed(schemas):
            await fixture.connection.execute('DROP SCHEMA IF EXISTS "' + schema + '" CASCADE')


async def test_positive_delta_catalog_publishes_with_ordinary_network_generation(
    compact_publication_db, delta_positive_descriptor, monkeypatch
):
    fixture, schemas = compact_publication_db, []
    try:
        recipes, descriptors = await _native_inputs(fixture, schemas, monkeypatch)
        incumbent = await _catalogue(fixture, schemas, descriptors)
        initial_runtime = _runtime(fixture, incumbent)
        async with _native_sealer(fixture, initial_runtime):
            initial = await _compose(fixture, recipes, initial_runtime)
            await _publish(fixture, initial, incumbent)
        async with _reader(fixture) as (_session, reader):
            before, _providers = await _api(fixture, reader)
        selected = await _catalogue(fixture, schemas, descriptors, delta_descriptor=delta_positive_descriptor)
        runtime = _runtime(fixture, selected)
        async with _native_sealer(fixture, runtime):
            async with fixture.connection.transaction(readonly=True):
                receipt = await validate_fhir_catalog_candidate(
                    fixture.connection,
                    SimpleNamespace(schema_name=selected["schema_name"]),
                    selected,
                    control_schema=fixture.control_schema,
                )
                assert receipt["tables"]["registry_fhir_catalog_source"]["rows"] == 3
            candidate = await _compose(fixture, recipes, runtime, previous=before)
            published = await _publish(fixture, candidate, selected)
        async with _reader(fixture) as (_session, reader):
            generation = published["generation_id"]
            assert generation > before.generation_id
            page = await read_registry_network_catalog(
                reader,
                RegistryNetworkCatalogQuery(generation_id=generation),
                excluded_network_ids=(),
                control_schema=fixture.control_schema,
            )
            assert _DELTA_NETWORK in {entry["display_name"] for entry in page["items"]}
    finally:
        for schema in reversed(schemas):
            await fixture.connection.execute('DROP SCHEMA IF EXISTS "' + schema + '" CASCADE')


async def _publication_rounds(fixture, recipes, runtime, selected):
    copy_target = await _compose(fixture, recipes, runtime)
    with pytest.raises(RuntimeError, match="synthetic failure"):
        await _publish(fixture, copy_target, selected, fail=True)
    assert (
        await fixture.connection.fetchval(
            f'SELECT generation_id FROM "{fixture.control_schema}".network_serving_control'
        )
        is None
    )
    await _publish(fixture, copy_target, selected)
    async with _reader(fixture) as (_session, reader):
        first, providers = await _api(fixture, reader)
        successor = await _compose(fixture, recipes, runtime, previous=first)
        await _publish(fixture, successor, selected)
        assert (await _api(fixture, reader))[0] == first
        assert (await _api(fixture, reader, first.generation_id))[1] == providers
    async with _reader(fixture) as (_session, reader):
        current, _providers = await _api(fixture, reader)
        assert current.generation_id > first.generation_id
        assert (await _api(fixture, reader, first.generation_id))[1] == providers


async def _typed_catalogue(fixture, copy_target, selected, recipe):
    """Carry an actual approved typed FHIR binding into the prepared catalogue input."""
    from uuid import UUID

    descriptor = recipe.source_pin.catalog_descriptor
    generation = digest(descriptor)
    namespace = '"' + copy_target.schema_name + '"'
    await fixture.connection.execute(
        f"INSERT INTO {namespace}.registry_fhir_catalog_source VALUES($1,$2,$3::jsonb,'published_normalized',$4,0,now(),now())",
        generation,
        recipe.binding_coordinates.source_id,
        json.dumps(descriptor),
        recipe.source_pin.capture["graph"]["counts"]["source_rows"],
    )
    binding_json = await fixture.connection.fetchval(
        f"SELECT record_json FROM \"{fixture.control_schema}\".registry_approved_record WHERE approved_revision=$1 AND record_kind='network_binding' AND record_json->>'source_system'='fhir'",
        fixture.approved_revision,
    )
    binding_by_field = json.loads(binding_json)
    binding_by_field["source_scope"] = binding_by_field.pop("source_scope_json")
    network_json = await fixture.connection.fetchval(
        f"SELECT record_json FROM \"{fixture.control_schema}\".registry_approved_record WHERE approved_revision=$1 AND record_kind='network' AND record_key=$2",
        fixture.approved_revision,
        str(binding_by_field["network_id"]),
    )
    network_by_field = json.loads(network_json)
    await fixture.connection.execute(
        f"INSERT INTO {namespace}.registry_fhir_catalog_network VALUES($1,$2,$3::text,$4::jsonb,false,1,NULL,$3::text)",
        selected["metadata_generation"],
        binding_by_field["network_id"],
        network_by_field["display_name"],
        json.dumps(network_by_field["aliases"]),
    )
    await fixture.connection.execute(
        f"INSERT INTO {namespace}.registry_fhir_catalog_binding VALUES($1,$2,$3,$4,$5::jsonb)",
        selected["metadata_generation"],
        binding_by_field["network_id"],
        UUID(binding_by_field["binding_id"]),
        recipe.binding_coordinates.source_id,
        json.dumps(binding_by_field),
    )


def _mixed_adapter(context, connection):
    from tests import test_network_fhir_source_witness_publication_postgres as publication

    worker = context.worker
    consumer = RegistryPublicationConsumerConfig(
        **publication._options(worker),
        reader_roles=tuple(sorted((worker.fixture.roles["reader"], worker.roles_by_kind["api"]))),
    )
    return SimpleNamespace(
        connection=worker.fixture.connection,
        engine=worker.fixture.engine,
        control_schema=worker.fixture.control_schema,
        execution_connection=connection,
        executor_role=worker.roles_by_kind["execution"],
        execution_url=worker.fixture.engine.url.set(
            username=worker.roles_by_kind["execution"], password=worker.role_password
        ),
        reader_url=worker.fixture.engine.url.set(username=worker.roles_by_kind["api"], password=worker.role_password),
        sql_limits=worker.sql_limits,
        roles={"owner": context.case.fixture.role_by_kind["owner"], "reader": worker.roles_by_kind["api"]},
        runtime_roles=context.runtime.runtime_roles,
        source=context.address,
        writer_roles=consumer.writer_roles,
        preparation_roles=publication._named_preparation_roles(context),
        preparation_ids=worker.fixture.preparation_ids,
        seal_candidate=None,
        consumer=consumer,
        approved_revision=context.approved.approved_revision,
        typed_network_id=context.network_id,
    )


async def test_mixed_sources_publish_native_providers_without_offices(monkeypatch):
    from tests import test_network_fhir_source_witness_publication_postgres as publication
    from tests.test_network_fhir_typed_source_postgres import _typed_worker_capture

    monkeypatch.setattr(publication, "_source_capture_for_workers", _typed_worker_capture)
    async with (
        publication._publication_case(monkeypatch, require_permission_reuse=False) as context,
        publication._login(context.worker, "execution") as writer,
    ):
        fixture, schemas = _mixed_adapter(context, writer), []
        try:
            native_recipes, descriptors = await _native_inputs(fixture, schemas, monkeypatch)
            selected = await _catalogue(fixture, schemas, descriptors, context.recipe)
            runtime = replace(
                context.runtime,
                catalog_bindings=PublishedFHIRCatalogBindingScope(selected["metadata_generation"], "e" * 64),
            )
            async with _native_sealer(fixture, runtime):
                copy_target = await _compose(fixture, (context.recipe, *native_recipes), runtime)
                await _publish(fixture, copy_target, selected)
            async with (
                publication._login(context.worker, "api") as reader,
                reader.transaction(isolation="repeatable_read", readonly=True),
            ):
                await _api(fixture, reader)
        finally:
            for schema in reversed(schemas):
                await fixture.connection.execute('DROP SCHEMA IF EXISTS "' + schema + '" CASCADE')
