# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Read catalog facts from an actual immutable capture through its genuine reader."""

import json
from contextlib import asynccontextmanager
from dataclasses import asdict, replace
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID, uuid4, uuid5

import pytest

from process import network_fhir_retained_capture as captures
from process import registry_fhir_catalog_retained as retained
from process import registry_fhir_catalog_sync as catalog_sync
from process.network_fhir_retained_capture_contract import RetainedFHIRCaptureError
from process.network_membership_pipeline import prepare_and_publish_network_candidate
from process.network_serving_read import NetworkServingReadUnavailable, resolve_network_serving_manifest
from process.network_source_binding_store import NetworkSourceBindingBatchCommand, apply_network_source_binding_batch
from process.registry_candidate_composition import (
    RegistryCompositionAddressSources,
    compose_registry_membership_candidate,
)
from process.registry_fhir_catalog_extraction import (
    RESOURCE_TYPES,
    FHIRCatalogExtractionError,
    extract_fhir_catalog_batch,
    iter_fhir_catalog_pages,
)
from process.registry_fhir_catalog_grant import FHIRCatalogStandingGrant, build_fhir_catalog_publication_runtime
from process.registry_fhir_catalog_publication import (
    complete_queued_fhir_catalog_publication,
    enqueue_fhir_catalog_publication,
)
from process.registry_fhir_catalog_read import FHIRCatalogQuery, read_fhir_catalog
from process.registry_fhir_catalog_sources import fhir_catalog_source_generation
from process.registry_fhir_catalog_store import discard_fhir_catalog_candidate
from process.registry_fhir_catalog_sync import prepare_fhir_catalog_snapshot
from process.registry_publication_queue import claim_registry_publication_request
from tests import test_network_fhir_retained_capture_postgres as retained_fixtures
from tests.test_network_fhir_retained_capture_postgres import _approved_capture_recipe, _capture, _retained_addresses
from tests.test_network_fhir_retained_capture_postgres import legacy_capture_db as legacy_capture_db
from tests.test_network_serving_schema_postgres import serving_schema as serving_schema
from tests.test_registry_approval_store_postgres import _actor, _approve, _command, _create, _draft
from tests.test_registry_fhir_catalog_custody_postgres import metadata_custody as metadata_custody
from tests.test_registry_fhir_catalog_retained import descriptor_fixture


@pytest.mark.parametrize("legacy_capture_db", ["legacy", "modern"], indirect=True)
async def test_catalog_pages_use_retained_heap_and_one_content_admission(legacy_capture_db, monkeypatch):
    fixture = legacy_capture_db
    capture = await _capture(fixture)
    disposition, recipe, _ = await descriptor_fixture(monkeypatch, value=capture)
    reader = await fixture.connect("reader")
    disposition.descriptor["database_oid"] = await reader.fetchval(
        "SELECT oid::bigint FROM pg_database WHERE datname=current_database()"
    )
    original = captures.require_retained_fhir_capture
    admissions = []

    async def verify(connection, selected, *, verify_content=True):
        admissions.append(verify_content)
        return await original(connection, selected, verify_content=verify_content)

    monkeypatch.setattr(captures, "require_retained_fhir_capture", verify)
    async with reader.transaction(isolation="repeatable_read", readonly=True):
        pin = await retained.pin_retained_fhir_catalog_source(reader, disposition.descriptor)
        resource_rows = [
            resource_row
            async for page in iter_fhir_catalog_pages(
                reader,
                schema_name=recipe.source_pin.read_schema_name,
                dataset_id=recipe.source_pin.dataset_id,
                page_rows=1,
                source_read=pin,
            )
            for resource_row in page
        ]
        await pin.verify(reader, capture)
        assert resource_rows and any(resource_row["resource_type"] == "Organization" for resource_row in resource_rows)
        batch = extract_fhir_catalog_batch(tuple(resource_rows), profile="retained_normalized")
        assert batch.organizations and batch.edges
        assert admissions.count(True) == 1
        assert admissions.count(False) >= len(resource_rows) + 1
        with pytest.raises(FHIRCatalogExtractionError, match="retained_read_changed"):
            async for _ in iter_fhir_catalog_pages(
                reader,
                schema_name=recipe.source_pin.schema_name,
                dataset_id=recipe.source_pin.dataset_id,
                source_read=pin,
            ):
                pytest.fail("an original mutable source heap was accepted")
    async with reader.transaction(isolation="repeatable_read", readonly=True):
        with pytest.raises(RetainedFHIRCaptureError):
            await pin.verify(reader, capture)


async def test_catalog_capture_pin_rejects_database_identity_before_content(legacy_capture_db, monkeypatch):
    fixture = legacy_capture_db
    capture = await _capture(fixture)
    disposition, _, _ = await descriptor_fixture(monkeypatch, value=capture)
    reader = await fixture.connect("reader")
    disposition.descriptor["database_oid"] = 1 + await reader.fetchval(
        "SELECT oid::bigint FROM pg_database WHERE datname=current_database()"
    )
    pinner = AsyncMock()
    monkeypatch.setattr(retained, "pin_retained_fhir_read", pinner)
    async with reader.transaction(isolation="repeatable_read", readonly=True):
        with pytest.raises(retained.FHIRCatalogRetainedError):
            await retained.pin_retained_fhir_catalog_source(reader, disposition.descriptor)
    pinner.assert_not_awaited()


@asynccontextmanager
async def _borrow(connection):
    yield connection


@pytest.fixture
def source_organization_inputs(monkeypatch):
    original = retained_fixtures._contents
    build_summary = retained_fixtures.build_source_summary

    def summary(**fields):
        fields["count_by_field"]["organization_resources"] = fields["count_by_resource"]["Organization"]
        return build_summary(**fields)

    def resources(source_id):
        rows = original(source_id)
        template = next(payload for kind, _, payload in rows if kind == "Organization")
        for kind, _, payload in rows:
            if kind == "InsurancePlan":
                payload["owned_by_ref"] = "Organization/catalog-insurer"
        rows.extend(
            [
                (
                    "Organization",
                    "catalog-insurer",
                    {
                        **template,
                        "resource_id": "catalog-insurer",
                        "type_codes": [],
                        "name": "Imported Insurer",
                        "part_of_ref": "Organization/catalog-parent",
                    },
                ),
                (
                    "Organization",
                    "catalog-parent",
                    {**template, "resource_id": "catalog-parent", "type_codes": [], "name": "Imported Parent"},
                ),
            ]
        )
        return rows

    monkeypatch.setattr(retained_fixtures, "_contents", resources)
    monkeypatch.setattr(retained_fixtures, "build_source_summary", summary)


@pytest.fixture
def catalog_organization_capture(source_organization_inputs, legacy_capture_db):
    return legacy_capture_db


async def _publish_retained_membership(fixture, serving, recipe, approved, addresses, expected_head):
    connection, control, _ = serving
    previous = None
    if expected_head:
        async with connection.transaction(isolation="repeatable_read", readonly=True):
            previous = await resolve_network_serving_manifest(
                connection, generation_id=expected_head, control_schema=control
            )
    request = uuid4()
    fixture.captures.append("network_composition_" + uuid5(request, "address:" + approved.generation_id).hex)
    roles_by_kind = {
        "owner_role": fixture.role_by_kind["owner"],
        "loader_roles": (),
        "reader_roles": (fixture.role_by_kind["reader"],),
    }
    try:
        copy_target, candidate_addresses = await compose_registry_membership_candidate(
            connection,
            request_id=request,
            approved_revision=approved.approved_revision,
            expected_head=expected_head,
            address_sources=RegistryCompositionAddressSources(
                addresses,
                initial_source_recipes=(recipe,) if previous is None else (),
                replacement_source_recipes=(recipe,) if previous is not None else (),
            ),
            writer_roles=roles_by_kind,
            source_manifest=previous,
            control_schema=control,
        )
        await prepare_and_publish_network_candidate(
            connection, copy_target, candidate_addresses, **roles_by_kind, control_schema=control
        )
        async with connection.transaction(isolation="repeatable_read", readonly=True):
            return await resolve_network_serving_manifest(connection, control_schema=control)
    finally:
        fixture.captures.extend(
            await connection.fetchval(
                f'SELECT coalesce(array_agg(schema_name),ARRAY[]::text[]) FROM "{control}".network_membership_candidate '
                "WHERE dataset_id=$1",
                uuid5(request, "dataset"),
            )
        )


async def _publish_catalog(connection, config, source_pool, snapshot, previous):
    runtime = build_fhir_catalog_publication_runtime(config.grant, source_pool)
    command_by_field = {
        "operation": "publish_fhir_catalog",
        "metadata_generation": snapshot["metadata_generation"],
        "expected_metadata_generation": previous["metadata_generation"] if previous else None,
        "expected_serving_generation": snapshot["network_generation"],
        "expected_approved_revision": snapshot["metadata_approved_revision"],
        "standing_grant_sha256": config.grant.sha256,
    }
    async with connection.transaction(isolation="repeatable_read"):
        queued = await enqueue_fhir_catalog_publication(
            connection,
            command_by_field,
            authorize_metadata=runtime.authorize_metadata,
            control_schema=config.grant.control_schema,
        )
        claimed = await claim_registry_publication_request(
            connection,
            operation_family="publication",
            allow_metadata=True,
            metadata_only=True,
            request_id=UUID(queued["request_id"]),
            control_schema=config.grant.control_schema,
        )
    return await complete_queued_fhir_catalog_publication(
        connection,
        claimed["request_id"],
        claimed["lease_id"],
        authorize_metadata=runtime.authorize_metadata,
        verify_sources=runtime.verify_sources,
        source_fence=runtime.source_fence,
        control_schema=config.grant.control_schema,
    )


async def _retained_catalog_config(source, metadata, serving, *, retained_sources=()):
    """Configure fixture metadata roles without granting any Source write authority."""
    connection, control, _ = serving
    metadata.control_schema = control
    await connection.execute(f'GRANT USAGE ON SCHEMA "{control}" TO "{metadata.roles["publisher"]}"')
    await connection.execute(f'GRANT SELECT ON ALL TABLES IN SCHEMA "{control}" TO "{metadata.roles["publisher"]}"')
    await connection.execute(
        f'GRANT INSERT ON "{control}".registry_fhir_catalog_snapshot TO "{metadata.roles["publisher"]}"'
    )
    grant = FHIRCatalogStandingGrant(
        "retained-roundtrip", 1, control, (), retained_source_recipes=True, retained_sources=retained_sources
    )
    config = SimpleNamespace(
        grant=grant,
        loader_role=metadata.roles["loader"],
        reader_roles=(source.role_by_kind["reader"],),
        consumer=SimpleNamespace(physical_owner_role=metadata.roles["owner"]),
    )
    reader = await source.connect("reader")
    return (
        config,
        reader,
        SimpleNamespace(acquire=lambda: _borrow(reader)),
        metadata.sealer_pool,
    )


@asynccontextmanager
async def _retained_roundtrip_case(source_fixture, serving, metadata, *, attach_directory=True):
    """Own real capture, recipe and metadata resources through both publications."""
    connection, control, _ = serving
    capture = await _capture(source_fixture)
    recipe, approved, canonical_id = await _approved_capture_recipe(source_fixture, serving, capture)
    canonical_name = await connection.fetchval(
        f"SELECT record_json->>'display_name' FROM \"{control}\".registry_approved_record "
        "WHERE approved_revision=$1 AND record_kind='network' AND record_key=$2",
        approved.approved_revision,
        str(canonical_id),
    )
    addresses = manifest = None
    if attach_directory:
        addresses = await _retained_addresses(source_fixture, serving)
        for extension in ("intarray", "btree_gin", "postgis"):
            await connection.execute(f"CREATE EXTENSION IF NOT EXISTS {extension}")
        manifest = await _publish_retained_membership(source_fixture, serving, recipe, approved, addresses, 0)
    config, reader, source_pool, sealer_pool = await _retained_catalog_config(
        source_fixture, metadata, serving, retained_sources=() if attach_directory else (recipe,)
    )
    try:
        yield SimpleNamespace(
            source=source_fixture,
            serving=serving,
            connection=connection,
            control=control,
            capture=capture,
            recipe=recipe,
            approved=approved,
            canonical_id=canonical_id,
            canonical_name=canonical_name,
            addresses=addresses,
            manifest=manifest,
            config=config,
            reader=reader,
            source_pool=source_pool,
            sealer_pool=sealer_pool,
        )
    finally:
        schemas = await connection.fetch(
            "SELECT nspname FROM pg_namespace WHERE nspowner=ANY(SELECT oid FROM pg_roles WHERE rolname=ANY($1::text[]))",
            [metadata.roles["loader"], metadata.roles["owner"]],
        )
        metadata.schemas.extend(entry["nspname"] for entry in schemas if entry["nspname"] not in metadata.schemas)


async def _retained_source_row(case, snapshot, generation):
    """Read the sealed descriptor and its original acceptance timestamp."""
    return await case.connection.fetchrow(
        f'SELECT accepted_at,descriptor_json::text FROM "{snapshot["schema_name"]}".registry_fhir_catalog_source '
        "WHERE source_generation=$1",
        generation,
    )


@pytest.fixture
def reviewed_product_inputs(source_organization_inputs, monkeypatch):
    original = retained_fixtures._contents

    def resources(source_id):
        rows = original(source_id)
        network = next(payload for kind, identity, payload in rows if identity == "network-one")
        plan = next(payload for kind, identity, payload in rows if identity == "plan-one")
        plan["network_refs"] = ["Organization/network-one", "Organization/network-two"]
        rows.extend(
            [
                ("Organization", "network-two", {**network, "resource_id": "network-two", "name": "Second network"}),
                (
                    "InsurancePlan",
                    "plan-two",
                    {
                        **plan,
                        "resource_id": "plan-two",
                        "name": "Unselected plan",
                        "network_refs": ["Organization/network-two"],
                    },
                ),
            ]
        )
        return rows

    monkeypatch.setattr(retained_fixtures, "_contents", resources)


@pytest.fixture
def reviewed_product_capture(reviewed_product_inputs, legacy_capture_db):
    return legacy_capture_db


async def _approve_catalog_products(case):
    """Use real native reviewed bindings and approval snapshots for two distinct products."""
    connection, control, engine = case.serving
    actor = _actor()
    networks = [
        await _draft(
            engine,
            control,
            replace(_create("network"), fields={"display_name": "Reviewed product " + label, "aliases": []}),
            actor,
        )
        for label in ("one", "two")
    ]
    inputs = [
        {
            "binding_id": str(uuid4()),
            **asdict(case.recipe.binding_coordinates),
            "source_key": "InsurancePlan/plan-one",
            "source_scope_json": {
                "insurance_plan_id": "plan-one",
                "capture_sha256": case.capture.capture_sha256,
                "product_network_id": network["record_id"],
            },
            "network_id": network["record_id"],
            "evidence_id": "reviewed-product-" + str(index),
            "evidence_sha256": str(index) * 64,
            "operation": "bind",
            "expected_revision": 0,
            "expected_network_id": None,
        }
        for index, network in enumerate(networks, 1)
    ]
    async with connection.transaction():
        bindings = (
            await apply_network_source_binding_batch(
                connection,
                NetworkSourceBindingBatchCommand(
                    json.dumps(inputs).encode(), "Review complete plan products", uuid4().hex
                ),
                actor,
                control_schema=control,
            )
        )["records"]
    await _approve(connection, control, await _command(connection, control, *networks, *bindings), actor)
    return inputs


async def _product_sealer_read_access(case, metadata):
    """Permit exact native sealing and cleanup through genuine fixture logins."""
    await case.connection.execute(
        f'GRANT "{case.source.role_by_kind["reader"]}" TO "{metadata.roles["publisher"]}" '
        "WITH ADMIN FALSE, INHERIT TRUE, SET FALSE"
    )
    await case.connection.execute(f'GRANT USAGE ON SCHEMA "{case.control}" TO "{metadata.roles["loader"]}"')
    await case.connection.execute(
        f'GRANT SELECT ON "{case.control}".registry_fhir_catalog_snapshot TO "{metadata.roles["loader"]}"'
    )
    async with case.sealer_pool.acquire() as sealer:
        assert await sealer.fetchval("SELECT current_user=session_user")
        assert await sealer.fetchval("SELECT current_user") == metadata.roles["publisher"]


async def _publish_reviewed_products(case, products):
    snapshot, previous, accounting = await prepare_fhir_catalog_snapshot(
        case.config, case.connection, case.source_pool, case.sealer_pool
    )
    assert previous is None
    assert accounting["products"] == {"binding": 2, "relationship": 8}
    await _publish_catalog(case.connection, case.config, case.source_pool, snapshot, previous)
    async with case.reader.transaction(isolation="repeatable_read", readonly=True):
        page = await read_fhir_catalog(
            case.reader, FHIRCatalogQuery(plan_search="Example"), excluded_network_ids=(), control_schema=case.control
        )
    by_id = {network_document["network_id"]: network_document for network_document in page["items"]}
    relationship_ids = set()
    for product in products:
        network_document = by_id[product["network_id"]]
        assert network_document["display_name"].startswith("Reviewed product ")
        assert network_document["directory_available"] is False
        assert network_document["source_bindings"]["total"] == 1
        binding = network_document["source_bindings"]["items"][0]
        for field in ("binding_id", "source_key", "evidence_id", "evidence_sha256"):
            assert binding[field] == product[field]
        assert binding["source_scope"] == product["source_scope_json"] and binding["status"] == "last_good"
        relationships = network_document["matching_relationships"]["items"]
        assert network_document["matching_relationships"]["total"] == len(relationships) == 4
        assert {entry["plan"]["resource_id"] for entry in relationships} == {"plan-one"}
        assert {entry["plan"]["scope_path"] for entry in relationships} == {
            "InsurancePlan/plan-one/normalized_aggregate/0",
            "InsurancePlan/plan-one/normalized_aggregate/1",
        }
        insurers = [entry for entry in relationships if entry["company_role"] == "insurer"]
        assert len(insurers) == 4
        assert all(entry["company"]["display_name"] == "Imported Insurer" for entry in insurers)
        assert sum(entry["group"]["display_name"] == "Imported Parent" for entry in insurers) == 2
        identities = {entry["relationship_id"] for entry in relationships}
        assert not relationship_ids & identities
        relationship_ids.update(identities)
    assert not await case.connection.fetchval(
        f'SELECT EXISTS(SELECT FROM "{case.control}".network_registry_alias WHERE network_id=ANY($1::integer[]))',
        [product["network_id"] for product in products],
    )
    return snapshot


async def test_reviewed_retained_products_publish_through_native_sealer(
    reviewed_product_capture, serving_schema, metadata_custody
):
    async with _retained_roundtrip_case(
        reviewed_product_capture, serving_schema, metadata_custody, attach_directory=False
    ) as case:
        await _product_sealer_read_access(case, metadata_custody)
        products = await _approve_catalog_products(case)
        snapshot = await _publish_reviewed_products(case, products)
        assert case.sealer_pool.get_size() == case.sealer_pool.get_max_size() == 2
        replay, previous, _ = await prepare_fhir_catalog_snapshot(
            case.config, case.connection, case.source_pool, case.sealer_pool
        )
        assert replay is None and previous["metadata_generation"] == snapshot["metadata_generation"]


@pytest.mark.parametrize("damage", ["omitted_products", "fresh_observation", "carried_observation"])
async def test_reviewed_retained_product_seal_rejects_untrusted_candidate(
    reviewed_product_capture, serving_schema, metadata_custody, monkeypatch, damage
):
    async with _retained_roundtrip_case(
        reviewed_product_capture, serving_schema, metadata_custody, attach_directory=False
    ) as case:
        await _product_sealer_read_access(case, metadata_custody)
        products = await _approve_catalog_products(case)
        previous = None
        if damage == "carried_observation":
            previous = await _publish_reviewed_products(case, products)
            actor = _actor()
            group = await _draft(case.serving[2], case.control, _create("group"), actor)
            await _approve(case.connection, case.control, await _command(case.connection, case.control, group), actor)
        real_seal = catalog_sync._seal_catalog_snapshot
        changed_candidates = []

        async def corrupt_then_seal(config, target, snapshot, pool, *, source_pool=None):
            changed_candidates.append(target)
            namespace = '"' + target.schema_name + '"'
            if previous:
                assert snapshot["source_vector_json"] == previous["source_vector_json"]
            if damage == "omitted_products":
                await case.connection.execute(
                    f"DELETE FROM {namespace}.registry_fhir_catalog_relationship WHERE binding_id=ANY($1::uuid[])",
                    [UUID(product["binding_id"]) for product in products],
                )
                await case.connection.execute(
                    f"DELETE FROM {namespace}.registry_fhir_catalog_binding WHERE document_json->'source_scope' ? 'insurance_plan_id'"
                )
            else:
                assert (
                    await case.connection.execute(
                        f"UPDATE {namespace}.registry_fhir_catalog_observation SET document_json="
                        "jsonb_set(document_json,'{plan_name}','\"Changed retained plan\"'::jsonb) "
                        "WHERE resource_type='InsurancePlan' AND resource_id='plan-one' AND observation_kind='edge'"
                    )
                    == "UPDATE 2"
                )
            await real_seal(config, target, snapshot, pool, source_pool=source_pool)

        monkeypatch.setattr(catalog_sync, "_seal_catalog_snapshot", corrupt_then_seal)
        message = "observed_projection_changed" if damage == "omitted_products" else "observation_facts_changed"
        with pytest.raises(ValueError, match=message):
            await prepare_fhir_catalog_snapshot(case.config, case.connection, case.source_pool, case.sealer_pool)
        assert len(changed_candidates) == 1
        async with metadata_custody.loader.transaction():
            assert await discard_fhir_catalog_candidate(
                metadata_custody.loader,
                changed_candidates[0],
                loader_role=case.config.loader_role,
                control_schema=case.control,
            )
        assert await case.connection.fetchval("SELECT to_regnamespace($1)", changed_candidates[0].schema_name) is None
        assert await case.connection.fetchval(
            f'SELECT metadata_generation FROM "{case.control}".network_serving_control WHERE id=1'
        ) == (previous["metadata_generation"] if previous else None)


async def test_typed_plan_key_without_product_scope_cannot_bypass_sealing(
    reviewed_product_capture, serving_schema, metadata_custody, monkeypatch
):
    async with _retained_roundtrip_case(
        reviewed_product_capture, serving_schema, metadata_custody, attach_directory=False
    ) as case:
        await _product_sealer_read_access(case, metadata_custody)
        actor = _actor()
        network = await _draft(case.serving[2], case.control, _create("network"), actor)
        binding_input_by_key = {
            "binding_id": str(uuid4()),
            **asdict(case.recipe.binding_coordinates),
            "source_key": "InsurancePlan/plan-one",
            "source_scope_json": {"organization_id": "network-two", "capture_sha256": case.capture.capture_sha256},
            "network_id": network["record_id"],
            "evidence_id": "reviewed-legacy-scope",
            "evidence_sha256": "e" * 64,
            "operation": "bind",
            "expected_revision": 0,
            "expected_network_id": None,
        }
        async with case.connection.transaction():
            binding = (
                await apply_network_source_binding_batch(
                    case.connection,
                    NetworkSourceBindingBatchCommand(
                        json.dumps([binding_input_by_key]).encode(), "Retain exact legacy scope", uuid4().hex
                    ),
                    actor,
                    control_schema=case.control,
                )
            )["records"][0]
        await _approve(
            case.connection, case.control, await _command(case.connection, case.control, network, binding), actor
        )
        omitted = AsyncMock(return_value={"binding": 0, "relationship": 0})
        monkeypatch.setattr(catalog_sync, "project_reviewed_fhir_catalog_products", omitted)
        with pytest.raises(ValueError) as refused:
            await prepare_fhir_catalog_snapshot(case.config, case.connection, case.source_pool, case.sealer_pool)
        assert refused.value.args == ("registry_fhir_catalog_product_unavailable",)
        omitted.assert_awaited_once()
        candidate = omitted.await_args.args[1]
        async with metadata_custody.loader.transaction():
            assert await discard_fhir_catalog_candidate(
                metadata_custody.loader,
                candidate,
                loader_role=case.config.loader_role,
                control_schema=case.control,
            )
        assert await case.connection.fetchval("SELECT to_regnamespace($1)", candidate.schema_name) is None
        assert (
            await case.connection.fetchval(
                f'SELECT metadata_generation FROM "{case.control}".network_serving_control WHERE id=1'
            )
            is None
        )
        assert (
            await case.connection.fetchval(f'SELECT count(*) FROM "{case.control}".registry_fhir_catalog_snapshot') == 0
        )


async def _assert_retained_catalog_page(case):
    """Read the real catalog and require the approved canonical network identity."""
    async with case.reader.transaction(isolation="repeatable_read", readonly=True):
        page = await read_fhir_catalog(
            case.reader, FHIRCatalogQuery(), excluded_network_ids=(), control_schema=case.control
        )
    assert len(page["items"]) == 1
    network = page["items"][0]
    assert network["network_id"] == case.canonical_id and network["display_name"] == case.canonical_name
    assert network["source_bindings"]["items"][0]["status"] == "last_good"
    return page


async def _initial_retained_catalog(case):
    """Stage, seal, publish and query the first genuinely admitted retained recipe."""
    snapshot, previous, accounting = await prepare_fhir_catalog_snapshot(
        case.config, case.connection, case.source_pool, case.sealer_pool
    )
    assert previous is None and accounting["accepted_sources"] == 1
    assert accounting["sources"][0]["extraction"]["source_rows"] == sum(
        count for resource_type, count in case.capture.origin.resource_counts if resource_type in RESOURCE_TYPES
    )
    published = await _publish_catalog(case.connection, case.config, case.source_pool, snapshot, previous)
    assert published["canonical_entities_deleted"] == published["canonical_entities_archived"] == 0
    generation = snapshot["source_vector_json"][0]["source_generation"]
    original = await _retained_source_row(case, snapshot, generation)
    await _assert_retained_catalog_page(case)
    return SimpleNamespace(snapshot=snapshot, generation=generation, source_row=original)


async def _replace_retained_manifest(case):
    """Publish the same capture through a successor and retire the former locator."""
    manifest = await _publish_retained_membership(
        case.source, case.serving, case.recipe, case.approved, case.addresses, case.manifest.generation_id
    )
    await case.connection.execute(
        f'UPDATE "{case.control}".network_serving_manifest SET eligible=false WHERE generation_id=$1',
        case.manifest.generation_id,
    )
    async with case.connection.transaction(isolation="repeatable_read", readonly=True):
        with pytest.raises(NetworkServingReadUnavailable):
            await resolve_network_serving_manifest(
                case.connection, generation_id=case.manifest.generation_id, control_schema=case.control
            )
    return manifest


async def _assert_cached_retained_catalog(case, first, manifest):
    """Reuse facts across manifest retirement while preserving source identity and timestamp."""
    snapshot, previous, accounting = await prepare_fhir_catalog_snapshot(
        case.config, case.connection, case.source_pool, case.sealer_pool
    )
    assert snapshot["source_vector_json"] == first.snapshot["source_vector_json"]
    assert accounting["sources"][0]["extraction"] == {"reused_observations": True}
    cached = await _retained_source_row(case, snapshot, first.generation)
    descriptor = json.loads(cached["descriptor_json"])
    assert cached["accepted_at"] == first.source_row["accepted_at"]
    assert fhir_catalog_source_generation(descriptor) == first.generation
    assert descriptor["retained_manifest"]["generation_id"] == manifest.generation_id
    assert (
        json.loads(first.source_row["descriptor_json"])["retained_manifest"]["generation_id"]
        == case.manifest.generation_id
    )
    await _publish_catalog(case.connection, case.config, case.source_pool, snapshot, previous)
    page = await _assert_retained_catalog_page(case)
    assert [network["network_id"] for network in page["items"]] == [case.canonical_id]
    assert page["items"][0]["source_bindings"]["items"][0]["accepted_at"] == first.source_row[
        "accepted_at"
    ].isoformat().replace("+00:00", "Z")


@pytest.mark.parametrize("legacy_capture_db", ["modern"], indirect=True)
async def test_published_retained_recipe_catalog_roundtrip_and_manifest_rollover(
    legacy_capture_db, serving_schema, metadata_custody
):
    """Use real recipe and capture authority through catalog publication and cached replay."""
    async with _retained_roundtrip_case(legacy_capture_db, serving_schema, metadata_custody) as case:
        first = await _initial_retained_catalog(case)
        manifest = await _replace_retained_manifest(case)
        await _assert_cached_retained_catalog(case, first, manifest)


@pytest.mark.parametrize("legacy_capture_db", ["modern"], indirect=True)
async def test_granted_capture_catalog_publishes_and_replays_without_directory(
    catalog_organization_capture, serving_schema, metadata_custody
):
    async with _retained_roundtrip_case(
        catalog_organization_capture, serving_schema, metadata_custody, attach_directory=False
    ) as case:
        first = await _initial_retained_catalog(case)
        assert first.snapshot["network_generation"] is first.snapshot["manifest_sha256"] is None
        page = await _assert_retained_catalog_page(case)
        assert page["generation"] is page["approved_custom_revision"] is None
        assert page["items"][0]["directory_available"] is False
        mapping = next(
            relationship
            for relationship in page["items"][0]["matching_relationships"]["items"]
            if relationship["group"]["group_id"]
        )
        assert mapping["company"]["display_name"] == "Imported Insurer"
        assert mapping["group"]["display_name"] == "Imported Parent"
        assert not await case.connection.fetchval(
            f'SELECT EXISTS(SELECT FROM "{case.control}".registry_approved_record '
            "WHERE record_kind IN ('company','group'))"
        )
        async with case.reader.transaction(isolation="repeatable_read", readonly=True):
            filtered = await read_fhir_catalog(
                case.reader,
                FHIRCatalogQuery(company_id=mapping["company"]["company_id"], plan_search="Example"),
                excluded_network_ids=(),
                control_schema=case.control,
            )
        assert filtered["total"] == 1
        assert (
            await case.connection.fetchval(
                f'SELECT generation_id FROM "{case.control}".network_serving_control WHERE id=1'
            )
            is None
        )
        snapshot, previous, _ = await prepare_fhir_catalog_snapshot(
            case.config, case.connection, case.source_pool, case.sealer_pool
        )
        assert snapshot is None and previous["metadata_generation"] == first.snapshot["metadata_generation"]
