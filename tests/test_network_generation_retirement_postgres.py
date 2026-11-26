# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Retire only explicitly selected published generations and preserve already pinned reads."""

import json
from uuid import UUID, uuid4

import asyncpg
import pytest

from process.network_compact_retention import NetworkCompactRetentionError
from process.network_fhir_source_witness_contract import canonical
from process.network_fhir_typed_capture import COMMENT as TYPED_COMMENT
from process.network_fhir_typed_capture import TABLES as TYPED_TABLES
from process.network_generation_retirement import NetworkGenerationRetirementError, retire_network_generations
from process.network_serving_read import resolve_network_serving_manifest
from process.registry_source_recipe_store import canonical_registry_source_recipes, registry_source_recipes_sha256
from tests.test_network_compact_publication_postgres import (
    _candidate,
    _prepare,
    _publish,
    _reader,
    _scoped_rows,
    _seal,
    compact_publication_db,
)


async def _generation(fixture, expected_head, *, source=None, networks=(42, 88)):
    candidate = await _candidate(fixture, networks, expected_head, source_manifest=source)
    await _prepare(fixture, candidate)
    await _seal(fixture, candidate)
    published = await _publish(fixture, candidate)
    async with fixture.connection.transaction():
        return await resolve_network_serving_manifest(
            fixture.connection, generation_id=published["generation_id"], control_schema=fixture.control_schema
        )


async def _catalog(fixture, manifest, generation, *, published=True):
    schema = "fhir_catalog_" + uuid4().hex
    ledger = f'"{fixture.control_schema}".registry_fhir_catalog_snapshot'
    await fixture.connection.execute(f'CREATE SCHEMA "{schema}"')
    await fixture.connection.execute(f'CREATE TABLE "{schema}".registry_fhir_catalog_source(descriptor_json jsonb)')
    await fixture.connection.execute(
        f"""INSERT INTO {ledger}(metadata_generation,schema_name,custody_json,network_generation,manifest_sha256,
         metadata_approved_revision,membership_mapping_sha256,mapping_sha256,standing_grant_sha256,
         source_vector_json,projector_version,as_of,accounting_json,published_at)
         VALUES($1,$2,'{{}}',$3,$4,0,$4,$4,$4,'[]','fixture',current_date,'{{}}',CASE WHEN $5 THEN now() END)""",
        generation,
        schema,
        manifest.generation_id,
        manifest.manifest_sha256,
        published,
    )
    return schema


async def test_pinned_network_read_and_batch_catalog_retirement(compact_publication_db):
    fixture = compact_publication_db
    first = await _generation(fixture, 0)
    second = await _generation(fixture, first.generation_id, source=first)
    current = await _generation(fixture, second.generation_id, networks=(88, 42))
    retired_catalog = await _catalog(fixture, first, "a" * 64)
    current_catalog = await _catalog(fixture, current, "b" * 64)
    control = f'"{fixture.control_schema}"'

    async def retire(*generations, execute=False):
        return await retire_network_generations(
            fixture.connection,
            generation_ids=generations,
            expected_head=current.generation_id,
            control_schema=fixture.control_schema,
            execute=execute,
        )

    with pytest.raises(NetworkGenerationRetirementError, match="selection_invalid"):
        await retire(current.generation_id, execute=True)
    await fixture.connection.execute(f"UPDATE {control}.network_serving_control SET metadata_generation=$1", "a" * 64)
    with pytest.raises(NetworkGenerationRetirementError, match="active_catalog_changed"):
        await retire(first.generation_id, second.generation_id, execute=True)
    await fixture.connection.execute(f"UPDATE {control}.network_serving_control SET metadata_generation=$1", "b" * 64)
    with pytest.raises((NetworkCompactRetentionError, NetworkGenerationRetirementError), match="referenced"):
        await retire(first.generation_id, execute=True)

    batch = (first.generation_id, second.generation_id)
    report = await retire(*batch)
    assert report == {
        "state": "listed",
        "generation_ids": list(batch),
        "network_schemas": [first.schema_name, second.schema_name],
        "metadata_generations": ["a" * 64],
        "catalog_schemas": [retired_catalog],
    }
    await _assert_pinned_read(fixture, first, retire, batch)

    # A live database dependency discovered by PostgreSQL must roll the whole batch back.
    await fixture.connection.execute(
        f'CREATE VIEW {control}.dependent AS SELECT * FROM "{retired_catalog}".registry_fhir_catalog_source'
    )
    with pytest.raises(asyncpg.DependentObjectsStillExistError):
        await retire(*batch, execute=True)
    assert await retire(*batch) == report
    await fixture.connection.execute(f"DROP VIEW {control}.dependent")
    assert await retire(*reversed(batch), execute=True) == {**report, "state": "retired"}
    assert await fixture.connection.fetchval(
        f"SELECT array_agg(generation_id) FROM {control}.network_serving_manifest"
    ) == [current.generation_id]
    assert await fixture.connection.fetchval(
        f"SELECT array_agg(schema_name) FROM {control}.registry_fhir_catalog_snapshot"
    ) == [current_catalog]
    for schema in [first.schema_name, second.schema_name, retired_catalog]:
        assert await fixture.connection.fetchval("SELECT to_regnamespace($1) IS NULL", schema)
    async with _reader(fixture) as (session, reader):
        pin = await resolve_network_serving_manifest(reader, api_read=True, control_schema=fixture.control_schema)
        assert pin == current and await _scoped_rows(session, pin, (42,))


async def _assert_pinned_read(fixture, first, retire, batch):
    async with _reader(fixture) as (session, reader):
        pin = await resolve_network_serving_manifest(
            reader, generation_id=first.generation_id, api_read=True, control_schema=fixture.control_schema
        )
        # The reader has not yet touched any selected network data table.
        with pytest.raises(NetworkGenerationRetirementError, match="pin_or_writer_active"):
            await retire(*batch, execute=True)
        assert await _scoped_rows(session, pin, (42,))


async def test_unpublished_catalog_keeps_its_schema_and_guards_its_previous_catalog(compact_publication_db):
    fixture = compact_publication_db
    previous = await _generation(fixture, 0)
    current = await _generation(fixture, previous.generation_id, networks=(88, 42))
    published = await _catalog(fixture, previous, "a" * 64)
    draft = await _catalog(fixture, previous, "b" * 64, published=False)
    ledger = f'"{fixture.control_schema}".registry_fhir_catalog_snapshot'
    connection = fixture.connection
    selection_by_field = dict(
        generation_ids=[previous.generation_id],
        expected_head=current.generation_id,
        control_schema=fixture.control_schema,
    )
    await connection.execute(
        f"UPDATE {ledger} SET custody_json=jsonb_build_object('preparation',jsonb_build_object("
        "'previous_metadata_generation',$1::text)) WHERE schema_name=$2",
        "a" * 64,
        draft,
    )
    with pytest.raises(NetworkGenerationRetirementError, match="catalog_preparation_referenced"):
        await retire_network_generations(connection, **selection_by_field, execute=True)
    await connection.execute(f"UPDATE {ledger} SET custody_json='{{}}' WHERE schema_name=$1", draft)
    report = await retire_network_generations(connection, **selection_by_field)
    assert report["catalog_schemas"] == [published]
    assert await retire_network_generations(connection, **selection_by_field, execute=True) == {
        **report,
        "state": "retired",
    }
    generation = await connection.fetchval(
        f"SELECT network_generation FROM {ledger} WHERE published_at IS NULL AND schema_name=$1", draft
    )
    assert generation == previous.generation_id
    assert await connection.fetchval("SELECT to_regclass($1) IS NOT NULL", draft + ".registry_fhir_catalog_source")


async def test_active_build_request_and_retained_catalog_dependency(compact_publication_db, monkeypatch):
    fixture = compact_publication_db
    first = await _generation(fixture, 0)
    current = await _generation(fixture, first.generation_id, networks=(88, 42))
    catalog = await _catalog(fixture, current, "c" * 64)
    retirement_by_field = dict(
        generation_ids=[first.generation_id],
        expected_head=current.generation_id,
        control_schema=fixture.control_schema,
        execute=True,
    )
    connection, control = fixture.connection, f'"{fixture.control_schema}"'
    source_table = f'"{catalog}".registry_fhir_catalog_source'
    await connection.execute(
        f"INSERT INTO {source_table} VALUES(jsonb_build_object('retained_manifest',jsonb_build_object('generation_id',$1::bigint)))",
        first.generation_id,
    )
    with pytest.raises(NetworkGenerationRetirementError, match="catalog_source_referenced"):
        await retire_network_generations(connection, **retirement_by_field)
    await connection.execute(f"DELETE FROM {source_table}")
    request = uuid4()
    await connection.execute(
        f"""INSERT INTO {control}.registry_publication_request(request_id,actor_key,session_token_sha256,actor_json,
        command_json,idempotency_key,request_sha256) VALUES($1,$2,$2,'{{}}',jsonb_build_object('target_generation',$3::bigint),'fixture',$2)""",
        request,
        "f" * 64,
        first.generation_id,
    )
    with pytest.raises(NetworkGenerationRetirementError, match="request_referenced"):
        await retire_network_generations(connection, **retirement_by_field)
    await connection.execute(f"DELETE FROM {control}.registry_publication_request WHERE request_id=$1", request)
    async with _reader(fixture) as (_session, reader):
        assert await reader.fetchval(
            "SELECT pg_try_advisory_xact_lock(hashtextextended($1,0))",
            "registry-fhir-catalog:" + fixture.control_schema,
        )
        with pytest.raises(NetworkGenerationRetirementError, match="catalog_build_active"):
            await retire_network_generations(connection, **retirement_by_field)
    await _assert_concurrent_candidate(fixture, first, retirement_by_field, monkeypatch)


async def _assert_concurrent_candidate(fixture, first, retirement_by_field, monkeypatch):
    from process import network_generation_retirement

    references = network_generation_retirement._require_catalog_sources

    async def commit_candidate(*args):
        await references(*args)
        await _candidate(fixture, (42, 88), first.generation_id, source_manifest=first)

    monkeypatch.setattr(network_generation_retirement, "_require_catalog_sources", commit_candidate)
    with pytest.raises(NetworkGenerationRetirementError, match="controls_changed"):
        await retire_network_generations(fixture.connection, **retirement_by_field)
    assert await fixture.connection.fetchval("SELECT to_regnamespace($1) IS NOT NULL", first.schema_name)


async def _composed_candidate(fixture, networks, expected_head, address_source):
    import hashlib
    import json

    from process.network_membership_candidate_lifecycle import (
        admit_network_membership_batch,
        create_network_candidate,
        seal_network_candidate,
    )
    from process.network_membership_copy import MembershipCopyTarget
    from tests.test_network_compact_publication_postgres import _bind_offices

    candidate_id = uuid4()
    copy_target = MembershipCopyTarget(
        fixture.dataset_id,
        fixture.schema_id,
        fixture.producer_id,
        str(candidate_id),
        "network_candidate_" + candidate_id.hex,
    )
    locations = await fixture.connection.fetch(
        f'SELECT location_key,entity_type,entity_id,address_key FROM "{address_source.schema_name}".entity_address_unified '
        "ORDER BY location_key"
    )
    facts = [
        {
            "network_id": network_id,
            "provider_system": office["entity_type"],
            "provider_id": office["entity_id"],
            "location_id": str(office["address_key"]),
            "evidence_id": hashlib.sha256(("synthetic-office-" + str(index)).encode()).hexdigest(),
        }
        for index, (network_id, office) in enumerate(zip(networks, locations, strict=True))
    ]
    input_bytes = json.dumps(facts, separators=(",", ":")).encode()
    connection = fixture.execution_connection
    async with connection.transaction():
        await create_network_candidate(
            connection,
            copy_target,
            source_generations={"unified_address": address_source.generation_id},
            approved_custom_revision=0,
            expected_head=expected_head,
            expected_rows=len(facts),
            control_schema=fixture.control_schema,
        )
        await _bind_offices(connection, copy_target, locations)
        await admit_network_membership_batch(
            connection,
            copy_target,
            batch_id=uuid4(),
            input_bytes=input_bytes,
            expected_input_sha256=hashlib.sha256(input_bytes).hexdigest(),
            control_schema=fixture.control_schema,
        )
        await seal_network_candidate(connection, copy_target, control_schema=fixture.control_schema)
    return copy_target


async def _composed_generation(fixture, expected_head, *, networks=(42, 88)):
    from process.network_custom_address_source import prepare_custom_address_source
    from process.network_membership_pipeline import prepare_network_candidate

    composition_id = uuid4()
    async with fixture.connection.transaction(isolation="repeatable_read"):
        receipt = await prepare_custom_address_source(
            fixture.connection,
            fixture.source,
            composition_id=str(composition_id),
            approved_revision=0,
            owner_role=fixture.preparation_roles["namespace_owner_role"],
            runtime_roles=fixture.runtime_roles,
            control_schema=fixture.control_schema,
        )
    candidate = await _composed_candidate(fixture, networks, expected_head, receipt.address_source)
    await prepare_network_candidate(
        fixture.execution_connection,
        candidate,
        receipt.address_source,
        **fixture.writer_roles,
        control_schema=fixture.control_schema,
        compact=False,
    )
    await _seal(fixture, candidate)
    published = await _publish(fixture, candidate)
    async with fixture.connection.transaction():
        manifest = await resolve_network_serving_manifest(
            fixture.connection, generation_id=published["generation_id"], control_schema=fixture.control_schema
        )
    return manifest, receipt.address_source


async def _assert_composition_guards(fixture, first, composition_schema, retire):
    control = f'"{fixture.control_schema}"'
    saved_validation = await fixture.connection.fetchval(
        f"SELECT validation_json FROM {control}.network_membership_candidate WHERE candidate_id=$1",
        UUID(first.candidate_id),
    )
    await fixture.connection.execute(
        f"UPDATE {control}.network_membership_candidate "
        "SET validation_json=jsonb_set(validation_json,'{address_source,schema_name}','\"network_composition_bad\"') "
        "WHERE candidate_id=$1",
        UUID(first.candidate_id),
    )
    try:
        with pytest.raises(NetworkGenerationRetirementError, match="schema_invalid"):
            await retire(first.generation_id, execute=False)
    finally:
        await fixture.connection.execute(
            f"UPDATE {control}.network_membership_candidate SET validation_json=$2 WHERE candidate_id=$1",
            UUID(first.candidate_id),
            saved_validation,
        )
    dependent = await _candidate(fixture, (42, 88), first.generation_id)
    await fixture.connection.execute(
        f"UPDATE {control}.network_membership_candidate "
        "SET validation_json=jsonb_set(coalesce(validation_json,'{}'::jsonb),'{address_source}',"
        "jsonb_build_object('schema_name',$2::text,'table_name','entity_address_unified','generation_id',$3::text)) "
        "WHERE candidate_id=$1",
        UUID(dependent.candidate_id),
        composition_schema,
        first.source_generations["unified_address"],
    )
    try:
        with pytest.raises(NetworkCompactRetentionError, match="artifact_referenced"):
            await retire(first.generation_id, execute=False)
    finally:
        await fixture.connection.execute(
            f"UPDATE {control}.network_membership_candidate SET state='rejected' WHERE candidate_id=$1",
            UUID(dependent.candidate_id),
        )


async def test_retire_network_generations_drops_owned_composition_schema(compact_publication_db):
    fixture = compact_publication_db
    first, composition_source = await _composed_generation(fixture, 0)
    composition_schema = composition_source.schema_name
    current = await _generation(fixture, first.generation_id, networks=(88, 42))
    assert (
        await fixture.connection.fetchval(
            "SELECT count(*) FROM hp_snapshot_retention.preparation_relation WHERE preparation_id=$1 AND schema_name=$2",
            UUID(first.candidate_id),
            composition_schema,
        )
        == 2
    )

    async def retire(*generations, execute=False):
        return await retire_network_generations(
            fixture.connection,
            generation_ids=generations,
            expected_head=current.generation_id,
            control_schema=fixture.control_schema,
            execute=execute,
        )

    await _assert_composition_guards(fixture, first, composition_schema, retire)
    listed = await retire(first.generation_id)
    assert listed == {
        "state": "listed",
        "generation_ids": [first.generation_id],
        "network_schemas": [first.schema_name],
        "metadata_generations": [],
        "catalog_schemas": [],
        "composition_schemas": [composition_schema],
    }
    assert await retire(first.generation_id, execute=True) == {**listed, "state": "retired"}
    for schema in (first.schema_name, composition_schema):
        assert await fixture.connection.fetchval("SELECT to_regnamespace($1) IS NULL", schema)
    assert not await fixture.connection.fetchval(
        "SELECT EXISTS(SELECT FROM hp_snapshot_retention.preparation_relation WHERE preparation_id=$1)",
        UUID(first.candidate_id),
    )
    async with _reader(fixture) as (session, reader):
        pin = await resolve_network_serving_manifest(reader, api_read=True, control_schema=fixture.control_schema)
        assert pin == current and await _scoped_rows(session, pin, (42,))


async def test_independent_network_storage_metadata_includes_composition_tables(compact_publication_db):
    from process.network_compact_dependencies import independent_network_storage_metadata
    from process.network_compact_layout import INDEPENDENT_STORAGE, INDEPENDENT_STORAGE_KEY, CompactAddressRelation

    fixture = compact_publication_db
    first, composition_source = await _composed_generation(fixture, 0)
    candidate_by_field = {
        "schema_name": first.schema_name,
        "source_generations": {INDEPENDENT_STORAGE_KEY: INDEPENDENT_STORAGE},
    }
    base = CompactAddressRelation(composition_source.schema_name, composition_source.table_name)
    metadata = await independent_network_storage_metadata(
        fixture.connection, candidate_by_field, base, control_schema=fixture.control_schema
    )
    recorded_relations = {(entry["schema_name"], entry["table_name"]) for entry in metadata["storage_relations"]}
    assert {
        (composition_source.schema_name, "entity_address_unified"),
        (composition_source.schema_name, "provider_location_binding"),
        (first.schema_name, "network_membership"),
    } <= recorded_relations


def _typed_capture_doc(capture_num, catalog, graph_doc):
    """Build one v5 typed capture document for a synthetic fixture schema."""
    from process.network_fhir_source_witness_contract import digest
    from process.network_fhir_typed_capture import PREFIX, TypedGraphAnchor

    anchor = TypedGraphAnchor(str(UUID(int=capture_num)), "2026-10-01T00:00:00.000000+00:00")
    typed_schema = PREFIX + UUID(anchor.capture_id).hex
    tables_by_kind = {kind: {"schema": typed_schema, "name": table_name} for kind, table_name in TYPED_TABLES.items()}
    content_by_kind = {kind: {"rows": 1, "sha256": "b" * 64} for kind in TYPED_TABLES}
    capture_by_field = {
        "component": "registry_fhir_typed_capture",
        "descriptor_version": 5,
        "capture_id": anchor.capture_id,
        "observed_at": anchor.observed_at,
        "schema_name": typed_schema,
        "tables": tables_by_kind,
        "content": content_by_kind,
        "graph": graph_doc,
        "catalog_descriptor": catalog,
    }
    capture_by_field["capture_sha256"] = digest(capture_by_field)
    return anchor, typed_schema, capture_by_field


async def _create_typed_schema_fixture(connection, *, capture_num, dataset_id):
    """Create a synthetic typed capture schema and signed comment."""
    from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
    from process.network_fhir_typed_contract import graph_document
    from process.network_fhir_typed_recipe import PinnedTypedFHIRMembershipSource
    from process.registry_fhir_catalog_observation import FHIRCatalogObservationDigests
    from process.registry_fhir_catalog_observed_source import build_fhir_catalog_observation_descriptor
    from process.registry_source_recipe_store import RegistrySourceMembershipRecipe
    from tests.test_network_fhir_source_witness_contract import origin_fields
    from tests.test_registry_fhir_catalog_sources import descriptor as catalog_descriptor
    from tests.test_registry_fhir_catalog_sources import published_row, read_profile

    observed = FHIRCatalogObservationDigests()
    observed.add(
        {
            "resource_type": "PractitionerRole",
            "resource_id": "role-one",
            "payload_hash": "e" * 64,
            "payload_json": {"resourceType": "PractitionerRole", "id": "role-one"},
        }
    )
    families = observed.document()
    catalog = build_fhir_catalog_observation_descriptor(
        read_profile(),
        published_row(dataset_id=dataset_id, resource_count=1),
        catalog_descriptor()["tables"],
        ("https://directory.example.test/fhir/", ["alias-a", "alias-b"]),
        families,
    )
    counts = tuple(sorted((kind, family["rows"]) for kind, family in families.items()))
    origin = origin_fields() | {
        "schema_name": catalog["coordinates"]["dataset_schema"],
        "source_id": catalog["coordinates"]["source_id"],
        "dataset_id": dataset_id,
        "source_aliases": tuple(catalog["source_aliases"]),
        "source_uri": catalog["source_uri"],
        "header_relation": catalog["tables"]["header"],
        "resource_relation": catalog["tables"]["resource"],
        "semantic_projection_as_of": "2026-10-01",
        "resource_counts": counts,
        "resource_hashes": tuple(sorted((kind, family["sha256"]) for kind, family in families.items())),
        "descriptor_counts": counts,
    }
    graph_doc = graph_document(
        origin,
        "2026-10-01",
        {"source_rows": 1, "projected_rows": 1, "pair_rows": 1, "reference_rows": 1, "provider_only_roles": 0},
    )
    anchor, typed_schema, capture_doc = _typed_capture_doc(capture_num, catalog, graph_doc)
    await connection.execute(f'CREATE SCHEMA "{typed_schema}"')
    for table_name in TYPED_TABLES.values():
        await connection.execute(f'CREATE TABLE "{typed_schema}"."{table_name}"(id integer PRIMARY KEY)')
    comment_literal = (TYPED_COMMENT + canonical(capture_doc)).replace("'", "''")
    await connection.execute(f"COMMENT ON SCHEMA \"{typed_schema}\" IS '{comment_literal}'")
    pin = PinnedTypedFHIRMembershipSource(capture_doc, anchor, catalog)
    return RegistrySourceMembershipRecipe(pin, RegistryNetworkSourceCoordinates(**catalog["coordinates"]))


async def _pin_typed_recipes(connection, control, candidate_id, recipes):
    encoded = canonical_registry_source_recipes(recipes)
    await connection.execute(
        f"""UPDATE {control}.network_membership_candidate
        SET source_recipes_json=$2::jsonb,
            source_generations=coalesce(source_generations,'{{}}'::jsonb)
              || jsonb_build_object('registry_source_recipes',$3::text)
        WHERE candidate_id=$1""",
        UUID(candidate_id),
        encoded,
        registry_source_recipes_sha256(encoded),
    )


async def _assert_queued_request_defers_typed_schema(fixture, control, first, current, stale_recipe):
    """Verify an active publication request prevents dropping a referenced typed schema."""
    request_id = uuid4()
    await fixture.connection.execute(
        f"""INSERT INTO {control}.registry_publication_request(request_id,actor_key,session_token_sha256,actor_json,
        command_json,idempotency_key,request_sha256) VALUES($1,$2,$2,'{{}}',jsonb_build_object('capture_id',$3::text),'fixture',$2)""",
        request_id,
        "c" * 64,
        stale_recipe.source_pin.anchor.capture_id,
    )
    blocked_report = await retire_network_generations(
        fixture.connection,
        generation_ids=[first.generation_id],
        expected_head=current.generation_id,
        control_schema=fixture.control_schema,
    )
    assert "typed_schemas" not in blocked_report
    await fixture.connection.execute(
        f"DELETE FROM {control}.registry_publication_request WHERE request_id=$1", request_id
    )


async def test_retire_network_generations_manages_typed_capture_schemas(compact_publication_db):
    """Retire unreferenced typed schemas while keeping live, reusable, and queued ones."""
    fixture = compact_publication_db
    shared_recipe = await _create_typed_schema_fixture(fixture.connection, capture_num=101, dataset_id="ds-shared")
    stale_recipe = await _create_typed_schema_fixture(fixture.connection, capture_num=102, dataset_id="ds-stale")
    reusable_recipe = await _create_typed_schema_fixture(fixture.connection, capture_num=103, dataset_id="ds-reuse")
    first = await _generation(fixture, 0)
    current = await _generation(fixture, first.generation_id, networks=(88, 42))
    retired_catalog = await _catalog(fixture, first, "a" * 64)
    current_catalog = await _catalog(fixture, current, "b" * 64)
    control = f'"{fixture.control_schema}"'
    await _pin_typed_recipes(
        fixture.connection, control, first.candidate_id, (shared_recipe, stale_recipe, reusable_recipe)
    )
    await _pin_typed_recipes(fixture.connection, control, current.candidate_id, (shared_recipe,))
    await fixture.connection.execute(
        f'INSERT INTO "{current_catalog}".registry_fhir_catalog_source VALUES($1::jsonb)',
        json.dumps(reusable_recipe.source_pin.catalog_descriptor),
    )
    await _assert_queued_request_defers_typed_schema(fixture, control, first, current, stale_recipe)
    stale_schema = stale_recipe.source_pin.capture["schema_name"]
    listed = await retire_network_generations(
        fixture.connection,
        generation_ids=[first.generation_id],
        expected_head=current.generation_id,
        control_schema=fixture.control_schema,
    )
    assert listed == {
        "state": "listed",
        "generation_ids": [first.generation_id],
        "network_schemas": [first.schema_name],
        "metadata_generations": ["a" * 64],
        "catalog_schemas": [retired_catalog],
        "typed_schemas": [stale_schema],
    }
    retired = await retire_network_generations(
        fixture.connection,
        generation_ids=[first.generation_id],
        expected_head=current.generation_id,
        control_schema=fixture.control_schema,
        execute=True,
    )
    assert retired == {**listed, "state": "retired"}
    assert await fixture.connection.fetchval("SELECT to_regnamespace($1) IS NULL", stale_schema)
    for kept_recipe in (shared_recipe, reusable_recipe):
        assert await fixture.connection.fetchval(
            "SELECT to_regnamespace($1) IS NOT NULL", kept_recipe.source_pin.capture["schema_name"]
        )


async def _create_bootstrap_schema_fixture(connection, schema_name):
    await connection.execute(f'CREATE SCHEMA "{schema_name}"')
    await connection.execute(f'CREATE TABLE "{schema_name}".entity_address_unified(id integer PRIMARY KEY)')
    await connection.execute(f'CREATE TABLE "{schema_name}".npi(npi bigint PRIMARY KEY)')


async def test_retire_network_generations_drops_unreferenced_bootstrap_schemas(compact_publication_db):
    fixture = compact_publication_db
    bootstrap_retired = "network_bootstrap_" + uuid4().hex
    bootstrap_shared = "network_bootstrap_" + uuid4().hex
    await _create_bootstrap_schema_fixture(fixture.connection, bootstrap_retired)
    await _create_bootstrap_schema_fixture(fixture.connection, bootstrap_shared)

    first = await _generation(fixture, 0, networks=(42, 88))
    second = await _generation(fixture, first.generation_id, networks=(42, 88))
    third = await _generation(fixture, second.generation_id, networks=(88, 42))
    fourth = await _generation(fixture, third.generation_id, networks=(42, 88))
    control = f'"{fixture.control_schema}"'
    for candidate_id, bootstrap_schema in (
        (first.candidate_id, bootstrap_retired),
        (second.candidate_id, bootstrap_shared),
        (third.candidate_id, bootstrap_shared),
    ):
        await fixture.connection.execute(
            f"""UPDATE {control}.network_membership_candidate
            SET validation_json=jsonb_set(coalesce(validation_json,'{{}}'::jsonb),'{{address_source}}',
              jsonb_build_object('schema_name',$2::text,'table_name','entity_address_unified','generation_id',$3::text)),
                source_generations=coalesce(source_generations,'{{}}'::jsonb)||jsonb_build_object('unified_address',$3::text)
            WHERE candidate_id=$1""",
            UUID(candidate_id),
            bootstrap_schema,
            fixture.source.generation_id,
        )

    listed_partial = await retire_network_generations(
        fixture.connection,
        generation_ids=[first.generation_id, second.generation_id],
        expected_head=fourth.generation_id,
        control_schema=fixture.control_schema,
    )
    assert listed_partial["bootstrap_schemas"] == [bootstrap_retired]
    await retire_network_generations(
        fixture.connection,
        generation_ids=[first.generation_id, second.generation_id],
        expected_head=fourth.generation_id,
        control_schema=fixture.control_schema,
        execute=True,
    )
    assert await fixture.connection.fetchval("SELECT to_regnamespace($1) IS NULL", bootstrap_retired)
    assert await fixture.connection.fetchval("SELECT to_regnamespace($1) IS NOT NULL", bootstrap_shared)

    retired_final = await retire_network_generations(
        fixture.connection,
        generation_ids=[third.generation_id],
        expected_head=fourth.generation_id,
        control_schema=fixture.control_schema,
        execute=True,
    )
    assert retired_final["bootstrap_schemas"] == [bootstrap_shared]
    assert await fixture.connection.fetchval("SELECT to_regnamespace($1) IS NULL", bootstrap_shared)
