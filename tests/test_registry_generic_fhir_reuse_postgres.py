# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Generic captures retain complete facts through independent seal and replay."""

import json
from contextlib import AsyncExitStack, aclosing, asynccontextmanager
from dataclasses import asdict
from types import SimpleNamespace
from uuid import UUID, uuid4

import asyncpg
import pytest

from db.models import EntityAddressUnified
from process.network_address_projection import PinnedAddressSource
from process.network_candidate_custody import NetworkCandidateCustodyError, seal_network_candidate_custody
from process.network_fhir_retained_capture_contract import GENERIC_UNVERIFIED_NETWORK
from process.network_membership_pipeline import prepare_and_publish_network_candidate
from process.network_serving_read import resolve_network_serving_manifest
from process.network_source_binding_store import apply_network_source_binding_batch
from process.registry_candidate_composition import (
    RegistryCompositionAddressSources,
    compose_registry_membership_candidate,
)
from process.registry_initial_source_composition import verify_initial_source_office_receipt
from process.registry_source_contribution_store import verify_registry_source_contributions
from process.registry_source_inventory_contract import source_inventory_root
from process.registry_source_inventory_reference import encode_source_inventory_reference, verify_source_recipe_metadata
from process.registry_source_inventory_store import source_inventory_entry
from process.registry_source_inventory_summaries import (
    contribution_summary_v3,
    contribution_summary_v4,
    office_summary_v4,
    office_summary_v5,
)
from process.registry_source_recipe_composition import (
    RegistrySourceCompositionError,
    SourceRecipeReadContext,
    stage_registry_source_recipes,
)
from tests import generic_fhir_capture_postgres_support as support
from tests.registry_candidate_custody_postgres_support import install_candidate_custody
from tests.test_network_custom_address_source_postgres import (
    bootstrap_db as bootstrap_db,
)
from tests.test_network_custom_address_source_postgres import (
    custom_db as custom_db,
)
from tests.test_network_serving_schema_postgres import serving_schema as serving_schema
from tests.test_registry_approval_store_postgres import _actor, _approve, _command

pytestmark = pytest.mark.asyncio


@pytest.fixture
async def generic_capture_db(request, monkeypatch):
    """Create every variation before publication hashes and real Reader capture."""
    original = support._contents

    def contents(source_id):
        rows = original(source_id)
        role = next(payload for kind, _, payload in rows if kind == "PractitionerRole")
        organization = next(payload for kind, _, payload in rows if kind == "Organization")
        rows.append(("Organization", "network-unapproved", {**organization, "resource_id": "network-unapproved"}))
        fields_by_role = {
            "role-permanent": dict(
                location_refs=["Location/office-one", "Location/office-two"],
                network_refs=["Organization/missing-a", "Organization/missing-b"],
            ),
            "role-mixed": dict(network_refs=["Organization/network-one", "Organization/missing-a"]),
            "role-provider-only": dict(location_refs=[]),
            "role-witnessed-omission": dict(network_refs=["Organization/network-unapproved"]),
        }
        for identity, fields in fields_by_role.items():
            rows.append(
                ("PractitionerRole", identity, {**role, "resource_id": identity, "insurance_plan_refs": [], **fields})
            )
        return rows

    monkeypatch.setattr(support, "_contents", contents)
    async with aclosing(support.legacy_capture_db.__wrapped__(request)) as fixture:
        yield await anext(fixture)


async def _fact_stage(connection, schema, recipe, approved):
    async with connection.transaction(isolation="repeatable_read"):
        stage = await stage_registry_source_recipes(
            connection,
            (recipe,),
            approved,
            request_id=uuid4(),
            control_schema=schema,
        )
        try:
            rows = await connection.fetch(
                f'SELECT fact_json FROM pg_temp."{stage.contribution_table_name}" ORDER BY ordinal'
            )
            return stage, [json.loads(row["fact_json"]) for row in rows]
        finally:
            await connection.execute(f'DROP TABLE pg_temp."{stage.table_name}"')
            await connection.execute(f'DROP TABLE pg_temp."{stage.contribution_table_name}"')


@pytest.mark.parametrize("generic_capture_db", ["legacy", "modern"], indirect=True)
async def test_complete_generic_facts_deduplicate_permanent_omissions(generic_capture_db, serving_schema):
    source = generic_capture_db
    capture = await support._capture(source)
    recipe, approved, _ = await support._approved_capture_recipe(source, serving_schema, capture)
    connection, schema, _ = serving_schema
    stage, facts = await _fact_stage(connection, schema, recipe, approved)
    assert (stage.membership_rows, stage.omitted_rows, stage.provider_only_rows) == (2, 3, 1)
    assert (stage.raw_source_rows, stage.accepted_raw_source_rows, stage.omitted_raw_source_rows) == (5, 3, 2)
    assert len(facts) == 6
    sentinels = [fact for fact in facts if fact["omission_reason"] == GENERIC_UNVERIFIED_NETWORK]
    assert {fact["resource_id"] for fact in sentinels} == {"role-permanent", "role-mixed"}
    assert len(sentinels) == 2
    edge_fields = {"source_key", "source_scope_json", "provider_system", "provider_id", "location_id", "evidence_basis"}
    assert all(all(fact[field] is None for field in edge_fields) for fact in sentinels)
    ordinary_facts = [fact for fact in facts if fact["resource_id"] == "role-witnessed-omission"]
    assert len(ordinary_facts) == 1 and ordinary_facts[0]["omission_reason"] is None
    assert ordinary_facts[0]["source_key"] == "network-unapproved"


@pytest.mark.parametrize("generic_capture_db", ["legacy", "modern"], indirect=True)
async def test_approved_missing_witness_refuses_complete_generic_stage(generic_capture_db, serving_schema):
    source = generic_capture_db
    capture = await support._capture(source)
    recipe, approved, _ = await support._approved_capture_recipe(
        source,
        serving_schema,
        capture,
        organization="missing-a",
    )
    connection, schema, _ = serving_schema
    async with connection.transaction(isolation="repeatable_read"):
        with pytest.raises(RegistrySourceCompositionError, match="unresolved"):
            await stage_registry_source_recipes(
                connection,
                (recipe,),
                approved,
                request_id=uuid4(),
                control_schema=schema,
            )
        assert not await connection.fetchval(
            "SELECT EXISTS(SELECT FROM pg_class WHERE relnamespace=pg_my_temp_schema() "
            "AND (relname LIKE 'registry_recipe_%' OR relname LIKE 'registry_source_page_%'))"
        )
        assert await connection.fetchval(f'SELECT generation_id FROM "{schema}".network_serving_control') is None


async def _native_generic_offices(fixture, source_fixture, serving, runtime_roles, stack):
    """Use the existing native EUA generation fixture and real address artifact capture."""
    from process import network_address_artifact as artifact
    from tests.registry_compact_source_office_postgres_support import _cleanup_native, _native_storage

    addresses = await support._retained_addresses(source_fixture, serving)
    schema = "generic_office_address_" + uuid4().hex
    roles_by_kind = {kind: "generic_office_" + kind + "_" + uuid4().hex for kind in ("owner", "publisher")}
    created_namespaces, created_roles = [False], []
    stack.push_async_callback(_cleanup_native, fixture, schema, roles_by_kind, created_namespaces, created_roles)

    async def offices(connection, target_schema):
        columns = ",".join('"' + name + '"' for name in EntityAddressUnified.__table__.columns.keys())
        await connection.execute(f'DELETE FROM "{target_schema}".entity_address_unified')
        await connection.execute(
            f'INSERT INTO "{target_schema}".entity_address_unified({columns}) '
            f'SELECT {columns} FROM "{addresses.schema_name}".entity_address_unified'
        )
        await connection.execute(f'UPDATE "{target_schema}".entity_address_unified SET lat=41,long=-87')

    await _native_storage(fixture, schema, roles_by_kind, created_namespaces, created_roles, offices)
    password = uuid4().hex
    await fixture.connection.execute(f"CREATE ROLE \"{roles_by_kind['publisher']}\" LOGIN PASSWORD '{password}'")
    created_roles.append(roles_by_kind["publisher"])
    await fixture.connection.execute(f'GRANT "{roles_by_kind["owner"]}" TO "{roles_by_kind["publisher"]}"')
    for role in runtime_roles:
        await fixture.connection.execute(f'GRANT USAGE ON SCHEMA "{schema}" TO "{role}"')
        await fixture.connection.execute(f'GRANT SELECT ON ALL TABLES IN SCHEMA "{schema}" TO "{role}"')
    publisher = await asyncpg.connect(
        fixture.engine.url.set(
            drivername="postgresql",
            username=roles_by_kind["publisher"],
            password=password,
        ).render_as_string(hide_password=False)
    )
    stack.push_async_callback(publisher.close)
    async with publisher.transaction(isolation="repeatable_read", readonly=True):
        authority = await artifact.generation.read_entity_address_result_generation_authority(
            artifact._CatalogSession(publisher),
            schema_name=schema,
        )
        address_source = PinnedAddressSource(
            schema,
            "entity_address_unified",
            artifact._digest(artifact._authority_document(authority)),
        )
        await artifact.capture_network_address_artifact(publisher, address_source, runtime_roles=runtime_roles)
    return address_source


async def _assert_original_prehead_anchor(context, revision):
    from sqlalchemy import text
    from sqlalchemy.ext.asyncio import async_sessionmaker

    from process.registry_generic_refresh_anchors import resolve_generic_refresh_anchors
    from process.registry_ptg_graph_reader import RegistryPTGGraphReadBudget
    from process.registry_refresh_anchor_contract import ApprovedRefreshBinding

    fixture = context.fixture
    capture = context.recipe.source_pin.retained_capture
    binding_records = await fixture.connection.fetch(
        f"""SELECT record_kind,record_key,record_revision,custom_revision,record_json::text,
        encode(sha256(convert_to(jsonb_build_array(record_kind,record_key,record_revision,custom_revision,
          record_json)::text,'UTF8')),'hex') AS approved_binding_sha256
        FROM "{fixture.control_schema}".registry_approved_record
        WHERE approved_revision=$1 AND record_kind='network_binding' ORDER BY record_key""",
        revision,
    )
    bindings = tuple(
        ApprovedRefreshBinding(**{**dict(binding_record), "record_json": json.loads(binding_record["record_json"])})
        for binding_record in binding_records
    )
    assert len(bindings) == 1
    sessions = async_sessionmaker(fixture.engine, expire_on_commit=False)
    async with sessions() as session, session.begin():
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"))
        assert (
            await session.scalar(text(f'SELECT generation_id FROM "{fixture.control_schema}".network_serving_control'))
            is None
        )
        anchors = await resolve_generic_refresh_anchors(
            session, bindings, read_budget=RegistryPTGGraphReadBudget(16 * 1024 * 1024)
        )
    binding = bindings[0]
    assert anchors == {
        binding.binding_id: dict(
            source_system="fhir",
            source_id=capture.origin.source_id,
            dataset_schema=capture.origin.schema_name,
            producer_id=context.recipe.binding_coordinates.producer_id,
            source_key="network-one",
            approved_binding_sha256=binding.approved_binding_sha256,
            capture_generation=capture.capture_sha256,
        )
    }


async def _compose(context, revision, previous=None):
    fixture = context.fixture
    request = uuid4()
    context.requests.append(request)
    target, address = await compose_registry_membership_candidate(
        fixture.execution_connection,
        request_id=request,
        approved_revision=revision,
        expected_head=0 if previous is None else previous.generation_id,
        source_manifest=previous,
        address_sources=RegistryCompositionAddressSources(
            context.address,
            compact=True,
            initial_source_recipes=(context.recipe,) if previous is None else (),
        ),
        writer_roles=fixture.writer_roles,
        control_schema=fixture.control_schema,
    )
    context.targets.append(target)
    assert address == context.address
    return target


async def _publish(context, copy_target, *, first=False):
    fixture = context.fixture
    arguments_by_name = dict(**fixture.writer_roles, control_schema=fixture.control_schema)
    if first:
        with pytest.raises(NetworkCandidateCustodyError, match="network_candidate_custody_unavailable"):
            await prepare_and_publish_network_candidate(
                fixture.execution_connection,
                copy_target,
                context.address,
                **arguments_by_name,
            )
        assert (
            await fixture.connection.fetchval(
                f'SELECT generation_id FROM "{fixture.control_schema}".network_serving_control'
            )
            is None
        )
        await context.seal(copy_target)
    operation_result = await prepare_and_publish_network_candidate(
        fixture.execution_connection,
        copy_target,
        context.address,
        seal_candidate=context.seal,
        **arguments_by_name,
    )
    async with fixture.execution_connection.transaction(isolation="repeatable_read", readonly=True):
        manifest = await resolve_network_serving_manifest(
            fixture.execution_connection,
            control_schema=fixture.control_schema,
        )
    assert operation_result["generation_id"] == manifest.generation_id
    return manifest


async def _read(context, manifest):
    from api.network_address_scope import network_address_read_scope
    from api.network_compact_address import compact_address_native_sql
    from api.network_provider_only_read import read_network_provider_only_page

    fixture = context.fixture
    async with fixture.execution_connection.transaction(isolation="repeatable_read", readonly=True):
        pin = await resolve_network_serving_manifest(
            fixture.execution_connection,
            generation_id=manifest.generation_id,
            control_schema=fixture.control_schema,
        )
        assert pin == manifest
        relation, arguments = compact_address_native_sql(
            manifest.compact_layout,
            first_parameter=1,
        )
        offices = await fixture.execution_connection.fetch(
            f"SELECT second_line,canonical_network_ids FROM {relation} office ORDER BY second_line",
            *arguments,
        )
        with network_address_read_scope(manifest, (context.network_id,)) as scope:
            providers = await read_network_provider_only_page(
                fixture.execution_connection,
                scope,
                control_schema=fixture.control_schema,
            )
        assert [(office["second_line"], office["canonical_network_ids"]) for office in offices] == [
            ("Suite 1", [context.network_id]),
            ("Suite 2", []),
        ]
        assert providers["total_count"] == 1
        assert providers["providers"][0]["provider_id"] == "1234567893"
        return [dict(office) for office in offices], providers


async def _add_mapping(context, organization):
    fixture = context.fixture
    connection, schema = fixture.connection, fixture.control_schema
    actor = _actor()
    command = support._binding_command(
        context.recipe.binding_coordinates,
        context.network_id,
        organization,
        context.recipe.source_pin.retained_capture.capture_sha256,
    )
    async with connection.transaction():
        binding = (await apply_network_source_binding_batch(connection, command, actor, control_schema=schema))[
            "records"
        ][0]
    approved = await _approve(connection, schema, await _command(connection, schema, binding), actor)
    return approved["approved_revision"]


async def _close_mappings(context, source_keys):
    fixture = context.fixture
    connection, schema = fixture.connection, fixture.control_schema
    actor = _actor()
    binding_records = await connection.fetch(
        f'SELECT * FROM "{schema}".registry_network_binding WHERE NOT archived AND source_key=ANY($1::text[])',
        list(source_keys),
    )
    assert len(binding_records) == len(source_keys)
    documents = []
    for binding_record in binding_records:
        documents.append(
            dict(
                binding_id=str(binding_record["binding_id"]),
                **{
                    key: binding_record[key]
                    for key in (
                        "source_system",
                        "source_id",
                        "dataset_schema",
                        "dataset_id",
                        "producer_id",
                        "edition_id",
                        "source_key",
                        "network_id",
                        "evidence_id",
                        "evidence_sha256",
                    )
                },
                source_scope_json=json.loads(binding_record["source_scope_json"]),
                operation="close",
                expected_revision=binding_record["revision"],
                expected_network_id=binding_record["network_id"],
            )
        )
    command = support.NetworkSourceBindingBatchCommand(
        json.dumps(documents).encode(), "Close reviewed mappings", uuid4().hex
    )
    async with connection.transaction():
        bindings = (await apply_network_source_binding_batch(connection, command, actor, control_schema=schema))[
            "records"
        ]
    approved = await _approve(connection, schema, await _command(connection, schema, *bindings), actor)
    return approved["approved_revision"]


async def _assert_missing_approval_boundary(context, original, monkeypatch):
    """Permanent omissions remain absent while valid fact offices keep serving."""
    from process import registry_candidate_composition as composition

    fixture = context.fixture
    revision = await _add_mapping(context, "missing-a")
    prepare = composition.stage_registry_source_recipes
    stage_counts = []

    async def observe_stage(*arguments, **options):
        stage = await prepare(*arguments, **options)
        stage_counts.append(
            (
                stage.membership_rows,
                stage.omitted_rows,
                stage.provider_only_rows,
                stage.reused_fact_rows,
                stage.own_fact_rows,
                stage.copied_reused_fact_rows,
            )
        )
        return stage

    with monkeypatch.context() as settings:
        settings.setattr(composition, "stage_registry_source_recipes", observe_stage)
        pending = await _compose(context, revision, original)
    assert stage_counts == [(2, 6, 1, 14, 0, 0)]
    _contribution, summary, _descriptor, derivation = await _inventory_metadata(
        fixture, pending, witness=True, address_source=context.address
    )
    assert (summary["revision"], summary["office_rows"], summary["page_count"]) == (5, 1, 1)
    assert summary["origin_manifest_sha256"] is None
    assert (
        await fixture.connection.fetchval(
            f"SELECT source_generations->>'retained_network_source' FROM \"{fixture.control_schema}\"."
            "network_membership_candidate WHERE candidate_id=$1",
            UUID(pending.candidate_id),
        )
        == original.manifest_sha256
    )
    assert derivation["fact_artifact"]["fact_candidate_id"] == original.candidate_id
    assert (
        await fixture.connection.fetchval(
            f'SELECT generation_id FROM "{fixture.control_schema}".network_serving_control'
        )
        == original.generation_id
    )
    return revision


@pytest.fixture
async def generic_composition_db(generic_capture_db, serving_schema, custom_db):
    """Keep source, address, loading and custody roles independent with exact cleanup."""
    from tests.test_registry_candidate_composition_postgres import _remove_candidates

    source_fixture, fixture = generic_capture_db, custom_db
    context = SimpleNamespace(fixture=fixture, requests=[], targets=[])
    async with AsyncExitStack() as stack:
        for extension in ("intarray", "btree_gin", "postgis"):
            await fixture.connection.execute(f'CREATE EXTENSION IF NOT EXISTS "{extension}"')
        await install_candidate_custody(fixture, stack)
        runtime_roles = tuple(
            sorted((fixture.executor_role, fixture.roles["reader"], fixture.custody_roles["custodian"]))
        )
        capture = await support._capture(
            source_fixture,
            runtime_roles=tuple(sorted((*runtime_roles, source_fixture.role_by_kind["reader"]))),
        )
        context.recipe, approved, context.network_id = await support._approved_capture_recipe(
            source_fixture, serving_schema, capture
        )
        context.revision = approved.approved_revision
        context.address = await _native_generic_offices(fixture, source_fixture, serving_schema, runtime_roles, stack)
        custodian = await asyncpg.connect(
            fixture.custody_url.set(drivername="postgresql").render_as_string(hide_password=False)
        )
        stack.push_async_callback(custodian.close)

        async def seal(target):
            async def authorize(connection, supplied):
                assert connection is custodian and supplied == target
                return await connection.fetchval("SELECT current_user=session_user")

            return await seal_network_candidate_custody(
                custodian,
                target,
                control_schema=fixture.control_schema,
                runtime_roles=(fixture.roles["reader"],),
                authorize=authorize,
            )

        context.seal = seal
        try:
            yield context
        finally:
            for request in context.requests:
                await _remove_candidates(fixture, context.targets, request)


async def _inventory_entry(fixture, manifest):
    """Read and fold the complete native inventory, rather than trusting bounded metadata alone."""
    candidate_by_field = dict(
        await fixture.connection.fetchrow(
            f'SELECT * FROM "{fixture.control_schema}".network_membership_candidate WHERE candidate_id=$1',
            UUID(manifest.candidate_id),
        )
    )
    metadata = verify_source_recipe_metadata(candidate_by_field)
    assert metadata.inventory is not None
    root = metadata.inventory.root
    assert (root.entry_count, root.contribution_count, root.derivation_count) == (1, 1, 1)
    assert len(encode_source_inventory_reference(metadata.inventory).encode()) <= 4096
    generations = candidate_by_field["source_generations"]
    generations = json.loads(generations) if type(generations) is str else generations
    assert generations["registry_source_inventory"] == root.entries_sha256
    assert not any(key.startswith("initial_source_recipe_") for key in generations)
    records = await fixture.connection.fetch(
        f'SELECT * FROM "{manifest.schema_name}".network_source_inventory ORDER BY ordinal'
    )
    entries = tuple(source_inventory_entry(record) for record in records)
    assert source_inventory_root(entries) == root and len(entries) == 1
    return candidate_by_field, root, entries[0]


def _fold_office_metadata(candidate, root, office, derivation, *, witness, address_source=None):
    """Recompute the exact office commitment from its complete typed inventory entry."""
    generations = candidate["source_generations"]
    generations = json.loads(generations) if type(generations) is str else generations
    report = candidate["validation_json"]
    report = json.loads(report) if type(report) is str else report
    address = PinnedAddressSource(**report["address_source"]) if address_source is None else address_source
    assert type(address) is PinnedAddressSource and address.generation_id == generations["unified_address"]
    if "address_source" in report:
        assert asdict(address) == report["address_source"]
    approved = SimpleNamespace(
        approved_revision=candidate["approved_custom_revision"], generation_id=generations["custom_membership"]
    )
    if not witness:
        assert "derivation_version" not in derivation
        return office_summary_v4(root, approved, address, (derivation,), office["origin_manifest_sha256"])
    assert derivation["derivation_version"] == 2
    policy_by_field = {
        name: derivation[name]
        for name in (
            "policy_version",
            "approved_revision",
            "approved_generation_sha256",
            "address_source",
            "address_identity",
            "origin_manifest_sha256",
            "canonicalizer_version",
        )
    }
    assert policy_by_field["approved_revision"] == approved.approved_revision
    assert policy_by_field["approved_generation_sha256"] == approved.generation_id
    assert policy_by_field["address_source"] == asdict(address)
    assert policy_by_field["origin_manifest_sha256"] == office["origin_manifest_sha256"]
    return office_summary_v5(root, policy_by_field, (derivation,))


async def _inventory_metadata(fixture, manifest, *, witness, address_source=None):
    candidate, root, entry = await _inventory_entry(fixture, manifest)
    contribution = verify_registry_source_contributions(candidate)
    office = verify_initial_source_office_receipt(candidate)
    descriptor, derivation = json.loads(entry.contribution_json), json.loads(entry.office_derivation_json)
    assert contribution["revision"] == (4 if witness else 3) and "sources" not in contribution
    assert office["revision"] == (5 if witness else 4) and "derivations" not in office
    if witness:
        assert descriptor["descriptor_version"] == 2
        assert contribution_summary_v4(root, (descriptor,)) == contribution
        assert office["origin_manifest_sha256"] is None and derivation["origin_manifest_sha256"] is None
        identity = derivation["address_identity"]
        source = derivation["address_source"]
        native_identity = await fixture.connection.fetchrow(
            "SELECT n.oid AS schema_oid,c.oid AS table_oid,c.relowner AS owner_oid "
            "FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=$1 AND c.relname=$2",
            source["schema_name"],
            source["table_name"],
        )
        assert dict(native_identity) == {key: identity[key] for key in ("schema_oid", "table_oid", "owner_oid")}
    else:
        assert "descriptor_version" not in descriptor
        assert contribution_summary_v3(root, (descriptor,)) == contribution
    assert (
        _fold_office_metadata(candidate, root, office, derivation, witness=witness, address_source=address_source)
        == office
    )
    return contribution, office, descriptor, derivation


async def _descriptor(context, manifest):
    _contribution, _office, descriptor, _derivation = await _inventory_metadata(context.fixture, manifest, witness=True)
    owner = descriptor["fact_candidate_id"] or manifest.candidate_id
    schema = "network_candidate_" + UUID(owner).hex
    fact_kinds = await context.fixture.connection.fetch(
        "SELECT fact_json->>'kind' AS kind,count(*) AS fact_rows,"
        "bool_and(fact_json->'fact_version'='2'::jsonb) AS witness_v2 "
        f'FROM "{schema}".network_source_contribution GROUP BY 1'
    )
    assert {fact_kind["kind"]: fact_kind["fact_rows"] for fact_kind in fact_kinds} == {"resource": 5, "edge": 9}
    assert all(fact_kind["witness_v2"] for fact_kind in fact_kinds)
    assert descriptor["fact_rows"] == 14
    return descriptor


async def _assert_independent_owners(context):
    fixture = context.fixture
    source_owner = await fixture.connection.fetchval(
        "SELECT nspowner FROM pg_namespace WHERE nspname=$1",
        context.recipe.source_pin.retained_capture.schema_name,
    )
    office_owner = await fixture.connection.fetchval(
        "SELECT nspowner FROM pg_namespace WHERE nspname=$1",
        context.address.schema_name,
    )
    loading_owner = await fixture.connection.fetchval(
        "SELECT oid FROM pg_roles WHERE rolname=$1",
        fixture.writer_roles["owner_role"],
    )
    assert len({source_owner, office_owner, loading_owner}) == 3


@asynccontextmanager
async def _pinned_reader(context, original):
    fixture = context.fixture
    connection = await asyncpg.connect(
        fixture.execution_url.set(drivername="postgresql").render_as_string(hide_password=False)
    )
    try:
        async with connection.transaction(isolation="repeatable_read", readonly=True):
            assert await resolve_network_serving_manifest(connection, control_schema=fixture.control_schema) == original
            yield connection
    finally:
        await connection.close()


async def _assert_reused_facts(context, original, rebound):
    descriptor = await _descriptor(context, rebound)
    original_descriptor = await _descriptor(context, original)
    assert descriptor["fact_candidate_id"] == original.candidate_id
    assert descriptor["fact_sha256"] == original_descriptor["fact_sha256"]
    assert tuple(
        descriptor[field]
        for field in ("fact_rows", "membership_rows", "omitted_rows", "unresolved_rows", "provider_only_rows")
    ) == (14, 3, 2, 3, 1)
    assert (
        await context.fixture.connection.fetchval(
            f'SELECT count(*) FROM "{rebound.schema_name}".network_source_contribution'
        )
        == 0
    )


@pytest.mark.parametrize("generic_capture_db", ["legacy", "modern"], indirect=True)
async def test_genuine_generic_capture_composes_seals_reuses_and_keeps_old_readers(generic_composition_db, monkeypatch):
    """Use actual capture, separate seal, native facts and pinned readers across head changes."""
    from process import registry_source_recipe_composition as composition

    context, fixture = generic_composition_db, generic_composition_db.fixture
    await _assert_original_prehead_anchor(context, context.revision)
    original_target = await _compose(context, context.revision)
    original = await _publish(context, original_target, first=True)
    before = await _read(context, original)
    descriptor = await _descriptor(context, original)
    assert (descriptor["fact_rows"], descriptor["omitted_rows"], descriptor["provider_only_rows"]) == (14, 6, 1)
    await _assert_independent_owners(context)

    async def forbidden(*arguments, **options):
        pytest.fail("unchanged retained source facts must not be re-extracted or copied")

    monkeypatch.setattr(composition, "_read_recipe_page", forbidden)
    monkeypatch.setattr(composition, "reuse_fhir_facts", forbidden)
    await _assert_missing_approval_boundary(context, original, monkeypatch)
    revision = await _add_mapping(context, "network-unapproved")
    async with _pinned_reader(context, original) as old_reader:
        rebound_target = await _compose(context, revision, original)
        rebound = await _publish(context, rebound_target)
        await _assert_reused_facts(context, original, rebound)
        assert await _read(context, original) == before
        assert await resolve_network_serving_manifest(old_reader, control_schema=fixture.control_schema) == original
    replay = await prepare_and_publish_network_candidate(
        fixture.execution_connection,
        rebound_target,
        context.address,
        **fixture.writer_roles,
        control_schema=fixture.control_schema,
        seal_candidate=context.seal,
    )
    assert replay["generation_id"] == rebound.generation_id
    revision = await _close_mappings(context, ("network-one", "network-unapproved"))
    removed_target = await _compose(context, revision, rebound)
    removed = await _publish(context, removed_target)
    assert removed.generation_id > rebound.generation_id
    assert await fixture.connection.fetchval(f'SELECT count(*) FROM "{removed.schema_name}".network_membership') == 0
    assert await _read(context, original) == before


async def _assert_manual_office(context, manifest, manual):
    from api.network_compact_address import compact_address_native_sql

    fixture = context.fixture
    network_id = manual.network["record_id"]
    async with fixture.execution_connection.transaction(isolation="repeatable_read", readonly=True):
        relation, arguments = compact_address_native_sql(
            manifest.compact_layout,
            first_parameter=1,
            network_ids=(network_id,),
        )
        offices = await fixture.execution_connection.fetch(
            f"SELECT entity_type,entity_id,row_origin,canonical_network_ids FROM {relation} office",
            *arguments,
        )
        assert [dict(office) for office in offices] == [
            dict(
                entity_type="manual",
                entity_id=manual.provider["record_id"],
                row_origin="network_custom",
                canonical_network_ids=[network_id],
            )
        ]
        members = await fixture.execution_connection.fetch(
            f'SELECT provider_system,provider_id,location_id FROM "{manifest.schema_name}".network_membership '
            "WHERE network_id=$1",
            network_id,
        )
        assert [tuple(member) for member in members] == [
            ("manual", manual.provider["record_id"], UUID(manual.first["record_id"])),
        ]


@pytest.mark.parametrize("generic_capture_db", ["modern"], indirect=True)
async def test_manual_office_survives_generic_remap_and_mapping_removal(generic_composition_db):
    """Keep a genuinely approved manual office when current generic bindings disappear."""
    from tests.test_network_custom_address_source_postgres import _seed

    context = generic_composition_db
    original_target = await _compose(context, context.revision)
    original = await _publish(context, original_target, first=True)
    manual = await _seed(context.fixture)
    revision = await _add_mapping(context, "network-unapproved")
    rebound_target = await _compose(context, revision, original)
    rebound = await _publish(context, rebound_target)
    await _assert_manual_office(context, rebound, manual)
    revision = await _close_mappings(context, ("network-one", "network-unapproved"))
    removed_target = await _compose(context, revision, rebound)
    removed = await _publish(context, removed_target)
    await _assert_manual_office(context, removed, manual)
    await _assert_manual_office(context, rebound, manual)
    assert (
        await context.fixture.connection.fetchval(
            f"SELECT count(*) FROM \"{removed.schema_name}\".network_membership WHERE provider_system='npi'"
        )
        == 0
    )
    assert (
        await context.fixture.connection.fetchval(
            f'SELECT count(*) FROM "{removed.schema_name}".provider_location_binding'
        )
        == 1
    )
