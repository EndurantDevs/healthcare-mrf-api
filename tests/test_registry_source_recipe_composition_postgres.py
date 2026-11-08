# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native raw-source replay, reviewed corrections and complete-stage rollback."""

import asyncio
import hashlib
import json
from contextlib import AsyncExitStack, aclosing, asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import Mock
from uuid import UUID, uuid4, uuid5

import asyncpg
import pytest

from process.network_address_projection import PinnedAddressSource, _identifier
from process.network_approved_membership_source import pin_approved_membership_source
from process.network_candidate_custody import seal_network_candidate_custody
from process.network_custom_address_source import prepare_custom_address_source
from process.network_fhir_membership_source import read_fhir_membership_batch
from process.network_fhir_retained_capture_contract import RetainedFHIRCaptureError
from process.network_membership_candidate_lifecycle import (
    admit_network_membership_batch,
    create_network_candidate,
    seal_network_candidate,
)
from process.network_membership_copy import MembershipCopyTarget
from process.network_membership_pipeline import prepare_and_publish_network_candidate
from process.network_serving_read import resolve_network_serving_manifest
from process.registry_candidate_composition import (
    RegistryAddressArtifactCustody,
    RegistryCompositionAddressSources,
    _bind_composition_stage,
    _bindings,
    _record_composition_address,
    _target,
    compose_registry_membership_candidate,
    prepare_registry_composition_address_artifact,
)
from process.registry_initial_source_composition import (
    OfficeSummaryRequest,
    prepare_initial_source_offices,
    record_initial_source_offices_with_reads,
)
from process.registry_membership_composition_stage import (
    build_registry_source_retention,
    drop_registry_command_stage,
    materialize_registry_composition,
    stage_approved_membership_commands,
)
from process.registry_source_recipe_composition import (
    RegistrySourceCompositionError,
    _require_recipe_custody,
    stage_registry_source_recipes,
)
from process.registry_source_recipe_store import RegistrySourceMembershipRecipe, canonical_registry_source_recipes
from process.registry_source_retention import retain_registry_source_memberships
from process.registry_source_selection_receipt import record_registry_source_selection
from tests.cms_registry_recipe_postgres_support import (
    cms_recipe_database as cms_recipe_database,
)
from tests.cms_registry_recipe_postgres_support import (
    cms_recipe_template as cms_recipe_template,
)
from tests.cms_registry_recipe_postgres_support import (
    reviewed_cms_source as reviewed_cms_source,
)
from tests.cms_registry_recipe_postgres_support import (
    serving_schema as serving_schema,
)
from tests.source_contribution_custody_postgres_support import install_source_contribution_custody
from tests.test_network_address_projection_postgres import projection_db as projection_db
from tests.test_network_custom_address_source_postgres import _seed
from tests.test_network_custom_address_source_postgres import bootstrap_db as bootstrap_db
from tests.test_network_custom_address_source_postgres import custom_db as custom_db
from tests.test_network_fhir_membership_source_postgres import (
    _reviewed_binding,
)
from tests.test_network_fhir_membership_source_postgres import (
    fhir_source as fhir_source,
)
from tests.test_network_fhir_membership_source_postgres import (
    reviewed_fhir_source as reviewed_fhir_source,
)
from tests.test_network_fhir_retained_capture_postgres import (
    _approved_capture_recipe,
    _capture,
    _retained_custody_base,
    _retained_custody_captured_input,
    _retained_custody_compose,
    _retained_custody_publisher_login,
)
from tests.test_network_fhir_retained_capture_postgres import legacy_capture_db as legacy_capture_db
from tests.test_network_fhir_retained_capture_postgres import (
    retained_address_custody_db as retained_address_custody_db,
)
from tests.test_network_legacy_membership_source_postgres import reviewed_aca_source as reviewed_aca_source
from tests.test_network_membership_pipeline_postgres import pipeline_db as pipeline_db
from tests.test_network_membership_validation_postgres import validation_db as validation_db
from tests.test_registry_approval_store_postgres import _approve, _command
from tests.test_registry_candidate_composition_postgres import _remove_candidates, _roles
from tests.test_registry_ptg_office_capture_postgres import _stamp_address_witness
from tests.test_registry_retained_site_adoption_postgres import _source_addresses

pytestmark = pytest.mark.asyncio


@asynccontextmanager
async def _complete_cms_office_source(database, serving_schema, monkeypatch, tmp_path):
    """Declare a canonical street office before real admission hashes and seals the source."""
    from tests import cms_registry_recipe_postgres_support as support

    original = support._membership_resource_rows

    def resources(revision_source, revision):
        documents = original(revision_source, revision)
        office = next(document for document in documents["Location"] if document["id"] == "site-1")
        office["address"].update(state="CA", postalCode="90210", country="US")
        return documents

    with monkeypatch.context() as settings:
        settings.setattr(support, "_membership_resource_rows", resources)
        async with aclosing(
            support.reviewed_cms_source.__wrapped__(database, serving_schema, settings, tmp_path)
        ) as sources:
            yield await anext(sources)


@pytest.fixture
async def reviewed_cms_office_source(cms_recipe_database, serving_schema, monkeypatch, tmp_path):
    async with _complete_cms_office_source(cms_recipe_database, serving_schema, monkeypatch, tmp_path) as reviewed:
        yield reviewed


async def _publish_raw_recipe(fixture, reviewed_source, copy_targets, *, seal_candidate=None, writer_roles=None):
    """Retain the actual reviewed office and its raw recipe in a serving manifest."""
    connection, schema = reviewed_source[0][:2]
    _, source_table = await _source_addresses(fixture, 1, "npi")
    await connection.execute(
        f"UPDATE {source_table} SET entity_id='1000000491',npi=1000000491,first_line='1 Sample Street',"
        "second_line=NULL,city_name='Example City',state_name='CA',postal_code='90210',country_code='US',"
        "address_precision='street'"
    )
    await _stamp_address_witness(connection, source_table)
    assert await connection.fetchval(f"SELECT count(*)=1 AND bool_and(address_key IS NOT NULL) FROM {source_table}")
    await connection.execute(f'UPDATE "{fixture.source_schema}".npi SET npi=1000000491')
    identity = uuid4()
    if seal_candidate is not None or getattr(fixture, "seal_candidate", None) is not None:
        copy_target, address_source = await _prepare_raw_recipe_candidate(
            fixture, identity, reviewed_source, copy_targets
        )
    else:
        copy_target = MembershipCopyTarget(
            *(str(uuid4()) for _ in range(3)), str(identity), "network_candidate_" + identity.hex
        )
        copy_targets.append(copy_target)
        address_source = await _prepare_unsealed_raw_recipe(fixture, copy_target, reviewed_source)
    return await _publish_raw_recipe_manifest(
        fixture, connection, copy_target, address_source, schema, seal_candidate, writer_roles
    )


async def _prepare_unsealed_raw_recipe(fixture, copy_target, reviewed_source):
    """Keep the unsealed legacy seed used by missing-custody and incomplete-office refusal tests."""
    (connection, schema, source_pin, sites), _, _, _, coordinates, approved = reviewed_source
    source_table = f'"{fixture.source_schema}".entity_address_unified'
    async with connection.transaction(isolation="repeatable_read"):
        batch = await read_fhir_membership_batch(
            connection,
            source_pin,
            registry_schema=schema,
            approved_source=approved,
            binding_coordinates=coordinates,
            approved_only=True,
        )
        await create_network_candidate(
            connection,
            copy_target,
            source_generations={
                "unified_address": fixture.base.generation_id,
                "fhir": batch.generation_id,
                "custom_membership": approved.generation_id,
            },
            source_recipes=(RegistrySourceMembershipRecipe(source_pin, coordinates),),
            approved_custom_revision=approved.approved_revision,
            expected_head=0,
            expected_rows=batch.membership_rows,
            control_schema=schema,
        )
        await connection.execute(f'''CREATE TABLE "{copy_target.schema_name}".provider_location_binding(
          provider_system text NOT NULL,provider_id text NOT NULL,location_id uuid NOT NULL,
          location_key varchar(64) NOT NULL,entity_type text NOT NULL,entity_id text NOT NULL,
          PRIMARY KEY(provider_system,provider_id,location_id))''')
        await connection.execute(
            f'''INSERT INTO "{copy_target.schema_name}".provider_location_binding
          SELECT 'npi',entity_id,$1,location_key,entity_type,entity_id FROM {source_table}''',
            sites[0],
        )
        await admit_network_membership_batch(
            connection,
            copy_target,
            batch_id=uuid4(),
            input_bytes=batch.input_bytes,
            expected_input_sha256=hashlib.sha256(batch.input_bytes).hexdigest(),
            control_schema=schema,
        )
        await seal_network_candidate(connection, copy_target, control_schema=schema)
    return fixture.base


async def _prepare_raw_recipe_candidate(fixture, request_id, reviewed_source, copy_targets):
    """Retain genuine raw SOURCE rows and approved commands before independent custody."""
    (connection, schema, source_pin, _sites), _, _, _, coordinates, approved = reviewed_source
    recipes = (RegistrySourceMembershipRecipe(source_pin, coordinates),)
    artifact_id = uuid4()
    fixture.composition_ids.append(artifact_id)
    async with connection.transaction(isolation="repeatable_read"):
        # The seed retains memberships; its successor owns the first retained immutable fact artifact.
        source_stage = await stage_registry_source_recipes(
            connection, recipes, approved, request_id=request_id, control_schema=schema
        )
        commands = await stage_approved_membership_commands(connection, approved, request_id, control_schema=schema)
        try:
            retention = await build_registry_source_retention(connection, source_stage, commands)
            artifact = await prepare_custom_address_source(
                connection,
                fixture.base,
                composition_id=str(artifact_id),
                approved_revision=approved.approved_revision,
                owner_role=fixture.roles["owner"],
                runtime_roles=(fixture.roles["reader"],),
                control_schema=schema,
            )
            summary, generations_by_source, office_reads_by_capture = await _prepare_raw_recipe_office_phase(
                connection, recipes, approved, artifact, source_stage, schema
            )
            copy_target = _target(request_id, approved.approved_revision, 0, generations_by_source, summary)
            copy_targets.append(copy_target)
            await _register_raw_recipe_candidate(
                connection,
                copy_target,
                recipes,
                approved,
                (source_stage, commands, retention),
                (artifact, summary, generations_by_source, office_reads_by_capture),
                schema,
            )
        finally:
            await drop_registry_command_stage(connection, commands)
            for name in (source_stage.table_name, source_stage.contribution_table_name):
                if name is not None:
                    await connection.execute(f'DROP TABLE IF EXISTS pg_temp."{name}"')
    return copy_target, artifact.address_source


async def _prepare_raw_recipe_office_phase(connection, recipes, approved, artifact, source_stage, schema):
    office_reads_by_capture = {}
    summary = await prepare_initial_source_offices(
        connection,
        recipes,
        approved,
        artifact.address_source,
        OfficeSummaryRequest(runtime_roles=artifact.runtime_roles, control_schema=schema),
        retained_fhir_reads=office_reads_by_capture,
    )
    generations_by_source = {
        "unified_address": artifact.address_source.generation_id,
        "fhir": source_stage.source_generations[0],
        "custom_membership": approved.generation_id,
    }
    _bind_composition_stage(generations_by_source, source_stage, True, True, False)
    return summary, generations_by_source, office_reads_by_capture


async def _register_raw_recipe_candidate(connection, copy_target, recipes, approved, stages, office_phase, schema):
    source_stage, commands, retention = stages
    artifact, summary, generations_by_source, office_reads_by_capture = office_phase
    assert len(source_stage.source_generations) == 1
    await create_network_candidate(
        connection,
        copy_target,
        source_generations=generations_by_source,
        source_recipes=recipes,
        source_retention=retention,
        approved_custom_revision=approved.approved_revision,
        expected_head=0,
        expected_rows=retention["effective_rows"],
        control_schema=schema,
    )
    await retain_registry_source_memberships(connection, copy_target, source_stage, control_schema=schema)
    await _record_composition_address(connection, copy_target, artifact.address_source, schema)
    await record_registry_source_selection(
        connection, copy_target, recipes, approved, source_stage, control_schema=schema
    )
    await _bindings(connection, copy_target, artifact, None, schema)
    recorded = await record_initial_source_offices_with_reads(
        connection,
        copy_target,
        recipes,
        approved,
        artifact.address_source,
        runtime_roles=artifact.runtime_roles,
        control_schema=schema,
        office_phase=(None, office_reads_by_capture),
    )
    assert recorded == summary
    await materialize_registry_composition(connection, copy_target, commands, control_schema=schema)
    await seal_network_candidate(
        connection, copy_target, source_stage=source_stage, command_stage=commands, control_schema=schema
    )


async def _publish_raw_recipe_manifest(
    fixture, connection, copy_target, address_source, schema, seal_candidate, writer_roles
):
    """Publish the prepared native recipe with its selected seal hook and return the pinned manifest."""
    await prepare_and_publish_network_candidate(
        connection,
        copy_target,
        address_source,
        **(writer_roles or getattr(fixture, "writer_roles", _roles(fixture))),
        control_schema=schema,
        seal_candidate=seal_candidate or getattr(fixture, "seal_candidate", None),
    )
    async with connection.transaction(isolation="repeatable_read", readonly=True):
        return await resolve_network_serving_manifest(connection, control_schema=schema)


async def _publish_replayed_source(fixture, original, approved, request_id, copy_targets):
    """Prepare independent address custody, prove missing-seal refusal, then publish."""
    from tests.test_network_provider_only_read_postgres import _assert_seal_required

    address_sources = await fixture.prepare_address_sources(
        request_id=request_id,
        approved_revision=approved.approved_revision,
        expected_head=original.generation_id,
        source_manifest=original,
        address_sources=RegistryCompositionAddressSources(
            PinnedAddressSource(original.schema_name, "entity_address_unified", original.manifest_sha256)
        ),
    )
    copy_target, addresses = await compose_registry_membership_candidate(
        fixture.execution_connection,
        request_id=request_id,
        approved_revision=approved.approved_revision,
        expected_head=original.generation_id,
        address_sources=address_sources,
        writer_roles=fixture.writer_roles,
        source_manifest=original,
        control_schema=fixture.control_schema,
    )
    copy_targets.append(copy_target)
    executing_fixture = SimpleNamespace(
        connection=fixture.execution_connection,
        writer_roles=fixture.writer_roles,
        control_schema=fixture.control_schema,
    )
    await _assert_seal_required(executing_fixture, copy_target, addresses, original.generation_id)
    await prepare_and_publish_network_candidate(
        fixture.execution_connection,
        copy_target,
        addresses,
        **fixture.writer_roles,
        control_schema=fixture.control_schema,
        seal_candidate=fixture.seal_candidate,
    )
    return copy_target


async def _compose_custodied_source(context, *, revision, source_manifest):
    fixture = context.fixture
    request_id = uuid4()
    context.request_ids.append(request_id)
    async with fixture.execution_connection.transaction(isolation="repeatable_read", readonly=True):
        approved = await pin_approved_membership_source(
            fixture.execution_connection, approved_revision=revision, control_schema=fixture.control_schema
        )
    fixture.composition_ids.append(uuid5(request_id, "address:" + approved.generation_id))
    copy_target = await _publish_replayed_source(fixture, source_manifest, approved, request_id, context.copy_targets)
    async with fixture.execution_connection.transaction(isolation="repeatable_read", readonly=True):
        manifest = await resolve_network_serving_manifest(
            fixture.execution_connection, control_schema=fixture.control_schema
        )
        assert (
            await fixture.execution_connection.fetchval(
                f'SELECT count(*) FROM "{fixture.control_schema}".network_candidate_custody WHERE candidate_id=$1',
                UUID(copy_target.candidate_id),
            )
            == 1
        )
    return manifest


@pytest.fixture
async def source_composition_custody(custom_db, reviewed_cms_office_source):
    async with AsyncExitStack() as stack:
        source_pin = reviewed_cms_office_source[0][2]
        await install_source_contribution_custody(custom_db, source_pin, stack)
        for role in sorted(set(source_pin.retained_epoch.runtime_roles)):
            await custom_db.connection.execute(
                f"GRANT {_identifier(role)} TO {_identifier(custom_db.executor_role)} "
                "WITH ADMIN FALSE, INHERIT TRUE, SET FALSE"
            )
        custodian = custom_db.source_custodian
        runtime_roles = tuple(
            sorted((custom_db.roles["reader"], custom_db.executor_role, custom_db.writer_roles["owner_role"]))
        )
        custody = RegistryAddressArtifactCustody(
            custom_db.custody_roles["owner"], runtime_roles, custom_db.control_schema
        )

        async def prepare_address_sources(**arguments):
            address_sources = arguments["address_sources"]
            async with custodian.transaction(isolation="repeatable_read"):
                artifact = await prepare_registry_composition_address_artifact(custodian, custody=custody, **arguments)
            return address_sources if artifact is None else replace(address_sources, prepared_address_artifact=artifact)

        async def seal_candidate(target):
            async def authorize(connection, supplied):
                assert connection is custodian and supplied == target
                return await connection.fetchval(
                    "SELECT current_user=session_user AND current_user::text=$1", custom_db.custody_roles["custodian"]
                )

            return await seal_network_candidate_custody(
                custodian,
                target,
                control_schema=custom_db.control_schema,
                runtime_roles=runtime_roles,
                authorize=authorize,
            )

        custom_db.seal_candidate = seal_candidate
        custom_db.prepare_address_sources = prepare_address_sources
        yield seal_candidate


@pytest.mark.parametrize("operation", ["rebind", "close"])
async def test_raw_recipe_publication_replays_reviewed_change_and_latest_manual(
    custom_db, reviewed_cms_office_source, source_composition_custody, operation
):
    """The retained raw edition follows a reviewed change and preserves later manual offices."""
    fixture = custom_db
    source_fixture, actor, networks, binding, coordinates, _ = reviewed_cms_office_source
    connection, schema, source_pin, sites = source_fixture
    copy_targets = []
    request_id = uuid4()
    try:
        original = await _publish_raw_recipe(fixture, reviewed_cms_office_source, copy_targets)
        manual = await _seed(fixture)
        changed = await _review_source_operation(connection, schema, actor, binding, operation, networks)
        approval = await _approve(connection, schema, await _command(connection, schema, changed), actor)
        async with connection.transaction(isolation="repeatable_read", readonly=True):
            approved = await pin_approved_membership_source(
                connection,
                approved_revision=approval["approved_revision"],
                control_schema=schema,
            )
        fixture.composition_ids.append(uuid5(request_id, "address:" + approved.generation_id))
        copy_target = await _publish_replayed_source(fixture, original, approved, request_id, copy_targets)
        await _assert_replayed_publication(
            fixture,
            copy_target,
            original,
            manual,
            networks,
            sites[0],
            operation,
            approved.approved_revision,
        )
        assert not await connection.fetchval(
            "SELECT EXISTS(SELECT 1 FROM pg_class WHERE relnamespace=pg_my_temp_schema() "
            "AND (relname LIKE 'registry_recipe_raw_%' OR relname LIKE 'registry_source_page_%'))"
        )
        recipe = RegistrySourceMembershipRecipe(source_pin, coordinates)
        candidate = await connection.fetchrow(
            f'SELECT source_recipes_json::text FROM "{schema}".network_membership_candidate WHERE candidate_id=$1',
            UUID(copy_target.candidate_id),
        )
        assert json.loads(candidate["source_recipes_json"]) == json.loads(canonical_registry_source_recipes((recipe,)))
    finally:
        await _remove_candidates(fixture, copy_targets, request_id)


async def _assert_replayed_publication(
    fixture,
    copy_target,
    original,
    manual,
    networks,
    source_site,
    operation,
    approved_revision,
):
    connection = fixture.connection
    async with connection.transaction(isolation="repeatable_read", readonly=True):
        current = await resolve_network_serving_manifest(connection, control_schema=fixture.control_schema)
        retained = await resolve_network_serving_manifest(
            connection,
            generation_id=original.generation_id,
            control_schema=fixture.control_schema,
        )
        assert retained == original and current.generation_id > original.generation_id
        assert current.approved_custom_revision == approved_revision > manual.revision
        assert (
            current.source_generations["registry_source_recipes"]
            == original.source_generations["registry_source_recipes"]
        )
        address_rows = await connection.fetch(f'''SELECT entity_type,entity_id,canonical_network_ids,
          pg_typeof(canonical_network_ids)::text AS array_type FROM "{current.schema_name}".entity_address_unified''')
        assert len(address_rows) == 2 and all(address_row["array_type"] == "integer[]" for address_row in address_rows)
        networks_by_system = {
            address_row["entity_type"]: address_row["canonical_network_ids"] for address_row in address_rows
        }
        assert networks_by_system == {
            "manual": [manual.network["record_id"]],
            "npi": [networks[1]["record_id"]] if operation == "rebind" else [],
        }
        members = await connection.fetch(f'SELECT * FROM "{copy_target.schema_name}".network_membership')
        manual_members = [member for member in members if member["provider_system"] == "manual"]
        imported_members = [member for member in members if member["provider_system"] == "npi"]
        assert len(manual_members) == 1 and str(manual_members[0]["location_id"]) == manual.first["record_id"]
        assert manual_members[0]["provider_id"] == manual.provider["record_id"]
        assert len(imported_members) == (1 if operation == "rebind" else 0)
        if imported_members:
            assert imported_members[0]["location_id"] == source_site
        assert await connection.fetchval(
            f'SELECT location_id FROM "{copy_target.schema_name}".provider_location_binding '
            "WHERE provider_system='npi' AND provider_id='1000000491'"
        ) == (source_site if operation == "rebind" else None)
        assert await connection.fetchval(
            f'SELECT canonical_network_ids FROM "{original.schema_name}".entity_address_unified'
        ) == [networks[0]["record_id"]]
        await _assert_source_selection_counts(connection, fixture.control_schema, copy_target, operation)


async def _review_source_operation(connection, schema, actor, binding, operation, networks):
    command_by_field = {
        **binding,
        "operation": operation,
        "expected_revision": 1,
        "expected_network_id": binding["network_id"],
        "network_id": networks[1]["record_id"] if operation == "rebind" else binding["network_id"],
    }
    return (await _reviewed_binding(connection, schema, actor, [command_by_field]))["records"][0]


async def _assert_source_selection_counts(connection, schema, copy_target, operation):
    from process.registry_publication_execution import _source_selection_counts

    candidate = await connection.fetchrow(
        f'SELECT * FROM "{schema}".network_membership_candidate WHERE candidate_id=$1',
        UUID(copy_target.candidate_id),
    )
    assert _source_selection_counts(dict(candidate)) == {
        "source_selection": {
            "mapped_rows": 1 if operation == "rebind" else 0,
            "omitted_rows": 0 if operation == "rebind" else 1,
        }
    }


async def _stage(connection, schema, recipes):
    async with connection.transaction(isolation="repeatable_read"):
        revision = await connection.fetchval(f'SELECT approved_revision FROM "{schema}".registry_revision_control')
        approved = await pin_approved_membership_source(connection, approved_revision=revision, control_schema=schema)
        stage = await stage_registry_source_recipes(
            connection, recipes, approved, request_id=uuid4(), control_schema=schema
        )
        try:
            documents = await connection.fetch(f'SELECT input_json FROM pg_temp."{stage.table_name}" ORDER BY ordinal')
            members = [json.loads(document["input_json"]) for document in documents]
            assert len(members) == stage.membership_rows
            assert await connection.fetchval(f'SELECT generation_id FROM "{schema}".network_serving_control') is None
            return stage, members
        finally:
            await connection.execute(f'DROP TABLE pg_temp."{stage.table_name}"')
            if stage.contribution_table_name is not None:
                await connection.execute(f'DROP TABLE pg_temp."{stage.contribution_table_name}"')


@pytest.mark.parametrize("source_kind", ["fhir", "aca"])
async def test_actual_reviewed_sources_stage_exact_offices(reviewed_cms_source, reviewed_aca_source, source_kind):
    """Actual sealed source readers feed native COPY and retain exact office identity."""
    fixture = reviewed_cms_source if source_kind == "fhir" else reviewed_aca_source
    source_fixture, _, networks, _, coordinates, _ = fixture
    connection, schema, source_pin, sites = source_fixture
    recipe = RegistrySourceMembershipRecipe(source_pin, coordinates)
    before = await connection.fetchval(f'SELECT count(*) FROM "{schema}".network_membership_candidate')
    stage, members = await _stage(connection, schema, (recipe,))
    assert stage.membership_rows == (1 if source_kind == "fhir" else 2) and stage.omitted_rows == 0
    assert {member["network_id"] for member in members} == {networks[0]["record_id"]}
    assert {member["location_id"] for member in members} == (
        {str(sites[0])} if source_kind == "fhir" else set(map(str, sites))
    )
    assert all(member["provider_system"] == "npi" and len(member["evidence_id"]) == 64 for member in members)
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".network_membership_candidate') == before
    assert await connection.fetchval("SELECT to_regclass($1)", "pg_temp." + stage.table_name) is None


async def test_rebind_and_close_replay_same_recipe(reviewed_cms_source):
    """A new approved mapping changes membership while the raw recipe stays exact."""
    source_fixture, actor, networks, binding, coordinates, _ = reviewed_cms_source
    connection, schema, source_pin, sites = source_fixture
    recipe = RegistrySourceMembershipRecipe(source_pin, coordinates)
    canonical = canonical_registry_source_recipes((recipe,))
    first, old_members = await _stage(connection, schema, (recipe,))
    rebound_by_field = {
        **binding,
        "operation": "rebind",
        "expected_revision": 1,
        "expected_network_id": binding["network_id"],
        "network_id": networks[1]["record_id"],
    }
    changed = (await _reviewed_binding(connection, schema, actor, [rebound_by_field]))["records"][0]
    await _approve(connection, schema, await _command(connection, schema, changed), actor)
    second, new_members = await _stage(connection, schema, (recipe,))
    assert new_members[0]["network_id"] == networks[1]["record_id"] != old_members[0]["network_id"]
    assert new_members[0]["location_id"] == old_members[0]["location_id"] == str(sites[0])
    assert first.generation_sha256 != second.generation_sha256
    closed_by_field = {
        **rebound_by_field,
        "operation": "close",
        "expected_revision": 2,
        "expected_network_id": networks[1]["record_id"],
    }
    changed = (await _reviewed_binding(connection, schema, actor, [closed_by_field]))["records"][0]
    await _approve(connection, schema, await _command(connection, schema, changed), actor)
    third, removed_members = await _stage(connection, schema, (recipe,))
    assert removed_members == [] and third.membership_rows == 0 and third.omitted_rows == 1
    assert third.generation_sha256 != second.generation_sha256
    assert canonical_registry_source_recipes((recipe,)) == canonical


async def test_generic_source_cannot_enter_production_stage(reviewed_fhir_source):
    """Legacy reader-compatible coordinates provide no production source custody."""
    source_fixture, _, _, _, coordinates, _ = reviewed_fhir_source
    connection, schema, source_pin, _ = source_fixture
    async with connection.transaction(isolation="repeatable_read"):
        revision = await connection.fetchval(f'SELECT approved_revision FROM "{schema}".registry_revision_control')
        approved = await pin_approved_membership_source(connection, approved_revision=revision, control_schema=schema)
        with pytest.raises(RegistrySourceCompositionError, match="custody_unavailable"):
            await stage_registry_source_recipes(
                connection,
                (RegistrySourceMembershipRecipe(source_pin, coordinates),),
                approved,
                request_id=uuid4(),
                control_schema=schema,
            )
        assert await connection.fetchval("SELECT 1") == 1
        assert not await connection.fetchval(
            "SELECT EXISTS(SELECT 1 FROM pg_class WHERE relnamespace=pg_my_temp_schema() AND relname LIKE 'registry_recipe_%')"
        )


class _FailedNativeCopy:
    def __init__(self, connection):
        self.connection = connection
        self.copied_rows = 0

    def __getattr__(self, name):
        return getattr(self.connection, name)

    async def copy_to_table(self, table_name, **options):
        result = await self.connection.copy_to_table(table_name, **options)
        self.copied_rows = await self.connection.fetchval(f'SELECT count(*) FROM pg_temp."{table_name}"')
        await self.connection.execute("SELECT 1/0")
        return result


async def test_native_copy_fault_rolls_back_complete_admitted_stage(reviewed_cms_source):
    """A real native error after successful COPY rolls back the complete temporary stage."""
    source_fixture, _, _, _, coordinates, _ = reviewed_cms_source
    connection, schema, source_pin, _ = source_fixture
    interrupted = _FailedNativeCopy(connection)
    async with connection.transaction(isolation="repeatable_read"):
        revision = await connection.fetchval(f'SELECT approved_revision FROM "{schema}".registry_revision_control')
        approved = await pin_approved_membership_source(connection, approved_revision=revision, control_schema=schema)
        with pytest.raises(asyncpg.DivisionByZeroError):
            await stage_registry_source_recipes(
                interrupted,
                (RegistrySourceMembershipRecipe(source_pin, coordinates),),
                approved,
                request_id=uuid4(),
                control_schema=schema,
            )
        assert interrupted.copied_rows == 1
        assert await connection.fetchval("SELECT 1") == 1
        assert not await connection.fetchval(
            "SELECT EXISTS(SELECT 1 FROM pg_class WHERE relnamespace=pg_my_temp_schema() AND (relname LIKE 'registry_recipe_raw_%' OR relname LIKE 'registry_source_page_%'))"
        )


@pytest.mark.parametrize("reviewed_aca_source", ["raw_missing", "raw_foreign_source_url"], indirect=True)
async def test_complete_aca_recipe_missing_raw_tail_rolls_back_all_staged_pages(reviewed_aca_source, monkeypatch):
    import process.registry_source_recipe_composition as composition

    original = composition._read_recipe_page
    visited_outcomes = []

    async def one_fact_page(
        connection, recipe, approved, cursor, control_schema, limit, office_source=None, *, capture_facts=False
    ):
        batch, next_cursor, _ = await original(
            connection, recipe, approved, cursor, control_schema, 1, office_source, capture_facts=capture_facts
        )
        visited_outcomes.extend(batch.source_outcomes)
        return batch, next_cursor, 1

    monkeypatch.setattr(composition, "_read_recipe_page", one_fact_page)
    fixture, _, _, _, coordinates, _ = reviewed_aca_source
    connection, schema, source, _ = fixture
    with pytest.raises(RegistrySourceCompositionError, match="unresolved"):
        await _stage(connection, schema, (RegistrySourceMembershipRecipe(source, coordinates),))
    assert [item.outcome for item in visited_outcomes] == ["accepted", "accepted", "unresolved"]
    assert not await connection.fetchval(
        "SELECT EXISTS(SELECT 1 FROM pg_class WHERE relnamespace=pg_my_temp_schema() "
        "AND (relname LIKE 'registry_recipe_raw_%' OR relname LIKE 'registry_source_page_%'))"
    )


async def test_complete_aca_recipe_reports_retained_fact_totals(reviewed_aca_source):
    fixture, _, _, _, coordinates, _ = reviewed_aca_source
    connection, schema, source, _ = fixture
    stage, members = await _stage(connection, schema, (RegistrySourceMembershipRecipe(source, coordinates),))
    assert (stage.raw_source_rows, stage.accepted_raw_source_rows, stage.omitted_raw_source_rows) == (2, 2, 0)
    assert len(members) == 2


@pytest.fixture
async def disposition_cms_source(request, cms_recipe_database, serving_schema, monkeypatch, tmp_path):
    """Admit the declared input variation before hashes, publication and real custody."""
    from tests import cms_registry_recipe_postgres_support as support

    original = support._membership_resource_rows

    def resources(revision_source, revision):
        documents = original(revision_source, revision)
        if request.param == "omitted":
            documents["PractitionerRole"].extend(
                [
                    {"id": "zz-expired", "period": {"end": "2025-01-01"}},
                    {"id": "zz-future", "period": {"start": "2027-01-01"}},
                ]
            )
        elif request.param == "provider_only":
            documents["PractitionerRole"].append(
                {**documents["PractitionerRole"][0], "id": "zz-provider-only", "location": []}
            )
        else:
            documents["PractitionerRole"].append(
                {
                    **documents["PractitionerRole"][0],
                    "id": "zz-partial",
                    "location": [{"reference": "Location/site-1"}, {"reference": "Location/missing"}],
                }
            )
        return documents

    monkeypatch.setattr(support, "_membership_resource_rows", resources)
    async with aclosing(
        support.reviewed_cms_source.__wrapped__(cms_recipe_database, serving_schema, monkeypatch, tmp_path)
    ) as fixture:
        yield await anext(fixture)


@pytest.mark.parametrize("disposition_cms_source", ["omitted"], indirect=True)
async def test_fhir_recipe_census_counts_omitted_tail_once(disposition_cms_source, monkeypatch):
    import process.registry_source_recipe_composition as composition

    fixture, _, _, _, coordinates, _ = disposition_cms_source
    connection, schema, source_pin, _ = fixture
    original = composition._read_recipe_page
    dispositions = []

    async def one_resource(
        connection, recipe, approved, cursor, control_schema, limit, office_source=None, *, capture_facts=False
    ):
        batch, next_cursor, _ = await original(
            connection, recipe, approved, cursor, control_schema, 1, office_source, capture_facts=capture_facts
        )
        dispositions.extend(batch.source_outcomes)
        return batch, next_cursor, 1

    monkeypatch.setattr(composition, "_read_recipe_page", one_resource)
    recipe = RegistrySourceMembershipRecipe(source_pin, coordinates)
    first, members = await _stage(connection, schema, (recipe,))
    assert (first.raw_source_rows, first.accepted_raw_source_rows, first.omitted_raw_source_rows) == (4, 1, 3)
    assert first.membership_rows == len(members) == 1
    assert len({(disposition.resource_type, disposition.resource_id) for disposition in dispositions}) == 4
    assert len(dispositions) == 4
    second, retried = await _stage(connection, schema, (recipe,))
    assert second.generation_sha256 == first.generation_sha256 and retried == members


@pytest.mark.parametrize("disposition_cms_source", ["partial"], indirect=True)
async def test_late_fhir_partial_resource_refuses_complete_recipe(disposition_cms_source, monkeypatch):
    import process.registry_source_recipe_composition as composition

    fixture, _, _, _, coordinates, _ = disposition_cms_source
    connection, schema, source_pin, _ = fixture
    original, encode = composition._read_recipe_page, composition._encode
    dispositions, encoded_pages = [], []

    async def one_resource(
        connection, recipe, approved, cursor, control_schema, limit, office_source=None, *, capture_facts=False
    ):
        batch, next_cursor, _ = await original(
            connection, recipe, approved, cursor, control_schema, 1, office_source, capture_facts=capture_facts
        )
        dispositions.extend(batch.source_outcomes)
        return batch, next_cursor, 1

    def observed_encode(input_bytes):
        encoded_pages.append(input_bytes)
        return encode(input_bytes)

    monkeypatch.setattr(composition, "_read_recipe_page", one_resource)
    monkeypatch.setattr(composition, "_encode", observed_encode)
    with pytest.raises(RegistrySourceCompositionError, match="unresolved"):
        await _stage(connection, schema, (RegistrySourceMembershipRecipe(source_pin, coordinates),))
    assert [disposition.outcome for disposition in dispositions] == ["omitted", "accepted", "unresolved"]
    assert dispositions[-1].resource_id == "zz-partial" and len(encoded_pages) == 2
    assert not await connection.fetchval(
        "SELECT EXISTS(SELECT FROM pg_class WHERE relnamespace=pg_my_temp_schema() "
        "AND (relname LIKE 'registry_recipe_raw_%' OR relname LIKE 'registry_source_page_%'))"
    )
    assert await connection.fetchval(f'SELECT generation_id FROM "{schema}".network_serving_control') is None


def _count_retained_content_reads(monkeypatch):
    from process import network_fhir_retained_capture as retained

    native_content, native_catalog = retained._content_tables, retained._catalog
    contents, catalogs = [], []

    async def content(connection, schema):
        contents.append(schema)
        return await native_content(connection, schema)

    async def catalog(connection, schema, owner, runtime_roles):
        catalogs.append(schema)
        return await native_catalog(connection, schema, owner, runtime_roles)

    monkeypatch.setattr(retained, "_content_tables", content)
    monkeypatch.setattr(retained, "_catalog", catalog)
    return contents, catalogs


@pytest.mark.parametrize("legacy_capture_db", ["legacy", "modern"], indirect=True)
@pytest.mark.parametrize("legacy_entry", [False, True])
async def test_initial_retained_composition_admits_content_once_per_transaction(
    retained_address_custody_db, legacy_capture_db, serving_schema, bootstrap_db, monkeypatch, legacy_entry
):
    case, source_fixture = retained_address_custody_db, legacy_capture_db
    capture = await _retained_custody_captured_input(source_fixture, case.complete_runtime_roles)
    recipe, approved, _ = await _approved_capture_recipe(source_fixture, serving_schema, capture)
    base = await _retained_custody_base(case, source_fixture, serving_schema, bootstrap_db)
    case.additional_schemas.append(case.copy_target.schema_name)
    _, publisher = await _retained_custody_publisher_login(case)
    contents, catalogs = _count_retained_content_reads(monkeypatch)
    if legacy_entry:
        from process import registry_candidate_composition as composition

        monkeypatch.setattr(composition, "_verified_office_inputs", Mock(return_value=None))
        monkeypatch.setattr(composition, "record_initial_source_offices_with_reads", _record_initial_offices_legacy)
    try:
        (copy_target, address), arguments = await _retained_custody_compose(
            case, publisher, base, recipe, approved.approved_revision
        )
        assert address == base and contents == [capture.schema_name] * (1 + 3 * legacy_entry)
        assert catalogs.count(capture.schema_name) >= 5
        assert await compose_registry_membership_candidate(publisher, **arguments) == (copy_target, address)
        # Legacy replay separately admits SOURCE and the independently rebuilt office summary.
        assert contents == [capture.schema_name] * (2 + 4 * legacy_entry)
        if not legacy_entry:
            await _assert_mixed_office_receipt_refused(case, publisher, copy_target, address, recipe, approved)
            assert contents == [capture.schema_name] * 3
    finally:
        await publisher.close()


async def _assert_mixed_office_receipt_refused(case, connection, target, address, recipe, approved):
    from process import registry_initial_source_composition as initial

    runtime_roles = tuple(sorted((case.writer_roles["loader"], case.writer_roles["reader"])))
    async with connection.transaction(isolation="repeatable_read"):
        legacy = await initial._office_summary(
            connection, (recipe,), approved, address, runtime_roles, case.control_schema
        )
        candidate_by_field = dict(
            await connection.fetchrow(
                f'SELECT * FROM "{case.control_schema}".network_membership_candidate WHERE candidate_id=$1',
                UUID(target.candidate_id),
            )
        )
        retained = initial.verify_initial_source_office_receipt(candidate_by_field)
        assert {key: value for key, value in retained.items() if key != "correspondence_sha256"} == {
            key: value for key, value in legacy.items() if key != "correspondence_sha256"
        }
        assert retained["correspondence_sha256"] != legacy["correspondence_sha256"]
        report = json.loads(candidate_by_field["validation_json"])
        report[initial.SUMMARY_KEY] = legacy
        with pytest.raises(initial.RegistryInitialSourceError, match="registry_initial_source_office_receipt_invalid"):
            initial.verify_initial_source_office_receipt(dict(candidate_by_field, validation_json=json.dumps(report)))


async def _record_initial_offices_legacy(connection, target, recipes, approved, address, **options_by_name):
    """Exercise the old record and summary signatures against genuine native offices."""
    from process import registry_initial_source_composition as initial

    authority, _retained_reads, verified_inputs = options_by_name.pop("office_phase")
    assert verified_inputs is None
    receipt = await initial.record_initial_source_offices(
        connection,
        target,
        recipes,
        approved,
        address,
        request=OfficeSummaryRequest(office_authority=authority, **options_by_name),
    )
    before = await initial._office_summary(
        connection, recipes, approved, address, options_by_name["runtime_roles"], options_by_name["control_schema"]
    )
    assert before == receipt
    return receipt


@pytest.mark.parametrize("scope", ["task", "connection", "transaction"])
async def test_retained_admission_reuse_refuses_another_native_scope(legacy_capture_db, serving_schema, scope):
    source = legacy_capture_db
    capture = await _capture(source)
    recipe, _, _ = await _approved_capture_recipe(source, serving_schema, capture)
    reader = await source.connect("reader")
    reads_by_capture = {}
    async with reader.transaction(isolation="repeatable_read", readonly=True):
        admitted = await _require_recipe_custody(reader, recipe, retained_fhir_reads=reads_by_capture)
        assert await _require_recipe_custody(reader, recipe, retained_fhir_reads=reads_by_capture) is admitted
        if scope == "task":
            with pytest.raises(RetainedFHIRCaptureError):
                await asyncio.create_task(_require_recipe_custody(reader, recipe, retained_fhir_reads=reads_by_capture))
        elif scope == "connection":
            with pytest.raises(RetainedFHIRCaptureError):
                await _reuse_retained_on_new_connection(source, recipe, reads_by_capture)
    if scope == "transaction":
        async with reader.transaction(isolation="repeatable_read", readonly=True):
            with pytest.raises(RetainedFHIRCaptureError):
                await _require_recipe_custody(reader, recipe, retained_fhir_reads=reads_by_capture)


async def _reuse_retained_on_new_connection(source, recipe, reads_by_capture):
    other = await source.connect("reader")
    async with other.transaction(isolation="repeatable_read", readonly=True):
        return await _require_recipe_custody(other, recipe, retained_fhir_reads=reads_by_capture)


async def test_new_retained_capture_requires_its_own_full_admission(legacy_capture_db, serving_schema, monkeypatch):
    source = legacy_capture_db
    first, second = await _capture(source), await _capture(source)
    recipe, _, _ = await _approved_capture_recipe(source, serving_schema, first)
    successor = replace(
        recipe,
        source_pin=replace(recipe.source_pin, retained_capture=second, release_id=second.release_id),
        binding_coordinates=replace(recipe.binding_coordinates, edition_id=second.capture_id),
    )
    reader, reads_by_capture = await source.connect("reader"), {}
    contents, _ = _count_retained_content_reads(monkeypatch)
    async with reader.transaction(isolation="repeatable_read", readonly=True):
        first_read = await _require_recipe_custody(reader, recipe, retained_fhir_reads=reads_by_capture)
        reads_by_capture[second] = first_read
        with pytest.raises(RetainedFHIRCaptureError):
            await _require_recipe_custody(reader, successor, retained_fhir_reads=reads_by_capture)
        del reads_by_capture[second]
        second_read = await _require_recipe_custody(reader, successor, retained_fhir_reads=reads_by_capture)
        assert first_read is not second_read and contents == [first.schema_name, second.schema_name]
        assert await _require_recipe_custody(reader, recipe, retained_fhir_reads=reads_by_capture) is first_read
        assert await _require_recipe_custody(reader, successor, retained_fhir_reads=reads_by_capture) is second_read
        assert contents == [first.schema_name, second.schema_name]


async def test_shared_retained_admission_rechecks_native_writer_privileges(legacy_capture_db, serving_schema):
    source = legacy_capture_db
    capture = await _capture(source)
    recipe, _, _ = await _approved_capture_recipe(source, serving_schema, capture)
    reader, reads_by_capture = await source.connect("reader"), {}
    table = f'"{capture.schema_name}".provider_directory_dataset_resource'
    role = '"' + source.role_by_kind["reader"] + '"'
    try:
        async with reader.transaction(isolation="repeatable_read", readonly=True):
            await _require_recipe_custody(reader, recipe, retained_fhir_reads=reads_by_capture)
            await source.admin.execute(f"GRANT UPDATE ON {table} TO {role}")
            with pytest.raises((RetainedFHIRCaptureError, ValueError)):
                await _require_recipe_custody(reader, recipe, retained_fhir_reads=reads_by_capture)
    finally:
        await source.admin.execute(f"REVOKE UPDATE ON {table} FROM {role}")


async def _assert_original_fact_reference(fixture, current, owner, operation):
    from process import registry_source_contribution_store as contributions

    candidate = await fixture.connection.fetchrow(
        f'SELECT * FROM "{fixture.control_schema}".network_membership_candidate WHERE candidate_id=$1',
        UUID(current.candidate_id),
    )
    descriptor = contributions.verify_registry_source_contributions(candidate)["sources"][0]
    assert descriptor["fact_candidate_id"] == owner.candidate_id
    assert descriptor["fact_rows"] == 2
    assert (
        await fixture.connection.fetchval(f'SELECT count(*) FROM "{current.schema_name}".network_source_contribution')
        == 0
    )
    assert await fixture.connection.fetchval(f'SELECT count(*) FROM "{current.schema_name}".network_membership') == (
        1 if operation == "rebind" else 0
    )


async def test_captured_fhir_facts_remap_rebind_and_close_without_membership_replay(
    custom_db, reviewed_cms_office_source, source_composition_custody, monkeypatch
):
    """Keep original facts while rechecking native office custody through rebind and close."""
    from process import registry_initial_source_composition as offices
    from process import registry_source_recipe_composition as composition

    fixture = custom_db
    _, actor, networks, binding, _, approved = reviewed_cms_office_source
    context = SimpleNamespace(fixture=fixture, copy_targets=[], request_ids=[])
    try:
        original = await _publish_raw_recipe(fixture, reviewed_cms_office_source, context.copy_targets)
        owner = await _compose_custodied_source(context, revision=approved.approved_revision, source_manifest=original)

        async def forbidden_raw_read(*args, **kwargs):
            pytest.fail("unchanged accepted FHIR membership facts were replayed")

        monkeypatch.setattr(composition, "_read_recipe_page", forbidden_raw_read)
        monkeypatch.setattr(offices, "_read_recipe_page", forbidden_raw_read)
        current = owner
        for operation, revision, old_network, new_network in (
            ("rebind", 1, networks[0]["record_id"], networks[1]["record_id"]),
            ("close", 2, networks[1]["record_id"], networks[1]["record_id"]),
        ):
            changed = (
                await _reviewed_binding(
                    fixture.connection,
                    fixture.control_schema,
                    actor,
                    [
                        {
                            **binding,
                            "operation": operation,
                            "expected_revision": revision,
                            "expected_network_id": old_network,
                            "network_id": new_network,
                        }
                    ],
                )
            )["records"][0]
            approval = await _approve(
                fixture.connection,
                fixture.control_schema,
                await _command(fixture.connection, fixture.control_schema, changed),
                actor,
            )
            current = await _compose_custodied_source(
                context, revision=approval["approved_revision"], source_manifest=current
            )
            await _assert_original_fact_reference(fixture, current, owner, operation)
    finally:
        for request_id in context.request_ids:
            await _remove_candidates(fixture, context.copy_targets, request_id)


@pytest.mark.parametrize("disposition_cms_source", ["provider_only"], indirect=True)
async def test_provider_only_fhir_facts_never_become_office_memberships(disposition_cms_source):
    fixture, _, _, _, coordinates, _ = disposition_cms_source
    connection, schema, source, sites = fixture
    stage, members = await _stage(connection, schema, (RegistrySourceMembershipRecipe(source, coordinates),))
    assert stage.provider_only_rows == 1
    assert stage.membership_rows == len(members) == 1
    assert stage.raw_source_rows == 3 and stage.accepted_raw_source_rows == 2
    assert members[0]["location_id"] == str(sites[0])
    assert stage.contribution_receipts[0]["provider_only_rows"] == 1


async def _assert_fact_storage_tamper(connection, copy_target, candidate_by_field, receipt, schema):
    from process import registry_source_contribution_store as contributions

    identity = UUID(copy_target.candidate_id)
    await connection.execute(
        f'''UPDATE "{copy_target.schema_name}".network_source_contribution SET fact_json=jsonb_set(
          fact_json,'{{provider_id}}','"1999999999"') WHERE fact_json->>'omission_reason' IS NULL'''
    )
    with pytest.raises(contributions.RegistrySourceContributionError, match="content_invalid"):
        await contributions.verify_registry_source_contributions_rows(connection, copy_target, candidate_by_field)
    forged = json.loads(json.dumps(receipt))
    forged["sources"][0]["fact_sha256"], _ = await contributions.contribution_fact_sha256(
        connection, f'"{copy_target.schema_name}".network_source_contribution', forged["sources"][0]["recipe_sha256"]
    )
    await connection.execute(
        f'''UPDATE "{schema}".network_membership_candidate SET source_generations=jsonb_set(
          source_generations,'{{registry_source_contributions}}',to_jsonb($2::text)),
          validation_json=jsonb_set(validation_json,'{{registry_source_contributions}}',$3::jsonb)
          WHERE candidate_id=$1''',
        identity,
        contributions.registry_source_contribution_sha256(forged),
        json.dumps(forged),
    )
    forged_candidate = await connection.fetchrow(
        f'SELECT * FROM "{schema}".network_membership_candidate WHERE candidate_id=$1', identity
    )
    assert (
        await contributions.verify_registry_source_contributions_rows(connection, copy_target, forged_candidate)
        == forged
    )


async def test_native_candidate_registration_retains_exact_source_facts(reviewed_cms_source):
    from process import registry_source_contribution_store as contributions

    fixture, _, _, _, coordinates, approved = reviewed_cms_source
    connection, schema, source_pin, _ = fixture
    recipe = RegistrySourceMembershipRecipe(source_pin, coordinates)
    identity = uuid4()
    copy_target = MembershipCopyTarget(
        *(str(uuid4()) for _ in range(3)), str(identity), "network_candidate_" + identity.hex
    )
    try:
        async with connection.transaction(isolation="repeatable_read"):
            stage = await stage_registry_source_recipes(
                connection, (recipe,), approved, request_id=uuid4(), control_schema=schema
            )
            receipt = contributions.registry_source_contribution_receipt(stage)
            await create_network_candidate(
                connection,
                copy_target,
                source_generations={
                    "custom_membership": approved.generation_id,
                    "registry_source_membership": stage.generation_sha256,
                },
                source_recipes=(recipe,),
                source_contributions=receipt,
                approved_custom_revision=approved.approved_revision,
                expected_head=0,
                expected_rows=stage.membership_rows,
                control_schema=schema,
            )
            assert (
                await contributions.retain_registry_source_contributions(
                    connection, copy_target, stage, control_schema=schema
                )
                == receipt
            )
            candidate = await connection.fetchrow(
                f'SELECT * FROM "{schema}".network_membership_candidate WHERE candidate_id=$1', identity
            )
            assert (
                await contributions.verify_registry_source_contributions_rows(
                    connection, copy_target, candidate, source_stage=stage
                )
                == receipt
            )
            await _assert_fact_storage_tamper(connection, copy_target, candidate, receipt, schema)
            await connection.execute(f'DROP TABLE pg_temp."{stage.table_name}"')
            await connection.execute(f'DROP TABLE pg_temp."{stage.contribution_table_name}"')
    finally:
        await connection.execute(f'DROP SCHEMA IF EXISTS "{copy_target.schema_name}" CASCADE')
        await connection.execute(f'DELETE FROM "{schema}".network_membership_candidate WHERE candidate_id=$1', identity)


async def test_initial_cms_reader_preserves_exact_independent_batch_contract(reviewed_cms_source):
    from process import registry_source_recipe_composition as composition
    from process.network_initial_cms_office_bindings import InitialCMSOfficeBindingError, _verify_batch

    fixture, _, _, _, coordinates, approved = reviewed_cms_source
    connection, schema, source_pin, _ = fixture
    recipe = RegistrySourceMembershipRecipe(source_pin, coordinates)
    async with connection.transaction(isolation="repeatable_read"):
        ordinary, _, _ = await composition._read_recipe_page(connection, recipe, approved, None, schema, 1000)
        independent = await read_fhir_membership_batch(
            connection,
            source_pin,
            registry_schema=schema,
            approved_source=approved,
            binding_coordinates=coordinates,
            approved_only=True,
        )
        assert ordinary == independent and ordinary.source_fact_bytes == b"[]"
        await _verify_batch(connection, ordinary, None, schema)
        captured, _, _ = await composition._read_recipe_page(
            connection, recipe, approved, None, schema, 1000, capture_facts=True
        )
        assert captured.input_bytes == ordinary.input_bytes and captured.source_fact_bytes != b"[]"
        with pytest.raises(InitialCMSOfficeBindingError, match="batch_differs"):
            await _verify_batch(connection, captured, None, schema)
        for revision in (1, 2):
            stage = await composition.stage_registry_source_recipes(
                connection,
                (recipe,),
                approved,
                request_id=uuid4(),
                control_schema=schema,
                contribution_revision=revision,
            )
            expected = ordinary.generation_id if revision == 2 else stage.contribution_receipts[0]["source_generation"]
            assert stage.source_generations == (expected,)
            assert stage.contribution_receipts[0]["source_generation"] != ordinary.generation_id
            await connection.execute(f'DROP TABLE pg_temp."{stage.table_name}"')
            await connection.execute(f'DROP TABLE pg_temp."{stage.contribution_table_name}"')
