# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native raw-source replay, reviewed corrections and complete-stage rollback."""

import hashlib
import json
from uuid import UUID, uuid4, uuid5

import asyncpg
import pytest

from process.network_address_projection import PinnedAddressSource
from process.network_approved_membership_source import pin_approved_membership_source
from process.network_fhir_membership_source import read_fhir_membership_batch
from process.network_membership_candidate_lifecycle import (
    admit_network_membership_batch,
    create_network_candidate,
    seal_network_candidate,
)
from process.network_membership_copy import MembershipCopyTarget
from process.network_membership_pipeline import prepare_and_publish_network_candidate
from process.network_serving_read import resolve_network_serving_manifest
from process.registry_candidate_composition import (
    RegistryCompositionAddressSources,
    compose_registry_membership_candidate,
)
from process.registry_source_recipe_composition import RegistrySourceCompositionError, stage_registry_source_recipes
from process.registry_source_recipe_store import RegistrySourceMembershipRecipe, canonical_registry_source_recipes
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
from tests.test_network_custom_address_source_postgres import _seed
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
from tests.test_network_legacy_membership_source_postgres import reviewed_aca_source as reviewed_aca_source
from tests.test_registry_approval_store_postgres import _approve, _command
from tests.test_registry_candidate_composition_postgres import _remove_candidates, _roles
from tests.test_registry_retained_site_adoption_postgres import _source_addresses

pytestmark = pytest.mark.asyncio


async def _publish_raw_recipe(fixture, reviewed_source, copy_targets):
    """Retain the actual reviewed office and its raw recipe in a serving manifest."""
    source_fixture, _, _, _, coordinates, approved = reviewed_source
    connection, schema, source_pin, sites = source_fixture
    _, source_table = await _source_addresses(fixture, 1, "npi")
    await connection.execute(f"UPDATE {source_table} SET entity_id='1000000491',npi=1000000491")
    await connection.execute(f'UPDATE "{fixture.source_schema}".npi SET npi=1000000491')
    identity = uuid4()
    copy_target = MembershipCopyTarget(
        *(str(uuid4()) for _ in range(3)), str(identity), "network_candidate_" + identity.hex
    )
    copy_targets.append(copy_target)
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
    await prepare_and_publish_network_candidate(
        connection, copy_target, fixture.base, **_roles(fixture), control_schema=schema
    )
    async with connection.transaction(isolation="repeatable_read", readonly=True):
        return await resolve_network_serving_manifest(connection, control_schema=schema)


@pytest.mark.parametrize("operation", ["rebind", "close"])
async def test_raw_recipe_publication_replays_reviewed_change_and_latest_manual(
    custom_db, reviewed_cms_source, operation
):
    """The retained raw edition follows a reviewed change and preserves later manual offices."""
    fixture = custom_db
    source_fixture, actor, networks, binding, coordinates, _ = reviewed_cms_source
    connection, schema, source_pin, sites = source_fixture
    copy_targets = []
    request_id = uuid4()
    try:
        original = await _publish_raw_recipe(fixture, reviewed_cms_source, copy_targets)
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
        copy_target, addresses = await compose_registry_membership_candidate(
            connection,
            request_id=request_id,
            approved_revision=approved.approved_revision,
            expected_head=original.generation_id,
            address_sources=RegistryCompositionAddressSources(
                PinnedAddressSource(original.schema_name, "entity_address_unified", original.manifest_sha256)
            ),
            writer_roles=_roles(fixture),
            source_manifest=original,
            control_schema=schema,
        )
        copy_targets.append(copy_target)
        await prepare_and_publish_network_candidate(
            connection, copy_target, addresses, **_roles(fixture), control_schema=schema
        )
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
        assert (
            await connection.fetchval(
                f'SELECT location_id FROM "{copy_target.schema_name}".provider_location_binding '
                "WHERE provider_system='npi' AND provider_id='1000000491'"
            )
            == source_site
        )
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
