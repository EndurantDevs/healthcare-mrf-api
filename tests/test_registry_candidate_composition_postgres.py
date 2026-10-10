# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Real approved custom composition, exact offices and durable native batch retry."""

import asyncio
import hashlib
import json
from types import SimpleNamespace
from uuid import UUID, uuid4, uuid5

import pytest
from sqlalchemy import MetaData

from db.models import NPIData
from process import registry_candidate_composition as composition
from process.network_address_projection import PinnedAddressSource
from process.network_approved_membership_source import pin_approved_membership_source
from process.network_legacy_membership_source import PinnedACAMembershipSource
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
from process.registry_initial_source_composition import (
    SUMMARY_KEY,
    RegistryInitialSourceError,
    validate_initial_source_office_receipt,
    verify_initial_source_office_receipt,
)
from process.registry_source_recipe_store import RegistrySourceMembershipRecipe
from tests.test_network_custom_address_source_postgres import _draft, _seed, custom_db
from tests.test_network_initial_source_office_bindings_postgres import (
    _archive,
    _cleanup_offices,
    _office_fixture,
    office_db,
)
from tests.test_network_legacy_membership_source_postgres import _aca_binding, _write_aca_bindings
from tests.test_network_serving_schema_postgres import serving_schema
from tests.test_registry_approval_store_postgres import _actor, _approve, _command, _create

pytestmark = pytest.mark.asyncio


def _roles(fixture):
    return {"owner_role": fixture.roles["owner"], "loader_roles": (), "reader_roles": (fixture.roles["reader"],)}


async def _remove_candidates(fixture, copy_targets, request_id):
    schema_names = {copy_target.schema_name for copy_target in copy_targets}
    owned_candidates = await fixture.connection.fetch(
        f'SELECT candidate_id,schema_name FROM "{fixture.control_schema}".network_membership_candidate WHERE dataset_id=$1',
        uuid5(request_id, "dataset"),
    )
    for candidate in owned_candidates:
        assert candidate["schema_name"] == "network_candidate_" + candidate["candidate_id"].hex
        schema_names.add(candidate["schema_name"])
    for schema_name in schema_names:
        await fixture.connection.execute(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE')
        assert await fixture.connection.fetchval("SELECT to_regnamespace($1)", schema_name) is None


async def _interrupt_after_source_copy(fixture, monkeypatch, composition_by_name, *, cancel=False):
    original_copy = composition.copy_approved_membership_batch

    async def fail_custom_copy(*args, **kwargs):
        if cancel:
            raise asyncio.CancelledError
        raise RuntimeError("synthetic interruption")

    monkeypatch.setattr(composition, "copy_approved_membership_batch", fail_custom_copy)
    with pytest.raises(asyncio.CancelledError if cancel else RuntimeError):
        await compose_registry_membership_candidate(fixture.connection, **composition_by_name)
    unfinished = await fixture.connection.fetchrow(
        f'SELECT state,accepted_rows,expected_rows FROM "{fixture.control_schema}".network_membership_candidate WHERE dataset_id=$1',
        uuid5(composition_by_name["request_id"], "dataset"),
    )
    assert tuple(unfinished) == ("open", 1, 2)
    monkeypatch.setattr(composition, "copy_approved_membership_batch", original_copy)


async def _imported_candidate(fixture, copy_targets):
    database_name = await fixture.connection.fetchval("SELECT quote_ident(current_database())")
    await fixture.connection.execute(f'GRANT CREATE ON DATABASE {database_name} TO "{fixture.roles["owner"]}"')
    network = await _draft(fixture, _create("network"), _actor())
    identity = uuid4()
    copy_target = MembershipCopyTarget(
        *(str(uuid4()) for _ in range(3)), str(identity), "network_candidate_" + identity.hex
    )
    copy_targets.append(copy_target)
    location_id = uuid4()
    membership_by_field = {
        "network_id": network["record_id"],
        "provider_system": "npi",
        "provider_id": "1000000004",
        "location_id": str(location_id),
        "evidence_id": "e" * 64,
    }
    input_bytes = json.dumps([membership_by_field]).encode()
    async with fixture.connection.transaction():
        await create_network_candidate(
            fixture.connection,
            copy_target,
            source_generations={"unified_address": fixture.base.generation_id, "aca": "retained-source-a"},
            approved_custom_revision=0,
            expected_head=0,
            expected_rows=1,
            control_schema=fixture.control_schema,
        )
        await fixture.connection.execute(f'''CREATE TABLE "{copy_target.schema_name}".provider_location_binding(
          provider_system text NOT NULL,provider_id text NOT NULL,location_id uuid NOT NULL,
          location_key varchar(64) NOT NULL,entity_type text NOT NULL,entity_id text NOT NULL,
          PRIMARY KEY(provider_system,provider_id,location_id))''')
        await fixture.connection.execute(
            f'INSERT INTO "{copy_target.schema_name}".provider_location_binding VALUES($1,$2,$3,$4,$1,$2)',
            "npi",
            "1000000004",
            location_id,
            "a" * 64,
        )
        await admit_network_membership_batch(
            fixture.connection,
            copy_target,
            batch_id=uuid4(),
            input_bytes=input_bytes,
            expected_input_sha256=hashlib.sha256(input_bytes).hexdigest(),
            control_schema=fixture.control_schema,
        )
        await seal_network_candidate(fixture.connection, copy_target, control_schema=fixture.control_schema)
    await prepare_and_publish_network_candidate(
        fixture.connection, copy_target, fixture.base, **_roles(fixture), control_schema=fixture.control_schema
    )
    async with fixture.connection.transaction(readonly=True):
        return await resolve_network_serving_manifest(fixture.connection, control_schema=fixture.control_schema)


async def test_approved_manual_composition_seals_without_publishing_and_replays(custom_db):
    fixture = custom_db
    seed = await _seed(fixture)
    request_id = uuid4()
    copy_targets = []
    async with fixture.connection.transaction(isolation="repeatable_read", readonly=True):
        pin = await pin_approved_membership_source(
            fixture.connection, approved_revision=seed.revision, control_schema=fixture.control_schema
        )
    fixture.composition_ids.append(uuid5(request_id, "address:" + pin.generation_id))
    try:
        composition_by_name = {
            "request_id": request_id,
            "approved_revision": seed.revision,
            "expected_head": 0,
            "address_sources": RegistryCompositionAddressSources(fixture.base),
            "writer_roles": _roles(fixture),
            "control_schema": fixture.control_schema,
        }
        copy_target, address_source = await compose_registry_membership_candidate(
            fixture.connection, **composition_by_name
        )
        copy_targets.append(copy_target)
        candidate = await fixture.connection.fetchrow(
            f'SELECT * FROM "{fixture.control_schema}".network_membership_candidate WHERE candidate_id=$1',
            UUID(copy_target.candidate_id),
        )
        assert candidate["state"] == "sealed" and candidate["accepted_rows"] == candidate["expected_rows"] == 1
        membership_records = await fixture.connection.fetch(
            f'SELECT * FROM "{copy_target.schema_name}".network_membership'
        )
        assert len(membership_records) == 1 and membership_records[0]["network_id"] == seed.network["record_id"]
        assert str(membership_records[0]["location_id"]) == seed.first["record_id"]
        assert membership_records[0]["evidence_id"].startswith("approved-custom:")
        assert seed.second["record_id"] not in json.dumps(
            [dict(member_record) for member_record in membership_records], default=str
        )
        assert (
            await fixture.connection.fetchval(
                f'SELECT generation_id FROM "{fixture.control_schema}".network_serving_control WHERE id=1'
            )
            is None
        )
        assert await compose_registry_membership_candidate(fixture.connection, **composition_by_name) == (
            copy_target,
            address_source,
        )
        assert (
            await fixture.connection.fetchval(
                f'SELECT count(*) FROM "{fixture.control_schema}".network_membership_batch WHERE candidate_id=$1',
                UUID(copy_target.candidate_id),
            )
            == 1
        )
    finally:
        await _remove_candidates(fixture, copy_targets, request_id)


@pytest.mark.parametrize("interrupt", [False, True])
async def test_retained_import_composes_latest_custom_and_cleans_source_pages(custom_db, monkeypatch, interrupt):
    fixture = custom_db
    copy_targets = []
    request_id = uuid4()
    try:
        source_manifest = await _imported_candidate(fixture, copy_targets)
        seed = await _seed(fixture)
        async with fixture.connection.transaction(isolation="repeatable_read", readonly=True):
            approved = await pin_approved_membership_source(
                fixture.connection, approved_revision=seed.revision, control_schema=fixture.control_schema
            )
        fixture.composition_ids.append(uuid5(request_id, "address:" + approved.generation_id))
        composition_by_name = {
            "request_id": request_id,
            "approved_revision": seed.revision,
            "expected_head": source_manifest.generation_id,
            "address_sources": RegistryCompositionAddressSources(
                PinnedAddressSource(
                    source_manifest.schema_name, "entity_address_unified", source_manifest.manifest_sha256
                )
            ),
            "writer_roles": _roles(fixture),
            "source_manifest": source_manifest,
            "control_schema": fixture.control_schema,
        }
        if interrupt:
            await _interrupt_after_source_copy(fixture, monkeypatch, composition_by_name)
        copy_target, addresses = await compose_registry_membership_candidate(fixture.connection, **composition_by_name)
        copy_targets.append(copy_target)
        members = await fixture.connection.fetch(
            f'SELECT * FROM "{copy_target.schema_name}".network_membership ORDER BY network_id'
        )
        assert len(members) == 2 and {membership_by_field["provider_system"] for membership_by_field in members} == {
            "npi",
            "manual",
        }
        assert str(members[1]["location_id"]) == seed.first["record_id"]
        assert members[0]["evidence_id"] == "e" * 64 and members[1]["evidence_id"].startswith("approved-custom:")
        assert not await fixture.connection.fetchval(
            "SELECT EXISTS(SELECT 1 FROM pg_class WHERE relnamespace=pg_my_temp_schema() AND relname LIKE 'registry_source_page_%')"
        )
        await prepare_and_publish_network_candidate(
            fixture.connection, copy_target, addresses, **_roles(fixture), control_schema=fixture.control_schema
        )
        projected = await fixture.connection.fetch(
            f'SELECT entity_type,canonical_network_ids FROM "{copy_target.schema_name}".entity_address_unified ORDER BY entity_type'
        )
        assert [member_record["entity_type"] for member_record in projected] == ["manual", "npi"]
        assert all(len(member_record["canonical_network_ids"]) == 1 for member_record in projected)
        async with fixture.connection.transaction(readonly=True):
            retained = await resolve_network_serving_manifest(
                fixture.connection, generation_id=source_manifest.generation_id, control_schema=fixture.control_schema
            )
        assert retained == source_manifest
    finally:
        await _remove_candidates(fixture, copy_targets, request_id)


@pytest.fixture
async def initial_composition(office_db, serving_schema):
    fixture = SimpleNamespace(
        connection=office_db.connection,
        control_schema=office_db.control,
        engine=serving_schema[2],
        source_schema=office_db.address_source.schema_name,
        base=office_db.address_source,
        roles={"owner": office_db.owner, "reader": office_db.reader},
        composition_ids=[],
        recipes=(RegistrySourceMembershipRecipe(office_db.source, office_db.batch.binding_coordinates),),
    )
    try:
        database_name = await fixture.connection.fetchval("SELECT quote_ident(current_database())")
        await fixture.connection.execute(f'GRANT CREATE ON DATABASE {database_name} TO "{office_db.owner}"')
        async with fixture.engine.begin() as setup:
            table = NPIData.__table__.to_metadata(MetaData(), schema=fixture.source_schema)
            await setup.run_sync(lambda sync: table.create(sync))
        await fixture.connection.execute(f'INSERT INTO "{fixture.source_schema}".npi(npi) VALUES(1000000491)')
        await fixture.connection.execute(f'ALTER TABLE "{fixture.source_schema}".npi OWNER TO "{office_db.owner}"')
        await fixture.connection.execute(f'GRANT SELECT ON "{fixture.source_schema}".npi TO "{office_db.reader}"')
        yield fixture
    finally:
        for identity in fixture.composition_ids:
            schema = "network_composition_" + identity.hex
            await fixture.connection.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
            assert await fixture.connection.fetchval("SELECT to_regnamespace($1)", schema) is None


async def _initial_arguments(fixture):
    seed = await _seed(fixture)
    async with fixture.connection.transaction(isolation="repeatable_read", readonly=True):
        approved = await pin_approved_membership_source(
            fixture.connection,
            approved_revision=seed.revision,
            control_schema=fixture.control_schema,
        )
    request_id = await fixture.connection.fetchval("SELECT gen_random_uuid()")
    assert isinstance(request_id, UUID) and type(request_id) is not UUID
    fixture.composition_ids.append(uuid5(request_id, "address:" + approved.generation_id))
    return seed, {
        "request_id": request_id,
        "approved_revision": seed.revision,
        "expected_head": 0,
        "address_sources": RegistryCompositionAddressSources(fixture.base, initial_source_recipes=fixture.recipes),
        "writer_roles": _roles(fixture),
        "control_schema": fixture.control_schema,
    }


async def _initial_candidate(fixture, target):
    return dict(
        await fixture.connection.fetchrow(
            f'SELECT * FROM "{fixture.control_schema}".network_membership_candidate WHERE candidate_id=$1',
            UUID(target.candidate_id),
        )
    )


async def test_actual_initial_aca_recipes_compose_manual_publish_and_replay(initial_composition):
    fixture = initial_composition
    seed, arguments = await _initial_arguments(fixture)
    copy_targets = []
    try:
        copy_target, addresses = await compose_registry_membership_candidate(fixture.connection, **arguments)
        copy_targets.append(copy_target)
        candidate = await _initial_candidate(fixture, copy_target)
        report = json.loads(candidate["validation_json"])
        summary = verify_initial_source_office_receipt(candidate)
        assert summary == report[SUMMARY_KEY]
        assert (summary["recipe_count"], summary["office_rows"], summary["page_count"]) == (1, 2, 1)
        assert summary["approved_revision"] == seed.revision
        assert report["registry_source_selection"]["mapped_rows"] == 2
        assert report["registry_source_selection"]["omitted_rows"] == 0
        assert candidate["state"] == "sealed" and candidate["accepted_rows"] == candidate["expected_rows"] == 3
        members = await fixture.connection.fetch(f'SELECT * FROM "{copy_target.schema_name}".network_membership')
        assert len(members) == 3 and sum(member["provider_system"] == "manual" for member in members) == 1
        assert await compose_registry_membership_candidate(fixture.connection, **arguments) == (copy_target, addresses)
        assert await _initial_candidate(fixture, copy_target) == candidate
        await prepare_and_publish_network_candidate(
            fixture.connection,
            copy_target,
            addresses,
            **_roles(fixture),
            control_schema=fixture.control_schema,
        )
        address_records = await fixture.connection.fetch(
            f'SELECT entity_type,second_line,canonical_network_ids FROM "{copy_target.schema_name}".entity_address_unified'
        )
        assert len(address_records) == 3 and all(
            len(address["canonical_network_ids"]) == 1 for address in address_records
        )
        assert {address["second_line"] for address in address_records if address["entity_type"] == "npi"} == {
            "Suite 2",
            "Suite 3",
        }
        assert not await fixture.connection.fetchval(
            "SELECT EXISTS(SELECT 1 FROM pg_class WHERE relnamespace=pg_my_temp_schema() "
            "AND (relname LIKE 'registry_source_page_%' OR relname LIKE 'initial_aca_offices_%'))"
        )
    finally:
        await _remove_candidates(fixture, copy_targets, arguments["request_id"])


@pytest.mark.parametrize("damage", ["office", "copy", "cancel"])
async def test_initial_aca_failure_rolls_back_complete_preparation(initial_composition, monkeypatch, damage):
    from process import registry_initial_source_composition as initial

    fixture = initial_composition
    _, arguments = await _initial_arguments(fixture)
    if damage == "office":
        await fixture.connection.execute(
            f"UPDATE \"{fixture.source_schema}\".entity_address_unified SET second_line='Suite 99' WHERE checksum=2"
        )
    else:
        original = initial.copy_initial_aca_office_bindings

        async def fail_after_copy(*args, **kwargs):
            await original(*args, **kwargs)
            if damage == "cancel":
                raise asyncio.CancelledError
            raise RuntimeError("synthetic office copy failure")

        monkeypatch.setattr(initial, "copy_initial_aca_office_bindings", fail_after_copy)
    try:
        with pytest.raises((ValueError, RuntimeError, asyncio.CancelledError)):
            await compose_registry_membership_candidate(fixture.connection, **arguments)
        assert not await fixture.connection.fetchval(
            f'SELECT EXISTS(SELECT 1 FROM "{fixture.control_schema}".network_membership_candidate WHERE dataset_id=$1)',
            uuid5(arguments["request_id"], "dataset"),
        )
        assert not await fixture.connection.fetchval(
            "SELECT EXISTS(SELECT 1 FROM pg_class WHERE relnamespace=pg_my_temp_schema() "
            "AND (relname LIKE 'registry_recipe_%' OR relname LIKE 'registry_source_page_%' OR relname LIKE 'initial_aca_offices_%'))"
        )
        assert (
            await fixture.connection.fetchval(
                f'SELECT generation_id FROM "{fixture.control_schema}".network_serving_control WHERE id=1'
            )
            is None
        )
    finally:
        await _remove_candidates(fixture, [], arguments["request_id"])


async def _close_initial_bindings(fixture, count):
    bindings = await fixture.connection.fetch(
        f'SELECT binding_id,network_id,revision FROM "{fixture.control_schema}".registry_network_binding '
        "ORDER BY source_key LIMIT $1",
        count,
    )
    actor = _actor()
    command_rows = [
        _aca_binding(
            fixture.recipes[0].binding_coordinates,
            binding["network_id"],
            index + 1,
            binding_id=str(binding["binding_id"]),
            operation="close",
            expected_revision=binding["revision"],
            expected_network_id=binding["network_id"],
        )
        for index, binding in enumerate(bindings)
    ]
    receipt = await _write_aca_bindings(fixture.connection, fixture.control_schema, actor, command_rows)
    await _approve(
        fixture.connection,
        fixture.control_schema,
        await _command(fixture.connection, fixture.control_schema, *receipt["records"]),
        actor,
    )


@pytest.mark.parametrize("closed_count", [1, 2])
async def test_initial_aca_omissions_are_explicit_and_preserve_manual(initial_composition, closed_count):
    fixture = initial_composition
    await _close_initial_bindings(fixture, closed_count)
    _, arguments = await _initial_arguments(fixture)
    copy_targets = []
    try:
        copy_target, _ = await compose_registry_membership_candidate(fixture.connection, **arguments)
        copy_targets.append(copy_target)
        candidate = await _initial_candidate(fixture, copy_target)
        summary = verify_initial_source_office_receipt(candidate)
        selection = json.loads(candidate["validation_json"])["registry_source_selection"]
        assert (selection["mapped_rows"], selection["omitted_rows"]) == (2 - closed_count, closed_count)
        assert summary["office_rows"] == 2 - closed_count and summary["page_count"] == 1
        assert candidate["accepted_rows"] == candidate["expected_rows"] == 3 - closed_count
        assert (
            await fixture.connection.fetchval(
                f"SELECT count(*) FROM \"{copy_target.schema_name}\".network_membership WHERE provider_system='manual'"
            )
            == 1
        )
    finally:
        await _remove_candidates(fixture, copy_targets, arguments["request_id"])


async def _publish_composition_target(fixture, copy_target, address_source):
    """Use the actual protected preparation and atomic publication pipeline."""
    await prepare_and_publish_network_candidate(
        fixture.connection,
        copy_target,
        address_source,
        **_roles(fixture),
        control_schema=fixture.control_schema,
    )


@pytest.fixture
async def replacement_aca_source(initial_composition):
    fixture = initial_composition
    fresh = _office_fixture((fixture.connection, fixture.control_schema, fixture.engine), 1)
    try:
        prepared, validation = await _archive(fresh)
        source_pin = PinnedACAMembershipSource(prepared, validation, **fresh.source_metadata_by_field)
        network_id = await fixture.connection.fetchval(
            f'''SELECT (record_json->>'network_id')::integer FROM "{fixture.control_schema}".registry_approved_record
            WHERE record_kind='network_binding' AND record_json->>'dataset_id'=$1 LIMIT 1''',
            fixture.recipes[0].binding_coordinates.dataset_id,
        )
        actor = _actor()
        binding = await _write_aca_bindings(
            fixture.connection, fixture.control_schema, actor, [_aca_binding(fresh.coordinates, network_id)]
        )
        await _approve(
            fixture.connection,
            fixture.control_schema,
            await _command(fixture.connection, fixture.control_schema, *binding["records"]),
            actor,
        )
        yield RegistrySourceMembershipRecipe(source_pin, fresh.coordinates)
    finally:
        await _cleanup_offices(fresh)


async def _published_initial_fixture(fixture, copy_targets, requests):
    seed, initial_arguments = await _initial_arguments(fixture)
    requests.append(initial_arguments["request_id"])
    target, addresses = await compose_registry_membership_candidate(fixture.connection, **initial_arguments)
    copy_targets.append(target)
    await _publish_composition_target(fixture, target, addresses)
    async with fixture.connection.transaction(isolation="repeatable_read", readonly=True):
        manifest = await resolve_network_serving_manifest(fixture.connection, control_schema=fixture.control_schema)
        approved = await pin_approved_membership_source(
            fixture.connection, approved_revision=seed.revision, control_schema=fixture.control_schema
        )
    return manifest, approved


@pytest.mark.parametrize("interruption", [None, "error", "cancel"])
async def test_replacement_closed_aca_dataset_preserves_manual_and_retained_generation(
    initial_composition, replacement_aca_source, monkeypatch, interruption
):
    """Replacement and durable retry preserve manual rows and the prior pinned snapshot."""
    fixture = initial_composition
    copy_targets, requests = [], []
    try:
        prior, approved = await _published_initial_fixture(fixture, copy_targets, requests)
        request = uuid4()
        requests.append(request)
        fixture.composition_ids.append(uuid5(request, "address:" + approved.generation_id))
        arguments_by_name = dict(
            request_id=request,
            approved_revision=approved.approved_revision,
            expected_head=prior.generation_id,
            writer_roles=_roles(fixture),
            source_manifest=prior,
            control_schema=fixture.control_schema,
            address_sources=RegistryCompositionAddressSources(
                fixture.base, replacement_source_recipes=(replacement_aca_source,)
            ),
        )
        if interruption:
            await _interrupt_after_source_copy(fixture, monkeypatch, arguments_by_name, cancel=interruption == "cancel")
        copy_target, addresses = await compose_registry_membership_candidate(fixture.connection, **arguments_by_name)
        copy_targets.append(copy_target)
        candidate = await _initial_candidate(fixture, copy_target)
        assert candidate["accepted_rows"] == candidate["expected_rows"] == 2
        assert verify_initial_source_office_receipt(candidate)["office_rows"] == 1
        assert (
            json.loads(candidate["source_recipes_json"])[0]["binding_coordinates"]["dataset_id"]
            == replacement_aca_source.binding_coordinates.dataset_id
        )
        replayed = await compose_registry_membership_candidate(fixture.connection, **arguments_by_name)
        assert replayed == (copy_target, addresses)
        assert (
            await fixture.connection.fetchval(
                f'SELECT generation_id FROM "{fixture.control_schema}".network_serving_control WHERE id=1'
            )
            == prior.generation_id
        )
        await _publish_composition_target(fixture, copy_target, addresses)
        members = await fixture.connection.fetch(f'SELECT * FROM "{copy_target.schema_name}".network_membership')
        assert len(members) == 2 and sum(member["provider_system"] == "manual" for member in members) == 1
        offices = await fixture.connection.fetch(
            f"SELECT second_line,canonical_network_ids FROM \"{copy_target.schema_name}\".entity_address_unified WHERE entity_type='npi' ORDER BY second_line"
        )
        assert [len(office["canonical_network_ids"]) for office in offices] == [1, 0]
        async with fixture.connection.transaction(isolation="repeatable_read", readonly=True):
            assert (
                await resolve_network_serving_manifest(
                    fixture.connection, generation_id=prior.generation_id, control_schema=fixture.control_schema
                )
                == prior
            )
        assert await fixture.connection.fetchval(f'SELECT count(*) FROM "{prior.schema_name}".network_membership') == 3
    finally:
        for request in requests:
            await _remove_candidates(fixture, copy_targets, request)


@pytest.mark.parametrize("damage", ["missing", "zero", "bool", "mismatch", "mixed"])
async def test_replacement_recipes_require_exact_retained_head(initial_composition, replacement_aca_source, damage):
    fixture = initial_composition
    copy_targets, requests = [], []
    try:
        prior, approved = await _published_initial_fixture(fixture, copy_targets, requests)
        address_sources = RegistryCompositionAddressSources(
            fixture.base, replacement_source_recipes=(replacement_aca_source,)
        )
        request = uuid4()
        if damage == "mixed":
            with pytest.raises(composition.RegistryCompositionError, match="initial_sources_invalid"):
                RegistryCompositionAddressSources(
                    fixture.base,
                    initial_source_recipes=fixture.recipes,
                    replacement_source_recipes=(replacement_aca_source,),
                )
        else:
            expected_head = {"zero": 0, "bool": True, "mismatch": prior.generation_id + 1}.get(
                damage, prior.generation_id
            )
            with pytest.raises(composition.RegistryCompositionError, match="replacement_sources_invalid"):
                await compose_registry_membership_candidate(
                    fixture.connection,
                    request_id=request,
                    approved_revision=approved.approved_revision,
                    expected_head=expected_head,
                    source_manifest=None if damage == "missing" else prior,
                    address_sources=address_sources,
                    writer_roles=_roles(fixture),
                    control_schema=fixture.control_schema,
                )
        assert not await fixture.connection.fetchval(
            f'SELECT EXISTS(SELECT 1 FROM "{fixture.control_schema}".network_membership_candidate WHERE dataset_id=$1)',
            uuid5(request, "dataset"),
        )
        assert (
            await fixture.connection.fetchval(
                f'SELECT generation_id FROM "{fixture.control_schema}".network_serving_control WHERE id=1'
            )
            == prior.generation_id
        )
    finally:
        for initial_request in requests:
            await _remove_candidates(fixture, copy_targets, initial_request)


async def test_initial_admitted_recipe_recomposes_from_retained_manifest(initial_composition):
    """A retained initial recipe recomposes without reapplying bootstrap authority."""
    fixture = initial_composition
    seed, initial_arguments = await _initial_arguments(fixture)
    next_request = uuid4()
    copy_targets = []
    try:
        first_target, addresses = await compose_registry_membership_candidate(fixture.connection, **initial_arguments)
        copy_targets.append(first_target)
        await _publish_composition_target(fixture, first_target, addresses)
        async with fixture.connection.transaction(isolation="repeatable_read", readonly=True):
            manifest = await resolve_network_serving_manifest(fixture.connection, control_schema=fixture.control_schema)
            approved = await pin_approved_membership_source(
                fixture.connection,
                approved_revision=seed.revision,
                control_schema=fixture.control_schema,
            )
        fixture.composition_ids.append(uuid5(next_request, "address:" + approved.generation_id))
        second_target, second_addresses = await compose_registry_membership_candidate(
            fixture.connection,
            request_id=next_request,
            approved_revision=seed.revision,
            expected_head=manifest.generation_id,
            writer_roles=_roles(fixture),
            source_manifest=manifest,
            address_sources=RegistryCompositionAddressSources(
                PinnedAddressSource(manifest.schema_name, "entity_address_unified", manifest.manifest_sha256),
            ),
            control_schema=fixture.control_schema,
        )
        copy_targets.append(second_target)
        candidate = await _initial_candidate(fixture, second_target)
        assert verify_initial_source_office_receipt(candidate) is None
        assert (
            candidate["source_recipes_json"] == (await _initial_candidate(fixture, first_target))["source_recipes_json"]
        )
        assert candidate["accepted_rows"] == candidate["expected_rows"] == 3
        await _publish_composition_target(fixture, second_target, second_addresses)
        async with fixture.connection.transaction(isolation="repeatable_read", readonly=True):
            assert (
                await resolve_network_serving_manifest(
                    fixture.connection,
                    generation_id=manifest.generation_id,
                    control_schema=fixture.control_schema,
                )
                == manifest
            )
    finally:
        await _remove_candidates(fixture, copy_targets, initial_arguments["request_id"])
        await _remove_candidates(fixture, [], next_request)


@pytest.mark.parametrize("expected_head", [1, True, False, -1])
async def test_initial_sources_require_first_head(initial_composition, expected_head):
    _, arguments = await _initial_arguments(initial_composition)
    with pytest.raises(composition.RegistryCompositionError, match="initial_sources_invalid"):
        await compose_registry_membership_candidate(
            initial_composition.connection,
            **(arguments | {"expected_head": expected_head}),
        )


@pytest.mark.parametrize("damage", ["extra", "bool", "overflow", "digest", "missing", "address", "recipe"])
async def test_initial_office_receipt_rejects_tamper(initial_composition, damage):
    fixture = initial_composition
    _, arguments = await _initial_arguments(fixture)
    targets = []
    try:
        target, _ = await compose_registry_membership_candidate(fixture.connection, **arguments)
        targets.append(target)
        candidate = await _initial_candidate(fixture, target)
        report = json.loads(candidate["validation_json"])
        summary = report[SUMMARY_KEY]
        changes_by_damage = {
            "extra": {"extra": 0},
            "bool": {"office_rows": True},
            "overflow": {"office_rows": 2**63 - 1},
            "digest": {"correspondence_sha256": "f" * 64},
            "address": {"address_generation_sha256": "e" * 64},
            "recipe": {"recipe_sha256": "d" * 64},
        }
        if damage == "missing":
            del report[SUMMARY_KEY]
        else:
            summary.update(changes_by_damage[damage])
        candidate["validation_json"] = report
        with pytest.raises(RegistryInitialSourceError, match="receipt_invalid"):
            verify_initial_source_office_receipt(candidate)
    finally:
        await _remove_candidates(fixture, targets, arguments["request_id"])


@pytest.mark.parametrize("recipes", [[], None, False, (object(),)])
async def test_initial_recipe_dto_rejects_noncanonical_shape(custom_db, recipes):
    with pytest.raises(ValueError):
        RegistryCompositionAddressSources(custom_db.base, initial_source_recipes=recipes)


async def test_initial_receipt_legacy_shape_and_strict_json_bounds():
    assert verify_initial_source_office_receipt({"source_generations": {}, "validation_json": None}) is None
    receipt_by_field = {
        "component": SUMMARY_KEY,
        "revision": 1,
        "recipe_sha256": "a" * 64,
        "approved_revision": 0,
        "address_generation_sha256": "b" * 64,
        "recipe_count": 1,
        "office_rows": 0,
        "page_count": 0,
        "correspondence_sha256": "c" * 64,
    }
    raw = json.dumps(receipt_by_field)
    assert validate_initial_source_office_receipt(raw + " " * (4096 - len(raw))) == receipt_by_field
    for invalid in (raw + " " * (4097 - len(raw)), raw[:-1] + ',"revision":1}', []):
        with pytest.raises(RegistryInitialSourceError, match="receipt_invalid"):
            validate_initial_source_office_receipt(invalid)


async def _rollback_source_versions(fixture, replacement, copy_targets, requests):
    seed, initial = await _initial_arguments(fixture)
    requests.append(initial["request_id"])
    first_target, first_addresses = await compose_registry_membership_candidate(fixture.connection, **initial)
    copy_targets.append(first_target)
    await _publish_composition_target(fixture, first_target, first_addresses)
    async with fixture.connection.transaction(isolation="repeatable_read", readonly=True):
        first = await resolve_network_serving_manifest(fixture.connection, control_schema=fixture.control_schema)
    replacement_request = uuid4()
    requests.append(replacement_request)
    async with fixture.connection.transaction(isolation="repeatable_read", readonly=True):
        approved = await pin_approved_membership_source(
            fixture.connection, approved_revision=seed.revision, control_schema=fixture.control_schema
        )
    fixture.composition_ids.append(uuid5(replacement_request, "address:" + approved.generation_id))
    copy_target, addresses = await compose_registry_membership_candidate(
        fixture.connection,
        request_id=replacement_request,
        approved_revision=approved.approved_revision,
        expected_head=first.generation_id,
        source_manifest=first,
        address_sources=RegistryCompositionAddressSources(fixture.base, replacement_source_recipes=(replacement,)),
        writer_roles=_roles(fixture),
        control_schema=fixture.control_schema,
    )
    copy_targets.append(copy_target)
    await _publish_composition_target(fixture, copy_target, addresses)
    async with fixture.connection.transaction(isolation="repeatable_read", readonly=True):
        second = await resolve_network_serving_manifest(fixture.connection, control_schema=fixture.control_schema)
    return seed, first, second


async def _approve_rollback_manual_office(fixture, seed):
    from dataclasses import replace

    original = seed.membership_command.fields["memberships_json"][0]
    command = replace(
        seed.membership_command,
        operation="correct",
        expected_revision=seed.membership["revision"],
        fields={"memberships_json": [{**original, "location_id": seed.second["record_id"]}]},
        idempotency_key=uuid4().hex,
    )
    corrected = await _draft(fixture, command, seed.actor)
    approved = await _approve(
        fixture.connection,
        fixture.control_schema,
        await _command(fixture.connection, fixture.control_schema, corrected),
        seed.actor,
    )
    pending = replace(
        command,
        expected_revision=corrected["revision"],
        fields=seed.membership_command.fields,
        idempotency_key=uuid4().hex,
    )
    await _draft(fixture, pending, seed.actor)
    return approved["approved_revision"]


@pytest.fixture
async def rollback_composition(initial_composition, replacement_aca_source):
    fixture = initial_composition
    copy_targets, requests = [], []
    try:
        seed, first, second = await _rollback_source_versions(fixture, replacement_aca_source, copy_targets, requests)
        revision = await _approve_rollback_manual_office(fixture, seed)
        request_id = uuid4()
        requests.append(request_id)
        async with fixture.connection.transaction(isolation="repeatable_read", readonly=True):
            approved = await pin_approved_membership_source(
                fixture.connection, approved_revision=revision, control_schema=fixture.control_schema
            )
        fixture.composition_ids.append(uuid5(request_id, "address:" + approved.generation_id))
        arguments_by_name = dict(
            request_id=request_id,
            approved_revision=revision,
            expected_head=second.generation_id,
            source_manifest=first,
            address_sources=RegistryCompositionAddressSources(
                PinnedAddressSource(first.schema_name, "entity_address_unified", first.manifest_sha256)
            ),
            writer_roles=_roles(fixture),
            control_schema=fixture.control_schema,
        )
        yield SimpleNamespace(
            fixture=fixture,
            seed=seed,
            first=first,
            second=second,
            revision=revision,
            arguments_by_name=arguments_by_name,
            copy_targets=copy_targets,
        )
    finally:
        for request_id in requests:
            await _remove_candidates(fixture, copy_targets, request_id)


async def _rollback_members(connection, manifest):
    return [
        tuple(row)
        for row in await connection.fetch(
            f'SELECT provider_system,provider_id,location_id,evidence_id,network_id FROM "{manifest.schema_name}".network_membership '
            "ORDER BY provider_system,provider_id,location_id,evidence_id"
        )
    ]


async def _rollback_reader(fixture):
    import os

    import asyncpg

    await fixture.connection.execute(f'GRANT USAGE ON SCHEMA "{fixture.control_schema}" TO "{fixture.roles["reader"]}"')
    await fixture.connection.execute(
        f'GRANT SELECT ON ALL TABLES IN SCHEMA "{fixture.control_schema}" TO "{fixture.roles["reader"]}"'
    )
    return await asyncpg.connect(
        os.environ["NETWORK_REGISTRY_TEST_DSN"].replace("postgresql+asyncpg://", "postgresql://")
    )


async def _assert_rollback_source_and_manual(history, current):
    fixture = history.fixture
    members = await _rollback_members(fixture.connection, current)
    manual_members = [member for member in members if member[0] == "manual"]
    assert len(manual_members) == 1 and str(manual_members[0][2]) == history.seed.second["record_id"]
    assert manual_members[0][3].startswith(f"approved-custom:{history.revision}:")
    old_members = await _rollback_members(fixture.connection, history.first)
    # Native evidence is regenerated against the latest approved binding pin.
    assert [member[:3] + member[4:] for member in members if member[0] == "npi"] == [
        member[:3] + member[4:] for member in old_members if member[0] == "npi"
    ]
    assert current.source_generations["retained_network_source"] == history.first.manifest_sha256
    assert len(await _rollback_members(fixture.connection, history.second)) == 2
    assert len(old_members) == 3
    assert (
        await resolve_network_serving_manifest(
            fixture.connection, generation_id=history.first.generation_id, control_schema=fixture.control_schema
        )
        == history.first
    )
    office_arrays = await fixture.connection.fetch(
        f'SELECT second_line,cardinality(canonical_network_ids) FROM "{current.schema_name}".entity_address_unified '
        "WHERE entity_type='npi' ORDER BY second_line"
    )
    assert [tuple(office) for office in office_arrays] == [("Suite 2", 1), ("Suite 3", 1)]


async def test_selected_old_aca_source_keeps_latest_manual_and_pinned_readers(rollback_composition):
    history = rollback_composition
    fixture = history.fixture
    reader = await _rollback_reader(fixture)
    try:
        async with reader.transaction(isolation="repeatable_read", readonly=True):
            await reader.execute(f'SET LOCAL ROLE "{fixture.roles["reader"]}"')
            pinned = await resolve_network_serving_manifest(reader, control_schema=fixture.control_schema)
            before = await _rollback_members(reader, pinned)
            old_source_before = await _rollback_members(reader, history.first)
            copy_target, addresses = await compose_registry_membership_candidate(
                fixture.connection, **history.arguments_by_name
            )
            history.copy_targets.append(copy_target)
            candidate = await _initial_candidate(fixture, copy_target)
            assert candidate["accepted_rows"] == candidate["expected_rows"] == 3
            assert candidate["approved_custom_revision"] == history.revision > history.first.approved_custom_revision
            assert (
                candidate["source_recipes_json"]
                == (await _initial_candidate(fixture, history.copy_targets[0]))["source_recipes_json"]
            )
            await _publish_composition_target(fixture, copy_target, addresses)
            assert await resolve_network_serving_manifest(reader, control_schema=fixture.control_schema) == pinned
            assert await _rollback_members(reader, pinned) == before
            assert await _rollback_members(reader, history.first) == old_source_before
        async with fixture.connection.transaction(isolation="repeatable_read", readonly=True):
            current = await resolve_network_serving_manifest(fixture.connection, control_schema=fixture.control_schema)
            assert current.generation_id > history.second.generation_id > history.first.generation_id
            assert current.approved_custom_revision == history.revision
            await _assert_rollback_source_and_manual(history, current)
        assert await compose_registry_membership_candidate(fixture.connection, **history.arguments_by_name) == (
            copy_target,
            addresses,
        )
    finally:
        await reader.close()


@pytest.mark.parametrize("cancel", [False, True])
async def test_source_rollback_interruption_resumes_exact_batches(rollback_composition, monkeypatch, cancel):
    history = rollback_composition
    fixture = history.fixture
    original = composition.copy_approved_membership_batch

    async def fail_custom_copy(*arguments, **options):
        raise asyncio.CancelledError if cancel else RuntimeError("synthetic interruption")

    monkeypatch.setattr(composition, "copy_approved_membership_batch", fail_custom_copy)
    with pytest.raises(asyncio.CancelledError if cancel else RuntimeError):
        await compose_registry_membership_candidate(fixture.connection, **history.arguments_by_name)
    unfinished = await fixture.connection.fetchrow(
        f'SELECT state,accepted_rows,expected_rows FROM "{fixture.control_schema}".network_membership_candidate WHERE dataset_id=$1',
        uuid5(history.arguments_by_name["request_id"], "dataset"),
    )
    assert tuple(unfinished) == ("open", 2, 3)
    assert not await fixture.connection.fetchval(
        "SELECT EXISTS(SELECT 1 FROM pg_class WHERE relnamespace=pg_my_temp_schema() "
        "AND relname LIKE 'registry_source_page_%')"
    )
    assert (
        await fixture.connection.fetchval(
            f'SELECT generation_id FROM "{fixture.control_schema}".network_serving_control WHERE id=1'
        )
        == history.second.generation_id
    )
    monkeypatch.setattr(composition, "copy_approved_membership_batch", original)
    copy_target, addresses = await compose_registry_membership_candidate(
        fixture.connection, **history.arguments_by_name
    )
    history.copy_targets.append(copy_target)
    assert await compose_registry_membership_candidate(fixture.connection, **history.arguments_by_name) == (
        copy_target,
        addresses,
    )
    await _publish_composition_target(fixture, copy_target, addresses)
    assert (
        await fixture.connection.fetchval(f'SELECT count(*) FROM "{copy_target.schema_name}".network_membership') == 3
    )


@pytest.mark.parametrize("drift", ["head", "approval"])
async def test_source_rollback_rechecks_publication_head_and_latest_approval(rollback_composition, drift):
    from process.network_membership_publication import NetworkPublicationError

    history = rollback_composition
    fixture = history.fixture
    arguments_by_name = dict(history.arguments_by_name)
    if drift == "head":
        arguments_by_name["expected_head"] = history.first.generation_id
    copy_target, addresses = await compose_registry_membership_candidate(fixture.connection, **arguments_by_name)
    history.copy_targets.append(copy_target)
    if drift == "approval":
        draft = await _draft(fixture, _create(), history.seed.actor)
        await _approve(
            fixture.connection,
            fixture.control_schema,
            await _command(fixture.connection, fixture.control_schema, draft),
            history.seed.actor,
        )
    expected_error = "Expected serving head changed" if drift == "head" else "Approved custom revision changed"
    with pytest.raises(NetworkPublicationError, match=expected_error):
        await _publish_composition_target(fixture, copy_target, addresses)
    assert (
        await fixture.connection.fetchval(
            f'SELECT generation_id FROM "{fixture.control_schema}".network_serving_control WHERE id=1'
        )
        == history.second.generation_id
    )
    assert not await fixture.connection.fetchval(
        f'SELECT EXISTS(SELECT 1 FROM "{fixture.control_schema}".network_serving_manifest WHERE candidate_id=$1)',
        UUID(copy_target.candidate_id),
    )
