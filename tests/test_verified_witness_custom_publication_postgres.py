# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Genuine retained Source v2 plus native manual commands; no NPI authority is manufactured."""

import json
import re
from dataclasses import replace
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest
from sqlalchemy import text

from api.network_address_scope import network_address_read_scope
from api.network_compact_address import compact_address_native_sql
from api.network_provider_only_read import read_network_provider_only_page
from process.network_cms_registry_address_capture import native_driver
from process.network_compact_custom_preparation import compact_custom_receipt
from process.network_fhir_membership_source import FHIR_SOURCE_WITNESS_TRANSFORM_V2
from process.network_serving_read import resolve_network_serving_manifest
from process.registry_initial_source_composition import verify_initial_source_office_receipt
from process.registry_membership_refresh_authority import COMMIT_AUTHORITY_PATH
from process.registry_membership_refresh_queue import validated_membership_refresh_request_status
from process.registry_record_store import RegistryRecordCommand, apply_registry_record_command
from process.registry_source_contribution_store import verify_registry_source_contributions
from process.registry_source_inventory_contract import source_inventory_root
from process.registry_source_inventory_reference import verify_source_recipe_metadata
from process.registry_source_inventory_store import source_inventory_entry
from process.registry_source_recipe_store import resolve_retained_registry_source_recipes
from process.registry_source_retention import verify_registry_source_retention
from tests import generic_source_successor_postgres_support as source
from tests import test_network_custom_address_source_postgres as custom
from tests.test_network_core_layer_provider_postgres import _assert_inventory_migration
from tests.test_network_membership_draft_store_postgres import _command as membership_command
from tests.test_registry_approval_store_postgres import _approve, _command
from tests.test_registry_generic_source_successor_postgres import _heap_oids
from tests.test_registry_membership_refresh_execution_postgres import _counts, _storage_rows

pytestmark = pytest.mark.asyncio


async def _snapshot(fixture, connection, generation=None):
    manifest = await resolve_network_serving_manifest(
        connection, generation_id=generation, control_schema=fixture.control_schema
    )
    relation, arguments = compact_address_native_sql(
        manifest.compact_layout, first_parameter=1, network_ids=fixture.reader_network_ids
    )
    addresses = await connection.fetch(
        f"SELECT location_key,entity_type,entity_id,npi,row_origin,second_line,canonical_network_ids FROM {relation} "
        "ORDER BY entity_type,entity_id,location_key",
        *arguments,
    )
    with network_address_read_scope(manifest, (fixture.network_id,)) as scope:
        provider_only = await read_network_provider_only_page(connection, scope, control_schema=fixture.control_schema)
    return (
        manifest,
        [dict(address) for address in addresses],
        tuple(provider["provider_id"] for provider in provider_only["providers"]),
    )


async def _candidate(fixture, manifest):
    return dict(
        await fixture.connection.fetchrow(
            f'SELECT * FROM "{fixture.control_schema}".network_membership_candidate WHERE candidate_id=$1',
            UUID(manifest.candidate_id),
        )
    )


async def _metadata(fixture, manifest, custom_pairs):
    candidate = await _candidate(fixture, manifest)
    reference = verify_source_recipe_metadata(candidate).inventory
    assert reference is not None
    assert (reference.root.entry_count, reference.root.contribution_count, reference.root.derivation_count) == (1, 1, 1)
    inventory_records = await fixture.connection.fetch(
        f'SELECT * FROM "{manifest.schema_name}".network_source_inventory ORDER BY ordinal'
    )
    entries = tuple(source_inventory_entry(inventory_record) for inventory_record in inventory_records)
    assert source_inventory_root(entries) == reference.root
    descriptor = json.loads(entries[0].contribution_json)
    derivation = json.loads(entries[0].office_derivation_json)
    assert descriptor["descriptor_version"] == derivation["derivation_version"] == 2
    assert descriptor["transform_version"] == FHIR_SOURCE_WITNESS_TRANSFORM_V2
    assert (
        descriptor["fact_rows"],
        descriptor["membership_rows"],
        descriptor["raw_source_rows"],
        descriptor["provider_only_rows"],
    ) == (4, 1, 2, 1)
    assert descriptor["unresolved_rows"] == descriptor["unresolved_raw_source_rows"] == 0
    assert verify_registry_source_contributions(candidate)["revision"] == 4
    office = verify_initial_source_office_receipt(candidate)
    assert office["revision"] == 5 and office["verified_derivation_count"] == 1
    assert derivation["input_membership_rows"] == derivation["accepted_membership_rows"] == 1
    assert derivation["approved_revision"] == manifest.approved_custom_revision
    generations = candidate["source_generations"]
    generations = json.loads(generations) if type(generations) is str else generations
    assert derivation["approved_generation_sha256"] == generations["custom_membership"]
    custom_receipt = compact_custom_receipt(candidate)
    assert custom_receipt is not None and custom_receipt.custom_pair_count == custom_pairs
    assert custom_receipt.npi_source is None and custom_receipt.npi_identity is None
    assert custom_receipt.approved_revision == manifest.approved_custom_revision
    return descriptor, derivation, verify_registry_source_retention(candidate)


async def _publish_refresh(fixture, custom_pairs):
    inputs = await fixture.bundle.inputs_service()
    queued = await fixture.enqueue(fixture.command(inputs))
    outcome = await fixture.consume(queued["request_id"])
    assert outcome["state"] == "completed", outcome
    async with fixture.api.transaction(isolation="repeatable_read", readonly=True):
        snapshot = await _snapshot(fixture, fixture.api)
    descriptor, derivation, retention = await _metadata(fixture, snapshot[0], custom_pairs)
    return SimpleNamespace(
        inputs=inputs,
        queued=queued,
        snapshot=snapshot,
        manifest=snapshot[0],
        descriptor=descriptor,
        derivation=derivation,
        retention=retention,
    )


def _assert_manual(snapshot, manual, present):
    manual_offices = [
        office
        for office in snapshot[1]
        if office["entity_type"] == "manual" and office["entity_id"] == manual.provider["record_id"]
    ]
    assert len(manual_offices) == int(present)
    if present:
        assert manual_offices[0]["npi"] is None and manual_offices[0]["row_origin"] == "network_custom"
        assert manual_offices[0]["canonical_network_ids"] == [manual.network["record_id"]]
        assert manual_offices[0]["second_line"] == "Suite 2"


async def _assert_original_anchor(fixture, published, owner):
    descriptor = published.descriptor
    assert descriptor["fact_candidate_id"] == owner.manifest.candidate_id
    assert descriptor["capture_sha256"] == owner.descriptor["capture_sha256"]
    assert descriptor["fact_sha256"] == owner.descriptor["fact_sha256"]
    assert descriptor["source_generation"] == owner.descriptor["source_generation"]
    assert (
        await fixture.connection.fetchval(
            f'SELECT count(*) FROM "{published.manifest.schema_name}".network_source_contribution'
        )
        == 0
    )


async def _approve_record(fixture, record, actor):
    return await _approve(
        fixture.connection,
        fixture.control_schema,
        await _command(fixture.connection, fixture.control_schema, record),
        actor,
    )


async def _approve_source_remove(fixture, approval_view, manifest, actor):
    """Adopt the exact served Source office through the real compact retained-site resolver before removal."""
    member = await fixture.connection.fetchrow(
        f'SELECT * FROM "{manifest.schema_name}".network_membership_source WHERE network_id=$1',
        fixture.network_id,
    )
    assert member is not None and member["provider_system"] == "npi"
    addresses, arguments = compact_address_native_sql(
        manifest.compact_layout, first_parameter=4, visible_membership_sql="TRUE"
    )
    binding = await fixture.connection.fetchrow(
        f'''SELECT binding.*,encode(sha256(convert_to(to_jsonb(address)::text,'UTF8')),'hex') AS address_hash
        FROM "{manifest.schema_name}".provider_location_binding binding JOIN {addresses} address
        ON (address.location_key,address.entity_type,address.entity_id)=(binding.location_key,binding.entity_type,binding.entity_id)
        WHERE (binding.provider_system,binding.provider_id,binding.location_id)=($1,$2,$3)''',
        member["provider_system"],
        member["provider_id"],
        member["location_id"],
        *arguments,
    )
    fields_by_name = dict(
        source_generation=manifest.generation_id,
        provider_system=member["provider_system"],
        provider_id=member["provider_id"],
        location_id=str(member["location_id"]),
        location_key=binding["location_key"],
        address_row_sha256=binding["address_hash"],
    )
    command = RegistryRecordCommand(
        "site_binding", uuid4(), "create", 0, fields_by_name, "Review exact retained Source office", uuid4().hex
    )
    async with fixture.edit_sessions() as session, session.begin():
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
        assert await session.scalar(text("SELECT current_user::text")) == fixture.roles_by_kind["api"]
        adopted = await apply_registry_record_command(
            session, command, actor, schema=fixture.control_schema, source_schema=approval_view.source_schema
        )
    await _approve_record(fixture, adopted, actor)
    removal_by_field = {
        key: (str(member_value) if key == "location_id" else member_value) for key, member_value in dict(member).items()
    }
    removal_by_field["operation"] = "remove"
    draft = await custom._draft(approval_view, membership_command(fixture.network_id, [removal_by_field]), actor)
    await _approve_record(fixture, draft, actor)
    return removal_by_field


async def _clear_manual(fixture, approval_view, manual):
    command = replace(
        manual.membership_command,
        operation="correct",
        expected_revision=manual.membership["revision"],
        fields={"memberships_json": []},
        idempotency_key=uuid4().hex,
    )
    draft = await custom._draft(approval_view, command, manual.actor)
    await _approve_record(fixture, draft, manual.actor)


async def _assert_source_suppressed(fixture, published, removed, additions):
    retention = published.retention
    assert (
        retention["source_rows"],
        retention["add_rows"],
        retention["remove_rows"],
        retention["suppressed_source_rows"],
        retention["effective_rows"],
    ) == (1, additions, 1, 1, additions)
    actual = await fixture.connection.fetch(
        f'SELECT * FROM "{published.manifest.schema_name}".network_membership WHERE network_id=$1',
        fixture.network_id,
    )
    assert actual == []
    retained = await fixture.connection.fetch(
        f'SELECT * FROM "{published.manifest.schema_name}".network_membership_source WHERE network_id=$1',
        fixture.network_id,
    )
    assert len(retained) == 1 and str(retained[0]["location_id"]) == removed["location_id"]
    assert published.descriptor["membership_rows"] == published.derivation["accepted_membership_rows"] == 1


async def _assert_replay(fixture, published):
    """Completed command replay preserves request MVCC, every physical heap/OID and commit count."""
    request_sql = f'SELECT request.*,xmin::text AS row_xmin,ctid::text AS row_ctid FROM "{fixture.control_schema}".registry_publication_request request WHERE request_id=$1'
    request_id = UUID(published.queued["request_id"])
    before_request = await fixture.connection.fetchrow(request_sql, request_id)
    before_counts = await _counts(fixture)
    before_storage = await _storage_rows(fixture, published.manifest.schema_name)
    before_oids = await _heap_oids(fixture, published.manifest)
    commits = fixture.calls.count(COMMIT_AUTHORITY_PATH)
    cas = list(fixture.cas_checks)
    command = fixture.command(published.inputs)
    assert validated_membership_refresh_request_status(before_request) == command
    assert await fixture.enqueue(command) == {**published.queued, "state": "completed", "replayed": True}
    assert await fixture.consume(published.queued["request_id"]) is None
    assert await fixture.connection.fetchrow(request_sql, request_id) == before_request
    assert await _counts(fixture) == before_counts
    assert await _storage_rows(fixture, published.manifest.schema_name) == before_storage
    assert await _heap_oids(fixture, published.manifest) == before_oids
    assert fixture.calls.count(COMMIT_AUTHORITY_PATH) == commits and fixture.cas_checks == cas


async def _successor_and_custom_changes(fixture, source_fixture, approval_view, manual, added):
    original_storage = await _storage_rows(fixture, added.manifest.schema_name)
    async with fixture.outer() as old_session, old_session.begin():
        await old_session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
        old_reader = await native_driver(old_session)
        assert await _snapshot(fixture, old_reader) == added.snapshot
        successor = await source.publish_edition(source_fixture, 2)
        current = await _publish_refresh(fixture, 1)
        assert len(current.snapshot[1]) == 2
        _assert_manual(current.snapshot, manual, True)
        assert current.snapshot[2] == ("1000000491",) and current.descriptor["fact_candidate_id"] is None
        assert current.manifest.compact_layout.base.table_oid == added.manifest.compact_layout.base.table_oid
        async with fixture.api.transaction(isolation="repeatable_read", readonly=True):
            recipes = await resolve_retained_registry_source_recipes(
                fixture.api, current.manifest, control_schema=fixture.control_schema
            )
        assert recipes[0].source_pin.retained_capture == source_fixture.captures[0]
        assert recipes[0].source_pin.retained_successor.retained_capture == successor
        removed = await _approve_source_remove(fixture, approval_view, current.manifest, manual.actor)
        suppressed = await _publish_refresh(fixture, 1)
        assert len(suppressed.snapshot[1]) == 1
        await _assert_original_anchor(fixture, suppressed, current)
        await _assert_source_suppressed(fixture, suppressed, removed, 1)
        _assert_manual(suppressed.snapshot, manual, True)
        await _clear_manual(fixture, approval_view, manual)
        cleared = await _publish_refresh(fixture, 0)
        assert len(cleared.snapshot[1]) == 0
        await _assert_original_anchor(fixture, cleared, current)
        await _assert_source_suppressed(fixture, cleared, removed, 0)
        _assert_manual(cleared.snapshot, manual, False)
        assert cleared.manifest.compact_layout.custom is None
        assert await _snapshot(fixture, old_reader) == added.snapshot
        assert await _snapshot(fixture, old_reader, added.manifest.generation_id) == added.snapshot
        await _assert_replay(fixture, cleared)
    assert await _storage_rows(fixture, added.manifest.schema_name) == original_storage
    return current, suppressed, cleared


async def test_source_v2_manual_commands_preserve_custody_and_replay(monkeypatch, record_property):
    async with source.native_generic_source(monkeypatch) as source_fixture:
        assert re.fullmatch(r"hc_cms_admission_test_[0-9a-f]{32}", source_fixture.component.admin_engine.url.database)
        await source.publish_edition(source_fixture, 1)
        async with source.generic_refresh_execution(source_fixture, monkeypatch) as (fixture, serving, stack, head_url):
            await _assert_inventory_migration(fixture)
            await source.start_refresh_runtime(fixture, source_fixture, serving, stack, head_url)
            edit_url = fixture.engine.url.set(username=fixture.roles_by_kind["api"], password=fixture.password)
            fixture.edit_sessions = await source.fixed_factory(stack, edit_url)
            assert fixture.roles_by_kind["execution"] != fixture.roles_by_kind["custodian"]
            assert await fixture.custodian.fetchval("SELECT current_user::text") == fixture.roles_by_kind["custodian"]
            fixture.reader_network_ids = (fixture.network_id,)
            first = await _publish_refresh(fixture, 0)
            assert len(first.snapshot[1]) == 1
            approval_view = SimpleNamespace(
                connection=fixture.connection,
                engine=fixture.engine,
                control_schema=fixture.control_schema,
                source_schema=source_fixture.schema,
            )
            manual = await custom._seed(approval_view)
            assert manual.provider["record"]["npi"] is None
            fixture.reader_network_ids = tuple(sorted((fixture.network_id, manual.network["record_id"])))
            added = await _publish_refresh(fixture, 1)
            assert len(added.snapshot[1]) == 2
            _assert_manual(added.snapshot, manual, True)
            await _assert_original_anchor(fixture, added, first)
            assert (added.retention["source_rows"], added.retention["add_rows"], added.retention["effective_rows"]) == (
                1,
                1,
                2,
            )
            current, suppressed, cleared = await _successor_and_custom_changes(
                fixture, source_fixture, approval_view, manual, added
            )
            assert fixture.cas_checks == [
                state.manifest.generation_id for state in (first, added, current, suppressed, cleared)
            ]
            assert fixture.calls.count(COMMIT_AUTHORITY_PATH) == 5
            assert (await _counts(fixture))[-1] == 5
            assert source_fixture.commits == [candidate.dataset_id for candidate in source_fixture.candidates]
            assert (
                source_fixture.source_writer_rejections == source_fixture.commits and len(source_fixture.commits) == 2
            )
            record_property(
                "qualification",
                "genuine Source v2 + manual direct offices + exact Source remove + independent custody + pinned old Reader + completed replay",
            )
            record_property(
                "missing_case", "NPI same-pair override requires separate genuine NPI capability and is not qualified"
            )
