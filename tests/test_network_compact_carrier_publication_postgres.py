# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native retained carriers, approved precedence, independent custody and pinned reads."""

import json
import re
from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker

from process.network_address_projection import PinnedAddressSource
from process.network_compact_retention import (
    NetworkCompactRetentionError,
    require_network_compact_artifacts_unreferenced,
)
from process.network_custom_address_source import NetworkCustomAddressSourceError
from process.network_membership_pipeline import prepare_and_publish_network_candidate
from process.network_serving_read import resolve_network_serving_manifest
from process.registry_candidate_composition import (
    RegistryCompositionAddressSources,
    compose_registry_membership_candidate,
)
from process.registry_initial_source_composition import RegistryInitialSourceError
from process.registry_record_store import RegistryRecordCommand, apply_registry_record_command
from process.uhc_flex_practitioner_async_safety import drain_operation
from tests import test_network_compact_publication_postgres as publication_tests
from tests.test_manual_location_identity_store_postgres import _create as _location
from tests.test_manual_provider_identity_store_postgres import _create as _provider
from tests.test_network_compact_custom_publication_postgres import _publish_registered
from tests.test_network_compact_publication_postgres import (
    _candidate,
    _prepare,
    _publish,
    _read,
    _reader,
    _scoped_rows,
    _seal,
)
from tests.test_network_custom_address_source_postgres import _draft
from tests.test_network_membership_draft_store_postgres import _command as _membership
from tests.test_network_membership_draft_store_postgres import _member
from tests.test_registry_approval_store_postgres import _actor, _approve, _command, _create

pytestmark = pytest.mark.asyncio


@pytest.fixture
async def carrier_publication_db(monkeypatch):
    """Shape synthetic base rows before their genuine native authority is captured."""
    original = publication_tests._full_two_offices
    manual_provider, manual_location = uuid4(), uuid4()

    async def source_rows(connection, roles, prepare):
        schema = await original(connection, roles, prepare)
        await connection.execute(
            f"UPDATE \"{schema}\".entity_address_unified SET entity_type='manual',entity_id=$1,address_key=$2,npi=NULL,inferred_npi=NULL WHERE location_key='old-office'",
            str(manual_provider),
            manual_location,
        )
        return schema

    monkeypatch.setattr(publication_tests, "_full_two_offices", source_rows)
    async with asynccontextmanager(publication_tests.compact_publication_db.__wrapped__)(monkeypatch) as fixture:
        fixture.manual_provider, fixture.manual_location = manual_provider, manual_location
        yield fixture


class _NoFullBaseCopy:
    """Spy on the real writer while preserving its native credentials and SQL."""

    def __init__(self, connection, base):
        self.connection = connection
        self.base = base.schema_name + "." + base.table_name
        self.exports = 0

    def __getattr__(self, name):
        return getattr(self.connection, name)

    async def copy_from_query(self, query, *args, **kwargs):
        self.exports += 1
        assert self.base not in query.replace('"', ""), "Compact publication exported the address base"
        return await self.connection.copy_from_query(query, *args, **kwargs)

    async def execute(self, query, *args, **kwargs):
        normalized = " ".join(query.replace('"', "").lower().split())
        if normalized.startswith(("create table ", "insert into ")) and ".entity_address_unified" in normalized:
            projection = re.sub(r"select (?:1 )?from " + re.escape(self.base) + r"\b", "base_exists_probe", normalized)
            assert "from " + self.base not in projection, "Compact publication cloned the address base"
        return await self.connection.execute(query, *args, **kwargs)


async def _drop_source(connection, namespace, owned_namespace_by_field):
    if "oid" not in owned_namespace_by_field:
        return
    native = await connection.fetchrow("SELECT oid,nspowner FROM pg_namespace WHERE nspname=$1", namespace)
    assert native is not None and native["oid"] == owned_namespace_by_field["oid"]
    assert native["nspowner"] in owned_namespace_by_field["allowed_owners"]
    await connection.execute(f'DROP SCHEMA IF EXISTS "{namespace}" CASCADE')
    assert await connection.fetchval("SELECT to_regnamespace($1)", namespace) is None


async def _source_namespace(fixture, namespace, owned_namespace_by_field):
    assert await fixture.connection.fetchval("SELECT to_regnamespace($1)", namespace) is None
    await fixture.connection.execute(f'CREATE SCHEMA "{namespace}"')
    owned_namespace_by_field["oid"] = await fixture.connection.fetchval("SELECT to_regnamespace($1)::oid", namespace)
    owned_namespace_by_field["allowed_owners"] = await fixture.connection.fetchval(
        "SELECT array_agg(oid) FROM pg_roles WHERE rolname=current_user OR rolname=$1", fixture.roles["owner"]
    )


async def _source_rows(fixture, namespace, manual_provider, manual_location):
    table = f'"{namespace}".entity_address_unified'
    await fixture.connection.execute(
        f'CREATE TABLE {table}(LIKE "{fixture.address.schema}".entity_address_unified INCLUDING DEFAULTS)'
    )
    selected_offices = []
    for index, (system, provider, location) in enumerate(
        (("manual", manual_provider, manual_location), ("provider_directory", uuid4(), uuid4())), 1
    ):
        office_by_field = dict(
            location_key=str(index) * 64,
            entity_type=system,
            entity_id=str(provider),
            address_key=str(location),
            npi=None,
            inferred_npi=None,
            checksum=index,
            first_line="123 Original Source Street",
            canonical_network_ids=[],
        )
        await fixture.connection.execute(
            f"INSERT INTO {table} SELECT (jsonb_populate_record(NULL::{table},to_jsonb(original)||$1::jsonb)).* "
            f'FROM "{fixture.address.schema}".entity_address_unified original LIMIT 1',
            json.dumps(office_by_field),
        )
        selected_offices.append(office_by_field)
    for statement in (
        f"ALTER TABLE {table} ADD PRIMARY KEY(location_key)",
        f'ALTER TABLE {table} OWNER TO "{fixture.roles["owner"]}"',
        f'ALTER SCHEMA "{namespace}" OWNER TO "{fixture.roles["owner"]}"',
        f'GRANT USAGE ON SCHEMA "{namespace}" TO "{fixture.roles["reader"]}"',
        f'GRANT SELECT ON {table} TO "{fixture.roles["reader"]}"',
    ):
        await fixture.connection.execute(statement)
    return selected_offices


@asynccontextmanager
async def _published_source(fixture):
    namespace = "carrier_publication_source_" + uuid4().hex
    manual_provider, manual_location = fixture.manual_provider, fixture.manual_location
    owned_namespace_by_field = {}
    try:
        await drain_operation(
            _source_namespace(fixture, namespace, owned_namespace_by_field), preserve_cancellation=True
        )
        for extension in ("intarray", "btree_gin"):
            await fixture.connection.execute(f"CREATE EXTENSION IF NOT EXISTS {extension}")
        offices = await _source_rows(fixture, namespace, manual_provider, manual_location)
        source_fixture = SimpleNamespace(
            **{
                **vars(fixture),
                "source": PinnedAddressSource(namespace, "entity_address_unified", "retained-carrier-addresses"),
            }
        )
        native_locations = [dict(office, address_key=UUID(office["address_key"])) for office in offices]
        copy_target = await _candidate(source_fixture, (42, 42), 0, locations=native_locations)
        await _publish_registered(source_fixture, copy_target)
        async with fixture.execution_connection.transaction(isolation="repeatable_read", readonly=True):
            manifest = await resolve_network_serving_manifest(
                fixture.execution_connection, control_schema=fixture.control_schema
            )
        assert manifest.compact_layout is None
        yield SimpleNamespace(
            manifest=manifest, offices=offices, manual_provider=manual_provider, manual_location=manual_location
        )
    finally:
        await drain_operation(
            _drop_source(fixture.connection, namespace, owned_namespace_by_field), preserve_cancellation=False
        )


async def _current_predecessor(fixture, historical):
    offices = await fixture.connection.fetch(
        f"SELECT location_key,entity_type,entity_id,address_key FROM \"{fixture.address.schema}\".entity_address_unified WHERE location_key='old-office'"
    )
    copy_target = await _candidate(fixture, (42,), historical.generation_id, locations=offices)

    await _prepare(fixture, copy_target)
    await _seal(fixture, copy_target)
    await _publish(fixture, copy_target)
    async with fixture.execution_connection.transaction(isolation="repeatable_read", readonly=True):
        manifest = await resolve_network_serving_manifest(
            fixture.execution_connection, control_schema=fixture.control_schema
        )
    assert manifest.compact_layout.base.table_oid == fixture.address.descriptor["table"]["oid"]
    return manifest


async def _site_record(fixture, approval, retained_source, actor):
    selection = await fixture.connection.fetchrow(
        f'''SELECT binding.provider_system,binding.provider_id,binding.location_id::text,binding.location_key,
        encode(sha256(convert_to(to_jsonb(address)::text,'UTF8')),'hex') AS address_row_sha256
        FROM "{retained_source.manifest.schema_name}".provider_location_binding binding
        JOIN "{retained_source.manifest.schema_name}".entity_address_unified address USING(location_key)
        WHERE binding.provider_system='provider_directory' '''
    )
    command = RegistryRecordCommand(
        "site_binding",
        uuid4(),
        "create",
        0,
        {"source_generation": retained_source.manifest.generation_id, **dict(selection)},
        "Review exact retained office",
        uuid4().hex,
    )
    sessions = async_sessionmaker(fixture.engine.execution_options(isolation_level="REPEATABLE READ"))
    async with sessions() as session, session.begin():
        record = await apply_registry_record_command(
            session, command, actor, schema=fixture.control_schema, source_schema=approval.source_schema
        )
    return record, dict(selection)


async def _approve_offices(fixture, retained_source):
    await fixture.connection.execute(f'CREATE TABLE "{fixture.address.schema}".npi(npi bigint PRIMARY KEY)')
    approval = SimpleNamespace(
        connection=fixture.connection,
        engine=fixture.engine,
        control_schema=fixture.control_schema,
        source_schema=fixture.address.schema,
    )
    actor = _actor()
    allocation = await fixture.connection.fetchval(
        f'SELECT allocation_key FROM "{fixture.control_schema}".network_registry_identity WHERE network_id=42'
    )
    network = await _draft(approval, replace(_create("network"), allocation_key=allocation), actor)
    assert network["record_id"] == 42
    provider = await _draft(approval, _provider(record_id=retained_source.manual_provider), actor)
    location = await _draft(approval, _location(record_id=retained_source.manual_location), actor)
    site, selected = await _site_record(fixture, approval, retained_source, actor)
    members = [
        _member(42, str(retained_source.manual_provider), str(retained_source.manual_location)),
        _member(42, selected["provider_id"], selected["location_id"], system="provider_directory"),
    ]
    membership_command = _membership(42, members)
    membership = await _draft(approval, membership_command, actor)
    approved = await _approve(
        fixture.connection,
        fixture.control_schema,
        await _command(fixture.connection, fixture.control_schema, network, provider, location, site, membership),
        actor,
    )
    return SimpleNamespace(
        approval=approval,
        actor=actor,
        revision=approved["approved_revision"],
        membership_command=membership_command,
        selected=selected,
    )


async def _compose(fixture, revision, retained_source):
    copy_target, address = await compose_registry_membership_candidate(
        fixture.execution_connection,
        request_id=uuid4(),
        approved_revision=revision,
        expected_head=retained_source.generation_id,
        source_manifest=retained_source,
        address_sources=RegistryCompositionAddressSources(fixture.source, compact=True),
        writer_roles=fixture.writer_roles,
        control_schema=fixture.control_schema,
    )
    assert address == fixture.source
    return copy_target


async def _assert_no_seal(fixture, copy_target):
    assert (
        await fixture.connection.fetchval(
            f'SELECT count(*) FROM "{fixture.control_schema}".network_candidate_custody WHERE candidate_id=$1',
            UUID(copy_target.candidate_id),
        )
        == 0
    )


async def _reject_extra_heap_row(fixture, copy_target, selected):
    table = f'"{copy_target.schema_name}".entity_address_unified'
    extra_key = "f" * 64
    await fixture.execution_connection.execute(
        f"INSERT INTO {table} SELECT (jsonb_populate_record(NULL::{table},to_jsonb(original)||$1::jsonb)).* "
        f"FROM {table} original WHERE location_key=$2",
        json.dumps({"location_key": extra_key}),
        selected["location_key"],
    )
    try:
        with pytest.raises(NetworkCustomAddressSourceError, match="compact_custom_offices_expected_rows_differ"):
            await _seal(fixture, copy_target)
        await _assert_no_seal(fixture, copy_target)
    finally:
        await fixture.execution_connection.execute(f"DELETE FROM {table} WHERE location_key=$1", extra_key)


async def _reject_extra_binding(fixture, copy_target, selected):
    table = f'"{copy_target.schema_name}".provider_location_binding'
    location = uuid4()
    await fixture.execution_connection.execute(
        f"INSERT INTO {table} VALUES('provider_directory','unselected-provider',$1,$2,'provider_directory',$3)",
        location,
        selected["location_key"],
        selected["provider_id"],
    )
    try:
        with pytest.raises(RegistryInitialSourceError, match="registry_source_office_bindings_differ"):
            await _seal(fixture, copy_target)
        await _assert_no_seal(fixture, copy_target)
    finally:
        await fixture.execution_connection.execute(f"DELETE FROM {table} WHERE location_id=$1", location)


async def _publish_with_refusals(fixture, copy_target, selected):
    async def independently_seal(target):
        assert target == copy_target
        ready = json.loads(
            await fixture.connection.fetchval(
                f'SELECT validation_json FROM "{fixture.control_schema}".network_membership_candidate WHERE candidate_id=$1',
                UUID(target.candidate_id),
            )
        )
        assert ready["compact_address_carriers"]["materialized_address_rows"] == 1
        await _reject_extra_heap_row(fixture, target, selected)
        await _reject_extra_binding(fixture, target, selected)
        return await _seal(fixture, target)

    return await prepare_and_publish_network_candidate(
        fixture.execution_connection,
        copy_target,
        fixture.source,
        **fixture.writer_roles,
        control_schema=fixture.control_schema,
        seal_candidate=independently_seal,
    )


async def _remove_offices(fixture, reviewed, first_pin):
    removals = [{**member, "operation": "remove"} for member in reviewed.membership_command.fields["memberships_json"]]
    command = replace(
        reviewed.membership_command,
        operation="correct",
        expected_revision=1,
        fields={"memberships_json": removals},
        idempotency_key=uuid4().hex,
    )
    corrected = await _draft(reviewed.approval, command, reviewed.actor)
    approved = await _approve(
        fixture.connection,
        fixture.control_schema,
        await _command(fixture.connection, fixture.control_schema, corrected),
        reviewed.actor,
    )
    second = await _compose(fixture, approved["approved_revision"], first_pin)
    await _publish_registered(fixture, second)
    new_pin, new_rows = await _read(fixture)
    assert new_pin.generation_id > first_pin.generation_id and new_rows == []
    assert await fixture.connection.fetchval(f'SELECT count(*) FROM "{second.schema_name}".network_membership') == 0
    assert (
        await fixture.connection.fetchval(f'SELECT count(*) FROM "{second.schema_name}".network_membership_source') == 1
    )


async def _assert_native_projection(fixture, copy_target, retained_source, offices):
    assert len(offices) == 2 and {office["entity_type"] for office in offices} == {"manual", "provider_directory"}
    assert all(office["canonical_network_ids"] == [42] for office in offices)
    manual = next(office for office in offices if office["entity_type"] == "manual")
    assert manual["location_key"] != retained_source.offices[0]["location_key"]
    original_key = await fixture.connection.fetchval(
        f'SELECT location_key FROM "{retained_source.predecessor.schema_name}".provider_location_binding '
        "WHERE provider_system='manual' AND provider_id=$1 AND location_id=$2",
        str(retained_source.manual_provider),
        retained_source.manual_location,
    )
    assert original_key == "old-office" and manual["location_key"] != original_key
    assert manual["second_line"] == "Suite 2"
    assert (
        await fixture.connection.fetchval(
            f'SELECT location_key FROM "{copy_target.schema_name}".provider_location_binding '
            "WHERE provider_system='manual' AND provider_id=$1 AND location_id=$2",
            str(retained_source.manual_provider),
            retained_source.manual_location,
        )
        == manual["location_key"]
    )
    assert (
        await fixture.connection.fetchval(f'SELECT count(*) FROM "{copy_target.schema_name}".entity_address_unified')
        == 2
    )
    columns = "network_id,provider_system,provider_id,location_id,evidence_id"
    original = await fixture.connection.fetch(
        f'SELECT {columns} FROM "{retained_source.predecessor.schema_name}".network_membership ORDER BY {columns}'
    )
    actual = await fixture.connection.fetch(
        f'SELECT {columns} FROM "{copy_target.schema_name}".network_membership_source ORDER BY {columns}'
    )
    assert len(actual) == 1 and actual == original
    carrier = json.loads(
        await fixture.connection.fetchval(
            f"SELECT validation_json->'compact_address_carriers' FROM \"{fixture.control_schema}\".network_membership_candidate WHERE candidate_id=$1",
            UUID(copy_target.candidate_id),
        )
    )
    assert carrier["selected_address_rows"] == carrier["materialized_address_rows"] == 1
    assert len(carrier["origin_references"]) == 1


async def _assert_historical_carrier_retained(fixture, manifest):
    assert not fixture.connection.is_in_transaction()
    oids = await fixture.connection.fetchrow(
        "SELECT to_regclass($1)::oid::bigint,to_regclass($2)::oid::bigint",
        manifest.schema_name + ".entity_address_unified",
        manifest.schema_name + ".provider_location_binding",
    )
    assert all(type(oid) is int for oid in oids.values())
    transaction = fixture.connection.transaction(isolation="read_committed")
    await transaction.start()
    try:
        await fixture.connection.execute(
            f'UPDATE "{fixture.control_schema}".network_serving_manifest SET eligible=false WHERE generation_id=$1',
            manifest.generation_id,
        )
        for oid in oids.values():
            with pytest.raises(NetworkCompactRetentionError, match="network_compact_retention_artifact_referenced"):
                await require_network_compact_artifacts_unreferenced(
                    fixture.connection, (oid,), control_schema=fixture.control_schema
                )
    finally:
        await transaction.rollback()


async def test_carrier_publication_manual_precedence_tamper_refusals_and_pinned_removal(carrier_publication_db):
    fixture = carrier_publication_db
    async with _published_source(fixture) as retained_source:
        retained_source.predecessor = await _current_predecessor(fixture, retained_source.manifest)
        reviewed = await _approve_offices(fixture, retained_source)
        original_base = await fixture.connection.fetch(
            f'SELECT * FROM "{fixture.address.schema}".entity_address_unified ORDER BY location_key'
        )
        assert not {office["location_key"] for office in retained_source.offices} & {
            office_row["location_key"] for office_row in original_base
        }
        native_writer = fixture.execution_connection
        fixture.execution_connection = _NoFullBaseCopy(native_writer, fixture.source)
        try:
            first = await _compose(fixture, reviewed.revision, retained_source.predecessor)
            await _publish_with_refusals(fixture, first, reviewed.selected)
            first_pin, offices = await _read(fixture)
            await _assert_native_projection(fixture, first, retained_source, offices)
            await _assert_historical_carrier_retained(fixture, retained_source.manifest)
            async with _reader(fixture) as (reader, native):
                pinned = await resolve_network_serving_manifest(native, control_schema=fixture.control_schema)
                assert await _scoped_rows(reader, pinned, (42,)) == offices
                await _remove_offices(fixture, reviewed, first_pin)
                assert await _scoped_rows(reader, pinned, (42,)) == offices
            assert (await _read(fixture, generation_id=first_pin.generation_id))[1] == offices
        finally:
            fixture.execution_connection = native_writer
        assert (
            await fixture.connection.fetch(
                f'SELECT * FROM "{fixture.address.schema}".entity_address_unified ORDER BY location_key'
            )
            == original_base
        )
