# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native carrier scratch ownership and selected-row preparation without base COPY."""

import asyncio
import json
from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker

from process.network_address_projection import PinnedAddressSource
from process.network_approved_membership_source import pin_approved_membership_source
from process.network_compact_address_carriers import (
    CompactAddressCarrierError,
    prepare_compact_address_carriers,
    verify_compact_address_carriers,
)
from process.network_custom_office_rows import _insert_custom_rows
from process.registry_record_store import RegistryRecordCommand, apply_registry_record_command
from tests.test_network_compact_custom_offices_postgres import (
    _target,
)
from tests.test_network_compact_custom_offices_postgres import (
    address_db as address_db,
)
from tests.test_network_compact_custom_offices_postgres import (
    compact_custom_db as compact_custom_db,
)
from tests.test_network_custom_address_source_postgres import _draft, _seed
from tests.test_network_membership_draft_store_postgres import _member
from tests.test_registry_approval_store_postgres import _approve, _command
from tests.test_registry_retained_site_adoption_postgres import _published

pytestmark = pytest.mark.asyncio


class _NoBaseCopy:
    def __init__(self, connection):
        self.connection = connection

    def __getattr__(self, name):
        return getattr(self.connection, name)

    async def copy_from_query(self, *args, **kwargs):
        raise AssertionError("Carrier preparation must not copy the full base")


async def _registered(fixture, revision=0, *, direct=False):
    copy_target = await _target(fixture, revision)
    async with fixture.publisher.transaction(isolation="repeatable_read"):
        approved = await pin_approved_membership_source(
            fixture.publisher, approved_revision=revision, control_schema=fixture.control_schema
        )
        await fixture.publisher.execute(
            f"UPDATE \"{fixture.control_schema}\".network_membership_candidate SET source_generations=source_generations||jsonb_build_object('custom_membership',$2::text) WHERE candidate_id=$1",
            UUID(copy_target.candidate_id),
            approved.generation_id,
        )
    if direct:
        await _insert_custom_rows(
            fixture.publisher, copy_target, fixture.base, None, '"' + fixture.control_schema + '"', revision
        )
        await fixture.publisher.execute(
            f'ALTER TABLE "{copy_target.schema_name}".custom_provider_location_binding RENAME TO provider_location_binding'
        )
    else:
        await fixture.publisher.execute(f'''CREATE TABLE "{copy_target.schema_name}".provider_location_binding(
      provider_system text NOT NULL,provider_id text NOT NULL,location_id uuid NOT NULL,
      location_key varchar(64) NOT NULL UNIQUE,entity_type text NOT NULL,entity_id text NOT NULL,
      PRIMARY KEY(provider_system,provider_id,location_id))''')
    return copy_target, approved


@asynccontextmanager
async def _retained_office(fixture):
    namespace = "carrier_source_" + uuid4().hex
    source_targets = []
    try:
        for extension in ("intarray", "btree_gin"):
            await fixture.connection.execute(f"CREATE EXTENSION IF NOT EXISTS {extension}")
        await fixture.connection.execute(f'CREATE SCHEMA "{namespace}"')
        await fixture.connection.execute(
            f'CREATE TABLE "{namespace}".entity_address_unified(LIKE "{fixture.source_schema}".entity_address_unified INCLUDING DEFAULTS)'
        )
        await fixture.connection.execute(
            f'INSERT INTO "{namespace}".entity_address_unified SELECT (jsonb_populate_record(NULL::"{namespace}".entity_address_unified,to_jsonb(original)||$1::jsonb)).* FROM "{fixture.source_schema}".entity_address_unified original LIMIT 1',
            json.dumps(dict(location_key="a" * 64, first_line="123 Example Street", checksum=42)),
        )
        for statement in (
            f'ALTER TABLE "{namespace}".entity_address_unified ADD PRIMARY KEY(location_key)',
            f'ALTER TABLE "{namespace}".entity_address_unified OWNER TO "{fixture.roles["owner"]}"',
            f'ALTER SCHEMA "{namespace}" OWNER TO "{fixture.roles["owner"]}"',
            f'GRANT USAGE ON SCHEMA "{namespace}" TO "{fixture.roles["reader"]}"',
            f'GRANT SELECT ON ALL TABLES IN SCHEMA "{namespace}" TO "{fixture.roles["reader"]}"',
        ):
            await fixture.connection.execute(statement)
        source_fixture = SimpleNamespace(
            **{
                **vars(fixture),
                "source_schema": namespace,
                "base": PinnedAddressSource(namespace, "entity_address_unified", "retained-carrier-source"),
            }
        )
        retained_source = await _published(source_fixture, 1, "provider_directory", source_targets)
        office_row = await fixture.connection.fetchrow(
            f'''SELECT provider_system,provider_id,location_id::text,location_key,
            encode(sha256(convert_to(to_jsonb(address)::text,'UTF8')),'hex') AS address_row_sha256
            FROM "{retained_source.schema_name}".provider_location_binding JOIN "{retained_source.schema_name}".entity_address_unified address USING(location_key)'''
        )
        yield retained_source, dict(office_row)
    finally:
        for copy_target in reversed(source_targets):
            await fixture.connection.execute(f'DROP SCHEMA IF EXISTS "{copy_target.schema_name}" CASCADE')
        await fixture.connection.execute(f'DROP SCHEMA IF EXISTS "{namespace}" CASCADE')
        assert await fixture.connection.fetchval("SELECT to_regnamespace($1)", namespace) is None


async def _approve_retained_pair(fixture, seed, retained_source, office):
    site_command = RegistryRecordCommand(
        "site_binding",
        uuid4(),
        "create",
        0,
        {"source_generation": retained_source.generation_id, **office},
        "Review retained office",
        uuid4().hex,
    )
    sessions = async_sessionmaker(fixture.engine.execution_options(isolation_level="REPEATABLE READ"))
    async with sessions() as session, session.begin():
        site = await apply_registry_record_command(
            session, site_command, seed.actor, schema=fixture.control_schema, source_schema=fixture.source_schema
        )
    membership = await _draft(
        fixture,
        replace(
            seed.membership_command,
            operation="correct",
            expected_revision=1,
            idempotency_key=uuid4().hex,
            fields={
                "memberships_json": [
                    *seed.membership_command.fields["memberships_json"],
                    _member(
                        seed.network["record_id"],
                        office["provider_id"],
                        office["location_id"],
                        system="provider_directory",
                    ),
                ]
            },
        ),
        seed.actor,
    )
    approval = await _approve(
        fixture.connection,
        fixture.control_schema,
        await _command(fixture.connection, fixture.control_schema, site, membership),
        seed.actor,
    )
    return approval["approved_revision"]


async def _prepare(fixture, copy_target, approved, connection=None):
    return await prepare_compact_address_carriers(
        connection or fixture.publisher,
        copy_target,
        fixture.base,
        approved,
        (),
        runtime_roles=(fixture.roles["reader"],),
        control_schema=fixture.control_schema,
    )


async def _assert_scratch_empty(connection):
    assert await connection.fetchval(
        "SELECT to_regclass('pg_temp.entity_address_unified') IS NULL AND to_regclass('pg_temp.provider_location_binding') IS NULL"
    )


async def test_empty_native_carrier_keeps_base_external_and_verifies_receipt(compact_custom_db):
    fixture = compact_custom_db
    copy_target, approved = await _registered(fixture)
    async with fixture.publisher.transaction(isolation="repeatable_read"):
        receipt = await _prepare(fixture, copy_target, approved, _NoBaseCopy(fixture.publisher))
        assert receipt.selected_address_rows == receipt.materialized_address_rows == 0
        assert receipt.address_table_oid is None and receipt.origin_references == ()
        await _assert_scratch_empty(fixture.publisher)
    async with fixture.publisher.transaction(isolation="repeatable_read"):
        assert (
            await verify_compact_address_carriers(
                fixture.publisher,
                receipt,
                runtime_roles=receipt.runtime_roles,
                control_schema=fixture.control_schema,
            )
            == receipt
        )
        await _assert_scratch_empty(fixture.publisher)
        with pytest.raises(CompactAddressCarrierError, match="native_receipt_changed"):
            await verify_compact_address_carriers(
                fixture.publisher,
                replace(receipt, approved_site_sha256="0" * 64),
                runtime_roles=receipt.runtime_roles,
            )
        await _assert_scratch_empty(fixture.publisher)


async def _remove_retained_carrier(fixture, seed, receipt):
    removed = await _draft(
        fixture,
        replace(seed.membership_command, operation="correct", expected_revision=2, idempotency_key=uuid4().hex),
        seed.actor,
    )
    approval = await _approve(
        fixture.connection,
        fixture.control_schema,
        await _command(fixture.connection, fixture.control_schema, removed),
        seed.actor,
    )
    next_target, next_approved = await _registered(fixture, approval["approved_revision"], direct=True)
    async with fixture.publisher.transaction(isolation="repeatable_read"):
        next_receipt = await _prepare(fixture, next_target, next_approved, _NoBaseCopy(fixture.publisher))
        assert next_receipt.selected_address_rows == next_receipt.materialized_address_rows == 0
        assert next_receipt.origin_references == ()
        assert (
            await fixture.publisher.fetchval(f'SELECT count(*) FROM "{next_target.schema_name}".entity_address_unified')
            == 1
        )
        assert (
            await verify_compact_address_carriers(fixture.publisher, receipt, runtime_roles=receipt.runtime_roles)
            == receipt
        )


async def test_selected_retained_office_preserves_direct_manual_and_full_row_proof(compact_custom_db):
    fixture = compact_custom_db
    async with _retained_office(fixture) as (retained_source, office):
        seed = await _seed(fixture)
        revision = await _approve_retained_pair(fixture, seed, retained_source, office)
        copy_target, approved = await _registered(fixture, revision, direct=True)
        async with fixture.publisher.transaction(isolation="repeatable_read"):
            receipt = await _prepare(fixture, copy_target, approved, _NoBaseCopy(fixture.publisher))
        assert receipt.selected_address_rows == receipt.selected_binding_rows == receipt.materialized_address_rows == 1
        assert len(receipt.origin_references) == 1
        assert receipt.origin_references[0][:2] == (retained_source.generation_id, retained_source.manifest_sha256)
        assert receipt.origin_references[0][6] == 1
        namespace = '"' + copy_target.schema_name + '"'
        assert await fixture.publisher.fetchval(f"SELECT count(*) FROM {namespace}.entity_address_unified") == 2
        assert await fixture.publisher.fetchval(f"SELECT count(*) FROM {namespace}.provider_location_binding") == 2
        await fixture.publisher.execute(
            f"UPDATE {namespace}.entity_address_unified SET canonical_network_ids=ARRAY[42] WHERE location_key=$1",
            office["location_key"],
        )
        async with fixture.publisher.transaction(isolation="repeatable_read"):
            assert (
                await verify_compact_address_carriers(fixture.publisher, receipt, runtime_roles=receipt.runtime_roles)
                == receipt
            )
            await _assert_scratch_empty(fixture.publisher)
        transaction = fixture.publisher.transaction(isolation="repeatable_read")
        await transaction.start()
        try:
            await fixture.publisher.execute(
                f"UPDATE {namespace}.entity_address_unified SET first_line=$1 WHERE location_key=$2",
                "456 Different Street",
                office["location_key"],
            )
            with pytest.raises(CompactAddressCarrierError, match="expected_rows_differ"):
                await verify_compact_address_carriers(fixture.publisher, receipt, runtime_roles=receipt.runtime_roles)
            await _assert_scratch_empty(fixture.publisher)
        finally:
            await transaction.rollback()
        await _remove_retained_carrier(fixture, seed, receipt)


@pytest.mark.parametrize("name", ["entity_address_unified", "provider_location_binding"])
async def test_preexisting_native_scratch_name_is_never_changed_or_dropped(compact_custom_db, name):
    fixture = compact_custom_db
    copy_target, approved = await _registered(fixture)
    async with fixture.publisher.transaction(isolation="repeatable_read"):
        await fixture.publisher.execute(f'CREATE TEMP TABLE "{name}"(marker integer) ON COMMIT DROP')
        await fixture.publisher.execute(f'INSERT INTO pg_temp."{name}" VALUES(71)')
        oid = await fixture.publisher.fetchval("SELECT to_regclass($1)::oid", "pg_temp." + name)
        with pytest.raises(CompactAddressCarrierError, match="scratch_names_in_use"):
            await _prepare(fixture, copy_target, approved)
        assert await fixture.publisher.fetchval("SELECT to_regclass($1)::oid", "pg_temp." + name) == oid
        assert await fixture.publisher.fetchval(f'SELECT marker FROM pg_temp."{name}"') == 71


async def test_cancelled_partial_native_scratch_creation_rolls_back_only_owned_work(compact_custom_db):
    fixture = compact_custom_db
    copy_target, approved = await _registered(fixture)

    class CancelAfterDDL(_NoBaseCopy):
        async def execute(self, statement, *args):
            result = await self.connection.execute(statement, *args)
            if statement.startswith("CREATE TEMP TABLE provider_location_binding"):
                raise asyncio.CancelledError
            return result

    async with fixture.publisher.transaction(isolation="repeatable_read"):
        with pytest.raises(asyncio.CancelledError):
            await _prepare(fixture, copy_target, approved, CancelAfterDDL(fixture.publisher))
        await _assert_scratch_empty(fixture.publisher)
        assert (
            await fixture.publisher.fetchval(
                "SELECT to_regclass($1)", copy_target.schema_name + ".entity_address_unified"
            )
            is None
        )
        assert (
            await fixture.publisher.fetchval(
                f'SELECT count(*) FROM "{copy_target.schema_name}".provider_location_binding'
            )
            == 0
        )
