# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Direct custom contribution parity without retained-address cloning."""

import asyncio
import json
from dataclasses import replace
from uuid import UUID, uuid4

import pytest

from process.network_custom_address_source import NetworkCustomAddressSourceError, NetworkCustomAddressUnresolved
from process.network_custom_office_rows import CustomOfficeRowsUnsupported, prepare_custom_office_rows
from process.network_membership_copy import MembershipCopyTarget
from tests.test_network_custom_address_source_postgres import _draft, _seed, custom_db
from tests.test_network_custom_address_source_postgres import _prepare as _compose
from tests.test_network_serving_schema_postgres import serving_schema
from tests.test_registry_approval_store_postgres import _approve, _command


class _NoCopyConnection:
    def __init__(self, connection):
        self.connection = connection

    def __getattr__(self, name):
        return getattr(self.connection, name)

    async def copy_from_query(self, *arguments, **kwargs):
        raise AssertionError("Direct custom preparation must not export address rows")

    async def copy_to_table(self, *arguments, **kwargs):
        raise AssertionError("Direct custom preparation must not clone address rows")

    async def copy_records_to_table(self, *arguments, **kwargs):
        raise AssertionError("Direct custom preparation must not clone address rows")


@pytest.fixture
async def office_rows_db(custom_db):
    fixture = custom_db
    fixture.custom_targets = []
    try:
        yield fixture
    finally:
        for target in reversed(fixture.custom_targets):
            await fixture.connection.execute(f'DROP SCHEMA IF EXISTS "{target.schema_name}" CASCADE')
            assert await fixture.connection.fetchval("SELECT to_regnamespace($1)", target.schema_name) is None


async def _candidate(fixture, revision):
    candidate_id = uuid4()
    target = MembershipCopyTarget(
        *[str(uuid4()) for _ in range(3)], str(candidate_id), "network_candidate_" + candidate_id.hex
    )
    fixture.custom_targets.append(target)
    await fixture.connection.execute(f'CREATE SCHEMA "{target.schema_name}"')
    await fixture.connection.execute(
        f'INSERT INTO "{fixture.control_schema}".network_membership_candidate '
        "(candidate_id,dataset_id,schema_id,producer_id,schema_name,state,source_generations,"
        "approved_custom_revision,expected_head,expected_rows) "
        "VALUES($1,$2,$3,$4,$5,'sealed',jsonb_build_object('unified_address',$6::text),$7,0,0)",
        candidate_id,
        *[UUID(getattr(target, name)) for name in ("dataset_id", "schema_id", "producer_id")],
        target.schema_name,
        fixture.base.generation_id,
        revision,
    )
    return target


async def _prepare_rows(fixture, target, revision, *, base=None, connection=None, npi_source=None):
    connection = connection or fixture.connection
    async with connection.transaction(isolation="repeatable_read"):
        return await prepare_custom_office_rows(
            connection,
            target,
            base or fixture.base,
            approved_revision=revision,
            runtime_roles=(fixture.roles["reader"],),
            npi_source=npi_source,
            control_schema=fixture.control_schema,
        )


async def _addresses(fixture, source, *, custom_only=False):
    predicate = " WHERE row_origin='network_custom'" if custom_only else ""
    return [
        dict(row)
        for row in await fixture.connection.fetch(
            f'SELECT * FROM "{source}".entity_address_unified{predicate} ORDER BY location_key'
        )
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("system", ["manual", "npi"])
async def test_direct_rows_match_complete_composition(office_rows_db, system):
    fixture = office_rows_db
    seed = await _seed(fixture, system=system)
    complete = await _compose(fixture, seed.revision)
    copy_target = await _candidate(fixture, seed.revision)
    statements = []
    fixture.connection.add_query_logger(statements.append)
    try:
        receipt = await _prepare_rows(
            fixture, copy_target, seed.revision, connection=_NoCopyConnection(fixture.connection)
        )
        await asyncio.sleep(0)
    finally:
        fixture.connection.remove_query_logger(statements.append)
    contribution = await _addresses(fixture, copy_target.schema_name)
    assert contribution == await _addresses(fixture, complete.address_source.schema_name, custom_only=True)
    assert len(contribution) == receipt.custom_pair_count == 1
    assert receipt.approved_records_sha256 == complete.approved_records_sha256
    assert receipt.base_identity == complete.source_oids and receipt.npi_identity == complete.npi_oids
    custom_binding = await fixture.connection.fetch(
        f'SELECT * FROM "{copy_target.schema_name}".custom_provider_location_binding'
    )
    assert custom_binding == await fixture.connection.fetch(
        f'SELECT * FROM "{complete.address_source.schema_name}".provider_location_binding'
    )
    assert str(custom_binding[0]["location_id"]) == seed.first["record_id"]
    assert seed.second["record_id"] not in json.dumps([dict(binding) for binding in custom_binding], default=str)
    assert contribution[0]["entity_type"] == system
    assert contribution[0]["npi"] == (1000000004 if system == "npi" else None)
    assert not any(
        "EXCEPT ALL" in query.query or query.query.lstrip().startswith("CREATE INDEX") for query in statements
    )
    assert any("WHERE base.location_key=custom.location_key LIMIT 1" in query.query for query in statements)
    base_name = f'"{fixture.base.schema_name}"."{fixture.base.table_name}"'
    assert not any(
        f"SELECT * FROM {base_name}" in query.query or "ORDER BY a.location_key" in query.query for query in statements
    )
    assert receipt.address_table_oid == await fixture.connection.fetchval(
        "SELECT to_regclass($1)::oid", copy_target.schema_name + ".entity_address_unified"
    )
    assert receipt.binding_table_oid == await fixture.connection.fetchval(
        "SELECT to_regclass($1)::oid", copy_target.schema_name + ".custom_provider_location_binding"
    )
    assert receipt.as_dict()["component"] == "network_custom_office_rows"


@pytest.mark.asyncio
async def test_removal_clears_current_contribution_and_excludes_historical_custom(office_rows_db):
    fixture = office_rows_db
    seed = await _seed(fixture)
    complete = await _compose(fixture, seed.revision)
    corrected = await _draft(
        fixture,
        replace(
            seed.membership_command,
            operation="correct",
            expected_revision=1,
            fields={"memberships_json": []},
            idempotency_key=uuid4().hex,
        ),
        seed.actor,
    )
    approval = await _approve(
        fixture.connection,
        fixture.control_schema,
        await _command(fixture.connection, fixture.control_schema, corrected),
        seed.actor,
    )
    revision = approval["approved_revision"]
    copy_target = await _candidate(fixture, revision)
    receipt = await _prepare_rows(fixture, copy_target, revision, base=complete.address_source)
    replacement = await _compose(fixture, revision, base=complete.address_source)
    assert receipt.custom_pair_count == 0 and await _addresses(fixture, copy_target.schema_name) == []
    assert (
        await fixture.connection.fetchval(
            f'SELECT count(*) FROM "{copy_target.schema_name}".custom_provider_location_binding'
        )
        == 0
    )
    composed = await fixture.connection.fetch(f"""SELECT * FROM (
        SELECT * FROM "{complete.address_source.schema_name}".entity_address_unified WHERE row_origin<>'network_custom'
        UNION ALL SELECT * FROM "{copy_target.schema_name}".entity_address_unified) addresses ORDER BY location_key""")
    assert [dict(address) for address in composed] == await _addresses(fixture, replacement.address_source.schema_name)
    assert len(await _addresses(fixture, complete.address_source.schema_name, custom_only=True)) == 1


@pytest.mark.asyncio
async def test_remove_commands_never_promote_another_office(office_rows_db):
    fixture = office_rows_db
    seed = await _seed(fixture)
    await fixture.connection.execute(
        f"UPDATE \"{fixture.control_schema}\".registry_approved_record SET record_json=jsonb_set(record_json,'{{memberships_json}}',"
        "(record_json->'memberships_json')||jsonb_build_array((record_json#>'{memberships_json,0}')"
        "||jsonb_build_object('operation','remove','location_id',$1::text))) WHERE record_kind='membership'",
        seed.second["record_id"],
    )
    complete = await _compose(fixture, seed.revision)
    target = await _candidate(fixture, seed.revision)
    receipt = await _prepare_rows(fixture, target, seed.revision)
    assert receipt.custom_pair_count == 1
    assert await _addresses(fixture, target.schema_name) == await _addresses(
        fixture, complete.address_source.schema_name, custom_only=True
    )
    bindings = await fixture.connection.fetch(
        f'SELECT location_id FROM "{target.schema_name}".custom_provider_location_binding'
    )
    assert [str(binding["location_id"]) for binding in bindings] == [seed.first["record_id"]]


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["missing_npi", "missing_provider", "archived_provider", "archived_site"])
async def test_unknown_or_revoked_identity_rolls_back(office_rows_db, damage):
    fixture = office_rows_db
    seed = await _seed(fixture, system="npi" if damage == "missing_npi" else "manual")
    target = await _candidate(fixture, seed.revision)
    if damage == "missing_npi":
        await fixture.connection.execute(f'DELETE FROM "{fixture.source_schema}".npi')
    elif damage == "missing_provider":
        await fixture.connection.execute(
            f"DELETE FROM \"{fixture.control_schema}\".registry_approved_record WHERE record_kind='provider'"
        )
    else:
        kind = "provider" if damage == "archived_provider" else "location"
        await fixture.connection.execute(
            f"UPDATE \"{fixture.control_schema}\".registry_approved_record SET record_json=jsonb_set(record_json,'{{archived}}','true'::jsonb) WHERE record_kind=$1",
            kind,
        )
    async with fixture.connection.transaction(isolation="repeatable_read"):
        with pytest.raises(NetworkCustomAddressUnresolved):
            await _prepare_rows(fixture, target, seed.revision)
        assert (
            await fixture.connection.fetchval("SELECT to_regclass($1)", target.schema_name + ".entity_address_unified")
            is None
        )
        assert await fixture.connection.fetchval("SELECT 1") == 1


@pytest.mark.asyncio
async def test_imported_site_pairs_require_their_own_carrier(office_rows_db):
    fixture = office_rows_db
    seed = await _seed(fixture)
    site_id = str(uuid4())
    await fixture.connection.execute(
        f"UPDATE \"{fixture.control_schema}\".registry_approved_record SET record_json=jsonb_set(record_json,'{{memberships_json}}',"
        "record_json->'memberships_json'||jsonb_build_array(jsonb_build_object('network_id',$1::integer,'provider_system',"
        "'provider_directory','provider_id','source-provider','location_id',$2::text,'evidence_id','source-site'))) "
        "WHERE record_kind='membership'",
        int(seed.network["record_id"]),
        site_id,
    )
    await fixture.connection.execute(
        f'INSERT INTO "{fixture.control_schema}".registry_approved_record '
        "(approved_revision,record_kind,record_key,record_revision,custom_revision,record_json) "
        "VALUES($1,'site_binding',$2,1,1,jsonb_build_object('archived',false,'provider_system','provider_directory',"
        "'provider_id','source-provider','location_id',$3::text))",
        seed.revision,
        str(uuid4()),
        site_id,
    )
    target = await _candidate(fixture, seed.revision)
    with pytest.raises(CustomOfficeRowsUnsupported, match="retained-source"):
        await _prepare_rows(fixture, target, seed.revision)
    assert (
        await fixture.connection.fetchval("SELECT to_regclass($1)", target.schema_name + ".entity_address_unified")
        is None
    )


@pytest.mark.asyncio
async def test_public_base_collision_rolls_back_and_same_target_can_resume(office_rows_db):
    fixture = office_rows_db
    seed = await _seed(fixture)
    await fixture.connection.execute(
        f'UPDATE "{fixture.source_schema}".entity_address_unified SET location_key='
        "encode(sha256(convert_to(jsonb_build_array('network_custom','manual',$1::text,$2::uuid)::text,'UTF8')),'hex')",
        seed.provider["record_id"],
        UUID(seed.first["record_id"]),
    )
    target = await _candidate(fixture, seed.revision)
    with pytest.raises(NetworkCustomAddressSourceError, match="collides"):
        await _prepare_rows(fixture, target, seed.revision)
    for table in ("entity_address_unified", "custom_provider_location_binding"):
        assert await fixture.connection.fetchval("SELECT to_regclass($1)", target.schema_name + "." + table) is None
    await fixture.connection.execute(
        f'UPDATE "{fixture.source_schema}".entity_address_unified SET location_key=$1', "a" * 64
    )
    assert (await _prepare_rows(fixture, target, seed.revision)).custom_pair_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["open", "approved_revision", "target_scope"])
async def test_exact_candidate_scope_is_required(office_rows_db, damage):
    fixture = office_rows_db
    seed = await _seed(fixture)
    target = await _candidate(fixture, seed.revision)
    if damage == "open":
        await fixture.connection.execute(
            f"UPDATE \"{fixture.control_schema}\".network_membership_candidate SET state='open'"
        )
    elif damage == "approved_revision":
        await fixture.connection.execute(
            f'UPDATE "{fixture.control_schema}".network_membership_candidate SET approved_custom_revision=0'
        )
    else:
        target = replace(target, producer_id=str(uuid4()))
    with pytest.raises(ValueError):
        await _prepare_rows(fixture, target, seed.revision)
    assert (
        await fixture.connection.fetchval("SELECT to_regclass($1)", target.schema_name + ".entity_address_unified")
        is None
    )
