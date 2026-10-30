# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Current approved custom rows are independently proven against genuine EAU."""

import os
from contextlib import asynccontextmanager
from copy import deepcopy
from dataclasses import replace
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest

from process import network_address_artifact as artifact
from process import network_compact_custom_offices as custom
from process import network_custom_address_source as composition
from process.network_address_projection import PinnedAddressSource
from process.network_custom_address_source import NetworkCustomAddressSourceError, _source_identity
from tests.test_network_address_artifact_postgres import address_db
from tests.test_network_custom_address_source_postgres import _draft, _seed
from tests.test_network_custom_office_rows_postgres import _candidate, _NoCopyConnection
from tests.test_network_npi_artifact_postgres import npi_db as npi_db
from tests.test_network_serving_schema_postgres import serving_schema
from tests.test_registry_approval_store_postgres import _approve, _command

pytestmark = pytest.mark.asyncio


@pytest.fixture
async def compact_custom_db(address_db):
    previous = os.environ["NETWORK_REGISTRY_TEST_DSN"]
    os.environ["NETWORK_REGISTRY_TEST_DSN"] = address_db.url.render_as_string(hide_password=False)
    registry = serving_schema.__wrapped__()
    candidate_targets = []
    try:
        connection, control, engine = await anext(registry)
        await connection.execute(f'ALTER TABLE "{address_db.schema}".entity_address_unified DROP COLUMN postal_address')
        await connection.execute(f'CREATE TABLE "{address_db.schema}".npi(npi bigint PRIMARY KEY)')
        await connection.execute(f'INSERT INTO "{address_db.schema}".npi VALUES(1000000004)')
        await connection.execute(f'ALTER TABLE "{address_db.schema}".npi OWNER TO "{address_db.roles["owner"]}"')
        publisher = await address_db.publisher.fetchval("SELECT current_user")
        await connection.execute(f'ALTER SCHEMA "{address_db.schema}" OWNER TO CURRENT_USER')
        await connection.execute(
            f'GRANT USAGE ON SCHEMA "{address_db.schema}" TO "{publisher}","{address_db.roles["reader"]}"'
        )
        await connection.execute(f'GRANT USAGE ON SCHEMA "{control}" TO "{publisher}"')
        await connection.execute(f'GRANT SELECT ON ALL TABLES IN SCHEMA "{control}" TO "{publisher}"')
        await connection.execute(f'GRANT UPDATE ON "{control}".network_membership_candidate TO "{publisher}"')
        yield SimpleNamespace(
            connection=connection,
            engine=engine,
            control_schema=control,
            source_schema=address_db.schema,
            base=address_db.source,
            roles=address_db.roles,
            publisher=address_db.publisher,
            publisher_role=publisher,
            custom_targets=candidate_targets,
            genuine=address_db,
        )
    finally:
        for copy_target in reversed(candidate_targets):
            await address_db.admin.execute(f'DROP SCHEMA IF EXISTS "{copy_target.schema_name}" CASCADE')
        await registry.aclose()
        os.environ["NETWORK_REGISTRY_TEST_DSN"] = previous


async def _target(fixture, revision, state="open"):
    target = await _candidate(fixture, revision)
    await fixture.connection.execute(f'ALTER SCHEMA "{target.schema_name}" OWNER TO "{fixture.publisher_role}"')
    await fixture.connection.execute(
        f'UPDATE "{fixture.control_schema}".network_membership_candidate SET state=$1 WHERE candidate_id=$2',
        state,
        UUID(target.candidate_id),
    )
    return target


async def _prepare(fixture, target, revision, npi_source=None):
    async with fixture.publisher.transaction(isolation="repeatable_read"):
        return await custom.prepare_compact_custom_offices(
            _NoCopyConnection(fixture.publisher),
            target,
            fixture.base,
            approved_revision=revision,
            runtime_roles=(fixture.roles["reader"],),
            npi_source=npi_source,
            control_schema=fixture.control_schema,
        )


async def _verify(fixture, receipt, **options):
    async with fixture.publisher.transaction(isolation="repeatable_read"):
        return await custom.verify_compact_custom_offices(
            fixture.publisher,
            receipt,
            runtime_roles=(fixture.roles["reader"],),
            control_schema=fixture.control_schema,
            **options,
        )


async def _merge(fixture, receipt):
    namespace = '"' + receipt.target.schema_name + '"'
    await fixture.publisher.execute(
        f"CREATE TABLE {namespace}.provider_location_binding(LIKE {namespace}.custom_provider_location_binding INCLUDING ALL)"
    )
    await fixture.publisher.execute(
        f"INSERT INTO {namespace}.provider_location_binding SELECT * FROM {namespace}.custom_provider_location_binding"
    )
    await fixture.publisher.execute(f"DROP TABLE {namespace}.custom_provider_location_binding")
    oid = await fixture.publisher.fetchval(
        "SELECT to_regclass($1)::oid::bigint", receipt.target.schema_name + ".provider_location_binding"
    )
    return replace(receipt, binding_table_oid=oid)


@asynccontextmanager
async def _full_composition_publisher(fixture):
    database = await fixture.connection.fetchval("SELECT quote_ident(current_database())")
    privileges_sql = (
        "SELECT oid::bigint,datacl::text AS raw_acl,ARRAY["
        "has_database_privilege($1::name,oid,'CONNECT'),"
        "has_database_privilege($1::name,oid,'CREATE'),"
        "has_database_privilege($1::name,oid,'TEMPORARY')] AS effective_privileges "
        "FROM pg_database WHERE datname=current_database()"
    )
    before = await fixture.connection.fetchrow(privileges_sql, fixture.publisher_role)
    has_created_grant = False
    try:
        if not before["effective_privileges"][1]:
            has_created_grant = True
            await fixture.connection.execute(f'GRANT CREATE ON DATABASE {database} TO "{fixture.publisher_role}"')
        assert await fixture.publisher.fetchval("SELECT current_user") == fixture.publisher_role
        yield fixture.publisher
    finally:
        if has_created_grant:
            await fixture.connection.execute(f'REVOKE CREATE ON DATABASE {database} FROM "{fixture.publisher_role}"')
        after = await fixture.connection.fetchrow(privileges_sql, fixture.publisher_role)
        assert (after["oid"], after["effective_privileges"]) == (before["oid"], before["effective_privileges"])
        # A materialized default ACL is removed with address_db's exact child database.


async def _check_full_native_composition(fixture, publisher, arguments_by_name, namespace):
    source_table = '"' + fixture.source_schema + '".entity_address_unified'
    before = await publisher.fetch(f"SELECT * FROM {source_table} ORDER BY location_key")
    async with publisher.transaction(isolation="repeatable_read"):
        descriptor = await artifact.capture_network_address_artifact(
            publisher, fixture.base, runtime_roles=arguments_by_name["runtime_roles"]
        )
        shape = await composition._source_shape(publisher, descriptor["table"]["oid"], address=True)
        assert descriptor["descriptor_sha256"] != shape
        receipt = await composition.prepare_custom_address_source(publisher, fixture.base, **arguments_by_name)
        assert receipt.source_oids == (
            descriptor["namespace"]["oid"],
            descriptor["table"]["oid"],
            descriptor["protected_owner_oid"],
            descriptor["descriptor_sha256"],
        )
    async with publisher.transaction(isolation="repeatable_read", readonly=True):
        assert await composition.verify_custom_address_source(publisher, receipt) == receipt
        assert await composition.prepare_custom_address_source(publisher, fixture.base, **arguments_by_name) == receipt
        assert await composition._source_shape(publisher, receipt.address_table_oid, address=True) == shape
        assert await publisher.fetch(f"SELECT * FROM {source_table} ORDER BY location_key") == before
        addresses = await publisher.fetch(f"SELECT * FROM {namespace}.entity_address_unified")
        assert len(addresses) == len(before) + receipt.custom_pair_count == len(before) + 1
        assert all(address in addresses for address in before)


async def test_native_base_full_composition_keeps_descriptor_and_shape_separate(compact_custom_db):
    fixture = compact_custom_db
    seed = await _seed(fixture)
    identity = uuid4()
    namespace = '"network_composition_' + identity.hex + '"'
    arguments_by_name = dict(
        composition_id=str(identity),
        approved_revision=seed.revision,
        owner_role=fixture.roles["owner"],
        runtime_roles=(fixture.roles["reader"],),
        control_schema=fixture.control_schema,
    )
    async with _full_composition_publisher(fixture) as publisher:
        try:
            await _check_full_native_composition(fixture, publisher, arguments_by_name, namespace)
        finally:
            await fixture.connection.execute(f"DROP SCHEMA IF EXISTS {namespace} CASCADE")
            assert await fixture.connection.fetchval("SELECT to_regnamespace($1)", namespace) is None


async def test_ready_custom_rows_can_be_verified_without_control_update_privilege(compact_custom_db):
    fixture = compact_custom_db
    seed = await _seed(fixture)
    target = await _target(fixture, seed.revision)
    receipt = await _merge(fixture, await _prepare(fixture, target, seed.revision))
    reader = fixture.roles["reader"]
    for schema in (fixture.control_schema, target.schema_name):
        await fixture.connection.execute(f'GRANT USAGE ON SCHEMA "{schema}" TO "{reader}"')
        await fixture.connection.execute(f'GRANT SELECT ON ALL TABLES IN SCHEMA "{schema}" TO "{reader}"')
    await fixture.connection.execute(
        f'UPDATE "{fixture.control_schema}".network_membership_candidate '
        "SET state='ready',index_ready=true,validation_json=jsonb_build_object('leaf_verification',true) WHERE candidate_id=$1",
        UUID(target.candidate_id),
    )
    async with fixture.genuine.reader.transaction(isolation="repeatable_read"):
        assert not await fixture.genuine.reader.fetchval(
            "SELECT has_table_privilege(current_user,$1,'UPDATE')",
            '"' + fixture.control_schema + '".network_membership_candidate',
        )
        relation = await custom.verify_compact_custom_offices(
            fixture.genuine.reader,
            receipt,
            runtime_roles=(reader,),
            control_schema=fixture.control_schema,
            binding_table="provider_location_binding",
        )
    assert relation.table_oid == receipt.address_table_oid


async def test_genuine_mixed_owner_eau_open_candidate_has_only_current_manual_rows(compact_custom_db):
    fixture = compact_custom_db
    seed = await _seed(fixture)
    target = await _target(fixture, seed.revision)
    async with fixture.publisher.transaction(isolation="repeatable_read"):
        identity = await _source_identity(fixture.publisher, fixture.base, (fixture.roles["reader"],), address=True)
        assert identity[1] == await fixture.publisher.fetchval(
            "SELECT to_regclass($1)::oid::bigint", fixture.base.schema_name + ".entity_address_unified"
        )
    receipt = await _prepare(fixture, target, seed.revision)
    assert custom.parse_compact_custom_office_receipt(receipt.as_dict()) == receipt
    assert receipt.custom_pair_count == 1 and receipt.npi_source is None and receipt.npi_identity is None
    async with fixture.publisher.transaction(isolation="repeatable_read"):
        base = await artifact.capture_network_address_artifact(
            fixture.publisher, fixture.base, runtime_roles=(fixture.roles["reader"],)
        )
        assert receipt.base_artifact_sha256 == base["descriptor_sha256"]
    rows = await fixture.publisher.fetch(f'SELECT * FROM "{target.schema_name}".entity_address_unified')
    assert len(rows) == 1 and rows[0]["entity_type"] == "manual" and rows[0]["npi"] is None
    assert rows[0]["entity_id"] == seed.provider["record_id"] and rows[0]["row_origin"] == "network_custom"
    assert (await _verify(fixture, receipt)).table_oid == receipt.address_table_oid
    assert not await fixture.publisher.fetchval(
        "SELECT EXISTS(SELECT FROM pg_class WHERE relnamespace=pg_my_temp_schema() AND relname LIKE 'custom_expected_%')"
    )


async def test_empty_revision_creates_no_custom_heap_and_final_subset_stays_empty(compact_custom_db):
    fixture = compact_custom_db
    target = await _target(fixture, 0)
    receipt = await _prepare(fixture, target, 0)
    assert receipt.custom_pair_count == 0 and receipt.address_table_oid is None and receipt.binding_table_oid is None
    assert await _verify(fixture, receipt) is None
    await fixture.publisher.execute(
        f'CREATE TABLE "{target.schema_name}".provider_location_binding(provider_system text,provider_id text,location_id uuid,location_key text,entity_type text,entity_id text)'
    )
    await fixture.publisher.execute(
        f"INSERT INTO \"{target.schema_name}\".provider_location_binding VALUES('npi','1000000491',$1,'public-key','npi','1000000491')",
        uuid4(),
    )
    oid = await fixture.publisher.fetchval(
        "SELECT to_regclass($1)::oid::bigint", target.schema_name + ".provider_location_binding"
    )
    assert (
        await _verify(fixture, replace(receipt, binding_table_oid=oid), binding_table="provider_location_binding")
        is None
    )
    assert (
        await fixture.publisher.fetchval("SELECT to_regclass($1)", target.schema_name + ".entity_address_unified")
        is None
    )


async def test_final_binding_subset_allows_public_rows_and_refuses_extra_custom_keys(compact_custom_db):
    fixture = compact_custom_db
    seed = await _seed(fixture)
    original = await _prepare(fixture, await _target(fixture, seed.revision), seed.revision)
    receipt = await _merge(fixture, original)
    with pytest.raises(NetworkCustomAddressSourceError, match="binding_identity_changed"):
        await _verify(fixture, original, binding_table="provider_location_binding")
    namespace = '"' + receipt.target.schema_name + '"'
    await fixture.publisher.execute(
        f"INSERT INTO {namespace}.provider_location_binding VALUES('provider_directory','public-provider',$1,'public-key','provider_directory','public-provider')",
        uuid4(),
    )
    assert (
        await _verify(fixture, receipt, binding_table="provider_location_binding")
    ).table_oid == receipt.address_table_oid
    await fixture.publisher.execute(
        f"UPDATE {namespace}.provider_location_binding SET entity_id='conflict' WHERE provider_system='manual'"
    )
    with pytest.raises(NetworkCustomAddressSourceError, match="expected_bindings_differ"):
        await _verify(fixture, receipt, binding_table="provider_location_binding")
    await fixture.publisher.execute(
        f"UPDATE {namespace}.provider_location_binding SET entity_id=provider_id WHERE provider_system='manual'"
    )
    await fixture.publisher.execute(
        f"""INSERT INTO {namespace}.provider_location_binding
        SELECT 'manual',$1,$2,encode(sha256(convert_to(jsonb_build_array('network_custom','manual',$1::text,$2::uuid)::text,'UTF8')),'hex'),'manual',$1""",
        str(uuid4()),
        uuid4(),
    )
    with pytest.raises(NetworkCustomAddressSourceError, match="expected_bindings_differ"):
        await _verify(fixture, receipt, binding_table="provider_location_binding")


@pytest.mark.parametrize(
    "damage", ["row", "binding", "fingerprint", "base_digest", "binding_oid", "address_oid", "target", "revision"]
)
async def test_independent_verifier_refuses_producer_rows_or_forged_receipt(compact_custom_db, damage):
    fixture = compact_custom_db
    seed = await _seed(fixture)
    receipt = await _prepare(fixture, await _target(fixture, seed.revision), seed.revision)
    namespace = '"' + receipt.target.schema_name + '"'
    match damage:
        case "row":
            await fixture.publisher.execute(
                f"UPDATE {namespace}.entity_address_unified SET city_name='Producer supplied city'"
            )
        case "binding":
            await fixture.publisher.execute(
                f"UPDATE {namespace}.custom_provider_location_binding SET entity_id='other-provider'"
            )
        case "fingerprint":
            receipt = replace(receipt, approved_records_sha256="b" * 64)
        case "base_digest":
            receipt = replace(receipt, base_artifact_sha256="b" * 64)
        case "binding_oid":
            receipt = replace(receipt, binding_table_oid=receipt.binding_table_oid + 1)
        case "address_oid":
            receipt = replace(receipt, address_table_oid=receipt.address_table_oid + 1)
        case "target":
            receipt = replace(receipt, target=replace(receipt.target, producer_id=str(uuid4())))
        case _:
            receipt = replace(receipt, approved_revision=0)
    with pytest.raises(ValueError):
        await _verify(fixture, receipt)
    assert not await fixture.publisher.fetchval(
        "SELECT EXISTS(SELECT FROM pg_class WHERE relnamespace=pg_my_temp_schema() AND relname LIKE 'custom_expected_%')"
    )


async def test_receipt_fields_and_native_identity_types_are_closed(compact_custom_db):
    fixture = compact_custom_db
    seed = await _seed(fixture)
    receipt = await _prepare(fixture, await _target(fixture, seed.revision), seed.revision)
    original = receipt.as_dict()
    for name, value in [
        ("revision", True),
        ("revision", 2),
        ("approved_revision", -1),
        ("custom_pair_count", True),
        ("custom_pair_count", 0),
        ("address_table_oid", 0),
        ("binding_table_oid", None),
        ("base_artifact_sha256", "A" * 64),
        ("runtime_roles", [fixture.roles["reader"]] * 2),
        ("control_schema", "bad.schema"),
        ("extra", True),
        ("approved_records_sha256", "x" * (256 * 1024)),
    ]:
        altered = deepcopy(original)
        altered[name] = value
        with pytest.raises(ValueError):
            custom.parse_compact_custom_office_receipt(altered)
    for name in ["target", "base_source"]:
        altered = deepcopy(original)
        altered[name]["unknown"] = True
        with pytest.raises(ValueError):
            custom.parse_compact_custom_office_receipt(altered)


async def test_current_revision_removal_drops_direct_contribution_without_base_copy(compact_custom_db):
    fixture = compact_custom_db
    seed = await _seed(fixture)
    old = await _prepare(fixture, await _target(fixture, seed.revision), seed.revision)
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
    new = await _prepare(fixture, await _target(fixture, revision), revision)
    assert new.custom_pair_count == 0 and new.address_table_oid is None
    with pytest.raises(NetworkCustomAddressSourceError, match="Approved revision differs"):
        await _verify(fixture, old)


async def test_retained_site_pair_requires_separate_carrier(compact_custom_db):
    fixture = compact_custom_db
    seed = await _seed(fixture)
    location = str(uuid4())
    await fixture.connection.execute(
        f'''UPDATE "{fixture.control_schema}".registry_approved_record
        SET record_json=jsonb_set(record_json,'{{memberships_json}}',record_json->'memberships_json'||jsonb_build_array(jsonb_build_object('network_id',$1::integer,'provider_system','provider_directory','provider_id','source-provider','location_id',$2::text,'evidence_id','source-site'))) WHERE record_kind='membership' ''',
        int(seed.network["record_id"]),
        location,
    )
    await fixture.connection.execute(
        f'''INSERT INTO "{fixture.control_schema}".registry_approved_record
        (approved_revision,record_kind,record_key,record_revision,custom_revision,record_json) VALUES($1,'site_binding',$2,1,1,jsonb_build_object('archived',false,'provider_system','provider_directory','provider_id','source-provider','location_id',$3::text))''',
        seed.revision,
        str(uuid4()),
        location,
    )
    receipt = await _prepare(fixture, await _target(fixture, seed.revision), seed.revision)
    assert receipt.custom_pair_count == 1
    bindings = await fixture.publisher.fetch(
        f'SELECT provider_system,provider_id FROM "{receipt.target.schema_name}".custom_provider_location_binding'
    )
    assert [tuple(binding) for binding in bindings] == [("manual", seed.provider["record_id"])]
    assert (await _verify(fixture, receipt)).table_oid == receipt.address_table_oid
    await fixture.publisher.execute(
        f'INSERT INTO "{receipt.target.schema_name}".custom_provider_location_binding '
        "VALUES('provider_directory','source-provider',$1,'retained-key','provider_directory','source-provider')",
        UUID(location),
    )
    with pytest.raises(NetworkCustomAddressSourceError, match="expected_bindings_differ"):
        await _verify(fixture, receipt)


async def test_explicit_protected_npi_source_is_pinned_and_presence_is_rederived(compact_custom_db):
    fixture = compact_custom_db
    seed = await _seed(fixture, system="npi")
    schema = "custom_npi_" + uuid4().hex
    try:
        await fixture.connection.execute(f'CREATE SCHEMA "{schema}" AUTHORIZATION "{fixture.roles["owner"]}"')
        await fixture.connection.execute(f'CREATE TABLE "{schema}".npi(npi bigint PRIMARY KEY)')
        await fixture.connection.execute(f'ALTER TABLE "{schema}".npi OWNER TO "{fixture.roles["owner"]}"')
        await fixture.connection.execute(f'GRANT USAGE ON SCHEMA "{schema}" TO "{fixture.roles["reader"]}"')
        await fixture.connection.execute(f'GRANT SELECT ON "{schema}".npi TO "{fixture.roles["reader"]}"')
        await fixture.connection.execute(f'INSERT INTO "{schema}".npi VALUES(1000000004)')
        source = PinnedAddressSource(schema, "npi", "npi_edition_1")
        receipt = await _prepare(fixture, await _target(fixture, seed.revision), seed.revision, npi_source=source)
        assert receipt.npi_source == source and receipt.npi_identity[1] > 0
        assert custom.parse_compact_custom_office_receipt(receipt.as_dict()) == receipt
        assert (await _verify(fixture, receipt)).table_oid == receipt.address_table_oid
        await fixture.connection.execute(f'DELETE FROM "{schema}".npi')
        with pytest.raises(NetworkCustomAddressSourceError, match="NPI identity is unresolved"):
            await _verify(fixture, receipt)
    finally:
        await fixture.connection.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


async def test_native_npi_family_is_used_and_never_falls_back_to_legacy(compact_custom_db, npi_db):
    from process.network_npi_artifact import NetworkNPIArtifactError, capture_network_npi_artifact

    fixture = compact_custom_db
    seed = await _seed(fixture, system="npi")
    async with fixture.publisher.transaction(isolation="repeatable_read"):
        source, identity = await capture_network_npi_artifact(
            fixture.publisher, npi_db.schema, runtime_roles=(fixture.roles["reader"],)
        )
    receipt = await _prepare(fixture, await _target(fixture, seed.revision), seed.revision, npi_source=source)
    assert (receipt.npi_source, receipt.npi_identity) == (source, identity)
    assert source.generation_id != fixture.base.generation_id
    assert (await _verify(fixture, receipt)).table_oid == receipt.address_table_oid
    with pytest.raises(NetworkCustomAddressSourceError, match="npi_generation_changed"):
        await _prepare(
            fixture,
            await _target(fixture, seed.revision),
            seed.revision,
            npi_source=replace(source, generation_id=fixture.base.generation_id),
        )
    await fixture.connection.execute(f'DROP TABLE "{npi_db.schema}".npi_taxonomy')
    with pytest.raises(NetworkNPIArtifactError):
        await _verify(fixture, receipt)


async def test_mixed_owner_npi_requires_a_separate_verified_family_capability(compact_custom_db):
    fixture = compact_custom_db
    seed = await _seed(fixture, system="npi")
    target = await _target(fixture, seed.revision)
    with pytest.raises(NetworkCustomAddressSourceError, match="unprotected"):
        await _prepare(fixture, target, seed.revision)
    assert (
        await fixture.publisher.fetchval("SELECT to_regclass($1)", target.schema_name + ".entity_address_unified")
        is None
    )


async def test_public_collision_rolls_back_the_small_contribution(compact_custom_db):
    fixture = compact_custom_db
    seed = await _seed(fixture)
    target = await _target(fixture, seed.revision)
    await fixture.connection.execute(
        f'''UPDATE "{fixture.source_schema}".entity_address_unified SET location_key=
        encode(sha256(convert_to(jsonb_build_array('network_custom','manual',$1::text,$2::uuid)::text,'UTF8')),'hex')''',
        seed.provider["record_id"],
        UUID(seed.first["record_id"]),
    )
    with pytest.raises(NetworkCustomAddressSourceError, match="collides"):
        await _prepare(fixture, target, seed.revision)
    for name in ("entity_address_unified", "custom_provider_location_binding"):
        assert await fixture.publisher.fetchval("SELECT to_regclass($1)", target.schema_name + "." + name) is None
