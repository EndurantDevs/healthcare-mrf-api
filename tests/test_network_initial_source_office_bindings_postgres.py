# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact ACA/EUA correspondence using real closed archives and native codecs."""

import asyncio
import hashlib
import importlib
import json
from dataclasses import asdict, replace
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest
from sqlalchemy import MetaData, text
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import EntityAddressUnified
from process import reference_family_archive as archive
from process.network_address_projection import PinnedAddressSource
from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
from process.network_initial_source_office_bindings import (
    InitialACAOfficeBindingError,
    copy_initial_aca_office_bindings,
    pin_aca_office_address_source,
)
from process.network_legacy_membership_source import PinnedACAMembershipSource, read_aca_membership_batch
from process.network_membership_candidate_lifecycle import create_network_candidate
from process.network_membership_copy import MembershipCopyTarget
from tests.cms_registry_recipe_postgres_support import cms_recipe_database as cms_recipe_database
from tests.cms_registry_recipe_postgres_support import cms_recipe_template as cms_recipe_template
from tests.cms_registry_recipe_postgres_support import reviewed_cms_source as reviewed_cms_source
from tests.cms_registry_recipe_postgres_support import serving_schema as serving_schema
from tests.test_network_custom_address_source_postgres import custom_db as custom_db
from tests.test_network_legacy_membership_source_postgres import (
    _aca_binding,
    _approved_aca_pin,
    _insert_evidence,
    _insert_reviewed_lineage,
    _seal_reviewed_archive,
    _write_aca_bindings,
)
from tests.test_registry_approval_store_postgres import _actor, _approve, _command, _CountedConnection, _create, _draft

pytestmark = pytest.mark.asyncio


class CountedOfficeConnection(_CountedConnection):
    async def fetch(self, *arguments):
        self.statements += 1
        return await self.connection.fetch(*arguments)

    async def copy_records_to_table(self, *arguments, **options):
        self.statements += 1
        return await self.connection.copy_records_to_table(*arguments, **options)


def _office_fixture(serving_schema, count):
    import ptg2_address_canon

    connection, control, engine = serving_schema
    scope_token, dataset_id = uuid4().hex, uuid4()
    source_schema = archive.reference_family_stage_schema(dataset_id)
    address_rows = [("123 Example Street", f"Suite {unit}", "Sample City", "CA", "90210", "US") for unit in (2, 3)]
    canonical_addresses = ptg2_address_canon.canonicalize_batch(address_rows)
    reader = "office_reader_" + scope_token
    return SimpleNamespace(
        connection=connection,
        control=control,
        engine=engine,
        count=count,
        coordinates=RegistryNetworkSourceCoordinates(
            "aca", "source-example", source_schema, str(dataset_id), "producer-example", "edition-example"
        ),
        address_schema="office_address_" + scope_token,
        owner="office_owner_" + scope_token,
        reader=reader,
        targets=[],
        raw=address_rows,
        canonical=canonical_addresses,
        sites=[UUID(canonical_address["address_key"]) for canonical_address in canonical_addresses],
        source_metadata_by_field={
            "source_id": "source-example",
            "release_id": "release-one",
            "import_id": "import-one",
            "issuer_id": 7,
            "year": 2026,
            "source_url": "https://source.example.test/providers",
            "alias_scope": "medical",
            "reviewed_alias_sha256": "a" * 64,
            "runtime_roles": (reader,),
        },
    )


@pytest.fixture
async def office_db(serving_schema, request):
    """Register exact isolated schema/role cleanup before any native creation."""
    fixture = _office_fixture(serving_schema, getattr(request, "param", 2))
    try:
        prepared, validation = await _archive(fixture)
        fixture.source = PinnedACAMembershipSource(prepared, validation, **fixture.source_metadata_by_field)
        await _prepare_addresses(fixture)
        fixture.batch = await _reviewed_batch(fixture)
        fixture.address_source = PinnedAddressSource(
            fixture.address_schema, "entity_address_unified", "exact-address-edition"
        )
        async with fixture.connection.transaction(isolation="repeatable_read"):
            fixture.address_pin = await pin_aca_office_address_source(
                fixture.connection,
                fixture.batch,
                fixture.address_source,
                runtime_roles=(fixture.reader,),
                control_schema=fixture.control,
            )
        yield fixture
    finally:
        await _cleanup_offices(fixture)


async def _prepare_addresses(fixture):
    connection, schema = fixture.connection, fixture.address_schema
    await connection.execute(f'CREATE SCHEMA "{schema}"')
    async with fixture.engine.begin() as setup:
        table = EntityAddressUnified.__table__.to_metadata(MetaData(), schema=schema)
        await setup.run_sync(lambda sync: table.create(sync))
    for index, address in enumerate(fixture.raw):
        await connection.execute(
            f'''INSERT INTO "{schema}".entity_address_unified
          (location_key,entity_type,entity_id,npi,checksum,type,first_line,second_line,city_name,state_name,
           postal_code,country_code,address_key,address_precision,premise_key)
          VALUES($1,'npi','1000000491',1000000491,$2,'practice',$3,$4,$5,$6,$7,$8,$9,'street',$10)''',
            str(index + 1) * 64,
            index + 1,
            *address,
            fixture.sites[index],
            UUID(fixture.canonical[index]["premise_key"]),
        )
    await connection.execute(f'ALTER SCHEMA "{schema}" OWNER TO "{fixture.owner}"')
    await connection.execute(f'ALTER TABLE "{schema}".entity_address_unified OWNER TO "{fixture.owner}"')
    await connection.execute(f'GRANT USAGE ON SCHEMA "{schema}" TO "{fixture.reader}"')
    await connection.execute(f'GRANT SELECT ON "{schema}".entity_address_unified TO "{fixture.reader}"')


async def _reviewed_batch(fixture):
    connection, control = fixture.connection, fixture.control
    actor = _actor()
    network = await _draft(fixture.engine, control, _create("network"), actor)
    binding_receipt = await _write_aca_bindings(
        connection, control, actor, [_aca_binding(fixture.coordinates, network["record_id"], index) for index in (1, 2)]
    )
    await _approve(
        connection, control, await _command(connection, control, network, *binding_receipt["records"]), actor
    )
    approved = await _approved_aca_pin(connection, control)
    async with connection.transaction(isolation="repeatable_read"):
        return await read_aca_membership_batch(
            connection,
            fixture.source,
            registry_schema=control,
            limit=fixture.count,
            approved_source=approved,
            binding_coordinates=fixture.coordinates,
        )


async def _cleanup_offices(fixture):
    connection = fixture.connection
    await connection.execute("RESET ROLE")
    for copy_target in fixture.targets:
        await connection.execute(f'DROP SCHEMA IF EXISTS "{copy_target.schema_name}" CASCADE')
        assert await connection.fetchval("SELECT to_regnamespace($1)", copy_target.schema_name) is None
    for schema in (fixture.address_schema, fixture.coordinates.dataset_schema):
        await connection.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        assert await connection.fetchval("SELECT to_regnamespace($1)", schema) is None
    for role in (fixture.reader, fixture.owner):
        if await connection.fetchval("SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname=$1)", role):
            await connection.execute(f'DROP OWNED BY "{role}"')
            await connection.execute(f'DROP ROLE "{role}"')
        assert not await connection.fetchval("SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname=$1)", role)


async def _archive(fixture):
    engine, coordinates = fixture.engine, fixture.coordinates
    metadata, owner, reader = fixture.source_metadata_by_field, fixture.owner, fixture.reader
    count, sites, raw, canonical = fixture.count, fixture.sites, fixture.raw, fixture.canonical
    schema = coordinates.dataset_schema
    async with AsyncSession(engine) as session, session.begin():
        await archive._create_model_family(session, archive.reference_family_spec("mrf"), schema)
        entries = [(index, 42 if index % 2 else 43, sites[(index - 1) % 2]) for index in range(1, count + 1)]
        await _insert_evidence(session, schema, entries)
        await _insert_reviewed_lineage(session, schema)
        for index, address in enumerate(raw):
            await session.execute(
                text(f'''UPDATE "{schema}".mrf_address_evidence SET
              first_line=:first,second_line=:second,city_name=:city,state_name=:state,postal_code=:zip,country_code=:country
              WHERE address_key=:key'''),
                dict(zip(("first", "second", "city", "state", "zip", "country"), address)) | {"key": sites[index]},
            )
            canonical_payload = canonical[index] | {
                "identity_version": 2,
                "precision": "street",
                "source_bits": 16,
                "merged_into": None,
            }
            await session.execute(
                text(f'INSERT INTO "{schema}".mrf_canonical_address VALUES(:key,CAST(:payload AS jsonb))'),
                {"key": sites[index], "payload": json.dumps(canonical_payload)},
            )
        manifest = await archive._family_manifest(
            session,
            spec=archive.reference_family_spec("mrf"),
            schema_name=schema,
            source_metadata={
                "network_membership": metadata,
                "network_bindings": {**asdict(coordinates), "source_key_kind": "hios_plan_id"},
            },
            dependencies={"plan-attributes": "b" * 64},
            auxiliary={"archive_name": archive.archive_table_name(), "publication_sha256": "a" * 64},
        )
        ownership = await archive.capture_reference_family_stage_ownership(
            session, importer_id="mrf", dataset_id=UUID(coordinates.dataset_id)
        )
        owner_oid = await _seal_reviewed_archive(session, ownership, owner, reader)
        validation = await archive.prepare_reference_family_activation(
            session,
            ownership=ownership,
            manifest=manifest,
            package_id=hashlib.sha256(json.dumps(entries, default=str).encode()).hexdigest(),
            profile_contract=archive.CONTRACT,
            sealed_owner_oid=owner_oid,
        )
    return archive.ReferenceFamilyPreparedSource(manifest, ownership), validation


async def _target(fixture, **changes):
    candidate = uuid4()
    copy_target = MembershipCopyTarget(
        *(str(uuid4()) for _ in range(3)), str(candidate), "network_candidate_" + candidate.hex
    )
    fixture.targets.append(copy_target)
    await create_network_candidate(
        fixture.connection,
        copy_target,
        source_generations={
            "aca": fixture.batch.generation_id,
            "custom_membership": fixture.batch.approved_source.generation_id,
            "unified_address": fixture.address_source.generation_id,
        },
        approved_custom_revision=fixture.batch.approved_source.approved_revision,
        expected_head=0,
        expected_rows=fixture.batch.membership_rows,
        control_schema=fixture.control,
        **changes,
    )
    return copy_target


async def _copy(fixture, copy_target, **changes):
    return await copy_initial_aca_office_bindings(
        fixture.connection,
        fixture.batch,
        copy_target,
        fixture.address_pin,
        runtime_roles=(fixture.reader,),
        control_schema=fixture.control,
        **changes,
    )


async def test_exact_two_units_retry_and_caller_rollback(office_db):
    fixture = office_db
    async with fixture.connection.transaction(isolation="repeatable_read"):
        copy_target = await _target(fixture)
        first = await _copy(fixture, copy_target)
        assert first == await _copy(fixture, copy_target)
        assert first["office_count"] == 2
        rows = await fixture.connection.fetch(
            f'SELECT * FROM "{copy_target.schema_name}".provider_location_binding ORDER BY location_id'
        )
        assert {row["location_id"] for row in rows} == set(fixture.sites)
        assert {row["location_key"] for row in rows} == {"1" * 64, "2" * 64}
        assert first["aca_source"]["canonical_address_oid"] == fixture.source.prepared.ownership.auxiliary_oid
        assert first["address_identity"][1] == await fixture.connection.fetchval(
            "SELECT $1::regclass::oid", fixture.address_source.schema_name + ".entity_address_unified"
        )
    with pytest.raises(RuntimeError, match="rollback"):
        async with fixture.connection.transaction(isolation="repeatable_read"):
            discarded = await _target(fixture)
            await _copy(fixture, discarded)
            raise RuntimeError("rollback")
    assert await fixture.connection.fetchval("SELECT to_regnamespace($1)", discarded.schema_name) is None


@pytest.mark.parametrize("damage", ["unit", "missing", "duplicate", "coarse", "inferred", "alias", "writable", "owner"])
async def test_whole_batch_denied_without_candidate_binding_writes(office_db, damage):
    fixture = office_db
    namespace = '"' + fixture.address_source.schema_name + '"'
    assignments_by_damage = {
        "unit": "second_line='Suite 9'",
        "coarse": "address_precision='city_zip'",
        "inferred": "inferred_npi=1000000491",
        "alias": "address_key=gen_random_uuid()",
    }
    if damage in assignments_by_damage:
        await fixture.connection.execute(
            f"UPDATE {namespace}.entity_address_unified SET {assignments_by_damage[damage]} WHERE location_key=$1",
            "2" * 64,
        )
    elif damage == "missing":
        await fixture.connection.execute(
            f"DELETE FROM {namespace}.entity_address_unified WHERE location_key=$1", "2" * 64
        )
    elif damage == "duplicate":
        await fixture.connection.execute(
            f"""INSERT INTO {namespace}.entity_address_unified SELECT
          (jsonb_populate_record(NULL::{namespace}.entity_address_unified,to_jsonb(a)||jsonb_build_object('location_key',$1::text))).*
          FROM {namespace}.entity_address_unified a WHERE location_key=$2""",
            "3" * 64,
            "2" * 64,
        )
    elif damage == "writable":
        await fixture.connection.execute(f'GRANT UPDATE ON {namespace}.entity_address_unified TO "{fixture.reader}"')
    else:
        await fixture.connection.execute(f'ALTER ROLE "{fixture.owner}" LOGIN')
    async with fixture.connection.transaction(isolation="repeatable_read"):
        copy_target = await _target(fixture)
        with pytest.raises(ValueError):
            await _copy(fixture, copy_target)
        assert (
            await fixture.connection.fetchval(
                "SELECT to_regclass($1)", copy_target.schema_name + ".provider_location_binding"
            )
            is None
        )


@pytest.mark.parametrize("office_db", [1, 5000], indirect=True)
async def test_fixed_bulk_query_count_native_batch_and_hash_receipt(office_db):
    fixture = office_db
    async with fixture.connection.transaction(isolation="repeatable_read"):
        copy_target = await _target(fixture)
        counted = CountedOfficeConnection(fixture.connection)
        receipt = await copy_initial_aca_office_bindings(
            counted,
            fixture.batch,
            copy_target,
            fixture.address_pin,
            runtime_roles=(fixture.reader,),
            control_schema=fixture.control,
        )
        assert counted.statements == 18
        assert receipt["office_count"] == min(fixture.batch.source_rows, 2)
        assert len(receipt["selected_offices_sha256"]) == 64


@pytest.mark.parametrize("damage", ["batch", "source_oid", "head_pin", "closed"])
async def test_exact_batch_source_and_candidate_pins_are_required(office_db, damage):
    fixture = office_db
    async with fixture.connection.transaction(isolation="repeatable_read"):
        copy_target = await _target(fixture)
        if damage == "batch":
            fixture.batch = replace(fixture.batch, membership_rows=fixture.batch.membership_rows + 1)
        elif damage == "source_oid":
            fixture.batch = replace(
                fixture.batch,
                source=replace(
                    fixture.source,
                    prepared=replace(
                        fixture.source.prepared, ownership=replace(fixture.source.prepared.ownership, auxiliary_oid=1)
                    ),
                ),
            )
        else:
            assignment = "source_generations='{}'::jsonb" if damage == "head_pin" else "state='sealed'"
            await fixture.connection.execute(
                f'UPDATE "{fixture.control}".network_membership_candidate SET {assignment} WHERE candidate_id=$1',
                UUID(copy_target.candidate_id),
            )
        with pytest.raises(ValueError):
            await _copy(fixture, copy_target)
        assert (
            await fixture.connection.fetchval(
                "SELECT to_regclass($1)", copy_target.schema_name + ".provider_location_binding"
            )
            is None
        )


@pytest.mark.parametrize("damage", ["row_hash", "heap_oid"])
async def test_native_pin_rejects_same_label_changed_rows_or_heap(office_db, damage):
    fixture = office_db
    namespace = '"' + fixture.address_source.schema_name + '"'
    if damage == "row_hash":
        await fixture.connection.execute(f"UPDATE {namespace}.entity_address_unified SET telephone_number='5550100'")
    else:
        await fixture.connection.execute(f"ALTER TABLE {namespace}.entity_address_unified RENAME TO old_addresses")
        await fixture.connection.execute(
            f"CREATE TABLE {namespace}.entity_address_unified (LIKE {namespace}.old_addresses INCLUDING ALL)"
        )
        await fixture.connection.execute(
            f"INSERT INTO {namespace}.entity_address_unified SELECT * FROM {namespace}.old_addresses"
        )
        await fixture.connection.execute(f'ALTER TABLE {namespace}.entity_address_unified OWNER TO "{fixture.owner}"')
        await fixture.connection.execute(f'GRANT SELECT ON {namespace}.entity_address_unified TO "{fixture.reader}"')
    async with fixture.connection.transaction(isolation="repeatable_read"):
        copy_target = await _target(fixture)
        with pytest.raises(InitialACAOfficeBindingError, match="rows_differ|native_identity_differs"):
            await _copy(fixture, copy_target)
        assert (
            await fixture.connection.fetchval(
                "SELECT to_regclass($1)", copy_target.schema_name + ".provider_location_binding"
            )
            is None
        )


@pytest.mark.parametrize("failure", [RuntimeError, asyncio.CancelledError])
async def test_late_binary_copy_fault_restores_candidate_and_temp_tables(office_db, failure):
    fixture = office_db

    class FailingCopyConnection(CountedOfficeConnection):
        async def copy_records_to_table(self, *arguments, **options):
            await super().copy_records_to_table(*arguments, **options)
            raise failure("copy interrupted")

    async with fixture.connection.transaction(isolation="repeatable_read"):
        copy_target = await _target(fixture)
        with pytest.raises(failure):
            await copy_initial_aca_office_bindings(
                FailingCopyConnection(fixture.connection),
                fixture.batch,
                copy_target,
                fixture.address_pin,
                runtime_roles=(fixture.reader,),
                control_schema=fixture.control,
            )
        assert (
            await fixture.connection.fetchval(
                "SELECT to_regclass($1)", copy_target.schema_name + ".provider_location_binding"
            )
            is None
        )
        assert not await fixture.connection.fetchval(
            "SELECT EXISTS(SELECT 1 FROM pg_class WHERE relnamespace=pg_my_temp_schema() AND relname LIKE 'initial_aca_offices_%')"
        )


async def test_unprivileged_reader_can_copy_only_candidate_offices(office_db):
    fixture = office_db
    async with fixture.connection.transaction(isolation="repeatable_read"):
        copy_target = await _target(fixture)
        await fixture.connection.execute(f'GRANT USAGE ON SCHEMA "{fixture.control}" TO "{fixture.reader}"')
        await fixture.connection.execute(
            f'GRANT SELECT ON ALL TABLES IN SCHEMA "{fixture.control}" TO "{fixture.reader}"'
        )
        await fixture.connection.execute(
            f'GRANT UPDATE ON "{fixture.control}".network_membership_candidate TO "{fixture.reader}"'
        )
        await fixture.connection.execute(f'ALTER SCHEMA "{copy_target.schema_name}" OWNER TO "{fixture.reader}"')
        await fixture.connection.execute(
            f'ALTER TABLE "{copy_target.schema_name}".network_membership OWNER TO "{fixture.reader}"'
        )
        await fixture.connection.execute(f'SET LOCAL ROLE "{fixture.reader}"')
        assert not await fixture.connection.fetchval("SELECT pg_has_role(current_user,$1,'MEMBER')", fixture.owner)
        receipt = await _copy(fixture, copy_target)
        assert receipt["office_count"] == 2
        assert not await fixture.connection.fetchval(
            "SELECT has_table_privilege(current_user,$1,'INSERT,UPDATE,DELETE,TRUNCATE')",
            fixture.address_source.schema_name + ".entity_address_unified",
        )


async def test_read_committed_rejected_and_empty_page_has_no_office_fallback(office_db):
    fixture = office_db
    async with fixture.connection.transaction():
        copy_target = await _target(fixture)
        with pytest.raises(ValueError, match="repeatable"):
            await _copy(fixture, copy_target)
    after = fixture.batch.next_evidence_checksum
    async with fixture.connection.transaction(isolation="repeatable_read"):
        fixture.batch = await read_aca_membership_batch(
            fixture.connection,
            fixture.source,
            registry_schema=fixture.control,
            after_evidence_checksum=after,
            approved_source=fixture.batch.approved_source,
            binding_coordinates=fixture.batch.binding_coordinates,
        )
        fixture.address_pin = await pin_aca_office_address_source(
            fixture.connection,
            fixture.batch,
            fixture.address_source,
            runtime_roles=(fixture.reader,),
            after_evidence_checksum=after,
            control_schema=fixture.control,
        )
        copy_target = await _target(fixture)
        receipt = await _copy(fixture, copy_target, after_evidence_checksum=after)
        assert receipt["office_count"] == 0
        assert (
            await fixture.connection.fetchval(
                f'SELECT count(*) FROM "{copy_target.schema_name}".provider_location_binding'
            )
            == 0
        )


@pytest.mark.parametrize("count", [True, False, 1, 3])
async def test_changed_or_boolean_pinned_office_count_rejected(office_db, count):
    fixture = office_db
    fixture.address_pin = replace(fixture.address_pin, office_count=count)
    async with fixture.connection.transaction(isolation="repeatable_read"):
        copy_target = await _target(fixture)
        with pytest.raises(InitialACAOfficeBindingError, match="source_pin_differs|count_differs"):
            await _copy(fixture, copy_target)
        assert (
            await fixture.connection.fetchval(
                "SELECT to_regclass($1)", copy_target.schema_name + ".provider_location_binding"
            )
            is None
        )


async def test_existing_conflict_preserved_without_partial_second_office(office_db):
    fixture = office_db
    async with fixture.connection.transaction(isolation="repeatable_read"):
        copy_target = await _target(fixture)
        namespace = '"' + copy_target.schema_name + '"'
        await fixture.connection.execute(f"""CREATE TABLE {namespace}.provider_location_binding(
          provider_system text NOT NULL,provider_id text NOT NULL,location_id uuid NOT NULL,
          location_key varchar(64) NOT NULL,entity_type text NOT NULL,entity_id text NOT NULL,
          PRIMARY KEY(provider_system,provider_id,location_id))""")
        await fixture.connection.execute(
            f"INSERT INTO {namespace}.provider_location_binding VALUES('npi','1000000491',$1,$2,'npi','1000000491')",
            fixture.sites[0],
            "f" * 64,
        )
        with pytest.raises(InitialACAOfficeBindingError, match="binding_conflict"):
            await _copy(fixture, copy_target)
        binding_rows = await fixture.connection.fetch(f"SELECT * FROM {namespace}.provider_location_binding")
        assert len(binding_rows) == 1 and binding_rows[0]["location_key"] == "f" * 64


async def test_input_and_office_proof_bounds_prevent_copy(office_db):
    fixture = office_db
    original = fixture.batch
    oversized = b" " * (8 * 1024 * 1024 + 1)
    fixture.batch = replace(original, input_bytes=oversized, input_sha256=hashlib.sha256(oversized).hexdigest())
    async with fixture.connection.transaction(isolation="repeatable_read"):
        copy_target = await _target(fixture)
        with pytest.raises(InitialACAOfficeBindingError, match="input_invalid"):
            await _copy(fixture, copy_target)
    fixture.batch = original
    source_namespace = '"' + fixture.source.prepared.ownership.schema_name + '"'
    await fixture.connection.execute(
        f"UPDATE {source_namespace}.mrf_canonical_address SET payload=payload||jsonb_build_object('extra',repeat('x',8388609)) WHERE address_key=$1",
        fixture.sites[0],
    )
    async with fixture.connection.transaction(isolation="repeatable_read"):
        copy_target = await _target(fixture)
        with pytest.raises(InitialACAOfficeBindingError, match="unresolved"):
            await _copy(fixture, copy_target)
        assert (
            await fixture.connection.fetchval(
                "SELECT to_regclass($1)", copy_target.schema_name + ".provider_location_binding"
            )
            is None
        )


@pytest.fixture
def cms_full_office_source(monkeypatch, request):
    """Set full native source-office address evidence before acquisition hashes."""
    from tests import cms_registry_recipe_postgres_support as cms_fixture

    original = cms_fixture._membership_resource_rows

    def full_address(resources, revision):
        resource_rows = original(resources, revision)
        provider_system = getattr(request, "param", "npi")
        if provider_system == "provider_directory":
            resource_rows["Practitioner"][0].pop("identifier", None)
        elif provider_system == "invalid_npi":
            resource_rows["Practitioner"][0]["identifier"][0]["value"] = "malformed"
        resource_rows["Location"][0]["address"] = {
            "line": ["123 Example Street", "Suite 2"],
            "city": "Sample City",
            "state": "CA",
            "postalCode": "90210",
            "country": "US",
        }
        return resource_rows

    monkeypatch.setattr(cms_fixture, "_membership_resource_rows", full_address)
    return getattr(request, "param", "npi")


@pytest.fixture
async def initial_cms_db(cms_full_office_source, reviewed_cms_source, custom_db):
    """Keep one protected typed source office and the same immutable NPI base."""
    import ptg2_address_canon

    fixture = custom_db
    from process.network_cms_provider_identity import CMSProviderIdentity

    fixture.cms_provider_system = "provider_directory" if cms_full_office_source == "provider_directory" else "npi"
    fixture.cms_provider_id = (
        CMSProviderIdentity("cms-npd", "Practitioner", "1234567893").provider_id
        if fixture.cms_provider_system == "provider_directory"
        else "1000000491"
    )
    raw = ("123 Example Street", "Suite 2", "Sample City", "CA", "90210", "US")
    canonical = ptg2_address_canon.canonicalize_batch([raw])[0]
    await fixture.connection.execute(
        f'''UPDATE "{fixture.source_schema}".entity_address_unified SET entity_id=$7,npi=$8,entity_type=$9,
        second_line=$1,address_key=$2,premise_key=$3,address_precision='street',zip5=$4,state_code=$5,city_norm=$6''',
        raw[1],
        UUID(canonical["address_key"]),
        UUID(canonical["premise_key"]),
        canonical["zip5"],
        canonical["state_code"],
        canonical["city_norm"],
        fixture.cms_provider_id,
        1000000491 if fixture.cms_provider_system == "npi" else None,
        fixture.cms_provider_system,
    )
    await fixture.connection.execute(f'UPDATE "{fixture.source_schema}".npi SET npi=1000000491')
    database_name = await fixture.connection.fetchval("SELECT quote_ident(current_database())")
    await fixture.connection.execute(f'GRANT CREATE ON DATABASE {database_name} TO "{fixture.roles["owner"]}"')
    fixture.reviewed_cms = reviewed_cms_source
    yield fixture


async def _cms_initial_arguments(fixture):
    from process.network_approved_membership_source import pin_approved_membership_source
    from process.registry_candidate_composition import RegistryCompositionAddressSources
    from process.registry_source_recipe_store import RegistrySourceMembershipRecipe
    from tests.test_network_custom_address_source_postgres import _seed

    seed = await _seed(fixture)
    source_fixture, _, _, _, coordinates, _ = fixture.reviewed_cms
    request_id = uuid4()
    async with fixture.connection.transaction(isolation="repeatable_read"):
        approved = await pin_approved_membership_source(
            fixture.connection, approved_revision=seed.revision, control_schema=fixture.control_schema
        )
    from uuid import uuid5

    fixture.composition_ids.append(uuid5(request_id, "address:" + approved.generation_id))
    return seed, {
        "request_id": request_id,
        "approved_revision": seed.revision,
        "expected_head": 0,
        "address_sources": RegistryCompositionAddressSources(
            fixture.base,
            npi_source=PinnedAddressSource(fixture.source_schema, "npi", "same-npi-base"),
            initial_source_recipes=(RegistrySourceMembershipRecipe(source_fixture[2], coordinates),),
        ),
        "writer_roles": {
            "owner_role": fixture.roles["owner"],
            "loader_roles": (),
            "reader_roles": (fixture.roles["reader"],),
        },
        "control_schema": fixture.control_schema,
    }


async def _assert_cms_native_read(fixture, manifest, network_id):
    from api.network_address_scope import network_address_read_scope
    from api.network_provider_read import read_network_provider_detail

    reader = fixture.roles["reader"]
    await fixture.connection.execute(f'GRANT USAGE ON SCHEMA "{fixture.control_schema}" TO "{reader}"')
    await fixture.connection.execute(f'GRANT SELECT ON ALL TABLES IN SCHEMA "{fixture.control_schema}" TO "{reader}"')
    async with fixture.connection.transaction(isolation="repeatable_read", readonly=True):
        await fixture.connection.execute(f'SET LOCAL ROLE "{reader}"')
        with network_address_read_scope(manifest, (network_id,)) as scope:
            detail = await read_network_provider_detail(
                fixture.connection,
                scope,
                provider_system=fixture.cms_provider_system,
                provider_id=fixture.cms_provider_id,
                control_schema=fixture.control_schema,
            )
        locations = detail["provider"]["locations"]
        assert len(locations) == 1 and locations[0]["location_id"] == str(fixture.reviewed_cms[0][3][0])


@pytest.mark.parametrize("cms_full_office_source", ["npi", "provider_directory"], indirect=True)
async def test_initial_cms_exact_office_manual_publish_and_replay(initial_cms_db):
    from process.network_membership_pipeline import prepare_and_publish_network_candidate
    from process.network_serving_read import resolve_network_serving_manifest
    from process.registry_candidate_composition import compose_registry_membership_candidate
    from process.registry_initial_source_composition import verify_initial_source_office_receipt
    from tests.test_registry_candidate_composition_postgres import _remove_candidates

    fixture = initial_cms_db
    seed, arguments = await _cms_initial_arguments(fixture)
    copy_targets = []
    try:
        copy_target, addresses = await compose_registry_membership_candidate(fixture.connection, **arguments)
        copy_targets.append(copy_target)
        candidate_by_field = dict(
            await fixture.connection.fetchrow(
                f'SELECT * FROM "{fixture.control_schema}".network_membership_candidate WHERE candidate_id=$1',
                UUID(copy_target.candidate_id),
            )
        )
        receipt = verify_initial_source_office_receipt(candidate_by_field)
        assert (receipt["office_rows"], receipt["page_count"], receipt["recipe_count"]) == (1, 1, 1)
        assert (
            candidate_by_field["accepted_rows"] == candidate_by_field["expected_rows"] == 2
            and candidate_by_field["state"] == "sealed"
        )
        assert await compose_registry_membership_candidate(fixture.connection, **arguments) == (copy_target, addresses)
        await prepare_and_publish_network_candidate(
            fixture.connection,
            copy_target,
            addresses,
            **arguments["writer_roles"],
            control_schema=fixture.control_schema,
        )
        async with fixture.connection.transaction(isolation="repeatable_read", readonly=True):
            manifest = await resolve_network_serving_manifest(fixture.connection, control_schema=fixture.control_schema)
            assert manifest.approved_custom_revision == seed.revision
            members = await fixture.connection.fetch(f'SELECT * FROM "{manifest.schema_name}".network_membership')
            assert len(members) == 2
            imported_members = [
                member for member in members if member["provider_system"] == fixture.cms_provider_system
            ]
            assert imported_members[0]["provider_id"] == fixture.cms_provider_id
            assert len(imported_members) == 1 and imported_members[0]["location_id"] == fixture.reviewed_cms[0][3][0]
            assert imported_members[0]["location_id"] != fixture.reviewed_cms[0][3][1]
            projected = await fixture.connection.fetch(
                f'SELECT entity_type,second_line,canonical_network_ids FROM "{manifest.schema_name}".entity_address_unified'
            )
            assert len(projected) == 2 and all(len(address["canonical_network_ids"]) == 1 for address in projected)
            assert (
                next(address for address in projected if address["entity_type"] == fixture.cms_provider_system)[
                    "second_line"
                ]
                == "Suite 2"
            )
        await _assert_cms_native_read(fixture, manifest, fixture.reviewed_cms[2][0]["record_id"])
    finally:
        await _remove_candidates(fixture, copy_targets, arguments["request_id"])


@pytest.mark.parametrize("cms_full_office_source", ["invalid_npi"], indirect=True)
async def test_initial_cms_malformed_npi_cannot_become_directory_identity(initial_cms_db):
    from process.network_initial_cms_office_bindings import InitialCMSOfficeBindingError
    from process.registry_candidate_composition import compose_registry_membership_candidate
    from process.registry_source_recipe_composition import RegistrySourceCompositionError
    from tests.test_registry_candidate_composition_postgres import _remove_candidates

    fixture = initial_cms_db
    _, arguments = await _cms_initial_arguments(fixture)
    before = await fixture.connection.fetchval(
        f'SELECT count(*) FROM "{fixture.control_schema}".network_membership_candidate'
    )
    try:
        with pytest.raises(
            (InitialCMSOfficeBindingError, RegistrySourceCompositionError),
            match="initial_cms_office_input_invalid|registry_source_composition_unresolved",
        ):
            await compose_registry_membership_candidate(fixture.connection, **arguments)
        assert (
            await fixture.connection.fetchval(
                f'SELECT count(*) FROM "{fixture.control_schema}".network_membership_candidate'
            )
            == before
        )
    finally:
        await _remove_candidates(fixture, [], arguments["request_id"])


async def test_cms_actual_native_codec_cannot_relabel_directory_identity_as_npi():
    from process.network_cms_provider_identity import CMSProviderIdentity
    from process.network_membership_copy import _encode

    identity = CMSProviderIdentity("cms-npd", "Practitioner", "1234567893")
    member_by_field = {
        "network_id": 42,
        "provider_system": "provider_directory",
        "provider_id": identity.provider_id,
        "location_id": "10000000-0000-4000-8000-000000000001",
        "evidence_id": "a" * 64,
    }
    wire, count = _encode(json.dumps([member_by_field], separators=(",", ":")).encode())
    assert count == 1 and b"provider_directory" in wire and identity.provider_id.encode() in wire
    member_by_field["provider_system"] = "npi"
    with pytest.raises(ValueError):
        _encode(json.dumps([member_by_field], separators=(",", ":")).encode())


async def _damage_cms_office(fixture, monkeypatch, damage):
    from process import registry_initial_source_composition as initial

    namespace = f'"{fixture.source_schema}".entity_address_unified'
    before = await fixture.connection.fetchval(f"SELECT jsonb_agg(to_jsonb(address))::text FROM {namespace} address")
    original = initial.copy_initial_cms_office_bindings
    if damage == "unit":
        await fixture.connection.execute(f"UPDATE {namespace} SET second_line='Suite 99'")
    elif damage == "missing":
        await fixture.connection.execute(f"DELETE FROM {namespace}")
    elif damage == "duplicate":
        await fixture.connection.execute(f"""INSERT INTO {namespace} SELECT (jsonb_populate_record(
            NULL::{namespace},to_jsonb(address)||jsonb_build_object('location_key',repeat('b',64),'checksum',43))).*
            FROM {namespace} address""")
    elif damage == "binding":
        from process import network_initial_cms_office_bindings as cms_offices

        native_copy = cms_offices._copy_bindings
        injection_receipts = []

        async def duplicate_binding(connection, copy_target, bindings):
            await native_copy(connection, copy_target, bindings)
            if not injection_receipts:
                injection_receipts.append(
                    await connection.execute(f"""INSERT INTO "{copy_target.schema_name}".provider_location_binding
                  SELECT provider_system,provider_id,gen_random_uuid(),location_key,entity_type,entity_id
                  FROM "{copy_target.schema_name}".provider_location_binding WHERE provider_system='npi' LIMIT 1""")
                )

        monkeypatch.setattr(cms_offices, "_copy_bindings", duplicate_binding)
    else:

        async def fault(*args, **options):
            await original(*args, **options)
            raise RuntimeError("synthetic office copy fault")

        monkeypatch.setattr(initial, "copy_initial_cms_office_bindings", fault)
    return before, original


@pytest.mark.parametrize("damage", ["unit", "missing", "duplicate", "binding", "copy"])
async def test_initial_cms_fault_is_atomic_and_exact_retry(initial_cms_db, monkeypatch, damage):
    from process import registry_initial_source_composition as initial
    from process.registry_candidate_composition import compose_registry_membership_candidate
    from tests.test_registry_candidate_composition_postgres import _remove_candidates

    fixture = initial_cms_db
    _, arguments = await _cms_initial_arguments(fixture)
    before, original = await _damage_cms_office(fixture, monkeypatch, damage)
    copy_targets = []
    try:
        with pytest.raises((ValueError, RuntimeError), match="office_|office copy"):
            await compose_registry_membership_candidate(fixture.connection, **arguments)
        assert not await fixture.connection.fetchval(
            f'SELECT EXISTS(SELECT 1 FROM "{fixture.control_schema}".network_membership_candidate)'
        )
        assert (
            await fixture.connection.fetchval(
                f'SELECT generation_id FROM "{fixture.control_schema}".network_serving_control'
            )
            is None
        )
        assert not await fixture.connection.fetchval(
            "SELECT EXISTS(SELECT 1 FROM pg_class WHERE relnamespace=pg_my_temp_schema() "
            "AND (relname LIKE 'registry_recipe_%' OR relname LIKE 'initial_aca_offices_%'))"
        )
        namespace = f'"{fixture.source_schema}".entity_address_unified'
        await fixture.connection.execute(f"DELETE FROM {namespace}")
        await fixture.connection.execute(
            f"INSERT INTO {namespace} SELECT * FROM jsonb_populate_recordset(NULL::{namespace},$1::jsonb)", before
        )
        monkeypatch.setattr(initial, "copy_initial_cms_office_bindings", original)
        copy_target, _ = await compose_registry_membership_candidate(fixture.connection, **arguments)
        copy_targets.append(copy_target)
        assert (
            await fixture.connection.fetchval(f'SELECT count(*) FROM "{copy_target.schema_name}".network_membership')
            == 2
        )
    finally:
        await _remove_candidates(fixture, copy_targets, arguments["request_id"])


async def _create_cms_native_office_tables(connection, schema, names):
    """Register absent exact names within the source fixture's owned transaction."""
    for name in names:
        assert await connection.fetchval("SELECT to_regclass($1)", schema + "." + name) is None
    await connection.execute(
        f'CREATE TABLE "{schema}"."{names[0]}" (source_id text,resource_id text,last_seen_run_id text,address_key text,first_line text,second_line text,city_name text,state_name text,postal_code text,country_code text)'
    )
    await connection.execute(
        f'CREATE TABLE "{schema}"."{names[1]}" (source_id text,resource_id text,computed_address_key text,computed_zip5 text,computed_state_code text,computed_city_norm text,restored_city_name text,restored_state_name text,normalized_country text,eligible_change bool)'
    )


async def _copy_cms_native_office(connection, source_pin, names):
    """Copy one admitted synthetic office through the production native codec."""
    import ptg2_address_canon

    from process import entity_address_candidate_preparation as preparation

    fhir = importlib.import_module("process.provider_directory_fhir")
    schema = source_pin.schema_name
    office_record = await connection.fetchrow(
        f"""SELECT dataset.acquisition_root_run_id,location.payload_json FROM "{schema}".provider_directory_endpoint_dataset dataset JOIN "{schema}".provider_directory_dataset_resource location USING(dataset_id) WHERE dataset_id=$1 AND resource_type='Location' AND resource_id='site-1'""",
        source_pin.dataset_id,
    )
    assert office_record is not None
    acquisition_run = office_record["acquisition_root_run_id"]
    office_by_field = json.loads(office_record["payload_json"])
    location_by_field = dict(
        office_by_field,
        source_id="cms-npd",
        resource_id="site-1",
        resolved_city=office_by_field["city_name"],
        resolved_state=office_by_field["state_name"],
        effective_country=office_by_field["country_code"],
        computed_zip5="90210",
    )
    copied, _ = await fhir._copy_location_key_batch(
        SimpleNamespace(native_module=ptg2_address_canon, stage_table=names[1], schema=schema),
        connection.copy_records_to_table,
        [location_by_field],
    )
    assert copied == 1
    address_key = await connection.fetchval(f'SELECT computed_address_key FROM "{schema}"."{names[1]}"')
    raw_fields = tuple(office_by_field[name] for name in _CMS_NATIVE_ADDRESS_FIELDS)
    assert address_key == ptg2_address_canon.canonicalize_batch([raw_fields])[0]["address_key"]
    await connection.execute(
        f'INSERT INTO "{schema}"."{names[0]}" VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)',
        "cms-npd",
        "site-1",
        acquisition_run,
        address_key,
        *raw_fields,
    )
    relation_oid = await connection.fetchval("SELECT to_regclass($1)::oid::bigint", schema + "." + names[0])
    dataset_pin = preparation.ProviderDirectoryAddressDatasetPin(
        "cms-npd", source_pin.endpoint_id, source_pin.dataset_id, source_pin.dataset_sha256, acquisition_run
    )
    inputs = preparation.ProviderDirectoryAddressPreparationInput(
        (dataset_pin,),
        names[0],
        relation_oid,
        0,
        (("provider_directory_location", names[0]),),
        source_pin.as_of,
    )
    return inputs, raw_fields, address_key


_CMS_NATIVE_ADDRESS_FIELDS = ("first_line", "second_line", "city_name", "state_name", "postal_code", "country_code")


@pytest.mark.parametrize("cms_full_office_source", ["provider_directory"], indirect=True)
async def test_admitted_cms_typed_office_native_source_sql(initial_cms_db):
    """Exercise real admitted witnesses and native COPY; not full Control admission."""
    import importlib

    from process import entity_address_candidate_preparation as preparation
    from process.provider_directory_cms_typed_offices import CMS_OFFICE_READ_TABLES, cms_typed_office_source

    native = importlib.import_module("process.entity_address_unified")
    connection, _, source_pin, _ = initial_cms_db.reviewed_cms[0]
    schema = source_pin.schema_name
    names = ["typed_location_" + uuid4().hex, "typed_key_" + uuid4().hex]
    async with connection.transaction(isolation="repeatable_read"):
        await _create_cms_native_office_tables(connection, schema, names)
        inputs, raw_fields, address_key = await _copy_cms_native_office(connection, source_pin, names)
        available_by_name = dict.fromkeys(
            (*native.PROVIDER_DIRECTORY_DATASET_FENCE_TABLES, *CMS_OFFICE_READ_TABLES, "provider_directory_location"),
            True,
        )
        token = preparation._PREPARATION.set(inputs)
        try:
            statement = preparation.source_sql(schema, cms_typed_office_source(native, schema, available_by_name))
            source_rows = await connection.fetch(statement)
            assert len(source_rows) == 1
            assert source_rows[0]["entity_id"] == initial_cms_db.cms_provider_id and source_rows[0]["npi"] is None
            assert tuple(source_rows[0][name] for name in _CMS_NATIVE_ADDRESS_FIELDS) == raw_fields
            assert str(source_rows[0]["address_key"]) == address_key
            source_record_id = source_rows[0]["source_record_id"]
            assert source_record_id.startswith("provider_directory_fhir:cms_typed:cms-npd:")
            assert await connection.fetchval(
                "SELECT "
                + native._compacted_source_record_ids_expr()
                + " FROM (SELECT $1::varchar[] AS source_record_ids,ARRAY['provider_directory_fhir']::varchar[] AS address_sources) compacted",
                [source_record_id],
            ) == [source_record_id]
            await connection.execute(f"""UPDATE \"{schema}\".\"{names[0]}\" SET second_line='wrong suite'""")
            with pytest.raises(Exception, match="cms_typed_office_correspondence_unavailable"):
                async with connection.transaction():
                    await connection.fetch(statement)
        finally:
            preparation._PREPARATION.reset(token)
            for name in reversed(names):
                await connection.execute(f'DROP TABLE "{schema}"."{name}"')
        for name in names:
            assert await connection.fetchval("SELECT to_regclass($1)", schema + "." + name) is None
