# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Genuine mixed CMS/PTG preparation, administrative freeze and complete data validation."""

import asyncio
import json
from contextlib import AsyncExitStack, aclosing
from dataclasses import replace
from types import SimpleNamespace
from uuid import UUID, uuid4, uuid5

import asyncpg
import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from process.network_address_projection import PinnedAddressSource
from process.network_custom_address_source import prepare_custom_address_source
from process.registry_candidate_composition import (
    RegistryAddressArtifactCustody,
    RegistryCompositionAddressSources,
    prepare_registry_composition_address_artifact,
)
from process.registry_company_approval_fence import registry_company_approval_transaction
from process.registry_ptg_graph_reader import RegistryPTGGraphReadBudget
from process.registry_publication_ptg_runtime import RegistryPublicationPTGRuntime
from process.registry_publication_worker import _configured_worker_runtime, _PTGWorkerConfig
from process.registry_source_recipe_store import RegistrySourceMembershipRecipe
from process.registry_source_retention import RegistrySourceRetentionError
from tests import cms_registry_recipe_postgres_support as cms_support
from tests import test_registry_ptg_office_capture_postgres as office_fixtures
from tests.cms_registry_recipe_postgres_support import cms_recipe_database as cms_recipe_database
from tests.cms_registry_recipe_postgres_support import cms_recipe_template as cms_recipe_template
from tests.registry_candidate_custody_postgres_support import administrative_freeze_callback
from tests.source_contribution_custody_postgres_support import publish_source_contribution_candidate
from tests.test_network_compact_publication_postgres import _named_compact_runtime
from tests.test_network_custom_address_source_postgres import custom_db as _base_custom_db
from tests.test_registry_ptg_office_coordinator_postgres import coordinator_db as coordinator_db
from tests.test_registry_ptg_office_coordinator_postgres import office_db as _coordinator_office_db
from tests.test_registry_ptg_office_coordinator_postgres import office_publisher as office_publisher
from tests.test_registry_publication_ptg_runtime_postgres import _retained_pin, _review_offices, _worker

pytestmark = pytest.mark.asyncio


async def _remove_retained_address_owner(connection, role):
    """The base fixture has already removed its registered schemas and original source."""
    if await connection.fetchval("SELECT EXISTS(SELECT FROM pg_roles WHERE rolname=$1)", role):
        await connection.execute(f'DROP OWNED BY "{role}"')
        await connection.execute(f'DROP ROLE "{role}"')
    assert not await connection.fetchval("SELECT EXISTS(SELECT FROM pg_roles WHERE rolname=$1)", role)


@pytest.fixture
async def custom_db(serving_schema):
    """Keep the protected queue owner separate from the immutable address owner."""
    connection = serving_schema[0]
    role = "mixed_retained_owner_" + uuid4().hex
    assert not await connection.fetchval("SELECT EXISTS(SELECT FROM pg_roles WHERE rolname=$1)", role)
    async with AsyncExitStack() as stack:
        stack.push_async_callback(_remove_retained_address_owner, connection, role)
        generator = await stack.enter_async_context(aclosing(_base_custom_db.__wrapped__(serving_schema)))
        fixture = await anext(generator)
        await connection.execute(
            f'CREATE ROLE "{role}" NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS'
        )
        database = await connection.fetchval("SELECT quote_ident(current_database())")
        await connection.execute(f'GRANT CREATE ON DATABASE {database} TO "{role}"')
        fixture.retained_address_owner = role
        fixture.roles["retained_address"] = role
        await connection.execute(f'ALTER SCHEMA "{fixture.source_schema}" OWNER TO "{role}"')
        for table in ("entity_address_unified", "npi"):
            await connection.execute(f'ALTER TABLE "{fixture.source_schema}"."{table}" OWNER TO "{role}"')
        yield fixture


@pytest.fixture
async def office_db(custom_db, tmp_path, monkeypatch, office_publisher):
    """Publish the genuine predecessor under the independent retained owner."""
    original_roles = office_fixtures._roles

    def retained_roles(fixture):
        roles_by_kind = original_roles(fixture)
        assert roles_by_kind["owner_role"] == custom_db.roles["owner"]
        return {**roles_by_kind, "owner_role": custom_db.retained_address_owner}

    async with AsyncExitStack() as stack:
        await _named_compact_runtime(custom_db, stack, monkeypatch)
        custom_db.mixed_copy_targets = []
        stack.push_async_callback(_remove_mixed_storage, custom_db, custom_db.mixed_copy_targets)

        async def publish(connection, target, address, **options):
            assert options.pop("control_schema") == custom_db.control_schema
            return await publish_source_contribution_candidate(
                custom_db, connection, target, address, writer_roles=options
            )

        with monkeypatch.context() as settings:
            settings.setattr(office_fixtures, "_roles", retained_roles)
            settings.setattr(office_fixtures, "prepare_and_publish_network_candidate", publish)
            async with aclosing(
                _coordinator_office_db.__wrapped__(custom_db, tmp_path, settings, office_publisher)
            ) as generator:
                yield await anext(generator)


@pytest.fixture
async def serving_schema(cms_recipe_database, monkeypatch):
    """Keep genuine PTG authority and CMS/registry fixtures in one owned database."""
    dsn = cms_recipe_database.engine.url.set(drivername="postgresql").render_as_string(hide_password=False)
    with monkeypatch.context() as settings:
        settings.setenv("HLTHPRT_PTG2_V4_MIGRATION_POSTGRES_DSN", dsn)
        async with aclosing(cms_support.serving_schema.__wrapped__(cms_recipe_database, settings)) as registry_fixture:
            registry = await anext(registry_fixture)
            yield registry


@pytest.fixture
async def reviewed_ptg_offices(coordinator_db):
    return await _review_offices(coordinator_db)


@pytest.fixture
async def reviewed_cms_source(cms_recipe_database, serving_schema, monkeypatch, tmp_path, reviewed_ptg_offices):
    """Declare a complete synthetic street before the actual source is admitted."""
    original = cms_support._membership_resource_rows

    def resources(revision_source, revision):
        documents = original(revision_source, revision)
        documents["Location"][0]["address"].update(state="CA", postalCode="90210", country="US")
        return documents

    monkeypatch.setattr(cms_support, "_membership_resource_rows", resources)
    async with aclosing(
        cms_support.reviewed_cms_source.__wrapped__(cms_recipe_database, serving_schema, monkeypatch, tmp_path)
    ) as fixture:
        yield await anext(fixture)


def _socket_url(url):
    if "host" in url.query:
        return url.set(host="localhost", port=int(url.query["port"]))
    return url


async def _remove_mixed_storage(fixture, copy_targets):
    """Remove only registered task candidates and address compositions after their clients close."""
    for target in copy_targets:
        await fixture.connection.execute(f'DROP SCHEMA IF EXISTS "{target.schema_name}" CASCADE')
        assert await fixture.connection.fetchval("SELECT to_regnamespace($1)", target.schema_name) is None
    for identity in fixture.composition_ids:
        schema = "network_composition_" + identity.hex
        await fixture.connection.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        assert await fixture.connection.fetchval("SELECT to_regnamespace($1)", schema) is None


async def _add_declared_cms_office(fixture, source_fixture):
    """Put the actual admitted CMS Location into the fixture's unpublished base."""
    import ptg2_address_canon

    pin = source_fixture[0][2]
    resource_payload = json.loads(
        await fixture.connection.fetchval(
            f'SELECT payload_json::text FROM "{pin.read_schema_name}".provider_directory_dataset_resource '
            "WHERE dataset_id=$1 AND resource_type='Location' AND resource_id='site-1'",
            pin.dataset_id,
        )
    )
    table = f'"{fixture.source_schema}".entity_address_unified'
    values_by_field = dict(location_key="c" * 64, entity_id="1000000491", npi=1000000491, checksum=45)
    values_by_field.update(
        {
            name: resource_payload.get(name)
            for name in ("first_line", "second_line", "city_name", "state_name", "postal_code", "country_code")
        }
    )
    canonical = ptg2_address_canon.canonicalize_batch(
        [
            tuple(
                values_by_field[name]
                for name in ("first_line", "second_line", "city_name", "state_name", "postal_code", "country_code")
            )
        ]
    )[0]
    assert canonical["identity_key"].endswith("|street")
    values_by_field.update(
        address_key=UUID(canonical["address_key"]),
        premise_key=UUID(canonical["premise_key"]),
        address_precision="street",
        zip5=canonical["zip5"],
        state_code=canonical["state_code"],
        city_norm=canonical["city_norm"],
    )
    columns = await fixture.connection.fetch(
        "SELECT attname FROM pg_attribute WHERE attrelid=to_regclass($1) AND attnum>0 AND NOT attisdropped ORDER BY attnum",
        table,
    )
    parameters = list(values_by_field.values())
    expressions_by_field = {name: "$" + str(index) for index, name in enumerate(values_by_field, 1)}
    await fixture.connection.execute(
        f"INSERT INTO {table} SELECT "
        + ",".join(expressions_by_field.get(column["attname"], f'original."{column["attname"]}"') for column in columns)
        + f" FROM {table} original WHERE original.location_key=$"
        + str(len(parameters) + 1),
        *parameters,
        "a" * 64,
    )
    await fixture.connection.execute(f'INSERT INTO "{fixture.source_schema}".npi(npi) VALUES(1000000491)')


async def _retained_cms_address_source(native, revision):
    """Freeze the newly declared CMS office with the existing Reader/Publisher roster."""
    fixture = native.custom
    identity = uuid4()
    fixture.composition_ids.append(identity)
    async with fixture.connection.transaction(isolation="repeatable_read"):
        return await prepare_custom_address_source(
            fixture.connection,
            fixture.base,
            composition_id=str(identity),
            approved_revision=revision,
            owner_role=fixture.retained_address_owner,
            runtime_roles=tuple(sorted((fixture.roles["reader"], native.roles_by_kind["publisher"]))),
            control_schema=fixture.control_schema,
        )


async def _prepare_mixed_addresses(consumer, fixture, approved, previous, selected, request_id):
    """Register exact cleanup before the original independent preparation callback creates storage."""
    composition_id = uuid5(request_id, "address:" + approved.generation_id)
    fixture.composition_ids.append(composition_id)
    return await consumer.prepare_address_sources(
        request_id=request_id,
        approved_revision=approved.approved_revision,
        expected_head=previous.generation_id,
        source_manifest=previous,
        address_sources=selected,
    )


async def _mixed_execution(native, cms, service, stack):
    """Keep the existing worker login, inherited readers and exact cleanup unchanged."""
    fixture = native.custom
    execution_role, loading_role = fixture.custody_roles["execution"], fixture.custody_roles["loading"]
    engine, _, execution = await _worker(native, execution_role, loading_role, fixture.execution_url.password)
    stack.push_async_callback(engine.dispose)
    stack.push_async_callback(execution.close)
    for role in cms_support.retained_fixture_roles(cms[0][2])[1:]:
        await fixture.connection.execute(
            f'GRANT "{role}" TO "{execution_role}" WITH ADMIN FALSE, INHERIT TRUE, SET FALSE'
        )
    runtime = RegistryPublicationPTGRuntime(
        async_sessionmaker(engine), service.store, RegistryPTGGraphReadBudget(16 * 1024 * 1024)
    )
    return runtime, execution


@pytest.fixture(params=(False, True), ids=("pure-ptg", "cms-ptg"))
async def mixed_ptg_custody(coordinator_db, reviewed_cms_source, reviewed_ptg_offices, request):
    """Prepare actual PTG/CMS source data and register exact candidate cleanup."""
    cms = reviewed_cms_source
    fixture = coordinator_db.custom
    service, ptg_recipes, _ = reviewed_ptg_offices
    revision = cms[-1].approved_revision
    recipes = (*ptg_recipes, RegistrySourceMembershipRecipe(cms[0][2], cms[4])) if request.param else ptg_recipes
    async with AsyncExitStack() as stack:
        copy_targets = fixture.mixed_copy_targets
        stack.push_async_callback(_remove_mixed_storage, fixture, copy_targets)
        runtime, execution = await _mixed_execution(coordinator_db, cms, service, stack)
        previous = await _retained_pin(coordinator_db)
        retained_address = None
        if request.param:
            await _add_declared_cms_office(fixture, cms)
            retained_address = await _retained_cms_address_source(coordinator_db, revision)
        consumer, config = await _mixed_consumer(coordinator_db, runtime, execution, service, stack)
        request_id = uuid4()
        selected = _mixed_address_sources(fixture, previous, retained_address, recipes)
        selected = await _prepare_mixed_addresses(consumer, fixture, cms[-1], previous, selected, request_id)
        copy_target, address = await runtime.compose(
            execution,
            request_id=request_id,
            approved_revision=revision,
            expected_head=previous.generation_id,
            source_manifest=previous,
            address_sources=selected,
            writer_roles=consumer.writer_roles,
            control_schema=fixture.control_schema,
        )
        copy_targets.append(copy_target)
        await consumer.composition_runtime.prepare(
            execution, copy_target, address, writer_roles=consumer.writer_roles, control_schema=fixture.control_schema
        )
        await _assert_mixed_source_rows(fixture, copy_target, recipes)
        yield SimpleNamespace(
            native=coordinator_db,
            fixture=fixture,
            target=copy_target,
            consumer=consumer,
            config=config,
            sessions=consumer.composition_runtime.sessions,
            previous=previous,
            producer_sessions=runtime.sessions,
            recipe_count=len(recipes),
            ptg_recipe=ptg_recipes[0],
            approved_revision=revision,
            retained_address=retained_address,
            retained_source=selected.base_source,
            address_source=address,
        )


async def _verify_projected_office(case):
    from process.network_approved_membership_source import pin_retained_approved_membership_source
    from process.registry_ptg_office_bindings import _offices
    from process.registry_ptg_office_membership import (
        read_ptg_office_membership_batch,
        require_ptg_office_authority,
        require_ptg_office_recipe,
    )

    async with registry_company_approval_transaction(
        case.producer_sessions, control_schema=case.fixture.control_schema
    ) as session:
        driver = (await (await session.connection()).get_raw_connection()).driver_connection
        authority = await require_ptg_office_authority(
            driver, case.fixture.control_schema, (session, case.config.ptg.store, case.config.ptg.read_budget)
        )
        approved = await pin_retained_approved_membership_source(
            driver, approved_revision=case.approved_revision, control_schema=case.fixture.control_schema
        )
        verified = await require_ptg_office_recipe(
            driver,
            case.ptg_recipe,
            approved,
            case.fixture.control_schema,
            authority,
        )
        batch = await read_ptg_office_membership_batch(
            driver, verified, approved, None, 100, case.fixture.control_schema
        )
        address = PinnedAddressSource(case.target.schema_name, "entity_address_unified", "prepared-fixture")
        return await _offices(driver, batch, address, None, case.fixture.control_schema)


async def test_genuine_mixed_candidate_validates_data_and_preserves_retained_head(mixed_ptg_custody, monkeypatch):
    """Check actual source parity, cancellation and validated replay before publication."""
    case = mixed_ptg_custody
    await _assert_mixed_native_tamper(case)
    await _assert_cancelled_mixed_seal(case, monkeypatch)
    await _assert_mixed_sealed_replay(case, monkeypatch)
    await _assert_mixed_reader_permissions(case)


async def _mixed_consumer(native, runtime, execution, service, stack):
    """Configure the existing PTG worker roles and its normal administrative handoff."""
    fixture = native.custom
    execution_role, loading_role = fixture.custody_roles["execution"], fixture.custody_roles["loading"]
    from process.registry_publication_consumer import RegistryPublicationConsumerConfig

    consumer = RegistryPublicationConsumerConfig(
        control_schema=fixture.control_schema,
        lifecycle_role=native.roles_by_kind["queue"],
        execution_role=execution_role,
        api_role=native.roles_by_kind["management"],
        queue_owner_role=fixture.roles["owner"],
        physical_owner_role=loading_role,
        reader_roles=(fixture.roles["reader"], native.roles_by_kind["publisher"]),
        composition_runtime=runtime,
    )
    config = SimpleNamespace(
        consumer=consumer,
        execution_url=_socket_url(fixture.execution_url),
        ptg=_PTGWorkerConfig(service.store, runtime.read_budget),
        sql_limits=fixture.sql_limits,
        preparation_roles={
            **fixture.preparation_roles,
            "application_roles": sorted(
                set(fixture.preparation_roles["application_roles"]) | set(native.roles_by_kind.values())
            ),
        },
    )
    consumer = await _configured_worker_runtime(config, stack)
    fixture.preparation_roles = consumer.preparation_roles
    validate = administrative_freeze_callback(fixture, consumer)

    async def validate_candidate(target, **options):
        if target.candidate_id not in fixture.preparation_ids:
            fixture.preparation_ids.append(target.candidate_id)
        return await validate(target, **options)

    async def prepare_addresses(**arguments):
        return await _mixed_address_artifact(consumer, arguments)

    return replace(consumer, seal_candidate=validate_candidate, prepare_address_sources=prepare_addresses), config


async def _mixed_address_artifact(consumer, arguments):
    """Prepare actual source addresses under the maintained PTG company approval transaction."""
    selected = arguments["address_sources"]
    runtime_roles = tuple(sorted({*consumer.reader_roles, consumer.execution_role}))
    custody = RegistryAddressArtifactCustody(consumer.physical_owner_role, runtime_roles, consumer.control_schema)
    async with consumer.composition_runtime.validation_snapshot() as (connection, office_authority):
        artifact = await prepare_registry_composition_address_artifact(
            connection, custody=custody, office_authority=office_authority, **arguments
        )
    return selected if artifact is None else replace(selected, prepared_address_artifact=artifact)


async def _assert_mixed_source_rows(fixture, copy_target, recipes):
    """Each actual source recipe must retain its distinct native network identity."""
    membership_records = await fixture.connection.fetch(
        f'SELECT network_id FROM "{copy_target.schema_name}".network_membership_source'
    )
    assert len(membership_records) == len(recipes) and len(
        {membership_record["network_id"] for membership_record in membership_records}
    ) == len(recipes)


async def _assert_mixed_native_tamper(case):
    """Restore every changed office or membership cell after its exact native refusal."""
    fixture, copy_target = case.fixture, case.target
    offices = await _verify_projected_office(case)
    assert len(offices) == 1
    address_table = f'"{copy_target.schema_name}".entity_address_unified'
    office = offices[0]
    actual = await fixture.connection.fetchrow(
        f"SELECT first_line,encode(sha256(convert_to(to_jsonb(address)::text,'UTF8')),'hex') AS sha FROM {address_table} address WHERE location_key=$1",
        office["location_key"],
    )
    assert actual["sha"] != office["address_row_sha256"]
    await fixture.connection.execute(
        f"UPDATE {address_table} SET first_line='Altered example address' WHERE location_key=$1", office["location_key"]
    )
    try:
        with pytest.raises(ValueError, match="office_binding_unresolved"):
            await _verify_projected_office(case)
    finally:
        await fixture.connection.execute(
            f"UPDATE {address_table} SET first_line=$1 WHERE location_key=$2",
            actual["first_line"],
            office["location_key"],
        )
    for table, reason in (
        ("network_membership_source", "retention_parity_invalid"),
        ("network_membership", "effective_parity_invalid"),
    ):
        relation = f'"{copy_target.schema_name}".{table}'
        original = await fixture.connection.fetchrow(
            f"SELECT network_id,evidence_id FROM {relation} ORDER BY network_id LIMIT 1"
        )
        await fixture.connection.execute(
            f"UPDATE {relation} SET evidence_id=$1 WHERE network_id=$2", "f" * 64, original["network_id"]
        )
        try:
            with pytest.raises(RegistrySourceRetentionError, match=reason):
                await case.consumer.seal_candidate(copy_target)
        finally:
            await fixture.connection.execute(
                f"UPDATE {relation} SET evidence_id=$1 WHERE network_id=$2",
                original["evidence_id"],
                original["network_id"],
            )


async def _assert_cancelled_mixed_seal(case, monkeypatch):
    """Cancellation before retaining final validation leaves the prior serving head intact."""
    from process import network_membership_publication as publication

    calls = []

    async def cancel_retention(connection, target, validation, control_schema, roles):
        calls.append(target)
        raise asyncio.CancelledError

    with monkeypatch.context() as settings:
        settings.setattr(publication, "_retain_network_final_validation", cancel_retention)
        with pytest.raises(asyncio.CancelledError):
            await case.consumer.seal_candidate(case.target)
    assert calls == [case.target]
    assert (
        await case.fixture.connection.fetchval(
            f"SELECT validation_json->'final_validation' FROM \"{case.fixture.control_schema}\".network_membership_candidate "
            "WHERE candidate_id=$1",
            UUID(case.target.candidate_id),
        )
        is None
    )
    assert await _retained_pin(case.native) == case.previous


async def _assert_mixed_sealed_replay(case, monkeypatch):
    """Retain one complete data result and replay it without another full source scan."""
    from process import network_membership_validation as validation

    fixture, target = case.fixture, case.target
    completed = await case.consumer.seal_candidate(target)
    assert completed["state"] == "validated" and completed["candidate_id"] == target.candidate_id
    before = await fixture.connection.fetchval(
        f'SELECT validation_json::text FROM "{fixture.control_schema}".network_membership_candidate WHERE candidate_id=$1',
        UUID(target.candidate_id),
    )

    async def no_recapture(*args, **kwargs):
        raise AssertionError("validated replay must not repeat complete source validation")

    with monkeypatch.context() as settings:
        settings.setattr(validation, "validate_network_candidate_final", no_recapture)
        assert await case.consumer.seal_candidate(target, pre_enrolled_only=True) == completed
    candidate = await fixture.connection.fetchrow(
        f"SELECT validation_json::text AS validation,source_recipes_json::text AS recipes "
        f'FROM "{fixture.control_schema}".network_membership_candidate WHERE candidate_id=$1',
        UUID(target.candidate_id),
    )
    assert candidate["validation"] == before
    final_validation = json.loads(before)["final_validation"]
    assert final_validation["complete"] is True and final_validation["schema_name"] == target.schema_name
    assert final_validation["candidate_id"] == target.candidate_id
    assert len(json.loads(candidate["recipes"])) == case.recipe_count
    assert await _retained_pin(case.native) == case.previous


async def _assert_mixed_reader_permissions(case):
    """Use actual PostgreSQL reads and write refusals after the administrative freeze."""
    connection, schema = case.fixture.connection, case.target.schema_name
    async with connection.transaction(readonly=True):
        await connection.execute(f'SET LOCAL ROLE "{case.fixture.roles["reader"]}"')
        assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".network_membership') > 0
    for role in (case.consumer.execution_role, case.consumer.physical_owner_role, *case.consumer.reader_roles):
        with pytest.raises(asyncpg.InsufficientPrivilegeError):
            async with connection.transaction():
                await connection.execute(f'SET LOCAL ROLE "{role}"')
                await connection.execute(f'DELETE FROM "{schema}".network_membership')
    with pytest.raises(asyncpg.InsufficientPrivilegeError):
        async with connection.transaction(readonly=True):
            await connection.execute(f'SET LOCAL ROLE "{case.fixture.roles["outsider"]}"')
            await connection.fetchval(f'SELECT count(*) FROM "{schema}".network_membership')


def _mixed_address_sources(fixture, previous, retained_address, recipes):
    """Select the original admitted base and exact replacement recipes for this cohort."""
    return RegistryCompositionAddressSources(
        retained_address.address_source
        if retained_address is not None
        else PinnedAddressSource(previous.schema_name, "entity_address_unified", previous.manifest_sha256),
        replacement_source_recipes=recipes,
    )
