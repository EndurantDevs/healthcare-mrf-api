# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Genuine mixed CMS/PTG candidate custody on an independent fenced operator."""

import asyncio
import json
from contextlib import AsyncExitStack, aclosing
from types import SimpleNamespace
from uuid import UUID, uuid4, uuid5

import asyncpg
import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from process.network_address_projection import PinnedAddressSource
from process.network_candidate_custody import (
    _address_runtime_closure,
    _retained_address_runtime_closure,
    seal_network_candidate_custody,
    seal_network_candidate_custody_in_session,
)
from process.network_custom_address_source import prepare_custom_address_source
from process.network_membership_pipeline import prepare_network_candidate
from process.registry_candidate_composition import RegistryCompositionAddressSources
from process.registry_company_approval_fence import registry_company_approval_transaction
from process.registry_ptg_graph_reader import RegistryPTGGraphReadBudget
from process.registry_publication_ptg_runtime import RegistryPublicationPTGRuntime
from process.registry_publication_worker import _custody_runtime, _PTGWorkerConfig
from process.registry_source_recipe_store import RegistrySourceMembershipRecipe
from process.registry_source_retention import RegistrySourceRetentionError
from tests import cms_registry_recipe_postgres_support as cms_support
from tests import test_registry_ptg_office_capture_postgres as office_fixtures
from tests.cms_registry_recipe_postgres_support import cms_recipe_database as cms_recipe_database
from tests.cms_registry_recipe_postgres_support import cms_recipe_template as cms_recipe_template
from tests.registry_candidate_custody_postgres_support import install_candidate_custody
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
        await connection.execute(f'ALTER SCHEMA "{fixture.source_schema}" OWNER TO "{role}"')
        for table in ("entity_address_unified", "npi"):
            await connection.execute(f'ALTER TABLE "{fixture.source_schema}"."{table}" OWNER TO "{role}"')
        yield fixture


@pytest.fixture
async def office_db(custom_db, tmp_path, monkeypatch, office_publisher):
    """Publish the genuine predecessor under the independent retained owner."""
    original_roles = office_fixtures._roles
    original_publish = office_fixtures.prepare_and_publish_network_candidate

    def retained_roles(fixture):
        roles_by_kind = original_roles(fixture)
        assert roles_by_kind["owner_role"] == custom_db.roles["owner"]
        return {**roles_by_kind, "owner_role": custom_db.retained_address_owner}

    async with AsyncExitStack() as stack:
        await install_candidate_custody(custom_db, stack)
        custom_db.mixed_copy_targets = []
        await _split_ledger_owner(custom_db, stack, custom_db.mixed_copy_targets)
        seal = await _predecessor_sealer(custom_db, stack)

        async def publish(connection, target, address, **options):
            readers = tuple(sorted(set(options["loader_roles"] + options["reader_roles"])))

            async def seal_candidate(copy_target):
                return await seal(copy_target, readers)

            return await original_publish(connection, target, address, **options, seal_candidate=seal_candidate)

        with monkeypatch.context() as settings:
            settings.setattr(office_fixtures, "_roles", retained_roles)
            settings.setattr(office_fixtures, "prepare_and_publish_network_candidate", publish)
            async with aclosing(
                _coordinator_office_db.__wrapped__(custom_db, tmp_path, settings, office_publisher)
            ) as generator:
                yield await anext(generator)


async def _predecessor_sealer(fixture, stack):
    """Use the existing independent login before publication, never the privileged fixture backend."""
    role = fixture.custody_roles["custodian"]
    await fixture.connection.execute(
        f'GRANT "{fixture.retained_address_owner}" TO "{role}" WITH ADMIN FALSE, INHERIT TRUE, SET TRUE'
    )
    custodian = await asyncpg.connect(
        fixture.custody_url.set(drivername="postgresql").render_as_string(hide_password=False)
    )
    stack.push_async_callback(custodian.close)

    async def seal(target, runtime_roles):
        assert role not in runtime_roles

        async def authorize(connection, supplied):
            return (
                connection is custodian
                and supplied == target
                and await connection.fetchval("SELECT current_user=session_user AND current_user::text=$1", role)
            )

        return await seal_network_candidate_custody(
            custodian, target, control_schema=fixture.control_schema, runtime_roles=runtime_roles, authorize=authorize
        )

    return seal


@pytest.fixture
async def serving_schema(cms_recipe_database, monkeypatch):
    """Keep genuine PTG authority and CMS/registry fixtures in one owned database."""
    dsn = cms_recipe_database.engine.url.set(drivername="postgresql").render_as_string(hide_password=False)
    with monkeypatch.context() as settings:
        settings.setenv("HLTHPRT_PTG2_V4_MIGRATION_POSTGRES_DSN", dsn)
        async with aclosing(cms_support.serving_schema.__wrapped__(cms_recipe_database, settings)) as registry_fixture:
            registry = await anext(registry_fixture)
            identity_sql = "SELECT current_database(),oid FROM pg_database WHERE datname=current_database()"
            ptg_connection = await asyncpg.connect(dsn)
            try:
                assert tuple(await ptg_connection.fetchrow(identity_sql)) == tuple(
                    await registry[0].fetchrow(identity_sql)
                )
            finally:
                await ptg_connection.close()
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


async def _split_ledger_owner(fixture, stack, copy_targets):
    """Keep independent seal authority separate from protected source-store ownership."""
    role = "ptg_seal_owner_" + uuid4().hex
    owner, custodian = fixture.roles["owner"], fixture.custody_roles["custodian"]
    ledger = f'"{fixture.control_schema}".network_candidate_custody'
    created_roles = []

    async def cleanup():
        await _remove_mixed_storage(fixture, copy_targets)
        if created_roles:
            await fixture.connection.execute(f'ALTER TABLE {ledger} OWNER TO "{owner}"')
            await fixture.connection.execute(f'DROP OWNED BY "{role}"')
            await fixture.connection.execute(f'DROP ROLE "{role}"')
            assert not await fixture.connection.fetchval("SELECT EXISTS(SELECT FROM pg_roles WHERE rolname=$1)", role)

    stack.push_async_callback(cleanup)
    await fixture.connection.execute(f'CREATE ROLE "{role}" NOLOGIN')
    created_roles.append(role)
    await fixture.connection.execute(f'ALTER TABLE {ledger} OWNER TO "{role}"')
    await fixture.connection.execute(f'GRANT USAGE ON SCHEMA "{fixture.control_schema}" TO "{role}"')
    database = await fixture.connection.fetchval("SELECT quote_ident(current_database())")
    await fixture.connection.execute(f'GRANT CREATE ON DATABASE {database} TO "{role}"')
    await fixture.connection.execute(f'REVOKE "{owner}" FROM "{custodian}"')
    await fixture.connection.execute(f'GRANT "{role}" TO "{custodian}" WITH ADMIN FALSE, INHERIT FALSE, SET TRUE')


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
    for role in cms[0][2].retained_epoch.runtime_roles:
        await fixture.connection.execute(
            f'GRANT "{role}" TO "{execution_role}" WITH ADMIN FALSE, INHERIT TRUE, SET FALSE'
        )
    runtime = RegistryPublicationPTGRuntime(
        async_sessionmaker(engine), service.store, RegistryPTGGraphReadBudget(16 * 1024 * 1024)
    )
    return runtime, execution


@pytest.fixture(params=(False, True), ids=("pure-ptg", "cms-ptg"))
async def mixed_ptg_custody(coordinator_db, reviewed_cms_source, reviewed_ptg_offices, request):
    """Prepare independent PTG/CMS custody actors and remove only their owned resources."""
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
        await prepare_network_candidate(
            execution, copy_target, address, **consumer.writer_roles, control_schema=fixture.control_schema
        )
        await _assert_mixed_source_rows(fixture, copy_target, recipes)
        custodian_sessions = _mixed_custodian_sessions(config, stack)
        yield SimpleNamespace(
            native=coordinator_db,
            fixture=fixture,
            target=copy_target,
            consumer=consumer,
            config=config,
            sessions=custodian_sessions,
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


async def test_genuine_mixed_candidate_requires_independent_fenced_session_then_commits(mixed_ptg_custody, monkeypatch):
    """Reject each incorrect custodian/session and commit only the independently fenced candidate."""
    case = mixed_ptg_custody
    fixture, copy_target = case.fixture, case.target
    options_by_name = await _mixed_custody_options(case)
    async with fixture.connection.transaction(isolation="repeatable_read", readonly=True):
        assert await fixture.connection.fetchval(
            "SELECT has_database_privilege($1,current_database(),'CREATE')", fixture.retained_address_owner
        )
        candidate = await fixture.connection.fetchrow(
            f'SELECT * FROM "{fixture.control_schema}".network_membership_candidate WHERE candidate_id=$1',
            UUID(copy_target.candidate_id),
        )
        await _retained_address_runtime_closure(
            fixture.connection, case.retained_source, options_by_name["runtime_roles"]
        )
        await _address_runtime_closure(fixture.connection, candidate, options_by_name["runtime_roles"])
    await _assert_wrong_custody_sessions(case, options_by_name)
    await _assert_mixed_native_tamper(case)
    await _assert_cancelled_mixed_seal(case, options_by_name)
    await _assert_mixed_sealed_replay(case, options_by_name, monkeypatch)


async def _mixed_consumer(native, runtime, execution, service, stack):
    """Configure the original independent custody consumer without changing its actors."""
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
        custody_url=_socket_url(fixture.custody_url),
        ptg=_PTGWorkerConfig(service.store, runtime.read_budget),
        sql_limits=fixture.sql_limits,
    )
    consumer = await _custody_runtime(config, stack, consumer, execution)
    return consumer, config


async def _assert_mixed_source_rows(fixture, copy_target, recipes):
    """Each actual source recipe must retain its distinct native network identity."""
    membership_records = await fixture.connection.fetch(
        f'SELECT network_id FROM "{copy_target.schema_name}".network_membership_source'
    )
    assert len(membership_records) == len(recipes) and len(
        {membership_record["network_id"] for membership_record in membership_records}
    ) == len(recipes)


def _mixed_custodian_sessions(config, stack):
    """Register the exact owned custodian engine before returning its session factory."""
    engine = create_async_engine(config.custody_url, pool_size=1, max_overflow=0)
    stack.push_async_callback(engine.dispose)
    return async_sessionmaker(engine)


async def _mixed_custody_options(case):
    """Capture the real independent custodian identity and its complete sealing options."""
    fixture, copy_target = case.fixture, case.target
    from process.registry_publication_worker import _custody_identity, _custody_runtime_roles

    async with fixture.execution_connection.transaction():
        identity = await _custody_identity(
            fixture.execution_connection, fixture.control_schema, fixture.custody_roles["execution"]
        )

    async def authorize(driver, supplied):
        return supplied == copy_target and await driver.fetchval(
            "SELECT current_user=session_user AND current_user::text=$1", fixture.custody_roles["custodian"]
        )

    options_by_name = dict(
        control_schema=fixture.control_schema,
        runtime_roles=_custody_runtime_roles(case.config, identity, case.consumer),
        authorize=authorize,
        office_store=case.config.ptg.store,
        office_read_budget=case.config.ptg.read_budget,
    )
    return options_by_name


async def _assert_wrong_custody_sessions(case, options_by_name):
    """Refuse unfenced, wrong-role, task-transferred and raw direct custody sessions."""
    fixture, copy_target = case.fixture, case.target
    with pytest.raises(ValueError, match="custody_unavailable"):
        await seal_network_candidate_custody_in_session(object(), copy_target, **options_by_name)
    async with case.sessions() as session, session.begin():
        with pytest.raises(ValueError, match="company_fence_unavailable"):
            await seal_network_candidate_custody_in_session(session, copy_target, **options_by_name)
    async with registry_company_approval_transaction(case.sessions, control_schema=fixture.control_schema) as session:
        with pytest.raises(ValueError, match="recipe_owner_unavailable"):
            await seal_network_candidate_custody_in_session(
                session, copy_target, **(options_by_name | {"office_store": object()})
            )
        from process.registry_ptg_office_membership import require_ptg_office_authority

        with pytest.raises(ValueError, match="recipe_owner_unavailable"):
            await require_ptg_office_authority(
                fixture.execution_connection,
                fixture.control_schema,
                (session, options_by_name["office_store"], options_by_name["office_read_budget"]),
            )
        with pytest.raises(ValueError, match="company_fence_unavailable"):
            await asyncio.create_task(
                seal_network_candidate_custody_in_session(session, copy_target, **options_by_name)
            )
    async with registry_company_approval_transaction(
        case.producer_sessions, control_schema=fixture.control_schema
    ) as session:

        async def has_custody_operator_authority(*args):
            return True

        with pytest.raises(ValueError, match="custody_unavailable"):
            await seal_network_candidate_custody_in_session(
                session, copy_target, **(options_by_name | {"authorize": has_custody_operator_authority})
            )
    async with case.sessions.kw["bind"].connect() as connection:
        driver = (await connection.get_raw_connection()).driver_connection
        with pytest.raises(ValueError, match="custody_unavailable"):
            await seal_network_candidate_custody(
                driver,
                copy_target,
                **{key: options_by_name[key] for key in ("control_schema", "runtime_roles", "authorize")},
            )


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


async def _assert_cancelled_mixed_seal(case, options_by_name):
    """Cancellation after the second authority check cannot leave a custody seal."""
    fixture, copy_target = case.fixture, case.target
    authorize = options_by_name["authorize"]
    calls = []

    async def cancel_after_seal(driver, supplied):
        calls.append(supplied)
        if len(calls) == 2:
            raise asyncio.CancelledError
        return await authorize(driver, supplied)

    async with registry_company_approval_transaction(case.sessions, control_schema=fixture.control_schema) as session:
        with pytest.raises(asyncio.CancelledError):
            await seal_network_candidate_custody_in_session(
                session, copy_target, **(options_by_name | {"authorize": cancel_after_seal})
            )
    assert calls == [copy_target, copy_target]
    assert (
        await fixture.connection.fetchval(
            f'SELECT count(*) FROM "{fixture.control_schema}".network_candidate_custody WHERE candidate_id=$1',
            UUID(copy_target.candidate_id),
        )
        == 0
    )


async def _assert_mixed_sealed_replay(case, options_by_name, monkeypatch):
    """Reverify one actual seal without reopening source authority or changing old serving."""
    fixture, copy_target = case.fixture, case.target
    await case.consumer.seal_candidate(copy_target)
    await case.consumer.seal_candidate(copy_target, pre_enrolled_only=True)

    async def no_recapture(*args, **kwargs):
        raise AssertionError("sealed verification must not reopen source authority")

    from process import network_source_contribution_custody as independent

    monkeypatch.setattr(independent, "stage_registry_source_recipes", no_recapture)
    async with case.sessions.kw["bind"].connect() as connection:
        driver = (await connection.get_raw_connection()).driver_connection
        sealed = await seal_network_candidate_custody(
            driver,
            copy_target,
            **{key: options_by_name[key] for key in ("control_schema", "runtime_roles", "authorize")},
        )
    assert sealed["seal_sha256"] == await fixture.connection.fetchval(
        f'SELECT seal_sha256 FROM "{fixture.control_schema}".network_candidate_custody WHERE candidate_id=$1',
        UUID(copy_target.candidate_id),
    )
    assert (
        await fixture.connection.fetchval(
            f'SELECT count(*) FROM "{fixture.control_schema}".network_candidate_custody WHERE candidate_id=$1',
            UUID(copy_target.candidate_id),
        )
        == 1
    )
    candidate = await fixture.connection.fetchrow(
        f'SELECT source_recipes_json::text AS recipes FROM "{fixture.control_schema}".network_membership_candidate WHERE candidate_id=$1',
        UUID(copy_target.candidate_id),
    )
    assert len(json.loads(candidate["recipes"])) == case.recipe_count
    assert await _retained_pin(case.native) == case.previous


def _mixed_address_sources(fixture, previous, retained_address, recipes):
    """Select the original admitted base and exact replacement recipes for this cohort."""
    return RegistryCompositionAddressSources(
        retained_address.address_source
        if retained_address is not None
        else PinnedAddressSource(previous.schema_name, "entity_address_unified", previous.manifest_sha256),
        replacement_source_recipes=recipes,
    )
