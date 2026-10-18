# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Record fixture DDL and publication boundaries without database authority claims."""

import json
from contextlib import asynccontextmanager
from contextvars import ContextVar
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from sqlalchemy import create_mock_engine
from sqlalchemy.sql.ddl import CreateIndex, CreateTable

from tests import provider_profile_snapshot_postgres_support as fixture


async def test_snapshot_doctors_fixtures_use_complete_native_models_and_indexes():
    statements = []
    engine = create_mock_engine("postgresql://", lambda statement, *args, **kwargs: statements.append(statement))

    async def run_sync(callback):
        return callback(engine)

    connection = SimpleNamespace(run_sync=run_sync)
    session = SimpleNamespace(connection=AsyncMock(return_value=connection), execute=AsyncMock())

    @asynccontextmanager
    async def transaction():
        yield session

    await fixture._create_doctors_families(SimpleNamespace(transaction=transaction), "snapshot_fixture")
    tables = [statement.element for statement in statements if isinstance(statement, CreateTable)]
    indexes = [statement.element for statement in statements if isinstance(statement, CreateIndex)]
    assert len(tables) == 6
    for model in fixture.doctors._models():
        for suffix in ("", "_next"):
            table = next(table for table in tables if table.name == model.__tablename__ + suffix)
            assert table.schema == "snapshot_fixture"
            assert [column.name for column in table.columns] == [column.name for column in model.__table__.columns]
            assert [column.name for column in table.primary_key] == [
                column.name for column in model.__table__.primary_key
            ]
            assert {tuple(column.name for column in index.columns) for index in indexes if index.table is table} == {
                tuple(spec["index_elements"]) for spec in getattr(model, "__my_additional_indexes__", ())
            }
    inserts = [call.args[0] for call in session.execute.await_args_list[:-1]]
    assert len(inserts) == 6 and all(statement.table.schema == "snapshot_fixture" for statement in inserts)


async def test_snapshot_cutover_publishes_through_original_doctors_owner(monkeypatch):
    events = []
    session = SimpleNamespace(execute=AsyncMock())
    component = SimpleNamespace(database=object(), factory=object())
    database = SimpleNamespace(_snapshot_doctors_component=component)
    reader_context = ContextVar("record_only_reader_context", default=None)
    reader_context.set("bound-reader")

    @asynccontextmanager
    async def factory_binding(actual):
        assert actual is component.factory and reader_context.get() is None
        yield

    previous = object()

    @asynccontextmanager
    async def transaction(actual):
        assert actual is component.database
        events.append("enter")
        yield session
        events.append("commit")

    async def capture(actual, schema):
        assert actual is session and schema == "snapshot_fixture"
        events.append("capture")
        return previous

    async def publish(actual, *, schema_name, expected_authority):
        assert actual is session and schema_name == "snapshot_fixture" and expected_authority is previous
        events.append("publish")

    monkeypatch.setattr(fixture, "bind_generic_source_transaction_factory", factory_binding)
    monkeypatch.setattr(fixture, "source_transaction", transaction)
    monkeypatch.setattr(fixture.doctors, "capture_doctors_predecessor", capture)
    monkeypatch.setattr(fixture.doctors, "publish_doctors_generation", publish)
    await fixture.publish_family(database, "snapshot_fixture")
    assert events == ["enter", "capture", "publish", "commit"]
    statements = [str(call.args[0]) for call in session.execute.await_args_list]
    assert statements[0] == "SET LOCAL statement_timeout = '5s'"
    assert len([statement for statement in statements if statement.startswith("ALTER TABLE")]) == 2 * len(
        fixture.TABLES
    )


@pytest.mark.parametrize(
    "name, allowed",
    [
        ("hc_network_registry_test_" + "1" * 32, True),
        ("hc_directory_entities_" + "a" * 32, True),
        ("hc_directory_entities_" + "a" * 31, False),
        ("hc_directory_entities_" + "a" * 33, False),
        ("hc_directory_entities_" + "A" * 32, False),
        ("prefix_hc_directory_entities_" + "a" * 32, False),
        ("hc_directory_entities_" + "a" * 32 + "_suffix", False),
        ("hc_directory_entities_production", False),
        (None, False),
    ],
)
def test_native_source_fixture_preserves_exact_entities_database_guard(name, allowed):
    from tests.provider_directory_source_postgres_support import _is_component_test_database

    assert _is_component_test_database(name) is allowed


@pytest.mark.parametrize("reused", [False, True])
@pytest.mark.parametrize("changed", [False, True])
async def test_profile_observer_forwards_verified_read_origin_and_preserves_catalog_checks(
    monkeypatch, reused, changed
):
    from tests.provider_directory_geo_profile9_acceptance_support import ProfileAcceptance

    origin = object() if reused else None
    result = object()
    delegate = AsyncMock(return_value=result)
    reads = SimpleNamespace(_attest=delegate)
    observation = ProfileAcceptance(object(), cancel_reader=False)
    observation.validated = True
    observation.relations = {"profile_live": "profile_stage"}
    observation.catalogs = {"profile_live": '{"columns": ["id"], "indexes": []}'}
    scope = SimpleNamespace(names_json='{"profile_live": "profile_stage"}')
    columns = ["changed"] if changed else ["id"]
    state_by_field = {"catalog_by_root": json.dumps({"profile_live": {"columns": columns, "indexes": "[]"}})}
    observation._observe_reads(monkeypatch, reads, SimpleNamespace(decoded=json.loads))
    if changed:
        with pytest.raises(AssertionError):
            await reads._attest(scope, state_by_field, reusable_origin=origin)
        assert observation.reads == []
    else:
        assert await reads._attest(scope, state_by_field, reusable_origin=origin) is result
        assert observation.reads == [("profile_live",)]
    delegate.assert_awaited_once_with(scope, state_by_field, reusable_origin=origin)


@pytest.mark.parametrize("complete", [False, True])
async def test_detail_nppes_composition_preserves_completed_migrations_and_actual_custody(monkeypatch, complete):
    from tests import nppes_candidate_postgres_support as nppes
    from tests.cms_registry_complete_publication_postgres_support import source_component_native_migrations

    @asynccontextmanager
    async def begin():
        yield SimpleNamespace(run_sync=AsyncMock())

    component = SimpleNamespace(
        admin_engine=SimpleNamespace(url=object(), begin=begin),
        policy=SimpleNamespace(schema="mrf"),
        factory=SimpleNamespace(_native_input_producer=object(), _require_current_policy=lambda: None),
    )
    migrations = source_component_native_migrations() if complete else None

    @asynccontextmanager
    async def attest(actual):
        assert actual is component
        yield actual

    for name in (
        "_install_nppes_historical_storage",
        "_install_existing_npi_publication",
        "_enroll_native_custody",
        "_drop_nppes_component_roles",
    ):
        monkeypatch.setattr(nppes, name, AsyncMock())
    monkeypatch.setattr(nppes, "existing_nppes_component", attest)
    token = nppes.source._FACTORY.set(component.factory)
    try:
        async with nppes.composed_nppes_component(component, migration_prefixes=migrations) as enrolled:
            assert enrolled is component
    finally:
        nppes.source._FACTORY.reset(token)
    assert nppes._install_nppes_historical_storage.await_count == (0 if complete else 1)
    assert nppes._install_existing_npi_publication.await_count == (0 if complete else 1)
    if not complete:
        nppes._install_existing_npi_publication.assert_awaited_once_with(component.admin_engine, "mrf")
    assert nppes._enroll_native_custody.await_count == 1
    assert nppes._enroll_native_custody.await_args.kwargs == {"native_inputs": True}
    assert nppes._drop_nppes_component_roles.await_count == 1


async def test_detail_nppes_composition_rejects_unknown_migration_profile_before_mutation():
    from tests import nppes_candidate_postgres_support as nppes

    with pytest.raises(RuntimeError, match="^caller_owned_nppes_migration_profile_required$"):
        async with nppes.composed_nppes_component(object(), migration_prefixes=("20260825090000",)):
            pytest.fail("An incomplete profile cannot reach custody setup")


async def test_detail_reader_fixture_does_not_add_publisher_to_closed_control_acl():
    from tests import provider_profile_detail_load_postgres_support as detail

    connection = SimpleNamespace(execute=AsyncMock())
    component = SimpleNamespace(
        policy=SimpleNamespace(
            owner_role="synthetic_owner",
            publisher_role="synthetic_publisher",
            runtime_roles=("synthetic_reader", "synthetic_builder"),
        )
    )
    oids_by_relation = {"mrf.provider_directory_cms_serving_receipt": 42}
    await detail._grant_detail_reads(connection, component, oids_by_relation, oids_by_relation)
    statements = [str(call.args[0]) for call in connection.execute.await_args_list]
    assert len(statements) == 3
    assert all("synthetic_publisher" not in statement for statement in statements)
    assert {statement.rsplit(" TO ", 1)[1] for statement in statements} == {
        '"synthetic_owner"',
        '"synthetic_reader"',
        '"synthetic_builder"',
    }


async def test_detail_incumbent_setup_uses_administration_without_rebinding_source(monkeypatch):
    from tests import provider_profile_detail_load_postgres_support as detail

    session = SimpleNamespace(scalar=AsyncMock(return_value=True))

    @asynccontextmanager
    async def transaction():
        yield session

    administrator = SimpleNamespace(transaction=transaction)
    fixture = SimpleNamespace(engine=object(), database=object(), schema="mrf")
    source_database = fixture.database
    constructor = Mock()

    def database(**kwargs):
        constructor(**kwargs)
        return administrator

    monkeypatch.setattr(detail, "Database", database)
    monkeypatch.setattr(detail, "async_sessionmaker", lambda engine, **kwargs: (engine, kwargs))
    for name in ("_seed_identity_and_locations", "_seed_enrichment", "_seed_projected_profile", "_insert"):
        monkeypatch.setattr(detail, name, AsyncMock())
    monkeypatch.setattr(detail.address_generation, "publish_local_entity_address_generation", AsyncMock())
    await detail._seed_detail_incumbents(fixture)
    assert fixture.database is source_database
    assert constructor.call_args.kwargs == {
        "engine": fixture.engine,
        "session_factory": (fixture.engine, {"expire_on_commit": False}),
    }
    detail._seed_identity_and_locations.assert_awaited_once_with(session)
    detail._seed_enrichment.assert_awaited_once_with(session)
    detail._seed_projected_profile.assert_awaited_once_with(session, "mrf")
    assert detail._insert.await_args.args == (session, detail.models.ImportRun)
    detail.address_generation.publish_local_entity_address_generation.assert_not_awaited()


async def test_detail_nppes_fixture_grants_only_clone_reads_and_metadata_append():
    from tests import provider_profile_detail_load_postgres_support as detail

    connection = SimpleNamespace(execute=AsyncMock())
    await detail._grant_nppes_fixture_inputs(connection, SimpleNamespace(schema="mrf", owner_role="synthetic_owner"))
    statements = {str(call.args[0]) for call in connection.execute.await_args_list}
    expected_grants = {
        f'GRANT SELECT ON TABLE "mrf"."{fields[0]}" TO "synthetic_owner"'
        for fields in detail.nppes_storage.ROOTS.values()
    }
    expected_grants.update(
        f'GRANT SELECT,INSERT ON TABLE "mrf"."{table}" TO "synthetic_owner"'
        for table in ("public_evidence_source_identity", "public_evidence_source_release")
    )
    foundation = detail.receipt_fixture._migration("20260808090000")
    expected_grants.update(
        f'GRANT EXECUTE ON FUNCTION "mrf"."{function}"({arguments}) TO "synthetic_owner"'
        for function, arguments in (
            ("public_evidence_source_identity_ref", "text,text,bytea"),
            ("public_evidence_source_release_valid", foundation._RELEASE_VALIDATOR_ARGUMENT_TYPES),
            ("public_evidence_nppes_archive_period", "text"),
            ("public_evidence_nppes_primary_period", "text"),
            ("public_evidence_record_digest", "text,text"),
            ("public_evidence_record_ref", "text,text,text"),
        )
    )
    assert statements == expected_grants
    assert connection.execute.await_count == 13


async def test_detail_receipt_validator_alignment_preserves_identity_body_and_explicit_grants():
    from tests import provider_profile_detail_load_postgres_support as detail

    before_by_field = {
        "oid": 42,
        "prosrc": "BEGIN RETURN NULL; END",
        "prosecdef": True,
        "proconfig": ["search_path=pg_catalog"],
        "owner": "synthetic_builder",
        "administrator": "synthetic_administrator",
        "explicit_grants": [[43, "EXECUTE", False]],
    }

    def result(values):
        return SimpleNamespace(mappings=lambda: SimpleNamespace(one=lambda: values))

    connection = SimpleNamespace(
        execute=AsyncMock(
            side_effect=[
                result(before_by_field),
                None,
                result({**before_by_field, "owner": "synthetic_administrator"}),
            ]
        )
    )
    await detail._align_npi_receipt_validator_owner(
        connection, SimpleNamespace(schema="mrf", control_runtime_roles=("synthetic_builder",))
    )
    assert str(connection.execute.await_args_list[1].args[0]) == (
        'ALTER FUNCTION "mrf"."validate_npi_canonical_publication_receipt"() OWNER TO CURRENT_USER'
    )
