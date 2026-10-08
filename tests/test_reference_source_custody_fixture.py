# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Host regressions for the transaction-bound native SOURCE fixture capabilities."""

import ast
from contextlib import AsyncExitStack, asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest

from process import reference_family_archive as archive
from tests.reference_family_generation_fixture import ReferenceSourceCustody


@pytest.mark.parametrize(
    ("module_name", "expected_clones", "expected_exports"),
    [
        ("cms_doctors_archive", 2, 1),
        ("tiger_result_archive", 1, 1),
        ("tiger_captured_epoch", 2, 1),
        ("tiger_snapshot_inheritance", 1, 0),
        ("label_archive", 1, 1),
    ],
)
def test_native_fixture_clone_and_export_calls_keep_custody(module_name, expected_clones, expected_exports):
    """Every new fixture clone declares bounded COPY and its real empty-stage owner callback."""
    path = Path(__file__).with_name(f"test_{module_name}_postgres.py")
    calls = [node for node in ast.walk(ast.parse(path.read_text())) if isinstance(node, ast.Call)]
    count_by_kind = {"clone": 0, "export": 0}
    for call in calls:
        name = call.func.attr if isinstance(call.func, ast.Attribute) else getattr(call.func, "id", None)
        keyword_by_name = {keyword.arg: ast.unparse(keyword.value) for keyword in call.keywords}
        if name in {"prepare_reference_family_archive_source", "prepare_captured_tiger_epoch"}:
            count_by_kind["clone"] += 1
            assert keyword_by_name["source_copy"] == "custody.source_copy"
            expected = "refuse_callback" if keyword_by_name["on_prepared"] == "refuse_callback" else "custody.precreate"
            assert keyword_by_name["on_precreated"] == expected
        elif name == "export_prepared_reference_family_archive":
            count_by_kind["export"] += 1
            assert keyword_by_name["verify_custody"] == "custody.verify"
    assert count_by_kind == {"clone": expected_clones, "export": expected_exports}


@pytest.mark.asyncio
async def test_readable_archive_uses_retained_owner_for_activation(monkeypatch):
    """Reader cutover must validate the closed owner, not the bootstrap session's identity."""
    from tests import test_cms_doctors_archive_postgres as fixture

    ownership = _ownership()
    custody = ReferenceSourceCustody("synthetic_owner", ())
    custody.retain, custody.verify = AsyncMock(), AsyncMock()
    prepared = SimpleNamespace(
        ownership=ownership,
        manifest=SimpleNamespace(as_dict=lambda: {}, source_serving_generation=SimpleNamespace(as_dict=lambda: {})),
    )
    session = SimpleNamespace(scalar=AsyncMock(return_value=712))

    @asynccontextmanager
    async def transaction():
        yield session

    session.begin = transaction
    observed = SimpleNamespace(database=SimpleNamespace(session_factory=transaction), schema="synthetic")

    async def prepare(_sessions, **options):
        assert options["source_copy"] is custody.source_copy
        assert options["on_precreated"] == custody.precreate
        await options["on_prepared"](session, prepared)
        return prepared

    monkeypatch.setattr(fixture, "_create_source", AsyncMock())
    monkeypatch.setattr(archive, "prepare_reference_family_archive_source", prepare)
    monkeypatch.setattr(archive, "capture_reference_family_incumbent", AsyncMock())
    validate = AsyncMock()
    monkeypatch.setattr(archive, "prepare_reference_family_activation", validate)
    activation_by_field = await fixture._prepare_readable_archive(observed, ownership.dataset_id, custody)
    custody.retain.assert_awaited_once_with(session, prepared)
    custody.verify.assert_awaited_once_with(session, prepared)
    assert session.scalar.call_args.args[1] == {"owner": custody.owner}
    assert validate.call_args.kwargs["sealed_owner_oid"] == 712
    assert (
        activation_by_field["cutover"].sealed_owner_oid
        == activation_by_field["cutover"].expected_stage_owner_oid
        == 712
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ["roundtrip", "continuous", "late"])
@pytest.mark.parametrize("cleanup_fails", [False, True])
async def test_clinician_fixture_drains_owner_and_engine_after_failure(monkeypatch, tmp_path, case, cleanup_fails):
    """Schema cleanup precedes owner removal; either failure still drains the engine."""
    from tests import test_cms_doctors_archive_postgres as fixture

    events = []

    async def fail_source(*_args, **_kwargs):
        raise RuntimeError("synthetic source failure")

    async def drop_schemas(*_args):
        events.append("schemas")
        if cleanup_fails:
            raise RuntimeError("synthetic cleanup failure")

    @asynccontextmanager
    async def custody_context(_sessions):
        try:
            yield ReferenceSourceCustody("synthetic_owner", ())
        finally:
            events.append("owner")

    @asynccontextmanager
    async def transaction():
        yield SimpleNamespace(begin=transaction)

    engine = SimpleNamespace(dispose=AsyncMock(side_effect=lambda: events.append("engine")))
    monkeypatch.setattr(fixture, "_database_url", lambda: "postgresql+asyncpg://localhost/unused")
    monkeypatch.setattr(fixture, "create_async_engine", lambda *_args: engine)
    monkeypatch.setattr(fixture, "async_sessionmaker", lambda *_args, **_kwargs: transaction)
    monkeypatch.setattr(fixture, "native_reference_source", custody_context)
    monkeypatch.setattr(fixture, "_drop_test_schemas", drop_schemas)
    monkeypatch.setattr(fixture, "_create_source", fail_source)
    with pytest.raises(RuntimeError, match="synthetic .* failure"):
        if case == "roundtrip":
            await fixture.test_clinician_migration_and_native_archive_are_one_closed_generation(
                monkeypatch, tmp_path, True
            )
        elif case == "continuous":
            await fixture.test_validated_archive_completes_with_continuous_snapshot_readers(monkeypatch)
        else:
            await fixture.test_archive_activation_preserves_late_readers(monkeypatch, True)
    assert events == (["schemas", "owner", "engine"] if case == "roundtrip" else ["schemas", "engine"])


@pytest.mark.asyncio
@pytest.mark.parametrize("cleanup_fails", [False, True])
@pytest.mark.parametrize("clone_fails", [False, True])
async def test_readable_archive_admits_reader_before_custody_and_drains_after_failure(
    monkeypatch, cleanup_fails, clone_fails
):
    """Stage grants disappear before owner and Reader removal; current schema retires last."""
    from tests import test_cms_doctors_archive_postgres as fixture

    events = []
    dataset_id = uuid4()
    observed = SimpleNamespace(engine=object(), schema="synthetic", database=SimpleNamespace())

    @asynccontextmanager
    async def transaction():
        yield SimpleNamespace(begin=transaction)

    async def create_source(_session, schema):
        assert schema == observed.schema
        events.append("source")

    reader_context, custody_context = _readable_authority_contexts(observed, events, transaction)

    async def prepare_clone(actual, identity, custody):
        assert actual is observed and identity == dataset_id and custody.readers == ("synthetic_reader",)
        events.append("clone")
        if clone_fails:
            raise RuntimeError("synthetic clone failure")
        return "prepared"

    async def drop_schemas(_engine, schemas):
        if schemas == (observed.schema,):
            events.append("source_cleanup")
        else:
            assert schemas == (
                archive.reference_family_stage_schema(dataset_id),
                archive.reference_family_predecessor_schema(dataset_id),
            )
            events.append("stage_cleanup")
            if cleanup_fails:
                raise RuntimeError("synthetic cleanup failure")

    observed.database.session_factory = transaction
    monkeypatch.setattr(fixture, "_create_source", create_source)
    monkeypatch.setattr(fixture, "profile_reader", reader_context)
    monkeypatch.setattr(fixture, "native_reference_source", custody_context)
    monkeypatch.setattr(fixture, "_prepare_readable_archive", prepare_clone)
    monkeypatch.setattr(fixture, "_drop_test_schemas", drop_schemas)
    monkeypatch.setattr(
        fixture, "_drop_readable_serving_tables", AsyncMock(side_effect=lambda *_args: events.append("serving_cleanup"))
    )
    with pytest.raises(RuntimeError, match="synthetic .* failure"):
        async with AsyncExitStack() as cleanup:
            cleanup.push_async_callback(AsyncMock(side_effect=lambda: events.append("engine_cleanup")))
            async with fixture._readable_archive(observed, dataset_id, monkeypatch) as prepared:
                assert not clone_fails and prepared == "prepared"
                raise RuntimeError("synthetic caller failure")
    assert ",".join(events) == (
        "source,reader,custody,clone,stage_cleanup,serving_cleanup,owner_cleanup,"
        "reader_cleanup,source_cleanup,engine_cleanup"
    )


@pytest.mark.asyncio
async def test_promoted_reader_fixture_cleanup_retires_only_declared_payloads():
    """Leave the current namespace and authority metadata for later Reader/schema cleanup."""
    from tests import test_cms_doctors_archive_postgres as fixture

    connection = SimpleNamespace(execute=AsyncMock())

    @asynccontextmanager
    async def transaction():
        yield connection

    await fixture._drop_readable_serving_tables(SimpleNamespace(begin=transaction), "synthetic")
    assert str(connection.execute.call_args.args[0]) == (
        'DROP TABLE IF EXISTS "synthetic"."doctor_clinician_address", '
        '"synthetic"."cms_doctor_education", "synthetic"."cms_doctor_group_site" RESTRICT'
    )


def _readable_authority_contexts(observed, events, transaction):
    """Bind the mocked authority contexts to the exact Reader admitted after source creation."""

    @asynccontextmanager
    async def reader_context(database, schema, _patch):
        assert events == ["source"] and schema == observed.schema
        database._reader_database = SimpleNamespace(_reader_login=("synthetic_reader", "unused"))
        events.append("reader")
        try:
            yield
        finally:
            events.append("reader_cleanup")

    @asynccontextmanager
    async def custody_context(sessions, *, readers):
        assert sessions is transaction and readers == ("synthetic_reader",)
        assert events == ["source", "reader"]
        events.append("custody")
        try:
            yield ReferenceSourceCustody("synthetic_owner", readers)
        finally:
            events.append("owner_cleanup")

    return reader_context, custody_context


def _ownership():
    dataset_id = uuid4()
    return archive.ReferenceFamilyStageOwnership(
        "places-zcta",
        dataset_id,
        archive.reference_family_stage_schema(dataset_id),
        12,
        (("pricing_places_zcta", 13),),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("observations", [(False,), (True, True), (True, False, True)])
async def test_precreation_refuses_foreign_nonempty_or_indexed_heap(monkeypatch, observations):
    """Ownership, emptiness and deferred indexes are all required before custody binds."""
    monkeypatch.setattr(archive, "verify_reference_family_stage_ownership", AsyncMock())
    session = SimpleNamespace(in_transaction=lambda: True, scalar=AsyncMock(side_effect=observations))
    custody = ReferenceSourceCustody("synthetic_owner", ())
    with pytest.raises(AssertionError):
        await custody.precreate(session, _ownership())
    assert custody.precreated == {}


@pytest.mark.asyncio
async def test_retention_requires_same_live_transaction_and_original_oids():
    """Neither reopening a transaction nor replacing an empty heap authenticates its handoff."""
    ownership = _ownership()
    transaction = SimpleNamespace(is_active=True)
    custody = ReferenceSourceCustody("synthetic_owner", ())
    custody.precreated[ownership.dataset_id] = (transaction, ownership)
    session = SimpleNamespace(get_transaction=lambda: SimpleNamespace(is_active=True))
    with pytest.raises(AssertionError):
        await custody.retain(session, SimpleNamespace(ownership=ownership))
    session.get_transaction = lambda: transaction
    foreign = archive.ReferenceFamilyStageOwnership(
        ownership.importer_id,
        ownership.dataset_id,
        ownership.schema_name,
        ownership.schema_oid,
        (("pricing_places_zcta", 14),),
    )
    with pytest.raises(AssertionError):
        await custody.retain(session, SimpleNamespace(ownership=foreign))
    assert custody.closed == {}


@pytest.mark.asyncio
async def test_reopened_source_refuses_catalog_drift(monkeypatch):
    """An authentic ownership token does not authorize a changed closed catalog."""
    monkeypatch.setattr(archive, "verify_reference_family_stage_ownership", AsyncMock())
    ownership = _ownership()
    custody = ReferenceSourceCustody("synthetic_owner", ())
    custody.closed[ownership.dataset_id] = ("original-native-catalog",)
    custody._receipt = AsyncMock(return_value=("changed-native-catalog",))
    with pytest.raises(AssertionError):
        await custody.verify(object(), SimpleNamespace(ownership=ownership))


@pytest.mark.asyncio
async def test_verified_retirement_releases_only_original_namespace(monkeypatch):
    """Restore may reuse a UUID only after exact native cleanup succeeds."""
    cleanup = AsyncMock()
    monkeypatch.setattr(archive, "cleanup_reference_family_stage", cleanup)
    ownership = _ownership()
    custody = ReferenceSourceCustody("synthetic_owner", ())
    custody.precreated[ownership.dataset_id] = (object(), ownership)
    custody.schemas.add(ownership.schema_name)
    await custody.retire(object(), ownership)
    cleanup.assert_awaited_once()
    assert custody.schemas == set()


@pytest.mark.asyncio
async def test_mrf_preparation_uses_independent_source_transaction():
    """Bootstrap/clone privileges must not become SOURCE executable-owner authority."""
    from tests.test_mrf_publication_receipt_postgres import _prepare_native_mrf_source

    prepare = AsyncMock(return_value="prepared")
    custody = ReferenceSourceCustody("synthetic_owner", ())
    clone_sessions, source_sessions = object(), object()
    assert (
        await _prepare_native_mrf_source(
            clone_sessions,
            SimpleNamespace(prepare_reference_family_archive_source=prepare),
            "synthetic",
            custody,
            source_sessions,
        )
        == "prepared"
    )
    assert prepare.call_args.args == (clone_sessions,)
    assert prepare.call_args.kwargs["source_sessions"] is source_sessions
    assert prepare.call_args.kwargs["on_precreated"] == custody.precreate
    assert prepare.call_args.kwargs["on_prepared"] == custody.retain
    assert prepare.call_args.kwargs["source_copy"] is custody.source_copy


@pytest.mark.asyncio
async def test_mrf_source_actor_keeps_code_owners_independent_and_reaps_engine(monkeypatch):
    """Initial grants cover real future replacements without payload DML or code-owner membership."""
    from tests import test_mrf_publication_receipt_postgres as fixture

    source_role = "mrf_source_" + "1" * 32
    statements = []
    engine = SimpleNamespace(dispose=AsyncMock())
    driver = SimpleNamespace(execute=AsyncMock(side_effect=lambda statement: statements.append(str(statement))))
    connection = SimpleNamespace(get_raw_connection=AsyncMock(return_value=SimpleNamespace(driver_connection=driver)))
    administrator = SimpleNamespace(
        execute=AsyncMock(side_effect=lambda statement: statements.append(str(statement))),
        scalar=AsyncMock(return_value=1),
        connection=AsyncMock(return_value=connection),
    )
    source_session = SimpleNamespace(scalar=AsyncMock(side_effect=[source_role, source_role, False, False, False]))

    @asynccontextmanager
    async def admin_transaction():
        yield administrator

    @asynccontextmanager
    async def source_transaction():
        yield source_session

    admin_sessions = SimpleNamespace(begin=admin_transaction)
    source_sessions = SimpleNamespace(begin=source_transaction)
    create_engine = Mock(return_value=engine)
    monkeypatch.setattr(fixture, "uuid4", lambda: SimpleNamespace(hex="1" * 32))
    monkeypatch.setattr(fixture, "create_async_engine", create_engine)
    monkeypatch.setattr(fixture, "async_sessionmaker", lambda *_args, **_kwargs: source_sessions)
    with pytest.raises(RuntimeError, match="synthetic failure"):
        async with fixture._native_mrf_source_sessions(
            admin_sessions, "synthetic", "postgresql+asyncpg://localhost/unused"
        ) as actual:
            assert actual is source_sessions
            engine.dispose.assert_not_awaited()
            raise RuntimeError("synthetic failure")
    engine.dispose.assert_awaited_once()
    assert create_engine.call_args.args[0].username == source_role
    assert create_engine.call_args.kwargs["hide_parameters"] is True
    assert "NOSUPERUSER" in statements[0] and "NOBYPASSRLS" in statements[0]
    assert all("UPDATE" not in statement and "GRANT ALL" not in statement for statement in statements)
    assert any(
        'ALTER DEFAULT PRIVILEGES IN SCHEMA "synthetic" GRANT SELECT,MAINTAIN' in statement for statement in statements
    )
    assert statements[-2:] == [f'DROP OWNED BY "{source_role}"', f'DROP ROLE "{source_role}"']


@pytest.mark.asyncio
@pytest.mark.parametrize("importer", ["mrf", "plan-attributes"])
async def test_source_fixture_indexes_match_completed_model_without_extra_indexes(monkeypatch, importer):
    """Cold source provisioning must match the actual producer's copied index layout."""
    from tests import test_reference_family_archive_postgres as fixture

    monkeypatch.setattr(fixture, "install_source_generation_guards", AsyncMock())
    source_session = SimpleNamespace(execute=AsyncMock())
    stage_session = SimpleNamespace(execute=AsyncMock())
    await fixture._create_live_family(source_session, importer, "synthetic")
    await archive._create_model_indexes(stage_session, archive.reference_family_spec(importer), "synthetic")
    source_indexes = [
        str(call.args[0])
        for call in source_session.execute.call_args_list
        if str(call.args[0]).startswith("CREATE INDEX")
    ]
    stage_indexes = [
        str(call.args[0])
        for call in stage_session.execute.call_args_list
        if str(call.args[0]).startswith("CREATE INDEX")
    ]
    assert sorted(source_indexes) == sorted(stage_indexes)
    if importer == "mrf":
        assert len(source_indexes) == 20
        assert sum('ON "synthetic"."mrf_address" (checksum)' in statement for statement in source_indexes) == 1
        for table in ("plan", "plan_drug_raw", "plan_npi_raw", "plan_networktier"):
            assert not any(f'ON "synthetic"."{table}" ' in statement for statement in source_indexes)


@pytest.mark.asyncio
@pytest.mark.parametrize("importer", ["mrf", "mrf-address"])
async def test_compiled_mrf_indexes_retain_actual_ordinary_initial_declarations(monkeypatch, importer):
    """New heaps keep the ordinary writer's checksum index without enrolling excluded indexes."""
    from process import initial

    compiled_spec = archive.reference_family_spec(importer)
    copied_tables = {"plan_benefits_marketplace", "mrf_address", "mrf_address_evidence", "plan_search_summary"}
    ordinary_indexes = []
    models_by_table = {model.__tablename__: model for model in compiled_spec.model_types}

    def capture_ordinary_index(table_name, definition, schema):
        ordinary_indexes.append(archive._additional_index_sql(schema, models_by_table[table_name], definition))
        return "captured ordinary declaration"

    monkeypatch.setattr(initial, "_create_index_sql", capture_ordinary_index)
    monkeypatch.setattr(initial.db, "status", AsyncMock())
    for model in compiled_spec.model_types:
        if model.__tablename__ in copied_tables:
            stage = initial.make_class(model, "synthetic_" + uuid4().hex[:12], schema_override="synthetic")
            models_by_table[stage.__tablename__] = model
            await initial._create_named_indexes(stage, "synthetic")
    session = SimpleNamespace(execute=AsyncMock())
    await archive._create_model_indexes(session, compiled_spec, "synthetic")
    compiled_indexes = [str(call.args[0]) for call in session.execute.call_args_list]
    assert sorted(compiled_indexes) == sorted(ordinary_indexes)
    assert sum('ON "synthetic"."mrf_address" (checksum)' in statement for statement in compiled_indexes) == 1
