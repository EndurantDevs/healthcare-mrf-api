# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Host call-flow checks for complete scoped composition and its caller-owned transaction."""

from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest

from db.models import CodeCatalog
from process import code_sets_result_archive as codes
from process import reference_family_archive as native
from process import scoped_catalog_named_cutover as cutover
from process import scoped_catalog_named_generation as generations
from process import scoped_catalog_named_retention as retention
from process import scoped_catalog_named_storage as storage
from process import scoped_catalog_publication as publication
from tests.test_scoped_catalog_handoff import _generation


def _session():
    return SimpleNamespace(in_transaction=lambda: True, scalar=AsyncMock(return_value="77"), execute=AsyncMock())


def _input(importer="ms-drg"):
    spec = publication.input_spec(importer)
    dataset_id = uuid4()
    pairs = tuple((name, index + 10) for index, name in enumerate(sorted(spec.table_names)))
    ownership = native.ReferenceFamilyStageOwnership(
        spec.importer_id, dataset_id, native.reference_family_stage_schema(dataset_id), 9, pairs, (), None
    )
    return publication.CatalogInput(spec, ownership)


def test_scoped_input_refuses_unknown_producer():
    """Only the two compiled catalog producers can obtain a native input model inventory."""
    with pytest.raises(native.ReferenceFamilyArchiveError, match="producer is unsupported"):
        publication.input_spec("other")


@pytest.mark.asyncio
@pytest.mark.parametrize("oids", [[10, None], [None, 11]])
async def test_current_family_refuses_partial_synonym_relationship_inventory(monkeypatch, oids):
    """A partially installed MS-DRG family cannot be hidden by a code-only producer."""
    monkeypatch.setattr(native, "_relation_oid", AsyncMock(side_effect=oids))
    with pytest.raises(RuntimeError, match="family is incomplete"):
        await storage.current_models(_session(), "serving")


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["missing", False, None])
async def test_named_tables_refuse_missing_or_unsafe_structure(monkeypatch, failure):
    session = _session()
    session.scalar.return_value = failure
    monkeypatch.setattr(native, "_lock_family", AsyncMock())
    monkeypatch.setattr(native, "_relation_oid", AsyncMock(return_value=None if failure == "missing" else 1))
    read_structure = AsyncMock()
    columns = AsyncMock()
    monkeypatch.setattr(storage, "_require_native_read_structure", read_structure)
    monkeypatch.setattr(storage.binding, "_require_model_columns", columns)
    with pytest.raises(RuntimeError, match="relation is unavailable|executable structure differs"):
        await storage.require_tables(session, "serving", (CodeCatalog,))
    native._lock_family.assert_awaited_once_with(session, "serving", ("code_catalog",), "ACCESS SHARE", nowait=True)
    read_structure.assert_not_awaited()
    columns.assert_not_awaited()
    if failure == "missing":
        session.scalar.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("names", "functions"),
    [
        (["code_catalog"], False),
        ([], False),
        (["code_catalog", "unexpected"], False),
        (["code_catalog"], True),
        (["code_catalog"], None),
    ],
)
async def test_named_candidate_requires_exact_heap_and_function_inventory(names, functions):
    session = _session()
    session.execute.return_value = SimpleNamespace(scalars=lambda: names)
    session.scalar.return_value = functions
    if names == ["code_catalog"] and functions is False:
        await storage.require_inventory(session, "candidate", ("code_catalog",))
    else:
        with pytest.raises(RuntimeError, match="candidate object inventory differs"):
            await storage.require_inventory(session, "candidate", ("code_catalog",))
    assert all(str(call.args[0]).startswith("SELECT ") for call in session.execute.await_args_list)


@pytest.mark.asyncio
@pytest.mark.parametrize("namespace", [False, True])
async def test_native_privilege_destination_enumeration_is_bounded(namespace):
    session = _session()
    session.execute.return_value = SimpleNamespace(scalars=lambda: [f"reader_{index}" for index in range(2049)])
    with pytest.raises(RuntimeError, match="privilege destinations exceed the bound"):
        await storage._acl_grantees(session, "candidate", ("code_catalog",), namespace=namespace)
    assert str(session.execute.await_args.args[0]).endswith("ORDER BY 1 LIMIT 2049")
    session.scalar.assert_not_awaited()


@pytest.mark.asyncio
async def test_candidate_protection_keeps_its_owner_while_revoking_public_access(monkeypatch):
    session = _session()
    session.execute.return_value = SimpleNamespace(scalars=lambda: ["code"])
    monkeypatch.setattr(native, "_lock_family", AsyncMock())
    monkeypatch.setattr(storage, "_acl_grantees", AsyncMock(side_effect=[["catalog_owner"], []]))
    settings = SimpleNamespace(owner_role="catalog_owner", reader_roles=())
    await storage.protect_tables(session, "candidate", (CodeCatalog,), settings, candidate=True, nowait=False)
    statements = [str(call.args[0]) for call in session.execute.await_args_list]
    assert 'ALTER SCHEMA "candidate" OWNER TO "catalog_owner"' in statements
    assert 'REVOKE ALL ON SCHEMA "candidate" FROM PUBLIC' in statements
    assert not any(statement.startswith("REVOKE") and statement.endswith('"catalog_owner"') for statement in statements)
    native._lock_family.assert_awaited_once_with(
        session, "candidate", ("code_catalog",), "ACCESS EXCLUSIVE", nowait=False
    )


@pytest.mark.asyncio
async def test_composition_requires_bounded_native_copy_before_allocating_candidate(monkeypatch):
    stage = AsyncMock()
    monkeypatch.setattr(storage, "create_stage", stage)
    with pytest.raises(RuntimeError, match="bounded native COPY is required"):
        await cutover._compose(_session(), {}, storage.MODELS, object())
    stage.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("importer", ["code-sets", "ms-drg"])
async def test_rollback_refuses_exhausted_revision_before_update(importer):
    logical_by_field = {"live_schema": "serving", "previous_generations": {importer: _generation()}}
    session = _session()
    with pytest.raises(RuntimeError, match="rollback generation is exhausted"):
        await cutover._restore_generation(
            session, logical_by_field, {importer: {"local_generation": 2**63 - 1}}, importer
        )
    session.execute.assert_not_awaited()
    session.scalar.assert_not_awaited()


@pytest.mark.asyncio
async def test_rollback_refuses_different_destination_before_locking(monkeypatch):
    monkeypatch.setattr(retention, "require_retained", AsyncMock(return_value={"live_schema": "other"}))
    family = AsyncMock()
    monkeypatch.setattr(storage, "require_tables", family)
    with pytest.raises(RuntimeError, match="rollback destination differs"):
        await cutover.rollback(
            _session(), {"retained": {"contract": retention.CONTRACT}}, SimpleNamespace(schema="serving")
        )
    family.assert_not_awaited()


@pytest.mark.asyncio
async def test_composition_refuses_different_producer_before_current_state(monkeypatch):
    """A complete input from the wrong producer cannot enter composition or acquire current authority."""
    current = AsyncMock()
    monkeypatch.setattr(storage, "current_models", current)
    monkeypatch.setattr(publication.PublisherSettings, "from_environment", lambda: SimpleNamespace(schema="serving"))
    with pytest.raises(RuntimeError, match="input differs"):
        await publication.compose_catalog_family(_session(), "serving", "ms-drg", _input("code-sets"), (), object())
    current.assert_not_awaited()


@pytest.mark.asyncio
async def test_configured_transaction_uses_declared_roles_without_operator_connection_settings(monkeypatch):
    configured = publication.CatalogPublicationPolicy("serving", "snapshot_owner", ("publisher", "reader"))
    environment = Mock(side_effect=AssertionError("operator connection settings must remain unused"))
    monkeypatch.setattr(publication.PublisherSettings, "from_environment", environment)
    current = AsyncMock()
    monkeypatch.setattr(storage, "current_models", current)
    with pytest.raises(RuntimeError, match="input differs"):
        await publication.compose_catalog_family(
            _session(), "serving", "ms-drg", _input("code-sets"), (), object(), publication_policy=configured
        )
    environment.assert_not_called()
    current.assert_not_awaited()


@pytest.mark.parametrize("readers", [(), [], ("reader", "reader"), ("snapshot_owner",), ('unsafe"reader',)])
def test_transaction_publication_roles_refuse_incomplete_or_unsafe_declarations(readers):
    with pytest.raises(ValueError, match="configuration|schema name"):
        publication.CatalogPublicationPolicy("serving", "snapshot_owner", readers)


@pytest.mark.asyncio
async def test_records_copy_before_complete_native_indexes(monkeypatch):
    incoming = _input("code-sets")
    session, events = _session(), []
    monkeypatch.setattr(publication, "_input_handles", AsyncMock())

    async def copy_batch(observed, model, **options):
        assert observed is session and model is CodeCatalog
        assert options["columns"] == tuple(model.__table__.columns.keys())
        assert options["schema_name"] == incoming.ownership.schema_name
        assert options["table_name"] == "code_catalog"
        assert all(len(record) == len(options["columns"]) for record in options["records"])
        events.append(len(options["records"]))
        return len(options["records"])

    async def complete(*arguments):
        assert arguments == (session, incoming.ownership.schema_name, incoming.spec.model_types)
        events.append("indexes")

    monkeypatch.setattr(native, "native_copy_record_batch", copy_batch)
    monkeypatch.setattr(storage, "complete_stage", complete)
    counts = await publication.copy_catalog_records(session, incoming, {"code_catalog": [{"code": "001"}] * 5001})
    assert counts == {"code_catalog": 5001}
    assert events == [5000, 1, "indexes"]
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("payloads", [{}, {"code_catalog": [{"unexpected": 1}]}])
async def test_record_copy_refuses_unbound_model_fields(monkeypatch, payloads):
    monkeypatch.setattr(publication, "_input_handles", AsyncMock())
    copy = AsyncMock()
    monkeypatch.setattr(native, "native_copy_record_batch", copy)
    with pytest.raises(RuntimeError, match="differ"):
        await publication.copy_catalog_records(_session(), _input("code-sets"), payloads)
    copy.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("answers", [[False, False], [True], [False, True], [None]])
async def test_native_semantics_use_fixed_scoped_antijoins(answers):
    session = _session()
    session.scalar = AsyncMock(side_effect=answers)
    if answers == [False, False]:
        await publication.validate_catalog_semantics(session, publication.input_spec("ms-drg"), "candidate")
        query, parameters = session.scalar.await_args.args
        assert "NOT EXISTS" in str(query)
        assert "synonym.code_system" in str(query) and "relation.from_code" in str(query)
        assert "relation.source=:pcs AND NOT EXISTS" in str(query)
        assert "relation.source=:cm AND NOT EXISTS" not in str(query)
        assert set(parameters) == {"drg", "cm", "pcs"}
    else:
        with pytest.raises(native.ReferenceFamilyArchiveError, match="semantics differ"):
            await publication.validate_catalog_semantics(session, publication.input_spec("ms-drg"), "candidate")


def test_code_upsert_retains_unmentioned_columns_and_keys():
    rule = publication.contributions("code-sets", upsert=True)[0]
    assert rule.source_values == tuple(source for source, _ in codes.SOURCES)
    assert rule.update_columns == (
        "display_name",
        "short_description",
        "long_description",
        "is_active",
        "source",
        "updated_at",
    )
    assert not {"code_type", "source_release", "source_attribution"}.intersection(rule.update_columns)


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", [None, "columns", "constraints", "index"])
async def test_native_schema_keeps_incumbent_constraints_and_indexes(monkeypatch, changed):
    from process import entity_address_snapshot_receipt as catalog

    for name in ("columns", "constraints"):
        monkeypatch.setattr(
            catalog, "_catalog_" + name, AsyncMock(side_effect=[["same"], ["changed" if changed == name else "same"]])
        )
    monkeypatch.setattr(
        publication.binding,
        "named_catalog_schema",
        lambda columns, constraints, indexes: dict(columns=columns, constraints=constraints, indexes=indexes),
    )
    monkeypatch.setattr(
        catalog,
        "_catalog_indexes",
        AsyncMock(side_effect=[["primary"], ["model"] if changed == "index" else ["primary", "model"]]),
    )
    if changed:
        with pytest.raises(native.ReferenceFamilyArchiveError, match="columns|constraints|index"):
            await publication.require_preserved_catalog_schema(_session(), ("old", 1), ("new", 2))
    else:
        await publication.require_preserved_catalog_schema(_session(), ("old", 1), ("new", 2))


@pytest.mark.asyncio
async def test_native_fixture_refuses_preexisting_names_before_registering_cleanup():
    from tests import scoped_catalog_native_fixture as fixture

    connection = SimpleNamespace(scalar=AsyncMock(return_value=9), execute=AsyncMock())

    @asynccontextmanager
    async def begin():
        yield connection

    attempted_roles = []
    engine = SimpleNamespace(begin=begin)
    with pytest.raises(AssertionError):
        await fixture._create_catalog_actors(
            engine, {"owner": "synthetic_owner"}, attempted_roles, uuid4().hex, ["synthetic_live"]
        )
    assert attempted_roles == []
    await fixture._remove_catalog_actors(engine, attempted_roles, ["synthetic_live"])
    connection.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("importer", ["code-sets", "ms-drg"])
async def test_native_fixture_preserves_declared_importer_and_complete_table_set(monkeypatch, importer):
    from process import ms_drg_result_generation as drg
    from tests import scoped_catalog_native_fixture as fixture

    generation = codes.TABLE if importer == "code-sets" else drg.TABLE
    connection = SimpleNamespace(execute=AsyncMock(), scalar=AsyncMock(side_effect=[True, False, True, False]))

    @asynccontextmanager
    async def begin():
        yield connection

    actors = SimpleNamespace(
        roles=dict(owner="synthetic_owner", builder="synthetic_builder", publisher="synthetic_publisher"),
        engine=SimpleNamespace(begin=begin),
        publisher=SimpleNamespace(begin=begin),
        builder=SimpleNamespace(begin=begin),
    )
    monkeypatch.setattr(native, "_schema_oid", AsyncMock(return_value=9))
    monkeypatch.setattr(native, "_relation_oid", AsyncMock(side_effect=[10, 11]))
    seal = AsyncMock()
    monkeypatch.setattr(native, "seal_model_family_storage", seal)
    await fixture.seal_catalog(actors, "synthetic_live", (CodeCatalog,), generation)
    statements = [str(call.args[0]) for call in connection.execute.await_args_list]
    for name in ("code_catalog", generation):
        assert f'ALTER TABLE "synthetic_live"."{name}" OWNER TO "synthetic_owner"' in statements
        assert f'GRANT SELECT ON "synthetic_live"."{name}" TO "synthetic_builder"' in statements
    assert all(
        "GRANT" not in statement or "GRANT SELECT" in statement or "GRANT USAGE" in statement
        for statement in statements
    )
    assert all("GRANT INSERT" not in statement and "GRANT UPDATE" not in statement for statement in statements)
    seal.assert_not_awaited()
    native._schema_oid.assert_not_awaited()
    native._relation_oid.assert_not_awaited()


@pytest.mark.asyncio
async def test_native_fixture_refuses_unknown_importer_before_custody_mutation():
    from tests import scoped_catalog_native_fixture as fixture

    with pytest.raises(ValueError, match="unsupported scoped catalog generation table"):
        await fixture.seal_catalog(object(), "synthetic_live", (CodeCatalog,), "unknown_generation")


@pytest.mark.asyncio
@pytest.mark.parametrize("change", [None, "companion", "data", "semantic"])
async def test_cutover_checks_complete_generation_and_content(monkeypatch, change):
    generations_by_importer = {name: _generation() for name in ("code-sets", "ms-drg")}
    handoff_by_field = {
        "schema_name": "serving",
        "importer_id": "code-sets",
        "expected_generations": generations_by_importer,
        "expected_tables": ["unchanged"],
    }
    observed_by_importer = {name: dict(generation) for name, generation in generations_by_importer.items()}
    if change == "companion":
        observed_by_importer["ms-drg"]["local_generation"] += 1
    lock = AsyncMock()
    monkeypatch.setattr(storage, "require_tables", lock)
    monkeypatch.setattr(generations, "read_generations", AsyncMock(return_value=observed_by_importer))
    monkeypatch.setattr(
        generations,
        "validate_published_content",
        AsyncMock(side_effect=RuntimeError("semantic drift") if change == "semantic" else None),
    )
    monkeypatch.setattr(
        storage, "table_receipts", AsyncMock(return_value=["changed" if change == "data" else "unchanged"])
    )
    if change:
        with pytest.raises(RuntimeError, match="changed|drift"):
            await cutover._require_current(_session(), handoff_by_field, storage.MODELS, lock=True)
    else:
        assert await cutover._require_current(_session(), handoff_by_field, storage.MODELS, lock=True) == (
            generations_by_importer,
            ["unchanged"],
        )
    assert lock.await_args.kwargs == {"lock": "ACCESS EXCLUSIVE"}


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, "current", "rotate", "copy", "publish", "record"])
async def test_atomic_exchange_preserves_generation_order_and_propagates_failure(monkeypatch, failure):
    events = []
    identifier = uuid4()
    generations_by_importer = {"code-sets": _generation()}
    handoff_by_field = {
        "schema_name": "serving",
        "importer_id": "code-sets",
        "expected_generations": generations_by_importer,
        "expected_tables": [],
    }
    spec = publication.input_spec("code-sets")
    session = _session()

    def operation(name, result=None):
        async def perform(*_args, **_kwargs):
            events.append(name)
            if failure == name:
                raise RuntimeError("refused " + name)
            return result

        return AsyncMock(side_effect=perform)

    monkeypatch.setattr(cutover, "_require_current", operation("current", (generations_by_importer, [])))
    monkeypatch.setattr(storage, "require_tables", AsyncMock(return_value=(("code_catalog", 10),)))
    monkeypatch.setattr(native, "_schema_oid", AsyncMock(return_value=9))
    monkeypatch.setattr(native, "_rotate_family_relations", operation("rotate", "retained"))
    monkeypatch.setattr(retention, "copy_generations", operation("copy"))
    monkeypatch.setattr(generations, "read_generations", AsyncMock(return_value=generations_by_importer))
    monkeypatch.setattr(retention, "record_retained", operation("record"))
    monkeypatch.setattr(storage, "protect_tables", operation("grants"))
    advance = operation("publish", "current")
    arguments = (session, handoff_by_field, (identifier, "candidate", spec), SimpleNamespace(schema="serving"), advance)
    if failure:
        with pytest.raises(RuntimeError, match="refused"):
            await cutover.activate_prepared(*arguments)
        assert events[-1] == failure
        session.execute.assert_not_awaited()
    else:
        current, receipt = await cutover.activate_prepared(*arguments)
        assert current == "current" and receipt["previous_generations"] == generations_by_importer
        assert events == ["current", "rotate", "copy", "publish", "record", "grants"]
        assert str(session.execute.await_args.args[0]) == 'DROP SCHEMA "candidate" RESTRICT'
    assert not hasattr(session, "commit")
