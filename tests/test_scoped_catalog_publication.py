# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Host call-flow checks for complete scoped composition and its caller-owned transaction."""

from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from db.models import CodeCatalog
from process import code_sets_result_archive as codes
from process import reference_family_archive as native
from process import scoped_catalog_publication as publication
from process import scoped_catalog_retention as retention


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


def _prepared():
    incoming = _input()
    incumbent = native.ReferenceFamilyIncumbent(
        incoming.spec.importer_id, "serving", tuple((name, oid + 20) for name, oid in incoming.ownership.relation_oids)
    )
    return publication.ComposedCatalog(
        "ms-drg",
        incoming.spec,
        incoming.ownership,
        incumbent,
        {"code-sets": object(), "ms-drg": object()},
        (),
        50,
        "77",
    )


def test_scoped_input_refuses_unknown_producer():
    """Only the two compiled catalog producers can obtain a native input model inventory."""
    with pytest.raises(native.ReferenceFamilyArchiveError, match="producer is unsupported"):
        publication.input_spec("other")


@pytest.mark.asyncio
@pytest.mark.parametrize("oids", [[10, None], [None, 11]])
async def test_current_family_refuses_partial_synonym_relationship_inventory(monkeypatch, oids):
    """A partially installed MS-DRG family cannot be hidden by a code-only producer."""
    monkeypatch.setattr(native, "_relation_oid", AsyncMock(side_effect=oids))
    lock = AsyncMock()
    monkeypatch.setattr(publication.binding, "lock_catalog_binding", lock)
    with pytest.raises(native.ReferenceFamilyArchiveError, match="family is incomplete"):
        await publication._current_family(_session(), "serving", "code-sets")
    lock.assert_not_awaited()


@pytest.mark.asyncio
async def test_composition_refuses_different_producer_before_current_state(monkeypatch):
    """A complete input from the wrong producer cannot enter composition or acquire current authority."""
    current = AsyncMock()
    monkeypatch.setattr(publication, "_current_family", current)
    with pytest.raises(native.ReferenceFamilyArchiveError, match="input producer differs"):
        await publication.compose_catalog_family(_session(), "serving", "ms-drg", _input("code-sets"), (), object())
    current.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("transaction", [None, 77, "not-a-transaction", "0"])
async def test_publication_requires_native_transaction_identity(transaction):
    """Only a positive native transaction identifier can fence a composed candidate."""
    session = _session()
    session.scalar.return_value = transaction
    with pytest.raises(native.ReferenceFamilyArchiveError, match="transaction is unavailable"):
        await publication._transaction_id(session)
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_current_generations_refuse_code_source_drift_before_other_family(monkeypatch):
    """An installed code generation's exact rows must still agree before its authority is captured."""
    generation = SimpleNamespace(origin_generation=1, row_count=3, row_sha256="a" * 64, code_catalog_oid=10)
    monkeypatch.setattr(publication.binding, "_lock_generation", AsyncMock(return_value=9))
    monkeypatch.setattr(codes, "read_generation", AsyncMock(return_value=generation))
    monkeypatch.setattr(codes, "scope_receipt", AsyncMock(return_value=(4, "a" * 64, 10)))
    session = _session()
    with pytest.raises(codes.CodeSetsArchiveError, match="predecessor drifted"):
        await publication._current_generations(session, "serving", "code-sets", read_only=True)
    assert publication.binding._lock_generation.await_count == 1
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_records_copy_before_complete_native_indexes(monkeypatch):
    incoming = _input("code-sets")
    session, events = _session(), []
    monkeypatch.setattr(native, "verify_model_family_stage_ownership", AsyncMock())

    async def copy_batch(observed, model, **options):
        assert observed is session and model is CodeCatalog
        assert options["columns"] == tuple(model.__table__.columns.keys())
        assert options["schema_name"] == incoming.ownership.schema_name
        assert options["table_name"] == "code_catalog"
        assert all(len(record) == len(options["columns"]) for record in options["records"])
        events.append(len(options["records"]))
        return len(options["records"])

    async def complete(*arguments):
        assert arguments == (session, incoming.spec, incoming.ownership)
        events.append("indexes")

    monkeypatch.setattr(native, "native_copy_record_batch", copy_batch)
    monkeypatch.setattr(native, "complete_model_family_stage", complete)
    counts = await publication.copy_catalog_records(session, incoming, {"code_catalog": [{"code": "001"}] * 5001})
    assert counts == {"code_catalog": 5001}
    assert events == [5000, 1, "indexes"]
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("payloads", [{}, {"code_catalog": [{"unexpected": 1}]}])
async def test_record_copy_refuses_unbound_model_fields(monkeypatch, payloads):
    monkeypatch.setattr(native, "verify_model_family_stage_ownership", AsyncMock())
    copy = AsyncMock()
    monkeypatch.setattr(native, "native_copy_record_batch", copy)
    with pytest.raises(native.ReferenceFamilyArchiveError, match="differ"):
        await publication.copy_catalog_records(_session(), _input("code-sets"), payloads)
    copy.assert_not_awaited()


def _composition_steps(monkeypatch, prepared, events, failure=None):
    outcomes_by_step = {
        "_current_family": (prepared.spec, prepared.incumbent, prepared.owner_oid),
        "_current_generations": prepared.generations,
        "_capture_access": prepared.access,
        "precreate_model_family_stage": prepared.ownership,
    }
    steps = (
        (publication, "_current_family"),
        (publication, "_current_generations"),
        (publication, "_capture_access"),
        (native, "precreate_model_family_stage"),
        (publication.binding, "match_catalog_text_columns"),
        (publication, "compose_model_family_stage"),
        (publication, "validate_catalog_semantics"),
        (publication, "_require_unchanged_schema"),
        (publication, "_seal_candidate"),
    )
    for module, name in steps:

        async def perform(*arguments, step=name, **keywords):
            events.append(step)
            if step == failure:
                raise RuntimeError("refused " + step)
            return outcomes_by_step.get(step)

        monkeypatch.setattr(module, name, perform)
    return [name for _, name in steps]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure",
    [None, "_current_family", "compose_model_family_stage", "validate_catalog_semantics", "_require_unchanged_schema"],
)
async def test_composition_verifies_every_boundary_before_sealing(monkeypatch, failure):
    prepared, events = _prepared(), []
    steps = _composition_steps(monkeypatch, prepared, events, failure)
    arguments = (_session(), "serving", "ms-drg", _input(), publication.contributions("ms-drg"), object())
    if failure:
        with pytest.raises(RuntimeError, match="refused"):
            await publication.compose_catalog_family(*arguments)
        assert events == steps[: steps.index(failure) + 1]
    else:
        assert await publication.compose_catalog_family(*arguments) == prepared
        assert events == steps


def _activation_steps(monkeypatch, prepared, events, failure=None):
    retained_by_field = {"retained": True}
    operations = (
        (native, "_lock_family", None),
        (native, "verify_model_family_stage_ownership", None),
        (native, "_incumbent_pairs", prepared.incumbent.relation_oids),
        (publication, "_current_generations", prepared.generations),
        (native, "_rotate_family_relations", "predecessor"),
        (retention, "retain_catalog_authority", retained_by_field),
        (publication.binding, "rebind_code_sets_generation", None),
        (publication, "_restore_access", None),
        (publication, "_seal_generations", None),
        (retention, "finish_catalog_authority", None),
    )
    for module, name, outcome in operations:

        async def perform(*arguments, step=name, result=outcome, **keywords):
            events.append(step)
            if step == failure:
                raise RuntimeError("refused " + step)
            return result

        monkeypatch.setattr(module, name, AsyncMock(side_effect=perform))

    async def publish():
        events.append("publish")
        return "current"

    return publish, retained_by_field


@pytest.mark.asyncio
async def test_atomic_swap_retains_authority_before_generation(monkeypatch):
    prepared, session, events = _prepared(), _session(), []
    publish, retained = _activation_steps(monkeypatch, prepared, events)
    current = await publication.activate_catalog_family(session, prepared, publish)
    assert current == ("current", retained)
    assert events == [
        "_lock_family",
        "verify_model_family_stage_ownership",
        "_incumbent_pairs",
        "_current_generations",
        "_rotate_family_relations",
        "retain_catalog_authority",
        "publish",
        "rebind_code_sets_generation",
        "_restore_access",
        "_seal_generations",
        "finish_catalog_authority",
    ]
    publication.binding.rebind_code_sets_generation.assert_awaited_once_with(
        session, "serving", prepared.generations["code-sets"]
    )
    native._lock_family.assert_awaited_once_with(
        session, "serving", tuple(sorted(prepared.spec.table_names)), "ACCESS EXCLUSIVE", nowait=True
    )
    assert str(session.execute.await_args.args[0]) == f'DROP SCHEMA "{prepared.ownership.schema_name}" RESTRICT'
    assert not hasattr(session, "commit")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure",
    [
        "verify_model_family_stage_ownership",
        "_rotate_family_relations",
        "retain_catalog_authority",
        "_restore_access",
        "finish_catalog_authority",
    ],
)
async def test_swap_errors_propagate_to_callers_transaction(monkeypatch, failure):
    prepared, session, events = _prepared(), _session(), []
    publish, _retained = _activation_steps(monkeypatch, prepared, events, failure)
    with pytest.raises(RuntimeError, match="refused"):
        await publication.activate_catalog_family(session, prepared, publish)
    assert events[-1] == failure
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", ["transaction", "oids", "generation"])
async def test_swap_rechecks_complete_current_vector(monkeypatch, changed):
    prepared, events = _prepared(), []
    publish, _retained = _activation_steps(monkeypatch, prepared, events)
    if changed == "transaction":
        prepared = replace(prepared, transaction_id="78")
    elif changed == "oids":
        monkeypatch.setattr(native, "_incumbent_pairs", AsyncMock(return_value=(("code_catalog", 99),)))
    else:
        monkeypatch.setattr(publication, "_current_generations", AsyncMock(return_value={}))
    with pytest.raises(native.ReferenceFamilyArchiveError, match="changed"):
        await publication.activate_catalog_family(_session(), prepared, publish)
    assert "_rotate_family_relations" not in events and "publish" not in events


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
async def test_ordinary_predecessor_capture_needs_no_live_row_update_privilege(monkeypatch):
    from process import ms_drg_result_archive as drg

    generation = SimpleNamespace(origin_generation=None)
    monkeypatch.setattr(publication.binding, "_lock_generation", AsyncMock(return_value=9))
    codes_read, drg_read = AsyncMock(return_value=generation), AsyncMock(return_value=({"local_generation": 0}, {}))
    monkeypatch.setattr(codes, "read_generation", codes_read)
    monkeypatch.setattr(drg, "_current", drg_read)
    session = _session()
    result = await publication._current_generations(session, "serving", "ms-drg", read_only=True)
    assert set(result) == {"code-sets", "ms-drg"}
    codes_read.assert_awaited_once_with(session, "serving", lock=False)
    drg_read.assert_awaited_once_with(session, "serving", lock=False)
    assert all(call.kwargs["read_only"] is True for call in publication.binding._lock_generation.await_args_list)


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
