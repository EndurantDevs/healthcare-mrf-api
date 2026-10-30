"""Protocol/unit checks only: actual Source and office authority need separate native verification."""

import asyncio
from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from process import registry_source_contribution_store as store
from process import registry_source_recipe_composition as composition
from process import registry_verified_witness_stage as stage
from process.network_address_projection import PinnedAddressSource
from process.network_approved_membership_source import ApprovedMembershipSource
from process.network_fhir_membership_source import FHIRMembershipBatchBoundsError, FHIRSourceWitnessPageV2
from process.registry_source_contribution_artifacts import ResolvedSourceFactArtifact, SourceFactArtifactReference
from process.registry_verified_witness_offices import WitnessOfficeSource
from tests.test_registry_ptg_office_membership_recipe import _recipe as ptg_recipe
from tests.test_registry_source_inventory_preparation import _descriptor
from tests.test_registry_source_recipe_store_postgres import _example
from tests.test_verified_inventory_v2 import context, derivation, rows
from tests.test_verified_office_source_witnesses_v2 import _edge, _header

APPROVED = ApprovedMembershipSource(7, "a" * 64)
ADDRESS = PinnedAddressSource("synthetic_addresses", "entity_address_unified", "b" * 64)


@asynccontextmanager
async def transaction():
    yield


def test_old_stage_defaults_and_generation_remain_unchanged():
    value = composition.RegistrySourceRecipeStage("page", 1, 2, "f" * 64)
    assert value.office_derivations == () and value.office_policy_context is None
    assert value.unresolved_rows == value.unresolved_raw_source_rows == 0
    assert composition._source_stage_generation("b" * 64, APPROVED, ("c" * 64,), 1, 2) == store._digest(
        ["b" * 64, APPROVED.generation_id, ["c" * 64], 1, 2]
    )


@pytest.mark.parametrize("kind", ["empty", "mixed", "legacy", "approved_type", "address"])
async def test_unsupported_scope_preserves_unbound_guard_before_native_io(kind):
    recipe, _, _ = rows()
    recipes = (recipe,)
    approved = APPROVED
    offices = (ADDRESS, ("reader",))
    if kind == "empty":
        recipes = ()
    elif kind == "mixed":
        recipes = (recipe, ptg_recipe())
    elif kind == "legacy":
        recipes = (_example(),)
    elif kind == "approved_type":
        approved = SimpleNamespace(approved_revision=7, generation_id="a" * 64, total_rows=1)
    else:
        offices = (object(), ("reader",))
    with pytest.raises(composition.RegistrySourceCompositionError, match="verified_inventory_unbound"):
        await composition.stage_registry_source_recipes(
            object(),
            recipes,
            approved,
            request_id=UUID(int=7),
            control_schema="synthetic",
            inventory=True,
            read_context=composition.SourceRecipeReadContext(verified_office_inputs=offices),
        )


def test_nonempty_approved_snapshot_still_uses_witness_scope():
    recipe, _, _ = rows()
    approved = replace(APPROVED, total_rows=1)
    stage._require_witness_scope((recipe,), approved, (ADDRESS, ("reader",)))
    from process.registry_membership_refresh_runtime import _uses_witness_office_policy

    assert _uses_witness_office_policy(approved, (recipe,)) is True
    assert _uses_witness_office_policy(approved, (recipe, ptg_recipe())) is False


async def test_capture_uses_actual_decoder_and_drained_copy_and_requires_terminal_page(monkeypatch):
    import json

    recipe, _, _ = rows()
    source = recipe.source_pin
    documents = [_header(source), _edge(source)]
    pages = [
        FHIRSourceWitnessPageV2(source, 1, ("PractitionerRole", "role-one"), json.dumps(documents).encode()),
        FHIRSourceWitnessPageV2(source, 0, None, b"[]"),
    ]
    reader = AsyncMock(side_effect=pages)
    monkeypatch.setattr(stage, "read_fhir_source_witness_page_v2", reader)
    connection = SimpleNamespace(is_in_transaction=lambda: True, copy_records_to_table=AsyncMock(return_value="COPY 2"))
    assert await stage._capture_witness_facts(connection, recipe, APPROVED, "synthetic", "facts", None) == (2, 1)
    assert reader.await_count == 2
    assert reader.await_args.kwargs["after_resource"] == ("PractitionerRole", "role-one")
    copied = connection.copy_records_to_table.await_args.kwargs["records"]
    assert [row[1] for row in copied] == [1, 2]
    assert all(row[0] == store.verified_office_fact_identity(recipe)[0] for row in copied)


async def test_capture_halves_bounded_page_and_checks_source_and_cursor(monkeypatch):
    recipe, _, _ = rows()
    reader = AsyncMock(
        side_effect=[
            FHIRMembershipBatchBoundsError("bound"),
            FHIRSourceWitnessPageV2(recipe.source_pin, 0, None, b"[]"),
        ]
    )
    monkeypatch.setattr(stage, "read_fhir_source_witness_page_v2", reader)
    connection = SimpleNamespace(is_in_transaction=lambda: True)
    assert await stage._capture_witness_facts(connection, recipe, APPROVED, "synthetic", "facts", None) == (0, 0)
    assert [call.kwargs["limit"] for call in reader.await_args_list] == [1000, 500]


async def test_native_census_uses_physical_scope_and_rejects_drift():
    recipe, _, _ = rows()
    expected_by_kind = dict(recipe.source_pin.retained_capture.origin.resource_counts)
    connection = SimpleNamespace(
        fetch=AsyncMock(
            return_value=[{"resource_type": kind, "rows": count} for kind, count in expected_by_kind.items()]
        )
    )
    assert await stage._native_witness_headers(connection, recipe) == 0
    assert connection.fetch.await_args.args[1] == recipe.source_pin.dataset_id
    connection.fetch.return_value.append({"resource_type": "PractitionerRole", "rows": 1})
    with pytest.raises(composition.RegistrySourceCompositionError, match="accounting_invalid"):
        await stage._native_witness_headers(connection, recipe)


@pytest.mark.parametrize("reuse", [False, True])
async def test_per_recipe_maps_actual_witness_contract_and_keeps_original_anchor(monkeypatch, reuse):
    recipe, descriptor, _ = rows()
    authorizer = AsyncMock(return_value=None)
    monkeypatch.setattr(stage, "_authorize_witness_recipe", authorizer)
    monkeypatch.setattr(stage, "_native_witness_headers", AsyncMock(return_value=1))
    monkeypatch.setattr(stage, "_create_source_stage_tables", AsyncMock())
    capture = AsyncMock(return_value=(3, 1))
    monkeypatch.setattr(stage, "_capture_witness_facts", capture)
    mapper = AsyncMock(return_value=descriptor)
    monkeypatch.setattr(stage, "map_fhir_source_witnesses_v2", mapper)
    artifact = (
        ResolvedSourceFactArtifact(
            SourceFactArtifactReference(str(UUID(int=9)), 71, "f" * 64), "synthetic_original", descriptor
        )
        if reuse
        else None
    )
    result = await stage._stage_witness_source(
        object(), recipe, APPROVED, "synthetic", ("facts", "members"), {}, artifact
    )
    assert result.table_name == "members"
    assert mapper.await_args.args[3] == (artifact.relation if reuse else 'pg_temp."facts"')
    assert authorizer.await_count == 2 and capture.await_count == (0 if reuse else 1)
    assert result.descriptor["fact_candidate_id"] == (str(UUID(int=9)) if reuse else None)


async def test_source_authorization_is_required_before_census_or_fact_reuse(monkeypatch):
    recipe, _, _ = rows()
    monkeypatch.setattr(stage, "_authorize_witness_recipe", AsyncMock(side_effect=ValueError("admission differs")))
    census = AsyncMock()
    monkeypatch.setattr(stage, "_native_witness_headers", census)
    with pytest.raises(ValueError, match="admission differs"):
        await stage._stage_witness_source(object(), recipe, APPROVED, "synthetic", ("facts", "members"), {}, None)
    census.assert_not_awaited()


async def test_finish_returns_complete_policy_commitments_and_selection_accounting(monkeypatch):
    recipe, descriptor, document = rows()
    document.update(accepted_membership_rows=0, omitted_membership_rows=1, office_rows=0)
    witness = WitnessOfficeSource(recipe, descriptor, "mapped_members")
    matches = object()  # protocol sentinel only; this test does not assert native office authority
    preparer = AsyncMock(return_value=(matches, (document,), context()))
    monkeypatch.setattr(stage, "prepare_witness_office_mapping_v2", preparer)
    finish = AsyncMock()
    monkeypatch.setattr(stage, "_finish_source_stage", finish)
    connection = SimpleNamespace(execute=AsyncMock(), fetchval=AsyncMock(return_value=descriptor["fact_rows"]))
    recipe_sha = composition._source_stage_recipes((recipe,), True)[1]
    finished_stage = await stage._finish_witness_inventory(
        connection,
        (recipe,),
        APPROVED,
        ("raw", "page", "facts"),
        [witness],
        (ADDRESS, ("reader",)),
        ("synthetic", recipe_sha),
    )
    assert preparer.await_count == 1 and preparer.await_args.args[3] == [witness]
    assert finished_stage.office_derivations == (document,) and finished_stage.office_policy_context == context()
    assert finished_stage.office_matches is matches and finished_stage.inventory
    assert finished_stage.membership_rows == 0 and finished_stage.omitted_rows == 2
    assert finished_stage.unresolved_rows == 1 and finished_stage.unresolved_raw_source_rows == 1
    assert (
        finished_stage.own_fact_rows == 3
        and finished_stage.reused_fact_rows == finished_stage.copied_reused_fact_rows == 0
    )
    assert finished_stage.source_generations == (store._digest([descriptor, document]),)
    assert finished_stage.raw_source_generations == (descriptor["source_generation"],)
    assert finished_stage.generation_sha256 == composition._source_stage_generation(
        recipe_sha, APPROVED, finished_stage.source_generations, 0, 2
    )
    assert finish.await_args.args[-1] == 0


async def test_entire_stage_savepoint_rolls_back_late_failure_and_passes_exact_baseline(monkeypatch):
    recipe, _, _ = rows()
    transitions = []

    @asynccontextmanager
    async def savepoint():
        transitions.append("enter")
        try:
            yield
        except RuntimeError:
            transitions.append("rollback")
            raise

    connection = SimpleNamespace(transaction=savepoint)
    monkeypatch.setattr(stage, "_require_source_stage_scope", AsyncMock())
    retained = AsyncMock(return_value={})
    monkeypatch.setattr(stage, "retained_source_contributions", retained)
    create = AsyncMock()
    monkeypatch.setattr(stage, "_create_source_stage_tables", create)
    monkeypatch.setattr(stage, "_stage_witness_source", AsyncMock(return_value=object()))
    monkeypatch.setattr(stage, "_finish_witness_inventory", AsyncMock(side_effect=RuntimeError("late mapping failure")))
    baseline = object()
    with pytest.raises(RuntimeError, match="late mapping failure"):
        await stage.stage_verified_witness_inventory(
            connection,
            (recipe,),
            APPROVED,
            request_id=UUID(int=7),
            control_schema="synthetic",
            retained_fhir_reads={},
            verified_office_inputs=(ADDRESS, ("reader",)),
            source_manifest=baseline,
        )
    assert transitions == ["enter", "rollback"] and create.await_count == 1
    assert retained.await_args.args[1] is baseline and retained.await_args.kwargs["witnesses"] is True


@pytest.mark.parametrize("witnesses", [False, True])
async def test_retained_selection_uses_exact_supplied_generation_and_explicit_identity(monkeypatch, witnesses):
    from process import registry_source_contribution_artifacts as artifacts

    recipe, witness, _ = rows(anchored=True)
    legacy_by_field = {**_descriptor(recipe), **{field: witness[field] for field in store.FACT_ANCHOR_FIELDS}}
    selected = witness if witnesses else legacy_by_field
    prior = SimpleNamespace(
        generation_id="e" * 64, candidate_id=str(UUID(int=7)), source_generations={store.SUMMARY_KEY: "f" * 64}
    )
    resolve = AsyncMock(return_value=prior)
    monkeypatch.setattr(store, "resolve_network_serving_manifest", resolve)
    monkeypatch.setattr(
        artifacts, "retained_source_fact_descriptors", AsyncMock(return_value=(legacy_by_field, witness))
    )
    artifact = ResolvedSourceFactArtifact(
        SourceFactArtifactReference(str(UUID(int=9)), 71, "f" * 64), "synthetic_original", selected
    )
    authenticate = AsyncMock(return_value={selected["recipe_sha256"]: artifact})
    monkeypatch.setattr(artifacts, "verify_anchored_source_fact_contents", authenticate)
    result = await store.retained_source_contributions(
        object(), prior, "synthetic", selected_recipes=(recipe,), witnesses=witnesses
    )
    assert result == {selected["capture_sha256"]: artifact}
    assert authenticate.await_args.args[1] == (selected,)
    assert resolve.await_args.kwargs["generation_id"] == prior.generation_id


@pytest.mark.parametrize("damage", ["source", "cursor", "counts"])
async def test_capture_rejects_changed_source_nonadvancing_cursor_and_incomplete_census(monkeypatch, damage):
    import json

    recipe, _, _ = rows()
    source = recipe.source_pin
    page = FHIRSourceWitnessPageV2(
        source, 1, ("PractitionerRole", "role-one"), json.dumps([_header(source), _edge(source)]).encode()
    )
    if damage == "source":
        page = replace(page, source=replace(source, as_of="2026-10-02"))
    reader = AsyncMock(side_effect=[page, page])
    monkeypatch.setattr(stage, "read_fhir_source_witness_page_v2", reader)
    connection = SimpleNamespace(is_in_transaction=lambda: True, copy_records_to_table=AsyncMock(return_value="COPY 2"))
    if damage != "counts":
        with pytest.raises(composition.RegistrySourceCompositionError):
            await stage._capture_witness_facts(connection, recipe, APPROVED, "synthetic", "facts", None)
        return
    monkeypatch.setattr(stage, "_authorize_witness_recipe", AsyncMock(return_value=None))
    monkeypatch.setattr(stage, "_native_witness_headers", AsyncMock(return_value=2))
    monkeypatch.setattr(stage, "_create_source_stage_tables", AsyncMock())
    monkeypatch.setattr(stage, "_capture_witness_facts", AsyncMock(return_value=(2, 1)))
    mapper = AsyncMock()
    monkeypatch.setattr(stage, "map_fhir_source_witnesses_v2", mapper)
    with pytest.raises(composition.RegistrySourceCompositionError, match="accounting_invalid"):
        await stage._stage_witness_source(connection, recipe, APPROVED, "synthetic", ("facts", "members"), {}, None)
    mapper.assert_not_awaited()


async def test_authorizer_rechecks_both_real_source_custody_and_reviewed_bindings(monkeypatch):
    recipe, _, _ = rows()
    custody = AsyncMock(return_value=None)
    reviewed = AsyncMock()
    monkeypatch.setattr(stage, "_require_recipe_custody", custody)
    monkeypatch.setattr(stage, "_require_source_recipe", reviewed)
    reads_by_capture = {}
    assert await stage._authorize_witness_recipe(object(), recipe, APPROVED, "synthetic", reads_by_capture) is None
    assert custody.await_args.kwargs["retained_fhir_reads"] is reads_by_capture
    assert reviewed.await_args.args[1] is recipe.source_pin
    assert reviewed.await_args.args[3:5] == (APPROVED, recipe.binding_coordinates)


async def test_one_global_policy_pass_covers_fresh_and_original_anchored_sources(monkeypatch):
    first = rows()
    second = rows(anchored=True)
    sources = [
        WitnessOfficeSource(first[0], first[1], "mapped_first"),
        WitnessOfficeSource(second[0], second[1], "mapped_second"),
    ]
    recipes = tuple(source.recipe for source in sources)
    docs = (first[2], second[2])
    preparer = AsyncMock(return_value=(object(), docs, context()))
    monkeypatch.setattr(stage, "prepare_witness_office_mapping_v2", preparer)
    monkeypatch.setattr(stage, "_finish_source_stage", AsyncMock())
    connection = SimpleNamespace(execute=AsyncMock(), fetchval=AsyncMock(return_value=first[1]["fact_rows"]))
    recipe_sha = composition._source_stage_recipes(recipes, True)[1]
    finished = await stage._finish_witness_inventory(
        connection,
        recipes,
        APPROVED,
        ("raw", "page", "facts"),
        sources,
        (ADDRESS, ("reader",)),
        ("synthetic", recipe_sha),
    )
    assert preparer.await_count == 1 and len(preparer.await_args.args[3]) == 2
    assert finished.own_fact_rows == finished.reused_fact_rows == 3 and finished.copied_reused_fact_rows == 0
    assert finished.contribution_receipts[1]["fact_candidate_id"] == str(UUID(int=9))
    assert finished.membership_rows == 2 and finished.omitted_rows == finished.unresolved_rows == 2
    assert finished.unresolved_rows <= finished.omitted_rows


async def test_reuse_selection_never_promotes_legacy_descriptors(monkeypatch):
    from process import registry_source_contribution_artifacts as artifacts

    recipe, _, _ = rows()
    legacy = _descriptor(recipe)
    prior = SimpleNamespace(
        generation_id="e" * 64, candidate_id=str(UUID(int=7)), source_generations={store.SUMMARY_KEY: "f" * 64}
    )
    monkeypatch.setattr(store, "resolve_network_serving_manifest", AsyncMock(return_value=prior))
    monkeypatch.setattr(artifacts, "retained_source_fact_descriptors", AsyncMock(return_value=(legacy,)))
    verify = AsyncMock(return_value={})
    monkeypatch.setattr(artifacts, "verify_anchored_source_fact_contents", verify)
    assert (
        await store.retained_source_contributions(
            object(), prior, "synthetic", selected_recipes=(recipe,), witnesses=True
        )
        == {}
    )
    assert verify.await_args.args[1] == ()


async def test_all_reused_keeps_local_fact_heap_empty_and_preserves_original_anchor(monkeypatch):
    recipe, descriptor, doc = rows(anchored=True)
    monkeypatch.setattr(
        stage, "prepare_witness_office_mapping_v2", AsyncMock(return_value=(object(), (doc,), context()))
    )
    monkeypatch.setattr(stage, "_finish_source_stage", AsyncMock())
    connection = SimpleNamespace(execute=AsyncMock(), fetchval=AsyncMock(return_value=0))
    recipe_sha = composition._source_stage_recipes((recipe,), True)[1]
    finished = await stage._finish_witness_inventory(
        connection,
        (recipe,),
        APPROVED,
        ("raw", "page", "facts"),
        [WitnessOfficeSource(recipe, descriptor, "members")],
        (ADDRESS, ("reader",)),
        ("synthetic", recipe_sha),
    )
    assert finished.own_fact_rows == 0 and finished.reused_fact_rows == 3
    assert finished.copied_reused_fact_rows == 0 and finished.contribution_receipts == (descriptor,)
    assert connection.fetchval.await_args.args[0] == 'SELECT count(*) FROM pg_temp."facts"'
