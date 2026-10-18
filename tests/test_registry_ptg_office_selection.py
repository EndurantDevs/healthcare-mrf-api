"""Closed D04 selector proof, bounded paired reads and current approval fencing."""

import json
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import registry_ptg_office_selection as selection
from process.registry_ptg_office_capture import _canonical, _digest
from process.registry_ptg_office_membership_contract import approved_office_recipe_receipt
from process.registry_source_recipe_store import decode_registry_source_recipes
from tests.test_registry_ptg_office_approval import _command


def _pair():
    command = _command()
    approval_by_field = {
        "contract": "registry_ptg_office_approval.v1",
        "capture_id": command["capture_id"],
        "client_id": command["client_id"],
        "command": command,
        "manifest_sha256": "a" * 64,
        "witness": {"custody": {"owner_role": "owner"}},
        "source_pin": {},
        "actor": {},
    }
    approval_sha = _digest(approval_by_field)
    hold_by_field = {
        "contract": "registry_ptg_office_hold.v1",
        "capture_id": command["capture_id"],
        "approval_sha256": approval_sha,
        "manifest_sha256": "a" * 64,
        "witness_sha256": _digest(approval_by_field["witness"]),
        "schema_name": "registry_ptg_office_" + command["capture_id"].replace("-", ""),
        "custody": approval_by_field["witness"]["custody"],
        "retained_serving": command["retained_serving"],
    }
    hold_sha = _digest(hold_by_field)
    receipt = approved_office_recipe_receipt(command, "a" * 64, approval_sha, hold_sha)
    binding_by_field = {
        **command["source"]["coordinates"],
        "source_key": command["source"]["binding_source_key"],
        "source_scope_json": command["source"]["source_scope"],
        "network_id": command["source"]["network_id"],
        "office_recipe_json": {"capture_id": command["capture_id"], "recipe_sha256": receipt["source_recipes_sha256"]},
    }
    receipt_by_name = {
        "ordinal": 1,
        "approval_sha256": approval_sha,
        "hold_sha256": hold_sha,
        "approval_json": _canonical(approval_by_field).decode(),
        "hold_json": _canonical(hold_by_field).decode(),
    }
    return binding_by_field, receipt_by_name, decode_registry_source_recipes(receipt["source_recipes"])[0]


def test_actual_recipe_decoder_matches_real_approval_hold_shape():
    binding_by_field, receipt_by_name, recipe = _pair()
    assert selection._paired_recipe(receipt_by_name, binding_by_field) == recipe
    assert selection.office_recipe_selection(None) is None


@pytest.mark.parametrize(
    "value",
    [
        {},
        {"capture_id": "bad", "recipe_sha256": "a" * 64},
        {"capture_id": "00000000-0000-0000-0000-000000000000", "recipe_sha256": "a" * 64},
        {"capture_id": "12345678-1234-1234-1234-123456789abc", "recipe_sha256": "A" * 64},
        {"capture_id": "12345678-1234-1234-1234-123456789abc", "recipe_sha256": "a" * 64, "extra": 1},
    ],
)
def test_selector_is_closed_and_canonical(value):
    with pytest.raises(ValueError):
        selection.office_recipe_selection(value)


@pytest.mark.parametrize(
    "field",
    [
        "source_id",
        "dataset_schema",
        "dataset_id",
        "producer_id",
        "edition_id",
        "source_key",
        "network_id",
        "source_scope_json",
        "office_recipe_json",
    ],
)
def test_wrong_full_binding_or_selected_recipe_refuses(field):
    binding_by_field, receipt_by_name, _ = _pair()
    bad = deepcopy(binding_by_field)
    if field == "office_recipe_json":
        bad[field]["recipe_sha256"] = "f" * 64
    elif field == "network_id":
        bad[field] += 1
    elif field == "source_scope_json":
        bad[field]["scope_id"] = "12345678-1234-1234-1234-123456789abc"
    else:
        bad[field] = "changed"
    with pytest.raises(ValueError):
        selection._paired_recipe(receipt_by_name, bad)


@pytest.mark.parametrize("field", ["approval_sha256", "hold_sha256", "approval_json", "hold_json"])
def test_missing_or_changed_pair_never_confers_selection(field):
    binding_by_field, receipt_by_name, _ = _pair()
    receipt_by_name[field] = None
    with pytest.raises(ValueError):
        selection._paired_recipe(receipt_by_name, binding_by_field)


@pytest.mark.asyncio
async def test_bounded_reference_pages_and_no_queries_for_legacy(monkeypatch):
    binding_by_field, receipt_by_name, _ = _pair()
    protected = AsyncMock()
    monkeypatch.setattr(selection, "_protected_tables", protected)
    calls = []

    async def fetch(sql, body):
        page = json.loads(body)
        calls.append(len(page))
        return [{**receipt_by_name, "ordinal": i + 1} for i in range(len(page))]

    connection = SimpleNamespace(fetch=fetch)
    assert await selection._read_references(connection, '"scope"', [], None) == ()
    assert not protected.called
    assert len(await selection._read_references(connection, '"scope"', [binding_by_field] * 51, object())) == 51
    assert calls == [25, 25, 1]


@pytest.mark.asyncio
@pytest.mark.parametrize("count", [0, 2])
async def test_unselected_or_ambiguous_current_approved_binding_refuses(monkeypatch, count):
    binding_by_field, _, recipe = _pair()
    approved = SimpleNamespace(approved_revision=9)
    monkeypatch.setattr(selection, "pin_approved_membership_source", AsyncMock(return_value=approved))
    driver = SimpleNamespace(fetch=AsyncMock(return_value=[{"document": json.dumps(binding_by_field)}] * count))
    with pytest.raises(ValueError, match="unselected"):
        await selection.require_selected_ptg_office_recipes(
            driver, (recipe,), approved, control_schema="control", office_authority=(object(), object(), object())
        )
    assert driver.fetch.await_args.args[1] == 9


@pytest.mark.asyncio
async def test_current_selected_pair_passes_and_approved_drift_refuses(monkeypatch):
    binding_by_field, _, recipe = _pair()
    approved = SimpleNamespace(approved_revision=9)
    pin = AsyncMock(return_value=approved)
    monkeypatch.setattr(selection, "pin_approved_membership_source", pin)
    monkeypatch.setattr(selection, "_read_references", AsyncMock(return_value=(recipe,)))
    driver = SimpleNamespace(fetch=AsyncMock(return_value=[{"document": json.dumps(binding_by_field)}]))
    await selection.require_selected_ptg_office_recipes(
        driver, (recipe,), approved, control_schema="control", office_authority=(object(), object(), object())
    )
    pin.return_value = object()
    with pytest.raises(ValueError, match="approved_changed"):
        await selection.require_selected_ptg_office_recipes(
            driver, (recipe,), approved, control_schema="control", office_authority=(object(), object(), object())
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("retained", [False, True])
async def test_actual_composer_checks_only_new_recipe_adoption(monkeypatch, retained):
    from process import registry_candidate_composition as composition

    _, _, recipe = _pair()
    manifest = SimpleNamespace(source_generations={"registry_source_recipes": "a" * 64}) if retained else None
    monkeypatch.setattr(composition, "_retained_source", AsyncMock(return_value=({}, 0)))
    monkeypatch.setattr(
        composition, "resolve_retained_registry_source_recipes", AsyncMock(return_value=(recipe,) if retained else ())
    )
    stage = SimpleNamespace(generation_sha256="b" * 64, membership_rows=0, source_generations=("c" * 64,))
    monkeypatch.setattr(composition, "stage_registry_source_recipes", AsyncMock(return_value=stage))
    monkeypatch.setattr(selection, "resolve_ptg_office_recipe_selection", AsyncMock(return_value=(recipe,)))
    guard = AsyncMock()
    monkeypatch.setattr(selection, "require_selected_ptg_office_recipes", guard)
    await composition._composition_sources(
        object(),
        manifest,
        SimpleNamespace(approved_revision=9),
        object(),
        "control",
        (recipe,),
        (object(), object(), object()),
    )
    assert guard.await_args.args[1] == (() if retained else (recipe,))


def test_additive_migration_matches_model_check_and_never_rewrites_legacy_receipts():
    import runpy
    from pathlib import Path

    from db.models.registry_network_binding import OFFICE_RECIPE_CHECK_SQL

    module = runpy.run_path(
        str(Path(__file__).parents[1] / "alembic/versions/20261017030000_registry_ptg_office_recipe_selection.py")
    )
    statements = module["_ddl"]("fixture")
    assert len(statements) == 2 and OFFICE_RECIPE_CHECK_SQL in statements[1]
    assert module["down_revision"] == "20261017020000_source_registry_snapshot_join"
    assert all("UPDATE " not in sql and "TRIGGER" not in sql and "FUNCTION" not in sql for sql in statements)


@pytest.mark.asyncio
@pytest.mark.parametrize("same_scope", [True, False])
async def test_explicit_replacement_cannot_silently_union_old_capture(monkeypatch, same_scope):
    from dataclasses import replace

    from process.registry_ptg_office_selection_replay import refuse_replaced_capture_union

    binding_by_field, _, recipe = _pair()
    old = replace(recipe, source_pin=replace(recipe.source_pin, capture_id="12345678-1234-1234-1234-123456789abc"))
    scope = deepcopy(binding_by_field["source_scope_json"])
    if not same_scope:
        scope["scope_id"] = "22345678-1234-1234-1234-123456789abc"
    driver = SimpleNamespace(
        fetch=AsyncMock(
            return_value=[
                {
                    "capture_id": old.source_pin.capture_id,
                    "approval_sha256": old.source_pin.approval_sha256,
                    "scope_json": json.dumps(scope),
                }
            ]
        )
    )
    if same_scope:
        with pytest.raises(ValueError, match="ambiguous"):
            await refuse_replaced_capture_union(driver, '"control"', [binding_by_field], (recipe,), (old, recipe))
    else:
        await refuse_replaced_capture_union(driver, '"control"', [binding_by_field], (recipe,), (old, recipe))


@pytest.mark.parametrize("scope", [None, [], "invalid"])
def test_selector_with_malformed_scope_is_request_invalid(scope):
    binding_by_field, _, _ = _pair()
    binding_by_field["source_scope_json"] = scope
    with pytest.raises(ValueError, match="invalid"):
        selection._requested([binding_by_field])


@pytest.mark.asyncio
async def test_effective_staged_selection_is_paged_after_omission_preservation(monkeypatch):
    from uuid import UUID

    binding, _, _ = _pair()
    first_rows = [{"binding_id": UUID(int=index + 1), "document": json.dumps(binding)} for index in range(25)]
    last_rows = [{"binding_id": UUID(int=26), "document": json.dumps(binding)}]
    connection = SimpleNamespace(execute=AsyncMock(), fetch=AsyncMock(side_effect=[first_rows, last_rows, []]))
    reads = AsyncMock()
    monkeypatch.setattr(selection, "_read_references", reads)
    await selection.require_staged_binding_office_references(connection, '"control"', '"private_stage"', object())
    preservation = connection.execute.call_args.args[0]
    assert "target.office_recipe_json IS NULL" in preservation
    assert "target.operation IN ('rebind','close')" in preservation
    assert [len(call.args[2]) for call in reads.call_args_list] == [25, 1]
    assert [call.args[1:] for call in connection.fetch.call_args_list] == [
        (None, 25),
        (UUID(int=25), 25),
        (UUID(int=26), 25),
    ]
    assert all("office_recipe_json<>'null'::jsonb" in call.args[0] for call in connection.fetch.call_args_list)


def test_openapi_allows_exact_published_plan_scope_for_office_selector():
    from pathlib import Path

    import yaml

    document = yaml.safe_load((Path(__file__).parents[1] / "doc/openapi.yaml").read_text())
    row_schema = document["components"]["schemas"]["RegistryNetworkSourceBindingRow"]
    scope = _pair()[0]["source_scope_json"]
    scopes = row_schema["properties"]["source_scope_json"]["oneOf"]
    published_scopes = [branch for branch in scopes if set(branch["required"]) == set(scope)]
    assert len(published_scopes) == 1
    branch = published_scopes[0]
    assert branch["additionalProperties"] is False
    assert set(branch["properties"]) == set(scope)
    assert branch["properties"]["review_type"]["enum"] == [scope["review_type"]]
    assert branch["properties"]["selection_mode"]["enum"] == [scope["selection_mode"]]
    for field, maximum in (("snapshot_id", 96), ("plan_id", 64), ("plan_market_type", 32)):
        assert branch["properties"][field]["minLength"] == 1
        assert branch["properties"][field]["maxLength"] == maximum
    selector = row_schema["properties"]["office_recipe_json"]
    assert selector["nullable"] is True
    assert "Omission preserves" in selector["description"]


def _change(previous, effective):
    return dict(
        has_previous=previous is not None,
        has_effective=effective is not None,
        previous_json=json.dumps(previous) if previous is not None else None,
        effective_json=json.dumps(effective) if effective is not None else None,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["replacement", "archive", "initial"])
async def test_shared_delta_changes_only_exact_linked_recipe(monkeypatch, operation):
    from dataclasses import replace

    from process import registry_ptg_office_selection_replay as replay

    monkeypatch.setattr(replay, "refuse_replaced_capture_union", AsyncMock())
    binding, _, recipe = _pair()
    old = replace(recipe, source_pin=replace(recipe.source_pin, capture_id="12345678-1234-1234-1234-123456789abc"))
    unrelated = replace(
        recipe, source_pin=replace(recipe.source_pin, capture_id="22345678-1234-1234-1234-123456789abc")
    )
    previous_by_name = {**binding, "archived": False}
    effective_by_name = {**binding, "archived": operation == "archive"}
    driver = SimpleNamespace(
        fetch=AsyncMock(return_value=[_change(None if operation == "initial" else previous_by_name, effective_by_name)])
    )
    references = AsyncMock(
        side_effect=[() if operation == "initial" else (old,), () if operation == "archive" else (recipe,)]
    )
    monkeypatch.setattr(selection, "_read_references", references)
    baseline = () if operation == "initial" else (old, unrelated)
    resolved = await selection.resolve_ptg_office_recipe_selection(
        driver, baseline, 9, None, control_schema="control", office_authority=(object(), object(), object())
    )
    expected = () if operation == "archive" else (recipe,)
    if operation != "initial":
        expected += (unrelated,)
    assert set(resolved) == set(expected)
    assert driver.fetch.await_args.args[1:] == (9, 0, 101)
    assert references.await_count == 2


@pytest.mark.asyncio
async def test_delta_limit_and_wrong_network_refuse_before_recipe_adoption(monkeypatch):
    binding, _, recipe = _pair()
    driver = SimpleNamespace(fetch=AsyncMock(return_value=[_change(None, {**binding, "archived": False})] * 101))
    references = AsyncMock(return_value=(recipe,))
    monkeypatch.setattr(selection, "_read_references", references)
    with pytest.raises(ValueError, match="too_many_changes"):
        await selection.resolve_ptg_office_recipe_selection(driver, (), 9, None, control_schema="control")
    references.assert_not_called()
    driver.fetch.return_value = [_change(None, {**binding, "archived": False, "network_id": binding["network_id"] + 1})]
    references.side_effect = [(), (recipe,)]
    with pytest.raises(ValueError, match="selection_changed"):
        await selection.resolve_ptg_office_recipe_selection(
            driver, (), 9, None, control_schema="control", office_authority=(object(), object(), object())
        )


def test_explicit_startup_selection_cannot_override_reviewed_delta():
    from dataclasses import replace

    _, _, recipe = _pair()
    other = replace(recipe, source_pin=replace(recipe.source_pin, capture_id="12345678-1234-1234-1234-123456789abc"))
    selection.require_ptg_startup_selection((recipe,), (recipe,))
    with pytest.raises(ValueError, match="startup_conflict"):
        selection.require_ptg_startup_selection((other,), (recipe,))


@pytest.mark.asyncio
@pytest.mark.parametrize("is_archived", [False, True])
async def test_active_cleared_selection_refuses_and_archive_removes_only_linked_recipe(monkeypatch, is_archived):
    from process import registry_ptg_office_selection_replay as replay

    binding_by_field, _, recipe = _pair()
    previous_by_name = {**binding_by_field, "archived": False}
    effective_by_name = {**binding_by_field, "archived": is_archived, "office_recipe_json": None}
    driver = SimpleNamespace(fetch=AsyncMock(return_value=[_change(previous_by_name, effective_by_name)]))
    references = AsyncMock(side_effect=[(recipe,), ()])
    monkeypatch.setattr(selection, "_read_references", references)
    monkeypatch.setattr(replay, "refuse_replaced_capture_union", AsyncMock())
    if is_archived:
        assert (
            await selection.resolve_ptg_office_recipe_selection(
                driver, (recipe,), 9, None, control_schema="control", office_authority=(object(), object(), object())
            )
            == ()
        )
    else:
        with pytest.raises(ValueError, match="selection_unavailable"):
            await selection.resolve_ptg_office_recipe_selection(
                driver, (recipe,), 9, None, control_schema="control", office_authority=(object(), object(), object())
            )
        references.assert_not_called()


@pytest.mark.asyncio
async def test_archiving_office_selection_preserves_unrelated_legacy_binding_edit(monkeypatch):
    from process import registry_ptg_office_selection_replay as replay

    binding, _, recipe = _pair()
    previous_by_name = {**binding, "archived": False}
    legacy_by_name = {**previous_by_name, "source_key": "unrelated-plan", "office_recipe_json": None}
    changes = [
        _change(previous_by_name, {**previous_by_name, "archived": True}),
        _change(legacy_by_name, {**legacy_by_name, "network_id": legacy_by_name["network_id"] + 1}),
    ]
    driver = SimpleNamespace(fetch=AsyncMock(return_value=changes))
    references = AsyncMock(side_effect=[(recipe,), ()])
    monkeypatch.setattr(selection, "_read_references", references)
    monkeypatch.setattr(replay, "refuse_replaced_capture_union", AsyncMock())

    assert (
        await selection.resolve_ptg_office_recipe_selection(
            driver, (recipe,), 9, None, control_schema="control", office_authority=(object(), object(), object())
        )
        == ()
    )
    assert references.await_count == 2


@pytest.mark.asyncio
async def test_default_composer_consumes_shared_selection_without_startup_recipe_override(monkeypatch):
    from process import registry_candidate_composition as composition

    _, _, recipe = _pair()
    selected = AsyncMock(return_value=(recipe,))
    monkeypatch.setattr(selection, "resolve_ptg_office_recipe_selection", selected)
    monkeypatch.setattr(composition, "_retained_source", AsyncMock(return_value=({}, 0)))
    stage = SimpleNamespace(generation_sha256="a" * 64, membership_rows=1, source_generations=("b" * 64,))
    staged = AsyncMock(return_value=stage)
    monkeypatch.setattr(composition, "stage_registry_source_recipes", staged)
    approved = SimpleNamespace(approved_revision=9)
    connection = object()
    authority = (object(), object(), object())
    generations, count, recipes, actual_stage = await composition._composition_sources(
        connection, None, approved, object(), "control", office_authority=authority
    )
    assert count == 1 and recipes == (recipe,) and actual_stage is stage
    assert generations == {"registry_source_membership": "a" * 64}
    assert staged.await_args.args[1:3] == ((recipe,), approved)
    assert staged.await_args.kwargs["office_authority"] is authority
    selected.assert_awaited_once()


@pytest.mark.asyncio
async def test_composer_refuses_startup_conflict_before_staging(monkeypatch):
    from dataclasses import replace

    from process import registry_candidate_composition as composition

    _, _, selected_recipe = _pair()
    configured_recipe = replace(
        selected_recipe,
        source_pin=replace(selected_recipe.source_pin, capture_id="12345678-1234-1234-1234-123456789abc"),
    )
    monkeypatch.setattr(composition, "_retained_source", AsyncMock(return_value=({}, 0)))
    monkeypatch.setattr(selection, "resolve_ptg_office_recipe_selection", AsyncMock(return_value=(selected_recipe,)))
    staging = AsyncMock()
    monkeypatch.setattr(composition, "stage_registry_source_recipes", staging)
    with pytest.raises(ValueError, match="startup_conflict"):
        await composition._composition_sources(
            object(),
            None,
            SimpleNamespace(approved_revision=9),
            object(),
            "control",
            (configured_recipe,),
            (object(), object(), object()),
        )
    staging.assert_not_called()


@pytest.mark.asyncio
async def test_historical_source_rollback_retains_requested_recipes_instead_of_latest_binding(monkeypatch):
    from process import registry_candidate_composition as composition
    from process.network_approved_membership_source import ApprovedMembershipSource
    from process.registry_membership_composition_stage import RegistryMembershipCommandStage
    from process.registry_source_recipe_composition import RegistrySourceRecipeStage

    _, _, recipe = _pair()
    manifest = SimpleNamespace(source_generations={"registry_source_recipes": "a" * 64}, generation_id=1)
    current_approved = ApprovedMembershipSource(9, "c" * 64, 2)
    retained_stage = RegistrySourceRecipeStage("registry_source_page_" + "1" * 32, 2, 0, "b" * 64)
    current_commands = RegistryMembershipCommandStage(
        current_approved, "registry_command_add_" + "2" * 32, "registry_command_remove_" + "2" * 32, 1, 1
    )
    driver = SimpleNamespace(
        is_in_transaction=lambda: True,
        fetchrow=AsyncMock(return_value=dict(source_rows=2, suppressed_source_rows=1, add_rows=1, remove_rows=1)),
    )
    monkeypatch.setattr(composition, "_retained_source", AsyncMock(return_value=({}, 2)))
    monkeypatch.setattr(composition, "resolve_retained_registry_source_recipes", AsyncMock(return_value=(recipe,)))
    resolver = AsyncMock(side_effect=AssertionError("historical source must not be replaced"))
    monkeypatch.setattr(selection, "resolve_ptg_office_recipe_selection", resolver)
    staged = AsyncMock(return_value=retained_stage)
    monkeypatch.setattr(composition, "stage_registry_source_recipes", staged)
    command_reader = AsyncMock(return_value=current_commands)
    monkeypatch.setattr(composition, "stage_approved_membership_commands", command_reader)
    request = object()
    _, count, recipes, source_stage = await composition._composition_sources(
        driver,
        manifest,
        current_approved,
        request,
        "control",
        office_authority=(object(), object(), object()),
        expected_head=2,
    )
    assert count == 2 and recipes == (recipe,) and staged.await_args.args[1] == (recipe,)
    resolver.assert_not_called()
    commands, retention = await composition._composition_retention(
        driver, current_approved, request, source_stage, "control"
    )
    command_reader.assert_awaited_once_with(driver, current_approved, request_id=request, control_schema="control")
    assert commands is current_commands and retention["source_generation_sha256"] == retained_stage.generation_sha256
    assert (
        retention["source_rows"],
        retention["add_rows"],
        retention["remove_rows"],
        retention["suppressed_source_rows"],
        retention["effective_rows"],
    ) == (2, 1, 1, 1, 2)
    sql = driver.fetchrow.await_args.args[0]
    assert (
        retained_stage.table_name in sql and current_commands.add_table in sql and current_commands.remove_table in sql
    )
