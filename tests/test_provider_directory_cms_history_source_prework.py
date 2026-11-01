# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pending prework preserves native bound identities and complete existing Source custody."""

import asyncio
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process import provider_directory_cms_history_bounds as bounds
from process import provider_directory_cms_history_source_prework as prework
from process import provider_directory_source_storage_binding as storage
from process.provider_directory_generic_candidate import GenericCandidateError
from tests.test_provider_directory_cms_history_bounds import _source_rows
from tests.test_provider_directory_source_history_bounds_custody import inventory_case as inventory_case


@pytest.fixture(params=[False, True], ids=["pending", "validated"])
def source_case(inventory_case, monkeypatch, request):
    """Bind unchanged Source data and explicit native bound states with catalog IO replaced."""
    case = inventory_case
    case.validated = request.param
    case.actor = dict(session="administrator", current="administrator", administrator=True, backend=301)
    case.roles = {"namespace_owner": {"oid": 4}, "source_owner": {"oid": 5}}
    case.coordinates = dict(
        database_oid=1, catalog_schema_oid=2, storage_schema_oid=3, owner_oid=5, namespace_owner_oid=4
    )
    case.publication = dict(owner_role="publication_owner", publisher_role="publication_reader")
    case.application = {"check": "unchanged"}
    case.admission = {root: {"oid": number} for number, root in enumerate(storage._CATALOG_ROOTS, 600)}
    case.metadata = {"metadata_function()": {"owner": 4}}
    original_evidence = deepcopy(case.evidence)
    for layout in original_evidence["layouts"]:
        if layout["nspname"] == "catalog" and layout["relname"] in case.expected:
            layout["constraints"] = []
    case.source = dict(
        version=2,
        policy=dict(migration_role="migration", namespace_owner_role="namespace_owner"),
        publication=deepcopy(case.publication),
        storage_custody=dict(
            version=1,
            coordinates=dict(catalog_schema="catalog", storage_schema=case.policy.schema, **case.coordinates),
            roles=deepcopy(case.roles),
            inventory=storage._isolated_inventory(case.policy, "catalog", case.roles, original_evidence),
            application_checks=deepcopy(case.application),
            canonical_authority=dict(admission=deepcopy(case.admission), metadata=deepcopy(case.metadata)),
        ),
    )
    for entry in case.expected.values():
        entry["native"]["constraint"]["validated"] = case.validated
    case.rows = _source_rows(case.expected)
    for native_bound in case.rows:
        native_bound["bound_definition"] = "CHECK (false)" + ("" if case.validated else " NOT VALID")
        layout = next(
            layout
            for layout in case.evidence["layouts"]
            if layout["nspname"] == "catalog" and layout["relname"] == native_bound["root"]
        )
        layout["constraints"][0][2:4] = [native_bound["bound_definition"], case.validated]
    case.connection = SimpleNamespace(
        is_in_transaction=Mock(return_value=True),
        is_closed=Mock(return_value=False),
        fetch=AsyncMock(return_value=case.rows),
        fetchrow=AsyncMock(return_value=case.actor),
    )
    _source_stubs(case, monkeypatch)
    return case


def _source_stubs(case, monkeypatch):
    async def identity(*args):
        return asyncio.current_task(), case.actor, case.roles, case.coordinates

    case.identity = AsyncMock(side_effect=identity)
    case.catalog = AsyncMock(return_value=case.evidence)
    case.access = AsyncMock()
    monkeypatch.setattr(storage, "_isolated_storage_identity", case.identity)
    monkeypatch.setattr(storage, "_isolated_catalog_rows", case.catalog)
    monkeypatch.setattr(storage, "_isolated_application_checks", lambda evidence: case.application)
    monkeypatch.setattr(storage, "_isolated_admission_rights", lambda schema, evidence: case.admission)
    monkeypatch.setattr(storage, "_isolated_metadata_custody", AsyncMock(return_value=case.metadata))
    monkeypatch.setattr(storage, "_isolated_access_closure", case.access)


async def _check(case, **options):
    return await prework.require_cms_history_source_prework(
        case.connection,
        case.policy,
        source_document=case.source,
        history_bounds=case.expected,
        bounds_validated=options.get("validated", case.validated),
    )


def _layout(case, *, private=False, root=None):
    return next(
        layout
        for layout in case.evidence["layouts"]
        if layout["nspname"] == (case.policy.schema if private else "catalog")
        and layout["relname"] == (root or bounds.history.ROOTS[0])
    )


async def test_full_source_check_accepts_only_the_explicit_state_without_mutating_inputs(source_case):
    case = source_case
    original = deepcopy((case.source, case.expected, case.evidence, case.rows))
    result = await _check(case)
    assert result == case.source["storage_custody"] and result is not case.source["storage_custody"]
    assert (case.source, case.expected, case.evidence, case.rows) == original
    case.catalog.assert_awaited_once_with(
        case.connection, case.policy, "catalog", case.roles, expected_history_bounds=case.expected
    )
    case.access.assert_awaited_once_with(
        case.connection, result["inventory"], case.roles, case.policy, "migration", case.publication
    )
    assert case.connection.fetch.await_count == 2
    assert all(entry["native"]["constraint"]["validated"] is case.validated for entry in case.expected.values())


async def test_nonempty_dataset_membership_is_checked_without_rewriting_the_native_proof(source_case):
    case = source_case
    for root, entry in case.expected.items():
        entry["dataset_ids"] = ["retained'one", "retained_two"]
        entry["native"]["constraint"]["columns"] = [1]
        native_bound = next(native_bound for native_bound in case.rows if native_bound["root"] == root)
        native_bound["bound"]["constraints"][0]["columns"] = [1]
        native_bound["bound"]["constraints"][0]["expression"] = bounds._expression(entry["dataset_ids"], True)
        native_bound["bound_definition"] = "CHECK (" + native_bound["bound"]["constraints"][0]["expression"] + ")"
        if not case.validated:
            native_bound["bound_definition"] += " NOT VALID"
        _layout(case, root=root)["constraints"][0][2] = native_bound["bound_definition"]
    original = deepcopy(case.expected)
    assert await _check(case) == case.source["storage_custody"]
    assert case.expected == original


@pytest.mark.parametrize("field", ["private", "admission", "metadata", "application", "roles", "coordinates"])
async def test_native_custody_changes_cannot_become_expected_policy(source_case, field):
    case = source_case
    changes_by_field = {
        "private": (_layout(case, private=True), "columns", [["changed"]]),
        "admission": (case.admission["import_run"], "oid", case.admission["import_run"]["oid"] + 1),
        "metadata": (case.metadata["metadata_function()"], "owner", 99),
        "application": (case.application, "check", "changed"),
        "roles": (case.roles["source_owner"], "oid", 99),
        "coordinates": (case.coordinates, "database_oid", 99),
    }
    changed, key, replacement = changes_by_field[field]
    changed[key] = replacement
    with pytest.raises(
        (ValueError, GenericCandidateError), match="storage_changed|coordinates_changed|template_layout_changed"
    ):
        await _check(case)


@pytest.mark.parametrize(
    "fault", ["oid", "expression", "validation", "owner", "column", "inherited", "root", "parents"]
)
async def test_changed_native_bound_refuses_before_template_projection(source_case, fault):
    case = source_case
    native_bound = case.rows[0]
    native = native_bound["bound"]["constraints"][0]
    changes_by_fault = {
        "oid": (native, "oid", native["oid"] + 100),
        "expression": (native, "expression", "true"),
        "validation": (native, "validated", not case.validated),
        "owner": (native_bound, "relowner", native_bound["relowner"] + 1),
        "column": (native_bound["bound"]["column"], "required", False),
        "inherited": (native, "inherited", 1),
        "root": (native_bound, "canonical_oid", native_bound["canonical_oid"] + 1),
        "parents": (native_bound, "parents", [999]),
    }
    changed, key, replacement = changes_by_fault[fault]
    changed[key] = replacement
    with pytest.raises(ValueError, match="history_|column_changed|validated_bound_changed"):
        await _check(case)
    case.catalog.assert_not_awaited()


@pytest.mark.parametrize("fault", ["oid", "definition", "state", "private", "other", "missing", "duplicate"])
async def test_projection_drops_only_the_exact_canonical_bound_tuple(source_case, fault):
    case = source_case
    layout = _layout(case)
    check = layout["constraints"][0]
    changes_by_fault = {
        "oid": (check, -1, check[-1] + 100),
        "definition": (check, 2, "CHECK (true)"),
        "state": (check, 3, not case.validated),
        "private": (_layout(case, private=True), "constraints", [deepcopy(check)]),
        "other": (layout, "constraints", [check, ["other_check", "c", "CHECK (true)", True, False, False, True]]),
        "missing": (layout, "constraints", []),
        "duplicate": (layout, "constraints", [check, deepcopy(check)]),
    }
    changed, key, replacement = changes_by_fault[fault]
    changed[key] = replacement
    original = deepcopy(case.evidence)
    with pytest.raises((ValueError, GenericCandidateError), match="history_|template_layout_changed"):
        await _check(case)
    assert case.evidence == original


@pytest.mark.parametrize("validated", [None, 0, 1, "false"])
async def test_validation_state_is_not_coerced(source_case, validated):
    with pytest.raises(ValueError, match="validation_state_required"):
        await _check(source_case, validated=validated)
    source_case.identity.assert_not_awaited()


async def test_supplied_bound_state_cannot_be_relabelled(source_case):
    with pytest.raises(ValueError, match="validated_bound_changed"):
        await _check(source_case, validated=not source_case.validated)
    source_case.identity.assert_not_awaited()


@pytest.mark.parametrize("fault", ["partial", "duplicate_oid", "already_bound"])
async def test_incomplete_or_repeated_source_transition_refuses_before_io(source_case, fault):
    case = source_case
    roots = bounds.history.ROOTS
    if fault == "partial":
        case.expected.pop(roots[0])
    elif fault == "duplicate_oid":
        case.expected[roots[0]]["old_oid"] = case.expected[roots[1]]["old_oid"]
    else:
        case.source["storage_custody"]["canonical_authority"]["history_bounds"] = deepcopy(case.expected)
    with pytest.raises(ValueError, match="expectations_required|identity_changed|unbound_source_required"):
        await _check(case)
    case.identity.assert_not_awaited()


async def test_access_closure_failure_prevents_acceptance(source_case):
    source_case.access.side_effect = ValueError("private_access_open")
    with pytest.raises(ValueError, match="private_access_open"):
        await _check(source_case)


async def test_late_bound_identity_change_is_detected(source_case):
    case = source_case
    changed = deepcopy(case.rows)
    changed[0]["bound"]["constraints"][0]["oid"] += 100
    case.connection.fetch.side_effect = [case.rows, changed]
    with pytest.raises(ValueError, match="history_bound_changed"):
        await _check(case)


async def test_original_actor_is_rechecked(source_case):
    source_case.connection.fetchrow.return_value = {**source_case.actor, "backend": 999}
    with pytest.raises(ValueError, match="transaction_changed"):
        await _check(source_case)


async def test_closed_transaction_refuses_before_any_capture(source_case):
    source_case.connection.is_closed.return_value = True
    with pytest.raises(ValueError, match="transaction_required"):
        await _check(source_case)
    source_case.identity.assert_not_awaited()


async def test_ordinary_source_capture_stays_strict(source_case):
    case = source_case
    arguments_by_field = dict(
        catalog_schema="catalog",
        migration_role="migration",
        namespace_owner_role="namespace_owner",
        publication=case.publication,
    )
    with pytest.raises(GenericCandidateError, match="template_layout_changed"):
        await storage.capture_isolated_source_storage_custody(case.connection, case.policy, **arguments_by_field)
    if not case.validated:
        with pytest.raises(ValueError, match="validated_bound_changed"):
            await storage.capture_isolated_source_storage_custody(
                case.connection, case.policy, expected_history_bounds=case.expected, **arguments_by_field
            )


async def test_ordinary_unbound_source_capture_is_unchanged(source_case):
    case = source_case
    for root in bounds.history.ROOTS:
        _layout(case, root=root)["constraints"] = []
    observed = await storage.capture_isolated_source_storage_custody(
        case.connection,
        case.policy,
        catalog_schema="catalog",
        migration_role="migration",
        namespace_owner_role="namespace_owner",
        publication=case.publication,
    )
    assert observed == case.source["storage_custody"]
    case.connection.fetch.assert_not_awaited()
