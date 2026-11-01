# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bound projection preserves private layouts and every other custody check."""

import asyncio
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process import provider_directory_cms_history_bounds as bounds
from process import provider_directory_generic_source_provision as provision
from process import provider_directory_source_storage_binding as binding
from process.provider_directory_generic_candidate import GenericCandidateError
from process.provider_directory_generic_source_policy import GenericSourcePolicy
from tests.test_provider_directory_cms_history_bounds import _observed_bound


@pytest.fixture
def inventory_case(monkeypatch):
    expected_by_root = {}
    for number, root in enumerate(bounds.history.ROOTS):
        native = _observed_bound([])
        native["constraints"][0]["oid"] += number
        expected_by_root[root] = dict(
            old_oid=701 + number, dataset_ids=[], native=bounds.require_cms_history_bound(native, [])
        )
    policy = GenericSourcePolicy("private_source", "source_owner", "source_publisher", ("source_reader",))
    evidence_by_field = dict(
        catalog=[], templates=[], layouts=[], physical=[], checkpoints=[], readers_closed=True, dependencies_closed=True
    )
    for number, root in enumerate(provision.ISOLATED_FHIR_ROOTS):
        private_by_field = dict(scope="private", nspname=policy.schema, relname=root, oid=101 + number)
        evidence_by_field["catalog"].append(private_by_field)
        evidence_by_field["templates"].append(dict(private_by_field, supported=True))
        for schema in (policy.schema, "catalog"):
            layout_by_field = dict(
                nspname=schema, relname=root, oid=private_by_field["oid"], columns=[], constraints=[], indexes=[]
            )
            if schema == "catalog":
                layout_by_field["oid"] += 1000
                if root in expected_by_root:
                    entry = expected_by_root[root]
                    layout_by_field["oid"] = entry["old_oid"]
                    layout_by_field["constraints"] = [
                        [
                            "cms_history_bound",
                            "c",
                            "CHECK (false)",
                            True,
                            False,
                            False,
                            True,
                            entry["native"]["constraint"]["oid"],
                        ]
                    ]
            evidence_by_field["layouts"].append(layout_by_field)
    monkeypatch.setattr(binding, "_require_isolated_relation_layouts", Mock())
    monkeypatch.setattr(provision, "_fhir_physical_rows", lambda rows, physical: rows)
    monkeypatch.setattr(provision, "_fhir_catalog_row", deepcopy)
    return SimpleNamespace(policy=policy, evidence=evidence_by_field, expected=expected_by_root)


def _inventory(case, *, expected=True):
    return binding._isolated_inventory(
        case.policy, "catalog", {}, case.evidence, expected_history_bounds=case.expected if expected else None
    )


def _layout(case, root=None, *, private=False):
    return next(
        row
        for row in case.evidence["layouts"]
        if row["nspname"] == (case.policy.schema if private else "catalog")
        and row["relname"] == (root or bounds.history.ROOTS[0])
    )


def test_only_explicit_authenticated_bound_delta_is_projected(inventory_case):
    case = inventory_case
    original = deepcopy(case.evidence)
    observed = _inventory(case)
    assert case.evidence == original
    assert all(not entry["native_layout"]["constraints"] for entry in observed.values())
    with pytest.raises(GenericCandidateError, match="template_layout_changed"):
        _inventory(case, expected=False)
    for root in bounds.history.ROOTS:
        _layout(case, root)["constraints"] = []
    assert _inventory(case, expected=False) == observed
    # After conversion the canonical roots have new OIDs and no history CHECK.
    for root in bounds.history.ROOTS:
        _layout(case, root)["oid"] += 2000
    assert _inventory(case) == observed


@pytest.mark.parametrize(
    "fault", ["private", "other_root", "oid", "constraint_oid", "flags", "kind", "missing", "duplicate"]
)
def test_projection_cannot_hide_an_unrelated_or_private_check(inventory_case, fault):
    case = inventory_case
    canonical = _layout(case)
    check = canonical["constraints"][0]
    faults_by_name = dict(
        private=(_layout(case, private=True), "constraints", [deepcopy(check)]),
        other_root=(_layout(case, provision.HEADER), "constraints", [deepcopy(check)]),
        oid=(canonical, "oid", canonical["oid"] + 1),
        constraint_oid=(check, -1, check[-1] + 1),
        flags=(check, 3, False),
        kind=(check, 1, "u"),
        missing=(canonical, "constraints", []),
        duplicate=(canonical, "constraints", [deepcopy(check), deepcopy(check)]),
    )
    changed, key, replacement = faults_by_name[fault]
    changed[key] = replacement
    with pytest.raises(GenericCandidateError, match="history_bound|template_layout_changed"):
        _inventory(case)


@pytest.mark.parametrize("field", ["columns", "constraints", "indexes"])
def test_projection_preserves_all_other_layout_comparisons(inventory_case, field):
    _layout(inventory_case)[field].append(
        ["other_check", "c", "CHECK (true)", True] if field == "constraints" else ["changed"]
    )
    with pytest.raises(GenericCandidateError, match="template_layout_changed"):
        _inventory(inventory_case)


def test_bound_constraint_identity_is_opt_in_to_native_layout_sql():
    marker = "THEN jsonb_build_array(k.oid::bigint)"
    assert marker not in binding._isolated_catalog_sql()
    assert binding._isolated_catalog_sql(include_history_bound_identity=True).count(marker) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("bounded", [False, True])
async def test_capture_validates_before_projection_and_keeps_exact_sealed_field(inventory_case, monkeypatch, bounded):
    case = inventory_case
    if not bounded:
        for root in bounds.history.ROOTS:
            _layout(case, root)["constraints"] = []
    original = deepcopy(case.expected)
    identity_by_field = dict(session="admin", current="admin", administrator=True)
    connection = SimpleNamespace(
        is_in_transaction=lambda: True,
        fetchval=AsyncMock(return_value="origin"),
        fetchrow=AsyncMock(side_effect=[dict(namespace_owner_oid=20, catalog_schema_oid=30), identity_by_field]),
    )
    monkeypatch.setattr(
        binding,
        "_require_source_storage_publisher",
        AsyncMock(return_value=(asyncio.current_task(), identity_by_field)),
    )
    monkeypatch.setattr(binding, "_isolated_roles", AsyncMock(return_value={"namespace_owner": {"oid": 20}}))
    native = AsyncMock(return_value=case.expected)
    monkeypatch.setattr(bounds, "require_cms_source_history_bounds", native)

    async def catalog(*args, **kwargs):
        assert native.await_count == int(bounded)
        assert kwargs["expected_history_bounds"] == (original if bounded else None)
        return case.evidence

    monkeypatch.setattr(binding, "_isolated_catalog_rows", catalog)
    monkeypatch.setattr(binding, "_isolated_application_checks", lambda evidence: {"check": "unchanged"})
    monkeypatch.setattr(binding, "_isolated_admission_rights", lambda schema, evidence: {"admission": "unchanged"})
    monkeypatch.setattr(binding, "_isolated_metadata_custody", AsyncMock(return_value={"metadata": "unchanged"}))
    monkeypatch.setattr(binding, "_isolated_access_closure", AsyncMock())
    observed = await binding.capture_isolated_source_storage_custody(
        connection,
        case.policy,
        catalog_schema="catalog",
        migration_role="migration",
        namespace_owner_role="namespace_owner",
        publication={},
        expected_history_bounds=case.expected if bounded else None,
    )
    if bounded:
        assert observed["canonical_authority"]["history_bounds"] == original
        case.expected.clear()
        assert observed["canonical_authority"]["history_bounds"] == original
        native.assert_awaited_once_with(connection, catalog_schema="catalog", expected=original, owner_oid=20)
    else:
        assert set(observed["canonical_authority"]) == {"admission", "metadata"}
        native.assert_not_awaited()


@pytest.mark.asyncio
async def test_invalid_expectations_fail_before_native_io(inventory_case):
    connection = SimpleNamespace(is_in_transaction=lambda: True)
    with pytest.raises(ValueError, match="expectations_required"):
        await binding.capture_isolated_source_storage_custody(
            connection,
            inventory_case.policy,
            catalog_schema="catalog",
            migration_role="migration",
            namespace_owner_role="namespace_owner",
            publication={},
            expected_history_bounds={},
        )


@pytest.mark.asyncio
async def test_native_bound_refusal_prevents_catalog_projection(inventory_case, monkeypatch):
    connection = object()
    monkeypatch.setattr(
        binding, "_isolated_storage_identity", AsyncMock(return_value=(None, None, {}, {"namespace_owner_oid": 20}))
    )
    monkeypatch.setattr(
        bounds, "require_cms_source_history_bounds", AsyncMock(side_effect=ValueError("native_pin_changed"))
    )
    catalog = AsyncMock()
    monkeypatch.setattr(binding, "_isolated_catalog_rows", catalog)
    with pytest.raises(ValueError, match="native_pin_changed"):
        await binding.capture_isolated_source_storage_custody(
            connection,
            inventory_case.policy,
            catalog_schema="catalog",
            migration_role="migration",
            namespace_owner_role="namespace_owner",
            publication={},
            expected_history_bounds=inventory_case.expected,
        )
    catalog.assert_not_awaited()
