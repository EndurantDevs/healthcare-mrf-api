# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Opted-in sibling membership rejects a whole source family before publication."""

from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process.custom_import import runner_graph
from process.custom_import.definition import (
    CustomImportDefinition,
    DefinitionError,
    canonical_json,
    load_json_definition,
)
from process.custom_import.family import FamilyBuildResult, assemble_root_families, has_child_membership
from process.custom_import.runner_types import CandidateRunnerError, StoredCandidateChild, StoredCandidateFamily

_FIXTURE = Path(__file__).with_name("fixtures") / "custom_import" / "v1_valid.json"
_MEMBERSHIP = {
    "outer_collection": "rates",
    "inner_collection": "details",
    "key_mapping": [{"outer_field": "service_code", "inner_field": "rate_code"}],
}


def _document():
    document = load_json_definition(_FIXTURE.read_text())
    document["streams"].append(
        {
            "id": "details",
            "kind": "child",
            "child": "details",
            "format": "ndjson",
            "compression": "none",
            "snapshot_token": "snapshot_id",
        }
    )
    document["schema"]["children"].append(
        {
            "name": "details",
            "parent_key": [{"child": "detail_npi", "root": "npi"}],
            "child_key": ["rate_code", "detail_id"],
            "fields": [
                {"id": "detail_npi", "slot": 6, "type": "string", "nullable": False},
                {"id": "rate_code", "slot": 7, "type": "string", "nullable": False, "projection_slot": 5},
                {"id": "detail_id", "slot": 8, "type": "integer", "nullable": False, "projection_slot": 6},
                {"id": "score", "slot": 9, "type": "integer", "nullable": True, "projection_slot": 7},
            ],
        }
    )
    return document


def _definition(*, constrained=True):
    document = _document()
    if constrained:
        document["child_memberships"] = [deepcopy(_MEMBERSHIP)]
    return CustomImportDefinition.from_mapping(document)


def _root(npi="1234567893"):
    return {"npi": npi, "display_name": "Synthetic Provider"}


def _outer(npi="1234567893", code="A100"):
    return {"rate_npi": npi, "service_code": code, "amount": None}


def _inner(npi="1234567893", code="A100", detail_id=1):
    return {"detail_npi": npi, "rate_code": code, "detail_id": detail_id, "score": 2}


def test_optional_membership_changes_only_definition_identity():
    document = _document()
    previous = CustomImportDefinition.from_mapping(document)
    assert previous.child_memberships == () and previous.canonical == canonical_json(document)
    document["revision"]["definition"] += 1
    document["child_memberships"] = [deepcopy(_MEMBERSHIP)]
    current = CustomImportDefinition.from_mapping(document, previous=previous)
    assert current.child_memberships[0].key_mapping == (("service_code", "rate_code"),)
    assert current.schema_digest == previous.schema_digest and current.schema_revision == previous.schema_revision
    assert current.digest != previous.digest
    assert CustomImportDefinition.from_json(current.canonical) == current


@pytest.mark.parametrize(
    "change",
    [
        {"outer_collection": "missing"},
        {"inner_collection": "rates"},
        {"key_mapping": []},
        {"key_mapping": [{"outer_field": "amount", "inner_field": "rate_code"}]},
        {"key_mapping": [{"outer_field": "service_code", "inner_field": "detail_id"}]},
        {"key_mapping": [{"outer_field": "service_code", "inner_field": "score"}]},
    ],
)
def test_membership_rejects_unavailable_or_incompatible_mapping(change):
    document = _document()
    document["child_memberships"] = [_MEMBERSHIP | change]
    with pytest.raises(DefinitionError, match="child_membership"):
        CustomImportDefinition.from_mapping(document)


def test_membership_rejects_unsupported_key_type_without_changing_other_keys():
    document = _document()
    outer, inner = document["schema"]["children"]
    outer["child_key"] = ["amount"]
    outer["fields"][2]["nullable"] = False
    inner["child_key"] = ["rate_code"]
    inner["fields"][1]["type"] = "decimal"
    document["child_memberships"] = [
        {
            **_MEMBERSHIP,
            "key_mapping": [{"outer_field": "amount", "inner_field": "rate_code"}],
        }
    ]
    with pytest.raises(DefinitionError, match="child_membership"):
        CustomImportDefinition.from_mapping(document)


@pytest.mark.parametrize("entries", [None, [], [_MEMBERSHIP] * 2, [_MEMBERSHIP] * 9])
def test_membership_array_is_explicit_bounded_and_unique(entries):
    document = _document()
    document["child_memberships"] = entries
    with pytest.raises(DefinitionError, match="child_memberships"):
        CustomImportDefinition.from_mapping(document)


def test_every_declared_membership_is_enforced():
    document = _document()
    document["schema"]["children"][1]["fields"][2]["type"] = "string"
    document["child_memberships"] = [
        deepcopy(_MEMBERSHIP),
        {
            **_MEMBERSHIP,
            "key_mapping": [{"outer_field": "service_code", "inner_field": "detail_id"}],
        },
    ]
    definition = CustomImportDefinition.from_mapping(document)
    assert len(definition.child_memberships) == 2
    assert has_child_membership(definition, {"rates": [_outer()], "details": [_inner(detail_id="A100")]})
    assert not has_child_membership(definition, {"rates": [_outer()], "details": [_inner(detail_id="B200")]})


def test_integer_membership_uses_typed_key_equality():
    document = _document()
    outer = document["schema"]["children"][0]
    outer["fields"].append({"id": "rate_id", "slot": 10, "type": "integer", "nullable": False})
    outer["child_key"] = ["rate_id"]
    document["child_memberships"] = [
        {
            **_MEMBERSHIP,
            "key_mapping": [{"outer_field": "rate_id", "inner_field": "detail_id"}],
        }
    ]
    definition = CustomImportDefinition.from_mapping(document)
    outer_row = _outer() | {"rate_id": 7}
    assert has_child_membership(definition, {"rates": [outer_row], "details": [_inner(detail_id=7)]})
    result = assemble_root_families(definition, [_root()], {"rates": [outer_row], "details": [_inner(detail_id=8)]})
    assert not result.families and [item.code for item in result.rejections] == ["child_membership_missing"]


def test_missing_sibling_rejects_root_after_collapse():
    document = _document()
    document["streams"][1]["duplicate_policy"] = "collapse_identical"
    document["child_memberships"] = [deepcopy(_MEMBERSHIP)]
    definition = CustomImportDefinition.from_mapping(document)
    roots = [_root(), _root("1003000126")]
    children_by_collection = {
        "rates": [_outer("1003000126"), _outer("1003000126")],
        "details": [_inner(), _inner("1003000126")],
    }
    result = assemble_root_families(definition, roots, children_by_collection)
    assert [family.root_key for family in result.families] == [("1003000126",)]
    assert len(result.families[0].children["rates"]) == 1
    assert [(rejection.root_key, rejection.code) for rejection in result.rejections] == [
        (("1234567893",), "child_membership_missing")
    ]
    assert result.candidate_errors == ()
    assert has_child_membership(definition, {"rates": [_outer()], "details": [_inner()]})
    assert not has_child_membership(definition, {"rates": [_outer()], "details": [_inner(code="B200")]})
    assert has_child_membership(_definition(constrained=False), {"rates": [], "details": [_inner()]})


@pytest.mark.asyncio
async def test_selected_retained_family_must_satisfy_new_membership(monkeypatch):
    definition = _definition()
    request = SimpleNamespace(definition=definition, schema_revision_id=12)
    pointer = SimpleNamespace(schema_revision_id=12)
    family = StoredCandidateFamily(
        root_record=SimpleNamespace(logical_key_sha256=b"x" * 32),
        root_revision=None,
        family=None,
        entity_binding=None,
        root_values_by_field={},
        children=(
            StoredCandidateChild("rates", None, _outer()),
            StoredCandidateChild("details", None, _inner(code="B200")),
        ),
    )
    monkeypatch.setattr(runner_graph, "prepare_materialization_statement", AsyncMock())
    monkeypatch.setattr(runner_graph, "load_previous_families", AsyncMock(return_value=(family,)))
    with pytest.raises(CandidateRunnerError, match="retained family violates child membership"):
        await runner_graph.select_candidate_families(None, request, FamilyBuildResult((), (), ()), pointer)

    matching = StoredCandidateChild("details", None, _inner())
    family = StoredCandidateFamily(
        family.root_record,
        family.root_revision,
        family.family,
        family.entity_binding,
        family.root_values_by_field,
        (family.children[0], matching),
    )
    runner_graph.load_previous_families.return_value = (family,)
    assert await runner_graph.select_candidate_families(None, request, FamilyBuildResult((), (), ()), pointer) == (
        family,
    )

    invalid_base = StoredCandidateFamily(
        SimpleNamespace(logical_key_sha256=runner_graph.root_key_hash(definition, _root())),
        family.root_revision,
        family.family,
        family.entity_binding,
        family.root_values_by_field,
        (family.children[0], StoredCandidateChild("details", None, _inner(code="B200"))),
    )
    runner_graph.load_previous_families.return_value = (invalid_base,)
    replacement = assemble_root_families(definition, [_root()], {"rates": [_outer()], "details": [_inner()]})
    assert await runner_graph.select_candidate_families(None, request, replacement, pointer) == replacement.families
