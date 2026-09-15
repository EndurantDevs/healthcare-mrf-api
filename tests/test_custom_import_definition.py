# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import copy
import json
from decimal import Decimal
from pathlib import Path

import pytest

from process.custom_import import (
    CandidateRejected,
    CustomImportDefinition,
    DefinitionError,
    SourceSnapshotError,
    assemble_root_families,
    load_json_definition,
    merge_families,
    validate_source_snapshot_tokens,
)

FIXTURES = Path(__file__).with_name("fixtures") / "custom_import"


@pytest.fixture
def definition():
    return CustomImportDefinition.from_json((FIXTURES / "v1_valid.json").read_text())


def _raw_definition():
    return load_json_definition((FIXTURES / "v1_valid.json").read_text())


def _root(npi="1234567893", name="Synthetic Provider"):
    return {"npi": npi, "display_name": name}


def _rate(npi="1234567893", code="A100", amount=Decimal("12.50")):
    return {"rate_npi": npi, "service_code": code, "amount": amount}


def test_json_and_yaml_have_one_canonical_definition_and_digest():
    json_definition = CustomImportDefinition.from_json((FIXTURES / "v1_valid.json").read_text())
    yaml_definition = CustomImportDefinition.from_yaml((FIXTURES / "v1_valid.yaml").read_text())

    assert json_definition.canonical == yaml_definition.canonical
    assert json_definition.digest == yaml_definition.digest
    assert json_definition.schema_digest == yaml_definition.schema_digest
    assert [alias.source_label for alias in json_definition.aliases] == [
        "Provider ID",
        "Provider Name",
        "Amount",
        "Provider ID",
        "Service Code",
    ]


@pytest.mark.parametrize(
    "serialized",
    [
        '{"contract":"custom-import/v1","contract":"custom-import/v1"}',
        '{"contract":"custom-import/v1","unexpected":true}',
    ],
)
def test_definition_rejects_duplicate_and_unknown_json_keys(serialized):
    with pytest.raises(DefinitionError):
        CustomImportDefinition.from_json(serialized)


def test_definition_rejects_yaml_aliases_and_unknown_nested_keys():
    with pytest.raises(DefinitionError, match="aliases"):
        CustomImportDefinition.from_yaml("x: &value {a: 1}\ny: *value\n")

    raw = _raw_definition()
    raw["schema"]["root"]["surprise"] = True
    with pytest.raises(DefinitionError, match="unknown key"):
        CustomImportDefinition.from_mapping(raw)


@pytest.mark.parametrize(
    "mutate",
    [
        lambda raw: raw.update({"refresh_mode": []}),
        lambda raw: raw["schema"]["root"]["fields"][0].update({"type": []}),
        lambda raw: raw["query"]["order"][0].update({"direction": []}),
        lambda raw: raw.update({"refresh_mode": ("upsert",)}),
    ],
)
def test_definition_mapping_rejects_non_scalar_enums_without_type_errors(mutate):
    raw = _raw_definition()
    mutate(raw)

    with pytest.raises(DefinitionError):
        CustomImportDefinition.from_mapping(raw)


@pytest.mark.parametrize("revision_name", ("definition", "schema"))
def test_definition_rejects_revision_numbers_outside_integer_storage(revision_name):
    raw = _raw_definition()
    raw["revision"][revision_name] = 2_147_483_648

    with pytest.raises(DefinitionError, match="integer <= 2147483647"):
        CustomImportDefinition.from_mapping(raw)


def test_alias_only_revision_keeps_schema_and_stable_slots():
    first = CustomImportDefinition.from_mapping(_raw_definition())
    second_raw = _raw_definition()
    second_raw["revision"]["definition"] = 2
    second_raw["aliases"]["providers"]["Provider Identifier"] = "npi"
    second = CustomImportDefinition.from_mapping(second_raw, previous=first)

    assert second.schema_revision == first.schema_revision
    assert second.schema_digest == first.schema_digest
    assert {field.field_slot for field in second.fields} == {1, 2, 3, 4, 5}


def test_type_or_key_change_requires_new_schema_revision():
    first = CustomImportDefinition.from_mapping(_raw_definition())
    changed = _raw_definition()
    changed["revision"]["definition"] = 2
    changed["schema"]["root"]["fields"][1]["type"] = "integer"

    with pytest.raises(DefinitionError, match="new schema revision"):
        CustomImportDefinition.from_mapping(changed, previous=first)

    changed["revision"]["schema"] = 2
    second = CustomImportDefinition.from_mapping(changed, previous=first)
    assert second.schema_revision == 2


def test_definition_enforces_profile_and_projection_limits():
    raw = _raw_definition()
    raw["schema"]["root"]["fields"].extend(
        {
            "id": f"field_{number}",
            "slot": number,
            "type": "string",
            "nullable": True,
            "projection_slot": number - 1,
        }
        for number in range(6, 23)
    )
    with pytest.raises(DefinitionError, match="projection_slot"):
        CustomImportDefinition.from_mapping(raw)

    raw = _raw_definition()
    raw["selection_profiles"] = [copy.deepcopy(raw["selection_profiles"][0]) for _ in range(5)]
    for number, profile in enumerate(raw["selection_profiles"]):
        profile["id"] = f"profile_{number}"
    with pytest.raises(DefinitionError, match="profile count"):
        CustomImportDefinition.from_mapping(raw)


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (
            lambda raw: raw["streams"][0].update({"kind": "unsupported"}),
            "kind must be root or child",
        ),
        (
            lambda raw: raw.update({"streams": raw["streams"][:1]}),
            "exactly one root stream",
        ),
    ],
)
def test_definition_rejects_invalid_streams_and_incomplete_child_coverage(mutate, message):
    """Stream-local validation and whole-definition coverage remain distinct."""

    raw = _raw_definition()
    mutate(raw)

    with pytest.raises(DefinitionError, match=message):
        CustomImportDefinition.from_mapping(raw)


def test_xml_stream_requires_a_bounded_record_path():
    raw = _raw_definition()
    root_stream = raw["streams"][0]
    root_stream.update({"format": "xml", "record_path": "r" * 63})

    definition = CustomImportDefinition.from_mapping(raw)
    assert definition.source_streams[0].record_path == "r" * 63

    raw["streams"][0].pop("record_path")
    with pytest.raises(DefinitionError, match="record_path is required"):
        CustomImportDefinition.from_mapping(raw)

    raw = _raw_definition()
    raw["streams"][0]["record_path"] = "provider"
    with pytest.raises(DefinitionError, match="only valid for XML"):
        CustomImportDefinition.from_mapping(raw)

    raw = _raw_definition()
    raw["streams"][0].update({"format": "xml", "record_path": "r" * 64})
    with pytest.raises(DefinitionError, match="record_path must be lower_snake_case"):
        CustomImportDefinition.from_mapping(raw)


def test_source_tokens_must_be_complete_single_and_shared(definition):
    assert (
        validate_source_snapshot_tokens(definition, {"providers": ["snapshot-1"], "rates": ("snapshot-1",)})
        == "snapshot-1"
    )
    for tokens in (
        ["providers", "rates"],
        {"providers": ["snapshot-1"]},
        {"providers": ["snapshot-1", "snapshot-2"], "rates": ["snapshot-1"]},
        {"providers": [None], "rates": ["snapshot-1"]},
        {"providers": [["snapshot-1"]], "rates": ["snapshot-1"]},
        {"providers": [1], "rates": ["snapshot-1"]},
        {"providers": ["snapshot-1"], "rates": ["snapshot-2"]},
    ):
        with pytest.raises(SourceSnapshotError):
            validate_source_snapshot_tokens(definition, tokens)


def test_family_build_requires_a_child_collection_mapping(definition):
    with pytest.raises(DefinitionError, match="children must contain one bounded array"):
        assemble_root_families(definition, [], [])


def test_invalid_children_and_duplicate_keys_reject_only_their_root_family(definition):
    invalid_child = _rate()
    invalid_child.pop("service_code")
    result = assemble_root_families(definition, [_root()], {"rates": [invalid_child]})
    assert result.families == ()
    assert {item.code for item in result.rejections} == {"required_field_missing"}

    duplicate = assemble_root_families(definition, [_root(), _root()], {"rates": [_rate(), _rate()]})
    assert duplicate.families == ()
    assert {item.code for item in duplicate.rejections} == {
        "duplicate_root_key",
        "duplicate_child_key",
    }


def test_missing_nullable_field_does_not_hide_a_later_required_field():
    raw = _raw_definition()
    child_fields = raw["schema"]["children"][0]["fields"]
    raw["schema"]["children"][0]["fields"] = [
        child_fields[0],
        child_fields[2],
        child_fields[1],
    ]
    definition = CustomImportDefinition.from_mapping(raw)

    result = assemble_root_families(
        definition,
        [_root()],
        {"rates": [{"rate_npi": "1234567893"}]},
    )

    assert result.families == ()
    assert {entry.code for entry in result.rejections} == {"required_field_missing"}


def test_orphan_child_rejects_the_whole_candidate(definition):
    result = assemble_root_families(definition, [_root()], {"rates": [_rate(npi="1003000126")]})
    assert result.candidate_errors == ("orphan_child",)
    with pytest.raises(CandidateRejected, match="orphan_child"):
        merge_families({}, result, refresh_mode="upsert")


def test_root_without_a_logical_key_rejects_the_whole_candidate(definition):
    result = assemble_root_families(
        definition,
        [{"display_name": "Synthetic Provider"}],
        {"rates": []},
    )

    assert result.candidate_errors == ("root_key_missing",)
    with pytest.raises(CandidateRejected, match="root_key_missing"):
        merge_families({}, result, refresh_mode="snapshot", complete_scope=True)


def test_non_hashable_key_values_are_rejected_without_raising(definition):
    root_result = assemble_root_families(
        definition,
        [{"npi": ["1234567893"], "display_name": "Synthetic Provider"}],
        {"rates": []},
    )
    assert root_result.candidate_errors == ("root_key_missing",)

    child_result = assemble_root_families(
        definition,
        [_root()],
        {
            "rates": [
                {
                    "rate_npi": ["1234567893"],
                    "service_code": "A100",
                    "amount": Decimal("12.50"),
                }
            ]
        },
    )
    assert child_result.candidate_errors == ("orphan_child",)


def test_upsert_retains_rejected_and_absent_families_but_snapshot_requires_scope(
    definition,
):
    first = assemble_root_families(definition, [_root()], {"rates": [_rate()]})
    previous_families_by_key = {family.root_key: family for family in first.families}
    rejected_child = _rate()
    rejected_child["amount"] = object()
    candidate = assemble_root_families(definition, [_root()], {"rates": [rejected_child]})

    assert merge_families(previous_families_by_key, candidate, refresh_mode="upsert") == previous_families_by_key
    assert (
        merge_families(
            previous_families_by_key,
            candidate,
            refresh_mode="snapshot",
            complete_scope=True,
        )
        == previous_families_by_key
    )
    empty = assemble_root_families(definition, [], {"rates": []})
    with pytest.raises(CandidateRejected, match="snapshot_scope_incomplete"):
        merge_families(previous_families_by_key, empty, refresh_mode="snapshot")
    assert (
        merge_families(
            previous_families_by_key,
            empty,
            refresh_mode="snapshot",
            complete_scope=True,
        )
        == {}
    )


def test_definition_records_are_not_mutated_by_canonicalization():
    raw = _raw_definition()
    original = json.dumps(raw, sort_keys=True)
    CustomImportDefinition.from_mapping(raw)
    assert json.dumps(raw, sort_keys=True) == original
