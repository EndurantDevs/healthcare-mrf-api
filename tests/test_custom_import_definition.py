# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import copy
import json
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

import pytest

import process.custom_import.contracts as contracts
import process.custom_import.definition as custom_import_definition
from process.custom_import import (
    CandidateRejected,
    CustomImportDefinition,
    DefinitionError,
    SourceSnapshotError,
    assemble_root_families,
    canonical_sha256,
    load_json_definition,
    load_yaml_definition,
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


def _assert_invalid_definition(mutate, message):
    raw = _raw_definition()
    mutate(raw)

    with pytest.raises(DefinitionError, match=message):
        CustomImportDefinition.from_mapping(raw)


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


@pytest.mark.parametrize(
    ("serialized", "message"),
    (
        (b"\xff", "must be UTF-8"),
        (42, "must be text"),
        ('{"value": 1.5}', "floating-point"),
        ('{"contract":', "invalid JSON definition"),
    ),
)
def test_definition_wire_parser_rejects_noncanonical_input(serialized, message):
    with pytest.raises(DefinitionError, match=message):
        load_json_definition(serialized)

    with pytest.raises(DefinitionError, match="invalid YAML definition"):
        load_yaml_definition("definition: [")
    with pytest.raises(ValueError, match="unsupported custom-import digest domain"):
        canonical_sha256({}, domain="unsupported")


def test_definition_wire_parser_rejects_structural_and_encoding_boundaries():
    with pytest.raises(DefinitionError, match="duplicate object key"):
        load_yaml_definition("field: one\nfield: two\n")
    with pytest.raises(DefinitionError, match="structural limits"):
        load_json_definition("[" * 33 + "0" + "]" * 33)
    with pytest.raises(DefinitionError, match="byte limit"):
        load_json_definition(" " * (1024 * 1024 + 1))
    with pytest.raises(DefinitionError, match="object keys must be strings"):
        CustomImportDefinition.from_mapping({1: "not-a-definition"})
    with pytest.raises(DefinitionError, match="definition must be an object"):
        CustomImportDefinition.from_mapping([])


def test_definition_wire_parser_converts_recursion_failures_to_structural_limits(monkeypatch):
    def recursive_json(*_args, **_kwargs):
        raise RecursionError("synthetic parser recursion")

    monkeypatch.setattr(custom_import_definition.json, "loads", recursive_json)
    with pytest.raises(DefinitionError, match="structural limits"):
        load_json_definition("{}")

    monkeypatch.undo()
    deeply_nested_yaml = "[" * 512 + "0" + "]" * 512
    with pytest.raises(DefinitionError, match="structural limits"):
        load_yaml_definition(deeply_nested_yaml)


def test_contract_exports_and_definition_indexes_are_usable(definition):
    assert contracts.CustomImportDefinition is CustomImportDefinition
    assert contracts.assemble_root_families is assemble_root_families
    assert definition.fields_by_id["npi"].field_slot == 1
    assert definition.collections_by_name["rates"].child_key == ("service_code",)


@pytest.mark.parametrize(
    ("mutate", "message"),
    (
        (lambda raw: raw["schema"]["root"].update({"logical_key": []}), "non-empty unique"),
        (lambda raw: raw["schema"]["root"]["entity"].update({"adapter": "other"}), "must be npi"),
        (lambda raw: raw["schema"]["root"]["entity"].update({"field": "missing"}), "NPI entity"),
        (lambda raw: raw["schema"]["root"]["fields"][1].update({"nullable": "false"}), "nullable"),
        (lambda raw: raw["schema"]["root"]["fields"][1].update({"slot": 1}), "stable field slots"),
        (lambda raw: raw["schema"]["root"]["fields"][1].update({"projection_slot": 1}), "projection slots"),
    ),
)
def test_definition_rejects_root_schema_identity_violations(mutate, message):
    _assert_invalid_definition(mutate, message)


@pytest.mark.parametrize(
    ("mutate", "message"),
    (
        (lambda raw: raw["schema"]["root"]["fields"][0].update({"nullable": True}), "required root"),
        (
            lambda raw: raw["schema"].update(
                {"children": [copy.deepcopy(raw["schema"]["children"][0]) for _ in range(9)]}
            ),
            "exceeds the v1 limit",
        ),
        (
            lambda raw: raw["schema"].update(
                {"children": [raw["schema"]["children"][0], copy.deepcopy(raw["schema"]["children"][0])]}
            ),
            "collection names must be unique",
        ),
        (lambda raw: raw.update({"streams": {}}), "streams must be an array"),
        (lambda raw: raw["streams"][1].update({"id": "providers"}), "streams must be non-empty"),
        (lambda raw: raw["aliases"].update({"providers": {"\x00": "npi"}}), "bounded printable"),
    ),
)
def test_definition_rejects_schema_and_stream_shape_boundaries(mutate, message):
    _assert_invalid_definition(mutate, message)


@pytest.mark.parametrize(
    ("mutate", "message"),
    (
        (
            lambda raw: raw["schema"]["children"][0]["parent_key"][0].update({"root": "display_name"}),
            "preserve root logical-key order",
        ),
        (
            lambda raw: raw["schema"]["children"][0]["parent_key"][0].update({"child": "amount"}),
            "required and type-compatible",
        ),
        (lambda raw: raw["schema"]["children"][0].update({"parent_key": []}), "complete root logical key"),
        (lambda raw: raw["schema"]["children"][0].update({"child_key": []}), "one to three unique"),
        (lambda raw: raw["schema"]["children"][0].update({"child_key": ["amount"]}), "required child"),
    ),
)
def test_definition_rejects_incomplete_child_identity_mappings(mutate, message):
    _assert_invalid_definition(mutate, message)


@pytest.mark.parametrize(
    ("mutate", "message"),
    (
        (lambda raw: raw["streams"][0].update({"child": "rates"}), "cannot declare child"),
        (lambda raw: raw["streams"][1].update({"child": "missing"}), "is not declared"),
        (lambda raw: raw["streams"][0].update({"format": "binary"}), "unsupported format"),
        (lambda raw: raw["aliases"].update({"providers": []}), "must be an object"),
        (lambda raw: raw["aliases"]["providers"].update({"Amount": "amount"}), "stream scope"),
        (lambda raw: raw["query"].update({"root_fields": ["missing"]}), "unique projected root"),
        (lambda raw: raw["query"]["child"].update({"collection": "missing"}), "is not declared"),
        (lambda raw: raw["query"]["child"].update({"fields": ["rate_npi"]}), "one collection"),
        (
            lambda raw: raw["query"]["order"][0].update({"field": "rate_npi"}),
            "permitted query fields",
        ),
        (
            lambda raw: raw["query"].update({"aliases": {"metric": "rate_npi"}}),
            "target permitted query fields",
        ),
        (
            lambda raw: raw["query"].update({"aliases": {"npi": "amount"}}),
            "collide with canonical field ids",
        ),
        (
            lambda raw: raw["query"].update({"sortable_fields": ["amount", "amount"]}),
            "unique permitted query fields",
        ),
        (
            lambda raw: raw["query"].update({"sortable_fields": ["rate_npi"]}),
            "unique permitted query fields",
        ),
        (
            lambda raw: raw["selection_profiles"][0].update({"context_dimensions": ["service_code", "service_code"]}),
            "at most two unique",
        ),
        (
            lambda raw: raw["selection_profiles"][0]["selection"][0].update({"field": "rate_npi"}),
            "one permitted query context",
        ),
    ),
)
def test_definition_rejects_out_of_scope_query_and_selection_values(mutate, message):
    _assert_invalid_definition(mutate, message)


@pytest.mark.parametrize(
    ("mutate", "message"),
    (
        (
            lambda raw: raw["selection_profiles"].append(copy.deepcopy(raw["selection_profiles"][0])),
            "profile ids must be unique",
        ),
        (lambda raw: raw["query"].update({"order": raw["query"]["order"] * 4}), "term limit"),
        (
            lambda raw: raw["query"]["order"].append(copy.deepcopy(raw["query"]["order"][0])),
            "cannot repeat a field",
        ),
    ),
)
def test_definition_rejects_ambiguous_profiles_and_sort_terms(mutate, message):
    _assert_invalid_definition(mutate, message)


def test_definition_permits_root_only_query_contexts():
    raw = _raw_definition()
    raw["query"].pop("child")
    raw["query"]["order"] = []
    raw["selection_profiles"][0]["selection"][0]["field"] = "npi"
    raw["selection_profiles"][0]["context_dimensions"] = []

    definition = CustomImportDefinition.from_mapping(raw)
    assert definition.query.child_collection is None
    assert definition.query.child_fields == ()


def test_definition_declares_separate_query_aliases_and_sortable_fields():
    raw = _raw_definition()
    raw["query"].update(
        {
            "aliases": {"metric": "amount", "provider": "display_name"},
            "sortable_fields": ["amount", "display_name"],
        }
    )

    definition = CustomImportDefinition.from_mapping(raw)

    assert definition.query.resolve_field_id("amount") == "amount"
    assert definition.query.resolve_field_id("metric") == "amount"
    assert definition.query.resolve_field_id("missing") is None
    assert definition.query.sortable_fields == ("amount", "display_name")


def test_query_contract_constructor_keeps_legacy_defaults():
    query = custom_import_definition.QueryContract(("npi",), None, (), ())

    assert query.aliases == ()
    assert query.sortable_fields == ()


def test_alias_only_revision_keeps_schema_and_stable_slots():
    first = CustomImportDefinition.from_mapping(_raw_definition())
    second_raw = _raw_definition()
    second_raw["revision"]["definition"] = 2
    second_raw["aliases"]["providers"]["Provider Identifier"] = "npi"
    second_raw["query"]["aliases"] = {"provider": "display_name"}
    second_raw["query"]["sortable_fields"] = ["display_name"]
    second = CustomImportDefinition.from_mapping(second_raw, previous=first)

    assert second.schema_revision == first.schema_revision
    assert second.schema_digest == first.schema_digest
    assert {field.field_slot for field in second.fields} == {1, 2, 3, 4, 5}


@pytest.mark.parametrize(
    "change", (lambda aliases: aliases.pop("metric"), lambda aliases: aliases.update(metric="display_name"))
)
def test_published_query_alias_cannot_be_removed_or_rebound(change):
    first_raw = _raw_definition()
    first_raw["query"]["aliases"] = {"metric": "amount"}
    first = CustomImportDefinition.from_mapping(first_raw)
    second_raw = copy.deepcopy(first_raw)
    second_raw["revision"]["definition"] = 2
    change(second_raw["query"]["aliases"])

    with pytest.raises(DefinitionError, match="cannot be removed or rebound"):
        CustomImportDefinition.from_mapping(second_raw, previous=first)


def test_definition_revision_cannot_move_an_existing_field_to_a_new_slot():
    first = CustomImportDefinition.from_mapping(_raw_definition())
    moved = _raw_definition()
    moved["revision"].update({"definition": 2, "schema": 2})
    moved["schema"]["root"]["fields"][1]["slot"] = 6

    with pytest.raises(DefinitionError, match="stable field slots cannot be rebound"):
        CustomImportDefinition.from_mapping(moved, previous=first)


def test_definition_revision_cannot_rebind_a_retained_field_slot():
    first = CustomImportDefinition.from_mapping(_raw_definition())
    rebound = _raw_definition()
    rebound["revision"].update({"definition": 2, "schema": 2})
    rebound["schema"]["root"]["fields"][1]["id"] = "provider_name"
    rebound["aliases"]["providers"]["Provider Name"] = "provider_name"
    rebound["query"]["root_fields"][1] = "provider_name"

    with pytest.raises(DefinitionError, match="stable field slots cannot be rebound"):
        CustomImportDefinition.from_mapping(rebound, previous=first)


def test_definition_revision_requires_monotonic_definition_and_schema_numbers():
    first = CustomImportDefinition.from_mapping(_raw_definition())

    with pytest.raises(DefinitionError, match="definition revision must increase"):
        CustomImportDefinition.from_mapping(_raw_definition(), previous=first)

    unchanged_schema = _raw_definition()
    unchanged_schema["revision"].update({"definition": 2, "schema": 2})
    with pytest.raises(DefinitionError, match="unchanged schema must retain"):
        CustomImportDefinition.from_mapping(unchanged_schema, previous=first)


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


def test_source_tokens_reject_empty_stream_observations(definition):
    with pytest.raises(SourceSnapshotError, match="no bounded token observations"):
        validate_source_snapshot_tokens(
            definition,
            {"providers": [], "rates": ["snapshot-1"]},
        )


def test_family_build_requires_a_child_collection_mapping(definition):
    with pytest.raises(DefinitionError, match="children must contain one bounded array"):
        assemble_root_families(definition, [], [])


def test_family_build_requires_bounded_root_and_child_record_arrays(definition):
    with pytest.raises(DefinitionError, match="roots must be a bounded record array"):
        assemble_root_families(definition, {"root": _root()}, {"rates": []})
    with pytest.raises(DefinitionError, match="children must contain one bounded array"):
        assemble_root_families(definition, [_root()], {"rates": {"rate": _rate()}})


def test_family_build_rejects_non_object_records_and_invalid_key_shapes(definition):
    root_result = assemble_root_families(definition, [None], {"rates": []})
    assert root_result.candidate_errors == ("root_not_object",)

    child_result = assemble_root_families(definition, [_root()], {"rates": [None]})
    assert child_result.candidate_errors == ("child_not_object",)

    orphan_result = assemble_root_families(
        definition,
        [_root()],
        {"rates": [{"service_code": "A100", "amount": Decimal("12.50")}]},
    )
    assert orphan_result.candidate_errors == ("orphan_child",)


def test_family_build_rejects_null_invalid_entity_and_decimal_values(definition):
    required_null = assemble_root_families(
        definition,
        [{"npi": "1234567893", "display_name": None}],
        {"rates": []},
    )
    assert {entry.code for entry in required_null.rejections} == {"required_field_null"}

    invalid_entity = assemble_root_families(definition, [_root(npi="1234567894")], {"rates": []})
    assert {entry.code for entry in invalid_entity.rejections} == {"entity_binding_invalid"}

    malformed_entity = assemble_root_families(definition, [_root(npi="not-an-npi")], {"rates": []})
    assert {entry.code for entry in malformed_entity.rejections} == {"entity_binding_invalid"}

    invalid_decimal = assemble_root_families(definition, [_root()], {"rates": [_rate(amount=True)]})
    assert {entry.code for entry in invalid_decimal.rejections} == {"field_type_invalid"}

    nullable_decimal = assemble_root_families(definition, [_root()], {"rates": [_rate(amount=None)]})
    assert nullable_decimal.families


def test_family_build_applies_each_declared_scalar_type():
    raw = _raw_definition()
    raw["schema"]["root"]["fields"].extend(
        (
            {"id": "rank", "slot": 6, "type": "integer", "nullable": False},
            {"id": "is_active", "slot": 7, "type": "boolean", "nullable": False},
            {"id": "born_on", "slot": 8, "type": "date", "nullable": False},
            {"id": "seen_at", "slot": 9, "type": "timestamp", "nullable": False},
        )
    )
    definition = CustomImportDefinition.from_mapping(raw)
    root_values_by_field = {
        **_root(),
        "rank": 1,
        "is_active": True,
        "born_on": date(2026, 9, 15),
        "seen_at": datetime(2026, 9, 15, tzinfo=timezone.utc),
    }

    assert assemble_root_families(definition, [root_values_by_field], {"rates": []}).families
    for field_id, invalid_value in (
        ("rank", True),
        ("is_active", 1),
        ("born_on", datetime(2026, 9, 15, tzinfo=timezone.utc)),
        ("seen_at", datetime(2026, 9, 15)),
    ):
        invalid_root_values_by_field = {**root_values_by_field, field_id: invalid_value}
        result = assemble_root_families(definition, [invalid_root_values_by_field], {"rates": []})
        assert {entry.code for entry in result.rejections} == {"field_type_invalid"}


def _definition_with_projected_root_decimal() -> CustomImportDefinition:
    definition_document = copy.deepcopy(_raw_definition())
    root_fields = definition_document["schema"]["root"]["fields"]
    assert isinstance(root_fields, list)
    root_fields.append({"id": "root_amount", "slot": 6, "type": "decimal", "nullable": False, "projection_slot": 5})
    return CustomImportDefinition.from_mapping(definition_document)


def _definition_with_projected_root_integer() -> CustomImportDefinition:
    definition_document = copy.deepcopy(_raw_definition())
    root_fields = definition_document["schema"]["root"]["fields"]
    assert isinstance(root_fields, list)
    root_fields.append({"id": "root_rank", "slot": 6, "type": "integer", "nullable": False, "projection_slot": 5})
    return CustomImportDefinition.from_mapping(definition_document)


@pytest.mark.parametrize(
    "display_name",
    ("Synthetic\x00Provider", "😺" * 513),
    ids=("nul", "utf8-byte-limit"),
)
def test_family_build_rejects_root_projected_string_storage_shapes(definition, display_name):
    result = assemble_root_families(definition, [_root(name=display_name)], {"rates": []})

    assert result.families == ()
    assert {entry.code for entry in result.rejections} == {"field_storage_invalid"}


@pytest.mark.parametrize(
    "service_code",
    ("A\x000", "😺" * 513),
    ids=("nul", "utf8-byte-limit"),
)
def test_family_build_rejects_child_projected_string_storage_shapes(definition, service_code):
    result = assemble_root_families(definition, [_root()], {"rates": [_rate(code=service_code)]})

    assert result.families == ()
    assert {entry.code for entry in result.rejections} == {"field_storage_invalid"}


@pytest.mark.parametrize("rank", (-(2**63) - 1, 2**63))
def test_family_build_rejects_projected_integers_outside_bigint(rank):
    definition = _definition_with_projected_root_integer()
    result = assemble_root_families(definition, [{**_root(), "root_rank": rank}], {"rates": []})

    assert result.families == ()
    assert {entry.code for entry in result.rejections} == {"field_storage_invalid"}


@pytest.mark.parametrize("rank", (-(2**63), 2**63 - 1))
def test_family_build_accepts_projected_bigint_boundaries(rank):
    definition = _definition_with_projected_root_integer()
    result = assemble_root_families(definition, [{**_root(), "root_rank": rank}], {"rates": []})

    assert len(result.families) == 1
    assert result.rejections == ()


@pytest.mark.parametrize(
    "amount",
    (Decimal("1000000000000000000"), "0.0000000000001"),
    ids=("too-many-integer-digits", "too-many-fractional-digits"),
)
def test_family_build_rejects_root_projected_decimal_storage_shapes(amount):
    definition = _definition_with_projected_root_decimal()
    result = assemble_root_families(definition, [{**_root(), "root_amount": amount}], {"rates": []})

    assert result.families == ()
    assert {entry.code for entry in result.rejections} == {"field_storage_invalid"}


@pytest.mark.parametrize(
    "amount",
    (Decimal("1000000000000000000"), "0.0000000000001"),
    ids=("too-many-integer-digits", "too-many-fractional-digits"),
)
def test_family_build_rejects_child_projected_decimal_storage_shapes(definition, amount):
    result = assemble_root_families(definition, [_root()], {"rates": [_rate(amount=amount)]})

    assert result.families == ()
    assert {entry.code for entry in result.rejections} == {"field_storage_invalid"}


def test_family_build_accepts_fitting_normalized_decimal_source_text():
    definition = _definition_with_projected_root_decimal()
    result = assemble_root_families(
        definition,
        [{**_root(), "root_amount": "4.0000000000000"}],
        {"rates": [_rate(amount="4.0000000000000")]},
    )

    assert len(result.families) == 1
    assert result.rejections == ()


def test_family_build_accepts_a_long_insignificant_decimal_zero_suffix():
    definition = _definition_with_projected_root_decimal()
    amount = "4." + ("0" * 50_000)
    result = assemble_root_families(definition, [{**_root(), "root_amount": amount}], {"rates": []})

    assert len(result.families) == 1
    assert result.rejections == ()


@pytest.mark.parametrize(
    "amount",
    (" 4", "4 ", "1_000", "٤", "4e0", "4E0"),
    ids=("leading-space", "trailing-space", "underscore", "non-ascii-digit", "lower-exponent", "upper-exponent"),
)
def test_family_build_rejects_noncanonical_decimal_source_text(definition, amount):
    result = assemble_root_families(definition, [_root()], {"rates": [_rate(amount=amount)]})

    assert result.families == ()
    assert {entry.code for entry in result.rejections} == {"field_type_invalid"}


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


def test_merge_families_rejects_unsupported_refresh_modes(definition):
    candidate = assemble_root_families(definition, [_root()], {"rates": [_rate()]})

    with pytest.raises(DefinitionError, match="refresh mode must be upsert or snapshot"):
        merge_families({}, candidate, refresh_mode="replace")


def test_definition_records_are_not_mutated_by_canonicalization():
    raw = _raw_definition()
    original = json.dumps(raw, sort_keys=True)
    CustomImportDefinition.from_mapping(raw)
    assert json.dumps(raw, sort_keys=True) == original
