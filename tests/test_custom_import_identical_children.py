# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Opted-in child collapse uses complete typed payloads and keeps strict defaults."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal

import pytest

from process.custom_import.definition import CustomImportDefinition, DefinitionError, canonical_json
from process.custom_import.family import _has_identical_values, assemble_root_families
from process.custom_import.runner_codec import record_payload
from process.custom_import.snowflake_preflight import SnowflakePreflightLimits, preflight_snowflake_bundle
from tests.test_custom_import_definition import _rate, _raw_definition, _root
from tests.test_custom_import_snowflake_preflight import _Adapter, _binding, _complete_rows, _connector
from tests.test_custom_import_snowflake_preflight import _definition as _preview_definition


def _definition():
    document = _raw_definition()
    document["streams"][1]["duplicate_policy"] = "collapse_identical"
    return CustomImportDefinition.from_mapping(document)


def test_optional_policy_changes_only_definition_identity_and_preserves_absent_bytes():
    document = _raw_definition()
    previous = CustomImportDefinition.from_mapping(document)
    assert previous.canonical == canonical_json(document)
    assert all(stream.duplicate_policy == "reject" for stream in previous.source_streams)
    document["revision"]["definition"] += 1
    document["streams"][1]["duplicate_policy"] = "collapse_identical"
    current = CustomImportDefinition.from_mapping(document, previous=previous)
    assert current.schema_digest == previous.schema_digest
    assert current.schema_revision == previous.schema_revision
    assert current.digest != previous.digest
    assert CustomImportDefinition.from_json(current.canonical) == current


@pytest.mark.parametrize("policy", [None, True, 1, [], {}, "last", "newest", ""])
def test_invalid_duplicate_policies_are_rejected(policy):
    document = _raw_definition()
    document["streams"][1]["duplicate_policy"] = policy
    with pytest.raises(DefinitionError, match="duplicate_policy"):
        CustomImportDefinition.from_mapping(document)


@pytest.mark.parametrize("policy", ["reject", "collapse_identical"])
def test_root_stream_cannot_declare_a_duplicate_policy(policy):
    document = _raw_definition()
    document["streams"][0]["duplicate_policy"] = policy
    with pytest.raises(DefinitionError, match="root streams cannot declare duplicate_policy"):
        CustomImportDefinition.from_mapping(document)


@pytest.mark.parametrize("explicit", [False, True])
def test_default_and_explicit_reject_keep_identical_duplicate_rejection(explicit):
    document = _raw_definition()
    if explicit:
        document["streams"][1]["duplicate_policy"] = "reject"
    definition = CustomImportDefinition.from_mapping(document)
    result = assemble_root_families(definition, [_root()], {"rates": [_rate(), _rate()]})
    assert not result.families
    assert [entry.code for entry in result.rejections] == ["duplicate_child_key"]


def test_identical_duplicates_retain_the_final_source_record_and_one_payload():
    first = _rate(amount=Decimal("12.50")) | {"unmapped": "first"}
    last = _rate(amount="12.500") | {"unmapped": "last"}
    definition = _definition()
    for records in ([first, last], [last, first]):
        result = assemble_root_families(definition, [_root()], {"rates": records})
        assert not result.rejections and not result.candidate_errors
        assert result.families[0].children["rates"] == (records[-1],)
        assert record_payload(definition.child_fields, result.families[0].children["rates"][0]) == record_payload(
            definition.child_fields, first
        )


@pytest.mark.parametrize("last_amount", ["13", None, "not-a-decimal"])
def test_conflicts_and_invalid_duplicates_still_reject_the_whole_family(last_amount):
    result = assemble_root_families(
        _definition(), [_root()], {"rates": [_rate(), _rate(amount=last_amount), _rate(code="other")]}
    )
    assert not result.families
    assert [entry.code for entry in result.rejections] == [
        "field_type_invalid" if last_amount == "not-a-decimal" else "duplicate_child_key"
    ]


def test_missing_null_and_unprojected_fields_remain_distinct():
    definition = _definition()
    missing = _rate()
    del missing["amount"]
    result = assemble_root_families(definition, [_root()], {"rates": [missing, _rate(amount=None)]})
    assert [entry.code for entry in result.rejections] == ["duplicate_child_key"]
    document = json.loads(definition.canonical)
    document["schema"]["children"][0]["fields"].append({"id": "note", "slot": 6, "type": "string", "nullable": True})
    definition = CustomImportDefinition.from_mapping(document)
    result = assemble_root_families(
        definition, [_root()], {"rates": [_rate() | {"note": "first"}, _rate() | {"note": "last"}]}
    )
    assert [entry.code for entry in result.rejections] == ["duplicate_child_key"]


def test_pure_equality_matches_retained_decimal_and_timestamp_payloads():
    document = _raw_definition()
    document["schema"]["children"][0]["fields"].append(
        {"id": "observed_at", "slot": 6, "type": "timestamp", "nullable": True}
    )
    definition = CustomImportDefinition.from_mapping(document)
    fields = definition.child_fields
    first = _rate(amount=Decimal("-0.000")) | {"observed_at": datetime(2030, 1, 1, tzinfo=UTC)}
    second = _rate(amount="0") | {"observed_at": datetime(2030, 1, 1, 1, tzinfo=timezone(timedelta(hours=1)))}
    assert _has_identical_values(fields, first, second)
    assert record_payload(fields, first) == record_payload(fields, second)
    assert not _has_identical_values(fields, first, second | {"observed_at": None})
    assert not _has_identical_values(fields, {"amount": None}, {})
    assert _has_identical_values(fields, {}, {})
    overflow = first | {"observed_at": datetime(1, 1, 1, tzinfo=timezone(timedelta(hours=1)))}
    assert not _has_identical_values(fields, overflow, overflow)


def test_policy_does_not_admit_duplicate_roots_or_orphans():
    definition = _definition()
    result = assemble_root_families(definition, [_root(), _root()], {"rates": [_rate(), _rate()]})
    assert [entry.code for entry in result.rejections] == ["duplicate_root_key"]
    orphan = assemble_root_families(definition, [_root()], {"rates": [_rate(npi="1003000126")]})
    assert orphan.candidate_errors == ("orphan_child",)


def test_preflight_collapses_sample_children_but_keeps_raw_observations_and_bounds():
    document = json.loads(_preview_definition().canonical)
    document["streams"][1]["duplicate_policy"] = "collapse_identical"
    definition = CustomImportDefinition.from_mapping(document)
    binding = _binding(definition)

    def rows(statement):
        records = _complete_rows(statement)
        return [*records[:-1], records[-2], records[-1]]

    for maximum, status in ((2, "complete"), (1, "unavailable")):
        result = preflight_snowflake_bundle(
            definition,
            binding,
            _connector(definition, binding),
            _Adapter(rows),
            limits=SnowflakePreflightLimits(maximum_root_keys=1, maximum_child_rows=maximum),
        )
        assert result.status == status
        assert result.observations[1].observed_rows == 2
        if result.sample is not None:
            assert len(result.sample.families[0].children["details"]) == 1
