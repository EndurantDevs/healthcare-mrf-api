# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""The whole-office review command accepts only its closed syntax."""

import pytest

from process.registry_ptg_office_review_contract import validated_office_review_command
from tests.test_registry_ptg_office_approval import _command


def test_reviewed_command_is_returned_unchanged():
    command_by_field = _command()
    assert validated_office_review_command(command_by_field) == command_by_field


@pytest.mark.parametrize(
    ("path", "value"),
    [
        ("operation", "review_another_capture"),
        ("office_evidence_kind", "inferred_office"),
        ("capture_id", "AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA"),
        ("canonical_input_sha256", "A" * 64),
        ("reason", " padded "),
        ("client_id", "system"),
        ("input_row_count", 0),
        ("retained_generation_id", True),
        ("source.source_scope.plan_market_type", "Group"),
        ("source.coordinates.source_system", "aca"),
        ("source.file_versions", []),
        ("source.file_versions", ["v" * 2048] * 128),
    ],
)
def test_command_outside_the_closed_syntax_is_refused(path, value):
    command_by_field = _command()
    *parents, name = path.split(".")
    target_by_field = command_by_field
    for parent in parents:
        target_by_field = target_by_field[parent]
    assert target_by_field[name] != value
    target_by_field[name] = value
    with pytest.raises(ValueError, match="registry_ptg_office_review_invalid"):
        validated_office_review_command(command_by_field)


def legacy_command():
    """Represent the exact historical record without interpreting its retired locator."""
    command = _command()
    command["retained_serving"].pop("address_relation")
    command["retained_serving"]["address_table_oid"] = 17
    return command


def test_known_legacy_command_projects_logical_fields_without_address_placeholder():
    command = legacy_command()
    normalized = validated_office_review_command(command)
    assert "address_table_oid" not in normalized["retained_serving"]
    assert "address_relation" not in normalized["retained_serving"]
    assert "retained_serving_sha256" not in normalized
    assert set(normalized["retained_serving"]) == {
        "generation_id",
        "candidate_id",
        "schema_name",
        "schema_revision",
        "source_generations",
        "approved_custom_revision",
        "manifest_sha256",
    }
    assert normalized["source"] == command["source"]
    assert validated_office_review_command(normalized) == normalized
    command["retained_serving"]["address_table_oid"] = None
    command["retained_serving_sha256"] = None
    assert validated_office_review_command(command) == normalized


@pytest.mark.parametrize("fault", ["unknown", "mixed", "wrong-generation", "address-shape", "logical-source"])
def test_named_and_historical_commands_refuse_unknown_or_changed_logical_scope(fault):
    command = legacy_command() if fault in {"unknown", "mixed", "logical-source"} else _command()
    match fault:
        case "unknown":
            command["retained_serving"]["extra"] = None
        case "mixed":
            command["retained_serving"]["address_relation"] = {"schema_name": "synthetic", "table_name": "addresses"}
        case "wrong-generation":
            command["retained_serving"]["generation_id"] += 1
        case "address-shape":
            command["retained_serving"]["address_relation"]["oid"] = 17
        case "logical-source":
            command["source"]["source_scope"]["scope_id"] = "00000000-0000-0000-0000-000000000001"
    with pytest.raises(ValueError):
        validated_office_review_command(command)
