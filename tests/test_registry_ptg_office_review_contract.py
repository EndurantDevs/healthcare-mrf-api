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
