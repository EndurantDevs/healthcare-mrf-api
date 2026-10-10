# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pure manual-command validation; native legacy correction proof is separate."""

from dataclasses import replace
from uuid import uuid4

import pytest

from process import registry_record_store as store


@pytest.mark.parametrize("kind", ["group", "company", "network", "provider", "location"])
@pytest.mark.parametrize("operation", ["create", "correct"])
def test_manual_name_write_validation(kind, operation):
    fields_by_name = {"display_name": "Réseau", "aliases": ["😀" * 512]}
    if kind == "group":
        fields_by_name["group_kind"] = "corporate_parent"
    elif kind == "company":
        fields_by_name["roles"] = ["employer"]
    elif kind == "provider":
        fields_by_name.update(provider_kind="organization", npi=None)
    elif kind == "location":
        fields_by_name["address_json"] = {
            "first_line": "123 Example Street",
            "second_line": None,
            "city": "Example",
            "state": "CA",
            "zip": "90210",
            "country": "US",
        }
    command_by_name = store.RegistryRecordCommand(
        kind,
        None if kind == "network" and operation == "create" else 7 if kind == "network" else uuid4(),
        operation,
        0 if operation == "create" else 1,
        fields_by_name,
        "Reviewed names",
        uuid4().hex,
        uuid4() if kind == "network" and operation == "create" else None,
    )
    assert store._validated_command(command_by_name)["aliases"] == fields_by_name["aliases"]
    invalid_characters = [chr(codepoint) for codepoint in [*range(32), *range(127, 160)]] + [
        "\ud800",
        "\udfff",
        "\ud800x\udfff",
    ]
    for character in invalid_characters:
        for field, field_value in [
            ("display_name", "Old" + character + "name"),
            ("aliases", ["Old" + character + "alias"]),
        ]:
            with pytest.raises(ValueError, match="registry_(display_name|alias)_invalid"):
                store._validated_command(replace(command_by_name, fields={**fields_by_name, field: field_value}))


def test_manual_name_unicode_limits():
    for maximum in [256, 512]:
        for character in ["a", "é", "😀", "\U0010ffff"]:
            assert store._bounded_name(character * maximum, maximum, "display_name") == character * maximum
            with pytest.raises(ValueError, match="display_name_invalid"):
                store._bounded_name(character * (maximum + 1), maximum, "display_name")
    assert store._bounded_name(" Réseau ", 512, "display_name") == "Réseau"
    for name in ["\tRéseau", "Réseau\n", "\u0085Réseau"]:
        with pytest.raises(ValueError, match="display_name_invalid"):
            store._bounded_name(name, 512, "display_name")


def test_legacy_management_read_compatibility():
    assert store._bounded_text("Legacy\nobservation", 512, "location_address") == "Legacy\nobservation"
    row_by_field = {"network_id": 7, "display_name": "Legacy\nname", "aliases": ["Legacy\u0085alias"], "revision": 1}
    assert store._snapshot(row_by_field) == row_by_field
    fields_by_name = {"display_name": "Corrected Réseau", "aliases": ["😀" * 512]}
    command_by_name = store.RegistryRecordCommand(
        "network", 7, "correct", 1, fields_by_name, "Reviewed labels", uuid4().hex
    )
    assert store._validated_command(command_by_name) == fields_by_name


def test_unsafe_undo_keeps_readable_history():
    from copy import deepcopy

    from process.registry_manual_undo import RegistryManualUndoCommand, _prepare_correction

    identity = uuid4()
    snapshot_by_field = {
        "group_id": str(identity),
        "group_kind": "corporate_parent",
        "display_name": "Legacy\nname",
        "aliases": ["Legacy\u0085alias"],
        "revision": 1,
        "archived": False,
    }
    history_by_field = {
        "record_json": snapshot_by_field,
        "request_sha256": "a" * 64,
        "custom_revision": 1,
        "current_revision": 2,
    }
    original = deepcopy(history_by_field)
    command = RegistryManualUndoCommand("group", identity, 2, 1, "Reviewed legacy undo", uuid4().hex)
    with pytest.raises(ValueError, match="display_name_invalid"):
        _prepare_correction(command, history_by_field, "group_id")
    assert history_by_field == original
    retained = store._history_result(
        {"record_kind": "group", "record_json": snapshot_by_field, "revision": 1, "custom_revision": 1}
    )
    assert retained["record"]["display_name"] == "Legacy\nname"
    clean_history = deepcopy(history_by_field)
    clean_history["record_json"].update(display_name="Corrected Réseau", aliases=["Corrected alias"])
    prepared = _prepare_correction(command, clean_history, "group_id")
    assert store._validated_command(prepared.command)["display_name"] == "Corrected Réseau"
    assert history_by_field == original
