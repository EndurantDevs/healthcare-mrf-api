# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded manual site classification uses actual compatible native address rules."""

import json
from dataclasses import FrozenInstanceError, replace
from uuid import uuid4

import pytest

from process import registry_record_store as store
from process.registry_record_store import RegistryRecordCommand, prepare_registry_location_commands


def _command(**changes):
    return replace(
        RegistryRecordCommand(
            "location",
            uuid4(),
            "create",
            0,
            {
                "display_name": "Example Site",
                "aliases": [],
                "address_json": {
                    "first_line": "123 Example Street",
                    "second_line": "Suite 2",
                    "city": "Sample City",
                    "state": "CA",
                    "zip": "90210",
                    "country": "US",
                },
            },
            "Reviewed explicit site",
            uuid4().hex,
        ),
        **changes,
    )


def _with_address(command, **changes):
    return replace(
        command,
        fields={
            **command.fields,
            "address_json": {**command.fields["address_json"], **changes},
        },
    )


def _known(command):
    prepared = prepare_registry_location_commands([command])[0]
    return {
        "location_id": str(command.record_id),
        **json.loads(prepared.fields_json),
        "archived": False,
        "revision": 7,
        "created_at": "2026-01-01T00:00:00+00:00",
    }


@pytest.fixture
def native_calls(monkeypatch):
    native = store._fast_module()
    if native is None:
        pytest.skip("compatible native canonicalizer is required in the native CI lane")
    calls = []

    class TrackedNative:
        def canonicalize_batch(self, rows):
            calls.append(rows)
            return native.canonicalize_batch(rows)

    monkeypatch.setattr(store, "_fast_module", lambda: TrackedNative())
    return calls


def test_one_native_batch_units_and_two_explicit_sites_without_automatic_binding(native_calls):
    first, second, third = _command(), _command(), _with_address(_command(), second_line="Suite 3")
    outcomes = prepare_registry_location_commands([first, second, third])
    assert len(native_calls) == 1 and len(native_calls[0]) == 3
    assert [outcome.status for outcome in outcomes] == ["ambiguous_address", "ambiguous_address", "create"]
    assert [outcome.location_id for outcome in outcomes] == [first.record_id, second.record_id, third.record_id]
    assert outcomes[0].matching_location_ids == (second.record_id,)
    assert outcomes[1].matching_location_ids == (first.record_id,)
    assert outcomes[2].matching_location_ids == ()


def test_explicit_uuid_classification_and_known_ledger_are_one_native_batch(native_calls):
    command = _command()
    known = _known(command)
    native_calls.clear()
    other, corrected = _command(), replace(command, operation="correct", expected_revision=7)
    corrected = replace(corrected, fields={**corrected.fields, "display_name": "Corrected Site"})
    outcomes = prepare_registry_location_commands([command, other], known_locations={command.record_id: known})
    assert len(native_calls) == 1 and len(native_calls[0]) == 3
    assert [outcome.status for outcome in outcomes] == ["known_uuid", "ambiguous_address"]
    assert (
        prepare_registry_location_commands([corrected], known_locations={command.record_id: known})[0].status
        == "known_uuid"
    )
    changed_create = replace(command, fields={**command.fields, "display_name": "Different Site"})
    assert (
        prepare_registry_location_commands([changed_create], known_locations={command.record_id: known})[0].status
        == "conflicting_identity"
    )
    stale = replace(corrected, expected_revision=6)
    assert (
        prepare_registry_location_commands([stale], known_locations={command.record_id: known})[0].status
        == "conflicting_identity"
    )
    stale_unchanged = replace(command, operation="correct", expected_revision=6)
    assert (
        prepare_registry_location_commands([stale_unchanged], known_locations={command.record_id: known})[0].status
        == "conflicting_identity"
    )
    assert (
        prepare_registry_location_commands([replace(other, operation="correct", expected_revision=1)])[0].status
        == "conflicting_identity"
    )


def test_requested_exact_source_and_mismatched_manual_bindings_never_overwrite(native_calls):
    command = _command()
    identity_key = json.loads(prepare_registry_location_commands([command])[0].fields_json)["canonical_address_json"][
        "identity_key"
    ]
    pair_by_field = {"provider_system": "manual", "provider_id": str(uuid4()), "location_id": command.record_id}
    source_binding_by_field = {**pair_by_field, "owner": "source", "identity_key": identity_key}
    manual_binding_by_field = {**pair_by_field, "owner": "manual", "identity_key": identity_key}
    assert prepare_registry_location_commands([command], bindings=[source_binding_by_field])[0].status == "create"
    assert (
        prepare_registry_location_commands(
            [command], bindings=[source_binding_by_field], requested_bindings=[pair_by_field]
        )[0].status
        == "conflicting_binding"
    )
    assert (
        prepare_registry_location_commands(
            [command], bindings=[manual_binding_by_field], requested_bindings=[pair_by_field]
        )[0].status
        == "create"
    )
    assert (
        prepare_registry_location_commands(
            [command],
            bindings=[{**manual_binding_by_field, "identity_key": "different"}],
            requested_bindings=[pair_by_field],
        )[0].status
        == "conflicting_binding"
    )


def test_repeated_preparation_is_frozen_and_singleton_facade_uses_same_rules(native_calls):
    command = _command()
    first = prepare_registry_location_commands([command])
    assert prepare_registry_location_commands([command]) == first
    expected = json.loads(first[0].fields_json)["canonical_address_json"]
    assert store._canonical_location_address(command) == expected
    with pytest.raises(FrozenInstanceError):
        first[0].status = "other"
    command.fields["aliases"].append("Changed later")
    assert json.loads(first[0].fields_json)["aliases"] == []


@pytest.mark.parametrize(
    "changes",
    [
        {"first_line": ""},
        {"city": ""},
        {"state": "Unknown"},
        {"zip": "ab"},
        {"country": "GB"},
    ],
)
def test_missing_address_precision_refuses_whole_batch(native_calls, changes):
    with pytest.raises(ValueError):
        prepare_registry_location_commands([_command(), _with_address(_command(), **changes)])
    assert len(native_calls) <= 1


def test_closed_shapes_duplicates_and_total_rows_refuse_before_native(native_calls):
    command = _command()
    known = _known(command)
    native_calls.clear()
    bad_inputs = [
        {"commands": []},
        {"commands": [command, command]},
        {"commands": [replace(command, record_kind="provider")]},
        {"commands": [replace(command, fields={**command.fields, "extra": True})]},
        {"commands": [_command()], "known_locations": {str(command.record_id): known}},
        {"commands": [_command()], "known_locations": {command.record_id: {**known, "extra": True}}},
        {"commands": [command], "bindings": [{}]},
        {
            "commands": [command],
            "requested_bindings": [{"provider_system": "manual", "provider_id": str(uuid4()), "location_id": uuid4()}],
        },
        {"commands": [command] * 5001},
        {"commands": [command], "bindings": [{}] * 5000},
    ]
    for parameters in bad_inputs:
        with pytest.raises(ValueError):
            prepare_registry_location_commands(**parameters)
    assert native_calls == []


def test_inconsistent_known_native_metadata_refuses_whole_batch(native_calls):
    command = _command()
    known = _known(command)
    known["canonical_address_json"]["identity_key"] = "inconsistent"
    native_calls.clear()
    with pytest.raises(ValueError, match="ledger_canonical_conflict"):
        prepare_registry_location_commands([command], known_locations={command.record_id: known})
    assert len(native_calls) == 1


def test_whole_input_bytes_are_bounded_before_native(native_calls):
    fields_by_name = {**_command().fields, "aliases": [str(index) + "x" * 500 for index in range(100)]}
    commands = [_command(fields=fields_by_name) for _ in range(170)]
    with pytest.raises(ValueError, match="batch_input_limit"):
        prepare_registry_location_commands(commands)
    assert native_calls == []


def test_ambiguous_match_output_is_bounded(native_calls):
    with pytest.raises(ValueError, match="batch_output_limit"):
        prepare_registry_location_commands([_command() for _ in range(500)])
    assert len(native_calls) == 1


@pytest.mark.parametrize("outcomes", [[], [{}], [{"identity_key": "incomplete"}]])
def test_malformed_native_batch_refuses_whole_result(monkeypatch, outcomes):
    class Native:
        def canonicalize_batch(self, _rows):
            return outcomes

    monkeypatch.setattr(store, "_fast_module", lambda: Native())
    with pytest.raises(ValueError, match="location_address_invalid"):
        prepare_registry_location_commands([_command()])


def test_unavailable_native_has_no_fallback(monkeypatch):
    monkeypatch.setattr(store, "_fast_module", lambda: None)
    with pytest.raises(store.RegistryAddressUnavailable, match="native_unavailable"):
        prepare_registry_location_commands([_command()])


@pytest.mark.parametrize(
    "changes",
    [
        {"provider_system": "company"},
        {"provider_id": True},
        {"provider_id": " 11111111-1111-4111-8111-111111111111"},
        {"provider_id": "11111111111141118111111111111111"},
        {"provider_system": "npi", "provider_id": "1234567890"},
        {"owner": "administrator"},
    ],
    ids=["unknown-namespace", "untyped-id", "padded-id", "noncanonical-uuid", "invalid-npi", "unknown-owner"],
)
def test_manual_location_binding_identity_refuses_before_native(monkeypatch, changes):
    command = _command()
    binding_by_field = {
        "provider_system": "manual",
        "provider_id": "11111111-1111-4111-8111-111111111111",
        "location_id": command.record_id,
        "owner": "manual",
        "identity_key": "reviewed-address",
        **changes,
    }

    def forbidden():
        pytest.fail("invalid binding_by_field reached native canonicalization")

    monkeypatch.setattr(store, "_fast_module", forbidden)
    with pytest.raises(ValueError, match="registry_location_binding_invalid"):
        prepare_registry_location_commands([command], bindings=[binding_by_field])


def test_repeated_provider_location_pair_refuses_before_native(monkeypatch):
    command = _command()
    binding_by_field = {
        "provider_system": "manual",
        "provider_id": "11111111-1111-4111-8111-111111111111",
        "location_id": command.record_id,
        "owner": "manual",
        "identity_key": "reviewed-address",
    }

    def forbidden():
        pytest.fail("duplicate binding_by_field reached native canonicalization")

    monkeypatch.setattr(store, "_fast_module", forbidden)
    with pytest.raises(ValueError, match="registry_location_binding_duplicate"):
        prepare_registry_location_commands([command], bindings=[binding_by_field, dict(binding_by_field)])
