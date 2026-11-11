# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Feature preparation preserves existing callable identities and native index custody."""

import copy
from functools import lru_cache
from unittest.mock import Mock

import pytest

from db import migration_rooted_publication_freeze as freeze
from tests.test_acquisition_validator_argument_types_migration import _current_functions, _migration
from tests.test_provider_directory_rooted_graph_publication_freeze import _current_bodies
from tests.test_provider_directory_source_callable_custody import _entry


@lru_cache
def _bodies():
    bodies = _current_bodies()
    for (name, _), attributes in _current_functions().items():
        if name not in {
            "provider_directory_rooted_graph_dataset_valid",
            "provider_directory_rooted_graph_dataset_ready",
        }:
            continue
        body = attributes[-1]
        for caller, old, new, count in _migration()._calls("snapshot_schema"):
            if caller == name:
                assert body.count(old) == count
                body = body.replace(old, new)
        bodies[name] = body
    return bodies


def _functions():
    functions_by_name = {}
    for name, body in _bodies().items():
        signature = name + ("()" if name in (freeze.GUARD, freeze.WORK_GUARD, freeze.SINGLE_ROOT_GUARD) else "(text)")
        entry = _entry(signature)
        identity_by_field = {key: value for key, value in entry["definition"].items() if key != "prosrc"}
        functions_by_name[name] = {
            "body": body,
            "definition": "CREATE OR REPLACE FUNCTION " + name + " AS $body$" + body + "$body$",
            "identity": identity_by_field,
            "description": "existing callable",
            "language": entry["language"],
        }
    return functions_by_name


def _installer(monkeypatch, records, *, prepared=True):
    connection = Mock()
    connection.in_transaction.return_value = True
    connection.execute.side_effect = [
        Mock(scalar_one_or_none=lambda: True),
        Mock(scalar_one_or_none=lambda: "preparation" if prepared else None),
    ]

    def snapshot(_connection, signature):
        name = signature.split(".", 1)[1].split("(", 1)[0].strip('"')
        return copy.deepcopy(records[name])

    def replace(statement):
        name = statement.split("CREATE OR REPLACE FUNCTION ", 1)[1].split(" AS ", 1)[0]
        record = records[name]
        body = freeze.wrapped_body(name, "snapshot_schema", record["body"])
        assert statement == record["definition"].replace(record["body"], body, 1)
        record.update(body=body, definition=statement)

    connection.exec_driver_sql.side_effect = replace
    monkeypatch.setattr(freeze, "_function_snapshot", snapshot)
    auxiliary = Mock(return_value={"existing": "dependencies"})
    index = Mock()
    monkeypatch.setattr(freeze, "_function_auxiliary", auxiliary)
    monkeypatch.setattr(freeze, "_ensure_preparation_handoff_index", index)
    return connection, auxiliary, index


@pytest.mark.parametrize("prepared", [False, True])
def test_upgrade_is_idempotent_and_preserves_callable_identity_and_dependencies(monkeypatch, prepared):
    records = _functions()
    originals = copy.deepcopy(records)
    connection, auxiliary, index = _installer(monkeypatch, records, prepared=prepared)
    freeze.upgrade_rooted_publication_freeze(connection, "snapshot_schema")
    assert connection.exec_driver_sql.call_count == (6 if prepared else 2)
    assert auxiliary.call_count == 2
    if prepared:
        index.assert_called_once_with(connection)
    else:
        index.assert_not_called()
    for name, record in records.items():
        assert record["identity"] == originals[name]["identity"]
        assert record["description"] == originals[name]["description"]
        if name in freeze.ORIGINAL_DIGESTS and (prepared or name in (freeze.WORK_GUARD, freeze.SINGLE_ROOT_GUARD)):
            assert freeze.is_approved_successor(name, "snapshot_schema", record["body"], freeze.ORIGINAL_DIGESTS[name])
        else:
            assert record["body"] == originals[name]["body"]
    connection, _, _ = _installer(monkeypatch, records, prepared=prepared)
    freeze.upgrade_rooted_publication_freeze(connection, "snapshot_schema")
    connection.exec_driver_sql.assert_not_called()


@pytest.mark.parametrize("name", [freeze.ROOTED, freeze.WORK_GUARD, freeze.SINGLE_ROOT_GUARD])
@pytest.mark.parametrize("mutation", ["missing", "body", "owner", "security", "settings", "definition"])
def test_upgrade_refuses_unrecognized_callables_before_replacing_them(monkeypatch, mutation, name):
    records = _functions()
    record = records[name]
    mutations_by_name = {
        "body": lambda: record.update(body=record["body"] + "\n-- changed body\n"),
        "owner": lambda: record["identity"].update(proowner="999"),
        "security": lambda: record["identity"].update(prosecdef=False),
        "settings": lambda: record["identity"].update(proconfig=["search_path=public"]),
        "definition": lambda: record.update(definition=record["definition"] + record["body"]),
    }
    if mutation in mutations_by_name:
        mutations_by_name[mutation]()
    connection, _, index = _installer(monkeypatch, records)
    if mutation == "missing":
        monkeypatch.setattr(freeze, "_function_snapshot", lambda *_args: None)
    with pytest.raises(RuntimeError, match="rooted_publication_(function|definition)"):
        freeze.upgrade_rooted_publication_freeze(connection, "snapshot_schema")
    connection.exec_driver_sql.assert_not_called()
    index.assert_not_called()


@pytest.mark.parametrize("drift", ["identity", "description", "dependencies"])
def test_upgrade_checks_post_replacement_custody(monkeypatch, drift):
    records = _functions()
    connection, auxiliary, index = _installer(monkeypatch, records)
    replace = connection.exec_driver_sql.side_effect

    def changed(statement):
        replace(statement)
        record = records[next(iter(records))]
        if drift == "identity":
            record["identity"]["proacl"] = ["changed"]
        elif drift == "description":
            record["description"] = "changed"

    connection.exec_driver_sql.side_effect = changed
    if drift == "dependencies":
        auxiliary.side_effect = [{"existing": "dependencies"}, {"unexpected": "dependency"}]
    with pytest.raises(RuntimeError, match="rooted_publication_function_(replacement|custody)_changed"):
        freeze.upgrade_rooted_publication_freeze(connection, "snapshot_schema")
    index.assert_not_called()


@pytest.mark.parametrize("prior_role", ["none", "installer_role"])
def test_handoff_hash_index_preserves_original_role_and_uses_native_jsonb_equality(prior_role):
    connection = Mock()
    before = ("administrator", prior_role)
    connection.execute.side_effect = [
        Mock(one=lambda: before),
        Mock(scalar_one=lambda: "protected_owner"),
        Mock(scalar_one_or_none=lambda: None),
        Mock(one=lambda: before),
        Mock(scalar_one_or_none=lambda: True),
    ]
    freeze._ensure_preparation_handoff_index(connection)
    statements = [call.args[0] for call in connection.exec_driver_sql.call_args_list]
    assert statements[0] == 'SET LOCAL ROLE "protected_owner"'
    assert "USING hash(handoff)" in statements[1] and "CREATE INDEX" in statements[1]
    assert statements[-1] == ("RESET ROLE" if prior_role == "none" else 'SET LOCAL ROLE "installer_role"')


@pytest.mark.parametrize("drift", ["role", "index"])
def test_existing_handoff_index_refuses_invalid_custody_without_rebuilding_it(drift):
    connection = Mock()
    before = ("administrator", "none")
    connection.execute.side_effect = [
        Mock(one=lambda: before),
        Mock(scalar_one=lambda: "protected_owner"),
        Mock(scalar_one_or_none=lambda: "existing_index"),
        Mock(one=lambda: ("other", "none") if drift == "role" else before),
        Mock(scalar_one_or_none=lambda: drift != "index"),
    ]
    with pytest.raises(RuntimeError, match="rooted_publication_(migration_role|handoff_index)_changed"):
        freeze._ensure_preparation_handoff_index(connection)
    connection.exec_driver_sql.assert_not_called()
