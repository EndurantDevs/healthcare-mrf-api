# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""The inverse successor preserves fresh guards and authenticates historical projections."""

from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from process import provider_directory_profile_initial_guards as guards
from process import provider_directory_profile_initial_history as history
from process import provider_directory_profile_initial_inverse_guard as inverse
from tests.test_provider_directory_profile_initial_history import _payload
from tests.test_provider_directory_profile_initial_migration import _migration


def load_migration():
    return _migration("20261031030000_profile_initial_inverse_guard.py")


@pytest.mark.parametrize("schema", ["fixture", "quoted_\"schema's"])
def test_successor_is_frozen_and_retains_original_fresh_publication(monkeypatch, schema):
    migration = load_migration()
    original = migration._initial()
    expected = inverse.body(schema)
    assert migration._body(original, schema) == expected
    assert original._serving_insert_body(schema) == guards._serving_insert_body(schema)
    assert expected.startswith(guards._serving_insert_body(schema).split("THEN RAISE", 1)[0])
    assert "IF TG_OP='UPDATE' AND EXISTS" in expected
    assert "SECURITY DEFINER" not in expected
    monkeypatch.setattr(inverse, "body", lambda _schema: "changed runtime")
    monkeypatch.setattr(guards, "_serving_insert_body", lambda _schema: "changed runtime")
    assert load_migration()._body(original, schema) == expected


@pytest.mark.parametrize("successor", [False, True])
def test_current_native_recognition_only_accepts_complete_known_guard(successor):
    expected = inverse.body("fixture") if successor else guards._serving_insert_body("fixture")
    assert guards.validated_serving_guard_body("fixture", " " + expected + " ") == expected
    with pytest.raises(RuntimeError, match="guard_shape_changed"):
        guards.validated_serving_guard_body("fixture", expected.replace("RETURN NULL", "RETURN NEW", 1))
    with pytest.raises(RuntimeError, match="guard_shape_changed"):
        guards.validated_serving_guard_body("other", expected)


def _serving_catalog():
    payload = _payload(history._SERVING)
    source = " " + inverse.body("fixture") + " "
    payload["triggers"][0].update(
        trigger_name="pd_profile_initial_serving_insert",
        trigger_function_source=source,
        trigger_function_definition="CREATE FUNCTION fixture.guard() RETURNS trigger AS $function$"
        + source
        + "$function$ LANGUAGE plpgsql",
    )
    return payload


def test_historical_successor_projection_preserves_all_oids_and_other_guard_fields():
    current = _serving_catalog()
    before = deepcopy(current)
    previous = history.previous_serving_fingerprint_payload(current, schema="fixture")
    expected = deepcopy(current)
    old_source = " " + guards._serving_insert_body("fixture") + " "
    expected["triggers"][0]["trigger_function_source"] = old_source
    expected["triggers"][0]["trigger_function_definition"] = expected["triggers"][0][
        "trigger_function_definition"
    ].replace(current["triggers"][0]["trigger_function_source"], old_source)
    assert previous == expected
    assert current == before
    assert history.original_fingerprint_payload(current, schema="fixture", table=history._SERVING) == (
        history.original_fingerprint_payload(previous, schema="fixture", table=history._SERVING)
    )


@pytest.mark.parametrize("change", ["body", "definition", "duplicate", "missing", "schema", "table"])
def test_historical_successor_projection_refuses_unknown_or_partial_changes(change):
    payload = _serving_catalog()
    match change:
        case "body":
            payload["triggers"][0]["trigger_function_source"] += " changed"
        case "definition":
            payload["triggers"][0]["trigger_function_definition"] = "different definition"
        case "duplicate":
            payload["triggers"].append(deepcopy(payload["triggers"][0]))
        case "missing":
            payload["triggers"] = []
        case "schema":
            payload["relation"]["schema_name"] = "other"
        case "table":
            payload["relation"]["relation_name"] = guards.RECEIPT_TABLE
    with pytest.raises(RuntimeError, match="history_catalog_changed"):
        history.previous_serving_fingerprint_payload(payload, schema="fixture")


def test_inverse_predicate_binds_actual_typed_rows_and_fresh_signed_worker():
    body = inverse.body("fixture")
    assert "to_jsonb(OLD)=to_jsonb(jsonb_populate_record(" in body
    assert "to_jsonb(NEW)=to_jsonb(jsonb_populate_record(" in body
    assert "jsonb_object_keys(to_jsonb(OLD))" in body
    assert "jsonb_object_keys(to_jsonb(NEW))" in body
    assert "authority.execution->>'published_event_sha256'=published.document_sha256" in body
    assert "preflight.receipt_json::jsonb=proof.receipt" in body
    assert "worker.run_id=consumed.run_id" in body
    assert "session_user=current_user" in body
    assert "'xid',pg_current_xact_id()::text" in body
    assert "(signed.lease->>'max_build_deadline')::timestamptz=consumed.max_build_deadline" in body
    assert "clock_timestamp()<consumed.max_build_deadline" in body


def test_migration_rejects_changed_function_identity_before_replacement(monkeypatch):
    migration = load_migration()
    calls = []
    monkeypatch.setattr(migration, "_function_identity", lambda *_: (_ for _ in ()).throw(RuntimeError("changed")))
    monkeypatch.setattr(migration.op, "execute", calls.append)
    with pytest.raises(RuntimeError, match="changed"):
        migration._replace(migration._initial(), "fixture", inverse=True)
    assert calls == []


@pytest.mark.parametrize("used", [True, False])
def test_downgrade_preserves_any_initial_or_inverse_history(monkeypatch, used):
    migration = load_migration()
    execute = Mock(return_value=SimpleNamespace(scalar_one=lambda: used))
    replace = Mock()
    monkeypatch.setattr(migration, "_locked", lambda _initial: "fixture")
    monkeypatch.setattr(migration, "_replace", replace)
    monkeypatch.setattr(migration.op, "get_bind", lambda: SimpleNamespace(execute=execute))
    if used:
        with pytest.raises(RuntimeError, match="history_requires_retention"):
            migration.downgrade()
        replace.assert_not_called()
    else:
        migration.downgrade()
        assert replace.call_args.kwargs == {"inverse": False}
    sql = str(execute.call_args.args[0])
    assert 'EXISTS(SELECT FROM "fixture"."provider_directory_profile_initial_receipt")' in sql
    assert "WHERE status='published'" in sql
    assert "WHERE admission_purpose='serving_inverse'" in sql
