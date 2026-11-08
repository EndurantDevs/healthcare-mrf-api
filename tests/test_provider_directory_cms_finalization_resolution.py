# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Only the exact typed-call variant extends the existing fixed body contract."""

from copy import deepcopy

import pytest

from db import migration_cms_snapshot_allocator as migration
from db.plan_release_storage import storage_relation
from process import provider_directory_cms_callable_custody as callables
from process import provider_directory_generic_source_release_checks as checks
from tests.test_provider_directory_cms_callee_custody import _catalog_rows, _original_allocation_pins
from tests.test_provider_directory_generic_source_release_checks import legacy_function_rows

NAME = "guard_pd_generic_dataset_finalization"


def _row(monkeypatch, schema="source_schema"):
    row = next(entry for entry in legacy_function_rows(monkeypatch) if entry["definition"]["proname"] == NAME)
    original = row["definition"]["prosrc"]
    row["definition"]["prosrc"] = original.replace('"source_schema".', '"' + schema + '".')
    return row


def test_hardened_body_changes_only_the_one_existing_argument_binding():
    original, hardened = migration.generic_finalization_bodies("source_schema")
    assert original != hardened
    assert (
        hardened.replace("(NEW.dataset_id::pg_catalog.text,'generic'::pg_catalog.text)", "(NEW.dataset_id,'generic')")
        == original
    )


@pytest.mark.parametrize("version", [0, 1])
def test_release_verifier_preserves_both_exact_bodies_and_all_properties(monkeypatch, version):
    row = _row(monkeypatch)
    row["definition"]["prosrc"] = migration.generic_finalization_bodies("source_schema")[version]
    before = deepcopy(row)
    checks._require_native_function(
        row["definition"], checks._LEGACY_FUNCTIONS[NAME + "()"], row["language"], "source_schema", 201, {105}
    )
    assert row == before


@pytest.mark.parametrize("fault", ["body", "owner", "settings", "security", "type"])
def test_typed_body_does_not_admit_other_native_changes(monkeypatch, fault):
    row = _row(monkeypatch)
    definition = row["definition"]
    definition["prosrc"] = migration.generic_finalization_bodies("source_schema")[1]
    if fault == "body":
        definition["prosrc"] += "\n-- unapproved change"
    elif fault == "owner":
        definition["proowner"] = "999"
    elif fault == "settings":
        definition["proconfig"] = ["search_path=pg_catalog,public"]
    elif fault == "security":
        definition["prosecdef"] = False
    else:
        definition["prorettype"] = "2278"
    with pytest.raises(ValueError):
        checks._require_native_function(
            definition, checks._LEGACY_FUNCTIONS[NAME + "()"], row["language"], "source_schema", 201, {105}
        )


@pytest.mark.parametrize("version", [0, 1])
def test_original_allocation_requires_the_same_complete_authenticated_body_pin(monkeypatch, version):
    rows = _catalog_rows("snapshot_schema")
    row = _row(monkeypatch, "snapshot_schema")
    row["definition"].update(oid="999", pronamespace="42")
    row["definition"]["prosrc"] = migration.generic_finalization_bodies("snapshot_schema")[version]
    row["signature"] = storage_relation("snapshot_schema", NAME) + "()"
    rows.append(row)
    pins = _original_allocation_pins(rows)
    assert len(callables.require_original_cms_allocation_callees(rows, **pins)) == len(rows)
    row["definition"]["prosrc"] += "\n-- changed after authentication"
    with pytest.raises(ValueError, match="original_allocation_pin_changed"):
        callables.require_original_cms_allocation_callees(rows, **pins)


def test_changed_template_refuses_instead_of_deriving_an_observed_variant(monkeypatch):
    source = migration.candidate_functions("source_schema")
    monkeypatch.setattr(
        migration, "candidate_functions", lambda _schema: source.replace("NEW.dataset_id,'generic'", "NULL")
    )
    with pytest.raises(RuntimeError, match="finalization_template_changed"):
        migration.generic_finalization_bodies("source_schema")
