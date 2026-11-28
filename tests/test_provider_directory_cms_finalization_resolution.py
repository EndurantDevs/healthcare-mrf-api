# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Only the exact typed-call variant extends the existing fixed body contract."""

import pytest

from db import migration_cms_snapshot_allocator as migration

NAME = "guard_pd_generic_dataset_finalization"


def test_hardened_body_changes_only_the_one_existing_argument_binding():
    original, hardened = migration.generic_finalization_bodies("source_schema")
    assert original != hardened
    assert (
        hardened.replace("(NEW.dataset_id::pg_catalog.text,'generic'::pg_catalog.text)", "(NEW.dataset_id,'generic')")
        == original
    )


def test_changed_template_refuses_instead_of_deriving_an_observed_variant(monkeypatch):
    source = migration.candidate_functions("source_schema")
    monkeypatch.setattr(
        migration, "candidate_functions", lambda _schema: source.replace("NEW.dataset_id,'generic'", "NULL")
    )
    with pytest.raises(RuntimeError, match="finalization_template_changed"):
        migration.generic_finalization_bodies("source_schema")
