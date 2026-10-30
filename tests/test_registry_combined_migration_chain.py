# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Catalog and compact membership migrations have one ordered successor chain."""

from pathlib import Path

from alembic.script import ScriptDirectory


def test_catalog_membership_and_inventory_extend_the_existing_target_head():
    scripts = ScriptDirectory(str(Path(__file__).resolve().parents[1] / "alembic"))
    chain = (
        "20261025020000_company_affiliation_join",
        "20261028010000_registry_fhir_catalog",
        "20261028020000_registry_membership_refresh",
        "20261029010000_registry_source_inventory",
    )
    assert scripts.get_heads() == [chain[-1]]
    for predecessor, successor in zip(chain, chain[1:]):
        assert scripts.get_revision(successor).down_revision == predecessor
