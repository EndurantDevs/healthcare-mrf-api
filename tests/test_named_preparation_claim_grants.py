# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Claim read grants reach catalogs created before the grants were part of the migration."""

from types import SimpleNamespace

import pytest

from db import migration_named_preparation as catalog


class _Bind:
    """Record statements against a catalog whose tables already exist."""

    def __init__(self, present):
        self.present, self.statements = present, []

    def execute(self, statement, parameters=None):
        sql = str(statement)
        self.statements.append(sql)
        return SimpleNamespace(scalars=lambda: list(self.present))


@pytest.mark.parametrize("present", [catalog.TABLES, ()])
def test_claim_read_grants_apply_to_existing_and_fresh_catalogs(monkeypatch, present):
    monkeypatch.setattr(catalog, "validate_named_preparation_catalog", lambda _bind: None)
    bind = _Bind(present)
    catalog.upgrade_named_preparation_catalog(bind)
    assert bind.statements[-2:] == list(catalog.CLAIM_READ_GRANTS)
    created_tables = [sql for sql in bind.statements if sql.startswith("CREATE TABLE")]
    assert len(created_tables) == (0 if present else len(catalog.TABLES))
