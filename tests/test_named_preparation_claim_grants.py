# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Claim read grants reach catalogs created before the grants were part of the migration."""

from types import SimpleNamespace

import pytest

from db import migration_named_preparation as catalog


class _Bind:
    """Record statements against a catalog whose tables already exist."""

    def __init__(self, present, can_grant):
        self.present = present
        self.can_grant = can_grant
        self.statements = []

    def execute(self, statement, parameters=None):
        sql = str(statement)
        self.statements.append(sql)
        return SimpleNamespace(
            all=lambda: [(name, "r") for name in self.present],
            scalar_one=lambda: self.can_grant,
        )


@pytest.mark.parametrize(
    ("present", "can_grant", "grants_expected"),
    [(catalog.TABLES, True, True), ((), True, True), (catalog.TABLES, False, False)],
)
def test_claim_read_grants_preserve_externally_managed_catalog(monkeypatch, present, can_grant, grants_expected):
    monkeypatch.setattr(catalog, "validate_named_preparation_catalog", lambda _bind: None)
    bind = _Bind(present, can_grant)
    catalog.upgrade_named_preparation_catalog(bind)
    grants = [sql for sql in bind.statements if sql.startswith("GRANT")]
    assert grants == (list(catalog.CLAIM_READ_GRANTS) if grants_expected else [])
    created_tables = [sql for sql in bind.statements if sql.startswith("CREATE TABLE")]
    assert len(created_tables) == (0 if present else len(catalog.TABLES))
