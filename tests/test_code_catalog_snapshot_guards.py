# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Relation access capture accepts only locally replayable grants."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import code_catalog_snapshot as catalog


@pytest.mark.parametrize(
    "changes",
    [
        {"grantee_name": 1},
        {"privilege_type": "SUPERUSER"},
        {"grantor_name": None},
        {"grantor_name": "other_owner"},
        {"column_name": "bad;column"},
        {"column_name": "code", "privilege_type": "TRUNCATE"},
    ],
)
def test_access_grants_cannot_change_grantor_or_privilege_scope(changes):
    grant_by_field = {
        "column_name": None,
        "grantee_name": "reader",
        "grantee_is_public": False,
        "privilege_type": "SELECT",
        "is_grantable": False,
        "grantor_name": "owner",
        **changes,
    }
    with pytest.raises(catalog.CodeCatalogSnapshotError):
        catalog._validated_access_grant(grant_by_field, "owner")


@pytest.mark.asyncio
@pytest.mark.parametrize("owner_name", [None, "", 17])
async def test_cutover_cannot_preserve_an_unavailable_owner(owner_name):
    session = SimpleNamespace(scalar=AsyncMock(return_value=owner_name), execute=AsyncMock())
    with pytest.raises(catalog.CodeCatalogSnapshotError, match="owner is unavailable"):
        await catalog._relation_access(session, 17)
    session.execute.assert_not_awaited()


def test_column_grant_keeps_its_validated_column_name():
    grant = catalog._validated_access_grant(
        {
            "column_name": "code",
            "grantee_name": "reader",
            "grantee_is_public": False,
            "privilege_type": "SELECT",
            "is_grantable": False,
            "grantor_name": "owner",
        },
        "owner",
    )
    assert grant.column_name == "code" and grant.grantee_name == "reader"
