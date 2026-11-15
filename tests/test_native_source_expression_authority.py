# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native expressions retain structural checks without role identity authority."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import mrf_address_publication as publication


@pytest.mark.asyncio
@pytest.mark.parametrize("roles", [None, ("snapshot_owner", "snapshot_publisher")])
async def test_expression_read_uses_layout_validator_without_role_membership_queries(monkeypatch, roles):
    check = AsyncMock()
    monkeypatch.setattr(publication, "_require_native_read_structure", check)
    session = SimpleNamespace(scalar=AsyncMock())
    await publication.require_native_read_catalog(session, (42,), allowed_type_oids=(8,), executable_roles=roles)
    check.assert_awaited_once_with(session, (42,), (8,))
    session.scalar.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("roles", [(), ("owner",), ("owner", "owner"), ["owner", "publisher"], ("owner", "bad-role")])
async def test_malformed_configured_role_names_cannot_reach_native_catalog(monkeypatch, roles):
    check = AsyncMock()
    monkeypatch.setattr(publication, "_require_native_read_structure", check)
    with pytest.raises((RuntimeError, ValueError)):
        await publication.require_native_read_catalog(object(), (42,), executable_roles=roles)
    check.assert_not_awaited()
