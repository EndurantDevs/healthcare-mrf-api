# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fail-closed projection-row and postflight census support contracts."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from scripts.research import plan_pricing_projection_v3_census_support as support


@pytest.mark.asyncio
async def test_projection_row_counts_requires_the_bound_relation(
    monkeypatch,
) -> None:
    missing_result = Mock()
    missing_result.scalar_one.return_value = False
    session = SimpleNamespace(execute=AsyncMock(return_value=missing_result))
    monkeypatch.setattr(
        support,
        "table",
        lambda relation_name: f'"bound_schema"."{relation_name}"',
    )

    with pytest.raises(RuntimeError, match="relation is unavailable"):
        await support.projection_row_counts(session, "f" * 64)

    _, parameters = session.execute.await_args.args
    assert parameters == {
        "relation_name": '"bound_schema"."plan_pricing_projection_candidate"'
    }


@pytest.mark.asyncio
async def test_postflight_sets_a_bounded_statement_timeout(monkeypatch) -> None:
    transaction = SimpleNamespace(is_active=True, rollback=AsyncMock())
    session = SimpleNamespace(
        begin=AsyncMock(return_value=transaction),
        execute=AsyncMock(),
        rollback=AsyncMock(),
    )

    class _SessionContext:
        async def __aenter__(self):
            return session

        async def __aexit__(self, *_args):
            return None

    monkeypatch.setattr(
        support, "db", SimpleNamespace(session=lambda: _SessionContext())
    )
    monkeypatch.setattr(
        support, "lock_provider_generation", AsyncMock(return_value=None)
    )
    monkeypatch.setattr(
        support,
        "locked_release_input",
        AsyncMock(return_value=SimpleNamespace(identity={"release": "current"})),
    )
    monkeypatch.setattr(
        support, "provider_signature", AsyncMock(return_value="signature")
    )
    monkeypatch.setattr(
        support,
        "projection_row_counts",
        AsyncMock(return_value={"candidate": 0}),
    )

    postflight_result = await support._postflight(
        "hprelease_test",
        {
            "projection_id": "f" * 64,
            "release": {"release": "current"},
            "provider_signature": "signature",
            "persistent_counts_before": {"candidate": 0},
        },
    )

    assert postflight_result["accepted"] is True
    assert "SET LOCAL statement_timeout = '20min'" in {
        str(call.args[0]) for call in session.execute.await_args_list
    }
