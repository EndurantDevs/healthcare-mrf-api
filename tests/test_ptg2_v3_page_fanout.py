# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded provider-set fanout contract for V3 reverse pages."""

from tests import test_ptg2_postgres_binary_v3_pages_api as page_proof


@page_proof.pytest.mark.asyncio
async def test_reverse_page_caps_provider_set_fanout(monkeypatch):
    availability = page_proof.AsyncMock(return_value=True)
    plan_order = page_proof.AsyncMock(
        side_effect=AssertionError("oversized page must skip plan aggregate")
    )
    provider_lookup = page_proof.AsyncMock(
        side_effect=AssertionError("oversized page must skip provider lookup")
    )
    serving = page_proof.ptg2_serving
    monkeypatch.setattr(serving, "has_shared_provider_pages_in_db", availability)
    monkeypatch.setattr(serving, "_has_single_plan_page_order", plan_order)
    monkeypatch.setattr(serving, "_provider_set_keys_for_ids", provider_lookup)
    oversized_query = page_proof.replace(
        page_proof.reverse_query(limit=1),
        provider_set_ids=tuple(
            f"{provider_index + 1:032x}" for provider_index in range(65)
        ),
    )

    page_scope = await serving._version_three_page_projection_scope(
        object(),
        page_proof.serving_tables(),
        oversized_query,
    )

    assert page_scope is None
    availability.assert_awaited_once()
    plan_order.assert_not_awaited()
    provider_lookup.assert_not_awaited()
