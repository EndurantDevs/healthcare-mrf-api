# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Verify browser submission choices without contacting the publisher."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process.delta_dental_directory_browser import FINDER_URL, _FinderBrowser
from process.delta_dental_directory_capture import FinderPaginationUnresolved, FinderSearch

PPO = FinderSearch("Delta Dental PPO", "01234", 30)
PREMIER = FinderSearch("Delta Dental Premier", "01234", 30)


def _browser(page):
    finder = _FinderBrowser(
        page, FINDER_URL, Mock(), AsyncMock(), {"network_switches": 0, "search_submissions": 0}, None
    )
    finder.active_search = PPO
    finder._snapshot = AsyncMock(return_value=b"rendered")
    return finder


@pytest.mark.asyncio
async def test_missing_results_option_uses_landing():
    picker = SimpleNamespace(
        get_by_role=Mock(return_value=SimpleNamespace(all_text_contents=AsyncMock(return_value=[PPO.selector]))),
        select_option=AsyncMock(),
    )
    landing_picker = SimpleNamespace(evaluate=AsyncMock(return_value=True), select_option=AsyncMock())
    submit = SimpleNamespace(click=AsyncMock())
    controls_by_label = {
        "Dentist network": picker,
        "Plan network (required):": landing_picker,
        "No": SimpleNamespace(check=AsyncMock()),
        "ZIP code, city, or address (required):": SimpleNamespace(fill=AsyncMock()),
        "Address:": SimpleNamespace(wait_for=AsyncMock(), input_value=AsyncMock(return_value=PREMIER.zip_code)),
    }
    page = SimpleNamespace(
        goto=AsyncMock(),
        get_by_role=Mock(
            side_effect=lambda role, *, name, **kwargs: submit if role == "button" else controls_by_label[name]
        ),
    )
    finder = _browser(page)
    assert await finder.fetch_page(PREMIER, 1) == b"rendered"
    picker.select_option.assert_not_awaited()
    landing_picker.select_option.assert_awaited_once_with("Premier")
    submit.click.assert_awaited_once()
    page.goto.assert_awaited_once()


@pytest.mark.asyncio
async def test_missing_landing_option_records_gap():
    picker = SimpleNamespace(evaluate=AsyncMock(return_value=False), select_option=AsyncMock())
    page = SimpleNamespace(goto=AsyncMock(), get_by_role=Mock(return_value=picker))
    finder = _browser(page)
    finder._switch_network = AsyncMock(return_value=None)
    with pytest.raises(FinderPaginationUnresolved, match="landing_network_option_unavailable"):
        await finder.fetch_page(PREMIER, 1)
    page.goto.assert_awaited_once()
    picker.select_option.assert_not_awaited()
    finder._snapshot.assert_not_awaited()


@pytest.mark.asyncio
async def test_unverified_switch_never_resubmits():
    picker = SimpleNamespace(
        get_by_role=Mock(return_value=SimpleNamespace(all_text_contents=AsyncMock(return_value=[PREMIER.selector]))),
        select_option=AsyncMock(),
        evaluate=AsyncMock(return_value=PREMIER.selector),
    )
    page = SimpleNamespace(get_by_role=Mock(return_value=picker), evaluate=AsyncMock())
    finder = _browser(page)
    finder._search = AsyncMock()
    finder._snapshot.side_effect = FinderPaginationUnresolved("results_network_transition_unverified")
    with pytest.raises(FinderPaginationUnresolved, match="results_network_transition_unverified"):
        await finder.fetch_page(PREMIER, 1)
    picker.select_option.assert_awaited_once_with(label=PREMIER.selector)
    finder._search.assert_not_awaited()


@pytest.mark.asyncio
async def test_same_search_does_not_submit():
    finder = _browser(None)
    finder._switch_network = AsyncMock()
    finder._search = AsyncMock()
    assert await finder.fetch_page(PPO, 1) == b"rendered"
    finder._switch_network.assert_not_awaited()
    finder._search.assert_not_awaited()
