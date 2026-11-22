# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Offline checks of exact search replay and office-level participation."""

import pytest

from process.delta_dental_directory_capture import (
    FinderCapturePlan,
    FinderCard,
    FinderPage,
    FinderSearch,
    iter_finder_windows,
)

ALL = FinderSearch("All networks", "00000", 30)
PATIENT_DIRECT = FinderSearch("Delta Dental Patient Direct", "00000", 30)
COMPOSITE = FinderSearch("Delta Dental PPO Plus Premier", "00000", 30)


async def _check():
    return None


def _card(practitioner, office, *labels):
    return FinderCard("Example Dentist", "Example Dental", "1 Example Street", labels, practitioner, office)


@pytest.mark.asyncio
async def test_resume_scoped_results(tmp_path):
    calls = []
    pages_by_body = {
        b"first": FinderPage(ALL, 1, 100, tuple(_card(f"p{n}", "o1", "Delta Dental PPO") for n in range(50))),
        b"second": FinderPage(ALL, 2, 100, tuple(_card(f"p{n}", "o1", "Delta Dental PPO") for n in range(50, 100))),
        b"patient": FinderPage(PATIENT_DIRECT, 1, 1, (_card("pd", "o2", "Delta Dental Patient Direct"),)),
    }
    body_by_search_page = {(ALL, 1): b"first", (ALL, 2): b"second", (PATIENT_DIRECT, 1): b"patient"}
    should_fail = True

    async def fetch(search, number, _previous_page):
        calls.append((search, number))
        if should_fail and (search, number) == (ALL, 2):
            raise TimeoutError
        return body_by_search_page[search, number]

    async def capture(*, resume=False):
        return [
            window
            async for window in iter_finder_windows(
                FinderCapturePlan(tmp_path, "example", "example", (ALL, ALL, PATIENT_DIRECT), resume=resume),
                fetch_page=fetch,
                parse_page=lambda raw, *_: pages_by_body[raw],
                cancel_check=_check,
            )
        ]

    with pytest.raises(TimeoutError):
        await capture()
    should_fail = False
    all_window, patient_window = await capture(resume=True)
    assert all_window.gaps == {"page_set_consistency_unverified", "result_cap_reached"}
    assert patient_window.gaps == set()
    assert patient_window.pages[0].cards[0].memberships == {"Delta Dental Patient Direct"}
    assert calls == [(ALL, 1), (ALL, 2), (ALL, 2), (PATIENT_DIRECT, 1)]
    replay = await capture(resume=True)
    assert [window.pages for window in replay] == [all_window.pages, patient_window.pages]
    assert [window.cached_pages for window in replay] == [2, 1]
    assert len(calls) == 4


@pytest.mark.asyncio
async def test_office_level_composite_memberships(tmp_path):
    page = FinderPage(
        COMPOSITE,
        1,
        3,
        (
            _card("p1", "o1", "Delta Dental PPO", "Delta Dental Premier"),
            _card("p1", "o2", "Delta Dental Premier"),
            FinderCard("Example Dentist", "Other Dental", "3 Example Street", ("Delta Dental Advantage",)),
        ),
    )

    async def fetch(_search, _number, _previous_page):
        return b"publisher bytes"

    windows = [
        window
        async for window in iter_finder_windows(
            FinderCapturePlan(tmp_path, "example", "example", (COMPOSITE,)),
            fetch_page=fetch,
            parse_page=lambda *_: page,
            cancel_check=_check,
        )
    ]
    assert [card.memberships for card in windows[0].pages[0].cards] == [
        {"Delta Dental PPO", "Delta Dental Premier"},
        {"Delta Dental Premier"},
        set(),
    ]
    assert windows[0].gaps == {
        "unmapped_card_network",
        "missing_card_membership",
        "missing_publisher_identity",
        "selected_network_not_observed",
    }
    assert all(card.practitioner_npi is None for card in page.cards)


@pytest.mark.asyncio
async def test_invalid_page_retained(tmp_path):
    total = 51
    calls = []

    async def fetch(_search, number, _previous_page):
        calls.append(number)
        return str(number).encode()

    def parse(raw, *_):
        if raw == b"1":
            return FinderPage(ALL, 1, 51, tuple(_card(f"p{n}", "o", "Delta Dental PPO") for n in range(50)))
        return FinderPage(ALL, 2, total, (_card("p50", "o", "Delta Dental PPO"),))

    async def capture(*, resume=False):
        return [
            window
            async for window in iter_finder_windows(
                FinderCapturePlan(tmp_path, "example", "example", (ALL,), resume=resume),
                fetch_page=fetch,
                parse_page=parse,
                cancel_check=_check,
            )
        ]

    total = 52
    with pytest.raises(ValueError, match="delta_dental_finder_count_changed"):
        await capture()
    with pytest.raises(ValueError, match="delta_dental_finder_count_changed"):
        await capture(resume=True)
    assert calls == [1, 2]
    with pytest.raises(ValueError, match="delta_dental_finder_capture_plan_changed"):
        [
            window
            async for window in iter_finder_windows(
                FinderCapturePlan(tmp_path, "example", "example", (PATIENT_DIRECT,), resume=True),
                fetch_page=fetch,
                parse_page=parse,
                cancel_check=_check,
            )
        ]
    assert calls == [1, 2]


@pytest.mark.asyncio
async def test_overlapping_plan_reuse(tmp_path):
    calls = []

    async def fetch(search, number, _previous_page):
        calls.append((search, number))
        return search.selector.encode()

    def parse(_body, search, number):
        return FinderPage(search, number, 0, ())

    async def run(edition, plan_id, searches, *, resume=False):
        return [
            window
            async for window in iter_finder_windows(
                FinderCapturePlan(tmp_path, edition, edition, searches, plan_id=plan_id, resume=resume),
                fetch_page=fetch,
                parse_page=parse,
                cancel_check=_check,
            )
        ]

    assert [window.cached_pages for window in await run("edition-a", "one", [ALL])] == [0]
    assert [window.cached_pages for window in await run("edition-a", "two", [ALL, PATIENT_DIRECT])] == [1, 0]
    assert [window.cached_pages for window in await run("edition-a", "three", [PATIENT_DIRECT, ALL])] == [1, 1]
    assert calls == [(ALL, 1), (PATIENT_DIRECT, 1)]
    with pytest.raises(ValueError, match="capture_plan_changed"):
        await run("edition-a", "two", [PATIENT_DIRECT, ALL], resume=True)
    assert [window.cached_pages for window in await run("edition-b", "four", [ALL])] == [0]
    assert calls == [(ALL, 1), (PATIENT_DIRECT, 1), (ALL, 1)]
