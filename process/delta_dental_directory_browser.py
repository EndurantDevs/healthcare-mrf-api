# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Drive the licensed Delta Dental finder through its ordinary visible controls."""

import asyncio
import re
from contextlib import aclosing

from process.delta_dental_directory_capture import (
    FinderCapturePlan,
    FinderPaginationUnresolved,
    iter_finder_windows,
    validate_finder_page,
)
from process.delta_dental_directory_observations import parse_rendered_finder_page

SOURCE_ID = "delta-dental-national-finder"
SOURCE_ORIGIN = "https://www.deltadental.com"
FINDER_URL = SOURCE_ORIGIN + "/member/find-a-dentist/"
SELECTOR_VALUES = {
    "Delta Dental PPO": "PPO",
    "Delta Dental Premier": "Premier",
    "Delta Dental PPO Plus Premier": "ppoPlusPremier",
    "Delta Dental Medicare Advantage": "MedicareAdvantage",
    "DeltaCare USA": "Deltacare",
    "Delta Dental Patient Direct": "PatientDirect",
    "All networks": "NationalNetworks",
}


_WATCH_CARD_CHANGES = """() => {
    const label = '[aria-label$=" participates in"], a[aria-label*="get directions for "]';
    const isCard = node => node.nodeType === Node.ELEMENT_NODE &&
        (node.matches(label) || node.querySelector(label));
    window.deltaDirectoryObservation?.disconnect();
    window.deltaDirectoryCardsChanged = false;
    window.deltaDirectoryObservation = new MutationObserver(changes => {
        if (changes.some(change => change.target.parentElement?.closest(label)
            || change.target.nodeType === Node.ELEMENT_NODE && change.target.matches(label)
            || [...change.addedNodes, ...change.removedNodes].some(isCard))) {
            window.deltaDirectoryCardsChanged = true;
        }
    });
    window.deltaDirectoryObservation.observe(document.body, {
        subtree: true, childList: true, characterData: true, attributes: true
    });
}"""


class _FinderBrowser:
    def __init__(self, page, source_url, verify_transport, cancel_check, stats, open_page):
        self.page = page
        self.open_page = open_page
        self.source_url = source_url
        self.active_search = None
        self.verify_transport = verify_transport
        self.cancel_check = cancel_check
        self.stats = stats

    async def _search(self, search):
        """Submit an exact ordinary finder search and verify the echoed location."""
        if self.page is None:
            self.page = await self.open_page()
        page = self.page
        self.active_search = None
        await page.goto(self.source_url, wait_until="domcontentloaded")
        picker = page.get_by_role("combobox", name="Plan network (required):", exact=True)
        option = SELECTOR_VALUES[search.selector]
        if not await picker.evaluate(
            "(select, value) => [...select.options].some(option => option.value === value)", option
        ):
            raise FinderPaginationUnresolved("landing_network_option_unavailable")
        await picker.select_option(option)
        await page.get_by_role("radio", name="No", exact=True).check()
        await page.get_by_role("textbox", name="ZIP code, city, or address (required):", exact=True).fill(
            search.zip_code
        )
        self.stats["search_submissions"] += 1
        await page.get_by_role("button", name=re.compile(r"^Find Dentists$", re.I)).click()
        address = page.get_by_role("textbox", name="Address:", exact=True)
        await address.wait_for()
        if re.search(rf"(?<!\d){search.zip_code}(?!\d)", await address.input_value()) is None:
            raise ValueError("delta_dental_finder_rendered_address_changed")
        if search.radius_miles != 30:
            await self._radius(search.radius_miles)
        self.active_search = search

    async def _switch_network(self, search):
        """Reuse the active location and wait for a fresh result card update."""
        page = self.page
        picker = page.get_by_role("combobox", name="Dentist network", exact=True)
        labels = {label.strip() for label in await picker.get_by_role("option").all_text_contents()}
        requested_label = search.selector
        if requested_label == "All networks" and "All of the above networks" in labels:
            requested_label = "All of the above networks"
        if requested_label not in labels:
            return None
        await page.evaluate(_WATCH_CARD_CHANGES)
        try:
            self.stats["network_switches"] += 1
            await picker.select_option(label=requested_label)
            label = await picker.evaluate("select => select.selectedOptions[0]?.label.trim()")
            if label != requested_label:
                raise FinderPaginationUnresolved("results_network_selection_unverified")
            output = await self._snapshot(search, 1, require_change=True)
        except FinderPaginationUnresolved:
            self.active_search = None
            raise
        finally:
            await page.evaluate("() => window.deltaDirectoryObservation?.disconnect()")
        self.active_search = search
        return output

    async def _radius(self, radius):
        """Use the visible controls for a requested nondefault radius."""
        page = self.page
        await page.get_by_role("button", name="Advanced search", exact=True).click()
        dialog = page.get_by_role("dialog")
        distance = dialog.get_by_role("combobox", name="Distance", exact=False)
        options = await distance.get_by_role("option").all_text_contents()
        labels = [label for label in options if re.fullmatch(rf"(?:Within )?{radius:g} miles?", label.strip(), re.I)]
        if len(labels) != 1:
            raise ValueError("delta_dental_finder_distance_option_unknown")
        await distance.select_option(label=labels[0])
        apply = dialog.get_by_role("button", name="Search", exact=True)
        if await apply.count():
            self.stats["radius_actions"] += 1
            await apply.click()
        else:
            self.stats["radius_actions"] += 1
            await dialog.get_by_role("button", name="Close", exact=True).click()

    async def _next_page(self, search, number, previous_page):
        """Open the expected page through the ordinary pagination control."""
        if self.active_search != search:
            self.stats["pagination_reentries"] += 1
            await self._search(search)
            first = parse_rendered_finder_page(await self._snapshot(search, 1), search, 1)
            if first != previous_page:
                raise FinderPaginationUnresolved("page_set_changed")
        pagination = self.page.get_by_role("navigation", name="Dentist search results pagination", exact=True)
        target = pagination.get_by_role("button", name=f"Page {number}", exact=True)
        if not await target.count():
            raise FinderPaginationUnresolved("remaining_page_unavailable")
        self.stats["page_actions"] += 1
        await target.click()

    async def _snapshot(self, search, number, previous_page=None, *, require_change=False):
        """Wait for rendered card evidence before preserving the page."""
        summary = self.page.get_by_text(
            re.compile(
                rf"^\d+ dentist results? found within {search.radius_miles:g} miles? for {re.escape(search.selector)}$"
            )
        )
        if not require_change:
            await summary.wait_for(state="attached")
        loop = asyncio.get_running_loop()
        deadline = loop.time() + 10
        previous_offices = {(card.dentist, card.address) for card in previous_page.cards} if previous_page else set()
        while True:
            self.verify_transport()
            await self.cancel_check()
            body = (await self.page.content()).encode()
            self.verify_transport()
            page = _qualify_snapshot(body, search, number, previous_page)
            if page is not None:
                cards_changed = not require_change or (
                    await summary.count() == 1 and await self.page.evaluate("() => window.deltaDirectoryCardsChanged")
                )
                new_offices = {(card.dentist, card.address) for card in page.cards if card.address} - previous_offices
                if cards_changed and (previous_page is None or new_offices):
                    return body
            if loop.time() >= deadline:
                if require_change:
                    raise FinderPaginationUnresolved("results_network_transition_unverified")
                if previous_page is not None:
                    raise FinderPaginationUnresolved("remaining_page_unverified")
                return body
            await asyncio.sleep(0.2)

    async def fetch_page(self, search, number, previous_page=None):
        """Return exactly one newly rendered page for an authorized cache miss."""
        if number == 1:
            active = self.active_search
            if active == search:
                return await self._snapshot(search, number, previous_page)
            if active is not None and (active.zip_code, active.radius_miles) == (search.zip_code, search.radius_miles):
                if (rendered := await self._switch_network(search)) is not None:
                    return rendered
            await self._search(search)
        else:
            await self._next_page(search, number, previous_page)
        return await self._snapshot(search, number, previous_page)


def _qualify_snapshot(body, search, number, previous_page):
    """Return a parsed snapshot only after the local UI is fully rendered."""
    try:
        page = parse_rendered_finder_page(body, search, number)
        validate_finder_page(page, search, number, previous_page.displayed_total if previous_page else None)
    except ValueError:
        return None
    return page


def _noop():
    """Allow an already-verified offline renderer to omit a live guard."""
    return None


async def collect_finder_windows(
    page,
    capture: FinderCapturePlan,
    *,
    cancel_check,
    source_url=FINDER_URL,
    verify_transport=_noop,
    stats=None,
    open_page=None,
):
    """Stream observations using an operator-supplied, source-bound browser page."""
    stats = {} if stats is None else stats
    stats.update(search_submissions=0, pagination_reentries=0, radius_actions=0, page_actions=0, network_switches=0)
    if page is None and not callable(open_page):
        raise ValueError("delta_dental_finder_browser_required")
    browser = _FinderBrowser(page, source_url, verify_transport, cancel_check, stats, open_page)
    async with aclosing(
        iter_finder_windows(
            capture,
            fetch_page=browser.fetch_page,
            parse_page=parse_rendered_finder_page,
            cancel_check=cancel_check,
        )
    ) as windows:
        async for window in windows:
            verify_transport()
            yield window
