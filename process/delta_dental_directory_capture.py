# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Retain raw Delta Dental finder pages and assess the observed search window."""

import asyncio
import json
import re
import sqlite3
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable
from contextlib import aclosing, closing
from dataclasses import dataclass
from os import O_CREAT, O_EXCL, O_WRONLY, close
from os import open as open_file
from pathlib import Path

from process.provider_directory_projection_fhir_values import is_valid_npi
from process.ptg_parts.artifacts import PTG2ArtifactStore

NETWORK_LABELS = frozenset(
    {
        "Delta Dental PPO",
        "Delta Dental Premier",
        "Delta Dental Medicare Advantage",
        "DeltaCare USA",
        "Delta Dental Patient Direct",
    }
)
COMPOSITE_SELECTOR = "Delta Dental PPO Plus Premier"
COMPOSITE_NETWORKS = frozenset({"Delta Dental PPO", "Delta Dental Premier"})
FINDER_SELECTORS = NETWORK_LABELS | {COMPOSITE_SELECTOR, "All networks"}
FINDER_RADII = frozenset({0.5, 1, 5, 10, 15, 20, 30, 50, 100})
PAGE_SIZE = 50
RESULT_CAP = 100
MAX_PAGE_BYTES = 8 * 1024 * 1024
DEFAULT_REQUEST_BUDGET = 100_000


@dataclass(frozen=True)
class FinderSearch:
    """An exact unfiltered national finder search."""

    selector: str
    zip_code: str
    radius_miles: float

    def __post_init__(self):
        """Validate the observed source fields before retaining structured results."""
        if self.selector not in FINDER_SELECTORS:
            raise ValueError("delta_dental_finder_selector_unsupported")
        if (
            not isinstance(self.zip_code, str)
            or re.fullmatch(r"[0-9]{5}", self.zip_code) is None
            or type(self.radius_miles) not in (int, float)
            or self.radius_miles not in FINDER_RADII
        ):
            raise ValueError("delta_dental_finder_search_invalid")


@dataclass(frozen=True)
class FinderCard:
    """One dentist-at-an-office observation; evidence may lack durable IDs."""

    dentist: str
    office: str | None
    address: str | None
    network_labels: tuple[str, ...]
    practitioner_id: str | None = None
    office_id: str | None = None
    practitioner_npi: str | None = None
    specialty: str | None = None
    phone: str | None = None
    provider_office_id: str | None = None
    office_npi: str | None = None

    def __post_init__(self):
        """Validate the observed source fields before retaining structured results."""
        names = (
            (self.dentist, *self.network_labels)
            if self.address is None
            else (self.dentist, self.address, *self.network_labels)
        )
        for field in names:
            if not isinstance(field, str) or not field.strip():
                raise ValueError("delta_dental_finder_card_invalid")
        for field in (self.practitioner_id, self.office_id, self.provider_office_id):
            if field is not None and (not isinstance(field, str) or not field.strip() or field.strip() != field):
                raise ValueError("delta_dental_finder_card_identity_invalid")
        if any(value is not None and not is_valid_npi(value) for value in (self.practitioner_npi, self.office_npi)):
            raise ValueError("delta_dental_finder_card_npi_invalid")

    @property
    def memberships(self) -> frozenset[str]:
        """Return exact card badges for networks understood by the national finder."""
        return NETWORK_LABELS.intersection(self.network_labels)

    @property
    def office_memberships(self) -> frozenset[str]:
        """Do not attach badge observations to an unresolved office."""
        return self.memberships if self.address else frozenset()


@dataclass(frozen=True)
class FinderPage:
    """Fields read from one source page, including the rendered search context."""

    search: FinderSearch
    number: int
    displayed_total: int
    cards: tuple[FinderCard, ...]
    single_response: bool = False
    gaps: frozenset[str] = frozenset()


@dataclass(frozen=True)
class FinderWindow:
    """The observations and gaps for one scoped search."""

    search: FinderSearch
    pages: tuple[FinderPage, ...]
    gaps: frozenset[str]
    cached_pages: int


@dataclass(frozen=True)
class FinderCapturePlan:
    """An exact job's authorized subset of a reusable edition cache."""

    root: Path
    edition_id: str
    authorization_scope: str
    searches: Iterable[FinderSearch]
    plan_id: str | None = None
    resume: bool = False
    allow_unstarted_resume: bool = False
    concurrency: int = 1
    requests_per_second: float | None = None
    max_origin_requests: int = DEFAULT_REQUEST_BUDGET


class FinderPaginationUnresolved(ValueError):
    """The retained window cannot be safely advanced in this edition."""


def validate_finder_page(page, search, number, total=None):
    """Check a rendered page envelope before counting or exporting observations."""
    if page.search != search or type(page.number) is not int or page.number != number or number not in (1, 2):
        raise ValueError("delta_dental_finder_page_identity_changed")
    if type(page.displayed_total) is not int or not 0 <= page.displayed_total <= RESULT_CAP:
        raise ValueError("delta_dental_finder_count_invalid")
    if total is not None and page.displayed_total != total:
        raise ValueError("delta_dental_finder_count_changed")
    if page.single_response:
        if number != 1 or len(page.cards) > page.displayed_total:
            raise ValueError("delta_dental_finder_page_count_mismatch")
        return
    count = min(PAGE_SIZE, max(0, page.displayed_total - (number - 1) * PAGE_SIZE))
    if len(page.cards) != count:
        raise ValueError("delta_dental_finder_page_count_mismatch")


async def _acquire_page(connection, search, number, previous_page, fetch_page, parse_page, journal):
    """Reuse exact bytes or persist a newly obtained page before parsing it."""
    key = (search.selector, search.zip_code, search.radius_miles, number)
    stored = connection.execute(
        "SELECT body FROM finder_page WHERE selector=? AND zip_code=? AND radius=? AND page=?", key
    ).fetchone()
    if stored:
        raw = stored[0]
    elif journal:
        if connection.execute(
            "SELECT 1 FROM finder_attempt WHERE selector=? AND zip_code=? AND radius=? AND page=?", key
        ).fetchone():
            raise FinderPaginationUnresolved("finder_request_outcome_uncertain")

        def start_attempt():
            """Journal a request before the transport starts it."""
            with connection:
                connection.execute("INSERT INTO finder_attempt VALUES (?, ?, ?, ?)", key)

        def allow_retry():
            """Clear a rejected attempt after its publisher hold is durable."""
            with connection:
                connection.execute(
                    "DELETE FROM finder_attempt WHERE selector=? AND zip_code=? AND radius=? AND page=?", key
                )

        raw = await fetch_page(search, number, previous_page, start_attempt=start_attempt, allow_retry=allow_retry)
    else:
        raw = await fetch_page(search, number, previous_page)
    if type(raw) is not bytes or not 0 < len(raw) <= MAX_PAGE_BYTES:
        raise ValueError("delta_dental_finder_page_size_invalid")
    if stored is None:
        with connection:
            connection.execute("INSERT INTO finder_page VALUES (?, ?, ?, ?, ?)", (*key, raw))
            if journal:
                connection.execute(
                    "DELETE FROM finder_attempt WHERE selector=? AND zip_code=? AND radius=? AND page=?", key
                )
    page = parse_page(raw, search, number)
    validate_finder_page(page, search, number, previous_page.displayed_total if previous_page else None)
    return page, int(stored is not None)


def _window(search, pages, cached_pages, pagination_gap):
    """Account for unresolved scope without inferring insurer participation."""
    gaps = {pagination_gap} if pagination_gap else set()
    gaps.update(reason for page in pages for reason in page.gaps)
    if pages[0].displayed_total == 0:
        gaps.add("empty_window_unverified")
    if pages[0].displayed_total == RESULT_CAP:
        gaps.add("result_cap_reached")
    if len(pages) > 1:
        gaps.add("page_set_consistency_unverified")
    expected = (
        COMPOSITE_NETWORKS if search.selector == COMPOSITE_SELECTOR else NETWORK_LABELS.intersection({search.selector})
    )
    identities = set()
    for page in pages:
        for card in page.cards:
            if card.address is None:
                gaps.add("missing_or_ambiguous_office")
            if frozenset(card.network_labels) - NETWORK_LABELS:
                gaps.add("unmapped_card_network")
            if not card.memberships:
                gaps.add("missing_card_membership")
            if expected and expected.isdisjoint(card.memberships):
                gaps.add("selected_network_not_observed")
            practitioner = card.practitioner_id or card.practitioner_npi
            if card.provider_office_id:
                identity = ("provider_office", card.provider_office_id)
            elif practitioner and card.office_id:
                identity = (practitioner, card.office_id)
            else:
                gaps.add("missing_publisher_identity")
                continue
            if identity in identities:
                gaps.add("repeated_office_card")
            identities.add(identity)
    return FinderWindow(search, tuple(pages), frozenset(gaps), cached_pages)


def _bind_search_plan(connection, plan_id, plan, *, resume, allow_unstarted_resume):
    """Freeze the original job subset while allowing other jobs to reuse exact pages."""
    connection.execute("CREATE TABLE IF NOT EXISTS finder_plan (id TEXT PRIMARY KEY, definition TEXT NOT NULL)")
    first = connection.execute("SELECT definition FROM finder_plan LIMIT 1").fetchone()
    if first is not None and json.loads(first[0])[0] != json.loads(plan)[0]:
        raise ValueError("delta_dental_finder_capture_authorization_changed")
    existing = connection.execute("SELECT definition FROM finder_plan WHERE id=?", (plan_id,)).fetchone()
    if resume and (existing is not None or not allow_unstarted_resume):
        if existing != (plan,):
            raise ValueError("delta_dental_finder_capture_plan_changed")
        return
    if existing is not None:
        raise ValueError("delta_dental_finder_capture_plan_exists")
    with connection:
        connection.execute("INSERT INTO finder_plan VALUES (?, ?)", (plan_id, plan))


def _record_window_gap(connection, key, gap, *, page, journal):
    """Preserve a terminal gap so future runs do not repeat the same request."""
    with connection:
        connection.execute("INSERT INTO finder_gap VALUES (?, ?, ?, ?)", (*key, gap))
        if journal and gap == "finder_search_rejected":
            connection.execute(
                "DELETE FROM finder_attempt WHERE selector=? AND zip_code=? AND radius=? AND page=?",
                (*key, page),
            )


async def _window_pages(connection, search, fetch_page, parse_page, *, journal):
    """Advance only within the activated scoped search window."""
    key = (search.selector, search.zip_code, search.radius_miles)
    blocked = connection.execute(
        "SELECT reason FROM finder_gap WHERE selector=? AND zip_code=? AND radius=?", key
    ).fetchone()
    if (
        blocked is not None
        and connection.execute(
            "SELECT 1 FROM finder_page WHERE selector=? AND zip_code=? AND radius=? AND page=1", key
        ).fetchone()
        is None
    ):
        return FinderWindow(search, (), frozenset({blocked[0]}), 0)
    try:
        first, cached = await _acquire_page(connection, search, 1, None, fetch_page, parse_page, journal)
    except FinderPaginationUnresolved as error:
        _record_window_gap(connection, key, str(error), page=1, journal=journal)
        return FinderWindow(search, (), frozenset({str(error)}), 0)
    if first.single_response or first.displayed_total <= PAGE_SIZE:
        return _window(search, [first], cached, None)
    if blocked is not None:
        return _window(search, [first], cached, blocked[0])
    try:
        second, second_cached = await _acquire_page(connection, search, 2, first, fetch_page, parse_page, journal)
    except FinderPaginationUnresolved as error:
        _record_window_gap(connection, key, str(error), page=2, journal=journal)
        return _window(search, [first], cached, str(error))
    return _window(search, [first, second], cached + second_cached, None)


def _create_capture_tables(connection):
    """Prepare the page and unresolved-window tables for this retained edition."""
    connection.execute(
        "CREATE TABLE IF NOT EXISTS finder_page ("
        "selector TEXT, zip_code TEXT, radius REAL, page INTEGER, body BLOB NOT NULL, "
        "PRIMARY KEY (selector, zip_code, radius, page)) WITHOUT ROWID"
    )
    connection.execute(
        "CREATE TABLE IF NOT EXISTS finder_attempt (selector TEXT, zip_code TEXT, radius REAL, page INTEGER, "
        "PRIMARY KEY (selector, zip_code, radius, page)) WITHOUT ROWID"
    )
    connection.execute(
        "CREATE TABLE IF NOT EXISTS finder_gap ("
        "selector TEXT, zip_code TEXT, radius REAL, reason TEXT NOT NULL, "
        "PRIMARY KEY (selector, zip_code, radius)) WITHOUT ROWID"
    )


def _capture_definition(capture):
    """Freeze the ordered unique search subset authorized by this run."""
    edition_id = capture.edition_id
    if not isinstance(edition_id, str) or re.fullmatch(r"[A-Za-z0-9_-]{1,128}", edition_id) is None:
        raise ValueError("delta_dental_finder_capture_invalid")
    plan_id = edition_id if capture.plan_id is None else capture.plan_id
    if not isinstance(plan_id, str) or re.fullmatch(r"[A-Za-z0-9_-]{1,128}", plan_id) is None:
        raise ValueError("delta_dental_finder_capture_plan_invalid")
    if not isinstance(capture.authorization_scope, str) or not 0 < len(capture.authorization_scope) <= 256:
        raise ValueError("delta_dental_finder_authorization_scope_invalid")
    unique_searches = tuple(dict.fromkeys(capture.searches))
    definition = json.dumps(
        [
            capture.authorization_scope,
            [(search.selector, search.zip_code, float(search.radius_miles)) for search in unique_searches],
        ]
    )
    return unique_searches, plan_id, definition


async def _capture_parallel(unique_searches, concurrency, capture_window):
    """Bound pending work and drain started requests before propagating a failure."""
    searches = iter(unique_searches)
    workers_by_task = {}
    failure = None

    def _enqueue(worker_id):
        if (search := next(searches, None)) is not None:
            workers_by_task[asyncio.create_task(capture_window(search, worker_id))] = worker_id

    for worker_id in range(concurrency):
        _enqueue(worker_id)
    try:
        while workers_by_task:
            done, _ = await asyncio.wait(workers_by_task, return_when=asyncio.FIRST_COMPLETED)
            finished_windows = []
            for task in done:
                worker_id = workers_by_task.pop(task)
                try:
                    finished_windows.append((worker_id, task.result()))
                except Exception as error:
                    failure = failure or error
            for worker_id, window in finished_windows:
                if failure is None:
                    _enqueue(worker_id)
                yield window
        if failure is not None:
            raise failure
    finally:
        for task in workers_by_task:
            task.cancel()
        await asyncio.gather(*workers_by_task, return_exceptions=True)


async def _capture_windows(unique_searches, concurrency, capture_window):
    """Keep the legacy serial order unless parallel capture is explicitly selected."""
    if concurrency == 1:
        for search in unique_searches:
            yield await capture_window(search)
        return
    async with aclosing(_capture_parallel(unique_searches, concurrency, capture_window)) as windows:
        async for window in windows:
            yield window


async def iter_finder_windows(
    capture: FinderCapturePlan,
    *,
    fetch_page: Callable[[FinderSearch, int, FinderPage | None], Awaitable[bytes]],
    parse_page: Callable[[bytes, FinderSearch, int], FinderPage],
    cancel_check: Callable[[], Awaitable[None]],
    journal: bool = False,
) -> AsyncIterator[FinderWindow]:
    """Resume exact completed pages within the same edition and serialize origin access."""
    unique_searches, plan_id, definition = _capture_definition(capture)
    edition_id = capture.edition_id
    capture_root = capture.root
    capture_root.mkdir(mode=0o750, parents=True, exist_ok=True)
    lock = PTG2ArtifactStore(capture_root).named_lock("delta-dental-directory", "source-acquisition")
    if lock.try_acquire(blocking=False) is None:
        raise RuntimeError("delta_dental_finder_capture_already_active")
    try:
        database = capture_root / f"finder-{edition_id}.sqlite3"
        if (not capture.resume or capture.allow_unstarted_resume) and not database.exists():
            close(open_file(database, O_CREAT | O_EXCL | O_WRONLY, 0o640))
        with closing(sqlite3.connect(f"{database.resolve().as_uri()}?mode=rw", uri=True)) as connection:
            _create_capture_tables(connection)
            _bind_search_plan(
                connection,
                plan_id,
                definition,
                resume=capture.resume,
                allow_unstarted_resume=capture.allow_unstarted_resume,
            )

            async def capture_window(search, worker_id=None):
                """Read one cached or fresh window on its assigned worker."""

                async def fetch_checked(search, number, previous_page, **attempt):
                    """Check cancellation before invoking the transport."""
                    await cancel_check()
                    if worker_id is not None:
                        attempt["worker_id"] = worker_id
                    return await fetch_page(search, number, previous_page, **attempt)

                await cancel_check()
                return await _window_pages(connection, search, fetch_checked, parse_page, journal=journal)

            async with aclosing(_capture_windows(unique_searches, capture.concurrency, capture_window)) as windows:
                async for window in windows:
                    yield window
    finally:
        lock.release()
