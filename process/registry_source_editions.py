# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Discover CMS edition candidates without accepting or publishing source data.

Catalog hashes describe retained HTML, never the linked artifact. Preparing an
edition still requires downloading and validating its bytes and source header.
"""

from __future__ import annotations

import asyncio
import hashlib
import re
from dataclasses import dataclass, field
from datetime import date, datetime
from html.parser import HTMLParser
from urllib.parse import urljoin, urlsplit

from process.registry_source_fetch import RegistrySourceFetchResult, RegistrySourceFetchSpec, fetch_registry_source

MAX_CATALOG_BYTES = 1024 * 1024
CATALOG_PARSER_VERSION = "cms-edition-catalog-v1"
CATALOG_URLS = {
    "commercial-mlr": "https://www.cms.gov/marketplace/resources/data/medical-loss-ratio-data-systems-resources",
    "plan-finder": "https://www.cms.gov/marketplace/resources/data/healthcaregov-plan-finder-data",
}
_MLR_LABEL = re.compile(r"Public Use File for (20\d{2})\b", re.I)
_REPORTING_YEAR = re.compile(r"(20\d{2}) Reporting Year\Z", re.I)
_PLAN_LABEL = re.compile(r"Quarter ([1-4]) (20\d{2}) HIOS and RBIS Data\b", re.I)
_PLAN_RELEASE = re.compile(r"Quarter ([1-4]) (20\d{2}) Data Release\b(?:\s*\((FINAL|PRELIMINARY)\))?", re.I)
_AS_OF = re.compile(r"\bas of ([A-Za-z]+ \d{1,2}, \d{4})\b", re.I)


class RegistrySourceEditionDiscoveryError(ValueError):
    """An unavailable, ambiguous or unsupported catalog; contains no source data."""

    def __init__(self):
        super().__init__("Registry source edition catalog is unavailable or invalid")


@dataclass(frozen=True)
class RegistrySourceEditionPolicy:
    edition_policy: str
    reporting_year: int | None = None
    include_preliminary: bool = False

    def __post_init__(self):
        if (
            type(self.edition_policy) is not str
            or self.edition_policy not in {"latest-verified", "fixed-year"}
            or type(self.include_preliminary) is not bool
            or (
                self.reporting_year is not None
                and (type(self.reporting_year) is not int or not 2010 <= self.reporting_year <= 2100)
            )
            or (self.edition_policy == "fixed-year" and self.reporting_year is None)
        ):
            raise RegistrySourceEditionDiscoveryError()


@dataclass(frozen=True)
class RegistrySourceEditionCandidate:
    source_id: str
    edition_id: str
    source_url: str
    reporting_year: int
    quarter: int | None
    release: str | None
    as_of_date: date | None
    source_system: str = field(default="cms", init=False)
    artifact_sha256: str | None = field(default=None, init=False)
    published_at: datetime | None = field(default=None, init=False)


@dataclass(frozen=True)
class RegistrySourceEditionDiscovery:
    candidate: RegistrySourceEditionCandidate
    catalog: RegistrySourceFetchResult


@dataclass
class _Item:
    text: list[str] = field(default_factory=list)
    title: str | None = None


class _CatalogParser(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.heading = ""
        self.heading_text = None
        self.items = []
        self.anchor = None
        self.links = []
        self.reporting_years = set()
        self.plan_periods = set()

    def handle_starttag(self, tag, attrs):
        """Retain release ancestry and cap nested lists and link counts."""
        if tag == "br" and self.items:
            if self.items[-1].title is None:
                self.items[-1].title = _text(self.items[-1].text)
            self.items[-1].text.append(" ")
        if tag == "h2":
            self.heading_text = []
        elif tag == "li":
            if len(self.items) >= 32:
                raise RegistrySourceEditionDiscoveryError()
            self.items.append(_Item())
        elif tag == "a":
            if self.anchor is not None or len(self.links) >= 1000:
                raise RegistrySourceEditionDiscoveryError()
            self.anchor = [dict(attrs).get("href", ""), [], self.heading, tuple(self.items)]

    def handle_data(self, data):
        """Collect anchor labels separately from direct release-list text."""
        if self.heading_text is not None:
            self.heading_text.append(data)
        if self.items:
            self.items[-1].text.append(data)
        if self.anchor is not None:
            self.anchor[1].append(data)

    def handle_endtag(self, tag):
        """Close captured labels while keeping their release context available."""
        if tag == "h2" and self.heading_text is not None:
            self.heading = _text(self.heading_text)
            self.heading_text = None
            match = _REPORTING_YEAR.fullmatch(self.heading)
            if match:
                self.reporting_years.add(int(match[1]))
        elif tag == "li" and self.items:
            match = _PLAN_RELEASE.match(_text(self.items[-1].text))
            if match:
                self.plan_periods.add((int(match[2]), int(match[1])))
            self.items.pop()
        elif tag == "a" and self.anchor is not None:
            self.links.append(self.anchor)
            self.anchor = None


def _text(parts):
    return " ".join("".join(parts).split())


def registry_source_catalog_spec(source_id):
    """The closed catalog identity allows reuse of verified HTTP validators."""
    if type(source_id) is not str or source_id not in CATALOG_URLS:
        raise RegistrySourceEditionDiscoveryError()
    return RegistrySourceFetchSpec(
        "cms",
        source_id,
        "edition-catalog",
        CATALOG_URLS[source_id],
        CATALOG_PARSER_VERSION,
        None,
        None,
        MAX_CATALOG_BYTES,
    )


def _artifact_url(source_id, href):
    url = urljoin(CATALOG_URLS[source_id], href)
    RegistrySourceFetchSpec("cms", source_id, "candidate", url, CATALOG_PARSER_VERSION, None, None, 1)
    if not urlsplit(url).path.lower().endswith(".zip"):
        raise RegistrySourceEditionDiscoveryError()
    return url


def _as_of(text):
    matches = set(_AS_OF.findall(text))
    if len(matches) > 1:
        raise RegistrySourceEditionDiscoveryError()
    return datetime.strptime(matches.pop(), "%B %d, %Y").date() if matches else None


def _release(text):
    preliminary = bool(re.search(r"\bpreliminary\b", text, re.I))
    final = bool(re.search(r"\bfinal\b", text, re.I))
    if preliminary and final:
        raise RegistrySourceEditionDiscoveryError()
    return "preliminary" if preliminary else "final" if final else "unspecified"


def _candidate(source_id, link):
    href, label_parts, heading, list_items = link
    label = _text(label_parts)
    if source_id == "commercial-mlr":
        match = _MLR_LABEL.match(label)
        if match is None:
            if label.lower().startswith("public use file for "):
                raise RegistrySourceEditionDiscoveryError()
            return None
        year = int(match[1])
        if _REPORTING_YEAR.fullmatch(heading) is None or int(_REPORTING_YEAR.fullmatch(heading)[1]) != year:
            raise RegistrySourceEditionDiscoveryError()
        as_of = _as_of(_text(list_items[-1].text) if list_items else label)
        edition_id = f"{year}-as-of-{as_of.isoformat()}" if as_of else str(year)
        quarter, release = None, None
    else:
        match = _PLAN_LABEL.match(label)
        if match is None:
            if label.lower().startswith("quarter ") and "hios and rbis data" in label.lower():
                raise RegistrySourceEditionDiscoveryError()
            return None
        quarter, year = int(match[1]), int(match[2])
        contexts = [list_item for list_item in list_items if _PLAN_RELEASE.match(_text(list_item.text))]
        if len(contexts) != 1:
            raise RegistrySourceEditionDiscoveryError()
        context_title = contexts[0].title if contexts[0].title is not None else _text(contexts[0].text)
        context_match = _PLAN_RELEASE.fullmatch(context_title)
        if context_match is None or (int(context_match[1]), int(context_match[2])) != (quarter, year):
            raise RegistrySourceEditionDiscoveryError()
        release = _release((context_match[3] or "") + " " + label)
        edition_id, as_of = f"{year}-q{quarter}-{release}", None
    if not 2010 <= year <= 2100:
        raise RegistrySourceEditionDiscoveryError()
    return RegistrySourceEditionCandidate(
        source_id,
        edition_id,
        _artifact_url(source_id, href),
        year,
        quarter,
        release,
        as_of,
    )


def parse_registry_source_editions(source_id, html):
    """Parse only recognized CMS release labels and their matching context."""
    registry_source_catalog_spec(source_id)
    try:
        if type(html) is not bytes or not 1 <= len(html) <= MAX_CATALOG_BYTES:
            raise RegistrySourceEditionDiscoveryError()
        parser = _CatalogParser()
        parser.feed(html.decode("utf-8"))
        parser.close()
        if parser.anchor is not None or parser.heading_text is not None:
            raise RegistrySourceEditionDiscoveryError()
        candidates_by_edition = {}
        for link in parser.links:
            candidate = _candidate(source_id, link)
            if candidate is not None:
                previous = candidates_by_edition.setdefault(candidate.edition_id, candidate)
                if previous != candidate:
                    raise RegistrySourceEditionDiscoveryError()
        if not candidates_by_edition:
            raise RegistrySourceEditionDiscoveryError()
        declared_periods = (
            {(year, 0) for year in parser.reporting_years} if source_id == "commercial-mlr" else parser.plan_periods
        )
        parsed_periods = {
            (candidate.reporting_year, candidate.quarter or 0) for candidate in candidates_by_edition.values()
        }
        if not declared_periods or max(declared_periods) > max(parsed_periods):
            raise RegistrySourceEditionDiscoveryError()
        return tuple(candidates_by_edition[key] for key in sorted(candidates_by_edition))
    except Exception:
        raise RegistrySourceEditionDiscoveryError() from None


def select_registry_source_edition(candidates, policy):
    """Select a reporting period; a verified catalog is not artifact acceptance."""
    if (
        type(policy) is not RegistrySourceEditionPolicy
        or type(candidates) is not tuple
        or any(type(candidate) is not RegistrySourceEditionCandidate for candidate in candidates)
    ):
        raise RegistrySourceEditionDiscoveryError()
    eligible_candidates = [
        candidate
        for candidate in candidates
        if (policy.reporting_year is None or candidate.reporting_year == policy.reporting_year)
        and (policy.include_preliminary or candidate.release != "preliminary")
    ]
    if not eligible_candidates or len({candidate.source_id for candidate in candidates}) != 1:
        raise RegistrySourceEditionDiscoveryError()
    if eligible_candidates[0].source_id == "commercial-mlr":
        latest_year = max(candidate.reporting_year for candidate in eligible_candidates)
        revisions = [candidate for candidate in eligible_candidates if candidate.reporting_year == latest_year]
        if len({candidate.as_of_date is None for candidate in revisions}) > 1:
            raise RegistrySourceEditionDiscoveryError()
    return max(
        eligible_candidates,
        key=lambda candidate: (
            candidate.reporting_year,
            candidate.quarter or 0,
            {None: 0, "preliminary": 0, "unspecified": 1, "final": 2}[candidate.release],
            candidate.as_of_date or date.min,
        ),
    )


def _read_catalog(source_id, catalog):
    with catalog.artifact_path.open("rb") as stream:
        html = stream.read(MAX_CATALOG_BYTES + 1)
    if (
        len(html) != catalog.receipt.artifact_bytes
        or hashlib.sha256(html).hexdigest() != catalog.receipt.artifact_sha256
    ):
        raise RegistrySourceEditionDiscoveryError()
    return parse_registry_source_editions(source_id, html)


async def discover_registry_source_edition(source_id, policy, artifact_dir, *, previous=None, _test_transport=None):
    """Fetch a bounded official catalog and return one candidate plus raw proof.

    Supply the previous catalog receipt to reuse verified conditional fetching.
    Failures do not update an accepted edition, recipe, or manual/custom data.
    """
    if type(policy) is not RegistrySourceEditionPolicy:
        raise RegistrySourceEditionDiscoveryError()
    try:
        catalog = await fetch_registry_source(
            registry_source_catalog_spec(source_id),
            artifact_dir,
            previous=previous,
            _test_transport=_test_transport,
        )
        candidates = await asyncio.to_thread(_read_catalog, source_id, catalog)
        return RegistrySourceEditionDiscovery(select_registry_source_edition(candidates, policy), catalog)
    except Exception:
        raise RegistrySourceEditionDiscoveryError() from None
