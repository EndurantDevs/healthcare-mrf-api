# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Deterministic official NPPES listing parsing and chain selection."""

from __future__ import annotations

from datetime import date, datetime, timedelta
from html.parser import HTMLParser
from pathlib import PurePosixPath
import re
from urllib.parse import urljoin, urlsplit

from process.nppes_public_evidence_archive_contract import (
    NppesArchiveCandidate,
    archive_error,
)


NPPES_LISTING_URL = "https://download.cms.gov/nppes/NPI_Files.html"
_MONTHLY_RE = re.compile(
    r"NPPES_Data_Dissemination_"
    r"(January|February|March|April|May|June|July|August|September|October|"
    r"November|December)_([0-9]{4})_V2\.zip",
    flags=re.ASCII,
)
_WEEKLY_RE = re.compile(
    r"NPPES_Data_Dissemination_([0-9]{6})_([0-9]{6})_Weekly_V2\.zip",
    flags=re.ASCII,
)
_MAX_LISTING_BYTES = 4 * 1024 * 1024


class _HrefParser(HTMLParser):
    """Collect exact href values from one bounded official listing."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.hrefs: list[str] = []

    def handle_starttag(self, _tag: str, attrs) -> None:
        """Collect string href attributes and ignore all other markup."""

        for name, value in attrs:
            if name.lower() == "href" and type(value) is str:
                self.hrefs.append(value)


def _official_archive_url(archive_name: str) -> str:
    return f"https://download.cms.gov/nppes/{archive_name}"


def _candidate_from_url(source_url: str) -> NppesArchiveCandidate | None:
    parsed = urlsplit(source_url)
    archive_name = PurePosixPath(parsed.path).name
    expected_url = _official_archive_url(archive_name)
    if source_url != expected_url:
        return None
    monthly_match = _MONTHLY_RE.fullmatch(archive_name)
    if monthly_match is not None:
        try:
            period_start = datetime.strptime(
                f"{monthly_match.group(1)} {monthly_match.group(2)}",
                "%B %Y",
            ).date()
        except ValueError:
            raise archive_error() from None
        return NppesArchiveCandidate(
            source_url=source_url,
            archive_name=archive_name,
            archive_kind="monthly",
            period_start=period_start,
            period_end=None,
        )
    weekly_match = _WEEKLY_RE.fullmatch(archive_name)
    if weekly_match is None:
        return None
    try:
        period_start = datetime.strptime(weekly_match.group(1), "%m%d%y").date()
        period_end = datetime.strptime(weekly_match.group(2), "%m%d%y").date()
    except ValueError:
        raise archive_error() from None
    if period_end < period_start:
        raise archive_error()
    return NppesArchiveCandidate(
        source_url=source_url,
        archive_name=archive_name,
        archive_kind="weekly",
        period_start=period_start,
        period_end=period_end,
    )


def validate_nppes_archive_candidate(candidate: object) -> NppesArchiveCandidate:
    """Rebuild one exact candidate and reject direct forged instances."""

    try:
        if (
            type(candidate) is not NppesArchiveCandidate
            or type(candidate.source_url) is not str
            or type(candidate.archive_name) is not str
            or type(candidate.archive_kind) is not str
            or type(candidate.period_start) is not date
            or (
                candidate.period_end is not None
                and type(candidate.period_end) is not date
            )
        ):
            raise archive_error()
        rebuilt_candidate = _candidate_from_url(candidate.source_url)
        if rebuilt_candidate is None or rebuilt_candidate != candidate:
            raise archive_error()
    except Exception:
        normalized_error = archive_error()
    else:
        return rebuilt_candidate
    raise normalized_error


def _candidate_order(candidate: NppesArchiveCandidate) -> tuple[object, ...]:
    return (
        0 if candidate.archive_kind == "monthly" else 1,
        candidate.period_start,
        candidate.period_end or candidate.period_start,
        candidate.archive_name,
    )


def parse_official_nppes_listing(
    raw_html: object,
) -> tuple[NppesArchiveCandidate, ...]:
    """Parse exact official archive links without trusting document order."""

    try:
        if type(raw_html) is not bytes or not 1 <= len(raw_html) <= _MAX_LISTING_BYTES:
            raise archive_error()
        parser = _HrefParser()
        parser.feed(raw_html.decode("utf-8", errors="strict"))
        parser.close()
        candidates_by_url: dict[str, NppesArchiveCandidate] = {}
        for href in parser.hrefs:
            resolved_url = urljoin(NPPES_LISTING_URL, href)
            candidate = _candidate_from_url(resolved_url)
            if candidate is None:
                continue
            candidates_by_url[resolved_url] = candidate
        if not candidates_by_url:
            raise archive_error()
        parsed_candidates = tuple(
            sorted(candidates_by_url.values(), key=_candidate_order)
        )
    except Exception:
        normalized_error = archive_error()
    else:
        return parsed_candidates
    raise normalized_error


def select_nppes_release_chain(
    candidates: object,
    monthly_snapshot_date: object,
) -> tuple[NppesArchiveCandidate, ...]:
    """Select the latest monthly base and contiguous later weeklies."""

    try:
        if type(candidates) is not tuple or type(monthly_snapshot_date) is not date:
            raise archive_error()
        fixed_candidates = tuple(
            validate_nppes_archive_candidate(candidate)
            for candidate in candidates
        )
        monthly_candidates = [
            candidate
            for candidate in fixed_candidates
            if candidate.archive_kind == "monthly"
        ]
        if not monthly_candidates:
            raise archive_error()
        monthly = max(monthly_candidates, key=lambda candidate: candidate.period_start)
        if monthly.period_start > monthly_snapshot_date:
            raise archive_error()
        weekly_candidates = sorted(
            (
                candidate
                for candidate in fixed_candidates
                if candidate.archive_kind == "weekly"
                and candidate.period_end is not None
                and candidate.period_end > monthly_snapshot_date
            ),
            key=lambda candidate: (
                candidate.period_start,
                candidate.period_end,
                candidate.archive_name,
            ),
        )
        previous_end = monthly_snapshot_date
        selected_candidates: list[NppesArchiveCandidate] = [monthly]
        for weekly in weekly_candidates:
            allowed_start = previous_end + timedelta(days=1)
            if weekly.period_start != allowed_start:
                raise archive_error()
            selected_candidates.append(weekly)
            previous_end = weekly.period_end
    except Exception:
        normalized_error = archive_error()
    else:
        return tuple(selected_candidates)
    raise normalized_error


__all__ = (
    "NPPES_LISTING_URL",
    "NppesArchiveCandidate",
    "_candidate_from_url",
    "parse_official_nppes_listing",
    "select_nppes_release_chain",
    "validate_nppes_archive_candidate",
)
