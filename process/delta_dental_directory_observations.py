# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Qualify already retained rendered Delta Dental finder HTML offline."""

import argparse
import json
import os
import re
import sqlite3
from collections import Counter
from contextlib import closing, contextmanager
from dataclasses import asdict, dataclass, field
from html.parser import HTMLParser
from pathlib import Path
from tempfile import NamedTemporaryFile

from process.delta_dental_directory_capture import (
    FINDER_RADII,
    FINDER_SELECTORS,
    MAX_PAGE_BYTES,
    RESULT_CAP,
    FinderCard,
    FinderPage,
    FinderSearch,
    validate_finder_page,
)

_RESULT_HEADING = re.compile(r"(\d+) results?\s*\(within ([\d.]+) miles?\)\s+(.+)")
_LIVE_SUMMARY = re.compile(r"(\d+) dentist results? found within ([\d.]+) miles? for (.+)")
_DENTIST_GROUP_SUFFIX = " participates in"
_VOID_TAGS = frozenset("area base br col embed hr img input link meta param source track wbr".split())


@dataclass
class _Element:
    tag: str
    attributes: dict
    text_parts: list = field(default_factory=list)
    parent: "_Element | None" = None

    def text(self):
        """Read visible text in source order."""
        if self.is_hidden():
            return ""
        return " ".join(" ".join(item if isinstance(item, str) else item.text() for item in self.text_parts).split())

    def is_hidden(self):
        """Exclude independently hidden template content."""
        return self.tag in {"script", "style", "template"} or (
            "hidden" in self.attributes or self.attributes.get("aria-hidden") == "true"
        )

    def descendants(self):
        """Iterate visible descendants in result order."""
        for item in self.text_parts:
            if isinstance(item, _Element) and not item.is_hidden():
                yield item
                yield from item.descendants()


class _RenderedHTML(HTMLParser):
    def __init__(self, raw):
        super().__init__()
        self.root = _Element("", {})
        self.stack = [self.root]
        self.feed(raw.decode("utf-8"))
        self.close()

    def handle_starttag(self, tag, attrs):
        """Preserve rendered element order and ancestry."""
        node = _Element(tag, dict(attrs), parent=self.stack[-1])
        self.stack[-1].text_parts.append(node)
        if tag not in _VOID_TAGS:
            self.stack.append(node)

    def handle_startendtag(self, tag, attrs):
        """Keep self-closing inputs and tags out of the open element stack."""
        self.handle_starttag(tag, attrs)
        self.handle_endtag(tag)

    def handle_endtag(self, tag):
        """Close the nearest matching start tag."""
        for index in range(len(self.stack) - 1, 0, -1):
            if self.stack[index].tag == tag:
                del self.stack[index:]
                return

    def handle_data(self, data):
        """Retain textual evidence outside executable and template nodes."""
        if self.stack[-1].tag not in {"script", "style", "template"}:
            self.stack[-1].text_parts.append(data)


def _is_role(node, name, tags):
    """Match intrinsic and explicit HTML semantics."""
    return node.attributes.get("role", "") == name or node.tag in tags


def _label(node, by_id):
    """Read an explicit accessible label within this rendered document."""
    references = (node.attributes.get("aria-labelledby") or "").split()
    if references and all(reference in by_id for reference in references):
        return " ".join(by_id[reference].text() for reference in references)
    labelled = " ".join((node.attributes.get("aria-label") or "").split())
    if labelled or node.tag != "fieldset":
        return labelled
    return next((child.text() for child in node.descendants() if child.tag == "legend"), "")


def _card_networks(group, by_id):
    """Preserve every explicit disabled badge in this dentist panel."""
    return tuple(
        dict.fromkeys(
            _label(item, by_id) or item.text()
            for item in group.descendants()
            if _is_role(item, "button", {"button"})
            and ("disabled" in item.attributes or item.attributes.get("aria-disabled") == "true")
        )
    )


def _card(group, dentist, by_id):
    """Associate badges with the nearest unambiguous dentist and office container."""
    suffix = f", get directions for {dentist}"
    container = group.parent
    while container is not None:
        links = [
            _label(item, by_id) or item.text() for item in container.descendants() if _is_role(item, "link", {"a"})
        ]
        addresses = [name.removesuffix(suffix) for name in links if name.endswith(suffix)]
        if len(addresses) == 1:
            if sum(_label(item, by_id).endswith(_DENTIST_GROUP_SUFFIX) for item in container.descendants()) != 1:
                break
            phones = [
                item.attributes["href"].removeprefix("tel:")
                for item in container.descendants()
                if item.tag == "a" and (item.attributes.get("href") or "").startswith("tel:")
            ]
            return FinderCard(
                dentist, None, addresses[0], _card_networks(group, by_id), phone=phones[0] if len(phones) == 1 else None
            )
        if len(addresses) > 1:
            break
        container = container.parent
    return FinderCard(dentist, None, None, _card_networks(group, by_id))


def _rendered_finder(raw):
    """Extract the reported search scope and office observations from retained HTML."""
    if type(raw) is not bytes or not 0 < len(raw) <= MAX_PAGE_BYTES:
        raise ValueError("delta_dental_finder_page_size_invalid")
    nodes = list(_RenderedHTML(raw).root.descendants())
    by_id = {node.attributes["id"]: node for node in nodes if node.attributes.get("id")}
    result_headings = [
        match
        for node in nodes
        if _is_role(node, "heading", {"h1", "h2", "h3", "h4", "h5", "h6"})
        if (match := _RESULT_HEADING.fullmatch(node.text())) is not None
    ]
    if not result_headings:
        result_headings = [
            match
            for node in nodes
            if "aria-live" in node.attributes or node.attributes.get("role") == "status"
            if (match := _LIVE_SUMMARY.fullmatch(node.text())) is not None
        ]
    if len(result_headings) != 1:
        raise ValueError("delta_dental_finder_rendered_result_heading_missing_or_ambiguous")
    total_text, radius_text, selector = result_headings[0].groups()
    total = int(total_text)
    radius = float(radius_text)
    if selector not in FINDER_SELECTORS or radius not in FINDER_RADII:
        raise ValueError("delta_dental_finder_rendered_scope_unknown")
    cards = []
    for node in nodes:
        name = _label(node, by_id)
        if name.endswith(_DENTIST_GROUP_SUFFIX) and name != _DENTIST_GROUP_SUFFIX.strip():
            cards.append(_card(node, name.removesuffix(_DENTIST_GROUP_SUFFIX), by_id))
    if total and not cards:
        raise ValueError("delta_dental_finder_rendered_membership_groups_missing")
    if len(cards) > total:
        raise ValueError("delta_dental_finder_rendered_count_mismatch")
    return total, radius, selector, tuple(cards)


def parse_rendered_finder_page(raw, search, number):
    """Parse browser-qualified page bytes and recheck the visible result scope."""
    total, radius, selector, cards = _rendered_finder(raw)
    if (selector, radius) != (search.selector, search.radius_miles):
        raise ValueError("delta_dental_finder_rendered_scope_changed")
    return FinderPage(search, number, total, cards)


def qualify_rendered_finder(raw, *, include_observations=False):
    """Qualify retained rendered HTML without inferring geography, page number or identities."""
    total, radius, selector, cards = _rendered_finder(raw)
    counts = Counter(network for card in cards for network in card.office_memberships)
    gap_codes = {"geographic_coverage_unverified", "page_identity_unverified"}
    if total >= RESULT_CAP:
        gap_codes.add("result_cap_reached")
    if cards:
        gap_codes.add("publisher_identity_unverified")
    if any(card.address is None for card in cards):
        gap_codes.add("missing_or_ambiguous_office")
    if len(cards) != total:
        gap_codes.add("displayed_total_exceeds_observations")
    if any(set(card.network_labels) - card.memberships for card in cards):
        gap_codes.add("unmapped_card_network")
    if any(not card.memberships for card in cards):
        gap_codes.add("missing_card_membership")
    report_by_field = {
        "selector": selector,
        "radius_miles": radius,
        "directory_complete": False,
        "displayed_total": total,
        "card_observations": len(cards),
        "membership_observations": dict(counts),
        "gaps": sorted(gap_codes),
    }
    if include_observations:
        report_by_field["office_observations"] = [asdict(card) for card in cards]
    return report_by_field


@contextmanager
def _private_output(path):
    """Publish a complete owner-only file without replacing existing artifacts."""
    with NamedTemporaryFile(mode="w", encoding="utf-8", dir=path.parent) as staging:
        yield staging
        staging.flush()
        os.fsync(staging.fileno())
        os.link(staging.name, path)


def export_retained_capture(database, output):
    """Export per-office NDJSON observations with their exact observation scope."""
    with closing(sqlite3.connect(f"{database.resolve().as_uri()}?mode=ro", uri=True)) as connection:
        connection.execute("BEGIN")
        editions = set()
        allowed_searches = set()
        for (definition,) in connection.execute("SELECT definition FROM finder_plan"):
            edition, configured_searches = json.loads(definition)
            editions.add(edition)
            allowed_searches.update(tuple(search) for search in configured_searches)
        if len(editions) != 1:
            raise ValueError("delta_dental_finder_capture_edition_invalid")
        edition = editions.pop()
        retained_pages = connection.execute(
            "SELECT selector, zip_code, radius, page, body FROM finder_page ORDER BY selector, zip_code, radius, page"
        )
        counts = Counter()
        gaps_by_reason = dict(connection.execute("SELECT reason, count(*) FROM finder_gap GROUP BY reason"))
        with _private_output(output) as export_stream:
            for selector, zip_code, radius, number, raw in retained_pages:
                if (selector, zip_code, radius) not in allowed_searches:
                    raise ValueError("delta_dental_finder_capture_scope_changed")
                search = FinderSearch(selector, zip_code, radius)
                try:
                    page = parse_rendered_finder_page(raw, search, number)
                    validate_finder_page(page, search, number)
                except ValueError:
                    counts["unparseable_pages"] += 1
                    continue
                counts["pages"] += 1
                for ordinal, card in enumerate(page.cards, 1):
                    observation_by_field = {
                        **asdict(card),
                        "memberships": sorted(card.office_memberships),
                        "edition": edition,
                        "search": asdict(search),
                        "page": number,
                        "ordinal": ordinal,
                        "observed_scope_only": True,
                    }
                    export_stream.write(json.dumps(observation_by_field, ensure_ascii=False) + "\n")
                    counts["card_observations"] += 1
                    counts["office_observations"] += int(card.address is not None)
        return {"directory_complete": False, "pagination_gaps": gaps_by_reason, **counts}


def main():
    """Export offline findings through the requested private artifact path."""
    parser = argparse.ArgumentParser(description=__doc__)
    sources = parser.add_mutually_exclusive_group(required=True)
    sources.add_argument("--rendered-html", type=Path)
    sources.add_argument("--capture", type=Path, help="retained finder capture database")
    parser.add_argument("--output", type=Path, help="new private output file")
    options = parser.parse_args()
    if options.capture is not None:
        if options.output is None:
            parser.error("--capture requires --output")
        report_by_field = export_retained_capture(options.capture, options.output)
    else:
        with options.rendered_html.open("rb") as source:
            report_by_field = qualify_rendered_finder(
                source.read(MAX_PAGE_BYTES + 1), include_observations=options.output is not None
            )
        if options.output is not None:
            with _private_output(options.output) as output:
                json.dump(report_by_field.pop("office_observations"), output)
    print(json.dumps(report_by_field, sort_keys=True))


if __name__ == "__main__":
    main()
