# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Resume a descending timestamp traversal at a verified, overlapping boundary."""

import hashlib
import re
from urllib.parse import urlencode, urljoin, urlsplit

from process.provider_directory_fhir_response import strict_search_next_link
from process.provider_directory_source_selection import fhir_census_pagination_url_allowed, has_fhir_continuation_query
from process.provider_directory_time_partition_primitives import format_utc_instant, parse_utc_instant

KEY = "descending_timestamp_recovery_v1"
FETCH_MODE = "descending_timestamp_recovery"
PAGE_SIZE = 20
SEGMENT_PAGES = 500
# shortcut: larger timestamp ties fail closed; add another partition when a source needs them.
MAX_TIES = 1000
_DIGEST = re.compile(r"[0-9a-f]{64}")
_ID = re.compile(r"[A-Za-z0-9.-]{1,64}")
_INSTANT = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]{1,6})?(?:Z|\+00:00)")
_IDENTITY = {
    "canonical_api_base",
    "source_id",
    "dataset_id",
    "acquisition_root_run_id",
    "resource_type",
    "start_url_hash",
    "source_scope_hash",
}
_FIELDS = {
    "original",
    "identity",
    "upper",
    "overlap_timestamp",
    "remaining_overlap",
    "lowest",
    "closed",
    "lowest_hashes",
    "segment_start_pages",
    "segment_pages",
    "pages",
    "observations",
    "action",
    "url",
    "terminal_request_url",
    "terminal_request_sha256",
}


def marked(proof):
    """Recognize tagged recovery records, including unsupported versions."""
    return isinstance(proof, dict) and any(str(key).startswith(FETCH_MODE) for key in proof)


def completion_receipt(proof):
    """Expose aggregate traversal evidence while retaining request URLs and matched IDs only in the checkpoint."""
    identity = proof[KEY]["identity"]
    state = _state(proof, identity)
    if state["action"] != "terminal":
        return {"version": 1, "complete": False}
    original = state["original"]
    return {
        "version": 1,
        "complete": True,
        "identity": dict(identity),
        "original_pages": original["pages"],
        "original_rows": original["rows"],
        "recovery_pages": state["pages"],
        "recovery_observations": state["observations"],
        "original_request_sha256": hashlib.sha256(original["url"].encode()).hexdigest(),
        "terminal_request_sha256": state["terminal_request_sha256"],
        "initial_upper": original["upper"],
        "initial_overlap": original["overlap_timestamp"],
        "final_upper": state["upper"],
    }


_RECEIPT_FIELDS = {
    "version",
    "complete",
    "identity",
    "original_pages",
    "original_rows",
    "recovery_pages",
    "recovery_observations",
    "original_request_sha256",
    "terminal_request_sha256",
    "initial_upper",
    "initial_overlap",
    "final_upper",
}


def diagnostic_is_complete(diagnostic, *, expected=None):
    """Verify the compact completion receipt and scope before accepting a recovery diagnostic."""
    if not isinstance(diagnostic, dict):
        return False
    mode = diagnostic.get("fetch_mode")
    if not marked(diagnostic):
        return not (isinstance(mode, str) and mode.startswith(FETCH_MODE))
    try:
        receipt = diagnostic[KEY]
        if (
            {field for field in diagnostic if str(field).startswith(FETCH_MODE)} != {KEY}
            or mode != FETCH_MODE
            or type(receipt) is not dict
            or receipt.keys() != _RECEIPT_FIELDS
            or type(receipt["version"]) is not int
            or receipt["version"] != 1
            or receipt["complete"] is not True
            or diagnostic["complete"] is not True
            or diagnostic["error"] is not None
            or diagnostic["bounded"] is not False
            or diagnostic["next_url_remaining"] is not False
        ):
            return False
        identity = receipt["identity"]
        _check_identity(identity)
        if expected is not None and (
            type(expected) is not dict
            or any(
                field not in identity or identity[field] != expected_value for field, expected_value in expected.items()
            )
        ):
            return False
        initial = receipt["initial_upper"]
        overlap = receipt["initial_overlap"]
        final = receipt["final_upper"]
        if not (
            all(_instant(stamp) == stamp for stamp in (initial, overlap, final))
            and overlap < initial
            and final <= initial
            and _is_sha256(receipt["original_request_sha256"])
            and _is_sha256(receipt["terminal_request_sha256"])
        ):
            return False
        original_pages, original_rows = receipt["original_pages"], receipt["original_rows"]
        recovery_pages, observations = receipt["recovery_pages"], receipt["recovery_observations"]
        pages_fetched, rows_fetched = diagnostic["pages_fetched"], diagnostic["rows_fetched"]
        return (
            all(
                type(count) is int
                for count in (original_pages, original_rows, recovery_pages, observations, pages_fetched, rows_fetched)
            )
            and original_pages > 0
            and 0 < original_rows <= PAGE_SIZE * original_pages
            and (1 <= recovery_pages <= SEGMENT_PAGES if final == initial else recovery_pages >= 2)
            and max(1, recovery_pages - 1) <= observations <= PAGE_SIZE * recovery_pages
            and original_pages + recovery_pages == pages_fetched
            and original_rows + observations == rows_fetched
        )
    except KeyError, TypeError, ValueError:
        return False


def _require(ok, reason):
    if not ok:
        raise ValueError("timestamp_recovery_" + reason)


def _instant(value):
    # fromisoformat silently drops fractional digits beyond microseconds.
    _require(type(value) is str and _INSTANT.fullmatch(value), "timestamp")
    return format_utc_instant(parse_utc_instant(value))


def _is_sha256(value):
    return type(value) is str and _DIGEST.fullmatch(value) is not None


def _hashes(values):
    return (
        type(values) is dict
        and len(values) <= MAX_TIES
        and all(type(key) is str and _ID.fullmatch(key) and _is_sha256(digest) for key, digest in values.items())
    )


def _fresh(identity, upper):
    query = urlencode((("_count", PAGE_SIZE), ("_lastUpdated", "lt" + upper)))
    return f"{identity['canonical_api_base']}/{identity['resource_type']}?{query}"


def _issued(identity, url):
    return (
        type(url) is str
        and fhir_census_pagination_url_allowed(
            api_base=identity["canonical_api_base"], resource_types=(identity["resource_type"],), next_url=url
        )
        and has_fhir_continuation_query(url)
    )


def _check_identity(identity):
    _require(
        type(identity) is dict
        and identity.keys() == _IDENTITY
        and all(type(value) is str and value and value == value.strip() for value in identity.values())
        and _is_sha256(identity["source_scope_hash"])
        and _is_sha256(identity["start_url_hash"])
        and _ID.fullmatch(identity["resource_type"]),
        "identity",
    )
    base = urlsplit(identity["canonical_api_base"])
    _require(
        base.scheme == "https"
        and base.hostname
        and base.username is None
        and base.password is None
        and not base.query
        and not base.fragment
        and not any(ord(character) < 33 for character in identity["canonical_api_base"]),
        "base",
    )


def begin(
    completeness, *, identity, original_url, original_pages, original_rows, upper, overlap_timestamp, overlap_hashes
):
    """Start only after the caller verifies the original cursor, prefix order and retained overlap."""
    _check_identity(identity)
    _require(
        type(completeness) is dict and not any(k.startswith("descending_timestamp_recovery") for k in completeness),
        "already_started",
    )
    _require(
        _issued(identity, original_url)
        and type(original_pages) is int
        and original_pages > 0
        and type(original_rows) is int
        and 0 < original_rows <= PAGE_SIZE * original_pages,
        "original",
    )
    upper, overlap_timestamp = _instant(upper), _instant(overlap_timestamp)
    _require(upper > overlap_timestamp and _hashes(overlap_hashes) and overlap_hashes, "boundary")
    return {
        **completeness,
        KEY: {
            "identity": dict(identity),
            "original": {
                "url": original_url,
                "pages": original_pages,
                "rows": original_rows,
                "upper": upper,
                "overlap_timestamp": overlap_timestamp,
            },
            "upper": upper,
            "overlap_timestamp": overlap_timestamp,
            "remaining_overlap": dict(overlap_hashes),
            "closed": None,
            "lowest": None,
            "lowest_hashes": {},
            "segment_start_pages": 0,
            "segment_pages": 0,
            "pages": 0,
            "observations": 0,
            "action": "fresh_lt",
            "url": _fresh(identity, upper),
            "terminal_request_url": None,
            "terminal_request_sha256": None,
        },
    }


def _state(completeness, identity):
    """Restore the bound checkpoint only when its source, time bounds and ties agree."""
    _check_identity(identity)
    _require(
        type(completeness) is dict
        and {k for k in completeness if k.startswith("descending_timestamp_recovery")} == {KEY},
        "version",
    )
    state = completeness[KEY]
    _require(type(state) is dict and state.keys() == _FIELDS and state["identity"] == identity, "scope")
    original = state["original"]
    _require(
        type(original) is dict
        and original.keys() == {"url", "pages", "rows", "upper", "overlap_timestamp"}
        and _issued(identity, original["url"])
        and type(original["pages"]) is int
        and type(original["rows"]) is int
        and original["pages"] > 0
        and 0 < original["rows"] <= PAGE_SIZE * original["pages"],
        "original",
    )
    upper, overlap = state["upper"], state["overlap_timestamp"]
    _require(_instant(upper) == upper and _instant(overlap) == overlap and upper > overlap, "bounds")
    lowest, closed = state["lowest"], state["closed"]
    _require(lowest is None or (_instant(lowest) == lowest and lowest < upper), "lowest")
    _require(closed is None or (lowest and _instant(closed) == closed and lowest < closed < upper), "closed")
    remaining, seen = state["remaining_overlap"], state["lowest_hashes"]
    _require(
        _hashes(remaining)
        and _hashes(seen)
        and bool(seen) == (lowest is not None)
        and (not remaining or lowest is None or lowest >= overlap)
        and (remaining or (lowest is not None and lowest <= overlap))
        and (lowest != overlap or remaining.keys().isdisjoint(seen)),
        "ties",
    )
    initial, initial_overlap = original["upper"], original["overlap_timestamp"]
    _require(
        _instant(initial) == initial and _instant(initial_overlap) == initial_overlap and initial_overlap < initial,
        "original_bounds",
    )
    if state["segment_start_pages"] == 0:
        _require((upper, overlap) == (initial, initial_overlap), "initial_segment")
    else:
        _require(upper < initial and overlap <= initial_overlap, "advanced_segment")
    _validate_progress(state)
    return state


def _validate_progress(state):
    """Check the saved counts and request against the current segment and action."""
    pages, observations, segment = state["pages"], state["observations"], state["segment_pages"]
    is_terminal = state["action"] == "terminal"
    _require(
        type(pages) is int
        and type(segment) is int
        and type(observations) is int
        and type(state["segment_start_pages"]) is int
        and 0 <= segment <= pages
        and state["segment_start_pages"] == pages - segment
        and pages - int(is_terminal) <= observations <= PAGE_SIZE * pages
        and observations >= len(state["lowest_hashes"]),
        "counters",
    )
    if is_terminal:
        request = state["terminal_request_url"]
        _require(
            0 < segment <= SEGMENT_PAGES
            and not state["remaining_overlap"]
            and state["url"] is None
            and (
                request == _fresh(state["identity"], state["upper"])
                if segment == 1
                else _issued(state["identity"], request)
            )
            and state["terminal_request_sha256"] == hashlib.sha256(request.encode()).hexdigest(),
            "terminal",
        )
    else:
        _require(
            segment < SEGMENT_PAGES
            and state["terminal_request_url"] is None
            and state["terminal_request_sha256"] is None,
            "progress",
        )
        if state["action"] == "fresh_lt":
            _require(
                not segment and state["lowest"] is None and state["url"] == _fresh(state["identity"], state["upper"]),
                "fresh",
            )
        else:
            _require(
                state["action"] == "issued"
                and segment > 0
                and state["lowest"] is not None
                and _issued(state["identity"], state["url"]),
                "issued",
            )


def validate_checkpoint(completeness, *, identity, next_url, pages_processed, rows_processed, complete):
    """Verify the saved strategy against the ordinary checkpoint, including on terminal reuse."""
    state = _state(completeness, identity)
    _require(
        type(complete) is bool
        and complete == (state["action"] == "terminal")
        and state["url"] == next_url
        and type(pages_processed) is int
        and state["original"]["pages"] + state["pages"] == pages_processed
        and type(rows_processed) is int
        and state["original"]["rows"] + state["observations"] == rows_processed,
        "checkpoint",
    )
    return state


def _roll(state):
    # A strictly older observed row closes the previous timestamp's entire tie group.
    upper = state["closed"]
    _require(not state["remaining_overlap"] and upper is not None and state["lowest_hashes"], "no_safe_rollover")
    return {
        **state,
        "upper": upper,
        "overlap_timestamp": state["lowest"],
        "remaining_overlap": dict(state["lowest_hashes"]),
        "closed": None,
        "lowest": None,
        "lowest_hashes": {},
        "segment_start_pages": state["pages"],
        "segment_pages": 0,
        "action": "fresh_lt",
        "url": _fresh(state["identity"], upper),
    }


def reanchor(completeness, *, identity):
    """Start a new retry from a closed boundary after validating the outer checkpoint."""
    state = _state(completeness, identity)
    _require(state["action"] != "terminal", "terminal_retry")
    return completeness if state["action"] == "fresh_lt" else {**completeness, KEY: _roll(state)}


def _matched_entries(identity, requested_url, link_base_url, bundle, trusted_next, content_hashes):
    """Validate the searchset and distinguish an issued next link from actual end of feed."""
    _require(
        type(bundle) is dict and bundle.get("resourceType") == "Bundle" and bundle.get("type") == "searchset",
        "searchset",
    )
    links = bundle.get("link", [])
    _require(
        type(links) is list
        and all(
            type(link) is dict and type(link.get("relation")) is str and type(link.get("url")) is str and link["url"]
            for link in links
        ),
        "links",
    )
    raw_next = strict_search_next_link(bundle)
    if raw_next is not None:
        # urljoin strips controls and can collapse a path segment before the scope check.
        _require("\\" not in raw_next and all(ord(character) >= 33 for character in raw_next), "next")
    resolved_next = urljoin(link_base_url, raw_next) if raw_next is not None else None
    _require(
        resolved_next == trusted_next
        and (
            trusted_next is None
            or (_issued(identity, trusted_next) and trusted_next not in (requested_url, link_base_url))
        ),
        "next",
    )
    entries = bundle.get("entry", [])
    _require(
        type(entries) is list
        and len(entries) <= PAGE_SIZE
        and (entries or trusted_next is None)
        and _hashes(content_hashes)
        and len(entries) == len(content_hashes),
        "entries",
    )
    return entries


def _record_ordered_entries(state, entries, content_hashes):
    """Apply the page in received order, closing groups only on a strictly older timestamp."""
    remaining, seen = dict(state["remaining_overlap"]), dict(state["lowest_hashes"])
    page_ids = set()
    for entry in entries:
        _require(
            type(entry) is dict
            and type(entry.get("resource")) is dict
            and type(entry.get("search", {})) is dict
            and entry.get("search", {}).get("mode", "match") == "match",
            "entry",
        )
        resource = entry["resource"]
        resource_id = resource.get("id")
        _require(
            type(resource_id) is str
            and resource_id in content_hashes
            and resource_id not in page_ids
            and resource.get("resourceType") == state["identity"]["resource_type"]
            and type(resource.get("meta")) is dict,
            "match",
        )
        page_ids.add(resource_id)
        stamp = _instant(resource["meta"].get("lastUpdated"))
        _require(stamp < state["upper"] and (state["lowest"] is None or stamp <= state["lowest"]), "order")
        if resource_id in remaining:
            _require(
                stamp == state["overlap_timestamp"] and content_hashes[resource_id] == remaining.pop(resource_id),
                "overlap",
            )
        _require(not remaining or stamp >= state["overlap_timestamp"], "missing_overlap")
        if stamp != state["lowest"]:
            state["closed"], state["lowest"], seen = state["lowest"], stamp, {}
        _require(resource_id not in seen and len(seen) < MAX_TIES, "tie_limit_or_duplicate")
        seen[resource_id] = content_hashes[resource_id]
    state.update(
        remaining_overlap=remaining,
        lowest_hashes=seen,
        segment_pages=state["segment_pages"] + 1,
        pages=state["pages"] + 1,
        observations=state["observations"] + len(entries),
    )


def advance(completeness, *, identity, requested_url, payload, trusted_next, content_hashes, link_base_url=None):
    """Validate one successful raw response before atomically saving its observations and new checkpoint."""
    state_by_field = dict(_state(completeness, identity))
    _require(state_by_field["action"] != "terminal" and requested_url == state_by_field["url"], "request")
    entries = _matched_entries(
        identity,
        requested_url,
        requested_url if link_base_url is None else link_base_url,
        payload,
        trusted_next,
        content_hashes,
    )
    _record_ordered_entries(state_by_field, entries, content_hashes)
    if trusted_next is None:
        _require(not state_by_field["remaining_overlap"], "missing_overlap")
        state_by_field.update(
            action="terminal",
            url=None,
            terminal_request_url=requested_url,
            terminal_request_sha256=hashlib.sha256(requested_url.encode()).hexdigest(),
        )
    elif state_by_field["segment_pages"] == SEGMENT_PAGES:
        state_by_field = _roll(state_by_field)
    else:
        state_by_field.update(action="issued", url=trusted_next)
    return {**completeness, KEY: state_by_field}
