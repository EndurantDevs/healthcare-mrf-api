# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Traverse issued search caches without trusting the gateway's page totals."""

from urllib.parse import parse_qsl, urlencode, urljoin, urlsplit

API_BASE = "https://api.bluecrossmn.com/fhir/provider"
CACHED_RESOURCES = frozenset({"InsurancePlan", "Location", "HealthcareService"})
STRATEGY_VERSION = "provider-directory-fhir-minnesota-time-cache-v1"
INITIAL_PAGE_COUNT = 50
PAGE_COUNT_CAP = 500
_QUERY_NAMES = frozenset(
    {"_count", "_getpages", "_getpagesoffset", "_pretty", "_bundletype", "_summary", "_total", "_format"}
)


def _query(url, resource):
    parsed = urlsplit(url)
    pairs = parse_qsl(parsed.query, keep_blank_values=True)
    query_by_name = dict(pairs)
    if (
        parsed.scheme != "https"
        or parsed.netloc != urlsplit(API_BASE).netloc
        or parsed.path not in {urlsplit(API_BASE).path, urlsplit(API_BASE).path + "/" + resource}
        or parsed.fragment
        or len(pairs) != len(query_by_name)
        or set(query_by_name) - _QUERY_NAMES
    ):
        raise ValueError("minnesota_cache_url_invalid")
    return query_by_name


def start_url(directory, url):
    """Zero requests the native first page, including its search-cache handle."""
    if urlsplit(url).path.rsplit("/", 1)[-1] in CACHED_RESOURCES:
        return directory._url_with_replaced_query_item(url, "_count", "0")
    return url


def page_error(bundle_payload, resource, current_url):
    """Refuse malformed pages and evidence contradicting cached completion."""
    links = bundle_payload.get("link", [])
    if not isinstance(links, list) or any(not isinstance(link, dict) for link in links):
        return "minnesota_cache_links_invalid"
    next_links = [link.get("url") for link in links if link.get("relation") == "next"]
    if len(next_links) > 1 or any(not isinstance(url, str) or not url.strip() for url in next_links):
        return "minnesota_cache_links_invalid"
    entries = bundle_payload.get("entry", [])
    if not isinstance(entries, list) or any(
        not isinstance(entry, dict)
        or not isinstance(entry.get("resource"), dict)
        or entry["resource"].get("resourceType") != resource
        or not isinstance(entry["resource"].get("id"), str)
        or not entry["resource"]["id"]
        for entry in entries
    ):
        return "minnesota_cache_resource_invalid"
    try:
        query = _query(current_url, resource)
        is_initial = query.get("_count") == "0"
        if "_getpages" in query:
            offset, count = int(query["_getpagesoffset"]), int(query["_count"])
            total = bundle_payload.get("total")
            if total is not None:
                if type(total) is not int or total < 0:
                    return "minnesota_cache_total_invalid"
                processed = offset + len(entries)
                if total < processed:
                    return "minnesota_cache_total_below_processed"
                if len(entries) < count and not next_links and total > processed:
                    return "minnesota_cache_short_page_before_total"
    except (KeyError, ValueError) as exc:
        return "minnesota_cache_page_invalid:" + str(exc)
    if is_initial and (bundle_payload.get("type") != "searchset" or (not entries and bundle_payload.get("total") != 0)):
        return "minnesota_cache_initial_page_missing"
    return None


def next_url(current_url, next_link, resource, page_count, entry_count):
    """Advance only a server-issued cache, using the verified 500-row cap."""
    if (
        resource not in CACHED_RESOURCES
        or type(entry_count) is not int
        or type(page_count) is not int
        or not 0 < page_count <= PAGE_COUNT_CAP
    ):
        raise ValueError("minnesota_cache_page_invalid")
    current = _query(current_url, resource)
    if "_getpages" not in current:
        if current.get("_count") != "0" or "_getpagesoffset" in current or not 0 <= entry_count <= INITIAL_PAGE_COUNT:
            raise ValueError("minnesota_cache_initial_page_invalid")
        if not next_link:
            if entry_count == INITIAL_PAGE_COUNT:
                raise ValueError("minnesota_cache_handle_missing")
            return None
        issued = _query(urljoin(current_url, next_link), resource)
        cache = issued.get("_getpages")
        if issued.get("_getpagesoffset") != str(entry_count) or issued.get("_count") != str(INITIAL_PAGE_COUNT):
            raise ValueError("minnesota_cache_initial_cursor_invalid")
        offset = entry_count
    else:
        cache = current["_getpages"]
        count = current.get("_count", "")
        offset = current.get("_getpagesoffset", "")
        if not count.isascii() or not count.isdigit() or not 0 < int(count) <= PAGE_COUNT_CAP:
            raise ValueError("minnesota_cache_count_invalid")
        if not offset.isascii() or not offset.isdigit() or not 0 <= entry_count <= int(count):
            raise ValueError("minnesota_cache_offset_invalid")
        offset = int(offset) + entry_count
        if next_link:
            issued = _query(urljoin(current_url, next_link), resource)
            # The gateway can append an extra -H to each continuation value.
            if issued.get("_getpages") not in {cache, cache + "-H"} or issued.get("_getpagesoffset", "").removesuffix(
                "-H"
            ) != str(offset):
                raise ValueError("minnesota_cache_cursor_changed")
        elif entry_count < int(count):
            return None
    if (
        not cache
        or len(cache) > 1024
        or not all(character.isascii() and (character.isalnum() or character == "-") for character in cache)
    ):
        raise ValueError("minnesota_cache_handle_invalid")
    if not entry_count:
        raise ValueError("minnesota_cache_empty_continuation")
    return (
        API_BASE
        + "?"
        + urlencode({"_getpages": cache, "_getpagesoffset": offset, "_count": page_count, "_bundletype": "searchset"})
    )
