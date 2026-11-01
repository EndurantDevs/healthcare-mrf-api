# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Resolve directory continuations and complementary service-presence searches."""

from collections.abc import Collection
from urllib.parse import parse_qsl, urlencode, urljoin, urlsplit, urlunsplit

API_BASE = "https://api.interop.molinahealthcare.com/providerdirectory"
PAGINATION_HOST = "molina.sapphirethreesixtyfive.com"
_SERVICE_PATH = "/providerdirectory/HealthcareService"
_PRESENCE = "name:missing"
_QUERY_NAMES = frozenset(
    {
        _PRESENCE,
        "_count",
        "_format",
        "format",
        "cursormark",
        "starting_after",
        "_getpages",
        "_getpagesoffset",
        "_bundletype",
    }
)


def resolved_next_url(current_url: str, next_url: str, resource_types: Collection[str]) -> str:
    """Keep the existing exact provider-host rewrite without broadening its trust."""
    canonical_base = urlsplit(API_BASE)
    parsed_next = urlsplit(next_url)
    if parsed_next.scheme.lower() == canonical_base.scheme and parsed_next.netloc.lower() == canonical_base.netloc:
        return next_url
    parsed_current = urlsplit(current_url)
    resource_prefix = f"{canonical_base.path.rstrip('/')}/"
    resource_type = (
        parsed_current.path.removeprefix(resource_prefix) if parsed_current.path.startswith(resource_prefix) else ""
    )
    cursor_values = [
        value for key, value in parse_qsl(parsed_next.query, keep_blank_values=True) if key == "cursorMark"
    ]
    if not (
        parsed_next.scheme.lower() == "https"
        and parsed_next.netloc.lower() == PAGINATION_HOST
        and not parsed_next.fragment
        and resource_type in resource_types
        and parsed_next.path == f"/fhir/{resource_type}"
        and len(cursor_values) == 1
    ):
        raise ValueError("untrusted_molina_pagination_link")
    return urlunsplit((canonical_base.scheme, canonical_base.netloc, parsed_current.path, parsed_next.query, ""))


def _presence_query(url: str) -> tuple[str, list[tuple[str, str]]]:
    """Require an explicit phase on every service start, continuation and resume."""
    try:
        parsed = urlsplit(url)
        port = parsed.port
        items = parse_qsl(parsed.query, keep_blank_values=True)
    except ValueError:
        raise ValueError("molina_service_presence_query_invalid") from None
    phases = [value for key, value in items if key == _PRESENCE]
    counts = [value for key, value in items if key == "_count"]
    if not (
        parsed.scheme.lower() == "https"
        and parsed.hostname == urlsplit(API_BASE).hostname
        and port in {None, 443}
        and parsed.username is None
        and parsed.password is None
        and parsed.path == _SERVICE_PATH
        and not parsed.fragment
        and len(phases) == 1
        and phases[0] in {"false", "true"}
        and len(counts) == 1
        and counts[0].isdigit()
        and int(counts[0]) > 0
        and all(key.lower() in _QUERY_NAMES for key, _value in items)
    ):
        raise ValueError("molina_service_presence_query_invalid")
    return phases[0], items


def healthcare_service_next_url(current_url: str, next_link: str | None, page_entry_count: int | None) -> str | None:
    """Exhaust the present-value search before starting its missing-value complement."""
    phase, items = _presence_query(current_url)
    if next_link:
        next_url = resolved_next_url(current_url, urljoin(current_url, next_link), ("HealthcareService",))
        next_phase, next_items = _presence_query(next_url)
        if next_phase != phase:
            raise ValueError("molina_service_presence_phase_changed")
        current_identity_items = sorted((key, value) for key, value in items if key != "_count")
        next_identity_items = sorted((key, value) for key, value in next_items if key != "_count")
        if page_entry_count != 0 or next_identity_items != current_identity_items:
            return next_url
        items = next_items
    if phase == "true":
        return None
    complement_items = [
        (_PRESENCE, "true"),
        *((key, value) for key, value in items if key in {"_count", "_format", "format"}),
    ]
    return API_BASE + "/HealthcareService?" + urlencode(complement_items)


def healthcare_service_resume_error(api_base: str | None, resource_type: str, next_url: str | None) -> str | None:
    """Reject a lost retained phase before issuing another request."""
    if api_base != API_BASE or resource_type != "HealthcareService" or next_url is None:
        return None
    try:
        _presence_query(next_url)
    except ValueError as error:
        return str(error)
    return None
