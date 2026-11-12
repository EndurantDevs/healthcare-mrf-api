# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Endpoint-scoped Anthem directory configuration for the shared FHIR importer."""

import hashlib
import re
from urllib.parse import parse_qsl, urljoin, urlsplit

API_BASE = "https://totalview.healthos.elevancehealth.com/resources/unregistered/api/v1/fhir/cms_mandate/mcd"
TOKEN_URL = "https://totalview.healthos.elevancehealth.com/client.oauth2/unregistered/api/v1/token"
DOC_URL = "https://www.anthem.com/content/dam/digital/developers-portal/Anthem-IOProviderDirectoryAndFormulary-API-Documentation.pdf"
RESOURCES = (
    "InsurancePlan",
    "PractitionerRole",
    "Practitioner",
    "Organization",
    "Location",
    "HealthcareService",
    "OrganizationAffiliation",
)
PAGE_COUNT_CAPS = dict.fromkeys(RESOURCES, 1000)
SORTED_LAST_ID_KEY = "anthem_sorted_last_id"
PAGE_COUNT_TRANSITION_KEY = "anthem_page_count_transition"
_RESOURCE_ID = re.compile(r"[A-Za-z0-9.-]{1,64}")


def start_url(resource_type: str, page_count: int, endpoint: str | None = None) -> str | None:
    """Unsorted searches can overlap even when following issued continuations."""
    if resource_type not in RESOURCES:
        return None
    if endpoint and urljoin(API_BASE + "/", endpoint) != f"{API_BASE}/{resource_type}":
        raise ValueError("untrusted_anthem_start_url")
    return f"{API_BASE}/{resource_type}?_count={page_count}&_sort=_id"


def _page_query(url: str, resource_type: str, page_count_cap: int) -> dict[str, str]:
    base = urlsplit(API_BASE)
    try:
        page = urlsplit(url)
        pairs = parse_qsl(page.query, keep_blank_values=True, strict_parsing=True)
        query_by_name = dict(pairs)
        count = query_by_name.get("_count", "")
        offset = query_by_name.get("_offset", "0")
        is_valid = (
            resource_type in RESOURCES
            and not any(ord(character) <= 32 or ord(character) == 127 for character in url)
            and "#" not in url
            and page.scheme == "https"
            and page.hostname == base.hostname
            and page.port in {None, 443}
            and page.username is None
            and page.password is None
            and page.path == f"{base.path}/{resource_type}"
            and len(query_by_name) == len(pairs)
            and set(query_by_name) in ({"_count", "_sort"}, {"_count", "_sort", "_offset"})
            and query_by_name.get("_sort") == "_id"
            and count.isascii()
            and count.isdecimal()
            and 1 <= int(count) <= page_count_cap
            and offset.isascii()
            and offset.isdecimal()
            and int(offset) >= 0
        )
    except TypeError, ValueError:
        is_valid = False
    if not is_valid:
        raise ValueError("untrusted_anthem_pagination_link")
    return query_by_name


def _continuation_query(
    current_url: str,
    next_link: str,
    resource_type: str,
    page_count_cap: int,
) -> tuple[dict[str, str], dict[str, str]]:
    current = _page_query(current_url, resource_type, page_count_cap)
    following = _page_query(next_link, resource_type, page_count_cap)
    if (
        "_offset" not in following
        or following["_count"] != current["_count"]
        or int(following["_offset"]) <= int(current.get("_offset", "0"))
    ):
        raise ValueError("untrusted_anthem_pagination_link")
    return current, following


def next_url(
    current_url: str,
    next_link: str | None,
    resource_type: str,
    page_count_cap: int,
    page_entry_count: int | None,
) -> str | None:
    """Require the issued row offset to advance by the received count, retaining URL bytes."""
    if next_link is None:
        return None
    current, following = _continuation_query(current_url, next_link, resource_type, page_count_cap)
    if (
        type(page_entry_count) is not int
        or not 1 <= page_entry_count <= int(current["_count"])
        or int(following["_offset"]) - int(current.get("_offset", "0")) != page_entry_count
    ):
        raise ValueError("untrusted_anthem_pagination_link")
    return next_link


def resume_url(current_url: str, next_link: str | None, resource_type: str, page_count_cap: int) -> str:
    """Recheck a saved cursor whose page step was validated before its atomic checkpoint."""
    if not isinstance(next_link, str) or not next_link:
        raise ValueError("untrusted_anthem_pagination_link")
    _continuation_query(current_url, next_link, resource_type, page_count_cap)
    return next_link


def checkpoint_start_hash(resource_type: str, start: str, checkpoint: dict, completeness: dict) -> str:
    """Keep the original checkpoint identity for the reviewed count-only upgrade."""
    current_hash = hashlib.sha256(start.encode()).hexdigest()
    legacy_hash = hashlib.sha256(start_url("PractitionerRole", 500).encode()).hexdigest()
    if (
        resource_type == "PractitionerRole"
        and start == start_url(resource_type, 1000)
        and checkpoint.get("start_url_hash") == legacy_hash
        and checkpoint.get("state") == "active"
        and int(checkpoint.get("rows_processed") or 0) > 0
    ):
        query = _page_query(checkpoint.get("next_url"), resource_type, 1000)
        if query["_count"] == "1000" and not completeness.get(PAGE_COUNT_TRANSITION_KEY):
            raise ValueError("anthem_page_count_transition_invalid")
        return legacy_hash
    return current_hash


def _validate_count_transition(proof: dict, saved: dict, rows: int, boundary: str | None) -> None:
    if not (
        type(proof) is dict
        and set(proof) == {"from", "to", "issued_offset", "issued_cursor_sha256", "sorted_last_id"}
        and type(proof["from"]) is int
        and proof["from"] == 500
        and type(proof["to"]) is int
        and proof["to"] == 1000
        and type(proof["issued_offset"]) is int
        and 0 < proof["issued_offset"] < rows
        and type(proof["issued_cursor_sha256"]) is str
        and re.fullmatch(r"[0-9a-f]{64}", proof["issued_cursor_sha256"])
        and type(proof["sorted_last_id"]) is str
        and _RESOURCE_ID.fullmatch(proof["sorted_last_id"])
        and boundary is not None
        and proof["sorted_last_id"] < boundary
        and saved["_count"] == "1000"
        and int(saved.get("_offset", "0")) == rows
    ):
        raise ValueError("anthem_page_count_transition_invalid")


def prepare_resume(start: str, saved_url: str, resource_type: str, cap: int, rows_processed: int, completeness: dict):
    """Resize only a retained role cursor; persist its proof with the accepted page."""
    proof_by_field = dict(completeness)
    transition = proof_by_field.get(PAGE_COUNT_TRANSITION_KEY)
    if resource_type != "PractitionerRole" or start != start_url(resource_type, 1000):
        if transition is not None:
            raise ValueError("anthem_page_count_transition_invalid")
        return resume_url(start, saved_url, resource_type, cap), proof_by_field
    saved = _page_query(saved_url, resource_type, cap)
    boundary = resume_boundary(rows_processed, completeness)
    if transition is not None:
        _validate_count_transition(transition, saved, rows_processed, boundary)
        return resume_url(start, saved_url, resource_type, cap), proof_by_field
    if saved["_count"] != "500":
        return resume_url(start, saved_url, resource_type, cap), proof_by_field
    resume_url(start_url(resource_type, 500), saved_url, resource_type, cap)
    if cap < 1000 or rows_processed <= 0 or int(saved["_offset"]) != rows_processed or boundary is None:
        raise ValueError("anthem_page_count_transition_invalid")
    proof_by_field[PAGE_COUNT_TRANSITION_KEY] = {
        "from": 500,
        "to": 1000,
        "issued_offset": rows_processed,
        "issued_cursor_sha256": hashlib.sha256(saved_url.encode()).hexdigest(),
        "sorted_last_id": boundary,
    }
    prefix, query = saved_url.split("?", 1)
    parts = query.split("&")
    for index, part in enumerate(parts):
        if parse_qsl(part, strict_parsing=True) == [("_count", "500")]:
            parts[index] = part.split("=", 1)[0] + "=1000"
    return prefix + "?" + "&".join(parts), proof_by_field


def prepare_acquisition(start: str, resource_type: str, cap: int, resume):
    """Prepare the trusted request and proof without changing the committed cursor."""
    rows = resume.rows_processed if resume else 0
    completeness = resume.completeness if resume else {}
    boundary = resume_boundary(rows, completeness)
    if resume and rows and not resume.complete:
        url, completeness = prepare_resume(start, resume.next_url, resource_type, cap, rows, completeness)
        return boundary, url, completeness
    return boundary, None, dict(completeness)


def next_link(payload: dict) -> str | None:
    """Preserve one issued next URL; refuse malformed links instead of ending the scan."""
    links = payload.get("link", [])
    if not isinstance(links, list) or any(not isinstance(link, dict) for link in links):
        raise ValueError("untrusted_anthem_pagination_link")
    values = [link.get("url") for link in links if link.get("relation") == "next"]
    if len(values) > 1 or (values and (type(values[0]) is not str or not values[0])):
        raise ValueError("untrusted_anthem_pagination_link")
    return values[0] if values else None


def resume_boundary(rows_processed: int, completeness: dict) -> str | None:
    """Require the durable sorted-ID boundary whenever a resumed scan has retained rows."""
    previous_id = completeness.get(SORTED_LAST_ID_KEY)
    if (rows_processed and previous_id is None) or (
        previous_id is not None and (type(previous_id) is not str or _RESOURCE_ID.fullmatch(previous_id) is None)
    ):
        raise ValueError("anthem_sorted_id_checkpoint_invalid")
    return previous_id


def page_boundary(payload: dict, resource_type: str, previous_id: str | None, page_count_cap: int) -> str | None:
    """Refuse unsorted, repeated or malformed identities before staging the page."""
    entries = payload.get("entry", [])
    if (
        payload.get("resourceType") != "Bundle"
        or payload.get("type") != "searchset"
        or not isinstance(entries, list)
        or len(entries) > page_count_cap
    ):
        raise ValueError("anthem_sorted_id_page_invalid")
    for entry in entries:
        resource = entry.get("resource") if isinstance(entry, dict) else None
        resource_id = resource.get("id") if isinstance(resource, dict) else None
        if (
            not isinstance(resource, dict)
            or resource.get("resourceType") != resource_type
            or type(resource_id) is not str
            or _RESOURCE_ID.fullmatch(resource_id) is None
            or (previous_id is not None and resource_id <= previous_id)
        ):
            raise ValueError("anthem_sorted_id_page_invalid")
        previous_id = resource_id
    return previous_id


def seed_rows() -> list[dict]:
    """Keep the registered endpoint separate from affiliate and historical routes."""
    return [
        {
            "id": "anthem-provider-directory",
            "org_name": "Anthem / Elevance Health",
            "plan_name": None,
            "api_base": API_BASE,
            "auth_type": "OAuth2 Client Credentials",
            "requires_registration": True,
            "requires_api_key": False,
            "last_validated_status": "auth_required",
            "fhir_version": "4.0.1",
            "source": "anthem-provider-directory",
            "source_url": DOC_URL,
            "metadata_json": {
                "provider_directory_confirmed_base": API_BASE,
                "provider_directory_confirmed_doc_url": DOC_URL,
                "provider_directory_confirmed_metadata_url": f"{API_BASE}/metadata",
                "provider_directory_confirmed_token_url": TOKEN_URL,
                "provider_directory_supported_resources": list(RESOURCES),
                "provider_directory_fully_enumerable_resources": list(RESOURCES),
                "provider_directory_expected_nonempty_resources": list(RESOURCES),
                "provider_directory_resource_page_count_caps": dict(PAGE_COUNT_CAPS),
                "provider_directory_resource_scan_concurrency": 8,
                "provider_directory_bulk_export_eligible": False,
                "provider_directory_coverage_mode": "carrier_directory",
                "provider_directory_directory_scope": "endpoint",
                "provider_directory_plan_provenance_neutralized": True,
                "provider_directory_acquisition_enabled": True,
            },
        }
    ]
