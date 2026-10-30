# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Reviewed public directory seeds and their source-issued continuation contract."""

import urllib.parse
import urllib.request

CHRISTUS_PROVIDER_DIRECTORY_BASE = "https://doctors.christushealthplan.org/fhir/provider-directory"
CHRISTUS_DIRECTORY_DOC_URL = (
    "https://www.christushealthplan.org/privacy-and-patient-rights/interoperability/provider-directory-api"
)
CHRISTUS_RESOURCES = (
    "InsurancePlan",
    "PractitionerRole",
    "Practitioner",
    "Organization",
    "Location",
    "HealthcareService",
    "OrganizationAffiliation",
)


def pagination_host_key(text: str | None) -> str | None:
    """Keep the existing HTTPS host-and-port identity for continuation trust."""
    if not text:
        return None
    parsed = urllib.parse.urlsplit(text if "://" in text else f"https://{text}")
    if parsed.scheme.lower() != "https" or not parsed.hostname:
        return None
    try:
        port = parsed.port or 443
    except ValueError:
        return None
    return f"{parsed.hostname.lower()}:{port}"


def is_christus_request_url(url: str) -> bool:
    """Scope byte-preserving transport to the reviewed directory origin and collections."""
    base = urllib.parse.urlsplit(CHRISTUS_PROVIDER_DIRECTORY_BASE)
    parsed = urllib.parse.urlsplit(url)
    try:
        return (
            parsed.scheme == base.scheme
            and parsed.hostname == base.hostname
            and (parsed.port or 443) == (base.port or 443)
            and parsed.username is None
            and parsed.password is None
            and not parsed.fragment
            and parsed.path.removeprefix(base.path + "/") in CHRISTUS_RESOURCES
        )
    except ValueError:
        return False


class _NoRedirects(urllib.request.HTTPRedirectHandler):
    """Keep the verified directory destination fixed through a synchronous fetch."""

    def redirect_request(self, *_args, **_kwargs):
        """Return the original redirect status without contacting its destination."""
        return None


def open_source_request(request, *, timeout, context):
    """Keep other sources unchanged while CHRISTUS refuses redirected resources."""
    if not is_christus_request_url(request.full_url):
        return urllib.request.urlopen(request, timeout=timeout, context=context)
    opener = urllib.request.build_opener(urllib.request.HTTPSHandler(context=context), _NoRedirects())
    return opener.open(request, timeout=timeout)


def health_partners_seed(api_base: str) -> dict:
    """Preserve the existing public directory seed identity."""
    return {
        "id": "health-partners-plans-provider-directory",
        "org_name": "Health Partners Plans",
        "plan_name": "Health Partners Plans Provider Directory",
        "api_base": api_base,
        "auth_type": "none",
        "last_validated_status": "valid",
        "requires_registration": False,
        "source": "health-partners-plans-fhir-root",
        "source_detail": "official Health Partners Plans public Provider Directory FHIR server",
        "source_url": api_base,
        "note": "Supplemental Provider Directory source from the Health Partners Plans public FHIR server.",
    }


def christus_seed_rows() -> list[dict]:
    """Return fresh metadata for all seven openly enumerable collections."""
    return [
        {
            "id": "christus-health-plan-provider-directory",
            "org_name": "CHRISTUS Health Plan",
            "plan_name": "CHRISTUS Health Plan Provider Directory",
            "api_base": CHRISTUS_PROVIDER_DIRECTORY_BASE,
            "auth_type": "none",
            "requires_registration": False,
            "fhir_version": "4.0.1",
            "source": "christus-health-plan-fhir-directory",
            "source_detail": "official public Provider Directory FHIR API",
            "source_url": CHRISTUS_DIRECTORY_DOC_URL,
            "note": "Complete REST collections use server-issued continuations; publication requires validation.",
            "metadata_json": {
                "provider_directory_confirmed_base": CHRISTUS_PROVIDER_DIRECTORY_BASE,
                "provider_directory_confirmed_catalog_url": CHRISTUS_DIRECTORY_DOC_URL,
                "provider_directory_supported_resources": list(CHRISTUS_RESOURCES),
                "provider_directory_fully_enumerable_resources": list(CHRISTUS_RESOURCES),
                "provider_directory_expected_nonempty_resources": list(CHRISTUS_RESOURCES),
                "provider_directory_resource_page_count_caps": {resource: 100 for resource in CHRISTUS_RESOURCES},
                "provider_directory_coverage_mode": "full",
                "provider_directory_acquisition_enabled": True,
            },
        }
    ]


def resolved_christus_next_url(current_url: str, next_url: str) -> str:
    """Retain exact cursor bytes only within the same reviewed resource collection."""
    base = urllib.parse.urlsplit(CHRISTUS_PROVIDER_DIRECTORY_BASE)
    current = urllib.parse.urlsplit(current_url)
    following = urllib.parse.urlsplit(next_url)
    resource = current.path.removeprefix(base.path + "/")
    query = urllib.parse.parse_qs(following.query, keep_blank_values=True)
    count = query.get("_count", [])
    cursors = query.get("_cursor", [])
    offsets = query.get("_offset", [])
    has_cursor = len(cursors) == 1 and bool(cursors[0]) and not offsets
    has_offset = resource == "Organization" and len(offsets) == 1 and offsets[0].isdecimal() and not cursors
    try:
        is_trusted = all(
            parsed.scheme == "https"
            and parsed.hostname == base.hostname
            and parsed.port in {None, 443}
            and parsed.username is None
            and parsed.password is None
            and not parsed.fragment
            for parsed in (current, following)
        )
    except ValueError:
        is_trusted = False
    if not (
        is_trusted
        and resource in CHRISTUS_RESOURCES
        and following.path == current.path
        and set(query) <= {"_count", "_cursor", "_offset"}
        and len(count) == 1
        and count[0].isdecimal()
        and 1 <= int(count[0]) <= 100
        and (has_cursor or has_offset)
    ):
        raise ValueError("untrusted_christus_pagination_link")
    return next_url
