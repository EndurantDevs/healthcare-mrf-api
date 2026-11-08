# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Reviewed public directory seeds and their source-issued continuation contract."""

import urllib.parse
import urllib.request

API_BASE = "https://doctors.christushealthplan.org/fhir/provider-directory"


DOC_URL = "https://www.christushealthplan.org/privacy-and-patient-rights/interoperability/provider-directory-api"


RESOURCES = (
    "InsurancePlan",
    "PractitionerRole",
    "Practitioner",
    "Organization",
    "Location",
    "HealthcareService",
    "OrganizationAffiliation",
)


def is_request_url(url: str) -> bool:
    """Scope byte-preserving transport to the reviewed directory origin and collections."""
    base = urllib.parse.urlsplit(API_BASE)
    parsed = urllib.parse.urlsplit(url)
    try:
        return (
            parsed.scheme == base.scheme
            and parsed.hostname == base.hostname
            and (parsed.port or 443) == (base.port or 443)
            and parsed.username is None
            and parsed.password is None
            and not parsed.fragment
            and parsed.path.removeprefix(base.path + "/") in RESOURCES
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
    if not is_request_url(request.full_url):
        return urllib.request.urlopen(request, timeout=timeout, context=context)
    opener = urllib.request.build_opener(urllib.request.HTTPSHandler(context=context), _NoRedirects())
    return opener.open(request, timeout=timeout)


def christus_seed_rows() -> list[dict]:
    """Return fresh metadata for all seven openly enumerable collections."""
    return [
        {
            "id": "christus-health-plan-provider-directory",
            "org_name": "CHRISTUS Health Plan",
            "plan_name": "CHRISTUS Health Plan Provider Directory",
            "api_base": API_BASE,
            "auth_type": "none",
            "requires_registration": False,
            "fhir_version": "4.0.1",
            "source": "christus-health-plan-fhir-directory",
            "source_detail": "official public Provider Directory FHIR API",
            "source_url": DOC_URL,
            "note": "Complete REST collections use server-issued continuations; publication requires validation.",
            "metadata_json": {
                "provider_directory_confirmed_base": API_BASE,
                "provider_directory_confirmed_catalog_url": DOC_URL,
                "provider_directory_supported_resources": list(RESOURCES),
                "provider_directory_fully_enumerable_resources": list(RESOURCES),
                "provider_directory_expected_nonempty_resources": list(RESOURCES),
                "provider_directory_resource_page_count_caps": {resource: 100 for resource in RESOURCES},
                "provider_directory_coverage_mode": "full",
                "provider_directory_acquisition_enabled": True,
            },
        }
    ]


def resolved_christus_next_url(current_url: str, next_url: str) -> str:
    """Retain exact cursor bytes only within the same reviewed resource collection."""
    base = urllib.parse.urlsplit(API_BASE)
    current = urllib.parse.urlsplit(current_url)
    following = urllib.parse.urlsplit(next_url)
    resource = current.path.removeprefix(base.path + "/")
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
    if not (is_trusted and resource in RESOURCES and following.path == current.path):
        raise ValueError("untrusted_christus_pagination_link")
    return next_url
