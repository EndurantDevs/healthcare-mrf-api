# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Source-aware hygiene predicates for automatic FHIR Organization network projection."""

from urllib.parse import urlsplit

from process.provider_directory_highmark import API_ROOT as HIGHMARK_API_ROOT
from process.registry_fhir_catalog_extraction import fhir_source_uri

AUTOMATIC_NETWORK_HYGIENE_VERSION = "fhir-automatic-org-hygiene-v1"
HIGHMARK_HYGIENE_KIND = "highmark_hmhs_v1"
IBX_HYGIENE_KIND = "ibx_pharmacy_tier_v1"
SUPPORTED_NETWORK_HYGIENE_POLICIES = frozenset(
    {
        f"{AUTOMATIC_NETWORK_HYGIENE_VERSION}:{HIGHMARK_HYGIENE_KIND}",
        f"{AUTOMATIC_NETWORK_HYGIENE_VERSION}:{IBX_HYGIENE_KIND}",
    }
)
_HIGHMARK_FEED_PREFIX = HIGHMARK_API_ROOT.rstrip("/") + "/"
_IBX_FEED_URI = "https://eapics.ibx.com/provider/v1/fhir"
_IBX_PHARMACY_TIER_REGEX = (
    "^(Retail Chain|Retail Non Chain|LTC|Home Infusion|Specialty|Mail|Mail Order)(-(Preferred|Non-Preferred))?$"
)


def _normalized_feed_uri(descriptor):
    if type(descriptor) is not dict or descriptor.get("profile") == "retained_normalized":
        return None
    raw_uri = descriptor.get("source_uri")
    if type(raw_uri) is not str or not raw_uri.strip():
        return None
    uri = fhir_source_uri(raw_uri)
    if uri is None:
        return None
    parsed = urlsplit(uri)
    return parsed._replace(
        scheme=parsed.scheme.lower(),
        netloc=parsed.netloc.lower(),
        path=parsed.path.rstrip("/"),
    ).geturl()


def automatic_network_hygiene_kind(descriptor):
    """Identify feeds that emit known non-network Organization fan-out."""
    normalized_uri = _normalized_feed_uri(descriptor)
    if normalized_uri is None:
        return None
    if normalized_uri.startswith(_HIGHMARK_FEED_PREFIX):
        return HIGHMARK_HYGIENE_KIND
    if normalized_uri == _IBX_FEED_URI:
        return IBX_HYGIENE_KIND
    return None


def automatic_network_hygiene_policy(descriptor):
    """Return the versioned source-vector hygiene pin for feeds with hygiene rules."""
    hygiene_kind = automatic_network_hygiene_kind(descriptor)
    if hygiene_kind is None:
        return None
    return f"{AUTOMATIC_NETWORK_HYGIENE_VERSION}:{hygiene_kind}"


def uses_network_targets_table(hygiene_kind):
    """Highmark network projection relies strictly on source-declared networks."""
    return hygiene_kind != HIGHMARK_HYGIENE_KIND


def automatic_network_organizations_sql(namespace, hygiene_kind=None):
    """Build the bounded FROM/WHERE clause for automatic Organization network selection."""
    base_query = (
        f"FROM {namespace}.registry_fhir_catalog_observation o "
        "WHERE o.source_generation=$1 AND o.resource_type='Organization' AND o.observation_kind='organization' "
    )
    if hygiene_kind == HIGHMARK_HYGIENE_KIND:
        return (
            base_query
            + "AND o.document_json->'declared_network'='true'::jsonb "
            + "AND o.resource_id NOT ILIKE 'UCD-%'"
        )
    candidate_clause = (
        base_query
        + "AND (o.document_json->'declared_network'='true'::jsonb "
        + "OR EXISTS(SELECT FROM pg_temp.fhir_catalog_network_targets n WHERE n.resource_id=o.resource_id))"
    )
    if hygiene_kind == IBX_HYGIENE_KIND:
        return (
            candidate_clause
            + " AND NOT (o.document_json->>'part_of_ref' IS NULL "
            + f"AND btrim(coalesce(o.document_json->>'name','')) ~* '{_IBX_PHARMACY_TIER_REGEX}')"
        )
    return candidate_clause


def hygiene_reprojected_generations(current_by_generation):
    """Collect current source generations whose automatic bindings must be rebuilt under hygiene rules."""
    return tuple(
        sorted(
            generation
            for generation, disposition in current_by_generation.items()
            if automatic_network_hygiene_kind(disposition.descriptor) is not None
        )
    )
