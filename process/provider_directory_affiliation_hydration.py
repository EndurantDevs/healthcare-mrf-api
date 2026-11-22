# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Opt-in canonical hydration for affiliation search projections."""

from __future__ import annotations

import re
from urllib.parse import urlencode, urlsplit, urlunsplit

BATCH_SIZE = 12
RESOURCE_TYPE = "OrganizationAffiliation"


def valid_id(value):
    """Accept native FHIR logical IDs that are safe as path segments."""
    return (
        isinstance(value, str) and value not in {".", ".."} and re.fullmatch(r"[A-Za-z0-9.-]{1,64}", value) is not None
    )


def network_refs(resource, base):
    """Read the native network references of a canonical affiliation."""
    networks = resource.get("network", [])
    if not isinstance(networks, list) or any(
        not isinstance(item, dict) or not isinstance(item.get("reference"), str) or not item["reference"].strip()
        for item in networks
    ):
        raise ValueError("canonical_affiliation_invalid_network")
    return {normalize_org_reference(item["reference"], base) for item in networks}


def institution_refs(resource, base):
    """Return normalized native organization/participatingOrganization reference union."""
    references = set()
    for field in ("organization", "participatingOrganization"):
        reference = resource.get(field)
        if reference is None:
            continue
        if not isinstance(reference, dict):
            raise ValueError("canonical_affiliation_invalid_institution")
        target = reference.get("reference")
        if target is not None:
            references.add(normalize_org_reference(target, base))
    return references


def normalize_org_reference(reference, base):
    """Normalize only Organization references on this exact FHIR origin."""
    if not isinstance(reference, str):
        raise ValueError("canonical_affiliation_invalid_network")
    parsed = urlsplit(reference)
    if parsed.scheme or parsed.netloc:
        if (parsed.scheme, parsed.netloc) != (base.scheme, base.netloc):
            raise ValueError("canonical_affiliation_untrusted_network")
        prefix = base.path.rstrip("/") + "/"
        if not parsed.path.startswith(prefix):
            raise ValueError("canonical_affiliation_untrusted_network")
        relative = parsed.path[len(prefix) :]
    else:
        relative = parsed.path
    if (
        parsed.query
        or parsed.fragment
        or not relative.startswith("Organization/")
        or not valid_id(relative.removeprefix("Organization/"))
    ):
        raise ValueError("canonical_affiliation_invalid_network")
    return relative


def affiliation_resource(entry):
    """Validate a native affiliation before using its logical ID."""
    resource = entry.get("resource") if isinstance(entry, dict) else None
    if (
        not isinstance(resource, dict)
        or resource.get("resourceType") != RESOURCE_TYPE
        or not valid_id(resource.get("id"))
    ):
        raise ValueError("canonical_affiliation_invalid_resource")
    return resource


def collection(source, kind):
    """Build the trusted collection from the registered source."""
    base = urlsplit(source.get("canonical_api_base") or source["api_base"])
    if (
        kind not in {"Organization", RESOURCE_TYPE}
        or base.scheme != "https"
        or not base.hostname
        or base.username
        or base.password
        or base.query
        or base.fragment
    ):
        raise ValueError("canonical_affiliation_untrusted_collection")
    return base._replace(path=base.path.rstrip("/") + "/" + kind)


def _check_url(parsed, url):
    target = urlsplit(url)
    if (
        (target.scheme, target.netloc, target.path) != (parsed.scheme, parsed.netloc, parsed.path)
        or target.username
        or target.password
        or target.fragment
    ):
        raise ValueError("canonical_affiliation_untrusted_continuation")


async def search_page(host, source, kind, url, *, timeout):
    """Read a single issued page. Keep the verified next URL byte-for-byte for checkpointing."""
    parsed = collection(source, kind)
    _check_url(parsed, url)
    status, payload, error, _ = await host._fetch_source_json(source, url, timeout=timeout)
    if status != 200 or error or not isinstance(payload, dict):
        raise ValueError(f"canonical_affiliation_fetch_failed_http_{status}")
    if host.fhir_response.strict_search_bundle_error(payload, kind):
        raise ValueError("canonical_affiliation_invalid_bundle")
    entries = payload.get("entry", [])
    next_url = host._resolved_fhir_next_url(
        source,
        url,
        host.fhir_response.strict_search_next_link(payload),
        resource_type=kind,
        page_entry_count=len(entries),
    )
    if next_url is not None:
        _check_url(parsed, next_url)
    return entries, payload.get("total"), next_url


async def canonical_pages(host, source, kind, search_parameters_by_name, *, timeout, max_pages):
    """Read full resources and retain the exact URL that supplied each entry."""
    url = urlunsplit(collection(source, kind)._replace(query=urlencode(search_parameters_by_name)))
    visited_urls = set()
    while url:
        if url in visited_urls or len(visited_urls) >= max_pages:
            raise ValueError("network_search_page_cycle_or_budget")
        visited_urls.add(url)
        entries, count, next_url = await search_page(host, source, kind, url, timeout=timeout)
        for entry in entries:
            meta = entry["resource"].get("meta") or {}
            if any(tag.get("code") == "SUBSETTED" for tag in meta.get("tag", [])):
                raise ValueError("canonical_affiliation_subsetted")
        yield [{**entry, "_retrieved_from": url} for entry in entries], count, bool(next_url)
        url = next_url


async def hydrate(host, source, expected_networks_by_id, *, timeout):
    """Read each unique affiliation once and check its observed network references."""
    base = urlsplit(source.get("canonical_api_base") or source["api_base"])
    networks_by_id = {}
    for resource_id, references in expected_networks_by_id.items():
        if not valid_id(resource_id):
            raise ValueError("canonical_affiliation_invalid_resource")
        if not isinstance(references, (set, frozenset, list, tuple)) or not references:
            raise ValueError("canonical_affiliation_missing_projected_network")
        networks_by_id[resource_id] = {normalize_org_reference(reference, base) for reference in references}
    ids = list(networks_by_id)
    entries_by_id = {}
    for offset in range(0, len(ids), BATCH_SIZE):
        requested = ids[offset : offset + BATCH_SIZE]
        search_parameters_by_name = {"_id": ",".join(requested), "_count": BATCH_SIZE}
        async for entries, _, _ in canonical_pages(
            host, source, RESOURCE_TYPE, search_parameters_by_name, timeout=timeout, max_pages=BATCH_SIZE
        ):
            for entry in entries:
                resource = affiliation_resource(entry)
                resource_id = resource["id"]
                if resource_id not in requested or resource_id in entries_by_id:
                    raise ValueError("canonical_affiliation_identity_conflict")
                if not networks_by_id[resource_id].issubset(network_refs(resource, base)):
                    raise ValueError("canonical_affiliation_network_conflict")
                entries_by_id[resource_id] = entry
        if any(resource_id not in entries_by_id for resource_id in requested):
            raise ValueError("canonical_affiliation_missing_identity")
    return [entries_by_id[resource_id] for resource_id in ids]
