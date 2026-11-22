# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Traverse native institutional affiliations from verified, source-owned Plan-Net networks."""

from urllib.parse import urlencode, urlsplit

from process import provider_directory_affiliation_hydration as fhir

SOURCE_ID = "pdfhir_16514e416a13c8b817e820bf"
API_BASE = "https://fhir.excellusbcbs.com/fhir/api"
METADATA_KEY = "provider_directory_network_snapshot"
LEGACY_CONFIG = {"name": "Excellus", "owner_reference": "Organization/10-40-0001"}
CONFIG = {**LEGACY_CONFIG, "name_search_characters": "ABCDEFGHIJLMNOPRSTUVWXY012345789"}
RESOURCES = ("Organization", "OrganizationAffiliation")
NETWORK_SYSTEM = "http://hl7.org/fhir/us/davinci-pdex-plan-net/CodeSystem/OrgTypeCS"


def _config(source_record):
    from process.provider_directory_fhir import _source_metadata

    return _source_metadata(source_record).get(METADATA_KEY)


def is_registered_origin(source_record):
    """Recognize the source or an attempted use of its traversal configuration."""
    return (
        source_record.get("source_id") == SOURCE_ID
        or API_BASE in (source_record.get("api_base"), source_record.get("canonical_api_base"))
        or _config(source_record) is not None
    )


def is_source(source_record):
    """Bind traversal to the reviewed carrier, origin and root predicate."""
    return (
        source_record.get("source_id") == SOURCE_ID
        and source_record.get("api_base") == API_BASE
        and source_record.get("canonical_api_base") == API_BASE
        and _config(source_record) in (LEGACY_CONFIG, CONFIG)
    )


def _work(kind, key):
    return {"kind": kind, "key": key, "url": None, "payload": {}}


def _root_names(source_record):
    config = _config(source_record)
    return (config["name"], *config.get("name_search_characters", ""))


def seed_work(source_record):
    """Register every selected query before any directory pages are consumed."""
    if not is_source(source_record):
        raise ValueError("network_snapshot_source_mismatch")
    owner = _work("owner", CONFIG["owner_reference"].removeprefix("Organization/"))
    return [owner, *(_work("roots", name) for name in _root_names(source_record))]


def _result(*, entries=(), frontier=(), memberships=(), ids=(), total=None, next_url=None, missing_ids=()):
    return dict(
        entries=list(entries),
        frontier=list(frontier),
        memberships=list(memberships),
        ids=list(ids),
        total=total,
        next_url=next_url,
        missing_ids=list(missing_ids),
    )


def _is_network(host, org):
    return any(c.get("code") == "ntwk" and c.get("system") == NETWORK_SYSTEM for c in host._codings(org.get("type")))


def _owned(org):
    part_of = (org.get("partOf") or {}).get("reference")
    return bool(part_of and fhir.normalize_org_reference(part_of, urlsplit(API_BASE)) == CONFIG["owner_reference"])


def _ids(entries):
    ids = [entry["resource"].get("id") for entry in entries]
    if not all(map(fhir.valid_id, ids)) or len(set(ids)) != len(ids):
        raise ValueError("network_snapshot_duplicate_or_invalid_id")
    return ids


async def step(host, source_record, work, *, timeout, page_count):
    """Acquire one durable query page, emitting only observed follow-up work."""
    if not is_source(source_record):
        raise ValueError("network_snapshot_source_mismatch")
    kind, key = work["kind"], work["key"]
    if kind == "owner" and key == CONFIG["owner_reference"].split("/")[1] and not work.get("url"):
        owner_page = await read_organizations(host, source_record, [key], timeout=timeout)
        if (
            len(owner_page["entries"]) != 1
            or owner_page["entries"][0]["resource"].get("name") != source_record["org_name"]
        ):
            raise ValueError("network_snapshot_owner_mismatch")
        return owner_page
    count = min(100, page_count)
    if count < 1:
        raise ValueError("network_snapshot_invalid_page_count")
    if kind == "roots" and key in _root_names(source_record):
        resource_type = "Organization"
        search_parameters_by_name = {"name": key, "type": "ntwk", "_count": count}
    elif kind == "network" and fhir.valid_id(key):
        resource_type = fhir.RESOURCE_TYPE
        ref = "Organization/" + key
        search_parameters_by_name = {"network": ref, "_count": count, "_elements": "id,network"}
    else:
        raise ValueError("network_snapshot_unrecognized_query")
    url = work.get("url") or (API_BASE + "/" + resource_type + "?" + urlencode(search_parameters_by_name))
    entries, total, next_url = await fhir.search_page(host, source_record, resource_type, url, timeout=timeout)
    ids = _ids(entries)
    if type(total) is not int or total < len(ids) or (next_url and not ids):
        raise ValueError("network_snapshot_unreconciled_page")
    metrics_by_field = dict(ids=ids, total=total, next_url=next_url)
    if kind == "roots":
        return _result(
            frontier=[
                _work("organization", org["id"])
                for entry in entries
                if _is_network(host, org := entry["resource"]) and _owned(org)
            ],
            **metrics_by_field,
        )
    if any(ref not in fhir.network_refs(entry["resource"], urlsplit(API_BASE)) for entry in entries):
        raise ValueError("network_snapshot_wrong_network")
    return _result(memberships=[(key_id, ref) for key_id in ids], **metrics_by_field)


def affiliation_result(source_record, entries):
    """Preserve full native resources and discover institutions and network identities once."""
    if not is_source(source_record):
        raise ValueError("network_snapshot_source_mismatch")
    org_refs = set()
    for entry in entries:
        resource = fhir.affiliation_resource(entry)
        org_refs.update(fhir.network_refs(resource, urlsplit(API_BASE)))
        org_refs.update(fhir.institution_refs(resource, urlsplit(API_BASE)))
    org_refs.discard(CONFIG["owner_reference"])
    return _result(
        entries=entries,
        ids=_ids(entries),
        frontier=[_work("organization", ref.split("/")[1]) for ref in sorted(org_refs)],
    )


async def read_organizations(host, source_record, ids, *, timeout):
    """Resolve bounded canonical IDs. Preserve source-unresolved references explicitly."""
    if (
        not is_source(source_record)
        or not 0 < len(ids) <= fhir.BATCH_SIZE
        or not all(map(fhir.valid_id, ids))
        or len(set(ids)) != len(ids)
    ):
        raise ValueError("network_snapshot_invalid_organization_batch")
    by_id = {}
    stable_total = None
    async for entries, total, _ in fhir.canonical_pages(
        host,
        source_record,
        "Organization",
        {"_id": ",".join(ids), "_count": fhir.BATCH_SIZE},
        timeout=timeout,
        max_pages=fhir.BATCH_SIZE,
    ):
        if type(total) is not int or not 0 <= total <= len(ids) or (stable_total is not None and stable_total != total):
            raise ValueError("network_snapshot_invalid_organization_total")
        stable_total = total
        for key, entry in zip(_ids(entries), entries, strict=True):
            if key not in ids or key in by_id:
                raise ValueError("network_snapshot_unrequested_organization")
            by_id[key] = entry
    if len(by_id) != stable_total:
        raise ValueError("network_snapshot_partial_organization_batch")
    owned_networks = []
    retained_entries = []
    for key, entry in by_id.items():
        org = entry["resource"]
        if _is_network(host, org):
            if not _owned(org):
                continue
            owned_networks.append(_work("network", key))
        retained_entries.append(entry)
    return _result(
        entries=retained_entries,
        frontier=owned_networks,
        ids=by_id,
        total=len(by_id),
        missing_ids=sorted(set(ids) - by_id.keys()),
    )
