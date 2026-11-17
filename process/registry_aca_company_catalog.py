# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Attach existing source-period HIOS company evidence without creating approvals or identities."""

import json
from uuid import UUID

from db.plan_release_storage import storage_identifier
from process.registry_issuer_resolution import MAX_ISSUERS, read_registry_issuer_resolutions

CONTRACT = "aca-issuer-company-catalog-v1"


def _labels(labels):
    return sorted({label for label in labels if type(label) is str and label.strip() and len(label) <= 512})


def _company_record(resolution):
    legal = resolution["legal_company"]
    if resolution["company_resolution_status"] != "resolved" or legal is None:
        return None
    labels = _labels(legal["source_labels"])
    origin = "source_company"
    if not labels:
        labels, origin = _labels(resolution["issuer_source_labels"]), "source_issuer"
    if not labels or len(labels) > 100:
        return None
    return {
        "company_id": legal["company_id"],
        "display_name": labels[0],
        "display_name_origin": origin,
        "roles": ["insurer"],
        "aliases": labels[1:],
        "archived": False,
    }


def _group_record(resolution, kinds_by_group):
    reported = resolution["reported_group"]
    if reported is None:
        return None
    labels = _labels(reported["source_labels"])
    kind = kinds_by_group.get(reported["group_id"])
    if not labels or len(labels) > 100 or kind not in {"corporate_parent", "naic_group"}:
        return None
    return {
        "group_id": reported["group_id"],
        "display_name": labels[0],
        "aliases": labels[1:],
        "archived": False,
        "group_kind": kind,
    }


async def enrich_aca_company_sources(connection, native_sources, *, control_schema):
    """Resolve each distinct HIOS issuer once in bounded batches in the same native snapshot."""
    issuers = sorted(
        {
            native_source["definitions"][0]["issuer_id"]
            for native_source in native_sources
            if native_source["source_system"] == "aca"
        }
    )
    if not issuers:
        return native_sources
    resolutions_by_issuer = {}
    for start in range(0, len(issuers), MAX_ISSUERS):
        resolutions = await read_registry_issuer_resolutions(
            connection, issuers[start : start + MAX_ISSUERS], control_schema=control_schema
        )
        resolutions_by_issuer.update((resolution["hios_issuer_id"], resolution) for resolution in resolutions)
    if set(resolutions_by_issuer) != set(issuers):
        raise ValueError("registry_aca_company_inventory_incomplete")
    kinds_by_group = await _source_group_kinds(connection, resolutions_by_issuer.values(), control_schema)
    enriched_sources = []
    for native_source in native_sources:
        if native_source["source_system"] != "aca":
            enriched_sources.append(native_source)
            continue
        resolution = resolutions_by_issuer[native_source["definitions"][0]["issuer_id"]]
        enriched_sources.append(
            {
                **native_source,
                "company_catalog": {
                    "contract": CONTRACT,
                    "resolution": resolution,
                    "company_record": _company_record(resolution),
                    "group_record": _group_record(resolution, kinds_by_group),
                },
            }
        )
    if (
        sum(
            len(json.dumps(native_source["company_catalog"]).encode())
            for native_source in enriched_sources
            if "company_catalog" in native_source
        )
        > 64 * 1024**2
    ):
        raise ValueError("registry_aca_company_inventory_bounds")
    return enriched_sources


async def _source_group_kinds(connection, resolutions, control_schema):
    """Read kinds from the exact immutable observations, preserving pending draft corrections."""
    claims = [
        (UUID(evidence["snapshot_id"]), evidence["source_record_key"], UUID(assertion["group_id"]))
        for resolution in resolutions
        for evidence in resolution["evidence"]
        if resolution["reported_group"] is not None
        and (assertion := evidence["group_assertion"]) is not None
        and assertion["group_id"] == resolution["reported_group"]["group_id"]
    ]
    if not claims:
        return {}
    groups = await connection.fetch(
        "WITH wanted AS (SELECT * FROM unnest($1::uuid[],$2::text[],$3::uuid[]) "
        "AS input(snapshot_id,source_record_key,group_id)) "
        "SELECT wanted.group_id::text,CASE WHEN count(DISTINCT observation.observation_json->>'group_kind')=1 "
        "THEN min(observation.observation_json->>'group_kind') END AS group_kind FROM wanted "
        f"JOIN {storage_identifier(control_schema)}.registry_source_observation observation "
        "USING(snapshot_id,source_record_key) WHERE observation.status<>'rejected' GROUP BY wanted.group_id",
        *[list(column) for column in zip(*claims, strict=True)],
    )
    return {group["group_id"]: group["group_kind"] for group in groups}


def aca_company_relationship(native_source, plan):
    """Preserve the reporting period and state, keeping affiliation separate from ownership."""
    metadata = native_source.get("company_catalog")
    if metadata is None:
        return None, None, "unresolved", "native_source_company_unmapped"
    if metadata.get("contract") != CONTRACT:
        raise ValueError("registry_aca_company_metadata_invalid")
    resolution = metadata["resolution"]
    if resolution["hios_issuer_id"] != plan["issuer_id"]:
        raise ValueError("registry_aca_company_metadata_invalid")
    if resolution["business_state"] is None:
        return None, None, "unresolved", "aca_issuer_company_state_missing"
    if resolution["business_state"] != plan["state"]:
        return None, None, "conflict", "aca_issuer_company_state_mismatch"
    company = metadata["company_record"]
    if company is None:
        status = "conflict" if resolution["company_resolution_status"] == "conflicting" else "unresolved"
        return None, None, status, "aca_issuer_company_" + resolution["company_resolution_status"]
    legal = resolution["legal_company"]
    if company["company_id"] != legal["company_id"] or resolution["company_resolution_status"] != "resolved":
        raise ValueError("registry_aca_company_metadata_invalid")
    group_record, reported = metadata["group_record"], resolution["reported_group"]
    group = None
    if group_record is not None:
        if reported is None or group_record["group_id"] != reported["group_id"]:
            raise ValueError("registry_aca_company_metadata_invalid")
        group = {name: group_record[name] for name in ("group_id", "display_name", "group_kind")} | {
            "relationship_kind": reported["relationship_kind"],
            "valid_from": None,
            "valid_to": None,
        }
    status = "conflict" if resolution["resolution_status"] == "conflicting" else resolution["resolution_status"]
    return (
        {name: company[name] for name in ("company_id", "display_name")},
        group,
        status,
        "aca_issuer_reporting_period_" + str(resolution["reporting_year"]),
    )
