# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Share office profiles and repair only unresolved publisher doctor identities."""

from __future__ import annotations

import hashlib
from collections import Counter

from process.provider_directory_projection_fhir_values import is_valid_npi
from process.vsp_directory_contract import (
    ADAPTER_ID,
    MAX_LOCATIONS,
    NETWORKS,
    PROFILE_MEMBERSHIP_UNRESOLVED,
    RESOURCE_TYPES,
    SEARCH_URL,
    SOURCE_ID,
    canonical_json,
    profile_url,
    public_id,
    require,
    search_result,
)
from process.vsp_directory_geometry import office_search_request
from process.vsp_directory_normalization import (
    normalize_vsp_office,
    vsp_network_organizations,
    vsp_profile_memberships,
)
from process.vsp_directory_store import _atomic


def _profiles(value):
    require(type(value) is list and all(type(x) is dict for x in value), "vsp_profile_response_invalid")
    identifiers = [public_id(x.get("doctorPublicId")) for x in value]
    require(len(set(identifiers)) == len(identifiers), "vsp_profile_doctors_duplicated")
    for profile in value:
        codes = profile.get("networkIds")
        require(
            type(codes) is list and all(type(x) is str and 0 < len(x) <= 64 for x in codes),
            "vsp_profile_networks_invalid",
        )
    return value


def _missing(capture, office, profiles, memberships):
    summaries_by_doctor = {public_id(x.get("pid")): x for x in office["providerSummaries"]}
    missing_ids = []
    for profile in profiles:
        did = public_id(profile["doctorPublicId"])
        if profile["doctorPublicId"] not in memberships:
            continue
        summary = summaries_by_doctor.get(did)
        if summary is None or not is_valid_npi(summary.get("npi")):
            identity = capture.identities.get(did)
            if identity is None:
                missing_ids.append(did)
            else:
                # Global identities never grant membership at another office.
                summaries_by_doctor[did] = identity["summary"]
                capture.identity_reuses.add((public_id(office["pid"]), did, identity["request_key"]))
    office["providerSummaries"] = list(summaries_by_doctor.values())
    return missing_ids


async def _repair_office(capture, office, profiles, memberships, missing_ids):
    oid, attempted_names = public_id(office["pid"]), set()
    while missing_ids:
        eligible_counts = Counter(
            NETWORKS[code]
            for profile in profiles
            if public_id(profile["doctorPublicId"]) in missing_ids
            for code in memberships[profile["doctorPublicId"]]
            if NETWORKS[code] not in attempted_names
        )
        if not eligible_counts:
            # A publisher doctor ID remains usable when every exact office lookup lacks an NPI.
            summary_ids = {public_id(row["pid"]) for row in office["providerSummaries"]}
            require(set(missing_ids) <= summary_ids, "vsp_doctor_identity_unresolved")
            break
        network = min(eligible_counts, key=lambda name: (-eligible_counts[name], name))
        attempted_names.add(network)
        value, receipt = await capture.fetch("GET", SEARCH_URL + "/" + oid + "?networkNames=" + network)
        require(type(value) is dict and public_id(value.get("pid")) == oid, "vsp_office_response_mismatch")
        capture.remember(value, receipt["request_key"])
        office = capture.offices[oid]
        missing_ids = _missing(capture, office, profiles, memberships)
    return office


async def _affiliate_searches(capture, office, profiles):
    if office.get("locationType") != "Affiliate" or len(vsp_profile_memberships(office, profiles, {})) == len(profiles):
        return office
    oid = public_id(office["pid"])
    for network in sorted(NETWORKS.values()):
        if network in capture.search_memberships.get(oid, {}):
            continue
        value, receipt = await capture.fetch("POST", SEARCH_URL, office_search_request(office, network))
        total, offices = search_result(value)
        matching_offices = [row for row in offices if public_id(row["pid"]) == oid]
        if matching_offices:
            capture.remember_search(matching_offices[0], network, receipt["request_key"])
        else:
            require(total < MAX_LOCATIONS, PROFILE_MEMBERSHIP_UNRESOLVED)
            capture.remember_search_scope(oid, network, ())
    return capture.offices[oid]


async def enrich_capture(capture):
    """Fetch each exact office/date profile once, then repair only missing NPI joins."""
    for oid in sorted(capture.offices):
        office = capture.offices[oid]
        if oid not in capture.profiles:
            profile_response, _ = await capture.fetch("GET", profile_url(office))
            profiles = _profiles(profile_response)
            capture.account_bytes("profile:" + oid, profiles)
            capture.profiles[oid] = profiles
        profiles = capture.profiles[oid]
        office = await _affiliate_searches(capture, office, profiles)
        search_memberships = capture.search_memberships.get(oid, {})
        memberships = vsp_profile_memberships(office, profiles, search_memberships)
        declared_names = {NETWORKS[code] for codes in memberships.values() for code in codes}
        require(
            all(oid not in capture.memberships[name] or name in declared_names for name in NETWORKS.values()),
            PROFILE_MEMBERSHIP_UNRESOLVED,
        )
        missing_ids = _missing(capture, office, profiles, memberships)
        if missing_ids:
            office = await _repair_office(capture, office, profiles, memberships, missing_ids)
        # The normalizer also validates conflicting IDs, profiles and source NPI checksums.
        normalize_vsp_office(office, profiles=profiles, search_memberships=search_memberships)
        for codes in memberships.values():
            for code in codes:
                capture.memberships[NETWORKS[code]].add(oid)
        capture.store.checkpoint(
            "enrichment", {"office_count": len(capture.offices), "profile_count": len(capture.profiles)}
        )
        await capture.progress("enrichment")


def _add_resource(resources_by_type, resource, byte_count):
    kind, rid = resource["resourceType"], resource["id"]
    raw = (canonical_json(resource) + "\n").encode()
    prior = resources_by_type[kind].get(rid)
    if prior is not None:
        if kind != "Practitioner":
            require(prior == raw, "vsp_normalized_identity_conflict")
        # Multiple offices can spell one person's name differently; retain a stable winner.
        raw = min(prior, raw, key=lambda encoded: hashlib.sha256(encoded).digest())
    byte_count += len(raw) - len(prior or b"")
    require(byte_count <= 512 * 1024 * 1024, "vsp_normalized_byte_budget_exhausted")
    resources_by_type[kind][rid] = raw
    return byte_count


async def _normalize_offices(capture):
    resources_by_type = {kind: {} for kind in RESOURCE_TYPES}
    byte_count = 0
    for organization in vsp_network_organizations():
        byte_count = _add_resource(resources_by_type, organization, byte_count)
    offices_by_network = {name: set() for name in NETWORKS.values()}
    npis_by_network = {name: set() for name in NETWORKS.values()}
    accounting, other_codes = Counter(), Counter()
    for oid, office in sorted(capture.offices.items()):
        await capture.cancel_check()
        _missing(
            capture,
            office,
            capture.profiles[oid],
            vsp_profile_memberships(office, capture.profiles[oid], capture.search_memberships.get(oid, {})),
        )
        normalized = normalize_vsp_office(
            office, profiles=capture.profiles[oid], search_memberships=capture.search_memberships.get(oid, {})
        )
        accounting.update(
            {
                "outside_profile_count": normalized.outside_profile_count,
                "outside_summary_count": normalized.outside_summary_count,
            }
        )
        other_codes.update(normalized.other_network_ids)
        for resource in normalized:
            byte_count = _add_resource(resources_by_type, resource, byte_count)
            if resource["resourceType"] == "Practitioner" and resource["id"].startswith("vsp-doctor-"):
                accounting["profiles_without_npi"] += 1
            if resource["resourceType"] != "PractitionerRole":
                continue
            practitioner = resource["practitioner"]["reference"]
            for extension in resource["extension"]:
                name = NETWORKS[extension["valueReference"]["reference"].rsplit("-", 1)[1]]
                offices_by_network[name].add(oid)
                if practitioner.startswith("Practitioner/vsp-npi-"):
                    npis_by_network[name].add(practitioner)
    require(
        all(offices_by_network[name] == capture.memberships[name] for name in NETWORKS.values()),
        PROFILE_MEMBERSHIP_UNRESOLVED,
    )
    return resources_by_type, npis_by_network, accounting, other_codes


def _write_outputs(store, resources_by_type):
    outputs, counts = {}, {}
    for kind in RESOURCE_TYPES:
        resources_by_id = resources_by_type[kind]
        counts[kind] = len(resources_by_id)
        if resources_by_id:
            raw = b"".join(resources_by_id[rid] for rid in sorted(resources_by_id))
            digest = hashlib.sha256(raw).hexdigest()
            filename = kind + "-" + digest + ".ndjson"
            _atomic(store.path / "normalized" / filename, raw, immutable=True)
            outputs[kind] = {
                "filename": filename,
                "sha256": digest,
                "byte_count": len(raw),
                "count": len(resources_by_id),
            }
    return outputs, counts


def _capture_manifest(capture, outputs, receipts, start, end, accounting, other_codes):
    return {
        "adapter_id": ADAPTER_ID,
        "source_id": SOURCE_ID,
        "capture_id": capture.store.capture_id,
        "outputs": outputs,
        "publisher_census_start": start,
        "publisher_census_end": end,
        "partition_sha256": hashlib.sha256(canonical_json(capture.partitions).encode()).hexdigest(),
        "request_receipts_sha256": hashlib.sha256(canonical_json(receipts).encode()).hexdigest(),
        "identity_reuse_sha256": hashlib.sha256(canonical_json(sorted(capture.identity_reuses)).encode()).hexdigest(),
        "other_network_ids": dict(sorted(other_codes.items())),
        "accounting": dict(accounting),
    }


async def write_normalized_capture(capture, start, end):
    """Close verified, sorted output and retain compact proof plus raw-query receipts."""
    resources_by_type, npis_by_network, accounting, other_codes = await _normalize_offices(capture)
    outputs, counts = _write_outputs(capture.store, resources_by_type)
    receipts = list(capture.store.requests())
    manifest_by_field = _capture_manifest(capture, outputs, receipts, start, end, accounting, other_codes)
    proof_by_field = {
        "adapter_id": ADAPTER_ID,
        "normalized_output_only": True,
        "resource_counts": counts,
        "publisher_locations": end,
        "network_npis": {name: len(npis) for name, npis in npis_by_network.items()},
        "retained_response_count": len(receipts),
        "http_attempts": capture.http.requests,
        "identity_reuses": len(capture.identity_reuses),
        "saturated_leaves_closed_by_global_census": len(capture.unresolved),
        "observation_start": min(x["observed_at"] for x in receipts),
        "observation_end": max(x["observed_at"] for x in receipts),
        "outside_profile_count": accounting["outside_profile_count"],
        "profiles_without_npi": accounting["profiles_without_npi"],
        "other_network_id_count": len(other_codes),
    }
    manifest_by_field["proof"] = proof_by_field
    proof_by_field["capture_sha256"] = hashlib.sha256(canonical_json(manifest_by_field).encode()).hexdigest()
    capture.store.checkpoint("complete", manifest_by_field)
    # Import here keeps acquisition state and normalized output independently testable.
    from process.vsp_directory_capture import CompletedVSPCapture

    return CompletedVSPCapture(capture.store.path, manifest_by_field, capture.store.capture_id)
