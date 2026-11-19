# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Resumable VSP acquisition with shared offices and publisher census reconciliation."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import aiohttp

from process.provider_directory_projection_fhir_values import is_valid_npi
from process.vsp_directory_contract import (
    ADAPTER_ID,
    CENSUS_OFFICE_DRIFT_LIMIT,
    ENDPOINT_SCOPE,
    MAX_LOCATIONS,
    NETWORKS,
    OFFICE_CENSUS_UNRESOLVED,
    RESOURCE_TYPES,
    SEARCH_URL,
    SOURCE_ID,
    canonical_json,
    office_position,
    profile_url,
    public_id,
    require,
    search_result,
    validate_census_memberships,
)
from process.vsp_directory_enrichment import enrich_capture, verify_profile_site_memberships, write_normalized_capture
from process.vsp_directory_geometry import census_request, distance_miles, world_tiles
from process.vsp_directory_http import VSPHTTP, strict_json
from process.vsp_directory_normalization import company_summary_name, is_company_summary
from process.vsp_directory_store import VSPCaptureStore, configured_root

_OFFICE_POSITION_TOLERANCE_MILES = 1.0 / 1609.344


def _same_office_position(previous, office, position):
    prior = office_position(previous)
    if prior == position:
        return True
    if (0.0, 0.0) in (prior, position):
        return False
    original_address, incoming_address = previous["addr"], office["addr"]
    return (
        previous.get("locationType") == office.get("locationType")
        and all(original_address.get(key) for key in ("street1", "city", "stateCode", "zipCode"))
        and all(
            original_address.get(key) == incoming_address.get(key)
            for key in ("street1", "street2", "city", "stateCode", "zipCode", "countryCode")
        )
        and distance_miles(*prior, *position) <= _OFFICE_POSITION_TOLERANCE_MILES
    )


@dataclass(frozen=True)
class CompletedVSPCapture:
    """Closed normalized output; readers verify its exact bytes before publication."""

    path: Path
    manifest: dict
    capture_id: str

    @property
    def proof(self):
        """Return compact source proof, excluding provider rows and request URLs."""
        return self.manifest["proof"]

    @property
    def resource_counts(self):
        """Include every normalized result family, including empty ones."""
        return self.proof["resource_counts"]

    def iter_resources(self):
        """Stream sorted unique resources and verify each immutable family hash/count."""
        proof_by_field = {key: field_value for key, field_value in self.proof.items() if key != "capture_sha256"}
        manifest_by_field = {**self.manifest, "proof": proof_by_field}
        require(
            self.manifest.get("capture_id") == self.capture_id
            and self.manifest.get("source_id") == SOURCE_ID
            and self.manifest.get("adapter_id") == ADAPTER_ID,
            "vsp_capture_identity_changed",
        )
        require(
            hashlib.sha256(canonical_json(manifest_by_field).encode()).hexdigest() == self.proof["capture_sha256"],
            "vsp_capture_manifest_changed",
        )
        require(set(self.resource_counts) == set(RESOURCE_TYPES), "vsp_normalized_families_invalid")
        previous = None
        for kind in sorted(RESOURCE_TYPES):
            descriptor = self.manifest["outputs"].get(kind)
            if descriptor is None:
                require(self.resource_counts[kind] == 0, "vsp_normalized_output_missing")
                continue
            require(
                descriptor["filename"] == kind + "-" + descriptor["sha256"] + ".ndjson"
                and len(descriptor["sha256"]) == 64
                and all(x in "0123456789abcdef" for x in descriptor["sha256"]),
                "vsp_normalized_output_invalid",
            )
            digest, count = hashlib.sha256(), 0
            with (self.path / "normalized" / descriptor["filename"]).open("rb") as stream:
                for line in stream:
                    digest.update(line)
                    resource = strict_json(line)
                    key = (resource.get("resourceType"), resource.get("id"))
                    require(
                        key[0] == kind and type(key[1]) is str and (previous is None or key > previous),
                        "vsp_normalized_order_invalid",
                    )
                    previous, count = key, count + 1
                    yield resource
            require(
                count == self.resource_counts[kind] and digest.hexdigest() == descriptor["sha256"],
                "vsp_normalized_output_changed",
            )


class VSPCapture:
    """Retain every successful query; replay checkpoints without repeating origin work."""

    def __init__(self, store, http, progress_callback, cancel_check):
        self.store, self.http = store, http
        self.progress_callback, self.cancel_check = progress_callback, cancel_check
        self.offices, self.identities, self.profiles = {}, {}, {}
        self.search_memberships = {}
        self.site_memberships = {name: set() for name in NETWORKS.values()}
        self.geographic_additions = {name: set() for name in NETWORKS.values()}
        self.memberships = {name: set() for name in NETWORKS.values()}
        self.partitions, self.unresolved = [], []
        self.exhausted_networks = set()
        self.census_shortfalls_by_network = {}
        self.cache_hits = 0
        self.identity_reuses = set()
        self._sizes, self._total_bytes = {}, 0

    def account_bytes(self, key, field_value):
        """Bound the in-memory source index independently of the durable raw capture."""
        size = len(canonical_json(field_value).encode())
        self._total_bytes += size - self._sizes.get(key, 0)
        require(self._total_bytes <= 512 * 1024 * 1024, "vsp_source_index_byte_budget_exhausted")
        self._sizes[key] = size

    async def progress(self, phase, **fields):
        """Keep cancellation and import progress live while long acquisition proceeds."""
        await self.cancel_check()
        await self.progress_callback(
            {
                "phase": phase,
                "request_count": self.http.requests,
                "office_count": len(self.offices),
                "retained_reuses": self.cache_hits,
                **fields,
            }
        )

    async def fetch(self, method, url, payload=None, *, fresh=False):
        """Use exact retained request identities, except explicit final census checks."""
        await self.cancel_check()
        retained = None if fresh else self.store.lookup(method, url, payload)
        if retained is not None:
            self.cache_hits += 1
            return retained[0], retained[1]
        field_value, raw, metadata = await self.http.request(method, url, payload)
        self.store.retain(raw, metadata)
        await self.progress("acquisition")
        return field_value, metadata

    def remember(self, office, request_id, *, new_doctor_ids=None):
        """Preserve distinct offices and NPI identities without cross-office memberships."""
        oid = public_id(office.get("pid"))
        position = office_position(office)
        profile_url(office)
        require(type(office.get("providerSummaries")) is list, "vsp_office_doctors_invalid")
        incoming_ids = [public_id(x.get("pid")) for x in office["providerSummaries"] if type(x) is dict]
        require(
            len(incoming_ids) == len(office["providerSummaries"]) and len(set(incoming_ids)) == len(incoming_ids),
            "vsp_office_doctors_duplicated",
        )
        previous = self.offices.get(oid)
        if previous is not None:
            require(_same_office_position(previous, office, position), "vsp_office_position_changed")
        else:
            require(len(self.offices) < 200000, "vsp_office_budget_exhausted")
        summaries_by_doctor = {public_id(x.get("pid")): x for x in (previous or {}).get("providerSummaries", [])}
        for summary in office["providerSummaries"]:
            require(type(summary) is dict, "vsp_office_doctors_invalid")
            did = public_id(summary.get("pid"))
            if new_doctor_ids is not None and did not in new_doctor_ids and did not in summaries_by_doctor:
                continue
            npi = summary.get("npi")
            has_valid_npi = type(npi) is str and is_valid_npi(npi)
            if has_valid_npi:
                prior = self.identities.get(did)
                resource_type = "Organization" if is_company_summary(office, summary) else "Practitioner"
                require(
                    prior is None
                    or prior["summary"]["npi"] == npi
                    and prior.get("resource_type", "Practitioner") == resource_type
                    and (
                        resource_type != "Organization"
                        or company_summary_name(prior["summary"]) == company_summary_name(summary)
                    ),
                    "vsp_doctor_identity_changed",
                )
                self.identities[did] = {"summary": summary, "request_key": request_id, "resource_type": resource_type}
            if did not in summaries_by_doctor or has_valid_npi:
                summaries_by_doctor[did] = summary
        # Keep the first office observation paired with its exact dated profile.
        self.offices[oid] = {**(previous or office), "providerSummaries": list(summaries_by_doctor.values())}
        self.account_bytes("office:" + oid, self.offices[oid])

    def remember_search_scope(self, oid, network, doctor_ids):
        """Retain same-office doctor membership from a single-network search."""
        require(network in NETWORKS.values(), "vsp_network_invalid")
        scopes = self.search_memberships.setdefault(oid, {})
        scopes.setdefault(network, set()).update(doctor_ids)
        self.account_bytes("search:" + oid, {name: sorted(ids) for name, ids in scopes.items()})

    def remember_search(self, office, network, request_id, *, scope_method=None):
        """Record scoped roster evidence before merging offices across networks."""
        self.remember(office, request_id)
        oid = public_id(office["pid"])
        doctor_ids = {public_id(summary["pid"]) for summary in office["providerSummaries"]}
        if doctor_ids:
            self.remember_search_scope(oid, network, doctor_ids)
        self.memberships[network].add(oid)
        if scope_method is None:
            self.geographic_additions[network].discard(oid)
        elif scope_method == "POST" and oid not in self.site_memberships[network]:
            self.geographic_additions[network].add(oid)
        self.site_memberships[network].add(oid)
        self.account_bytes("site:" + oid, sorted(name for name, sites in self.site_memberships.items() if oid in sites))

    @staticmethod
    def _census_count(response):
        require(
            type(response) is dict
            and not response.get("errors")
            and type(response.get("totalCounts")) is int
            and response["totalCounts"] > 0,
            "vsp_census_invalid",
        )
        return response["totalCounts"]

    async def census(self, *, fresh=False):
        """Independent single-network counts avoid the publisher's first-network-only trap."""
        counts_by_network = {}
        for name in sorted(NETWORKS.values()):
            response, _ = await self.fetch("POST", SEARCH_URL, census_request(name), fresh=fresh)
            counts_by_network[name] = self._census_count(response)
        return counts_by_network

    async def discover(self, network, expected):
        """Cover the globe; source counts decide subdivision and capture completion."""
        pending_tiles = list(reversed(world_tiles()))
        self.exhausted_networks.discard(network)
        while pending_tiles:
            tile = pending_tiles.pop()
            field_value, receipt = await self.fetch("POST", SEARCH_URL, tile.request(network))
            total, offices = search_result(field_value)
            for office in offices:
                self.remember_search(office, network, receipt["request_key"])
            observed = len(self.site_memberships[network])
            require(observed <= expected + CENSUS_OFFICE_DRIFT_LIMIT, "vsp_census_changed_during_capture")
            if observed == expected or observed - len(self.geographic_additions[network]) >= expected:
                self.partitions.append(
                    {
                        "network": network,
                        "tile": tile.key,
                        "count": total,
                        "request_key": receipt["request_key"],
                        "closed_by_global_census": observed == expected,
                    }
                )
                break
            if total < MAX_LOCATIONS:
                self.partitions.append(
                    {"network": network, "tile": tile.key, "count": total, "request_key": receipt["request_key"]}
                )
            else:
                points = {office_position(x) for x in offices}
                is_saturated = (
                    len(offices) == MAX_LOCATIONS and len(points) == 1 and tile.has_point(*next(iter(points)))
                )
                if is_saturated or tile.depth == 24:
                    self.unresolved.append(
                        {"network": network, "tile": tile.key, "count": total, "request_key": receipt["request_key"]}
                    )
                else:
                    pending_tiles.extend(reversed(tile.split()))
            self.store.checkpoint(
                "discovery",
                {
                    "network": network,
                    "tile": tile.key,
                    "pending": len(pending_tiles),
                    "office_count": len(self.offices),
                },
            )
            await self.progress("discovery", network=network, pending_tiles=len(pending_tiles))
        else:
            if all(leaf["network"] != network for leaf in self.unresolved):
                self.exhausted_networks.add(network)

    async def capture_network(self, name):
        """Establish each network's independent count and enrich newly discovered offices."""
        request = census_request(name)
        retained = self.store.lookup("POST", SEARCH_URL, request)
        response, _ = await self.fetch("POST", SEARCH_URL, request, fresh=True)
        refreshed = self._census_count(response)
        start = self._census_count(retained[0]) if retained is not None else refreshed
        require(abs(start - refreshed) <= CENSUS_OFFICE_DRIFT_LIMIT, "vsp_census_changed_during_capture")
        publisher_counts = {start, refreshed}
        needs_discovery = len(self.site_memberships[name]) not in publisher_counts
        if needs_discovery:
            # A lower refresh must not close discovery before the retained census is accounted for.
            await self.discover(name, max(publisher_counts))
        baseline_addition_ids = (
            self.geographic_additions[name].copy() if len(self.site_memberships[name]) in publisher_counts else set()
        )
        if needs_discovery:
            await enrich_capture(self)
        require(len(self.site_memberships[name]) <= refreshed + CENSUS_OFFICE_DRIFT_LIMIT, OFFICE_CENSUS_UNRESOLVED)
        return start, refreshed, baseline_addition_ids

    async def run(self):
        """Refresh one network at a time, sharing office profiles across networks."""
        start, refreshed, additions_at_discovery_by_network = {}, {}, {}
        for name in NETWORKS.values():
            start[name], refreshed[name], additions_at_discovery_by_network[name] = await self.capture_network(name)
        scoped_additions = await verify_profile_site_memberships(self, additions_at_discovery_by_network)
        end = await self.census(fresh=True)
        observed_by_network = {name: len(self.memberships[name]) for name in NETWORKS.values()}
        self.census_shortfalls_by_network = validate_census_memberships(
            start,
            refreshed,
            end,
            observed_by_network,
            scoped_additions=scoped_additions,
            exhausted_networks=self.exhausted_networks,
        )
        # Even a saturated spatial leaf is closed only by the independent global census.
        return await write_normalized_capture(self, start, end, refreshed=refreshed, scoped_additions=scoped_additions)


async def _noop(*_args):
    return None


def _saved_binding(store, proxy_binding):
    require(proxy_binding is None or callable(getattr(proxy_binding, "select", None)), "vsp_source_proxy_required")
    prior = store.load_checkpoint("binding")
    binding_by_field = {
        "policy_revision": getattr(proxy_binding, "policy_revision", None),
        "pool_revision": getattr(proxy_binding, "pool_revision", None),
    }
    require(
        proxy_binding is None or all(type(revision) is str and revision for revision in binding_by_field.values()),
        "vsp_source_proxy_required",
    )
    if prior is not None:
        require(
            all(prior.get(key) == field_value for key, field_value in binding_by_field.items()),
            "vsp_capture_binding_changed",
        )
        if proxy_binding is not None:
            proxy_binding.restore_cooldowns(prior["cooldowns"])
        else:
            require(prior["cooldowns"] is None, "vsp_capture_binding_changed")
    return prior, binding_by_field


def _bound_http(store, session, proxy_binding, cancel_check, *, require_retained=False):
    """Restore counted attempts and proxy cooldowns before another origin request."""
    prior, binding_by_field = _saved_binding(store, proxy_binding)
    require(not require_retained or prior is not None, "vsp_retained_capture_required")

    async def pause_and_account(boundary, cooldowns):
        """Persist each counted attempt and cooldown before sending."""
        await store.pause(boundary, cooldowns)
        store.checkpoint("binding", {**binding_by_field, "attempts": http.requests, "cooldowns": cooldowns})

    http = VSPHTTP(session, proxy_binding, pause=pause_and_account, cancel_check=cancel_check)
    if prior is not None:
        require(
            type(prior.get("attempts")) is int and 0 <= prior["attempts"] <= http.max_requests,
            "vsp_capture_attempts_invalid",
        )
        http.requests = prior["attempts"]
    return http


async def _with_capture(
    source_record, root_run_id, proxy_binding, progress_callback, cancel_check, *, probe, require_retained=False
):
    """Hold one source lock across acquisition and its closing verification."""
    require(
        source_record.get("source_id") == SOURCE_ID and source_record.get("canonical_api_base") == ENDPOINT_SCOPE,
        "vsp_source_identity_invalid",
    )
    store = VSPCaptureStore(configured_root(), root_run_id, require_retained=require_retained)
    store.acquire()
    try:
        complete = store.load_checkpoint("complete")
        if complete is not None and not probe:
            prior, _ = _saved_binding(store, proxy_binding)
            require(
                prior is not None
                and complete.get("capture_id") == store.capture_id
                and complete.get("source_id") == SOURCE_ID
                and complete.get("adapter_id") == ADAPTER_ID,
                "vsp_capture_identity_changed",
            )
            return CompletedVSPCapture(store.path, complete, store.capture_id)
        async with aiohttp.ClientSession(
            auto_decompress=False, trust_env=False, cookie_jar=aiohttp.DummyCookieJar()
        ) as session:
            http = _bound_http(store, session, proxy_binding, cancel_check, require_retained=require_retained)
            pause = store.pause_state()
            if pause is not None:
                boundary = datetime.fromisoformat(pause["not_before"])
                await http.wait(max(0, (boundary - datetime.now(UTC)).total_seconds()))
            capture = VSPCapture(store, http, progress_callback, cancel_check)
            if probe:
                census_result, receipt = await capture.fetch("POST", SEARCH_URL, census_request("Signature"))
                require(
                    type(census_result.get("totalCounts")) is int and census_result["totalCounts"] > 0,
                    "vsp_census_invalid",
                )
                return {
                    "network": "Signature",
                    "total_count": census_result["totalCounts"],
                    "capture_sha256": receipt["sha256"],
                    "complete": False,
                }
            return await capture.run()
    finally:
        store.release()


async def capture_vsp_directory(
    *, source_record, root_run_id, proxy_binding, progress_callback, cancel_check, require_retained=False
):
    """Acquire all three network scopes using the source worker's configured transport."""
    return await _with_capture(
        source_record,
        root_run_id,
        proxy_binding,
        progress_callback,
        cancel_check,
        probe=False,
        require_retained=require_retained,
    )


async def probe_vsp_directory(*, source_record, root_run_id, proxy_binding):
    """Retain one probe without implying that a network or resource census completed."""
    return await _with_capture(source_record, root_run_id, proxy_binding, _noop, _noop, probe=True)
