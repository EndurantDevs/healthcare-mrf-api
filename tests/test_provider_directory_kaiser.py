# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fake-server tests for the sparse offset windows, _include crawl and reference repair of one public directory."""

import contextlib
import importlib
import urllib.parse
from collections import Counter

import pytest

from process import provider_directory_kaiser as kaiser
from process import provider_directory_source_selection as source_selection
from process import provider_directory_uhc_scan as uhc_scan

importer = importlib.import_module("process.provider_directory_fhir")
BASE = kaiser.API_BASE
SOURCE_ID = "pdfhir_67f47e3af69543ce63cfbf6d"
LOGICAL_ROLES = 1500
WINDOW = 250


def _is_visible(role_number):
    # Every third row is filtered inside its window, and windows 2 and 3 are filtered completely.
    return role_number % 3 and not 2 * WINDOW <= role_number < 4 * WINDOW


VISIBLE_ROLE_IDS = {f"role-{number:04d}" for number in range(LOGICAL_ROLES) if _is_visible(number)}


def _role(number):
    return {
        "resourceType": "PractitionerRole",
        "id": f"role-{number:04d}",
        "practitioner": {"reference": f"Practitioner/prac-{number % 7}"},
        "location": [{"reference": f"Location/loc-{number % 5}"}],
    }


def _included(reference):
    resource_type, resource_id = reference.split("/")
    return {"resourceType": resource_type, "id": resource_id}


def _window(url):
    """Serve one offset window: rows are cut first, then filtered; includes ride on top of the matches."""
    query_by_name = dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(url).query))
    assert query_by_name["_sort"] == "_id" and query_by_name["_include"] == "*"
    offset, count = int(query_by_name["_offset"]), int(query_by_name["_count"])
    roles = [_role(number) for number in range(offset, min(offset + count, LOGICAL_ROLES)) if _is_visible(number)]
    references = dict.fromkeys(
        reference
        for role in roles
        for reference in (role["practitioner"]["reference"], role["location"][0]["reference"])
    )
    bundle_by_field = {
        "resourceType": "Bundle",
        "type": "searchset",
        "total": LOGICAL_ROLES,
        "entry": [{"resource": role, "search": {"mode": "match"}} for role in roles]
        + [{"resource": _included(reference), "search": {"mode": "include"}} for reference in references],
    }
    if offset + count < LOGICAL_ROLES:
        next_query = urllib.parse.urlencode({**query_by_name, "_offset": offset + count})
        bundle_by_field["link"] = [{"relation": "next", "url": f"{BASE}/PractitionerRole?{next_query}"}]
    return bundle_by_field


def _source_row():
    return importer._source_row_from_seed(kaiser.seed_rows()[0])


class FakeKaiserCursor:
    """Hold checkpoint state and record requests around the real shared fetch loop."""

    def __init__(self, saved=None):
        self.requests, self.saved = [], saved
        self.persisted = Counter()
        self.context = importer.PaginationCheckpointContext(
            canonical_api_base=BASE,
            source_scope_hash="synthetic_kaiser_scope",
            source_ids=(SOURCE_ID,),
            owner_run_id="synthetic_kaiser_run",
            acquisition_root_run_id="synthetic_kaiser_run",
            dataset_id="synthetic_kaiser_dataset",
        )

    async def load(self, _context, _resource_type, start_url):
        return self.saved or importer.PaginationResumeState(start_url, 0, 0, ())

    async def save(self, _context, _resource_type, **values):
        self.saved = importer.PaginationResumeState(
            values["next_url"],
            values["pages_processed"],
            values["rows_processed"],
            tuple(values["recent_url_hashes"]),
            complete=values["next_url"] is None,
        )

    async def fetch(self, _source, url, **_options):
        self.requests.append(url)
        return 200, _window(url), None, 1

    async def persist(self, model, resource_rows):
        self.persisted.update((importer.RESOURCE_TYPES_BY_MODEL[model], row["resource_id"]) for row in resource_rows)
        return len(resource_rows)

    async def run(self, monkeypatch, run_id):
        monkeypatch.setattr(importer, "_load_or_initialize_pagination_checkpoint", self.load)
        monkeypatch.setattr(importer, "_save_pagination_checkpoint", self.save)
        monkeypatch.setattr(importer, "_fetch_source_json", self.fetch)
        monkeypatch.setattr(importer, "source_checkpoint_transaction", lambda *_a, **_k: contextlib.nullcontext())
        return await importer._fetch_resource_rows(
            _source_row(),
            "PractitionerRole",
            per_resource_limit=0,
            page_limit=0,
            page_count=WINDOW,
            timeout=3,
            run_id=run_id,
            row_batch_handler=self.persist,
            retain_rows=False,
            pagination_checkpoint=self.context,
        )

    def stored_ids(self, resource_type):
        return {resource_id for stored_type, resource_id in self.persisted if stored_type == resource_type}


def _offsets(urls):
    return [int(dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(url).query))["_offset"]) for url in urls]


@pytest.mark.asyncio
async def test_sparse_and_empty_windows_store_every_visible_row_and_include_once(monkeypatch):
    cursor = FakeKaiserCursor()
    result = await cursor.run(monkeypatch, "synthetic_full_run")

    assert _offsets(cursor.requests) == list(range(0, LOGICAL_ROLES, WINDOW))
    assert result.complete and not result.error and result.rows_fetched == len(VISIBLE_ROLE_IDS)
    assert cursor.stored_ids("PractitionerRole") == VISIBLE_ROLE_IDS
    assert max(cursor.persisted.values()) == 1
    visible_numbers = [int(role_id.removeprefix("role-")) for role_id in VISIBLE_ROLE_IDS]
    assert cursor.stored_ids("Practitioner") == {f"prac-{number % 7}" for number in visible_numbers}
    assert cursor.stored_ids("Location") == {f"loc-{number % 5}" for number in visible_numbers}
    assert cursor.saved.complete


@pytest.mark.asyncio
async def test_resume_inside_empty_run_keeps_include_and_reads_rest(monkeypatch):
    resume_url = f"{BASE}/PractitionerRole?_count={WINDOW}&_include=*&_sort=_id&_offset={3 * WINDOW}"
    cursor = FakeKaiserCursor(saved=importer.PaginationResumeState(resume_url, 3, 333, (), resumed=True))
    result = await cursor.run(monkeypatch, "synthetic_resumed_run")

    assert _offsets(cursor.requests) == [3 * WINDOW, 4 * WINDOW, 5 * WINDOW]
    assert result.complete and not result.error
    assert cursor.stored_ids("PractitionerRole") == {
        role_id for role_id in VISIBLE_ROLE_IDS if int(role_id.removeprefix("role-")) >= 3 * WINDOW
    }


def test_seed_keeps_source_id_and_crawls_three_link_types():
    row = _source_row()
    assert row["source_id"] == SOURCE_ID == importer._stable_source_id(kaiser.seed_rows()[0])
    assert not importer._is_manual_only_source(row)
    assert source_selection.acquisition_blocked_reason(importer, row, list(kaiser.CRAWLED_RESOURCES)) is None
    page_count = source_selection.full_refresh_page_counts(importer)[BASE]
    start = urllib.parse.urlsplit(importer._resource_start_url(row, "PractitionerRole", page_count=page_count))
    assert dict(urllib.parse.parse_qsl(start.query)) == {
        "_count": "250",
        "_include": "*",
        "_sort": "_id",
        "_offset": "0",
    }
    assert importer._resource_start_url(row, "Practitioner", page_count=page_count) is None


class RepairServer:
    """Serve stored rows from a mutable store and answer _id lists and reads by id."""

    def __init__(self, served_by_key, failing_keys=()):
        self.rows_by_type = {
            "PractitionerRole": [
                {
                    "resource_id": "role-1",
                    "practitioner_ref": "Practitioner/prac-1",
                    "organization_ref": "Organization/org-gone",
                    "location_refs": ["Location/loc-1"],
                    "healthcare_service_refs": ["HealthcareService/hs-1"],
                }
            ],
            "OrganizationAffiliation": [{"resource_id": "aff-1", "organization_ref": "Organization/org-1"}],
            "InsurancePlan": [{"resource_id": "plan-1"}],
            "Practitioner": [{"resource_id": "prac-1"}],
            "Organization": [{"resource_id": "org-1"}],
            "Location": [{"resource_id": "loc-1"}],
            "HealthcareService": [],
        }
        self.served_by_key, self.failing_keys = served_by_key, set(failing_keys)
        self.advertised_after = 100

    async def stored_rows(self, _directory, dataset_id, resource_type):
        assert dataset_id == "synthetic_kaiser_dataset"
        for stored_row in list(self.rows_by_type[resource_type]):
            yield stored_row

    async def persist(self, model, resource_rows):
        self.rows_by_type[importer.RESOURCE_TYPES_BY_MODEL[model]].extend(resource_rows)
        return len(resource_rows)

    def _resource(self, key):
        if key in self.failing_keys:
            return 500, {}, None, 1
        served = self.served_by_key.get(key)
        return (200, served, None, 1) if served else (404, {"resourceType": "OperationOutcome"}, None, 1)

    async def fetch(self, _source, url, **_options):
        parsed = urllib.parse.urlsplit(url)
        resource_type, _, resource_id = parsed.path.removeprefix(urllib.parse.urlsplit(BASE).path + "/").partition("/")
        query_by_name = dict(urllib.parse.parse_qsl(parsed.query))
        if query_by_name.get("_summary") == "count":
            return 200, {"resourceType": "Bundle", "total": self.advertised_after}, None, 1
        if resource_id:
            return self._resource(f"{resource_type}/{resource_id}")
        keys = [f"{resource_type}/{requested_id}" for requested_id in query_by_name["_id"].split(",")]
        if any(key in self.failing_keys for key in keys):
            return 500, {}, None, 1
        entries = [{"resource": self.served_by_key[key]} for key in keys if key in self.served_by_key]
        return 200, {"resourceType": "Bundle", "type": "searchset", "entry": entries}, None, 1


SERVED = {
    "HealthcareService/hs-1": {
        "resourceType": "HealthcareService",
        "id": "hs-1",
        "providedBy": {"reference": "Organization/org-2"},
    },
    "Organization/org-2": {"resourceType": "Organization", "id": "org-2", "name": "Synthetic Medical Group"},
}


async def _close(monkeypatch, server, run_id, *, minimum_rows=1):
    monkeypatch.setattr(uhc_scan, "_stored_rows", server.stored_rows)
    monkeypatch.setattr(uhc_scan, "RETRY_DELAY_SECONDS", 0.0)
    monkeypatch.setattr(importer, "_fetch_source_json", server.fetch)
    monkeypatch.setattr(importer, "is_source_storage_required", lambda: True)
    monkeypatch.setattr(kaiser, "MINIMUM_ROWS", dict.fromkeys(kaiser.CRAWLED_RESOURCES, minimum_rows))
    for resource_type in kaiser.CRAWLED_RESOURCES:
        monkeypatch.setitem(kaiser._TOTALS, (SOURCE_ID, run_id, resource_type), 100)
    diagnostics_by_type = {
        resource_type: {"complete": True, "error": None, "bounded": False} for resource_type in kaiser.CRAWLED_RESOURCES
    }
    count_by_type = {}
    await uhc_scan.close_references(
        importer,
        {**_source_row(), "_endpoint_dataset_id": "synthetic_kaiser_dataset"},
        server.persist,
        diagnostics_by_type,
        count_by_type,
        timeout=3,
        run_id=run_id,
    )
    return diagnostics_by_type, count_by_type


@pytest.mark.asyncio
async def test_repair_reads_missing_nodes_and_their_second_hop_and_counts_404(monkeypatch):
    server = RepairServer(SERVED)
    diagnostics_by_type, count_by_type = await _close(monkeypatch, server, "synthetic_repair_run")

    assert {row["resource_id"] for row in server.rows_by_type["HealthcareService"]} == {"hs-1"}
    assert {row["resource_id"] for row in server.rows_by_type["Organization"]} == {"org-1", "org-2"}
    role_metrics = diagnostics_by_type["PractitionerRole"]["kaiser_acquisition"]
    assert role_metrics["reference_repair"]["Organization"] == {"fetched": 1, "missing": 1, "failed": 0}
    assert role_metrics["advertised_total_before"] == role_metrics["advertised_total_after"] == 100
    assert role_metrics["unresolved_references"] == 0
    assert all(diagnostic["error"] is None for diagnostic in diagnostics_by_type.values())
    assert count_by_type == {"Practitioner": 1, "Organization": 2, "Location": 1, "HealthcareService": 1}


@pytest.mark.asyncio
async def test_unreadable_reference_fails_the_crawl(monkeypatch):
    server = RepairServer(SERVED, failing_keys={"Organization/org-gone"})
    diagnostics_by_type, _counts = await _close(monkeypatch, server, "synthetic_failed_repair_run")

    assert {diagnostic["error"] for diagnostic in diagnostics_by_type.values()} == {kaiser.REFERENCE_REPAIR_ERROR}


@pytest.mark.asyncio
async def test_advertised_total_drift_fails_the_crawl(monkeypatch):
    server = RepairServer(SERVED)
    server.advertised_after = 102
    diagnostics_by_type, _counts = await _close(monkeypatch, server, "synthetic_drift_run")

    assert {diagnostic["error"] for diagnostic in diagnostics_by_type.values()} == {kaiser.TOTAL_DRIFT_ERROR}


@pytest.mark.asyncio
async def test_rows_below_the_previous_edition_fail_the_crawl(monkeypatch):
    server = RepairServer(SERVED)
    diagnostics_by_type, _counts = await _close(monkeypatch, server, "synthetic_short_run", minimum_rows=2)

    assert {diagnostic["error"] for diagnostic in diagnostics_by_type.values()} == {kaiser.ROWS_BELOW_MINIMUM_ERROR}
