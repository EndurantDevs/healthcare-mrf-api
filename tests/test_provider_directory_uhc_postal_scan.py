"""Focused fake-HTTP tests for the capped UHC FHIR partition route."""

import asyncio
import gzip
import importlib
import json
import urllib.parse
from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import replace
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, Mock

import aiohttp
import pytest
from aiohttp import web

from db.models import (
    ProviderDirectoryLocation,
    ProviderDirectoryPractitioner,
    ProviderDirectoryPractitionerRole,
)
from process import provider_directory_postal_checkpoint as postal_checkpoint
from process import provider_directory_source_proxy as source_proxy
from process import provider_directory_source_selection as source_selection
from process import provider_directory_uhc_scan as uhc_scan
from process.provider_directory_generic_source_storage import _require_postal_start_checkpoint
from process.provider_directory_source_storage_binding import NamedFHIRSourceStorage

importer = importlib.import_module("process.provider_directory_fhir")
BASE = importer.UHC_PROVIDER_DIRECTORY_BASE
SOURCE = {"source_id": "uhc_synthetic", "api_base": BASE, "canonical_api_base": BASE}
ROLE_PARAM = "location.address-postalcode"


def _options(**overrides: Any) -> importer.PartitionFetchOptions:
    return importer.PartitionFetchOptions(
        **{
            "timeout": 3,
            "run_id": None,
            "row_batch_handler": None,
            "row_batch_size": 100,
            "retain_rows": True,
            "deadline_at": None,
            "max_pages": 0,
            **overrides,
        }
    )


def _query(url: str) -> dict[str, str]:
    return dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(url).query))


def _bundle(total: int, resource_type: str = "PractitionerRole", resource_id: str | None = None) -> dict[str, Any]:
    entries = [{"resource": {"resourceType": resource_type, "id": resource_id}}] if resource_id else []
    return {"resourceType": "Bundle", "total": total, "entry": entries}


def _role_url(prefix: str) -> str:
    return f"{BASE}/PractitionerRole?_count=100&{urllib.parse.urlencode({ROLE_PARAM: prefix})}"


@pytest.fixture(autouse=True)
def _no_retry_delay(monkeypatch):
    monkeypatch.setattr(uhc_scan, "LEAF_RETRY_DELAYS_SECONDS", (0.0, 0.0))
    monkeypatch.setattr(uhc_scan, "RETRY_DELAY_SECONDS", 0.0)


def _organization_seed_pages(next_url):
    first = _bundle(2, "Organization", "network-one")
    first["entry"][0]["resource"]["type"] = [{"coding": [{"code": "ntwk"}]}]
    first["entry"].append({"resource": {"resourceType": "Organization", "id": "not-a-network"}})
    first["link"] = [{"relation": "next", "url": next_url}]
    second = _bundle(2, "Organization", "network-two")
    second["entry"][0]["resource"]["type"] = [{"coding": [{"code": "ntwk"}]}]
    second["entry"].append(first["entry"][0])
    first["entry"].append(
        {
            "resource": {
                "resourceType": "Organization",
                "id": "subsetted-network",
                "type": [{"coding": [{"code": "ntwk"}]}],
                "meta": {"tag": [{"code": "SUBSETTED"}]},
            }
        }
    )
    return first, second


@pytest.mark.asyncio
@pytest.mark.parametrize("next_status", (200, 504))
async def test_optum_network_seed_follows_cursor_before_required_organization_scan(monkeypatch, next_status):
    next_url = f"{BASE}?_getpages=synthetic&_getpagesoffset=100&_count=100"
    first_page, second_page = _organization_seed_pages(next_url)
    visits = []
    written_ids = []

    async def fetch(_source, url, **_kwargs):
        visits.append(url)
        assert source_proxy.PARTITION_CURSOR_KEY.get() == "Organization:type=ntwk"
        if url == next_url:
            return next_status, second_page if next_status == 200 else None, None, 1
        assert _query(url) == {"_count": "100", "type": "ntwk"}
        return 200, first_page, None, 1

    async def persist(resource_type, model, rows):
        assert resource_type == "Organization" and model is importer.ProviderDirectoryOrganization
        written_ids.extend(row["resource_id"] for row in rows)
        return len(rows)

    async def scan(resource_type, **_kwargs):
        assert resource_type == "Organization"
        assert source_proxy.PARTITION_CURSOR_KEY.get() is None
        assert written_ids == (["network-one", "network-two"] if next_status == 200 else ["network-one"])
        return "required-scan"

    monkeypatch.setattr(importer, "_fetch_source_json", fetch)
    monkeypatch.setattr(importer, "is_source_storage_required", lambda: True)
    monkeypatch.setattr(source_proxy, "current_run", lambda: SimpleNamespace(enabled=lambda _source: True))
    runner = SimpleNamespace(
        source_record={**SOURCE, "_endpoint_dataset_id": "isolated-candidate"},
        options=SimpleNamespace(
            per_resource_limit=0,
            page_limit=0,
            timeout=3,
            run_id="synthetic-run",
            stream_batch_size=2,
            resource_deadline_seconds=0,
            cancel_ctx=None,
            cancel_task=None,
        ),
        is_streaming=True,
        effective_concurrency=1,
        persist_batch=persist,
        import_one=scan,
        count_by_resource={"Organization": 0},
    )
    role_done = asyncio.Event()
    role_done.set()
    scan_result = await importer.ResourceGroupScanRunner._run_bounded(
        runner, "Organization", asyncio.Semaphore(1), role_done
    )

    assert scan_result == "required-scan"
    assert visits[1:] == [next_url]
    assert runner.count_by_resource["Organization"] == len(written_ids)


@pytest.mark.parametrize(
    ("prefix", "first_child", "last_child"),
    (
        ("7703", "77030", "77039"),
        ("77030", "77030-0", "77030-9"),
        ("77030-1", "77030-10", "77030-19"),
        ("77030-12", "77030-120", "77030-129"),
        ("77030-123", "77030-1230", "77030-1239"),
    ),
)
def test_uhc_postal_descent_reaches_zip4(prefix, first_child, last_child):
    children = importer._uhc_adaptive_partition_child_urls(
        SOURCE, "PractitionerRole", _role_url(prefix), _bundle(10000)
    )

    assert len(children) == 10
    assert _query(children[0])[ROLE_PARAM] == first_child
    assert _query(children[-1])[ROLE_PARAM] == last_child


def test_uhc_full_zip4_cap_is_exhausted():
    leaf_url = _role_url("77030-1234")

    assert importer._uhc_adaptive_partition_child_urls(SOURCE, "PractitionerRole", leaf_url, _bundle(10000)) == []
    assert importer._is_uhc_partition_cap_exhausted(SOURCE, leaf_url, _bundle(10000))
    assert importer._is_uhc_role_zip_cap_hit(SOURCE, "PractitionerRole", leaf_url, _bundle(10000))


@pytest.mark.asyncio
@pytest.mark.parametrize("parent", (None, "failed-run"))
async def test_named_role_retry_reuses_proved_numeric_splits(monkeypatch, parent):
    context = SimpleNamespace(retry_of_run_id=parent)
    leaves = {"7102", "71030", "88000-1234"}
    monkeypatch.setattr(postal_checkpoint, "isolated_context", lambda *_: context)
    monkeypatch.setattr(postal_checkpoint, "completed_partitions", AsyncMock(return_value=(leaves, 7, 64)))
    monkeypatch.setattr(postal_checkpoint, "unreadable_partitions", AsyncMock(return_value=(set(), 0, 0)))
    starts = [_role_url(prefix) for prefix in ("710", "88000", "9", "Q")]

    queue, state = await importer._prepare_partition_fetch(
        SOURCE, "PractitionerRole", ProviderDirectoryPractitionerRole, starts, "run"
    )

    urls = [queue.get_nowait() for _ in range(queue.qsize())]
    assert state.resumed_partition_pages == 7 and state.resumed_partition_rows == 64
    assert state.completed_partition_identities == leaves
    if parent:
        expect = ["71031", "88000"] + [f"7103{digit}" for digit in range(2, 10)]
        expect += ["7100", "7101", "7104", "7105", "7106", "7107", "7108", "7109", "9", "Q"]
        assert urls == [_role_url(prefix) for prefix in expect]
    else:
        assert urls == starts


def test_uhc_affiliation_uses_postal_partitions():
    affiliation_urls = importer._partitioned_resource_start_urls(SOURCE, "OrganizationAffiliation", page_count=100)
    affiliation_children = importer._uhc_adaptive_partition_child_urls(
        SOURCE,
        "OrganizationAffiliation",
        f"{BASE}/OrganizationAffiliation?_count=100&{ROLE_PARAM}=82001",
        _bundle(10000),
    )

    assert len(affiliation_urls) == len(importer.UHC_POSTAL_ROOT_ALPHABET)
    assert _query(affiliation_urls[0]) == {"_count": "100", ROLE_PARAM: "0"}
    assert _query(affiliation_children[0])[ROLE_PARAM] == "82001-0"
    assert importer._uhc_partition_checkpoint_type(SOURCE, "OrganizationAffiliation") is not None


@pytest.mark.asyncio
async def test_uhc_location_scan_includes_national_zip_codes(monkeypatch):
    monkeypatch.setattr(uhc_scan, "location_zip_codes", lambda: ("00501", "00926", "11372"))
    zip_lookup = AsyncMock(return_value=[("00501",), ("11372",)])
    partitioned_fetch = AsyncMock(return_value=None)
    monkeypatch.setattr(importer.db, "all", zip_lookup)
    monkeypatch.setattr(importer, "_fetch_partitioned_resource_rows", partitioned_fetch)

    await importer._fetch_resource_rows(
        SOURCE, "Location", per_resource_limit=0, page_limit=0, page_count=100, timeout=3, run_id=None
    )

    zip_lookup.assert_awaited_once()
    assert '"geo_zip_lookup"' in zip_lookup.await_args.args[0]
    assert [_query(url) for url in partitioned_fetch.await_args.args[3]] == [
        {"_count": "100", "address-postalcode": "00501"},
        {"_count": "100", "address-postalcode": "00926"},
        {"_count": "100", "address-postalcode": "11372"},
    ]


@pytest.mark.asyncio
async def test_uhc_location_scan_fails_if_reference_zip_inventory_is_empty(monkeypatch):
    monkeypatch.setattr(importer.db, "all", AsyncMock(return_value=[]))

    with pytest.raises(RuntimeError, match="provider_directory_uhc_location_zip_inventory_empty"):
        await uhc_scan.location_start_urls(importer, SOURCE, 100)


def test_optum_zip_plan_includes_legacy_and_territories():
    codes = uhc_scan.location_zip_codes()
    assert len(codes) == 41527
    assert {"31084", "00926", "96913", "96799", "99747"} <= set(codes)


def test_optum_checkpoint_scope_binds_location_plan(monkeypatch):
    target_by_field = {
        **SOURCE,
        importer.RESOURCE_ENDPOINT_FIELDS["Location"]: f"{BASE}/Location?address-postalcode=60654",
    }
    monkeypatch.setattr(uhc_scan, "_location_zip_digest", lambda: "first")
    first = importer._pagination_checkpoint_scope_identity(SOURCE, [SOURCE["source_id"]])
    targeted = importer._pagination_checkpoint_scope_identity(target_by_field, [SOURCE["source_id"]])
    monkeypatch.setattr(uhc_scan, "_location_zip_digest", lambda: "second")
    assert importer._pagination_checkpoint_scope_identity(SOURCE, [SOURCE["source_id"]]) != first
    assert importer._pagination_checkpoint_scope_identity(target_by_field, [SOURCE["source_id"]]) == targeted


@pytest.mark.asyncio
async def test_optum_zip_plan_replay_and_targeted_filter(monkeypatch):
    monkeypatch.setattr(uhc_scan, "location_zip_codes", lambda: ("00926", "11372"))
    lookup = AsyncMock(return_value=[("99999",)])
    monkeypatch.setattr(importer.db, "all", lookup)
    with pytest.raises(RuntimeError, match="uhc_location_zip_inventory_changed"):
        await uhc_scan.location_start_urls(importer, SOURCE, 100)
    lookup.reset_mock()
    retry_by_field = {
        **SOURCE,
        "_pagination_checkpoint_context": SimpleNamespace(retry_of_run_id="old", lineage_verified=True),
    }
    assert [
        _query(url)["address-postalcode"] for url in await uhc_scan.location_start_urls(importer, retry_by_field, 100)
    ] == [
        "00926",
        "11372",
    ]
    target_by_field = {
        **SOURCE,
        importer.RESOURCE_ENDPOINT_FIELDS["Location"]: f"{BASE}/Location?address-postalcode=60654",
    }
    assert [
        _query(url)["address-postalcode"] for url in await uhc_scan.location_start_urls(importer, target_by_field, 100)
    ] == ["60654"]
    retry_by_field["_pagination_checkpoint_context"].lineage_verified = False
    with pytest.raises(RuntimeError, match="uhc_location_zip_retry_unverified"):
        await uhc_scan.location_start_urls(importer, retry_by_field, 100)
    assert uhc_scan.location_plan_identity(importer, target_by_field) is None
    lookup.assert_not_awaited()


@pytest.mark.asyncio
async def test_uhc_location_zip_starts_resume_completed_older_prefixes(monkeypatch):
    monkeypatch.setattr(importer, "_load_partition_checkpoints", AsyncMock(return_value={"address-postalcode=113"}))
    start_urls = [f"{BASE}/Location?_count=100&address-postalcode={zip_code}" for zip_code in ("11372", "11411")]

    queue, _state = await importer._prepare_partition_fetch(
        SOURCE, "Location", ProviderDirectoryLocation, start_urls, None
    )

    assert queue.get_nowait() == start_urls[1]
    assert queue.empty()


def _fake_role_server(location_totals: dict[str, int], fail_once: set[str] | None = None, fail_always=()):
    """Serve a capped 77030 role prefix whose ZIP+4 children each hold one role."""
    calls: list[str] = []
    remaining_failures = set(fail_once or ())

    async def fetch(_source, url, *, timeout):
        calls.append(url)
        query = _query(url)
        if "/Location" in url:
            return 200, _bundle(location_totals[query["address-postalcode"]], "Location"), None, 1
        prefix = query[ROLE_PARAM]
        if prefix in fail_always:
            return 500, {}, None, 1
        if prefix in remaining_failures:
            remaining_failures.discard(prefix)
            return 500, {}, None, 1
        if prefix == "77030":
            return 200, _bundle(10000), None, 1
        return 200, _bundle(1, resource_id=f"role-{prefix}"), None, 1

    return fetch, calls


async def _scan_roles(monkeypatch, fetch, start_prefixes=("77030",)):
    async def no_checkpoints(*_args, **_kwargs):
        return set()

    async def no_clear(*_args, **_kwargs):
        return None

    monkeypatch.setattr(importer, "_fetch_source_json", fetch)
    monkeypatch.setattr(importer, "_load_partition_checkpoints", no_checkpoints)
    monkeypatch.setattr(importer, "_clear_reverse_lookup_checkpoints", no_clear)
    return await importer._fetch_partitioned_resource_rows(
        SOURCE,
        "PractitionerRole",
        ProviderDirectoryPractitionerRole,
        [_role_url(prefix) for prefix in start_prefixes],
        _options(),
    )


@pytest.mark.asyncio
async def test_uhc_clean_zip4_scan_completes(monkeypatch):
    fetch, calls = _fake_role_server({"77030": 1290, "77030-": 1290})

    result = await _scan_roles(monkeypatch, fetch)

    assert result.complete is True
    assert result.error is None
    assert result.rows_fetched == 10
    assert any("_summary=count" in url for url in calls)


@pytest.mark.parametrize("location_totals", ({"77030": 1291, "77030-": 1290}, {"77030": 10000, "77030-": 10000}))
@pytest.mark.asyncio
async def test_uhc_bare_zip_mismatch_leaves_scan_incomplete(monkeypatch, location_totals):
    fetch, _calls = _fake_role_server(location_totals)

    result = await _scan_roles(monkeypatch, fetch)

    assert result.complete is False
    assert result.error.endswith(uhc_scan.BARE_ZIP_ERROR)
    assert result.rows_fetched == 10


@pytest.mark.asyncio
async def test_uhc_transient_leaf_failure_is_retried_in_run(monkeypatch):
    fetch, calls = _fake_role_server({"77030": 5, "77030-": 5}, fail_once={"77030-4"})

    result = await _scan_roles(monkeypatch, fetch)

    assert result.complete is True
    assert result.rows_fetched == 10
    assert sum(_query(url).get(ROLE_PARAM) == "77030-4" for url in calls) == 2


@pytest.mark.asyncio
async def test_uhc_persistent_leaf_failure_leaves_scan_incomplete(monkeypatch):
    fetch, _calls = _fake_role_server({"77030": 5, "77030-": 5}, fail_always={"77030-4"})

    result = await _scan_roles(monkeypatch, fetch)

    assert result.complete is False
    assert result.error == "partition_errors_1_last_http_500"


@pytest.mark.asyncio
async def test_uhc_split_children_run_concurrently(monkeypatch):
    in_flight_by_name = {"now": 0, "peak": 0}
    fetch, _calls = _fake_role_server({"77030": 5, "77030-": 5})

    async def slow_fetch(source, url, *, timeout):
        is_child = _query(url).get(ROLE_PARAM, "").startswith("77030-")
        in_flight_by_name["now"] += is_child
        in_flight_by_name["peak"] = max(in_flight_by_name["peak"], in_flight_by_name["now"])
        await asyncio.sleep(0.01)
        in_flight_by_name["now"] -= is_child
        return await fetch(source, url, timeout=timeout)

    monkeypatch.setenv("HLTHPRT_PROVIDER_DIRECTORY_PARTITION_CONCURRENCY", "8")
    result = await _scan_roles(monkeypatch, slow_fetch, start_prefixes=("77030", "1", "2", "3"))

    assert result.complete is True
    assert in_flight_by_name["peak"] >= 4


@pytest.mark.asyncio
async def test_uhc_practitioner_name_cap_is_best_effort_but_postal_cap_fails(monkeypatch):
    async def fetch(_source, url, *, timeout):
        resource_type = urllib.parse.urlsplit(url).path.rsplit("/", 1)[-1]
        payload = _bundle(10000, resource_type, "row-1")
        if resource_type == "Location":
            payload["entry"][0]["resource"]["address"] = {"postalCode": "77030-1234"}
        return 200, payload, None, 1

    async def no_checkpoints(*_args, **_kwargs):
        return set()

    monkeypatch.setattr(importer, "_fetch_source_json", fetch)
    monkeypatch.setattr(importer, "_load_partition_checkpoints", no_checkpoints)
    monkeypatch.setattr(importer, "_clear_reverse_lookup_checkpoints", no_checkpoints)
    practitioner = await importer._fetch_partitioned_resource_rows(
        SOURCE,
        "Practitioner",
        ProviderDirectoryPractitioner,
        [f"{BASE}/Practitioner?_count=100&family=ABCD"],
        _options(),
    )
    location = await importer._fetch_partitioned_resource_rows(
        SOURCE,
        "Location",
        ProviderDirectoryLocation,
        [f"{BASE}/Location?_count=100&address-postalcode=77030-1234"],
        _options(),
    )

    assert practitioner.complete is True
    assert practitioner.error is None
    assert location.complete is False
    assert location.error == "partition_errors_1_last_uhc_location_partition_cap_exhausted"


@pytest.mark.asyncio
async def test_uhc_resume_keeps_remaining_partition_queue(monkeypatch):
    async def completed(*_args, **_kwargs):
        return {"0", "1"}

    monkeypatch.setattr(importer, "_load_partition_checkpoints", completed)
    start_urls = importer._partitioned_resource_start_urls(SOURCE, "PractitionerRole", page_count=100)

    queue, state = await importer._prepare_partition_fetch(
        SOURCE, "PractitionerRole", ProviderDirectoryPractitionerRole, start_urls, "run_retry"
    )

    assert state.completed_partition_identities == {"0", "1"}
    assert queue.qsize() == len(start_urls) - 2


def _postal_scan_spies(monkeypatch):
    """Check each persisted leaf follows its payload write and stays inside its root."""
    checkpoint = importer.postal_checkpoint
    active_root = ContextVar("active_postal_root")
    binding = NamedFHIRSourceStorage("mrf", "candidate_schema")
    counts_by_leaf = {}
    requested_postcodes = []
    saved_payloads = set()

    @asynccontextmanager
    async def transaction(_database, *, schema, dataset_ids, root_run_ids):
        assert schema == "mrf" and dataset_ids == ("synthetic-dataset",)
        token = active_root.set(root_run_ids[0])
        try:
            yield
        finally:
            active_root.reset(token)

    async def read(statement, *, dataset_id, root, checkpoint_type):
        assert '"candidate_schema"."provider_directory_postal_partition_checkpoint"' in statement
        assert dataset_id == "synthetic-dataset" and root == active_root.get()
        return [
            (identity, *counts)
            for (owner, kind, identity), counts in counts_by_leaf.items()
            if (owner, kind) == (root, checkpoint_type)
        ]

    async def save(statement, *, dataset_id, root, checkpoints):
        assert '"candidate_schema"."provider_directory_postal_partition_checkpoint"' in statement
        assert dataset_id == "synthetic-dataset" and root == active_root.get()
        for leaf in json.loads(checkpoints):
            assert leaf["identity"] in saved_payloads
            counts_by_leaf[root, leaf["kind"], leaf["identity"]] = leaf["page_count"], leaf["row_count"]

    async def fetch(_source, url, *, timeout):
        postcode = _query(url)[ROLE_PARAM]
        requested_postcodes.append(postcode)
        return 200, _bundle(1, resource_id=postcode), None, 1

    async def persist(_model, rows):
        saved_payloads.update(row["resource_id"] for row in rows)
        return len(rows)

    monkeypatch.setattr(checkpoint, "source_checkpoint_transaction", transaction)
    monkeypatch.setattr(checkpoint.source, "bound_publisher", lambda: SimpleNamespace(storage_binding=binding))
    monkeypatch.setattr(importer, "db", SimpleNamespace(all=read, status=save))
    monkeypatch.setattr(importer, "_fetch_source_json", fetch)
    factory = SimpleNamespace(storage_binding=binding, transaction=transaction)
    return factory, requested_postcodes, persist


@pytest.mark.asyncio
async def test_isolated_postal_scan_resumes_completed_leaves(monkeypatch):
    """The same root skips stored leaves while a distinct root reads them anew."""
    factory, requested_postcodes, persist = _postal_scan_spies(monkeypatch)
    origin = "run_" + "a" * 32
    successor = "run_" + "b" * 32
    other = "run_" + "c" * 32
    context = importer.PaginationCheckpointContext(
        BASE, "1" * 64, (SOURCE["source_id"],), origin, origin, dataset_id="synthetic-dataset"
    )
    complete, remaining = "77030-1234", "77030-1235"
    options = _options(retain_rows=False, row_batch_handler=persist)

    async def scan(scan_context, *prefixes):
        source_by_field = {**SOURCE, "_pagination_checkpoint_context": scan_context}
        return await importer._fetch_partitioned_resource_rows(
            source_by_field,
            "PractitionerRole",
            ProviderDirectoryPractitionerRole,
            [_role_url(prefix) for prefix in prefixes],
            replace(options, run_id=scan_context.owner_run_id),
        )

    async with importer.postal_checkpoint.source.bind_generic_source_transaction_factory(factory):
        assert (await scan(context, complete)).complete
        retry = replace(context, owner_run_id=successor, retry_of_run_id=origin, lineage_verified=True)
        resumed = await scan(retry, complete, remaining)
        assert resumed.complete and resumed.pages_fetched == resumed.rows_fetched == 2
        assert resumed.rows_written == 1
        assert requested_postcodes == [complete, remaining]
        assert (await scan(replace(context, owner_run_id=other, acquisition_root_run_id=other), complete)).complete
    assert requested_postcodes == [complete, remaining, complete]


def test_postal_handoff_requires_unstarted_resources():
    identity_by_field = {
        "failed_run_id": "run_synthetic",
        "dataset_id": "dataset_synthetic",
        "acquisition_root_run_id": "run_synthetic",
    }
    checkpoint_by_field = {
        "owner_run_id": identity_by_field["failed_run_id"],
        "dataset_id": identity_by_field["dataset_id"],
        "acquisition_root_run_id": identity_by_field["acquisition_root_run_id"],
        "canonical_api_base": BASE,
        "source_ids": [SOURCE["source_id"]],
        "resource_type": "InsurancePlan",
        "state": "complete",
        "next_url": None,
        "source_scope_hash": "1" * 64,
        "completed_at": "2026-01-01",
        "rows_processed": 2,
        "pages_processed": 2,
    }
    counts = dict.fromkeys(importer.UHC_SUPPORTED_RESOURCES, 0) | {"InsurancePlan": 2}
    _require_postal_start_checkpoint(
        [checkpoint_by_field], identity_by_field, counts, "failed", checkpoint_by_field["source_ids"]
    )
    with pytest.raises(ValueError, match="transition_checkpoint_changed"):
        _require_postal_start_checkpoint(
            [checkpoint_by_field],
            identity_by_field,
            counts | {"Location": 1},
            "failed",
            checkpoint_by_field["source_ids"],
        )


def _stored_rows_factory(rows_by_type: dict[str, list[dict[str, Any]]]):
    async def stored_rows(_directory, dataset_id, resource_type):
        assert dataset_id == "dataset_synthetic"
        for resource_row in rows_by_type.get(resource_type, []):
            yield resource_row

    return stored_rows


def _complete_diagnostics(*resource_types: str) -> dict[str, dict[str, Any]]:
    return {resource_type: {"complete": True, "error": None, "bounded": False} for resource_type in resource_types}


CLOSURE_STORED_ROWS_BY_TYPE = {
    "Practitioner": [{"resource_id": "prac-1"}],
    "Organization": [],
    "Location": [{"resource_id": "loc-1"}],
    "PractitionerRole": [
        {
            "resource_id": "role-1",
            "practitioner_ref": "Practitioner/prac-2",
            "location_refs": ["Location/loc-1", "Location/loc-gone"],
            "network_refs": ["Organization/net-1"],
        }
    ],
    "InsurancePlan": [{"resource_id": "plan-1", "network_refs": ["Organization/net-1"]}],
}
CLOSURE_SERVED_BY_PATH = {
    "Practitioner/prac-2": {"resourceType": "Practitioner", "id": "prac-2"},
    "Organization/net-1": {"resourceType": "Organization", "id": "net-1", "name": "Synthetic Network"},
}


@pytest.mark.asyncio
async def test_uhc_reference_closure_reads_missing_ids_and_counts_404(monkeypatch):
    """Unacquired references are read by id once; a 404 is counted missing, not failed."""
    requested_paths: list[str] = []

    async def fetch(_source, url, *, timeout):
        relative = url.removeprefix(f"{BASE}/")
        requested_paths.append(relative)
        if relative in CLOSURE_SERVED_BY_PATH:
            return 200, CLOSURE_SERVED_BY_PATH[relative], None, 1
        return 404, {"resourceType": "OperationOutcome"}, None, 1

    persisted_ids_by_model: dict[type, list[str]] = {}

    async def persist(model, resource_rows):
        persisted_ids_by_model.setdefault(model, []).extend(row["resource_id"] for row in resource_rows)
        return len(resource_rows)

    monkeypatch.setattr(uhc_scan, "_stored_rows", _stored_rows_factory(CLOSURE_STORED_ROWS_BY_TYPE))
    monkeypatch.setattr(importer, "_fetch_source_json", fetch)
    monkeypatch.setattr(importer, "is_source_storage_required", lambda: True)
    resolve = Mock(wraps=importer._reference_resource_key)
    monkeypatch.setattr(importer, "_reference_resource_key", resolve)
    diagnostics_by_type = _complete_diagnostics(
        "InsurancePlan", "Practitioner", "Organization", "Location", "PractitionerRole"
    )
    count_by_type: dict[str, int] = {}

    await uhc_scan.close_references(
        importer,
        {**SOURCE, "_endpoint_dataset_id": "dataset_synthetic"},
        persist,
        diagnostics_by_type,
        count_by_type,
        timeout=3,
        run_id="run_1",
    )

    assert sorted(requested_paths) == ["Location/loc-gone", "Organization/net-1", "Practitioner/prac-2"]
    assert [call.args for call in resolve.call_args_list].count(("Organization/net-1", "Organization")) == 1
    assert persisted_ids_by_model == {
        importer.ProviderDirectoryPractitioner: ["prac-2"],
        importer.ProviderDirectoryOrganization: ["net-1"],
    }
    assert diagnostics_by_type["Location"]["reference_closure"] == {"fetched": 0, "missing": 1, "failed": 0}
    assert diagnostics_by_type["Organization"]["reference_closure"]["fetched"] == 1
    assert all(not diagnostic["error"] for diagnostic in diagnostics_by_type.values())
    assert count_by_type == {"Practitioner": 1, "Organization": 1, "Location": 0}


@pytest.mark.asyncio
async def test_uhc_reference_closure_failure_marks_target_incomplete(monkeypatch):
    stored_rows_by_type = {
        "OrganizationAffiliation": [{"resource_id": "aff-1", "participating_organization_ref": "Organization/o-9"}]
    }

    async def fetch(_source, _url, *, timeout):
        return 500, {}, None, 1

    async def persist(_model, resource_rows):
        return len(resource_rows)

    monkeypatch.setattr(uhc_scan, "_stored_rows", _stored_rows_factory(stored_rows_by_type))
    monkeypatch.setattr(importer, "_fetch_source_json", fetch)
    monkeypatch.setattr(importer, "is_source_storage_required", lambda: True)
    diagnostics_by_type = _complete_diagnostics("Organization", "OrganizationAffiliation")

    await uhc_scan.close_references(
        importer,
        {**SOURCE, "_endpoint_dataset_id": "dataset_synthetic"},
        persist,
        diagnostics_by_type,
        {},
        timeout=3,
        run_id=None,
    )

    assert diagnostics_by_type["Organization"]["error"] == uhc_scan.REFERENCE_CLOSURE_ERROR
    assert diagnostics_by_type["Organization"]["reference_closure"]["failed"] == 1
    assert not diagnostics_by_type["OrganizationAffiliation"]["error"]


@pytest.mark.asyncio
async def test_uhc_reference_closure_waits_for_complete_scans(monkeypatch):
    async def must_not_read(*_args, **_kwargs):
        raise AssertionError("closure ran over an incomplete scan")
        yield  # pragma: no cover

    monkeypatch.setattr(uhc_scan, "_stored_rows", must_not_read)
    diagnostics_by_type = {"Practitioner": {"complete": False, "error": "partition_errors_1_last_http_500"}}

    await uhc_scan.close_references(importer, SOURCE, None, diagnostics_by_type, {}, timeout=3, run_id=None)

    assert "reference_closure" not in diagnostics_by_type["Practitioner"]


@pytest.mark.asyncio
@pytest.mark.parametrize("page_count", [1, 2])
async def test_uhc_reference_closure_preserves_unreadable_scan_gaps(monkeypatch, page_count):
    rows_by_type = {
        "OrganizationAffiliation": [{"resource_id": "aff-1", "participating_organization_ref": "Organization/o-9"}]
    }
    fetch_organization = AsyncMock(
        return_value=(200, {"resourceType": "Organization", "id": "o-9", "name": "Example"}, None, 1)
    )
    persist = AsyncMock(return_value=1)
    monkeypatch.setattr(uhc_scan, "_stored_rows", _stored_rows_factory(rows_by_type))
    monkeypatch.setattr(importer, "_fetch_source_json", fetch_organization)
    monkeypatch.setattr(importer, "is_source_storage_required", lambda: True)
    diagnostics = _complete_diagnostics("Organization", "OrganizationAffiliation", "Location")
    gap_error = "partition_errors_1_last_optum_unreadable_search_slots"
    diagnostics["Location"].update(
        complete=False,
        error=gap_error,
        bounded=False,
        next_url_remaining=False,
        source_fetch={"unreadable_optum_slot_count": 2, "unreadable_optum_page_count": page_count},
    )

    await uhc_scan.close_references(
        importer,
        {**SOURCE, "_endpoint_dataset_id": "dataset_synthetic"},
        persist,
        diagnostics,
        {},
        timeout=3,
        run_id=None,
    )

    if page_count == 1:
        fetch_organization.assert_awaited_once()
        persist.assert_awaited_once()
        assert diagnostics["Organization"]["reference_closure"]["fetched"] == 1
    else:
        fetch_organization.assert_not_awaited()
        persist.assert_not_awaited()
    assert diagnostics["Location"]["error"] == gap_error
    assert not importer._is_resource_diagnostic_acquisition_complete(diagnostics["Location"])


@pytest.mark.asyncio
async def test_initial_location_prefill_times_out_before_blocking_the_zip_scan(monkeypatch):
    from process import provider_directory_generic_source_adapters as adapters

    cancelled = asyncio.Event()

    async def waiting_for_saved_links(*_args):
        try:
            await asyncio.Future()
        finally:
            cancelled.set()

    monkeypatch.setattr(adapters, "missing_saved_role_links", waiting_for_saved_links)
    monkeypatch.setattr(uhc_scan, "REFERENCE_PREFILL_DISCOVERY_SECONDS", 0.01)
    persist_batch = AsyncMock()
    writes = await uhc_scan.hydrate_saved_role_links(
        importer,
        {**SOURCE, "_endpoint_dataset_id": "isolated-candidate"},
        "Location",
        persist_batch,
        asyncio.Event(),
        timeout=3,
        run_id="synthetic-run",
        max_batches=1,
    )

    assert writes == 0 and cancelled.is_set()
    persist_batch.assert_not_awaited()


def test_uhc_route_uses_partitions_not_plan_graph(monkeypatch):
    resources = list(importer.UHC_SUPPORTED_RESOURCES)

    assert not importer._should_use_uhc_plan_graph(SOURCE, resources, per_resource_limit=0, page_limit=0)
    monkeypatch.setenv("HLTHPRT_PROVIDER_DIRECTORY_UHC_ROLE_POSTAL_PARTITIONS", "0")
    assert importer._should_use_uhc_plan_graph(SOURCE, resources, per_resource_limit=0, page_limit=0)


@pytest.mark.asyncio
async def test_uhc_route_accepts_deferred_materialization(monkeypatch):
    """The deferred guard passes; the run then fails later on the stubbed group import, not on the guard."""

    def stop_after_guard(*_args, **_kwargs):
        raise RuntimeError("stopped_after_guard")

    monkeypatch.setattr(importer, "_validate_provider_directory_endpoint_scope", stop_after_guard)
    with pytest.raises(RuntimeError, match="stopped_after_guard"):
        await importer._import_resources(
            [dict(SOURCE)],
            resources=list(importer.UHC_SUPPORTED_RESOURCES),
            per_resource_limit=0,
            page_limit=0,
            page_count=100,
            timeout=3,
            run_id="run_1",
            is_pagination_checkpointing_enabled=True,
            defer_typed_materialization=True,
        )


def test_uhc_request_concurrency_follows_partition_setting(monkeypatch):
    monkeypatch.setenv("HLTHPRT_PROVIDER_DIRECTORY_PARTITION_CONCURRENCY", "32")
    other_source_dict = {"source_id": "other_source", "api_base": "https://example.test/fhir"}

    assert uhc_scan.request_concurrency(importer, [SOURCE]) == 32
    assert uhc_scan.request_concurrency(importer, [SOURCE, other_source_dict]) == 1
    assert uhc_scan.request_concurrency(importer, [other_source_dict]) == 1
    assert source_selection.requires_verified_session(importer, SOURCE)


async def _gzip_server(seen_headers: list[str]):
    async def handle(request):
        seen_headers.append(request.headers.get("Accept-Encoding", ""))
        body = gzip.compress(json.dumps(_bundle(3, "Location", "loc-1")).encode())
        return web.Response(body=body, headers={"Content-Encoding": "gzip", "Content-Type": "application/fhir+json"})

    app = web.Application()
    app.router.add_get("/{tail:.*}", handle)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]
    return runner, f"http://127.0.0.1:{port}/fhirpublic/R4/Location?address-postalcode=7"


@pytest.mark.asyncio
async def test_uhc_direct_fetch_requests_and_decodes_gzip(monkeypatch):
    seen_headers: list[str] = []
    runner, url = await _gzip_server(seen_headers)
    monkeypatch.setattr(importer.source_proxy, "selected_run", lambda *_args: None)
    try:
        async with importer._source_http_session_scope(force=True, request_concurrency=32):
            status, payload, error, _elapsed = await source_selection.fetch_unpaced_source_json(
                importer, SOURCE, url, timeout=5
            )
            session = importer._SOURCE_HTTP_SESSION.get()
            assert session.connector.limit_per_host == 32
    finally:
        await runner.cleanup()

    assert (status, error) == (200, None)
    assert payload["total"] == 3
    assert seen_headers == ["gzip"]


class _FakeBinding:
    def __init__(self):
        self.cursor_keys: list[str] = []

    def select(self, _url, cursor_key, exclude_ids=frozenset()):
        self.cursor_keys.append(cursor_key)
        return type("Selection", (), {"proxy_id": "proxy-a", "request_options": {}})()


@pytest.mark.asyncio
async def test_uhc_proxy_fetch_keeps_partition_cursor_and_gzip(monkeypatch):
    proxy_run = object.__new__(source_proxy.SourceProxyRun)
    proxy_run.directory, proxy_run.bindings, proxy_run._last_quota = importer, {}, {}
    proxy_run.credentials, proxy_run._audit = {SOURCE["source_id"]: None}, {}
    binding = _FakeBinding()
    proxy_run.bindings[SOURCE["source_id"]] = binding
    sent_requests: list[dict[str, Any]] = []

    async def fake_session_fetch(_session, url, **options):
        sent_requests.append({"url": url, **options})
        return 200, _bundle(1, "PractitionerRole", "role-1"), None, 1

    monkeypatch.setattr(importer, "_fetch_json_with_source_session", fake_session_fetch)
    monkeypatch.setattr(importer.source_proxy, "selected_run", lambda *_args: proxy_run)
    page_urls = (_role_url("77030-1"), f"{BASE}?_getpages=synthetic&_getpagesoffset=100", f"{BASE}/Location/loc-1")
    async with importer._source_http_session_scope(force=True):
        for page_url in page_urls[:2]:
            token = uhc_scan.enter_proxy_cursor(importer, SOURCE, "PractitionerRole", page_urls[0])
            try:
                await source_selection.fetch_unpaced_source_json(importer, SOURCE, page_url, timeout=5)
            finally:
                source_proxy.PARTITION_CURSOR_KEY.reset(token)
        await source_selection.fetch_unpaced_source_json(importer, SOURCE, page_urls[2], timeout=5)

    partition_key = "PractitionerRole:77030-1"
    assert binding.cursor_keys == [partition_key, partition_key, partition_key, partition_key, "Location", "Location"]
    assert [request["url"] for request in sent_requests] == list(page_urls)
    assert all(request["allow_gzip"] is True and request["proxy_options"] == {} for request in sent_requests)


def test_uhc_override_metadata_is_runnable():
    metadata = importer._uhc_provider_directory_override_metadata({"api_base": BASE})

    assert metadata["provider_directory_acquisition_enabled"] is True
    assert metadata["provider_directory_fully_enumerable_resources"] == list(importer.UHC_SUPPORTED_RESOURCES)
    assert (
        importer._resource_acquisition_blocked_reason(
            {**SOURCE, "metadata": metadata}, list(importer.UHC_SUPPORTED_RESOURCES)
        )
        is None
    )


@pytest.mark.asyncio
async def test_uhc_failed_name_residual_page_does_not_requeue_split_parent(monkeypatch):
    """A capped name parent whose residual page fails keeps its children and does not re-walk."""
    calls: list[str] = []
    next_url = f"{BASE}?_getpages=synthetic&_getpagesoffset=100"

    async def fetch(_source, url, *, timeout):
        calls.append(url)
        if url == next_url:
            return 500, {}, None, 1
        family = _query(url)["family"]
        if family == "AB":
            capped_payload = _bundle(10000, "Practitioner", "prac-ab")
            capped_payload["link"] = [{"relation": "next", "url": next_url}]
            return 200, capped_payload, None, 1
        return 200, _bundle(1, "Practitioner", f"prac-{family}"), None, 1

    async def no_checkpoints(*_args, **_kwargs):
        return set()

    monkeypatch.setattr(importer, "_fetch_source_json", fetch)
    monkeypatch.setattr(importer, "_load_partition_checkpoints", no_checkpoints)
    monkeypatch.setattr(importer, "_clear_reverse_lookup_checkpoints", no_checkpoints)
    practitioner_result = await importer._fetch_partitioned_resource_rows(
        SOURCE, "Practitioner", ProviderDirectoryPractitioner, [f"{BASE}/Practitioner?_count=100&family=AB"], _options()
    )

    assert practitioner_result.error is None
    assert practitioner_result.complete is True
    assert calls.count(f"{BASE}/Practitioner?_count=100&family=AB") == 1
    assert len(calls) == 2 + len(importer.UHC_TEXT_SEARCH_ALPHABET)


@pytest.mark.asyncio
async def test_uhc_reference_closure_requires_isolated_storage(monkeypatch):
    monkeypatch.setattr(importer, "is_source_storage_required", lambda: False)
    diagnostics_by_type = _complete_diagnostics("Practitioner", "PractitionerRole")

    await uhc_scan.close_references(
        importer,
        {**SOURCE, "_endpoint_dataset_id": "dataset_synthetic"},
        None,
        diagnostics_by_type,
        {},
        timeout=3,
        run_id=None,
    )

    assert diagnostics_by_type["Practitioner"]["error"] == uhc_scan.REFERENCE_CLOSURE_ERROR


@pytest.mark.asyncio
async def test_uhc_postal_scan_states_location_without_postal_code_residual(monkeypatch):
    complete_fetch, _calls = _fake_role_server({"77030": 1290, "77030-": 1290})
    failed_fetch, _calls = _fake_role_server({"77030": 5, "77030-": 5}, fail_always={"77030-4"})

    complete_result = await _scan_roles(monkeypatch, complete_fetch)
    failed_result = await _scan_roles(monkeypatch, failed_fetch)
    diagnostic = importer._resource_fetch_diagnostic(complete_result, rows_written=complete_result.rows_written)

    assert (complete_result.complete, complete_result.error) == (True, None)
    assert diagnostic["source_fetch"] == {"unenumerable_residual": "location_without_postal_code"}
    assert failed_result.fetch_diagnostic is None


@pytest.mark.asyncio
async def test_uhc_split_guard_rereads_bare_total(monkeypatch):
    """A bare-ZIP total that changes between the guard's reads fails closed."""
    totals_by_prefix = {"77030": [1290, 1291], "77030-": [1290]}
    location_prefixes: list[str] = []
    role_fetch, _calls = _fake_role_server({})

    async def fetch(source, url, *, timeout):
        if "/Location" not in url:
            return await role_fetch(source, url, timeout=timeout)
        prefix = _query(url)["address-postalcode"]
        location_prefixes.append(prefix)
        return 200, _bundle(totals_by_prefix[prefix].pop(0), "Location"), None, 1

    result = await _scan_roles(monkeypatch, fetch)

    assert location_prefixes == ["77030", "77030-", "77030"]
    assert result.complete is False
    assert result.error.endswith(uhc_scan.BARE_ZIP_ERROR)


@pytest.mark.asyncio
async def test_retained_role_scans_guarded_zip4_children_before_broader_root(monkeypatch):
    monkeypatch.setenv("HLTHPRT_PROVIDER_DIRECTORY_PARTITION_CONCURRENCY", "1")
    context = SimpleNamespace(retry_of_run_id="previous-run")
    monkeypatch.setattr(postal_checkpoint, "isolated_context", lambda *_: context)
    monkeypatch.setattr(postal_checkpoint, "completed_partitions", AsyncMock(return_value=({"77031"}, 1, 1)))
    monkeypatch.setattr(postal_checkpoint, "unreadable_partitions", AsyncMock(return_value=(set(), 0, 0)))
    record_checkpoint = AsyncMock()
    monkeypatch.setattr(postal_checkpoint, "record_partition", record_checkpoint)
    delayed_child = _role_url("77030-4")
    fetch, calls = _fake_role_server({"77030": 7, "77030-": 7}, fail_once={"77030-4"})
    monkeypatch.setattr(importer, "_fetch_source_json", fetch)

    scan_result = await importer._fetch_partitioned_resource_rows(
        SOURCE,
        "PractitionerRole",
        ProviderDirectoryPractitionerRole,
        [_role_url("77030"), _role_url("8")],
        _options(run_id="retained-run"),
    )

    assert scan_result.complete and scan_result.error is None
    assert calls[0] == _role_url("77030")
    assert [_query(url)["address-postalcode"] for url in calls[1:4]] == ["77030", "77030-", "77030"]
    child_urls = [_role_url(f"77030-{digit}") for digit in range(10)]
    assert calls[4:] == child_urls + [_role_url("8"), delayed_child]
    assert [call.args[4] for call in record_checkpoint.await_args_list] == [
        *[url for url in child_urls if url != delayed_child],
        _role_url("8"),
        delayed_child,
    ]


@pytest.mark.asyncio
async def test_uhc_retried_multi_page_leaf_walks_every_page(monkeypatch):
    page_urls = [_role_url("77030-4"), *(f"{BASE}?_getpages=leaf&_getpagesoffset={offset}" for offset in (100, 200))]
    remaining_failures = {page_urls[2]}
    calls: list[str] = []

    async def fetch(_source, url, *, timeout):
        calls.append(url)
        if url in remaining_failures:
            remaining_failures.discard(url)
            return 500, {}, None, 1
        page_index = page_urls.index(url)
        payload = _bundle(3, resource_id=f"role-{page_index}")
        if page_index < 2:
            payload["link"] = [{"relation": "next", "url": page_urls[page_index + 1]}]
        return 200, payload, None, 1

    result = await _scan_roles(monkeypatch, fetch, start_prefixes=("77030-4",))

    assert (result.complete, result.error) == (True, None)
    assert calls == page_urls + page_urls


@pytest.mark.asyncio
async def test_uhc_retries_a_failed_first_page_after_a_gateway_burst(monkeypatch):
    monkeypatch.setattr(uhc_scan, "LEAF_RETRY_DELAYS_SECONDS", (5.0, 30.0))
    monkeypatch.setenv("HLTHPRT_PROVIDER_DIRECTORY_PARTITION_CONCURRENCY", "1")
    fetch = AsyncMock(
        side_effect=[(502, {}, None, 1), (502, {}, None, 1), (200, _bundle(1, resource_id="role"), None, 1)]
    )
    sleep = AsyncMock()
    monkeypatch.setattr(uhc_scan.asyncio, "sleep", sleep)

    scan_result = await _scan_roles(monkeypatch, fetch, start_prefixes=("77030-4",))

    assert scan_result.complete
    assert [call.args[1] for call in fetch.await_args_list] == [_role_url("77030-4")] * 3
    assert [call.args for call in sleep.await_args_list] == [(5.0,), (30.0,)]


@pytest.mark.asyncio
async def test_uhc_default_leaf_retry_schedule_survives_four_gateway_bursts(monkeypatch):
    default_delays = (5.0, 30.0, 120.0, 300.0)
    monkeypatch.setattr(uhc_scan, "LEAF_RETRY_DELAYS_SECONDS", default_delays)
    monkeypatch.setenv("HLTHPRT_PROVIDER_DIRECTORY_PARTITION_CONCURRENCY", "1")
    fetch = AsyncMock(
        side_effect=[
            (502, {}, None, 1),
            (502, {}, None, 1),
            (502, {}, None, 1),
            (502, {}, None, 1),
            (200, _bundle(1, resource_id="role"), None, 1),
        ]
    )
    sleep = AsyncMock()
    monkeypatch.setattr(uhc_scan.asyncio, "sleep", sleep)

    scan_result = await _scan_roles(monkeypatch, fetch, start_prefixes=("77030-4",))

    assert scan_result.complete
    assert [call.args for call in sleep.await_args_list] == [(delay,) for delay in default_delays]


def _gapped_and_flaky_fetch(gapped_prefix, flaky_prefix, flaky_failures, requested_urls):
    """Serve one leaf with a verified unreadable slot and one leaf with a transient 502."""
    from tests.test_provider_directory_optum_outage import PAGE_ERROR

    gapped_cursor = f"{BASE}?_getpages=gapped&_getpagesoffset=2&_count=2&_pagetype=opaque"

    async def fetch(_directory, request):
        url = request.current_url
        requested_urls.append(url)
        query = _query(url)
        if query.get("_getpages") == "gapped":
            start, count = int(query["_getpagesoffset"]), int(query["_count"])
            if start <= 3 < start + count:
                return PAGE_ERROR
            entries = [
                {"resource": {"resourceType": "PractitionerRole", "id": f"gap-{idx}"}}
                for idx in range(start, start + count)
            ]
            return 200, {"resourceType": "Bundle", "type": "searchset", "total": 4, "entry": entries}, None, 1
        prefix = query.get(ROLE_PARAM)
        if prefix == gapped_prefix:
            first_entries = [
                {"resource": {"resourceType": "PractitionerRole", "id": gapped_prefix}},
                {"resource": {"resourceType": "PractitionerRole", "id": f"{gapped_prefix}-1"}},
            ]
            return (
                200,
                {
                    "resourceType": "Bundle",
                    "type": "searchset",
                    "total": 4,
                    "entry": first_entries,
                    "link": [{"relation": "next", "url": gapped_cursor}],
                },
                None,
                1,
            )
        if prefix == flaky_prefix and flaky_failures:
            return 502, {}, None, 1
        return 200, _bundle(1, resource_id=flaky_prefix), None, 1

    return fetch


@pytest.mark.asyncio
async def test_isolated_postal_scan_checkpoints_and_restores_unreadable_slot_leaves(monkeypatch):
    """A walked leaf with verified unreadable slots is skipped on retry while preserving gap counts."""
    from process import provider_directory_optum_pages as optum_pages

    factory, _requested_postcodes, persist = _postal_scan_spies(monkeypatch)
    origin, successor = "run_" + "a" * 32, "run_" + "b" * 32
    context = importer.PaginationCheckpointContext(
        BASE, "1" * 64, (SOURCE["source_id"],), origin, origin, dataset_id="synthetic-dataset"
    )
    gapped_prefix, flaky_prefix = "77030-1234", "77030-1235"
    flaky_failures, requested_urls = ["http_502"], []
    monkeypatch.setattr(
        importer.fhir_response,
        "fetch_partition_response",
        _gapped_and_flaky_fetch(gapped_prefix, flaky_prefix, flaky_failures, requested_urls),
    )
    options = _options(retain_rows=False, row_batch_handler=persist)

    async def scan(scan_context):
        return await importer._fetch_partitioned_resource_rows(
            {**SOURCE, "_pagination_checkpoint_context": scan_context},
            "PractitionerRole",
            ProviderDirectoryPractitionerRole,
            [_role_url(gapped_prefix), _role_url(flaky_prefix)],
            replace(options, run_id=scan_context.owner_run_id),
        )

    async with importer.postal_checkpoint.source.bind_generic_source_transaction_factory(factory):
        first_result = await scan(context)
        assert not first_result.complete and first_result.error == "partition_errors_2_last_http_502"
        requested_urls.clear()
        flaky_failures.clear()
        retry_context = replace(context, owner_run_id=successor, retry_of_run_id=origin, lineage_verified=True)
        resumed_result = await scan(retry_context)

    assert requested_urls == [_role_url(flaky_prefix)]
    assert not resumed_result.complete
    assert resumed_result.error == "partition_errors_1_last_optum_unreadable_search_slots"
    assert resumed_result.fetch_diagnostic["unreadable_optum_page_count"] == 1
    assert resumed_result.fetch_diagnostic["unreadable_optum_slot_count"] == 1
    resumed_diagnostic = importer._resource_fetch_diagnostic(resumed_result, rows_written=resumed_result.rows_written)
    assert optum_pages.has_only_unreadable_slots(resumed_diagnostic)


@pytest.mark.asyncio
async def test_uhc_reference_reader_stops_workers_when_persist_fails(monkeypatch):
    persist_calls: list[int] = []
    release_other_reads = asyncio.Event()

    async def fetch(_source, url, *, timeout):
        resource_id = url.rsplit("/", 1)[-1]
        if resource_id != "prac-0":
            await release_other_reads.wait()
        return 200, {"resourceType": "Practitioner", "id": resource_id}, None, 1

    async def persist(_model, resource_rows):
        persist_calls.append(len(resource_rows))
        raise RuntimeError("persist_failed")

    monkeypatch.setattr(uhc_scan, "REFERENCE_FLUSH_ROWS", 1)
    monkeypatch.setenv("HLTHPRT_PROVIDER_DIRECTORY_PARTITION_CONCURRENCY", "4")
    monkeypatch.setattr(importer, "_fetch_source_json", fetch)
    reader = uhc_scan._ReferenceReader(importer, SOURCE, persist, timeout=3, run_id=None)

    with pytest.raises(RuntimeError, match="persist_failed"):
        await reader.read([("Practitioner", f"prac-{index}") for index in range(8)])
    release_other_reads.set()
    await asyncio.sleep(0.05)

    assert persist_calls == [1]


def _practitioner_fetch(fail_id=None):
    async def fetch(_source, url, *, timeout):
        resource_id = url.rsplit("/", 1)[-1]
        await asyncio.sleep(0)
        if resource_id == fail_id:
            raise ValueError("fetch_bug")
        return 200, {"resourceType": "Practitioner", "id": resource_id}, None, 1

    return fetch


@pytest.mark.asyncio
@pytest.mark.parametrize("resource_type", ("Practitioner", "Organization"))
@pytest.mark.parametrize("batch_ids", [("one", "two"), ("one",), ("unexpected",)])
async def test_uhc_reference_reader_accepts_only_matching_batch_results(monkeypatch, resource_type, batch_ids):
    calls = []

    async def fetch(_source, url, *, timeout):
        if ids := _query(url).get("_id"):
            calls.append(ids)
            bundle = _bundle(len(batch_ids))
            bundle["entry"] = [{"resource": {"resourceType": resource_type, "id": row_id}} for row_id in batch_ids]
            return 200, bundle, None, 1
        row_id = url.rsplit("/", 1)[-1]
        calls.append(row_id)
        return 200, {"resourceType": resource_type, "id": row_id}, None, 1

    monkeypatch.setattr(importer, "_fetch_source_json", fetch)
    persist = AsyncMock(side_effect=lambda _model, rows: len(rows))
    reader = uhc_scan._ReferenceReader(importer, SOURCE, persist, timeout=3, run_id=None, use_batches=True)
    keys = [(resource_type, "one"), (resource_type, "two")]

    assert await reader.read(keys) == []

    accepted = set(batch_ids) if "unexpected" not in batch_ids else set()
    assert calls == ["one,two", *(row_id for _, row_id in keys if row_id not in accepted)]
    persisted = persist.await_args.args[1]
    assert {row["resource_id"] for row in persisted} == {row_id for _, row_id in keys}


@pytest.mark.asyncio
async def test_uhc_reference_reader_persists_in_reading_task(monkeypatch):
    """Threshold and final flushes run in the task that owns the transaction binding, never in a worker."""
    from db import connection as db_connection

    flush_calls: list[tuple[asyncio.Task, int]] = []

    async def persist(_model, resource_rows):
        db_connection.db._transaction_binding()  # raises in any task but the binding owner
        flush_calls.append((asyncio.current_task(), len(resource_rows)))
        return len(resource_rows)

    monkeypatch.setattr(uhc_scan, "REFERENCE_FLUSH_ROWS", 2)
    monkeypatch.setenv("HLTHPRT_PROVIDER_DIRECTORY_PARTITION_CONCURRENCY", "3")
    monkeypatch.setattr(importer, "_fetch_source_json", _practitioner_fetch())
    binding = db_connection._TransactionBinding(id(db_connection.db), object(), asyncio.current_task())
    token = db_connection._TRANSACTION.set((binding,))
    try:
        with pytest.raises(RuntimeError, match="child asyncio task"):
            await asyncio.create_task(persist(None, []))
        reader = uhc_scan._ReferenceReader(importer, SOURCE, persist, timeout=3, run_id=None)
        assert await reader.read([("Practitioner", f"prac-{index}") for index in range(7)]) == []
    finally:
        db_connection._TRANSACTION.reset(token)

    assert {task for task, _rows in flush_calls} == {asyncio.current_task()}
    assert [rows for _task, rows in flush_calls] == [2, 2, 2, 1]
    assert reader.written_by_type["Practitioner"] == 7


@pytest.mark.asyncio
async def test_uhc_reference_reader_worker_error_stops_persist_loop(monkeypatch):
    persist = AsyncMock(side_effect=lambda _model, rows: len(rows))
    monkeypatch.setenv("HLTHPRT_PROVIDER_DIRECTORY_PARTITION_CONCURRENCY", "3")
    monkeypatch.setattr(importer, "_fetch_source_json", _practitioner_fetch(fail_id="prac-4"))
    reader = uhc_scan._ReferenceReader(importer, SOURCE, persist, timeout=3, run_id=None)

    with pytest.raises(ValueError, match="fetch_bug"):
        await asyncio.wait_for(reader.read([("Practitioner", f"prac-{index}") for index in range(8)]), 5)
    persist.assert_not_awaited()  # partial batches flush only after a successful read


@pytest.mark.asyncio
async def test_uhc_gzip_fetch_keeps_configured_credentials(monkeypatch):
    sent_requests: list[dict[str, Any]] = []

    async def fetch_with_options(url, **options):
        sent_requests.append({"url": url, **options})
        return 200, _bundle(1, "Location", "loc-1"), None, 1

    monkeypatch.setattr(importer.source_proxy, "selected_run", lambda *_args: None)
    monkeypatch.setattr(
        importer,
        "_credential_request_options_for_source",
        lambda *_args, **_kwargs: {"headers": {"X-Synthetic": "h"}, "query_params": {"key": "q"}, "descriptor": None},
    )
    monkeypatch.setattr(importer, "_fetch_json_with_options", fetch_with_options)
    url = f"{BASE}/Location?address-postalcode=7"

    status, _payload, error, _elapsed = await source_selection.fetch_unpaced_source_json(
        importer, SOURCE, url, timeout=5
    )

    assert (status, error) == (200, None)
    assert sent_requests == [
        {
            "url": url,
            "timeout": 5,
            "extra_headers": {"X-Synthetic": "h"},
            "query_params_by_name": {"key": "q"},
            "allow_gzip": True,
        }
    ]


@pytest.mark.asyncio
async def test_uhc_reference_closure_runs_for_unlimited_non_deferred_run(monkeypatch):
    closure_calls: list[dict[str, Any]] = []

    async def no_scan(_runner, _resources):
        return None

    async def record_closure(_directory, source_record, persist, diagnostics_by_type, _count_by_type, **_options):
        closure_calls.append({"source_id": source_record["source_id"], "persist": persist})

    monkeypatch.setattr(importer, "_update_source_resource_import_metadata", AsyncMock())
    monkeypatch.setattr(importer.ResourceGroupScanRunner, "scan", no_scan)
    monkeypatch.setattr(uhc_scan, "close_references", record_closure)

    await importer._import_resources(
        [dict(SOURCE)],
        resources=["Practitioner", "PractitionerRole"],
        per_resource_limit=0,
        page_limit=0,
        page_count=100,
        timeout=3,
        run_id="run_1",
        defer_typed_materialization=False,
    )

    assert len(closure_calls) == 1
    assert callable(closure_calls[0]["persist"])
