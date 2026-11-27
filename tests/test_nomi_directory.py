# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Nomi search retention, coverage and normalized source admission."""

import errno
import json
import os
from contextlib import nullcontext
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from importlib import import_module
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process import delta_dental_directory_worker as finder
from process import nomi_directory_capture as nomi
from process import nomi_directory_integration as adapter
from process import provider_directory_generic_source_adapters as source_adapters
from process import provider_directory_generic_source_policy as source_policy
from process import provider_directory_generic_source_storage as source_storage
from process import provider_directory_source_proxy as source_proxy
from process.ext.source_http_proxy import SourceProxyError, SourceProxyUnavailable
from process.nomi_directory_normalization import NETWORK_ID

directory = import_module("process.provider_directory_fhir")


def _plan(tmp_path):
    return nomi.SearchPlan(tmp_path, "TEST_GROUP", (("00001", 25),), 2, 1, 10)


def _row(kind, row_id):
    row_by_field = {
        "locationId": row_id,
        "name": "Example Directory Record",
        "facilityName": "Example Site",
        "taxonomy": "Example Specialty",
        "taxonomyId": 1,
        "specialtyId": 2,
        "isVirtualAddress": False,
        "addressLine1": "1 Example Street",
        "city": "Example",
        "state": "NY",
        "zipCode": "00001",
        "latitude": 40.0,
        "longitude": -75.0,
    }
    if kind == "providers":
        row_by_field.update(providerId=row_id, npiNumber="1234567893", providerType=[1, 3])
    return row_by_field


def _page(number, kind="providers", ids=(), total=None):
    result_by_field = {"pageNumber": number, "nbrOfRecords": len(ids), "startingLocation": {}}
    if ids:
        result_by_field.update(providers=[_row(kind, row_id) for row_id in ids], totalNumberOfRecords=total)
    return json.dumps({"status": 0, "result": [result_by_field]}).encode()


def _acquire(plan, root_run_id, *, require_retained=False):
    return nomi.acquire_directory(
        plan,
        root_run_id,
        SimpleNamespace(cooldown_snapshot=lambda: {}),
        cancel_check=AsyncMock(),
        progress=AsyncMock(),
        pause=AsyncMock(),
        require_retained=require_retained,
    )


def _mock_transport(monkeypatch, origin_page):
    route = SimpleNamespace(proxy_id="test-proxy")
    monkeypatch.setattr(nomi, "_selection", lambda *_args, **_kwargs: route)
    monkeypatch.setattr(nomi.FinderAdmission, "admit", lambda _self, reserve: _reserve(reserve))
    monkeypatch.setattr(nomi, "_origin_page", origin_page)


async def _reserve(reserve):
    reserve()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "interruption", [SourceProxyError("nomi_directory_request_failed"), SourceProxyUnavailable(None), TimeoutError()]
)
async def test_resume_reuses_only_same_edition_and_fresh_root_refetches(monkeypatch, tmp_path, interruption):
    plan = _plan(tmp_path)
    calls = []
    attempts = []

    async def request(_http, _binding, _route, _admission, url, payload):
        kind = url.rsplit("/", 1)[-1]
        page = payload["pageNbr"]
        calls.append((kind, page))
        attempts.append((kind, page))
        assert payload.get("providerType") == ("1,2,3" if kind == "providers" else None)
        if kind == "providers" and page == 2 and len(attempts) == 4:
            raise interruption
        if page == 1:
            return _page(page, kind, (101,), 1)
        return _page(page)

    _mock_transport(monkeypatch, request)
    with pytest.raises(type(interruption)):
        async with _acquire(plan, "first-root"):
            pytest.fail("interrupted capture became eligible to publish")
    assert calls == [("facilities", 1), ("facilities", 2), ("providers", 1), ("providers", 2)]

    calls.clear()
    async with _acquire(plan, "first-root", require_retained=True) as (capture, receipt):
        resources = list(capture.iter_resources())
        assert receipt["distinct_source_rows"] == {"facilities": 1, "providers": 1}
        assert receipt["origin_requests"] == 5
        assert receipt["resource_counts"] == {
            "Location": 1,
            "Organization": 2,
            "OrganizationAffiliation": 1,
            "Practitioner": 1,
            "PractitionerRole": 1,
        }
        assert all(
            resource["network"] == [{"reference": f"Organization/{NETWORK_ID}"}]
            for resource in resources
            if resource["resourceType"] in {"PractitionerRole", "OrganizationAffiliation"}
        )
        role = next(resource for resource in resources if resource["resourceType"] == "PractitionerRole")
        assert [resource["coding"][0]["code"] for resource in role["code"]] == ["1", "3"]
    assert calls == [("providers", 2)]

    calls.clear()
    async with _acquire(plan, "second-root") as (_capture, receipt):
        assert receipt["origin_requests"] == 4
    assert calls == [("facilities", 1), ("facilities", 2), ("providers", 1), ("providers", 2)]

    with pytest.raises(ValueError, match="retained_edition_required"):
        async with _acquire(replace(plan, page_size=3), "first-root", require_retained=True):
            pytest.fail("changed pagination reused a root")


@pytest.mark.asyncio
@pytest.mark.parametrize("missing", [("scope.json",), ("capture.sqlite3",), ("scope.json", "capture.sqlite3"), ()])
async def test_retry_refuses_missing_or_empty_artifacts_before_origin(monkeypatch, tmp_path, missing):
    plan = _plan(tmp_path)
    origin = AsyncMock(side_effect=[_page(1, "facilities", (101,), 1), TimeoutError()])
    _mock_transport(monkeypatch, origin)
    with pytest.raises(TimeoutError):
        async with _acquire(plan, "retained-root"):
            pytest.fail("incomplete capture became eligible for COPY")
    root = tmp_path / "retained-root"
    for artifact in missing:
        (root / artifact).unlink()
    if not missing:
        (root / "capture.sqlite3").write_bytes(b"")
    origin.reset_mock()
    with pytest.raises(ValueError, match="retained_edition_required"):
        async with _acquire(plan, "retained-root", require_retained=True):
            pytest.fail("missing capture was restarted")
    origin.assert_not_awaited()
    assert all(not (root / artifact).exists() for artifact in missing)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("second_page", "error"),
    [
        (_page(2), "pagination_truncated"),
        (_page(2, "facilities", (101,), 3), "pagination_repeated"),
        (_page(1, "facilities", (102,), 3), "returned_page_changed"),
        (b'{"status":1,"error":"invalid selector"}', "application_error"),
    ],
)
async def test_unresolved_pagination_never_yields_a_candidate(monkeypatch, tmp_path, second_page, error):
    origin = AsyncMock(side_effect=[_page(1, "facilities", (101,), 3), second_page])
    _mock_transport(monkeypatch, origin)
    with pytest.raises(ValueError, match=error):
        async with _acquire(_plan(tmp_path), "failing-root"):
            pytest.fail("invalid page admitted")
    assert origin.await_count == 2


@pytest.mark.asyncio
async def test_empty_windows_and_advisory_totals_preserve_observed_coverage(monkeypatch, tmp_path):
    plan = replace(_plan(tmp_path), searches=(("00001", 25), ("00002", 25)))

    async def request(_http, _binding, _route, _admission, url, payload):
        page = payload["pageNbr"]
        kind = url.rsplit("/", 1)[-1]
        if payload["zipCode"] == "00001" and page <= 2:
            return _page(page, kind, (100 + page,), 1)
        return _page(page)

    _mock_transport(monkeypatch, request)
    async with _acquire(plan, "example-root") as (_capture, receipt):
        assert receipt["distinct_source_rows"] == {"facilities": 2, "providers": 2}
        assert receipt["endpoints"]["providers"] == {
            "windows": 2,
            "pages": 4,
            "rows": 2,
            "distinct_window_rows": 2,
            "windows_with_total_drift": 1,
        }


def test_catalog_requires_configured_route(monkeypatch):
    from api import provider_directory_sources as catalog

    monkeypatch.setattr(catalog, "has_fresh_source_route", lambda *_: False)
    assert all(
        nomi.SOURCE_ID not in entry["source_ids"] for entry in catalog.provider_directory_source_catalog()["items"]
    )
    monkeypatch.setattr(catalog, "has_fresh_source_route", lambda source_id, _: source_id == nomi.SOURCE_ID)
    entry = next(
        entry for entry in catalog.provider_directory_source_catalog()["items"] if nomi.SOURCE_ID in entry["source_ids"]
    )
    assert entry["runnable"] is True
    assert entry["profile_enabled"] is False
    assert entry["resources"] == list(nomi.RESOURCE_TYPES)


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [403, 429])
async def test_publisher_hold_fences_retries_until_expiry(monkeypatch, tmp_path, status):
    origin_posts = []
    route = SimpleNamespace(proxy_id="test-proxy", request_options={})
    binding = SimpleNamespace(defer=lambda *_: None, cooldown_snapshot=lambda: {"cooldowns": []})
    pause = AsyncMock()
    plan = _plan(tmp_path)
    stopped_after = 1 if status == 403 else 2

    class HTTP:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_):
            return False

    class Exchange(HTTP):
        def __init__(self, response):
            self.response = response

        async def __aenter__(self):
            return self.response

    class Session(HTTP):
        def post(self, url, **kwargs):
            assert kwargs["allow_redirects"] is False
            kind = url.rsplit("/", 1)[-1]
            page = kwargs["json"]["pageNbr"]
            origin_posts.append((kind, page))
            return Exchange(
                SimpleNamespace(
                    status=status if len(origin_posts) == stopped_after else 200,
                    headers={"Retry-After": "600"},
                    raw=_page(page, kind, (101,), 1) if page == 1 else _page(page),
                )
            )

    monkeypatch.setattr(nomi.aiohttp, "ClientSession", lambda **_: Session())
    monkeypatch.setattr(nomi, "_selection", lambda *_args, **_kwargs: route)
    monkeypatch.setattr(nomi, "_json_body", AsyncMock(side_effect=lambda response, **_: response.raw))
    monkeypatch.setattr(nomi.FinderAdmission, "admit", lambda _self, reserve: _reserve(reserve))
    for edition in ("first-root", "later-root"):
        with pytest.raises(SourceProxyError):
            async with nomi.acquire_directory(
                plan, edition, binding, cancel_check=AsyncMock(), progress=AsyncMock(), pause=pause
            ):
                pytest.fail("held source admitted a candidate")
    assert len(origin_posts) == stopped_after
    assert pause.await_count == 2
    if status == 429:

        class AfterHold(datetime):
            @classmethod
            def now(cls, _tz):
                return datetime.now(UTC) + timedelta(seconds=601)

        monkeypatch.setattr(finder, "datetime", AfterHold)
        async with _acquire(plan, "first-root", require_retained=True) as (_capture, proof):
            assert proof["origin_requests"] == 5
        assert origin_posts[2:] == [("facilities", 2), ("providers", 1), ("providers", 2)]


@pytest.mark.asyncio
async def test_normalized_rows_use_the_ordinary_candidate_writer(monkeypatch, tmp_path):
    binding = SimpleNamespace()
    scope = SimpleNamespace(bindings={nomi.SOURCE_ID: binding}, credentials={})
    monkeypatch.setattr(source_proxy, "current_run", lambda: scope)
    monkeypatch.setattr(adapter, "load_search_plan", lambda: _plan(tmp_path))
    origin = AsyncMock(
        side_effect=[
            _page(1, "facilities", (101,), 1),
            _page(2),
            _page(1, "providers", (101,), 1),
            _page(2),
        ]
    )
    _mock_transport(monkeypatch, origin)
    source_record_by_field = {
        **adapter.source_row(directory._now()),
        "_pagination_checkpoint_context": SimpleNamespace(
            acquisition_root_run_id="root", dataset_id="candidate", retry_of_run_id=None
        ),
        "_endpoint_dataset_id": "candidate",
        "_resource_hash_contract": "fixture-resource-hash",
        "_semantic_projection_as_of": "2026-01-01",
    }
    endpoint = directory._provider_directory_api_endpoint_row(source_record_by_field)
    assert endpoint["canonical_api_base"] == nomi.ENDPOINT_SCOPE
    assert directory._endpoint_dataset_expected_resources([source_record_by_field]) == nomi.RESOURCE_TYPES
    copied_rows = []

    async def persist(model, rows):
        copied_rows.extend((model.__name__, copied_row) for copied_row in rows)
        return len(rows)

    source_ids, diagnostics, counts, *_ = await adapter.import_group(
        directory,
        source_record_by_field,
        run_id="fixture-run",
        persist=persist,
        progress_callback=AsyncMock(),
        cancel_check=AsyncMock(),
    )
    assert source_ids == [nomi.SOURCE_ID]
    assert len(copied_rows) == sum(counts.values())
    assert set(diagnostics) == set(nomi.RESOURCE_TYPES)
    assert all(diagnostic["complete"] for diagnostic in diagnostics.values())
    assert next(copied_row for model, copied_row in copied_rows if model == "ProviderDirectoryPractitionerRole")[
        "network_refs"
    ] == [f"Organization/{NETWORK_ID}"]
    assert next(copied_row for model, copied_row in copied_rows if model == "ProviderDirectoryOrganizationAffiliation")[
        "network_refs"
    ] == [f"Organization/{NETWORK_ID}"]


@pytest.fixture
def failed_retry():
    record = adapter.source_row(directory._now())
    context = directory._resource_group_pagination_checkpoint_context(
        [record], run_id="retry", retry_of_run_id="root", pagination_root_run_id="root", is_checkpointing_enabled=True
    )
    header_by_field = {
        "dataset_id": "candidate",
        "endpoint_id": "endpoint",
        "acquisition_root_run_id": "root",
        "import_run_id": "root",
        "status": "failed",
        "resource_count": 0,
        "is_current": False,
    }
    return record, context, header_by_field


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("import_run_id", "wrong-parent"),
        ("resource_count", 1),
        ("status", "acquiring"),
        ("completion_proof_sha256", "existing-proof"),
        ("canonical_api_base", "https://example.com"),
        ("source_ids", ("different-source",)),
        ("checkpoint_dataset_id", "candidate"),
    ],
)
def test_retry_selection_requires_the_exact_empty_failed_parent(failed_retry, field, value):
    _, context, header = failed_retry
    assert directory._should_repair_empty_endpoint_dataset_orphan(header, "endpoint", "root", None, "root", context)
    if hasattr(context, field):
        context = replace(context, **{field: value})
    elif field in header or field == "completion_proof_sha256":
        header[field] = value
    checkpoint_id = value if field == "checkpoint_dataset_id" else None
    with pytest.raises(RuntimeError, match="uncheckpointed_candidate_exists"):
        directory._should_repair_empty_endpoint_dataset_orphan(
            header, "endpoint", "root", checkpoint_id, "root", context
        )


async def _build_retry_candidate(monkeypatch, failed_retry, error):
    record, context, header = failed_retry
    header["publication_metadata_json"] = {"error": error}
    monkeypatch.setattr(directory, "_checkpoint_candidate_dataset_id", AsyncMock(return_value=None))
    monkeypatch.setattr(directory, "_previous_endpoint_dataset_id", AsyncMock(return_value=None))
    profile = directory.EndpointDatasetVerificationProfile(is_twin_root_required=False)
    selection = await directory._select_resumable_endpoint_dataset_candidate(
        header, "candidate", "endpoint", nomi.RESOURCE_TYPES, "root", "root", context, profile
    )
    candidate = directory._build_endpoint_dataset_candidate(
        [record], "endpoint", nomi.RESOURCE_TYPES, nomi.RESOURCE_TYPES, selection, "retry", context, profile
    )
    header["publication_metadata_json"] = {**directory._endpoint_dataset_candidate_metadata(candidate), "error": error}
    return candidate


@pytest.mark.asyncio
@pytest.mark.parametrize("before_origin", [True, False])
async def test_retry_reopens_verified_named_storage(monkeypatch, tmp_path, failed_retry, before_origin):
    plan = replace(_plan(tmp_path), root=tmp_path / nomi.SOURCE_ID)
    monkeypatch.setenv("HLTHPRT_PROVIDER_DIRECTORY_ARTIFACT_ROOT", str(tmp_path))
    location = plan.root / "tmp" if before_origin else plan.root / "root" / "capture.sqlite3"
    error = str(PermissionError(errno.EACCES, os.strerror(errno.EACCES), str(location)))
    candidate = await _build_retry_candidate(monkeypatch, failed_retry, error)
    context = candidate.checkpoint_context
    assert context.allow_initial_nomi_capture is before_origin
    _, _, header = failed_retry
    connection = SimpleNamespace(first=AsyncMock(return_value=header))
    monkeypatch.setattr(source_policy, "source_transaction", lambda _db: nullcontext())
    monkeypatch.setattr(source_adapters, "require_source_transaction", Mock())
    monkeypatch.setattr(source_adapters, "require_generic_source_layout", AsyncMock())
    monkeypatch.setattr(directory.db, "acquire", lambda: nullcontext(connection))
    monkeypatch.setattr(directory, "_lock_endpoint_dataset_candidate_admission", AsyncMock())
    monkeypatch.setattr(directory, "_assert_no_conflicting_endpoint_candidate", AsyncMock(return_value=candidate))
    proof = AsyncMock(return_value=True)
    upsert, reset, prepare = AsyncMock(), AsyncMock(), AsyncMock()
    monkeypatch.setattr(source_storage, "has_empty_named_root_storage", proof)
    monkeypatch.setattr(directory, "_upsert_endpoint_dataset_candidate", upsert)
    monkeypatch.setattr(directory, "_reset_new_endpoint_candidate", reset)
    monkeypatch.setattr(source_storage, "prepare_generic_source_candidate", prepare)

    await directory._ensure_endpoint_dataset_candidate(candidate)
    proof.assert_awaited_once_with(directory, candidate, header)
    upsert.assert_awaited_once()
    reset.assert_not_awaited()
    prepare.assert_not_awaited()
    proof.return_value = False
    with pytest.raises(RuntimeError, match="candidate_stale"):
        await directory._ensure_endpoint_dataset_candidate(candidate)
    upsert.assert_awaited_once()
    await _assert_retry_origin_boundary(monkeypatch, plan, failed_retry[0], candidate, before_origin)


async def _assert_retry_origin_boundary(monkeypatch, plan, record, candidate, before_origin):
    scope = SimpleNamespace(bindings={nomi.SOURCE_ID: object()}, credentials={})
    monkeypatch.setattr(source_proxy, "current_run", lambda: scope)
    monkeypatch.setattr(adapter, "load_search_plan", lambda: plan)
    origin = AsyncMock(side_effect=TimeoutError())
    _mock_transport(monkeypatch, origin)
    record.update(
        _pagination_checkpoint_context=candidate.checkpoint_context,
        _endpoint_dataset_id=candidate.dataset_id,
        _resource_hash_contract=candidate.resource_hash_contract,
    )
    persist = AsyncMock()
    with pytest.raises(TimeoutError if before_origin else ValueError) as failure:
        await adapter.import_group(
            directory, record, run_id="retry", persist=persist, progress_callback=AsyncMock(), cancel_check=AsyncMock()
        )
    persist.assert_not_awaited()
    if before_origin:
        origin.assert_awaited_once()
        assert (plan.root / "root" / "scope.json").is_file()
    else:
        assert str(failure.value) == "nomi_directory_retained_edition_required"
        origin.assert_not_awaited()
