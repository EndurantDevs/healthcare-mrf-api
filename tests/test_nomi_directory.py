# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Nomi search retention, coverage and normalized source admission."""

import json
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import nomi_directory_capture as nomi
from process.ext.source_http_proxy import SourceProxyError
from process.nomi_directory_normalization import NETWORK_ID


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


def _acquire(plan, root_run_id):
    return nomi.acquire_directory(
        plan,
        root_run_id,
        object(),
        cancel_check=AsyncMock(),
        progress=AsyncMock(),
        pause=AsyncMock(),
    )


def _mock_transport(monkeypatch, origin_page):
    route = SimpleNamespace(proxy_id="test-proxy")
    monkeypatch.setattr(nomi, "_selection", lambda *_args, **_kwargs: route)
    monkeypatch.setattr(nomi.FinderAdmission, "admit", lambda _self, reserve: _reserve(reserve))
    monkeypatch.setattr(nomi, "_origin_page", origin_page)


async def _reserve(reserve):
    reserve()


@pytest.mark.asyncio
async def test_resume_reuses_only_same_edition_and_fresh_root_refetches(monkeypatch, tmp_path):
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
            raise SourceProxyError("fixture_interruption")
        if page == 1:
            return _page(page, kind, (101,), 1)
        return _page(page)

    _mock_transport(monkeypatch, request)
    with pytest.raises(SourceProxyError, match="fixture_interruption"):
        async with _acquire(plan, "first-root"):
            pytest.fail("interrupted capture became eligible to publish")
    assert calls == [("facilities", 1), ("facilities", 2), ("providers", 1), ("providers", 2)]

    calls.clear()
    async with _acquire(plan, "first-root") as (capture, receipt):
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
        async with _acquire(replace(plan, page_size=3), "first-root"):
            pytest.fail("changed pagination reused a root")


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
async def test_publisher_hold_blocks_a_new_edition_without_rotating_routes(monkeypatch, tmp_path, status):
    origin_posts = []
    route = SimpleNamespace(proxy_id="test-proxy", request_options={})
    binding = SimpleNamespace(defer=lambda *_: None, cooldown_snapshot=lambda: {"cooldowns": []})
    pause = AsyncMock()
    response = SimpleNamespace(status=status, headers={"Retry-After": "600"})

    class HTTP:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_):
            return False

    class Exchange(HTTP):
        async def __aenter__(self):
            return response

    class Session(HTTP):
        def post(self, url, **kwargs):
            assert kwargs["allow_redirects"] is False
            assert url.endswith("/facilities")
            origin_posts.append(url)
            return Exchange()

    monkeypatch.setattr(nomi.aiohttp, "ClientSession", lambda **_: Session())
    monkeypatch.setattr(nomi, "_selection", lambda *_args, **_kwargs: route)
    for edition in ("first-root", "later-root"):
        with pytest.raises(SourceProxyError):
            async with nomi.acquire_directory(
                _plan(tmp_path), edition, binding, cancel_check=AsyncMock(), progress=AsyncMock(), pause=pause
            ):
                pytest.fail("held source admitted a candidate")
    assert len(origin_posts) == 1
    assert pause.await_count == 2


@pytest.mark.asyncio
async def test_normalized_rows_use_the_ordinary_candidate_writer(monkeypatch, tmp_path):
    from importlib import import_module

    from process import nomi_directory_integration as adapter
    from process import provider_directory_source_proxy as source_proxy

    directory = import_module("process.provider_directory_fhir")
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
        "_pagination_checkpoint_context": SimpleNamespace(acquisition_root_run_id="root", dataset_id="candidate"),
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
