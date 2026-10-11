# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Synthetic CMS catalogs prove selection, never source population coverage."""

import asyncio
from datetime import date

import pytest

from process import registry_source_editions as editions
from process.registry_source_editions import (
    RegistrySourceEditionDiscoveryError,
    RegistrySourceEditionPolicy,
    discover_registry_source_edition,
    parse_registry_source_editions,
    registry_source_catalog_spec,
    select_registry_source_edition,
)
from tests.test_registry_source_fetch import _source_http


def _mlr(year=2024, *, href="/files/zip/synthetic-mlr.zip", as_of="September 12, 2025", heading=None):
    heading = year if heading is None else heading
    suffix = f" (as of {as_of})" if as_of else ""
    return (
        f"<h2>{heading} Reporting Year</h2><ul><li>"
        f'<a href="{href}">Public Use File for {year} (ZIP)</a>{suffix}</li></ul>'
    )


def _plan(year=2026, quarter=1, *, release="", href="/files/zip/synthetic-plan.zip", label_release=None):
    label_release = release if label_release is None else label_release
    return (
        f"<ul><li>Quarter {quarter} {year}&nbsp;Data Release {release}<br>Release description"
        f'<ul><li><a href="{href}">Quarter {quarter} {year} HIOS and RBIS Data '
        f"{label_release} (ZIP)</a></li></ul></li></ul>"
    )


def _parse(source_id, text):
    return parse_registry_source_editions(source_id, text.encode())


def test_mlr_reporting_year_as_of_and_unknown_artifact_provenance():
    (candidate,) = _parse("commercial-mlr", _mlr())
    assert candidate.reporting_year == 2024 and candidate.as_of_date == date(2025, 9, 12)
    assert candidate.edition_id == "2024-as-of-2025-09-12"
    assert candidate.source_url == "https://www.cms.gov/files/zip/synthetic-mlr.zip"
    assert candidate.artifact_sha256 is None and candidate.published_at is None
    assert candidate.quarter is None and candidate.release is None


def test_mlr_missing_as_of_remains_unknown_and_duplicate_links_coalesce():
    (candidate,) = _parse("commercial-mlr", _mlr(as_of=None) * 2)
    assert candidate.edition_id == "2024" and candidate.as_of_date is None


def test_latest_fixed_year_and_constrained_latest_ignore_document_order():
    candidates = _parse("commercial-mlr", _mlr(2024) + _mlr(2022) + _mlr(2023))
    assert (
        select_registry_source_edition(candidates, RegistrySourceEditionPolicy("latest-verified")).reporting_year
        == 2024
    )
    for policy in ("fixed-year", "latest-verified"):
        assert (
            select_registry_source_edition(candidates, RegistrySourceEditionPolicy(policy, 2022)).reporting_year == 2022
        )
    with pytest.raises(RegistrySourceEditionDiscoveryError):
        select_registry_source_edition(candidates, RegistrySourceEditionPolicy("fixed-year", 2020))


def test_same_year_mlr_revisions_use_explicit_as_of_date():
    candidates = _parse("commercial-mlr", _mlr(as_of="October 2, 2025") + _mlr())
    selected = select_registry_source_edition(candidates, RegistrySourceEditionPolicy("fixed-year", 2024))
    assert selected.as_of_date == date(2025, 10, 2)


def test_undated_mlr_revision_cannot_be_ordered_against_dated_revision():
    candidates = _parse("commercial-mlr", _mlr() + _mlr(as_of=None, href="/files/zip/undated.zip"))
    with pytest.raises(RegistrySourceEditionDiscoveryError):
        select_registry_source_edition(candidates, RegistrySourceEditionPolicy("fixed-year", 2024))


def test_planfinder_quarter_and_release_identity_are_explicit():
    candidates = _parse("plan-finder", _plan(release="(PRELIMINARY)") + _plan(release="(FINAL)"))
    assert {candidate.edition_id for candidate in candidates} == {"2026-q1-preliminary", "2026-q1-final"}
    assert (
        select_registry_source_edition(candidates, RegistrySourceEditionPolicy("fixed-year", 2026)).release == "final"
    )
    assert all(candidate.artifact_sha256 is None and candidate.published_at is None for candidate in candidates)


def test_preliminary_requires_opt_in_and_newer_quarter_orders_before_release_channel():
    candidates = _parse("plan-finder", _plan(release="(FINAL)") + _plan(quarter=2, release="(PRELIMINARY)"))
    assert select_registry_source_edition(candidates, RegistrySourceEditionPolicy("latest-verified")).quarter == 1
    selected = select_registry_source_edition(
        candidates, RegistrySourceEditionPolicy("latest-verified", include_preliminary=True)
    )
    assert selected.quarter == 2 and selected.release == "preliminary"
    with pytest.raises(RegistrySourceEditionDiscoveryError):
        select_registry_source_edition(
            _parse("plan-finder", _plan(release="(PRELIMINARY)")), RegistrySourceEditionPolicy("latest-verified")
        )


def test_planfinder_unlabelled_release_does_not_infer_final_from_description():
    (candidate,) = _parse(
        "plan-finder", _plan(quarter=4).replace("Release description", "This is final iteration data")
    )
    assert candidate.release == "unspecified" and candidate.edition_id == "2026-q4-unspecified"


@pytest.mark.parametrize("release", ["(PRELIMINARY RELEASE)", "(FINAL) (PRELIMINARY)"])
def test_unsupported_or_conflicting_release_title_cannot_bypass_preliminary_policy(release):
    with pytest.raises(RegistrySourceEditionDiscoveryError):
        _parse("plan-finder", _plan(release=release, label_release=""))


@pytest.mark.parametrize(
    "text,source_id",
    [
        (_mlr(heading=2023), "commercial-mlr"),
        (_mlr(2009), "commercial-mlr"),
        (_mlr(as_of="September 31, 2025"), "commercial-mlr"),
        (_mlr() + _mlr(href="/files/zip/different.zip"), "commercial-mlr"),
        (_plan(release="(FINAL)", label_release="(Preliminary)"), "plan-finder"),
        (_plan(quarter=5), "plan-finder"),
        (_plan().replace("Quarter 1 2026&nbsp;Data Release", "Quarter 2 2026&nbsp;Data Release"), "plan-finder"),
        ('<a href="/files/zip/synthetic.zip">Quarter 1 2026 HIOS and RBIS Data</a>', "plan-finder"),
        ("<h2>Unknown catalog layout</h2>", "commercial-mlr"),
        ('<a href="/files/zip/rbis.zip">Quarter 1 2020 RBIS Data</a>', "plan-finder"),
    ],
)
def test_unknown_mismatched_and_conflicting_catalogs_refuse(text, source_id):
    with pytest.raises(RegistrySourceEditionDiscoveryError):
        _parse(source_id, text)


@pytest.mark.parametrize(
    "source_id,text",
    [
        ("commercial-mlr", _mlr() + '<h2>2025 Reporting Year</h2><a href="/files/zip/new.zip">Changed layout</a>'),
        (
            "plan-finder",
            _plan()
            + '<ul><li>Quarter 2 2026 Data Release<ul><li><a href="/files/zip/new.zip">Changed layout</a></li></ul></li></ul>',
        ),
    ],
)
def test_unsupported_newer_declared_period_cannot_silently_select_older(source_id, text):
    with pytest.raises(RegistrySourceEditionDiscoveryError):
        _parse(source_id, text)


@pytest.mark.parametrize(
    "href",
    [
        "https://example.com/synthetic.zip",
        "http://www.cms.gov/files/synthetic.zip",
        "https://www.cms.gov@localhost/synthetic.zip",
        "/files/zip/synthetic.zip?token=value",
        "/files/zip/synthetic.zip#fragment",
        "/files/document/synthetic.pdf",
    ],
)
def test_artifact_links_require_existing_official_origin_contract(href):
    with pytest.raises(RegistrySourceEditionDiscoveryError):
        _parse("commercial-mlr", _mlr(href=href))


@pytest.mark.parametrize(
    "policy,year,preliminary",
    [
        ("unknown", None, False),
        ("fixed-year", None, False),
        ("fixed-year", True, False),
        ("fixed-year", 2009, False),
        ("latest-verified", 2101, False),
        ("latest-verified", None, "yes"),
        ([], None, False),
    ],
)
def test_policy_is_closed(policy, year, preliminary):
    with pytest.raises(RegistrySourceEditionDiscoveryError):
        RegistrySourceEditionPolicy(policy, year, preliminary)


@pytest.mark.parametrize(
    "source_id,html",
    [
        ("naic", b"synthetic"),
        ("commercial-mlr", b""),
        ("commercial-mlr", b"x" * (editions.MAX_CATALOG_BYTES + 1)),
        ("commercial-mlr", b"\xff"),
    ],
)
def test_unknown_sources_and_catalog_bounds_refuse(source_id, html):
    with pytest.raises(RegistrySourceEditionDiscoveryError):
        parse_registry_source_editions(source_id, html)


@pytest.mark.parametrize(
    "suffix",
    [
        '<a href="/files/zip/unfinished.zip">',
        "<h2>unfinished",
        "<li>" * 33,
        '<a href="/">unrelated</a>' * 1001,
    ],
)
def test_truncated_and_excessively_nested_catalogs_refuse(suffix):
    with pytest.raises(RegistrySourceEditionDiscoveryError):
        _parse("commercial-mlr", _mlr() + suffix)


def test_selection_refuses_mixed_sources_and_non_candidates():
    mlr = _parse("commercial-mlr", _mlr())
    plan = _parse("plan-finder", _plan())
    policy = RegistrySourceEditionPolicy("latest-verified")
    for candidates in (mlr + plan, (object(),), []):
        with pytest.raises(RegistrySourceEditionDiscoveryError):
            select_registry_source_edition(candidates, policy)


@pytest.mark.asyncio
async def test_discovery_reuses_genuine_fetch_receipt_and_retained_catalog(tmp_path):
    async with _source_http(_mlr().encode()) as (state, transport):
        state.headers = {"ETag": '"synthetic-catalog"', "Last-Modified": "Fri, 12 Sep 2025 00:00:00 GMT"}
        state.conditional = True
        first = await discover_registry_source_edition(
            "commercial-mlr", RegistrySourceEditionPolicy("latest-verified"), tmp_path, _test_transport=transport
        )
        replay = await discover_registry_source_edition(
            "commercial-mlr",
            RegistrySourceEditionPolicy("fixed-year", 2024),
            tmp_path,
            previous=first.catalog.receipt,
            _test_transport=transport,
        )
    assert replay.catalog.unchanged and replay.candidate == first.candidate
    assert state.requests[-1]["If-None-Match"] == '"synthetic-catalog"'
    assert replay.catalog.receipt.spec == registry_source_catalog_spec("commercial-mlr")
    assert replay.catalog.receipt.spec.reporting_year is None and replay.catalog.receipt.spec.published_at is None
    assert replay.candidate.artifact_sha256 is None and replay.candidate.published_at is None
    assert len(list(tmp_path.iterdir())) == 1


@pytest.mark.asyncio
async def test_newer_catalog_candidate_and_failed_layout_preserve_retained_bytes(tmp_path):
    async with _source_http(_mlr(2023).encode()) as (state, transport):
        first = await discover_registry_source_edition(
            "commercial-mlr", RegistrySourceEditionPolicy("latest-verified"), tmp_path, _test_transport=transport
        )
        old_bytes = first.catalog.artifact_path.read_bytes()
        state.body = _mlr(2024).encode()
        newer = await discover_registry_source_edition(
            "commercial-mlr",
            RegistrySourceEditionPolicy("latest-verified"),
            tmp_path,
            previous=first.catalog.receipt,
            _test_transport=transport,
        )
        assert newer.candidate.reporting_year == 2024 and not newer.catalog.unchanged
        state.body = b"unsupported layout"
        with pytest.raises(RegistrySourceEditionDiscoveryError):
            await discover_registry_source_edition(
                "commercial-mlr",
                RegistrySourceEditionPolicy("latest-verified"),
                tmp_path,
                previous=newer.catalog.receipt,
                _test_transport=transport,
            )
        state.status = 503
        with pytest.raises(RegistrySourceEditionDiscoveryError):
            await discover_registry_source_edition(
                "commercial-mlr", RegistrySourceEditionPolicy("latest-verified"), tmp_path, _test_transport=transport
            )
    assert first.catalog.artifact_path.read_bytes() == old_bytes
    assert newer.catalog.artifact_path.is_file()


def test_catalog_tampering_is_refused_before_parse(tmp_path):
    import hashlib
    from types import SimpleNamespace

    html = _mlr().encode()
    path = tmp_path / "synthetic.artifact"
    path.write_bytes(html.replace(b"2024", b"2023"))
    catalog = SimpleNamespace(
        artifact_path=path,
        receipt=SimpleNamespace(artifact_bytes=len(html), artifact_sha256=hashlib.sha256(html).hexdigest()),
    )
    with pytest.raises(RegistrySourceEditionDiscoveryError):
        editions._read_catalog("commercial-mlr", catalog)


@pytest.mark.asyncio
async def test_cancelled_discovery_propagates_without_catalog_result(tmp_path, monkeypatch):
    async def cancelled(*args, **kwargs):
        raise asyncio.CancelledError()

    monkeypatch.setattr(editions, "fetch_registry_source", cancelled)
    with pytest.raises(asyncio.CancelledError):
        await discover_registry_source_edition(
            "commercial-mlr", RegistrySourceEditionPolicy("latest-verified"), tmp_path
        )
