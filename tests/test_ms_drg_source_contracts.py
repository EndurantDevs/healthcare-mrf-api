# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import importlib
from types import SimpleNamespace

import pytest

import process.ms_drg_sources as sources
from process.control_cancel import ImportCancelledError

ms_drg = importlib.import_module("process.ms_drg")


def test_table_parser_preserves_breaks_and_ignores_empty_rows():
    """CMS table cells normalize entities and line breaks without inventing rows."""
    source_html = """
    <table>
      <tr></tr>
      <tr><th>CODE</th><th>DESCRIPTION</th></tr>
      <tr><td>A00<br>detail</td><td>Alpha&nbsp; entry</td></tr>
    </table>
    """

    assert sources._parse_tables(source_html) == [
        ["CODE", "DESCRIPTION"],
        ["A00 detail", "Alpha entry"],
    ]
    assert sources._clean_text(None) == ""


def test_catalog_parser_rejects_malformed_and_empty_entries():
    """Only four-field, three-digit CMS catalog records satisfy the parser contract."""
    source_html = """
    <tr><td>001,MDC 01,P,Synthetic surgical group
    <tr><td>12,MDC 01,M,Short code
    <tr><td>002,MDC 02,M,
    <tr><td>003,too,few
    """

    catalog_rows = sources._parse_ms_drg_catalog_rows(source_html)

    assert [(catalog_record.code, catalog_record.title) for catalog_record in catalog_rows] == [
        ("001", "Synthetic surgical group")
    ]
    assert sources._parse_ms_drg_catalog_rows("") == []


def test_code_range_expansion_enforces_bounded_forward_ranges():
    """Single codes and bounded ascending ranges expand; malformed ranges do not."""
    assert sources._expand_ms_drg_values("001, 003-005 and 020") == [
        "001",
        "003",
        "004",
        "005",
        "020",
    ]
    assert sources._expand_ms_drg_values("010-001 001-999") == []
    assert sources._expand_ms_drg_values("") == []


def test_index_parsers_skip_headers_invalid_codes_and_short_rows():
    """Diagnosis and procedure indexes retain only structurally valid code relationships."""
    diagnosis_html = """
    <table>
      <tr><th>DIAGNOSIS</th><th>MDC</th><th>MS-DRG</th></tr>
      <tr><td>?</td><td>01</td><td>001</td></tr>
      <tr><td>A00.1</td><td>01</td><td>001-002</td></tr>
      <tr><td>A001</td></tr>
    </table>
    """
    procedure_html = """
    <table>
      <tr><th>CODE</th><th>MDC</th><th>MS-DRG</th><th>CATEGORY</th></tr>
      <tr><td>SHORT</td><td>01</td><td>001</td><td>Invalid</td></tr>
      <tr><td>0ABCDEF</td><td>01</td><td>001</td><td></td></tr>
      <tr><td></td><td>01</td><td>002</td><td>Synthetic category</td></tr>
      <tr><td>only</td></tr>
    </table>
    """

    diagnosis_relationships, diagnosis_codes = (
        sources._parse_diagnosis_index_relationships(diagnosis_html)
    )
    procedure_relationships, procedure_category_by_code = (
        sources._parse_procedure_index_relationships(procedure_html)
    )

    assert diagnosis_codes == {"A001"}
    assert len(diagnosis_relationships) == 4
    assert procedure_category_by_code == {"0ABCDEF": "Synthetic category"}
    assert len(procedure_relationships) == 4


def test_manual_discovery_scores_final_links_and_handles_absence():
    """Final-rule links outrank drafts, while pages without a matching link return none."""
    source_html = """
    <h2>FY 2027 Proposed Rule</h2>
    <a href="/draft-v44/P0001.html">Definitions Manual Table of Contents draft</a>
    <h2>FY 2026 Final Rule</h2>
    <a href="/FY2026-fr-v43.1/P0001.html">Definitions Manual Table of Contents</a>
    <a href="/FY2026-v43/P0001.html">Definitions Manual Table of Contents</a>
    """

    discovered_url = sources._find_latest_manual_toc_url(
        source_html,
        "https://www.cms.gov/root",
    )

    assert discovered_url == "https://www.cms.gov/FY2026-fr-v43.1/P0001.html"
    assert sources._find_latest_manual_toc_url("<p>none</p>", "https://cms.test") is None
    assert sources._find_link(source_html, r"missing", "https://cms.test") is None


@pytest.mark.parametrize(
    ("source_html", "fallback_url", "expected_release"),
    [
        ("Version 44.2 Definitions Manual", "https://cms.test/page", "v44.2"),
        ("Definitions Manual", "https://cms.test/v43-1/page", "v43.1"),
        ("Definitions Manual", "https://cms.test/page", "unknown"),
    ],
)
def test_release_extraction_has_html_url_and_unknown_contracts(
    source_html,
    fallback_url,
    expected_release,
):
    """Release labels are deterministic across HTML, URL, and missing metadata."""
    assert sources._extract_release(source_html, fallback_url) == expected_release


def test_sequential_page_discovery_handles_limits_and_bad_navigation():
    """Sequential CMS index URLs obey page limits and reject unknown next-link formats."""
    page_html = """
    <p>Page 1 of 4</p>
    <a id="next_page" href="P0101.html">Next</a>
    """
    assert sources._discover_sequential_index_urls(
        page_html,
        "https://cms.test/P0100.html",
    ) == [
        "https://cms.test/P0100.html",
        "https://cms.test/P0101.html",
        "https://cms.test/P0102.html",
        "https://cms.test/P0103.html",
    ]
    assert len(
        sources._discover_sequential_index_urls(
            page_html,
            "https://cms.test/P0100.html",
            limit=2,
        )
    ) == 2
    assert sources._discover_sequential_index_urls(
        '<p>Page 1 of 4</p><a id="next_page" href="next.html">Next</a>',
        "https://cms.test/P0100.html",
    ) == ["https://cms.test/P0100.html"]
    assert sources._discover_sequential_index_urls(
        "<p>single page</p>",
        "https://cms.test/P0100.html",
        limit=1,
    ) == ["https://cms.test/P0100.html"]


@pytest.mark.asyncio
async def test_parallel_download_returns_url_payload_pairs(monkeypatch):
    """Concurrent downloads retain URL association and clamp invalid concurrency."""
    monkeypatch.setattr(sources, "_download_text", lambda url: f"body:{url}")

    page_pairs = await sources._download_many(["one", "two"], concurrency=0)

    assert page_pairs == [("one", "body:one"), ("two", "body:two")]


def test_cancel_probe_supports_redis_paths_and_fail_open(monkeypatch):
    """MS-DRG cancellation recognizes both Redis configuration paths."""
    connections = []

    class CancelClient:
        def get(self, key):
            assert key == "cancel:run-5"
            return b"1"

    class RedisFactory:
        @staticmethod
        def from_url(redis_dsn, **timeouts):
            connections.append((redis_dsn, timeouts))
            return CancelClient()

        def __new__(cls, **connection):
            connections.append(connection)
            return CancelClient()

    monkeypatch.setattr(sources, "redis", SimpleNamespace(Redis=RedisFactory))
    monkeypatch.setattr(
        sources,
        "build_redis_settings",
        lambda: SimpleNamespace(host="cache", port=6379, password=None, database=0),
    )
    monkeypatch.setenv("HLTHPRT_REDIS_ADDRESS", "redis://cache")
    assert sources._is_cancel_requested("run-5") is True
    monkeypatch.delenv("HLTHPRT_REDIS_ADDRESS")
    assert sources._is_cancel_requested("run-5") is True
    assert sources._is_cancel_requested(None) is False

    monkeypatch.setattr(sources, "redis", None)
    assert sources._is_cancel_requested("run-5") is False
    monkeypatch.setattr(
        sources,
        "redis",
        SimpleNamespace(
            Redis=type(
                "BrokenRedis",
                (),
                {"__new__": lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError())},
            )
        ),
    )
    assert sources._is_cancel_requested("run-5") is False


def test_raise_if_cancelled_preserves_run_context(monkeypatch):
    """Cancellation errors identify the import run that must stop."""
    monkeypatch.setattr(sources, "_is_cancel_requested", lambda _run_id: True)
    with pytest.raises(ImportCancelledError, match="run-6"):
        sources._raise_if_cancelled("run-6")

    monkeypatch.setattr(sources, "_is_cancel_requested", lambda _run_id: False)
    assert sources._raise_if_cancelled("run-6") is None
