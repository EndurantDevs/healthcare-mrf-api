# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import importlib
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest


ms_drg = importlib.import_module("process.ms_drg")


MS_DRG_LIST_HTML = """
<table class=appnda>
<tr><td>001,  ,P,Heart Transplant or Implant of Heart Assist System with MCC
<tr><td>014,  ,M,Allogeneic Bone Marrow Transplant
<tr><td>470,MDC 08,P,Major Hip and Knee Joint Replacement or Reattachment of Lower Extremity without MCC
</table>
"""


PROCEDURE_INDEX_HTML = """
<table class="codelst">
<tr class="tabhed"><th>CODE</th><th>MDC</th><th>MS-DRG</th><th>SURGICAL CATEGORY</th></tr>
<tr><td>0016070</td><td>01</td><td>031-033</td><td>Ventricular Shunt Procedures</td></tr>
<tr><td></td><td>17</td><td>820-822</td><td>Lymphoma and Leukemia with Major O.R. Procedures</td></tr>
<tr><td>0SR90JZ</td><td>08</td><td>469-470</td><td>Major Hip and Knee Joint Replacement</td></tr>
</table>
"""


DIAGNOSIS_INDEX_HTML = """
<table class="codelst">
<tr><th>DX</th><th>MDC</th><th>MS-DRG</th><th></th><th>DX</th><th>MDC</th><th>MS-DRG</th></tr>
<tr><td>A000</td><td>06</td><td>371-373</td><td></td><td>A4181</td><td>15</td><td>791</td></tr>
<tr><td>A850</td><td>01</td><td>097-099</td><td></td><td></td><td></td><td></td></tr>
</table>
"""


def test_parse_ms_drg_catalog_rows():
    rows = ms_drg._parse_ms_drg_catalog_rows(MS_DRG_LIST_HTML)

    by_code = {row.code: row for row in rows}
    assert by_code["001"].designation == "P"
    assert by_code["014"].designation == "M"
    assert by_code["470"].mdc == "MDC 08"
    assert by_code["470"].title.startswith("Major Hip and Knee Joint Replacement")


def test_parse_procedure_index_relationships_carries_blank_code_and_expands_ranges():
    relationships, procedure_codes = ms_drg._parse_procedure_index_relationships(PROCEDURE_INDEX_HTML)

    assert procedure_codes["0016070"] == "Ventricular Shunt Procedures"
    assert procedure_codes["0SR90JZ"] == "Major Hip and Knee Joint Replacement"
    assert ("MS_DRG", "031", "uses_icd10pcs", "ICD10PCS", "0016070") in relationships
    assert ("MS_DRG", "822", "uses_icd10pcs", "ICD10PCS", "0016070") in relationships
    assert ("ICD10PCS", "0SR90JZ", "groups_to_ms_drg", "MS_DRG", "470") in relationships


def test_parse_diagnosis_index_relationships_handles_three_column_groups():
    relationships, diagnosis_codes = ms_drg._parse_diagnosis_index_relationships(DIAGNOSIS_INDEX_HTML)

    assert {"A000", "A4181", "A850"}.issubset(diagnosis_codes)
    assert ("MS_DRG", "371", "uses_icd10cm", "ICD10CM", "A000") in relationships
    assert ("MS_DRG", "791", "uses_icd10cm", "ICD10CM", "A4181") in relationships
    assert ("ICD10CM", "A850", "groups_to_ms_drg", "MS_DRG", "099") in relationships


def test_find_latest_manual_toc_url_skips_proposed_links():
    html = """
    <h3>FY 2027 - Version 44 Test GROUPER - DRAFT</h3>
    <a href="/icd10m/FY2027-nprm-v44-fullcode-cms/fullcode_cms/P0001.html">
      Proposed ICD-10-CM/PCS MS-DRG V44 Definitions Manual Table of Contents - Full Titles - HTML Version
    </a>
    <h3>FY 2026 - Version 43.1</h3>
    <a href="/icd10m/FY2026-fr-v43.1-fullcode-cms/fullcode_cms/P0001.html">
      V43.1 Definitions Manual Table of Contents - Full Titles - HTML Version
    </a>
    """

    result = ms_drg._find_latest_manual_toc_url(html, "https://www.cms.gov/ms-drg")

    assert result == "https://www.cms.gov/icd10m/FY2026-fr-v43.1-fullcode-cms/fullcode_cms/P0001.html"


@pytest.mark.asyncio
async def test_catalog_only_import_replaces_only_ms_drg_sources(monkeypatch):
    class FakeDb:
        async def status(self, *_args, **_kwargs):
            return None

        async def create_table(self, *_args, **_kwargs):
            return None

    class FakeStage:
        __tablename__ = "stage"
        __table__ = object()

    html_by_url = {
        "https://example.test/toc.html": """
            <a href="appendix-a.html">Appendix A List of MS-DRGs</a>
        """,
        "https://example.test/appendix-a.html": """
            <a href="list.html">List of MS-DRGs</a>
        """,
        "https://example.test/list.html": MS_DRG_LIST_HTML,
    }
    calls = []

    async def noop(*_args, **_kwargs):
        return None

    async def push(_stage, rows):
        return len(rows)

    async def merge_catalog(_stage, _schema, sources):
        calls.append(("catalog", sources))

    async def merge_synonym(_stage, _schema, sources):
        calls.append(("synonym", sources))

    async def merge_relationship(_stage, _schema, sources):
        calls.append(("relationship", sources))

    monkeypatch.setattr(ms_drg, "db", FakeDb())
    monkeypatch.setattr(ms_drg, "ensure_database", noop)
    monkeypatch.setattr(ms_drg, "_ensure_tables", noop)
    monkeypatch.setattr(ms_drg, "_download_text", lambda url: html_by_url[url])
    monkeypatch.setattr(ms_drg, "make_class", lambda *_args, **_kwargs: FakeStage)
    monkeypatch.setattr(ms_drg, "_push", push)
    monkeypatch.setattr(ms_drg, "_merge_catalog_stage", merge_catalog)
    monkeypatch.setattr(ms_drg, "_merge_synonym_stage", merge_synonym)
    monkeypatch.setattr(ms_drg, "_merge_relationship_stage", merge_relationship)

    import_summary = await ms_drg.import_ms_drg(
        include_relationships=False,
        manual_toc_url="https://example.test/toc.html",
        import_id="unit",
    )

    assert import_summary["relationship_rows"] == 0
    assert calls == [
        ("catalog", (ms_drg.SOURCE_MS_DRG,)),
        ("synonym", (ms_drg.SOURCE_MS_DRG,)),
    ]


def test_download_text_rejects_file_url(tmp_path, monkeypatch):
    secret = tmp_path / "secret.txt"
    secret.write_text("top-secret")
    monkeypatch.delenv("HLTHPRT_FETCH_ALLOW_LOCAL", raising=False)

    with pytest.raises(ValueError, match="http"):
        ms_drg._download_text(f"file://{secret}")


def test_download_text_rejects_private_hosts(monkeypatch):
    monkeypatch.delenv("HLTHPRT_FETCH_ALLOW_LOCAL", raising=False)

    with pytest.raises(ValueError, match="non-public IP"):
        ms_drg._download_text("http://169.254.169.254/latest/meta-data/")

    with pytest.raises(ValueError, match="non-public IP"):
        ms_drg._download_text("http://127.0.0.1/toc.html")


def test_download_text_aborts_oversize_body(monkeypatch):
    body = b"x" * (256 * 1024)

    class _Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *_args):  # silence test server logging
            return

    server = HTTPServer(("127.0.0.1", 0), _Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        port = server.server_address[1]
        monkeypatch.setenv("HLTHPRT_FETCH_ALLOW_LOCAL", "true")
        monkeypatch.setenv("HLTHPRT_FETCH_MAX_BYTES", str(16 * 1024))

        with pytest.raises(ValueError, match="exceeds"):
            ms_drg._download_text(f"http://127.0.0.1:{port}/toc.html")
    finally:
        server.shutdown()
        thread.join(timeout=5)
