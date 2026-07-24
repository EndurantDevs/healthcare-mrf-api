# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import importlib
from contextlib import asynccontextmanager

import pytest

import process.ms_drg_publication as publication
from db.models import CodeCatalog, CodeRelationship, CodeSynonym

ms_drg = importlib.import_module("process.ms_drg")


MS_DRG_LIST_HTML = """
<table>
<tr><td>001,MDC 01,P,Synthetic surgical group
<tr><td>470,,M,Synthetic medical group
</table>
"""

DIAGNOSIS_HTML = """
<p>Page 1 of 2</p>
<a id="next_page" href="P0101.html">Next</a>
<table>
<tr><th>DX</th><th>MDC</th><th>MS-DRG</th></tr>
<tr><td>A001</td><td>01</td><td>001</td></tr>
</table>
"""

PROCEDURE_HTML = """
<p>Page 1 of 2</p>
<a id="next_page" href="P0201.html">Next</a>
<table>
<tr><th>CODE</th><th>MDC</th><th>MS-DRG</th><th>CATEGORY</th></tr>
<tr><td>0ABCDEF</td><td>01</td><td>001</td><td>Synthetic procedure</td></tr>
</table>
"""

FULL_CMS_URL = "https://example.test/cms"
FULL_TOC_URL = "https://example.test/manual/P0001.html"
FULL_DIAGNOSIS_LANDING = "https://example.test/manual/diagnosis.html"
FULL_PROCEDURE_LANDING = "https://example.test/manual/procedure.html"
FULL_HTML_BY_URL = {
    FULL_CMS_URL: (
        '<h2>Final Rule</h2><a href="/manual/P0001.html">'
        "Definitions Manual Table of Contents</a>"
    ),
    FULL_TOC_URL: (
        "Version 44.2 "
        '<a href="appendix.html">Appendix A List of MS-DRGs</a>'
        '<a href="diagnosis.html">Diagnosis Code/MDC/MS-DRG Index</a>'
        '<a href="procedure.html">Procedure Code/MS-DRG Index</a>'
    ),
    "https://example.test/manual/appendix.html": (
        '<a href="list.html">List of MS-DRGs</a>'
    ),
    "https://example.test/manual/list.html": MS_DRG_LIST_HTML,
    FULL_DIAGNOSIS_LANDING: (
        '<a href="diagnosis/P0100.html">Diagnosis Code/MDC/MS-DRG Index</a>'
    ),
    "https://example.test/manual/diagnosis/P0100.html": DIAGNOSIS_HTML,
    FULL_PROCEDURE_LANDING: (
        '<a href="procedure/P0200.html">Procedure Code/MS-DRG Index</a>'
    ),
    "https://example.test/manual/procedure/P0200.html": PROCEDURE_HTML,
}
SECOND_PAGE_BY_URL = {
    "https://example.test/manual/diagnosis/P0101.html": (
        "<table><tr><td>A999</td><td>01</td><td>999</td></tr></table>"
    ),
    "https://example.test/manual/procedure/P0201.html": (
        "<table><tr><td>0ZZZZZZ</td><td>01</td><td>999</td>"
        "<td>Filtered procedure</td></tr></table>"
    ),
}


class _FakeStage:
    __tablename__ = "stage"
    __table__ = object()


class _RecordingDb:
    def __init__(self):
        self.queries = []
        self.created_tables = []
        self.disconnected = False

    async def status(self, query):
        self.queries.append(" ".join(query.split()))

    async def create_table(self, table, **options):
        self.created_tables.append((table, options))

    async def disconnect(self):
        self.disconnected = True

    @asynccontextmanager
    async def transaction(self):
        yield


def _manual_source(toc_html="<p>toc</p>", catalog_rows=None):
    return ms_drg.MsDrgManualSource(
        cms_page_url="https://example.test/cms",
        toc_url="https://example.test/toc.html",
        toc_html=toc_html,
        list_url="https://example.test/list.html",
        release="v44.2",
        catalog_rows=catalog_rows
        or [
            ms_drg.MsDrgCatalogRow(
                code="001",
                mdc="MDC 01",
                designation="P",
                title="Synthetic surgical group",
            )
        ],
    )


def _request(**overrides):
    request_options_map = {
        "test_mode": False,
        "include_relationships": True,
        "relationship_page_limit": None,
        "concurrency": 2,
        "cms_page_url": "https://example.test/cms",
        "manual_toc_url": "https://example.test/toc.html",
        "import_suffix": "unit",
        "run_id": None,
    }
    request_options_map.update(overrides)
    return ms_drg.MsDrgImportRequest(**request_options_map)


def test_request_normalizes_limits_concurrency_and_environment(monkeypatch):
    """Test mode supplies a page cap and deployment values supply source defaults."""
    monkeypatch.setenv("HLTHPRT_MS_DRG_CONCURRENCY", "0")
    monkeypatch.setenv("HLTHPRT_MS_DRG_CMS_PAGE_URL", "https://example.test/env")
    monkeypatch.setenv("HLTHPRT_MS_DRG_IMPORT_ID", " release-44! ")

    request = ms_drg._build_request(
        True,
        True,
        None,
        None,
        None,
        None,
        None,
        "run-7",
    )

    assert request.relationship_page_limit == ms_drg.TEST_INDEX_PAGE_LIMIT
    assert request.concurrency == 1
    assert request.cms_page_url == "https://example.test/env"
    assert request.import_suffix == "release44"
    assert request.run_id == "run-7"


@pytest.mark.asyncio
async def test_manual_source_discovery_and_validation(monkeypatch):
    """CMS discovery resolves the final manual, appendix, list, and smoke catalog."""
    cms_url = "https://example.test/cms"
    toc_url = "https://example.test/manual/P0001.html"
    html_by_url = {
        cms_url: (
            '<h2>Final Rule</h2><a href="/manual/P0001.html">'
            "Definitions Manual Table of Contents</a>"
        ),
        toc_url: (
            "Version 44.2 "
            '<a href="appendix.html">Appendix A List of MS-DRGs</a>'
        ),
        "https://example.test/manual/appendix.html": (
            '<a href="list.html">List of MS-DRGs</a>'
        ),
        "https://example.test/manual/list.html": MS_DRG_LIST_HTML,
    }
    monkeypatch.setattr(ms_drg, "_download_text", html_by_url.__getitem__)
    manual_source = await ms_drg._load_manual_source(
        _request(
            test_mode=True,
            cms_page_url=cms_url,
            manual_toc_url=None,
        )
    )

    assert manual_source.toc_url == toc_url
    assert manual_source.release == "v44.2"
    assert [catalog_record.code for catalog_record in manual_source.catalog_rows] == [
        "001",
        "470",
    ]

    monkeypatch.setattr(ms_drg, "_download_text", lambda _url: "<p>no appendix</p>")
    with pytest.raises(RuntimeError, match="Appendix A"):
        await ms_drg._load_manual_source(_request())


@pytest.mark.asyncio
async def test_manual_source_rejects_empty_catalog(monkeypatch):
    """A located list that parses to zero rows fails with the list URL for context."""
    html_by_url = {
        "https://example.test/toc.html": (
            '<a href="appendix.html">Appendix A List of MS-DRGs</a>'
        ),
        "https://example.test/appendix.html": (
            '<a href="list.html">List of MS-DRGs</a>'
        ),
        "https://example.test/list.html": "<table></table>",
    }
    monkeypatch.setattr(ms_drg, "_download_text", html_by_url.__getitem__)

    with pytest.raises(RuntimeError, match="list produced no rows"):
        await ms_drg._load_manual_source(_request())


def test_relationship_link_errors_name_every_missing_index():
    """Relationship discovery identifies one or both absent CMS indexes."""
    with pytest.raises(RuntimeError) as both_missing:
        ms_drg._relationship_landing_urls(_manual_source())
    assert "diagnosis" in str(both_missing.value)
    assert "procedure" in str(both_missing.value)

    diagnosis_only_html = (
        '<a href="diagnosis.html">Diagnosis Code/MDC/MS-DRG Index</a>'
    )
    with pytest.raises(RuntimeError) as procedure_missing:
        ms_drg._relationship_landing_urls(
            _manual_source(toc_html=diagnosis_only_html)
        )
    assert "procedure" in str(procedure_missing.value)


def _install_full_import_stubs(monkeypatch):
    recording_db = _RecordingDb()
    merge_calls = []

    async def no_operation(*_args, **_kwargs):
        return None

    async def download_many(page_urls, _concurrency):
        return [(page_url, SECOND_PAGE_BY_URL[page_url]) for page_url in page_urls]

    async def push_rows(_stage, row_maps):
        return len(row_maps)

    async def merge_catalog(_stage, _schema, source_names):
        merge_calls.append(("catalog", source_names))

    async def merge_synonyms(_stage, _schema, source_names):
        merge_calls.append(("synonym", source_names))

    async def merge_relationships(_stage, _schema, source_names):
        merge_calls.append(("relationship", source_names))

    monkeypatch.setattr(ms_drg, "db", recording_db)
    monkeypatch.setattr(ms_drg, "ensure_database", no_operation)
    monkeypatch.setattr(ms_drg, "_ensure_tables", no_operation)
    monkeypatch.setattr(ms_drg, "_download_text", FULL_HTML_BY_URL.__getitem__)
    monkeypatch.setattr(ms_drg, "_download_many", download_many)
    monkeypatch.setattr(ms_drg, "make_class", lambda *_args: _FakeStage)
    monkeypatch.setattr(ms_drg, "_push", push_rows)
    monkeypatch.setattr(ms_drg, "_merge_catalog_stage", merge_catalog)
    monkeypatch.setattr(ms_drg, "_merge_synonym_stage", merge_synonyms)
    monkeypatch.setattr(ms_drg, "_merge_relationship_stage", merge_relationships)
    return merge_calls


@pytest.mark.asyncio
async def test_full_import_publishes_filtered_relationships(monkeypatch):
    """A synthetic full flow downloads both indexes and publishes only smoke DRGs."""
    merge_calls = _install_full_import_stubs(monkeypatch)

    summary_map = await ms_drg.import_ms_drg(
        test_mode=True,
        source_url=FULL_CMS_URL,
        relationship_page_limit=2,
        import_id="unit",
    )

    assert summary_map["relationship_rows"] == 4
    assert summary_map["icd10cm_codes_observed"] == 2
    assert summary_map["icd10pcs_rows"] == 1
    assert summary_map["diagnosis_index_pages"] == 2
    assert summary_map["procedure_index_pages"] == 2
    assert merge_calls == [
        ("catalog", ms_drg.SOURCES),
        ("synonym", (ms_drg.SOURCE_MS_DRG,)),
        (
            "relationship",
            (ms_drg.SOURCE_ICD10CM_INDEX, ms_drg.SOURCE_ICD10PCS_INDEX),
        ),
    ]


@pytest.mark.asyncio
async def test_relationship_loader_rejects_empty_indexes(monkeypatch):
    """Relationship-enabled imports cannot silently publish empty index data."""
    toc_html = (
        '<a href="diagnosis.html">Diagnosis Code/MDC/MS-DRG Index</a>'
        '<a href="procedure.html">Procedure Code/MS-DRG Index</a>'
    )

    async def empty_pages(*_args):
        return ["<table></table>"]

    monkeypatch.setattr(ms_drg, "_download_index_pages", empty_pages)
    with pytest.raises(RuntimeError, match="produced no relationships"):
        await ms_drg._load_relationship_rows(
            _manual_source(toc_html=toc_html),
            _request(),
        )

    catalog_only_rows = await ms_drg._load_relationship_rows(
        _manual_source(),
        _request(include_relationships=False),
    )
    assert catalog_only_rows.relationships == set()


def test_payload_builder_assigns_relationship_sources():
    """ICD-10-CM and ICD-10-PCS relationships retain distinct provenance."""
    relationship_rows = ms_drg.MsDrgRelationshipRows(
        relationships={
            ("MS_DRG", "001", "uses_icd10cm", "ICD10CM", "A001"),
            ("MS_DRG", "001", "uses_icd10pcs", "ICD10PCS", "0ABCDEF"),
        },
        procedure_category_by_code={"0ABCDEF": "Synthetic procedure"},
    )

    import_payloads = ms_drg._build_payloads(
        _manual_source(),
        relationship_rows,
    )

    assert len(import_payloads.catalog_payloads) == 2
    assert len(import_payloads.synonym_payloads) == 3
    assert {
        relationship_map["source"]
        for relationship_map in import_payloads.relationship_payloads
    } == {
        ms_drg.SOURCE_ICD10CM_INDEX,
        ms_drg.SOURCE_ICD10PCS_INDEX,
    }


@pytest.mark.asyncio
async def test_publication_helpers_batch_and_merge_owned_rows(monkeypatch):
    """Publication helpers create tables, batch rows, and scope every source replacement."""
    recording_db = _RecordingDb()
    pushed_chunks = []

    async def push_objects(row_chunk, stage_class):
        pushed_chunks.append((stage_class.__tablename__, list(row_chunk)))

    monkeypatch.setattr(publication, "db", recording_db)
    monkeypatch.setattr(publication, "push_objects", push_objects)
    monkeypatch.setattr(publication, "BATCH_SIZE", 1)

    await publication._ensure_tables("unit")
    pushed_count = await publication._push(
        _FakeStage,
        [{"code": "1"}, {"code": "2"}],
    )
    await publication._merge_catalog_stage(
        _FakeStage,
        "unit",
        (publication.SOURCE_MS_DRG,),
    )
    await publication._merge_synonym_stage(
        _FakeStage,
        "unit",
        (publication.SOURCE_MS_DRG,),
    )
    await publication._merge_relationship_stage(
        _FakeStage,
        "unit",
        (
            publication.SOURCE_ICD10CM_INDEX,
            publication.SOURCE_ICD10PCS_INDEX,
        ),
    )

    assert pushed_count == 2
    assert [len(row_chunk) for _stage, row_chunk in pushed_chunks] == [1, 1]
    assert len(recording_db.created_tables) == 3
    sql_text = "\n".join(recording_db.queries)
    assert "DELETE FROM unit.code_catalog" in sql_text
    assert "DELETE FROM unit.code_synonym" in sql_text
    assert "DELETE FROM unit.code_relationship" in sql_text


def test_catalog_rows_cover_designation_and_description_contracts():
    """Catalog payloads describe medical, surgical, and unspecified designations."""
    catalog_rows = [
        ms_drg.MsDrgCatalogRow("001", "MDC 01", "P", "Surgical"),
        ms_drg.MsDrgCatalogRow("002", None, "M", "Medical"),
        ms_drg.MsDrgCatalogRow("003", None, None, "Unspecified"),
    ]

    catalog_payloads, synonym_payloads = publication._build_catalog_and_synonym_rows(
        catalog_rows,
        {},
        "v44",
    )

    assert "surgical designation" in catalog_payloads[0]["long_description"]
    assert "medical designation" in catalog_payloads[1]["long_description"]
    assert "unspecified designation" in catalog_payloads[2]["long_description"]
    assert len(synonym_payloads) == 9


@pytest.mark.asyncio
async def test_entrypoint_disconnects_after_import_failure(monkeypatch):
    """The worker entry point releases its database connection on every exit path."""
    lifecycle_events = []

    class LifecycleDb:
        async def disconnect(self):
            lifecycle_events.append("disconnect")

    async def initialize(_db):
        lifecycle_events.append("init")

    async def fail_import(**_options):
        raise RuntimeError("synthetic import failure")

    monkeypatch.setattr(ms_drg, "db", LifecycleDb())
    monkeypatch.setattr(ms_drg, "init_db", initialize)
    monkeypatch.setattr(ms_drg, "import_ms_drg", fail_import)

    with pytest.raises(RuntimeError, match="synthetic import failure"):
        await ms_drg.main()
    assert lifecycle_events == ["init", "disconnect"]
