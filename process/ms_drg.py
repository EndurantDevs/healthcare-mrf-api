# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""MS-DRG import orchestration over CMS source and publication contracts."""

from __future__ import annotations

import asyncio
import datetime
import os
from typing import Any, Callable

from db.connection import init_db
from db.models import CodeCatalog, CodeRelationship, CodeSynonym, db
from process.ext.utils import ensure_database, make_class
from process.ms_drg_contracts import (
    RelationshipTuple,
    MsDrgImportRequest,
    MsDrgManualSource,
    MsDrgPayloads,
    MsDrgPublishCounts,
    MsDrgRelationshipRows,
)
from process.ms_drg_publication import (
    BATCH_SIZE,
    CMS_MS_DRG_ATTRIBUTION,
    SOURCE_ICD10CM_INDEX,
    SOURCE_ICD10PCS_INDEX,
    SOURCE_MS_DRG,
    SOURCES,
    _build_catalog_and_synonym_rows,
    _catalog_row,
    _ensure_tables,
    _merge_catalog_stage,
    _merge_relationship_stage,
    _merge_synonym_stage,
    _push,
    _relationship_row,
    _source_sql_list,
    _synonym_row,
)
from process.ms_drg_sources import (
    DEFAULT_CMS_MS_DRG_PAGE_URL,
    DEFAULT_MANUAL_TOC_URL,
    MS_DRG_DEFAULT_MAX_BYTES,
    MsDrgCatalogRow,
    _TableParser,
    _clean_text,
    _discover_sequential_index_urls,
    _download_many,
    _download_text,
    _expand_ms_drg_values,
    _extract_links,
    _extract_release,
    _find_latest_manual_toc_url,
    _find_link,
    _is_cancel_requested,
    _parse_diagnosis_index_relationships,
    _parse_ms_drg_catalog_rows,
    _parse_procedure_index_relationships,
    _parse_tables,
    _raise_if_cancelled,
)
from process.reference_stage import _drop_stage_tables, build_reference_stage_suffix

DEFAULT_CONCURRENCY = 10
TEST_INDEX_PAGE_LIMIT = 2

def _schema() -> str:
    return os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"


def _now() -> datetime.datetime:
    return datetime.datetime.utcnow()


def _normalize_import_id(raw_import_id: str | None) -> str:
    configured_import_id = (
        raw_import_id or os.getenv("HLTHPRT_MS_DRG_IMPORT_ID") or ""
    )
    cleaned_import_id = "".join(
        character
        for character in str(configured_import_id)
        if character.isalnum()
    )
    if cleaned_import_id:
        return cleaned_import_id[:32]
    return _now().strftime("%Y%m%d")


def _ms_drg_stage_suffix(import_suffix: str, run_id: str | None = None) -> str:
    return build_reference_stage_suffix("drg", import_suffix, run_id)


def _build_request(
    test_mode: bool,
    include_relationships: bool,
    relationship_page_limit: int | None,
    concurrency: int | None,
    source_url: str | None,
    manual_toc_url: str | None,
    import_id: str | None,
    run_id: str | None,
) -> MsDrgImportRequest:
    page_limit = relationship_page_limit
    if test_mode and page_limit is None:
        page_limit = TEST_INDEX_PAGE_LIMIT
    configured_concurrency = concurrency or os.getenv(
        "HLTHPRT_MS_DRG_CONCURRENCY",
        DEFAULT_CONCURRENCY,
    )
    return MsDrgImportRequest(
        test_mode=test_mode,
        include_relationships=include_relationships,
        relationship_page_limit=page_limit,
        concurrency=max(int(configured_concurrency), 1),
        cms_page_url=source_url
        or os.getenv("HLTHPRT_MS_DRG_CMS_PAGE_URL")
        or DEFAULT_CMS_MS_DRG_PAGE_URL,
        manual_toc_url=manual_toc_url,
        import_suffix=_normalize_import_id(import_id),
        run_id=run_id,
    )


async def _resolve_manual_toc(
    request: MsDrgImportRequest,
) -> tuple[str, str]:
    if request.manual_toc_url:
        return request.manual_toc_url, ""
    cms_page_html = await asyncio.to_thread(_download_text, request.cms_page_url)
    toc_url = (
        os.getenv("HLTHPRT_MS_DRG_MANUAL_TOC_URL")
        or _find_latest_manual_toc_url(cms_page_html, request.cms_page_url)
        or DEFAULT_MANUAL_TOC_URL
    )
    return toc_url, cms_page_html


async def _load_manual_source(
    request: MsDrgImportRequest,
) -> MsDrgManualSource:
    toc_url, _cms_page_html = await _resolve_manual_toc(request)
    _raise_if_cancelled(request.run_id)
    toc_html = await asyncio.to_thread(_download_text, toc_url)
    release = _extract_release(toc_html, toc_url)
    appendix_url = _find_link(
        toc_html,
        r"appendix\s+a\s+list\s+of\s+ms-drgs",
        toc_url,
    )
    if not appendix_url:
        raise RuntimeError(f"Could not find MS-DRG Appendix A link in {toc_url}")
    appendix_html = await asyncio.to_thread(_download_text, appendix_url)
    list_url = (
        _find_link(appendix_html, r"list\s+of\s+ms-drgs", appendix_url)
        or appendix_url
    )
    list_html = await asyncio.to_thread(_download_text, list_url)
    catalog_rows = _parse_ms_drg_catalog_rows(list_html)
    if request.test_mode:
        smoke_codes = {"001", "031", "097", "371", "470", "714", "791", "820"}
        catalog_rows = [
            catalog_record
            for catalog_record in catalog_rows
            if catalog_record.code in smoke_codes
        ] or catalog_rows[:20]
    if not catalog_rows:
        raise RuntimeError(f"CMS MS-DRG list produced no rows: {list_url}")
    return MsDrgManualSource(
        cms_page_url=request.cms_page_url,
        toc_url=toc_url,
        toc_html=toc_html,
        list_url=list_url,
        release=release,
        catalog_rows=catalog_rows,
    )


def _relationship_landing_urls(
    manual_source: MsDrgManualSource,
) -> tuple[str, str]:
    diagnosis_landing = _find_link(
        manual_source.toc_html,
        r"diagnosis\s+code/mdc/ms-drg\s+index",
        manual_source.toc_url,
    )
    procedure_landing = _find_link(
        manual_source.toc_html,
        r"procedure\s+code/ms-drg\s+index",
        manual_source.toc_url,
    )
    missing_link_names: list[str] = []
    if not diagnosis_landing:
        missing_link_names.append("diagnosis code/MDC/MS-DRG index")
    if not procedure_landing:
        missing_link_names.append("procedure code/MS-DRG index")
    if missing_link_names:
        raise RuntimeError(
            "Could not find CMS MS-DRG relationship index link(s): "
            f"{', '.join(missing_link_names)}"
        )
    return diagnosis_landing, procedure_landing


async def _download_index_pages(
    landing_url: str,
    link_pattern: str,
    request: MsDrgImportRequest,
) -> list[str]:
    landing_html = await asyncio.to_thread(_download_text, landing_url)
    first_url = _find_link(landing_html, link_pattern, landing_url) or landing_url
    first_html = await asyncio.to_thread(_download_text, first_url)
    page_urls = _discover_sequential_index_urls(
        first_html,
        first_url,
        request.relationship_page_limit,
    )
    page_payloads = [(first_url, first_html)]
    page_payloads.extend(
        await _download_many(page_urls[1:], request.concurrency)
    )
    return [page_html for _page_url, page_html in page_payloads]


def _collect_index_rows(
    page_html_values: list[str],
    parser: Callable[[str], tuple[set[RelationshipTuple], Any]],
    run_id: str | None,
) -> tuple[set[RelationshipTuple], Any]:
    collected_relationships: set[RelationshipTuple] = set()
    collected_codes: Any = None
    for page_html in page_html_values:
        page_relationships, page_codes = parser(page_html)
        collected_relationships.update(page_relationships)
        if collected_codes is None:
            collected_codes = page_codes.copy()
        else:
            collected_codes.update(page_codes)
        _raise_if_cancelled(run_id)
    return collected_relationships, collected_codes


async def _load_relationship_rows(
    manual_source: MsDrgManualSource,
    request: MsDrgImportRequest,
) -> MsDrgRelationshipRows:
    if not request.include_relationships:
        return MsDrgRelationshipRows()
    diagnosis_landing, procedure_landing = _relationship_landing_urls(manual_source)
    diagnosis_pages = await _download_index_pages(
        diagnosis_landing,
        r"diagnosis\s+code/mdc/ms-drg\s+index",
        request,
    )
    diagnosis_relationships, diagnosis_codes = _collect_index_rows(
        diagnosis_pages,
        _parse_diagnosis_index_relationships,
        request.run_id,
    )
    procedure_pages = await _download_index_pages(
        procedure_landing,
        r"procedure\s+code/ms-drg\s+index",
        request,
    )
    procedure_relationships, procedure_category_by_code = _collect_index_rows(
        procedure_pages,
        _parse_procedure_index_relationships,
        request.run_id,
    )
    relationship_rows = MsDrgRelationshipRows(
        relationships=diagnosis_relationships | procedure_relationships,
        procedure_category_by_code=procedure_category_by_code,
        diagnosis_codes=diagnosis_codes,
        diagnosis_page_count=len(diagnosis_pages),
        procedure_page_count=len(procedure_pages),
    )
    _filter_test_relationships(relationship_rows, manual_source, request.test_mode)
    if not relationship_rows.relationships:
        raise RuntimeError("CMS MS-DRG relationship indexes produced no relationships")
    return relationship_rows


def _filter_test_relationships(
    relationship_rows: MsDrgRelationshipRows,
    manual_source: MsDrgManualSource,
    test_mode: bool,
) -> None:
    if not test_mode or not relationship_rows.relationships:
        return
    allowed_ms_drg_codes = {
        catalog_record.code for catalog_record in manual_source.catalog_rows
    }
    relationship_rows.relationships = {
        relationship_tuple
        for relationship_tuple in relationship_rows.relationships
        if (
            relationship_tuple[0] == "MS_DRG"
            and relationship_tuple[1] in allowed_ms_drg_codes
        )
        or (
            relationship_tuple[3] == "MS_DRG"
            and relationship_tuple[4] in allowed_ms_drg_codes
        )
    }
    relationship_rows.procedure_category_by_code = {
        procedure_code: procedure_category
        for procedure_code, procedure_category in (
            relationship_rows.procedure_category_by_code.items()
        )
        if any(
            (
                relationship_tuple[3] == "ICD10PCS"
                and relationship_tuple[4] == procedure_code
            )
            or (
                relationship_tuple[0] == "ICD10PCS"
                and relationship_tuple[1] == procedure_code
            )
            for relationship_tuple in relationship_rows.relationships
        )
    }


def _build_payloads(
    manual_source: MsDrgManualSource,
    relationship_rows: MsDrgRelationshipRows,
) -> MsDrgPayloads:
    catalog_payloads, synonym_payloads = _build_catalog_and_synonym_rows(
        manual_source.catalog_rows,
        relationship_rows.procedure_category_by_code,
        manual_source.release,
    )
    relationship_payloads = [
        _relationship_row(
            *relationship_tuple,
            SOURCE_ICD10PCS_INDEX
            if "ICD10PCS" in {relationship_tuple[0], relationship_tuple[3]}
            else SOURCE_ICD10CM_INDEX,
        )
        for relationship_tuple in sorted(relationship_rows.relationships)
    ]
    return MsDrgPayloads(
        catalog_payloads,
        synonym_payloads,
        relationship_payloads,
    )


async def _stage_and_publish(
    schema: str,
    request: MsDrgImportRequest,
    import_payloads: MsDrgPayloads,
) -> MsDrgPublishCounts:
    stage_suffix = _ms_drg_stage_suffix(request.import_suffix, request.run_id)
    stage_by_model = {
        CodeCatalog: make_class(CodeCatalog, stage_suffix),
        CodeSynonym: make_class(CodeSynonym, stage_suffix),
        CodeRelationship: make_class(CodeRelationship, stage_suffix),
    }
    await _drop_stage_tables(db, schema, stage_by_model.values())
    try:
        for stage_class in stage_by_model.values():
            await db.create_table(stage_class.__table__, checkfirst=True)
        catalog_count = await _push(
            stage_by_model[CodeCatalog],
            import_payloads.catalog_payloads,
        )
        synonym_count = await _push(
            stage_by_model[CodeSynonym],
            import_payloads.synonym_payloads,
        )
        relationship_count = 0
        if request.include_relationships:
            relationship_count = await _push(
                stage_by_model[CodeRelationship],
                import_payloads.relationship_payloads,
            )
        catalog_sources = SOURCES if request.include_relationships else (SOURCE_MS_DRG,)
        _raise_if_cancelled(request.run_id)
        async with db.transaction():
            await _merge_catalog_stage(
                stage_by_model[CodeCatalog],
                schema,
                catalog_sources,
            )
            await _merge_synonym_stage(
                stage_by_model[CodeSynonym],
                schema,
                (SOURCE_MS_DRG,),
            )
            if request.include_relationships:
                await _merge_relationship_stage(
                    stage_by_model[CodeRelationship],
                    schema,
                    (SOURCE_ICD10CM_INDEX, SOURCE_ICD10PCS_INDEX),
                )
        return MsDrgPublishCounts(
            catalog_count,
            synonym_count,
            relationship_count,
        )
    finally:
        await _drop_stage_tables(db, schema, stage_by_model.values())


def _build_summary(
    request: MsDrgImportRequest,
    manual_source: MsDrgManualSource,
    relationship_rows: MsDrgRelationshipRows,
    publish_counts: MsDrgPublishCounts,
) -> dict[str, Any]:
    return {
        "source_url": manual_source.cms_page_url,
        "manual_toc_url": manual_source.toc_url,
        "ms_drg_list_url": manual_source.list_url,
        "source_release": manual_source.release,
        "catalog_rows": publish_counts.catalog_count,
        "ms_drg_rows": len(manual_source.catalog_rows),
        "synonym_rows": publish_counts.synonym_count,
        "relationship_rows": publish_counts.relationship_count,
        "icd10cm_codes_observed": len(relationship_rows.diagnosis_codes),
        "icd10pcs_rows": len(relationship_rows.procedure_category_by_code),
        "diagnosis_index_pages": relationship_rows.diagnosis_page_count,
        "procedure_index_pages": relationship_rows.procedure_page_count,
        "include_relationships": request.include_relationships,
    }


async def import_ms_drg(
    *,
    test_mode: bool = False,
    include_relationships: bool = True,
    relationship_page_limit: int | None = None,
    concurrency: int | None = None,
    source_url: str | None = None,
    manual_toc_url: str | None = None,
    import_id: str | None = None,
    run_id: str | None = None,
) -> dict[str, Any]:
    """Download, normalize, and persist MS-DRG reference data."""
    await ensure_database(test_mode)
    schema = _schema()
    await _ensure_tables(schema)
    request = _build_request(
        test_mode,
        include_relationships,
        relationship_page_limit,
        concurrency,
        source_url,
        manual_toc_url,
        import_id,
        run_id,
    )
    manual_source = await _load_manual_source(request)
    relationship_rows = await _load_relationship_rows(manual_source, request)
    import_payloads = _build_payloads(manual_source, relationship_rows)
    publish_counts = await _stage_and_publish(schema, request, import_payloads)
    summary_map = _build_summary(
        request,
        manual_source,
        relationship_rows,
        publish_counts,
    )
    print(
        "MS-DRG import done: "
        f"MS_DRG={len(manual_source.catalog_rows):,} "
        f"catalog={publish_counts.catalog_count:,} "
        f"synonyms={publish_counts.synonym_count:,} "
        f"relationships={publish_counts.relationship_count:,} "
        f"ICD10PCS={len(relationship_rows.procedure_category_by_code):,} "
        f"release={manual_source.release} at {_now().isoformat()}Z"
    )
    return summary_map


async def main(
    test_mode: bool = False,
    include_relationships: bool = True,
    relationship_page_limit: int | None = None,
    concurrency: int | None = None,
    source_url: str | None = None,
    manual_toc_url: str | None = None,
    import_id: str | None = None,
    run_id: str | None = None,
) -> dict[str, Any]:
    """Run the MS-DRG import entry point."""
    await init_db(db)
    try:
        return await import_ms_drg(
            test_mode=test_mode,
            include_relationships=include_relationships,
            relationship_page_limit=relationship_page_limit,
            concurrency=concurrency,
            source_url=source_url,
            manual_toc_url=manual_toc_url,
            import_id=import_id,
            run_id=run_id,
        )
    finally:
        await db.disconnect()
