# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Normalized row and staged publication contracts for MS-DRG imports."""

from __future__ import annotations

import datetime
from typing import Any

from db.models import CodeCatalog, CodeRelationship, CodeSynonym, db
from process.ms_drg_sources import DEFAULT_CMS_MS_DRG_PAGE_URL, MsDrgCatalogRow

SOURCE_MS_DRG = "cms_ms_drg_definitions_manual"
SOURCE_ICD10CM_INDEX = "cms_ms_drg_icd10cm_index"
SOURCE_ICD10PCS_INDEX = "cms_ms_drg_icd10pcs_index"
SOURCES = (SOURCE_MS_DRG, SOURCE_ICD10CM_INDEX, SOURCE_ICD10PCS_INDEX)

CMS_MS_DRG_ATTRIBUTION = (
    "Centers for Medicare & Medicaid Services (CMS), MS-DRG Classifications and Software, "
    f"{DEFAULT_CMS_MS_DRG_PAGE_URL}"
)

BATCH_SIZE = 5000


def _now() -> datetime.datetime:
    return datetime.datetime.utcnow()


def _catalog_row(
    *,
    code_system: str,
    code: str,
    code_type: str,
    display_name: str,
    short_description: str | None,
    long_description: str | None,
    source: str,
    source_release: str,
) -> dict[str, Any]:
    return {
        "code_system": code_system,
        "code": code,
        "code_type": code_type,
        "display_name": display_name,
        "short_description": short_description,
        "long_description": long_description,
        "is_active": True,
        "source": source,
        "source_release": source_release,
        "source_attribution": CMS_MS_DRG_ATTRIBUTION,
        "updated_at": _now(),
    }


def _synonym_row(code: str, synonym: str, term_type: str) -> dict[str, Any]:
    return {
        "code_system": "MS_DRG",
        "code": code,
        "synonym": synonym,
        "term_type": term_type,
        "language": "ENG",
        "source": SOURCE_MS_DRG,
        "source_attribution": CMS_MS_DRG_ATTRIBUTION,
        "updated_at": _now(),
    }


def _relationship_row(
    from_system: str,
    from_code: str,
    relationship: str,
    to_system: str,
    to_code: str,
    source: str,
) -> dict[str, Any]:
    return {
        "from_system": from_system,
        "from_code": from_code,
        "relationship": relationship,
        "to_system": to_system,
        "to_code": to_code,
        "source": source,
        "source_attribution": CMS_MS_DRG_ATTRIBUTION,
        "updated_at": _now(),
    }


def _build_catalog_and_synonym_rows(
    catalog_rows: list[MsDrgCatalogRow],
    procedure_category_by_code: dict[str, str],
    release: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    catalog_payloads: list[dict[str, Any]] = []
    synonym_payloads: list[dict[str, Any]] = []
    for catalog_record in catalog_rows:
        designation = {
            "M": "medical",
            "P": "surgical",
        }.get(
            catalog_record.designation or "",
            catalog_record.designation or "unspecified",
        )
        description_parts = [
            f"MS-DRG {catalog_record.code}",
            f"{designation} designation",
        ]
        if catalog_record.mdc:
            description_parts.append(catalog_record.mdc)
        catalog_payloads.append(
            _catalog_row(
                code_system="MS_DRG",
                code=catalog_record.code,
                code_type="inpatient_case_group",
                display_name=catalog_record.title,
                short_description=catalog_record.title,
                long_description="; ".join(description_parts),
                source=SOURCE_MS_DRG,
                source_release=release,
            )
        )
        synonym_payloads.extend(
            (
                _synonym_row(catalog_record.code, f"MS-DRG {catalog_record.code}", "alias"),
                _synonym_row(catalog_record.code, f"DRG {catalog_record.code}", "alias"),
                _synonym_row(catalog_record.code, catalog_record.title, "preferred"),
            )
        )

    for procedure_code, procedure_category in sorted(procedure_category_by_code.items()):
        catalog_payloads.append(
            _catalog_row(
                code_system="ICD10PCS",
                code=procedure_code,
                code_type="procedure",
                display_name=f"ICD-10-PCS {procedure_code}",
                short_description=procedure_category or None,
                long_description=procedure_category or None,
                source=SOURCE_ICD10PCS_INDEX,
                source_release=release,
            )
        )
    return catalog_payloads, synonym_payloads


async def _ensure_tables(schema: str) -> None:
    await db.create_table(CodeCatalog.__table__, checkfirst=True)
    await db.create_table(CodeSynonym.__table__, checkfirst=True)
    await db.create_table(CodeRelationship.__table__, checkfirst=True)
