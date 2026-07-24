# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Clinical-reference import orchestration and staged publication contracts."""

from __future__ import annotations

import datetime
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from db.connection import init_db
from db.models import db
from process.clinical_reference_parsers import (
    _mesh_text,
    _parse_icd10cm,
    _parse_mesh_file,
)
from process.clinical_reference_rows import (
    NLM_ATTRIBUTION,
    SNOMED_FSN_TYPE_ID,
    SNOMED_SYNONYM_TYPE_ID,
    _area_condition_row,
    _area_row,
    _area_treatment_row,
    _build_clinical_area_rows,
    _code_type_for_mesh,
    _code_type_for_snomed,
    _concept_row,
    _crosswalk_row,
    _mesh_clinical_area_root,
    _relationship_row,
    _synonym_row,
)
from process.clinical_reference_umls_parsers import (
    _parse_rxnorm,
    _parse_snomed,
    _parse_snomed_icd_map,
)
from process.clinical_reference_sources import (
    DEFAULT_CLINICAL_REFERENCE_SOURCES,
    RESTRICTED_SOURCE_ALIASES,
    _download_url,
    _is_cancel_requested,
    _is_restricted_terminology_enabled,
    _load_medrt_from_rxclass,
    _load_product_rxcuis,
    _raise_if_cancelled,
    _redact_sensitive_url,
    _release_current,
    _rxclass_for_rxcui,
    _selected_sources,
    _sha256_file,
    _umls_download_url,
)
from process.clinical_reference_publication import (
    CLINICAL_REFERENCE_SOURCES,
    DEFAULT_BATCH_SIZE,
    ClinicalAreaRows,
    ClinicalStageModels,
    _batch,
    _catalog_stage_count,
    _create_stage_indexes,
    _create_stage_models,
    _ensure_schema_exists,
    _ensure_unified_code_tables,
    _merge_code_catalog_stage,
    _merge_code_crosswalk_stage,
    _merge_replace_source_table,
    _publish_reference_stages,
    _publish_table,
    _push,
    _source_sql_list,
    _stage_index_name,
    _stage_reference_rows,
)
from process.control_cancel import ImportCancelledError
from process.ext.utils import ensure_database

CDC_ICD10CM_URL = (
    "https://ftp.cdc.gov/pub/Health_Statistics/NCHS/Publications/ICD10CM/2026/"
    "icd10cm-Code%20Descriptions-2026.zip"
)
MESH_DESC_URL = "https://nlmpubs.nlm.nih.gov/projects/mesh/MESH_FILES/xmlmesh/desc2026.gz"
MESH_SUPP_URL = "https://nlmpubs.nlm.nih.gov/projects/mesh/MESH_FILES/xmlmesh/supp2026.gz"

DEFAULT_ARTIFACT_ROOT = Path("/Volumes/Data/data/artifacts/terminology")


@dataclass(frozen=True)
class ClinicalReferenceRequest:
    test_mode: bool
    import_suffix: str
    artifact_root: Path
    selected_source_names: set[str]
    umls_key: str | None
    source_test_limit: int | None
    force_download: bool
    run_id: str | None


@dataclass
class ClinicalReferenceRows:
    concept_rows: list[dict[str, Any]] = field(default_factory=list)
    synonym_rows: list[dict[str, Any]] = field(default_factory=list)
    crosswalk_rows: list[dict[str, Any]] = field(default_factory=list)
    relationship_rows: list[dict[str, Any]] = field(default_factory=list)
    source_count_by_name: dict[str, int] = field(default_factory=dict)

    def merge(self, source_rows: ClinicalReferenceRows) -> None:
        """Merge one normalized source bundle into the import collection."""
        self.concept_rows.extend(source_rows.concept_rows)
        self.synonym_rows.extend(source_rows.synonym_rows)
        self.crosswalk_rows.extend(source_rows.crosswalk_rows)
        self.relationship_rows.extend(source_rows.relationship_rows)
        self.source_count_by_name.update(source_rows.source_count_by_name)


@dataclass(frozen=True)
class ClinicalReferenceIndexes:
    concepts_by_identity: dict[tuple[str, str], dict[str, Any]]
    synonyms_by_identity: dict[tuple[str, str, str, str], dict[str, Any]]
    crosswalks_by_identity: dict[tuple[str, str, str, str], dict[str, Any]]
    relationships_by_identity: dict[tuple[str, str, str, str, str], dict[str, Any]]


def _schema() -> str:
    return os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"


def _artifact_root(override: str | None = None) -> Path:
    configured_root = override or os.getenv("HLTHPRT_TERMINOLOGY_ARTIFACT_ROOT")
    return Path(configured_root).expanduser() if configured_root else DEFAULT_ARTIFACT_ROOT


def _now() -> datetime.datetime:
    return datetime.datetime.utcnow()


def _normalize_import_id(raw_import_id: str | None) -> str:
    configured_import_id = raw_import_id or os.getenv("HLTHPRT_CLINICAL_REFERENCE_IMPORT_ID")
    if configured_import_id:
        cleaned_import_id = "".join(
            character
            for character in str(configured_import_id)
            if character.isalnum()
        )
        if cleaned_import_id:
            return cleaned_import_id[:32]
    return _now().strftime("%Y%m%d")


def _build_request(
    test_mode: bool,
    import_id: str | None,
    source_names: str | None,
    artifact_root: str | None,
    force_download: bool,
    run_id: str | None,
) -> ClinicalReferenceRequest:
    source_test_limit = (
        int(os.getenv("HLTHPRT_CLINICAL_REFERENCE_TEST_LIMIT", "250"))
        if test_mode
        else None
    )
    return ClinicalReferenceRequest(
        test_mode=test_mode,
        import_suffix=_normalize_import_id(import_id),
        artifact_root=_artifact_root(artifact_root),
        selected_source_names=_selected_sources(source_names),
        umls_key=os.getenv("HLTHPRT_UMLS_API_KEY") or os.getenv("UMLS_API_KEY"),
        source_test_limit=source_test_limit,
        force_download=force_download,
        run_id=run_id,
    )


def _load_icd10cm_source(request: ClinicalReferenceRequest) -> ClinicalReferenceRows:
    artifact_path = _download_url(
        os.getenv("HLTHPRT_ICD10CM_URL", CDC_ICD10CM_URL),
        request.artifact_root / "icd10cm" / "icd10cm-CodeDescriptions-2026.zip",
        force=request.force_download,
        run_id=request.run_id,
    )
    concepts, synonyms, crosswalks = _parse_icd10cm(
        artifact_path,
        request.source_test_limit,
    )
    return ClinicalReferenceRows(
        concept_rows=concepts,
        synonym_rows=synonyms,
        crosswalk_rows=crosswalks,
        source_count_by_name={"icd10cm": len(concepts)},
    )


def _load_mesh_source(request: ClinicalReferenceRequest) -> ClinicalReferenceRows:
    mesh_rows = ClinicalReferenceRows()
    source_specs = (
        ("HLTHPRT_MESH_DESC_URL", MESH_DESC_URL, "desc2026.gz", "nlm_mesh_descriptor"),
        ("HLTHPRT_MESH_SUPP_URL", MESH_SUPP_URL, "supp2026.gz", "nlm_mesh_supplemental"),
    )
    for environment_name, default_url, filename, source_name in source_specs:
        _raise_if_cancelled(request.run_id)
        artifact_path = _download_url(
            os.getenv(environment_name, default_url),
            request.artifact_root / "mesh" / filename,
            force=request.force_download,
            run_id=request.run_id,
        )
        concepts, synonyms, relationships = _parse_mesh_file(
            artifact_path,
            source_name,
            request.source_test_limit,
        )
        mesh_rows.merge(
            ClinicalReferenceRows(
                concept_rows=concepts,
                synonym_rows=synonyms,
                relationship_rows=relationships,
                source_count_by_name={source_name: len(concepts)},
            )
        )
    return mesh_rows


def _load_rxnorm_source(request: ClinicalReferenceRequest) -> ClinicalReferenceRows:
    _raise_if_cancelled(request.run_id)
    release_map = _release_current("rxnorm-full-monthly-release")
    artifact_path = _download_url(
        release_map["downloadUrl"],
        request.artifact_root / "rxnorm" / release_map["fileName"],
        api_key=request.umls_key,
        force=request.force_download,
        run_id=request.run_id,
    )
    concepts, synonyms, relationships = _parse_rxnorm(
        artifact_path,
        request.source_test_limit,
    )
    for concept_map in concepts:
        concept_map["source_release"] = release_map.get("releaseVersion")
    return ClinicalReferenceRows(
        concept_rows=concepts,
        synonym_rows=synonyms,
        relationship_rows=relationships,
        source_count_by_name={"rxnorm": len(concepts)},
    )


def _load_snomed_source(request: ClinicalReferenceRequest) -> ClinicalReferenceRows:
    _raise_if_cancelled(request.run_id)
    if not request.umls_key:
        raise RuntimeError("SNOMED import requires HLTHPRT_UMLS_API_KEY or UMLS_API_KEY.")
    release_map = _release_current("snomed-ct-us-edition")
    artifact_path = _download_url(
        release_map["downloadUrl"],
        request.artifact_root / "snomedct_us" / release_map["fileName"],
        api_key=request.umls_key,
        force=request.force_download,
        run_id=request.run_id,
    )
    concepts, synonyms, relationships = _parse_snomed(
        artifact_path,
        request.source_test_limit,
    )
    for concept_map in concepts:
        concept_map["source_release"] = release_map.get("releaseVersion")
    snomed_rows = ClinicalReferenceRows(
        concept_rows=concepts,
        synonym_rows=synonyms,
        relationship_rows=relationships,
        source_count_by_name={"snomed": len(concepts)},
    )
    _load_snomed_map(request, snomed_rows)
    return snomed_rows


def _load_snomed_map(
    request: ClinicalReferenceRequest,
    snomed_rows: ClinicalReferenceRows,
) -> None:
    try:
        release_map = _release_current("snomed-ct-to-icd-10-cm-mapping-resources")
        artifact_path = _download_url(
            release_map["downloadUrl"],
            request.artifact_root / "snomedct_icd10cm" / release_map["fileName"],
            api_key=request.umls_key,
            force=request.force_download,
            run_id=request.run_id,
        )
        crosswalks = _parse_snomed_icd_map(artifact_path, request.source_test_limit)
        snomed_rows.crosswalk_rows.extend(crosswalks)
        snomed_rows.source_count_by_name["snomed_icd10cm_map"] = len(crosswalks)
    except ImportCancelledError:
        raise
    except Exception as exc:
        print(f"SNOMED ICD-10-CM map skipped: {exc}")


async def _collect_reference_rows(
    request: ClinicalReferenceRequest,
) -> ClinicalReferenceRows:
    collected_rows = ClinicalReferenceRows()
    if "icd10cm" in request.selected_source_names:
        collected_rows.merge(_load_icd10cm_source(request))
    if "mesh" in request.selected_source_names:
        collected_rows.merge(_load_mesh_source(request))
    if "rxnorm" in request.selected_source_names:
        collected_rows.merge(_load_rxnorm_source(request))
    if "snomed" in request.selected_source_names:
        collected_rows.merge(_load_snomed_source(request))
    if "medrt" in request.selected_source_names:
        _raise_if_cancelled(request.run_id)
        concepts, synonyms, relationships = await _load_medrt_from_rxclass(
            request.test_mode
        )
        collected_rows.merge(
            ClinicalReferenceRows(
                concept_rows=concepts,
                synonym_rows=synonyms,
                relationship_rows=relationships,
                source_count_by_name={"medrt": len(concepts)},
            )
        )
    return collected_rows


def _index_reference_rows(source_rows: ClinicalReferenceRows) -> ClinicalReferenceIndexes:
    return ClinicalReferenceIndexes(
        concepts_by_identity={
            (concept_map["code_system"], concept_map["code"]): concept_map
            for concept_map in source_rows.concept_rows
        },
        synonyms_by_identity={
            (
                synonym_map["code_system"],
                synonym_map["code"],
                synonym_map["synonym"],
                synonym_map["term_type"],
            ): synonym_map
            for synonym_map in source_rows.synonym_rows
        },
        crosswalks_by_identity={
            (
                crosswalk_map["from_system"],
                crosswalk_map["from_code"],
                crosswalk_map["to_system"],
                crosswalk_map["to_code"],
            ): crosswalk_map
            for crosswalk_map in source_rows.crosswalk_rows
        },
        relationships_by_identity={
            (
                relationship_map["from_system"],
                relationship_map["from_code"],
                relationship_map["relationship"],
                relationship_map["to_system"],
                relationship_map["to_code"],
            ): relationship_map
            for relationship_map in source_rows.relationship_rows
        },
    )


def _build_import_summary(
    request: ClinicalReferenceRequest,
    source_rows: ClinicalReferenceRows,
    reference_indexes: ClinicalReferenceIndexes,
    clinical_area_rows: ClinicalAreaRows,
    concept_count: int,
) -> dict[str, Any]:
    return {
        "import_id": request.import_suffix,
        "sources": sorted(request.selected_source_names),
        "source_counts": source_rows.source_count_by_name,
        "concept_rows": concept_count,
        "synonym_rows": len(reference_indexes.synonyms_by_identity),
        "crosswalk_rows": len(reference_indexes.crosswalks_by_identity),
        "relationship_rows": len(reference_indexes.relationships_by_identity),
        "clinical_area_rows": len(clinical_area_rows.area_rows),
        "clinical_area_condition_rows": len(clinical_area_rows.condition_rows),
        "clinical_area_treatment_rows": len(clinical_area_rows.treatment_rows),
        "artifact_root": str(request.artifact_root),
        "test_mode": request.test_mode,
    }


async def import_clinical_reference(
    test_mode: bool = False,
    import_id: str | None = None,
    sources: str | None = None,
    artifact_root: str | None = None,
    force_download: bool = False,
    run_id: str | None = None,
) -> dict[str, Any]:
    """Load and persist clinical reference relationships."""
    await ensure_database(test_mode)
    schema = _schema()
    request = _build_request(
        test_mode,
        import_id,
        sources,
        artifact_root,
        force_download,
        run_id,
    )
    return await _execute_clinical_reference_import(schema, request)


async def _execute_clinical_reference_import(
    schema: str,
    request: ClinicalReferenceRequest,
) -> dict[str, Any]:
    await _ensure_schema_exists(schema)
    await _ensure_unified_code_tables(schema)
    stage_models = await _create_stage_models(schema, request.import_suffix)
    source_rows = await _collect_reference_rows(request)
    reference_indexes = _index_reference_rows(source_rows)
    clinical_area_rows = await _stage_reference_rows(
        schema,
        stage_models,
        reference_indexes,
    )
    concept_count = await _catalog_stage_count(schema, stage_models, request.test_mode)
    await _publish_reference_stages(schema, stage_models)
    summary_map = _build_import_summary(
        request,
        source_rows,
        reference_indexes,
        clinical_area_rows,
        concept_count,
    )
    print(f"Clinical reference import done: {summary_map}")
    return summary_map


async def main(
    test_mode: bool = False,
    import_id: str | None = None,
    sources: str | None = None,
    artifact_root: str | None = None,
    force_download: bool = False,
    run_id: str | None = None,
) -> dict[str, Any]:
    """Run the clinical reference import entry point."""
    await init_db(db)
    try:
        return await import_clinical_reference(
            test_mode=test_mode,
            import_id=import_id,
            sources=sources,
            artifact_root=artifact_root,
            force_download=force_download,
            run_id=run_id,
        )
    finally:
        await db.disconnect()
