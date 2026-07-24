# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from contextlib import AbstractAsyncContextManager
import importlib
from pathlib import Path
from types import SimpleNamespace

import pytest

import process.clinical_reference_publication as publication
from process.control_cancel import ImportCancelledError
from db.models import (
    ClinicalArea,
    ClinicalAreaCondition,
    ClinicalAreaTreatment,
    CodeCatalog,
    CodeCrosswalk,
    CodeRelationship,
    CodeSynonym,
)

clinical = importlib.import_module("process.clinical_reference")


class _Transaction(AbstractAsyncContextManager):
    def __init__(self, events):
        self.events = events

    async def __aenter__(self):
        self.events.append("transaction-enter")
        return self

    async def __aexit__(self, *_exception):
        self.events.append("transaction-exit")
        return False


class _RecordingDb:
    def __init__(self, scalar_value=1):
        self.events = []
        self.scalar_value = scalar_value

    async def status(self, query):
        self.events.append(("status", " ".join(query.split())))

    async def create_table(self, table, **options):
        self.events.append(("create", table, options))

    async def scalar(self, query):
        self.events.append(("scalar", query))
        return self.scalar_value

    def transaction(self):
        return _Transaction(self.events)


def _stage_class(name, *, primary=True, additional=True):
    attributes_map = {
        "__tablename__": name,
        "__table__": object(),
        "__my_index_elements__": ["code"] if primary else [],
        "__my_additional_indexes__": (
            [
                {
                    "name": "search",
                    "index_elements": ["display_name"],
                    "using": "gin",
                    "where": "is_active",
                }
            ]
            if additional
            else []
        ),
    }
    return type(f"{name.title()}Stage", (), attributes_map)


def _reference_request(tmp_path, source_names):
    return clinical.ClinicalReferenceRequest(
        test_mode=True,
        import_suffix="unit",
        artifact_root=tmp_path,
        selected_source_names=set(source_names),
        umls_key="synthetic-key",
        source_test_limit=3,
        force_download=False,
        run_id="run-4",
    )


def _configure_source_loader_stubs(monkeypatch, downloaded_paths):
    def download(_url, path, **_options):
        downloaded_paths.append(path)
        return path

    monkeypatch.setattr(clinical, "_download_url", download)
    monkeypatch.setattr(
        clinical,
        "_parse_icd10cm",
        lambda *_args: (
            [{"code_system": "ICD10CM", "code": "A00"}],
            [{"code_system": "ICD10CM", "code": "A00", "synonym": "Alpha"}],
            [{"from_system": "ICD10CM_COMPACT", "from_code": "A00"}],
        ),
    )
    monkeypatch.setattr(
        clinical,
        "_parse_mesh_file",
        lambda path, source_name, _limit: (
            [{"code_system": "MESH", "code": path.name, "source": source_name}],
            [],
            [{"from_system": "MESH", "from_code": path.name}],
        ),
    )
    monkeypatch.setattr(
        clinical,
        "_release_current",
        lambda release_type: {
            "downloadUrl": f"https://example.test/{release_type}.zip",
            "fileName": f"{release_type}.zip",
            "releaseVersion": "2026.1",
        },
    )
    monkeypatch.setattr(
        clinical,
        "_parse_rxnorm",
        lambda *_args: ([{"code_system": "RXNORM", "code": "10"}], [], []),
    )
    monkeypatch.setattr(
        clinical,
        "_parse_snomed",
        lambda *_args: ([{"code_system": "SNOMEDCT_US", "code": "20"}], [], []),
    )
    monkeypatch.setattr(
        clinical,
        "_parse_snomed_icd_map",
        lambda *_args: [{"from_system": "SNOMEDCT_US", "from_code": "20"}],
    )


def test_source_loaders_preserve_release_and_source_counts(monkeypatch, tmp_path):
    """Each source loader maps parser output into the shared normalized contract."""
    request = _reference_request(
        tmp_path,
        {"icd10cm", "mesh", "rxnorm", "snomed"},
    )
    downloaded_paths = []
    _configure_source_loader_stubs(monkeypatch, downloaded_paths)

    icd_rows = clinical._load_icd10cm_source(request)
    mesh_rows = clinical._load_mesh_source(request)
    rxnorm_rows = clinical._load_rxnorm_source(request)
    snomed_rows = clinical._load_snomed_source(request)

    assert icd_rows.source_count_by_name == {"icd10cm": 1}
    assert mesh_rows.source_count_by_name == {
        "nlm_mesh_descriptor": 1,
        "nlm_mesh_supplemental": 1,
    }
    assert rxnorm_rows.concept_rows[0]["source_release"] == "2026.1"
    assert snomed_rows.concept_rows[0]["source_release"] == "2026.1"
    assert snomed_rows.source_count_by_name["snomed_icd10cm_map"] == 1
    assert {path.parent.name for path in downloaded_paths} >= {
        "icd10cm",
        "mesh",
        "rxnorm",
        "snomedct_us",
        "snomedct_icd10cm",
    }


def test_snomed_requires_key_and_tolerates_optional_map_failure(
    monkeypatch,
    tmp_path,
    capsys,
):
    """The licensed terminology gate is hard, while its optional map is best effort."""
    request_without_key = clinical.ClinicalReferenceRequest(
        **{
            **_reference_request(tmp_path, {"snomed"}).__dict__,
            "umls_key": None,
        }
    )
    with pytest.raises(RuntimeError, match="SNOMED import requires"):
        clinical._load_snomed_source(request_without_key)

    request = _reference_request(tmp_path, {"snomed"})
    snomed_rows = clinical.ClinicalReferenceRows()
    monkeypatch.setattr(
        clinical,
        "_release_current",
        lambda *_args: (_ for _ in ()).throw(OSError("map unavailable")),
    )
    clinical._load_snomed_map(request, snomed_rows)

    assert "map skipped" in capsys.readouterr().out
    assert snomed_rows.crosswalk_rows == []

    monkeypatch.setattr(
        clinical,
        "_release_current",
        lambda *_args: (_ for _ in ()).throw(ImportCancelledError("cancelled")),
    )
    with pytest.raises(ImportCancelledError, match="cancelled"):
        clinical._load_snomed_map(request, snomed_rows)


@pytest.mark.asyncio
async def test_source_collection_executes_only_selected_sources(monkeypatch, tmp_path):
    """Selection drives source execution and MED-RT participates in the same row contract."""
    request = _reference_request(
        tmp_path,
        {"icd10cm", "mesh", "rxnorm", "snomed", "medrt"},
    )
    executed_sources = []

    def source_bundle(source_name):
        executed_sources.append(source_name)
        return clinical.ClinicalReferenceRows(
            concept_rows=[{"code_system": source_name.upper(), "code": "1"}],
            source_count_by_name={source_name: 1},
        )

    monkeypatch.setattr(
        clinical,
        "_load_icd10cm_source",
        lambda _request: source_bundle("icd10cm"),
    )
    monkeypatch.setattr(
        clinical,
        "_load_mesh_source",
        lambda _request: source_bundle("mesh"),
    )
    monkeypatch.setattr(
        clinical,
        "_load_rxnorm_source",
        lambda _request: source_bundle("rxnorm"),
    )
    monkeypatch.setattr(
        clinical,
        "_load_snomed_source",
        lambda _request: source_bundle("snomed"),
    )

    async def load_medrt(_test_mode):
        executed_sources.append("medrt")
        return ([{"code_system": "MEDRT", "code": "1"}], [], [])

    monkeypatch.setattr(clinical, "_load_medrt_from_rxclass", load_medrt)
    collected_rows = await clinical._collect_reference_rows(request)

    assert executed_sources == ["icd10cm", "mesh", "rxnorm", "snomed", "medrt"]
    assert len(collected_rows.concept_rows) == 5
    assert collected_rows.source_count_by_name["medrt"] == 1


@pytest.mark.asyncio
async def test_empty_source_selection_skips_every_loader(tmp_path):
    """An explicit empty source selection produces an empty normalized bundle."""
    collected_rows = await clinical._collect_reference_rows(
        _reference_request(tmp_path, set())
    )

    assert collected_rows == clinical.ClinicalReferenceRows()


def test_code_type_classifiers_cover_all_domain_branches():
    """SNOMED semantic tags and MeSH trees map to their documented code types."""
    assert clinical._code_type_for_snomed("Therapy (regime/therapy)") == "treatment"
    assert clinical._code_type_for_snomed("Agent (product)") == "substance"
    assert clinical._code_type_for_snomed("Unclassified concept") == "concept"
    assert clinical._code_type_for_mesh(["F03.100"]) == "condition"
    assert clinical._code_type_for_mesh(["E01.100"]) == "treatment"
    assert clinical._code_type_for_mesh(["D01.100"]) == "substance"
    assert clinical._code_type_for_mesh([], descriptor_class="SCR") == "concept"


def test_row_indexes_deduplicate_by_public_contract():
    """Later duplicates replace earlier rows at each published uniqueness boundary."""
    source_rows = clinical.ClinicalReferenceRows(
        concept_rows=[
            {"code_system": "ICD10CM", "code": "A00", "display_name": "old"},
            {"code_system": "ICD10CM", "code": "A00", "display_name": "new"},
        ],
        synonym_rows=[
            {
                "code_system": "ICD10CM",
                "code": "A00",
                "synonym": "Alpha",
                "term_type": "preferred",
            }
        ],
        crosswalk_rows=[
            {
                "from_system": "LOCAL",
                "from_code": "A00",
                "to_system": "ICD10CM",
                "to_code": "A00",
            }
        ],
        relationship_rows=[
            {
                "from_system": "RXNORM",
                "from_code": "1",
                "relationship": "may_treat",
                "to_system": "MESH",
                "to_code": "D1",
            }
        ],
    )

    indexes = clinical._index_reference_rows(source_rows)

    assert indexes.concepts_by_identity[("ICD10CM", "A00")]["display_name"] == "new"
    assert len(indexes.synonyms_by_identity) == 1
    assert len(indexes.crosswalks_by_identity) == 1
    assert len(indexes.relationships_by_identity) == 1


def _complete_stage_models():
    stage_by_model = {
        model_class: _stage_class(f"{model_class.__tablename__}_unit")
        for model_class in (
            CodeCatalog,
            CodeCrosswalk,
            CodeSynonym,
            CodeRelationship,
            ClinicalArea,
            ClinicalAreaCondition,
            ClinicalAreaTreatment,
        )
    }
    return publication.ClinicalStageModels(
        stage_by_model=stage_by_model,
        shared_models=(CodeCatalog, CodeCrosswalk, CodeSynonym, CodeRelationship),
        area_models=(ClinicalArea, ClinicalAreaCondition, ClinicalAreaTreatment),
    )


@pytest.mark.asyncio
async def test_publication_builds_indexes_and_stages(monkeypatch):
    """Stage setup creates every table, index variant, and bounded push batch."""
    recording_db = _RecordingDb(scalar_value=2)
    monkeypatch.setattr(publication, "db", recording_db)
    pushed_batches = []

    async def push_objects(row_chunk, stage_class):
        pushed_batches.append((stage_class.__tablename__, list(row_chunk)))

    monkeypatch.setattr(publication, "push_objects", push_objects)
    monkeypatch.setattr(publication, "DEFAULT_BATCH_SIZE", 1)

    await publication._ensure_schema_exists("unit")
    await publication._ensure_unified_code_tables("unit")
    custom_stage = _stage_class("custom_stage")
    await publication._create_stage_indexes(custom_stage, "unit")
    await publication._create_stage_indexes(
        _stage_class("plain_stage", primary=False, additional=False),
        "unit",
    )
    await publication._push(custom_stage, [{"code": "1"}, {"code": "2"}])
    monkeypatch.setattr(
        publication,
        "make_class",
        lambda model_class, _suffix: _stage_class(
            f"{model_class.__tablename__}_created"
        ),
    )
    created_stage_models = await publication._create_stage_models("unit", "unit")
    assert len(created_stage_models.stage_by_model) == 7
    assert [len(row_chunk) for _stage, row_chunk in pushed_batches] == [2]
    sql_text = "\n".join(
        event[1] for event in recording_db.events if event[0] == "status"
    )
    assert "CREATE UNIQUE INDEX" in sql_text


@pytest.mark.asyncio
async def test_publication_replaces_owned_sources(monkeypatch):
    """Validated stages replace every owned live source inside one transaction."""
    recording_db = _RecordingDb(scalar_value=2)
    monkeypatch.setattr(publication, "db", recording_db)
    stage_models = _complete_stage_models()
    indexes = SimpleNamespace(
        concepts_by_identity={},
        synonyms_by_identity={},
        crosswalks_by_identity={},
        relationships_by_identity={},
    )
    area_rows = await publication._stage_reference_rows(
        "unit",
        stage_models,
        indexes,
    )
    concept_count = await publication._catalog_stage_count("unit", stage_models, True)
    await publication._publish_reference_stages("unit", stage_models)

    assert area_rows == publication.ClinicalAreaRows([], [], [])
    assert concept_count == 2
    sql_text = "\n".join(
        event[1] for event in recording_db.events if event[0] == "status"
    )
    assert "DELETE FROM unit.code_catalog" in sql_text
    assert "ALTER TABLE IF EXISTS" in sql_text
    assert "transaction-enter" in recording_db.events
    assert "transaction-exit" in recording_db.events


@pytest.mark.asyncio
async def test_catalog_threshold_blocks_publication(monkeypatch):
    """A below-minimum stage fails before any live table can be replaced."""
    recording_db = _RecordingDb(scalar_value=0)
    monkeypatch.setattr(publication, "db", recording_db)
    monkeypatch.setenv("HLTHPRT_CLINICAL_REFERENCE_MIN_ROWS", "2")
    stage_models = publication.ClinicalStageModels(
        stage_by_model={CodeCatalog: _stage_class("catalog_stage")},
        shared_models=(),
        area_models=(),
    )

    with pytest.raises(RuntimeError, match="below minimum 2"):
        await publication._catalog_stage_count("unit", stage_models, True)


@pytest.mark.asyncio
async def test_import_publishes_summary_contract(monkeypatch, tmp_path):
    """The public importer prepares, validates, publishes, and summarizes its stage."""
    lifecycle_events = []

    async def ensure_database(test_mode):
        lifecycle_events.append(("ensure", test_mode))

    async def ensure_schema(schema):
        lifecycle_events.append(("schema", schema))

    async def ensure_tables(schema):
        lifecycle_events.append(("tables", schema))

    async def create_stages(schema, suffix, run_id):
        lifecycle_events.append(("stages", suffix))
        return publication.ClinicalStageModels({}, (), ())

    async def collect_rows(_request):
        return clinical.ClinicalReferenceRows(
            concept_rows=[{"code_system": "ICD10CM", "code": "A00"}],
            source_count_by_name={"icd10cm": 1},
        )

    async def stage_rows(_schema, _stages, _indexes):
        return publication.ClinicalAreaRows([], [], [])

    async def count_rows(_schema, _stages, _test_mode):
        return 1

    async def publish_rows(_schema, _stages):
        lifecycle_events.append("published")

    monkeypatch.setattr(clinical, "ensure_database", ensure_database)
    monkeypatch.setattr(clinical, "_ensure_schema_exists", ensure_schema)
    monkeypatch.setattr(clinical, "_ensure_unified_code_tables", ensure_tables)
    monkeypatch.setattr(clinical, "_create_stage_models", create_stages)
    monkeypatch.setattr(clinical, "_collect_reference_rows", collect_rows)
    monkeypatch.setattr(clinical, "_stage_reference_rows", stage_rows)
    monkeypatch.setattr(clinical, "_catalog_stage_count", count_rows)
    monkeypatch.setattr(clinical, "_publish_reference_stages", publish_rows)
    monkeypatch.setattr(clinical, "_selected_sources", lambda _raw: {"icd10cm"})
    summary_map = await clinical.import_clinical_reference(
        test_mode=True,
        import_id="unit",
        artifact_root=str(tmp_path),
    )

    assert summary_map["concept_rows"] == 1
    assert summary_map["source_counts"] == {"icd10cm": 1}
    assert lifecycle_events[-1] == "published"


@pytest.mark.asyncio
async def test_entrypoint_disconnects_after_failure(monkeypatch):
    """The worker entry point disconnects even when its importer fails."""
    lifecycle_events = []

    class LifecycleDb:
        async def disconnect(self):
            lifecycle_events.append("disconnect")

    async def init_database(_db):
        lifecycle_events.append("init")

    async def fail_import(**_options):
        raise RuntimeError("synthetic failure")

    monkeypatch.setattr(clinical, "db", LifecycleDb())
    monkeypatch.setattr(clinical, "init_db", init_database)
    monkeypatch.setattr(clinical, "import_clinical_reference", fail_import)
    with pytest.raises(RuntimeError, match="synthetic failure"):
        await clinical.main()
    assert lifecycle_events == ["init", "disconnect"]
