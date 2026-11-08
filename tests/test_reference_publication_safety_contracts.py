# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import importlib
from contextlib import AbstractAsyncContextManager
from dataclasses import replace
from unittest.mock import AsyncMock, Mock

import pytest

import process.clinical_reference_publication as clinical_publication
import process.reference_stage as reference_stage
from api import control_imports
from process import scoped_catalog_handoff as catalog_handoff
from process.control_cancel import ImportCancelledError

clinical = importlib.import_module("process.clinical_reference")
ms_drg = importlib.import_module("process.ms_drg")


@pytest.mark.parametrize("importer_name", ["clinical-reference", "ms-drg"])
def test_reference_importers_expose_cooperative_cancellation(importer_name):
    importer_by_name = {importer["name"]: importer for importer in control_imports.importer_registry()}
    assert importer_by_name[importer_name]["cancelable"] is True
    assert control_imports._supports_active_cancel(importer_name) is True


class _FakeStage:
    __tablename__ = "reference_stage"
    __table__ = object()


class _RecordingTransaction(AbstractAsyncContextManager):
    def __init__(self, database):
        self.database = database

    async def __aenter__(self):
        self.database.is_transaction_active = True
        self.database.transaction_events.append("transaction-enter")
        return self

    async def __aexit__(self, exception_type, *_exception):
        event_name = "transaction-rollback" if exception_type is not None else "transaction-commit"
        self.database.transaction_events.append(event_name)
        self.database.is_transaction_active = False
        return False


class _RecordingDb:
    def __init__(self):
        self.is_transaction_active = False
        self.transaction_events = []
        self.status_queries = []

    async def status(self, query, *_args, **_kwargs):
        self.status_queries.append(query)
        return None

    async def create_table(self, *_args, **_kwargs):
        return None

    def transaction(self):
        return _RecordingTransaction(self)


class _FailingCreateDb(_RecordingDb):
    def __init__(self):
        super().__init__()
        self.create_count = 0

    async def create_table(self, *_args, **_kwargs):
        self.create_count += 1
        if self.create_count == 2:
            raise RuntimeError("synthetic stage creation failure")


def _ms_drg_request(run_id="run-safe", import_suffix="shared"):
    return ms_drg.MsDrgImportRequest(
        test_mode=True,
        include_relationships=True,
        relationship_page_limit=2,
        concurrency=1,
        cms_page_url="https://example.test/cms",
        manual_toc_url="https://example.test/manual",
        import_suffix=import_suffix,
        run_id=run_id,
    )


def _ms_drg_payloads():
    return ms_drg.MsDrgPayloads(
        catalog_payloads=[{"code": "001"}],
        synonym_payloads=[{"code": "001", "synonym": "Synthetic group"}],
        relationship_payloads=[
            {
                "from_system": "MS_DRG",
                "from_code": "001",
                "relationship": "uses_icd10cm",
                "to_system": "ICD10CM",
                "to_code": "A001",
            }
        ],
    )


def _stage_drop_count(database):
    return sum(query.startswith("DROP TABLE IF EXISTS unit.reference_stage") for query in database.status_queries)


def _install_ms_drg_handoff_contract(monkeypatch, database, events, failure=None):
    """Double candidate I/O only; exercise the real handoff transaction and importer boundary."""
    incoming, receipt = object(), object()

    async def precreate(session, importer, schema):
        assert session.database is database and database.is_transaction_active
        assert (importer, schema) == ("ms-drg", "unit")
        events.append("create")
        return incoming

    async def copy_rows(session, selected, payloads):
        assert session.database is database and database.is_transaction_active
        assert selected is incoming
        assert payloads == {
            "code_catalog": _ms_drg_payloads().catalog_payloads,
            "code_synonym": _ms_drg_payloads().synonym_payloads,
            "code_relationship": _ms_drg_payloads().relationship_payloads,
        }
        events.extend(payloads)
        if failure is not None:
            raise failure

    async def record(session, context, **options):
        assert session.database is database and database.is_transaction_active
        assert context["context"]["control_run_id"] == "run-safe"
        assert options == {
            "importer": "ms-drg",
            "schema": "unit",
            "incoming": incoming,
            "options": {"include_relationships": True, "test_mode": False},
            "metrics": {"catalog_rows": 1, "synonym_rows": 1, "relationship_rows": 1},
        }
        events.append("handoff")
        return receipt

    monkeypatch.setattr(ms_drg, "db", database)
    monkeypatch.setattr(ms_drg, "_raise_if_cancelled", lambda _run_id: None)
    monkeypatch.setattr(catalog_handoff.publication, "precreate_catalog_input", precreate)
    monkeypatch.setattr(catalog_handoff.publication, "copy_catalog_records", copy_rows)
    record_handoff = AsyncMock(side_effect=record)
    activate = AsyncMock()
    monkeypatch.setattr(catalog_handoff, "record_catalog_handoff", record_handoff)
    monkeypatch.setattr(catalog_handoff.publication, "activate_catalog_family", activate)
    return receipt, record_handoff, activate


@pytest.mark.asyncio
async def test_ms_drg_candidate_and_handoff_share_one_transaction(monkeypatch):
    """All payload families commit one deferred handoff, never a worker-side live replacement."""
    database, events = _RecordingDb(), []
    receipt, recorded, activate = _install_ms_drg_handoff_contract(monkeypatch, database, events)
    context_by_field = {"context": {"control_run_id": "run-safe"}}
    summary_map = {"catalog_rows": 1, "synonym_rows": 1, "relationship_rows": 1}

    outcome = await ms_drg._stage_and_publish(
        "unit", replace(_ms_drg_request(), test_mode=False), _ms_drg_payloads(), summary_map, context_by_field
    )

    assert outcome == {**summary_map, "status": "finalizing", catalog_handoff.METRIC: receipt}
    assert context_by_field["context"]["_control_committed_result"] is outcome
    assert context_by_field["context"]["control_run_handoff_committed"] is True
    assert events == ["create", "code_catalog", "code_synonym", "code_relationship", "handoff"]
    assert database.transaction_events == ["transaction-enter", "transaction-commit"]
    recorded.assert_awaited_once()
    activate.assert_not_awaited()
    assert database.status_queries == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure", [RuntimeError("relationship copy failed"), ImportCancelledError("canceled after staging")]
)
async def test_ms_drg_late_preparation_refusal_rolls_back_without_handoff(monkeypatch, failure):
    """A late failure or cancellation leaves neither a committed handoff nor live publication."""
    database, events = _RecordingDb(), []
    _receipt, recorded, activate = _install_ms_drg_handoff_contract(monkeypatch, database, events, failure)
    context_by_field = {"context": {"control_run_id": "run-safe"}}
    with pytest.raises(type(failure)) as caught:
        await ms_drg._stage_and_publish(
            "unit", replace(_ms_drg_request(), test_mode=False), _ms_drg_payloads(), {}, context_by_field
        )

    assert caught.value is failure
    assert events == ["create", "code_catalog", "code_synonym", "code_relationship"]
    assert database.transaction_events == ["transaction-enter", "transaction-rollback"]
    assert "control_run_handoff_committed" not in context_by_field["context"]
    assert "_control_committed_result" not in context_by_field["context"]
    recorded.assert_not_awaited()
    activate.assert_not_awaited()
    assert database.status_queries == []


@pytest.mark.asyncio
async def test_ms_drg_cancellation_before_handoff_prevents_preparation(monkeypatch):
    """Cancellation is checked before opening candidate custody or the publication handoff."""
    database, events = _RecordingDb(), []
    _receipt, recorded, activate = _install_ms_drg_handoff_contract(monkeypatch, database, events)
    cancel_import = Mock(side_effect=ImportCancelledError("canceled before preparation"))
    monkeypatch.setattr(ms_drg, "_raise_if_cancelled", cancel_import)
    with pytest.raises(ImportCancelledError, match="before preparation"):
        await ms_drg._stage_and_publish("unit", _ms_drg_request(), _ms_drg_payloads(), {})

    cancel_import.assert_called_once_with("run-safe")
    assert events == []
    assert database.transaction_events == []
    recorded.assert_not_awaited()
    activate.assert_not_awaited()
    assert database.status_queries == []


@pytest.mark.asyncio
async def test_stage_names_are_isolated_by_importer_and_execution(monkeypatch):
    database = _RecordingDb()
    clinical_suffixes = []

    def make_clinical_stage(_model_class, stage_suffix):
        clinical_suffixes.append(stage_suffix)
        return _FakeStage

    monkeypatch.setattr(clinical_publication, "db", database)
    monkeypatch.setattr(
        clinical_publication,
        "make_class",
        make_clinical_stage,
    )
    await clinical_publication._create_stage_models(
        "unit",
        "Release44",
        "clinical-run-one",
    )
    await clinical_publication._create_stage_models(
        "unit",
        "Release44",
        "clinical-run-two",
    )

    clinical_request = clinical.ClinicalReferenceRequest(
        test_mode=True,
        import_suffix="Release44",
        artifact_root=clinical.DEFAULT_ARTIFACT_ROOT,
        selected_source_names=set(),
        umls_key=None,
        source_test_limit=1,
        force_download=False,
        run_id="run-safe",
    )
    ms_drg_request = _ms_drg_request(
        run_id="clinical-run-one",
        import_suffix="Release44",
    )
    first_clinical_suffix = clinical_suffixes[0]
    second_clinical_suffix = clinical_suffixes[7]
    ms_drg_suffix = ms_drg._ms_drg_stage_suffix(
        ms_drg_request.import_suffix,
        ms_drg_request.run_id,
    )
    assert clinical_suffixes[:7] == [first_clinical_suffix] * 7
    assert clinical_suffixes[7:] == [second_clinical_suffix] * 7
    assert len({first_clinical_suffix, second_clinical_suffix, ms_drg_suffix}) == 3
    stage_suffix_values = (
        first_clinical_suffix,
        second_clinical_suffix,
        ms_drg_suffix,
    )
    assert all(suffix == suffix.lower() and suffix.replace("_", "").isalnum() for suffix in stage_suffix_values)
    assert max(map(len, stage_suffix_values)) <= 25
    assert clinical_request.import_suffix == ms_drg_request.import_suffix == "Release44"


def test_direct_stage_names_use_a_fresh_execution_nonce(monkeypatch):
    nonce_values = iter(("firstnonce", "secondnonce"))
    monkeypatch.setattr(
        reference_stage.secrets,
        "token_hex",
        lambda _byte_count: next(nonce_values),
    )

    first_suffix = ms_drg._ms_drg_stage_suffix("Release44")
    second_suffix = ms_drg._ms_drg_stage_suffix("Release44")

    assert first_suffix != second_suffix


@pytest.mark.asyncio
async def test_partial_clinical_stage_creation_is_cleaned(monkeypatch):
    database = _FailingCreateDb()
    monkeypatch.setattr(clinical_publication, "db", database)
    monkeypatch.setattr(
        clinical_publication,
        "make_class",
        lambda *_args: _FakeStage,
    )

    with pytest.raises(RuntimeError, match="stage creation failure"):
        await clinical_publication._create_stage_models(
            "unit",
            "shared",
            "run-partial",
        )

    assert database.create_count == 2
    assert _stage_drop_count(database) == 14


@pytest.mark.asyncio
async def test_clinical_cancellation_after_staging_prevents_publication(
    monkeypatch,
):
    stage_models = clinical_publication.ClinicalStageModels({}, (), ())
    stage_rows = clinical_publication.ClinicalAreaRows([], [], [])
    catalog_count = AsyncMock(return_value=1)
    publish_stages = AsyncMock()
    drop_stages = AsyncMock()
    cancel_import = Mock(side_effect=ImportCancelledError("cancelled after staging"))
    monkeypatch.setattr(clinical, "_ensure_schema_exists", AsyncMock())
    monkeypatch.setattr(clinical, "_ensure_unified_code_tables", AsyncMock())
    monkeypatch.setattr(
        clinical,
        "_create_stage_models",
        AsyncMock(return_value=stage_models),
    )
    monkeypatch.setattr(
        clinical,
        "_collect_reference_rows",
        AsyncMock(return_value=clinical.ClinicalReferenceRows()),
    )
    monkeypatch.setattr(
        clinical,
        "_stage_reference_rows",
        AsyncMock(return_value=stage_rows),
    )
    monkeypatch.setattr(clinical, "_catalog_stage_count", catalog_count)
    monkeypatch.setattr(clinical, "_raise_if_cancelled", cancel_import)
    monkeypatch.setattr(clinical, "_publish_reference_stages", publish_stages)
    monkeypatch.setattr(clinical, "_drop_stage_models", drop_stages)

    with pytest.raises(ImportCancelledError, match="after staging"):
        await clinical._execute_clinical_reference_import(
            "unit",
            clinical.ClinicalReferenceRequest(
                True,
                "shared",
                clinical.DEFAULT_ARTIFACT_ROOT,
                set(),
                None,
                1,
                False,
                "run-safe",
            ),
        )

    catalog_count.assert_awaited_once()
    cancel_import.assert_called_once_with("run-safe")
    publish_stages.assert_not_awaited()
    drop_stages.assert_awaited_once_with("unit", stage_models)
