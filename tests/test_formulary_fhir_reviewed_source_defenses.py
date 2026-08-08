# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Failure-path coverage for the reviewed formulary source candidate."""

from __future__ import annotations

import copy
import datetime as dt
import json
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, Mock

import pytest

import process.formulary_fhir.repository_proof as proof_module
import process.formulary_fhir.reviewed_source as reviewed_module
from process.formulary_fhir.manual_lock import ManualSourceLockError
from process.formulary_fhir.repository_shared import AliasRef
from process.formulary_fhir.repository_shared import AliasVersionWrite
from process.formulary_fhir.repository_shared import DatasetRef
from process.formulary_fhir.source import EnabledSourceBinding
from process.formulary_fhir.synchronizer import SynchronizationResult
from process.formulary_fhir.types import AlternativeCorrection


CUTOFF = dt.datetime(2024, 1, 2, 12, tzinfo=dt.UTC)


def _manifest_document() -> dict[str, object]:
    return json.loads(
        reviewed_module.DEFAULT_REVIEWED_SOURCE_MANIFEST.read_text(
            encoding="utf-8"
        )
    )


def _manifest() -> reviewed_module.ReviewedSourceManifest:
    return reviewed_module.reviewed_source_manifest()


def _result() -> SynchronizationResult:
    return SynchronizationResult(
        dataset_id="ffd_" + "1" * 48,
        acquisition_contract_hash="a" * 64,
        list_count=1,
        alias_count=2,
        medication_membership_count=4,
        coverage_hash="b" * 64,
        membership_hash="c" * 64,
        full_aliases=2,
        reused_aliases=0,
        resumed_aliases=0,
        request_count=3,
        transient_retry_count=0,
        throttle_count=0,
    )


@asynccontextmanager
async def _transaction():
    yield


def _database(
    *,
    source_rows: tuple[dict[str, object], ...] = (),
    dataset_by_field: dict[str, object] | None = None,
    pointer_by_field: dict[str, object] | None = None,
    status_result: int | None = None,
):
    database = Mock()
    database.transaction = _transaction
    database.all = AsyncMock(return_value=list(source_rows))

    async def first(statement: str, **_params: object):
        if "fhir_formulary_current" in statement:
            return pointer_by_field
        return dataset_by_field

    database.first = AsyncMock(side_effect=first)
    database.status = AsyncMock(return_value=status_result)
    return database


@pytest.mark.parametrize(
    "field_path, replacement",
    [
        ((), None),
        (("unexpected",), True),
        (("schema_version",), True),
        (("importer",), "other-importer"),
        (("source",), None),
        (("source", "unexpected"), True),
        (("reviewed_at",), 20240102),
        (("reviewed_at",), "20240102"),
        (
            (
                "source",
                "metadata_json",
                "alternative_reference_correction",
            ),
            [],
        ),
    ],
)
def test_manifest_shape_failures_are_sanitized(field_path, replacement):
    manifest_document: object = copy.deepcopy(_manifest_document())
    if field_path:
        target_by_field = manifest_document
        for field_name in field_path[:-1]:
            target_by_field = target_by_field[field_name]
        target_by_field[field_path[-1]] = replacement
    else:
        manifest_document = replacement

    with pytest.raises(reviewed_module.ReviewedSourceError) as caught:
        reviewed_module._validated_manifest_document(manifest_document)

    assert caught.value.code == "manifest"
    assert str(caught.value) == reviewed_module.ERROR_MESSAGES["manifest"]


@pytest.mark.parametrize("mode", ["missing", "invalid_json", "non_object"])
def test_manifest_read_failures_are_sanitized(monkeypatch, tmp_path, mode):
    manifest_path = tmp_path / "reviewed-source.json"
    if mode == "invalid_json":
        manifest_path.write_text("{", encoding="utf-8")
    elif mode == "non_object":
        manifest_path.write_text("[]", encoding="utf-8")
    monkeypatch.setattr(
        reviewed_module,
        "DEFAULT_REVIEWED_SOURCE_MANIFEST",
        manifest_path,
    )

    with pytest.raises(reviewed_module.ReviewedSourceError) as caught:
        reviewed_module.reviewed_source_manifest()

    assert caught.value.code == "manifest"


def test_unknown_reviewed_error_code_falls_back_to_sanitized_source_error():
    error = reviewed_module.ReviewedSourceError("unexpected")

    assert error.code == "source"
    assert str(error) == reviewed_module.ERROR_MESSAGES["source"]


@pytest.mark.asyncio
@pytest.mark.parametrize("inserted_count", [0, 2])
async def test_source_insert_rejects_nonexact_status(inserted_count):
    database = _database(status_result=inserted_count)

    with pytest.raises(reviewed_module.ReviewedSourceError) as caught:
        await reviewed_module._insert_source(database, _manifest())

    assert caught.value.code == "source"


@pytest.mark.asyncio
async def test_registration_rejects_duplicate_exact_rows():
    manifest = _manifest()
    source_row = reviewed_module._source_values(manifest)
    database = _database(source_rows=(source_row, copy.deepcopy(source_row)))

    with pytest.raises(reviewed_module.ReviewedSourceError) as caught:
        await reviewed_module._register_manifest(database, manifest)

    assert caught.value.code == "catalog"


@pytest.mark.asyncio
async def test_registration_rejects_postlock_configuration_mismatch(monkeypatch):
    manifest = _manifest()
    source_row = reviewed_module._source_values(manifest)
    database = _database(source_rows=(source_row,))
    mismatched_binding = EnabledSourceBinding(
        manifest.source_id,
        manifest.config,
        "f" * 64,
        alternative_correction=manifest.alternative_correction,
        launch_mode="manual-library",
    )
    monkeypatch.setattr(
        reviewed_module,
        "load_enabled_source",
        AsyncMock(return_value=mismatched_binding),
    )

    with pytest.raises(reviewed_module.ReviewedSourceError) as caught:
        await reviewed_module._register_manifest(database, manifest)

    assert caught.value.code == "source"


@pytest.mark.asyncio
async def test_current_pointer_accepts_exact_id_and_rejects_corruption():
    pointer_id = "ffd_" + "2" * 48
    assert await reviewed_module._current_pointer(
        _database(pointer_by_field={"dataset_id": pointer_id}),
        "source-alpha",
    ) == pointer_id

    with pytest.raises(reviewed_module.ReviewedSourceError) as caught:
        await reviewed_module._current_pointer(
            _database(pointer_by_field={"dataset_id": ""}),
            "source-alpha",
        )

    assert caught.value.code == "catalog"


@pytest.mark.asyncio
@pytest.mark.parametrize("failure_name", ["catalog", "dataset", "pointer"])
async def test_nonpublishing_postflight_fails_closed(failure_name):
    manifest = _manifest()
    source_rows = (reviewed_module._source_values(manifest),)
    dataset_by_field = {
        "status": "verified",
        "publish_requested": False,
        "seed_eligible": False,
    }
    pointer_by_field = None
    expected_code = "source"
    if failure_name == "catalog":
        source_rows = ()
        expected_code = "catalog"
    elif failure_name == "dataset":
        dataset_by_field["publish_requested"] = True
    else:
        pointer_by_field = {"dataset_id": "ffd_" + "3" * 48}
    database = _database(
        source_rows=source_rows,
        dataset_by_field=dataset_by_field,
        pointer_by_field=pointer_by_field,
    )

    with pytest.raises(reviewed_module.ReviewedSourceError) as caught:
        await reviewed_module._require_nonpublishing_candidate(
            database,
            manifest,
            _result(),
            None,
        )

    assert caught.value.code == expected_code


@pytest.mark.asyncio
async def test_nonpublishing_postflight_accepts_exact_unchanged_state():
    manifest = _manifest()
    database = _database(
        source_rows=(reviewed_module._source_values(manifest),),
        dataset_by_field={
            "status": "verified",
            "publish_requested": False,
            "seed_eligible": False,
        },
    )

    await reviewed_module._require_nonpublishing_candidate(
        database,
        manifest,
        _result(),
        None,
    )


class _ClientContext:
    def __init__(self) -> None:
        self.client = object()

    async def __aenter__(self):
        return self.client

    async def __aexit__(self, *_error):
        return None


@pytest.mark.asyncio
async def test_registered_candidate_forwards_only_nonpublishing_intent(monkeypatch):
    manifest = _manifest()
    binding = EnabledSourceBinding(
        manifest.source_id,
        manifest.config,
        "d" * 64,
        alternative_correction=manifest.alternative_correction,
        launch_mode="manual-library",
    )
    client_context = _ClientContext()
    client_factory = Mock(return_value=client_context)
    repository = object()
    run_sync = AsyncMock(return_value=_result())
    monkeypatch.setattr(
        reviewed_module,
        "_register_manifest",
        AsyncMock(return_value=binding),
    )
    repository_factory = Mock(return_value=repository)
    monkeypatch.setattr(
        reviewed_module,
        "FHIRFormularyRepository",
        repository_factory,
    )
    monkeypatch.setattr(reviewed_module, "_run_verified_sync", run_sync)

    observed = await reviewed_module._verify_registered_candidate(
        object(),
        manifest,
        client_factory,
        "candidate-run",
        CUTOFF,
    )

    assert observed == _result()
    assert run_sync.await_args.kwargs["intent"] == "none"
    assert run_sync.await_args.kwargs["client"] is client_context.client
    assert run_sync.await_args.kwargs["repository"] is repository


@pytest.mark.asyncio
async def test_candidate_propagates_postflight_failure_before_return(monkeypatch):
    manifest = _manifest()

    @asynccontextmanager
    async def source_lease(*_args, **_kwargs):
        yield

    monkeypatch.setattr(reviewed_module, "reviewed_source_manifest", lambda: manifest)
    monkeypatch.setattr(reviewed_module.manual_lock, "manual_source_lease", source_lease)
    monkeypatch.setattr(
        reviewed_module,
        "_current_pointer",
        AsyncMock(return_value="ffd_" + "4" * 48),
    )
    monkeypatch.setattr(
        reviewed_module,
        "_verify_registered_candidate",
        AsyncMock(return_value=_result()),
    )
    postflight = AsyncMock(side_effect=reviewed_module.ReviewedSourceError("source"))
    monkeypatch.setattr(
        reviewed_module,
        "_require_nonpublishing_candidate",
        postflight,
    )

    with pytest.raises(reviewed_module.ReviewedSourceError) as caught:
        await reviewed_module.verify_reviewed_source_candidate(
            run_id="candidate-run",
            cutoff=CUTOFF,
            database=object(),
            client_factory=Mock(),
        )

    assert caught.value.code == "source"
    postflight.assert_awaited_once()


@pytest.mark.asyncio
async def test_candidate_returns_only_after_nonpublishing_postflight(monkeypatch):
    manifest = _manifest()
    synchronization_result = _result()

    @asynccontextmanager
    async def source_lease(*_args, **_kwargs):
        yield

    monkeypatch.setattr(reviewed_module, "reviewed_source_manifest", lambda: manifest)
    monkeypatch.setattr(reviewed_module.manual_lock, "manual_source_lease", source_lease)
    monkeypatch.setattr(
        reviewed_module,
        "_current_pointer",
        AsyncMock(return_value="ffd_" + "4" * 48),
    )
    monkeypatch.setattr(
        reviewed_module,
        "_verify_registered_candidate",
        AsyncMock(return_value=synchronization_result),
    )
    postflight = AsyncMock()
    monkeypatch.setattr(
        reviewed_module,
        "_require_nonpublishing_candidate",
        postflight,
    )

    observed = await reviewed_module.verify_reviewed_source_candidate(
        run_id="candidate-run",
        cutoff=CUTOFF,
        database=object(),
        client_factory=Mock(),
    )

    assert observed == synchronization_result
    postflight.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("lock_code", ["busy", "cleanup", "lock_unavailable"])
async def test_candidate_maps_manual_lock_failures(monkeypatch, lock_code):
    @asynccontextmanager
    async def source_lease(*_args, **_kwargs):
        raise ManualSourceLockError(lock_code)
        yield

    monkeypatch.setattr(reviewed_module.manual_lock, "manual_source_lease", source_lease)

    with pytest.raises(reviewed_module.ReviewedSourceError) as caught:
        await reviewed_module.verify_reviewed_source_candidate(
            run_id="candidate-run",
            cutoff=CUTOFF,
            database=object(),
        )

    assert caught.value.code == lock_code


def test_invalid_correction_object_is_rejected_before_proof(monkeypatch):
    monkeypatch.setattr(
        proof_module,
        "medication_variant_hash",
        Mock(return_value="e" * 64),
    )

    with pytest.raises(ValueError, match="alternative correction"):
        proof_module.source_medication_variant_hash(object(), object())


def test_source_binding_rejects_unvalidated_correction_object():
    manifest = _manifest()

    with pytest.raises(ValueError, match="source binding"):
        EnabledSourceBinding(
            manifest.source_id,
            manifest.config,
            "d" * 64,
            alternative_correction=object(),
        )


@pytest.mark.parametrize(
    "prefix, rule_version",
    [
        (None, "v1"),
        ("bad prefix", "v1"),
        ("PRE-", None),
        ("PRE-", ""),
        ("PRE-", "v" * 65),
        ("PRE-", " v1"),
        ("PRE-", "v1\n"),
    ],
)
def test_alternative_correction_rejects_noncanonical_values(prefix, rule_version):
    with pytest.raises(ValueError, match="correction policy"):
        AlternativeCorrection(prefix=prefix, rule_version=rule_version)


def test_alias_write_rejects_unvalidated_correction_object():
    dataset = DatasetRef(
        source_id="source-alpha",
        dataset_id="ffd_" + "5" * 48,
        run_id="candidate-run",
        previous_dataset_id=None,
        cutoff_at=CUTOFF,
        acquisition_contract_hash="f" * 64,
        intent="none",
        status="building",
    )
    alias = AliasRef(
        source_id="source-alpha",
        public_id="fhir_" + "6" * 26,
        alias_id="ffa_" + "7" * 48,
        source_plan_identifier="SYNTHETIC-PLAN",
    )

    with pytest.raises(ValueError, match="alternative correction"):
        AliasVersionWrite(dataset, alias, 0, (), 1, object())
