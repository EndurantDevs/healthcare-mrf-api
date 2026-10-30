# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Retained common reuse must spend the new pair and prove every current component."""

from contextlib import asynccontextmanager
from copy import deepcopy
from dataclasses import dataclass, replace
from functools import partial
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_ordinary_serving as ordinary
from process import provider_directory_ordinary_serving_publication as publication
from process import provider_directory_ordinary_serving_reuse as reuse
from tests.provider_directory_serving_capacity_request_test_support import ordinary_execution
from tests.test_provider_directory_common_serving_recovery_document import common as common


@dataclass
class Serving:
    control_generation: int
    generation_id: str
    profile_as_of: str
    profile_rows: int = 1
    evidence_rows: int = 2
    source_vector_hash: str = "a" * 64


def _captured_inputs():
    return SimpleNamespace(
        predecessor={"kind": "published"},
        semantic_inputs={"dependency_proofs_complete": True, "inputs": "complete_current_inputs"},
        affected_scope={"source_ids": ["source-a"]},
        scope_payload={"requested_source_ids": ["source-a"]},
        recheck=AsyncMock(),
    )


@pytest.fixture
def case(common, monkeypatch):
    """Keep the real canonical Profile result while isolating native observation boundaries."""
    document = common.document
    profile_result = document["preparation"]["profile_result"]
    capture = _captured_inputs()
    document["preparation"]["semantic_inputs"] = deepcopy(capture.semantic_inputs)
    admission = SimpleNamespace(
        plan=object(), captured_inputs=capture, before_scratch=AsyncMock(), assert_ready=AsyncMock()
    )
    fhir = SimpleNamespace(
        _schema=lambda: "unit",
        _pagination_checkpoint_row_mapping=lambda row: row,
        _provider_directory_artifact_resource_types=lambda *args, **kwargs: frozenset({"Location"}),
        _profile_serving_state_from_row=lambda row: Serving(
            profile_result["generation"], profile_result["profile_generation_id"], profile_result["profile_as_of"]
        ),
        profile_selection_result=lambda *args, **kwargs: deepcopy(profile_result),
        PROFILE_SELECTION_RESULT_METRIC="profile_selection_result",
        db=SimpleNamespace(all=AsyncMock(return_value=[{"generation": document["generation"]}])),
    )
    prepared = SimpleNamespace(
        fhir=fhir,
        nonprofile_admission=admission,
        execution=replace(ordinary_execution(), generation=profile_result["generation"] + 1),
        fence=object(),
        metrics={},
    )
    context = SimpleNamespace(session=object(), actor={"owner": 20})

    @asynccontextmanager
    async def snapshot(_prepared):
        yield context

    event_by_field = {"document": document, "sha256": "e" * 64}
    monkeypatch.setattr(reuse, "_snapshot", snapshot)
    monkeypatch.setattr(reuse, "require_capture", lambda *_: capture)
    monkeypatch.setattr(reuse.journal, "capture_state", AsyncMock(return_value=deepcopy(document["after"])))
    monkeypatch.setattr(reuse.recovery, "_journal", AsyncMock())
    monkeypatch.setattr(reuse.recovery, "_load_history", AsyncMock(return_value={"published": event_by_field}))
    monkeypatch.setattr(reuse, "_lock_relations", AsyncMock(return_value=document["preparation"]["retention"]))
    monkeypatch.setattr(reuse.observations, "_retained", AsyncMock())
    monkeypatch.setattr(reuse, "_initial_retention", AsyncMock())
    monkeypatch.setattr(
        reuse.postimages,
        "capture_postimages",
        AsyncMock(return_value=deepcopy(document["preparation"]["expected_postimages"])),
    )
    monkeypatch.setattr(publication, "_historical_profile_result", AsyncMock(return_value=profile_result))
    monkeypatch.setattr(reuse.preparation, "_admit_profile_before_full_build", AsyncMock())
    return SimpleNamespace(
        prepared=prepared,
        capture=capture,
        admission=admission,
        event=event_by_field,
        context=context,
        profile_result=profile_result,
    )


@pytest.mark.parametrize("fault", [None, "incomplete", "semantic", "requested", "mandatory", "proof", "future"])
def test_only_complete_inputs_and_all_source_work_can_reuse(case, fault):
    recorded = case.event["document"]["preparation"]
    match fault:
        case "incomplete":
            case.capture.semantic_inputs["dependency_proofs_complete"] = False
        case "semantic":
            recorded["semantic_inputs"]["inputs"] = "old_dependencies"
        case "requested":
            case.capture.scope_payload["requested_source_ids"].append("unqualified-source")
        case "mandatory":
            case.capture.affected_scope["source_ids"].append("unqualified-source")
        case "proof":
            recorded["profile_result"]["proof_id"] = "b" * 64
        case "future":
            recorded["profile_result"]["generation"] = case.prepared.execution.generation + 1
    assert reuse._can_reuse(case.prepared, case.event["document"]) is (fault is None)


@pytest.mark.parametrize("fault", ["initial", "incomplete", "legacy", "recovered", "alias"])
async def test_ineligible_state_falls_back_before_consuming_either_lease(case, fault):
    if fault == "initial":
        case.capture.predecessor["kind"] = "initial"
    elif fault == "incomplete":
        case.capture.semantic_inputs["dependency_proofs_complete"] = False
    elif fault == "legacy":
        case.prepared.fhir.db.all.return_value = []
    elif fault == "recovered":
        reuse.recovery._load_history.return_value["recovered"] = case.event
    else:
        reuse.journal.capture_state.return_value["metadata"]["alias"] = {"generation": 99}
    assert (
        await reuse.reuse_current_ordinary_serving(case.prepared, object(), run_id="run", control_run_id="run") is None
    )
    case.admission.before_scratch.assert_not_awaited()
    reuse.preparation._admit_profile_before_full_build.assert_not_awaited()
    assert case.prepared.metrics == {}


@pytest.mark.parametrize("fault", ["state", "rows", "retained", "selection", "history", "capture"])
async def test_proof_drift_after_consumption_is_failure_not_fallback_or_success(case, monkeypatch, fault):
    monkeypatch.setattr(reuse, "_candidate", AsyncMock(return_value=case.event))
    match fault:
        case "state":
            reuse.journal.capture_state.return_value["roots"]["provider_directory_profile"]["oid"] += 1
        case "rows":
            reuse.postimages.capture_postimages.return_value["unified"]["rows"] += 1
        case "retained":
            reuse.observations._retained.side_effect = RuntimeError("retained_changed")
        case "selection":
            case.prepared.fhir.profile_selection_result = lambda *args, **kwargs: {"wrong": "selection"}
        case "history":
            reuse.recovery._load_history.return_value["recovered"] = case.event
        case _:
            case.capture.recheck.side_effect = RuntimeError("capture_changed")
    with pytest.raises(RuntimeError):
        await reuse.reuse_current_ordinary_serving(
            case.prepared, SimpleNamespace(artifact_projection=object()), run_id="run", control_run_id="run"
        )
    case.admission.before_scratch.assert_awaited_once()
    reuse.preparation._admit_profile_before_full_build.assert_awaited_once()
    assert case.prepared.metrics == {}


async def test_reuse_spends_new_leases_and_keeps_original_receipt(case, monkeypatch):
    event_names = []

    async def record(name, *args):
        event_names.append(name)

    case.admission.before_scratch.side_effect = partial(record, "address")
    reuse.preparation._admit_profile_before_full_build.side_effect = partial(record, "profile")
    case.admission.assert_ready.side_effect = partial(record, "ready")
    assert await reuse.reuse_current_ordinary_serving(
        case.prepared, SimpleNamespace(artifact_projection=object()), run_id="new_run", control_run_id="new_run"
    )
    assert event_names == ["address", "profile", "ready", "ready"]
    assert case.prepared.metrics["profile_selection_result"] == case.profile_result
    assert case.profile_result["generation"] < case.prepared.execution.generation
    result = case.prepared.metrics["serving_recovery"]
    assert result["phase"] == "published" and result["outcome"] == "reused"
    assert result["generation"] == case.event["document"]["generation"]
    assert result["journal_sha256"] == case.event["sha256"]
    case.capture.recheck.assert_awaited_once_with(case.prepared.fhir)
    publication._historical_profile_result.assert_awaited_once()


async def test_empty_requested_scope_never_causes_source_autodiscovery(case):
    case.capture.scope_payload["requested_source_ids"] = []
    case.capture.affected_scope["source_ids"] = []
    assert reuse._can_reuse(case.prepared, case.event["document"])
    assert await reuse._candidate(case.prepared) == case.event


@pytest.mark.parametrize("boundary", ["address", "profile", "readiness"])
async def test_refused_new_authority_never_returns_old_success(case, monkeypatch, boundary):
    monkeypatch.setattr(reuse, "_candidate", AsyncMock(return_value=case.event))
    refused = {
        "address": case.admission.before_scratch,
        "profile": reuse.preparation._admit_profile_before_full_build,
        "readiness": case.admission.assert_ready,
    }[boundary]
    refused.side_effect = RuntimeError("new_authority_refused")
    with pytest.raises(RuntimeError, match="new_authority_refused"):
        await reuse.reuse_current_ordinary_serving(
            case.prepared, SimpleNamespace(artifact_projection=object()), run_id="run", control_run_id="run"
        )
    assert case.prepared.metrics == {}


async def test_ambiguous_current_receipts_fail_closed_before_authority(case):
    case.prepared.fhir.db.all.return_value *= 2
    with pytest.raises(RuntimeError, match="reuse_changed"):
        await reuse.reuse_current_ordinary_serving(case.prepared, object(), run_id="run", control_run_id="run")
    case.admission.before_scratch.assert_not_awaited()


async def test_initial_publication_requires_authentic_immutable_receipt(case, monkeypatch):
    monkeypatch.setattr(reuse, "_candidate", AsyncMock(return_value=case.event))
    case.event["document"]["preparation"]["initial_receipt"] = {"build_id": "build", "generation_id": "generation"}
    monkeypatch.setattr(reuse.recovery, "assert_initial_common_receipt", AsyncMock(return_value=False))
    with pytest.raises(RuntimeError, match="reuse_changed"):
        await reuse.reuse_current_ordinary_serving(
            case.prepared, SimpleNamespace(artifact_projection=object()), run_id="run", control_run_id="run"
        )
    assert case.prepared.metrics == {}


@pytest.mark.parametrize("changed", [None, "owner", "oid", "acl", "index_name", "rows", "layout"])
async def test_initial_retention_preserves_original_native_custody(common, monkeypatch, changed):
    document = common.document
    entry = next(item for item in document["preparation"]["retention"]["families"] if item["family"] == "profile")
    original = document["before"]["roots"]["provider_directory_profile"]
    original["indexes"] = [{"oid": 1234, "name": "original_npi"}]
    entry.update(reused_initial=True, oid=original["oid"], name=reuse.receipts.retained_name(original["oid"]))
    document["before"]["initial_targets"] = {"profile_target_oid": original["oid"], "profile_rows": entry["rows"]}
    native = deepcopy(original)
    native["relname"] = entry["name"]
    native["indexes"][0]["name"] = reuse.receipts.retained_name(1234, index=True)
    if changed in {"owner", "oid", "acl"}:
        native[{"owner": "relowner", "oid": "oid", "acl": "acl"}[changed]] = "changed"
    if changed == "index_name":
        native["indexes"][0]["name"] = "unrelated_name"
    context = SimpleNamespace(
        fhir=SimpleNamespace(db=object()),
        driver=SimpleNamespace(fetchval=AsyncMock(return_value=2 if changed == "rows" else entry["rows"])),
    )
    monkeypatch.setattr(reuse.receipts, "catalog", AsyncMock(return_value={entry["name"]: native}))
    monkeypatch.setattr(reuse.retention, "_catalog", AsyncMock(return_value=({}, None)))
    monkeypatch.setattr(
        reuse.retention, "_layout_digest", lambda _: "wrong" if changed == "layout" else entry["layout_sha256"]
    )
    retention_by_field = {"families": [entry]}
    if changed is None:
        await reuse._initial_retention(context, "unit", document, retention_by_field)
    else:
        with pytest.raises((RuntimeError, ValueError)):
            await reuse._initial_retention(context, "unit", document, retention_by_field)


async def test_publisher_skips_all_candidate_and_cutover_work_on_proved_reuse(case, monkeypatch):
    @asynccontextmanager
    async def suppress(_run):
        yield

    fhir = case.prepared.fhir
    fhir._validated_admission_run_id = lambda *args: "run"
    fhir.suppress_control_run_heartbeat_persistence = suppress
    fhir._record_artifact_promotion_metrics = AsyncMock(return_value=case.prepared.metrics)
    monkeypatch.setattr(ordinary, "_current_inputs", AsyncMock(return_value=(case.prepared, object())))
    monkeypatch.setattr(ordinary.preparation, "remaining_build_seconds", AsyncMock(return_value=30))
    monkeypatch.setattr(reuse, "reuse_current_ordinary_serving", AsyncMock(return_value=case.prepared.metrics))
    monkeypatch.setattr(ordinary, "_prepare_current", AsyncMock(side_effect=AssertionError("candidate_work")))
    monkeypatch.setattr(publication, "commit_ordinary_serving", AsyncMock(side_effect=AssertionError("publication")))
    assert (
        await ordinary.publish_current_ordinary_serving(
            fhir, execution=case.prepared.execution, run_id="run", control_run_id="run", metrics=case.prepared.metrics
        )
        is case.prepared.metrics
    )
    ordinary._prepare_current.assert_not_called()
    publication.commit_ordinary_serving.assert_not_awaited()
    fhir._record_artifact_promotion_metrics.assert_awaited_once_with(case.prepared.fence, case.prepared.metrics, 0)
