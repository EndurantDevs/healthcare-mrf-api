# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio
import copy
import hashlib
import json
from contextlib import asynccontextmanager
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import new_york_nysed_profile_acquisition as nysed_acquisition
from process import new_york_profile_binding as binding
from process import new_york_profile_store as module
from process import provider_profile_source_store as shared
from process.new_york_profile_registry import build_acquisition_cohort
from process.new_york_profile_retained import read_held_acquisition
from tests.test_new_york_nysed_profile import (
    PUBLIC_HEADER,
    _profile_body,
)
from tests.test_new_york_nysed_profile import (
    SourceResponse as NysedResponse,
)
from tests.test_new_york_nysed_profile import (
    SourceSession as NysedSession,
)
from tests.test_new_york_profile_acquisition import SourceResponse, _acquire, _education, _search, source_session
from tests.test_new_york_profile_binding import _candidate, _save_snapshot, _snapshot

RUN_ID = "a" * 32


def _artifact(run, cohort, profiles, metrics):
    bundle_by_field = {
        "schema_version": module.SCHEMA_VERSION,
        "run_id": run["run_id"],
        "source_manifest": run["source_manifest"],
        "cohort": cohort,
        "profiles": profiles,
        "acquisition": metrics,
    }
    return {
        "artifact_id": module._hash([run["run_id"], module.SOURCE_KEY]),
        "run_id": run["run_id"],
        "source_key": module.SOURCE_KEY,
        "category": "profile",
        "file_name": "manifest.json",
        "source_url": module.SOURCE_URL,
        "content_sha256": module._hash(bundle_by_field),
        "content_bytes": len(module.encoded_json(bundle_by_field)),
        "header": None,
        "downloaded_at": None,
        "metadata_json": bundle_by_field,
    }


def source_run(cohort, snapshot_sha256, row_count):
    manifest_by_field = {
        "control_run_id": "synthetic-control-run",
        "expected_current_run_id": None,
        "max_providers": None,
        "resume_from": None,
        "categories": list(module.CATEGORIES),
        "snapshot_sha256": snapshot_sha256,
        "snapshot_row_count": row_count,
        "cohort_sha256": module._hash(cohort),
        "full_cohort_licenses": 3,
        "requested_licenses": 3,
        "source": {
            "source_key": module.SOURCE_KEY,
            "source_kind": "state_regulator",
            "jurisdiction": "NY",
            "agency": "New York State Department of Health",
            "source_url": module.SOURCE_URL,
            "coverage_scope": module.COVERAGE_SCOPE,
            "registry_generation": snapshot_sha256,
        },
    }
    run_by_field = {
        "run_id": RUN_ID,
        "source_key": module.SOURCE_KEY,
        "schema_version": module.SCHEMA_VERSION,
        "jurisdiction": "NY",
        "status": "running",
        "started_at": shared._now(),
        "source_manifest": manifest_by_field,
    }
    return run_by_field


async def _capture_nysed_support(tmp_path, monkeypatch, license_number):
    destination = tmp_path / ("nysed-" + license_number)
    is_held = license_number == "222222"
    response = (
        NysedResponse(raw=b"", status=204, headers=[])
        if is_held
        else NysedResponse(_profile_body(license_number, name="EXAMPLE ALEX"))
    )
    session = NysedSession(response)
    monkeypatch.setattr(nysed_acquisition.aiohttp, "ClientSession", lambda **_options: session)
    acquired = await nysed_acquisition.acquire_license(
        license_number, destination, run_id=RUN_ID, api_key=PUBLIC_HEADER
    )
    reader = module.nysed.read_held_acquisition if is_held else module.nysed.read_acquisition
    replayed = reader(destination, receipt_sha256=acquired["receipt_sha256"])
    assert session.closed and replayed == acquired
    identity_by_field = None
    if not is_held:
        nysed_record = replayed["source_record"]
        identity_by_field = {
            "source_record_id": nysed_record["record_id"],
            "artifact_id": nysed_record["artifact_id"],
            **{
                field: replayed["facts"][0]["source_json"][field]
                for field in (
                    "source_key",
                    "source_url",
                    "content_sha256",
                    "downloaded_at",
                )
            },
            "profession_code": nysed_record["profession_code"],
            "license_number": nysed_record["license_number"],
            "legal_name": nysed_record["raw_payload"]["name"]["value"],
        }
    return destination, {
        "capture_manifest": json.loads((destination / "manifest.json").read_bytes()),
        "file_sha256": {path.name: hashlib.sha256(path.read_bytes()).hexdigest() for path in destination.iterdir()},
        "receipt": json.loads((destination / "result.json").read_bytes()),
        "receipt_sha256": acquired["receipt_sha256"],
        "source_identity": identity_by_field,
    }


async def _capture_profile(tmp_path, source_session, snapshot, root, monkeypatch):
    license_number = root["license_number"]
    is_held = license_number == "333333"
    responses = (
        [SourceResponse(_search(total=0))]
        if is_held
        else [SourceResponse(_search()), SourceResponse(_education(license_number))]
    )
    session = source_session(*responses)
    session.destination = tmp_path / license_number
    await _acquire(session, license_number, run_id=RUN_ID)
    file_hashes_by_name = {
        path.name: hashlib.sha256(path.read_bytes()).hexdigest() for path in session.destination.iterdir()
    }
    capture_manifest = json.loads((session.destination / "manifest.json").read_bytes())
    options_by_field = {
        "manifest_sha256": file_hashes_by_name["manifest.json"],
        "acquisition_sha256": module._hash(file_hashes_by_name),
    }
    support = None
    if is_held:
        bound = read_held_acquisition(session.destination, **options_by_field)
    else:
        destination, support = await _capture_nysed_support(tmp_path, monkeypatch, license_number)
        bound = (
            snapshot.bind_retained_acquisition(session.destination, **options_by_field)
            if support["source_identity"] is None
            else snapshot.bind_corroborated_acquisition(
                session.destination,
                **options_by_field,
                nysed_destination=destination,
                nysed_receipt_sha256=support["receipt_sha256"],
            )
        )
    descriptor, source_record, captured_facts = module.prepare_profile(
        RUN_ID, root, bound, capture_manifest=capture_manifest, file_sha256=file_hashes_by_name
    )
    return (root, bound, capture_manifest, file_hashes_by_name), descriptor, source_record, captured_facts, support


@pytest.fixture
async def captured_case(tmp_path, source_session, monkeypatch):
    candidates = [_candidate(license_number=license_number) for license_number in ("111111", "222222", "333333")]
    candidates.append(_candidate(license_number="222222", entity_type_code=2))
    saved = _save_snapshot(tmp_path, _snapshot(candidates))
    cohort = build_acquisition_cohort(**saved)
    snapshot = binding.RegistrySnapshot(
        **{("path" if key == "snapshot_path" else key): val for key, val in saved.items()}
    )
    run_by_field = source_run(cohort, saved["snapshot_sha256"], len(candidates))
    profiles, source_records, facts, inputs, nysed_support = {}, [], [], [], {}
    for root in cohort["roots"]:
        original_input, descriptor, source_record, captured_facts, support = await _capture_profile(
            tmp_path, source_session, snapshot, root, monkeypatch
        )
        inputs.append(original_input)
        license_number = root["license_number"]
        profiles[license_number] = descriptor
        if source_record is not None:
            nysed_support[license_number] = support
            source_records.append(source_record)
            facts.extend(captured_facts)
    metrics_by_field = {
        "acquisition_complete": True,
        "transport_failures": 0,
        "responses": 3,
        "acquired_profiles": 2,
        "held_attempts": 1,
        "facts": 2,
        "cohort_sha256": run_by_field["source_manifest"]["cohort_sha256"],
        "nysed_support": nysed_support,
    }
    return SimpleNamespace(
        run=run_by_field,
        cohort=cohort,
        profiles=profiles,
        records=source_records,
        facts=facts,
        metrics=metrics_by_field,
        artifact=_artifact(run_by_field, cohort, profiles, metrics_by_field),
        inputs=inputs,
    )


def _counts():
    return {
        "retained_source_records": 2,
        "received_profiles": 2,
        "retained_facts": 2,
        "matched_public_providers": 1,
        "portfolio_only_public_providers": 0,
        "invalid_source_records": 0,
        "invalid_facts": 0,
        "foreign_artifacts": 0,
    }


def _install_retained(monkeypatch, case, *, cancel=False):
    closed_streams = []

    class Rows:
        def __init__(self, table):
            self.rows = case.records if table is shared.ProviderProfileSourceRecord.__table__ else case.facts

        def where(self, *_conditions):
            return self

        async def iterate(self):
            try:
                for source_record in self.rows:
                    if cancel:
                        raise asyncio.CancelledError
                    yield SimpleNamespace(_mapping=copy.deepcopy(source_record))
            finally:
                closed_streams.append(True)

    monkeypatch.setattr(shared.SourceProfileStore, "retained_counts", AsyncMock(return_value=_counts()))
    monkeypatch.setattr(type(module.store), "_read_run", AsyncMock(return_value=case.run))
    monkeypatch.setattr(shared.db, "all", AsyncMock(return_value=[SimpleNamespace(_mapping=case.artifact)]))
    monkeypatch.setattr(shared.db, "select", Rows)
    return closed_streams


async def test_complete_mixed_inventory_retains_real_unmatched_rows_without_fake_held_profiles(
    captured_case, monkeypatch
):
    case = captured_case
    before = copy.deepcopy(case.inputs)
    _install_retained(monkeypatch, case)
    counts = await module.store.retained_counts(RUN_ID)
    final = module.store._completion_metrics(case.run, case.metrics, counts)
    assert final["requested_licenses"] == 3 and final["received_profiles"] == 2 and final["held_attempts"] == 1
    assert [record["matched_npi"] for record in case.records] == [1000000004, None]
    assert [fact["npi"] for fact in case.facts] == [1000000004, None]
    assert case.profiles["333333"]["record_id"] is None and case.profiles["333333"]["facts"] == {}
    assert case.inputs == before
    for record, fact in zip(case.records, case.facts, strict=True):
        original = record["normalized_payload"]["profile_capture"]
        assert record["artifact_id"] == case.artifact["artifact_id"] != original["artifact_id"]
        assert fact["source_json"]["artifact_id"] == record["artifact_id"]
        assert fact["source_json"]["capture_artifact_id"] == original["artifact_id"]
    assert module.completion.store is module.store and module.completion.importer == module.IMPORTER
    supports = case.metrics["nysed_support"]
    assert set(supports) == {"111111", "222222"}
    assert supports["111111"]["receipt"]["outcome"] == "acquired"
    assert supports["222222"]["receipt"]["outcome"] == "held"
    assert supports["222222"]["source_identity"] is None
    assert PUBLIC_HEADER.encode() not in module.encoded_json(case.artifact)
    assert all(record["source_key"] == module.SOURCE_KEY for record in case.records)


async def test_completion_keeps_support_in_artifact_and_returns_only_aggregate_metrics(captured_case, monkeypatch):
    case = captured_case
    before = copy.deepcopy(case.artifact)
    _install_retained(monkeypatch, case)
    counts = await module.store.retained_counts(RUN_ID)
    final = module.store._completion_metrics(case.run, case.metrics, counts)
    assert "nysed_support" not in final and "bundle_metrics" not in final
    assert set(case.artifact["metadata_json"]["acquisition"]["nysed_support"]) == {"111111", "222222"}
    assert case.artifact == before
    assert final["facts"] == final["retained_facts"] == 2
    modified_metrics = copy.deepcopy(case.metrics)
    modified_metrics["nysed_support"]["111111"]["receipt_sha256"] = "0" * 64
    with pytest.raises(RuntimeError, match="retained_count_mismatch"):
        module.store._completion_metrics(case.run, modified_metrics, counts)


def _refresh_artifact(case):
    case.artifact["content_sha256"] = module._hash(case.artifact["metadata_json"])
    case.artifact["content_bytes"] = len(module.encoded_json(case.artifact["metadata_json"]))


@pytest.mark.parametrize("failure", ["absent", "missing_acquired", "missing_held", "extra", "substituted"])
async def test_support_inventory_covers_acquired_profiles_exactly(captured_case, failure):
    case = captured_case
    supports = case.metrics["nysed_support"]
    if failure == "absent":
        del case.metrics["nysed_support"]
    if failure == "missing_acquired":
        del supports["111111"]
    if failure == "missing_held":
        del supports["222222"]
    if failure == "extra":
        supports["333333"] = copy.deepcopy(supports["222222"])
    if failure == "substituted":
        supports["111111"] = copy.deepcopy(supports["222222"])
    _refresh_artifact(case)
    with pytest.raises(ValueError, match="nysed_"):
        module.store._bundle(case.run, [case.artifact])


@pytest.mark.parametrize(
    "path,replacement",
    [
        (("capture_manifest", "run_id"), "b" * 32),
        (("capture_manifest", "source_key"), module.SOURCE_KEY),
        (("capture_manifest", "profession_code"), "061"),
        (("capture_manifest", "license_number"), "999999"),
        (("file_sha256", "manifest.json"), "0" * 64),
        (("file_sha256", "request.json"), "0" * 64),
        (("file_sha256", "response.json"), "0" * 64),
        (("file_sha256", "result.json"), "0" * 64),
        (("receipt_sha256",), "0" * 64),
        (("receipt", "outcome"), "failed"),
        (("receipt", "fact_count"), 0),
        (("receipt", "fact_count"), True),
        (("receipt", "completed_at"), "2000-01-01T00:00:00+00:00"),
        (("source_identity", "source_record_id"), "0" * 64),
        (("source_identity", "artifact_id"), "0" * 64),
        (("source_identity", "source_key"), module.SOURCE_KEY),
        (("source_identity", "source_url"), module.SOURCE_URL),
        (("source_identity", "profession_code"), "061"),
        (("source_identity", "license_number"), "999999"),
        (("source_identity", "legal_name"), ""),
        (("source_identity", "downloaded_at"), "2000-01-01T00:00:00+00:00"),
        (("source_identity", "content_sha256"), "invalid"),
        (("source_identity",), None),
        (("unexpected",), "synthetic-extra"),
    ],
)
async def test_support_validates_capture_receipt_and_parser_identity(captured_case, path, replacement):
    case = captured_case
    support = case.metrics["nysed_support"]["111111"]
    parent = support if len(path) == 1 else support[path[0]]
    parent[path[-1]] = replacement
    if path[0] == "receipt":
        support["receipt_sha256"] = support["file_sha256"]["result.json"] = module._hash(support["receipt"])
    _refresh_artifact(case)
    with pytest.raises(ValueError, match="nysed_"):
        module.store._bundle(case.run, [case.artifact])


@pytest.mark.parametrize("failure", ["files_missing", "files_extra", "reason", "facts", "identity"])
async def test_empty_nysed_support_keeps_exact_four_files_without_source_identity(captured_case, failure):
    case = captured_case
    support = case.metrics["nysed_support"]["222222"]
    if failure == "files_missing":
        del support["file_sha256"]["response.json"]
    if failure == "files_extra":
        support["file_sha256"]["extra.json"] = "0" * 64
    if failure == "reason":
        support["receipt"]["reason"] = "transport_error"
    if failure == "facts":
        support["receipt"]["fact_count"] = 1
    if failure == "identity":
        support["source_identity"] = copy.deepcopy(case.metrics["nysed_support"]["111111"]["source_identity"])
    support["receipt_sha256"] = support["file_sha256"]["result.json"] = module._hash(support["receipt"])
    _refresh_artifact(case)
    with pytest.raises(ValueError, match="nysed_"):
        module.store._bundle(case.run, [case.artifact])


def _substitute_corroboration_receipt(case):
    case.records[0]["match_evidence"]["registry_binding"]["nysed_corroboration"]["receipt_sha256"] = "0" * 64
    case.profiles["111111"]["record_sha256"] = module._hash(case.records[0])
    _refresh_artifact(case)


@pytest.mark.parametrize(
    "field,replacement",
    [(field, "0" * 64) for field in ("receipt_sha256", *module.NYSED_IDENTITY_FIELDS)]
    + [("license_matches", False), ("header_legal_name_matches", False), ("license_matches", 1)],
)
async def test_sql_corroboration_must_match_support_even_with_rehashed_row(
    captured_case, monkeypatch, field, replacement
):
    case = captured_case
    case.records[0]["match_evidence"]["registry_binding"]["nysed_corroboration"][field] = replacement
    case.profiles["111111"]["record_sha256"] = module._hash(case.records[0])
    _refresh_artifact(case)
    _install_retained(monkeypatch, case)
    counts = await module.store.retained_counts(RUN_ID)
    assert counts["invalid_bundle_records"] == 1
    with pytest.raises(RuntimeError, match="retained_integrity_invalid"):
        module.store._completion_metrics(case.run, case.metrics, counts)


@pytest.mark.parametrize("failure", ["absent", "strict", "held_corroboration", "held_method"])
async def test_support_outcome_controls_binding_method(captured_case, monkeypatch, failure):
    case = captured_case
    record = case.records[1 if failure.startswith("held") else 0]
    evidence = record["match_evidence"]["registry_binding"]
    if failure == "absent":
        del evidence["nysed_corroboration"]
    if failure == "strict":
        evidence["method"] = "exact_ny_license_name_components"
    if failure == "held_corroboration":
        evidence["nysed_corroboration"] = copy.deepcopy(
            case.records[0]["match_evidence"]["registry_binding"]["nysed_corroboration"]
        )
    if failure == "held_method":
        evidence["method"] = binding.CORROBORATED_METHOD
    case.profiles[record["license_number"]]["record_sha256"] = module._hash(record)
    _refresh_artifact(case)
    _install_retained(monkeypatch, case)
    counts = await module.store.retained_counts(RUN_ID)
    assert counts["invalid_bundle_records"] == 1


@pytest.mark.parametrize(
    "changes",
    [
        {"max_providers": 1},
        {"resume_from": "b" * 32},
        {"requested_licenses": 2},
        {"categories": ["education"]},
        {"control_run_id": ""},
        {"snapshot_sha256": "invalid"},
        {"snapshot_row_count": True},
        {"snapshot_row_count": 2},
        {"source": {}},
        {"extra": True},
    ],
)
async def test_manifest_freezes_full_supported_source_scope(captured_case, changes):
    captured_case.run["source_manifest"].update(changes)
    with pytest.raises(ValueError):
        module.store._manifest(captured_case.run)


@pytest.mark.parametrize(
    "failure",
    [
        "missing",
        "extra",
        "capture_pin",
        "session",
        "held_total",
        "held_record",
        "duplicate_index",
        "count",
        "bundle_hash",
    ],
)
async def test_bundle_requires_every_exact_capture_and_full_cohort(captured_case, failure):
    case = captured_case
    bundle = case.artifact["metadata_json"]
    if failure == "missing":
        del bundle["profiles"]["333333"]
    if failure == "extra":
        bundle["profiles"]["444444"] = copy.deepcopy(bundle["profiles"]["333333"])
    if failure == "capture_pin":
        bundle["profiles"]["111111"]["file_sha256"]["search.response.json"] = "0" * 64
    if failure == "session":
        bundle["profiles"]["111111"]["capture_manifest"]["session_id"] = "0" * 32
    if failure == "held_total":
        bundle["profiles"]["333333"]["reported_total"] = 1
    if failure == "held_record":
        bundle["profiles"]["333333"]["record_id"] = "0" * 64
    if failure == "duplicate_index":
        bundle["cohort"]["roots"][1]["registry_occurrence_indexes"] = [0]
        bundle["source_manifest"]["cohort_sha256"] = module._hash(bundle["cohort"])
    if failure == "count":
        bundle["cohort"]["summary"]["selected_row_count"] -= 1
        bundle["source_manifest"]["cohort_sha256"] = module._hash(bundle["cohort"])
    case.artifact["content_sha256"] = "0" * 64 if failure == "bundle_hash" else module._hash(bundle)
    case.artifact["content_bytes"] = len(module.encoded_json(bundle))
    with pytest.raises(ValueError):
        module.store._bundle(case.run, [case.artifact])


@pytest.mark.parametrize(
    "kind,field,replacement",
    [
        ("record", "raw_payload", {}),
        ("record", "normalized_payload", {}),
        ("record", "matched_npi", None),
        ("record", "license_number", "999999"),
        ("record", "match_evidence", {}),
        ("record", "artifact_id", "0" * 64),
        ("fact", "value_json", {}),
        ("fact", "source_json", {}),
        ("fact", "npi", None),
        ("fact", "source_record_id", "0" * 64),
        ("fact", "published_at", datetime(2026, 1, 1)),
    ],
)
async def test_actual_retained_content_must_match_bundle_hashes(captured_case, monkeypatch, kind, field, replacement):
    case = captured_case
    (case.records if kind == "record" else case.facts)[0][field] = replacement
    closed = _install_retained(monkeypatch, case)
    counts = await module.store.retained_counts(RUN_ID)
    assert counts["invalid_bundle_records" if kind == "record" else "invalid_bundle_facts"] > 0
    with pytest.raises(RuntimeError, match="retained_integrity_invalid"):
        module.store._completion_metrics(case.run, case.metrics, counts)
    assert len(closed) == 2


@pytest.mark.parametrize("kind", ["record", "fact", "duplicate_fact", "fake_held_record"])
async def test_missing_or_extra_sql_inventory_is_rejected(captured_case, monkeypatch, kind):
    case = captured_case
    if kind == "record":
        case.records.pop()
    if kind == "fact":
        case.facts.pop()
    if kind == "duplicate_fact":
        case.facts.append(copy.deepcopy(case.facts[0]))
    if kind == "fake_held_record":
        case.records.append({**case.records[0], "record_id": "0" * 64, "license_number": "333333"})
    _install_retained(monkeypatch, case)
    with pytest.raises(RuntimeError, match="retained_integrity_invalid"):
        module.store._completion_metrics(case.run, case.metrics, await module.store.retained_counts(RUN_ID))


@pytest.mark.parametrize(
    "changes",
    [
        {"acquisition_complete": False},
        {"transport_failures": 1},
        {"transport_failures": False},
        {"responses": 2},
        {"responses": True},
        {"acquired_profiles": 1},
        {"held_attempts": 0},
        {"facts": 1},
        {"cohort_sha256": "0" * 64},
    ],
)
async def test_declared_metrics_cannot_replace_actual_inventory(captured_case, monkeypatch, changes):
    case = captured_case
    _install_retained(monkeypatch, case)
    counts = await module.store.retained_counts(RUN_ID)
    metrics_by_field = {**case.metrics, **changes}
    counts["bundle_metrics"] = metrics_by_field
    with pytest.raises(RuntimeError):
        module.store._completion_metrics(case.run, metrics_by_field, counts)


async def test_prepare_rejects_run_id_rewriting_and_keeps_nysed_corroboration(captured_case):
    root, bound, manifest, files = copy.deepcopy(captured_case.inputs[0])
    with pytest.raises(ValueError, match="capture_identity_changed"):
        module.prepare_profile("b" * 32, root, bound, capture_manifest=manifest, file_sha256=files)
    _, record, facts = module.prepare_profile(RUN_ID, root, bound, capture_manifest=manifest, file_sha256=files)
    assert (
        record["match_evidence"]["registry_binding"]["nysed_corroboration"]
        == bound["source_record"]["match_evidence"]["registry_binding"]["nysed_corroboration"]
    )
    assert all(fact["source_json"]["source_key"] == module.SOURCE_KEY for fact in facts)


async def test_cancelled_inventory_scan_closes_stream_and_cannot_publish(captured_case, monkeypatch):
    closed = _install_retained(monkeypatch, captured_case, cancel=True)
    completed = AsyncMock()
    monkeypatch.setattr(type(module.store), "_complete_run", completed)
    with pytest.raises(asyncio.CancelledError):
        await module.store.retained_counts(RUN_ID)
    assert closed == [True]
    completed.assert_not_awaited()


def test_volume_refuses_empty_publication_and_uses_existing_relative_guard():
    baseline_by_field = {"acquired_profiles": 100, "retained_facts": 100, "matched_public_providers": 100}
    module.store._publication_volume(baseline_by_field, None)
    module.store._publication_volume({key: 80 for key in baseline_by_field}, baseline_by_field)
    for field in baseline_by_field:
        with pytest.raises(RuntimeError, match="empty_publication"):
            module.store._publication_volume({**baseline_by_field, field: 0}, None)
        with pytest.raises(RuntimeError, match="volume_drop:" + field):
            module.store._publication_volume({**baseline_by_field, field: 79}, baseline_by_field)
