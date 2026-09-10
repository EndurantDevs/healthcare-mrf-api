# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Validate Tennessee bulk completeness and source-scoped publication policy."""

import hashlib
import importlib
from copy import deepcopy
from dataclasses import FrozenInstanceError
from unittest.mock import AsyncMock

import pytest

from process import provider_profile_source_store as shared_store
from process import tennessee_profile_binding as binding
from process import tennessee_profile_store as tennessee
from process.massachusetts_profile_acquisition import encoded_json
from tests.test_massachusetts_profile_store import _database, _guard_database
from tests.test_tennessee_profile_binding import _reports, _snapshot

worker = importlib.import_module("process.tennessee_profile")


def _run():
    return {
        "run_id": "1" * 64,
        "source_key": tennessee.SOURCE_KEY,
        "jurisdiction": "TN",
        "schema_version": tennessee.SCHEMA_VERSION,
        "status": "running",
        "started_at": shared_store._now(),
        "source_manifest": {
            "control_run_id": "run_tennessee_synthetic",
            "expected_current_run_id": None,
            "max_providers": None,
            "resume_from": None,
            "categories": list(tennessee.CATEGORIES),
            "report_professions": list(tennessee.PROFESSIONS),
            "snapshot_sha256": "a" * 64,
            "snapshot_row_count": 12,
            "source": {
                "source_key": tennessee.SOURCE_KEY,
                "source_kind": "state_regulator",
                "jurisdiction": "TN",
                "agency": "Tennessee Department of Health",
                "source_url": tennessee.SOURCE_URL,
                "coverage_scope": tennessee.COVERAGE_SCOPE,
                "registry_generation": "a" * 64,
            },
        },
    }


def _report_inputs(run_id="1" * 64):
    artifact_id = hashlib.sha256(encoded_json([run_id, tennessee.SOURCE_KEY])).hexdigest()
    return {
        profession: {
            "content": profession.encode(),
            "evidence": {
                "run_id": run_id,
                "artifact_id": artifact_id,
                "content_sha256": hashlib.sha256(profession.encode()).hexdigest(),
                "source_url": tennessee.SOURCE_URL + "/synthetic.csv",
                "downloaded_at": "2026-09-10T00:00:00+00:00",
            },
        }
        for profession in tennessee.PROFESSIONS
    }


def _metrics():
    return {
        "acquisition_complete": True,
        "transport_failures": 0,
        "http_responses": 8,
        "report_responses": 2,
        "reports_sha256": binding.reports_content_sha256(_report_inputs()),
        "snapshot_sha256": "a" * 64,
        "response_bytes": 168129657,
        "source_records_by_profession": {"1606": 70018, "1907": 7030},
        "facts_by_profession": {"1606": 258427, "1907": 22525},
    }


def _artifact(run_by_field=None, metrics_by_field=None):
    run_by_field = run_by_field or _run()
    metrics_by_field = metrics_by_field or _metrics()
    bundle_by_field = {
        "schema_version": tennessee.SCHEMA_VERSION,
        "run_id": run_by_field["run_id"],
        "source_manifest": deepcopy(run_by_field["source_manifest"]),
        "reports_sha256": metrics_by_field["reports_sha256"],
        "snapshot": {"content_sha256": "a" * 64, "content_bytes": 1024, "file_name": "snapshot.json"},
        "reports": {
            profession: {
                **{field: report["evidence"][field] for field in ("content_sha256", "source_url", "downloaded_at")},
                "content_bytes": len(report["content"]),
                "file_name": profession + ".csv",
            }
            for profession, report in _report_inputs(run_by_field["run_id"]).items()
        },
        "acquisition": deepcopy(metrics_by_field),
    }
    canonical = encoded_json(bundle_by_field)
    return {
        "artifact_id": hashlib.sha256(encoded_json([run_by_field["run_id"], tennessee.SOURCE_KEY])).hexdigest(),
        "run_id": run_by_field["run_id"],
        "source_key": tennessee.SOURCE_KEY,
        "category": "profile",
        "file_name": "manifest.json",
        "source_url": tennessee.SOURCE_URL,
        "metadata_json": bundle_by_field,
        "content_sha256": hashlib.sha256(canonical).hexdigest(),
        "content_bytes": len(canonical),
    }


def _counts():
    metrics_by_field = _metrics()
    return {
        "retained_source_records": 77048,
        "received_profiles": 77048,
        "retained_facts": 280952,
        "matched_public_providers": 9894,
        "portfolio_only_public_providers": 84,
        "invalid_source_records": 0,
        "invalid_facts": 0,
        "foreign_artifacts": 0,
        "invalid_bundle_records": 0,
        "invalid_report_facts": 0,
        "retained_artifacts": 1,
        "md_source_records": 70018,
        "do_source_records": 7030,
        "md_facts": 258427,
        "do_facts": 22525,
        "source_records_by_profession": metrics_by_field["source_records_by_profession"],
        "facts_by_profession": metrics_by_field["facts_by_profession"],
        **tennessee.store._bundle_counts(_run(), [_artifact()]),
    }


def test_source_policy_uses_shared_lifecycle_without_mutable_configuration():
    assert tennessee.completion.store is tennessee.store
    assert tennessee.completion.importer == "tennessee-tdh-profile"
    assert isinstance(tennessee.store, shared_store.SourceProfileStore)
    with pytest.raises(FrozenInstanceError):
        tennessee.store.policy.source_key = "florida-mqa"
    assert tennessee.store._manifest(_run())["snapshot_row_count"] == 12
    progress = tennessee.completion._terminal_progress({"retained_source_records": 77048, "published": True})
    assert progress == {
        "unit": "record",
        "done": 77048,
        "total": 77048,
        "pct": 100,
        "phase": "tennessee-tdh-profile published",
        "message": "succeeded",
    }


@pytest.mark.parametrize(
    "changes",
    [
        {"max_providers": 1},
        {"max_providers": False},
        {"resume_from": "2" * 64},
        {"categories": ["education"]},
        {"report_professions": ["1606"]},
        {"report_professions": ["1907", "1606"]},
        {"snapshot_sha256": "invalid"},
        {"snapshot_row_count": -1},
        {"snapshot_row_count": True},
        {"expected_current_run_id": "../foreign"},
        {"control_run_id": " "},
        {"requested_licenses": 2},
        {"full_cohort_licenses": 77048},
    ],
)
def test_manifest_rejects_partial_scope_or_unbound_capture(changes):
    candidate = _run()
    candidate["source_manifest"].update(changes)
    with pytest.raises(ValueError, match="tennessee_profile_"):
        tennessee.store._manifest(candidate)


@pytest.mark.parametrize("manifest", [None, {}, {"source": None}])
def test_manifest_requires_complete_fixed_fields(manifest):
    with pytest.raises(ValueError, match="manifest_invalid"):
        tennessee.store._manifest({"source_manifest": manifest})


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("source_key", "florida-mqa"),
        ("source_kind", "cms_doctors"),
        ("jurisdiction", "MA"),
        ("agency", "Other board"),
        ("source_url", "https://example.test"),
        ("coverage_scope", "physicians_only"),
        ("registry_generation", "f" * 64),
    ],
)
def test_manifest_descriptor_cannot_change_source_or_registry(field, value):
    candidate = _run()
    candidate["source_manifest"]["source"][field] = value
    with pytest.raises(ValueError, match="manifest_source_invalid"):
        tennessee.store._manifest(candidate)


async def test_partial_manifest_cannot_claim_or_complete_through_shared_lifecycle(monkeypatch):
    candidate = _run()
    candidate["source_manifest"]["max_providers"] = 1
    with _guard_database(monkeypatch) as database:
        with pytest.raises(ValueError, match="partial_scope_forbidden"):
            await tennessee.store.claim_run(candidate)
        database.first.assert_not_called()
        database.scalar.assert_not_called()
    with _guard_database(monkeypatch, [_run()]):
        with pytest.raises(RuntimeError, match="bounded_completion_invalid"):
            await tennessee.store.finish_unpublished_run(_run()["run_id"], _metrics())


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("acquisition_complete", False),
        ("acquisition_complete", 1),
        ("transport_failures", 1),
        ("transport_failures", False),
        ("http_responses", 7),
        ("http_responses", True),
        ("report_responses", 1),
        ("response_bytes", 0),
        ("response_bytes", True),
        ("reports_sha256", "f" * 64),
        ("snapshot_sha256", "e" * 64),
        ("source_records_by_profession", {"1606": 77048}),
        ("facts_by_profession", {"1606": 258427, "1907": True}),
    ],
)
def test_completion_rejects_incomplete_or_changed_acquisition(field, value):
    metrics_by_field = _metrics()
    metrics_by_field[field] = value
    with pytest.raises(RuntimeError, match="tennessee_profile_"):
        tennessee.store._completion_metrics(_run(), metrics_by_field, _counts())


@pytest.mark.parametrize(
    "field",
    [
        "invalid_source_records",
        "invalid_facts",
        "foreign_artifacts",
        "invalid_bundle_artifacts",
        "invalid_bundle_records",
        "invalid_report_facts",
        "retained_source_records",
        "received_profiles",
        "retained_facts",
    ],
)
def test_completion_uses_retained_integrity_counts(field):
    counts = _counts()
    counts[field] += 1
    with pytest.raises(RuntimeError, match="retained_integrity_invalid"):
        tennessee.store._completion_metrics(_run(), _metrics(), counts)


def test_completion_separates_report_counts_and_uses_actual_coverage():
    metrics_by_field = {**_metrics(), "matched_public_providers": 999999}
    completed = tennessee.store._completion_metrics(_run(), metrics_by_field, _counts())
    assert completed["http_responses"] == 8 and completed["report_responses"] == 2
    assert completed["retained_source_records"] == 77048 and completed["matched_public_providers"] == 9894
    assert "requested_licenses" not in completed and "full_cohort_licenses" not in completed


@pytest.mark.parametrize(
    ("field", "minimum"),
    [
        ("md_source_records", 60000),
        ("do_source_records", 6000),
        ("matched_public_providers", 8000),
    ],
)
def test_each_first_publication_floor_is_required(field, minimum):
    counts = _counts()
    counts[field] = minimum
    tennessee.store._publication_volume(counts, None)
    counts[field] -= 1
    with pytest.raises(RuntimeError, match="first_publication_too_small"):
        tennessee.store._publication_volume(counts, None)


@pytest.mark.parametrize(
    "field", ["md_source_records", "do_source_records", "matched_public_providers", "received_profiles"]
)
def test_each_incumbent_guard_preserves_eighty_percent(field):
    incumbent = _counts()
    incumbent[field] = 10000
    counts = _counts()
    counts[field] = 8000
    tennessee.store._publication_volume(counts, incumbent)
    counts[field] -= 1
    with pytest.raises(RuntimeError, match="publication_volume_drop:" + field):
        tennessee.store._publication_volume(counts, incumbent)


@pytest.mark.parametrize(
    "change", ["missing", "duplicate", "wrong_id", "changed_metadata", "missing_do", "same_file", "snapshot"]
)
def test_bundle_requires_one_artifact_and_both_pinned_reports(change):
    artifact = _artifact()
    artifacts = [artifact]
    bundle_by_field = artifact["metadata_json"]
    match change:
        case "missing":
            artifacts = []
        case "duplicate":
            artifacts.append(deepcopy(artifact))
        case "wrong_id":
            artifact["artifact_id"] = "e" * 64
        case "changed_metadata":
            bundle_by_field["reports"]["1606"]["content_sha256"] = "f" * 64
        case "missing_do":
            del bundle_by_field["reports"]["1907"]
        case "same_file":
            bundle_by_field["reports"]["1907"]["file_name"] = bundle_by_field["reports"]["1606"]["file_name"]
        case "snapshot":
            bundle_by_field["snapshot"]["content_sha256"] = "f" * 64
    if change in {"missing_do", "same_file", "snapshot"}:
        artifact["content_sha256"] = hashlib.sha256(encoded_json(bundle_by_field)).hexdigest()
        artifact["content_bytes"] = len(encoded_json(bundle_by_field))
    assert tennessee.store._bundle_counts(_run(), artifacts)["invalid_bundle_artifacts"] == 1


@pytest.mark.parametrize(
    ("field", "changed"),
    [
        ("content_sha256", "e" * 64),
        ("source_url", "https://example.test/changed.csv"),
        ("downloaded_at", "2026-09-11T00:00:00+00:00"),
    ],
)
def test_rehashed_bundle_cannot_change_report_identity_while_keeping_old_pair_pin(field, changed):
    artifact = _artifact()
    bundle_by_field = artifact["metadata_json"]
    bundle_by_field["reports"]["1907"][field] = changed
    artifact["content_sha256"] = hashlib.sha256(encoded_json(bundle_by_field)).hexdigest()
    artifact["content_bytes"] = len(encoded_json(bundle_by_field))
    assert tennessee.store._bundle_counts(_run(), [artifact])["invalid_bundle_artifacts"] == 1


async def test_retention_keeps_corrupt_payload_but_accepts_already_pruned_audit(monkeypatch):
    counts = _counts()
    counts["invalid_report_facts"] = 1
    probe = AsyncMock(return_value=counts)
    monkeypatch.setattr(tennessee.TennesseeProfileStore, "retained_counts", probe)
    with pytest.raises(RuntimeError, match="retention_foreign_payload"):
        await tennessee.store._assert_source_ownership([_run()["run_id"]])
    probe.return_value = {**counts, "retained_source_records": 0, "retained_facts": 0, "retained_artifacts": 0}
    await tennessee.store._assert_source_ownership([_run()["run_id"]])


def _native_fact(candidate, artifact, record_id, npi, profession):
    """Build a public synthetic fact bound to its report descriptor."""
    report = artifact["metadata_json"]["reports"][profession]
    fact_by_field = {
        "fact_id": record_id,
        "run_id": candidate["run_id"],
        "npi": npi,
        "source_record_id": record_id,
        "logical_fact_key": record_id,
        "category": "education",
        "fact_type": "education_history",
        "display": "Synthetic School",
        "value_json": {"institution": "Synthetic School"},
        "availability": "available",
        "assertion_type": "source_reported",
        "verification_status": "not_independently_verified",
        "source_json": {
            "source_key": tennessee.SOURCE_KEY,
            "schema_version": tennessee.SCHEMA_VERSION,
            "source_record_id": record_id,
            "run_id": candidate["run_id"],
            "artifact_id": artifact["artifact_id"],
            "agency": "Tennessee Department of Health",
            "jurisdiction": "TN",
            **{field: report[field] for field in ("content_sha256", "source_url", "downloaded_at")},
        },
        "sensitive": False,
        "public_default": True,
    }
    return fact_by_field


async def _seed_native(database):
    """Retain mixed public, held and empty records for native integrity checks."""
    candidate = _run()
    metrics_by_field = {
        **_metrics(),
        "source_records_by_profession": {"1606": 3, "1907": 1},
        "facts_by_profession": {"1606": 1, "1907": 1},
    }
    artifact = _artifact(candidate, metrics_by_field)
    await tennessee.store.claim_run(candidate)
    await database.insert(shared_store.ProviderProfileArtifact.__table__).values(artifact).status()
    for number, profession, visibility in (
        (1, "1606", "public"),
        (2, "1606", "held_identity"),
        (3, "1606", "public"),
        (4, "1907", "public"),
    ):
        record_id = str(number) * 64
        npi = 1000000004 if number == 1 else 1000000012 if number == 4 else None
        source_by_field = {
            "record_id": record_id,
            "run_id": candidate["run_id"],
            "artifact_id": artifact["artifact_id"],
            "source_key": tennessee.SOURCE_KEY,
            "source_record_key": f"tennessee-tdh:{profession}:{number}",
            "profession_code": profession,
            "license_number": None if number == 2 else str(number),
            "raw_payload": {"rows": [{"fields": {"LicenseNumber": str(number)}}]},
            "normalized_payload": {"schema_version": tennessee.SCHEMA_VERSION, "visibility": visibility},
            "matched_npi": npi,
            "match_status": "deterministic" if npi else "unmatched",
            "match_evidence": {
                "registry_binding": {
                    "snapshot_sha256": "a" * 64,
                    "reports_sha256": metrics_by_field["reports_sha256"],
                    "status": "deterministic" if npi else "unmatched",
                    "npi": npi,
                }
            },
        }
        await database.insert(shared_store.ProviderProfileSourceRecord.__table__).values(source_by_field).status()
        if npi is None:
            continue
        fact_by_field = _native_fact(candidate, artifact, record_id, npi, profession)
        await database.insert(shared_store.ProviderProfileFact.__table__).values(fact_by_field).status()
    return candidate, metrics_by_field


async def test_native_retained_counts_include_unlicensed_held_and_blank_profiles(monkeypatch):
    async with _database(monkeypatch) as database:
        candidate, metrics_by_field = await _seed_native(database)
        counts = await tennessee.store.retained_counts(candidate["run_id"])
        assert counts["received_profiles"] == counts["retained_source_records"] == 4
        assert counts["source_records_by_profession"] == {"1606": 3, "1907": 1}
        assert counts["facts_by_profession"] == {"1606": 1, "1907": 1}
        assert counts["matched_public_providers"] == 2
        completed = tennessee.store._completion_metrics(candidate, metrics_by_field, counts)
        assert completed["retained_artifacts"] == 1 and completed["retained_facts"] == 2
        with pytest.raises(RuntimeError, match="first_publication_too_small"):
            await tennessee.store.publish_run(
                candidate["run_id"], expected_current_run_id=None, metrics=metrics_by_field
            )
        assert await tennessee.store.read_publication() is None
        assert (await tennessee.store._read_run(candidate["run_id"]))["status"] == "running"


def _bound_artifact(tmp_path):
    """Retain both synthetic reports and bind their original capture evidence."""
    snapshot = _snapshot()
    snapshot_path = tmp_path / "snapshot.json"
    snapshot_path.write_bytes(encoded_json(snapshot))
    candidate = _run()
    candidate["source_manifest"] = worker._source_manifest({"run_id": "run_tennessee_synthetic"}, snapshot, None)
    report_by_profession = _reports()
    descriptors_by_profession = {}
    for profession, report in report_by_profession.items():
        report["evidence"].update(
            run_id=candidate["run_id"],
            source_url=worker.acquisition.REPORT_URL,
            artifact_id=hashlib.sha256(encoded_json([candidate["run_id"], tennessee.SOURCE_KEY])).hexdigest(),
        )
        file_name = profession + ".csv"
        (tmp_path / file_name).write_bytes(report["content"])
        descriptors_by_profession[profession] = {
            **{field: report["evidence"][field] for field in ("content_sha256", "source_url", "downloaded_at")},
            "file_name": file_name,
            "content_bytes": len(report["content"]),
        }
    bound = binding.bind_reports(
        report_by_profession,
        reports_sha256=binding.reports_content_sha256(report_by_profession),
        snapshot_path=snapshot_path,
        snapshot_sha256=candidate["source_manifest"]["snapshot_sha256"],
    )
    artifact, metrics_by_field = worker._artifact(
        candidate, tmp_path, descriptors_by_profession, {"responses": [{"content_bytes": 100} for _ in range(8)]}, bound
    )
    return candidate, bound, artifact, metrics_by_field


async def test_native_real_binding_bundle_and_store_agree(monkeypatch, tmp_path):
    """Real parser bindings agree with retained counts and cannot bypass first floors."""
    candidate, bound, artifact, metrics_by_field = _bound_artifact(tmp_path)
    async with _database(monkeypatch) as database:
        await tennessee.store.claim_run(candidate)
        await database.insert(shared_store.ProviderProfileArtifact.__table__).values(artifact).status()
        await (
            database.insert(shared_store.ProviderProfileSourceRecord.__table__).values(bound["source_records"]).status()
        )
        await database.insert(shared_store.ProviderProfileFact.__table__).values(bound["facts"]).status()
        counts = await tennessee.store.retained_counts(candidate["run_id"])
        assert all(
            counts[name] == 0
            for name in (
                "invalid_source_records",
                "invalid_facts",
                "foreign_artifacts",
                "invalid_bundle_artifacts",
                "invalid_bundle_records",
                "invalid_report_facts",
            )
        )
        assert counts["source_records_by_profession"] == {"1606": 1, "1907": 1}
        assert counts["facts_by_profession"] == {"1606": 3, "1907": 3}
        assert counts["matched_public_providers"] == 2
        assert counts["retained_source_records"] == counts["received_profiles"] == 2
        assert tennessee.store._completion_metrics(candidate, metrics_by_field, counts)["retained_facts"] == 6
        with pytest.raises(RuntimeError, match="first_publication_too_small"):
            await tennessee.store.publish_run(
                candidate["run_id"], expected_current_run_id=None, metrics=metrics_by_field
            )
        assert await tennessee.store.read_publication() is None
        assert (await tennessee.store._read_run(candidate["run_id"]))["status"] == "running"
        assert (
            await database.scalar(
                f"SELECT count(*) FROM {tennessee.store._table(shared_store.ProviderProfileFact)} "
                "WHERE published_at IS NOT NULL"
            )
            == 0
        )


async def test_native_unusable_school_placeholders_cannot_count_toward_floor(monkeypatch):
    async with _database(monkeypatch) as database:
        candidate, _ = await _seed_native(database)
        fact_table = shared_store.ProviderProfileFact.__table__
        for school in ("", "other", "Unknown", " N/A ", "\t Not  reported\n", None, 123):
            await database.update(fact_table).values(value_json={"institution": school}).status()
            counts = await tennessee.store.retained_counts(candidate["run_id"])
            assert counts["matched_public_providers"] == 0
            assert counts["portfolio_only_public_providers"] == 2


@pytest.mark.parametrize("change", ["report_fact", "artifact_link", "held_fact", "snapshot_binding", "profession"])
async def test_native_corrupt_report_pair_or_fact_blocks_completion_and_retention(monkeypatch, change):
    async with _database(monkeypatch) as database:
        candidate, metrics_by_field = await _seed_native(database)
        source_table = shared_store.ProviderProfileSourceRecord.__table__
        match change:
            case "report_fact":
                await database.status(
                    f"UPDATE {tennessee.store._table(shared_store.ProviderProfileFact)} "
                    "SET source_json=jsonb_set(source_json::jsonb,'{content_sha256}',to_jsonb(CAST(:digest AS text)))::json",
                    digest="f" * 64,
                )
            case "artifact_link":
                await database.update(source_table).values(artifact_id="e" * 64).status()
            case "held_fact":
                await (
                    database.update(source_table)
                    .where(source_table.c.record_id == "1" * 64)
                    .values(
                        normalized_payload={"schema_version": tennessee.SCHEMA_VERSION, "visibility": "held_identity"}
                    )
                    .status()
                )
            case "snapshot_binding":
                await (
                    database.update(source_table)
                    .values(match_evidence={"registry_binding": {"snapshot_sha256": "f" * 64}})
                    .status()
                )
            case "profession":
                await database.update(source_table).values(profession_code="9999").status()
        counts = await tennessee.store.retained_counts(candidate["run_id"])
        with pytest.raises(RuntimeError, match="tennessee_profile_"):
            tennessee.store._completion_metrics(candidate, metrics_by_field, counts)
        with pytest.raises(RuntimeError, match="retention_foreign_payload"):
            await tennessee.store._assert_source_ownership([candidate["run_id"]])
        assert (
            await database.scalar(f"SELECT count(*) FROM {tennessee.store._table(shared_store.ProviderProfileFact)}")
            == 2
        )


@pytest.mark.parametrize(
    ("field", "changed"),
    [
        ("status", "identity_conflict"),
        ("status", None),
        ("status", 1),
        ("npi", None),
        ("npi", "1000000004"),
        ("npi", 1000000012),
    ],
)
async def test_native_top_level_identity_cannot_contradict_retained_binding(monkeypatch, field, changed):
    async with _database(monkeypatch) as database:
        candidate, metrics_by_field = await _seed_native(database)
        source_table = shared_store.ProviderProfileSourceRecord.__table__
        decision_by_field = {
            "snapshot_sha256": "a" * 64,
            "reports_sha256": metrics_by_field["reports_sha256"],
            "status": "deterministic",
            "npi": 1000000004,
            field: changed,
        }
        await (
            database.update(source_table)
            .where(source_table.c.record_id == "1" * 64)
            .values(match_evidence={"registry_binding": decision_by_field})
            .status()
        )
        counts = await tennessee.store.retained_counts(candidate["run_id"])
        assert counts["invalid_bundle_records"] == 1
        with pytest.raises(RuntimeError, match="retained_integrity_invalid"):
            tennessee.store._completion_metrics(candidate, metrics_by_field, counts)


@pytest.mark.parametrize("field", ["status", "npi"])
async def test_native_missing_binding_identity_is_not_equivalent_to_json_null(monkeypatch, field):
    async with _database(monkeypatch) as database:
        candidate, metrics_by_field = await _seed_native(database)
        source_table = shared_store.ProviderProfileSourceRecord.__table__
        decision_by_field = {
            "snapshot_sha256": "a" * 64,
            "reports_sha256": metrics_by_field["reports_sha256"],
            "status": "unmatched",
            "npi": None,
        }
        del decision_by_field[field]
        await (
            database.update(source_table)
            .where(source_table.c.record_id == "2" * 64)
            .values(match_evidence={"registry_binding": decision_by_field})
            .status()
        )
        counts = await tennessee.store.retained_counts(candidate["run_id"])
        assert counts["invalid_bundle_records"] == 1
        with pytest.raises(RuntimeError, match="retained_integrity_invalid"):
            tennessee.store._completion_metrics(candidate, metrics_by_field, counts)
