# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Reprocess a sealed acquisition without consulting the registry or network."""

import hashlib
import json
from unittest.mock import AsyncMock, Mock

import pytest

from api.provider_profile_states import _state_projection
from process import massachusetts_profile_retained as retained
from process.control_cancel import ImportCancelledError
from tests.test_massachusetts_profile import PREDECESSOR, harness, worker


def _completed_parent(harness):
    parent_by_field = {
        "run_id": PREDECESSOR,
        "status": "completed",
        "source_manifest": worker._source_manifest(
            {},
            harness.cohort,
            None,
            categories=worker.LEGACY_CATEGORIES,
        ),
    }
    directory = harness.artifact_root / parent_by_field["run_id"]
    (directory / "profiles").mkdir(parents=True)
    worker.acquisition.write_new_json(directory / "cohort.json", harness.cohort)
    response_hash = hashlib.sha256()
    total_bytes = 0
    for root in harness.cohort["roots"]:
        license_number = root["license_number"]
        response = harness.responses_by_license[license_number]
        worker.acquisition.write_new_json(directory / "profiles" / f"{license_number}.json", response)
        total_bytes += len(response["body_text"].encode())
        response_hash.update(
            worker.acquisition.encoded_json([license_number, response["content_sha256"], response["downloaded_at"]])
        )
    metrics_by_field = {
        "responses": len(harness.cohort["roots"]),
        "response_bytes": total_bytes,
        "reused_responses": 0,
        "responses_sha256": response_hash.hexdigest(),
        "acquisition_complete": True,
        "transport_failures": 0,
    }
    artifact = worker._artifact(parent_by_field, directory, metrics_by_field)
    parent_by_field["metrics"] = {**metrics_by_field, "published": True}
    return parent_by_field, artifact, directory


def _install_parent(monkeypatch, harness, parent, artifact):
    monkeypatch.setattr(worker.store, "read_reprocess_run", AsyncMock(return_value=(parent, artifact)))
    worker.acquisition.capture_registry_cohort.side_effect = AssertionError(
        "No registry read during retained reprocessing"
    )
    monkeypatch.setattr(
        worker.acquisition.aiohttp,
        "ClientSession",
        Mock(side_effect=AssertionError("No transport during retained reprocessing")),
    )


def _assert_reprocessed_facts(harness, run, expected_count, lineage):
    """Keep original fact values and timestamps through profile composition."""
    facts = harness.rows_for(worker.ProviderProfileFact)
    assert len(facts) == 3 * expected_count
    assert {fact["category"] for fact in facts} == {"education", "certifications", "specialties"}
    for fact in facts:
        assert "reprocessing" not in fact["source_json"]
        assert fact["source_json"]["downloaded_at"] == "2026-09-08T12:00:00+00:00"
    assert all(
        source_record["normalized_payload"]["reprocessing"] == lineage
        for source_record in harness.rows_for(worker.ProviderProfileSourceRecord)
    )
    assert all(
        source_record["raw_payload"]["languages"] == ["Example Language"]
        for source_record in harness.rows_for(worker.ProviderProfileSourceRecord)
    )
    assert "Example Language" not in str(facts)
    npi = facts[0]["npi"]
    projection = _state_projection(
        npi,
        [
            {
                **fact,
                "generation_id": run["run_id"],
                "run_status": "completed",
                "source_manifest": run["source_manifest"],
            }
            for fact in facts
            if fact["npi"] == npi
        ],
    )
    assert projection["generation_id"] == run["run_id"]
    assert all(
        "reprocessing" not in evidence_record and evidence_record["downloaded_at"] == "2026-09-08T12:00:00+00:00"
        for evidence_record in projection["evidence"]["records"]
    )


@pytest.mark.parametrize("limit", [None, 2, 100])
async def test_managed_reprocessing_preserves_parent_bytes_and_public_evidence(harness, monkeypatch, limit):
    """Reprocess all or bounded parents without fetching or changing retained evidence."""
    for response in harness.responses_by_license.values():
        profile = worker.acquisition.decoded_profile(response)
        profile["specialties"] = ["Example Specialty"]
        profile["boardCertifications"] = {
            "abms": [{"boardName": "Example Board", "specialties": [], "subspecialties": []}]
        }
        profile["languages"] = ["Example Language"]
        body = worker.acquisition.encoded_json(profile)
        response.update(body_text=body.decode(), content_sha256=hashlib.sha256(body).hexdigest())
    parent, artifact, directory = _completed_parent(harness)
    before_by_name = {str(path.relative_to(directory)): path.read_bytes() for path in directory.rglob("*.json")}
    _install_parent(monkeypatch, harness, parent, artifact)
    await worker.import_profiles(
        harness.ctx, {**harness.task, "max_providers": limit, "reprocess_from": parent["run_id"]}
    )
    run = harness.store_by_name["claim_run"].call_args.args[0]
    assert run["run_id"] != parent["run_id"] and run["source_manifest"]["expected_current_run_id"] == parent["run_id"]
    assert run["source_manifest"]["categories"] == list(worker.PROFILE_CATEGORIES)
    assert run["source_manifest"]["resume_from"] is None
    lineage = run["source_manifest"]["reprocessing"]
    assert lineage["source_run_id"] == parent["run_id"] and lineage["manifest_sha256"] == artifact["content_sha256"]
    assert lineage["artifact_id"] == artifact["artifact_id"]
    child = harness.artifact_root / run["run_id"]
    for path in (child / "profiles").iterdir():
        assert path.read_bytes() == before_by_name[f"profiles/{path.name}"]
    assert {str(path.relative_to(directory)): path.read_bytes() for path in directory.rglob("*.json")} == before_by_name
    expected_count = min(limit, 3) if limit is not None else 3
    metrics = harness.finish.call_args.args[3]
    assert metrics["reused_responses"] == metrics["responses"] == expected_count
    assert harness.requests == []
    _assert_reprocessed_facts(harness, run, expected_count, lineage)
    assert json.loads((child / "manifest.json").read_bytes())["source_manifest"] == run["source_manifest"]


@pytest.mark.parametrize("failure", ["missing", "body", "envelope", "cohort", "manifest", "extra_file", "symlink"])
async def test_entire_parent_is_validated_before_claim_even_for_bounded_selection(harness, monkeypatch, failure):
    parent, artifact, directory = _completed_parent(harness)
    selected = worker._selected_roots(harness.cohort, 1)[0]["license_number"]
    other = next(root["license_number"] for root in harness.cohort["roots"] if root["license_number"] != selected)
    path = directory / "profiles" / f"{other}.json"
    if failure == "missing":
        path.unlink()
    elif failure in {"body", "envelope"}:
        response = json.loads(path.read_bytes())
        response["body_text" if failure == "body" else "source_url"] += "changed"
        path.write_bytes(worker.acquisition.encoded_json(response))
    elif failure in {"cohort", "manifest"}:
        (directory / f"{failure}.json").write_text("{}")
    elif failure == "extra_file":
        (directory / "profiles" / "unexpected.json").write_text("{}")
    else:
        path.unlink()
        path.symlink_to(directory / "profiles" / f"{selected}.json")
    _install_parent(monkeypatch, harness, parent, artifact)
    with pytest.raises(ValueError, match="massachusetts_profile_"):
        await worker.import_profiles(
            harness.ctx, {**harness.task, "max_providers": 1, "reprocess_from": parent["run_id"]}
        )
    harness.store_by_name["claim_run"].assert_not_called()
    assert harness.writes == [] and harness.requests == []
    harness.finish.assert_not_called()


@pytest.mark.parametrize("phase", ["claim", "retaining"])
async def test_changed_envelope_after_preflight_cannot_reach_completion(harness, monkeypatch, phase):
    parent, artifact, directory = _completed_parent(harness)
    _install_parent(monkeypatch, harness, parent, artifact)

    def change_response(path):
        response = json.loads(path.read_bytes())
        response["content_type"] = "application/json; charset=utf-8"
        path.write_bytes(worker.acquisition.encoded_json(response))

    if phase == "claim":
        harness.store_by_name["claim_run"].side_effect = lambda _run: change_response(
            directory / "profiles" / "123.json"
        )
    else:
        original_upsert = harness.upsert

        async def upsert(model, source_rows, key):
            await original_upsert(model, source_rows, key)
            if model is worker.ProviderProfileArtifact:
                change_response(harness.artifact_root / source_rows[0]["run_id"] / "profiles" / "123.json")

        monkeypatch.setattr(worker, "_upsert_rows", upsert)
    with pytest.raises(ValueError, match="retained_envelope_changed"):
        await worker.import_profiles(harness.ctx, {**harness.task, "reprocess_from": parent["run_id"]})
    assert harness.rows_for(worker.ProviderProfileFact) == [] and harness.requests == []
    harness.finish.assert_not_called()
    harness.store_by_name["mark_run_failed"].assert_awaited_once()


async def test_recorded_parent_envelope_digest_is_checked_before_claim(harness, monkeypatch):
    parent, artifact, directory = _completed_parent(harness)
    _, preparation = await retained.validate_acquisition(directory, parent, artifact, AsyncMock())
    digest = preparation["lineage"]["response_envelopes_sha256"]
    manifest_path = directory / "manifest.json"
    manifest = json.loads(manifest_path.read_bytes())
    manifest["acquisition"]["response_envelopes_sha256"] = digest
    parent["metrics"]["response_envelopes_sha256"] = digest
    body = worker.acquisition.encoded_json(manifest)
    manifest_path.write_bytes(body)
    artifact.update(content_bytes=len(body), content_sha256=hashlib.sha256(body).hexdigest())
    _, unchanged = await retained.validate_acquisition(directory, parent, artifact, AsyncMock())
    assert unchanged["lineage"]["response_envelopes_sha256"] == digest

    response_path = directory / "profiles" / "123.json"
    response = json.loads(response_path.read_bytes())
    response["content_type"] = "application/json; charset=utf-8"
    response_path.write_bytes(worker.acquisition.encoded_json(response))
    _install_parent(monkeypatch, harness, parent, artifact)
    with pytest.raises(ValueError, match="retained_acquisition_changed"):
        await worker.import_profiles(harness.ctx, {**harness.task, "reprocess_from": parent["run_id"]})
    harness.store_by_name["claim_run"].assert_not_called()
    assert harness.writes == [] and harness.requests == []
    harness.finish.assert_not_called()


async def test_retained_validation_and_copy_honor_cancellation(harness):
    parent, artifact, directory = _completed_parent(harness)
    progress = AsyncMock(side_effect=ImportCancelledError("synthetic cancellation"))
    with pytest.raises(ImportCancelledError):
        await retained.validate_acquisition(directory, parent, artifact, progress)
    cohort, preparation = await retained.validate_acquisition(directory, parent, artifact, AsyncMock())
    destination = harness.artifact_root / "copy"
    with pytest.raises(ImportCancelledError):
        await retained.copy_profiles(
            cohort["roots"], directory / "profiles", destination, preparation["hashes_by_license"], progress
        )
    assert list(destination.iterdir()) == []


@pytest.mark.parametrize(
    "params",
    [
        {"reprocess_from": "../other"},
        {"reprocess_from": 1},
        {"resume_from": "a" * 64, "reprocess_from": "b" * 64},
        {"reprocessing_from": "a" * 64},
        {"test_mode": True},
        [],
    ],
)
def test_managed_parameters_reject_invalid_mixed_and_ignored_modes(params):
    with pytest.raises(ValueError, match="massachusetts_profile_"):
        worker.validate_request_parameters(params)


async def test_copy_missing_response_or_existing_destination_never_falls_back(harness):
    parent, artifact, directory = _completed_parent(harness)
    cohort, preparation = await retained.validate_acquisition(directory, parent, artifact, AsyncMock())
    destination = harness.artifact_root / "copy"
    destination.mkdir()
    with pytest.raises(FileExistsError):
        await retained.copy_profiles(
            cohort["roots"], directory / "profiles", destination, preparation["hashes_by_license"], AsyncMock()
        )
    (directory / "profiles" / "123.json").unlink()
    with pytest.raises(ValueError, match="retained_response_missing"):
        await retained.copy_profiles(
            cohort["roots"],
            directory / "profiles",
            harness.artifact_root / "fresh-copy",
            preparation["hashes_by_license"],
            AsyncMock(),
        )


@pytest.mark.parametrize(
    "change",
    [
        "symlink",
        "large",
        "identity",
        "metrics",
        "negative_bytes",
        "invalid_reuse",
        "count",
        "aggregate_bytes",
        "aggregate_hash",
    ],
)
async def test_parent_artifact_validation_rejects_invalid_completed_receipts(harness, change):
    parent, artifact, directory = _completed_parent(harness)
    path = directory / "manifest.json"
    if change == "symlink":
        path.rename(directory / "other.json")
        path.symlink_to(directory / "other.json")
    elif change == "large":
        path.write_bytes(b" " * (1024 * 1024 + 1))
    else:
        manifest = json.loads(path.read_bytes())
        if change == "identity":
            manifest["run_id"] = "f" * 64
        elif change == "metrics":
            manifest["acquisition"]["acquisition_complete"] = False
        else:
            key, value = {
                "negative_bytes": ("response_bytes", -1),
                "invalid_reuse": ("reused_responses", 4),
                "count": ("responses", 4),
                "aggregate_bytes": ("response_bytes", 0),
                "aggregate_hash": ("responses_sha256", "f" * 64),
            }[change]
            manifest["acquisition"][key] = value
            parent["metrics"][key] = value
        body = worker.acquisition.encoded_json(manifest)
        path.write_bytes(body)
        artifact.update(content_bytes=len(body), content_sha256=hashlib.sha256(body).hexdigest())
    with pytest.raises(ValueError, match="massachusetts_profile_retained_"):
        await retained.validate_acquisition(directory, parent, artifact, AsyncMock())


async def test_parent_cohort_must_equal_the_published_registry_snapshot(harness):
    parent, artifact, directory = _completed_parent(harness)
    parent["source_manifest"]["cohort_sha256"] = "f" * 64
    path = directory / "manifest.json"
    manifest = json.loads(path.read_bytes())
    manifest["source_manifest"] = parent["source_manifest"]
    body = worker.acquisition.encoded_json(manifest)
    path.write_bytes(body)
    artifact.update(content_bytes=len(body), content_sha256=hashlib.sha256(body).hexdigest())
    with pytest.raises(ValueError, match="retained_cohort_changed"):
        await retained.validate_acquisition(directory, parent, artifact, AsyncMock())


async def test_retained_validation_and_copy_enforce_acquisition_byte_cap(harness, monkeypatch):
    parent, artifact, directory = _completed_parent(harness)
    cohort, preparation = await retained.validate_acquisition(directory, parent, artifact, AsyncMock())
    monkeypatch.setattr(worker.acquisition, "MAX_ACQUISITION_BYTES", 1)
    with pytest.raises(ValueError, match="acquisition_too_large"):
        await retained.validate_acquisition(directory, parent, artifact, AsyncMock())
    destination = harness.artifact_root / "oversize-copy"
    with pytest.raises(ValueError, match="acquisition_too_large"):
        await retained.copy_profiles(
            cohort["roots"], directory / "profiles", destination, preparation["hashes_by_license"], AsyncMock()
        )
    assert list(destination.iterdir()) == []


def test_worker_parameters_reject_test_mode_before_access():
    with pytest.raises(ValueError, match="parameters_invalid"):
        worker._parameters({"run_id": "synthetic-control", "test_mode": True})
