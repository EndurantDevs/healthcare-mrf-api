# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""CMS tax candidates follow source publication without blocking recovery."""

import json
from contextlib import asynccontextmanager

import pytest

from process import cms_npd_tax_candidate_followup as followup
from process import cms_npd_tax_candidate_runner as runner
from process.cms_npd_tax_candidate_report import CmsNpiTaxCandidate
from tests.test_cms_npd_tax_candidate_runner import _admitted_release, _organization


class _Database:
    def __init__(self):
        self.read_only_statements = []

    @asynccontextmanager
    async def session(self):
        database = self

        class _Session:
            async def execute(self, statement):
                database.read_only_statements.append(str(statement))

        yield _Session()


class _Fhir:
    def __init__(self):
        self.db = _Database()

    def _schema(self):
        return "mrf"


@pytest.mark.asyncio
async def test_followup_runs_after_admission_and_replays_unchanged_bytes(tmp_path, monkeypatch):
    directory, receipt = _admitted_release(tmp_path, [_organization(1)])
    fhir = _Fhir()
    lookups = []

    async def pin(_session, *, schema_name):
        assert schema_name == "mrf"
        return "synthetic-snapshot", 17, "b" * 64

    async def lookup(_session, **lookup_kwargs_by_name):
        lookups.append(tuple(lookup_kwargs_by_name["npis"]))
        return {npi: CmsNpiTaxCandidate((5,), 1, 0) for npi in lookup_kwargs_by_name["npis"]}

    monkeypatch.setattr(followup, "current_sealed_v4_tax_pin", pin)
    monkeypatch.setattr(runner, "lookup_pinned_tax_candidates", lookup)
    followup_kwargs_by_name = {
        "release_directory": directory,
        "dataset_id": "synthetic-dataset",
        "vector_sha256": receipt["vector_sha256"],
        "generated_at": receipt["generated_at"],
    }
    first = await followup.cms_npd_tax_candidate_followup(fhir, **followup_kwargs_by_name)
    second = await followup.cms_npd_tax_candidate_followup(fhir, **followup_kwargs_by_name)
    assert first == second
    assert first["status"] == "complete"
    assert first["candidate_organization_count"] == 1
    assert first["retryable"] is False
    assert lookups == [(), (1000000004,), (), (1000000004,)]
    assert await followup.completed_cms_tax_candidate_report(fhir, **followup_kwargs_by_name) == first
    reports = list((directory / "tax-candidates").glob("[0-9a-f]" * 64 + ".json"))
    assert len(reports) == 1
    report = json.loads(reports[0].read_bytes())
    assert report["extraction_cutoff"] == "2026-09-24T23:59:59.999999Z"
    assert report["tax_manifest_sha256"] == "b" * 64
    assert len(fhir.db.read_only_statements) == 7


@pytest.mark.asyncio
async def test_completed_report_probe_retries_missing_stale_or_changed_evidence(tmp_path, monkeypatch):
    directory, receipt = _admitted_release(tmp_path, [_organization(1)])
    fhir = _Fhir()
    current_pin_values = ["synthetic-snapshot", 17, "b" * 64]

    async def pin(_session, *, schema_name):
        assert schema_name == "mrf"
        return tuple(current_pin_values)

    async def lookup(_session, **kwargs):
        return {npi: CmsNpiTaxCandidate((5,), 1, 0) for npi in kwargs["npis"]}

    monkeypatch.setattr(followup, "current_sealed_v4_tax_pin", pin)
    monkeypatch.setattr(runner, "lookup_pinned_tax_candidates", lookup)
    followup_kwargs_by_name = {
        "release_directory": directory,
        "dataset_id": "synthetic-dataset",
        "vector_sha256": receipt["vector_sha256"],
        "generated_at": receipt["generated_at"],
    }
    assert await followup.completed_cms_tax_candidate_report(fhir, **followup_kwargs_by_name) is None
    completed = await followup.cms_npd_tax_candidate_followup(fhir, **followup_kwargs_by_name)
    assert completed["status"] == "complete"
    assert await followup.completed_cms_tax_candidate_report(fhir, **followup_kwargs_by_name) == completed
    current_pin_values[1] = 18
    assert await followup.completed_cms_tax_candidate_report(fhir, **followup_kwargs_by_name) is None
    current_pin_values[1] = 17
    report = next((directory / "tax-candidates").glob("[0-9a-f]" * 64 + ".json"))
    report.write_bytes(report.read_bytes() + b" ")
    assert await followup.completed_cms_tax_candidate_report(fhir, **followup_kwargs_by_name) is None


@pytest.mark.asyncio
async def test_unavailable_or_failed_tax_followup_does_not_fail_publication(tmp_path, monkeypatch):
    directory, receipt = _admitted_release(tmp_path, [_organization(1, npi=False)])
    fhir = _Fhir()

    async def unavailable(_session, *, schema_name):
        return None

    monkeypatch.setattr(followup, "current_sealed_v4_tax_pin", unavailable)
    followup_kwargs_by_name = {
        "release_directory": directory,
        "dataset_id": "synthetic-dataset",
        "vector_sha256": receipt["vector_sha256"],
        "generated_at": receipt["generated_at"],
    }
    assert await followup.cms_npd_tax_candidate_followup(fhir, **followup_kwargs_by_name) == {
        "status": "unavailable",
        "retryable": True,
        "retry_via": "same_byte_import",
    }

    async def failing(_session, *, schema_name):
        raise ValueError("synthetic failure")

    monkeypatch.setattr(followup, "current_sealed_v4_tax_pin", failing)
    assert await followup.cms_npd_tax_candidate_followup(fhir, **followup_kwargs_by_name) == {
        "status": "failed",
        "retryable": True,
        "retry_via": "same_byte_import",
    }
    assert not (directory / "tax-candidates").exists()


@pytest.mark.asyncio
async def test_failed_report_retries_on_same_byte_import_replay(tmp_path, monkeypatch):
    directory, receipt = _admitted_release(tmp_path, [_organization(1)])
    fhir = _Fhir()
    lookup_attempts = []

    async def pin(_session, *, schema_name):
        return "synthetic-snapshot", 17, "b" * 64

    async def fail_once(_session, **lookup_kwargs_by_name):
        selected_npis = tuple(lookup_kwargs_by_name["npis"])
        lookup_attempts.append(selected_npis)
        if selected_npis and lookup_attempts.count(selected_npis) == 1:
            raise ValueError("synthetic lookup interruption")
        return {npi: CmsNpiTaxCandidate((5,), 1, 0) for npi in selected_npis}

    monkeypatch.setattr(followup, "current_sealed_v4_tax_pin", pin)
    monkeypatch.setattr(runner, "lookup_pinned_tax_candidates", fail_once)
    replay_kwargs_by_name = {
        "release_directory": directory,
        "dataset_id": "synthetic-dataset",
        "vector_sha256": receipt["vector_sha256"],
        "generated_at": receipt["generated_at"],
    }
    first = await followup.cms_npd_tax_candidate_followup(fhir, **replay_kwargs_by_name)
    assert first == {"status": "failed", "retryable": True, "retry_via": "same_byte_import"}
    assert not list((directory / "tax-candidates").glob("*.json"))
    second = await followup.cms_npd_tax_candidate_followup(fhir, **replay_kwargs_by_name)
    assert second["status"] == "complete"
    assert second["retryable"] is False
    assert len(list((directory / "tax-candidates").glob("[0-9a-f]" * 64 + ".json"))) == 1
    assert lookup_attempts == [(), (1000000004,), (), (1000000004,)]
