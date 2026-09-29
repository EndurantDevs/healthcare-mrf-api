# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Admitted CMS rows yield complete bounded candidate reports without tax projection."""

import json
from contextlib import asynccontextmanager

import pytest

from process import cms_npd_tax_candidate_runner as runner
from process.cms_npd_source import _decoded_resource_fingerprints, acquire_release, parse_manifest
from process.cms_npd_tax_candidate_report import CmsNpiTaxCandidate
from tests.test_cms_npd_source import _client, _source


def _organization(index, *, npi=True, name="Synthetic"):
    identifiers = [
        {
            "system": "https://npd.cms.gov/fhir/sid/us-pseudo-ein",
            "value": "synthetic-surrogate",
        }
    ]
    if npi:
        identifiers.append({"system": "http://hl7.org/fhir/sid/us-npi", "value": "1000000004"})
    return {
        "resourceType": "Organization",
        "id": f"synthetic-{index}",
        "name": name,
        "identifier": identifiers,
    }


def _admitted_release(tmp_path, organizations):
    raw = b"".join(
        json.dumps(organization, ensure_ascii=False).encode("utf-8") + b"\n" for organization in organizations
    )
    manifest, payloads = _source({"01-Organization.ndjson": raw})
    client, _ = _client(manifest, payloads)
    with client:
        return acquire_release(tmp_path, client=client, base_url="https://example.test/downloads")


async def _run_candidate_report(monkeypatch, directory, output):
    calls = []
    transactions = []

    class _Session:
        async def execute(self, statement):
            transactions.append(str(statement))

    @asynccontextmanager
    async def session_factory():
        yield _Session()

    async def lookup(_session, **kwargs):
        npis = tuple(kwargs["npis"])
        calls.append(npis)
        assert len(npis) <= 256
        return {npi: CmsNpiTaxCandidate((7,), 1, 0) for npi in npis}

    monkeypatch.setattr(runner, "lookup_pinned_tax_candidates", lookup)
    report_result = await runner.run_admitted_cms_tax_candidate_report(
        session_factory,
        release_directory=directory,
        output_path=output,
        schema_name="mrf",
        dataset_id="synthetic-dataset",
        snapshot_key=17,
        manifest_sha256="b" * 64,
        evidence_as_of="2026-09-24T00:00:00.000000Z",
    )
    assert transactions == ["SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"] * len(calls)
    return json.loads(output.read_bytes()), calls, report_result


@pytest.mark.asyncio
async def test_runner_streams_complete_admitted_rows_with_unicode_hash_and_bounded_batches(tmp_path, monkeypatch):
    organizations = [_organization(0, name="Clínica")] + [_organization(index) for index in range(1, 257)]
    directory, receipt = _admitted_release(tmp_path, organizations)
    expected_hash = next(
        _decoded_resource_fingerprints(
            directory / "01-Organization.ndjson.zst",
            parse_manifest((directory / "manifest.json").read_bytes()).files[0],
        )
    )[1].hex()
    output = tmp_path / "candidate-report.json"
    report, calls, result = await _run_candidate_report(monkeypatch, directory, output)
    assert calls == [(), (1000000004,), (1000000004,)]
    assert report["release_id"] == receipt["vector_sha256"]
    assert report["source_file_row_count"] == 257
    assert report["candidate_organization_count"] == 257
    assert len(report["organizations"]) == 257
    assert (
        next(row for row in report["organizations"] if row["resource_id"] == "synthetic-0")["payload_sha256"]
        == expected_hash
    )
    assert report["extraction_policy_sha256"] == runner.CMS_NPD_NPI_ONLY_POLICY.descriptor_sha256
    assert report["extraction_cutoff"] == "2026-09-24T00:00:00.000000Z"
    assert report["organizations"][0]["missing_real_ein"] is True
    assert b"synthetic-surrogate" not in output.read_bytes()
    assert output.stat().st_mode & 0o077 == 0
    assert len(result.sha256) == 64
    assert result.candidate_organization_count == 257


@pytest.mark.asyncio
async def test_runner_persists_explicit_empty_report_and_rejects_changed_witness(tmp_path, monkeypatch):
    directory, _ = _admitted_release(tmp_path, [_organization(1, npi=False)])
    output = tmp_path / "empty-report.json"
    report, calls, _ = await _run_candidate_report(monkeypatch, directory, output)
    assert calls == [()]
    assert report["organizations"] == []
    assert report["candidate_organization_count"] == 0
    assert report["skipped_distinct_organizations"] == {"missing_identifiers": 1}
    repeated, _, repeated_result = await _run_candidate_report(monkeypatch, directory, output)
    assert repeated == report
    assert repeated_result.sha256 == runner._file_sha256(output)
    assert repeated_result.candidate_organization_count == 0
    with (directory / "01-Organization.ndjson.zst").open("r+b") as source:
        source.write(b"X")
    with pytest.raises(ValueError, match="Organization file changed"):
        await _run_candidate_report(monkeypatch, directory, tmp_path / "should-not-exist.json")
    assert not (tmp_path / "should-not-exist.json").exists()


@pytest.mark.asyncio
async def test_runner_skips_unreviewed_identifier_system_without_projecting_tax(tmp_path, monkeypatch):
    organization = _organization(1)
    organization["identifier"].append({"system": "https://example.test/tax", "value": "12-3456789"})
    directory, _ = _admitted_release(tmp_path, [organization])
    report, calls, _ = await _run_candidate_report(monkeypatch, directory, tmp_path / "report.json")
    assert calls == [()]
    assert report["organizations"] == []
    assert report["skipped_distinct_organizations"] == {"unreviewed_identifier_system": 1}
    assert b"12-3456789" not in (tmp_path / "report.json").read_bytes()


@pytest.mark.asyncio
async def test_runner_removes_partial_report_and_seen_index_after_lookup_failure(tmp_path, monkeypatch):
    directory, _ = _admitted_release(tmp_path, [_organization(1)])

    async def fail_lookup(_session, **kwargs):
        if kwargs["npis"]:
            raise ValueError("synthetic pinned lookup failure")
        return {}

    monkeypatch.setattr(runner, "lookup_pinned_tax_candidates", fail_lookup)

    @asynccontextmanager
    async def session_factory():
        class _Session:
            async def execute(self, _statement):
                return None

        yield _Session()

    with pytest.raises(ValueError, match="pinned lookup failure"):
        await runner.run_admitted_cms_tax_candidate_report(
            session_factory,
            release_directory=directory,
            output_path=tmp_path / "failed-report.json",
            schema_name="mrf",
            dataset_id="synthetic-dataset",
            snapshot_key=17,
            manifest_sha256="b" * 64,
            evidence_as_of="2026-09-24T00:00:00.000000Z",
        )
    assert not (tmp_path / "failed-report.json").exists()
    assert not list(tmp_path.glob(".cms-tax-*"))
