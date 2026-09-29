# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Retained-byte provenance is distinct from a future serving acceptance."""

import json

import pytest

from process import cms_doctors_artifact as artifact
from process import cms_doctors_education as education
from process import cms_doctors_source_provenance as provenance


def source_metrics(tmp_path, monkeypatch):
    """Retain and verify a synthetic distribution using the actual source conventions."""
    source = tmp_path / "doctors.csv"
    source.write_text(
        "NPI,Med_sch,Grd_yr,ind_enrl_id,org_pac_id,adrs_id,Facility Name,num_org_mem,adr_ln_1,citytown,state,zip_code\n"
        "1000000004,Synthetic School,2001,123,,site-1,,,10 Example St,Example City,NY,10001\n"
    )
    monkeypatch.setattr(artifact, "_artifact_root", lambda: tmp_path / "retained")
    url = "https://example.test/doctors.csv"
    retained = artifact.retain_doctors_artifact(str(source), url)
    artifact.verify_doctors_artifact(retained)
    manifest = education.education_source_manifest(source, url)
    manifest.update(source_rows=1, education_rows=1)
    return {
        "rows": 1,
        "artifact": retained,
        "education": manifest,
        "group_site": {"generation_id": manifest["generation_id"], "source_rows": 1},
        "organization_groups": 0,
        "sites": 0,
    }


def test_source_provenance_copies_actual_retained_bytes_without_native_acceptance(tmp_path, monkeypatch):
    metrics = source_metrics(tmp_path, monkeypatch)
    evidence = provenance.mint_doctors_source_provenance(metrics)
    payload = provenance.read_doctors_source_provenance(evidence)
    assert payload["artifact"] == metrics["artifact"]
    assert payload["education"] == metrics["education"]
    assert set(payload) == {
        "contract_id",
        "artifact",
        "education",
        "group_site",
        "rows",
        "organization_groups",
        "sites",
    }
    assert len(provenance.doctors_source_provenance_digest(evidence)) == 64
    metrics["education"]["source_rows"] = 2
    assert provenance.read_doctors_source_provenance(evidence) == payload


@pytest.mark.parametrize("change", ["digest", "generation", "rows", "bytes", "filename", "url", "time", "missing"])
def test_source_provenance_rejects_inconsistent_source_receipts(tmp_path, monkeypatch, change):
    metrics = source_metrics(tmp_path, monkeypatch)
    mutations_by_name = {
        "digest": lambda: metrics["education"].update(content_sha256="f" * 64),
        "generation": lambda: metrics["group_site"].update(generation_id="f" * 64),
        "rows": lambda: metrics["group_site"].update(source_rows=2),
        "bytes": lambda: metrics["artifact"].update(content_bytes=True),
        "filename": lambda: metrics["artifact"].update(file_name="unretained.csv"),
        "url": lambda: metrics["education"].update(source_url="https://example.test/other.csv"),
        "time": lambda: metrics["education"].update(downloaded_at="invalid"),
        "missing": lambda: metrics.pop("artifact"),
    }
    mutations_by_name[change]()
    with pytest.raises(ValueError, match="cms_doctors_source_"):
        provenance.mint_doctors_source_provenance(metrics)


@pytest.mark.parametrize("value", [None, "{}", "null", "[]", "invalid"])
def test_source_provenance_requires_minted_complete_evidence(value):
    with pytest.raises(ValueError, match="cms_doctors_source_provenance"):
        provenance.read_doctors_source_provenance(value)


def test_source_provenance_rejects_extra_fields_or_noncanonical_encoding(tmp_path, monkeypatch):
    evidence = provenance.mint_doctors_source_provenance(source_metrics(tmp_path, monkeypatch))
    payload = json.loads(evidence)
    payload["local_generation"] = 2
    with pytest.raises(ValueError, match="provenance_changed"):
        provenance.read_doctors_source_provenance(json.dumps(payload, sort_keys=True, separators=(",", ":")))
    with pytest.raises(ValueError, match="provenance_changed"):
        provenance.read_doctors_source_provenance(json.dumps(json.loads(evidence)))
