# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""CMS Doctors source-grain and exact artifact preservation contracts."""

import hashlib
import zipfile
from unittest.mock import AsyncMock

import pytest

from process import cms_doctors_artifact as artifact
from process import cms_doctors_groups as groups

NPI = "1000000004"
MANIFEST = {
    "source_key": "cms-doctors",
    "dataset_id": "mj5m-pzi6",
    "content_sha256": "a" * 64,
    "generation_id": "b" * 64,
    "downloaded_at": "2026-09-01T00:00:00",
}


def _row(**overrides):
    source_row_by_name = {
        "NPI": NPI,
        "Ind_enrl_ID": "E-1",
        "Org_PAC_ID": "0012345678",
        "adrs_id": "site-full-id-001",
        "Facility Name": "Example Medical Group",
        "num_org_mem": "12",
        "adr_ln_1": "10 Main Street",
        "City/Town": "Springfield",
        "State": "IL",
        "ZIP Code": "62704",
        "pri_spec": "Internal Medicine",
    }
    source_row_by_name.update(overrides)
    return source_row_by_name


def test_group_site_observations_preserve_group_grain_and_unknown_dates():
    first = groups.doctor_group_site_row(_row(), 1, MANIFEST)
    second = groups.doctor_group_site_row(
        _row(**{"Org_PAC_ID": "0098765432", "Facility Name": "Second Group"}),
        2,
        MANIFEST,
    )
    unlinked = groups.doctor_group_site_row(
        _row(**{"Org_PAC_ID": "", "Facility Name": "", "num_org_mem": ""}),
        3,
        MANIFEST,
    )
    assert first["address_checksum"] == second["address_checksum"]
    assert (first["row_number"], second["row_number"]) == (1, 2)
    assert first["org_pac_id"] == "0012345678"
    assert second["org_pac_id"] == "0098765432"
    assert unlinked["org_pac_id"] is None and unlinked["num_org_mem"] is None
    assert first["adrs_id"] == "site-full-id-001"
    assert first["source_json"]["raw_fields"]["adrs_id"] == "site-full-id-001"
    assert first["membership_start_at"] is first["membership_end_at"] is None


@pytest.mark.parametrize("count", ["twelve", "12.5", "-1"])
def test_group_site_rejects_non_numeric_member_counts(count):
    with pytest.raises(ValueError, match="cms_group_site_invalid_member_count"):
        groups.doctor_group_site_row(_row(num_org_mem=count), 4, MANIFEST)


async def test_group_site_stages_every_physical_row_before_address_dedup(tmp_path, monkeypatch):
    source = tmp_path / "doctors.csv"
    source.write_text(
        "NPI,Ind_enrl_ID,Org_PAC_ID,adrs_id,Facility Name,num_org_mem,adr_ln_1,City/Town,State,ZIP Code,pri_spec\n"
        f"{NPI},E-1,0012345678,site-1,First Group,12,10 Main Street,Springfield,IL,62704,Internal Medicine\n"
        f"{NPI},E-1,0098765432,site-1,Second Group,40,10 Main Street,Springfield,IL,62704,Internal Medicine\n"
        f"{NPI},E-1,,, , ,,,, ,\n"
    )
    staged_rows = []

    async def push(batch, _stage):
        staged_rows.extend(dict(row) for row in batch)

    monkeypatch.setattr(groups.db, "create_table", AsyncMock())
    monkeypatch.setattr(groups, "push_objects", push)
    monkeypatch.setattr(groups, "raise_if_cancelled", AsyncMock())
    worker_context_by_key = {"import_date": "synthetic", "context": {}}
    receipt = await groups.import_group_site_rows(source, worker_context_by_key, {}, MANIFEST)
    assert receipt == {"source_rows": 3, "generation_id": MANIFEST["generation_id"]}
    assert [row["org_pac_id"] for row in staged_rows] == ["0012345678", "0098765432", None]
    assert staged_rows[0]["address_checksum"] == staged_rows[1]["address_checksum"]
    assert staged_rows[2]["address_checksum"] is None
    assert worker_context_by_key["context"]["group_site_stage_owned"] is True


def test_exact_zip_artifact_is_content_addressed(monkeypatch, tmp_path):
    source = tmp_path / "source.zip"
    with zipfile.ZipFile(source, "w") as archive:
        archive.writestr("doctors.csv", "NPI,Ind_enrl_ID\n1000000004,E-1\n")
    payload = source.read_bytes()
    monkeypatch.setattr(artifact, "_artifact_root", lambda: tmp_path / "retained")
    first = artifact.retain_doctors_artifact(str(source), "https://example.test/source.zip")
    second = artifact.retain_doctors_artifact(str(source), "https://example.test/source.zip")
    assert first == second
    assert first["content_sha256"] == hashlib.sha256(payload).hexdigest()
    assert first["content_bytes"] == len(payload)
    assert (tmp_path / "retained" / first["file_name"]).read_bytes() == payload
    assert len(list((tmp_path / "retained").iterdir())) == 1


def test_artifact_verification_rejects_changed_missing_and_malformed_receipts(monkeypatch, tmp_path):
    source = tmp_path / "source.csv"
    source.write_bytes(b"synthetic")
    monkeypatch.setattr(artifact, "_artifact_root", lambda: tmp_path / "retained")
    receipt = artifact.retain_doctors_artifact(str(source), "https://example.test/source.csv")
    artifact.verify_doctors_artifact(receipt)

    retained = tmp_path / "retained" / receipt["file_name"]
    retained.write_bytes(b"different")
    with pytest.raises(RuntimeError, match="artifact_changed"):
        artifact.verify_doctors_artifact(receipt)
    retained.unlink()
    with pytest.raises(RuntimeError, match="artifact_missing"):
        artifact.verify_doctors_artifact(receipt)
    with pytest.raises(RuntimeError, match="receipt_invalid"):
        artifact.verify_doctors_artifact({**receipt, "file_name": []})


def test_artifact_rejects_missing_or_temporary_root(tmp_path, monkeypatch):
    source = tmp_path / "source.csv"
    source.write_text("synthetic")
    monkeypatch.delenv("HLTHPRT_CMS_DOCTORS_ARTIFACT_ROOT", raising=False)
    monkeypatch.delenv("HLTHPRT_PROVIDER_DIRECTORY_ARTIFACT_ROOT", raising=False)
    with pytest.raises(RuntimeError, match="artifact_root_required"):
        artifact.retain_doctors_artifact(str(source), "https://example.test/source.csv")
    for temporary_root in ("/tmp", "/var/tmp", "/private/var/tmp", "/dev/shm", "/run"):
        monkeypatch.setenv("HLTHPRT_CMS_DOCTORS_ARTIFACT_ROOT", f"{temporary_root}/cms-doctors")
        with pytest.raises(RuntimeError, match="artifact_root_not_durable"):
            artifact.retain_doctors_artifact(str(source), "https://example.test/source.csv")


def test_artifact_uses_existing_directory_root_when_dedicated_root_is_unset(monkeypatch):
    monkeypatch.delenv("HLTHPRT_CMS_DOCTORS_ARTIFACT_ROOT", raising=False)
    monkeypatch.setenv("HLTHPRT_PROVIDER_DIRECTORY_ARTIFACT_ROOT", "/srv/example-evidence")
    assert str(artifact._artifact_root()) == "/srv/example-evidence/cms-doctors"
