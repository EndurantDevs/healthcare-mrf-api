# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Retained-byte provenance is distinct from a future serving acceptance."""

import csv
import importlib
import json
import zipfile
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process import cms_doctors_artifact as artifact
from process import cms_doctors_education as education
from process import cms_doctors_groups as groups
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


def _replay_source_rows():
    return [
        [
            "1000000004",
            "0001",
            "000010",
            "site-" + "a" * 200,
            "Synthetic Group",
            "2",
            "MD",
            "Synthetic School",
            "2021",
            "10 Example St",
        ],
        [
            "1000000004",
            "0002",
            "000020",
            "site-" + "a" * 200,
            "Other Group",
            "1",
            "MD",
            "Synthetic School",
            "2021",
            "10 Example St",
        ],
        [
            "1000000004",
            "0003",
            "000010",
            "site-b",
            "Synthetic Group",
            "2",
            "DO",
            "Other School",
            "1999",
            "20 Example St",
        ],
        ["1000000004", "0004", "", "site-c", "", "", "NP", "School Only", "", ""],
        ["1000000004", "0005", "", "site-c", "", "", "NP", "OTHER", "1998", ""],
        ["1000000004", "0006", "000010", "site-b", "Synthetic Group", "2", "PA", "", "", "20 Example St"],
    ]


def _write_replay_distribution(tmp_path, extension):
    source_file_path = tmp_path / "doctors.csv"
    with source_file_path.open("w", newline="") as source_file:
        writer = csv.writer(source_file)
        writer.writerow(
            [
                "NPI",
                "Ind_enrl_ID",
                "Org_PAC_ID",
                "adrs_id",
                "Facility Name",
                "num_org_mem",
                "Cred",
                "Med_sch",
                "Grd_yr",
                "adr_ln_1",
                "citytown",
                "state",
                "zip_code",
            ]
        )
        writer.writerows([*row, "Example City", "NY", "10001"] for row in _replay_source_rows())
    if extension == ".zip":
        distribution = tmp_path / "doctors.zip"
        with zipfile.ZipFile(distribution, "w") as archive:
            archive.write(source_file_path, "doctors.csv")
    else:
        distribution = source_file_path
    return distribution


def _mock_replay_staging(native, monkeypatch):
    staged_by_family = {"education": [], "group": [], "address": []}

    async def collect(batch, stage):
        family = (
            "education"
            if stage.__tablename__.startswith("cms_doctor_education")
            else "group"
            if stage.__tablename__.startswith("cms_doctor_group_site")
            else "address"
        )
        staged_by_family[family].extend(dict(row) for row in batch)

    monkeypatch.setattr(native, "ensure_database", AsyncMock())
    monkeypatch.setattr(native, "_create_stage_indexes", AsyncMock())
    monkeypatch.setattr(native.db, "create_table", AsyncMock())
    monkeypatch.setattr(native.db, "status", AsyncMock())
    for module in (native, education, groups):
        monkeypatch.setattr(module, "push_objects", collect)
        monkeypatch.setattr(module, "raise_if_cancelled", AsyncMock())
    return staged_by_family


def _forbid_replay_acquisition(native, monkeypatch):
    clock = Mock(side_effect=AssertionError("retained replay must not reacquire observation time"))
    monkeypatch.setattr(education, "datetime", SimpleNamespace(utcnow=clock, fromisoformat=datetime.fromisoformat))
    monkeypatch.setattr(
        native,
        "datetime",
        SimpleNamespace(datetime=SimpleNamespace(utcnow=clock, fromisoformat=datetime.fromisoformat)),
    )
    monkeypatch.setattr(
        native,
        "_fetch_doctors_download_url",
        AsyncMock(side_effect=AssertionError("retained replay must not download")),
    )


async def _retained_replay_source(tmp_path, monkeypatch, extension):
    """Retain original CSV/ZIP staging facts, then forbid current acquisition."""
    native = importlib.import_module("process.cms_doctors")
    distribution = _write_replay_distribution(tmp_path, extension)
    retained_root = tmp_path / "retained"
    monkeypatch.setattr(artifact, "_artifact_root", lambda: retained_root)
    monkeypatch.setattr(
        education,
        "datetime",
        SimpleNamespace(utcnow=lambda: datetime(2020, 12, 31), fromisoformat=datetime.fromisoformat),
    )
    staged_by_family = _mock_replay_staging(native, monkeypatch)
    ctx_by_field = {"import_date": "original", "context": {}}
    url = "https://example.test/doctors" + extension
    await native._stage_doctors_sidecars(distribution, url, ctx_by_field, {}, False)
    address_count = await native._import_doctors_source(
        distribution,
        ctx=ctx_by_field,
        task={},
        stage_cls=native.make_class(native.DoctorClinicianAddress, "original"),
        batch_size=2,
        test_mode=False,
        test_row_limit=5000,
    )
    manifest = provenance.mint_doctors_source_provenance(
        {**ctx_by_field["context"], "rows": address_count, "organization_groups": 2, "sites": 3}
    )
    manifest_file = tmp_path / "source-manifest.json"
    manifest_file.write_text(manifest)
    original_by_family = {name: list(family_rows) for name, family_rows in staged_by_family.items()}
    for family_rows in staged_by_family.values():
        family_rows.clear()
    distribution.unlink()
    _forbid_replay_acquisition(native, monkeypatch)
    return native, manifest, manifest_file, staged_by_family, original_by_family, retained_root


@pytest.mark.asyncio
@pytest.mark.parametrize("extension", [".csv", ".zip"])
async def test_retained_file_and_original_manifest_replay_all_source_observations(tmp_path, monkeypatch, extension):
    native, manifest, manifest_file, staged, original_rows, _ = await _retained_replay_source(
        tmp_path, monkeypatch, extension
    )
    redis = SimpleNamespace(enqueue_job=AsyncMock())
    monkeypatch.setattr(native, "create_pool", AsyncMock(return_value=redis))
    await native.main(retained_source_manifest=str(manifest_file))
    task_by_field = redis.enqueue_job.await_args.args[1]
    assert task_by_field["cms_doctors_retained_source_provenance"] == manifest
    ctx_by_field = {"import_date": "replay", "context": {}}
    await native.import_cms_doctors_data(ctx_by_field, task_by_field)
    assert staged == original_rows
    assert ctx_by_field["context"]["education"] == json.loads(manifest)["education"]
    assert ctx_by_field["context"]["run"] == 1
    assert len(staged["group"]) == 6 and len(staged["address"]) == 2
    assert [source_row["org_pac_id"] for source_row in staged["group"][:3]] == ["000010", "000020", "000010"]
    assert staged["group"][0]["adrs_id"] == staged["group"][1]["adrs_id"] == "site-" + "a" * 200
    assert staged["group"][0]["address_checksum"] == staged["group"][1]["address_checksum"]
    assert staged["group"][0]["adrs_id"] != staged["group"][2]["adrs_id"]
    assert staged["group"][3]["org_pac_id"] is None
    assert all(
        source_row["membership_start_at"] is None and source_row["membership_end_at"] is None
        for source_row in staged["group"]
    )
    assert {(source_row["medical_school"], source_row["graduation_year"]) for source_row in staged["education"]} == {
        ("Synthetic School", 2021),
        ("Other School", 1999),
        ("School Only", None),
        (None, 1998),
    }
    assert staged["education"][0]["source_json"]["quality_flags"] == ["graduation_year_in_future"]
    assert [source_row["source_json"]["raw_fields"]["cred"] for source_row in staged["group"]] == [
        "MD",
        "MD",
        "DO",
        "NP",
        "NP",
        "PA",
    ]
    retained = artifact.verify_doctors_artifact(json.loads(manifest)["artifact"])
    with education.open_doctors_csv(retained) as reader:
        assert [source_row["Cred"] for source_row in reader] == ["MD", "MD", "DO", "NP", "NP", "PA"]
    native._fetch_doctors_download_url.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["bytes", "during_parse", "closed_manifest", "row_counts"])
async def test_retained_replay_rejects_changed_source_or_manifest_without_completion(tmp_path, monkeypatch, change):
    native, manifest, _, staged, _, retained_root = await _retained_replay_source(tmp_path, monkeypatch, ".csv")
    task_by_field = json.loads(manifest)
    if change == "bytes":
        (retained_root / task_by_field["artifact"]["file_name"]).write_bytes(b"changed")
    elif change == "during_parse":
        source_importer = native._import_doctors_source

        async def mutate_after_parse(source_path, **options):
            accepted_rows = await source_importer(source_path, **options)
            with source_path.open("ab") as source_file:
                source_file.write(b"changed after parsing")
            return accepted_rows

        monkeypatch.setattr(native, "_import_doctors_source", mutate_after_parse)
    elif change == "closed_manifest":
        task_by_field["unexpected"] = True
    else:
        task_by_field["education"]["education_rows"] += 1
    ctx_by_field = {"import_date": "replayfailure", "context": {}}
    with pytest.raises((ValueError, RuntimeError), match="cms_doctors_"):
        await native.import_cms_doctors_data(
            ctx_by_field,
            {
                "cms_doctors_retained_source_provenance": json.dumps(
                    task_by_field, sort_keys=True, separators=(",", ":")
                )
            },
        )
    assert not ctx_by_field["context"].get("run")
    assert not ctx_by_field["context"].get("education_stage_owned")
    assert not ctx_by_field["context"].get("group_site_stage_owned")
    if change in {"bytes", "closed_manifest"}:
        assert not any(staged.values())


@pytest.mark.asyncio
async def test_retained_manifest_is_validated_before_queue_creation(tmp_path, monkeypatch):
    native = importlib.import_module("process.cms_doctors")
    pool = AsyncMock()
    monkeypatch.setattr(native, "create_pool", pool)
    manifest_file = tmp_path / "invalid-manifest.json"
    for value in ("{}", " " * (64 * 1024 + 1)):
        manifest_file.write_text(value)
        with pytest.raises(ValueError, match="cms_doctors_"):
            await native.main(retained_source_manifest=str(manifest_file))
    pool.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("changed_count", [None, "organization_groups", "sites"])
async def test_retained_manifest_fences_group_site_preparation_before_publication(tmp_path, monkeypatch, changed_count):
    native = importlib.import_module("process.cms_doctors")
    original = provenance.read_doctors_source_provenance(
        provenance.mint_doctors_source_provenance(source_metrics(tmp_path, monkeypatch))
    )
    ctx_by_field = {"import_date": "replay", "context": {"retained_source": original}}
    monkeypatch.setattr(native, "DEFAULT_MIN_ROWS", 1)
    monkeypatch.setattr(
        native,
        "_validate_cms_doctors_publication_sources",
        AsyncMock(return_value=(original["education"], original["group_site"])),
    )
    monkeypatch.setattr(
        native, "bind_group_site_organizations", AsyncMock(return_value=int(changed_count == "organization_groups"))
    )
    monkeypatch.setattr(native, "bind_cms_doctors_sites", AsyncMock(return_value=int(changed_count == "sites")))
    resolver = AsyncMock(return_value=None)
    monkeypatch.setattr(native, "_resolve_cms_doctors_addresses", resolver)
    monkeypatch.setattr(native, "raise_if_cancelled", AsyncMock())
    if changed_count:
        with pytest.raises(RuntimeError, match="retained_source_binding_counts_changed"):
            await native._prepare_cms_doctors_sources(ctx_by_field, object(), "mrf", 1)
        resolver.assert_not_awaited()
    else:
        metrics = await native._prepare_cms_doctors_sources(ctx_by_field, object(), "mrf", 1)
        assert (metrics["organization_groups"], metrics["sites"]) == (0, 0)
        resolver.assert_awaited_once()
