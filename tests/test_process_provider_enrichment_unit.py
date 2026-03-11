import importlib
import json
import shutil
import zipfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

pytest.importorskip("pytz")

provider_enrichment = importlib.import_module("process.provider_enrichment")


def test_archived_identifier_truncates_long_name():
    long_name = "x" * 120
    archived = provider_enrichment._archived_identifier(long_name)
    assert len(archived) <= 63
    assert archived.endswith("_old")


@pytest.mark.asyncio
async def test_discover_sources_returns_registered_and_unmapped(monkeypatch):
    catalog = {
        "dataset": [
            {
                "title": "Hospital Enrollments",
                "distribution": [
                    {
                        "downloadURL": "https://example.com/hospital.csv",
                        "mediaType": "text/csv",
                        "title": "Hospital Enrollments : 2026-02-01",
                        "modified": "2026-03-04",
                        "temporal": "2026-02-01/2026-02-28",
                    }
                ],
            },
            {
                "title": "Unknown Provider Enrollments",
                "distribution": [
                    {
                        "downloadURL": "https://example.com/unknown.csv",
                        "mediaType": "text/csv",
                        "title": "Unknown Provider Enrollments : 2026-02-01",
                        "modified": "2026-03-01",
                        "temporal": "2026-02-01/2026-02-28",
                    }
                ],
            },
        ]
    }

    monkeypatch.setattr(provider_enrichment, "STRICT_SOURCE_PRESENCE", False)
    monkeypatch.setattr(provider_enrichment, "download_it", AsyncMock(return_value=json.dumps(catalog)))

    sources, unmapped = await provider_enrichment._discover_sources(test_mode=True)

    assert len(sources) == 1
    assert sources[0]["spec_key"] == "hospital"
    assert "Unknown Provider Enrollments" in unmapped


@pytest.mark.asyncio
async def test_discover_sources_includes_latest_ffs_resource_bundle(monkeypatch):
    catalog = {
        "dataset": [
            {
                "title": "Medicare Fee-For-Service  Public Provider Enrollment",
                "distribution": [
                    {
                        "description": "latest",
                        "format": "API",
                        "resourcesAPI": "https://example.com/ffs-resources",
                        "modified": "2026-01-15",
                        "temporal": "2025-10-01/2025-12-31",
                    }
                ],
            }
        ]
    }
    resources = {
        "data": [
            {
                "name": "Medicare FFS Public Provider Enrollment Q4 2025",
                "downloadURL": "https://example.com/ffs.csv",
            },
            {
                "name": "Additional NPIs Sub-File Q4 2025",
                "downloadURL": "https://example.com/additional.csv",
            },
            {
                "name": "Reassignment Sub-File Q4 2025",
                "downloadURL": "https://example.com/reassignment.csv",
            },
            {
                "name": "Address Sub-File Q4 2025",
                "downloadURL": "https://example.com/address.csv",
            },
            {
                "name": "Secondary Specialty Sub-File Q4 2025",
                "downloadURL": "https://example.com/secondary.csv",
            },
            {
                "name": "Medicare FFS  Public Provider Enrollment Methodology (Current)",
                "downloadURL": "https://example.com/methodology.pdf",
            },
            {
                "name": "Historical Medicare FFS Public Provider Enrollment Data 2021-2022",
                "downloadURL": "https://example.com/historical.zip",
            },
        ]
    }

    async def fake_download(url: str):
        if url == provider_enrichment.CATALOG_URL:
            return json.dumps(catalog)
        if url == "https://example.com/ffs-resources":
            return json.dumps(resources)
        raise AssertionError(f"unexpected URL: {url}")

    monkeypatch.setattr(provider_enrichment, "STRICT_SOURCE_PRESENCE", False)
    monkeypatch.setattr(provider_enrichment, "download_it", fake_download)

    sources, unmapped = await provider_enrichment._discover_sources(test_mode=False)

    assert unmapped == []
    assert {src["spec_key"] for src in sources} == {
        "ffs_public",
        "ffs_additional_npi",
        "ffs_reassignment",
        "ffs_address",
        "ffs_secondary_specialty",
    }
    assert all(src["reporting_year"] == 2025 for src in sources)


@pytest.mark.asyncio
async def test_discover_sources_non_test_defaults_to_latest_only(monkeypatch):
    catalog = {
        "dataset": [
            {
                "title": "Hospital Enrollments",
                "distribution": [
                    {
                        "downloadURL": "https://example.com/hospital-older.csv",
                        "mediaType": "text/csv",
                        "title": "Hospital Enrollments : 2026-01-01",
                        "modified": "2026-01-15",
                        "temporal": "2026-01-01/2026-01-31",
                    },
                    {
                        "downloadURL": "https://example.com/hospital-latest.csv",
                        "mediaType": "text/csv",
                        "title": "Hospital Enrollments : 2026-02-01",
                        "modified": "2026-02-15",
                        "temporal": "2026-02-01/2026-02-28",
                    },
                ],
            }
        ]
    }

    monkeypatch.setattr(provider_enrichment, "STRICT_SOURCE_PRESENCE", False)
    monkeypatch.setattr(provider_enrichment, "INCLUDE_PROVIDER_ENRICHMENT_HISTORY", False)
    monkeypatch.setenv("HLTHPRT_PROVIDER_ENRICHMENT_MAX_SOURCES_PER_DATASET", "1")
    monkeypatch.setattr(provider_enrichment, "download_it", AsyncMock(return_value=json.dumps(catalog)))

    sources, _ = await provider_enrichment._discover_sources(test_mode=False)

    assert len(sources) == 1
    assert sources[0]["download_url"] == "https://example.com/hospital-latest.csv"


@pytest.mark.asyncio
async def test_discover_sources_history_mode_includes_multiple_distributions(monkeypatch):
    catalog = {
        "dataset": [
            {
                "title": "Hospital Enrollments",
                "distribution": [
                    {
                        "downloadURL": "https://example.com/hospital-older.csv",
                        "mediaType": "text/csv",
                        "title": "Hospital Enrollments : 2026-01-01",
                        "modified": "2026-01-15",
                        "temporal": "2026-01-01/2026-01-31",
                    },
                    {
                        "downloadURL": "https://example.com/hospital-latest.csv",
                        "mediaType": "text/csv",
                        "title": "Hospital Enrollments : 2026-02-01",
                        "modified": "2026-02-15",
                        "temporal": "2026-02-01/2026-02-28",
                    },
                ],
            }
        ]
    }

    monkeypatch.setattr(provider_enrichment, "STRICT_SOURCE_PRESENCE", False)
    monkeypatch.setattr(provider_enrichment, "INCLUDE_PROVIDER_ENRICHMENT_HISTORY", True)
    monkeypatch.setattr(provider_enrichment, "download_it", AsyncMock(return_value=json.dumps(catalog)))

    sources, _ = await provider_enrichment._discover_sources(test_mode=False)

    assert len(sources) == 2
    assert {src["download_url"] for src in sources} == {
        "https://example.com/hospital-latest.csv",
        "https://example.com/hospital-older.csv",
    }


def test_validate_headers_fails_when_required_missing():
    spec = provider_enrichment.SPEC_BY_KEY["ffs_public"]
    headers = ["NPI", "PROVIDER_TYPE_CD", "PROVIDER_TYPE_DESC"]

    with pytest.raises(RuntimeError, match="missing required mapped fields"):
        provider_enrichment._validate_headers(headers, spec, "FFS")


def test_build_row_payload_drops_missing_npi():
    spec = provider_enrichment.SPEC_BY_KEY["hospital"]
    model_columns = provider_enrichment._model_columns(spec["model"])
    source = {
        "dataset_title": "Hospital Enrollments",
        "distribution_title": "Hospital Enrollments : 2026-02-01",
        "download_url": "https://example.com/hospital.csv",
        "source_modified": None,
        "source_temporal": "2026-02-01/2026-02-28",
        "reporting_period_start": None,
        "reporting_period_end": None,
        "reporting_year": 2026,
    }
    row = {
        "NPI": "",
        "ENROLLMENT ID": "E123",
        "PROVIDER TYPE CODE": "44",
        "PROVIDER TYPE TEXT": "Hospital",
    }

    payload, reason = provider_enrichment._build_row_payload(row, spec, source, model_columns)
    assert payload is None
    assert reason == "missing_npi"


def test_build_row_payload_maps_ffs_fields():
    spec = provider_enrichment.SPEC_BY_KEY["ffs_public"]
    model_columns = provider_enrichment._model_columns(spec["model"])
    source = {
        "dataset_title": "Medicare Fee-For-Service Public Provider Enrollment",
        "distribution_title": "Medicare Fee-For-Service Public Provider Enrollment : 2026-01-01",
        "download_url": "https://example.com/ffs.csv",
        "source_modified": None,
        "source_temporal": "2026-01-01/2026-01-31",
        "reporting_period_start": None,
        "reporting_period_end": None,
        "reporting_year": 2026,
    }
    row = {
        "NPI": "1234567890",
        "ENRLMT_ID": "ENR-1",
        "PROVIDER_TYPE_CD": "01",
        "PROVIDER_TYPE_DESC": "Physician",
        "STATE_CD": "tx",
        "FIRST_NAME": "Ada",
        "MDL_NAME": "L",
        "LAST_NAME": "Lovelace",
        "ORG_NAME": "Ada Clinic",
    }

    payload, reason = provider_enrichment._build_row_payload(row, spec, source, model_columns)

    assert reason is None
    assert payload is not None
    assert payload["npi"] == 1234567890
    assert payload["state"] == "TX"
    assert payload["first_name"] == "Ada"
    assert payload["last_name"] == "Lovelace"
    assert isinstance(payload["record_hash"], int)


def test_build_row_payload_maps_ffs_additional_npi_fields():
    spec = provider_enrichment.SPEC_BY_KEY["ffs_additional_npi"]
    model_columns = provider_enrichment._model_columns(spec["model"])
    source = {
        "dataset_title": "Medicare Fee-For-Service  Public Provider Enrollment",
        "distribution_title": "Additional NPIs Sub-File Q4 2025",
        "download_url": "https://example.com/additional.csv",
        "source_modified": None,
        "source_temporal": "2025-10-01/2025-12-31",
        "reporting_period_start": None,
        "reporting_period_end": None,
        "reporting_year": 2025,
    }
    row = {
        "ENRLMT_ID": "O20020815000031",
        "NPI": "1730834656",
    }

    payload, reason = provider_enrichment._build_row_payload(row, spec, source, model_columns)

    assert reason is None
    assert payload["enrollment_id"] == "O20020815000031"
    assert payload["additional_npi"] == 1730834656
    assert isinstance(payload["record_hash"], int)


def test_build_row_payload_maps_ffs_reassignment_fields():
    spec = provider_enrichment.SPEC_BY_KEY["ffs_reassignment"]
    model_columns = provider_enrichment._model_columns(spec["model"])
    source = {
        "dataset_title": "Medicare Fee-For-Service  Public Provider Enrollment",
        "distribution_title": "Reassignment Sub-File Q4 2025",
        "download_url": "https://example.com/reassignment.csv",
        "source_modified": None,
        "source_temporal": "2025-10-01/2025-12-31",
        "reporting_period_start": None,
        "reporting_period_end": None,
        "reporting_year": 2025,
    }
    row = {
        "REASGN_BNFT_ENRLMT_ID": "I20031103000001",
        "RCV_BNFT_ENRLMT_ID": "O20031216000213",
    }

    payload, reason = provider_enrichment._build_row_payload(row, spec, source, model_columns)

    assert reason is None
    assert payload["reassigning_enrollment_id"] == "I20031103000001"
    assert payload["receiving_enrollment_id"] == "O20031216000213"
    assert isinstance(payload["record_hash"], int)


@pytest.mark.asyncio
async def test_save_provider_enrichment_data_dispatch(monkeypatch):
    push_calls = []

    async def fake_push(rows, cls, rewrite=False, use_copy=True):
        push_calls.append((cls.__tablename__, rewrite, use_copy, rows))

    monkeypatch.setattr(provider_enrichment, "ensure_database", AsyncMock())
    monkeypatch.setattr(provider_enrichment, "push_objects", fake_push)
    monkeypatch.setattr(
        provider_enrichment,
        "make_class",
        lambda model, suffix: SimpleNamespace(__tablename__=f"{model.__tablename__}_{suffix}"),
    )

    ctx = {"import_date": "20260306", "context": {"test_mode": True}}
    task = {
        "hospital_rows": [{"record_hash": 1, "npi": 1234567890}],
        "unknown_key": [{"record_hash": 2, "npi": 1234567891}],
    }

    await provider_enrichment.save_provider_enrichment_data(ctx, task)

    assert len(push_calls) == 1
    assert push_calls[0][0] == "provider_enrollment_hospital_20260306"
    assert push_calls[0][1] is True
    assert push_calls[0][2] is False


@pytest.mark.asyncio
async def test_run_nppes_gap_check_detects_medical_school(monkeypatch, tmp_path):
    zip_name = "NPPES_Data_Dissemination_20260101_20260131_V2.zip"
    local_zip = tmp_path / zip_name
    csv_name = "npidata_pfile_20260101-20260131.csv"

    with zipfile.ZipFile(local_zip, "w") as archive:
        archive.writestr(
            csv_name,
            "NPI,Provider First Name,Provider Medical School Name\n1234567890,Ada,Sample Medical School\n",
        )

    monkeypatch.setenv("HLTHPRT_NPPES_DOWNLOAD_URL_DIR", "https://example.com/")
    monkeypatch.setenv("HLTHPRT_NPPES_DOWNLOAD_URL_FILE", "index.html")
    monkeypatch.setattr(
        provider_enrichment,
        "download_it",
        AsyncMock(return_value=f'<a href="{zip_name}">zip</a>'),
    )

    async def fake_download(_url: str, target_path: str):
        shutil.copyfile(local_zip, target_path)

    monkeypatch.setattr(provider_enrichment, "_download_source", fake_download)

    ctx: dict[str, object] = {"context": {}}
    report = await provider_enrichment._run_nppes_gap_check(ctx)

    assert report["checked"] is True
    assert "Provider Medical School Name" in report["medical_school_headers"]
    assert report["unmapped_field_count"] >= 1
