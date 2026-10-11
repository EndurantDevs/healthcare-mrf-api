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
    catalog_map = {
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
    monkeypatch.setattr(provider_enrichment, "download_it", AsyncMock(return_value=json.dumps(catalog_map)))

    discovered_sources, unmapped = await provider_enrichment._discover_sources(test_mode=True)

    assert len(discovered_sources) == 1
    assert discovered_sources[0]["spec_key"] == "hospital"
    assert "Unknown Provider Enrollments" in unmapped


def _latest_ffs_resource_catalogs():
    catalog_map = {
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
    resource_map = {
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
    return catalog_map, resource_map


@pytest.mark.asyncio
async def test_discover_sources_includes_latest_ffs_resource_bundle(monkeypatch):
    """Verify discover sources includes latest ffs resource bundle."""
    catalog_map, resource_map = _latest_ffs_resource_catalogs()

    async def fake_download(url: str):
        if url == provider_enrichment.CATALOG_URL:
            return json.dumps(catalog_map)
        if url == "https://example.com/ffs-resources":
            return json.dumps(resource_map)
        raise AssertionError(f"unexpected URL: {url}")

    monkeypatch.setattr(provider_enrichment, "STRICT_SOURCE_PRESENCE", False)
    monkeypatch.setattr(provider_enrichment, "download_it", fake_download)

    discovered_sources, unmapped = await provider_enrichment._discover_sources(test_mode=False)

    assert unmapped == []
    assert {src["spec_key"] for src in discovered_sources} == {
        "ffs_public",
        "ffs_additional_npi",
        "ffs_reassignment",
        "ffs_address",
        "ffs_secondary_specialty",
    }
    assert all(src["reporting_year"] == 2025 for src in discovered_sources)


@pytest.mark.asyncio
async def test_discover_sources_non_test_defaults_to_latest_only(monkeypatch):
    catalog_map = {
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
    monkeypatch.setattr(provider_enrichment, "download_it", AsyncMock(return_value=json.dumps(catalog_map)))

    discovered_sources, _ = await provider_enrichment._discover_sources(test_mode=False)

    assert len(discovered_sources) == 1
    assert discovered_sources[0]["download_url"] == "https://example.com/hospital-latest.csv"


@pytest.mark.asyncio
async def test_discover_sources_history_mode_includes_multiple_distributions(monkeypatch):
    catalog_map = {
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
    monkeypatch.setattr(provider_enrichment, "download_it", AsyncMock(return_value=json.dumps(catalog_map)))

    discovered_sources, _ = await provider_enrichment._discover_sources(test_mode=False)

    assert len(discovered_sources) == 2
    assert {src["download_url"] for src in discovered_sources} == {
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
    source_metadata_map = {
        "dataset_title": "Hospital Enrollments",
        "distribution_title": "Hospital Enrollments : 2026-02-01",
        "download_url": "https://example.com/hospital.csv",
        "source_modified": None,
        "source_temporal": "2026-02-01/2026-02-28",
        "reporting_period_start": None,
        "reporting_period_end": None,
        "reporting_year": 2026,
    }
    source_row_map = {
        "NPI": "",
        "ENROLLMENT ID": "E123",
        "PROVIDER TYPE CODE": "44",
        "PROVIDER TYPE TEXT": "Hospital",
    }

    provider_row_payload, reason = provider_enrichment._build_row_payload(
        source_row_map, spec, source_metadata_map, model_columns
    )
    assert provider_row_payload is None
    assert reason == "missing_npi"


def test_build_row_payload_maps_ffs_fields():
    spec = provider_enrichment.SPEC_BY_KEY["ffs_public"]
    model_columns = provider_enrichment._model_columns(spec["model"])
    source_metadata_map = {
        "dataset_title": "Medicare Fee-For-Service Public Provider Enrollment",
        "distribution_title": "Medicare Fee-For-Service Public Provider Enrollment : 2026-01-01",
        "download_url": "https://example.com/ffs.csv",
        "source_modified": None,
        "source_temporal": "2026-01-01/2026-01-31",
        "reporting_period_start": None,
        "reporting_period_end": None,
        "reporting_year": 2026,
    }
    source_row_map = {
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

    provider_row_payload, reason = provider_enrichment._build_row_payload(
        source_row_map, spec, source_metadata_map, model_columns
    )

    assert reason is None
    assert provider_row_payload is not None
    assert provider_row_payload["npi"] == 1234567890
    assert provider_row_payload["state"] == "TX"
    assert provider_row_payload["first_name"] == "Ada"
    assert provider_row_payload["last_name"] == "Lovelace"
    assert isinstance(provider_row_payload["record_hash"], int)


def test_build_row_payload_maps_ffs_additional_npi_fields():
    spec = provider_enrichment.SPEC_BY_KEY["ffs_additional_npi"]
    model_columns = provider_enrichment._model_columns(spec["model"])
    source_metadata_map = {
        "dataset_title": "Medicare Fee-For-Service  Public Provider Enrollment",
        "distribution_title": "Additional NPIs Sub-File Q4 2025",
        "download_url": "https://example.com/additional.csv",
        "source_modified": None,
        "source_temporal": "2025-10-01/2025-12-31",
        "reporting_period_start": None,
        "reporting_period_end": None,
        "reporting_year": 2025,
    }
    source_row_map = {
        "ENRLMT_ID": "O20020815000031",
        "NPI": "1730834656",
    }

    provider_row_payload, reason = provider_enrichment._build_row_payload(
        source_row_map, spec, source_metadata_map, model_columns
    )

    assert reason is None
    assert provider_row_payload["enrollment_id"] == "O20020815000031"
    assert provider_row_payload["additional_npi"] == 1730834656
    assert isinstance(provider_row_payload["record_hash"], int)


def test_build_row_payload_maps_ffs_reassignment_fields():
    spec = provider_enrichment.SPEC_BY_KEY["ffs_reassignment"]
    model_columns = provider_enrichment._model_columns(spec["model"])
    source_metadata_map = {
        "dataset_title": "Medicare Fee-For-Service  Public Provider Enrollment",
        "distribution_title": "Reassignment Sub-File Q4 2025",
        "download_url": "https://example.com/reassignment.csv",
        "source_modified": None,
        "source_temporal": "2025-10-01/2025-12-31",
        "reporting_period_start": None,
        "reporting_period_end": None,
        "reporting_year": 2025,
    }
    source_row_map = {
        "REASGN_BNFT_ENRLMT_ID": "I20031103000001",
        "RCV_BNFT_ENRLMT_ID": "O20031216000213",
    }

    provider_row_payload, reason = provider_enrichment._build_row_payload(
        source_row_map, spec, source_metadata_map, model_columns
    )

    assert reason is None
    assert provider_row_payload["reassigning_enrollment_id"] == "I20031103000001"
    assert provider_row_payload["receiving_enrollment_id"] == "O20031216000213"
    assert isinstance(provider_row_payload["record_hash"], int)


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

    context_map = {"import_date": "20260306", "context": {"test_mode": True}}
    task_map = {
        "hospital_rows": [{"record_hash": 1, "npi": 1234567890}],
        "unknown_key": [{"record_hash": 2, "npi": 1234567891}],
    }

    await provider_enrichment.save_provider_enrichment_data(context_map, task_map)

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

    context_map: dict[str, object] = {"context": {}}
    report = await provider_enrichment._run_nppes_gap_check(context_map)

    assert report["checked"] is True
    assert "Provider Medical School Name" in report["medical_school_headers"]
    assert report["unmapped_field_count"] >= 1


def _complete_source_catalogs():
    catalog_map, resource_map = _latest_ffs_resource_catalogs()
    for spec_key, title in (
        ("hospital", "Hospital Enrollments"),
        ("hha", "Home Health Agency Enrollments"),
        ("hospice", "Hospice Enrollments"),
        ("fqhc", "Federally Qualified Health Center Enrollments"),
        ("rhc", "Rural Health Clinic Enrollments"),
        ("snf", "Skilled Nursing Facility Enrollments"),
    ):
        catalog_map["dataset"].append(
            {
                "title": title,
                "distribution": [
                    {
                        "title": f"{title} 2026",
                        "downloadURL": f"https://example.com/{spec_key}.csv",
                        "mediaType": "text/csv",
                        "modified": "2026-02-01",
                    }
                ],
            }
        )
    return catalog_map, resource_map


@pytest.mark.asyncio
async def test_strict_discovery_ignores_noise_and_duplicate_sources(monkeypatch):
    catalog_map, resource_map = _complete_source_catalogs()
    latest_distributions = catalog_map["dataset"][0]["distribution"]
    latest_distributions[:0] = [None, {"description": "historical"}, {"description": "latest"}]
    hospital_distributions = catalog_map["dataset"][1]["distribution"]
    hospital_distributions.extend(
        [
            None,
            {"mediaType": "text/csv"},
            {"downloadURL": "https://example.com/hospital.pdf", "mediaType": "application/pdf"},
            dict(hospital_distributions[0]),
        ]
    )
    catalog_map["dataset"].extend([None, {}, {"title": "Monthly Enrollment"}])
    resource_map["data"].extend(
        [
            None,
            {"name": "Missing download"},
            {"name": "Unregistered resource", "downloadURL": "https://example.com/unregistered.csv"},
            dict(resource_map["data"][0]),
        ]
    )
    download = AsyncMock(side_effect=[json.dumps(catalog_map), json.dumps(resource_map)])
    monkeypatch.setattr(provider_enrichment, "download_it", download)
    monkeypatch.setattr(provider_enrichment, "STRICT_SOURCE_PRESENCE", True)
    monkeypatch.setattr(provider_enrichment, "INCLUDE_PROVIDER_ENRICHMENT_HISTORY", True)

    discovered_sources, unmapped = await provider_enrichment._discover_sources(test_mode=False)

    assert unmapped == []
    assert len(discovered_sources) == 11
    assert {source_map["spec_key"] for source_map in discovered_sources} == {
        "ffs_public",
        "ffs_additional_npi",
        "ffs_reassignment",
        "ffs_address",
        "ffs_secondary_specialty",
        "hospital",
        "hha",
        "hospice",
        "fqhc",
        "rhc",
        "snf",
    }
    assert len({source_map["download_url"] for source_map in discovered_sources}) == 11
    assert [call.args[0] for call in download.await_args_list] == [
        provider_enrichment.CATALOG_URL,
        "https://example.com/ffs-resources",
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("datasets", "message"),
    [
        ([], "No FFS provider-enrollment dataset"),
        ([{"title": "Medicare Fee-For-Service Public Provider Enrollment"}], "missing a latest resources API"),
        (
            [
                {
                    "title": "Medicare Fee-For-Service Public Provider Enrollment",
                    "distribution": [None, {"description": "historical"}, {"description": "latest"}],
                }
            ],
            "missing a latest resources API",
        ),
    ],
)
async def test_strict_discovery_requires_a_latest_resource_api(monkeypatch, datasets, message):
    download = AsyncMock(return_value=json.dumps({"dataset": datasets}))
    monkeypatch.setattr(provider_enrichment, "download_it", download)
    monkeypatch.setattr(provider_enrichment, "STRICT_SOURCE_PRESENCE", True)

    with pytest.raises(RuntimeError, match=message):
        await provider_enrichment._discover_sources(test_mode=False)

    download.assert_awaited_once_with(provider_enrichment.CATALOG_URL)


@pytest.mark.asyncio
async def test_optional_discovery_skips_bundle_without_resource_api(monkeypatch):
    catalog_map, _ = _latest_ffs_resource_catalogs()
    catalog_map["dataset"][0]["distribution"][0].pop("resourcesAPI")
    download = AsyncMock(return_value=json.dumps(catalog_map))
    monkeypatch.setattr(provider_enrichment, "download_it", download)
    monkeypatch.setattr(provider_enrichment, "STRICT_SOURCE_PRESENCE", False)

    assert await provider_enrichment._discover_sources(test_mode=False) == ([], [])

    download.assert_awaited_once_with(provider_enrichment.CATALOG_URL)


@pytest.mark.asyncio
async def test_strict_discovery_rejects_incomplete_subfile_bundle(monkeypatch):
    catalog_map, resource_map = _complete_source_catalogs()
    resource_map["data"].pop(3)
    download = AsyncMock(side_effect=[json.dumps(catalog_map), json.dumps(resource_map)])
    monkeypatch.setattr(provider_enrichment, "download_it", download)
    monkeypatch.setattr(provider_enrichment, "STRICT_SOURCE_PRESENCE", True)

    with pytest.raises(RuntimeError, match="missing required CSV files: ffs_address"):
        await provider_enrichment._discover_sources(test_mode=False)

    assert download.await_count == 2


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("remove_dataset", "message"),
    [
        (True, "No sources discovered for registered dataset 'Hospital Enrollments'"),
        (False, "Registered provider-enrichment dataset has no CSV distributions: Hospital Enrollments"),
    ],
)
async def test_strict_discovery_requires_each_registered_catalog(monkeypatch, remove_dataset, message):
    catalog_map, resource_map = _complete_source_catalogs()
    if remove_dataset:
        catalog_map["dataset"].pop(1)
    else:
        catalog_map["dataset"][1]["distribution"] = []
    download = AsyncMock(side_effect=[json.dumps(catalog_map), json.dumps(resource_map)])
    monkeypatch.setattr(provider_enrichment, "download_it", download)
    monkeypatch.setattr(provider_enrichment, "STRICT_SOURCE_PRESENCE", True)

    with pytest.raises(RuntimeError, match=message):
        await provider_enrichment._discover_sources(test_mode=False)

    assert download.await_count == 2


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("archive_members", "expected_error"),
    [
        ({"readme.txt": "No provider rows"}, "npidata_pfile*.csv was not found"),
        ({"npidata_pfile_sample.csv": ""}, None),
        (None, "File is not a zip file"),
    ],
)
async def test_nppes_archive_boundaries_remove_downloaded_files(monkeypatch, tmp_path, archive_members, expected_error):
    zip_name = "NPPES_Data_Dissemination_20260101_V2.zip"
    archive_path = tmp_path / zip_name
    if archive_members is None:
        archive_path.write_bytes(b"not a ZIP archive")
    else:
        with zipfile.ZipFile(archive_path, "w") as archive:
            for member_name, content in archive_members.items():
                archive.writestr(member_name, content)
    downloaded_paths = []

    async def copy_archive(url, target_path):
        assert url == f"https://example.com/{zip_name}"
        downloaded_paths.append(Path(target_path))
        shutil.copyfile(archive_path, target_path)

    monkeypatch.setenv("HLTHPRT_NPPES_DOWNLOAD_URL_DIR", "https://example.com/")
    monkeypatch.setenv("HLTHPRT_NPPES_DOWNLOAD_URL_FILE", "index.html")
    monkeypatch.setattr(provider_enrichment, "download_it", AsyncMock(return_value=f'<a href="{zip_name}">ZIP</a>'))
    monkeypatch.setattr(provider_enrichment, "_download_source", copy_archive)
    context_map = {"context": {}}

    report = await provider_enrichment._run_nppes_gap_check(context_map)

    assert report["source_zip"] == zip_name
    assert report["checked"] is (expected_error is None)
    assert len(downloaded_paths) == 1
    assert not downloaded_paths[0].exists()
    assert not downloaded_paths[0].parent.exists()
    if expected_error is None:
        assert report["error"] is None
        assert report["unmapped_fields"] == []
        assert report["medical_school_headers"] == []
        assert context_map["context"]["audit"]["nppes_gap_report"] is report
    else:
        assert expected_error in report["error"]


@pytest.mark.asyncio
async def test_nppes_listing_without_archives_does_not_download(monkeypatch):
    monkeypatch.setenv("HLTHPRT_NPPES_DOWNLOAD_URL_DIR", "https://example.com/")
    monkeypatch.setenv("HLTHPRT_NPPES_DOWNLOAD_URL_FILE", "index.html")
    listing_download = AsyncMock(return_value="<html>No archives available</html>")
    archive_download = AsyncMock()
    monkeypatch.setattr(provider_enrichment, "download_it", listing_download)
    monkeypatch.setattr(provider_enrichment, "_download_source", archive_download)

    report = await provider_enrichment._run_nppes_gap_check({"context": {}})

    assert report["checked"] is False
    assert report["error"] == "no NPPES dissemination zip links found"
    listing_download.assert_awaited_once_with("https://example.com/index.html")
    archive_download.assert_not_awaited()


@pytest.mark.asyncio
async def test_nppes_audit_without_configuration_does_not_fetch(monkeypatch):
    monkeypatch.delenv("HLTHPRT_NPPES_DOWNLOAD_URL_DIR", raising=False)
    monkeypatch.delenv("HLTHPRT_NPPES_DOWNLOAD_URL_FILE", raising=False)
    download = AsyncMock()
    monkeypatch.setattr(provider_enrichment, "download_it", download)

    report = await provider_enrichment._run_nppes_gap_check({"context": {}})

    assert report["checked"] is False
    assert "are not configured" in report["error"]
    download.assert_not_awaited()
