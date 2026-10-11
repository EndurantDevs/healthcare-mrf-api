# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import csv
import datetime
import io
import json
import zipfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import ANY, AsyncMock, Mock

import pytest

from process import initial


def _catalog_csv(rows):
    stream = io.StringIO()
    writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
    writer.writeheader()
    writer.writerows(rows)
    return "\ufeff" + stream.getvalue()


def _catalog_zip(members):
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w") as archive:
        for name, content in members.items():
            archive.writestr(name, content)
    return stream.getvalue()


@pytest.fixture
def catalog_archives(monkeypatch):
    archive_by_url = {}
    downloads = []

    async def download(source_url, destination):
        destination = Path(destination)
        downloads.append((source_url, destination.parent))
        destination.write_bytes(archive_by_url[source_url])

    monkeypatch.setattr(initial, "download_it_and_save", download)
    return archive_by_url, downloads


def _federal_attribute_row(**overrides):
    return {
        "StandardComponentId": "12345AA0000001",
        "PlanId": "12345AA0000001-00",
        "BusinessYear": "2026",
        "IssuerId": "12345",
        "StateCode": "aa",
        "PlanMarketingName": "Synthetic Plan, Silver",
        "URLForSummaryofBenefitsCoverage": "https://data.example.invalid/summary",
        "PlanBrochure": "https://data.example.invalid/brochure",
        "FormularyURL": "https://data.example.invalid/formulary",
        "NetworkId": "network-a",
        "ImportDate": "2026-01-02",
        "IssuerMarketPlaceMarketingName": "  Synthetic Issuer  ",
        **overrides,
    }


def _state_attribute_row(**overrides):
    return {
        "StandardComponentId": "23456BB0000001",
        "PlanId": "",
        "BusinessYear": "2026",
        "IssuerId": "23456",
        "StateCode": "bb",
        "ImportDate": "2026-02-03",
        "IssuerName": "  State Issuer  ",
        **overrides,
    }


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, None),
        (True, True),
        (False, False),
        (" YES ", True),
        ("1", True),
        ("no", False),
        ("0", False),
        ([], False),
        (["value"], True),
    ],
)
def test_optional_boolean_normalization_is_explicit(value, expected):
    assert initial._parse_optional_bool(value) is expected


class _Workbook:
    def __init__(self, worksheet_name, rows):
        self.ws_names = [worksheet_name]
        self._rows = rows

    def ws(self, *, ws):
        assert ws == self.ws_names[0]
        return SimpleNamespace(rows=self._rows)


def _initial_import_workbooks():
    transparency_rows = [
        [],
        [],
        [
            "State",
            "Issuer_Name",
            "Issuer_ID",
            "Is_Issuer_New_to_Exchange? (Yes_or_No)",
            "SADP_Only?",
            "Plan_ID",
            "QHP/SADP",
            "Plan_Type",
            "Metal_Level",
            "URL_Claims_Payment_Policies",
        ],
        [
            "aa",
            "Synthetic Issuer",
            12345,
            "Yes",
            "No",
            "12345AA0000000",
            "QHP",
            "PPO",
            "SILVER",
            "https://data.example.invalid/policies",
        ],
    ]
    index_rows = [
        ["State", "Issuer ID", "Index URL", "Contact"],
        [
            "aa",
            12345,
            json.dumps(
                [
                    "https://data.example.invalid/index-b.json",
                    "https://data.example.invalid/index-a.json",
                ]
            ),
            "data@example.invalid",
        ],
        ["bb", 23456, "https://data.example.invalid/index-c.json", ""],
        ["cc", 34567, "", ""],
    ]
    return {
        "/tmp/transparency.xlsx": _Workbook("Transparency 2026", transparency_rows),
        "/tmp/mrf-index.xlsx": _Workbook("Index", index_rows),
    }


def _install_initial_import_mocks(
    monkeypatch,
    workbook_by_path,
    pushed_batches,
    progress_events,
):
    workbook_paths = iter(workbook_by_path)

    async def push(rows, stage, **_kwargs):
        pushed_batches.append((stage.__name__, [dict(row) for row in rows]))

    monkeypatch.setenv(
        "HLTHPRT_CMSGOV_MRF_URL_PUF",
        json.dumps(["https://data.example.invalid/catalog.zip"]),
    )
    monkeypatch.setenv(
        "HLTHPRT_CMSGOV_PLAN_TRANSPARENCY_URL_PUF",
        json.dumps(
            [
                {"url": "https://data.example.invalid/transparency.zip", "year": 2026},
                {"url": "https://data.example.invalid/ignored.zip", "year": 2025},
            ]
        ),
    )
    monkeypatch.setenv("HLTHPRT_SAVE_PER_PACK", "0")
    for name in (
        "ensure_database",
        "mark_control_run",
        "_prepare_import_tables",
        "download_it_and_save",
        "unzip",
        "_init_mrf_run_state",
    ):
        monkeypatch.setattr(initial, name, AsyncMock())
    monkeypatch.setattr(initial.glob, "glob", lambda _pattern: [next(workbook_paths)])
    monkeypatch.setattr(initial.xl, "readxl", lambda path: workbook_by_path[path])
    monkeypatch.setattr(initial.os, "unlink", lambda _path: None)
    monkeypatch.setattr(initial, "make_class", lambda model, *_args, **_kwargs: model)
    monkeypatch.setattr(initial, "push_objects", push)


def _install_initial_import_catalog_mocks(monkeypatch, progress_events):
    async def has_registered_work(*_args, **_kwargs):
        return True

    monkeypatch.setattr(
        initial,
        "import_unknown_state_issuers_data",
        AsyncMock(
            return_value=(
                {
                    12345: {
                        "issuer_id": 12345,
                        "issuer_name": "",
                        "issuer_marketing_name": "",
                        "data_contact_email": "",
                        "mrf_url": "",
                    }
                },
                {"12345AA0000000_2026": {"plan_id": "12345AA0000000", "year": 2026}},
            )
        ),
    )
    monkeypatch.setattr(
        initial,
        "update_issuer_names_data",
        AsyncMock(
            return_value={
                99999: {
                    "issuer_id": 99999,
                    "issuer_name": "Reference Issuer",
                    "mrf_url": "",
                }
            }
        ),
    )
    monkeypatch.setattr(initial.db, "scalar", AsyncMock(return_value="Catalog Issuer"))
    monkeypatch.setattr(
        initial,
        "_has_registered_mrf_work",
        has_registered_work,
    )
    monkeypatch.setattr(
        initial,
        "enqueue_live_progress",
        lambda **event: progress_events.append(event),
    )


def _assert_initial_import_catalog_result(context_by_field, pushed_batches):
    assert context_by_field["context"] == {
        "import_date": "20260724",
        "run": 1,
        "test_mode": True,
        "control_run_id": "run-contract",
        "mrf_file_chunking": "providers,formularies",
    }
    transparency_rows = [
        pushed_row for label, row_dicts in pushed_batches for pushed_row in row_dicts if label == "PlanTransparency"
    ]
    assert transparency_rows == [
        {
            "state": "AA",
            "issuer_name": "Synthetic Issuer",
            "issuer_id": 12345,
            "new_issuer_to_exchange": True,
            "sadp_only": False,
            "plan_id": "12345AA0000000",
            "year": 2026,
            "qhp_sadp": "QHP",
            "plan_type": "PPO",
            "metal": "SILVER",
            "claims_payment_policies_url": "https://data.example.invalid/policies",
        }
    ]
    issuer_rows = [pushed_row for label, row_dicts in pushed_batches for pushed_row in row_dicts if label == "Issuer"]
    assert {issuer_row["issuer_id"] for issuer_row in issuer_rows} == {
        12345,
        23456,
        99999,
    }
    assert next(issuer_row for issuer_row in issuer_rows if issuer_row["issuer_id"] == 12345) == {
        "issuer_id": 12345,
        "issuer_name": "Synthetic Issuer",
        "issuer_marketing_name": "",
        "data_contact_email": "data@example.invalid",
        "mrf_url": "https://data.example.invalid/index-b.json",
    }
    assert initial.db.scalar.await_count == 0


def _assert_initial_import_queue_result(progress_events, redis):
    index_calls = [call for call in redis.enqueue_job.await_args_list if call.args[0] == "process_json_index"]
    assert [call.args[1]["url"] for call in index_calls] == [
        "https://data.example.invalid/index-a.json",
        "https://data.example.invalid/index-b.json",
    ]
    assert all(call.kwargs["_queue_name"] == initial.MRF_QUEUE_NAME for call in index_calls)
    shutdown_call = redis.enqueue_job.await_args_list[-1]
    assert shutdown_call.args[0] == "shutdown"
    assert shutdown_call.kwargs == {
        "_job_id": "shutdown_mrf_20260724",
        "_queue_name": initial.MRF_FINISH_QUEUE_NAME,
    }
    assert [event["phase"] for event in progress_events] == [
        "mrf issuer data staged",
        "mrf index jobs enqueued",
        "mrf index jobs enqueued",
    ]


@pytest.mark.asyncio
async def test_initial_import_stages_catalog_rows_and_enqueues_bounded_index_jobs(
    monkeypatch,
):
    """Stage catalog records before enqueuing a bounded, deterministic URL set."""
    workbook_by_path = _initial_import_workbooks()
    pushed_batches = []
    progress_events = []
    _install_initial_import_mocks(monkeypatch, workbook_by_path, pushed_batches, progress_events)
    _install_initial_import_catalog_mocks(monkeypatch, progress_events)
    redis = SimpleNamespace(enqueue_job=AsyncMock(return_value=SimpleNamespace()))
    context_by_field = {
        "redis": redis,
        "context": {"import_date": "20260724", "run": 0},
    }

    await initial.init_file(
        context_by_field,
        {
            "test_mode": True,
            "run_id": "run-contract",
            "mrf_file_chunking": "providers,formularies",
        },
    )
    _assert_initial_import_catalog_result(context_by_field, pushed_batches)
    _assert_initial_import_queue_result(progress_events, redis)


@pytest.mark.parametrize(
    ("raw_source", "message"),
    [
        ("[not-json", "must be JSON array or single URL"),
        ("[]", "did not provide any usable URLs"),
        ('["", "   "]', "did not provide any usable URLs"),
    ],
)
@pytest.mark.asyncio
async def test_initial_import_rejects_invalid_source_configuration(
    monkeypatch,
    raw_source,
    message,
):
    monkeypatch.setenv("HLTHPRT_CMSGOV_MRF_URL_PUF", raw_source)
    monkeypatch.setattr(initial, "ensure_database", AsyncMock())
    context_by_field = {
        "redis": SimpleNamespace(),
        "context": {"import_date": "20260724", "run": 0},
    }

    with pytest.raises(RuntimeError, match=message):
        await initial.init_file(context_by_field, {"test_mode": True})


@pytest.mark.parametrize(
    ("configured_name", "configured_value", "expected"),
    [
        (None, None, 99),
        ("HLTHPRT_LIMIT_MB", "2", 2 * 1024 * 1024),
        ("HLTHPRT_LIMIT_BYTES", "2048", 2048),
        ("HLTHPRT_LIMIT_BYTES", "3KiB", 3 * 1024),
        ("HLTHPRT_LIMIT_BYTES", "4mb", 4 * 1024 * 1024),
        ("HLTHPRT_LIMIT_BYTES", "1g", 1024 * 1024 * 1024),
        ("HLTHPRT_LIMIT_BYTES", "1t", 1024 * 1024 * 1024 * 1024),
        ("HLTHPRT_LIMIT_BYTES", "invalid", 99),
        ("HLTHPRT_LIMIT_BYTES", "   ", 99),
    ],
)
def test_chunk_size_configuration_accepts_bytes_units_and_legacy_megabytes(
    monkeypatch,
    configured_name,
    configured_value,
    expected,
):
    monkeypatch.delenv("HLTHPRT_LIMIT_BYTES", raising=False)
    monkeypatch.delenv("HLTHPRT_LIMIT_MB", raising=False)
    if configured_name:
        monkeypatch.setenv(configured_name, configured_value)

    assert initial._mrf_size_bytes("HLTHPRT_LIMIT_BYTES", 99) == expected


@pytest.mark.parametrize(
    ("kind", "configured", "expected"),
    [
        ("provider", "", False),
        ("provider", "none", False),
        ("provider", "off", False),
        ("provider", "all", True),
        ("provider", "YES", True),
        ("provider", "plans, providers", True),
        ("formulary", "drugs", True),
        ("plan", "formularies", False),
        ("custom", "custom", True),
    ],
)
def test_chunking_configuration_maps_operator_values_to_import_kinds(
    monkeypatch,
    kind,
    configured,
    expected,
):
    monkeypatch.setenv("HLTHPRT_MRF_FILE_CHUNKING", configured)

    assert initial._is_mrf_file_chunking_enabled(kind) is expected
    assert (
        initial._is_mrf_file_chunking_enabled(
            kind,
            {"context": {"mrf_file_chunking": configured}},
        )
        is expected
    )


@pytest.mark.asyncio
async def test_large_file_chunk_handoff_registers_each_chunk_before_enqueue(
    monkeypatch,
    tmp_path,
):
    source_file = tmp_path / "providers.json"
    source_file.write_text("[]", encoding="utf-8")
    chunks = [
        {"path": str(tmp_path / "provider_00000.json"), "record_count": 2, "byte_count": 20},
        {"path": str(tmp_path / "provider_00001.json"), "record_count": 1, "byte_count": 10},
    ]
    registered = AsyncMock(side_effect=[True, False])
    redis = SimpleNamespace(enqueue_job=AsyncMock(return_value=SimpleNamespace()))
    monkeypatch.setattr(initial, "_is_mrf_file_chunking_enabled", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(initial, "_mrf_size_bytes", lambda name, _default: 1 if name.endswith("MIN_BYTES") else 10)
    monkeypatch.setattr(initial, "_mrf_chunk_dir", lambda *_args, **_kwargs: tmp_path)
    monkeypatch.setattr(initial, "_split_json_array_file_to_chunks", lambda *_args, **_kwargs: chunks)
    monkeypatch.setattr(initial, "_has_registered_mrf_work", registered)
    context_by_field = {
        "redis": redis,
        "context": {"import_date": "20260724", "control_run_id": "run-contract"},
    }
    task_by_field = {
        "url": "https://data.example.invalid/providers.json",
        "issuer_array": [12345],
    }

    handed_off = await initial._has_enqueued_mrf_file_chunks(
        context_by_field,
        task_by_field,
        str(source_file),
        "provider",
        "process_provider",
    )

    assert handed_off is True
    assert registered.await_count == 2
    first_task = registered.await_args_list[0].kwargs["task"]
    assert first_task["source_url"] == task_by_field["url"]
    assert first_task["input_url"].startswith("file://")
    assert first_task["chunk_index"] == 0
    assert first_task["chunk_count"] == 2
    redis.enqueue_job.assert_awaited_once_with(
        "process_provider",
        first_task,
        _queue_name=initial.MRF_QUEUE_NAME,
        _job_id=first_task["work_id"],
    )


@pytest.mark.parametrize(
    ("has_input_url", "chunking_enabled", "file_size", "has_chunks"),
    [
        (True, True, 100, True),
        (False, False, 100, True),
        (False, True, 0, True),
        (False, True, 100, False),
    ],
)
@pytest.mark.asyncio
async def test_chunk_handoff_skips_ineligible_or_unsplittable_files(
    monkeypatch,
    tmp_path,
    has_input_url,
    chunking_enabled,
    file_size,
    has_chunks,
):
    source_file = tmp_path / "plans.json"
    source_file.write_bytes(b"x" * file_size)
    split = Mock(return_value=[{"path": str(tmp_path / "chunk.json")}] if has_chunks else [])
    monkeypatch.setattr(initial, "_is_mrf_file_chunking_enabled", lambda *_args, **_kwargs: chunking_enabled)
    monkeypatch.setattr(initial, "_mrf_size_bytes", lambda name, _default: 50 if name.endswith("MIN_BYTES") else 10)
    monkeypatch.setattr(initial, "_mrf_chunk_dir", lambda *_args, **_kwargs: tmp_path)
    monkeypatch.setattr(initial, "_split_json_array_file_to_chunks", split)
    task_by_field = {"url": "https://data.example.invalid/plans.json"}
    if has_input_url:
        task_by_field["input_url"] = source_file.as_uri()

    handed_off = await initial._has_enqueued_mrf_file_chunks(
        {"redis": SimpleNamespace(), "context": {"import_date": "20260724"}},
        task_by_field,
        str(source_file),
        "plan",
        "process_plan",
    )

    assert handed_off is False


@pytest.mark.parametrize("fallback", [False, True])
@pytest.mark.asyncio
async def test_federal_attribute_archive_merges_identities_without_overwriting_duplicates(
    monkeypatch, catalog_archives, fallback
):
    archive_by_url, downloads = catalog_archives
    url = "https://data.example.invalid/federal.zip"
    attribute_rows = [
        _federal_attribute_row(StandardComponentId=""),
        _federal_attribute_row(PlanId=""),
        _federal_attribute_row(),
        _federal_attribute_row(PlanMarketingName="Duplicate", IssuerMarketPlaceMarketingName="Duplicate"),
        _federal_attribute_row(
            StandardComponentId="34567CC0000001", IssuerId="34567", IssuerMarketPlaceMarketingName=" "
        ),
    ]
    archive_by_url[url] = _catalog_zip({"attributes.csv": _catalog_csv(attribute_rows)})
    if fallback:
        monkeypatch.setattr(initial, "unzip", AsyncMock(side_effect=OSError("synthetic extraction failure")))
    plans, issuers = {}, {}

    await initial._read_unknown_federal_attribute_file(url, plans, issuers)

    assert set(plans) == {"12345AA0000001_2026", "34567CC0000001_2026"}
    plan = plans["12345AA0000001_2026"]
    assert plan == {
        "plan_id": "12345AA0000001",
        "plan_id_type": "CMS-HIOS-PLAN-ID",
        "year": 2026,
        "issuer_id": 12345,
        "state": "AA",
        "marketing_name": "Synthetic Plan, Silver",
        "summary_url": "https://data.example.invalid/summary",
        "marketing_url": "https://data.example.invalid/brochure",
        "formulary_url": "https://data.example.invalid/formulary",
        "plan_contact": "",
        "network": ["network-a"],
        "benefits": [],
        "last_updated_on": datetime.datetime(2026, 1, 2),
        "checksum": initial.return_checksum(["12345aa0000001", 2026], crc=32),
    }
    assert issuers[12345]["issuer_name"] == "Synthetic Issuer"
    assert issuers[34567]["issuer_name"] == "34567"
    assert issuers[12345]["state"] == "AA"
    assert all(not directory.exists() for _, directory in downloads)


@pytest.mark.parametrize(("preferred_name", "row_limit"), [(True, None), (False, 2)])
@pytest.mark.asyncio
async def test_state_attribute_archive_selects_csv_and_counts_only_new_identities(
    monkeypatch, catalog_archives, preferred_name, row_limit
):
    archive_by_url, downloads = catalog_archives
    url = "https://data.example.invalid/state.zip"
    attribute_rows = [
        _state_attribute_row(StandardComponentId=""),
        _state_attribute_row(),
        _state_attribute_row(StandardComponentId="23456bb0000001", IssuerName="Duplicate"),
        _state_attribute_row(StandardComponentId="34567CC0000001", IssuerId="34567", IssuerName=" "),
    ]
    content_by_name = {"StatePlans.csv" if preferred_name else "attributes.csv": _catalog_csv(attribute_rows)}
    if preferred_name:
        content_by_name["ignored.csv"] = "not,a,plan\n"
    else:
        monkeypatch.setattr(initial, "unzip", AsyncMock(side_effect=OSError("synthetic extraction failure")))
    archive_by_url[url] = _catalog_zip(content_by_name)
    plans, issuers = {}, {}

    count = await initial._read_unknown_state_attribute_file(url, plans, issuers, 1, row_limit)

    assert count == (3 if row_limit is None else 2)
    assert set(plans) == (
        {"23456BB0000001_2026", "34567CC0000001_2026"} if row_limit is None else {"23456BB0000001_2026"}
    )
    plan = plans["23456BB0000001_2026"]
    assert (plan["plan_id_type"], plan["year"], plan["issuer_id"], plan["state"]) == (
        "STATE-HIOS-PLAN-ID",
        2026,
        23456,
        "BB",
    )
    assert plan["last_updated_on"] == datetime.datetime(2026, 2, 3)
    assert plan["marketing_name"] is None
    assert plan["summary_url"] is None
    assert plan["marketing_url"] is None
    assert plan["formulary_url"] is None
    assert plan["network"] == [None]
    assert issuers[23456]["issuer_name"] == "State Issuer"
    if row_limit is None:
        assert issuers[34567]["issuer_name"] == "34567"
    assert all(not directory.exists() for _, directory in downloads)


@pytest.mark.parametrize("test_mode", [False, True])
@pytest.mark.asyncio
async def test_identity_import_uses_real_archives_and_global_state_limit(monkeypatch, catalog_archives, test_mode):
    archive_by_url, downloads = catalog_archives
    urls = [f"https://data.example.invalid/{name}.zip" for name in ("federal", "state-a", "state-b")]
    archive_by_url[urls[0]] = _catalog_zip({"attributes.csv": _catalog_csv([_federal_attribute_row()])})
    archive_by_url[urls[1]] = _catalog_zip(
        {
            "Plans.csv": _catalog_csv(
                [
                    _state_attribute_row(
                        StandardComponentId="12345AA0000001", IssuerId="12345", IssuerName="Duplicate"
                    ),
                    _state_attribute_row(),
                ]
            )
        }
    )
    archive_by_url[urls[2]] = _catalog_zip(
        {
            "Plans.csv": _catalog_csv(
                [
                    _state_attribute_row(StandardComponentId="34567CC0000001", IssuerId="34567"),
                ]
            )
        }
    )
    monkeypatch.setenv("HLTHPRT_CMSGOV_PLAN_ATTRIBUTES_URL_PUF", json.dumps([{"url": urls[0]}]))
    monkeypatch.setenv("HLTHPRT_CMSGOV_STATE_PLAN_ATTRIBUTES_URL_PUF", json.dumps([{"url": url} for url in urls[1:]]))
    monkeypatch.setattr(initial, "TEST_UNKNOWN_STATE_ROWS", 1)

    issuers, plans = await initial.import_unknown_state_issuers_data(test_mode=test_mode)

    assert issuers[12345]["issuer_name"] == "Synthetic Issuer"
    assert plans["12345AA0000001_2026"]["plan_id_type"] == "CMS-HIOS-PLAN-ID"
    assert (34567 in issuers) is (not test_mode)
    assert len(plans) == (2 if test_mode else 3)
    assert [url for url, _ in downloads] == (urls[:2] if test_mode else urls)
    assert all(not directory.exists() for _, directory in downloads)


@pytest.mark.parametrize("test_mode", [False, True])
@pytest.mark.asyncio
async def test_rate_review_archives_refresh_names_with_remaining_global_budget(
    monkeypatch, catalog_archives, test_mode
):
    archive_by_url, downloads = catalog_archives
    urls = [f"https://data.example.invalid/review-{index}.zip" for index in range(3)]
    for url, review_rows in zip(
        urls,
        [
            [{"ISSUER_ID": "12345", "STATE": "aa", "COMPANY": " Original, Issuer "}],
            [
                {"ISSUER_ID": "12345", "STATE": "bb", "COMPANY": " Renamed Issuer "},
                {"ISSUER_ID": "23456", "STATE": "bb", "COMPANY": " "},
            ],
            [{"ISSUER_ID": "34567", "STATE": "cc", "COMPANY": "Third Issuer"}],
        ],
        strict=True,
    ):
        archive_by_url[url] = _catalog_zip({"RatePUF.csv": _catalog_csv(review_rows), "ignored.csv": "invalid\n"})
    monkeypatch.setenv("HLTHPRT_CMSGOV_RATE_REVIEW_URL_PUF", json.dumps([{"url": url} for url in urls]))
    monkeypatch.setattr(initial, "TEST_UNKNOWN_STATE_ROWS", 2)

    issuers = await initial.update_issuer_names_data(test_mode=test_mode)

    assert issuers[12345]["issuer_name"] == "Renamed Issuer"
    assert issuers[12345]["state"] == "BB"
    assert set(issuers) == ({12345} if test_mode else {12345, 23456, 34567})
    if not test_mode:
        assert issuers[23456]["issuer_name"] == "23456"
    assert [url for url, _ in downloads] == (urls[:2] if test_mode else urls)
    assert all(not directory.exists() for _, directory in downloads)


@pytest.mark.parametrize("reader", ["federal", "state", "rate_review"])
@pytest.mark.parametrize(
    ("malformation", "error"),
    [
        ("bad_zip", zipfile.BadZipFile),
        ("invalid_id", ValueError),
        ("missing_columns", KeyError),
    ],
)
@pytest.mark.asyncio
async def test_catalog_failures_propagate_and_remove_extracted_files(
    monkeypatch, catalog_archives, reader, malformation, error
):
    archive_by_url, downloads = catalog_archives
    url = "https://data.example.invalid/malformed.zip"
    failure_rows = {
        "federal": [_federal_attribute_row(BusinessYear="invalid")],
        "state": [_state_attribute_row(IssuerId="invalid")],
        "rate_review": [{"ISSUER_ID": "invalid", "STATE": "aa", "COMPANY": "Issuer"}],
    }[reader]
    if malformation == "missing_columns":
        failure_rows = {
            "federal": [{"StandardComponentId": "12345AA0000001"}],
            "state": [_state_attribute_row(IssuerId=None)],
            "rate_review": [{"STATE": "aa", "COMPANY": "Issuer"}],
        }[reader]
        if reader == "state":
            del failure_rows[0]["IssuerId"]
            error = TypeError
    archive_by_url[url] = (
        b"not a ZIP archive"
        if malformation == "bad_zip"
        else _catalog_zip({"PlansPUF.csv": _catalog_csv(failure_rows)})
    )

    with pytest.raises(error):
        if reader == "federal":
            await initial._read_unknown_federal_attribute_file(url, {}, {})
        elif reader == "state":
            await initial._read_unknown_state_attribute_file(url, {}, {}, 0, None)
        else:
            monkeypatch.setenv("HLTHPRT_CMSGOV_RATE_REVIEW_URL_PUF", json.dumps([{"url": url}]))
            await initial.update_issuer_names_data()

    assert len(downloads) == 1
    assert not downloads[0][1].exists()
