# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import datetime
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import ANY, AsyncMock, Mock

import pytest

from process import initial


class _Stage:
    issuer_id = object()
    issuer_name = object()
    issuer_marketing_name = object()
    mrf_url = object()

    def __init__(self, label: str):
        self.label = label


def _configure_import_boundary(monkeypatch, source_records):
    pushed_batches = []
    duplicate_tolerant_batches = []

    async def download(_url, filename, **_kwargs):
        Path(filename).write_text(json.dumps(source_records), encoding="utf-8")

    async def push(rows, stage, **_kwargs):
        pushed_batches.append((stage.label, [dict(row) for row in rows]))

    async def push_duplicate_tolerant(rows, stage):
        duplicate_tolerant_batches.append(
            (stage.label, [dict(row) for row in rows])
        )

    monkeypatch.setattr(initial, "download_it_and_save", download)
    monkeypatch.setattr(initial, "ensure_database", AsyncMock())
    monkeypatch.setattr(initial, "get_import_schema", lambda *_args, **_kwargs: "mrf_test")
    monkeypatch.setattr(
        initial,
        "make_class",
        lambda model, *_args, **_kwargs: _Stage(model.__name__),
    )
    monkeypatch.setattr(initial, "push_objects", push)
    monkeypatch.setattr(
        initial,
        "_push_mrf_duplicate_tolerant_rows",
        push_duplicate_tolerant,
    )
    monkeypatch.setattr(initial, "_has_enqueued_mrf_file_chunks", AsyncMock(return_value=False))
    monkeypatch.setattr(initial, "_mark_mrf_work_done", AsyncMock())
    monkeypatch.setattr(initial, "_mark_mrf_task_terminal", AsyncMock())
    monkeypatch.setattr(initial, "_cleanup_mrf_chunk_file", lambda _task: None)
    monkeypatch.setattr(initial, "flush_error_log", AsyncMock())
    monkeypatch.setattr(initial, "log_error", AsyncMock())
    return pushed_batches, duplicate_tolerant_batches


def _import_context():
    return {
        "context": {
            "import_date": "20260724",
            "control_run_id": "run-contract",
            "test_mode": True,
        }
    }


def _import_task(kind: str):
    return {
        "url": f"https://data.example.invalid/{kind}.json",
        "issuer_array": [12345],
    }


async def _assert_plan_import_rows(duplicate_tolerant, monkeypatch, plan_id, pushed):
    """Assert plan import rows."""
    outcome = await initial.process_plan(_import_context(), _import_task("plans"))

    assert outcome == 1
    plan_rows = [
        plan_row
        for label, row_dicts in pushed + duplicate_tolerant
        for plan_row in row_dicts
        if label == "Plan"
    ]
    benefit_rows = [
        benefit_row
        for label, row_dicts in pushed + duplicate_tolerant
        for benefit_row in row_dicts
        if label == "PlanBenefitsMarketplace"
    ]
    formulary_rows = [
        formulary_row
        for label, row_dicts in duplicate_tolerant
        for formulary_row in row_dicts
        if label == "PlanFormulary"
    ]
    assert {
        (plan_row["plan_id"], plan_row["year"]) for plan_row in plan_rows
    } == {
        (plan_id, 2026),
        (plan_id, 2027),
        ("54321BB0000000", 2026),
    }
    assert {benefit_row["benefit_name"] for benefit_row in benefit_rows} >= {
        "virtual_visit",
        "deductible",
        "benefit_2",
    }
    assert any(
        formulary_row["pharmacy_type"] == "RETAIL"
        and formulary_row["copay_amount"] == 12.5
        and formulary_row["coinsurance_rate"] == 0.2
        for formulary_row in formulary_rows
    )
    assert initial.log_error.await_count >= 4
    initial._mark_mrf_work_done.assert_awaited_once()


@pytest.mark.asyncio
async def test_plan_import_preserves_year_benefit_and_cost_sharing_contracts(
    monkeypatch,
):
    """Preserve multi-year plans, normalized benefits, and cost sharing rows."""

    plan_id = "12345AA0000000"
    plan_source_records = [
        "not-an-object",
        {"plan_id": plan_id, "years": []},
        {"plan_id": "12345AA0000000TOO-LONG", "years": [2026]},
        {
            "plan_id": plan_id,
            "plan_id_type": "CMS-HIOS-PLAN-ID",
            "years": [2026, "2027.0", 2026],
            "marketing_name": "Synthetic Plan",
            "summary_url": "https://data.example.invalid/summary",
            "plan_contact": "support@example.invalid",
            "network": {"network_tier": "PREFERRED"},
            "formulary": {
                "drug_tier": "GENERIC",
                "mail_order": "yes",
                "cost_sharing": [
                    {
                        "pharmacy_type": "RETAIL",
                        "copay_amount": "12.5",
                        "copay_opt": "AFTER_DEDUCTIBLE",
                        "coinsurance_rate": "0.2",
                        "coinsurance_opt": "NONE",
                    },
                    {"pharmacy_type": "MAIL"},
                ],
            },
            "benefits": [
                {"name": "virtual_visit", "value": True},
                {"deductible": 500},
                "free-form benefit",
            ],
            "last_updated_on": "2026-07-01",
        },
        {
            "plan_id": "54321BB0000000",
            "plan_id_type": "CMS-HIOS-PLAN-ID",
            "years": [2026],
            "marketing_name": "Out-of-scope synthetic plan",
            "summary_url": "https://data.example.invalid/other-summary",
            "plan_contact": "",
            "network": [],
            "formulary": [{"drug_tier": "BRAND", "mail_order": False}],
            "benefits": [],
            "last_updated_on": "2026-07-02",
        },
    ]
    pushed, duplicate_tolerant = _configure_import_boundary(
        monkeypatch, plan_source_records
    )
    monkeypatch.setattr(initial, "_mrf_plan_flush_rows", lambda _test_mode: 0)

    await _assert_plan_import_rows(duplicate_tolerant, monkeypatch, plan_id, pushed)


@pytest.mark.asyncio
async def test_provider_import_preserves_person_facility_and_network_contracts(
    monkeypatch,
):
    """Keep person and facility identity while filtering invalid plan links."""

    current_year = datetime.datetime.now().year
    valid_plan_by_field = {
        "plan_id": "12345AA0000000",
        "network_tier": "PREFERRED",
        "years": [current_year, current_year + 1, current_year - 1],
    }
    provider_source_records = [
        {},
        {"npi": "bad", "plans": [valid_plan_by_field]},
        {
            "npi": "1000000001",
            "plans": [{**valid_plan_by_field, "plan_id": "short"}],
            "last_updated_on": "2026-07-01",
        },
        {
            "npi": "1000000002",
            "type": "INDIVIDUAL",
            "name": {
                "prefix": "Dr.",
                "first": "Ada",
                "middle": "M",
                "last": "Example",
                "suffix": "III",
            },
            "specialty": ["Synthetic Specialty"],
            "languages": ["en", 2],
            "addresses": [{"address": "1 Example Way"}],
            "accepting": True,
            "gender": "X",
            "plans": [valid_plan_by_field],
            "last_updated_on": "2026-07-01",
        },
        {
            "npi": "1000000003",
            "type": "FACILITY",
            "facility_name": "Synthetic Clinic",
            "facility_type": ["CLINIC"],
            "plans": [valid_plan_by_field],
            "last_updated_on": "2026-07-02",
        },
    ]
    _pushed, duplicate_tolerant = _configure_import_boundary(
        monkeypatch, provider_source_records
    )
    monkeypatch.setattr(initial, "_mrf_provider_flush_rows", lambda _test_mode: 0)
    monkeypatch.setattr(
        initial.db,
        "select",
        lambda *_args: SimpleNamespace(
            all=AsyncMock(
                return_value=[
                    SimpleNamespace(
                        issuer_id=12345,
                        issuer_name="Synthetic Issuer",
                        issuer_marketing_name="",
                        mrf_url="https://data.example.invalid/index.json",
                    )
                ]
            )
        ),
    )

    def address_rows(record, *_args, **_kwargs):
        npi = int(record["npi"])
        return (
            [{"npi": npi, "checksum": npi, "type": "PRIMARY"}],
            [{"evidence_checksum": npi, "npi": npi}],
        )

    monkeypatch.setattr(initial, "_build_mrf_address_rows", address_rows)
    monkeypatch.setattr(initial, "_push_mrf_address_rows", AsyncMock())
    monkeypatch.setattr(initial, "_mark_mrf_provider_file_progress", AsyncMock())

    outcome = await initial.process_provider(
        _import_context(), _import_task("providers")
    )

    assert outcome == 1
    provider_rows = [
        provider_row
        for label, row_dicts in duplicate_tolerant
        for provider_row in row_dicts
        if label == "PlanNPIRaw"
    ]
    assert {provider_row["npi"] for provider_row in provider_rows} == {
        1000000002,
        1000000003,
    }
    assert any(
        provider_row["name_or_facility_name"] == "Dr. Ada M Example III"
        for provider_row in provider_rows
    )
    assert any(
        provider_row["name_or_facility_name"] == "Synthetic Clinic"
        for provider_row in provider_rows
    )
    assert {provider_row["year"] for provider_row in provider_rows} == {
        current_year,
        current_year + 1,
    }
    initial._mark_mrf_provider_file_progress.assert_awaited_once_with(
        ANY,
        url="https://data.example.invalid/providers.json",
        processed_providers=3,
    )
    initial._mark_mrf_work_done.assert_awaited_once()


def _assert_formulary_import_rows(duplicate_tolerant):
    """Assert formulary import rows."""
    drug_rows = [
        drug_row
        for label, row_dicts in duplicate_tolerant
        for drug_row in row_dicts
        if label == "PlanDrugRaw"
    ]
    assert len(drug_rows) == 2
    first, second = drug_rows
    assert first["drug_tier"] == "GENERIC"
    assert (
        first["prior_authorization"],
        first["step_therapy"],
        first["quantity_limit"],
    ) == (True, False, None)
    assert second["last_updated_on"] is None
    assert (
        second["prior_authorization"],
        second["step_therapy"],
        second["quantity_limit"],
    ) == (True, False, False)
    assert initial.log_error.await_count == 2
    initial._mark_mrf_work_done.assert_awaited_once()


@pytest.mark.asyncio
async def test_formulary_import_normalizes_flags_and_rejects_incomplete_rows(
    monkeypatch,
):
    """Normalize formulary flags while rejecting incomplete drug and plan rows."""

    formulary_source_records = [
        {"rxnorm_id": "", "drug_name": "Missing identifier", "plans": []},
        {
            "rxnorm_id": "100",
            "drug_name": "Synthetic Drug",
            "plans": [{"plan_id": "", "plan_id_type": ""}],
        },
        {
            "rxnorm_id": "101",
            "drug_name": "Synthetic Drug A",
            "last_updated_on": "2026-07-03",
            "plans": [
                {
                    "plan_id": "12345AA0000000",
                    "plan_id_type": "CMS-HIOS-PLAN-ID",
                    "drug_tier": " generic ",
                    "prior_authorization": "yes",
                    "step_therapy": "0",
                    "quantity_limit": None,
                }
            ],
        },
        {
            "rxnorm_id": "102",
            "drug_name": "Synthetic Drug B",
            "last_updated_on": "not-a-date",
            "plans": [
                {
                    "plan_id": "12345AA0000000",
                    "plan_id_type": "CMS-HIOS-PLAN-ID",
                    "drug_tier": None,
                    "prior_authorization": 1,
                    "step_therapy": False,
                    "quantity_limit": "n",
                }
            ],
        },
    ]
    _pushed, duplicate_tolerant = _configure_import_boundary(
        monkeypatch, formulary_source_records
    )
    monkeypatch.setattr(initial, "_mrf_formulary_flush_rows", lambda _test_mode: 0)

    outcome = await initial.process_formulary(
        _import_context(), _import_task("formulary")
    )

    assert outcome == 1
    _assert_formulary_import_rows(duplicate_tolerant)


@pytest.mark.parametrize(
    ("importer", "kind"),
    [
        (initial.process_plan, "plan"),
        (initial.process_provider, "provider"),
        (initial.process_formulary, "formulary"),
    ],
)
@pytest.mark.asyncio
async def test_import_download_failure_marks_terminal_state(
    monkeypatch,
    importer,
    kind,
):
    _configure_import_boundary(monkeypatch, [])

    async def fail_download(*_args, **_kwargs):
        raise OSError("synthetic download failure")

    monkeypatch.setattr(initial, "download_it_and_save", fail_download)
    if kind == "provider":
        monkeypatch.setattr(
            initial.db,
            "select",
            lambda *_args: SimpleNamespace(all=AsyncMock(return_value=[])),
        )

    outcome = await importer(_import_context(), _import_task(kind))

    assert outcome is None
    initial._mark_mrf_task_terminal.assert_awaited_once()
    assert initial._mark_mrf_task_terminal.await_args.args[2] == kind
    assert initial._mark_mrf_task_terminal.await_args.kwargs == {"cleanup_chunk": True}


@pytest.mark.parametrize(
    ("importer", "kind"),
    [
        (initial.process_plan, "plan"),
        (initial.process_provider, "provider"),
        (initial.process_formulary, "formulary"),
    ],
)
@pytest.mark.asyncio
async def test_import_chunk_handoff_finishes_parent_work_without_parsing(
    monkeypatch,
    importer,
    kind,
):
    _configure_import_boundary(monkeypatch, [])
    initial._has_enqueued_mrf_file_chunks.return_value = True
    if kind == "provider":
        monkeypatch.setattr(
            initial.db,
            "select",
            lambda *_args: SimpleNamespace(all=AsyncMock(return_value=[])),
        )

    outcome = await importer(_import_context(), _import_task(kind))

    assert outcome == 1
    initial._has_enqueued_mrf_file_chunks.assert_awaited_once()
    initial._mark_mrf_work_done.assert_awaited_once()
    initial.flush_error_log.assert_not_awaited()


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


@pytest.mark.asyncio
async def test_initial_import_stages_catalog_rows_and_enqueues_bounded_index_jobs(
    monkeypatch,
):
    """Stage catalog records before enqueuing a bounded, deterministic URL set."""

    transparency_headings = [
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
    ]
    transparency_rows = [
        [],
        [],
        transparency_headings,
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
    workbook_by_path = {
        "/tmp/transparency.xlsx": _Workbook("Transparency 2026", transparency_rows),
        "/tmp/mrf-index.xlsx": _Workbook("Index", index_rows),
    }
    workbook_paths = iter(workbook_by_path)
    pushed_batches = []
    progress_events = []

    async def push(rows, stage, **_kwargs):
        pushed_batches.append((stage.__name__, [dict(row) for row in rows]))

    async def has_registered_work(*_args, **_kwargs):
        return True

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
    monkeypatch.setattr(initial, "ensure_database", AsyncMock())
    monkeypatch.setattr(initial, "mark_control_run", AsyncMock())
    monkeypatch.setattr(initial, "_prepare_import_tables", AsyncMock())
    monkeypatch.setattr(initial, "download_it_and_save", AsyncMock())
    monkeypatch.setattr(initial, "unzip", AsyncMock())
    monkeypatch.setattr(initial.glob, "glob", lambda _pattern: [next(workbook_paths)])
    monkeypatch.setattr(initial.xl, "readxl", lambda path: workbook_by_path[path])
    monkeypatch.setattr(initial.os, "unlink", lambda _path: None)
    monkeypatch.setattr(initial, "make_class", lambda model, *_args, **_kwargs: model)
    monkeypatch.setattr(initial, "push_objects", push)
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
                {
                    "12345AA0000000_2026": {
                        "plan_id": "12345AA0000000",
                        "year": 2026,
                    }
                },
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
    monkeypatch.setattr(initial, "_init_mrf_run_state", AsyncMock())
    monkeypatch.setattr(initial, "_has_registered_mrf_work", has_registered_work)
    monkeypatch.setattr(
        initial,
        "enqueue_live_progress",
        lambda **event: progress_events.append(event),
    )

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

    assert context_by_field["context"] == {
        "import_date": "20260724",
        "run": 1,
        "test_mode": True,
        "control_run_id": "run-contract",
        "mrf_file_chunking": "providers,formularies",
    }
    transparency_row_dicts = [
        transparency_row
        for label, row_dicts in pushed_batches
        for transparency_row in row_dicts
        if label == "PlanTransparency"
    ]
    assert transparency_row_dicts == [
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
    issuer_rows = [
        issuer_row
        for label, row_dicts in pushed_batches
        for issuer_row in row_dicts
        if label == "Issuer"
    ]
    assert {issuer_row["issuer_id"] for issuer_row in issuer_rows} == {
        12345,
        23456,
        99999,
    }
    assert next(
        issuer_row for issuer_row in issuer_rows if issuer_row["issuer_id"] == 12345
    ) == {
        "issuer_id": 12345,
        "issuer_name": "Catalog Issuer",
        "issuer_marketing_name": "",
        "data_contact_email": "data@example.invalid",
        "mrf_url": "https://data.example.invalid/index-b.json",
    }
    index_calls = [
        call
        for call in redis.enqueue_job.await_args_list
        if call.args[0] == "process_json_index"
    ]
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
    split = Mock(return_value=[{"path": str(tmp_path / "chunk.json")} ] if has_chunks else [])
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
