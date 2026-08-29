# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import datetime
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import ANY, AsyncMock

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
async def test_plan_import_preserves_below_threshold_cost_sharing_and_year_contracts(
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
    monkeypatch.setattr(initial, "_mrf_plan_flush_rows", lambda _test_mode: 100)

    await _assert_plan_import_rows(duplicate_tolerant, monkeypatch, plan_id, pushed)


def _provider_contract_fixture(current_year):
    valid_plan_by_field = {
        "plan_id": "12345AA0000000",
        "network_tier": "PREFERRED",
        "years": [current_year, current_year + 1, current_year - 1],
    }
    return [
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


def _assert_provider_contract_rows(duplicate_tolerant, current_year):
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


@pytest.mark.asyncio
async def test_provider_import_preserves_person_facility_and_network_contracts(
    monkeypatch,
):
    """Keep person and facility identity while filtering invalid plan links."""

    current_year = datetime.datetime.now().year
    provider_source_records = _provider_contract_fixture(current_year)
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
    _assert_provider_contract_rows(duplicate_tolerant, current_year)
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
    ("parse_error", "message_prefix"),
    [
        (initial.ijson.IncompleteJSONError("truncated"), "Incomplete JSON"),
        (initial.ijson.JSONError("malformed"), "JSON Parsing Error"),
    ],
)
@pytest.mark.asyncio
async def test_formulary_parse_errors_preserve_terminal_messages(
    monkeypatch,
    parse_error,
    message_prefix,
):
    monkeypatch.setattr(
        initial,
        "_read_formulary_file",
        AsyncMock(side_effect=parse_error),
    )
    monkeypatch.setattr(initial, "_log_formulary_row_error", AsyncMock())
    monkeypatch.setattr(initial, "_mark_mrf_task_terminal", AsyncMock())
    task = _import_task("formulary")

    is_imported = await initial._is_formulary_file_imported(
        _import_context(), task, object(), object(), "formulary.json", None, 1
    )

    assert is_imported is False
    assert initial._log_formulary_row_error.await_args.args[2].startswith(message_prefix)
    initial._mark_mrf_task_terminal.assert_awaited_once_with(
        ANY, task, "formulary", cleanup_chunk=True
    )


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
