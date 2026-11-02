# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Protected-header commitment parity without exposing retained supplier receipts."""

import hashlib
import json
from copy import deepcopy
from dataclasses import replace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.dialects.postgresql import asyncpg as postgresql_asyncpg

from api import plan_release_source_binding_custody as custody
from api import plan_release_source_bindings as source
from api import plan_release_supporting_metadata as supporting
from process import registry_pricing_release_binding_lookup as lookup
from tests.test_plan_release_serving_resolution import PLAN_RELEASE_ID, SERVING_REVISION_ID, _binding_row


def commitment_page(binding_rows):
    """Represent the bounded SQL projection for unchanged legacy Source fixtures."""
    commitments = [
        {
            **{
                field: binding[field]
                for field in (
                    "plan_release_id",
                    "serving_revision_id",
                    "binding_set_digest",
                    "expected_binding_count",
                    "role",
                    "binding_ordinal",
                )
            },
            "marker_present": False,
            "marker_type": None,
            "marker": None,
            "manifest_contract": "hp-plan-serving-revision-v1",
            "metadata_type": "object",
            "supporting_metadata": {},
        }
        for binding in binding_rows
    ]
    return [{"bounded": True, "rows_json": json.dumps(commitments)}]


def _admission():
    return source.SourceBindingAdmission(PLAN_RELEASE_ID, SERVING_REVISION_ID, "a" * 64, 2)


def _receipt(case_character="1"):
    return {
        "contract": "reviewed-supporting-rate-file-receipt-v1",
        "review_case_id": "hpreview_" + case_character * 32,
        "approved_association_sha256": "a" * 64,
        "association": {"source_id": "source_supplier", "content_version": "2026-11", "note": "café"},
        "publisher_proof": {"archive_sha256": "b" * 64},
        "supplier_observation": {"source_discovered_plan_id": "supplier_alias"},
        "validation_context": {"review": {"status": "approved"}, "import_month": "2026-11"},
    }


def _committed_rows():
    commitments = json.loads(
        commitment_page([_binding_row(expected_binding_count=2, binding_ordinal=ordinal) for ordinal in range(2)])[0][
            "rows_json"
        ]
    )
    commitments[1]["supporting_metadata"] = {"supporting_rate_file_receipts": [_receipt()]}
    slots = [["in_network", 1, commitments[1]["supporting_metadata"]]]
    body = json.dumps(slots, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)
    marker = hashlib.sha256(("hp_supporting_binding_metadata_v1:" + body).encode()).hexdigest()
    for commitment in commitments:
        commitment.update(marker_present=True, marker_type="string", marker=marker)
    return commitments


def test_source_encoder_parity_and_opaque_receipt_integrity():
    commitments = _committed_rows()
    before = deepcopy(commitments)
    supporting.require_supporting_metadata_commitment(commitments)
    supporting.require_supporting_metadata_commitment(list(reversed(commitments)))
    assert commitments == before
    assert (
        supporting.pricing_control_digest(
            [["in_network", 1, commitments[1]["supporting_metadata"]]], domain="hp_supporting_binding_metadata_v1"
        )
        == commitments[0]["marker"]
    )


@pytest.mark.parametrize(
    "change",
    [
        "erased",
        "replaced",
        "relocated",
        "missing_marker",
        "null_marker",
        "wrong_marker",
        "uppercase_marker",
        "wrong_type",
        "wrong_contract",
        "partial_erasure",
        "duplicate_receipt",
        "empty_receipts",
    ],
)
def test_erased_replaced_and_relocated_receipts_refuse(change):
    commitments = _committed_rows()
    receipts_key = "supporting_rate_file_receipts"
    amendments_by_name = {
        "erased": [(1, ("supporting_metadata",), {})],
        "replaced": [(1, ("supporting_metadata", receipts_key), [_receipt("2")])],
        "relocated": [
            (0, ("supporting_metadata",), deepcopy(commitments[1]["supporting_metadata"])),
            (1, ("supporting_metadata",), {}),
        ],
        "partial_erasure": [(1, ("supporting_metadata", receipts_key, 0, "publisher_proof"), {})],
        "duplicate_receipt": [(0, ("supporting_metadata",), deepcopy(commitments[1]["supporting_metadata"]))],
        "empty_receipts": [(1, ("supporting_metadata", receipts_key), [])],
    }
    header_by_name = {
        "missing_marker": {"marker_present": False, "marker_type": None, "marker": None},
        "null_marker": {"marker_type": "null", "marker": None},
        "wrong_marker": {"marker": "0" * 64},
        "uppercase_marker": {"marker": commitments[0]["marker"].upper()},
        "wrong_type": {"marker_type": "boolean", "marker": True},
        "wrong_contract": {"manifest_contract": "other"},
    }
    for ordinal, path, replacement in amendments_by_name.get(change, []):
        branch_by_field = commitments[ordinal]
        for field in path[:-1]:
            branch_by_field = branch_by_field[field]
        branch_by_field[path[-1]] = replacement
    if change in header_by_name:
        for commitment in commitments:
            commitment.update(header_by_name[change])
    with pytest.raises(ValueError, match="plan_release_supporting_"):
        supporting.require_supporting_metadata_commitment(commitments)


@pytest.mark.parametrize(
    "field,value",
    [
        ("role", "unknown"),
        ("role", []),
        ("binding_ordinal", True),
        ("binding_ordinal", -1),
        ("binding_ordinal", 2_147_483_648),
        ("metadata_type", "array"),
        ("supporting_metadata", []),
        ("supporting_metadata", {"unexpected": []}),
    ],
)
def test_malformed_slots_never_become_legacy(field, value):
    commitments = _committed_rows()
    commitments[1][field] = value
    with pytest.raises(ValueError, match="plan_release_supporting_"):
        supporting.require_supporting_metadata_commitment(commitments)


def test_duplicate_slots_ambiguous_header_and_nonfinite_json_refuse():
    commitments = _committed_rows()
    for changed in (
        [commitments[1], commitments[1]],
        [commitments[0], {**commitments[1], "marker": "0" * 64}],
    ):
        with pytest.raises(ValueError, match="plan_release_supporting_"):
            supporting.require_supporting_metadata_commitment(changed)
    commitments[1]["supporting_metadata"]["supporting_rate_file_receipts"][0]["number"] = float("nan")
    with pytest.raises(ValueError, match="metadata_invalid"):
        supporting.require_supporting_metadata_commitment(commitments)


def test_legacy_absence_preserves_unrelated_metadata_redaction():
    commitments = json.loads(commitment_page([_binding_row()])[0]["rows_json"])
    supporting.require_supporting_metadata_commitment(commitments)


def test_every_slot_and_receipt_order_are_committed():
    commitments = _committed_rows()
    commitments[0]["supporting_metadata"] = {"supporting_rate_file_receipts": [_receipt("2"), _receipt("3")]}
    slots = [[entry["role"], entry["binding_ordinal"], entry["supporting_metadata"]] for entry in commitments]
    marker = supporting.pricing_control_digest(slots, domain="hp_supporting_binding_metadata_v1")
    for commitment in commitments:
        commitment["marker"] = marker
    supporting.require_supporting_metadata_commitment(commitments)
    commitments[0]["supporting_metadata"]["supporting_rate_file_receipts"].reverse()
    with pytest.raises(ValueError, match="commitment_changed"):
        supporting.require_supporting_metadata_commitment(commitments)
    commitments[0]["supporting_metadata"]["supporting_rate_file_receipts"].reverse()
    commitments[1]["supporting_metadata"] = {}
    with pytest.raises(ValueError, match="commitment_changed"):
        supporting.require_supporting_metadata_commitment(commitments)


@pytest.mark.parametrize("commitments", [[], None, [None]])
def test_empty_or_nonobject_commitment_refuses(commitments):
    with pytest.raises(ValueError, match="metadata_invalid"):
        supporting.require_supporting_metadata_commitment(commitments)


@pytest.mark.asyncio
async def test_empty_admission_and_invalid_coordinates_never_fetch():
    fetch = AsyncMock(side_effect=AssertionError("unadmitted metadata read"))
    await supporting.require_supporting_metadata_fetch(fetch, "mrf", "sample_registry", [], max_bytes=100_000)
    for schema, registry_schema, admissions, maximum in (
        ("bad.schema", "sample_registry", [_admission()], 100_000),
        ("mrf", "bad.schema", [_admission()], 100_000),
        ("mrf", "sample_registry", [_admission(), _admission()], 100_000),
        ("mrf", "sample_registry", [_admission()], True),
    ):
        with pytest.raises(ValueError):
            await supporting.require_supporting_metadata_fetch(
                fetch, schema, registry_schema, admissions, max_bytes=maximum
            )
    fetch.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change", ["bounded", "oversized", "not_text", "not_json", "not_list", "missing", "wrong_revision"]
)
async def test_bounded_fetch_refuses_incomplete_or_changed_inputs(change):
    commitments = _committed_rows()
    commitments[1]["serving_revision_id"] = "hpserve_" + "9" * 26
    encoded_by_change = {
        "bounded": None,
        "oversized": " " * 100_001,
        "not_text": None,
        "not_json": "{",
        "not_list": "{}",
        "missing": json.dumps(commitments[:1]),
        "wrong_revision": json.dumps(commitments),
    }
    page_by_field = {"bounded": change != "bounded", "rows_json": encoded_by_change[change]}
    fetch = AsyncMock(return_value=[page_by_field])
    with pytest.raises(ValueError, match="plan_release_(supporting_|source_binding_incomplete)"):
        await supporting.require_supporting_metadata_fetch(
            fetch, "mrf", "sample_registry", [_admission()], max_bytes=100_000
        )
    fetch.assert_awaited_once()


class CommitmentReader:
    def __init__(self, commitments, physical_rows):
        self.commitments = commitments
        self.physical_rows = physical_rows
        self.calls = []
        self.statements = []

    async def execute(self, statement, parameters):
        sql = str(statement)
        self.calls.append((sql, parameters))
        self.statements.append((statement, parameters))
        if "supporting_commitments AS MATERIALIZED" in sql:
            assert set(parameters) == {"p1", "p2"}
            return [{"bounded": True, "rows_json": json.dumps(self.commitments)}]
        return self.physical_rows


@pytest.mark.asyncio
async def test_scalar_statement_binds_both_parameters():
    reader = CommitmentReader(_committed_rows(), [])
    await supporting.require_session_supporting_metadata(reader, "mrf", "sample_registry", _admission())
    assert len(reader.statements) == 1
    statement, parameters = reader.statements[0]
    compiled = statement.bindparams(**parameters).compile(dialect=postgresql_asyncpg.dialect())
    assert compiled.params == parameters
    assert set(compiled.positiontup) == {"p1", "p2"}
    assert ":p1" not in str(compiled) and ":p2" not in str(compiled)
    assert json.loads(compiled.params["p1"]) == [
        {
            "plan_release_id": PLAN_RELEASE_ID,
            "serving_revision_id": SERVING_REVISION_ID,
            "binding_set_digest": "a" * 64,
            "expected_binding_count": 2,
        }
    ]
    assert compiled.params["p2"] == supporting.MAX_SUPPORTING_METADATA_BYTES


@pytest.mark.asyncio
@pytest.mark.parametrize("include_projection", [False, True])
async def test_scalar_loader_checks_same_session_and_returns_only_physical_rows(monkeypatch, include_projection):
    monkeypatch.setenv("HLTHPRT_PLAN_REGISTRY_SCHEMA", "sample_registry")
    monkeypatch.setattr(custody, "require_source_binding_custody", AsyncMock())
    physical_rows = [_binding_row(expected_binding_count=2, binding_ordinal=ordinal) for ordinal in range(2)]
    reader = CommitmentReader(_committed_rows(), physical_rows)
    before = deepcopy(physical_rows)
    assert (
        await source.load_source_binding_rows(
            reader, "mrf", _admission(), include_pricing_projection=include_projection
        )
        == before
    )
    assert len(reader.calls) == 2
    assert all("supporting_rate_file_receipts" not in binding for binding in physical_rows)
    reader.commitments[1]["supporting_metadata"] = {}
    with pytest.raises(ValueError, match="commitment_invalid"):
        await source.load_source_binding_rows(
            reader, "mrf", _admission(), include_pricing_projection=include_projection
        )


class CommitmentDriver:
    def __init__(self, commitments, physical_rows):
        self.commitments = commitments
        self.physical_rows = physical_rows
        self.calls = []

    async def fetch(self, sql, *parameters):
        self.calls.append((sql, parameters))
        return [{"bounded": True, "rows_json": json.dumps(self.commitments)}]

    async def fetchrow(self, sql, *parameters):
        self.calls.append((sql, parameters))
        return {"bounded": True, "rows_json": json.dumps(self.physical_rows)}


@pytest.mark.asyncio
async def test_bulk_checks_all_source_receipts_once_without_exposing_them(monkeypatch):
    monkeypatch.setenv("HLTHPRT_PLAN_REGISTRY_SCHEMA", "sample_registry")
    custody_check = AsyncMock()
    monkeypatch.setattr(lookup, "require_source_binding_custody_fetch", custody_check)
    first = _admission()
    second = replace(first, plan_release_id="hprelease_" + "4" * 26, serving_revision_id="hpserve_" + "5" * 26)
    admissions_by_release_id = {entry.plan_release_id: entry for entry in (first, second)}
    commitments = _committed_rows()
    commitments += [
        {**entry, "plan_release_id": second.plan_release_id, "serving_revision_id": second.serving_revision_id}
        for entry in deepcopy(commitments)
    ]
    physical_rows = [
        _binding_row(
            plan_release_id=entry.plan_release_id,
            serving_revision_id=entry.serving_revision_id,
            expected_binding_count=2,
            binding_ordinal=ordinal,
        )
        for entry in (first, second)
        for ordinal in range(2)
    ]
    driver = CommitmentDriver(commitments, physical_rows)
    loaded = await lookup._fetch_admitted_binding_rows(
        driver, list(admissions_by_release_id), 100_000, admissions_by_release_id, retained=False, has_projection=False
    )
    assert loaded == physical_rows
    assert custody_check.await_count == 2
    assert len(driver.calls) == 2
    sql, parameters = driver.calls[0]
    assert len(json.loads(parameters[0])) == 2
    assert "supporting_commitments AS MATERIALIZED" in sql
    assert "plan_release_snapshot_binding" not in sql
    assert "supporting_rate_file_receipts" not in json.dumps(loaded)
    driver.commitments[-1]["supporting_metadata"]["supporting_rate_file_receipts"] = [_receipt("2")]
    driver.calls.clear()
    with pytest.raises(ValueError, match="commitment_changed"):
        await lookup._fetch_admitted_binding_rows(
            driver,
            list(admissions_by_release_id),
            100_000,
            admissions_by_release_id,
            retained=False,
            has_projection=False,
        )
    assert len(driver.calls) == 1
