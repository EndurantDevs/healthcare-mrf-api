# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Host orchestration proof with genuine existing Profile lineage validators."""

import asyncio
import importlib
import json
from copy import deepcopy
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import provider_directory_profile_lineage as lineage
from tests.test_provider_directory_profile_selection_attestation import _execution

fhir = importlib.import_module("process.provider_directory_fhir")
GENERATION = "pdprofile_" + "a" * 32


def _result(*, dated=True, **changes):
    result = fhir.profile_selection_result(
        _execution(),
        profile_generation_id=GENERATION,
        profile_as_of="2026-07-30",
        profile_rows=3,
        profile_source_evidence_rows=5,
    )
    if not dated:
        result.pop("profile_as_of")
    return result | changes


def _head():
    vector = (("pdfhir_payer", "head-dataset"),)
    contexts = (("pdfhir_payer", "1" * 64),)
    return dict(
        singleton_key="global",
        status="published",
        operation="publish",
        control_generation=11,
        generation_id=GENERATION,
        selection_proof_id="b" * 64,
        authority_revision=7,
        profile_schema_version=1,
        profile_strategy_version="legacy-profile-v3",
        source_vector_json=fhir._provider_directory_profile_source_vector_json(vector),
        source_vector_hash=fhir._provider_directory_profile_source_vector_hash(vector),
        source_context_vector_json=fhir._provider_directory_profile_source_context_vector_json(contexts),
        source_context_vector_hash=fhir._provider_directory_profile_source_context_vector_hash(contexts),
        executable_plan_hash="c" * 64,
        capacity_geometry_status="legacy_unavailable",
        capacity_geometry_hash=None,
        capacity_geometry_json=None,
        cutover_forecast_hash=None,
        evidence_target_oid=101,
        profile_target_oid=102,
        evidence_rows=5,
        profile_rows=3,
        profile_as_of="2026-07-30",
        published_at=datetime(2026, 7, 30, tzinfo=UTC),
    )


def _module(heads=(), results=()):
    returned_results = [
        SimpleNamespace(all=lambda: list(heads)),
        SimpleNamespace(all=lambda: [{"result": result} for result in results]),
    ]
    return SimpleNamespace(_runtime_db_schema=lambda: "mrf", _execute_stmt=AsyncMock(side_effect=returned_results))


@pytest.mark.asyncio
async def test_matching_head_uses_existing_parser_and_skips_history():
    module, session = _module(heads=[_head()]), object()
    assert await lineage.profile_source_datasets(module, GENERATION, session=session) == (
        ("pdfhir_payer", "head-dataset"),
    )
    module._execute_stmt.assert_awaited_once()
    statement = str(module._execute_stmt.await_args.args[0])
    assert "singleton_key='global'" in statement and "status='published'" in statement
    assert "generation_id=:profile_generation_id" in statement
    assert module._execute_stmt.await_args.kwargs == {
        "session": session,
        "params": {"profile_generation_id": GENERATION},
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["hash", "generation", "date", "status", "duplicate"])
async def test_invalid_matching_head_never_falls_back_to_terminal(fault):
    head = _head()
    if fault == "hash":
        head["source_vector_hash"] = "0" * 64
    elif fault == "generation":
        head["generation_id"] = "pdprofile_" + "b" * 32
    elif fault == "date":
        head["profile_as_of"] = None
    elif fault == "status":
        head["status"] = "purged"
    module = _module(heads=[head, head] if fault == "duplicate" else [head], results=[_result()])
    with pytest.raises(RuntimeError, match="lineage_ambiguous|lineage_unavailable"):
        await lineage.profile_source_datasets(module, GENERATION, session=object())
    assert module._execute_stmt.await_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("dated", [False, True])
@pytest.mark.parametrize("encoded", [False, True])
async def test_terminal_history_is_exact_generation_and_supports_original_undated_result(dated, encoded):
    result = _result(dated=dated)
    original = deepcopy(result)
    raw = json.dumps(result) if encoded else result
    module, session = _module(results=[raw]), object()
    expected = fhir._provider_directory_profile_source_vector(
        (pair["source_id"], pair["dataset_id"]) for pair in result["pairs"]
    )
    assert await lineage.profile_source_datasets(module, GENERATION, session=session) == expected
    assert result == original
    assert ("profile_as_of" in result) is dated
    statement = str(module._execute_stmt.await_args.args[0])
    assert "status='succeeded'" in statement and "finished_at IS NOT NULL" in statement
    assert "metrics::jsonb -> :metric ->> 'profile_generation_id')=:profile_generation_id" in statement
    assert "ORDER BY" not in statement and "LIMIT" not in statement
    assert "provider_directory_source" not in statement
    assert module._execute_stmt.await_args.kwargs == {
        "session": session,
        "params": {"profile_generation_id": GENERATION, "metric": "profile_selection_result"},
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["generation", "proof", "date", "extra", "missing", "malformed", "nonobject"])
async def test_wrong_or_unavailable_terminal_identity_refuses(fault):
    result = _result()
    if fault == "generation":
        result["profile_generation_id"] = "pdprofile_" + "b" * 32
    if fault == "proof":
        result["pairs"][0]["dataset_id"] = "unattested-dataset"
    if fault == "date":
        result["profile_as_of"] = "2026-99-99"
    if fault == "extra":
        result["synthetic_head"] = True
    if fault == "malformed":
        result = "{not-json"
    if fault == "nonobject":
        result = 42
    module = _module(results=[] if fault == "missing" else [result])
    with pytest.raises(RuntimeError, match="lineage_unavailable"):
        await lineage.profile_source_datasets(module, GENERATION, session=object())


@pytest.mark.asyncio
@pytest.mark.parametrize("conflict", [False, True])
async def test_terminal_duplicates_must_have_identical_canonical_results(conflict):
    first = _result()
    second = deepcopy(first)
    if conflict:
        second["row_counts"]["profile_rows"] = 4
    module = _module(results=[first, json.dumps(second)])
    if conflict:
        with pytest.raises(RuntimeError, match="lineage_ambiguous"):
            await lineage.profile_source_datasets(module, GENERATION, session=object())
    else:
        assert await lineage.profile_source_datasets(module, GENERATION, session=object())


@pytest.mark.asyncio
@pytest.mark.parametrize("generation", [None, "", "content:" + "a" * 64, "a" * 64, "pdprofile_" + "A" * 32, 123])
async def test_non_fhir_generation_is_rejected_before_query(generation):
    module = _module()
    with pytest.raises(RuntimeError, match="lineage_unavailable"):
        await lineage.profile_source_datasets(module, generation, session=object())
    module._execute_stmt.assert_not_awaited()


@pytest.mark.asyncio
async def test_query_failure_is_unavailable_without_fallback():
    module = _module()
    module._execute_stmt.side_effect = ValueError("reader failed")
    with pytest.raises(RuntimeError, match="lineage_unavailable"):
        await lineage.profile_source_datasets(module, GENERATION, session=object())
    assert module._execute_stmt.await_count == 1


@pytest.mark.asyncio
async def test_reader_cancellation_is_preserved():
    module = _module()
    module._execute_stmt.side_effect = asyncio.CancelledError()
    with pytest.raises(asyncio.CancelledError):
        await lineage.profile_source_datasets(module, GENERATION, session=object())
    assert module._execute_stmt.await_count == 1
