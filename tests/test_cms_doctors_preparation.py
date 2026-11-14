# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Small preparation preconditions and shared source-validation contracts."""

import importlib
from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import cms_doctors_preparation as preparation
from tests.test_cms_doctors_organizations import _mock_publication, _publication_context
from tests.test_provider_directory_cms_serving_receipt import _published_payload

native = importlib.import_module("process.cms_doctors")


@pytest.mark.asyncio
@pytest.mark.parametrize("entrypoint", ["_apply_cms_doctors_stage", "_apply_locked_cms_doctors_stage"])
async def test_publication_refuses_missing_owner_transaction(monkeypatch, entrypoint):
    database = SimpleNamespace(_transaction_binding=lambda: None, status=AsyncMock())
    monkeypatch.setattr(native, "db", database)
    with pytest.raises(RuntimeError, match="^cms_doctors_publication_requires_transaction$"):
        await getattr(native, entrypoint)(object(), "mrf", "synthetic")
    database.status.assert_not_awaited()


def test_uuid_length_stage_indexes_are_distinct_and_bounded():
    """PostgreSQL must not silently truncate several index names into one."""
    stage = "doctor_clinician_address_" + "a" * 32
    index_names = [
        native._stage_index_name(stage, suffix)
        for suffix in ("primary", "zip_provider_type", "provider_type", "address_key")
    ]
    assert len(set(index_names)) == len(index_names)
    assert all(len(name) <= 63 for name in index_names)


@pytest.mark.asyncio
@pytest.mark.parametrize("import_date", [None, 4, "", "bad-name", "a" * 33])
async def test_preparation_rejects_invalid_stage_identity(import_date):
    with pytest.raises(RuntimeError, match="requires_completed_import"):
        async with preparation.prepare_cms_doctors_generation({"import_date": import_date, "context": {"run": True}}):
            pytest.fail("invalid stage identity must not reach preparation")


@pytest.mark.asyncio
async def test_source_preparation_reuses_validation_without_publication(monkeypatch):
    calls = _mock_publication(monkeypatch, None)
    ctx = _publication_context()
    metrics = await native._prepare_cms_doctors_sources(ctx, object(), "mrf", 20000)
    assert calls == ["education", "group", "artifact", "binding", "sites", "cancel"]
    assert metrics["organization_groups"] == 101 and metrics["sites"] == 71
    native._publish_cms_doctors_stage.assert_not_awaited()
    native.mark_control_run.assert_not_awaited()


def _commit_case(monkeypatch):
    receipt_payload = _published_payload()
    doctors = receipt_payload["doctors"]
    origin = preparation.generation.validate_reference_family_serving_generation(
        {key: doctors[key] for key in ("origin_lineage_id", "origin_generation", "published_at")}
    )
    receipt = preparation.generation.ReferenceFamilyResultGenerationAuthority(
        "cms-doctors", doctors["local_lineage_id"], doctors["local_generation"], origin, (201, 202, 203)
    )
    names = preparation.generation.RELATION_NAMES_BY_IMPORTER["cms-doctors"]
    prepared = preparation.PreparedCMSDoctorsGeneration(
        "serving",
        "candidate",
        tuple((name, name + "_candidate", index) for index, name in enumerate(names, 1)),
        None,
        None,
        {},
        {},
        native_receipt=receipt,
    )
    connection = object()

    @asynccontextmanager
    async def session():
        yield connection

    database = SimpleNamespace(_transaction_binding=lambda: None, session=session)
    monkeypatch.setattr(native, "db", database)
    verify = AsyncMock(return_value=True)
    monkeypatch.setattr(preparation.common_receipts, "verify_historical_receipt", verify)
    return prepared, receipt_payload, verify, connection


@pytest.mark.asyncio
@pytest.mark.parametrize("legacy_ids", [None, [901, 902, 903]])
async def test_commit_matches_logical_generation_without_physical_ids(monkeypatch, legacy_ids):
    prepared, payload, verify, connection = _commit_case(monkeypatch)
    payload["doctors"]["relation_oids"] = legacy_ids
    await prepared.mark_committed("receipt", payload)
    verify.assert_awaited_once_with(connection, "serving", "receipt", payload)
    assert prepared.committed and prepared.metrics == {"published": True, "publication_state": "published"}
    assert prepared.context["publication_state"] == "published"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,value",
    [
        ("importer_id", "nucc"),
        ("local_lineage_id", "550e8400-e29b-41d4-a716-446655440001"),
        ("local_generation", 2),
        ("origin_lineage_id", "550e8400-e29b-41d4-a716-446655440001"),
        ("origin_generation", 2),
        ("published_at", "2026-01-02T00:00:00Z"),
    ],
)
async def test_commit_refuses_logical_generation_drift_before_receipt_read(monkeypatch, field, value):
    prepared, payload, verify, _ = _commit_case(monkeypatch)
    payload["doctors"][field] = value
    with pytest.raises(RuntimeError, match="receipt_changed"):
        await prepared.mark_committed("receipt", payload)
    verify.assert_not_awaited()
    assert not prepared.committed and prepared.metrics == {} and prepared.context == {}


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["transaction", "unapplied", "family", "origin", "receipt"])
async def test_commit_keeps_pending_and_uncommitted_results_retryable(monkeypatch, failure):
    prepared, payload, verify, _ = _commit_case(monkeypatch)
    if failure == "transaction":
        native.db._transaction_binding = lambda: object()
    if failure == "unapplied":
        prepared.native_receipt = None
    if failure == "family":
        prepared.stage_oids = prepared.stage_oids[:1]
    if failure == "origin":
        prepared.native_receipt = replace(prepared.native_receipt, serving_generation=None)
    verify.return_value = failure != "receipt"
    with pytest.raises(RuntimeError, match="commit_pending|receipt_changed|commit_unproved"):
        await prepared.mark_committed("receipt", payload)
    assert verify.await_count == (1 if failure == "receipt" else 0)
    assert not prepared.committed and prepared.metrics == {} and prepared.context == {}
