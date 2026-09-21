# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Publication receipts fail closed when durable identities change."""

from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process import mrf_address_publication as address
from process import mrf_publication_receipt as receipt


def _result(value):
    return SimpleNamespace(
        scalar_one=lambda: value,
        scalar_one_or_none=lambda: value,
        mappings=lambda: SimpleNamespace(one_or_none=lambda: value),
    )


def _publication(monkeypatch):
    generation = SimpleNamespace(relation_oids=(11,), as_dict=lambda: {"generation": 1})
    content = {"archive_oid": 12, "tables": {"mrf_address": {"uncovered": 0}}}
    row = {
        "contract_version": 1,
        "state": "complete",
        "generation": generation.as_dict(),
        "summary_inputs": {"plan": 13},
        "summary_oid": 14,
        "address_content": content,
    }
    monkeypatch.setattr(receipt, "capture_summary_inputs", AsyncMock(return_value={"plan": 13}))
    monkeypatch.setattr(
        receipt, "read_reference_family_result_generation_authority", AsyncMock(return_value=generation)
    )
    monkeypatch.setattr(receipt, "current_reference_family_relation_oids", AsyncMock(return_value=(11,)))
    monkeypatch.setattr(receipt, "capture_address_content", AsyncMock(return_value=content))
    return generation, row


@pytest.mark.parametrize("schema,name", [("bad-name", "valid"), ("valid", "x" * 64)])
def test_receipt_qualification_rejects_untrusted_identifiers(schema, name):
    with pytest.raises(ValueError, match="invalid MRF receipt identifier"):
        receipt.qualified(schema, name)


@pytest.mark.asyncio
async def test_pending_claim_requires_its_own_durable_transaction(monkeypatch):
    monkeypatch.setattr(receipt.db, "_transaction_binding", lambda: object())
    transaction = Mock()
    monkeypatch.setattr(receipt.db, "transaction", transaction)
    with pytest.raises(RuntimeError, match="independent durable transaction"):
        await receipt.begin_publication("synthetic", "attempt")
    transaction.assert_not_called()


@pytest.mark.asyncio
async def test_pending_claim_cannot_replace_an_existing_pending_writer(monkeypatch):
    session = SimpleNamespace(execute=AsyncMock(return_value=_result(None)))

    @asynccontextmanager
    async def transaction():
        yield session

    monkeypatch.setattr(receipt.db, "_transaction_binding", lambda: None)
    monkeypatch.setattr(receipt.db, "transaction", transaction)
    with pytest.raises(RuntimeError, match="publication is pending"):
        await receipt.begin_publication("synthetic", "attempt")
    assert session.execute.await_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure,message",
    [
        ("execution", "explicit boolean"),
        ("inputs", "summary inputs changed"),
        ("generation", "generation changed"),
        ("oids", "generation changed"),
        ("claim", "claim changed"),
    ],
)
async def test_completion_rejects_changed_publication_authority(monkeypatch, failure, message):
    generation, _ = _publication(monkeypatch)
    session = SimpleNamespace(execute=AsyncMock(side_effect=[_result(14), _result(None)]))
    if failure == "generation":
        monkeypatch.setattr(receipt, "read_reference_family_result_generation_authority", AsyncMock(return_value=None))
    if failure == "oids":
        monkeypatch.setattr(receipt, "current_reference_family_relation_oids", AsyncMock(return_value=(99,)))
    with pytest.raises((ValueError, RuntimeError), match=message):
        await receipt.complete_publication(
            session,
            "synthetic",
            "attempt",
            generation,
            {} if failure == "inputs" else {"plan": 13},
            None if failure == "execution" else True,
        )
    assert session.execute.await_count == (2 if failure == "claim" else 0)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure,message",
    [
        ("transaction", "capture transaction"),
        ("absent", "completion is unavailable"),
        ("version", "completion is unavailable"),
        ("pending", "completion is unavailable"),
        ("generation", "generation differs"),
        ("oids", "generation differs"),
        ("summary", "summary identity differs"),
        ("address", "address content differs"),
    ],
)
async def test_export_admission_rechecks_all_completed_identities(monkeypatch, failure, message):
    _, row = _publication(monkeypatch)
    if failure == "version":
        row["contract_version"] = 2
    if failure == "pending":
        row["state"] = "pending"
    if failure == "generation":
        row["generation"] = {}
    if failure == "oids":
        monkeypatch.setattr(receipt, "current_reference_family_relation_oids", AsyncMock(return_value=(99,)))
    if failure == "summary":
        row["summary_oid"] = 99
    if failure == "address":
        row["address_content"] = {}
    session = SimpleNamespace(
        in_transaction=lambda: failure != "transaction",
        execute=AsyncMock(side_effect=[_result(10), _result(None if failure == "absent" else row), None, _result(14)]),
    )
    with pytest.raises(RuntimeError, match=message):
        await receipt.require_completed_publication(session, "synthetic")


@pytest.mark.asyncio
async def test_missing_canonical_archive_records_uncovered_addresses(monkeypatch):
    session = SimpleNamespace(scalar=AsyncMock(side_effect=[None, 2, 3]), execute=AsyncMock())
    projected = AsyncMock(side_effect=[(2, "a" * 64), (3, "b" * 64)])
    monkeypatch.setattr(address, "_projected_row_identity", projected)
    content = await address.capture_address_content(session, "synthetic", receipt.qualified)
    assert content["archive_oid"] is None
    assert [table["uncovered"] for table in content["tables"].values()] == [2, 3]
    assert all(call.kwargs["row_json_sql"] == "to_jsonb(row_value)" for call in projected.await_args_list)
    session.execute.assert_not_awaited()
    with pytest.raises(RuntimeError, match="coverage is incomplete"):
        address.require_address_coverage(content)
