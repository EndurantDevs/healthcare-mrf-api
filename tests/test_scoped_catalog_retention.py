# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Logical retained records, native readback and bounded exact-name cleanup."""

from copy import deepcopy
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from process import reference_family_archive as native
from process import scoped_catalog_named_retention as named
from process import scoped_catalog_retention as retention
from tests.test_scoped_catalog_handoff import _generation


def _receipt():
    before = _generation()
    after_by_field = {**before, "local_generation": 1}
    return {
        "contract": named.CONTRACT,
        "importer": "code-sets",
        "dataset_id": str(UUID(int=1)),
        "schema_name": native.reference_family_predecessor_schema(UUID(int=1)),
        "live_schema": "serving",
        "generation_table": "code_sets_result_generation",
        "previous_generations": {"code-sets": before},
        "published_generations": {"code-sets": after_by_field},
        "publication_handoff_sha256": "c" * 64,
        "tables": [{"table": "code_catalog", "row_count": 2, "row_sha256": "a" * 64, "schema_sha256": "b" * 64}],
    }


@pytest.mark.parametrize("unsupported", [object(), datetime(2026, 1, 1)])
def test_metadata_refuses_unsupported_or_timezone_free_values(unsupported):
    with pytest.raises(TypeError, match="unsupported catalog metadata scalar"):
        retention.canonical_metadata({"published_at": unsupported})


def test_retained_json_roundtrip_preserves_input_and_checks_destination():
    receipt = _receipt()
    before = deepcopy(receipt)
    assert retention.validate_retained_catalog(receipt, importer="code-sets", live_schema="serving") is receipt
    assert receipt == before
    for options in ({"importer": "ms-drg"}, {"live_schema": "other"}):
        with pytest.raises(RuntimeError, match="destination"):
            retention.validate_retained_catalog(receipt, **options)


@pytest.mark.parametrize(
    "field,replacement",
    [
        ("contract", "other"),
        ("importer", "other"),
        ("schema_name", "serving"),
        ("generation_table", "code_catalog"),
        ("previous_generations", {}),
        ("published_generations", {}),
        ("tables", []),
        ("tables", None),
        ("tables", [None]),
        ("publication_handoff_sha256", True),
        ("publication_handoff_sha256", "x" * 64),
    ],
)
def test_retained_parser_refuses_wrong_logical_scope(field, replacement):
    receipt = _receipt()
    receipt[field] = replacement
    with pytest.raises((RuntimeError, KeyError, TypeError, ValueError)):
        retention.validate_retained_catalog(receipt)


@pytest.mark.parametrize(
    "field,replacement", [("row_count", True), ("row_count", -1), ("row_sha256", "x" * 64), ("schema_sha256", "bad")]
)
def test_retained_parser_refuses_invalid_data_receipt(field, replacement):
    receipt = _receipt()
    receipt["tables"][0][field] = replacement
    with pytest.raises(RuntimeError, match="receipt differs"):
        retention.validate_retained_catalog(receipt)


def test_old_retained_record_uses_logical_fields_without_rewriting_copied_metadata():
    receipt = _receipt()
    receipt["contract"] = "scoped-catalog-retained.v1"
    receipt["current_generation"] = receipt.pop("published_generations")["code-sets"]
    receipt.update(database_oid=99, owner_oid=98, schema_oid=97, generation_oid=96, receipt_sha256="old")
    receipt["tables"][0]["relation_oid"] = 95
    original = deepcopy(receipt)
    logical = named.logical_receipt(receipt)
    assert logical["published_generations"]["code-sets"] == receipt["current_generation"]
    assert logical["tables"][0] == _receipt()["tables"][0]
    assert receipt == original


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["bound", "cas"])
async def test_retained_record_refuses_oversize_or_existing_slot_before_grants(monkeypatch, failure):
    receipt = _receipt()
    if failure == "bound":
        receipt["extra"] = "x" * 65536
    session = SimpleNamespace(scalar=AsyncMock(return_value=None), execute=AsyncMock())
    protect = AsyncMock()
    monkeypatch.setattr(named.storage, "protect_tables", protect)
    with pytest.raises(RuntimeError, match="bound|already exists"):
        await named.record_retained(session, receipt, object())
    protect.assert_not_awaited()
    if failure == "bound":
        session.execute.assert_not_awaited()
    else:
        assert "retained_family IS NULL RETURNING id" in str(session.scalar.await_args.args[0])


@pytest.mark.asyncio
@pytest.mark.parametrize("change", [None, "generation", "content", "persisted", "receipt"])
async def test_retained_readback_compares_actual_content_and_generations(monkeypatch, change):
    receipt = _receipt()
    persisted = deepcopy(receipt)
    if change == "persisted":
        persisted["tables"][0]["row_count"] = 3
    session = SimpleNamespace(scalar=AsyncMock(return_value=persisted), execute=AsyncMock())
    vector = deepcopy(receipt["previous_generations"])
    if change == "generation":
        vector["code-sets"]["local_generation"] = 1
    monkeypatch.setattr(named.generations, "read_generations", AsyncMock(return_value=vector))
    monkeypatch.setattr(
        named.storage, "table_receipts", AsyncMock(return_value=[] if change == "content" else receipt["tables"])
    )
    validate = AsyncMock(side_effect=RuntimeError("source receipt differs") if change == "receipt" else None)
    monkeypatch.setattr(named.generations, "validate_published_content", validate)
    if change:
        with pytest.raises(RuntimeError, match="differs"):
            await retention.require_retained_catalog(session, receipt)
    else:
        assert await retention.require_retained_catalog(session, receipt) is receipt
    if change in (None, "receipt"):
        validate.assert_awaited_once_with(session, receipt["schema_name"], receipt["previous_generations"])
    else:
        validate.assert_not_awaited()
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("refusal", [None, "record", "pinned", "recheck"])
async def test_retained_cleanup_rechecks_after_lock_before_exact_restrict_drops(monkeypatch, refusal):
    receipt = _receipt()
    session = SimpleNamespace(execute=AsyncMock())
    logical = named.logical_receipt(receipt)
    require = AsyncMock(return_value=logical)
    if refusal == "record":
        require.side_effect = RuntimeError("record refused")
    elif refusal == "recheck":
        require.side_effect = [logical, RuntimeError("record changed")]
    monkeypatch.setattr(named, "require_retained", require)
    monkeypatch.setattr(
        native, "_lock_family", AsyncMock(side_effect=RuntimeError("reader pinned") if refusal == "pinned" else None)
    )
    if refusal:
        with pytest.raises(RuntimeError):
            await retention.cleanup_retained_catalog(session, receipt)
        session.execute.assert_not_awaited()
    else:
        await retention.cleanup_retained_catalog(session, receipt)
        assert require.await_count == 2
        assert [str(call.args[0]) for call in session.execute.await_args_list] == [
            f'DROP TABLE "{receipt["schema_name"]}"."code_catalog" RESTRICT',
            f'DROP TABLE "{receipt["schema_name"]}"."code_sets_result_generation" RESTRICT',
            f'DROP SCHEMA "{receipt["schema_name"]}" RESTRICT',
        ]
