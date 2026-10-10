# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact retained authority serialization, native readback and bounded cleanup contracts."""

import hashlib
from copy import deepcopy
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from process import reference_family_archive as native
from process import scoped_catalog_binding as binding
from process import scoped_catalog_retention as retention


def _signed(receipt):
    receipt_by_field = {key: value for key, value in receipt.items() if key != "receipt_sha256"}
    receipt_by_field["receipt_sha256"] = hashlib.sha256(
        retention.canonical_metadata(receipt_by_field).encode()
    ).hexdigest()
    return receipt_by_field


def _receipt():
    return _signed(
        {
            "contract": retention.CONTRACT,
            "importer": "code-sets",
            "dataset_id": str(UUID(int=1)),
            "schema_name": native.reference_family_predecessor_schema(UUID(int=1)),
            "schema_oid": 10,
            "owner_oid": 11,
            "database_oid": 12,
            "live_schema": "serving",
            "generation_table": "code_sets_result_generation",
            "generation_oid": 14,
            "previous_generations": {"code-sets": {"local_generation": 0}},
            "current_generation": {"local_generation": 1},
            "tables": [
                {
                    "table": "code_catalog",
                    "relation_oid": 13,
                    "row_count": 2,
                    "row_sha256": "a" * 64,
                    "schema_sha256": "b" * 64,
                }
            ],
            "published_oids": [["code_catalog", 23]],
            "publication_handoff_sha256": None,
        }
    )


@pytest.mark.parametrize("unsupported", [object(), datetime(2026, 1, 1)])
def test_metadata_refuses_unsupported_or_timezone_free_authority(unsupported):
    """Durable authority never silently stringifies unsupported values or ambiguous timestamps."""
    with pytest.raises(TypeError, match="unsupported catalog metadata scalar"):
        retention.canonical_metadata({"published_at": unsupported})


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["namespace", "generation"])
async def test_retention_refuses_wrong_namespace_or_changed_source_generation(monkeypatch, failure):
    """Retained authority must use its owned predecessor and the exact captured source singleton."""
    from tests.test_scoped_catalog_publication import _prepared

    prepared = _prepared()
    schema = native.reference_family_predecessor_schema(prepared.ownership.dataset_id)
    copy = AsyncMock(return_value={"local_generation": 9})
    monkeypatch.setattr(retention, "copy_generation_authority", copy)
    monkeypatch.setattr(retention, "generation_value", lambda _generation: {"local_generation": 0})
    session = SimpleNamespace(execute=AsyncMock())
    with pytest.raises(native.ReferenceFamilyArchiveError, match="namespace differs|generation differs"):
        await retention.retain_catalog_authority(session, prepared, "other" if failure == "namespace" else schema)
    if failure == "namespace":
        copy.assert_not_awaited()
    else:
        copy.assert_awaited_once_with(session, prepared.importer, "serving", schema)
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["bound", "cas"])
async def test_retained_finish_refuses_oversized_or_changed_authority_before_sealing(monkeypatch, failure):
    """Final authority is bounded and compare-and-set fenced before immutable storage is sealed."""
    from process import scoped_catalog_publication as publication

    receipt = _receipt()
    session = SimpleNamespace(scalar=AsyncMock(return_value=None), execute=AsyncMock())
    seal = AsyncMock()
    monkeypatch.setattr(publication, "_seal_candidate", seal)
    if failure == "bound":
        receipt["extra"] = "x" * 65536
    with pytest.raises(native.ReferenceFamilyArchiveError, match="exceeds its bound|authority changed"):
        await retention.finish_catalog_authority(
            session, SimpleNamespace(owner_oid=11), receipt, {"local_generation": 1}
        )
    if failure == "bound":
        session.scalar.assert_not_awaited()
    else:
        assert "retained_family IS NULL RETURNING id" in str(session.scalar.await_args.args[0])
    seal.assert_not_awaited()
    session.execute.assert_not_awaited()


@pytest.mark.parametrize("tables", [None, [None]])
def test_retained_authority_refuses_non_table_receipts(tables):
    """Re-signing cannot turn a malformed table inventory into native retained authority."""
    receipt = _receipt()
    receipt["tables"] = tables
    with pytest.raises(native.ReferenceFamilyArchiveError, match="tables differ"):
        retention.validate_retained_catalog(_signed(receipt))


def _session(receipt):
    async def scalar(statement, _parameters=None):
        if "current_database()" in str(statement):
            return receipt["database_oid"]
        if "retained_family" in str(statement):
            return receipt
        raise AssertionError(str(statement))

    return SimpleNamespace(in_transaction=lambda: True, scalar=AsyncMock(side_effect=scalar), execute=AsyncMock())


def test_retained_json_roundtrip_has_no_authority_side_effect():
    receipt = _receipt()
    assert retention.validate_retained_catalog(receipt, importer="code-sets", live_schema="serving") is receipt
    with pytest.raises(native.ReferenceFamilyArchiveError, match="importer"):
        retention.validate_retained_catalog(receipt, importer="ms-drg")
    with pytest.raises(native.ReferenceFamilyArchiveError, match="destination"):
        retention.validate_retained_catalog(receipt, live_schema="other")


@pytest.mark.parametrize(
    "field,replacement",
    [
        ("contract", "other"),
        ("importer", "other"),
        ("schema_name", "serving"),
        ("schema_oid", True),
        ("owner_oid", 0),
        ("database_oid", 2**32),
        ("generation_oid", 13),
        ("generation_table", "code_catalog"),
        ("previous_generations", {}),
        ("current_generation", []),
        ("tables", []),
        ("published_oids", [["code_catalog", True]]),
        ("published_oids", [["other", 23]]),
        ("publication_handoff_sha256", True),
        ("publication_handoff_sha256", "x" * 64),
    ],
)
def test_retained_parser_refuses_re_signed_bad_inventory(field, replacement):
    receipt = _receipt()
    receipt[field] = replacement
    with pytest.raises(native.ReferenceFamilyArchiveError):
        retention.validate_retained_catalog(_signed(receipt))


@pytest.mark.parametrize("field,replacement", [("row_count", True), ("row_sha256", "x" * 64), ("relation_oid", -1)])
def test_retained_parser_refuses_invalid_table_receipt(field, replacement):
    receipt = _receipt()
    receipt["tables"][0][field] = replacement
    with pytest.raises(native.ReferenceFamilyArchiveError, match="table receipt"):
        retention.validate_retained_catalog(_signed(receipt))


def _native_boundaries(monkeypatch, receipt):
    pairs = (("code_catalog", 13), ("code_sets_result_generation", 14))
    monkeypatch.setattr(native, "_lock_family", AsyncMock())
    monkeypatch.setattr(native, "_schema_oid", AsyncMock(return_value=receipt["schema_oid"]))
    monkeypatch.setattr(native, "_relation_oid", AsyncMock(side_effect=lambda _s, _schema, name: dict(pairs)[name]))
    monkeypatch.setattr(
        native, "_namespace_relations", AsyncMock(return_value=[{"oid": oid, "relkind": "r"} for _, oid in pairs])
    )
    monkeypatch.setattr(binding, "require_closed_catalog_binding", AsyncMock(return_value=receipt["owner_oid"]))
    monkeypatch.setattr(native, "require_native_read_catalog", AsyncMock())
    monkeypatch.setattr(
        retention, "_read_retained_generation", AsyncMock(return_value=receipt["previous_generations"]["code-sets"])
    )
    monkeypatch.setattr(retention, "_family_receipts", AsyncMock(return_value=receipt["tables"]))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changed", [None, "namespace", "heap", "extra", "owner", "native", "singleton", "content", "persisted", "database"]
)
async def test_retained_authority_requires_native_readback(monkeypatch, changed):
    receipt = _receipt()
    session = _session(receipt)
    _native_boundaries(monkeypatch, receipt)
    if changed == "namespace":
        native._schema_oid.return_value = 99
    if changed == "heap":
        native._relation_oid.side_effect = None
        native._relation_oid.return_value = 99
    if changed == "extra":
        native._namespace_relations.return_value.append({"oid": 99, "relkind": "r"})
    if changed == "owner":
        binding.require_closed_catalog_binding.return_value = 99
    if changed == "native":
        native.require_native_read_catalog.side_effect = native.ReferenceFamilyArchiveError("unsafe native catalog")
    if changed == "singleton":
        retention._read_retained_generation.return_value = {"local_generation": 1}
    if changed == "content":
        retention._family_receipts.return_value = []
    if changed == "persisted":
        session.scalar = AsyncMock(side_effect=[12, {}])
    if changed == "database":
        session.scalar = AsyncMock(return_value=99)
    if changed:
        with pytest.raises(native.ReferenceFamilyArchiveError):
            await retention.require_retained_catalog(session, receipt)
    else:
        assert await retention.require_retained_catalog(session, receipt) is receipt
        binding.require_closed_catalog_binding.assert_awaited_once_with(
            session, receipt["schema_name"], (("code_catalog", 13), ("code_sets_result_generation", 14))
        )
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("refusal", [None, "authority", "serving", "pinned", "recheck"])
async def test_retained_cleanup_has_no_unbound_drop(monkeypatch, refusal):
    receipt, session = _receipt(), _session(_receipt())
    require = AsyncMock(return_value=receipt)
    if refusal == "authority":
        require.side_effect = native.ReferenceFamilyArchiveError("authority refused")
    elif refusal == "recheck":
        require.side_effect = [receipt, native.ReferenceFamilyArchiveError("authority changed")]
    monkeypatch.setattr(retention, "require_retained_catalog", require)
    monkeypatch.setattr(native, "_relation_oid", AsyncMock(return_value=13 if refusal == "serving" else 23))
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


def test_retained_digest_detects_inventory_edit():
    receipt = deepcopy(_receipt())
    receipt["tables"][0]["row_count"] += 1
    with pytest.raises(native.ReferenceFamilyArchiveError, match="digest"):
        retention.validate_retained_catalog(receipt)
