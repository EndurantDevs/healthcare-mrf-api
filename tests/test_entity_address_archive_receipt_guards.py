# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Corrupted archive receipts must not become destination activation evidence."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import entity_address_snapshot_receipt as receipt
from tests.test_entity_address_archive_restore_guards import _stored_restore
from tests.test_entity_address_snapshot_stage import _receipt


@pytest.mark.parametrize(
    "changes",
    [
        {"contract": "unknown"},
        {"main_input_sha256": None},
        {"main_input_sha256": "bad"},
        {"tables": {}},
        {"tables": []},
        {"schema_sha256": "0" * 64},
        {"content_sha256": "0" * 64},
        {"extra": 1},
    ],
)
@pytest.mark.parametrize("stage", [False, True])
def test_receipts_reject_incomplete_or_corrupted_identity(changes, stage):
    stored = _stored_restore()
    value = stored["stage_integrity"] if stage else _receipt()
    value.update(changes)
    with pytest.raises(receipt.EntityAddressArchiveReceiptError, match="receipt is invalid"):
        if stage:
            receipt.validate_entity_address_stage_integrity_receipt(value, stage_table_names=_stage_names())
        else:
            receipt.validate_entity_address_archive_receipt(value)


def _stage_names():
    return {model.__tablename__: f"{model.__tablename__}_20260914" for model in receipt._models()}


def _set_receipt():
    from process.entity_address_snapshot_alias import EntityAddressAliasAuthority

    tables = tuple(
        receipt.EntityAddressArchiveTableReceipt(model.__name__, model.__tablename__, "a" * 64, 1)
        for model in (*receipt._models(), EntityAddressAliasAuthority)
    )
    return receipt.EntityAddressArchiveReceipt(
        tables, receipt._table_schema_digest(tables), contract=receipt.CONTRACT
    ).as_dict()


def test_v2_descriptor_closed_inventory():
    stored = _set_receipt()
    validated = receipt.validate_entity_address_archive_receipt(stored)
    assert validated.as_dict() == stored
    assert set(stored) == {"contract", "receipt_version", "tables", "schema_sha256"}
    assert len(stored["tables"]) == 8
    assert all(set(entry) == {"model_name", "table_name", "schema_sha256", "row_count"} for entry in stored["tables"])


async def test_v2_alias_requires_typed_content():
    from process import entity_address_snapshot_alias as aliases

    descriptor_by_field = {
        "contract": aliases.SET_CONTRACT,
        "receipt_version": aliases.SET_CONTRACT,
        "alias_schema_version": 2,
        "active_ruleset_version": 1,
        "local_generation": 3,
        "active_alias_count": 1,
    }
    validated = aliases.validate_entity_address_alias_semantic_receipt(descriptor_by_field)
    assert validated.as_dict() == descriptor_by_field
    with pytest.raises(aliases.EntityAddressSnapshotAliasError, match="actual alias set authority"):
        aliases.require_matching_entity_address_alias_semantics(validated, validated)
    session = SimpleNamespace(scalar=AsyncMock(return_value=False))
    with pytest.raises(aliases.EntityAddressSnapshotAliasError, match="authority differs"):
        await aliases.require_matching_entity_address_alias_authority(
            session, authority_schema="isolated", alias_schema="example"
        )
    statement = str(session.scalar.await_args.args[0])
    assert "FULL JOIN" in statement and "revoked_at IS NULL" in statement
    assert all(f'a."{name}" IS DISTINCT FROM b."{name}"' in statement for name in aliases._SEMANTIC_COLUMNS)
    assert "sha" not in statement.lower()


@pytest.mark.parametrize("mutation", ["legacy_version", "payload_hash", "missing_auxiliary", "extra_auxiliary"])
def test_v2_descriptor_rejects_mixed_versions_and_open_inventory(mutation):
    stored = _set_receipt()
    if mutation == "legacy_version":
        stored["receipt_version"] = receipt.LEGACY_CONTRACT
    elif mutation == "payload_hash":
        stored["tables"][0]["row_sha256"] = "b" * 64
    elif mutation == "missing_auxiliary":
        stored["tables"].pop()
    else:
        stored["tables"].append(dict(stored["tables"][-1]))
    with pytest.raises(receipt.EntityAddressArchiveReceiptError):
        receipt.validate_entity_address_archive_receipt(stored)


@pytest.mark.parametrize("entry", [None, {}, {"unexpected": True}])
@pytest.mark.parametrize("stage", [False, True])
def test_receipt_table_entries_must_be_closed_records(entry, stage):
    value = _stored_restore()["stage_integrity"] if stage else _receipt()
    value["tables"][0] = entry
    with pytest.raises(receipt.EntityAddressArchiveReceiptError, match="receipt is invalid"):
        if stage:
            receipt.validate_entity_address_stage_integrity_receipt(value, stage_table_names=_stage_names())
        else:
            receipt.validate_entity_address_archive_receipt(value)


@pytest.mark.parametrize(
    "field,value",
    [
        ("table_name", "unrelated"),
        ("model_name", "Unknown"),
        ("row_count", True),
        ("row_count", -1),
        ("schema_sha256", "invalid"),
        ("row_sha256", "invalid"),
    ],
)
def test_stage_integrity_rejects_forged_table_fields(field, value):
    stored = _stored_restore()["stage_integrity"]
    stored["tables"][0][field] = value
    with pytest.raises(receipt.EntityAddressArchiveReceiptError, match="receipt is invalid"):
        receipt.validate_entity_address_stage_integrity_receipt(stored, stage_table_names=_stage_names())


@pytest.mark.parametrize("mutation", ["missing", "duplicate", "unsafe", "nonstring"])
def test_stage_receipt_mapping_cannot_alias_or_omit_relations(mutation):
    names = _stage_names()
    main_name = receipt.entity_address_unified.EntityAddressUnified.__tablename__
    if mutation == "missing":
        names.pop(main_name)
    elif mutation == "duplicate":
        names[main_name] = names[receipt.entity_address_unified.SUPPORT_TABLE_MODELS[0].__tablename__]
    elif mutation == "unsafe":
        names[main_name] = "other.stage"
    else:
        names[main_name] = None
    with pytest.raises(receipt.EntityAddressArchiveReceiptError, match="relation family is invalid"):
        receipt.validate_entity_address_stage_integrity_receipt({}, stage_table_names=names)


@pytest.mark.parametrize("value", [None, 1, object()])
def test_receipt_schema_requires_a_string(value):
    with pytest.raises(receipt.EntityAddressArchiveReceiptError, match="schema is invalid"):
        receipt._schema_name(value)


def test_catalog_digest_rejects_unsupported_values():
    with pytest.raises(TypeError, match="unsupported"):
        receipt._canonical_digest({"unexpected": object()})


@pytest.mark.asyncio
async def test_receipt_without_transaction_cannot_acquire_locks():
    session = SimpleNamespace(in_transaction=lambda: False, execute=AsyncMock())
    with pytest.raises(receipt.EntityAddressArchiveReceiptError, match="caller transaction"):
        await receipt.capture_entity_address_archive_receipt(session, schema_name="stage")
    session.execute.assert_not_awaited()


@pytest.mark.parametrize(
    "group,key", [(0, "default_expression"), (1, "check_expression"), (2, "predicate"), (2, "expressions")]
)
@pytest.mark.parametrize("expression", ["source_stage.table_name", '"source_stage".table_name'])
def test_schema_bound_expressions_are_not_portable(group, key, expression):
    catalogs = [[], [], []]
    catalogs[group].append({key: expression})
    with pytest.raises(receipt.EntityAddressArchiveReceiptError, match="expression is unsupported"):
        receipt._reject_schema_qualified_expressions("source_stage", *catalogs)


def test_schema_name_inside_unqualified_owned_sequence_is_portable():
    catalogs = [[{"default_expression": "nextval('mrf_address_id_seq'::regclass)"}], [], []]

    receipt._reject_schema_qualified_expressions("mrf", *catalogs)


@pytest.mark.asyncio
async def test_schema_receipt_rejects_relation_without_columns(monkeypatch):
    for reader in ("_catalog_columns", "_catalog_constraints", "_catalog_indexes"):
        monkeypatch.setattr(receipt, reader, AsyncMock(return_value=[]))
    with pytest.raises(receipt.EntityAddressArchiveReceiptError, match="has no columns"):
        await receipt._schema_identity(SimpleNamespace(), 1, "stage", "address")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changes", [{"chunk_ordinal": 1}, {"chunk_row_count": 0}, {"chunk_row_count": 4097}, {"chunk_sha256": "invalid"}]
)
async def test_invalid_row_chunks_fail_closed(changes):
    chunk_by_field = {"chunk_ordinal": 0, "chunk_row_count": 1, "chunk_sha256": "a" * 64, **changes}
    result = SimpleNamespace(mappings=lambda: [chunk_by_field])
    session = SimpleNamespace(execute=AsyncMock(return_value=result))
    with pytest.raises(receipt.EntityAddressArchiveReceiptError, match="row receipt is invalid"):
        await receipt._row_identity(session, "stage", "address")


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", [False, True])
async def test_inconsistent_main_input_census_cannot_produce_receipt(monkeypatch, stage):
    monkeypatch.setattr(receipt, "_relation_oid", AsyncMock(return_value=1))
    monkeypatch.setattr(receipt, "_schema_identity", AsyncMock(return_value="a" * 64))
    monkeypatch.setattr(receipt, "_row_identity", AsyncMock(return_value=(1, "b" * 64)))
    monkeypatch.setattr(receipt, "_main_input_identity", AsyncMock(return_value=(2, "c" * 64)))
    session = SimpleNamespace(in_transaction=lambda: True, execute=AsyncMock())
    with pytest.raises(receipt.EntityAddressArchiveReceiptError, match="main input receipt is invalid"):
        if stage:
            await receipt.capture_entity_address_stage_integrity_receipt(
                session, schema_name="stage", stage_table_names=_stage_names()
            )
        else:
            await receipt.capture_entity_address_archive_receipt(session, schema_name="stage")
    assert receipt._row_identity.await_count == 7
    assert any(str(call.args[0]).startswith("LOCK TABLE ") for call in session.execute.await_args_list)
