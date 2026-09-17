# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Reject malformed persisted restore state before any destination SQL."""

from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import entity_address_snapshot_receipt as receipt
from process import entity_address_snapshot_restore as restore
from tests.test_entity_address_archive_ownership_guards import _owner
from tests.test_entity_address_snapshot_stage import _receipt


def _stored_restore():
    owner = _owner()
    semantic = receipt.validate_entity_address_archive_receipt(_receipt())
    _, _, stage_names = restore._stage_plan(db_schema="mrf", import_date="20260914")
    tables = tuple(replace(table, table_name=stage_names[table.table_name]) for table in semantic.tables)
    integrity = receipt.EntityAddressStageIntegrityReceipt(
        tables,
        receipt._canonical_digest(
            [
                {"model_name": table.model_name, "table_name": table.table_name, "schema_sha256": table.schema_sha256}
                for table in tables
            ]
        ),
        receipt._content_identity(tables, semantic.main_input_sha256),
        semantic.main_input_sha256,
    )
    return {
        "ownership": owner.as_dict(),
        "db_schema": "mrf",
        "import_date": "20260914",
        "stage_relation_oids": [{"table_name": stage_names[name], "oid": oid} for name, oid in owner.relation_oids],
        "semantic_receipt": semantic.as_dict(),
        "stage_integrity": integrity.as_dict(),
        "context": {"address_alias_generation": 7, "stage_persistence": "p"},
        "native_validation": {"address_alias_generation": 7},
    }


def test_valid_restore_record_round_trips_exact_owner_and_alias_generation():
    stored = _stored_restore()
    schema, date, names, stage_oids, integrity, context, validation = restore._validated_rehydration_state(stored)
    assert (schema, date) == ("mrf", "20260914")
    assert dict(stage_oids) == {names[name]: oid for name, oid in _owner().relation_oids}
    assert integrity.as_dict() == stored["stage_integrity"]
    assert context == stored["context"] and validation == stored["native_validation"]
    context["address_alias_generation"] = 8
    assert stored["context"]["address_alias_generation"] == 7


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,value",
    [
        ("extra", True),
        ("ownership", {}),
        ("db_schema", "bad;schema"),
        ("stage_relation_oids", None),
        ("stage_relation_oids", [{}]),
        ("semantic_receipt", {}),
        ("stage_integrity", {}),
        ("context", []),
        ("context", {"address_alias_generation": -1, "stage_persistence": "p"}),
        ("context", {"address_alias_generation": True, "stage_persistence": "p"}),
        ("context", {"address_alias_generation": 7, "stage_persistence": "u"}),
        ("context", {"address_alias_generation": 7, "stage_persistence": "p", "unexpected": 1}),
        ("context", {"address_alias_generation": 7, "stage_persistence": "p", "phase_timings": object()}),
        ("native_validation", {"address_alias_generation": 8}),
    ],
)
async def test_invalid_persisted_restore_fails_before_locks_or_ddl(field, value):
    stored_by_field = {**_stored_restore(), field: value}
    session = SimpleNamespace(in_transaction=lambda: True, scalar=AsyncMock(), execute=AsyncMock())
    with pytest.raises(restore.EntityAddressSnapshotRestoreError):
        await restore.rehydrate_entity_address_archive_restore(session, stored=stored_by_field)
    session.execute.assert_not_awaited()
    session.scalar.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("mutation", ["duplicate_oid", "foreign_oid", "missing", "semantic_object"])
async def test_restore_metadata_cannot_substitute_owned_stage_relations(mutation):
    stored = _stored_restore()
    if mutation == "duplicate_oid":
        stored["stage_relation_oids"][0]["oid"] = stored["stage_relation_oids"][1]["oid"]
    elif mutation == "foreign_oid":
        stored["stage_relation_oids"][0]["oid"] = 999
    elif mutation == "missing":
        stored["stage_relation_oids"].pop()
    else:
        stored["semantic_receipt"] = receipt.validate_entity_address_archive_receipt(stored["semantic_receipt"])
    session = SimpleNamespace(in_transaction=lambda: True, execute=AsyncMock())
    with pytest.raises(restore.EntityAddressSnapshotRestoreError):
        await restore.rehydrate_entity_address_archive_restore(session, stored=stored)
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_restore_without_caller_transaction_never_touches_destination():
    session = SimpleNamespace(in_transaction=lambda: False, execute=AsyncMock())
    with pytest.raises(restore.EntityAddressSnapshotRestoreError, match="caller transaction"):
        await restore.precreate_entity_address_archive_restore(
            session,
            dataset_id=_owner().dataset_id,
            db_schema="mrf",
            import_date="20260914",
        )
    session.execute.assert_not_awaited()


@pytest.mark.parametrize(
    "index",
    [
        {},
        {"index_elements": []},
        {"index_elements": [1]},
        {"index_elements": ["location_key"], "using": 1},
        {"index_elements": ["location_key"], "include": "not-a-list"},
        {"index_elements": ["location_key"], "where": False},
    ],
)
def test_restore_rejects_invalid_model_index_descriptions(index):
    with pytest.raises(restore.EntityAddressSnapshotRestoreError, match="model index is invalid"):
        restore._additional_index_sql(
            schema_name="stage", table_name="address", stage_table_name="address_20260914", index=index
        )


def test_restore_derives_missing_index_name_from_trusted_columns():
    sql = restore._additional_index_sql(
        schema_name="stage",
        table_name="address",
        stage_table_name="address_20260914",
        index={
            "index_elements": ["location_key"],
            "using": "btree",
            "include": ["entity_id"],
            "where": "entity_id IS NOT NULL",
        },
    )
    assert sql == (
        'CREATE INDEX "address_20260914_idx_location_key" ON "stage"."address"'
        " USING btree (location_key) INCLUDE (entity_id) WHERE entity_id IS NOT NULL"
    )


@pytest.mark.parametrize(
    "sequence_name,stage_name", [("unrelated_id_seq", "address_20260914"), ("address_id_seq", "a" * 63)]
)
def test_restore_sequence_names_cannot_target_unrelated_or_truncated_identifiers(sequence_name, stage_name):
    with pytest.raises(restore.EntityAddressSnapshotRestoreError, match="sequence name is invalid"):
        restore._stage_sequence_name("address", stage_name, sequence_name)


@pytest.mark.asyncio
@pytest.mark.parametrize("primary_name", [None, "", 99])
async def test_restore_rejects_missing_primary_index(primary_name):
    session = SimpleNamespace(scalar=AsyncMock(return_value=primary_name))
    with pytest.raises(restore.EntityAddressSnapshotRestoreError, match="primary index is unavailable"):
        await restore._primary_index_name(session, 17)


@pytest.mark.asyncio
async def test_restore_rejects_duplicate_sequence_ownership_records():
    rows = [{"relname": "address_id_seq"}, {"relname": "address_id_seq"}]
    session = SimpleNamespace(execute=AsyncMock(return_value=SimpleNamespace(mappings=lambda: rows)))
    with pytest.raises(restore.EntityAddressSnapshotRestoreError, match="sequence ownership is invalid"):
        await restore._owned_sequence_names(session, 17)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "sequences,sequence_value", [((), 1), (("other_seq",), 1), (("entity_address_evidence_evidence_id_seq",), 0)]
)
async def test_restore_requires_its_owned_evidence_sequence(monkeypatch, sequences, sequence_value):
    monkeypatch.setattr(restore, "_owned_sequence_names", AsyncMock(return_value=sequences))
    session = SimpleNamespace(scalar=AsyncMock(return_value=sequence_value))
    with pytest.raises(restore.EntityAddressSnapshotRestoreError, match="evidence sequence"):
        await restore._reset_restored_evidence_sequence(session, _owner())
    if sequences != ("entity_address_evidence_evidence_id_seq",):
        session.scalar.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("catalog_results", [[100], [99, 1]])
async def test_restore_never_drops_substituted_or_nonempty_namespace(catalog_results):
    session = SimpleNamespace(scalar=AsyncMock(side_effect=catalog_results), execute=AsyncMock())
    with pytest.raises(restore.EntityAddressSnapshotRestoreError, match="ownership"):
        await restore._drop_empty_owned_schema(session, _owner())
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_restore_rejects_substituted_stage_oid_before_activation():
    session = SimpleNamespace(scalar=AsyncMock(return_value=999), execute=AsyncMock())
    with pytest.raises(restore.EntityAddressSnapshotRestoreError, match="stage OID differs"):
        await restore._verify_stored_stage_oids(session, db_schema="mrf", stage_oids=(("address_20260914", 17),))
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_restore_rejects_semantic_receipt_drift(monkeypatch):
    expected = receipt.validate_entity_address_archive_receipt(_receipt())
    monkeypatch.setattr(
        restore,
        "capture_entity_address_archive_receipt",
        AsyncMock(return_value=replace(expected, main_input_sha256="f" * 64)),
    )
    with pytest.raises(restore.EntityAddressSnapshotRestoreError, match="semantic receipt differs"):
        await restore._actual_receipt(SimpleNamespace(), schema_name="stage", expected=expected)
