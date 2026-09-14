# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Focused malformed-evidence guards for native entity-address snapshots."""

from __future__ import annotations

import asyncio
import importlib
from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from sqlalchemy.exc import SQLAlchemyError

alias = importlib.import_module("process.entity_address_snapshot_alias")
destination = importlib.import_module("process.entity_address_snapshot_destination")
serving = importlib.import_module("process.entity_address_snapshot_serving")
source = importlib.import_module("process.entity_address_snapshot_source")


def _mapped_result(*, rows=None, row=None):
    """Return a SQLAlchemy-shaped mapping result."""

    mappings = MagicMock()
    mappings.all.return_value = [] if rows is None else rows
    mappings.one_or_none.return_value = row
    result = MagicMock()
    result.mappings.return_value = mappings
    return result


def _geo_signature(schema_name="mrf"):
    """Return one complete local geo dependency signature."""

    names = (
        f"{schema_name}.doctor_clinician_address",
        f"{schema_name}.geo_zip_lookup",
        f"{schema_name}.mrf_address",
        f"{schema_name}.npi_address",
        "tiger.zcta5",
        "tiger.zip_state",
    )
    return {name: [index + 10, index + 20] for index, name in enumerate(names)}


def _observed_capture():
    """Return one valid source-local serving capture."""

    return serving.EntityAddressObservedServingCapture(
        contract=serving.CONTRACT,
        source_schema="mrf",
        relation_oids=tuple(range(100, 100 + len(serving._RELATIONS))),
        alias_schema_version=serving.address_alias_sql.ADDRESS_ALIAS_SCHEMA_VERSION,
        alias_ruleset_version=serving.address_alias_sql.ADDRESS_ALIAS_RULESET_VERSION,
        alias_generation=3,
        geo_assurance_version=serving.geo_projection.GEO_ASSURANCE_VERSION,
        geo_active_table_oid=100,
        geo_active_relation_signature=tuple(
            (name, identity[0], identity[1]) for name, identity in sorted(_geo_signature().items())
        ),
    )


@pytest.mark.parametrize("invalid", [None, 0, -1, True, (1 << 32)])
def test_serving_oid_guard_rejects_non_postgres_identity(invalid):
    """Reject booleans, missing values, and out-of-range relation identities."""

    with pytest.raises(ValueError, match="OID.*invalid"):
        serving._positive_oid(invalid, field_name="relation OID")


@pytest.mark.parametrize(
    "mutation",
    [
        lambda value: None,
        lambda value: {**value, "unexpected": True},
        lambda value: {**value, "contract": "future"},
        lambda value: {**value, "source_schema": "other"},
        lambda value: {**value, "relations": value["relations"][:-1]},
        lambda value: {
            **value,
            "relations": [{**value["relations"][0], "table_name": "substitute"}, *value["relations"][1:]],
        },
        lambda value: {**value, "alias_state": {**value["alias_state"], "generation": -1}},
        lambda value: {**value, "geo_assurance": {**value["geo_assurance"], "version": 999}},
        lambda value: {**value, "geo_assurance": {**value["geo_assurance"], "active_table_oid": 999}},
    ],
)
def test_serving_capture_rejects_malformed_or_drifted_identity(mutation):
    """Reject scope, relation, alias, and geo substitutions in queued evidence."""

    valid_by_field = _observed_capture().as_dict()
    with pytest.raises(ValueError):
        serving.validate_entity_address_observed_serving_capture(mutation(valid_by_field), schema_name="mrf")


def test_serving_signature_rejects_missing_and_malformed_dependencies():
    """Require the exact dependency set and two positive catalog identities."""

    signature_by_name = _geo_signature()
    with pytest.raises(ValueError, match="geo signature"):
        serving._signature_tuple(dict(list(signature_by_name.items())[:-1]), schema_name="mrf")
    signature_by_name["mrf.npi_address"] = [1]
    with pytest.raises(ValueError, match="geo signature"):
        serving._signature_tuple(signature_by_name, schema_name="mrf")


def test_serving_schema_and_observed_alias_guards_reject_wrong_types():
    """Reject non-string scope and unsupported observed alias authority."""

    with pytest.raises(ValueError, match="requires a schema name"):
        serving._schema_name(None)
    with pytest.raises(RuntimeError, match="alias state is unsupported"):
        serving._validated_observed_alias_state({"schema_version": 2, "active_ruleset_version": 1, "generation": -1})


@pytest.mark.asyncio
async def test_serving_database_guards_reject_missing_state_and_bad_geo_signature():
    """Reject absent relation/state rows and malformed current assurance evidence."""

    session = SimpleNamespace(execute=AsyncMock(return_value=_mapped_result(row=None)))
    with pytest.raises(RuntimeError, match="relation is unavailable"):
        await serving._relation_oid(session, "mrf", "entity_address_unified")

    session.execute.return_value = _mapped_result(rows=[])
    with pytest.raises(RuntimeError, match="alias state is invalid"):
        await serving._alias_state(session, "mrf")
    with pytest.raises(RuntimeError, match="geo assurance state is invalid"):
        await serving._geo_assurance_state(session, schema_name="mrf", live_table_oid=10)

    session.execute.return_value = _mapped_result(
        rows=[
            {
                "singleton": True,
                "active_geo_assurance_version": serving.geo_projection.GEO_ASSURANCE_VERSION,
                "active_table_oid": 10,
                "active_relation_signature": _geo_signature(),
                "current_relation_signature": {"substitute": [1, 2]},
            }
        ]
    )
    with pytest.raises(RuntimeError, match="geo assurance state is invalid"):
        await serving._geo_assurance_state(session, schema_name="mrf", live_table_oid=10)


@pytest.mark.asyncio
async def test_alias_lock_and_relation_shape_fail_closed():
    """Normalize missing lock targets and unsupported catalog shapes."""

    session = SimpleNamespace(execute=AsyncMock(side_effect=[None, SQLAlchemyError("missing")]))
    with pytest.raises(alias.EntityAddressSnapshotAliasError, match="relations are unavailable"):
        await alias._lock_alias_relations(session, "mrf")

    session.execute = AsyncMock(return_value=_mapped_result(row={"oid": 1, "relkind": "v"}))
    with pytest.raises(alias.EntityAddressSnapshotAliasError, match="relation shape is unsupported"):
        await alias._relation_oid(session, "mrf", "address_alias_v1")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "state_by_field, message",
    [
        ({}, "singleton state is invalid"),
        (
            {"singleton": True, "schema_version": 2, "active_ruleset_version": 999, "generation": 1},
            "ruleset version is unsupported",
        ),
        (
            {"singleton": True, "schema_version": 2, "active_ruleset_version": 1, "generation": -1},
            "generation is invalid",
        ),
    ],
)
async def test_alias_state_rejects_missing_or_unsupported_authority(state_by_field, message):
    """Reject absent singleton authority and unsupported version counters."""

    rows = [] if not state_by_field else [state_by_field]
    session = SimpleNamespace(execute=AsyncMock(return_value=_mapped_result(rows=rows)))
    with pytest.raises(alias.EntityAddressSnapshotAliasError, match=message):
        await alias._alias_state(session, "mrf")


@pytest.mark.parametrize(
    "change",
    [
        {"contract": "future"},
        {"receipt_version": "future"},
        {"local_generation": -1},
        {"active_alias_sha256": "not-a-digest"},
    ],
)
def test_alias_receipt_rejects_untrusted_versions_and_digest(change):
    """Reject persisted alias evidence that cannot identify reviewed semantics."""

    valid_by_field = alias.EntityAddressAliasSemanticReceipt(2, 1, 3, 0, "a" * 64).as_dict()
    with pytest.raises(alias.EntityAddressSnapshotAliasError, match="receipt is invalid"):
        alias.validate_entity_address_alias_semantic_receipt({**valid_by_field, **change})


@pytest.mark.asyncio
async def test_alias_chunk_stream_rejects_noncontiguous_or_malformed_evidence():
    """Close the stream and reject a forged chunk ordinal before hashing it."""

    async def invalid_chunks():
        yield {"chunk_ordinal": 1, "chunk_row_count": 1, "chunk_sha256": "a" * 64}

    chunks = MagicMock()
    chunks.mappings.return_value = invalid_chunks()
    chunks.close = AsyncMock()
    session = SimpleNamespace(stream=AsyncMock(return_value=chunks))
    with pytest.raises(alias.EntityAddressSnapshotAliasError, match="receipt is invalid"):
        await alias._active_alias_identity(session, "mrf")
    chunks.close.assert_awaited_once()


def test_source_export_evidence_requires_exactly_one_receipt_of_each_kind():
    """Never pair a manifest with absent or duplicate semantic evidence."""

    evidence = source._EntityAddressExportEvidence(None, "mrf", uuid4(), AsyncMock(), None)
    with pytest.raises(RuntimeError, match="stage receipt is unavailable"):
        evidence.bound_receipts(SimpleNamespace())
    evidence.archive_receipts.append(SimpleNamespace())
    with pytest.raises(RuntimeError, match="alias receipt is unavailable"):
        evidence.bound_receipts(SimpleNamespace())


def test_source_family_schema_snapshot_and_clone_guards(monkeypatch):
    """Reject invalid model closure, schema, snapshot, and clone capture."""

    duplicate = SimpleNamespace(__name__="Duplicate", __tablename__="entity_address_unified")
    monkeypatch.setattr(source.entity_address_unified, "SUPPORT_TABLE_MODELS", (duplicate,) * 6)
    with pytest.raises(RuntimeError, match="family is incomplete"):
        source.entity_address_archive_relations()
    with pytest.raises(ValueError, match="requires a schema name"):
        source._schema_name(None)

    support_models = tuple(
        SimpleNamespace(__name__=f"Support{index}", __tablename__=("unsafe-name" if index == 0 else f"support_{index}"))
        for index in range(6)
    )
    monkeypatch.setattr(source.entity_address_unified, "SUPPORT_TABLE_MODELS", support_models)
    with pytest.raises(RuntimeError, match="family is invalid"):
        source.entity_address_archive_relations()


@pytest.mark.asyncio
async def test_source_snapshot_tokens_fail_before_archive_or_clone_work():
    """Reject malformed exported and caller-supplied PostgreSQL snapshot tokens."""

    scalar_result = MagicMock()
    scalar_result.scalar_one.return_value = "not a snapshot!"
    session = SimpleNamespace(execute=AsyncMock(return_value=scalar_result))
    with pytest.raises(RuntimeError, match="did not export"):
        await source._export_postgres_snapshot(session)

    capture = source.EntityAddressArchiveSourceCapture(source._CONTRACT, "mrf", (), "not a snapshot!")
    session.execute.reset_mock()
    with pytest.raises(RuntimeError, match="snapshot is invalid"):
        await source._clone_entity_address_archive_source(session, source_capture=capture, stage_schema="stage")
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_source_cleanup_drains_after_repeated_cancellation():
    """Keep exact stage cleanup running and re-raise cancellation only afterward."""

    cleanup_started = asyncio.Event()
    release_cleanup = asyncio.Event()
    cleanup = AsyncMock()

    async def cleanup_side_effect(*_args, **_kwargs):
        cleanup_started.set()
        await release_cleanup.wait()

    cleanup.side_effect = cleanup_side_effect
    ownership = SimpleNamespace(cleanup_entity_address_archive_stage=cleanup)
    session = SimpleNamespace()

    @asynccontextmanager
    async def transaction():
        yield

    @asynccontextmanager
    async def session_factory():
        session.begin = transaction
        yield session

    task = asyncio.create_task(source._cleanup_owned_archive_stage(session_factory, ownership, SimpleNamespace()))
    await cleanup_started.wait()
    task.cancel()
    await asyncio.sleep(0)
    assert not task.done()
    release_cleanup.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    cleanup.assert_awaited_once()


def test_destination_transaction_geo_and_stage_identity_guards():
    """Reject missing transaction, malformed geo receipts, and substituted stage OIDs."""

    with pytest.raises(destination.EntityAddressSnapshotDestinationError, match="caller transaction"):
        destination._require_caller_transaction(SimpleNamespace(in_transaction=lambda: False))
    with pytest.raises(destination.EntityAddressSnapshotDestinationError, match="dependency signature"):
        destination._geo_signature({}, db_schema="mrf")
    invalid_signature = _geo_signature()
    invalid_signature["mrf.npi_address"] = [0, 2]
    with pytest.raises(destination.EntityAddressSnapshotDestinationError, match="dependency signature"):
        destination._geo_signature(invalid_signature, db_schema="mrf")
    with pytest.raises(destination.EntityAddressSnapshotDestinationError, match="preparation receipt"):
        destination._validated_geo_preparation({}, db_schema="mrf")

    prepared = SimpleNamespace(stage_cls=SimpleNamespace(__tablename__="entity_address_unified_stage"))
    with pytest.raises(destination.EntityAddressSnapshotDestinationError, match="stage identity is invalid"):
        destination._prepared_main_stage_oid({"restored": {"stage_relation_oids": []}}, prepared=prepared)
    stored_by_field = {"restored": {"stage_relation_oids": [{"table_name": "entity_address_unified_stage", "oid": 0}]}}
    with pytest.raises(destination.EntityAddressSnapshotDestinationError, match="stage identity is invalid"):
        destination._prepared_main_stage_oid(stored_by_field, prepared=prepared)


@pytest.mark.asyncio
async def test_destination_runtime_cas_guards(monkeypatch):
    """Reject stale geo candidates and post-validation base-version changes."""

    session = SimpleNamespace(execute=AsyncMock(return_value=_mapped_result(row=None)))
    with pytest.raises(destination.EntityAddressSnapshotDestinationError, match="candidate is stale"):
        await destination._capture_geo_preparation(session, db_schema="mrf", stage_table_oid=1, projected_rows=0)

    monkeypatch.setattr(destination, "_base_version_counts", AsyncMock(return_value=(0, 0, 1, 0)))
    prepared = SimpleNamespace(db_schema="mrf", stage_cls=SimpleNamespace(__tablename__="stage"))
    remap = destination.EntityAddressBaseVersionRemapEvidence(1, 2, 0, 0, 0, 0, "a" * 64, "b" * 64)
    local_alias = alias.EntityAddressAliasSemanticReceipt(2, 1, 2, 0, "c" * 64)
    with pytest.raises(
        destination.EntityAddressSnapshotDestinationError, match="base_address_version evidence differs"
    ):
        await destination._require_prepared_base_versions(
            session,
            prepared=prepared,
            remap_evidence=remap,
            destination_alias=local_alias,
        )


@pytest.mark.asyncio
async def test_destination_alias_source_and_remap_failures_are_normalized(monkeypatch):
    """Normalize invalid source authority and reject partial base-version rewrites."""

    with pytest.raises(destination.EntityAddressSnapshotDestinationError):
        await destination._destination_alias_binding(
            SimpleNamespace(),
            db_schema="mrf",
            source_alias_receipt={},
        )
    with pytest.raises(destination.EntityAddressSnapshotDestinationError):
        await destination._validate_owned_source(
            SimpleNamespace(),
            owner={},
            semantic_receipt={},
            db_schema="mrf",
            import_date="20260914",
        )

    monkeypatch.setattr(destination, "_base_version_counts", AsyncMock(return_value=(0, 0, 2, 0)))
    update_result = SimpleNamespace(rowcount=1)
    session = SimpleNamespace(execute=AsyncMock(return_value=update_result))
    source_alias = alias.EntityAddressAliasSemanticReceipt(2, 1, 1, 0, "a" * 64)
    local_alias = replace(source_alias, local_generation=2)
    with pytest.raises(destination.EntityAddressSnapshotDestinationError, match="remap row count differs"):
        await destination._remap_base_versions(
            session,
            schema_name="mrf",
            source_alias=source_alias,
            destination_alias=local_alias,
            pre_remap_receipt=SimpleNamespace(content_sha256="b" * 64),
        )


def test_destination_metadata_and_remap_receipts_reject_substitution():
    """Reject wrong envelopes and internally inconsistent remap evidence."""

    with pytest.raises(destination.EntityAddressSnapshotDestinationError, match="destination preparation is invalid"):
        destination._validated_destination_metadata({})

    table = destination.EntityAddressArchiveTableReceipt("Main", "main", "a" * 64, 1, "b" * 64)
    source_receipt = destination.EntityAddressArchiveReceipt((table,), "c" * 64, "d" * 64, "e" * 64)
    restored_receipt = replace(source_receipt, content_sha256="f" * 64)
    source_alias = alias.EntityAddressAliasSemanticReceipt(2, 1, 1, 0, "1" * 64)
    destination_alias = replace(source_alias, local_generation=2)
    with pytest.raises(destination.EntityAddressSnapshotDestinationError, match="remap receipt is invalid"):
        destination._validated_remap_evidence(
            {},
            source_receipt=source_receipt,
            restored_receipt=restored_receipt,
            source_alias=source_alias,
            destination_alias=destination_alias,
        )
    invalid_remap_by_field = {
        "contract": destination.BASE_VERSION_REMAP_CONTRACT,
        "source_alias_generation": 1,
        "destination_alias_generation": 2,
        "alias_rows_bound": 1,
        "alias_rows_rewritten": 0,
        "null_rows_preserved": 0,
        "plain_base_rows_preserved": 0,
        "pre_remap_content_sha256": source_receipt.content_sha256,
        "post_remap_content_sha256": restored_receipt.content_sha256,
    }
    with pytest.raises(destination.EntityAddressSnapshotDestinationError, match="remap receipt is invalid"):
        destination._validated_remap_evidence(
            invalid_remap_by_field,
            source_receipt=source_receipt,
            restored_receipt=restored_receipt,
            source_alias=source_alias,
            destination_alias=destination_alias,
        )

    stage_table = replace(table, schema_sha256="9" * 64)
    stage_receipt = destination.EntityAddressStageIntegrityReceipt((stage_table,), "c" * 64, "d" * 64, "e" * 64)
    with pytest.raises(destination.EntityAddressSnapshotDestinationError, match="receipt lineage differs"):
        destination._require_receipt_lineage(source_receipt, source_receipt, stage_receipt)


@pytest.mark.asyncio
@pytest.mark.parametrize("drift", ["alias", "stage_oid", "geo"])
async def test_destination_activation_rechecks_alias_stage_and_geo_cas(monkeypatch, drift):
    """Reject each destination-local identity if it changes after preparation."""

    source_alias = alias.EntityAddressAliasSemanticReceipt(2, 1, 1, 0, "a" * 64)
    local_alias = replace(source_alias, local_generation=2)
    remap = destination.EntityAddressBaseVersionRemapEvidence(1, 2, 0, 0, 0, 0, "b" * 64, "c" * 64)
    expected_geo = destination.EntityAddressGeoAssurancePreparation(10, 0, ())

    @asynccontextmanager
    async def preserve_settings():
        yield

    monkeypatch.setattr(
        destination, "_validated_destination_metadata", lambda _stored: (source_alias, local_alias, remap)
    )
    monkeypatch.setattr(destination, "_preserve_receipt_settings", preserve_settings)
    observed_alias = replace(local_alias, local_generation=3) if drift == "alias" else local_alias
    monkeypatch.setattr(
        destination,
        "_destination_alias_binding",
        AsyncMock(return_value=(source_alias, observed_alias)),
    )
    prepared = SimpleNamespace(db_schema="mrf", stage_cls=SimpleNamespace(__tablename__="stage"))
    monkeypatch.setattr(
        destination.restore, "rehydrate_entity_address_archive_restore", AsyncMock(return_value=prepared)
    )
    monkeypatch.setattr(destination, "_require_prepared_base_versions", AsyncMock())
    monkeypatch.setattr(destination, "_validated_geo_preparation", lambda *_args, **_kwargs: expected_geo)
    monkeypatch.setattr(
        destination, "_prepared_main_stage_oid", lambda *_args, **_kwargs: 11 if drift == "stage_oid" else 10
    )
    actual_geo = replace(expected_geo, projected_rows=1) if drift == "geo" else expected_geo
    monkeypatch.setattr(destination, "_capture_geo_preparation", AsyncMock(return_value=actual_geo))
    monkeypatch.setattr(destination.adoption, "adopt_prepared_entity_address_snapshot", AsyncMock())
    session = SimpleNamespace(execute=AsyncMock())
    message = {
        "alias": "alias generation changed",
        "stage_oid": "stage identity differs",
        "geo": "preparation changed",
    }[drift]
    with pytest.raises(destination.EntityAddressSnapshotDestinationError, match=message):
        await destination._activate_bound_destination(
            session,
            stored={"restored": {"db_schema": "mrf"}, "geo_assurance": {}},
            callbacks=SimpleNamespace(),
        )
