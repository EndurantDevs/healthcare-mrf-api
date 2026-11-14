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


def _legacy_prepared_adoption():
    """Build complete legacy prepared state for destination refusal fixtures."""
    return destination.adoption.PreparedEntityAddressSnapshotAdoption(
        db_schema="mrf",
        stage_cls=type("Stage", (), {"__tablename__": "stage"}),
        support_stage_class_map={},
        swaps=[],
        patch_statements=[],
        relation_names=["stage"],
        required_names=["stage"],
        context={
            "address_alias_generation": 2,
            "stage_persistence": "p",
            "result_generation_mode": "adoption",
            "source_serving_generation": None,
        },
        publish_validation={},
    )


@pytest.mark.parametrize("legacy", [None, 0, -1, True, (1 << 32)])
def test_serving_ignores_legacy_catalog_numbers(legacy):
    capture = _observed_capture().as_dict()
    capture["contract"] = serving.LEGACY_CONTRACT
    capture["geo_assurance"] = {"version": 1, "active_table_oid": legacy, "active_relation_signature": None}
    for relation in capture["relations"]:
        relation["relation_oid"] = legacy
    capture["geo_assurance"]["active_table_oid"] = legacy
    normalized = serving.validate_entity_address_observed_serving_capture(capture)
    assert normalized.relation_oids == (None,) * 7
    assert normalized.geo_active_table_oid is None


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
    ],
)
def test_serving_capture_rejects_malformed_or_drifted_identity(mutation):
    """Reject scope, relation, alias, and geo substitutions in queued evidence."""

    valid_by_field = _observed_capture().as_dict()
    with pytest.raises(ValueError):
        serving.validate_entity_address_observed_serving_capture(mutation(valid_by_field), schema_name="mrf")


def test_serving_ignores_legacy_dependency_signatures():
    capture = _observed_capture().as_dict()
    capture["contract"] = serving.LEGACY_CONTRACT
    for relation in capture["relations"]:
        relation["relation_oid"] = None
    capture["geo_assurance"] = {
        "version": 1,
        "active_table_oid": None,
        "active_relation_signature": {"old.snapshot": [1, 2]},
    }
    normalized = serving.validate_entity_address_observed_serving_capture(capture)
    assert normalized.geo_active_relation_signature is None


def test_serving_schema_and_observed_alias_guards_reject_wrong_types():
    """Reject non-string scope and unsupported observed alias authority."""

    with pytest.raises(ValueError, match="requires a schema name"):
        serving._schema_name(None)
    with pytest.raises(RuntimeError, match="alias state is unsupported"):
        serving._validated_observed_alias_state({"schema_version": 2, "active_ruleset_version": 1, "generation": -1})


@pytest.mark.asyncio
async def test_serving_database_guards_reject_missing_state_and_geo_version():
    """Keep named-relation and semantic-version checks after legacy IDs are ignored."""

    session = SimpleNamespace(execute=AsyncMock(return_value=_mapped_result(row=None)))
    with pytest.raises(RuntimeError, match="relation is unavailable"):
        await serving._relation_name(session, "mrf", "entity_address_unified")

    session.execute.return_value = _mapped_result(rows=[])
    with pytest.raises(RuntimeError, match="alias state is invalid"):
        await serving._alias_state(session, "mrf")
    with pytest.raises(RuntimeError, match="geo assurance state is invalid"):
        await serving._geo_assurance_state(session, schema_name="mrf")

    session.execute.return_value = _mapped_result(
        rows=[
            {
                "singleton": True,
                "active_geo_assurance_version": 999,
                "active_table_oid": 10,
                "active_relation_signature": _geo_signature(),
                "current_relation_signature": {"substitute": [1, 2]},
            }
        ]
    )
    with pytest.raises(RuntimeError, match="geo assurance is not active"):
        await serving._geo_assurance_state(session, schema_name="mrf")


@pytest.mark.asyncio
@pytest.mark.parametrize("receiving", (False, True))
async def test_stale_geo_incumbent_is_replaceable_but_not_source_authority(receiving):
    state_by_field = {
        "singleton": True,
        "active_geo_assurance_version": 1,
        "npi_generation_matches": receiving,
    }
    session = SimpleNamespace(execute=AsyncMock(return_value=_mapped_result(rows=[state_by_field])))
    if receiving:
        assert await serving._geo_assurance_state(session, schema_name="mrf", allow_unpublished_geo=True) == 1
    else:
        with pytest.raises(RuntimeError, match="geo assurance is not active"):
            await serving._geo_assurance_state(session, schema_name="mrf")
    statement = str(session.execute.await_args.args[0])
    assert ("npi_result_generation" in statement) is not receiving


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


def test_destination_transaction_and_named_geo_guards():
    """Reject missing transactions and malformed logical geo receipts."""

    with pytest.raises(destination.EntityAddressSnapshotDestinationError, match="caller transaction"):
        destination._require_caller_transaction(SimpleNamespace(in_transaction=lambda: False))
    with pytest.raises(destination.EntityAddressSnapshotDestinationError, match="dependency signature"):
        destination._geo_signature({}, db_schema="mrf")
    signature_by_name = {
        name: dict(zip(("schema_name", "table_name"), name.split("."), strict=True)) for name in _geo_signature()
    }
    generation = ("12345678-1234-5678-1234-567812345678", 1)
    signature_by_name["mrf.npi_address"]["generation"] = {
        "origin_lineage_id": generation[0],
        "origin_generation": generation[1],
    }
    invalid_signature_by_name = {
        **signature_by_name,
        "mrf.npi_address": {"schema_name": "bad;schema", "table_name": "npi_address"},
    }
    with pytest.raises(destination.EntityAddressSnapshotDestinationError, match="dependency signature"):
        destination._geo_signature(invalid_signature_by_name, db_schema="mrf")
    with pytest.raises(destination.EntityAddressSnapshotDestinationError, match="preparation receipt"):
        destination._validated_geo_preparation({}, db_schema="mrf")

    stored = destination.EntityAddressGeoAssurancePreparation(
        None, 0, destination._geo_signature(signature_by_name, db_schema="mrf"), npi_generation=generation
    ).as_dict()
    stored["stage_table_oid"] = 999
    assert destination._validated_geo_preparation(stored, db_schema="mrf").stage_table_oid is None


def test_historical_geo_pairs_use_stored_named_context_without_rewriting():
    signature = _geo_signature()
    bindings_by_name = {
        name: {
            "schema_name": "retained_inputs",
            "table_name": name.split(".")[1],
            "relation_oid": 0,
            "relfilenode": False,
        }
        for name in signature
    }
    stored_by_field = {
        "contract": destination.GEO_PREPARATION_CONTRACT,
        "geo_assurance_version": 1,
        "stage_table_oid": 777,
        "projected_rows": 5,
        "relation_signature": signature,
    }
    normalized = destination._validated_geo_preparation(
        stored_by_field, db_schema="mrf", dependency_bindings=bindings_by_name
    )
    assert normalized.stage_table_oid is None and normalized.projected_rows == 5
    assert all(schema == "retained_inputs" for _name, schema, _table in normalized.relation_signature)
    assert stored_by_field["relation_signature"] == signature and stored_by_field["stage_table_oid"] == 777
    with pytest.raises(destination.EntityAddressSnapshotDestinationError, match="dependency signature"):
        destination._validated_geo_preparation(stored_by_field, db_schema="mrf")
    stored_by_field["publisher_selected_inputs_sha256"] = "a" * 64
    with pytest.raises(destination.EntityAddressSnapshotDestinationError, match="legacy selected geo dependencies"):
        destination._validated_geo_preparation(stored_by_field, db_schema="mrf")
    assert (
        destination._validated_geo_preparation(
            stored_by_field, db_schema="mrf", dependency_bindings=bindings_by_name
        ).publisher_selected_inputs_sha256
        == "a" * 64
    )


@pytest.mark.asyncio
async def test_historical_geo_candidate_compares_named_context_and_row_accounting():
    bindings_by_name = {
        name: {"schema_name": "retained_inputs", "table_name": name.split(".")[1]} for name in _geo_signature()
    }
    state_by_field = {
        "candidate_geo_assurance_version": 1,
        "candidate_table_oid": 777,
        "candidate_projected_rows": 5,
        "candidate_relation_signature": _geo_signature(),
        "current_signature": bindings_by_name,
        "candidate_dependency_bindings": {
            name: {**binding, "relation_oid": 0, "relfilenode": False} for name, binding in bindings_by_name.items()
        },
        "bindings_match": True,
    }
    session = SimpleNamespace(execute=AsyncMock(return_value=_mapped_result(row=state_by_field)))
    captured = await destination._capture_geo_preparation(
        session, db_schema="mrf", stage_table_oid=None, projected_rows=5, dependency_bindings=bindings_by_name
    )
    assert captured.stage_table_oid is None
    assert all(schema == "retained_inputs" for _name, schema, _table in captured.relation_signature)
    for field, changed in (("candidate_projected_rows", 6), ("bindings_match", False)):
        original = state_by_field[field]
        state_by_field[field] = changed
        with pytest.raises(destination.EntityAddressSnapshotDestinationError, match="candidate is stale"):
            await destination._capture_geo_preparation(
                session, db_schema="mrf", stage_table_oid=None, projected_rows=5, dependency_bindings=bindings_by_name
            )
        state_by_field[field] = original


@pytest.mark.asyncio
async def test_destination_runtime_cas_guards(monkeypatch):
    """Reject stale geo candidates and post-validation base-version changes."""

    session = SimpleNamespace(execute=AsyncMock(return_value=_mapped_result(row=None)))
    with pytest.raises(destination.EntityAddressSnapshotDestinationError, match="candidate is stale"):
        await destination._capture_geo_preparation(session, db_schema="mrf", stage_table_oid=1, projected_rows=0)

    monkeypatch.setattr(destination, "_base_version_counts", AsyncMock(return_value=(0, 0, 1, 0)))
    prepared = _legacy_prepared_adoption()
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
@pytest.mark.parametrize("drift", ["alias", "geo"])
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
    prepared = _legacy_prepared_adoption()
    monkeypatch.setattr(
        destination.restore, "rehydrate_entity_address_archive_restore", AsyncMock(return_value=prepared)
    )
    monkeypatch.setattr(destination, "_require_prepared_base_versions", AsyncMock())
    monkeypatch.setattr(destination, "_validated_geo_preparation", lambda *_args, **_kwargs: expected_geo)
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
    destination.adoption.adopt_prepared_entity_address_snapshot.assert_not_awaited()
