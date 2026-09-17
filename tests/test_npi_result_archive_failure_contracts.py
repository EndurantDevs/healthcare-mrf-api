# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Failure-path contracts for NPI archive admission and generation authority."""

from __future__ import annotations

import asyncio
import datetime
from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import ANY, AsyncMock
from uuid import uuid4

import pytest

from process import npi_result_archive as archive
from process import npi_result_generation as generation
from process.npi_canonical_publication import (
    NpiCanonicalPublicationInput,
    build_npi_canonical_publication_receipt,
)


def _tables() -> tuple[archive.NpiTableReceipt, ...]:
    return tuple(
        archive.NpiTableReceipt(model.__name__, table_name, "a" * 64, ordinal)
        for ordinal, (model, table_name) in enumerate(
            zip(archive._MODEL_TYPES, generation.RELATION_NAMES, strict=True),
            1,
        )
    )


def _serving(*, revision: int = 2, lineage_id: str | None = None) -> generation.NpiServingGeneration:
    return generation.NpiServingGeneration(
        lineage_id or str(uuid4()),
        revision,
        datetime.datetime(2026, 9, 17, 12, tzinfo=datetime.UTC),
    )


def _provenance() -> generation.NpiCanonicalProvenance:
    return generation.NpiCanonicalProvenance(
        "nppub1_" + "a" * 43,
        4,
        "penpc1_" + "b" * 43,
        datetime.date(2026, 9, 17),
    )


def _manifest(*, tracked: bool = True) -> archive.NpiResultManifest:
    metadata, metadata_sha256 = archive._source_metadata({"release": "synthetic"})
    tables = _tables()
    return archive.NpiResultManifest(
        tables,
        metadata,
        metadata_sha256,
        archive._schema_digest(tables),
        "tracked-generation" if tracked else "legacy-manual",
        _serving() if tracked else None,
        _provenance() if tracked else None,
    )


def _ownership() -> archive.NpiStageOwnership:
    dataset_id = uuid4()
    return archive.NpiStageOwnership(
        dataset_id,
        archive.npi_stage_schema(dataset_id),
        90,
        tuple((name, ordinal) for ordinal, name in enumerate(sorted(generation.RELATION_NAMES), 101)),
        (("npi_id_seq", 201, "npi", "id"),),
        301,
        ((generation.RELATION_NAMES[0], 101, 401),),
        ((generation.RELATION_NAMES[0], 401, "write", "definition"),),
    )


def _validation(
    ownership: archive.NpiStageOwnership,
    manifest: archive.NpiResultManifest,
) -> archive.NpiValidationReceipt:
    receipt_dict = {
        "contract": archive.VALIDATION_CONTRACT,
        "package_id": "c" * 64,
        "profile_contract": archive.CONTRACT,
        "stage_schema": ownership.schema_name,
        "stage_schema_oid": ownership.schema_oid,
        "relation_oids": [list(pair) for pair in ownership.relation_oids],
        "sealed_owner_oid": 501,
        "manifest_sha256": archive._manifest_digest(manifest),
        "tables": [table.as_dict() for table in manifest.tables],
    }
    return archive.validate_npi_validation_receipt(
        {**receipt_dict, "validation_sha256": archive._validation_digest(receipt_dict)}
    )


def _authority(
    *,
    serving: generation.NpiServingGeneration | None = None,
    relation_oids: tuple[int, ...] | None = None,
    local_generation: int = 7,
) -> generation.NpiResultGenerationAuthority:
    return generation.NpiResultGenerationAuthority(
        str(uuid4()),
        local_generation,
        serving,
        relation_oids,
        None,
    )


def _authority_row(
    *,
    local_generation: int = 7,
    serving: generation.NpiServingGeneration | None = None,
    relation_oids: tuple[int, ...] | None = None,
) -> dict[str, object]:
    return {
        "singleton": True,
        "local_lineage_id": str(uuid4()),
        "local_generation": local_generation,
        "origin_lineage_id": None if serving is None else serving.origin_lineage_id,
        "origin_generation": None if serving is None else serving.origin_generation,
        "published_at": None if serving is None else serving.published_at,
        "relation_oids": None if relation_oids is None else list(relation_oids),
        "canonical_publication_ref": None,
        "canonical_publication_generation": None,
        "canonical_chain_ref": None,
        "canonical_import_date": None,
    }


def _publication_receipt(relation_oids: tuple[int, ...]):
    return build_npi_canonical_publication_receipt(
        NpiCanonicalPublicationInput(
            "run_npi_generation",
            "run_npi_generation:" + "a" * 32,
            "2026-09-17T12:00:00.000000+00:00",
            "penpc1_" + "b" * 43,
            "2026-09-17",
            relation_oids,
            (1, 1, 1, 1, 1, 1),
        ),
        publication_generation=1,
        created_at="2026-09-17T12:01:00.000000+00:00",
    )


@pytest.mark.parametrize("builder", [archive.npi_stage_schema, archive.npi_predecessor_schema])
def test_stage_names_reject_non_uuid_owners(builder) -> None:
    with pytest.raises(archive.NpiResultArchiveError, match="UUID dataset_id"):
        builder(str(uuid4()))


@pytest.mark.parametrize("value", ["", "bad-name", "x" * 64])
def test_archive_identifiers_reject_unsafe_names(value: str) -> None:
    with pytest.raises(archive.NpiResultArchiveError, match="schema is invalid"):
        archive._schema_name(value)
    if len(value) < 64:
        with pytest.raises(archive.NpiResultArchiveError, match="identifier is invalid"):
            archive._quoted(value)


@pytest.mark.parametrize("value", [{"not": {1}}, {"nan": float("nan")}])
def test_archive_metadata_rejects_noncanonical_json(value: object) -> None:
    with pytest.raises(archive.NpiResultArchiveError, match="metadata is invalid"):
        archive._canonical_json(value)


def test_archive_transaction_and_timeout_guards_fail_closed() -> None:
    for session in (object(), SimpleNamespace(in_transaction=lambda: False)):
        with pytest.raises(archive.NpiResultArchiveError, match="caller transaction"):
            archive._require_transaction(session)


@pytest.mark.asyncio
async def test_archive_timeout_and_snapshot_values_are_typed() -> None:
    session = SimpleNamespace(scalar=AsyncMock(return_value=None))
    with pytest.raises(archive.NpiResultArchiveError, match="timeout state"):
        await archive._timeout_value(session, "lock_timeout")

    result = SimpleNamespace(scalar_one=lambda: "not/a/snapshot")
    session = SimpleNamespace(execute=AsyncMock(return_value=result))
    with pytest.raises(archive.NpiResultArchiveError, match="snapshot is invalid"):
        await archive._export_stage_snapshot(session)


@pytest.mark.asyncio
async def test_relation_and_table_receipts_reject_missing_or_untyped_catalog_state(monkeypatch) -> None:
    session = SimpleNamespace(scalar=AsyncMock(side_effect=[None, True, -1]))
    assert await archive._relation_oid(session, "mrf", "npi") is None
    with pytest.raises(archive.NpiResultArchiveError, match="relation is unavailable"):
        await archive._relation_oid(session, "mrf", "npi")
    with pytest.raises(archive.NpiResultArchiveError, match="relation is unavailable"):
        await archive._relation_oid(session, "mrf", "npi")

    relation_oid = AsyncMock(return_value=None)
    monkeypatch.setattr(archive, "_relation_oid", relation_oid)
    with pytest.raises(archive.NpiResultArchiveError, match="relation is missing"):
        await archive._table_receipt(session, schema_name="mrf", model_type=archive._MODEL_TYPES[0])

    relation_oid.return_value = 10
    monkeypatch.setattr(archive, "_npi_schema_identity", AsyncMock(side_effect=RuntimeError("catalog drift")))
    with pytest.raises(archive.NpiResultArchiveError, match="schema identity is unavailable"):
        await archive._table_receipt(session, schema_name="mrf", model_type=archive._MODEL_TYPES[0])

    monkeypatch.setattr(archive, "_npi_schema_identity", AsyncMock(return_value="a" * 64))
    session.scalar.side_effect = None
    session.scalar.return_value = True
    with pytest.raises(archive.NpiResultArchiveError, match="row count is invalid"):
        await archive._table_receipt(session, schema_name="mrf", model_type=archive._MODEL_TYPES[0])


@pytest.mark.asyncio
async def test_source_capture_rejects_ambiguous_metadata_before_catalog_work() -> None:
    session = SimpleNamespace(in_transaction=lambda: True, execute=AsyncMock())
    for metadata, factory in ((None, None), ({}, AsyncMock(return_value={}))):
        with pytest.raises(archive.NpiResultArchiveError, match="exactly one source metadata"):
            await archive.capture_npi_source(
                session,
                schema_name="mrf",
                source_metadata=metadata,
                source_metadata_factory=factory,
            )
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["incomplete-family", "incomplete-generation", "drifted", "snapshot"])
async def test_source_capture_rejects_incoherent_family_authority(monkeypatch, failure: str) -> None:
    @asynccontextmanager
    async def bounded(_session):
        yield

    oids = tuple(range(1, 7))
    pairs = tuple(zip(generation.RELATION_NAMES, oids, strict=True))
    authority = _authority(serving=_serving(), relation_oids=oids)
    if failure == "incomplete-family":
        pairs = ((generation.RELATION_NAMES[0], None), *pairs[1:])
        message = "source family is incomplete"
    elif failure == "incomplete-generation":
        authority = _authority(relation_oids=oids)
        message = "source generation is incomplete"
    elif failure == "drifted":
        authority = _authority(serving=_serving(), relation_oids=tuple(range(11, 17)))
        message = "source generation is drifted"
    else:
        message = "source snapshot is invalid"
    monkeypatch.setattr(archive, "_bounded_catalog_work", bounded)
    monkeypatch.setattr(archive, "_lock_family", AsyncMock())
    monkeypatch.setattr(archive, "read_npi_result_generation_authority", AsyncMock(return_value=authority))
    monkeypatch.setattr(archive, "_relation_pairs", AsyncMock(return_value=pairs))
    snapshot_result = SimpleNamespace(scalar_one=lambda: "invalid/snapshot")
    session = SimpleNamespace(
        in_transaction=lambda: True,
        execute=AsyncMock(side_effect=[None, snapshot_result]),
    )
    with pytest.raises(archive.NpiResultArchiveError, match=message):
        await archive.capture_npi_source(
            session,
            schema_name="mrf",
            source_metadata={"release": "synthetic"},
        )


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("contract", "wrong", "authority classification"),
        ("capture_authority", "trusted", "authority classification"),
        ("source_metadata", [], "source metadata"),
        ("tables", [], "table set"),
        ("source_serving_generation", {"origin_lineage_id": "forged"}, "source authority"),
        ("source_metadata_sha256", "0" * 64, "manifest digest"),
        ("schema_sha256", "0" * 64, "manifest digest"),
    ],
)
def test_manifest_rejects_malformed_or_forged_evidence(field: str, value: object, message: str) -> None:
    candidate = _manifest().as_dict()
    candidate[field] = value
    with pytest.raises(archive.NpiResultArchiveError, match=message):
        archive.validate_npi_result_manifest(candidate)


def test_manifest_rejects_open_fields_and_authority_mismatch() -> None:
    candidate = _manifest().as_dict()
    candidate["unreviewed"] = True
    with pytest.raises(archive.NpiResultArchiveError, match="manifest is invalid"):
        archive.validate_npi_result_manifest(candidate)

    candidate = _manifest(tracked=False).as_dict()
    candidate["source_serving_generation"] = _serving().as_dict()
    with pytest.raises(archive.NpiResultArchiveError, match="classification differs"):
        archive.validate_npi_result_manifest(candidate)


@pytest.mark.parametrize(
    "mutate",
    [
        lambda table: table.update(extra=True),
        lambda table: table.update(model_name="wrong"),
        lambda table: table.update(table_name="wrong"),
        lambda table: table.update(schema_sha256="not-sha256"),
        lambda table: table.update(row_count=True),
        lambda table: table.update(row_count=-1),
    ],
)
def test_manifest_table_inventory_is_exact_and_typed(mutate) -> None:
    candidate = _manifest().as_dict()
    mutate(candidate["tables"][0])
    with pytest.raises(archive.NpiResultArchiveError, match="table receipt is invalid"):
        archive.validate_npi_result_manifest(candidate)


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("contract", "wrong", "receipt is invalid"),
        ("package_id", "short", "receipt is invalid"),
        ("stage_schema_oid", True, "receipt is invalid"),
        ("sealed_owner_oid", 0, "receipt is invalid"),
        ("stage_schema", "bad-name", "schema is invalid"),
        ("relation_oids", [], "inventory is invalid"),
        ("validation_sha256", "0" * 64, "digest differs"),
    ],
)
def test_validation_receipt_rejects_unbound_evidence(field: str, value: object, message: str) -> None:
    ownership = _ownership()
    candidate = _validation(ownership, _manifest()).as_dict()
    candidate[field] = value
    with pytest.raises(archive.NpiResultArchiveError, match=message):
        archive.validate_npi_validation_receipt(candidate)


def test_validation_receipt_rejects_non_mapping_input() -> None:
    with pytest.raises(archive.NpiResultArchiveError, match="receipt is invalid"):
        archive.validate_npi_validation_receipt(object())


@pytest.mark.parametrize(
    "mutate",
    [
        lambda pairs: pairs[0].__setitem__(0, "wrong"),
        lambda pairs: pairs[0].__setitem__(1, True),
        lambda pairs: pairs[0].__setitem__(1, 0),
        lambda pairs: pairs.__setitem__(0, ["only-one"]),
        lambda pairs: pairs.__setitem__(0, "not-a-pair"),
        lambda pairs: pairs.pop(),
    ],
)
def test_validation_inventory_rejects_wrong_names_oids_and_shapes(mutate) -> None:
    ownership = _ownership()
    candidate = _validation(ownership, _manifest()).as_dict()
    mutate(candidate["relation_oids"])
    with pytest.raises(archive.NpiResultArchiveError, match="inventory is invalid"):
        archive.validate_npi_validation_receipt(candidate)


@pytest.mark.parametrize(
    "change",
    [
        {"package_id": "d" * 64},
        {"sealed_owner_oid": 502},
        {"stage_schema": "other_stage"},
        {"stage_schema_oid": 91},
        {"relation_oids": (("wrong", 1),)},
        {"manifest_sha256": "d" * 64},
        {"tables": ()},
    ],
)
def test_cutover_binding_mismatch_is_rejected_before_mutation(change: dict[str, object]) -> None:
    ownership = _ownership()
    manifest = _manifest()
    validation = replace(_validation(ownership, manifest), **change)
    cutover = archive.NpiCutoverAuthority("c" * 64, 501, 501, "manual")
    with pytest.raises(archive.NpiResultArchiveError, match="authority differs"):
        archive._validate_cutover_bindings(ownership, manifest, validation, cutover)


@pytest.mark.asyncio
async def test_public_activation_rejects_wrong_binding_before_mutation_or_callback(monkeypatch) -> None:
    ownership = _ownership()
    manifest = _manifest()
    validation = _validation(ownership, manifest)
    activate = AsyncMock()
    callback = AsyncMock()
    monkeypatch.setattr(archive, "_activate_npi_relations", activate)
    session = SimpleNamespace(in_transaction=lambda: True)

    with pytest.raises(archive.NpiResultArchiveError, match="authority differs"):
        await archive.activate_validated_npi_stage(
            session,
            ownership=ownership,
            manifest=manifest,
            incumbent=archive.NpiIncumbent("mrf", tuple((name, None) for name in generation.RELATION_NAMES)),
            validation_receipt=validation,
            cutover=archive.NpiCutoverAuthority("d" * 64, 501, 501, "manual"),
            on_activated=callback,
        )

    activate.assert_not_awaited()
    callback.assert_not_awaited()


@pytest.mark.asyncio
async def test_automatic_cutover_rejects_missing_legacy_and_drifted_authority(monkeypatch) -> None:
    incumbent = archive.NpiIncumbent("mrf", tuple((name, oid) for oid, name in enumerate(generation.RELATION_NAMES, 1)))
    authority = _authority()
    with pytest.raises(archive.NpiResultArchiveError, match="source generation is unavailable"):
        await archive._admit_automatic_cutover(
            object(), incumbent=incumbent, current_authority=authority, source_generation=None
        )

    monkeypatch.setattr(archive, "_has_populated_incumbent", AsyncMock(return_value=True))
    with pytest.raises(archive.NpiResultArchiveError, match="legacy incumbent requires manual"):
        await archive._admit_automatic_cutover(
            object(), incumbent=incumbent, current_authority=authority, source_generation=_serving()
        )

    tracked = _authority(serving=_serving(), relation_oids=(10, 11, 12, 13, 14, 15))
    with pytest.raises(archive.NpiResultArchiveError, match="incumbent generation is drifted"):
        await archive._admit_automatic_cutover(
            object(), incumbent=incumbent, current_authority=tracked, source_generation=_serving()
        )


@pytest.mark.asyncio
async def test_automatic_cutover_accepts_empty_first_copy_and_strict_successor(monkeypatch) -> None:
    empty = archive.NpiIncumbent("mrf", tuple((name, None) for name in generation.RELATION_NAMES))
    populated = AsyncMock(return_value=False)
    monkeypatch.setattr(archive, "_has_populated_incumbent", populated)
    await archive._admit_automatic_cutover(
        object(), incumbent=empty, current_authority=_authority(), source_generation=_serving()
    )
    populated.assert_awaited_once()

    lineage = str(uuid4())
    incumbent = archive.NpiIncumbent("mrf", tuple((name, oid) for oid, name in enumerate(generation.RELATION_NAMES, 1)))
    await archive._admit_automatic_cutover(
        object(),
        incumbent=incumbent,
        current_authority=_authority(
            serving=_serving(revision=2, lineage_id=lineage), relation_oids=tuple(range(1, 7))
        ),
        source_generation=_serving(revision=3, lineage_id=lineage),
    )


@pytest.mark.asyncio
async def test_automatic_cutover_wraps_stale_or_foreign_generation() -> None:
    lineage = str(uuid4())
    incumbent = archive.NpiIncumbent("mrf", tuple((name, oid) for oid, name in enumerate(generation.RELATION_NAMES, 1)))
    authority = _authority(serving=_serving(revision=2, lineage_id=lineage), relation_oids=tuple(range(1, 7)))
    for candidate in (_serving(revision=2, lineage_id=lineage), _serving(revision=3)):
        with pytest.raises(archive.NpiResultArchiveError, match="stale or unrelated"):
            await archive._admit_automatic_cutover(
                object(), incumbent=incumbent, current_authority=authority, source_generation=candidate
            )


@pytest.mark.asyncio
async def test_populated_incumbent_short_circuits_and_empty_family_does_not_query() -> None:
    empty = archive.NpiIncumbent("mrf", tuple((name, None) for name in generation.RELATION_NAMES))
    session = SimpleNamespace(scalar=AsyncMock())
    assert await archive._has_populated_incumbent(session, empty) is False
    session.scalar.assert_not_awaited()

    present = archive.NpiIncumbent("mrf", tuple((name, oid) for oid, name in enumerate(generation.RELATION_NAMES, 1)))
    session.scalar.side_effect = [False, True]
    assert await archive._has_populated_incumbent(session, present) is True
    assert session.scalar.await_count == 2

    session.scalar.reset_mock()
    session.scalar.side_effect = None
    session.scalar.return_value = False
    assert await archive._has_populated_incumbent(session, present) is False
    assert session.scalar.await_count == len(generation.RELATION_NAMES)


@pytest.mark.asyncio
async def test_activation_preparation_rejects_invalid_package_before_owner_checks(monkeypatch) -> None:
    owner_check = AsyncMock()
    monkeypatch.setattr(archive, "verify_npi_stage_ownership", owner_check)
    session = SimpleNamespace(in_transaction=lambda: True)
    with pytest.raises(archive.NpiResultArchiveError, match="package identity is invalid"):
        await archive.prepare_npi_activation(
            session,
            ownership=_ownership(),
            manifest=_manifest(),
            package_id="not-a-package",
            sealed_owner_oid=501,
        )
    owner_check.assert_not_awaited()


@pytest.mark.asyncio
async def test_sequence_copy_preserves_existing_owner_and_rejects_drift(monkeypatch) -> None:
    session = SimpleNamespace(execute=AsyncMock(), scalar=AsyncMock(return_value=None))
    assert (
        await archive._ensure_stage_owned_sequence(
            session,
            stage_schema="stage",
            source_sequence_name="npi_id_seq",
            owner_table="npi",
            owner_column="id",
            stage_sequences_by_owner={("npi", "id"): ("existing_seq", 10)},
        )
        == "existing_seq"
    )
    session.execute.assert_not_awaited()
    assert (
        await archive._advance_stage_sequence(
            session,
            stage_schema="stage",
            sequence_name="existing_seq",
            owner_table="npi",
            owner_column="id",
        )
        is None
    )

    ownership = _ownership()
    monkeypatch.setattr(archive, "_owned_sequences", AsyncMock(return_value=()))
    with pytest.raises(archive.NpiResultArchiveError, match="sequence ownership differs"):
        await archive._advance_and_verify_stage_sequences(session, ownership)
    with pytest.raises(archive.NpiResultArchiveError, match="sequence ownership differs"):
        await archive._verify_stage_sequence_owners(
            session,
            ownership.schema_oid,
            (("npi_id_seq", 1, "npi", "id"),),
        )


@pytest.mark.asyncio
async def test_sequence_state_rejects_missing_minimum_and_post_setval_drift(monkeypatch) -> None:
    ownership = _ownership()
    session = SimpleNamespace(scalar=AsyncMock(return_value=None), execute=AsyncMock())
    monkeypatch.setattr(archive, "_owned_sequences", AsyncMock(return_value=ownership.sequence_oids))
    monkeypatch.setattr(archive, "_advance_stage_sequence", AsyncMock(return_value=None))
    with pytest.raises(archive.NpiResultArchiveError, match="state is unavailable"):
        await archive._advance_and_verify_stage_sequences(session, ownership)

    session.scalar.return_value = 1
    state = SimpleNamespace(one=lambda: (2, False))
    session.execute.return_value = state
    with pytest.raises(archive.NpiResultArchiveError, match="state differs"):
        await archive._advance_and_verify_stage_sequences(session, ownership)


@pytest.mark.asyncio
async def test_clone_and_schema_identity_reject_invalid_catalog_tokens() -> None:
    capture = archive.NpiSourceCapture({}, "a" * 64, "legacy-manual", None, None, "mrf", "invalid/snapshot")
    session = SimpleNamespace(execute=AsyncMock(), scalar=AsyncMock(return_value=True))
    with pytest.raises(archive.NpiResultArchiveError, match="snapshot is invalid"):
        await archive._clone_source(session, capture, "stage")
    session.execute.assert_not_awaited()
    with pytest.raises(archive.NpiResultArchiveError, match="owned schema is unavailable"):
        await archive._schema_oid(session, "stage")


def test_freeze_seals_reject_mutable_function_and_incomplete_triggers() -> None:
    function_row_dict = {
        "oid": 1,
        "lanname": "sql",
        "prosecdef": True,
        "prosrc": archive._FREEZE_FUNCTION_BODY,
        "proconfig": ["search_path=pg_catalog"],
    }
    with pytest.raises(archive.NpiResultArchiveError, match="freeze function differs"):
        archive._validate_freeze_function(function_row_dict)

    with pytest.raises(archive.NpiResultArchiveError, match="trigger set differs"):
        archive._validate_freeze_triggers([], 1)
    trigger_row_dict = {
        "table_name": generation.RELATION_NAMES[0],
        "tgname": archive._FREEZE_WRITE_TRIGGER,
        "tgenabled": "D",
        "tgtype": 30,
        "tgfoid": 1,
        "unconditional": True,
        "tgnargs": 0,
        "trigger_columns": "",
        "oid": 2,
    }
    with pytest.raises(archive.NpiResultArchiveError, match="freeze trigger differs"):
        archive._validate_freeze_triggers([trigger_row_dict], 1)


@pytest.mark.asyncio
async def test_freeze_seal_rejects_orphan_triggers(monkeypatch) -> None:
    monkeypatch.setattr(archive, "_read_freeze_function", AsyncMock(return_value=None))
    monkeypatch.setattr(archive, "_read_freeze_triggers", AsyncMock(return_value=[{"oid": 1}]))
    with pytest.raises(archive.NpiResultArchiveError, match="freeze function differs"):
        await archive._freeze_seal(object(), 90)


@pytest.mark.asyncio
async def test_stage_guard_entry_points_reject_invalid_or_unfrozen_tokens(monkeypatch) -> None:
    session = SimpleNamespace(in_transaction=lambda: True)
    with pytest.raises(archive.NpiResultArchiveError, match="ownership is invalid"):
        await archive.freeze_npi_stage(session, ownership=object())
    with pytest.raises(archive.NpiResultArchiveError, match="ownership is invalid"):
        await archive.verify_npi_stage_ownership(session, object())

    ownership = replace(_ownership(), freeze_function_oid=None, freeze_trigger_oids=(), freeze_catalog_versions=())
    with pytest.raises(archive.NpiResultArchiveError, match="not frozen"):
        await archive.export_prepared_npi_archive(
            object(),
            prepared=archive.NpiPreparedSource(_manifest(), ownership),
            archive_copy=AsyncMock(),
        )
    with pytest.raises(archive.NpiResultArchiveError, match="prepared source is invalid"):
        await archive.export_prepared_npi_archive(object(), prepared=object(), archive_copy=AsyncMock())

    verify = AsyncMock()
    monkeypatch.setattr(archive, "verify_npi_stage_ownership", verify)
    with pytest.raises(archive.NpiResultArchiveError, match="callback is required"):
        await archive.prepare_npi_archive_source(
            object(),
            schema_name="mrf",
            source_metadata={},
            dataset_id=uuid4(),
            on_prepared=None,
        )
    verify.assert_not_awaited()


@pytest.mark.asyncio
async def test_stage_owner_rejects_invalid_schema_and_relation_owners() -> None:
    ownership = _ownership()
    session = SimpleNamespace(scalar=AsyncMock(return_value=501), execute=AsyncMock())
    with pytest.raises(archive.NpiResultArchiveError, match="owner is invalid"):
        await archive._verify_stage_owner(session, ownership, True)
    session.scalar.assert_not_awaited()

    session.scalar.return_value = 999
    with pytest.raises(archive.NpiResultArchiveError, match="owner differs"):
        await archive._verify_stage_owner(session, ownership, 501)
    session.execute.assert_not_awaited()

    session.scalar.return_value = 501
    rows_result = SimpleNamespace(mappings=lambda: [])
    session.execute.return_value = rows_result
    with pytest.raises(archive.NpiResultArchiveError, match="owner differs"):
        await archive._verify_stage_owner(session, ownership, 501)


@pytest.mark.asyncio
async def test_incumbent_capture_rejects_partial_and_changed_families(monkeypatch) -> None:
    @asynccontextmanager
    async def bounded(_session):
        yield

    monkeypatch.setattr(archive, "_bounded_catalog_work", bounded)
    monkeypatch.setattr(archive, "_lock_family", AsyncMock())
    session = SimpleNamespace(in_transaction=lambda: True)
    partial_pairs = tuple(
        (name, None if ordinal == 0 else ordinal) for ordinal, name in enumerate(generation.RELATION_NAMES)
    )
    relation_pairs = AsyncMock(return_value=partial_pairs)
    monkeypatch.setattr(archive, "_relation_pairs", relation_pairs)
    with pytest.raises(archive.NpiResultArchiveError, match="incumbent is incomplete"):
        await archive.capture_npi_incumbent(session, schema_name="mrf")

    complete_pairs = tuple((name, ordinal) for ordinal, name in enumerate(generation.RELATION_NAMES, 1))
    relation_pairs.side_effect = [
        complete_pairs,
        tuple((name, ordinal + 10) for name, ordinal in complete_pairs),
    ]
    with pytest.raises(archive.NpiResultArchiveError, match="changed during capture"):
        await archive.capture_npi_incumbent(session, schema_name="mrf")


@pytest.mark.asyncio
async def test_activation_entry_rejects_untyped_authority_and_missing_callback() -> None:
    ownership = _ownership()
    incumbent = archive.NpiIncumbent("mrf", tuple((name, None) for name in generation.RELATION_NAMES))
    session = SimpleNamespace(in_transaction=lambda: True)
    for activation_ownership, cutover, callback, message in (
        (object(), archive.NpiCutoverAuthority("c" * 64, 501, 501, "manual"), AsyncMock(), "ownership"),
        (ownership, object(), AsyncMock(), "authority is unsupported"),
        (
            ownership,
            archive.NpiCutoverAuthority("c" * 64, 501, 501, "manual"),
            None,
            "callback is required",
        ),
    ):
        with pytest.raises(archive.NpiResultArchiveError, match=message):
            await archive.activate_validated_npi_stage(
                session,
                ownership=activation_ownership,
                manifest=_manifest(),
                incumbent=incumbent,
                validation_receipt={},
                cutover=cutover,
                on_activated=callback,
            )


def test_generation_scalar_validators_reject_ambiguous_values() -> None:
    invalid_calls = (
        lambda: generation._schema_name("bad-name"),
        lambda: generation._identifier(1, field_name="stage table"),
        lambda: generation._uuid_text(object()),
        lambda: generation._timestamp("not-a-time"),
        lambda: generation._timestamp(datetime.datetime(2026, 9, 17)),
        lambda: generation._generation(True),
        lambda: generation._generation(0),
        lambda: generation._canonical_generation(1 << 53),
        lambda: generation._relation_oids([1, 2]),
        lambda: generation._relation_oids([1, 2, 3, 4, 5, 5]),
    )
    for call in invalid_calls:
        with pytest.raises(ValueError):
            call()


def test_generation_structures_reject_open_or_malformed_values() -> None:
    with pytest.raises(ValueError, match="serving generation is invalid"):
        generation.validate_npi_serving_generation({"origin_lineage_id": str(uuid4())})
    with pytest.raises(ValueError, match="canonical provenance is invalid"):
        generation.validate_npi_canonical_provenance(_provenance().as_dict() | {"extra": True})
    bad_date = _provenance().as_dict() | {"import_date": "not-a-date"}
    with pytest.raises(ValueError, match="canonical provenance is invalid"):
        generation.validate_npi_canonical_provenance(bad_date)
    bad_reference = _provenance().as_dict() | {"publication_ref": "forged"}
    with pytest.raises(ValueError, match="canonical provenance is invalid"):
        generation.validate_npi_canonical_provenance(bad_reference)
    with pytest.raises(RuntimeError, match="authority is unavailable"):
        generation._row_mapping(object())
    assert generation.validate_npi_canonical_provenance(_provenance()) == _provenance()


def test_generation_authority_rejects_missing_singleton_and_local_counter() -> None:
    with pytest.raises(RuntimeError, match="authority is unavailable"):
        generation.validate_npi_result_generation_authority({"singleton": False})
    for local_generation in (True, -1, generation._MAX_GENERATION + 1):
        with pytest.raises(RuntimeError, match="local result generation is invalid"):
            generation.validate_npi_result_generation_authority(
                {"singleton": True, "local_generation": local_generation}
            )


@pytest.mark.asyncio
async def test_generation_reads_reject_absent_and_malformed_relation_rows() -> None:
    empty = SimpleNamespace(mappings=lambda: SimpleNamespace(one_or_none=lambda: None))
    session = SimpleNamespace(execute=AsyncMock(return_value=empty))
    with pytest.raises(RuntimeError, match="authority is unavailable"):
        await generation.read_npi_result_generation_authority(session, schema_name="mrf")

    for rows in ([("npi", None)], [(name, oid) for oid, name in enumerate(reversed(generation.RELATION_NAMES), 1)]):
        result = SimpleNamespace(all=lambda rows=rows: rows)
        session.execute.return_value = result
        with pytest.raises(RuntimeError, match="serving relations are unavailable"):
            await generation.current_npi_relation_oids(session, schema_name="mrf")


@pytest.mark.asyncio
async def test_capture_generation_rejects_legacy_and_oid_drift(monkeypatch) -> None:
    read = AsyncMock(return_value=_authority())
    current_oids = AsyncMock(return_value=tuple(range(1, 7)))
    monkeypatch.setattr(generation, "read_npi_result_generation_authority", read)
    monkeypatch.setattr(generation, "current_npi_relation_oids", current_oids)
    with pytest.raises(RuntimeError, match="unavailable or drifted"):
        await generation.capture_npi_serving_generation(object(), schema_name="mrf")

    matching = _authority(serving=_serving(), relation_oids=tuple(range(1, 7)))
    read.return_value = matching
    assert await generation.capture_npi_serving_generation(object(), schema_name="mrf") == matching

    read.return_value = _authority(serving=_serving(), relation_oids=tuple(range(11, 17)))
    with pytest.raises(RuntimeError, match="unavailable or drifted"):
        await generation.capture_npi_serving_generation(object(), schema_name="mrf")


@pytest.mark.asyncio
async def test_matching_provenance_returns_none_without_a_sealed_receipt() -> None:
    result = SimpleNamespace(mappings=lambda: SimpleNamespace(one_or_none=lambda: None))
    session = SimpleNamespace(execute=AsyncMock(return_value=result))
    assert (
        await generation._matching_canonical_provenance(
            session,
            schema_name="mrf",
            relation_oids=tuple(range(1, 7)),
        )
        is None
    )

    provenance = _provenance()
    receipt_dict = {
        "publication_ref": provenance.publication_ref,
        "publication_generation": provenance.publication_generation,
        "chain_ref": provenance.chain_ref,
        "import_date": provenance.import_date,
    }
    result.mappings = lambda: SimpleNamespace(one_or_none=lambda: receipt_dict)
    assert (
        await generation._matching_canonical_provenance(
            session,
            schema_name="mrf",
            relation_oids=tuple(range(1, 7)),
        )
        == provenance
    )


@pytest.mark.asyncio
async def test_bootstrap_rejects_drift_and_exhaustion_without_writing(monkeypatch) -> None:
    session = SimpleNamespace(in_transaction=lambda: True, execute=AsyncMock())

    @asynccontextmanager
    async def bounded(_session):
        yield

    monkeypatch.setattr(generation, "_bounded_bootstrap", bounded)
    monkeypatch.setattr(generation, "current_npi_relation_oids", AsyncMock(return_value=tuple(range(1, 7))))
    read = AsyncMock(return_value=_authority(serving=_serving(), relation_oids=tuple(range(11, 17))))
    monkeypatch.setattr(generation, "read_npi_result_generation_authority", read)
    write = AsyncMock()
    monkeypatch.setattr(generation, "_write_bootstrap_authority", write)
    with pytest.raises(RuntimeError, match="drifted"):
        await generation.bootstrap_npi_result_generation(session, schema_name="mrf")
    write.assert_not_awaited()

    read.return_value = _authority(local_generation=generation._MAX_GENERATION)
    with pytest.raises(RuntimeError, match="exhausted"):
        await generation.bootstrap_npi_result_generation(session, schema_name="mrf")
    write.assert_not_awaited()


@pytest.mark.asyncio
async def test_bootstrap_requires_transaction_before_locking() -> None:
    session = SimpleNamespace(in_transaction=lambda: False, execute=AsyncMock())
    with pytest.raises(ValueError, match="caller transaction"):
        await generation.bootstrap_npi_result_generation(session, schema_name="mrf")
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_bootstrap_returns_matching_tracked_authority_without_write(monkeypatch) -> None:
    session = SimpleNamespace(in_transaction=lambda: True, execute=AsyncMock())

    @asynccontextmanager
    async def bounded(_session):
        yield

    oids = tuple(range(1, 7))
    current = _authority(serving=_serving(), relation_oids=oids)
    monkeypatch.setattr(generation, "_bounded_bootstrap", bounded)
    monkeypatch.setattr(generation, "current_npi_relation_oids", AsyncMock(return_value=oids))
    monkeypatch.setattr(generation, "read_npi_result_generation_authority", AsyncMock(return_value=current))
    write = AsyncMock()
    monkeypatch.setattr(generation, "_write_bootstrap_authority", write)
    assert await generation.bootstrap_npi_result_generation(session, schema_name="mrf") == current
    write.assert_not_awaited()


@pytest.mark.asyncio
async def test_guard_installation_rejects_incomplete_family_before_ddl() -> None:
    connection = SimpleNamespace(execute=AsyncMock())
    with pytest.raises(ValueError, match="stage family is invalid"):
        await generation.install_npi_stage_mutation_guards(
            connection,
            schema_name="mrf",
            stage_tables=("npi",),
        )
    connection.execute.assert_not_awaited()
    with pytest.raises(ValueError, match="stage family is invalid"):
        generation._guard_statements("mrf", ("npi",))


@pytest.mark.asyncio
async def test_generation_timeout_and_asyncpg_ddl_paths_fail_closed() -> None:
    session = SimpleNamespace(scalar=AsyncMock(return_value=None))
    with pytest.raises(RuntimeError, match="timeout state is unavailable"):
        await generation._timeout_value(session, "lock_timeout")

    connection = SimpleNamespace(fetchrow=AsyncMock(), execute=AsyncMock())
    await generation._execute_ddl(connection, "SELECT 1")
    connection.execute.assert_awaited_once_with("SELECT 1")


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["missing-authority", "exhausted", "missing-family", "different-family"])
async def test_local_publication_rejects_unbound_state_before_update(failure: str) -> None:
    oids = tuple(range(101, 107))
    receipt = _publication_receipt(oids)
    current = _authority_row()
    live_oids_by_field = {f"relation_{ordinal}": oid for ordinal, oid in enumerate(oids, 1)}
    if failure == "missing-authority":
        responses = [None]
        message = "authority is unavailable"
    elif failure == "exhausted":
        responses = [_authority_row(local_generation=generation._MAX_GENERATION)]
        message = "generation is exhausted"
    elif failure == "missing-family":
        responses = [current, None]
        message = "relations are unavailable"
    else:
        responses = [current, {**live_oids_by_field, "relation_6": 999}]
        message = "relation identity differs"
    connection = SimpleNamespace(fetchrow=AsyncMock(side_effect=responses))
    with pytest.raises(RuntimeError, match=message):
        await generation.publish_local_npi_result_generation(
            connection,
            schema_name="mrf",
            receipt=receipt,
        )
    assert connection.fetchrow.await_count == len(responses)
    assert all(not str(call.args[0]).lstrip().startswith("UPDATE") for call in connection.fetchrow.await_args_list)


@pytest.mark.asyncio
async def test_local_publication_rejects_missing_update_returning() -> None:
    oids = tuple(range(101, 107))
    live_oids_by_field = {f"relation_{ordinal}": oid for ordinal, oid in enumerate(oids, 1)}
    connection = SimpleNamespace(fetchrow=AsyncMock(side_effect=[_authority_row(), live_oids_by_field, None]))
    with pytest.raises(RuntimeError, match="authority is unavailable"):
        await generation.publish_local_npi_result_generation(
            connection,
            schema_name="mrf",
            receipt=_publication_receipt(oids),
        )
    assert connection.fetchrow.await_count == 3


@pytest.mark.asyncio
async def test_bootstrap_write_rejects_missing_update_returning() -> None:
    result = SimpleNamespace(mappings=lambda: SimpleNamespace(one_or_none=lambda: None))
    session = SimpleNamespace(execute=AsyncMock(return_value=result))
    with pytest.raises(RuntimeError, match="authority is unavailable"):
        await generation._write_bootstrap_authority(
            session,
            schema_name="mrf",
            next_generation=1,
            relation_oids=tuple(range(1, 7)),
            provenance=None,
        )


@pytest.mark.asyncio
async def test_adopted_generation_rejects_missing_or_changed_local_authority(monkeypatch) -> None:
    current = _authority()
    monkeypatch.setattr(generation, "read_npi_result_generation_authority", AsyncMock(return_value=current))
    monkeypatch.setattr(
        generation,
        "_adopted_generation_parameters",
        AsyncMock(
            return_value={
                "origin_lineage_id": None,
                "origin_generation": None,
                "published_at": None,
                "relation_oids": None,
            }
        ),
    )
    missing_update_result = SimpleNamespace(mappings=lambda: SimpleNamespace(one_or_none=lambda: None))
    session = SimpleNamespace(execute=AsyncMock(return_value=missing_update_result))
    with pytest.raises(RuntimeError, match="authority is unavailable"):
        await generation.publish_adopted_npi_result_generation(
            session,
            schema_name="mrf",
            source_generation=None,
            canonical_provenance=None,
        )

    changed_authority = _authority_row(local_generation=current.local_generation + 1)
    changed_authority["local_lineage_id"] = current.local_lineage_id
    changed_update_result = SimpleNamespace(mappings=lambda: SimpleNamespace(one_or_none=lambda: changed_authority))
    session.execute.return_value = changed_update_result
    with pytest.raises(RuntimeError, match="changed during adoption"):
        await generation.publish_adopted_npi_result_generation(
            session,
            schema_name="mrf",
            source_generation=None,
            canonical_provenance=None,
        )


@pytest.mark.parametrize(
    "index_spec",
    [
        {"unknown": True, "index_elements": ["id"]},
        {"index_elements": []},
        {"index_elements": ["id"], "name": "bad-name"},
        {"index_elements": ["id"], "using": "unsupported"},
        {"index_elements": ["id"], "include": "id"},
        {"index_elements": ["id"], "include": ["bad-name"]},
    ],
)
def test_model_index_rejects_unbounded_or_invalid_declarations(index_spec: dict[str, object]) -> None:
    with pytest.raises(archive.NpiResultArchiveError, match="index"):
        archive._additional_index_sql("mrf", archive._MODEL_TYPES[0], index_spec)


def test_model_index_normalizes_postgis_and_preserves_bounded_options() -> None:
    statement = archive._additional_index_sql(
        "mrf",
        archive._MODEL_TYPES[0],
        {
            "index_elements": ["Geography(ST_MakePoint(longitude, latitude))"],
            "name": "location",
            "using": "gist",
            "unique": True,
            "include": ["npi"],
            "where": "longitude IS NOT NULL",
        },
    )
    assert "public.Geography(public.ST_MakePoint" in statement
    assert "CREATE UNIQUE INDEX" in statement
    assert ' INCLUDE ("npi")' in statement
    assert " WHERE longitude IS NOT NULL" in statement
    assert archive._uses_postgis_index({"index_elements": ["geography(point)"]}) is True
    assert archive._uses_postgis_index({"index_elements": ["npi"]}) is False


@pytest.mark.asyncio
async def test_cleanup_skips_absent_stage_and_rejects_changed_owner_before_drop(monkeypatch) -> None:
    ownership = _ownership()
    session = SimpleNamespace(in_transaction=lambda: True, scalar=AsyncMock(return_value=None), execute=AsyncMock())
    await archive.cleanup_npi_stage(session, ownership)
    session.execute.assert_not_awaited()

    session.scalar.return_value = ownership.schema_oid

    @asynccontextmanager
    async def bounded(_session):
        yield

    monkeypatch.setattr(archive, "_bounded_catalog_work", bounded)
    monkeypatch.setattr(archive, "_lock_family", AsyncMock())
    monkeypatch.setattr(
        archive,
        "verify_npi_stage_ownership",
        AsyncMock(side_effect=archive.NpiResultArchiveError("NPI archive stage ownership differs")),
    )
    with pytest.raises(archive.NpiResultArchiveError, match="ownership differs"):
        await archive.cleanup_npi_stage(session, ownership)
    assert all("DROP" not in str(call.args[0]) for call in session.execute.await_args_list)


@pytest.mark.asyncio
async def test_export_copy_failure_still_cleans_exact_prepared_owner(monkeypatch) -> None:
    ownership = _ownership()
    prepared = archive.NpiPreparedSource(_manifest(), ownership)
    monkeypatch.setattr(archive, "prepare_npi_archive_source", AsyncMock(return_value=prepared))
    monkeypatch.setattr(
        archive,
        "export_prepared_npi_archive",
        AsyncMock(side_effect=RuntimeError("copy failed")),
    )
    cleanup = AsyncMock()
    monkeypatch.setattr(archive, "_shielded_cleanup", cleanup)
    with pytest.raises(RuntimeError, match="copy failed"):
        await archive.export_npi_archive(
            object(),
            schema_name="mrf",
            source_metadata={"release": "synthetic"},
            dataset_id=ownership.dataset_id,
            archive_copy=AsyncMock(),
        )
    cleanup.assert_awaited_once_with(ANY, ownership)


@pytest.mark.asyncio
async def test_shielded_cleanup_finishes_once_before_forwarding_cancellation(monkeypatch) -> None:
    ownership = _ownership()
    cleaned = asyncio.Event()

    class SessionContext:
        async def __aenter__(self):
            return SimpleNamespace(begin=lambda: self)

        async def __aexit__(self, *_args):
            return False

    cleaned_ownerships = []

    async def cleanup(_session, exact_ownership):
        cleaned_ownerships.append(exact_ownership)
        assert exact_ownership == ownership
        cleaned.set()

    monkeypatch.setattr(archive, "cleanup_npi_stage", cleanup)
    task = asyncio.create_task(archive._shielded_cleanup(lambda: SessionContext(), ownership))
    await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert cleaned.is_set()
    assert cleaned_ownerships == [ownership]
