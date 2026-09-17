# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import datetime
from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from process import npi_result_archive as archive
from process import npi_result_generation as generation


class _Result:
    def __init__(self, value=None):
        self.value = value

    def mappings(self):
        return self

    def one_or_none(self):
        return self.value

    def scalar_one(self):
        return self.value

    def one(self):
        return self.value

    def __iter__(self):
        return iter(self.value or ())


class _AsyncContext:
    def __init__(self, value):
        self.value = value

    async def __aenter__(self):
        return self.value

    async def __aexit__(self, *_args):
        return False


def _session(**overrides):
    session_by_field = {
        "in_transaction": lambda: True,
        "execute": AsyncMock(),
        "scalar": AsyncMock(),
    }
    session_by_field.update(overrides)
    return SimpleNamespace(**session_by_field)


def _tables(row_count=0):
    return tuple(
        archive.NpiTableReceipt(model.__name__, model.__tablename__, "a" * 64, row_count)
        for model in archive._MODEL_TYPES
    )


def _manifest(*, tracked=False):
    metadata, metadata_sha256 = archive._source_metadata({"release": "synthetic"})
    tables = _tables()
    serving = None
    if tracked:
        serving = generation.NpiServingGeneration(
            str(uuid4()),
            3,
            datetime.datetime(2026, 9, 14, 12, tzinfo=datetime.UTC),
        )
    return archive.NpiResultManifest(
        tables,
        metadata,
        metadata_sha256,
        archive._schema_digest(tables),
        "tracked-generation" if tracked else "legacy-manual",
        serving,
        None,
    )


def _legacy_authority():
    authority_by_field = {
        "singleton": True,
        "local_lineage_id": str(uuid4()),
        "local_generation": 4,
        "origin_lineage_id": None,
        "origin_generation": None,
        "published_at": None,
        "relation_oids": None,
        "canonical_publication_ref": None,
        "canonical_publication_generation": None,
        "canonical_chain_ref": None,
        "canonical_import_date": None,
    }
    return generation.validate_npi_result_generation_authority(authority_by_field)


def _ownership(*, frozen=False, sequences=()):
    dataset_id = uuid4()
    relations = tuple((name, ordinal) for ordinal, name in enumerate(sorted(generation.RELATION_NAMES), 10))
    return archive.NpiStageOwnership(
        dataset_id,
        archive.npi_stage_schema(dataset_id),
        9,
        relations,
        sequences,
        70 if frozen else None,
        ((generation.RELATION_NAMES[0], 71, 72),) if frozen else (),
        (("function", 70, "1", "(0,1)"),) if frozen else (),
    )


def _validation(manifest, ownership, *, owner=50, package_id="b" * 64):
    receipt_by_field = {
        "contract": archive.VALIDATION_CONTRACT,
        "package_id": package_id,
        "profile_contract": archive.CONTRACT,
        "stage_schema": ownership.schema_name,
        "stage_schema_oid": ownership.schema_oid,
        "relation_oids": [list(pair) for pair in ownership.relation_oids],
        "sealed_owner_oid": owner,
        "manifest_sha256": archive._manifest_digest(manifest),
        "tables": [table.as_dict() for table in manifest.tables],
    }
    return archive.validate_npi_validation_receipt(
        {**receipt_by_field, "validation_sha256": archive._validation_digest(receipt_by_field)}
    )


@pytest.mark.parametrize(
    ("call", "message"),
    [
        (lambda: archive.npi_stage_schema("bad"), "UUID dataset_id"),
        (lambda: archive.npi_predecessor_schema("bad"), "UUID dataset_id"),
        (lambda: archive._schema_name("bad-name"), "schema is invalid"),
        (lambda: archive._quoted("bad-name"), "identifier is invalid"),
        (lambda: archive._canonical_json({"bad": {1}}), "metadata is invalid"),
        (lambda: archive._source_metadata([]), "source metadata is invalid"),
        (lambda: archive._require_transaction(object()), "caller transaction"),
    ],
)
def test_archive_input_guards(call, message) -> None:
    with pytest.raises(archive.NpiResultArchiveError, match=message):
        call()


@pytest.mark.asyncio
async def test_archive_timeout_and_snapshot_guards() -> None:
    session = _session(
        scalar=AsyncMock(return_value=None),
        execute=AsyncMock(return_value=_Result("invalid snapshot")),
    )
    with pytest.raises(archive.NpiResultArchiveError, match="timeout state"):
        await archive._timeout_value(session, "lock_timeout")
    with pytest.raises(archive.NpiResultArchiveError, match="snapshot is invalid"):
        await archive._export_stage_snapshot(session)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("value", "expected", "message"),
    [(None, None, None), ("bad", None, "relation is unavailable"), (12, 12, None)],
)
async def test_relation_oid_contract(value, expected, message) -> None:
    session = _session(scalar=AsyncMock(return_value=value))
    if message:
        with pytest.raises(archive.NpiResultArchiveError, match=message):
            await archive._relation_oid(session, "mrf", "npi")
    else:
        assert await archive._relation_oid(session, "mrf", "npi") == expected


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["missing", "identity", "count"])
async def test_table_receipt_rejects_incomplete_evidence(monkeypatch, failure) -> None:
    relation_oid = None if failure == "missing" else 12
    monkeypatch.setattr(archive, "_relation_oid", AsyncMock(return_value=relation_oid))
    if failure == "identity":
        monkeypatch.setattr(archive, "_npi_schema_identity", AsyncMock(side_effect=ValueError("bad")))
    else:
        monkeypatch.setattr(archive, "_npi_schema_identity", AsyncMock(return_value="a" * 64))
    session = _session(scalar=AsyncMock(return_value="bad" if failure == "count" else 0))
    message_by_failure = {
        "missing": "relation is missing",
        "identity": "schema identity is unavailable",
        "count": "row count is invalid",
    }
    with pytest.raises(archive.NpiResultArchiveError, match=message_by_failure[failure]):
        await archive._table_receipt(
            session,
            schema_name="mrf",
            model_type=archive._MODEL_TYPES[0],
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["default", "missing-column"])
async def test_schema_identity_rejects_sequence_drift(monkeypatch, failure) -> None:
    columns = [{"attname": "id", "default_expression": "invalid" if failure == "default" else None}]
    if failure == "missing-column":
        columns[0]["attname"] = "other"
    monkeypatch.setattr(archive.catalog_identity, "_catalog_columns", AsyncMock(return_value=columns))
    monkeypatch.setattr(archive.catalog_identity, "_catalog_constraints", AsyncMock(return_value=[]))
    monkeypatch.setattr(archive.catalog_identity, "_catalog_indexes", AsyncMock(return_value=[]))
    monkeypatch.setattr(archive, "_schema_oid", AsyncMock(return_value=9))
    monkeypatch.setattr(
        archive,
        "_owned_sequences",
        AsyncMock(return_value=(("seq", 20, "npi", "id"),)),
    )
    with pytest.raises(archive.NpiResultArchiveError, match="owned sequence"):
        await archive._npi_schema_identity(_session(), 10, "mrf", "npi")


@pytest.mark.asyncio
async def test_schema_identity_allows_sequence_without_explicit_default(monkeypatch) -> None:
    columns = [{"attname": "id", "default_expression": None}]
    monkeypatch.setattr(archive.catalog_identity, "_catalog_columns", AsyncMock(return_value=columns))
    monkeypatch.setattr(archive.catalog_identity, "_catalog_constraints", AsyncMock(return_value=[]))
    monkeypatch.setattr(archive.catalog_identity, "_catalog_indexes", AsyncMock(return_value=[]))
    monkeypatch.setattr(archive.catalog_identity, "_reject_schema_qualified_expressions", lambda *_args: None)
    monkeypatch.setattr(archive.catalog_identity, "_canonical_digest", lambda _value: "a" * 64)
    monkeypatch.setattr(archive, "_schema_oid", AsyncMock(return_value=9))
    monkeypatch.setattr(
        archive,
        "_owned_sequences",
        AsyncMock(return_value=(("seq", 20, "npi", "id"),)),
    )
    assert await archive._npi_schema_identity(_session(), 10, "mrf", "npi") == "a" * 64


@pytest.mark.parametrize("mutation", ["outer", "shape", "value"])
def test_table_receipt_validation_rejects_malformed_sets(mutation) -> None:
    values = [table.as_dict() for table in _tables()]
    if mutation == "outer":
        values.pop()
        message = "table set"
    elif mutation == "shape":
        values[0].pop("row_count")
        message = "table receipt"
    else:
        values[0]["row_count"] = -1
        message = "table receipt"
    with pytest.raises(archive.NpiResultArchiveError, match=message):
        archive._validate_tables(values)


@pytest.mark.parametrize("mutation", ["shape", "authority", "source", "classification", "digest"])
def test_manifest_validation_rejects_untrusted_fields(mutation) -> None:
    values = _manifest().as_dict()
    if mutation == "shape":
        values.pop("contract")
        message = "manifest is invalid"
    elif mutation == "authority":
        values["capture_authority"] = "unknown"
        message = "authority classification is invalid"
    elif mutation == "source":
        values["canonical_provenance"] = {}
        message = "source authority is invalid"
    elif mutation == "classification":
        values["capture_authority"] = "tracked-generation"
        message = "classification differs"
    else:
        values["schema_sha256"] = "0" * 64
        message = "manifest digest differs"
    with pytest.raises(archive.NpiResultArchiveError, match=message):
        archive.validate_npi_result_manifest(values)


@pytest.mark.asyncio
async def test_source_capture_requires_one_metadata_input() -> None:
    with pytest.raises(archive.NpiResultArchiveError, match="exactly one"):
        await archive.capture_npi_source(
            _session(),
            schema_name="mrf",
            source_metadata={},
            source_metadata_factory=AsyncMock(return_value={}),
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["family", "incomplete-generation", "drift", "snapshot"])
async def test_source_capture_rejects_inconsistent_authority(monkeypatch, failure) -> None:
    """Reject ambiguous inputs and every inconsistent source-authority shape."""

    session = _session(execute=AsyncMock(return_value=_Result("invalid snapshot")))

    @asynccontextmanager
    async def no_limits(_session):
        yield

    monkeypatch.setattr(archive, "_bounded_catalog_work", no_limits)
    monkeypatch.setattr(archive, "_lock_family", AsyncMock())
    authority = _legacy_authority()
    relation_pairs = tuple((name, ordinal) for ordinal, name in enumerate(generation.RELATION_NAMES, 10))
    if failure == "family":
        relation_pairs = ((generation.RELATION_NAMES[0], None), *relation_pairs[1:])
    elif failure == "incomplete-generation":
        authority = replace(authority, relation_oids=(10, 11, 12, 13, 14, 15))
    elif failure == "drift":
        authority = replace(
            authority,
            serving_generation=generation.NpiServingGeneration(str(uuid4()), 1, datetime.datetime.now(datetime.UTC)),
            relation_oids=(20, 21, 22, 23, 24, 25),
        )
    monkeypatch.setattr(archive, "read_npi_result_generation_authority", AsyncMock(return_value=authority))
    monkeypatch.setattr(archive, "_relation_pairs", AsyncMock(return_value=relation_pairs))
    message_by_failure = {
        "family": "family is incomplete",
        "incomplete-generation": "generation is incomplete",
        "drift": "generation is drifted",
        "snapshot": "snapshot is invalid",
    }
    with pytest.raises(archive.NpiResultArchiveError, match=message_by_failure[failure]):
        await archive.capture_npi_source(
            session,
            schema_name="mrf",
            source_metadata={"release": "synthetic"},
        )


@pytest.mark.asyncio
async def test_stage_sequence_paths_retain_exact_state(monkeypatch) -> None:
    existing = await archive._ensure_stage_owned_sequence(
        _session(),
        stage_schema="stage",
        source_sequence_name="npi_id_seq",
        owner_table="npi",
        owner_column="id",
        stage_sequences_by_owner={("npi", "id"): ("stage_seq", 8)},
    )
    assert existing == "stage_seq"
    assert (
        await archive._advance_stage_sequence(
            _session(scalar=AsyncMock(return_value=None)),
            stage_schema="stage",
            sequence_name="seq",
            owner_table="npi",
            owner_column="id",
        )
        is None
    )

    ownership = _ownership(sequences=(("seq", 20, "npi", "id"),))
    monkeypatch.setattr(archive, "_owned_sequences", AsyncMock(return_value=()))
    with pytest.raises(archive.NpiResultArchiveError, match="ownership differs"):
        await archive._advance_and_verify_stage_sequences(_session(), ownership)

    monkeypatch.setattr(archive, "_owned_sequences", AsyncMock(return_value=ownership.sequence_oids))
    monkeypatch.setattr(archive, "_advance_stage_sequence", AsyncMock(return_value=None))
    session = _session(
        scalar=AsyncMock(return_value=None),
        execute=AsyncMock(return_value=_Result((1, False))),
    )
    with pytest.raises(archive.NpiResultArchiveError, match="state is unavailable"):
        await archive._advance_and_verify_stage_sequences(session, ownership)

    session.scalar.return_value = 1
    session.execute.return_value = _Result((2, False))
    with pytest.raises(archive.NpiResultArchiveError, match="state differs"):
        await archive._advance_and_verify_stage_sequences(session, ownership)


@pytest.mark.asyncio
async def test_sequence_owner_and_clone_snapshot_guards(monkeypatch) -> None:
    monkeypatch.setattr(archive, "_owned_sequences", AsyncMock(return_value=()))
    with pytest.raises(archive.NpiResultArchiveError, match="ownership differs"):
        await archive._verify_stage_sequence_owners(
            _session(),
            9,
            (("seq", 20, "npi", "id"),),
        )
    capture = archive.NpiSourceCapture({}, "a" * 64, "legacy-manual", None, None, "mrf", "nope!")
    with pytest.raises(archive.NpiResultArchiveError, match="snapshot is invalid"):
        await archive._clone_source(_session(), capture, "stage")
    with pytest.raises(archive.NpiResultArchiveError, match="owned schema is unavailable"):
        await archive._schema_oid(_session(scalar=AsyncMock(return_value=None)), "stage")


def _valid_trigger_rows(function_oid=70):
    rows = []
    trigger_oid = 100
    for table_name in generation.RELATION_NAMES:
        for name, kind in (
            (archive._FREEZE_WRITE_TRIGGER, 30),
            (archive._FREEZE_TRUNCATE_TRIGGER, 34),
        ):
            rows.append(
                {
                    "table_name": table_name,
                    "oid": trigger_oid,
                    "tgname": name,
                    "tgenabled": "A",
                    "tgtype": kind,
                    "tgfoid": function_oid,
                    "unconditional": True,
                    "tgnargs": 0,
                    "trigger_columns": "",
                    "xmin": "1",
                    "ctid": f"(0,{trigger_oid})",
                }
            )
            trigger_oid += 1
    return rows


def test_freeze_contract_validators_reject_drift_and_accept_complete_set() -> None:
    invalid_function_by_field = {
        "oid": 70,
        "lanname": "sql",
        "prosecdef": True,
        "prosrc": archive._FREEZE_FUNCTION_BODY,
        "proconfig": ["search_path=pg_catalog"],
    }
    with pytest.raises(archive.NpiResultArchiveError, match="function differs"):
        archive._validate_freeze_function(invalid_function_by_field)
    trigger_rows = _valid_trigger_rows()
    invalid_trigger_rows = [dict(row) for row in trigger_rows]
    invalid_trigger_rows[0]["tgenabled"] = "D"
    with pytest.raises(archive.NpiResultArchiveError, match="trigger differs"):
        archive._validate_freeze_triggers(invalid_trigger_rows, 70)
    with pytest.raises(archive.NpiResultArchiveError, match="trigger set differs"):
        archive._validate_freeze_triggers(trigger_rows[:-1], 70)
    assert set(archive._validate_freeze_triggers(trigger_rows, 70)) == set(generation.RELATION_NAMES)


@pytest.mark.asyncio
async def test_freeze_seal_rejects_orphan_triggers(monkeypatch) -> None:
    monkeypatch.setattr(archive, "_read_freeze_function", AsyncMock(return_value=None))
    monkeypatch.setattr(archive, "_read_freeze_triggers", AsyncMock(return_value=_valid_trigger_rows()))
    with pytest.raises(archive.NpiResultArchiveError, match="function differs"):
        await archive._freeze_seal(_session(), 9)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["missing-relation", "sequences", "unexpected"])
async def test_stage_ownership_rejects_catalog_drift(monkeypatch, failure) -> None:
    relation_values = iter([None] if failure == "missing-relation" else range(10, 16))
    monkeypatch.setattr(archive, "_schema_oid", AsyncMock(return_value=9))
    monkeypatch.setattr(archive, "_relation_oid", AsyncMock(side_effect=lambda *_a, **_k: next(relation_values)))
    sequence_oids = ()
    if failure == "sequences":
        sequence_oids = (("a", 30, "npi", "id"), ("b", 31, "npi", "id"))
    monkeypatch.setattr(archive, "_owned_sequences", AsyncMock(return_value=sequence_oids))
    namespace_relations = [] if failure != "unexpected" else [{"oid": 999, "relkind": "r", "index_table_oid": None}]
    monkeypatch.setattr(archive, "_namespace_relations", AsyncMock(return_value=namespace_relations))
    monkeypatch.setattr(archive, "_freeze_seal", AsyncMock(return_value=(None, (), ())))
    message_by_failure = {
        "missing-relation": "owned relation is missing",
        "sequences": "sequence set is invalid",
        "unexpected": "unexpected relation",
    }
    with pytest.raises(archive.NpiResultArchiveError, match=message_by_failure[failure]):
        await archive.capture_npi_stage_ownership(_session(), dataset_id=uuid4())


@pytest.mark.asyncio
async def test_stage_ownership_accepts_byte_catalog_kinds(monkeypatch) -> None:
    relation_oids = iter(range(10, 16))
    monkeypatch.setattr(archive, "_schema_oid", AsyncMock(return_value=9))
    monkeypatch.setattr(archive, "_relation_oid", AsyncMock(side_effect=lambda *_a, **_k: next(relation_oids)))
    monkeypatch.setattr(archive, "_owned_sequences", AsyncMock(return_value=()))
    monkeypatch.setattr(
        archive,
        "_namespace_relations",
        AsyncMock(
            return_value=[
                {"oid": oid, "relkind": b"r" if oid == 10 else "r", "index_table_oid": None} for oid in range(10, 16)
            ]
        ),
    )
    monkeypatch.setattr(archive, "_freeze_seal", AsyncMock(return_value=(None, (), ())))
    ownership = await archive.capture_npi_stage_ownership(_session(), dataset_id=uuid4())
    assert tuple(oid for _, oid in ownership.relation_oids) == tuple(range(10, 16))


@pytest.mark.asyncio
async def test_freeze_and_ownership_type_guards() -> None:
    with pytest.raises(archive.NpiResultArchiveError, match="stage ownership is invalid"):
        await archive.freeze_npi_stage(_session(), ownership=object())
    with pytest.raises(archive.NpiResultArchiveError, match="stage ownership is invalid"):
        await archive.verify_npi_stage_ownership(_session(), object())
    with pytest.raises(archive.NpiResultArchiveError, match="already frozen"):
        await archive._freeze_npi_clone(_session(), _ownership(frozen=True))


@pytest.mark.asyncio
async def test_freeze_requires_new_catalog_seal(monkeypatch) -> None:
    ownership = _ownership()

    @asynccontextmanager
    async def no_limits(_session):
        yield

    monkeypatch.setattr(archive, "_bounded_catalog_work", no_limits)
    monkeypatch.setattr(archive, "_lock_family", AsyncMock())
    monkeypatch.setattr(archive, "verify_npi_stage_ownership", AsyncMock())
    monkeypatch.setattr(archive, "_install_freeze_triggers", AsyncMock())
    monkeypatch.setattr(archive, "capture_npi_stage_ownership", AsyncMock(return_value=ownership))
    with pytest.raises(archive.NpiResultArchiveError, match="clone freeze differs"):
        await archive._freeze_npi_clone(_session(), ownership)


@pytest.mark.asyncio
async def test_stage_manifest_rejects_content_drift(monkeypatch) -> None:
    ownership = _ownership()
    manifest = _manifest()
    monkeypatch.setattr(archive, "_manifest_tables", AsyncMock(return_value=_tables(row_count=1)))
    with pytest.raises(archive.NpiResultArchiveError, match="restored stage differs"):
        await archive._validate_stage_manifest(
            _session(),
            ownership,
            manifest,
            ownership_verified=True,
        )


@pytest.mark.asyncio
async def test_cleanup_returns_for_absent_stage_and_rejects_remaining_objects(monkeypatch) -> None:
    ownership = _ownership()
    session = _session(scalar=AsyncMock(return_value=None))
    assert await archive.cleanup_npi_stage(session, ownership) is None

    @asynccontextmanager
    async def no_limits(_session):
        yield

    session.scalar.side_effect = [ownership.schema_oid, 1]
    monkeypatch.setattr(archive, "_bounded_catalog_work", no_limits)
    monkeypatch.setattr(archive, "_lock_family", AsyncMock())
    monkeypatch.setattr(archive, "verify_npi_stage_ownership", AsyncMock())
    with pytest.raises(archive.NpiResultArchiveError, match="schema is not empty"):
        await archive.cleanup_npi_stage(session, ownership)


@pytest.mark.asyncio
async def test_shielded_cleanup_finishes_before_propagating_cancellation(monkeypatch) -> None:
    ownership = _ownership()
    transaction = _AsyncContext(None)
    session = SimpleNamespace(begin=lambda: transaction)
    session_factory = lambda: _AsyncContext(session)
    cleanup = AsyncMock()
    monkeypatch.setattr(archive, "cleanup_npi_stage", cleanup)
    original_shield = asyncio.shield
    call_state = SimpleNamespace(count=0)

    async def cancel_once(task):
        call_state.count += 1
        if call_state.count == 1:
            await asyncio.sleep(0)
            raise asyncio.CancelledError
        return await original_shield(task)

    monkeypatch.setattr(archive.asyncio, "shield", cancel_once)
    with pytest.raises(asyncio.CancelledError):
        await archive._shielded_cleanup(session_factory, ownership)
    cleanup.assert_awaited_once_with(session, ownership)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["shape", "frozen"])
async def test_export_prepared_source_rejects_invalid_authority(failure) -> None:
    prepared = object() if failure == "shape" else archive.NpiPreparedSource(_manifest(), _ownership())
    with pytest.raises(archive.NpiResultArchiveError, match="prepared source"):
        await archive.export_prepared_npi_archive(
            object(),
            prepared=prepared,
            archive_copy=AsyncMock(),
        )


@pytest.mark.asyncio
async def test_prepare_source_requires_durable_callback() -> None:
    with pytest.raises(archive.NpiResultArchiveError, match="callback is required"):
        await archive.prepare_npi_archive_source(
            object(),
            schema_name="mrf",
            source_metadata={},
            dataset_id=uuid4(),
            on_prepared=None,
        )


@pytest.mark.asyncio
async def test_export_wrapper_cleans_owned_stage(monkeypatch) -> None:
    prepared = archive.NpiPreparedSource(_manifest(), _ownership(frozen=True))
    session_factory = object()
    prepare = AsyncMock(return_value=prepared)
    export = AsyncMock(return_value=prepared.manifest)
    cleanup = AsyncMock()
    monkeypatch.setattr(archive, "prepare_npi_archive_source", prepare)
    monkeypatch.setattr(archive, "export_prepared_npi_archive", export)
    monkeypatch.setattr(archive, "_shielded_cleanup", cleanup)
    result = await archive.export_npi_archive(
        session_factory,
        schema_name="mrf",
        source_metadata={},
        dataset_id=prepared.ownership.dataset_id,
        archive_copy=AsyncMock(),
    )
    assert result == prepared.manifest
    callback = prepare.await_args.kwargs["on_prepared"]
    assert await callback(object(), prepared) is None
    cleanup.assert_awaited_once_with(session_factory, prepared.ownership)


@pytest.mark.parametrize(
    "index_spec",
    [
        {"unknown": True, "index_elements": ["id"]},
        {"index_elements": []},
        {"index_elements": ["id"], "name": "bad-name"},
        {"index_elements": ["id"], "using": "bad"},
        {"index_elements": ["id"], "include": ["bad-name"]},
    ],
)
def test_additional_index_rejects_unsupported_shapes(index_spec) -> None:
    with pytest.raises(archive.NpiResultArchiveError, match="model index"):
        archive._additional_index_sql("stage", archive._MODEL_TYPES[0], index_spec)


def test_additional_index_accepts_bounded_options() -> None:
    sql = archive._additional_index_sql(
        "stage",
        archive._MODEL_TYPES[0],
        {
            "index_elements": ["id"],
            "name": "synthetic",
            "using": "btree",
            "unique": True,
            "include": ["npi"],
            "where": "id > 0",
        },
    )
    assert "CREATE UNIQUE INDEX" in sql
    assert "INCLUDE" in sql


@pytest.mark.parametrize("mutation", ["shape", "fields", "inventory", "digest"])
def test_validation_receipt_rejects_untrusted_fields(mutation) -> None:
    manifest = _manifest()
    ownership = _ownership()
    values = _validation(manifest, ownership).as_dict()
    if mutation == "shape":
        values.pop("contract")
        message = "receipt is invalid"
    elif mutation == "fields":
        values["sealed_owner_oid"] = 0
        message = "receipt is invalid"
    elif mutation == "inventory":
        values["relation_oids"] = []
        message = "inventory is invalid"
    else:
        values["validation_sha256"] = "0" * 64
        message = "digest differs"
    with pytest.raises(archive.NpiResultArchiveError, match=message):
        archive.validate_npi_validation_receipt(values)


def test_validation_inventory_rejects_invalid_pair() -> None:
    values = [[name, ordinal] for ordinal, name in enumerate(sorted(generation.RELATION_NAMES), 10)]
    values[0][1] = 0
    with pytest.raises(archive.NpiResultArchiveError, match="inventory is invalid"):
        archive._validation_inventory(values)


@pytest.mark.asyncio
async def test_prepare_activation_rejects_invalid_package_identity() -> None:
    with pytest.raises(archive.NpiResultArchiveError, match="package identity is invalid"):
        await archive.prepare_npi_activation(
            _session(),
            ownership=_ownership(),
            manifest=_manifest(),
            package_id="bad",
            sealed_owner_oid=50,
        )


@pytest.mark.asyncio
async def test_stage_owner_checks_exact_catalog_identity() -> None:
    ownership = _ownership()
    with pytest.raises(archive.NpiResultArchiveError, match="owner is invalid"):
        await archive._verify_stage_owner(_session(), ownership, 0)
    session = _session(scalar=AsyncMock(return_value=51))
    with pytest.raises(archive.NpiResultArchiveError, match="owner differs"):
        await archive._verify_stage_owner(session, ownership, 50)
    session.scalar.return_value = 50
    session.execute.return_value = _Result([])
    with pytest.raises(archive.NpiResultArchiveError, match="owner differs"):
        await archive._verify_stage_owner(session, ownership, 50)


@pytest.mark.asyncio
async def test_incumbent_capture_rejects_partial_and_changed_families(monkeypatch) -> None:
    @asynccontextmanager
    async def no_limits(_session):
        yield

    monkeypatch.setattr(archive, "_bounded_catalog_work", no_limits)
    monkeypatch.setattr(archive, "_lock_family", AsyncMock())
    partial_pairs = tuple(
        (name, None if ordinal == 0 else ordinal + 10) for ordinal, name in enumerate(generation.RELATION_NAMES)
    )
    relation_pairs = AsyncMock(return_value=partial_pairs)
    monkeypatch.setattr(archive, "_relation_pairs", relation_pairs)
    with pytest.raises(archive.NpiResultArchiveError, match="incumbent is incomplete"):
        await archive.capture_npi_incumbent(_session(), schema_name="mrf")

    complete_pairs = tuple((name, ordinal + 10) for ordinal, name in enumerate(generation.RELATION_NAMES))
    relation_pairs.side_effect = [
        complete_pairs,
        tuple((name, ordinal + 20) for ordinal, name in enumerate(generation.RELATION_NAMES)),
    ]
    with pytest.raises(archive.NpiResultArchiveError, match="changed during capture"):
        await archive.capture_npi_incumbent(_session(), schema_name="mrf")

    absent_pairs = tuple((name, None) for name in generation.RELATION_NAMES)
    relation_pairs.side_effect = None
    relation_pairs.return_value = absent_pairs
    assert await archive.capture_npi_incumbent(_session(), schema_name="mrf") == archive.NpiIncumbent(
        "mrf", absent_pairs
    )


@pytest.mark.asyncio
async def test_incumbent_population_handles_absent_and_empty_families() -> None:
    absent = archive.NpiIncumbent("mrf", tuple((name, None) for name in generation.RELATION_NAMES))
    session = _session(scalar=AsyncMock(return_value=False))
    assert await archive._has_populated_incumbent(session, absent) is False

    complete = archive.NpiIncumbent(
        "mrf",
        tuple((name, ordinal + 10) for ordinal, name in enumerate(generation.RELATION_NAMES)),
    )
    assert await archive._has_populated_incumbent(session, complete) is False
    assert session.scalar.await_count == len(generation.RELATION_NAMES)


@pytest.mark.asyncio
async def test_relation_rotation_rejects_nonempty_stage() -> None:
    ownership = _ownership()
    incumbent = archive.NpiIncumbent("mrf", tuple((name, None) for name in generation.RELATION_NAMES))
    session = _session(scalar=AsyncMock(return_value=1))
    with pytest.raises(archive.NpiResultArchiveError, match="stage schema is not empty"):
        await archive._rotate_relations(session, ownership, incumbent)


@pytest.mark.asyncio
async def test_activation_lock_rejects_changed_incumbent(monkeypatch) -> None:
    @asynccontextmanager
    async def no_limits(_session):
        yield

    ownership = _ownership()
    incumbent = archive.NpiIncumbent("mrf", ownership.relation_oids)
    monkeypatch.setattr(archive, "_bounded_catalog_work", no_limits)
    locks = AsyncMock()
    monkeypatch.setattr(archive, "_lock_family", locks)
    monkeypatch.setattr(archive, "verify_npi_stage_ownership", AsyncMock())
    monkeypatch.setattr(
        archive,
        "_relation_pairs",
        AsyncMock(return_value=tuple((name, oid + 100) for name, oid in ownership.relation_oids)),
    )
    with pytest.raises(archive.NpiResultArchiveError, match="incumbent changed"):
        await archive._lock_and_verify_activation(_session(), ownership, incumbent)
    assert locks.await_count == 2


def test_cutover_bindings_reject_mismatched_authority() -> None:
    manifest = _manifest()
    ownership = _ownership()
    validation = _validation(manifest, ownership)
    cutover = archive.NpiCutoverAuthority("c" * 64, 50, 50, "manual")
    with pytest.raises(archive.NpiResultArchiveError, match="authority differs"):
        archive._validate_cutover_bindings(ownership, manifest, validation, cutover)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["missing-source", "legacy", "drift", "order"])
async def test_automatic_cutover_rejects_unsafe_generation(monkeypatch, failure) -> None:
    incumbent = archive.NpiIncumbent(
        "mrf",
        tuple((name, ordinal) for ordinal, name in enumerate(generation.RELATION_NAMES, 10)),
    )
    current_authority = generation.NpiResultGenerationAuthority(str(uuid4()), 1, None, None, None)
    source_generation = generation.NpiServingGeneration(
        str(uuid4()), 2, datetime.datetime(2026, 9, 14, tzinfo=datetime.UTC)
    )
    if failure == "missing-source":
        source_generation = None
        message = "source generation is unavailable"
    elif failure == "legacy":
        monkeypatch.setattr(archive, "_has_populated_incumbent", AsyncMock(return_value=True))
        message = "legacy incumbent"
    else:
        current_authority = replace(
            current_authority,
            serving_generation=generation.NpiServingGeneration(
                source_generation.origin_lineage_id,
                3,
                datetime.datetime(2026, 9, 13, tzinfo=datetime.UTC),
            ),
            relation_oids=(20, 21, 22, 23, 24, 25) if failure == "drift" else tuple(range(10, 16)),
        )
        message = "generation is drifted" if failure == "drift" else "stale or unrelated"
    with pytest.raises(archive.NpiResultArchiveError, match=message):
        await archive._admit_automatic_cutover(
            _session(),
            incumbent=incumbent,
            current_authority=current_authority,
            source_generation=source_generation,
        )


@pytest.mark.asyncio
async def test_automatic_cutover_allows_empty_legacy_incumbent(monkeypatch) -> None:
    incumbent = archive.NpiIncumbent(
        "mrf",
        tuple((name, None) for name in generation.RELATION_NAMES),
    )
    current = generation.NpiResultGenerationAuthority(str(uuid4()), 1, None, None, None)
    source = generation.NpiServingGeneration(str(uuid4()), 2, datetime.datetime(2026, 9, 14, tzinfo=datetime.UTC))
    monkeypatch.setattr(archive, "_has_populated_incumbent", AsyncMock(return_value=False))
    assert (
        await archive._admit_automatic_cutover(
            _session(),
            incumbent=incumbent,
            current_authority=current,
            source_generation=source,
        )
        is None
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["identity", "legacy-adoption", "tracked-adoption"])
async def test_relation_activation_rejects_identity_or_adoption_drift(monkeypatch, failure) -> None:
    is_tracked = failure == "tracked-adoption"
    manifest = _manifest(tracked=is_tracked)
    ownership = _ownership()
    incumbent = archive.NpiIncumbent("mrf", tuple((name, None) for name in generation.RELATION_NAMES))
    validation = _validation(manifest, ownership)
    monkeypatch.setattr(archive, "_remove_npi_clone_freeze", AsyncMock())
    monkeypatch.setattr(archive, "install_npi_stage_mutation_guards", AsyncMock())
    monkeypatch.setattr(archive, "_rotate_relations", AsyncMock(return_value=None))
    live_pairs = ownership.relation_oids
    if failure == "identity":
        live_pairs = tuple((name, None) for name in generation.RELATION_NAMES)
    monkeypatch.setattr(archive, "_relation_pairs", AsyncMock(return_value=live_pairs))
    adopted = generation.NpiResultGenerationAuthority(
        str(uuid4()),
        1,
        generation.NpiServingGeneration(str(uuid4()), 1, datetime.datetime(2026, 9, 14, tzinfo=datetime.UTC)),
        tuple(range(10, 16)),
        None,
    )
    monkeypatch.setattr(archive, "publish_adopted_npi_result_generation", AsyncMock(return_value=adopted))
    message_by_failure = {
        "identity": "activated relation identity differs",
        "legacy-adoption": "generation-less adoption differs",
        "tracked-adoption": "adopted generation differs",
    }
    with pytest.raises(archive.NpiResultArchiveError, match=message_by_failure[failure]):
        await archive._activate_npi_relations(
            _session(),
            ownership=ownership,
            incumbent=incumbent,
            manifest=manifest,
            validation=validation,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["ownership", "authority", "callback"])
async def test_activation_entry_rejects_invalid_controller_inputs(failure) -> None:
    ownership = _ownership()
    incumbent = archive.NpiIncumbent("mrf", tuple((name, None) for name in generation.RELATION_NAMES))
    cutover = archive.NpiCutoverAuthority("b" * 64, 50, 50, "manual")
    callback = AsyncMock()
    if failure == "ownership":
        ownership = object()
        message = "ownership is invalid"
    elif failure == "authority":
        cutover = archive.NpiCutoverAuthority("b" * 64, 50, 50, "bad")
        message = "authority is unsupported"
    else:
        callback = None
        message = "callback is required"
    with pytest.raises(archive.NpiResultArchiveError, match=message):
        await archive.activate_validated_npi_stage(
            _session(),
            ownership=ownership,
            manifest=_manifest(),
            incumbent=incumbent,
            validation_receipt={},
            cutover=cutover,
            on_activated=callback,
        )
