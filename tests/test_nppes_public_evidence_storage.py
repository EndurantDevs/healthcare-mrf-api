# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native-family cursor, closed publication and retry boundaries without a database."""

import hashlib
from contextlib import asynccontextmanager
from copy import deepcopy
from datetime import UTC, date, datetime
from functools import partial
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import nppes_public_evidence_storage as storage
from process.nppes_public_evidence_prepared_chunks import prepare_nppes_row
from process.nppes_public_evidence_writer_contract import NppesPublicEvidenceWriterError
from process.provider_directory_generic_candidate import GenericCandidateError
from process.provider_directory_generic_source_policy import GenericSourcePolicy

PRODUCER = ("a" * 40, "sha256:" + "b" * 64)


def _buffers(first=1, count=2):
    buffers = SimpleNamespace()
    for root, (_, columns, attribute, _) in storage.ROOTS.items():
        records = []
        for ordinal in range(first, first + count):
            record_entries = tuple(
                ordinal
                if column in {"source_row_ordinal", "source_record_ordinal", "source_record_count"}
                else bytes.fromhex("a" * 64)
                if column.endswith("_sha256")
                else date(2020, 1, 1)
                if column
                in {"provider_enumeration_date", "last_update_date", "npi_deactivation_date", "npi_reactivation_date"}
                else datetime(2020, 1, 1, tzinfo=UTC)
                if column in {"observed_at", "effective_start_at", "effective_end_at"}
                else True
                if column in {"positive_evidence_only", "publication_enabled"}
                else "value"
                for column in columns
            )
            records.append(prepare_nppes_row(root, record_entries))
        setattr(buffers, attribute, records)
    return buffers


def _proofs(names):
    return {
        root: dict(name=name, oid=index + 100, owner=500, layout_sha256="c" * 64)
        for root, name in names.items()
        for index in [sorted(storage.ROOTS).index(root)]
    }


async def _execute_storage_query(state_by_key, query, *args):
    state_by_key["sql"].append(query)
    if query.startswith("INSERT"):
        state_by_key["prepared_chunks"].append(args)
        return "INSERT 0 1"
    if query.startswith("DROP TABLE"):
        state_by_key["prepared_chunks"].clear()
    if query.startswith("UPDATE"):
        assert storage._json(args[2]) == state_by_key["binding"]
        state_by_key["binding"] = storage._json(args[1])
        return "UPDATE 1"
    return "OK"


async def _fetch_storage_header(state_by_key, *args):
    return dict(
        admission_ref="admission",
        source_release_ref="release",
        candidate_storage_json=deepcopy(state_by_key["binding"]),
    )


async def _copy_storage_records(state_by_key, name, **kwargs):
    state_by_key["copies"].append((name, kwargs))
    return f"COPY {len(kwargs['records'])}"


@asynccontextmanager
async def _storage_savepoint(state_by_key, connection, policy, database):
    before = deepcopy(state_by_key)
    try:
        yield connection, policy
    except BaseException:
        state_by_key.clear()
        state_by_key.update(before)
        raise


def _storage_harness_identity():
    return dict(
        admission_ref="admission",
        admission_contract_sha256="c" * 64,
        source_release_ref="release",
        source_release_contract_sha256="d" * 64,
        source_kind="nppes_entity_address",
        artifact_sha256="e" * 64,
        manifest_sha256="f" * 64,
    )


def _patch_storage_harness(monkeypatch, transaction, binding_by_key, identity_by_key):
    monkeypatch.setattr(storage, "_transaction", transaction)
    monkeypatch.setattr(storage, "_header_custody", AsyncMock())
    monkeypatch.setattr(
        storage, "_capture", AsyncMock(side_effect=lambda conn, configured, tables, **kwargs: _proofs(tables))
    )
    chunk_proof = deepcopy(binding_by_key["chunks"]["table"])
    monkeypatch.setattr(
        storage, "_capture_chunks", AsyncMock(side_effect=lambda conn, configured, name: {**chunk_proof, "name": name})
    )
    verify_manifest = storage._verify_chunk_manifest
    monkeypatch.setattr(storage, "_verify_chunk_manifest", AsyncMock())
    monkeypatch.setattr(storage.source_policy, "source_producer_identity", lambda: PRODUCER)
    monkeypatch.setattr(storage, "_identity", lambda prepared: deepcopy(identity_by_key))
    return verify_manifest


@pytest.fixture
def harness(monkeypatch):
    identity_by_key = _storage_harness_identity()
    policy = GenericSourcePolicy("source_schema", "source_owner", "source_publisher", ("source_reader",))
    names_by_key = {root: "npec_" + "a" * 32 + "_" + fields[3] for root, fields in storage.ROOTS.items()}
    binding_by_key = dict(
        contract=storage.CONTRACT,
        identity=identity_by_key,
        producer_identity=list(PRODUCER),
        policy=storage._policy_fields(policy),
        phase="loading",
        ordinal=0,
        counts=dict.fromkeys(storage.ROOTS, 0),
        last_batch=None,
        tables=_proofs(names_by_key),
        validation_sha256=None,
        chunks=dict(
            table=dict(name="npec_" + "a" * 32 + "_p", oid=900, owner=500, layout_sha256="c" * 64), count=0, sha256=""
        ),
    )
    binding_by_key["chunks"]["sha256"] = storage._chunk_seed(binding_by_key)
    state_by_key = {"binding": binding_by_key, "copies": [], "sql": [], "prepared_chunks": []}

    execute = partial(_execute_storage_query, state_by_key)

    fetchrow = partial(_fetch_storage_header, state_by_key)

    copy = partial(_copy_storage_records, state_by_key)

    connection = SimpleNamespace(
        execute=AsyncMock(side_effect=execute),
        fetchrow=AsyncMock(side_effect=fetchrow),
        fetchval=AsyncMock(return_value=True),
        copy_records_to_table=AsyncMock(side_effect=copy),
        is_in_transaction=lambda: True,
    )

    transaction = partial(_storage_savepoint, state_by_key, connection, policy)

    verify_manifest = _patch_storage_harness(monkeypatch, transaction, binding_by_key, identity_by_key)
    return SimpleNamespace(
        state=state_by_key,
        verify_manifest=verify_manifest,
        connection=connection,
        policy=policy,
        identity=identity_by_key,
        candidate=storage._candidate(binding_by_key),
        prepared=SimpleNamespace(admission_row=SimpleNamespace(source_record_count=2, projected_record_count=2)),
    )


@pytest.mark.asyncio
async def test_unbound_ambient_database_cannot_open_storage_transaction():
    with pytest.raises(GenericCandidateError, match="publisher_connection_required"):
        async with storage._transaction(object()):
            pytest.fail("ambient database became a publisher")


@pytest.mark.asyncio
async def test_batch_copy_and_cursor_commit_are_one_unit(harness):
    candidate = await storage.append_nppes_evidence_batch(object(), harness.candidate, _buffers())
    assert candidate.ordinal == candidate.source_record_count == 2
    assert candidate.counts == dict.fromkeys(storage.ROOTS, 2)
    assert len(harness.state["copies"]) == 5
    assert all(copy[1]["schema_name"] == "source_schema" for copy in harness.state["copies"])
    assert len(harness.state["sql"]) == 2
    assert harness.state["sql"][0].startswith("INSERT")
    assert harness.state["sql"][1].startswith("UPDATE")
    assert harness.state["binding"]["chunks"]["count"] == 1


@pytest.mark.asyncio
async def test_uncertain_commit_replay_does_not_duplicate_payload(harness):
    first = await storage.append_nppes_evidence_batch(object(), harness.candidate, _buffers())
    replayed = await storage.append_nppes_evidence_batch(object(), harness.candidate, _buffers())
    assert replayed == first
    assert len(harness.state["copies"]) == 5
    assert len(harness.state["sql"]) == 2


@pytest.mark.asyncio
async def test_stale_cursor_cannot_append_a_later_batch(harness):
    await storage.append_nppes_evidence_batch(object(), harness.candidate, _buffers())
    with pytest.raises(NppesPublicEvidenceWriterError):
        await storage.append_nppes_evidence_batch(object(), harness.candidate, _buffers(3))
    assert len(harness.state["copies"]) == 5


@pytest.mark.asyncio
async def test_last_stream_failure_rolls_back_the_whole_prefix(harness):
    harness.connection.copy_records_to_table.side_effect = ["COPY 2"] * 4 + [RuntimeError("last stream failed")]
    before = deepcopy(harness.state)
    with pytest.raises(RuntimeError, match="last stream"):
        await storage.append_nppes_evidence_batch(object(), harness.candidate, _buffers())
    assert harness.state == before


@pytest.mark.asyncio
async def test_cancel_after_copy_keeps_the_old_prefix(harness):
    callback = AsyncMock(side_effect=[None, None, RuntimeError("canceled")])
    with pytest.raises(RuntimeError, match="canceled"):
        await storage.append_nppes_evidence_batch(object(), harness.candidate, _buffers(), cancel_check=callback)
    assert harness.state["binding"]["ordinal"] == 0
    assert harness.state["copies"] == []


@pytest.mark.asyncio
async def test_changed_producer_fails_before_copy(harness, monkeypatch):
    monkeypatch.setattr(storage.source_policy, "source_producer_identity", lambda: ("d" * 40, PRODUCER[1]))
    with pytest.raises(NppesPublicEvidenceWriterError):
        await storage.append_nppes_evidence_batch(object(), harness.candidate, _buffers())
    assert harness.state["copies"] == []


@pytest.mark.parametrize(
    "mutation",
    [
        lambda buffers: buffers.members.pop(),
        lambda buffers: buffers.typed_rows.pop(),
        lambda buffers: buffers.members.reverse(),
        lambda buffers: buffers.source_records.append(buffers.source_records[0]),
        lambda buffers: setattr(buffers, "members", [tuple("bad" for _ in storage.MEMBER_COLUMNS)]),
    ],
)
def test_incomplete_or_noncontiguous_batch_is_rejected(mutation):
    buffers = _buffers()
    mutation(buffers)
    with pytest.raises(NppesPublicEvidenceWriterError):
        storage._batch(buffers)


def test_batch_is_bounded_by_bytes_and_rows(monkeypatch):
    monkeypatch.setattr(storage, "COPY_MAX_ROWS", 1)
    with pytest.raises(NppesPublicEvidenceWriterError):
        storage._batch(_buffers())
    monkeypatch.setattr(storage, "COPY_MAX_ROWS", 4096)
    monkeypatch.setattr(storage, "COPY_MAX_BYTES", 32)
    with pytest.raises(NppesPublicEvidenceWriterError):
        storage._batch(_buffers())


@pytest.mark.asyncio
async def test_failed_validation_cannot_prepare_or_publish(harness, monkeypatch):
    from process import nppes_public_evidence_validation as validation

    await storage.append_nppes_evidence_batch(object(), harness.candidate, _buffers())
    monkeypatch.setattr(storage, "_close_grants", AsyncMock())
    check = AsyncMock(side_effect=RuntimeError("invalid child"))
    monkeypatch.setattr(validation, "validate_nppes_evidence_family", check)
    before = deepcopy(harness.state["binding"])
    with pytest.raises(RuntimeError, match="invalid child"):
        await storage.prepare_nppes_evidence_publication(object(), harness.candidate, harness.prepared)
    assert harness.state["binding"] == before
    with pytest.raises(NppesPublicEvidenceWriterError):
        await storage.publish_nppes_evidence_candidate(object(), harness.candidate, object())


@pytest.mark.asyncio
async def test_prepare_closes_writes_and_indexes_before_validation(harness, monkeypatch):
    from process import nppes_public_evidence_validation as validation

    candidate = await storage.append_nppes_evidence_batch(object(), harness.candidate, _buffers())
    closed_entries = []
    monkeypatch.setattr(
        storage, "_close_grants", AsyncMock(side_effect=lambda conn, policy, name: closed_entries.append(name))
    )

    async def validate(connection, schema, tables, prepared, *, prepared_chunks, chunk_count):
        assert prepared_chunks.endswith("_p") and chunk_count == 1
        assert len(closed_entries) == 5
        assert sum(query.startswith("ALTER TABLE") for query in harness.state["sql"]) == 8
        assert sum(query.startswith("ANALYZE") for query in harness.state["sql"]) == 5

    monkeypatch.setattr(validation, "validate_nppes_evidence_family", validate)
    prepared = await storage.prepare_nppes_evidence_publication(object(), candidate, harness.prepared)
    assert prepared.ordinal == 2
    assert harness.state["binding"]["phase"] == "prepared"
    with pytest.raises(NppesPublicEvidenceWriterError):
        await storage.append_nppes_evidence_batch(object(), candidate, _buffers(3))


@pytest.mark.asyncio
async def test_grant_closure_includes_column_grants_and_public():
    connection = SimpleNamespace(
        fetchval=AsyncMock(return_value=10),
        execute=AsyncMock(),
        fetch=AsyncMock(
            return_value=[
                dict(grantee=0, rolname=None, column_name=None),
                dict(grantee=30, rolname="runtime", column_name="payload_sha256"),
            ]
        ),
    )
    policy = GenericSourcePolicy("source_schema", "source_owner", "source_publisher", ("source_reader",))
    await storage._close_grants(connection, policy, "candidate")
    commands = [call.args[0] for call in connection.execute.await_args_list]
    assert commands == [
        'REVOKE ALL PRIVILEGES ON TABLE "source_schema"."candidate" FROM PUBLIC CASCADE',
        'REVOKE ALL PRIVILEGES ("payload_sha256") ON TABLE "source_schema"."candidate" FROM "runtime" CASCADE',
    ]


@pytest.mark.asyncio
async def test_unsealed_or_loading_family_cannot_resolve(harness):
    with pytest.raises(NppesPublicEvidenceWriterError):
        await storage.resolve_nppes_evidence_family(harness.connection, "source_schema", "admission")


@pytest.mark.parametrize(
    "mutation",
    [
        lambda binding: binding["tables"]["member"].update(name="unrelated"),
        lambda binding: binding["tables"]["member"].update(oid=0),
        lambda binding: binding["counts"].update(source_record=1),
        lambda binding: binding.update(phase="disposed"),
    ],
)
def test_binding_refuses_names_identity_and_cursor_drift(harness, mutation):
    binding = deepcopy(harness.state["binding"])
    mutation(binding)
    with pytest.raises(NppesPublicEvidenceWriterError):
        storage._binding(binding)


def _catalog_rows(policy, names):
    catalog_rows = []
    for root, name in names.items():
        columns = (
            (*storage.ROOTS[root][1], "created_at")
            if root == "member"
            else (*storage.ROOTS[root][1][:-1], "created_at", storage.ROOTS[root][1][-1])
        )
        catalog_rows.append(
            dict(
                relname=name,
                nspname=policy.schema,
                relowner=500,
                oid=100 + len(catalog_rows),
                relkind="r",
                relpersistence="p",
                parents=[],
                relispartition=False,
                relrowsecurity=False,
                relforcerowsecurity=False,
                inert=True,
                native_checks=True,
                direct_acl_closed=True,
                readers_closed=True,
                constraints=[],
                indexes=[],
                columns=[
                    [
                        column,
                        "text",
                        column not in storage._NULLABLE[root],
                        "transaction_timestamp()" if column == "created_at" else None,
                    ]
                    for column in columns
                ],
            )
        )
    return catalog_rows


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mutation",
    [
        lambda rows: rows[0].update(inert=False),
        lambda rows: rows[0].update(native_checks=False),
        lambda rows: rows[0].update(direct_acl_closed=False),
        lambda rows: rows[0].update(relowner=501),
        lambda rows: rows[0].update(relrowsecurity=True),
        lambda rows: rows[0]["columns"][0].__setitem__(2, False),
        lambda rows: rows[0]["columns"][0].__setitem__(3, "custom_default()"),
        lambda rows: rows[0]["constraints"].append(dict(type="f", valid=True)),
    ],
)
async def test_native_family_proof_rejects_hooks_acl_owner_shape_and_fk_drift(monkeypatch, mutation):
    policy = GenericSourcePolicy("source_schema", "source_owner", "source_publisher", ("source_reader",))
    names_by_key = {root: "npec_" + "a" * 32 + "_" + fields[3] for root, fields in storage.ROOTS.items()}
    rows = _catalog_rows(policy, names_by_key)
    connection = SimpleNamespace(fetch=AsyncMock(return_value=rows), fetchval=AsyncMock(return_value=True))
    monkeypatch.setattr(storage, "storage_roles", AsyncMock(return_value=(500, [501])))
    assert set(await storage._capture(connection, policy, names_by_key, indexed=False)) == set(storage.ROOTS)
    mutation(rows)
    with pytest.raises(NppesPublicEvidenceWriterError):
        await storage._capture(connection, policy, names_by_key, indexed=False)


@pytest.mark.asyncio
async def test_native_family_proof_rejects_custom_types_or_operator_classes(monkeypatch):
    policy = GenericSourcePolicy("source_schema", "source_owner", "source_publisher", ("source_reader",))
    names_by_key = {root: "npec_" + "a" * 32 + "_" + fields[3] for root, fields in storage.ROOTS.items()}
    connection = SimpleNamespace(
        fetch=AsyncMock(return_value=_catalog_rows(policy, names_by_key)), fetchval=AsyncMock(return_value=False)
    )
    monkeypatch.setattr(storage, "storage_roles", AsyncMock(return_value=(500, [501])))
    with pytest.raises(NppesPublicEvidenceWriterError):
        await storage._capture(connection, policy, names_by_key, indexed=False)


@pytest.mark.asyncio
async def test_published_release_replays_after_producer_image_changes(harness, monkeypatch):
    candidate = await storage.append_nppes_evidence_batch(object(), harness.candidate, _buffers())
    _sealed(harness.state["binding"], "published")
    harness.state["sql"].clear()
    harness.connection.execute.reset_mock()
    harness.connection.fetchrow.reset_mock()
    current = ("f" * 40, "sha256:" + "e" * 64)
    monkeypatch.setattr(storage.source_policy, "source_producer_identity", lambda: current)
    controls = AsyncMock()
    monkeypatch.setattr(storage, "_assert_control_rows", controls)
    resumed = await storage.prepare_nppes_evidence_candidate(object(), object(), producer_identity=current)
    assert resumed.producer_identity == list(PRODUCER)
    assert resumed.ordinal == candidate.ordinal
    assert resumed.phase == "published"
    harness.connection.execute.assert_not_awaited()
    assert all("FOR UPDATE" not in call.args[0] for call in harness.connection.fetchrow.await_args_list)
    assert await storage.publish_nppes_evidence_candidate(object(), resumed, object()) == "already_present"
    assert not any(query.startswith(("INSERT", "UPDATE", "ALTER")) for query in harness.state["sql"])
    assert controls.await_count == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ("loading", "prepared"))
async def test_unfinished_prepare_retains_mutation_locks_and_verified_phase(harness, phase):
    if phase == "prepared":
        _sealed(harness.state["binding"], phase)
    candidate = await storage.prepare_nppes_evidence_candidate(object(), object(), producer_identity=PRODUCER)
    assert candidate.phase == phase
    assert any("pg_advisory_xact_lock" in query for query in harness.state["sql"])
    assert any("FOR UPDATE" in call.args[0] for call in harness.connection.fetchrow.await_args_list)


@pytest.mark.asyncio
async def test_prepared_family_cannot_change_its_producing_image(harness, monkeypatch):
    candidate = await storage.append_nppes_evidence_batch(object(), harness.candidate, _buffers())
    _sealed(harness.state["binding"], "prepared")
    monkeypatch.setattr(storage.source_policy, "source_producer_identity", lambda: ("f" * 40, PRODUCER[1]))
    with pytest.raises(NppesPublicEvidenceWriterError):
        await storage.publish_nppes_evidence_candidate(object(), candidate, harness.prepared)


def _sealed(binding, phase):
    if phase == "published":
        binding["chunks"]["table"] = None
    binding.update(
        phase=phase,
        validation_sha256=storage._validation_fingerprint(binding),
    )


@pytest.mark.asyncio
async def test_publication_inserts_controls_seal_and_binding_together(harness, monkeypatch):
    from process import nppes_public_evidence_writer as writer

    candidate = await storage.append_nppes_evidence_batch(object(), harness.candidate, _buffers())
    _sealed(harness.state["binding"], "prepared")
    harness.connection.fetchval.return_value = False
    insert = AsyncMock()
    close = AsyncMock()
    monkeypatch.setattr(writer, "_insert_single", insert)
    monkeypatch.setattr(storage, "_close_grants", close)
    monkeypatch.setattr(storage, "source_identity_values", lambda release: ("identity",))
    monkeypatch.setattr(storage, "source_release_values", lambda release: ("release",))
    monkeypatch.setattr(storage, "admission_values", lambda admission: ("admission",))
    prepared = SimpleNamespace(manifest=SimpleNamespace(release=object()), admission_row=object())
    assert await storage.publish_nppes_evidence_candidate(object(), candidate, prepared) == "inserted"
    assert [call.args[2] for call in insert.await_args_list] == [
        "public_evidence_source_identity",
        "public_evidence_source_release",
        "public_evidence_nppes_registry_admission",
        "public_evidence_nppes_registry_admission_seal",
    ]
    assert harness.state["binding"]["phase"] == "published"
    assert close.await_count == 5 and all(call.kwargs == {"readable": True} for call in close.await_args_list)


def _legacy_controls(harness, monkeypatch):
    values_by_key = {}
    for table, columns, codec, identity in (
        ("public_evidence_source_identity", storage.SOURCE_IDENTITY_COLUMNS, "source_identity_values", "identity"),
        ("public_evidence_source_release", storage.SOURCE_RELEASE_COLUMNS, "source_release_values", "release"),
        ("public_evidence_nppes_registry_admission", storage.ADMISSION_COLUMNS, "admission_values", "admission"),
    ):
        catalog_row = (identity, *(f"canonical:{column}" for column in columns[1:]))
        values_by_key[table] = dict(zip(columns, catalog_row, strict=True))
        monkeypatch.setattr(storage, codec, lambda value, catalog_row=catalog_row: catalog_row)
    values_by_key["public_evidence_nppes_registry_admission_seal"] = dict(
        admission_ref="admission", sealed_at="original"
    )
    harness.state.update(binding=None, controls=values_by_key, sealed=True)
    original_execute = harness.connection.execute.side_effect

    async def execute(query, *args):
        if query.startswith("INSERT"):
            if f'"{storage.HEADER}"' in query:
                harness.state["binding"] = storage._json(args[2])
                return "INSERT 0 1"
            if '_p"' in query:
                return await original_execute(query, *args)
            assert "ON CONFLICT DO NOTHING" in query
            return "INSERT 0 0"
        return await original_execute(query, *args)

    async def fetchrow(query, *args):
        if f'"{storage.HEADER}"' in query:
            binding = harness.state["binding"]
            return (
                None
                if binding is None
                else dict(
                    admission_ref="admission", source_release_ref="release", candidate_storage_json=deepcopy(binding)
                )
            )
        for table, catalog_row in harness.state["controls"].items():
            if f'"{table}"' in query:
                return deepcopy(catalog_row)
        raise AssertionError(query)

    async def fetchval(query, *args):
        if "JOIN" in query:
            return harness.state["sealed"]
        assert "WHERE source_release_ref=$1" in query and args == ("release",)
        return True

    harness.connection.execute.side_effect = execute
    harness.connection.fetchrow.side_effect = fetchrow
    harness.connection.fetchval.side_effect = fetchval
    harness.connection.fetch = AsyncMock(return_value=[])
    monkeypatch.setattr(storage, "_close_grants", AsyncMock())
    return SimpleNamespace(
        manifest=SimpleNamespace(release=object()),
        admission_row=SimpleNamespace(source_record_count=2, projected_record_count=2),
    )


async def _requalify_legacy(harness, monkeypatch):
    from process import nppes_public_evidence_validation as validation

    prepared = _legacy_controls(harness, monkeypatch)
    candidate = await storage.prepare_nppes_evidence_candidate(object(), prepared, producer_identity=PRODUCER)
    assert candidate.ordinal == 0
    candidate = await storage.append_nppes_evidence_batch(object(), candidate, _buffers())
    validate = AsyncMock()
    monkeypatch.setattr(validation, "validate_nppes_evidence_family", validate)
    candidate = await storage.prepare_nppes_evidence_publication(object(), candidate, prepared)
    validate.assert_awaited_once()
    storage._close_grants.reset_mock()
    return candidate, prepared


@pytest.mark.asyncio
async def test_sealed_legacy_without_header_is_replayed_and_reuses_unchanged_controls(harness, monkeypatch):
    candidate, prepared = await _requalify_legacy(harness, monkeypatch)
    controls = deepcopy(harness.state["controls"])
    assert await storage.publish_nppes_evidence_candidate(object(), candidate, prepared) == "already_present"
    assert harness.state["binding"]["phase"] == "published"
    assert harness.state["binding"]["producer_identity"] == list(PRODUCER)
    assert harness.state["binding"]["counts"] == dict.fromkeys(storage.ROOTS, 2)
    assert harness.state["controls"] == controls
    assert controls["public_evidence_source_release"]["import_run_ref"] == "canonical:import_run_ref"
    assert len(harness.state["copies"]) == 5


@pytest.mark.parametrize(
    "table,column",
    [
        ("public_evidence_source_identity", "content_sha256"),
        ("public_evidence_source_release", "import_run_ref"),
        ("public_evidence_nppes_registry_admission", "source_record_count"),
    ],
)
@pytest.mark.asyncio
async def test_legacy_semantic_mismatch_rolls_back_publication(harness, monkeypatch, table, column):
    candidate, prepared = await _requalify_legacy(harness, monkeypatch)
    harness.state["controls"][table][column] = "changed"
    before = deepcopy(harness.state)
    with pytest.raises(NppesPublicEvidenceWriterError):
        await storage.publish_nppes_evidence_candidate(object(), candidate, prepared)
    assert harness.state == before
    storage._close_grants.assert_not_awaited()


@pytest.mark.asyncio
async def test_unsealed_legacy_admission_cannot_be_reused(harness, monkeypatch):
    candidate, prepared = await _requalify_legacy(harness, monkeypatch)
    harness.state["sealed"] = False
    before = deepcopy(harness.state)
    with pytest.raises(NppesPublicEvidenceWriterError):
        await storage.publish_nppes_evidence_candidate(object(), candidate, prepared)
    assert harness.state == before


@pytest.mark.asyncio
async def test_interrupted_legacy_publication_can_retry_and_then_resume_published(harness, monkeypatch):
    candidate, prepared = await _requalify_legacy(harness, monkeypatch)
    before = deepcopy(harness.state)
    with pytest.raises(RuntimeError, match="interrupted"):
        await storage.publish_nppes_evidence_candidate(
            object(),
            candidate,
            prepared,
            cancel_check=AsyncMock(side_effect=[None, None, RuntimeError("interrupted")]),
        )
    assert harness.state == before
    assert await storage.publish_nppes_evidence_candidate(object(), candidate, prepared) == "already_present"
    published = deepcopy(harness.state)
    assert await storage.publish_nppes_evidence_candidate(object(), candidate, prepared) == "already_present"
    assert harness.state == published


@pytest.mark.asyncio
async def test_published_family_requires_its_exact_seal(harness, monkeypatch):
    candidate = await storage.append_nppes_evidence_batch(object(), harness.candidate, _buffers())
    _sealed(harness.state["binding"], "published")
    monkeypatch.setattr(storage, "_assert_control_rows", AsyncMock())
    harness.connection.fetchval.return_value = False
    with pytest.raises(NppesPublicEvidenceWriterError):
        await storage.publish_nppes_evidence_candidate(object(), candidate, object())


@pytest.mark.asyncio
async def test_prepare_creates_only_private_unindexed_persistent_heaps(harness):
    harness.connection.fetchrow.side_effect = None
    harness.connection.fetchrow.return_value = None

    async def execute(query, *args):
        harness.state["sql"].append(query)
        return "INSERT 0 1" if query.startswith("INSERT") else "OK"

    harness.connection.execute.side_effect = execute
    harness.connection.fetch = AsyncMock(return_value=[])
    candidate = await storage.prepare_nppes_evidence_candidate(object(), object(), producer_identity=PRODUCER)
    assert candidate.ordinal == 0 and candidate.counts == dict.fromkeys(storage.ROOTS, 0)
    creates = [query for query in harness.state["sql"] if query.startswith("CREATE")]
    assert len(creates) == 6 and all(query.startswith("CREATE TABLE") for query in creates)
    payload_creates = [query for query in creates if '_p"' not in query]
    assert len(payload_creates) == 5
    assert not any(
        token in query
        for token in ("INCLUDING CONSTRAINTS", "FOREIGN KEY", "TRIGGER", "INDEX")
        for query in payload_creates
    )
    harness.connection.copy_records_to_table.assert_not_awaited()


@pytest.mark.asyncio
async def test_refreshed_cursor_can_append_the_next_complete_batch(harness):
    first = await storage.append_nppes_evidence_batch(object(), harness.candidate, _buffers())
    second = await storage.append_nppes_evidence_batch(object(), first, _buffers(3))
    assert second.ordinal == 4 and second.counts == dict.fromkeys(storage.ROOTS, 4)
    assert harness.state["binding"]["last_batch"]["start"] == 3
    assert len(harness.state["copies"]) == 10


@pytest.mark.asyncio
async def test_unknown_commit_replay_with_changed_content_is_rejected(harness):
    await storage.append_nppes_evidence_batch(object(), harness.candidate, _buffers())
    changed = _buffers()
    changed.source_records[0] = prepare_nppes_row("source_record", ("different", *changed.source_records[0].values[1:]))
    with pytest.raises(NppesPublicEvidenceWriterError):
        await storage.append_nppes_evidence_batch(object(), harness.candidate, changed)
    assert len(harness.state["copies"]) == 5


def _manifest_connection(chunk_values, is_missing):
    async def cursor(query, maximum, **kwargs):
        assert "pg_catalog.sha256" in query and maximum == storage.COPY_MAX_BYTES
        assert kwargs == {"prefetch": 64}
        if is_missing:
            return
        chunk_by_field = dict(
            chunk_ordinal=chunk_values[0],
            start_ordinal=chunk_values[1],
            end_ordinal=chunk_values[2],
            batch_sha256=chunk_values[3],
            counts_json=chunk_values[4],
            bounded=True,
        )
        for index, root in enumerate(storage.ROOTS):
            payload, digest = chunk_values[5 + 2 * index : 7 + 2 * index]
            chunk_by_field[root + "_sha256"] = digest
            chunk_by_field[root + "_valid"] = hashlib.sha256(payload).hexdigest() == digest
        yield chunk_by_field

    return SimpleNamespace(cursor=cursor)


@pytest.mark.asyncio
@pytest.mark.parametrize("corruption", (None, "payload", "digest", "missing", "scope", "producer", "range", "counts"))
async def test_prepared_chunk_manifest_rejects_tamper_scope_and_missing_batches(harness, corruption):
    await storage.append_nppes_evidence_batch(object(), harness.candidate, _buffers())
    binding = deepcopy(harness.state["binding"])
    chunk_values = list(harness.state["prepared_chunks"][0])
    change_by_corruption = {
        "payload": (chunk_values, 5, chunk_values[5] + b" "),
        "digest": (chunk_values, 6, "0" * 64),
        "range": (chunk_values, 1, 2),
        "counts": (chunk_values, 4, storage._encoded(dict.fromkeys(storage.ROOTS, 1))),
        "scope": (binding["policy"], "schema", "other_schema"),
        "producer": (binding, "producer_identity", ["e" * 40, PRODUCER[1]]),
    }
    if corruption in change_by_corruption:
        container, key, changed = change_by_corruption[corruption]
        container[key] = changed

    connection = _manifest_connection(chunk_values, corruption == "missing")
    if corruption is None:
        await harness.verify_manifest(connection, harness.policy, binding)
    else:
        with pytest.raises(NppesPublicEvidenceWriterError):
            await harness.verify_manifest(connection, harness.policy, binding)


@pytest.mark.asyncio
async def test_closed_set_validator_uses_retained_chunks_without_archive_replay(harness, monkeypatch):
    from process import nppes_public_evidence_validation as validation

    assert not any(
        hasattr(validation, name)
        for name in ("NppesPrimaryCsvRows", "NppesRegistryMemberEncoder", "NppesRegistryDatabaseRowEncoder")
    )
    connection = SimpleNamespace(fetchval=AsyncMock(return_value=False))
    tables_by_root = {root: '"source_schema"."' + root + '"' for root in storage.ROOTS}
    await validation._assert_prepared_chunk(connection, tables_by_root, '"source_schema"."prepared"', 1, "admission")
    assert connection.fetchval.await_count == 5
    for call in connection.fetchval.await_args_list:
        assert "jsonb_to_recordset" in call.args[0] and "EXCEPT ALL" in call.args[0]
        assert call.args[1:] == (1, "admission")
    connection.fetchval.return_value = True
    with pytest.raises(NppesPublicEvidenceWriterError):
        await validation._assert_prepared_chunk(
            connection, tables_by_root, '"source_schema"."prepared"', 1, "admission"
        )


def _chunk_catalog_record(policy, name):
    kinds = (
        "bigint",
        "bigint",
        "bigint",
        "text",
        "jsonb",
        *(kind for _root in storage.ROOTS for kind in ("bytea", "text")),
    )
    return dict(
        relname=name,
        nspname=policy.schema,
        relowner=500,
        oid=900,
        relkind="r",
        relpersistence="p",
        parents=[],
        relispartition=False,
        relrowsecurity=False,
        relforcerowsecurity=False,
        inert=True,
        native_checks=True,
        direct_acl_closed=True,
        readers_closed=True,
        columns=[[column, kind, True, None] for column, kind in zip(storage._CHUNK_COLUMNS, kinds, strict=True)],
        constraints=[dict(type="p", valid=True)],
        indexes=[
            dict(
                keys=["chunk_ordinal"],
                primary=True,
                unique=True,
                valid=True,
                method="btree",
                predicate=None,
                expression=None,
            )
        ],
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "corruption", (None, "owner", "direct_acl", "reader", "hook", "rls", "column", "type", "index", "native_type")
)
async def test_prepared_chunk_custody_refuses_owner_acl_hook_and_layout_drift(monkeypatch, corruption):
    policy = GenericSourcePolicy("source_schema", "source_owner", "source_publisher", ("source_reader",))
    name = "npec_" + "a" * 32 + "_p"
    relation_by_field = _chunk_catalog_record(policy, name)
    change_by_corruption = {
        "owner": (relation_by_field, "relowner", 501),
        "direct_acl": (relation_by_field, "direct_acl_closed", False),
        "reader": (relation_by_field, "readers_closed", False),
        "hook": (relation_by_field, "inert", False),
        "rls": (relation_by_field, "relrowsecurity", True),
        "column": (relation_by_field, "columns", relation_by_field["columns"][:-1]),
        "type": (relation_by_field["columns"][0], 1, "text"),
        "index": (relation_by_field["indexes"][0], "unique", False),
    }
    if corruption in change_by_corruption:
        container, key, changed = change_by_corruption[corruption]
        container[key] = changed
    connection = SimpleNamespace(
        fetch=AsyncMock(return_value=[relation_by_field]), fetchval=AsyncMock(return_value=corruption != "native_type")
    )
    monkeypatch.setattr(storage, "storage_roles", AsyncMock(return_value=(500, [600])))
    if corruption is None:
        proof = await storage._capture_chunks(connection, policy, name)
        assert proof["name"] == name and proof["owner"] == 500 and proof["oid"] == 900
        assert connection.fetch.await_args.args[-1] == []
    else:
        with pytest.raises(NppesPublicEvidenceWriterError):
            await storage._capture_chunks(connection, policy, name)
