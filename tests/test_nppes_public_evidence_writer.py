# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded, resumable candidate publication for NPPES evidence admission."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from copy import deepcopy
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import nppes_public_evidence_writer as writer
from process.control_cancel import ImportCancelledError
from process.nppes_public_evidence_members import NppesPrimaryCsvRows
from process.nppes_public_evidence_replay import prepare_nppes_registry_replay
from process.nppes_public_evidence_rows import NppesRegistryDatabaseRowEncoder
from process.nppes_public_evidence_writer import (
    NppesPublicEvidenceWriterError,
    _batch_size,
    _insert_single,
    _invoke,
    _schema_name,
    admit_nppes_registry_archive,
)
from process.provider_directory_generic_source_policy import GenericSourcePolicy
from public_evidence.nppes_registry_storage_contract import (
    NppesRegistryMemberEncoder as RealMemberEncoder,
)
from tests.nppes_public_evidence_process_support import prepared_sized_archive
from tests.test_nppes_public_evidence_replay import _config, _prepared


@pytest.mark.asyncio
async def test_sync_async_and_absent_callbacks_preserve_arguments():
    observed_entries = []
    await _invoke(None)
    await _invoke(observed_entries.append, 7)
    callback = AsyncMock()
    await _invoke(callback, 8)
    assert observed_entries == [7]
    callback.assert_awaited_once_with(8)


def test_schema_and_batch_size_contract(monkeypatch) -> None:
    with pytest.raises(NppesPublicEvidenceWriterError):
        _schema_name("bad-name")

    monkeypatch.delenv("HLTHPRT_NPPES_PUBLIC_EVIDENCE_BATCH_SIZE", raising=False)
    assert _batch_size() == 20_000
    for raw_value in ("not-a-number", "99", "100001"):
        monkeypatch.setenv("HLTHPRT_NPPES_PUBLIC_EVIDENCE_BATCH_SIZE", raw_value)
        with pytest.raises(NppesPublicEvidenceWriterError):
            _batch_size()
    for raw_value, expected in (("100", 100), ("100000", 100_000)):
        monkeypatch.setenv("HLTHPRT_NPPES_PUBLIC_EVIDENCE_BATCH_SIZE", raw_value)
        assert _batch_size() == expected


@pytest.mark.asyncio
@pytest.mark.parametrize("stored_row", (None, {"identity_ref": "different"}))
async def test_identical_insert_replay_rejects_missing_or_different_row(
    stored_row,
) -> None:
    class _InsertConnection:
        async def execute(self, *_args):
            return "INSERT 0 0"

        async def fetchrow(self, *_args):
            return stored_row

    with pytest.raises(NppesPublicEvidenceWriterError):
        await _insert_single(
            _InsertConnection(),
            "mrf",
            "example",
            ("identity_ref",),
            ("expected",),
            allow_identical=True,
        )


@pytest.mark.asyncio
async def test_public_writer_rejects_wrong_config_type(tmp_path) -> None:
    with pytest.raises(NppesPublicEvidenceWriterError):
        await admit_nppes_registry_archive(_prepared(tmp_path), object())


class _NoAmbientDatabase:
    def acquire_driver(self):
        raise AssertionError("ambient driver must not be used")


def _canonical_family(prepared):
    encoder = RealMemberEncoder(prepared.manifest, prepared.header, prepared.archive_observation)
    database = NppesRegistryDatabaseRowEncoder(prepared.manifest.release, prepared.admission_row)
    rows_by_key = {root: [] for root in writer.storage.ROOTS}
    with NppesPrimaryCsvRows(prepared.archive) as source:
        for raw in source:
            encoded, member = encoder.encode(raw)
            rows_by_key["source_record"].append(database.source_record(encoded.source_record))
            rows_by_key["member"].append(database.member(member))
            projected = database.projected(encoded)
            if projected is not None:
                for root, row in zip(("common", "source_link", "typed"), projected):
                    rows_by_key[root].append(row)
    assert encoder.finish() == prepared.manifest
    return rows_by_key


class _CandidateStorage:
    def __init__(self, prepared):
        self.prepared = prepared
        self.producer = ("a" * 40, "sha256:" + "b" * 64)
        self.candidate = writer.storage.NppesEvidenceCandidate(
            writer.storage._identity(prepared), list(self.producer), 0, dict.fromkeys(writer.storage.ROOTS, 0)
        )
        self.rows = {root: [] for root in writer.storage.ROOTS}
        self.events = []
        self.batches = []
        self.published = False
        self.fail_after_commit = False
        self.transaction_depth = 0

    async def prepare(self, database, prepared, *, producer_identity):
        assert isinstance(database, _NoAmbientDatabase)
        assert prepared == self.prepared and producer_identity == self.producer
        self.events.append("prepare")
        return self.candidate

    async def append(self, _database, candidate, buffers, *, cancel_check):
        await writer._invoke(cancel_check)
        assert candidate == self.candidate
        rows, counts, batch = writer.storage._batch(buffers)
        assert batch["start"] == candidate.ordinal + 1
        self.batches.append((rows, counts, batch))
        for root, records in rows.items():
            self.rows[root].extend(records)
        self.candidate = replace(
            candidate,
            ordinal=batch["end"],
            counts={root: candidate.counts[root] + counts[root] for root in counts},
        )
        self.events.append("append")
        if self.fail_after_commit:
            self.fail_after_commit = False
            raise RuntimeError("synthetic unknown commit result")
        await writer._invoke(cancel_check)
        return self.candidate

    async def prepare_publication(self, _database, candidate, prepared, *, cancel_check):
        await writer._invoke(cancel_check)
        assert candidate == self.candidate and prepared == self.prepared
        assert self.rows == _canonical_family(prepared)
        self.events.append("prepare_publication")
        return candidate

    async def publish(self, _database, candidate, prepared, *, cancel_check):
        await writer._invoke(cancel_check)
        assert candidate == self.candidate and prepared == self.prepared
        assert self.transaction_depth > 0
        self.events.append("publish")
        state = "already_present" if self.published else "inserted"
        self.published = True
        self.candidate = replace(candidate, phase="published")
        return state

    def install(self, monkeypatch):
        monkeypatch.setattr(writer.storage, "prepare_nppes_evidence_candidate", self.prepare)
        monkeypatch.setattr(writer.storage, "append_nppes_evidence_batch", self.append)
        monkeypatch.setattr(writer.storage, "prepare_nppes_evidence_publication", self.prepare_publication)
        monkeypatch.setattr(writer.storage, "publish_nppes_evidence_candidate", self.publish)
        monkeypatch.setattr(writer.source_policy, "source_producer_identity", lambda: self.producer)

        @asynccontextmanager
        async def transaction(database):
            assert isinstance(database, _NoAmbientDatabase)
            self.transaction_depth += 1
            try:
                yield
            finally:
                self.transaction_depth -= 1

        publisher = SimpleNamespace(connection=object(), policy=SimpleNamespace(schema="mrf"))
        monkeypatch.setattr(writer, "assert_nppes_candidate_catalog", AsyncMock(), raising=False)
        monkeypatch.setattr(writer.source_policy, "source_transaction", transaction)
        monkeypatch.setattr(writer.source_policy, "bound_publisher", lambda: publisher)


async def _candidate_setup(monkeypatch, tmp_path):
    prepared = await prepare_nppes_registry_replay(_prepared(tmp_path), _config())
    storage = _CandidateStorage(prepared)
    storage.install(monkeypatch)
    return prepared, storage


@pytest.mark.asyncio
async def test_real_route_only_uses_candidate_lifecycle(monkeypatch, tmp_path):
    prepared, storage = await _candidate_setup(monkeypatch, tmp_path)
    progress = []
    for name in (
        "_create_stages",
        "_stage_complete_replay",
        "_insert_stage",
        "_analyze_admission_tables",
        "_finalize",
        "_drop_stages",
        "_drain_stage_cleanup",
        "_copy_stage",
        "_flush_buffers",
        "_assert_stage_counts",
        "_has_existing_admission",
        "_assert_no_partial_release",
        "_assert_catalog",
        "_TABLE_BY_STAGE",
        "_COLUMNS_BY_STAGE",
    ):
        assert not hasattr(writer, name)
    receipt = await admit_nppes_registry_archive(
        prepared, _config(), database=_NoAmbientDatabase(), progress=progress.append
    )
    assert receipt.write_state == "inserted" and receipt.source_record_count == 2
    assert receipt.projected_record_count == receipt.excluded_record_count == 1
    assert repr(receipt) == "<nppes-registry-admission-receipt>"
    assert storage.events == ["prepare", "append", "prepare_publication", "publish"]
    assert progress == [2]
    assert storage.rows == _canonical_family(prepared)


@pytest.mark.asyncio
async def test_no_factory_authority_fails_without_ambient_driver_or_storage(monkeypatch, tmp_path):
    prepared = await prepare_nppes_registry_replay(_prepared(tmp_path), _config())
    prepare = AsyncMock(side_effect=AssertionError("storage called without authority"))
    monkeypatch.setattr(writer.storage, "prepare_nppes_evidence_candidate", prepare)
    with pytest.raises(NppesPublicEvidenceWriterError):
        await admit_nppes_registry_archive(prepared, _config(), database=_NoAmbientDatabase())
    prepare.assert_not_awaited()


@pytest.mark.asyncio
async def test_schema_must_match_trusted_bound_policy(monkeypatch, tmp_path):
    prepared, storage = await _candidate_setup(monkeypatch, tmp_path)
    with pytest.raises(NppesPublicEvidenceWriterError):
        await admit_nppes_registry_archive(prepared, _config(), schema="different", database=_NoAmbientDatabase())
    assert not storage.events


@pytest.mark.asyncio
@pytest.mark.parametrize("failure_type", (asyncio.CancelledError, ImportCancelledError))
async def test_cancellation_before_preparation_never_opens_candidate(monkeypatch, tmp_path, failure_type):
    prepared, storage = await _candidate_setup(monkeypatch, tmp_path)

    def cancel():
        raise failure_type("cancelled")

    with pytest.raises(failure_type):
        await admit_nppes_registry_archive(prepared, _config(), database=_NoAmbientDatabase(), cancel_check=cancel)
    assert not storage.events


@pytest.mark.asyncio
async def test_changed_prepared_manifest_is_rejected_before_storage(monkeypatch, tmp_path):
    prepared, storage = await _candidate_setup(monkeypatch, tmp_path)
    changed = replace(prepared, manifest=replace(prepared.manifest, manifest_sha256="00" * 32))
    with pytest.raises(NppesPublicEvidenceWriterError):
        await admit_nppes_registry_archive(changed, _config(), database=_NoAmbientDatabase())
    assert not storage.events


@pytest.mark.asyncio
async def test_candidate_replay_flushes_configured_batches_with_progress(monkeypatch, tmp_path):
    prepared, storage = await _candidate_setup(monkeypatch, tmp_path)
    monkeypatch.setattr(writer, "_batch_size", lambda: 1)
    progress = []
    await admit_nppes_registry_archive(prepared, _config(), database=_NoAmbientDatabase(), progress=progress.append)
    assert [counts["member"] for _, counts, _ in storage.batches] == [1, 1]
    assert progress == [1, 2] and storage.rows == _canonical_family(prepared)


@pytest.mark.asyncio
@pytest.mark.parametrize("corruption", ("ordinal", "counts"))
async def test_committed_batch_result_requires_exact_accounting(monkeypatch, tmp_path, corruption):
    prepared, storage = await _candidate_setup(monkeypatch, tmp_path)

    async def corrupt_result(*args, **kwargs):
        result = await storage.append(*args, **kwargs)
        return replace(result, **({"ordinal": result.ordinal + 1} if corruption == "ordinal" else {"counts": {}}))

    monkeypatch.setattr(writer.storage, "append_nppes_evidence_batch", corrupt_result)
    with pytest.raises(NppesPublicEvidenceWriterError):
        await admit_nppes_registry_archive(prepared, _config(), database=_NoAmbientDatabase())
    assert storage.events == ["prepare", "append"] and not storage.published


@pytest.mark.asyncio
async def test_partial_resume_skips_exact_canonical_persisted_prefix(monkeypatch, tmp_path):
    prepared, storage = await _candidate_setup(monkeypatch, tmp_path)
    monkeypatch.setattr(writer, "_batch_size", lambda: 1)
    storage.fail_after_commit = True
    with pytest.raises(NppesPublicEvidenceWriterError) as caught:
        await admit_nppes_registry_archive(prepared, _config(), database=_NoAmbientDatabase())
    assert caught.value.__cause__ is caught.value.__context__ is None
    assert storage.candidate.ordinal == 1 and storage.events == ["prepare", "append"]
    progress = []
    receipt = await admit_nppes_registry_archive(
        prepared, _config(), database=_NoAmbientDatabase(), progress=progress.append
    )
    assert receipt.write_state == "inserted" and progress == [1, 2]
    assert [batch[2]["start"] for batch in storage.batches] == [1, 2]
    assert storage.rows == _canonical_family(prepared)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure_type", (asyncio.CancelledError, ImportCancelledError))
async def test_committed_batch_cancellation_preserves_resume_prefix(monkeypatch, tmp_path, failure_type):
    prepared, storage = await _candidate_setup(monkeypatch, tmp_path)
    monkeypatch.setattr(writer, "_batch_size", lambda: 1)

    def cancel():
        if storage.candidate.ordinal:
            raise failure_type("cancelled")

    with pytest.raises(failure_type):
        await admit_nppes_registry_archive(prepared, _config(), database=_NoAmbientDatabase(), cancel_check=cancel)
    assert storage.candidate.ordinal == 1 and not storage.published
    await admit_nppes_registry_archive(prepared, _config(), database=_NoAmbientDatabase())
    assert storage.rows == _canonical_family(prepared)


@pytest.mark.asyncio
async def test_completed_redeploy_reuses_published_family_without_appending(monkeypatch, tmp_path):
    prepared, storage = await _candidate_setup(monkeypatch, tmp_path)
    await admit_nppes_registry_archive(prepared, _config(), database=_NoAmbientDatabase())
    storage.producer = ("c" * 40, "sha256:" + "d" * 64)
    monkeypatch.setattr(writer, "NppesRegistryMemberEncoder", lambda *_args: pytest.fail("complete family replayed"))
    storage.events.clear()
    receipt = await admit_nppes_registry_archive(prepared, _config(), database=_NoAmbientDatabase())
    assert receipt.write_state == "already_present"
    assert storage.events == ["prepare"]
    assert len(storage.batches) == 1


def _published_replay_connection(prepared, binding_by_key, controls_by_key, corruption):
    queries = []

    async def fetchrow(query, *_args):
        assert query.startswith("SELECT") and "FOR UPDATE" not in query and "advisory" not in query
        queries.append(query)
        if f'"{writer.storage.HEADER}"' in query:
            return dict(
                admission_ref=prepared.admission_row.admission_ref,
                source_release_ref=prepared.manifest.release.source_release_ref,
                candidate_storage_json=deepcopy(binding_by_key),
            )
        return next(deepcopy(row) for table, row in controls_by_key.items() if f'"{table}"' in query)

    connection = SimpleNamespace(
        fetchrow=fetchrow,
        fetchval=AsyncMock(return_value=corruption != "seal"),
        execute=AsyncMock(side_effect=AssertionError("read-only transaction")),
    )

    return connection, queries


def _published_replay_binding(prepared, policy):
    family = _canonical_family(prepared)
    buffers = writer._StageBuffers()
    for root, prepared_records in family.items():
        getattr(buffers, writer.storage.ROOTS[root][2]).extend(
            writer.prepare_nppes_row(root, field_values) for field_values in prepared_records
        )
    _, counts, batch = writer.storage._batch(buffers)
    proofs_by_key = {
        root: dict(name="npec_" + "a" * 32 + "_" + fields[3], oid=index + 100, owner=500, layout_sha256="c" * 64)
        for index, (root, fields) in enumerate(writer.storage.ROOTS.items())
    }
    binding_by_key = dict(
        contract=writer.storage.CONTRACT,
        identity=writer.storage._identity(prepared),
        producer_identity=["a" * 40, "sha256:" + "b" * 64],
        policy=writer.storage._policy_fields(policy),
        phase="published",
        ordinal=len(family["member"]),
        counts=counts,
        last_batch=batch,
        tables=deepcopy(proofs_by_key),
        validation_sha256=None,
        chunks=dict(table=None, count=1, sha256="d" * 64),
    )
    controls_by_key = {}
    for table, columns, field_values in (
        (
            "public_evidence_source_identity",
            writer.storage.SOURCE_IDENTITY_COLUMNS,
            writer.storage.source_identity_values(prepared.manifest.release),
        ),
        (
            "public_evidence_source_release",
            writer.storage.SOURCE_RELEASE_COLUMNS,
            writer.storage.source_release_values(prepared.manifest.release),
        ),
        (
            "public_evidence_nppes_registry_admission",
            writer.storage.ADMISSION_COLUMNS,
            writer.storage.admission_values(prepared.admission_row),
        ),
    ):
        controls_by_key[table] = dict(zip(columns, field_values, strict=True))
    return binding_by_key, controls_by_key, proofs_by_key


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "corruption",
    (None, "identity", "policy", "physical", "artifact", "release", "admission", "seal", "counts", "validation"),
)
async def test_public_published_replay_enforces_read_only_and_reproves_binding(monkeypatch, tmp_path, corruption):
    prepared = await prepare_nppes_registry_replay(_prepared(tmp_path), _config())
    policy = GenericSourcePolicy("mrf", "source_owner", "source_publisher", ("source_reader",))
    binding_by_key, controls_by_key, proofs_by_key = _published_replay_binding(prepared, policy)
    match corruption:
        case "identity":
            binding_by_key["identity"]["manifest_sha256"] = "00" * 32
        case "policy":
            binding_by_key["policy"]["owner_role"] = "different_owner"
        case "physical":
            binding_by_key["tables"]["member"]["oid"] += 1
        case "artifact" | "release" | "admission":
            table, column = {
                "artifact": ("public_evidence_source_identity", "content_sha256"),
                "release": ("public_evidence_source_release", "import_run_ref"),
                "admission": ("public_evidence_nppes_registry_admission", "source_record_count"),
            }[corruption]
            controls_by_key[table][column] = "changed"
        case "counts":
            for root in ("common", "source_link", "typed"):
                binding_by_key["counts"][root] = binding_by_key["last_batch"]["counts"][root] = 0
    binding_by_key["validation_sha256"] = writer.storage._validation_fingerprint(binding_by_key)
    if corruption == "validation":
        binding_by_key["validation_sha256"] = "00" * 32
    connection, queries = _published_replay_connection(prepared, binding_by_key, controls_by_key, corruption)

    @asynccontextmanager
    async def transaction(database):
        yield

    @asynccontextmanager
    async def storage_transaction(database):
        yield connection, policy

    monkeypatch.setattr(writer.source_policy, "source_transaction", transaction)
    monkeypatch.setattr(
        writer.source_policy, "bound_publisher", lambda: SimpleNamespace(connection=connection, policy=policy)
    )
    monkeypatch.setattr(writer, "assert_nppes_candidate_catalog", AsyncMock(), raising=False)
    monkeypatch.setattr(writer.source_policy, "source_producer_identity", lambda: ("d" * 40, "sha256:" + "e" * 64))
    monkeypatch.setattr(writer.storage, "_transaction", storage_transaction)
    monkeypatch.setattr(writer.storage, "_header_custody", AsyncMock())
    capture = AsyncMock(return_value=proofs_by_key)
    monkeypatch.setattr(writer.storage, "_capture", capture)
    monkeypatch.setattr(writer, "NppesRegistryMemberEncoder", lambda *_args: pytest.fail("published archive replayed"))
    if corruption is None:
        receipt = await admit_nppes_registry_archive(prepared, _config(), database=_NoAmbientDatabase())
        assert receipt.write_state == "already_present"
        capture.assert_awaited_once()
        assert len(queries) == 4
    else:
        with pytest.raises(NppesPublicEvidenceWriterError):
            await admit_nppes_registry_archive(prepared, _config(), database=_NoAmbientDatabase())
    connection.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_published_fast_path_still_honors_cancellation(monkeypatch, tmp_path):
    prepared, storage = await _candidate_setup(monkeypatch, tmp_path)
    await admit_nppes_registry_archive(prepared, _config(), database=_NoAmbientDatabase())
    storage.events.clear()
    cancel = AsyncMock(side_effect=[None, ImportCancelledError("cancelled")])
    with pytest.raises(ImportCancelledError):
        await admit_nppes_registry_archive(prepared, _config(), database=_NoAmbientDatabase(), cancel_check=cancel)
    assert storage.events == ["prepare"]


@pytest.mark.asyncio
@pytest.mark.parametrize("corruption", ("prefix_projection", "complete_counts", "negative", "extra_root"))
async def test_inconsistent_committed_prefix_counts_fail_closed(monkeypatch, tmp_path, corruption):
    prepared, storage = await _candidate_setup(monkeypatch, tmp_path)
    counts = dict.fromkeys(writer.storage.ROOTS, 0)
    ordinal = 1
    counts.update(source_record=1, member=1)
    if corruption == "complete_counts":
        ordinal = 2
        counts.update(source_record=2, member=2)
    elif corruption == "negative":
        ordinal = -1
    elif corruption == "extra_root":
        counts["unexpected"] = 0
    storage.candidate = replace(storage.candidate, ordinal=ordinal, counts=counts)
    with pytest.raises(NppesPublicEvidenceWriterError):
        await admit_nppes_registry_archive(prepared, _config(), database=_NoAmbientDatabase())
    assert storage.events == ["prepare"] and not storage.batches


@pytest.mark.asyncio
async def test_native_batch_row_limit_caps_large_configured_batch(monkeypatch, tmp_path):
    archive = prepared_sized_archive(tmp_path, "NPPES_Data_Dissemination_July_2026_V2.zip", "20260713", 4097)
    prepared = await prepare_nppes_registry_replay(archive, _config())
    storage = _CandidateStorage(prepared)
    storage.install(monkeypatch)
    monkeypatch.setattr(writer, "_batch_size", lambda: 100_000)
    await admit_nppes_registry_archive(prepared, _config(), database=_NoAmbientDatabase())
    assert [counts["member"] for _, counts, _ in storage.batches] == [4096, 1]


@pytest.mark.asyncio
async def test_aggregate_byte_limit_flushes_and_rejects_one_oversized_family(monkeypatch, tmp_path):
    prepared, storage = await _candidate_setup(monkeypatch, tmp_path)
    canonical = _canonical_family(prepared)
    maximum = (
        writer.storage.COPY_FRAME_BYTES
        + 2 * len(writer.storage.ROOTS)
        + sum(
            max(writer.storage._record_bytes(rows[0]), len(writer.prepare_nppes_row(root, rows[0]).payload) + 1)
            for root, rows in canonical.items()
        )
    )
    monkeypatch.setattr(writer.storage, "COPY_MAX_BYTES", maximum)
    await admit_nppes_registry_archive(prepared, _config(), database=_NoAmbientDatabase())
    assert [counts["member"] for _, counts, _ in storage.batches] == [1, 1]
    for rows, _, _ in storage.batches:
        size = writer.storage.COPY_FRAME_BYTES + sum(
            writer.storage._record_bytes(row) for records in rows.values() for row in records
        )
        assert size <= maximum
    storage = _CandidateStorage(prepared)
    storage.install(monkeypatch)
    monkeypatch.setattr(writer.storage, "COPY_MAX_BYTES", maximum - 1)
    with pytest.raises(NppesPublicEvidenceWriterError):
        await admit_nppes_registry_archive(prepared, _config(), database=_NoAmbientDatabase())
    assert not storage.batches


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "operation,events",
    [
        ("prepare_nppes_evidence_candidate", []),
        ("append_nppes_evidence_batch", ["prepare"]),
        ("prepare_nppes_evidence_publication", ["prepare", "append"]),
        ("publish_nppes_evidence_candidate", ["prepare", "append", "prepare_publication"]),
    ],
)
async def test_storage_failure_is_normalized_without_advancing_to_publication(monkeypatch, tmp_path, operation, events):
    prepared, storage = await _candidate_setup(monkeypatch, tmp_path)
    monkeypatch.setattr(writer.storage, operation, AsyncMock(side_effect=RuntimeError("synthetic failure")))
    with pytest.raises(NppesPublicEvidenceWriterError) as caught:
        await admit_nppes_registry_archive(prepared, _config(), database=_NoAmbientDatabase())
    assert str(caught.value) == "nppes_public_evidence_writer_invalid"
    assert caught.value.__cause__ is caught.value.__context__ is None
    assert storage.events == events and not storage.published


@pytest.mark.asyncio
@pytest.mark.parametrize("corruption", ("header", "manifest"))
async def test_candidate_replay_requires_exact_header_and_finished_manifest(monkeypatch, tmp_path, corruption):
    prepared, storage = await _candidate_setup(monkeypatch, tmp_path)
    if corruption == "header":

        class WrongHeader:
            header = ("wrong",)

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return None

        monkeypatch.setattr(writer, "NppesPrimaryCsvRows", lambda _archive: WrongHeader())
    else:

        class WrongManifest:
            def __init__(self, *args):
                self.encoder = RealMemberEncoder(*args)

            def encode(self, raw):
                return self.encoder.encode(raw)

            def finish(self):
                return replace(self.encoder.finish(), manifest_sha256="00" * 32)

        monkeypatch.setattr(writer, "NppesRegistryMemberEncoder", WrongManifest)
    with pytest.raises(NppesPublicEvidenceWriterError):
        await admit_nppes_registry_archive(prepared, _config(), database=_NoAmbientDatabase())
    assert storage.events == ["prepare"] and not storage.batches


@pytest.mark.asyncio
async def test_progress_failure_after_complete_commit_resumes_directly_to_publication(monkeypatch, tmp_path):
    prepared, storage = await _candidate_setup(monkeypatch, tmp_path)

    def fail_progress(_ordinal):
        raise RuntimeError("synthetic progress failure")

    with pytest.raises(NppesPublicEvidenceWriterError):
        await admit_nppes_registry_archive(prepared, _config(), database=_NoAmbientDatabase(), progress=fail_progress)
    assert storage.candidate.ordinal == prepared.manifest.source_record_count and not storage.published
    storage.events.clear()
    monkeypatch.setattr(writer, "NppesRegistryMemberEncoder", lambda *_args: pytest.fail("complete prefix replayed"))
    receipt = await admit_nppes_registry_archive(prepared, _config(), database=_NoAmbientDatabase())
    assert receipt.write_state == "inserted"
    assert storage.events == ["prepare", "prepare_publication", "publish"]
    assert len(storage.batches) == 1


@pytest.mark.asyncio
async def test_unknown_publication_commit_retries_as_already_present(monkeypatch, tmp_path):
    prepared, storage = await _candidate_setup(monkeypatch, tmp_path)

    async def publish_then_lose_result(*args, **kwargs):
        await storage.publish(*args, **kwargs)
        raise RuntimeError("synthetic lost publication result")

    monkeypatch.setattr(writer.storage, "publish_nppes_evidence_candidate", publish_then_lose_result)
    with pytest.raises(NppesPublicEvidenceWriterError):
        await admit_nppes_registry_archive(prepared, _config(), database=_NoAmbientDatabase())
    assert storage.published
    monkeypatch.setattr(writer.storage, "publish_nppes_evidence_candidate", storage.publish)
    receipt = await admit_nppes_registry_archive(prepared, _config(), database=_NoAmbientDatabase())
    assert receipt.write_state == "already_present" and len(storage.batches) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("boundary", ("entry", "replay", "publication"))
async def test_archive_controls_are_rechecked_at_each_original_source_boundary(monkeypatch, tmp_path, boundary):
    prepared, storage = await _candidate_setup(monkeypatch, tmp_path)
    database = _NoAmbientDatabase()
    if boundary == "replay":
        await admit_nppes_registry_archive(prepared, _config(), database=database)
    storage.events.clear()
    check_calls = []

    async def refuse_changed_control(connection, schema, policy):
        publisher = writer.source_policy.bound_publisher()
        assert connection is publisher.connection and policy is publisher.policy and schema == "mrf"
        assert storage.transaction_depth > 0
        check_calls.append(connection)
        if len(check_calls) == (1 if boundary == "entry" else 2):
            raise RuntimeError("synthetic changed control guard")

    monkeypatch.setattr(writer, "assert_nppes_candidate_catalog", refuse_changed_control, raising=False)
    with pytest.raises(NppesPublicEvidenceWriterError):
        await admit_nppes_registry_archive(prepared, _config(), database=database)
    assert "publish" not in storage.events
    assert len(check_calls) == (1 if boundary == "entry" else 2)
    if boundary == "entry":
        assert storage.events == []
    elif boundary == "replay":
        assert storage.events == ["prepare"]
    else:
        assert storage.events[-1] == "prepare_publication"
