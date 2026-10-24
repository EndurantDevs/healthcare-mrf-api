# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Mocked metadata paging and shared lease gates; no native CAS or fencing proof."""

from __future__ import annotations

import datetime as dt
import hashlib
import json
from contextlib import asynccontextmanager
from copy import copy
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.dialects.postgresql import dialect

from process.custom_import import prepared_source_seal as sealing
from process.custom_import import prepared_source_store as store
from process.custom_import import runner_registry
from process.custom_import.capture_store import CaptureBundleConflict
from process.custom_import.definition import canonical_json
from process.custom_import.execution import LeaseGrant, lease_token_sha256
from process.custom_import.prepared_source_context import PreparedSourceContext
from process.custom_import.prepared_source_metadata import PreparedPartMetadata, PreparedSourceMetadataError
from process.custom_import.prepared_source_progress import PreparedStreamProgress, canonical_progress, totals
from process.custom_import.runner_types import CandidateRunnerError, LeaseAuthorityLost
from tests.test_custom_import_prepared_source_context import _retained_rows


def _context(retained):
    loaded = retained.loaded
    return PreparedSourceContext(
        dataset_id=loaded.dataset_id,
        definition_revision_id=loaded.definition_revision_id,
        schema_revision_id=loaded.schema_revision_id,
        execution_id=retained.execution.execution_id,
        capture_bundle_id=retained.bundle.capture_bundle_id,
        source_binding_revision_id=loaded.source_binding_revision_id,
        source_binding_sha256=loaded.source_binding_sha256,
        request_identity_sha256=retained.execution.request_identity_sha256,
        raw_manifest_sha256=retained.bundle.manifest_sha256,
        definition=loaded.definition,
        policy=loaded.binding.processing_policy,
        stream_slots=tuple((stream.stream_id, slot) for slot, stream in enumerate(loaded.definition.source_streams, 1)),
        fence=7,
        lease_token=retained.token,
        build_deadline_at=retained.execution.started_at + dt.timedelta(seconds=3600),
        source_permit=retained.permit,
    )


def _part_rows(context, count):
    parts = [PreparedPartMetadata(1, ordinal, ordinal, 1, 10, b"p" * 32) for ordinal in range(1, count + 1)]
    parts.append(PreparedPartMetadata(2, 1, 1, 0, 0, hashlib.sha256(b"").digest()))
    return [
        (
            context.dataset_id,
            context.definition_revision_id,
            context.schema_revision_id,
            part.stream_slot,
            part.part_ordinal,
            part.first_source_ordinal,
            part.record_count,
            part.byte_count,
            part.payload_sha256,
            sealing.PREPARED_SOURCE_CONTRACT,
            part.canonical,
            part.digest,
        )
        for part in parts
    ]


def _closed_header(context, metadata_rows):
    progress = tuple(
        PreparedStreamProgress(
            slot,
            len(selected_rows),
            sum(part_row[6] for part_row in selected_rows),
            sum(part_row[7] for part_row in selected_rows),
            sum(len(part_row[10].encode()) for part_row in selected_rows),
            True,
        )
        for slot in (1, 2)
        if (selected_rows := [part_row for part_row in metadata_rows if part_row[3] == slot])
    )
    part_count, record_count, byte_count, manifest_byte_count = totals(progress)
    return SimpleNamespace(
        **vars(store.prepared_source_identity(context)),
        canonical_processing_policy=context.policy.canonical,
        encoding_contract=sealing.PREPARED_SOURCE_CONTRACT,
        preparation_deadline_at=context.build_deadline_at,
        state="closed",
        version=4,
        producer_fence=context.fence,
        producer_token_sha256=lease_token_sha256(context.lease_token),
        canonical_progress=canonical_progress(progress),
        part_count=part_count,
        record_count=record_count,
        byte_count=byte_count,
        manifest_byte_count=manifest_byte_count,
        canonical_summary=None,
        prepared_sha256=None,
        sealed_at=None,
    )


class _SealHarness:
    def __init__(self, monkeypatch, count=1):
        retained = _retained_rows()
        self.context, self.execution = _context(retained), retained.execution
        self.rows = _part_rows(self.context, count)
        self.header = _closed_header(self.context, self.rows)
        self.now = retained.permit.expires_at - dt.timedelta(seconds=30)
        self.lease = SimpleNamespace(
            fence=self.context.fence,
            token_sha256=lease_token_sha256(retained.token),
            expires_at=self.now + dt.timedelta(minutes=2),
        )
        self.events, self.queries, self.quotas = [], [], []
        self.authority_open = 0
        self.header_reads = 0
        self.header_hook = lambda: None
        self.flush_hook = lambda: None
        self.undeclared = False
        self.quota_error = None
        self._install(monkeypatch)

    def _install(self, monkeypatch):
        staging = sealing.staging
        monkeypatch.setattr(staging, "_set_timeout", AsyncMock())
        monkeypatch.setattr(staging, "_prepare_statement", AsyncMock())
        monkeypatch.setattr(staging, "_initial_page_window", AsyncMock(side_effect=lambda *_: self.lease.expires_at))
        monkeypatch.setattr(staging, "lock_dataset", self.lock_dataset)
        monkeypatch.setattr(staging, "lock_execution", AsyncMock(side_effect=lambda *_: self.execution))
        monkeypatch.setattr(staging, "lock_lease", AsyncMock(side_effect=lambda *_: self.lease))
        monkeypatch.setattr(staging, "heartbeat_execution", self.heartbeat)
        monkeypatch.setattr(staging, "_flush_page", self.flush)
        monkeypatch.setattr(runner_registry, "database_now", AsyncMock(side_effect=lambda *_: self.now))
        monkeypatch.setattr(store, "require_prepared_source_context", AsyncMock(side_effect=self.require_context))
        monkeypatch.setattr(sealing, "_quota", self.quota)

    @asynccontextmanager
    async def sessions(self):
        session = SimpleNamespace(
            info={},
            new=(),
            dirty=(),
            deleted=(),
            authority=False,
            in_transaction=lambda: True,
            execute=self.execute,
            scalar=self.scalar,
        )

        @asynccontextmanager
        async def transaction():
            before = copy(self.header)
            self.events.append("begin")
            try:
                yield
            except BaseException:
                self.header.__dict__.update(vars(before))
                self.events.append("rollback")
                raise
            else:
                self.events.append("commit")
            finally:
                if session.authority:
                    self.authority_open -= 1

        session.begin = transaction
        yield session

    async def lock_dataset(self, session, dataset_id):
        assert dataset_id == self.context.dataset_id and not self.authority_open
        session.authority = True
        self.authority_open += 1
        self.events.append("authority")

    async def require_context(self, session, context):
        assert session.authority and context is self.context
        self.events.append("context")

    async def heartbeat(self, _session, **pins):
        assert self.authority_open and pins["fence"] == self.context.fence
        self.events.append("heartbeat")
        return LeaseGrant(self.context.execution_id, self.context.fence, self.lease.expires_at, "running")

    async def flush(self, _session):
        self.events.append("flush")
        if self.header.state == "sealed":
            self.flush_hook()

    async def quota(self, _session, context, extra):
        assert self.authority_open and self.header.state == "closed" and context is self.context
        self.quotas.append(extra)
        self.events.append("quota")
        if self.quota_error:
            raise self.quota_error

    async def execute(self, statement):
        columns = tuple(statement.selected_columns.keys())
        if columns == ("clock_timestamp",):
            return SimpleNamespace(scalar_one=lambda: self.now)
        assert not self.authority_open
        compiled = statement.compile(dialect=dialect())
        self.queries.append(statement)
        self.events.append("metadata")
        parameters = compiled.params
        assert parameters["execution_id_1"] == self.context.execution_id
        assert parameters["param_1"] == 256
        selected_rows = [
            row for row in self.rows if row[3] == parameters["stream_slot_1"] and row[4] > parameters["part_ordinal_1"]
        ]
        return SimpleNamespace(all=lambda: selected_rows[:256])

    async def scalar(self, statement):
        if statement.get_final_froms():
            table = statement.get_final_froms()[0].name
            if table == "custom_import_prepared_source":
                assert self.authority_open and statement._for_update_arg is not None
                assert statement.get_execution_options()["populate_existing"]
                self.header_reads += 1
                self.header_hook()
                self.events.append("header")
                return self.header
            assert table == "custom_import_prepared_source_part" and not self.authority_open
            self.events.append("undeclared")
            parameters = statement.compile(dialect=dialect()).params
            assert parameters["stream_slot_1"] == [1, 2] and parameters["param_1"] == 1
            return self.context.execution_id if self.undeclared else None
        return self.now


@pytest.mark.asyncio
@pytest.mark.parametrize("count", (256, 257))
async def test_seal_keysets_metadata_outside_authority_and_renews_each_page(monkeypatch, count):
    case = _SealHarness(monkeypatch, count)
    digest = await sealing.seal_prepared_source(case.sessions, case.context)
    assert case.header.state == "sealed" and case.header.version == 5
    assert digest == case.header.prepared_sha256
    assert case.quotas == [len(case.header.canonical_summary.encode())]
    assert case.events.count("heartbeat") == 5
    assert case.header_reads == 5 and case.authority_open == 0
    assert case.events[-4:] == ["header", "quota", "flush", "commit"]
    assert [statement.compile().params["part_ordinal_1"] for statement in case.queries] == [0, 256, 0]
    for statement in case.queries:
        names = tuple(statement.selected_columns.keys())
        assert names == (
            "dataset_id",
            "definition_revision_id",
            "schema_revision_id",
            "stream_slot",
            "part_ordinal",
            "first_source_ordinal",
            "record_count",
            "byte_count",
            "payload_sha256",
            "payload_contract",
            "canonical_manifest",
            "manifest_sha256",
        )
        assert statement._for_update_arg is None
        sql = str(statement.compile(dialect=dialect(), compile_kwargs={"literal_binds": True}))
        assert "LIMIT 256" in sql and "OFFSET" not in sql and ".payload," not in sql


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "position,value",
    (
        (0, 99),
        (1, 99),
        (2, 99),
        (3, 2),
        (4, 2),
        (5, 2),
        (6, 2),
        (7, 11),
        (8, b"q" * 32),
        (9, "other/v1"),
        (10, "{}"),
        (11, b"q" * 32),
    ),
)
async def test_seal_refuses_changed_part_identity_manifest_or_order(monkeypatch, position, value):
    case = _SealHarness(monkeypatch)
    changed_fields = list(case.rows[0])
    changed_fields[position] = value
    case.rows[0] = tuple(changed_fields)
    with pytest.raises((CandidateRunnerError, PreparedSourceMetadataError)):
        await sealing.seal_prepared_source(case.sessions, case.context)
    assert case.header.state == "closed" and not case.quotas


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault", ("count", "undeclared", "version_between_pages", "version_before_seal", "producer_before_seal")
)
async def test_seal_refuses_changed_closed_snapshot_before_publication(monkeypatch, fault):
    case = _SealHarness(monkeypatch, 257)
    if fault == "count":
        case.rows.pop(256)
    elif fault == "undeclared":
        case.undeclared = True
    else:

        def change_header():
            target = 3 if fault == "version_between_pages" else 5
            if case.header_reads == target:
                field = "producer_fence" if fault == "producer_before_seal" else "version"
                setattr(case.header, field, getattr(case.header, field) + 1)

        case.header_hook = change_header
    with pytest.raises((CandidateRunnerError, PreparedSourceMetadataError)):
        await sealing.seal_prepared_source(case.sessions, case.context)
    assert case.header.state == "closed" and not case.quotas
    if fault == "version_between_pages":
        assert len(case.queries) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ("fence", "token", "lease", "authorization", "deadline"))
async def test_shared_final_gate_rolls_back_seal_when_authority_changes(monkeypatch, fault):
    case = _SealHarness(monkeypatch)

    def expire():
        if fault == "fence":
            case.lease.fence += 1
        elif fault == "token":
            case.lease.token_sha256 = b"x" * 32
        elif fault == "lease":
            case.lease.expires_at = case.now
        else:
            case.now = (
                case.context.authorization_expires_at if fault == "authorization" else case.context.build_deadline_at
            )
            case.lease.expires_at = case.now + dt.timedelta(seconds=60)

    case.flush_hook = expire
    with pytest.raises(LeaseAuthorityLost):
        await sealing.seal_prepared_source(case.sessions, case.context)
    assert case.quotas and case.events[-1] == "rollback" and case.authority_open == 0
    assert (case.header.state, case.header.version, case.header.canonical_summary, case.header.prepared_sha256) == (
        "closed",
        4,
        None,
        None,
    )


@pytest.mark.asyncio
async def test_quota_refusal_precedes_any_sealed_header_mutation(monkeypatch):
    case = _SealHarness(monkeypatch)
    case.quota_error = CaptureBundleConflict("synthetic quota")
    with pytest.raises(CaptureBundleConflict):
        await sealing.seal_prepared_source(case.sessions, case.context)
    assert case.events[-2:] == ["quota", "rollback"]
    assert case.header.state == "closed" and case.header.version == 4 and case.header.canonical_summary is None


@pytest.mark.asyncio
@pytest.mark.parametrize("new_fence", (False, True))
async def test_sealed_replay_returns_exact_digest_without_metadata_reads_or_quota(monkeypatch, new_fence):
    case = _SealHarness(monkeypatch)
    expected = await sealing.seal_prepared_source(case.sessions, case.context)
    case.events.clear()
    case.queries.clear()
    case.quotas.clear()
    if new_fence:
        case.context = replace(case.context, fence=case.context.fence + 1)
        case.lease.fence = case.context.fence
    assert await sealing.seal_prepared_source(case.sessions, case.context) == expected
    assert not case.queries and not case.quotas and "undeclared" not in case.events
    assert case.header.version == 5


def _change_summary(canonical, fault):
    document = json.loads(canonical)
    if fault == "empty":
        return "{}"
    if fault == "deep":
        return "[" * 2000 + "0" + "]" * 2000
    if fault == "key_order":
        return json.dumps(dict(reversed(tuple(document.items()))), separators=(",", ":"))
    if fault == "identity":
        document["dataset_id"] += 1
    if fault == "counter":
        document["streams"][0]["byte_count"] += 1
    if fault == "missing":
        del document["streams"][0]["byte_count"]
    if fault == "null":
        document["streams"][0]["byte_count"] = None
    if fault == "contract":
        document["contract"] = "other/v1"
    if fault == "extra":
        document["extra"] = 1
    if fault == "reordered":
        document["streams"].reverse()
    if fault == "part_digest":
        document["streams"][0]["part_set_sha256"] = "A" * 64
    changed = canonical_json(document)
    return changed + "\n" if fault == "noncanonical" else changed


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault",
    (
        "empty",
        "identity",
        "counter",
        "missing",
        "null",
        "contract",
        "extra",
        "reordered",
        "key_order",
        "noncanonical",
        "deep",
        "part_digest",
        "digest",
    ),
)
async def test_sealed_replay_refuses_changed_summary_even_with_recomputed_hash(monkeypatch, fault):
    case = _SealHarness(monkeypatch)
    await sealing.seal_prepared_source(case.sessions, case.context)
    case.queries.clear()
    if fault == "digest":
        case.header.prepared_sha256 = b"x" * 32
    else:
        case.header.canonical_summary = _change_summary(case.header.canonical_summary, fault)
        case.header.prepared_sha256 = hashlib.sha256(
            sealing.PREPARED_SOURCE_CONTRACT.encode() + b":" + case.header.canonical_summary.encode()
        ).digest()
    with pytest.raises(CandidateRunnerError):
        await sealing.seal_prepared_source(case.sessions, case.context)
    assert not case.queries and case.events[-1] == "rollback"
