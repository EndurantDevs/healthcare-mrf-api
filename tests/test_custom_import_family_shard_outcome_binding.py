# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Outcome binding with genuine producers and explicit native transport doubles."""

import importlib.util
import sqlite3
from contextlib import asynccontextmanager
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import BigInteger, CheckConstraint

from db.models.custom_import_family_block import CustomImportFamilyBlockLayout
from process.custom_import import family_shard_outcome_binding as binding
from process.custom_import.build_counts import SourceOutcomeCounts
from process.custom_import.continuation_runtime import BuildContinuationContext
from process.custom_import.family_block_binding import FamilyBlockUnavailable
from process.custom_import.family_block_layout import FamilyBlockLayout
from process.custom_import.runner_types import CandidateRunnerError, LeaseAuthorityLost
from tests.test_custom_import_build_graph import _registry, _request
from tests.test_custom_import_family_shard_source_completion import _completion_case


class _NativeBinding:
    """Fake native transaction only; the SOURCE producers remain genuine."""

    def __init__(self, context, receipt):
        self.descriptor = SimpleNamespace(
            source_accepted_family_count=None, source_rejection_count=None, source_record_count=991
        )
        self.build = SimpleNamespace(
            phase="admission",
            source_frozen_at=object(),
            source_occurrence_count=context.source.source.counts[0],
            capture_bundle_id=dict(context.source.source.scope[0])["capture_bundle_id"],
        )
        self.committed, self.entered, self.fail_commit = receipt, 0, False
        self.native_binding = SimpleNamespace(tables_by_role=context.layout.tables(context.source.snapshot_family_id))

    @asynccontextmanager
    async def page(self, *_args):
        self.entered += 1
        before = vars(self.descriptor).copy()
        try:
            yield object(), self.build
        except BaseException:
            vars(self.descriptor).update(before)
            raise
        if self.fail_commit:
            raise ConnectionError("synthetic SOURCE outcome commit reply lost")

    async def candidate(self, *_args):
        return self.descriptor, self.native_binding

    async def execute(self, *_args):
        return SimpleNamespace(mappings=lambda: SimpleNamespace(one_or_none=lambda: self.committed))

    def install(self, monkeypatch):
        monkeypatch.setattr(binding, "_page_session", self.page)
        monkeypatch.setattr(binding.store, "_candidate", self.candidate)
        monkeypatch.setattr(binding.store, "_execute", self.execute)


@pytest.mark.parametrize("empty", [False, True])
async def test_bind_actual_outcomes_after_frozen_source_and_exact_retry(tmp_path, monkeypatch, empty):
    async with _completion_case(tmp_path, monkeypatch, empty=empty) as (context, admission, common, receipt, _limits):
        from process.custom_import.family_shard_counts import count_admitted_source_outcomes
        from process.custom_import.family_shard_plan import PlanWork

        counts = count_admitted_source_outcomes(
            context, admission, work=PlanWork(tmp_path / "counts", _limits.spool, lambda _p: None)
        )
        native = _NativeBinding(context, receipt)
        native.install(monkeypatch)
        for _retry in range(2):
            assert await binding.bind_source_outcomes(None, context, 7, admission, common, receipt, counts) is counts
        assert binding._read_pair(native.descriptor) == counts
        assert native.descriptor.source_record_count == 991
        wrong = replace(counts, rejection_count=counts.rejection_count + 1)
        with pytest.raises(FamilyBlockUnavailable, match="retry counts"):
            await binding.bind_source_outcomes(None, context, 7, admission, common, receipt, wrong)
        assert binding._read_pair(native.descriptor) == counts


@pytest.mark.parametrize("damage", ["unfrozen", "count", "capture", "phase", "receipt", "partial", "scope"])
async def test_binding_refuses_incomplete_or_foreign_native_evidence(tmp_path, monkeypatch, damage):
    async with _completion_case(tmp_path, monkeypatch) as (context, admission, common, receipt, _limits):
        native = _NativeBinding(context, receipt)
        native.install(monkeypatch)
        match damage:
            case "unfrozen":
                native.build.source_frozen_at = None
            case "count":
                native.build.source_occurrence_count += 1
            case "capture":
                native.build.capture_bundle_id += 1
            case "phase":
                native.build.phase = "source"
            case "receipt":
                native.committed = receipt | {"result_sha256": b"x" * 32}
            case "partial":
                native.descriptor.source_accepted_family_count = 1
            case "scope":
                admission = replace(admission, allocation_sha256=b"x" * 32)
        with pytest.raises(FamilyBlockUnavailable):
            await binding.bind_source_outcomes(None, context, 7, admission, common, receipt, SourceOutcomeCounts(0, 1))


async def test_uncertain_binding_commit_replays_same_pair_and_cancellation_rolls_back(tmp_path, monkeypatch):
    async with _completion_case(tmp_path, monkeypatch) as (context, admission, common, receipt, _limits):
        native = _NativeBinding(context, receipt)
        native.install(monkeypatch)
        counts = SourceOutcomeCounts(0, 1)
        native.fail_commit = True
        with pytest.raises(ConnectionError, match="reply lost"):
            await binding.bind_source_outcomes(None, context, 7, admission, common, receipt, counts)
        native.fail_commit = False
        await binding.bind_source_outcomes(None, context, 7, admission, common, receipt, counts)
        native.descriptor.source_accepted_family_count = native.descriptor.source_rejection_count = None
        calls = iter((0, 1))

        def check():
            if next(calls) == 1:
                raise InterruptedError("stop before SOURCE outcome commit")

        with pytest.raises(InterruptedError, match="before SOURCE"):
            await binding.bind_source_outcomes(None, context, 7, admission, common, receipt, counts, check=check)
        assert native.descriptor.source_accepted_family_count is native.descriptor.source_rejection_count is None


@pytest.mark.parametrize("pair", [(-1, 0), (0, -1), (True, 0), (0, 2**63)])
def test_invalid_outcome_integer_domains_refuse(pair):
    with pytest.raises(ValueError):
        binding._pair(SourceOutcomeCounts(*pair))


class _Terminal:
    """Fake publication/native transport, retaining real request identity checks."""

    def __init__(self, *, no_change=False, continued=False):
        context = BuildContinuationContext(6, 7, b"p" * 32) if continued else None
        self.request = _request(expected_base_generation_id=5, expected_pointer_version=2, continuation=context)
        self.registry = _registry(self.request.definition)
        self.layout = FamilyBlockLayout(self.request.definition, self.registry.child_collection_slots)
        self.execution = SimpleNamespace(
            capture_bundle_id=8, request_identity_sha256=b"i" * 32, state="no_change" if no_change else "completed"
        )
        request_fields = (
            "dataset_id",
            "definition_revision_id",
            "schema_revision_id",
            "execution_id",
            "complete_scope",
            "page_row_limit",
            "page_byte_limit",
            "statement_timeout_ms",
            "build_deadline_at",
        )
        self.build = SimpleNamespace(**{name: getattr(self.request, name) for name in request_fields})
        vars(self.build).update(
            build_id=7,
            capture_bundle_id=8,
            request_identity_sha256=b"i" * 32,
            phase="verified",
            generation_id=41,
            producing_fence=self.request.fence,
            producing_token_sha256=binding.producer_token_sha256(self.request),
            base_generation_id=5,
            base_pointer_version=2,
            refresh_mode=self.request.definition.refresh_mode,
            source_frozen_at=object(),
        )
        self.lease = SimpleNamespace(continuation_id=6 if continued else None)
        self.family = SimpleNamespace(
            **{
                name: getattr(self.request, name)
                for name in ("dataset_id", "definition_revision_id", "schema_revision_id", "execution_id")
            },
            family_id=17,
            frozen_at=object(),
            generation_id=41,
            capture_bundle_id=8,
            producing_fence=self.request.fence,
            producing_token_sha256=binding.producer_token_sha256(self.request),
        )
        self.descriptor = SimpleNamespace(
            family_id=17,
            phase="logged",
            layout_contract=binding.FAMILY_BLOCK_LAYOUT_CONTRACT,
            definition_sha256=bytes.fromhex(self.request.definition.digest),
            layout_sha256=self.layout.digest,
            collection_slots=dict(self.layout.collection_slots),
            source_accepted_family_count=7,
            source_rejection_count=3,
        )
        self.generation, self.seal, self.no_change = object(), object(), no_change
        self.no_change_receipt = object()
        self.calls, self.missing = [], False

    @asynccontextmanager
    async def session(self, *_args, **_kwargs):
        yield self

    async def get(self, model, identity, **_kwargs):
        assert model is binding.CustomImportBuildAttempt and identity == self.build.build_id
        return self.build

    async def execute(self, statement):
        compiled = statement.compile().params
        assert self.request.execution_id in compiled.values() and self.request.fence in compiled.values()
        self.calls.append("candidate")
        return SimpleNamespace(one_or_none=lambda: None if self.missing else (self.family, self.descriptor))

    async def no_change_replay(self, _session, request, execution):
        assert execution is self.execution
        assert (request.expected_generation_id, request.candidate_generation_id) == (5, 41)
        self.calls.append("no-change")
        return self.no_change_receipt

    def install(self, monkeypatch):
        result_by_operation = {
            "_begin_finality_operation": None,
            "_locked_dataset": None,
            "_locked_execution": self.execution,
            "_locked_lease": self.lease,
            "_locked_generation": self.generation,
            "_validated_generation_seal": self.seal,
            "_database_now": object(),
        }
        for name, result in result_by_operation.items():
            monkeypatch.setattr(binding.publication, name, AsyncMock(return_value=result))
        monkeypatch.setattr(binding.publication, "_generation_seal_replay", lambda *_args: self.calls.append("seal"))
        monkeypatch.setattr(binding.publication, "_replayed_no_change_receipt", self.no_change_replay)
        monkeypatch.setattr(binding, "verify_terminal_continuation", AsyncMock())
        monkeypatch.setattr(binding, "_set_timeout", AsyncMock())
        monkeypatch.setattr(binding, "load_registry", AsyncMock(return_value=self.registry))
        monkeypatch.setattr(binding, "_session", self.session)


@pytest.mark.parametrize("no_change", [False, True])
@pytest.mark.parametrize("continued", [False, True])
async def test_terminal_candidate_pair_uses_existing_seal_and_continuation_authority(monkeypatch, no_change, continued):
    native = _Terminal(no_change=no_change, continued=continued)
    native.install(monkeypatch)
    assert await binding.terminal_source_outcomes(None, native.request, 7) == SourceOutcomeCounts(7, 3)
    assert native.calls == ["no-change" if no_change else "seal", "candidate"]
    assert binding.verify_terminal_continuation.await_count == int(continued)


@pytest.mark.parametrize(
    "damage", ["missing", "partial", "base", "capture", "producer", "layout", "unfrozen", "request"]
)
async def test_terminal_counts_never_fall_back_to_zero_or_base_candidate(monkeypatch, damage):
    native = _Terminal(no_change=True)
    native.install(monkeypatch)
    match damage:
        case "missing":
            native.missing = True
        case "partial":
            native.descriptor.source_rejection_count = None
        case "base":
            native.family.generation_id = 5
        case "capture":
            native.family.capture_bundle_id = 9
        case "producer":
            native.family.producing_token_sha256 = b"x" * 32
        case "layout":
            native.descriptor.layout_sha256 = b"x" * 32
        case "unfrozen":
            native.family.frozen_at = None
        case "request":
            native.build.page_byte_limit += 1
    with pytest.raises((FamilyBlockUnavailable, CandidateRunnerError)):
        await binding.terminal_source_outcomes(None, native.request, 7)


async def test_terminal_missing_no_change_receipt_and_revoked_continuation_refuse(monkeypatch):
    native = _Terminal(no_change=True, continued=True)
    native.install(monkeypatch)
    native.no_change_receipt = None
    with pytest.raises(FamilyBlockUnavailable, match="no-change receipt"):
        await binding.terminal_source_outcomes(None, native.request, 7)
    binding.verify_terminal_continuation.side_effect = LeaseAuthorityLost("revoked terminal continuation")
    with pytest.raises(LeaseAuthorityLost, match="revoked"):
        await binding.terminal_source_outcomes(None, native.request, 7)
    assert "candidate" not in native.calls


@pytest.mark.parametrize("phase", ["loading", "closed", "verified", "logged"])
@pytest.mark.parametrize("pair", [(None, None), (1, None), (None, 2), (-1, 0), (0, -1), (0, 0), (7, 3)])
def test_model_pair_check_handles_nulls_and_all_closed_phases(phase, pair):
    table = CustomImportFamilyBlockLayout.__table__
    assert all(
        isinstance(table.c[name].type, BigInteger)
        for name in ("source_accepted_family_count", "source_rejection_count")
    )
    check = next(
        item
        for item in table.constraints
        if isinstance(item, CheckConstraint) and "source_accepted_family_count" in str(item.sqltext)
    )
    with sqlite3.connect(":memory:") as database:
        database.execute(
            f"CREATE TABLE outcomes (phase TEXT, source_accepted_family_count BIGINT, source_rejection_count BIGINT, CHECK ({check.sqltext}))"
        )
        valid = (pair == (None, None) and phase == "loading") or all(
            type(value) is int and value >= 0 for value in pair
        )
        if valid:
            database.execute("INSERT INTO outcomes VALUES (?,?,?)", (phase, *pair))
        else:
            with pytest.raises(sqlite3.IntegrityError):
                database.execute("INSERT INTO outcomes VALUES (?,?,?)", (phase, *pair))


def test_unpublished_migration_matches_outcome_pair_check():
    path = Path("alembic/versions/20261104020000_custom_import_family_blocks.py")
    spec = importlib.util.spec_from_file_location("outcome_pair_migration", path)
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    ddl = "".join(migration._DDL).replace(" ", "").replace("\n", "")
    check = next(
        item
        for item in CustomImportFamilyBlockLayout.__table__.constraints
        if isinstance(item, CheckConstraint) and "source_accepted_family_count" in str(item.sqltext)
    )
    assert str(check.sqltext).replace(" ", "") in ddl
