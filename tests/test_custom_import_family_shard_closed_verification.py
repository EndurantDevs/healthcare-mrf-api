# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Real producer files and comparisons; native rows/catalog/authority are fakes.

These tests execute the actual reconstruction and equality leaves. They do not
establish PostgreSQL registration, owner privileges, a publication or a seal.
"""

import asyncio
import threading
from contextlib import asynccontextmanager, closing
from dataclasses import replace
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
from sqlalchemy.sql.elements import TextClause
from sqlalchemy.sql.util import find_tables

from db.models import custom_import as models
from db.models.custom_import_family_block import CustomImportFamilyBlockLayout
from db.models.custom_import_storage import CustomImportSnapshotFamily
from process.custom_import import family_shard_allocate as source_allocation
from process.custom_import import family_shard_closed_verification as verification
from process.custom_import import family_shard_events as events
from process.custom_import import family_shard_metadata as source_metadata
from process.custom_import import family_shard_projection_verify as projection
from process.custom_import import family_shard_retained_storage_validation as stored
from process.custom_import.family_block_binding import FamilyBlockBinding
from process.custom_import.family_block_layout import FAMILY_BLOCK_LAYOUT_CONTRACT
from process.custom_import.family_block_store import ShardOutputProof
from process.custom_import.family_block_validate import (
    CandidateScope,
    FamilyBlockValidationError,
    check_closed_identity,
)
from process.custom_import.family_shard_allocate import SourceAllocationLimits
from process.custom_import.family_shard_facts import SourceFactLimits
from process.custom_import.family_shard_member_winners import build_original_member_winners
from process.custom_import.family_shard_plan import PlanWork
from process.custom_import.family_shard_source_values import prepared_source_family_inputs
from process.custom_import.family_shard_winner_validation import RetainedWinnerInput
from process.custom_import.family_shard_winners import cleanup_winner_output
from process.custom_import.materialization import DefinitionIdentity, GenerationIdentity, selection_profile_models
from tests import test_custom_import_family_shard_events as event_fixture
from tests import test_custom_import_family_shard_retained_encoding as fixture

_STAMP = datetime(2026, 11, 5, tzinfo=timezone.utc)
_FACTS = SourceFactLimits(256 * 1024**2, 4 * 1024**2, 8 * 1024**2)
_LIMITS = SourceAllocationLimits(_FACTS, 32, 1024**2)


class _Result:
    def __init__(self, rows):
        self.rows = list(rows)

    def all(self):
        return self.rows

    def scalars(self):
        return self

    def mappings(self):
        return self

    def one(self):
        assert len(self.rows) == 1
        return self.rows[0]

    def scalar_one(self):
        return self.one()

    def scalar_one_or_none(self):
        assert len(self.rows) <= 1
        return self.rows[0] if self.rows else None

    def first(self):
        return self.rows[0] if self.rows else None


class _Cursor:
    def __init__(self, rows, names=None):
        self.rows, self.names, self.closed = iter(rows), names, False
        self.as_mapping = False

    def mappings(self):
        self.as_mapping = True
        return self

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            row = next(self.rows)
        except StopIteration:
            raise StopAsyncIteration from None
        return dict(zip(self.names, row, strict=True)) if self.as_mapping else row

    async def partitions(self, count):
        rows = []
        async for row in self:
            rows.append(row)
            if len(rows) == count:
                yield rows
                rows = []
        if rows:
            yield rows

    async def close(self):
        self.closed = True


class _Native:
    """Finite synthetic SQL transport, not a native accepted snapshot."""

    def __init__(self, case, rows, receipts):
        self.case, self.rows, self.receipts = case, rows, receipts
        self.root, self.connection_root = SimpleNamespace(is_active=True), SimpleNamespace(is_active=True)
        self.new, self.dirty, self.deleted, self.info = set(), set(), set(), {}
        self.cursors, self.predicates = [], []
        self.manifest = None
        self._controls()

    def _candidate_models(self):
        context, request = self.case.context, self.case.context.common.request
        layout, family_id = context.common.layout, context.common.source.snapshot_family_id
        self.binding = FamilyBlockBinding(
            family_id,
            layout,
            layout.tables(family_id),
            {role: 1000 + ordinal for ordinal, role in enumerate(layout.tables(family_id))},
            9,
        )
        scope = CandidateScope(
            family_id,
            self.case.generation.generation_id,
            request.dataset_id,
            request.definition_revision_id,
            request.schema_revision_id,
            request.execution_id,
            dict(context.common.source.source.scope[0])["capture_bundle_id"],
            request.fence,
            verification.producer_token_sha256(request),
            request.expected_base_generation_id,
        )
        producer = scope.parameters()
        producer.pop("family_id")
        self.snapshot = CustomImportSnapshotFamily(
            family_id=family_id,
            **{name: observed_value for name, observed_value in producer.items() if name != "base_generation_id"},
        )
        self.descriptor = CustomImportFamilyBlockLayout(
            family_id=family_id,
            phase="closed",
            closed_at=_STAMP,
            layout_contract=FAMILY_BLOCK_LAYOUT_CONTRACT,
            layout_sha256=layout.digest,
            definition_sha256=bytes.fromhex(layout.definition.digest),
            collection_slots=dict(layout.collection_slots),
        )
        return context, request, layout, scope, producer

    def _build_models(self, context, request, layout, scope, producer):
        planned = verification._complete_plan(context.common, context.final_plan, context.admission, lambda: None)
        self.build = models.CustomImportBuildAttempt(
            build_id=context.build_id,
            **producer,
            phase="verifying",
            plan_stage="complete",
            plan_complete_at=_STAMP,
            selected_family_count=planned.selected_count,
            completed_family_count=planned.selected_count,
            source_occurrence_count=context.common.source.source.counts[0],
            candidate_error_count=0,
            next_rejection_ordinal=sum(self.case.events.counts[:2]),
            generation_family_count=planned.selected_count,
            candidate_context_count=sum(
                stream[2] for _slot, streams, _count in self.case.winners.profiles for stream in streams
            ),
            winner_count=self.case.winners.winner_count,
            source_frozen_at=_STAMP,
            graph_frozen_at=_STAMP,
            output_frozen_at=_STAMP,
        )
        self.source_digest = b"s" * 32
        self.generation = models.CustomImportGeneration(
            **verification._generation_values(context.common, self.build, planned, self.source_digest)
        )
        self.generation.generation_id = scope.generation_id
        self.identity = check_closed_identity(
            self.binding,
            scope,
            verification._native_values(self.snapshot),
            verification._native_values(self.descriptor),
        )
        self.profiles = selection_profile_models(
            request.definition,
            identity=DefinitionIdentity(request.dataset_id, request.definition_revision_id, request.schema_revision_id),
            child_collection_slots=layout.collection_slots,
        )

    def _controls(self):
        context, request, layout, scope, producer = self._candidate_models()
        self._build_models(context, request, layout, scope, producer)
        scratch = verification._Scratch(PlanWork(self.case.root / "census", _FACTS, lambda _path: None))
        try:
            self.source_counts = verification._source_census(context, scratch)
            ids = sorted(verification._home_ids(context, self.case.inputs, scratch))
            self.ranges = [(kind, identity, identity) for kind, identity in ids]
        finally:
            scratch.close()
        tails_by_key = dict(self.case.destination.destination_pack_tail)
        self.streams = [
            models.CustomImportBuildStream(
                build_id=context.build_id,
                stream_slot=slot,
                next_part_ordinal=counts[0] + 1,
                next_part_row_ordinal=0,
                next_source_ordinal=counts[1],
                next_pack_ordinal=tails_by_key[slot],
                replay_verified_at=_STAMP,
            )
            for slot, counts in self.source_counts.items()
        ]

    def get_transaction(self):
        return self.root

    def get_nested_transaction(self):
        return None

    def in_transaction(self):
        return self.root.is_active

    async def connection(self):
        return SimpleNamespace(get_transaction=lambda: self.connection_root, get_nested_transaction=lambda: None)

    async def get(self, model, identity, **options):
        return {
            models.CustomImportBuildAttempt: self.build,
            models.CustomImportGeneration: self.generation,
            CustomImportSnapshotFamily: self.snapshot,
            CustomImportFamilyBlockLayout: self.descriptor,
        }.get(model)

    def _roles(self, statement):
        names = {table.name for table in find_tables(statement, check_columns=True) if table is not None}
        return {role for role, table in self.binding.tables_by_role.items() if table.name in names}, names

    def _role_result(self, statement, sql):
        roles, names = self._roles(statement)
        if "custom_import_query_projection" in names:
            return _Result([] if self.manifest is None else [self.manifest])
        if "custom_import_snapshot_family" in names:
            return _Result([verification._native_values(self.snapshot)])
        if "custom_import_family_block_layout" in names:
            return _Result([verification._native_values(self.descriptor)])
        if "custom_import_build_stream" in names:
            return _Result(self.streams)
        if "custom_import_selection_profile" in names:
            return _Result(self.profiles)
        if "custom_import_revision_home" in names:
            return _Result([SimpleNamespace(row_count=len(self.ranges), row_bytes=2048 if self.ranges else 0)])
        assert len(roles) == 1, names
        role = next(iter(roles))
        family_rows = self.rows[role]
        if "count(" not in sql:
            return _Result(family_rows)
        width = max(
            (
                1024 + sum(128 + (8 * len(cell) if isinstance(cell, str | bytes) else 0) for cell in family_row)
                for family_row in family_rows
            ),
            default=0,
        )
        return _Result([(len(family_rows), width)])

    async def execute(self, statement, parameters=None):
        sql = str(statement)
        if "transaction_isolation" in sql:
            return _Result([("repeatable read", "on")])
        if isinstance(statement, TextClause):
            self.predicates.append(sql)
            return _Result([])
        return self._role_result(statement, sql)

    async def stream(self, statement):
        roles, names = self._roles(statement)
        if "custom_import_revision_home" in names:
            rows, columns = self.ranges, None
        elif len(roles) > 1:
            rows, columns = self._joined("winner" in roles), None
        else:
            role = next(iter(roles))
            table = self.binding.tables_by_role[role]
            columns = tuple(table.c.keys())
            indices = tuple(columns.index(name) for name in table.primary_key.columns.keys())
            rows = sorted(self.rows[role], key=lambda row: tuple(row[index] for index in indices))
        cursor = _Cursor(rows, columns)
        self.cursors.append(cursor)
        return cursor

    async def stream_scalars(self, statement):
        roles, names = self._roles(statement)
        if "custom_import_selection_profile" in names:
            rows = self.profiles
        else:
            role = next(iter(roles))
            model = models.CustomImportPack if role == "pack" else models.CustomImportRejection
            rows = [model(**self._named(role, row)) for row in self.rows[role]]
        cursor = _Cursor(rows)
        self.cursors.append(cursor)
        return cursor

    def _named(self, role, row):
        return dict(zip(self.binding.tables_by_role[role].c.keys(), row, strict=True))

    def _joined(self, winner):
        roots_by_key = {
            joined_record[0]: models.CustomImportRootRecord(**self._named("root_record", joined_record))
            for joined_record in self.rows["root_record"]
        }
        entities_by_key = {
            joined_record[0]: models.CustomImportEntityBinding(**self._named("entity_binding", joined_record))
            for joined_record in self.rows["entity_binding"]
        }
        members_by_key = {}
        for joined_record in self.rows["generation_family"]:
            member = models.CustomImportGenerationFamily(**self._named("generation_family", joined_record))
            members_by_key[member.family_revision_id] = member
        families_by_key = {joined_record[0]: joined_record for joined_record in self.rows["family"]}
        if winner:
            for joined_record in self.rows["winner"]:
                selected = models.CustomImportWinner(**self._named("winner", joined_record))
                family = families_by_key[selected.family_revision_id]
                profile = next(profile for profile in self.profiles if profile.profile_slot == selected.profile_slot)
                yield (
                    selected,
                    entities_by_key[family[3]],
                    roots_by_key[family[1]],
                    profile,
                    members_by_key[family[0]],
                    *family,
                )
        else:
            for identity in sorted(families_by_key):
                family = families_by_key[identity]
                yield roots_by_key[family[1]], entities_by_key[family[3]], members_by_key[identity], *family


def _receipt(layout, batches, shard, partition):
    proof = ShardOutputProof(layout, shard, partition, 0)
    with closing(batches):
        for batch in batches:
            for role, rows in batch.items():
                proof.add(role, rows)
    return proof.receipt()


@asynccontextmanager
async def _event_originals(tmp_path, monkeypatch, empty):
    journals = []
    original_allocate = source_allocation.allocate_source_plan

    async def allocate(*args, **kwargs):
        journals.append(kwargs["journal"])
        return await original_allocate(*args, **kwargs)

    monkeypatch.setattr(source_allocation, "allocate_source_plan", allocate)
    try:
        async with fixture._case(tmp_path, monkeypatch, empty=empty) as case:
            assert len(journals) == 1
            case.events = await _case_events(monkeypatch, case, journals[0])
            yield case
    finally:
        for journal in journals:
            journal.close_terminal()


async def _case_events(monkeypatch, case, journal):
    common = case.context.common
    limits = SourceAllocationLimits(journal.work.limits, 32, 1024**2)
    event_fixture._metadata_boundary(monkeypatch, common.source)
    keys = tuple((digest, text) for digest, text, _rank, _variant in case.query_keys())
    async with source_metadata.owner_source_metadata(
        None, common.request, common.layout, common.source, journal=journal, limits=limits, retained_query_keys=keys
    ):
        event_fixture._native_boundary(
            monkeypatch,
            SimpleNamespace(request=common.request, layout=common.layout, source=common.source, journal=journal),
        )
        return await events.allocate_admission_events(
            None, common.request, common.layout, common.source, case.context.admission, journal=journal, limits=limits
        )


def _closed_members(tmp_path, case, prepared):
    context, common = case.context, case.context.common
    workers = tuple(case.native.workers)
    retained = RetainedWinnerInput(case.destination, case.comparison)
    auxiliary = len(workers)
    with verification.selected_shard_families(
        context,
        workers,
        prepared,
        retained,
        work=PlanWork(tmp_path / "union", _FACTS, case.registered.append),
    ) as families:
        case.members, case.winners = build_original_member_winners(
            common,
            case.generation,
            context.final_plan,
            families,
            len(workers) + 1,
            work=PlanWork(tmp_path / "members", _FACTS, case.registered.append),
            member_shard_id=auxiliary + 3,
        )
    return workers, retained


def _closed_inputs(tmp_path, case, prepared):
    context, common = case.context, case.context.common
    workers, retained = _closed_members(tmp_path, case, prepared)
    auxiliary = len(workers)
    partition = bytes.fromhex(common.source.source.partition_sha256)
    options_by_key = dict(maximum_rows=32, maximum_bytes=1024**2)
    scratch = verification._Scratch(PlanWork(tmp_path / "receipts", _FACTS, case.registered.append))
    common_receipt = _receipt(common.layout, context.common_output.batches(**options_by_key), auxiliary, partition)
    event_receipt = _receipt(
        common.layout, events.admission_event_batches(case.events, **options_by_key), auxiliary + 1, partition
    )
    retained_receipt = _receipt(common.layout, case.destination.batches(**options_by_key), auxiliary + 2, partition)
    winner_receipt = _receipt(
        common.layout, verification._winner_batches(case.winners, _LIMITS, scratch), auxiliary + 4, partition
    )
    scratch.close()
    return verification.ClosedShardInputs(
        workers,
        prepared,
        case.query_keys,
        case.events,
        retained,
        case.output,
        case.stored,
        case.members,
        case.winners,
        None,
        None,
        common_receipt,
        event_receipt,
        retained_receipt,
        winner_receipt,
    )


def _closed_rows(tmp_path, case):
    context, common = case.context, case.context.common
    family_rows_by_key = {role: [] for role in common.layout.tables(common.source.snapshot_family_id)}
    receipts = []
    scratch = verification._Scratch(PlanWork(tmp_path / "rows", _FACTS, case.registered.append))
    try:
        for batch in verification._expected_batches(context, case.inputs, _LIMITS, scratch, receipts):
            for role, observed_values in batch.items():
                family_rows_by_key[role].extend(observed_values)
    finally:
        scratch.close()
    family_rows_by_key["shard_receipt"] = [
        tuple(
            receipt[name]
            for name in ("shard_id", "partition_sha256", "result_sha256", "source_record_count", "role_counts")
        )
        + (_STAMP,)
        for receipt in receipts
    ]
    return family_rows_by_key, receipts


@asynccontextmanager
async def _case(tmp_path, monkeypatch, *, empty=False):
    async with _event_originals(tmp_path, monkeypatch, empty) as case:
        case.root = tmp_path
        context, common = case.context, case.context.common
        case.generation = GenerationIdentity(
            501, common.request.dataset_id, common.request.definition_revision_id, common.request.schema_revision_id
        )

        case.stored = await stored.prove_retained_output(
            context,
            case.destination,
            case.comparison,
            case.output,
            case.query_keys,
            work=PlanWork(tmp_path / "retained-stored", _FACTS, case.registered.append),
        )
        with prepared_source_family_inputs(
            common,
            context.admission,
            context.final_plan,
            work=PlanWork(tmp_path / "prepared-source", _FACTS, case.registered.append),
        ) as prepared:
            case.inputs = _closed_inputs(tmp_path, case, prepared)
            family_rows_by_key, receipts = _closed_rows(tmp_path, case)
            case.session = _Native(case, family_rows_by_key, receipts)
            _native_boundary(monkeypatch, case)
            proof = await projection.verify_closed_query_projection(
                case.session,
                case.session.binding,
                case.generation,
                case.session.identity,
                work=PlanWork(tmp_path / "projection-proof", _FACTS, case.registered.append),
                limits=_LIMITS,
            )
            case.inputs = replace(case.inputs, projection=proof)
            try:
                yield case
            finally:
                case.members.close()
                cleanup_winner_output(case.winners)
                events.cleanup_admission_events(case.events)


def _native_boundary(monkeypatch, case):
    session = case.session

    async def candidate(current, request, family, layout, phases, *, locked):
        assert current is session and not locked and family == session.binding.family_id
        return session.descriptor, session.binding

    async def binding(*arguments, **options):
        return session.binding

    async def indexes(*arguments, **options):
        return None

    async def schema(current):
        assert current is session
        return None, "synthetic_control"

    async def source_digest(current, common):
        assert current is session and common is case.context.common
        return session.source_digest

    async def captures(current, common, counts):
        assert current is session and counts == session.source_counts

    monkeypatch.setattr(verification, "_candidate", candidate)
    monkeypatch.setattr(verification, "validate_family_block_indexes", indexes)
    monkeypatch.setattr(verification, "_control_schema", schema)
    monkeypatch.setattr(verification, "_source_digest", source_digest)
    monkeypatch.setattr(verification, "_source_capture_counts", captures)
    monkeypatch.setattr(projection, "resolve_family_block_binding", binding)
    monkeypatch.setattr(projection, "validate_family_block_indexes", indexes)


async def _verify(tmp_path, case):
    async def prepare(current):
        assert current is case.session

    return await verification.verify_closed_shard_build(
        case.session,
        case.context,
        case.inputs,
        work=PlanWork(tmp_path / "closed-proof", _FACTS, case.registered.append),
        limits=_LIMITS,
        prepare=prepare,
    )


@pytest.mark.parametrize("empty", [True, False])
async def test_genuine_producer_files_compose_through_actual_reconstruction(tmp_path, monkeypatch, empty):
    async with _case(tmp_path, monkeypatch, empty=empty) as case:

        def repeated(*arguments, **options):
            raise AssertionError("completed retained or projection comparison must not be repeated")

        monkeypatch.setattr(stored, "prove_retained_output", repeated)
        monkeypatch.setattr(projection, "verify_closed_query_projection", repeated)
        result = await _verify(tmp_path, case)
        assert result.identity == case.session.identity
        assert result.counts[:3] == ((0, 0, 0) if empty else (2, 2, 2))
        assert result.source_record_count == case.context.common.source.source.counts[0]
        assert dict(result.role_counts)["shard_receipt"] == result.completed_shards == len(case.session.receipts)
        assert len(result.validation_sha256) == 32 and await _verify(tmp_path, case) == result
        assert all(cursor.closed for cursor in case.session.cursors)
        assert not (tmp_path / "closed-proof").exists()


@pytest.mark.parametrize("damage", ["producer", "tail", "role", "receipt", "home", "disabled_manifest"])
async def test_native_transport_changes_are_refused(tmp_path, monkeypatch, damage):
    async with _case(tmp_path, monkeypatch) as case:
        session = case.session
        match damage:
            case "producer":
                session.build.execution_id += 1
            case "tail":
                session.streams[0].next_pack_ordinal += 1
            case "role":
                row = session.rows["key_dictionary"][0]
                session.rows["key_dictionary"][0] = row[:3] + (row[3] + "changed",)
            case "receipt":
                row = session.rows["shard_receipt"][0]
                session.rows["shard_receipt"][0] = row[:-1] + (None,)
            case "home":
                session.ranges.pop()
            case _:
                session.manifest = SimpleNamespace(phase="complete")
        with pytest.raises(FamilyBlockValidationError):
            await _verify(tmp_path, case)
        assert all(cursor.closed for cursor in session.cursors)
        assert not (tmp_path / "closed-proof").exists()
        assert not case.inputs.source_families.is_closed


@pytest.mark.parametrize("damage", ["source_factory", "stored_scope", "stored_split", "duplicate_shard", "dictionary"])
async def test_exact_completed_original_inputs_are_required(tmp_path, monkeypatch, damage):
    async with _case(tmp_path, monkeypatch) as case:
        match damage:
            case "source_factory":
                case.inputs = replace(case.inputs, source_families=lambda: iter(()))
            case "stored_scope":
                case.inputs = replace(case.inputs, retained_stored=replace(case.stored, scope=()))
            case "stored_split":
                case.inputs = replace(case.inputs, retained_stored=replace(case.stored, root_scalar_count=True))
            case "duplicate_shard":
                receipt_by_key = dict(case.inputs.common_receipt, shard_id=case.inputs.workers[0].receipt["shard_id"])
                case.inputs = replace(case.inputs, common_receipt=receipt_by_key)
            case _:
                original_keys = case.query_keys

                def keys():
                    with closing(verification._input_iterator(original_keys())) as rows:
                        for digest, text, rank, variant in rows:
                            yield digest, text + "changed", rank, variant

                case.inputs = replace(case.inputs, query_keys=keys)
        with pytest.raises((ValueError, FamilyBlockValidationError)):
            await _verify(tmp_path, case)
        assert not (tmp_path / "closed-proof").exists()


async def test_prepare_must_preserve_the_original_read_transaction(tmp_path, monkeypatch):
    async with _case(tmp_path, monkeypatch) as case:

        async def changed(session):
            session.root = SimpleNamespace(is_active=True)

        with pytest.raises(FamilyBlockValidationError, match="transaction"):
            await verification.verify_closed_shard_build(
                case.session,
                case.context,
                case.inputs,
                work=PlanWork(tmp_path / "closed-proof", _FACTS, case.registered.append),
                limits=_LIMITS,
                prepare=changed,
            )
        assert not (tmp_path / "closed-proof").exists()


async def test_missing_native_predicate_coverage_is_refused(tmp_path, monkeypatch):
    async with _case(tmp_path, monkeypatch) as case:
        original = verification.run_structural_checks

        async def incomplete(*arguments, **options):
            return (await original(*arguments, **options))[:-1]

        monkeypatch.setattr(verification, "run_structural_checks", incomplete)
        with pytest.raises(FamilyBlockValidationError, match="predicate coverage"):
            await _verify(tmp_path, case)
        assert not (tmp_path / "closed-proof").exists()


async def test_cancellation_drains_cpu_before_owned_scratch_removal(tmp_path, monkeypatch):
    async with _case(tmp_path, monkeypatch) as case:
        entered, drained, removed_early = threading.Event(), threading.Event(), []
        original = verification._complete_plan

        def blocked(*arguments):
            entered.set()
            try:
                while not drained.wait(0.005):
                    arguments[-1]()
            finally:
                removed_early.append(not (tmp_path / "closed-proof").exists())
                drained.set()

        monkeypatch.setattr(verification, "_complete_plan", blocked)
        pending = asyncio.create_task(_verify(tmp_path, case))
        assert await asyncio.to_thread(entered.wait, 5)
        pending.cancel()
        with pytest.raises(asyncio.CancelledError):
            await pending
        assert drained.is_set() and removed_early == [False]
        assert not (tmp_path / "closed-proof").exists()
        assert not case.inputs.source_families.is_closed
        assert all(cursor.closed for cursor in case.session.cursors)
        monkeypatch.setattr(verification, "_complete_plan", original)
        assert (await _verify(tmp_path, case)).identity == case.session.identity


def _union(tmp_path, case, **changes):
    inputs = case.inputs
    return verification.selected_shard_families(
        case.context,
        changes.get("workers", inputs.workers),
        changes.get("prepared", inputs.source_families),
        changes.get("retained", inputs.retained),
        work=PlanWork(tmp_path / "selected-union", _FACTS, case.registered.append, changes.get("check")),
    )


def _assert_borrowed_reader_closed(factory):
    with pytest.raises(ValueError):
        next(factory())


async def test_public_union_preserves_route_order_and_borrowed_children(tmp_path, monkeypatch):
    async with _case(tmp_path, monkeypatch) as case:
        observed, prior_factory = [], None
        with _union(tmp_path, case) as families:
            for shard, plan, family in families:
                if prior_factory is not None:
                    _assert_borrowed_reader_closed(prior_factory)
                expected = (
                    len(case.inputs.workers)
                    if plan.selection_kind == "retained"
                    else int.from_bytes(plan.typed_key[1][:8], "big") % len(case.inputs.workers)
                )
                assert shard == expected
                prior_factory = family.children_by_collection["details"]
                with closing(prior_factory()) as children:
                    assert sum(1 for _child in children) > 0
                observed.append((plan.typed_key[::-1], plan.selection_kind))
        assert observed == sorted(observed) and {kind for _key, kind in observed} == {"source", "retained"}
        with pytest.raises(ValueError):
            next(prior_factory())
        assert not (tmp_path / "selected-union").exists()
        assert not case.inputs.source_families.is_closed


async def test_public_union_requires_eof_and_releases_prepared_owner(tmp_path, monkeypatch):
    async with _case(tmp_path, monkeypatch) as case:
        with (
            pytest.raises(FamilyBlockValidationError, match="complete original EOF"),
            _union(tmp_path, case) as families,
        ):
            _shard, _plan, family = next(families)
            borrowed = family.children_by_collection["details"]
        with pytest.raises(ValueError):
            next(borrowed())
        assert not (tmp_path / "selected-union").exists()
        with _union(tmp_path, case) as families:
            assert sum(1 for _family in families) == 2


@pytest.mark.parametrize("damage", ["factory", "cohort", "comparison"])
async def test_public_union_requires_exact_original_owners_before_scratch(tmp_path, monkeypatch, damage):
    async with _case(tmp_path, monkeypatch) as case:
        changes = {
            "factory": {"prepared": lambda: iter(())},
            "cohort": {"workers": case.inputs.workers[:-1]},
            "comparison": {
                "retained": replace(
                    case.inputs.retained, comparison=replace(case.comparison, destination_sha256=b"x" * 32)
                )
            },
        }[damage]
        with (
            pytest.raises((ValueError, FamilyBlockValidationError)),
            _union(tmp_path, case, **changes),
        ):
            raise AssertionError("changed original owner must be refused before borrowing")
        assert not (tmp_path / "selected-union").exists()


async def test_public_union_preserves_original_failure_and_closes_readers(tmp_path, monkeypatch):
    async with _case(tmp_path, monkeypatch) as case:
        failure = RuntimeError("synthetic requester stop")
        enabled_items = []

        def check():
            if enabled_items:
                raise failure

        with (
            pytest.raises(RuntimeError) as caught,
            _union(tmp_path, case, check=check) as families,
        ):
            next(families)
            enabled_items.append(True)
            next(families)
        assert caught.value is failure
        assert not (tmp_path / "selected-union").exists()
        with _union(tmp_path, case) as families:
            assert sum(1 for _family in families) == 2
