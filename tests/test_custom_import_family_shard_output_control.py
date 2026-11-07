# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Real completed plan/winner files; only native transactions and SQL are fake."""

import asyncio
import hashlib
import json
import threading
from contextlib import asynccontextmanager, closing, contextmanager, nullcontext
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest

from process.custom_import import family_shard_admission as audit
from process.custom_import import family_shard_graph_control as graph
from process.custom_import import family_shard_output_control as control
from process.custom_import import family_shard_plan as planner
from process.custom_import import family_shard_winners as winners
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.family_block_binding import FamilyBlockBinding, FamilyBlockUnavailable
from process.custom_import.family_block_layout import ROLE_SPECS, index_specifications
from process.custom_import.family_shard_output import _pack_row
from process.custom_import.materialization import GenerationIdentity, ValidatedWinnerCandidateStream, WinnerCandidate
from process.custom_import.query_projection_binding import QueryProjectionUnavailable
from process.custom_import.runner_codec import _sorted_child_documents, digest_text, new_family_hash_ordered
from tests import test_custom_import_family_shard_graph_control as fixtures
from tests import test_custom_import_query_projection_evidence as projections


def _profile_definition(original, mode):
    document = json.loads(original(mode).canonical)
    document["selection_profiles"] = [
        {
            "id": "by_detail",
            "selection": [{"field": "amount", "direction": "asc", "nulls": "last"}],
            "context_dimensions": ["detail_id"],
        },
        {
            "id": "by_root",
            "selection": [{"field": "score", "direction": "desc", "nulls": "last"}],
            "context_dimensions": ["npi"],
        },
    ]
    return CustomImportDefinition.from_mapping(document)


def _projection_definition(original, mode):
    document = json.loads(original(mode).canonical)
    document["schema"]["root"]["logical_key"] = ["npi", "score", "segment"]
    document["schema"]["root"]["fields"].append(
        {"id": "segment", "slot": 7, "type": "string", "nullable": False, "projection_slot": 7}
    )
    child = document["schema"]["children"][0]
    for slot, parent, field_type in ((8, "score", "integer"), (9, "segment", "string")):
        child["parent_key"].append({"child": "detail_" + parent, "root": parent})
        child["fields"].append({"id": "detail_" + parent, "slot": slot, "type": field_type, "nullable": False})
    document["query"]["root_fields"].append("segment")
    document["query"]["entity_selection"] = {
        "contract": "custom-import/grouped-entity-selection/v1",
        "field": "score",
        "default": "max_per_entity",
        "family_dimension": {"field": "segment", "values": ["A", "B"]},
        "by_value_profile": "by_value",
        "default_profile": "latest",
    }
    document["selection_profiles"] = [
        {"id": "by_value", "selection": [], "context_dimensions": ["score", "segment"]},
        {
            "id": "latest",
            "selection": [{"field": "score", "direction": "desc", "nulls": "last"}],
            "context_dimensions": [],
        },
    ]
    document["query"]["derived_fields"] = [
        {
            "id": "selected_score",
            "expression": {
                "type": "preferred_non_null",
                "scope": "root",
                "field_id": "score",
                "group_values": ["A", "B"],
            },
        }
    ]
    return CustomImportDefinition.from_mapping(document)


def _source_candidates(native):
    definition = native.context.request.definition
    with closing(planner.read_family_plan(native.plan)) as plans:
        selected_by_key = {plan.typed_key: plan for plan in plans if plan.selection_kind == "source"}
    with closing(audit.read_admission_occurrences(native.admission)) as occurrences:
        original_occurrences = tuple(occurrences)
    for key, plan in selected_by_key.items():
        matching_occurrences = [occurrence for occurrence in original_occurrences if occurrence.family_key == key]
        field_documents = [dict(occurrence.record.values) for occurrence in matching_occurrences]
        root = next(
            fields_by_name
            for occurrence, fields_by_name in zip(matching_occurrences, field_documents, strict=True)
            if not occurrence.collection_slot
        )
        child_values = [
            fields_by_name
            for occurrence, fields_by_name in zip(matching_occurrences, field_documents, strict=True)
            if occurrence.collection_slot
        ]
        family_hash = new_family_hash_ordered(
            definition, root, {"details": _sorted_child_documents(definition, "details", child_values)}
        )
        family_id, entity_id = plan.source_metadata
        permitted_fields = definition.query.root_fields + definition.query.child_fields
        for occurrence, fields_by_name in zip(matching_occurrences, field_documents, strict=True):
            yield WinnerCandidate(
                entity_id,
                family_id,
                family_hash,
                occurrence.collection_slot,
                occurrence.revision_id if occurrence.collection_slot else None,
                digest_text("child-key", occurrence.record.child_key) if occurrence.collection_slot else None,
                {name: scalar for name, scalar in fields_by_name.items() if name in permitted_fields},
            )


def _winner_file(native, root, *, slots=None):
    request = native.context.request
    generation = GenerationIdentity(
        21001, request.dataset_id, request.definition_revision_id, request.schema_revision_id
    )
    scopes = control.materialization._profile_scopes(request.definition, native.context.layout.collection_slots)
    candidates = tuple(
        candidate
        for candidate in _source_candidates(native)
        if candidate.context_collection_slot in {scope.collection_slot for scope in scopes}
    )

    @contextmanager
    def streams(_slot):
        # Other declared scopes remain in the audit without inflating the
        # matching-context count for this profile.
        yield (ValidatedWinnerCandidateStream(generation, iter(candidates)),)

    return winners.build_winner_output(
        native.context.layout,
        generation,
        streams,
        work=winners.WinnerWork(root, fixtures._LIMITS, lambda _path: None),
        profile_slots=slots,
    )


@asynccontextmanager
async def _case(tmp_path, monkeypatch, *, empty=False, slots=None, projection=False):
    original_definition = fixtures._definition
    definition_factory = _projection_definition if projection else _profile_definition
    monkeypatch.setattr(fixtures, "_definition", lambda mode: definition_factory(original_definition, mode))
    if projection:
        original_root, original_child = fixtures._root, fixtures._child
        monkeypatch.setattr(fixtures, "_root", lambda: original_root() | {"segment": "A"})
        monkeypatch.setattr(fixtures, "_child", lambda: original_child() | {"detail_score": 7, "detail_segment": "A"})
    async with fixtures._plan_case(tmp_path, monkeypatch, "empty" if empty else "source") as native:
        native.winners = _winner_file(native, tmp_path / "winners", slots=slots)
        try:
            yield native
        finally:
            winners.cleanup_winner_output(native.winners)


def _binding(native):
    family_id, layout = native.context.source.snapshot_family_id, native.context.layout
    return FamilyBlockBinding(
        family_id,
        layout,
        layout.tables(family_id),
        {role: 1000 + slot for role, (slot, _name) in ROLE_SPECS.items()},
        91,
        {role: bytes([slot % 256]) * 32 for role, (slot, _name) in ROLE_SPECS.items()},
    )


def _index_rows(binding):
    flags_by_name = dict.fromkeys(
        ("valid", "ready", "live", "plain", "complete", "default_options", "default_ops"), True
    )
    for role, name, columns in index_specifications(binding.family_id, binding.layout):
        table = binding.tables_by_role[role]
        collations = [
            "pg_catalog.C"
            if name == "canonical_value"
            else (
                "pg_catalog." + (table.c[name].type.collation or "default")
                if hasattr(table.c[name].type, "collation")
                else None
            )
            for name in columns
        ]
        yield dict(
            flags_by_name,
            index_name=name,
            table_oid=binding.oids_by_role[role],
            index_owner=binding.owner_oid,
            index_kind="i",
            access_method="btree",
            key_names=list(columns),
            key_count=len(columns),
            attribute_count=len(columns),
            collation_names=collations,
            uncollated=not any(collations),
            unique_index=False,
            primary_index=False,
            exclusion_index=False,
        )


class _NativeBoundary(fixtures._NativeBoundary):
    """SQL/catalog observations only; no PostgreSQL or structural-proof claim."""

    def __init__(self, native):
        super().__init__(native)
        self.binding = _binding(native)
        self.phase, self.index_damage = "closed", None
        self.manifest = None
        self.planned, self.expected = control._complete_inputs(
            native.context, native.plan, native.admission, native.winners, None
        )
        vars(self.build).update(
            phase="output",
            graph_frozen_at=fixtures._STAMP,
            generation_id=21001,
            plan_stage="complete",
            plan_complete_at=fixtures._STAMP,
            selected_family_count=self.expected.selected,
            completed_family_count=self.expected.selected,
            generation_family_count=0,
            candidate_context_count=0,
            winner_count=0,
            output_after_profile_slot=None,
            output_after_entity_binding_id=None,
            output_after_context_key_sha256=None,
        )
        self.role_counts = {
            "family": self.expected.selected,
            "generation_family": (self.expected.selected, self.expected.selected),
            "winner": (self.expected.winners, self.expected.winners),
        }

    async def ready(self, monkeypatch):
        super().install(monkeypatch)
        for name, method in (
            ("_page_session", self.page),
            ("_candidate", self.candidate),
            ("_execute", self.prepared_execute),
            ("_prepare_statement", self.prepare),
            ("_flush_page", self.flush),
        ):
            monkeypatch.setattr(control, name, method)
        digest = await graph._source_digest(self, self.native.context)
        self.generation = SimpleNamespace(
            generation_id=21001,
            **graph._generation_values(self.native.context, self.build, self.planned, digest),
        )
        self.observations.clear()

    async def candidate(self, session, request, family, layout, phases):
        assert session is self and request is self.native.context.request
        assert family == self.binding.family_id and layout is self.binding.layout and phases == {"closed"}
        self.observations.append("closed_candidate")
        if self.phase != "closed":
            raise FamilyBlockUnavailable("synthetic candidate is not closed")
        return None, self.binding

    async def execute(self, statement, parameters=None):
        sql = str(statement)
        if "pg_catalog.pg_index" in sql:
            self.observations.append("indexes")
            rows = list(_index_rows(self.binding))
            if self.index_damage == "missing":
                rows.pop()
            elif self.index_damage == "ready":
                rows[-1]["ready"] = False
            elif self.index_damage == "oid":
                rows[-1]["table_oid"] += 1
            return SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: rows))
        for role in ("generation_family", "winner", "family"):
            if self.binding.tables_by_role[role].name in sql:
                self.observations.append(role + "_census")
                value = self.role_counts[role]
                return SimpleNamespace(one=lambda: value, scalar_one=lambda: value)
        if "custom_import_generation" in sql:
            assert "FOR UPDATE" in sql and statement.get_execution_options()["populate_existing"] is True
        return await super().execute(statement, parameters)

    async def get(self, model, identifier):
        assert model.__name__ == "CustomImportGenerationSeal" and identifier == self.generation.generation_id
        self.observations.append("seal_lookup")
        return None

    async def scalars(self, statement):
        assert "custom_import_query_projection" in str(statement)
        assert statement.get_execution_options()["populate_existing"] is True
        self.observations.append("projection_lookup")
        return SimpleNamespace(one_or_none=lambda: self.manifest)

    async def flush(self, session):
        assert session is self and self.build.phase == "verifying"
        assert self.build.verified_at is None
        assert self.build.output_frozen_at == fixtures._STAMP
        self.observations.append("output_flush")


async def _complete(native, *, check=None):
    return await control.complete_shard_output(
        None, native.context, 31, native.plan, native.admission, native.winners, check=check
    )


@pytest.mark.parametrize("empty", (False, True))
async def test_completed_inputs_freeze_only_provisional_counts_and_exact_retry(tmp_path, monkeypatch, empty):
    async with _case(tmp_path, monkeypatch, empty=empty) as native:
        boundary = _NativeBoundary(native)
        await boundary.ready(monkeypatch)
        expected_counts = (0, 0, 0) if empty else (1, 2, 2)
        assert await _complete(native) == 21001
        assert (
            boundary.build.generation_family_count,
            boundary.build.candidate_context_count,
            boundary.build.winner_count,
        ) == expected_counts
        assert boundary.build.phase == "verifying" and boundary.build.verified_at is None
        assert boundary.observations[-2:] == ["fresh_authority", "commit"]
        assert boundary.observations.index("winner_census") < boundary.observations.index("indexes")
        assert boundary.observations.index("indexes") < boundary.observations.index("output_flush")
        boundary.observations.clear()
        assert await _complete(native) == 21001
        assert "output_flush" not in boundary.observations
        assert all(
            name in boundary.observations for name in ("generation_lookup", "indexes", "seal_lookup", "fresh_authority")
        )


async def test_valid_partial_profile_file_is_not_complete_output(tmp_path, monkeypatch):
    async with _case(tmp_path, monkeypatch, slots=(1,)) as native:
        assert tuple(winners.read_winner_rows(native.winners))
        with pytest.raises(FamilyBlockUnavailable, match="profile coverage"):
            await _complete(native)


@pytest.mark.parametrize(
    "damage",
    (
        "plan_sha",
        "admission_scope",
        "winner_sha",
        "layout",
        "definition",
        "generation",
        "generation_scope",
        "generation_shape",
    ),
)
async def test_complete_scope_and_digest_failures_precede_native_work(tmp_path, monkeypatch, damage):
    async with _case(tmp_path, monkeypatch) as native:
        boundary = _NativeBoundary(native)
        await boundary.ready(monkeypatch)
        match damage:
            case "plan_sha":
                native.plan = replace(native.plan, sha256=b"p" * 32)
            case "admission_scope":
                native.admission = replace(native.admission, snapshot_family_id=999)
            case "winner_sha":
                native.winners = replace(native.winners, sha256=b"w" * 32)
            case "layout":
                native.winners = replace(native.winners, layout_sha256="0" * 64)
            case "definition":
                native.winners = replace(native.winners, definition_sha256="0" * 64)
            case "generation":
                native.winners = replace(native.winners, generation=(True, *native.winners.generation[1:]))
            case "generation_scope":
                scope = native.winners.generation
                native.winners = replace(native.winners, generation=(scope[0], scope[1] + 1, *scope[2:]))
            case "generation_shape":
                native.winners = replace(native.winners, generation=())
        with pytest.raises((FamilyBlockUnavailable, ValueError)):
            await _complete(native)
        assert boundary.observations == []


@contextmanager
def _damaged_file(descriptor, damage):
    path = Path(descriptor.root) / winners._FILE
    original = path.read_bytes()
    entries = list(winners._file_entries(descriptor, None))
    if damage == "incomplete":
        entries.pop()
    elif damage == "trailing":
        entries.append(("unexpected",))
    else:
        index = next(index for index, entry in enumerate(entries) if entry[0] == "profile_complete")
        entries[index] = (*entries[index][:-1], entries[index][-1] + 1)
    packed_frames = [_pack_row(entry) for entry in entries]
    changed = b"".join(len(frame).to_bytes(8, "big") + frame for frame in packed_frames)
    path.chmod(0o600)
    path.write_bytes(changed)
    path.chmod(0o400)
    try:
        yield replace(descriptor, file_bytes=len(changed), sha256=hashlib.sha256(changed).digest())
    finally:
        path.chmod(0o600)
        path.write_bytes(original)
        path.chmod(0o400)


@pytest.mark.parametrize("damage", ("incomplete", "trailing", "census"))
async def test_self_consistent_winner_hash_never_substitutes_for_complete_eof(tmp_path, monkeypatch, damage):
    async with _case(tmp_path, monkeypatch) as native:
        boundary = _NativeBoundary(native)
        await boundary.ready(monkeypatch)
        original = native.winners
        try:
            with _damaged_file(original, damage) as changed:
                native.winners = changed
                with pytest.raises(ValueError):
                    await _complete(native)
        finally:
            native.winners = original
        assert boundary.observations == []


@pytest.mark.parametrize(
    "damage",
    (
        "candidate",
        "family",
        "membership",
        "winner",
        "foreign_member",
        "foreign_winner",
        "missing_index",
        "invalid_index",
        "index_oid",
    ),
)
async def test_closed_census_and_every_serving_index_precede_phase_change(tmp_path, monkeypatch, damage):
    async with _case(tmp_path, monkeypatch) as native:
        boundary = _NativeBoundary(native)
        await boundary.ready(monkeypatch)
        match damage:
            case "candidate":
                boundary.phase = "loading"
            case "family":
                boundary.role_counts["family"] = 0
            case "membership":
                boundary.role_counts["generation_family"] = (0, 0)
            case "winner":
                boundary.role_counts["winner"] = (0, 0)
            case "foreign_member":
                boundary.role_counts["generation_family"] = (1, 0)
            case "foreign_winner":
                boundary.role_counts["winner"] = (2, 1)
            case "missing_index":
                boundary.index_damage = "missing"
            case "invalid_index":
                boundary.index_damage = "ready"
            case "index_oid":
                boundary.index_damage = "oid"
        with pytest.raises(FamilyBlockUnavailable):
            await _complete(native)
        assert boundary.build.phase == "output" and boundary.build.output_frozen_at is None
        assert "output_flush" not in boundary.observations and boundary.observations[-1] == "rollback"


@pytest.mark.parametrize(
    "field,value",
    (
        ("producing_token_sha256", b"x" * 32),
        ("candidate_sha256", b"c" * 32),
        ("root_count", 2),
        ("capture_bundle_id", 999),
    ),
)
async def test_retry_reloads_the_original_generation_header(tmp_path, monkeypatch, field, value):
    async with _case(tmp_path, monkeypatch) as native:
        boundary = _NativeBoundary(native)
        await boundary.ready(monkeypatch)
        await _complete(native)
        setattr(boundary.generation, field, value)
        with pytest.raises(FamilyBlockUnavailable, match="generation header"):
            await _complete(native)
        assert boundary.build.phase == "verifying" and boundary.build.verified_at is None


@pytest.mark.parametrize(
    "field,value",
    (
        ("phase", "verified"),
        ("candidate_error_count", 1),
        ("source_frozen_at", None),
        ("graph_frozen_at", None),
        ("verified_at", fixtures._STAMP),
        ("plan_stage", "base"),
        ("plan_complete_at", None),
        ("completed_family_count", 0),
        ("output_after_profile_slot", 1),
        ("winner_count", 1),
    ),
)
async def test_partial_or_verified_build_cannot_reenter_provisional_freeze(tmp_path, monkeypatch, field, value):
    async with _case(tmp_path, monkeypatch) as native:
        boundary = _NativeBoundary(native)
        await boundary.ready(monkeypatch)
        setattr(boundary.build, field, value)
        with pytest.raises(FamilyBlockUnavailable):
            await _complete(native)
        assert boundary.observations[-1] == "rollback" and "output_flush" not in boundary.observations


async def test_fresh_authority_failure_rolls_back_all_counters_then_retries(tmp_path, monkeypatch):
    async with _case(tmp_path, monkeypatch) as native:
        boundary = _NativeBoundary(native)
        await boundary.ready(monkeypatch)
        failure = RuntimeError("synthetic current authority changed")
        boundary.fail_commit = failure
        with pytest.raises(RuntimeError) as caught:
            await _complete(native)
        assert caught.value is failure
        assert boundary.build.phase == "output" and boundary.build.output_frozen_at is None
        assert (
            boundary.build.generation_family_count
            == boundary.build.candidate_context_count
            == boundary.build.winner_count
            == 0
        )
        boundary.fail_commit = None
        assert await _complete(native) == 21001


@pytest.mark.parametrize("field,value", (("candidate_context_count", 0), ("output_frozen_at", None)))
async def test_retry_requires_exact_completed_accounting(tmp_path, monkeypatch, field, value):
    async with _case(tmp_path, monkeypatch) as native:
        boundary = _NativeBoundary(native)
        await boundary.ready(monkeypatch)
        await _complete(native)
        setattr(boundary.build, field, value)
        boundary.observations.clear()
        with pytest.raises(FamilyBlockUnavailable, match="retry frozen accounting"):
            await _complete(native)
        assert "output_flush" not in boundary.observations and boundary.observations[-1] == "rollback"


async def test_cancellation_after_counter_flush_rolls_back_every_change(tmp_path, monkeypatch):
    async with _case(tmp_path, monkeypatch) as native:
        boundary = _NativeBoundary(native)
        await boundary.ready(monkeypatch)
        entered, released = asyncio.Event(), asyncio.Event()

        async def paused(session):
            await boundary.flush(session)
            entered.set()
            await released.wait()

        monkeypatch.setattr(control, "_flush_page", paused)
        pending = asyncio.create_task(_complete(native))
        try:
            await asyncio.wait_for(entered.wait(), 5)
            pending.cancel()
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(pending, 5)
        finally:
            released.set()
            if not pending.done():
                pending.cancel()
            await asyncio.gather(pending, return_exceptions=True)
        assert boundary.build.phase == "output" and boundary.build.output_frozen_at is None
        assert (
            boundary.build.winner_count
            == boundary.build.candidate_context_count
            == boundary.build.generation_family_count
            == 0
        )
        assert boundary.observations[-1] == "rollback" and tuple(winners.read_winner_rows(native.winners))


async def test_repeated_cancellation_drains_complete_file_reader_before_return(tmp_path, monkeypatch):
    async with _case(tmp_path, monkeypatch) as native:
        boundary = _NativeBoundary(native)
        await boundary.ready(monkeypatch)
        entered, released, finished = threading.Event(), threading.Event(), threading.Event()
        original_reader = control.read_winner_rows

        def paused(*arguments, **keywords):
            with closing(original_reader(*arguments, **keywords)) as rows:
                try:
                    yield next(rows)
                    entered.set()
                    assert released.wait(5)
                    yield from rows
                finally:
                    finished.set()

        monkeypatch.setattr(control, "read_winner_rows", paused)
        pending = asyncio.create_task(_complete(native))
        try:
            assert await asyncio.to_thread(entered.wait, 5)
            pending.cancel()
            await asyncio.sleep(0)
            pending.cancel()
            assert not pending.done() and not finished.is_set()
            released.set()
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(pending, 5)
        finally:
            released.set()
            await asyncio.gather(pending, return_exceptions=True)
        assert finished.is_set() and boundary.observations == []
        assert tuple(winners.read_winner_rows(native.winners))


@pytest.mark.parametrize("damage", (None, "missing", "incomplete", "scope", "digest"))
async def test_native_projection_boundary_keeps_actual_completion_validation(tmp_path, monkeypatch, damage):
    from tests.test_custom_import_family_block_projection_binding import _custody

    async with _case(tmp_path, monkeypatch, projection=True) as native:
        boundary = _NativeBoundary(native)
        await boundary.ready(monkeypatch)
        definition = native.context.request.definition
        evidence = projections._evidence(definition)
        boundary.manifest = projections._manifest(
            definition,
            evidence,
            generation=boundary.generation,
            family_id=boundary.binding.family_id,
            **_custody(boundary.binding),
        )
        match damage:
            case "missing":
                boundary.manifest = None
            case "incomplete":
                boundary.manifest.complete_at = None
            case "scope":
                boundary.manifest.family_id += 1
            case "digest":
                boundary.manifest.projection_sha256 = b"x" * 32
        with nullcontext() if damage is None else pytest.raises(QueryProjectionUnavailable):
            await _complete(native)
        assert "indexes" in boundary.observations and "projection_lookup" in boundary.observations
        assert boundary.build.phase == ("verifying" if damage is None else "output")
        assert boundary.build.verified_at is None
