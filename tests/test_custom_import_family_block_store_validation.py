# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Actual transition/identity code over transactional native-boundary fakes."""

import asyncio
from contextlib import asynccontextmanager
from copy import deepcopy
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from process.custom_import import family_block_store as store
from process.custom_import.family_block_binding import FamilyBlockUnavailable
from process.custom_import.family_block_validate import CandidateScope, check_closed_identity
from tests import test_custom_import_family_block_store as fixture


class _Transaction(fixture._Transaction):
    @asynccontextmanager
    async def begin(self):
        self.pending = []
        previous_by_oid = dict(self.persistence)
        self.events.append("begin")
        try:
            yield
        except BaseException:
            self.persistence = previous_by_oid
            self.pending = []
            self.events.append("rollback")
            raise
        else:
            self._commit_transitions()
            self.committed.extend(self.pending)
            self.events.append("commit")

    def _commit_transitions(self):
        for kind, values in self.pending:
            if kind == "transition":
                for name, value in values.items():
                    if name != "family_id_1":
                        setattr(self.descriptor, name, value)
                self.phase = self.descriptor.phase


def _result(rows):
    return SimpleNamespace(
        mappings=lambda: SimpleNamespace(one_or_none=lambda: rows[0] if rows else None, all=lambda: rows)
    )


def _identity_rows(transaction, layout, request):
    scope = CandidateScope(
        11,
        12,
        request.dataset_id,
        request.definition_revision_id,
        request.schema_revision_id,
        request.execution_id,
        17,
        request.fence,
        store.producer_token_sha256(request),
        request.expected_base_generation_id,
    )
    transaction.snapshot = {name: value for name, value in scope.parameters().items() if name != "base_generation_id"}
    transaction.snapshot["frozen_at"] = None
    transaction.generation = {name: value for name, value in scope.parameters().items() if name != "family_id"}
    transaction.build = dict(transaction.generation)
    descriptor = transaction.descriptor
    descriptor.closed_at = datetime(2026, 1, 1, tzinfo=UTC)
    descriptor.layout_contract = "custom-import/family-block-layout/v1"
    descriptor.definition_sha256 = bytes.fromhex(layout.definition.digest)
    descriptor.collection_slots = dict(layout.collection_slots)
    return check_closed_identity(
        transaction.binding, scope, transaction.snapshot, store._descriptor_state(descriptor), phase=descriptor.phase
    )


def _native_boundaries(monkeypatch, transaction):
    original_execute = store._execute

    async def execute(session, statement, parameters=None):
        source = str(statement)
        if "custom_import_snapshot_family" in source and getattr(statement, "is_select", False):
            transaction.events.append("identity-snapshot")
            return _result([transaction.snapshot])
        if "custom_import_generation" in source and getattr(statement, "is_select", False):
            transaction.events.append("identity-generation")
            return _result([transaction.generation])
        if "custom_import_build_attempt" in source and getattr(statement, "is_select", False):
            transaction.events.append("identity-build")
            return _result([transaction.build])
        if "pg_catalog.pg_class" in source:
            return _result([dict(oid=identity, persistence=kind) for identity, kind in transaction.persistence.items()])
        if source.startswith("ALTER TABLE"):
            for role, table in transaction.binding.tables_by_role.items():
                if table.name in source:
                    transaction.persistence[transaction.binding.oids_by_role[role]] = "p"
        return await original_execute(session, statement, parameters)

    async def indexes(session, binding, **_options):
        transaction.events.append("indexes")
        if not transaction.has_indexes:
            raise FamilyBlockUnavailable("native indexes differ")

    async def census(session, binding, expected):
        transaction.events.append("census")
        if expected != transaction.native_counts:
            raise FamilyBlockUnavailable("native shard census differs")
        if transaction.after_census is not None:
            transaction.after_census()

    async def logged(*_arguments, **options):
        assert options["persistence"] == "logged"
        transaction.events.append("logged-custody")
        if set(transaction.persistence.values()) != {"p"}:
            raise FamilyBlockUnavailable("native logged persistence differs")
        return transaction.binding

    monkeypatch.setattr(store, "_execute", execute)
    monkeypatch.setattr(store, "validate_family_block_indexes", indexes)
    monkeypatch.setattr(store, "_verify_shard_census", census)
    monkeypatch.setattr(store, "resolve_family_block_binding", logged)
    monkeypatch.setattr(store, "_control_schema", lambda _session: _control())


async def _control():
    return fixture.dialect().identifier_preparer, "synthetic"


def _case(monkeypatch, phase="closed"):
    layout, transaction = fixture._layout(), _Transaction(phase=phase)
    fixture._install(monkeypatch, transaction, layout)
    request = fixture._request(definition=layout.definition)
    identity = _identity_rows(transaction, layout, request)
    counts = fixture._checked_empty_counts(layout)
    transaction.native_counts = deepcopy(counts)
    transaction.persistence = {
        identity: "p" if phase == "logged" else "u" for identity in transaction.binding.oids_by_role.values()
    }
    transaction.has_indexes, transaction.after_census = True, None
    if phase != "closed":
        transaction.descriptor.validation_sha256 = b"v" * 32
        for name, value in counts.items():
            setattr(transaction.descriptor, name, deepcopy(value))
    _native_boundaries(monkeypatch, transaction)
    return SimpleNamespace(
        transaction=transaction,
        layout=layout,
        request=request,
        identity=identity,
        arguments=dict(validation_sha256=b"v" * 32, **counts),
    )


async def _run(case, operation, *, identity=None, arguments=None):
    return await getattr(store, operation)(
        case.transaction,
        case.request,
        case.identity if identity is None else identity,
        case.layout,
        **(case.arguments if arguments is None else arguments),
    )


async def test_exact_proof_replays_closed_verified_logged_without_rewriting_history(monkeypatch):
    case = _case(monkeypatch)
    transaction = case.transaction
    await _run(case, "accept_validation")
    assert transaction.phase == "verified" and transaction.events.count("identity-snapshot") == 2
    transitions = transaction.events.count("transition-update")
    await _run(case, "accept_validation")
    assert transaction.events.count("transition-update") == transitions
    await _run(case, "make_logged")
    assert transaction.phase == "logged" and set(transaction.persistence.values()) == {"p"}
    after_durability = len(transaction.committed), transaction.events.count("ddl")
    await _run(case, "make_logged")
    await _run(case, "accept_validation")
    assert (len(transaction.committed), transaction.events.count("ddl")) == after_durability
    assert transaction.events.count("logged-custody") == 3
    assert transaction.events.count("identity-build") == 10
    assert transaction.events[-1] == "commit"


@pytest.mark.parametrize("operation", ["accept_validation", "make_logged"])
@pytest.mark.parametrize("damage", ["digest", "shards", "source", "roles"])
async def test_completed_proof_changes_are_refused_before_history_or_ddl(monkeypatch, operation, damage):
    case = _case(monkeypatch, "verified")
    arguments = deepcopy(case.arguments)
    if damage == "digest":
        arguments["validation_sha256"] = b"x" * 32
    if damage == "shards":
        arguments["expected_completed_shards"] += 1
    if damage == "source":
        arguments["source_record_count"] += 1
    if damage == "roles":
        arguments["role_counts"]["family"] += 1
    with pytest.raises(FamilyBlockUnavailable, match="proof or census"):
        await _run(case, operation, arguments=arguments)
    assert not case.transaction.committed and "ddl" not in case.transaction.events


@pytest.mark.parametrize("damage", ["closure", "owner", "role", "generation", "base"])
async def test_original_identity_must_match_inside_the_actual_transition(monkeypatch, damage):
    case = _case(monkeypatch)
    identity = case.identity
    changed = {
        "closure": replace(identity, closed_at=identity.closed_at + timedelta(seconds=1)),
        "owner": replace(identity, owner_oid=identity.owner_oid + 1),
        "role": replace(identity, role_oids=identity.role_oids[:-1]),
        "generation": replace(identity, scope=replace(identity.scope, generation_id=999)),
        "base": replace(identity, scope=replace(identity.scope, base_generation_id=999)),
    }[damage]
    with pytest.raises(FamilyBlockUnavailable):
        await _run(case, "accept_validation", identity=changed)
    assert not case.transaction.committed and "transition-update" not in case.transaction.events


@pytest.mark.parametrize("damage", ["closure", "owner", "role", "generation", "capture", "build", "build_base"])
async def test_identity_is_rebound_after_current_authority_before_transition(monkeypatch, damage):
    case = _case(monkeypatch)
    transaction = case.transaction

    def changed():
        if damage == "closure":
            transaction.descriptor.closed_at += timedelta(seconds=1)
        if damage == "owner":
            transaction.binding.owner_oid += 1
        if damage == "role":
            transaction.binding.oids_by_role["family"] += 100
        if damage == "generation":
            transaction.generation["generation_id"] += 100
        if damage == "capture":
            transaction.snapshot["capture_bundle_id"] += 100
        if damage == "build":
            transaction.build["generation_id"] += 100
        if damage == "build_base":
            transaction.build["base_generation_id"] = 999

    transaction.after_census = changed
    with pytest.raises(FamilyBlockUnavailable):
        await _run(case, "accept_validation")
    assert not transaction.committed and "transition-update" not in transaction.events
    assert transaction.events[-1] == "rollback"


@pytest.mark.parametrize("damage", ["generation_id", "producing_token_sha256", "base_generation_id"])
async def test_build_scope_is_required_before_the_original_proof_can_transition(monkeypatch, damage):
    case = _case(monkeypatch)
    case.transaction.build[damage] = b"x" * 32 if damage == "producing_token_sha256" else 999
    with pytest.raises(FamilyBlockUnavailable, match="build or base scope"):
        await _run(case, "accept_validation")
    assert "census" not in case.transaction.events and not case.transaction.committed


@pytest.mark.parametrize("phase", ["verified", "logged"])
def test_default_identity_check_remains_closed_only_and_never_accepts_frozen(monkeypatch, phase):
    case = _case(monkeypatch, phase)
    descriptor = store._descriptor_state(case.transaction.descriptor)
    with pytest.raises(FamilyBlockUnavailable, match="layout identity"):
        check_closed_identity(case.transaction.binding, case.identity.scope, case.transaction.snapshot, descriptor)
    case.transaction.snapshot["frozen_at"] = datetime(2026, 1, 2, tzinfo=UTC)
    with pytest.raises(FamilyBlockUnavailable, match="already been frozen"):
        check_closed_identity(
            case.transaction.binding, case.identity.scope, case.transaction.snapshot, descriptor, phase=phase
        )


@pytest.mark.parametrize("phase", ["loading", "unknown"])
def test_identity_check_never_widens_to_an_unclosed_phase(monkeypatch, phase):
    case = _case(monkeypatch)
    with pytest.raises(FamilyBlockUnavailable, match="identity phase"):
        check_closed_identity(
            case.transaction.binding,
            case.identity.scope,
            case.transaction.snapshot,
            store._descriptor_state(case.transaction.descriptor),
            phase=phase,
        )


@pytest.mark.parametrize("operation", ["accept_validation", "make_logged"])
async def test_missing_identity_never_opens_an_acceptance_transaction(monkeypatch, operation):
    case = _case(monkeypatch)
    with pytest.raises(ValueError, match="original candidate identity"):
        await getattr(store, operation)(case.transaction, case.request, None, case.layout, **case.arguments)
    assert not case.transaction.events


@pytest.mark.parametrize("damage", ["census", "indexes", "unlogged"])
async def test_logged_replay_still_checks_actual_native_state(monkeypatch, damage):
    case = _case(monkeypatch, "logged")
    if damage == "census":
        case.transaction.native_counts["source_record_count"] += 1
    if damage == "indexes":
        case.transaction.has_indexes = False
    if damage == "unlogged":
        case.transaction.persistence[next(iter(case.transaction.persistence))] = "u"
    with pytest.raises(FamilyBlockUnavailable):
        await _run(case, "make_logged")
    assert not case.transaction.committed and "ddl" not in case.transaction.events


async def test_verified_mixed_persistence_converts_only_unlogged_roles(monkeypatch):
    case = _case(monkeypatch, "verified")
    first = next(iter(case.transaction.persistence))
    case.transaction.persistence[first] = "p"
    await _run(case, "make_logged")
    assert case.transaction.events.count("ddl") == len(case.transaction.persistence) - 1
    assert case.transaction.phase == "logged"


@pytest.mark.parametrize("damage", ["missing", "extra", "temporary"])
async def test_logging_requires_the_exact_native_role_persistence_census(monkeypatch, damage):
    case = _case(monkeypatch, "verified")
    persistence_by_oid = case.transaction.persistence
    first = next(iter(persistence_by_oid))
    if damage == "missing":
        del persistence_by_oid[first]
    if damage == "extra":
        persistence_by_oid[9999] = "u"
    if damage == "temporary":
        persistence_by_oid[first] = "t"
    with pytest.raises(FamilyBlockUnavailable, match="persistence census"):
        await _run(case, "make_logged")
    assert not case.transaction.committed and "ddl" not in case.transaction.events


@pytest.mark.parametrize("failure", ["expiry", "cancel"])
async def test_logging_rolls_back_on_final_authority_loss(monkeypatch, failure):
    case = _case(monkeypatch, "verified")
    case.transaction.expired = failure == "expiry"
    case.transaction.cancelled = failure == "cancel"
    error = RuntimeError if failure == "expiry" else asyncio.CancelledError
    with pytest.raises(error, match="expired|cancellation"):
        await _run(case, "make_logged")
    assert not case.transaction.committed and case.transaction.phase == "verified"
    assert set(case.transaction.persistence.values()) == {"u"}
