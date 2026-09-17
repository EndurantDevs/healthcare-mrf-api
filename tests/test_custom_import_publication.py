# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import datetime as dt
import json
from decimal import Decimal
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest

from db.models import custom_import as custom_import_models
from db.models.custom_import import CustomImportChildRevision, CustomImportRootRevision
from process.custom_import import publication
from process.custom_import.execution import MAX_BIGINT
from process.custom_import.publication import (
    PublicationConflict,
    _effective_output_revision_document,
    _event_document,
    _generation_publication_request,
    _increment_pointer_version,
    _materialization_document,
    _pointer_version,
    _positive_integer,
    _PublicationEventDetails,
)


@pytest.mark.parametrize(
    ("revision", "source_ordinal"),
    (
        (CustomImportRootRevision(source_ordinal=7), 7),
        (CustomImportChildRevision(source_ordinal=9), 9),
    ),
    ids=("root", "child"),
)
def test_effective_output_revision_document_omits_only_source_position(revision, source_ordinal):
    materialization_document = _materialization_document(revision)
    effective_output_document = _effective_output_revision_document(revision)

    assert materialization_document["source_ordinal"] == source_ordinal
    assert "source_ordinal" not in effective_output_document
    assert effective_output_document == {
        key: value for key, value in materialization_document.items() if key != "source_ordinal"
    }


def test_publication_event_is_canonical_and_domain_separated():
    canonical_event, digest = _event_document(
        _PublicationEventDetails(
            dataset_id=1,
            definition_revision_id=2,
            schema_revision_id=3,
            execution_id=4,
            event_kind="activated",
            from_generation_id=None,
            to_generation_id=5,
            expected_pointer_version=0,
            committed_pointer_version=1,
        )
    )

    assert json.loads(canonical_event) == {
        "committed_pointer_version": 1,
        "contract": "custom-import-publication-event/v1",
        "dataset_id": 1,
        "definition_revision_id": 2,
        "event_kind": "activated",
        "execution_id": 4,
        "expected_pointer_version": 0,
        "from_generation_id": None,
        "schema_revision_id": 3,
        "to_generation_id": 5,
    }
    assert len(digest) == 32
    assert digest != bytes.fromhex("00" * 32)


@pytest.mark.parametrize("value", [True, False, 0, -1, "1", None])
def test_positive_publication_identifiers_reject_ambiguous_values(value):
    with pytest.raises(PublicationConflict, match="positive integer"):
        _positive_integer(value, "synthetic id")


@pytest.mark.parametrize("value", [True, False, -1, "0", None])
def test_pointer_version_rejects_ambiguous_values(value):
    with pytest.raises(PublicationConflict, match="non-negative integer"):
        _pointer_version(value)


def test_pointer_version_accepts_empty_and_existing_versions():
    assert _pointer_version(0) == 0
    assert _pointer_version(7) == 7


def test_publication_identifiers_and_pointer_increment_reject_postgresql_bigint_overflow():
    assert _positive_integer(MAX_BIGINT, "synthetic id") == MAX_BIGINT
    assert _pointer_version(MAX_BIGINT) == MAX_BIGINT
    with pytest.raises(PublicationConflict, match="positive integer"):
        _positive_integer(MAX_BIGINT + 1, "synthetic id")
    with pytest.raises(PublicationConflict, match="non-negative integer"):
        _pointer_version(MAX_BIGINT + 1)
    with pytest.raises(PublicationConflict, match="cannot advance"):
        _increment_pointer_version(MAX_BIGINT)
    with pytest.raises(PublicationConflict, match="cannot advance"):
        _generation_publication_request(
            event_kind="activated",
            dataset_id=1,
            target_generation_id=2,
            expected_generation_id=None,
            expected_pointer_version=MAX_BIGINT,
        )


class _Result:
    def __init__(self, value: Any = None, *, rowcount: int = 0):
        self.value = value
        self.rowcount = rowcount

    def scalar_one_or_none(self):
        return self.value

    def scalar_one(self):
        return self.value


class _ExecuteSession:
    def __init__(self, *results: _Result):
        self.results = iter(results)
        self.info: dict[str, Any] = {}
        self.new = ()
        self.dirty = ()
        self.deleted = ()

    async def execute(self, _statement):
        return next(self.results)

    def in_transaction(self):
        return True


def test_digest_transaction_and_clean_session_guards_fail_closed():
    digest = b"d" * 32
    assert publication._sha256(memoryview(digest), "digest") == digest
    with pytest.raises(PublicationConflict, match="32 bytes"):
        publication._sha256("not-bytes", "digest")
    with pytest.raises(PublicationConflict, match="32 bytes"):
        publication._sha256(b"short", "digest")

    publication._require_transaction(SimpleNamespace(in_transaction=lambda: True))
    with pytest.raises(PublicationConflict, match="caller-owned transaction"):
        publication._require_transaction(SimpleNamespace(in_transaction=lambda: False))

    publication._require_clean_session(SimpleNamespace(new=(), dirty=(), deleted=()))
    with pytest.raises(PublicationConflict, match="clean session"):
        publication._require_clean_session(SimpleNamespace(new=(), dirty=(object(),), deleted=()))


def test_persisted_event_material_must_match_its_canonical_document():
    details = _PublicationEventDetails(
        dataset_id=1,
        definition_revision_id=2,
        schema_revision_id=3,
        execution_id=4,
        event_kind="activated",
        from_generation_id=None,
        to_generation_id=5,
        expected_pointer_version=0,
        committed_pointer_version=1,
    )
    canonical, digest = _event_document(details)
    event = SimpleNamespace(**details.__dict__, canonical_event=canonical, event_sha256=digest)
    publication._verify_event_material(event)
    event.canonical_event = "{}"
    with pytest.raises(PublicationConflict, match="not canonical"):
        publication._verify_event_material(event)


@pytest.mark.asyncio
async def test_locked_publication_loaders_reject_missing_authoritative_rows():
    missing = _ExecuteSession(*(_Result() for _ in range(6)))
    with pytest.raises(PublicationConflict, match="dataset does not exist"):
        await publication._locked_dataset(missing, 1)
    with pytest.raises(PublicationConflict, match="does not belong to the dataset"):
        await publication._generation_execution_id(missing, dataset_id=1, generation_id=2)
    with pytest.raises(PublicationConflict, match="does not belong to the dataset"):
        await publication._generation_snapshot(missing, dataset_id=1, generation_id=2)
    with pytest.raises(PublicationConflict, match="does not belong to the dataset"):
        await publication._locked_execution(missing, execution_id=3, dataset_id=1)
    with pytest.raises(PublicationConflict, match="lease does not exist"):
        await publication._locked_lease(missing, 3)
    with pytest.raises(PublicationConflict, match="does not belong to the dataset"):
        await publication._locked_generation(missing, dataset_id=1, generation_id=2)


@pytest.mark.asyncio
async def test_database_time_and_finality_window_metadata_are_typed():
    with pytest.raises(PublicationConflict, match="aware timestamp"):
        await publication._database_now(_ExecuteSession(_Result(dt.datetime(2026, 9, 17))))

    session = SimpleNamespace(info={publication._FINALITY_SCAN_WINDOW_KEY: "invalid"})
    with pytest.raises(PublicationConflict, match="scan window is invalid"):
        publication._current_finality_scan_window(session)


class _FinalitySession:
    def __init__(self, statement_timeout: Any, *, restore_error: Exception | None = None):
        self.info: dict[str, Any] = {}
        self.statement_timeout = statement_timeout
        self.restore_error = restore_error
        self.restore_calls = 0

    async def scalar(self, _statement):
        return self.statement_timeout

    async def execute(self, _statement):
        self.restore_calls += 1
        if self.restore_error is not None:
            raise self.restore_error
        return _Result()


@pytest.mark.asyncio
async def test_finality_scan_window_rejects_expired_or_untyped_budgets():
    now = dt.datetime(2026, 9, 17, tzinfo=dt.UTC)
    with pytest.raises(PublicationConflict, match="lease expired"):
        async with publication._finality_scan_window(_FinalitySession("1s"), now=now, expires_at=now):
            pytest.fail("an expired finality window yielded")

    with pytest.raises(PublicationConflict, match="current statement timeout"):
        async with publication._finality_scan_window(
            _FinalitySession(1000), now=now, expires_at=now + dt.timedelta(seconds=1)
        ):
            pytest.fail("an untyped statement timeout yielded")


@pytest.mark.asyncio
async def test_finality_scan_window_restores_prior_state_and_preserves_primary_failure():
    now = dt.datetime(2026, 9, 17, tzinfo=dt.UTC)
    previous = publication._FinalityScanWindow(
        expires_at=now + dt.timedelta(seconds=5),
        monotonic_deadline=10.0,
    )
    session = _FinalitySession("5s")
    session.info[publication._FINALITY_SCAN_WINDOW_KEY] = previous
    async with publication._finality_scan_window(
        session,
        now=now,
        expires_at=now + dt.timedelta(seconds=1),
    ):
        assert session.info[publication._FINALITY_SCAN_WINDOW_KEY] is not previous
    assert session.info[publication._FINALITY_SCAN_WINDOW_KEY] is previous
    assert session.restore_calls == 1

    restore_failure = _FinalitySession("5s", restore_error=RuntimeError("restore failed"))
    with pytest.raises(ValueError, match="primary failure"):
        async with publication._finality_scan_window(
            restore_failure,
            now=now,
            expires_at=now + dt.timedelta(seconds=1),
        ):
            raise ValueError("primary failure")

    failed_restore = _FinalitySession("5s", restore_error=RuntimeError("restore failed"))
    with pytest.raises(RuntimeError, match="restore failed"):
        async with publication._finality_scan_window(
            failed_restore,
            now=now,
            expires_at=now + dt.timedelta(seconds=1),
        ):
            assert publication._FINALITY_SCAN_WINDOW_KEY in failed_restore.info

    empty_session = _FinalitySession("5s")
    async with publication._finality_scan_window(
        empty_session,
        now=now,
        expires_at=now + dt.timedelta(seconds=1),
    ):
        assert publication._FINALITY_SCAN_WINDOW_KEY in empty_session.info
    assert publication._FINALITY_SCAN_WINDOW_KEY not in empty_session.info


@pytest.mark.asyncio
async def test_materialization_budget_stops_expired_database_and_client_work(monkeypatch):
    now = dt.datetime(2026, 9, 17, tzinfo=dt.UTC)
    session = _ExecuteSession()
    session.info[publication._FINALITY_SCAN_WINDOW_KEY] = publication._FinalityScanWindow(
        expires_at=now,
        monotonic_deadline=1.0,
    )
    monkeypatch.setattr(publication.time, "monotonic", lambda: 2.0)
    with pytest.raises(PublicationConflict, match="lease-bounded materialization window"):
        await publication._prepare_bounded_materialization_statement(session)
    with pytest.raises(PublicationConflict, match="lease-bounded materialization window"):
        publication._require_materialization_budget(session)


@pytest.mark.asyncio
async def test_finality_lease_renewal_requires_the_guarded_update(monkeypatch):
    now = dt.datetime(2026, 9, 17, tzinfo=dt.UTC)
    token = b"t" * 32
    execution = SimpleNamespace(execution_id=7, state="running")
    lease = SimpleNamespace(
        fence=3,
        token_sha256=token,
        expires_at=now + dt.timedelta(seconds=30),
    )
    monkeypatch.setattr(publication, "_database_now", AsyncMock(return_value=now))
    with pytest.raises(PublicationConflict, match="lost authority"):
        await publication._renew_finality_lease(
            _ExecuteSession(_Result()),
            execution,
            lease,
            fence=3,
            token_sha256=token,
            conflict_message="lost authority",
        )


def test_expected_pointer_and_scalar_materialization_validation():
    publication._require_expected_pointer(
        None,
        expected_generation_id=None,
        expected_pointer_version=0,
    )
    with pytest.raises(PublicationConflict, match="expected empty pointer"):
        publication._require_expected_pointer(
            None,
            expected_generation_id=1,
            expected_pointer_version=0,
        )
    with pytest.raises(PublicationConflict, match="compare-and-swap"):
        publication._require_expected_pointer(
            SimpleNamespace(generation_id=1, pointer_version=2),
            expected_generation_id=1,
            expected_pointer_version=3,
        )

    assert publication._json_value(None) is None
    assert publication._json_value(True) is True
    assert publication._json_value(3) == 3
    assert publication._json_value("value") == "value"
    assert publication._json_value(memoryview(b"\x01\x02")) == "0102"
    assert publication._json_value(Decimal("1.230")) == "1.230"
    assert publication._json_value(dt.date(2026, 9, 17)) == "2026-09-17"
    aware = dt.datetime(2026, 9, 17, 12, 0, tzinfo=dt.timezone(dt.timedelta(hours=2)))
    assert publication._json_value(aware) == "2026-09-17T10:00:00Z"
    with pytest.raises(PublicationConflict, match="naive timestamp"):
        publication._json_value(dt.datetime(2026, 9, 17))
    with pytest.raises(PublicationConflict, match="unsupported scalar"):
        publication._json_value(object())


@pytest.mark.asyncio
async def test_materialization_identity_loaders_reject_missing_or_mismatched_sources(monkeypatch):
    missing = _ExecuteSession(_Result())
    with pytest.raises(PublicationConflict, match="immutable execution identity"):
        await publication._capture_bundle_for_identity(
            missing,
            capture_bundle_id=1,
            dataset_id=2,
            definition_revision_id=3,
            schema_revision_id=4,
        )

    generation = SimpleNamespace(
        capture_bundle_id=1,
        dataset_id=2,
        definition_revision_id=3,
        schema_revision_id=4,
        source_bundle_sha256=b"a" * 32,
    )
    session = SimpleNamespace(get=AsyncMock(return_value=None))
    with pytest.raises(PublicationConflict, match="identity is missing"):
        await publication._effective_output_materialization(session, generation)
    with pytest.raises(PublicationConflict, match="identity is missing"):
        await publication._add_generation_identity_material(
            session,
            publication._new_digest("test-domain"),
            generation,
        )

    monkeypatch.setattr(
        publication,
        "_capture_source_bundle_digest",
        AsyncMock(return_value=b"b" * 32),
    )
    with pytest.raises(PublicationConflict, match="does not match retained captures"):
        await publication._validated_source_bundle_digest(SimpleNamespace(), generation)


def _generation_seal_fixtures():
    token = b"t" * 32
    materialization_digest = b"m" * 32
    effective_digest = b"e" * 32
    generation = SimpleNamespace(
        generation_id=9,
        dataset_id=1,
        definition_revision_id=2,
        schema_revision_id=3,
        execution_id=4,
        capture_bundle_id=5,
        producing_fence=6,
        producing_token_sha256=token,
        root_count=7,
        family_count=8,
    )
    materialization = publication._Materialization(
        source_bundle_sha256=b"s" * 32,
        materialization_sha256=materialization_digest,
        effective_output_sha256=effective_digest,
        root_count=7,
        family_count=8,
        generation_family_count=9,
        family_child_count=10,
        winner_count=11,
        profile_count=12,
        root_scalar_count=13,
        child_scalar_count=14,
    )
    seal = SimpleNamespace(
        seal_contract=publication._GENERATION_SEAL_CONTRACT,
        generation_id=9,
        dataset_id=1,
        definition_revision_id=2,
        schema_revision_id=3,
        execution_id=4,
        capture_bundle_id=5,
        sealing_fence=6,
        sealing_token_sha256=token,
        materialization_sha256=materialization_digest,
        effective_output_sha256=effective_digest,
        root_count=7,
        family_count=8,
        generation_family_count=9,
        family_child_count=10,
        winner_count=11,
        profile_count=12,
        root_scalar_count=13,
        child_scalar_count=14,
    )
    return generation, materialization, seal, token


def test_generation_seal_validation_rejects_identity_authority_counts_and_digests():
    generation, materialization, seal, _token = _generation_seal_fixtures()
    publication._validate_generation_seal(seal, generation, materialization)

    seal.seal_contract = "wrong"
    with pytest.raises(PublicationConflict, match="immutable materialization"):
        publication._validate_generation_seal_identity(seal, generation)
    seal.seal_contract = publication._GENERATION_SEAL_CONTRACT

    generation.producing_fence = None
    with pytest.raises(PublicationConflict, match="authority differs"):
        publication._validate_generation_seal_identity(seal, generation)
    generation.producing_fence = 6

    seal.root_count = 99
    with pytest.raises(PublicationConflict, match="immutable materialization"):
        publication._validate_generation_seal(seal, generation, materialization)
    seal.root_count = 7
    seal.materialization_sha256 = b"x" * 32
    with pytest.raises(PublicationConflict, match="materialization digest"):
        publication._validate_generation_seal(seal, generation, materialization)
    seal.materialization_sha256 = materialization.materialization_sha256
    seal.effective_output_sha256 = b"x" * 32
    with pytest.raises(PublicationConflict, match="effective output digest"):
        publication._validate_generation_seal(seal, generation, materialization)


def test_generation_attempt_and_replay_authority_are_required():
    generation, _materialization, seal, token = _generation_seal_fixtures()
    generation.producing_fence = None
    with pytest.raises(PublicationConflict, match="no producing attempt authority"):
        publication._generation_attempt_authority(generation)
    generation.producing_fence = 6

    request = publication._GenerationSealRequest(
        dataset_id=1,
        generation_id=9,
        lease_fence=7,
        token_sha256=token,
    )
    with pytest.raises(PublicationConflict, match="replay authority differs"):
        publication._generation_seal_replay(seal, generation, request)
    with pytest.raises(PublicationConflict, match="from 1 through"):
        publication._generation_seal_request(
            dataset_id=1,
            generation_id=9,
            lease_fence=6,
            lease_token=b"",
        )


def test_generation_sealing_requires_live_authority_and_exact_counts():
    generation, materialization, _seal, token = _generation_seal_fixtures()
    now = dt.datetime(2026, 9, 17, tzinfo=dt.UTC)
    lease = SimpleNamespace(
        fence=6,
        token_sha256=token,
        expires_at=now + dt.timedelta(seconds=30),
    )
    request = publication._GenerationSealRequest(1, 9, 6, token)
    with pytest.raises(PublicationConflict, match="current running lease"):
        publication._validate_generation_sealing_authority(
            SimpleNamespace(execution_id=4, state="completed"),
            lease,
            generation,
            materialization,
            request,
            now,
        )

    generation.root_count = 99
    with pytest.raises(PublicationConflict, match="exact membership"):
        publication._validate_generation_sealing_authority(
            SimpleNamespace(execution_id=4, state="running"),
            lease,
            generation,
            materialization,
            request,
            now,
        )


def test_publication_target_and_producer_identity_are_exact():
    with pytest.raises(PublicationConflict, match="rollback requires"):
        publication._validate_target_generation(
            SimpleNamespace(event_kind="rolled_back", expected_generation_id=None),
            SimpleNamespace(base_generation_id=None),
        )

    producer = SimpleNamespace(
        state="completed",
        execution_id=1,
        definition_revision_id=2,
        schema_revision_id=3,
        capture_bundle_id=4,
    )
    generation = SimpleNamespace(
        execution_id=99,
        definition_revision_id=2,
        schema_revision_id=3,
        capture_bundle_id=4,
    )
    with pytest.raises(PublicationConflict, match="identity is inconsistent"):
        publication._validate_generation_producer(
            producer,
            generation,
            SimpleNamespace(execution_id=1),
        )


def _no_change_fixtures():
    token = b"t" * 32
    output_digest = b"o" * 32
    request = publication._NoChangeRequest(1, 2, 3, 4, 5, 6, token)
    execution = SimpleNamespace(
        execution_id=2,
        definition_revision_id=7,
        schema_revision_id=8,
        capture_bundle_id=9,
        state="running",
    )
    base_generation = SimpleNamespace(
        generation_id=3,
        source_bundle_sha256=b"b" * 32,
    )
    candidate_generation = SimpleNamespace(
        generation_id=5,
        execution_id=2,
        definition_revision_id=7,
        schema_revision_id=8,
        capture_bundle_id=9,
        source_bundle_sha256=b"c" * 32,
    )
    base_seal = SimpleNamespace(effective_output_sha256=output_digest)
    candidate_seal = SimpleNamespace(effective_output_sha256=output_digest)
    canonical, receipt_digest = publication._no_change_receipt_document(
        request,
        execution,
        base_generation=base_generation,
        candidate_generation=candidate_generation,
        base_seal=base_seal,
        candidate_seal=candidate_seal,
    )
    seal = SimpleNamespace(
        seal_contract=publication._NO_CHANGE_SEAL_CONTRACT,
        dataset_id=1,
        execution_id=2,
        base_generation_id=3,
        candidate_generation_id=5,
        base_pointer_version=4,
        definition_revision_id=7,
        schema_revision_id=8,
        capture_bundle_id=9,
        sealing_fence=6,
        sealing_token_sha256=token,
        base_source_bundle_sha256=base_generation.source_bundle_sha256,
        candidate_source_bundle_sha256=candidate_generation.source_bundle_sha256,
        effective_output_sha256=output_digest,
        canonical_receipt=canonical,
        receipt_sha256=receipt_digest,
    )
    return request, execution, base_generation, candidate_generation, base_seal, candidate_seal, seal


def test_no_change_request_and_receipt_validation_fail_closed():
    with pytest.raises(PublicationConflict, match="from 1 through"):
        publication._no_change_request(
            dataset_id=1,
            execution_id=2,
            expected_generation_id=3,
            expected_pointer_version=4,
            candidate_generation_id=5,
            lease_fence=6,
            lease_token=b"",
        )

    fixtures = _no_change_fixtures()
    request, execution, base, candidate, base_seal, candidate_seal, seal = fixtures
    publication._validate_no_change_seal(seal, request, execution, base, candidate, base_seal, candidate_seal)
    seal.seal_contract = "wrong"
    with pytest.raises(PublicationConflict, match="immutable receipt"):
        publication._validate_no_change_seal(seal, request, execution, base, candidate, base_seal, candidate_seal)
    seal.seal_contract = publication._NO_CHANGE_SEAL_CONTRACT
    seal.canonical_receipt = "{}"
    with pytest.raises(PublicationConflict, match="not canonical"):
        publication._validate_no_change_seal(seal, request, execution, base, candidate, base_seal, candidate_seal)

    with pytest.raises(PublicationConflict, match="publication event does not match"):
        publication._validate_no_change_event(
            SimpleNamespace(event_kind="activated"),
            seal,
        )


@pytest.mark.asyncio
async def test_no_change_state_updates_and_replays_require_complete_receipts(monkeypatch):
    request, execution, base, candidate, base_seal, candidate_seal, seal = _no_change_fixtures()
    with pytest.raises(PublicationConflict, match="execution changed"):
        await publication._finalize_no_change_execution(
            _ExecuteSession(_Result(rowcount=1), _Result(rowcount=0)),
            request,
            dt.datetime(2026, 9, 17, tzinfo=dt.UTC),
        )

    monkeypatch.setattr(publication, "_locked_no_change_seal", AsyncMock(return_value=seal))
    monkeypatch.setattr(publication, "_locked_generation", AsyncMock(side_effect=(base, candidate)))
    monkeypatch.setattr(
        publication,
        "_validated_generation_seal",
        AsyncMock(side_effect=(base_seal, candidate_seal)),
    )
    monkeypatch.setattr(publication, "_validate_no_change_seal", lambda *_args: None)
    monkeypatch.setattr(publication, "_no_change_event", AsyncMock(return_value=None))
    with pytest.raises(PublicationConflict, match="no publication event"):
        await publication._replayed_no_change_receipt(
            SimpleNamespace(),
            request,
            execution,
        )


def test_no_change_candidate_must_belong_to_the_running_execution():
    _request, execution, _base, candidate, *_rest = _no_change_fixtures()
    candidate.execution_id = 99
    with pytest.raises(PublicationConflict, match="does not belong"):
        publication._require_no_change_candidate_execution(execution, candidate)


@pytest.mark.asyncio
async def test_existing_no_change_candidate_seal_requires_exact_authority(monkeypatch):
    request, execution, _base, candidate, _base_seal, _candidate_seal, seal = _no_change_fixtures()
    seal.sealing_fence = request.lease_fence
    seal.sealing_token_sha256 = request.token_sha256
    monkeypatch.setattr(publication, "_locked_generation_seal", AsyncMock(return_value=seal))
    monkeypatch.setattr(publication, "_validate_generation_seal_identity", lambda *_args: None)
    assert (
        await publication._seal_no_change_candidate(SimpleNamespace(), request, execution, SimpleNamespace(), candidate)
        is seal
    )

    seal.sealing_fence += 1
    with pytest.raises(PublicationConflict, match="authority differs"):
        await publication._seal_no_change_candidate(SimpleNamespace(), request, execution, SimpleNamespace(), candidate)


def test_custom_import_schema_names_cannot_disagree(monkeypatch):
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "current")
    monkeypatch.setenv("DB_SCHEMA", "legacy")
    with pytest.raises(RuntimeError, match="must match"):
        custom_import_models._schema()


@pytest.mark.asyncio
async def test_generation_completion_requires_both_guarded_updates():
    request = publication._GenerationSealRequest(1, 2, 3, b"t" * 32)
    with pytest.raises(PublicationConflict, match="producer changed"):
        await publication._complete_generation_sealing_execution(
            _ExecuteSession(_Result(rowcount=1), _Result(rowcount=0)),
            SimpleNamespace(execution_id=4),
            request,
            dt.datetime(2026, 9, 17, tzinfo=dt.UTC),
        )


@pytest.mark.asyncio
async def test_pointer_advance_rejects_current_target_and_lost_compare_and_swap():
    target = SimpleNamespace(definition_revision_id=3, schema_revision_id=4)
    current_request = publication._GenerationPublicationRequest("activated", 1, 2, 2, 3, 4)
    pointer = SimpleNamespace(generation_id=2, pointer_version=3)
    with pytest.raises(PublicationConflict, match="already current"):
        await publication._advance_generation_pointer(_ExecuteSession(), pointer, current_request, target)

    update_request = publication._GenerationPublicationRequest("activated", 1, 2, 1, 3, 4)
    pointer.generation_id = 1
    with pytest.raises(PublicationConflict, match="changed during publication"):
        await publication._advance_generation_pointer(
            _ExecuteSession(_Result(rowcount=0)), pointer, update_request, target
        )


@pytest.mark.asyncio
async def test_no_change_requires_a_current_pointer(monkeypatch):
    request = publication._NoChangeRequest(1, 2, None, 0, 5, 6, b"t" * 32)
    monkeypatch.setattr(publication, "_locked_lease", AsyncMock(return_value=SimpleNamespace()))
    monkeypatch.setattr(publication, "_locked_pointer", AsyncMock(return_value=None))
    with pytest.raises(PublicationConflict, match="requires a current generation"):
        await publication._locked_no_change_candidate(SimpleNamespace(), request, SimpleNamespace())


def test_family_child_must_belong_to_the_selected_generation():
    generation_family = SimpleNamespace(family_revision_id=9)
    family_child = SimpleNamespace(family_revision_id=9, collection_slot=0)
    child_revision = SimpleNamespace(
        canonical_parent_key="root",
        parent_key_sha256=b"r" * 32,
        child_key_sha256=b"c" * 32,
    )
    root_record = SimpleNamespace(
        canonical_logical_key="root",
        logical_key_sha256=b"r" * 32,
    )

    with pytest.raises(PublicationConflict, match="selected generation"):
        publication._validate_and_add_family_child(
            publication._new_digest("test-family-child"),
            (generation_family, family_child, child_revision, root_record),
            {},
            {},
            set(),
            effective_output=False,
        )


@pytest.mark.asyncio
async def test_root_scalar_materialization_hashes_each_retained_projection(monkeypatch):
    scalar = custom_import_models.CustomImportRootScalar(
        root_revision_id=1,
        dataset_id=2,
        schema_revision_id=3,
        root_record_id=4,
        field_slot=5,
        field_collection_slot=0,
        projection_slot=0,
        field_type="string",
        value_state="value",
        string_value="synthetic",
    )
    root_record = SimpleNamespace(logical_key_sha256=b"r" * 32)

    async def _records(_session, _statement):
        yield SimpleNamespace(), SimpleNamespace(), scalar, root_record

    monkeypatch.setattr(publication, "_root_scalar_material_statement", lambda _generation: object())
    monkeypatch.setattr(publication, "_stream_materialization_records", _records)
    digest = publication._new_digest("test-root-scalars")
    digest_before_materialization = digest.digest()
    assert await publication._add_root_scalar_material(SimpleNamespace(), digest, SimpleNamespace()) == 1
    assert digest.digest() != digest_before_materialization


@pytest.mark.asyncio
async def test_generation_sealing_returns_a_replay_found_after_authority_lock(monkeypatch):
    receipt = SimpleNamespace(replayed=True)
    snapshot = SimpleNamespace()
    generation = SimpleNamespace()
    monkeypatch.setattr(publication, "_begin_finality_operation", AsyncMock())
    monkeypatch.setattr(publication, "_locked_dataset", AsyncMock())
    monkeypatch.setattr(publication, "_generation_snapshot", AsyncMock(return_value=snapshot))
    monkeypatch.setattr(
        publication,
        "_replayed_generation_seal",
        AsyncMock(side_effect=(None, receipt)),
    )
    monkeypatch.setattr(
        publication,
        "_lock_generation_sealing_authority",
        AsyncMock(return_value=(SimpleNamespace(), SimpleNamespace(), generation)),
    )
    assert (
        await publication.seal_generation(
            SimpleNamespace(),
            dataset_id=1,
            generation_id=2,
            lease_fence=3,
            lease_token=b"token",
        )
        is receipt
    )
