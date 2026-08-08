# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Unit contracts for fixed publication of one reviewed admission."""

from __future__ import annotations

from contextlib import asynccontextmanager
import datetime as dt

import pytest

import process.formulary_fhir.reviewed_publication as publication_module
import process.formulary_fhir.reviewed_source as reviewed_source_module
from process.formulary_fhir.repository_shared import PublicationResult
from process.formulary_fhir.repository_shared import stable_id
from process.formulary_fhir.reviewed_operation import ACQUISITION_ENABLED_ENV
from process.formulary_fhir.reviewed_operation import PUBLICATION_ENABLED_ENV
from process.formulary_fhir.reviewed_operation import ReviewedOperationError


CUTOFF = dt.datetime(2026, 8, 7, 12, 34, 56, tzinfo=dt.UTC)
BASELINE_VERIFIED_AT = CUTOFF + dt.timedelta(minutes=1)
CANDIDATE_VERIFIED_AT = CUTOFF + dt.timedelta(minutes=2)
ADMITTED_AT = CUTOFF + dt.timedelta(minutes=3)
PUBLISHED_AT = CUTOFF + dt.timedelta(minutes=4)


def _set_gates(monkeypatch, acquisition: str | None, publication: str | None):
    for variable_name, value in (
        (ACQUISITION_ENABLED_ENV, acquisition),
        (PUBLICATION_ENABLED_ENV, publication),
    ):
        if value is None:
            monkeypatch.delenv(variable_name, raising=False)
        else:
            monkeypatch.setenv(variable_name, value)


def _identities():
    return publication_module.reviewed_run_identities(CUTOFF)


def _candidate_row(
    *,
    status: str = "verified",
    **changes: object,
) -> dict[str, object]:
    manifest = publication_module.reviewed_source_manifest()
    identities = _identities()
    row_by_field: dict[str, object] = {
        "source_id": manifest.source_id,
        "baseline_dataset_id": stable_id(
            "ffd_",
            manifest.source_id,
            identities.baseline_run_id,
        ),
        "baseline_run_id": identities.baseline_run_id,
        "candidate_dataset_id": stable_id(
            "ffd_",
            manifest.source_id,
            identities.candidate_run_id,
        ),
        "candidate_run_id": identities.candidate_run_id,
        "predecessor_dataset_id": None,
        "cutoff_at": CUTOFF,
        "source_configuration_hash": "a" * 64,
        "acquisition_contract_hash": "b" * 64,
        "list_count": 2,
        "alias_count": 3,
        "medication_count": 5,
        "coverage_hash": "c" * 64,
        "membership_hash": "d" * 64,
        "alternative_count": 1,
        "alternative_hash": "e" * 64,
        "baseline_verified_at": BASELINE_VERIFIED_AT,
        "candidate_verified_at": CANDIDATE_VERIFIED_AT,
        "admitted_at": ADMITTED_AT,
        "candidate_status": status,
        "candidate_publish_requested": True,
        "candidate_seed_eligible": False,
        "candidate_previous_dataset_id": None,
        "candidate_cutoff_at": CUTOFF,
    }
    row_by_field.update(changes)
    return row_by_field


def _candidate(status: str = "verified"):
    return publication_module._candidate_from_row(
        _candidate_row(status=status),
        publication_module.reviewed_source_manifest(),
        _identities(),
    )


class _SourceDatabase:
    def __init__(self, source_rows):
        self.source_rows = source_rows
        self.statements: list[str] = []

    async def status(self, statement, **_params):
        self.statements.append(statement)
        return None

    async def all(self, statement, **_params):
        self.statements.append(statement)
        return self.source_rows


@pytest.mark.parametrize(
    "acquisition_gate,publication_gate,expected_code",
    [
        (None, None, "disabled"),
        ("true", None, "disabled"),
        ("true", "true", "gate_conflict"),
    ],
)
@pytest.mark.asyncio
async def test_publication_gate_precedes_identity_lease_and_database(
    monkeypatch,
    acquisition_gate,
    publication_gate,
    expected_code,
):
    _set_gates(monkeypatch, acquisition_gate, publication_gate)
    downstream_calls: list[str] = []

    def forbidden_identity(_cutoff):
        downstream_calls.append("identity")
        raise AssertionError("identities derived")

    def forbidden_lease(*_args, **_kwargs):
        downstream_calls.append("lease")
        raise AssertionError("source lease opened")

    monkeypatch.setattr(
        publication_module,
        "reviewed_run_identities",
        forbidden_identity,
    )
    monkeypatch.setattr(
        publication_module.manual_lock,
        "manual_source_lease",
        forbidden_lease,
    )
    with pytest.raises(ReviewedOperationError) as caught:
        await publication_module.publish_reviewed_candidate(
            cutoff=CUTOFF,
            database=object(),
        )

    assert caught.value.code == expected_code
    assert downstream_calls == []


@pytest.mark.asyncio
async def test_publication_source_lock_requires_one_exact_manifest_row():
    manifest = publication_module.reviewed_source_manifest()
    exact_source = reviewed_source_module._source_values(manifest)
    exact_database = _SourceDatabase([exact_source])

    await publication_module._lock_exact_source(exact_database, manifest)

    assert exact_database.statements[0].startswith("LOCK TABLE")
    for source_rows in ([], [{**exact_source, "enabled": False}]):
        with pytest.raises(ReviewedOperationError) as caught:
            await publication_module._lock_exact_source(
                _SourceDatabase(source_rows),
                manifest,
            )
        assert caught.value.code == "evidence"


@pytest.mark.parametrize("status", ["verified", "published"])
def test_candidate_is_fixed_and_accepts_exact_publication_replay(status):
    admission, candidate = _candidate(status)
    identities = _identities()
    manifest = publication_module.reviewed_source_manifest()

    assert candidate.source_id == manifest.source_id
    assert candidate.dataset_id == stable_id(
        "ffd_",
        manifest.source_id,
        identities.candidate_run_id,
    )
    assert candidate.run_id == identities.candidate_run_id
    assert candidate.previous_dataset_id is None
    assert candidate.cutoff_at == CUTOFF
    assert candidate.intent == "requested"
    assert candidate.status == status
    assert admission.candidate_dataset_id == candidate.dataset_id


def test_candidate_accepts_exact_nonnull_predecessor():
    predecessor_dataset_id = "ffd_" + ("9" * 48)
    admission, candidate = publication_module._candidate_from_row(
        _candidate_row(
            predecessor_dataset_id=predecessor_dataset_id,
            candidate_previous_dataset_id=predecessor_dataset_id,
        ),
        publication_module.reviewed_source_manifest(),
        _identities(),
    )

    assert admission.predecessor_dataset_id == predecessor_dataset_id
    assert candidate.previous_dataset_id == predecessor_dataset_id
    publication_result = publication_module._publication_result(
        admission,
        PublicationResult(
            admission.source_id,
            admission.candidate_dataset_id,
            2,
            PUBLISHED_AT,
        ),
    )
    assert publication_result.predecessor_dataset_id == predecessor_dataset_id


@pytest.mark.parametrize(
    "changed_field,changed_value",
    [
        ("source_id", "source-neutral"),
        ("baseline_run_id", "different-baseline-root"),
        ("candidate_run_id", "different-candidate-root"),
        ("baseline_dataset_id", "ffd_" + ("7" * 48)),
        ("candidate_dataset_id", "ffd_" + ("0" * 48)),
        ("cutoff_at", CUTOFF - dt.timedelta(days=1)),
        ("candidate_status", "building"),
        ("candidate_status", "failed"),
        ("candidate_publish_requested", False),
        ("candidate_publish_requested", 1),
        ("candidate_seed_eligible", True),
        ("candidate_previous_dataset_id", "ffd_" + ("1" * 48)),
        ("candidate_cutoff_at", CUTOFF + dt.timedelta(seconds=1)),
    ],
)
def test_candidate_rejects_admission_or_dataset_drift(changed_field, changed_value):
    row_by_field = _candidate_row(**{changed_field: changed_value})
    with pytest.raises(ReviewedOperationError) as caught:
        publication_module._candidate_from_row(
            row_by_field,
            publication_module.reviewed_source_manifest(),
            _identities(),
        )
    assert caught.value.code == "evidence"


class _RowsDatabase:
    def __init__(self, rows):
        self.rows = rows
        self.statement = ""
        self.params: dict[str, object] = {}

    async def all(self, statement, **params):
        self.statement = statement
        self.params = params
        return self.rows


@pytest.mark.asyncio
async def test_candidate_query_is_exact_and_requires_one_admission():
    manifest = publication_module.reviewed_source_manifest()
    identities = _identities()
    database = _RowsDatabase([_candidate_row()])

    admission, candidate = await publication_module._admitted_candidate(
        database,
        manifest,
        identities,
    )
    assert candidate.dataset_id == admission.candidate_dataset_id
    assert database.params == {
        "source_id": manifest.source_id,
        "baseline_run_id": identities.baseline_run_id,
        "candidate_run_id": identities.candidate_run_id,
        "cutoff_at": CUTOFF,
    }
    assert "ORDER BY admission.candidate_dataset_id LIMIT 2" in database.statement

    for rows in ([], [_candidate_row(), _candidate_row()]):
        with pytest.raises(ReviewedOperationError) as caught:
            await publication_module._admitted_candidate(
                _RowsDatabase(rows),
                manifest,
                identities,
            )
        assert caught.value.code == "missing"


class _TransactionDatabase:
    def __init__(self, events):
        self.events = events

    @asynccontextmanager
    async def transaction(self):
        self.events.append("transaction-enter")
        yield
        self.events.append("transaction-exit")


@pytest.mark.parametrize("status", ["verified", "published"])
@pytest.mark.asyncio
async def test_transaction_locks_then_publishes_fixed_candidate(
    monkeypatch,
    status,
):
    """Require transaction, source lock, admission read, then publication."""
    events: list[str] = []
    database = _TransactionDatabase(events)
    admission, candidate = _candidate(status)

    async def lock_source(observed_database, manifest):
        assert observed_database is database
        assert manifest.source_id == admission.source_id
        events.append("source-lock")

    async def admitted_candidate(observed_database, manifest, identities):
        assert observed_database is database
        assert manifest.source_id == admission.source_id
        assert identities == _identities()
        events.append("candidate")
        return admission, candidate

    class Repository:
        def __init__(self, *, source_id, database: object):
            assert source_id == admission.source_id
            assert database is database_instance

        async def publish_dataset(self, *, dataset):
            assert dataset is candidate
            events.append("publish")
            return PublicationResult(
                admission.source_id,
                admission.candidate_dataset_id,
                7,
                PUBLISHED_AT,
            )

    database_instance = database
    monkeypatch.setattr(publication_module, "_lock_exact_source", lock_source)
    monkeypatch.setattr(
        publication_module,
        "_admitted_candidate",
        admitted_candidate,
    )
    monkeypatch.setattr(publication_module, "FHIRFormularyRepository", Repository)
    publication_result = await publication_module._publish_transaction(
        database,
        _identities(),
    )

    assert publication_result.candidate_dataset_id == candidate.dataset_id
    assert publication_result.predecessor_dataset_id is None
    assert publication_result.generation == 7
    assert publication_result.published_at == PUBLISHED_AT
    assert events == [
        "transaction-enter",
        "source-lock",
        "candidate",
        "publish",
        "transaction-exit",
    ]


@pytest.mark.asyncio
async def test_publication_holds_exact_source_lease(monkeypatch):
    _set_gates(monkeypatch, None, "true")
    database = object()
    events: list[str] = []
    expected_result = publication_module._publication_result(
        _candidate()[0],
        PublicationResult(
            publication_module.reviewed_source_manifest().source_id,
            _candidate()[0].candidate_dataset_id,
            7,
            PUBLISHED_AT,
        ),
    )

    @asynccontextmanager
    async def source_lease(
        observed_database,
        source_id,
        *,
        wait_seconds,
        retry_seconds,
    ):
        assert observed_database is database
        assert source_id == publication_module.reviewed_source_manifest().source_id
        assert wait_seconds > retry_seconds > 0
        events.append("lease-enter")
        yield
        events.append("lease-exit")

    async def publish_transaction(observed_database, identities):
        assert observed_database is database
        assert identities == _identities()
        events.append("publish")
        return expected_result

    monkeypatch.setattr(
        publication_module.manual_lock,
        "manual_source_lease",
        source_lease,
    )
    monkeypatch.setattr(
        publication_module,
        "_publish_transaction",
        publish_transaction,
    )

    publication_result = await publication_module.publish_reviewed_candidate(
        cutoff=CUTOFF,
        database=database,
    )
    assert publication_result == expected_result
    assert events == ["lease-enter", "publish", "lease-exit"]
