# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Unit contracts for the fixed synthetic seed publisher."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
import datetime as dt
from dataclasses import replace

import pytest

import process.formulary_fhir.synthetic_seed_publisher as publisher_module
from process.formulary_fhir.manual_lock import ManualSourceLockError
from process.formulary_fhir.repository_shared import DatasetVerification
from process.formulary_fhir.repository_shared import PublicationResult
from process.formulary_fhir.synthetic_canary_contract import CANARY_CUTOFF
from process.formulary_fhir.synthetic_canary_contract import (
    CANARY_FINAL_TABLE_COUNTS,
)
from process.formulary_fhir.synthetic_canary_contract import (
    CANARY_PUBLISHED_TABLE_COUNTS,
)
from process.formulary_fhir.synthetic_canary_contract import CANARY_RUN_ID
from process.formulary_fhir.synthetic_canary_contract import CANARY_SOURCE_ID
from process.formulary_fhir.synthetic_canary_contract import (
    SEED_PUBLICATION_ENABLED_ENV,
)
from process.formulary_fhir.synthetic_canary_contract import expected_evidence
from process.formulary_fhir.synthetic_seed_publisher import (
    SyntheticSeedPublicationError,
)
from process.formulary_fhir.synthetic_seed_publisher import (
    SyntheticSeedPublicationResult,
)
from process.formulary_fhir.synthetic_seed_publisher import publication_result_json
from process.formulary_fhir.synthetic_seed_publisher import publish_synthetic_seed
from tests.test_formulary_fhir_synthetic_canary import _source_row


PUBLISHED_AT = dt.datetime(2026, 8, 7, 18, tzinfo=dt.UTC)


def _dataset_row(
    *,
    status: str = "verified",
    **changes: object,
) -> dict[str, object]:
    expected_by_field = expected_evidence()
    dataset_by_field: dict[str, object] = {
        "source_id": CANARY_SOURCE_ID,
        "dataset_id": expected_by_field["dataset_id"],
        "run_id": CANARY_RUN_ID,
        "previous_dataset_id": None,
        "cutoff_at": CANARY_CUTOFF,
        "status": status,
        "publish_requested": False,
        "seed_eligible": True,
        "list_count": 1,
        "alias_count": 2,
        "medication_count": 2,
        "coverage_hash": expected_by_field["coverage_hash"],
        "membership_hash": expected_by_field["membership_hash"],
        "summary_json": publisher_module._expected_summary(),
        "verified_at": PUBLISHED_AT - dt.timedelta(minutes=1),
        "published_at": PUBLISHED_AT if status == "published" else None,
        "failed_at": None,
        "error_json": None,
    }
    dataset_by_field.update(changes)
    return dataset_by_field


def _pointer() -> dict[str, object]:
    return {
        "source_id": CANARY_SOURCE_ID,
        "dataset_id": expected_evidence()["dataset_id"],
        "generation": 1,
        "published_at": PUBLISHED_AT,
    }


def _verification(**changes: object) -> DatasetVerification:
    expected_by_field = expected_evidence()
    verification_by_field: dict[str, object] = {
        "source_id": CANARY_SOURCE_ID,
        "dataset_id": expected_by_field["dataset_id"],
        "list_count": 1,
        "alias_count": 2,
        "medication_membership_count": 2,
        "coverage_hash": expected_by_field["coverage_hash"],
        "membership_hash": expected_by_field["membership_hash"],
    }
    verification_by_field.update(changes)
    return DatasetVerification(**verification_by_field)


def _publication_result() -> SyntheticSeedPublicationResult:
    expected_by_field = expected_evidence()
    return SyntheticSeedPublicationResult(
        dataset_id=expected_by_field["dataset_id"],
        generation=1,
        published_at=PUBLISHED_AT,
        source_configuration_hash=expected_by_field[
            "source_configuration_hash"
        ],
        acquisition_contract_hash=expected_by_field[
            "acquisition_contract_hash"
        ],
        list_count=1,
        alias_count=2,
        medication_membership_count=2,
        coverage_hash=expected_by_field["coverage_hash"],
        membership_hash=expected_by_field["membership_hash"],
    )


@pytest.mark.parametrize("disabled_setting", [None, "", "0", "false", "typo"])
@pytest.mark.asyncio
async def test_publication_gate_rejects_before_lease(monkeypatch, disabled_setting):
    if disabled_setting is None:
        monkeypatch.delenv(SEED_PUBLICATION_ENABLED_ENV, raising=False)
    else:
        monkeypatch.setenv(SEED_PUBLICATION_ENABLED_ENV, disabled_setting)
    lease_calls: list[bool] = []

    def forbidden_lease(*_args, **_kwargs):
        lease_calls.append(True)
        raise AssertionError("source lease opened")

    monkeypatch.setattr(
        publisher_module.manual_lock,
        "manual_source_lease",
        forbidden_lease,
    )

    with pytest.raises(SyntheticSeedPublicationError) as caught:
        await publish_synthetic_seed(database=object())

    assert caught.value.code == "disabled"
    assert lease_calls == []


@pytest.mark.parametrize("enabled_setting", ["1", "true", "YES", " on "])
@pytest.mark.asyncio
async def test_publication_holds_exact_source_lease(monkeypatch, enabled_setting):
    monkeypatch.setenv(SEED_PUBLICATION_ENABLED_ENV, enabled_setting)
    events: list[str] = []

    @asynccontextmanager
    async def source_lease(database, source_id, *, wait_seconds, retry_seconds):
        assert database is publisher_module.db
        assert source_id == CANARY_SOURCE_ID
        assert wait_seconds > retry_seconds > 0
        events.append("lock")
        yield
        events.append("unlock")

    async def publish_transaction(database):
        assert database is publisher_module.db
        events.append("publish")
        return _publication_result()

    monkeypatch.setattr(
        publisher_module.manual_lock,
        "manual_source_lease",
        source_lease,
    )
    monkeypatch.setattr(
        publisher_module,
        "_publish_transaction",
        publish_transaction,
    )

    publication = await publish_synthetic_seed()

    assert publication == _publication_result()
    assert events == ["lock", "publish", "unlock"]


@pytest.mark.asyncio
async def test_publication_maps_lock_and_private_failures(monkeypatch):
    monkeypatch.setenv(SEED_PUBLICATION_ENABLED_ENV, "true")

    @asynccontextmanager
    async def unavailable_lease(*_args, **_kwargs):
        raise ManualSourceLockError("busy")
        yield

    monkeypatch.setattr(
        publisher_module.manual_lock,
        "manual_source_lease",
        unavailable_lease,
    )
    with pytest.raises(SyntheticSeedPublicationError) as caught:
        await publish_synthetic_seed(database=object())
    assert caught.value.code == "busy"

    @asynccontextmanager
    async def source_lease(*_args, **_kwargs):
        yield

    async def fail_transaction(_database):
        raise RuntimeError("https://private.invalid/fhir?token=secret")

    monkeypatch.setattr(
        publisher_module.manual_lock,
        "manual_source_lease",
        source_lease,
    )
    monkeypatch.setattr(
        publisher_module,
        "_publish_transaction",
        fail_transaction,
    )
    with pytest.raises(SyntheticSeedPublicationError) as caught:
        await publish_synthetic_seed(database=object())
    assert caught.value.code == "publication"
    assert "private" not in str(caught.value)

    domain_error = SyntheticSeedPublicationError("evidence")

    async def fail_with_domain_error(_database):
        raise domain_error

    monkeypatch.setattr(
        publisher_module,
        "_publish_transaction",
        fail_with_domain_error,
    )
    with pytest.raises(SyntheticSeedPublicationError) as caught:
        await publish_synthetic_seed(database=object())
    assert caught.value is domain_error


@pytest.mark.asyncio
async def test_publication_timeout_and_task_cancellation_exit_lease(monkeypatch):
    monkeypatch.setenv(SEED_PUBLICATION_ENABLED_ENV, "true")
    monkeypatch.setattr(publisher_module, "CANARY_PUBLICATION_TIMEOUT_SECONDS", 0)
    lease_exits: list[str] = []

    @asynccontextmanager
    async def source_lease(*_args, **_kwargs):
        try:
            yield
        finally:
            lease_exits.append("exit")

    async def blocked_transaction(_database):
        await asyncio.Event().wait()

    monkeypatch.setattr(
        publisher_module.manual_lock,
        "manual_source_lease",
        source_lease,
    )
    monkeypatch.setattr(
        publisher_module,
        "_publish_transaction",
        blocked_transaction,
    )
    with pytest.raises(TimeoutError):
        await publish_synthetic_seed(database=object())
    assert lease_exits == ["exit"]

    monkeypatch.setattr(publisher_module, "CANARY_PUBLICATION_TIMEOUT_SECONDS", 60)
    publication_task = asyncio.create_task(
        publish_synthetic_seed(database=object())
    )
    await asyncio.sleep(0)
    publication_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await publication_task
    assert lease_exits == ["exit", "exit"]


@pytest.mark.parametrize(
    "changed_field,changed_value",
    [
        ("source_id", "source-beta"),
        ("dataset_id", "ffd_" + ("0" * 48)),
        ("run_id", "different-run"),
        ("previous_dataset_id", "ffd_" + ("1" * 48)),
        ("cutoff_at", CANARY_CUTOFF - dt.timedelta(days=1)),
        ("publish_requested", True),
        ("seed_eligible", False),
        ("list_count", 0),
        ("alias_count", 1),
        ("medication_count", 1),
        ("coverage_hash", "0" * 64),
        ("membership_hash", "0" * 64),
        ("status", "building"),
        ("verified_at", None),
        ("verified_at", dt.datetime(2026, 8, 7, 17, 59)),
        ("failed_at", PUBLISHED_AT),
        ("error_json", {"type": "failure"}),
        ("summary_json", {}),
        ("summary_json", "{"),
    ],
)
def test_candidate_requires_every_exact_field(changed_field, changed_value):
    assert publisher_module._candidate_dataset(_dataset_row()).status == "verified"
    with pytest.raises(SyntheticSeedPublicationError) as caught:
        publisher_module._candidate_dataset(
            _dataset_row(**{changed_field: changed_value})
        )
    assert caught.value.code == "evidence"


def test_candidate_accepts_exact_published_replay():
    dataset = publisher_module._candidate_dataset(_dataset_row(status="published"))
    assert dataset.status == "published" and dataset.intent == "seed"


def test_exact_state_requires_lifecycle_pointer_and_counts():
    publisher_module._require_exact_state(
        _dataset_row(),
        {},
        CANARY_FINAL_TABLE_COUNTS,
    )
    publisher_module._require_exact_state(
        _dataset_row(status="published"),
        _pointer(),
        CANARY_PUBLISHED_TABLE_COUNTS,
    )
    invalid_states = (
        (_dataset_row(published_at=PUBLISHED_AT), {}, CANARY_FINAL_TABLE_COUNTS),
        (_dataset_row(), _pointer(), CANARY_FINAL_TABLE_COUNTS),
        (_dataset_row(), {}, CANARY_PUBLISHED_TABLE_COUNTS),
        (
            _dataset_row(status="published"),
            _pointer() | {"generation": 2},
            CANARY_PUBLISHED_TABLE_COUNTS,
        ),
        (
            _dataset_row(status="published", published_at=None),
            _pointer(),
            CANARY_PUBLISHED_TABLE_COUNTS,
        ),
        (
            _dataset_row(status="published"),
            _pointer() | {"source_id": "source-beta"},
            CANARY_PUBLISHED_TABLE_COUNTS,
        ),
        (
            _dataset_row(status="published"),
            _pointer() | {"dataset_id": "ffd_" + ("0" * 48)},
            CANARY_PUBLISHED_TABLE_COUNTS,
        ),
        (
            _dataset_row(status="published"),
            _pointer()
            | {"published_at": PUBLISHED_AT + dt.timedelta(seconds=1)},
            CANARY_PUBLISHED_TABLE_COUNTS,
        ),
        (
            _dataset_row(status="published"),
            _pointer(),
            CANARY_FINAL_TABLE_COUNTS,
        ),
        (_dataset_row(status="published"), {}, CANARY_PUBLISHED_TABLE_COUNTS),
    )
    for dataset_by_field, pointer_by_field, counts_by_table in invalid_states:
        with pytest.raises(SyntheticSeedPublicationError, match="catalog"):
            publisher_module._require_exact_state(
                dataset_by_field,
                pointer_by_field,
                counts_by_table,
            )


def test_exact_verification_rejects_every_drift():
    publisher_module._require_exact_verification(_verification())
    for changed_field, changed_value in (
        ("source_id", "source-beta"),
        ("dataset_id", "ffd_" + ("0" * 48)),
        ("list_count", 0),
        ("alias_count", 1),
        ("medication_membership_count", 1),
        ("coverage_hash", "0" * 64),
        ("membership_hash", "0" * 64),
    ):
        with pytest.raises(SyntheticSeedPublicationError, match="evidence"):
            publisher_module._require_exact_verification(
                _verification(**{changed_field: changed_value})
            )
    with pytest.raises(SyntheticSeedPublicationError, match="evidence"):
        publisher_module._require_exact_verification(object())


def test_publication_json_is_exact_and_rejects_drift():
    rendered = publication_result_json(_publication_result())
    assert '"status":"published"' in rendered
    assert '"generation":1' in rendered
    assert '"published_at":"2026-08-07T18:00:00Z"' in rendered
    assert CANARY_SOURCE_ID not in rendered

    for changed_field, changed_value in (
        ("dataset_id", "ffd_" + ("0" * 48)),
        ("generation", 2),
        ("published_at", "not-a-timestamp"),
        ("published_at", dt.datetime(2026, 8, 7, 18)),
        ("source_configuration_hash", "0" * 64),
        ("acquisition_contract_hash", "0" * 64),
        ("list_count", 0),
        ("alias_count", 1),
        ("medication_membership_count", 1),
        ("coverage_hash", "0" * 64),
        ("membership_hash", "0" * 64),
    ):
        with pytest.raises(SyntheticSeedPublicationError, match="evidence"):
            publication_result_json(
                replace(
                    _publication_result(),
                    **{changed_field: changed_value},
                )
            )
    with pytest.raises(SyntheticSeedPublicationError, match="evidence"):
        publication_result_json(object())


def test_publication_result_requires_exact_repository_evidence():
    repository_publication = PublicationResult(
        CANARY_SOURCE_ID,
        expected_evidence()["dataset_id"],
        1,
        PUBLISHED_AT,
    )
    publisher_module._require_exact_publication(
        repository_publication,
        _dataset_row(status="published"),
        _pointer(),
    )
    for bad_publication in (
        object(),
        PublicationResult("source-beta", repository_publication.dataset_id, 1, PUBLISHED_AT),
        PublicationResult(CANARY_SOURCE_ID, "ffd_" + ("0" * 48), 1, PUBLISHED_AT),
        PublicationResult(CANARY_SOURCE_ID, repository_publication.dataset_id, 2, PUBLISHED_AT),
        PublicationResult(
            CANARY_SOURCE_ID,
            repository_publication.dataset_id,
            1,
            dt.datetime(2026, 8, 7, 18),
        ),
    ):
        with pytest.raises(SyntheticSeedPublicationError, match="publication"):
            publisher_module._require_exact_publication(
                bad_publication,
                _dataset_row(status="published"),
                _pointer(),
            )
    for dataset_by_field, pointer_by_field in (
        (
            _dataset_row(
                status="published",
                published_at=PUBLISHED_AT + dt.timedelta(seconds=1),
            ),
            _pointer(),
        ),
        (
            _dataset_row(status="published"),
            _pointer()
            | {"published_at": PUBLISHED_AT + dt.timedelta(seconds=1)},
        ),
    ):
        with pytest.raises(SyntheticSeedPublicationError, match="publication"):
            publisher_module._require_exact_publication(
                repository_publication,
                dataset_by_field,
                pointer_by_field,
            )


def test_source_comparison_is_exact_and_disabled():
    assert publisher_module._is_exact_disabled_source(_source_row(enabled=False))
    assert not publisher_module._is_exact_disabled_source(_source_row())
    assert not publisher_module._is_exact_disabled_source(
        _source_row(enabled=False) | {"metadata_json": {"synthetic": 1}}
    )


def test_publication_error_unknown_code_is_sanitized():
    publication_error = SyntheticSeedPublicationError("private-detail")
    assert publication_error.code == "publication"
    assert "private" not in str(publication_error)
