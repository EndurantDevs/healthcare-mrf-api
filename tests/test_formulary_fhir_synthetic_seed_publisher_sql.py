# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""SQL, lock-order, and rollback tests for the fixed seed publisher."""

from __future__ import annotations

from contextlib import asynccontextmanager
import datetime as dt
from dataclasses import replace

import pytest

import process.formulary_fhir.synthetic_seed_publisher as publisher_module
from process.formulary_fhir.repository_shared import PublicationResult
from process.formulary_fhir.synthetic_canary_contract import CANARY_SOURCE_ID
from process.formulary_fhir.synthetic_canary_contract import expected_evidence
from process.formulary_fhir.synthetic_seed_publisher import (
    SyntheticSeedPublicationError,
)
from process.formulary_fhir.synthetic_seed_publisher import (
    publication_result_json,
)
from tests.test_formulary_fhir_synthetic_canary import _source_row
from tests.test_formulary_fhir_synthetic_seed_publisher import PUBLISHED_AT
from tests.test_formulary_fhir_synthetic_seed_publisher import _dataset_row
from tests.test_formulary_fhir_synthetic_seed_publisher import _pointer
from tests.test_formulary_fhir_synthetic_seed_publisher import _publication_result
from tests.test_formulary_fhir_synthetic_seed_publisher import _verification


class _Database:
    def __init__(
        self,
        *,
        all_results: list[object] | None = None,
        first_results: list[object] | None = None,
        scalar_results: list[object] | None = None,
        status_results: list[object] | None = None,
    ) -> None:
        self.all_results = list(all_results or [])
        self.first_results = list(first_results or [])
        self.scalar_results = list(scalar_results or [])
        self.status_results = list(status_results or [])
        self.calls: list[tuple[str, str, dict[str, object]]] = []

    @staticmethod
    def _next(responses: list[object], operation: str) -> object:
        if not responses:
            raise AssertionError(f"unexpected {operation} call")
        response = responses.pop(0)
        if isinstance(response, BaseException):
            raise response
        return response

    async def all(self, statement: str, **params: object):
        self.calls.append(("all", statement, params))
        return self._next(self.all_results, "all")

    async def first(self, statement: str, **params: object):
        self.calls.append(("first", statement, params))
        return self._next(self.first_results, "first")

    async def scalar(self, statement: str, **params: object):
        self.calls.append(("scalar", statement, params))
        return self._next(self.scalar_results, "scalar")

    async def status(self, statement: str, **params: object):
        self.calls.append(("status", statement, params))
        if self.status_results:
            return self._next(self.status_results, "status")
        return 1

    @asynccontextmanager
    async def transaction(self):
        self.calls.append(("transaction", "begin", {}))
        try:
            yield
        except BaseException:
            self.calls.append(("transaction", "rollback", {}))
            raise
        else:
            self.calls.append(("transaction", "commit", {}))


def _scalar_counts(counts_by_table: dict[str, int]) -> list[int]:
    return [
        counts_by_table[table]
        for table in publisher_module.CANARY_PUBLISHED_TABLE_COUNTS
    ]


@pytest.mark.asyncio
async def test_source_lock_uses_table_then_exact_disabled_row():
    database = _Database(all_results=[[_source_row(enabled=False)]])

    await publisher_module._lock_exact_source(database)

    assert [call[0] for call in database.calls] == ["status", "all"]
    assert "LOCK TABLE" in database.calls[0][1]
    assert "FOR UPDATE" in database.calls[1][1]
    assert CANARY_SOURCE_ID not in database.calls[1][1]

    for source_records in (
        [],
        [_source_row()],
        [_source_row(), _source_row(enabled=False)],
    ):
        with pytest.raises(SyntheticSeedPublicationError, match="catalog"):
            await publisher_module._lock_exact_source(
                _Database(all_results=[source_records])
            )


@pytest.mark.asyncio
async def test_fixed_dataset_and_pointer_queries_are_fully_bound():
    database = _Database(
        first_results=[_dataset_row(), _pointer()],
    )

    dataset_by_field = await publisher_module._locked_dataset_row(database)
    pointer_by_field = await publisher_module._locked_pointer(database)

    assert dataset_by_field["status"] == "verified"
    assert pointer_by_field["generation"] == 1
    dataset_call, pointer_call = database.calls
    assert CANARY_SOURCE_ID not in dataset_call[1]
    assert expected_evidence()["dataset_id"] not in dataset_call[1]
    assert dataset_call[2] == {
        "source_id": CANARY_SOURCE_ID,
        "dataset_id": expected_evidence()["dataset_id"],
        "run_id": publisher_module.CANARY_RUN_ID,
    }
    assert pointer_call[2] == {"source_id": CANARY_SOURCE_ID}


@pytest.mark.asyncio
async def test_table_counts_query_every_fixed_table():
    expected_counts = publisher_module.CANARY_PUBLISHED_TABLE_COUNTS
    database = _Database(scalar_results=_scalar_counts(expected_counts))

    assert await publisher_module._table_counts(database) == expected_counts
    assert len(database.calls) == len(expected_counts)
    assert all(not call[2] for call in database.calls)


def test_published_state_rejects_naive_timestamp():
    with pytest.raises(SyntheticSeedPublicationError, match="catalog"):
        publisher_module._require_exact_state(
            _dataset_row(
                status="published",
                published_at=dt.datetime(2026, 8, 7, 18),
            ),
            _pointer(),
            publisher_module.CANARY_PUBLISHED_TABLE_COUNTS,
        )


def test_exact_integer_contract_rejects_boolean_values():
    for count_field in ("list_count", "alias_count", "medication_count"):
        with pytest.raises(SyntheticSeedPublicationError, match="evidence"):
            publisher_module._candidate_dataset(
                _dataset_row(**{count_field: True})
            )
    for count_field in (
        "list_count",
        "alias_count",
        "medication_membership_count",
    ):
        with pytest.raises(SyntheticSeedPublicationError, match="evidence"):
            publisher_module._require_exact_verification(
                _verification(**{count_field: True})
            )
        with pytest.raises(SyntheticSeedPublicationError, match="evidence"):
            publication_result_json(
                replace(_publication_result(), **{count_field: True})
            )
    with pytest.raises(SyntheticSeedPublicationError, match="catalog"):
        publisher_module._require_exact_state(
            _dataset_row(status="published"),
            _pointer() | {"generation": True},
            publisher_module.CANARY_PUBLISHED_TABLE_COUNTS,
        )
    forged_publication = PublicationResult(
        CANARY_SOURCE_ID,
        expected_evidence()["dataset_id"],
        True,
        PUBLISHED_AT,
    )
    with pytest.raises(SyntheticSeedPublicationError, match="publication"):
        publisher_module._require_exact_publication(
            forged_publication,
            _dataset_row(status="published"),
            _pointer(),
        )
    with pytest.raises(SyntheticSeedPublicationError, match="evidence"):
        publication_result_json(replace(_publication_result(), generation=True))


@pytest.mark.asyncio
async def test_preflight_uses_source_dataset_graph_pointer_order(monkeypatch):
    events: list[str] = []
    dataset_by_field = _dataset_row()

    async def lock_source(_database):
        events.append("source")

    async def locked_dataset(_database):
        events.append("dataset")
        return dataset_by_field

    async def recompute(_database, source_id, dataset):
        assert source_id == CANARY_SOURCE_ID
        assert dataset.status == "verified"
        events.append("graph")
        return _verification()

    async def locked_pointer(_database):
        events.append("pointer")
        return {}

    async def table_counts(_database):
        events.append("counts")
        return publisher_module.CANARY_FINAL_TABLE_COUNTS

    monkeypatch.setattr(publisher_module, "_lock_exact_source", lock_source)
    monkeypatch.setattr(publisher_module, "_locked_dataset_row", locked_dataset)
    monkeypatch.setattr(
        publisher_module,
        "_recompute_dataset_verification",
        recompute,
    )
    monkeypatch.setattr(publisher_module, "_locked_pointer", locked_pointer)
    monkeypatch.setattr(publisher_module, "_table_counts", table_counts)

    dataset = await publisher_module._preflight(object())

    assert dataset.status == "verified"
    assert events == ["source", "dataset", "graph", "pointer", "counts"]


@pytest.mark.asyncio
async def test_preflight_rejects_recomputed_evidence_drift(monkeypatch):
    async def no_op(_database):
        return None

    async def locked_dataset(_database):
        return _dataset_row()

    async def recompute(*_args):
        return _verification(coverage_hash="0" * 64)

    monkeypatch.setattr(publisher_module, "_lock_exact_source", no_op)
    monkeypatch.setattr(publisher_module, "_locked_dataset_row", locked_dataset)
    monkeypatch.setattr(
        publisher_module,
        "_recompute_dataset_verification",
        recompute,
    )

    with pytest.raises(SyntheticSeedPublicationError, match="evidence"):
        await publisher_module._preflight(object())


@pytest.mark.asyncio
async def test_postflight_requires_published_exact_state(monkeypatch):
    repository_publication = PublicationResult(
        CANARY_SOURCE_ID,
        expected_evidence()["dataset_id"],
        1,
        PUBLISHED_AT,
    )

    async def source_rows(_database):
        return (_source_row(enabled=False),)

    async def published_dataset(_database):
        return _dataset_row(status="published")

    async def pointer(_database):
        return _pointer()

    async def counts(_database):
        return publisher_module.CANARY_PUBLISHED_TABLE_COUNTS

    monkeypatch.setattr(publisher_module, "_source_rows", source_rows)
    monkeypatch.setattr(
        publisher_module,
        "_locked_dataset_row",
        published_dataset,
    )
    monkeypatch.setattr(publisher_module, "_locked_pointer", pointer)
    monkeypatch.setattr(publisher_module, "_table_counts", counts)

    publication = await publisher_module._postflight(
        object(),
        repository_publication,
    )
    assert publication.generation == 1
    assert publication.published_at == PUBLISHED_AT

    async def enabled_source_rows(_database):
        return (_source_row(),)

    monkeypatch.setattr(publisher_module, "_source_rows", enabled_source_rows)
    with pytest.raises(SyntheticSeedPublicationError, match="publication"):
        await publisher_module._postflight(object(), repository_publication)

    for invalid_source_records in ((), (_source_row(), _source_row())):
        async def invalid_sources(_database, records=invalid_source_records):
            return records

        monkeypatch.setattr(publisher_module, "_source_rows", invalid_sources)
        with pytest.raises(SyntheticSeedPublicationError, match="publication"):
            await publisher_module._postflight(object(), repository_publication)


@pytest.mark.asyncio
async def test_postflight_rejects_verified_dataset(monkeypatch):
    async def source_rows(_database):
        return (_source_row(enabled=False),)

    async def verified_dataset(_database):
        return _dataset_row()

    async def no_pointer(_database):
        return {}

    async def counts(_database):
        return publisher_module.CANARY_FINAL_TABLE_COUNTS

    monkeypatch.setattr(publisher_module, "_source_rows", source_rows)
    monkeypatch.setattr(
        publisher_module,
        "_locked_dataset_row",
        verified_dataset,
    )
    monkeypatch.setattr(publisher_module, "_locked_pointer", no_pointer)
    monkeypatch.setattr(publisher_module, "_table_counts", counts)

    with pytest.raises(SyntheticSeedPublicationError, match="publication"):
        await publisher_module._postflight(
            object(),
            PublicationResult(
                CANARY_SOURCE_ID,
                expected_evidence()["dataset_id"],
                1,
                PUBLISHED_AT,
            ),
        )


@pytest.mark.asyncio
async def test_publish_transaction_uses_one_outer_transaction(monkeypatch):
    database = _Database()
    events: list[str] = []
    repository_publication = PublicationResult(
        CANARY_SOURCE_ID,
        expected_evidence()["dataset_id"],
        1,
        PUBLISHED_AT,
    )

    async def preflight(_database):
        events.append("preflight")
        return publisher_module._candidate_dataset(_dataset_row())

    class _Repository:
        def __init__(self, *, source_id, database):
            assert source_id == CANARY_SOURCE_ID
            assert database is not None

        async def publish_verified_seed(self, *, dataset):
            assert dataset.status == "verified"
            events.append("publish")
            return repository_publication

    async def postflight(_database, publication):
        assert publication is repository_publication
        events.append("postflight")
        return publisher_module._publication_result(publication)

    monkeypatch.setattr(publisher_module, "_preflight", preflight)
    monkeypatch.setattr(publisher_module, "FHIRFormularyRepository", _Repository)
    monkeypatch.setattr(publisher_module, "_postflight", postflight)

    publication = await publisher_module._publish_transaction(database)

    assert publication.generation == 1
    assert events == ["preflight", "publish", "postflight"]
    assert [call[1] for call in database.calls] == ["begin", "commit"]


@pytest.mark.asyncio
async def test_postflight_failure_rolls_back_outer_transaction(monkeypatch):
    database = _Database()

    async def preflight(_database):
        return publisher_module._candidate_dataset(_dataset_row())

    class _Repository:
        def __init__(self, **_kwargs):
            assert _kwargs["source_id"] == CANARY_SOURCE_ID
            assert _kwargs["database"] is database

        async def publish_verified_seed(self, *, dataset):
            assert dataset.status == "verified"
            return PublicationResult(
                CANARY_SOURCE_ID,
                expected_evidence()["dataset_id"],
                1,
                dt.datetime.now(dt.UTC),
            )

    async def fail_postflight(*_args):
        raise SyntheticSeedPublicationError("publication")

    monkeypatch.setattr(publisher_module, "_preflight", preflight)
    monkeypatch.setattr(publisher_module, "FHIRFormularyRepository", _Repository)
    monkeypatch.setattr(publisher_module, "_postflight", fail_postflight)

    with pytest.raises(SyntheticSeedPublicationError, match="publication"):
        await publisher_module._publish_transaction(database)
    assert [call[1] for call in database.calls] == ["begin", "rollback"]
