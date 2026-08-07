# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""SQL and defensive state branches for the fixed synthetic canary."""

from __future__ import annotations

from contextlib import asynccontextmanager
import pytest

import process.formulary_fhir.synthetic_canary as canary_module
from process.formulary_fhir.synthetic_canary import SyntheticCanaryError
from process.formulary_fhir.synthetic_canary_contract import CANARY_CUTOFF
from process.formulary_fhir.synthetic_canary_contract import (
    CANARY_FINAL_TABLE_COUNTS,
)
from process.formulary_fhir.synthetic_canary_contract import CANARY_RUN_ID
from process.formulary_fhir.synthetic_canary_contract import CANARY_SOURCE_ID
from process.formulary_fhir.synthetic_canary_contract import expected_evidence
from tests.test_formulary_fhir_synthetic_canary import _source_row


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
        return self._next(self.status_results, "status")

    @asynccontextmanager
    async def transaction(self):
        self.calls.append(("transaction", "begin", {}))
        yield
        self.calls.append(("transaction", "commit", {}))


def _dataset_row(**changes: object) -> dict[str, object]:
    expected_by_field = expected_evidence()
    dataset_by_field: dict[str, object] = {
        "source_id": CANARY_SOURCE_ID,
        "dataset_id": expected_by_field["dataset_id"],
        "run_id": CANARY_RUN_ID,
        "previous_dataset_id": None,
        "cutoff_at": CANARY_CUTOFF,
        "acquisition_contract_hash": expected_by_field[
            "acquisition_contract_hash"
        ],
        "status": "building",
        "publish_requested": False,
        "seed_eligible": True,
    }
    dataset_by_field.update(changes)
    return dataset_by_field


def _count_results(
    changes: dict[str, int] | None = None,
) -> list[int]:
    counts_by_table = {table: 0 for table in CANARY_FINAL_TABLE_COUNTS}
    counts_by_table.update(changes or {})
    return [counts_by_table[table] for table in CANARY_FINAL_TABLE_COUNTS]


@pytest.mark.parametrize(
    "changed_field,changed_value",
    [
        ("source_id", "source-beta"),
        ("dataset_id", "ffd_" + ("0" * 48)),
        ("run_id", "different-run"),
        ("previous_dataset_id", "ffd_" + ("1" * 48)),
        ("cutoff_at", CANARY_CUTOFF.replace(day=5)),
        ("acquisition_contract_hash", "0" * 64),
        ("status", "failed"),
        ("publish_requested", True),
        ("seed_eligible", False),
    ],
)
def test_recoverable_dataset_requires_every_exact_field(
    changed_field,
    changed_value,
):
    assert canary_module._is_recoverable_dataset(_dataset_row())
    assert not canary_module._is_recoverable_dataset(
        _dataset_row(**{changed_field: changed_value})
    )


@pytest.mark.asyncio
async def test_pointer_and_catalog_queries_fail_closed():
    pointer_database = _Database(
        first_results=[{"source_id": CANARY_SOURCE_ID, "dataset_id": "candidate"}]
    )
    with pytest.raises(SyntheticCanaryError, match="catalog"):
        await canary_module._require_empty_pointer(pointer_database)

    too_many_database = _Database(
        first_results=[None],
        all_results=[[_dataset_row(), _dataset_row(status="verified")]],
        scalar_results=_count_results(),
    )
    with pytest.raises(SyntheticCanaryError, match="catalog"):
        await canary_module._require_recoverable_catalog(too_many_database)

    malformed_database = _Database(
        first_results=[None],
        all_results=[[_dataset_row(status="failed")]],
        scalar_results=_count_results(),
    )
    with pytest.raises(SyntheticCanaryError, match="catalog"):
        await canary_module._require_recoverable_catalog(malformed_database)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "dataset_rows,count_changes",
    [
        ([], {"fhir_formulary_drug_plan_alias": 1}),
        ([_dataset_row()], {"fhir_formulary_checkpoint": 3}),
    ],
)
async def test_recoverable_catalog_rejects_orphan_or_over_limit_graph(
    dataset_rows,
    count_changes,
):
    database = _Database(
        first_results=[None],
        all_results=[dataset_rows],
        scalar_results=_count_results(count_changes),
    )

    with pytest.raises(SyntheticCanaryError, match="catalog"):
        await canary_module._require_recoverable_catalog(database)


@pytest.mark.asyncio
async def test_exact_verified_graph_rejects_source_dataset_and_count_drift():
    source_database = _Database(
        first_results=[None],
        all_results=[[], [_dataset_row(status="verified")]],
    )
    with pytest.raises(SyntheticCanaryError, match="catalog"):
        await canary_module._require_exact_verified_graph(source_database)

    dataset_database = _Database(
        first_results=[None],
        all_results=[[_source_row()], [_dataset_row(status="building")]],
    )
    with pytest.raises(SyntheticCanaryError, match="evidence"):
        await canary_module._require_exact_verified_graph(dataset_database)

    count_database = _Database(
        first_results=[None],
        all_results=[[_source_row()], [_dataset_row(status="verified")]],
        scalar_results=_count_results({"fhir_formulary_checkpoint": 1}),
    )
    with pytest.raises(SyntheticCanaryError, match="evidence"):
        await canary_module._require_exact_verified_graph(count_database)


@pytest.mark.asyncio
async def test_source_write_rowcounts_and_bindings_are_exact():
    insert_database = _Database(status_results=[0])
    with pytest.raises(SyntheticCanaryError, match="source"):
        await canary_module._insert_exact_source(insert_database, "source_table")
    insert_call = insert_database.calls[0]
    assert CANARY_SOURCE_ID not in insert_call[1]
    assert insert_call[2]["source_id"] == CANARY_SOURCE_ID

    enabled_database = _Database()
    await canary_module._enable_existing_source(
        enabled_database,
        "source_table",
        _source_row(),
    )
    assert enabled_database.calls == []

    with pytest.raises(SyntheticCanaryError, match="catalog"):
        await canary_module._enable_existing_source(
            _Database(),
            "source_table",
            _source_row() | {"canonical_base": "https://collision.invalid/fhir"},
        )

    update_database = _Database(status_results=[0])
    with pytest.raises(SyntheticCanaryError, match="source"):
        await canary_module._enable_existing_source(
            update_database,
            "source_table",
            _source_row(enabled=False),
        )


@pytest.mark.asyncio
async def test_enable_rejects_multi_source_and_post_enable_drift():
    multi_source_database = _Database(
        all_results=[[_source_row(), _source_row(enabled=False)]],
        status_results=[1],
    )
    with pytest.raises(SyntheticCanaryError, match="catalog"):
        await canary_module._enable_exact_source(multi_source_database)

    post_enable_database = _Database(
        all_results=[[], [], []],
        first_results=[None],
        scalar_results=_count_results(),
        status_results=[1, 1],
    )
    with pytest.raises(SyntheticCanaryError, match="source"):
        await canary_module._enable_exact_source(post_enable_database)


@pytest.mark.asyncio
async def test_disable_policy_handles_absence_collision_and_update_failure():
    absent_database = _Database(first_results=[None], status_results=[1])
    await canary_module._disable_exact_source(
        absent_database,
        require_verified_graph=False,
        is_reserved_source_claimed=False,
    )
    with pytest.raises(SyntheticCanaryError, match="source"):
        await canary_module._disable_exact_source(
            _Database(first_results=[None], status_results=[1]),
            require_verified_graph=False,
            is_reserved_source_claimed=True,
        )

    collision_row = _source_row() | {
        "canonical_base": "https://collision.invalid/fhir"
    }
    collision_database = _Database(
        first_results=[collision_row],
        status_results=[1],
    )
    await canary_module._disable_exact_source(
        collision_database,
        require_verified_graph=False,
        is_reserved_source_claimed=False,
    )
    assert not any(
        call[0] == "status" and "UPDATE" in call[1]
        for call in collision_database.calls
    )

    update_database = _Database(
        first_results=[_source_row()],
        status_results=[1, 0],
    )
    with pytest.raises(SyntheticCanaryError, match="cleanup"):
        await canary_module._disable_exact_source(
            update_database,
            require_verified_graph=False,
            is_reserved_source_claimed=True,
        )


@pytest.mark.asyncio
async def test_final_disable_commits_before_reporting_graph_drift():
    database = _Database(
        first_results=[
            {"source_id": CANARY_SOURCE_ID, "dataset_id": "unexpected"},
            _source_row(),
        ],
        status_results=[1, 1],
    )

    with pytest.raises(SyntheticCanaryError, match="catalog"):
        await canary_module._disable_exact_source(
            database,
            require_verified_graph=True,
            is_reserved_source_claimed=True,
        )

    operations = [call[0] for call in database.calls]
    assert operations[-1] == "transaction"
    assert any(call[0] == "status" and "UPDATE" in call[1] for call in database.calls)


@pytest.mark.asyncio
async def test_disable_claimed_source_handles_config_drift_and_disabled_state():
    collision_row = _source_row() | {
        "canonical_base": "https://collision.invalid/fhir"
    }
    drift_database = _Database(
        first_results=[None, collision_row],
        all_results=[[_source_row()], [_dataset_row(status="verified")]],
        scalar_results=_count_results(CANARY_FINAL_TABLE_COUNTS),
        status_results=[1, 1],
    )
    with pytest.raises(SyntheticCanaryError, match="source"):
        await canary_module._disable_exact_source(
            drift_database,
            require_verified_graph=True,
            is_reserved_source_claimed=True,
        )
    assert any(
        call[0] == "status" and "UPDATE" in call[1]
        for call in drift_database.calls
    )

    disabled_database = _Database(
        first_results=[_source_row(enabled=False)],
        status_results=[1],
    )
    await canary_module._disable_exact_source(
        disabled_database,
        require_verified_graph=False,
        is_reserved_source_claimed=True,
    )
    assert not any(
        call[0] == "status" and "UPDATE" in call[1]
        for call in disabled_database.calls
    )
