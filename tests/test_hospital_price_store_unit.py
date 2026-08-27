# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Focused failure and orchestration proof for hospital-price storage."""

from __future__ import annotations

from contextlib import asynccontextmanager
import hashlib
import importlib.util
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from tests.hospital_price_control_support import ROOT, store_module as _store_module


class _Driver:
    def __init__(self) -> None:
        self.records: list[tuple[Any, ...]] = []
        self.payloads: list[bytes] = []

    async def copy_records_to_table(self, _table: str, **kwargs: Any) -> None:
        self.records = list(kwargs["records"])

    async def copy_to_table(self, _table: str, **kwargs: Any) -> None:
        self.payloads.append(kwargs["source"].read())


class _Connection:
    def __init__(
        self,
        *,
        statuses: list[int] | None = None,
        scalars: list[Any] | None = None,
        firsts: list[Any] | None = None,
        all_rows: list[Any] | None = None,
        driver: Any | None = None,
    ) -> None:
        self.driver = driver or _Driver()
        self.raw_connection = SimpleNamespace(driver_connection=self.driver)
        self.statuses = list(statuses or ())
        self.scalars = list(scalars or ())
        self.firsts = list(firsts or ())
        self.all_rows = list(all_rows or ())
        self.statements: list[str] = []

    async def status(self, statement: str, **_kwargs: Any) -> int:
        self.statements.append(statement)
        return self.statuses.pop(0) if self.statuses else 1

    async def scalar(self, statement: str, **_kwargs: Any) -> Any:
        self.statements.append(statement)
        return self.scalars.pop(0) if self.scalars else False

    async def first(self, statement: str, **_kwargs: Any) -> Any:
        self.statements.append(statement)
        return self.firsts.pop(0)

    async def all(self, statement: str, **_kwargs: Any) -> list[Any]:
        self.statements.append(statement)
        return self.all_rows.pop(0) if self.all_rows else []


def _receipt(native: Any, directory: Path, *, row_count: int = 1) -> Any:
    directory.mkdir(parents=True, exist_ok=True)
    artifacts = []
    for kind in native.HOSPITAL_MRF_COPY_COLUMNS:
        artifact_bytes = f"{kind}\n".encode()
        path = directory / f"{kind}.copy"
        path.write_bytes(artifact_bytes)
        artifacts.append(SimpleNamespace(
            kind=kind,
            path=path,
            rows=row_count,
            bytes=len(artifact_bytes),
            sha256=hashlib.sha256(artifact_bytes).hexdigest(),
        ))
    return SimpleNamespace(
        version_id="a" * 64,
        source_format="json",
        semantic_sha256="b" * 64,
        artifacts=tuple(artifacts),
        root=SimpleNamespace(
            service_count=1,
            charge_count=1,
            fact_count=1,
            code_selector_key_count=1,
            payer_plan_selector_key_count=1,
            code_selector_ref_count=1,
            payer_plan_selector_ref_count=1,
            service_block_count=row_count, fact_block_count=row_count,
            code_selector_page_count=row_count,
            payer_plan_selector_page_count=row_count,
            code_selector_block_count=row_count,
            payer_plan_selector_block_count=row_count,
        ),
    )


def _acquire(connection: Any):
    @asynccontextmanager
    async def acquire():
        yield connection

    return acquire


@pytest.mark.asyncio
async def test_attempt_short_circuits_and_successful_lease_renewal() -> None:
    store, _native = _store_module()
    with pytest.raises(ValueError, match="lease is invalid"):
        await store.admit_attempts((), lease_owner=" ", lease_seconds=1)
    assert await store.fail_attempts((), "failed", None) == 0
    assert await store.renew_attempt_leases(
        (), lease_owner="worker", lease_seconds=30
    ) == 0

    async def renewed(_statement: str, **kwargs: Any) -> tuple[int, int, int]:
        assert kwargs["attempt_ids"] == ("attempt-a",)
        return 1, 0, 0

    store.db.first = renewed
    attempts = (
        SimpleNamespace(attempt_id="attempt-a"),
        SimpleNamespace(attempt_id="attempt-a"),
    )
    assert await store.renew_attempt_leases(
        attempts, lease_owner="worker", lease_seconds=30
    ) == 1


@pytest.mark.asyncio
async def test_fail_attempts_copies_bounded_final_evidence() -> None:
    store, _native = _store_module()
    connection = _Connection(statuses=[1, 2])
    store.db.acquire = _acquire(connection)
    attempt = SimpleNamespace(
        attempt_id="attempt-a",
        final_source_url="https://hospital.example/prices.json",
        source_http_status=503,
    )

    failed = await store.fail_attempts((attempt,), "x" * 80, None)

    assert failed == 2
    assert connection.driver.records == [(
        "attempt-a", "https://hospital.example/prices.json", 503,
    )]
    assert "ON COMMIT DROP" in connection.statements[0]


@pytest.mark.asyncio
async def test_refreshed_source_rebind_is_atomic_and_provenance_bound() -> None:
    """Persist the exact fresh locator observation before retrying its source."""

    store, _native = _store_module()
    await store.rebind_attempt_sources(())
    connection = _Connection(all_rows=[[('attempt-a',)]])
    store.db.acquire = _acquire(connection)
    attempt = SimpleNamespace(attempt_id="attempt-a", hospital_id="hospital-a")
    candidate = SimpleNamespace(
        hospital_id="hospital-a",
        locator_id="locator-a",
        observation_id="observation-fresh",
        source_url="https://hospital.example/prices.json?sig=fresh",
    )
    with pytest.raises(ValueError, match="binding is invalid"):
        await store.rebind_attempt_sources(((
            attempt, SimpleNamespace(**{**vars(candidate), "hospital_id": "other"})
        ),))

    await store.rebind_attempt_sources(((attempt, candidate),))

    assert connection.driver.records == [(
        "attempt-a", "hospital-a", "locator-a", "observation-fresh",
        candidate.source_url,
    )]
    update_sql = connection.statements[-1]
    assert "locator_observation_id=staged.observation_id" in update_sql
    assert "requested_source_url=staged.source_url" in update_sql
    assert "attempt.status='running'" in update_sql

    connection.all_rows = [[]]
    with pytest.raises(RuntimeError, match="changed before source retry"):
        await store.rebind_attempt_sources(((attempt, candidate),))


@pytest.mark.asyncio
async def test_copy_and_stage_validation_reject_drift(tmp_path: Path) -> None:
    store, _native = _store_module()
    receipt = _receipt(_native, tmp_path)
    stage_by_kind = {
        artifact.kind: f"stage_{artifact.kind}" for artifact in receipt.artifacts
    }
    no_copy = _Connection(driver=SimpleNamespace())
    with pytest.raises(NotImplementedError, match="text COPY"):
        await store.copy_stages(no_copy, receipt, stage_by_kind, "mrf")

    drifted = _receipt(_native, tmp_path / "drift")
    drifted.artifacts[0].sha256 = "0" * 64
    with pytest.raises(RuntimeError, match="COPY changed"):
        await store.copy_stages(_Connection(), drifted, stage_by_kind, "mrf")

    good = _Connection(
        firsts=[(artifact.rows, 0) for artifact in receipt.artifacts],
        scalars=[False] * 4,
    )
    await store.validate_stages(good, receipt, stage_by_kind)
    assert sum("CREATE UNIQUE INDEX" in sql for sql in good.statements) == len(
        _native.HOSPITAL_MRF_TEXT_COPY_COLUMNS
    )

    bad_count = _Connection(firsts=[(0, 0)])
    with pytest.raises(RuntimeError, match="staging count"):
        await store.validate_stages(bad_count, receipt, stage_by_kind)

    two_headers = _receipt(_native, tmp_path / "headers", row_count=2)
    with pytest.raises(RuntimeError, match="one MRF header"):
        await store.validate_stages(
            _Connection(firsts=[(2, 0)]), two_headers, stage_by_kind
        )

    unresolved = _Connection(
        firsts=[(artifact.rows, 0) for artifact in receipt.artifacts],
        scalars=[True],
    )
    with pytest.raises(RuntimeError, match="unresolved reference"):
        await store.validate_stages(unresolved, receipt, stage_by_kind)


@pytest.mark.asyncio
async def test_content_version_children_and_count_invariants(
    tmp_path: Path, monkeypatch
) -> None:
    store, _native = _store_module()
    receipt = _receipt(_native, tmp_path)
    receipt.source_format = "csv-tall"
    stage_by_kind = {
        artifact.kind: f"stage_{artifact.kind}" for artifact in receipt.artifacts
    }

    await store._insert_content(_Connection(scalars=[7]), "c" * 64, 7, None)
    with pytest.raises(RuntimeError, match="conflicting byte count"):
        await store._insert_content(_Connection(scalars=[6]), "c" * 64, 7, None)

    count_by_kind = {artifact.kind: artifact.rows for artifact in receipt.artifacts}
    expected_version = (
        "c" * 64,
        store.HOSPITAL_MRF_PARSER_CONTRACT_SHA256,
        receipt.semantic_sha256,
        receipt.source_format,
        count_by_kind["location"],
        count_by_kind["npi"],
        count_by_kind["license"],
        receipt.root.service_count,
        receipt.root.charge_count,
        receipt.root.fact_count,
    )
    await store._has_inserted_version(
        _Connection(firsts=[expected_version]), receipt, stage_by_kind, "c" * 64
    )
    with pytest.raises(RuntimeError, match="stored projection"):
        await store._has_inserted_version(
            _Connection(firsts=[None]), receipt, stage_by_kind, "c" * 64
        )

    children = _Connection()
    await store._insert_children(children, stage_by_kind)
    assert len(children.statements) == len(store._CHILDREN)

    async def packed_valid(*_args: Any) -> None:
        return None

    monkeypatch.setattr(store, "validate_packed_storage", packed_valid)
    stored_counts = [
        (artifact.kind, artifact.rows)
        for artifact in receipt.artifacts
        if artifact.kind in _native.HOSPITAL_MRF_TEXT_COPY_COLUMNS
    ]
    await store._validate_stored_counts(
        _Connection(all_rows=[stored_counts]), receipt
    )
    with pytest.raises(RuntimeError, match="count is invalid"):
        await store._validate_stored_counts(
            _Connection(all_rows=[[(*stored_counts[0][:1], 0), *stored_counts[1:]]]),
            receipt,
        )


def test_location_binding_is_exact_and_rejects_ambiguity() -> None:
    store, _native = _store_module()
    attempts = (
        SimpleNamespace(
            hospital_id="hospital-a", locator_name=" Same ", hospital_name="A"
        ),
        SimpleNamespace(
            hospital_id="hospital-b", locator_name=None, hospital_name="Unique"
        ),
    )
    assert store._location_ordinals(
        attempts, ((0, None), (1, "Same"), (2, " same "), (3, "Unique"))
    ) == {"hospital-a": None, "hospital-b": 3}


@pytest.mark.asyncio
async def test_publication_stage_keeps_location_and_filename_evidence() -> None:
    store, _native = _store_module()
    connection = _Connection()
    attempt = SimpleNamespace(
        hospital_id="hospital-a",
        attempt_id="attempt-a",
        expected_generation=3,
        locator_name="Hospital A",
        hospital_name="Catalog A",
        final_source_url="https://cdn.hospital.example/prices.json",
        source_http_status=200,
        source_url="https://hospital.example/12-3456789_prices.json",
    )

    stage_name, stage = await store._publication_stage(
        connection, (attempt,), ((7, "Hospital A"),)
    )

    assert stage == f'"{stage_name}"'
    assert connection.driver.records == [(
        "hospital-a",
        "attempt-a",
        3,
        7,
        "https://cdn.hospital.example/prices.json",
        200,
        "123456789",
    )]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("statuses", "scalars", "message"),
    (
        ([1], [True], "binding conflicts"),
        ([1, 0], [False], "attempt changed"),
        ([1, 1, 1], [False, True], "NPI provenance"),
    ),
)
async def test_evidence_binding_fails_closed(
    statuses: list[int], scalars: list[bool], message: str
) -> None:
    store, _native = _store_module()
    with pytest.raises(RuntimeError, match=message):
        await store._bind_evidence(
            _Connection(statuses=statuses, scalars=scalars),
            '"stage"',
            "a" * 64,
            "b" * 64,
            1,
        )


@pytest.mark.asyncio
async def test_bind_and_publish_checks_cardinality(monkeypatch) -> None:
    store, _native = _store_module()
    assert await store._bind_and_publish(object(), "v", "c", (), ()) == (0, 0, 0)

    async def publication(*_args: Any) -> tuple[str, str]:
        return "stage", '"stage"'

    async def bind(*_args: Any) -> None:
        return None

    outcomes = [(0, 0, 0), (1, 0, 0)]

    async def publish(*_args: Any) -> tuple[int, int, int]:
        return outcomes.pop(0)

    monkeypatch.setattr(store, "_publication_stage", publication)
    monkeypatch.setattr(store, "_bind_evidence", bind)
    monkeypatch.setattr(store, "_cas_publish", publish)
    attempts = (SimpleNamespace(),)
    with pytest.raises(RuntimeError, match="result count"):
        await store._bind_and_publish(object(), "v", "c", attempts, ())
    assert await store._bind_and_publish(
        object(), "v", "c", attempts, ()
    ) == (1, 0, 0)


@pytest.mark.asyncio
async def test_existing_version_and_publication_paths(monkeypatch) -> None:
    store, _native = _store_module()
    expected = (
        "c" * 64,
        store.HOSPITAL_MRF_PARSER_CONTRACT_SHA256,
        7,
    )
    stored_versions = [
        None,
        ("wrong", "contract", 1),
        (*expected, False, True),
        (*expected, True, True),
    ]

    async def first(*_args: Any, **_kwargs: Any) -> Any:
        return stored_versions.pop(0)

    store.db.first = first
    assert not await store.has_existing_version("v", "c" * 64, 7)
    with pytest.raises(RuntimeError, match="conflicts with source"):
        await store.has_existing_version("v", "c" * 64, 7)
    with pytest.raises(RuntimeError, match="packed version is incomplete"):
        await store.has_existing_version("v", "c" * 64, 7)
    assert await store.has_existing_version("v", "c" * 64, 7)

    connection = _Connection(all_rows=[[((4, "Hospital A"))]])
    store.db.acquire = _acquire(connection)
    seen_arguments: list[Any] = []

    async def bind(*args: Any) -> tuple[int, int, int]:
        seen_arguments.extend(args)
        return 1, 0, 0

    monkeypatch.setattr(store, "_bind_and_publish", bind)
    attempts = (SimpleNamespace(),)
    assert await store.publish_existing("v", "c", attempts) == (1, 0, 0)
    assert seen_arguments[-1] == [(4, "Hospital A")]


@pytest.mark.asyncio
async def test_stage_content_runs_one_transactional_pipeline(monkeypatch) -> None:
    store, _native = _store_module()
    connection = _Connection()
    store.db.acquire = _acquire(connection)
    calls: list[str] = []

    def record(name: str):
        async def call(*_args: Any) -> None:
            calls.append(name)

        return call

    for name in (
        "_copy_stages",
        "_validate_stages",
        "_insert_content",
        "_insert_children",
        "_insert_packed_root",
        "copy_packed_blocks",
        "_validate_stored_counts",
    ):
        monkeypatch.setattr(store, name, record(name))

    inserted_outcomes = [True, False]

    async def has_inserted_version(*_args: Any) -> bool:
        calls.append("_has_inserted_version")
        return inserted_outcomes.pop(0)

    monkeypatch.setattr(store, "_has_inserted_version", has_inserted_version)
    raw = SimpleNamespace(
        raw_sha256="c" * 64,
        byte_count=7,
        head=SimpleNamespace(content_type="application/json"),
    )

    await store.stage_content(SimpleNamespace(), raw)

    assert calls == [
        "_copy_stages",
        "_validate_stages",
        "_insert_content",
        "_has_inserted_version",
        "_insert_children",
        "_insert_packed_root",
        "copy_packed_blocks",
        "_validate_stored_counts",
    ]

    calls.clear()
    await store.stage_content(SimpleNamespace(), raw)
    assert calls == [
        "_copy_stages",
        "_validate_stages",
        "_insert_content",
        "_has_inserted_version",
        "_validate_stored_counts",
    ]


def test_model_schema_rejects_conflicting_environment(monkeypatch) -> None:
    pytest.importorskip("sqlalchemy")
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "hospital_a")
    monkeypatch.setenv("DB_SCHEMA", "hospital_b")
    spec = importlib.util.spec_from_file_location(
        "hospital_price_conflicting_schema_test",
        ROOT / "db/models/hospital_price.py",
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    with pytest.raises(RuntimeError, match="must match"):
        spec.loader.exec_module(module)
