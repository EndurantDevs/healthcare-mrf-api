from __future__ import annotations

import importlib
from datetime import UTC, datetime, timedelta

import pytest

from db.models import ProviderProfileProjection

florida = importlib.import_module("process.florida_mqa_profile")


class _Row:
    def __init__(self, **mapping):
        self._mapping = mapping


class _Transaction:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        return False


class _Statement:
    def __init__(self, writes):
        self.writes = writes

    def where(self, *_criteria):
        return self

    def values(self, *rows, **values):
        self.writes.append((rows, values))
        return self

    async def status(self):
        return 1


class _PublicationDb:
    def __init__(self, *, scalar_results, all_results, current_run=None):
        self.scalar_results = iter(scalar_results)
        self.all_results = iter(all_results)
        self.current_run = current_run
        self.status_calls = []
        self.writes = []

    async def status(self, statement):
        self.status_calls.append(str(statement))

    async def scalar(self, *_args, **_kwargs):
        return next(self.scalar_results)

    async def all(self, *_args, **_kwargs):
        return next(self.all_results)

    async def first(self, *_args, **_kwargs):
        return self.current_run

    def transaction(self):
        return _Transaction()

    def insert(self, _table):
        return _Statement(self.writes)

    def update(self, _table):
        return _Statement(self.writes)


def _source_metrics(rows: int) -> dict:
    return {
        "profile_master": {
            "schema_complete": True,
            "rows": rows,
            "matched": rows,
            "facts": rows,
            "quarantined_rows": 0,
            "max_quarantined_rows": 100,
            "max_quarantined_ratio": 0.001,
            "header_sha256": "a" * 64,
        }
    }


def _completion_metrics(rows: int) -> dict:
    return {
        "source_records": rows,
        "selected_sources": ["profile_master"],
        "source_metrics": _source_metrics(rows),
    }


async def _one_projection_row(run_id: str):
    yield [
        {
            "npi": 1000000004,
            "generation_id": run_id,
            "schema_version": florida.PROFILE_SCHEMA_VERSION,
            "profile_json": {"categories": {}},
            "evidence_json": {"records": []},
            "source_keys": [florida.FL_MQA_SOURCE_KEY],
            "published_at": datetime(2026, 7, 27, tzinfo=UTC),
        }
    ]


@pytest.mark.asyncio
async def test_publication_rejects_empty_and_non_unique_stage(monkeypatch):
    empty_database = _PublicationDb(scalar_results=[], all_results=[])
    monkeypatch.setattr(florida, "db", empty_database)

    async def empty_batches():
        yield []

    with pytest.raises(RuntimeError, match="stage_empty"):
        await florida._publish_projection_swap(
            "a" * 32,
            empty_batches(),
            started_at=datetime(2026, 7, 27, tzinfo=UTC),
            completion_metrics=_completion_metrics(1),
            allow_volume_drop=False,
            min_first_publish_providers=1,
            min_publish_ratio=0.8,
        )

    invalid_database = _PublicationDb(
        scalar_results=[2, 1],
        all_results=[],
    )
    monkeypatch.setattr(florida, "db", invalid_database)
    with pytest.raises(RuntimeError, match="stage_validation_failed"):
        await florida._publish_projection_swap(
            "b" * 32,
            _one_projection_row("b" * 32),
            started_at=datetime(2026, 7, 27, tzinfo=UTC),
            completion_metrics=_completion_metrics(1),
            allow_volume_drop=False,
            min_first_publish_providers=1,
            min_publish_ratio=0.8,
        )


@pytest.mark.asyncio
async def test_publication_rejects_mixed_or_newer_live_generations(monkeypatch):
    mixed_database = _PublicationDb(
        scalar_results=[1, 1, 1],
        all_results=[
            [
                _Row(generation_id="1" * 32, provider_count=1),
                _Row(generation_id="2" * 32, provider_count=1),
            ]
        ],
    )
    monkeypatch.setattr(florida, "db", mixed_database)
    with pytest.raises(RuntimeError, match="live_generation_mixed"):
        await florida._publish_projection_swap(
            "3" * 32,
            _one_projection_row("3" * 32),
            started_at=datetime(2026, 7, 27, tzinfo=UTC),
            completion_metrics=_completion_metrics(1),
            allow_volume_drop=False,
            min_first_publish_providers=1,
            min_publish_ratio=0.8,
        )

    candidate_started_at = datetime(2026, 7, 27, tzinfo=UTC)
    newer_database = _PublicationDb(
        scalar_results=[1, 1, 1],
        all_results=[
            [_Row(generation_id="4" * 32, provider_count=10)],
        ],
        current_run=_Row(
            started_at=candidate_started_at + timedelta(minutes=1),
            metrics={
                "source_records": 10,
                "source_metrics": _source_metrics(10),
            },
        ),
    )
    monkeypatch.setattr(florida, "db", newer_database)
    with pytest.raises(RuntimeError, match="newer_generation_already_published"):
        await florida._publish_projection_swap(
            "5" * 32,
            _one_projection_row("5" * 32),
            started_at=candidate_started_at,
            completion_metrics=_completion_metrics(1),
            allow_volume_drop=False,
            min_first_publish_providers=1,
            min_publish_ratio=0.8,
        )


@pytest.mark.asyncio
async def test_publication_volume_override_is_auditable_and_rotates_tables(
    monkeypatch,
):
    run_id = "6" * 32
    live_name = ProviderProfileProjection.__tablename__
    started_at = datetime(2026, 7, 27, tzinfo=UTC)
    database = _PublicationDb(
        scalar_results=[1, 1, 1],
        all_results=[
            [_Row(generation_id="7" * 32, provider_count=100)],
            [
                _Row(tablename=live_name),
                _Row(tablename=f"{live_name}_old"),
                _Row(tablename=f"{live_name}_{run_id[:16]}"),
                _Row(tablename="unrelated_projection_table"),
            ],
        ],
        current_run=_Row(
            started_at=started_at - timedelta(days=1),
            metrics={
                "source_records": 100,
                "source_metrics": _source_metrics(100),
            },
        ),
    )
    monkeypatch.setattr(florida, "db", database)

    publication, metrics = await florida._publish_projection_swap(
        run_id,
        _one_projection_row(run_id),
        started_at=started_at,
        completion_metrics=_completion_metrics(1),
        allow_volume_drop=True,
        min_first_publish_providers=100,
        min_publish_ratio=0.8,
    )

    assert publication["source_guard"]["ratio_reasons"]
    assert publication["volume_guard"]["reasons"]
    assert publication["volume_guard"]["allow_volume_drop"] is True
    assert metrics["published_providers"] == 1
    dropped_defaults = [
        statement
        for statement in database.status_calls
        if "ALTER COLUMN npi DROP DEFAULT" in statement
    ]
    assert len(dropped_defaults) == 3
    assert all("unrelated_projection_table" not in profile_item for profile_item in dropped_defaults)
    assert any("RENAME TO provider_profile_projection_old" in profile_item for profile_item in database.status_calls)


@pytest.mark.asyncio
async def test_publication_rejects_incomplete_source_metrics(monkeypatch):
    database = _PublicationDb(
        scalar_results=[1, 1, 1],
        all_results=[[]],
    )
    monkeypatch.setattr(florida, "db", database)

    with pytest.raises(RuntimeError, match="source_validation_guard"):
        await florida._publish_projection_swap(
            "8" * 32,
            _one_projection_row("8" * 32),
            started_at=datetime(2026, 7, 27, tzinfo=UTC),
            completion_metrics={
                "source_records": 1,
                "selected_sources": ["profile_master"],
                "source_metrics": {},
            },
            allow_volume_drop=False,
            min_first_publish_providers=1,
            min_publish_ratio=0.8,
        )


@pytest.mark.asyncio
async def test_publication_rejects_unsafe_stage_identifier(monkeypatch):
    monkeypatch.setattr(
        florida.ProviderProfileProjection,
        "__tablename__",
        "unsafe-stage",
    )

    with pytest.raises(RuntimeError, match="stage_name_invalid"):
        await florida._publish_projection_swap(
            "9" * 32,
            _one_projection_row("9" * 32),
            started_at=datetime(2026, 7, 27, tzinfo=UTC),
            completion_metrics=_completion_metrics(1),
            allow_volume_drop=False,
            min_first_publish_providers=1,
            min_publish_ratio=0.8,
        )


@pytest.mark.asyncio
async def test_publication_distinguishes_source_and_provider_volume_guards(
    monkeypatch,
):
    started_at = datetime(2026, 7, 27, tzinfo=UTC)
    current_run = _Row(
        started_at=started_at - timedelta(days=1),
        metrics={
            "source_records": 100,
            "source_metrics": _source_metrics(100),
        },
    )

    source_drop_database = _PublicationDb(
        scalar_results=[1, 1, 1],
        all_results=[
            [_Row(generation_id="a" * 32, provider_count=100)],
        ],
        current_run=current_run,
    )
    monkeypatch.setattr(florida, "db", source_drop_database)
    with pytest.raises(RuntimeError, match="source_volume_guard"):
        await florida._publish_projection_swap(
            "b" * 32,
            _one_projection_row("b" * 32),
            started_at=started_at,
            completion_metrics=_completion_metrics(1),
            allow_volume_drop=False,
            min_first_publish_providers=1,
            min_publish_ratio=0.8,
        )

    provider_drop_database = _PublicationDb(
        scalar_results=[1, 1, 1],
        all_results=[
            [_Row(generation_id="c" * 32, provider_count=100)],
        ],
        current_run=current_run,
    )
    monkeypatch.setattr(florida, "db", provider_drop_database)
    with pytest.raises(RuntimeError, match="publication_volume_guard"):
        await florida._publish_projection_swap(
            "d" * 32,
            _one_projection_row("d" * 32),
            started_at=started_at,
            completion_metrics=_completion_metrics(100),
            allow_volume_drop=False,
            min_first_publish_providers=1,
            min_publish_ratio=0.8,
        )


@pytest.mark.asyncio
async def test_publication_normalizes_non_mapping_source_metrics(monkeypatch):
    database = _PublicationDb(
        scalar_results=[1, 1, 1],
        all_results=[[]],
    )
    monkeypatch.setattr(florida, "db", database)

    with pytest.raises(RuntimeError, match="source_validation_guard"):
        await florida._publish_projection_swap(
            "e" * 32,
            _one_projection_row("e" * 32),
            started_at=datetime(2026, 7, 27, tzinfo=UTC),
            completion_metrics={
                "source_records": 1,
                "selected_sources": ["profile_master"],
                "source_metrics": "invalid",
            },
            allow_volume_drop=False,
            min_first_publish_providers=1,
            min_publish_ratio=0.8,
        )


@pytest.mark.asyncio
async def test_publication_treats_malformed_previous_metrics_as_unavailable(
    monkeypatch,
):
    run_id = "f" * 32
    live_name = ProviderProfileProjection.__tablename__
    started_at = datetime(2026, 7, 27, tzinfo=UTC)
    database = _PublicationDb(
        scalar_results=[1, 1, 1],
        all_results=[
            [_Row(generation_id="1" * 32, provider_count=1)],
            [_Row(tablename=live_name)],
        ],
        current_run=_Row(
            started_at=started_at - timedelta(days=1),
            metrics={
                "source_records": "unknown",
                "source_metrics": "unknown",
            },
        ),
    )
    monkeypatch.setattr(florida, "db", database)

    publication, metrics = await florida._publish_projection_swap(
        run_id,
        _one_projection_row(run_id),
        started_at=started_at,
        completion_metrics=_completion_metrics(1),
        allow_volume_drop=False,
        min_first_publish_providers=1,
        min_publish_ratio=0.8,
    )

    assert publication["published_rows"] == 1
    assert metrics["published_providers"] == 1
    assert publication["volume_guard"]["previous_source_records"] is None
