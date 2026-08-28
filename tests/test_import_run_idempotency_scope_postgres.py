# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import pytest
from sqlalchemy import select, text, update
from sqlalchemy.exc import IntegrityError

from api import control_imports
from db.models import ImportRun, db
from tests.test_control_imports_db import (
    _drop_import_run_schema,
    _fake_enqueue,
    _reset_import_run_schema,
)


pytestmark = [
    pytest.mark.asyncio(loop_scope="module"),
    pytest.mark.filterwarnings(
        "ignore:coroutine 'Connection._cancel' was never awaited:RuntimeWarning"
    ),
]


async def _create_run(run_id: str, importer: str):
    return await control_imports.create_import_run(
        {
            "run_id": run_id,
            "importer": importer,
            "idempotency_key": "idem-db",
        }
    )


async def _assert_same_importer_integrity_recovery(monkeypatch):
    real_find = control_imports.find_active_run_by_idempotency_key
    real_find_importer = control_imports.find_earliest_active_run_by_importer
    lookup_count_by_kind = {"active": 0}

    async def race_miss_then_real(importer: str, idempotency_key: str):
        lookup_count_by_kind["active"] += 1
        if lookup_count_by_kind["active"] == 1:
            return None
        return await real_find(importer, idempotency_key)

    async def race_importer_miss(importer: str):
        assert importer == "nucc"
        return None

    monkeypatch.setattr(
        control_imports,
        "find_active_run_by_idempotency_key",
        race_miss_then_real,
    )
    monkeypatch.setattr(
        control_imports,
        "find_earliest_active_run_by_importer",
        race_importer_miss,
    )
    replayed, created = await _create_run("run_replayed", "nucc")
    assert created is False
    assert replayed["run_id"] == "run_first"
    assert lookup_count_by_kind["active"] == 2
    return real_find, real_find_importer


async def _assert_prepared_index_definitions():
    index_definition_by_name = {
        str(index_record.index_name): str(index_record.index_definition)
        for index_record in (
            await db.execute(
                text(
                    """
                    SELECT index_record.relname AS index_name,
                           pg_get_indexdef(index_record.oid) AS index_definition
                      FROM pg_class AS index_record
                      JOIN pg_namespace AS namespace_record
                        ON namespace_record.oid = index_record.relnamespace
                     WHERE namespace_record.nspname = :schema
                       AND index_record.relname IN (
                           'import_run_active_idempotency_idx',
                           'import_run_importer_active_idempotency_idx'
                       )
                    """
                ),
                schema=ImportRun.__table__.schema or "mrf",
            )
        ).all()
    }
    assert set(index_definition_by_name) == {
        "import_run_active_idempotency_idx",
        "import_run_importer_active_idempotency_idx",
    }
    assert "(idempotency_key)" in index_definition_by_name[
        "import_run_active_idempotency_idx"
    ]
    assert "(importer, idempotency_key)" in index_definition_by_name[
        "import_run_importer_active_idempotency_idx"
    ]


async def test_prepared_idempotency_indexes_fail_closed_until_activation(
    monkeypatch,
):
    """Keep global safety while preparing importer-scoped activation."""

    await _reset_import_run_schema()
    try:
        monkeypatch.setattr(control_imports, "_enqueue_import_start", _fake_enqueue)
        first, first_created = await _create_run("run_first", "nucc")
        assert first_created is True and first["run_id"] == "run_first"

        with pytest.raises(IntegrityError):
            await _create_run("run_cross_importer", "npi")
        await _assert_prepared_index_definitions()

        real_find, real_find_importer = await _assert_same_importer_integrity_recovery(
            monkeypatch
        )
        await db.execute(
            update(ImportRun)
            .where(ImportRun.run_id == "run_first")
            .values(status="succeeded", finished_at=control_imports.utc_now())
        )
        monkeypatch.setattr(
            control_imports,
            "find_active_run_by_idempotency_key",
            real_find,
        )
        monkeypatch.setattr(
            control_imports,
            "find_earliest_active_run_by_importer",
            real_find_importer,
        )

        after_terminal, created = await _create_run("run_after_terminal", "nucc")
        assert created is True
        assert after_terminal["run_id"] == "run_after_terminal"
        import_runs = (
            (await db.execute(select(ImportRun).order_by(ImportRun.run_id)))
            .scalars()
            .all()
        )
        assert [run.run_id for run in import_runs] == [
            "run_after_terminal",
            "run_first",
        ]
    finally:
        await _drop_import_run_schema()
