# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from contextlib import asynccontextmanager
import importlib

import pytest


entity_address_unified = importlib.import_module("process.entity_address_unified")


class _LiveTable:
    __main_table__ = "entity_address_unified"
    __my_additional_indexes__ = []


class _StageTable:
    __tablename__ = "entity_address_unified_stage"
    __my_additional_indexes__ = []


class _SupportLiveTable:
    __main_table__ = "entity_address_evidence"
    __my_additional_indexes__ = []


class _SupportStageTable:
    __tablename__ = "entity_address_evidence_stage"
    __my_additional_indexes__ = []


class _RecordingDB:
    def __init__(
        self,
        persistence_values=("p",),
        advisory_values=(True,),
        existing_names=None,
    ):
        self.persistence_values = list(persistence_values)
        self.advisory_values = list(advisory_values)
        self.statements = []
        self.transaction_events = []
        self.existing_names = set(
            existing_names
            or {
                "entity_address_unified",
                "entity_address_unified_stage",
            }
        )

    async def scalar(self, statement, **_params):
        if "relpersistence" in statement:
            return self.persistence_values.pop(0)
        if "pg_try_advisory_xact_lock" in statement:
            return self.advisory_values.pop(0)
        raise AssertionError(f"unexpected scalar SQL: {statement}")

    async def all(self, statement, **params):
        assert "ORDER BY c.relname" in statement
        return [
            (name,)
            for name in sorted(set(params["relation_names"]) & self.existing_names)
        ]

    async def status(self, statement, **_params):
        self.statements.append(statement)
        return 0

    @asynccontextmanager
    async def transaction(self):
        self.transaction_events.append("BEGIN")
        try:
            yield
        except Exception:
            self.transaction_events.append("ROLLBACK")
            raise
        else:
            self.transaction_events.append("COMMIT")


@pytest.mark.asyncio
async def test_entity_address_cutover_uses_one_fail_fast_transaction(monkeypatch):
    recording_db = _RecordingDB()
    monkeypatch.setattr(entity_address_unified, "db", recording_db)
    monkeypatch.delenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_CUTOVER_LOCK_TIMEOUT", raising=False)

    cutover_context_map = {}
    await entity_address_unified._publish_staged_entity_address_tables(
        "mrf",
        _StageTable,
        {},
        partial_support_patch=False,
        affected_group_table="",
        context=cutover_context_map,
    )

    assert recording_db.transaction_events == ["BEGIN", "COMMIT"]
    assert recording_db.statements[0] == "SET LOCAL lock_timeout = '50ms';"
    assert recording_db.statements[1] == (
        "LOCK TABLE mrf.entity_address_unified, mrf.entity_address_unified_stage "
        "IN ACCESS EXCLUSIVE MODE NOWAIT;"
    )
    assert recording_db.statements[2:5] == [
        "DROP TABLE IF EXISTS mrf.entity_address_unified_old;",
        "ALTER TABLE IF EXISTS mrf.entity_address_unified RENAME TO entity_address_unified_old;",
        "ALTER TABLE mrf.entity_address_unified_stage RENAME TO entity_address_unified;",
    ]
    assert cutover_context_map["cutover_attempts"] == 1
    assert cutover_context_map["stage_persistence"] == "p"


@pytest.mark.asyncio
async def test_entity_address_cutover_converts_unlogged_stage_before_transaction(monkeypatch):
    recording_db = _RecordingDB(persistence_values=("u", "p"))
    monkeypatch.setattr(entity_address_unified, "db", recording_db)

    await entity_address_unified._publish_staged_entity_address_tables(
        "mrf",
        _StageTable,
        {},
        partial_support_patch=False,
        affected_group_table="",
        context={},
    )

    assert recording_db.statements[0] == "ALTER TABLE mrf.entity_address_unified_stage SET LOGGED;"
    assert recording_db.transaction_events == ["BEGIN", "COMMIT"]


@pytest.mark.asyncio
async def test_entity_address_cutover_converts_every_promoted_stage_to_logged(monkeypatch):
    recording_db = _RecordingDB(
        persistence_values=("u", "p", "u", "p"),
        existing_names={
            "entity_address_unified",
            "entity_address_unified_stage",
            "entity_address_evidence",
            "entity_address_evidence_stage",
        },
    )
    monkeypatch.setattr(entity_address_unified, "db", recording_db)

    await entity_address_unified._publish_staged_entity_address_tables(
        "mrf",
        _StageTable,
        {_SupportLiveTable: _SupportStageTable},
        partial_support_patch=False,
        affected_group_table="",
        context={},
    )

    assert recording_db.statements[:2] == [
        "ALTER TABLE mrf.entity_address_unified_stage SET LOGGED;",
        "ALTER TABLE mrf.entity_address_evidence_stage SET LOGGED;",
    ]
    assert recording_db.transaction_events == ["BEGIN", "COMMIT"]


@pytest.mark.asyncio
async def test_entity_address_cutover_retries_only_within_bound(monkeypatch):
    recording_db = _RecordingDB(advisory_values=(False, False, True))
    monkeypatch.setattr(entity_address_unified, "db", recording_db)
    monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_CUTOVER_RETRY_ATTEMPTS", "3")
    monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_CUTOVER_RETRY_BACKOFF_MS", "0")
    monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_CUTOVER_RETRY_MAX_BACKOFF_MS", "0")

    cutover_context_map = {}
    await entity_address_unified._publish_staged_entity_address_tables(
        "mrf",
        _StageTable,
        {},
        partial_support_patch=False,
        affected_group_table="",
        context=cutover_context_map,
    )

    assert recording_db.transaction_events == [
        "BEGIN",
        "ROLLBACK",
        "BEGIN",
        "ROLLBACK",
        "BEGIN",
        "COMMIT",
    ]
    assert cutover_context_map["cutover_attempts"] == 3


@pytest.mark.asyncio
async def test_entity_address_cutover_does_not_retry_non_lock_failure(monkeypatch):
    recording_db = _RecordingDB()

    async def fail_catalog_query(_statement, **_params):
        raise RuntimeError("catalog failure")

    recording_db.all = fail_catalog_query
    monkeypatch.setattr(entity_address_unified, "db", recording_db)
    monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_CUTOVER_RETRY_ATTEMPTS", "4")

    with pytest.raises(RuntimeError, match="catalog failure"):
        await entity_address_unified._publish_staged_entity_address_tables(
            "mrf",
            _StageTable,
            {},
            partial_support_patch=False,
            affected_group_table="",
            context={},
        )

    assert recording_db.transaction_events == ["BEGIN", "ROLLBACK"]
