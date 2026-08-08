"""Exact-wave recovery storage and selection boundary contracts."""

from __future__ import annotations

import importlib.util
from pathlib import Path
from unittest.mock import AsyncMock

import pytest
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from db.models import (
    PTGImportWaveQuarantine,
    PTGImportWaveSupersession,
)
from process import ptg_wave_controller as controller
from process.ptg_parts import ptg_wave_admission_fence as fence


ROOT = Path(__file__).resolve().parents[1]
MIGRATION_PATH = ROOT / "alembic" / "versions" / (
    "20260807120000_ptg_import_wave_recovery_storage.py"
)


def _load_migration():
    module_spec = importlib.util.spec_from_file_location(
        "ptg_import_wave_recovery_storage_migration", MIGRATION_PATH,
    )
    assert module_spec is not None and module_spec.loader is not None
    migration = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(migration)
    return migration


def _upgrade_sql(monkeypatch) -> tuple[object, str]:
    migration = _load_migration()
    statements: list[str] = []
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "wave_recovery_test")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    monkeypatch.setattr(migration.op, "execute", statements.append)
    migration.upgrade()
    return migration, " ".join(" ".join(statement.split()) for statement in statements)


def _downgrade_sql(monkeypatch) -> str:
    migration = _load_migration()
    statements: list[str] = []
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "wave_recovery_test")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    monkeypatch.setattr(migration.op, "execute", statements.append)
    migration.downgrade()
    return " ".join(" ".join(statement.split()) for statement in statements)


def _sql(statement) -> str:
    return str(statement.compile(
        dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True},
    ))


def _constraint(model, name: str):
    return next(item for item in model.__table__.constraints if item.name == name)


def test_recovery_storage_is_head_child_and_append_only(monkeypatch):
    migration, sql = _upgrade_sql(monkeypatch)

    assert migration.revision == "20260807120000_ptg_import_wave_recovery_storage"
    assert migration.down_revision == "20260807110000_fhir_formulary_storage_foundation"
    assert 'CREATE TABLE "wave_recovery_test"."ptg_import_wave_quarantine"' in sql
    assert 'CREATE TABLE "wave_recovery_test"."ptg_import_wave_supersession"' in sql
    assert "predecessor_wave_id varchar(64) PRIMARY KEY" in sql
    assert "successor_wave_id varchar(64) NOT NULL" in sql
    assert "ptg_import_wave_supersession_successor_wave_id_key" in sql
    assert "recovery_basis = 'logical_preclaim_failure'" in sql
    assert "jsonb_typeof(recovery_evidence) = 'object'" in sql
    assert "recovery_evidence_sha256 ~ '^[0-9a-f]{64}$'" in sql
    assert "recovery_evidence_canonical bytea NOT NULL" in sql
    assert "encode(sha256(recovery_evidence_canonical), 'hex') = recovery_evidence_sha256" in sql
    assert "convert_from(recovery_evidence_canonical, 'UTF8')::jsonb = recovery_evidence - 'proof_digest'" in sql
    assert "BEFORE UPDATE OR DELETE" in sql
    assert "BEFORE TRUNCATE" in sql
    assert sql.count("ENABLE ALWAYS TRIGGER") == 5
    assert "DEFERRABLE INITIALLY DEFERRED" in sql
    assert "DROP INDEX IF EXISTS \"wave_recovery_test\".\"ptg_import_wave_single_capacity_owner_idx\"" in sql
    assert "LOCK TABLE \"wave_recovery_test\".\"ptg_import_wave\", \"wave_recovery_test\".\"ptg_import_wave_quarantine\", \"wave_recovery_test\".\"ptg_import_wave_supersession\", \"wave_recovery_test\".\"ptg_import_wave_intent\", \"wave_recovery_test\".\"ptg_import_wave_claim\", \"wave_recovery_test\".\"ptg_import_wave_outcome\", \"wave_recovery_test\".\"import_run\", \"wave_recovery_test\".\"ptg_source_attempt_event\" IN SHARE ROW EXCLUSIVE MODE" in sql
    assert sql.count("CREATE CONSTRAINT TRIGGER") == 3
    assert "pg_advisory_xact_lock" in sql
    assert "PTG_IMPORT_WAVE_EFFECTIVE_OWNER_CONFLICT" in sql
    assert "NOT EXISTS ( SELECT 1 FROM \"wave_recovery_test\".\"ptg_import_wave_supersession\" AS retired" in sql
    assert sql.index("LOCK TABLE") < sql.index("INSERT INTO \"wave_recovery_test\".\"ptg_import_wave_quarantine\"")
    assert "CREATE TRIGGER \"ptg_import_wave_supersession_preclaim_guard\" BEFORE INSERT" in sql
    assert "PTG_IMPORT_WAVE_SUPERSESSION_PRECLAIM_REQUIRED" in sql
    assert "PTG_IMPORT_WAVE_SUPERSESSION_EVIDENCE_INVALID" in sql
    assert "FOR UPDATE" in sql
    assert "event_kind = 'worker_start_admitted'" in sql
    assert "predecessor_wave_id = NEW.predecessor_wave_id" in sql
    assert "run.phase_detail IS DISTINCT FROM 'wave admitted; controller materialization pending'" in sql
    assert "run.progress::jsonb IS DISTINCT FROM jsonb_build_object" in sql
    assert "run.metrics::jsonb IS DISTINCT FROM jsonb_build_object" in sql
    assert "NEW.recovery_evidence->>'proof_digest' IS DISTINCT FROM NEW.recovery_evidence_sha256" in sql
    assert "count(*) FROM jsonb_object_keys(NEW.recovery_evidence)) <> 8" in sql
    assert "count(*) FROM jsonb_object_keys(NEW.recovery_evidence->'predecessor')) <> 5" in sql
    assert "count(*) FROM jsonb_object_keys(NEW.recovery_evidence->'database')) <> 4" in sql
    assert "count(*) FROM jsonb_object_keys(NEW.recovery_evidence->'kubernetes')) <> 13" in sql
    assert "count(*) FROM jsonb_object_keys(NEW.recovery_evidence->'redis')) <> 9" in sql
    assert "NEW.recovery_evidence #>> '{kubernetes,completions}' !~ '^12$'" in sql
    assert "NEW.recovery_evidence #>> '{kubernetes,backoff_limit}' !~ '^0$'" in sql
    assert "'hpw-ptg-wave-' || left(predecessor.wave_digest, 40)" in sql
    assert "NEW.recovery_evidence #> '{kubernetes,failed_condition}' IS DISTINCT FROM 'true'::jsonb" in sql
    assert "NEW.recovery_evidence #> '{redis,release_present}' IS DISTINCT FROM 'false'::jsonb" in sql
    assert "NEW.recovery_evidence #>> '{redis,unclaimed_attestation_digest}' !~ '^[0-9a-f]{64}$'" in sql
    assert "CREATE CONSTRAINT TRIGGER \"ptg_import_wave_supersession_successor_binding_guard\"" in sql
    assert "PTG_IMPORT_WAVE_SUPERSESSION_SUCCESSOR_BINDING_INVALID" in sql
    assert "successor.inserted_xid IS DISTINCT FROM pg_current_xact_id()::xid" in sql
    assert "successor.state IS DISTINCT FROM 'admitted'" in sql
    assert "healthporta.ptg-import-wave-attestation.v3" in sql
    assert "successor.cohort_attestation::jsonb->'supersession' IS DISTINCT FROM NEW.recovery_evidence" in sql
    assert "CREATE TRIGGER \"ptg_import_wave_quarantine_update_guard\" BEFORE UPDATE" in sql
    assert "PTG_IMPORT_WAVE_QUARANTINED_IMMUTABLE" in sql


def test_legacy_seed_is_narrow_and_does_not_modify_waves(monkeypatch):
    _migration, sql = _upgrade_sql(monkeypatch)

    assert "INSERT INTO \"wave_recovery_test\".\"ptg_import_wave_quarantine\"" in sql
    assert "state = 'uncertain'" in sql
    assert "uncertainty_resume_state = 'slots_waiting'" in sql
    assert "k8s_post_ticket IS NOT NULL" in sql
    assert "k8s_post_started_at IS NOT NULL" in sql
    assert "kubernetes_job_uid IS NULL" in sql
    assert "kubernetes_job_receipt IS NULL" in sql
    assert "kubernetes_job_receipt_digest IS NULL" in sql
    assert "redis_release_attestation_digest IS NULL" in sql
    assert "cleanup_evidence_digest IS NULL" in sql
    assert "resolved_at IS NULL" in sql
    assert "UPDATE \"wave_recovery_test\".\"ptg_import_wave\"" not in sql
    assert "DELETE FROM \"wave_recovery_test\".\"ptg_import_wave\"" not in sql


def test_downgrade_refuses_recovery_evidence_and_restores_prior_owner_index(monkeypatch):
    sql = _downgrade_sql(monkeypatch)

    assert "PTG_IMPORT_WAVE_RECOVERY_DOWNGRADE_REFUSED" in sql
    assert "LOCK TABLE \"wave_recovery_test\".\"ptg_import_wave\", \"wave_recovery_test\".\"ptg_import_wave_quarantine\", \"wave_recovery_test\".\"ptg_import_wave_supersession\" IN ACCESS EXCLUSIVE MODE" in sql
    assert "DROP TRIGGER \"ptg_import_wave_effective_owner_guard\"" in sql
    assert "DROP TRIGGER \"ptg_import_wave_quarantine_update_guard\"" in sql
    assert "DROP FUNCTION \"wave_recovery_test\".\"ptg_import_wave_quarantine_update_guard\"()" in sql
    assert "DROP FUNCTION \"wave_recovery_test\".\"ptg_import_wave_supersession_successor_binding_guard\"()" in sql
    assert "CREATE UNIQUE INDEX \"ptg_import_wave_single_capacity_owner_idx\"" in sql


def test_models_bind_immutable_predecessor_and_successor_contracts():
    quarantine = PTGImportWaveQuarantine.__table__
    supersession = PTGImportWaveSupersession.__table__

    assert tuple(quarantine.primary_key.columns.keys()) == ("predecessor_wave_id",)
    assert tuple(supersession.primary_key.columns.keys()) == ("predecessor_wave_id",)
    assert isinstance(
        _constraint(
            PTGImportWaveSupersession,
            "ptg_import_wave_supersession_successor_wave_id_key",
        ),
        sa.UniqueConstraint,
    )
    assert supersession.c.recovery_evidence.type.__class__.__name__ == "JSONB"
    assert supersession.c.recovery_evidence_canonical.type.__class__.__name__ == "LargeBinary"
    assert "jsonb_typeof(recovery_evidence)" in str(
        _constraint(
            PTGImportWaveSupersession,
            "ptg_import_wave_supersession_evidence_check",
        ).sqltext
    )
    assert "sha256(recovery_evidence_canonical)" in str(
        _constraint(
            PTGImportWaveSupersession,
            "ptg_import_wave_supersession_evidence_check",
        ).sqltext
    )
    assert "legacy_uncertain_slots_waiting_pre_receipt" in str(
        _constraint(
            PTGImportWaveQuarantine,
            "ptg_import_wave_quarantine_reason_check",
        ).sqltext
    )
    successor_fk = _constraint(
        PTGImportWaveSupersession,
        "ptg_import_wave_supersession_successor_wave_fkey",
    )
    assert successor_fk.deferrable is True
    assert successor_fk.initially == "DEFERRED"


class _Result:
    def __init__(self, rows):
        self.rows = list(rows)

    def scalars(self):
        return self

    def all(self):
        return list(self.rows)


class _Session:
    def __init__(self, *results):
        self.results = list(results)
        self.statements = []

    async def execute(self, statement):
        self.statements.append(statement)
        return self.results.pop(0)


class _Context:
    def __init__(self, session):
        self.session = session

    async def __aenter__(self):
        return self.session

    async def __aexit__(self, exc_type, exc, traceback):
        return False


@pytest.mark.asyncio
async def test_quarantine_only_silences_controller_selection(monkeypatch):
    session = _Session(_Result([]), _Result([]))
    monkeypatch.setattr(controller.db, "session", lambda: _Context(session))

    assert await controller.load_capacity_owning_wave() is None
    sql = _sql(session.statements[0])
    assert "ptg_import_wave_quarantine" in sql
    assert "ptg_import_wave_supersession" not in sql
    assert await controller.reconcile_ptg_wave_once(
        object(), image="unused", runtime_image="unused",
    ) == "idle"
    assert len(session.statements) == 2


@pytest.mark.asyncio
async def test_normal_work_still_blocks_and_supersession_is_predecessor_scoped(monkeypatch):
    capacity_owning_waves = fence._capacity_owning_waves
    monkeypatch.setattr(fence, "_capacity_owning_waves", AsyncMock(return_value=[]))
    captured = []

    async def active_run(_executor, statement):
        captured.append(statement)
        return [("normal-active-run",)]

    monkeypatch.setattr(fence, "_all", active_run)
    with pytest.raises(fence.PTGWaveCapacityConflict, match="active PTG work"):
        await fence.require_wave_admission_capacity(object())

    active_sql = _sql(captured[0])
    assert "ptg_import_wave_supersession" in active_sql
    assert "ptg_import_wave_intent" in active_sql
    assert "ptg_import_wave_quarantine" not in active_sql

    captured.clear()
    monkeypatch.setattr(fence, "_has_wave_table", AsyncMock(return_value=True))
    monkeypatch.setattr(fence, "_all", active_run)
    executor = type("Executor", (), {"all": object()})()
    assert await capacity_owning_waves(executor)
    owner_sql = _sql(captured[0])
    assert "ptg_import_wave_supersession" in owner_sql
    assert "ptg_import_wave_quarantine" not in owner_sql
