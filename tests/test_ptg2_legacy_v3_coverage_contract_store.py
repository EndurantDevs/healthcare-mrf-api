"""Branch contracts for the legacy-V3 plan, orchestration, and CAS store."""

from __future__ import annotations

import datetime as dt
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process.ptg_parts import ptg2_legacy_v3_metadata_contract as contract
from process.ptg_parts import ptg2_legacy_v3_metadata_reconcile as reconcile
from process.ptg_parts import ptg2_legacy_v3_metadata_store as store


_SNAPSHOT_ID = "ptg2:202607:coverage"
_INTERNAL_RUN_ID = "ptg2:coverage-run"
_OUTER_RUN_ID = "run_coverage"
_SOURCE_IMPORT_ID = "source-import-coverage"
_DIGEST = "a" * 64

class _QueryResult:
    def __init__(self, *, scalar=None, rowcount: int = 1) -> None:
        self._scalar = scalar
        self.rowcount = rowcount

    def scalar_one_or_none(self):
        return self._scalar

class _Session:
    def __init__(self, *responses: _QueryResult) -> None:
        self.responses = list(responses)
        self.calls: list[tuple[str, dict | None]] = []

    async def execute(self, statement, parameters=None):
        self.calls.append((str(statement), parameters))
        if self.responses:
            return self.responses.pop(0)
        return _QueryResult()

class _Transaction:
    def __init__(self, session: _Session) -> None:
        self.session = session

    async def __aenter__(self):
        return self.session

    async def __aexit__(self, exc_type, exc, traceback):
        return False

def _database(session: _Session):
    return SimpleNamespace(transaction=lambda: _Transaction(session))

def _coordinates() -> reconcile._ReconcileCoordinates:
    return reconcile._ReconcileCoordinates(
        snapshot_id=_SNAPSHOT_ID,
        internal_run_id=_INTERNAL_RUN_ID,
        outer_run_id=_OUTER_RUN_ID,
    )

def _ready_plan() -> dict[str, object]:
    return {
        "status": "ready",
        "reason_codes": [],
        "target_digest": "b" * 64,
        "plan_digest": _DIGEST,
        "attachment_digest": "c" * 64,
        "catalog_digest": "d" * 64,
        "event_high_water_mark": "7",
        "retained_state_digest": "e" * 64,
        "preserved_row_digest": "f" * 64,
    }

def _reconcile_write() -> store.LegacyV3ReconcileWrite:
    return store.LegacyV3ReconcileWrite(
        schema_name="mrf",
        snapshot_id=_SNAPSHOT_ID,
        internal_run_id=_INTERNAL_RUN_ID,
        source_file_import_id=_SOURCE_IMPORT_ID,
        outer_run_id=_OUTER_RUN_ID,
        target_digest="b" * 64,
        plan_digest=_DIGEST,
        attachment_digest="c" * 64,
        catalog_digest="d" * 64,
        event_high_water_mark=7,
        reconciliation_id="e" * 64,
        marker={"observed_at": dt.datetime(2026, 8, 1, tzinfo=dt.UTC)},
    )

def test_contract_payload_timestamp_and_stale_age_edges() -> None:
    assert contract._payload(None) == {}
    assert contract._payload({"payload": "not-a-map"}) == {}
    assert contract._payload({"payload": {"status": "running"}}) == {
        "status": "running"
    }
    assert contract._timestamp(None) is None
    assert contract._timestamp("not-a-date") is None
    assert contract._timestamp(dt.datetime(2026, 8, 1)).tzinfo is dt.UTC
    assert contract._timestamp("2026-08-01T02:00:00+02:00") == dt.datetime(
        2026, 8, 1, tzinfo=dt.UTC
    )
    assert contract._stale_age_seconds(
        {"internal_run": {"payload": {"heartbeat_at": "2026-08-02Z"}}},
        dt.datetime(2026, 8, 1, tzinfo=dt.UTC),
    ) == 0

def test_contract_attachment_reasons_report_every_catalog_failure() -> None:
    counts_by_name = {name: 1 for name in contract.ALLOWED_ATTACHMENT_NAMES}
    first_name, second_name = sorted(counts_by_name)[:2]
    counts_by_name[first_name] = -1
    counts_by_name[second_name] = 0
    counts_by_name["unexpected_relation"] = 2
    reasons = contract._attachment_reasons(
        {
            "attachment_counts": counts_by_name,
            "dynamic_relations": {
                "suffix_valid": False,
                "relation_count": 2,
            },
        }
    )
    assert set(reasons) == {
        "attachment_catalog_incomplete",
        "attachment_set_not_exact",
        "retained_attachment_missing",
        "retained_attachment_cardinality_changed",
        "legacy_suffix_unproved",
        "legacy_dynamic_relation_present",
    }

def test_contract_state_reasons_report_all_invalid_state_views() -> None:
    reasons, source_import_id = contract._state_reasons(
        {
            "snapshot": {
                "payload": {
                    "snapshot_id": "other",
                    "import_run_id": "other",
                    "status": "published",
                    "validated_at": "now",
                    "published_at": "now",
                    "manifest": {"present": True},
                }
            },
            "internal_run": {
                "payload": {
                    "import_run_id": "other",
                    "status": "failed",
                    "finished_at": "now",
                    "options": {
                        "storage_generation": "shared_blocks_v4",
                        "snapshot_arch": "arrow_v4",
                        "source_file_import_id": " spaced ",
                    },
                }
            },
            "run_snapshots": [{"snapshot_id": "other"}],
        },
        snapshot_id=_SNAPSHOT_ID,
        internal_run_id=_INTERNAL_RUN_ID,
    )
    assert source_import_id == " spaced "
    assert len(reasons) == 13
    assert "internal_run_reverse_pair_changed" in reasons
    assert "source_file_import_id_missing" in reasons


def test_contract_source_pairs_and_eligibility_fail_closed() -> None:
    pair_reasons = contract._source_pair_reasons(
        {
            "source_internal_runs": [{"payload": "invalid"}],
            "source_snapshots": [{"snapshot_id": "other"}],
        },
        snapshot_id=_SNAPSHOT_ID,
        internal_run_id=_INTERNAL_RUN_ID,
    )
    reasons, _, stale_age = contract._eligibility_reasons(
        {},
        {"exact_external_absence": False},
        snapshot_id=_SNAPSHOT_ID,
        internal_run_id=_INTERNAL_RUN_ID,
        outer_run_id=_OUTER_RUN_ID,
        observed_at=dt.datetime(2026, 8, 1, tzinfo=dt.UTC),
        capabilities_ready=False,
    )
    assert pair_reasons == [
        "source_internal_run_cardinality_changed",
        "source_snapshot_cardinality_changed",
    ]
    assert stale_age is None
    assert "stale_reference_missing" in reasons
    assert "shared_attempt_guard_capability_missing" in reasons
    assert "external_attempt_identity_present" in reasons


def test_contract_existing_audit_paths(monkeypatch) -> None:
    observation_by_field = {
        "audit": {"payload": {"plan_digest": _DIGEST}},
        "source_file_import_id": _SOURCE_IMPORT_ID,
    }
    arguments_by_name = {
        "snapshot_id": _SNAPSHOT_ID,
        "internal_run_id": _INTERNAL_RUN_ID,
        "outer_run_id": _OUTER_RUN_ID,
        "target_digest": "b" * 64,
        "capabilities_ready": True,
    }
    monkeypatch.setattr(contract, "reconciled_state_reasons", lambda _review: ["audit_changed"])
    rejected = contract._existing_audit_plan(
        observation_by_field, {}, **arguments_by_name
    )
    monkeypatch.setattr(contract, "reconciled_state_reasons", lambda _review: [])
    accepted = contract._existing_audit_plan(
        observation_by_field, {}, **arguments_by_name
    )
    assert rejected["reason_codes"] == ["audit_changed"]
    assert accepted["status"] == "already_reconciled"
    assert accepted["plan_digest"] == _DIGEST


@pytest.mark.parametrize(
    ("identifier", "field_name"),
    (("", "snapshot_id"), ("bad/id", "outer_run_id"), ("x" * 97, "internal_run_id")),
)
def test_reconcile_identifiers_fail_closed(identifier: str, field_name: str) -> None:
    with pytest.raises(ValueError, match=field_name):
        reconcile._identifier(identifier, field_name)


def test_reconcile_coordinates_and_digest_normalize() -> None:
    coordinates = reconcile._coordinates(
        f" {_SNAPSHOT_ID} ",
        _INTERNAL_RUN_ID,
        _OUTER_RUN_ID,
    )
    assert coordinates.snapshot_id == _SNAPSHOT_ID
    assert reconcile._digest(f" {'A' * 64} ") == _DIGEST
    with pytest.raises(ValueError, match="SHA-256"):
        reconcile._digest("not-a-digest")


@pytest.mark.asyncio
async def test_reconcile_capability_adapters(monkeypatch) -> None:
    available = AsyncMock(return_value=None)
    monkeypatch.setattr(reconcile, "require_source_attempt_capabilities", available)
    assert await reconcile._has_required_capabilities(object()) is True
    await reconcile._require_complete_capabilities(object())

    unavailable = AsyncMock(side_effect=RuntimeError("PTG_SOURCE_ATTEMPT_CAPABILITY missing"))
    monkeypatch.setattr(reconcile, "require_source_attempt_capabilities", unavailable)
    assert await reconcile._has_required_capabilities(object()) is False
    with pytest.raises(reconcile.LegacyV3MetadataConflict, match="capability is unavailable"):
        await reconcile._require_complete_capabilities(object())

    unexpected = AsyncMock(side_effect=RuntimeError("unexpected"))
    monkeypatch.setattr(reconcile, "require_source_attempt_capabilities", unexpected)
    with pytest.raises(RuntimeError, match="unexpected"):
        await reconcile._has_required_capabilities(object())
    with pytest.raises(RuntimeError, match="unexpected"):
        await reconcile._require_complete_capabilities(object())


@pytest.mark.asyncio
async def test_reconcile_public_plan_orchestration(monkeypatch) -> None:
    observation_by_field = {"outer_runs": [1], "event_rows": [2]}
    monkeypatch.setattr(
        reconcile,
        "_database_observation",
        AsyncMock(return_value=(observation_by_field, False)),
    )
    absence = AsyncMock(return_value={"exact_external_absence": True})
    monkeypatch.setattr(reconcile, "load_exact_operational_absence", absence)
    builder = Mock()
    expected_plan_by_field = {"status": "ineligible"}
    monkeypatch.setattr(
        reconcile,
        "build_legacy_v3_reconcile_plan",
        lambda *_args, **kwargs: (builder(kwargs), expected_plan_by_field)[1],
    )
    plan = await reconcile.plan_legacy_v3_metadata_reconcile(
        snapshot_id=f" {_SNAPSHOT_ID} ",
        internal_run_id=_INTERNAL_RUN_ID,
        outer_run_id=_OUTER_RUN_ID,
    )
    assert plan is expected_plan_by_field
    absence.assert_awaited_once_with([1], [2])
    assert builder.call_args.args[0]["capabilities_ready"] is False


@pytest.mark.asyncio
async def test_reconcile_lock_target_uses_both_durable_locks(monkeypatch) -> None:
    session = _Session(_QueryResult(scalar=_SOURCE_IMPORT_ID), _QueryResult(), _QueryResult())
    lifecycle_lock = AsyncMock()
    relation_lock = AsyncMock()
    monkeypatch.setattr(reconcile, "acquire_ptg2_lifecycle_lock", lifecycle_lock)
    monkeypatch.setattr(reconcile, "lock_legacy_v3_reconcile_relations", relation_lock)
    monkeypatch.setenv(reconcile._ATTEMPT_AUTHORITY_SCHEMA_ENV, "source_authority")
    source_import_id = await reconcile._lock_reconcile_target(
        session,
        schema_name="mrf",
        snapshot_id=_SNAPSHOT_ID,
        internal_run_id=_INTERNAL_RUN_ID,
        outer_run_id=_OUTER_RUN_ID,
    )
    assert source_import_id == _SOURCE_IMPORT_ID
    assert len(session.calls) == 3
    assert session.calls[1][1]["lock_key"].endswith(_SOURCE_IMPORT_ID)
    assert session.calls[2][1]["pair_lock_key"].startswith(reconcile._PAIR_LOCK_NAMESPACE)
    lifecycle_lock.assert_awaited_once_with(session)
    assert relation_lock.await_args.kwargs["control_schema_name"] == "source_authority"


@pytest.mark.asyncio
@pytest.mark.parametrize("observed_source", (_SOURCE_IMPORT_ID, "changed-source"))
async def test_reconcile_locked_observation_checks_source_identity(monkeypatch, observed_source: str) -> None:
    monkeypatch.setattr(reconcile, "_require_complete_capabilities", AsyncMock())
    monkeypatch.setattr(reconcile, "_lock_reconcile_target", AsyncMock(return_value=_SOURCE_IMPORT_ID))
    monkeypatch.setattr(
        reconcile,
        "load_legacy_v3_reconcile_observation",
        AsyncMock(return_value={"source_file_import_id": observed_source}),
    )
    monkeypatch.setenv(reconcile._ATTEMPT_AUTHORITY_SCHEMA_ENV, "source_authority")
    if observed_source != _SOURCE_IMPORT_ID:
        with pytest.raises(reconcile.LegacyV3MetadataConflict, match="source attempt changed"):
            await reconcile._locked_observation(_Session(), schema_name="mrf", coordinates=_coordinates())
    else:
        source_import_id, observation = await reconcile._locked_observation(
            _Session(), schema_name="mrf", coordinates=_coordinates()
        )
        assert source_import_id == observation["source_file_import_id"]


@pytest.mark.parametrize(
    ("status", "plan_digest", "error"),
    (
        ("already_reconciled", _DIGEST, None),
        ("already_reconciled", "b" * 64, "completed reconciliation"),
        ("ineligible", None, "not eligible"),
        ("ready", "b" * 64, "state changed"),
        ("ready", _DIGEST, None),
    ),
)
def test_reconcile_review_locked_plan(monkeypatch, status, plan_digest, error) -> None:
    plan_by_field = {
        "status": status,
        "plan_digest": plan_digest,
        "reason_codes": ["blocked"],
    }
    monkeypatch.setattr(
        reconcile,
        "build_legacy_v3_reconcile_plan",
        lambda *_args, **_kwargs: plan_by_field,
    )
    if error:
        with pytest.raises(reconcile.LegacyV3MetadataConflict, match=error):
            reconcile._review_locked_plan({}, {}, coordinates=_coordinates(), reviewed_digest=_DIGEST)
    else:
        assert reconcile._review_locked_plan(
            {}, {}, coordinates=_coordinates(), reviewed_digest=_DIGEST
        ) is plan_by_field


@pytest.mark.asyncio
async def test_reconcile_write_builds_exact_store_command(monkeypatch) -> None:
    apply_rows = AsyncMock()
    monkeypatch.setattr(reconcile, "apply_legacy_v3_reconcile_rows", apply_rows)
    reconciliation_id = await reconcile._write_reconciliation(
        _Session(),
        schema_name="mrf",
        coordinates=_coordinates(),
        source_file_import_id=_SOURCE_IMPORT_ID,
        plan=_ready_plan(),
    )
    write = apply_rows.await_args.args[1]
    assert write.reconciliation_id == reconciliation_id
    assert write.event_high_water_mark == 7
    assert write.marker["snapshot_id"] == _SNAPSHOT_ID


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ("ready", "already_reconciled"))
async def test_reconcile_transaction_handles_new_and_existing_fence(monkeypatch, status: str) -> None:
    monkeypatch.setattr(reconcile, "db", _database(_Session()))
    monkeypatch.setattr(reconcile, "resolve_ptg2_schema", lambda: "mrf")
    monkeypatch.setattr(
        reconcile,
        "_locked_observation",
        AsyncMock(return_value=(_SOURCE_IMPORT_ID, {"locked": True})),
    )
    plan_by_field = {"status": status}
    monkeypatch.setattr(
        reconcile,
        "_review_locked_plan",
        lambda *_args, **_kwargs: plan_by_field,
    )
    writer = AsyncMock(return_value="reconciliation-id")
    monkeypatch.setattr(reconcile, "_write_reconciliation", writer)
    returned = await reconcile._apply_reconcile_transaction(
        _coordinates(), reviewed_digest=_DIGEST, operational_evidence={}
    )
    assert returned[:2] == (plan_by_field, {"locked": True})
    assert returned[2] == (None if status == "already_reconciled" else "reconciliation-id")
    assert writer.await_count == (0 if status == "already_reconciled" else 1)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("status", "postcheck_absence", "expected_state"),
    (("already_reconciled", None, "already_reconciled"), ("ready", False, "applied_postcheck_red"), ("ready", True, "applied")),
)
async def test_reconcile_public_execution_reports_terminal_state(
    monkeypatch, status: str, postcheck_absence, expected_state: str
) -> None:
    initial_observation_by_field = {
        "outer_runs": ["outer"],
        "event_rows": ["event"],
    }
    locked_observation_by_field = {
        "outer_runs": ["locked-outer"],
        "event_rows": ["locked-event"],
    }
    monkeypatch.setattr(
        reconcile,
        "_database_observation",
        AsyncMock(return_value=(initial_observation_by_field, True)),
    )
    evidence_rows = [{"exact_external_absence": True}]
    if postcheck_absence is not None:
        evidence_rows.append({"exact_external_absence": postcheck_absence})
    monkeypatch.setattr(
        reconcile,
        "load_exact_operational_absence",
        AsyncMock(side_effect=evidence_rows),
    )
    plan_by_field = {"status": status, "reason_codes": ["existing"]}
    monkeypatch.setattr(
        reconcile,
        "_apply_reconcile_transaction",
        AsyncMock(
            return_value=(
                plan_by_field,
                locked_observation_by_field,
                "reconciliation-id",
            )
        ),
    )
    report = await reconcile.reconcile_legacy_v3_metadata(
        snapshot_id=_SNAPSHOT_ID,
        internal_run_id=_INTERNAL_RUN_ID,
        outer_run_id=_OUTER_RUN_ID,
        expected_plan_digest=_DIGEST,
    )
    assert report["state"] == expected_state
    if expected_state == "applied_postcheck_red":
        assert "postcommit_external_identity_present" in report["reason_codes"]


@pytest.mark.asyncio
async def test_store_relation_lock_includes_present_attachments(monkeypatch) -> None:
    session = _Session()
    first_table = store.ATTEMPT_ATTACHMENTS[0].table_name
    monkeypatch.setattr(
        store,
        "has_relation",
        AsyncMock(side_effect=lambda _session, _schema, table_name: table_name == first_table),
    )
    await store.lock_legacy_v3_reconcile_relations(
        session, schema_name="mrf", control_schema_name="source_authority"
    )
    statement = session.calls[0][0]
    assert statement.startswith("LOCK TABLE")
    assert f'"mrf"."{first_table}"' in statement
    assert '"source_authority"."source_file_import"' in statement
    assert statement.count(f'"mrf"."{first_table}"') == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("writer_name", "error_text"),
    (
        ("_update_snapshot_row", "snapshot CAS changed"),
        ("_update_internal_run_row", "internal-run CAS changed"),
        ("_insert_reconcile_audit", "audit insert changed"),
    ),
)
@pytest.mark.parametrize("rowcount", (0, 1))
async def test_store_cas_requires_exactly_one_changed_row(writer_name, error_text, rowcount) -> None:
    session = _Session(_QueryResult(rowcount=rowcount))
    writer = getattr(store, writer_name)
    if rowcount == 0:
        with pytest.raises(RuntimeError, match=error_text):
            await writer(session, _reconcile_write())
    else:
        await writer(session, _reconcile_write())
        parameters = session.calls[0][1]
        assert parameters["internal_run_id"] == _INTERNAL_RUN_ID
        if writer_name == "_insert_reconcile_audit":
            assert json.loads(parameters["marker"])["observed_at"].startswith("2026-08-01")
