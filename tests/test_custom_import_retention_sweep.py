# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""The retention sweep commits, rolls back or defers each family on its own."""

import asyncio
import json
import sys
import types
from types import SimpleNamespace

import pytest
from sqlalchemy.exc import DBAPIError

from process.custom_import import retention_sweep
from process.custom_import.build_scratch import (
    BuildScratchCleanupReceipt,
    BuildScratchCleanupRefused,
)
from process.custom_import.publication import PublicationConflict
from process.custom_import.retirement import (
    SnapshotRetirementReceipt,
    SnapshotRetirementRefused,
)


class _Transaction:
    def __init__(self, log):
        self.log = log

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        self.log.append("rollback" if exc_type else "commit")
        return False


class _Session:
    def __init__(self, log):
        self.log = log

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc_info):
        return False

    def begin(self):
        return _Transaction(self.log)

    async def execute(self, statement):
        self.log.append(str(statement))


class _OtherRefusal(ValueError):
    pass


def _sessions(events):
    return lambda: _Session(events)


def _db_error(sqlstate):
    return DBAPIError("statement", {}, SimpleNamespace(sqlstate=sqlstate))


async def _retired(session, family_id):
    return SnapshotRetirementReceipt(family_id, "superseded", 7, False)


@pytest.mark.parametrize(
    ("execute", "state", "outcome"), ((True, "retired", "commit"), (False, "would_retire", "rollback"))
)
async def test_completed_retirement_commits_only_when_executing(execute, state, outcome):
    events = []

    record = await retention_sweep.retire_one(
        _sessions(events), 5, _retired, (SnapshotRetirementRefused,), execute=execute
    )

    assert record == {
        "operation": "retire",
        "state": state,
        "family_id": 5,
        "reason": "superseded",
        "retained_bytes": 7,
        "replayed": False,
    }
    assert events == ["SET LOCAL statement_timeout='5s'", "SET LOCAL lock_timeout='100ms'", outcome]


@pytest.mark.parametrize(
    ("error", "reason"),
    (
        (
            SnapshotRetirementRefused("target is current or retained for rollback"),
            "target is current or retained for rollback",
        ),
        (_OtherRefusal("control plane keeps it"), "control plane keeps it"),
        (_db_error("55P03"), "retirement bound elapsed"),
        (_db_error("57014"), "retirement bound elapsed"),
        (TimeoutError(), "retirement bound elapsed"),
    ),
)
async def test_refused_or_busy_family_is_deferred_and_rolled_back(error, reason):
    events = []

    async def refuse(session, family_id):
        raise error

    record = await retention_sweep.retire_one(
        _sessions(events), 3, refuse, (SnapshotRetirementRefused, _OtherRefusal), execute=True
    )

    assert record == {"operation": "retire", "state": "deferred", "family_id": 3, "reason": reason}
    assert events[-1] == "rollback"


async def test_unexpected_database_error_stops_the_sweep():
    async def broken(session, family_id):
        raise _db_error("42501")

    with pytest.raises(DBAPIError):
        await retention_sweep.retire_one(_sessions([]), 3, broken, (SnapshotRetirementRefused,), execute=True)


async def test_overall_bound_defers_a_stalled_family(monkeypatch):
    monkeypatch.setattr(retention_sweep.retirement, "MAX_RETIREMENT_SECONDS", 0.01)

    async def stalled(session, family_id):
        await asyncio.sleep(1)

    record = await retention_sweep.retire_one(_sessions([]), 4, stalled, (SnapshotRetirementRefused,), execute=True)

    assert record["state"] == "deferred"


async def test_sweep_reports_every_retained_family_in_order(monkeypatch):
    async def families(sessions, *, sealed=False):
        return [3] if sealed else [2, 3]

    async def retire(session, family_id):
        if family_id == 3:
            raise SnapshotRetirementRefused("target is current or retained for rollback")
        return SnapshotRetirementReceipt(family_id, "no_change", 1, False)

    async def trim(session, family_id, *, engine_id):
        assert engine_id == "synthetic_engine"
        return BuildScratchCleanupReceipt(family_id, 9, 1, False)

    monkeypatch.setattr(retention_sweep, "retained_family_ids", families)
    monkeypatch.setattr(retention_sweep.build_scratch, "trim_build_scratch", trim)
    lines = []

    outcomes = await retention_sweep.sweep(
        _sessions([]),
        retire,
        (SnapshotRetirementRefused,),
        engine_id="synthetic_engine",
        execute=True,
        emit=lines.append,
    )

    assert [(outcome["operation"], outcome["state"]) for outcome in outcomes] == [
        ("retire", "retired"),
        ("retire", "deferred"),
        ("trim", "trimmed"),
    ]
    assert [json.loads(line)["family_id"] for line in lines] == [2, 3, 3]


@pytest.mark.parametrize(
    ("execute", "state", "outcome"), ((True, "trimmed", "commit"), (False, "would_trim", "rollback"))
)
async def test_completed_trim_commits_only_when_executing(monkeypatch, execute, state, outcome):
    events = []

    async def trim(session, family_id, *, engine_id):
        assert engine_id == "synthetic_engine"
        return BuildScratchCleanupReceipt(family_id, 9, 1, True)

    monkeypatch.setattr(retention_sweep.build_scratch, "trim_build_scratch", trim)

    trim_outcome = await retention_sweep.trim_one(_sessions(events), 5, engine_id="synthetic_engine", execute=execute)

    assert trim_outcome == {
        "operation": "trim",
        "state": state,
        "family_id": 5,
        "retained_bytes": 9,
        "remaining_bytes": 1,
        "replayed": True,
    }
    assert events == ["SET LOCAL statement_timeout='5s'", "SET LOCAL lock_timeout='100ms'", outcome]


@pytest.mark.parametrize(
    ("error", "reason"),
    (
        (
            BuildScratchCleanupRefused("build scratch cleanup requires full automated post-seal verification"),
            "build scratch cleanup requires full automated post-seal verification",
        ),
        (PublicationConflict("target generation is not sealed"), "target generation is not sealed"),
        (_db_error("55P03"), "trim bound elapsed"),
        (_db_error("57014"), "trim bound elapsed"),
        (TimeoutError(), "trim bound elapsed"),
    ),
)
async def test_refused_or_busy_trim_is_deferred_and_rolled_back(monkeypatch, error, reason):
    events = []

    async def refuse(session, family_id, *, engine_id):
        raise error

    monkeypatch.setattr(retention_sweep.build_scratch, "trim_build_scratch", refuse)

    outcome = await retention_sweep.trim_one(_sessions(events), 3, engine_id="synthetic_engine", execute=True)

    assert outcome == {"operation": "trim", "state": "deferred", "family_id": 3, "reason": reason}
    assert events[-1] == "rollback"


async def test_unexpected_trim_error_stops_the_sweep(monkeypatch):
    async def broken(session, family_id, *, engine_id):
        raise _db_error("42501")

    monkeypatch.setattr(retention_sweep.build_scratch, "trim_build_scratch", broken)

    with pytest.raises(DBAPIError):
        await retention_sweep.trim_one(_sessions([]), 3, engine_id="synthetic_engine", execute=True)


async def test_trim_bound_defers_a_stalled_family(monkeypatch):
    monkeypatch.setattr(retention_sweep.build_scratch, "MAX_CLEANUP_SECONDS", 0.01)

    async def stalled(session, family_id, *, engine_id):
        await asyncio.sleep(1)

    monkeypatch.setattr(retention_sweep.build_scratch, "trim_build_scratch", stalled)

    outcome = await retention_sweep.trim_one(_sessions([]), 4, engine_id="synthetic_engine", execute=True)

    assert outcome["state"] == "deferred"


async def test_dry_run_never_trims_a_family_it_would_retire(monkeypatch):
    async def families(sessions, *, sealed=False):
        return [2, 3]

    async def retire(session, family_id):
        if family_id == 3:
            raise SnapshotRetirementRefused("target is current or retained for rollback")
        return SnapshotRetirementReceipt(family_id, "superseded", 1, False)

    trimmed_ids = []

    async def trim(session, family_id, *, engine_id):
        trimmed_ids.append(family_id)
        return BuildScratchCleanupReceipt(family_id, 9, 1, False)

    monkeypatch.setattr(retention_sweep, "retained_family_ids", families)
    monkeypatch.setattr(retention_sweep.build_scratch, "trim_build_scratch", trim)

    outcomes = await retention_sweep.sweep(
        _sessions([]), retire, (SnapshotRetirementRefused,), engine_id="synthetic_engine", execute=False, emit=print
    )

    assert trimmed_ids == [3]
    assert [(outcome["operation"], outcome["state"], outcome["family_id"]) for outcome in outcomes] == [
        ("retire", "would_retire", 2),
        ("retire", "deferred", 3),
        ("trim", "would_trim", 3),
    ]


async def test_control_binding_passes_live_identity_and_both_refusal_types(monkeypatch):
    module = types.ModuleType("synthetic_control_retirement")

    class Refused(ValueError):
        pass

    calls = []

    async def retire_exact_snapshot_family(session, family_id, *, retire_snapshot_family, context):
        calls.append((session, family_id, retire_snapshot_family, context))
        return "receipt"

    module.HostedSnapshotRetirementRefused = Refused
    module.SnapshotRetirementOwnerContext = lambda **fields: fields
    module.retire_exact_snapshot_family = retire_exact_snapshot_family
    monkeypatch.setitem(sys.modules, module.__name__, module)

    class Session:
        async def execute(self, statement):
            return SimpleNamespace(one=lambda: ("synthetic_db", "synthetic_owner", "42"))

    retire, refusals = retention_sweep.control_plane_retire("synthetic_engine", module.__name__)
    session = Session()

    assert await retire(session, 9) == "receipt"
    assert refusals == (SnapshotRetirementRefused, Refused)
    [(called_session, family_id, operation, context)] = calls
    assert called_session is session and family_id == 9
    assert operation is retention_sweep.retirement.retire_snapshot_family
    assert context == {
        "engine_id": "synthetic_engine",
        "engine_namespace": str(retention_sweep.CustomImportSnapshotFamily.__table__.schema),
        "expected_database": "synthetic_db",
        "expected_system_identifier": "42",
        "expected_owner": "synthetic_owner",
    }
