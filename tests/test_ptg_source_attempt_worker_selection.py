"""Worker selection and response identity contracts."""

import sqlite3
from contextlib import closing
from types import SimpleNamespace

import pytest

from api import control_workers
from process.ptg_parts import ptg_source_attempt_actions as actions
from tests.test_ptg_source_attempt_coverage_edges import MappingResult, transaction_for

RUN_ID = "synthetic-run-coverage"


def test_worker_selection_and_response_preserve_run_identity():
    selection = control_workers._worker_action_selection(
        "",
        [
            control_workers._BY_QUEUE["arq:ClaimsPricing"],
            control_workers._BY_QUEUE["arq:ClaimsPricing_finish"],
        ],
    )
    assert selection.request_importer is None
    assert selection.allowed_importers == frozenset({"claims-pricing", "claims-procedures"})
    assert selection.allowed_roles == frozenset({"start", "finish"})
    response = control_workers._failed_worker_admission(
        {"run_id": RUN_ID},
        "synthetic failure",
    )
    assert response["contract_id"] == control_workers.WORKER_ENSURE_RUN_IDENTITY_CONTRACT
    assert response["run_id"] == RUN_ID
    assert control_workers._worker_ensure_response({}, status="inactive", items=[])["status"] == "inactive"


@pytest.mark.parametrize(
    ("total_chunks", "accepted"),
    ((5, True), (0, True), (None, False), (False, False), (-1, False)),
)
def test_running_chunked_import_selects_finish_worker(total_chunks, accepted):
    outer_run_by_field = {"importer": "provider-quality", "status": "running"}
    if total_chunks is not None:
        outer_run_by_field["metrics"] = {"total_chunks": total_chunks}
    selection = actions.PTGWorkerActionSelection(
        request_importer="provider-quality",
        allowed_importers=frozenset({"provider-quality"}),
        allowed_roles=frozenset({"finish"}),
    )
    if accepted:
        actions._validate_worker_selection(outer_run_by_field, selection)
    else:
        with pytest.raises(actions.PTGSourceAttemptIdentityError):
            actions._validate_worker_selection(outer_run_by_field, selection)


@pytest.mark.parametrize(
    ("importer", "phase", "role"),
    (
        ("mrf", "mrf finalizing", "finish"),
        ("mrf", "mrf finalizing parser queue drained", "finish"),
        ("mrf", "mrf parsing", "start"),
        ("mrf", "mrf finalizing-unrecognized", "start"),
        ("mrf", None, "start"),
        ("ptg", "mrf finalizing", "start"),
        ("provider-quality", "mrf finalizing", "start"),
    ),
)
def test_worker_selection_uses_the_durable_mrf_finalization_phase(importer, phase, role):
    outer_run_by_field = {"importer": importer, "status": "running", "phase_detail": phase}
    for selected_role in ("start", "finish"):
        selection = actions.PTGWorkerActionSelection(
            request_importer=importer,
            allowed_importers=frozenset({importer}),
            allowed_roles=frozenset({selected_role}),
        )
        if selected_role == role:
            actions._validate_worker_selection(outer_run_by_field, selection)
        else:
            with pytest.raises(actions.PTGSourceAttemptIdentityError):
                actions._validate_worker_selection(outer_run_by_field, selection)


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ("mrf finalizing", "mrf finalizing parser queue drained"))
async def test_mrf_finish_admission_reads_the_persisted_phase(monkeypatch, phase):
    with closing(sqlite3.connect(":memory:")) as connection:
        connection.row_factory = sqlite3.Row
        connection.executescript("""
            ATTACH DATABASE ':memory:' AS mrf;
            CREATE TABLE mrf.import_run (
                run_id TEXT PRIMARY KEY, importer TEXT, status TEXT, phase_detail TEXT,
                params TEXT, metrics TEXT, source_file_import_id TEXT, import_id TEXT,
                retry_of_run_id TEXT, heartbeat_at TEXT
            );
        """)
        connection.execute(
            "INSERT INTO mrf.import_run (run_id,importer,status,phase_detail) VALUES (?,?,?,?)",
            (RUN_ID, "mrf", "running", phase),
        )

        async def execute(statement, parameters):
            row = connection.execute(str(statement), parameters).fetchone()
            return MappingResult(dict(row) if row else None)

        monkeypatch.setattr(
            actions, "db", SimpleNamespace(transaction=transaction_for(SimpleNamespace(execute=execute)))
        )
        monkeypatch.setattr(actions, "resolve_ptg2_schema", lambda: "mrf")
        admitted = await actions.admit_existing_outer_run_action(
            run_id=RUN_ID,
            event_kind="ensure_admitted",
            worker_selection=actions.PTGWorkerActionSelection(
                request_importer="mrf",
                allowed_importers=frozenset({"mrf"}),
                allowed_roles=frozenset({"finish"}),
            ),
        )
        assert admitted["phase_detail"] == phase
