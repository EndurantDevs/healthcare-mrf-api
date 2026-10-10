# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Host control and custody regressions; SQL observations do not qualify native publication."""

import asyncio
import json
from contextlib import asynccontextmanager
from copy import deepcopy
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from sqlalchemy import Column, Identity, Index, Integer, MetaData, Table, Text, func

from process import reference_family_archive as archive
from process.ext.utils import make_class
from tests.test_model_archive_validation_refusals import _retained_inventory
from tests.test_reference_family_archive import (
    _incumbent,
    _manifest,
    _ownership,
    _serving_generation,
    _validation_receipt,
)
from tests.test_reference_family_source_copy import (
    _nucc_abandoned_run,
    _nucc_completed_handoff,
    _nucc_stage_receipt,
)
from tests.test_reference_handoff_boundary import _cleanup_inputs


class _Rows(list):
    def mappings(self):
        return self

    def all(self):
        return list(self)

    def one(self):
        assert len(self) == 1
        return self[0]

    def one_or_none(self):
        assert len(self) <= 1
        return self[0] if self else None


def _handoff_observations():
    stage = _nucc_stage_receipt()
    stage["source_contract_sha256"] = archive._nucc_source_contract(stage, {})
    stage["stage_sha256"] = archive.nucc_native_digest(
        {key: value for key, value in stage.items() if key != "stage_sha256"}
    )
    handoff = _nucc_completed_handoff(stage)
    run = _nucc_abandoned_run(stage, "finalizing")
    run.update(finished_at=None, phase_detail=archive.NUCC_HANDOFF_PHASE)
    run["metrics"]["nucc_handoff"] = handoff
    return SimpleNamespace(
        handoff=handoff,
        run=run,
        stage=deepcopy(handoff["stage"]),
        database_oid=41,
        history_oid=42,
        marker=json.dumps({key: handoff[key] for key in ("contract", "run_id", "attempt_id", "handoff_sha256")}),
    )


def _handoff_execute(observed, statement, parameters=None):
    sql = str(statement)
    if sql.startswith("SELECT node_id,engine,importer"):
        return _Rows([] if observed.run is None else [observed.run])
    if sql.startswith(("LOCK TABLE", "SET LOCAL", "SELECT set_config")):
        return _Rows()
    if sql.startswith("SELECT oid::bigint AS relation_oid"):
        return _Rows([{key: value for key, value in observed.stage.items() if key not in {"table_name", "indexes"}}])
    if sql.startswith("SELECT indexrelid::bigint"):
        return _Rows(observed.stage["indexes"])
    raise AssertionError("unexpected query: " + sql)


def _handoff_scalar(observed, statement, parameters=None):
    sql = str(statement)
    if sql.startswith("SELECT oid::bigint FROM pg_catalog.pg_database"):
        return observed.database_oid
    if sql.startswith("SELECT relation.oid FROM pg_catalog.pg_class"):
        assert parameters == {"schema_name": "mrf", "table_name": "import_run"}
        return observed.history_oid
    if sql == "SHOW search_path":
        return "public"
    if sql.startswith(("SELECT pg_catalog.obj_description", "SELECT obj_description")):
        assert parameters == {"oid": observed.handoff["stage"]["relation_oid"]}
        return observed.marker
    if sql.startswith("SELECT session_user=current_user AND builder.rolcanlogin"):
        return observed.is_builder_safe
    raise AssertionError("unexpected scalar: " + sql)


def _handoff_session(observed):
    return SimpleNamespace(
        in_transaction=lambda: True,
        execute=AsyncMock(side_effect=lambda *args: _handoff_execute(observed, *args)),
        scalar=AsyncMock(side_effect=lambda *args: _handoff_scalar(observed, *args)),
    )


def _publication(handoff):
    incumbent = handoff["incumbent"]
    validation_by_field = {"contract": "nucc-indexed-set.v1", "row_count": 1, "csv_upper_bound": 128}
    return archive.validate_nucc_native_publication(
        {
            "contract": archive.NUCC_IMMUTABLE_PUBLICATION_CONTRACT,
            "handoff": handoff,
            "sealed_owner_oid": 50,
            "validation": validation_by_field,
            "validation_sha256": archive.nucc_native_digest(validation_by_field),
            "result_generation": {
                **incumbent,
                "local_generation": incumbent["local_generation"] + 1,
                "relation_oids": [handoff["stage"]["relation_oid"]],
                "serving_generation": {
                    "origin_lineage_id": incumbent["local_lineage_id"],
                    "origin_generation": 1,
                    "published_at": "2026-10-01T00:01:00Z",
                },
            },
        }
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", (None, "finished", "phase", "missing", "stage", "marker"))
async def test_handoff_authenticates_persisted_control_and_exact_heap_before_returning(fault):
    observed = _handoff_observations()
    session = _handoff_session(observed)
    changes_by_fault = {
        "finished": (observed.run, "finished_at", "2026-10-01T00:02:00Z"),
        "phase": (observed.run, "phase_detail", "other"),
        "missing": (observed.run["metrics"], "nucc_handoff", None),
    }
    if fault in changes_by_fault:
        record, field, field_value = changes_by_fault[fault]
        record[field] = field_value
    elif fault == "stage":
        observed.stage["relfilenode"] += 1
    elif fault == "marker":
        observed.marker = None
    if fault:
        with pytest.raises(
            archive.ReferenceFamilyArchiveError,
            match="source or location differs|stage identity differs|stage marker differs",
        ):
            await archive.require_nucc_native_handoff(session, observed.handoff)
    else:
        assert await archive.require_nucc_native_handoff(session, observed.handoff) == observed.handoff
    queries = [str(call.args[0]) for call in session.execute.await_args_list]
    locks = [sql for sql in queries if sql.startswith("LOCK TABLE")]
    assert len(locks) == int(fault not in {"finished", "phase", "missing"})
    if locks:
        assert locks == [f'LOCK TABLE ONLY "mrf"."{observed.stage["table_name"]}" IN ACCESS EXCLUSIVE MODE NOWAIT']
    assert not any(sql.startswith(("INSERT", "UPDATE", "DELETE", "DROP")) for sql in queries)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,value",
    (
        ("engine", "foreign"),
        ("importer", "other"),
        ("status", "failed"),
        ("error", "failed"),
        ("progress", None),
        ("params", []),
        ("metrics", []),
    ),
)
async def test_handoff_rejects_foreign_or_malformed_locked_attempt_before_catalog_reads(field, value):
    observed = _handoff_observations()
    observed.run[field] = value
    session = _handoff_session(observed)
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="native attempt differs"):
        await archive.require_nucc_native_handoff(session, observed.handoff)
    assert session.execute.await_count == 1
    session.scalar.assert_not_awaited()
    query, parameters = session.execute.await_args.args
    assert "FOR UPDATE NOWAIT" in str(query) and "octet_length(metrics::text)<=393216" in str(query)
    assert parameters == {"run_id": observed.handoff["run_id"]}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault", ("missing", "attempt", "started", "test", "test_mode", "node", "source", "database", "history")
)
async def test_handoff_readback_cannot_rebind_attempt_source_or_database(fault):
    observed = _handoff_observations()
    changes_by_fault = {
        "attempt": (observed.run["progress"], "attempt_id", "other"),
        "started": (observed.run["progress"], "attempt_started_at", "other"),
        "test": (observed.run["params"], "test", True),
        "test_mode": (observed.run["params"], "test_mode", True),
        "source": (observed.run["params"], "source", True),
        "node": (observed.run, "node_id", "other"),
    }
    if fault == "missing":
        observed.run = None
    elif fault in changes_by_fault:
        record, field, field_value = changes_by_fault[fault]
        record[field] = field_value
    else:
        field = "database_oid" if fault == "database" else "history_oid"
        setattr(observed, field, getattr(observed, field) + 1)
    session = _handoff_session(observed)
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="attempt differs|source or location differs"):
        await archive.read_nucc_native_handoff(session, observed.handoff)
    assert session.execute.await_count == 1
    assert not any("obj_description" in str(call.args[0]) for call in session.scalar.await_args_list)


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ("running", "finalizing", "succeeded", "canceling", "canceled", "cancelled"))
@pytest.mark.parametrize("is_persisted", (False, True))
async def test_commit_readback_distinguishes_absent_handoff_from_committed_terminal_history(status, is_persisted):
    observed = _handoff_observations()
    observed.run["status"] = status
    if not is_persisted:
        observed.run["metrics"] = None
    session = _handoff_session(observed)
    if status == "running" and is_persisted:
        with pytest.raises(archive.ReferenceFamilyArchiveError, match="committed handoff differs"):
            await archive.read_nucc_native_handoff(session, observed.handoff)
    else:
        assert await archive.read_nucc_native_handoff(session, observed.handoff) == (
            observed.handoff if is_persisted else None
        )
    assert session.execute.await_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", (None, "absent", "status", "unfinished", "different-handoff"))
async def test_publication_readback_requires_exact_committed_success_not_current_payload(fault):
    observed = _handoff_observations()
    observed.run.update(status="succeeded", finished_at="2026-10-01T00:02:00Z")
    receipt = _publication(observed.handoff)
    observed.run["metrics"]["nucc_native_publication"] = receipt
    if fault == "absent":
        observed.run["metrics"].pop("nucc_native_publication")
    elif fault == "status":
        observed.run["status"] = "canceled"
    elif fault == "unfinished":
        observed.run["finished_at"] = None
    elif fault == "different-handoff":
        different = deepcopy(observed.handoff)
        different["stage"]["indexes"][0]["relfilenode"] += 1
        different["handoff_sha256"] = archive.nucc_native_digest(
            {key: value for key, value in different.items() if key != "handoff_sha256"}
        )
        observed.run["metrics"]["nucc_native_publication"] = _publication(different)
    session = _handoff_session(observed)
    if fault in {"status", "unfinished", "different-handoff"}:
        with pytest.raises(archive.ReferenceFamilyArchiveError, match="committed receipt differs"):
            await archive.read_nucc_native_publication(session, observed.handoff)
    else:
        assert await archive.read_nucc_native_publication(session, observed.handoff) == (
            None if fault == "absent" else receipt
        )
    assert session.execute.await_count == 1
    assert all("nucc_taxonomy" not in str(call.args[0]) for call in session.scalar.await_args_list)


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ("outer-cancel", "inner-cancel", "query-failure"))
async def test_commit_reconciliation_finishes_its_owned_readback_before_classifying_cancellation(fault):
    observed = _handoff_observations()
    session = _handoff_session(observed)
    entered, release = asyncio.Event(), asyncio.Event()
    exits = []
    failure = asyncio.CancelledError() if fault == "inner-cancel" else RuntimeError("catalog unavailable")

    async def execute(statement, parameters=None):
        entered.set()
        await release.wait()
        if fault != "outer-cancel":
            raise failure
        return _handoff_execute(observed, statement, parameters)

    @asynccontextmanager
    async def transaction():
        try:
            yield session
        finally:
            exits.append("closed")

    session.execute.side_effect = execute
    pending = asyncio.create_task(
        archive.reconcile_nucc_native_handoff(SimpleNamespace(transaction=transaction), observed.handoff)
    )
    try:
        await asyncio.wait_for(entered.wait(), timeout=1)
        if fault == "outer-cancel":
            pending.cancel()
            await asyncio.sleep(0)
            assert not pending.done() and exits == []
        release.set()
        if fault == "outer-cancel":
            assert await pending == observed.handoff
        else:
            with pytest.raises(type(failure)) as caught:
                await pending
            if fault == "query-failure":
                assert caught.value is failure
        assert exits == ["closed"]
    finally:
        release.set()
        await asyncio.gather(pending, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", ("synthetic", None, "other"))
async def test_native_completion_uses_one_exact_attempt_cas_and_preserves_unrelated_metrics(changed):
    receipt = _publication(_handoff_observations().handoff)
    session = SimpleNamespace(scalar=AsyncMock(return_value=changed))
    if changed != receipt["handoff"]["run_id"]:
        with pytest.raises(archive.ReferenceFamilyArchiveError, match="success fence changed"):
            await archive._finish_nucc_native_attempt(session, receipt)
    else:
        await archive._finish_nucc_native_attempt(session, receipt)
    query, parameters = session.scalar.await_args.args
    sql = str(query)
    assert "metrics=(metrics::jsonb||jsonb_build_object" in sql and "progress=(progress::jsonb||" in sql
    for predicate in (
        "status='finalizing'",
        "finished_at IS NULL",
        "error IS NULL",
        "progress->>'attempt_id'=:attempt_id",
        "progress->>'attempt_started_at'=:attempt_started_at",
        "metrics::jsonb->'nucc_handoff'=CAST(:handoff AS jsonb)",
    ):
        assert predicate in sql
    assert json.loads(parameters["receipt"]) == receipt
    assert json.loads(parameters["handoff"]) == receipt["handoff"]
    assert parameters["rows"] == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", (None, "identity", "empty", "oversized"))
async def test_published_indexes_are_renamed_only_for_exact_heap_without_rebuilding(fault):
    indexes = [(17, "nucc_stage_primary"), (18, "nucc_stage_lookup")]
    if fault == "empty":
        indexes = []
    elif fault == "oversized":
        indexes = [(oid, "nucc_index_" + str(oid)) for oid in range(1, 18)]
    session = SimpleNamespace(
        in_transaction=lambda: True,
        scalar=AsyncMock(return_value=44 if fault == "identity" else 43),
        execute=AsyncMock(return_value=_Rows(indexes)),
    )
    if fault:
        with pytest.raises(archive.ReferenceFamilyArchiveError, match="identity differs|index inventory differs"):
            await archive.rename_nucc_published_indexes(session, schema_name="mrf", expected_relation_oid=43)
    else:
        await archive.rename_nucc_published_indexes(session, schema_name="mrf", expected_relation_oid=43)
    statements = [str(call.args[0]) for call in session.execute.await_args_list]
    assert statements[1:] == (
        []
        if fault
        else [
            'ALTER INDEX "mrf"."nucc_stage_primary" RENAME TO "nucc_published_idx_11"',
            'ALTER INDEX "mrf"."nucc_stage_lookup" RENAME TO "nucc_published_idx_12"',
        ]
    )
    assert not any(sql.startswith(("DROP", "CREATE")) for sql in statements)


@pytest.mark.asyncio
@pytest.mark.parametrize("copied", (1, 1024, 0, 1025, True, None))
async def test_source_model_copy_uses_exact_columns_order_and_shared_remaining_deadline(copied):
    copier = AsyncMock(return_value=copied)
    source_copy = archive.ReferenceFamilySourceCopy(copier, 1024, 30)
    deadline = asyncio.get_running_loop().time() + 10
    owner = _ownership("nucc", (("nucc_taxonomy", 43),))
    session = object()
    if type(copied) is int and 0 < copied <= 1024:
        await archive._copy_nucc_source_model(session, SimpleNamespace(schema_name="mrf"), owner, source_copy, deadline)
    else:
        with pytest.raises(archive.ReferenceFamilyArchiveError, match="COPY accounting is invalid"):
            await archive._copy_nucc_source_model(
                session, SimpleNamespace(schema_name="mrf"), owner, source_copy, deadline
            )
    arguments = copier.await_args
    assert arguments.args[0] is session
    columns = tuple(archive.models.NUCCTaxonomy.__table__.columns.keys())
    assert (
        arguments.args[1]
        == "SELECT "
        + ", ".join('"' + name + '"' for name in columns)
        + ' FROM "mrf"."nucc_taxonomy" ORDER BY code COLLATE "C"'
    )
    assert arguments.kwargs["columns"] == columns and arguments.kwargs["schema_name"] == owner.schema_name
    assert arguments.kwargs["table_name"] == "nucc_taxonomy" and arguments.kwargs["max_bytes"] == 1024
    assert 0 < arguments.kwargs["timeout"] <= 10


@pytest.mark.asyncio
async def test_source_copy_expired_budget_never_opens_the_native_callback():
    copier = AsyncMock()
    with pytest.raises(TimeoutError, match="deadline expired"):
        await archive._copy_nucc_source_model(
            object(),
            SimpleNamespace(schema_name="mrf"),
            _ownership(),
            archive.ReferenceFamilySourceCopy(copier, 1024, 30),
            asyncio.get_running_loop().time() - 1,
        )
    copier.assert_not_awaited()


def _source_clone_observations(monkeypatch):
    owner = _ownership("nucc", (("nucc_taxonomy", 43),))
    monkeypatch.setattr(archive, "_schema_oid", AsyncMock(return_value=owner.schema_oid))
    monkeypatch.setattr(archive, "_relation_oid", AsyncMock(return_value=43))
    monkeypatch.setattr(archive, "_owned_sequences", AsyncMock(return_value=()))
    monkeypatch.setattr(archive, "_namespace_relations", AsyncMock(return_value=[{"relkind": "r", "oid": 43}]))
    model = archive.models.NUCCTaxonomy
    columns = [
        {"attnum": number, "attname": column.name, "attnotnull": not column.nullable}
        for number, column in enumerate(model.__table__.columns, 1)
    ]
    monkeypatch.setattr(archive.catalog_identity, "_catalog_columns", AsyncMock(return_value=columns))
    monkeypatch.setattr(archive.catalog_identity, "_catalog_constraints", AsyncMock(return_value=[]))
    monkeypatch.setattr(archive.catalog_identity, "_catalog_indexes", AsyncMock(return_value=[]))
    digest = archive.catalog_identity._canonical_digest(
        {"table_name": "nucc_taxonomy", "columns": columns, "constraints": [], "indexes": []}
    )
    table = archive.ReferenceTableReceipt(model.__name__, model.__tablename__, digest, 1)
    manifest = replace(_manifest(), importer_id="nucc", tables=(table,), schema_sha256=archive._schema_digest((table,)))
    census_by_field = {"row_count": 1, "distinct_codes": 1, "invalid": 0, "csv_upper_bound": 128}
    session = SimpleNamespace(
        in_transaction=lambda: True,
        execute=AsyncMock(return_value=_Rows([census_by_field])),
        scalar=AsyncMock(side_effect=[True, 1]),
    )
    return owner, manifest, census_by_field, session


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", (None, "set", "equality", "inventory"))
async def test_source_clone_requires_indexed_set_and_reverse_key_equality(monkeypatch, fault):
    owner, manifest, census, session = _source_clone_observations(monkeypatch)
    if fault == "set":
        census["invalid"] = 1
    elif fault == "equality":
        session.scalar.side_effect = [False]
    elif fault == "inventory":
        monkeypatch.setattr(archive, "_schema_oid", AsyncMock(return_value=owner.schema_oid + 1))
    if fault:
        with pytest.raises(archive.ReferenceFamilyArchiveError, match="set is invalid|clone differs|ownership differs"):
            await archive._validate_nucc_source_clone(
                session, SimpleNamespace(schema_name="mrf", manifest=manifest), owner
            )
        archive.catalog_identity._catalog_columns.assert_not_awaited()
    else:
        prepared = await archive._validate_nucc_source_clone(
            session, SimpleNamespace(schema_name="mrf", manifest=manifest), owner
        )
        assert prepared == archive.ReferenceFamilyPreparedSource(manifest, owner)
        query = str(session.scalar.await_args_list[0].args[0])
        assert "IS DISTINCT FROM (SELECT ROW(" in query and "WHERE NOT EXISTS" in query
        assert '"mrf"."nucc_taxonomy"' in query and owner.schema_name in query
    assert session.scalar.await_count == (2 if fault is None else int(fault == "equality"))


def _native_driver_session(driver):
    connection = SimpleNamespace(get_raw_connection=AsyncMock(return_value=SimpleNamespace(driver_connection=driver)))
    return SimpleNamespace(in_transaction=lambda: True, connection=AsyncMock(return_value=connection))


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", (None, "builder", "model", "count"))
async def test_native_batch_rejoins_precreated_custody_before_one_model_ordered_copy(fault):
    observed = _handoff_observations()
    stage = observed.handoff["precreated_stage"]
    observed.run.update(status="running", metrics={"nucc_native_stage": stage})
    observed.stage = deepcopy(stage["stage"])
    observed.marker = archive._canonical_json(stage).decode("ascii")
    observed.is_builder_safe = fault != "builder"
    session = _handoff_session(observed)
    driver = SimpleNamespace(
        is_in_transaction=lambda: True,
        copy_records_to_table=AsyncMock(return_value="COPY 0" if fault == "count" else "COPY 1"),
        terminate=Mock(),
    )
    session.connection = _native_driver_session(driver).connection
    model = make_class(archive.models.NUCCTaxonomy, "other" if fault == "model" else stage["import_date"])
    nucc_records = [{"code": "101Y00000X", "int_code": 12345}]
    if fault:
        with pytest.raises(
            archive.ReferenceFamilyArchiveError, match="Builder authority differs|COPY model differs|COPY count differs"
        ):
            await archive.copy_nucc_native_batch(session, stage, model, nucc_records)
    else:
        assert await archive.copy_nucc_native_batch(session, stage, model, nucc_records) == 1
    assert driver.copy_records_to_table.await_count == int(fault in {None, "count"})
    if fault in {None, "count"}:
        arguments = driver.copy_records_to_table.await_args
        columns = tuple(archive.models.NUCCTaxonomy.__table__.columns.keys())
        assert arguments.args == (stage["stage"]["table_name"],)
        assert arguments.kwargs["records"] == [tuple(nucc_records[0].get(name) for name in columns)]
        assert arguments.kwargs["columns"] == columns and arguments.kwargs["schema_name"] == "mrf"
    driver.terminate.assert_not_called()
    assert not any(str(call.args[0]).startswith("INSERT") for call in session.execute.await_args_list)


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ("transaction", "missing-method", "non-callable-state"))
async def test_native_copy_requires_the_same_actual_driver_transaction(fault):
    driver = SimpleNamespace(is_in_transaction=lambda: fault != "transaction", copy_records_to_table=AsyncMock())
    if fault == "missing-method":
        driver.copy_records_to_table = None
    elif fault == "non-callable-state":
        driver.is_in_transaction = True
    session = _native_driver_session(driver)
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="acquisition COPY is unavailable"):
        await archive._native_model_copy_driver(session, ("copy_records_to_table",))
    session.connection.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ("columns", "records", "oversized", "timeout", "schema"))
async def test_native_record_batch_refuses_unbounded_or_foreign_scope_before_driver(fault):
    model = archive.models.NUCCTaxonomy
    options_by_field = {
        "schema_name": "mrf",
        "table_name": "nucc_taxonomy",
        "columns": tuple(model.__table__.columns.keys()),
        "records": [tuple(None for _column in model.__table__.columns)],
        "timeout": 30,
    }
    changes_by_fault = {
        "columns": ("columns", ("foreign",)),
        "records": ("records", []),
        "oversized": ("records", [()] * 5001),
        "timeout": ("timeout", float("nan")),
        "schema": ("schema_name", "mrf;other"),
    }
    field, field_value = changes_by_fault[fault]
    options_by_field[field] = field_value
    session = SimpleNamespace(in_transaction=lambda: True, connection=AsyncMock())
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="acquisition COPY scope differs"):
        await archive.native_copy_record_batch(session, model, **options_by_field)
    session.connection.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ("projection-out", "projection-in", "records"))
@pytest.mark.parametrize("failure_type", (asyncio.CancelledError, TimeoutError))
async def test_interrupted_native_protocol_terminates_driver_without_reporting_completion(operation, failure_type):
    failure = failure_type()

    async def capture(_query, *, output, **_options):
        await output(b"bounded native bytes")
        return "COPY 1"

    driver = SimpleNamespace(
        is_in_transaction=lambda: True,
        copy_from_query=AsyncMock(side_effect=capture),
        copy_to_table=AsyncMock(return_value="COPY 1"),
        copy_records_to_table=AsyncMock(return_value="COPY 1"),
        terminate=Mock(),
    )
    method = {
        "projection-out": driver.copy_from_query,
        "projection-in": driver.copy_to_table,
        "records": driver.copy_records_to_table,
    }[operation]
    method.side_effect = failure
    session = _native_driver_session(driver)
    with pytest.raises(failure_type) as caught:
        if operation == "records":
            model = archive.models.NUCCTaxonomy
            await archive.native_copy_record_batch(
                session,
                model,
                schema_name="mrf",
                table_name="nucc_taxonomy",
                columns=tuple(model.__table__.columns.keys()),
                records=[tuple(None for _column in model.__table__.columns)],
            )
        else:
            await archive.native_copy_projection(
                session,
                "SELECT 1",
                schema_name="candidate",
                table_name="rows",
                columns=("value",),
                max_bytes=1024,
                timeout=30,
            )
    assert caught.value is failure
    driver.terminate.assert_called_once_with()
    if operation == "projection-out":
        driver.copy_to_table.assert_not_awaited()
    if operation == "projection-in":
        assert driver.copy_to_table.await_args.kwargs["source"].closed


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", (None, "overflow", "status", "count"))
async def test_projection_drains_output_before_restore_and_refuses_overflow_or_count_drift(fault):
    events = []

    async def capture(query, *, output, **options):
        assert query == "SELECT 1" and options["format"] == "binary" and 0 < options["timeout"] <= 30
        await output(b"1234")
        await output(b"5678")
        await output(b"9")
        events.append("drained")
        return None if fault == "status" else "COPY 1"

    async def restore(table, *, source, **options):
        assert events == ["drained"]
        assert table == "rows" and options["schema_name"] == "candidate"
        assert source.read() == b"123456789"
        events.append("restored")
        return "COPY 0" if fault == "count" else "COPY 1"

    driver = SimpleNamespace(
        copy_from_query=AsyncMock(side_effect=capture), copy_to_table=AsyncMock(side_effect=restore), terminate=Mock()
    )
    if fault:
        with pytest.raises(archive.ReferenceFamilyArchiveError, match="byte cap exceeded|COPY count differs"):
            await archive._copy_native_projection(
                driver, "SELECT 1", "candidate", "rows", ("value",), 4 if fault == "overflow" else 9, 30
            )
    else:
        assert await archive._copy_native_projection(driver, "SELECT 1", "candidate", "rows", ("value",), 9, 30) == 9
    assert events == (["drained"] if fault == "overflow" else ["drained", "restored"])
    assert driver.copy_to_table.await_count == int(fault != "overflow")
    driver.terminate.assert_not_called()


def _indexed_model(*, expression=False):
    table = Table(
        "native_payload", MetaData(), Column("id", Integer, Identity(), primary_key=True), Column("value", Text)
    )
    Index(
        "native_value_index",
        func.lower(table.c.value) if expression else table.c.value,
        postgresql_using="gin",
        postgresql_ops={"value": "gin_trgm_ops"},
    )
    return type("NativePayload", (), {"__table__": table, "__tablename__": table.name})


def test_native_witness_binds_installed_operator_classes_and_removes_identity_allocation():
    model = _indexed_model()
    resolved_by_input = {
        ("btree", None, "id"): ("pg_catalog", "int4_ops"),
        ("gin", "gin_trgm_ops", "value"): ("public", "gin_trgm_ops"),
    }
    assert set(archive._model_index_catalog_inputs(model)) == set(resolved_by_input)
    table, statements = archive._model_index_catalog_plan(model, "mrf", resolved_opclasses=resolved_by_input)
    compiled_statements = [str(statement.compile(dialect=archive.postgresql.dialect())) for statement in statements]
    assert table.schema == "pg_temp" and all(column.identity is None for column in table.columns)
    assert "CREATE TEMPORARY TABLE" in compiled_statements[0] and "ON COMMIT DROP" in compiled_statements[0]
    assert "GENERATED" not in compiled_statements[0] and "CREATE SEQUENCE" not in "\n".join(compiled_statements)
    assert 'USING gin (value "public"."gin_trgm_ops")' in compiled_statements[1]


@pytest.mark.parametrize("compiler", (archive._model_index_catalog_inputs, archive._model_index_catalog_plan))
def test_native_witness_refuses_undeclared_executable_index_expressions(compiler):
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="index expression is unsupported"):
        if compiler is archive._model_index_catalog_plan:
            compiler(_indexed_model(expression=True), "mrf")
        else:
            compiler(_indexed_model(expression=True))


def _publisher_session(proof):
    async def execute(statement, _parameters=None):
        if str(statement).startswith("SELECT owner.oid AS owner_oid"):
            return _Rows([] if proof is None else [proof])
        return _Rows()

    return SimpleNamespace(
        in_transaction=lambda: True,
        execute=AsyncMock(side_effect=execute),
        scalar=AsyncMock(return_value='"native_owner"'),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", (None, "missing", "owner", "caller", "transaction"))
@pytest.mark.parametrize("nucc", (False, True))
async def test_publisher_owner_uses_actual_protected_namespace_and_preserves_refusal_cause(fault, nucc):
    proof_by_field = {"owner_oid": 45, "owner_safe": fault != "owner", "caller_safe": fault != "caller"}
    session = _publisher_session(None if fault == "missing" else proof_by_field)
    session.in_transaction = lambda: fault != "transaction"
    operation = archive._nucc_native_publisher_owner if nucc else archive.protected_publisher_owner
    if fault:
        with pytest.raises(
            archive.ReferenceFamilyArchiveError, match="publisher is unavailable|caller transaction"
        ) as caught:
            await operation(session)
        if fault != "transaction":
            assert caught.value.__cause__ is not None
        if nucc:
            assert isinstance(caught.value.__cause__, archive.ReferenceFamilyArchiveError)
    else:
        assert await operation(session) == 45
    assert session.execute.await_count == int(fault != "transaction")
    if fault != "transaction":
        query = str(session.execute.await_args.args[0])
        assert "namespace.nspname='hp_snapshot_retention'" in query and "session_user=current_user" in query
        assert "pg_catalog.pg_has_role(caller.oid,owner.oid,'USAGE')" in query
        assert "session_replication_role" in query
    session.scalar.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("owner", (45, 46))
async def test_isolated_model_creation_authenticates_owner_before_any_native_ddl(owner):
    session = _publisher_session({"owner_oid": 45, "owner_safe": True, "caller_safe": True})
    arguments_by_field = {
        "create_indexes": False,
        "ordinary_heaps": True,
        "protected_owner_oid": owner,
    }
    spec = archive.reference_family_spec("nucc")
    if owner != 45:
        with pytest.raises(archive.ReferenceFamilyArchiveError, match="publisher owner differs"):
            await archive._create_model_family(session, spec, "candidate", **arguments_by_field)
        session.scalar.assert_not_awaited()
        assert session.execute.await_count == 1
        return
    await archive._create_model_family(session, spec, "candidate", **arguments_by_field)
    statements = [str(call.args[0]) for call in session.execute.await_args_list]
    assert statements[1] == 'CREATE SCHEMA "candidate" AUTHORIZATION "native_owner"'
    assert "CREATE TABLE candidate.nucc_taxonomy" in statements[2]
    assert "code VARCHAR NOT NULL" in statements[2]
    assert not any(phrase in statements[2] for phrase in ("PRIMARY KEY", "FOREIGN KEY", "CREATE TRIGGER"))
    assert len(statements) == 3
    assert session.scalar.await_args.args[1] == {"owner": 45}


def _nucc_columns():
    return [
        {
            "attnum": position,
            "attname": column.name,
            "type": str(column.type.compile(dialect=archive.postgresql.dialect()))
            .lower()
            .replace("varchar", "character varying"),
            "attnotnull": not column.nullable,
            "attgenerated": "",
            "attidentity": "",
            "default_expression": None,
        }
        for position, column in enumerate(archive.models.NUCCTaxonomy.__table__.columns, 1)
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", (None, "type", "order", "missing", "nullability", "generated", "identity", "default"))
async def test_native_candidate_columns_match_installed_model_without_executable_defaults(fault):
    columns = _nucc_columns()
    changes_by_fault = {
        "type": ("type", "text"),
        "nullability": ("attnotnull", not columns[0]["attnotnull"]),
        "generated": ("attgenerated", "s"),
        "identity": ("attidentity", "a"),
        "default": ("default_expression", "nextval('untrusted_sequence')"),
    }
    if fault in changes_by_fault:
        field, field_value = changes_by_fault[fault]
        columns[0][field] = field_value
    elif fault == "order":
        columns.reverse()
    elif fault == "missing":
        columns.pop()
    session = SimpleNamespace(execute=AsyncMock(return_value=_Rows(columns)))
    if fault:
        with pytest.raises(archive.ReferenceFamilyArchiveError, match="candidate columns differ"):
            await archive._require_nucc_native_columns(session, 43)
    else:
        await archive._require_nucc_native_columns(session, 43)
    query, parameters = session.execute.await_args.args
    assert parameters == {"relation_oid": 43}
    assert "NOT attribute.attisdropped" in str(query) and "ORDER BY attribute.attnum" in str(query)
    session.execute.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("cleanup_fails", (False, True))
async def test_catalog_setting_restore_never_masks_the_original_custody_failure(cleanup_fails):
    failure = ValueError("synthetic catalog failure")
    session = SimpleNamespace(
        scalar=AsyncMock(return_value="public"),
        execute=AsyncMock(
            side_effect=[_Rows(), RuntimeError("synthetic restore failure") if cleanup_fails else _Rows()]
        ),
    )
    with pytest.raises(ValueError) as caught:
        async with archive._nucc_catalog_search_path(session):
            raise failure
    assert caught.value is failure
    calls = session.execute.await_args_list
    assert str(calls[0].args[0]) == "SET LOCAL search_path=pg_catalog,pg_temp"
    assert str(calls[1].args[0]) == "SELECT set_config('search_path',:path,true)"
    assert calls[1].args[1] == {"path": "public"}


def _resign_handoff(handoff):
    handoff["handoff_sha256"] = archive.nucc_native_digest(
        {key: value for key, value in handoff.items() if key != "handoff_sha256"}
    )
    return handoff


@pytest.mark.parametrize(
    "field,value",
    (
        ("import_date", "other"),
        ("node_id", ""),
        ("database_oid", True),
        ("row_count", 0),
        ("source_contract_sha256", "g" * 64),
    ),
)
def test_resigned_handoff_cannot_bypass_closed_attempt_identity(field, value):
    handoff = _handoff_observations().handoff
    handoff[field] = value
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="handoff identity differs"):
        archive.validate_nucc_native_handoff(_resign_handoff(handoff))


@pytest.mark.parametrize(
    "fault", ("table", "owner", "empty", "oversized", "shape", "identity", "definition", "duplicate", "order")
)
def test_resigned_handoff_preserves_exact_physical_heap_and_bounded_sorted_indexes(fault):
    handoff = _handoff_observations().handoff
    stage = handoff["stage"]
    first = stage["indexes"][0]
    changes_by_fault = {
        "table": (stage, "table_name", "nucc_taxonomy"),
        "owner": (stage, "owner_oid", True),
        "empty": (stage, "indexes", []),
        "oversized": (stage, "indexes", [first] * 17),
        "identity": (first, "index_oid", True),
        "definition": (first, "definition", "x" * 8193),
        "duplicate": (stage, "indexes", [first, first]),
        "order": (stage, "indexes", [{**first, "index_oid": 49}, first]),
        "shape": (first, "unexpected", "field"),
    }
    record, field, field_value = changes_by_fault[fault]
    record[field] = field_value
    with pytest.raises(
        archive.ReferenceFamilyArchiveError, match="native relation differs|native indexes differ|native indexes repeat"
    ):
        archive.validate_nucc_native_handoff(_resign_handoff(handoff))


@pytest.mark.parametrize(
    "path,value,reason",
    (
        (("contract",), archive.NUCC_PUBLICATION_CONTRACT, "contract differs"),
        (("sealed_owner_oid",), True, "validation differs"),
        (("validation", "row_count"), 2, "validation differs"),
        (("validation", "csv_upper_bound"), 0, "validation differs"),
        (("validation_sha256",), "b" * 64, "validation differs"),
        (("result_generation", "relation_oids"), [99], "generation differs"),
        (("result_generation", "local_generation"), 2, "generation differs"),
        (("result_generation", "serving_generation", "origin_generation"), 2, "generation differs"),
        (
            ("result_generation", "serving_generation", "origin_lineage_id"),
            "550e8400-e29b-41d4-a716-446655440000",
            "generation differs",
        ),
    ),
)
def test_publication_receipt_cannot_rebind_validation_generation_or_storage(path, value, reason):
    receipt = _publication(_handoff_observations().handoff)
    record = receipt
    for field in path[:-1]:
        record = record[field]
    record[path[-1]] = value
    with pytest.raises(archive.ReferenceFamilyArchiveError, match=reason):
        archive.validate_nucc_native_publication(receipt)


def _declared_manifest(importer_id, *, canonical=False):
    """Create closed synthetic metadata from the actual registered model projection."""
    spec = archive.reference_family_spec(importer_id, canonical=canonical)
    tables = tuple(
        archive.ReferenceTableReceipt(model.__name__, model.__tablename__, "a" * 64, 1) for model in spec.model_types
    )
    auxiliary = None
    if importer_id == "mrf":
        auxiliary = (
            archive._canonical_model_receipt()
            if canonical
            else {
                "contract": archive._AUX_NATIVE_SET_CONTRACT,
                "table_name": archive.STAGE_TABLE,
                "archive_name": "address_archive_v2",
                "row_count": 1,
                "schema_sha256": archive.hashlib.sha256(archive._AUX_SCHEMA.encode()).hexdigest(),
                "publication_sha256": "c" * 64,
            }
        )
    manifest = replace(
        _manifest(),
        importer_id=importer_id,
        tables=tables,
        schema_sha256=archive._schema_digest(tables),
        dependencies={name: "b" * 64 for name in spec.dependencies},
        auxiliary=auxiliary,
    )
    return archive.validate_reference_family_manifest(manifest)


@pytest.mark.parametrize(
    "field,value",
    (
        ("copy_rows", None),
        ("max_bytes", True),
        ("max_bytes", 0),
        ("max_bytes", 2**63),
        ("timeout", False),
        ("timeout", 0),
        ("timeout", float("nan")),
        ("timeout", float("inf")),
    ),
)
def test_source_copy_capability_refuses_unbounded_or_non_callable_inputs(field, value):
    arguments_by_field = {"copy_rows": AsyncMock(), "max_bytes": 1024, "timeout": 30}
    arguments_by_field[field] = value
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="source COPY bounds are invalid"):
        archive.ReferenceFamilySourceCopy(**arguments_by_field)


@pytest.mark.parametrize("fault", ("unknown-option", "foreign-fence", "session-owner"))
def test_source_creation_refuses_unknown_or_cross_family_authority_before_opening_sessions(fault):
    factory = Mock()
    copied, recorded = AsyncMock(), AsyncMock()
    options_by_field = {"source_copy": archive.ReferenceFamilySourceCopy(copied, 1024, 30), "on_precreated": recorded}
    field, value, reason = {
        "unknown-option": ("allow_unowned", True, "source options are invalid"),
        "foreign-fence": ("canonical_source_fence", object(), "fence scope differs"),
        "session-owner": ("source_sessions", None, "owning session is unavailable"),
    }[fault]
    options_by_field[field] = value
    with pytest.raises(archive.ReferenceFamilyArchiveError, match=reason):
        archive._source_capture_options(factory, "geo", options_by_field)
    factory.assert_not_called()
    copied.assert_not_awaited()
    recorded.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,value",
    (
        ("max_bytes", True),
        ("max_bytes", -1),
        ("max_bytes", 2**63),
        ("timeout", 86401),
        ("timeout", 0),
        ("timeout", -1),
        ("timeout", float("inf")),
        ("timeout", float("nan")),
        ("query", "DELETE FROM candidate"),
        ("columns", ()),
        ("columns", ("value", "value")),
        ("columns", ("value",) * 1601),
        ("columns", ("value;DROP",)),
        ("schema_name", "candidate;DROP"),
    ),
)
async def test_projection_refuses_unbounded_or_executable_scope_before_driver_acquisition(field, value):
    session = SimpleNamespace(in_transaction=lambda: True, connection=AsyncMock())
    options_by_field = {
        "query": "SELECT 1",
        "schema_name": "candidate",
        "table_name": "rows",
        "columns": ("value",),
        "max_bytes": 1024,
        "timeout": 30,
    }
    options_by_field[field] = value
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="projection COPY bounds differ"):
        await archive.native_copy_projection(session, **options_by_field)
    session.connection.assert_not_awaited()


@pytest.mark.asyncio
async def test_projection_refuses_unsupported_wire_format_before_copy_or_spooling():
    driver, spool = SimpleNamespace(copy_from_query=AsyncMock()), Mock()
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="COPY format differs"):
        await archive._capture_native_projection(driver, "SELECT 1", spool, 1024, 30, copy_format="json")
    driver.copy_from_query.assert_not_awaited()
    spool.write.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "spec",
    (
        None,
        archive.ReferenceFamilySpec("empty", ()),
        archive.ReferenceFamilySpec("duplicate", (archive.models.NUCCTaxonomy,) * 2),
    ),
)
async def test_model_stage_refuses_invalid_declarations_before_namespace_creation(spec):
    session = SimpleNamespace(in_transaction=lambda: True, execute=AsyncMock(), scalar=AsyncMock())
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="model declaration is invalid"):
        await archive.precreate_model_family_stage(session, spec, _ownership().dataset_id)
    session.execute.assert_not_awaited()
    session.scalar.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ("set", "complete", "cleanup"))
async def test_stage_entrypoints_refuse_untyped_ownership_before_catalog_or_ddl(operation):
    session = SimpleNamespace(in_transaction=lambda: True, execute=AsyncMock(), scalar=AsyncMock())
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="ownership is invalid"):
        if operation == "set":
            await archive.validate_nucc_reference_set(session, ownership={})
        elif operation == "complete":
            await archive.complete_reference_family_restore(session, {})
        else:
            await archive.cleanup_model_family_stage(session, archive.reference_family_spec("nucc"), {})
    session.execute.assert_not_awaited()
    session.scalar.assert_not_awaited()


@pytest.mark.parametrize("fault", (None, "importer", "authority"))
def test_immutable_source_contract_requires_exact_nucc_tracked_provenance(fault):
    manifest = replace(
        _declared_manifest("nucc"),
        publication_authority="tracked-generation",
        source_serving_generation=_serving_generation(),
        source_capture_contract=archive.IMMUTABLE_NUCC_SOURCE_CAPTURE_CONTRACT,
    )
    document = manifest.as_dict()
    if fault:
        field, value = ("importer_id", "geo") if fault == "importer" else ("publication_authority", "manual-only")
        document[field] = value
        with pytest.raises(archive.ReferenceFamilyArchiveError, match="immutable capture requires the NUCC family"):
            archive.validate_reference_family_manifest(document)
    else:
        assert archive.validate_reference_family_manifest(document) == manifest
        assert archive.reference_family_profile_contract(manifest) == archive.NUCC_CONTRACT


@pytest.mark.parametrize("importer", ("claims-pricing", "drug-claims"))
def test_terminal_source_history_cannot_claim_a_tracked_producer_generation(importer):
    manifest = replace(
        _declared_manifest(importer),
        publication_authority="tracked-generation",
        source_serving_generation=_serving_generation(),
    )
    with pytest.raises(
        archive.ReferenceFamilyArchiveError, match="terminal reference captures have no producer generation"
    ):
        archive.validate_reference_family_manifest(manifest)


@pytest.mark.parametrize("fault", ("automatic", "generation", "authority"))
def test_terminal_cutover_never_promotes_history_into_automatic_authority(fault):
    manifest = _declared_manifest("claims-pricing")
    cutover = archive.ReferenceFamilyCutoverAuthority("a" * 64, archive.CONTRACT, 12, 12, "manual")
    if fault == "automatic":
        cutover = replace(cutover, authority="automatic")
    elif fault == "generation":
        manifest = replace(manifest, source_serving_generation=_serving_generation())
    else:
        manifest = replace(manifest, publication_authority="tracked-generation")
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="require explicit manual activation"):
        archive._require_terminal_manual_cutover(archive.reference_family_spec("claims-pricing"), manifest, cutover)


@pytest.mark.asyncio
@pytest.mark.parametrize("importer,run_id", (("nucc", "run"), ("claims-pricing", ""), ("drug-claims", None)))
async def test_terminal_capture_refuses_unscoped_selector_before_authority_queries(importer, run_id):
    session = SimpleNamespace(in_transaction=lambda: True, execute=AsyncMock(), scalar=AsyncMock())
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="source selector is invalid"):
        await archive.capture_terminal_reference_source(session, importer_id=importer, schema_name="mrf", run_id=run_id)
    session.execute.assert_not_awaited()
    session.scalar.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("importer,schema", (("geo", "mrf"), ("tiger", "other")))
async def test_captured_epoch_cannot_be_reused_for_another_family_or_schema(importer, schema):
    session = SimpleNamespace(execute=AsyncMock(), scalar=AsyncMock())
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="capture contract is unsupported"):
        await archive._capture_source_generation(
            session, archive.reference_family_spec(importer), schema, {}, archive.CAPTURED_TIGER_CONTRACT
        )
    session.execute.assert_not_awaited()
    session.scalar.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("observed", (True, False, None, 1))
async def test_native_handoff_decision_requires_actual_catalog_boolean(observed):
    session = SimpleNamespace(in_transaction=lambda: True, scalar=AsyncMock(return_value=observed))
    assert await archive.is_nucc_native_handoff_required(session, schema_name="mrf") is (observed is True)
    query, parameters = session.scalar.await_args.args
    assert "hp_snapshot_retention" in str(query) and "pg_catalog.pg_has_role(current_user,relowner,'USAGE')" in str(
        query
    )
    assert parameters == {"relation": '"mrf"."nucc_taxonomy"'}


@pytest.mark.asyncio
async def test_unprotected_predecessor_does_not_reserve_a_native_attempt():
    session = SimpleNamespace(in_transaction=lambda: True, scalar=AsyncMock(return_value=False), execute=AsyncMock())
    context_by_field = {}
    assert await archive.bind_nucc_native_attempt(session, context_by_field, schema_name="mrf") is None
    assert context_by_field == {}
    session.execute.assert_not_awaited()
    session.scalar.assert_awaited_once()


@pytest.mark.asyncio
async def test_missing_native_heap_refuses_before_index_catalog_or_mutation():
    session = SimpleNamespace(execute=AsyncMock(return_value=_Rows()))
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="native candidate is missing"):
        await archive._observe_nucc_native_stage(session, "mrf", "nucc_taxonomy_candidate")
    session.execute.assert_awaited_once()
    query, parameters = session.execute.await_args.args
    assert "NOT relrowsecurity" in str(query) and "NOT indisvalid OR NOT indisready OR NOT indislive" in str(query)
    assert parameters == {"relation": '"mrf"."nucc_taxonomy_candidate"'}


def test_resigned_native_handoff_cannot_reuse_its_predecessor_heap():
    handoff = _handoff_observations().handoff
    handoff.pop("precreated_stage")
    handoff["contract"] = archive.NUCC_HANDOFF_CONTRACT
    handoff["incumbent"].update(
        local_generation=1,
        relation_oids=[handoff["stage"]["relation_oid"]],
        serving_generation=_serving_generation().as_dict(),
    )
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="stage reuses its predecessor"):
        archive.validate_nucc_native_handoff(_resign_handoff(handoff))


@pytest.mark.parametrize("fault", ("custody", "reason", "completed", "identity", "digest"))
def test_abandonment_decoder_binds_exact_stage_and_canonical_identity_before_cleanup(fault):
    _run, handoff, abandonment = _cleanup_inputs()
    if fault == "custody":
        abandonment["candidate_custody"]["stage"] = {**handoff["stage"], "relfilenode": 999}
    else:
        field, value = {
            "reason": ("reason", "expired"),
            "completed": ("physical_cleanup_completed", True),
            "identity": ("preparation_id", abandonment["preparation_id"].upper()),
            "digest": ("admission_sha256", "g" * 64),
        }[fault]
        abandonment[field] = value
    with pytest.raises(
        archive.ReferenceFamilyArchiveError, match="abandonment custody|abandonment identity|abandonment digest"
    ):
        archive._nucc_abandonment(handoff, abandonment)


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ("absent", "params", "test", "published", "retired", "unrecorded", "observed"))
async def test_cleanup_rechecks_exact_terminal_ledger_before_any_physical_lookup(fault):
    run, handoff, abandonment = _cleanup_inputs()
    changes_by_fault = {
        "params": (run, "params", []),
        "test": (run["params"], "test", True),
        "published": (run["metrics"], "nucc_native_publication", {"committed": True}),
        "retired": (run["metrics"], "nucc_native_candidate_cleanups", {abandonment["preparation_id"]: {}}),
        "unrecorded": (run["metrics"], "nucc_native_abandonments", {}),
        "observed": (abandonment["observed_run"], "status", "failed"),
    }
    if fault == "absent":
        run = None
    else:
        target_fields, field, field_value = changes_by_fault[fault]
        target_fields[field] = field_value

    async def scalar(statement, _parameters=None):
        sql = str(statement)
        if "pg_database" in sql:
            return handoff["database_oid"]
        assert "relation.oid FROM pg_catalog.pg_class" in sql
        return handoff["import_run_oid"]

    session = SimpleNamespace(
        execute=AsyncMock(return_value=_Rows([] if run is None else [run])), scalar=AsyncMock(side_effect=scalar)
    )
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="cleanup run differs|cleanup receipt differs"):
        await archive._require_nucc_cleanup_run(session, handoff, abandonment)
    session.execute.assert_awaited_once()
    query = str(session.execute.await_args.args[0])
    assert "FOR UPDATE NOWAIT" in query and "node_id=:node_id" in query
    if fault in {"absent", "params", "test"}:
        session.scalar.assert_not_awaited()


@pytest.mark.parametrize("fault", ("shape", "empty", "extra", "live", "relation", "oid", "database"))
def test_retained_inventory_refuses_live_or_substituted_storage_before_cleanup(fault):
    inventory = _retained_inventory()
    if fault == "shape":
        inventory["unexpected"] = True
    elif fault == "empty":
        inventory["relations"] = []
    elif fault == "extra":
        inventory["relations"].append(dict(inventory["relations"][0]))
    elif fault == "database":
        inventory["database_oid"] = True
    else:
        field, value = {
            "live": ("schema_name", "mrf"),
            "relation": ("relation_name", "other"),
            "oid": ("relation_oid", 2**32),
        }[fault]
        inventory["relations"][0][field] = value
    with pytest.raises(
        archive.ReferenceFamilyArchiveError, match="retained inventory|retained family|retained location"
    ):
        archive._nucc_retained_inventory(inventory)


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ("cleanup-owner", "cleanup-callback", "retire", "publish"))
async def test_native_retirement_and_publication_require_trusted_callbacks_before_sql(operation):
    _run, handoff, abandonment = _cleanup_inputs()
    session = SimpleNamespace(in_transaction=lambda: True, execute=AsyncMock(), scalar=AsyncMock())
    with pytest.raises(
        archive.ReferenceFamilyArchiveError, match="trusted .* authority|trusted publication continuation"
    ):
        if operation.startswith("cleanup"):
            await archive.cleanup_nucc_native_handoff(
                session,
                handoff,
                abandonment=abandonment,
                runtime_owner_oids=() if operation == "cleanup-owner" else (handoff["stage"]["owner_oid"],),
                assert_unreferenced=None if operation == "cleanup-callback" else AsyncMock(),
            )
        elif operation == "retire":
            await archive.cleanup_nucc_retained_publication(
                session, inventory=_retained_inventory(), assert_unreferenced=None
            )
        else:
            await archive.complete_nucc_native_handoff(session, handoff, publication_continuation=None)
    session.execute.assert_not_awaited()
    session.scalar.assert_not_awaited()


@pytest.mark.parametrize("value", ({"payload": "x" * 131072}, ["x" * 131072]))
def test_native_control_digest_refuses_oversized_metadata(value):
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="metadata exceeds its bound"):
        archive.nucc_native_digest(value)


def test_publication_decoder_refuses_additional_unbound_authority():
    receipt = _publication(_handoff_observations().handoff)
    receipt["approved"] = True
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="native publication differs"):
        archive.validate_nucc_native_publication(receipt)


@pytest.mark.asyncio
async def test_manual_activation_cannot_rotate_an_owner_sealed_nucc_predecessor():
    session = SimpleNamespace(in_transaction=lambda: True, scalar=AsyncMock(return_value=True), execute=AsyncMock())
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="requires validated publisher activation"):
        await archive.activate_reference_family_stage(
            session,
            ownership=_ownership("nucc", (("nucc_taxonomy", 43),)),
            manifest=_declared_manifest("nucc"),
            expected_incumbent=_incumbent("nucc", (("nucc_taxonomy", 44),)),
            authority="manual",
        )
    session.execute.assert_not_awaited()
    session.scalar.assert_awaited_once()


@pytest.mark.parametrize("fault", ("unregistered-canonical", "canonical-fields", "contract-envelope"))
def test_canonical_manifest_never_upgrades_another_family_or_legacy_envelope(fault):
    with pytest.raises(
        archive.ReferenceFamilyArchiveError,
        match="canonical model family|canonical model receipt|canonical model contract",
    ):
        if fault == "unregistered-canonical":
            archive.reference_family_spec("nucc", canonical=True)
        elif fault == "canonical-fields":
            archive._validate_mrf_auxiliary_receipt({**archive._canonical_model_receipt(), "source_bit": 1})
        else:
            document = _declared_manifest("mrf", canonical=True).as_dict()
            document["contract"] = archive.CONTRACT
            archive.validate_reference_family_manifest(document)


@pytest.mark.asyncio
async def test_legacy_mrf_source_cannot_create_a_new_noncanonical_clone():
    manifest = _declared_manifest("mrf")
    assert archive.reference_family_profile_contract(manifest) == archive.MRF_CONTRACT
    capture = archive.ReferenceFamilySourceCapture(manifest, "mrf", "00000003-00000018-1")
    session = SimpleNamespace(execute=AsyncMock(), scalar=AsyncMock())
    copier, recorded = AsyncMock(), AsyncMock()
    with pytest.raises(
        archive.ReferenceFamilyArchiveError, match="new MRF source requires the canonical model contract"
    ):
        await archive._clone_source(
            session,
            capture,
            _ownership().schema_name,
            source_copy=archive.ReferenceFamilySourceCopy(copier, 1024, 30),
            on_precreated=recorded,
            deadline=asyncio.get_running_loop().time() + 30,
        )
    assert [str(call.args[0]) for call in session.execute.await_args_list] == [
        "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ",
        "SET TRANSACTION SNAPSHOT '00000003-00000018-1'",
    ]
    copier.assert_not_awaited()
    recorded.assert_not_awaited()
    session.scalar.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ("lock-source", "clone"))
async def test_canonical_copy_rechecks_live_publisher_fence_before_candidate_creation(operation):
    transaction = SimpleNamespace(is_active=True)
    holder = SimpleNamespace(get_transaction=lambda: transaction)
    fence = archive.CanonicalSourceFence(holder, transaction, 21, 22, 23, "4/5")
    session = SimpleNamespace(in_transaction=lambda: True, execute=AsyncMock(), scalar=AsyncMock(return_value=False))
    copied, recorded = AsyncMock(), AsyncMock()
    with pytest.raises(RuntimeError, match="canonical source fence identity differs"):
        if operation == "lock-source":
            await archive._lock_source_family(
                session, archive.reference_family_spec("mrf", canonical=True), "mrf", fence
            )
        else:
            capture = archive.ReferenceFamilySourceCapture(
                _declared_manifest("mrf", canonical=True), "mrf", "00000003-00000018-1", fence
            )
            await archive._clone_source(
                session,
                capture,
                _ownership().schema_name,
                source_copy=archive.ReferenceFamilySourceCopy(copied, 1024, 30),
                on_precreated=recorded,
                deadline=asyncio.get_running_loop().time() + 30,
            )
    assert session.scalar.await_args.args[1] == {"pid": 21, "database": 22, "relation": 23, "transaction": "4/5"}
    queries = [str(call.args[0]) for call in session.execute.await_args_list]
    assert queries[-1] == 'LOCK TABLE "mrf"."address_archive_v2" IN ACCESS SHARE MODE NOWAIT'
    assert not any(sql.startswith(("CREATE", "INSERT", "COPY")) for sql in queries)
    copied.assert_not_awaited()
    recorded.assert_not_awaited()


@pytest.mark.asyncio
async def test_canonical_table_receipt_refuses_missing_actual_source_heap():
    session = SimpleNamespace(scalar=AsyncMock(return_value=None), execute=AsyncMock())
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="canonical source model is unavailable"):
        await archive._canonical_table_receipt(session, "mrf", "mrf", archive.canonical_contribution_model("mrf"))
    session.scalar.assert_awaited_once()
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("native_set", (False, True))
async def test_restored_canonical_auxiliary_keeps_native_set_and_historical_hash_contracts_separate(
    monkeypatch, native_set
):
    columns = [
        {"attname": name, "type": kind, "attnotnull": True, "default_expression": None}
        for name, kind in (("address_key", "uuid"), ("payload", "jsonb"))
    ]
    monkeypatch.setattr(archive.catalog_identity, "_catalog_columns", AsyncMock(return_value=columns))
    publication = _declared_manifest("mrf").auxiliary.copy()
    if not native_set:
        publication.pop("contract")
    session = SimpleNamespace(
        scalar=AsyncMock(side_effect=[43, 1, 0, 2] if native_set else [43, 1, 0]),
        execute=AsyncMock(return_value=_Rows([{"chunk_ordinal": 0, "chunk_row_count": 2, "chunk_sha256": "d" * 64}])),
    )
    receipt = await archive._mrf_auxiliary_receipt(session, "candidate", publication=publication)
    assert receipt["row_count"] == 2 and receipt["publication_sha256"] == publication["publication_sha256"]
    assert (receipt.get("contract") == archive._AUX_NATIVE_SET_CONTRACT) is native_set
    assert ("content_sha256" in receipt) is not native_set
    assert archive._validate_mrf_auxiliary_receipt(receipt) == receipt
    assert session.execute.await_count == int(not native_set)
    if native_set:
        assert str(session.scalar.await_args.args[0]) == f'SELECT count(*) FROM "candidate"."{archive.STAGE_TABLE}"'
    else:
        assert "row_value.payload" in str(session.execute.await_args.args[0])


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ("oid", "protected-owner"))
async def test_native_attempt_authenticates_predecessor_before_recording_context_or_precreating(fault):
    stage = _nucc_stage_receipt()
    context_by_field = {
        "control_run_id": stage["run_id"],
        "context": {
            "_control_attempt_id": stage["attempt_id"],
            "_control_attempt_started_at": stage["attempt_started_at"],
        },
    }
    original_context = deepcopy(context_by_field)
    authority_by_field = {
        **stage["incumbent"],
        **_serving_generation().as_dict(),
        "local_generation": 1,
        "relation_oids": [46],
    }
    session = SimpleNamespace(
        in_transaction=lambda: True,
        execute=AsyncMock(return_value=_Rows([authority_by_field])),
        scalar=AsyncMock(side_effect=[True, 47] if fault == "oid" else [True, 46, None]),
    )
    with pytest.raises(RuntimeError, match="native predecessor differs|protected owner is unavailable"):
        await archive.bind_nucc_native_attempt(session, context_by_field, schema_name="mrf")
    assert context_by_field == original_context
    session.execute.assert_awaited_once()
    assert str(session.execute.await_args.args[0]).startswith("SELECT importer_id, local_lineage_id")
    assert session.execute.await_args.args[1] == {"importer_id": "nucc"}
    assert session.scalar.await_count == (2 if fault == "oid" else 3)


@pytest.mark.asyncio
@pytest.mark.parametrize("importer", ("claims-pricing", "drug-claims"))
async def test_terminal_source_has_no_generation_authority_even_if_other_ledgers_exist(importer):
    session = SimpleNamespace(scalar=AsyncMock(), execute=AsyncMock())
    assert await archive._source_serving_generation(session, archive.reference_family_spec(importer), "mrf") is None
    session.scalar.assert_not_awaited()
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("importer", ("claims-pricing", "drug-claims"))
async def test_terminal_incumbent_capture_rejects_partial_dictionary_before_locks(importer):
    spec = archive.reference_family_receive_spec(importer)
    scoped = archive.TERMINAL_CAPABILITIES[importer].dictionary_models
    missing = scoped[0].__tablename__

    async def scalar(statement, parameters=None):
        if str(statement).startswith("SHOW "):
            return "0"
        assert str(statement).startswith("SELECT relation.oid")
        return None if parameters["table_name"] == missing else 40 + spec.table_names.index(parameters["table_name"])

    session = SimpleNamespace(in_transaction=lambda: True, scalar=AsyncMock(side_effect=scalar), execute=AsyncMock())
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="terminal reference dictionary family is incomplete"):
        await archive.capture_reference_family_incumbent(session, importer_id=importer, schema_name="mrf")
    assert all(str(call.args[0]).startswith("SELECT pg_catalog.set_config") for call in session.execute.await_args_list)
    assert session.scalar.await_count == 2 + len(spec.model_types)


@pytest.mark.asyncio
async def test_drug_incumbent_recheck_includes_destination_local_effect_relations():
    spec = archive.reference_family_receive_spec("drug-claims")
    pairs = tuple((name, 40 + index) for index, name in enumerate(spec.table_names))
    missing = archive._DRUG_EFFECT_MODELS[0].__tablename__
    session = SimpleNamespace(
        scalar=AsyncMock(
            side_effect=lambda _sql, values: (
                None if values["table_name"] == missing else dict(pairs)[values["table_name"]]
            )
        )
    )
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="incumbent changed"):
        await archive._verify_incumbent(session, _incumbent("drug-claims", pairs))
    assert [call.args[1]["table_name"] for call in session.scalar.await_args_list] == list(spec.table_names)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "importer,present",
    (("claims-pricing", "partial"), ("claims-pricing", "all"), ("claims-pricing", "none"), ("drug-claims", "all")),
)
async def test_activation_cannot_rotate_before_complete_dictionary_and_shared_custody(importer, present):
    spec = archive.reference_family_receive_spec(importer)
    scoped_names = {model.__tablename__ for model in archive.TERMINAL_CAPABILITIES[importer].dictionary_models}
    absent = next(iter(sorted(scoped_names))) if present == "partial" else None
    pairs = tuple(
        (name, None if (present == "none" and name in scoped_names) or name == absent else 40 + index)
        for index, name in enumerate(spec.table_names)
    )
    owner = _ownership(importer, tuple(sorted((name, 100 + index) for index, name in enumerate(spec.table_names))))
    session = SimpleNamespace(in_transaction=lambda: True, execute=AsyncMock(), scalar=AsyncMock(return_value=None))
    with pytest.raises(
        archive.ReferenceFamilyArchiveError,
        match="dictionary predecessor is incomplete|shared dictionary is unavailable",
    ):
        await archive._complete_validated_stage_activation(
            session, spec, owner, _incumbent(importer, pairs), _declared_manifest(importer)
        )
    session.execute.assert_not_awaited()
    assert session.scalar.await_count == (0 if present == "partial" else 2)


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ("canonical", "mrf"))
async def test_selected_canonical_projection_requires_authenticator_before_catalog(operation):
    session = SimpleNamespace(in_transaction=lambda: True, scalar=AsyncMock(), execute=AsyncMock())
    context = (
        archive.selected_canonical_archive(
            session, schema_name="mrf", owner_oid=43, selected_relations={}, authenticate_inventory=None
        )
        if operation == "canonical"
        else archive.selected_mrf_canonical_archive(
            session, schema_name="mrf", owner_oid=43, selected_relation={}, authenticate_inventory=None
        )
    )
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="publisher authority differs"):
        async with context:
            pytest.fail("unauthenticated selected projection was exposed")
    session.execute.assert_not_awaited()
    session.scalar.assert_not_awaited()


@pytest.mark.parametrize("staging_name", ("", "candidate;DROP", 1))
def test_index_compiler_refuses_untrusted_staging_identifiers(staging_name):
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="staging index name is invalid"):
        archive._additional_index_sql(
            "candidate", archive.models.NUCCTaxonomy, {"index_elements": ("code",), "staging_name": staging_name}
        )


def test_drug_validation_receipt_requires_destination_effects_not_only_portable_models():
    spec = archive.reference_family_spec("drug-claims")
    receipt = _validation_receipt()
    receipt.update(
        importer_id="drug-claims",
        relation_oids=sorted([[model.__tablename__, 40 + index] for index, model in enumerate(spec.model_types)]),
        tables=[table.as_dict() for table in _declared_manifest("drug-claims").tables],
    )
    receipt["validation_sha256"] = archive._validation_digest(
        {key: value for key, value in receipt.items() if key != "validation_sha256"}
    )
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="validation inventory is invalid"):
        archive.validate_reference_family_validation_receipt(receipt)


@pytest.mark.asyncio
async def test_restored_nucc_manifest_runs_set_validation_before_accepting_schema_receipts(monkeypatch):
    owner, _manifest_value, census, session = _source_clone_observations(monkeypatch)
    census["invalid"] = 1
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="indexed set is invalid"):
        await archive._validate_stage_manifest(session, ownership=owner, manifest=_declared_manifest("nucc"))
    archive.catalog_identity._catalog_columns.assert_not_awaited()
    session.scalar.assert_not_awaited()
    session.execute.assert_awaited_once()


def _generationless_source_session(snapshot):
    async def scalar(statement, _parameters=None):
        sql = str(statement)
        if sql.startswith("SHOW "):
            return "0"
        if sql.startswith("SELECT to_regclass(format("):
            return False
        if sql.startswith("SELECT relation.oid FROM"):
            return 43
        assert sql.startswith("SELECT count(*)::bigint FROM")
        return 1

    async def execute(statement, _parameters=None):
        sql = str(statement)
        if sql == "SELECT pg_export_snapshot()":
            return SimpleNamespace(scalar_one=lambda: snapshot)
        assert sql.startswith(("SELECT pg_catalog.set_config", "LOCK TABLE"))
        return _Rows()

    return SimpleNamespace(
        in_transaction=lambda: True, scalar=AsyncMock(side_effect=scalar), execute=AsyncMock(side_effect=execute)
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("snapshot", ("00000003-00000018-1", "not a PostgreSQL snapshot"))
async def test_nucc_source_refuses_untracked_generation_and_rolls_back_before_precreation(monkeypatch, snapshot):
    session = _generationless_source_session(snapshot)
    monkeypatch.setattr(archive.catalog_identity, "_catalog_columns", AsyncMock(return_value=_nucc_columns()))
    monkeypatch.setattr(archive.catalog_identity, "_catalog_constraints", AsyncMock(return_value=[]))
    monkeypatch.setattr(archive.catalog_identity, "_catalog_indexes", AsyncMock(return_value=[]))
    events = []

    @asynccontextmanager
    async def transaction():
        events.append("begin")
        try:
            yield
        except BaseException:
            events.append("rollback")
            raise
        else:
            events.append("commit")

    @asynccontextmanager
    async def factory():
        yield session

    session.begin = transaction
    copied, precreated, prepared = AsyncMock(), AsyncMock(), AsyncMock()
    with pytest.raises(
        archive.ReferenceFamilyArchiveError, match="source generation is unavailable|source snapshot is invalid"
    ):
        await archive.prepare_nucc_reference_archive_source(
            factory,
            dataset_id=_ownership().dataset_id,
            on_prepared=prepared,
            on_precreated=precreated,
            source_copy=archive.ReferenceFamilySourceCopy(copied, 1024, 30),
            source_metadata_factory=AsyncMock(return_value={"source_release": "synthetic"}),
        )
    assert events == ["begin", "rollback"]
    copied.assert_not_awaited()
    precreated.assert_not_awaited()
    prepared.assert_not_awaited()
    assert any(str(call.args[0]) == "SELECT pg_export_snapshot()" for call in session.execute.await_args_list)
